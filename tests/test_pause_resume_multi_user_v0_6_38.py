"""v0.6.38: pause/resume resolve specs from the per-user state dirs.

In multi-user mode each job's spec lives under
``/var/lib/vq/users/<uid>/queue/``, not the single-user
``paths.queue_dir()``. Before this fix every pause/resume entry
point (`pause_job` / `resume_job` / `pause_all` / `resume_all` /
`pause_provides_branches` / `resume_jobs`) looked only in the
single-user queue dir — so on a multi-user host:

* `vq pause`/`vq resume` failed with "no such job", and
* the daemon's v0.6.20 host-pressure auto-pause (the OOM-prevention
  safety feature) silently no-op'd — `_host_pressure_pass` calls
  `pause_job`, which raised `FileNotFoundError` for every job.

These tests pin the per-user spec resolution for all six entry
points.
"""
from __future__ import annotations

import contextlib
import os
import subprocess
from pathlib import Path

import pytest

from vq import config, ownership, paths, pause_resume
from vq.pause_resume import (
    pause_all,
    pause_job,
    pause_provides_branches,
    resume_all,
    resume_job,
    resume_jobs,
)
from vq.spec import JobSpec, JobState


@pytest.fixture
def mu_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Multi-user state root + a provisioned dir for the test's uid."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "single"))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path / "mu"))
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    (config_dir / "config.toml").write_text(
        "[multi_user]\n"
        "enabled = true\n"
        'admin_group = "test-vq-admins"\n'
    )
    paths.provision_user_state(os.getuid(), os.getgid())
    return tmp_path


def _running_job_in_user_dir(jobid: str) -> tuple[JobSpec, subprocess.Popen]:
    """Spawn a real sleep, write a RUNNING spec into the per-user
    queue dir capturing its pid/pgid. Returns (spec, popen)."""
    uid = os.getuid()
    workspace = paths.user_workspace_dir(uid, jobid)
    workspace.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=proc.pid,
        pgid=os.getpgid(proc.pid),
        started_at="2026-05-21T12:00:00+00:00",
        submitter=str(uid),
    )
    spec.write(paths.user_spec_path(uid, jobid))
    return spec, proc


def _write_user_spec(jobid: str, *, state: JobState, branch: str | None = None,
                     pgid: int | None = 999000) -> None:
    """Drop a spec (no real process) into the per-user queue dir."""
    uid = os.getuid()
    ws = paths.user_workspace_dir(uid, jobid)
    ws.mkdir(parents=True, exist_ok=True)
    JobSpec(
        id=jobid, command=["true"], cwd=str(ws), cpus=1, state=state,
        pgid=pgid, branch=branch, submitter=str(uid),
        started_at="2026-05-21T12:00:00+00:00",
    ).write(paths.user_spec_path(uid, jobid))


def _enable_system_multi_user_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Make the system policy authoritative while personal policy is off."""
    config.config_path().write_text(
        "[multi_user]\n"
        "enabled = false\n"
        'admin_group = "personal-admins"\n'
    )
    system_config = tmp_path / "system-config.toml"
    system_config.write_text(
        "[multi_user]\n"
        "enabled = true\n"
        'admin_group = "test-vq-admins"\n'
    )
    monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_config)
    monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)


def _write_foreign_user_spec(
    jobid: str,
    *,
    state: JobState,
    paused_by: str | None = None,
) -> Path:
    foreign_uid = os.geteuid() + 10_000
    queue = paths.user_queue_dir(foreign_uid)
    workspace = paths.user_workspace_dir(foreign_uid, jobid)
    queue.mkdir(parents=True, exist_ok=True)
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        pgid=991_337,
        submitter=str(foreign_uid),
        started_at="2026-05-21T12:00:00+00:00",
        paused_by=paused_by,
    )
    if state == JobState.SUSPENDED:
        spec.paused_at = "2026-05-21T12:00:00+00:00"
        spec.paused_monotonic_at = 1.0
    path = paths.user_spec_path(foreign_uid, jobid)
    spec.write(path)
    return path


class TestSingleJobResolution:
    def test_pause_job_finds_per_user_spec(
        self, mu_state: Path
    ) -> None:
        spec, proc = _running_job_in_user_dir("mujob-pause-1")
        try:
            # Single-user resolution can't see the per-user spec.
            with pytest.raises(FileNotFoundError):
                pause_job("localhost", "mujob-pause-1")
            # Multi-user resolution finds + pauses it.
            msg = pause_job("localhost", "mujob-pause-1", multi_user=True)
            assert "paused" in msg
            recovered = JobSpec.read(
                paths.user_spec_path(os.getuid(), "mujob-pause-1")
            )
            assert recovered.state == JobState.SUSPENDED
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_resume_job_finds_per_user_spec(self, mu_state: Path) -> None:
        spec, proc = _running_job_in_user_dir("mujob-resume-1")
        try:
            pause_job("localhost", "mujob-resume-1", multi_user=True)
            with pytest.raises(FileNotFoundError):
                resume_job("localhost", "mujob-resume-1")
            msg = resume_job("localhost", "mujob-resume-1", multi_user=True)
            assert "resumed" in msg
            recovered = JobSpec.read(
                paths.user_spec_path(os.getuid(), "mujob-resume-1")
            )
            assert recovered.state == JobState.RUNNING
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()


class TestMultiUserOwnership:
    def test_foreign_pause_and_filtered_resume_fail_before_signal_or_state_leak(
        self,
        mu_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enable_system_multi_user_policy(mu_state, monkeypatch)
        pause_path = _write_foreign_user_spec(
            "foreignpause1", state=JobState.RUNNING
        )
        resume_path = _write_foreign_user_spec(
            "foreignresum1",
            state=JobState.SUSPENDED,
            paused_by="private-operator-tag",
        )
        signals: list[tuple[int, int]] = []
        monkeypatch.setattr(
            pause_resume.os,
            "killpg",
            lambda pgid, sig: signals.append((pgid, sig)),
        )

        with pytest.raises(ownership.OwnershipError, match="belongs to uid"):
            pause_job("localhost", "foreignpause1", multi_user=True)
        with pytest.raises(ownership.OwnershipError, match="belongs to uid"):
            resume_job(
                "localhost",
                "foreignresum1",
                paused_by_filter="different-tag",
                multi_user=True,
            )

        assert signals == []
        assert JobSpec.read(pause_path).state == JobState.RUNNING
        resumed = JobSpec.read(resume_path)
        assert resumed.state == JobState.SUSPENDED
        assert resumed.paused_by == "private-operator-tag"

    def test_bulk_pause_isolates_foreign_job_without_classifying_its_state(
        self,
        mu_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enable_system_multi_user_policy(mu_state, monkeypatch)
        own, proc = _running_job_in_user_dir("ownedbulkpaus")
        foreign_path = _write_foreign_user_spec(
            "foreignbulkpa", state=JobState.SUSPENDED
        )
        try:
            summary = pause_all("localhost", multi_user=True)

            assert summary == "paused 1 job (1 error(s))"
            assert "already suspended" not in summary
            assert JobSpec.read(
                paths.user_spec_path(os.getuid(), own.id)
            ).state == JobState.SUSPENDED
            assert JobSpec.read(foreign_path).state == JobState.SUSPENDED
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_bulk_resume_isolates_foreign_job_without_classifying_its_state(
        self,
        mu_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enable_system_multi_user_policy(mu_state, monkeypatch)
        own, proc = _running_job_in_user_dir("ownedbulkresu")
        pause_job("localhost", own.id, multi_user=True)
        foreign_path = _write_foreign_user_spec(
            "foreignbulkrs", state=JobState.RUNNING
        )
        try:
            summary = resume_all("localhost", multi_user=True)

            assert summary == "resumed 1 job (1 error(s))"
            assert "already running" not in summary
            assert JobSpec.read(
                paths.user_spec_path(os.getuid(), own.id)
            ).state == JobState.RUNNING
            assert JobSpec.read(foreign_path).state == JobState.RUNNING
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_scoped_bulk_helpers_isolate_foreign_state(
        self,
        mu_state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _enable_system_multi_user_policy(mu_state, monkeypatch)
        pause_path = _write_foreign_user_spec(
            "foreignbranch", state=JobState.SUSPENDED
        )
        pause_spec = JobSpec.read(pause_path)
        pause_spec.branch = "dev-env"
        pause_spec.write(pause_path)
        resume_path = _write_foreign_user_spec(
            "foreignlisted", state=JobState.RUNNING
        )

        pause_summary, paused = pause_provides_branches(
            "localhost", ["dev-env"], multi_user=True
        )
        resume_summary = resume_jobs(
            "localhost", ["foreignlisted"], multi_user=True
        )

        assert paused == []
        assert pause_summary == (
            "paused 0 jobs (2 error(s)) [scope: branches=['dev-env']]"
        )
        assert "already suspended" not in pause_summary
        assert resume_summary == "resumed 0 jobs (1 error(s))"
        assert "already running" not in resume_summary
        assert JobSpec.read(pause_path).state == JobState.SUSPENDED
        assert JobSpec.read(resume_path).state == JobState.RUNNING

class TestBulkSweepsPerUserDirs:
    def test_pause_all_sweeps_per_user_dirs(self, mu_state: Path) -> None:
        spec, proc = _running_job_in_user_dir("mujob-pa-1")
        try:
            # Single-user pause_all sees nothing under the per-user root.
            su = pause_all("localhost")
            assert "paused 0 jobs" in su
            # Multi-user pause_all finds + pauses the per-user job.
            mu = pause_all("localhost", multi_user=True)
            assert "paused 1 job" in mu
            recovered = JobSpec.read(
                paths.user_spec_path(os.getuid(), "mujob-pa-1")
            )
            assert recovered.state == JobState.SUSPENDED
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_resume_all_sweeps_per_user_dirs(self, mu_state: Path) -> None:
        spec, proc = _running_job_in_user_dir("mujob-ra-1")
        try:
            pause_all("localhost", multi_user=True)
            mu = resume_all("localhost", multi_user=True)
            assert "resumed 1 job" in mu
            recovered = JobSpec.read(
                paths.user_spec_path(os.getuid(), "mujob-ra-1")
            )
            assert recovered.state == JobState.RUNNING
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_resume_jobs_resolves_per_user(self, mu_state: Path) -> None:
        spec, proc = _running_job_in_user_dir("mujob-rj-1")
        try:
            pause_job("localhost", "mujob-rj-1", multi_user=True)
            mu = resume_jobs("localhost", ["mujob-rj-1"], multi_user=True)
            assert "resumed 1 job" in mu
            # Single-user can't resolve it → counted as gone.
            su = resume_jobs("localhost", ["mujob-rj-1"])
            assert "resumed 0 jobs" in su
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()

    def test_pause_provides_branches_sweeps_per_user_dirs(
        self, mu_state: Path
    ) -> None:
        spec, proc = _running_job_in_user_dir("mujob-pb-1")
        # Tag the spec's branch so pause_provides_branches matches it.
        sp = paths.user_spec_path(os.getuid(), "mujob-pb-1")
        s = JobSpec.read(sp)
        s.branch = "dev-env"
        s.write(sp)
        try:
            summary, paused = pause_provides_branches(
                "localhost", ["dev-env"], multi_user=True
            )
            assert paused == ["mujob-pb-1"]
            recovered = JobSpec.read(sp)
            assert recovered.state == JobState.SUSPENDED
        finally:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(os.getpgid(proc.pid), 9)
            proc.wait()


class TestSingleUserUnaffected:
    def test_multi_user_false_still_uses_queue_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # With multi_user=False (default), a missing single-user queue
        # dir yields the clean "0 jobs" summary — no per-user behaviour
        # leaks into single-user mode.
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "su"))
        msg = pause_all("localhost")
        assert "paused 0 jobs" in msg
