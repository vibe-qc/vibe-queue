"""Regression: `vq admin update` must resume ONLY the jobs IT paused.

Incident (2026-06-22, host_b): a ``vq admin update vibeqc-queue`` resumed
22 jobs that a PRIOR *interrupted* update had left SUSPENDED — not just
the jobs this run paused — spiking the box to load 88 on heavy btrfs+LUKS
I/O. Root cause: the non-surgical resume path used a blanket
``resume_all`` (no filter), which SIGCONTs every SUSPENDED job in the
queue regardless of who paused it (a prior run's stragglers, operator
pauses, the build script's ``update-script``-tagged pauses).

The fix (v0.11.1): tag each admin-update pause with a per-invocation
token (:func:`vq.admin._new_pause_token`) and resume only jobs carrying
that exact token. A prior run's tag (or none) no longer matches, so its
stuck-paused jobs stay paused. NOT a shared constant tag — a later run
would re-match a constant and re-wake the stragglers.

These tests drive the REAL pause/resume path (real SIGSTOP'd child
processes + on-disk specs); only the git pull / update_script subprocess
is mocked.
"""
from __future__ import annotations

import contextlib
import os
import signal
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config, paths
from vq.pause_resume import pause_job
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated vq state + config dirs. Also forces the single-user
    queue path: the suite may run on a host with a live system
    multi-user daemon (host_a/host_d) or a personal daemon (host_c2),
    and the incident + fix are orthogonal to the multi-user sweep."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: False)
    return tmp_path


def _spawn_running_job(jobid: str) -> tuple[JobSpec, subprocess.Popen]:
    """Spawn a real ``sleep`` in its own session; write a RUNNING spec
    capturing its pid + pgid so the real pause/resume verbs can
    SIGSTOP/SIGCONT it. Returns (spec, popen) for cleanup."""
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(["sleep", "300"], start_new_session=True)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "300"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=proc.pid,
        pgid=os.getpgid(proc.pid),
        started_at="2026-06-22T12:00:00+00:00",
    )
    spec.write(paths.spec_path(jobid))
    return spec, proc


def _kill(proc: subprocess.Popen) -> None:
    """Best-effort teardown: SIGCONT a possibly-stopped child so it can
    process the kill, then SIGKILL + reap."""
    with contextlib.suppress(ProcessLookupError, OSError):
        os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
    with contextlib.suppress(ProcessLookupError, OSError):
        proc.kill()
    with contextlib.suppress(Exception):
        proc.wait(timeout=5)


def _make_git_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / ".git").mkdir()
    return path


def _ok_proc(stdout: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout=stdout, stderr="",
    )


def _write_queue_env(cfg_dir: Path, repo: Path, name: str = "vibeqc-queue") -> None:
    """A non-surgical venv env (no ``provides_branches`` → the
    ``pause_all`` / ``resume_all`` path), no ``update_script`` so the
    only subprocess is the (mocked) git pull."""
    (cfg_dir / "config.toml").write_text(
        'default_host = "localhost"\n'
        f'[programs.{name}]\n'
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
    )


# ----------------------------------------------------------------------
# The per-invocation token primitive
# ----------------------------------------------------------------------


class TestPauseToken:
    def test_token_is_unique_per_call(self) -> None:
        tokens = {admin._new_pause_token() for _ in range(200)}
        assert len(tokens) == 200, "pause tokens must be unique per call"

    def test_token_is_paused_by_charset_safe(self) -> None:
        """The token flows into the spec's ``paused_by`` field, which is
        validated by the same strict charset as ``job_name``. A token
        that fails it would crash the pause mid-update."""
        from vq.spec import JOB_NAME_PATTERN

        for _ in range(50):
            token = admin._new_pause_token()
            assert token.startswith("admin-update-")
            assert JOB_NAME_PATTERN.fullmatch(token), token
            assert len(token) <= 50


# ----------------------------------------------------------------------
# The incident: a later update must not wake a prior run's paused jobs
# ----------------------------------------------------------------------


class TestAdminUpdateResumeScope:
    def test_prior_paused_job_not_resumed_by_update_env(
        self, state: Path
    ) -> None:
        """`update_env` resumes only the jobs it paused this run. A job a
        prior interrupted update left SUSPENDED stays SUSPENDED."""
        repo = _make_git_repo(state / "repo")
        _write_queue_env(state / "cfg", repo)
        cfg = config.load_config()

        # Job A: left SUSPENDED by a PRIOR (interrupted) update — tagged
        # the way the build script's cooperative pause would tag it.
        spec_a, proc_a = _spawn_running_job("aaaa00000001")
        # Job B: a normal RUNNING job this update will pause + resume.
        spec_b, proc_b = _spawn_running_job("bbbb00000002")
        try:
            pause_job("localhost", spec_a.id, paused_by="update-script")
            assert (
                JobSpec.read(paths.spec_path(spec_a.id)).state
                is JobState.SUSPENDED
            )

            with patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc("Already up to date.\n")],
            ):
                result = admin.update_env(
                    "vibeqc-queue", cfg, host="localhost",
                )

            assert result.success

            # The regression: Job A must NOT have been woken.
            a_after = JobSpec.read(paths.spec_path(spec_a.id))
            assert a_after.state is JobState.SUSPENDED, (
                "a prior interrupted update's paused job was resumed by "
                "the later update — the 2026-06-22 host_b load-88 regression"
            )
            assert a_after.paused_by == "update-script"

            # Sanity: this run's own job WAS paused and resumed (the
            # normal bracket still works; the fix only narrows the scope).
            b_after = JobSpec.read(paths.spec_path(spec_b.id))
            assert b_after.state is JobState.RUNNING
            assert b_after.paused_by is None
            assert b_after.paused_seconds_total >= 0.0
        finally:
            _kill(proc_a)
            _kill(proc_b)

    def test_prior_paused_job_not_resumed_by_update_all(
        self, state: Path
    ) -> None:
        """Same scoping guarantee for the batch verb `update_all`, whose
        pause/resume bracket the whole batch once."""
        repo = _make_git_repo(state / "repo")
        _write_queue_env(state / "cfg", repo)
        cfg = config.load_config()

        spec_a, proc_a = _spawn_running_job("aaaa00000003")
        spec_b, proc_b = _spawn_running_job("bbbb00000004")
        try:
            pause_job("localhost", spec_a.id, paused_by="update-script")

            with patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc("Already up to date.\n")],
            ):
                results = admin.update_all(cfg, host="localhost")

            assert all(r.success for r in results)

            a_after = JobSpec.read(paths.spec_path(spec_a.id))
            assert a_after.state is JobState.SUSPENDED
            assert a_after.paused_by == "update-script"

            b_after = JobSpec.read(paths.spec_path(spec_b.id))
            assert b_after.state is JobState.RUNNING
            assert b_after.paused_by is None
        finally:
            _kill(proc_a)
            _kill(proc_b)

    def test_operator_paused_job_also_preserved(self, state: Path) -> None:
        """The same mechanism preserves an OPERATOR's manual pause
        (``paused_by=None`` — bare ``vq pause``): an untagged pause can't
        be claimed by the update's tagged resume filter."""
        repo = _make_git_repo(state / "repo")
        _write_queue_env(state / "cfg", repo)
        cfg = config.load_config()

        spec_op, proc_op = _spawn_running_job("aaaa00000005")
        spec_b, proc_b = _spawn_running_job("bbbb00000006")
        try:
            # Bare operator pause — no tag.
            pause_job("localhost", spec_op.id)
            assert (
                JobSpec.read(paths.spec_path(spec_op.id)).paused_by is None
            )

            with patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc("Already up to date.\n")],
            ):
                result = admin.update_env(
                    "vibeqc-queue", cfg, host="localhost",
                )

            assert result.success
            op_after = JobSpec.read(paths.spec_path(spec_op.id))
            assert op_after.state is JobState.SUSPENDED, (
                "operator's manual pause was undone by an unrelated "
                "admin update"
            )
            assert JobSpec.read(paths.spec_path(spec_b.id)).state is JobState.RUNNING
        finally:
            _kill(proc_op)
            _kill(proc_b)
