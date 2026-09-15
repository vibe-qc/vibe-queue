"""Tests for pause/resume: SUSPENDED state + paused_seconds_total
accounting + the CLI verbs."""
from __future__ import annotations

import contextlib
import json
import math
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import config, ownership, paths, pause_resume
from vq.cli import main
from vq.pause_resume import (
    PauseError,
    pause_job,
    pause_scheduler_job,
    resume_job,
    resume_scheduler_job,
)
from vq.scheduler_dialect import SchedulerPhase
from vq.scheduler_dispatch import SchedulerError, SchedulerHandle
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _running_job_with_real_process(jobid: str, state_dir: Path) -> tuple[JobSpec, subprocess.Popen]:
    """Spawn a sleep that we can SIGSTOP/SIGCONT, write a RUNNING spec
    capturing its real pid + pgid. Returns (spec, popen) so the test
    can clean up."""
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    pgid = os.getpgid(proc.pid)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=proc.pid,
        pgid=pgid,
        started_at="2026-05-09T12:00:00+00:00",
    )
    spec.write(paths.spec_path(jobid))
    return spec, proc


def _release_for_cleanup(
    proc: subprocess.Popen,
    pgid: int | None,
    *,
    killpg: Callable[[int, int], None] = os.killpg,
) -> None:
    """SIGCONT a test's own job group in cleanup, best effort (#29).

    Sent only while the group's leader, this test's unreaped ``proc``, has not
    exited: an unreaped leader pins the pgid, so the signal cannot reach a
    reused id. ``OSError`` is tolerated, because on macOS a group whose
    members are exiting answers ``EPERM`` before ``ESRCH`` (#27). ``killpg``
    lets a test that patched ``os.killpg`` pass the real one.
    """
    if pgid is None or proc.poll() is not None:
        return
    with contextlib.suppress(OSError):
        killpg(pgid, signal.SIGCONT)


class _FakeSchedulerDispatcher:
    def __init__(
        self,
        phase: SchedulerPhase = SchedulerPhase.PENDING,
        *,
        remote_root: str = "/remote",
        poll_hook: Callable[[list[SchedulerHandle]], None] | None = None,
        hold_hook: Callable[[SchedulerHandle], None] | None = None,
        release_hook: Callable[[SchedulerHandle], None] | None = None,
        hold_error: BaseException | None = None,
        release_error: BaseException | None = None,
    ) -> None:
        self.phase = phase
        self.remote_root = remote_root
        self.poll_hook = poll_hook
        self.hold_hook = hold_hook
        self.release_hook = release_hook
        self.hold_error = hold_error
        self.release_error = release_error
        self.held: list[str] = []
        self.released: list[str] = []
        self.polled: list[str] = []

    def remote_workspace(self, jobid: str) -> str:
        return f"{self.remote_root}/{jobid}"

    def poll(self, handles: list[SchedulerHandle]) -> dict[str, SchedulerPhase]:
        self.polled.extend(h.job_id for h in handles)
        if self.poll_hook is not None:
            self.poll_hook(handles)
        return {h.job_id: self.phase for h in handles}

    def hold(self, handle: SchedulerHandle) -> None:
        self.held.append(handle.job_id)
        if self.hold_hook is not None:
            self.hold_hook(handle)
        if self.hold_error is not None:
            raise self.hold_error

    def release(self, handle: SchedulerHandle) -> None:
        self.released.append(handle.job_id)
        if self.release_hook is not None:
            self.release_hook(handle)
        if self.release_error is not None:
            raise self.release_error


class _BlockingFirstSchedulerMutation(_FakeSchedulerDispatcher):
    """Expose whether a second same-kind mutation overlaps the first."""

    def __init__(self, operation: str) -> None:
        super().__init__()
        self.operation = operation
        self.first_entered = threading.Event()
        self.second_entered = threading.Event()
        self.allow_first_return = threading.Event()
        self._counts = {"hold": 0, "release": 0}
        self._counts_lock = threading.Lock()

    def _gate(self, operation: str) -> None:
        if operation != self.operation:
            return
        with self._counts_lock:
            self._counts[operation] += 1
            call_number = self._counts[operation]
        if call_number == 1:
            self.first_entered.set()
            if not self.allow_first_return.wait(timeout=5):
                raise SchedulerError("test timed out waiting to release mutation")
        elif call_number == 2:
            self.second_entered.set()

    def hold(self, handle: SchedulerHandle) -> None:
        self.held.append(handle.job_id)
        self._gate("hold")

    def release(self, handle: SchedulerHandle) -> None:
        self.released.append(handle.job_id)
        self._gate("release")


def _scheduler_host_config() -> config.HostConfig:
    return config.HostConfig(
        ssh="host_f.invalid",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/home/USER",
        scheduler_driver="localhost",
    )


def _write_scheduler_cli_config(state: Path, *, driver: str = "localhost") -> None:
    remote_driver = (
        ""
        if driver == "localhost"
        else "\n[hosts.driver]\nssh = \"driver.invalid\"\nremote_vq = \"vq\"\n"
    )
    (state / "cfg" / "config.toml").write_text(
        "[hosts.localhost]\n"
        'ssh = "localhost"\n'
        f"{remote_driver}"
        "\n"
        "[hosts.host_f-scheduler-test]\n"
        'ssh = "host_f.invalid"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/home/USER"\n'
        f'scheduler_driver = "{driver}"\n'
    )


def _scheduler_job(
    jobid: str,
    *,
    state: JobState = JobState.RUNNING,
    scheduler_target: str = "host_f-scheduler-test",
    scheduler_state: str | None = "queued",
    paused_by: str | None = None,
) -> JobSpec:
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=["python", "run.py"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        scheduler_target=scheduler_target,
        scheduler_job_id="555.cluster",
        scheduler_state=scheduler_state,
        started_at="2026-06-30T12:00:00+00:00",
        paused_by=paused_by,
    )
    if state == JobState.SUSPENDED:
        spec.paused_at = "2026-06-30T12:00:00+00:00"
        spec.paused_monotonic_at = time.monotonic() - 0.1
    spec.write(paths.spec_path(jobid))
    return spec


def _install_owner_gate_spy(
    monkeypatch: pytest.MonkeyPatch,
    *,
    denied_jobids: set[str],
) -> None:
    """Require ownership checks to happen while the spec lock is held."""
    real_spec_lock = paths.spec_lock
    depth = 0

    @contextlib.contextmanager
    def tracked_spec_lock(spec_path: Path):
        nonlocal depth
        with real_spec_lock(spec_path):
            depth += 1
            try:
                yield
            finally:
                depth -= 1

    def check_owner(
        spec: JobSpec,
        *,
        cfg: config.Config | None = None,
        multi_user: bool = False,
    ) -> None:
        del cfg, multi_user
        assert depth > 0, "ownership must be decided inside the spec lock"
        if spec.id in denied_jobids:
            raise ownership.OwnershipError(f"job {spec.id} is foreign")

    monkeypatch.setattr(paths, "spec_lock", tracked_spec_lock)
    monkeypatch.setattr(ownership, "check_owner", check_owner)


class TestSchedulerHandleProjection:
    def test_vq_array_metadata_stays_off_the_scheduler_handle(
        self, state: Path
    ) -> None:
        spec = _scheduler_job("handlearray1")
        spec.array_index = 2
        spec.array_total = 5
        spec.array_group_id = "array-group"
        dispatcher = _FakeSchedulerDispatcher()

        handle = pause_resume._scheduler_handle(dispatcher, spec)

        assert (
            handle.job_id,
            handle.remote_workspace,
            handle.array_size,
        ) == ("555.cluster", "/remote/handlearray1", None)

    def test_missing_scheduler_id_raises_pause_error(self, state: Path) -> None:
        spec = _scheduler_job("handlenone01")
        spec.scheduler_job_id = None

        with pytest.raises(
            PauseError,
            match=(
                "job handlenone01 has no scheduler_job_id recorded; "
                "cannot control it"
            ),
        ):
            pause_resume._scheduler_handle(_FakeSchedulerDispatcher(), spec)

    def test_empty_scheduler_id_remains_a_handle_value(self, state: Path) -> None:
        spec = _scheduler_job("handleempty1")
        spec.scheduler_job_id = ""

        handle = pause_resume._scheduler_handle(
            _FakeSchedulerDispatcher(), spec
        )

        assert (
            handle.job_id,
            handle.remote_workspace,
            handle.array_size,
        ) == ("", "/remote/handleempty1", None)


class TestPauseResume:
    def test_pause_running_job_marks_suspended(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("aaaa11112222", state)
        try:
            msg = pause_job("localhost", spec.id)
            assert "paused" in msg
            assert f"pgid {spec.pgid}" in msg

            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.SUSPENDED
            assert recovered.paused_at is not None
            # Process should now be SIGSTOP'd. On Linux we can confirm
            # via /proc/<pid>/status "State:\tT (stopped)"; on macOS we
            # just trust that os.killpg(pgid, SIGSTOP) succeeded and the
            # spec write committed.
            time.sleep(0.05)
            try:
                with open(f"/proc/{proc.pid}/status") as f:
                    text = f.read()
                # T (stopped) is what we expect; T+ also possible
                assert "State:\tT" in text, (
                    f"expected stopped process; got:\n{text[:200]}"
                )
            except FileNotFoundError:
                # macOS: no /proc. Skip this kernel-level confirmation;
                # the spec mutation alone is the contract this test
                # cares about.
                pass
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_suspended_job_marks_running_and_accumulates(
        self, state: Path
    ) -> None:
        spec, proc = _running_job_with_real_process("bbbb11112222", state)
        try:
            pause_job("localhost", spec.id)
            time.sleep(0.2)  # so paused_seconds_total > 0
            msg = resume_job("localhost", spec.id)
            assert "resumed" in msg

            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.RUNNING
            assert recovered.paused_at is None
            assert recovered.paused_seconds_total >= 0.1
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_bills_monotonic_not_wallclock(self, state: Path) -> None:
        """PA-1: resume measures the pause interval from the monotonic anchor
        stamped at SIGSTOP, so a wall-clock step during the pause can't
        mis-bill paused_seconds_total (and thus the wall-time budget)."""
        spec, proc = _running_job_with_real_process("pa1mono00001", state)
        try:
            pause_job("localhost", spec.id)
            paused = JobSpec.read(paths.spec_path(spec.id))
            assert paused.paused_monotonic_at is not None  # PA-1 stamped it
            # Simulate a large backward wall-clock step during the pause:
            # paused_at now looks like the year 2020. The monotonic anchor is
            # untouched, so PA-1 must bill the real (sub-second) interval, not
            # the multi-year wall-clock diff.
            paused.paused_at = "2020-01-01T00:00:00+00:00"
            paused.write(paths.spec_path(spec.id))
            time.sleep(0.1)
            resume_job("localhost", spec.id)

            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.RUNNING
            assert recovered.paused_monotonic_at is None  # cleared on resume
            assert recovered.paused_seconds_total < 60, (
                "PA-1: billed the wall-clock diff (years) instead of monotonic"
            )
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_falls_back_to_wallclock_without_monotonic(
        self, state: Path
    ) -> None:
        """PA-1 back-compat: a spec paused before PA-1 shipped has no
        monotonic anchor on disk; resume falls back to the wall-clock diff."""
        spec, proc = _running_job_with_real_process("pa1fall00001", state)
        try:
            pause_job("localhost", spec.id)
            paused = JobSpec.read(paths.spec_path(spec.id))
            paused.paused_monotonic_at = None  # simulate a pre-PA-1 on-disk spec
            paused.write(paths.spec_path(spec.id))
            time.sleep(0.2)
            resume_job("localhost", spec.id)

            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.paused_seconds_total >= 0.1  # wall-clock fallback billed it
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_malformed_pause_anchor_fails_before_sigcont(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        workspace = paths.jobs_dir() / "badpauseclock"
        workspace.mkdir(parents=True)
        spec = JobSpec(
            id="badpauseclock",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.SUSPENDED,
            pid=1234,
            pgid=1234,
            paused_at="2026-08-26T00:00:00+00:00",
        )
        payload = spec.model_dump(mode="json")
        payload["paused_monotonic_at"] = -math.inf
        paths.spec_path(spec.id).write_text(json.dumps(payload))
        signals: list[tuple[int, signal.Signals]] = []
        monkeypatch.setattr(os, "killpg", lambda pgid, sig: signals.append((pgid, sig)))

        with pytest.raises(ValidationError, match="paused_monotonic_at"):
            resume_job("localhost", spec.id)

        assert signals == []

    def test_pause_already_suspended_errors(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("ccccdddd5555", state)
        try:
            pause_job("localhost", spec.id)
            with pytest.raises(PauseError, match="already suspended"):
                pause_job("localhost", spec.id)
        finally:
            _release_for_cleanup(proc, spec.pgid)  # so kill works
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_non_running_state_errors(self, state: Path) -> None:
        # Hand-write a PENDING spec; no process needed
        workspace = paths.jobs_dir() / "pending00001"
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id="pending00001",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.PENDING,
        )
        spec.write(paths.spec_path(spec.id))
        with pytest.raises(PauseError, match="only RUNNING jobs"):
            pause_job("localhost", spec.id)

    def test_pause_unknown_jobid(self, state: Path) -> None:
        with pytest.raises(FileNotFoundError, match="no such job"):
            pause_job("localhost", "noexist0001")

    def test_resume_non_suspended_errors(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("dddd55556666", state)
        try:
            with pytest.raises(PauseError, match="only SUSPENDED jobs"):
                resume_job("localhost", spec.id)
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_without_pgid_errors(self, state: Path) -> None:
        workspace = paths.jobs_dir() / "nopgid000001"
        workspace.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id="nopgid000001",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.RUNNING,
            pid=99999,
            # pgid intentionally omitted (None)
        )
        spec.write(paths.spec_path(spec.id))
        with pytest.raises(PauseError, match="no pgid recorded"):
            pause_job("localhost", spec.id)


class TestDurablePauseIntent:
    """Crash windows around SIGSTOP remain recoverable from disk alone."""

    def test_death_after_intent_before_sigstop_is_reconciled(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec, proc = _running_job_with_real_process("intentpre001", state)
        real_killpg = os.killpg

        def die_before_signal(pgid: int, sig: signal.Signals) -> None:
            assert pgid == spec.pgid
            assert sig == signal.SIGSTOP
            durable = JobSpec.read(paths.spec_path(spec.id))
            assert durable.state == JobState.RUNNING
            assert durable.pause_intent_at is not None
            assert durable.pause_intent_pgid == spec.pgid
            assert durable.pause_intent_by == "admin-update-test"
            raise SystemExit("simulated SIGKILL boundary")

        monkeypatch.setattr(pause_resume.os, "killpg", die_before_signal)
        try:
            with pytest.raises(SystemExit, match="SIGKILL boundary"):
                pause_job(
                    "localhost", spec.id, paused_by="admin-update-test",
                )

            pending = JobSpec.read(paths.spec_path(spec.id))
            assert pending.state == JobState.RUNNING
            assert pending.pause_intent_at is not None

            monkeypatch.setattr(pause_resume.os, "killpg", real_killpg)
            result = pause_resume.reconcile_pause_intents("localhost")
            assert result.success
            assert result.completed == (spec.id,)
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.SUSPENDED
            assert recovered.paused_by == "admin-update-test"
            assert recovered.pause_intent_at is None
            assert recovered.pause_intent_pgid is None
        finally:
            _release_for_cleanup(proc, spec.pgid, killpg=real_killpg)
            proc.kill()
            proc.wait(timeout=2)

    def test_death_after_sigstop_before_state_write_is_reconciled(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec, proc = _running_job_with_real_process("intentpost01", state)
        real_write = JobSpec.write
        failed = False

        def fail_final_write(self: JobSpec, path: Path) -> None:
            nonlocal failed
            if (
                not failed
                and self.state == JobState.SUSPENDED
                and self.pause_intent_at is None
            ):
                failed = True
                raise RuntimeError("simulated death before final spec write")
            real_write(self, path)

        monkeypatch.setattr(JobSpec, "write", fail_final_write)
        try:
            with pytest.raises(RuntimeError, match="final spec write"):
                pause_job("localhost", spec.id, paused_by="admin-post")

            pending = JobSpec.read(paths.spec_path(spec.id))
            assert pending.state == JobState.RUNNING
            assert pending.pause_intent_at is not None
            assert pending.pause_intent_by == "admin-post"

            monkeypatch.setattr(JobSpec, "write", real_write)
            result = pause_resume.reconcile_pause_intents("localhost")
            assert result.success
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.SUSPENDED
            assert recovered.paused_by == "admin-post"
            assert recovered.pause_intent_at is None
        finally:
            _release_for_cleanup(proc, spec.pgid)
            proc.kill()
            proc.wait(timeout=2)

    def test_gone_process_disarms_intent_without_false_suspended_state(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec, proc = _running_job_with_real_process("intentgone01", state)

        def gone(_pgid: int, _sig: signal.Signals) -> None:
            raise ProcessLookupError

        monkeypatch.setattr(pause_resume.os, "killpg", gone)
        try:
            with pytest.raises(PauseError, match="is gone"):
                pause_job("localhost", spec.id, paused_by="admin-gone")
            durable = JobSpec.read(paths.spec_path(spec.id))
            assert durable.state == JobState.RUNNING
            assert durable.pause_intent_at is None
            assert durable.pause_intent_by is None
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestResumeScopeProof:
    def test_read_only_absence_proof_never_reconciles_or_resumes(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            pause_resume,
            "reconcile_pause_intents",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("read-only proof reconciled pause intents")
            ),
        )
        monkeypatch.setattr(
            pause_resume,
            "resume_all",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                AssertionError("read-only proof attempted resume")
            ),
        )

        proof = pause_resume.prove_pause_token_absent(
            "localhost", "admin-orphan",
        )

        assert proof.proven_clear
        proof.require_clear()
        assert proof.summary.startswith("read-only pause-token scan")

    @pytest.mark.parametrize("evidence", ["paused-by", "pause-intent", "corrupt"])
    def test_read_only_absence_proof_retains_every_ambiguous_row(
        self,
        state: Path,
        evidence: str,
    ) -> None:
        token = "admin-orphan"
        workspace = paths.jobs_dir() / "orphanproof1"
        workspace.mkdir(parents=True)
        spec = JobSpec(
            id="orphanproof1",
            command=["true"],
            cwd=str(workspace),
            cpus=1,
            state=JobState.PENDING,
        )
        if evidence == "paused-by":
            spec.paused_by = token
            spec.write(paths.spec_path(spec.id))
        elif evidence == "pause-intent":
            spec.pause_intent_at = "2026-08-30T12:00:00+00:00"
            spec.pause_intent_monotonic_at = 1.0
            spec.pause_intent_pgid = 999_999
            spec.pause_intent_by = token
            spec.write(paths.spec_path(spec.id))
        else:
            paths.spec_path(spec.id).write_text("{", encoding="utf-8")

        proof = pause_resume.prove_pause_token_absent(
            "localhost", token,
        )

        assert not proof.proven_clear
        assert proof.unresolved[0][0] == spec.id
        with pytest.raises(PauseError, match="not durably proven"):
            proof.require_clear()

    def test_partial_resume_failure_is_not_proof_and_retry_clears(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        token = "admin-proof"
        spec1, proc1 = _running_job_with_real_process("proofresume1", state)
        spec2, proc2 = _running_job_with_real_process("proofresume2", state)
        real_resume_job = pause_resume.resume_job
        try:
            pause_job("localhost", spec1.id, paused_by=token)
            pause_job("localhost", spec2.id, paused_by=token)

            def fail_one(host: str, jobid: str, **kwargs: object) -> str:
                if jobid == spec2.id:
                    raise PauseError("injected SIGCONT failure")
                return real_resume_job(host, jobid, **kwargs)

            monkeypatch.setattr(pause_resume, "resume_job", fail_one)
            proof = pause_resume.resume_token_scope_with_proof(
                "localhost", token,
            )
            assert not proof.proven_clear
            assert any(jobid == spec2.id for jobid, _ in proof.unresolved)
            with pytest.raises(PauseError, match="not durably proven"):
                proof.require_clear()
            assert (
                JobSpec.read(paths.spec_path(spec2.id)).paused_by == token
            )

            monkeypatch.setattr(pause_resume, "resume_job", real_resume_job)
            retry = pause_resume.resume_token_scope_with_proof(
                "localhost", token,
            )
            assert retry.proven_clear
            retry.require_clear()
            assert (
                JobSpec.read(paths.spec_path(spec2.id)).state
                == JobState.RUNNING
            )
        finally:
            for proc in (proc1, proc2):
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
                proc.kill()
                proc.wait(timeout=2)

    def test_partial_surgical_pause_exception_is_found_by_token(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        token = "admin-surgical"
        spec1, proc1 = _running_with_branch("proofsurg001", "main", state)
        spec2, proc2 = _running_with_branch("proofsurg002", "main", state)
        real_pause_job = pause_resume.pause_job
        calls = 0

        def die_on_second(host: str, jobid: str, **kwargs: object) -> str:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected mid-scan death")
            return real_pause_job(host, jobid, **kwargs)

        monkeypatch.setattr(pause_resume, "pause_job", die_on_second)
        try:
            with pytest.raises(RuntimeError, match="mid-scan death"):
                pause_resume.pause_provides_branches(
                    "localhost", ["main"], paused_by=token,
                )
            first = JobSpec.read(paths.spec_path(spec1.id))
            second = JobSpec.read(paths.spec_path(spec2.id))
            assert first.state == JobState.SUSPENDED
            assert first.paused_by == token
            assert second.state == JobState.RUNNING

            proof = pause_resume.resume_token_scope_with_proof(
                "localhost", token,
            )
            assert proof.proven_clear
            assert (
                JobSpec.read(paths.spec_path(spec1.id)).state
                == JobState.RUNNING
            )
        finally:
            monkeypatch.setattr(pause_resume, "pause_job", real_pause_job)
            for proc in (proc1, proc2):
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
                proc.kill()
                proc.wait(timeout=2)


class TestPauseAdmissionProof:
    def test_bulk_pause_error_cannot_admit_environment_mutation(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        token = "admin-admission"
        spec1, proc1 = _running_job_with_real_process("proofpause01", state)
        spec2, proc2 = _running_job_with_real_process("proofpause02", state)
        real_pause_job = pause_resume.pause_job
        try:
            def fail_one(host: str, jobid: str, **kwargs: object) -> str:
                if jobid == spec2.id:
                    raise PauseError("injected SIGSTOP failure")
                return real_pause_job(host, jobid, **kwargs)

            monkeypatch.setattr(pause_resume, "pause_job", fail_one)
            proof = pause_resume.pause_token_scope_with_proof(
                "localhost", token,
            )
            assert not proof.proven_quiescent
            assert any(jobid == spec2.id for jobid, _ in proof.unresolved)
            with pytest.raises(PauseError, match="admission is not durably proven"):
                proof.require_quiescent()
            assert (
                JobSpec.read(paths.spec_path(spec1.id)).state
                == JobState.SUSPENDED
            )
            assert (
                JobSpec.read(paths.spec_path(spec2.id)).state
                == JobState.RUNNING
            )

            monkeypatch.setattr(pause_resume, "pause_job", real_pause_job)
            resume = pause_resume.resume_token_scope_with_proof(
                "localhost", token,
            )
            assert resume.proven_clear
        finally:
            for proc in (proc1, proc2):
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(proc.pid), signal.SIGCONT)
                proc.kill()
                proc.wait(timeout=2)

    def test_job_racing_to_running_after_capture_fails_admission(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec, proc = _running_job_with_real_process("proofrace001", state)
        pending = JobSpec.read(paths.spec_path(spec.id))
        pending.state = JobState.PENDING
        pending.write(paths.spec_path(spec.id))

        def race_dispatch(*args: object, **kwargs: object) -> str:
            del args, kwargs
            raced = JobSpec.read(paths.spec_path(spec.id))
            raced.state = JobState.RUNNING
            raced.write(paths.spec_path(spec.id))
            return "paused 0 jobs"

        monkeypatch.setattr(pause_resume, "pause_all", race_dispatch)
        try:
            proof = pause_resume.pause_token_scope_with_proof(
                "localhost", "admin-race",
            )
            assert not proof.proven_quiescent
            assert proof.unresolved == (
                (spec.id, "eligible job remains RUNNING after pause"),
            )
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestPauseResumeCLI:
    def test_pause_then_resume_via_cli(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("clitest00001", state)
        try:
            pause_result = CliRunner().invoke(main, ["pause", "localhost", spec.id])
            assert pause_result.exit_code == 0, pause_result.output
            assert "paused" in pause_result.output
            assert (
                JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED
            )
            time.sleep(0.1)

            resume_result = CliRunner().invoke(main, ["resume", "localhost", spec.id])
            assert resume_result.exit_code == 0, resume_result.output
            assert "resumed" in resume_result.output
            assert (
                JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING
            )
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_with_default_host(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        spec, proc = _running_job_with_real_process("clitest00002", state)
        try:
            r = CliRunner().invoke(main, ["pause", spec.id])
            assert r.exit_code == 0, r.output
            assert "paused" in r.output
        finally:
            _release_for_cleanup(proc, spec.pgid)
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_help_shows_both_forms(self) -> None:
        r = CliRunner().invoke(main, ["pause", "--help"])
        assert r.exit_code == 0
        assert "HOST JOBID" in r.output
        assert "default_host" in r.output

    def test_pause_cli_renders_ownership_denial_as_usage_error(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def deny(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise ownership.OwnershipError("foreign job denied")

        monkeypatch.setattr("vq.cli.pause_job", deny)

        result = CliRunner().invoke(
            main, ["pause", "localhost", "foreigndenied"]
        )

        assert result.exit_code == 2
        assert "Error: foreign job denied" in result.output
        assert not isinstance(result.exception, ownership.OwnershipError)

    def test_scheduler_pause_uses_qhold_for_queued_job(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedpause01")

        msg = pause_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
            paused_by="ops",
        )

        assert "qhold 555.cluster" in msg
        assert fake.held == ["555.cluster"]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.scheduler_state == "held"
        assert recovered.paused_at is not None
        assert recovered.paused_monotonic_at is not None
        assert recovered.paused_by == "ops"

    def test_scheduler_pause_refuses_live_running_phase(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(phase=SchedulerPhase.RUNNING)
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedrun001")

        with pytest.raises(PauseError, match="already running on scheduler"):
            pause_scheduler_job("host_f-scheduler-test", _scheduler_host_config(), spec.id)

        assert fake.held == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING

    def test_scheduler_pause_revalidates_after_poll_before_qhold(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job("schedpolldrf")

        def replace_scheduler_identity(handles: list[SchedulerHandle]) -> None:
            del handles
            changed = JobSpec.read(paths.spec_path(spec.id))
            changed.scheduler_job_id = "999.cluster"
            changed.write(paths.spec_path(spec.id))

        fake = _FakeSchedulerDispatcher(poll_hook=replace_scheduler_identity)
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )

        with pytest.raises(PauseError, match="changed before scheduler hold"):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert fake.held == []
        assert fake.released == []
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.scheduler_job_id == "999.cluster"
        assert recovered.state == JobState.RUNNING

    def test_scheduler_pause_apply_then_raise_runs_exact_inverse(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            hold_error=SchedulerError("qhold applied then transport failed")
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedholdapp")

        with pytest.raises(
            PauseError,
            match="applied then transport failed.*scheduler rollback completed",
        ):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert fake.held == ["555.cluster"]
        assert fake.released == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING

    def test_overlapping_scheduler_pauses_cannot_inverse_committed_hold(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _BlockingFirstSchedulerMutation("hold")
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedpauserace")
        successes: list[str] = []
        failures: list[BaseException] = []

        def invoke_pause() -> None:
            try:
                successes.append(
                    pause_scheduler_job(
                        "host_f-scheduler-test",
                        _scheduler_host_config(),
                        spec.id,
                    )
                )
            except BaseException as exc:
                failures.append(exc)

        first = threading.Thread(target=invoke_pause)
        second = threading.Thread(target=invoke_pause)
        first.start()
        assert fake.first_entered.wait(timeout=2)
        second.start()
        overlapped = fake.second_entered.wait(timeout=1)
        fake.allow_first_return.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert not first.is_alive()
        assert not second.is_alive()
        assert not overlapped
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], PauseError)
        assert fake.held == ["555.cluster"]
        assert fake.released == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED

    def test_scheduler_pause_pre_replace_failure_with_failed_qrls_marks_unknown(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            release_error=SchedulerError("qrls rollback failed")
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedevfail1")
        real_write = JobSpec.write

        def fail_before_replace(self: JobSpec, path: Path) -> None:
            if (
                self.id == spec.id
                and self.state == JobState.SUSPENDED
                and self.scheduler_state == "held"
            ):
                raise OSError("pre-replace spec failure")
            real_write(self, path)

        monkeypatch.setattr(JobSpec, "write", fail_before_replace)

        with pytest.raises(PauseError) as exc_info:
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        message = str(exc_info.value)
        assert "pre-replace spec failure" in message
        assert "qrls rollback failed" in message
        assert "outcome unknown" in message
        assert "hold_outcome_unknown" in message
        assert "rollback completed" not in message
        assert fake.released == ["555.cluster"]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.scheduler_state == "hold_outcome_unknown"
        assert recovered.paused_at is not None
        assert recovered.paused_monotonic_at is not None

    def test_scheduler_pause_apply_then_raise_and_failed_inverse_marks_unknown(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            hold_error=SchedulerError("qhold applied then transport failed"),
            release_error=SchedulerError("qrls inverse outcome unknown"),
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedholdunk")

        with pytest.raises(PauseError) as exc_info:
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by="ops",
            )

        message = str(exc_info.value)
        assert "qhold applied then transport failed" in message
        assert "qrls inverse outcome unknown" in message
        assert "outcome unknown" in message
        assert f"vq resume host_f-scheduler-test {spec.id}" in message
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.scheduler_state == "hold_outcome_unknown"
        assert recovered.paused_by == "ops"

        # SUSPENDED/hold_outcome_unknown routes the next ordinary resume
        # through the exact qrls inverse rather than another qhold.
        fake.hold_error = None
        fake.release_error = None
        recovery = resume_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
            paused_by_filter="ops",
        )
        assert "released scheduler job" in recovery
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.RUNNING
        assert recovered.scheduler_state == "queued"

    def test_scheduler_pause_post_write_failure_restores_exact_spec(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedwrfail1")
        before = JobSpec.read(paths.spec_path(spec.id))
        real_write = JobSpec.write

        def write_then_fail(self: JobSpec, path: Path) -> None:
            real_write(self, path)
            if self.id == spec.id and self.state == JobState.SUSPENDED:
                raise OSError("post-replace spec failure")

        monkeypatch.setattr(JobSpec, "write", write_then_fail)

        with pytest.raises(
            PauseError,
            match="post-replace spec failure.*scheduler rollback completed",
        ):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert fake.held == ["555.cluster"]
        assert fake.released == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(spec.id)) == before

    def test_scheduler_pause_restore_retries_and_verifies_exact_pre_state(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedrestpa1")
        spec_path = paths.spec_path(spec.id)
        before = JobSpec.read(spec_path)
        real_write = JobSpec.write
        write_attempts = 0
        faults_enabled = True

        def staged_write_failure(self: JobSpec, path: Path) -> None:
            nonlocal write_attempts
            if self.id != spec.id or not faults_enabled:
                real_write(self, path)
                return
            write_attempts += 1
            if write_attempts == 1:
                real_write(self, path)
                raise OSError("forward write replaced then raised")
            if write_attempts == 2:
                raise OSError("first restore failed before replace")
            if write_attempts == 3:
                real_write(self, path)
                raise OSError("retry restore replaced then raised")
            real_write(self, path)

        monkeypatch.setattr(JobSpec, "write", staged_write_failure)

        with pytest.raises(
            PauseError,
            match="forward write replaced then raised.*rollback completed",
        ):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert write_attempts == 3
        assert JobSpec.read(spec_path) == before
        assert spec_path.read_text() == before.to_json()

        # The verified pre-state is ordinary and safe to operate on again.
        faults_enabled = False
        recovered = pause_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
        )
        assert "held scheduler job" in recovered
        assert JobSpec.read(spec_path).state == JobState.SUSPENDED

    def test_scheduler_pause_restore_stops_when_policy_revoked_before_retry(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedrestdeny")
        spec_path = paths.spec_path(spec.id)
        real_write = JobSpec.write
        restore_attempts = 0
        restore_failed = False
        post_bytes: bytes | None = None

        def staged_write_failure(self: JobSpec, path: Path) -> None:
            nonlocal post_bytes, restore_attempts, restore_failed
            if self.id != spec.id:
                real_write(self, path)
                return
            if self.state == JobState.SUSPENDED:
                real_write(self, path)
                post_bytes = path.read_bytes()
                raise OSError("forward write replaced then raised")
            restore_attempts += 1
            restore_failed = True
            raise OSError("restore failed before replace")

        def revoke_after_restore_failure(
            checked: JobSpec,
            *,
            cfg: config.Config | None = None,
            multi_user: bool = False,
        ) -> None:
            del checked, cfg, multi_user
            if restore_failed:
                raise ownership.OwnershipError(
                    "policy revoked before restore retry"
                )

        monkeypatch.setattr(JobSpec, "write", staged_write_failure)
        monkeypatch.setattr(
            ownership,
            "check_owner",
            revoke_after_restore_failure,
        )

        with pytest.raises(PauseError) as exc_info:
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        message = str(exc_info.value)
        assert "forward write replaced then raised" in message
        assert "policy revoked before restore retry" in message
        assert "outcome unknown" in message
        assert restore_attempts == 1
        assert post_bytes is not None
        assert spec_path.read_bytes() == post_bytes
        recovered = JobSpec.read(spec_path)
        assert recovered.state == JobState.SUSPENDED
        assert recovered.scheduler_state == "held"
        assert fake.released == ["555.cluster"]

    def test_scheduler_pause_event_append_then_raise_keeps_commit(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedevcommit")
        real_append = pause_resume.events.append_event

        def append_then_raise(*args, **kwargs):  # type: ignore[no-untyped-def]
            real_append(*args, **kwargs)
            raise RuntimeError("event reported then raised")

        monkeypatch.setattr(
            "vq.pause_resume.events.append_event",
            append_then_raise,
        )

        message = pause_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
        )

        assert "held scheduler job" in message
        assert fake.held == ["555.cluster"]
        assert fake.released == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED
        recorded = pause_resume.events.read_events(Path(spec.cwd))
        assert len(recorded) == 1

    def test_scheduler_pause_final_owner_recheck_failure_runs_inverse(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedownerd3")
        real_spec_lock = paths.spec_lock
        depth = 0
        owner_checks = 0

        @contextlib.contextmanager
        def tracked_spec_lock(spec_path: Path):
            nonlocal depth
            with real_spec_lock(spec_path):
                depth += 1
                try:
                    yield
                finally:
                    depth -= 1

        def check_owner(
            checked: JobSpec,
            *,
            cfg: config.Config | None = None,
            multi_user: bool = False,
        ) -> None:
            nonlocal owner_checks
            del checked, cfg, multi_user
            assert depth > 0
            owner_checks += 1
            if owner_checks == 3:
                raise ownership.OwnershipError("policy changed after qhold")

        monkeypatch.setattr(paths, "spec_lock", tracked_spec_lock)
        monkeypatch.setattr(ownership, "check_owner", check_owner)

        with pytest.raises(
            ownership.OwnershipError,
            match="policy changed after qhold",
        ):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert owner_checks == 3
        assert fake.held == ["555.cluster"]
        assert fake.released == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING

    def test_scheduler_pause_denies_foreign_job_under_lock_before_dispatch(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("scheddenypa1")
        _install_owner_gate_spy(monkeypatch, denied_jobids={spec.id})

        with pytest.raises(ownership.OwnershipError, match="foreign"):
            pause_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert fake.polled == []
        assert fake.held == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING

    def test_scheduler_resume_uses_qrls_and_accounts_hold_time(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedresume1",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )

        msg = resume_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
            paused_by_filter="ops",
        )

        assert "qrls 555.cluster" in msg
        assert fake.released == ["555.cluster"]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.RUNNING
        assert recovered.scheduler_state == "queued"
        assert recovered.paused_by is None
        assert recovered.paused_at is None
        assert recovered.paused_monotonic_at is None
        assert recovered.paused_seconds_total > 0

    def test_malformed_scheduler_pause_anchor_fails_before_release(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "badschclock1",
            state=JobState.SUSPENDED,
            scheduler_state="held",
        )
        payload = spec.model_dump(mode="json")
        payload["paused_monotonic_at"] = math.nan
        paths.spec_path(spec.id).write_text(json.dumps(payload))

        with pytest.raises(ValidationError, match="paused_monotonic_at"):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
            )

        assert fake.released == []

    def test_scheduler_resume_revalidates_filter_before_qrls(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job(
            "schedprefilt",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )
        real_handle = pause_resume._scheduler_handle
        handle_calls = 0

        def handle_then_drift(
            dispatcher: _FakeSchedulerDispatcher,
            checked: JobSpec,
        ) -> SchedulerHandle:
            nonlocal handle_calls
            handle_calls += 1
            handle = real_handle(dispatcher, checked)
            if handle_calls == 1:
                changed = JobSpec.read(paths.spec_path(spec.id))
                changed.paused_by = "manual"
                changed.write(paths.spec_path(spec.id))
            return handle

        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        monkeypatch.setattr(
            "vq.pause_resume._scheduler_handle",
            handle_then_drift,
        )

        with pytest.raises(PauseError, match="does not match"):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        assert fake.released == []
        assert fake.held == []
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.paused_by == "manual"

    def test_scheduler_resume_apply_then_raise_runs_exact_inverse(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            release_error=SchedulerError("qrls applied then transport failed")
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedrlsapply",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )

        with pytest.raises(
            PauseError,
            match="applied then transport failed.*scheduler rollback completed",
        ):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        assert fake.released == ["555.cluster"]
        assert fake.held == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED

    def test_overlapping_scheduler_resumes_cannot_inverse_committed_release(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _BlockingFirstSchedulerMutation("release")
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedresurace",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )
        successes: list[str] = []
        failures: list[BaseException] = []

        def invoke_resume() -> None:
            try:
                successes.append(
                    resume_scheduler_job(
                        "host_f-scheduler-test",
                        _scheduler_host_config(),
                        spec.id,
                        paused_by_filter="ops",
                    )
                )
            except BaseException as exc:
                failures.append(exc)

        first = threading.Thread(target=invoke_resume)
        second = threading.Thread(target=invoke_resume)
        first.start()
        assert fake.first_entered.wait(timeout=2)
        second.start()
        overlapped = fake.second_entered.wait(timeout=1)
        fake.allow_first_return.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert not first.is_alive()
        assert not second.is_alive()
        assert not overlapped
        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], PauseError)
        assert fake.released == ["555.cluster"]
        assert fake.held == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING

    def test_scheduler_resume_rechecks_paused_by_before_commit(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job(
            "schedtagdrft",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )

        def replace_pause_owner(handle: SchedulerHandle) -> None:
            del handle
            changed = JobSpec.read(paths.spec_path(spec.id))
            changed.paused_by = "manual"
            changed.write(paths.spec_path(spec.id))

        fake = _FakeSchedulerDispatcher(release_hook=replace_pause_owner)
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )

        with pytest.raises(
            PauseError,
            match="does not match.*scheduler rollback completed",
        ):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        assert fake.released == ["555.cluster"]
        assert fake.held == ["555.cluster"]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.paused_by == "manual"

    def test_scheduler_resume_pre_replace_failure_with_failed_qhold_marks_unknown(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            hold_error=SchedulerError("qhold rollback failed")
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedrevfail",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )
        real_write = JobSpec.write

        def fail_before_replace(self: JobSpec, path: Path) -> None:
            if (
                self.id == spec.id
                and self.state == JobState.RUNNING
                and self.scheduler_state == "queued"
            ):
                raise OSError("pre-replace spec failure")
            real_write(self, path)

        monkeypatch.setattr(JobSpec, "write", fail_before_replace)

        with pytest.raises(PauseError) as exc_info:
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        message = str(exc_info.value)
        assert "pre-replace spec failure" in message
        assert "qhold rollback failed" in message
        assert "outcome unknown" in message
        assert "release_outcome_unknown" in message
        assert "rollback completed" not in message
        assert fake.held == ["555.cluster"]
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.RUNNING
        assert recovered.scheduler_state == "release_outcome_unknown"
        assert recovered.paused_by is None

    def test_scheduler_resume_restore_retries_and_verifies_exact_pre_state(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedrestrs1",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )
        spec_path = paths.spec_path(spec.id)
        before = JobSpec.read(spec_path)
        real_write = JobSpec.write
        write_attempts = 0
        faults_enabled = True

        def staged_write_failure(self: JobSpec, path: Path) -> None:
            nonlocal write_attempts
            if self.id != spec.id or not faults_enabled:
                real_write(self, path)
                return
            write_attempts += 1
            if write_attempts == 1:
                real_write(self, path)
                raise OSError("forward write replaced then raised")
            if write_attempts == 2:
                raise OSError("first restore failed before replace")
            if write_attempts == 3:
                real_write(self, path)
                raise OSError("retry restore replaced then raised")
            real_write(self, path)

        monkeypatch.setattr(JobSpec, "write", staged_write_failure)

        with pytest.raises(
            PauseError,
            match="forward write replaced then raised.*rollback completed",
        ):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        assert write_attempts == 3
        assert JobSpec.read(spec_path) == before
        assert spec_path.read_text() == before.to_json()

        # The verified pre-state is ordinary and safe to operate on again.
        faults_enabled = False
        recovered = resume_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
            paused_by_filter="ops",
        )
        assert "released scheduler job" in recovered
        assert JobSpec.read(spec_path).state == JobState.RUNNING

    def test_scheduler_resume_apply_then_raise_and_failed_inverse_marks_unknown(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher(
            release_error=KeyboardInterrupt("async after qrls applied"),
            hold_error=SchedulerError("qhold inverse outcome unknown"),
        )
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedrlsunkn",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )

        with pytest.raises(PauseError) as exc_info:
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="ops",
            )

        message = str(exc_info.value)
        assert "async after qrls applied" in message
        assert "qhold inverse outcome unknown" in message
        assert "outcome unknown" in message
        assert f"vq pause host_f-scheduler-test {spec.id}" in message
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.RUNNING
        assert recovered.scheduler_state == "release_outcome_unknown"
        assert recovered.paused_by is None

        # RUNNING/release_outcome_unknown routes the next ordinary pause
        # through the exact qhold inverse rather than another qrls.
        fake.release_error = None
        fake.hold_error = None
        recovery = pause_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
        )
        assert "held scheduler job" in recovery
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.scheduler_state == "held"

    def test_scheduler_resume_event_append_then_raise_keeps_commit(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "schedrevcommit",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="ops",
        )
        real_append = pause_resume.events.append_event

        def append_then_raise(*args, **kwargs):  # type: ignore[no-untyped-def]
            real_append(*args, **kwargs)
            raise RuntimeError("event reported then raised")

        monkeypatch.setattr(
            "vq.pause_resume.events.append_event",
            append_then_raise,
        )

        message = resume_scheduler_job(
            "host_f-scheduler-test",
            _scheduler_host_config(),
            spec.id,
            paused_by_filter="ops",
        )

        assert "released scheduler job" in message
        assert fake.released == ["555.cluster"]
        assert fake.held == []
        assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING
        recorded = pause_resume.events.read_events(Path(spec.cwd))
        assert len(recorded) == 1

    def test_scheduler_resume_denies_foreign_job_under_lock_before_dispatch(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job(
            "scheddenyrs1",
            state=JobState.SUSPENDED,
            scheduler_state="held",
            paused_by="private-tag",
        )
        _install_owner_gate_spy(monkeypatch, denied_jobids={spec.id})

        with pytest.raises(ownership.OwnershipError, match="foreign"):
            resume_scheduler_job(
                "host_f-scheduler-test",
                _scheduler_host_config(),
                spec.id,
                paused_by_filter="different-tag",
            )

        assert fake.released == []
        recovered = JobSpec.read(paths.spec_path(spec.id))
        assert recovered.state == JobState.SUSPENDED
        assert recovered.paused_by == "private-tag"

    def test_scheduler_pause_all_isolates_foreign_state_under_lock(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        owned = _scheduler_job("schedownpa01")
        foreign = _scheduler_job(
            "schedforpa01",
            state=JobState.SUSPENDED,
            scheduler_state="held",
        )
        _install_owner_gate_spy(monkeypatch, denied_jobids={foreign.id})

        summary = pause_resume.pause_scheduler_all(
            "host_f-scheduler-test",
            _scheduler_host_config(),
        )

        assert summary == "paused 1 job (1 error(s))"
        assert "already suspended" not in summary
        assert fake.held == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(owned.id)).state == JobState.SUSPENDED

    def test_scheduler_resume_all_isolates_foreign_state_under_lock(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        owned = _scheduler_job(
            "schedownrs01",
            state=JobState.SUSPENDED,
            scheduler_state="held",
        )
        foreign = _scheduler_job("schedforrs01")
        _install_owner_gate_spy(monkeypatch, denied_jobids={foreign.id})

        summary = pause_resume.resume_scheduler_all(
            "host_f-scheduler-test",
            _scheduler_host_config(),
        )

        assert summary == "resumed 1 job (1 error(s))"
        assert "already running" not in summary
        assert fake.released == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(owned.id)).state == JobState.RUNNING

    def test_cli_scheduler_pause_resume_local_driver(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_scheduler_cli_config(state)
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        spec = _scheduler_job("schedcli0001")

        pause_result = CliRunner().invoke(
            main,
            ["pause", "host_f-scheduler-test", spec.id, "--paused-by", "ops"],
        )
        assert pause_result.exit_code == 0, pause_result.output
        assert "held scheduler job" in pause_result.output
        assert fake.held == ["555.cluster"]

        resume_result = CliRunner().invoke(
            main,
            ["resume", "host_f-scheduler-test", spec.id, "--paused-by", "ops"],
        )
        assert resume_result.exit_code == 0, resume_result.output
        assert "released scheduler job" in resume_result.output
        assert fake.released == ["555.cluster"]

    def test_cli_scheduler_pause_all_filters_to_target(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_scheduler_cli_config(state)
        fake = _FakeSchedulerDispatcher()
        monkeypatch.setattr(
            "vq.pause_resume.scheduler_dispatcher_for",
            lambda host_cfg: fake,
        )
        target = _scheduler_job("schedall0001")
        _scheduler_job("othercluster1", scheduler_target="other-cluster")

        result = CliRunner().invoke(
            main,
            ["pause", "host_f-scheduler-test", "--all", "--paused-by", "batch"],
        )

        assert result.exit_code == 0, result.output
        assert "paused 1 job" in result.output
        assert fake.held == ["555.cluster"]
        assert JobSpec.read(paths.spec_path(target.id)).state == JobState.SUSPENDED
        assert JobSpec.read(paths.spec_path("othercluster1")).state == JobState.RUNNING

    def test_scheduler_pause_remote_driver_delegates(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_scheduler_cli_config(state, driver="driver")
        captured: dict[str, object] = {}

        def fake_delegate(host, cfg, *args, stdin_data=None):
            captured["host"] = host
            captured["args"] = args
            return "held remotely\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)

        result = CliRunner().invoke(
            main,
            ["pause", "host_f-scheduler-test", "abc123def456"],
        )

        assert result.exit_code == 0, result.output
        assert result.output == "held remotely\n"
        assert captured == {
            "host": "driver",
            "args": ("pause", "host_f-scheduler-test", "abc123def456"),
        }

    def test_kill_handles_suspended_job(self, state: Path) -> None:
        """SUSPENDED job: vq kill must SIGCONT first so SIGTERM can be
        delivered, otherwise the process would stay frozen and the kill
        would only update the spec without actually ending the process."""
        spec, proc = _running_job_with_real_process("killsus000001", state)
        try:
            pause_job("localhost", spec.id)
            time.sleep(0.1)
            r = CliRunner().invoke(main, ["kill", "localhost", spec.id])
            assert r.exit_code == 0, r.output
            assert "killed suspended job" in r.output
            # Process should now be exiting
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pytest.fail("SIGCONT+SIGTERM did not actually terminate the process")
            killed = JobSpec.read(paths.spec_path(spec.id))
            assert killed.state == JobState.KILLED
            assert killed.paused_seconds_total >= 0.1
            assert killed.paused_at is None
            assert killed.paused_monotonic_at is None
        finally:
            try:
                proc.kill()
                proc.wait(timeout=1)
            except Exception:
                pass


class TestPauseResumeAll:
    """v0.5.2: --all suspends/resumes the entire queue. Mixed-state
    queues are handled idempotently: jobs in the wrong state are
    skipped (not an error), counted in the summary line."""

    def test_pause_all_with_three_running(self, state: Path) -> None:
        from vq.pause_resume import pause_all
        procs: list[subprocess.Popen] = []
        try:
            for jobid in ("aaaaall000001", "aaaaall000002", "aaaaall000003"):
                _, proc = _running_job_with_real_process(jobid, state)
                procs.append(proc)
            msg = pause_all("localhost")
            assert "paused 3 jobs" in msg
            for jobid in ("aaaaall000001", "aaaaall000002", "aaaaall000003"):
                assert (
                    JobSpec.read(paths.spec_path(jobid)).state == JobState.SUSPENDED
                )
        finally:
            for p in procs:
                p.kill()
                p.wait(timeout=2)

    def test_pause_all_skips_non_running_states(self, state: Path) -> None:
        from vq.pause_resume import pause_all
        # 1 RUNNING (real process), 1 PENDING (no process), 1 COMPLETED
        spec, proc = _running_job_with_real_process("mixedstate01", state)
        try:
            for jid, st in [
                ("pendingmix01", JobState.PENDING),
                ("completedmx1", JobState.COMPLETED),
            ]:
                ws = paths.jobs_dir() / jid
                ws.mkdir(parents=True, exist_ok=True)
                JobSpec(
                    id=jid, command=["true"], cwd=str(ws), cpus=1, state=st,
                ).write(paths.spec_path(jid))
            msg = pause_all("localhost")
            # Only the 1 RUNNING was paused; pending and completed are
            # "not RUNNING" and folded into the skipped count.
            assert "paused 1 job" in msg
            assert "not RUNNING" in msg
            assert (
                JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED
            )
            assert (
                JobSpec.read(paths.spec_path("pendingmix01")).state == JobState.PENDING
            )
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_all_skips_already_suspended(self, state: Path) -> None:
        from vq.pause_resume import pause_all, pause_job
        spec, proc = _running_job_with_real_process("alreadysus01", state)
        try:
            pause_job("localhost", spec.id)
            # Now pause_all sees a SUSPENDED job; it should report 0 paused
            # + 1 already-suspended.
            msg = pause_all("localhost")
            assert "paused 0 jobs" in msg
            assert "already suspended" in msg
        finally:
            _release_for_cleanup(proc, spec.pgid)
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_all_empty_queue(self, state: Path) -> None:
        from vq.pause_resume import pause_all
        msg = pause_all("localhost")
        assert "paused 0 jobs" in msg

    def test_resume_all_brings_back_suspended_jobs(self, state: Path) -> None:
        from vq.pause_resume import pause_job, resume_all
        procs: list[subprocess.Popen] = []
        specs: list[JobSpec] = []
        try:
            for jobid in ("resumeall001", "resumeall002"):
                spec, proc = _running_job_with_real_process(jobid, state)
                procs.append(proc)
                specs.append(spec)
                pause_job("localhost", jobid)
            msg = resume_all("localhost")
            assert "resumed 2 jobs" in msg
            for spec in specs:
                assert (
                    JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING
                )
        finally:
            for p in procs:
                p.kill()
                p.wait(timeout=2)


class TestPauseResumeAllCLI:
    def test_cli_pause_all_works(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("clipallx0001", state)
        try:
            r = CliRunner().invoke(main, ["pause", "localhost", "--all"])
            assert r.exit_code == 0, r.output
            assert "paused 1 job" in r.output
            assert (
                JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED
            )
        finally:
            _release_for_cleanup(proc, spec.pgid)
            proc.kill()
            proc.wait(timeout=2)

    def test_cli_pause_all_with_default_host(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text('default_host = "localhost"\n')
        spec, proc = _running_job_with_real_process("clipallx0002", state)
        try:
            r = CliRunner().invoke(main, ["pause", "--all"])
            assert r.exit_code == 0, r.output
            assert "paused 1 job" in r.output
        finally:
            _release_for_cleanup(proc, spec.pgid)
            proc.kill()
            proc.wait(timeout=2)

    def test_cli_pause_all_with_jobid_rejected(self, state: Path) -> None:
        # `vq pause --all somejobid` is ambiguous; reject explicitly
        r = CliRunner().invoke(
            main, ["pause", "localhost", "--all", "somejobid01"]
        )
        assert r.exit_code != 0
        assert "mutually exclusive" in r.output

    def test_cli_pause_no_jobid_no_all_errors(self, state: Path) -> None:
        # No host, no jobid, no --all -> useful error message
        r = CliRunner().invoke(main, ["pause"])
        assert r.exit_code != 0
        assert "missing JOBID" in r.output

    def test_cli_resume_all_works(self, state: Path) -> None:
        from vq.pause_resume import pause_job
        spec, proc = _running_job_with_real_process("clirallx0001", state)
        try:
            pause_job("localhost", spec.id)
            r = CliRunner().invoke(main, ["resume", "localhost", "--all"])
            assert r.exit_code == 0, r.output
            assert "resumed 1 job" in r.output
            assert (
                JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING
            )
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestWatchdogSuspendedHandling:
    """SUSPENDED jobs: the watchdog must skip every kill path (otherwise
    a paused job could get TIME_EXCEEDED'd or STARVED-killed for being
    paused, defeating the purpose)."""

    def test_suspended_returns_ok_immediately(
        self, tmp_path: Path
    ) -> None:
        from vq.watchdog import Watchdog, WatchdogAction
        wd = Watchdog(interval_seconds=0.0)
        spec = JobSpec(
            id="susp00000001",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.SUSPENDED,
            wall_time_seconds=1,  # would normally trip
            paused_at="2026-05-09T12:00:00+00:00",
        )
        # Make wall_time look exceeded
        wd.register("susp00000001", started_monotonic=time.monotonic() - 10)
        v = wd.evaluate("susp00000001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_paused_seconds_total_subtracted_from_wall_clock(
        self, tmp_path: Path
    ) -> None:
        """A RUNNING job that was previously paused for 5 s with a
        wall_time_seconds=10 budget and an actual elapsed of 9 s
        (= 4 s active + 5 s paused) should NOT trip TIME_EXCEEDED."""
        from vq.watchdog import Watchdog, WatchdogAction
        wd = Watchdog(interval_seconds=0.0)
        spec = JobSpec(
            id="paused0000001",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.RUNNING,
            wall_time_seconds=10,
            paused_seconds_total=5.0,
        )
        wd.register("paused0000001", started_monotonic=time.monotonic() - 9)
        v = wd.evaluate("paused0000001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.OK

    def test_paused_seconds_does_not_save_a_truly_overran_job(
        self, tmp_path: Path
    ) -> None:
        """Same setup but the active wall time IS over budget: TIME_EXCEEDED
        still fires. Pause accounting subtracts paused time, doesn't grant
        free wall-clock."""
        from vq.watchdog import Watchdog, WatchdogAction
        wd = Watchdog(interval_seconds=0.0)
        spec = JobSpec(
            id="overran0000001",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            state=JobState.RUNNING,
            wall_time_seconds=10,
            paused_seconds_total=2.0,  # only 2 s paused
        )
        wd.register("overran0000001", started_monotonic=time.monotonic() - 15)
        # active = 15 - 2 = 13 > 10 -> TIME_EXCEEDED
        v = wd.evaluate("overran0000001", pid=1, pgid=1, spec=spec)
        assert v.action == WatchdogAction.SIGTERM
        assert v.terminal_state == JobState.TIME_EXCEEDED


# ----------------------------------------------------------------------
# v0.5.47: surgical pause_provides_branches + resume_jobs
# ----------------------------------------------------------------------


def _running_with_branch(
    jobid: str, branch: str | None, state_dir: Path,
) -> tuple[JobSpec, subprocess.Popen]:
    """Variant of _running_job_with_real_process that stamps a branch
    tag on the spec — fixture for the surgical-pause tests."""
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    proc = subprocess.Popen(["sleep", "30"], start_new_session=True)
    pgid = os.getpgid(proc.pid)
    spec = JobSpec(
        id=jobid,
        command=["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        pid=proc.pid,
        pgid=pgid,
        started_at="2026-05-17T12:00:00+00:00",
        branch=branch,
    )
    spec.write(paths.spec_path(jobid))
    return spec, proc


class TestPauseProvidesBranches:
    """v0.5.47: pause_provides_branches filters by spec.branch and
    returns the paused jobid list for symmetric resume."""

    def test_pauses_only_matching_branches(self, state: Path) -> None:
        from vq.pause_resume import pause_provides_branches
        procs: list[subprocess.Popen] = []
        try:
            spec_main, proc_main = _running_with_branch(
                "surgical0001", "main", state,
            )
            procs.append(proc_main)
            spec_rel, proc_rel = _running_with_branch(
                "surgical0002", "release", state,
            )
            procs.append(proc_rel)
            spec_untagged, proc_untagged = _running_with_branch(
                "surgical0003", None, state,
            )
            procs.append(proc_untagged)

            summary, paused_ids = pause_provides_branches(
                "localhost", ["main", "dev"],
            )
            assert "paused 1 job" in summary
            assert "branches=['dev', 'main']" in summary
            assert paused_ids == ["surgical0001"]
            # main paused, release + untagged still running
            assert JobSpec.read(paths.spec_path("surgical0001")).state == JobState.SUSPENDED
            assert JobSpec.read(paths.spec_path("surgical0002")).state == JobState.RUNNING
            assert JobSpec.read(paths.spec_path("surgical0003")).state == JobState.RUNNING
        finally:
            for p in procs:
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(p.pid), signal.SIGCONT)
                p.kill()
                p.wait(timeout=2)

    def test_pauses_alias_via_provides_branches_list(
        self, state: Path,
    ) -> None:
        """When the env's provides_branches lists the alias name
        ("dev" → main), a job submitted with --branch dev (stored
        verbatim on spec.branch) gets paused."""
        from vq.pause_resume import pause_provides_branches
        spec, proc = _running_with_branch("aliasjob0001", "dev", state)
        try:
            summary, paused_ids = pause_provides_branches(
                "localhost", ["main", "dev", "development"],
            )
            assert "paused 1 job" in summary
            assert paused_ids == ["aliasjob0001"]
        finally:
            with contextlib.suppress(ProcessLookupError, OSError):
                os.killpg(spec.pgid, signal.SIGCONT)
            proc.kill()
            proc.wait(timeout=2)

    def test_empty_branches_list_is_zero_pause(self, state: Path) -> None:
        from vq.pause_resume import pause_provides_branches
        spec, proc = _running_with_branch("nopause00001", "main", state)
        try:
            summary, paused_ids = pause_provides_branches("localhost", [])
            assert "paused 0 jobs" in summary
            assert "no branches to match" in summary
            assert paused_ids == []
            # Spec untouched.
            assert JobSpec.read(paths.spec_path("nopause00001")).state == JobState.RUNNING
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestResumeJobs:
    """v0.5.47: resume_jobs targets an explicit jobid list (the symmetric
    counterpart of pause_provides_branches's return value)."""

    def test_resumes_only_listed_jobs(self, state: Path) -> None:
        from vq.pause_resume import pause_job, resume_jobs
        spec1, proc1 = _running_with_branch("listed00001", "main", state)
        spec2, proc2 = _running_with_branch("listed00002", "main", state)
        spec_other, proc_other = _running_with_branch(
            "untouched001", "main", state,
        )
        try:
            pause_job("localhost", spec1.id)
            pause_job("localhost", spec2.id)
            pause_job("localhost", spec_other.id)
            # Resume only the first two — third stays suspended.
            summary = resume_jobs("localhost", [spec1.id, spec2.id])
            assert "resumed 2 jobs" in summary
            assert JobSpec.read(paths.spec_path(spec1.id)).state == JobState.RUNNING
            assert JobSpec.read(paths.spec_path(spec2.id)).state == JobState.RUNNING
            assert JobSpec.read(paths.spec_path(spec_other.id)).state == JobState.SUSPENDED
        finally:
            for p in (proc1, proc2, proc_other):
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(p.pid), signal.SIGCONT)
                p.kill()
                p.wait(timeout=2)

    def test_empty_list_is_zero_resume(self, state: Path) -> None:
        from vq.pause_resume import resume_jobs
        summary = resume_jobs("localhost", [])
        assert "resumed 0 jobs" in summary
        assert "nothing to resume" in summary

    def test_missing_jobid_recorded_as_error(self, state: Path) -> None:
        from vq.pause_resume import resume_jobs
        summary = resume_jobs("localhost", ["doesnotexist"])
        assert "resumed 0 jobs" in summary
        assert "error" in summary.lower()


# ----------------------------------------------------------------------
# v0.6.22: paused_by tag
# ----------------------------------------------------------------------


class TestPausedByTag:
    """v0.6.22: --paused-by TAG records the actor on the spec; the
    field clears on resume; resume --paused-by FILTER scopes the
    resume to matching specs."""

    def test_pause_job_records_paused_by(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("tag00000pj01", state)
        try:
            pause_job("localhost", spec.id, paused_by="update-script")
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.SUSPENDED
            assert recovered.paused_by == "update-script"
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_pause_job_none_paused_by_leaves_field_none(
        self, state: Path
    ) -> None:
        """Backwards-compat: pause without the tag leaves paused_by None,
        matching pre-v0.6.22 behavior."""
        spec, proc = _running_job_with_real_process("tag00000pj02", state)
        try:
            pause_job("localhost", spec.id)
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.paused_by is None
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_job_clears_paused_by(self, state: Path) -> None:
        spec, proc = _running_job_with_real_process("tag00000rj01", state)
        try:
            pause_job("localhost", spec.id, paused_by="update-script")
            resume_job("localhost", spec.id)
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.state == JobState.RUNNING
            assert recovered.paused_by is None
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_invalid_paused_by_charset_rejected(self, state: Path) -> None:
        """Validator rejects spaces / slashes (same charset as job_name)."""
        spec, proc = _running_job_with_real_process("tag00000bc01", state)
        try:
            with pytest.raises(ValueError, match="invalid paused_by"):
                pause_job("localhost", spec.id, paused_by="has space")
            recovered = JobSpec.read(paths.spec_path(spec.id))
            # Pause should have been aborted before SIGSTOP — spec
            # still RUNNING.
            assert recovered.state == JobState.RUNNING
        finally:
            proc.kill()
            proc.wait(timeout=2)


class TestPauseAllWithTag:
    """pause_all forwards --paused-by to each pause_job; resume_all
    paused_by_filter scopes the resume."""

    def test_pause_all_tags_every_paused_job(self, state: Path) -> None:
        from vq.pause_resume import pause_all
        spec1, p1 = _running_job_with_real_process("tag0pall0001", state)
        spec2, p2 = _running_job_with_real_process("tag0pall0002", state)
        try:
            pause_all("localhost", paused_by="my-script")
            for jid in (spec1.id, spec2.id):
                recovered = JobSpec.read(paths.spec_path(jid))
                assert recovered.state == JobState.SUSPENDED
                assert recovered.paused_by == "my-script"
        finally:
            for p in (p1, p2):
                p.kill()
                p.wait(timeout=2)

    def test_resume_all_with_filter_only_resumes_matching(
        self, state: Path
    ) -> None:
        """One job paused by script-A, one by script-B, one untagged.
        resume_all(paused_by_filter='script-A') only resumes the
        first."""
        from vq.pause_resume import resume_all
        spec_a, pa = _running_job_with_real_process("tag0pby000A1", state)
        spec_b, pb = _running_job_with_real_process("tag0pby000B1", state)
        spec_n, pn = _running_job_with_real_process("tag0pby000N1", state)
        try:
            pause_job("localhost", spec_a.id, paused_by="script-A")
            pause_job("localhost", spec_b.id, paused_by="script-B")
            pause_job("localhost", spec_n.id)  # untagged

            summary = resume_all("localhost", paused_by_filter="script-A")
            assert "resumed 1 job" in summary
            # script-B + untagged are "paused by other tag, left paused"
            assert "paused by other tag" in summary

            # Only spec_a is back to RUNNING
            assert JobSpec.read(paths.spec_path(spec_a.id)).state == JobState.RUNNING
            assert JobSpec.read(paths.spec_path(spec_b.id)).state == JobState.SUSPENDED
            assert JobSpec.read(paths.spec_path(spec_n.id)).state == JobState.SUSPENDED
        finally:
            for p in (pa, pb, pn):
                p.kill()
                p.wait(timeout=2)

    def test_resume_all_no_filter_resumes_everything(
        self, state: Path
    ) -> None:
        """resume_all() without a filter keeps the pre-v0.6.22 behavior:
        resume every SUSPENDED job regardless of paused_by."""
        from vq.pause_resume import resume_all
        spec_a, pa = _running_job_with_real_process("tag0nofilt001", state)
        spec_b, pb = _running_job_with_real_process("tag0nofilt002", state)
        try:
            pause_job("localhost", spec_a.id, paused_by="script-A")
            pause_job("localhost", spec_b.id)
            resume_all("localhost")  # no filter — both come back
            assert JobSpec.read(paths.spec_path(spec_a.id)).state == JobState.RUNNING
            assert JobSpec.read(paths.spec_path(spec_b.id)).state == JobState.RUNNING
        finally:
            for p in (pa, pb):
                p.kill()
                p.wait(timeout=2)


class TestPausedByCLI:
    """vq pause --paused-by / vq resume --paused-by via CliRunner."""

    def test_pause_cli_with_paused_by_records_tag(
        self, state: Path
    ) -> None:
        from click.testing import CliRunner
        spec, proc = _running_job_with_real_process("cli0pby0001a", state)
        try:
            result = CliRunner().invoke(
                main,
                ["pause", "localhost", spec.id, "--paused-by", "my-tag"],
            )
            assert result.exit_code == 0, result.output
            assert "my-tag" in result.output  # the per-job message echoes it
            recovered = JobSpec.read(paths.spec_path(spec.id))
            assert recovered.paused_by == "my-tag"
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_cli_paused_by_mismatch_errors(
        self, state: Path
    ) -> None:
        """vq resume JOBID --paused-by FOO errors when the spec's
        paused_by != FOO — caught at the CLI boundary so the script
        sees a clean error instead of silently no-op'ing."""
        from click.testing import CliRunner
        spec, proc = _running_job_with_real_process("cli0pby0001b", state)
        try:
            pause_job("localhost", spec.id, paused_by="foo")
            result = CliRunner().invoke(
                main,
                ["resume", "localhost", spec.id, "--paused-by", "bar"],
            )
            assert result.exit_code != 0
            assert "does not match" in result.output
            # Spec stays SUSPENDED
            assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.SUSPENDED
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_cli_paused_by_match_resumes(self, state: Path) -> None:
        from click.testing import CliRunner
        spec, proc = _running_job_with_real_process("cli0pby0001c", state)
        try:
            pause_job("localhost", spec.id, paused_by="foo")
            result = CliRunner().invoke(
                main,
                ["resume", "localhost", spec.id, "--paused-by", "foo"],
            )
            assert result.exit_code == 0, result.output
            assert JobSpec.read(paths.spec_path(spec.id)).state == JobState.RUNNING
        finally:
            proc.kill()
            proc.wait(timeout=2)

    def test_resume_cli_passes_filter_into_locked_authorized_helper(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "test-vq-admins"\n'
        )
        captured: dict[str, object] = {}

        def fake_resume(host: str, jobid: str, **kwargs: object) -> str:
            captured.update(host=host, jobid=jobid, **kwargs)
            return "resumed through authorized helper"

        monkeypatch.setattr("vq.cli.resume_job", fake_resume)

        result = CliRunner().invoke(
            main,
            ["resume", "localhost", "filteredjob01", "--paused-by", "mine"],
        )

        assert result.exit_code == 0, result.output
        assert captured == {
            "host": "localhost",
            "jobid": "filteredjob01",
            "paused_by_filter": "mine",
            "multi_user": True,
        }

    def test_pause_cli_invalid_charset_rejected(self, state: Path) -> None:
        from click.testing import CliRunner
        result = CliRunner().invoke(
            main,
            ["pause", "localhost", "nonexist0001", "--paused-by", "has space"],
        )
        assert result.exit_code != 0
        assert "invalid" in result.output.lower()


class TestBulkTerminalHistory:
    @pytest.mark.parametrize("operation", [
        "pause_all", "resume_all", "pause_provides_branches",
        "pause_scheduler_all", "resume_scheduler_all",
    ])
    def test_terminal_history_never_enters_control_locks(
        self, state: Path, monkeypatch: pytest.MonkeyPatch, operation: str,
    ) -> None:
        """Retained history cannot contend with a live control transaction."""
        history = []
        for i in range(128):
            spec = _scheduler_job(f"history{i:05d}", state=JobState.COMPLETED)
            spec.branch = "main"
            path = paths.spec_path(spec.id)
            spec.write(path)
            history.append((path, path.read_bytes()))
        live = _scheduler_job("historylive1", state=JobState.PENDING)
        live.branch = "main"
        live.write(paths.spec_path(live.id))
        locked = []
        real_lock = paths.spec_lock

        @contextlib.contextmanager
        def record_lock(path: Path):
            locked.append(path.stem)
            with real_lock(path):
                yield

        monkeypatch.setattr(paths, "spec_lock", record_lock)
        args = (
            "host_f-scheduler-test", _scheduler_host_config()
        ) if "scheduler" in operation else (
            ("localhost", ["main"]) if operation == "pause_provides_branches"
            else ("localhost",)
        )
        result = getattr(pause_resume, operation)(*args)
        assert locked == [live.id]
        assert "128" not in str(result)
        assert all(path.read_bytes() == data for path, data in history)

    @pytest.mark.parametrize("field,value", [
        ("paused_by", "unfinished-update"),
        ("pause_intent_at", "2026-09-07T12:00:00+00:00"),
        ("pause_intent_by", "unfinished-update"),
    ])
    def test_terminal_pause_evidence_retains_locked_handling(
        self, state: Path, monkeypatch: pytest.MonkeyPatch, field: str, value: str,
    ) -> None:
        spec = _scheduler_job("historyproof", state=JobState.FAILED)
        setattr(spec, field, value)
        spec.write(paths.spec_path(spec.id))
        checked = []

        def check_owner(spec: JobSpec, **kwargs: object) -> None:
            checked.append(spec.id)

        monkeypatch.setattr(ownership, "check_owner", check_owner)
        pause_resume.resume_all("localhost")
        assert checked == [spec.id]

    def test_replaced_live_hint_cannot_bypass_final_authorization(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job("historyrace1", state=JobState.SUSPENDED)
        original_read = pause_resume.spec_access.read_bounded_regular_spec

        def replace_after_hint(path: Path) -> JobSpec:
            hint = original_read(path)
            changed = JobSpec.read(path)
            changed.submitter = "foreign-owner"
            changed.write(path)
            return hint

        monkeypatch.setattr(
            pause_resume.spec_access, "read_bounded_regular_spec", replace_after_hint,
        )
        _install_owner_gate_spy(monkeypatch, denied_jobids={spec.id})
        summary = pause_resume.resume_all("localhost")
        assert "1 error(s)" in summary
        fresh = JobSpec.read(paths.spec_path(spec.id))
        assert fresh.state == JobState.SUSPENDED
        assert fresh.submitter == "foreign-owner"

    def test_terminal_rows_remain_in_exact_token_proof(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job("historytoken", state=JobState.COMPLETED)
        spec.paused_by = "unfinished-update"
        spec.write(paths.spec_path(spec.id))
        proof = pause_resume.prove_pause_token_absent("localhost", "unfinished-update")
        assert not proof.proven_clear
        assert proof.unresolved[0][0] == spec.id

    def test_denied_terminal_hint_keeps_isolated_error(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        spec = _scheduler_job("historydeny1", state=JobState.COMPLETED)
        checks = []

        def deny(spec: JobSpec, **kwargs: object) -> None:
            checks.append(spec.id)
            raise ownership.OwnershipError("foreign job")

        monkeypatch.setattr(ownership, "check_owner", deny)
        summary = pause_resume.resume_all("localhost")
        assert checks == [spec.id, spec.id]
        assert "1 error(s)" in summary
        assert "completed" not in summary

    def test_terminal_hint_cannot_hide_invalid_policy(
        self, state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _scheduler_job("historybadcfg", state=JobState.COMPLETED)

        def invalid_policy(*args: object, **kwargs: object) -> None:
            raise config.ConfigError("policy unavailable")

        monkeypatch.setattr(ownership, "check_owner", invalid_policy)
        with pytest.raises(config.ConfigError, match="policy unavailable"):
            pause_resume.resume_all("localhost")
