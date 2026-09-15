"""Tests for v0.5.30 opt-in auto-resume after host reboot.

Covers: the JobSpec fields (recover_on_reboot, parent_jobid), the
--auto-resume submit flag, and — the core behaviour — the daemon's
startup-recovery auto-resume pass that emits a sibling resubmit for a
RUNNING job killed by a reboot.

The daemon tests drive ``_reattach_or_interrupt_at_startup`` directly
and monkeypatch ``_pgroup_alive`` to simulate "process group gone"
(the hard-reboot signature) without spawning + killing real processes.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState, ProgramRuntimePin
from vq.submit import submit_local

# ----------------------------------------------------------------------
# JobSpec fields
# ----------------------------------------------------------------------


class TestAutoResumeSpecFields:
    def test_defaults(self) -> None:
        spec = JobSpec(id="a" * 12, command=["true"], cwd="/tmp", cpus=1)
        assert spec.recover_on_reboot is False
        assert spec.parent_jobid is None

    def test_explicit_values(self) -> None:
        spec = JobSpec(
            id="b" * 12, command=["true"], cwd="/tmp", cpus=1,
            recover_on_reboot=True, parent_jobid="a" * 12,
        )
        assert spec.recover_on_reboot is True
        assert spec.parent_jobid == "a" * 12

    def test_old_spec_reads_clean(self, tmp_path: Path) -> None:
        """Additive fields: a pre-v0.5.30 spec JSON (no recover_on_reboot
        / parent_jobid keys) reads into the current model with the
        defaults — no SPEC_VERSION bump."""
        old = {
            "spec_version": 2,
            "id": "c" * 12,
            "command": ["true"],
            "cwd": "/tmp/c",
            "cpus": 1,
            "state": "pending",
            "submitted_at": "2026-05-09T12:00:00+00:00",
        }
        path = tmp_path / "old.json"
        path.write_text(json.dumps(old))
        spec = JobSpec.read(path)
        assert spec.recover_on_reboot is False
        assert spec.parent_jobid is None

    def test_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(
            id="d" * 12, command=["true"], cwd="/tmp", cpus=1,
            recover_on_reboot=True, parent_jobid="e" * 12,
        )
        path = tmp_path / "spec.json"
        spec.write(path)
        back = JobSpec.read(path)
        assert back.recover_on_reboot is True
        assert back.parent_jobid == "e" * 12


# ----------------------------------------------------------------------
# submit_local + CLI --auto-resume
# ----------------------------------------------------------------------


@pytest.fixture
def submit_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


class TestSubmitAutoResume:
    def test_submit_local_default_off(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(host="localhost", input_file=str(script))
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.recover_on_reboot is False

    def test_submit_local_auto_resume_on(self, submit_state: Path) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(
            host="localhost", input_file=str(script), auto_resume=True,
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.recover_on_reboot is True

    def test_cli_auto_resume_flag(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--auto-resume"]
        )
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.recover_on_reboot is True

    def test_cli_default_off(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(main, ["submit", str(script)])
        assert result.exit_code == 0, result.output
        spec = JobSpec.read(paths.queue_dir() / f"{result.output.strip()}.json")
        assert spec.recover_on_reboot is False

    def test_cli_help_mentions_auto_resume(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--auto-resume" in result.output


# ----------------------------------------------------------------------
# Daemon startup auto-resume pass
# ----------------------------------------------------------------------


@pytest.fixture
def daemon(tmp_path: Path) -> Iterator[Daemon]:
    d = Daemon(
        max_cpus=8,
        poll_interval=0.05,
        queue_dir=tmp_path / "queue",
        jobs_dir=tmp_path / "jobs",
    )
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield d
    for rj in d._running.values():
        try:
            rj.popen.kill()
            rj.popen.wait(timeout=1)
        except Exception:
            pass
        rj.close_logs()


def _write_spec(
    daemon: Daemon,
    jobid: str,
    *,
    state: JobState,
    pgid: int | None = 999001,
    recover_on_reboot: bool = False,
    priority: int = 0,
    cpus: int = 1,
    command: list[str] | None = None,
    program: str | None = None,
    program_runtime_pin: ProgramRuntimePin | None = None,
) -> JobSpec:
    """Write a spec + its workspace dir. pgid defaults to a high value
    that won't exist as a real process group (tests monkeypatch
    _pgroup_alive anyway, but a non-real pgid keeps it honest)."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=command or ["echo", "hi"],
        cwd=str(workspace),
        cpus=cpus,
        state=state,
        pgid=pgid,
        recover_on_reboot=recover_on_reboot,
        priority=priority,
        program=program,
        program_runtime_pin=program_runtime_pin,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _all_specs(daemon: Daemon) -> list[JobSpec]:
    return [JobSpec.read(p) for p in sorted(daemon.queue_dir.glob("*.json"))]


def _siblings_of(daemon: Daemon, parent_id: str) -> list[JobSpec]:
    return [s for s in _all_specs(daemon) if s.parent_jobid == parent_id]


class TestDaemonAutoResume:
    def test_running_with_flag_gets_sibling(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The core case: RUNNING at daemon-down, pgid gone, no exit
        marker, recover_on_reboot=True → ABORTED_BY_QUEUE + a sibling
        PENDING resubmit."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "deadjob00001", state=JobState.RUNNING,
            recover_on_reboot=True, priority=5, cpus=4,
            command=["python", "sweep.py"],
        )
        daemon._reattach_or_interrupt_at_startup()

        dead = JobSpec.read(daemon._spec_path("deadjob00001"))
        assert dead.state == JobState.ABORTED_BY_QUEUE

        siblings = _siblings_of(daemon, "deadjob00001")
        assert len(siblings) == 1
        sib = siblings[0]
        assert sib.id != "deadjob00001"          # fresh jobid
        assert sib.state == JobState.PENDING
        assert sib.command == ["python", "sweep.py"]  # same command
        assert sib.cwd == dead.cwd                # SAME workspace
        assert sib.cpus == 4                      # inherited
        assert sib.priority == 5                  # inherited
        assert sib.recover_on_reboot is True      # propagates
        assert sib.parent_jobid == "deadjob00001"

    def test_auto_resume_sibling_does_not_inherit_idempotency_binding(
        self,
        daemon: Daemon,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda _pgid: False)
        dead = _write_spec(
            daemon,
            "keyeddead001",
            state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        dead.idempotency_key_hash = "a" * 64
        dead.submission_owner_hash = "b" * 64
        dead.submission_intent_digest = "c" * 64
        dead.write(daemon._spec_path(dead.id))

        daemon._reattach_or_interrupt_at_startup()

        (sibling,) = _siblings_of(daemon, dead.id)
        assert sibling.idempotency_key_hash is None
        assert sibling.submission_owner_hash is None
        assert sibling.submission_intent_digest is None

    def test_running_without_flag_no_sibling(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """recover_on_reboot=False (default): job lands in
        ABORTED_BY_QUEUE, NO sibling — unchanged v0.4.1 behaviour."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "deadjob00002", state=JobState.RUNNING,
            recover_on_reboot=False,
        )
        daemon._reattach_or_interrupt_at_startup()

        dead = JobSpec.read(daemon._spec_path("deadjob00002"))
        assert dead.state == JobState.ABORTED_BY_QUEUE
        assert _all_specs(daemon) == [dead]  # no sibling spawned

    def test_no_resubmit_storm_preexisting_aborted(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A spec ALREADY in ABORTED_BY_QUEUE (from a previous run) with
        recover_on_reboot=True must NOT be resumed on a fresh startup —
        only jobs that were RUNNING-at-entry to THIS pass are eligible.
        Otherwise every daemon restart resubmit-storms."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "oldaborted001", state=JobState.ABORTED_BY_QUEUE,
            recover_on_reboot=True,
        )
        daemon._reattach_or_interrupt_at_startup()
        # Still exactly one spec — nothing resubmitted.
        assert len(_all_specs(daemon)) == 1

    def test_suspended_then_killed_not_resumed(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A SUSPENDED job whose pgid is gone (user paused it, then the
        host died) is NOT auto-resumed even with the flag — un-pausing
        via resubmit would contradict the user's pause intent."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "suspjob000001", state=JobState.SUSPENDED,
            recover_on_reboot=True,
        )
        daemon._reattach_or_interrupt_at_startup()

        dead = JobSpec.read(daemon._spec_path("suspjob000001"))
        assert dead.state == JobState.ABORTED_BY_QUEUE
        assert len(_all_specs(daemon)) == 1  # no sibling

    def test_exit_marker_present_completes_no_resume(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """RUNNING, pgid gone, BUT an exit marker exists (job actually
        finished while the daemon was down) + recover_on_reboot=True →
        _record_orphan_finish classifies it COMPLETED; NO auto-resume
        (the job ran to completion, resuming would re-run finished work)."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        spec = _write_spec(
            daemon, "finishedjob01", state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        # Write the v0.5.9 exit marker: job exited 0 before the gap.
        marker = Path(spec.cwd) / "_vq" / "exit-code"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("0\n")

        daemon._reattach_or_interrupt_at_startup()
        done = JobSpec.read(daemon._spec_path("finishedjob01"))
        assert done.state == JobState.COMPLETED
        assert len(_all_specs(daemon)) == 1  # no sibling — it finished

    def test_live_pgid_reattached_not_resumed(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """RUNNING, pgid still ALIVE (daemon-only restart, host didn't
        reboot) + recover_on_reboot=True → re-attached as orphan, stays
        RUNNING, no resubmit."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: True)
        _write_spec(
            daemon, "alivejob00001", state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        daemon._reattach_or_interrupt_at_startup()

        still = JobSpec.read(daemon._spec_path("alivejob00001"))
        assert still.state == JobState.RUNNING
        assert "alivejob00001" in daemon._orphans
        assert len(_all_specs(daemon)) == 1  # no sibling

    def test_sibling_dispatchable_and_resumable_again(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The sibling is PENDING (so the dispatch loop will pick it up)
        AND carries recover_on_reboot=True itself — a sibling that dies
        in a LATER reboot gets resumed again. Simulate a second reboot
        on the sibling."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "genzero000001", state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        daemon._reattach_or_interrupt_at_startup()
        gen1 = _siblings_of(daemon, "genzero000001")[0]
        assert gen1.recover_on_reboot is True

        # Simulate gen1 having run + been killed by a second reboot:
        # flip it to RUNNING with a (now-dead) pgid, re-run the pass.
        gen1.state = JobState.RUNNING
        gen1.pgid = 999002
        gen1.write(daemon._spec_path(gen1.id))
        daemon._reattach_or_interrupt_at_startup()

        # gen1 is now ABORTED_BY_QUEUE and has its OWN sibling (gen2).
        gen1_after = JobSpec.read(daemon._spec_path(gen1.id))
        assert gen1_after.state == JobState.ABORTED_BY_QUEUE
        gen2 = _siblings_of(daemon, gen1.id)
        assert len(gen2) == 1
        assert gen2[0].parent_jobid == gen1.id      # lineage chains

    def test_multiple_jobs_mixed_flags(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two RUNNING jobs die in the reboot — only the flagged one
        gets a sibling."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        _write_spec(
            daemon, "flagged000001", state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        _write_spec(
            daemon, "unflagged0001", state=JobState.RUNNING,
            recover_on_reboot=False,
        )
        daemon._reattach_or_interrupt_at_startup()

        assert len(_siblings_of(daemon, "flagged000001")) == 1
        assert len(_siblings_of(daemon, "unflagged0001")) == 0
        # 2 originals + 1 sibling = 3
        assert len(_all_specs(daemon)) == 3

    def test_sibling_logs_submitted_event(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The sibling's SUBMITTED event lands in the shared workspace
        events.jsonl with parent_jobid recorded."""
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        spec = _write_spec(
            daemon, "eventjob00001", state=JobState.RUNNING,
            recover_on_reboot=True,
        )
        daemon._reattach_or_interrupt_at_startup()

        events_file = Path(spec.cwd) / "_vq" / "events.jsonl"
        assert events_file.exists()
        lines = [json.loads(line) for line in events_file.read_text().splitlines()]
        # append_event stores **data flat at the top level of the record.
        submitted = [e for e in lines if e.get("kind") == "submitted"]
        assert len(submitted) == 1
        assert submitted[0]["parent_jobid"] == "eventjob00001"
        assert "auto-resume" in submitted[0]["reason"]
        # The sibling's jobid is the event's jobid, and it's not the parent.
        assert submitted[0]["jobid"] != "eventjob00001"

    def test_program_survives_auto_resume(
        self, daemon: Daemon, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("vq.daemon._pgroup_alive", lambda pgid: False)
        spec = _write_spec(
            daemon,
            "programjob001",
            state=JobState.RUNNING,
            recover_on_reboot=True,
            program="vibeqc-release",
            program_runtime_pin=ProgramRuntimePin(expected_git_sha="abc123"),
        )

        daemon._reattach_or_interrupt_at_startup()

        siblings = _siblings_of(daemon, "programjob001")
        assert len(siblings) == 1
        assert siblings[0].program == "vibeqc-release"
        assert siblings[0].program_runtime_pin is not None
        assert siblings[0].program_runtime_pin.expected_git_sha == "abc123"
        events_file = Path(spec.cwd) / "_vq" / "events.jsonl"
        submitted = []
        for line in events_file.read_text().splitlines():
            rec = json.loads(line)
            if rec.get("kind") == "submitted":
                submitted.append(rec)
        assert submitted[-1]["program"] == "vibeqc-release"
