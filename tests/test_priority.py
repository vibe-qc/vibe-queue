"""Tests for v0.5.29 job priority: JobSpec.priority field, daemon
dispatch ordering by (-priority, submitted_at), --priority CLI flag,
and the conditional PRI column in `vq queue`."""
from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.listing import format_table
from vq.spec import JobSpec, JobState
from vq.submit import submit_local

# ----------------------------------------------------------------------
# JobSpec.priority field
# ----------------------------------------------------------------------


class TestPrioritySpecField:
    def test_default_is_zero(self) -> None:
        spec = JobSpec(id="a" * 12, command=["true"], cwd="/tmp", cpus=1)
        assert spec.priority == 0

    def test_accepts_positive(self) -> None:
        spec = JobSpec(
            id="b" * 12, command=["true"], cwd="/tmp", cpus=1, priority=10,
        )
        assert spec.priority == 10

    def test_accepts_negative(self) -> None:
        """Negative priority = 'run after the default-priority work'."""
        spec = JobSpec(
            id="c" * 12, command=["true"], cwd="/tmp", cpus=1, priority=-5,
        )
        assert spec.priority == -5

    def test_old_spec_without_priority_reads_clean(
        self, tmp_path: Path
    ) -> None:
        """Additive field: a v2 spec JSON written before v0.5.29 (no
        'priority' key) must read into the current model with
        priority defaulting to 0 — no SPEC_VERSION bump."""
        old_json = {
            "spec_version": 2,
            "id": "d" * 12,
            "command": ["true"],
            "cwd": "/tmp/d",
            "cpus": 1,
            "state": "pending",
            "submitted_at": "2026-05-09T12:00:00+00:00",
            # NOTE: no "priority" key — simulates a pre-v0.5.29 spec
        }
        path = tmp_path / "old.json"
        path.write_text(json.dumps(old_json))
        spec = JobSpec.read(path)
        assert spec.priority == 0
        assert spec.id == "d" * 12

    def test_priority_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(
            id="e" * 12, command=["true"], cwd="/tmp", cpus=1, priority=7,
        )
        path = tmp_path / "spec.json"
        spec.write(path)
        back = JobSpec.read(path)
        assert back.priority == 7


# ----------------------------------------------------------------------
# Daemon dispatch ordering
# ----------------------------------------------------------------------


@pytest.fixture
def serial_daemon(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[Daemon]:
    """Daemon with max_jobs=1 so we can observe WHICH pending job the
    dispatcher picks first."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    d = Daemon(
        max_cpus=8,
        max_jobs=1,
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


def _submit_pending(
    daemon: Daemon,
    jobid: str,
    *,
    priority: int = 0,
    submitted_at: str = "2026-05-14T00:00:00+00:00",
    command: list[str] | None = None,
) -> JobSpec:
    """Write a PENDING spec with controllable priority + submitted_at."""
    workspace = daemon.jobs_dir / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    spec = JobSpec(
        id=jobid,
        command=command or ["sleep", "30"],
        cwd=str(workspace),
        cpus=1,
        priority=priority,
        submitted_at=submitted_at,
    )
    spec.write(daemon._spec_path(jobid))
    return spec


def _running_jobid(daemon: Daemon) -> str | None:
    """The single jobid the daemon currently has running, or None."""
    for path in sorted(daemon.queue_dir.glob("*.json")):
        spec = JobSpec.read(path)
        if spec.state == JobState.RUNNING:
            return spec.id
    return None


class TestPriorityDispatchOrder:
    def test_higher_priority_dispatched_first(
        self, serial_daemon: Daemon
    ) -> None:
        """Three PENDING jobs at priority 0, 10, 5 — submitted in that
        order. With max_jobs=1 the daemon must pick the priority-10 one
        first, even though it was submitted between the other two."""
        # submitted_at strictly increasing so FIFO would pick 'low' first
        _submit_pending(
            serial_daemon, "joblow00000a", priority=0,
            submitted_at="2026-05-14T00:00:01+00:00",
        )
        _submit_pending(
            serial_daemon, "jobhigh0000b", priority=10,
            submitted_at="2026-05-14T00:00:02+00:00",
        )
        _submit_pending(
            serial_daemon, "jobmid00000c", priority=5,
            submitted_at="2026-05-14T00:00:03+00:00",
        )
        serial_daemon._dispatch_pending()
        # Only the priority-10 job should be running.
        assert _running_jobid(serial_daemon) == "jobhigh0000b"
        # The other two stay PENDING.
        assert (
            JobSpec.read(serial_daemon._spec_path("joblow00000a")).state
            == JobState.PENDING
        )
        assert (
            JobSpec.read(serial_daemon._spec_path("jobmid00000c")).state
            == JobState.PENDING
        )

    def test_fifo_within_same_priority(
        self, serial_daemon: Daemon
    ) -> None:
        """Two jobs at the SAME priority — the earlier-submitted one
        dispatches first (FIFO tiebreak preserved)."""
        _submit_pending(
            serial_daemon, "jobearly000a", priority=5,
            submitted_at="2026-05-14T00:00:01+00:00",
        )
        _submit_pending(
            serial_daemon, "joblate0000b", priority=5,
            submitted_at="2026-05-14T00:00:09+00:00",
        )
        serial_daemon._dispatch_pending()
        assert _running_jobid(serial_daemon) == "jobearly000a"

    def test_negative_priority_runs_last(
        self, serial_daemon: Daemon
    ) -> None:
        """A negative-priority job submitted FIRST still loses to a
        default-priority job submitted later."""
        _submit_pending(
            serial_daemon, "jobbg000000a", priority=-10,
            submitted_at="2026-05-14T00:00:01+00:00",
        )
        _submit_pending(
            serial_daemon, "jobnorm0000b", priority=0,
            submitted_at="2026-05-14T00:00:05+00:00",
        )
        serial_daemon._dispatch_pending()
        assert _running_jobid(serial_daemon) == "jobnorm0000b"

    def test_all_default_priority_is_pure_fifo(
        self, serial_daemon: Daemon
    ) -> None:
        """Regression guard: when every job is default priority, the
        dispatch order is identical to the pre-v0.5.29 pure-FIFO
        behaviour (earliest submitted_at wins)."""
        _submit_pending(
            serial_daemon, "jobfirst000a", priority=0,
            submitted_at="2026-05-14T00:00:01+00:00",
        )
        _submit_pending(
            serial_daemon, "jobsecond00b", priority=0,
            submitted_at="2026-05-14T00:00:02+00:00",
        )
        serial_daemon._dispatch_pending()
        assert _running_jobid(serial_daemon) == "jobfirst000a"


# ----------------------------------------------------------------------
# submit_local writes priority
# ----------------------------------------------------------------------


@pytest.fixture
def submit_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


class TestSubmitWritesPriority:
    def test_submit_local_default_priority_zero(
        self, submit_state: Path
    ) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(host="localhost", input_file=str(script))
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == 0

    def test_submit_local_explicit_priority(
        self, submit_state: Path
    ) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(
            host="localhost", input_file=str(script), priority=15,
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == 15

    def test_submit_local_negative_priority(
        self, submit_state: Path
    ) -> None:
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        jobid = submit_local(
            host="localhost", input_file=str(script), priority=-3,
        )
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == -3


# ----------------------------------------------------------------------
# CLI --priority flag
# ----------------------------------------------------------------------


class TestPriorityCLI:
    def test_submit_priority_flag_writes_spec(
        self, submit_state: Path
    ) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--priority", "20"]
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == 20

    def test_submit_default_priority_zero(
        self, submit_state: Path
    ) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(main, ["submit", str(script)])
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == 0

    def test_submit_negative_priority(self, submit_state: Path) -> None:
        (submit_state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        script = submit_state / "job.py"
        script.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", str(script), "--priority", "-8"]
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.priority == -8

    def test_submit_help_mentions_priority(self) -> None:
        result = CliRunner().invoke(main, ["submit", "--help"])
        assert result.exit_code == 0
        assert "--priority" in result.output


# ----------------------------------------------------------------------
# vq queue PRI column (conditional)
# ----------------------------------------------------------------------


def _spec(jobid: str, *, priority: int = 0,
          state: JobState = JobState.PENDING) -> JobSpec:
    return JobSpec(
        id=jobid, command=["true"], cwd=f"/tmp/{jobid}", cpus=1,
        priority=priority, state=state,
    )


class TestPriorityListingColumn:
    def test_pri_column_hidden_when_all_default(self) -> None:
        """The common case: no job has a non-zero priority → no PRI
        column at all. Zero noise."""
        specs = [_spec("a" * 12), _spec("b" * 12)]
        table = format_table(specs)
        assert "PRI" not in table
        # Sanity: the other columns are still there.
        assert "ID" in table
        assert "STATE" in table
        assert "CPUS" in table

    def test_pri_column_shown_when_any_nonzero(self) -> None:
        """As soon as one job has a non-zero priority, the PRI column
        appears — for every row, so the values line up."""
        specs = [
            _spec("a" * 12, priority=0),
            _spec("b" * 12, priority=10),
        ]
        table = format_table(specs)
        assert "PRI" in table
        # Both the 0 and the 10 should be rendered in the column.
        lines = table.splitlines()
        # header + 2 rows
        assert len(lines) == 3
        assert "10" in lines[2] or "10" in lines[1]

    def test_pri_column_shown_for_negative_priority(self) -> None:
        specs = [_spec("a" * 12, priority=-5)]
        table = format_table(specs)
        assert "PRI" in table
        assert "-5" in table

    def test_empty_listing_unaffected(self) -> None:
        assert format_table([]) == "(no jobs)"
