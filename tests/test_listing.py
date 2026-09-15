"""Tests for queue listing and table formatting."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import capacity, config, paths
from vq.cli import main
from vq.listing import (
    effective_queue_state,
    format_table,
    list_jobs,
    matches_queue_state_filter,
    pending_configured_capacity_known,
    queue_host_for_scheduler_target,
    scheduler_running_confirmed,
    scheduler_target_is_safe,
)
from vq.spec import JobSpec, JobState


def _write_spec(queue: Path, jobid: str, **fields: object) -> JobSpec:
    queue.mkdir(parents=True, exist_ok=True)
    base: dict[str, object] = {
        "id": jobid,
        "command": ["true"],
        "cwd": "/tmp/" + jobid,
        "cpus": 1,
    }
    base.update(fields)
    spec = JobSpec(**base)
    spec.write(queue / f"{jobid}.json")
    return spec


class TestListJobs:
    def test_empty_queue_returns_empty_list(self, tmp_path: Path) -> None:
        assert list_jobs("localhost", queue_dir=tmp_path / "queue") == []

    def test_returns_all_specs(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        for i in range(3):
            _write_spec(queue, f"j{i}")
        assert {s.id for s in list_jobs("localhost", queue_dir=queue)} == {"j0", "j1", "j2"}

    def test_corrupt_spec_silently_skipped(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write_spec(queue, "ok")
        (queue / "broken.json").write_text("not json")
        result = list_jobs("localhost", queue_dir=queue)
        assert [s.id for s in result] == ["ok"]

    def test_remote_host_rejected(self) -> None:
        with pytest.raises(NotImplementedError):
            list_jobs("some.other.host")


class TestSorting:
    def test_active_states_before_pending_before_terminal(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write_spec(queue, "done", state=JobState.COMPLETED)
        _write_spec(queue, "held", state=JobState.SUSPENDED)
        _write_spec(queue, "pend", state=JobState.PENDING)
        _write_spec(queue, "run", state=JobState.RUNNING)
        ids = [s.id for s in list_jobs("localhost", queue_dir=queue)]
        assert ids == ["run", "held", "pend", "done"]

    def test_terminal_states_have_stable_order(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write_spec(queue, "done", state=JobState.COMPLETED)
        _write_spec(queue, "abort", state=JobState.ABORTED_BY_QUEUE)
        _write_spec(queue, "oom", state=JobState.OOM_KILLED)
        _write_spec(queue, "fail", state=JobState.FAILED)
        _write_spec(queue, "time", state=JobState.TIME_EXCEEDED)
        ids = [s.id for s in list_jobs("localhost", queue_dir=queue)]
        assert ids == ["fail", "oom", "time", "abort", "done"]

    def test_within_state_sorted_by_submission_time_ascending(self, tmp_path: Path) -> None:
        queue = tmp_path / "queue"
        _write_spec(
            queue, "third", state=JobState.PENDING, submitted_at="2026-01-03T00:00:00+00:00"
        )
        _write_spec(
            queue, "first", state=JobState.PENDING, submitted_at="2026-01-01T00:00:00+00:00"
        )
        _write_spec(
            queue, "second", state=JobState.PENDING, submitted_at="2026-01-02T00:00:00+00:00"
        )
        ids = [s.id for s in list_jobs("localhost", queue_dir=queue)]
        assert ids == ["first", "second", "third"]


class TestFormatTable:
    def test_empty_list_renders_placeholder(self) -> None:
        assert format_table([]) == "(no jobs)"

    def test_table_includes_header_and_jobid(self) -> None:
        spec = JobSpec(id="abc12345", command=["python", "x.py"], cwd="/tmp", cpus=1)
        suspended = JobSpec(
            id="heldcluster",
            command=["true"],
            cwd="/tmp/heldcluster",
            cpus=2,
            state=JobState.SUSPENDED,
            scheduler_target="host_c",
            scheduler_state="held",
        )

        table = format_table([spec, suspended])
        lines = table.splitlines()
        assert "ID" in lines[0]
        assert "STATE" in lines[0]
        assert "abc12345" in lines[1]
        assert "pending" in lines[1]

    def test_pending_request_over_configured_cap_is_labelled(self) -> None:
        spec = JobSpec(
            id="wide",
            command=["true"],
            cwd="/tmp/wide",
            cpus=32,
            state=JobState.PENDING,
        )
        snapshot = capacity.DaemonCapacity(
            max_cpus=16,
            max_mem_mb=None,
            written_at="2026-08-17T12:00:00+00:00",
        )

        table = format_table([spec], capacity_snapshot=snapshot)

        assert "pending (over cap)" in table

    def test_scheduler_pending_request_ignores_driver_local_cap(self) -> None:
        spec = JobSpec(
            id="cluster",
            command=["true"],
            cwd="/tmp/cluster",
            cpus=32,
            state=JobState.PENDING,
            scheduler_target="host_f",
        )
        snapshot = capacity.DaemonCapacity(
            max_cpus=16,
            max_mem_mb=None,
            written_at="2026-08-17T12:00:00+00:00",
        )

        table = format_table([spec], capacity_snapshot=snapshot)

        assert "pending (over cap)" not in table

    def test_undeclared_memory_needs_mixed_version_default_metadata(
        self,
    ) -> None:
        spec = JobSpec(
            id="legacy",
            command=["true"],
            cwd="/tmp/legacy",
            cpus=1,
            mem_mb=None,
            state=JobState.PENDING,
        )
        missing_default = capacity.DaemonCapacity.model_validate(
            {
                "max_cpus": 8,
                "max_mem_mb": 4_000,
                "written_at": "2026-08-20T12:00:00+00:00",
            }
        )
        explicit_no_default = capacity.DaemonCapacity(
            max_cpus=8,
            max_mem_mb=4_000,
            default_job_mem_mb=None,
            written_at="2026-08-20T12:00:00+00:00",
        )

        assert "default_job_mem_mb" not in missing_default.model_fields_set
        assert not pending_configured_capacity_known(spec, missing_default)
        assert pending_configured_capacity_known(spec, explicit_no_default)

    def test_total_cpus_footer_when_provided(self) -> None:
        running = JobSpec(
            id="r", command=["true"], cwd="/tmp", cpus=2, state=JobState.RUNNING, pid=1
        )
        suspended = JobSpec(
            id="s",
            command=["true"],
            cwd="/tmp",
            cpus=3,
            state=JobState.SUSPENDED,
            pid=2,
        )
        pending = JobSpec(
            id="p", command=["true"], cwd="/tmp", cpus=4, state=JobState.PENDING
        )
        table = format_table([running, suspended, pending], total_cpus=8)
        assert "active cpus: 5/8" in table

    def test_long_command_truncated_with_ellipsis(self) -> None:
        spec = JobSpec(id="x", command=["python", "a" * 200], cwd="/tmp", cpus=1)
        table = format_table([spec])
        assert "..." in table
        # No row should exceed the truncation budget by much
        for line in table.splitlines():
            assert len(line) < 200

    def test_archived_state_annotated(self) -> None:
        spec = JobSpec(
            id="arc",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            state=JobState.COMPLETED,
            archived_at="2026-05-03T00:00:00+00:00",
            archive_path="/tmp/arc.tar.bz2",
        )
        table = format_table([spec])
        assert "completed (archived)" in table

    def test_suspended_state_shows_paused_by_tag(self) -> None:
        spec = JobSpec(
            id="sus",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            state=JobState.SUSPENDED,
            paused_by="admin-update-123abc",
        )
        table = format_table([spec])
        assert "suspended (paused_by admin-update-123abc)" in table

    def test_suspended_state_without_paused_by_stays_compact(self) -> None:
        spec = JobSpec(
            id="sus",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            state=JobState.SUSPENDED,
        )
        table = format_table([spec])
        assert "suspended (paused_by" not in table

    def test_cluster_column_hidden_for_local_jobs(self) -> None:
        spec = JobSpec(id="local", command=["true"], cwd="/tmp", cpus=1)
        table = format_table([spec])
        assert "CLUSTER" not in table.splitlines()[0]

    def test_cluster_column_shows_scheduler_phase(self) -> None:
        local = JobSpec(id="local", command=["true"], cwd="/tmp/local", cpus=1)
        cluster = JobSpec(
            id="cluster",
            command=["true"],
            cwd="/tmp/cluster",
            cpus=4,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="queued",
        )
        table = format_table([cluster, local])
        header = table.splitlines()[0]
        assert "CLUSTER" in header
        cluster_row = next(line for line in table.splitlines() if "cluster" in line)
        assert "queued" in cluster_row.split()
        assert "host_f:vq=running,sched=queued" in table

    @pytest.mark.parametrize(
        ("scheduler_state", "expected"),
        [
            ("running", "running"),
            ("queued", "queued"),
            ("held", "held"),
            ("poll_failed", "poll_failed"),
            ("finishing", "finishing"),
            ("marker_probe_failed", "marker_probe_failed"),
            ("fetch_failed", "fetch_failed"),
            ("reattach_failed", "reattach_failed"),
            ("release_outcome_unknown", "release_outcome_unknown"),
            ("scheduler_unknown", "scheduler_unknown"),
            (None, "unpolled"),
        ],
    )
    def test_effective_scheduler_state_is_exact_last_phase(
        self, scheduler_state: str | None, expected: str
    ) -> None:
        spec = JobSpec(
            id="cluster",
            command=["true"],
            cwd="/tmp/cluster",
            cpus=4,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state=scheduler_state,
        )

        assert effective_queue_state(spec) == expected
        assert scheduler_running_confirmed(spec) == (expected == "running")

    def test_literal_unknown_display_token_remains_a_legal_target(self) -> None:
        target = "scheduler-target-unknown"
        spec = JobSpec(
            id="sentinelhost",
            command=["true"],
            cwd="/tmp/sentinelhost",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target=target,
            scheduler_state="running",
        )

        assert scheduler_target_is_safe(target) is True
        assert queue_host_for_scheduler_target(target, "driver") == target
        assert effective_queue_state(spec) == "running"
        assert scheduler_running_confirmed(spec) is True
        assert matches_queue_state_filter(
            spec,
            {"scheduler_unknown"},
            active=False,
        ) is False
        table = format_table([spec])
        assert "scheduler occupancy: 1/1" in table

    def test_raw_running_held_is_owned_and_matches_both_filters(self) -> None:
        spec = JobSpec(
            id="externalheld",
            command=["true"],
            cwd="/tmp/externalheld",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="held",
        )

        assert effective_queue_state(spec) == "held"
        assert scheduler_running_confirmed(spec) is False
        assert matches_queue_state_filter(spec, {"running"}, active=False)
        assert matches_queue_state_filter(spec, {"held"}, active=False)
        table = format_table([spec])
        assert "scheduler occupancy: 0/1" in table
        assert "held=1" in table

    def test_effective_state_keeps_local_and_terminal_lifecycle(self) -> None:
        local = JobSpec(
            id="local",
            command=["true"],
            cwd="/tmp/local",
            cpus=1,
            state=JobState.RUNNING,
        )
        terminal = JobSpec(
            id="done",
            command=["true"],
            cwd="/tmp/done",
            cpus=1,
            state=JobState.COMPLETED,
            scheduler_target="host_f",
            scheduler_state="poll_failed",
        )

        assert effective_queue_state(local) == "running"
        assert effective_queue_state(terminal) == "completed"
        assert scheduler_running_confirmed(local) is None
        assert scheduler_running_confirmed(terminal) is None

    @pytest.mark.parametrize(
        ("scheduler_target", "scheduler_state"),
        [
            ("", "running"),
            ("host_f", ""),
        ],
    )
    def test_empty_scheduler_identity_projects_unknown_and_unconfirmed(
        self,
        scheduler_target: str,
        scheduler_state: str,
    ) -> None:
        spec = JobSpec(
            id="invalidempty",
            command=["true"],
            cwd="/tmp/invalidempty",
            cpus=1,
            state=JobState.RUNNING,
            scheduler_target=scheduler_target,
            scheduler_state=scheduler_state,
        )

        assert effective_queue_state(spec) == "scheduler_unknown"
        assert scheduler_running_confirmed(spec) is False
        table = format_table([spec])
        assert "scheduler_unknown" in table
        assert "scheduler occupancy: 0/1" in table

    def test_cluster_label_preserves_bounded_transaction_phases(self) -> None:
        specs = [
            JobSpec(
                id="submitting01",
                command=["true"],
                cwd="/tmp/submitting01",
                cpus=1,
                state=JobState.SUBMITTING,
                scheduler_target="host_c",
                scheduler_state="submitting",
            ),
            JobSpec(
                id="unknownsub01",
                command=["true"],
                cwd="/tmp/unknownsub01",
                cpus=1,
                state=JobState.SUBMIT_OUTCOME_UNKNOWN,
                scheduler_target="host_c",
                scheduler_state="submit_evidence_conflict",
            ),
            JobSpec(
                id="unknownhold1",
                command=["true"],
                cwd="/tmp/unknownhold1",
                cpus=1,
                state=JobState.SUSPENDED,
                scheduler_target="host_c",
                scheduler_state="hold_outcome_unknown",
            ),
            JobSpec(
                id="rejected0001",
                command=["true"],
                cwd="/tmp/rejected0001",
                cpus=1,
                state=JobState.FAILED,
                scheduler_target="host_c",
                scheduler_state="submit_rejected_after_terminal",
            ),
        ]

        table = format_table(specs)

        for phase in (
            "submitting",
            "submit_evidence_conflict",
            "hold_outcome_unknown",
            "submit_rejected_after_terminal",
        ):
            assert phase in table

    def test_poll_failed_row_is_not_rendered_as_running_and_surfaces_fetch(
        self,
    ) -> None:
        spec = JobSpec(
            id="stalecluster",
            command=["true"],
            cwd="/tmp/stalecluster",
            cpus=4,
            state=JobState.RUNNING,
            started_at="2026-08-22T15:19:27+00:00",
            wall_time_seconds=23 * 60 * 60,
            scheduler_target="host_c",
            scheduler_state="poll_failed",
        )

        table = format_table([spec])

        row = next(line for line in table.splitlines() if "stalecluster" in line)
        assert "poll_failed" in row.split()
        assert "scheduler occupancy: 0/1" in table
        assert "not confirmed running" in table
        assert "vq status HOST JOBID" in table
        assert "vq fetch HOST JOBID" in table

    def test_unrenderable_scheduler_reservation_still_counts_unknown(self) -> None:
        table = format_table(
            [],
            additional_unconfirmed_scheduler_running=1,
        )

        assert "(no jobs)" in table
        assert "scheduler occupancy: 0/1" in table
        assert "scheduler_unknown=1" in table

    def test_cluster_column_marks_unpolled_scheduler_job(self) -> None:
        spec = JobSpec(
            id="cluster",
            command=["true"],
            cwd="/tmp/cluster",
            cpus=4,
            scheduler_target="host_f",
        )
        table = format_table([spec])
        assert "host_f:vq=pending,sched=unpolled" in table

    def test_cluster_column_marks_terminal_scheduler_job_as_local_terminal(
        self,
    ) -> None:
        spec = JobSpec(
            id="cluster",
            command=["true"],
            cwd="/tmp/cluster",
            cpus=4,
            state=JobState.COMPLETED,
            scheduler_target="host_f",
            scheduler_state="running",
            finished_at="2026-06-30T10:00:00+00:00",
            exit_code=0,
        )
        table = format_table([spec])
        assert "host_f:vq=completed,sched_last=running" in table

    def test_scheduler_tasks_column_hidden_for_plain_cpu_jobs(self) -> None:
        spec = JobSpec(id="local", command=["true"], cwd="/tmp/local", cpus=4)

        table = format_table([spec])

        assert "TASKS" not in table.splitlines()[0]

    def test_scheduler_tasks_column_shows_when_any_job_uses_tasks(self) -> None:
        local = JobSpec(id="local", command=["true"], cwd="/tmp/local", cpus=4)
        slurm = JobSpec(
            id="slurm",
            command=["orca", "input.inp"],
            cwd="/tmp/slurm",
            cpus=1,
            scheduler_tasks=8,
            scheduler_target="host_c",
        )

        table = format_table([slurm, local])
        header = table.splitlines()[0]
        slurm_row = next(line for line in table.splitlines() if "slurm" in line)

        assert "TASKS" in header
        assert "CPUS" in header
        assert header.index("CPUS") < header.index("TASKS")
        assert "  1     8  " in slurm_row


# ----------------------------------------------------------------------
# v0.5.27: vq queue --state / --active filter
# ----------------------------------------------------------------------


@pytest.fixture
def queue_with_mixed_states(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Populate a queue with one spec in each common state. Returns
    tmp_path so the test can pass it to CliRunner with the right env."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    qd = paths.queue_dir()
    qd.mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    # Six representative states.
    for state in [
        JobState.PENDING, JobState.RUNNING, JobState.SUSPENDED,
        JobState.COMPLETED, JobState.FAILED, JobState.KILLED,
    ]:
        _write_spec(qd, f"id_{state.value}"[:12], state=state)
    return tmp_path


class TestQueueStateFilter:
    """`vq queue --state STATE` filters by job state. Repeatable.
    `--active` includes every non-terminal lifecycle state."""

    def test_no_filter_shows_all_states(
        self, queue_with_mixed_states: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue"])
        assert result.exit_code == 0
        for state in ("pending", "running", "suspended",
                      "completed", "failed", "killed"):
            assert state in result.output, (
                f"state {state} missing from unfiltered output"
            )

    def test_single_state_filter(
        self, queue_with_mixed_states: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue", "-s", "running"])
        assert result.exit_code == 0
        assert "running" in result.output
        # Other states must NOT appear in the table rows.
        # (They might appear in column headers like "SUBMITTED" — but
        # those don't contain "completed"/"failed"/etc. literals.)
        assert "completed" not in result.output
        assert "failed" not in result.output
        assert "killed" not in result.output

    def test_multiple_state_filter(
        self, queue_with_mixed_states: Path
    ) -> None:
        result = CliRunner().invoke(
            main, ["queue", "-s", "running", "-s", "pending"]
        )
        assert result.exit_code == 0
        assert "running" in result.output
        assert "pending" in result.output
        assert "completed" not in result.output
        assert "suspended" not in result.output  # not selected

    def test_active_flag_includes_running_pending_suspended(
        self, queue_with_mixed_states: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue", "--active"])
        assert result.exit_code == 0
        for state in ("running", "pending", "suspended"):
            assert state in result.output, (
                f"--active should include {state}"
            )
        for state in ("completed", "failed", "killed"):
            assert state not in result.output, (
                f"--active should NOT include {state}"
            )

    def test_active_plus_explicit_state(
        self, queue_with_mixed_states: Path
    ) -> None:
        """--active composes with -s. Adding -s completed includes it."""
        result = CliRunner().invoke(
            main, ["queue", "--active", "-s", "completed"]
        )
        assert result.exit_code == 0
        for state in ("running", "pending", "suspended", "completed"):
            assert state in result.output
        assert "killed" not in result.output

    def test_scheduler_running_filter_retains_owned_rows_and_qualifies_them(
        self, queue_with_mixed_states: Path
    ) -> None:
        qd = paths.queue_dir()
        _write_spec(
            qd,
            "confirmedrun",
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_state="running",
        )
        _write_spec(
            qd,
            "pollfailed01",
            state=JobState.RUNNING,
            started_at="2026-08-22T15:19:27+00:00",
            wall_time_seconds=23 * 60 * 60,
            scheduler_target="host_c",
            scheduler_state="poll_failed",
        )
        _write_spec(
            qd,
            "finishing001",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="finishing",
        )
        _write_spec(
            qd,
            "schedulerq01",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="queued",
        )
        _write_spec(
            qd,
            "unpolled0001",
            state=JobState.RUNNING,
            scheduler_target="host_c",
        )

        result = CliRunner().invoke(main, ["queue", "-s", "running"])

        assert result.exit_code == 0, result.output
        assert "id_running" in result.output
        assert "confirmedrun" in result.output
        assert "pollfailed01" in result.output
        assert "finishing001" in result.output
        assert "schedulerq01" in result.output
        assert "unpolled0001" in result.output
        assert "scheduler occupancy: 1/5" in result.output
        assert "4 not confirmed running" in result.output
        assert "finishing=1" in result.output
        assert "poll_failed=1" in result.output
        assert "queued=1" in result.output
        assert "unpolled=1" in result.output

        json_result = CliRunner().invoke(
            main, ["queue", "-s", "running", "--json"]
        )
        assert json_result.exit_code == 0, json_result.output
        rows = {row["id"]: row for row in json.loads(json_result.output)}
        assert set(rows) == {
            "id_running",
            "confirmedrun",
            "pollfailed01",
            "finishing001",
            "schedulerq01",
            "unpolled0001",
        }
        assert rows["id_running"]["scheduler_running_confirmed"] is None
        assert rows["id_running"]["effective_state"] == "running"
        assert rows["confirmedrun"]["scheduler_running_confirmed"] is True
        assert rows["confirmedrun"]["effective_state"] == "running"
        for jobid, phase in (
            ("pollfailed01", "poll_failed"),
            ("finishing001", "finishing"),
            ("schedulerq01", "queued"),
            ("unpolled0001", "unpolled"),
        ):
            assert rows[jobid]["scheduler_running_confirmed"] is False
            assert rows[jobid]["effective_state"] == phase

    def test_scheduler_phase_is_queryable_and_json_carries_effective_state(
        self, queue_with_mixed_states: Path
    ) -> None:
        _write_spec(
            paths.queue_dir(),
            "pollfailed01",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="poll_failed",
        )

        result = CliRunner().invoke(
            main, ["queue", "-s", "poll_failed", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert [row["id"] for row in payload] == ["pollfailed01"]
        assert payload[0]["state"] == "running"
        assert payload[0]["effective_state"] == "poll_failed"
        assert payload[0]["scheduler_running_confirmed"] is False

    def test_held_phase_filter_matches_scheduler_suspended_row(
        self, queue_with_mixed_states: Path
    ) -> None:
        _write_spec(
            paths.queue_dir(),
            "schedulerheld",
            state=JobState.SUSPENDED,
            scheduler_target="host_c",
            scheduler_state="held",
        )

        result = CliRunner().invoke(main, ["queue", "-s", "held"])
        json_result = CliRunner().invoke(
            main,
            ["queue", "-s", "held", "--json"],
        )

        assert result.exit_code == 0, result.output
        assert "schedulerheld" in result.output
        assert "held" in result.output
        assert "id_suspended" not in result.output
        assert json_result.exit_code == 0, json_result.output
        payload = json.loads(json_result.output)
        assert [row["id"] for row in payload] == ["schedulerheld"]
        assert payload[0]["state"] == "suspended"
        assert payload[0]["effective_state"] == "held"
        assert payload[0]["scheduler_running_confirmed"] is None

    def test_unknown_scheduler_phase_is_bounded_but_preserved_in_json(
        self, queue_with_mixed_states: Path
    ) -> None:
        raw_phase = "running\nFORGED idle\x00" + ("x" * 300)
        _write_spec(
            paths.queue_dir(),
            "unknownphase",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state=raw_phase,
        )

        text_result = CliRunner().invoke(
            main, ["queue", "-s", "scheduler_unknown"]
        )
        json_result = CliRunner().invoke(
            main, ["queue", "-s", "scheduler_unknown", "--json"]
        )

        assert text_result.exit_code == 0, text_result.output
        assert "unknownphase" in text_result.output
        assert "scheduler_unknown" in text_result.output
        assert "FORGED idle" not in text_result.output
        assert json_result.exit_code == 0, json_result.output
        payload = json.loads(json_result.output)
        assert [row["id"] for row in payload] == ["unknownphase"]
        assert payload[0]["scheduler_state"] == raw_phase
        assert payload[0]["effective_state"] == "scheduler_unknown"
        assert payload[0]["scheduler_running_confirmed"] is False

    def test_unsafe_scheduler_target_cannot_forge_confirmation_or_text(
        self, queue_with_mixed_states: Path
    ) -> None:
        raw_target = "host_c\nFORGED idle\x1b[31m" + ("x" * 150)
        _write_spec(
            paths.queue_dir(),
            "unknowntarget",
            state=JobState.RUNNING,
            scheduler_target=raw_target,
            scheduler_state="running",
        )

        text_result = CliRunner().invoke(
            main, ["queue", "-s", "scheduler_unknown"]
        )
        json_result = CliRunner().invoke(
            main, ["queue", "-s", "scheduler_unknown", "--json"]
        )

        assert text_result.exit_code == 0, text_result.output
        assert "unknowntarget" in text_result.output
        assert "scheduler-target-unknown" in text_result.output
        assert "FORGED idle" not in text_result.output
        assert json_result.exit_code == 0, json_result.output
        payload = json.loads(json_result.output)
        assert [row["id"] for row in payload] == ["unknowntarget"]
        assert payload[0]["scheduler_target"] == raw_target
        assert payload[0]["effective_state"] == "scheduler_unknown"
        assert payload[0]["scheduler_running_confirmed"] is False
        assert payload[0]["queue_handle"]["host"] == "localhost"

    def test_scheduler_phase_cannot_collide_with_lifecycle_filter(
        self, queue_with_mixed_states: Path
    ) -> None:
        _write_spec(
            paths.queue_dir(),
            "phasecollision",
            state=JobState.RUNNING,
            scheduler_target="host_c",
            scheduler_state="completed",
        )

        completed_result = CliRunner().invoke(
            main, ["queue", "-s", "completed", "--json"]
        )
        unknown_result = CliRunner().invoke(
            main, ["queue", "-s", "scheduler_unknown", "--json"]
        )

        assert completed_result.exit_code == 0, completed_result.output
        assert [row["id"] for row in json.loads(completed_result.output)] == [
            "id_completed"
        ]
        assert unknown_result.exit_code == 0, unknown_result.output
        assert [row["id"] for row in json.loads(unknown_result.output)] == [
            "phasecollision"
        ]

    def test_active_keeps_unconfirmed_scheduler_rows_visible(
        self, queue_with_mixed_states: Path
    ) -> None:
        qd = paths.queue_dir()
        for jobid, scheduler_state in (
            ("pollfailed01", "poll_failed"),
            ("finishing001", "finishing"),
        ):
            _write_spec(
                qd,
                jobid,
                state=JobState.RUNNING,
                scheduler_target="host_c",
                scheduler_state=scheduler_state,
            )
        _write_spec(qd, "submitting01", state=JobState.SUBMITTING)
        _write_spec(
            qd,
            "unknownsub01",
            state=JobState.SUBMIT_OUTCOME_UNKNOWN,
        )

        result = CliRunner().invoke(main, ["queue", "--active"])

        assert result.exit_code == 0, result.output
        assert "pollfailed01" in result.output
        assert "finishing001" in result.output
        assert "submitting01" in result.output
        assert "unknownsub01" in result.output
        assert "id_completed" not in result.output

    def test_unknown_state_errors_clearly(
        self, queue_with_mixed_states: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue", "-s", "garbled"])
        assert result.exit_code != 0
        assert "unknown state" in result.output
        # Should list valid states so the user can correct
        assert "running" in result.output

    def test_filter_with_no_matches_returns_no_jobs(
        self, queue_with_mixed_states: Path
    ) -> None:
        """A valid state with zero jobs returns the "(no jobs)" line."""
        # Remove the killed spec so -s killed yields zero matches.
        for path in paths.queue_dir().glob("id_killed*"):
            path.unlink()
        result = CliRunner().invoke(main, ["queue", "-s", "killed"])
        assert result.exit_code == 0
        assert "(no jobs)" in result.output

    def test_help_mentions_state_and_active(self) -> None:
        result = CliRunner().invoke(main, ["queue", "--help"])
        assert result.exit_code == 0
        assert "--state" in result.output or "-s" in result.output
        assert "--active" in result.output

    def test_help_lists_valid_state_names(self) -> None:
        """The help text should enumerate valid state names so users
        don't have to guess (or grep the source)."""
        result = CliRunner().invoke(main, ["queue", "--help"])
        # At least the common ones should be named.
        for state in (
            "running",
            "pending",
            "completed",
            "poll_failed",
            "finishing",
        ):
            assert state in result.output


# ----------------------------------------------------------------------
# v0.5.33: vq queue hides archived jobs by default; --show-archived opts in
# ----------------------------------------------------------------------


@pytest.fixture
def queue_with_archived_and_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """A queue with one live COMPLETED job and one archived COMPLETED job.

    Two-COMPLETED scenario isolates the archived-filter from the state
    filter — both jobs are in the same state, so any difference in
    visibility under ``vq queue`` is solely the archived flag at work.
    """
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
    )
    qd = paths.queue_dir()
    qd.mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    _write_spec(qd, "alivejob", state=JobState.COMPLETED)
    _write_spec(
        qd,
        "archedjob",
        state=JobState.COMPLETED,
        archived_at="2026-05-03T00:00:00+00:00",
        archive_path="/tmp/archedjob.tar.bz2",
    )
    return tmp_path


class TestQueueArchivedFilter:
    """v0.5.33: archived jobs are hidden from `vq queue` by default;
    `--show-archived` opts back in."""

    def test_default_hides_archived(
        self, queue_with_archived_and_live: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue"])
        assert result.exit_code == 0
        assert "alivejob" in result.output, "live job missing from default listing"
        assert "archedjob" not in result.output, (
            "archived job leaked into default listing — should be hidden"
        )
        # The "(archived)" annotation should not appear either when
        # there are no archived rows in the table.
        assert "(archived)" not in result.output

    def test_show_archived_includes_archived(
        self, queue_with_archived_and_live: Path
    ) -> None:
        result = CliRunner().invoke(main, ["queue", "--show-archived"])
        assert result.exit_code == 0
        assert "alivejob" in result.output
        assert "archedjob" in result.output
        # And the annotation is present so the user can spot which is which.
        assert "(archived)" in result.output

    def test_show_archived_only_archived_via_state_filter(
        self, queue_with_archived_and_live: Path
    ) -> None:
        """`--show-archived` composes with `-s` so you can filter to
        just completed-AND-archived if you want to."""
        result = CliRunner().invoke(
            main, ["queue", "--show-archived", "-s", "completed"]
        )
        assert result.exit_code == 0
        # Both are COMPLETED so both should appear.
        assert "alivejob" in result.output
        assert "archedjob" in result.output

    def test_state_filter_without_show_archived_still_hides_archived(
        self, queue_with_archived_and_live: Path
    ) -> None:
        """An explicit -s on the same state as an archived job still
        hides the archived row — the user filtered by state, not by
        "show me the historical bin." Belt-and-braces invariant."""
        result = CliRunner().invoke(main, ["queue", "-s", "completed"])
        assert result.exit_code == 0
        assert "alivejob" in result.output
        assert "archedjob" not in result.output

    def test_help_mentions_show_archived(self) -> None:
        result = CliRunner().invoke(main, ["queue", "--help"])
        assert result.exit_code == 0
        assert "--show-archived" in result.output


# ----------------------------------------------------------------------
# v0.5.34: NAME column appears conditionally (same policy as v0.5.29 PRI)
# ----------------------------------------------------------------------


class TestFormatTableNameColumn:
    """v0.5.34: NAME column appears in ``vq queue`` listing iff at least
    one spec has ``job_name`` set. Same zero-noise-when-unused policy
    as v0.5.29's PRI column."""

    def test_no_name_column_when_no_specs_have_names(self) -> None:
        specs = [
            JobSpec(id=f"j{i}", command=["true"], cwd=f"/tmp/{i}", cpus=1)
            for i in range(2)
        ]
        table = format_table(specs)
        header = table.splitlines()[0]
        assert "NAME" not in header, (
            "NAME column leaked into the unnamed-only listing"
        )

    def test_name_column_appears_when_any_spec_has_name(self) -> None:
        specs = [
            JobSpec(id="anon", command=["true"], cwd="/tmp/a", cpus=1),
            JobSpec(
                id="named",
                command=["true"],
                cwd="/tmp/b",
                cpus=1,
                job_name="mgo-pbe",
            ),
        ]
        table = format_table(specs)
        header = table.splitlines()[0]
        assert "NAME" in header
        # The named job's name appears in its row.
        assert "mgo-pbe" in table
        # The unnamed job's NAME column is blank (not the literal "None").
        assert "None" not in table

    def test_name_truncated_when_long(self) -> None:
        """The NAME column is truncated to keep total row width readable
        on a 132-col terminal. 20-char budget + ellipsis."""
        # 30 chars: longer than the 20-char column budget.
        long_name = "a-very-long-jobname-30-chars-X"
        specs = [
            JobSpec(
                id="lng",
                command=["true"],
                cwd="/tmp/lng",
                cpus=1,
                job_name=long_name,
            ),
        ]
        table = format_table(specs)
        # The full name should NOT appear (truncated).
        assert long_name not in table
        # An ellipsis form should be present.
        assert "..." in table

    def test_name_column_ordering_id_name_state(self) -> None:
        """NAME sits between ID and STATE (both are 'what is this job'
        identifiers from the user's perspective). Same row as ID."""
        spec = JobSpec(
            id="x",
            command=["true"],
            cwd="/tmp",
            cpus=1,
            job_name="myname",
        )
        table = format_table([spec])
        header = table.splitlines()[0]
        # Order: ID, NAME, STATE
        id_pos = header.index("ID")
        name_pos = header.index("NAME")
        state_pos = header.index("STATE")
        assert id_pos < name_pos < state_pos


class TestPendingQueuePosition:
    """v0.9.2: rank a PENDING job by the daemon's (-priority, submitted_at)
    dispatch order."""

    def _p(
        self,
        jobid: str,
        *,
        priority: int = 0,
        submitted_at: str,
        scheduler_target: str | None = None,
    ) -> JobSpec:
        return JobSpec(
            id=jobid, command=["x"], cwd="/tmp", cpus=1,
            state=JobState.PENDING, priority=priority, submitted_at=submitted_at,
            scheduler_target=scheduler_target,
        )

    def test_ranks_by_priority_then_submit_time(self) -> None:
        from vq.listing import pending_queue_position

        a = self._p("a", submitted_at="2026-06-05T10:00:00+00:00")
        b = self._p("b", submitted_at="2026-06-05T11:00:00+00:00")
        c = self._p("c", priority=5, submitted_at="2026-06-05T09:00:00+00:00")
        pending = [a, b, c]
        # c (priority 5) is first; then a (earlier submit) then b.
        assert pending_queue_position(c, pending) == (1, 3)
        assert pending_queue_position(a, pending) == (2, 3)
        assert pending_queue_position(b, pending) == (3, 3)

    def test_single_pending_is_position_one(self) -> None:
        from vq.listing import pending_queue_position

        a = self._p("a", submitted_at="2026-06-05T10:00:00+00:00")
        assert pending_queue_position(a, [a]) == (1, 1)

    def test_scheduler_targets_have_independent_pending_lanes(self) -> None:
        from vq.listing import pending_queue_position

        itwin = self._p(
            "itwin", submitted_at="2026-06-05T10:00:00+00:00",
            scheduler_target="host_f-itwin",
        )
        itwin_later = self._p(
            "itwin2", submitted_at="2026-06-05T11:00:00+00:00",
            scheduler_target="host_f-itwin",
        )
        big = self._p(
            "big", submitted_at="2026-06-05T08:00:00+00:00",
            scheduler_target="host_f-big",
        )
        local = self._p("local", submitted_at="2026-06-05T07:00:00+00:00")

        pending = [big, local, itwin_later, itwin]

        assert pending_queue_position(itwin, pending) == (1, 2)
        assert pending_queue_position(itwin_later, pending) == (2, 2)
        assert pending_queue_position(big, pending) == (1, 1)
        assert pending_queue_position(local, pending) == (1, 1)
