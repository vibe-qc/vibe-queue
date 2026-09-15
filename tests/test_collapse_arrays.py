"""v0.7.10 *McCarthy's List* — `vq queue --collapse-arrays` tests.

Pins the invariants for the array-row fold:

1. **Helper** — `_array_state_summary` produces stable compact
   breakdowns: single-state ``N/M done`` or ``N/M S``; mixed
   ``5P/25C/30`` with a deterministic letter order.
2. **Table** — `format_table(collapse_arrays=True)` renders one
   row per array_group_id; non-array specs unchanged in the same
   table; column widths still balance.
3. **CLI** — `vq queue --collapse-arrays` produces the folded
   table; without the flag the per-element table renders.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.listing import (
    _STATE_LETTER,
    _array_state_summary,
    format_table,
)
from vq.spec import JobSpec, JobState


@pytest.fixture
def cli_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


def _mk_spec(
    jobid: str,
    *,
    state: JobState = JobState.PENDING,
    array_index: int | None = None,
    array_total: int | None = None,
    array_group_id: str | None = None,
    job_name: str | None = None,
    command: list[str] | None = None,
    scheduler_target: str | None = None,
    scheduler_state: str | None = None,
) -> JobSpec:
    return JobSpec(
        id=jobid,
        command=command or ["python", "run.py"],
        cwd="/tmp",
        cpus=2,
        submitter="x@y",
        state=state,
        submitted_at="2026-05-27T10:00:00+00:00",
        array_index=array_index,
        array_total=array_total,
        array_group_id=array_group_id,
        job_name=job_name,
        scheduler_target=scheduler_target,
        scheduler_state=scheduler_state,
    )


# ----------------------------------------------------------------------
# 1. _array_state_summary
# ----------------------------------------------------------------------


class TestArrayStateSummary:
    def test_all_pending(self) -> None:
        elems = [
            _mk_spec(f"{i:012x}", state=JobState.PENDING) for i in range(30)
        ]
        assert _array_state_summary(elems) == "30/30 P"

    def test_all_completed_says_done(self) -> None:
        elems = [
            _mk_spec(f"{i:012x}", state=JobState.COMPLETED) for i in range(30)
        ]
        assert _array_state_summary(elems) == "30/30 done"

    def test_mixed_states_breakdown(self) -> None:
        elems = (
            [_mk_spec(f"a{i:011x}", state=JobState.PENDING) for i in range(5)]
            + [_mk_spec(f"b{i:011x}", state=JobState.RUNNING) for i in range(3)]
            + [_mk_spec(f"c{i:011x}", state=JobState.COMPLETED) for i in range(22)]
        )
        # Letter order: P, R, C → "5P/3R/22C/30"
        assert _array_state_summary(elems) == "5P/3R/22C/30"

    def test_failed_and_completed(self) -> None:
        elems = (
            [_mk_spec(f"a{i:011x}", state=JobState.COMPLETED) for i in range(28)]
            + [_mk_spec(f"b{i:011x}", state=JobState.FAILED) for i in range(2)]
        )
        assert _array_state_summary(elems) == "28C/2F/30"

    def test_scheduler_phases_do_not_collapse_to_running(self) -> None:
        elems = (
            [
                _mk_spec(
                    f"a{i:011x}",
                    state=JobState.RUNNING,
                    scheduler_target="host_c",
                    scheduler_state="running",
                )
                for i in range(2)
            ]
            + [
                _mk_spec(
                    f"b{i:011x}",
                    state=JobState.RUNNING,
                    scheduler_target="host_c",
                    scheduler_state="poll_failed",
                )
                for i in range(3)
            ]
            + [
                _mk_spec(
                    "c00000000000",
                    state=JobState.RUNNING,
                    scheduler_target="host_c",
                    scheduler_state="finishing",
                )
            ]
        )

        assert _array_state_summary(elems) == "2R/3PF/1FN/6"

    def test_submitting_and_scheduler_unpolled_have_distinct_tokens(self) -> None:
        elems = [
            _mk_spec("submitting01", state=JobState.SUBMITTING),
            _mk_spec(
                "unpolled0001",
                state=JobState.RUNNING,
                scheduler_target="host_c",
            ),
        ]

        assert _array_state_summary(elems) == "1U/1UP/2"

    def test_future_scheduler_phase_remains_visible(self) -> None:
        elems = [
            _mk_spec(
                "running00001",
                state=JobState.RUNNING,
                scheduler_target="host_c",
                scheduler_state="running",
            ),
            _mk_spec(
                "future000001",
                state=JobState.RUNNING,
                scheduler_target="host_c",
                scheduler_state="future_phase",
            ),
        ]

        assert _array_state_summary(elems) == "1R/1SU/2"

    def test_known_letters_for_all_states(self) -> None:
        """Every state in JobState should map to a letter so the
        breakdown can't silently drop categories on a future
        addition. If this test fails after a new JobState is added,
        extend _STATE_LETTER + _STATE_RENDER_ORDER."""
        for state in JobState:
            assert state in _STATE_LETTER, f"missing letter for {state}"


# ----------------------------------------------------------------------
# 2. format_table with collapse_arrays
# ----------------------------------------------------------------------


class TestFormatTableCollapse:
    def test_collapse_off_renders_each_element(self) -> None:
        elems = [
            _mk_spec(
                f"e{i:011x}", state=JobState.COMPLETED,
                array_index=i, array_total=4, array_group_id="grp00001",
            )
            for i in range(4)
        ]
        out = format_table(elems)
        # Per-element rows: 4 jobids should appear.
        for s in elems:
            assert s.id in out
        # The collapsed ARRAY label should not.
        assert "ARRAY" not in out

    def test_collapse_on_yields_single_row_per_group(self) -> None:
        elems = [
            _mk_spec(
                f"e{i:011x}", state=JobState.COMPLETED,
                array_index=i, array_total=4, array_group_id="grp00001",
            )
            for i in range(4)
        ]
        out = format_table(elems, collapse_arrays=True)
        # No per-element jobids visible.
        for s in elems:
            assert s.id not in out
        # The group id IS visible (as the synthetic row's ID).
        assert "grp00001" in out
        # And the state breakdown.
        assert "ARRAY 4/4 done" in out

    def test_collapse_mixed_array_and_non_array(self) -> None:
        non_array = _mk_spec("solo00000001", state=JobState.RUNNING)
        array_elems = [
            _mk_spec(
                f"e{i:011x}", state=(
                    JobState.COMPLETED if i < 3 else JobState.PENDING
                ),
                array_index=i, array_total=4, array_group_id="grp00002",
            )
            for i in range(4)
        ]
        specs = [non_array] + array_elems
        out = format_table(specs, collapse_arrays=True)
        # Non-array spec renders verbatim.
        assert "solo00000001" in out
        # Array group folded.
        assert "grp00002" in out
        assert "ARRAY 1P/3C/4" in out
        # And the per-element ids vanish.
        for s in array_elems:
            assert s.id not in out

    def test_collapse_table_surfaces_mixed_scheduler_phases(self) -> None:
        elems = [
            _mk_spec(
                f"e{i:011x}",
                state=JobState.RUNNING,
                array_index=i,
                array_total=3,
                array_group_id="scheduler-phases",
                scheduler_target="host_c",
                scheduler_state=("running" if i == 0 else "poll_failed"),
            )
            for i in range(3)
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "ARRAY 1R/2PF/3" in out
        assert "host_c:sched=mixed(poll_failed=2,running=1)" in out
        assert "poll_failed=2" in out
        assert "not confirmed running" in out

    def test_collapse_cluster_summary_preserves_terminal_scheduler_phase(
        self,
    ) -> None:
        elems = [
            _mk_spec(
                "live00000001",
                state=JobState.RUNNING,
                array_index=0,
                array_total=2,
                array_group_id="terminal-phase",
                scheduler_target="host_c",
                scheduler_state="poll_failed",
            ),
            _mk_spec(
                "terminal0001",
                state=JobState.FAILED,
                array_index=1,
                array_total=2,
                array_group_id="terminal-phase",
                scheduler_target="host_c",
                scheduler_state="submit_rejected_after_terminal",
            ),
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "ARRAY 1PF/1F/2" in out
        assert (
            "host_c:sched=mixed(poll_failed=1,"
            "submit_rejected_after_terminal=1)"
        ) in out

    def test_collapse_same_phase_does_not_borrow_live_lifecycle_label(
        self,
    ) -> None:
        elems = [
            _mk_spec(
                "live00000001",
                state=JobState.RUNNING,
                array_index=0,
                array_total=2,
                array_group_id="same-phase-mixed-lifecycle",
                scheduler_target="host_f",
                scheduler_state="poll_failed",
            ),
            _mk_spec(
                "terminal0001",
                state=JobState.FAILED,
                array_index=1,
                array_total=2,
                array_group_id="same-phase-mixed-lifecycle",
                scheduler_target="host_f",
                scheduler_state="poll_failed",
            ),
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "ARRAY 1PF/1F/2" in out
        assert "host_f:vq=mixed,sched=poll_failed" in out
        assert "host_f:vq=running,sched=poll_failed" not in out

    def test_collapse_cluster_summary_does_not_hide_mixed_targets(self) -> None:
        elems = [
            _mk_spec(
                "local0000001",
                state=JobState.RUNNING,
                array_index=0,
                array_total=2,
                array_group_id="mixed-target",
            ),
            _mk_spec(
                "remote000001",
                state=JobState.RUNNING,
                array_index=1,
                array_total=2,
                array_group_id="mixed-target",
                scheduler_target="host_c",
                scheduler_state="poll_failed",
            ),
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "CLUSTER" in out.splitlines()[0]
        assert "mixed-targets:sched=unknown" in out

    def test_collapse_does_not_merge_valid_display_token_with_invalid_target(
        self,
    ) -> None:
        elems = [
            _mk_spec(
                "sentinel0001",
                state=JobState.RUNNING,
                array_index=0,
                array_total=2,
                array_group_id="sentinel-target",
                scheduler_target="scheduler-target-unknown",
                scheduler_state="running",
            ),
            _mk_spec(
                "invalid00001",
                state=JobState.RUNNING,
                array_index=1,
                array_total=2,
                array_group_id="sentinel-target",
                scheduler_target="",
                scheduler_state="running",
            ),
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "mixed-targets:sched=unknown" in out

    def test_collapse_keeps_shared_literal_display_token_target(self) -> None:
        elems = [
            _mk_spec(
                f"sentinel{i:04d}",
                state=JobState.RUNNING,
                array_index=i,
                array_total=2,
                array_group_id="shared-sentinel-target",
                scheduler_target="scheduler-target-unknown",
                scheduler_state="running",
            )
            for i in range(2)
        ]

        out = format_table(elems, collapse_arrays=True)

        assert "scheduler-target-unknown:vq=running,sched=running" in out
        assert "scheduler occupancy: 2/2" in out

    def test_collapse_multiple_groups(self) -> None:
        groupA = [
            _mk_spec(
                f"a{i:011x}", state=JobState.COMPLETED,
                array_index=i, array_total=5, array_group_id="grpAAAAA",
            )
            for i in range(5)
        ]
        groupB = [
            _mk_spec(
                f"b{i:011x}", state=JobState.FAILED,
                array_index=i, array_total=3, array_group_id="grpBBBBB",
            )
            for i in range(3)
        ]
        out = format_table(groupA + groupB, collapse_arrays=True)
        # Both groups present as folded rows.
        assert "grpAAAAA" in out
        assert "grpBBBBB" in out
        assert "ARRAY 5/5 done" in out
        assert "ARRAY 3/3 F" in out

    def test_collapse_preserves_name_and_priority_columns(self) -> None:
        """An array element with job_name set still drives the
        optional NAME column on the folded row."""
        elems = [
            _mk_spec(
                f"e{i:011x}", state=JobState.COMPLETED,
                array_index=i, array_total=2, array_group_id="grp00003",
                job_name="my-experiment",
            )
            for i in range(2)
        ]
        out = format_table(elems, collapse_arrays=True)
        assert "my-experiment" in out
        # Header should carry the NAME column.
        assert "NAME" in out


# ----------------------------------------------------------------------
# 3. CLI
# ----------------------------------------------------------------------


class TestCliCollapseArrays:
    def _submit_array(
        self, cli_state: Path, n: int = 3,
    ) -> tuple[list[str], str]:
        """Submit a --array N to localhost. Returns (jobids, gid)."""
        f = cli_state / "in.py"
        f.write_text("pass")
        result = CliRunner().invoke(
            main, ["submit", "localhost", "--array", str(n), str(f)],
        )
        assert result.exit_code == 0, result.output
        jobids = result.output.strip().split("\n")
        # All elements share a group_id. Pull it from any spec.
        spec = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        assert spec.array_group_id is not None
        return jobids, spec.array_group_id

    def test_collapse_flag_folds_into_one_row(
        self, cli_state: Path
    ) -> None:
        jobids, gid = self._submit_array(cli_state, n=4)
        result = CliRunner().invoke(
            main, ["queue", "localhost", "--collapse-arrays"],
        )
        assert result.exit_code == 0, result.output
        # Per-element jobids should not appear.
        for jid in jobids:
            assert jid not in result.output
        # The group id should.
        assert gid in result.output
        assert "ARRAY 4/4" in result.output

    def test_default_renders_each_element(
        self, cli_state: Path
    ) -> None:
        jobids, _ = self._submit_array(cli_state, n=4)
        result = CliRunner().invoke(main, ["queue", "localhost"])
        assert result.exit_code == 0, result.output
        for jid in jobids:
            assert jid in result.output
        assert "ARRAY" not in result.output

    def test_collapse_combines_with_state_filter(
        self, cli_state: Path
    ) -> None:
        """--collapse-arrays applies AFTER state filtering, so
        the breakdown reflects only the filtered elements."""
        jobids, gid = self._submit_array(cli_state, n=4)
        # Force two of the array elements into FAILED.
        for jid in jobids[:2]:
            sp = JobSpec.read(paths.queue_dir() / f"{jid}.json")
            sp.state = JobState.FAILED
            sp.finished_at = "2026-05-27T11:00:00+00:00"
            sp.exit_code = 1
            sp.write(paths.queue_dir() / f"{jid}.json")
        result = CliRunner().invoke(
            main,
            ["queue", "localhost", "--collapse-arrays", "-s", "failed"],
        )
        assert result.exit_code == 0, result.output
        # Breakdown reflects the filtered subset: 2 failed out of
        # 2 filtered elements.
        assert "ARRAY 2/2 F" in result.output
