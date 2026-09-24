"""Issue #23: the queue/list surface reports why a job has not started.

`vq status` has carried the scheduler's own explanation since the vibe-qc#148
work, and the queue JSON has carried it too (it is a `JobSpec` field, so
`model_dump` picks it up). The *human* `vq queue` / `vq list` table did not:
five jobs blocked behind one down node rendered as five undifferentiated
`queued` rows, which is the exact display that let the original starvation run
for six days.

The reason strings here are not invented. They are parsed out of the same
canned scheduler output the dialect layer is tested against, so what the table
renders is tied to what Torque and Slurm actually emit.
"""
from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from vq import paths
from vq.cli import main
from vq.listing import format_table
from vq.scheduler_dialect import SlurmDialect, TorqueDialect
from vq.spec import JobSpec, JobState

# A Torque `qstat -f` record for a job that cannot be placed, folded onto a
# tab-continued line exactly as Torque wraps a long value.
_QSTAT_F_UNSCHEDULABLE = (
    "Job Id: 12345.host_f\n"
    "    Job_Name = runjob\n"
    "    job_state = Q\n"
    "    queue = compute\n"
    "    Resource_List.nodes = 1:ppn=128\n"
    "    comment = Not Running: Not enough of the right type of nodes are availab\n"
    "\tle to run the job\n"
)

# A Slurm poll row: `%i|%T|%M|%l|%R|%r`. Only PENDING rows carry a reason.
_SQUEUE_TABLE = (
    "123|RUNNING|00:01|01:00:00|node001|None\n"
    "124|PENDING|00:00|01:00:00|(Priority)|Priority\n"
)


def _pbs_reason() -> str:
    """The verbatim Torque comment, straight out of the dialect parser."""
    detail = TorqueDialect().parse_qstat_detail(_QSTAT_F_UNSCHEDULABLE)
    reason = detail["12345.host_f"].queued_reason
    assert reason is not None
    return reason


def _slurm_reason() -> str:
    """The verbatim Slurm `Reason`, straight out of the dialect parser."""
    return SlurmDialect().parse_poll_reasons(_SQUEUE_TABLE)["124"]


_HEADING = "not started, in the scheduler's own words:"


def _reason_block(table: str) -> list[str]:
    """The footnote's own lines, without the occupancy footer beneath it."""
    assert _HEADING in table, table
    tail = table.split(_HEADING + "\n")[1]
    lines: list[str] = []
    for line in tail.splitlines():
        if not line.strip():
            break
        lines.append(line)
    return lines


def _queued(jobid: str, reason: str | None, **fields: object) -> JobSpec:
    """A scheduler reservation that the cluster has not started."""
    base: dict[str, object] = {
        "id": jobid,
        "command": ["true"],
        "cwd": "/tmp/" + jobid,
        "cpus": 128,
        "state": JobState.RUNNING,
        "scheduler_target": "pbs-cluster",
        "scheduler_state": "queued",
        "scheduler_job_id": f"{jobid}.host_f",
        "scheduler_queued_reason": reason,
    }
    base.update(fields)
    return JobSpec(**base)


class TestHumanTable:
    def test_pbs_reason_is_reported_verbatim(self) -> None:
        reason = _pbs_reason()
        table = format_table([_queued("j1", reason)])
        assert reason in table
        assert f"j1: {reason}" in table

    def test_slurm_reason_is_reported_verbatim(self) -> None:
        reason = _slurm_reason()
        table = format_table([_queued("j1", reason, scheduler_target="slurm-cluster")])
        assert f"j1: {reason}" in table

    def test_reason_is_a_footnote_not_a_column(self) -> None:
        """The table's aligned rows must not move.

        A verbatim comment is as wide as a sentence; putting it in a cell
        would push COMMAND off any terminal. The header and the job's own row
        are therefore identical with and without a reason.
        """
        plain = format_table([_queued("j1", None)])
        annotated = format_table([_queued("j1", _pbs_reason())])
        assert annotated.splitlines()[:2] == plain.splitlines()[:2]
        assert "REASON" not in annotated.splitlines()[0]
        # ...and the block really is below the row it explains.
        assert annotated.splitlines().index(_HEADING) > 1

    def test_a_reason_wider_than_the_table_is_not_fitted_to_a_column(self) -> None:
        """Torque puts the distinguishing detail at the end of the comment.

        A fixed-width cell would cut off precisely the part worth reading, so
        the footnote carries the whole string even though it is wider than
        every column in the table above it.
        """
        reason = _pbs_reason()
        assert len(reason) > 60, "fixture no longer exercises a wide value"
        table = format_table([_queued("j1", reason)])
        assert reason in table
        assert "truncated" not in table
        # The footnote outgrows every aligned row, which is the whole reason
        # it is a footnote.
        rows = [line for line in table.splitlines() if line.startswith(("ID ", "j1 "))]
        assert len(_reason_block(table)[0]) > max(len(row) for row in rows)

    def test_remote_free_text_is_bounded_before_it_reaches_a_terminal(self) -> None:
        """A reason is free text from a remote scheduler, like every other
        poll diagnostic this codebase refuses to interpolate raw."""
        block = _reason_block(format_table([_queued("j1", "Not Running: " + "x" * 400)]))
        assert len(block) == 1
        assert "truncated" in block[0]
        assert "`vq status` has it whole" in block[0]
        assert len(block[0]) < 400

    def test_control_characters_cannot_reshape_the_block(self) -> None:
        """A newline in the value would otherwise forge a second entry, and
        an escape sequence would reach the terminal."""
        table = format_table(
            [_queued("j1", "Not Running:\n  j2: forged\x1b[31m entry")]
        )
        assert _reason_block(table) == ["  j1: Not Running: j2: forged [31m entry"]
        assert "\x1b" not in table

    def test_json_is_never_abridged(self) -> None:
        """The bound is a rendering choice; the contract carries the field
        whole, which is what a consumer that needs it byte for byte reads."""
        raw = "Not Running: " + "x" * 400
        row = _queued("j1", raw).model_dump(mode="json")
        assert row["scheduler_queued_reason"] == raw

    def test_one_line_per_reason_names_every_blocked_job(self) -> None:
        """The #23 shape: several jobs, one condition, said once."""
        reason = _pbs_reason()
        specs = [_queued(f"j{n}", reason) for n in range(1, 6)]
        block = _reason_block(format_table(specs))
        assert block == [f"  j1, j2, j3, j4, j5: {reason}"]

    def test_reasons_are_ordered_by_how_many_rows_they_hold(self) -> None:
        busy = _pbs_reason()
        held = _slurm_reason()
        specs = [_queued("j1", held)] + [_queued(f"j{n}", busy) for n in range(2, 5)]
        block = _reason_block(format_table(specs))
        assert busy in block[0]
        assert held in block[1]

    def test_many_ids_are_capped_with_a_remainder(self) -> None:
        reason = _pbs_reason()
        specs = [_queued(f"j{n:02d}", reason) for n in range(20)]
        block = _reason_block(format_table(specs))
        assert block == [
            "  j00, j01, j02, j03, j04, j05, j06, j07, +12 more: " + reason
        ]

    def test_many_distinct_reasons_are_capped_with_a_remainder(self) -> None:
        """A scheduler that appends an estimated start time per job.

        Then every reason is distinct, and an uncapped block would be longer
        than the table it annotates.
        """
        specs = [
            _queued(f"j{n:02d}", f"Not Running: estimated start slot {n:02d}")
            for n in range(12)
        ]
        block = _reason_block(format_table(specs))
        assert len(block) == 9  # 8 reasons + the remainder line
        assert block[-1] == (
            "  (+4 further reason(s); "
            "`vq status HOST JOBID` reports one job's in full)"
        )

    def test_a_terminal_row_does_not_explain_why_it_is_waiting(self) -> None:
        """The daemon clears the field on leaving PENDING, but an archived or
        failed row can still carry the last value it was given."""
        table = format_table(
            [_queued("j1", _pbs_reason(), state=JobState.FAILED)]
        )
        assert _HEADING not in table

    def test_a_listing_with_no_reasons_is_unchanged(self) -> None:
        """Zero noise for a local queue: no heading, no trailing blank line."""
        local = JobSpec(id="j1", command=["true"], cwd="/tmp/j1", cpus=1)
        table = format_table([local])
        assert "not started" not in table
        assert table.splitlines()[-1].startswith("j1")

    def test_an_empty_listing_stays_empty(self) -> None:
        assert format_table([]) == "(no jobs)"


class TestCliSurface:
    """Both spellings of the verb, human and `--json`."""

    def _write(self, reason: str) -> None:
        queue = paths.queue_dir()
        queue.mkdir(parents=True, exist_ok=True)
        spec = _queued("j1", reason)
        spec.write(queue / "j1.json")

    def test_vq_queue_human_reports_the_reason(self, tmp_path: Path) -> None:
        reason = _pbs_reason()
        self._write(reason)
        result = CliRunner().invoke(main, ["queue", "localhost"])
        assert result.exit_code == 0
        assert reason in result.output

    def test_vq_list_human_reports_the_reason(self, tmp_path: Path) -> None:
        """`list` is a name-level alias for `queue`; #23 names both."""
        reason = _pbs_reason()
        self._write(reason)
        result = CliRunner().invoke(main, ["list", "localhost"])
        assert result.exit_code == 0
        assert reason in result.output

    def test_vq_queue_json_carries_the_field(self, tmp_path: Path) -> None:
        reason = _pbs_reason()
        self._write(reason)
        result = CliRunner().invoke(main, ["queue", "localhost", "--json"])
        assert result.exit_code == 0
        rows = json.loads(result.output)
        assert rows[0]["scheduler_queued_reason"] == reason

    def test_vq_status_json_carries_the_same_field(self, tmp_path: Path) -> None:
        """The agent contract promises one spelling across both verbs."""
        reason = _pbs_reason()
        self._write(reason)
        result = CliRunner().invoke(main, ["status", "localhost", "j1", "--json"])
        assert result.exit_code == 0
        assert json.loads(result.output)["scheduler_queued_reason"] == reason
