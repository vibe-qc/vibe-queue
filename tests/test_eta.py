"""Tests for queue ETA estimates from retained job history."""
from __future__ import annotations

import pytest

from vq import eta
from vq.spec import JobSpec, JobState


def _spec(
    jobid: str,
    *,
    cpus: int = 1,
    state: JobState = JobState.COMPLETED,
    submitted_at: str = "2026-06-30T09:00:00+00:00",
    started_at: str | None = "2026-06-30T10:00:00+00:00",
    finished_at: str | None = "2026-06-30T11:00:00+00:00",
    tags: list[str] | None = None,
    command: list[str] | None = None,
    **fields: object,
) -> JobSpec:
    base: dict[str, object] = {
        "id": jobid,
        "command": command or ["python", "job.py"],
        "cwd": f"/tmp/{jobid}",
        "cpus": cpus,
        "state": state,
        "submitted_at": submitted_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "tags": tags or [],
    }
    base.update(fields)
    return JobSpec(**base)


class TestJobDurationEstimate:
    def test_prefers_tag_command_cpus_history(self) -> None:
        target = _spec(
            "pending",
            state=JobState.PENDING,
            cpus=4,
            tags=["paper"],
            finished_at=None,
        )
        history = [
            _spec(
                "match",
                cpus=4,
                tags=["paper"],
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T12:00:00+00:00",
            ),
            _spec(
                "broad",
                cpus=4,
                tags=[],
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T20:00:00+00:00",
            ),
        ]

        estimate = eta.estimate_job_duration(target, history)

        assert estimate is not None
        assert estimate.source == "tag+command+cpus"
        assert estimate.samples == 1
        assert estimate.seconds == pytest.approx(7200)

    def test_falls_back_to_cpus_history(self) -> None:
        target = _spec(
            "pending",
            state=JobState.PENDING,
            cpus=8,
            command=["python", "new.py"],
            finished_at=None,
        )
        history = [
            _spec(
                "old",
                cpus=8,
                command=["python", "other.py"],
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T10:30:00+00:00",
            )
        ]

        estimate = eta.estimate_job_duration(target, history)

        assert estimate is not None
        assert estimate.source == "cpus"
        assert estimate.seconds == pytest.approx(1800)

    def test_ignores_non_completed_history(self) -> None:
        target = _spec("pending", state=JobState.PENDING, finished_at=None)
        history = [
            _spec(
                "running",
                state=JobState.RUNNING,
                finished_at=None,
                scheduler_target="host_f",
                scheduler_walltime_used="02:00:00",
            )
        ]

        assert eta.estimate_job_duration(target, history) is None

    def test_scheduler_history_prefers_reported_walltime(self) -> None:
        target = _spec(
            "pending",
            state=JobState.PENDING,
            scheduler_target="host_f",
            finished_at=None,
        )
        history = [
            _spec(
                "done",
                scheduler_target="host_f",
                scheduler_walltime_used="00:20:00",
                started_at="2026-06-30T00:00:00+00:00",
                finished_at="2026-06-30T10:00:00+00:00",
            )
        ]

        estimate = eta.estimate_job_duration(target, history)

        assert estimate is not None
        assert estimate.seconds == pytest.approx(1200)


class TestPendingWaitEstimate:
    def test_sums_jobs_ahead_only(self) -> None:
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
        )
        ahead = _spec(
            "ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
        )
        behind = _spec(
            "behind",
            state=JobState.PENDING,
            submitted_at="2026-06-30T13:00:00+00:00",
            finished_at=None,
        )
        history = [
            _spec(
                "done",
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T11:30:00+00:00",
            )
        ]

        estimate = eta.estimate_pending_wait(target, [ahead, target, behind], history)

        assert estimate is not None
        assert estimate.jobs_ahead == 1
        assert estimate.seconds == pytest.approx(5400)
        assert estimate.source_counts == {"command+cpus": 1}

    def test_unknown_history_makes_eta_unavailable(self) -> None:
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
        )
        ahead = _spec(
            "ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
        )

        assert eta.estimate_pending_wait(target, [ahead, target], []) is None

    def test_first_pending_job_has_zero_eta(self) -> None:
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
        )

        estimate = eta.estimate_pending_wait(target, [target], [])

        assert estimate is not None
        assert estimate.seconds == 0
        assert eta.format_eta_duration(estimate.seconds) == "0s"

    def test_generic_single_sample_source_is_flagged_weak(self) -> None:
        """IID 293: one command-shape sample must not read as a confident ETA."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
        )
        ahead = _spec(
            "ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
        )
        history = [
            _spec(
                "done",
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T11:30:00+00:00",
            )
        ]

        estimate = eta.estimate_pending_wait(target, [ahead, target], history)

        assert estimate is not None
        assert estimate.weak_sources == {"command+cpus": 1}

    def test_tag_matched_single_sample_is_not_weak(self) -> None:
        """IID 293: a tag-informed match identifies the work, even at one sample."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
            tags=["paper"],
        )
        ahead = _spec(
            "ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
            tags=["paper"],
        )
        history = [
            _spec(
                "done",
                tags=["paper"],
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T11:30:00+00:00",
            )
        ]

        estimate = eta.estimate_pending_wait(target, [ahead, target], history)

        assert estimate is not None
        assert estimate.source_counts == {"tag+command+cpus": 1}
        assert estimate.weak_sources == {}

    def test_three_generic_samples_are_not_weak(self) -> None:
        """IID 293: three matching samples lift a generic source past the flag."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
        )
        ahead = _spec(
            "ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
        )
        history = [
            _spec(
                f"done{i}",
                started_at=f"2026-06-30T{8 + i:02d}:00:00+00:00",
                finished_at=f"2026-06-30T{9 + i:02d}:00:00+00:00",
            )
            for i in range(3)
        ]

        estimate = eta.estimate_pending_wait(target, [ahead, target], history)

        assert estimate is not None
        assert estimate.source_counts == {"command+cpus": 1}
        assert estimate.weak_sources == {}

    def test_mixed_weak_and_reliable_sources_are_distinguished(self) -> None:
        """IID 293: weak generic history beside a tag-informed match is flagged."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T13:00:00+00:00",
            finished_at=None,
        )
        generic_ahead = _spec(
            "generic-ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T11:00:00+00:00",
            finished_at=None,
        )
        tagged_ahead = _spec(
            "tagged-ahead",
            state=JobState.PENDING,
            submitted_at="2026-06-30T12:00:00+00:00",
            finished_at=None,
            tags=["paper"],
        )
        history = [
            _spec(
                "generic-done",
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T11:00:00+00:00",
            ),
            _spec(
                "tagged-done",
                tags=["paper"],
                started_at="2026-06-30T10:00:00+00:00",
                finished_at="2026-06-30T11:00:00+00:00",
            ),
        ]

        estimate = eta.estimate_pending_wait(
            target, [generic_ahead, tagged_ahead, target], history
        )

        assert estimate is not None
        assert estimate.weak_sources == {"command+cpus": 2}
        assert set(estimate.source_counts) == {"command+cpus", "tag+command+cpus"}

    def test_reusing_one_generic_sample_for_three_jobs_stays_weak(self) -> None:
        """IID 293: repeated consumers do not multiply one retained row."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T14:00:00+00:00",
            finished_at=None,
        )
        ahead = [
            _spec(
                f"ahead{i}",
                state=JobState.PENDING,
                submitted_at=f"2026-06-30T{10 + i:02d}:00:00+00:00",
                finished_at=None,
            )
            for i in range(3)
        ]
        history = [_spec("only-retained-row")]

        estimate = eta.estimate_pending_wait(target, [*ahead, target], history)

        assert estimate is not None
        assert estimate.seconds == pytest.approx(3 * 3600)
        assert estimate.jobs_ahead == 3
        assert estimate.source_counts == {"command+cpus": 3}
        assert estimate.sample_count == 1
        assert estimate.weak_sources == {"command+cpus": 1}

    def test_reusing_two_generic_samples_for_two_jobs_stays_weak(self) -> None:
        """IID 293: two retained rows remain N=2 however often reused."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T13:00:00+00:00",
            finished_at=None,
        )
        ahead = [
            _spec(
                f"ahead{i}",
                state=JobState.PENDING,
                submitted_at=f"2026-06-30T{10 + i:02d}:00:00+00:00",
                finished_at=None,
            )
            for i in range(2)
        ]
        history = [_spec("retained-a"), _spec("retained-b")]

        estimate = eta.estimate_pending_wait(target, [*ahead, target], history)

        assert estimate is not None
        assert estimate.seconds == pytest.approx(2 * 3600)
        assert estimate.jobs_ahead == 2
        assert estimate.source_counts == {"command+cpus": 2}
        assert estimate.sample_count == 2
        assert estimate.weak_sources == {"command+cpus": 2}

    def test_three_independent_generic_rows_reach_normal_confidence(self) -> None:
        """IID 293: distinct retained evidence, not consumer count, lifts N."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T14:00:00+00:00",
            finished_at=None,
        )
        ahead = [
            _spec(
                f"ahead{i}",
                command=["python", f"job{i}.py"],
                cpus=i + 1,
                state=JobState.PENDING,
                submitted_at=f"2026-06-30T{10 + i:02d}:00:00+00:00",
                finished_at=None,
            )
            for i in range(3)
        ]
        history = [
            _spec(
                f"retained{i}",
                command=["python", f"job{i}.py"],
                cpus=i + 1,
            )
            for i in range(3)
        ]

        estimate = eta.estimate_pending_wait(target, [*ahead, target], history)

        assert estimate is not None
        assert estimate.source_counts == {"command+cpus": 3}
        assert estimate.sample_count == 3
        assert estimate.weak_sources == {}

    def test_evidence_aggregation_is_order_invariant(self) -> None:
        """IID 293: queue/history ordering cannot change confidence."""
        target = _spec(
            "target",
            state=JobState.PENDING,
            submitted_at="2026-06-30T13:00:00+00:00",
            finished_at=None,
        )
        ahead = [
            _spec(
                f"ahead{i}",
                state=JobState.PENDING,
                submitted_at=f"2026-06-30T{10 + i:02d}:00:00+00:00",
                finished_at=None,
            )
            for i in range(2)
        ]
        history = [_spec("retained-a"), _spec("retained-b")]

        forward = eta.estimate_pending_wait(target, [*ahead, target], history)
        reverse = eta.estimate_pending_wait(
            target,
            [target, *reversed(ahead)],
            list(reversed(history)),
        )

        assert forward == reverse

    def test_evidence_identity_is_immutable_and_does_not_alias_inputs(self) -> None:
        """IID 293: confidence metadata owns immutable retained-row IDs."""
        target = _spec("target", state=JobState.PENDING, finished_at=None)
        history = [_spec("retained-a"), _spec("retained-b")]
        original_history = list(history)

        estimate = eta.estimate_job_duration(target, history)

        assert estimate is not None
        assert estimate.evidence_ids == frozenset({"retained-a", "retained-b"})
        assert history == original_history
        history.clear()
        assert estimate.evidence_ids == frozenset({"retained-a", "retained-b"})
