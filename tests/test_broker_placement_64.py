"""Pure broker placement: admission first, existing fleet ranking second."""
from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from vq import overview


def _snapshot(host="alpha", **changes):
    fields = dict(
        host=host, observed_at=100.0, free_cpus=8, free_mem_mb=16000, free_jobs=1,
        reachable=True, daemon_healthy=True, state_root_agreement=True,
        admission_open=True, queue_ready=True,
    )
    fields.update(changes)
    return overview.BrokerCapacitySnapshot(**fields)


def _request(**changes):
    return overview.BrokerPlacementRequest(**{"cpus": 4, "mem_mb": 8000, **changes})


def _place(snapshots, request=None, **changes):
    return overview.place_broker_job(
        snapshots, request or _request(), **{"now": 105.0, "max_age_seconds": 10.0, **changes})


def test_job_stays_held_when_no_host_can_run_now():
    snapshots = [_snapshot("small", free_mem_mb=7999), _snapshot("busy", free_cpus=3)]

    decision = _place(snapshots)

    assert decision.host is None
    assert decision.ranking == ()
    assert dict(decision.blockers) == {"small": "memory-insufficient", "busy": "cpus-insufficient"}


@pytest.mark.parametrize(("changes", "reason"), [
    ({"is_scheduler_host": True}, "scheduler-unsupported"),
    ({"reachable": False}, "unreachable"),
    ({"daemon_healthy": False}, "daemon-not-ready"),
    ({"state_root_agreement": False}, "state-root-unverified"),
    ({"admission_open": False}, "admission-blocked"),
    ({"queue_ready": False}, "queue-not-ready"),
    ({"observed_at": 94.9}, "snapshot-stale"),
    ({"observed_at": 106.0}, "snapshot-future"),
    ({"observed_at": float("nan")}, "snapshot-invalid"),
    ({"free_cpus": None}, "capacity-unknown"),
    ({"free_mem_mb": None}, "capacity-unknown"),
    ({"free_jobs": None}, "capacity-unknown"),
    ({"free_mem_mb": float("nan")}, "capacity-invalid"),
    ({"free_cpus": True}, "capacity-invalid"),
    ({"pending_cpus": -1}, "capacity-invalid"),
    ({"free_cpus": 3}, "cpus-insufficient"),
    ({"free_mem_mb": 7999}, "memory-insufficient"),
    ({"free_jobs": 0}, "job-slots-insufficient"),
])
def test_unavailable_or_uncertain_hosts_do_not_rank(changes, reason):
    bad = _snapshot("bad", **changes)

    decision = _place([bad, _snapshot("good")])

    assert decision.host == "good"
    assert dict(decision.blockers) == {"bad": reason}
    assert [rank[-1] for rank in decision.ranking] == ["good"]


def test_exact_resource_and_age_boundaries_fit():
    candidate = _snapshot(observed_at=95.0, free_cpus=4, free_mem_mb=8000)

    assert _place([candidate]).host == "alpha"


def test_pool_and_wall_time_are_eligibility_constraints():
    request = _request(pool="science", wall_time_seconds=120)
    candidates = [
        _snapshot("outside", pools=frozenset({"other"})),
        _snapshot("short", pools=frozenset({"science"}), max_wall_time_seconds=119),
        _snapshot("fits", pools=frozenset({"science"}), max_wall_time_seconds=120),
    ]

    decision = _place(candidates, request)

    assert decision.host == "fits"
    assert dict(decision.blockers) == {"outside": "pool-mismatch", "short": "wall-time-exceeded"}
    assert dict(_place([candidates[1]], _request(pool="science")).blockers) == {
        "short": "wall-time-unknown"}


@pytest.mark.parametrize("field", ["cpus", "mem_mb"])
@pytest.mark.parametrize("value", [0, -1, True, 1.5, None])
def test_request_requires_known_positive_resource_demand(field, value):
    with pytest.raises(ValueError):
        _request(**{field: value})


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_request_refuses_invalid_wall_time(value):
    with pytest.raises(ValueError):
        _request(wall_time_seconds=value)


@pytest.mark.parametrize("changes", [
    {"now": float("nan")}, {"now": float("inf")},
    {"max_age_seconds": 0}, {"max_age_seconds": -1},
    {"max_age_seconds": float("nan")},
])
def test_invalid_observation_policy_refuses(changes):
    with pytest.raises(ValueError):
        _place([_snapshot()], **changes)


def test_duplicate_host_authorities_refuse():
    with pytest.raises(ValueError, match="duplicate"):
        _place([_snapshot(), _snapshot()])


def test_ranking_is_shared_with_existing_recommendation_and_order_independent():
    candidates = [
        _snapshot("ram", free_mem_mb=32000, free_cpus=4),
        _snapshot("cores", free_cpus=12),
        _snapshot("quiet", pending_cpus=0, idle_seconds=90),
        _snapshot("queued", pending_cpus=2, idle_seconds=100),
        _snapshot("idle", idle_seconds=120),
        _snapshot("zulu", idle_seconds=120),
    ]
    old_style = [overview.HostOverview(
        host=s.host, max_cpus=s.free_cpus + s.pending_cpus, pending_cpus=s.pending_cpus,
        mem_available_mb=s.free_mem_mb, idle_seconds=s.idle_seconds,
    ) for s in candidates]
    expected = []
    while old_style:
        host = overview.recommend_host(old_style, job_cpus=4, job_mem_mb=8000)
        expected.append(host)
        old_style = [s for s in old_style if s.host != host]

    forward = _place(candidates)

    assert [rank[-1] for rank in forward.ranking] == expected
    assert expected == ["ram", "cores", "idle", "zulu", "quiet", "queued"]
    assert forward == _place(list(reversed(candidates)))
    assert forward.host == "ram"
    assert candidates[0].free_mem_mb == 32000
    with pytest.raises(FrozenInstanceError):
        candidates[0].free_mem_mb = 0


def test_reservation_debit_changes_the_next_decision():
    candidates = [_snapshot("roomy", free_mem_mb=18000), _snapshot("other")]
    assert _place(candidates).host == "roomy"
    # B4 supplies the next snapshot after recording/debiting its reservation.
    candidates[0] = replace(candidates[0], free_mem_mb=10000, free_cpus=4, free_jobs=0)
    assert _place(candidates).host == "other"


def test_unknown_admission_defaults_do_not_enable_placement():
    unknown = overview.BrokerCapacitySnapshot(
        host="unknown", observed_at=100, free_cpus=8, free_mem_mb=16000, free_jobs=1)
    assert _place([unknown]).host is None
    assert _place([]).host is None
