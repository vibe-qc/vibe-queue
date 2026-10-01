"""Strict-FIFO broker placement pass (#64 B4b)."""
from __future__ import annotations

import pytest

from tests import test_broker_placement_b4a_64 as b4a
from tests.test_broker_store_64 import _intent, _snapshot
from vq import broker_store

world = b4a.world
ROOTS = b4a.ROOTS


def _accept(world, at, **intent):
    return world.store.accept(world.payload, _intent(**intent), now=float(at))


def _run(world, snapshots=None, **kwargs):
    kwargs.setdefault("max_age_seconds", 60.0)
    return world.placer.run_once(snapshots or [_snapshot()], **kwargs)


def _states(world, *records):
    return [world.store.get(record.broker_job_id).state for record in records]


def test_a_head_that_cannot_run_blocks_every_later_job(world):
    first = _accept(world, 100, cpus=2, mem_mb=2000)
    blocked = _accept(world, 101, mem_mb=900_000)
    small = _accept(world, 102, cpus=1, mem_mb=1000)

    # Room for three jobs: `small` would fit on alpha if it were evaluated.
    snapshots = [_snapshot(free_jobs=3)]

    report = _run(world, snapshots)

    assert _states(world, first, blocked, small) == ["placed", "held", "held"]
    assert report.head == blocked.broker_job_id
    assert report.waiting == (small.broker_job_id,)
    assert [o.broker_job_id for o in report.placements] == [
        first.broker_job_id, blocked.broker_job_id]
    assert len(world.target.calls) == 1
    # Proof that it would fit: once the head is gone, it is placed from the
    # very same snapshot.
    current = world.store.get(blocked.broker_job_id)
    world.store.request_cancel(blocked.broker_job_id, current.revision, reason="u", now=200.0)
    assert _run(world, snapshots).placements[0].broker_job_id == small.broker_job_id
    assert _states(world, small) == ["placed"]


def test_the_queue_is_ordered_by_acceptance_time_not_by_id(world, monkeypatch):
    ids = iter(["b-ffffffffffff", "b-000000000000"])
    monkeypatch.setattr(broker_store, "new_broker_job_id", lambda: next(ids))
    older = _accept(world, 100)
    newer = _accept(world, 101)

    report = _run(world, [_snapshot(free_jobs=1)])

    assert _states(world, older, newer) == ["placed", "held"]
    assert report.head == newer.broker_job_id


def test_jobs_that_fit_are_placed_oldest_first(world):
    jobs = [_accept(world, 100 + i, cpus=2, mem_mb=2000) for i in range(3)]

    report = _run(world, [_snapshot(free_jobs=3)])

    assert report.head is None and report.waiting == ()
    assert [o.broker_job_id for o in report.placements] == [j.broker_job_id for j in jobs]
    assert [c.idempotency_key for c in world.target.calls] == [j.broker_job_id for j in jobs]


def test_an_unplaceable_head_stops_the_pass_and_names_itself(world):
    head = _accept(world, 100)
    later = _accept(world, 101)
    broker_store.os.unlink(world.store.payload_path(head.broker_job_id))

    report = _run(world, [_snapshot(free_jobs=2)])

    assert report.head == head.broker_job_id and "unavailable" in report.stopped
    assert report.waiting == (later.broker_job_id,)
    assert _states(world, head, later) == ["held", "held"]
    assert world.target.calls == []


def test_an_unusable_record_stops_the_pass_before_any_placement(world):
    good, bad = _accept(world, 100), _accept(world, 101)
    (world.root / "records" / f"{bad.broker_job_id}.json").write_text("{broken")

    report = _run(world)

    assert "unusable broker records" in report.stopped
    assert world.store.get(good.broker_job_id) == good
    assert world.target.calls == []


def test_an_uncertain_transfer_does_not_block_the_queue(world):
    first, second = _accept(world, 100), _accept(world, 101)
    failures = iter([TimeoutError("ssh timed out")])
    original = world.target.submit

    def flaky(request):
        failure = next(failures, None)
        if failure is not None:
            raise failure
        return original(request)

    world.placer._submit = flaky
    report = _run(world, [_snapshot(free_jobs=2)])

    assert [o.outcome for o in report.placements] == ["uncertain", "placed"]
    assert _states(world, first, second) == ["transferring", "placed"]


def test_uncertain_transfers_are_replayed_only_after_the_interval(world):
    record = _accept(world, 100)
    world.target.fail = TimeoutError("ssh timed out")
    _run(world)
    world.target.fail = None

    early = _run(world, retry_interval_seconds=1_000_000)
    assert early.replayed == ()
    assert world.store.get(record.broker_job_id).state == "transferring"

    due = _run(world, retry_interval_seconds=0)
    assert [o.outcome for o in due.replayed] == ["placed"]
    assert world.target.calls[-1].idempotency_key == record.broker_job_id


def test_a_charge_is_settled_once_a_newer_target_listing_includes_the_job(world):
    record = _accept(world, 100)
    _run(world)
    placed = world.store.get(record.broker_job_id)
    target_job = placed.current_attempt.target_job_id
    accepted = placed.current_attempt.accepted_at

    stale = _run(world, [_snapshot(observed_at=accepted - 1)],
                 target_jobs={"alpha": frozenset({target_job})})
    missing = _run(world, [_snapshot(observed_at=accepted + 1)],
                   target_jobs={"alpha": frozenset()})
    assert stale.settled == () and missing.settled == ()
    assert world.store.outstanding_charges()["alpha"].cpus == 4

    fresh = _run(world, [_snapshot(observed_at=accepted + 1)],
                 target_jobs={"alpha": frozenset({target_job})})
    assert fresh.settled == (record.broker_job_id,)
    assert world.store.outstanding_charges() == {}


def test_a_crashed_reservation_is_finished_and_placed_in_the_same_pass(world):
    record = _accept(world, 100)
    b4a._reserve_only(world, record)

    report = _run(world)

    assert [o.outcome for o in report.recovered] == ["released"]
    assert world.store.get(record.broker_job_id).state == "placed"


def test_a_negative_retry_interval_is_refused(world):
    with pytest.raises(ValueError):
        _run(world, retry_interval_seconds=-1)


def test_a_replay_that_cannot_be_built_does_not_stall_the_queue(world):
    pinned = _accept(world, 100)
    world.target.fail = TimeoutError("ssh timed out")
    _run(world)
    world.target.fail = None
    broker_store.os.unlink(world.store.payload_path(pinned.broker_job_id))
    later = _accept(world, 101)

    for _ in range(2):
        report = _run(world, [_snapshot(free_jobs=2)], retry_interval_seconds=0)
        assert "unavailable" in report.refused[pinned.broker_job_id]
    assert _states(world, pinned, later) == ["transferring", "placed"]


def test_a_future_dated_listing_never_settles_a_charge(world):
    record = _accept(world, 100)
    _run(world)
    target_job = world.store.get(record.broker_job_id).current_attempt.target_job_id

    report = _run(world, [_snapshot(observed_at=1e9)],
                  target_jobs={"alpha": frozenset({target_job})})

    assert report.settled == ()
    assert world.store.outstanding_charges()["alpha"].cpus == 4


@pytest.mark.parametrize(
    ("kwargs", "snapshots"),
    [
        ({"retry_interval_seconds": float("nan")}, None),
        ({"max_age_seconds": float("inf")}, None),
        ({}, [_snapshot(), _snapshot()]),
    ],
)
def test_bad_pass_inputs_are_refused_before_any_step_writes(world, kwargs, snapshots):
    record = _accept(world, 100)
    b4a._reserve_only(world, record)
    before = world.store.get(record.broker_job_id)

    with pytest.raises(ValueError):
        _run(world, snapshots, **kwargs)

    assert world.store.get(record.broker_job_id) == before


# -- maintainer decisions 2026-09-27: fallback, and cancellation wins -------------


def _two_hosts():
    # alpha ranks first (more free memory); beta can also run the job.
    return [_snapshot("alpha", free_mem_mb=32000), _snapshot("beta")]


def test_a_refused_top_host_falls_back_to_the_next_ranked_one(world):
    world.placer._refuse_target = lambda host: "scheduler lane" if host == "alpha" else None
    record = _accept(world, 100)

    outcome = world.placer.place(record.broker_job_id, _two_hosts(), max_age_seconds=60.0)

    assert outcome.outcome == "placed" and outcome.host == "beta"
    attempt = world.store.get(record.broker_job_id).current_attempt
    assert attempt.target_host == "beta" and attempt.target_state_root == ROOTS["beta"]
    assert attempt.blockers["alpha"] == "target-refused: scheduler lane"
    assert attempt.snapshot["host"] == "beta"
    assert [c.target_host for c in world.target.calls] == ["beta"]


@pytest.mark.parametrize("probe", ["unknown", "raises"])
def test_an_unknown_top_state_root_falls_back(world, probe):
    def state_root_of(host):
        if host == "alpha":
            if probe == "raises":
                raise TimeoutError("ssh probe timed out")
            return None
        return ROOTS[host]

    world.placer._state_root_of = state_root_of
    record = _accept(world, 100)

    outcome = world.placer.place(record.broker_job_id, _two_hosts(), max_age_seconds=60.0)

    assert outcome.outcome == "placed" and outcome.host == "beta"
    assert [c.target_host for c in world.target.calls] == ["beta"]
    reason = world.store.get(record.broker_job_id).current_attempt.blockers["alpha"]
    assert reason.startswith("state-root-unknown")


def test_no_allowed_host_keeps_the_job_held_with_every_reason(world):
    world.placer._refuse_target = lambda host: f"{host} refused"
    record = _accept(world, 100)

    outcome = world.placer.place(record.broker_job_id, _two_hosts(), max_age_seconds=60.0)

    assert outcome.outcome == "held"
    assert "alpha: target-refused: alpha refused" in outcome.detail
    assert "beta: target-refused: beta refused" in outcome.detail
    assert world.store.get(record.broker_job_id) == record
    assert world.target.calls == []


def test_a_refused_top_host_no_longer_holds_the_fifo_queue(world):
    world.placer._refuse_target = lambda host: "refused" if host == "alpha" else None
    jobs = [_accept(world, 100 + i, cpus=2, mem_mb=2000) for i in range(2)]

    report = _run(world, [_snapshot("alpha", free_mem_mb=32000, free_jobs=3),
                          _snapshot("beta", free_jobs=3)])

    assert report.head is None
    assert _states(world, *jobs) == ["placed", "placed"]
    assert {c.target_host for c in world.target.calls} == {"beta"}


def _cancel_while_uncertain(world):
    record = _accept(world, 100)
    world.target.fail = TimeoutError("ssh timed out")
    _run(world)
    world.target.fail = None
    current = world.store.get(record.broker_job_id)
    world.store.request_cancel(record.broker_job_id, current.revision, reason="user", now=107.0)
    return record


def test_a_cancelled_uncertain_transfer_is_never_replayed_by_recovery(world):
    record = _cancel_while_uncertain(world)

    (outcome,) = world.placer.recover().outcomes

    assert outcome.outcome == "cancel-pending"
    assert record.broker_job_id in outcome.detail and "kill it there" in outcome.detail
    stored = world.store.get(record.broker_job_id)
    assert stored.state == "transferring"
    assert stored.cancellation.completed_at is None  # nothing claims it was killed
    assert world.store.outstanding_charges()["alpha"].cpus == 4  # it may exist
    assert len(world.target.calls) == 1  # only the original, uncertain send


def test_a_cancelled_uncertain_transfer_is_never_replayed_by_the_pass(world):
    record = _cancel_while_uncertain(world)
    later = _accept(world, 101)

    report = _run(world, [_snapshot(free_jobs=2)], retry_interval_seconds=0)

    assert [o.outcome for o in report.replayed] == ["cancel-pending"]
    assert [c.idempotency_key for c in world.target.calls] == [
        record.broker_job_id, later.broker_job_id]
    assert _states(world, record, later) == ["transferring", "placed"]


def test_fallback_keeps_b2_rank_order(world):
    world.target.roots["gamma"] = "/srv/vq-state/gamma"
    world.placer._refuse_target = lambda host: "refused" if host == "alpha" else None
    record = _accept(world, 100)
    snapshots = [  # rank: alpha, beta, gamma (by free memory)
        _snapshot("gamma", free_mem_mb=12000),
        _snapshot("alpha", free_mem_mb=32000),
        _snapshot("beta", free_mem_mb=24000),
    ]

    outcome = world.placer.place(record.broker_job_id, snapshots, max_age_seconds=60.0)

    assert outcome.host == "beta"
    assert "gamma" not in world.store.get(record.broker_job_id).current_attempt.blockers


def test_a_cancellation_during_the_replay_probe_stops_the_resend(world):
    record = _accept(world, 100)
    world.target.fail = TimeoutError("ssh timed out")
    _run(world)
    world.target.fail = None

    def probe_then_cancel(host):
        current = world.store.get(record.broker_job_id)
        world.store.request_cancel(record.broker_job_id, current.revision, reason="u", now=107.0)
        return ROOTS[host]

    world.placer._state_root_of = probe_then_cancel
    (outcome,) = world.placer.recover().outcomes

    assert outcome.outcome == "cancel-pending"
    assert len(world.target.calls) == 1


def test_a_cancelled_job_with_a_lost_payload_still_names_the_kill(world):
    record = _cancel_while_uncertain(world)
    broker_store.os.unlink(world.store.payload_path(record.broker_job_id))

    report = world.placer.recover()

    (outcome,) = report.outcomes
    assert outcome.outcome == "cancel-pending" and "kill it there" in outcome.detail
    assert report.refused == {}


# -- concurrent writers (per-operation locking) -----------------------------------


def test_a_cancel_committed_during_the_submit_keeps_the_acceptance(world):
    record = _accept(world, 100)
    original = world.target.submit

    def submit_while_cancelled(request):
        with broker_store.BrokerStore.open(world.root) as other:
            current = other.get(request.broker_job_id)
            other.request_cancel(request.broker_job_id, current.revision, reason="u", now=107.0)
        return original(request)

    world.placer._submit = submit_while_cancelled
    outcome = world.placer.place(record.broker_job_id, [_snapshot()], max_age_seconds=60.0)

    # The target accepted it: that must be recorded, with the kill still owed.
    assert outcome.outcome == "placed"
    stored = world.store.get(record.broker_job_id)
    assert stored.state == "placed" and stored.current_attempt.target_job_id
    assert stored.cancellation is not None and stored.cancellation.completed_at is None


def test_a_held_job_cancelled_mid_pass_is_skipped_not_the_head(world):
    first, cancelled, last = (_accept(world, 100 + i) for i in range(3))
    original = world.target.submit

    def submit_and_cancel_the_next(request):
        if request.broker_job_id == first.broker_job_id:
            with broker_store.BrokerStore.open(world.root) as other:
                other.request_cancel(cancelled.broker_job_id, 1, reason="u", now=107.0)
        return original(request)

    world.placer._submit = submit_and_cancel_the_next
    report = _run(world, [_snapshot(free_jobs=3)])

    assert report.head is None and report.refused == {}
    assert report.skipped == {cancelled.broker_job_id: "cancelled during the pass"}
    assert _states(world, first, cancelled, last) == ["placed", "cancelled", "placed"]


def test_a_second_pass_is_refused_while_one_runs(world):
    with broker_store.BrokerStore.open(world.root) as other, other.pass_lock():
        with pytest.raises(broker_store.BrokerStoreBusy, match="placement pass"):
            _run(world)
        with pytest.raises(broker_store.BrokerStoreBusy, match="placement pass"):
            world.placer.recover()


def test_a_cancel_during_the_settle_step_does_not_crash_the_pass(world):
    record = _accept(world, 100)
    later = _accept(world, 101)
    _run(world)  # places `record`; `later` waits for the job slot
    placed = world.store.get(record.broker_job_id)
    original = world.store.settle_charge

    def cancel_first(broker_job_id, revision, *, now):
        world.store.settle_charge = original  # cancel once, then behave
        with broker_store.BrokerStore.open(world.root) as other:
            other.request_cancel(broker_job_id, revision, reason="u", now=now)
        return original(broker_job_id, revision, now=now)

    world.store.settle_charge = cancel_first
    report = _run(world, [_snapshot(observed_at=placed.current_attempt.accepted_at + 1)],
                  target_jobs={"alpha": frozenset({placed.current_attempt.target_job_id})})

    assert report.stopped is None and report.settled == (record.broker_job_id,)
    stored = world.store.get(record.broker_job_id)
    assert stored.current_attempt.charge_released_at is not None
    assert stored.cancellation is not None
    del later


def test_a_cancel_racing_crash_recovery_completes_the_cancellation(world):
    record = _accept(world, 100)
    b4a._reserve_only(world, record)
    original = world.store.release_reservation

    def cancel_first(broker_job_id, revision, *, reason, now):
        with broker_store.BrokerStore.open(world.root) as other:
            other.request_cancel(broker_job_id, revision, reason="u", now=now)
        world.store.release_reservation = original
        return original(broker_job_id, revision, reason=reason, now=now)

    world.store.release_reservation = cancel_first
    report = _run(world)

    assert report.stopped is None
    assert world.store.get(record.broker_job_id).state == "cancelled"
    assert world.target.calls == []


def test_a_failed_create_is_withdrawn_before_any_pass_can_reserve_it(world, monkeypatch):
    import threading

    real = broker_store._fsync_dir
    records_fd = world.store._dirs["records"]
    outcome = {}

    def fsync_dir(fd):
        if fd == records_fd:
            # Another process races to reserve the just-linked record.
            def race():
                with broker_store.BrokerStore.open(world.root, lock_timeout=0.3) as other:
                    try:
                        (entry,) = [n for n in broker_store.os.listdir(world.root / "records")
                                    if n.endswith(".json")]
                        rec = other.get(entry[:-5])
                        b4a._reserve_only(SimpleWorld(other), rec)
                        outcome["result"] = "reserved"
                    except broker_store.BrokerStoreError as exc:
                        outcome["result"] = type(exc).__name__
            worker = threading.Thread(target=race)
            worker.start()
            worker.join()
            raise OSError(5, "Input/output error")
        real(fd)

    monkeypatch.setattr(broker_store, "_fsync_dir", fsync_dir)
    with pytest.raises(OSError):
        _accept(world, 100)

    assert outcome["result"] == "BrokerStoreBusy"  # the writer lock kept it out
    assert world.store.scan() == ([], {})


class SimpleWorld:
    def __init__(self, store):
        self.store = store


def test_two_direct_placements_never_share_the_last_job_slot(world):
    first, second = _accept(world, 100), _accept(world, 101)
    with broker_store.BrokerStore.open(world.root) as other_store:
        _race_two_placements(world, first, second, other_store)


def _race_two_placements(world, first, second, other_store):
    other_placer = b4a.BrokerPlacer(
        other_store, submit=world.target.submit, state_root_of=world.target.state_root_of,
    )
    results = {}

    def probe_then_race(host):
        try:
            other_placer.place(second.broker_job_id, [_snapshot()], max_age_seconds=60.0)
            results["second"] = "placed"
        except broker_store.BrokerStoreBusy:
            results["second"] = "busy"
        except Exception as exc:  # noqa: BLE001 - surface the unexpected
            results["second"] = repr(exc)
        return ROOTS[host]

    world.placer._state_root_of = probe_then_race
    world.placer.place(first.broker_job_id, [_snapshot()], max_age_seconds=60.0)

    assert results["second"] == "busy"
    assert world.store.outstanding_charges()["alpha"].job_slots == 1


def test_an_unbounded_lock_timeout_is_refused(tmp_path):
    with (
        pytest.raises(broker_store.BrokerStoreError, match="lock timeout"),
        broker_store.BrokerStore.open(tmp_path / "b", lock_timeout=float("inf")),
    ):
        pass
