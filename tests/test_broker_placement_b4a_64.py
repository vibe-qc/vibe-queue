"""Broker placement of one held job and restart recovery (#64 B4a)."""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tests.test_broker_store_64 import _archive, _intent, _snapshot
from vq import broker_placement, broker_store, submit
from vq.broker_placement import BrokerPlacer, debit_snapshots
from vq.broker_store import BrokerRecordConflict, BrokerRecordCorrupt, BrokerStore

ROOTS = {"alpha": "/srv/vq-state/alpha", "beta": "/srv/vq-state/beta"}


class Target:
    """Doubles for the keyed remote submit and the state-root probe."""

    def __init__(self) -> None:
        self.calls: list[broker_store.BrokerTransferRequest] = []
        self.fail: Exception | None = None
        self.roots = dict(ROOTS)
        self.next_id = 0

    def submit(self, request):
        self.calls.append(request)
        if self.fail is not None:
            raise self.fail
        self.next_id += 1
        return f"{self.next_id:012x}"

    def state_root_of(self, host):
        return self.roots.get(host)


@pytest.fixture
def world(tmp_path):
    payload = _archive(tmp_path / "payload.tar")
    target = Target()
    clock = iter(range(106, 10_000))
    with BrokerStore.open(tmp_path / "broker") as store:
        placer = BrokerPlacer(
            store, submit=target.submit, state_root_of=target.state_root_of,
            clock=lambda: float(next(clock)),
        )
        yield SimpleNamespace(store=store, placer=placer, target=target, payload=payload,
                              root=tmp_path / "broker")


def _accept(world, **intent):
    return world.store.accept(world.payload, _intent(**intent), now=100.0)


def _place(world, record, snapshots=None):
    return world.placer.place(
        record.broker_job_id, snapshots or [_snapshot()], max_age_seconds=60.0
    )


def test_a_held_job_is_reserved_transferred_with_its_key_and_placed(world):
    record = _accept(world)

    outcome = _place(world, record)

    assert outcome.outcome == "placed" and outcome.host == "alpha"
    assert outcome.target_job_id == "000000000001"
    (request,) = world.target.calls
    assert request.idempotency_key == record.broker_job_id
    assert request.submit_kwargs["idempotency_key"] == record.broker_job_id
    assert request.target_state_root == ROOTS["alpha"]
    stored = world.store.get(record.broker_job_id)
    assert stored.state == "placed"
    assert stored.current_attempt.target_state_root == ROOTS["alpha"]
    # The charge stays until a refreshed target snapshot includes the job.
    assert world.store.outstanding_charges()["alpha"].cpus == 4


def test_no_eligible_host_leaves_the_job_held_and_untouched(world):
    record = _accept(world)

    outcome = _place(world, record, [_snapshot(free_mem_mb=100)])

    assert outcome.outcome == "held"
    assert dict(outcome.blockers) == {"alpha": "memory-insufficient"}
    assert world.store.get(record.broker_job_id) == record
    assert world.target.calls == []


def test_outstanding_charges_are_debited_before_the_next_decision(world):
    first, second = _accept(world), _accept(world)
    snapshots = [_snapshot("alpha", free_jobs=1), _snapshot("beta", free_cpus=4)]

    assert _place(world, first, snapshots).host == "alpha"
    outcome = _place(world, second, snapshots)

    # alpha's only job slot is promised to the first job.
    assert outcome.host == "beta"
    attempt = world.store.get(second.broker_job_id).current_attempt
    assert attempt.blockers == {"alpha": "job-slots-insufficient"}


def test_the_recorded_snapshot_is_the_debited_one(world):
    first, second = _accept(world), _accept(world)
    snapshots = [_snapshot("alpha", free_jobs=2)]
    _place(world, first, snapshots)
    _place(world, second, snapshots)
    evidence = world.store.get(second.broker_job_id).current_attempt.snapshot
    assert (evidence["free_cpus"], evidence["free_mem_mb"], evidence["free_jobs"]) == (4, 8000, 1)


def test_an_unknown_state_root_starts_no_attempt(world):
    record = _accept(world)
    world.target.roots.pop("alpha")

    outcome = _place(world, record)

    assert outcome.outcome == "held" and "state root" in outcome.detail
    assert world.store.get(record.broker_job_id) == record
    assert world.target.calls == []


def test_a_refused_target_starts_no_attempt(world):
    world.placer._refuse_target = lambda host: f"{host} is a scheduler lane"
    record = _accept(world)

    outcome = _place(world, record)

    assert outcome.outcome == "held" and "scheduler lane" in outcome.detail
    assert world.store.get(record.broker_job_id) == record


@pytest.mark.parametrize(
    "failure",
    [TimeoutError("ssh timed out"), RuntimeError("exit 255"), ValueError("bad receipt")],
)
def test_a_failed_submit_stays_transferring_and_pinned(world, failure):
    record = _accept(world)
    world.target.fail = failure

    outcome = _place(world, record)

    assert outcome.outcome == "uncertain" and "outcome unknown" in outcome.detail
    stored = world.store.get(record.broker_job_id)
    assert stored.state == "transferring"
    assert stored.current_attempt.diagnostics[-1].startswith("transfer outcome unknown")
    assert world.store.outstanding_charges()["alpha"].cpus == 4


def test_an_invalid_target_job_id_is_not_an_acceptance(world):
    record = _accept(world)
    world.placer._submit = lambda request: "../../etc"
    assert _place(world, record).outcome == "uncertain"
    assert world.store.get(record.broker_job_id).state == "transferring"


def test_recovery_replays_the_same_key_to_the_same_target(world):
    record = _accept(world)
    world.target.fail = TimeoutError("ssh timed out")
    _place(world, record)
    world.target.fail = None

    report = world.placer.recover()

    assert [o.outcome for o in report.outcomes] == ["placed"]
    first, replay = world.target.calls
    assert replay.target_host == first.target_host == "alpha"
    assert replay.request_fingerprint == first.request_fingerprint
    assert replay.submit_kwargs == first.submit_kwargs
    assert world.store.get(record.broker_job_id).state == "placed"


def test_recovery_never_replays_to_a_changed_state_root(world):
    record = _accept(world)
    world.target.fail = TimeoutError("ssh timed out")
    _place(world, record)
    world.target.fail = None
    world.target.roots["alpha"] = "/srv/vq-state/other"

    (outcome,) = world.placer.recover().outcomes

    assert outcome.outcome == "uncertain" and "not the recorded" in outcome.detail
    assert len(world.target.calls) == 1
    assert world.store.get(record.broker_job_id).state == "transferring"


def test_a_crash_after_writing_transferring_is_replayed(world):
    record = _accept(world)
    decision = broker_store.overview.place_broker_job(
        [_snapshot()], record.intent.placement_request(), now=105.0, max_age_seconds=60.0
    )
    record = world.store.reserve(
        record.broker_job_id, record.revision, decision=decision, snapshot=_snapshot(),
        target_state_root=ROOTS["alpha"], now=106.0,
    )
    world.store.begin_transfer(record.broker_job_id, record.revision, now=107.0)

    (outcome,) = world.placer.recover().outcomes

    assert outcome.outcome == "placed"
    assert world.target.calls[0].idempotency_key == record.broker_job_id


def test_recovery_releases_unstarted_reservations_and_finishes_returns(world):
    reserved, cancelled = _accept(world), _accept(world)
    decision = broker_store.overview.place_broker_job(
        [_snapshot()], reserved.intent.placement_request(), now=105.0, max_age_seconds=60.0
    )
    for record in (reserved, cancelled):
        world.store.reserve(
            record.broker_job_id, record.revision, decision=decision, snapshot=_snapshot(),
            target_state_root=ROOTS["alpha"], now=106.0,
        )
    current = world.store.get(cancelled.broker_job_id)
    world.store.request_cancel(cancelled.broker_job_id, current.revision, reason="u", now=107.0)

    report = world.placer.recover()

    assert {o.broker_job_id for o in report.outcomes} == {
        reserved.broker_job_id, cancelled.broker_job_id}
    assert world.store.get(reserved.broker_job_id).state == "held"
    assert world.store.get(cancelled.broker_job_id).state == "cancelled"
    assert world.store.outstanding_charges() == {}
    assert world.target.calls == []


def test_recovery_leaves_unparseable_records_alone(world):
    good, bad = _accept(world), _accept(world)
    (world.root / "records" / f"{bad.broker_job_id}.json").write_text("{broken")

    report = world.placer.recover()

    assert bad.broker_job_id in report.refused
    assert world.store.get(good.broker_job_id) == good
    with pytest.raises(BrokerRecordCorrupt):
        _place(world, good)  # charges cannot be reconstructed
    assert world.target.calls == []


def test_a_record_that_fails_during_recovery_does_not_stop_the_others(world, monkeypatch):
    ids = iter(["b-000000000001", "b-000000000002"])
    monkeypatch.setattr(broker_store, "new_broker_job_id", lambda: next(ids))
    pinned, reserved = _accept(world), _accept(world)
    _pin_transferring(world, pinned)
    _reserve_only(world, reserved)
    broker_store.os.unlink(world.store.payload_path(pinned.broker_job_id))

    report = world.placer.recover()

    assert "unavailable" in report.refused[pinned.broker_job_id]
    assert world.store.get(reserved.broker_job_id).state == "held"
    assert world.target.calls == []


def test_a_raising_state_root_probe_is_recorded_not_fatal(world, monkeypatch):
    ids = iter(["b-000000000001", "b-000000000002"])
    monkeypatch.setattr(broker_store, "new_broker_job_id", lambda: next(ids))
    pinned, reserved = _accept(world), _accept(world)
    _pin_transferring(world, pinned)
    _reserve_only(world, reserved)

    def unreachable(host):
        raise TimeoutError("ssh probe timed out")

    world.placer._state_root_of = unreachable
    report = world.placer.recover()

    by_id = {o.broker_job_id: o for o in report.outcomes}
    assert by_id[pinned.broker_job_id].outcome == "uncertain"
    stored = world.store.get(pinned.broker_job_id)
    assert stored.state == "transferring"
    assert "state root unavailable" in stored.current_attempt.diagnostics[-1]
    assert world.store.get(reserved.broker_job_id).state == "held"
    assert world.target.calls == []


def test_a_raising_probe_during_placement_starts_no_attempt(world):
    record = _accept(world)

    def unreachable(host):
        raise TimeoutError("ssh probe timed out")

    world.placer._state_root_of = unreachable
    outcome = _place(world, record)

    assert outcome.outcome == "held" and "ssh probe timed out" in outcome.detail
    assert world.store.get(record.broker_job_id) == record


def test_a_missing_payload_never_reaches_transferring(world):
    record = _accept(world)
    broker_store.os.unlink(world.store.payload_path(record.broker_job_id))

    with pytest.raises(BrokerRecordCorrupt, match="unavailable"):
        _place(world, record)

    assert world.store.get(record.broker_job_id) == record
    assert world.store.outstanding_charges() == {}
    assert world.target.calls == []


def test_a_fresh_placement_probes_the_state_root_once(world):
    probes = []
    world.placer._state_root_of = lambda host: probes.append(host) or ROOTS[host]
    assert _place(world, _accept(world)).outcome == "placed"
    assert probes == ["alpha"]


def _decision(record):
    return broker_store.overview.place_broker_job(
        [_snapshot()], record.intent.placement_request(), now=105.0, max_age_seconds=60.0
    )


def _reserve_only(world, record):
    return world.store.reserve(
        record.broker_job_id, record.revision, decision=_decision(record),
        snapshot=_snapshot(), target_state_root=ROOTS["alpha"], now=106.0,
    )


def _pin_transferring(world, record):
    reserved = _reserve_only(world, record)
    return world.store.begin_transfer(record.broker_job_id, reserved.revision, now=107.0)


def test_place_refuses_jobs_that_are_not_plainly_held(world):
    placed = _accept(world)
    _place(world, placed)
    cancelling = _accept(world)
    decision = broker_store.overview.place_broker_job(
        [_snapshot()], cancelling.intent.placement_request(), now=105.0, max_age_seconds=60.0
    )
    record = world.store.reserve(
        cancelling.broker_job_id, cancelling.revision, decision=decision,
        snapshot=_snapshot(), target_state_root=ROOTS["alpha"], now=106.0,
    )
    for job_id in (placed.broker_job_id, record.broker_job_id):
        with pytest.raises(BrokerRecordConflict, match="not held"):
            world.placer.place(job_id, [_snapshot()], max_age_seconds=60.0)


def test_debit_keeps_unknown_headroom_unknown():
    charge = broker_store.BrokerCharge(4, 8000, 1)
    unknown = _snapshot(free_cpus=None, free_mem_mb=None, free_jobs=None)
    (debited,) = debit_snapshots([unknown], {"alpha": charge})
    assert (debited.free_cpus, debited.free_mem_mb, debited.free_jobs) == (None, None, None)
    (over,) = debit_snapshots([_snapshot(free_cpus=2)], {"alpha": charge})
    assert over.free_cpus == -2


# -- the real keyed submit wiring -----------------------------------------------


def _cfg(**hosts):
    return SimpleNamespace(hosts={
        name: SimpleNamespace(scheduler=scheduler) for name, scheduler in hosts.items()
    })


def _request(host="alpha"):
    return broker_store.BrokerTransferRequest(
        broker_job_id="b-0123456789ab", target_host=host, target_state_root="/s",
        idempotency_key="b-0123456789ab", request_fingerprint="f" * 64,
        payload_path=broker_store.Path("/p.tar"),
        submit_kwargs={"archive": "/p.tar", "command": ["python3", "run.py"],
                       "idempotency_key": "b-0123456789ab"},
    )


def test_remote_submitter_forwards_the_keyed_request(monkeypatch):
    seen = []

    def submit_remote(**kwargs):
        seen.append(kwargs)
        return ["0123456789ab"]

    monkeypatch.setattr(submit, "submit_remote", submit_remote)
    cfg = _cfg(alpha="local")

    assert broker_placement.remote_submitter(cfg)(_request()) == "0123456789ab"
    assert seen[0]["host"] == "alpha" and seen[0]["host_cfg"] is cfg.hosts["alpha"]
    assert seen[0]["idempotency_key"] == "b-0123456789ab"


def test_remote_wiring_refuses_scheduler_lanes_and_extra_receipts(monkeypatch):
    monkeypatch.setattr(submit, "submit_remote", lambda **kwargs: ["a" * 12, "b" * 12])
    cfg = _cfg(alpha="local", cluster="pbs")
    refuse = broker_placement.remote_target_refusal(cfg)

    assert refuse("alpha") is None
    assert "scheduler lane" in refuse("cluster")
    assert "not configured" in refuse("ghost")
    with pytest.raises(broker_store.BrokerStoreError, match="scheduler lane"):
        broker_placement.remote_submitter(cfg)(_request("cluster"))
    with pytest.raises(broker_store.BrokerStoreError, match="2 job ids"):
        broker_placement.remote_submitter(cfg)(_request())


def test_outcomes_are_json_serialisable(world):
    outcome = _place(world, _accept(world))
    json.dumps(outcome.__dict__)
