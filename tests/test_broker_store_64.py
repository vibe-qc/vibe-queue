"""Durable broker held store (#64 B3): records, revisions and keyed transfer."""
from __future__ import annotations

import inspect
import io
import json
import os
import stat
import tarfile

import pytest

from vq import broker_store, overview, submit
from vq.broker_store import (
    BrokerIntent,
    BrokerRecordConflict,
    BrokerRecordCorrupt,
    BrokerStore,
    BrokerStoreBusy,
    BrokerStoreError,
)
from vq.spec import validate_job_id

STATE_ROOT = "/srv/vq-state/alpha"


def _archive(path, files=None):
    files = {"run.py": b"print('hi')\n"} if files is None else files
    with tarfile.open(path, "w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return path


def _intent(**changes):
    return BrokerIntent(**{"command": ("python3", "run.py"), "cpus": 4, "mem_mb": 8000, **changes})


def _snapshot(host="alpha", **changes):
    fields = dict(
        host=host, observed_at=100.0, free_cpus=8, free_mem_mb=16000, free_jobs=1,
        reachable=True, daemon_healthy=True, state_root_agreement=True,
        admission_open=True, queue_ready=True,
    )
    fields.update(changes)
    return overview.BrokerCapacitySnapshot(**fields)


def _decide(record, snapshots=None):
    snapshots = snapshots or [_snapshot()]
    return overview.place_broker_job(
        snapshots, record.intent.placement_request(), now=105.0, max_age_seconds=10.0
    )


@pytest.fixture
def root(tmp_path):
    return tmp_path / "broker"


@pytest.fixture
def payload(tmp_path):
    return _archive(tmp_path / "payload.tar")


def _entries(root):
    return {
        name: sorted(os.listdir(root / name)) for name in ("records", "payloads", "staging")
    }


def _reserve(store, record, snapshot=None):
    snapshot = snapshot or _snapshot()
    return store.reserve(
        record.broker_job_id, record.revision, decision=_decide(record, [snapshot]),
        snapshot=snapshot, target_state_root=STATE_ROOT, now=106.0,
    )


def _transferring(store, payload):
    record = store.accept(payload, _intent(), now=100.0)
    record = _reserve(store, record)
    return store.begin_transfer(record.broker_job_id, record.revision, now=107.0)


# -- acceptance ---------------------------------------------------------------


def test_accept_publishes_payload_and_held_record_owner_only(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)

        assert record.state == "held" and record.revision == 1
        assert broker_store.BROKER_JOB_ID_PATTERN.fullmatch(record.broker_job_id)
        assert store.get(record.broker_job_id) == record
        assert store.verify_payload(record) == store.payload_path(record.broker_job_id)
        assert store.payload_path(record.broker_job_id).read_bytes() == payload.read_bytes()
    entries = _entries(root)
    assert entries == {
        "records": [f"{record.broker_job_id}.json"],
        "payloads": [f"{record.broker_job_id}.tar"],
        "staging": [],
    }
    for sub in ("", "records", "payloads", "staging"):
        assert stat.S_IMODE(os.stat(root / sub).st_mode) == 0o700
    for path in (
        root / "records" / f"{record.broker_job_id}.json",
        root / "payloads" / f"{record.broker_job_id}.tar",
    ):
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600


def test_broker_job_id_is_a_valid_job_id_and_idempotency_key():
    for _ in range(50):
        job_id = broker_store.new_broker_job_id()
        assert validate_job_id(job_id) == job_id
        submit._validate_idempotency_key(job_id)


@pytest.mark.parametrize(
    "make_payload",
    [
        pytest.param(lambda p: p.write_bytes(b"not an archive") or p, id="not-tar"),
        pytest.param(lambda p: _archive(p, {"other.py": b""}), id="entrypoint-missing"),
    ],
)
def test_refused_payload_leaves_no_accepted_job(root, tmp_path, make_payload):
    bad = make_payload(tmp_path / "bad.tar")
    with BrokerStore.open(root) as store, pytest.raises(BrokerStoreError):
        store.accept(bad, _intent(), now=100.0)
    assert _entries(root) == {"records": [], "payloads": [], "staging": []}


def test_symlinked_payload_source_is_refused(root, payload, tmp_path):
    link = tmp_path / "link.tar"
    link.symlink_to(payload)
    with BrokerStore.open(root) as store, pytest.raises(BrokerStoreError):
        store.accept(link, _intent(), now=100.0)
    assert _entries(root) == {"records": [], "payloads": [], "staging": []}


def test_record_publication_failure_withdraws_the_payload(root, payload, monkeypatch):
    def fail_link(*args, **kwargs):
        raise OSError(28, "No space left on device")

    with BrokerStore.open(root) as store:
        monkeypatch.setattr(broker_store.os, "link", fail_link)
        with pytest.raises(OSError):
            store.accept(payload, _intent(), now=100.0)
    assert _entries(root) == {"records": [], "payloads": [], "staging": []}


def test_record_directory_fsync_failure_withdraws_record_and_payload(root, payload, monkeypatch):
    real_fsync_dir = broker_store._fsync_dir

    with BrokerStore.open(root) as store:
        records_fd = store._dirs["records"]

        def fsync_dir(fd):
            if fd == records_fd:
                raise OSError(5, "Input/output error")
            real_fsync_dir(fd)

        monkeypatch.setattr(broker_store, "_fsync_dir", fsync_dir)
        with pytest.raises(OSError):
            store.accept(payload, _intent(), now=100.0)
    assert _entries(root) == {"records": [], "payloads": [], "staging": []}


def test_an_id_collision_never_touches_the_other_jobs_payload(root, payload, tmp_path, monkeypatch):
    other = _archive(tmp_path / "other.tar", {"run.py": b"print('other')\n"})
    monkeypatch.setattr(broker_store, "new_broker_job_id", lambda: "b-0123456789ab")
    with BrokerStore.open(root) as store:
        first = store.accept(payload, _intent(), now=100.0)
        with pytest.raises(BrokerStoreError, match="collides"):
            store.accept(other, _intent(), now=101.0)
        store.verify_payload(first)
        assert store.payload_path(first.broker_job_id).read_bytes() == payload.read_bytes()
    assert _entries(root)["staging"] == []


def test_a_fifo_payload_source_is_refused_without_blocking(root, tmp_path):
    import threading

    fifo = tmp_path / "upload.tar"
    os.mkfifo(fifo)
    outcome = []

    def run():
        with BrokerStore.open(root) as store:
            try:
                store.accept(fifo, _intent(), now=100.0)
            except BrokerStoreError as exc:
                outcome.append(str(exc))

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    worker.join(timeout=10)
    if worker.is_alive():
        # Unblock the reader so the thread (and its lock) can finish.
        with open(fifo, "wb"):
            pass
        worker.join(timeout=10)
        pytest.fail("accept() blocked opening a FIFO while holding the store lock")
    assert outcome and "regular" in outcome[0]


def test_payload_is_a_snapshot_not_a_reference(root, payload):
    original = payload.read_bytes()
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        _archive(payload, {"run.py": b"print('changed')\n"})
        assert store.payload_path(record.broker_job_id).read_bytes() == original
        store.verify_payload(record)


# -- writer lock and path safety ------------------------------------------------


def test_two_open_stores_serialize_transitions_and_refuse_stale_views(root, payload):
    with BrokerStore.open(root) as first, BrokerStore.open(root) as second:
        record = first.accept(payload, _intent(), now=100.0)
        other = second.accept(payload, _intent(), now=100.0)
        assert {r.broker_job_id for r in first.scan()[0]} == {
            record.broker_job_id, other.broker_job_id}
        second.request_cancel(record.broker_job_id, 1, reason="elsewhere", now=101.0)
        # `first` still holds revision 1: its view is stale and must fail.
        with pytest.raises(BrokerRecordConflict, match="revision 2"):
            _reserve(first, record)
        assert first.get(record.broker_job_id).state == "cancelled"


def test_a_held_writer_lock_times_out_as_busy(root, payload):
    import fcntl

    with BrokerStore.open(root, lock_timeout=0.2) as store:
        record = store.accept(payload, _intent(), now=100.0)
        holder = os.open(root / ".lock", os.O_RDWR)
        try:
            fcntl.flock(holder, fcntl.LOCK_EX)
            with pytest.raises(BrokerStoreBusy, match="busy"):
                store.request_cancel(record.broker_job_id, 1, reason="u", now=101.0)
        finally:
            os.close(holder)
        assert store.get(record.broker_job_id) == record
        store.request_cancel(record.broker_job_id, 1, reason="u", now=101.0)


def test_only_one_placement_pass_at_a_time(root):
    with BrokerStore.open(root) as first, BrokerStore.open(root) as second:
        with (
            first.pass_lock(),
            pytest.raises(BrokerStoreBusy, match="placement pass"),
            second.pass_lock(),
        ):
            pass
        with second.pass_lock():
            pass


def test_an_unsafe_lock_file_is_refused(root):
    root.mkdir(mode=0o700)
    (root / ".lock").write_text("")
    os.chmod(root / ".lock", 0o644)
    with pytest.raises(BrokerStoreError, match="owner-only"), BrokerStore.open(root):
        pass


def test_group_writable_root_is_refused(root):
    root.mkdir(mode=0o700)
    os.chmod(root, 0o770)
    with pytest.raises(BrokerStoreError, match="unsafe permissions"), BrokerStore.open(root):
        pass


def test_symlinked_root_and_subdirectory_are_refused(tmp_path):
    real = tmp_path / "real"
    real.mkdir(mode=0o700)
    link = tmp_path / "broker"
    link.symlink_to(real)
    with pytest.raises(BrokerStoreError, match="not a real directory"), BrokerStore.open(link):
        pass

    root = tmp_path / "other"
    root.mkdir(mode=0o700)
    (root / "records").symlink_to(real)
    with pytest.raises(BrokerStoreError, match="not a real directory"), BrokerStore.open(root):
        pass


def test_relative_root_is_refused():
    relative = broker_store.Path("broker")
    with pytest.raises(BrokerStoreError, match="absolute"), BrokerStore.open(relative):
        pass


# -- revisions and the state machine ---------------------------------------------


def test_stale_revision_cannot_apply_a_transition(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        reserved = _reserve(store, record)
        with pytest.raises(BrokerRecordConflict, match="revision 2"):
            store.release_reservation(record.broker_job_id, 1, reason="stale", now=107.0)
        assert store.get(record.broker_job_id) == reserved


def test_placement_lifecycle_and_outstanding_charges(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        assert store.outstanding_charges() == {}

        record = _reserve(store, record)
        attempt = record.current_attempt
        assert record.state == "reserved" and attempt.number == 1
        assert attempt.target_host == "alpha" and attempt.target_state_root == STATE_ROOT
        assert attempt.idempotency_key == record.broker_job_id
        assert attempt.request_fingerprint is None
        assert attempt.snapshot["free_mem_mb"] == 16000
        assert attempt.ranking and attempt.ranking[0][-1] == "alpha"
        charge = broker_store.BrokerCharge(4, 8000, 1)
        assert store.outstanding_charges() == {"alpha": charge}

        record = store.begin_transfer(record.broker_job_id, record.revision, now=107.0)
        fingerprint = record.current_attempt.request_fingerprint
        assert record.state == "transferring" and fingerprint
        assert store.outstanding_charges() == {"alpha": charge}

        record = store.record_placed(
            record.broker_job_id, record.revision, request_fingerprint=fingerprint,
            target_job_id="0123456789ab", now=108.0,
        )
        assert record.state == "placed"
        assert record.current_attempt.target_job_id == "0123456789ab"
        # Kept until a refreshed target snapshot includes the accepted job.
        assert store.outstanding_charges() == {"alpha": charge}

        record = store.settle_charge(record.broker_job_id, record.revision, now=120.0)
        assert store.outstanding_charges() == {}
        assert store.get(record.broker_job_id) == record
        assert record.revision == 5
        with pytest.raises(BrokerRecordConflict):
            store.settle_charge(record.broker_job_id, record.revision, now=121.0)


def test_unstarted_reservation_returns_to_holding_with_evidence(root, payload):
    with BrokerStore.open(root) as store:
        record = _reserve(store, store.accept(payload, _intent(), now=100.0))
        record = store.release_reservation(
            record.broker_job_id, record.revision, reason="snapshot went stale", now=107.0
        )
        assert record.state == "returned"
        assert store.outstanding_charges() == {}
        record = store.resume_holding(record.broker_job_id, record.revision, now=108.0)
        assert record.state == "held"

        record = _reserve(store, record, _snapshot("beta"))
        assert [a.number for a in record.attempts] == [1, 2]
        assert record.attempts[0].return_reason == "snapshot went stale"
        assert store.outstanding_charges() == {"beta": broker_store.BrokerCharge(4, 8000, 1)}


def test_started_transfer_can_never_be_released_or_rerouted(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        for call in (
            lambda: store.release_reservation(
                record.broker_job_id, record.revision, reason="timeout", now=110.0),
            lambda: store.resume_holding(record.broker_job_id, record.revision, now=110.0),
            lambda: _reserve(store, record, _snapshot("beta")),
        ):
            with pytest.raises(BrokerRecordConflict):
                call()
        record = store.record_transfer_diagnostic(
            record.broker_job_id, record.revision, message="ssh timed out", now=111.0
        )
        assert record.state == "transferring"
        assert record.current_attempt.diagnostics == ("ssh timed out",)
        assert store.outstanding_charges() == {"alpha": broker_store.BrokerCharge(4, 8000, 1)}


def test_acceptance_for_another_request_is_refused(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        with pytest.raises(BrokerRecordConflict, match="different transfer"):
            store.record_placed(
                record.broker_job_id, record.revision, request_fingerprint="0" * 64,
                target_job_id="0123456789ab", now=108.0,
            )
        assert store.get(record.broker_job_id) == record


def test_reserve_requires_the_chosen_hosts_snapshot_and_an_absolute_root(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        decision = _decide(record)
        with pytest.raises(BrokerStoreError, match="chosen host"):
            store.reserve(
                record.broker_job_id, 1, decision=decision, snapshot=_snapshot("beta"),
                target_state_root=STATE_ROOT, now=106.0,
            )
        held = overview.place_broker_job(
            [_snapshot(free_cpus=1)], record.intent.placement_request(),
            now=105.0, max_age_seconds=10.0,
        )
        with pytest.raises(BrokerStoreError, match="chosen host"):
            store.reserve(
                record.broker_job_id, 1, decision=held, snapshot=_snapshot(),
                target_state_root=STATE_ROOT, now=106.0,
            )
        with pytest.raises(BrokerStoreError, match="absolute"):
            store.reserve(
                record.broker_job_id, 1, decision=decision, snapshot=_snapshot(),
                target_state_root="relative/state", now=106.0,
            )
        assert store.get(record.broker_job_id) == record


# -- cancellation -----------------------------------------------------------------


def test_held_cancellation_completes_locally(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        record = store.request_cancel(record.broker_job_id, 1, reason="user", now=101.0)
        assert record.state == "cancelled"
        assert record.cancellation.completed_at == 101.0
        with pytest.raises(BrokerRecordConflict):
            store.request_cancel(record.broker_job_id, record.revision, reason="again", now=102.0)
        with pytest.raises(BrokerRecordConflict):
            _reserve(store, record)


def test_reserved_cancellation_blocks_transfer_and_completes_on_release(root, payload):
    with BrokerStore.open(root) as store:
        record = _reserve(store, store.accept(payload, _intent(), now=100.0))
        record = store.request_cancel(record.broker_job_id, record.revision, reason="u", now=107.0)
        assert record.state == "reserved" and record.cancellation.completed_at is None
        with pytest.raises(BrokerRecordConflict, match="release"):
            store.begin_transfer(record.broker_job_id, record.revision, now=108.0)
        record = store.release_reservation(
            record.broker_job_id, record.revision, reason="cancelled", now=108.0)
        record = store.resume_holding(record.broker_job_id, record.revision, now=109.0)
        assert record.state == "cancelled" and record.cancellation.completed_at == 109.0
        assert store.outstanding_charges() == {}


def test_transferring_cancellation_is_intent_only_and_placement_still_records(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        record = store.request_cancel(record.broker_job_id, record.revision, reason="u", now=109.0)
        assert record.state == "transferring"
        assert record.cancellation.completed_at is None
        record = store.record_placed(
            record.broker_job_id, record.revision,
            request_fingerprint=record.current_attempt.request_fingerprint,
            target_job_id="0123456789ab", now=110.0,
        )
        # The target kill is still owed; nothing claims the job was killed.
        assert record.state == "placed" and record.cancellation.completed_at is None


# -- keyed transfer request ---------------------------------------------------------


def test_transfer_request_replays_one_keyed_submit(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        request = store.transfer_request(record)
        assert request.idempotency_key == record.broker_job_id
        assert request.target_host == "alpha"
        assert request.target_state_root == STATE_ROOT
        assert request.request_fingerprint == record.current_attempt.request_fingerprint
        kwargs = request.submit_kwargs
        assert kwargs["idempotency_key"] == record.broker_job_id
        assert kwargs["archive"] == str(store.payload_path(record.broker_job_id))
        assert kwargs["command"] == ["python3", "run.py"]
        assert (kwargs["cpus"], kwargs["mem_mb"]) == (4, 8000)
        for absent in ("scheduler_target", "array", "chain", "depends_on", "python", "branch"):
            assert absent not in kwargs
        # The same inputs every time: a replay, never a new submission.
        assert store.transfer_request(record) == request
        inspect.signature(submit.submit_remote).bind(host="alpha", host_cfg=object(), **kwargs)


def test_transfer_request_needs_a_transferring_record(root, payload):
    with BrokerStore.open(root) as store:
        record = _reserve(store, store.accept(payload, _intent(), now=100.0))
        with pytest.raises(BrokerRecordConflict, match="no transfer"):
            store.transfer_request(record)


def test_transfer_request_refuses_a_stale_record_view(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        store.record_transfer_diagnostic(
            record.broker_job_id, record.revision, message="retrying", now=110.0)
        with pytest.raises(BrokerRecordConflict, match="changed since"):
            store.transfer_request(record)


def test_transfer_request_refuses_changed_payload_bytes(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        stored = store.payload_path(record.broker_job_id)
        os.chmod(stored, 0o600)
        _archive(stored, {"run.py": b"print('swapped')\n"})
        with pytest.raises(BrokerRecordCorrupt, match="does not match"):
            store.transfer_request(record)


def test_transfer_request_refuses_intent_changed_after_transfer_began(root, payload):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        path = root / "records" / f"{record.broker_job_id}.json"
        data = json.loads(path.read_text())
        data["intent"]["cpus"] = 2
        data["intent_digest"] = _intent(cpus=2).digest()
        path.write_text(json.dumps(data))
        tampered = store.get(record.broker_job_id)
        with pytest.raises(BrokerRecordConflict, match="changed after the transfer began"):
            store.transfer_request(tampered)


# -- corrupt and contradictory records ------------------------------------------------


def _rewrite(root, record, mutate):
    path = root / "records" / f"{record.broker_job_id}.json"
    data = json.loads(path.read_text())
    mutate(data)
    path.write_text(json.dumps(data))


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d.update(schema="vq.broker-record.v9"), "schema"),
        (lambda d: d.update(extra=1), "unexpected fields"),
        (lambda d: d.update(state="lost"), "unknown broker state"),
        (lambda d: d["intent"].update(mem_mb=1), "digest"),
        (lambda d: d.update(revision=True), "revision"),
        (lambda d: d["attempts"][0].update(idempotency_key="b-000000000000"), "idempotency"),
        # A started transfer shown as returned: no fence proves it cannot land.
        (lambda d: (d.update(state="returned"), d["attempts"][0].update(
            phase="returned", returned_at=110.0, return_reason="x", charge_released_at=110.0)),
         "returned after its transfer began"),
        (lambda d: d["attempts"][0].update(charge_released_at=110.0), "released the charge"),
        (lambda d: d.update(state="held"), "contradicts"),
    ],
)
def test_contradictory_records_are_refused(root, payload, mutate, message):
    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
        _rewrite(root, record, mutate)
        with pytest.raises(BrokerRecordCorrupt, match=message):
            store.get(record.broker_job_id)


def test_scan_reports_unusable_records_and_charges_refuse_to_guess(root, payload):
    with BrokerStore.open(root) as store:
        good = _reserve(store, store.accept(payload, _intent(), now=100.0))
        bad = store.accept(payload, _intent(), now=100.0)
        (root / "records" / f"{bad.broker_job_id}.json").write_text("{not json")
        (root / "records" / "stray.txt").write_text("")
        (root / "records" / "b-0123456789ab.json.1234.tmp").write_text("")

        records, problems = store.scan()
        assert [r.broker_job_id for r in records] == [good.broker_job_id]
        assert set(problems) == {bad.broker_job_id, "stray.txt"}
        with pytest.raises(BrokerRecordCorrupt, match=bad.broker_job_id):
            store.outstanding_charges()


def test_symlinked_record_is_refused(root, payload, tmp_path):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        path = root / "records" / f"{record.broker_job_id}.json"
        elsewhere = tmp_path / "elsewhere.json"
        elsewhere.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(elsewhere)
        with pytest.raises(BrokerRecordCorrupt):
            store.get(record.broker_job_id)


# -- intent validation ------------------------------------------------------------------


@pytest.mark.parametrize(
    "changes",
    [
        {"mem_mb": 0},
        {"cpus": True},
        {"cpus": 0},
        {"command": ()},
        {"command": ("--flag",)},
        {"command": ["python3", "run.py"]},
        {"expected_sha": "abcdef1234"},  # needs a program
        {"wall_time_seconds": 1.5},
        {"pool": ""},
        {"auto_resume": 1},
        {"tags": ("ok", 3)},
    ],
)
def test_invalid_intent_is_refused(changes):
    with pytest.raises(BrokerStoreError):
        _intent(**changes)


def test_intent_round_trips_and_has_no_target_specific_fields():
    intent = _intent(
        job_name="scan", program="vibeqc", expected_sha="abcdef1234", tags=("a", "b"),
        wall_time_seconds=3600, pool="gpu",
    )
    assert BrokerIntent.from_json(json.loads(json.dumps(intent.to_json()))) == intent
    for field_name in ("scheduler_target", "array", "chain", "depends_on", "python", "branch"):
        assert field_name not in intent.to_json()
    assert intent.placement_request() == overview.BrokerPlacementRequest(
        cpus=4, mem_mb=8000, pool="gpu", wall_time_seconds=3600)


def _append_diagnostics(root: str, job_id: str, count: int) -> None:
    from vq.broker_store import BrokerRecordConflict, BrokerStore

    with BrokerStore.open(broker_store.Path(root)) as store:
        done = 0
        while done < count:
            current = store.get(job_id)
            try:
                store.record_transfer_diagnostic(
                    job_id, current.revision, message=f"{os.getpid()}-{done}", now=200.0
                )
            except BrokerRecordConflict:
                continue
            done += 1


def test_transitions_from_several_processes_never_lose_an_update(root, payload):
    import multiprocessing

    with BrokerStore.open(root) as store:
        record = _transferring(store, payload)
    context = multiprocessing.get_context("spawn")
    workers = [
        context.Process(target=_append_diagnostics, args=(str(root), record.broker_job_id, 10))
        for _ in range(4)
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=60)
        assert worker.exitcode == 0
    with BrokerStore.open(root) as store:
        final = store.get(record.broker_job_id)
    assert final.revision == record.revision + 40
    assert len(final.current_attempt.diagnostics) == 40


def test_held_blockers_use_revision_cas_and_cannot_overwrite_cancellation(root, payload):
    with BrokerStore.open(root) as store:
        record = store.accept(payload, _intent(), now=100.0)
        assert "held_blockers" not in record.to_json()
        assert broker_store.parse_record(record.to_json()) == record
        saved = store.record_held_blockers(
            record.broker_job_id, record.revision,
            blockers=(("alpha", "unreachable"),), now=105.0,
        )
        with pytest.raises(BrokerRecordConflict):
            store.record_held_blockers(
                record.broker_job_id, record.revision,
                blockers=(("alpha", "memory-insufficient"),), now=106.0,
            )
        cancelled = store.request_cancel(
            saved.broker_job_id, saved.revision, reason="cancel", now=107.0,
        )
        with pytest.raises(BrokerRecordConflict):
            store.record_held_blockers(
                cancelled.broker_job_id, cancelled.revision,
                blockers=(("alpha", "memory-insufficient"),), now=108.0,
            )
        assert store.get(record.broker_job_id) == cancelled
        assert store.outstanding_charges() == {}


@pytest.mark.parametrize("blockers", [[], None, {"alpha": []}, {"": "unreachable"},
                                     {"alpha": ""}, {"alpha": 1}])
def test_malformed_held_blockers_fail_closed(root, payload, blockers):
    with BrokerStore.open(root) as store:
        raw = store.accept(payload, _intent(), now=100.0).to_json()
    raw["held_blockers"] = blockers
    with pytest.raises(BrokerRecordCorrupt, match="held blockers"):
        broker_store.parse_record(raw)
