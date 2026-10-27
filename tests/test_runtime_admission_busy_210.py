"""Authenticated authority contention defers, never terminalizes or dispatches."""

import json

import pytest

from tests.test_runtime_admission_192 import INTENT, JOB, KEY, OWNER, cli_submit, reserve
from vq import runtime_admission as admission

pytest_plugins = ["tests.test_runtime_admission_192"]


def test_real_lock_contention_is_typed_and_preserves_original_reservation(setup_store):
    store, identity = setup_store
    with store.transaction():
        original = reserve(store, identity)
    other = admission.ReservationStore(store.directory, store.verify)
    with other.transaction(), pytest.raises(admission.AdmissionBusy), store.transaction():
        store.commit(KEY, owner=OWNER, intent=INTENT, job=JOB)
    with store.transaction():
        assert store._read(KEY) == original
        store.commit(KEY, owner=OWNER, intent=INTENT, job=JOB)
        assert store.check(KEY, owner=OWNER, intent=INTENT, job=JOB)["state"] == "committed"


def test_pending_dispatch_recovers_commit_and_check_contention(provider_submission, monkeypatch):
    from tests.test_daemon_scheduler import MockDispatcher, _inject
    from vq import cgroup, daemon, paths, runtime_participant
    from vq.scheduler_dialect import SchedulerPhase
    from vq.spec import JobSpec, JobState

    _, _, _, script = provider_submission
    result = cli_submit(script)
    assert result.exit_code == 0, result.output
    path = next(paths.queue_dir().glob("*.json"))
    original = path.read_bytes()
    spec = JobSpec.read(path)
    runner = runtime_participant.RUNNER
    busy = ["commit"]

    def respond(host, argv, stdin, timeout):
        request = json.loads(stdin)
        if request["op"] == "generation-" + busy[0]:
            response = dict(
                schema=runtime_participant.SCHEMA,
                op=request["op"],
                op_id=request["op_id"],
                outcome="refused",
                receipt=None,
                receipt_sha256=None,
                participant={"adapter_sha256": "6" * 64},
                errors=["AdmissionError: provider cleanup/admission authority is busy"],
            )
            return 0, json.dumps(response), ""
        return runner(host, argv, stdin, timeout)

    monkeypatch.setattr(runtime_participant, "RUNNER", respond)
    monkeypatch.setattr(cgroup, "available", lambda: False)
    worker = daemon.Daemon(max_cpus=4, max_jobs=4)
    dispatcher = MockDispatcher(phase=SchedulerPhase.RUNNING)
    _inject(worker, dispatcher, target="cluster")
    assert not worker._start_job(spec)
    assert path.read_bytes() == original and not dispatcher.submitted
    assert worker._admission_retry_at[spec.id] > daemon.time.monotonic()
    # Cooldown does not call the provider again.
    monkeypatch.setattr(
        runtime_participant, "RUNNER", lambda *_: pytest.fail("busy cooldown ignored")
    )
    assert not worker._start_job(spec)
    monkeypatch.setattr(runtime_participant, "RUNNER", respond)
    busy[0] = "check"
    worker._admission_retry_at[spec.id] = 0
    assert not worker._start_job(spec)
    assert path.read_bytes() == original and not dispatcher.submitted
    busy[0] = "none"
    worker._admission_retry_at[spec.id] = 0
    assert worker._start_job(spec)
    assert JobSpec.read(path).state == JobState.RUNNING
    assert dispatcher.submitted == [spec.id]
    assert spec.id not in worker._admission_retry_at


@pytest.mark.parametrize(
    "message",
    [
        "AdmissionError: generation changed since its requested identity",
        "AdmissionError: provider cleanup/admission authority is busy; identity changed",
    ],
)
def test_only_exact_contention_is_transient(provider_submission, monkeypatch, message):
    from vq import config, runtime_participant

    *_, script = provider_submission

    def respond(host, argv, stdin, timeout):
        req = json.loads(stdin)
        return (
            0,
            json.dumps(
                dict(
                    schema=runtime_participant.SCHEMA,
                    op=req["op"],
                    op_id=req["op_id"],
                    outcome="refused",
                    receipt=None,
                    receipt_sha256=None,
                    participant={"adapter_sha256": "6" * 64},
                    errors=[message],
                )
            ),
            "",
        )

    monkeypatch.setattr(runtime_participant, "RUNNER", respond)
    with pytest.raises(admission.AdmissionError) as caught:
        admission._provider_call(config.load_config(), "cluster", "describe", {})
    assert not isinstance(caught.value, admission.AdmissionBusy)


@pytest.mark.parametrize("op", ["describe", "reserve", "commit"])
def test_busy_submit_boundary_reuses_same_intent(provider_submission, monkeypatch, op):
    from vq import paths, runtime_participant
    from vq.spec import JobSpec, JobState

    _, _, _, script = provider_submission
    runner = runtime_participant.RUNNER

    def busy(host, argv, stdin, timeout):
        req = json.loads(stdin)
        if req["op"] == "generation-" + op:
            return (
                0,
                json.dumps(
                    dict(
                        schema=runtime_participant.SCHEMA,
                        op=req["op"],
                        op_id=req["op_id"],
                        outcome="refused",
                        receipt=None,
                        receipt_sha256=None,
                        participant={"adapter_sha256": "6" * 64},
                        errors=["AdmissionBusy: provider cleanup/admission authority is busy"],
                    )
                ),
                "",
            )
        return runner(host, argv, stdin, timeout)

    monkeypatch.setattr(runtime_participant, "RUNNER", busy)
    cli_submit(script)
    assert bool(list(paths.queue_dir().glob("*.json"))) == (op == "commit")
    monkeypatch.setattr(runtime_participant, "RUNNER", runner)
    second = cli_submit(script)
    assert second.exit_code == 0, second.output
    specs = list(paths.queue_dir().glob("*.json"))
    assert len(specs) == 1 and JobSpec.read(specs[0]).state == JobState.PENDING
