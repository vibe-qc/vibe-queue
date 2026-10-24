"""Slurm broker driver authority, admission, replay and proxy acceptance."""
from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import replace

import pytest

from tests.test_broker_capacity_64 import _admission, _payload, _spec, host  # noqa: F401
from tests.test_broker_store_64 import _archive, _intent
from vq import (
    broker_capacity,
    broker_relay,
    broker_service,
    broker_store,
    broker_target,
    config,
    overview,
    transport,
)
from vq.scheduler_admission import SchedulerTargetAdmission
from vq.spec import JobState


def cfg(backend="slurm", **changes):
    target = config.HostConfig(
        ssh="cluster", scheduler=backend,
        scheduler_dialect="slurm" if backend == "slurm" else "torque", scratch_root="/scratch",
        scheduler_driver="driver", scheduler_max_jobs=3, **changes,
    )
    return config.Config(hosts={
        "driver": config.HostConfig(ssh="localhost", remote_vq="/usr/local/bin/vq"),
        "cluster": target,
    }, broker={"enabled": True, "targets": ["cluster"]})


def evidence(c=None, **changes):
    c = c or cfg()
    _, authority = broker_target.route(c, "cluster")
    return broker_capacity.evidence_from_payload("cluster", _payload(
        scheduler_target="cluster", authority=authority, free_cpus=None, free_mem_mb=None,
        **changes,
    ), observed_at=100)


def test_scheduler_admission_is_not_driver_cpu_or_memory():
    found = evidence()
    request = overview.BrokerPlacementRequest(cpus=256, mem_mb=500000)
    assert overview.broker_host_blocker(found.snapshot, request,
                                       now=101, max_age_seconds=30) is None
    assert overview.place_broker_job([found.snapshot], request,
                                    now=101, max_age_seconds=30).host == "cluster"
    for slots, expected in [(None, "capacity-unknown"), (0, "job-slots-insufficient")]:
        assert overview.broker_host_blocker(evidence(free_jobs=slots).snapshot, request,
                                            now=101, max_age_seconds=30) == expected


def test_scheduler_budget_counts_alias_and_uncertain_reservations():
    c = cfg()
    c.hosts["alias"] = c.hosts["cluster"].model_copy(update={
        "fleet_role": "alias", "fleet_canonical_host": "cluster", "scheduler_max_jobs": 2,
    })
    admission = SchedulerTargetAdmission(c, [("alias", 1), (None, 1)], {})
    assert admission.free_jobs("cluster") == 0
    assert admission.total_reserved() == 2
    assert not admission.can_attempt("cluster", 100)
    config.Config(hosts=c.hosts, broker={"enabled": True, "targets": ["cluster", "alias"]})


@pytest.mark.parametrize("changes", [{"scheduler_driver": "absent"}])
def test_scheduler_missing_driver_or_budget_is_refused(changes):
    c = cfg()
    c.hosts["cluster"] = c.hosts["cluster"].model_copy(update=changes)
    with pytest.raises(ValueError):
        config.Config(hosts=c.hosts, broker=c.broker)


def test_capacity_uses_target_snapshot_and_preserves_holds(host):  # noqa: F811
    c = cfg()
    _, binding = broker_target.route(c, "cluster")
    row = {"holds": [], "free_jobs": 2, "can_attempt": True, "queue_ready": True,
           "authority": binding}
    host.admission = _admission(holds=["local memory pressure"], scheduler_targets={"cluster": row})
    payload = broker_capacity.sample_local(c, now=lambda: 10000, scheduler_target="cluster")
    assert payload["admission"]["open"]
    assert payload["free_cpus"] is None and payload["free_mem_mb"] is None
    assert payload["free_jobs"] == 2
    row["holds"] = ["target is drained"]
    assert not broker_capacity.sample_local(c, now=lambda: 10000,
                                           scheduler_target="cluster")["admission"]["open"]
    row["holds"] = []
    host.specs = [_spec("aaaaaaaaaaaa", JobState.PENDING, scheduler_target="cluster")]
    assert not broker_capacity.sample_local(c, now=lambda: 10000,
                                           scheduler_target="cluster")["queue_ready"]
    row["authority"] = {**binding, "target_config_sha256": "changed"}
    assert not broker_capacity.sample_local(c, now=lambda: 10000,
                                           scheduler_target="cluster")["admission"]["open"]


def test_capacity_routes_to_driver_and_rejects_different_authority():
    c = cfg()
    calls = []
    _, binding = broker_target.route(c, "cluster")
    payload = _payload(scheduler_target="cluster", authority=binding)
    def run(h, *argv, **kwargs):
        calls.append((h, argv))
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload), "")
    broker_capacity.fetch_payload(c, "cluster", runner=run)
    assert calls[0][0] == c.hosts["driver"]
    assert calls[0][1][-2:] == ("--scheduler-target", "cluster")
    payload["authority"] = {**binding, "driver": "other"}
    with pytest.raises(ValueError, match="different scheduler authority"):
        broker_capacity.fetch_payload(c, "cluster", runner=run)


@pytest.mark.parametrize("backend", ["slurm", "pbs"])
def test_restart_replays_one_key_through_bound_driver_and_proxies(tmp_path, monkeypatch, backend):
    c = cfg(backend)
    root = tmp_path / "broker"
    archive = _archive(tmp_path / "payload.tar")
    record = broker_service.accept(c, archive, _intent(program="vibeqc"),
                                   root=root, clock=lambda: 100)
    requests = []
    receipt = "abcdef012345"
    lose_first = True
    def run(h, *argv, **kwargs):
        nonlocal lose_first
        assert h == c.hosts["driver"]
        if argv[0] == "submit":
            requests.append(argv)
            assert argv[1] == "cluster"
            assert argv[argv.index("--idempotency-key") + 1] == record.broker_job_id
            assert argv[argv.index("--expected-sha") + 1] == "a" * 40
            if lose_first:
                lose_first = False
                raise transport.RemoteOutcomeUnknown("lost reply after admission")
            return subprocess.CompletedProcess(argv, 0, receipt + "\n", "")
        return subprocess.CompletedProcess(argv, 0, "{}", "")
    monkeypatch.setattr(broker_target, "run", run)
    observations = 0
    def collect(*args, **kwargs):
        nonlocal observations
        observations += 1
        # The deployment changes after admission; replay must retain A.
        return {"cluster": evidence(c, runtime_pins={
            "vibeqc": ("a" if observations == 1 else "b") * 40,
        })}
    broker_service.run_pass(c, root=root, clock=lambda: 101, collect=collect)
    with broker_store.BrokerStore.open(root) as store:
        first = store.get(record.broker_job_id)
        assert first.state == "transferring"
        fingerprint = first.current_attempt.request_fingerprint
    # New store/pass simulates broker restart after lost acknowledgement.
    broker_service.run_pass(c, root=root, clock=lambda: 1001, collect=collect)
    with broker_store.BrokerStore.open(root) as store:
        placed = store.get(record.broker_job_id)
    assert placed.state == "placed" and placed.current_attempt.target_job_id == receipt, placed
    assert placed.current_attempt.request_fingerprint == fingerprint
    assert requests[0] == requests[1]
    assert broker_relay.target_of(c, placed) == (c.hosts["driver"], receipt)
    broker_relay.status_payload(c, placed)
    broker_relay.proxy_text(c, placed, "kill")
    c.hosts["cluster"] = c.hosts["cluster"].model_copy(update={"scratch_root": "/other"})
    with pytest.raises(ValueError, match="authority changed"):
        broker_relay.target_of(c, placed)
    with pytest.raises(ValueError, match="authority changed"):
        broker_service._fence_on_target(c, placed.current_attempt)


@pytest.mark.parametrize("backend", ["slurm", "pbs"])
def test_fence_uses_recorded_driver_and_root(tmp_path, monkeypatch, backend):
    c = cfg(backend)
    root = tmp_path / "broker"
    with broker_store.BrokerStore.open(root) as store:
        record = store.accept(_archive(tmp_path / "p.tar"), _intent(), now=100)
        snapshot = evidence(c).snapshot
        record = store.reserve(record.broker_job_id, record.revision, snapshot=snapshot,
                               decision=overview.BrokerPlacementDecision("cluster", (), ()),
                               target_state_root="/driver/state", now=101)
    def run(h, *argv, **kwargs):
        assert h == c.hosts["driver"]
        assert argv == ("admin", "fence-idempotency-key", record.broker_job_id)
        return subprocess.CompletedProcess(argv, 0, json.dumps({
            "schema": broker_service.FENCE_SCHEMA, "state": "fenced", "job_id": None,
            "state_root": "/driver/state",
        }), "")
    monkeypatch.setattr(broker_target, "run", run)
    assert broker_service._fence_on_target(c, record.current_attempt)["state"] == "fenced"
    with pytest.raises(ValueError, match="state root"):
        broker_service._fence_on_target(c, replace(record.current_attempt,
                                                 target_state_root="/old/state"))


def test_local_driver_does_not_inherit_gateway_isolated_state(monkeypatch):
    calls = []
    monkeypatch.setattr(transport, "run_owned_subprocess", lambda argv, **kw:
                        calls.append(argv) or subprocess.CompletedProcess(argv, 0, "ok", ""))
    broker_target.run(cfg().hosts["driver"], "status", "localhost", timeout=5)
    assert calls == [["/usr/bin/env", "-u", "VQ_CONFIG_DIR", "-u", "VQ_STATE_DIR",
                      "/usr/local/bin/vq", "status", "localhost"]]


@pytest.mark.parametrize("backend", ["slurm", "pbs"])
def test_daemon_publishes_target_holds_and_global_budget(tmp_path, monkeypatch, backend):
    from tests.test_broker_capacity_64 import _daemon
    from vq import daemon as daemon_mod
    c = cfg(backend)
    monkeypatch.setattr(daemon_mod, "load_config", lambda: c)
    d = _daemon(tmp_path, max_scheduler_jobs=2)
    try:
        admission = SchedulerTargetAdmission(c, [("cluster", 1), ("other", 1)], {})
        d._broker_scheduler_facts = (c, admission, [])
        d._admission_spec_facts = (0, False, False)
        d._publish_admission_snapshot()
        row = d._admission_snapshot["scheduler_targets"]["cluster"]
        assert row["free_jobs"] == 0 and not row["holds"]
        d._broker_scheduler_facts = (c, admission, [])
        d._admin_update_marker_present = True
        d._admin_update_hold_scope = frozenset({"cluster"})
        d._admission_spec_facts = (0, False, False)
        d._publish_admission_snapshot()
        assert d._admission_snapshot["scheduler_targets"]["cluster"]["holds"]
        d._broker_scheduler_facts = None
        d._publish_admission_snapshot()
        assert not d._admission_snapshot["scheduler_targets"]
    finally:
        d._queue_lock_fd.close()


def test_local_driver_stream_and_command_use_real_helper(tmp_path, monkeypatch):
    helper = tmp_path / "vq-helper"
    helper.write_text(f'#!{sys.executable}\nimport os,sys\n'
                      'assert "VQ_STATE_DIR" not in os.environ\n'
                      'assert "VQ_CONFIG_DIR" not in os.environ\n'
                      'print(" ".join(sys.argv[1:]))\n')
    helper.chmod(0o700)
    monkeypatch.setenv("VQ_STATE_DIR", str(tmp_path / "isolated-state"))
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path / "isolated-cfg"))
    h = config.HostConfig(ssh="localhost", remote_vq=str(helper))
    assert broker_target.run(h, "status", "localhost", timeout=5).stdout == "status localhost\n"
    with broker_target.stream(h, "logs", "123456abcdef") as stream:
        assert stream.stdout.read() == b"logs 123456abcdef\n"


def test_configured_unlimited_budget_is_not_missing_evidence():
    c = cfg()
    c.hosts["cluster"] = c.hosts["cluster"].model_copy(update={"scheduler_max_jobs": None})
    config.Config(hosts=c.hosts, broker=c.broker)
    admission = SchedulerTargetAdmission(c, [("cluster", 4), (None, 1)], {})
    assert admission.free_jobs("cluster") == 1 << 30
    assert admission.can_attempt("cluster", 100)


def test_alias_placements_do_not_duplicate_driver_budget(tmp_path):
    from vq import broker_placement
    c = cfg()
    c.hosts["alias"] = c.hosts["cluster"].model_copy(update={
        "fleet_role": "alias", "fleet_canonical_host": "cluster",
    })
    c = config.Config(hosts=c.hosts, broker={"enabled": True,
                                           "targets": ["cluster", "alias"]})
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "payload.tar")
    ids = [broker_service.accept(c, payload, _intent(), root=root,
                                 clock=lambda: 100).broker_job_id for _ in range(3)]
    snapshots = [replace(evidence(c).snapshot, host=name, free_jobs=2,
                         authority=broker_target.route(c, name)[1])
                 for name in ["cluster", "alias"]]
    submitted = []
    with broker_store.BrokerStore.open(root) as store:
        placer = broker_placement.BrokerPlacer(
            store, submit=lambda request: submitted.append(request) or f"{len(submitted):012x}",
            state_root_of=lambda host: "/same/driver/state", clock=lambda: 101,
        )
        for job_id in ids:
            placer.place(job_id, snapshots, max_age_seconds=60)
        assert len(submitted) == 2
        assert store.get(ids[2]).state == "held"
        assert all(reason == "job-slots-insufficient"
                   for _, reason in store.get(ids[2]).held_blockers)
