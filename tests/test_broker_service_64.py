"""The broker role on the broker host: config, accept, pass, views (#64 B6)."""
from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests.test_broker_capacity_64 import _payload
from tests.test_broker_store_64 import _archive, _intent
from vq import broker_capacity, broker_service, broker_store, config
from vq.cli import main

ROOTS = {"alpha": "/srv/vq-state/alpha", "beta": "/srv/vq-state/beta"}


def _cfg(enabled=True, targets=("alpha",), **hosts):
    host_table = {
        "alpha": config.HostConfig(ssh="alpha"),
        "beta": config.HostConfig(ssh="beta"),
        **hosts,
    }
    return config.Config(
        hosts=host_table,
        broker={"enabled": enabled, "targets": list(targets)} if enabled else {},
    )


# -- configuration --------------------------------------------------------------


@pytest.mark.parametrize(
    ("broker", "message"),
    [
        ({"enabled": True}, "needs at least one target"),
        ({"enabled": True, "targets": ["ghost"]}, "not a configured host"),
        ({"enabled": True, "targets": ["alpha", "alpha"]}, "twice"),
        ({"enabled": True, "targets": ["cluster"]}, "scheduler lane"),
        ({"interval_seconds": 1}, "greater than or equal to 5"),
        ({"enabled": True, "targets": ["alias-a"]}, "canonical host"),
        ({"enabled": True, "targets": ["old"]}, "canonical host"),
    ],
)
def test_broker_config_is_validated_at_load(broker, message):
    hosts = {
        "alpha": config.HostConfig(ssh="alpha"),
        "cluster": config.HostConfig(ssh="c", scheduler="pbs", scheduler_dialect="torque",
                                     scratch_root="/s", scheduler_driver="alpha"),
        "alias-a": config.HostConfig(ssh="alpha", fleet_role="alias", fleet_canonical_host="alpha"),
        "old": config.HostConfig(ssh="old", fleet_role="excluded"),
    }
    with pytest.raises(ValueError, match=message):
        config.Config(hosts=hosts, broker=broker)


def test_the_broker_role_is_off_by_default():
    assert config.Config().broker.enabled is False
    with pytest.raises(broker_service.BrokerDisabled, match="not a broker"):
        broker_service.require_enabled(config.Config())


# -- accept -----------------------------------------------------------------------


def test_accept_holds_a_relayed_submission(tmp_path):
    payload = _archive(tmp_path / "upload.tar")
    record = broker_service.accept(_cfg(), payload, _intent(), root=tmp_path / "broker",
                                   clock=lambda: 100.0)
    assert record.state == "held" and record.created_at == 100.0


def test_accept_refuses_on_a_host_that_is_not_a_broker(tmp_path):
    payload = _archive(tmp_path / "upload.tar")
    with pytest.raises(broker_service.BrokerDisabled):
        broker_service.accept(_cfg(enabled=False), payload, _intent(), root=tmp_path / "b")
    assert not (tmp_path / "b").exists()


# -- one pass ---------------------------------------------------------------------


def _evidence(host, **changes):
    return broker_capacity.evidence_from_payload(
        host, _payload(state_root=ROOTS[host], **changes), observed_at=100.0
    )


def test_a_pass_collects_evidence_then_places_in_fifo_order(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg(targets=("alpha", "beta"))
    first = broker_service.accept(cfg, payload, _intent(program="vibeqc"), root=root,
                                  clock=lambda: 100.0)
    second = broker_service.accept(cfg, payload, _intent(), root=root, clock=lambda: 101.0)
    collected = {}
    submitted = []

    def collect(cfg_, hosts, *, clock, timeout, programs):
        collected.update(hosts=list(hosts), timeout=timeout, programs=list(programs))
        return {"alpha": _evidence("alpha", free_jobs=2), "beta": _evidence("beta")}

    def submit(request):
        submitted.append(request)
        return f"{len(submitted):012x}"

    report = broker_service.run_pass(cfg, root=root, clock=lambda: 105.0,
                                     collect=collect, submit=submit)

    assert collected == {"hosts": ["alpha", "beta"], "timeout": 60.0, "programs": ["vibeqc"]}
    assert [o["broker_job_id"] for o in report["placements"]] == [
        first.broker_job_id, second.broker_job_id]
    assert {o["outcome"] for o in report["placements"]} == {"placed"}
    assert [r.idempotency_key for r in submitted] == [first.broker_job_id, second.broker_job_id]
    assert report["targets"]["alpha"] == {"error": None, "closed": [], "state_root": ROOTS["alpha"]}
    json.dumps(report, default=str)


def test_a_pass_never_places_outside_the_configured_targets(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg(targets=("alpha",))
    record = broker_service.accept(cfg, payload, _intent(), root=root, clock=lambda: 100.0)

    def collect(cfg_, hosts, **kwargs):
        # A collector that (wrongly) answers for more hosts than it was asked.
        return {"alpha": _evidence("alpha", daemon_ok=False), "beta": _evidence("beta")}

    report = broker_service.run_pass(cfg, root=root, clock=lambda: 105.0, collect=collect,
                                     submit=lambda request: pytest.fail("must not submit"))

    assert report["head"] == record.broker_job_id
    assert report["placements"][0]["outcome"] == "held"


def test_a_pass_refuses_on_a_host_that_is_not_a_broker(tmp_path):
    with pytest.raises(broker_service.BrokerDisabled):
        broker_service.run_pass(_cfg(enabled=False), root=tmp_path / "b")


# -- views ------------------------------------------------------------------------


def test_views_show_state_target_and_the_operator_action(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg()
    record = broker_service.accept(cfg, payload, _intent(), root=root, clock=lambda: 100.0)

    def fail(request):
        raise TimeoutError("ssh timed out")

    broker_service.run_pass(cfg, root=root, clock=lambda: 105.0,
                            collect=lambda *a, **k: {"alpha": _evidence("alpha")}, submit=fail)
    with broker_store.BrokerStore.open(root) as store:
        current = store.get(record.broker_job_id)
        store.request_cancel(record.broker_job_id, current.revision, reason="u", now=106.0)

    (view,), problems = broker_service.list_records(root)
    assert problems == {}
    assert (view["state"], view["target_host"]) == ("transferring", "alpha")
    assert "kill it there" in view["operator_action"]
    assert broker_service.show_record(record.broker_job_id, root) == view


# -- CLI --------------------------------------------------------------------------


@pytest.fixture
def broker_host(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    monkeypatch.setattr(config, "load_config", lambda: _cfg())
    return tmp_path


def test_cli_accept_list_and_show(broker_host):
    payload = _archive(broker_host / "upload.tar")
    accepted = CliRunner().invoke(main, [
        "broker", "accept", str(payload), "--cpus", "2", "--mem-mb", "2000",
        "--program", "vibeqc", "--tag", "a", "--", "python3", "run.py",
    ])
    assert accepted.exit_code == 0, accepted.output
    job_id = accepted.output.strip()
    assert broker_store.BROKER_JOB_ID_PATTERN.fullmatch(job_id)

    listed = CliRunner().invoke(main, ["broker", "list", "--json"])
    (view,) = json.loads(listed.output)["jobs"]
    assert (view["broker_job_id"], view["state"], view["program"]) == (job_id, "held", "vibeqc")
    shown = CliRunner().invoke(main, ["broker", "show", job_id])
    assert shown.exit_code == 0 and f"broker_job_id: {job_id}" in shown.output


def test_cli_accept_requires_memory(broker_host):
    payload = _archive(broker_host / "upload.tar")
    result = CliRunner().invoke(main, ["broker", "accept", str(payload), "--cpus", "2",
                                       "--", "python3", "run.py"])
    assert result.exit_code == 2 and "--mem-mb" in result.output


def test_cli_refuses_on_a_host_that_is_not_a_broker(broker_host, monkeypatch):
    monkeypatch.setattr(config, "load_config", lambda: _cfg(enabled=False))
    for argv in (["broker", "list"], ["broker", "pass"], ["broker", "show", "b-0123456789ab"]):
        result = CliRunner().invoke(main, argv)
        assert result.exit_code == 1 and "not a broker" in result.output, argv


# -- the daemon runs the pass ---------------------------------------------------------


def _daemon(tmp_path):
    from vq.daemon import Daemon

    d = Daemon(poll_interval=0.05, queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs",
               max_cpus=4)
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


class Child:
    def __init__(self):
        self.returncode = None

    def poll(self):
        return self.returncode


def test_the_daemon_starts_one_pass_per_interval(tmp_path, monkeypatch):
    from vq import daemon as daemon_module

    d = _daemon(tmp_path)
    try:
        clock = SimpleNamespace(now=1000.0)
        monkeypatch.setattr(daemon_module.time, "monotonic", lambda: clock.now)
        cfg = _cfg()
        monkeypatch.setattr(daemon_module, "load_config", lambda: cfg)
        started = []
        monkeypatch.setattr(d, "_spawn_broker_pass_child",
                            lambda: started.append(clock.now) or Child())

        d._maybe_broker_pass()
        assert started == [1000.0]
        clock.now += 30
        d._maybe_broker_pass()  # still running: never a second one
        d._broker_pass_child.returncode = 0
        d._maybe_broker_pass()  # finished, but the interval has not elapsed
        assert started == [1000.0]
        clock.now += 31
        d._maybe_broker_pass()
        assert started == [1000.0, 1061.0]
    finally:
        d._queue_lock_fd.close()


def test_the_daemon_starts_nothing_without_the_role(tmp_path, monkeypatch):
    from vq import daemon as daemon_module

    d = _daemon(tmp_path)
    try:
        monkeypatch.setattr(daemon_module, "load_config", lambda: _cfg(enabled=False))
        monkeypatch.setattr(d, "_spawn_broker_pass_child", lambda: pytest.fail("spawned"))
        d._maybe_broker_pass()

        def broken():
            raise config.ConfigError("broken config")

        d._broker_settings_checked = None
        monkeypatch.setattr(daemon_module, "load_config", broken)
        d._maybe_broker_pass()  # logged, not raised
    finally:
        d._queue_lock_fd.close()


def test_the_real_spawn_runs_the_pass_command(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    d = _daemon(tmp_path)
    try:
        seen = []
        monkeypatch.setattr(subprocess, "Popen",
                            lambda argv, **kwargs: seen.append((argv, kwargs)) or Child())
        d._spawn_broker_pass_child()
        (argv, kwargs), = seen
        assert argv[-4:] == ["vq.cli", "broker", "pass", "--json"][-4:]
        assert kwargs["start_new_session"] is True
    finally:
        d._queue_lock_fd.close()


# -- review follow-ups -------------------------------------------------------------


def test_targets_sharing_one_state_root_are_both_refused(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg(targets=("alpha", "beta"))
    record = broker_service.accept(cfg, payload, _intent(), root=root, clock=lambda: 100.0)
    same = broker_capacity.evidence_from_payload(
        "beta", _payload(state_root=ROOTS["alpha"]), observed_at=100.0)

    report = broker_service.run_pass(
        cfg, root=root, clock=lambda: 105.0,
        collect=lambda *a, **k: {"alpha": _evidence("alpha"), "beta": same},
        submit=lambda request: pytest.fail("must not submit"))

    assert report["head"] == record.broker_job_id
    assert "share one state root" in report["placements"][0]["detail"]


@pytest.mark.parametrize(
    ("pools", "pool", "message"),
    [({}, "gpu", "unknown pool"), ({"gpu": {"hosts": ["beta"]}}, "gpu", r"no \[broker\] target")],
)
def test_accept_refuses_a_pool_no_target_can_serve(tmp_path, pools, pool, message):
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg().model_copy(update={"pools": {
        name: config.PoolConfig(**value) for name, value in pools.items()}})
    with pytest.raises(broker_store.BrokerStoreError, match=message):
        broker_service.accept(cfg, payload, _intent(pool=pool), root=tmp_path / "broker")
    assert not (tmp_path / "broker" / "records").exists() or not list(
        (tmp_path / "broker" / "records").iterdir())


def test_the_programs_of_jobs_resumed_in_the_pass_are_asked_for(tmp_path):
    from tests import test_broker_placement_b4a_64 as b4a

    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg()
    first = broker_service.accept(cfg, payload, _intent(program="vibeqc"), root=root,
                                  clock=lambda: 100.0)
    with broker_store.BrokerStore.open(root) as store:
        b4a._reserve_only(SimpleNamespace(store=store), first)  # left by a crash
    seen = {}

    def collect(cfg_, hosts, *, programs, **kwargs):
        seen["programs"] = list(programs)
        return {"alpha": _evidence("alpha")}

    report = broker_service.run_pass(cfg, root=root, clock=lambda: 105.0, collect=collect,
                                     submit=lambda request: "0123456789ab")

    assert seen["programs"] == ["vibeqc"]
    assert report["placements"][0]["outcome"] == "placed"


def test_a_second_pass_stops_before_any_ssh(tmp_path):
    root = tmp_path / "broker"
    with (
        broker_store.BrokerStore.open(root) as other,
        other.pass_lock(),
        pytest.raises(broker_store.BrokerStoreBusy, match="placement pass"),
    ):
        broker_service.run_pass(_cfg(), root=root,
                                collect=lambda *a, **k: pytest.fail("collected"))


def test_cancel_a_held_job_and_repeat_idempotently(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    record = broker_service.accept(_cfg(), payload, _intent(), root=root, clock=lambda: 100.0)
    first = broker_service.cancel(_cfg(), record.broker_job_id, reason="u", root=root,
                                  clock=lambda: 101.0)
    again = broker_service.cancel(_cfg(), record.broker_job_id, reason="u", root=root)
    assert first.state == again.state == "cancelled"


def test_the_role_is_refused_on_a_multi_user_host(monkeypatch):
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    with pytest.raises(broker_service.BrokerDisabled, match="multi-user"):
        broker_service.require_enabled(_cfg())


def test_a_replay_to_a_target_without_evidence_says_so(tmp_path):
    root = tmp_path / "broker"
    payload = _archive(tmp_path / "upload.tar")
    cfg = _cfg()
    record = broker_service.accept(cfg, payload, _intent(), root=root, clock=lambda: 100.0)

    def fail(request):
        raise TimeoutError("ssh timed out")

    broker_service.run_pass(cfg, root=root, clock=lambda: 105.0,
                            collect=lambda *a, **k: {"alpha": _evidence("alpha")}, submit=fail)
    ticks = iter([1000.0 + i for i in range(100)])
    report = broker_service.run_pass(
        cfg, root=root, clock=lambda: next(ticks),
        collect=lambda *a, **k: {}, submit=lambda request: pytest.fail("sent"))
    assert "gave no capacity evidence" in report["replayed"][0]["detail"]
    assert record.broker_job_id == report["replayed"][0]["broker_job_id"]


def test_the_pass_log_keeps_one_previous_generation(tmp_path, monkeypatch):
    from vq import paths

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    (tmp_path / "state").mkdir()
    log = tmp_path / "state" / "broker-pass.log"
    log.write_bytes(b"x" * (8 * 1024 * 1024 + 1))
    d = _daemon(tmp_path)
    try:
        monkeypatch.setattr(subprocess, "Popen", lambda argv, **kwargs: Child())
        d._spawn_broker_pass_child()
        assert (tmp_path / "state" / "broker-pass.log.1").stat().st_size > 8 * 1024 * 1024
        assert log.stat().st_size == 0
    finally:
        d._queue_lock_fd.close()


def test_cli_cancel_and_the_list_flag(broker_host):
    payload = _archive(broker_host / "upload.tar")
    job_id = CliRunner().invoke(main, [
        "broker", "accept", str(payload), "--cpus", "2", "--mem-mb", "2000",
        "--", "python3", "run.py"]).output.strip()
    result = CliRunner().invoke(main, ["broker", "cancel", job_id])
    assert result.exit_code == 0 and f"{job_id}: cancelled" in result.output
