"""Broker capacity evidence: target sampling and broker conversion (#64 B5/B6)."""
from __future__ import annotations

import json
import subprocess
from types import SimpleNamespace

import pytest

from tests import test_broker_placement_b4a_64 as b4a
from tests.test_broker_store_64 import _intent
from vq import broker_capacity, config, overview
from vq.spec import JobSpec, JobState

ROOT = "/srv/vq-state/alpha"
world = b4a.world


def _payload(**changes):
    payload = {
        "schema": broker_capacity.CAPACITY_SCHEMA,
        "sampled_at": 5.0,
        "state_root": ROOT,
        "daemon_ok": True,
        "state_root_agreement": True,
        "admission": {"open": True, "reasons": []},
        "queue_ready": True,
        "pending_local_jobs": 0,
        "pending_cpus": 0,
        "free_cpus": 8,
        "free_mem_mb": 16000,
        "free_jobs": 2,
        "idle_seconds": 30,
        "programs": {"vibeqc": "OK", "broken": "MISSING"},
        "jobs": ["0123456789ab"],
    }
    payload.update(changes)
    return payload


def _eligible(evidence, **request):
    return overview.broker_host_blocker(
        evidence.snapshot,
        overview.BrokerPlacementRequest(**{"cpus": 4, "mem_mb": 8000, **request}),
        now=evidence.snapshot.observed_at + 1, max_age_seconds=60.0,
    )


def test_a_complete_open_answer_is_eligible_evidence():
    evidence = broker_capacity.evidence_from_payload(
        "alpha", _payload(), observed_at=100.0, pools=frozenset({"gpu"})
    )

    assert evidence.error is None and _eligible(evidence) is None
    assert evidence.state_root == ROOT
    assert evidence.snapshot.observed_at == 100.0  # the broker's clock, not sampled_at
    assert evidence.snapshot.pools == frozenset({"gpu"})
    assert evidence.jobs == frozenset({"0123456789ab"})


@pytest.mark.parametrize(
    ("changes", "blocker"),
    [
        ({"daemon_ok": False}, "daemon-not-ready"),
        ({"state_root_agreement": False}, "state-root-unverified"),
        ({"state_root": None}, "state-root-unverified"),
        ({"admission": {"open": False, "reasons": ["the host is drained"]}}, "admission-blocked"),
        # A reason with open=true is still closed: open means no reason at all.
        ({"admission": {"open": True, "reasons": ["odd"]}}, "admission-blocked"),
        ({"queue_ready": False}, "queue-not-ready"),
        ({"free_mem_mb": None}, "capacity-unknown"),
        ({"free_cpus": 2}, "cpus-insufficient"),
    ],
)
def test_each_closed_gate_or_unknown_makes_the_host_ineligible(changes, blocker):
    evidence = broker_capacity.evidence_from_payload(
        "alpha", _payload(**changes), observed_at=100.0
    )
    assert _eligible(evidence) == blocker


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        _payload(schema="vq.broker-capacity/9"),
        _payload(daemon_ok="yes"),
        _payload(free_cpus=4.5),
        _payload(free_jobs=True),
        _payload(state_root="relative"),
        _payload(jobs="0123456789ab"),
        _payload(programs={"vibeqc": 1}),
        {k: v for k, v in _payload().items() if k != "admission"},
    ],
)
def test_a_malformed_answer_is_no_evidence(payload):
    evidence = broker_capacity.evidence_from_payload("alpha", payload, observed_at=100.0)
    assert evidence.error and "unusable capacity answer" in evidence.error
    assert _eligible(evidence) is not None
    assert evidence.state_root is None


def test_collect_stamps_the_broker_clock_and_survives_unreachable_hosts():
    cfg = SimpleNamespace(
        hosts={"alpha": SimpleNamespace(ssh="alpha"), "beta": SimpleNamespace(ssh="beta")},
        pools={"gpu": SimpleNamespace(hosts=["alpha"])},
    )
    calls = []

    def runner(host_cfg, *argv, **kwargs):
        calls.append((host_cfg.ssh, argv, kwargs["timeout"]))
        if host_cfg.ssh == "beta":
            raise TimeoutError("ssh timed out")
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(_payload()))

    ticks = iter([500.0, 600.0])
    evidence = broker_capacity.collect(
        cfg, ["alpha", "beta", "ghost"], runner=runner, clock=lambda: next(ticks, 700.0),
        timeout=20.0,
    )

    assert set(evidence) == {"alpha", "beta", "ghost"}
    assert evidence["alpha"].error is None
    assert evidence["alpha"].snapshot.pools == frozenset({"gpu"})
    assert evidence["alpha"].snapshot.observed_at in {500.0, 600.0, 700.0}
    assert "no capacity evidence" in evidence["beta"].error
    assert "not configured" in evidence["ghost"].error
    assert {argv for _host, argv, _timeout in calls} == {("broker", "capacity", "--json")}
    assert {timeout for *_rest, timeout in calls} == {20.0}


def test_program_refusal_needs_the_program_reported_ok():
    evidence = {"alpha": broker_capacity.evidence_from_payload(
        "alpha", _payload(), observed_at=1.0)}
    refuse = broker_capacity.program_refusal(evidence)

    def job(program):
        return SimpleNamespace(intent=SimpleNamespace(program=program))

    assert refuse(job(None), "alpha") is None
    assert refuse(job("vibeqc"), "alpha") is None
    assert "MISSING" in refuse(job("broken"), "alpha")
    assert "not registered" in refuse(job("other"), "alpha")
    assert "not registered" in refuse(job("vibeqc"), "beta")


def test_a_host_missing_the_program_falls_back_to_one_that_has_it(world):
    evidence = {
        "alpha": broker_capacity.evidence_from_payload(
            "alpha", _payload(programs={}), observed_at=100.0),
        "beta": broker_capacity.evidence_from_payload(
            "beta", _payload(programs={"vibeqc": "OK"}, free_mem_mb=12000), observed_at=100.0),
    }
    world.placer._job_refusal = broker_capacity.program_refusal(evidence)
    record = world.store.accept(world.payload, _intent(program="vibeqc"), now=100.0)

    outcome = world.placer.place(
        record.broker_job_id, [e.snapshot for e in evidence.values()], max_age_seconds=60.0
    )

    assert outcome.host == "beta"
    blockers = world.store.get(record.broker_job_id).current_attempt.blockers
    assert blockers["alpha"].startswith("job-refused: program vibeqc")


# -- the target-side sampler --------------------------------------------------


def _spec(job_id, state, *, cpus=2, mem_mb=1000, **fields):
    spec = JobSpec(id=job_id, cwd="/tmp", cpus=cpus, mem_mb=mem_mb,
                   command=["sh", "run.sh"], **fields)
    spec.state = state
    return spec


def _admission(**changes):
    snapshot = {
        "schema": "vq.daemon-admission/1", "published_at": 9_995.0, "holds": [],
        "local_pending": 0, "max_cpus": 16, "max_jobs": 4, "max_mem_mb": 32000,
        "local_active": 2, "used_cpus": 6, "used_mem_mb": 5000,
    }
    snapshot.update(changes)
    return snapshot


@pytest.fixture
def host(monkeypatch):
    """A healthy single-user daemon host; tests change one input at a time."""
    from vq import daemon_control, hostmem, listing, rpc

    state = SimpleNamespace(
        ping={"ok": True, "state_roots": {
            "client": ROOT, "daemons": [{"pid": 7, "state_root": ROOT}]}},
        admission=_admission(),
        rpc_error=None,
        mem_available=20000,
        specs=[],
        multi_user=False,
        rpc_calls=[],
    )

    def call(method, args=None, *, multi_user=False, timeout=None, socket_override=None):
        state.rpc_calls.append(method)
        if state.rpc_error is not None:
            raise state.rpc_error
        return state.admission

    monkeypatch.setattr(daemon_control, "local_daemon_ping",
                        lambda timeout, *, multi_user, verbose: (0, state.ping))
    monkeypatch.setattr(rpc, "call", call)
    monkeypatch.setattr(hostmem, "sample_host_mem",
                        lambda: SimpleNamespace(mem_available_mb=state.mem_available))
    monkeypatch.setattr(listing, "list_jobs", lambda host, **kwargs: list(state.specs))
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: state.multi_user)
    return state


def _sample(programs=None, names=None):
    cfg = config.Config(programs=programs or {})
    return broker_capacity.sample_local(cfg, now=lambda: 10_000.0, programs=names)


def test_the_daemons_own_snapshot_decides_admission_and_headroom(host):
    host.specs = [
        _spec("aaaaaaaaaaaa", JobState.RUNNING),
        _spec("cccccccccccc", JobState.COMPLETED, finished_at="1970-01-01T02:00:00+00:00"),
        _spec("dddddddddddd", JobState.PENDING, scheduler_target="cluster"),
    ]

    payload = _sample()

    assert host.rpc_calls == ["get_admission_snapshot"]
    assert payload["admission"] == {"open": True, "reasons": []}
    assert payload["queue_ready"] is True
    assert payload["state_root"] == ROOT
    assert payload["free_cpus"] == 16 - 6
    assert payload["free_jobs"] == 4 - 2
    # Budget remainder 32000 - 5000 = 27000; live free memory 20000 is lower.
    assert payload["free_mem_mb"] == 20000
    assert payload["jobs"] == ["aaaaaaaaaaaa", "cccccccccccc", "dddddddddddd"]
    json.dumps(payload)


def test_the_budget_remainder_bounds_memory_when_it_is_lower(host):
    host.admission = _admission(used_mem_mb=30000)
    assert _sample()["free_mem_mb"] == 2000


def test_an_unknown_usage_charge_leaves_the_budget_unproven(host):
    host.admission = _admission(used_mem_mb=None)
    host.mem_available = 20000
    # Live free memory alone must not stand in for a budget it cannot prove.
    assert _sample()["free_mem_mb"] is None


def test_no_job_cap_is_large_headroom_not_unknown(host):
    host.admission = _admission(max_jobs=None)
    assert _sample()["free_jobs"] == broker_capacity.UNLIMITED_JOBS


def test_daemon_holds_close_admission_verbatim(host):
    host.admission = _admission(holds=["dispatch is held for host memory pressure"])
    payload = _sample()
    assert payload["admission"]["open"] is False
    assert "dispatch is held for host memory pressure" in payload["admission"]["reasons"]


def test_pending_local_jobs_in_the_daemons_count_mean_not_ready(host):
    host.admission = _admission(local_pending=1)
    assert _sample()["queue_ready"] is False


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        (lambda h: setattr(h, "ping", {"ok": False}), "did not answer"),
        (lambda h: h.ping.update(state_roots={"client": ROOT, "daemons": [
            {"pid": 7, "state_root": "/elsewhere"}]}), "state root"),
        (lambda h: h.ping.update(state_roots=None), "state root"),
        (lambda h: setattr(h, "multi_user", True), "multi-user"),
        (lambda h: setattr(h, "rpc_error", RuntimeError("unknown method")), "no admission"),
        (lambda h: setattr(h, "admission", None), "malformed"),
        (lambda h: setattr(h, "admission", _admission(schema="other")), "malformed"),
        (lambda h: setattr(h, "admission", _admission(used_cpus=True)), "malformed"),
        (lambda h: setattr(h, "admission", _admission(max_cpus=0)), "malformed"),
        (lambda h: setattr(h, "admission", _admission(holds="x")), "malformed"),
        (lambda h: setattr(h, "admission", _admission(published_at=9_000.0)), "s old"),
        (lambda h: setattr(h, "admission", _admission(published_at=10_060.0)), "s old"),
    ],
)
def test_each_missing_proof_closes_admission(host, change, reason):
    change(host)
    payload = _sample()
    assert payload["admission"]["open"] is False
    assert any(reason in r for r in payload["admission"]["reasons"]), payload["admission"]


def test_a_rejected_snapshot_gives_no_headroom_and_no_ready_queue(host):
    host.admission = _admission(published_at=0.0)
    payload = _sample()
    assert (payload["free_cpus"], payload["free_mem_mb"], payload["free_jobs"]) == (None,) * 3
    assert payload["queue_ready"] is False


def test_only_the_named_programs_are_checked(host, tmp_path):
    missing = config.VenvProgram(kind="venv", python=str(tmp_path / "none/bin/python"),
                                 git_dir=str(tmp_path / "none"))
    payload = _sample({"vibeqc": missing}, names=["vibeqc", "ghost"])
    assert payload["programs"] == {"vibeqc": "MISSING", "ghost": "NOT_REGISTERED"}


def test_real_sampler_output_converts_through_the_broker_side(host):
    evidence = broker_capacity.evidence_from_payload(
        "alpha", _sample(), observed_at=1_000.0
    )
    assert evidence.error is None and _eligible(evidence) is None


def test_the_cli_prints_both_forms(host, monkeypatch):
    import time

    from click.testing import CliRunner

    from vq.cli import main

    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    host.admission = _admission(published_at=time.time())
    as_json = CliRunner().invoke(main, ["broker", "capacity", "--json"])
    assert as_json.exit_code == 0, as_json.output
    assert json.loads(as_json.output)["schema"] == broker_capacity.CAPACITY_SCHEMA
    text = CliRunner().invoke(main, ["broker", "capacity"])
    assert text.exit_code == 0 and "admission: open" in text.output


# -- the daemon publishes what it decides dispatch with ----------------------------


def _daemon(tmp_path, **caps):
    from vq.daemon import Daemon

    d = Daemon(poll_interval=0.05, queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs",
               **{"max_cpus": 8, "max_jobs": 3, "max_mem_mb": 16000, **caps})
    d.queue_dir.mkdir(parents=True, exist_ok=True)
    d.jobs_dir.mkdir(parents=True, exist_ok=True)
    return d


def _tick(d):
    d._dispatch_pending()
    d._publish_admission_snapshot()
    return d._admission_snapshot


def test_orphans_and_surviving_killed_jobs_count_as_usage(tmp_path):
    from vq.daemon import _OrphanJob, _TerminalSurvivor

    d = _daemon(tmp_path)
    try:
        d._orphans["o1"] = _OrphanJob(pgid=900001, cpus=2, mem_mb=3000)
        d._terminal_survivors["k1"] = _TerminalSurvivor(
            pgid=900002, cpus=1, mem_mb=1000, state=JobState.KILLED,
            workspace=d.jobs_dir / "k1", deadline=float("inf"),
        )
        snapshot = _tick(d)
        assert (snapshot["local_active"], snapshot["used_cpus"], snapshot["used_mem_mb"]) == (
            2, 3, 4000)
        assert snapshot["holds"] == [] and snapshot["local_pending"] == 0
    finally:
        d._queue_lock_fd.close()


def test_the_host_pressure_flag_is_published_as_a_hold(tmp_path):
    d = _daemon(tmp_path)
    try:
        d.watchdog._host_pressure_active = True  # set by the pressure pass
        assert "dispatch is held for host memory pressure" in _tick(d)["holds"]
    finally:
        d._queue_lock_fd.close()


def test_a_pending_refresh_and_waiting_local_jobs_are_published(tmp_path):
    d = _daemon(tmp_path, max_jobs=1)
    try:
        from vq.daemon import _OrphanJob

        d._orphans["o1"] = _OrphanJob(pgid=900001, cpus=1, mem_mb=None)  # fills the slot
        pending = (("aaaaaaaaaaaa", {}), ("bbbbbbbbbbbb", {"refresh_before": "vibeqc"}))
        for job_id, fields in pending:
            ws = d.jobs_dir / job_id
            ws.mkdir(parents=True)
            JobSpec(id=job_id, command=["true"], cwd=str(ws), cpus=1, state=JobState.PENDING,
                    **fields).write(d._spec_path(job_id))
        snapshot = _tick(d)
        assert snapshot["local_pending"] == 2
        assert any("refresh" in hold for hold in snapshot["holds"])
        assert snapshot["used_mem_mb"] is None  # an undeclared orphan, no default
    finally:
        d._queue_lock_fd.close()


def test_a_publication_failure_withdraws_the_snapshot(tmp_path, monkeypatch):
    d = _daemon(tmp_path)
    try:
        _tick(d)
        assert d._admission_snapshot is not None
        from vq import drain

        def broken(**kwargs):
            raise OSError("drain state unreadable")

        monkeypatch.setattr(drain, "read_effective_drain_state", broken)
        d._publish_admission_snapshot()
        assert d._admission_snapshot is None
    finally:
        d._queue_lock_fd.close()


def test_the_rpc_method_only_returns_the_published_value():
    from vq import rpc

    registered = {}
    server = SimpleNamespace(register=lambda name: lambda fn: registered.setdefault(name, fn))
    published = {"schema": "vq.daemon-admission/1"}
    rpc.register_get_admission_snapshot_method(server, lambda: published)
    answer = registered["get_admission_snapshot"]()
    assert answer == published and answer is not published
    rpc.register_get_admission_snapshot_method(server := SimpleNamespace(
        register=lambda name: lambda fn: registered.__setitem__(name, fn)), lambda: None)
    assert registered["get_admission_snapshot"]() is None


# -- review follow-ups: snapshot age, live queue, holds, publication --------------


def test_the_snapshot_age_backdates_the_observation(host):
    host.admission = _admission(published_at=9_980.0)  # 20 s before sampling
    payload = _sample()
    assert payload["snapshot_age_seconds"] == 20.0

    evidence = broker_capacity.evidence_from_payload("alpha", payload, observed_at=1_000.0)

    # A job accepted 10 s before the call is newer than these figures: its
    # charge must not settle on them.
    assert evidence.snapshot.observed_at == 980.0 < 990.0


def test_a_live_pending_job_overrides_an_older_empty_snapshot(host):
    host.specs = [_spec("aaaaaaaaaaaa", JobState.PENDING)]
    host.admission = _admission(local_pending=0)
    assert _sample()["queue_ready"] is False


@pytest.mark.parametrize("age", [-1, float("nan"), "10", True])
def test_a_malformed_snapshot_age_is_no_evidence(age):
    evidence = broker_capacity.evidence_from_payload(
        "alpha", _payload(snapshot_age_seconds=age), observed_at=100.0
    )
    assert evidence.error and "snapshot_age_seconds" in evidence.error


def test_admin_update_scope_decides_the_local_hold(tmp_path):
    d = _daemon(tmp_path)
    try:
        d._admin_update_marker_present = True
        for scope, held in ((None, True), (frozenset({"@local"}), True),
                            (frozenset({"cluster"}), False)):
            d._admin_update_hold_scope = scope
            d._admission_spec_facts = (0, False, False)
            d._publish_admission_snapshot()
            holds = d._admission_snapshot["holds"]
            assert ("an admin update holds local dispatch" in holds) is held, scope
    finally:
        d._queue_lock_fd.close()


def test_drains_hold_local_dispatch_unless_they_hold_scheduler_targets_only(
    tmp_path, monkeypatch
):
    from vq import drain

    d = _daemon(tmp_path)
    try:
        for state, held in (
            (drain.DrainState(), True),
            (drain.DrainState(max_jobs=1), True),
            (drain.DrainState(scheduler_hosts=["cluster"]), False),
        ):
            monkeypatch.setattr(drain, "read_effective_drain_state",
                                lambda *, via_rpc, multi_user, s=state: s)
            d._admission_spec_facts = (0, False, False)
            d._publish_admission_snapshot()
            assert ("the host is drained" in d._admission_snapshot["holds"]) is held, state
    finally:
        d._queue_lock_fd.close()


def test_a_changed_config_and_a_running_build_are_holds(tmp_path, monkeypatch):
    from vq import daemon as daemon_module

    d = _daemon(tmp_path)
    try:
        d._admission_spec_facts = (0, False, True)
        monkeypatch.setattr(daemon_module, "_config_fingerprint", lambda: "changed")
        d._publish_admission_snapshot()
        holds = d._admission_snapshot["holds"]
        assert "a build job is running" in holds
        assert "the config changed and is not yet applied" in holds
    finally:
        d._queue_lock_fd.close()


def test_the_loop_publishes_after_dispatch_even_when_it_raises(tmp_path, monkeypatch):
    d = _daemon(tmp_path)
    try:
        d._admission_spec_facts = (0, False, False)  # stale facts from a tick ago

        def failing_dispatch():
            d._admission_spec_facts = None  # what the real one does first
            raise RuntimeError("dispatch failed")

        monkeypatch.setattr(d, "_dispatch_pending", failing_dispatch)
        for name in ("_reconcile_running", "_reconcile_scheduler_jobs", "_host_pressure_pass",
                     "_watchdog_pass", "_maybe_auto_cleanup", "_maybe_check_version_drift"):
            if hasattr(d, name):
                monkeypatch.setattr(d, name, lambda: None)
        with pytest.raises(RuntimeError, match="dispatch failed"):
            d.iterate()
        assert "this tick did not evaluate the queue" in d._admission_snapshot["holds"]
    finally:
        d._queue_lock_fd.close()


def test_a_fresh_dispatch_pass_never_reuses_last_ticks_facts(tmp_path):
    d = _daemon(tmp_path)
    try:
        d._admission_spec_facts = (0, False, False)
        d.watchdog._host_pressure_active = True  # _dispatch_pending returns early
        d._dispatch_pending()
        assert d._admission_spec_facts is None
    finally:
        d._queue_lock_fd.close()
