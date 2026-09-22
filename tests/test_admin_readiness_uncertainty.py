"""Readiness evidence must retain uncertainty at every boundary (#77–#79)."""
from __future__ import annotations

import itertools
import json
import subprocess
from types import SimpleNamespace

import pytest

from vq import admin, rpc

SHA = "a" * 40
TREE = "1" * 64


@pytest.fixture
def clock(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(admin, "time", SimpleNamespace(
        monotonic=lambda: now[0],
        sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
    ))
    monkeypatch.delenv("VQ_DAEMON_HEALTH_TIMEOUT", raising=False)
    return now


class Record:
    def __init__(self, live, clock):
        self.name = "record.json"
        self.live = live
        self.clock = clock

    def read_bytes(self):
        self.clock[0] += 1.0
        return json.dumps({
            "scheduler_target": "cluster-a",
            "state": "running" if self.live else "completed",
        }).encode()


def inventory(monkeypatch, entries):
    monkeypatch.setattr(admin.paths, "queue_dir", lambda: SimpleNamespace(
        stat=lambda: None, iterdir=lambda: iter(entries),
        glob=lambda _pattern: iter(entries),  # Preserve parent behavioral controls.

    ))


def test_77_all_prefix_orders_bound_the_same_population(monkeypatch, clock):
    """All 210 placements of four live records: no representative-prefix claim."""
    monkeypatch.setattr(admin, "SCHEDULER_STARTUP_SCAN_BUDGET_SECONDS", 2.0)
    for positions in itertools.combinations(range(10), 4):
        clock[0] = 0
        records = [Record(i in positions, clock) for i in range(10)]
        scan = admin._count_live_scheduler_jobs(10, entries=records)
        assert scan.seen == 2 and not scan.complete
        assert scan.live == len(set(positions).intersection({0, 1}))
        assert scan.allowance_jobs == scan.live + 8
        assert 4 <= scan.allowance_jobs <= 10


@pytest.mark.parametrize("live_first", [False, True])
def test_77_public_window_does_not_treat_a_prefix_as_a_sample(monkeypatch, clock, live_first):
    live = [True] * 300
    dead = [False] * 700
    inventory(monkeypatch, [Record(x, clock) for x in (live + dead if live_first else dead + live)])
    monkeypatch.setattr(admin, "SCHEDULER_STARTUP_SCAN_BUDGET_SECONDS", 1.0)
    assert admin._daemon_health_timeout() == 1800.0


def test_77_complete_snapshot_is_measured_even_if_earlier_count_was_larger(monkeypatch, clock):
    scan = admin._count_live_scheduler_jobs(
        100, entries=[Record(True, clock), Record(False, clock)],
    )
    assert scan.complete and scan.live == scan.allowance_jobs == 1


def test_77_broken_walk_preserves_observations_and_unread_bound(monkeypatch, clock):
    def records():
        yield Record(True, clock)
        raise OSError("directory read interrupted")
    scan = admin._count_live_scheduler_jobs(20, entries=records())
    assert not scan.complete and scan.live == scan.seen == 1
    assert scan.allowance_jobs == 20


@pytest.mark.parametrize("error", [OSError, RecursionError, FileNotFoundError])
def test_77_missing_inventory_uses_cap_without_claiming_a_count(monkeypatch, clock, caplog, error):
    def broken():
        yield Record(False, clock)
        raise error("no reliable census")
    monkeypatch.setattr(admin.paths, "queue_dir", lambda: SimpleNamespace(
        stat=lambda: None, iterdir=broken,
    ))
    assert admin._daemon_health_timeout() == 1800.0
    assert "inventory unavailable" in caplog.text
    assert "live in" not in caplog.text


def test_77_census_and_scan_use_one_snapshot(monkeypatch, clock):
    calls = []
    def walk():
        calls.append(True)
        assert len(calls) == 1, "a second walk cannot bind the first census"
        return iter([Record(True, clock), Record(False, clock)])
    monkeypatch.setattr(admin.paths, "queue_dir", lambda: SimpleNamespace(
        stat=lambda: None, iterdir=walk,
    ))
    assert admin._daemon_health_timeout() == pytest.approx(62.55)


def test_78_observation_rejects_wrong_identity_on_first_answer(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 1800.0)
    answers = []
    def ping():
        answers.append(True)
        return {"source_sha": "b" * 40, "source_tree_sha256": TREE}
    monkeypatch.setattr(rpc, "ping_user_daemon", ping)
    lifecycle = SimpleNamespace(pre_source_sha=SHA, pre_source_tree_sha256=TREE,
                                manager=admin._DaemonServiceManager.SYSTEMD)
    ok, detail = admin._verify_preupdate_daemon_identity(lifecycle)
    assert not ok and "strict identity mismatch" in detail
    assert len(answers) == 1 and clock[0] == 0


@pytest.mark.parametrize("envelope", [
    {}, {"multi_user": True, "source_sha": SHA, "source_tree_sha256": TREE},
    {"source_sha": SHA, "source_tree_sha256": "2" * 64},
])
def test_78_observation_never_accepts_missing_tree_or_wrong_socket(monkeypatch, clock, envelope):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 100.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: envelope)
    result = admin._verify_restarted_daemon(SHA, expected_tree_sha256=TREE,
        require_exact_identity=True, wait_for_identity_change=False)
    assert not result.verified and clock[0] == 0


def test_78_restart_allows_outgoing_identity_then_exact_new_answer(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: {
        "source_sha": SHA if clock[0] >= 1 else "b" * 40,
        "source_tree_sha256": TREE,
    })
    result = admin._verify_restarted_daemon(
        SHA, expected_tree_sha256=TREE, require_exact_identity=True,
    )
    assert result.verified and 1 <= clock[0] < 2


def test_78_observation_can_wait_for_first_answer_without_accepting_a_mismatch(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None if clock[0] < 1 else {
        "source_sha": SHA, "source_tree_sha256": TREE,
    })
    result = admin._verify_restarted_daemon(SHA, expected_tree_sha256=TREE,
        require_exact_identity=True, wait_for_identity_change=False)
    assert result.verified and 1 <= clock[0] < 2


def systemd_reply(state="active", pid="123", rc=0):
    return subprocess.CompletedProcess([], rc,
        f"ExecStart=\nActiveState={state}\nMainPID={pid}\n", "")


@pytest.mark.parametrize("reply,expected", [
    (OSError("unavailable"), None),
    (subprocess.TimeoutExpired("systemctl", 30), None),
    (systemd_reply(rc=1), None),
    (subprocess.CompletedProcess([], 0, "garbled", ""), None),
    (systemd_reply("inactive", "0"), False),
    (systemd_reply("failed", "0"), False),
    (systemd_reply(), True),
    (systemd_reply("activating", "0"), None),
    (systemd_reply("active", "0"), None),
])
def test_79_real_systemd_probe_preserves_unknown(monkeypatch, reply, expected):
    def run(*args, **kwargs):
        assert args[0][:4] == ["systemctl", "--user", "show", "vq-daemon"]
        assert 0 < kwargs["timeout"] <= 30
        if isinstance(reply, Exception):
            raise reply
        return reply
    monkeypatch.setattr(admin.subprocess, "run", run)
    assert admin._service_daemon_alive_probe(admin._DaemonServiceManager.SYSTEMD)() is expected


@pytest.mark.parametrize("rc,raw,expected", [
    (127, "failed to invoke launchctl: file not found", None),
    (124, "timed out", None), (1, "permission denied", None),
    (113, "Could not find service", False), (0, "malformed", None),
    (0, "state = running\npid = 42", True),
    (0, "state = not running", False),
    (0, "state = running", None),
    (0, "state = not running\nstate = running", None),
])
def test_79_launchd_unknown_is_not_absence(monkeypatch, rc, raw, expected):
    monkeypatch.setattr(admin, "_query_launchd_daemon", lambda: (rc, raw))
    assert admin._service_daemon_alive_probe(admin._DaemonServiceManager.LAUNCHD)() is expected


def test_79_real_probe_failure_extends_but_does_not_report_alive(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
    def unavailable(*args, **kwargs):
        raise OSError("manager unavailable")
    monkeypatch.setattr(admin.subprocess, "run", unavailable)
    result = admin._verify_restarted_daemon(SHA,
        daemon_alive=admin._service_daemon_alive_probe(admin._DaemonServiceManager.SYSTEMD))
    assert not result.verified and clock[0] == pytest.approx(30.0, abs=.1)
    assert "liveness was unavailable" in result.detail
    assert "reported a running daemon" not in result.detail
    assert "reports no running daemon" not in result.detail


def test_79_unknown_then_confirmed_dead_stops_at_next_probe(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
    evidence = iter([None, False])
    result = admin._verify_restarted_daemon(SHA, daemon_alive=lambda: next(evidence))
    assert not result.verified and clock[0] == pytest.approx(12.0, abs=.1)
    assert "reports no running daemon" in result.detail


def test_79_prior_answer_then_silence_never_earns_unknown_extension(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: {} if clock[0] == 0 else None)
    def forbidden_probe():
        pytest.fail("an answered daemon may not earn a liveness extension")
    result = admin._verify_restarted_daemon(SHA, daemon_alive=forbidden_probe)
    assert not result.verified and clock[0] == pytest.approx(10.0, abs=.1)


def test_77_79_truncated_dead_prefix_and_unknown_manager_can_reach_ready(monkeypatch, clock):
    inventory(monkeypatch, [Record(False, clock)] * 10 + [Record(True, clock)] * 3)
    monkeypatch.setattr(admin, "SCHEDULER_STARTUP_SCAN_BUDGET_SECONDS", 1.0)
    window = admin._daemon_health_timeout()
    clock[0] = 0
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: window)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: (
        None if clock[0] < window + 1 else {"source_sha": SHA}
    ))
    result = admin._verify_restarted_daemon(SHA, daemon_alive=lambda: None)
    assert result.verified and window < clock[0] < 3 * window


def test_79_unknown_probes_obey_absolute_ceiling(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 1800.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
    result = admin._verify_restarted_daemon(SHA, daemon_alive=lambda: None)
    assert not result.verified and clock[0] == pytest.approx(3600, abs=.1)
    assert "liveness was unavailable" in result.detail


def test_79_unknown_then_alive_keeps_the_evidence_gap_visible(monkeypatch, clock):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
    samples = [None]
    result = admin._verify_restarted_daemon(
        SHA, daemon_alive=lambda: samples.pop() if samples else True,
    )
    assert not result.verified and clock[0] == pytest.approx(30, abs=.1)
    assert "liveness was unavailable" in result.detail
    assert "reported a running daemon" not in result.detail


def test_77_unreadable_content_retains_one_unknown_job(monkeypatch, clock):
    def denied():
        raise PermissionError("temporarily unreadable")
    entries = [Record(False, clock), SimpleNamespace(name="denied.json", read_bytes=denied)]
    scan = admin._count_live_scheduler_jobs(2, entries=entries)
    assert scan.complete and scan.seen == 2 and scan.live == 0
    assert scan.unreadable == scan.allowance_jobs == 1
    inventory(monkeypatch, entries)
    assert admin._daemon_health_timeout() == pytest.approx(62.55)


def test_79_slow_unknown_query_does_not_trigger_more_queries_after_hard_deadline(
    monkeypatch, clock,
):
    monkeypatch.setattr(admin, "_daemon_health_timeout", lambda: 10.0)
    monkeypatch.setattr(rpc, "ping_user_daemon", lambda: None)
    probes = []

    def slow_query():
        probes.append(True)
        clock[0] += 30
        return None

    result = admin._verify_restarted_daemon(SHA, daemon_alive=slow_query)
    assert not result.verified and probes == [True]
    assert clock[0] == pytest.approx(40, abs=.1)
    assert "within 30s" in result.detail
