"""#160: a placement fleet file built from live `vq broker capacity` answers.

`vq placement fleet BASE --capacity NAME=ANSWER.json` overlays each daemon
target's live answer on the operator's base record: when it was observed,
whether it is reachable, what is free now (only with open admission and a
ready queue), its GPUs per node, and programs it reports unavailable.
Answers come from the real target-side sampler; the base fleet is the
simulated one of test_placement_policy_160.py.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from vq import placement_policy as pp
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_broker_capacity_64 import _admission, _sample, host  # noqa: E402,F401
from test_placement_policy_160 import NOW, _fleet, _rejected, _request  # noqa: E402


def _answer(**changes: Any) -> dict[str, Any]:
    """A real sampler answer, sampled 30 s before NOW with a 5 s old snapshot."""
    answer = _sample()
    answer.update({"sampled_at": NOW.timestamp() - 30, **changes})
    return answer


def _target(fleet: dict[str, Any], name: str) -> dict[str, Any]:
    return next(t for t in fleet["targets"] if t["name"] == name)


def _plan(fleet: dict[str, Any], **request: object) -> dict[str, Any]:
    return pp.explain(pp.Fleet.model_validate(fleet),
                      pp.Request.model_validate(_request(**request)),
                      now=NOW, max_age_seconds=300)


class TestOverlay:
    def test_a_live_answer_replaces_the_daemons_observation(self, host: Any) -> None:  # noqa: F811
        host.admission = _admission(gpu_devices=["0", "1"], used_gpus=1)
        base = _fleet()
        fleet = pp.fleet_with_capacity(base, {"stale": _answer()})
        stale = _target(fleet, "stale")
        assert stale["observed_at"] == "2026-10-12T11:59:25+00:00"  # less the snapshot age
        assert stale["reachable"] is True
        assert stale["free"] == {"cpus": 10, "mem_mb": 20000, "gpus": 1}
        assert stale["node"] == {"cpus": 64, "mem_mb": 256000, "gpus": 2}
        assert stale["programs"] == base["targets"][5]["programs"]
        # Every other target, and the base document, are as they were.
        assert [t for t in fleet["targets"] if t["name"] != "stale"] == [
            t for t in base["targets"] if t["name"] != "stale"]
        assert base == _fleet()
        # The once-stale daemon is now planned on, and fits a small job now.
        plan = _plan(fleet, cpus=8, mem_mb_per_node=16000)
        assert "stale" not in _rejected(plan)
        live = next(c for c in plan["candidates"] if c["target"] == "stale")
        assert live["components"]["queue_wait"]["source"] == "fits the free capacity now"

    @pytest.mark.parametrize("closed", [
        {"holds": ["the host is drained"]}, {"local_pending": 1}])
    def test_without_open_admission_nothing_is_free_now(
        self, host: Any, closed: dict[str, Any],  # noqa: F811
    ) -> None:
        host.admission = _admission(**closed)
        stale = _target(pp.fleet_with_capacity(_fleet(), {"stale": _answer()}), "stale")
        assert "free" not in stale and stale["reachable"] is True

    def test_a_daemon_that_did_not_answer_is_unreachable(self, host: Any) -> None:  # noqa: F811
        host.ping = {"ok": False}
        fleet = pp.fleet_with_capacity(_fleet(), {"idle-slow": _answer()})
        assert _target(fleet, "idle-slow")["reachable"] is False
        assert "unreachable when observed" in _rejected(_plan(fleet))["idle-slow"]

    def test_an_unavailable_program_is_removed(self, host: Any) -> None:  # noqa: F811
        fleet = pp.fleet_with_capacity(_fleet(), {"idle-slow": _answer(
            programs={"vibe-qc": "BROKEN: venv missing", "other": "OK"})})
        assert _target(fleet, "idle-slow")["programs"] == {}
        assert "program vibe-qc is not available" in _rejected(_plan(fleet))["idle-slow"]

    def test_unknown_gpus_keep_the_base_record(self, host: Any) -> None:  # noqa: F811
        base = _fleet()
        _target(base, "idle-slow")["node"]["gpus"] = 4
        fleet = pp.fleet_with_capacity(base, {"idle-slow": _answer()})
        assert _target(fleet, "idle-slow")["node"]["gpus"] == 4
        assert _target(fleet, "idle-slow")["free"]["gpus"] is None


class TestRefusals:
    @pytest.mark.parametrize(("name", "answer", "message"), [
        ("nowhere", {}, "no such target"),
        ("fast-busy", {}, "a scheduler lane has no capacity answer"),
        ("idle-slow", {"schema": "something/1"}, "not a vq.broker-capacity/1 answer"),
        ("idle-slow", [], "not a vq.broker-capacity/1 answer"),
    ])
    def test_a_wrong_answer_refuses_the_build(
        self, name: str, answer: object, message: str,
    ) -> None:
        with pytest.raises(pp.PlacementError, match=message):
            pp.fleet_with_capacity(_fleet(), {name: answer})

    @pytest.mark.parametrize(("changes", "message"), [
        ({"daemon_ok": "yes"}, "daemon_ok must be a boolean"),
        ({"sampled_at": None}, "sampled_at must be a finite time"),
        ({"sampled_at": True}, "sampled_at must be a finite time"),
        ({"free_gpus": -1}, "free_gpus must be a non-negative integer"),
        ({"gpus": 1.5}, "gpus must be a non-negative integer"),
    ])
    def test_a_malformed_answer_refuses_the_build(
        self, host: Any, changes: dict[str, Any], message: str,  # noqa: F811
    ) -> None:
        with pytest.raises(pp.PlacementError, match=message):
            pp.fleet_with_capacity(_fleet(), {"idle-slow": _answer(**changes)})

    def test_an_invalid_base_is_refused(self) -> None:
        with pytest.raises(ValueError):
            pp.fleet_with_capacity({"schema_name": "vq.placement-fleet/1", "targets": []}, {})


def test_the_cli_builds_a_fleet_that_explain_uses(tmp_path: Path, host: Any) -> None:  # noqa: F811
    host.admission = _admission(gpu_devices=["0"], used_gpus=0)
    base, answer = tmp_path / "base.json", tmp_path / "stale.json"
    base.write_text(json.dumps(_fleet()))
    answer.write_text(json.dumps(_answer()))
    out = tmp_path / "fleet.json"
    runner = CliRunner()
    built = runner.invoke(main, ["placement", "fleet", str(base), "--capacity",
                                 f"stale={answer}", "-o", str(out)])
    assert built.exit_code == 0, built.output
    assert "1 with live capacity" in built.output
    assert _target(json.loads(out.read_text()), "stale")["free"]["gpus"] == 1
    request = tmp_path / "request.json"
    request.write_text(json.dumps(_request(cpus=8, mem_mb_per_node=16000, gpus_per_node=1)))
    explained = runner.invoke(main, ["placement", "explain", str(out), str(request),
                                     "--now", NOW.isoformat(), "--json"])
    assert explained.exit_code == 0, explained.output
    assert json.loads(explained.output)["selected"]["target"] == "stale"
    printed = runner.invoke(main, ["placement", "fleet", str(base)])
    assert json.loads(printed.output) == _fleet()  # no answers: the base itself

    for args, message in [
        (["--capacity", f"stale={answer}", "--capacity", f"stale={answer}"],
         "two capacity answers for stale"),
        (["--capacity", "stale"], "expected NAME=ANSWER.json"),
        (["--capacity", f"stale={answer}", "-o", str(base)], "must not replace BASE_FILE"),
    ]:
        refused = runner.invoke(main, ["placement", "fleet", str(base), *args])
        assert refused.exit_code != 0 and message in refused.output, refused.output
    assert json.loads(base.read_text()) == _fleet()


class TestCollect:
    """`--collect NAME` asks configured daemon hosts over SSH, as the broker does."""

    @pytest.fixture
    def ssh(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, host: Any,  # noqa: F811
            ) -> dict[str, Any]:
        from vq import config, transport

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            '[hosts.stale]\nssh = "stale"\n[hosts.idle-slow]\nssh = "idle-slow"\n'
            '[hosts.fast-busy]\nssh = "fast-busy"\n')
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        state: dict[str, Any] = {"calls": [], "fail": set(), "answer": _sample()}

        def run_remote_vq(host_cfg: Any, *argv: str, **kwargs: Any) -> Any:
            state["calls"].append((host_cfg.ssh, argv))
            assert kwargs["owned_process_group"] is True and kwargs["timeout"] == 60.0
            if host_cfg.ssh in state["fail"]:
                raise TimeoutError("ssh timed out")
            return SimpleNamespace(stdout=json.dumps(state["answer"]))

        monkeypatch.setattr(transport, "run_remote_vq", run_remote_vq)
        base = tmp_path / "base.json"
        base.write_text(json.dumps(_fleet()))
        state["base"] = base
        return state

    def _fleet_cli(self, ssh: dict[str, Any], *args: str) -> Any:
        return CliRunner().invoke(main, ["placement", "fleet", str(ssh["base"]), *args])

    def test_a_collected_answer_is_dated_by_this_hosts_clock(self, ssh: dict[str, Any]) -> None:
        before = time.time()
        result = self._fleet_cli(ssh, "--collect", "stale")
        assert result.exit_code == 0, result.output
        stale = _target(json.loads(result.output), "stale")
        observed = datetime.fromisoformat(stale["observed_at"]).timestamp()
        # This host's clock at the call, less the daemon's 5 s snapshot age;
        # never the answer's own sampled_at (10 000 s after the epoch).
        assert before - 5 - 1 <= observed <= time.time() - 5 + 1
        assert stale["reachable"] is True and stale["free"]["cpus"] == 10
        assert ssh["calls"] == [("stale", ("broker", "capacity", "--json",
                                           "--program", "vibe-qc"))]

    def test_a_daemon_that_does_not_answer_is_recorded_unreachable_now(
        self, ssh: dict[str, Any],
    ) -> None:
        ssh["fail"].add("idle-slow")
        before = time.time()
        result = self._fleet_cli(ssh, "--collect", "idle-slow", "--collect", "stale")
        assert result.exit_code == 0, result.output
        fleet = json.loads(result.stdout)
        idle = _target(fleet, "idle-slow")
        assert idle["reachable"] is False and "free" not in idle
        assert datetime.fromisoformat(idle["observed_at"]).timestamp() >= before - 1
        assert _target(fleet, "stale")["reachable"] is True
        assert "idle-slow: unreachable (idle-slow gave no capacity evidence" in result.stderr

    @pytest.mark.parametrize(("args", "message"), [
        (["--collect", "fast-busy"], "a scheduler lane has no capacity answer"),
        (["--collect", "down"], "not a configured [hosts.down]"),
        (["--collect", "nowhere"], "no such target"),
    ])
    def test_misdirected_collection_contacts_no_host(
        self, ssh: dict[str, Any], args: list[str], message: str,
    ) -> None:
        result = self._fleet_cli(ssh, *args)
        assert result.exit_code != 0 and message in result.output
        assert ssh["calls"] == []

    def test_one_target_cannot_have_both_an_answer_and_a_collection(
        self, ssh: dict[str, Any], tmp_path: Path,
    ) -> None:
        answer = tmp_path / "stale.json"
        answer.write_text(json.dumps(_answer()))
        result = self._fleet_cli(ssh, "--capacity", f"stale={answer}", "--collect", "stale")
        assert result.exit_code != 0
        assert "stale is given both --capacity and --collect" in result.output
        assert ssh["calls"] == []

    def test_an_answer_and_no_answer_for_one_target_is_refused(self) -> None:
        with pytest.raises(pp.PlacementError, match="both an answer and no answer"):
            pp.fleet_with_capacity(_fleet(), {"stale": {}}, unreachable={"stale": 1.0})
