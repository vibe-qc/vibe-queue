"""#165: cleanup under referenced artifacts.

`vq campaign cleanup` lists, and with `--apply` removes, what a run
directory no longer needs: by default only the controller's own copies
(accepted attempts' payloads, leftovers of an interrupted stage or publish),
and with `--intermediate` also outputs whose every consumer succeeded. An
output still referenced, an output nothing consumes, and the payload of an
attempt that may yet be resent are never touched.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import campaign as cp
from vq import campaign_controller as cc
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_campaign_controller_165 import FakeQueue  # noqa: E402

CALC = {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
        "outputs": [{"name": "data", "path": "data", "kind": "directory"}]}
CHECK = {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
         "inputs": [{"name": "data", "source": "calc.data", "path": "in"}],
         "outputs": [{"name": "verdict", "path": "verdict.txt", "kind": "file"}]}
CAMPAIGN = {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": [CALC, CHECK]}


def _controller(tmp_path: Path, campaign: dict[str, Any] = CAMPAIGN,
                ) -> tuple[cc.Controller, FakeQueue]:
    queue = FakeQueue(tmp_path / "jobs")
    return cc.Controller(cp.Campaign.model_validate(campaign), tmp_path / "run", queue), queue


def _paths(plan: list[dict[str, Any]], run_dir: Path) -> list[str]:
    return [str(Path(item["path"]).relative_to(run_dir)) for item in plan]


def test_referenced_artifacts_are_kept_until_their_consumers_succeed(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n", "data/log": "x" * 1000})
    controller.step()  # calc published; check staged and accepted
    # check still runs: calc.data is on the retention list and stays.
    assert controller.status()["retain"] == [{"node": "calc", "attempt": 1,
                                             "needed_by": ["check"]}]
    plan = controller.cleanup_plan(intermediate=True)
    assert _paths(plan, controller.run_dir) == ["payloads/calc/1", "payloads/check/1"]

    queue.finish("h2o.check.a1", "completed", {"verdict.txt": "PASS\n"})
    controller.step()
    assert controller.status()["status"] == "succeeded"
    default = controller.cleanup_plan()
    assert _paths(default, controller.run_dir) == ["payloads/calc/1", "payloads/check/1"]
    everything = controller.cleanup_plan(intermediate=True)
    assert _paths(everything, controller.run_dir) == [
        "payloads/calc/1", "payloads/check/1", "artifacts/calc/1/data"]
    assert everything[-1]["bytes"] == 1006 and "consumed by check" in everything[-1]["reason"]

    assert cc.apply_cleanup(everything) == sum(item["bytes"] for item in everything)
    # The result nothing consumes, and every manifest, stay; the status is unchanged.
    assert (controller.artifact_path("check", 1, "verdict")).read_text() == "PASS\n"
    assert controller.artifact_path("calc", 1, "data").with_name("data.manifest.json").exists()
    assert not controller.artifact_path("calc", 1, "data").exists()
    assert controller.status()["status"] == "succeeded"
    assert controller.step() is False and controller.problems == {}
    assert controller.cleanup_plan(intermediate=True) == []


def test_a_payload_that_may_be_resent_is_kept_and_leftovers_go(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path)
    queue.refuse.add("calc")
    controller.step()
    (attempt,) = controller.journal.attempts
    assert attempt.state == "submitting"
    (controller.run_dir / "incoming" / "calc" / "1").mkdir(parents=True)
    (controller.run_dir / "payloads" / "check" / ".1.staging").mkdir(parents=True)
    plan = controller.cleanup_plan(intermediate=True)
    assert _paths(plan, controller.run_dir) == ["incoming/calc/1", "payloads/check/.1.staging"]
    assert controller.payload_dir("calc", 1).exists()


def test_the_cli_lists_by_default_and_removes_only_with_apply(tmp_path: Path) -> None:
    # calc ran on a host this machine's configuration no longer names: the
    # controller could not run the campaign here, but cleanup needs no host.
    hosted = {**CAMPAIGN, "nodes": [{**CALC, "host": "retired-cluster"}, CHECK]}
    controller, queue = _controller(tmp_path, hosted)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n"})
    controller.step()
    queue.finish("h2o.check.a1", "completed", {"verdict.txt": "PASS\n"})
    controller.step()
    campaign_file = tmp_path / "campaign.json"
    campaign_file.write_text(json.dumps(hosted))
    run_dir = str(controller.run_dir)
    refused = CliRunner().invoke(main, ["campaign", "run", str(campaign_file), run_dir, "--once"])
    assert refused.exit_code != 0 and "retired-cluster" in refused.output

    listed = CliRunner().invoke(main, ["campaign", "cleanup", str(campaign_file), run_dir,
                                       "--intermediate"])
    assert listed.exit_code == 0, listed.output
    assert "would remove 3 paths," in listed.output and "(rerun with --apply)" in listed.output
    assert controller.artifact_path("calc", 1, "data").exists()

    applied = CliRunner().invoke(main, ["campaign", "cleanup", str(campaign_file), run_dir,
                                        "--intermediate", "--apply", "--json"])
    assert applied.exit_code == 0, applied.output
    report = json.loads(applied.output)
    assert report["applied"] is True and report["freed"] == report["bytes"] > 0
    assert not controller.artifact_path("calc", 1, "data").exists()
    assert not controller.payload_dir("check", 1).exists()


def test_an_artifact_whose_publish_was_interrupted_is_kept(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, queue = _controller(tmp_path)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n"})
    real = cc._write_atomic

    def crash(path: Path, text: str) -> None:
        if path.name.endswith(".manifest.json"):
            raise OSError("interrupted")
        real(path, text)

    monkeypatch.setattr(cc, "_write_atomic", crash)
    controller.step()
    assert controller.problems == {"calc": "interrupted"}
    monkeypatch.setattr(cc, "_write_atomic", real)
    # The copy is in place but unrecorded. check has not run, so calc stays
    # on the retention list and its output is kept; the fetched copy goes,
    # since the publish fetches again.
    restarted = cc.Controller(controller.campaign, controller.run_dir, queue)
    assert restarted.artifact_path("calc", 1, "data").exists()
    plan = restarted.cleanup_plan(intermediate=True)
    assert _paths(plan, restarted.run_dir) == ["payloads/calc/1", "incoming/calc/1"]
    cc.apply_cleanup(plan)
    restarted.step()  # the publish is redone from the job
    assert restarted.status()["nodes"][0]["attempts"][0]["artifacts"]["data"]["status"] \
        == "verified"
