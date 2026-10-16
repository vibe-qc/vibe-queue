"""#165: the fleet console's campaign graphs.

`vq campaign run` registers its run directory in the controller user's vq
state; `/fleet/campaigns` reads each registered journal through the same
`campaign_status` as `vq campaign status` and draws it: columns by
dependency depth, nodes by status, success and completion edges, the latest
attempt's job link, each output's publication, and blockers. Read-only, and
an unreadable campaign shows its reason instead of breaking the page.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from vq import auth, campaign_registry, config, paths
from vq import campaign as cp
from vq import campaign_controller as cc
from vq.cli import main
from vq.web import authn, create_app
from vq.web import campaigns as view

sys.path.insert(0, str(Path(__file__).parent))
from test_campaign_controller_165 import FakeQueue  # noqa: E402

CALC = {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
        "outputs": [{"name": "data", "path": "data", "kind": "directory"}]}
CHECK = {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
         "inputs": [{"name": "data", "source": "calc.data", "path": "in"}],
         "outputs": [{"name": "verdict", "path": "verdict.txt", "kind": "file"}]}
RENDER = {"id": "render", "kind": "render", "cpus": 1, "command": ["true"],
          "inputs": [{"name": "data", "source": "calc.data", "path": "in"}],
          "after": [{"node": "check", "on": "success"}],
          "outputs": [{"name": "plot", "path": "plot.svg", "kind": "file"}]}
CLEANUP = {"id": "cleanup", "kind": "other", "cpus": 1, "command": ["true"],
           "after": [{"node": "check", "on": "completion"}]}
CAMPAIGN = {"schema_name": "vq.campaign/1", "campaign_id": "h2o",
            "nodes": [CALC, CHECK, RENDER, CLEANUP]}


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _run(tmp_path: Path, data: dict[str, Any] = CAMPAIGN) -> tuple[cc.Controller, Path]:
    """calc published, check failed, render blocked, cleanup started."""
    campaign_file = tmp_path / "h2o.json"
    campaign_file.write_text(json.dumps(data))
    queue = FakeQueue(tmp_path / "jobs")
    controller = cc.Controller(cp.load_campaign(campaign_file), tmp_path / "run-h2o", queue)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n"})
    controller.step()
    queue.finish("h2o.check.a1", "failed")
    controller.step()
    campaign_registry.register(controller.campaign, campaign_file, controller.run_dir)
    return controller, campaign_file


def _only_view() -> dict[str, Any]:
    context = view.campaigns_context()
    assert context["registry_problems"] == []
    (campaign,) = context["campaigns"]  # type: ignore[misc]
    return campaign


def test_the_graph_shows_each_stage_its_edges_blockers_and_job_links(state: Path) -> None:
    _run(state)
    campaign = _only_view()
    assert (campaign["campaign_id"], campaign["status"], campaign["error"]) == (
        "h2o", "failed", None)
    assert campaign["run_dir_name"] == "run-h2o"
    graph = campaign["graph"]
    nodes = {n["id"]: n for n in graph["nodes"]}
    assert {i: n["status"] for i, n in nodes.items()} == {
        "calc": "succeeded", "check": "failed", "render": "blocked", "cleanup": "running"}
    # Columns follow dependency depth: render and cleanup both follow check.
    assert [nodes[i]["x"] for i in ("calc", "check", "render", "cleanup")] == [6, 252, 498, 498]
    assert nodes["render"]["y"] < nodes["cleanup"]["y"]
    assert sorted((e["from"], e["to"], e["on"]) for e in graph["edges"]) == [
        ("calc", "check", "success"), ("calc", "render", "success"),
        ("check", "cleanup", "completion"), ("check", "render", "success")]
    # calc -> render skips check's column: it runs in the lane above the
    # boxes instead of straight through check.
    (skip,) = [e for e in graph["edges"] if (e["from"], e["to"]) == ("calc", "render")]
    assert skip["path"] == "M 196 43 H 224 V 7 H 470 V 43 H 498"
    assert min(n["y"] for n in graph["nodes"]) > 7
    assert nodes["calc"]["outputs"] == [{"name": "data", "status": "verified"}]
    assert nodes["calc"]["attempt"]["href"] == "/fleet/jobs/job000000000?host=localhost"
    assert nodes["check"]["outputs"] == [{"name": "verdict", "status": "not published"}]
    assert "a retry is an explicit attempt 2" in nodes["check"]["next"]
    assert nodes["render"]["reasons"] == ["needs check to succeed; it failed"]
    assert nodes["render"]["attempt"] is None
    assert nodes["cleanup"]["next"] == "wait for attempt 1"
    # A failed check may still be retried, so calc's output is kept for it too.
    assert campaign["retain"] == [{"node": "calc", "attempt": 1,
                                   "needed_by": ["check", "render"]}]


@pytest.mark.parametrize(("action", "phrase"), [
    ({"action": "submit", "attempt": 1, "stage": [{"input": "data"}, {"input": "geom"}]},
     "submit attempt 1 (stages data, geom)"),
    ({"action": "submit", "attempt": 1, "stage": []}, "submit attempt 1"),
    ({"action": "publish", "outputs": ["data", "plot"]}, "publish data, plot"),
    ({"action": "reconcile", "attempt": 2}, "resend attempt 2 under its own key"),
    ({"action": "none"}, ""),
])
def test_next_steps_read_as_phrases(action: dict[str, Any], phrase: str) -> None:
    assert view.next_step(action) == phrase


def test_a_campaign_that_cannot_be_read_shows_why(state: Path) -> None:
    controller, campaign_file = _run(state)
    (controller.run_dir / "journal.json").write_text("{not json")
    campaign = _only_view()
    assert campaign["graph"] is None and "not UTF-8 JSON" in campaign["error"]
    # The registration's absolute paths stay on the server.
    assert str(state) not in campaign["error"] and "run-h2o/journal.json" in campaign["error"]

    campaign_file.write_text(json.dumps({**CAMPAIGN, "nodes": [CALC]}))
    assert "campaign file has changed" in _only_view()["error"]
    campaign_file.write_text(json.dumps(CAMPAIGN))
    (controller.run_dir / "journal.json").unlink()
    controller.run_dir.rename(state / "elsewhere")
    assert _only_view()["error"] == "the run directory is gone"


def test_a_registered_run_without_a_journal_is_ready(state: Path) -> None:
    campaign_file = state / "h2o.json"
    campaign_file.write_text(json.dumps(CAMPAIGN))
    (state / "fresh").mkdir()
    campaign_registry.register(cp.load_campaign(campaign_file), campaign_file, state / "fresh")
    nodes = {n["id"]: n["status"] for n in _only_view()["graph"]["nodes"]}
    assert nodes == {"calc": "ready", "check": "waiting", "render": "waiting",
                     "cleanup": "waiting"}


def test_registrations_are_one_per_run_directory_and_report_bad_entries(state: Path) -> None:
    controller, campaign_file = _run(state)
    campaign_registry.register(controller.campaign, campaign_file, controller.run_dir)
    (campaign_registry.registry_dir() / "broken.json").write_text('{"schema_name": 1}')
    found, problems = campaign_registry.registrations()
    assert [r.run_dir for r in found] == [str(controller.run_dir.resolve())]
    assert len(problems) == 1 and problems[0].startswith("registration broken.json:")


def test_the_page_board_and_api(state: Path) -> None:
    _run(state)
    client = TestClient(create_app())
    page = client.get("/fleet/campaigns")
    assert page.status_code == 200
    assert '<svg class="campaign-graph"' in page.text
    assert 'class="cg-edge cg-edge-completion"' in page.text
    assert 'href="/fleet/jobs/job000000000?host=localhost"' in page.text
    assert "needs check to succeed; it failed" in page.text
    assert '<a href="/fleet/campaigns">campaigns</a>' in page.text
    board = client.get("/fleet/campaigns/_board")
    assert board.status_code == 200 and board.text.startswith('<div id="campaign-board"')
    api = client.get("/api/v1/fleet/campaigns").json()
    assert api["campaigns"][0]["status"] == "failed"
    assert str(state) not in json.dumps(api)


def test_the_pages_sit_behind_the_console_login(state: Path) -> None:
    _run(state)
    authn.add_user("viewer", "pw", "viewer")
    client = TestClient(create_app())
    assert client.get("/fleet/campaigns", follow_redirects=False).status_code == 302
    assert client.get("/fleet/campaigns/_board").status_code == 401
    assert client.get("/api/v1/fleet/campaigns").status_code == 401


def test_campaign_run_registers_its_run_directory(
    state: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    campaign_file = state / "h2o.json"
    campaign_file.write_text(json.dumps({**CAMPAIGN, "nodes": [CALC]}))

    def no_run(self: cc.Controller, *, once: bool, interval: float) -> dict[str, Any]:
        return self.status()

    monkeypatch.setattr(cc.Controller, "run", no_run)
    result = CliRunner().invoke(main, ["campaign", "run", str(campaign_file),
                                       str(state / "run"), "--once"])
    assert result.exit_code == 0, result.output
    (registration,) = campaign_registry.registrations()[0]
    assert (registration.run_dir, registration.campaign_file) == (
        str((state / "run").resolve()), str(campaign_file.resolve()))
    assert registration.campaign_sha256 == cp.campaign_digest(cp.load_campaign(campaign_file))


def test_registrations_are_listed_newest_first(state: Path) -> None:
    campaign_file = state / "h2o.json"
    campaign_file.write_text(json.dumps(CAMPAIGN))
    campaign = cp.load_campaign(campaign_file)
    for name in ("older", "newer"):
        (state / name).mkdir()
        path = campaign_registry.register(campaign, campaign_file, state / name)
        stamp = "2026-10-01T00:00:00+00:00" if name == "older" else "2026-10-02T00:00:00+00:00"
        path.write_text(path.read_text().replace(
            json.loads(path.read_text())["registered_at"], stamp))
    assert [Path(r.run_dir).name for r in campaign_registry.registrations()[0]] == [
        "newer", "older"]


def test_an_output_that_did_not_verify_shows_its_verdict() -> None:
    node = cp.Campaign.model_validate(CAMPAIGN).node("calc")
    entry = {"status": "succeeded", "reasons": [], "next": {"action": "publish",
                                                           "outputs": ["data"]},
             "attempts": [{"attempt": 1, "state": "succeeded", "retry_of": None,
                           "job": {"host": "hpc-a", "job_id": "4242"},
                           "artifacts": {"data": {"manifest_sha256": "a" * 64,
                                                  "status": "digest_mismatch"}}}]}
    box = view._box(node, entry, 0, 0)
    assert box["outputs"] == [{"name": "data", "status": "digest_mismatch"}]
    assert (box["next"], box["attempt"]["href"]) == ("publish data", "/fleet/jobs/4242?host=hpc-a")


def test_a_registration_with_a_relative_path_is_refused(state: Path) -> None:
    directory = campaign_registry.registry_dir()
    directory.mkdir(parents=True)
    (directory / "relative.json").write_text(json.dumps({
        "schema_name": "vq.campaign-registration/1", "campaign_id": "h2o",
        "campaign_sha256": "a" * 64, "campaign_file": "h2o.json", "run_dir": "/abs/run",
        "registered_at": "2026-10-15T00:00:00+00:00"}))
    found, problems = campaign_registry.registrations()
    assert found == [] and problems == [
        "registration relative.json: Value error, must be an absolute path"]
