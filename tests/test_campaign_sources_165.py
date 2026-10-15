"""#165: node sources, pinned in the campaign file and verified on every use.

A node's ``sources`` directory sits next to the campaign file and is pinned
by a content digest, so the campaign digest covers the exact scripts a node
runs. `vq campaign validate` and the controller check the pin; the
controller copies the sources into the payload, verifies the copy, keeps
them apart from inputs and outputs, and verifies them again before any
resend. Campaigns without sources keep the digest they had before.
"""
from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import campaign as cp
from vq import campaign_controller as cc
from vq import config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

#: A campaign without sources, and the digest the code before ``sources``
#: existed gave it. Journals and keys of such campaigns must stay valid.
UNSOURCED = {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": [
    {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
     "outputs": [{"name": "data", "path": "data.txt", "kind": "file"}]},
    {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
     "inputs": [{"name": "data", "source": "calc.data", "path": "in.txt"}]}]}
UNSOURCED_DIGEST = "695ca1df1c2ae3f640cae0218764862391e7441bffed0e94d06e1010f9b029e4"


def _tree(root: Path, files: dict[str, str]) -> Path:
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return root


def _pinned(base: Path, name: str, files: dict[str, str]) -> dict[str, str]:
    return {"path": name, "sha256": cp.source_digest(_tree(base / name, files))}


def _write(base: Path, nodes: list[dict[str, Any]]) -> Path:
    path = base / "campaign.json"
    path.write_text(json.dumps({"schema_name": "vq.campaign/1", "campaign_id": "h2o",
                                "nodes": nodes}))
    return path


class TestPin:
    def test_campaigns_without_sources_keep_their_digest(self) -> None:
        assert cp.campaign_digest(cp.Campaign.model_validate(UNSOURCED)) == UNSOURCED_DIGEST

    def test_the_pin_is_part_of_the_campaign_digest(self, tmp_path: Path) -> None:
        node = {**UNSOURCED["nodes"][0], "sources": {"path": "calc", "sha256": "a" * 64}}
        pinned = cp.Campaign.model_validate({**UNSOURCED, "nodes": [node]})
        repinned = cp.Campaign.model_validate(
            {**UNSOURCED, "nodes": [{**node, "sources": {"path": "calc", "sha256": "b" * 64}}]})
        assert cp.campaign_digest(pinned) != cp.campaign_digest(repinned)

    @pytest.mark.parametrize("sources", [
        {"path": "../calc", "sha256": "a" * 64},
        {"path": "/abs", "sha256": "a" * 64},
        {"path": "calc"},
        {"path": "calc", "sha256": "not-a-digest"},
    ])
    def test_unsafe_or_unpinned_sources_are_refused(self, sources: dict[str, str]) -> None:
        node = {**UNSOURCED["nodes"][0], "sources": sources}
        with pytest.raises(ValidationError):
            cp.Campaign.model_validate({**UNSOURCED, "nodes": [node]})

    def test_the_digest_covers_names_and_bytes_not_creation_order(self, tmp_path: Path) -> None:
        one = _tree(tmp_path / "one", {"run.sh": "echo 1\n", "lib/util.sh": "x=1\n"})
        two = _tree(tmp_path / "two", {"lib/util.sh": "x=1\n", "run.sh": "echo 1\n"})
        assert cp.source_digest(one) == cp.source_digest(two)
        (two / "run.sh").write_text("echo 2\n")
        assert cp.source_digest(one) != cp.source_digest(two)
        (two / "run.sh").write_text("echo 1\n")
        (two / "lib/util.sh").rename(two / "lib/utils.sh")
        assert cp.source_digest(one) != cp.source_digest(two)

    def test_links_in_sources_are_refused(self, tmp_path: Path) -> None:
        root = _tree(tmp_path / "calc", {"run.sh": "echo\n"})
        os.symlink("/etc/hosts", root / "hosts")
        with pytest.raises(cp.CampaignError, match="symbolic links are refused"):
            cp.source_digest(root)


class TestValidate:
    def test_matching_pins_validate(self, tmp_path: Path) -> None:
        node = {**UNSOURCED["nodes"][0],
                "sources": _pinned(tmp_path, "calc", {"run.sh": "echo\n"})}
        result = CliRunner().invoke(main, ["campaign", "validate", str(_write(tmp_path, [node]))])
        assert result.exit_code == 0, result.output
        assert "sources calc match their pin" in result.output

    def test_a_changed_script_fails_with_its_actual_digest(self, tmp_path: Path) -> None:
        node = {**UNSOURCED["nodes"][0],
                "sources": _pinned(tmp_path, "calc", {"run.sh": "echo\n"})}
        campaign_file = _write(tmp_path, [node])
        (tmp_path / "calc/run.sh").write_text("echo changed\n")
        actual = cp.source_digest(tmp_path / "calc")
        result = CliRunner().invoke(main, ["campaign", "validate", str(campaign_file), "--json"])
        assert result.exit_code == 1
        report = json.loads(result.output)["sources"]["calc"]
        assert report["actual"] == actual
        assert f"have sha256 {actual}, the campaign pins" in report["problem"]

    @pytest.mark.parametrize("setup", ["missing", "linked"])
    def test_missing_or_linked_sources_fail(self, tmp_path: Path, setup: str) -> None:
        if setup == "linked":
            _tree(tmp_path / "elsewhere", {"run.sh": "echo\n"})
            os.symlink(tmp_path / "elsewhere", tmp_path / "calc")
        node = {**UNSOURCED["nodes"][0], "sources": {"path": "calc", "sha256": "a" * 64}}
        result = CliRunner().invoke(main, ["campaign", "validate", str(_write(tmp_path, [node]))])
        assert result.exit_code == 1
        assert ("do not exist" if setup == "missing" else "is a symbolic link") in result.output


# --- the controller -------------------------------------------------------------


class FakeQueue:
    """Copies each payload into a workspace; the test decides how jobs end."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.jobs: dict[str, tuple[str, Path]] = {}  # job name -> (job id, workspace)
        self.states: dict[str, str] = {}
        self.refuse = False

    def submit(self, node: cp.Node, payload: Path, key: str, job_name: str) -> cp.JobRef:
        if self.refuse:
            raise cc.SubmitRefused("vq submit refused: synthetic")
        if job_name not in self.jobs:
            job_id = f"job{len(self.jobs):09d}"
            workspace = self.root / job_id
            workspace.mkdir(parents=True)
            for child in payload.iterdir():
                cc._copy(child, workspace / child.name)
            self.jobs[job_name] = (job_id, workspace)
            self.states[job_id] = "pending"
        return cp.JobRef(host="localhost", job_id=self.jobs[job_name][0])

    def observe(self, job: cp.JobRef) -> cc.Observation:
        workspace = next(w for j, w in self.jobs.values() if j == job.job_id)
        return cc.observation_from_status(
            {"state": self.states[job.job_id], "cwd": str(workspace), "exit_code": 0})

    def fetch(self, job: cp.JobRef, name: str, into: Path) -> Path:
        workspace = next(w for j, w in self.jobs.values() if j == job.job_id)
        cc._copy(workspace / name, into / name)
        return into / name

    def finish(self, name: str, files: dict[str, str]) -> None:
        job_id, workspace = self.jobs[name]
        _tree(workspace, files)
        self.states[job_id] = "completed"


def _controller(base: Path, nodes: list[dict[str, Any]]) -> tuple[cc.Controller, FakeQueue]:
    queue = FakeQueue(base / "jobs")
    campaign = cp.Campaign.model_validate(
        {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": nodes})
    return cc.Controller(campaign, base / "run", queue, sources_root=base), queue


def _produce(base: Path) -> dict[str, Any]:
    return {"id": "calc", "kind": "compute", "cpus": 1, "command": ["sh", "calc.sh"],
            "sources": _pinned(base, "calc", {"calc.sh": "echo 42 > data.txt\n"}),
            "outputs": [{"name": "data", "path": "data.txt", "kind": "file"}]}


def test_inputs_beside_and_inside_source_directories_verify_on_resend(tmp_path: Path) -> None:
    """The resend check leaves out exactly the inputs and the directories made
    for them: one input inside a source directory, one in a new directory."""
    check = {"id": "check", "kind": "validate", "cpus": 1, "command": ["sh", "check.sh"],
             "sources": _pinned(tmp_path, "check", {"check.sh": "true\n",
                                                    "ref/expected.txt": "42\n"}),
             "inputs": [{"name": "a", "source": "calc.data", "path": "ref/got.txt"},
                        {"name": "b", "source": "calc.data", "path": "new/deep/got.txt"}]}
    controller, queue = _controller(tmp_path, [_produce(tmp_path), check])
    controller.step()
    queue.finish("h2o.calc.a1", {"data.txt": "42\n"})
    queue.refuse = True  # the consumer's first send gets no job
    controller.step()
    assert controller.problems["check"] == "vq submit refused: synthetic"

    queue.refuse = False
    restarted = cc.Controller(controller.campaign, controller.run_dir, queue,
                              sources_root=tmp_path)
    restarted.step()
    assert restarted.problems == {}
    _, workspace = queue.jobs["h2o.check.a1"]
    assert sorted(str(p.relative_to(workspace)) for p in workspace.rglob("*") if p.is_file()) == [
        "check.sh", "new/deep/got.txt", "ref/expected.txt", "ref/got.txt"]


def test_a_tampered_staged_source_is_never_resent(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, [_produce(tmp_path)])
    queue.refuse = True
    controller.step()
    (controller.payload_dir("calc", 1) / "calc.sh").write_text("rm -rf ~\n")

    queue.refuse = False
    restarted = cc.Controller(controller.campaign, controller.run_dir, queue,
                              sources_root=tmp_path)
    restarted.step()
    assert "staged sources of calc no longer match their pin" in restarted.problems["calc"]
    assert queue.jobs == {}


def test_a_source_changed_while_copied_is_not_staged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, queue = _controller(tmp_path, [_produce(tmp_path)])
    real_copy = cc._copy

    def racing_copy(source: Path, dest: Path) -> None:
        real_copy(source, dest)
        if dest.name == "calc.sh":
            dest.write_text("echo 43 > data.txt\n")

    monkeypatch.setattr(cc, "_copy", racing_copy)
    controller.step()
    assert "the copied sources of calc do not match their pin" in controller.problems["calc"]
    assert queue.jobs == {} and controller.journal.attempts == []


@pytest.mark.parametrize(("change", "message"), [
    ({"outputs": [{"name": "data", "path": "calc.sh", "kind": "file"}]},
     "output data at calc.sh is already a source file"),
    ({"outputs": [], "inputs": [{"name": "x", "source": "seed.data", "path": "calc.sh"}]},
     "input x at calc.sh collides with a source file"),
    ({"outputs": [], "inputs": [{"name": "x", "source": "seed.data", "path": "calc.sh/x"}]},
     "input x at calc.sh/x is under a source file"),
])
def test_sources_never_mix_with_inputs_or_outputs(
    tmp_path: Path, change: dict[str, Any], message: str,
) -> None:
    seed = {"id": "seed", "kind": "compute", "cpus": 1, "command": ["true"],
            "outputs": [{"name": "data", "path": "data.txt", "kind": "file"}]}
    controller, queue = _controller(tmp_path, [seed, {**_produce(tmp_path), **change}])
    controller.step()
    queue.finish("h2o.seed.a1", {"data.txt": "seed\n"})
    controller.step()
    assert message in controller.problems["calc"]
    assert "h2o.calc.a1" not in queue.jobs
    assert not controller.payload_dir("calc", 1).exists()


def test_a_changed_script_stops_the_run_before_anything_is_submitted(tmp_path: Path) -> None:
    campaign_file = _write(tmp_path, [_produce(tmp_path)])
    (tmp_path / "calc/calc.sh").write_text("echo 43 > data.txt\n")
    result = CliRunner().invoke(main, ["campaign", "run", str(campaign_file),
                                       str(tmp_path / "run")])
    assert result.exit_code == 1
    assert "node calc: sources calc have sha256" in result.output
    assert not (tmp_path / "run" / cc.JOURNAL_NAME).exists()


# --- end to end: real submit, real daemon ------------------------------------------


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    queue = Daemon(max_cpus=4, poll_interval=0.05)
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    yield queue
    for running in queue._running.values():
        running.popen.kill()
        running.popen.wait(timeout=10)
        running.close_logs()


def _execute(daemon: Daemon) -> None:
    daemon._dispatch_pending()
    deadline = time.monotonic() + 30
    while daemon._running:
        assert time.monotonic() < deadline, "jobs did not finish"
        daemon._reconcile_running()
        time.sleep(0.05)


def test_a_campaign_runs_its_own_scripts_end_to_end(tmp_path: Path, daemon: Daemon) -> None:
    base = tmp_path / "campaign"
    calc = {"id": "calculate", "kind": "compute", "cpus": 1, "command": ["sh", "calc.sh"],
            "sources": _pinned(base, "calculate", {
                "calc.sh": "mkdir -p out && cp input/h2o.xyz out/ && echo -76.02 > out/energy\n",
                "input/h2o.xyz": "3\nwater\nO 0 0 0\nH 0 0 1\nH 0 1 0\n"}),
            "outputs": [{"name": "result", "path": "out", "kind": "directory"}]}
    check = {"id": "validate", "kind": "validate", "cpus": 1, "command": ["sh", "check.sh"],
             "sources": _pinned(base, "validate", {
                 "check.sh": 'test "$(cat got/energy)" = "$(cat expected)" '
                             '&& echo PASS > verdict\n',
                 "expected": "-76.02\n"}),
             "inputs": [{"name": "result", "source": "calculate.result", "path": "got"}],
             "outputs": [{"name": "verdict", "path": "verdict", "kind": "file"}]}
    campaign_file = _write(base, [calc, check])
    run_dir = tmp_path / "run"

    for _ in range(2):
        result = CliRunner().invoke(main, ["campaign", "run", str(campaign_file), str(run_dir),
                                           "--once"])
        assert result.exit_code == 0, result.output
        _execute(daemon)
    final = CliRunner().invoke(main, ["campaign", "run", str(campaign_file), str(run_dir),
                                      "--interval", "1"])
    assert final.exit_code == 0, final.output

    specs = sorted((JobSpec.read(p) for p in daemon.queue_dir.glob("*.json")),
                   key=lambda s: s.job_name or "")
    assert [(s.job_name, s.state) for s in specs] == [
        ("h2o.calculate.a1", JobState.COMPLETED), ("h2o.validate.a1", JobState.COMPLETED)]
    assert (run_dir / "artifacts/validate/1/verdict").read_text() == "PASS\n"
    manifest = cp.load_manifest(run_dir / "artifacts/calculate/1/result.manifest.json")
    assert [e.path for e in manifest.entries] == ["energy", "h2o.xyz"]
