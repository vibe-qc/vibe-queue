"""#165: campaign nodes on other hosts, with the controller relaying artifacts.

A node with ``host`` is submitted with `vq submit HOST`, observed with
`vq status HOST`, and each declared output comes back with
`vq fetch HOST JOB --name <top-level>` into the controller's run directory,
where it is hashed, stored and verified before any consumer is staged from
it. The CLI contract is pinned against a stand-in `vq` executable; the
relay is exercised with an in-memory queue that keeps one workspace root
per host.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import campaign as cp
from vq import campaign_controller as cc

STAND_IN = r'''
import json, os, sys
from pathlib import Path
argv = sys.argv[1:]
with open(os.environ["VQ_STAND_IN_LOG"], "a") as log:
    log.write(json.dumps(argv) + "\n")
mode = os.environ.get("VQ_STAND_IN_MODE", "ok")
if argv[0] == "submit":
    if mode == "unknown":
        print(json.dumps({"outcome": "unknown", "jobids": []}))
        sys.exit(1)
    print(json.dumps({"jobids": ["abc123def456"], "host": argv[1]}))
elif argv[0] == "status":
    print(json.dumps({"state": "completed", "exit_code": 0, "cwd": "/remote/ws"}))
elif argv[0] == "fetch":
    name, into = argv[argv.index("--name") + 1], Path(argv[argv.index("-o") + 1])
    if mode == "missing":
        print("Error: no such file: " + name, file=sys.stderr)
        sys.exit(2)
    if mode == "down":
        print("Error: ssh: connect timed out", file=sys.stderr)
        sys.exit(1)
    (into / name).mkdir(parents=True)
    (into / name / "energy").write_text("-76.02\n")
    landed = into / name if mode != "elsewhere" else into / "other"
    print(json.dumps({"destination": str(landed), "kind": "artifact"}))
'''


@pytest.fixture
def stand_in(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[cc.CliQueue, Path]:
    script = tmp_path / "vq_stand_in.py"
    script.write_text(STAND_IN)
    log = tmp_path / "argv.jsonl"
    monkeypatch.setenv("VQ_STAND_IN_LOG", str(log))
    return cc.CliQueue(vq=(sys.executable, str(script))), log


def _calls(log: Path) -> list[list[str]]:
    return [json.loads(line) for line in log.read_text().splitlines()]


REMOTE_CALC = {"id": "calc", "kind": "compute", "host": "cluster-a", "cpus": 16, "mem_mb": 64000,
               "command": ["vibe-qc", "run.py"],
               "outputs": [{"name": "result", "path": "out", "kind": "directory"}]}


class TestCliContract:
    def test_submit_status_and_fetch_name_the_nodes_host(
        self, tmp_path: Path, stand_in: tuple[cc.CliQueue, Path],
    ) -> None:
        queue, log = stand_in
        node = cp.Node.model_validate(REMOTE_CALC)
        payload = tmp_path / "payload"
        payload.mkdir()
        job = queue.submit(node, payload, "k" * 64, "h2o.calc.a1")
        assert (job.host, job.job_id) == ("cluster-a", "abc123def456")
        assert queue.observe(job).state == "succeeded"
        into = tmp_path / "incoming"
        into.mkdir()
        assert queue.fetch(job, "out", into) == into / "out"
        assert (into / "out/energy").read_text() == "-76.02\n"
        assert _calls(log) == [
            ["submit", "cluster-a", "--dir", str(payload), "--cpus", "16", "--job-name",
             "h2o.calc.a1", "--idempotency-key", "k" * 64, "--json", "--mem-mb", "64000",
             "--", "vibe-qc", "run.py"],
            ["status", "cluster-a", "abc123def456", "--json"],
            ["fetch", "cluster-a", "abc123def456", "--name", "out", "-o", str(into), "--json"],
        ]

    def test_a_node_without_host_runs_here(
        self, tmp_path: Path, stand_in: tuple[cc.CliQueue, Path],
    ) -> None:
        queue, log = stand_in
        node = cp.Node.model_validate({**REMOTE_CALC, "host": None})
        job = queue.submit(node, tmp_path, "k" * 64, "h2o.calc.a1")
        assert job.host == "localhost" and _calls(log)[0][:2] == ["submit", "localhost"]

    @pytest.mark.parametrize(("mode", "error"), [
        ("missing", cc.FetchMissing), ("down", cc.FetchError), ("elsewhere", cc.FetchError),
    ])
    def test_fetch_failures_are_told_apart(
        self, tmp_path: Path, stand_in: tuple[cc.CliQueue, Path],
        monkeypatch: pytest.MonkeyPatch, mode: str, error: type[Exception],
    ) -> None:
        queue, _ = stand_in
        monkeypatch.setenv("VQ_STAND_IN_MODE", mode)
        with pytest.raises(error):
            queue.fetch(cp.JobRef(host="cluster-a", job_id="abc123def456"), "out", tmp_path)
        # A missing entry is permanent; a failed copy is asked again next pass.
        assert issubclass(error, cc.Transient) == (mode != "missing")

    def test_an_unknown_remote_outcome_is_resent_not_failed(
        self, tmp_path: Path, stand_in: tuple[cc.CliQueue, Path],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        queue, _ = stand_in
        monkeypatch.setenv("VQ_STAND_IN_MODE", "unknown")
        with pytest.raises(cc.SubmitUncertain, match="outcome unknown"):
            queue.submit(cp.Node.model_validate(REMOTE_CALC), tmp_path, "k" * 64, "x.a1")


class TwoHosts:
    """An in-memory queue with one workspace root per host."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.jobs: dict[str, cp.JobRef] = {}
        self.states: dict[str, str] = {}
        self.fetch_down = False
        self.fetches: list[tuple[str, str]] = []

    def workspace(self, job: cp.JobRef) -> Path:
        return self.root / job.host / job.job_id

    def submit(self, node: cp.Node, payload: Path, key: str, job_name: str) -> cp.JobRef:
        if job_name not in self.jobs:
            job = cp.JobRef(host=node.host or "localhost", job_id=f"job{len(self.jobs):09d}")
            self.workspace(job).mkdir(parents=True)
            for child in payload.iterdir():
                cc._copy(child, self.workspace(job) / child.name)
            self.jobs[job_name], self.states[job.job_id] = job, "pending"
        return self.jobs[job_name]

    def observe(self, job: cp.JobRef) -> cc.Observation:
        return cc.observation_from_status({"state": self.states[job.job_id], "exit_code": 0})

    def fetch(self, job: cp.JobRef, name: str, into: Path) -> Path:
        if self.fetch_down:
            raise cc.FetchError("ssh: connect timed out")
        source = self.workspace(job) / name
        if not source.exists():
            raise cc.FetchMissing(f"no {name}")
        self.fetches.append((job.host, name))
        cc._copy(source, into / name)
        return into / name

    def finish(self, name: str, files: dict[str, str]) -> None:
        job = self.jobs[name]
        for relative, text in files.items():
            path = self.workspace(job) / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.states[job.job_id] = "completed"


CHECK = {"id": "check", "kind": "validate", "cpus": 1, "command": ["sh", "check.sh"],
         "inputs": [{"name": "result", "source": "calc.result", "path": "got"}],
         "outputs": [{"name": "verdict", "path": "verdict.txt", "kind": "file"}]}


def _controller(tmp_path: Path, *nodes: dict[str, Any]) -> tuple[cc.Controller, TwoHosts]:
    queue = TwoHosts(tmp_path / "hosts")
    campaign = cp.Campaign.model_validate(
        {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": list(nodes)})
    return cc.Controller(campaign, tmp_path / "run", queue), queue


def test_a_remote_producer_feeds_a_local_consumer_through_the_controller(
    tmp_path: Path,
) -> None:
    controller, queue = _controller(tmp_path, REMOTE_CALC, CHECK)
    controller.step()
    queue.finish("h2o.calc.a1", {"out/energy": "-76.02\n", "out/raw/scf.log": "converged\n",
                                 "scratch.tmp": "not an output\n"})
    controller.step()
    calc = next(a for a in controller.journal.attempts if a.node == "calc")
    check = next(a for a in controller.journal.attempts if a.node == "check")
    assert calc.job is not None and calc.job.host == "cluster-a"
    assert check.job is not None and check.job.host == "localhost"
    # Only the declared output came back, once, and it was verified here.
    assert queue.fetches == [("cluster-a", "out")]
    assert calc.artifacts["result"].status == "verified"
    assert (controller.artifact_path("calc", 1, "result") / "raw/scf.log").exists()
    assert not (controller.run_dir / "incoming").joinpath("calc", "1").exists()
    # The local consumer's workspace holds the producer's bytes.
    assert (queue.workspace(check.job) / "got/energy").read_text() == "-76.02\n"
    assert check.inputs["result"].manifest_sha256 == calc.artifacts["result"].manifest_sha256

    queue.finish("h2o.check.a1", {"verdict.txt": "PASS\n"})
    controller.step()
    assert controller.status()["status"] == "succeeded"


def test_a_failed_copy_is_retried_and_a_missing_output_is_a_problem(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, REMOTE_CALC, CHECK)
    controller.step()
    queue.finish("h2o.calc.a1", {"out/energy": "-76.02\n"})
    queue.fetch_down = True
    controller.step()
    assert controller.transient == {"calc": "ssh: connect timed out"}
    assert "h2o.check.a1" not in queue.jobs

    queue.fetch_down = False
    controller.step()
    assert controller.transient == {} and "h2o.check.a1" in queue.jobs

    other, other_queue = _controller(tmp_path / "second", REMOTE_CALC, CHECK)
    other.step()
    other_queue.finish("h2o.calc.a1", {"elsewhere/energy": "-76.02\n"})
    other.run(interval=1, sleep=lambda _s: None)
    assert "output result is missing" in other.problems["calc"]
    assert "h2o.check.a1" not in other_queue.jobs


def test_hosts_must_be_this_machine_or_configured() -> None:
    campaign = cp.Campaign.model_validate(
        {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": [REMOTE_CALC]})
    assert cc.preflight(campaign, hosts=frozenset({"cluster-a"})) == []
    (problem,) = cc.preflight(campaign, hosts=frozenset({"cluster-b"}))
    assert "host cluster-a is neither this machine nor a configured host" in problem
