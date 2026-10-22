"""#165: a node's declared runtime_digest binds its job to that generation.

A node declaring ``runtime_digest`` names ``runtime_root``, the
``vq runtime-bundle deploy`` target on its daemon host. The controller submits
with ``--runtime-root/--runtime-artifact`` so the queue binds exactly that
generation (#161), and confirms from the job's own record that it ran it
before anything the job produced is published. The decisive case runs end to
end: a real controller, ``vq submit`` and daemon, with the deployment's
current selection moved to another generation.
"""
from __future__ import annotations

import contextlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import campaign as cp
from vq import campaign_controller as cc

sys.path.insert(0, str(Path(__file__).parent))
from test_campaign_controller_165 import (  # noqa: E402
    FakeQueue,
    _execute,
    _journal,
    _run,
    queue_env,  # noqa: F401 -- pytest fixture
)
from test_runtime_binding_161 import _deploy, generations  # noqa: E402, F401

A = "a" * 64
B = "b" * 64


def _node(**fields: Any) -> dict[str, Any]:
    return {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
            "outputs": [{"name": "out", "path": "out", "kind": "directory"}], **fields}


def _campaign(*nodes: dict[str, Any]) -> cp.Campaign:
    return cp.Campaign.model_validate(
        {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": list(nodes)})


class TestModel:
    def test_a_root_needs_a_digest(self) -> None:
        with pytest.raises(ValueError, match="runtime_root needs runtime_digest"):
            _campaign(_node(runtime_root="/opt/runtime"))
        assert _campaign(_node(runtime_digest=A, runtime_root="/opt/runtime"))

    def test_a_campaign_without_a_root_digests_as_before(self) -> None:
        campaign = _campaign(
            _node(runtime_digest=A),
            {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
             "inputs": [{"name": "out", "source": "calc.out", "path": "in"}]})
        # Computed with the previous code, which had no runtime_root.
        assert cp.campaign_digest(campaign) == (
            "5c2374b79c9e0106cea15e30e865ce4ebb320b063fb4ca37e95afd244d8422a9")
        bound = _campaign(_node(runtime_digest=A, runtime_root="/opt/runtime"))
        assert cp.campaign_digest(bound) != cp.campaign_digest(_campaign(_node(runtime_digest=A)))


class TestPreflight:
    def test_a_digest_without_a_root_cannot_run(self) -> None:
        (problem,) = cc.preflight(_campaign(_node(runtime_digest=A)))
        assert "declares runtime_digest without runtime_root" in problem

    def test_a_daemon_host_can(self) -> None:
        campaign = _campaign(_node(host="ws_a", runtime_digest=A, runtime_root="/opt/rt"))
        assert cc.preflight(campaign, hosts=frozenset({"ws_a"}),
                            scheduler_hosts=frozenset({"hpc_a"})) == []


def test_the_submission_asks_the_queue_to_bind_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[list[str]] = []

    def run(self: cc.CliQueue, argv: list[str], timeout: float) -> Any:
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(
            {"jobid": "job000000001", "host": "localhost"}), stderr="")

    monkeypatch.setattr(cc.CliQueue, "_run", run)
    node = _campaign(_node(runtime_digest=A, runtime_root="/opt/rt")).nodes[0]
    # The receipt's shape is not what is under test, only the argv sent.
    with contextlib.suppress(Exception):
        cc.CliQueue().submit(node, tmp_path, "k" * 64, "h2o.calc.a1")
    argv = seen[0]
    assert argv[argv.index("--runtime-root") + 1] == "/opt/rt"
    assert argv[argv.index("--runtime-artifact") + 1] == A
    assert argv.index("--runtime-artifact") < argv.index("--")
    plain = _campaign(_node()).nodes[0]
    seen.clear()
    with contextlib.suppress(Exception):
        cc.CliQueue().submit(plain, tmp_path, "k" * 64, "h2o.calc.a1")
    assert "--runtime-root" not in seen[0]


class TestExecutedRuntime:
    def _payload(self, **fields: Any) -> dict[str, Any]:
        return {"state": "running", "program": None, "program_runtime_pin": None,
                "runtime_bundle": {"root": "/opt/rt", "artifact_id": A},
                "started_at": "2026-10-18T10:00:00+00:00", **fields}

    def test_a_started_bound_job_records_its_generation(self) -> None:
        runtime = cc.executed_runtime(self._payload())
        assert runtime is not None
        assert (runtime.program, runtime.artifact_identity, runtime.runtime_root) == (
            "runtime-bundle", A, f"/opt/rt/generations/{A}/payload")
        named = cc.executed_runtime(self._payload(program="vibe-qc"))
        assert named is not None and named.program == "vibe-qc"

    @pytest.mark.parametrize("fields", [
        {"started_at": None},  # bound at submit, not started: nothing ran yet
        {"runtime_bundle": {"root": "/opt/rt"}},
        {"runtime_bundle": None},
    ])
    def test_otherwise_nothing_is_recorded(self, fields: dict[str, Any]) -> None:
        assert cc.executed_runtime(self._payload(**fields)) is None


class BoundQueue(FakeQueue):
    """Reports the generation each started job ran, as the queue's record does."""

    def __init__(self, root: Path, ran: str | None) -> None:
        super().__init__(root)
        self.ran = ran

    def observe(self, job: cp.JobRef) -> cc.Observation:
        state = self.states[job.job_id]
        payload: dict[str, Any] = {
            "state": state, "cwd": str(self.payloads[job.job_id]),
            "exit_code": 0 if state == "completed" else None, "program": None,
            "started_at": None if state == "pending" else "2026-10-18T10:00:00+00:00",
            "runtime_bundle": ({"root": "/opt/rt", "artifact_id": self.ran}
                               if self.ran is not None else None),
        }
        return cc.observation_from_status(payload)


def _controller(tmp_path: Path, ran: str | None) -> tuple[cc.Controller, BoundQueue]:
    queue = BoundQueue(tmp_path / "jobs", ran)
    campaign = _campaign(_node(runtime_digest=A, runtime_root="/opt/rt"))
    return cc.Controller(campaign, tmp_path / "run", queue), queue


def test_the_declared_generation_is_published_with_its_outputs(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, ran=A)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"out/value": "41\n"})
    controller.step()
    assert controller.problems == {}
    manifest = cp.load_manifest(controller.artifact_path("calc", 1, "out")
                                .with_name("out.manifest.json"))
    assert manifest.runtime_digest == A
    assert manifest.executed_runtime is not None
    assert manifest.executed_runtime.artifact_identity == A
    assert controller.status()["status"] == "succeeded"


@pytest.mark.parametrize(("ran", "says"), [
    (B, f"ran runtime {B[:12]}, not the declared runtime {A[:12]}"),
    (None, f"ran no recorded runtime generation, not the declared runtime {A[:12]}"),
])
def test_a_job_that_ran_anything_else_is_never_published(
    tmp_path: Path, ran: str | None, says: str,
) -> None:
    controller, queue = _controller(tmp_path, ran=ran)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"out/value": "41\n"})
    controller.step()
    assert says in controller.problems["calc"]
    assert "its outputs are not published" in controller.problems["calc"]
    assert not controller.artifact_path("calc", 1, "out").exists()
    assert controller.status()["status"] != "succeeded"


def test_end_to_end_the_node_runs_its_declared_generation(
    tmp_path: Path, queue_env, generations: tuple[Path, str, str, Path],  # noqa: F811
) -> None:
    root, a, b, trust = generations
    _deploy(tmp_path, root, b, trust)  # the current selection is B; the node declares A
    campaign = {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": [{
        "id": "calc", "kind": "compute", "cpus": 1, "runtime_digest": a,
        "runtime_root": str(root),
        "command": ["sh", "-c", 'mkdir -p out && cat "$VQ_RUNTIME_PAYLOAD/data/value" > out/value'],
        "outputs": [{"name": "out", "path": "out", "kind": "directory"}]}]}
    campaign_file = tmp_path / "campaign.json"
    campaign_file.write_text(json.dumps(campaign))
    run_dir = tmp_path / "run"
    first = _run(str(campaign_file), str(run_dir), "--once")
    assert first.exit_code == 0, first.output
    _execute(queue_env)
    final = _run(str(campaign_file), str(run_dir), "--json", "--interval", "1")
    assert final.exit_code == 0, final.output
    (attempt,) = _journal(run_dir).attempts
    assert attempt.runtime is not None and attempt.runtime.artifact_identity == a
    artifact = run_dir / "artifacts" / "calc" / "1" / "out"
    assert (artifact / "value").read_text() == "41\n"
    manifest = json.loads((artifact.with_name("out.manifest.json")).read_text())
    assert manifest["runtime_digest"] == a

