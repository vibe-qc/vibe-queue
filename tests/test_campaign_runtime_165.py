"""#165: each attempt records the runtime its job actually ran with.

Until a declared ``runtime_digest`` can be bound to a submission, the
controller archives what the queue itself resolved at dispatch: the job's
program, version, authenticated git SHA, immutable artifact and slot. The
journal keeps it per attempt, every published output's manifest carries
it, and `vq campaign status` and the console show it. Nothing is guessed:
a job without a registered program, or not dispatched yet, records
nothing. Without a recorded runtime, journals and manifests are written
and hashed exactly as before.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

from vq import campaign as cp
from vq import campaign_controller as cc
from vq.spec import JobSpec, JobState, ProgramRuntimePin
from vq.web import campaigns as view

sys.path.insert(0, str(Path(__file__).parent))
from test_campaign_controller_165 import FakeQueue  # noqa: E402

SHA = "a1b2c3d4e5f6" + "0" * 28
CALC = {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
        "program": "vibeqc-release",
        "outputs": [{"name": "data", "path": "data", "kind": "directory"}]}
CHECK = {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
         "inputs": [{"name": "data", "source": "calc.data", "path": "in"}]}
CAMPAIGN = {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": [CALC, CHECK]}
RAN = {"program": "vibeqc-release", "version": "0.26.0", "git_sha": SHA,
       "artifact_identity": None, "runtime_root": "/opt/vq-runtimes/vibeqc-release/gen-7"}


def _payload(**pin: Any) -> dict[str, Any]:
    """`vq status --json` as the queue writes it: a JobSpec dump."""
    spec = JobSpec(id="abc", command=["python", "run.py"], cwd="/ws", cpus=1,
                   state=JobState.RUNNING, program="vibeqc-release",
                   program_runtime_pin=ProgramRuntimePin(**pin))
    return json.loads(spec.model_dump_json())


class TestExecutedRuntime:
    def test_a_dispatched_job_reports_what_its_queue_resolved(self) -> None:
        payload = _payload(expected_git_sha=SHA, resolved_git_sha=SHA, program_version="0.26.0",
                           resolved_runtime_root=RAN["runtime_root"])
        runtime = cc.executed_runtime(payload)
        assert runtime is not None and runtime.model_dump() == RAN
        assert cc.observation_from_status(payload).runtime == runtime

    def test_a_scheduler_launcher_without_a_git_sha_still_counts(self) -> None:
        runtime = cc.executed_runtime(_payload(resolved_runtime_root="/scratch/rt/gen-3",
                                               artifact_identity="/scratch/rt/bin/run-0.26.0"))
        assert runtime is not None and runtime.git_sha is None
        assert runtime.artifact_identity == "/scratch/rt/bin/run-0.26.0"
        assert cp.runtime_text(runtime.model_dump()) == (
            "vibeqc-release (immutable runtime, no git SHA recorded)")

    @pytest.mark.parametrize("payload", [
        # Submit-time intent only: not dispatched yet.
        _payload(expected_git_sha=SHA, program_version="0.26.0"),
        # No registered program.
        {"state": "running", "program": None, "program_runtime_pin": None},
        {"state": "running", "program": None,
         "program_runtime_pin": {"resolved_git_sha": SHA}},
        # A value the record would not accept is dropped, not guessed at.
        _payload(resolved_git_sha="not-a-sha"),
    ])
    def test_nothing_is_recorded_without_a_dispatch_time_resolution(
        self, payload: dict[str, Any],
    ) -> None:
        assert cc.executed_runtime(payload) is None


class RecordingQueue(FakeQueue):
    """Reports the program pin a dispatched job carries, as the queue does."""

    def observe(self, job: cp.JobRef) -> cc.Observation:
        seen = super().observe(job)
        if self.states[job.job_id] == "pending":
            return seen
        return cc.observation_from_status({
            **_payload(resolved_git_sha=SHA, program_version="0.26.0",
                       resolved_runtime_root=RAN["runtime_root"]),
            "state": self.states[job.job_id], "cwd": str(self.payloads[job.job_id]),
            "exit_code": 0 if self.states[job.job_id] == "completed" else None,
        })


def _controller(tmp_path: Path) -> tuple[cc.Controller, RecordingQueue]:
    queue = RecordingQueue(tmp_path / "jobs")
    return cc.Controller(cp.Campaign.model_validate(CAMPAIGN), tmp_path / "run", queue), queue


def test_the_journal_manifest_and_status_keep_what_ran(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path)
    logged: list[str] = []
    controller.log = logged.append
    controller.step()
    queue.states["job000000000"] = "running"
    assert controller.step() is True
    assert controller.journal.attempts[0].runtime is not None
    assert controller.journal.attempts[0].runtime.model_dump() == RAN
    assert any(line == f"calc: attempt 1 runs vibeqc-release 0.26.0 @ {SHA[:12]}"
               for line in logged)
    # Seen again unchanged, it is not progress.
    assert controller.step() is False

    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n"})
    controller.step()
    manifest = cp.load_manifest(controller.artifact_path("calc", 1, "data")
                                .with_name("data.manifest.json"))
    assert manifest.executed_runtime is not None
    assert manifest.executed_runtime.model_dump() == RAN
    on_disk = json.loads((controller.run_dir / "journal.json").read_text())
    assert on_disk["attempts"][0]["runtime"] == RAN

    report = controller.status()
    calc = next(n for n in report["nodes"] if n["node"] == "calc")
    assert calc["attempts"][0]["runtime"] == RAN
    assert (f"Ran with (as each job's queue recorded it):\n"
            f"  calc attempt 1: vibeqc-release 0.26.0 @ {SHA[:12]}") in cp.status_text(report)


def test_without_a_runtime_files_and_digests_are_as_before(tmp_path: Path) -> None:
    queue = FakeQueue(tmp_path / "jobs")
    controller = cc.Controller(cp.Campaign.model_validate(CAMPAIGN), tmp_path / "run", queue)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data/energy": "-76.0\n"})
    controller.step()
    on_disk = json.loads((controller.run_dir / "journal.json").read_text())
    assert all("runtime" not in attempt for attempt in on_disk["attempts"])
    raw = json.loads(controller.artifact_path("calc", 1, "data")
                     .with_name("data.manifest.json").read_text())
    assert "executed_runtime" not in raw
    # The digest the journal recorded is the hash of exactly those bytes'
    # content, as it was before the field existed.
    assert cp._canonical_digest(raw) == cp.manifest_digest(cp.ArtifactManifest.model_validate(raw))
    assert controller.journal.attempts[0].artifacts["data"].manifest_sha256 == (
        cp._canonical_digest(raw))
    assert "Ran with" not in cp.status_text(controller.status())


def test_the_console_shows_what_each_attempt_ran(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path)
    controller.step()
    queue.states["job000000000"] = "running"
    controller.step()
    graph = view.graph(controller.campaign, controller.status())
    calc = next(n for n in graph["nodes"] if n["id"] == "calc")
    assert calc["attempt"]["runtime"] == f"vibeqc-release 0.26.0 @ {SHA[:12]}"
    check = next(n for n in graph["nodes"] if n["id"] == "check")
    assert check["attempt"] is None


def test_the_rendered_board_says_what_ran(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from vq import auth, campaign_registry, config, paths
    from vq.web import create_app

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    campaign_file = tmp_path / "h2o.json"
    campaign_file.write_text(json.dumps(CAMPAIGN))
    queue = RecordingQueue(tmp_path / "jobs")
    controller = cc.Controller(cp.load_campaign(campaign_file), tmp_path / "run", queue)
    controller.step()
    queue.states["job000000000"] = "running"
    controller.step()
    campaign_registry.register(controller.campaign, campaign_file, controller.run_dir)
    html = TestClient(create_app()).get("/fleet/campaigns/_board").text
    assert f"ran vibeqc-release 0.26.0 @ {SHA[:12]}" in html
