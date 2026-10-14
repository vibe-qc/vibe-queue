"""#165: the campaign controller runs a campaign to its end, restartably.

The decisive case runs end to end: `vq campaign run` submits through the
real `vq submit --idempotency-key`, a real daemon executes the jobs, and the
controller is stopped after the producer is accepted and started again. It
submits exactly one producer and one consumer job, and the consumer is
staged from the producer's verified artifact. The other cases drive the
controller against an in-memory queue.
"""
from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import campaign as cp
from vq import campaign_controller as cc
from vq import config, paths, submit
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState

CALCULATE = {
    "id": "calculate", "kind": "compute", "cpus": 1,
    "command": ["sh", "-c", "mkdir -p out/raw && echo -1.25 > out/energy.txt "
                            "&& echo converged > out/raw/scf.log"],
    "outputs": [{"name": "result", "path": "out", "kind": "directory"}],
}
VALIDATE = {
    "id": "validate", "kind": "validate", "cpus": 1,
    "command": ["sh", "-c", 'test "$(cat in/energy.txt)" = -1.25 && echo PASS > verdict.txt'],
    "inputs": [{"name": "result", "source": "calculate.result", "path": "in"}],
    "outputs": [{"name": "verdict", "path": "verdict.txt", "kind": "file"}],
}


def _campaign(*nodes: dict[str, Any], campaign_id: str = "h2o") -> dict[str, Any]:
    return {"schema_name": "vq.campaign/1", "campaign_id": campaign_id, "nodes": list(nodes)}


def _write(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "campaign.json"
    path.write_text(json.dumps(data))
    return path


# --- end to end: real submit, real daemon ------------------------------------------


@pytest.fixture
def queue_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    """A private queue: `vq` subprocesses inherit it, and a daemon runs its jobs."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    daemon = Daemon(max_cpus=4, poll_interval=0.05)
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    yield daemon
    for running in daemon._running.values():
        running.popen.kill()
        running.popen.wait(timeout=10)
        running.close_logs()


def _execute(daemon: Daemon) -> None:
    """Dispatch every pending job and wait for each to finish."""
    daemon._dispatch_pending()
    deadline = time.monotonic() + 30
    while daemon._running:
        assert time.monotonic() < deadline, "jobs did not finish"
        daemon._reconcile_running()
        time.sleep(0.05)


def _specs(daemon: Daemon) -> list[JobSpec]:
    return sorted((JobSpec.read(p) for p in daemon.queue_dir.glob("*.json")),
                  key=lambda s: s.job_name or "")


def _run(*args: str) -> Any:
    return CliRunner().invoke(main, ["campaign", "run", *args], catch_exceptions=False)


def _journal(run_dir: Path) -> cp.Journal:
    return cp.load_journal(run_dir / cc.JOURNAL_NAME)


def test_a_restarted_controller_submits_each_node_exactly_once(
    tmp_path: Path, queue_env: Daemon,
) -> None:
    campaign_file = _write(tmp_path, _campaign(CALCULATE, VALIDATE))
    run_dir = tmp_path / "run"

    # First controller: submits the producer, then stops (--once).
    first = _run(str(campaign_file), str(run_dir), "--once")
    assert first.exit_code == 0, first.output
    assert "calculate: attempt 1 accepted as job" in first.output
    (producer,) = _specs(queue_env)
    assert producer.job_name == "h2o.calculate.a1"

    # Restarted while the job is still queued: nothing new is submitted.
    again = _run(str(campaign_file), str(run_dir), "--once")
    assert again.exit_code == 0, again.output
    assert [s.id for s in _specs(queue_env)] == [producer.id]

    _execute(queue_env)
    # Restarted after the producer finished: it is published and verified,
    # and the consumer is staged from it and submitted.
    third = _run(str(campaign_file), str(run_dir), "--once")
    assert third.exit_code == 0, third.output
    specs = _specs(queue_env)
    assert [s.job_name for s in specs] == ["h2o.calculate.a1", "h2o.validate.a1"]

    _execute(queue_env)
    final = _run(str(campaign_file), str(run_dir), "--json", "--interval", "1")
    assert final.exit_code == 0, final.output
    report = json.loads(final.output)
    assert (report["status"]["status"], report["problems"]) == ("succeeded", {})

    # Exactly one job per node, each completed, under the attempt's own key.
    specs = _specs(queue_env)
    assert [(s.job_name, s.state) for s in specs] == [
        ("h2o.calculate.a1", JobState.COMPLETED), ("h2o.validate.a1", JobState.COMPLETED),
    ]
    journal = _journal(run_dir)
    by_node = {a.node: a for a in journal.attempts}
    assert {a.node: (a.attempt, a.state) for a in journal.attempts} == {
        "calculate": (1, "succeeded"), "validate": (1, "succeeded"),
    }
    assert by_node["calculate"].job is not None and by_node["calculate"].job.job_id == producer.id
    # The queue holds each job under the journal's key for its attempt.
    for attempt in journal.attempts:
        assert attempt.job is not None
        assert submit.fence_idempotency_key(paths.queue_dir(), attempt.idempotency_key) == {
            "state": "accepted", "job_id": attempt.job.job_id}
    # The consumer ran on the producer's verified artifact, byte for byte.
    result = by_node["calculate"].artifacts["result"]
    assert result.status == "verified"
    assert by_node["validate"].inputs["result"].model_dump() == {
        "producer_attempt": 1, "manifest_sha256": result.manifest_sha256,
    }
    manifest = cp.load_manifest(run_dir / "artifacts/calculate/1/result.manifest.json")
    assert cp.manifest_digest(manifest) == result.manifest_sha256
    assert (manifest.campaign_sha256, manifest.node, manifest.attempt) == (
        journal.campaign_sha256, "calculate", 1)
    assert [e.path for e in manifest.entries] == ["energy.txt", "raw", "raw/scf.log"]
    consumer_workspace = Path(specs[1].cwd)
    assert (consumer_workspace / "in/energy.txt").read_text() == "-1.25\n"
    assert (consumer_workspace / "verdict.txt").read_text() == "PASS\n"
    assert (run_dir / "artifacts/validate/1/verdict").read_text() == "PASS\n"


def test_a_lost_answer_is_resent_under_its_key_not_submitted_twice(
    tmp_path: Path, queue_env: Daemon,
) -> None:
    """The submission landed but the controller never heard back (a crash or
    a lost answer). The restarted controller resends under the same key and
    the queue returns the job it already accepted."""
    campaign = cp.Campaign.model_validate(_campaign(CALCULATE))
    run_dir = tmp_path / "run"

    class LostAnswer(cc.CliQueue):
        def submit(self, *args: Any) -> str:
            super().submit(*args)
            raise cc.SubmitUncertain("the answer was lost")

    with cc.locked(run_dir):
        lost = cc.Controller(campaign, run_dir, LostAnswer())
        lost.step()
    assert lost.transient == {"calculate": "the answer was lost"}
    (landed,) = _specs(queue_env)
    (attempt,) = _journal(run_dir).attempts
    assert (attempt.state, attempt.job) == ("submitting", None)

    with cc.locked(run_dir):
        restarted = cc.Controller(campaign, run_dir, cc.CliQueue())
        assert restarted.status()["nodes"][0]["next"]["action"] == "reconcile"
        restarted.step()
    assert [s.id for s in _specs(queue_env)] == [landed.id]
    (attempt,) = _journal(run_dir).attempts
    assert attempt.state == "accepted" and attempt.job is not None
    assert attempt.job.job_id == landed.id


# --- the controller's decisions, against an in-memory queue ------------------------


class FakeQueue:
    """Submits into memory; the test decides how each job ends."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.submitted: list[tuple[str, str, str]] = []  # (job_id, key, job_name)
        self.states: dict[str, str] = {}
        self.payloads: dict[str, Path] = {}
        self.refuse: set[str] = set()

    def submit(self, node: cp.Node, payload: Path, key: str, job_name: str) -> cp.JobRef:
        if node.id in self.refuse:
            raise cc.SubmitRefused("vq submit refused: synthetic")
        host = node.host or "localhost"
        for job_id, seen_key, _ in self.submitted:
            if seen_key == key:
                return cp.JobRef(host=host, job_id=job_id)
        job_id = f"job{len(self.submitted):09d}"
        self.submitted.append((job_id, key, job_name))
        self.states[job_id] = "pending"
        workspace = self.root / job_id
        workspace.mkdir(parents=True)
        for child in payload.iterdir():
            cc._copy(child, workspace / child.name)
        self.payloads[job_id] = workspace
        return cp.JobRef(host=host, job_id=job_id)

    def observe(self, job: cp.JobRef) -> cc.Observation:
        return cc.observation_from_status(
            {"state": self.states[job.job_id], "cwd": str(self.payloads[job.job_id]),
             "exit_code": 0 if self.states[job.job_id] == "completed" else 1})

    def fetch(self, job: cp.JobRef, name: str, into: Path) -> Path:
        source = self.payloads[job.job_id] / name
        if not source.exists():
            raise cc.FetchMissing(f"no {name} in job {job.job_id}")
        cc._copy(source, into / name)
        return into / name

    def finish(self, name: str, state: str, files: dict[str, str] | None = None) -> None:
        job_id = next(j for j, _, n in self.submitted if n == name)
        for relative, text in (files or {}).items():
            path = self.payloads[job_id] / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        self.states[job_id] = state

    def names(self) -> list[str]:
        return [name for _, _, name in self.submitted]


PRODUCE = {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"],
           "outputs": [{"name": "data", "path": "data.txt", "kind": "file"}]}
CONSUME = {"id": "check", "kind": "validate", "cpus": 1, "command": ["true"],
           "inputs": [{"name": "data", "source": "calc.data", "path": "in.txt"}]}
SIDE = {"id": "side", "kind": "other", "cpus": 1, "command": ["true"]}


def _controller(tmp_path: Path, *nodes: dict[str, Any]) -> tuple[cc.Controller, FakeQueue]:
    queue = FakeQueue(tmp_path / "jobs")
    campaign = cp.Campaign.model_validate(_campaign(*nodes))
    return cc.Controller(campaign, tmp_path / "run", queue), queue


def _statuses(controller: cc.Controller) -> dict[str, str]:
    return {n["node"]: n["status"] for n in controller.status()["nodes"]}


def test_a_failed_producer_leaves_its_child_unrun_while_others_progress(
    tmp_path: Path,
) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME, SIDE)
    controller.step()
    assert queue.names() == ["h2o.calc.a1", "h2o.side.a1"]
    queue.finish("h2o.calc.a1", "failed")
    queue.finish("h2o.side.a1", "completed")

    slept: list[float] = []
    report = controller.run(interval=5, sleep=slept.append)
    assert report["status"] == "failed"
    assert _statuses(controller) == {"calc": "failed", "check": "blocked", "side": "succeeded"}
    # The child was never submitted, nothing was retried, and the controller
    # stopped instead of waiting: nothing more can run.
    assert queue.names() == ["h2o.calc.a1", "h2o.side.a1"]
    assert slept == []


def test_the_controller_waits_while_jobs_run_and_advances_children(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME)

    def sleep(_seconds: float) -> None:
        # The "daemon": finish whatever is running between passes.
        for name in queue.names():
            job_id = next(j for j, _, n in queue.submitted if n == name)
            if queue.states[job_id] == "pending":
                queue.finish(name, "completed",
                             {"data.txt": "42\n"} if name.startswith("h2o.calc") else None)

    report = controller.run(interval=1, sleep=sleep)
    assert report["status"] == "succeeded"
    assert queue.names() == ["h2o.calc.a1", "h2o.check.a1"]
    assert [key for _, key, _ in queue.submitted] == [
        cp.idempotency_key(controller.digest, "calc", 1),
        cp.idempotency_key(controller.digest, "check", 1),
    ]
    check_job = next(j for j, _, n in queue.submitted if n == "h2o.check.a1")
    assert (queue.payloads[check_job] / "in.txt").read_text() == "42\n"


def test_a_retry_is_explicit_and_stages_from_the_new_attempt(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME)
    controller.step()
    queue.finish("h2o.calc.a1", "failed")
    controller.step()
    assert _statuses(controller)["calc"] == "failed"

    with pytest.raises(cp.CampaignError, match="only a failed or cancelled attempt"):
        controller.authorize_retry("check")
    retry = controller.authorize_retry("calc")
    assert (retry.attempt, retry.retry_of, retry.state) == (2, 1, "submitting")
    assert retry.idempotency_key == cp.idempotency_key(controller.digest, "calc", 2)
    assert queue.names() == ["h2o.calc.a1"]  # authorized, not sent

    controller.step()
    queue.finish("h2o.calc.a2", "completed", {"data.txt": "second\n"})
    controller.step()
    queue.finish("h2o.check.a1", "completed")
    controller.step()
    assert controller.status()["status"] == "succeeded"
    assert queue.names() == ["h2o.calc.a1", "h2o.calc.a2", "h2o.check.a1"]
    check = next(a for a in controller.journal.attempts if a.node == "check")
    assert check.inputs["data"].producer_attempt == 2


def test_an_output_the_job_did_not_leave_blocks_its_consumer(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME)
    controller.step()
    queue.finish("h2o.calc.a1", "completed")  # exit 0, but no data.txt

    report = controller.run(interval=1, sleep=lambda _s: None)
    assert "output data is missing" in controller.problems["calc"]
    assert _statuses(controller)["check"] == "blocked"
    assert report["status"] == "blocked"
    assert queue.names() == ["h2o.calc.a1"]


def test_an_output_that_changes_while_published_is_not_staged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data.txt": "42\n"})
    real_publish = cc.publish_artifact

    def racing_publish(root: Path, *args: Any, **kwargs: Any) -> cp.ArtifactManifest:
        manifest = real_publish(root, *args, **kwargs)
        (root / "data.txt").write_text("43\n")  # the job's workspace changes under us
        return manifest

    monkeypatch.setattr(cc, "publish_artifact", racing_publish)
    controller.step()
    calc = controller.journal.attempts[0]
    assert calc.artifacts["data"].status == "digest_mismatch"
    assert not controller.artifact_path("calc", 1, "data").exists()
    assert queue.names() == ["h2o.calc.a1"]

    monkeypatch.setattr(cc, "publish_artifact", real_publish)
    controller.step()  # published again, verified this time
    assert controller.journal.attempts[0].artifacts["data"].status == "verified"
    assert queue.names() == ["h2o.calc.a1", "h2o.check.a1"]


def test_a_refused_submission_is_not_a_failure_and_is_not_resubmitted(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, PRODUCE)
    queue.refuse.add("calc")
    controller.run(interval=1, sleep=lambda _s: None)
    assert controller.problems == {"calc": "vq submit refused: synthetic"}
    (attempt,) = controller.journal.attempts
    # Left for reconciliation under its key, never marked failed (which
    # would invite a retry under a new key).
    assert attempt.state == "submitting"
    assert _statuses(controller) == {"calc": "uncertain"}


def test_a_changed_payload_is_never_resent(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, PRODUCE, CONSUME)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"data.txt": "42\n"})
    queue.refuse.add("check")  # the consumer's first send gets no job
    controller.step()
    payload = controller.payload_dir("check", 1)
    (payload / "in.txt").write_text("tampered\n")

    queue.refuse.clear()
    restarted = cc.Controller(controller.campaign, controller.run_dir, queue)
    restarted.step()
    assert "staged input data is digest_mismatch" in restarted.problems["check"]
    assert queue.names() == ["h2o.calc.a1"]


def test_unsupported_nodes_are_refused_before_anything_is_submitted(tmp_path: Path) -> None:
    remote = {**SIDE, "host": "cluster-a.example.invalid"}
    pinned = {**PRODUCE, "runtime_digest": "a" * 64}
    campaign_file = _write(tmp_path, _campaign(remote, pinned))
    result = _run(str(campaign_file), str(tmp_path / "run"))
    assert result.exit_code == 1
    assert ("host cluster-a.example.invalid is neither this machine nor a configured host"
            in result.output)
    assert "node calc: declares runtime_digest" in result.output
    assert not (tmp_path / "run" / cc.JOURNAL_NAME).exists()


def test_one_controller_per_run_directory(tmp_path: Path) -> None:
    with (cc.locked(tmp_path / "run"),
          pytest.raises(cp.CampaignError, match="another controller"),
          cc.locked(tmp_path / "run")):
        pass


@pytest.mark.parametrize(("payload", "expected"), [
    ({"state": "pending"}, "accepted"),
    ({"state": "submit_outcome_unknown"}, "accepted"),
    ({"state": "suspended"}, "running"),
    ({"state": "completed", "exit_code": 0}, "succeeded"),
    ({"state": "completed", "exit_code": 3}, "failed"),
    ({"state": "killed"}, "cancelled"),
    ({"state": "oom_killed"}, "failed"),
    ({"state": "aborted_by_queue"}, "failed"),
    ({"state": "something_newer"}, "unknown"),
])
def test_queue_states_map_to_attempt_states(payload: dict[str, Any], expected: str) -> None:
    assert cc.observation_from_status(payload).state == expected


def test_job_names_fit_the_queue_limit() -> None:
    assert cc.job_name("h2o", "calc", 1) == "h2o.calc.a1"
    long = cc.job_name("c" * 64, "n" * 64, 12)
    assert len(long) <= 50 and long.startswith("n")
