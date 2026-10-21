"""#165: a node's declared runtime_digest binds its job on a scheduler target too.

The controller submits a bound node on a scheduler target exactly as on a
daemon host, and the queue records the named generation for its job script
to verify on the compute node (#161). What differs is the proof that it ran:
a scheduler job's ``started_at`` is set when it is handed to the scheduler,
before its node checks the generation, and a failed check exits 126 without
starting the command. So the generation counts as executed only once the
job completed with exit code 0; until then the attempt records none and is
not taken to have run anything else either. The decisive case runs end to
end on a local stand-in for the cluster whose ``qsub`` runs the real job
script: a refused generation fails the attempt with nothing recorded or
published, and after the deployment is repaired the retry publishes the
declared generation's result while the current selection is another one.
"""
from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import campaign as cp
from vq import campaign_controller as cc
from vq.cli import main
from vq.daemon import Daemon
from vq.scheduler_dialect import TorqueDialect
from vq.scheduler_dispatch import RUNTIME_GENERATION_FAILURE_EXIT_CODE, SchedulerDispatcher
from vq.spec import JobSpec, JobState, RuntimeBundlePin

sys.path.insert(0, str(Path(__file__).parent))
from test_campaign_controller_165 import (  # noqa: E402
    FakeQueue,
    _journal,
    _run,
    queue_env,  # noqa: F401 -- pytest fixture
)
from test_runtime_binding_161 import _deploy, generations  # noqa: E402, F401
from test_runtime_binding_scheduler_161 import LocalCluster  # noqa: E402
from test_scheduler_dispatch import _install_fake_gnu_time  # noqa: E402

A = "a" * 64
B = "b" * 64
LAUNCHER = "/cluster/launchers/vibe-qc-" + "c" * 40


def _node(**fields: Any) -> dict[str, Any]:
    return {"id": "calc", "kind": "compute", "cpus": 1, "command": ["true"], "host": "hpc_a",
            "runtime_digest": A, "runtime_root": "/cluster/rt",
            "outputs": [{"name": "out", "path": "out", "kind": "directory"}], **fields}


def _campaign(*nodes: dict[str, Any]) -> cp.Campaign:
    return cp.Campaign.model_validate(
        {"schema_name": "vq.campaign/1", "campaign_id": "h2o", "nodes": list(nodes)})


def _config(scratch: Path) -> str:
    return "\n".join([
        "[hosts.hpc_a]", 'ssh = "hpc-a.invalid"', 'scheduler = "pbs"',
        'scheduler_dialect = "torque"', f'scratch_root = "{scratch}"',
        'scheduler_driver = "localhost"', ""])


class TestPreflight:
    def test_a_scheduler_target_can_bind_one(self) -> None:
        assert cc.preflight(_campaign(_node()), hosts=frozenset({"hpc_a"}),
                            scheduler_hosts=frozenset({"hpc_a"})) == []

    @pytest.mark.parametrize("root", ["/cluster/../etc", "/cluster/rt\n"])
    def test_its_root_must_be_a_plain_cluster_path(self, root: str) -> None:
        campaign = _campaign(_node(runtime_root=root))
        (problem,) = cc.preflight(campaign, hosts=frozenset({"hpc_a"}),
                                  scheduler_hosts=frozenset({"hpc_a"}))
        assert problem == ("node calc: vq submit would refuse its runtime_root: "
                           "--runtime-root must be a plain absolute path on the cluster")
        # A daemon host resolves its own path, as `vq submit` lets it.
        assert cc.preflight(campaign, hosts=frozenset({"hpc_a"})) == []


def test_the_cli_checks_a_configured_scheduler_targets_root(
    tmp_path: Path, queue_env: Daemon,  # noqa: F811
) -> None:
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(_config(tmp_path / "scratch"))
    campaign_file = tmp_path / "campaign.json"
    campaign_file.write_text(json.dumps({
        "schema_name": "vq.campaign/1", "campaign_id": "h2o",
        "nodes": [_node(runtime_root="/cluster/../rt")]}))
    result = CliRunner().invoke(main, ["campaign", "run", str(campaign_file),
                                       str(tmp_path / "run"), "--once"])
    assert result.exit_code != 0
    assert "vq submit would refuse its runtime_root" in result.output
    assert list(queue_env.queue_dir.glob("*.json")) == []
    help_text = " ".join(CliRunner().invoke(main, ["campaign", "run", "--help"]).output.split())
    assert "on a scheduler target once it completed with exit code 0" in help_text


def _status(state: str, exit_code: int | None, **fields: Any) -> dict[str, Any]:
    """A bound scheduler job's `vq status --json` payload, its driver's JobSpec
    dump: ``started_at`` is set once the job was handed to the scheduler."""
    spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=1, scheduler_target="hpc_a",
                   runtime_bundle=RuntimeBundlePin(root="/cluster/rt", artifact_id=A),
                   started_at="2026-10-19T10:00:00+00:00")
    return {**json.loads(spec.model_dump_json()), "state": state, "exit_code": exit_code,
            **fields}


class TestConfirmation:
    def test_only_a_zero_exit_proves_the_node_ran_it(self) -> None:
        done = cc.observation_from_status(_status("completed", 0))
        assert done.runtime is not None and not done.generation_unconfirmed
        assert (done.runtime.program, done.runtime.artifact_identity, done.runtime.runtime_root) \
            == ("runtime-bundle", A, f"/cluster/rt/generations/{A}/payload")

    @pytest.mark.parametrize(("state", "exit_code"), [
        ("running", None),  # handed to the scheduler: started_at is already set
        ("completed", None),
        ("failed", RUNTIME_GENERATION_FAILURE_EXIT_CODE),  # the node refused it
        ("failed", 1),
        ("failed", 0),  # e.g. failed after its command; only a completion counts
        ("killed", None),
    ])
    def test_otherwise_no_generation_is_recorded(self, state: str, exit_code: int | None) -> None:
        seen = cc.observation_from_status(_status(state, exit_code))
        assert seen.runtime is None and seen.generation_unconfirmed

    @pytest.mark.parametrize(("stderr", "reason"), [
        ("vq: bound runtime generation aaaaaaaaaaaa unavailable on this node: its manifest "
         "is not that generation; command not started\n",
         "vq: bound runtime generation aaaaaaaaaaaa unavailable on this node: its manifest "
         "is not that generation; command not started"),
        ("", "its node refused the bound runtime generation"),
    ])
    def test_a_refusal_says_why(self, stderr: str, reason: str) -> None:
        seen = cc.observation_from_status(_status(
            "failed", RUNTIME_GENERATION_FAILURE_EXIT_CODE, stderr=stderr,
            resource_usage={"status": "error", "error": "runtime_generation_unavailable"}))
        assert (seen.state, seen.detail) == ("failed", f"job failed: {reason}")

    def test_another_failure_is_not_taken_for_one(self) -> None:
        seen = cc.observation_from_status(_status(
            "failed", RUNTIME_GENERATION_FAILURE_EXIT_CODE, stderr="sh: run: Permission denied\n",
            resource_usage={"status": "ok"}))
        assert seen.detail == "job failed"

    def test_a_daemon_job_is_still_confirmed_once_started(self) -> None:
        seen = cc.observation_from_status(_status("running", None, scheduler_target=None))
        assert seen.runtime is not None and seen.runtime.artifact_identity == A
        assert not seen.generation_unconfirmed

    def test_an_unbound_job_is_not_waiting_for_one(self) -> None:
        seen = cc.observation_from_status(_status("running", None, runtime_bundle=None))
        assert seen.runtime is None and not seen.generation_unconfirmed


class SchedulerQueue(FakeQueue):
    """Reports each job as its driver's record does: bound to ``bound``, started
    once accepted, and resolved to a program's launcher when ``launcher``."""

    def __init__(self, root: Path, bound: str, launcher: bool = False) -> None:
        super().__init__(root)
        self.bound, self.launcher = bound, launcher
        self.exit_codes: dict[str, int] = {}

    def observe(self, job: cp.JobRef) -> cc.Observation:
        state = self.states[job.job_id]
        program = {"program": "vibe-qc", "program_runtime_pin": {
            "program_version": "1.0", "resolved_runtime_root": "/cluster/slots/gen-3",
            "artifact_identity": LAUNCHER}} if self.launcher else {}
        return cc.observation_from_status(_status(
            state, self.exit_codes.get(job.job_id, 0 if state == "completed" else None),
            cwd=str(self.payloads[job.job_id]),
            runtime_bundle={"root": "/cluster/rt", "artifact_id": self.bound}, **program))


def _controller(tmp_path: Path, bound: str, launcher: bool = False,
                ) -> tuple[cc.Controller, SchedulerQueue]:
    queue = SchedulerQueue(tmp_path / "jobs", bound, launcher)
    return cc.Controller(_campaign(_node()), tmp_path / "run", queue), queue


@pytest.mark.parametrize("launcher", [False, True])
def test_a_running_job_waits_for_its_exit_and_then_publishes(
    tmp_path: Path, launcher: bool,
) -> None:
    controller, queue = _controller(tmp_path, A, launcher)
    controller.step()
    queue.finish("h2o.calc.a1", "running")
    controller.step()
    # Neither a missing generation nor the program's launcher is a mismatch
    # while the node may not have checked it yet, and neither is recorded as it.
    assert controller.problems == {}
    (attempt,) = controller.journal.attempts
    assert attempt.state == "running"
    assert (attempt.runtime.artifact_identity if attempt.runtime else None) == (
        LAUNCHER if launcher else None)
    queue.finish("h2o.calc.a1", "completed", {"out/value": "41\n"})
    controller.step()
    assert controller.problems == {}
    (attempt,) = controller.journal.attempts
    assert attempt.runtime is not None and attempt.runtime.artifact_identity == A
    manifest = cp.load_manifest(controller.artifact_path("calc", 1, "out")
                                .with_name("out.manifest.json"))
    assert manifest.runtime_digest == A
    assert manifest.executed_runtime == attempt.runtime
    assert controller.status()["status"] == "succeeded"


@pytest.mark.parametrize("exit_code", [None, 0])
def test_a_job_bound_to_anything_else_or_unproven_is_never_published(
    tmp_path: Path, exit_code: int | None,
) -> None:
    # Exit 0 bound to B ran B; a completion without an exit code proves nothing.
    controller, queue = _controller(tmp_path, B if exit_code == 0 else A)
    controller.step()
    queue.finish("h2o.calc.a1", "completed", {"out/value": "42\n"})
    queue.exit_codes[queue.submitted[0][0]] = exit_code  # type: ignore[assignment]
    controller.step()
    ran = f"runtime {B[:12]}" if exit_code == 0 else "no recorded runtime generation"
    assert f"ran {ran}, not the declared runtime {A[:12]}" in controller.problems["calc"]
    assert not controller.artifact_path("calc", 1, "out").exists()


def test_a_refused_generation_fails_the_attempt_without_recording_it(tmp_path: Path) -> None:
    controller, queue = _controller(tmp_path, A)
    controller.step()
    queue.exit_codes[queue.submitted[0][0]] = RUNTIME_GENERATION_FAILURE_EXIT_CODE
    queue.finish("h2o.calc.a1", "failed")
    controller.step()
    (attempt,) = controller.journal.attempts
    assert (attempt.state, attempt.runtime, attempt.artifacts) == ("failed", None, {})
    assert controller.problems == {}  # a failed attempt, retried only when authorized
    assert not controller.artifact_path("calc", 1, "out").exists()


@pytest.fixture
def cluster(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, queue_env: Daemon,  # noqa: F811
) -> Iterator[LocalCluster]:
    """hpc_a: a configured scheduler target that this machine's daemon drives,
    whose ``qsub`` runs the job script here."""
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(_config(tmp_path / "scratch"))
    stand_in = LocalCluster()
    queue_env._scheduler_dispatchers["hpc_a"] = SchedulerDispatcher(
        TorqueDialect(), stand_in, scratch_root=str(tmp_path / "scratch"),  # type: ignore[arg-type]
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)))
    yield stand_in
    stand_in.stop()


def _hand_over(daemon: Daemon, cluster: LocalCluster) -> JobSpec:
    """Hand the pending job to the scheduler and let its script finish on the
    node; the driver has not reconciled it, so it still reads running."""
    daemon._dispatch_pending()
    (spec,) = [s for s in (JobSpec.read(p) for p in daemon.queue_dir.glob("*.json"))
               if not s.is_terminal]
    assert spec.state == JobState.RUNNING and spec.started_at
    cluster.wait()
    return spec


def _reconcile(daemon: Daemon, spec: JobSpec) -> JobSpec:
    deadline = time.monotonic() + 30
    while not (spec := JobSpec.read(daemon._spec_path(spec.id))).is_terminal:
        assert time.monotonic() < deadline, spec.state
        daemon._reconcile_scheduler()
        time.sleep(0.05)
    return spec


def test_end_to_end_the_node_runs_its_declared_generation_or_nothing(
    tmp_path: Path, queue_env: Daemon, cluster: LocalCluster,  # noqa: F811
    generations: tuple[Path, str, str, Path],  # noqa: F811
) -> None:
    root, a, b, trust = generations
    _deploy(tmp_path, root, b, trust)  # the current selection is B; the node declares A
    payload = root / "generations" / a / "payload"
    mode = payload.stat().st_mode
    payload.chmod(0o775)  # the node check refuses a group-writable payload
    campaign_file = tmp_path / "campaign.json"
    campaign_file.write_text(json.dumps({
        "schema_name": "vq.campaign/1", "campaign_id": "h2o",
        "nodes": [_node(runtime_digest=a, runtime_root=str(root), command=[
            "sh", "-c", 'mkdir -p out && cat "$VQ_RUNTIME_PAYLOAD/data/value" > out/value'])]}))
    run_dir = tmp_path / "run"

    def attempt(number: int) -> cp.Attempt:
        return _journal(run_dir).attempts[number - 1]

    assert _run(str(campaign_file), str(run_dir), "--once").exit_code == 0
    refused = _hand_over(queue_env, cluster)
    # The node has already refused the generation, but the job still reads
    # running: nothing says it ran A, and nothing says it ran anything else.
    middle = _run(str(campaign_file), str(run_dir), "--once")
    assert middle.exit_code == 0, middle.output
    assert (attempt(1).state, attempt(1).runtime) == ("running", None)
    failed = _reconcile(queue_env, refused)
    assert (failed.state, failed.exit_code) == (
        JobState.FAILED, RUNTIME_GENERATION_FAILURE_EXIT_CODE)
    ended = _run(str(campaign_file), str(run_dir), "--interval", "1")
    assert ended.exit_code == 1, ended.output
    assert (f"calc: attempt 1 failed (job failed: vq: bound runtime generation {a[:12]} "
            "unavailable on this node: its payload is group or world writable; command not "
            "started)") in ended.output
    assert (attempt(1).state, attempt(1).runtime, attempt(1).artifacts) == ("failed", None, {})
    assert not (run_dir / "artifacts").exists()

    payload.chmod(mode)  # the deployment is repaired; the operator authorizes a retry
    retry = CliRunner().invoke(main, ["campaign", "retry", str(campaign_file), str(run_dir),
                                      "calc"])
    assert retry.exit_code == 0, retry.output
    assert _run(str(campaign_file), str(run_dir), "--once").exit_code == 0
    ran = _hand_over(queue_env, cluster)
    assert _run(str(campaign_file), str(run_dir), "--once").exit_code == 0
    assert (attempt(2).state, attempt(2).runtime) == ("running", None)
    assert _reconcile(queue_env, ran).state == JobState.COMPLETED
    final = _run(str(campaign_file), str(run_dir), "--interval", "1")
    assert final.exit_code == 0, final.output
    assert attempt(2).runtime is not None and attempt(2).runtime.artifact_identity == a
    artifact = run_dir / "artifacts" / "calc" / "2" / "out"
    assert (artifact / "value").read_text() == "41\n"  # A's payload, not the current B's 42
    manifest = json.loads(artifact.with_name("out.manifest.json").read_text())
    assert manifest["runtime_digest"] == a
    assert manifest["executed_runtime"]["artifact_identity"] == a
