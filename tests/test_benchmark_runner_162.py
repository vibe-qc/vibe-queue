"""#162 acceptance item 1: run a benchmark plan through the queue.

`vq benchmark run PLAN BINDING RUN_DIR` submits the plan's runs in its
seeded order, one at a time per node class, through the keyed queue, and
collects each finished run from the job's report and measured usage into
`vq.benchmark-results/1`. The decisive case runs end to end: real
`vq submit`, an in-process daemon, real `vq fetch` and `vq status`, and the
real analysis over what came back.
"""
from __future__ import annotations

import json
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import benchmark as bm
from vq import benchmark_runner as br
from vq import campaign as cp
from vq import campaign_controller as cc
from vq import config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec

GENERIC, TUNED = "a" * 64, "b" * 64

#: The workload's own script: it plays the program under test, reporting a
#: steady time that depends on which variant ran it.
RUN_SH = '''variant="$1"
if [ "$variant" = tuned ]; then digest=@TUNED@; steady=8.0; else digest=@GENERIC@; steady=10.0; fi
cat > vq-benchmark-run.json <<EOF
{"schema_name": "vq.benchmark-run-report/1", "runtime_digest": "$digest",
 "steady_seconds": $steady, "startup_seconds": 0.5, "scf_iterations": 12,
 "values": {"total_energy": -76.0241}, "cpu_model": "Example CPU"}
EOF
'''.replace("@TUNED@", TUNED).replace("@GENERIC@", GENERIC)


def _setup(tmp_path: Path, *, node_classes: list[str] | None = None,
           hosts: dict[str, str | None] | None = None,
           script: str = RUN_SH) -> tuple[Path, Path, bm.Plan]:
    inputs = tmp_path / "inputs" / "h2o"
    inputs.mkdir(parents=True)
    (inputs / "run.sh").write_text(script)
    (inputs / "geometry.xyz").write_text("3\nwater\nO 0 0 0\nH 0 0 1\nH 0 1 0\n")
    classes = node_classes or ["class-a"]
    plan_data = {
        "schema_name": "vq.benchmark-plan/1", "plan_id": "isa-pilot", "baseline": "generic",
        "variants": [{"name": "generic", "runtime_digest": GENERIC},
                     {"name": "tuned", "runtime_digest": TUNED}],
        "workloads": [{"name": "h2o", "input_digest": cp.source_digest(inputs),
                       "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]}],
        "node_classes": classes, "layout": {"mpi_ranks": 1, "threads_per_rank": 1},
        "warmup": 0, "repeats": 5, "seed": 7,
    }
    plan = bm.Plan.model_validate(plan_data)
    (tmp_path / "plan.json").write_text(json.dumps(plan_data))
    binding = {
        "schema_name": "vq.benchmark-binding/1", "plan_sha256": bm.plan_digest(plan),
        "node_classes": {name: {"host": (hosts or {}).get(name)} for name in classes},
        "variants": {"generic": {"command": ["sh", "run.sh", "generic"]},
                     "tuned": {"command": ["sh", "run.sh", "tuned"]}},
        "workloads": {"h2o": {"sources": "inputs/h2o"}},
    }
    (tmp_path / "binding.json").write_text(json.dumps(binding))
    return tmp_path / "plan.json", tmp_path / "binding.json", plan


# --- end to end ----------------------------------------------------------------------


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


def test_a_plan_runs_end_to_end_one_job_per_run_never_overlapping(
    tmp_path: Path, daemon: Daemon,
) -> None:
    plan_file, binding_file, plan = _setup(tmp_path)
    run_dir = tmp_path / "run"
    first = CliRunner().invoke(main, ["benchmark", "run", str(plan_file), str(binding_file),
                                      str(run_dir), "--once"], catch_exceptions=False)
    assert first.exit_code == 0, first.output
    for _ in range(len(bm.run_order(plan)) + 1):
        _execute(daemon)
        with br.locked(run_dir):
            runner = br.open_runner(plan_file, binding_file, run_dir, hosts=frozenset())
            runner.step()
            # Never two runs of the node class in flight at once.
            assert sum(e.state == "accepted" for e in runner.journal.runs) <= 1
    specs = sorted((JobSpec.read(p) for p in daemon.queue_dir.glob("*.json")),
                   key=lambda s: s.submitted_at or "")
    assert len(specs) == len(bm.run_order(plan)) == 10
    for earlier, later in zip(specs, specs[1:], strict=False):
        assert earlier.finished_at is not None and later.submitted_at is not None
        assert later.submitted_at >= earlier.finished_at
    # The submission order is the plan's seeded order.
    order = bm.run_order(plan)
    assert [s.command[-1] for s in specs] == [str(r["variant"]) for r in order]

    results = bm.load_results(run_dir / "results.json")
    assert len(results.runs) == 10
    run = results.runs[0]
    assert (run.status, run.hardware.cpu_model, run.scf_iterations) == (
        "completed", "Example CPU", 12)
    assert run.peak_rss_mb is not None and run.cpu_seconds is not None
    report = bm.analyze(plan, results)
    (cell,) = report["cells"]  # type: ignore[misc]
    assert (cell["pairs"], cell["ratio"]["geomean"], cell["verdict"]) == (5, 1.25, "faster")

    again = CliRunner().invoke(main, ["benchmark", "run", str(plan_file), str(binding_file),
                                      str(run_dir)], catch_exceptions=False)
    assert again.exit_code == 0, again.output
    assert "10 recorded, 0 missing, 0 not yet run" in again.output
    assert len(list(daemon.queue_dir.glob("*.json"))) == 10


# --- the runner's decisions, against an in-memory queue -----------------------------


class FakeQueue:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.jobs: dict[str, cp.JobRef] = {}  # key -> job
        self.nodes: dict[str, cp.Node] = {}
        self.states: dict[str, str] = {}
        self.usage: dict[str, dict[str, Any]] = {}

    def workspace(self, job: cp.JobRef) -> Path:
        return self.root / job.job_id

    def submit(self, node: cp.Node, payload: Path, key: str, job_name: str) -> cp.JobRef:
        if key not in self.jobs:
            job = cp.JobRef(host=node.host or "localhost", job_id=f"job{len(self.jobs):09d}")
            self.workspace(job).mkdir(parents=True)
            for child in payload.iterdir():
                cc._copy(child, self.workspace(job) / child.name)
            self.jobs[key], self.nodes[job.job_id] = job, node
            self.states[job.job_id] = "pending"
        return self.jobs[key]

    def observe(self, job: cp.JobRef) -> cc.Observation:
        return cc.observation_from_status({
            "state": self.states[job.job_id], "exit_code": 0,
            "resource_usage": self.usage.get(job.job_id)})

    def fetch(self, job: cp.JobRef, name: str, into: Path) -> Path:
        source = self.workspace(job) / name
        if not source.exists():
            raise cc.FetchMissing(f"no {name}")
        cc._copy(source, into / name)
        return into / name

    def in_flight(self) -> list[cp.JobRef]:
        return [job for job in self.jobs.values() if self.states[job.job_id] == "pending"]

    def finish(self, job: cp.JobRef, state: str = "completed",
               report: dict[str, Any] | str | None = None) -> None:
        if report is not None:
            text = report if isinstance(report, str) else json.dumps(report)
            (self.workspace(job) / br.REPORT_NAME).write_text(text)
        self.states[job.job_id] = state


def _report(variant: str = "generic", **fields: Any) -> dict[str, Any]:
    return {"schema_name": br.REPORT_SCHEMA,
            "runtime_digest": GENERIC if variant == "generic" else TUNED,
            "steady_seconds": 10.0, "values": {"total_energy": -76.0241}, **fields}


def _runner(tmp_path: Path, **setup: Any) -> tuple[br.Runner, FakeQueue]:
    plan_file, binding_file, _ = _setup(tmp_path, **setup)
    queue = FakeQueue(tmp_path / "jobs")
    runner = br.open_runner(plan_file, binding_file, tmp_path / "run",
                            hosts=frozenset({"cluster-a", "cluster-b"}), queue=queue)
    return runner, queue


def _variant(queue: FakeQueue, job: cp.JobRef) -> str:
    return queue.nodes[job.job_id].command[-1]


def test_each_finished_job_becomes_one_result_row(tmp_path: Path) -> None:
    runner, queue = _runner(tmp_path)
    runner.step()
    (job,) = queue.in_flight()
    queue.usage[job.job_id] = {"status": "ok", "peak_rss_mb": 900.0, "active_cpu_seconds": 9.5}
    queue.finish(job, report=_report(_variant(queue, job), converged=False, mpi_ranks=1))
    runner.step()
    (run,) = runner.results.runs
    assert (run.status, run.peak_rss_mb, run.cpu_seconds) == ("unconverged", 900.0, 9.5)
    assert run.block == 0 and len(queue.in_flight()) == 1  # the next run is out


@pytest.mark.parametrize(("state", "status"), [
    ("oom_killed", "oom_killed"), ("time_exceeded", "time_exceeded"), ("killed", "killed"),
    ("failed", "failed"), ("aborted_by_queue", "failed"),
])
def test_a_job_that_did_not_complete_is_recorded_with_its_status(
    tmp_path: Path, state: str, status: str,
) -> None:
    runner, queue = _runner(tmp_path)
    runner.step()
    (job,) = queue.in_flight()
    queue.finish(job, state)
    runner.step()
    (run,) = runner.results.runs
    planned = runner.plan.variant(_variant(queue, job)).runtime_digest
    assert (run.status, run.runtime_digest, run.steady_seconds) == (status, planned, None)


@pytest.mark.parametrize(("report", "detail"), [
    (None, "the job wrote no vq-benchmark-run.json"),
    ("not json", "is not usable"),
    ({"schema_name": "vq.benchmark-run-report/1", "steady_seconds": 1.0},
     "is not usable"),
])
def test_a_completed_job_without_a_valid_report_is_left_missing(
    tmp_path: Path, report: Any, detail: str,
) -> None:
    runner, queue = _runner(tmp_path)
    runner.step()
    (job,) = queue.in_flight()
    queue.finish(job, report=report)
    runner.step()
    assert runner.results.runs == []
    (entry,) = [e for e in runner.journal.runs if e.state == "missing"]
    assert entry.detail is not None and detail in entry.detail
    assert len(queue.in_flight()) == 1  # the matrix carries on


def test_node_classes_run_side_by_side_but_never_overlap_within_one(tmp_path: Path) -> None:
    runner, queue = _runner(tmp_path, node_classes=["class-a", "class-b"],
                            hosts={"class-a": "cluster-a", "class-b": "cluster-b"})
    for _ in range(25):
        runner.step()
        flying = queue.in_flight()
        assert sorted(job.host for job in flying) in ([], ["cluster-a", "cluster-b"],
                                                      ["cluster-a"], ["cluster-b"])
        for job in flying:
            queue.finish(job, report=_report(_variant(queue, job)))
    assert len(runner.results.runs) == 20 and runner.pending() == 0


def test_a_restart_resends_under_the_same_key(tmp_path: Path) -> None:
    runner, queue = _runner(tmp_path)

    class LostAnswer(FakeQueue):
        lose = True

        def submit(self, *args: Any) -> cp.JobRef:
            job = super().submit(*args)
            if self.lose:
                raise cc.SubmitUncertain("the answer was lost")
            return job

    lossy = LostAnswer(tmp_path / "jobs")
    runner.queue = lossy
    runner.step()
    assert runner.transient and len(lossy.jobs) == 1
    (entry,) = runner.journal.runs
    assert entry.state == "submitting"
    lossy.lose = False
    restarted = br.Runner(runner.plan, runner.binding, runner.base, runner.run_dir, lossy)
    restarted.step()
    assert len(lossy.jobs) == 1
    (entry,) = restarted.journal.runs
    assert entry.state == "accepted" and entry.job is not None


@pytest.mark.parametrize("interrupted_file", ["results.json", "journal.json"])
def test_an_interrupted_result_checkpoint_is_recovered_without_losing_or_duplicating_a_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupted_file: str,
) -> None:
    runner, queue = _runner(tmp_path)
    runner.step()
    (job,) = queue.in_flight()
    queue.finish(job, report=_report(_variant(queue, job)))
    write_atomic = br._write_atomic

    def interrupt(path: Path, text: str) -> None:
        if path.name == interrupted_file:
            raise OSError("interrupted checkpoint write")
        write_atomic(path, text)

    with monkeypatch.context() as patch:
        patch.setattr(br, "_write_atomic", interrupt)
        runner.step()
    assert runner.problems == {0: "interrupted checkpoint write"}

    restarted = br.Runner(runner.plan, runner.binding, runner.base, runner.run_dir, queue)
    restarted.step()
    (run,) = bm.load_results(runner.run_dir / "results.json").runs
    assert run.key == ("h2o", _variant(queue, job), "class-a", 0)
    assert run.status == "completed" and run.steady_seconds == 10.0
    assert restarted.journal.runs[0].state == "recorded"
    # Recovery collects the accepted job again, then starts only the next run.
    assert len(queue.jobs) == 2 and len(queue.in_flight()) == 1


def test_the_runtime_the_job_reports_is_recorded_not_the_planned_one(tmp_path: Path) -> None:
    runner, queue = _runner(tmp_path)
    runner.step()
    (job,) = queue.in_flight()
    queue.finish(job, report=_report(_variant(queue, job), runtime_digest="d" * 64))
    runner.step()
    (run,) = runner.results.runs
    assert run.runtime_digest == "d" * 64  # the analysis then excludes it as off-plan


def test_a_changed_staged_input_is_never_sent(tmp_path: Path) -> None:
    runner, queue = _runner(tmp_path)

    class Refusing(FakeQueue):
        def submit(self, *args: Any) -> cp.JobRef:
            raise cc.SubmitUncertain("no answer")

    runner.queue = Refusing(tmp_path / "jobs")
    runner.step()
    (runner.run_dir / "payloads/0/run.sh").write_text("rm -rf ~\n")
    restarted = br.Runner(runner.plan, runner.binding, runner.base, runner.run_dir, queue)
    restarted.step()
    assert "does not match the plan's input_digest" in restarted.problems[0]
    assert queue.jobs == {}


class TestBinding:
    def test_problems_are_listed_before_anything_runs(self, tmp_path: Path) -> None:
        plan_file, binding_file, plan = _setup(tmp_path, hosts={"class-a": "nowhere"})
        binding = json.loads(binding_file.read_text())
        binding["variants"].pop("tuned")
        binding["workloads"]["extra"] = {"sources": "inputs/h2o"}
        binding_file.write_text(json.dumps(binding))
        (tmp_path / "inputs/h2o/run.sh").write_text("changed\n")
        problems = br.check_binding(plan, br.load_binding(binding_file), tmp_path)
        assert "the binding has no variant tuned" in problems
        assert "the binding names workload extra, which the plan does not" in problems
        assert any("host nowhere is neither this machine nor a configured host" in p
                   for p in problems)
        assert any("inputs/h2o has sha256" in p and "the plan pins" in p for p in problems)
        with pytest.raises(br.RunnerError, match="cannot run the plan"):
            br.open_runner(plan_file, binding_file, tmp_path / "run", hosts=frozenset())
        assert not (tmp_path / "run").exists()

    def test_a_binding_for_another_plan_is_refused(self, tmp_path: Path) -> None:
        _, binding_file, plan = _setup(tmp_path)
        binding = json.loads(binding_file.read_text())
        binding["plan_sha256"] = "c" * 64
        binding_file.write_text(json.dumps(binding))
        (problem,) = br.check_binding(plan, br.load_binding(binding_file), tmp_path)
        assert "the binding is for plan cccccccccccc" in problem
