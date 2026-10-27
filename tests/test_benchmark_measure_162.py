"""#162: `vq benchmark measure` writes a benchmark run's report.

The wrapper runs the program, passes its output through and keeps its exit
code; it splits startup from steady time at the first SCF iteration line,
reads energy, iterations and convergence with the calculation-preview
parsers, and names the runtime from a verified portable deployment, so the
reported digest is the code that ran. The last test runs it inside real
benchmark jobs through `vq benchmark run`.
"""
from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from vq import benchmark as bm
from vq import benchmark_measure as bmm
from vq import benchmark_runner as br
from vq import campaign as cp
from vq import config, paths
from vq import runtime_bundle as rb
from vq.daemon import Daemon

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle import _description, _profile, _trust  # noqa: E402

DIGEST = "d" * 64

#: A program that prints vibe-qc's output shape, pausing before its first SCF
#: iteration (startup) and between iterations (steady work).
FAKE_VIBEQC = r'''
import sys, time
startup, step, mode = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
print("Job: RHF basis=sto-3g", flush=True)
print("Atoms (bohr)", flush=True)
time.sleep(startup)
if mode == "silent":
    time.sleep(step)
    sys.exit(0)
if mode == "energy-only":
    print("  Total energy     -74.9629000000", flush=True)
    sys.exit(0)
print("  iter   energy (Ha)        dE          residual    nfock", flush=True)
for i, e in enumerate([-74.90, -74.95, -74.9629], start=1):
    print(f"  {i}   {e:.10f}   {'--' if i == 1 else '1.0e-03'}   1.0e-05   {i}", flush=True)
    time.sleep(step)
if mode == "fatal":
    print("FATAL: SCF diverged", flush=True)
    sys.exit(3)
print("SCF converged in 3 iterations; E = -74.9629 Ha", flush=True)
print("  Total energy     -74.9629000000", flush=True)
if mode == "incomplete-next-scf":
    print("  iter   energy (Ha)        dE          residual    nfock", flush=True)
    print("  1   -74.9629000000   --   1.0e-05   1", flush=True)
    sys.exit(0)
if mode == "unconverged-opt":
    print("Geometry optimization: 50 iters, E = -74.9629 Ha, max|grad| = 1.0e-02 "
          "converged=False", flush=True)
'''


@pytest.fixture
def fake(tmp_path: Path) -> Path:
    path = tmp_path / "fake_vibeqc.py"
    path.write_text(FAKE_VIBEQC)
    return path


def _measure(tmp_path: Path, fake: Path, *args: str, **kwargs: Any) -> tuple[int, dict]:
    report = tmp_path / "report.json"
    code = bmm.measure("vibe-qc", [sys.executable, str(fake), *args],
                       report=report, **{"runtime_digest": DIGEST, **kwargs})
    return code, json.loads(report.read_text())


class TestMeasure:
    def test_startup_and_steady_split_at_the_first_iteration(
        self, tmp_path: Path, fake: Path, capsys: pytest.CaptureFixture[str],
    ) -> None:
        code, report = _measure(tmp_path, fake, "1.0", "0.05", "ok")
        assert code == 0
        assert report["startup_seconds"] >= 1.0
        # Three 0.05 s iterations: steady time is the iterations, not the startup.
        assert 0.15 <= report["steady_seconds"] < report["startup_seconds"]
        assert (report["converged"], report["scf_iterations"], report["values"]) == (
            True, 3, {"total_energy": -74.9629})
        assert report["runtime_digest"] == DIGEST
        assert "SCF converged in 3 iterations" in capsys.readouterr().out  # passed through
        assert br.RunReport.model_validate(report).steady_seconds > 0

    def test_without_an_iteration_line_it_is_all_steady(self, tmp_path: Path, fake: Path) -> None:
        code, report = _measure(tmp_path, fake, "0.1", "0.1", "silent")
        assert code == 0 and "startup_seconds" not in report
        assert report["steady_seconds"] >= 0.2 and report["converged"] is False

    @pytest.mark.parametrize(("mode", "code"), [("fatal", 3), ("unconverged-opt", 0)])
    def test_a_failure_or_an_unconverged_optimisation_is_not_converged(
        self, tmp_path: Path, fake: Path, mode: str, code: int,
    ) -> None:
        returned, report = _measure(tmp_path, fake, "0", "0", mode)
        assert returned == code and report["converged"] is False

    @pytest.mark.parametrize("mode", ["energy-only", "incomplete-next-scf"])
    def test_energy_or_an_earlier_scf_result_does_not_prove_final_convergence(
        self, tmp_path: Path, fake: Path, mode: str,
    ) -> None:
        code, report = _measure(tmp_path, fake, "0", "0", mode)
        assert code == 0
        assert report["values"] == {"total_energy": -74.9629}
        assert report["converged"] is False
        assert ("startup_seconds" in report) == (mode != "energy-only")

    @pytest.mark.parametrize(("program", "output", "converged"), [
        ("orca", "FINAL SINGLE POINT ENERGY -74.9629\n", False),
        ("orca", "  1  -74.9000  -0.01  1e-5  1e-5\n"
         "FINAL SINGLE POINT ENERGY -74.9629\n", True),
        ("orca", "  1  -74.9000  -0.01  1e-5  1e-5\n"
         "FINAL SINGLE POINT ENERGY -74.9629\n"
         "  1  -74.9629  -0.01  1e-5  1e-5\n", False),
        ("vibe-qc", "  1  -74.9000  --  1e-5  1\n"
         "SCF converged in 1 iterations; E = -74.9629 Ha\n"
         "  iter   energy (Ha)        dE          residual    nfock\n", False),
    ])
    def test_completion_belongs_to_the_last_scf(
        self, tmp_path: Path, program: str, output: str, converged: bool,
    ) -> None:
        report = tmp_path / "report.json"
        code = bmm.measure(program, [sys.executable, "-c", f"print({output!r})"],
                           runtime_digest=DIGEST, report=report)
        assert code == 0
        assert json.loads(report.read_text())["converged"] is converged

    @pytest.mark.parametrize("fatal", [False, True])
    def test_scrolling_stream_evidence_out_of_the_tail_does_not_change_it(
        self, tmp_path: Path, fatal: bool,
    ) -> None:
        output = ("FATAL: SCF diverged\n" if fatal else "")
        output += "  1  -74.9000  --  1e-5  1\n"
        output += "SCF converged in 1 iterations; E = -74.9629 Ha\n"
        output += "additional output\n" * 20 + "  Total energy -74.9629\n"
        report = tmp_path / "report.json"
        code = bmm.measure("vibe-qc", [sys.executable, "-c", f"print({output!r})"],
                           runtime_digest=DIGEST, report=report, tail_bytes=80)
        assert code == 0
        measured = json.loads(report.read_text())
        assert measured["values"] == {"total_energy": -74.9629}
        assert measured["converged"] is not fatal

    @pytest.mark.parametrize(("kwargs", "message"), [
        ({"runtime_digest": None}, "exactly one of --runtime-root and --runtime-digest"),
        ({"runtime_digest": "short"}, "full SHA256"),
    ])
    def test_the_runtime_identity_is_required(
        self, tmp_path: Path, fake: Path, kwargs: dict, message: str,
    ) -> None:
        with pytest.raises(bmm.MeasureError, match=message):
            _measure(tmp_path, fake, "0", "0", "ok", **kwargs)

    def test_refusals_before_running_anything(self, tmp_path: Path) -> None:
        with pytest.raises(bmm.MeasureError, match="no output parser"):
            bmm.measure("crystal", ["true"], runtime_digest=DIGEST, report=tmp_path / "r")
        with pytest.raises(bmm.MeasureError, match="needs --runtime-root"):
            bmm.measure("vibe-qc", ["{runtime}/bin/x"], runtime_digest=DIGEST,
                        report=tmp_path / "r")
        assert not (tmp_path / "r").exists()


@pytest.fixture
def deployed(tmp_path: Path) -> tuple[Path, str]:
    """A real portable runtime deployment whose payload holds the fake program."""
    payload = tmp_path / "built"
    (payload / "bin").mkdir(parents=True)
    (payload / "bin/probe").write_text("#!/bin/sh\nexit 0\n")
    (payload / "bin/probe").chmod(0o755)
    (payload / "bin/fakeqc.py").write_text(FAKE_VIBEQC)
    manifest = rb.pack(payload, _description(tmp_path), tmp_path / "bundle")
    trust = _trust(tmp_path, manifest)
    rb.publish(tmp_path / "bundle", tmp_path / "store", trust)
    root = tmp_path / "runtime"
    rb.deploy(tmp_path / "store", manifest.artifact_id, root, trust, _profile())
    return root, manifest.artifact_id


class TestRuntimeRoot:
    def test_the_selected_generation_runs_and_is_reported(
        self, tmp_path: Path, deployed: tuple[Path, str],
    ) -> None:
        root, artifact = deployed
        report = tmp_path / "report.json"
        code = bmm.measure("vibe-qc",
                           [sys.executable, "{runtime}/bin/fakeqc.py", "0", "0", "ok"],
                           runtime_root=root, report=report)
        assert code == 0
        assert json.loads(report.read_text())["runtime_digest"] == artifact

    def test_a_selection_that_changes_mid_run_is_refused(
        self, tmp_path: Path, deployed: tuple[Path, str], monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root, artifact = deployed
        real = bmm.selected_runtime
        calls: list[int] = []

        def changing(path: Path) -> tuple[str, Path]:
            calls.append(1)
            current, payload = real(path)
            return (current if len(calls) == 1 else "e" * 64), payload

        monkeypatch.setattr(bmm, "selected_runtime", changing)
        with pytest.raises(bmm.MeasureError, match="selection changed during the run"):
            bmm.measure("vibe-qc", [sys.executable, "{runtime}/bin/fakeqc.py", "0", "0", "ok"],
                        runtime_root=root, report=tmp_path / "report.json")
        assert not (tmp_path / "report.json").exists()

    def test_an_unverified_or_empty_selection_is_refused(
        self, tmp_path: Path, deployed: tuple[Path, str],
    ) -> None:
        root, artifact = deployed
        receipt = root / "generations" / artifact / "verification.json"
        receipt.write_text(json.dumps({**json.loads(receipt.read_text()), "verified": False}))
        with pytest.raises(bmm.MeasureError, match="not a verified deployment"):
            bmm.selected_runtime(root)
        receipt.unlink()
        with pytest.raises(bmm.MeasureError, match="not a verified deployment"):
            bmm.selected_runtime(root)
        empty = tmp_path / "empty-runtime"
        empty.mkdir()
        with pytest.raises(bmm.MeasureError, match="has no selected runtime"):
            bmm.selected_runtime(empty)


# --- inside real benchmark jobs ------------------------------------------------------


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


def test_measure_inside_benchmark_jobs_end_to_end(tmp_path: Path, daemon: Daemon) -> None:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "fakeqc.py").write_text(FAKE_VIBEQC)
    generic, tuned = "1" * 64, "2" * 64
    plan_data = {
        "schema_name": "vq.benchmark-plan/1", "plan_id": "measure-pilot", "baseline": "generic",
        "variants": [{"name": "generic", "runtime_digest": generic},
                     {"name": "tuned", "runtime_digest": tuned}],
        "workloads": [{"name": "h2o", "input_digest": cp.source_digest(inputs),
                       "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]}],
        "node_classes": ["class-a"], "layout": {"mpi_ranks": 1, "threads_per_rank": 1},
        "warmup": 0, "repeats": 5, "seed": 3,
    }
    plan = bm.Plan.model_validate(plan_data)
    (tmp_path / "plan.json").write_text(json.dumps(plan_data))

    def wrapped(digest: str, step: str) -> list[str]:
        return [sys.executable, "-m", "vq", "benchmark", "measure", "--program", "vibe-qc",
                "--runtime-digest", digest, "--", sys.executable, "fakeqc.py", "0", step, "ok"]

    (tmp_path / "binding.json").write_text(json.dumps({
        "schema_name": "vq.benchmark-binding/1", "plan_sha256": bm.plan_digest(plan),
        "node_classes": {"class-a": {}},
        "variants": {"generic": {"command": wrapped(generic, "0.06")},
                     "tuned": {"command": wrapped(tuned, "0.03")}},
        "workloads": {"h2o": {"sources": "inputs"}},
    }))
    run_dir = tmp_path / "run"
    for _ in range(len(bm.run_order(plan)) + 1):
        with br.locked(run_dir):
            br.open_runner(tmp_path / "plan.json", tmp_path / "binding.json", run_dir,
                           hosts=frozenset()).step()
        daemon._dispatch_pending()
        deadline = time.monotonic() + 30
        while daemon._running:
            assert time.monotonic() < deadline
            daemon._reconcile_running()
            time.sleep(0.05)
    with br.locked(run_dir):
        br.open_runner(tmp_path / "plan.json", tmp_path / "binding.json", run_dir,
                       hosts=frozenset()).step()

    results = bm.load_results(run_dir / "results.json")
    assert len(results.runs) == 10
    assert {run.status for run in results.runs} == {"completed"}
    assert {run.variant: run.runtime_digest for run in results.runs} == {
        "generic": generic, "tuned": tuned}
    assert all(run.values == {"total_energy": -74.9629} and run.scf_iterations == 3
               for run in results.runs)
    (cell,) = bm.analyze(plan, results)["cells"]  # type: ignore[misc]
    assert cell["pairs"] == 5 and cell["excluded"] == []
