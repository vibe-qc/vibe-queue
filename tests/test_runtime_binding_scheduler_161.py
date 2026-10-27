"""#161: a scheduler job bound to a runtime generation verifies it on its node.

A daemon host checks a generation on its own filesystem; a scheduler driver
cannot see the cluster's. So a scheduler target takes the generation by its
identity, records it unverified, and the job script checks it on the compute
node before the command: the canonical manifest's SHA256 is the identity, the
deployment receipt records it verified, nothing is a link and the payload is
not group or world writable. Otherwise the command never starts and the job
fails with exit code 126. The decisive case runs the daemon's real scheduler
start path on a local stand-in for the cluster, with real deployed
generations.
"""
from __future__ import annotations

import os
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import paths
from vq import runtime_bundle as rb
from vq import submit as submit_module
from vq.cli import main
from vq.daemon import Daemon, _runtime_bundle_dispatch_failure
from vq.scheduler_dialect import SchedulerPhase, TorqueDialect
from vq.scheduler_dispatch import (
    RUNTIME_GENERATION_FAILURE_EXIT_CODE,
    RemoteResult,
    SchedulerDispatcher,
)
from vq.spec import JobSpec, JobState, RuntimeBundlePin

sys.path.insert(0, str(Path(__file__).parent))
from test_daemon_scheduler import MockDispatcher, _inject, _submit_scheduler  # noqa: E402
from test_runtime_binding_161 import _deploy, generations  # noqa: E402, F401
from test_scheduler_dispatch import (  # noqa: E402
    FakeRunner,
    _install_fake_gnu_time,
    make_dispatcher,
)
from test_submit_plan_contract import PlanHarness, plan_harness  # noqa: E402, F401

Generations = tuple[Path, str, str, Path]


@pytest.fixture
def daemon(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Daemon]:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    queue = Daemon(max_cpus=4, poll_interval=0.05, queue_dir=tmp_path / "queue",
                   jobs_dir=tmp_path / "jobs")
    queue.queue_dir.mkdir(parents=True, exist_ok=True)
    queue.jobs_dir.mkdir(parents=True, exist_ok=True)
    yield queue


class TestSubmit:
    def test_a_scheduler_target_needs_the_generation_named(
        self, plan_harness: PlanHarness,  # noqa: F811
    ) -> None:
        source = plan_harness.root / "bound.py"
        source.write_text("pass\n", encoding="utf-8")
        base = ["submit", "--host", "local-host_f", "--python", "/local-cluster/runtime/python",
                "--json", "--runtime-root", "/cluster/runtime"]
        unnamed = CliRunner().invoke(main, [*base, str(source)])
        assert unnamed.exit_code != 0
        assert "needs --runtime-artifact ID" in unnamed.output
        assert plan_harness.calls == []
        named = CliRunner().invoke(main, [*base, "--runtime-artifact", "a" * 64, str(source)])
        assert named.exit_code == 0, named.output
        ((_kind, kwargs),) = plan_harness.calls
        assert (kwargs["runtime_root"], kwargs["runtime_artifact"], kwargs["scheduler_target"]) \
            == ("/cluster/runtime", "a" * 64, "local-host_f")
        help_text = " ".join(CliRunner().invoke(main, ["submit", "--help"]).output.split())
        assert "verifies that generation on its node and fails with exit code 126" in help_text
        assert "Required on a scheduler target." in help_text

    @pytest.mark.parametrize("root", ["/cluster/../etc", "/cluster/rt\n", "/" + "r" * 4096])
    def test_a_cluster_path_must_be_plain(self, root: str) -> None:
        with pytest.raises(ValueError, match="plain absolute path on the cluster"):
            submit_module._validate_runtime_bundle_request(root, "a" * 64,
                                                           scheduler_target="host_f")

    def test_the_binding_is_recorded_as_named_without_a_local_check(self, tmp_path: Path) -> None:
        (tmp_path / "payload").mkdir()
        (tmp_path / "payload" / "README").write_text("calculation\n")
        jobid = submit_module.submit_local(
            host="localhost", directory=str(tmp_path / "payload"), command=["true"],
            scheduler_target="host_f",
            runtime_root="/cluster//runtime/", runtime_artifact="a" * 64,
            queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs")
        spec = JobSpec.read(tmp_path / "queue" / f"{jobid}.json")
        # Nothing exists at that path here: only the node can check it.
        assert spec.runtime_bundle == RuntimeBundlePin(root="/cluster/runtime",
                                                       artifact_id="a" * 64)


class TestDaemon:
    def test_the_gate_leaves_a_scheduler_job_to_its_node(self) -> None:
        bound = JobSpec(id="x", command=["true"], cwd="/w", cpus=1, scheduler_target="host_f",
                        runtime_bundle=RuntimeBundlePin(root="/nowhere", artifact_id="a" * 64))
        assert _runtime_bundle_dispatch_failure(bound) is None
        # The same unverifiable binding on a daemon host is refused.
        local = bound.model_copy(update={"scheduler_target": None})
        assert "bound runtime generation unavailable" in str(
            _runtime_bundle_dispatch_failure(local))

    @pytest.mark.parametrize("bound", [True, False])
    def test_the_dispatcher_gets_the_binding_only_when_set(
        self, daemon: Daemon, bound: bool,
    ) -> None:
        mock = MockDispatcher(phase=SchedulerPhase.RUNNING)
        _inject(daemon, mock)
        spec = _submit_scheduler(daemon, "j1")
        pin = RuntimeBundlePin(root="/cluster/rt", artifact_id="a" * 64)
        if bound:
            spec.runtime_bundle = pin
            spec.write(daemon._spec_path("j1"))
        daemon._start_scheduler_job(spec)
        if bound:
            assert mock.submit_kwargs[-1]["runtime_bundle"] == pin
        else:
            assert "runtime_bundle" not in mock.submit_kwargs[-1]


class TestScript:
    def test_the_check_runs_after_the_collector_and_before_the_command(self) -> None:
        dispatcher = make_dispatcher(FakeRunner())
        pin = RuntimeBundlePin(root="/cluster/rt", artifact_id="a" * 64)
        script = dispatcher.build_job_script(
            job_id="j1", command=["python3", "run.py"], remote_workspace="/w", cpus=1,
            runtime_bundle=pin)
        check = script.index("__vq_rt_check ||")
        assert script.index("gnu_time_output_incompatible") < check < script.index("run.py")
        assert f"  __vq_write_marker {RUNTIME_GENERATION_FAILURE_EXIT_CODE}" in script
        plain = dispatcher.build_job_script(
            job_id="j1", command=["python3", "run.py"], remote_workspace="/w", cpus=1)
        assert "__vq_rt_" not in plain and "runtime_generation_unavailable" not in plain


def _tools(tmp_path: Path, *names: str) -> str:
    """A PATH holding only ``names`` (the check needs ls, cut and grep)."""
    directory = tmp_path / ("bin-" + "-".join(names))
    directory.mkdir()
    for name in names:
        found = shutil.which(name)
        assert found is not None, name
        (directory / name).symlink_to(found)
    return str(directory)


def _check(root: Path, artifact: str, *, path: str | None = None) -> tuple[int, str]:
    script = "\n".join(["set -u", 'fail() { printf "%s\\n" "$1"; exit 3; }',
                        *rb.node_check_lines(str(root), artifact, fail="fail"), "echo ok"])
    done = subprocess.run(["/bin/bash", "-c", script], capture_output=True, text=True,
                          env={"PATH": path or os.environ["PATH"]}, check=False)
    return done.returncode, done.stdout.strip()


def _writable(path: Path) -> Path:
    path.chmod(path.stat().st_mode | 0o200)
    return path


def _manifest_changed(gen: Path) -> None:
    _writable(gen / "manifest.json").write_bytes((gen / "manifest.json").read_bytes() + b" ")


def _manifest_linked(gen: Path) -> None:
    copy = gen.parent.parent / "manifest-copy.json"
    shutil.copyfile(gen / "manifest.json", copy)
    _writable(gen / "manifest.json").unlink()
    (gen / "manifest.json").symlink_to(copy)


def _receipt(gen: Path, old: str, new: str) -> None:
    receipt = _writable(gen / "verification.json")
    text = receipt.read_text()
    assert old in text
    receipt.write_text(text.replace(old, new))


class TestNodeCheck:
    def test_a_deployed_generation_holds_with_either_tool(
        self, tmp_path: Path, generations: Generations,  # noqa: F811
    ) -> None:
        root, a, _b, _trust = generations
        assert _check(root, a) == (0, "ok")
        assert _check(root, a, path=_tools(tmp_path, "ls", "cut", "grep", "shasum")) == (0, "ok")
        assert _check(root, a, path=_tools(tmp_path, "ls", "cut", "grep", "sha256sum")) == (
            0, "ok")
        assert _check(root, a, path=_tools(tmp_path, "ls", "cut", "grep")) == (
            3, "no sha256sum or shasum to check its manifest")

    @pytest.mark.parametrize(("tamper", "reason"), [
        (_manifest_changed, "its manifest is not that generation"),
        (_manifest_linked, "manifest.json is not a regular file"),
        (lambda gen: _receipt(gen, '"verified":true', '"verified":false'),
         "its deployment receipt does not record it verified"),
        (lambda gen: _receipt(gen, '"schema":"vq.runtime-deployment/1"', '"schema":"other/1"'),
         "its deployment receipt does not record it verified"),
        (lambda gen: (gen / "payload").chmod(0o775), "its payload is group or world writable"),
        (lambda gen: (gen / "payload").chmod(0o757), "its payload is group or world writable"),
    ])
    def test_anything_else_is_refused_with_its_reason(
        self, generations: Generations, tamper: Callable[[Path], Any],  # noqa: F811
        reason: str,
    ) -> None:
        root, a, _b, _trust = generations
        tamper(root / "generations" / a)
        code, said = _check(root, a)
        assert code == 3 and said.endswith(reason), said

    def test_a_receipt_for_another_generation_is_refused(
        self, tmp_path: Path, generations: Generations,  # noqa: F811
    ) -> None:
        root, a, b, trust = generations
        _deploy(tmp_path, root, b, trust)
        _receipt(root / "generations" / a, f'"artifact_id":"{a}"', f'"artifact_id":"{b}"')
        assert _check(root, a) == (3, "its deployment receipt does not record it verified")
        assert _check(root, b) == (0, "ok")

    def test_an_undeployed_or_linked_generation_is_refused(
        self, tmp_path: Path, generations: Generations,  # noqa: F811
    ) -> None:
        root, a, b, _trust = generations
        assert _check(root, b) == (3, f"{root}/generations/{b} is not a directory")
        linked = tmp_path / "linked-root"
        (linked / "generations").mkdir(parents=True)
        (linked / "generations" / a).symlink_to(root / "generations" / a)
        assert _check(linked, a) == (3, f"{linked}/generations/{a} is not a directory")
        with pytest.raises(rb.BundleError, match="full SHA256"):
            rb.node_check_lines(str(root), "abc", fail="fail")


class LocalCluster:
    """``qsub`` runs the job script here; the workspace stands in for shared storage."""

    def __init__(self) -> None:
        self.processes: list[subprocess.Popen[bytes]] = []

    def run(self, argv: list[str], *, stdin_data: str | None = None,
            check: bool = False) -> RemoteResult:
        if argv[0] == "qsub":
            self.processes.append(subprocess.Popen(
                ["/bin/bash", argv[1]], start_new_session=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            return RemoteResult(0, f"{len(self.processes)}.cluster\n", "")
        if argv[0] == "qstat":
            states = {f"{i}.cluster": "R" if p.poll() is None else "C"
                      for i, p in enumerate(self.processes, start=1)}
            if "-f" in argv:
                ident = argv[-1]
                return RemoteResult(0, f"Job Id: {ident}\n    job_state = {states[ident]}\n", "")
            return RemoteResult(0, "".join(f"{i} example user 00:00:00 {s} compute\n"
                                           for i, s in states.items()), "")
        done = subprocess.run(argv, input=stdin_data, text=True, capture_output=True,
                              check=check, timeout=10)
        return RemoteResult(done.returncode, done.stdout, done.stderr)

    def upload_file(self, source: Path, destination: str) -> None:
        shutil.copyfile(source, destination)

    def download_file(self, source: str, destination: Path) -> None:
        shutil.copyfile(source, destination)

    def wait(self) -> None:
        for process in self.processes:
            process.wait(timeout=30)

    def stop(self) -> None:
        for process in self.processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def test_end_to_end_the_node_runs_its_bound_generation_or_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, daemon: Daemon,
    generations: Generations,  # noqa: F811
) -> None:
    root, a, b, trust = generations
    _deploy(tmp_path, root, b, trust)  # the current selection is B; the jobs name A
    cluster = LocalCluster()
    daemon._scheduler_dispatchers["cluster"] = SchedulerDispatcher(
        TorqueDialect(), cluster, scratch_root=str(tmp_path / "scratch"),  # type: ignore[arg-type]
        gnu_time_command=str(_install_fake_gnu_time(tmp_path, monkeypatch)))
    payload = tmp_path / "payload"
    payload.mkdir()
    (payload / "README").write_text("calculation\n")

    def submit() -> str:
        jobid = submit_module.submit_local(
            host="localhost", directory=str(payload), scheduler_target="cluster",
            command=["sh", "-c", 'cat "$VQ_RUNTIME_PAYLOAD/data/value" > value; '
                     'printf "%s\\n" "$VQ_RUNTIME_ARTIFACT" > artifact'],
            runtime_root=str(root), runtime_artifact=a,
            queue_dir=daemon.queue_dir, jobs_dir=daemon.jobs_dir)
        daemon._dispatch_pending()  # the real gate, then the scheduler route
        assert JobSpec.read(daemon._spec_path(jobid)).state == JobState.RUNNING
        cluster.wait()
        return jobid

    def finish(jobid: str) -> JobSpec:
        deadline = time.monotonic() + 30
        while (spec := JobSpec.read(daemon._spec_path(jobid))).state not in (
                JobState.COMPLETED, JobState.FAILED):
            assert time.monotonic() < deadline, spec.state
            daemon._reconcile_scheduler()
            time.sleep(0.05)
        return spec

    try:
        ran = submit()
        workspace = Path(JobSpec.read(daemon._spec_path(ran)).scheduler_remote_workspace or "")
        assert (workspace / "value").read_text() == "41\n"  # A's, not the current B's 42
        assert (workspace / "artifact").read_text() == f"{a}\n"
        done = finish(ran)
        assert (done.state, done.exit_code) == (JobState.COMPLETED, 0)

        (root / "generations" / a / "payload").chmod(0o775)
        refused = submit()
        workspace = Path(JobSpec.read(daemon._spec_path(refused)).scheduler_remote_workspace
                         or "")
        assert not (workspace / "value").exists()  # the command never started
        stderr = (workspace / "stderr.log").read_text()
        assert stderr == (f"vq: bound runtime generation {a[:12]} unavailable on this node: "
                          "its payload is group or world writable; command not started\n")
        assert '"error": "runtime_generation_unavailable"' in (
            workspace / "_vq" / "resource-usage.json").read_text()
        failed = finish(refused)
        assert (failed.state, failed.exit_code) == (
            JobState.FAILED, RUNTIME_GENERATION_FAILURE_EXIT_CODE)
    finally:
        cluster.stop()
