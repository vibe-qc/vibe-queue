"""#161: a daemon-host job bound to a verified portable runtime generation.

`vq submit HOST --runtime-root ROOT [--runtime-artifact ID]` resolves the
current selection (or the named generation) of a `vq runtime-bundle deploy`
target on the execution host, requires its own verification, and records it
on the job. The daemon verifies that generation again before dispatch, runs
the job with it (VQ_RUNTIME_PAYLOAD / VQ_RUNTIME_ARTIFACT) and never with
another: a selection that moved, or rolled back, after submission affects only
later jobs. A generation that is gone fails the job instead. Unset, nothing
changes.
"""
from __future__ import annotations

import json
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths, resubmit, status
from vq import runtime_bundle as rb
from vq import submit as submit_module
from vq.cli import main
from vq.daemon import Daemon, _vq_job_env
from vq.spec import JobSpec, JobState, RuntimeBundlePin
from vq.web import calculations

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle import _description, _profile, _trust  # noqa: E402
from test_submit_plan_contract import PlanHarness, plan_harness  # noqa: E402, F401

PROBE = "#!/bin/sh\ntest -f data/value\n"


def _bundle(tmp_path: Path, value: str, dependency: str) -> rb.BundleManifest:
    payload = tmp_path / f"built-{value}"
    (payload / "bin").mkdir(parents=True)
    (payload / "bin/probe").write_text(PROBE)
    (payload / "bin/probe").chmod(0o755)
    (payload / "data").mkdir()
    (payload / "data/value").write_text(f"{value}\n")
    return rb.pack(payload, _description(tmp_path, dependency), tmp_path / f"bundle-{value}")


@pytest.fixture
def generations(tmp_path: Path) -> tuple[Path, str, str, Path]:
    """Two published generations, A (value 41) and B (value 42); only A deployed."""
    a, b = _bundle(tmp_path, "41", "b"), _bundle(tmp_path, "42", "d")
    trust = _trust(tmp_path, a, b)
    for value in ("41", "42"):
        rb.publish(tmp_path / f"bundle-{value}", tmp_path / "store", trust)
    root = tmp_path / "runtime"
    rb.deploy(tmp_path / "store", a.artifact_id, root, trust, _profile())
    return root, a.artifact_id, b.artifact_id, trust


def _deploy(tmp_path: Path, root: Path, artifact: str, trust: Path) -> None:
    rb.deploy(tmp_path / "store", artifact, root, trust, _profile())


class TestVerifiedGeneration:
    def test_the_current_selection_or_a_named_generation(
        self, tmp_path: Path, generations: tuple[Path, str, str, Path],
    ) -> None:
        root, a, b, trust = generations
        assert rb.verified_generation(root) == (a, root / "generations" / a / "payload")
        _deploy(tmp_path, root, b, trust)
        assert rb.verified_generation(root)[0] == b
        assert rb.verified_generation(root, a)[0] == a  # still retained and verified

    def test_refusals(self, tmp_path: Path, generations: tuple[Path, str, str, Path]) -> None:
        root, a, b, _trust_file = generations
        with pytest.raises(rb.BundleError, match=f"runtime {b[:12]} is not a verified deployment"):
            rb.verified_generation(root, b)  # published, never deployed
        with pytest.raises(rb.BundleError, match="full SHA256"):
            rb.verified_generation(root, "abc")
        receipt = root / "generations" / a / "verification.json"
        receipt.write_text(json.dumps({**json.loads(receipt.read_text()), "verified": False}))
        with pytest.raises(rb.BundleError, match="not a verified deployment"):
            rb.verified_generation(root)
        empty = tmp_path / "empty"
        empty.mkdir()
        with pytest.raises(rb.BundleError, match="has no selected runtime"):
            rb.verified_generation(empty)

    def test_a_writable_payload_is_not_trusted(
        self, generations: tuple[Path, str, str, Path],
    ) -> None:
        root, a, _b, _trust_file = generations
        (root / "generations" / a / "payload").chmod(0o775)
        with pytest.raises(rb.BundleError, match="not a verified deployment"):
            rb.verified_generation(root, a)

    def test_a_refusal_never_echoes_file_contents(
        self, generations: tuple[Path, str, str, Path],
    ) -> None:
        root, a, _b, _trust_file = generations
        (root / "generations" / a / "manifest.json").write_text('{"secret": "do-not-echo"}')
        with pytest.raises(rb.BundleError) as refused:
            rb.verified_generation(root, a)
        assert "do-not-echo" not in str(refused.value)


class TestSubmit:
    def test_the_job_records_the_generation(
        self, tmp_path: Path, generations: tuple[Path, str, str, Path],
    ) -> None:
        root, a, b, trust = generations
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        queue, jobs = tmp_path / "queue", tmp_path / "jobs"
        current = submit_module.submit_local(host="localhost", input_file=str(source),
                                             runtime_root=str(root), queue_dir=queue,
                                             jobs_dir=jobs)
        assert JobSpec.read(queue / f"{current}.json").runtime_bundle == RuntimeBundlePin(
            root=str(root.resolve()), artifact_id=a)
        _deploy(tmp_path, root, b, trust)
        named = submit_module.submit_local(host="localhost", input_file=str(source),
                                           runtime_root=str(root), runtime_artifact=a,
                                           queue_dir=queue, jobs_dir=jobs)
        assert JobSpec.read(queue / f"{named}.json").runtime_bundle.artifact_id == a  # type: ignore[union-attr]

    def test_an_unverified_root_refuses_before_writing_anything(self, tmp_path: Path) -> None:
        source = tmp_path / "run.py"
        source.write_text("pass\n")
        (tmp_path / "nothing").mkdir()
        with pytest.raises(ValueError, match="--runtime-root: .* has no selected runtime"):
            submit_module.submit_local(host="localhost", input_file=str(source),
                                       runtime_root=str(tmp_path / "nothing"),
                                       queue_dir=tmp_path / "queue", jobs_dir=tmp_path / "jobs")
        assert not (tmp_path / "queue").exists() or not list((tmp_path / "queue").iterdir())

    @pytest.mark.parametrize(("root", "artifact", "message"), [
        (None, "a" * 64, "--runtime-artifact requires --runtime-root"),
        ("relative/root", None, "must be an absolute path"),
        ("/abs", "short", "full SHA256 identity"),
    ])
    def test_shapes_are_checked_before_any_host(
        self, root: str | None, artifact: str | None, message: str,
    ) -> None:
        with pytest.raises(ValueError, match=message):
            submit_module._validate_runtime_bundle_request(root, artifact, scheduler_target=None)

    # A scheduler target's binding: tests/test_runtime_binding_scheduler_161.py.

    def test_the_plan_and_wire_carry_it_only_when_set(
        self, plan_harness: PlanHarness,  # noqa: F811
    ) -> None:
        source = plan_harness.root / "plain.py"
        source.write_text("pass\n", encoding="utf-8")
        result = CliRunner().invoke(main, ["submit", "--host", "localhost", "--json",
                                           str(source)])
        assert result.exit_code == 0, result.output
        assert "runtime_root" not in plan_harness.calls[0][1]
        common = dict(
            remote_tar="/tmp/x.tar", remote_input_file=None, remote_command=["true"], cpus=1,
            scheduler_tasks=None, mem_mb=None, wall_time_seconds=None, priority=0,
            auto_resume=False, retry=0, job_name=None, branch=None, program=None,
            expected_sha=None, tags=None, not_before=None, depends_on=None, depends_on_any=None,
            clean_workdir_on_terminal=False, array=1, chain=1, rerun_until_file_exists=None,
            rerun_max=10, refresh_before=None, scheduler_target=None, qvf_force=False)
        bound = submit_module._build_remote_submit_argv(
            **common, runtime_root="/opt/rt", runtime_artifact="a" * 64)
        assert bound[bound.index("--runtime-root") + 1] == "/opt/rt"
        assert bound[bound.index("--runtime-artifact") + 1] == "a" * 64
        assert "--runtime-root" not in submit_module._build_remote_submit_argv(**common)

    def test_it_is_part_of_the_keyed_intent_only_when_bound(self) -> None:
        def digest(bundle: RuntimeBundlePin | None) -> str:
            spec = JobSpec(id="abc", command=["true"], cwd="/ws", cpus=4,
                           scheduler_target="host_f", runtime_bundle=bundle)
            return submit_module._submission_intent_digest(
                spec, payload_kind="file", payload_digest="0" * 64, vibeqc_preflight=False)

        unbound = digest(None)
        # The pre-field hash (see tests/test_gpus_168.py) is unchanged.
        assert unbound == "eda89c1ba8023e2d7cb89df52737afc21c239b80870e248be42a074a7cff2db8"
        assert digest(RuntimeBundlePin(root="/rt", artifact_id="a" * 64)) != unbound


# --- a real daemon ---------------------------------------------------------------


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


def _run(daemon: Daemon) -> None:
    daemon._dispatch_pending()
    deadline = time.monotonic() + 30
    while daemon._running:
        assert time.monotonic() < deadline, "jobs did not finish"
        daemon._reconcile_running()
        time.sleep(0.05)


def _submit(tmp_path: Path, root: Path, name: str, artifact: str | None = None) -> str:
    payload = tmp_path / f"payload-{name}"
    payload.mkdir()
    return submit_module.submit_local(
        host="localhost", directory=str(payload),
        command=["sh", "-c", 'cat "$VQ_RUNTIME_PAYLOAD/data/value" > seen; '
                             'echo "$VQ_RUNTIME_ARTIFACT" >> seen'],
        runtime_root=str(root), runtime_artifact=artifact)


def test_a_queued_job_keeps_its_generation_when_the_selection_moves(
    tmp_path: Path, daemon: Daemon, generations: tuple[Path, str, str, Path],
) -> None:
    root, a, b, trust = generations
    bound_to_a = _submit(tmp_path, root, "first")
    # The selection moves to B while the job is still queued.
    _deploy(tmp_path, root, b, trust)
    bound_to_b = _submit(tmp_path, root, "second")
    _run(daemon)
    first = JobSpec.read(daemon._spec_path(bound_to_a))
    second = JobSpec.read(daemon._spec_path(bound_to_b))
    assert (first.state, second.state) == (JobState.COMPLETED, JobState.COMPLETED)
    assert (Path(first.cwd) / "seen").read_text() == f"41\n{a}\n"
    assert (Path(second.cwd) / "seen").read_text() == f"42\n{b}\n"


def test_a_generation_that_is_no_longer_verified_fails_the_job(
    tmp_path: Path, daemon: Daemon, generations: tuple[Path, str, str, Path],
) -> None:
    root, a, _b, _trust_file = generations
    jobid = _submit(tmp_path, root, "doomed")
    receipt = root / "generations" / a / "verification.json"
    receipt.write_text(json.dumps({**json.loads(receipt.read_text()), "verified": False}))
    _run(daemon)
    spec = JobSpec.read(daemon._spec_path(jobid))
    assert spec.state == JobState.FAILED
    assert "bound runtime generation unavailable before dispatch" in (spec.failure_reason or "")
    assert not (Path(spec.cwd) / "seen").exists()


def test_the_cli_status_resubmit_and_console(
    tmp_path: Path, daemon: Daemon, generations: tuple[Path, str, str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, a, _b, _trust_file = generations
    payload = tmp_path / "cli-payload"
    payload.mkdir()
    result = CliRunner().invoke(main, ["submit", "localhost", "--runtime-root", str(root),
                                       "-d", str(payload), "--", "true"])
    assert result.exit_code == 0, result.output
    jobid = result.output.strip().splitlines()[-1]
    spec = JobSpec.read(daemon._spec_path(jobid))
    assert spec.runtime_bundle == RuntimeBundlePin(root=str(root.resolve()), artifact_id=a)
    env = _vq_job_env(spec, workdir="/w")
    assert env["VQ_RUNTIME_ARTIFACT"] == a
    assert env["VQ_RUNTIME_PAYLOAD"] == str(root.resolve() / "generations" / a / "payload")
    monkeypatch.setattr(status.rpc, "request_scheduler_status_refresh", lambda *a, **k: None)
    assert f"runtime:      generation {a[:12]} of {root.resolve()}" in status.show_status(
        "localhost", jobid)
    _run(daemon)
    again = resubmit.resubmit_local(jobid)
    assert JobSpec.read(daemon._spec_path(again)).runtime_bundle == spec.runtime_bundle
    row = json.loads(spec.model_dump_json())
    assert calculations.software_view(row) == f"runtime {a[:12]}"
    assert calculations.software_view({**row, "program": "vibe-qc"}) == (
        f"vibe-qc · runtime {a[:12]}")
    assert "VQ_RUNTIME_ARTIFACT" not in _vq_job_env(
        JobSpec(id="x", command=["true"], cwd="/w", cpus=1), workdir="/w")


class TestDispatchGate:
    def test_it_checks_the_pinned_generation_not_the_current_one(
        self, tmp_path: Path, generations: tuple[Path, str, str, Path],
    ) -> None:
        from vq.daemon import _runtime_bundle_dispatch_failure

        root, a, b, trust = generations
        _deploy(tmp_path, root, b, trust)  # B is current and fine
        pinned = JobSpec(id="x", command=["true"], cwd="/w", cpus=1,
                         runtime_bundle=RuntimeBundlePin(root=str(root), artifact_id=a))
        assert _runtime_bundle_dispatch_failure(pinned) is None
        receipt = root / "generations" / a / "verification.json"
        receipt.write_text(json.dumps({**json.loads(receipt.read_text()), "verified": False}))
        failure = _runtime_bundle_dispatch_failure(pinned)
        assert failure is not None and a[:12] in failure

    def test_an_unbound_job_passes(self) -> None:
        from vq.daemon import _runtime_bundle_dispatch_failure

        # A bound scheduler job is checked on its node instead, see
        # tests/test_runtime_binding_scheduler_161.py.
        assert _runtime_bundle_dispatch_failure(
            JobSpec(id="x", command=["true"], cwd="/w", cpus=1)) is None
