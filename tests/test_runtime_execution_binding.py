"""A migrated local dispatch must execute the generation its receipt names."""
from __future__ import annotations

import json
import subprocess
import sys
import venv
from pathlib import Path

import pytest

from vq import admin, cli, config, daemon, paths, resubmit, runtime_slots, submit
from vq.spec import JobSpec, JobState, ProgramRuntimePin


@pytest.fixture
def runtimes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "config"))
    repo = tmp_path / "legacy"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.check_output(
            ["git", "-C", str(repo), *args], text=True, stderr=subprocess.PIPE,
        ).strip()

    git("init", "-q", "-b", "main")
    root = tmp_path / "runtime"
    shas = []
    for index in (1, 2):
        (repo / "version.txt").write_text(str(index))
        git("add", "version.txt")
        git("-c", "user.name=Runtime Test", "-c", "user.email=test@example.invalid",
            "commit", "-qm", f"generation {index}")
        sha = git("rev-parse", "HEAD")
        shas.append(sha)
        transaction = str(index) * 32
        runtime_slots.begin_slot_build(root, sha, transaction_id=transaction,
                                       in_use=lambda: set())
        runtime_slots.materialize_source(repo, root, sha)
        python = runtime_slots.slot_python(root, sha)
        venv.EnvBuilder(with_pip=False, symlinks=True).create(python.parent.parent)
        runtime_slots.seal_slot_build(root, sha, transaction_id=transaction)
    runtime_slots.activate(root, shas[0])
    legacy_python = repo / ".venv/bin/python"
    legacy_python.parent.mkdir(parents=True)
    legacy_python.symlink_to(sys.executable)
    program = config.VenvProgram(
        kind="venv",
        python=str(legacy_python), git_dir=str(repo), runtime_slot_root=str(root),
        runtime_slot_execution=True,
    )
    cfg = config.Config(programs={"demo": program})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(daemon, "load_config", lambda: cfg)
    return root, shas, program, cfg


def _spec(tmp_path: Path, sha: str, *, qvf: bool = False) -> JobSpec:
    return JobSpec(
        id="123456abcdef", cwd=str(tmp_path / "workspace"), cpus=1, program="demo",
        command=(["/legacy/python", "-m", "vibeqc._cli", "run", "input.qvf"]
                 if qvf else ["sh", "payload.sh"]),
        qvf_artifact_name="input.qvf" if qvf else None,
        program_runtime_pin=ProgramRuntimePin(expected_git_sha=sha, enforce_git_sha=False),
    )


def test_binding_requires_a_store() -> None:
    with pytest.raises(ValueError, match="requires runtime_slot_root"):
        config.VenvProgram(kind="venv", python="/opt/demo/.venv/bin/python", git_dir="/opt/demo",
                           runtime_slot_execution=True)


def test_execution_selection_keeps_original_build_inputs(runtimes) -> None:
    root, shas, program, _cfg = runtimes
    selected = program.execution_program()
    runtime_slots.activate(root, shas[1])
    assert selected.python == str(runtime_slots.slot_python(root, shas[0]))
    assert selected.git_dir == str(runtime_slots.slot_source(root, shas[0]))
    assert not selected.runtime_slot_execution
    assert selected.runtime_slot_root is None
    assert "releases" not in program.git_dir
    assert program.runtime_slot_execution


def test_program_inventory_reports_one_execution_generation(runtimes) -> None:
    root, shas, program, _cfg = runtimes
    record = cli._program_record("demo", program)
    assert record["status"] == "OK"
    assert record["python"] == str(runtime_slots.slot_python(root, shas[0]))
    assert record["current_git_sha_full"] == shas[0]
    assert record["build_python"] == program.python
    assert submit._program_runtime_pin_for_submit(_cfg, "demo").expected_git_sha == shas[0]


def test_missing_current_never_falls_back_to_legacy(runtimes, tmp_path: Path) -> None:
    root, shas, program, cfg = runtimes
    (root / "current").unlink()
    assert program.availability_status().status == "MISSING"
    record = cli._program_record("demo", program)
    assert record["python"] is None
    with pytest.raises(ValueError, match="no active runtime"):
        submit._program_runtime_pin_for_submit(cfg, "demo")
    result = daemon._program_runtime_pin_dispatch_resolution(_spec(tmp_path, shas[0]))
    assert result.failure and "no active runtime" in result.failure


@pytest.mark.parametrize("qvf", [False, True])
@pytest.mark.parametrize("legacy_pin", [False, True])
def test_dispatch_persists_and_uses_one_generation_across_pointer_flip(
    runtimes, tmp_path: Path, qvf: bool, legacy_pin: bool,
) -> None:
    root, shas, _program, _cfg = runtimes
    spec = _spec(tmp_path, shas[0], qvf=qvf)
    if legacy_pin:
        spec.program_runtime_pin = None
    worker = daemon.Daemon(max_cpus=1)
    spec.write(worker._spec_path(spec.id))
    runtime_slots.activate(root, shas[1])
    selected = daemon._program_runtime_pin_dispatch_resolution(spec)
    assert selected.failure is None
    assert selected.resolved_git_sha == shas[1]
    runtime_slots.activate(root, shas[0])
    assert worker._persist_program_runtime_resolution(spec, selected)
    persisted = JobSpec.read(worker._spec_path(spec.id))
    assert persisted.program_runtime_pin.resolved_git_sha == shas[1]
    env = daemon._program_env(spec)
    assert env["VQ_PROGRAM_PYTHON"] == str(runtime_slots.slot_python(root, shas[1]))
    assert env["VQ_PROGRAM_GIT_DIR"] == str(runtime_slots.slot_source(root, shas[1]))
    if qvf:
        assert spec.command[0] == env["VQ_PROGRAM_PYTHON"]
        assert persisted.command == spec.command
    else:
        assert spec.command == ["sh", "payload.sh"]


def test_fixed_sha_still_refuses_a_different_current(runtimes, tmp_path: Path) -> None:
    root, shas, _program, _cfg = runtimes
    spec = _spec(tmp_path, shas[0])
    spec.program_runtime_pin.enforce_git_sha = True
    runtime_slots.activate(root, shas[1])
    result = daemon._program_runtime_pin_dispatch_resolution(spec)
    assert result.failure and "git_sha expected" in result.failure


def test_real_dispatch_uses_selected_interpreter_after_activation_changes(
    runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, shas, _program, _cfg = runtimes
    spec = _spec(tmp_path, shas[0])
    code = (
        "import json,os,sys; print(json.dumps([sys.executable, "
        "os.environ['VQ_PROGRAM_GIT_SHA'], os.environ['VQ_PROGRAM_GIT_DIR']]))"
    )
    spec.command = ["sh", "-c", 'exec "$VQ_PROGRAM_PYTHON" -c "$1"', "fixture", code]
    worker = daemon.Daemon(max_cpus=1)
    worker.cgroup_enabled = False
    spec.write(worker._spec_path(spec.id))
    runtime_slots.activate(root, shas[1])
    resolve = daemon._program_runtime_pin_dispatch_resolution

    def flip_after_selection(pending):
        selected = resolve(pending)
        runtime_slots.activate(root, shas[0])
        return selected

    monkeypatch.setattr(daemon, "_program_runtime_pin_dispatch_resolution", flip_after_selection)
    assert worker._start_job(spec)
    running = worker._running[spec.id]
    try:
        assert running.popen.wait(timeout=20) == 0
        observed = json.loads((Path(spec.cwd) / spec.stdout_path).read_text())
        assert observed == [str(runtime_slots.slot_python(root, shas[1])), shas[1],
                            str(runtime_slots.slot_source(root, shas[1]))]
    finally:
        if running.popen.poll() is None:
            running.popen.kill()
            running.popen.wait(timeout=20)
        running.close_logs()


def test_raced_command_prevents_persisting_a_selection(runtimes, tmp_path: Path) -> None:
    _root, shas, _program, _cfg = runtimes
    spec = _spec(tmp_path, shas[0], qvf=True)
    worker = daemon.Daemon(max_cpus=1)
    spec.write(worker._spec_path(spec.id))
    selected = daemon._program_runtime_pin_dispatch_resolution(spec)
    raced = spec.model_copy(update={"command": ["sh", "other.sh"]}, deep=True)
    raced.write(worker._spec_path(spec.id))
    assert not worker._persist_program_runtime_resolution(spec, selected)
    assert JobSpec.read(worker._spec_path(spec.id)).command == raced.command


def test_binding_rollback_reselects_legacy_qvf_without_losing_old_slot(
    runtimes, tmp_path: Path,
) -> None:
    root, shas, program, cfg = runtimes
    spec = _spec(tmp_path, shas[0], qvf=True)
    spec.command[0] = str(runtime_slots.slot_python(root, shas[0]))
    cfg.programs["demo"] = program.model_copy(update={"runtime_slot_execution": False})
    selected = daemon._program_runtime_pin_dispatch_resolution(spec)
    assert selected.failure is None
    worker = daemon.Daemon(max_cpus=1)
    spec.write(worker._spec_path(spec.id))
    assert worker._persist_program_runtime_resolution(spec, selected)
    assert spec.command[0] == program.python
    assert spec.program_runtime_pin.resolved_git_sha == shas[1]
    assert runtime_slots.slot_python(root, shas[0]).exists()


def test_reset_resolution_preserves_scheduler_submission_identity() -> None:
    pin = ProgramRuntimePin(
        expected_git_sha="a" * 40, scheduler_host="compute", resolved_executable="/opt/run",
        resolved_git_sha="a" * 40, resolved_runtime_root="/opt/runtime",
        resolved_runtime_source="/opt/source", resolved_runtime_python="/opt/python",
    ).without_resolution()
    assert pin.resolved_executable == "/opt/run"
    assert pin.expected_git_sha == "a" * 40
    assert pin.resolved_git_sha is None
    assert pin.resolved_runtime_python is None


def test_pending_legacy_record_cannot_replay_a_stale_execution_selection(
    runtimes, tmp_path: Path,
) -> None:
    root, shas, program, cfg = runtimes
    cfg.programs["demo"] = program.model_copy(update={"runtime_slot_execution": False})
    spec = _spec(tmp_path, shas[0])
    spec.program_runtime_pin = ProgramRuntimePin(
        enforce_git_sha=False, resolved_git_sha=shas[0],
        resolved_runtime_root=str(root),
        resolved_runtime_source=str(runtime_slots.slot_source(root, shas[0])),
        resolved_runtime_python=str(runtime_slots.slot_python(root, shas[0])),
    )
    worker = daemon.Daemon(max_cpus=1)
    spec.write(worker._spec_path(spec.id))
    selected = daemon._program_runtime_pin_dispatch_resolution(spec)
    assert worker._persist_program_runtime_resolution(spec, selected)
    assert spec.program_runtime_pin.resolved_runtime_python is None
    assert spec.program_runtime_pin.resolved_git_sha is None
    assert daemon._program_env(spec)["VQ_PROGRAM_PYTHON"] == program.python


def test_resubmit_preserves_generated_qvf_and_clears_execution_selection(
    runtimes, tmp_path: Path,
) -> None:
    root, shas, _program, _cfg = runtimes
    spec = _spec(tmp_path, shas[0], qvf=True)
    workspace = Path(spec.cwd)
    workspace.mkdir()
    (workspace / "input.qvf").write_bytes(b"fixture container")
    worker = daemon.Daemon(max_cpus=1)
    spec.write(worker._spec_path(spec.id))
    selected = daemon._program_runtime_pin_dispatch_resolution(spec)
    assert worker._persist_program_runtime_resolution(spec, selected)
    spec.state = JobState.COMPLETED
    spec.write(worker._spec_path(spec.id))
    new_id = resubmit.resubmit_local(spec.id)
    copied = JobSpec.read(worker._spec_path(new_id))
    assert copied.qvf_artifact_name == "input.qvf"
    assert copied.program_runtime_pin.resolved_runtime_python is None
    runtime_slots.activate(root, shas[1])
    selected = daemon._program_runtime_pin_dispatch_resolution(copied)
    assert selected.resolved_git_sha == shas[1]


@pytest.mark.no_autopatch_self_update_probe
def test_serving_slot_is_not_misclassified_as_an_ordinary_update(
    runtimes, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, shas, program, _cfg = runtimes
    monkeypatch.setattr(admin, "_select_daemon_service_manager",
                        lambda: admin._DaemonServiceManager.SYSTEMD)
    from types import SimpleNamespace
    state = SimpleNamespace(
        executable=str(runtime_slots.slot_python(root, shas[0]).parent / "vq"),
        running=True, diagnostic="fixture service",
    )
    monkeypatch.setattr(admin, "_query_daemon_service_state", lambda _manager: state)
    monkeypatch.setattr(admin, "_service_executable_matches",
                        lambda executable, prog, **kw: Path(executable).parent ==
                        Path(prog.python).parent)
    assert admin._detect_vq_self_update(program).is_self_update
