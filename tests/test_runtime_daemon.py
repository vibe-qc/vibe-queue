"""Serving runtime generations require exact reversible manager bindings."""
from __future__ import annotations

import contextlib
import copy
import datetime
import json
import plistlib
import tomllib
from pathlib import Path

import pytest

from tests.test_runtime_execution_binding import runtimes as runtimes
from vq import admin, config, runtime_daemon, runtime_slots

pytestmark = pytest.mark.no_autopatch_branch_check


@pytest.fixture
def service_programs(
    runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest,
):
    root, shas, program, _cfg = runtimes
    registered = program.model_copy(update={"runtime_slot_execution": False})
    old = registered.model_copy(update={"runtime_slot_root": None})
    if getattr(request, "param", "legacy") == "current":
        registered = registered.model_copy(update={
            "runtime_slot_execution": True, "runtime_slot_build_input": "current",
        })
        old = runtime_slots.slot_local_program(registered, root, shas[0])
        Path(registered.git_dir).rename(tmp_path / "retired-legacy")
    target = runtime_slots.slot_local_program(registered, root, shas[1])
    directory = tmp_path / "registry"
    directory.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(directory))
    raw = {"programs": {"queue": registered.model_dump(exclude_none=True)}}
    config.config_path().write_bytes(runtime_daemon._toml_bytes(raw))
    return registered, old, target


def _unit(old) -> bytes:
    executable = str(Path(old.python).parent / "vq")
    return (
        "[Unit]\nDescription=Retained daemon\n[Service]\n"
        f'ExecStart={json.dumps(executable)} daemon run --cpus 2\n'
        'Environment="VQ_STATE_DIR=/opt/example/state"\nRestart=always\n'
        "[Install]\nWantedBy=default.target\n"
    ).encode()


def _plist(old, *, binary=False) -> bytes:
    return plistlib.dumps({
        "Label": admin.LAUNCHD_DAEMON_LABEL,
        "ProgramArguments": [old.python, "-m", "vq", "daemon", "run", "--cpus", "2"],
        "EnvironmentVariables": {"VQ_STATE_DIR": "/opt/example/state"},
        "KeepAlive": True,
    }, fmt=plistlib.FMT_BINARY if binary else plistlib.FMT_XML)


def test_toml_candidate_preserves_supported_values():
    raw = {"text": "quoted \"value\"\n雪", "bool": True, "int": 4, "float": 1.25,
           "date": datetime.date(2026, 9, 22), "time": datetime.time(4, 3, 2),
           "datetime": datetime.datetime(2026, 9, 22, 4, 3, 2, tzinfo=datetime.UTC),
           "nested": {"array": [1, "two"], "records": [{"x": "a"}, {"x": "b"}]}}
    assert tomllib.loads(runtime_daemon._toml_bytes(raw).decode()) == raw


def test_registry_changes_only_execution_flag(service_programs):
    registered, _old, _target = service_programs
    before = tomllib.loads(config.config_path().read_text())
    binding = runtime_daemon.registry_binding("queue", registered)
    data = runtime_daemon.runtime_migration._bytes(binding["after"])
    after = tomllib.loads(data.decode())
    expected = copy.deepcopy(before)
    expected["programs"]["queue"]["runtime_slot_execution"] = True
    assert after == expected
    assert tomllib.loads(config.config_path().read_text()) == before


@pytest.mark.parametrize("binary", [False, True])
def test_launchd_rebind_preserves_environment_and_arguments(service_programs, binary):
    _registered, old, target = service_programs
    data = _plist(old, binary=binary)
    updated, command = runtime_daemon.service_bytes(
        data, admin._DaemonServiceManager.LAUNCHD, old, target,
    )
    expected = plistlib.loads(data)
    expected["ProgramArguments"][0] = target.python
    assert plistlib.loads(updated) == expected
    assert command == tuple(expected["ProgramArguments"])
    assert updated.startswith(b"bplist") == binary


def test_systemd_rebind_preserves_state_and_only_changes_executable(service_programs):
    _registered, old, target = service_programs
    data = _unit(old)
    updated, command = runtime_daemon.service_bytes(
        data, admin._DaemonServiceManager.SYSTEMD, old, target,
    )
    assert b'Environment="VQ_STATE_DIR=/opt/example/state"' in updated
    assert b"Restart=always" in updated
    assert admin._systemd_command_identity_is_vq_daemon(command)
    normalized = admin._normalize_systemd_command_identity(command)
    assert normalized[0] == str(Path(target.python).parent / "vq")
    assert normalized[1].endswith("daemon run --cpus 2")
    assert Path(old.git_dir).as_posix().encode() not in updated


@pytest.mark.parametrize("change", ["multiple", "expansion", "wrong-executable"])
def test_systemd_ambiguous_unit_refuses(service_programs, change):
    _registered, old, target = service_programs
    data = _unit(old)
    if change == "multiple":
        data = data.replace(b"ExecStart=", b"ExecStart=\nExecStart=")
    elif change == "expansion":
        data = data.replace(b"--cpus 2", b"--cpus $LIMIT")
    else:
        data = data.replace(str(Path(old.python).parent / "vq").encode(), b"/opt/other/vq")
    with pytest.raises(admin.AdminError):
        runtime_daemon.service_bytes(data, admin._DaemonServiceManager.SYSTEMD, old, target)


def test_launchd_disagreeing_program_refuses(service_programs):
    _registered, old, target = service_programs
    data = plistlib.loads(_plist(old))
    data["Program"] = "/opt/other/python"
    with pytest.raises(admin.AdminError, match="Program differs"):
        runtime_daemon.service_bytes(plistlib.dumps(data), admin._DaemonServiceManager.LAUNCHD,
                                     old, target)


@pytest.mark.parametrize("manager", list(admin._DaemonServiceManager))
def test_staged_service_registry_and_pointer_bindings_restore_exactly(
    service_programs, runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, manager,
):
    registered, old, target = service_programs
    root, shas, _program, _cfg = runtimes
    service = tmp_path / "service"
    service.write_bytes(
        _unit(old) if manager is admin._DaemonServiceManager.SYSTEMD else _plist(old)
    )
    service.chmod(0o600)
    monkeypatch.setattr(runtime_daemon, "service_path", lambda _manager: service)
    context = runtime_daemon.binding_plan("queue", registered, old, target, manager, shas[1])
    originals = [config.config_path().read_bytes(), service.read_bytes()]
    lifecycle = admin._ManagedDaemonUpdate(
        manager=manager, env="queue", pre_pid=123, was_running=True, was_stopped=False,
        pre_source_sha=shas[0], pre_source_tree_sha256="a" * 64, pre_checkout_branch="main",
        venv_path=Path(old.python).parent.parent, venv_backup=None,
        service_executable=old.python, transaction_id="a" * 32, slot_transaction=context,
    )
    receipts = []
    monkeypatch.setattr(admin, "_persist_managed_update_receipt",
                        lambda _prog, life: receipts.append(copy.deepcopy(life.slot_transaction)))
    runtime_daemon.stage_bindings(registered, lifecycle)
    assert [config.config_path().read_bytes(), service.read_bytes()] == originals
    assert receipts[-1]["staged"]
    runtime_daemon.replace_bindings(registered, lifecycle, rollback=False)
    assert runtime_slots.resolve_current(root) == shas[1]
    assert service.stat().st_mode & 0o777 == 0o600
    runtime_daemon.replace_bindings(registered, lifecycle, rollback=False)
    runtime_daemon.replace_bindings(registered, lifecycle, rollback=True)
    assert [config.config_path().read_bytes(), service.read_bytes()] == originals
    assert runtime_slots.resolve_current(root) == shas[0]
    runtime_daemon.replace_bindings(registered, lifecycle, rollback=True)


@pytest.fixture
def manager_sim(service_programs, runtimes, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                request: pytest.FixtureRequest):
    from types import SimpleNamespace

    registered, old, target = service_programs
    manager = request.param
    service = tmp_path / "daemon-service"
    service.write_bytes(
        _unit(old) if manager is admin._DaemonServiceManager.SYSTEMD else _plist(old)
    )
    if registered.runtime_slot_build_input == "legacy":
        (Path(old.python).parent.parent / "pyvenv.cfg").write_text("home = /example\n")
    monkeypatch.setattr(runtime_daemon, "service_path", lambda _manager: service)
    monkeypatch.setattr(admin, "_vq_project_root_for_program", lambda prog: Path(prog.git_dir))
    monkeypatch.setattr(admin, "source_tree_sha256_at_git_commit", lambda *_: "a" * 64)
    monkeypatch.setattr(admin, "_installed_tree_digest", lambda _: "a" * 64)
    monkeypatch.setattr(admin, "_run_git_status_porcelain", lambda _: (0, ""))
    monkeypatch.setattr(admin, "_managed_update_script_args", lambda *_: ["--extras", "core"])
    monkeypatch.setattr(admin, "pause_token_scope_with_proof", lambda *a, **k: SimpleNamespace(
        summary="sandbox scope paused", require_quiescent=lambda: None,
    ))
    resumed = []
    monkeypatch.setattr(admin, "resume_token_scope_with_proof", lambda *a, **k: (
        resumed.append(a) or SimpleNamespace(
            summary="sandbox scope resumed", require_clear=lambda: None,
        )
    ))
    state = {"loaded": True, "running": True, "pid": 100, "fail_target": False,
             "command": runtime_daemon.service_bytes(service.read_bytes(), manager, old, old)[1]}
    commands = []
    checks = []

    def executable():
        if manager is admin._DaemonServiceManager.SYSTEMD:
            return admin._normalize_systemd_command_identity(state["command"])[0]
        return state["command"][0]

    def load_definition():
        data = service.read_bytes()
        for candidate in (old, target):
            try:
                _data, command = runtime_daemon.service_bytes(data, manager, candidate, candidate)
            except admin.AdminError:
                continue
            state["command"] = command
            return
        raise AssertionError("simulator received an unauthorized daemon definition")

    def query(_manager):
        return admin._DaemonServiceState(
            manager=manager, running=state["running"],
            pid=state["pid"] if state["running"] else None,
            executable=executable() if state["loaded"] else None,
            diagnostic="sandbox manager state",
            command_identity=(state["command"] if state["loaded"] else
                              tuple(plistlib.loads(service.read_bytes())["ProgramArguments"])),
        )

    def command(argv, *, display):
        commands.append(list(argv))
        if "stop" in argv or "bootout" in argv:
            state["running"] = False
            if "bootout" in argv:
                state["loaded"] = False
        elif "daemon-reload" in argv or "bootstrap" in argv:
            load_definition()
            state["loaded"] = True
        elif "start" in argv or "kickstart" in argv:
            state["running"] = True
            state["pid"] += 1
        return True, display

    def verify(sha, **kwargs):
        checks.append((sha, kwargs, executable()))
        is_target = Path(executable()).parent == Path(target.python).parent
        return admin.DaemonProvenance(
            verified=not (state["fail_target"] and is_target), actual_sha=sha,
            actual_tree_sha256="a" * 64, detail="sandbox exact RPC proof",
        )

    def prepare(*_args, **kwargs):
        assert state["running"] and Path(executable()).parent == Path(old.python).parent
        return admin.UpdateResult(
            env="queue", git_dir=target.git_dir, branch=None, update_script=None,
            git_pull_rc=0, expected_sha=kwargs["expected_sha"], actual_sha=kwargs["expected_sha"],
            sha_check_rc=0,
        )

    monkeypatch.setattr(admin, "_query_daemon_service_state", query)
    monkeypatch.setattr(admin, "_run_daemon_service_command", command)
    monkeypatch.setattr(admin, "_verify_restarted_daemon", verify)
    monkeypatch.setattr(runtime_daemon, "prepare_generation", prepare)
    if manager is admin._DaemonServiceManager.LAUNCHD:
        monkeypatch.setattr(admin, "_launchd_plist_argv",
                            lambda: plistlib.loads(service.read_bytes())["ProgramArguments"])
        monkeypatch.setattr(admin, "_query_launchd_daemon", lambda: (
            (0, f'pid = {state["pid"]}\nprogram = {executable()}') if state["loaded"]
            else (3, "not found")
        ))
    probe = admin._SelfUpdateProbe(
        is_self_update=True, daemon_running=True, service_manager=manager.value,
        manager_available=True, diagnostic="sandbox owned service",
    )
    sim = SimpleNamespace(
        registered=registered, old=old, target=target, manager=manager, service=service,
        state=state, commands=commands, checks=checks, resumed=resumed, probe=probe,
        cfg=config.Config(programs={"queue": registered}),
    )
    yield sim
    admin._set_owned_admin_update_marker_path(None)


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
@pytest.mark.parametrize("failed_provenance", [False, True])
@pytest.mark.parametrize("service_programs", ["legacy", "current"], indirect=True)
def test_managed_slot_update_proves_target_or_restores_old_daemon(
    manager_sim, runtimes, failed_provenance,
):
    sim = manager_sim
    root, shas, _program, _cfg = runtimes
    old_registry = config.config_path().read_bytes()
    old_service = sim.service.read_bytes()
    sim.state["fail_target"] = failed_provenance
    result = runtime_daemon.update(
        "queue", sim.cfg, sim.registered, sim.probe, host="localhost",
        expected_sha=shas[1], expected_tag=None, supplied=None,
        restart_daemon=True, admin_token=None, force=False,
    )
    assert result.success is not failed_provenance, result.work_errors
    assert sim.state["running"], (result.daemon_restart_message, result.work_errors)
    assert sim.resumed
    assert sim.checks and all(check[1]["require_exact_identity"] for check in sim.checks)
    assert Path(sim.old.python).exists()
    assert runtime_slots.slot_python(root, shas[0]).exists()
    assert runtime_slots.slot_python(root, shas[1]).exists()
    if failed_provenance:
        assert config.config_path().read_bytes() == old_registry
        assert sim.service.read_bytes() == old_service
        assert runtime_slots.resolve_current(root) == shas[0]
        marker = admin.read_admin_update_marker()
        assert marker.managed_transaction["schema"] == runtime_daemon.SCHEMA
        assert marker.managed_transaction["phase"] == "old_restored"
        _prog, lifecycle = admin._parse_managed_update_receipt(marker, sim.cfg)
        assert lifecycle.slot_transaction is not None
    else:
        assert runtime_slots.resolve_current(root) == shas[1]
        assert tomllib.loads(config.config_path().read_text())["programs"]["queue"][
            "runtime_slot_execution"
        ]
        assert admin.read_admin_update_marker() is None


def _update(sim, sha):
    return runtime_daemon.update(
        "queue", sim.cfg, sim.registered, sim.probe, host="localhost",
        expected_sha=sha, expected_tag=None, supplied=None,
        restart_daemon=True, admin_token=None, force=False,
    )


def _stale_marker(monkeypatch):
    marker = admin.read_admin_update_marker()
    assert marker is not None
    marker.pid = 999_999
    marker.pid_start_time = 0
    admin._write_admin_update_marker_atomic(marker)
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(admin, "_pid_liveness", lambda _: False)
    return marker


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
@pytest.mark.parametrize("boundary", [
    "armed", "staged", "current", "registry", "service", "slot_activated",
    "target_ready", "target_verified", "target_committed",
])
@pytest.mark.parametrize("service_programs", ["legacy", "current"], indirect=True)
def test_interrupted_slot_transaction_recovers_only_durable_terminal_identity(
    manager_sim, runtimes, monkeypatch, boundary,
):
    sim = manager_sim
    root, shas, *_ = runtimes
    original = (config.config_path().read_bytes(), sim.service.read_bytes())
    persist = admin._persist_managed_update_receipt
    recover = admin._recover_managed_daemon_after_exception
    interrupted = []

    def checkpoint(prog, lifecycle):
        persist(prog, lifecycle)
        if interrupted:
            return
        context = lifecycle.slot_transaction
        reached = lifecycle.receipt_phase == boundary
        if boundary == "staged":
            reached = context["staged"]
        if boundary in {"current", "registry", "service"}:
            path = (root / "current" if boundary == "current" else
                    Path(context[boundary]["before"]["path"]))
            action = next(a for a in context["actions"] if a["before"]["path"] == str(path))
            reached = (context["staged"] and runtime_daemon.runtime_migration._snapshot(path)[
                "entry"
            ] == action.get("prepared_after"))
        if reached:
            interrupted.append(lifecycle.receipt_phase)
            raise RuntimeError("sandbox interruption at durable checkpoint")

    monkeypatch.setattr(admin, "_persist_managed_update_receipt", checkpoint)
    monkeypatch.setattr(admin, "_recover_managed_daemon_after_exception",
                        lambda *a, **k: (False, "sandbox simulates updater exit"))
    with contextlib.suppress(RuntimeError, admin.AdminError):
        _update(sim, shas[1])
    assert interrupted, boundary
    marker = _stale_marker(monkeypatch)
    monkeypatch.setattr(admin, "_persist_managed_update_receipt", persist)
    monkeypatch.setattr(admin, "_recover_managed_daemon_after_exception", recover)
    _raw, current_cfg = runtime_daemon.runtime_migration._model(
        config.config_path().read_bytes(), str(config.config_path()),
    )
    recovered = admin.recover_managed_update(current_cfg, marker_id=marker.marker_id)
    assert recovered.recovered, recovered.detail
    assert sim.state["running"]
    assert admin.read_admin_update_marker() is None
    if boundary == "target_committed":
        assert runtime_slots.resolve_current(root) == shas[1]
        assert current_cfg.programs["queue"].runtime_slot_execution
    else:
        assert runtime_slots.resolve_current(root) == shas[0]
        assert (config.config_path().read_bytes(), sim.service.read_bytes()) == original
    assert Path(sim.old.python).exists()
    assert all(runtime_slots.slot_python(root, sha).exists() for sha in shas)


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
@pytest.mark.parametrize("damage", ["unknown-field", "old-executable", "process", "action-path",
                                    "stage-path", "config-edit", "terminal-state"])
@pytest.mark.parametrize("service_programs", ["legacy", "current"], indirect=True)
def test_recovery_refuses_untrusted_receipt_or_binding_before_service_mutation(
    manager_sim, runtimes, monkeypatch, damage,
):
    sim = manager_sim
    sim.state["fail_target"] = True
    assert not _update(sim, runtimes[1][1]).success
    marker = _stale_marker(monkeypatch)
    raw = marker.managed_transaction
    context = raw["slot_transaction"]
    if damage == "unknown-field":
        context["extra"] = True
    elif damage == "old-executable":
        context["old_service_executable"] = "/opt/foreign/python"
    elif damage == "process":
        raw["pre_pid"] = True
    elif damage == "action-path":
        context["actions"][0]["before"]["path"] = "/opt/foreign/current"
    elif damage == "stage-path":
        context["actions"][0]["rollback_stage"] = "../foreign"
    elif damage == "config-edit":
        with config.config_path().open("ab") as stream:
            stream.write(b"# operator edit\n")
    elif damage == "terminal-state":
        raw["phase"] = "target_committed"
    admin._write_admin_update_marker_atomic(marker)
    commands = list(sim.commands)
    data = config.config_path().read_bytes()
    with pytest.raises(admin.AdminError):
        admin.recover_managed_update(sim.cfg, marker_id=marker.marker_id)
    assert sim.commands == commands
    assert config.config_path().read_bytes() == data
    assert admin.read_admin_update_marker().managed_transaction == raw


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
@pytest.mark.parametrize("service_programs", ["legacy", "current"], indirect=True)
@pytest.mark.no_autopatch_self_update_probe
def test_public_admin_and_first_class_resolver_select_slot_transaction(
    manager_sim, runtimes, monkeypatch,
):
    sim = manager_sim
    monkeypatch.setattr(admin, "_select_daemon_service_manager", lambda: sim.manager)
    assert admin.resolve_vq_self_update_target(sim.cfg)[:2] == ("queue", sim.registered)
    result = admin.update_env("queue", sim.cfg, host="localhost", expected_sha=runtimes[1][1],
                              require_self_update=True)
    assert result.success, result.work_errors
    assert runtime_slots.resolve_current(runtimes[0]) == runtimes[1][1]


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
@pytest.mark.parametrize("refusal", ["no-restart", "force", "multi-user", "post-hook", "unknown"])
def test_slot_admission_refuses_before_preparation_or_pause(
    manager_sim, runtimes, monkeypatch, refusal,
):
    sim = manager_sim
    monkeypatch.setattr(runtime_daemon, "prepare_generation",
                        lambda *a, **k: pytest.fail("refused update prepared a generation"))
    options = {"restart_daemon": True, "force": False}
    if refusal == "no-restart":
        options["restart_daemon"] = False
    elif refusal == "force":
        options["force"] = True
    elif refusal == "multi-user":
        monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    elif refusal == "post-hook":
        sim.registered = sim.registered.model_copy(update={"post_update_script": "post.sh"})
    elif refusal == "unknown":
        from dataclasses import replace
        sim.probe = replace(sim.probe, manager_available=False)
    before = config.config_path().read_bytes(), sim.service.read_bytes()
    with pytest.raises(admin.AdminError):
        runtime_daemon.update(
            "queue", sim.cfg, sim.registered, sim.probe, host="localhost",
            expected_sha=runtimes[1][1], expected_tag=None, supplied=None,
            admin_token=None, **options,
        )
    assert not sim.commands and not sim.resumed
    assert admin.read_admin_update_marker() is None
    assert (config.config_path().read_bytes(), sim.service.read_bytes()) == before


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
def test_same_serving_generation_requires_exact_rpc_without_restarting(
    manager_sim, runtimes, monkeypatch,
):
    sim = manager_sim
    sha = runtimes[1][1]
    assert _update(sim, sha).success
    _raw, sim.cfg = runtime_daemon.runtime_migration._model(
        config.config_path().read_bytes(), str(config.config_path()),
    )
    sim.registered = sim.cfg.programs["queue"]
    monkeypatch.setattr(runtime_daemon, "prepare_generation", lambda *a, **k: admin.UpdateResult(
        env="queue", git_dir=sim.target.git_dir, branch=None, update_script=None,
        git_pull_rc=0, expected_sha=sha, actual_sha=sha, sha_check_rc=0,
    ))
    commands = list(sim.commands)
    original = config.config_path().read_bytes(), sim.service.read_bytes()
    result = _update(sim, sha)
    assert result.success and result.already_current and result.daemon_health_verified
    assert sim.commands == commands
    assert sim.checks[-1][1]["wait_for_identity_change"] is False
    assert (config.config_path().read_bytes(), sim.service.read_bytes()) == original
    assert admin.read_admin_update_marker() is None


@pytest.mark.no_autopatch_lifecycle_lock
@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
def test_public_slot_update_refuses_concurrent_target_generation_owner(
    manager_sim, runtimes, monkeypatch,
):
    import threading

    sim = manager_sim
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _: sim.probe)
    failures = []

    def attempt():
        try:
            admin.update_env("queue", sim.cfg, host="localhost", expected_sha=runtimes[1][1])
        except admin.AdminError as exc:
            failures.append(str(exc))

    with admin.toolset_lifecycle_lock([sim.target], action="sandbox other updater"):
        worker = threading.Thread(target=attempt)
        worker.start()
        worker.join(timeout=10)
        assert not worker.is_alive()
        assert failures == [
            f"another toolset lifecycle operation owns checkout {sim.target.git_dir}",
        ]
    assert not sim.commands
    assert admin.read_admin_update_marker() is None


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
def test_loaded_command_must_match_effective_unit_before_build(manager_sim, runtimes, monkeypatch):
    sim = manager_sim
    sim.state["command"] = tuple(x.replace("--cpus 2", "--cpus 9") for x in sim.state["command"])
    monkeypatch.setattr(runtime_daemon, "prepare_generation",
                        lambda *a, **k: pytest.fail("unproven manager reached preparation"))
    with pytest.raises(admin.AdminError, match="loaded daemon command"):
        _update(sim, runtimes[1][1])
    assert not sim.commands


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
def test_first_migration_refuses_a_corrupt_rollback_generation_before_stopping(
    manager_sim, runtimes,
):
    root, shas, *_ = runtimes
    (runtime_slots.slot_source(root, shas[0]) / "version.txt").write_text("foreign bytes")
    with pytest.raises(runtime_slots.RuntimeSlotError):
        _update(manager_sim, shas[1])
    assert not manager_sim.commands
    assert admin.read_admin_update_marker() is None


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
def test_manager_command_change_after_plan_refuses_before_stop(manager_sim, runtimes, monkeypatch):
    sim = manager_sim
    plan = runtime_daemon.binding_plan

    def changed(*args, **kwargs):
        result = plan(*args, **kwargs)
        sim.state["command"] = tuple(x.replace("--cpus 2", "--cpus 9")
                                     for x in sim.state["command"])
        return result

    monkeypatch.setattr(runtime_daemon, "binding_plan", changed)
    with pytest.raises(admin.AdminError, match="changed after immutable binding planning"):
        _update(sim, runtimes[1][1])
    assert not sim.commands
    assert runtime_slots.resolve_current(runtimes[0]) == runtimes[1][0]


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
def test_same_generation_rpc_cannot_hide_a_pointer_race(manager_sim, runtimes, monkeypatch):
    sim = manager_sim
    root, shas, *_ = runtimes
    assert _update(sim, shas[1]).success
    _raw, sim.cfg = runtime_daemon.runtime_migration._model(
        config.config_path().read_bytes(), str(config.config_path()),
    )
    sim.registered = sim.cfg.programs["queue"]
    monkeypatch.setattr(runtime_daemon, "prepare_generation", lambda *a, **k: admin.UpdateResult(
        env="queue", git_dir=sim.target.git_dir, branch=None, update_script=None,
        git_pull_rc=0, expected_sha=shas[1], actual_sha=shas[1], sha_check_rc=0,
    ))
    verify = admin._verify_restarted_daemon

    def raced(*args, **kwargs):
        runtime_slots.activate(root, shas[0])
        return verify(*args, **kwargs)

    monkeypatch.setattr(admin, "_verify_restarted_daemon", raced)
    commands = list(sim.commands)
    with pytest.raises(admin.AdminError, match="retained daemon pointer changed"):
        _update(sim, shas[1])
    assert sim.commands == commands
    assert admin.read_admin_update_marker() is None


@pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
@pytest.mark.parametrize("scenario", ["older-than-serving", "already-serving"])
def test_first_class_ancestry_uses_serving_generation_not_legacy_build_checkout(
    manager_sim, runtimes, monkeypatch, scenario,
):
    import subprocess

    from click.testing import CliRunner

    from vq import cli

    sim = manager_sim
    root, shas, *_ = runtimes
    serving_sha = shas[1] if scenario == "older-than-serving" else shas[0]
    serving = runtime_slots.slot_local_program(sim.registered, root, serving_sha)
    runtime_slots.activate(root, serving_sha)
    data, command = runtime_daemon.service_bytes(
        sim.service.read_bytes(), sim.manager, sim.old, serving,
    )
    sim.service.write_bytes(data)
    sim.state["command"] = command
    sim.registered = sim.registered.model_copy(update={"runtime_slot_execution": True})
    sim.cfg = config.Config(programs={"queue": sim.registered})
    config.config_path().write_bytes(runtime_daemon._toml_bytes({
        "programs": {"queue": sim.registered.model_dump(exclude_none=True)},
    }))
    if scenario == "older-than-serving":
        subprocess.run(["git", "-C", sim.registered.git_dir, "checkout", "--detach", shas[0]],
                       capture_output=True, check=True)
    monkeypatch.setattr(config, "load_config", lambda: sim.cfg)
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _: sim.probe)
    monkeypatch.setattr(admin, "_run_git_fetch_sha", lambda *a, **k: (0, "sandbox fetch"))
    calls = []

    def update(*args, **kwargs):
        calls.append(kwargs)
        return admin.UpdateResult(env="queue", git_dir=serving.git_dir, branch=None,
                                  update_script=None, already_current=True)

    monkeypatch.setattr(admin, "update_env", update)
    result = CliRunner().invoke(cli.main, ["self-update", "--expected-sha", shas[0], "--json"])
    if scenario == "older-than-serving":
        assert result.exit_code != 0 and "older" in result.output, result.output
        assert not calls
    else:
        assert result.exit_code == 0, (result.output, result.exception)
        assert len(calls) == 1
        assert calls[0]["require_self_update"] is True


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
def test_first_class_rechecks_forward_ancestry_after_prepare_before_stopping(
    manager_sim, runtimes, monkeypatch,
):
    import subprocess

    sim = manager_sim
    root, shas, *_ = runtimes
    subprocess.run(["git", "-C", sim.old.git_dir, "checkout", "--detach", shas[0]],
                   capture_output=True, check=True)
    original = config.config_path().read_bytes(), sim.service.read_bytes()
    pointers = runtime_slots.resolve_current(root)
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda _: sim.probe)

    def prepare(*args, **kwargs):
        # Model a non-cooperating legacy checkout writer after CLI preflight.
        # The serving path and service argv have not changed, but its SHA has.
        subprocess.run(["git", "-C", sim.old.git_dir, "checkout", "--detach", shas[1]],
                       capture_output=True, check=True)
        return admin.UpdateResult(
            env="queue", git_dir=sim.target.git_dir, branch=None, update_script=None,
            git_pull_rc=0, expected_sha=shas[0], actual_sha=shas[0], sha_check_rc=0,
        )

    monkeypatch.setattr(runtime_daemon, "prepare_generation", prepare)
    with pytest.raises(admin.AdminError, match="forward, exact-descendant"):
        admin.update_env("queue", sim.cfg, host="localhost", expected_sha=shas[0],
                         require_self_update=True)
    assert not sim.commands and not sim.resumed
    assert sim.state["running"]
    assert (config.config_path().read_bytes(), sim.service.read_bytes()) == original
    assert runtime_slots.resolve_current(root) == pointers
    assert admin.read_admin_update_marker() is None


def test_slot_admission_accepts_a_daemon_install_unit_with_preserved_environment(
    service_programs, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """#125 with #73/#112: the supported route off a hand-written ``%h`` unit is
    ``vq daemon install``, which now carries the operator's Environment= lines
    over verbatim, ``%h`` included. The serving-slot rebind must accept that
    unit and keep those lines byte for byte."""
    from vq.web import install

    _registered, old, target = service_programs
    vq = Path(old.python).parent / "vq"
    vq.write_text("#!/bin/sh\n", encoding="utf-8")
    vq.chmod(0o755)
    monkeypatch.setattr(install, "resolve_service_command",
                        lambda kind: [str(vq), *kind.verb])
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "xdg"))
    unit = tmp_path / "xdg" / "systemd" / "user" / "vq-daemon.service"
    unit.parent.mkdir(parents=True)
    preserved = [
        "Environment=PATH=%h/bin:/opt/orca:/usr/bin:/bin",
        "Environment=\"GREETING=hello world\" 'SCRATCH=%h/scr'",
    ]
    unit.write_text("[Service]\n" + "\n".join(preserved)
                    + "\nExecStart=%h/.local/bin/vq daemon run\n")
    plan = install.build_plan(manager="systemd-user", kind=install.DAEMON_SERVICE,
                              preserve_environment=True)
    assert plan.dropped_environment == []
    generated = plan.writes[0].content.encode()

    updated, command = runtime_daemon.service_bytes(
        generated, admin._DaemonServiceManager.SYSTEMD, old, target,
    )

    assert admin._systemd_command_identity_is_vq_daemon(command)
    for line in preserved:
        assert line.encode() + b"\n" in updated
    assert [line for line in updated.splitlines() if not line.startswith(b"ExecStart=")] == [
        line for line in generated.splitlines() if not line.startswith(b"ExecStart=")
    ]


@pytest.mark.parametrize("manager_sim", list(admin._DaemonServiceManager), indirect=True)
@pytest.mark.parametrize("service_programs", ["current"], indirect=True)
def test_failed_recovery_without_checkout_persists_a_retryable_receipt(
    manager_sim, runtimes, monkeypatch,
):
    sim = manager_sim
    sim.state["fail_target"] = True
    assert not _update(sim, runtimes[1][1]).success
    marker = _stale_marker(monkeypatch)
    verify = admin._verify_restarted_daemon
    monkeypatch.setattr(admin, "_verify_restarted_daemon", lambda *a, **k: admin.DaemonProvenance(
        verified=False, actual_sha=None, actual_tree_sha256=None,
        detail="sandbox transient RPC failure",
    ))

    failed = admin.recover_managed_update(sim.cfg, marker_id=marker.marker_id)

    assert not failed.recovered
    saved = admin.read_admin_update_marker()
    assert saved.state == admin.ADMIN_UPDATE_STATE_FAILED
    assert saved.managed_transaction["git_dir"] == sim.old.git_dir
    assert not Path(sim.registered.git_dir).exists()
    monkeypatch.setattr(admin, "_verify_restarted_daemon", verify)
    _stale_marker(monkeypatch)
    retried = admin.recover_managed_update(sim.cfg, marker_id=marker.marker_id)
    assert retried.recovered, retried.detail
    assert admin.read_admin_update_marker() is None
    assert runtime_slots.resolve_current(runtimes[0]) == runtimes[1][0]
