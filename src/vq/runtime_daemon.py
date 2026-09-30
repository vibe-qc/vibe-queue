"""Exact service and registry binding plans for immutable daemon generations.

The managed admin transaction owns stopping, restarting and RPC verification.
This module keeps its reversible file bindings separate from legacy venv moves.
"""
from __future__ import annotations

import base64
import contextlib
import copy
import datetime
import json
import math
import os
import plistlib
import re
import shlex
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from vq import admin, config, fleet_release, runtime_migration, runtime_slots

SCHEMA = "vq-managed-daemon-slots-v1"

_BINDING_RULE = (
    "it must be a regular file owned by this user, not a symlink, in a directory "
    "that only its owner can write, reached through no symlinked path component"
)


@contextlib.contextmanager
def _binding_inspection(what: str, path: object) -> Iterator[None]:
    """Turn a file the transaction cannot bind into an admission error.

    The migration helpers refuse symlinks, foreign or group-writable files
    and directories, and symlinked path components with their own
    exception types. Before this wrapper those escaped ``vq self-update``
    and ``vq admin update`` as tracebacks; now they are refusals that name
    the file and the rule, and nothing has been changed when they fire.
    """
    try:
        yield
    except runtime_migration.MigrationError as exc:
        raise admin.AdminError(
            f"immutable daemon update cannot bind the {what} at {path}: {exc}; "
            f"{_BINDING_RULE}. Nothing was changed."
        ) from None
    except OSError as exc:
        detail = exc.strerror or str(exc)
        raise admin.AdminError(
            f"immutable daemon update cannot inspect the {what} at {path}: {detail}; "
            f"{_BINDING_RULE}. Nothing was changed."
        ) from None


def _toml_value(value: Any) -> str:
    """Serialize parsed TOML values; exact original bytes remain rollback authority."""
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if type(value) is bool:
        return "true" if value else "false"
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_toml_value(item) for item in value) + "]"
    if isinstance(value, dict):
        return "{ " + ", ".join(json.dumps(key) + " = " + _toml_value(item)
                                  for key, item in value.items()) + " }"
    raise admin.AdminError("unsupported TOML value in serving registry")


def _toml_bytes(value: dict[str, Any]) -> bytes:
    return ("\n".join(json.dumps(key) + " = " + _toml_value(item)
                      for key, item in value.items()) + "\n").encode()


def _file_after(before: dict[str, Any], data: bytes) -> dict[str, Any]:
    entry = before["entry"]
    return {"kind": "file", "data": base64.b64encode(data).decode(),
            "sha256": runtime_migration._digest(data), "mode": entry["mode"],
            "gid": entry["identity"][4]}


def registry_binding(env: str, prog: config.VenvProgram) -> dict[str, Any]:
    """Change only this component's execution binding, preserving parsed config."""
    registry = str(config.config_path())
    with _binding_inspection("serving registry", registry):
        before, data = runtime_migration._file(registry)
    raw, parsed = runtime_migration._model(data, before["path"])
    if parsed.programs.get(env) != prog:
        raise admin.AdminError("serving registry differs from the admitted program")
    expected = copy.deepcopy(raw)
    aliases = []
    for name, candidate in parsed.programs.items():
        if not isinstance(candidate, config.VenvProgram):
            continue
        if candidate.git_dir != prog.git_dir and candidate.python != prog.python:
            continue
        if runtime_migration._profile(candidate) != runtime_migration._profile(prog):
            raise admin.AdminError("serving aliases have conflicting runtime profiles")
        if candidate.runtime_slot_root != prog.runtime_slot_root:
            raise admin.AdminError("serving aliases have conflicting runtime stores")
        expected["programs"][name]["runtime_slot_execution"] = True
        aliases.append(name)
    candidate_bytes = _toml_bytes(expected)
    candidate_raw, _candidate = runtime_migration._model(candidate_bytes, before["path"])
    if candidate_raw != expected:
        raise admin.AdminError("serving registry serialization changed unrelated values")
    return {"before": before, "after": _file_after(before, candidate_bytes),
            "aliases": aliases}


def _systemd_fragment() -> Path:
    proc = subprocess.run(
        ["systemctl", "--user", "show", "vq-daemon", "--property=FragmentPath",
         "--property=DropInPaths", "--no-pager"],
        capture_output=True, text=True, timeout=30, stdin=subprocess.DEVNULL,
    )
    values: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key not in {"FragmentPath", "DropInPaths"} or key in values:
            raise admin.AdminError("ambiguous systemd unit file authority")
        values[key] = value
    if proc.returncode or set(values) != {"FragmentPath", "DropInPaths"}:
        raise admin.AdminError("systemd unit file authority is unavailable")
    if values["DropInPaths"]:
        raise admin.AdminError("slot migration requires a single user unit without drop-ins")
    return runtime_migration._path(values["FragmentPath"])


def service_path(manager: admin._DaemonServiceManager) -> Path:
    if manager is admin._DaemonServiceManager.SYSTEMD:
        return _systemd_fragment()
    return Path.home() / "Library" / "LaunchAgents" / f"{admin.LAUNCHD_DAEMON_LABEL}.plist"


def service_bytes(
    data: bytes, manager: admin._DaemonServiceManager, old: config.VenvProgram,
    target: config.VenvProgram,
) -> tuple[bytes, tuple[str, ...]]:
    """Replace only the exact daemon executable; retain environment and argv tail."""
    if manager is admin._DaemonServiceManager.LAUNCHD:
        payload = plistlib.loads(data)
        if not isinstance(payload, dict) or payload.get("Label") != admin.LAUNCHD_DAEMON_LABEL:
            raise admin.AdminError("launchd plist has a different service identity")
        argv = payload.get("ProgramArguments")
        if not admin._launchd_argv_matches_venv(argv, Path(old.python).parent):
            raise admin.AdminError("launchd plist does not name the old daemon interpreter")
        if "Program" in payload and payload["Program"] != argv[0]:
            raise admin.AdminError("launchd Program differs from ProgramArguments")
        executable = str(Path(target.python).parent / Path(argv[0]).name)
        new_argv = [executable, *argv[1:]]
        payload["ProgramArguments"] = new_argv
        if "Program" in payload:
            payload["Program"] = executable
        fmt = plistlib.FMT_BINARY if data.startswith(b"bplist") else plistlib.FMT_XML
        return plistlib.dumps(payload, fmt=fmt, sort_keys=False), tuple(new_argv)

    lines = data.decode().splitlines(keepends=True)
    section = ""
    selected: int | None = None
    argv = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped
        if section != "[Service]" or not stripped.startswith("ExecStart="):
            continue
        if selected is not None or any(c in stripped for c in ("\\", "%", "$")):
            raise admin.AdminError("unsupported compound or expanded systemd ExecStart")
        selected = index
        argv = shlex.split(stripped.partition("=")[2])
    if (selected is None or len(argv) < 3 or argv[1:3] != ["daemon", "run"]
            or not admin._execstart_matches_vq(argv[0], Path(old.python).parent)):
        raise admin.AdminError("systemd unit does not name the exact canonical old daemon")
    executable = str(Path(target.python).parent / "vq")
    new_argv = [executable, *argv[1:]]
    if any(any(c in arg for c in ("\n", "\r", "%", "$", "\\")) for arg in new_argv):
        raise admin.AdminError("unsupported expanded systemd command argument")
    # systemd's double-quoted arguments accept these JSON-compatible escapes.
    lines[selected] = "ExecStart=" + " ".join(json.dumps(arg) for arg in new_argv) + "\n"
    definition = (
        "systemd-execstart",
        "{ path=" + executable + " ; argv[]=" + " ".join(new_argv)
        + " ; ignore_errors=no ; start_time=[] ; stop_time=[] ; pid=0 ; code=(null) ; status=0 }",
    )
    if not admin._systemd_command_identity_is_vq_daemon(definition):
        raise admin.AdminError("new systemd command is not canonical")
    return "".join(lines).encode(), definition


def service_binding(
    manager: admin._DaemonServiceManager, old: config.VenvProgram, target: config.VenvProgram,
) -> dict[str, Any]:
    with _binding_inspection("daemon service file", manager.value):
        path = str(service_path(manager))
    with _binding_inspection("daemon service file", path):
        before, data = runtime_migration._file(path)
    candidate, command = service_bytes(data, manager, old, target)
    return {"before": before, "after": _file_after(before, candidate),
            "target_command": list(command)}


def prepare_generation(
    env: str, registered: config.VenvProgram, serving: config.VenvProgram,
    *, expected_sha: str, expected_tag: str | None, supplied: list[str] | None,
) -> admin.UpdateResult:
    """Build at the final path while the original service keeps running."""
    if registered.runtime_slot_root is None:
        raise admin.AdminError("serving runtime slot root is required")
    args = admin._managed_update_script_args(serving, supplied)
    target = runtime_slots.slot_local_program(
        registered, registered.runtime_slot_root, expected_sha,
    )
    args[args.index("--venv") + 1] = str(Path(target.python).parent.parent)
    if registered.post_update_script:
        raise admin.AdminError("managed daemon slots reject post_update_script")
    result = admin._do_slot_update_work(
        env, registered, expected_sha=expected_sha, expected_tag=expected_tag,
        update_script_args=args, prepare_only=True, managed_daemon_prepare=True,
    )
    if result.success:
        expected_tree = admin.source_tree_sha256_at_git_commit(
            admin._vq_project_root_for_program(target), expected_sha,
        )
        if admin._installed_tree_digest(target.python) != expected_tree:
            result.work_errors.append("prepared daemon package is not bound to the target Git tree")
        metadata = admin._read_managed_install_metadata(Path(target.python).parent.parent)
        if metadata["extras"] != args[args.index("--extras") + 1]:
            result.work_errors.append("prepared daemon extras differ from the admitted profile")
        wanted_mode = "1" if "--editable" in args else "0"
        configured_modes = shlex.split(serving.update_script or "")
        if "--editable" in configured_modes:
            wanted_mode = "1"
        if metadata["editable"] != wanted_mode:
            result.work_errors.append(
                "prepared daemon install mode differs from the admitted profile"
            )
    return result


def binding_plan(
    env: str, registered: config.VenvProgram, serving: config.VenvProgram,
    target: config.VenvProgram, manager: admin._DaemonServiceManager, sha: str,
) -> dict[str, Any]:
    root = registered.runtime_slot_root
    if root is None:
        raise admin.AdminError("slot binding plan has no runtime root")
    with _binding_inspection("runtime pointers", root):
        current = runtime_migration._pointer(Path(root), "current")
        previous = runtime_migration._pointer(Path(root), "previous")
    for pointer in (current, previous):
        if pointer["entry"]["kind"] == "link":
            runtime_slots._assert_generation(
                runtime_slots.layout(root), Path(pointer["entry"]["target"]).name,
                verify_content=True,
            )
    registry = registry_binding(env, registered)
    service = service_binding(manager, serving, target)
    actions: list[dict[str, Any]] = []
    target_link = str(Path("releases") / sha)
    if current["entry"].get("target") != target_link:
        if current["entry"]["kind"] == "link":
            actions.append({"before": previous,
                            "after": {"kind": "link", "target": current["entry"]["target"]}})
        actions.append({"before": current, "after": {"kind": "link", "target": target_link}})
    actions.extend([
        {"before": registry["before"], "after": registry["after"]},
        {"before": service["before"], "after": service["after"]},
    ])
    return {"root": root, "sha": sha, "registered": registered.model_dump(mode="json"),
            "serving": serving.model_dump(mode="json"), "target": target.model_dump(mode="json"),
            "current": current, "previous": previous, "registry": registry,
            "service": service, "actions": actions, "staged": False}


def stage_bindings(prog: config.VenvProgram, lifecycle: admin._ManagedDaemonUpdate) -> None:
    """Persist rollback bytes and prepared inode identities before service stop."""
    context = lifecycle.slot_transaction
    assert context is not None
    for index, action in enumerate(context["actions"]):
        action["forward_stage"] = f".vq-daemon-{lifecycle.transaction_id}-{index}-forward"
        action["rollback_stage"] = f".vq-daemon-{lifecycle.transaction_id}-{index}-rollback"
    admin._persist_managed_update_receipt(prog, lifecycle)
    for action in context["actions"]:
        path = Path(action["before"]["path"])
        if runtime_migration._snapshot(path) != action["before"]:
            raise admin.AdminError("daemon binding changed before staging")
        with runtime_migration._directory(path.parent) as fd:
            action["prepared_after"] = runtime_migration._write_new(
                fd, action["forward_stage"], action["after"],
            )
            action["prepared_before"] = runtime_migration._write_new(
                fd, action["rollback_stage"], action["before"]["entry"],
            )
        admin._persist_managed_update_receipt(prog, lifecycle)
    context["staged"] = True
    admin._persist_managed_update_receipt(prog, lifecycle)


def check_bindings(context: dict[str, Any]) -> None:
    changed = {action["before"]["path"] for action in context["actions"]}
    for pointer in (context["current"], context["previous"]):
        if pointer["path"] in changed:
            continue
        with _binding_inspection("runtime pointer", pointer["path"]):
            snapshot = runtime_migration._snapshot(Path(pointer["path"]))
        if snapshot != pointer:
            raise admin.AdminError("retained daemon pointer changed outside the transaction")
    for action in context["actions"]:
        with _binding_inspection("daemon binding", action["before"]["path"]):
            snapshot = runtime_migration._snapshot(Path(action["before"]["path"]))
        if snapshot["parent"] != action["before"]["parent"]:
            raise admin.AdminError("daemon binding parent changed")
        allowed = [action["before"]["entry"]]
        allowed.extend(
            action[name] for name in ("prepared_after", "prepared_before") if name in action
        )
        if snapshot["entry"] not in allowed:
            raise admin.AdminError("daemon binding has a foreign edit; refusing replacement")


def replace_bindings(
    prog: config.VenvProgram, lifecycle: admin._ManagedDaemonUpdate, *, rollback: bool,
) -> None:
    context = lifecycle.slot_transaction
    assert context is not None
    check_bindings(context)
    if not context["staged"]:
        if rollback:
            # No switch is admitted until every staged inode has a durable receipt.
            return
        raise admin.AdminError("daemon bindings are not durably staged")
    actions = list(reversed(context["actions"])) if rollback else context["actions"]
    for action in actions:
        path = Path(action["before"]["path"])
        desired = action["prepared_before"] if rollback else action["prepared_after"]
        stage = action["rollback_stage"] if rollback else action["forward_stage"]
        with runtime_migration._directory(path.parent) as fd:
            info = os.fstat(fd)
            if [info.st_dev, info.st_ino] != action["before"]["parent"]:
                raise admin.AdminError("daemon binding parent changed before replacement")
            actual = runtime_migration._entry(fd, path.name)
            if actual == desired or (rollback and actual == action["before"]["entry"]):
                continue
            expected = action["prepared_after"] if rollback else action["before"]["entry"]
            if actual != expected:
                raise admin.AdminError("daemon binding changed before replacement")
            if desired["kind"] == "absent":
                os.unlink(path.name, dir_fd=fd)
            else:
                if runtime_migration._entry(fd, stage) != desired:
                    raise admin.AdminError("staged daemon binding changed")
                os.replace(stage, path.name, src_dir_fd=fd, dst_dir_fd=fd)
            os.fsync(fd)
        admin._persist_managed_update_receipt(prog, lifecycle)


def reload_service(manager: admin._DaemonServiceManager) -> None:
    if manager is admin._DaemonServiceManager.SYSTEMD:
        ok, detail = admin._run_daemon_service_command(
            ["systemctl", "--user", "daemon-reload"], display="systemctl --user daemon-reload",
        )
        if not ok:
            raise admin.AdminError("service definition reload failed: " + detail)


def select_lifecycle_binding(
    lifecycle: admin._ManagedDaemonUpdate, *, target: bool,
) -> config.VenvProgram:
    context = lifecycle.slot_transaction
    assert context is not None
    selected = config.VenvProgram.model_validate(context["target" if target else "serving"])
    lifecycle.venv_path = Path(selected.python).parent.parent
    if target:
        command = tuple(context["service"]["target_command"])
        if lifecycle.manager is admin._DaemonServiceManager.SYSTEMD:
            definition = admin._normalize_systemd_command_identity(command)
            assert definition is not None
            executable = definition[0]
        else:
            executable = command[0]
    else:
        command = tuple(context["old_service_command"])
        executable = context["old_service_executable"]
    lifecycle.service_command = command
    lifecycle.service_executable = executable
    return selected


def serving_program(
    registered: config.VenvProgram, manager: admin._DaemonServiceManager,
) -> config.VenvProgram:
    state = admin._query_daemon_service_state(manager)
    legacy = registered.model_copy(update={
        "runtime_slot_execution": False, "runtime_slot_root": None,
    })
    if admin._service_executable_matches(state.executable, legacy, manager=manager):
        return legacy
    if registered.runtime_slot_root is not None:
        selected = registered.model_copy(
            update={"runtime_slot_execution": True},
        ).execution_program()
        if admin._service_executable_matches(state.executable, selected, manager=manager):
            return selected
    raise admin.AdminError("service no longer names the registered legacy or current generation")


def admit(
    cfg: config.Config, prog: config.VenvProgram, probe: admin._SelfUpdateProbe,
) -> config.VenvProgram:
    if cfg.multi_user.enabled or config.system_multi_user_enabled() or os.geteuid() == 0:
        raise admin.AdminError(
            "immutable daemon self-update currently requires a single-user service"
        )
    if not probe.is_self_update or not probe.manager_available or probe.service_manager is None:
        raise admin.AdminError(
            "immutable daemon self-update requires exact service-manager identity"
        )
    if prog.runtime_slot_root is None:
        raise admin.AdminError("immutable daemon self-update requires runtime_slot_root")
    if prog.post_update_script:
        raise admin.AdminError("immutable daemon updates reject post_update_script")
    manager = admin._DaemonServiceManager(probe.service_manager)
    selected = serving_program(prog, manager)
    admin._guard_managed_git_environment()
    admin._guard_git_index_unlocked(Path(selected.git_dir))
    admin._guard_admin_update_marker(force=False)
    # Validate the owned effective service file before fetching/building anything.
    binding = service_binding(manager, selected, selected)
    state = admin._query_daemon_service_state(manager)
    if not admin._service_command_identities_match(
        manager, state.command_identity, tuple(binding["target_command"]),
    ):
        raise admin.AdminError("loaded daemon command differs from the effective service file")
    return selected


def update(
    env: str, cfg: config.Config, registered: config.VenvProgram,
    probe: admin._SelfUpdateProbe, *, host: str, expected_sha: str | None,
    expected_tag: str | None, supplied: list[str] | None, restart_daemon: bool,
    admin_token: str | None, force: bool, forward_only: bool = False,
) -> admin.UpdateResult:
    if not restart_daemon:
        raise admin.AdminError("immutable serving-daemon update requires restart verification")
    if force:
        raise admin.AdminError("immutable daemon updates require recover-update, not force")
    sha = admin._require_full_hex(expected_sha, length=40, field_name="expected_sha")
    serving = admit(cfg, registered, probe)
    assert registered.runtime_slot_root is not None
    target = runtime_slots.slot_local_program(registered, registered.runtime_slot_root, sha)
    manager = admin._DaemonServiceManager(probe.service_manager)
    # Both the selected serving slot and legacy build source remain fenced.
    with admin.toolset_lifecycle_lock([serving], action="immutable daemon update"):
        args = admin._managed_update_script_args(serving, supplied)
        if registered.post_update_script:
            raise admin.AdminError("immutable daemon updates reject post_update_script")
        # Semantic binding checks precede preparation. The target need not exist
        # yet; neither service_bytes nor registry_binding executes it.
        registry_binding(env, registered)
        service_binding(manager, serving, target)
        with admin.admin_run_log(
            env, what=f"immutable admin update {env}", multi_user=False,
        ) as log:
            result = prepare_generation(
                env, registered, serving, expected_sha=sha, expected_tag=expected_tag,
                supplied=supplied,
            )
            if not result.success:
                if log.available:
                    result.run_log_path = str(log.path)
                return result
            # Recheck service, config and prepared target after the potentially
            # lengthy build, still before acquiring a pause scope or stopping.
            if admit(cfg, registered, probe) != serving:
                raise admin.AdminError("serving generation changed during preparation")
            if forward_only:
                current_sha = admin.current_source_sha(Path(serving.git_dir))
                # The CLI's earlier ancestry check is advisory to this transaction:
                # prove it again from the actual serving runtime and prepared Git
                # graph before taking a pause scope or stopping the service.
                if current_sha is None or (
                    current_sha != sha and fleet_release.git_is_ancestor(
                        Path(target.git_dir), current_sha, sha,
                    ) is not True
                ):
                    raise admin.AdminError(
                        "self-update requires a forward, exact-descendant target "
                        "of the serving generation; ancestry could not be proved"
                    )
            if serving == target and supplied:
                raise admin.AdminError(
                    "a published serving generation cannot be rebuilt in place; select a new SHA"
                )
            context = binding_plan(env, registered, serving, target, manager, sha)
            _raw, binding_cfg = runtime_migration._model(
                runtime_migration._bytes(context["registry"]["before"]["entry"]),
                context["registry"]["before"]["path"],
            )
            aliases_bound = all(binding_cfg.programs[name].runtime_slot_execution
                                for name in context["registry"]["aliases"])
            if serving == target and aliases_bound:
                state = admin._query_daemon_service_state(manager)
                tree = admin.source_tree_sha256_at_git_commit(
                    admin._vq_project_root_for_program(target), sha,
                )
                provenance = admin._verify_restarted_daemon(
                    sha, expected_tree_sha256=tree, require_exact_identity=True,
                    wait_for_identity_change=False,
                    daemon_alive=admin._service_daemon_alive_probe(manager),
                )
                after = admin._query_daemon_service_state(manager)
                check_bindings(context)
                if (provenance.verified and state.running is True and after.running is True
                        and state.executable == after.executable
                        and admin._service_command_identities_match(
                            manager, state.command_identity, after.command_identity,
                        )):
                    result.already_current = True
                    result.daemon_health_verified = True
                    result.daemon_service_manager = manager.value
                    result.daemon_expected_source_sha = sha
                    result.daemon_expected_source_tree_sha256 = tree
                    result.daemon_actual_source_sha = provenance.actual_sha
                    result.daemon_actual_source_tree_sha256 = provenance.actual_tree_sha256
                    return result
            result = admin._update_env_logged(
                env, cfg, prog=serving, multi_user=False, host=host,
                admin_token=admin_token, expected_tag=expected_tag, expected_sha=sha,
                force=False, restart_daemon=True, require_self_update=True,
                managed_daemon_restart=True, initial_self_update_probe=probe,
                update_script_args=args, managed_request_args=supplied,
                slot_transaction=context, prepared_result=result,
            )
            if log.available:
                result.run_log_path = str(log.path)
            return result


def _loaded_binding(lifecycle: admin._ManagedDaemonUpdate) -> config.VenvProgram:
    context = lifecycle.slot_transaction
    assert context is not None
    state = admin._query_daemon_service_state(lifecycle.manager)
    for label, command in (("serving", context["old_service_command"]),
                           ("target", context["service"]["target_command"])):
        prog = config.VenvProgram.model_validate(context[label])
        executable = state.executable
        if lifecycle.manager is admin._DaemonServiceManager.LAUNCHD and executable is None:
            executable = admin._launchd_plist_executable()
        if (admin._service_executable_matches(executable, prog, manager=lifecycle.manager)
                and admin._service_command_identities_match(
                    lifecycle.manager, state.command_identity, tuple(command),
                )):
            return prog
    raise admin.AdminError("service definition is outside both transaction-owned daemon identities")


def restore(
    prog: config.VenvProgram, lifecycle: admin._ManagedDaemonUpdate,
) -> tuple[bool, str]:
    context = lifecycle.slot_transaction
    assert context is not None
    try:
        check_bindings(context)
        loaded = _loaded_binding(lifecycle)
        state = admin._query_daemon_service_state(lifecycle.manager)
        if state.running is not False or state.pid is not None:
            argv = (["systemctl", "--user", "stop", "vq-daemon"]
                    if lifecycle.manager is admin._DaemonServiceManager.SYSTEMD
                    else ["launchctl", "bootout", admin._launchd_daemon_target()])
            ok, detail = admin._run_daemon_service_command(argv, display=shlex.join(argv))
            if not ok:
                return False, detail
        quiescent, detail = admin._wait_for_managed_daemon_quiescence(lifecycle.manager, loaded)
        if not quiescent:
            return False, detail
        old = config.VenvProgram.model_validate(context["serving"])
        if old.current_git_sha(full=True) != lifecycle.pre_source_sha:
            return False, "retained old daemon source SHA changed"
        if admin._installed_tree_digest(old.python) != lifecycle.pre_source_tree_sha256:
            return False, "retained old daemon package tree changed"
        for pointer in (context["current"], context["previous"]):
            if pointer["entry"]["kind"] == "link":
                runtime_slots._assert_generation(
                    runtime_slots.layout(context["root"]),
                    Path(pointer["entry"]["target"]).name, verify_content=True,
                )
        lifecycle.receipt_phase = "restoring_old"
        admin._persist_managed_update_receipt(prog, lifecycle)
        replace_bindings(prog, lifecycle, rollback=True)
        reload_service(lifecycle.manager)
        select_lifecycle_binding(lifecycle, target=False)
        lifecycle.receipt_phase = "files_restored"
        admin._persist_managed_update_receipt(old, lifecycle)
        return True, "old service/registry/pointers restored; both daemon generations retained"
    except (OSError, ValueError, admin.AdminError, config.ConfigError,
            runtime_slots.RuntimeSlotError) as exc:
        return False, str(exc)


def require_binding_state(context: dict[str, Any], *, target: bool) -> None:
    check_bindings(context)
    for action in context["actions"]:
        actual = runtime_migration._snapshot(Path(action["before"]["path"]))["entry"]
        allowed = ([action["prepared_after"]] if target else
                   [action["before"]["entry"], action.get("prepared_before")])
        if actual not in allowed:
            raise admin.AdminError("daemon bindings do not prove the terminal transaction state")


def _entry_value(entry: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in entry.items() if k not in {"identity", "gid"}}


def recovery_resources(
    registered: config.VenvProgram, raw: dict[str, Any],
) -> tuple[tuple[str, str], ...]:
    context = raw.get("slot_transaction")
    if not isinstance(context, dict) or context.get("root") != registered.runtime_slot_root:
        raise admin.AdminError("slot receipt root differs from its registered component")
    sha = admin._require_full_hex(context.get("sha"), length=40, field_name="slot sha")
    resources = list(admin._runtime_slot_lifecycle_resources(registered, expected_sha=sha))
    old_sha = admin._require_full_hex(raw.get("pre_source_sha"), length=40, field_name="old sha")
    resources.extend(admin._runtime_slot_lifecycle_resources(registered, expected_sha=old_sha))
    return tuple(resources)


def parse_receipt(
    marker: admin.AdminUpdateMarker, cfg: config.Config,
) -> tuple[config.VenvProgram, admin._ManagedDaemonUpdate]:
    try:
        return _parse_receipt(marker, cfg)
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise admin.AdminError("malformed immutable daemon recovery receipt") from exc


def _parse_receipt(
    marker: admin.AdminUpdateMarker, cfg: config.Config,
) -> tuple[config.VenvProgram, admin._ManagedDaemonUpdate]:
    """Authenticate each path and reversible byte change before adopting a receipt."""
    raw = marker.managed_transaction
    if not isinstance(raw, dict) or raw.get("schema") != SCHEMA:
        raise admin.AdminError("unrecognized immutable daemon receipt")
    if set(raw) != admin._MANAGED_RECEIPT_FIELDS | {"slot_transaction"}:
        raise admin.AdminError("immutable daemon receipt has unexpected fields")
    multi_user, _root = admin._require_admin_update_pause_queue_scope(marker)
    if multi_user or cfg.multi_user.enabled or config.system_multi_user_enabled():
        raise admin.AdminError("immutable daemon recovery requires a single-user authority")
    if (type(raw.get("owner_uid")) is not int or raw["owner_uid"] != os.geteuid()
            or os.geteuid() == 0 or raw.get("backup_moved") is not False):
        raise admin.AdminError("immutable daemon receipt has invalid owner or moved-venv state")
    if (type(raw.get("was_running")) is not bool
            or (raw.get("pre_pid") is not None and
                (type(raw["pre_pid"]) is not int or raw["pre_pid"] <= 0))
            or (raw.get("pre_checkout_branch") is not None and
                not isinstance(raw["pre_checkout_branch"], str))
            or not isinstance(marker.paused_jobids, list)
            or not all(isinstance(job, str) and job for job in marker.paused_jobids)):
        raise admin.AdminError("immutable daemon receipt has invalid process or pause identity")
    tx = raw.get("transaction_id")
    if not isinstance(tx, str) or re.fullmatch(r"[0-9a-f]{32}", tx) is None:
        raise admin.AdminError("invalid immutable daemon transaction id")
    env = raw.get("env")
    if not isinstance(env, str) or marker.envs != [env]:
        raise admin.AdminError("immutable daemon receipt is outside its exact marker scope")
    if (not isinstance(marker.pause_token, str)
            or re.fullmatch(r"admin-update-[0-9a-f]{12}", marker.pause_token) is None
            or type(marker.surgical_pause) is not bool):
        raise admin.AdminError("immutable daemon receipt lacks its exact pause scope")
    phase = raw.get("phase")
    if phase not in {"armed", "slot_activated", "target_ready", "target_verified",
                     "restoring_old", "files_restored", "old_restored", "target_committed"}:
        raise admin.AdminError("invalid immutable daemon receipt phase")
    registered = admin._resolve_venv_program(env, cfg)
    context = raw.get("slot_transaction")
    if not isinstance(context, dict):
        raise admin.AdminError("immutable daemon receipt lacks its binding plan")
    if set(context) != {"root", "sha", "registered", "serving", "target", "current",
                        "previous", "registry", "service", "actions", "staged",
                        "old_service_command", "old_service_executable"}:
        raise admin.AdminError("immutable daemon binding plan has unexpected fields")
    recovery_resources(registered, raw)
    original = config.VenvProgram.model_validate(context["registered"])
    original_flag = {"runtime_slot_execution": original.runtime_slot_execution}
    if registered.model_copy(update=original_flag) != original:
        raise admin.AdminError("registered build inputs or profile changed since daemon update")
    root = runtime_slots.layout(context["root"]).root
    if str(root) != original.runtime_slot_root:
        raise admin.AdminError("immutable daemon root is not canonical")
    sha = context["sha"]
    expected_target = runtime_slots.slot_local_program(original, root, sha)
    target = config.VenvProgram.model_validate(context["target"])
    if target != expected_target:
        raise admin.AdminError("receipt target is not the exact derived generation")
    serving = config.VenvProgram.model_validate(context["serving"])
    old_sha = admin._require_full_hex(raw.get("pre_source_sha"), length=40, field_name="old sha")
    legacy = original.model_copy(update={
        "runtime_slot_root": None, "runtime_slot_execution": False,
    })
    if serving != legacy and serving != runtime_slots.slot_local_program(original, root, old_sha):
        raise admin.AdminError("receipt old runtime is outside the legacy/retained component")
    if raw.get("git_dir") not in {original.git_dir, serving.git_dir, target.git_dir}:
        raise admin.AdminError("receipt checkout is outside its registered component")
    manager = admin._DaemonServiceManager(raw["manager"])
    registry = context["registry"]
    service = context["service"]
    if registry["before"]["path"] != str(config.config_path()):
        raise admin.AdminError("receipt registry is outside this queue's config authority")
    if service["before"]["path"] != str(service_path(manager)):
        raise admin.AdminError("receipt service file is outside the manager's effective authority")
    before_raw, before_cfg = runtime_migration._model(
        runtime_migration._bytes(registry["before"]["entry"]), registry["before"]["path"],
    )
    if before_cfg.programs.get(env) != original:
        raise admin.AdminError("receipt original registry has a different component")
    expected_raw = copy.deepcopy(before_raw)
    aliases = []
    for name, alias in before_cfg.programs.items():
        if not isinstance(alias, config.VenvProgram):
            continue
        if alias.git_dir != original.git_dir and alias.python != original.python:
            continue
        if (runtime_migration._profile(alias) != runtime_migration._profile(original)
                or alias.runtime_slot_root != original.runtime_slot_root):
            raise admin.AdminError("receipt aliases do not describe one component")
        aliases.append(name)
        expected_raw["programs"][name]["runtime_slot_execution"] = True
    expected_registry = _file_after(registry["before"], _toml_bytes(expected_raw))
    if registry["after"] != expected_registry or registry["aliases"] != aliases:
        raise admin.AdminError("receipt registry changes unrelated fields or aliases")
    old_service = runtime_migration._bytes(service["before"]["entry"])
    new_service, command = service_bytes(old_service, manager, serving, target)
    _same_service, old_command = service_bytes(old_service, manager, serving, serving)
    old_executable = (admin._normalize_systemd_command_identity(old_command)[0]
                      if manager is admin._DaemonServiceManager.SYSTEMD else old_command[0])
    if (service["after"] != _file_after(service["before"], new_service)
            or service["target_command"] != list(command)
            or context["old_service_executable"] != old_executable
            or not admin._service_command_identities_match(
                manager, tuple(context["old_service_command"]), old_command,
            )):
        raise admin.AdminError("receipt service changes are not an executable-only rebind")
    expected_actions = []
    current, previous = context["current"], context["previous"]
    for label, pointer in (("current", current), ("previous", previous)):
        if pointer["path"] != str(root / label):
            raise admin.AdminError("receipt pointer escapes the runtime root")
        entry = pointer["entry"]
        if entry["kind"] == "link":
            pointer_sha = Path(entry["target"]).name
            runtime_slots.slot_path(root, pointer_sha)
            if entry["target"] != str(Path("releases") / pointer_sha):
                raise admin.AdminError("receipt pointer is not an exact generation")
        elif entry["kind"] != "absent":
            raise admin.AdminError("receipt pointer has an invalid kind")
    desired = str(Path("releases") / sha)
    if current["entry"].get("target") != desired:
        if current["entry"]["kind"] == "link":
            expected_actions.append({"before": previous, "after": {
                "kind": "link", "target": current["entry"]["target"],
            }})
        expected_actions.append({"before": current, "after": {"kind": "link", "target": desired}})
    expected_actions.extend({"before": item["before"], "after": item["after"]}
                            for item in (registry, service))
    if (type(context.get("staged")) is not bool
            or len(context["actions"]) != len(expected_actions)):
        raise admin.AdminError("receipt binding set is incomplete")
    pairs = zip(context["actions"], expected_actions, strict=True)
    for index, (action, expected) in enumerate(pairs):
        if not {"before", "after"} <= set(action) <= {
            "before", "after", "forward_stage", "rollback_stage",
            "prepared_before", "prepared_after",
        }:
            raise admin.AdminError("receipt binding action has unexpected fields")
        if action["before"] != expected["before"] or action["after"] != expected["after"]:
            raise admin.AdminError("receipt contains an unauthorized binding change")
        for label, suffix, entry in (("forward", "after", action["after"]),
                                    ("rollback", "before", action["before"]["entry"])):
            stage = action.get(label + "_stage")
            if stage is not None and stage != f".vq-daemon-{tx}-{index}-{label}":
                raise admin.AdminError("receipt staging path is not transaction-owned")
            prepared = action.get("prepared_" + suffix)
            if prepared is None:
                if context["staged"]:
                    raise admin.AdminError("receipt lacks a prepared binding identity")
                continue
            if _entry_value(prepared) != _entry_value(entry):
                raise admin.AdminError("prepared receipt bytes differ from the approved binding")
            if prepared["kind"] != "absent":
                identity = prepared.get("identity")
                if (not isinstance(identity, list) or len(identity) != 7
                        or not all(type(x) is int for x in identity)
                        or identity[3] != os.geteuid()):
                    raise admin.AdminError("prepared receipt inode has an invalid owner")
    check_bindings(context)
    path = Path(raw["venv_path"])
    if path not in (Path(serving.python).parent.parent, Path(target.python).parent.parent):
        raise admin.AdminError("receipt active interpreter escapes its component")
    backup = admin._managed_update_backup_path(serving, Path(serving.python).parent.parent, tx)
    if str(backup) != raw.get("venv_backup"):
        raise admin.AdminError("receipt rollback identity is not transaction-derived")
    lifecycle = admin._ManagedDaemonUpdate(
        manager=manager, env=env, pre_pid=raw.get("pre_pid"),
        was_running=raw.get("was_running") is True,
        was_stopped=True, pre_source_sha=old_sha,
        pre_source_tree_sha256=admin._require_full_hex(
            raw.get("pre_source_tree_sha256"), length=64, field_name="old tree",
        ),
        pre_checkout_branch=raw.get("pre_checkout_branch"), venv_path=path, venv_backup=backup,
        service_executable=raw["service_executable"], service_command=tuple(raw["service_command"]),
        transaction_id=tx, receipt_phase=phase, slot_transaction=context,
        target_source_sha=raw.get("target_source_sha"),
        target_source_tree_sha256=raw.get("target_source_tree_sha256"),
    )
    selected_target = path == Path(target.python).parent.parent
    executable, definition = lifecycle.service_executable, lifecycle.service_command
    select_lifecycle_binding(lifecycle, target=selected_target)
    if (executable != lifecycle.service_executable or not admin._service_command_identities_match(
        manager, definition, lifecycle.service_command,
    )):
        raise admin.AdminError("receipt service identity differs from its selected generation")
    if lifecycle.target_source_sha is not None and lifecycle.target_source_sha != sha:
        raise admin.AdminError("receipt target SHA differs from its binding")
    if lifecycle.target_source_tree_sha256 is not None:
        admin._require_full_hex(
            lifecycle.target_source_tree_sha256, length=64, field_name="target tree",
        )
    if phase in {"target_committed", "old_restored"}:
        if serving != target and selected_target != (phase == "target_committed"):
            raise admin.AdminError("terminal receipt selects the wrong service generation")
        require_binding_state(context, target=phase == "target_committed")
    _loaded_binding(lifecycle)
    return registered, lifecycle
