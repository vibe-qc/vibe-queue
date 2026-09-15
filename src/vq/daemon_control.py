"""Daemon process lifecycle: pidfile management + start/stop/status helpers.

The daemon's own loop lives in ``vq.daemon``; this module is the thin wrapper
that turns "spawn a background process and write a pidfile" into a single
function the CLI can call.
"""

from __future__ import annotations

import contextlib
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from vq import paths

_SYSTEMD_EXECSTART_PATH = re.compile(r"(?:^|[ {;])path=([^\s;]+)")
_SYSTEMD_EXECSTART_ARGV = re.compile(r"(?:^|[ {;])argv\[\]=([^;]+)")
_SOURCE_SHA = re.compile(r"[0-9a-f]{40}")
_SOURCE_TREE_SHA256 = re.compile(r"[0-9a-f]{64}")


def _safe_service_diagnostic(value: object, *, limit: int = 240) -> str:
    """Collapse untrusted service-manager output to one control-free line."""
    text = "".join(
        character if character.isprintable() else " "
        for character in str(value)
    )
    return " ".join(text.split())[:limit]


def _empty_process_identity(
    status: str,
    error: str,
) -> dict[str, object]:
    return {
        "status": status,
        "error": _safe_service_diagnostic(error),
        "pid": None,
        "euid": None,
        "python_executable": None,
        "argv": None,
    }


def _validated_process_identity(value: object) -> dict[str, object]:
    """Return strict process evidence or one fail-closed diagnostic."""
    if not isinstance(value, dict):
        return _empty_process_identity(
            "unknown",
            "get_process_identity returned a non-object",
        )
    pid = value.get("pid")
    euid = value.get("euid")
    python_executable = value.get("python_executable")
    argv = value.get("argv")
    version = value.get("version")
    source_sha = value.get("source_sha")
    source_tree_sha256 = value.get("source_tree_sha256")
    multi_user = value.get("multi_user")
    socket_path = value.get("socket_path")
    valid = (
        type(pid) is int
        and pid > 0
        and type(euid) is int
        and euid >= 0
        and isinstance(python_executable, str)
        and bool(python_executable)
        and all(character.isprintable() for character in python_executable)
        and isinstance(argv, list)
        and bool(argv)
        and all(
            isinstance(argument, str)
            and all(character.isprintable() for character in argument)
            for argument in argv
        )
        and isinstance(version, str)
        and bool(version)
        and all(character.isprintable() for character in version)
        and (
            source_sha is None
            or (
                isinstance(source_sha, str)
                and _SOURCE_SHA.fullmatch(source_sha) is not None
            )
        )
        and (
            source_tree_sha256 is None
            or (
                isinstance(source_tree_sha256, str)
                and _SOURCE_TREE_SHA256.fullmatch(source_tree_sha256)
                is not None
            )
        )
        and type(multi_user) is bool
        and isinstance(socket_path, str)
        and bool(socket_path)
        and all(character.isprintable() for character in socket_path)
    )
    if not valid:
        return _empty_process_identity(
            "unknown",
            "get_process_identity returned malformed fields",
        )
    return {
        "status": "ok",
        "error": None,
        "pid": pid,
        "euid": euid,
        "python_executable": python_executable,
        "argv": argv,
        "version": version,
        "source_sha": source_sha,
        "source_tree_sha256": source_tree_sha256,
        "multi_user": multi_user,
        "socket_path": socket_path,
    }


def _trusted_systemctl_path() -> Path | None:
    """Return a fixed system executable path, never a PATH lookup."""
    for candidate in (Path("/usr/bin/systemctl"), Path("/bin/systemctl")):
        if candidate.is_file():
            return candidate
    return None


def _empty_systemd_service_evidence(
    error: str,
    *,
    source: Path | None,
    status: str = "unknown",
) -> dict[str, object]:
    return {
        "active_state": None,
        "argv": None,
        "error": _safe_service_diagnostic(error),
        "exec_start": None,
        "executable": None,
        "id": None,
        "load_state": None,
        "main_pid": None,
        "source": str(source) if source is not None else None,
        "status": status,
        "sub_state": None,
        "user": None,
    }


def _systemd_multi_user_service_evidence() -> dict[str, object]:
    """Return a strict, read-only snapshot of the system daemon unit.

    This fixed-argv probe never invokes a shell. Missing, duplicated,
    malformed, timed-out, or unavailable properties produce one fail-closed
    diagnostic instead of a partial identity.
    """
    from vq import provision  # noqa: PLC0415 - avoids the doctor import cycle

    systemctl = _trusted_systemctl_path()
    if systemctl is None:
        return _empty_systemd_service_evidence(
            "trusted systemctl executable is unavailable",
            source=None,
            status=(
                "unknown" if sys.platform.startswith("linux") else "unsupported"
            ),
        )
    argv = [
        str(systemctl),
        "show",
        provision.MULTI_USER_UNIT,
        "--no-pager",
        "--property=Id",
        "--property=LoadState",
        "--property=ActiveState",
        "--property=SubState",
        "--property=MainPID",
        "--property=User",
        "--property=ExecStart",
    ]
    try:
        proc = subprocess.run(
            argv,
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return _empty_systemd_service_evidence(
            "systemctl show timed out after 10 seconds",
            source=systemctl,
        )
    except OSError as exc:
        return _empty_systemd_service_evidence(
            f"could not invoke systemctl show: {exc}",
            source=systemctl,
        )
    if proc.returncode != 0:
        detail = proc.stderr or proc.stdout or "no output"
        return _empty_systemd_service_evidence(
            f"systemctl show exited {proc.returncode}: {detail}",
            source=systemctl,
        )

    expected = {
        "Id",
        "LoadState",
        "ActiveState",
        "SubState",
        "MainPID",
        "User",
        "ExecStart",
    }
    properties: dict[str, str] = {}
    unparsed_output = False
    for line in (proc.stdout or "").splitlines():
        key, separator, value = line.partition("=")
        if separator != "=":
            unparsed_output |= bool(line.strip())
            continue
        if key not in expected:
            continue
        if key in properties:
            return _empty_systemd_service_evidence(
                f"systemctl show returned duplicate {key}",
                source=systemctl,
            )
        properties[key] = value
    missing = sorted(expected - set(properties))
    if (
        missing == ["ExecStart"]
        and not unparsed_output
        and properties == {
            "Id": provision.MULTI_USER_UNIT,
            "LoadState": "not-found",
            "ActiveState": "inactive",
            "SubState": "dead",
            "MainPID": "0",
            "User": "",
        }
    ):
        # Real systemd omits ExecStart for a nonexistent unit, even with
        # --all. Only this complete canonical negative identity establishes
        # an empty command; loaded, active or partial evidence stays unknown.
        properties["ExecStart"] = ""
        missing = []
    if missing:
        return _empty_systemd_service_evidence(
            "systemctl show omitted " + ", ".join(missing),
            source=systemctl,
        )

    raw_pid = properties["MainPID"]
    try:
        main_pid = int(raw_pid)
    except ValueError:
        return _empty_systemd_service_evidence(
            f"systemctl show returned invalid MainPID {raw_pid!r}",
            source=systemctl,
        )
    if main_pid < 0:
        return _empty_systemd_service_evidence(
            f"systemctl show returned invalid MainPID {raw_pid!r}",
            source=systemctl,
        )

    exec_start = properties["ExecStart"]
    if (
        main_pid == 0
        and properties["ActiveState"] == "inactive"
        and properties["SubState"] == "dead"
        and not exec_start
    ):
        # ``systemctl show`` represents a missing or stopped unit with an
        # empty ExecStart. That is complete negative evidence, not a parse
        # failure; it lets an absent system config remain not applicable.
        return {
            "active_state": properties["ActiveState"] or None,
            "argv": [],
            "error": None,
            "exec_start": "",
            "executable": None,
            "id": properties["Id"] or None,
            "load_state": properties["LoadState"] or None,
            "main_pid": 0,
            "source": str(systemctl),
            "status": "ok",
            "sub_state": properties["SubState"] or None,
            "user": properties["User"] or None,
        }
    path_matches = _SYSTEMD_EXECSTART_PATH.findall(exec_start)
    argv_matches = _SYSTEMD_EXECSTART_ARGV.findall(exec_start)
    if len(path_matches) != 1 or len(argv_matches) != 1:
        return _empty_systemd_service_evidence(
            "systemctl show returned an ambiguous or unparseable ExecStart",
            source=systemctl,
        )
    return {
        "active_state": properties["ActiveState"] or None,
        "argv": argv_matches[0].strip().split(),
        "error": None,
        "exec_start": exec_start,
        "executable": path_matches[0],
        "id": properties["Id"] or None,
        "load_state": properties["LoadState"] or None,
        "main_pid": main_pid,
        "source": str(systemctl),
        "status": "ok",
        "sub_state": properties["SubState"] or None,
        "user": properties["User"] or None,
    }


def write_pidfile(path: Path | None = None, *, multi_user: bool = False) -> None:
    p = path or paths.daemon_pidfile(multi_user=multi_user)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"{os.getpid()}\n")


def remove_pidfile(path: Path | None = None, *, multi_user: bool = False) -> None:
    p = path or paths.daemon_pidfile(multi_user=multi_user)
    with contextlib.suppress(FileNotFoundError):
        p.unlink()


def read_pidfile(path: Path | None = None, *, multi_user: bool = False) -> int | None:
    p = path or paths.daemon_pidfile(multi_user=multi_user)
    if not p.exists():
        return None
    try:
        return int(p.read_text().strip())
    except ValueError:
        return None


def is_daemon_running(path: Path | None = None, *, multi_user: bool = False) -> bool:
    """Return True iff the pidfile points at a live process.

    Stale pidfiles (PID gone) are removed as a side effect.
    """
    pid = read_pidfile(path, multi_user=multi_user)
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        remove_pidfile(path, multi_user=multi_user)
        return False
    except PermissionError:
        # Process exists but is owned by someone else; treat as "running" so we
        # don't try to start a duplicate.
        return True


def is_daemon_serving(path: Path | None = None, *, multi_user: bool = False) -> bool:
    """Return True when the daemon is reachable through the public control path.

    ``is_daemon_running`` is intentionally pidfile-based: start/stop code uses
    it to avoid duplicate local processes and to clean stale pidfiles. Operator
    status, queue listings, and stale-state warnings need the daemon's serving
    state instead. On macOS and manually supervised hosts the daemon can be
    healthy via RPC even when no pidfile exists, so prefer RPC and keep the old
    pidfile probe as a compatibility fallback for no-RPC/old-daemon cases.
    """
    try:
        from vq import rpc as _rpc  # noqa: PLC0415

        if _rpc.ping(multi_user=multi_user) is not None:
            return True
    except Exception:  # noqa: BLE001 - any RPC probe failure falls back below
        pass
    return is_daemon_running(path, multi_user=multi_user)


def _system_multi_user_evidence() -> dict[str, object]:
    """Return strict, read-only evidence for the system multi-user policy.

    The ordinary socket selector is intentionally best-effort so a malformed
    ``/etc/vq/config.toml`` cannot break a genuinely single-user CLI. Fleet
    provenance needs the opposite contract: missing, invalid, and explicitly
    disabled are three different facts. Keep this opt-in evidence on verbose
    ping only so the stable base monitoring envelope does not change.
    """
    from vq import config  # noqa: PLC0415 - avoids lifecycle/config coupling

    source = str(config.SYSTEM_CONFIG_PATH)
    try:
        cfg = config.load_system_config()
    except config.ConfigError as exc:
        return {
            "enabled": None,
            "error": str(exc),
            "source": source,
            "status": "unknown",
        }
    if cfg is None:
        return {
            "enabled": False,
            "error": None,
            "source": source,
            "status": "absent",
        }
    enabled = cfg.multi_user.enabled
    return {
        "enabled": enabled,
        "error": None,
        "source": source,
        "status": "enabled" if enabled else "disabled",
    }


def local_daemon_ping(
    timeout: float,
    *,
    multi_user: bool,
    verbose: bool = False,
) -> tuple[int, dict[str, object]]:
    """Probe one local daemon socket and return its stable CLI envelope.

    Socket-mode selection belongs to the caller: CLI and doctor both know the
    already-loaded configuration, while this helper owns only RPC timing,
    error classification, and the monitoring payload.
    """
    from vq import rpc as _rpc  # noqa: PLC0415

    sock_path = _rpc.socket_path(multi_user=multi_user)
    start = time.perf_counter()
    error: str | None = None
    result: dict[str, object] | None = None
    exit_code = 0
    try:
        result = _rpc.call("ping", multi_user=multi_user, timeout=timeout)
    except ConnectionError as exc:
        error = str(exc)
        exit_code = 1
    except _rpc.RPCError as exc:
        error = str(exc)
        exit_code = 2
    except Exception as exc:  # noqa: BLE001 - defensive probe envelope
        error = f"{type(exc).__name__}: {exc}"
        exit_code = 2
    latency_ms = round((time.perf_counter() - start) * 1000.0, 2)
    envelope: dict[str, object] = {
        "ok": exit_code == 0,
        "version": (result or {}).get("version") if result else None,
        "source_sha": (result or {}).get("source_sha") if result else None,
        # Content-derived, unlike source_sha, which is a declaration a marker
        # file can carry across an upgrade. None from a daemon older than the
        # key; callers must treat absence as "unknown", never as a mismatch.
        "source_tree_sha256": (
            (result or {}).get("source_tree_sha256") if result else None
        ),
        "multi_user": (
            (result or {}).get("multi_user") if result else multi_user
        ),
        "socket_path": str(sock_path),
        "latency_ms": latency_ms,
        "error": error,
    }
    if verbose:
        methods: list[str] | None = None
        methods_error: str | None = None
        if exit_code == 0:
            try:
                method_result = _rpc.call(
                    "get_methods", multi_user=multi_user, timeout=timeout
                )
                if isinstance(method_result, dict):
                    raw = method_result.get("methods")
                    if isinstance(raw, list) and all(
                        isinstance(method, str) for method in raw
                    ):
                        methods = list(raw)
                    else:
                        methods_error = "get_methods returned malformed fields"
                else:
                    methods_error = "get_methods returned a non-object"
            except Exception as exc:  # noqa: BLE001 - auxiliary probe only
                methods_error = (
                    "get_methods probe failed: "
                    + _safe_service_diagnostic(exc)
                )
        if exit_code != 0:
            process_identity = _empty_process_identity(
                "unknown",
                error or "daemon ping did not succeed",
            )
        elif methods_error is not None:
            process_identity = _empty_process_identity(
                "unknown",
                methods_error,
            )
        elif methods is not None and "get_process_identity" not in methods:
            process_identity = _empty_process_identity(
                "unsupported",
                "daemon does not advertise get_process_identity",
            )
        elif methods is None:
            process_identity = _empty_process_identity(
                "unknown",
                "get_methods capability was not observed",
            )
        else:
            try:
                identity = _rpc.call(
                    "get_process_identity",
                    multi_user=multi_user,
                    timeout=timeout,
                )
                process_identity = _validated_process_identity(identity)
            except Exception as exc:  # noqa: BLE001 - auxiliary probe only
                process_identity = _empty_process_identity(
                    "unknown",
                    f"get_process_identity probe failed: {exc}",
                )
        system_mode = _system_multi_user_evidence()
        envelope["methods"] = methods
        envelope["process_identity"] = process_identity
        envelope["system_multi_user"] = system_mode
        # A responding user daemon or absent config cannot prove that the
        # system unit is inactive. Always sample it on verbose diagnostics so
        # an active root service overrides contradictory socket/config data.
        envelope["system_service"] = _systemd_multi_user_service_evidence()
    return exit_code, envelope


@dataclass
class StartResult:
    pid: int
    log_file: Path


class DaemonAlreadyRunning(RuntimeError):
    pass


class DaemonStartFailed(RuntimeError):
    pass


def start_daemon(
    *,
    max_cpus: int | None = None,
    max_jobs: int | None = None,
    max_mem_mb: int | None = None,
    default_job_mem_mb: int | None = None,
    poll_interval: float = 1.0,
    pidfile: Path | None = None,
    log_file: Path | None = None,
    spawn_timeout: float = 5.0,
) -> StartResult:
    """Spawn the daemon as a detached background process.

    Waits up to ``spawn_timeout`` seconds for the child to write its pidfile,
    raises DaemonStartFailed if it crashes or times out.
    """
    pidfile = pidfile or paths.daemon_pidfile()
    log_file = log_file or paths.daemon_logfile()
    if is_daemon_running(pidfile):
        raise DaemonAlreadyRunning(f"daemon already running (pid {read_pidfile(pidfile)})")

    log_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "vq", "daemon", "run", "--poll-interval", str(poll_interval)]
    if max_cpus is not None:
        cmd.extend(["--max-cpus", str(max_cpus)])
    if max_jobs is not None:
        cmd.extend(["--max-jobs", str(max_jobs)])
    if max_mem_mb is not None:
        cmd.extend(["--max-mem-mb", str(max_mem_mb)])
    if default_job_mem_mb is not None:
        cmd.extend(["--default-job-mem-mb", str(default_job_mem_mb)])

    # Pass the pidfile/log overrides through env so the child uses the same
    # locations the parent intended (important for tests using VQ_STATE_DIR).
    env = os.environ.copy()
    log_fh = log_file.open("ab")
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
            env=env,
        )
    finally:
        log_fh.close()

    deadline = time.monotonic() + spawn_timeout
    while time.monotonic() < deadline:
        if is_daemon_running(pidfile):
            return StartResult(pid=read_pidfile(pidfile) or proc.pid, log_file=log_file)
        rc = proc.poll()
        if rc is not None:
            raise DaemonStartFailed(f"daemon exited with code {rc} during startup; see {log_file}")
        time.sleep(0.05)
    raise DaemonStartFailed(f"daemon did not write pidfile within {spawn_timeout}s; see {log_file}")


def stop_daemon(
    *,
    pidfile: Path | None = None,
    timeout: float = 10.0,
) -> int | None:
    """Send SIGTERM to the running daemon and wait for it to exit.

    Returns the killed PID, or None if no daemon was running. Raises
    TimeoutError if the daemon doesn't exit within ``timeout`` seconds.
    """
    pidfile = pidfile or paths.daemon_pidfile()
    if not is_daemon_running(pidfile):
        return None
    pid = read_pidfile(pidfile)
    assert pid is not None
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_pidfile(pidfile)
        return pid
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not is_daemon_running(pidfile):
            return pid
        time.sleep(0.1)
    raise TimeoutError(f"daemon (pid {pid}) did not stop within {timeout}s")
