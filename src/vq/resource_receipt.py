"""Terminal resource receipt for directly supervised local jobs.

The daemon starts this module inside the same process group and cgroup as the
effective command. It waits for that command once, records POSIX child usage,
then preserves the command's shell-style exit code for the daemon and orphan
recovery marker.
"""

from __future__ import annotations

import argparse
import contextlib
import errno
import json
import os
import resource
import sys
import time
from pathlib import Path

from vq import cgroup

RESOURCE_USAGE_BASENAME = "resource-usage.json"
RESOURCE_USAGE_SCHEMA = "vq.direct-resource-usage.v1"
COLLECTOR_FAILURE_EXIT_CODE = 125


def _read_cgroup_counter(cgroup_path: Path, name: str) -> int | None:
    try:
        value = int((cgroup_path / name).read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    return value if value >= 0 else None


def _dedicated_scope_path(scope_name: str | None) -> Path | None:
    """Return this process's cgroup only when it is the promised job scope."""
    if scope_name is None:
        return None
    expected = f"{scope_name.removesuffix('.scope')}.scope"
    raw_path = cgroup.cgroup_path_for_pid(os.getpid())
    if raw_path is None:
        return None
    cgroup_path = Path(raw_path)
    if cgroup_path.name != expected:
        return None
    return cgroup_path


def _process_counter_baseline(cgroup_path: Path | None) -> tuple[Path, int] | None:
    """Return the dedicated job cgroup and its pre-command task count."""
    if cgroup_path is None:
        return None
    baseline = _read_cgroup_counter(cgroup_path, "pids.current")
    if baseline is None:
        return None
    return cgroup_path, baseline


def _peak_command_process_count(
    baseline: tuple[Path, int] | None,
) -> tuple[int | None, str | None, str]:
    if baseline is None:
        return (
            None,
            None,
            "unavailable without a dedicated cgroup-v2 pids.peak counter",
        )
    cgroup_path, collector_tasks = baseline
    peak_tasks = _read_cgroup_counter(cgroup_path, "pids.peak")
    if peak_tasks is None or peak_tasks < collector_tasks:
        return (
            None,
            None,
            "unavailable because the dedicated cgroup lacks pids.peak",
        )
    return (
        peak_tasks - collector_tasks,
        "cgroup-v2-pids.peak-minus-collector-baseline",
        "peak concurrent command tasks above the collector baseline; threads count as tasks",
    )


def _shell_exit_code(wait_status: int) -> int:
    code = os.waitstatus_to_exitcode(wait_status)
    return code if code >= 0 else 128 - code


def _exec_and_wait(
    command: list[str],
) -> tuple[int, float, resource.struct_rusage]:
    started = time.monotonic()
    child_pid = os.fork()
    if child_pid == 0:
        try:
            os.execvpe(command[0], command, os.environ)
        except OSError as exc:
            exit_code = 127 if exc.errno == errno.ENOENT else 126
            message = f"vq: cannot execute {command[0]!r}: {exc}\n".encode("utf-8", "replace")
            try:
                os.write(2, message)
            finally:
                os._exit(exit_code)

    while True:
        try:
            waited_pid, wait_status, usage = os.wait4(child_pid, 0)
            break
        except InterruptedError:
            continue
    if waited_pid != child_pid:  # pragma: no cover - wait4(pid) guarantees this
        raise ChildProcessError(f"wait4 returned unexpected pid {waited_pid}")
    return _shell_exit_code(wait_status), time.monotonic() - started, usage


def _peak_rss_kb(usage: resource.struct_rusage) -> int:
    native = int(usage.ru_maxrss)
    # Darwin reports bytes; Linux and the BSDs supported by vq report KiB.
    return native // 1024 if sys.platform == "darwin" else native


def _ok_receipt(
    *,
    command_exit_code: int,
    wall_seconds: float,
    usage: resource.struct_rusage,
    process_counter: tuple[Path, int] | None,
) -> dict[str, object]:
    user_cpu = float(usage.ru_utime)
    system_cpu = float(usage.ru_stime)
    peak_rss_kb = _peak_rss_kb(usage)
    process_count, process_source, process_semantics = _peak_command_process_count(process_counter)
    return {
        "schema": RESOURCE_USAGE_SCHEMA,
        "status": "ok",
        "collector": "posix-wait4",
        "scope": "effective-command",
        "command_status": "succeeded" if command_exit_code == 0 else "failed",
        "command_exit_code": command_exit_code,
        "wall_seconds": round(wall_seconds, 6),
        "user_cpu_seconds": round(user_cpu, 6),
        "system_cpu_seconds": round(system_cpu, 6),
        "active_cpu_seconds": round(user_cpu + system_cpu, 6),
        "peak_rss_kb": peak_rss_kb,
        "peak_rss_mb": round(peak_rss_kb / 1024.0, 6),
        "process_count": process_count,
        "metric_sources": {
            "wall_seconds": "clock-monotonic",
            "cpu_seconds": "posix-wait4-rusage",
            "peak_rss": "posix-wait4-ru_maxrss",
            "process_count": process_source,
        },
        "aggregation_semantics": {
            "wall_seconds": "elapsed effective command",
            "active_cpu_seconds": (
                "user plus system CPU for the command and descendants "
                "included by POSIX child accounting"
            ),
            "peak_rss": (
                "maximum resident set from ru_maxrss; not a sum across concurrent processes"
            ),
            "process_count": process_semantics,
        },
    }


def _error_receipt(error: str) -> dict[str, object]:
    return {
        "schema": RESOURCE_USAGE_SCHEMA,
        "status": "error",
        "collector": "posix-wait4",
        "scope": "effective-command",
        "error": error,
        "command_status": "not_run",
        "command_exit_code": None,
        "wall_seconds": None,
        "user_cpu_seconds": None,
        "system_cpu_seconds": None,
        "active_cpu_seconds": None,
        "peak_rss_kb": None,
        "peak_rss_mb": None,
        "process_count": None,
        "metric_sources": {
            "wall_seconds": None,
            "cpu_seconds": None,
            "peak_rss": None,
            "process_count": None,
        },
        "aggregation_semantics": {
            "wall_seconds": "unavailable",
            "active_cpu_seconds": "unavailable",
            "peak_rss": "unavailable",
            "process_count": "unavailable",
        },
    }


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_exit_marker(path: Path, exit_code: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{exit_code}\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--exit-marker", type=Path, required=True)
    parser.add_argument("--cgroup-scope-name")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        return COLLECTOR_FAILURE_EXIT_CODE

    scope_path = _dedicated_scope_path(args.cgroup_scope_name)
    if args.cgroup_scope_name is not None and scope_path is None:
        command_exit_code = COLLECTOR_FAILURE_EXIT_CODE
        receipt = _error_receipt("cgroup_scope_mismatch")
    else:
        process_counter = _process_counter_baseline(scope_path)
        try:
            command_exit_code, wall_seconds, usage = _exec_and_wait(command)
        except OSError:
            command_exit_code = COLLECTOR_FAILURE_EXIT_CODE
            receipt = _error_receipt("command_fork_or_wait_failed")
        else:
            receipt = _ok_receipt(
                command_exit_code=command_exit_code,
                wall_seconds=wall_seconds,
                usage=usage,
                process_counter=process_counter,
            )

    # Once the required scope gate has passed, telemetry must never replace the
    # effective command's outcome. A broken workspace can lose either artifact,
    # but the wrapper still returns the command rc.
    with contextlib.suppress(OSError):
        _atomic_write_json(args.receipt, receipt)
    with contextlib.suppress(OSError):
        _write_exit_marker(args.exit_marker, command_exit_code)
    return command_exit_code


if __name__ == "__main__":
    raise SystemExit(main())
