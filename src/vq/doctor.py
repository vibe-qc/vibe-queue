"""Read-only diagnostics shared by the doctor CLI and fleet web board.

The public functions in this module build and classify the plain dictionary
records emitted by ``vq doctor``. Transport probing and per-host orchestration
move here in later, independently testable increments; terminal rendering and
web fan-out remain adapter responsibilities.
"""
from __future__ import annotations

import json
import math
import re
import shlex
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import cast

from vq import admin as admin_module
from vq import config, host_status, ssh_probe, transport
from vq import drain as drain_module
from vq.host import is_local_host
from vq.scheduler_dispatch import (
    RemoteResult,
    RemoteRunner,
    SchedulerError,
    wrapper_already_applied,
)

# Default outer deadline for one doctor check's subprocess work.
DEFAULT_CHECK_TIMEOUT_SECONDS = 10.0


class _DoctorCheckTimeout(RuntimeError):
    """A doctor-owned subprocess exhausted its logical check deadline."""

    def __init__(
        self,
        *,
        subprobe: str,
        elapsed_seconds: float,
        timeout_seconds: float,
    ) -> None:
        self.subprobe = subprobe
        self.elapsed_seconds = elapsed_seconds
        self.timeout_seconds = timeout_seconds
        super().__init__(f"doctor subprobe {subprobe} timed out")


class _CheckDeadline:
    """One monotonic budget shared by a check and all its subprobes."""

    def __init__(self, timeout_seconds: float) -> None:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("check_timeout must be finite and greater than zero")
        self.timeout_seconds = float(timeout_seconds)
        self.started_at = time.monotonic()

    def elapsed(self) -> float:
        return max(0.0, time.monotonic() - self.started_at)

    def timed_out(self, subprobe: str) -> _DoctorCheckTimeout:
        return _DoctorCheckTimeout(
            subprobe=subprobe,
            elapsed_seconds=self.elapsed(),
            timeout_seconds=self.timeout_seconds,
        )

    def remaining(self, subprobe: str) -> float:
        remaining = self.timeout_seconds - self.elapsed()
        if remaining <= 0:
            raise self.timed_out(subprobe)
        return remaining

    def ensure_unexpired(self, subprobe: str) -> None:
        if self.elapsed() >= self.timeout_seconds:
            raise self.timed_out(subprobe)


def _exception_is_subprocess_timeout(exc: BaseException) -> bool:
    """Recognize transport's re-raised ``subprocess.TimeoutExpired``.

    ``transport.run_remote_vq`` raises its timeout ``from None`` so the raw,
    possibly token-bearing ssh argv never rides an explicit cause chain. The
    implicit ``__context__`` still links the ``TimeoutExpired``, so both links
    are walked. Walking only ``__cause__`` let an outer-limited remote-vq
    timeout surface as a plain check failure ("SOURCE-SHA check failed:
    remote vq timed out after 2.37s on pbs-cluster") instead of the structured
    ``timed_out`` verdict, which a rollout planner cannot tell from a wrong
    SHA.
    """
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if isinstance(current, subprocess.TimeoutExpired):
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False


def _run_remote_vq_with_deadline(
    host_cfg: config.HostConfig,
    *vq_args: str,
    deadline: _CheckDeadline,
    subprobe: str,
    inner_timeout: float = transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    """Run one doctor remote-vq subprobe inside its shared outer budget."""
    remaining = deadline.remaining(subprobe)
    timeout = min(inner_timeout, remaining)
    outer_limited = remaining <= inner_timeout
    try:
        proc = transport.run_remote_vq(
            host_cfg,
            *vq_args,
            check=False,
            timeout=timeout,
            owned_process_group=True,
        )
    except transport.RemoteError as exc:
        if outer_limited and _exception_is_subprocess_timeout(exc):
            raise deadline.timed_out(subprobe) from exc
        raise
    deadline.ensure_unexpired(subprobe)
    return proc


def _run_remote_shell_with_deadline(
    host_cfg: config.HostConfig,
    *shell_args: str,
    deadline: _CheckDeadline,
    subprobe: str,
    stdin_data: str | None = None,
    check_result: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run one doctor scheduler command inside its shared outer budget."""
    remaining = deadline.remaining(subprobe)
    timeout = min(transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS, remaining)
    outer_limited = remaining <= transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS
    try:
        proc = transport.run_remote_shell(
            host_cfg,
            *shell_args,
            check=check_result,
            timeout=timeout,
            stdin_data=stdin_data,
            retry_transient=0,
            owned_process_group=True,
        )
    except transport.RemoteError as exc:
        if outer_limited and _exception_is_subprocess_timeout(exc):
            raise deadline.timed_out(subprobe) from exc
        raise
    deadline.ensure_unexpired(subprobe)
    return proc


def _timeout_metadata(
    exc: BaseException, *, subprobe: str,
) -> dict[str, object]:
    """``{"timed_out": True, "subprobe": ...}`` for a timeout, else ``{}``.

    Empty rather than ``timed_out: False`` so a check that simply failed keeps
    the payload it always had; only the new fact is new.
    """
    if not _exception_is_subprocess_timeout(exc):
        return {}
    return {"timed_out": True, "subprobe": subprobe}


def _remote_failure_check(
    name: str,
    exc: transport.RemoteError,
    *,
    subprobe: str,
) -> dict[str, object]:
    """Render a failed remote probe, marking a timeout as one.

    A probe that ran out of time and a probe that answered "no" are different
    facts, and only one of them is a verdict about the host. The doctor's own
    outer deadline already says so through :func:`_timed_out_check`; the
    transport's inner per-call timeout said it only in prose, so a caller
    reading the payload could not tell a slow login node from an unhealthy
    one.

    That is not hypothetical. On 2026-09-10 pbs-cluster needed 1.6-2.5 s for each of
    three remote vq calls sharing one 10 s budget, ``source-sha``
    intermittently timed out, the helper's live SHA went missing from that
    sweep, and the lane read as not converged -- so a supersede gate refused
    five times for "lacks strictly healthy exact-target evidence" while
    nothing was wrong.
    """
    return check(
        name, False, str(exc), **_timeout_metadata(exc, subprobe=subprobe),
    )


def _timed_out_check(
    name: str,
    exc: _DoctorCheckTimeout,
) -> dict[str, object]:
    """Render a stable timeout without exposing captured remote output."""
    elapsed = exc.elapsed_seconds
    limit = exc.timeout_seconds
    return check(
        name,
        False,
        (
            f"{exc.subprobe} timed out after {elapsed:.6g}s "
            f"(outer deadline {limit:.6g}s)"
        ),
        timed_out=True,
        elapsed_seconds=elapsed,
        subprobe=exc.subprobe,
        timeout_seconds=limit,
    )


_DRIVER_IDENTITY_SCRIPT = """
import sys
from vq import admin

kind = sys.argv[1]
try:
    if kind == "source_sha":
        value = admin.current_source_sha()
        if value is None:
            raise SystemExit(3)
    elif kind == "source_tree_sha256":
        value = admin.source_tree_sha256()
    else:
        raise SystemExit(2)
except Exception as exc:
    print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
    raise SystemExit(4) from None
print(value)
""".strip()

_LOCAL_DAEMON_PROBE_SCRIPT = """
import json
import sys
from vq import daemon_control

exit_code, envelope = daemon_control.local_daemon_ping(
    float(sys.argv[1]),
    multi_user=sys.argv[2] == "1",
    verbose=True,
)
print(json.dumps({
    "exit_code": exit_code,
    "envelope": envelope,
}))
""".strip()

_OwnedProbeRunner = Callable[
    [Sequence[str], float],
    subprocess.CompletedProcess[str],
]


def _driver_identity_probe(
    kind: str,
    deadline: _CheckDeadline,
) -> tuple[str | None, str | None]:
    """Read one local driver identity in a killable owned subprocess."""
    subprobe = (
        "driver_source_sha"
        if kind == "source_sha"
        else "driver_source_tree_sha256"
    )
    try:
        proc = transport.run_owned_subprocess(
            [sys.executable, "-c", _DRIVER_IDENTITY_SCRIPT, kind],
            timeout=deadline.remaining(subprobe),
            capture_output=True,
            text=True,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise deadline.timed_out(subprobe) from exc
    deadline.ensure_unexpired(subprobe)
    output = (proc.stdout or "").strip().splitlines()
    value = output[0].strip().lower() if output else None
    if proc.returncode == 0 and value:
        return value, None
    detail = (proc.stderr or "").strip().splitlines()
    return None, detail[0] if detail else f"exit {proc.returncode}"


def _local_daemon_probe(
    timeout: float,
    *,
    multi_user: bool,
    deadline: _CheckDeadline,
    runner: _OwnedProbeRunner | None = None,
) -> tuple[int, dict[str, object]]:
    """Run the complete verbose local daemon diagnosis in an owned group."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("daemon RPC timeout must be finite and greater than zero")
    remaining = deadline.remaining("daemon_rpc")
    inner_timeout = min(timeout, remaining)
    argv = [
        sys.executable,
        "-c",
        _LOCAL_DAEMON_PROBE_SCRIPT,
        repr(inner_timeout),
        "1" if multi_user else "0",
    ]
    try:
        if runner is None:
            proc = transport.run_owned_subprocess(
                argv,
                timeout=remaining,
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            proc = runner(argv, remaining)
    except subprocess.TimeoutExpired as exc:
        raise deadline.timed_out("daemon_rpc") from exc
    deadline.ensure_unexpired("daemon_rpc")
    if proc.returncode != 0:
        return 2, {
            "error": f"local daemon probe worker failed (exit {proc.returncode})"
        }
    try:
        payload = json.loads(proc.stdout or "{}")
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        return 2, {"error": "local daemon probe worker returned malformed output"}
    exit_code = payload.get("exit_code")
    envelope = payload.get("envelope")
    if type(exit_code) is not int or not isinstance(envelope, dict):
        return 2, {"error": "local daemon probe worker returned malformed fields"}
    return exit_code, cast(dict[str, object], envelope)


def check(
    name: str,
    ok: bool,
    message: str,
    **metadata: object,
) -> dict[str, object]:
    """Build one stable read-only doctor verdict record."""
    return {"name": name, "ok": ok, "message": message, **metadata}


_DAEMON_PROVENANCE_FIELDS = (
    "version",
    "source_sha",
    "source_tree_sha256",
    "multi_user",
    "process_identity",
    "socket_path",
    "system_service",
    "system_multi_user",
)

_LEGACY_VERBOSE_OPTION_ERROR = re.compile(
    r"\A(?:Usage: (?P<program>[^\r\n]+) daemon ping \[OPTIONS\] \[HOST\]\r?\n"
    r"Try '(?P=program) daemon ping --help' for help\.\r?\n\r?\n)?"
    r"(?:Error: No such option: --verbose|"
    r"Error: No such option '--verbose'\.)\r?\n?\Z"
)


def _daemon_provenance_metadata(
    envelope: object,
) -> dict[str, object]:
    """Copy only structured daemon identity fields into a doctor check."""
    if not isinstance(envelope, dict):
        return {}
    return {
        field: envelope[field]
        for field in _DAEMON_PROVENANCE_FIELDS
        if field in envelope
    }


def remote_vq_nonzero_message(
    host_cfg: config.HostConfig,
    proc: subprocess.CompletedProcess[str],
    *,
    host_label: str | None = None,
    parse_error: str | None = None,
) -> str:
    """Explain a remote-vq failure and preserve its operator hints."""
    stderr = proc.stderr.strip()
    stdout = proc.stdout.strip()
    detail = stderr or stdout or "(empty)"
    config_host = host_label or host_cfg.ssh
    lines = [
        f"{host_cfg.remote_vq} failed on {host_cfg.ssh} "
        f"(exit {proc.returncode}): {detail}",
    ]
    if parse_error is not None:
        lines.append(f"hint: remote vq returned non-JSON output: {parse_error}")
    if proc.returncode == 127:
        lines.extend(
            [
                "hint: the configured remote_vq command was not found on "
                "the remote host.",
                f"configured remote_vq: {host_cfg.remote_vq}",
                f"next: update the host's vq install or fix "
                f"[hosts.{config_host}].remote_vq in config.toml.",
                f"optional: to keep fleet sweeps quiet until repaired, run "
                f"`vq host down {config_host} --reason \"remote_vq missing\"`; "
                f"restore it with `vq host up {config_host}`.",
            ]
        )
    return "\n".join(lines)


def _verbose_ping_is_unsupported(
    proc: subprocess.CompletedProcess[str],
) -> bool:
    """Recognize only Click's legacy unknown-option failure."""
    return (
        proc.returncode == 2
        and not (proc.stdout or "").strip()
        and _LEGACY_VERBOSE_OPTION_ERROR.fullmatch(proc.stderr or "")
        is not None
    )


def ping_remote(
    host_cfg: config.HostConfig,
    *,
    host_label: str,
    timeout: float,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> tuple[list[dict[str, object]], bool]:
    """Return remote daemon checks and whether SSH transport failed."""
    deadline = _CheckDeadline(check_timeout)
    try:
        proc = _run_remote_vq_with_deadline(
            host_cfg,
            "daemon",
            "ping",
            "--verbose",
            "--json",
            "--timeout",
            str(timeout),
            "localhost",
            deadline=deadline,
            subprobe="daemon_ping",
        )
    except _DoctorCheckTimeout as exc:
        return (
            [
                _timed_out_check("remote_vq", exc),
                check("daemon_rpc", False, "not checked; remote vq timed out"),
            ],
            False,
        )
    except transport.RemoteError as exc:
        return (
            [
                check("remote_vq", False, str(exc)),
                check("daemon_rpc", False, "not checked; remote vq failed"),
            ],
            looks_like_ssh_transport_failure(str(exc)),
        )

    if _verbose_ping_is_unsupported(proc):
        try:
            proc = _run_remote_vq_with_deadline(
                host_cfg,
                "daemon",
                "ping",
                "--json",
                "--timeout",
                str(timeout),
                "localhost",
                deadline=deadline,
                subprobe="daemon_ping_legacy_retry",
            )
        except _DoctorCheckTimeout as exc:
            return (
                [
                    _timed_out_check("remote_vq", exc),
                    check(
                        "daemon_rpc",
                        False,
                        "not checked; remote vq timed out",
                    ),
                ],
                False,
            )
        except transport.RemoteError as exc:
            return (
                [
                    check("remote_vq", False, str(exc)),
                    check(
                        "daemon_rpc",
                        False,
                        "not checked; remote vq failed",
                    ),
                ],
                looks_like_ssh_transport_failure(str(exc)),
            )

    transport_ok = proc.returncode != 255
    checks = [
        check(
            "remote_vq",
            transport_ok,
            (
                "remote vq command returned"
                if transport_ok
                else proc.stderr.strip() or "ssh transport failed"
            ),
        )
    ]
    if not transport_ok:
        checks.append(
            check("daemon_rpc", False, "not checked; remote vq failed")
        )
        return checks, True
    if proc.returncode != 0 and not (proc.stdout or "").strip():
        checks[0] = check(
            "remote_vq",
            False,
            remote_vq_nonzero_message(
                host_cfg,
                proc,
                host_label=host_label,
            ),
        )
        checks.append(
            check("daemon_rpc", False, "not checked; remote vq failed")
        )
        return checks, False
    try:
        envelope = json.loads(proc.stdout or "{}")
    except ValueError as exc:
        if proc.returncode != 0:
            checks[0] = check(
                "remote_vq",
                False,
                remote_vq_nonzero_message(
                    host_cfg,
                    proc,
                    host_label=host_label,
                    parse_error=str(exc),
                ),
            )
            checks.append(
                check("daemon_rpc", False, "not checked; remote vq failed")
            )
            return checks, False
        checks.append(
            check(
                "daemon_rpc",
                False,
                f"remote daemon ping returned invalid JSON: {exc}",
            )
        )
        return checks, False

    ok = bool(envelope.get("ok"))
    if ok:
        message = f"responsive (version={envelope.get('version')})"
    else:
        message = str(envelope.get("error") or "daemon did not respond")
    checks.append(
        check(
            "daemon_rpc",
            ok,
            message,
            **_daemon_provenance_metadata(envelope),
        )
    )
    return checks, False


def ping_local(
    timeout: float,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> tuple[list[dict[str, object]], bool]:
    """Return the local daemon verdict in the shared doctor record shape."""
    deadline = _CheckDeadline(check_timeout)
    try:
        local_cfg = config.load_config()
    except Exception:  # noqa: BLE001 - config errors must not block a ping
        local_cfg = None
    multi_user = bool(
        local_cfg is not None and local_cfg.multi_user.enabled
    ) or config.system_multi_user_enabled()
    try:
        exit_code, envelope = _local_daemon_probe(
            timeout,
            multi_user=multi_user,
            deadline=deadline,
        )
    except _DoctorCheckTimeout as exc:
        return [_timed_out_check("daemon_rpc", exc)], False
    if exit_code == 0:
        version = envelope.get("version")
        return (
            [
                check(
                    "daemon_rpc",
                    True,
                    f"responsive (version={version})",
                    **_daemon_provenance_metadata(envelope),
                )
            ],
            False,
        )
    error = str(envelope.get("error") or "daemon did not respond")
    return [
        check(
            "daemon_rpc",
            False,
            error,
            **_daemon_provenance_metadata(envelope),
        )
    ], False


def console_runtime_checks(
    cfg: config.Config,
    host: str,
    *,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> list[dict[str, object]]:
    """Whether the host's installed web console can run at all (#28).

    Uses the host's own ``vq web status`` verdict, which probes the interpreter
    recorded in the console's install marker. Nothing is added when no console
    is installed, or when the host's vq predates the verdict: no console is not
    a failure, and an older vq has nothing to say. A remote answer that does
    not arrive adds nothing either; the daemon checks own reachability.
    """
    if is_local_host(host):
        from vq.web import install as web_install  # noqa: PLC0415

        status = web_install.console_service_status()
        payload: object = {
            "installed": status.installed,
            "runtime_ok": status.runtime_ok,
            "runtime_detail": status.runtime_detail,
            "runtime_remedy": status.runtime_remedy,
        }
    else:
        try:
            host_cfg = cfg.host(host)
        except config.ConfigError:
            return []
        try:
            proc = _run_remote_vq_with_deadline(
                host_cfg,
                "web",
                "status",
                "--json",
                deadline=_CheckDeadline(check_timeout),
                subprobe="console_status",
            )
        except (_DoctorCheckTimeout, transport.RemoteError):
            return []
        if proc.returncode != 0:
            return []
        try:
            payload = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            return []
    if (
        not isinstance(payload, dict)
        or not payload.get("installed")
        or "runtime_ok" not in payload
    ):
        return []
    runtime_ok = payload.get("runtime_ok")
    detail = payload.get("runtime_detail")
    if runtime_ok is True:
        return [
            check(
                "console_runtime",
                True,
                "the installed web console's interpreter imports uvicorn and "
                "vq.web.create_app",
            )
        ]
    if runtime_ok is False:
        message = f"the installed web console cannot start: {detail}"
        remedy = payload.get("runtime_remedy")
        if remedy:
            message += f"\n{remedy}"
        return [check("console_runtime", False, message)]
    return [
        check(
            "console_runtime",
            True,
            f"web console runtime not determined: {detail or 'no verdict'}",
        )
    ]


def daemon_checks(
    cfg: config.Config,
    host: str,
    *,
    timeout: float,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> tuple[list[dict[str, object]], bool]:
    """Return daemon checks and whether the remote transport failed."""
    if is_local_host(host):
        return ping_local(timeout, check_timeout)
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        return [check("config", False, str(exc))], False
    return ping_remote(
        host_cfg,
        host_label=host,
        timeout=timeout,
        check_timeout=check_timeout,
    )


# These shapes mean no trusted remote result came back. They do not prove
# whether the remote command started; doctor only classifies reachability.
_SSH_TRANSPORT_FAILURE_MARKERS = (
    "exit 255",
    "ssh transport failed",
    "ssh transport to ",
)


def looks_like_ssh_transport_failure(message: str) -> bool:
    """Return whether a failed check describes an SSH transport failure."""
    lowered = message.lower()
    return any(
        marker in lowered for marker in _SSH_TRANSPORT_FAILURE_MARKERS
    )


def payload_hit_transport_failure(payload: object) -> bool:
    """Return whether a doctor payload failed because SSH did not connect.

    Real remote-side failures are deliberately excluded: only a transport
    failure warrants the CLI's one serial retry after a contended fan-out.
    """
    if not isinstance(payload, dict):
        return False
    checks = payload.get("checks")
    if not isinstance(checks, list):
        return False
    return any(
        isinstance(item, dict)
        and not bool(item.get("ok"))
        and looks_like_ssh_transport_failure(
            str(item.get("message") or "")
        )
        for item in checks
    )


def local_ssh_checks(
    host_cfg: config.HostConfig,
    *,
    probe_cache: dict[tuple[str, int], ssh_probe.TcpProbe],
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> tuple[
    list[dict[str, object]],
    ssh_probe.SshRoute | None,
    ssh_probe.TcpProbe | None,
    bool,
]:
    """Describe and probe the local first hop for one doctor target.

    Returns ``(checks, route, probe, blocked)``. A blocked route short-circuits
    remote checks that could only reproduce the same connection failure.
    """
    route_deadline = _CheckDeadline(check_timeout)
    try:
        route_remaining = route_deadline.remaining("ssh_route")
        route_timeout = min(
            ssh_probe.DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS,
            route_remaining,
        )
        route_outer_limited = (
            route_remaining <= ssh_probe.DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS
        )
        route = ssh_probe.resolve_route(
            host_cfg.ssh,
            timeout=route_timeout,
        )
        route_deadline.ensure_unexpired("ssh_route")
    except _DoctorCheckTimeout as exc:
        return (
            [_timed_out_check("ssh_route", exc)],
            None,
            None,
            True,
        )
    except ssh_probe.SshProbeTimeout as exc:
        if not route_outer_limited:
            return ([check("ssh_route", False, str(exc))], None, None, True)
        timeout_exc = route_deadline.timed_out("ssh_route")
        return (
            [_timed_out_check("ssh_route", timeout_exc)],
            None,
            None,
            True,
        )
    except ssh_probe.SshProbeError as exc:
        return ([check("ssh_route", False, str(exc))], None, None, True)

    checks = [check("ssh_route", True, route.describe())]
    hop = route.first_hop
    if hop is None:
        return (
            checks
            + [
                check(
                    "ssh_first_hop",
                    True,
                    f"not probed: {route.resolve_note}",
                )
            ],
            route,
            None,
            False,
        )

    key = (hop.host, hop.port)
    probe = probe_cache.get(key)
    first_hop_deadline = _CheckDeadline(check_timeout)
    if probe is None:
        try:
            probe_remaining = first_hop_deadline.remaining("ssh_first_hop")
        except _DoctorCheckTimeout as exc:
            checks.append(_timed_out_check("ssh_first_hop", exc))
            return checks, route, None, True
        probe_timeout = min(
            ssh_probe.DEFAULT_TCP_PROBE_TIMEOUT_SECONDS,
            probe_remaining,
        )
        probe_outer_limited = (
            probe_remaining <= ssh_probe.DEFAULT_TCP_PROBE_TIMEOUT_SECONDS
        )
        probe = ssh_probe.probe_tcp(
            hop.host,
            hop.port,
            timeout=probe_timeout,
        )
        probe_cache[key] = probe
        if (
            probe_outer_limited
            and
            probe.outcome == "timeout"
            and probe.elapsed_seconds >= probe_timeout * 0.99
        ):
            checks.append(
                _timed_out_check(
                    "ssh_first_hop",
                    first_hop_deadline.timed_out("ssh_first_hop"),
                )
            )
            return checks, route, probe, True
        try:
            first_hop_deadline.ensure_unexpired("ssh_first_hop")
        except _DoctorCheckTimeout as exc:
            checks.append(_timed_out_check("ssh_first_hop", exc))
            return checks, route, probe, True

    if probe.reachable:
        checks.append(
            check(
                "ssh_first_hop",
                True,
                f"{hop.describe()} reachable ({probe.elapsed_seconds:.2f}s)",
            )
        )
        return checks, route, probe, False

    verdict = ssh_probe.classify(route, probe, "")
    lines = [
        f"{hop.describe()}: {probe.detail}",
        f"verdict: {verdict.summary}",
        f"next: {verdict.next_step}",
    ]
    # A live multiplexed connection outranks the socket layer: remote checks
    # can still succeed until the established control master expires.
    try:
        master_remaining = first_hop_deadline.remaining("ssh_control_master")
        multiplexed = ssh_probe.control_master_active(
            host_cfg.ssh,
            timeout=min(
                ssh_probe.DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS,
                master_remaining,
            ),
        )
        first_hop_deadline.ensure_unexpired("ssh_control_master")
    except _DoctorCheckTimeout as exc:
        checks.append(_timed_out_check("ssh_first_hop", exc))
        return checks, route, probe, True
    if multiplexed:
        lines.append(
            "note: a multiplexed ssh connection to this host is still live, so "
            "vq may keep reaching it until that master expires. The remote "
            "checks below were run for that reason."
        )
    checks.append(check("ssh_first_hop", False, "\n".join(lines)))
    return checks, route, probe, not multiplexed


def ssh_transport_check(
    host_cfg: config.HostConfig,
    route: ssh_probe.SshRoute,
    probe: ssh_probe.TcpProbe | None,
    *,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Turn an SSH exit-255 into a named verdict from verbose evidence."""
    deadline = _CheckDeadline(check_timeout)
    try:
        remaining = deadline.remaining("ssh_transport")
    except _DoctorCheckTimeout as exc:
        return _timed_out_check("ssh_transport", exc)
    timeout = min(ssh_probe.DEFAULT_VERBOSE_PROBE_TIMEOUT_SECONDS, remaining)
    outer_limited = remaining <= ssh_probe.DEFAULT_VERBOSE_PROBE_TIMEOUT_SECONDS
    verbose = ssh_probe.verbose_probe(
        host_cfg.ssh,
        timeout=timeout,
    )
    if verbose.timed_out:
        if not outer_limited:
            return check("ssh_transport", False, verbose.error)
        return _timed_out_check(
            "ssh_transport",
            deadline.timed_out("ssh_transport"),
        )
    try:
        deadline.ensure_unexpired("ssh_transport")
    except _DoctorCheckTimeout as exc:
        return _timed_out_check("ssh_transport", exc)
    if verbose.error:
        return check("ssh_transport", False, verbose.error)
    verdict = ssh_probe.classify(
        route,
        probe,
        "\n".join(verbose.diagnostics),
        returncode=verbose.returncode,
    )
    lines = [verdict.summary, f"next: {verdict.next_step}"]
    if verdict.evidence:
        lines.append("ssh -v reported:")
        lines.extend(f"  {line}" for line in verdict.evidence)
    return check("ssh_transport", verdict.kind == "ok", "\n".join(lines))


def scheduler_probe_check_from_payload(
    payload: dict[str, object],
    host_cfg: config.HostConfig,
) -> dict[str, object]:
    """Classify scheduler-client availability from a probe payload."""
    binaries_obj = payload.get("binaries")
    binaries = binaries_obj if isinstance(binaries_obj, dict) else {}
    required = (
        ("sbatch", "squeue", "scancel", "sacct", "scontrol")
        if host_cfg.scheduler_dialect == "slurm"
        else ("qsub", "qstat", "qdel", "qhold", "qrls")
    )
    missing = [name for name in required if not bool(binaries.get(name))]
    dialect = payload.get("dialect")
    if missing:
        return check(
            "scheduler_clients",
            False,
            f"missing scheduler client(s): {', '.join(missing)}",
        )
    if dialect is not None and dialect != host_cfg.scheduler_dialect:
        return check(
            "scheduler_clients",
            False,
            f"detected {dialect!r}, config expects {host_cfg.scheduler_dialect!r}",
        )
    version = payload.get("version") or "?"
    confidence = payload.get("confidence") or "unknown"
    dialect_text = dialect or host_cfg.scheduler_dialect or "configured"
    return check(
        "scheduler_clients",
        True,
        f"{dialect_text} clients available (version={version}, {confidence})",
    )


def _scheduler_dispatch_classification(
    host: str,
    *,
    mechanism: str,
    held: list[str],
    driver_is_local: bool,
) -> dict[str, object]:
    """Attribute a scheduler-side dispatch stop to scheduler admin or vq."""
    classification: dict[str, object] = {
        "dispatching_new_jobs": False,
        "authority": "scheduler_admin",
        "mechanism": mechanism,
        "held": list(held),
        "vq_drain_active": None,
        "vq_drain_detail": None,
        "admin_update_marker_active": None,
        "admin_update_marker_envs": None,
        "admin_update_marker_started_at": None,
        "running_jobs_continue": True,
        "queued_jobs_wait": True,
        "operator_action_required": True,
    }
    if not driver_is_local:
        classification["attribution"] = (
            "partial: vq drain and marker state live on the driver; "
            "run vq doctor there"
        )
        return classification
    drain_active = False
    drain_detail: str | None = None
    try:
        state = drain_module.read_effective_drain_state()
    except Exception:  # noqa: BLE001 — diagnostics must never break doctor
        state = None
    if state is not None:
        if state.is_full_drain:
            drain_active = True
            drain_detail = "full drain"
        elif state.drains_scheduler_target(host):
            drain_active = True
            drain_detail = f"scheduler drain lane holds {host}"
    marker_active = False
    live_markers = [
        marker
        for marker in admin_module.read_admin_update_markers()
        if admin_module.admin_update_marker_stale_reason(marker) is None
    ]
    scope = admin_module.admin_update_markers_scope()
    if live_markers and (scope is None or host in scope):
        matching = [
            marker
            for marker in live_markers
            if (
                admin_module.admin_update_marker_scope(marker) is None
                or host in admin_module.admin_update_marker_scope(marker)
            )
        ]
        marker_active = True
        classification["admin_update_marker_envs"] = [
            env for marker in matching for env in marker.envs
        ]
        classification["admin_update_marker_started_at"] = min(
            marker.started_at for marker in matching
        )
    classification["vq_drain_active"] = drain_active
    classification["vq_drain_detail"] = drain_detail
    classification["admin_update_marker_active"] = marker_active
    if drain_active or marker_active:
        classification["authority"] = "scheduler_admin_and_vq"
    return classification


def _scheduler_dispatch_stop_message(
    classification: dict[str, object],
    *,
    scheduler_line: str,
    release_hint: str,
) -> str:
    parts = [scheduler_line]
    if classification.get("attribution"):
        parts.append(str(classification["attribution"]))
    elif classification["authority"] == "scheduler_admin":
        parts.append(
            "this is a scheduler-side administrative stop, NOT a vq hold "
            "(vq drain inactive, no admin-update marker for this target). "
            "Running jobs continue; the scheduler will not start queued "
            f"jobs. {release_hint}"
        )
    else:
        vq_bits = []
        if classification["vq_drain_active"]:
            vq_bits.append(str(classification["vq_drain_detail"]))
        if classification["admin_update_marker_active"]:
            vq_bits.append(
                "admin-update marker "
                f"envs={classification['admin_update_marker_envs']} "
                f"started={classification['admin_update_marker_started_at']}"
            )
        parts.append(
            "BOTH the scheduler admin and vq are holding dispatch ("
            + "; ".join(vq_bits)
            + f"). Releasing the vq hold will not lift the scheduler-side "
            f"stop. {release_hint}"
        )
    return " — ".join(parts)


def scheduler_liveness_check_from_payload(
    payload: dict[str, object],
    *,
    host: str,
    driver_is_local: bool,
) -> dict[str, object]:
    """Classify scheduler liveness and attribute administrative stops."""
    slurm_squeue_ok = payload.get("slurm_squeue_ok")
    if slurm_squeue_ok is False:
        detail = payload.get("slurm_squeue_error") or "squeue failed"
        return check(
            "scheduler_liveness",
            False,
            f"SLURM squeue failed: {detail}",
        )
    if slurm_squeue_ok is True:
        partitions_obj = payload.get("slurm_partitions_not_up")
        held = (
            [str(name) for name in partitions_obj]
            if isinstance(partitions_obj, list)
            else None
        )
        if held:
            classification = _scheduler_dispatch_classification(
                host,
                mechanism="slurm_partition_not_up",
                held=held,
                driver_is_local=driver_is_local,
            )
            result = check(
                "scheduler_liveness",
                False,
                _scheduler_dispatch_stop_message(
                    classification,
                    scheduler_line=(
                        "SLURM partition(s) not up: " + ", ".join(held)
                    ),
                    release_hint=(
                        "Re-enabling a partition is the SLURM "
                        "administrator's call; `vq drain --release` cannot "
                        "do it."
                    ),
                ),
            )
            result["scheduler_dispatch"] = classification
            return result
        return check(
            "scheduler_liveness",
            True,
            "SLURM squeue reachable"
            + (
                "; partition availability unreadable (no attribution claimed)"
                if held is None
                else "; all partitions up"
            ),
        )

    server_state = payload.get("server_state")
    pbs_sched_running = payload.get("pbs_sched_running")
    daemons_obj = payload.get("scheduler_daemons")
    daemons = (
        [str(name) for name in daemons_obj]
        if isinstance(daemons_obj, list)
        else None
    )
    alt_daemons = [name for name in (daemons or []) if name != "pbs_sched"]
    queues_obj = payload.get("queues")
    queues = queues_obj if isinstance(queues_obj, list) else []
    stopped = []
    for item in queues:
        if not isinstance(item, dict):
            continue
        if item.get("enabled") is True and item.get("started") is False:
            stopped.append(str(item.get("name") or "?"))

    if pbs_sched_running is False and not alt_daemons:
        return check(
            "scheduler_liveness",
            False,
            "pbs_sched is not running and no alternative scheduler daemon "
            f"(maui/moab) was found; server_state={server_state or '?'}",
        )
    if stopped:
        classification = _scheduler_dispatch_classification(
            host,
            mechanism="pbs_queue_not_started",
            held=stopped,
            driver_is_local=driver_is_local,
        )
        result = check(
            "scheduler_liveness",
            False,
            _scheduler_dispatch_stop_message(
                classification,
                scheduler_line=(
                    "PBS queue(s) enabled but not started: "
                    + ", ".join(stopped)
                ),
                release_hint=(
                    "Do not run qstart unless the scheduler administrator "
                    "authorizes it; `vq drain --release` cannot start PBS "
                    "queues."
                ),
            ),
        )
        result["scheduler_dispatch"] = classification
        return result
    if pbs_sched_running is True or alt_daemons:
        if pbs_sched_running is True:
            parts = ["pbs_sched running"]
        else:
            parts = [
                ", ".join(alt_daemons)
                + " scheduler running (external scheduler drives dispatch)"
            ]
        if server_state is not None:
            parts.append(f"server_state={server_state}")
        if queues:
            parts.append(f"queues={len(queues)}")
        return check("scheduler_liveness", True, "; ".join(parts))
    return check(
        "scheduler_liveness",
        True,
        "scheduler liveness unavailable from probe; client checks passed",
    )


class _DoctorSchedulerProbeRunner:
    """Scheduler-probe runner whose shell calls share one doctor deadline."""

    def __init__(
        self,
        host_cfg: config.HostConfig,
        deadline: _CheckDeadline,
    ) -> None:
        self._host_cfg = host_cfg
        self._deadline = deadline

    def run(
        self,
        argv: Sequence[str],
        *,
        stdin_data: str | None = None,
        check: bool = False,
    ) -> RemoteResult:
        try:
            proc = _run_remote_shell_with_deadline(
                self._host_cfg,
                *argv,
                deadline=self._deadline,
                subprobe="scheduler_probe",
                stdin_data=stdin_data,
                check_result=check,
            )
        except _DoctorCheckTimeout:
            raise
        except transport.RemoteError as exc:
            raise SchedulerError(
                f"remote scheduler command failed on {self._host_cfg.ssh}: "
                f"{shlex.join(argv)}\n  {exc}"
            ) from exc
        if proc.returncode == 255:
            raise SchedulerError(
                "ssh transport failed for scheduler command on "
                f"{self._host_cfg.ssh}: {shlex.join(argv)}\n"
                f"  stderr: {proc.stderr.strip() or '(empty)'}"
            )
        return RemoteResult(
            returncode=proc.returncode,
            stdout=proc.stdout,
            stderr=proc.stderr,
        )


def scheduler_probe_checks(
    cfg: config.Config,
    host: str,
    host_cfg: config.HostConfig,
    *,
    driver: str,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> list[dict[str, object]]:
    """Run the scheduler-client preflight locally or through its driver."""
    deadline = _CheckDeadline(check_timeout)
    try:
        if is_local_host(driver):
            from vq import scheduler_probe as probe_mod  # noqa: PLC0415

            result = probe_mod.probe(
                cast(
                    RemoteRunner,
                    _DoctorSchedulerProbeRunner(host_cfg, deadline),
                )
            )
            payload = probe_mod.to_json_dict(result)
            return [
                scheduler_probe_check_from_payload(payload, host_cfg),
                scheduler_liveness_check_from_payload(
                    payload, host=host, driver_is_local=True
                ),
            ]
        driver_cfg = cfg.host(driver)
        proc = _run_remote_vq_with_deadline(
            driver_cfg,
            "scheduler-probe",
            host,
            "--json",
            deadline=deadline,
            subprobe="scheduler_probe",
        )
    except _DoctorCheckTimeout as exc:
        return [
            _timed_out_check("scheduler_clients", exc),
            check(
                "scheduler_liveness",
                False,
                "not checked; probe timed out",
            ),
        ]
    except (config.ConfigError, SchedulerError, transport.RemoteError) as exc:
        return [
            check("scheduler_clients", False, str(exc)),
            check("scheduler_liveness", False, "not checked; probe failed"),
        ]

    if proc.returncode != 0:
        detail = (
            proc.stderr.strip()
            or proc.stdout.strip()
            or f"exit {proc.returncode}"
        )
        return [
            check("scheduler_clients", False, detail),
            check("scheduler_liveness", False, "not checked; probe failed"),
        ]
    try:
        payload = json.loads(proc.stdout or "{}")
    except ValueError as exc:
        return [
            check(
                "scheduler_clients",
                False,
                f"scheduler-probe returned invalid JSON: {exc}",
            ),
            check("scheduler_liveness", False, "not checked; probe failed"),
        ]
    if not isinstance(payload, dict):
        return [
            check(
                "scheduler_clients",
                False,
                "scheduler-probe returned non-object JSON",
            ),
            check("scheduler_liveness", False, "not checked; probe failed"),
        ]
    return [
        scheduler_probe_check_from_payload(payload, host_cfg),
        scheduler_liveness_check_from_payload(
            payload, host=host, driver_is_local=False
        ),
    ]


# Helpers newer than this release answer ``vq source-identity``: version,
# package digest and SOURCE-SHA in ONE remote round trip. Older helpers are
# asked the legacy pair (``source-tree-sha256`` then ``source-sha``), which is
# two more python start-ups on the login node. A helper the gate misjudges
# answers "No such command" and falls back to the legacy pair, so the gate
# only decides how many round trips the check costs, never its verdict.
_HELPER_SOURCE_IDENTITY_SINCE = (0, 26, 0)

# What one helper identity subprobe is called in the structured timeout
# verdict and in the message of a failed check.
_HELPER_SOURCE_IDENTITY_SUBPROBE = "helper_source_identity"


def _parse_helper_version(version: str | None) -> tuple[int, ...] | None:
    """Return the leading numeric release tuple of a helper version, if any."""
    if version is None:
        return None
    match = re.match(r"(\d+(?:\.\d+)*)", version)
    if match is None:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def _helper_answers_source_identity(version: str | None) -> bool:
    parsed = _parse_helper_version(version)
    return parsed is not None and parsed > _HELPER_SOURCE_IDENTITY_SINCE


def _looks_like_unknown_command(proc: subprocess.CompletedProcess[str]) -> bool:
    """Recognize click's usage error for a subcommand this vq predates."""
    if proc.returncode != 2:
        return False
    combined = f"{proc.stderr or ''}\n{proc.stdout or ''}".lower()
    return "no such command" in combined


@dataclass(frozen=True)
class _HelperIdentity:
    """What the helper answered about its own bytes and its declaration.

    ``tree``/``sha`` are the lower-cased values the helper printed (empty when
    it printed none); ``tree_ok``/``sha_ok`` say whether each read succeeded.
    ``sha_failure`` is the detail appended to the missing-marker hint.
    """

    tree: str
    tree_ok: bool
    sha: str
    sha_ok: bool
    sha_failure: str


class _HelperProbeFailed(Exception):
    """A helper identity subprobe finished with a verdict instead of a value."""

    def __init__(self, verdict: dict[str, object]) -> None:
        self.verdict = verdict
        super().__init__(str(verdict.get("message")))


class _HelperCommandUnknown(Exception):
    """The helper predates ``source-identity``; ask the legacy pair instead."""


def _first_line(output: str | None) -> str:
    stripped = (output or "").strip()
    return stripped.splitlines()[0].strip() if stripped else ""


def _read_helper_identity_one_shot(
    host_cfg: config.HostConfig,
    *,
    deadline: _CheckDeadline,
    prefix: str,
    helper_version: str | None,
) -> _HelperIdentity:
    """Read version, digest and SOURCE-SHA from ``vq source-identity``."""
    try:
        proc = _run_remote_vq_with_deadline(
            host_cfg,
            "source-identity",
            deadline=deadline,
            subprobe=_HELPER_SOURCE_IDENTITY_SUBPROBE,
            inner_timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except transport.RemoteError as exc:
        raise _HelperProbeFailed(
            check(
                "scheduler_remote_vq",
                False,
                f"{prefix}; source-identity check failed: {exc}",
                version=helper_version,
                **_timeout_metadata(exc, subprobe=_HELPER_SOURCE_IDENTITY_SUBPROBE),
            )
        ) from exc
    if _looks_like_unknown_command(proc):
        raise _HelperCommandUnknown
    if proc.returncode != 0:
        raise _HelperProbeFailed(
            check(
                "scheduler_remote_vq",
                False,
                (
                    f"{prefix}; source-identity exit {proc.returncode}: "
                    f"{_first_line(proc.stderr or proc.stdout) or '(no output)'}"
                ),
                version=helper_version,
            )
        )
    try:
        payload = json.loads(proc.stdout or "")
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        raise _HelperProbeFailed(
            check(
                "scheduler_remote_vq",
                False,
                f"{prefix}; source-identity returned malformed output",
                version=helper_version,
            )
        )
    tree = payload.get("source_tree_sha256")
    sha = payload.get("source_sha")
    sha_error = payload.get("source_sha_error")
    return _HelperIdentity(
        tree=tree.strip().lower() if isinstance(tree, str) else "",
        tree_ok=isinstance(tree, str) and bool(tree.strip()),
        sha=sha.strip().lower() if isinstance(sha, str) else "",
        sha_ok=isinstance(sha, str) and bool(sha.strip()),
        sha_failure=(
            "source-identity reported no SOURCE-SHA: "
            f"{sha_error if isinstance(sha_error, str) and sha_error else '(no detail)'}"
        ),
    )


def _read_helper_identity_legacy(
    host_cfg: config.HostConfig,
    *,
    deadline: _CheckDeadline,
    prefix: str,
    helper_version: str | None,
) -> _HelperIdentity:
    """Read the digest and SOURCE-SHA with one remote call each."""
    try:
        tree_proc = _run_remote_vq_with_deadline(
            host_cfg,
            "source-tree-sha256",
            deadline=deadline,
            subprobe="helper_source_tree_sha256",
            inner_timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except transport.RemoteError as exc:
        raise _HelperProbeFailed(
            check(
                "scheduler_remote_vq",
                False,
                f"{prefix}; source-tree check failed: {exc}",
                **_timeout_metadata(exc, subprobe="helper_source_tree_sha256"),
            )
        ) from exc
    helper_tree = _first_line(tree_proc.stdout or tree_proc.stderr).lower()
    # Always read SOURCE-SHA before classifying a tree mismatch. Rollout
    # planning still needs the helper's exact live identity when the
    # driver checkout has already moved ahead of the deployed fleet.
    try:
        sha_proc = _run_remote_vq_with_deadline(
            host_cfg,
            "source-sha",
            deadline=deadline,
            subprobe="helper_source_sha",
            inner_timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except transport.RemoteError as exc:
        # pbs-cluster's case: three remote vq calls share one budget and this, the
        # third, is the one that runs out. An outer-limited call already
        # surfaced as the deadline's own verdict above; a plain inner stall
        # is still marked, so a caller can tell "the helper is wrong" from
        # "we did not find out".
        raise _HelperProbeFailed(
            check(
                "scheduler_remote_vq",
                False,
                f"{prefix}; SOURCE-SHA check failed: {exc}",
                version=helper_version,
                source_tree_sha256=helper_tree or None,
                **_timeout_metadata(exc, subprobe="helper_source_sha"),
            )
        ) from exc
    sha_output = (sha_proc.stdout or sha_proc.stderr or "").strip()
    helper_sha = _first_line(sha_output).lower()
    if sha_proc.returncode != 0:
        helper_sha = ""
    return _HelperIdentity(
        tree=helper_tree,
        tree_ok=tree_proc.returncode == 0,
        sha=helper_sha,
        sha_ok=sha_proc.returncode == 0,
        sha_failure=(
            f"source-sha exit {sha_proc.returncode}: "
            f"{sha_output or '(no output)'}"
        ),
    )


def _read_helper_identity(
    host_cfg: config.HostConfig,
    *,
    deadline: _CheckDeadline,
    prefix: str,
    helper_version: str | None,
) -> _HelperIdentity:
    """Read the helper's identity in as few remote round trips as it allows."""
    if _helper_answers_source_identity(helper_version):
        try:
            return _read_helper_identity_one_shot(
                host_cfg,
                deadline=deadline,
                prefix=prefix,
                helper_version=helper_version,
            )
        except _HelperCommandUnknown:
            pass
    return _read_helper_identity_legacy(
        host_cfg,
        deadline=deadline,
        prefix=prefix,
        helper_version=helper_version,
    )


def _classify_helper_identity(
    host_cfg: config.HostConfig,
    *,
    deadline: _CheckDeadline,
    prefix: str,
    helper_version: str | None,
) -> dict[str, object]:
    """Compare the helper's live identity with the driver's own."""
    expected_sha, _sha_error = _driver_identity_probe("source_sha", deadline)
    if expected_sha is None:
        return check(
            "scheduler_remote_vq",
            False,
            (
                f"{prefix}; driver source SHA unavailable, cannot verify "
                "scheduler helper provenance"
            ),
        )
    expected_tree, tree_error = _driver_identity_probe(
        "source_tree_sha256",
        deadline,
    )
    if expected_tree is None:
        return check(
            "scheduler_remote_vq",
            False,
            (
                f"{prefix}; driver source-tree digest unavailable: "
                f"{tree_error or '(no detail)'}"
            ),
        )
    try:
        identity = _read_helper_identity(
            host_cfg,
            deadline=deadline,
            prefix=prefix,
            helper_version=helper_version,
        )
    except _HelperProbeFailed as exc:
        return exc.verdict
    if not identity.tree_ok or identity.tree != expected_tree:
        return check(
            "scheduler_remote_vq",
            False,
            (
                f"{prefix}; source-tree SHA-256 mismatch: helper "
                f"{identity.tree or '(missing)'}, driver {expected_tree}"
            ),
            version=helper_version,
            source_sha=identity.sha or None,
            source_tree_sha256=identity.tree or None,
        )
    if not identity.sha_ok:
        marker_hint = (
            "no SOURCE-SHA marker reported by scheduler helper; this "
            "usually means the helper predates the provenance contract or "
            "was installed outside `vq admin update <scheduler-host>`"
        )
        return check(
            "scheduler_remote_vq",
            False,
            f"{prefix}; {marker_hint}; {identity.sha_failure}",
        )
    if identity.sha != expected_sha:
        return check(
            "scheduler_remote_vq",
            False,
            (
                f"{prefix}; SOURCE-SHA mismatch: helper {identity.sha}, "
                f"driver {expected_sha}"
            ),
            version=helper_version,
            source_sha=identity.sha or None,
            source_tree_sha256=identity.tree,
        )
    return check(
        "scheduler_remote_vq",
        True,
        (
            f"{prefix}; source-tree SHA-256 {identity.tree} and SOURCE-SHA "
            f"{identity.sha} match driver"
        ),
        version=helper_version,
        source_sha=identity.sha,
        source_tree_sha256=identity.tree,
    )


def scheduler_remote_vq_check(
    host_cfg: config.HostConfig,
    *,
    host_label: str,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
) -> dict[str, object]:
    """Verify the scheduler helper's version and source identity.

    Every subprocess shares one ``check_timeout`` budget: the remote
    ``--version``, the two owned driver identity probes, and the helper
    identity read. A helper newer than :data:`_HELPER_SOURCE_IDENTITY_SINCE`
    answers that read in one round trip; an older one costs two. On a login
    node that needs 2.5 s per python start-up the legacy shape sits right at
    the default budget, which is what ``[fleet] check_timeout_seconds`` is
    for on the rollout sweep.
    """
    deadline = _CheckDeadline(check_timeout)
    try:
        proc = _run_remote_vq_with_deadline(
            host_cfg,
            "--version",
            deadline=deadline,
            subprobe="helper_version",
            inner_timeout=transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
        )
    except _DoctorCheckTimeout as exc:
        return _timed_out_check("scheduler_remote_vq", exc)
    except transport.RemoteError as exc:
        return _remote_failure_check(
            "scheduler_remote_vq", exc, subprobe="helper_version",
        )

    output = (proc.stdout or proc.stderr or "").strip()
    detail = output.splitlines()[0] if output else "(no output)"
    version_match = re.search(r"\bversion\s+([^\s,;]+)", detail)
    helper_version = (
        version_match.group(1) if version_match is not None else None
    )
    if proc.returncode == 0:
        try:
            return _classify_helper_identity(
                host_cfg,
                deadline=deadline,
                prefix=f"{host_cfg.remote_vq} available on {host_cfg.ssh}: {detail}",
                helper_version=helper_version,
            )
        except _DoctorCheckTimeout as exc:
            return _timed_out_check("scheduler_remote_vq", exc)
    if proc.returncode == 127:
        return check(
            "scheduler_remote_vq",
            False,
            remote_vq_nonzero_message(
                host_cfg,
                proc,
                host_label=host_label,
            ),
        )
    return check(
        "scheduler_remote_vq",
        False,
        f"{host_cfg.remote_vq} failed on {host_cfg.ssh} "
        f"(exit {proc.returncode}): {detail}",
    )


def scheduler_admin_update_check(
    host: str,
    host_cfg: config.HostConfig,
) -> dict[str, object]:
    """Check whether admin update is configured for a scheduler host."""
    if host_cfg.scheduler_update_command is None:
        return check(
            "scheduler_admin_update",
            False,
            (
                "no scheduler_update_command configured; "
                f"`vq admin update {host}` is unavailable"
            ),
        )
    install_note = (
        "scheduler_install_command configured"
        if host_cfg.scheduler_install_command is not None
        else "no scheduler_install_command; --cluster-install unavailable"
    )
    update_host_note = (
        f"; update host {host_cfg.scheduler_update_host}"
        if host_cfg.scheduler_update_host is not None
        else ""
    )
    return check(
        "scheduler_admin_update",
        True,
        f"scheduler_update_command configured; {install_note}{update_host_note}",
    )


def scheduler_program_hooks_check(
    cfg: config.Config,
    host_cfg: config.HostConfig,
) -> dict[str, object]:
    """Check that scheduler per-program hook keys name configured programs."""
    hooks = host_cfg.scheduler_program_hooks
    if not hooks:
        return check("scheduler_program_hooks", True, "none configured")
    missing = sorted(name for name in hooks if name not in cfg.programs)
    if missing:
        return check(
            "scheduler_program_hooks",
            False,
            (
                "hook(s) for unknown program(s): "
                f"{', '.join(missing)}; add matching [programs.NAME] "
                "entries or fix the hook key"
            ),
        )
    return check(
        "scheduler_program_hooks",
        True,
        f"configured for: {', '.join(sorted(hooks))}",
    )


def scheduler_command_wrapper_check(
    host_cfg: config.HostConfig,
) -> dict[str, object]:
    """Check that no command wrapper duplicates the job interpreter."""
    hooks = host_cfg.scheduler_program_hooks
    wrapped = {
        name: hook.command_wrapper[0]
        for name, hook in hooks.items()
        if hook.command_wrapper
    }
    if not wrapped:
        return check(
            "scheduler_command_wrapper", True, "no command_wrapper configured"
        )
    interpreters: dict[str, str] = {
        f"branches.{key}": value for key, value in host_cfg.branches.items()
    }
    if host_cfg.remote_python:
        interpreters["remote_python"] = host_cfg.remote_python
    collisions = [
        f"scheduler_program_hooks.{name}.command_wrapper[0] ({wrapper}) "
        f"resolves to the same program as {field} ({interp})"
        for name, wrapper in sorted(wrapped.items())
        for field, interp in sorted(interpreters.items())
        if wrapper_already_applied([wrapper], [interp])
    ]
    if collisions:
        return check(
            "scheduler_command_wrapper",
            False,
            (
                "; ".join(collisions)
                + ". A submitted command already starts with its interpreter, "
                "so the wrapper would double-wrap it — vq de-duplicates at "
                "dispatch, but configure one or the other, not both"
            ),
        )
    return check(
        "scheduler_command_wrapper",
        True,
        f"configured for: {', '.join(sorted(wrapped))}",
    )


def scheduler_runtime_deployments_check(
    cfg: config.Config,
    host_cfg: config.HostConfig,
) -> dict[str, object]:
    """Check that scheduler runtime deployments name configured programs."""
    deployments = host_cfg.scheduler_runtime_deployments
    if not deployments:
        return check(
            "scheduler_runtime_deployments",
            True,
            "none configured",
        )
    missing = sorted(name for name in deployments if name not in cfg.programs)
    if missing:
        return check(
            "scheduler_runtime_deployments",
            False,
            "deployment(s) for unknown program(s): " + ", ".join(missing),
        )
    return check(
        "scheduler_runtime_deployments",
        True,
        "configured for: " + ", ".join(sorted(deployments)),
    )


def diagnose_host(
    cfg: config.Config,
    host: str,
    *,
    timeout: float,
    check_timeout: float = DEFAULT_CHECK_TIMEOUT_SECONDS,
    admin_update: bool = False,
    probe_cache: dict[tuple[str, int], ssh_probe.TcpProbe] | None = None,
    as_driver: str | None = None,
) -> dict[str, object]:
    """Run the read-only doctor checks for one configured host."""
    checks: list[dict[str, object]] = []
    host_cfg: config.HostConfig | None = None
    scheduler = "local"
    driver: str | None = None
    route: ssh_probe.SshRoute | None = None
    probe: ssh_probe.TcpProbe | None = None
    transport_failed = False
    # Checks that crossed THIS host's ssh, as opposed to a scheduler driver's.
    # Only these may trigger the ssh -v verdict, which is run against
    # ``host_cfg.ssh`` and would otherwise describe the wrong destination.
    own_ssh_checks: list[dict[str, object]] = []
    if probe_cache is None:
        probe_cache = {}

    down = host_status.is_down(host)
    if down is not None:
        checks.append(
            check("admin_down", False, f"marked down: {down.describe()}")
        )

    if is_local_host(host) and host not in cfg.hosts:
        checks.append(check("config", True, "implicit localhost"))
    else:
        try:
            host_cfg = cfg.host(host)
        except config.ConfigError as exc:
            checks.append(check("config", False, str(exc)))
            return {
                "host": host,
                "ok": False,
                "scheduler": scheduler,
                "driver": driver,
                "checks": checks,
            }
        scheduler = host_cfg.scheduler
        checks.append(
            check(
                "config",
                True,
                f"configured ssh={host_cfg.ssh!r} scheduler={scheduler!r}",
            )
        )

    if down is not None:
        payload: dict[str, object] = {
            "host": host,
            "ok": False,
            "scheduler": scheduler,
            "driver": driver,
            "checks": checks,
        }
        if host_cfg is not None:
            lane = host_cfg.scheduler_lane_metadata()
            if lane is not None:
                payload["scheduler_lane"] = lane
        return payload

    # Resolve and probe the local first hop before remote diagnostics so a dead
    # link or bastion is named once instead of repeated by every remote check.
    if host_cfg is not None and not is_local_host(host):
        local_checks, route, probe, blocked = local_ssh_checks(
            host_cfg,
            probe_cache=probe_cache,
            check_timeout=check_timeout,
        )
        checks.extend(local_checks)
        if blocked:
            payload = {
                "host": host,
                "ok": False,
                "scheduler": scheduler,
                "driver": driver,
                "checks": checks,
            }
            lane = host_cfg.scheduler_lane_metadata()
            if lane is not None:
                payload["scheduler_lane"] = lane
            return payload

    if host_cfg is not None and host_cfg.scheduler != "local":
        driver = host_cfg.scheduler_driver
        checks.append(
            check(
                "scheduler",
                True,
                f"daemonless scheduler host; driver={driver!r}",
            )
        )
        checks.append(scheduler_program_hooks_check(cfg, host_cfg))
        checks.append(scheduler_command_wrapper_check(host_cfg))
        # This check crosses the scheduler host's own SSH route. Driver-side
        # failures below belong to the driver's separate route and diagnosis.
        own_ssh_checks.append(
            scheduler_remote_vq_check(
                host_cfg,
                host_label=host,
                check_timeout=check_timeout,
            )
        )
        checks.extend(own_ssh_checks[-1:])
        if admin_update:
            checks.append(scheduler_admin_update_check(host, host_cfg))
            checks.append(scheduler_runtime_deployments_check(cfg, host_cfg))

        # A candidate replaces the configured driver for every driver-side
        # check without changing configuration, so a migration can be
        # rehearsed before the scheduler_driver value is repointed.
        effective_driver = as_driver if as_driver is not None else driver
        if effective_driver is None:
            checks.append(check("scheduler_driver", False, "no scheduler_driver"))
        else:
            try:
                cfg.host(effective_driver)
            except config.ConfigError as exc:
                checks.append(check("scheduler_driver", False, str(exc)))
            else:
                checks.append(
                    check(
                        "scheduler_driver",
                        True,
                        f"driver host {effective_driver!r} is configured"
                        if as_driver is None
                        else (
                            f"evaluating candidate driver {as_driver!r} "
                            f"(configured driver: {driver!r})"
                        ),
                    )
                )
                driver_checks, _driver_transport_failed = daemon_checks(
                    cfg,
                    effective_driver,
                    timeout=timeout,
                    check_timeout=check_timeout,
                )
                checks.extend(driver_checks)
                checks.extend(
                    scheduler_probe_checks(
                        cfg,
                        host,
                        host_cfg,
                        driver=effective_driver,
                        check_timeout=check_timeout,
                    )
                )
    else:
        host_checks, transport_failed = daemon_checks(
            cfg,
            host,
            timeout=timeout,
            check_timeout=check_timeout,
        )
        checks.extend(host_checks)
        own_ssh_checks.extend(host_checks)
        # Ask about the console only where a vq answered: a remote whose vq
        # failed or timed out is already diagnosed, and one more round trip
        # would only repeat that verdict (#28).
        remote_vq_answered = any(
            item["name"] == "remote_vq" and bool(item["ok"]) for item in host_checks
        )
        if is_local_host(host) or remote_vq_answered:
            checks.extend(
                console_runtime_checks(cfg, host, check_timeout=check_timeout)
            )

    # If the first hop answered but SSH later failed at the transport layer,
    # spend one verbose probe to classify the broken link. RemoteError checks
    # carry exit 255 in their messages; daemon ping reports this structurally.
    if (
        host_cfg is not None
        and route is not None
        and (
            transport_failed
            or any(
                not bool(item["ok"])
                and looks_like_ssh_transport_failure(str(item["message"]))
                for item in own_ssh_checks
            )
        )
    ):
        checks.append(
            ssh_transport_check(
                host_cfg,
                route,
                probe,
                check_timeout=check_timeout,
            )
        )

    payload: dict[str, object] = {
        "host": host,
        "ok": all(bool(item["ok"]) for item in checks),
        "scheduler": scheduler,
        "driver": driver,
        "checks": checks,
    }
    if as_driver is not None:
        payload["as_driver"] = as_driver
    if host_cfg is not None:
        lane = host_cfg.scheduler_lane_metadata()
        if lane is not None:
            payload["scheduler_lane"] = lane
    return payload
