"""Daemon lifecycle health checks (v0.5.49, first piece of v0.6.0 backbone).

Cross-checks the four sources of truth about who-owns-the-daemon, so
operators stop guessing when something looks wrong. The 2026-05-17
workstation incident — user-systemd process detached from its service unit,
``systemctl --user`` returning "Transport endpoint is not connected",
``vq --version`` reporting the on-disk version while no daemon was
actually running — would have been a 3-second diagnosis with this
module's :func:`verify_user_systemd_contract`.

Sources cross-checked:

1. **loginctl show-user $USER** — PAM/login layer's view of the user
   session. Should report ``State=active`` with a ``RuntimePath`` set.
2. **pgrep 'systemd --user'** — does a user-manager process actually
   exist in the process table? A live PID here is the authoritative
   "is the user manager actually running" signal.
3. **systemctl --user show vq-daemon.service** — systemd's view of the
   daemon: ``ActiveState`` (active/inactive/failed/…) and ``MainPID``.
4. **<state_root>/daemon.pid** — the daemon's own pidfile, written by
   :func:`vq.daemon_control.write_pidfile`.

When all four agree and the daemon process is alive, the contract is
intact. Any disagreement adds a ``FAIL`` finding and flips ``ok`` to
False; ``WARN`` and ``INFO`` findings cover degraded-but-recoverable
states.

On a host with no user systemd manager at all (macOS, or any
non-systemd box — the daemon is started directly via ``vq daemon run``
there), the four-source contract is meaningless: every systemd probe
reports "absent" and would FAIL a perfectly healthy daemon. In that
case :func:`verify_user_systemd_contract` falls back to RPC-socket
liveness (:func:`_verify_via_rpc_liveness`) — the daemon is ``ok`` iff
it answers ``ping`` on its socket, the same signal ``vq queue`` and job
dispatch rely on.

Strictly read-only: this module never restarts, kills, or reconfigures
anything. The companion CLI verb (``vq daemon health``) just renders
the verdict. Acting on the diagnosis is the operator's call.
"""
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import asdict, dataclass, field

from vq import daemon_control

_SUBPROCESS_TIMEOUT_SECONDS = 5.0
"""Cap on every external probe (pgrep / loginctl / systemctl). Short
because all four are local syscalls / unix-socket roundtrips that
should return in milliseconds; a hang here usually means the user
manager is the wedged thing we're trying to diagnose."""


@dataclass
class ContractVerdict:
    """Structured result of :func:`verify_user_systemd_contract`.

    The verdict captures one snapshot from each of the four sources +
    a list of human-readable findings. ``ok`` is False iff any
    finding starts with ``FAIL``; ``WARN`` and ``INFO`` lines are
    informational and don't fail the verdict.
    """

    ok: bool
    manager_pid: int | None
    """systemd --user process PID from pgrep, or None if no manager
    is running."""
    loginctl_state: str | None
    """``State=`` field from ``loginctl show-user``. ``"active"`` is
    the healthy value; ``"closing"``/``"online"`` are transitional;
    None means loginctl was unavailable or returned nothing."""
    loginctl_runtime_path: str | None
    """``RuntimePath=`` field. Should be ``/run/user/<UID>``."""
    systemctl_user_reachable: bool
    """True iff ``systemctl --user show vq-daemon.service`` returned
    rc=0. False is the canonical "user-systemd is broken" signal —
    every subsequent systemctl-derived field will be None when this
    is False."""
    vq_daemon_state: str | None
    """``ActiveState`` from systemd (active/inactive/failed/…), or
    None when systemctl is unreachable."""
    vq_daemon_main_pid: int | None
    """``MainPID`` from systemd. 0 (= unit inactive) is reported as
    None for cleanliness."""
    daemon_pidfile_pid: int | None
    """PID from ``<state_root>/daemon.pid``, or None if no pidfile.
    Pre-v0.6.0 the daemon may be started without a pidfile (via the
    systemd unit's ExecStart) — None here is informational, not a
    failure."""
    daemon_process_alive: bool | None
    """Result of ``kill(pid, 0)`` against ``vq_daemon_main_pid`` (or
    the pidfile pid as fallback). None when no candidate pid was
    available; True/False reflects whether that pid is in the process
    table."""
    memory_pressure_pct: float | None = None
    """v0.6.21: snapshot of host memory pressure at health-check time
    from ``/proc/meminfo``: ``100 * (MemTotal - MemAvailable) / MemTotal``.
    None on macOS / no /proc / parse failure. Higher = more pressure;
    the v0.6.20 watchdog auto-pauses jobs at 85% by default. Useful
    so a single ``vq daemon health`` call answers both 'is the
    daemon healthy?' and 'is the host under memory pressure?' —
    saves a second SSH call from monitoring tools."""
    drain_active: bool = False
    """v0.6.23: True iff a non-expired ``drain.json`` is in effect on
    this host (operator ran ``vq drain`` and either it's full-drain
    or partial-cap-override mode). Surfaced so a single
    ``vq daemon health`` answers 'is the daemon dispatching at all?'
    without a second `vq drain --status` round trip."""
    drain_reason: str | None = None
    """v0.6.23: operator-supplied free-text label for the drain
    (``vq drain --reason 'kids gaming'``), or None when no drain or
    no reason was given. Only meaningful when ``drain_active=True``."""
    findings: list[str] = field(default_factory=list)
    """Per-check finding lines tagged ``OK:`` / ``WARN:`` / ``FAIL:``
    / ``INFO:``. Caller renders them via
    :func:`format_contract_verdict`."""


def _pgrep_systemd_user() -> int | None:
    """Return the PID of the current user's user-systemd manager, or
    None if no such process is running. Uses ``pgrep -u <uid> -f
    'systemd --user'`` so we only see THIS user's manager (not a
    different user's, in shared-host scenarios)."""
    try:
        proc = subprocess.run(
            ["pgrep", "-u", str(os.getuid()), "-f", "systemd --user"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        try:
            return int(line.strip())
        except ValueError:
            continue
    return None


def _loginctl_user_state() -> tuple[str | None, str | None]:
    """Return ``(State, RuntimePath)`` from
    ``loginctl show-user <uid>``, or ``(None, None)`` on any failure
    (loginctl not present, no session for this user, timeout)."""
    try:
        proc = subprocess.run(
            ["loginctl", "show-user", str(os.getuid()),
             "-p", "State", "-p", "RuntimePath"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return None, None
    if proc.returncode != 0:
        return None, None
    state: str | None = None
    runtime_path: str | None = None
    for line in proc.stdout.splitlines():
        if line.startswith("State="):
            state = line.split("=", 1)[1].strip() or None
        elif line.startswith("RuntimePath="):
            runtime_path = line.split("=", 1)[1].strip() or None
    return state, runtime_path


def _systemctl_user_show(unit: str, prop: str) -> tuple[int, str]:
    """Thin wrapper around ``systemctl --user show <unit> -p <prop>
    --value`` that swallows OSError / TimeoutExpired (returning
    ``(1, "")`` so callers can treat any failure as
    "user-systemd unreachable")."""
    try:
        proc = subprocess.run(
            ["systemctl", "--user", "show", unit, "-p", prop, "--value"],
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        return 1, ""
    return proc.returncode, proc.stdout or ""


def _systemctl_user_reachable() -> bool:
    """True iff we can talk to the user manager. Probes by asking for
    a trivially-cheap property; a zombie / orphan manager returns
    rc != 0 with "Transport endpoint is not connected" or similar."""
    rc, _ = _systemctl_user_show("vq-daemon.service", "ActiveState")
    return rc == 0


def _pid_alive(pid: int | None) -> bool | None:
    """``True`` if pid is alive, ``False`` if not, ``None`` if pid is
    None (no candidate to check). ``PermissionError`` is treated as
    True (process exists but we don't own it — for our purposes "in
    the process table" is what we're measuring)."""
    if pid is None:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _memory_pressure_finding() -> tuple[float | None, str | None]:
    """v0.6.21: snapshot host memory pressure for the verdict. Pure
    ``/proc/meminfo`` read; None on macOS / no /proc. Shared by both the
    systemd-contract path and the RPC-liveness fall-back so the verdict
    surface is identical across platforms.

    Returns ``(pct, finding_line)``. ``pct`` is None when the probe is
    unavailable; ``finding_line`` is None when there's nothing to report
    (so the caller appends only a real line). Pressure is informational —
    it never fails the verdict (the v0.6.20 watchdog already handles
    high-pressure response); the WARN at 80% just fires before the 85%
    auto-pause so an operator inspecting health gets a heads-up first.
    """
    try:
        from vq.watchdog import read_host_memory_pressure_pct
        pct = read_host_memory_pressure_pct()
    except Exception:  # pragma: no cover — defensive
        return None, None
    if pct is None:
        return None, None
    if pct >= 80.0:
        return pct, (
            f"WARN: host memory pressure {pct:.1f}% "
            f"is high — v0.6.20 watchdog will auto-pause running "
            f"jobs at 85% to avoid OOM cascade"
        )
    return pct, f"OK: host memory pressure {pct:.1f}%"


def _drain_finding() -> tuple[bool, str | None, str | None]:
    """v0.6.23: read the persisted drain state for the verdict. Expired
    drains are silently cleared by ``read_drain_state()`` so there's no
    false-positive. Shared by both verdict paths.

    Returns ``(drain_active, drain_reason, finding_line)``; the finding
    is None when no drain is in effect.
    """
    try:
        from vq.drain import read_effective_drain_state
        drain = read_effective_drain_state()
        if drain is not None and drain.enabled:
            line = (
                f"INFO: drain is active ({'full' if drain.is_full_drain else 'partial-cap'}"
                f"{'; reason: ' + drain.reason if drain.reason else ''}) "
                f"— daemon dispatch is gated"
            )
            return True, drain.reason, line
    except Exception:  # pragma: no cover — defensive
        pass
    return False, None, None


def _rpc_ping_liveness(*, multi_user: bool = False) -> tuple[bool, str | None, str]:
    """Probe the daemon's RPC socket. Returns
    ``(responsive, version, socket_path)``.

    ``responsive`` is True iff the daemon answered ``ping`` — the same
    signal ``vq queue`` / ``vq programs`` / job dispatch rely on, and the
    one that works on every platform (no systemd / cgroup dependency).
    Never raises: any failure — socket missing, connect refused, timeout,
    malformed reply — is reported as not-responding, mirroring how the
    rest of vq degrades when the daemon is down.
    """
    from vq import rpc as _rpc
    sock = str(_rpc.socket_path(multi_user=multi_user))
    try:
        resp = _rpc.ping(multi_user=multi_user)
    except Exception:  # noqa: BLE001 — any failure == not responding
        return False, None, sock
    if not isinstance(resp, dict):
        return False, None, sock
    return True, resp.get("version"), sock


def _verify_via_rpc_liveness(*, multi_user: bool = False) -> ContractVerdict:
    """Liveness verdict for hosts where systemd-user supervision is not
    the daemon's supervision mechanism — macOS (the documented guarded
    manual-start node), or any host without a user systemd manager.

    The v0.5.49 four-source contract (loginctl / pgrep `systemd --user` /
    ``systemctl --user`` / pidfile) is meaningless there: the daemon is
    started directly (e.g. ``vq daemon run`` from a wrapper script), so
    every systemd probe reports "absent" and the contract would FAIL a
    perfectly healthy daemon. This was the macOS ``vq overview`` →
    ``daemon: FAIL (pidfile)`` false-negative.

    The fall-back uses the signals that actually work off Linux:

    * the **RPC socket** answering ``ping`` — authoritative "is the daemon
      serving?", the same probe ``vq queue`` / ``vq programs`` use; and
    * the **pidfile** pointing at a live process — secondary, informational.

    ``ok`` tracks RPC responsiveness alone: a live pidfile pid without a
    responsive socket means the process exists but isn't serving, which is
    not OK. Mirrors the STATUS-1 (v0.8.18 *Chandra's Detector*) precedent
    of trusting daemon liveness — not a Linux-only mechanism — as the
    source of truth. Single-user paths throughout, matching the rest of
    this module (the macOS guarded node never runs the root multi-user
    daemon).
    """
    findings: list[str] = [
        (
            "INFO: this daemon is the system-wide multi-user daemon, which "
            "systemd-user does not supervise. Using RPC-socket liveness "
            "instead of the systemd four-source contract."
            if multi_user
            else "INFO: no systemd-user supervision on this host (no `systemd "
            "--user`, loginctl, or `systemctl --user`) — normal off Linux "
            "(e.g. a macOS guarded node). Using RPC-socket liveness instead "
            "of the systemd four-source contract."
        )
    ]

    # Pidfile identity + process liveness — informational; the RPC ping
    # below is the authoritative health signal.
    pidfile_pid = daemon_control.read_pidfile()
    daemon_alive = _pid_alive(pidfile_pid)
    if pidfile_pid is None:
        findings.append(
            "INFO: no daemon pidfile present (daemon not started via "
            "`vq daemon start` / `vq daemon run`)"
        )
    elif daemon_alive:
        findings.append(
            f"OK: daemon process pid={pidfile_pid} is alive (pidfile)"
        )
    else:
        # WARN, not FAIL: a stale pidfile must not flip the verdict when
        # the RPC probe proves the daemon is serving (ok is rpc-driven).
        findings.append(
            f"WARN: daemon pid={pidfile_pid} in pidfile but the process "
            f"is gone (stale pidfile)"
        )

    # RPC-socket liveness — the authoritative, platform-independent signal.
    rpc_ok, rpc_version, sock_path = _rpc_ping_liveness(multi_user=multi_user)
    if rpc_ok:
        version_part = f" (version {rpc_version})" if rpc_version else ""
        findings.append(
            f"OK: daemon RPC socket responsive at {sock_path}{version_part}"
        )
    else:
        findings.append(
            f"FAIL: daemon RPC socket at {sock_path} not responding "
            f"(daemon down?)"
        )

    memory_pressure_pct, mem_finding = _memory_pressure_finding()
    if mem_finding is not None:
        findings.append(mem_finding)
    drain_active, drain_reason, drain_finding = _drain_finding()
    if drain_finding is not None:
        findings.append(drain_finding)

    return ContractVerdict(
        ok=rpc_ok,
        manager_pid=None,
        loginctl_state=None,
        loginctl_runtime_path=None,
        systemctl_user_reachable=False,
        vq_daemon_state=None,
        vq_daemon_main_pid=None,
        daemon_pidfile_pid=pidfile_pid,
        daemon_process_alive=daemon_alive,
        memory_pressure_pct=memory_pressure_pct,
        drain_active=drain_active,
        drain_reason=drain_reason,
        findings=findings,
    )


def verify_user_systemd_contract() -> ContractVerdict:
    """v0.5.49: cross-check the four daemon-lifecycle sources of truth
    and return a single structured verdict.

    Pure read-only. Never restarts, kills, or reconfigures anything.

    The verdict is ``ok=True`` only when:
      * loginctl says the user is active
      * a live ``systemd --user`` process exists in the process table
      * ``systemctl --user`` can reach that manager
      * if ``vq-daemon.service`` is in the registry, its MainPID
        matches the pidfile (when both are present) and the recorded
        PID is alive
    Any disagreement adds a ``FAIL`` finding and flips ``ok`` to
    False. ``WARN`` / ``INFO`` lines describe degraded-but-recoverable
    states that don't fail the verdict.
    """
    # Probe the three systemd-user signals up front. If ALL are absent,
    # this host has no user systemd manager at all (macOS / a non-systemd
    # box) — the four-source contract can't apply and would FAIL a healthy
    # daemon, so fall back to RPC-socket liveness. A healthy Linux host
    # always has at least a loginctl State + a manager pid here, so the
    # fleet path is unchanged; the orphan/zombie incidents (manager
    # present but systemctl unreachable) still take the systemd path and
    # FAIL correctly because manager_pid is not None.
    # A multi-user host's daemon is a SYSTEM service. systemd-user does not
    # supervise it, so the four-source user contract is not merely unhelpful
    # there -- it is measuring a different manager entirely, and reports FAIL
    # for a daemon that is serving normally. workstation and compute-a did exactly that
    # on 2026-07-27: `vq overview` showed `daemon: FAIL` while `vq doctor` was
    # 5/5 on both, `doctor --all` was 14/14 fleet-wide, and RPC answered. The
    # FAIL invited a repair that was never needed and cost a wrong handover, a
    # retraction and a cross-chat investigation.
    #
    # This is the same class as the macOS false negative the RPC fallback below
    # was written for -- "systemd-user supervision is not the daemon's
    # supervision mechanism" -- it just was never wired for multi-user, because
    # a multi-user Linux box DOES have a user manager. It is simply irrelevant
    # to the daemon in question.
    from vq import config as _config  # noqa: PLC0415 — circular if top-level

    if _config.system_multi_user_enabled():
        return _verify_via_rpc_liveness(multi_user=True)

    loginctl_state, loginctl_runtime = _loginctl_user_state()
    manager_pid = _pgrep_systemd_user()
    sysctl_reachable = _systemctl_user_reachable()
    if manager_pid is None and not sysctl_reachable and loginctl_state is None:
        return _verify_via_rpc_liveness()

    findings: list[str] = []

    # 1. loginctl — login-layer view of the user session.
    if loginctl_state is None:
        findings.append("WARN: loginctl unavailable or returned no data")
    elif loginctl_state != "active":
        findings.append(
            f"WARN: loginctl reports State={loginctl_state!r} "
            f"(expected 'active')"
        )
    else:
        findings.append(
            f"OK: loginctl State=active, "
            f"RuntimePath={loginctl_runtime}"
        )

    # 2. user-manager process actually in the process table.
    if manager_pid is None:
        findings.append(
            "FAIL: no `systemd --user` process found via pgrep "
            "(user manager is not running — pam_systemd may not have "
            "spawned one, or it died and wasn't restarted)"
        )
    elif not _pid_alive(manager_pid):
        findings.append(
            f"FAIL: pgrep returned pid={manager_pid} but the process "
            f"is not alive (zombie / unreaped)"
        )
    else:
        findings.append(
            f"OK: `systemd --user` running at pid={manager_pid}"
        )

    # 3. systemctl --user can actually reach the manager.
    if not sysctl_reachable:
        findings.append(
            "FAIL: `systemctl --user` cannot reach the user manager "
            "(orphan / zombie — see operations.md § "
            "'Failed to connect to user scope bus...')"
        )

    # 4. vq-daemon service state via systemd.
    daemon_state: str | None = None
    daemon_main_pid: int | None = None
    if sysctl_reachable:
        rc_state, raw_state = _systemctl_user_show(
            "vq-daemon.service", "ActiveState",
        )
        if rc_state == 0:
            daemon_state = raw_state.strip() or None
        rc_pid, raw_pid = _systemctl_user_show(
            "vq-daemon.service", "MainPID",
        )
        if rc_pid == 0:
            try:
                daemon_main_pid = int(raw_pid.strip())
            except ValueError:
                daemon_main_pid = None
            # MainPID=0 is systemd's "unit inactive" signal.
            if daemon_main_pid == 0:
                daemon_main_pid = None
        if daemon_state == "active":
            findings.append(
                f"OK: vq-daemon.service active "
                f"(systemd MainPID={daemon_main_pid})"
            )
        elif daemon_state is None:
            findings.append(
                "WARN: vq-daemon.service ActiveState unavailable "
                "(unit may not be installed)"
            )
        else:
            findings.append(
                f"WARN: vq-daemon.service state={daemon_state!r} "
                f"(expected 'active')"
            )

    # 5. daemon pidfile — the second identity source.
    pidfile_pid = daemon_control.read_pidfile()
    if pidfile_pid is None:
        findings.append(
            "INFO: no daemon pidfile present (daemon was not started "
            "via `vq daemon start`; the systemd unit path doesn't "
            "write one)"
        )
    elif daemon_main_pid is not None and pidfile_pid != daemon_main_pid:
        findings.append(
            f"FAIL: daemon pidfile pid={pidfile_pid} but systemd "
            f"MainPID={daemon_main_pid} — two competing identity "
            f"sources, daemon may have been started twice"
        )
    elif daemon_main_pid is not None and pidfile_pid == daemon_main_pid:
        findings.append(
            f"OK: pidfile and systemd agree on daemon pid="
            f"{pidfile_pid}"
        )
    else:
        # Pidfile present but systemd is unreachable (sysctl_reachable
        # False) — can't compare. Surface the pidfile state on its own.
        findings.append(
            f"INFO: daemon pidfile pid={pidfile_pid} (systemd "
            f"unavailable for cross-check)"
        )

    # 6. Is the recorded daemon PID actually alive?
    candidate_pid = daemon_main_pid or pidfile_pid
    daemon_alive = _pid_alive(candidate_pid)
    if candidate_pid is not None:
        if daemon_alive:
            findings.append(
                f"OK: daemon process pid={candidate_pid} is alive"
            )
        else:
            findings.append(
                f"FAIL: daemon pid={candidate_pid} recorded but the "
                f"process is gone"
            )

    # Memory pressure + drain — shared with the RPC-liveness fall-back so
    # the verdict surface is identical across platforms. Both are
    # informational and never flip the OK verdict (memory pressure is the
    # v0.6.20 watchdog's job; drain is an operator-set dispatch gate).
    memory_pressure_pct, mem_finding = _memory_pressure_finding()
    if mem_finding is not None:
        findings.append(mem_finding)
    drain_active, drain_reason, drain_finding = _drain_finding()
    if drain_finding is not None:
        findings.append(drain_finding)

    ok = not any(f.startswith("FAIL") for f in findings)

    return ContractVerdict(
        ok=ok,
        manager_pid=manager_pid,
        loginctl_state=loginctl_state,
        loginctl_runtime_path=loginctl_runtime,
        systemctl_user_reachable=sysctl_reachable,
        vq_daemon_state=daemon_state,
        vq_daemon_main_pid=daemon_main_pid,
        daemon_pidfile_pid=pidfile_pid,
        daemon_process_alive=daemon_alive,
        memory_pressure_pct=memory_pressure_pct,
        drain_active=drain_active,
        drain_reason=drain_reason,
        findings=findings,
    )


def format_contract_verdict(v: ContractVerdict) -> str:
    """Human-readable text rendering for ``vq daemon health``.

    Layout::

       == daemon lifecycle health ==
       verdict: OK | FAILED

       findings:
         OK: loginctl State=active, RuntimePath=/run/user/1000
         OK: `systemd --user` running at pid=...
         ...
    """
    lines = [
        "== daemon lifecycle health ==",
        f"verdict: {'OK' if v.ok else 'FAILED'}",
        "",
        "findings:",
    ]
    if v.findings:
        lines.extend(f"  {line}" for line in v.findings)
    else:
        lines.append("  (no findings recorded)")
    return "\n".join(lines)


def format_contract_verdict_json(v: ContractVerdict) -> str:
    """JSON rendering for ``vq daemon health --json``. Pretty-printed
    (indent=2, sort_keys=True) so a human reading the JSON output
    still gets a readable result. Stable schema mirroring the dataclass
    field names — every documented field appears with the documented
    type (no fields are dropped on null)."""
    return json.dumps(asdict(v), indent=2, sort_keys=True)
