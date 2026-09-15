"""v0.7.5 *Hopper's Compiler* — host recovery channel audit.

Probes the three tiers of the host recovery contract documented in
``docs/host_recovery_channels.md``:

  Tier 1 — Hardware management (BMC / IPMI / iDRAC)
  Tier 2 — Cockpit web admin on :9090
  Tier 3 — Recovery sshd on a separate port (default 22222)

For each tier the probe answers one binary question: *if my primary
SSH path broke right now, could I get back in via this channel?*
Tier-by-tier results plus an overall verdict feed the CLI verb
``vq admin audit-recovery HOST``.

The probes are independent of vq's normal SSH transport — they
must work even when the primary key trust is broken, otherwise
they'd be testing the wrong thing.
"""
from __future__ import annotations

import logging
import socket
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from vq import config

log = logging.getLogger(__name__)


# Independent probe timeout. Short enough that an unreachable host
# doesn't drag the audit, long enough to tolerate slow Cockpit
# startup or a high-latency network. Independent of the main
# transport's ConnectTimeout because we want the audit to feel
# snappy regardless of vq's other transport tuning.
_PROBE_TIMEOUT_SECONDS = 5


TierStatus = Literal["green", "yellow", "red", "skipped"]


@dataclass(frozen=True)
class TierResult:
    """One tier's audit verdict."""

    tier: int
    name: str
    status: TierStatus
    detail: str
    """One-line explanation suitable for the operator-facing table.
    Examples:
      * "port 22222 reachable; recovery key accepted"
      * "port 9090 connection refused (cockpit not installed?)"
      * "no bmc_url configured — tier marked N/A"
    """


@dataclass(frozen=True)
class AuditReport:
    """Full audit outcome for one host."""

    host_name: str
    tiers: list[TierResult]
    overall: Literal["green", "yellow", "red"]
    """green:  ≥ 2 tiers green (or 1 green + 1 N/A for tier 1)
       yellow: exactly 1 tier green, others degraded
       red:    no tier green — host is unrecoverable from this laptop"""


def _resolve_host_address(ssh_alias: str) -> str | None:
    """v0.7.5: probes need an IP/hostname they can connect to
    directly (TCP socket + curl). ``ssh_alias`` may be an
    ~/.ssh/config alias — resolve via ``ssh -G`` to find the
    actual hostname. Returns None on any failure (probes then
    skip with a clear error)."""
    try:
        proc = subprocess.run(
            ["ssh", "-G", ssh_alias],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            stdin=subprocess.DEVNULL,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.lower().startswith("hostname "):
            return line.split(None, 1)[1]
    return None


def _probe_tcp(host: str, port: int) -> tuple[bool, str]:
    """Returns ``(reachable, detail)``. ``reachable`` is True iff
    a TCP connection succeeds within the probe timeout."""
    try:
        with socket.create_connection(
            (host, port), timeout=_PROBE_TIMEOUT_SECONDS,
        ):
            return True, f"port {port} reachable"
    except TimeoutError:
        return False, f"port {port} timed out after {_PROBE_TIMEOUT_SECONDS}s"
    except ConnectionRefusedError:
        return False, f"port {port} connection refused"
    except OSError as e:
        return False, f"port {port} failed: {e}"


def _probe_cockpit(host: str, port: int) -> TierResult:
    """Tier 2: TCP probe to Cockpit's port. We don't require an
    HTTP response — even a 401/redirect proves Cockpit is alive
    and the firewall is open. Most operators will reach Cockpit
    via SSH-tunnel from the laptop, but the audit probes the
    direct path because that's the one that survives a primary-
    sshd-broken scenario (when the tunnel would also fail)."""
    reachable, detail = _probe_tcp(host, port)
    if reachable:
        return TierResult(
            tier=2, name="Cockpit",
            status="green",
            detail=f"{detail} (cockpit responding)",
        )
    # Not reachable. Could be firewall, could be cockpit not
    # installed. The script + tier-3 path lets the operator
    # in to find out — yellow not red since tier 3 is still
    # an option.
    return TierResult(
        tier=2, name="Cockpit",
        status="red",
        detail=f"{detail} — cockpit not installed OR firewall blocking from this laptop",
    )


def _probe_recovery_ssh(
    ssh_alias: str, rec: config.RecoveryConfig,
) -> TierResult:
    """Tier 3: actually try to authenticate with the recovery key
    on the recovery port. This is the most important tier — it's
    the channel that survives a wiped ``~/.ssh/authorized_keys``,
    which was the 2026-05-26 workstation case."""
    key_path = Path(rec.ssh_key_path).expanduser()
    if not key_path.is_file():
        return TierResult(
            tier=3, name="Recovery SSH",
            status="red",
            detail=(
                f"recovery key not found at {key_path} — "
                f"generate it per docs/host_recovery_channels.md "
                f"and re-run audit"
            ),
        )

    cmd = [
        "ssh",
        "-i", str(key_path),
        "-p", str(rec.ssh_port),
        "-o", f"ConnectTimeout={_PROBE_TIMEOUT_SECONDS}",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        # We need to override the ssh_alias's normal IdentityFile.
        # The right invocation uses user@host form rather than the
        # alias, so ssh_config doesn't override our -i. But ssh -G
        # already gave us the hostname; use that here.
        "-o", "IdentitiesOnly=yes",
    ]
    target = ssh_alias
    if rec.ssh_user:
        target = f"{rec.ssh_user}@{ssh_alias}"
    cmd += [target, "true"]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_SECONDS + 5,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return TierResult(
            tier=3, name="Recovery SSH",
            status="red",
            detail=f"port {rec.ssh_port} ssh timed out",
        )
    except OSError as e:
        return TierResult(
            tier=3, name="Recovery SSH",
            status="red",
            detail=f"ssh failed to start: {e}",
        )

    if proc.returncode == 0:
        return TierResult(
            tier=3, name="Recovery SSH",
            status="green",
            detail=f"port {rec.ssh_port} reachable; recovery key accepted",
        )
    # Non-zero exit. Try to discriminate "port closed" from "key
    # rejected" by looking at stderr signals.
    stderr = (proc.stderr or "").strip()
    if "Connection refused" in stderr:
        return TierResult(
            tier=3, name="Recovery SSH",
            status="red",
            detail=(
                f"port {rec.ssh_port} connection refused — "
                f"recovery sshd not configured (run "
                f"contrib/setup-recovery-channels.sh)"
            ),
        )
    if "Permission denied" in stderr:
        return TierResult(
            tier=3, name="Recovery SSH",
            status="red",
            detail=(
                f"port {rec.ssh_port} reachable but key rejected — "
                f"recovery_authorized_keys missing the right key; "
                f"re-run contrib/setup-recovery-channels.sh"
            ),
        )
    # Unknown failure — surface the stderr.
    return TierResult(
        tier=3, name="Recovery SSH",
        status="red",
        detail=f"ssh rc={proc.returncode}: {stderr[:120] or '(no stderr)'}",
    )


def _probe_bmc(rec: config.RecoveryConfig) -> TierResult:
    """Tier 1: hardware BMC. We don't probe the BMC's auth surface
    (BMCs are designed to resist unauthenticated requests and
    treating them like Cockpit would just clutter the firewall
    logs). Instead: if ``bmc_url`` is configured we render it as
    a click-through; if absent, mark tier 1 N/A (skipped)."""
    if not rec.bmc_url:
        return TierResult(
            tier=1, name="BMC / IPMI",
            status="skipped",
            detail="no bmc_url configured (workstation hardware? document the gap)",
        )
    return TierResult(
        tier=1, name="BMC / IPMI",
        status="green",
        detail=f"configured at {rec.bmc_url} (not probed; verify manually)",
    )


def audit_host(host_name: str, host_cfg: config.HostConfig) -> AuditReport:
    """Probe all three tiers for ``host_cfg`` and return the
    consolidated audit. Synchronous; takes up to a few
    ``_PROBE_TIMEOUT_SECONDS`` to complete."""
    rec = host_cfg.recovery

    # Resolve ssh alias to a hostname/IP for direct TCP probes.
    resolved_host = _resolve_host_address(host_cfg.ssh)
    if resolved_host is None:
        # Fall back to the alias itself — many cases work because
        # ssh aliases ARE hostnames; for genuine alias mismatches
        # we'll get clearer errors per-tier.
        resolved_host = host_cfg.ssh

    tiers = [
        _probe_bmc(rec),
        _probe_cockpit(resolved_host, rec.cockpit_port),
        _probe_recovery_ssh(host_cfg.ssh, rec),
    ]

    # Overall verdict: green if both tier 2 + 3 are green (tier 1
    # is informational; many setups won't have a BMC URL). Yellow
    # if exactly one of tier 2 / 3 is green. Red otherwise.
    green_count = sum(1 for t in tiers if t.tier in (2, 3) and t.status == "green")
    if green_count >= 2:
        overall = "green"
    elif green_count == 1:
        overall = "yellow"
    else:
        overall = "red"

    return AuditReport(
        host_name=host_name,
        tiers=tiers,
        overall=overall,
    )


def format_audit_text(report: AuditReport) -> str:
    """Render an :class:`AuditReport` as the human-facing
    text block emitted by ``vq admin audit-recovery``."""
    glyph = {
        "green": "✓",
        "yellow": "~",
        "red": "✗",
        "skipped": "-",
    }
    lines = [f"==== {report.host_name} ===="]
    for t in report.tiers:
        label = f"Tier {t.tier} ({t.name}):"
        lines.append(f"  {glyph[t.status]} {label:<28} {t.detail}")
    overall_glyph = {"green": "GREEN", "yellow": "YELLOW", "red": "RED"}[report.overall]
    lines.append(f"  Overall: {overall_glyph}")
    if report.overall == "red":
        lines.append(
            "  Action: run contrib/setup-recovery-channels.sh on this host before "
            "trusting it to the fleet."
        )
    return "\n".join(lines)


def format_audit_json(report: AuditReport) -> dict:
    """Render an :class:`AuditReport` as a JSON-serialisable dict
    suitable for ``vq admin audit-recovery --json``."""
    return {
        "host_name": report.host_name,
        "overall": report.overall,
        "tiers": [
            {
                "tier": t.tier,
                "name": t.name,
                "status": t.status,
                "detail": t.detail,
            }
            for t in report.tiers
        ],
    }
