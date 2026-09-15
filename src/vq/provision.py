"""Read-only verification of a host's vq provisioning preconditions.

``vq doctor`` answers "is this host reachable, and is its daemon healthy?".
That is not the same question as "is this host *provisioned correctly*", and
the gap is not academic: every precondition checked here has cost hours of a
fleet sweep at least once, while ``vq doctor`` reported the host green
throughout. The clearest example is ``remote_vq`` pointing straight at
``/opt/vq/venv/bin/vq`` instead of at the wrapper: doctor proves only that the
command answered, so the host passes, while the CLI reads the *per-user* store
and ``set_admin_status`` writes the *canonical* one. Updates then report
``success: True`` with ``work_errors: []`` and ``LAST OK`` never advances --
the writes succeed, they are simply read back out of a different file.

**These verdicts are deliberately NOT emitted into the doctor payload.**
``fleet_rollout._host_hold_reason`` turns any failed doctor check other than
``scheduler_remote_vq`` into a rollout *defer*, so folding provisioning checks
into doctor would defer every host on the first rollout after a new check
landed. They carry their own schema, ``vq.admin.provision_check/1``, with the
same inner record shape so the renderer and the check builder are shared.

Every probe here is read-only. Repairing what it finds needs root on the target
(``chgrp`` on ``/var/lib/vq``, installing units, writing ``/opt/vq``), so the
remediation is reported as an exact command rather than run. See
``docs/multi_user_deployment.md``.
"""

from __future__ import annotations

import re
import shlex
from typing import Any

from vq import config, transport
from vq.doctor import check, local_ssh_checks
from vq.host import is_local_host

SCHEMA = "vq.admin.provision_check/1"
FLEET_SCHEMA = "vq.admin.provision_check_fleet/1"
"""Distinct from :data:`SCHEMA` on purpose: the ``--all`` envelope wraps
per-host payloads under ``hosts``, and one schema string covering two
incompatible shapes is not a contract a consumer can rely on."""

STATE_ROOT = "/var/lib/vq"
OPT_VENV_VQ = "/opt/vq/venv/bin/vq"
REFRESH_HELPER = "/opt/vq/bin/vq-multi-user-refresh"
MULTI_USER_UNIT = "vq-daemon-multi-user.service"

_STAT_RE = re.compile(r"^([0-7]{3,4})\s+(\S+):(\S+)$")
_MODE_RE = re.compile(r"^[0-7]{3,4}$")

# `stat -c` is GNU, `stat -f` is BSD. The BSD arm uses %Lp (the full low-order
# mode, including setgid) rather than %OLp (permission bits only), because the
# state-root check is precisely a setgid check and %OLp cannot render it.
_STAT_MODE_OWNER = (
    'stat -c "%a %U:%G" {path} 2>/dev/null || stat -f "%Lp %Su:%Sg" {path}'
)
_STAT_MODE = 'stat -c "%a" {path} 2>/dev/null || stat -f "%Lp" {path}'

# Parse the target's canonical config instead of grepping it. The root-owned
# vq interpreter, when present, also applies that installation's full config
# schema. A malformed or unreadable file is not evidence for either deployment
# mode: the caller must retain that uncertainty rather than falling back to the
# driver's own (different host's) policy.
_MULTI_USER_PROBE = r"""if [ -x /opt/vq/venv/bin/python ]; then
    vq_provision_python=/opt/vq/venv/bin/python
else
    vq_provision_python=$(command -v python3 2>/dev/null || true)
fi
if [ -z "$vq_provision_python" ] || [ ! -x "$vq_provision_python" ]; then
    printf '%s\n' 'no Python interpreter can validate /etc/vq/config.toml' >&2
    exit 2
fi
"$vq_provision_python" - <<'PY'
from pathlib import Path
import sys
import tomllib

path = Path("/etc/vq/config.toml")
try:
    with path.open("rb") as stream:
        payload = tomllib.load(stream)
except FileNotFoundError:
    print("missing")
    raise SystemExit(0)
except (OSError, tomllib.TOMLDecodeError) as exc:
    print(f"cannot parse {path}: {exc}", file=sys.stderr)
    raise SystemExit(2)

try:
    from vq import config as vq_config
except ImportError:
    vq_config = None
if vq_config is not None:
    try:
        vq_config.Config.model_validate(payload)
    except Exception as exc:
        # Key and reason only, rendered here rather than through a vq helper
        # because this runs under whatever vq the target has installed.
        try:
            detail = "; ".join(
                ".".join(str(part) for part in item["loc"]) + ": " + item["msg"]
                for item in exc.errors(
                    include_url=False, include_context=False, include_input=False,
                )
            )
        except Exception:
            detail = type(exc).__name__
        print(f"invalid config in {path}: {detail}", file=sys.stderr)
        raise SystemExit(2)

section = payload.get("multi_user")
if section is None:
    print("absent")
elif not isinstance(section, dict):
    print("multi_user must be a table", file=sys.stderr)
    raise SystemExit(2)
else:
    enabled = section.get("enabled", False)
    if not isinstance(enabled, bool):
        print("multi_user.enabled must be a boolean", file=sys.stderr)
        raise SystemExit(2)
    print("enabled" if enabled else "disabled")
PY"""


class ProvisionError(RuntimeError):
    """Provisioning could not be assessed at all."""


def _remote(
    host_cfg: config.HostConfig,
    script: str,
    *,
    timeout: float = 30.0,
) -> tuple[int, str, str]:
    """Run one read-only shell snippet on the target, never raising.

    Returns ``(returncode, stdout, stderr)`` -- deliberately NOT combined. Every
    probe below parses stdout positionally, and an SSH banner, a ``stat``
    complaint from the first arm of a portability fallback, or a login-shell
    warning would otherwise be glued in front of the value being read. That is
    not hypothetical: it turns "no ``/opt/vq`` install" into a passing check and
    turns a missing token file into a ``ValueError`` on ``int(mode, 8)``.

    A probe that cannot run at all is a failed check carrying the transport's
    own words, not an exception: one unanswerable question must not hide the
    answers to the others.
    """
    try:
        proc = transport.run_remote_shell(
            host_cfg,
            "sh",
            "-c",
            script,
            check=False,
            timeout=timeout,
        )
    except transport.RemoteError as exc:
        return 255, "", str(exc)
    return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()


def _local(script: str) -> tuple[int, str, str]:
    import subprocess  # noqa: PLC0415 - only needed on the localhost path

    try:
        proc = subprocess.run(
            ["sh", "-c", script],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 255, "", str(exc)
    return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()


def _runner(host_cfg: config.HostConfig):
    """Pick the local or remote probe path for one host."""
    if is_local_host(host_cfg.ssh):
        return _local
    return lambda script: _remote(host_cfg, script)


def _remote_vq_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool,
) -> list[dict[str, Any]]:
    """Is ``remote_vq`` a wrapper that puts the CLI in the right world?

    Only meaningful on a multi-user host. There, the CLI must export
    ``VQ_CONFIG_DIR`` and ``VQ_STATE_DIR`` before exec-ing the venv vq, or it
    reads the per-user store while canonical writes land in the system one --
    a split that presents as ``LAST_UPDATED_AT`` frozen at an old timestamp
    while the checkout is demonstrably current.
    """
    command = host_cfg.remote_vq
    if not multi_user:
        return [
            check(
                "remote_vq_wrapper",
                True,
                f"not applicable: {host_cfg.ssh} is not a multi-user host "
                f"(remote_vq={command!r})",
                remote_vq=command,
            )
        ]
    # shlex.split, not the raw string: a remote_vq carrying arguments
    # ("vq --config /etc/vq") must have its PROGRAM resolved, not the whole
    # command line, which `command -v` would simply fail to find.
    try:
        program = shlex.split(command)[0]
    except (ValueError, IndexError):
        program = command
    quoted = shlex.quote(program)
    _rc, out, _err = run(f"command -v {quoted} 2>/dev/null || true")
    resolved = out.splitlines()[0].strip() if out else ""
    if not resolved:
        return [
            check(
                "remote_vq_wrapper",
                False,
                f"configured remote_vq {command!r} is not on PATH for the SSH "
                f"user on {host_cfg.ssh}. Set [hosts.*].remote_vq to the "
                "absolute path of a wrapper script.",
                remote_vq=command,
            )
        ]
    if resolved == OPT_VENV_VQ:
        return [
            check(
                "remote_vq_wrapper",
                False,
                f"remote_vq resolves straight to {OPT_VENV_VQ}, not to a "
                "wrapper. The CLI will read the per-user store while "
                "canonical admin writes go to the system one, so updates "
                "report success and LAST OK never advances. Point remote_vq "
                "at a wrapper that exports VQ_CONFIG_DIR=/etc/vq and "
                "VQ_STATE_DIR=/var/lib/vq before exec-ing "
                f"{OPT_VENV_VQ} (docs/fleet_update_runbook.md section 3b).",
                remote_vq=command,
                resolved_path=resolved,
            )
        ]
    _rc, body, _err = run(f"cat {shlex.quote(resolved)} 2>/dev/null || true")
    # An assignment on a non-comment line, not a bare substring: a wrapper whose
    # comment merely mentions VQ_STATE_DIR would otherwise pass while exporting
    # nothing, which is exactly the split-store failure this check exists for.
    exports = [
        name
        for name in ("VQ_CONFIG_DIR", "VQ_STATE_DIR")
        if any(
            re.match(rf"^\s*(export\s+)?{name}\s*=", line)
            for line in body.splitlines()
            if not line.lstrip().startswith("#")
        )
    ]
    ok = len(exports) == 2
    return [
        check(
            "remote_vq_wrapper",
            ok,
            (
                f"{resolved} exports VQ_CONFIG_DIR and VQ_STATE_DIR"
                if ok
                else f"{resolved} does not export "
                + ", ".join(
                    name
                    for name in ("VQ_CONFIG_DIR", "VQ_STATE_DIR")
                    if name not in exports
                )
                + ". Without both, the CLI and the daemon disagree about "
                "which store is canonical."
            ),
            remote_vq=command,
            resolved_path=resolved,
            exports=exports,
        )
    ]


def _admin_token_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool,
) -> list[dict[str, Any]]:
    """Is ``admin_token_file`` configured, present, and 0600?

    Without it, canonical writes fail with ``PermissionError: admin token
    required in multi-user mode`` -- and the loader refuses a token file whose
    mode has any group or other bits set, so a readable-but-loose copy fails
    just as hard as a missing one.
    """
    path = host_cfg.admin_token_file
    if not multi_user:
        return [
            check(
                "admin_token_file",
                True,
                f"not applicable: {host_cfg.ssh} is not a multi-user host",
            )
        ]
    if path is None:
        return [
            check(
                "admin_token_file",
                False,
                "admin_token_file is unset for this host. Multi-user "
                "canonical writes need it: copy the daemon's "
                "/etc/vq/web-token to a 0600 file readable by the SSH user "
                "and set [hosts.*].admin_token_file to that absolute path.",
            )
        ]
    quoted = shlex.quote(path)
    rc, out, err = run(
        f"if [ -f {quoted} ]; then "
        + _STAT_MODE.format(path=quoted)
        + '; else printf "missing"; fi'
    )
    mode = out.splitlines()[0].strip() if out else ""
    if mode == "missing" or rc != 0 or not _MODE_RE.match(mode):
        return [
            check(
                "admin_token_file",
                False,
                f"{path} does not exist or could not be inspected on "
                f"{host_cfg.ssh} ({err or out or 'no output'}). Copy "
                "/etc/vq/web-token there, mode 0600, owned by the SSH user.",
                path=path,
            )
        ]
    loose = bool(int(mode, 8) & 0o077)
    return [
        check(
            "admin_token_file",
            not loose,
            (
                f"{path} present, mode {mode}"
                if not loose
                else f"{path} is mode {mode}; the loader refuses any token "
                "file with group or other bits set. Fix: chmod 600 " + path
            ),
            path=path,
            mode=mode,
        )
    ]


def _state_root_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool,
    admin_group: str,
) -> list[dict[str, Any]]:
    """Is ``/var/lib/vq`` ``root:<admin_group>`` 2775?

    The admin-update marker is written directly under the state root, so a
    ``root:root 0755`` one leaves the host fully provisioned, daemon running,
    and unable to perform any admin operation -- which reads as a vq bug rather
    than a missing permission. ``deploy-multi-user.sh`` has set this since
    v0.15.106; hosts provisioned before that still need the manual fix.
    """
    if not multi_user:
        return [
            check(
                "state_root_perms",
                True,
                f"not applicable: {host_cfg.ssh} is not a multi-user host",
            )
        ]
    rc, out, err = run(_STAT_MODE_OWNER.format(path=STATE_ROOT))
    if rc != 0 or not out:
        return [
            check(
                "state_root_perms",
                False,
                f"could not stat {STATE_ROOT} on {host_cfg.ssh}: "
                f"{err or out or 'no output'}",
            )
        ]
    match = _STAT_RE.match(out.splitlines()[0].strip())
    if match is None:
        return [
            check(
                "state_root_perms",
                False,
                f"unrecognised stat output for {STATE_ROOT}: {out!r}",
            )
        ]
    mode, owner, group = match.groups()
    ok = mode == "2775" and owner == "root" and group == admin_group
    return [
        check(
            "state_root_perms",
            ok,
            (
                f"{STATE_ROOT} is {mode} {owner}:{group}"
                if ok
                else f"{STATE_ROOT} is {mode} {owner}:{group}, expected 2775 "
                f"root:{admin_group}. Every admin operation fails at marker "
                f"acquisition until this is fixed. Fix: ssh -t "
                f"{host_cfg.ssh} 'sudo chgrp {admin_group} {STATE_ROOT} && "
                f"sudo chmod 2775 {STATE_ROOT}'"
            ),
            mode=mode,
            owner=owner,
            group=group,
        )
    ]


def _root_install_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool,
) -> list[dict[str, Any]]:
    """Is the root-owned ``/opt/vq`` install present, and can it be refreshed?

    A multi-user host whose ``/opt/vq`` cannot be refreshed by the sanctioned
    helper falls back to the hand-typed three-command sequence, whose wrong
    ordering silently stamps a new commit onto old code.
    """
    if not multi_user:
        return [
            check(
                "root_owned_install",
                True,
                f"not applicable: {host_cfg.ssh} is not a multi-user host",
            )
        ]
    checks: list[dict[str, Any]] = []
    rc, out, err = run(
        f'if [ -x {OPT_VENV_VQ} ]; then {OPT_VENV_VQ} --version; '
        'else printf "missing"; fi'
    )
    first = out.splitlines()[0].strip() if out else ""
    present = first not in {"", "missing"} and rc == 0
    checks.append(
        check(
            "root_owned_install",
            present,
            (
                f"{OPT_VENV_VQ}: {first}"
                if present
                else f"no usable root-owned install at {OPT_VENV_VQ} "
                f"({err or out or 'no output'}). Run "
                "contrib/deploy-multi-user.sh on that host."
            ),
        )
    )
    rc, out, err = run(
        f"if [ -x {REFRESH_HELPER} ]; then "
        + _STAT_MODE_OWNER.format(path=REFRESH_HELPER)
        + '; else printf "missing"; fi'
    )
    first = out.splitlines()[0].strip() if out else ""
    helper_present = first not in {"", "missing"} and rc == 0
    match = _STAT_RE.match(first) if helper_present else None
    root_owned = match is not None and match.group(2) == "root"
    checks.append(
        check(
            "refresh_helper",
            helper_present and root_owned,
            (
                f"{REFRESH_HELPER} present, {first}"
                if helper_present and root_owned
                else f"{REFRESH_HELPER} is missing or not root-owned "
                f"({err or out or 'no output'}). Without it, refreshing /opt/vq "
                "falls back to a hand-typed pip+marker sequence whose wrong "
                "ordering stamps a new commit onto old code. Re-run "
                "contrib/deploy-multi-user.sh to install it."
            ),
        )
    )
    return checks


def _delegation_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool,
) -> list[dict[str, Any]]:
    """Can the daemon drop privileges into a per-job cgroup?

    ``deploy-multi-user.sh`` refuses to install without ``systemd-run`` for
    exactly this reason: a multi-user daemon that cannot drop privileges would
    run every job as root.
    """
    if not multi_user:
        return [
            check(
                "delegation",
                True,
                f"not applicable: {host_cfg.ssh} is not a multi-user host",
            )
        ]
    _rc, out, _err = run(
        "command -v systemd-run >/dev/null && printf yes || printf no"
    )
    available = out.strip() == "yes"
    return [
        check(
            "delegation",
            available,
            (
                "systemd-run is available for per-job privilege drop"
                if available
                else "systemd-run is not on PATH; the multi-user daemon "
                "cannot drop job privileges. See docs/cgroup-setup.md."
            ),
        )
    ]


def _daemon_unit_check(
    host_cfg: config.HostConfig,
    run,
    *,
    multi_user: bool | None,
) -> list[dict[str, Any]]:
    """Cross-check the canonical root unit against target config policy.

    The unit is probed for every local-execution host. In particular, a
    readable single-user config cannot make an active root unit disappear from
    the report: that combination would execute submitted payloads as root.
    """
    # `systemctl is-active` exits non-zero for inactive/failed and still prints
    # the state on stdout, so the rc is not the signal; the word is. Single-user
    # hosts need no service manager at all, so distinguish an absent systemctl
    # from an unanswerable probe instead of turning supported launchd and other
    # non-systemd hosts into provisioning failures.
    _rc, out, err = run(
        "if command -v systemctl >/dev/null 2>&1; then "
        f"systemctl is-active {MULTI_USER_UNIT} || true; "
        "else printf unavailable; fi"
    )
    reported = out or err
    state = reported.splitlines()[0].strip() if reported else "unknown"
    active = state == "active"

    if multi_user is True:
        ok = active
        message = (
            f"{MULTI_USER_UNIT} is active"
            if active
            else f"{MULTI_USER_UNIT} is {state}. "
            f"Check: journalctl -u {MULTI_USER_UNIT} -n 40"
        )
    elif multi_user is False:
        ok = state in {"inactive", "not-found", "unavailable"}
        if active:
            message = (
                f"unsafe deployment contradiction: {MULTI_USER_UNIT} is "
                "active while /etc/vq/config.toml disables or omits "
                "[multi_user]. The root daemon would take the single-user "
                "execution path and run submitted payloads as root. Stop "
                "dispatch and reconcile the deployment mode before "
                "restarting the service."
            )
        elif state == "unavailable":
            message = (
                "systemctl is unavailable, so the root multi-user unit is "
                "not applicable to this single-user host"
            )
        elif ok:
            message = (
                f"{MULTI_USER_UNIT} is {state}, consistent with the target's "
                "single-user configuration"
            )
        else:
            message = (
                f"could not prove {MULTI_USER_UNIT} inactive for the "
                f"target's single-user configuration (reported {state})"
            )
    else:
        ok = False
        message = (
            f"{MULTI_USER_UNIT} is {state}, but /etc/vq/config.toml could not "
            "be parsed or read; root-daemon applicability is unknown and the "
            "driver's own multi-user setting is not authority for this host"
        )

    return [
        check(
            "daemon_unit",
            ok,
            message,
            unit=MULTI_USER_UNIT,
            state=state,
            config_multi_user=multi_user,
        )
    ]


def _programs_check(
    host: str,
    host_cfg: config.HostConfig,
) -> list[dict[str, Any]]:
    """Does *this host* have managed programs registered at all?

    Asked of the target, not of the driver's own config: ``vq admin update
    <env> <host>`` resolves ``<env>`` against the registry the **target**
    reads, so the driver's `[programs.*]` says nothing about whether the host
    has a lane. A host with none is planned as "no managed lane" and silently
    skipped by ``rollout-latest`` -- indistinguishable in the output from a
    host that is fully converged.
    """
    try:
        if is_local_host(host_cfg.ssh):
            import subprocess  # noqa: PLC0415
            import sys  # noqa: PLC0415

            proc = subprocess.run(
                [sys.executable, "-m", "vq", "programs", "--json"],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
                stdin=subprocess.DEVNULL,
            )
        else:
            proc = transport.run_remote_vq(
                host_cfg,
                "programs",
                "--json",
                check=False,
                timeout=60,
            )
    except Exception as exc:  # noqa: BLE001 - one probe must not sink the rest
        return [
            check(
                "programs_registered",
                False,
                f"could not list programs on {host}: {exc}",
            )
        ]
    import json  # noqa: PLC0415

    if proc.returncode != 0:
        # Checked before parsing: an empty stdout would otherwise parse as an
        # empty list and report "no programs registered", which is a different
        # and much less actionable fault than "the command did not run".
        detail = (proc.stderr or proc.stdout or "no output").strip()
        return [
            check(
                "programs_registered",
                False,
                f"`vq programs --json` on {host} exited "
                f"{proc.returncode}: {detail}",
            )
        ]
    try:
        entries = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError:
        detail = (proc.stderr or proc.stdout or "no output").strip()
        return [
            check(
                "programs_registered",
                False,
                f"`vq programs --json` on {host} did not return JSON: {detail}",
            )
        ]
    names = sorted(
        str(entry.get("name"))
        for entry in entries
        if isinstance(entry, dict) and entry.get("name")
    ) if isinstance(entries, list) else []
    return [
        check(
            "programs_registered",
            bool(names),
            (
                "registered programs: " + ", ".join(names)
                if names
                else f"no programs are registered on {host}, so no update "
                "lane can target it. A host with no managed program is "
                "planned as 'no managed lane' and silently skipped by "
                "rollout-latest -- which looks exactly like convergence."
            ),
            programs=names,
            scheduler=host_cfg.scheduler,
        )
    ]


def _multi_user_state(
    host: str,
    host_cfg: config.HostConfig,
    run,
    cfg: config.Config,
) -> tuple[bool | None, dict[str, Any]]:
    """Is the TARGET a multi-user host?

    Asked of the target's own ``/etc/vq/config.toml``, not of the driver's
    config. Those are different files describing different machines, and in a
    mixed fleet the driver's answer is simply about the driver: taking it as the
    target's would report a single-user host as missing ``/opt/vq``,
    ``/var/lib/vq`` and a root daemon it is not supposed to have -- six
    confident failures for a correctly provisioned machine -- and, in the
    reverse case, would silently skip every multi-user check on the hosts that
    need them most.

    An unreadable or malformed target config is unknown. The driver's setting
    describes a different host and therefore cannot safely fill that gap.
    """
    rc, out, err = run(_MULTI_USER_PROBE)
    if rc == 0:
        state = out.strip()
        if state in {"enabled", "disabled", "absent", "missing"}:
            enabled = state == "enabled"
            return enabled, check(
                "multi_user_mode",
                True,
                (
                    f"{host} runs multi-user mode "
                    "(/etc/vq/config.toml has [multi_user] enabled = true)"
                    if enabled
                    else f"{host} is configured for single-user operation "
                    f"([multi_user] is {state})"
                ),
                multi_user=enabled,
                config_state=state,
                source="target /etc/vq/config.toml",
            )
    detail = err or out or "no output"
    return None, check(
        "multi_user_mode",
        False,
        f"could not parse or read /etc/vq/config.toml on {host} ({detail}); "
        "multi-user mode is unknown. The driver's own [multi_user] setting "
        f"({cfg.multi_user.enabled}) describes the driver, not this host, and "
        "is not used as fallback authority.",
        multi_user=None,
        config_state="unknown",
        source="target /etc/vq/config.toml",
    )


def diagnose_host(
    cfg: config.Config,
    host: str,
    *,
    probe_cache: dict[tuple[str, int], Any] | None = None,
) -> dict[str, Any]:
    """Verify one host's provisioning preconditions. Changes nothing."""
    try:
        host_cfg = cfg.host(host)
    except config.ConfigError as exc:
        raise ProvisionError(str(exc)) from None

    checks: list[dict[str, Any]] = []
    blocked = False
    if not is_local_host(host_cfg.ssh):
        ssh_checks, _route, _probe, blocked = local_ssh_checks(
            host_cfg,
            probe_cache=probe_cache if probe_cache is not None else {},
        )
        checks.extend(ssh_checks)
    if blocked:
        # Every remaining probe would only reproduce the same connection
        # failure, reported N more times as N distinct provisioning faults.
        checks.append(
            check(
                "provisioning",
                False,
                "not attempted: the first hop is unreachable, so every "
                "remote probe below would only restate that failure",
            )
        )
        return _payload(host, host_cfg, checks)

    run = _runner(host_cfg)
    admin_group = cfg.multi_user.admin_group
    multi_user, multi_user_check = _multi_user_state(host, host_cfg, run, cfg)
    checks.append(multi_user_check)
    enabled = multi_user is True
    checks.extend(_remote_vq_check(host_cfg, run, multi_user=enabled))
    checks.extend(_admin_token_check(host_cfg, run, multi_user=enabled))
    checks.extend(
        _state_root_check(
            host_cfg,
            run,
            multi_user=enabled,
            admin_group=admin_group,
        )
    )
    checks.extend(_root_install_check(host_cfg, run, multi_user=enabled))
    checks.extend(_delegation_check(host_cfg, run, multi_user=enabled))
    checks.extend(_daemon_unit_check(host_cfg, run, multi_user=multi_user))
    checks.extend(_programs_check(host, host_cfg))
    return _payload(host, host_cfg, checks)


def _payload(
    host: str,
    host_cfg: config.HostConfig,
    checks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "host": host,
        "ssh": host_cfg.ssh,
        "ok": all(bool(item["ok"]) for item in checks),
        "checks": checks,
    }


def remediations(payload: dict[str, Any]) -> list[str]:
    """The failed checks' messages, in check order.

    Repairing any of these needs root on the target, so the verb reports the
    exact command instead of running it. Returning the messages rather than a
    second remediation field keeps one source of truth for each fix.
    """
    checks = payload.get("checks")
    if not isinstance(checks, list):
        return []
    return [
        f"{item['name']}: {item['message']}"
        for item in checks
        if isinstance(item, dict) and not item.get("ok")
    ]


def render_text(payload: dict[str, Any]) -> str:
    """Human-readable per-check report, shaped like ``vq doctor``'s."""
    lines = [f"== vq admin provision: {payload['host']} =="]
    checks = payload["checks"]
    assert isinstance(checks, list)
    for item in checks:
        assert isinstance(item, dict)
        label = "OK" if item.get("ok") else "FAIL"
        first, *rest = str(item["message"]).splitlines()
        lines.append(f"{label} {item['name']}: {first}")
        lines.extend(f"  {line}" for line in rest)
    lines.append(f"verdict: {'ok' if payload.get('ok') else 'failed'}")
    return "\n".join(lines)
