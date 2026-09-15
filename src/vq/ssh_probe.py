"""Local-side SSH reachability diagnostics -- the ``vq doctor`` local leg.

Every other ``vq doctor`` check is *remote*-side: ``remote_vq``,
``daemon_rpc``, and the scheduler probes each need an SSH session to the host
before they can say anything at all. So when SSH itself is what broke, doctor
could only report that everything failed. It could not separate

1. the local link / VPN being down,
2. the jump host (``ProxyJump`` / ``ProxyCommand`` first hop) being unreachable,
3. the target host being down or refusing, from
4. the key being rejected.

All four look identical at the OpenSSH level, because OpenSSH reports a
failure *anywhere* in a proxied chain against the FINAL target::

    ssh: connect to host pbs-cluster port 22: Connection closed by UNKNOWN port 65535

The 2026-07-22 gateway outage is the canonical case: a shared jump host's sshd
was refusing connections while the targets and their keys were perfectly
healthy, and the only line naming the real culprit was one row up in
``ssh -v``. Finding it took a long manual session. This module turns that into
a verdict.

Three primitives, all local, none of which need the target to be reachable:

* :func:`resolve_route` -- ask OpenSSH itself (``ssh -G``) what an alias
  actually resolves to and whether a ``ProxyJump`` / ``ProxyCommand`` sits in
  front of it. This alone is worth having: it makes an otherwise invisible
  bastion visible, and it is the only part of a chain vq can learn without
  touching the network.
* :func:`probe_tcp` -- open a bare TCP socket to the FIRST hop of that chain
  (the jump host when proxied, else the target) and classify the outcome:
  reachable / refused / timed out / DNS failure / unreachable.
* :func:`classify` -- fold an ssh exit-255, the probe result, and OpenSSH's own
  stderr into ONE named verdict plus the next step to take.

:func:`verbose_probe` backs the third one up for the case vq cannot probe
directly: a ``ProxyCommand`` is an opaque local program, so instead of guessing
its first hop we re-run the connection under ``ssh -v`` and keep the lines that
actually name a cause -- including the ProxyCommand's own stderr, which OpenSSH
passes through unprefixed.

Nothing here is site-specific and nothing here knows what a VPN is: any vq user
behind a bastion gets "first hop unreachable" instead of "connection closed by
UNKNOWN port 65535".
"""
from __future__ import annotations

import errno
import json
import math
import re
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from vq import transport

# ``ssh -G`` parses config files and prints; it never touches the network, so
# this only guards against a pathological config or a wedged ssh binary.
DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS = 5.0

# Bare TCP connect to one hop. Deliberately shorter than transport's
# ConnectTimeout=10: this is a triage probe whose job is to answer "is anything
# listening" fast, not to ride out a slow link. A host that needs more than 3 s
# to complete a TCP handshake is already a finding.
DEFAULT_TCP_PROBE_TIMEOUT_SECONDS = 3.0

# Bound for the ``ssh -v`` re-run. Only ever used on a path that has ALREADY
# failed, so the cost is paid once per diagnosis, never on the happy path.
DEFAULT_VERBOSE_PROBE_TIMEOUT_SECONDS = 30.0
_VERBOSE_PROBE_CONNECT_TIMEOUT_SECONDS = 10

# A ProxyJump chain longer than this is either a loop or a configuration nobody
# wants vq to spend `ssh -G` round-trips on.
_MAX_JUMP_DEPTH = 8

_MAX_DIAGNOSTIC_LINES = 12
_MAX_DIAGNOSTIC_LINE_CHARS = 200
# Generous, because a failover ProxyCommand names its gateways in the tail of
# the command and that is exactly what the operator needs to read.
_MAX_PROXY_COMMAND_CHARS = 240

_TCP_PROBE_WORKER_SCRIPT = """
import json
import sys
from vq import ssh_probe

result = ssh_probe._probe_tcp_in_process(
    sys.argv[1],
    int(sys.argv[2]),
    timeout=float(sys.argv[3]),
)
print(json.dumps({
    "outcome": result.outcome,
    "detail": result.detail,
}))
""".strip()

_DEBUG_PREFIX_RE = re.compile(r"^debug\d*:\s*")

# Lines from an ``ssh -v`` transcript worth showing the operator. A debug line
# is kept only if it matches one of these; a NON-debug line is kept
# unconditionally, because that is where a ProxyCommand's own stderr lands.
_DIAGNOSTIC_MARKERS = (
    "connection refused",
    "connection closed",
    "connection reset",
    "connection timed out",
    "operation timed out",
    "no route to host",
    "network is unreachable",
    "could not resolve",
    "permission denied",
    "host key",
    "banner exchange",
    "kex_exchange_identification",
    "broken pipe",
    "authentication",
    "timed out",
)

_HOST_KEY_MARKERS = (
    "remote host identification has changed",
    "host key verification failed",
)
_AUTH_MARKERS = (
    "permission denied",
    "no supported authentication methods",
    "too many authentication failures",
    "authentication failed",
)
_REFUSED_MARKERS = ("connection refused",)
_UNREACHABLE_MARKERS = (
    "no route to host",
    "network is unreachable",
    "network is down",
    "host is down",
)
_TIMEOUT_MARKERS = (
    "operation timed out",
    "connection timed out",
    "timed out",
)
_DNS_MARKERS = (
    "could not resolve hostname",
    "name or service not known",
    "nodename nor servname",
    "temporary failure in name resolution",
)
# The signature of a broken hop *inside* a proxy chain: OpenSSH has no channel
# to the target, so it blames the target with a placeholder peer.
_PROXY_COLLAPSE_MARKERS = (
    "connection closed by unknown port 65535",
    "connection closed by remote host",
    "kex_exchange_identification",
    "banner exchange",
    "broken pipe",
    "connection reset by peer",
)


class SshProbeError(RuntimeError):
    """``ssh -G`` could not be run or did not understand the destination."""


class SshProbeTimeout(SshProbeError):
    """SSH route resolution exhausted its total diagnostic deadline."""


SshRunner = Callable[[Sequence[str], float], "subprocess.CompletedProcess[str]"]


def _default_runner(
    argv: Sequence[str], timeout: float
) -> subprocess.CompletedProcess[str]:
    return transport.run_owned_subprocess(
        list(argv),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


@dataclass(frozen=True)
class Hop:
    """One endpoint in a resolved SSH route."""

    label: str  # "target" or "jump host 'ssh4'"
    host: str
    port: int

    def describe(self) -> str:
        return f"{self.label} {self.host}:{self.port}"


@dataclass(frozen=True)
class SshRoute:
    """What OpenSSH will actually do with a destination alias.

    ``first_hop`` is the endpoint vq can meaningfully TCP-probe: the jump host
    when the route is proxied, the target when it is direct, and ``None`` when
    a ``ProxyCommand`` makes it unknowable (``resolve_note`` says why).
    """

    destination: str
    hostname: str
    port: int
    user: str
    proxy_jump: str | None
    proxy_command: str | None
    identity_files: tuple[str, ...]
    first_hop: Hop | None
    resolve_note: str = ""

    @property
    def proxied(self) -> bool:
        return self.proxy_jump is not None or self.proxy_command is not None

    def describe(self) -> str:
        target = f"{self.hostname}:{self.port}"
        if self.user:
            target = f"{self.user}@{target}"
        if self.proxy_jump is not None:
            via = f"ProxyJump {self.proxy_jump!r}"
            if self.first_hop is not None:
                via += f" -> {self.first_hop.host}:{self.first_hop.port}"
        elif self.proxy_command is not None:
            via = f"ProxyCommand {_truncate(self.proxy_command, _MAX_PROXY_COMMAND_CHARS)!r}"
        else:
            via = "direct"
        text = f"{self.destination!r} -> {via} -> {target}"
        if self.resolve_note:
            text += f"\nnote: {self.resolve_note}"
        return text


@dataclass(frozen=True)
class TcpProbe:
    """Outcome of a bare TCP connect to one hop."""

    host: str
    port: int
    outcome: str  # reachable | refused | timeout | dns | unreachable | error
    detail: str
    elapsed_seconds: float

    @property
    def reachable(self) -> bool:
        return self.outcome == "reachable"


@dataclass(frozen=True)
class TransportVerdict:
    """A named cause for an SSH transport failure, plus what to do about it."""

    kind: str
    summary: str
    next_step: str
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True)
class VerboseProbe:
    """Result of re-running the connection under ``ssh -v``."""

    returncode: int
    diagnostics: tuple[str, ...]
    error: str = ""
    timed_out: bool = False


def _truncate(text: str, limit: int) -> str:
    text = text.strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _clean_option(value: str | None) -> str | None:
    """Normalise an ``ssh -G`` value, treating OpenSSH's ``none`` as unset."""
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() == "none":
        return None
    return value


def _dump_config(
    destination: str, runner: SshRunner, timeout: float
) -> dict[str, list[str]]:
    """Return ``ssh -G <destination>`` as ``{lowercased key: [values]}``.

    ``ssh -G`` is OpenSSH's own answer to "what would you do with this alias",
    so it accounts for ``Match`` blocks, ``Include``s, and the system-wide
    config -- none of which vq could reproduce by parsing ``~/.ssh/config``
    itself.
    """
    argv = ["ssh", "-G", destination]
    try:
        proc = runner(argv, timeout)
    except subprocess.TimeoutExpired as exc:
        raise SshProbeTimeout(
            f"`ssh -G {destination}` timed out after {timeout}s"
        ) from exc
    except OSError as exc:
        raise SshProbeError(f"could not run `ssh -G {destination}`: {exc}") from exc
    if proc.returncode != 0:
        detail = (proc.stderr or "").strip() or f"exit {proc.returncode}"
        raise SshProbeError(f"`ssh -G {destination}` failed: {detail}")
    fields: dict[str, list[str]] = {}
    for line in (proc.stdout or "").splitlines():
        key, _, value = line.partition(" ")
        if not key:
            continue
        fields.setdefault(key.strip().lower(), []).append(value.strip())
    return fields


def _first(fields: dict[str, list[str]], key: str) -> str | None:
    values = fields.get(key)
    return values[0] if values else None


def _as_port(value: str | None, default: int = 22) -> int:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _split_hop_spec(spec: str) -> tuple[str, int | None]:
    """Split a ``[user@]host[:port]`` ProxyJump element into host + port.

    Bracketed IPv6 literals (``[2001:db8::1]:2222``) are handled explicitly so
    the address's own colons are not mistaken for a port separator.
    """
    spec = spec.strip()
    _, _, hostpart = spec.rpartition("@")
    hostpart = hostpart or spec
    if hostpart.startswith("["):
        host, sep, rest = hostpart[1:].partition("]")
        if sep and rest.startswith(":") and rest[1:].isdigit():
            return host, int(rest[1:])
        return host, None
    host, sep, maybe_port = hostpart.rpartition(":")
    if sep and host and maybe_port.isdigit():
        return host, int(maybe_port)
    return hostpart, None


def _route_timeout_remaining(deadline: float, alias: str) -> float:
    """Return the remaining total ``ssh -G`` route-resolution budget."""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SshProbeTimeout(
            f"`ssh -G {alias}` has no remaining diagnostic time"
        )
    return remaining


def _resolve_first_hop(
    alias: str,
    explicit_port: int | None,
    runner: SshRunner,
    deadline: float,
    depth: int,
) -> tuple[Hop | None, str]:
    """Walk a ProxyJump chain to the endpoint the local machine dials first."""
    label = "target" if depth == 0 else f"jump host {alias!r}"
    if depth > _MAX_JUMP_DEPTH:
        return None, (
            f"ProxyJump chain is deeper than {_MAX_JUMP_DEPTH} hops "
            f"(or loops) at {alias!r}; not probing"
        )
    try:
        fields = _dump_config(
            alias,
            runner,
            _route_timeout_remaining(deadline, alias),
        )
    except SshProbeTimeout:
        raise
    except SshProbeError as exc:
        return None, f"could not resolve jump alias {alias!r}: {exc}"
    proxy_jump = _clean_option(_first(fields, "proxyjump"))
    if proxy_jump is not None:
        next_alias, next_port = _split_hop_spec(proxy_jump.split(",")[0])
        if not next_alias:
            return None, f"{alias!r} has an unparseable ProxyJump: {proxy_jump!r}"
        return _resolve_first_hop(
            next_alias,
            next_port,
            runner,
            deadline,
            depth + 1,
        )
    proxy_command = _clean_option(_first(fields, "proxycommand"))
    if proxy_command is not None:
        return None, (
            f"{alias!r} uses a ProxyCommand, which is an opaque local program; "
            "vq cannot know its first hop"
        )
    host = _first(fields, "hostname") or alias
    port = explicit_port if explicit_port is not None else _as_port(
        _first(fields, "port")
    )
    return Hop(label, host, port), ""


def resolve_route(
    destination: str,
    *,
    timeout: float = DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS,
    runner: SshRunner | None = None,
) -> SshRoute:
    """Resolve what ``ssh <destination>`` will really connect to.

    Raises :class:`SshProbeError` only when the destination itself cannot be
    resolved. A jump alias that fails to resolve is reported through
    ``resolve_note`` instead, so a broken bastion entry still yields a usable
    route description for the target.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise SshProbeTimeout(
            f"`ssh -G {destination}` has no remaining diagnostic time"
        )
    deadline = time.monotonic() + timeout
    runner = runner or _default_runner
    fields = _dump_config(
        destination,
        runner,
        _route_timeout_remaining(deadline, destination),
    )
    proxy_jump = _clean_option(_first(fields, "proxyjump"))
    proxy_command = _clean_option(_first(fields, "proxycommand"))
    # OpenSSH clears ProxyCommand when ProxyJump is set, so at most one of
    # these is ever populated in practice; ProxyJump still wins if both are.
    if proxy_jump is not None:
        alias, port = _split_hop_spec(proxy_jump.split(",")[0])
        if alias:
            first_hop, note = _resolve_first_hop(
                alias,
                port,
                runner,
                deadline,
                1,
            )
        else:
            first_hop, note = None, f"unparseable ProxyJump: {proxy_jump!r}"
    elif proxy_command is not None:
        first_hop = None
        note = (
            "ProxyCommand is an opaque local program; vq cannot probe its first "
            "hop directly -- the ssh -v transcript is used instead"
        )
    else:
        first_hop = Hop(
            "target",
            _first(fields, "hostname") or destination,
            _as_port(_first(fields, "port")),
        )
        note = ""
    return SshRoute(
        destination=destination,
        hostname=_first(fields, "hostname") or destination,
        port=_as_port(_first(fields, "port")),
        user=_first(fields, "user") or "",
        proxy_jump=proxy_jump,
        proxy_command=proxy_command,
        identity_files=tuple(fields.get("identityfile", ())),
        first_hop=first_hop,
        resolve_note=note,
    )


def _probe_tcp_in_process(
    host: str,
    port: int,
    *,
    timeout: float = DEFAULT_TCP_PROBE_TIMEOUT_SECONDS,
) -> TcpProbe:
    """Open and immediately close a TCP connection, classifying the failure.

    Deliberately below the SSH layer: no key material, no protocol banner, no
    auth. "Is anything accepting TCP on this endpoint" is the one question that
    separates a dead link from a rejected key, and it is answerable in
    milliseconds when the answer is yes.
    """
    start = time.monotonic()

    def elapsed() -> float:
        return time.monotonic() - start

    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except socket.gaierror as exc:
        return TcpProbe(host, port, "dns", f"cannot resolve {host!r}: {exc}", elapsed())
    except TimeoutError:
        return TcpProbe(
            host,
            port,
            "timeout",
            f"no answer within {timeout:g}s (filtered, or the route is dead)",
            elapsed(),
        )
    except ConnectionRefusedError:
        return TcpProbe(
            host,
            port,
            "refused",
            "connection refused (host is up, nothing listening on this port)",
            elapsed(),
        )
    except OSError as exc:
        unreachable = exc.errno in (
            errno.EHOSTUNREACH,
            errno.ENETUNREACH,
            errno.ENETDOWN,
            errno.EHOSTDOWN,
        )
        return TcpProbe(
            host,
            port,
            "unreachable" if unreachable else "error",
            str(exc) or exc.__class__.__name__,
            elapsed(),
        )
    return TcpProbe(host, port, "reachable", "accepted a TCP connection", elapsed())


def probe_tcp(
    host: str,
    port: int,
    *,
    timeout: float = DEFAULT_TCP_PROBE_TIMEOUT_SECONDS,
    runner: SshRunner | None = None,
) -> TcpProbe:
    """Probe TCP behind an owned subprocess boundary that also bounds DNS.

    A socket connect timeout does not cover ``getaddrinfo``. Running the
    resolver and connect in a new, owned process group lets doctor terminate
    the complete probe even when the platform resolver wedges indefinitely.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("TCP probe timeout must be finite and greater than zero")
    started = time.monotonic()
    runner = runner or _default_runner
    argv = [
        sys.executable,
        "-c",
        _TCP_PROBE_WORKER_SCRIPT,
        host,
        str(port),
        repr(timeout),
    ]
    try:
        proc = runner(argv, timeout)
    except subprocess.TimeoutExpired:
        elapsed = max(0.0, time.monotonic() - started)
        return TcpProbe(
            host,
            port,
            "timeout",
            f"{host!r} gave no answer within {timeout:g}s "
            "(DNS or connect probe stalled)",
            elapsed,
        )
    except OSError as exc:
        elapsed = max(0.0, time.monotonic() - started)
        return TcpProbe(
            host,
            port,
            "error",
            f"could not start bounded TCP probe: {type(exc).__name__}",
            elapsed,
        )

    elapsed = max(0.0, time.monotonic() - started)
    if proc.returncode != 0:
        return TcpProbe(
            host,
            port,
            "error",
            f"bounded TCP probe failed (exit {proc.returncode})",
            elapsed,
        )
    try:
        payload = json.loads(proc.stdout or "{}")
    except ValueError:
        payload = None
    if not isinstance(payload, dict):
        return TcpProbe(
            host,
            port,
            "error",
            "bounded TCP probe returned malformed output",
            elapsed,
        )
    outcome = payload.get("outcome")
    detail = payload.get("detail")
    if (
        not isinstance(outcome, str)
        or outcome
        not in {
            "reachable",
            "refused",
            "timeout",
            "dns",
            "unreachable",
            "error",
        }
        or not isinstance(detail, str)
    ):
        return TcpProbe(
            host,
            port,
            "error",
            "bounded TCP probe returned malformed fields",
            elapsed,
        )
    return TcpProbe(host, port, outcome, detail, elapsed)


def control_master_active(
    destination: str,
    *,
    timeout: float = DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS,
    runner: SshRunner | None = None,
) -> bool:
    """Is a persisted multiplexed connection to ``destination`` still alive?

    This is the one thing that can make a failed first-hop probe non-decisive.
    The fleet's ``~/.ssh/config`` uses ``ControlMaster auto`` + ``ControlPersist``
    (see :func:`vq.transport._multiplex_bypass_options`), so ssh can still reach
    a host over an established master socket after the route underneath it has
    died. Concluding "unreachable" from the socket layer alone would then be
    wrong about what vq can currently do.

    ``ssh -O check`` asks the local master, not the network, so this stays a
    local probe. Exit 0 means a master is running; anything else (no socket, no
    ControlPath configured, ssh missing) means there is nothing to reuse.
    """
    runner = runner or _default_runner
    try:
        proc = runner(["ssh", "-O", "check", destination], timeout)
    except (subprocess.TimeoutExpired, OSError):
        return False
    return proc.returncode == 0


def _contains(haystack: str, markers: Sequence[str]) -> bool:
    return any(marker in haystack for marker in markers)


def _verdict_from_probe(route: SshRoute, probe: TcpProbe) -> TransportVerdict:
    hop = route.first_hop
    where = hop.describe() if hop is not None else f"{probe.host}:{probe.port}"
    proxied = route.proxied
    if probe.outcome == "dns":
        return TransportVerdict(
            "dns_failure",
            f"{where} does not resolve",
            "fix the hostname in ~/.ssh/config, or the resolver/search domain "
            "this machine is using.",
            (probe.detail,),
        )
    if probe.outcome == "refused":
        kind = "first_hop_refused" if proxied else "target_refused"
        subject = "the jump host" if proxied else "the target host"
        return TransportVerdict(
            kind,
            f"{where} refused the connection -- {subject} is up but its sshd "
            "is not accepting on that port",
            f"nothing is wrong with vq or your key. Check sshd on {probe.host} "
            f"(or the port in ~/.ssh/config)"
            + (
                "; the target host was never contacted."
                if proxied
                else "."
            ),
            (probe.detail,),
        )
    kind = "first_hop_unreachable" if proxied else "target_unreachable"
    subject = (
        "the first hop of the proxy chain" if proxied else "the target host"
    )
    return TransportVerdict(
        kind,
        f"{where} is unreachable -- no TCP path to {subject}",
        "check local connectivity first (link, VPN, routing), then whether "
        f"{probe.host} is up. vq never got far enough to use your key"
        + ("; the target host was never contacted." if proxied else "."),
        (probe.detail,),
    )


def classify(
    route: SshRoute,
    probe: TcpProbe | None,
    stderr: str,
    *,
    returncode: int | None = None,
) -> TransportVerdict:
    """Name the cause of an SSH transport failure.

    Precedence is deliberate. A failed TCP probe is *evidence* -- it says a
    specific endpoint did not accept a connection -- while ssh's stderr is a
    *report*, and on a proxied route that report is written against the wrong
    endpoint. So the probe wins whenever it has something to say, and stderr is
    only consulted when the socket layer looked fine.
    """
    if returncode == 0:
        return TransportVerdict(
            "ok",
            "ssh reached the host successfully on re-check",
            "the earlier failure looks transient; re-run the command that "
            "failed.",
        )
    if probe is not None and not probe.reachable:
        return _verdict_from_probe(route, probe)

    text = stderr.lower()
    evidence = tuple(
        line.strip() for line in stderr.splitlines() if line.strip()
    )[:_MAX_DIAGNOSTIC_LINES]

    if _contains(text, _HOST_KEY_MARKERS):
        return TransportVerdict(
            "host_key_changed",
            "the host key presented does not match ~/.ssh/known_hosts",
            "do NOT clear the entry blindly -- a changed host key is also what "
            "a man-in-the-middle looks like. Get the new fingerprint "
            "out-of-band, compare, then remove the stale entry.",
            evidence,
        )
    if _contains(text, _AUTH_MARKERS):
        return TransportVerdict(
            "auth_rejected",
            "the connection reached an sshd, which rejected the key",
            "the network path is fine. Check the key vq is offering "
            f"({', '.join(route.identity_files) or 'ssh default identities'}) "
            "against the remote authorized_keys.",
            evidence,
        )
    if _contains(text, _DNS_MARKERS):
        return TransportVerdict(
            "dns_failure",
            "a hostname in the route did not resolve",
            "fix the hostname in ~/.ssh/config, or the resolver this machine "
            "is using.",
            evidence,
        )
    if route.proxied and _contains(text, _PROXY_COLLAPSE_MARKERS + _REFUSED_MARKERS):
        return TransportVerdict(
            "proxy_failed",
            "the proxy chain broke before the target was reached -- OpenSSH "
            "reports this against the target, but the target was never "
            "contacted",
            "the failing hop is in the proxy chain. Run "
            f"`ssh -v {route.destination}` and read the line ABOVE the final "
            "error; that names the hop that actually failed.",
            evidence,
        )
    if _contains(text, _REFUSED_MARKERS):
        return TransportVerdict(
            "target_refused",
            "the target host refused the connection",
            f"check sshd on {route.hostname} and the port in ~/.ssh/config.",
            evidence,
        )
    if _contains(text, _PROXY_COLLAPSE_MARKERS):
        # Same signature as a collapsed proxy chain, but there is no chain: the
        # far end accepted TCP and then dropped us before the SSH banner.
        return TransportVerdict(
            "target_closed",
            "the target accepted the connection and then closed it before the "
            "SSH banner",
            f"something on {route.hostname}:{route.port} is answering but is "
            "not (or not yet) sshd. Check sshd's logs, MaxStartups / rate "
            "limiting, and any TCP wrapper or firewall in front of it.",
            evidence,
        )
    if _contains(text, _UNREACHABLE_MARKERS + _TIMEOUT_MARKERS):
        return TransportVerdict(
            "target_unreachable",
            "no TCP path to the target host",
            "check local connectivity (link, VPN, routing), then whether "
            f"{route.hostname} is up.",
            evidence,
        )
    return TransportVerdict(
        "unknown",
        "ssh failed at the transport layer for a reason vq does not recognise",
        f"run `ssh -vv {route.destination}` by hand and read the last few "
        "lines before the failure.",
        evidence,
    )


def diagnostic_lines(stderr: str) -> tuple[str, ...]:
    """Keep the lines of an ``ssh -v`` transcript that name a cause.

    Debug lines are kept only when they match a known marker; NON-debug lines
    are kept unconditionally, because a ProxyCommand's own stderr is passed
    through unprefixed and is frequently the only honest description of what
    broke.
    """
    kept: list[str] = []
    for raw in stderr.splitlines():
        line = raw.strip()
        if not line:
            continue
        is_debug = bool(_DEBUG_PREFIX_RE.match(line))
        text = _DEBUG_PREFIX_RE.sub("", line)
        low = text.lower()
        if low.startswith("openssh_") or low.startswith("reading configuration"):
            continue
        if is_debug and not _contains(low, _DIAGNOSTIC_MARKERS):
            continue
        text = _truncate(text, _MAX_DIAGNOSTIC_LINE_CHARS)
        if text not in kept:
            kept.append(text)
        if len(kept) >= _MAX_DIAGNOSTIC_LINES:
            break
    return tuple(kept)


def verbose_probe(
    destination: str,
    *,
    timeout: float = DEFAULT_VERBOSE_PROBE_TIMEOUT_SECONDS,
    connect_timeout: int = _VERBOSE_PROBE_CONNECT_TIMEOUT_SECONDS,
    runner: SshRunner | None = None,
) -> VerboseProbe:
    """Re-run the connection under ``ssh -v`` and keep the telling lines.

    Runs ``true`` on the remote -- a shell builtin, so it exists everywhere and
    changes nothing. ``BatchMode=yes`` matches :mod:`vq.transport`, so a host
    whose key trust is broken fails here exactly as it does in real use rather
    than stalling on a password prompt.

    Only called on a path that has already failed, so its cost never lands on a
    healthy fleet sweep.
    """
    runner = runner or _default_runner
    argv = [
        "ssh",
        "-v",
        "-o", f"ConnectTimeout={connect_timeout}",
        "-o", "BatchMode=yes",
        destination,
        "true",
    ]
    try:
        proc = runner(argv, timeout)
    except subprocess.TimeoutExpired:
        return VerboseProbe(
            255,
            (),
            f"`ssh -v {destination} true` did not return within {timeout:g}s",
            timed_out=True,
        )
    except OSError as exc:
        return VerboseProbe(255, (), f"could not run `ssh -v {destination}`: {exc}")
    return VerboseProbe(proc.returncode, diagnostic_lines(proc.stderr or ""))
