"""SSH transport primitives for cross-machine vq operations.

Three thin wrappers over ``subprocess`` so the rest of the package can be
tested without touching SSH:

* :func:`run_remote_vq` -- invoke ``<host_cfg.remote_vq> <args...>`` on the
  remote, capturing stdout/stderr.
* :func:`run_remote_shell` -- run an arbitrary shell command on the remote
  (used for cleanup, e.g. ``rm -f /tmp/vq-upload-*.tar``).
* :func:`upload_file` -- scp a single local file to a remote path.

All three raise :class:`RemoteError` on non-zero exit (when ``check=True``)
with the remote stderr included so the user can diagnose ssh / auth /
PATH issues without re-running by hand.

v0.6.17: every subprocess.run gets a timeout, and ssh gets explicit
``ConnectTimeout`` + ``ServerAliveInterval`` / ``ServerAliveCountMax``
options. Without these, a momentary network glitch or a half-broken
remote sshd could hang the daemon main loop indefinitely (cgroup +
watchdog paths call into transport during routine bookkeeping; one
hung SSH stalls every running-job reconciliation).
"""
from __future__ import annotations

import contextlib
import logging
import math
import os
import selectors
import shlex
import signal
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import IO

from vq.config import HostConfig

log = logging.getLogger(__name__)

# Default time we'll wait for a ``vq`` subcommand to finish on the
# remote (admin update can pull big repos; submit's tarball is
# already past the network at this point). Most invocations finish
# in tens of seconds; admin update on a clean tree can take minutes
# of building. 600s is a balance — long enough to never bite real
# work, short enough that a hung remote is detected before the
# operator gives up.
DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS = 600.0

# Outer cap for remote ``vq admin update`` delegation. The remote update
# command has its own per-build watchdog; this only prevents the local SSH
# wrapper from giving up while the remote marker is still live and compiling.
# Keep ten minutes beyond the default four-hour update_script cap for git,
# process-group cleanup, activation, verification, and daemon restart work.
DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS = 15000.0

_REMOTE_ADMIN_TIMEOUT_ENV_RULES = {
    "VQ_UPDATE_SCRIPT_TIMEOUT": False,
    "VQ_BUILD_STALL_TIMEOUT": True,
}
"""The complete environment surface accepted by :func:`run_remote_vq`.

The boolean says whether zero is valid. This deliberately is not a generic
remote-environment API: environment values are visible in the local SSH argv
and the remote shell command, so credentials and ambient process state must
never travel through it.
"""

# Shorter timeout for plain shell housekeeping (rm, ls, stat, kill).
# These should finish in well under a second; 30s catches a hung
# sshd without delaying real work.
DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS = 30.0

# scp can move large tarballs. Allow more time but still bounded —
# vq workspaces on the fleet are typically <100 MB; even a slow
# residential uplink does that in <2 min. 1200s (20 min) catches
# truly stuck transfers without aborting real uploads.
DEFAULT_UPLOAD_TIMEOUT_SECONDS = 1200.0

# scp is normally quiet, but a broken wrapper/remote can emit without bound.
# Upload ambiguity is replay-unsafe, so own the process group and cap both
# pipes instead of letting subprocess.run buffer arbitrary output in memory.
SCP_STDOUT_MAX_BYTES = 64 * 1024
SCP_STDERR_MAX_BYTES = 64 * 1024

# SSH connection-establishment timeout. Independent of the per-call
# overall timeout above. Catches dead routes / wrong-port misconfigs
# quickly so the daemon doesn't sit on the TCP SYN retry budget.
_SSH_CONNECT_TIMEOUT_SECONDS = 10

# Server-alive heartbeat: ssh sends a no-op every N seconds; if M
# of them go unanswered, ssh exits with a non-zero code (and a
# "Connection to <host> closed by remote host" stderr line). Without
# this, a half-broken remote (sshd accepting connections but never
# replying) would hang for the OS TCP keepalive (~2 hours on Linux).
_SSH_SERVER_ALIVE_INTERVAL = 30
_SSH_SERVER_ALIVE_COUNT_MAX = 3

# Backoff between retries of a transient SSH *transport* failure (exit 255 /
# connect timeout). Linear: retry N sleeps N * this. Only callers that opt in
# (retry_transient > 0) retry; the daemon's bookkeeping calls stay fast-fail.
_SSH_RETRY_BACKOFF_SECONDS = 2.0

# Doctor probes own their local subprocess trees and must return promptly even
# when an SSH/ProxyCommand descendant inherits stdout/stderr and ignores TERM.
_OWNED_PROCESS_TERM_GRACE_SECONDS = 0.25
_OWNED_PROCESS_KILL_GRACE_SECONDS = 1.0


class SubprocessOutputLimitExceeded(RuntimeError):
    """An owned subprocess exceeded a caller-declared capture bound."""


def _communicate_owned_bounded(
    proc: subprocess.Popen[bytes],
    *,
    argv: Sequence[str],
    timeout: float,
    max_stdout_bytes: int,
    max_stderr_bytes: int,
) -> tuple[str, str]:
    """Drain two pipes with hard byte/time caps and no unbounded buffering."""
    if proc.stdout is None or proc.stderr is None:
        raise ValueError("bounded owned subprocess requires captured pipes")
    selector = selectors.DefaultSelector()
    selector.register(proc.stdout, selectors.EVENT_READ, ("stdout", max_stdout_bytes))
    selector.register(proc.stderr, selectors.EVENT_READ, ("stderr", max_stderr_bytes))
    chunks: dict[str, list[bytes]] = {"stdout": [], "stderr": []}
    sizes = {"stdout": 0, "stderr": 0}
    deadline = time.monotonic() + timeout
    try:
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(
                    list(argv),
                    timeout,
                    output=b"".join(chunks["stdout"]).decode("utf-8", "replace"),
                    stderr=b"".join(chunks["stderr"]).decode("utf-8", "replace"),
                )
            ready = selector.select(remaining)
            if not ready:
                continue
            for key, _events in ready:
                stream_name, limit = key.data
                data = os.read(key.fd, min(65536, limit - sizes[stream_name] + 1))
                if not data:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                chunks[stream_name].append(data)
                sizes[stream_name] += len(data)
                if sizes[stream_name] > limit:
                    raise SubprocessOutputLimitExceeded(
                        f"owned subprocess {stream_name} exceeded {limit} bytes"
                    )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(list(argv), timeout)
        proc.wait(timeout=remaining)
    finally:
        selector.close()
    return (
        b"".join(chunks["stdout"]).decode("utf-8", "replace"),
        b"".join(chunks["stderr"]).decode("utf-8", "replace"),
    )

# Backoff for scp file transfers, which are a different problem shape from the
# short bookkeeping calls above: a staging upload is the opening move of a
# multi-hour deploy, so aborting it costs orders of magnitude more than waiting.
# Exponential (base * 2**attempt, capped) rather than the linear schedule, so a
# handful of attempts spans minutes instead of seconds and outlives a gateway
# cutover rather than burning the whole budget inside one blip.
_UPLOAD_RETRY_BACKOFF_BASE_SECONDS = 5.0
_UPLOAD_RETRY_BACKOFF_MAX_SECONDS = 120.0


def _retry_delay(attempt: int, *, base: float, cap: float) -> float:
    """Exponential backoff for retry number ``attempt`` (0-based), capped.

    Deterministic (no jitter): these retries are driven by a single operator
    process against one host, so there is no thundering herd to spread out, and
    a predictable schedule is far easier to reason about in a support log.
    """
    return min(cap, base * (2.0**attempt))


class RemoteError(RuntimeError):
    """Any failure crossing the SSH boundary: ssh exit non-zero, scp upload
    fail, ssh timeout, or the remote vq returning an error."""


class RemoteCommandError(RemoteError):
    """A completed remote command failed with a known exit and redacted output.

    ``stdout`` is carried as well as ``stderr`` because a remote
    ``vq admin update --json`` answers a classified refusal with one JSON
    object on stdout and an exit code from ``ADMIN_OUTCOME_EXIT_CODES``; the
    caller that relays that code wants the object's ``error`` too.
    """

    def __init__(
        self, message: str, *, returncode: int, stderr: str, stdout: str = "",
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stderr = stderr
        self.stdout = stdout


class RemoteOutcomeUnknown(RemoteError):
    """SSH stopped observing a command that may have run remotely."""


class RemoteLaunchError(RemoteError):
    """The local SSH process could not be started."""


def _subprocess_session_kwargs(*, has_stdin_data: bool = False) -> dict[str, object]:
    """Run local ssh/scp children outside the caller's process group, with the
    caller's stdin explicitly withheld.

    If a buggy remote helper or SSH transport path dies by signal, the local
    vq process should return a bounded diagnostic instead of sharing fate with
    the helper's process group.

    (#118) — **stdin is never inherited.** ``ssh`` reads its stdin
    greedily and forwards it to the remote command, so a vq invocation inside
    a shell read-loop (``while read id; do vq status … ; done < list.tsv``)
    used to swallow the rest of the caller's input file: the loop processed
    the FIRST row and exited 0, looking complete. A 28-row fleet poll returned
    9 rows that way. No vq subprocess wants the operator's stdin, so it is
    withheld unless this call is explicitly piping ``stdin_data``.
    """
    kwargs: dict[str, object] = {"start_new_session": True}
    if not has_stdin_data:
        kwargs["stdin"] = subprocess.DEVNULL
    return kwargs


def _signal_owned_process_group(
    proc: subprocess.Popen[str],
    sig: signal.Signals,
) -> None:
    """Signal the complete session created for an owned probe subprocess."""
    try:
        if os.name == "posix":
            os.killpg(proc.pid, sig)
        elif sig == signal.SIGTERM:
            proc.terminate()
        else:
            proc.kill()
    except (ProcessLookupError, PermissionError):
        # On some POSIX systems a group containing only an unreaped zombie
        # reports EPERM rather than ESRCH. The leader is still ours and has
        # not been reaped, so its PGID cannot be reused; continue to reap it.
        pass


def _kill_and_reap_owned_process_group(
    proc: subprocess.Popen[str],
    *,
    kill_grace_seconds: float,
) -> tuple[str, str]:
    """SIGKILL an owned group, close its pipes, and reap its leader."""
    _signal_owned_process_group(proc, signal.SIGKILL)
    try:
        return proc.communicate(timeout=kill_grace_seconds)
    except BaseException:
        # A second communicate can itself be interrupted or fail because the
        # first call left a pipe in an unusual state. Closing our pipe ends and
        # waiting still reaps the direct child; SIGKILL already covered every
        # same-process-group descendant before either operation could reap the
        # leader and make its PGID reusable.
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            if stream is not None:
                with contextlib.suppress(BaseException):
                    stream.close()
        try:
            proc.wait(timeout=kill_grace_seconds)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                "owned subprocess group did not terminate after SIGKILL"
            ) from exc
        except BaseException:
            try:
                proc.wait(timeout=kill_grace_seconds)
            except BaseException as final_wait_error:
                raise RuntimeError(
                    "owned subprocess leader could not be reaped after SIGKILL"
                ) from final_wait_error
        return "", ""


def run_owned_subprocess(
    argv: Sequence[str],
    *,
    timeout: float,
    capture_output: bool = True,
    text: bool = True,
    check: bool = False,
    input: str | None = None,
    max_stdout_bytes: int | None = None,
    max_stderr_bytes: int | None = None,
    terminate_grace_seconds: float = _OWNED_PROCESS_TERM_GRACE_SECONDS,
    kill_grace_seconds: float = _OWNED_PROCESS_KILL_GRACE_SECONDS,
) -> subprocess.CompletedProcess[str]:
    """Run a bounded read-only probe and reap its complete process group.

    ``subprocess.run(..., timeout=...)`` kills only the direct child before it
    calls ``communicate()`` again. A ProxyCommand or other descendant can keep
    the captured pipes open forever, so that nominal timeout is not a bound.
    This helper owns a new session. On timeout it gives the complete process
    group one fixed TERM grace without polling or reaping the leader, then
    sends KILL to the same still-owned PGID and reaps the direct child. Any
    other exception after Popen gets the same KILL-and-reap cleanup.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and greater than zero")
    if (
        not math.isfinite(terminate_grace_seconds)
        or terminate_grace_seconds < 0
        or not math.isfinite(kill_grace_seconds)
        or kill_grace_seconds <= 0
    ):
        raise ValueError("owned-process grace periods are invalid")
    if not capture_output or not text:
        raise ValueError("owned subprocesses require captured text output")
    bounded = max_stdout_bytes is not None or max_stderr_bytes is not None
    if bounded:
        if input is not None:
            raise ValueError("bounded owned subprocess does not accept stdin data")
        if not isinstance(max_stdout_bytes, int) or max_stdout_bytes <= 0:
            raise ValueError("max_stdout_bytes must be a positive integer")
        if not isinstance(max_stderr_bytes, int) or max_stderr_bytes <= 0:
            raise ValueError("max_stderr_bytes must be a positive integer")

    proc = subprocess.Popen(
        list(argv),
        # (#118): DEVNULL, never an inherited stdin — see
        # _subprocess_session_kwargs for why.
        stdin=subprocess.PIPE if input is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=not bounded,
        start_new_session=True,
    )
    try:
        if bounded:
            assert max_stdout_bytes is not None
            assert max_stderr_bytes is not None
            stdout, stderr = _communicate_owned_bounded(
                proc,
                argv=argv,
                timeout=timeout,
                max_stdout_bytes=max_stdout_bytes,
                max_stderr_bytes=max_stderr_bytes,
            )
        else:
            stdout, stderr = proc.communicate(input=input, timeout=timeout)
    except subprocess.TimeoutExpired as initial_timeout:
        try:
            _signal_owned_process_group(proc, signal.SIGTERM)
            if terminate_grace_seconds:
                time.sleep(terminate_grace_seconds)
        except BaseException:
            _kill_and_reap_owned_process_group(
                proc,
                kill_grace_seconds=kill_grace_seconds,
            )
            raise
        stdout, stderr = _kill_and_reap_owned_process_group(
            proc,
            kill_grace_seconds=kill_grace_seconds,
        )
        raise subprocess.TimeoutExpired(
            list(argv),
            timeout,
            output=stdout or initial_timeout.output,
            stderr=stderr or initial_timeout.stderr,
        ) from initial_timeout
    except BaseException:
        _kill_and_reap_owned_process_group(
            proc,
            kill_grace_seconds=kill_grace_seconds,
        )
        raise

    completed = subprocess.CompletedProcess(
        args=list(argv),
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )
    if check:
        completed.check_returncode()
    return completed


def _returncode_label(returncode: int) -> str:
    if returncode >= 0:
        return f"exit {returncode}"
    signum = -returncode
    try:
        name = signal.Signals(signum).name
    except ValueError:
        name = f"signal {signum}"
    return f"terminated by signal {name} ({returncode})"


def _is_signal_encoded_returncode(returncode: int) -> bool:
    """Whether a shell likely encoded signal N as status ``128 + N``."""
    if returncode <= 128:
        return False
    try:
        signal.Signals(returncode - 128)
    except ValueError:
        return False
    return True


def _signal_failure_hint(lines: list[str], returncode: int, host: str) -> None:
    if returncode >= 0:
        return
    lines.extend(
        [
            "  hint: the local ssh/scp helper was terminated by a signal before "
            "it could return normal remote output.",
            "  next: treat this as a transport/helper failure, not as an empty "
            f"queue response; run `vq doctor {host} --verbose` and reconcile "
            "the command's live state before deciding whether another "
            "invocation is safe.",
        ]
    )


def _multiplex_bypass_options() -> list[str]:
    """ssh/scp options that force a brand-new connection for this invocation.

    vq passes no ``ControlMaster``/``ControlPath`` of its own, so it inherits
    whatever the operator's ``~/.ssh/config`` sets — and the fleet's blocks use
    ``ControlMaster auto`` + ``ControlPersist``. That is the right default: it
    makes the daemon's high-frequency ``qstat`` polling cheap. It is exactly
    wrong on a *retry*, because a poisoned master socket (a gateway that moved
    out from under a persisted connection) makes every attempt multiplexed over
    it fail identically. Retrying without bypassing the mux is a no-op against
    that failure mode.

    ``ControlPath=none`` is the load-bearing option: ``ControlMaster=no`` alone
    still *uses* an existing socket, it only declines to create one.
    """
    return ["-o", "ControlMaster=no", "-o", "ControlPath=none"]


def _ssh_base(host_cfg: HostConfig, *, fresh_connection: bool = False) -> list[str]:
    """Construct the ssh argv prefix with v0.6.17 connection / liveness
    options plus v0.7.3 *Dijkstra's Semaphore* ``BatchMode=yes`` so a
    host whose authorized_keys lost the laptop's key fails fast
    instead of hanging on a password prompt.

    ``fresh_connection`` (default False, so every existing caller is
    byte-identical) adds :func:`_multiplex_bypass_options`.

    Without ``BatchMode=yes``: when pubkey auth fails, ssh falls back
    to keyboard-interactive / password prompts. In a subprocess
    context the inherited stdin keeps those prompts waiting
    indefinitely — which is exactly what the 2026-05-26 workstation
    lockout exposed (ConnectTimeout passed, TCP+sshd handshake
    succeeded, but the post-auth prompt never returned).
    ``BatchMode=yes`` forces ssh to fail the auth immediately and
    return non-zero, which the per-host aggregation already renders
    inline as ``(error querying <host>: ...)``. One bad host no
    longer blocks ``--all`` listings on the rest of the fleet."""
    return [
        "ssh",
        "-o", f"ConnectTimeout={_SSH_CONNECT_TIMEOUT_SECONDS}",
        "-o", f"ServerAliveInterval={_SSH_SERVER_ALIVE_INTERVAL}",
        "-o", f"ServerAliveCountMax={_SSH_SERVER_ALIVE_COUNT_MAX}",
        "-o", "BatchMode=yes",
        *(_multiplex_bypass_options() if fresh_connection else []),
        host_cfg.ssh,
    ]


def _scp_base(*, fresh_connection: bool = False) -> list[str]:
    """Construct the scp argv prefix mirroring _ssh_base options.
    v0.7.3: ``BatchMode=yes`` matches _ssh_base — uploads to a host
    with broken key trust fail fast rather than hang on a prompt.

    ``fresh_connection`` mirrors :func:`_ssh_base`; see
    :func:`_multiplex_bypass_options` for why retries need it."""
    return [
        "scp",
        "-q",
        "-o", f"ConnectTimeout={_SSH_CONNECT_TIMEOUT_SECONDS}",
        "-o", f"ServerAliveInterval={_SSH_SERVER_ALIVE_INTERVAL}",
        "-o", f"ServerAliveCountMax={_SSH_SERVER_ALIVE_COUNT_MAX}",
        "-o", "BatchMode=yes",
        *(_multiplex_bypass_options() if fresh_connection else []),
    ]


def _remote_admin_timeout_env_prefix(
    remote_env: Mapping[str, str],
) -> list[str]:
    """Validate and render the exact direct-update timeout environment.

    The CLI performs the same domain checks to produce a Click usage error.
    Repeating them here is intentional defense in depth: no future transport
    caller can turn this narrow API into ambient environment or credential
    forwarding, and malformed values fail before ``subprocess.run``.
    """
    expected = set(_REMOTE_ADMIN_TIMEOUT_ENV_RULES)
    actual = set(remote_env)
    if actual != expected:
        raise ValueError(
            "remote admin timeout environment must contain exactly "
            f"{sorted(expected)!r}"
        )

    canonical: dict[str, str] = {}
    for name, zero_allowed in _REMOTE_ADMIN_TIMEOUT_ENV_RULES.items():
        raw = remote_env[name]
        if not isinstance(raw, str):
            raise ValueError(
                "remote admin timeout environment contains a non-string "
                f"{name} value"
            )
        try:
            value = float(raw)
        except ValueError:
            value = math.nan
        valid = math.isfinite(value) and (value >= 0 if zero_allowed else value > 0)
        if not valid:
            raise ValueError(
                "remote admin timeout environment contains an invalid "
                f"{name} value"
            )
        canonical[name] = str(value)

    return [
        "/usr/bin/env",
        *(f"{name}={canonical[name]}" for name in sorted(canonical)),
    ]


def run_remote_vq(
    host_cfg: HostConfig,
    *vq_args: str,
    check: bool = True,
    timeout: float | None = DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
    stdin_data: str | None = None,
    retry_transient: int = 0,
    remote_env: Mapping[str, str] | None = None,
    owned_process_group: bool = False,
    max_stdout_bytes: int | None = None,
    max_stderr_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``<host_cfg.remote_vq> <vq_args...>`` over ssh.

    The argv is :func:`shlex.join`-ed into a single shell-safe command
    line before being handed to ssh. ssh ALWAYS passes whatever follows
    the host argument to a shell on the remote (sh -c style); the
    remote shell re-tokenises that string and re-interprets unquoted
    shell metacharacters (``>``, ``;``, ``*``, ``$``, quotes, ...).
    Pre-quoting on the laptop ensures the remote shell hands each argv
    element to ``host_cfg.remote_vq`` as a single token with
    metacharacters intact — i.e. the remote process actually receives
    the argv the caller meant.

    Pre-v0.5.32 this passed argv as separate ssh args and claimed the
    remote shell saw them as argv directly — wrong. ``ssh host a b c``
    joins ``a b c`` with spaces and the remote shell parses the
    result. An unquoted ``>`` in the laptop argv was a remote-side
    redirection: ``vq submit -- bash -c 'echo X > /tmp/y'`` redirected
    the WHOLE remote ``vq submit`` command's stdout (the jobid!) into
    ``/tmp/y`` on the remote, leaving local ``proc.stdout`` empty and
    the local parser raising "expected 12-hex jobid, got ''".

    ``stdin_data`` (v0.6.46): if provided, the string is piped to the
    remote process's stdin. The token-forwarding path uses this with
    ``--token-stdin`` so the bearer token never appears in the local
    laptop's ``ps -ef`` argv or in the remote sh's command line — see
    security audit #3 (2026-05-24 pass) for the threat model.

    A non-empty ``remote_env`` is intentionally restricted to the complete
    pair of direct-admin-update wall/stall timeouts. It is rendered through
    ``/usr/bin/env`` ahead of the configured vq executable, so an older remote
    vq needs no new CLI flags. Unknown keys, missing keys, non-finite values,
    and out-of-domain values fail before SSH. Never use it for credentials.
    """
    from vq import auth  # noqa: PLC0415 — avoid cycle at import time

    if (max_stdout_bytes is not None or max_stderr_bytes is not None) and not (
        owned_process_group
    ):
        raise ValueError("bounded remote-vq output requires owned_process_group")

    # None and {} retain the byte-identical legacy command shape. Direct venv
    # admin updates always pass the complete pair; the empty case exists for
    # generic transport compatibility and test doubles.
    env_prefix = _remote_admin_timeout_env_prefix(remote_env) if remote_env else []
    remote_cmd = shlex.join([*env_prefix, host_cfg.remote_vq, *vq_args])
    cmd = [*_ssh_base(host_cfg), remote_cmd]
    # v0.6.46: scrub the token-bearing pair from the debug log line
    # so the credential isn't preserved verbatim in journal entries.
    # The actual ssh argv is unchanged — argv exposure is a separate
    # mitigation (use stdin_data + --token-stdin to address that).
    safe_remote_cmd = shlex.join(
        [
            *env_prefix,
            host_cfg.remote_vq,
            *auth.redact_token_args(list(vq_args)),
        ]
    )
    safe_cmd = [*_ssh_base(host_cfg), safe_remote_cmd]
    sensitive_values = auth.sensitive_arg_values(list(vq_args))
    log.debug("run_remote_vq: %s", shlex.join(safe_cmd))
    # Retry SSH transport failures only when a caller has explicitly declared
    # its command replay-safe. Exit 255 and a local observer timeout do *not*
    # prove that the remote command never ran. Mutating admin delegates
    # therefore keep retry_transient=0; read-only callers may still opt in.
    attempt = 0
    while True:
        try:
            if owned_process_group:
                if timeout is None:
                    raise ValueError(
                        "owned remote-vq subprocess requires a timeout"
                    )
                proc = run_owned_subprocess(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout,
                    input=stdin_data,
                    max_stdout_bytes=max_stdout_bytes,
                    max_stderr_bytes=max_stderr_bytes,
                )
            else:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout,
                    input=stdin_data,
                    **_subprocess_session_kwargs(
                        has_stdin_data=stdin_data is not None
                    ),
                )
        except subprocess.TimeoutExpired:
            if attempt < retry_transient:
                attempt += 1
                time.sleep(_SSH_RETRY_BACKOFF_SECONDS * attempt)
                continue
            raise RemoteOutcomeUnknown(
                f"remote vq timed out after {timeout}s on {host_cfg.ssh}:\n"
                f"  cmd: {safe_remote_cmd}\n"
                f"  ssh server-alive heartbeat may have failed silently — "
                f"check the host is reachable + sshd is responding."
            ) from None
        except (FileNotFoundError, PermissionError) as e:
            if attempt:
                raise RemoteOutcomeUnknown(
                    f"local ssh retry did not start for {host_cfg.ssh} after "
                    f"{attempt} earlier ambiguous transport attempt(s): {e}"
                ) from e
            raise RemoteLaunchError(
                f"local ssh process did not start for {host_cfg.ssh}: {e}"
            ) from e
        except SubprocessOutputLimitExceeded as e:
            raise RemoteOutcomeUnknown(
                f"remote vq output exceeded its bounded capture on "
                f"{host_cfg.ssh}: {safe_remote_cmd}"
            ) from e
        except OSError as e:
            raise RemoteOutcomeUnknown(
                f"local ssh observer failed for {host_cfg.ssh}: {e}"
            ) from e
        if proc.returncode == 255 and attempt < retry_transient:
            attempt += 1
            log.debug(
                "run_remote_vq: transient ssh exit 255 on %s (retry %d/%d)",
                host_cfg.ssh, attempt, retry_transient,
            )
            time.sleep(_SSH_RETRY_BACKOFF_SECONDS * attempt)
            continue
        break
    if check and proc.returncode != 0:
        message = _remote_vq_error_message(
            host_cfg,
            safe_remote_cmd,
            proc,
            sensitive_values=sensitive_values,
        )
        if (
            proc.returncode == 255
            or proc.returncode < 0
            or _is_signal_encoded_returncode(proc.returncode)
        ):
            raise RemoteOutcomeUnknown(message)
        raise RemoteCommandError(
            message,
            returncode=proc.returncode,
            stderr=auth.redact_sensitive_text(proc.stderr or "", sensitive_values),
            stdout=auth.redact_sensitive_text(proc.stdout or "", sensitive_values),
        )
    return proc


def _remote_vq_error_message(
    host_cfg: HostConfig,
    remote_cmd: str,
    proc: subprocess.CompletedProcess[str],
    *,
    sensitive_values: tuple[str, ...] = (),
) -> str:
    from vq import auth  # noqa: PLC0415 — avoid cycle at import time

    stderr = auth.redact_sensitive_text(
        proc.stderr.strip(),
        sensitive_values,
    ) or "(empty)"
    lines = [
        f"remote vq failed ({_returncode_label(proc.returncode)}) "
        f"on {host_cfg.ssh}:",
        f"  cmd: {remote_cmd}",
        f"  stderr: {stderr}",
    ]
    if proc.returncode == 127:
        lines.extend(
            [
                "  hint: the configured remote_vq command was not found on "
                "the remote host.",
                f"  configured remote_vq: {host_cfg.remote_vq}",
                f"  next: run `vq doctor {host_cfg.ssh} --verbose` and either "
                f"update the host's vq install or fix "
                f"[hosts.{host_cfg.ssh}].remote_vq in config.toml.",
                f"  optional: to keep fleet sweeps quiet until repaired, run "
                f"`vq host down {host_cfg.ssh} --reason \"remote_vq missing\"`; "
                f"restore it with `vq host up {host_cfg.ssh}`.",
            ]
        )
    _signal_failure_hint(lines, proc.returncode, host_cfg.ssh)
    return "\n".join(lines)


class RemoteStream:
    """Handle yielded by :func:`stream_remote_vq`.

    ``stdout`` is the live binary pipe the caller reads (e.g. a tarball);
    ``stderr_text`` is the fully-drained remote stderr, populated once the
    context manager exits (so error messages raised *after* the ``with``
    block can include it).
    """

    def __init__(self, stdout: IO[bytes]) -> None:
        self.stdout = stdout
        self.stderr_text = ""


@contextlib.contextmanager
def stream_remote_vq(
    host_cfg: HostConfig, *vq_args: str
) -> Iterator[RemoteStream]:
    """Stream the *stdout* of ``<remote_vq> <vq_args...>`` over ssh.

    For large payloads (a workspace / workdir tarball) that must NOT be
    buffered into memory the way :func:`run_remote_vq` does. Yields a
    :class:`RemoteStream` whose ``.stdout`` pipe the caller reads.

    Hardening this provides (vs. a hand-rolled ``ssh`` Popen):

    * **``_ssh_base`` + ``shlex.join``** — the same ``ConnectTimeout`` /
      ``BatchMode=yes`` / ``ServerAlive`` options as every other ssh call,
      and the remote argv is shell-quoted into ONE command string so the
      remote shell hands ``remote_vq`` the exact tokens (the v0.5.32
      word-split class of bug, where ``ssh host a b c`` lets the remote
      shell re-tokenise ``a b c``).
    * **concurrent stderr drain** — a daemon thread reads stderr while the
      caller reads stdout, so a chatty remote can't fill the stderr pipe
      buffer and deadlock against our blocked stdout read.
    * **exit-code discipline** — on a *clean* exit (caller consumed the
      whole stream, no exception) the remote's exit code is checked and an
      ssh-transport failure (exit 255: connection refused / host
      unreachable / key rejected) is surfaced distinctly from a real
      ``remote_vq`` non-zero exit. If the caller's block raises (e.g.
      destination exists, truncated tar), the remote is killed and the
      caller's exception propagates — the rc is meaningless once we kill it,
      so we don't mask the caller's error with an rc-based one.
    """
    remote_cmd = shlex.join([host_cfg.remote_vq, *vq_args])
    cmd = [*_ssh_base(host_cfg), remote_cmd]
    log.debug("stream_remote_vq: %s", shlex.join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **_subprocess_session_kwargs(),
    )
    assert proc.stdout is not None
    stderr_pipe = proc.stderr
    assert stderr_pipe is not None
    stderr_chunks: list[bytes] = []

    def _drain() -> None:
        for chunk in iter(lambda: stderr_pipe.read(65536), b""):
            stderr_chunks.append(chunk)

    drainer = threading.Thread(target=_drain, daemon=True)
    drainer.start()
    handle = RemoteStream(proc.stdout)
    try:
        yield handle
    except BaseException:
        # The caller's block failed (destination exists, tar truncated,
        # KeyboardInterrupt, ...). Abort the remote; its exit code is now
        # meaningless, so don't second-guess the caller's exception with an
        # rc-based RemoteError — re-raise the caller's exception as-is.
        proc.kill()
        proc.wait()
        drainer.join()
        handle.stderr_text = b"".join(stderr_chunks).decode(errors="replace").strip()
        raise
    # Clean exit: the remote finished on its own. Drain stderr fully, reap.
    with contextlib.suppress(OSError):
        proc.stdout.close()
    rc = proc.wait()
    drainer.join()
    handle.stderr_text = b"".join(stderr_chunks).decode(errors="replace").strip()
    if rc == 255:
        raise RemoteError(
            f"ssh transport to {host_cfg.ssh} failed (exit 255): "
            + (
                handle.stderr_text
                or "(no stderr — connection refused / host unreachable / "
                "key rejected)"
            )
        )
    if rc < 0:
        raise RemoteError(
            f"ssh transport to {host_cfg.ssh} failed "
            f"({_returncode_label(rc)}): "
            + (
                handle.stderr_text
                or "(no stderr from ssh before signal termination)"
            )
        )
    if rc != 0:
        raise RemoteError(
            _remote_vq_error_message(
                host_cfg,
                remote_cmd,
                subprocess.CompletedProcess(
                    args=cmd,
                    returncode=rc,
                    stdout="",
                    stderr=handle.stderr_text,
                ),
            )
        )


def run_remote_shell(
    host_cfg: HostConfig,
    *shell_args: str,
    check: bool = True,
    timeout: float | None = DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS,
    stdin_data: str | None = None,
    retry_transient: int = 0,
    owned_process_group: bool = False,
    max_stdout_bytes: int | None = None,
    max_stderr_bytes: int | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run an argv on the remote via ssh, shlex-quoted.

    Same quoting model as :func:`run_remote_vq`: caller passes argv as
    separate string args; we :func:`shlex.join` into one shell-safe
    command line before handing to ssh. Used for housekeeping like
    ``run_remote_shell(host_cfg, "rm", "-f", remote_tar)``.

    If you specifically WANT the remote shell to interpret
    metacharacters (a glob, a redirection, a compound command), pass
    them through an explicit shell — e.g. ``run_remote_shell(host_cfg,
    "sh", "-c", "rm -f /tmp/vq-upload-*.tar")`` — so the inner ``sh``
    does the expansion in a known scope. The function name's "shell"
    refers to the SSH-remote shell that always runs; this function
    does not invoke a shell on the laptop.

    ``stdin_data`` (v1.0, mirroring :func:`run_remote_vq`): if provided,
    the string is piped to the remote process's stdin. The PBS/SGE
    scheduler dispatcher uses this to feed a rendered job script to a
    remote ``cat > <path>`` (or ``qsub`` reading the script on stdin)
    without materialising a local temp file or quoting a multi-line
    script through the argv.
    """
    if retry_transient < 0:
        raise ValueError("retry_transient must be >= 0")
    if (max_stdout_bytes is not None or max_stderr_bytes is not None) and not (
        owned_process_group
    ):
        raise ValueError("bounded remote-shell output requires owned_process_group")
    remote_cmd = shlex.join(shell_args)
    cmd = [*_ssh_base(host_cfg), remote_cmd]
    log.debug("run_remote_shell: %s", shlex.join(cmd))
    for attempt in range(retry_transient + 1):
        try:
            if owned_process_group:
                if timeout is None:
                    raise ValueError(
                        "owned remote-shell subprocess requires a timeout"
                    )
                proc = run_owned_subprocess(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout,
                    input=stdin_data,
                    max_stdout_bytes=max_stdout_bytes,
                    max_stderr_bytes=max_stderr_bytes,
                )
            else:
                proc = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=timeout,
                    input=stdin_data,
                    **_subprocess_session_kwargs(
                        has_stdin_data=stdin_data is not None
                    ),
                )
        except subprocess.TimeoutExpired as e:
            if attempt < retry_transient:
                time.sleep(_SSH_RETRY_BACKOFF_SECONDS * (attempt + 1))
                continue
            raise RemoteOutcomeUnknown(
                f"remote shell timed out after {timeout}s on {host_cfg.ssh}:\n"
                f"  cmd: {remote_cmd}"
            ) from e
        except (FileNotFoundError, PermissionError) as e:
            if attempt:
                raise RemoteOutcomeUnknown(
                    f"local ssh retry did not start for {host_cfg.ssh} after "
                    f"{attempt} earlier ambiguous transport attempt(s): {e}"
                ) from e
            raise RemoteLaunchError(
                f"local ssh process did not start for {host_cfg.ssh}: {e}"
            ) from e
        except SubprocessOutputLimitExceeded as e:
            # The owned observer terminates the SSH process when either pipe
            # crosses its cap.  A remote mutation may already have committed,
            # so this is the same replay-unsafe ambiguity as a timeout or lost
            # SSH connection, not proof of rejection.
            raise RemoteOutcomeUnknown(
                f"remote shell output exceeded its bounded capture on "
                f"{host_cfg.ssh}: {remote_cmd}"
            ) from e
        except OSError as e:
            raise RemoteOutcomeUnknown(
                f"local ssh observer failed for {host_cfg.ssh}: {e}"
            ) from e
        if proc.returncode == 255 and attempt < retry_transient:
            log.debug(
                "run_remote_shell: transient ssh exit 255 on %s (retry %d/%d)",
                host_cfg.ssh,
                attempt + 1,
                retry_transient,
            )
            time.sleep(_SSH_RETRY_BACKOFF_SECONDS * (attempt + 1))
            continue
        break
    if check and proc.returncode != 0:
        message = _remote_shell_error_message(host_cfg, remote_cmd, proc)
        if (
            proc.returncode == 255
            or proc.returncode < 0
            or _is_signal_encoded_returncode(proc.returncode)
        ):
            raise RemoteOutcomeUnknown(message)
        raise RemoteError(message)
    return proc


def _remote_shell_error_message(
    host_cfg: HostConfig,
    remote_cmd: str,
    proc: subprocess.CompletedProcess[str],
) -> str:
    stderr = proc.stderr.strip() or "(empty)"
    lines = [
        f"remote shell failed ({_returncode_label(proc.returncode)}) "
        f"on {host_cfg.ssh}:",
        f"  cmd: {remote_cmd}",
        f"  stderr: {stderr}",
    ]
    if proc.returncode == 127:
        lines.extend(
            [
                "  hint: the remote shell command was not found on the "
                "remote host.",
                "  next: check the host's vq config for stale scheduler, "
                "cleanup, or provisioning command paths and run "
                f"`vq doctor {host_cfg.ssh} --verbose`.",
            ]
        )
    _signal_failure_hint(lines, proc.returncode, host_cfg.ssh)
    return "\n".join(lines)


def _run_scp_with_retry(
    operation: str,
    host_cfg: HostConfig,
    *,
    build_cmd: Callable[[bool], list[str]],
    remote_path: str,
    local_path: Path,
    timeout: float | None,
    retry_transient: int,
    on_retry: Callable[[int, int, float, str], None] | None,
) -> None:
    """Shared scp driver for :func:`upload_file` / :func:`download_file`.

    Retries only *transport* failures — scp exit 255 or a local timeout — for
    callers that explicitly declare the transfer replay-safe. Those outcomes
    do not prove the first transfer was absent. Staging uploads reuse the same
    target and content and are verified before activation. A non-255 scp
    failure is a real error about the file or destination ("No space left on
    device", "Permission denied", a bad path), and retrying it would burn the
    whole backoff window on something that cannot succeed.

    Every retry bypasses SSH connection multiplexing, because the failure this
    exists for — a persisted master socket whose gateway moved — is invisible
    to an attempt that reuses the same socket.
    """
    if retry_transient < 0:
        raise ValueError("retry_transient must be >= 0")
    for attempt in range(retry_transient + 1):
        fresh = attempt > 0
        cmd = build_cmd(fresh)
        log.debug("%s_file: %s", operation, shlex.join(cmd))
        failure: str | None = None
        try:
            proc = run_owned_subprocess(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
                max_stdout_bytes=SCP_STDOUT_MAX_BYTES,
                max_stderr_bytes=SCP_STDERR_MAX_BYTES,
            )
        except subprocess.TimeoutExpired as e:
            message = _scp_timeout_message(
                operation,
                host_cfg,
                remote_path=remote_path,
                local_path=local_path,
                timeout=timeout,
            )
            message += f"\n  transfer stopped after {attempt + 1} attempt(s)"
            if attempt >= retry_transient:
                raise RemoteOutcomeUnknown(message) from e
            failure = f"timed out after {timeout}s"
        except (FileNotFoundError, PermissionError) as e:
            if attempt:
                raise RemoteOutcomeUnknown(
                    f"local scp retry did not start for {host_cfg.ssh} after "
                    f"{attempt} earlier ambiguous transport attempt(s): {e}"
                ) from e
            raise RemoteLaunchError(
                f"local scp process did not start for {host_cfg.ssh}: {e}"
            ) from e
        except OSError as e:
            raise RemoteOutcomeUnknown(
                f"local scp observer failed for {host_cfg.ssh}: {e}"
            ) from e
        except SubprocessOutputLimitExceeded as e:
            raise RemoteOutcomeUnknown(
                f"scp {operation} output exceeded its bounded capture on "
                f"{host_cfg.ssh}"
            ) from e
        else:
            if proc.returncode == 0:
                return
            message = _scp_error_message(
                operation,
                host_cfg,
                remote_path=remote_path,
                local_path=local_path,
                proc=proc,
            )
            message += f"\n  transfer stopped after {attempt + 1} attempt(s)"
            ambiguous = (
                proc.returncode == 255
                or proc.returncode < 0
                or _is_signal_encoded_returncode(proc.returncode)
            )
            if not ambiguous:
                raise RemoteError(message)
            if attempt >= retry_transient:
                raise RemoteOutcomeUnknown(message)
            failure = f"{_returncode_label(proc.returncode)}"
        delay = _retry_delay(
            attempt,
            base=_UPLOAD_RETRY_BACKOFF_BASE_SECONDS,
            cap=_UPLOAD_RETRY_BACKOFF_MAX_SECONDS,
        )
        log.warning(
            "scp %s: transient failure on %s (%s); retrying %d/%d in %.0fs "
            "on a fresh connection",
            operation,
            host_cfg.ssh,
            failure,
            attempt + 1,
            retry_transient,
            delay,
        )
        if on_retry is not None:
            on_retry(attempt + 1, retry_transient, delay, failure)
        time.sleep(delay)


def upload_file(
    host_cfg: HostConfig,
    local_path: Path,
    remote_path: str,
    *,
    timeout: float | None = DEFAULT_UPLOAD_TIMEOUT_SECONDS,
    retry_transient: int = 0,
    on_retry: Callable[[int, int, float, str], None] | None = None,
) -> None:
    """Upload a single local file to ``remote_path`` via scp.

    ``retry_transient`` (default 0, so existing callers are unchanged) retries
    transport-level failures with exponential backoff on a fresh, unmultiplexed
    connection — see :func:`_run_scp_with_retry`. ``on_retry`` is called before
    each backoff sleep as ``(attempt, total, delay_seconds, reason)`` so a
    long-running caller can surface progress instead of going quiet.

    Raises :class:`RemoteOutcomeUnknown` when the local observer cannot prove
    whether the transfer completed, and :class:`RemoteError` for a committed
    remote rejection.
    """
    _run_scp_with_retry(
        "upload",
        host_cfg,
        build_cmd=lambda fresh: [
            *_scp_base(fresh_connection=fresh),
            str(local_path),
            f"{host_cfg.ssh}:{remote_path}",
        ],
        remote_path=remote_path,
        local_path=local_path,
        timeout=timeout,
        retry_transient=retry_transient,
        on_retry=on_retry,
    )


def download_file(
    host_cfg: HostConfig,
    remote_path: str,
    local_path: Path,
    *,
    timeout: float | None = DEFAULT_UPLOAD_TIMEOUT_SECONDS,
    retry_transient: int = 0,
    on_retry: Callable[[int, int, float, str], None] | None = None,
) -> None:
    """Download a single remote file to ``local_path`` via scp.

    The mirror of :func:`upload_file`, used by the PBS/SGE scheduler dispatcher
    to stage a finished job's result tarball back from the cluster. Raises
    :class:`RemoteError` on failure.
    """
    _run_scp_with_retry(
        "download",
        host_cfg,
        build_cmd=lambda fresh: [
            *_scp_base(fresh_connection=fresh),
            f"{host_cfg.ssh}:{remote_path}",
            str(local_path),
        ],
        remote_path=remote_path,
        local_path=local_path,
        timeout=timeout,
        retry_transient=retry_transient,
        on_retry=on_retry,
    )


def _scp_timeout_message(
    operation: str,
    host_cfg: HostConfig,
    *,
    remote_path: str,
    local_path: Path,
    timeout: float | None,
) -> str:
    if operation == "upload":
        location = f"to {host_cfg.ssh}:{remote_path}"
    else:
        location = f"from {host_cfg.ssh}:{remote_path}"
    return "\n".join(
        [
            f"scp {operation} timed out after {timeout}s {location}:",
            f"  local: {local_path}",
            "  hint: the SSH connection or remote filesystem transfer stalled.",
            "  next: check host reachability and key trust, then run "
            f"`vq doctor {host_cfg.ssh} --verbose`.",
        ]
    )


def _scp_error_message(
    operation: str,
    host_cfg: HostConfig,
    *,
    remote_path: str,
    local_path: Path,
    proc: subprocess.CompletedProcess[str],
) -> str:
    stderr = proc.stderr.strip() or "(empty)"
    if operation == "upload":
        location = f"to {host_cfg.ssh}:{remote_path}"
    else:
        location = f"from {host_cfg.ssh}:{remote_path}"
    lines = [
        f"scp {operation} failed ({_returncode_label(proc.returncode)}) "
        f"{location}:",
        f"  local: {local_path}",
        f"  stderr: {stderr}",
    ]
    stderr_lower = stderr.lower()
    if proc.returncode == 255:
        lines.extend(
            [
                "  hint: scp failed at the SSH transport/auth layer.",
                "  next: check host reachability and key trust, then run "
                f"`vq doctor {host_cfg.ssh} --verbose`.",
            ]
        )
    elif proc.returncode < 0:
        _signal_failure_hint(lines, proc.returncode, host_cfg.ssh)
    elif any(
        marker in stderr_lower
        for marker in (
            "no space left",
            "disk quota exceeded",
            "quota exceeded",
        )
    ):
        lines.extend(
            [
                "  hint: scp reached the host but the remote filesystem appears "
                "full or over quota.",
                f"  next: free space or run `vq cleanup {host_cfg.ssh} "
                "--auto-status` before retrying.",
            ]
        )
    elif any(
        marker in stderr_lower
        for marker in (
            "no such file",
            "not a directory",
            "permission denied",
            "failure",
        )
    ):
        lines.extend(
            [
                "  hint: scp reached the host but the source/destination path "
                "or permissions look wrong.",
                "  next: check the configured scratch/workspace path and "
                "remote filesystem permissions.",
            ]
        )
    return "\n".join(lines)


def remote_temp_tar_path() -> str:
    """Return a unique **home-relative** staging tarball path for a remote upload.

    Deliberately NOT under node-local ``/tmp``. A multi-login-node cluster
    (e.g. slurm-cluster's balanced login01/login02) routes the ``scp`` upload and the
    subsequent ssh that consumes the tarball to *different* login nodes, and
    ``/tmp`` is node-local — the tarball scp'd onto one node is then invisible
    to the command on the other (``… Cannot open: No such file or directory``).
    A path relative to the remote ``$HOME`` — the default working directory of
    both ``ssh <host> <cmd>`` and scp's SFTP transfer — lands on the shared home
    filesystem that every login node sees, so node rotation between the two
    connections no longer matters. ``$HOME`` always exists, so no ``mkdir``
    round-trip is needed; the leading dot keeps the transient tarball out of a
    plain ``ls`` while it exists.

    Caller is responsible for cleaning it up after the remote has consumed
    the tarball (e.g. with :func:`run_remote_shell` ``("rm", "-f", path)``).
    """
    return f".vq-upload-{uuid.uuid4().hex[:12]}.tar"
