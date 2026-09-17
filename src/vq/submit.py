"""Submit logic.

Two entry points:

* :func:`submit_local` -- materialize a workspace under the local state dir,
  write a spec, return jobid. Used directly by the CLI when the target host
  is local, and indirectly by the remote daemon (the local vq's submit
  remote upload pipes the workspace through this on the far end).

* :func:`submit_remote` -- pack the workspace into a temp tarball, scp it
  to the remote, invoke ``<host_cfg.remote_vq> submit localhost -c <tar>
  -- <cmd>`` over ssh, capture and return the jobid printed by the remote.
"""

from __future__ import annotations

import contextlib
import errno
import fcntl
import getpass
import hashlib
import io
import json
import logging
import os
import re
import shutil
import socket
import stat
import sys
import tarfile
import tempfile
import unicodedata
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath

from vq import capacity, config, drain, events, paths, spec_access, transport
from vq.config import HostConfig, VenvProgram
from vq.host import is_local_host
from vq.scheduler_dialect import (
    enforce_scheduler_wall_time_limit,
    scheduler_width_warning,
)
from vq.spec import (
    JOB_NAME_MAX_LEN,
    JOB_NAME_PATTERN,
    JobSpec,
    JobState,
    ProgramRuntimePin,
    validate_job_id,
)
from vq.vibeqc_preflight import vibeqc_dry_run_preflight

log = logging.getLogger(__name__)

_SUBMIT_WARNING_PREFIX = "vq: warning: "
_IMPOSSIBLE_CAPACITY_WARNING_PATTERNS = (
    re.compile(
        r"requested [0-9]+ CPUs but this daemon caps at --max-cpus [0-9]+; "
        r"job (?P<jobid>[0-9a-f]{12}) will park PENDING until the daemon "
        r"is restarted with a higher cap"
    ),
    re.compile(
        r"requested [0-9]+ MB memory but this daemon caps at --max-mem(?:-mb)? "
        r"[0-9]+ MB; job (?P<jobid>[0-9a-f]{12}) will park PENDING until "
        r"the daemon is restarted with a higher cap"
    ),
    re.compile(
        r"undeclared memory is charged at daemon default [0-9]+ MB but this "
        r"daemon caps at --max-mem(?:-mb)? [0-9]+ MB; job "
        r"(?P<jobid>[0-9a-f]{12}) will park PENDING until the daemon is "
        r"restarted with a higher cap"
    ),
)

_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_IDEMPOTENCY_CLAIM_SCHEMA = "vq.submit-idempotency.v1"
_IDEMPOTENCY_CLAIM_LIMIT = 16 * 1024
REMOTE_SUBMIT_STDOUT_MAX_BYTES = 1024 * 1024
REMOTE_SUBMIT_STDERR_MAX_BYTES = 64 * 1024
REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES = 4096
REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES = 16 * 1024


def _non_masking_warning(message: str, *args: object) -> None:
    """Emit advisory diagnostics without ever replacing an authority result."""
    with contextlib.suppress(BaseException):
        log.warning(message, *args)


class IdempotencyConflict(ValueError):
    """One owner reused a submit key for a different canonical intent."""


class _AuthorityReplacedError(ValueError):
    """A held queue/workspace authority lost its canonical pathname."""


class _IdempotencyAuthorityReplacedError(_AuthorityReplacedError):
    """The held queue/claim authority is no longer canonically reachable."""


class _WorkspaceAuthorityReplacedError(_AuthorityReplacedError):
    """The held jobs/workspace authority is no longer canonically reachable."""


@dataclass(frozen=True)
class _IdempotencyBinding:
    claim_path: Path
    owner_hash: str
    key_hash: str
    intent_digest: str


@dataclass(frozen=True)
class _IdempotencyStore:
    """Open, inode-stable authority directory held by the per-key lock."""

    queue_path: Path
    queue_fd: int
    namespace_fd: int
    owner_fd: int
    owner_name: str
    authority_uid: int
    authority_gid: int
    lock_fd: int | None
    lock_name: str | None
    claim_name: str


@dataclass(frozen=True)
class _PayloadSnapshot:
    """One immutable local copy used for intent digest and staging."""

    source_file: Path | None
    source_directory: Path | None
    source_archive: Path | None


@dataclass(frozen=True)
class _ValidatedSubmitPayload:
    """Source shape resolved before target-specific submit planning."""

    input_file: str | None
    directory: str | None
    archive: str | None
    command: list[str] | None
    is_qvf_input: bool


@dataclass(frozen=True)
class _ResolvedRequestedSubmitTarget:
    """User-facing target before scheduler-driver projection."""

    requested_host: str
    remaining_args: tuple[str, ...]
    auto_selected: bool
    placement_mem_mb: int | None
    estimated_mem_mb: int | None


@dataclass(frozen=True)
class _ResolvedSubmitBranch:
    """Interpreter path and durable branch label for the requested target."""

    python: str | None
    branch_for_spec: str | None


@dataclass(frozen=True)
class _ClassifiedSubmitTarget:
    """Requested target classified without resolving its driver config."""

    requested_host: str
    receipt_host: str
    requested_host_config: HostConfig | None
    scheduler_target: str | None
    driver_host: str | None


@dataclass(frozen=True)
class _ResolvedSubmitExecutionTarget:
    """Host that stores the spec and owns status, wait, and fetch."""

    requested_host: str
    receipt_host: str
    execution_host: str
    execution_host_config: HostConfig | None
    scheduler_target: str | None


@dataclass(frozen=True)
class _NormalizedSubmitRequest:
    """Validated payload and options independent of execution placement."""

    input_file: str | None
    directory: str | None
    archive: str | None
    command: list[str] | None
    is_qvf_input: bool
    python: str | None
    cpus: int
    scheduler_tasks: int | None
    mem_mb: int | None
    wall_time_seconds: int | None
    priority: int
    auto_resume: bool
    retry: int
    job_name: str | None
    branch: str | None
    program: str | None
    expected_sha: str | None
    tags: list[str] | None
    not_before: str | None
    depends_on: list[str] | None
    depends_on_any: list[str] | None
    rerun_until_file_exists: str | None
    rerun_max: int
    clean_workdir_on_terminal: bool
    vibeqc_preflight: bool
    array: int
    chain: int
    refresh_before: str | None
    qvf_force: bool
    idempotency_key: str | None = None


@dataclass(frozen=True)
class _ResolvedSubmitPlan:
    """Concrete target, request, and runtime identities for one submit."""

    target: _ResolvedSubmitExecutionTarget
    request: _NormalizedSubmitRequest
    program_runtime_pin: ProgramRuntimePin | None
    receipt_runtime_pin: ProgramRuntimePin | None


def _validate_submit_source_choice(
    *,
    directory: str | None,
    archive: str | None,
) -> None:
    """Reject mutually exclusive workspace sources."""
    if directory and archive:
        raise ValueError("--dir and --compressed are mutually exclusive")


def _resolve_requested_submit_target(
    cfg: config.Config,
    *,
    host_opt: str | None,
    positional: Sequence[str],
    pool: str | None,
    cpus: int,
    mem_mb: int | None,
    directory: str | None,
    archive: str | None,
    estimate_job_mem_mb: Callable[[config.Config, list[str]], int | None],
    pick_auto_host: Callable[..., str],
) -> _ResolvedRequestedSubmitTarget:
    """Resolve the requested host without rendering placement narration."""
    args = list(positional)
    auto_selected = bool(
        host_opt == "auto"
        or (not host_opt and args and args[0] == "auto")
    )
    if pool is not None and not auto_selected:
        raise ValueError(
            "--pool only applies to `vq submit auto` (it scopes the "
            "memory-aware host pick to a [pools.POOL] group)."
        )

    estimated_mem_mb: int | None = None
    placement_mem_mb = mem_mb
    if auto_selected:
        remaining_args = args if host_opt == "auto" else args[1:]
        if placement_mem_mb is None and not directory and not archive:
            estimated_mem_mb = estimate_job_mem_mb(cfg, remaining_args)
            placement_mem_mb = estimated_mem_mb
        requested_host = pick_auto_host(
            cfg,
            cpus,
            pool=pool,
            job_mem_mb=placement_mem_mb,
        )
    elif host_opt:
        requested_host = host_opt
        remaining_args = args
    elif args and (args[0] in cfg.hosts or is_local_host(args[0])):
        requested_host = args[0]
        remaining_args = args[1:]
    else:
        try:
            requested_host = cfg.resolve_host(None)
        except config.ConfigError as e:
            # The CLI historically rendered a missing default host as a
            # usage error, while placement errors from the auto picker kept
            # their native failure class.
            raise ValueError(str(e)) from None
        remaining_args = args

    return _ResolvedRequestedSubmitTarget(
        requested_host=requested_host,
        remaining_args=tuple(remaining_args),
        auto_selected=auto_selected,
        placement_mem_mb=placement_mem_mb,
        estimated_mem_mb=estimated_mem_mb,
    )


def _resolve_submit_branch(
    cfg: config.Config,
    *,
    requested_host: str,
    branch_name: str | None,
    branch_name_passthrough: str | None,
    python_path: str | None,
) -> _ResolvedSubmitBranch:
    """Resolve an interpreter against the requested host, before rewriting."""
    if branch_name is not None:
        if python_path is not None:
            raise ValueError(
                "--branch and --python are mutually exclusive; "
                "--branch resolves a name through the host's [branches] "
                "table, --python takes a literal interpreter path"
            )
        try:
            host_cfg = cfg.host(requested_host)
        except config.ConfigError:
            raise ValueError(
                f"--branch requires a [hosts.{requested_host}] section in "
                f"{config.config_path()}; branches live under "
                f"[hosts.{requested_host}.branches]"
            ) from None
        resolved = host_cfg.resolve_branch(branch_name)
        if resolved is None:
            known = host_cfg.known_branch_names()
            if known:
                raise ValueError(
                    f"unknown --branch '{branch_name}' for host "
                    f"'{requested_host}'; known: {', '.join(known)}"
                )
            raise ValueError(
                f"host '{requested_host}' has no [branches] configured; "
                f"add a [hosts.{requested_host}.branches] section to "
                f"{config.config_path()}"
            )
        python_path = resolved

    # Deliberately after branch lookup: an invalid operator branch remains the
    # first error even when the internal forwarded label is also present.
    if branch_name is not None and branch_name_passthrough is not None:
        raise ValueError(
            "--branch and --branch-name are mutually exclusive "
            "(--branch-name is internal-only, forwarded by "
            "`submit_remote` after laptop-side --branch resolution; "
            "operators should use --branch)"
        )
    return _ResolvedSubmitBranch(
        python=python_path,
        branch_for_spec=branch_name or branch_name_passthrough,
    )


def _resolve_submit_payload(
    *,
    directory: str | None,
    archive: str | None,
    args: Sequence[str],
    qvf_force: bool,
    array: int,
    chain: int,
) -> _ValidatedSubmitPayload:
    """Validate and normalize the CLI payload shape without side effects."""
    input_file: str | None
    command: list[str] | None

    if directory or archive:
        if not args:
            raise ValueError(
                "--dir / --compressed require a command, e.g. `python run.py`"
            )
        input_file = None
        command = list(args)
    elif args:
        if len(args) > 1:
            raise ValueError(
                f"single-file submit takes exactly one INPUT file "
                f"(got {len(args)} positionals); use --dir or --compressed "
                f"for multi-file"
            )
        input_file = args[0]
        command = None
    else:
        raise ValueError("provide an input file or one of --dir/--compressed")

    is_qvf_input = bool(
        input_file is not None and Path(input_file).suffix.lower() == ".qvf"
    )
    if qvf_force and not is_qvf_input:
        raise ValueError("--qvf-force only applies to a single .qvf input")
    if is_qvf_input and (array > 1 or chain > 1):
        raise ValueError(
            "single-QVF submit does not support --array/--chain; submit "
            "independent container files so each result artifact is unique"
        )
    return _ValidatedSubmitPayload(
        input_file=input_file,
        directory=directory,
        archive=archive,
        command=command,
        is_qvf_input=is_qvf_input,
    )


def _validate_submit_variants(
    *,
    array: int,
    chain: int,
    refresh_before: str | None,
    idempotency_key: str | None = None,
) -> None:
    """Validate cardinality combinations after common option parsing."""
    if array > 1 and chain > 1:
        raise ValueError(
            "--array and --chain are mutually exclusive: array "
            "siblings run in parallel as independent jobs, chain "
            "links run strictly sequentially. Pick one."
        )
    if refresh_before is not None and (array > 1 or chain > 1):
        raise ValueError(
            "--refresh is not supported with --array / --chain in v1 "
            "(it rebuilds the env once before a single job runs). Submit "
            "the refresh as a standalone single-file job, then the "
            "array/chain without --refresh."
        )
    if idempotency_key is not None and (array > 1 or chain > 1):
        raise ValueError(
            "--idempotency-key initially supports one single logical job; "
            "--array and --chain are not supported"
        )


def _validate_remote_submit_variants(
    *,
    requested_host: str,
    is_local: bool,
    scheduler_submit_host: bool,
    chain: int,
    rerun_until_file_exists: str | None,
) -> None:
    """Reject variants unsupported by an ordinary remote daemon."""
    if is_local or scheduler_submit_host:
        return
    if chain > 1:
        raise ValueError(
            f"--chain is local-only; host {requested_host!r} is remote. The "
            "remote daemon never receives the chain linkage, so only one "
            "link would run. Run the chain on the remote host directly "
            "(ssh <host> vq submit --chain ...), or submit locally."
        )
    if rerun_until_file_exists:
        raise ValueError(
            f"--rerun-until is local-only; host {requested_host!r} is "
            "remote. The remote daemon never receives the flag, so the job "
            "would run once and report COMPLETED without the convergence "
            "loop. Run it on the remote host directly, or submit locally."
        )


def _classify_submit_target(
    cfg: config.Config,
    *,
    requested_host: str,
    delegated_scheduler_target: str | None,
) -> _ClassifiedSubmitTarget:
    """Classify a target without loading a scheduler driver's config."""
    if is_local_host(requested_host):
        return _ClassifiedSubmitTarget(
            requested_host=requested_host,
            receipt_host=requested_host,
            requested_host_config=None,
            scheduler_target=delegated_scheduler_target,
            driver_host=None,
        )
    requested_host_config = cfg.host(requested_host)
    if requested_host_config.scheduler == "local":
        return _ClassifiedSubmitTarget(
            requested_host=requested_host,
            receipt_host=requested_host,
            requested_host_config=requested_host_config,
            scheduler_target=delegated_scheduler_target,
            driver_host=None,
        )
    return _ClassifiedSubmitTarget(
        requested_host=requested_host,
        receipt_host=requested_host,
        requested_host_config=requested_host_config,
        scheduler_target=requested_host,
        driver_host=requested_host_config.scheduler_driver,
    )


def _resolve_submit_execution_target(
    cfg: config.Config,
    target: _ClassifiedSubmitTarget,
) -> _ResolvedSubmitExecutionTarget:
    """Resolve the spec-owning host after target runtime validation."""
    host_cfg = target.requested_host_config
    if host_cfg is None:
        return _ResolvedSubmitExecutionTarget(
            requested_host=target.requested_host,
            receipt_host=target.receipt_host,
            execution_host=target.requested_host,
            execution_host_config=None,
            scheduler_target=target.scheduler_target,
        )
    if host_cfg.scheduler == "local":
        return _ResolvedSubmitExecutionTarget(
            requested_host=target.requested_host,
            receipt_host=target.receipt_host,
            execution_host=target.requested_host,
            execution_host_config=host_cfg,
            scheduler_target=target.scheduler_target,
        )

    driver = target.driver_host
    if driver is None:
        raise ValueError(
            f"scheduler host {target.requested_host!r} has no "
            "scheduler_driver"
        )
    if is_local_host(driver):
        driver_cfg = None
    else:
        try:
            driver_cfg = cfg.host(driver)
        except config.ConfigError as e:
            raise ValueError(
                f"scheduler host {target.requested_host!r} names driver "
                f"{driver!r}, which is not in config: {e}"
            ) from None
    return _ResolvedSubmitExecutionTarget(
        requested_host=target.requested_host,
        receipt_host=target.receipt_host,
        execution_host=driver,
        execution_host_config=driver_cfg,
        scheduler_target=target.scheduler_target,
    )


_PYTHON_LAUNCHER_RE = re.compile(r"python(?:3(?:\.\d+)*t?)?$")
_PYTHON_OPTIONS_WITH_ARGUMENT = frozenset(
    {"-W", "-X", "--check-hash-based-pycs"}
)
_PYTHON_TERMINATING_OPTIONS = frozenset(
    {
        "-?",
        "-h",
        "--help",
        "--help-all",
        "--help-env",
        "--help-xoptions",
        "-V",
        "-VV",
        "--version",
    }
)
_ENV_ASSIGNMENT_RE = re.compile(r"[^=]*=.*", re.DOTALL)
_ENV_OPTIONS_WITH_ARGUMENT = frozenset(
    {"-a", "--argv0", "-C", "--chdir", "-P", "-u", "--unset"}
)
_ENV_OPTIONS_WITHOUT_ARGUMENT = frozenset(
    {
        "-i",
        "--ignore-environment",
        "-v",
        "--debug",
        "--list-signal-handling",
    }
)
_ENV_OPTIONS_WITH_OPTIONAL_ARGUMENT = frozenset(
    {"--block-signal", "--default-signal", "--ignore-signal"}
)
_ENV_TERMINATING_OPTIONS = frozenset(
    {"-0", "--null", "--help", "--version"}
)
_ENV_SHORT_OPTIONS_WITHOUT_ARGUMENT = frozenset({"i", "v"})
_ENV_SHORT_OPTIONS_WITH_ARGUMENT = frozenset({"a", "C", "P", "S", "u"})


class PayloadValidationError(ValueError):
    """A staged payload cannot satisfy the command it declares."""


def _is_python_launcher(command_head: str) -> bool:
    """Return whether an argv head names Python or a managed Python wrapper."""
    basename = PurePosixPath(command_head).name
    return bool(
        _PYTHON_LAUNCHER_RE.fullmatch(basename)
        or basename.endswith("-python")
    )


def _split_env_string(value: str) -> list[str]:
    """Split one GNU/BSD ``env -S`` operand without shell semantics.

    ``env -S`` is deliberately not ``shlex``: outside quotes ``\\_`` is an
    argument separator, a newly started ``#`` argument comments out the
    remainder, and ``\\c`` terminates the split. Environment substitution is
    target-dependent, so a payload using ``${NAME}`` cannot be proven safe on
    the submitting host and is rejected with an actionable validation error.
    """
    result: list[str] = []
    current: list[str] = []
    token_started = False
    quote: str | None = None
    index = 0

    def finish_token() -> None:
        nonlocal token_started
        if token_started:
            result.append("".join(current))
            current.clear()
            token_started = False

    while index < len(value):
        char = value[index]
        if quote is None and char in {" ", "\t", "\n", "\r", "\v", "\f"}:
            finish_token()
            index += 1
            continue
        if char in {"'", '"'}:
            if quote is None:
                quote = char
                token_started = True
                index += 1
                continue
            if quote == char:
                quote = None
                index += 1
                continue
            current.append(char)
            token_started = True
            index += 1
            continue
        if char == "#" and not token_started:
            break
        if (
            char == "$"
            and quote != "'"
            and index + 1 < len(value)
            and value[index + 1] == "{"
        ):
            raise PayloadValidationError(
                "payload validation failed: /usr/bin/env -S environment "
                "substitution is target-dependent and cannot be validated; "
                "spell the command argv explicitly"
            )
        if char != "\\":
            current.append(char)
            token_started = True
            index += 1
            continue

        if index + 1 >= len(value):
            raise ValueError("trailing backslash in env -S operand")
        escaped = value[index + 1]
        if quote == "'" and escaped not in {"'", "\\"}:
            current.extend(("\\", escaped))
            token_started = True
            index += 2
            continue
        if escaped in {" ", "\t", "\n", "\r", "\v", "\f"}:
            current.append(escaped)
            token_started = True
            index += 2
            continue
        if escaped == "c":
            if quote == '"':
                raise ValueError("env -S \\c is invalid inside double quotes")
            finish_token()
            return result
        replacements = {
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
            "v": "\v",
            "#": "#",
            "$": "$",
            '"': '"',
            "'": "'",
            "\\": "\\",
        }
        if escaped == "_":
            if quote == '"':
                current.append(" ")
                token_started = True
            else:
                finish_token()
            index += 2
            continue
        replacement = replacements.get(escaped)
        if replacement is None:
            raise ValueError(f"invalid env -S escape: \\{escaped}")
        current.append(replacement)
        token_started = True
        index += 2

    if quote is not None:
        raise ValueError("unterminated quote in env -S operand")
    finish_token()
    return result


def _env_wrapped_command(
    command: Sequence[str],
) -> tuple[list[str], str | None] | None:
    """Return the command operand of a recognized ``env`` invocation.

    This parser exists only for staged-entrypoint validation. It deliberately
    does not make ``env`` authoritative for runtime routing or SHA selection.
    The supported grammar covers POSIX/GNU/BSD option, assignment, and ``-S``
    forms without guessing past an unknown option.
    """
    if not command or PurePosixPath(command[0]).name != "env":
        return None
    args = list(command[1:])
    index = 0
    options_done = False
    working_directory: str | None = None
    while index < len(args):
        token = args[index]
        if not options_done:
            if token == "--":
                options_done = True
                index += 1
                continue
            if token in _ENV_TERMINATING_OPTIONS:
                return [], working_directory
            if token == "-" or token in _ENV_OPTIONS_WITHOUT_ARGUMENT:
                index += 1
                continue
            if token in _ENV_OPTIONS_WITH_OPTIONAL_ARGUMENT or any(
                token.startswith(f"{option}=")
                for option in _ENV_OPTIONS_WITH_OPTIONAL_ARGUMENT
            ):
                index += 1
                continue
            if token in _ENV_OPTIONS_WITH_ARGUMENT:
                if index + 1 >= len(args):
                    return [], working_directory
                if token in {"-C", "--chdir"}:
                    working_directory = args[index + 1]
                index += 2
                continue
            if token.startswith("--chdir="):
                working_directory = token.partition("=")[2]
                index += 1
                continue
            if token.startswith(("--argv0=", "--unset=")):
                index += 1
                continue
            if token == "--split-string":
                if index + 1 >= len(args):
                    return [], working_directory
                try:
                    split = _split_env_string(args[index + 1])
                except PayloadValidationError:
                    raise
                except ValueError:
                    return [], working_directory
                args[index : index + 2] = split
                continue
            if token.startswith("--split-string="):
                try:
                    split = _split_env_string(token.partition("=")[2])
                except PayloadValidationError:
                    raise
                except ValueError:
                    return [], working_directory
                args[index : index + 1] = split
                continue
            if token.startswith("-") and not token.startswith("--"):
                cluster = token[1:]
                position = 0
                split_applied = False
                while position < len(cluster):
                    option = cluster[position]
                    if option == "0":
                        # env refuses to combine NUL-delimited environment
                        # output with a utility operand, so no later token can
                        # be a launched Python command.
                        return [], working_directory
                    if option in _ENV_SHORT_OPTIONS_WITHOUT_ARGUMENT:
                        position += 1
                        continue
                    if option not in _ENV_SHORT_OPTIONS_WITH_ARGUMENT:
                        raise PayloadValidationError(
                            "payload validation failed: unsupported "
                            f"/usr/bin/env option {token!r}; spell the "
                            "wrapped command without abbreviated or "
                            "implementation-specific env options"
                        )
                    attached = cluster[position + 1 :]
                    if option == "S":
                        if attached:
                            split_source = attached
                            consumed = 1
                        elif index + 1 < len(args):
                            split_source = args[index + 1]
                            consumed = 2
                        else:
                            return [], working_directory
                        try:
                            split = _split_env_string(split_source)
                        except PayloadValidationError:
                            raise
                        except ValueError:
                            return [], working_directory
                        args[index : index + consumed] = split
                        split_applied = True
                    else:
                        if attached:
                            option_argument = attached
                            index += 1
                        elif index + 1 < len(args):
                            option_argument = args[index + 1]
                            index += 2
                        else:
                            return [], working_directory
                        if option == "C":
                            working_directory = option_argument
                    break
                if split_applied:
                    continue
                if position == len(cluster):
                    index += 1
                continue
            if token.startswith("-"):
                raise PayloadValidationError(
                    "payload validation failed: unsupported "
                    f"/usr/bin/env option {token!r}; spell the wrapped "
                    "command without abbreviated or implementation-specific "
                    "env options"
                )
        if _ENV_ASSIGNMENT_RE.fullmatch(token):
            # GNU and BSD env stop option parsing at the first NAME=VALUE.
            # A later ``-S`` or ``--`` is the utility operand, not another
            # env option that can reveal a Python command farther in argv.
            options_done = True
            index += 1
            continue
        break
    return args[index:], working_directory


def _staged_python_entrypoint(
    command: Sequence[str],
    *,
    interpreter_explicit: bool = False,
) -> PurePosixPath | None:
    """Return the relative Python script operand that must be staged.

    Only Python's file-execution mode has a staged entrypoint. ``-m``, ``-c``,
    stdin, interpreter-only/help invocations, absolute target-side paths, and
    arbitrary non-Python commands deliberately return ``None``.
    """
    if not command:
        return None
    argv = list(command)
    env_working_directories: list[str] = []
    for _env_depth in range(16):
        env_invocation = _env_wrapped_command(argv)
        if env_invocation is None:
            break
        env_command, env_working_directory = env_invocation
        if not env_command:
            return None
        argv = env_command
        interpreter_explicit = False
        if env_working_directory is not None:
            env_working_directories.append(env_working_directory)
    else:
        if _env_wrapped_command(argv) is not None:
            raise PayloadValidationError(
                "payload validation failed: nested /usr/bin/env wrappers "
                "exceed the supported validation depth"
            )

    head = argv[0]
    raw_head_parts = head.split("/")
    raw_head_python = next(
        (part for part in reversed(raw_head_parts) if part not in {"", "."}),
        "",
    ).lower().endswith(".py")
    if raw_head_python and not interpreter_explicit:
        candidate = head
    elif interpreter_explicit or _is_python_launcher(head):
        candidate = None
        index = 1
        while index < len(argv):
            token = argv[index]
            if token == "--":
                index += 1
                candidate = argv[index] if index < len(argv) else None
                break
            if token in _PYTHON_TERMINATING_OPTIONS:
                return None
            if token in {"-c", "-m", "-"}:
                return None
            if token in _PYTHON_OPTIONS_WITH_ARGUMENT:
                index += 2
                continue
            if token.startswith("--check-hash-based-pycs="):
                index += 1
                continue
            if token.startswith("-") and not token.startswith("--"):
                consumes_next = False
                for position, option in enumerate(token[1:]):
                    if option in {"?", "V", "c", "h", "m"}:
                        return None
                    if option in {"W", "X"}:
                        consumes_next = position == len(token[1:]) - 1
                        break
                index += 2 if consumes_next else 1
                continue
            if token.startswith("-"):
                index += 1
                continue
            candidate = token
            break
        if candidate is None:
            return None
    else:
        return None

    if candidate == "-":
        return None

    entrypoint = PurePosixPath(candidate)
    if entrypoint.is_absolute():
        return None
    raw_parts = candidate.split("/")
    if raw_parts[-1] in {"", "."}:
        raise PayloadValidationError(
            "payload validation failed: Python entrypoint "
            f"{candidate!r} must be a relative path naming a regular file "
            "inside the staged payload"
        )
    parts = tuple(part for part in entrypoint.parts if part not in {"", "."})
    if not parts or ".." in parts:
        raise PayloadValidationError(
            "payload validation failed: Python entrypoint "
            f"{candidate!r} must be a relative path inside the staged payload"
        )
    for env_working_directory in reversed(env_working_directories):
        if not env_working_directory:
            return None
        env_cwd = PurePosixPath(env_working_directory)
        if env_cwd.is_absolute():
            raise PayloadValidationError(
                "payload validation failed: /usr/bin/env working directory "
                f"{env_working_directory!r} must stay inside the staged payload"
            )
        cwd_parts = tuple(
            part for part in env_cwd.parts if part not in {"", "."}
        )
        if ".." in cwd_parts:
            raise PayloadValidationError(
                "payload validation failed: /usr/bin/env working directory "
                f"{env_working_directory!r} must stay inside the staged payload"
            )
        parts = (*cwd_parts, *parts)
    return PurePosixPath(*parts)


def _regular_input_source(input_file: str) -> Path:
    """Return one no-follow regular input path before staging mutates state."""
    source = Path(input_file)
    try:
        mode = source.lstat().st_mode
    except (FileNotFoundError, NotADirectoryError):
        mode = 0
    if not stat.S_ISREG(mode):
        raise FileNotFoundError(
            "input file not found or not a regular file: "
            f"{source.absolute()}"
        )
    return source.resolve()


def _directory_entrypoint_is_regular_file(
    source_directory: Path,
    entrypoint: PurePosixPath,
) -> bool:
    """Check one entrypoint without following any payload symlink."""
    candidate = source_directory
    traversed: list[str] = []
    for index, part in enumerate(entrypoint.parts):
        traversed.append(part)
        try:
            with os.scandir(candidate) as entries:
                exact_entry = next(
                    (entry for entry in entries if entry.name == part),
                    None,
                )
        except (FileNotFoundError, NotADirectoryError):
            return False
        if exact_entry is None:
            return False
        candidate = Path(exact_entry.path)
        mode = exact_entry.stat(follow_symlinks=False).st_mode
        if stat.S_ISLNK(mode):
            raise PayloadValidationError(
                "payload validation failed: Python entrypoint "
                f"{entrypoint.as_posix()!r} traverses symlink "
                f"{PurePosixPath(*traversed).as_posix()!r} in staged "
                f"directory payload {source_directory}"
            )
        if index < len(entrypoint.parts) - 1:
            if not stat.S_ISDIR(mode):
                return False
        elif not stat.S_ISREG(mode):
            raise PayloadValidationError(
                "payload validation failed: Python entrypoint "
                f"{entrypoint.as_posix()!r} must be a regular file in staged "
                f"directory payload {source_directory}"
            )
    return True


def _normalized_archive_parts(
    name: str,
    *,
    source_archive: Path,
    member_name: str,
) -> tuple[str, ...]:
    """Resolve lexical archive components without allowing root escape."""
    normalized: list[str] = []
    for part in name.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not normalized:
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member_name!r} escapes staged archive payload "
                    f"{source_archive}"
                )
            normalized.pop()
            continue
        normalized.append(part)
    return tuple(normalized)


def _portable_archive_parts(parts: tuple[str, ...]) -> tuple[str, ...]:
    """Canonicalize path components for portable extraction collision checks."""
    return tuple(
        unicodedata.normalize(
            "NFC",
            unicodedata.normalize("NFC", part).casefold(),
        )
        for part in parts
    )


def _record_portable_archive_spelling(
    spellings: dict[tuple[str, ...], tuple[tuple[str, ...], str]],
    *,
    parts: tuple[str, ...],
    member_name: str,
    source: Path,
    generated: bool = False,
) -> None:
    """Reject case/Unicode-equivalent explicit or implicit archive paths."""
    for length in range(1, len(parts) + 1):
        prefix = parts[:length]
        key = _portable_archive_parts(prefix)
        previous = spellings.get(key)
        if previous is not None and previous[0] != prefix:
            previous_parts, previous_member = previous
            previous_display = PurePosixPath(*previous_parts).as_posix()
            display = PurePosixPath(*prefix).as_posix()
            member_kind = "generated archive members" if generated else "archive members"
            raise PayloadValidationError(
                f"payload validation failed: {member_kind} "
                f"{previous_member!r} and {member_name!r} use paths "
                f"{previous_display!r} and {display!r} that collide under "
                "case/Unicode normalization in staged payload "
                f"{source}"
            )
        if previous is None:
            spellings[key] = (prefix, member_name)


def _reject_parent_traversing_archive_link(
    member: tarfile.TarInfo,
    *,
    source: Path,
    generated: bool = False,
) -> None:
    """Reject order-dependent link resolution before staging can mutate.

    ``tarfile.data_filter`` resolves a relative link against the filesystem as
    it exists at that moment.  Validating every member against one empty root
    is therefore not extraction-equivalent when an earlier archive link
    changes a later link's realpath.  Parent components in a link target are
    never needed by vq payloads and are refused conservatively for both user
    archives and directory metadata generated by ``tarfile.add``.
    """
    if not (member.issym() or member.islnk()):
        return
    if member.islnk():
        raise PayloadValidationError(
            "payload validation failed: archive member "
            f"{member.name!r} is a hardlink; hardlinks are not supported in "
            f"staged payload {source} because extraction depends on archive "
            "member order and target identity"
        )
    if not member.linkname:
        raise PayloadValidationError(
            "payload validation failed: archive member "
            f"{member.name!r} has an empty link target in staged payload "
            f"{source}"
        )
    if ".." not in member.linkname.split("/"):
        return
    kind = "generated archive member" if generated else "archive member"
    raise PayloadValidationError(
        "payload validation failed: "
        f"{kind} {member.name!r} has a link target containing parent "
        f"traversal in staged payload {source}"
    )


def _validated_archive_members(
    source_archive: Path,
) -> dict[tuple[str, ...], tarfile.TarInfo]:
    """Validate every member before the later ``data`` extraction."""
    validation_root = (
        source_archive.parent
        / f".vq-archive-validation-{uuid.uuid4().hex}"
    )
    members: dict[tuple[str, ...], tarfile.TarInfo] = {}
    portable_spellings: dict[
        tuple[str, ...], tuple[tuple[str, ...], str]
    ] = {}
    traversal_members: list[tarfile.TarInfo] = []
    with tarfile.open(source_archive) as tf:
        for member in tf.getmembers():
            try:
                filtered = tarfile.data_filter(member, str(validation_root))
            except (OSError, UnicodeError, tarfile.FilterError) as exc:
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} is unsafe in staged archive payload "
                    f"{source_archive}: {exc}"
                ) from exc
            if filtered is None:
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} was excluded by the data filter in "
                    f"staged archive payload {source_archive}"
                )
            member_parts = _normalized_archive_parts(
                filtered.name,
                source_archive=source_archive,
                member_name=member.name,
            )
            raw_member_parts = filtered.name.split("/")
            if not filtered.isdir() and (
                raw_member_parts[-1] == ""
                or next(
                    (
                        part
                        for part in reversed(raw_member_parts)
                        if part != ""
                    ),
                    "",
                )
                == "."
            ):
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} has a non-file terminal path "
                    f"component in staged archive payload {source_archive}"
                )
            if ".." in filtered.name.split("/"):
                traversal_members.append(filtered)
            if not member_parts and not filtered.isdir():
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} would replace the staged archive root "
                    f"for payload {source_archive}"
                )
            previous = members.get(member_parts)
            if previous is not None:
                display = PurePosixPath(*member_parts).as_posix() or "."
                raise PayloadValidationError(
                    "payload validation failed: archive path "
                    f"{display!r} is ambiguous: 2 members in staged archive "
                    f"payload {source_archive}, {previous.name!r} and "
                    f"{member.name!r}, normalize to {display!r}"
                )
            _record_portable_archive_spelling(
                portable_spellings,
                parts=member_parts,
                member_name=member.name,
                source=source_archive,
            )
            # The empty-linkname rejection must run on the raw member:
            # tarfile.data_filter rewrites "" to ".", which would make
            # the check inside _reject_parent_traversing_archive_link
            # unreachable for the filtered member. Regular-file members
            # legitimately carry an empty linkname, so gate on link types.
            if (member.issym() or member.islnk()) and not member.linkname:
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} has an empty link target in staged "
                    f"payload {source_archive}"
                )
            _reject_parent_traversing_archive_link(
                filtered,
                source=source_archive,
            )
            members[member_parts] = filtered

    if traversal_members:
        # A lexically in-root ``..`` is not necessarily in-root during the
        # real extraction: an earlier symlink changes what its parent means.
        # Normalized duplicate checks above still get the more precise error;
        # every other parent-traversing member is refused before extraction.
        member = traversal_members[0]
        raise PayloadValidationError(
            "payload validation failed: archive member "
            f"{member.name!r} contains parent traversal whose extraction "
            "can depend on earlier archive links in staged archive payload "
            f"{source_archive}"
        )

    for member_parts, member in members.items():
        for length in range(1, len(member_parts)):
            ancestor_parts = member_parts[:length]
            ancestor = members.get(ancestor_parts)
            if ancestor is not None and not ancestor.isdir():
                display = PurePosixPath(*member_parts).as_posix()
                ancestor_display = PurePosixPath(*ancestor_parts).as_posix()
                raise PayloadValidationError(
                    "payload validation failed: archive member "
                    f"{member.name!r} at {display!r} traverses non-directory "
                    f"archive ancestor {ancestor_display!r} in staged payload "
                    f"{source_archive}"
                )
    return members


def _validate_directory_archive_members(source_directory: Path) -> None:
    """Validate metadata that ``tarfile.add`` will emit for a directory."""
    validation_root = (
        source_directory.parent
        / f".vq-directory-validation-{uuid.uuid4().hex}"
    )
    portable_spellings: dict[
        tuple[str, ...], tuple[tuple[str, ...], str]
    ] = {}

    def validate_path(
        metadata_tar: tarfile.TarFile,
        path: Path,
        archive_name: str,
    ) -> None:
        member = metadata_tar.gettarinfo(str(path), archive_name)
        if member is None:
            # ``tarfile.add`` skips unsupported filesystem objects too.
            return
        try:
            filtered = tarfile.data_filter(member, str(validation_root))
        except (OSError, UnicodeError, tarfile.FilterError) as exc:
            raise PayloadValidationError(
                "payload validation failed: generated archive member "
                f"{member.name!r} is unsafe in staged directory payload "
                f"{source_directory}: {exc}"
            ) from exc
        if filtered is None:
            raise PayloadValidationError(
                "payload validation failed: generated archive member "
                f"{member.name!r} was excluded by the data filter in staged "
                f"directory payload {source_directory}"
            )
        _reject_parent_traversing_archive_link(
            filtered,
            source=source_directory,
            generated=True,
        )
        member_parts = tuple(
            part for part in filtered.name.split("/") if part not in {"", "."}
        )
        _record_portable_archive_spelling(
            portable_spellings,
            parts=member_parts,
            member_name=member.name,
            source=source_directory,
            generated=True,
        )
        if member.isdir():
            for child_name in sorted(os.listdir(path)):
                validate_path(
                    metadata_tar,
                    path / child_name,
                    f"{archive_name}/{child_name}",
                )

    with tarfile.open(fileobj=io.BytesIO(), mode="w") as metadata_tar:
        for child_name in sorted(os.listdir(source_directory)):
            validate_path(
                metadata_tar,
                source_directory / child_name,
                child_name,
            )


def _archive_entrypoint_is_regular_file(
    source_archive: Path,
    entrypoint: PurePosixPath,
    members: dict[tuple[str, ...], tarfile.TarInfo],
) -> bool:
    """Require one unambiguous regular entrypoint and safe ancestors."""
    entrypoint_parts = tuple(entrypoint.parts)
    for length in range(1, len(entrypoint_parts) + 1):
        prefix = entrypoint_parts[:length]
        member = members.get(prefix)
        display = PurePosixPath(*prefix).as_posix()
        if member is None:
            if length == len(entrypoint_parts):
                return False
            continue
        if length < len(entrypoint_parts):
            if not member.isdir():
                raise PayloadValidationError(
                    "payload validation failed: Python entrypoint "
                    f"{entrypoint.as_posix()!r} has non-directory archive "
                    f"ancestor {display!r} in staged payload {source_archive}"
                )
        elif not member.isfile():
            raise PayloadValidationError(
                "payload validation failed: Python entrypoint "
                f"{entrypoint.as_posix()!r} must be a regular file in staged "
                f"archive payload {source_archive}"
            )
    return True


def validate_staged_python_entrypoint(
    *,
    command: Sequence[str],
    source_directory: Path | None = None,
    source_archive: Path | None = None,
    interpreter_explicit: bool = False,
) -> None:
    """Fail before mutation when a Python file-mode command is unstaged."""
    _validate_command_head(command)
    if (source_directory is None) == (source_archive is None):
        raise ValueError(
            "exactly one staged payload source is required for entrypoint validation"
        )
    archive_members = (
        _validated_archive_members(source_archive)
        if source_archive is not None
        else None
    )
    entrypoint = _staged_python_entrypoint(
        command,
        interpreter_explicit=interpreter_explicit,
    )

    if source_directory is not None:
        present = (
            _directory_entrypoint_is_regular_file(
                source_directory,
                entrypoint,
            )
            if entrypoint is not None
            else True
        )
        # Keep every generated tar member safe as part of the same pure
        # preflight. This also protects direct dispatcher callers and legacy
        # specs before their first tempfile/upload, not just current CLI
        # submissions.
        _validate_directory_archive_members(source_directory)
        source_kind = "directory"
        source = source_directory
    else:
        assert source_archive is not None
        assert archive_members is not None
        present = (
            _archive_entrypoint_is_regular_file(
                source_archive,
                entrypoint,
                archive_members,
            )
            if entrypoint is not None
            else True
        )
        source_kind = "archive"
        source = source_archive

    if entrypoint is None:
        return
    if not present:
        raise PayloadValidationError(
            "payload validation failed: Python entrypoint "
            f"{entrypoint.as_posix()!r} is not present in staged "
            f"{source_kind} payload {source}; add the file or correct the command"
        )


def _scheduler_program_has_managed_runtime(
    cfg: config.Config,
    scheduler_host: str,
    program: str | None,
) -> bool:
    """Whether the target or a same-cluster alias manages this program."""
    if program is None:
        return False
    try:
        target_cfg = cfg.host(scheduler_host)
    except config.ConfigError:
        return False
    if program in target_cfg.scheduler_runtime_deployments:
        return True
    return any(
        candidate.ssh == target_cfg.ssh
        and program in candidate.scheduler_runtime_deployments
        for candidate in cfg.hosts.values()
    )


def _scheduler_python_payload_needs_runtime(
    cfg: config.Config,
    scheduler_host: str,
    request: _NormalizedSubmitRequest,
) -> bool:
    """Whether a named scheduler program must supply the Python launcher."""
    if request.program is None or request.is_qvf_input:
        return False
    if request.input_file is not None:
        # The non-QVF single-file submit form is a Python script by contract.
        python_payload = True
    elif request.python is not None:
        python_payload = True
    elif request.command:
        python_payload = _is_python_launcher(
            request.command[0]
        ) or request.command[0].lower().endswith(".py")
    else:
        python_payload = False
    if not python_payload:
        return False
    if request.expected_sha is not None:
        return True

    # Unpinned Python routing applies only to managed scheduler runtimes.
    # An arbitrary named binary such as ORCA may legitimately consume a
    # Python-generated payload through its own scheduler hook and has no
    # managed git runtime to resolve. Scheduler aliases share the deployment
    # contract of the canonical host behind the same SSH endpoint.
    return _scheduler_program_has_managed_runtime(
        cfg,
        scheduler_host,
        request.program,
    )


def _normalize_scheduler_python_payload(
    cfg: config.Config,
    *,
    scheduler_host: str,
    request: _NormalizedSubmitRequest,
    runtime_pin: ProgramRuntimePin | None,
) -> _NormalizedSubmitRequest:
    """Route a scheduler Python payload through its verified target runtime.

    This runs while submit planning is still side-effect free. Directory and
    archive payloads are reduced to one explicit argv so the normalized
    launcher crosses a remote-driver hop unchanged; single-file payloads keep
    using the existing ``python`` field consumed by both submit backends.
    """
    if request.program is None:
        return request
    if (
        runtime_pin is None
        or not runtime_pin.expected_git_sha
        or not runtime_pin.resolved_executable
    ):
        raise ValueError(
            f"--program {request.program!r} on scheduler target "
            f"{scheduler_host!r} requires a verified target "
            "ProgramRuntimePin.resolved_executable for this Python payload; "
            "repair or verify the managed runtime before submitting"
        )
    launcher = runtime_pin.resolved_executable

    if request.input_file is not None:
        normalized = replace(request, python=launcher)
        effective_command = [launcher, Path(request.input_file).name]
    else:
        assert request.command
        effective_command = _payload_command(request.command, request.python)
        if request.python is not None:
            effective_command[0] = launcher
            # Collapse a redundant explicit ``--python ... -- python ...``
            # shape rather than handing the managed wrapper another Python
            # executable as its script argument.
            if len(effective_command) > 1 and _is_python_launcher(
                effective_command[1]
            ):
                del effective_command[1]
        elif _is_python_launcher(effective_command[0]):
            effective_command[0] = launcher
        else:
            effective_command.insert(0, launcher)
        normalized = replace(
            request,
            command=effective_command,
            python=None,
        )

    try:
        target_cfg = cfg.host(scheduler_host)
    except config.ConfigError as exc:
        raise ValueError(
            f"cannot inspect scheduler program hooks for {scheduler_host!r}: {exc}"
        ) from exc
    hooks = target_cfg.scheduler_program_hooks.get(request.program)
    command_wrapper = hooks.command_wrapper if hooks is not None else []
    if command_wrapper:
        # Keep this decision identical to the scheduler renderer. A different
        # legacy wrapper would otherwise be prepended later and recreate
        # ``managed-python python input.py`` under a new spelling.
        from vq.scheduler_dispatch import wrapper_already_applied

        if not wrapper_already_applied(command_wrapper, effective_command):
            raise ValueError(
                f"--program {request.program!r} on scheduler target "
                f"{scheduler_host!r} resolves to managed launcher "
                f"{launcher!r}, but [hosts.{scheduler_host}."
                f"scheduler_program_hooks.{request.program}].command_wrapper "
                f"would also prepend {command_wrapper!r}. Refusing an "
                "ambiguous double launcher; remove the legacy command_wrapper "
                "or make it name the verified managed launcher."
            )
    return normalized


def _validate_scheduler_effective_staged_entrypoint(
    cfg: config.Config,
    *,
    scheduler_host: str,
    request: _NormalizedSubmitRequest,
) -> None:
    """Validate a scheduler hook's effective argv while planning is pure."""
    if (
        request.program is None
        or (request.directory is None and request.archive is None)
        or not request.command
    ):
        return
    try:
        target_cfg = cfg.host(scheduler_host)
    except config.ConfigError as exc:
        raise ValueError(
            f"cannot inspect scheduler program hooks for {scheduler_host!r}: {exc}"
        ) from exc
    hooks = target_cfg.scheduler_program_hooks.get(request.program)
    command_wrapper = hooks.command_wrapper if hooks is not None else []
    if not command_wrapper:
        return

    # Keep composition byte-for-byte aligned with the scheduler renderer, but
    # do not bake the target-side wrapper into the durable client/driver spec.
    from vq.scheduler_dispatch import wrapper_already_applied

    payload_command = _payload_command(request.command, request.python)
    if wrapper_already_applied(command_wrapper, payload_command):
        return
    effective_command = [*command_wrapper, *payload_command]
    if request.directory is not None:
        validate_staged_python_entrypoint(
            command=effective_command,
            source_directory=Path(request.directory).resolve(),
        )
    else:
        assert request.archive is not None
        validate_staged_python_entrypoint(
            command=effective_command,
            source_archive=Path(request.archive).resolve(),
        )


def _resolve_submit_plan(
    cfg: config.Config,
    *,
    target: _ClassifiedSubmitTarget,
    request: _NormalizedSubmitRequest,
) -> _ResolvedSubmitPlan:
    """Resolve runtime identities and execution placement in authority order."""
    if target.scheduler_target is not None:
        try:
            scheduler_cfg = cfg.host(target.scheduler_target)
        except config.ConfigError as exc:
            raise ValueError(
                f"scheduler target {target.scheduler_target!r} is not configured: "
                f"{exc}"
            ) from None
        lane = scheduler_cfg.scheduler_lane_metadata()
        partition = lane.get("partition") if lane is not None else None
        enforce_scheduler_wall_time_limit(
            request.wall_time_seconds,
            scheduler_cfg.scheduler_max_wall_time_seconds,
            scheduler_host=target.scheduler_target,
            partition=partition if isinstance(partition, str) else None,
        )
    receipt_runtime_pin: ProgramRuntimePin | None = None
    scheduler_runtime_pin: ProgramRuntimePin | None = None
    program_runtime_pin: ProgramRuntimePin | None = None
    directly_requested_local = target.requested_host_config is None
    managed_python_payload = (
        target.scheduler_target is not None
        and _scheduler_python_payload_needs_runtime(
            cfg,
            target.scheduler_target,
            request,
        )
    )
    managed_scheduler_program = (
        target.scheduler_target is not None
        and _scheduler_program_has_managed_runtime(
            cfg,
            target.scheduler_target,
            request.program,
        )
    )
    needs_scheduler_runtime = (
        managed_scheduler_program
        or request.is_qvf_input
        or request.expected_sha is not None
        or managed_python_payload
    )
    # Runtime identity comes only from the executable slot. Payload arguments
    # may legitimately contain wrapper-looking paths as data, and scanning
    # them would let an argument override (or contradict) what actually
    # launches. An explicit ``--python`` is the outer executable and therefore
    # authoritative; otherwise argv[0] is. Deliberately do not interpret an
    # ``/usr/bin/env python ...`` command here: env-wrapper expansion is a
    # separate command-grammar milestone.
    if request.python is not None:
        command_candidates = [request.python]
    elif request.command:
        command_candidates = [request.command[0]]
    else:
        command_candidates = []

    # A direct local request owns registry validation before any hidden
    # scheduler target is consulted.
    if directly_requested_local:
        _validate_program_for_submit(
            cfg,
            request.program,
            local_spec=True,
        )
        if target.scheduler_target is not None:
            scheduler_runtime_pin = (
                _validate_scheduler_target_expected_sha(
                    cfg,
                    target.scheduler_target,
                    request.program,
                    request.expected_sha,
                    command_candidates=command_candidates,
                )
                if needs_scheduler_runtime
                else None
            )
            program_runtime_pin = (
                scheduler_runtime_pin
                or _program_runtime_pin_for_submit(
                    cfg,
                    request.program,
                    expected_sha=None,
                )
            )
            # Keep the existing value even when it is only an observational
            # local fallback. Receipt rendering filters non-scheduler pins.
            receipt_runtime_pin = program_runtime_pin
        else:
            local_expected_sha = _validate_expected_sha_for_submit(
                cfg,
                request.program,
                request.expected_sha,
                local_spec=True,
            )
            program_runtime_pin = _program_runtime_pin_for_submit(
                cfg,
                request.program,
                expected_sha=local_expected_sha,
            )
    elif (
        target.requested_host_config is not None
        and target.requested_host_config.scheduler != "local"
    ):
        # A named scheduler target is authoritative before its driver config
        # is loaded or any local driver registry is consulted.
        if needs_scheduler_runtime:
            scheduler_runtime_pin = (
                _validate_scheduler_target_expected_sha(
                    cfg,
                    target.requested_host,
                    request.program,
                    request.expected_sha,
                    command_candidates=command_candidates,
                )
            )
        receipt_runtime_pin = scheduler_runtime_pin

    if managed_python_payload:
        assert target.scheduler_target is not None
        request = _normalize_scheduler_python_payload(
            cfg,
            scheduler_host=target.scheduler_target,
            request=request,
            runtime_pin=scheduler_runtime_pin,
        )

    if target.scheduler_target is not None:
        _validate_scheduler_effective_staged_entrypoint(
            cfg,
            scheduler_host=target.scheduler_target,
            request=request,
        )

    execution_target = _resolve_submit_execution_target(cfg, target)
    if is_local_host(execution_target.execution_host) and not directly_requested_local:
        # A local scheduler driver contributes only an observational fallback
        # when the target did not require an authoritative runtime lookup.
        _validate_program_for_submit(
            cfg,
            request.program,
            local_spec=True,
        )
        if execution_target.scheduler_target is not None:
            program_runtime_pin = (
                scheduler_runtime_pin
                or _program_runtime_pin_for_submit(
                    cfg,
                    request.program,
                    expected_sha=None,
                )
            )
        else:
            local_expected_sha = _validate_expected_sha_for_submit(
                cfg,
                request.program,
                request.expected_sha,
                local_spec=True,
            )
            program_runtime_pin = _program_runtime_pin_for_submit(
                cfg,
                request.program,
                expected_sha=local_expected_sha,
            )

    return _ResolvedSubmitPlan(
        target=execution_target,
        request=request,
        program_runtime_pin=program_runtime_pin,
        receipt_runtime_pin=receipt_runtime_pin,
    )


def _execute_submit_plan(
    plan: _ResolvedSubmitPlan,
    *,
    multi_user: bool,
    warning_sink: Callable[[str], None] | None = None,
) -> list[str]:
    """Execute one resolved plan without CLI rendering or error translation."""
    request = plan.request
    target = plan.target
    if is_local_host(target.execution_host):
        common: dict[str, object] = {
            "host": target.execution_host,
            "input_file": request.input_file,
            "directory": request.directory,
            "archive": request.archive,
            "command": request.command,
            "python": request.python,
            "cpus": request.cpus,
            "scheduler_tasks": request.scheduler_tasks,
            "mem_mb": request.mem_mb,
            "wall_time_seconds": request.wall_time_seconds,
            "priority": request.priority,
            "auto_resume": request.auto_resume,
            "retry": request.retry,
            "job_name": request.job_name,
            "branch": request.branch,
            "program": request.program,
            "program_runtime_pin": plan.program_runtime_pin,
            "tags": request.tags,
            "not_before": request.not_before,
            "depends_on": request.depends_on,
            "depends_on_any": request.depends_on_any,
            "rerun_until_file_exists": request.rerun_until_file_exists,
            "rerun_max": request.rerun_max,
            "clean_workdir_on_terminal": request.clean_workdir_on_terminal,
            "vibeqc_preflight": request.vibeqc_preflight,
            "multi_user": multi_user,
            "scheduler_target": target.scheduler_target,
        }
        if request.idempotency_key is not None:
            common["idempotency_key"] = request.idempotency_key
        if warning_sink is not None:
            common["warning_sink"] = warning_sink
        if request.chain > 1:
            return submit_local_chain(chain=request.chain, **common)
        if request.array > 1:
            return submit_local_array(array=request.array, **common)
        return [
            submit_local(
                **common,
                refresh_before=request.refresh_before,
                qvf_force=request.qvf_force,
            )
        ]

    host_cfg = target.execution_host_config
    assert host_cfg is not None
    forwarded_expected_sha = request.expected_sha
    if (
        target.scheduler_target is not None
        and request.program is not None
        and not request.is_qvf_input
        and plan.receipt_runtime_pin is not None
        and plan.receipt_runtime_pin.scheduler_host is not None
    ):
        # Carry the resolved target identity across the existing remote-driver
        # wire. Otherwise an unpinned submit can audit launcher A on the client,
        # then silently re-resolve launcher B when the driver receives it.
        forwarded_expected_sha = plan.receipt_runtime_pin.expected_git_sha
    return submit_remote(
        host=target.execution_host,
        host_cfg=host_cfg,
        input_file=request.input_file,
        directory=request.directory,
        archive=request.archive,
        command=request.command,
        python=request.python,
        cpus=request.cpus,
        scheduler_tasks=request.scheduler_tasks,
        mem_mb=request.mem_mb,
        wall_time_seconds=request.wall_time_seconds,
        priority=request.priority,
        auto_resume=request.auto_resume,
        retry=request.retry,
        job_name=request.job_name,
        branch=request.branch,
        program=request.program,
        expected_sha=forwarded_expected_sha,
        tags=request.tags,
        not_before=request.not_before,
        depends_on=request.depends_on,
        depends_on_any=request.depends_on_any,
        clean_workdir_on_terminal=request.clean_workdir_on_terminal,
        array=request.array,
        chain=request.chain,
        rerun_until_file_exists=request.rerun_until_file_exists,
        rerun_max=request.rerun_max,
        refresh_before=request.refresh_before,
        scheduler_target=target.scheduler_target,
        qvf_force=request.qvf_force,
        warning_sink=warning_sink,
        **(
            {"idempotency_key": request.idempotency_key}
            if request.idempotency_key is not None
            else {}
        ),
    )


def _validate_program_for_submit(
    cfg: config.Config,
    program_name: str | None,
    *,
    local_spec: bool,
) -> None:
    """Validate ``--program`` at the boundary that owns the registry."""
    if program_name is None:
        return
    if not JOB_NAME_PATTERN.fullmatch(program_name):
        raise ValueError(
            f"--program {program_name!r} is invalid: must be alphanumerics "
            f"+ '-', '_', '.' only (1-{JOB_NAME_MAX_LEN} chars)."
        )
    if local_spec and program_name not in cfg.programs:
        known = ", ".join(sorted(cfg.programs)) or "(none registered)"
        raise ValueError(
            f"unknown --program {program_name!r}; known programs: {known}. "
            "Run `vq programs` to inspect the registry."
        )
    if local_spec:
        prog = cfg.programs[program_name]
        if isinstance(prog, VenvProgram):
            mismatches = prog.runtime_pin_mismatches()
            if mismatches:
                raise ValueError(
                    f"--program {program_name!r} runtime pin mismatch: "
                    + "; ".join(mismatches)
                )


def _git_sha_matches(actual: str, expected: str) -> bool:
    actual_norm = actual.strip().lower()
    expected_norm = expected.strip().lower()
    return (
        actual_norm == expected_norm
        or actual_norm.startswith(expected_norm)
        or expected_norm.startswith(actual_norm)
    )


EXPECTED_SHA_MIN_PREFIX_LEN = 7


def _validate_expected_sha_for_submit(
    cfg: config.Config,
    program_name: str | None,
    expected_sha: str | None,
    *,
    local_spec: bool,
) -> str | None:
    """Validate ``--expected-sha`` at the owning vq boundary.

    Returns the canonical host-local SHA snapshot when the checkout is local.
    For ordinary remote delegation, returns the stripped operator argument so
    the receiving vq can validate and canonicalize its own checkout.
    """
    if expected_sha is None:
        return None
    expected_sha = expected_sha.strip()
    if not expected_sha:
        raise ValueError("--expected-sha requires a non-empty git SHA")
    if not all(ch in "0123456789abcdefABCDEF" for ch in expected_sha):
        raise ValueError(
            f"--expected-sha {expected_sha!r} is invalid: expected a hex git SHA"
        )
    if len(expected_sha) < EXPECTED_SHA_MIN_PREFIX_LEN:
        raise ValueError(
            f"--expected-sha {expected_sha!r} is too short: use at least "
            f"{EXPECTED_SHA_MIN_PREFIX_LEN} hex characters"
        )
    if program_name is None:
        raise ValueError("--expected-sha requires --program NAME")
    if not local_spec:
        return expected_sha
    prog = cfg.programs.get(program_name)
    if not isinstance(prog, VenvProgram):
        raise ValueError(
            f"--expected-sha requires --program {program_name!r} to be a "
            "venv program with a git_dir"
        )
    actual_sha = prog.current_git_sha(full=len(expected_sha) == 40)
    if actual_sha is None:
        raise ValueError(
            f"--program {program_name!r} expected git SHA {expected_sha}, "
            "but current git SHA could not be read"
        )
    if not _git_sha_matches(actual_sha, expected_sha):
        raise ValueError(
            f"--program {program_name!r} expected git SHA {expected_sha}, "
            f"got {actual_sha}"
        )
    # Do not shorten a full operator-supplied identity after matching it.
    return expected_sha.lower() if len(expected_sha) == 40 else actual_sha


_WRAPPER_IDENTITY_RE = re.compile(
    r"-(?P<version>\d[\w.]*)-(?P<sha>[0-9a-f]{7,40})(?:-|$|\.)"
)


def _scheduler_registry_identity(
    cfg: config.Config,
    scheduler_host: str,
    program_name: str,
    expected_sha: str | None,
) -> ProgramRuntimePin | None:
    """Resolve runtime identity from the scheduler target's registry.

    An unreachable, missing, or unparseable registry entry is not itself an
    error: the caller falls back to the verified deployment record.
    """
    try:
        host_cfg = cfg.host(scheduler_host)
    except config.ConfigError:
        return None
    try:
        proc = transport.run_remote_vq(
            host_cfg,
            "programs",
            "--json",
            check=False,
            timeout=transport.DEFAULT_REMOTE_VQ_TIMEOUT_SECONDS,
        )
    except transport.RemoteError:
        return None
    if proc.returncode != 0:
        return None
    try:
        import json as _json

        entries = _json.loads(proc.stdout or "[]")
    except ValueError:
        return None
    if not isinstance(entries, list):
        return None
    entry = next(
        (
            item
            for item in entries
            if isinstance(item, dict) and item.get("name") == program_name
        ),
        None,
    )
    if entry is None:
        return None
    executable = str(entry.get("binary") or entry.get("python") or "")
    match = (
        _WRAPPER_IDENTITY_RE.search(Path(executable).name)
        if executable
        else None
    )
    reported_sha = str(
        entry.get("current_git_sha_full")
        or entry.get("current_git_sha")
        or (match.group("sha") if match is not None else "")
    )
    if not reported_sha:
        return None
    version = str(
        entry.get("import_version")
        or (match.group("version") if match is not None else "")
    )
    if expected_sha is not None and not _git_sha_matches(
        reported_sha, expected_sha
    ):
        raise ValueError(
            f"--program {program_name!r} on scheduler target "
            f"{scheduler_host!r} expected git SHA {expected_sha}, but the "
            f"registered runtime is {executable} (version {version}, SHA "
            f"{reported_sha}) — that wrapper is what a submitted job "
            "executes. The driver-local program of the same name is not "
            "consulted for scheduler targets."
        )
    return ProgramRuntimePin(
        expected_git_sha=(
            expected_sha.lower() if expected_sha is not None else reported_sha
        ),
        scheduler_host=scheduler_host,
        resolved_executable=executable,
        program_kind=str(entry.get("kind") or "binary"),
        program_version=version,
        artifact_identity=executable,
    )


def _command_wrapper_identity(
    program_name: str,
    expected_sha: str,
    command_candidates: Sequence[str],
    scheduler_host: str,
) -> ProgramRuntimePin | None:
    """Resolve an immutable runtime wrapper named in the submit command."""
    for token in command_candidates:
        base = Path(token).name
        if not base.startswith(f"{program_name}-"):
            continue
        match = _WRAPPER_IDENTITY_RE.search(base)
        if match is None:
            continue
        wrapper_sha = match.group("sha")
        version = match.group("version")
        if not _git_sha_matches(wrapper_sha, expected_sha):
            raise ValueError(
                f"--program {program_name!r} on scheduler target "
                f"{scheduler_host!r} expected git SHA {expected_sha}, but "
                f"the submit command executes {token} (version {version}, "
                f"SHA {wrapper_sha}). The pin must describe the wrapper in "
                "the command."
            )
        return ProgramRuntimePin(
            expected_git_sha=expected_sha.lower(),
            scheduler_host=scheduler_host,
            resolved_executable=token,
            program_kind="binary",
            program_version=version,
            artifact_identity=token,
        )
    return None


def _validate_scheduler_target_expected_sha(
    cfg: config.Config,
    scheduler_host: str,
    program_name: str | None,
    expected_sha: str | None,
    *,
    command_candidates: Sequence[str] | None = None,
) -> ProgramRuntimePin | None:
    """Resolve the scheduler target's authoritative runtime identity.

    Scheduler placement later rewrites execution to a driver. Identity must
    remain anchored to what the target job executes, never a same-named
    driver-local venv. Authority order is an immutable command wrapper, the
    target registry, then the target's verified deployment record.
    """
    if program_name is None:
        if expected_sha is None:
            return None
        raise ValueError("--expected-sha requires --program NAME")
    if expected_sha is not None:
        expected_sha = expected_sha.strip()
    # A wrapper named in argv is the exact executable and outranks a registry
    # that may intentionally have rolled forward.
    if command_candidates and expected_sha is not None:
        command_pin = _command_wrapper_identity(
            program_name,
            expected_sha,
            command_candidates,
            scheduler_host,
        )
        if command_pin is not None:
            return command_pin
    registry_pin = _scheduler_registry_identity(
        cfg,
        scheduler_host,
        program_name,
        expected_sha,
    )
    if registry_pin is not None:
        return registry_pin

    # Deployment records are the fail-closed fallback when target registry
    # metadata cannot identify the executable.
    # Avoid loading the large admin service graph for ordinary submit imports.
    from vq import admin as admin_module

    records = admin_module.load_scheduler_runtime_status()
    record = records.get(f"{scheduler_host}:{program_name}")
    if record is None:
        try:
            target_ssh = cfg.host(scheduler_host).ssh
        except config.ConfigError:
            target_ssh = None
        if target_ssh is not None:
            for _key, candidate in sorted(records.items()):
                if candidate.program != program_name:
                    continue
                try:
                    if cfg.host(candidate.host).ssh == target_ssh:
                        record = candidate
                        break
                except config.ConfigError:
                    continue
    if record is None:
        raise ValueError(
            f"--expected-sha on scheduler target {scheduler_host!r}: no "
            f"verified runtime deployment record for program "
            f"{program_name!r} on that cluster. Managed identity comes from "
            "`vq admin status <host>`; deploy (or verify) the runtime "
            "first. Unmanaged hook programs cannot be SHA-pinned."
        )
    if record.last_success:
        actual_sha = record.actual_sha
        actual_tag = record.actual_tag
        active_path = record.active_path
    else:
        actual_sha = record.last_ok_sha
        actual_tag = record.last_ok_tag
        active_path = record.last_ok_active_path
    if not actual_sha:
        raise ValueError(
            f"--expected-sha on scheduler target {scheduler_host!r}: program "
            f"{program_name!r} has no verified runtime (LAST OK=false and no "
            "LAST GOOD identity). Repair the deployment before pinned "
            "submits; see `vq admin status " + scheduler_host + "`."
        )
    if expected_sha is not None and not _git_sha_matches(
        actual_sha, expected_sha
    ):
        raise ValueError(
            f"--program {program_name!r} on scheduler target "
            f"{scheduler_host!r} expected git SHA {expected_sha}, got "
            f"{actual_sha} (version {actual_tag or '?'}, active path "
            f"{active_path or '?'}). The driver-local program of the same "
            "name is not consulted for scheduler targets."
        )
    version = None
    if actual_tag:
        version = actual_tag[1:] if actual_tag.startswith("v") else actual_tag
    return ProgramRuntimePin(
        expected_git_sha=actual_sha,
        scheduler_host=scheduler_host,
        resolved_executable=active_path,
        program_kind="scheduler-runtime",
        program_version=version,
        artifact_identity=active_path,
    )


def _program_runtime_pin_for_submit(
    cfg: config.Config,
    program_name: str | None,
    *,
    expected_sha: str | None = None,
) -> ProgramRuntimePin | None:
    """Snapshot configured venv runtime identity for a local spec."""
    if program_name is None:
        return None
    prog = cfg.programs.get(program_name)
    if not isinstance(prog, VenvProgram):
        return None
    # An explicit or configured SHA is enforced. A discovered SHA without
    # either requirement is observational and must survive runtime rollovers.
    expected_git_sha = expected_sha or prog.expected_git_sha
    enforce_git_sha = bool(expected_git_sha)
    if not expected_git_sha:
        expected_git_sha = prog.current_git_sha(full=True)
    if not expected_git_sha and not prog.expected_import_version:
        return None
    return ProgramRuntimePin(
        expected_git_sha=expected_git_sha,
        enforce_git_sha=enforce_git_sha,
        expected_import_version=prog.expected_import_version,
        import_check=prog.import_check if prog.expected_import_version else None,
        import_symbols=(
            list(prog.import_symbols)
            if prog.expected_import_version and prog.import_symbols
            else []
        ),
    )


def new_jobid() -> str:
    """Short opaque jobid (12 hex chars from a UUID4)."""
    return uuid.uuid4().hex[:12]


def _is_qvf_input(path: str | Path) -> bool:
    return Path(path).suffix.lower() == ".qvf"


def _qvf_command(
    *,
    artifact_name: str,
    program: str | None,
    program_runtime_pin: ProgramRuntimePin | None,
    scheduler_target: str | None,
    force: bool,
) -> list[str]:
    """Resolve a complete-calculation QVF to its managed vibe-qc command.

    Local jobs use the registered venv interpreter.  Scheduler jobs use the
    target's configured command wrapper, which names the immutable runtime on
    the compute host; including it in the stored command also makes scheduler
    wrapper injection idempotent.
    """
    if program is None:
        raise ValueError(
            "single-QVF submit requires --program NAME so vq can resolve "
            "the managed vibe-qc runtime"
        )
    if (
        program_runtime_pin is None
        or not program_runtime_pin.expected_git_sha
    ):
        raise ValueError(
            "single-QVF submit requires a healthy managed vibe-qc runtime "
            "with a readable git identity; vq snapshots that identity "
            "automatically"
        )
    if scheduler_target is not None:
        try:
            target_cfg = config.load_config().host(scheduler_target)
        except config.ConfigError as exc:
            raise ValueError(
                f"cannot resolve QVF runtime for scheduler target "
                f"{scheduler_target!r}: {exc}"
            ) from exc
        hooks = target_cfg.scheduler_program_hooks.get(program)
        if hooks is None or not hooks.command_wrapper:
            raise ValueError(
                f"single-QVF submit to scheduler host {scheduler_target!r} "
                f"requires [hosts.{scheduler_target}."
                f"scheduler_program_hooks.{program}].command_wrapper; "
                "vq will not guess a compute-node executable"
            )
        command = [
            *hooks.command_wrapper,
            "-m",
            "vibeqc._cli",
            "run",
            artifact_name,
        ]
    else:
        cfg = config.load_config()
        prog = cfg.programs.get(program)
        if not isinstance(prog, VenvProgram):
            known = ", ".join(sorted(cfg.programs)) or "(none registered)"
            raise ValueError(
                f"single-QVF submit requires --program {program!r} to be a "
                f"managed venv program; known programs: {known}"
            )
        command = [prog.python, "-m", "vibeqc._cli", "run", artifact_name]
    if force:
        command.append("--force")
    return command


def _payload_command(command: Sequence[str], python: str | None) -> list[str]:
    """The explicit command for a ``--dir`` / ``--compressed`` submit.

    These payloads carry no implied entry point, so ``command`` is required and
    is shipped as given. What changed on 2026-08-01 is that an interpreter may
    now be named alongside it: ``[python, *command]``.

    Both flags used to raise here, with "put the interpreter in the explicit
    command". That reads as a small ergonomic restriction and is not: on a
    scheduler host the interpreter is a per-host pinned wrapper path, so the
    only way to obey it was to hardcode a cluster path into every submit line
    -- exactly the leak ``[hosts.HOST.branches]`` exists to prevent. In
    practice sites reached instead for
    ``scheduler_program_hooks.NAME.command_wrapper``, the one remaining
    mechanism that could inject a launcher; when pbs-cluster's wrapper was removed on
    2026-07-21 its ``--dir`` submits silently lost their launcher and ran as
    bare ``run.py`` (exit 127), which is what ``_refuse_launcherless_script``
    was later added to catch.

    So this is not new capability so much as routing ``--dir`` through the
    mechanism single-file submits already use. Prepending is unambiguous:
    ``--python`` / ``--branch`` is the operator naming a launcher, and the only
    coherent place for a launcher is in front. A payload that already carries
    its own interpreter simply keeps not passing these flags, exactly as today.
    """
    result = list(command) if python is None else [python, *command]
    _validate_command_head(result)
    return result


def _validate_command_head(command: Sequence[str]) -> None:
    """Reject malformed argv without consulting the submitting host's PATH.

    Scheduler wrappers, module setup and remote runtimes may supply an
    executable that does not exist on the driver. Their arguments (including
    Python's -c/-m with an explicit interpreter) remain verbatim.
    """
    head = command[0] if command else ""
    if not head or head.startswith("-") or "\x00" in head:
        raise PayloadValidationError(
            f"invalid command executable {head!r}; put vq options before -- "
            "and the payload executable after it, e.g. "
            "--idempotency-key KEY -- bash run.sh"
        )


def _reject_driver_local_interpreter(
    scheduler_target: str | None, *, interpreter: str
) -> None:
    """Fail closed rather than bake a driver-local interpreter into a qsub job.

    §17 scheduler dispatch ships ``spec.command`` verbatim into the generated
    batch script: ``scheduler_dispatch.build_job_script`` only *prepends* an
    optional ``[hosts.HOST.scheduler_program_hooks.NAME] command_wrapper``, it
    never rewrites the argv. Every absolute path in the command must therefore
    be valid ON THE CLUSTER.

    A single-file submit defaults its interpreter to the driver's own
    (``sys.executable`` locally, ``remote_python`` for a remote driver). The
    driver is a different machine -- frequently a different OS -- than the
    scheduler host, so that path generally cannot exist there and the job can
    only die at run time with a ``FileNotFoundError`` naming a driver-local
    path. Rejecting the submit turns that into an immediate, actionable error.

    Scope, deliberately narrow:

    * Only the *defaulted* interpreter is rejected. An explicit ``--python``
      is the operator asserting the path is valid on the cluster, so callers
      pass this guard only when the operator supplied nothing.
    * Only *absolute* interpreters are rejected: a bare name (``python``) is a
      remote ``PATH`` lookup, not a driver-local path, and stays allowed.
    * Local and ordinary remote-daemon hosts (``scheduler_target is None``)
      are unaffected -- there the interpreter is resolved on the machine that
      actually runs the job.
    """
    if scheduler_target is None:
        return
    if not Path(interpreter).is_absolute():
        return
    raise ValueError(
        f"single-file submit to scheduler host {scheduler_target!r} would run "
        f"the driver's own interpreter {interpreter!r}, which is a path on the "
        f"driver and cannot be assumed to exist on {scheduler_target!r}. "
        "vq does not inject driver-local paths into scheduler jobs. Either "
        "pass --python with an interpreter path valid on "
        f"{scheduler_target!r}, or configure "
        f"[hosts.{scheduler_target}.scheduler_program_hooks.NAME] with a "
        "command_wrapper and submit via --dir/--compressed with an explicit "
        "cluster-side command."
    )


def new_array_group_id() -> str:
    """v0.6.52: short opaque array group id (8 hex chars from a UUID4).

    Shorter than jobid (8 vs 12) so a status display can comfortably
    show the group id alongside a per-element jobid; the namespace
    is "groups submitted from this user's session" which has
    nowhere near the collision-likelihood pressure jobids face.
    """
    return uuid.uuid4().hex[:8]


def _scheduler_width_warnings(
    *,
    cpus: int,
    jobid: str,
    scheduler_target: str,
) -> tuple[str, ...]:
    """Warn when a request is wider than its scheduler lane can ever run.

    vibe-qc#148: five ``ppn=128`` jobs were accepted onto a lane whose only
    nodes that wide were one busy and one offline, and queued for six days.
    The scheduler accepts such a request without complaint, and until now so
    did vq -- this path returned nothing at all for a scheduler target, so
    the one moment a human was watching passed in silence.

    Advisory and best effort in both directions: an unreadable config costs
    the warning and never the submission, and an exceeded limit warns rather
    than refuses, because a declared lane width goes stale as nodes return
    to service.
    """
    try:
        host_cfg = config.load_config().host(scheduler_target)
    except Exception:
        return ()
    lane = host_cfg.scheduler_lane_metadata()
    partition = lane.get("partition") if lane is not None else None
    message = scheduler_width_warning(
        cpus,
        host_cfg.scheduler_max_cpus,
        scheduler_host=scheduler_target,
        partition=partition if isinstance(partition, str) else None,
    )
    return () if message is None else (f"job {jobid}: {message}",)


def _impossible_capacity_warnings(
    *,
    cpus: int,
    mem_mb: int | None,
    jobid: str,
    multi_user: bool,
    scheduler_target: str | None,
) -> tuple[str, ...]:
    """Describe requests this host can never run as asked."""
    if scheduler_target is not None:
        return _scheduler_width_warnings(
            cpus=cpus,
            jobid=jobid,
            scheduler_target=scheduler_target,
        )
    try:
        caps = capacity.read_daemon_capacity(multi_user=multi_user)
    except Exception:
        return ()
    if caps is None:
        return ()

    warnings: list[str] = []
    for overage in capacity.configured_capacity_overages(
        cpus=cpus,
        mem_mb=mem_mb,
        snapshot=caps,
    ):
        if overage.resource == "cpus":
            warnings.append(
                f"requested {overage.requested} CPUs but this daemon caps at "
                f"--max-cpus {overage.limit}; job {jobid} will park PENDING "
                "until the daemon is restarted with a higher cap"
            )
        elif overage.uses_default:
            warnings.append(
                "undeclared memory is charged at daemon default "
                f"{overage.requested} MB but this daemon caps at --max-mem-mb "
                f"{overage.limit} MB; job {jobid} will park PENDING until "
                "the daemon is restarted with a higher cap"
            )
        else:
            warnings.append(
                f"requested {overage.requested} MB memory but this daemon "
                f"caps at --max-mem-mb {overage.limit} MB; job {jobid} will "
                "park PENDING until the daemon is restarted with a higher cap"
            )
    return tuple(warnings)


def _deliver_capacity_warnings(
    capacity_warnings: Sequence[str],
    warning_sink: Callable[[str], None] | None,
) -> None:
    """Deliver accepted-submit warnings without masking its receipt."""
    for message in capacity_warnings:
        _non_masking_warning("%s", message)
        if warning_sink is not None:
            try:
                warning_sink(message)
            except BaseException as exc:
                _non_masking_warning(
                    "submit warning consumer failed after acceptance receipt: "
                    "%s",
                    type(exc).__name__,
                )


def _deliver_idempotent_replay_capacity_warnings(
    *,
    queue_dir: Path,
    jobid: str,
    multi_user: bool,
    warning_sink: Callable[[str], None] | None,
) -> None:
    """Best-effort current-cap warning for an already accepted replay.

    The persisted spec is authoritative here: an old keyed job may have
    advanced beyond PENDING since its original acceptance, in which case a
    new "will park PENDING" warning would be false. Nothing in this advisory
    path may hide the durable replay receipt.
    """
    try:
        replayed = spec_access.read_bounded_regular_spec(
            queue_dir / f"{jobid}.json"
        )
        if replayed.id != jobid:
            raise ValueError(
                f"replayed spec id {replayed.id!r} does not match {jobid!r}"
            )
        if replayed.state != JobState.PENDING:
            return
        capacity_warnings = _impossible_capacity_warnings(
            cpus=replayed.cpus,
            mem_mb=replayed.mem_mb,
            jobid=replayed.id,
            multi_user=multi_user,
            scheduler_target=replayed.scheduler_target,
        )
    except FileNotFoundError:
        # A durable idempotency tombstone intentionally outlives ordinary
        # cleanup of its accepted spec. It still replays the original receipt,
        # but there is no live pending job left to classify.
        return
    except BaseException as exc:
        _non_masking_warning(
            "capacity warning probe failed after idempotent replay receipt: %s",
            type(exc).__name__,
        )
        return
    _deliver_capacity_warnings(capacity_warnings, warning_sink)


def _remote_impossible_capacity_warning(
    line: str,
    *,
    jobids: Sequence[str],
) -> str | None:
    """Return one recognized warning for a job accepted by this request."""
    if not line.startswith(_SUBMIT_WARNING_PREFIX):
        return None
    message = line.removeprefix(_SUBMIT_WARNING_PREFIX)
    for pattern in _IMPOSSIBLE_CAPACITY_WARNING_PATTERNS:
        match = pattern.fullmatch(message)
        if match is not None and match.group("jobid") in jobids:
            return message
    return None


def _validate_idempotency_key(key: str) -> None:
    if _IDEMPOTENCY_KEY_PATTERN.fullmatch(key) is None:
        raise ValueError(
            "idempotency key must be 1-128 characters, start with an "
            "alphanumeric character, and contain only alphanumerics plus "
            "'.', '_', ':', or '-'"
        )


def _digest_file(path: Path, digest: object) -> None:
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)  # type: ignore[attr-defined]


def _file_sha256(path: Path) -> bytes:
    digest = hashlib.sha256()
    _digest_file(path, digest)
    return digest.digest()


def _file_sha256_at(directory_fd: int, name: str) -> bytes:
    """Hash one stable no-follow regular child of a trusted directory."""
    descriptor = os.open(
        name,
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_CLOEXEC", 0),
        dir_fd=directory_fd,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise PayloadValidationError(
                "staged payload contains a non-regular file"
            )
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        return digest.digest()
    finally:
        os.close(descriptor)


def _digest_framed_bytes(digest: object, value: bytes) -> None:
    """Append one unambiguous binary field to a payload digest."""
    digest.update(len(value).to_bytes(8, "big"))  # type: ignore[attr-defined]
    digest.update(value)  # type: ignore[attr-defined]


def _copy_open_regular_file(source: Path, destination: Path) -> None:
    """Copy one no-follow regular inode into a private snapshot."""
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    descriptor = os.open(source, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise FileNotFoundError(
                f"payload source is not a real regular file: {source}"
            )
        output = os.open(
            destination,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            stat.S_IMODE(metadata.st_mode),
        )
        try:
            while chunk := os.read(descriptor, 1024 * 1024):
                view = memoryview(chunk)
                while view:
                    written = os.write(output, view)
                    view = view[written:]
            os.fchmod(output, stat.S_IMODE(metadata.st_mode))
            os.fsync(output)
        finally:
            os.close(output)
    finally:
        os.close(descriptor)


def _copy_open_regular_file_at(
    source: Path,
    destination_fd: int,
    destination_name: str,
) -> None:
    """Copy a no-follow regular source below one trusted directory fd."""
    flags = os.O_RDONLY
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    source_fd = os.open(source, flags)
    try:
        metadata = os.fstat(source_fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise FileNotFoundError(
                f"payload source is not a real regular file: {source}"
            )
        output_fd = os.open(
            destination_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            stat.S_IMODE(metadata.st_mode),
            dir_fd=destination_fd,
        )
        try:
            while chunk := os.read(source_fd, 1024 * 1024):
                view = memoryview(chunk)
                while view:
                    written = os.write(output_fd, view)
                    view = view[written:]
            os.fchmod(output_fd, stat.S_IMODE(metadata.st_mode))
        finally:
            os.close(output_fd)
    finally:
        os.close(source_fd)


def _copy_directory_snapshot(
    source_fd: int,
    destination: Path,
) -> None:
    """Recursively copy a stable no-follow view from one directory fd."""
    names_before = sorted(os.listdir(source_fd), key=os.fsencode)
    for name in names_before:
        metadata = os.stat(
            name,
            dir_fd=source_fd,
            follow_symlinks=False,
        )
        target = destination / name
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=source_fd,
            )
            try:
                opened = os.fstat(child_fd)
                if (opened.st_dev, opened.st_ino) != (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    raise PayloadValidationError(
                        "payload directory changed while its immutable "
                        "submission snapshot was created"
                    )
                target.mkdir(mode=stat.S_IMODE(metadata.st_mode))
                _copy_directory_snapshot(child_fd, target)
                os.chmod(target, stat.S_IMODE(metadata.st_mode))
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            flags = os.O_RDONLY
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            flags |= getattr(os, "O_NONBLOCK", 0)
            child_fd = os.open(name, flags, dir_fd=source_fd)
            try:
                opened = os.fstat(child_fd)
                if (opened.st_dev, opened.st_ino) != (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    raise PayloadValidationError(
                        "payload file changed while its immutable submission "
                        "snapshot was created"
                    )
                output = os.open(
                    target,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    stat.S_IMODE(opened.st_mode),
                )
                try:
                    while chunk := os.read(child_fd, 1024 * 1024):
                        view = memoryview(chunk)
                        while view:
                            written = os.write(output, view)
                            view = view[written:]
                    os.fchmod(output, stat.S_IMODE(opened.st_mode))
                    os.fsync(output)
                finally:
                    os.close(output)
            finally:
                os.close(child_fd)
        elif stat.S_ISLNK(metadata.st_mode):
            link_target = os.readlink(name, dir_fd=source_fd)
            after = os.stat(
                name,
                dir_fd=source_fd,
                follow_symlinks=False,
            )
            if (after.st_dev, after.st_ino) != (
                metadata.st_dev,
                metadata.st_ino,
            ):
                raise PayloadValidationError(
                    "payload symlink changed while its immutable submission "
                    "snapshot was created"
                )
            target.symlink_to(link_target)
        else:
            raise PayloadValidationError(
                "payload validation failed: directory payload contains an "
                f"unsupported filesystem object at {name!r}"
            )
    if sorted(os.listdir(source_fd), key=os.fsencode) != names_before:
        raise PayloadValidationError(
            "payload directory changed while its immutable submission "
            "snapshot was created"
        )


def _copy_directory_snapshot_at(source_fd: int, destination_fd: int) -> None:
    """Copy a stable tree between already-open directory authorities."""
    names_before = sorted(os.listdir(source_fd), key=os.fsencode)
    for name in names_before:
        metadata = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            source_child = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=source_fd,
            )
            try:
                opened = os.fstat(source_child)
                if not _same_inode(metadata, opened):
                    raise PayloadValidationError(
                        "payload directory changed while its immutable "
                        "submission snapshot was staged"
                    )
                os.mkdir(
                    name,
                    stat.S_IMODE(metadata.st_mode),
                    dir_fd=destination_fd,
                )
                destination_child = os.open(
                    name,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=destination_fd,
                )
                try:
                    _copy_directory_snapshot_at(
                        source_child,
                        destination_child,
                    )
                    os.fchmod(
                        destination_child,
                        stat.S_IMODE(metadata.st_mode),
                    )
                finally:
                    os.close(destination_child)
            finally:
                os.close(source_child)
        elif stat.S_ISREG(metadata.st_mode):
            source_child = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=source_fd,
            )
            try:
                opened = os.fstat(source_child)
                if not _same_inode(metadata, opened):
                    raise PayloadValidationError(
                        "payload file changed while its immutable submission "
                        "snapshot was staged"
                    )
                output_fd = os.open(
                    name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    stat.S_IMODE(opened.st_mode),
                    dir_fd=destination_fd,
                )
                try:
                    while chunk := os.read(source_child, 1024 * 1024):
                        view = memoryview(chunk)
                        while view:
                            written = os.write(output_fd, view)
                            view = view[written:]
                    os.fchmod(output_fd, stat.S_IMODE(opened.st_mode))
                finally:
                    os.close(output_fd)
            finally:
                os.close(source_child)
        elif stat.S_ISLNK(metadata.st_mode):
            target = os.readlink(name, dir_fd=source_fd)
            after = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
            if not _same_inode(metadata, after):
                raise PayloadValidationError(
                    "payload symlink changed while its immutable submission "
                    "snapshot was staged"
                )
            os.symlink(target, name, dir_fd=destination_fd)
        else:
            raise PayloadValidationError(
                "payload validation failed: directory payload contains an "
                f"unsupported filesystem object at {name!r}"
            )
    if sorted(os.listdir(source_fd), key=os.fsencode) != names_before:
        raise PayloadValidationError(
            "payload directory changed while its immutable submission "
            "snapshot was staged"
        )


@contextlib.contextmanager
def _immutable_payload_snapshot(
    *,
    source_file: Path | None,
    source_directory: Path | None,
    source_archive: Path | None,
    final_command: Sequence[str],
    interpreter_explicit: bool,
) -> Iterator[_PayloadSnapshot]:
    """Yield bytes that cannot be swapped between digest and staging."""
    root = Path(tempfile.mkdtemp(prefix="vq-submit-snapshot-"))
    try:
        if source_file is not None:
            snapshot_file = root / source_file.name
            _copy_open_regular_file(source_file, snapshot_file)
            snapshot = _PayloadSnapshot(snapshot_file, None, None)
        elif source_archive is not None:
            snapshot_archive = root / "payload.tar"
            _copy_open_regular_file(source_archive, snapshot_archive)
            validate_staged_python_entrypoint(
                command=final_command,
                source_archive=snapshot_archive,
                interpreter_explicit=interpreter_explicit,
            )
            snapshot = _PayloadSnapshot(None, None, snapshot_archive)
        else:
            assert source_directory is not None
            source_fd = os.open(
                source_directory,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
            )
            snapshot_directory = root / "payload"
            snapshot_directory.mkdir()
            try:
                _copy_directory_snapshot(source_fd, snapshot_directory)
            finally:
                os.close(source_fd)
            validate_staged_python_entrypoint(
                command=final_command,
                source_directory=snapshot_directory,
                interpreter_explicit=interpreter_explicit,
            )
            snapshot = _PayloadSnapshot(None, snapshot_directory, None)
        yield snapshot
    finally:
        # This private snapshot is best-effort scratch. In a keyed submit the
        # context exits after the durable claim has proven acceptance, so even
        # an asynchronous cleanup failure must not replace the job receipt.
        with contextlib.suppress(BaseException):
            shutil.rmtree(root, ignore_errors=True)


def _payload_digest(
    *,
    source_file: Path | None,
    source_directory: Path | None,
    source_archive: Path | None,
) -> tuple[str, str]:
    """Return the logical payload kind and deterministic SHA-256 digest."""
    digest = hashlib.sha256()
    if source_file is not None:
        kind = "file"
        digest.update(b"vq.payload.file.v2\0")
        _digest_framed_bytes(
            digest,
            source_file.name.encode("utf-8", errors="surrogateescape"),
        )
        _digest_framed_bytes(digest, _file_sha256(source_file))
        return kind, digest.hexdigest()
    if source_archive is not None:
        kind = "archive"
        digest.update(b"vq.payload.archive.v2\0")
        _digest_framed_bytes(digest, _file_sha256(source_archive))
        return kind, digest.hexdigest()

    assert source_directory is not None
    kind = "directory"
    digest.update(b"vq.payload.directory.v2\0")

    def visit(directory_path: Path, prefix: tuple[str, ...]) -> None:
        with os.scandir(directory_path) as iterator:
            entries = sorted(iterator, key=lambda entry: os.fsencode(entry.name))
        for entry in entries:
            relative = (*prefix, entry.name)
            encoded_path = "/".join(relative).encode(
                "utf-8", errors="surrogateescape"
            )
            metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                entry_kind = b"d"
            elif stat.S_ISREG(metadata.st_mode):
                entry_kind = b"f"
            elif stat.S_ISLNK(metadata.st_mode):
                entry_kind = b"l"
            else:
                entry_kind = b"o"
            _digest_framed_bytes(digest, entry_kind)
            _digest_framed_bytes(digest, encoded_path)
            _digest_framed_bytes(
                digest,
                str(stat.S_IMODE(metadata.st_mode)).encode("ascii"),
            )
            entry_path = Path(entry.path)
            if entry_kind == b"d":
                _digest_framed_bytes(digest, b"")
                visit(entry_path, relative)
            elif entry_kind == b"f":
                _digest_framed_bytes(digest, _file_sha256(entry_path))
            elif entry_kind == b"l":
                _digest_framed_bytes(
                    digest,
                    os.readlink(entry_path).encode(
                        "utf-8", errors="surrogateescape"
                    ),
                )
            else:
                _digest_framed_bytes(digest, b"")

    visit(source_directory, ())
    return kind, digest.hexdigest()


def _directory_payload_digest_at(directory_fd: int) -> str:
    """Return the v2 directory digest through an inode-stable root fd."""
    digest = hashlib.sha256()
    digest.update(b"vq.payload.directory.v2\0")

    def visit(parent_fd: int, prefix: tuple[str, ...]) -> None:
        for name in sorted(os.listdir(parent_fd), key=os.fsencode):
            relative = (*prefix, name)
            metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                entry_kind = b"d"
            elif stat.S_ISREG(metadata.st_mode):
                entry_kind = b"f"
            elif stat.S_ISLNK(metadata.st_mode):
                entry_kind = b"l"
            else:
                entry_kind = b"o"
            _digest_framed_bytes(digest, entry_kind)
            _digest_framed_bytes(
                digest,
                "/".join(relative).encode(
                    "utf-8",
                    errors="surrogateescape",
                ),
            )
            _digest_framed_bytes(
                digest,
                str(stat.S_IMODE(metadata.st_mode)).encode("ascii"),
            )
            if entry_kind == b"d":
                _digest_framed_bytes(digest, b"")
                child_fd = os.open(
                    name,
                    os.O_RDONLY
                    | getattr(os, "O_DIRECTORY", 0)
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=parent_fd,
                )
                try:
                    visit(child_fd, relative)
                finally:
                    os.close(child_fd)
            elif entry_kind == b"f":
                _digest_framed_bytes(
                    digest,
                    _file_sha256_at(parent_fd, name),
                )
            elif entry_kind == b"l":
                _digest_framed_bytes(
                    digest,
                    os.readlink(name, dir_fd=parent_fd).encode(
                        "utf-8",
                        errors="surrogateescape",
                    ),
                )
            else:
                _digest_framed_bytes(digest, b"")

    visit(directory_fd, ())
    return digest.hexdigest()


def _file_payload_digest_at(directory_fd: int, name: str) -> str:
    digest = hashlib.sha256()
    digest.update(b"vq.payload.file.v2\0")
    _digest_framed_bytes(
        digest,
        name.encode("utf-8", errors="surrogateescape"),
    )
    _digest_framed_bytes(digest, _file_sha256_at(directory_fd, name))
    return digest.hexdigest()


def _submission_intent_digest(
    spec: JobSpec,
    *,
    payload_kind: str,
    payload_digest: str,
    vibeqc_preflight: bool,
) -> str:
    runtime_pin = (
        spec.program_runtime_pin.model_dump(
            mode="json",
            exclude={"resolved_git_sha"},
        )
        if spec.program_runtime_pin is not None
        else None
    )
    intent: dict[str, object] = {
        "schema": "vq.submission-intent.v1",
        "scheduler_target": spec.scheduler_target,
        "program": spec.program,
        "program_runtime_pin": runtime_pin,
        "payload": {"kind": payload_kind, "sha256": payload_digest},
        "job_name": spec.job_name,
        "command": spec.command,
        "resources": {
            "cpus": spec.cpus,
            "scheduler_tasks": spec.scheduler_tasks,
            "mem_mb": spec.mem_mb,
            "wall_time_seconds": spec.wall_time_seconds,
            "priority": spec.priority,
        },
        "dependencies": {
            "afterok": spec.depends_on,
            "afterany": spec.depends_on_any,
        },
        "tags": spec.tags,
        "execution": {
            "recover_on_reboot": spec.recover_on_reboot,
            "retry_max": spec.retry_max,
            "branch": spec.branch,
            "not_before": spec.not_before,
            "rerun_until_file_exists": spec.rerun_until_file_exists,
            "rerun_max": spec.rerun_max,
            "clean_workdir_on_terminal": spec.clean_workdir_on_terminal,
            "refresh_before": spec.refresh_before,
            "qvf_artifact_name": spec.qvf_artifact_name,
            "vibeqc_preflight": vibeqc_preflight,
        },
    }
    encoded = json.dumps(
        intent,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _idempotency_binding(
    queue_dir: Path,
    *,
    key: str,
    intent_digest: str,
) -> _IdempotencyBinding:
    owner_hash = hashlib.sha256(f"uid:{os.geteuid()}".encode()).hexdigest()
    key_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
    claim_path = (
        queue_dir
        / ".submit-idempotency"
        / owner_hash
        / f"{key_hash}.json"
    )
    return _IdempotencyBinding(
        claim_path=claim_path,
        owner_hash=owner_hash,
        key_hash=key_hash,
        intent_digest=intent_digest,
    )


def _open_real_directory(path: Path) -> int:
    flags = os.O_RDONLY
    flags |= getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise ValueError(f"unsafe authority directory {path}")
    return descriptor


def _fsync_durable_directory(descriptor: int) -> None:
    """Durably publish authority metadata when the filesystem supports it.

    A few filesystems reject directory ``fsync`` with a documented
    unsupported-operation error. Those platforms still get atomic metadata
    publication. I/O and capacity failures must propagate: treating EIO or
    ENOSPC as success would falsely claim that an idempotency receipt is
    durable.
    """
    try:
        os.fsync(descriptor)
    except OSError as exc:
        unsupported = {
            errno.EINVAL,
            getattr(errno, "ENOTSUP", errno.EINVAL),
            getattr(errno, "EOPNOTSUPP", errno.EINVAL),
        }
        if exc.errno not in unsupported:
            raise


def _open_child_authority_directory(
    parent_fd: int,
    name: str,
    *,
    create: bool,
    authority_uid: int,
    authority_gid: int,
) -> int | None:
    created = False
    if create:
        try:
            os.mkdir(name, 0o700, dir_fd=parent_fd)
            created = True
        except FileExistsError:
            pass
    flags = os.O_RDONLY
    flags |= getattr(os, "O_DIRECTORY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            return None
        raise
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode):
        os.close(descriptor)
        raise ValueError("unsafe idempotency authority directory")
    try:
        if created:
            _align_authority_owner(
                descriptor,
                authority_uid=authority_uid,
                authority_gid=authority_gid,
            )
            metadata = os.fstat(descriptor)
        if (
            metadata.st_uid != authority_uid
            or metadata.st_gid != authority_gid
        ):
            raise ValueError(
                "idempotency authority directory has the wrong owner"
            )
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise ValueError(
                "idempotency authority directory has unsafe permissions"
            )
    except BaseException:
        os.close(descriptor)
        raise
    # Sync on both creation and authoritative reopen. A prior caller may have
    # created the entry but observed an EIO/ENOSPC while publishing its parent;
    # retrying must repair that failed durability barrier before trusting it.
    try:
        _fsync_durable_directory(parent_fd)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _align_authority_owner(
    descriptor: int,
    *,
    authority_uid: int,
    authority_gid: int,
) -> None:
    """Make a root-created authority entry usable by its queue owner."""
    metadata = os.fstat(descriptor)
    if metadata.st_uid == authority_uid and metadata.st_gid == authority_gid:
        return
    os.fchown(descriptor, authority_uid, authority_gid)
    updated = os.fstat(descriptor)
    if updated.st_uid != authority_uid or updated.st_gid != authority_gid:
        raise ValueError("failed to preserve idempotency authority owner")


def _same_inode(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _revalidate_idempotency_store(store: _IdempotencyStore) -> None:
    """Prove every held authority fd is still reachable at its exact name."""
    try:
        queue_entry = os.lstat(store.queue_path)
        queue_open = os.fstat(store.queue_fd)
        namespace_entry = os.stat(
            ".submit-idempotency",
            dir_fd=store.queue_fd,
            follow_symlinks=False,
        )
        namespace_open = os.fstat(store.namespace_fd)
        owner_entry = os.stat(
            store.owner_name,
            dir_fd=store.namespace_fd,
            follow_symlinks=False,
        )
        owner_open = os.fstat(store.owner_fd)
    except OSError as exc:
        raise _IdempotencyAuthorityReplacedError(
            "durable idempotency authority was replaced during submission"
        ) from exc
    if (
        not stat.S_ISDIR(queue_entry.st_mode)
        or not stat.S_ISDIR(namespace_entry.st_mode)
        or not stat.S_ISDIR(owner_entry.st_mode)
        or not _same_inode(queue_entry, queue_open)
        or not _same_inode(namespace_entry, namespace_open)
        or not _same_inode(owner_entry, owner_open)
        or namespace_open.st_uid != store.authority_uid
        or namespace_open.st_gid != store.authority_gid
        or owner_open.st_uid != store.authority_uid
        or owner_open.st_gid != store.authority_gid
    ):
        raise _IdempotencyAuthorityReplacedError(
            "durable idempotency authority was replaced during submission"
        )
    if store.lock_fd is not None and store.lock_name is not None:
        try:
            lock_entry = os.stat(
                store.lock_name,
                dir_fd=store.owner_fd,
                follow_symlinks=False,
            )
            lock_open = os.fstat(store.lock_fd)
        except OSError as exc:
            raise _IdempotencyAuthorityReplacedError(
                "durable idempotency authority lock was replaced"
            ) from exc
        if not stat.S_ISREG(lock_entry.st_mode) or not _same_inode(
            lock_entry,
            lock_open,
        ) or (
            lock_open.st_uid != store.authority_uid
            or lock_open.st_gid != store.authority_gid
        ):
            raise _IdempotencyAuthorityReplacedError(
                "durable idempotency authority lock was replaced"
            )


def _revalidate_workspace_authority(
    jobs_path: Path,
    jobs_fd: int,
    job_id: str,
    workspace_fd: int,
) -> None:
    """Bind a keyed workspace to its canonical parent/name and held inode."""
    try:
        jobs_entry = os.lstat(jobs_path)
        jobs_open = os.fstat(jobs_fd)
        workspace_entry = os.stat(
            job_id,
            dir_fd=jobs_fd,
            follow_symlinks=False,
        )
        workspace_open = os.fstat(workspace_fd)
    except OSError as exc:
        raise _WorkspaceAuthorityReplacedError(
            "durable workspace authority was replaced during submission"
        ) from exc
    if (
        not stat.S_ISDIR(jobs_entry.st_mode)
        or not stat.S_ISDIR(workspace_entry.st_mode)
        or not _same_inode(jobs_entry, jobs_open)
        or not _same_inode(workspace_entry, workspace_open)
    ):
        raise _WorkspaceAuthorityReplacedError(
            "durable workspace authority was replaced during submission"
        )


def _remove_tree_at(
    parent_fd: int,
    name: str,
    *,
    expected_root: os.stat_result | None = None,
) -> None:
    """Remove one owned tree relative to a held parent without path lookup."""
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if expected_root is not None:
        if not _same_inode(metadata, expected_root):
            raise _AuthorityReplacedError(
                "owned workspace entry was replaced before rollback"
            )
        # POSIX offers no identity-qualified recursive unlink/rmdir. Another
        # process with authority over this state tree can replace a pathname
        # between any inode check and the following mutation. Retain the exact
        # unclaimed reservation intact instead of risking deletion of an
        # unrelated replacement. With no published spec or claim it is inert.
        return
    if not stat.S_ISDIR(metadata.st_mode):
        os.unlink(name, dir_fd=parent_fd)
        return
    child_fd = os.open(
        name,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
        dir_fd=parent_fd,
    )
    try:
        opened = os.fstat(child_fd)
        if not _same_inode(metadata, opened):
            raise ValueError("owned workspace changed during rollback")
        for child_name in os.listdir(child_fd):
            _remove_tree_at(child_fd, child_name)
        final_entry = os.stat(
            name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if not _same_inode(opened, final_entry):
            raise _AuthorityReplacedError(
                "owned workspace entry was replaced during rollback"
            )
        # POSIX has no identity-qualified rmdir-at operation. Even with the
        # original fd held open, a same-owner process can replace ``name``
        # between the stat above and a pathname rmdir. Leave this now-empty
        # exclusive reservation as a harmless orphan rather than risk deleting
        # an unrelated replacement. The absent spec/claim means it is never
        # dispatchable and an operator can reclaim it under exclusive state
        # maintenance.
    finally:
        os.close(child_fd)


@contextlib.contextmanager
def _idempotency_store_lock(
    queue_dir: Path,
    binding: _IdempotencyBinding,
    *,
    create_namespace: bool,
    create_lock: bool,
) -> Iterator[_IdempotencyStore | None]:
    """Lock one key through no-follow dirfds, never pathname re-resolution."""
    if create_namespace:
        queue_dir.mkdir(parents=True, exist_ok=True)
    try:
        queue_fd = _open_real_directory(queue_dir)
    except FileNotFoundError:
        if not create_namespace:
            yield None
            return
        raise
    namespace_fd: int | None = None
    owner_fd: int | None = None
    lock_fd: int | None = None
    try:
        queue_metadata = os.fstat(queue_fd)
        authority_uid = queue_metadata.st_uid
        authority_gid = queue_metadata.st_gid
        namespace_fd = _open_child_authority_directory(
            queue_fd,
            ".submit-idempotency",
            create=create_namespace,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
        )
        if namespace_fd is None:
            yield None
            return
        owner_fd = _open_child_authority_directory(
            namespace_fd,
            binding.owner_hash,
            create=create_namespace,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
        )
        if owner_fd is None:
            yield None
            return
        lock_name = f"{binding.key_hash}.lock"
        flags = os.O_RDWR
        flags |= getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        created = False
        if create_lock:
            try:
                lock_fd = os.open(
                    lock_name,
                    flags | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=owner_fd,
                )
                created = True
                _align_authority_owner(
                    lock_fd,
                    authority_uid=authority_uid,
                    authority_gid=authority_gid,
                )
            except FileExistsError:
                lock_fd = os.open(lock_name, flags, dir_fd=owner_fd)
        else:
            try:
                lock_fd = os.open(lock_name, flags, dir_fd=owner_fd)
            except FileNotFoundError:
                yield None
                return
        lock_metadata = os.fstat(lock_fd)
        if not stat.S_ISREG(lock_metadata.st_mode):
            raise ValueError("invalid durable idempotency claim lock")
        if (
            lock_metadata.st_uid != authority_uid
            or lock_metadata.st_gid != authority_gid
        ):
            raise ValueError("durable idempotency claim lock has wrong owner")
        if stat.S_IMODE(lock_metadata.st_mode) & 0o077:
            raise ValueError(
                "durable idempotency claim lock has unsafe permissions"
            )
        if created:
            os.fsync(lock_fd)
        # Publish a new lock or repair a prior failed parent-directory fsync
        # before any existing claim/spec is accepted as authoritative.
        _fsync_durable_directory(owner_fd)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        entry = os.stat(
            lock_name,
            dir_fd=owner_fd,
            follow_symlinks=False,
        )
        if (entry.st_dev, entry.st_ino) != (
            lock_metadata.st_dev,
            lock_metadata.st_ino,
        ):
            raise ValueError("durable idempotency claim lock was replaced")
        yield _IdempotencyStore(
            queue_path=queue_dir,
            queue_fd=queue_fd,
            namespace_fd=namespace_fd,
            owner_fd=owner_fd,
            owner_name=binding.owner_hash,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
            lock_fd=lock_fd,
            lock_name=lock_name,
            claim_name=f"{binding.key_hash}.json",
        )
    finally:
        if lock_fd is not None:
            with contextlib.suppress(BaseException):
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            with contextlib.suppress(BaseException):
                os.close(lock_fd)
        if owner_fd is not None:
            with contextlib.suppress(BaseException):
                os.close(owner_fd)
        if namespace_fd is not None:
            with contextlib.suppress(BaseException):
                os.close(namespace_fd)
        with contextlib.suppress(BaseException):
            os.close(queue_fd)


@contextlib.contextmanager
def _existing_idempotency_store(
    queue_dir: Path,
    binding: _IdempotencyBinding,
) -> Iterator[_IdempotencyStore | None]:
    """Open an existing claim namespace without creating or locking it."""
    try:
        queue_fd = _open_real_directory(queue_dir)
    except FileNotFoundError:
        yield None
        return
    namespace_fd: int | None = None
    owner_fd: int | None = None
    try:
        queue_metadata = os.fstat(queue_fd)
        authority_uid = queue_metadata.st_uid
        authority_gid = queue_metadata.st_gid
        namespace_fd = _open_child_authority_directory(
            queue_fd,
            ".submit-idempotency",
            create=False,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
        )
        if namespace_fd is None:
            yield None
            return
        owner_fd = _open_child_authority_directory(
            namespace_fd,
            binding.owner_hash,
            create=False,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
        )
        if owner_fd is None:
            yield None
            return
        _fsync_durable_directory(owner_fd)
        yield _IdempotencyStore(
            queue_path=queue_dir,
            queue_fd=queue_fd,
            namespace_fd=namespace_fd,
            owner_fd=owner_fd,
            owner_name=binding.owner_hash,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
            lock_fd=None,
            lock_name=None,
            claim_name=f"{binding.key_hash}.json",
        )
    finally:
        if owner_fd is not None:
            with contextlib.suppress(BaseException):
                os.close(owner_fd)
        if namespace_fd is not None:
            with contextlib.suppress(BaseException):
                os.close(namespace_fd)
        with contextlib.suppress(BaseException):
            os.close(queue_fd)


def _read_idempotency_claim(
    binding: _IdempotencyBinding,
    store: _IdempotencyStore,
) -> tuple[str, str] | None:
    _revalidate_idempotency_store(store)
    try:
        descriptor = os.open(
            store.claim_name,
            os.O_RDONLY
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=store.owner_fd,
        )
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ValueError("invalid durable idempotency claim record") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != store.authority_uid
            or metadata.st_gid != store.authority_gid
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size > _IDEMPOTENCY_CLAIM_LIMIT
        ):
            raise ValueError("invalid durable idempotency claim record")
        raw = os.read(descriptor, _IDEMPOTENCY_CLAIM_LIMIT + 1)
        if len(raw) > _IDEMPOTENCY_CLAIM_LIMIT:
            raise ValueError("invalid durable idempotency claim record")
        entry = os.stat(
            store.claim_name,
            dir_fd=store.owner_fd,
            follow_symlinks=False,
        )
        if (entry.st_dev, entry.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ValueError("durable idempotency claim was replaced")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid durable idempotency claim record") from exc
    required = {
        "schema",
        "owner_hash",
        "key_hash",
        "intent_digest",
        "job_id",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError("invalid durable idempotency claim record")
    if (
        payload.get("schema") != _IDEMPOTENCY_CLAIM_SCHEMA
        or payload.get("owner_hash") != binding.owner_hash
        or payload.get("key_hash") != binding.key_hash
    ):
        raise ValueError("invalid durable idempotency claim binding")
    observed_intent = payload.get("intent_digest")
    job_id = payload.get("job_id")
    if (
        not isinstance(observed_intent, str)
        or re.fullmatch(r"[0-9a-f]{64}", observed_intent) is None
        or not isinstance(job_id, str)
    ):
        raise ValueError("invalid durable idempotency claim record")
    validate_job_id(job_id)
    _revalidate_idempotency_store(store)
    return job_id, observed_intent


def _write_idempotency_claim(
    binding: _IdempotencyBinding,
    job_id: str,
    store: _IdempotencyStore,
) -> None:
    _revalidate_idempotency_store(store)
    payload = {
        "schema": _IDEMPOTENCY_CLAIM_SCHEMA,
        "owner_hash": binding.owner_hash,
        "key_hash": binding.key_hash,
        "intent_digest": binding.intent_digest,
        "job_id": validate_job_id(job_id),
    }
    encoded = (
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")
    temporary = f".{store.claim_name}.{uuid.uuid4().hex}.tmp"
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=store.owner_fd,
        )
        try:
            _align_authority_owner(
                descriptor,
                authority_uid=store.authority_uid,
                authority_gid=store.authority_gid,
            )
            view = memoryview(encoded)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(
                temporary,
                store.claim_name,
                src_dir_fd=store.owner_fd,
                dst_dir_fd=store.owner_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            existing = _read_idempotency_claim(binding, store)
            if existing != (job_id, binding.intent_digest):
                raise IdempotencyConflict(
                    "durable idempotency key is already bound to a "
                    "different job"
                ) from None
        else:
            _fsync_durable_directory(store.owner_fd)
        _revalidate_idempotency_store(store)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(temporary, dir_fd=store.owner_fd)


def ensure_idempotency_claim_for_spec(
    queue_dir: Path,
    spec: JobSpec,
) -> None:
    """Repair a spec-first claim before lifecycle code clears its binding.

    A keyed submit deliberately publishes its spec before its tombstone claim.
    If the process dies in that narrow gap, the spec hashes are the repair
    evidence.  Retry/resubmit code must not erase that evidence before turning
    it into the durable claim, or a later identical client retry can create a
    duplicate logical job.  The caller must hold the spec lock while invoking
    this helper; the per-key lock serializes it against submitters.
    """
    fields = (
        spec.submission_owner_hash,
        spec.idempotency_key_hash,
        spec.submission_intent_digest,
    )
    if all(value is None for value in fields):
        return
    if any(value is None for value in fields):
        raise ValueError("incomplete durable idempotency binding on job spec")
    owner_hash, key_hash, intent_digest = fields
    assert owner_hash is not None
    assert key_hash is not None
    assert intent_digest is not None
    binding = _IdempotencyBinding(
        claim_path=(
            queue_dir
            / ".submit-idempotency"
            / owner_hash
            / f"{key_hash}.json"
        ),
        owner_hash=owner_hash,
        key_hash=key_hash,
        intent_digest=intent_digest,
    )
    with _idempotency_store_lock(
        queue_dir,
        binding,
        create_namespace=True,
        create_lock=True,
    ) as store:
        assert store is not None
        existing = _read_idempotency_claim(binding, store)
        if existing is not None:
            claimed_job_id, claimed_intent = existing
            if claimed_job_id != spec.id or claimed_intent != intent_digest:
                raise IdempotencyConflict(
                    "durable idempotency key is already bound to a different job"
                )
            return
        _write_idempotency_claim(binding, spec.id, store)


def _claim_or_repair_idempotent_submission(
    queue_dir: Path,
    binding: _IdempotencyBinding,
    store: _IdempotencyStore,
) -> str | None:
    match = _lookup_idempotent_submission(queue_dir, binding, store)
    if match is None:
        return None
    job_id, needs_repair = match
    if needs_repair:
        _write_idempotency_claim(binding, job_id, store)
    return job_id


def _replay_preexisting_idempotent_submission(
    queue_dir: Path,
    binding: _IdempotencyBinding,
) -> str | None:
    """Return an already-accepted keyed job before new-admission gates.

    Existing claims remain authoritative even if a crash lost the directory
    entry for their lock. An unlocked first classification is read-only; when
    it finds evidence, a new durable lock is acquired and evidence is checked
    again before any claim repair.
    """
    with _idempotency_store_lock(
        queue_dir,
        binding,
        create_namespace=False,
        create_lock=False,
    ) as store:
        if store is not None:
            return _claim_or_repair_idempotent_submission(
                queue_dir,
                binding,
                store,
            )
    # A lock can be absent after a crash even though its fsync'd claim/spec
    # survived. Classify existing evidence without creating a new-key lock,
    # then create a durable lock only when that read proves there is something
    # to reconcile. The classification is repeated under the new lock.
    with _existing_idempotency_store(queue_dir, binding) as store:
        if store is None:
            return None
        preliminary = _lookup_idempotent_submission(
            queue_dir,
            binding,
            store,
        )
    if preliminary is None:
        return None
    with _idempotency_store_lock(
        queue_dir,
        binding,
        create_namespace=False,
        create_lock=True,
    ) as store:
        if store is None:
            return None
        return _claim_or_repair_idempotent_submission(
            queue_dir,
            binding,
            store,
        )


def _lookup_idempotent_submission(
    queue_dir: Path,
    binding: _IdempotencyBinding,
    store: _IdempotencyStore,
) -> tuple[str, bool] | None:
    """Classify a claim or crash-gap spec without mutating queue state.

    Every top-level JSON entry is potential crash-repair evidence.  If one
    cannot be read as a bounded, stable, no-follow regular spec, accepting a
    new keyed job could create a duplicate.  Fail closed rather than silently
    skipping evidence we could not classify.
    """
    _revalidate_idempotency_store(store)
    claim = _read_idempotency_claim(binding, store)
    if claim is not None:
        job_id, observed_intent = claim
        if observed_intent != binding.intent_digest:
            raise IdempotencyConflict(
                "idempotency key was already used for a different submission intent"
            )
        _revalidate_idempotency_store(store)
        return job_id, False

    # Scan through the same held queue descriptor whose ancestry and inode are
    # revalidated above. Reopening ``queue_dir`` by pathname here would admit
    # a rename/restore ABA: both pathname checks could see the right directory
    # while the repair scan briefly classified an unrelated replacement.
    _fsync_durable_directory(store.queue_fd)
    spec_names = sorted(
        name
        for name in os.listdir(store.queue_fd)
        if name.endswith(".json")
    )
    candidates: list[tuple[str, JobSpec]] = []
    for spec_name in spec_names:
        try:
            candidate = spec_access.read_bounded_regular_spec_at(
                store.queue_fd,
                spec_name,
                display_directory=queue_dir,
            )
        except (OSError, UnicodeError, ValueError) as exc:
            raise ValueError(
                "cannot safely classify durable job specs while repairing "
                "an idempotency claim"
            ) from exc
        if candidate.id != spec_name.removesuffix(".json"):
            raise ValueError(
                "cannot safely classify a durable job spec whose id does "
                "not match its authority filename"
            )
        candidates.append((spec_name, candidate))

    matches: list[JobSpec] = []
    for _spec_name, candidate in candidates:
        try:
            fields = (
                candidate.submission_owner_hash,
                candidate.idempotency_key_hash,
                candidate.submission_intent_digest,
            )
        except AttributeError as exc:
            raise ValueError("invalid durable idempotency binding") from exc
        if any(value is not None for value in fields) and any(
            value is None for value in fields
        ):
            raise ValueError("incomplete durable idempotency binding on job spec")
        if (
            candidate.submission_owner_hash == binding.owner_hash
            and candidate.idempotency_key_hash == binding.key_hash
        ):
            matches.append(candidate)
    if not matches:
        _revalidate_idempotency_store(store)
        return None
    if len(matches) != 1:
        raise IdempotencyConflict(
            "idempotency key is bound to multiple durable job specs"
        )
    match = matches[0]
    if match.submission_intent_digest != binding.intent_digest:
        raise IdempotencyConflict(
            "idempotency key was already used for a different submission intent"
        )
    _revalidate_idempotency_store(store)
    return match.id, True


def _write_spec_exclusive(
    spec: JobSpec,
    spec_path: Path,
    *,
    strict_directory_fsync: bool = False,
    directory_fd: int | None = None,
) -> None:
    """Atomically publish a complete spec without replacing another writer.

    ``JobSpec.write`` intentionally replaces an existing spec for lifecycle
    transitions. Initial submission has the opposite contract: a random-ID
    collision must never overwrite an accepted job. Write and fsync a unique
    same-directory inode, then hard-link it into the final name. ``link`` is
    an atomic create-if-absent operation; readers see either no spec or the
    fully written bytes, and a concurrent winner is preserved.
    """
    if directory_fd is None:
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temp_path = tempfile.mkstemp(
            dir=str(spec_path.parent),
            prefix=f".{spec_path.name}.",
            suffix=".submit",
        )
        temp_name = Path(temp_path).name
    else:
        temp_name = f".{spec_path.name}.{uuid.uuid4().hex}.submit"
        descriptor = os.open(
            temp_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(spec.to_json().encode("utf-8"))
            stream.flush()
            os.fchmod(stream.fileno(), 0o644)
            os.fsync(stream.fileno())
        try:
            if directory_fd is None:
                os.link(spec_path.parent / temp_name, spec_path)
            else:
                os.link(
                    temp_name,
                    spec_path.name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
        except FileExistsError:
            raise FileExistsError(
                f"job ID collision: durable spec already exists for {spec.id}"
            ) from None
        # The link above is the publication point. Unsupported directory
        # fsync remains portable, but EIO/ENOSPC must surface. The caller's
        # exact-byte publication check retains the workspace on such an error;
        # a keyed retry re-fsyncs the queue directory before repairing a claim.
        if directory_fd is None:
            directory_descriptor = _open_real_directory(spec_path.parent)
            try:
                if strict_directory_fsync:
                    _fsync_durable_directory(directory_descriptor)
                else:
                    with contextlib.suppress(OSError):
                        os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        elif strict_directory_fsync:
            _fsync_durable_directory(directory_fd)
        else:
            with contextlib.suppress(OSError):
                os.fsync(directory_fd)
    finally:
        with contextlib.suppress(OSError):
            if directory_fd is None:
                os.unlink(spec_path.parent / temp_name)
            else:
                os.unlink(temp_name, dir_fd=directory_fd)


def _copy_directory_contents(source: Path, destination: Path) -> None:
    for item in source.iterdir():
        target = destination / item.name
        if item.is_symlink():
            target.symlink_to(
                os.readlink(item),
                target_is_directory=item.is_dir(),
            )
        elif item.is_dir():
            shutil.copytree(item, target, symlinks=True)
        else:
            shutil.copy2(item, target)


def _fsync_workspace_tree(directory: Path) -> None:
    """Make staged regular bytes durable before publishing their spec."""
    with os.scandir(directory) as iterator:
        entries = list(iterator)
    for entry in entries:
        metadata = entry.stat(follow_symlinks=False)
        path = Path(entry.path)
        if stat.S_ISDIR(metadata.st_mode):
            _fsync_workspace_tree(path)
        elif stat.S_ISREG(metadata.st_mode):
            descriptor = os.open(
                path,
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino) != (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    raise PayloadValidationError(
                        "staged payload changed before durable publication"
                    )
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        elif not stat.S_ISLNK(metadata.st_mode):
            raise PayloadValidationError(
                "staged payload contains an unsupported filesystem object"
            )
    descriptor = _open_real_directory(directory)
    try:
        _fsync_durable_directory(descriptor)
    finally:
        os.close(descriptor)


def _fsync_workspace_tree_at(directory_fd: int) -> None:
    """Durably sync a staged tree below one held directory descriptor."""
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(child_fd)
                if not _same_inode(metadata, opened):
                    raise PayloadValidationError(
                        "staged payload changed before durable publication"
                    )
                _fsync_workspace_tree_at(child_fd)
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            descriptor = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(descriptor)
                if not _same_inode(metadata, opened):
                    raise PayloadValidationError(
                        "staged payload changed before durable publication"
                    )
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        elif not stat.S_ISLNK(metadata.st_mode):
            raise PayloadValidationError(
                "staged payload contains an unsupported filesystem object"
            )
    _fsync_durable_directory(directory_fd)


def _commit_local_submission(
    *,
    spec: JobSpec,
    queue_dir: Path,
    jobs_dir: Path,
    source_file: Path | None,
    source_directory: Path | None,
    source_archive: Path | None,
    final_command: list[str],
    vibeqc_preflight: bool,
    source_hint: str | None,
    binding: _IdempotencyBinding | None,
    idempotency_store: _IdempotencyStore | None,
    expected_payload: tuple[str, str] | None,
    multi_user: bool,
    warning_sink: Callable[[str], None] | None,
    capacity_warnings: tuple[str, ...],
) -> str:
    """Publish one already-validated spec, workspace, and optional claim."""
    jobid = spec.id
    workspace = Path(spec.cwd)
    spec_path = queue_dir / f"{jobid}.json"
    keyed = binding is not None
    jobs_descriptor: int | None = None
    workspace_descriptor: int | None = None
    reserved_workspace: os.stat_result | None = None
    spec_published = False
    claim_published = False
    workspace_authority_failed = False
    queue_authority_failed = False
    try:
        if keyed:
            assert idempotency_store is not None
            _revalidate_idempotency_store(idempotency_store)
            jobs_dir.mkdir(parents=True, exist_ok=True)
            jobs_descriptor = _open_real_directory(jobs_dir)
            _fsync_durable_directory(jobs_descriptor)
            try:
                os.stat(
                    spec_path.name,
                    dir_fd=idempotency_store.queue_fd,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            else:
                raise FileExistsError(
                    "job ID collision: durable spec already exists for "
                    f"{jobid}"
                )
            try:
                os.mkdir(jobid, dir_fd=jobs_descriptor)
            except FileExistsError:
                raise FileExistsError(
                    f"job ID collision: workspace already exists for {jobid}"
                ) from None
            reserved_workspace = os.stat(
                jobid,
                dir_fd=jobs_descriptor,
                follow_symlinks=False,
            )
            workspace_descriptor = os.open(
                jobid,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=jobs_descriptor,
            )
            if not _same_inode(
                reserved_workspace,
                os.fstat(workspace_descriptor),
            ):
                raise _WorkspaceAuthorityReplacedError(
                    "reserved workspace was replaced before staging"
                )
            _revalidate_workspace_authority(
                jobs_dir,
                jobs_descriptor,
                jobid,
                workspace_descriptor,
            )
        else:
            queue_dir.mkdir(parents=True, exist_ok=True)
            jobs_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.lstat(spec_path)
            except FileNotFoundError:
                pass
            else:
                raise FileExistsError(
                    "job ID collision: durable spec already exists for "
                    f"{jobid}"
                )
            try:
                # The exclusive directory creation is the job-id reservation.
                # It refuses a real path or symlink without following either.
                workspace.mkdir(parents=False, exist_ok=False)
            except FileExistsError:
                raise FileExistsError(
                    f"job ID collision: workspace already exists for {jobid}"
                ) from None
            reserved_workspace = os.lstat(workspace)

        if keyed:
            assert workspace_descriptor is not None
            if source_file is not None:
                _copy_open_regular_file_at(
                    source_file,
                    workspace_descriptor,
                    source_file.name,
                )
                observed_payload = (
                    "file",
                    _file_payload_digest_at(
                        workspace_descriptor,
                        source_file.name,
                    ),
                )
            elif source_directory is not None:
                source_descriptor = _open_real_directory(source_directory)
                try:
                    _copy_directory_snapshot_at(
                        source_descriptor,
                        workspace_descriptor,
                    )
                finally:
                    os.close(source_descriptor)
                observed_payload = (
                    "directory",
                    _directory_payload_digest_at(workspace_descriptor),
                )
            else:
                assert source_archive is not None
                observed_payload = _payload_digest(
                    source_file=None,
                    source_directory=None,
                    source_archive=source_archive,
                )
                with tempfile.TemporaryDirectory(
                    prefix="vq-submit-extract-"
                ) as extracted_name:
                    extracted = Path(extracted_name)
                    with tarfile.open(source_archive) as archive_file:
                        archive_file.extractall(extracted, filter="data")
                    extracted_descriptor = _open_real_directory(extracted)
                    try:
                        _copy_directory_snapshot_at(
                            extracted_descriptor,
                            workspace_descriptor,
                        )
                    finally:
                        os.close(extracted_descriptor)
        elif source_file is not None:
            staged_file = workspace / source_file.name
            shutil.copy2(source_file, staged_file)
            observed_payload = None
        elif source_directory is not None:
            _copy_directory_contents(source_directory, workspace)
            observed_payload = None
        else:
            assert source_archive is not None
            with tarfile.open(source_archive) as archive_file:
                archive_file.extractall(workspace, filter="data")
            observed_payload = None

        if (
            expected_payload is not None
            and observed_payload != expected_payload
        ):
            raise PayloadValidationError(
                "submission payload changed while acquiring idempotency claim; "
                "no job was published, so retry with a stable source"
            )

        expected_outputs: list[str] = []
        output_stem: str | None = None
        if vibeqc_preflight:
            if not keyed:
                preflight = vibeqc_dry_run_preflight(workspace, final_command)
            else:
                # A keyed intent binds the immutable staged payload. Execute
                # opt-in dry-run user code only in a disposable mirror so it
                # cannot alter the bytes later dispatched under that intent.
                assert workspace_descriptor is not None
                with tempfile.TemporaryDirectory(
                    prefix="vq-submit-preflight-"
                ) as preflight_name:
                    preflight_workspace = Path(preflight_name)
                    preflight_descriptor = _open_real_directory(
                        preflight_workspace
                    )
                    try:
                        _copy_directory_snapshot_at(
                            workspace_descriptor,
                            preflight_descriptor,
                        )
                    finally:
                        os.close(preflight_descriptor)
                    preflight = vibeqc_dry_run_preflight(
                        preflight_workspace,
                        final_command,
                    )
            if preflight is not None:
                expected_outputs = list(preflight.expected_outputs)
                output_stem = preflight.output_stem
        spec.expected_outputs = expected_outputs
        spec.output_stem = output_stem
        if keyed:
            assert workspace_descriptor is not None
            assert jobs_descriptor is not None
            assert idempotency_store is not None
            _fsync_workspace_tree_at(workspace_descriptor)
            _fsync_durable_directory(jobs_descriptor)
            _revalidate_workspace_authority(
                jobs_dir,
                jobs_descriptor,
                jobid,
                workspace_descriptor,
            )
            assert idempotency_store is not None
            _revalidate_idempotency_store(idempotency_store)
        _write_spec_exclusive(
            spec,
            spec_path,
            strict_directory_fsync=keyed,
            directory_fd=(
                idempotency_store.queue_fd
                if idempotency_store is not None
                else None
            ),
        )
        spec_published = True
        if keyed:
            assert binding is not None
            assert idempotency_store is not None
            assert jobs_descriptor is not None
            assert workspace_descriptor is not None
            _revalidate_workspace_authority(
                jobs_dir,
                jobs_descriptor,
                jobid,
                workspace_descriptor,
            )
            _revalidate_idempotency_store(idempotency_store)
            # Spec first, claim second. A crash in this narrow gap is repaired
            # by scanning the bound spec through the held queue descriptor.
            try:
                _write_idempotency_claim(binding, jobid, idempotency_store)
            except BaseException:
                # The immutable hard-link may already have published the
                # canonical claim before a later fsync/close/observer failure.
                # When exact claim bytes remain readable through the held
                # authority, acceptance is proven and the job ID must not be
                # hidden by post-publication housekeeping.
                try:
                    existing_claim = _read_idempotency_claim(
                        binding,
                        idempotency_store,
                    )
                except BaseException:
                    raise
                if existing_claim != (jobid, binding.intent_digest):
                    raise
            # This is the irreversible acceptance point. Every fallible
            # authority check is deliberately above it. If local cleanup or an
            # observer interrupt follows, retain the spec/workspace and let a
            # replay return the durable claim instead of hiding acceptance.
            claim_published = True
    except _WorkspaceAuthorityReplacedError:
        workspace_authority_failed = True
        raise
    except _IdempotencyAuthorityReplacedError:
        if idempotency_store is not None:
            try:
                queue_authority_failed = not _same_inode(
                    os.lstat(queue_dir),
                    os.fstat(idempotency_store.queue_fd),
                )
            except OSError:
                queue_authority_failed = True
        raise
    finally:
        if not spec_published:
            # Publication is the successful hard-link inside
            # ``_write_spec_exclusive``. An exception or local interrupt can
            # arrive after that point but before the helper returns. Detect
            # the exact bounded regular spec and retain its owned workspace;
            # a later keyed replay repairs the claim gap.
            try:
                if idempotency_store is None:
                    published = spec_access.read_bounded_regular_spec(spec_path)
                else:
                    published = spec_access.read_bounded_regular_spec_at(
                        idempotency_store.queue_fd,
                        spec_path.name,
                        display_directory=queue_dir,
                    )
            except (OSError, UnicodeError, ValueError):
                published = None
            if (
                published is not None
                and published.to_json() == spec.to_json()
            ):
                spec_published = True
        if (
            (workspace_authority_failed or queue_authority_failed)
            and spec_published
            and not claim_published
            and idempotency_store is not None
        ):
            # A workspace-parent failure means the canonical spec cannot be
            # allowed to point at detached or replaced payload bytes. Remove
            # only our exact serialized spec through the held queue authority,
            # then durably publish that removal. If either step fails, retain
            # the workspace as crash evidence; never create a dangling spec by
            # deleting payload bytes after an uncertain namespace update.
            try:
                published = spec_access.read_bounded_regular_spec_at(
                    idempotency_store.queue_fd,
                    spec_path.name,
                    display_directory=queue_dir,
                )
                if published.to_json() == spec.to_json():
                    os.unlink(
                        spec_path.name,
                        dir_fd=idempotency_store.queue_fd,
                    )
                    _fsync_durable_directory(idempotency_store.queue_fd)
                    spec_published = False
            except (OSError, UnicodeError, ValueError):
                # The original authority failure remains the user-facing
                # error. Retaining both artifacts is safer than risking a
                # durable spec whose exact workspace was removed.
                spec_published = True
        if not spec_published:
            # The exclusive mkdir above proves ownership of this reservation.
            # Roll back only when the public entry still names that exact
            # inode. If it was replaced, leave an orphan rather than deleting
            # another process's tree. A published spec is always retained as
            # crash-gap repair evidence, even if claim publication failed.
            if jobs_descriptor is not None and reserved_workspace is not None:
                with contextlib.suppress(OSError, ValueError):
                    _remove_tree_at(
                        jobs_descriptor,
                        jobid,
                        expected_root=reserved_workspace,
                    )
            else:
                try:
                    current = os.lstat(workspace)
                except FileNotFoundError:
                    current = None
                if (
                    current is not None
                    and reserved_workspace is not None
                    and _same_inode(current, reserved_workspace)
                ):
                    shutil.rmtree(workspace, ignore_errors=True)
        if workspace_descriptor is not None:
            with contextlib.suppress(BaseException):
                os.close(workspace_descriptor)
        if jobs_descriptor is not None:
            with contextlib.suppress(BaseException):
                os.close(jobs_descriptor)
        if claim_published:
            spec_published = True
    # Events are best-effort observability, not an acceptance journal. Once
    # the spec (and, for keyed submissions, the durable claim) is published,
    # no event-writer failure or local interrupt may hide the job receipt.
    try:
        events.append_event(
            workspace,
            events.EventKind.SUBMITTED,
            jobid,
            command=final_command,
            cpus=spec.cpus,
            scheduler_tasks=spec.scheduler_tasks,
            mem_mb=spec.mem_mb,
            wall_time_seconds=spec.wall_time_seconds,
            priority=spec.priority,
            recover_on_reboot=spec.recover_on_reboot,
            retry_max=spec.retry_max,
            job_name=spec.job_name,
            branch=spec.branch,
            program=spec.program,
            submitter=spec.submitter,
            workspace_source=source_hint,
        )
    except BaseException as exc:
        _non_masking_warning(
            "submitted-event writer failed after acceptance receipt: %s",
            type(exc).__name__,
        )
    _deliver_capacity_warnings(capacity_warnings, warning_sink)
    return jobid


def submit_local(
    *,
    host: str,
    input_file: str | None = None,
    directory: str | None = None,
    archive: str | None = None,
    command: list[str] | None = None,
    python: str | None = None,
    cpus: int = 1,
    scheduler_tasks: int | None = None,
    mem_mb: int | None = None,
    wall_time_seconds: int | None = None,
    priority: int = 0,
    auto_resume: bool = False,
    retry: int = 0,
    job_name: str | None = None,
    branch: str | None = None,
    program: str | None = None,
    program_runtime_pin: ProgramRuntimePin | None = None,
    tags: list[str] | None = None,
    not_before: str | None = None,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
    array_index: int | None = None,
    array_total: int | None = None,
    array_group_id: str | None = None,
    chain_index: int | None = None,
    chain_total: int | None = None,
    chain_group_id: str | None = None,
    rerun_until_file_exists: str | None = None,
    rerun_max: int = 10,
    clean_workdir_on_terminal: bool = False,
    refresh_before: str | None = None,
    vibeqc_preflight: bool = False,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_target: str | None = None,
    qvf_force: bool = False,
    idempotency_key: str | None = None,
    warning_sink: Callable[[str], None] | None = None,
) -> str:
    """Materialize a workspace, write a spec, return the jobid.

    ``scheduler_target`` (v1.0 cluster backend, design doc §17): when set, this
    spec is dispatched to that scheduler host over SSH+qsub by the daemon (a
    *driver* daemon), not run as a local process. Set by the CLI when forwarding
    a ``vq submit --host <cluster>`` to the driver; ``None`` for an ordinary
    local job.

    Exactly one of `input_file`, `directory`, `archive` must be set.

    - input_file: a single Python script, or a complete-calculation ``.qvf``.
      Python scripts use ``[python or sys.executable, basename]``. QVF input
      requires ``program`` plus a pinned runtime and resolves to the managed
      ``vibeqc run basename`` console entry point. Explicit `command` is
      rejected.
    - directory: contents are copied into the workspace; `command` required;
      ``python`` is rejected (the user encodes the interpreter in `command`).
    - archive: a .tar/.tar.gz/.tgz unpacked into the workspace with the
      tarfile data filter; `command` required; same `python` rule as directory.
    """
    if not is_local_host(host):
        # The CLI dispatcher should have routed remote hosts to submit_remote;
        # if one reaches this function it's a programming error. Phrased to
        # match the v0.1 message so existing CLI error-handling stays valid
        # until step 4 refactors the dispatch.
        raise NotImplementedError(
            f"remote submit to {host!r} not implemented in submit_local; "
            "use submit_remote for cross-machine submission"
        )

    if idempotency_key is not None:
        _validate_idempotency_key(idempotency_key)
        if any(
            value is not None
            for value in (
                array_index,
                array_total,
                array_group_id,
                chain_index,
                chain_total,
                chain_group_id,
            )
        ):
            raise ValueError(
                "idempotency keys initially support one single logical job; "
                "array and chain submissions are not supported"
            )

    sources = sum(1 for s in (input_file, directory, archive) if s)
    if sources != 1:
        raise ValueError("exactly one of input_file, directory, archive must be provided")

    # Checked before any workspace/jobid is minted so a rejected submit leaves
    # nothing behind.
    qvf_input = input_file is not None and _is_qvf_input(input_file)
    if qvf_input and python is not None:
        raise ValueError(
            "--python/--branch cannot be used with a QVF input; "
            "use --program NAME"
        )
    source_file: Path | None = None
    source_directory: Path | None = None
    source_archive: Path | None = None
    if input_file is not None:
        if command is not None:
            raise ValueError("single-file submit does not accept an explicit command")
        source_file = _regular_input_source(input_file)
        if qvf_input:
            final_command = _qvf_command(
                artifact_name=source_file.name,
                program=program,
                program_runtime_pin=program_runtime_pin,
                scheduler_target=scheduler_target,
                force=qvf_force,
            )
        else:
            interp = python or sys.executable
            if python is None:
                _reject_driver_local_interpreter(
                    scheduler_target,
                    interpreter=interp,
                )
            final_command = [interp, source_file.name]
        source_hint: str | None = str(source_file)
    elif directory is not None:
        if not command:
            raise ValueError("--dir submit requires an explicit command")
        source_directory = Path(directory).resolve()
        if not source_directory.is_dir():
            raise FileNotFoundError(f"directory not found: {source_directory}")
        final_command = _payload_command(command, python)
        validate_staged_python_entrypoint(
            command=final_command,
            source_directory=source_directory,
            interpreter_explicit=python is not None,
        )
        source_hint = str(source_directory)
    else:
        assert archive is not None
        if not command:
            raise ValueError("--compressed submit requires an explicit command")
        source_archive = Path(archive).resolve()
        if not source_archive.is_file():
            raise FileNotFoundError(f"archive not found: {source_archive}")
        final_command = _payload_command(command, python)
        validate_staged_python_entrypoint(
            command=final_command,
            source_archive=source_archive,
            interpreter_explicit=python is not None,
        )
        source_hint = str(source_archive)

    _validate_command_head(final_command)
    queue_dir = queue_dir or (
        paths.user_queue_dir(os.geteuid()) if multi_user else paths.queue_dir()
    )
    jobs_dir = jobs_dir or (paths.user_jobs_dir(os.geteuid()) if multi_user else paths.jobs_dir())
    jobid = new_jobid()

    # v0.6.51: validate --depends-on predecessors at submit time so
    # the operator hears about typos / nonexistent jobids immediately,
    # not as a silently-stuck PENDING job hours later. Validation
    # scope is intentionally the SUBMITTER's queue dir only — in
    # multi-user mode a user can't depend on another user's job
    # (the depender would otherwise need read access into a
    # different per-user state tree to even check). Cross-user
    # dependencies are a future feature if anyone asks.
    deps: list[str] = [validate_job_id(value) for value in (depends_on or [])]
    # Dedupe + preserve order (operator's intent: A first, then B).
    seen: set[str] = set()
    deps = [d for d in deps if not (d in seen or seen.add(d))]
    if jobid in deps:
        # Can't happen with the random-jobid path (jobid not yet
        # known to caller), but guard for completeness in case some
        # caller pre-supplies an id.
        raise ValueError(
            f"--depends-on cannot include the dependent's own jobid {jobid!r}"
        )
    # v0.7.8: same validation pass for --depends-on-any. Dedupe per
    # list; cross-list dedup is the operator's choice (an id in both
    # lists effectively degrades the afterany predicate to afterok
    # for that pred, which is harmless but redundant — we log no
    # warning, just let it ride).
    deps_any: list[str] = [
        validate_job_id(value) for value in (depends_on_any or [])
    ]
    seen_any: set[str] = set()
    deps_any = [d for d in deps_any if not (d in seen_any or seen_any.add(d))]
    if jobid in deps_any:
        raise ValueError(
            f"--depends-on-any cannot include the dependent's own "
            f"jobid {jobid!r}"
        )
    workspace = jobs_dir / jobid
    # Construct the exact durable model before creating queue/workspace state.
    # This catches resource, metadata, tag, retry, array/chain, and other model
    # validation failures while submission remains side-effect free.
    spec = JobSpec(
        id=jobid,
        command=final_command,
        cwd=str(workspace.resolve()),
        cpus=cpus,
        scheduler_tasks=scheduler_tasks,
        mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds,
        priority=priority,
        recover_on_reboot=auto_resume,
        retry_max=retry,
        job_name=job_name,
        branch=branch,
        program=program,
        program_runtime_pin=program_runtime_pin,
        tags=tags or [],
        not_before=not_before,
        depends_on=deps,
        depends_on_any=deps_any,
        array_index=array_index,
        array_total=array_total,
        array_group_id=array_group_id,
        chain_index=chain_index,
        chain_total=chain_total,
        chain_group_id=chain_group_id,
        rerun_until_file_exists=rerun_until_file_exists,
        rerun_max=rerun_max,
        clean_workdir_on_terminal=clean_workdir_on_terminal,
        scheduler_target=scheduler_target,
        refresh_before=refresh_before,
        submitter=(
            str(os.geteuid())
            if multi_user
            else f"{getpass.getuser()}@{socket.gethostname()}"
        ),
        workspace_source=source_hint,
        qvf_artifact_name=source_file.name if qvf_input else None,
    )
    payload_context = (
        _immutable_payload_snapshot(
            source_file=source_file,
            source_directory=source_directory,
            source_archive=source_archive,
            final_command=final_command,
            interpreter_explicit=python is not None,
        )
        if idempotency_key is not None
        else contextlib.nullcontext(
            _PayloadSnapshot(
                source_file,
                source_directory,
                source_archive,
            )
        )
    )
    with payload_context as snapshot:
        effective_file = snapshot.source_file
        effective_directory = snapshot.source_directory
        effective_archive = snapshot.source_archive
        binding: _IdempotencyBinding | None = None
        expected_payload: tuple[str, str] | None = None
        if idempotency_key is not None:
            payload_kind, digest = _payload_digest(
                source_file=effective_file,
                source_directory=effective_directory,
                source_archive=effective_archive,
            )
            expected_payload = (payload_kind, digest)
            intent_digest = _submission_intent_digest(
                spec,
                payload_kind=payload_kind,
                payload_digest=digest,
                vibeqc_preflight=vibeqc_preflight,
            )
            binding = _idempotency_binding(
                queue_dir,
                key=idempotency_key,
                intent_digest=intent_digest,
            )
            spec = JobSpec.model_validate(
                {
                    **spec.model_dump(mode="python"),
                    "idempotency_key_hash": binding.key_hash,
                    "submission_intent_digest": binding.intent_digest,
                    "submission_owner_hash": binding.owner_hash,
                }
            )
            replay = _replay_preexisting_idempotent_submission(
                queue_dir,
                binding,
            )
            if replay is not None:
                _deliver_idempotent_replay_capacity_warnings(
                    queue_dir=queue_dir,
                    jobid=replay,
                    multi_user=multi_user,
                    warning_sink=warning_sink,
                )
                return replay

        # These gates govern new authority acceptance only. A caller retrying
        # an already-accepted keyed intent receives its original receipt even
        # if a rollout drain activated or a predecessor was later cleaned up.
        drain_state = drain.read_drain_state()
        if (
            drain_state is not None
            and drain_state.enabled
            and drain_state.reject_submits
        ):
            detail = drain_state.reason or "queue paused for update"
            raise ValueError(
                "queue is paused for update and denying new submissions: "
                f"{detail}. Try again after `vq drain --release`."
            )
        for pred in deps:
            pred_path = queue_dir / f"{pred}.json"
            if not pred_path.exists():
                raise ValueError(
                    f"--depends-on {pred!r}: no such job in this user's "
                    f"queue ({queue_dir}). vq queue lists known jobids."
                )
        for pred in deps_any:
            pred_path = queue_dir / f"{pred}.json"
            if not pred_path.exists():
                raise ValueError(
                    f"--depends-on-any {pred!r}: no such job in this "
                    f"user's queue ({queue_dir}). vq queue lists known "
                    f"jobids."
                )

        # This advisory probe may consult RPC or the bounded fallback file.
        # Resolve it after accepted-key replay, but before creating any new
        # authority namespace, workspace, spec, or claim. It can therefore
        # never strand accepted work behind a missing client receipt.
        capacity_warnings = _impossible_capacity_warnings(
            cpus=spec.cpus,
            mem_mb=spec.mem_mb,
            jobid=spec.id,
            multi_user=multi_user,
            scheduler_target=spec.scheduler_target,
        )

        if binding is not None:
            with _idempotency_store_lock(
                queue_dir,
                binding,
                create_namespace=True,
                create_lock=True,
            ) as store:
                assert store is not None
                existing = _claim_or_repair_idempotent_submission(
                    queue_dir,
                    binding,
                    store,
                )
                if existing is not None:
                    _deliver_idempotent_replay_capacity_warnings(
                        queue_dir=queue_dir,
                        jobid=existing,
                        multi_user=multi_user,
                        warning_sink=warning_sink,
                    )
                    return existing
                return _commit_local_submission(
                    spec=spec,
                    queue_dir=queue_dir,
                    jobs_dir=jobs_dir,
                    source_file=effective_file,
                    source_directory=effective_directory,
                    source_archive=effective_archive,
                    final_command=final_command,
                    vibeqc_preflight=vibeqc_preflight,
                    source_hint=source_hint,
                    binding=binding,
                    idempotency_store=store,
                    expected_payload=expected_payload,
                    multi_user=multi_user,
                    warning_sink=warning_sink,
                    capacity_warnings=capacity_warnings,
                )
        return _commit_local_submission(
            spec=spec,
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            source_file=effective_file,
            source_directory=effective_directory,
            source_archive=effective_archive,
            final_command=final_command,
            vibeqc_preflight=vibeqc_preflight,
            source_hint=source_hint,
            binding=None,
            idempotency_store=None,
            expected_payload=None,
            multi_user=multi_user,
            warning_sink=warning_sink,
            capacity_warnings=capacity_warnings,
        )


def submit_local_array(
    *,
    array: int,
    host: str,
    input_file: str | None = None,
    directory: str | None = None,
    archive: str | None = None,
    command: list[str] | None = None,
    python: str | None = None,
    cpus: int = 1,
    scheduler_tasks: int | None = None,
    mem_mb: int | None = None,
    wall_time_seconds: int | None = None,
    priority: int = 0,
    auto_resume: bool = False,
    retry: int = 0,
    job_name: str | None = None,
    branch: str | None = None,
    program: str | None = None,
    program_runtime_pin: ProgramRuntimePin | None = None,
    tags: list[str] | None = None,
    not_before: str | None = None,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
    rerun_until_file_exists: str | None = None,
    rerun_max: int = 10,
    clean_workdir_on_terminal: bool = False,
    vibeqc_preflight: bool = False,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_target: str | None = None,
    warning_sink: Callable[[str], None] | None = None,
) -> list[str]:
    """v0.6.52: spawn ``array`` near-identical specs sharing a group id.

    The SLURM-array analogue. Each element gets:

    * Its own jobid (new_jobid) and workspace (jobs_dir/<jobid>/).
    * The same source content (input_file / directory / archive)
      copied into the workspace.
    * ``array_index`` = 0..array-1, ``array_total`` = array,
      ``array_group_id`` = a shared 8-hex-char tag.
    * All other fields (cpus, mem_mb, tags, priority, depends_on,
      ...) identical across elements.

    Returns the list of jobids in submission (=index) order.

    Workspace duplication: each element gets a full copy of the
    source. For a single small input file (the common case) this
    is trivial; for a large ``--dir`` / ``--compressed`` submit
    with array=100, the cost is N × source-size — operator's
    call. A future optimisation could stage once and ref-copy,
    but the single-workspace-per-job invariant is deep enough in
    the daemon (chown, archive, cleanup) that keeping it intact
    here is the right v0.6.52 trade-off.

    Same dispatch semantics as N independent submits: the daemon
    does NOT gang-schedule, has no notion of "array completion,"
    and applies per-user budgets / quotas to each element
    individually. The shared ``array_group_id`` is purely
    metadata for the operator (and for a future
    ``vq queue --array-group GID`` filter).

    Composes with --depends-on (all elements share the same
    predecessor list), --rerun-until (each element reruns independently
    until its own flag appears), --wait (the CLI waits on each element
    in sequence), --tag (every element gets the same tag set).
    ``--job-name`` is set verbatim on every element (it's decorative,
    not unique — see v0.5.34).
    """
    if array < 1:
        raise ValueError(f"--array must be >= 1, got {array}")
    group_id = new_array_group_id()
    jobids: list[str] = []
    # vibeqc-preflight only makes sense once per source — running
    # it N times executes the user's script N times before queue
    # entry, which is the opposite of what --array operators want.
    # Run it once and propagate the result via expected_outputs /
    # output_stem on each element. Implementation note: each per-
    # element submit_local call re-runs the preflight; for v0.6.52
    # we just disable it inside the loop. Operators who need
    # preflight on arrays can submit one element first to capture
    # the outputs, then submit the rest without preflight.
    if vibeqc_preflight:
        log.warning(
            "vibeqc_preflight is disabled inside submit_local_array "
            "(array=%d): the preflight executes the user's script "
            "and is not safe to repeat N times. Submit a single "
            "element first to capture preflight outputs.",
            array,
        )
    for idx in range(array):
        jobid = submit_local(
            host=host,
            input_file=input_file,
            directory=directory,
            archive=archive,
            command=command,
            python=python,
            cpus=cpus,
            scheduler_tasks=scheduler_tasks,
            mem_mb=mem_mb,
            wall_time_seconds=wall_time_seconds,
            priority=priority,
            auto_resume=auto_resume,
            retry=retry,
            job_name=job_name,
            branch=branch,
            program=program,
            program_runtime_pin=program_runtime_pin,
            tags=tags,
            not_before=not_before,
            depends_on=depends_on,
            depends_on_any=depends_on_any,
            array_index=idx,
            array_total=array,
            array_group_id=group_id,
            rerun_until_file_exists=rerun_until_file_exists,
            rerun_max=rerun_max,
            clean_workdir_on_terminal=clean_workdir_on_terminal,
            vibeqc_preflight=False,
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            multi_user=multi_user,
            scheduler_target=scheduler_target,
            warning_sink=warning_sink,
        )
        jobids.append(jobid)
    return jobids


def new_chain_group_id() -> str:
    """v0.8.7 *Hoare's Triple*: short opaque chain group id (8 hex
    chars from a UUID4). Mirrors :func:`new_array_group_id`."""
    return uuid.uuid4().hex[:8]


def submit_local_chain(
    *,
    chain: int,
    host: str,
    input_file: str | None = None,
    directory: str | None = None,
    archive: str | None = None,
    command: list[str] | None = None,
    python: str | None = None,
    cpus: int = 1,
    scheduler_tasks: int | None = None,
    mem_mb: int | None = None,
    wall_time_seconds: int | None = None,
    priority: int = 0,
    auto_resume: bool = False,
    retry: int = 0,
    job_name: str | None = None,
    branch: str | None = None,
    program: str | None = None,
    program_runtime_pin: ProgramRuntimePin | None = None,
    tags: list[str] | None = None,
    not_before: str | None = None,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
    rerun_until_file_exists: str | None = None,
    rerun_max: int = 10,
    clean_workdir_on_terminal: bool = False,
    vibeqc_preflight: bool = False,
    queue_dir: Path | None = None,
    jobs_dir: Path | None = None,
    multi_user: bool = False,
    scheduler_target: str | None = None,
    warning_sink: Callable[[str], None] | None = None,
) -> list[str]:
    """v0.8.7 *Hoare's Triple*: spawn ``chain`` near-identical specs
    linked by depends_on so they run strictly in sequence.

    The NEB image-by-image + DFT+U self-consistency analogue. Each
    element gets:

    * Its own jobid and workspace (jobs_dir/<jobid>/).
    * The same source content copied into the workspace.
    * ``chain_index`` = 0..chain-1, ``chain_total`` = chain,
      ``chain_group_id`` = a shared 8-hex-char tag.
    * Element k (k>=1) has ``depends_on=[jobids[k-1]]`` merged with
      any user-supplied ``depends_on`` list, so element k starts
      only after element k-1 has succeeded. Element 0 has only the
      user-supplied predecessors.

    Returns the list of jobids in submission (=index) order.

    **Cascade-fail semantic:** the underlying ``depends_on`` machinery
    already implements "if predecessor fails, dependent is marked
    cascade-failed without dispatching." So a failed chain[k] kills
    chain[k+1..]. Operators who want "best-effort" sequential
    execution (run k+1 regardless of k's outcome) want
    ``--depends-on-any`` instead, which the array primitive supports.

    Distinct from ``--array``: array siblings are independent +
    parallel; chain elements are strictly sequential. The CLI
    rejects ``--chain`` combined with ``--array``.

    Use case:

    * **NEB image-by-image:** chain=5 for a 5-image NEB sweep
      where each image initializes from the previous's relaxed
      geometry. Inside the script: read
      ``$VQ_WORKDIR/../<prev jobid>/...`` or use the
      ``VQ_CHAIN_INDEX`` env var to branch on "first image" vs
      "iterate from prev".
    * **DFT+U self-consistency:** chain=10 with the script
      checking ``VQ_CHAIN_INDEX`` to decide when to stop refining
      U (or use the v0.8.8 ``--rerun-until`` companion for
      convergence-flag termination).

    Workspace duplication mirrors ``submit_local_array``: each
    element gets a full source copy. Operators with large
    ``--dir`` / ``--compressed`` submits pay N × source-size.

    ``--rerun-until`` composes with chain semantics: each chain
    element reruns independently until its own convergence flag exists,
    then the next chain element becomes eligible through ``depends_on``.
    """
    if chain < 1:
        raise ValueError(f"--chain must be >= 1, got {chain}")
    group_id = new_chain_group_id()
    jobids: list[str] = []
    if vibeqc_preflight:
        log.warning(
            "vibeqc_preflight is disabled inside submit_local_chain "
            "(chain=%d): the preflight executes the user's script "
            "and is not safe to repeat N times. Submit a single "
            "element first to capture preflight outputs.",
            chain,
        )
    user_deps = list(depends_on or [])
    for idx in range(chain):
        # v0.8.7: link element k to element k-1 by adding the prev
        # jobid to the depends_on list. Element 0 carries only the
        # user-supplied predecessors.
        if idx == 0:
            effective_depends_on: list[str] = user_deps
        else:
            effective_depends_on = user_deps + [jobids[idx - 1]]
        jobid = submit_local(
            host=host,
            input_file=input_file,
            directory=directory,
            archive=archive,
            command=command,
            python=python,
            cpus=cpus,
            scheduler_tasks=scheduler_tasks,
            mem_mb=mem_mb,
            wall_time_seconds=wall_time_seconds,
            priority=priority,
            auto_resume=auto_resume,
            retry=retry,
            job_name=job_name,
            branch=branch,
            program=program,
            program_runtime_pin=program_runtime_pin,
            tags=tags,
            not_before=not_before,
            depends_on=effective_depends_on,
            depends_on_any=depends_on_any,
            chain_index=idx,
            chain_total=chain,
            chain_group_id=group_id,
            rerun_until_file_exists=rerun_until_file_exists,
            rerun_max=rerun_max,
            clean_workdir_on_terminal=clean_workdir_on_terminal,
            vibeqc_preflight=False,
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            multi_user=multi_user,
            scheduler_target=scheduler_target,
            warning_sink=warning_sink,
        )
        jobids.append(jobid)
    return jobids


def _build_remote_submit_argv(
    *,
    remote_tar: str,
    remote_input_file: str | None,
    remote_command: Sequence[str] | None,
    cpus: int,
    scheduler_tasks: int | None,
    mem_mb: int | None,
    wall_time_seconds: int | None,
    priority: int,
    auto_resume: bool,
    retry: int,
    job_name: str | None,
    branch: str | None,
    program: str | None,
    expected_sha: str | None,
    tags: Sequence[str] | None,
    not_before: str | None,
    depends_on: Sequence[str] | None,
    depends_on_any: Sequence[str] | None,
    clean_workdir_on_terminal: bool,
    array: int,
    chain: int,
    rerun_until_file_exists: str | None,
    rerun_max: int,
    refresh_before: str | None,
    scheduler_target: str | None,
    qvf_force: bool,
    idempotency_key: str | None = None,
) -> list[str]:
    """Serialize resolved submit inputs for the remote ``vq`` CLI.

    Upload and payload resolution stay with :func:`submit_remote`; this helper
    owns only the mixed-version wire token order. Quoting remains the
    transport layer's responsibility.
    """
    remote_argv: list[str] = ["submit", "localhost"]
    if remote_input_file is None:
        remote_argv.extend(["-c", remote_tar])
    remote_argv.extend(["--cpus", str(cpus)])
    if scheduler_tasks is not None:
        remote_argv.extend(["--scheduler-tasks", str(scheduler_tasks)])
    if mem_mb is not None:
        remote_argv.extend(["--mem-mb", str(mem_mb)])
    if wall_time_seconds is not None:
        remote_argv.extend(["--wall-time-seconds", str(wall_time_seconds)])
    if priority != 0:
        remote_argv.extend(["--priority", str(priority)])
    if auto_resume:
        remote_argv.append("--auto-resume")
    if retry != 0:
        remote_argv.extend(["--retry", str(retry)])
    if job_name is not None:
        remote_argv.extend(["--job-name", job_name])
    # The interpreter has already been resolved locally. Forward only the
    # branch's stored identity so the receiver does not resolve it again.
    if branch is not None:
        remote_argv.extend(["--branch-name", branch])
    if program is not None:
        remote_argv.extend(["--program", program])
    if expected_sha is not None:
        remote_argv.extend(["--expected-sha", expected_sha])
    for tag in tags or ():
        remote_argv.extend(["--tag", tag])
    if not_before is not None:
        remote_argv.extend(["--at", not_before])
    for dep in depends_on or ():
        remote_argv.extend(["--depends-on", dep])
    for dep in depends_on_any or ():
        remote_argv.extend(["--depends-on-any", dep])
    if clean_workdir_on_terminal:
        remote_argv.append("--clean-tmp")
    if array > 1:
        remote_argv.extend(["--array", str(array)])
    if chain > 1:
        remote_argv.extend(["--chain", str(chain)])
    if rerun_until_file_exists is not None:
        remote_argv.extend(["--rerun-until", rerun_until_file_exists])
        if rerun_max != 10:
            remote_argv.extend(["--rerun-max", str(rerun_max)])
    if refresh_before is not None:
        remote_argv.extend(["--refresh", refresh_before])
    if scheduler_target is not None:
        remote_argv.extend(["--scheduler-target", scheduler_target])
    if idempotency_key is not None:
        remote_argv.extend(["--idempotency-key", idempotency_key])
    if remote_input_file is not None:
        if qvf_force:
            remote_argv.append("--qvf-force")
        remote_argv.append(remote_input_file)
    else:
        assert remote_command is not None
        remote_argv.extend(["--", *remote_command])
    return remote_argv


def _validate_remote_submit_fields(
    *,
    command: Sequence[str] | None,
    cpus: int,
    scheduler_tasks: int | None,
    mem_mb: int | None,
    wall_time_seconds: int | None,
    priority: int,
    auto_resume: bool,
    retry: int,
    job_name: str | None,
    branch: str | None,
    program: str | None,
    expected_sha: str | None,
    tags: Sequence[str] | None,
    not_before: str | None,
    depends_on: Sequence[str] | None,
    depends_on_any: Sequence[str] | None,
    clean_workdir_on_terminal: bool,
    array: int,
    chain: int,
    rerun_until_file_exists: str | None,
    rerun_max: int,
    refresh_before: str | None,
    scheduler_target: str | None,
    idempotency_key: str | None = None,
) -> None:
    """Validate the remote wire intent before temp, upload, or SSH mutation."""
    validated_depends_on = [
        validate_job_id(value) for value in (depends_on or ())
    ]
    validated_depends_on_any = [
        validate_job_id(value) for value in (depends_on_any or ())
    ]
    if expected_sha is not None:
        normalized_sha = expected_sha.strip()
        if (
            len(normalized_sha) < EXPECTED_SHA_MIN_PREFIX_LEN
            or not all(
                character in "0123456789abcdefABCDEF"
                for character in normalized_sha
            )
        ):
            raise ValueError(
                "--expected-sha must be a hexadecimal git SHA of at least "
                f"{EXPECTED_SHA_MIN_PREFIX_LEN} characters"
            )
        if program is None:
            raise ValueError("--expected-sha requires --program NAME")
    if idempotency_key is not None:
        _validate_idempotency_key(idempotency_key)
        if array > 1 or chain > 1:
            raise ValueError(
                "idempotency keys initially support one single logical job; "
                "array and chain submissions are not supported"
            )
    JobSpec(
        id="000000000000",
        command=list(command or ["vq-qvf-receiver-validation"]),
        cwd="/vq-submit-validation",
        cpus=cpus,
        scheduler_tasks=scheduler_tasks,
        mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds,
        priority=priority,
        recover_on_reboot=auto_resume,
        retry_max=retry,
        job_name=job_name,
        branch=branch,
        program=program,
        tags=list(tags or ()),
        not_before=not_before,
        depends_on=validated_depends_on,
        depends_on_any=validated_depends_on_any,
        array_index=0 if array > 1 else None,
        array_total=array if array > 1 else None,
        array_group_id="validation" if array > 1 else None,
        chain_index=0 if chain > 1 else None,
        chain_total=chain if chain > 1 else None,
        chain_group_id="validation" if chain > 1 else None,
        rerun_until_file_exists=rerun_until_file_exists,
        rerun_max=rerun_max,
        clean_workdir_on_terminal=clean_workdir_on_terminal,
        refresh_before=refresh_before,
        scheduler_target=scheduler_target,
    )


def submit_remote(
    *,
    host: str,
    host_cfg: HostConfig,
    input_file: str | None = None,
    directory: str | None = None,
    archive: str | None = None,
    command: list[str] | None = None,
    python: str | None = None,
    cpus: int = 1,
    scheduler_tasks: int | None = None,
    mem_mb: int | None = None,
    wall_time_seconds: int | None = None,
    priority: int = 0,
    auto_resume: bool = False,
    retry: int = 0,
    job_name: str | None = None,
    branch: str | None = None,
    program: str | None = None,
    expected_sha: str | None = None,
    tags: list[str] | None = None,
    not_before: str | None = None,
    depends_on: list[str] | None = None,
    depends_on_any: list[str] | None = None,
    clean_workdir_on_terminal: bool = False,
    array: int = 1,
    chain: int = 1,
    rerun_until_file_exists: str | None = None,
    rerun_max: int = 10,
    refresh_before: str | None = None,
    scheduler_target: str | None = None,
    qvf_force: bool = False,
    idempotency_key: str | None = None,
    warning_sink: Callable[[str], None] | None = None,
) -> list[str]:
    """Submit a job to a remote host via SSH.

    Builds a tarball locally (depending on input_file / directory / archive),
    scp's it to a unique home-relative staging path on the remote (on the shared
    home filesystem, not node-local ``/tmp`` — see
    :func:`~vq.transport.remote_temp_tar_path` for the multi-login-node
    rationale), then invokes
    ``<host_cfg.remote_vq> submit localhost -c <tarball> --cpus N -- <cmd>``
    over ssh and returns the jobid(s) printed by the remote vq.

    For single-file submits the remote command is constructed locally as
    ``[interp, basename]`` where ``interp`` is the first defined of:
    ``python`` argument, ``host_cfg.remote_python``, then literal ``"python"``.
    Note ``interp`` is a path *on the remote* -- the local CLI's --python
    flag passes through unchanged.

    v0.7.11 *Stroustrup's Stencil*: ``array > 1`` forwards
    ``--array N`` on the remote argv, letting the remote vq spawn N
    specs from a single source upload (vs. the v0.6.52 pattern of N
    SSH roundtrips with N source uploads). The remote's submit verb
    prints one jobid per line; we split + validate each. Returns a
    list of length ``array`` (length 1 for the default no-array
    case). The remote elements share an ``array_group_id`` (the
    remote vq's ``submit_local_array`` mints it), so the remote
    array is finally indistinguishable from a local array except for
    where it was submitted from.

    v0.8.10 *Tarjan's Bridge*: same single-roundtrip pattern for
    ``chain > 1`` (forwards ``--chain N``) and the v0.8.8 rerun
    fields (``--rerun-until PATH``, ``--rerun-max N``). Closes the
    v0.8.7/v0.8.8 local-only gap so operators can run NEB / DFT+U
    workflows on remote hosts: ``vq submit workstation --chain 5
    --rerun-until '$VQ_WORKDIR/CONVERGED' neb.py``. ``chain`` and
    ``array`` are mutually exclusive client-side; rerun fields
    compose with either.

    Cleanup: local and remote staging are removed only after the receiver's
    outcome is proven. A timeout, SSH-255/signal loss, or success without the
    complete expected job-id receipt retains both copies for reconciliation;
    automatically replaying that submit could create duplicate jobs.
    """
    if array < 1:
        raise ValueError(f"array must be >= 1, got {array}")
    if chain < 1:
        raise ValueError(f"chain must be >= 1, got {chain}")
    if array > 1 and chain > 1:
        raise ValueError(
            "array and chain are mutually exclusive; pick one",
        )
    expected_receipts = max(array, chain)
    if expected_receipts * 13 > REMOTE_SUBMIT_STDOUT_MAX_BYTES:
        raise ValueError(
            "remote array/chain receipt would exceed the bounded submit "
            f"capture ({REMOTE_SUBMIT_STDOUT_MAX_BYTES} bytes)"
        )
    sources = sum(1 for s in (input_file, directory, archive) if s)
    if sources != 1:
        raise ValueError("exactly one of input_file, directory, archive must be provided")

    cleanup_local: Path | None = None
    remote_command: list[str] | None
    remote_input_file: str | None = None
    temp_source_file: Path | None = None
    temp_source_directory: Path | None = None

    if input_file is not None:
        if command is not None:
            raise ValueError("single-file submit does not accept an explicit command")
        src = _regular_input_source(input_file)
        if _is_qvf_input(src):
            if python is not None:
                raise ValueError(
                    "--python/--branch cannot be used with a QVF input; "
                    "use --program NAME"
                )
            # Upload the container itself and let the receiving vq submit it as
            # a first-class single-file payload.  That boundary owns the
            # program registry and scheduler config, so it resolves the
            # managed executable without leaking laptop paths into the spec.
            local_tar = src
            remote_command = None
        else:
            interp = python or host_cfg.remote_python or "python"
            # When ``scheduler_target`` is set, ``host_cfg`` is the *driver*
            # daemon's config, not the cluster's: its ``remote_python`` is a
            # path on the driver. Reject it before uploading anything rather
            # than qsub a command the compute node cannot execute.
            if python is None:
                _reject_driver_local_interpreter(
                    scheduler_target, interpreter=interp
                )
            remote_command = [interp, src.name]
            local_tar = src
            temp_source_file = src
    elif directory is not None:
        if not command:
            raise ValueError("--dir submit requires an explicit command")
        src_dir = Path(directory).resolve()
        if not src_dir.is_dir():
            raise FileNotFoundError(f"directory not found: {src_dir}")
        remote_command = _payload_command(command, python)
        validate_staged_python_entrypoint(
            command=remote_command,
            source_directory=src_dir,
            interpreter_explicit=python is not None,
        )
        local_tar = src_dir
        temp_source_directory = src_dir
    else:
        assert archive is not None
        if not command:
            raise ValueError("--compressed submit requires an explicit command")
        local_tar = Path(archive).resolve()
        if not local_tar.is_file():
            raise FileNotFoundError(f"archive not found: {local_tar}")
        # Don't delete the user's tarball -- they own it.
        remote_command = _payload_command(command, python)
        validate_staged_python_entrypoint(
            command=remote_command,
            source_archive=local_tar,
            interpreter_explicit=python is not None,
        )

    _validate_remote_submit_fields(
        command=remote_command,
        cpus=cpus,
        scheduler_tasks=scheduler_tasks,
        mem_mb=mem_mb,
        wall_time_seconds=wall_time_seconds,
        priority=priority,
        auto_resume=auto_resume,
        retry=retry,
        job_name=job_name,
        branch=branch,
        program=program,
        expected_sha=expected_sha,
        tags=tags,
        not_before=not_before,
        depends_on=depends_on,
        depends_on_any=depends_on_any,
        clean_workdir_on_terminal=clean_workdir_on_terminal,
        array=array,
        chain=chain,
        rerun_until_file_exists=rerun_until_file_exists,
        rerun_max=rerun_max,
        refresh_before=refresh_before,
        scheduler_target=scheduler_target,
        idempotency_key=idempotency_key,
    )
    # Allocate the remote name before creating any owned local staging file.
    # Although normally pure, a local entropy/path failure must not leak a
    # temp tar before the transport transaction has even begun.
    remote_tar = transport.remote_temp_tar_path()
    remote_stage_dir: str | None = None
    if temp_source_file is not None or temp_source_directory is not None:
        local_tar = _make_temp_tar()
        cleanup_local = local_tar
        try:
            with tarfile.open(local_tar, "w") as tf:
                if temp_source_file is not None:
                    tf.add(temp_source_file, arcname=temp_source_file.name)
                else:
                    assert temp_source_directory is not None
                    for child in temp_source_directory.iterdir():
                        tf.add(child, arcname=child.name)
        except BaseException:
            cleanup_local.unlink(missing_ok=True)
            raise

    outcome_unknown = False
    try:
        if input_file is not None and _is_qvf_input(input_file):
            # Preserve the user's basename across the SSH staging boundary.
            # The receiving vq uses that basename as both the workspace
            # artifact name and the lifecycle/fetch identity. Uploading the
            # file as ``.vq-upload-<token>.qvf`` made a successful chemistry
            # run impossible to retrieve as ``fetch --name job.qvf``.
            remote_stage_dir = f"{remote_tar}.d"
            remote_input_file = f"{remote_stage_dir}/{src.name}"
            transport.run_remote_shell(
                host_cfg,
                "mkdir",
                "-p",
                remote_stage_dir,
                owned_process_group=True,
                max_stdout_bytes=REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES,
                max_stderr_bytes=REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES,
            )
            remote_tar = remote_input_file
        transport.upload_file(host_cfg, local_tar, remote_tar)
        remote_argv = _build_remote_submit_argv(
            remote_tar=remote_tar,
            remote_input_file=remote_input_file,
            remote_command=remote_command,
            cpus=cpus,
            scheduler_tasks=scheduler_tasks,
            mem_mb=mem_mb,
            wall_time_seconds=wall_time_seconds,
            priority=priority,
            auto_resume=auto_resume,
            retry=retry,
            job_name=job_name,
            branch=branch,
            program=program,
            expected_sha=expected_sha,
            tags=tags,
            not_before=not_before,
            depends_on=depends_on,
            depends_on_any=depends_on_any,
            clean_workdir_on_terminal=clean_workdir_on_terminal,
            array=array,
            chain=chain,
            rerun_until_file_exists=rerun_until_file_exists,
            rerun_max=rerun_max,
            refresh_before=refresh_before,
            scheduler_target=scheduler_target,
            qvf_force=qvf_force,
            idempotency_key=idempotency_key,
        )
        proc = transport.run_remote_vq(
            host_cfg,
            *remote_argv,
            owned_process_group=True,
            max_stdout_bytes=REMOTE_SUBMIT_STDOUT_MAX_BYTES,
            max_stderr_bytes=REMOTE_SUBMIT_STDERR_MAX_BYTES,
        )
        # v0.7.11: the remote's submit verb prints one jobid per
        # line — true for both the v0.6.52 ``submit_local_array``
        # path AND the single-element path (which still prints
        # one line). So splitting on newlines works uniformly.
        jobids = [jid for jid in proc.stdout.strip().splitlines() if jid]
        for jid in jobids:
            if len(jid) != 12 or not all(
                c in "0123456789abcdef" for c in jid
            ):
                raise transport.RemoteOutcomeUnknown(
                    "unexpected output from remote vq submit: a valid "
                    "12-hex job-id receipt was not observed; acceptance "
                    "cannot be determined and staging was retained"
                )
        if len(set(jobids)) != len(jobids):
            raise transport.RemoteOutcomeUnknown(
                "remote vq submit returned duplicate job-id receipts; "
                "acceptance cannot be determined and staging was retained"
            )
        # v0.8.10: chain produces N jobids same as array. Pick whichever the
        # caller asked for; they are mutually exclusive.
        expected = max(array, chain)
        if len(jobids) != expected:
            kind = "--chain" if chain > 1 else "--array"
            raise transport.RemoteOutcomeUnknown(
                f"remote vq submit {kind} {expected} returned "
                f"{len(jobids)} jobids (expected {expected}); "
                "acceptance cannot be determined and staging was retained"
            )
        if warning_sink is not None:
            try:
                for line in proc.stderr.splitlines():
                    message = _remote_impossible_capacity_warning(
                        line,
                        jobids=jobids,
                    )
                    if message is not None:
                        try:
                            warning_sink(message)
                        except BaseException as exc:
                            _non_masking_warning(
                                "remote submit warning consumer failed after "
                                "acceptance receipt: %s",
                                type(exc).__name__,
                            )
            except BaseException as exc:
                _non_masking_warning(
                    "remote submit warning parser failed after acceptance "
                    "receipt: %s",
                    type(exc).__name__,
                )
        return jobids
    except transport.RemoteOutcomeUnknown:
        outcome_unknown = True
        raise
    except (KeyboardInterrupt, SystemExit, GeneratorExit):
        # The local observer disappeared while a mutating remote submit may
        # have crossed the acceptance boundary. Preserve the interrupt type,
        # but retain both staging copies so a human can reconcile instead of
        # turning Ctrl-C into an implicit destructive acknowledgement.
        outcome_unknown = True
        raise
    finally:
        if not outcome_unknown:
            # Proven acceptance/rejection means the receiver either extracted
            # the payload or rejected a transfer/submit. Remove any partial or
            # complete remote staging from the whole mkdir/upload/submit scope.
            try:
                transport.run_remote_shell(
                    host_cfg,
                    "rm",
                    "-f",
                    remote_tar,
                    check=False,
                    owned_process_group=True,
                    max_stdout_bytes=REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES,
                    max_stderr_bytes=REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES,
                )
                if remote_stage_dir is not None:
                    transport.run_remote_shell(
                        host_cfg,
                        "rmdir",
                        remote_stage_dir,
                        check=False,
                        owned_process_group=True,
                        max_stdout_bytes=REMOTE_HOUSEKEEPING_STDOUT_MAX_BYTES,
                        max_stderr_bytes=REMOTE_HOUSEKEEPING_STDERR_MAX_BYTES,
                    )
            except BaseException as exc:
                _non_masking_warning(
                    "remote staging cleanup failed after a proven submit "
                    "outcome for %s: %s",
                    remote_tar,
                    type(exc).__name__,
                )
        if cleanup_local is not None and not outcome_unknown:
            try:
                cleanup_local.unlink(missing_ok=True)
            except BaseException as exc:
                # Housekeeping follows a proven accept/reject result. Never
                # replace that authoritative outcome with a local cleanup
                # error; retain the path and log only the exception type.
                _non_masking_warning(
                    "local submit staging cleanup failed after proven "
                    "outcome: %s",
                    type(exc).__name__,
                )


def _make_temp_tar() -> Path:
    """Allocate a uniquely-named tempfile path with .tar suffix and return it."""
    fd, name = tempfile.mkstemp(suffix=".tar", prefix="vq-")
    os.close(fd)
    return Path(name)
