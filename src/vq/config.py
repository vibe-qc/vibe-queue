"""Configuration: optional ~/.config/vq/config.toml.

Schema (every field is optional):

    default_host = "compute"

    [hosts.compute]
    ssh           = "compute"             # ssh target (alias or user@host)
    remote_vq     = "vq"                  # command on remote PATH
    remote_python = "/home/USER/vibeqc-dev/.venv/bin/python"

    # v0.5.6: named-branch routing. Lets `vq submit --branch NAME` pick
    # an interpreter without the chat knowing the absolute path.
    [hosts.compute.branches]
    main    = "/home/USER/vibeqc-dev/.venv/bin/python"
    release = "/home/USER/vibeqc-release/.venv/bin/python"

    [hosts.compute.branch_aliases]
    dev         = "main"
    development = "main"
    latest      = "release"

    # v0.5.18: program registry. What `vq programs` lists, and the
    # source of truth for the future v0.6.0 `vq admin update <name>`
    # CLI. Three kinds:
    #
    # binary: an executable file on disk (CRYSTAL, ORCA, Psi4, ...).
    #         Availability = exists + executable bit.
    [programs.crystal]
    kind = "binary"
    binary = "/home/USER/bin/crystal"
    description = "CRYSTAL14 serial SCF"
    #
    # venv: a python interpreter + a git checkout that vq can refresh
    #       with `git pull && bash <update_script>`.
    [programs.vibeqc-dev]
    kind = "venv"
    python = "/home/USER/vibeqc-dev/.venv/bin/python"
    git_dir = "/home/USER/vibeqc-dev"
    branch = "main"
    update_script = "scripts/update-dev.sh"  # relative to git_dir
    description = "vibe-qc development (main branch)"
    #
    # import: a python module that should be importable from an interpreter.
    [programs.pyscf]
    kind = "import"
    python = "/home/USER/vibeqc-dev/.venv/bin/python"
    import_check = "pyscf"
    description = "PySCF 2.13"

If the file is absent, ``load_config()`` returns an empty Config. The CLI
falls back to "localhost" with no extra metadata, preserving v0.1 behavior.
"""

from __future__ import annotations

import math
import os
import re
import sys
import tomllib
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)

import vq
from vq import _program_probe

shlex = _program_probe.shlex
subprocess = _program_probe.subprocess
_first_nonempty_line = _program_probe._first_nonempty_line
_is_orca_binary = _program_probe._is_orca_binary
_orca_mpi_availability = _program_probe._orca_mpi_availability
_orca_mpi_loader_failure = _program_probe._orca_mpi_loader_failure
_is_crystal_serial_binary = _program_probe._is_crystal_serial_binary
_crystal_serial_availability = _program_probe._crystal_serial_availability
_IMPORT_VERSION_PREFIX = _program_probe._IMPORT_VERSION_PREFIX
import_probe_code = _program_probe.import_probe_code
_split_import_version = _program_probe._split_import_version
run_import_identity_probe = _program_probe.run_import_identity_probe
run_import_runtime_identity_probe = (
    _program_probe.run_import_runtime_identity_probe
)
run_import_probe = _program_probe.run_import_probe
vibeqc_native_source_mtimes = _program_probe.vibeqc_native_source_mtimes
VIBEQC_UNSUPPORTED_CORE_ERROR = _program_probe.VIBEQC_UNSUPPORTED_CORE_ERROR
_query_git = _program_probe._query_git
_git_has_changes = _program_probe._git_has_changes
_last_nonempty_line = _program_probe._last_nonempty_line
_timeout_stream_text = _program_probe._timeout_stream_text

ENV_CONFIG_DIR = "VQ_CONFIG_DIR"
_SCHEDULER_PROGRAM_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,50}$")


class ConfigError(RuntimeError):
    """Raised for any user-facing configuration problem (parse error,
    missing host, missing default_host)."""


VIBEQC_REPO_SLUG = "mpei/vibe-qc"
"""Slug under which :attr:`Config.pin_source_repos` names the vibe-qc
checkout, and the modern spelling of the deprecated
:attr:`Config.scheduler_runtime_source_repo`."""

KNOWN_PROGRAM_EXTRAS = ("web", "test", "dev")
"""Optional dependency groups vq publishes, as pip spells them.

Mirrors ``[project.optional-dependencies]`` in ``pyproject.toml``, which is
not readable from an installed wheel. ``tests/test_program_extras.py`` keeps
the two in step.
"""

MIN_VERSION_KEY = "min_vq_version"
"""Top-level key by which a config declares the oldest vq allowed to load it.

See :func:`_enforce_min_vq_version` for why this exists and when to set it.
"""

_RELEASE_VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)")
_EXACT_RELEASE_VERSION = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")


def _release_tuple(value: str) -> tuple[int, int, int] | None:
    """Parse the *running* vq version's release part, or None.

    Tolerant of a trailing suffix, because the running version legitimately
    carries one on a working checkout (``0.26.0.dev3+g28620dc``) and refusing
    to compare would be worse than comparing the release part: an operator on
    a dev build still wants the floor enforced against the release it is
    built from.
    """
    return _parse_release(value, _RELEASE_VERSION)


def _declared_release_tuple(value: str) -> tuple[int, int, int] | None:
    """Parse a *declared* floor, or None. Exact ``X.Y.Z`` only.

    Deliberately stricter than :func:`_release_tuple`. The running version is
    whatever this build happens to be; the floor is a statement somebody
    wrote, and the tolerant parser reads ``"0.26.0.1"`` and ``"0.26.0-rc1"``
    as ``0.26.0`` -- quietly enforcing something other than what was written,
    on the one key whose entire job is to be precise about a version.
    """
    return _parse_release(value, _EXACT_RELEASE_VERSION)


def _parse_release(
    value: str, pattern: re.Pattern[str],
) -> tuple[int, int, int] | None:
    match = pattern.match(value.strip())
    if match is None:
        return None
    major, minor, patch = match.groups()
    return (int(major), int(minor), int(patch))


def _enforce_min_vq_version(data: dict, path: Path) -> None:
    """Refuse a config that declares a vq floor newer than this vq.

    The complement of :func:`_split_unknown_top_level`. Tolerating unknown
    keys is right when they are additive, and wrong when the new key is what
    makes the config *correct* -- a policy-bearing key in the system config, a
    pin repository the loader must consult. This key is how the author of such
    a change says so, and it fails with a sentence an operator can act on
    rather than a pydantic dump.

    Checked against the raw mapping before validation, so the refusal survives
    every future schema change: a vq that knows only this key can still read
    the floor out of a config written years later.
    """
    declared = data.get(MIN_VERSION_KEY)
    if declared is None:
        return
    required = (
        _declared_release_tuple(declared) if isinstance(declared, str) else None
    )
    if required is None:
        raise ConfigError(
            f"invalid config in {path}: {MIN_VERSION_KEY} must be a release "
            f'version string like "0.26.0" (got {declared!r})'
        )
    running = _release_tuple(vq.__version__)
    if running is not None and running < required:
        raise ConfigError(
            f"{path} requires vq >= {declared}; this is vq {vq.__version__}. "
            f"Upgrade vq on this host, or -- if this host does not need the "
            f"newer keys -- lower {MIN_VERSION_KEY} in the config."
        )


_UNKNOWN_TOP_LEVEL_WARNED: set[tuple[str, tuple[str, ...]]] = set()
"""(path, keys) pairs already reported, so one process warns once per config."""


def _split_unknown_top_level(data: dict, path: Path) -> dict:
    """Drop unrecognized *top-level* keys, warning once, and return the rest.

    An older vq must degrade, not disappear. Adding ``pin_source_repos`` made
    vq 0.25.7 reject entire configs with an ``extra_forbidden`` dump, which on
    a fleet takes the host out completely rather than costing it one feature it
    could not have used anyway. Additive top-level keys are therefore ignored
    from here on, and :data:`MIN_VERSION_KEY` is how an addition that must
    *not* be ignored says so.

    Deliberately top-level only. Nested sections keep ``extra="forbid"``
    because that is where the typos are: ``[notifications] webhook_urls`` has
    exactly one plausible meaning and silently disabling notifications is a
    worse outcome than a load error. The same reasoning applies to constructing
    :class:`Config` in code, which also stays strict -- this tolerance is a
    property of reading a *file written by another vq*, not of the model.
    """
    unknown = tuple(sorted(key for key in data if key not in Config.model_fields))
    if not unknown:
        return data
    token = (str(path), unknown)
    if token not in _UNKNOWN_TOP_LEVEL_WARNED:
        _UNKNOWN_TOP_LEVEL_WARNED.add(token)
        _warn_operator(
            f"{path}: ignoring unrecognized top-level "
            f"{'keys' if len(unknown) > 1 else 'key'} {', '.join(unknown)} "
            f"-- this is vq {vq.__version__}, and a newer one may define "
            f"{'them' if len(unknown) > 1 else 'it'}. Check the spelling if "
            f"that is not what you meant."
        )
    return {key: value for key, value in data.items() if key in Config.model_fields}


_DEPRECATED_SOURCE_REPO_WARNED = False


def _warn_deprecated_source_repo() -> None:
    """Say once that the global vibe-qc checkout setting has a modern spelling.

    Once per process, not once per load: policy is re-read per queue row
    (#547), and a warning that repeats thousands of times is a warning nobody
    reads.
    """
    global _DEPRECATED_SOURCE_REPO_WARNED
    if _DEPRECATED_SOURCE_REPO_WARNED:
        return
    _DEPRECATED_SOURCE_REPO_WARNED = True
    _warn_operator(
        "scheduler_runtime_source_repo is deprecated; it is the "
        f'[pin_source_repos] "{VIBEQC_REPO_SLUG}" entry under an older name. '
        "Move it there and delete this key."
    )


def _warn_operator(message: str) -> None:
    """One operator-facing advisory on stderr, in the ``vq submit`` format.

    A library module writing to a terminal is normally this codebase's mistake
    to avoid (see :class:`vq.output.Channel`). It is the right call here for
    one reason: the alternative to a warning is silence, and silence is how a
    dropped config key becomes a host that looks configured and is not. stderr
    keeps ``--json`` stdout clean, and the write is guarded because a detached
    daemon may have closed it.
    """
    try:
        sys.stderr.write(f"vq: warning: {message}\n")
        sys.stderr.flush()
    except (OSError, ValueError):
        pass


class RecoveryConfig(BaseModel):
    """v0.7.5 *Hopper's Compiler*: per-host recovery-channel
    configuration, used by ``vq admin audit-recovery`` to verify
    the 3-tier contract (BMC / Cockpit / recovery sshd) documented
    in ``docs/host_recovery_channels.md``.

    All fields optional with sensible defaults — bare-minimum host
    configs (no ``[hosts.X.recovery]`` block) get the standard
    contract probed; hosts that need custom ports / keys / URLs
    override here."""

    model_config = ConfigDict(extra="forbid")

    ssh_port: int = Field(default=22222, strict=True, ge=1, le=65535)
    """Port the recovery-sshd Match block listens on. The default
    matches ``contrib/setup-recovery-channels.sh``. Must be an integer
    in the TCP port range 1..65535."""

    ssh_key_path: str = "~/.ssh/id_ed25519_vibeqc-recovery"
    """Path on the LAPTOP to the recovery private key. Expanded
    via ``Path.expanduser()`` at probe time. The default matches
    the path the contract doc recommends generating."""

    ssh_user: str | None = None
    """Username on the remote for the recovery ssh. ``None`` means
    "use the ssh-alias's default user" (typically picks up the
    AlphAuthUsers from ssh_config)."""

    cockpit_port: int = Field(default=9090, strict=True, ge=1, le=65535)
    """Port Cockpit listens on. Default per Cockpit's own
    convention. The audit verb probes for HTTPS here. Must be an
    integer in the TCP port range 1..65535."""

    bmc_url: str | None = None
    """Optional. URL of the host's BMC / IPMI / iDRAC web UI for
    operator click-through (e.g. ``https://compute-a-bmc.example.org/``). The
    audit verb doesn't probe this — BMC auth surfaces are usually
    hostile to unauthenticated probes — but reporting the URL
    next to the other tiers means the operator doesn't have to
    hunt for it in a panic. ``None`` ⇒ doc-flag this host as
    Tier-1-N/A so the rest of the audit still passes."""


def _validate_scheduler_hook_lines(value: list[str]) -> list[str]:
    for line in value:
        if "\x00" in line or "\n" in line or "\r" in line:
            raise ValueError("scheduler hook entries must be single shell lines")
    return value


def _validate_scheduler_argv_entries(value: list[str]) -> list[str]:
    for entry in value:
        if not entry:
            raise ValueError("scheduler command_wrapper entries must be non-empty")
        if "\x00" in entry or "\n" in entry or "\r" in entry:
            raise ValueError("scheduler command_wrapper entries must be single argv tokens")
    return value


def _validate_optional_scheduler_string(
    value: str | None, field_name: str
) -> str | None:
    if value is None:
        return None
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ValueError(f"{field_name} must be a single line")
    if not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return value


class SchedulerProgramHooks(BaseModel):
    """Trusted per-program command customization for scheduler-backed jobs.

    These are host-maintainer config, not user-submitted command text. They let
    a scheduler host specialize qsub scripts for a submitted ``--program`` while
    preserving the same command argv and exit-marker wrapper.
    """

    model_config = ConfigDict(extra="forbid")

    prologue: list[str] = Field(default_factory=list)
    """Shell lines inserted after the host-level scheduler prologue and before
    the user command when ``JobSpec.program`` matches this table key."""

    epilogue: list[str] = Field(default_factory=list)
    """Shell lines inserted after the user command rc is captured and before the
    host-level scheduler epilogue when ``JobSpec.program`` matches."""

    command_wrapper: list[str] = Field(default_factory=list)
    """Argv prefix prepended to the submitted command when ``JobSpec.program``
    matches. Use for trusted site wrappers such as ``/site/bin/orcasub`` that
    must receive the user's command argv while vq still owns stdout/stderr
    capture and exit-marker handling."""

    @field_validator("prologue", "epilogue")
    @classmethod
    def _validate_hook_lines(cls, value: list[str]) -> list[str]:
        return _validate_scheduler_hook_lines(value)

    @field_validator("command_wrapper")
    @classmethod
    def _validate_command_wrapper(cls, value: list[str]) -> list[str]:
        return _validate_scheduler_argv_entries(value)


class SchedulerBuildAllocation(BaseModel):
    """SLURM-allocation build target for a runtime deployment.

    An alternative to :attr:`SchedulerRuntimeDeployment.update_host`. When set,
    ``vq admin update PROGRAM HOST`` submits the deployment command from the
    scheduler login host (:attr:`HostConfig.ssh`) as an ``sbatch --parsable
    --wait`` batch job with these arguments, so the compile runs inside a
    compute-node allocation rather than on a fixed SSH build host. ``sbatch
    --wait`` runs server-side and exits with the job's return code, so a
    multi-hour cold build survives an SSH blip that a foreground ``srun`` would
    not. The same plain deployment script runs in both modes; only the
    transport differs. This is the daemonless-SLURM analog of pbs-cluster's dedicated
    ``update_host`` build node: a cluster with no persistent build host obtains
    one per deployment from the scheduler.
    """

    model_config = ConfigDict(extra="forbid")

    scheduler: Literal["slurm"] = "slurm"
    """Allocation scheduler. Only ``slurm`` is implemented; the field is
    explicit so the shape can grow a ``pbs`` interactive-allocation form
    (``qsub`` batch) later without a silent default flip."""

    sbatch_args: list[str] = Field(default_factory=list)
    """Arguments passed to ``sbatch`` before the deployment command, e.g.
    ``["--account=grp", "--partition=build", "--time=02:00:00", "--ntasks=1",
    "--cpus-per-task=8", "--mem=16G"]``. The chosen partition's MaxTime must
    cover the whole build wall time, so prefer a build/production partition over
    a short development one for a cold native rebuild."""

    @field_validator("sbatch_args")
    @classmethod
    def _validate_sbatch_args(cls, value: list[str]) -> list[str]:
        for arg in value:
            if not isinstance(arg, str) or not arg.strip():
                raise ValueError("sbatch_args entries must be non-empty strings")
        return value


class SchedulerRuntimeDeployment(BaseModel):
    """Trusted deployment contract for one scheduler-host program runtime.

    Scheduler hosts are daemonless, so their program environments cannot use
    the normal remote-daemon ``[programs]`` update path.  This profile names a
    host-maintainer command that builds and atomically activates one runtime,
    plus an independent command on the scheduler login host that reports the
    active runtime identity as JSON.

    Both commands receive ``--program NAME --expected-sha FULL_SHA`` and an
    optional ``--tag TAG`` from ``vq admin update PROGRAM SCHEDULER_HOST``.
    The deployment command must stage away from the active path and atomically
    switch it only after its own build checks pass.  The verification command
    must emit the receipt documented in ``docs/config.toml.example``.

    The build host is either a fixed SSH target (:attr:`update_host`, pbs-cluster's
    dedicated build node) or a per-deployment SLURM allocation
    (:attr:`update_allocation`, for a daemonless SLURM login host with no fixed
    build node). The two are mutually exclusive; leave both unset to build on
    the login host itself.
    """

    model_config = ConfigDict(extra="forbid")

    update_command: str
    install_command: str | None = None
    update_host: str | None = None
    update_allocation: SchedulerBuildAllocation | None = None
    stage_source: bool = False
    """Stage vibe-qc source at ``--expected-sha`` from the driver before the
    build. Set this when the build host cannot itself fetch the source (a
    daemonless SLURM login host with no repo credentials): vq archives the tree
    at the exact SHA from :attr:`Config.scheduler_runtime_source_repo` on the
    driver, uploads it to a per-deployment stage dir under ``scratch_root``,
    verifies it, and appends ``--source-archive <remote-path>`` to the
    deployment command. Requires ``scheduler_runtime_source_repo`` to be set."""
    feed_source_mirror: str | None = None
    """Path (login-host-relative or absolute) of a PUSH-FED bare source
    mirror to feed with the exact ``--expected-sha`` before preparation.
    pbs-cluster's compute build node is offline and the online login node is not
    provisioned with private GitLab credentials, so its mirror at
    ``.local/share/vq-pbs-cluster/vibeqc.git`` is authoritative only for what has
    been pushed into it. When set, ``vq admin update PROGRAM HOST`` pushes
    the exact SHA into the two refs for that profile -- ``refs/heads/release``
    plus ``refs/remotes/origin/release`` for ``vibeqc-release``, or the
    corresponding ``main`` refs for branch-tip programs -- together with all
    tags from the driver's :attr:`Config.scheduler_runtime_source_repo`
    (which is therefore required) over the host's ssh alias. Keeping the ref
    pairs separate prevents a release feed from replacing the dev identity and
    replaces the manual per-release feed step from the fleet runbook.

    **The mirror must already exist and be push-fed**: a bare repository whose
    ``origin`` names its own path, which is how the preparer tells a push-fed
    mirror from a fetch-fed one. ``git init --bare`` leaves no ``origin`` at
    all, so create it with both commands -- see
    ``docs/scheduler_runtime_deployment.md``. vq checks this before feeding
    and refuses with the exact remedy; it does not create or repair the
    mirror, because an ``origin`` pointing elsewhere is a working fetch-fed
    setup rather than damage."""

    prepare_command: str | None = None
    """Login-host command that stages an offline-buildable source bundle
    (for example, a site-provided ``prepare-runtime-source`` wrapper
    — ABSOLUTE path required: the argv is shell-quoted in transit, so a
    ``~`` would reach the remote shell quoted and never expand).
    When set, ``vq admin update PROGRAM HOST`` runs it on :attr:`ssh` with
    ``--program NAME --expected-sha SHA [--tag TAG]`` after the mirror feed
    and before the build, under marker heartbeats, with its output teed
    into the update transcript. Idempotent preparers return success when the
    exact bundle already exists and keep re-runs cheap. A failure
    leaves the current runtime untouched."""

    prepare_timeout_seconds: float | None = 1800.0
    """Timeout for :attr:`prepare_command` (staging + wheelhouse download
    + the login-host import gate; no compilation)."""

    detached_build: bool = False
    """Run the deployment command detached on the fixed build host (requires
    :attr:`update_host`). The command starts under ``setsid`` with its output,
    return code, and pid written to a per-run directory under the host's
    ``scratch_root``; the driver polls over fresh SSH connections and streams
    output increments into the update transcript. A dropped SSH connection
    then costs one missed poll instead of the whole build — the 2026-07-24
    pbs-cluster deploy lost a 90-minute compile to exactly such a drop (ssh rc=255
    mid-build). The SLURM-allocation lane needs none of this: ``sbatch
    --wait`` already runs server-side."""
    verify_command: str
    verify_timeout_seconds: float | None = None
    """Independent timeout for the login-host verify command. ``None`` keeps the
    legacy behavior (``min(timeout_seconds, 60)``): a cheap in-line import check
    like pbs-cluster's must finish in a minute. Set this when the verify command must
    itself acquire a compute-node allocation (a runtime that refuses to run on
    the login node), so verification can wait for the scheduler to grant a node
    and then run the in-node import/banner check before emitting the receipt."""
    timeout_seconds: float | None = 14400.0

    @field_validator(
        "update_command",
        "install_command",
        "update_host",
        "verify_command",
        "feed_source_mirror",
        "prepare_command",
    )
    @classmethod
    def _validate_command_strings(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        return _validate_optional_scheduler_string(value, info.field_name)

    @field_validator(
        "timeout_seconds", "verify_timeout_seconds", "prepare_timeout_seconds"
    )
    @classmethod
    def _validate_timeout(
        cls, value: float | None, info: ValidationInfo
    ) -> float | None:
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError(f"{info.field_name} must be finite and positive")
        return value

    @model_validator(mode="after")
    def _validate_build_target(self) -> SchedulerRuntimeDeployment:
        if self.update_host is not None and self.update_allocation is not None:
            raise ValueError(
                "update_host and update_allocation are mutually exclusive: a "
                "runtime deployment builds either on a fixed SSH host or in a "
                "scheduler allocation, not both"
            )
        if self.detached_build and self.update_host is None:
            raise ValueError(
                "detached_build requires update_host: the detach/poll "
                "protocol targets a fixed SSH build host. An allocation "
                "build (update_allocation) already survives connection "
                "drops via sbatch --wait and must not set detached_build."
            )
        return self


class HostConfig(BaseModel):
    """Per-host SSH + remote-vq configuration."""

    model_config = ConfigDict(extra="forbid")

    ssh: str
    """SSH target understood by the local ssh client (alias from
    ~/.ssh/config or user@host)."""

    remote_vq: str = "vq"
    """How to invoke vq on the remote machine. Defaults to ``vq`` on PATH;
    set to an absolute path (e.g. ``/home/USER/vibeqc-queue/vibe-queue/.venv/bin/vq``)
    if vq isn't on the remote shell's default PATH."""

    admin_token_file: str | None = None
    """Absolute path to this host's bearer-token file, read by the remote
    ``vq`` process for delegated admin writes. The token contents never pass
    through the fleet driver. Leave unset for single-user hosts or when one
    explicit token is intentionally shared across the delegation."""

    @field_validator("admin_token_file")
    @classmethod
    def _validate_admin_token_file(cls, value: str | None) -> str | None:
        if value is not None and not Path(value).is_absolute():
            raise ValueError(
                "admin_token_file must be an absolute path on the remote host"
            )
        return value

    update_script_timeout_seconds: float | None = Field(
        default=None, gt=0, allow_inf_nan=False,
    )
    """Wall-clock cap, in seconds, for a managed venv update's update script
    on this host. Unset means the four-hour default.

    A small host can need more than four hours for a cold native rebuild:
    compute-b (6 cores) was reaped at 14400 s on 2026-09-12 fifteen minutes short
    of finishing, and the only override was an environment variable a
    planner-driven roll never sets (#32). An explicitly set
    ``VQ_UPDATE_SCRIPT_TIMEOUT`` still wins. A delegated update carries the
    resolved cap to the target, including into a detached update's transient
    unit, and the SSH observer's own cap grows with it."""

    fleet_role: Literal["auto", "managed", "vq-only", "alias", "excluded"] = "auto"
    """How driver-owned fleet rollouts treat this host.

    ``managed`` is a canonical runtime target. ``vq-only`` is a coordinator
    that must never receive vibe-qc or vibe-view lanes. ``alias`` is another
    queue/campaign name for :attr:`fleet_canonical_host`. ``excluded`` is
    intentionally outside fleet maintenance. ``auto`` preserves old configs
    during migration; rollout planning resolves only unambiguous cases.
    """

    fleet_canonical_host: str | None = None
    """Canonical host key when :attr:`fleet_role` is ``alias``."""

    remote_python: str | None = None
    """Default ``--python`` used for jobs submitted to this host when the
    user doesn't pass one explicitly. Typically points at a vibe-qc venv
    so submitted scripts can ``import vibeqc``."""

    branches: dict[str, str] = Field(default_factory=dict)
    """Named-branch -> Python interpreter path. Lets ``vq submit --branch
    NAME`` route to a specific venv without the chat knowing the
    absolute path. Typical use: ``main`` -> the vibeqc-dev clone's
    venv, ``release`` -> the vibeqc-release clone's venv. Empty by
    default; add entries to enable ``--branch`` routing for this host.
    """

    branch_aliases: dict[str, str] = Field(default_factory=dict)
    """Alias name -> canonical branch name (must be a key in ``branches``).
    Lets a single venv have multiple ``--branch`` spellings, e.g.
    ``dev``/``development`` -> ``main``, ``latest`` -> ``release``.
    Validated at load time: every alias's target must exist in
    ``branches``, and aliases must not collide with canonical branch
    names. Empty by default.
    """

    recovery: RecoveryConfig = Field(default_factory=RecoveryConfig)
    """v0.7.5 *Hopper's Compiler*: per-host recovery-channel
    configuration audited by ``vq admin audit-recovery``. Defaults
    are the values produced by ``contrib/setup-recovery-channels.sh``;
    customise per-host only when the install diverged from the
    contract documented in ``docs/host_recovery_channels.md``."""

    # ------------------------------------------------------------------
    # v1.0 external-scheduler backend (PBS / SGE), see
    # docs/pbs_dispatcher_backend_design.md. Additive + gated: the
    # default ``scheduler = "local"`` leaves every existing host
    # unchanged and the SchedulerDispatcher inert until a host opts in.
    # ------------------------------------------------------------------
    scheduler: Literal["local", "pbs", "sge", "slurm"] = "local"
    """How jobs land on this host. ``local`` (default) ⇒ the daemon's
    LocalDispatcher spawns a child process, exactly as before. ``pbs`` /
    ``sge`` ⇒ the SchedulerDispatcher submits to the host's own batch
    scheduler via ``qsub``/``sbatch`` over SSH (the host runs no vq daemon).
    Anything other than ``local`` requires ``scheduler_dialect`` and
    ``scratch_root`` to be set."""

    scheduler_dialect: Literal["torque", "pbspro", "sge", "slurm"] | None = None
    """Which command dialect the scheduler speaks (set from
    ``vq scheduler-probe``). ``torque`` and ``slurm`` are implemented today.
    Required when ``scheduler`` is not ``local``; must be ``None`` for a
    ``local`` host."""

    remote_scheduler_host: str | None = None
    """Arch 2 only: the ssh target the always-on vq daemon reaches to run
    ``qsub`` / ``qstat`` / ``qdel``. Usually equal to ``ssh``; kept separate
    so a future split (submit host ≠ login host) stays expressible. Defaults
    to ``ssh`` when a scheduler is set and this is ``None``."""

    submit_extra: list[str] = Field(default_factory=list)
    """Site scheduler flags as a flat argv list, e.g.
    ``["-q", "compute", "-A", "proj1"]`` for PBS/Torque or
    ``["--account", "proj", "--partition", "debug"]`` for SLURM. The dispatcher
    splits these into queue / account / extra directives that render once in
    the job script — they are NOT passed verbatim to ``qsub`` / ``sbatch``.
    Validated at load time via the dialect's parser."""

    scratch_root: str | None = None
    """Absolute directory on the scheduler host under which staged
    workspaces and ``$VQ_WORKDIR``-equivalent scratch live (e.g.
    ``/home/USER`` when the cluster has no dedicated ``/scratch``). Required
    when ``scheduler`` is not ``local``."""

    node_scratch_dir: str | None = None
    """Optional node-local scratch directory **template** on the compute node
    (e.g. ``/tmp1/$USER`` on pbs-cluster). When set, a scheduler job runs in a fresh
    per-task dir here on the node's local disk and copies all output back to the
    shared ``scratch_root`` workspace, sparing the NFS ``/home`` the heavy
    intermediate I/O. A trusted shell-expanded string (``$USER`` resolves on the
    node), not a literal path. ``None`` ⇒ jobs run directly in the workspace."""

    scheduler_mem_directive: Literal["request", "omit"] = "request"
    """Whether the job script requests memory from the scheduler.

    ``request`` (default) renders the spec's ``mem_mb`` as the dialect's
    memory directive (Torque ``-l mem=Nmb``, SLURM ``--mem=NM``) exactly as
    before. ``omit`` drops the scheduler memory directive while keeping
    ``mem_mb`` for vq-side accounting and the ``VQ_MEM_MB`` job env.

    ``omit`` exists because Torque's pbs_mom applies ``-l mem=`` as a **hard
    per-process RLIMIT_DATA / RLIMIT_RSS** on the compute node, and on Linux
    kernels >= 4.7 RLIMIT_DATA also counts mmap allocations — so a
    tightly-sized request kills the payload with ``std::bad_alloc`` seconds
    into execution even when the node has ample free RAM (BUG 118: every BH9
    child on a site-specific subqueue died this way). On sites whose
    scheduler does not admit on memory anyway (pbs-cluster has no queue/server
    ``resources_max``; local users submit without ``-l mem``), the directive
    is pure downside. Scheduler-host-only; must stay at the default for
    ``scheduler = 'local'`` hosts."""

    scheduler_gnu_time_command: str = "/usr/bin/time"
    """Absolute compute-node path to GNU ``time`` for scheduler telemetry.

    Scheduler jobs fail closed before starting their payload when this path is
    missing, non-executable, or not GNU Time. Override it for sites that do not
    install GNU Time at ``/usr/bin/time``. The configured executable must be
    visible on compute nodes without relying on interactive-shell ``PATH``.
    Scheduler-host-only; local hosts must leave the default unchanged.
    """

    scheduler_max_wall_time_seconds: Annotated[
        int,
        Field(strict=True, gt=0),
    ] | None = None
    """Operator-declared maximum wall time for this exact scheduler lane.

    This is a pre-admission safety boundary, not a capacity observation.
    ``None`` means the limit is unknown, not that the scheduler is unlimited.
    Scheduler-host-only; local hosts must leave it unset.
    """

    scheduler_max_cpus: Annotated[
        int,
        Field(strict=True, gt=0),
    ] | None = None
    """Operator-declared widest request this scheduler lane can ever run.

    The companion to ``scheduler_max_wall_time_seconds``, and read the same
    way: a pre-admission boundary, not a live capacity observation. ``None``
    means unknown, not unlimited.

    Unlike wall time, exceeding this does **not** refuse the submission. A
    node census goes stale -- a node returns to service, a partition is
    resized -- and refusing on a stale number would reject work the cluster
    can now run. ``vq submit`` warns instead, which is what the vibe-qc#148
    starvation needed: the request was admitted and then never started, with
    nothing anywhere saying it could not.

    `vq scheduler-probe HOST` reports the figure to put here as
    ``capacity.max_cpus_when_free``. Scheduler-host-only; local hosts must
    leave it unset.
    """

    scheduler_prologue: list[str] = Field(default_factory=list)
    """Trusted shell lines inserted into every scheduler job script after the
    job has entered its working directory and before the user command runs.
    Use for site-specific setup such as ``module load`` or sourcing a cluster
    environment. Entries are rendered verbatim, one shell line each."""

    scheduler_epilogue: list[str] = Field(default_factory=list)
    """Trusted shell lines inserted after the user command has finished and its
    rc has been captured, but before vq copies node-local scratch back and writes
    the terminal marker. Use sparingly for site cleanup/copyback glue. Entries
    are rendered verbatim, one shell line each."""

    scheduler_program_hooks: dict[str, SchedulerProgramHooks] = Field(
        default_factory=dict
    )
    """Trusted per-program scheduler hooks keyed by the submitted
    ``--program`` name (the same safe name class as ``JobSpec.program``). These
    run inside the generated qsub script only when the spec carries that program
    identity; unset or unmatched programs keep the generic script."""

    scheduler_runtime_deployments: dict[str, SchedulerRuntimeDeployment] = Field(
        default_factory=dict
    )
    """Independent managed-runtime deployment profiles for this scheduler host.

    Keys are program registry names such as ``vibeqc-release``, ``vibeqc-dev``,
    or ``vibe-view``.  These profiles are deliberately separate from
    ``scheduler_update_command``, which continues to own only the scheduler-side
    vq helper.
    """

    scheduler_driver: str | None = None
    """For a scheduler host (``scheduler != "local"``): the host-config key of
    the always-on **driver daemon** that dispatches this host's jobs over
    SSH+qsub. A scheduler host (e.g. pbs-cluster) runs no vq daemon of its own, so
    ``vq submit --host pbs-cluster`` forwards the spec to the driver (e.g.
    ``"coordinator"``), which drives the cluster (design doc §17). Required when
    ``scheduler`` is not ``local``; must name a host that runs a vq daemon."""

    scheduler_update_command: str | None = None
    """Optional remote command, run on the scheduler login host by
    ``vq admin update <scheduler-host>``, that refreshes the cluster-managed
    checkouts (for pbs-cluster: ``/home/USER/vibeqc-dev/scripts/update_cluster.sh``).
    The command is :func:`shlex.split`-parsed, then optional
    ``--update-script-arg`` values are appended. It is intentionally scheduler
    host config, not a ``programs`` entry, because the scheduler host runs no
    vq daemon and is updated through its driver."""

    scheduler_update_host: str | None = None
    """Optional SSH target for scheduler update/install commands.

    Leave unset to run ``scheduler_update_command`` /
    ``scheduler_install_command`` on :attr:`ssh`, preserving the original
    login-node behavior. Set this when the scheduler login node can submit jobs
    but compilation must happen on a specific build node. This is an SSH target,
    not a ``[hosts.*]`` config key, matching :attr:`remote_scheduler_host`."""

    scheduler_update_stage: str | None = None
    """Optional absolute shared path for scheduler-helper source staging.

    ``vq admin update HOST`` refreshes this stage from the driver's exact git
    revision before invoking the scheduler update command and exports it as
    ``VQ_SCHEDULER_STAGE``. When unset, vq uses
    ``$HOME/.cache/vq-admin/HOST`` on the update SSH target.
    """

    scheduler_install_command: str | None = None
    """Optional fresh-provisioning command for ``vq admin update
    <scheduler-host> --cluster-install`` (for pbs-cluster:
    ``/home/USER/vibeqc-dev/scripts/install_cluster.sh``). Same parsing and
    trust boundary as :attr:`scheduler_update_command`."""

    scheduler_update_timeout_seconds: float | None = 7200.0
    """Timeout for the scheduler host's remote provisioning command. ``None``
    disables the transport timeout. The default is long enough for login-node
    staging/wheelhouse refreshes while still bounding a hung SSH session."""

    @field_validator(
        "remote_scheduler_host",
        "scratch_root",
        "node_scratch_dir",
        "scheduler_driver",
        "scheduler_update_command",
        "scheduler_update_host",
        "scheduler_update_stage",
        "scheduler_install_command",
    )
    @classmethod
    def _validate_scheduler_strings(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        return _validate_optional_scheduler_string(value, info.field_name)

    @field_validator("scheduler_update_timeout_seconds")
    @classmethod
    def _validate_scheduler_update_timeout(cls, value: float | None) -> float | None:
        if value is not None and (not math.isfinite(value) or value <= 0):
            raise ValueError(
                "scheduler_update_timeout_seconds must be finite and positive"
            )
        return value

    @field_validator("scheduler_gnu_time_command")
    @classmethod
    def _validate_scheduler_gnu_time_command(cls, value: str) -> str:
        if not value.startswith("/"):
            raise ValueError("scheduler_gnu_time_command must be an absolute path")
        if any(char in value for char in ("\n", "\r", "\x00")):
            raise ValueError(
                "scheduler_gnu_time_command must be a single absolute path"
            )
        return value

    @field_validator("submit_extra")
    @classmethod
    def _validate_submit_extra(cls, value: list[str]) -> list[str]:
        """Fail fast on a malformed ``submit_extra`` (e.g. a dangling ``-q``).

        Parsing here, at load time, turns a site-config typo into an
        immediate, clear error rather than a cryptic ``qsub`` rejection at
        the first submission. Imports the pure dialect parser lazily so the
        config module stays free of the transport dependency.
        """
        from vq.scheduler_dialect import (  # noqa: PLC0415 — avoid import cycle
            DialectError,
            parse_submit_extra,
        )

        try:
            parse_submit_extra(value)
        except DialectError as exc:
            raise ValueError(str(exc)) from exc
        return value

    @field_validator("scheduler_prologue", "scheduler_epilogue")
    @classmethod
    def _validate_scheduler_hook_lines(cls, value: list[str]) -> list[str]:
        return _validate_scheduler_hook_lines(value)

    @field_validator("scheduler_program_hooks")
    @classmethod
    def _validate_scheduler_program_hooks(
        cls, value: dict[str, SchedulerProgramHooks]
    ) -> dict[str, SchedulerProgramHooks]:
        for name in value:
            if not _SCHEDULER_PROGRAM_KEY_PATTERN.fullmatch(name):
                raise ValueError(
                    "scheduler_program_hooks keys must match "
                    f"{_SCHEDULER_PROGRAM_KEY_PATTERN.pattern}"
                )
        return value

    @field_validator("scheduler_runtime_deployments")
    @classmethod
    def _validate_scheduler_runtime_deployments(
        cls, value: dict[str, SchedulerRuntimeDeployment]
    ) -> dict[str, SchedulerRuntimeDeployment]:
        for name in value:
            if not _SCHEDULER_PROGRAM_KEY_PATTERN.fullmatch(name):
                raise ValueError(
                    "scheduler_runtime_deployments keys must match "
                    f"{_SCHEDULER_PROGRAM_KEY_PATTERN.pattern}"
                )
        return value

    @model_validator(mode="after")
    def _validate_scheduler(self) -> HostConfig:
        """Cross-field validation for the scheduler backend.

        Keeps the gating contract honest: a ``local`` host carries no
        scheduler settings, and a scheduler host carries the minimum needed
        to submit (dialect + scratch root). ``remote_scheduler_host`` defaults
        to the ``ssh`` alias so the common single-host case needs no extra
        config.
        """
        if self.scheduler == "local":
            if self.submit_extra:
                raise ValueError("submit_extra must be unset when scheduler = 'local'")
            if self.remote_scheduler_host is not None:
                raise ValueError(
                    "remote_scheduler_host must be unset when scheduler = 'local'"
                )
            if self.scratch_root is not None:
                raise ValueError("scratch_root must be unset when scheduler = 'local'")
            if self.node_scratch_dir is not None:
                raise ValueError(
                    "node_scratch_dir must be unset when scheduler = 'local'"
                )
            if self.scheduler_mem_directive != "request":
                raise ValueError(
                    "scheduler_mem_directive must be left at 'request' when "
                    "scheduler = 'local'"
                )
            if self.scheduler_gnu_time_command != "/usr/bin/time":
                raise ValueError(
                    "scheduler_gnu_time_command must be left at '/usr/bin/time' "
                    "when scheduler = 'local'"
                )
            if self.scheduler_max_wall_time_seconds is not None:
                raise ValueError(
                    "scheduler_max_wall_time_seconds must be unset when "
                    "scheduler = 'local'"
                )
            if self.scheduler_max_cpus is not None:
                raise ValueError(
                    "scheduler_max_cpus must be unset when scheduler = 'local'"
                )
            if self.scheduler_dialect is not None:
                raise ValueError(
                    "scheduler_dialect must be unset when scheduler = 'local'"
                )
            if self.scheduler_driver is not None:
                raise ValueError(
                    "scheduler_driver must be unset when scheduler = 'local'"
                )
            if self.scheduler_update_command is not None:
                raise ValueError(
                    "scheduler_update_command must be unset when scheduler = 'local'"
                )
            if self.scheduler_install_command is not None:
                raise ValueError(
                    "scheduler_install_command must be unset when scheduler = 'local'"
                )
            if self.scheduler_update_host is not None:
                raise ValueError(
                    "scheduler_update_host must be unset when scheduler = 'local'"
                )
            if self.scheduler_update_stage is not None:
                raise ValueError(
                    "scheduler_update_stage must be unset when scheduler = 'local'"
                )
            if self.scheduler_prologue:
                raise ValueError(
                    "scheduler_prologue must be unset when scheduler = 'local'"
                )
            if self.scheduler_epilogue:
                raise ValueError(
                    "scheduler_epilogue must be unset when scheduler = 'local'"
                )
            if self.scheduler_program_hooks:
                raise ValueError(
                    "scheduler_program_hooks must be unset when scheduler = 'local'"
                )
            if self.scheduler_runtime_deployments:
                raise ValueError(
                    "scheduler_runtime_deployments must be unset when "
                    "scheduler = 'local'"
                )
            return self
        if self.scheduler_dialect is None:
            raise ValueError(
                f"scheduler = '{self.scheduler}' requires scheduler_dialect to be set "
                "(e.g. 'torque'); run `vq scheduler-probe <host>` to detect it"
            )
        if self.scheduler == "slurm" and self.scheduler_dialect != "slurm":
            raise ValueError(
                "scheduler = 'slurm' requires scheduler_dialect = 'slurm'"
            )
        for program, deployment in self.scheduler_runtime_deployments.items():
            allocation = deployment.update_allocation
            if allocation is not None and allocation.scheduler != self.scheduler:
                raise ValueError(
                    f"scheduler_runtime_deployments.{program}.update_allocation "
                    f"is {allocation.scheduler!r} but host scheduler is "
                    f"{self.scheduler!r}: a build allocation must target this "
                    "host's own scheduler"
                )
        if self.scheduler in {"pbs", "sge"} and self.scheduler_dialect == "slurm":
            raise ValueError(
                f"scheduler = '{self.scheduler}' cannot use scheduler_dialect = 'slurm'"
            )
        if (
            self.scheduler_update_stage is not None
            and not Path(self.scheduler_update_stage).is_absolute()
        ):
            raise ValueError("scheduler_update_stage must be an absolute path")
        if self.scratch_root is None:
            raise ValueError(
                f"scheduler = '{self.scheduler}' requires scratch_root to be set "
                "(an absolute dir on the scheduler host for staged workspaces)"
            )
        if not self.scratch_root.startswith("/"):
            raise ValueError(
                "scratch_root must be an absolute path on the scheduler host"
            )
        if self.scheduler_driver is None:
            raise ValueError(
                f"scheduler = '{self.scheduler}' requires scheduler_driver to be set "
                "(the host-config key of the always-on daemon that drives qsub; "
                "the scheduler host runs no daemon of its own)"
            )
        if self.remote_scheduler_host is None:
            self.remote_scheduler_host = self.ssh
        return self

    @model_validator(mode="after")
    def _validate_branches(self) -> HostConfig:
        """Cross-field validation for the branches/branch_aliases pair.

        Two invariants enforced at load time so a typo'd config fails
        fast (before any submit) rather than mysteriously at runtime:
          * No alias may share a name with a canonical branch (the
            collision would be silently shadowed by branches' priority).
          * Every alias's target must be a key in ``branches``.
        """
        if not self.branch_aliases:
            return self
        collisions = set(self.branch_aliases) & set(self.branches)
        if collisions:
            raise ValueError(
                f"branch_aliases collides with branches names: "
                f"{sorted(collisions)} -- an alias cannot have the same "
                f"name as a canonical branch"
            )
        unknown = {
            alias: target
            for alias, target in self.branch_aliases.items()
            if target not in self.branches
        }
        if unknown:
            raise ValueError(
                f"branch_aliases targets not found in branches: {unknown} "
                f"(known canonical branches: {sorted(self.branches)})"
            )
        return self

    @model_validator(mode="after")
    def _validate_fleet_role(self) -> HostConfig:
        if self.fleet_role == "alias":
            if self.fleet_canonical_host is None:
                raise ValueError(
                    "fleet_role = 'alias' requires fleet_canonical_host"
                )
        elif self.fleet_canonical_host is not None:
            raise ValueError(
                "fleet_canonical_host is only valid when fleet_role = 'alias'"
            )
        return self

    def resolve_branch(self, name: str) -> str | None:
        """Look up a branch name (canonical or alias) and return the
        interpreter path. Returns ``None`` if the name doesn't match.

        Canonical branches have priority; aliases are checked only if
        the name isn't already a canonical branch.
        """
        if name in self.branches:
            return self.branches[name]
        if name in self.branch_aliases:
            return self.branches[self.branch_aliases[name]]
        return None

    def scheduler_lane_metadata(self) -> dict[str, object] | None:
        """Return additive host/lane metadata for doctor and programs JSON.

        Partition extraction reuses the scheduler dialect's authoritative
        ``submit_extra`` parser so metadata, admission, and rendered job
        directives cannot disagree about the effective lane.
        """
        if self.scheduler == "local":
            return None
        from vq.scheduler_dialect import (  # noqa: PLC0415 - avoid import cycle
            parse_submit_extra,
        )

        partition, _account, _extra = parse_submit_extra(self.submit_extra)
        return {
            "partition": partition,
            "max_wall_time_seconds": self.scheduler_max_wall_time_seconds,
            "max_cpus": self.scheduler_max_cpus,
            "source": "host-config",
        }

    def known_branch_names(self) -> list[str]:
        """All names that ``--branch`` will accept, sorted (canonical +
        aliases). Used to build "did you mean" error messages."""
        return sorted(set(self.branches) | set(self.branch_aliases))


# ----------------------------------------------------------------------
# v0.5.18: program registry
# ----------------------------------------------------------------------


PROGRAM_STATUS_OK = "OK"
PROGRAM_STATUS_MISSING = "MISSING"
PROGRAM_STATUS_UNHEALTHY = "UNHEALTHY"


@dataclass(frozen=True)
class ProgramAvailability:
    """``vq programs``' classification of one program.

    ``status`` is ``OK``, ``MISSING`` (work dispatched here will fail) or
    ``UNHEALTHY`` (the runtime loads; its configured healthcheck does not
    pass). ``healthcheck_status`` is one of the ``_program_probe.HEALTHCHECK_*``
    values for a venv program and ``None`` for kinds without a healthcheck.
    """

    status: str
    reason: str
    healthcheck_status: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == PROGRAM_STATUS_OK


class _ProgramBase(BaseModel):
    """Common fields all program kinds share."""

    model_config = ConfigDict(extra="forbid")

    description: str | None = None
    """Free-text label shown by ``vq programs``."""

    def availability(self) -> tuple[bool, str]:
        raise NotImplementedError

    def availability_status(self) -> ProgramAvailability:
        ok, reason = self.availability()
        return ProgramAvailability(
            PROGRAM_STATUS_OK if ok else PROGRAM_STATUS_MISSING, reason,
        )


def _git_sha_matches(actual: str, expected: str) -> bool:
    """Accept exact or unambiguous configured-prefix SHA pins."""
    actual_norm = actual.strip().lower()
    expected_norm = expected.strip().lower()
    return (
        actual_norm == expected_norm
        or actual_norm.startswith(expected_norm)
        or expected_norm.startswith(actual_norm)
    )


class BinaryProgram(_ProgramBase):
    """An executable file on disk. CRYSTAL, ORCA, Psi4, etc."""

    kind: Literal["binary"]
    binary: str
    """Absolute path to the executable.

    Availability checks existence and the executable bit. Recognized serial
    CRYSTAL frontends additionally receive a bounded no-input startup probe;
    ORCA installations retain their MPI-launcher probe.
    """

    def availability(self) -> tuple[bool, str]:
        """Return (ok, reason). ``reason`` is a one-line human-readable
        explanation for the status."""
        path = Path(self.binary)
        if not path.exists():
            return False, f"not found: {self.binary}"
        if not path.is_file():
            return False, f"not a file: {self.binary}"
        if not os.access(path, os.X_OK):
            return False, f"not executable: {self.binary}"
        if _is_crystal_serial_binary(path):
            startup_ok, startup_reason = _crystal_serial_availability(path)
            if not startup_ok:
                return False, startup_reason
            return True, f"executable at {self.binary}; {startup_reason}"
        if _is_orca_binary(path):
            mpi_ok, mpi_reason = _orca_mpi_availability(path)
            if not mpi_ok:
                return True, (
                    f"executable at {self.binary}; {mpi_reason}; "
                    "serial ORCA only until MPI runtime is fixed"
                )
            return True, f"executable at {self.binary}; {mpi_reason}"
        return True, f"executable at {self.binary}"


class VenvProgram(_ProgramBase):
    """A python interpreter + a git checkout vq can refresh.

    Used for vibeqc-dev / vibeqc-release. The future v0.6.0
    ``vq admin update <name>`` will use ``git_dir`` + ``branch`` +
    ``update_script`` to keep these fresh.
    """

    kind: Literal["venv"]
    python: str
    """Absolute path to the venv's python (e.g.
    ``/home/USER/vibeqc-dev/.venv/bin/python``)."""

    git_dir: str
    """Absolute path to the git checkout (= ``cd <git_dir> && git pull``)."""

    branch: str | None = None
    """Branch to track (``main`` for dev, ``release`` for stable tag).
    None means "whatever git currently has checked out; user is
    responsible for keeping the branch right."""

    upstream: str | None = None
    """Git URL or local path this checkout is cloned from by ``vq admin
    install``.

    Only used to *create* ``git_dir``; an existing checkout keeps whatever
    ``origin`` it has, and vq never repoints it. Unset means this program
    cannot be provisioned through vq and must be cloned by hand -- which is
    what every host in the 2026-09 migration needed, and the one step of that
    migration that could not go through vibe-queue."""

    install_script: str | None = None
    """Program's own installer, relative to ``git_dir``, run once by ``vq
    admin install`` after the clone. Shlex-split like
    :attr:`update_script`, so it can carry arguments
    (``"scripts/install.sh --editable --extras dev"``).

    Distinct from :attr:`update_script` because the two are genuinely
    different operations: one builds an environment that does not exist, the
    other refreshes one that does. ``vq admin install`` never runs the update
    script, and ``vq admin update`` never runs this one."""

    update_script: str | None = None
    """Relative path under ``git_dir`` of the script to run after
    ``git pull``. None means "no script, just the pull." Reserved for
    v0.6.0's ``vq admin update``; ``vq programs`` doesn't run it."""

    runtime_slot_root: str | None = None
    """Opt this venv program into per-SHA runtime slots, rooted here.

    Absolute path to a slot root laid out by :mod:`vq.runtime_slots`
    (``releases/<sha>/`` plus ``current`` / ``previous`` symlinks). ``None`` --
    the default, and every host today -- keeps the historical in-place
    behaviour: ``git pull`` plus an editable install into the existing venv.

    Why the opt-in exists: an in-place update rewrites the very files a live
    interpreter imports from, so a job paused across it can serve some modules
    from ``sys.modules`` and import others off the rewritten disk. Slots make
    the update land out-of-place and flip a pointer, so a running job keeps the
    runtime it started with. Scheduler hosts already work this way; this is how
    a venv host gets there, one host at a time.

    **Deployment ordering matters, and it is sharper than it looks.**
    ``_ProgramBase`` sets ``extra="forbid"`` and ``load_config`` raises
    ``ConfigError`` on a validation failure, so a vq that predates this field
    does not merely ignore the key -- it fails to load the config at all, and
    every command on that host breaks. Adding the field to this model is safe;
    writing the key into a config a host reads is not, until that host runs a vq
    carrying this field. Introduce it per host, after that host is updated.

    (Contrast ``DrainState``, which now tolerates unknown keys: that is
    machine-written state where a strict reader silently un-drained a host. This
    is a hand-edited file, where rejecting a typo is worth having.)
    """

    extras: list[str] = Field(default_factory=list)
    """Optional dependency groups this program's virtualenv **requires**.

    Names from ``[project.optional-dependencies]`` (``web``, ``test``,
    ``dev``). Empty -- the default, and every program that only runs the
    daemon -- keeps the historical behaviour exactly: ``vq admin update``
    rebuilds the venv with whatever profile ``.vq-install-metadata`` recorded.

    The recorded profile is evidence of how the venv was last built, not a
    statement of what it is for, and the two diverge the moment a host starts
    serving something new. ``update_script`` cannot carry ``--extras`` --
    :func:`vq.admin._managed_update_script_args` allows only source selectors
    and the recorded install mode there, because a serving venv's install
    target must come from something vq validates, not from a free-form command
    line.

    Distinct from the per-update ``--update-script-arg --recreate-venv
    --update-script-arg --extras --update-script-arg PROFILE`` of #11, which
    changes one environment once. That is the right tool for "fix this host
    now"; this is the right one for "this venv is *for* serving the console",
    because it is re-applied on every managed rebuild and it survives a venv
    rebuilt outside that path -- a fresh provision, ``install.sh``,
    ``reinstall.sh`` -- where ``.vq-install-metadata`` dies with the venv it
    described. An operator request that does not cover this declaration is
    refused rather than silently widened.

    Concretely (coordinator, 2026-09-09): one venv served ``vq-daemon`` and
    ``vq-web``; its recorded profile was ``core``; ``vq web install`` had
    pointed the console unit at it anyway, and the unit failed on every start
    for two days with "vq web requires the 'web' extra". Declaring
    ``extras = ["web"]`` is the fix, and the only one that survives the next
    rebuild.

    A declaration is a floor, never a ceiling: the effective profile is the
    smallest one containing both what is declared here and what the venv
    already had, so adding ``"web"`` to a ``dev`` environment resolves to
    ``all`` rather than dropping its test tooling. See
    :func:`vq.admin._resolve_extras_profile`.

    **Where it takes effect:** the managed daemon transaction -- ``vq admin
    update`` / ``vq self-update`` on the env serving this host's daemon. That
    is the one path that has proved the updater is vq's own
    ``scripts/update.sh`` before naming an install target on it; an ordinary
    update runs whatever ``update_script`` says, which for a vibe-qc or
    vibe-view program is a script that has never heard of ``--extras``. An
    update that cannot apply a declaration says so rather than passing over it
    (:func:`vq.admin._warn_if_declared_extras_are_inert`).

    **Deployment ordering matters**, for the same reason it does on
    :attr:`runtime_slot_root`: ``_ProgramBase`` sets ``extra="forbid"``, so a
    vq that predates this field does not ignore the key -- it fails to load the
    config, and every command on that host breaks. Update the host first, then
    write the key.
    """

    post_update_script: str | None = None
    """Relative path under ``git_dir`` of an additional script to run after
    a successful pull + optional ``update_script``. Use this for deployment
    finalizers that should not be part of the main build script, such as
    installing bundled runtime data into a freshly rebuilt venv. The command
    uses the same parsing and trust boundary as ``update_script``."""

    healthcheck_command: str | None = None
    """Optional command run by ``vq programs`` after the basic venv/import
    probes. It is shlex-split and executed from ``git_dir``. Use this for
    quick runtime checks that a plain import cannot prove, such as
    ``xvfb-run -a vibe-view capture-selftest`` for the managed
    ``vibeview-dev`` capture program. The healthcheck runner exports
    ``PYVISTA_OFF_SCREEN=True`` and prepends the venv ``bin/`` to ``PATH``.
    That lets a venv-local ``xvfb-run`` shim fall back to direct OSMesa/VTK
    offscreen rendering on hosts where no system ``xvfb-run`` is available.

    **A binary that exists only inside the venv is fragile**, and vq says so
    while it still works: a successful healthcheck whose ``argv[0]`` resolves
    in the venv ``bin/`` and nowhere on PATH reports a warning beside its
    result. On two hosts that ``xvfb-run`` was a hand-written shim that no
    reinstall reproduces, so migrating the venv silently broke every
    vibe-view healthcheck until it was copied across by hand. Ship such a
    helper from the program's own repository, or name it by absolute path."""

    import_check: str | None = None
    """v0.12.x: module name passed to ``python -c "import <name>"`` after
    a rebuild, to verify the freshly-built env actually imports (an
    ``rc=0`` from ``update_script`` does NOT guarantee a usable
    extension — the 2026-06-26 build-host ABI skew imported broken). When set
    *together with* ``update_script`` it ALSO arms atomic checkout/package
    rollback for generic environments. Structural vibe-qc registrations add a
    stricter contract: ``vq admin update`` requires a clean checkout, native
    source-freshness evidence, and the actual compiled core reported by the
    registered interpreter before in-place mutation. A failed build or
    post-build import probe restores that state so a deployed env is only ever
    a consistent ``{Python tree, .so}`` pair. None (default) disables both the
    probe and atomic machinery for ordinary pure-git envs with no native
    extension. Managed vibe-qc source checkouts using their default ``.venv``
    are recognized from that combined layout and implicitly use ``vibeqc`` so
    legacy registrations fail closed; setting it explicitly remains
    recommended and wins over inference. Other venv programs keep the opt-in
    behavior."""

    import_symbols: list[str] = Field(default_factory=list)
    """Optional exported names that must exist on ``import_check``.

    Use this to catch stale native-extension/API-surface mismatches that a
    plain module import can miss, e.g. ``import_check = "vibeqc"`` plus
    ``import_symbols = ["CosxVariant"]`` for pbs-cluster release/dev envs."""

    auto_update_policy: Literal["tag", "branch"] = "tag"
    """v0.7.4 *Ritchie's Pipe*: which drift signal ``vq admin
    auto-update`` watches for this env.

    * ``"tag"`` (default; v0.6.11 behavior): the env is "current"
      when its HEAD points at the newest semver-shaped tag on
      ``origin``. ``auto-update`` runs ``vq admin update --tag X``
      when a newer tag appears upstream. Right for release-tracking
      envs (vibeqc-release) where the deploy unit IS the tag.
    * ``"branch"`` (new in v0.7.4): the env is "current" when its
      HEAD SHA matches ``origin/<branch>``. ``auto-update`` runs
      ``vq admin update`` (no --tag) when ``origin/<branch>``
      advances past the local HEAD. Right for dev-tracking envs
      (vibeqc-dev) where the deploy unit IS the branch tip.

    Why this wasn't here from the start (v0.6.11 originally refused
    branch-mode entirely): the concern was "auto-deploying a dev-tip
    commit unattended is a footgun" — the dev tree could be dirty
    (chats writing scratch into the checkout) or drifted (wrong
    branch checked out). Three later ships closed those failure
    modes one by one:

      v0.6.54: agent protocol — chats write to ``$VQ_WORKDIR``,
               not into the git checkout
      v0.7.1 item 1: post-pull branch validation — silent branch
                     drift fails loudly with ``branch_mismatch``
      v0.7.1 item 5: ``fail_on_dirty`` opt-in — operators can pin
                     "this env should always be clean"

    So a properly-configured vibeqc-dev env is now safe to
    auto-update from branch tip. Operators with looser dev
    discipline can keep ``"tag"`` and continue running
    ``vq admin update vibeqc-dev`` manually.

    Requires ``branch`` to be set when ``"branch"``."""

    fail_on_dirty: bool = False
    """v0.7.1 *Lamport's Clock* Item 5: when True, ``vq admin update``
    flips ``LAST OK=False`` if the post-update working tree has
    uncommitted changes (the standard ``git status --porcelain``
    notion of dirty). Default False preserves pre-v0.7.1 behavior
    for dev clones where dirty is expected (vibeqc-dev with
    basissetdev artifacts, in-flight experimental edits, etc.).

    Recommended setting per env:
      * vibeqc-queue, vibeqc-release → ``fail_on_dirty = true``
        (these should never be dirty; dirty signals a config /
        deploy bug)
      * vibeqc-dev → ``fail_on_dirty = false`` (default; dirty
        is normal during research work)

    See ``docs/v0_7_1_lamports_clock_design.md`` § Item 5."""

    provides_branches: list[str] | None = None
    """v0.5.47: branch names this env serves under ``vq submit
    --branch X``. When set and non-empty, ``vq admin update <env>``
    pauses ONLY jobs whose ``spec.branch`` is in this list (instead of
    the default queue-wide pause). Release-branch jobs keep running
    while a dev-env rebuild churns, and vice versa.

    Include both canonical branch names (the keys in
    ``[hosts.X.branches]``) and any aliases that should route to this
    env (the keys in ``[hosts.X.branch_aliases]`` pointing at this
    env's canonical branch). Example for the typical dev/release
    split::

        [programs.vibeqc-dev]
        ...
        provides_branches = ["main", "dev", "development"]

        [programs.vibeqc-release]
        ...
        provides_branches = ["release", "latest"]

    Leaving this None (or an empty list) preserves the pre-v0.5.47
    behavior: ``vq admin update <env>`` pauses the whole queue. Safe
    default; opt in only when you have a clean branch/env mapping."""

    expected_git_sha: str | None = None
    """Optional runtime pin for production submits using this program.

    When set, ``vq submit --program NAME`` fails fast if the configured
    checkout's current HEAD does not match this SHA (short prefixes are
    accepted), and the daemon repeats the check immediately before dispatch so
    queued jobs do not start after a mutable checkout drifts. ``vq programs``
    also marks the program unavailable on mismatch. This is for paper/release
    envs where running off-pin is worse than refusing the job."""

    expected_import_version: str | None = None
    """Optional module-version pin for production submits using this program.

    Requires ``import_check``. ``vq submit --program NAME`` rejects the job when
    the import probe reports a different ``version_info()``/``__version__``;
    the daemon repeats that check before dispatch.
    """

    @field_validator("python", "git_dir")
    @classmethod
    def _validate_absolute_venv_paths(cls, value: str, info) -> str:
        """Require one CWD-independent identity for updater locks/mutation."""
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{info.field_name} must not be empty")
        if not Path(value).is_absolute():
            raise ValueError(
                f"{info.field_name} must be an absolute path, got {value!r}"
            )
        return value

    @field_validator("extras")
    @classmethod
    def _validate_known_extras(cls, value: list[str]) -> list[str]:
        """Reject a typo here rather than at rebuild time on the host.

        ``pip install 'vq[wbe]'`` is not an error -- pip warns about an unknown
        extra and installs the base package -- so an unvalidated typo would
        produce a venv that is quietly missing exactly what was declared.
        """
        seen: list[str] = []
        for name in value:
            if name not in KNOWN_PROGRAM_EXTRAS:
                known = ", ".join(KNOWN_PROGRAM_EXTRAS)
                raise ValueError(
                    f"unknown extra {name!r}; vq publishes: {known}"
                )
            if name in seen:
                raise ValueError(f"extra {name!r} is listed twice")
            seen.append(name)
        return seen

    @model_validator(mode="after")
    def _declared_extras_have_a_route_into_the_venv(self) -> VenvProgram:
        """``extras`` is inert without the updater that applies it.

        The declaration is read on the way to ``scripts/update.sh``, so a
        program vq cannot rebuild would accept the key, change nothing, and
        leave an operator believing the capability was declared -- the exact
        belief that kept coordinator's console down. Refuse instead.
        """
        if self.extras and not self.update_script:
            raise ValueError(
                "extras requires update_script; vq applies a declared extra "
                "when it rebuilds the environment, and a program without an "
                "update script is never rebuilt by vq. Note that having one "
                "is necessary but not sufficient: only a managed daemon "
                "update applies the declaration"
            )
        return self

    @model_validator(mode="after")
    def _runtime_probe_settings_are_consistent(self) -> VenvProgram:
        if self.import_symbols and not self.import_check:
            raise ValueError("import_symbols requires import_check")
        if self.expected_import_version and not self.import_check:
            raise ValueError("expected_import_version requires import_check")
        if self.healthcheck_command and any(
            c in self.healthcheck_command for c in ("\x00", "\n", "\r")
        ):
            raise ValueError(
                "healthcheck_command must be a single shell-style command line"
            )
        return self

    def current_git_sha(self, *, full: bool = False) -> str | None:
        args = ("HEAD",) if full else ("--short=12", "HEAD")
        return _query_git(self.git_dir, "rev-parse", *args)

    def current_git_branch(self) -> str | None:
        return _query_git(self.git_dir, "branch", "--show-current") or _query_git(
            self.git_dir, "rev-parse", "--abbrev-ref", "HEAD"
        )

    def current_git_describe(self) -> str | None:
        return _query_git(self.git_dir, "describe", "--tags", "--always")

    def current_git_dirty(self) -> bool | None:
        return _git_has_changes(self.git_dir)

    def vibeqc_source_root(self) -> Path | None:
        """Return this checkout when it has vibe-qc's native source layout."""
        root = Path(self.git_dir)
        if (
            (root / "python" / "vibeqc" / "__init__.py").is_file()
            and (root / "cpp" / "src" / "bindings.cpp").is_file()
        ):
            return root
        return None

    def managed_vibeqc_source_root(self) -> Path | None:
        """Return a vibe-qc source checkout using its default venv."""
        root = self.vibeqc_source_root()
        if root is not None and Path(self.python).parent.parent == root / ".venv":
            return root
        return None

    def effective_import_check(self) -> str | None:
        """Resolve the runtime probe without making config host-dependent."""
        if self.import_check:
            return self.import_check
        if self.managed_vibeqc_source_root() is not None:
            return "vibeqc"
        return None

    def _import_identity(self) -> tuple[int, str, str | None]:
        module = self.effective_import_check()
        if module is None:
            return 0, "", None
        source_root = (
            self.vibeqc_source_root()
            if module == "vibeqc"
            else None
        )
        if source_root is not None:
            rc, output, version, _module_path, _native_core_path = (
                run_import_runtime_identity_probe(
                    self.python,
                    module,
                    symbols=self.import_symbols,
                    source_root=source_root,
                )
            )
            return rc, output, version
        return run_import_identity_probe(
            self.python, module, symbols=self.import_symbols,
        )

    def import_version(self) -> str | None:
        if self.effective_import_check() is None:
            return None
        rc, _output, version = self._import_identity()
        return version if rc == 0 else None

    def runtime_pin_mismatches(self, *, include_import: bool = True) -> list[str]:
        """Return configured runtime-pin mismatches for this venv program."""
        mismatches: list[str] = []
        if self.expected_git_sha:
            actual_sha = self.current_git_sha(
                full=len(self.expected_git_sha) == 40
            )
            if actual_sha is None:
                mismatches.append(
                    f"git_sha expected {self.expected_git_sha}, "
                    "but current git SHA could not be read"
                )
            elif not _git_sha_matches(actual_sha, self.expected_git_sha):
                mismatches.append(
                    f"git_sha expected {self.expected_git_sha}, got {actual_sha}"
                )
        if include_import and self.expected_import_version:
            actual_version = self.import_version()
            label = f"import {self.import_check} version"
            if actual_version is None:
                mismatches.append(
                    f"{label} expected {self.expected_import_version}, "
                    "but current import version could not be read"
                )
            elif actual_version != self.expected_import_version:
                mismatches.append(
                    f"{label} expected {self.expected_import_version}, "
                    f"got {actual_version}"
                )
        return mismatches

    def _runtime_detail(self) -> str:
        parts = [f"python={self.python}", f"git_dir={self.git_dir}"]
        sha = self.current_git_sha()
        describe = self.current_git_describe()
        branch = self.current_git_branch()
        dirty = self.current_git_dirty()
        if sha:
            parts.append(f"git_sha={sha}")
        if describe:
            parts.append(f"git_describe={describe}")
        if branch:
            parts.append(f"git_branch={branch}")
        if dirty is True:
            parts.append("git_dirty=true")
        elif dirty is None:
            parts.append("git_dirty=unknown")
        return "; ".join(parts)

    def _run_healthcheck(self, git_dir: Path) -> tuple[bool, str | None]:
        return _program_probe.run_venv_healthcheck(
            self.healthcheck_command,
            python=self.python,
            git_dir=git_dir,
        )

    def _classify_healthcheck(
        self, git_dir: Path
    ) -> _program_probe.VenvHealthcheck:
        return _program_probe.classify_venv_healthcheck(
            self.healthcheck_command,
            python=self.python,
            git_dir=git_dir,
        )

    def availability(self) -> tuple[bool, str]:
        result = self.availability_status()
        return result.ok, result.reason

    def availability_status(self) -> ProgramAvailability:
        """Classify the venv as ``OK``, ``MISSING`` or ``UNHEALTHY``.

        ``MISSING`` means work dispatched here will fail: no interpreter or
        checkout, a pin mismatch, or a runtime that does not import.
        ``UNHEALTHY`` means everything vq loads does load and only the
        configured healthcheck failed, which on 2026-09-12 was a Linux-only
        ``xvfb-run`` on a macOS host beside a vibe-view that imported fine.
        """
        not_run = (
            _program_probe.HEALTHCHECK_NOT_RUN
            if self.healthcheck_command
            else _program_probe.HEALTHCHECK_NOT_CONFIGURED
        )

        def missing(reason: str) -> ProgramAvailability:
            return ProgramAvailability(PROGRAM_STATUS_MISSING, reason, not_run)

        py = Path(self.python)
        if not py.exists():
            return missing(f"python not found: {self.python}")
        if not py.is_file():
            return missing(f"python not a file: {self.python}")
        gd = Path(self.git_dir)
        if not gd.is_dir():
            return missing(f"git_dir missing: {self.git_dir}")
        if not (gd / ".git").exists():
            return missing(f"git_dir is not a git checkout: {self.git_dir}")
        detail = self._runtime_detail()
        mismatches = self.runtime_pin_mismatches(include_import=False)
        if mismatches:
            return missing("runtime pin mismatch: " + "; ".join(mismatches))
        loaded = f"venv ok ({detail})"
        import_check = self.effective_import_check()
        if import_check:
            rc, output, import_version = self._import_identity()
            if rc != 0:
                stderr = output.strip().splitlines()
                tail = stderr[-1] if stderr else "(no stderr)"
                return missing(f"import {import_check} failed: {tail}")
            if (
                self.expected_import_version
                and import_version != self.expected_import_version
            ):
                got = import_version or "(not reported)"
                return missing(
                    "runtime pin mismatch: "
                    f"import {import_check} version expected "
                    f"{self.expected_import_version}, got {got}"
                )
            import_detail = f"`import {import_check}`"
            if import_version:
                import_detail += f" (__version__={import_version})"
            if self.import_symbols:
                names = ", ".join(self.import_symbols)
                import_detail += f" + symbols [{names}]"
            loaded += f"; {import_detail} ok"
        health = self._classify_healthcheck(gd)
        if not health.ok:
            return ProgramAvailability(
                PROGRAM_STATUS_UNHEALTHY,
                health.detail or "healthcheck failed",
                health.status,
            )
        suffix = f"; healthcheck ok ({health.detail})" if health.detail else ""
        return ProgramAvailability(
            PROGRAM_STATUS_OK, loaded + suffix, health.status,
        )

    @field_validator("runtime_slot_root")
    @classmethod
    def _validate_runtime_slot_root(cls, value: str | None) -> str | None:
        """Reject a slot root that cannot mean the same thing everywhere.

        The root is handed to a build that runs elsewhere, so a relative path
        would resolve against whatever working directory that build inherited.
        Caught at config load rather than at deploy time: the alternative is
        discovering it when a release is already half-applied.
        """
        if value is None:
            return None
        if not value.strip():
            raise ValueError("runtime_slot_root must not be empty")
        if not value.startswith("/"):
            raise ValueError(
                f"runtime_slot_root must be an absolute path, got {value!r}"
            )
        return value


class ImportProgram(_ProgramBase):
    """A python module that should be importable from a specific
    interpreter. Lets us register e.g. ``pyscf`` as a program even
    though it's not its own binary or its own venv -- it's bundled
    inside the vibeqc-dev venv."""

    kind: Literal["import"]
    python: str
    """Absolute path to the interpreter that should be able to import."""

    import_check: str
    """Python module name passed to ``-c 'import <name>'``."""

    import_symbols: list[str] = Field(default_factory=list)
    """Optional exported names that must exist on ``import_check``."""

    def import_version(self) -> str | None:
        rc, _output, version = run_import_identity_probe(
            self.python, self.import_check, symbols=self.import_symbols,
        )
        return version if rc == 0 else None

    def availability(self) -> tuple[bool, str]:
        py = Path(self.python)
        if not py.exists():
            return False, f"python not found: {self.python}"
        rc, output, import_version = run_import_identity_probe(
            str(py), self.import_check, symbols=self.import_symbols,
        )
        if rc != 0:
            stderr = output.strip().splitlines()
            tail = stderr[-1] if stderr else "(no stderr)"
            return False, f"import {self.import_check} failed: {tail}"
        import_detail = f"`import {self.import_check}`"
        if import_version:
            import_detail += f" (__version__={import_version})"
        if self.import_symbols:
            names = ", ".join(self.import_symbols)
            return True, (
                f"{import_detail} + symbols [{names}] "
                f"from {self.python} ok"
            )
        return True, f"{import_detail} from {self.python} ok"


ProgramConfig = Annotated[
    BinaryProgram | VenvProgram | ImportProgram,
    Field(discriminator="kind"),
]


class MultiUserConfig(BaseModel):
    """v0.6.x: multi-user deployment configuration.

    When ``enabled = true`` and the daemon runs as root, vq operates in
    multi-user mode: state is laid out under a system-level root
    (``/var/lib/vq`` by default) with per-user subdirectories, and
    ownership checks apply to kill / fetch / resubmit operations.

    Absent section = single-user mode (default, backward-compatible).
    """

    model_config = ConfigDict(extra="forbid")

    enabled: StrictBool = False
    """Set to true to activate multi-user mode. Requires root daemon."""

    admin_group: str = "vq-admins"
    """Unix group whose members bypass ownership checks on kill / fetch /
    resubmit. Operators in this group can manage any user's jobs.
    Default: ``vq-admins``."""


_QuotaCap = Annotated[int, Field(strict=True, ge=0)]


class PerUserQuotaConfig(BaseModel):
    """v0.6.x: per-user quota overrides.

    Any field left at its default (None) falls back to the corresponding
    default in :class:`QuotaConfig`.
    """

    model_config = ConfigDict(extra="forbid")

    max_pending_jobs: _QuotaCap | None = None
    """Maximum number of PENDING/RUNNING/SUSPENDED jobs this user may have
    at once. None = use the global default."""

    max_concurrent_cpus: _QuotaCap | None = None
    """Maximum number of CPU cores this user may consume concurrently.
    None = use the global default."""


class QuotaConfig(BaseModel):
    """v0.6.x: per-user quota enforcement.

    Absent section = quotas disabled (all defaults are None/empty).
    When the section is present but no overrides are configured, every
    user gets the default limits (if set).
    """

    model_config = ConfigDict(extra="forbid")

    default_max_pending_jobs: _QuotaCap | None = None
    """Default cap on active jobs (PENDING + RUNNING + SUSPENDED) per user.
    None = unlimited."""

    default_max_concurrent_cpus: _QuotaCap | None = None
    """Default cap on concurrent CPU usage per user.
    None = unlimited."""

    users: dict[str, PerUserQuotaConfig] = Field(default_factory=dict)
    """Per-user overrides keyed by UID (string, e.g. ``"1000"``).
    Each entry's fields override the corresponding defaults."""

    def effective_max_pending_jobs(self, uid: int | str) -> int | None:
        uid_str = str(uid)
        if uid_str in self.users and self.users[uid_str].max_pending_jobs is not None:
            return self.users[uid_str].max_pending_jobs
        return self.default_max_pending_jobs

    def effective_max_concurrent_cpus(self, uid: int | str) -> int | None:
        uid_str = str(uid)
        if uid_str in self.users and self.users[uid_str].max_concurrent_cpus is not None:
            return self.users[uid_str].max_concurrent_cpus
        return self.default_max_concurrent_cpus


class NotificationConfig(BaseModel):
    """v0.5.35: webhook notifications fired when a job goes terminal.

    Designed for Slack / Discord / Mattermost incoming webhooks — the
    daemon POSTs a small JSON payload that carries both ``text`` (Slack /
    Mattermost) and ``content`` (Discord) keys, so the same URL works
    against any of them. Custom HTTP receivers can read the structured
    ``job`` sub-object for the full job summary.

    Best-effort: a failed POST is logged but never stalls the daemon,
    never re-tries, never re-fires. If you need stronger delivery
    semantics, point the webhook at a queue/relay that owns retry policy.

    ``webhook_url`` is the only required field today; empty / unset =
    notifications disabled.

    v0.7.17 *Postel's Robustness*: ``notify_on_states`` filters the
    notification stream so a busy queue doesn't drown the operator's
    Slack channel in every COMPLETED ping. Empty list (the default)
    preserves pre-v0.7.17 behaviour — fire on every terminal state.
    A non-empty list restricts notifications to the listed states
    (case-insensitive; validated at config load against
    :data:`vq.spec.TERMINAL_STATES`). Typical "alert on failure only"
    config:

    .. code-block:: toml

        [notifications]
        webhook_url = "https://hooks.slack.com/..."
        notify_on_states = ["failed", "oom_killed", "starved",
                            "time_exceeded", "killed",
                            "aborted_by_queue"]
    """

    model_config = ConfigDict(extra="forbid")

    webhook_url: str | None = None
    notify_on_states: list[str] = Field(default_factory=list)
    """v0.7.17: state filter for the webhook. Empty = fire on every
    terminal state (backward-compat default). Non-empty = fire only
    when the spec's state matches. Validated at load — an entry
    that doesn't match a real JobState name fails the config parse
    with a ConfigError naming the bad value."""

    @field_validator("notify_on_states", mode="after")
    @classmethod
    def _validate_state_names(cls, v: list[str]) -> list[str]:
        # Lazy import to avoid module-load circular: spec imports
        # nothing from config, but we use spec's TERMINAL_STATES
        # to validate, so importing it here keeps the dependency
        # one-way.
        from vq.spec import TERMINAL_STATES  # noqa: PLC0415
        valid = {s.value for s in TERMINAL_STATES}
        normalised: list[str] = []
        for entry in v:
            if not isinstance(entry, str):
                raise ValueError(
                    f"notify_on_states entries must be strings; "
                    f"got {entry!r}"
                )
            low = entry.lower()
            if low not in valid:
                raise ValueError(
                    f"notify_on_states {entry!r}: not a valid "
                    f"terminal state. Valid: {sorted(valid)}"
                )
            normalised.append(low)
        # Dedupe + preserve order.
        seen: set[str] = set()
        deduped: list[str] = []
        for state in normalised:
            if state in seen:
                continue
            seen.add(state)
            deduped.append(state)
        return deduped


class PoolConfig(BaseModel):
    """v0.11.0: a named group of hosts for ``vq submit auto`` placement —
    ``[pools.<name>] hosts = ["a", "b"]``. Lets a submitter scope
    load-aware placement to a subset (e.g. a "compute" pool that excludes
    the daily-driver / gaming boxes) without a manual ``vq host down``
    each time."""

    model_config = ConfigDict(extra="forbid")

    hosts: list[str] = Field(default_factory=list)
    """The hosts in this pool. Each must be a key in the top-level
    ``[hosts.*]`` table (validated at load time)."""


class WebConsoleConfig(BaseModel):
    """v0.25.0: host-local defaults for ``vq web run`` — the ``[web]`` section.

    Same contract as :class:`DaemonRunConfig`: every field mirrors a CLI
    flag, an explicit flag always wins, and the config value fills in only
    when the flag is absent. Purpose is the same too — make the config
    file, not a hand-edited unit ``ExecStart`` line, the durable home for
    how this host serves its console.

    It matters more here than for the daemon, because the console's
    settings were previously reachable *only* through argv and two
    undocumented environment variables. A console deployed by copying a
    unit file therefore carried its entire configuration in a single
    shell line that nothing validated and no upgrade path rewrote. The
    2026-08-05 audit found exactly that failure on the reference fleet:
    an ``ExecStart`` pinned to a hand-staged checkout, 1081 commits
    behind the vq that owned it, with no record anywhere in the config.

    Precedence, highest first: CLI flag > environment variable > this
    section > built-in default. :mod:`vq.web.settings` is the single
    place that resolves it.
    """

    model_config = ConfigDict(extra="forbid")

    bind: str | None = None
    """Default for ``--host`` (listen address). Built-in default is
    ``127.0.0.1`` — loopback-only, because the read surface is
    unauthenticated unless fleet accounts exist."""

    port: int | None = Field(default=None, ge=1, le=65535)
    """Default for ``--port``. Built-in default is 8765 -- the port
    ``vq web run`` has used since v0.5."""

    fleet: bool | None = None
    """Default for ``--fleet`` (fleet-console mode). Built-in default is
    False: most hosts serve only their own queue and must not start an
    SSH fan-out."""

    fleet_interval_seconds: int | None = Field(default=None, ge=5)
    """Seconds between background fleet sweeps. Built-in default 30.
    Values below 5 are rejected rather than silently clamped."""

    log_level: str | None = None
    """Default for ``--log-level`` (uvicorn). Built-in default ``info``."""

    public_bind_ack: bool | None = None
    """Config-file equivalent of ``--i-understand-public-bind``.

    Set this only on a host whose bind is deliberately non-loopback and
    whose exposure you have accepted. It suppresses the startup warning;
    it does not change what is served."""

    title: str | None = None
    """Brand text in the page header. Built-in default ``vq``. Set it to
    your fleet's name so an operator with two consoles open can tell them
    apart."""

    @field_validator("log_level")
    @classmethod
    def _known_log_level(cls, value: str | None) -> str | None:
        if value is None:
            return None
        allowed = {"critical", "error", "warning", "info", "debug", "trace"}
        lowered = value.strip().lower()
        if lowered not in allowed:
            raise ValueError(
                f"log_level '{value}' is not one of {sorted(allowed)}"
            )
        return lowered

    @field_validator("bind", "title")
    @classmethod
    def _non_empty(cls, value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be empty")
        return stripped


class HostRetirement(BaseModel):
    """Explicit operator retirement, bound to exact historical host evidence."""

    model_config = ConfigDict(extra="forbid")

    retired_at: str
    reason: str
    authorization_reference: str
    retained_receipts: dict[str, str] = Field(min_length=1)

    @field_validator("retired_at")
    @classmethod
    def _dated_decision(cls, value: str) -> str:
        from datetime import datetime

        timestamp = datetime.fromisoformat(value)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("retired_at must include a timezone")
        return value

    @field_validator("reason", "authorization_reference")
    @classmethod
    def _explicit_decision(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("retirement requires a reason and authorization reference")
        return value.strip()

    @field_validator("retained_receipts")
    @classmethod
    def _exact_receipts(cls, value: dict[str, str]) -> dict[str, str]:
        if any(not key.strip() or re.fullmatch(r"[0-9a-f]{64}", digest) is None
               for key, digest in value.items()):
            raise ValueError("retained_receipts must name rollouts with lowercase SHA-256 digests")
        return value


class FleetConfig(BaseModel):
    """v0.26.1: fleet-wide knobs for ``vq admin rollout-latest``."""

    model_config = ConfigDict(extra="forbid")

    retired_hosts: dict[str, HostRetirement] = Field(default_factory=dict)
    """Audit declarations only; these names are never active fleet hosts."""

    check_timeout_seconds: float | None = Field(default=None, gt=0)
    """Budget for each ``vq doctor`` check during a rollout sweep.

    The sweep runs ``vq doctor --all --json`` and, until this existed, could
    not pass a budget at all -- so every host got the 10 s default forever.
    That is not a lot for a check that makes three remote vq calls inside it:
    pbs-cluster's login node needs 1.6-2.5 s each, so ``source-sha`` intermittently
    ran out, the helper's live SHA went missing from that sweep, and the lane
    read as not converged. Five supersede refusals on 2026-09-10 came from
    exactly that, with nothing wrong on the host.

    Raise it for a fleet with a slow login node. It is a *budget*, not a
    delay: a healthy host answers in well under it and the sweep is no slower.
    Unset keeps ``doctor.DEFAULT_CHECK_TIMEOUT_SECONDS``.

    Note this is a mitigation and not the fix. A probe that runs out of time
    is now reported as a timeout rather than as a negative verdict (see
    ``LaneState.probe_unavailable``), which is what makes the refusal
    retryable; raising the budget only makes it rarer."""


class DaemonRunConfig(BaseModel):
    """v0.16.0: host-local defaults for the ``vq daemon run`` capacity caps.

    ``[daemon]`` in the daemon host's own config file. Each field mirrors
    the CLI flag of the same name; an explicit CLI flag always wins, the
    config value fills in only when the flag is absent. Purpose: make the
    config file — not hand-edits of the systemd unit's ExecStart line —
    the durable home for per-host caps. Unit rewrites (fresh installs,
    cap re-deployments, contrib-file copies) have repeatedly dropped
    flags like ``--default-job-mem-mb`` on fleet hosts; a ``[daemon]``
    section survives all of that because nothing rewrites the config.
    """

    model_config = ConfigDict(extra="forbid")

    max_cpus: int | None = Field(default=None, ge=1)
    """Default for ``--max-cpus`` (concurrent CPU slots)."""

    max_jobs: int | None = Field(default=None, ge=1)
    """Default for ``--max-jobs`` (concurrent local jobs)."""

    max_scheduler_jobs: int | None = Field(default=None, ge=1)
    """Default for ``--max-scheduler-jobs`` (active scheduler-backed jobs)."""

    max_mem_mb: int | None = Field(default=None, ge=1)
    """Default for ``--max-mem-mb`` (host memory budget in MB)."""

    default_job_mem_mb: int | None = Field(default=None, ge=1)
    """Default for ``--default-job-mem-mb`` (assumed + cgroup-capped MB
    for jobs that declare no ``--mem-mb``)."""


class Config(BaseModel):
    """Top-level config. Missing file -> empty Config (all defaults)."""

    model_config = ConfigDict(extra="forbid")

    min_vq_version: str | None = None
    """Oldest vq allowed to load this config, as ``"X.Y.Z"``.

    Unset (the default) means any vq may load it, ignoring the top-level keys
    it does not know -- see :func:`_split_unknown_top_level`. Set it when a
    key you are adding must not be ignored, so an older vq refuses the file
    with the version it needs instead of running on a config it has silently
    half-read. A system config that gains a policy-bearing key must set it:
    tolerance there would weaken ``[multi_user] admin_group`` rather than
    merely lose a feature."""

    default_host: str | None = None
    hosts: dict[str, HostConfig] = Field(default_factory=dict)
    programs: dict[str, ProgramConfig] = Field(default_factory=dict)
    """v0.5.18: registered programs visible to ``vq programs`` and (in
    v0.6.0) ``vq admin update``. Empty by default; chats add entries to
    enable both discoverability and future-maintenance."""
    notifications: NotificationConfig = Field(default_factory=NotificationConfig)
    """v0.5.35: webhook notifications on terminal job state. Absent
    section = disabled (default). See :class:`NotificationConfig`."""
    multi_user: MultiUserConfig = Field(default_factory=MultiUserConfig)
    """v0.6.x: multi-user deployment configuration. Absent section =
    single-user mode (default). See :class:`MultiUserConfig`."""
    fleet: FleetConfig = Field(default_factory=FleetConfig)
    """v0.26.1: fleet-wide rollout knobs. Absent section = built-in defaults.
    See :class:`FleetConfig`."""
    daemon: DaemonRunConfig = Field(default_factory=DaemonRunConfig)
    """v0.16.0: host-local defaults for the ``vq daemon run`` capacity
    caps. Absent section = no defaults (CLI flags or unlimited). See
    :class:`DaemonRunConfig`."""
    web: WebConsoleConfig = Field(default_factory=WebConsoleConfig)
    """v0.25.0: host-local defaults for ``vq web run``. Absent section =
    built-in defaults (loopback bind, port 8765, single-host mode). See
    :class:`WebConsoleConfig`."""
    quotas: QuotaConfig = Field(default_factory=QuotaConfig)
    """v0.6.x: per-user quota enforcement. Absent section = quotas
    disabled (default). See :class:`QuotaConfig`."""
    pools: dict[str, PoolConfig] = Field(default_factory=dict)
    """v0.11.0: named host groups for ``vq submit auto`` (``[pools.<name>]
    hosts = [...]``). Empty = no pools; ``vq submit auto`` then considers
    every configured host."""
    default_pool: str | None = None
    """v0.11.0: if set, a bare ``vq submit auto`` (no ``--pool``) scopes to
    this pool instead of every host — e.g. point it at a "compute" pool so
    the daily-driver / gaming boxes are excluded by default."""
    fleet_rollout_order: list[str] = Field(default_factory=list)
    """Canonical host order for ``vq admin rollout-latest``.

    Hosts omitted here are appended deterministically. Naming an alias,
    vq-only coordinator, or excluded host does not turn it into a runtime
    target; :attr:`HostConfig.fleet_role` remains authoritative.
    """
    scheduler_runtime_source_repo: str | None = None
    """DEPRECATED since v0.26.1: spell it ``pin_source_repos["mpei/vibe-qc"]``.

    Absolute path to a local vibe-qc git repository on the driver machine.
    When a ``[hosts.*.scheduler_runtime_deployments.*]`` profile sets
    ``stage_source = true``, vq archives this repo's tree at the requested
    ``--expected-sha`` and uploads it to the scheduler host, so a build host
    without repo credentials can still build the exact commit. Point it at a
    full vibe-qc checkout (the repository root that contains ``python/vibeqc``).

    It predates the 2026-09-08 split, when one checkout held every program and
    one global setting was sufficient. :attr:`pin_source_repos` now describes
    the same thing per repository, and the two overlapping settings are one
    setting too many: this one is exactly the ``"mpei/vibe-qc"`` entry.

    Still accepted, and still authoritative for vibe-qc programs when
    ``pin_source_repos`` has no entry for them, so a driver that has not
    migrated keeps working. Setting both to different paths is an error rather
    than a precedence rule -- there is no reading of that config that is
    obviously right. Setting this one at all warns once."""

    fleet_report_repo: str | None = None
    """External private Git checkout containing accepted fleet reports.

    Report storage is independent of the running controller's source checkout.
    Reports keep their existing ``vibe-queue/releases/`` or ``releases/`` paths.
    Required only for commands that select accepted reports; ordinary queue
    operation and exact-SHA updates do not require a report repository.
    """

    fleet_report_history_repo: str | None = None
    """Retained private Git history for authenticating old report digests.

    This preserves the original commits and pre-hardening ancestry proof when
    current reports move to a separate operations repository. It is read only
    apart from fetching refs, and never supplies the newest deployable report.
    """

    pin_source_repos: dict[str, str] = Field(default_factory=dict)
    """Local checkout for each repository a release-report pin resolves in.

    Keyed by the pin's ``repo`` slug, e.g.::

        [pin_source_repos]
        "mpei/vibe-qc"    = "/home/USER/vq/vibe-qc-dev"
        "mpei/vibe-queue" = "/home/USER/vq/vibe-queue"
        "mpei/vibe-view"  = "/home/USER/vq/vibe-view-dev"

    Schema ``/3`` reports pin four components across three repositories, and
    every pin's provenance -- tag, tag object, and SHA ancestry -- can only be
    checked in the repository that commit actually lives in. Before the
    2026-09-08 split one checkout held them all, so the loader used the
    runtime clone for everything; that is still what ``/2`` reports get.

    Unset entries are not fatal on their own: a ``/2`` report never consults
    this, and a ``/3`` report fails closed naming the slug it could not
    resolve, rather than validating a pin against the wrong repository."""

    build_path_dirs: list[str] = Field(default_factory=list)
    """Extra absolute directories prepended to PATH for a build on this host.

    ``vq admin update`` runs a program's ``update_script`` from a remote
    command, which a **non-login** shell serves, so anything a login profile
    puts on PATH is missing. On Arch/Manjaro that is ``/usr/bin/core_perl``,
    and libecpint's vendored libcerf dies generating man pages with
    ``pod2man``. vq's answer is to put the directory on PATH for the build
    rather than to source a host's login profile -- see
    ``docs/fleet_update_runbook.md`` for why, and for the alternatives that
    were rejected.

    The Arch perl directories are built in. This is for the next gap, so it
    does not need a vq release and a fleet rollout: configured directories are
    prepended ahead of the built-in ones, and any that do not exist on this
    host are skipped, so one shared config is safe across a mixed fleet."""

    estimate_python: str | None = None
    """v0.11.0: absolute path to a local vibe-qc interpreter used to estimate
    a job's peak memory for ``vq submit auto`` placement. When set and the
    job is a single-file vibe-qc ``.py`` submit *without* an explicit
    ``--mem-mb``, the submit host runs the job's dry-run once with this
    interpreter (``VIBEQC_DRY_RUN_ESTIMATE``) to read
    ``[memory].estimate_bytes`` and place by RAM-fit. Unset / non-vibe-qc job
    / any failure → fall back to core-fit. Point it at a venv where vibe-qc
    is installed (e.g. ``vibeqc-dev/.venv/bin/python``)."""

    @property
    def vibeqc_source_repo(self) -> str | None:
        """The driver-local vibe-qc checkout, under either spelling.

        :attr:`pin_source_repos` wins, since it is the one that survives; the
        deprecated :attr:`scheduler_runtime_source_repo` fills in. A
        disagreement between the two is rejected at load time, so this cannot
        be silently choosing between two different checkouts.
        """
        return (
            self.pin_source_repos.get(VIBEQC_REPO_SLUG)
            or self.scheduler_runtime_source_repo
        )

    @field_validator("min_vq_version")
    @classmethod
    def _release_version_string(cls, value: str | None) -> str | None:
        if value is not None and _declared_release_tuple(value) is None:
            raise ValueError(
                f'{MIN_VERSION_KEY} must be a release version string like '
                f'"0.26.0" (got {value!r})'
            )
        return value

    @model_validator(mode="after")
    def _validate_pools(self) -> Config:
        """Fail fast at load time on a pool that names an unknown host, an
        empty pool, or a ``default_pool`` that isn't a defined pool."""
        overlap = self.hosts.keys() & self.fleet.retired_hosts.keys()
        if overlap:
            raise ValueError(f"hosts cannot be both active and retired: {sorted(overlap)}")
        for name, pool in self.pools.items():
            if not pool.hosts:
                raise ValueError(f"pool '{name}' has no hosts")
            unknown = [h for h in pool.hosts if h not in self.hosts]
            if unknown:
                raise ValueError(
                    f"pool '{name}' references unknown host(s) {unknown}; "
                    f"known hosts: {sorted(self.hosts) or '(none)'}"
                )
        if self.default_pool is not None and self.default_pool not in self.pools:
            raise ValueError(
                f"default_pool '{self.default_pool}' is not a defined pool; "
                f"known pools: {sorted(self.pools) or '(none)'}"
            )
        if len(self.fleet_rollout_order) != len(set(self.fleet_rollout_order)):
            raise ValueError("fleet_rollout_order contains duplicate host names")
        unknown_rollout_hosts = [
            host for host in self.fleet_rollout_order if host not in self.hosts
        ]
        if unknown_rollout_hosts:
            raise ValueError(
                "fleet_rollout_order references unknown host(s) "
                f"{unknown_rollout_hosts}; known hosts: "
                f"{sorted(self.hosts) or '(none)'}"
            )
        for name, host in self.hosts.items():
            canonical = host.fleet_canonical_host
            if canonical is None:
                continue
            if canonical == name:
                raise ValueError(
                    f"host '{name}' cannot be its own fleet_canonical_host"
                )
            if canonical not in self.hosts:
                raise ValueError(
                    f"host '{name}' references unknown fleet_canonical_host "
                    f"'{canonical}'"
                )
            target = self.hosts[canonical]
            if target.fleet_role in {"alias", "vq-only", "excluded"}:
                raise ValueError(
                    f"host '{name}' aliases '{canonical}', whose fleet_role "
                    f"is {target.fleet_role!r}; aliases must point to a "
                    "canonical managed/auto host"
                )
        for directory in self.build_path_dirs:
            if not directory.startswith("/"):
                raise ValueError(
                    f"build_path_dirs entry {directory!r} must be an absolute "
                    "path; PATH entries are resolved on the host that builds"
                )
        for slug, repo_path in self.pin_source_repos.items():
            if not repo_path.startswith("/"):
                raise ValueError(
                    f"pin_source_repos[{slug!r}] must be an absolute path on "
                    "the driver machine"
                )
        if self.scheduler_runtime_source_repo is not None:
            if not self.scheduler_runtime_source_repo.startswith("/"):
                raise ValueError(
                    "scheduler_runtime_source_repo must be an absolute path on "
                    "the driver machine"
                )
            modern = self.pin_source_repos.get(VIBEQC_REPO_SLUG)
            if modern is not None and modern != self.scheduler_runtime_source_repo:
                raise ValueError(
                    "scheduler_runtime_source_repo "
                    f"({self.scheduler_runtime_source_repo}) and "
                    f"pin_source_repos[{VIBEQC_REPO_SLUG!r}] ({modern}) name "
                    "different vibe-qc checkouts. They are the same setting; "
                    "keep the pin_source_repos entry and delete the other"
                )
            _warn_deprecated_source_repo()
        stagers = [
            f"{host}:{program}"
            for host, host_cfg in self.hosts.items()
            for program, deployment in host_cfg.scheduler_runtime_deployments.items()
            if deployment.stage_source
        ]
        # Either spelling satisfies this. Requiring only the deprecated one
        # locked a fully migrated driver out of source staging entirely.
        if stagers and self.vibeqc_source_repo is None:
            raise ValueError(
                "scheduler_runtime_deployments with stage_source = true "
                f"require a vibe-qc checkout on the driver: {sorted(stagers)}. "
                f'Set [pin_source_repos] "{VIBEQC_REPO_SLUG}" = "/path/to/'
                'vibe-qc"'
            )
        return self

    def host(self, name: str) -> HostConfig:
        """Return the HostConfig for ``name`` or raise ConfigError."""
        if name not in self.hosts:
            raise ConfigError(
                f"host '{name}' not found in config; "
                f"add a [hosts.{name}] section to {config_path()}"
            )
        return self.hosts[name]

    def resolve_host(self, host: str | None) -> str:
        """Pick a host: explicit arg wins, else default_host, else error."""
        if host:
            return host
        if self.default_host:
            return self.default_host
        raise ConfigError(f"HOST is required (or set default_host in {config_path()})")

    def resolve_pool_hosts(self, pool: str | None) -> list[str]:
        """Candidate hosts for ``vq submit auto`` placement: an explicit
        ``pool`` name wins; else ``default_pool`` if set; else every
        configured host. Raises ConfigError on an unknown pool name."""
        name = pool if pool is not None else self.default_pool
        if name is None:
            return list(self.hosts.keys())
        if name not in self.pools:
            raise ConfigError(
                f"unknown pool '{name}'; known pools: "
                f"{sorted(self.pools) or '(none)'}"
            )
        return list(self.pools[name].hosts)


def config_dir() -> Path:
    """Config dir. Override with $VQ_CONFIG_DIR; otherwise XDG_CONFIG_HOME/vq."""
    # Keep both historical config-dir entry points behind the same test-safety
    # boundary. Import lazily to avoid broadening the module import graph.
    from vq import paths

    paths.require_explicit_test_path(ENV_CONFIG_DIR, "per-user vq config root")
    env = os.environ.get(ENV_CONFIG_DIR)
    if env:
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_CONFIG_HOME") or "~/.config"
    return Path(xdg).expanduser() / "vq"


def config_path() -> Path:
    """Path of the config file (does not require it to exist)."""
    return config_dir() / "config.toml"


_CANONICAL_SYSTEM_CONFIG_PATH = Path("/etc/vq/config.toml")
SYSTEM_CONFIG_PATH = _CANONICAL_SYSTEM_CONFIG_PATH
"""v0.6.x: canonical location of the system-wide vq config on a
multi-user host. The root multi-user daemon reads it (its systemd
unit sets ``VQ_CONFIG_DIR=/etc/vq``); the CLI consults it via
:func:`system_multi_user_enabled` so a user on a multi-user host
auto-detects multi-user mode without mirroring ``[multi_user]`` into
their own ``~/.config/vq/config.toml``."""
ENV_TEST_SYSTEM_CONFIG_FILE = "VQ_TEST_SYSTEM_CONFIG_FILE"
"""Internal inherited pytest substitute for the canonical system config.

The public :data:`SYSTEM_CONFIG_PATH` remains canonical so provenance and
deployment checks keep their production identity. Test subprocesses use this
separate capability-bound path rather than attempting to inspect ``/etc``.
"""


def _guarded_system_config_path() -> Path:
    """Return the system config only after enforcing pytest containment."""
    from vq import paths

    path = SYSTEM_CONFIG_PATH.expanduser()
    if (
        paths._running_under_pytest()
        and path == _CANONICAL_SYSTEM_CONFIG_PATH
        and (test_path := os.environ.get(ENV_TEST_SYSTEM_CONFIG_FILE))
    ):
        path = Path(test_path).expanduser()
    paths.require_test_path_within_sandbox(path, "vq system config file")
    return path


@lru_cache(maxsize=2)
def _parse_config_bytes(contents: bytes) -> dict:
    """Cache only parsing, keyed by complete bytes, never a policy decision.

    Queue sweeps authorize each row under its lock (#547). Re-reading the
    current file preserves revocation and read-error behavior while avoiding
    repeated TOML parsing for thousands of unchanged rows. Two entries cover
    the system and personal policies without retaining an unbounded history.
    """
    return tomllib.loads(contents.decode("utf-8"))


def _copy_toml_value(value: Any) -> Any:
    """Isolate TOML containers without copying immutable scalar values.

    This accepts only the built-in values returned by ``tomllib.loads`` with
    its default float parser: dicts and lists are the only mutable types.
    Strings, numbers, booleans and date/time values can safely be shared.
    Unlike a general object graph, parsed TOML has no cycles or aliases that
    require deepcopy's memo table or object reconstruction protocol.
    """
    if isinstance(value, dict):
        return {key: _copy_toml_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_toml_value(item) for item in value]
    return value


def _read_config_data(path: Path) -> dict:
    # Always open/read: mtime, size and inode are not content authorities.
    # Validation receives a private mapping because validators/callers may
    # mutate nested containers. Validation itself is deliberately not cached.
    with path.open("rb") as stream:
        return _copy_toml_value(_parse_config_bytes(stream.read()))


def system_multi_user_enabled() -> bool:
    """v0.6.x: True iff a system-wide ``/etc/vq/config.toml`` exists
    and has ``[multi_user] enabled = true``.

    The vq CLI ORs this into its multi-user decision so that on a
    multi-user host the client and the daemon agree on where job
    state lives (``/var/lib/vq/users/<uid>/``) — without every user
    having to hand-edit their personal config.

    Best-effort: a missing file, a parse error, or an unreadable
    file all return False. A broken ``/etc/vq/config.toml`` must
    never break the single-user CLI."""
    path = _guarded_system_config_path()
    try:
        if not path.is_file():
            return False
        data = _read_config_data(path)
    except (OSError, tomllib.TOMLDecodeError):
        return False
    mu = data.get("multi_user")
    return isinstance(mu, dict) and mu.get("enabled") is True


def load_system_config() -> Config | None:
    """Load and fully validate the system config when it is present.

    Unlike :func:`system_multi_user_enabled`, this is an authorization-policy
    input, not a best-effort mode hint.  A present but unreadable, malformed,
    or invalid system config therefore raises :class:`ConfigError`; falling
    back to a user's personal config could otherwise weaken the system
    ``admin_group`` policy.
    """
    path = _guarded_system_config_path()
    try:
        data = _read_config_data(path)
    except FileNotFoundError:
        return None
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"failed to parse {path}: {e}") from e
    except OSError as e:
        raise ConfigError(f"failed to read {path}: {e}") from e
    return _validate_config_data(data, path)


def load_config() -> Config:
    """Load the config file or return an empty Config if it doesn't exist.

    Raises ConfigError on parse / validation failure with the offending file
    path included so the user can fix it.
    """
    path = config_path()
    if not path.exists():
        return Config()
    try:
        data = _read_config_data(path)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"failed to parse {path}: {e}") from e
    return _validate_config_data(data, path)


def _validate_config_data(data: dict, path: Path) -> Config:
    """Turn one parsed config file into a Config, or raise ConfigError.

    The version floor is enforced before anything else so that a config
    written for a newer vq reports the version it needs, rather than a list of
    keys this vq happens not to recognize -- the keys are the symptom.
    """
    _enforce_min_vq_version(data, path)
    known = _split_unknown_top_level(data, path)
    try:
        return Config.model_validate(known)
    except ValidationError as e:
        raise ConfigError(
            f"invalid config in {path}: {validation_error_summary(e)}"
        ) from None
    except Exception as e:
        raise ConfigError(
            f"invalid config in {path}: {type(e).__name__}"
        ) from None


def validation_error_summary(error: ValidationError) -> str:
    """Render a config validation error as the key and the reason only.

    Built from pydantic's structured error list, without the input values,
    the context or the documentation link that its default string form
    carries. One problem reads as one line; several are one line each.
    """
    problems = [
        f"{'.'.join(str(part) for part in item['loc']) or 'config'}: {item['msg']}"
        for item in error.errors(
            include_url=False, include_context=False, include_input=False,
        )
    ]
    if len(problems) == 1:
        return problems[0]
    return f"{len(problems)} problems:\n" + "\n".join(
        f"  {problem}" for problem in problems
    )
