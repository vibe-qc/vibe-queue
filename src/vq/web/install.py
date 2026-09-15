"""Install a vq service — the web console or the queue daemon — under a
supervisor.

``vq web install`` and ``vq daemon install`` replace the
copy-a-template-and-edit-the-ExecStart ritual that ``contrib/vq-web.service``
and ``contrib/vq-daemon.service`` documented. That ritual worked, and it also
produced the failure this module exists to prevent -- twice, once per service.

Both are described by a :class:`ServiceKind`, which names the verb to run, the
unit and marker names, and the unit's own header text. Everything else here is
shared, because the hard parts -- the unprivileged-account access checks, the
atomic unit write, the idempotent manager commands, the provenance marker
deferred until every command has succeeded -- are not specific to either
service, and a second copy of them is a second thing to get wrong.

It lives under ``vq/web/`` for history rather than for architecture: the
console needed it first. It imports nothing from the ``[web]`` extra.

The failure, concretely (reference fleet, 2026-08-05): a console unit was
written by hand, pointed at a hand-staged copy of vq, and then forgotten.
It ran for weeks on code 1081 commits behind the vq installed beside it,
serving a stale version number ten times over as if it were a property of
the fleet. Nothing detected it, because nothing owned it — no rollout
lane, no convergence check, no record anywhere that the unit existed.

The daemon's version of it (2026-09-10): three hosts, three different
ExecStart lines, two of them naming a venv path that outlived the vq it
pointed at. Repointing ``~/.local/bin/vq`` moved the one host whose unit went
through the symlink and left the others running the old vq after a restart,
with their config and their symlink both looking correct.

Three design choices follow from that, and they are the point of this
module:

1. **The unit points at the vq that installed it.** Not at ``vq`` on
   ``$PATH`` (a user unit usually has a different PATH than the shell you
   typed in), and not at a path you typed. :func:`resolve_service_command`
   derives it from the running interpreter, so "which vq owns this
   service" has exactly one answer and it is recorded on disk.

2. **The unit carries no configuration.** The console's bind, port, fleet
   mode, sweep interval and title live in ``[web]``; the daemon's capacity
   caps live in ``[daemon]``. Both are validated by vq, and the ``ExecStart``
   line is just ``vq web run`` or ``vq daemon run``. Changing a setting no
   longer means rewriting a service file, so a re-install cannot silently
   drop a flag -- which it has, repeatedly, for the daemon's caps. See
   :func:`daemon_caps_not_in_config`, which is how an install over a
   hand-written unit refuses rather than dropping them.

3. **The install is recorded.** A provenance marker, named per service by
   :attr:`ServiceKind.marker_name`, records which vq version, from which
   path, wrote which unit, when. ``vq web status`` compares it against
   the vq you are running now and says so when they differ — the check
   that was missing. The daemon's equivalent is ``vq doctor``'s
   ``daemon_rpc`` version, which is what eventually caught its drift.

4. **The unit is not written unless it could run.** Pointing the console at
   *this* vq is only an improvement if this vq can serve the console, and one
   whose venv lacks the ``[web]`` extra cannot. :func:`require_console_runtime`
   makes the same import the service entry point makes, before any file is
   written, and refuses with the remedy for the host it is actually on.

   Its incident (coordinator, 2026-09-09): a console unit installed against a
   venv shared with the queue daemon, which had been rebuilt without uvicorn.
   systemd restarted it 5230 times over two days, each one exiting on the same
   message, none of it visible anywhere but the journal. Installing is the
   moment that is cheapest to catch, and the only one where the operator is
   still at the keyboard.

Platforms: systemd user units (the Linux default), systemd system units
(``--manager systemd-system``, needs root), and launchd user agents (the
macOS default). Everything is idempotent and everything supports
``--dry-run``, which prints the exact files and commands and touches
nothing.
"""
from __future__ import annotations

import contextlib
import json
import os
import plistlib
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from vq import __version__, config, paths

#: Provenance marker, written next to the config so it travels with the
#: host's vq state rather than with the unit file.
INSTALL_MARKER_NAME = "web-console-install.json"

#: Default service name. Overridable so a host can run two consoles (say
#: a fleet console and a single-host sidecar) without a name collision.
DEFAULT_UNIT_NAME = "vq-web"

DAEMON_UNIT_NAME = "vq-daemon"
DAEMON_MARKER_NAME = "daemon-install.json"


@dataclass(frozen=True)
class ServiceKind:
    """One installable vq service: what it runs, and under what names.

    The console was the first, and everything below it turned out to be about
    "a unit that points at this vq and records that it does" rather than about
    the console specifically. The queue daemon needs exactly that and had
    none: across the reference fleet its units disagreed -- one referenced
    ``%h/.local/bin/vq``, two hardcoded a pre-split venv path -- so repointing
    the symlink moved one host and left the other two running the old vq after
    a restart, with their config and their symlink both looking correct. Only
    ``vq doctor``'s ``daemon_rpc`` version revealed it.
    """

    verb: tuple[str, ...]
    unit_name: str
    marker_name: str
    description: str
    command_key: str
    """Key under which the marker records the resolved argv. Distinct per
    service so a marker cannot be read as the wrong one."""
    install_verb: str
    """The command that writes this unit, named in the unit's own header."""
    config_note: str
    """Why this unit carries no configuration, in the unit's own words. The
    reader of a service file is somebody debugging it, and the first thing
    they will want to do is add a flag to ExecStart."""


CONSOLE_SERVICE = ServiceKind(
    verb=("web", "run"),
    unit_name=DEFAULT_UNIT_NAME,
    marker_name=INSTALL_MARKER_NAME,
    description="vq web console",
    command_key="console_command",
    install_verb="vq web install",
    config_note=(
        "Bind, port, fleet mode, sweep interval and title live in the [web]\n"
        "# section of vq's config file, where they are validated and where\n"
        "# `vq web config` can show you which layer set each one."
    ),
)

DAEMON_SERVICE = ServiceKind(
    verb=("daemon", "run"),
    unit_name=DAEMON_UNIT_NAME,
    marker_name=DAEMON_MARKER_NAME,
    description="vq job queue daemon",
    command_key="daemon_command",
    install_verb="vq daemon install",
    config_note=(
        "The capacity caps (--max-jobs, --max-cpus, --max-mem-mb,\n"
        "# --max-scheduler-jobs, --default-job-mem-mb) live in the [daemon]\n"
        "# section of this host's config file, which is what makes them\n"
        "# survive a unit rewrite."
    ),
)


_PORTABLE_UNIT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
_SERVICE_PATH_ENVIRONMENT = (
    config.ENV_CONFIG_DIR,
    paths.ENV_STATE_DIR,
    paths.ENV_MULTI_USER_ROOT,
)


class InstallError(RuntimeError):
    """Raised for any condition an operator must fix before installing."""


@dataclass(frozen=True)
class ServiceAccount:
    """Resolved Unix identity used by a system-level service."""

    name: str
    home: Path
    uid: int
    gids: frozenset[int]


@dataclass(frozen=True)
class FileWrite:
    """One file the plan will create or replace."""

    path: Path
    content: str
    mode: int = 0o644
    #: What this file is for, in one phrase, for the plan preview.
    purpose: str = ""

    def exists_with_same_content(self) -> bool:
        try:
            return self.path.read_text(encoding="utf-8") == self.content
        except OSError:
            return False


@dataclass(frozen=True)
class Command:
    """One command the plan will run, with why."""

    argv: list[str]
    purpose: str = ""
    allow_failure: bool = False
    replace_launchd_target: str | None = None

    def display(self) -> str:
        return " ".join(self.argv)


def _command_failure_is_allowed(command: Command, detail: str) -> bool:
    """Ignore only an expected first-install/already-removed result."""
    if not command.allow_failure:
        return False
    lowered = detail.lower()
    return any(
        phrase in lowered
        for phrase in (
            "no such process",
            "could not find service",
            "service not found",
            "does not exist",
            "not loaded",
        )
    )


@dataclass
class InstallPlan:
    """Everything an install would do, before it does any of it.

    Built first and printed by ``--dry-run`` so the operator sees the
    exact bytes and the exact commands. An install that cannot be
    previewed is an install that gets deployed by guesswork.
    """

    manager: str
    unit_name: str
    writes: list[FileWrite] = field(default_factory=list)
    commands: list[Command] = field(default_factory=list)
    final_writes: list[FileWrite] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def render(self) -> str:
        out: list[str] = [
            f"manager: {self.manager}",
            f"service: {self.unit_name}",
            "",
        ]
        for write in [*self.writes, *self.final_writes]:
            status = (
                "unchanged"
                if write.exists_with_same_content()
                else ("replace" if write.path.exists() else "create")
            )
            out.append(f"--- {status}: {write.path} ({write.purpose})")
            out.append(write.content.rstrip("\n"))
            out.append("")
        for command in self.commands:
            if command.replace_launchd_target is not None:
                out.append(
                    f"$ launchctl print {command.replace_launchd_target}"
                    "      # poll until unloaded; shared 15s bootstrap deadline"
                )
            out.append(f"$ {command.display()}      # {command.purpose}")
        if self.notes:
            out.append("")
            out.extend(f"note: {note}" for note in self.notes)
        return "\n".join(out)


@dataclass
class UninstallPlan:
    """Ordered service removal steps.

    Service-manager disable happens before unlinking the unit; daemon-reload
    happens after. Keeping that order explicit prevents the old implementation
    from reloading systemd before the service file had actually been removed.
    """

    manager: str
    unit_name: str
    commands_before_remove: list[Command] = field(default_factory=list)
    removals: list[Path] = field(default_factory=list)
    commands_after_remove: list[Command] = field(default_factory=list)
    final_removals: list[Path] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            *(f"$ {item.display()}      # {item.purpose}" for item in self.commands_before_remove),
            *(f"rm {path}" for path in self.removals),
            *(f"$ {item.display()}      # {item.purpose}" for item in self.commands_after_remove),
            *(f"rm {path}" for path in self.final_removals),
        ]
        return "\n".join(lines)


def resolve_console_command() -> list[str]:
    """The argv a service unit should use to run this vq's console.

    Deliberately derived from :data:`sys.executable` rather than from
    ``shutil.which("vq")``. A systemd user unit and a launchd agent both
    run with a minimal PATH that rarely contains the venv you installed
    vq into, so a bare ``vq`` resolves to something else or to nothing.
    More importantly, deriving it pins the console to *this* vq — the one
    whose ``vq web install`` you just ran — which is the property that
    makes drift detectable later.

    Prefers the ``vq`` console script beside the interpreter (it carries
    the venv's shebang); falls back to ``<python> -m vq``, which works for
    any install layout including one with no scripts directory.
    """
    return resolve_service_command(CONSOLE_SERVICE)


def console_runtime_import_error() -> str | None:
    """Name of the ``[web]`` dependency ``vq web run`` cannot import, or None.

    The console's own entry point imports ``uvicorn`` and :func:`vq.web.create_app`
    and exits with a ``UsageError`` when either is missing. Everything an
    install decides is derived from :data:`sys.executable`, so the interpreter
    running this check IS the one the unit will name -- an in-process import is
    the same proof the service would make at startup, half a second earlier and
    somewhere an operator can read it.

    Why an install cares (coordinator, 2026-09-09): ``vq web install`` pointed
    ``vq-web.service`` at a venv with no uvicorn in it. Installing succeeded,
    enabling succeeded, starting "succeeded", and the unit then failed on every
    start for two days -- 5230 restarts under ``Restart=on-failure`` -- with an
    error only ``journalctl`` ever showed. The install had all the information
    needed to refuse.
    """
    try:
        import uvicorn  # noqa: F401, PLC0415 - optional dep, probed on demand

        from vq.web import create_app  # noqa: F401, PLC0415
    except ImportError as exc:
        return exc.name or "the 'web' extra"
    return None


def _managed_program_serving_this_vq(
    python: str | None = None,
) -> tuple[str, list[str]] | None:
    """The configured venv program an interpreter belongs to, and its extras.

    ``python`` defaults to this interpreter; ``vq web status`` passes the
    one recorded in the console's install marker instead.

    Used only to word a remedy. On a fleet host the supported fix is a config
    key plus ``vq admin update``; on a laptop checkout it is ``pip``. Telling
    an operator to run pip inside a vq-managed checkout is telling them to do
    the one thing the fleet rules forbid, so the message has to know which
    host it is on.
    """
    try:
        cfg = config.load_config()
    except Exception:  # noqa: BLE001 - a broken config must not mask the remedy
        return None
    try:
        running_bin = Path(python or sys.executable).parent.resolve()
    except OSError:
        return None
    for name, program in cfg.programs.items():
        python = getattr(program, "python", None)
        if not python:
            continue
        try:
            # .parent before .resolve(): bin/python is a symlink out of the
            # venv, and resolving the file first lands in /usr/bin.
            if Path(python).parent.resolve() != running_bin:
                continue
        except OSError:
            continue
        return name, list(getattr(program, "extras", []))
    return None


def _console_runtime_remedy(python: str | None = None) -> str:
    """What to actually do about it, on this host, in its own words."""
    managed = _managed_program_serving_this_vq(python)
    if managed is None:
        return (
            "Install the extra into this environment and re-run:\n"
            "    pip install -e '.[web]'"
        )
    program, extras = managed
    if "web" in extras:
        # Declared but not yet built: the config is already right and the
        # venv has not caught up. Do not send the operator back to edit it.
        return (
            f"The managed program {program!r} already declares the web extra; "
            "its environment has not been rebuilt since. Rebuild it:\n"
            f"    $ vq admin update {program}"
        )
    wanted = ", ".join(f'"{name}"' for name in [*extras, "web"])
    return (
        f"This interpreter is the managed program {program!r}, so install the "
        "extra the way the next rebuild will preserve — declare it, then "
        "update:\n"
        f"    [programs.{program}]\n"
        f"    extras = [{wanted}]\n"
        f"    $ vq admin update {program}"
    )


CONSOLE_RUNTIME_PROBE_TIMEOUT_SECONDS = 30.0
"""How long ``vq web status`` waits for the recorded interpreter to answer."""

_CONSOLE_RUNTIME_MISSING_EXIT = 3
_CONSOLE_RUNTIME_PROBE = (
    "import sys\n"
    "try:\n"
    "    import uvicorn\n"
    "    from vq.web import create_app\n"
    "except ImportError as exc:\n"
    "    print(exc.name or \"the 'web' extra\")\n"
    f"    sys.exit({_CONSOLE_RUNTIME_MISSING_EXIT})\n"
)


@dataclass(frozen=True)
class ConsoleRuntimeVerdict:
    """Whether an installed console's interpreter can import what it runs.

    ``ok`` is None when the probe could not decide, which is not the same as
    broken: a busy host that does not answer in time is reported as unknown.
    """

    ok: bool | None
    detail: str | None = None
    remedy: str | None = None


def probe_console_runtime(python: str) -> ConsoleRuntimeVerdict:
    """Can ``python`` import ``uvicorn`` and :func:`vq.web.create_app`?

    Out of process, and against the interpreter the console unit runs, which
    ``vq web status`` reads from the install marker. After the install that
    may be a different vq from the one asking, so an in-process import would
    answer the wrong question (#28). The probe runs in its own session and the
    whole group is killed on timeout, so a stuck child cannot hold the pipes.
    """
    if not Path(python).exists():
        return ConsoleRuntimeVerdict(
            False,
            f"the console's recorded interpreter {python} does not exist",
            _console_runtime_remedy(python),
        )
    try:
        proc = subprocess.Popen(
            [python, "-c", _CONSOLE_RUNTIME_PROBE],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        return ConsoleRuntimeVerdict(
            False,
            f"the console's recorded interpreter {python} could not start: {exc}",
            _console_runtime_remedy(python),
        )
    try:
        stdout, stderr = proc.communicate(
            timeout=CONSOLE_RUNTIME_PROBE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        with contextlib.suppress(OSError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate()
        return ConsoleRuntimeVerdict(
            None,
            f"the console's recorded interpreter {python} did not answer "
            f"within {CONSOLE_RUNTIME_PROBE_TIMEOUT_SECONDS:g}s",
        )
    if proc.returncode == 0:
        return ConsoleRuntimeVerdict(True)
    if proc.returncode == _CONSOLE_RUNTIME_MISSING_EXIT:
        names = stdout.strip().splitlines()
        missing = names[-1] if names else "the 'web' extra"
        return ConsoleRuntimeVerdict(
            False,
            f"{missing} is missing for {python}",
            _console_runtime_remedy(python),
        )
    tail = (stderr or stdout).strip().splitlines()
    return ConsoleRuntimeVerdict(
        False,
        f"{python} exited {proc.returncode} importing the console: "
        f"{tail[-1] if tail else 'no output'}",
        _console_runtime_remedy(python),
    )


def require_console_runtime() -> None:
    """Refuse to install a console unit this vq could not actually run.

    The console install's first step, ahead of ``--dry-run``: a plan whose
    service cannot start is not a plan worth previewing. It is deliberately
    not inside :func:`build_plan`, which stays a pure planner over unit text
    and is exercised as one -- the precondition belongs to installing, and
    ``vq daemon install`` shares that planner and needs no ``[web]`` extra.
    """
    missing = console_runtime_import_error()
    if missing is None:
        return
    raise InstallError(
        f"this vq cannot serve the console ({missing} is missing), so the "
        "unit would fail on every start.\n"
        f"    {sys.executable}\n"
        f"{_console_runtime_remedy()}"
    )


def resolve_service_command(kind: ServiceKind) -> list[str]:
    """The argv a unit should use to run one of this vq's services."""
    python = Path(sys.executable)
    candidate = python.parent / "vq"
    if candidate.is_file() and os.access(candidate, os.X_OK):
        return [str(candidate), *kind.verb]
    return [str(python), "-m", "vq", *kind.verb]


def _install_marker_path(kind: ServiceKind = CONSOLE_SERVICE) -> Path:
    return paths.config_dir() / kind.marker_name


def read_install_marker(
    kind: ServiceKind = CONSOLE_SERVICE,
) -> dict[str, object] | None:
    """The recorded provenance of the installed service, or None."""
    path = _install_marker_path(kind)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _marker_payload(
    manager: str,
    unit_name: str,
    unit_path: Path,
    kind: ServiceKind = CONSOLE_SERVICE,
) -> str:
    return (
        json.dumps(
            {
                "vq_version": __version__,
                kind.command_key: resolve_service_command(kind),
                "python": sys.executable,
                "manager": manager,
                "unit_name": unit_name,
                "unit_path": str(unit_path),
                "installed_at": datetime.now(UTC).isoformat(),
            },
            indent=2,
        )
        + "\n"
    )


# --------------------------------------------------------------------------
# Service managers
# --------------------------------------------------------------------------


_EXEC_START = re.compile(r"^ExecStart=(?P<argv>.*)$", re.MULTILINE)
_UNIT_CONTINUATION = re.compile(r"\\\n\s*")

DAEMON_CAP_FLAGS = {
    "--max-cpus": "max_cpus",
    "--max-jobs": "max_jobs",
    "--max-scheduler-jobs": "max_scheduler_jobs",
    "--max-mem-mb": "max_mem_mb",
    "--default-job-mem-mb": "default_job_mem_mb",
}
"""``vq daemon run`` caps that a hand-written unit typically hardcodes, and
the ``[daemon]`` config field each one belongs in."""


def unit_exec_start_flags(unit_path: Path) -> list[str]:
    """Cap flags currently baked into an existing unit's ExecStart line.

    A generated unit carries no configuration -- caps live in ``[daemon]``,
    which is what makes them survive a unit rewrite. Installing over a
    hand-written unit therefore *drops* whatever its ExecStart carried, and
    that has happened repeatedly in the field. Reading them back is how the
    install can refuse instead.
    """
    try:
        text = unit_path.read_text(encoding="utf-8")
    except OSError:
        return []
    # systemd continues a directive across lines with a trailing backslash.
    # Reading only the first physical line would miss a flag on the second,
    # and a missed flag is silently dropped -- the outcome this check exists
    # to prevent -- so fold continuations before matching.
    text = _UNIT_CONTINUATION.sub(" ", text)
    found: list[str] = []
    for match in _EXEC_START.finditer(text):
        try:
            # shlex on the raw value, not on a quote-stripped copy: systemd's
            # per-token `"..."` quoting is close enough to POSIX here, and
            # stripping first splits an executable path that contains spaces.
            tokens = shlex.split(match.group("argv"))
        except ValueError:
            # Unbalanced quotes: systemd would reject this unit too. Reading
            # nothing would claim the unit carries no caps, so fall back to
            # whitespace splitting, which cannot miss a flag.
            tokens = match.group("argv").split()
        for token in tokens:
            flag = token.split("=", 1)[0]
            if flag in DAEMON_CAP_FLAGS and flag not in found:
                found.append(flag)
    return found


def daemon_caps_not_in_config(
    unit_path: Path, daemon_config: object,
) -> list[str]:
    """Cap flags an existing unit carries that ``[daemon]`` does not.

    Only the difference matters: a flag already mirrored in the config is
    preserved by the rewrite, because the daemon reads it from there.
    """
    missing: list[str] = []
    for flag in unit_exec_start_flags(unit_path):
        if getattr(daemon_config, DAEMON_CAP_FLAGS[flag], None) is None:
            missing.append(flag)
    return missing


def available_managers() -> list[str]:
    """Managers usable on this host, best first."""
    found: list[str] = []
    if sys.platform == "darwin" and shutil.which("launchctl"):
        found.append("launchd-user")
    if shutil.which("systemctl"):
        found.append("systemd-user")
        found.append("systemd-system")
    return found


def detect_manager() -> str:
    """Pick the manager for this host, or explain why there is none."""
    managers = available_managers()
    if not managers:
        raise InstallError(
            "no supported service manager found (looked for systemctl and "
            "launchctl). Run the console under your own supervisor with:\n"
            f"    {' '.join(resolve_console_command())}"
        )
    return managers[0]


def validate_unit_name(unit_name: str) -> str:
    """Validate the portable service-name subset accepted by every backend."""
    if not _PORTABLE_UNIT_NAME.fullmatch(unit_name):
        raise InstallError(
            "invalid service name: use 1-128 ASCII letters, digits, '_' or '-', "
            "starting with a letter or digit"
        )
    return unit_name


def _systemd_quote(value: str) -> str:
    """Quote one systemd command/environment word without shell semantics."""
    if any(character in value for character in ("\x00", "\n", "\r")):
        raise InstallError("service command contains a control character")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    return f'"{escaped}"'


def _service_account(username: str) -> ServiceAccount:
    """Resolve a non-root account for a system-level web service."""
    try:
        import pwd  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - systemd is Unix-only
        raise InstallError("systemd-system service accounts require a Unix host") from exc
    try:
        account = pwd.getpwnam(username)
    except KeyError as exc:
        raise InstallError(f"system service user {username!r} does not exist") from exc
    if account.pw_uid == 0:
        raise InstallError(
            "the web console must not run as root; choose an unprivileged --service-user"
        )
    home = Path(account.pw_dir)
    if not home.is_absolute():
        raise InstallError(f"system service user {username!r} has no absolute home")
    if not home.is_dir():
        raise InstallError(f"system service user {username!r} has no existing home: {home}")
    try:
        gids = frozenset(os.getgrouplist(account.pw_name, account.pw_gid))
    except (OSError, RuntimeError) as exc:
        raise InstallError(f"cannot resolve groups for system service user {username!r}") from exc
    return ServiceAccount(account.pw_name, home, account.pw_uid, gids)


def _account_mode_bits(path: Path, account: ServiceAccount) -> int:
    """Return owner/group/other rwx bits that apply to ``account``."""
    try:
        metadata = path.stat()
    except OSError as exc:
        raise InstallError(f"system service path is not accessible: {path}: {exc}") from exc
    if metadata.st_uid == account.uid:
        return stat.S_IMODE(metadata.st_mode) >> 6
    if metadata.st_gid in account.gids:
        return (stat.S_IMODE(metadata.st_mode) >> 3) & 0o7
    return stat.S_IMODE(metadata.st_mode) & 0o7


def _require_account_access(
    path: Path,
    account: ServiceAccount,
    *,
    needed: int,
    description: str,
) -> None:
    """Fail early when a generated system unit cannot use a required path."""
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise InstallError(
            f"{description} does not exist or cannot be resolved: {path}: {exc}"
        ) from exc
    for candidate in {path, resolved}:
        for parent in candidate.parents:
            if (_account_mode_bits(parent, account) & 0o1) != 0o1:
                raise InstallError(
                    f"system service user {account.name!r} cannot traverse {parent} "
                    f"to reach {description} {path}"
                )
    if (_account_mode_bits(resolved, account) & needed) != needed:
        raise InstallError(
            f"system service user {account.name!r} lacks required access to "
            f"{description} {path}"
        )


def _service_path_environment() -> dict[str, str]:
    """Capture explicit vq roots that a supervisor would otherwise drop."""
    environment: dict[str, str] = {}
    for name in _SERVICE_PATH_ENVIRONMENT:
        value = os.environ.get(name)
        if value is None:
            continue
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise InstallError(f"{name} must be an absolute path for a supervised service")
        environment[name] = str(path)
    return environment


def _systemd_unit_text(
    argv: list[str],
    *,
    description: str,
    user_unit: bool,
    service_user: str | None = None,
    service_home: Path | None = None,
    environment: dict[str, str] | None = None,
    kind: ServiceKind = CONSOLE_SERVICE,
) -> str:
    exec_start = " ".join(_systemd_quote(item) for item in argv)
    target = "default.target" if user_unit else "multi-user.target"
    service_lines: list[str] = []
    if not user_unit:
        if service_user is None or service_home is None:
            raise InstallError("systemd-system requires an unprivileged service user")
        service_lines.extend(
            [
                f"User={service_user}",
                f"Environment={_systemd_quote(f'HOME={service_home}')}",
            ]
        )
    for name, value in sorted((environment or {}).items()):
        service_lines.append(f"Environment={_systemd_quote(f'{name}={value}')}")
    service_preamble = "\n".join(service_lines)
    if service_preamble:
        service_preamble += "\n"
    return f"""\
# {kind.description} — generated by `{kind.install_verb}` (vq {__version__}).
#
# Do not add configuration flags to ExecStart.
# {kind.config_note}
# A unit that carries its own configuration is a unit that loses a flag the
# next time somebody rewrites it.
#
# ExecStart points at the vq that installed this unit, on purpose: it is what
# makes "which vq owns this service" answerable from disk.
#
# Re-run `{kind.install_verb}` after upgrading vq to re-point and restart.

[Unit]
Description={description}
After=network.target

[Service]
Type=simple
{service_preamble}ExecStart={exec_start}
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy={target}
"""


def _launchd_plist_bytes(
    argv: list[str],
    label: str,
    log_dir: Path,
    environment: dict[str, str] | None = None,
) -> bytes:
    payload: dict[str, object] = {
        "Label": label,
        "ProgramArguments": argv,
        "RunAtLoad": True,
        # Restart on failure, but not on a clean exit — a deliberate stop
        # should stay stopped.
        "KeepAlive": {"SuccessfulExit": False},
        "StandardOutPath": str(log_dir / f"{label}.out"),
        "StandardErrorPath": str(log_dir / f"{label}.err"),
    }
    if environment:
        payload["EnvironmentVariables"] = environment
    return plistlib.dumps(payload)


def _systemd_paths(manager: str, unit_name: str) -> Path:
    if manager == "systemd-user":
        base = Path(
            os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))
        )
        return base / "systemd" / "user" / f"{unit_name}.service"
    return Path("/etc/systemd/system") / f"{unit_name}.service"


def _launchd_label(unit_name: str) -> str:
    """Reverse-DNS label for a launchd agent.

    launchd labels are a flat global namespace per user, so they get a
    reverse-DNS prefix by convention. ``vq-web`` becomes ``com.vq.web``,
    matching the existing ``com.vq.daemon`` agent.
    """
    suffix = unit_name.removeprefix("vq-") or unit_name
    return f"com.vq.{suffix}"


def _launchd_path(label: str) -> Path:
    return Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"


def build_plan(
    *,
    manager: str,
    unit_name: str | None = None,
    description: str | None = None,
    start: bool = True,
    service_user: str | None = None,
    kind: ServiceKind = CONSOLE_SERVICE,
) -> InstallPlan:
    """Everything the install will do, without doing any of it."""
    unit_name = unit_name or kind.unit_name
    description = description or kind.description
    validate_unit_name(unit_name)
    argv = resolve_service_command(kind)
    service_environment = _service_path_environment()
    executable = Path(argv[0])
    if not executable.is_absolute() or not executable.is_file():
        raise InstallError(f"console executable is not a usable absolute file: {executable}")
    plan = InstallPlan(manager=manager, unit_name=unit_name)

    if manager in {"systemd-user", "systemd-system"}:
        user_unit = manager == "systemd-user"
        resolved_account: ServiceAccount | None = None
        service_home: Path | None = None
        if user_unit and service_user is not None:
            raise InstallError("--service-user is only valid with systemd-system")
        if not user_unit:
            missing_roots = [
                name
                for name in (config.ENV_CONFIG_DIR, paths.ENV_STATE_DIR)
                if name not in service_environment
            ]
            if missing_roots:
                names = " and ".join(missing_roots)
                raise InstallError(
                    f"systemd-system requires explicit absolute {names}; "
                    "the root installer and unprivileged service must use the same roots"
                )
            if not service_user:
                raise InstallError(
                    "systemd-system requires --service-user USER; the web "
                    "console is never installed as a root service"
                )
            resolved_account = _service_account(service_user)
            service_home = resolved_account.home
            _require_account_access(
                executable,
                resolved_account,
                needed=0o5,
                description="console executable",
            )
            _require_account_access(
                resolved_account.home,
                resolved_account,
                needed=0o5,
                description="service home",
            )
            for name in (config.ENV_CONFIG_DIR, paths.ENV_STATE_DIR):
                _require_account_access(
                    Path(service_environment[name]),
                    resolved_account,
                    needed=0o7,
                    description=name,
                )
        unit_path = _systemd_paths(manager, unit_name)
        plan.writes.append(
            FileWrite(
                path=unit_path,
                content=_systemd_unit_text(
                    argv,
                    description=description,
                    user_unit=user_unit,
                    service_user=(resolved_account.name if resolved_account else None),
                    service_home=service_home,
                    environment=service_environment,
                    kind=kind,
                ),
                purpose="service unit",
            )
        )
        scope = ["--user"] if user_unit else []
        plan.commands.append(
            Command(["systemctl", *scope, "daemon-reload"], "load the new unit")
        )
        plan.commands.append(
            Command(
                ["systemctl", *scope, "enable", unit_name],
                "start on boot",
            )
        )
        if start:
            plan.commands.append(
                Command(
                    ["systemctl", *scope, "restart", unit_name],
                    "start now (restart is idempotent)",
                )
            )
        if user_unit:
            plan.notes.append(
                "a user unit stops when your last session ends unless "
                "lingering is on: sudo loginctl enable-linger $USER"
            )
        else:
            assert resolved_account is not None
            plan.notes.append(f"system service runs as unprivileged user {resolved_account.name}")
    elif manager == "launchd-user":
        if service_user is not None:
            raise InstallError("--service-user is only valid with systemd-system")
        label = _launchd_label(unit_name)
        plist_path = _launchd_path(label)
        # launchd has no journal: it only redirects stdout/stderr to
        # files, so the console needs somewhere durable to write them.
        # vq's state root is already per-user and already exists.
        log_dir = paths.state_root()
        plan.writes.append(
            FileWrite(
                path=plist_path,
                content=_launchd_plist_bytes(
                    argv,
                    label,
                    log_dir,
                    service_environment,
                ).decode("utf-8"),
                purpose="launchd user agent",
            )
        )
        uid = os.getuid()
        plan.commands.append(
            Command(
                ["launchctl", "bootout", f"gui/{uid}/{label}"],
                "unload any previous copy (fails harmlessly if absent)",
                allow_failure=True,
            )
        )
        plan.commands.append(
            Command(
                ["launchctl", "bootstrap", f"gui/{uid}", str(plist_path)],
                "wait for unload and load the agent (bounded transition retry)",
                replace_launchd_target=f"gui/{uid}/{label}",
            )
        )
        plan.commands.append(
            Command(
                ["launchctl", "enable", f"gui/{uid}/{label}"], "enable at login"
            )
        )
        if start:
            plan.commands.append(
                Command(
                    ["launchctl", "kickstart", "-k", f"gui/{uid}/{label}"],
                    "start now",
                )
            )
    else:
        raise InstallError(
            f"unknown service manager {manager!r}; "
            f"available here: {', '.join(available_managers()) or 'none'}"
        )

    unit_path = plan.writes[0].path
    plan.final_writes.append(
        FileWrite(
            path=_install_marker_path(kind),
            content=_marker_payload(manager, unit_name, unit_path, kind),
            mode=0o600,
            purpose=f"provenance marker (which vq installed this {kind.description})",
        )
    )
    return plan


_LAUNCHD_REPLACE_TIMEOUT_SECONDS = 15.0


def _run_install_command(command: Command) -> subprocess.CompletedProcess[str]:
    """Wait for asynchronous launchd removal before replacing the same label.

    launchd can still return bootstrap EIO immediately after the label has
    disappeared. Retry only that transition error within the same deadline;
    permission, malformed-plist and unknown status errors remain failures.
    """
    if command.replace_launchd_target is None:
        return subprocess.run(
            command.argv, capture_output=True, text=True, timeout=60,
            stdin=subprocess.DEVNULL,
        )
    deadline = time.monotonic() + _LAUNCHD_REPLACE_TIMEOUT_SECONDS
    target = command.replace_launchd_target
    detail = "previous service is still loaded"
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise InstallError(f"launchd replacement timed out for {target}: {detail}")
        status = subprocess.run(
            ["launchctl", "print", target], capture_output=True, text=True,
            timeout=remaining, stdin=subprocess.DEVNULL,
        )
        if status.returncode != 0:
            detail = (status.stderr or status.stdout or f"exit {status.returncode}").strip()
            absent = _command_failure_is_allowed(
                Command([], allow_failure=True), detail,
            )
            if not absent:
                raise InstallError(f"cannot prove launchd service {target} is unloaded: {detail}")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise InstallError(f"launchd replacement timed out for {target}: {detail}")
            proc = subprocess.run(
                command.argv, capture_output=True, text=True, timeout=remaining,
                stdin=subprocess.DEVNULL,
            )
            detail = (proc.stderr or proc.stdout or "").strip()
            if proc.returncode == 0 or not (
                proc.returncode == 5 or "bootstrap failed: 5:" in detail.lower()
            ):
                return proc
        else:
            detail = "previous service is still loaded"
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))


def apply_plan(plan: InstallPlan) -> list[str]:
    """Execute a plan. Returns a human-readable log of what happened.

    A failed manager command is an installation failure. The provenance marker
    is deliberately deferred until every required command succeeds, so status
    can never claim that a half-applied service was installed successfully.
    """
    log: list[str] = []

    def write_file(write: FileWrite) -> None:
        temporary = write.path.with_name(f".{write.path.name}.vq-install-{os.getpid()}")
        try:
            write.path.parent.mkdir(parents=True, exist_ok=True)
            temporary.write_text(write.content, encoding="utf-8")
            os.chmod(temporary, write.mode)
            temporary.replace(write.path)
        except OSError as e:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)
            raise InstallError(f"cannot write {write.path}: {e}") from e
        log.append(f"wrote {write.path}")

    for write in plan.writes:
        write_file(write)

    for command in plan.commands:
        try:
            proc = _run_install_command(command)
        except (OSError, subprocess.SubprocessError) as exc:
            raise InstallError(f"could not run {command.display()}: {exc}") from exc
        if proc.returncode == 0:
            log.append(f"ok: {command.display()}")
            continue
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        first = detail[0] if detail else f"exit {proc.returncode}"
        if _command_failure_is_allowed(command, first):
            log.append(f"skipped: {command.display()} ({first})")
            continue
        raise InstallError(f"command failed: {command.display()}: {first}")
    for write in plan.final_writes:
        write_file(write)
    return log


def build_uninstall_plan(
    *,
    manager: str,
    unit_name: str | None = None,
    purge: bool = False,
    kind: ServiceKind = CONSOLE_SERVICE,
) -> UninstallPlan:
    """Commands to stop/disable the service, and files to remove.

    ``purge`` additionally removes the provenance marker. It is off by
    default so a reinstall can still see what the previous install did.
    """
    unit_name = unit_name or kind.unit_name
    validate_unit_name(unit_name)
    plan = UninstallPlan(manager=manager, unit_name=unit_name)
    if manager in {"systemd-user", "systemd-system"}:
        scope = ["--user"] if manager == "systemd-user" else []
        plan.commands_before_remove.append(
            Command(
                ["systemctl", *scope, "disable", "--now", unit_name],
                "stop and disable",
                allow_failure=True,
            )
        )
        plan.removals.append(_systemd_paths(manager, unit_name))
        plan.commands_after_remove.append(
            Command(["systemctl", *scope, "daemon-reload"], "forget the unit")
        )
    elif manager == "launchd-user":
        label = _launchd_label(unit_name)
        plan.commands_before_remove.append(
            Command(
                ["launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
                "unload",
                allow_failure=True,
            )
        )
        plan.removals.append(_launchd_path(label))
    else:
        raise InstallError(f"unknown service manager {manager!r}")
    if purge:
        plan.final_removals.append(_install_marker_path(kind))
    return plan


def apply_uninstall_plan(plan: UninstallPlan) -> list[str]:
    """Stop, remove, and reload a console service or fail non-zero."""
    log: list[str] = []

    def run(command: Command) -> None:
        try:
            proc = subprocess.run(
                command.argv,
                capture_output=True,
                text=True,
                timeout=60,
                stdin=subprocess.DEVNULL,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise InstallError(f"could not run {command.display()}: {exc}") from exc
        if proc.returncode == 0:
            log.append(f"ok: {command.display()}")
            return
        detail = (proc.stderr or proc.stdout or "").strip().splitlines()
        first = detail[0] if detail else f"exit {proc.returncode}"
        if _command_failure_is_allowed(command, first):
            log.append(f"skipped: {command.display()} ({first})")
            return
        raise InstallError(f"command failed: {command.display()}: {first}")

    for command in plan.commands_before_remove:
        run(command)
    def remove(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            raise InstallError(f"cannot remove {path}: {exc}") from exc
        log.append(f"removed {path}")

    for path in plan.removals:
        remove(path)
    for command in plan.commands_after_remove:
        run(command)
    for path in plan.final_removals:
        remove(path)
    return log


# --------------------------------------------------------------------------
# Status
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class ConsoleServiceStatus:
    """What ``vq web status`` reports."""

    installed: bool
    manager: str | None
    unit_name: str | None
    unit_path: str | None
    installed_by_version: str | None
    installed_at: str | None
    running_version: str
    drifted: bool
    """True when the vq that installed the unit is not the vq running now.

    This is the check whose absence let a console serve 1081-commit-stale
    code for weeks. It compares the *installer's* version against the
    version you are running the status command with — so it fires as soon
    as you upgrade vq and have not re-installed the console."""
    active: bool | None
    """Whether the service manager reports it running. None if unknown."""
    detail: str | None = None
    runtime_ok: bool | None = None
    """Whether the interpreter the console unit runs can import ``uvicorn``
    and ``vq.web.create_app``. None when nothing was probed (no console, the
    daemon service) or the probe could not decide (#28)."""
    runtime_detail: str | None = None
    runtime_remedy: str | None = None


def _service_active(manager: str, unit_name: str) -> bool | None:
    try:
        if manager in {"systemd-user", "systemd-system"}:
            scope = ["--user"] if manager == "systemd-user" else []
            proc = subprocess.run(
                ["systemctl", *scope, "is-active", unit_name],
                capture_output=True,
                text=True,
                timeout=15,
                stdin=subprocess.DEVNULL,
            )
            return proc.stdout.strip() == "active"
        if manager == "launchd-user":
            label = _launchd_label(unit_name)
            proc = subprocess.run(
                ["launchctl", "print", f"gui/{os.getuid()}/{label}"],
                capture_output=True,
                text=True,
                timeout=15,
                stdin=subprocess.DEVNULL,
            )
            return proc.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return None
    return None


def console_service_status(
    kind: ServiceKind = CONSOLE_SERVICE,
) -> ConsoleServiceStatus:
    """Read the provenance marker and check the service. Never raises."""
    marker = read_install_marker(kind)
    if marker is None:
        return ConsoleServiceStatus(
            installed=False,
            manager=None,
            unit_name=None,
            unit_path=None,
            installed_by_version=None,
            installed_at=None,
            running_version=__version__,
            drifted=False,
            active=None,
            detail=(
                f"no {kind.description} service installed by vq on this host "
                f"(or it was installed by hand, before `{kind.install_verb}` "
                f"existed — re-run `{kind.install_verb}` to adopt it)"
            ),
        )
    manager = str(marker.get("manager") or "")
    unit_name = str(marker.get("unit_name") or kind.unit_name)
    installed_by = marker.get("vq_version")
    installed_by_version = (
        str(installed_by) if isinstance(installed_by, str) else None
    )
    runtime = ConsoleRuntimeVerdict(None)
    recorded_python = marker.get("python")
    if (
        kind.marker_name == CONSOLE_SERVICE.marker_name
        and isinstance(recorded_python, str)
        and recorded_python
    ):
        runtime = probe_console_runtime(recorded_python)
    return ConsoleServiceStatus(
        installed=True,
        manager=manager or None,
        unit_name=unit_name,
        unit_path=str(marker.get("unit_path") or "") or None,
        installed_by_version=installed_by_version,
        installed_at=str(marker.get("installed_at") or "") or None,
        running_version=__version__,
        drifted=(
            installed_by_version is not None
            and installed_by_version != __version__
        ),
        active=_service_active(manager, unit_name) if manager else None,
        runtime_ok=runtime.ok,
        runtime_detail=runtime.detail,
        runtime_remedy=runtime.remedy,
    )


# --------------------------------------------------------------------------
# Writing the [web] config section
# --------------------------------------------------------------------------


def render_web_section(
    *,
    bind: str | None = None,
    port: int | None = None,
    fleet: bool | None = None,
    fleet_interval_seconds: int | None = None,
    title: str | None = None,
    public_bind_ack: bool | None = None,
) -> str:
    """A ``[web]`` TOML block for the values the operator actually gave.

    Only the settings passed are emitted: an unset value must keep
    deferring to the built-in default rather than being frozen into the
    file at whatever the default happened to be on install day.
    """
    lines = [
        "",
        "# vq web console. Written by `vq web install`.",
        "# Precedence: CLI flag > environment > this section > default.",
        "# `vq web config` prints the resolved values and their source.",
        "[web]",
    ]
    if bind is not None:
        lines.append(f'bind = "{bind}"')
    if port is not None:
        lines.append(f"port = {port}")
    if fleet is not None:
        lines.append(f"fleet = {str(fleet).lower()}")
    if fleet_interval_seconds is not None:
        lines.append(f"fleet_interval_seconds = {fleet_interval_seconds}")
    if title is not None:
        lines.append(f'title = "{title}"')
    if public_bind_ack is not None:
        lines.append(f"public_bind_ack = {str(public_bind_ack).lower()}")
    return "\n".join(lines) + "\n"


def config_has_web_section(cfg_path: Path | None = None) -> bool:
    """Whether the config file already declares ``[web]``."""
    path = cfg_path or (paths.config_dir() / "config.toml")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return any(
        line.strip() == "[web]" or line.strip().startswith("[web.")
        for line in text.splitlines()
    )


def append_web_section(section: str, cfg_path: Path | None = None) -> Path:
    """Append a ``[web]`` block to the config file, and validate the result.

    Appending rather than rewriting is deliberate: vq's config files are
    hand-maintained and heavily commented, and no TOML writer in the
    stdlib round-trips comments. Appending cannot lose a comment, cannot
    reorder a table, and cannot reformat a value the operator cared about.

    The file is written through a temp-and-rename and the result is
    re-parsed before the rename is kept, so a malformed append can never
    leave the host with a config that vq refuses to load.
    """
    path = cfg_path or (paths.config_dir() / "config.toml")
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = ""
    if path.exists():
        existing = path.read_text(encoding="utf-8")
        if existing and not existing.endswith("\n"):
            existing += "\n"
    candidate = existing + section

    tmp = path.with_suffix(path.suffix + ".vq-web-install.tmp")
    tmp.write_text(candidate, encoding="utf-8")
    try:
        _validate_toml(tmp)
    except Exception as e:
        from pydantic import ValidationError  # noqa: PLC0415

        tmp.unlink(missing_ok=True)
        detail = (
            config.validation_error_summary(e)
            if isinstance(e, ValidationError)
            else str(e)
        )
        raise InstallError(
            f"refusing to write config: the result would not parse ({detail})"
        ) from e
    tmp.replace(path)
    return path


def _validate_toml(path: Path) -> None:
    """Parse-check a candidate config, including vq's own schema.

    Two layers, because they fail differently: ``tomllib`` catches a
    syntax error from the append landing in the middle of another table,
    and the model catches a value the console would reject at startup —
    which is the failure that would otherwise show up as a restart loop
    hours later.
    """
    import tomllib  # noqa: PLC0415 — stdlib

    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    config.WebConsoleConfig.model_validate(raw.get("web") or {})
