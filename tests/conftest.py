"""Shared pytest configuration for the vq test suite."""
from __future__ import annotations

import hashlib
import os
import stat
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

# Make `tests/` importable as a package so cross-test imports like
# `from tests.test_scheduler_dispatch import ...` work regardless of
# the working directory pytest is invoked from.
_tests_dir = Path(__file__).resolve().parent
_tests_parent = str(_tests_dir.parent)
if _tests_parent not in sys.path:
    sys.path.insert(0, _tests_parent)

# A stale editable install was part of the #527 incident: the live receipt
# reported an older vq version than the checkout under test. Put this
# checkout's src tree first before collection imports any test module, then
# verify below that an already-imported foreign copy cannot slip through.
_source_root = (_tests_dir.parent / "src").resolve()
if str(_source_root) not in sys.path:
    sys.path.insert(0, str(_source_root))

_SESSION_ENV_NAMES = (
    "HOME",
    "TMPDIR",
    "XDG_DATA_HOME",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "VQ_STATE_DIR",
    "VQ_CONFIG_DIR",
    "VQ_ARCHIVE_DIR",
    "VQ_MULTI_USER_ROOT",
    "VQ_TEST_SANDBOX_ROOT",
    "VQ_TEST_SHORT_TMPDIR",
    "VQ_TEST_SYSTEM_CONFIG_FILE",
    "VQ_WEB_TOKEN_FILE",
)
_session_sandbox: tempfile.TemporaryDirectory[str] | None = None
_session_short_tmp_link: Path | None = None
_session_original_env: dict[str, str | None] = {}
_session_sentinel_roots: tuple[Path, ...] = ()
_session_sentinel_snapshot: tuple[
    tuple[str, str, int, int, int, int, int, int, str], ...
] = ()


def _tree_snapshot(
    roots: tuple[Path, ...],
) -> tuple[tuple[str, str, int, int, int, int, int, int, str], ...]:
    """Return a deterministic, content-sensitive snapshot of sentinel roots."""
    rows: list[tuple[str, str, int, int, int, int, int, int, str]] = []
    for root_index, root in enumerate(roots):
        for path in sorted((root, *root.rglob("*"))):
            info = path.lstat()
            relative = "." if path == root else path.relative_to(root).as_posix()
            mode = stat.S_IMODE(info.st_mode)
            if path.is_symlink():
                kind, payload = "symlink", os.readlink(path)
            elif path.is_file():
                kind = "file"
                payload = hashlib.sha256(path.read_bytes()).hexdigest()
            elif path.is_dir():
                kind, payload = "directory", ""
            else:
                kind, payload = "other", str(info.st_mode)
            rows.append(
                (
                    f"{root_index}:{relative}",
                    kind,
                    mode,
                    info.st_ino,
                    info.st_uid,
                    info.st_gid,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                    payload,
                )
            )
    return tuple(rows)


def _start_session_sandbox(config: pytest.Config) -> None:
    """Install safe roots before test-module collection begins."""
    global _session_sandbox
    global _session_short_tmp_link
    global _session_original_env
    global _session_sentinel_roots
    global _session_sentinel_snapshot

    if _session_sandbox is not None:
        return
    _session_sandbox = tempfile.TemporaryDirectory(prefix="vq-pytest-session-")
    root = Path(_session_sandbox.name)
    declared_sandbox = os.environ.get("VQ_TEST_SANDBOX_ROOT")
    sandbox_root = (
        Path(declared_sandbox).expanduser().resolve(strict=False)
        if declared_sandbox
        else root.resolve(strict=False)
    )
    resolved_root = root.resolve(strict=False)
    if resolved_root != sandbox_root and sandbox_root not in resolved_root.parents:
        raise pytest.UsageError(
            f"vq pytest session root {resolved_root} is outside declared test "
            f"sandbox {sandbox_root}"
        )
    # Force pytest's own tmp_path tree beneath the same unique capability.
    # This hook runs try-first, before pytest creates TempPathFactory.
    config.option.basetemp = str(root / "pytest-tmp")
    roots = {
        "HOME": root / "home",
        "TMPDIR": root / "tmp",
        "XDG_DATA_HOME": root / "xdg-data",
        "XDG_CONFIG_HOME": root / "xdg-config",
        "XDG_CACHE_HOME": root / "xdg-cache",
        "VQ_STATE_DIR": root / "isolated-state",
        "VQ_CONFIG_DIR": root / "isolated-config",
        "VQ_ARCHIVE_DIR": root / "isolated-archive",
        "VQ_MULTI_USER_ROOT": root / "isolated-multi-user",
        "VQ_TEST_SANDBOX_ROOT": sandbox_root,
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    short_tmp_target = root / "short-tmp"
    short_tmp_target.mkdir()
    short_tmp_link = Path(tempfile.mkdtemp(prefix="vqt-", dir="/tmp"))
    short_tmp_link.rmdir()
    short_tmp_link.symlink_to(short_tmp_target, target_is_directory=True)
    _session_short_tmp_link = short_tmp_link

    # These are deliberately outside VQ_STATE_DIR/VQ_CONFIG_DIR. Any direct
    # fallback to the process's default XDG roots changes the snapshot and
    # makes the complete run fail even if the offending test itself passes.
    sentinel_state = roots["XDG_DATA_HOME"] / "vq"
    sentinel_config = roots["XDG_CONFIG_HOME"] / "vq"
    home_state = roots["HOME"] / ".local" / "share" / "vq"
    home_config = roots["HOME"] / ".config" / "vq"
    _session_sentinel_roots = (
        sentinel_state,
        sentinel_config,
        home_state,
        home_config,
    )
    for index, sentinel in enumerate(_session_sentinel_roots):
        sentinel.mkdir(parents=True)
        (sentinel / "live-root-sentinel").write_bytes(
            f"vq pytest must not change default persistence tree {index}\n".encode()
        )
    _session_sentinel_snapshot = _tree_snapshot(_session_sentinel_roots)

    _session_original_env = {name: os.environ.get(name) for name in _SESSION_ENV_NAMES}
    for name, path in roots.items():
        os.environ[name] = str(path)
    os.environ["VQ_TEST_SHORT_TMPDIR"] = str(short_tmp_link)
    os.environ["VQ_TEST_SYSTEM_CONFIG_FILE"] = str(
        roots["VQ_CONFIG_DIR"] / "system-config.toml"
    )
    os.environ["VQ_WEB_TOKEN_FILE"] = str(
        roots["VQ_CONFIG_DIR"] / "web-token"
    )


def _assert_local_vq_import() -> None:
    """Refuse to test a stale installed vq instead of this checkout."""
    import vq

    module_path = Path(vq.__file__).resolve()
    try:
        module_path.relative_to(_source_root)
    except ValueError as exc:
        raise pytest.UsageError(
            f"refusing foreign vq import {module_path}; expected source under "
            f"{_source_root}. Use this checkout's editable install or PYTHONPATH."
        ) from exc


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers so ``pytest --strict-markers`` (which we
    do not set today, but might in future) wouldn't reject them."""
    _start_session_sandbox(config)
    _assert_local_vq_import()
    config.addinivalue_line(
        "markers",
        "no_autopatch_branch_check: opt OUT of the v0.7.1 autouse "
        "branch-check stub. The test wants to exercise the real "
        "_run_git_branch_check (typically by routing subprocess.run "
        "with its own per-test side_effect).",
    )
    config.addinivalue_line(
        "markers",
        "no_autopatch_build_runner: opt OUT of the v0.12.x autouse "
        "build-runner stub. The test wants to exercise the real "
        "_run_monitored_build (process-group reap / stall / heartbeat) "
        "instead of the subprocess.run-delegating stub.",
    )
    config.addinivalue_line(
        "markers",
        "no_autopatch_self_update_probe: opt OUT of the default inert "
        "daemon self-update probe. The test exercises service-manager "
        "detection directly.",
    )
    config.addinivalue_line(
        "markers",
        "no_autopatch_lifecycle_lock: opt OUT of the default no-op admin "
        "lifecycle-lock handoff. The test exercises the real shared "
        "checkout/venv fcntl locks or inherited file descriptors.",
    )
    config.addinivalue_line(
        "markers",
        "no_autopatch_host_pressure: opt OUT of the autouse host-memory-"
        "pressure pin (#563). The test wants the daemon's host-pressure "
        "watchdog to see a pressure value it injects itself, or the real "
        "host (never in a gate lane).",
    )
    config.addinivalue_line(
        "markers",
        "no_autopatch_ssh_probe: opt OUT of the autouse ssh_probe stub. "
        "The test wants to exercise the real resolve_route / probe_tcp / "
        "verbose_probe (typically by injecting its own runner, or by "
        "probing a local socket it opened itself).",
    )


def pytest_sessionfinish(session: pytest.Session) -> None:
    """Turn any default persistence-tree mutation into a run failure."""
    after = _tree_snapshot(_session_sentinel_roots)
    if after == _session_sentinel_snapshot:
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_sep(
            "=",
            "vq test isolation failure: default persistence sentinel tree changed",
            red=True,
        )
    session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_unconfigure(config: pytest.Config) -> None:
    """Restore the caller environment and remove the session sandbox."""
    global _session_sandbox
    global _session_short_tmp_link
    for name, value in _session_original_env.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value
    if _session_short_tmp_link is not None:
        _session_short_tmp_link.unlink(missing_ok=True)
        _session_short_tmp_link = None
    if _session_sandbox is not None:
        _session_sandbox.cleanup()
        _session_sandbox = None


@pytest.fixture(autouse=True)
def _isolate_vq_state_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[None]:
    """Give every test distinct HOME, XDG, and vq persistence roots.

    The session sandbox protects collection and inherited subprocesses. This
    fixture adds per-test separation so one test cannot consume another's
    state. Default-path semantics are exercised in non-pytest subprocesses;
    there is intentionally no fixture opt-out.
    """
    from vq import admin as _admin
    from vq import config as _config
    from vq import paths

    tmp_path = tmp_path_factory.mktemp("vq-isolation")
    sandbox_root = Path(os.environ["VQ_TEST_SANDBOX_ROOT"]).resolve(strict=False)
    resolved_tmp_path = tmp_path.resolve(strict=False)
    if (
        resolved_tmp_path != sandbox_root
        and sandbox_root not in resolved_tmp_path.parents
    ):
        raise pytest.UsageError(
            f"vq per-test root {resolved_tmp_path} is outside declared "
            f"sandbox {sandbox_root}"
        )
    roots = {
        "HOME": tmp_path / "home",
        "TMPDIR": tmp_path / "tmp",
        "XDG_DATA_HOME": tmp_path / "xdg-data",
        "XDG_CONFIG_HOME": tmp_path / "xdg-config",
        "XDG_CACHE_HOME": tmp_path / "xdg-cache",
        paths.ENV_STATE_DIR: tmp_path / "vq-state",
        _config.ENV_CONFIG_DIR: tmp_path / "vq-config",
        paths.ENV_ARCHIVE_DIR: tmp_path / "vq-archive",
        paths.ENV_MULTI_USER_ROOT: tmp_path / "vq-multi-user",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)
    sentinel_state = roots["XDG_DATA_HOME"] / "vq"
    sentinel_config = roots["XDG_CONFIG_HOME"] / "vq"
    home_state = roots["HOME"] / ".local" / "share" / "vq"
    home_config = roots["HOME"] / ".config" / "vq"
    sentinel_roots = (sentinel_state, sentinel_config, home_state, home_config)
    for index, sentinel in enumerate(sentinel_roots):
        sentinel.mkdir(parents=True)
        (sentinel / "live-root-sentinel").write_bytes(
            f"vq pytest must not change per-test persistence tree {index}\n".encode()
        )
    sentinel_snapshot = _tree_snapshot(sentinel_roots)
    for name, path in roots.items():
        monkeypatch.setenv(name, str(path))
    monkeypatch.setenv(
        _config.ENV_TEST_SYSTEM_CONFIG_FILE,
        str(roots[_config.ENV_CONFIG_DIR] / "system-config.toml"),
    )
    monkeypatch.setenv(
        "VQ_WEB_TOKEN_FILE",
        str(roots[_config.ENV_CONFIG_DIR] / "web-token"),
    )
    _admin._set_owned_admin_update_marker_path(None)
    try:
        yield
    finally:
        _admin._set_owned_admin_update_marker_path(None)
        if _tree_snapshot(sentinel_roots) != sentinel_snapshot:
            pytest.fail(
                "vq test isolation failure: per-test default persistence sentinel "
                "tree changed",
                pytrace=False,
            )


@pytest.fixture(autouse=True)
def _autopatch_host_pressure_reader(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """#563: pin the daemon's host-memory-pressure probe to a quiet host.

    The watchdog's ``_host_pressure_pass`` SIGSTOPs every running job when
    the REAL host crosses ``host_pressure_pause_pct`` (85 %). A daemon test
    that dispatches a local job therefore depended on the memory load of
    whatever else the CI runner was building: on 2026-09-03 the
    release-candidate/v0.15.158 gate failed
    ``test_scoped_admin_update_hold.py::test_local_job_dispatches_during_a_host_f_rebuild``
    with ``assert SUSPENDED == COMPLETED`` while ``build-test`` and
    ``vibe-view-test`` ran beside it (pipeline 4998, job 10057, host
    pressure 85.4 %). Identical tree, green on retry: not a gate.

    Every test now sees a 10 % host unless it opts out with
    ``@pytest.mark.no_autopatch_host_pressure`` and injects its own value
    (``monkeypatch.setattr("vq.watchdog.read_host_memory_pressure_pct", ...)``);
    the daemon resolves the probe through the module at call time so the
    pin reaches it (the old default-argument binding did not).
    """
    if "no_autopatch_host_pressure" in request.keywords:
        return
    monkeypatch.setattr(
        "vq.watchdog.read_host_memory_pressure_pct", lambda: 10.0
    )


@pytest.fixture(autouse=True)
def _autopatch_branch_check(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """v0.7.1 *Lamport's Clock*: stub ``admin._run_git_branch_check``
    to always return ``(0, "main")`` so pre-existing tests with rigid
    ``side_effect=[...]`` lists for ``subprocess.run`` don't fail at
    the new branch-check step that ``_do_update_work`` inserts after
    git pull.

    Why we need this: v0.7.1 added a post-pull branch verification
    (``git rev-parse --abbrev-ref HEAD``) gated on
    ``VenvProgram.branch != None``. Most of the pre-existing
    ``test_admin.py`` tests configure ``branch = "main"`` and mock
    ``subprocess.run`` with a tightly-sized list — pull + script (2
    calls). The branch check would consume a list slot meant for the
    script, fail with empty stdout, mismatch "main", short-circuit,
    and the script call never happens. That manifests as a cascade
    of state-machine / marker / pause-resume regressions.

    Opt out per-test (when you want to exercise the real check
    path) via:

        @pytest.mark.no_autopatch_branch_check
        class TestBranchVerificationCore:
            ...

    The opt-out is what ``test_admin_branch_validation.py`` uses;
    every other test in the suite gets the transparent stub.

    Long term (v0.7.2+): the right fix is converting
    ``test_admin.py``'s tight ``side_effect=[...]`` mocks into
    command-aware routers like the one in
    ``test_admin_branch_validation.py``. The conftest stub keeps
    v0.7.1's diff bounded; the cleanup can land on its own cadence
    without blocking the operator-visibility ship.
    """
    if "no_autopatch_branch_check" in request.keywords:
        return
    # Return ``(None, None)`` rather than ``(0, "main")`` so that
    # ``UpdateResult.branch_verification_attempted`` evaluates to
    # False (it gates on ``branch_check_rc is not None``). That
    # short-circuits the branch gate in ``UpdateResult.success``
    # regardless of which branch the test pinned in its config —
    # vibeqc-dev (main), vibeqc-release (release), or any other.
    # The semantic in tests becomes "branch check didn't run",
    # which is the pre-v0.7.1 contract those tests were written
    # against. Tests that opt out via the marker exercise the
    # real (0, "<branch>") return path.
    monkeypatch.setattr(
        "vq.admin._run_git_branch_check",
        lambda git_dir: (None, None),
    )
    # Immutable selectors now capture HEAD + attachment before any fetch or
    # checkout.  Keep old rigid subprocess fixtures isolated from that new
    # preflight; real-git selector tests already opt out through this marker.
    monkeypatch.setattr(
        "vq.admin._capture_checkout_state",
        lambda git_dir: ("0" * 40, None),
    )
    monkeypatch.setattr(
        "vq.admin._run_git_status_porcelain",
        lambda git_dir: (0, ""),
    )
    monkeypatch.setattr(
        "vq.admin._run_git_resolve_commit",
        lambda git_dir, ref: (0, "0" * 40, ""),
    )
    # Preserve the legacy single-subprocess seam for tightly-sized tag tests.
    # Real immutable-selector tests opt out of this fixture and exercise the
    # named-ref + HEAD comparison used in production.
    monkeypatch.setattr(
        "vq.admin._run_expected_git_tag_check",
        lambda git_dir, expected: _legacy_expected_tag_check(
            git_dir, expected,
        ),
    )


def _legacy_expected_tag_check(git_dir, expected):
    from vq import admin as _admin

    rc, actual = _admin._run_git_tag_check(git_dir)
    return (rc if actual == expected else (rc or 1)), actual


@pytest.fixture(autouse=True)
def _autopatch_self_update_probe(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Keep generic update tests independent of the host service manager.

    Self-update detection now runs before checkout/build work so the update
    script can cooperate with the outer daemon restart.  Generic admin tests
    use tightly-sized ``subprocess.run`` response lists; allowing a Linux
    ``systemctl --user`` probe to consume those entries makes them platform
    dependent.  Dedicated lifecycle tests opt out or override this stub.
    """
    if "no_autopatch_self_update_probe" in request.keywords:
        return
    from vq import admin as _admin

    monkeypatch.setattr(
        _admin,
        "_detect_vq_self_update",
        lambda _prog: _admin._SelfUpdateProbe(
            is_self_update=False,
            daemon_running=False,
            service_manager="systemd",
            manager_available=True,
            diagnostic="test fixture: authoritative distinct daemon venv",
        ),
    )


@pytest.fixture(autouse=True)
def _autopatch_admin_lifecycle_lock(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Keep legacy subprocess fixtures focused on update work.

    The production admin lifecycle now resolves the git top-level before the
    pull and hands two inherited lock descriptors to every configured update
    script.  Older admin tests deliberately mock ``subprocess.run`` with only
    the pull/build responses, so allowing lock discovery to consume those
    responses tests the mock rather than the update contract.  Dedicated lock
    tests opt out and exercise the real cross-language boundary.
    """
    if "no_autopatch_lifecycle_lock" in request.keywords:
        return

    from contextlib import contextmanager

    @contextmanager
    def _unlocked(*args, **kwargs):
        yield None

    monkeypatch.setattr("vq.admin.toolset_lifecycle_lock", _unlocked)
    monkeypatch.setattr(
        "vq.admin._current_toolset_lock_handoff",
        lambda git_dir, target: ({}, ()),
    )


@pytest.fixture(autouse=True)
def _autopatch_local_tag_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep legacy tag-drift tests on their existing tag-check seam."""
    from vq import admin as _admin
    from vq import auto_update as _auto_update

    def _tags(git_dir):
        _rc, tag = _admin._run_git_tag_check(git_dir)
        return ([tag] if tag else []), None

    monkeypatch.setattr(_auto_update, "_local_semver_tags_at_head", _tags)


@pytest.fixture(autouse=True)
def _autopatch_build_runner(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """v0.12.x: stub ``admin._run_monitored_build`` so it delegates to a
    single ``vq.admin.subprocess.run`` call — the same seam the
    pre-v0.12.x ``_run_update_script`` used.

    Why we need this: fix 1 of the 2026-06-26 fleet incident replaced the
    update_script's plain ``subprocess.run`` with a ``Popen``-based
    supervisor (own process group + stall cap + heartbeat). That moved
    real execution off ``subprocess.run``, so the many ``test_admin.py`` /
    ``test_auto_update.py`` tests with rigid ``side_effect=[pull, script]``
    lists would stop intercepting the script step and start really
    spawning ``bash``. The stub keeps those tests routing the script
    result through their existing ``subprocess.run`` mock — exactly the
    transparent-compatibility role :func:`_autopatch_branch_check` plays
    for the v0.7.1 branch check.

    Opt out (to exercise the real supervisor — process-group reap, stall
    detection, heartbeat) via::

        @pytest.mark.no_autopatch_build_runner
        class TestBuildEnvWedgeReaping:
            ...
    """
    if "no_autopatch_build_runner" in request.keywords:
        return
    from vq import admin as _admin

    def _stub(
        argv, *, cwd, env, wall_timeout, stall_timeout,
        heartbeat_interval, log_label, emit=None, pass_fds=(),
    ):
        # Route through the module-level subprocess.run the tests patch,
        # mapping its CompletedProcess (or raised Timeout/OSError) onto a
        # _BuildRunResult exactly as the real supervisor would.
        try:
            proc = _admin.subprocess.run(
                argv, capture_output=True, text=True,
                cwd=cwd, env=env, timeout=wall_timeout,
            )
        except _admin.subprocess.TimeoutExpired:
            return _admin._BuildRunResult(rc=None, output="", timed_out=True)
        except OSError as e:
            return _admin._BuildRunResult(rc=None, output="", error=str(e))
        return _admin._BuildRunResult(
            rc=proc.returncode,
            output=(proc.stdout or "") + (proc.stderr or ""),
        )

    monkeypatch.setattr("vq.admin._run_monitored_build", _stub)


@pytest.fixture(autouse=True)
def _autopatch_ssh_probe(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Keep the ``vq doctor`` local leg off the network and off the
    developer's real ``~/.ssh/config``.

    :func:`vq.ssh_probe.resolve_route` shells out to ``ssh -G``, which reads
    the *developer's own* SSH config: an unstubbed ``doctor host_a`` test would
    resolve the maintainer's real host, then :func:`vq.ssh_probe.probe_tcp`
    would dial it. That makes the outcome depend on whose laptop (and which
    network) the suite runs on, which is the exact non-hermeticity the
    state-isolation fixture above exists to prevent.

    The default stub answers "direct route, first hop reachable", which is the
    pre-local-leg contract every existing doctor test was written against, so
    they keep asserting remote-side behaviour only. Tests that care about the
    local leg monkeypatch these names again with their own fakes (a later
    ``setattr`` wins); tests that want the real implementations opt out with::

        @pytest.mark.no_autopatch_ssh_probe
        class TestRouteResolution:
            ...
    """
    if "no_autopatch_ssh_probe" in request.keywords:
        return
    from vq import ssh_probe

    def _stub_route(destination: str, **_kwargs: object) -> ssh_probe.SshRoute:
        return ssh_probe.SshRoute(
            destination=destination,
            hostname=destination,
            port=22,
            user="",
            proxy_jump=None,
            proxy_command=None,
            identity_files=(),
            first_hop=ssh_probe.Hop("target", destination, 22),
        )

    def _stub_probe(host: str, port: int, **_kwargs: object) -> ssh_probe.TcpProbe:
        return ssh_probe.TcpProbe(host, port, "reachable", "stubbed", 0.0)

    def _stub_verbose(destination: str, **_kwargs: object) -> ssh_probe.VerboseProbe:
        return ssh_probe.VerboseProbe(255, ("stubbed ssh -v transcript",))

    monkeypatch.setattr("vq.ssh_probe.resolve_route", _stub_route)
    monkeypatch.setattr("vq.ssh_probe.probe_tcp", _stub_probe)
    monkeypatch.setattr("vq.ssh_probe.verbose_probe", _stub_verbose)
    monkeypatch.setattr(
        "vq.ssh_probe.control_master_active", lambda destination, **_kw: False
    )
