"""Process-boundary regressions for pytest-to-live-state isolation (#527)."""
from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

import vq
from vq import admin, config, drain, paths, rpc

_VQ_ROOT = Path(__file__).resolve().parents[1]
_SOURCE_ROOT = _VQ_ROOT / "src"
_CONTAMINATION_NODES = (
    "tests/test_admin_managed_durable_recovery.py::"
    "test_verified_success_commits_and_removes_real_backup",
    "tests/test_admin_lifecycle_transaction.py::"
    "test_failed_completion_attempts_recovery_before_resume",
)


def test_conftest_overrides_every_inherited_persistent_path() -> None:
    sandbox = Path(os.environ[paths.ENV_TEST_SANDBOX_ROOT]).resolve()
    assert sandbox != Path(tempfile.gettempdir()).resolve()
    for name in (
        "HOME",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        paths.ENV_ARCHIVE_DIR,
        paths.ENV_MULTI_USER_ROOT,
        config.ENV_TEST_SYSTEM_CONFIG_FILE,
        "VQ_WEB_TOKEN_FILE",
    ):
        resolved = Path(os.environ[name]).expanduser().resolve(strict=False)
        assert resolved == sandbox or sandbox in resolved.parents


def test_system_config_identity_stays_canonical_while_reads_are_sandboxed(
    tmp_path: Path,
) -> None:
    test_system_config = Path(os.environ[config.ENV_TEST_SYSTEM_CONFIG_FILE])
    test_system_config.write_text(
        "[multi_user]\nenabled = true\n",
        encoding="utf-8",
    )

    assert Path("/etc/vq/config.toml") == config.SYSTEM_CONFIG_PATH
    assert config._guarded_system_config_path() == test_system_config
    assert config.system_multi_user_enabled() is True


def _snapshot(
    roots: tuple[Path, ...],
) -> tuple[tuple[str, str, int, int, int, int, int, int, str], ...]:
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


@pytest.mark.parametrize("node", _CONTAMINATION_NODES)
def test_conftestless_contamination_nodes_fail_closed_with_unchanged_xdg(
    node: str,
    tmp_path: Path,
) -> None:
    """Even --noconftest cannot turn a passing test into a live-state write."""
    home = tmp_path / "home"
    xdg_data = tmp_path / "xdg-data"
    xdg_config = tmp_path / "xdg-config"
    tmpdir = tmp_path / "tmp"
    state_sentinel = xdg_data / "vq"
    config_sentinel = xdg_config / "vq"
    for directory in (home, tmpdir, state_sentinel, config_sentinel):
        directory.mkdir(parents=True)
    (state_sentinel / "sentinel").write_bytes(b"unchanged state\n")
    (config_sentinel / "sentinel").write_bytes(b"unchanged config\n")
    sentinel_roots = (state_sentinel, config_sentinel)
    before = _snapshot(sentinel_roots)

    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        paths.ENV_ARCHIVE_DIR,
        paths.ENV_MULTI_USER_ROOT,
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
    ):
        env.pop(name, None)
    env.update(
        {
            "HOME": str(home),
            "TMPDIR": str(tmpdir),
            "XDG_DATA_HOME": str(xdg_data),
            "XDG_CONFIG_HOME": str(xdg_config),
            "XDG_CACHE_HOME": str(tmp_path / "xdg-cache"),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(_SOURCE_ROOT),
        }
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            node,
            "--noconftest",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=_VQ_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.TESTS_FAILED, output
    assert "UnsafeImplicitTestPathError" in output
    assert "refusing implicit per-user vq" in output
    assert _snapshot(sentinel_roots) == before


def test_collection_time_path_resolution_fails_before_touching_xdg(
    tmp_path: Path,
) -> None:
    """Collection selects this checkout and fails before fixtures exist."""
    home = tmp_path / "collection-home"
    xdg_data = tmp_path / "collection-xdg-data"
    xdg_config = tmp_path / "collection-xdg-config"
    state_sentinel = xdg_data / "vq"
    config_sentinel = xdg_config / "vq"
    for directory in (home, state_sentinel, config_sentinel):
        directory.mkdir(parents=True)
    (state_sentinel / "sentinel").write_bytes(b"unchanged state\n")
    (config_sentinel / "sentinel").write_bytes(b"unchanged config\n")
    sentinel_roots = (state_sentinel, config_sentinel)
    before = _snapshot(sentinel_roots)
    collection_test = tmp_path / "test_collection_path.py"
    collection_test.write_text(
        "from vq import paths\n"
        "paths.state_root()\n",
        encoding="utf-8",
    )
    foreign_package = tmp_path / "foreign" / "vq"
    foreign_package.mkdir(parents=True)
    (foreign_package / "__init__.py").write_text(
        "from . import paths\n",
        encoding="utf-8",
    )
    (foreign_package / "paths.py").write_text(
        "import os\n"
        "from pathlib import Path\n"
        "def state_root():\n"
        "    root = Path(os.environ['XDG_DATA_HOME']) / 'vq'\n"
        "    (root / 'foreign-vq-wrote-here').write_text('unsafe\\n')\n"
        "    return root\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
    ):
        env.pop(name, None)
    env.update(
        {
            "HOME": str(home),
            "XDG_DATA_HOME": str(xdg_data),
            "XDG_CONFIG_HOME": str(xdg_config),
            "PYTHONDONTWRITEBYTECODE": "1",
            # A stale installed/editable copy must lose to pyproject.toml's
            # checkout-local pythonpath even when conftest is disabled.
            "PYTHONPATH": str(foreign_package.parent),
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(collection_test),
            "--noconftest",
            "-c",
            str(_VQ_ROOT / "pyproject.toml"),
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=_VQ_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.INTERRUPTED, output
    assert "UnsafeImplicitTestPathError" in output
    assert _snapshot(sentinel_roots) == before


def test_collection_time_plain_child_inherits_session_boundary(
    tmp_path: Path,
) -> None:
    """PYTEST_VERSION protects children spawned before test setup begins."""
    home = tmp_path / "collection-child-home"
    xdg_data = tmp_path / "collection-child-xdg-data"
    xdg_config = tmp_path / "collection-child-xdg-config"
    state_sentinel = xdg_data / "vq"
    config_sentinel = xdg_config / "vq"
    for directory in (home, state_sentinel, config_sentinel):
        directory.mkdir(parents=True)
    (state_sentinel / "sentinel").write_bytes(b"unchanged state\n")
    (config_sentinel / "sentinel").write_bytes(b"unchanged config\n")
    sentinel_roots = (state_sentinel, config_sentinel)
    before = _snapshot(sentinel_roots)
    collection_test = tmp_path / "test_collection_child.py"
    collection_test.write_text(
        "import subprocess\n"
        "import sys\n"
        "result = subprocess.run(\n"
        "    [sys.executable, '-c', \n"
        "     \"from vq import paths; root = paths.state_root(); \"\n"
        "     \"root.mkdir(parents=True, exist_ok=True); \"\n"
        "     \"(root / 'collection-child-write').write_text('unsafe')\"],\n"
        "    capture_output=True, text=True, check=False,\n"
        ")\n"
        "assert result.returncode != 0, result.stderr\n"
        "assert 'UnsafeImplicitTestPathError' in result.stderr\n"
        "def test_placeholder():\n"
        "    pass\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        paths.ENV_TEST_SANDBOX_ROOT,
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
    ):
        env.pop(name, None)
    env.update(
        {
            "HOME": str(home),
            "XDG_DATA_HOME": str(xdg_data),
            "XDG_CONFIG_HOME": str(xdg_config),
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(_SOURCE_ROOT),
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(collection_test),
            "--noconftest",
            "-c",
            str(_VQ_ROOT / "pyproject.toml"),
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=_VQ_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.OK, output
    assert "1 test collected" in output
    assert _snapshot(sentinel_roots) == before


def test_inherited_pytest_marker_keeps_plain_child_fail_closed(
    tmp_path: Path,
) -> None:
    """A non-pytest child remains inside the test isolation boundary."""
    home = tmp_path / "child-home"
    xdg_data = tmp_path / "child-xdg-data"
    xdg_config = tmp_path / "child-xdg-config"
    state_sentinel = xdg_data / "vq"
    config_sentinel = xdg_config / "vq"
    for directory in (home, state_sentinel, config_sentinel):
        directory.mkdir(parents=True)
    (state_sentinel / "sentinel").write_bytes(b"unchanged state\n")
    (config_sentinel / "sentinel").write_bytes(b"unchanged config\n")
    sentinel_roots = (state_sentinel, config_sentinel)
    before = _snapshot(sentinel_roots)

    env = dict(os.environ)
    for name in (
        paths.ENV_STATE_DIR,
        paths.ENV_CONFIG_DIR,
        "PYTEST_ADDOPTS",
    ):
        env.pop(name, None)
    env.update(
        {
            "HOME": str(home),
            "XDG_DATA_HOME": str(xdg_data),
            "XDG_CONFIG_HOME": str(xdg_config),
            "PYTEST_CURRENT_TEST": "outer.py::test_parent (call)",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(_SOURCE_ROOT),
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from vq import paths; paths.state_root()",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "UnsafeImplicitTestPathError" in output
    assert _snapshot(sentinel_roots) == before


def test_explicit_live_override_without_sandbox_capability_fails_closed(
    tmp_path: Path,
) -> None:
    """An operator's exported root cannot authorize a raw pytest child."""
    live_like_root = tmp_path / "operator-vq-state"
    live_like_root.mkdir()
    (live_like_root / "sentinel").write_bytes(b"unchanged\n")
    before = _snapshot((live_like_root,))
    env = dict(os.environ)
    env.pop(paths.ENV_TEST_SANDBOX_ROOT, None)
    env.pop("PYTEST_ADDOPTS", None)
    env.update(
        {
            paths.ENV_STATE_DIR: str(live_like_root),
            "PYTEST_CURRENT_TEST": "outer.py::test_parent (call)",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(_SOURCE_ROOT),
        }
    )
    result = subprocess.run(
        [sys.executable, "-c", "from vq import paths; paths.state_root()"],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )

    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert paths.ENV_TEST_SANDBOX_ROOT in output
    assert _snapshot((live_like_root,)) == before


def test_sentinel_discriminator_fails_a_mutating_nested_run(
    tmp_path: Path,
) -> None:
    """Pin the negative branch: a direct XDG bypass makes pytest exit 1."""
    nested_test = tmp_path / "test_mutate_default_xdg.py"
    nested_test.write_text(
        "import os\n"
        "from pathlib import Path\n"
        "def test_mutate_default_xdg():\n"
        "    root = Path(os.environ['XDG_DATA_HOME']) / 'vq'\n"
        "    (root / 'unexpected-write').write_text('unsafe\\n')\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(nested_test),
            "-c",
            str(_VQ_ROOT / "pyproject.toml"),
            "-p",
            "tests.conftest",
            "-p",
            "no:cacheprovider",
            "-q",
        ],
        cwd=_VQ_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.TESTS_FAILED, output
    assert "per-test default persistence sentinel tree changed" in output


def test_session_sentinel_fails_a_collection_time_mutation(
    tmp_path: Path,
) -> None:
    """Collection writes are caught by pytest_sessionfinish, not fixtures."""
    nested_test = tmp_path / "test_mutate_session_xdg.py"
    nested_test.write_text(
        "import os\n"
        "from pathlib import Path\n"
        "root = Path(os.environ['XDG_CONFIG_HOME']) / 'vq'\n"
        "(root / 'collection-write').write_text('unsafe\\n')\n"
        "def test_body_still_passes():\n"
        "    pass\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("PYTEST_ADDOPTS", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(nested_test),
            "-c",
            str(_VQ_ROOT / "pyproject.toml"),
            "-p",
            "tests.conftest",
            "-p",
            "no:cacheprovider",
            "-q",
        ],
        cwd=_VQ_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )

    output = result.stdout + result.stderr
    assert result.returncode == pytest.ExitCode.TESTS_FAILED, output
    assert "default persistence sentinel tree changed" in output


def test_foreign_vq_import_is_rejected_before_collection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A stale editable install cannot impersonate the selected checkout."""
    from tests import conftest as suite_conftest

    foreign_module = tmp_path / "foreign" / "vq" / "__init__.py"
    monkeypatch.setattr(vq, "__file__", str(foreign_module))

    with pytest.raises(pytest.UsageError, match="refusing foreign vq import"):
        suite_conftest._assert_local_vq_import()


def test_path_consumers_rebind_and_stale_marker_ownership_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Every path is dynamic; the one cached owner cannot cross roots."""
    state_a = tmp_path / "state-a"
    state_b = tmp_path / "state-b"
    config_a = tmp_path / "config-a"
    config_b = tmp_path / "config-b"
    for directory in (state_a, state_b, config_a, config_b):
        directory.mkdir()

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_a))
    monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_a))
    assert paths.state_root() == state_a
    assert paths.config_dir() == config_a
    assert config.config_path() == config_a / "config.toml"
    assert admin.admin_update_marker_path().parent == state_a
    assert admin.admin_status_path().parent == state_a
    assert drain.drain_state_path(multi_user=False).parent == state_a
    assert rpc.socket_path(multi_user=False).parent == state_a

    owned_a = admin.admin_update_marker_path()
    admin._set_owned_admin_update_marker_path(owned_a)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_b))
    monkeypatch.setenv(paths.ENV_CONFIG_DIR, str(config_b))

    assert paths.state_root() == state_b
    assert paths.config_dir() == config_b
    assert config.config_path() == config_b / "config.toml"
    assert admin.admin_update_marker_path().parent == state_b
    assert admin.admin_status_path().parent == state_b
    assert drain.drain_state_path(multi_user=False).parent == state_b
    assert rpc.socket_path(multi_user=False).parent == state_b
    with pytest.raises(admin.AdminError, match="previous state root"):
        admin._owned_admin_update_marker_path()
    admin._set_owned_admin_update_marker_path(None)


@pytest.mark.parametrize(
    ("old_relative", "new_relative"),
    (("root/nested", "root"), ("root", "root/nested")),
)
def test_cached_marker_owner_rejects_ancestor_descendant_root_rebinding(
    old_relative: str,
    new_relative: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    old_root = tmp_path / old_relative
    new_root = tmp_path / new_relative
    old_root.mkdir(parents=True, exist_ok=True)
    new_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(old_root))
    owner = old_root / admin.ADMIN_UPDATE_MARKER_FILENAME
    admin._set_owned_admin_update_marker_path(owner)

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(new_root))
    with pytest.raises(admin.AdminError, match="previous state root"):
        admin._owned_admin_update_marker_path()
    admin._set_owned_admin_update_marker_path(None)


def test_cached_marker_owner_accepts_alias_of_same_canonical_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    root = tmp_path / "state"
    root.mkdir()
    alias = tmp_path / "state-alias"
    alias.symlink_to(root, target_is_directory=True)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(root))
    owner = root / admin.ADMIN_UPDATE_MARKER_FILENAME
    admin._set_owned_admin_update_marker_path(owner)

    monkeypatch.setenv(paths.ENV_STATE_DIR, str(alias))
    assert admin._owned_admin_update_marker_path() == owner
    admin._set_owned_admin_update_marker_path(None)
