"""Single- and multi-user contracts for JobSpec storage primitives.

These tests landed and ran against the pre-extraction implementation first.
They pin physical layout, atomic-write error boundaries, and sidecar-lock
behavior without choosing how duplicate bare job IDs should resolve across
owners.
"""
from __future__ import annotations

import fcntl
import os
import re
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from vq import _storage, config, paths
from vq.spec import JobSpec

_JOB_ID = "a1b2c3d4e5f6"
_UID = 1000


@pytest.fixture
def storage_roots(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[Path, Path, Path]:
    single_root = tmp_path / "single-state"
    multi_root = tmp_path / "multi-state"
    archive_root = tmp_path / "single-archive"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(single_root))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_root))
    monkeypatch.setenv(paths.ENV_ARCHIVE_DIR, str(archive_root))
    return single_root, multi_root, archive_root


@pytest.fixture(params=("single", "multi"))
def layout_spec_path(
    request: pytest.FixtureRequest,
    storage_roots: tuple[Path, Path, Path],
) -> Path:
    if request.param == "single":
        return paths.spec_path(_JOB_ID)
    return paths.user_spec_path(_UID, _JOB_ID)


def _spec(*, command: str = "true") -> JobSpec:
    return JobSpec(
        id=_JOB_ID,
        command=[command],
        cwd="/tmp/vq-storage-contract",
        cpus=1,
    )


def _hold_lock(spec_path: Path) -> int:
    lock_path = paths.spec_lock_path(spec_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o666)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def test_single_user_path_matrix(
    storage_roots: tuple[Path, Path, Path],
) -> None:
    single_root, _multi_root, archive_root = storage_roots

    assert {
        "queue": paths.queue_dir(),
        "spec": paths.spec_path(_JOB_ID),
        "jobs": paths.jobs_dir(),
        "workspace": paths.workspace_dir(_JOB_ID),
        "archive": paths.archive_dir(),
        "archive_file": paths.archive_path(_JOB_ID),
        "workdirs": paths.workdir_root(),
        "workdir": paths.workdir_for(_JOB_ID),
        "lock": paths.spec_lock_path(paths.spec_path(_JOB_ID)),
    } == {
        "queue": single_root / "queue",
        "spec": single_root / "queue" / f"{_JOB_ID}.json",
        "jobs": single_root / "jobs",
        "workspace": single_root / "jobs" / _JOB_ID,
        "archive": archive_root,
        "archive_file": archive_root / f"{_JOB_ID}.tar.bz2",
        "workdirs": single_root / "workdirs",
        "workdir": single_root / "workdirs" / _JOB_ID,
        "lock": single_root / "queue" / f"{_JOB_ID}.json.lock",
    }


@pytest.mark.parametrize("uid", (_UID, str(_UID)))
def test_multi_user_path_matrix(
    uid: int | str,
    storage_roots: tuple[Path, Path, Path],
) -> None:
    _single_root, multi_root, _archive_root = storage_roots
    owner_root = multi_root / "users" / str(_UID)
    spec_path = paths.user_spec_path(uid, _JOB_ID)

    assert {
        "user": paths.user_dir(uid),
        "queue": paths.user_queue_dir(uid),
        "spec": spec_path,
        "jobs": paths.user_jobs_dir(uid),
        "workspace": paths.user_workspace_dir(uid, _JOB_ID),
        "archive": paths.user_archive_dir(uid),
        "archive_file": paths.user_archive_dir(uid) / f"{_JOB_ID}.tar.bz2",
        "workdirs": paths.user_workdir_root(uid),
        "workdir": paths.user_workdir(uid, _JOB_ID),
        "lock": paths.spec_lock_path(spec_path),
    } == {
        "user": owner_root,
        "queue": owner_root / "queue",
        "spec": owner_root / "queue" / f"{_JOB_ID}.json",
        "jobs": owner_root / "jobs",
        "workspace": owner_root / "jobs" / _JOB_ID,
        "archive": owner_root / "archive",
        "archive_file": owner_root / "archive" / f"{_JOB_ID}.tar.bz2",
        "workdirs": owner_root / "workdirs",
        "workdir": owner_root / "workdirs" / _JOB_ID,
        "lock": owner_root / "queue" / f"{_JOB_ID}.json.lock",
    }


def test_same_job_id_has_owner_qualified_lock_paths(
    storage_roots: tuple[Path, Path, Path],
) -> None:
    _ = storage_roots
    first = paths.user_spec_path(1000, _JOB_ID)
    second = paths.user_spec_path(2000, _JOB_ID)

    assert first != second
    assert paths.spec_lock_path(first) != paths.spec_lock_path(second)


def test_known_uid_zero_resolves_without_existing_tree(
    storage_roots: tuple[Path, Path, Path],
) -> None:
    _single_root, multi_root, _archive_root = storage_roots
    assert paths.resolve_spec_path(_JOB_ID, multi_user=True, uid=0) == (
        multi_root / "users" / "0" / "queue" / f"{_JOB_ID}.json"
    )


def test_job_spec_round_trip_is_config_free_and_preserves_modes(
    layout_spec_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unexpected_config_load() -> config.Config:
        raise AssertionError("explicit storage operations must not load config")

    monkeypatch.setattr(config, "load_config", unexpected_config_load)

    initial = _spec()
    initial.write(layout_spec_path)
    assert JobSpec.read(layout_spec_path) == initial
    assert layout_spec_path.stat().st_mode & 0o777 == 0o644
    assert list(layout_spec_path.parent.glob(f"{layout_spec_path.name}.*.tmp")) == []

    layout_spec_path.chmod(0o640)
    replacement = _spec(command="false")
    replacement.write(layout_spec_path)
    assert JobSpec.read(layout_spec_path) == replacement
    assert layout_spec_path.stat().st_mode & 0o777 == 0o640


def test_paths_reexports_the_atomic_writer_identity() -> None:
    assert paths.atomic_write_text is _storage.atomic_write_text


def test_lock_on_absent_spec_creates_only_adjacent_sidecar(
    layout_spec_path: Path,
) -> None:
    lock_path = paths.spec_lock_path(layout_spec_path)
    assert not layout_spec_path.parent.exists()

    with paths.spec_lock(layout_spec_path):
        assert lock_path == layout_spec_path.parent / f"{layout_spec_path.name}.lock"
        assert lock_path.is_file()
        assert not layout_spec_path.exists()


def test_held_lock_times_out_then_can_be_reacquired(layout_spec_path: Path) -> None:
    fd = _hold_lock(layout_spec_path)
    try:
        with (
            pytest.raises(
                paths.SpecLockTimeout,
                match=re.escape(layout_spec_path.name + ".lock"),
            ),
            paths.spec_lock(layout_spec_path, timeout=0),
        ):
            pass
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    with paths.spec_lock(layout_spec_path, timeout=0):
        pass


def test_sidecar_lock_survives_atomic_spec_replacement(
    layout_spec_path: Path,
) -> None:
    initial = _spec()
    initial.write(layout_spec_path)
    fd = _hold_lock(layout_spec_path)
    lock_path = paths.spec_lock_path(layout_spec_path)
    lock_inode = lock_path.stat().st_ino
    try:
        replacement = _spec(command="false")
        replacement.write(layout_spec_path)
        assert JobSpec.read(layout_spec_path) == replacement
        assert lock_path.stat().st_ino == lock_inode
        with (
            pytest.raises(paths.SpecLockTimeout),
            paths.spec_lock(layout_spec_path, timeout=0),
        ):
            pass
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    with paths.spec_lock(layout_spec_path, timeout=0):
        pass


def test_exception_inside_lock_releases_it(layout_spec_path: Path) -> None:
    with (
        pytest.raises(RuntimeError, match="inside lock"),
        paths.spec_lock(layout_spec_path, timeout=0),
    ):
        raise RuntimeError("inside lock")

    with paths.spec_lock(layout_spec_path, timeout=0):
        pass


def test_failed_replace_preserves_spec_and_removes_temp(
    layout_spec_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    initial = _spec()
    initial.write(layout_spec_path)
    real_replace = os.replace

    def fail_replace(source: str, destination: Path) -> None:
        if Path(destination) == layout_spec_path:
            raise OSError("injected replace failure")
        real_replace(source, destination)

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        _spec(command="false").write(layout_spec_path)

    assert JobSpec.read(layout_spec_path) == initial
    assert list(layout_spec_path.parent.iterdir()) == [layout_spec_path]


def test_readonly_lock_fallback_is_permissionerror_only(
    storage_roots: tuple[Path, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = storage_roots
    spec_path = paths.user_spec_path(_UID, _JOB_ID)
    lock_path = paths.spec_lock_path(spec_path)
    lock_path.parent.mkdir(parents=True)
    lock_path.touch()
    real_open = os.open
    seen_flags: list[int] = []

    def controlled_open(path: str, flags: int, mode: int = 0o777) -> int:
        if Path(path) != lock_path:
            return real_open(path, flags, mode)
        seen_flags.append(flags)
        if len(seen_flags) == 1:
            raise PermissionError("injected write denial")
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", controlled_open)
    with paths.spec_lock(spec_path):
        pass

    assert len(seen_flags) == 2
    assert seen_flags[0] & os.O_ACCMODE == os.O_RDWR
    assert seen_flags[0] & os.O_CREAT
    assert seen_flags[1] & os.O_ACCMODE == os.O_RDONLY


def test_nonpermission_lock_open_error_propagates_without_fallback(
    storage_roots: tuple[Path, Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    _ = storage_roots
    spec_path = paths.user_spec_path(_UID, _JOB_ID)
    lock_path = paths.spec_lock_path(spec_path)
    real_open = os.open
    attempts = 0

    def fail_open(path: str, flags: int, mode: int = 0o777) -> int:
        nonlocal attempts
        if Path(path) != lock_path:
            return real_open(path, flags, mode)
        attempts += 1
        raise OSError("injected open failure")

    monkeypatch.setattr(os, "open", fail_open)
    with (
        pytest.raises(OSError, match="injected open failure"),
        paths.spec_lock(spec_path),
    ):
        pass
    assert attempts == 1


def test_nonroot_multi_user_check_does_not_load_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(paths, "_is_root", lambda: False)

    def unexpected_config_load() -> config.Config:
        raise AssertionError("non-root check must short-circuit")

    monkeypatch.setattr(config, "load_config", unexpected_config_load)
    assert paths.is_multi_user() is False


@pytest.mark.parametrize("enabled", (False, True))
def test_root_multi_user_check_uses_config(
    enabled: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(paths, "_is_root", lambda: True)

    def load() -> SimpleNamespace:
        return SimpleNamespace(multi_user=SimpleNamespace(enabled=enabled))

    monkeypatch.setattr(config, "load_config", load)
    assert paths.is_multi_user() is enabled


def test_importing_spec_does_not_import_path_or_config_policy(tmp_path: Path) -> None:
    code = (
        "import sys; import vq.spec; "
        "unexpected = {'vq.config', 'vq.paths'} & sys.modules.keys(); "
        "assert not unexpected, sorted(unexpected)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
