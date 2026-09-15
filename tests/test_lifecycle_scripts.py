"""Regression tests for vq's source-install lifecycle scripts."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import stat
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]
# vibe-queue is its own repository since the 2026-09 split: the project
# directory IS the checkout root, where it used to be one level down inside
# the monorepo. The lifecycle-lock capability is keyed on that exact path.
REPO = PROJECT
SCRIPTS = PROJECT / "scripts"
HELPER = SCRIPTS / "_venv_helpers.sh"
OWNER_MARKER = ".vq-checkout-owner"


def _stamp_owner(venv: Path, project: Path = PROJECT) -> None:
    (venv / OWNER_MARKER).write_text(
        f"version=1\nproject={project.resolve()}\n",
        encoding="utf-8",
    )


def _write_legacy_direct_url(venv: Path, project: Path = PROJECT) -> Path:
    metadata = (
        venv / "lib" / "python3.12" / "site-packages" / "vq-0.0.dist-info" / "direct_url.json"
    )
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(
        json.dumps({"dir_info": {}, "url": project.resolve().as_uri()}),
        encoding="utf-8",
    )
    return metadata


def _write_direct_url(
    venv: Path, *, editable: bool, project: Path = PROJECT,
) -> Path:
    """Write the PEP 610 record pip leaves for a directory install."""
    metadata = (
        venv / "lib" / "python3.12" / "site-packages"
        / "vq-0.26.0.dist-info" / "direct_url.json"
    )
    metadata.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {"url": project.resolve().as_uri()}
    payload["dir_info"] = {"editable": True} if editable else {}
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    return metadata


def _make_venv_shape(venv: Path, *, with_python: bool = True) -> None:
    (venv / "bin").mkdir(parents=True)
    (venv / "pyvenv.cfg").write_text(
        f"home = test\nexecutable = {sys.executable}\n",
        encoding="utf-8",
    )
    if with_python:
        (venv / "bin" / "python").symlink_to(sys.executable)


def _preview_args(script: str, venv: Path, *, adopt: bool = False) -> list[str]:
    common = ["--venv", str(venv), "--dry-run"]
    if script == "install.sh":
        args = ["--force", "--python", sys.executable, *common]
    elif script == "update.sh":
        args = ["--skip-git", "--recreate-venv", "--python", sys.executable, *common]
    elif script == "reinstall.sh":
        args = ["--python", sys.executable, *common]
    else:
        args = ["--yes", *common]
    if adopt:
        args.append("--adopt-legacy")
    return args


def _run_script(
    name: str,
    *args: str,
    env: dict[str, str] | None = None,
    pass_fds: tuple[int, ...] = (),
) -> subprocess.CompletedProcess[str]:
    complete_env = os.environ.copy()
    if env:
        complete_env.update(env)
    return subprocess.run(
        ["bash", str(SCRIPTS / name), *args],
        cwd=REPO,
        env=complete_env,
        capture_output=True,
        text=True,
        check=False,
        pass_fds=pass_fds,
    )


@contextlib.contextmanager
def _admin_lock_capability(
    checkout: Path,
    target: Path,
) -> Iterator[tuple[dict[str, str], tuple[int, int]]]:
    descriptors: list[int] = []
    paths: list[Path] = []
    try:
        for scope, resource in (
            ("checkout", str(checkout)),
            ("target", str(target)),
        ):
            current = Path(resource)
            while not current.exists():
                current = current.parent
            owner_uid = current.lstat().st_uid
            root = Path(f"/tmp/vibe-toolset-lifecycle-locks-{owner_uid}")
            root.mkdir(mode=0o700, exist_ok=True)
            root.chmod(0o700)
            digest = hashlib.sha256(f"{scope}:{resource}".encode()).hexdigest()
            path = root / f"{scope}-{digest}.lock"
            fd = os.open(
                path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            paths.append(path)
            descriptors.append(fd)
        yield (
            {
                "VIBE_TOOLSET_ADMIN_LOCK_PID": str(os.getpid()),
                "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT": str(checkout),
                "VIBE_TOOLSET_ADMIN_LOCK_TARGET": str(target),
                "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT_PATH": str(paths[0]),
                "VIBE_TOOLSET_ADMIN_LOCK_TARGET_PATH": str(paths[1]),
                "VIBE_TOOLSET_ADMIN_LOCK_CHECKOUT_FD": str(descriptors[0]),
                "VIBE_TOOLSET_ADMIN_LOCK_TARGET_FD": str(descriptors[1]),
                "VIBE_TOOLSET_ADMIN_LOCK_PYTHON": str(Path(sys.executable).resolve()),
            },
            (descriptors[0], descriptors[1]),
        )
    finally:
        for fd in descriptors:
            os.close(fd)


def _run_helper(body: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "-c", f'set -euo pipefail; . "{HELPER}"; {body}', "_", *args],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    ("script", "option"),
    [
        ("install.sh", "--extras"),
        ("install.sh", "--python"),
        ("install.sh", "--venv"),
        ("update.sh", "--branch"),
        ("update.sh", "--ref"),
        ("update.sh", "--extras"),
        ("update.sh", "--python"),
        ("update.sh", "--venv"),
        ("reinstall.sh", "--extras"),
        ("reinstall.sh", "--python"),
        ("reinstall.sh", "--venv"),
        ("uninstall.sh", "--venv"),
    ],
)
@pytest.mark.parametrize(
    ("value", "message"),
    [
        (None, "requires a non-empty argument"),
        ("", "requires a non-empty argument"),
        ("--dry-run", "option-like argument"),
    ],
)
def test_value_taking_options_fail_closed(
    script: str,
    option: str,
    value: str | None,
    message: str,
) -> None:
    args = (option,) if value is None else (option, value)

    proc = _run_script(script, *args)

    assert proc.returncode != 0
    assert message in proc.stderr


@pytest.mark.parametrize("bad_value", ["", "--dry-run"])
def test_explicit_empty_or_option_like_venv_never_falls_back_to_environment(
    tmp_path: Path,
    bad_value: str,
) -> None:
    home = tmp_path / "home"
    venv = tmp_path / "fallback-venv"
    home.mkdir()
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").symlink_to(sys.executable)
    (venv / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
    sentinel = venv / "must-survive"
    sentinel.write_text("still here\n", encoding="utf-8")

    proc = _run_script(
        "uninstall.sh",
        "--venv",
        bad_value,
        "--yes",
        env={"HOME": str(home), "VQ_VENV": str(venv)},
    )

    assert proc.returncode != 0
    assert sentinel.read_text(encoding="utf-8") == "still here\n"


@pytest.mark.parametrize(
    ("script", "args", "message"),
    [
        (
            "update.sh",
            ("--skip-git", "--python", "python3", "--dry-run"),
            "--python is only used with --recreate-venv",
        ),
        (
            "reinstall.sh",
            ("--keep-venv", "--python", "python3", "--dry-run"),
            "--python cannot be used with --keep-venv",
        ),
        (
            "uninstall.sh",
            ("--force", "--dry-run"),
            "--force is only meaningful with --purge-state",
        ),
    ],
)
def test_mode_specific_options_are_not_silently_ignored(
    script: str,
    args: tuple[str, ...],
    message: str,
) -> None:
    proc = _run_script(script, *args)

    assert proc.returncode != 0
    assert message in proc.stderr


@pytest.mark.parametrize(
    "script",
    ["install.sh", "update.sh", "reinstall.sh", "uninstall.sh"],
)
def test_adopt_legacy_flag_is_documented_symmetrically(script: str) -> None:
    proc = _run_script(script, "--help")

    assert proc.returncode == 0
    assert "--adopt-legacy" in proc.stdout
    assert "PEP 610" in proc.stdout


@pytest.mark.parametrize(
    "script",
    ["install.sh", "update.sh", "reinstall.sh", "uninstall.sh"],
)
@pytest.mark.parametrize("marker_kind", ["missing", "foreign", "malformed", "symlink"])
def test_mutating_scripts_reject_unowned_or_invalid_ownership_markers(
    tmp_path: Path,
    script: str,
    marker_kind: str,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    marker = venv / OWNER_MARKER
    if marker_kind == "foreign":
        _stamp_owner(venv, tmp_path / "another-checkout")
    elif marker_kind == "malformed":
        marker.write_text("version=1\nproject\n", encoding="utf-8")
    elif marker_kind == "symlink":
        payload = tmp_path / "marker-payload"
        payload.write_text(
            f"version=1\nproject={PROJECT.resolve()}\n",
            encoding="utf-8",
        )
        marker.symlink_to(payload)
    sentinel = venv / "must-survive"
    sentinel.write_text("still here\n", encoding="utf-8")

    proc = _run_script(script, *_preview_args(script, venv))

    assert proc.returncode != 0
    assert sentinel.read_text(encoding="utf-8") == "still here\n"
    if marker_kind == "missing":
        assert "unowned virtualenv" in proc.stderr
    elif marker_kind == "foreign":
        assert "belongs to another checkout" in proc.stderr
    else:
        assert "malformed or symlinked" in proc.stderr


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_lifecycle_mutator_refuses_a_marked_immutable_generation_before_mutation(
    tmp_path: Path,
    script: str,
) -> None:
    venv = tmp_path / "immutable-venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)
    sentinel = venv / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    (venv / ".vq-immutable-runtime").write_text(
        json.dumps(
            {
                "schema": 1,
                "kind": "vq-multi-user-generation",
                "id": "a" * 40,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    proc = _run_script(script, *_preview_args(script, venv))

    assert proc.returncode != 0
    assert "refusing to mutate an immutable runtime generation" in proc.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_lifecycle_mutator_refuses_a_symlinked_immutable_marker(
    tmp_path: Path,
    script: str,
) -> None:
    venv = tmp_path / "immutable-venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)
    payload = tmp_path / "marker-payload"
    payload.write_text("{}\n", encoding="utf-8")
    (venv / ".vq-immutable-runtime").symlink_to(payload)

    proc = _run_script(script, *_preview_args(script, venv))

    assert proc.returncode != 0
    assert "marker is not a regular file" in proc.stderr


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_direct_mutator_refuses_an_unmarked_runtime_slot(
    tmp_path: Path,
    script: str,
) -> None:
    sha = "b" * 40
    venv = tmp_path / "runtime" / "releases" / sha / "source" / ".venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)

    proc = _run_script(script, *_preview_args(script, venv))

    assert proc.returncode != 0
    assert "refusing direct in-place mutation of runtime slot" in proc.stderr


def test_exact_admin_handoff_may_populate_a_new_runtime_slot(
    tmp_path: Path,
) -> None:
    sha = "c" * 40
    generation = tmp_path / "runtime" / "releases" / sha
    generation.mkdir(parents=True)
    state = generation / ".vq-runtime-slot-state"
    state.write_text(
        json.dumps(
            {
                "schema": 1,
                "kind": "vq-runtime-slot-build",
                "id": sha,
                "transaction": "d" * 32,
                "state": "building",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    venv = generation / "source" / ".venv"

    with _admin_lock_capability(REPO, venv) as (environment, descriptors):
        proc = _run_script(
            "update.sh",
            "--skip-git",
            "--recreate-venv",
            "--python",
            sys.executable,
            "--venv",
            str(venv),
            "--dry-run",
            env=environment,
            pass_fds=descriptors,
        )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "would create" in proc.stdout


@pytest.mark.parametrize("marked", [False, True])
def test_durable_helper_cannot_bypass_runtime_slot_immutability(
    tmp_path: Path,
    marked: bool,
) -> None:
    sha = "e" * 40
    venv = tmp_path / "runtime" / "releases" / sha / "source" / ".venv"
    _make_venv_shape(venv)
    if marked:
        (venv / ".vq-immutable-runtime").write_text("sealed\n", encoding="utf-8")
    sentinel = venv / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")

    proc = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "_venv_transaction.py"),
            "begin",
            str(REPO),
            str(venv),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert proc.returncode != 0
    assert "runtime" in proc.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert not _direct_replacement_receipt(venv).exists()


@pytest.mark.parametrize(
    "script",
    ["install.sh", "update.sh", "reinstall.sh", "uninstall.sh"],
)
def test_exact_pep610_legacy_adoption_previews_without_writing(
    tmp_path: Path,
    script: str,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_legacy_direct_url(venv)

    proc = _run_script(script, *_preview_args(script, venv, adopt=True))

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not (venv / OWNER_MARKER).exists()


def test_exact_pep610_legacy_adoption_accepts_lib64_alias(
    tmp_path: Path,
) -> None:
    """A normal Linux venv exposes the same metadata via lib and lib64."""
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_legacy_direct_url(venv)
    (venv / "lib64").symlink_to("lib", target_is_directory=True)

    proc = _run_script(
        "reinstall.sh",
        *_preview_args("reinstall.sh", venv, adopt=True),
        "--extras",
        "core",
        "--editable",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "capabilities: core" in proc.stdout
    assert "mode:         editable" in proc.stdout
    assert not (venv / OWNER_MARKER).exists()


def test_legacy_adoption_rejects_lib64_alias_to_independent_metadata(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_legacy_direct_url(venv)
    other_lib = tmp_path / "other-lib"
    other_metadata = (
        other_lib
        / "python3.12"
        / "site-packages"
        / "vq-0.0.dist-info"
        / "direct_url.json"
    )
    other_metadata.parent.mkdir(parents=True)
    other_metadata.write_text(
        json.dumps({"dir_info": {}, "url": PROJECT.resolve().as_uri()}),
        encoding="utf-8",
    )
    (venv / "lib64").symlink_to(other_lib, target_is_directory=True)

    proc = _run_script(
        "reinstall.sh",
        *_preview_args("reinstall.sh", venv, adopt=True),
    )

    assert proc.returncode != 0
    assert "does not prove this venv came from" in proc.stderr
    assert not (venv / OWNER_MARKER).exists()


def test_legacy_adoption_rejects_independent_lib_and_lib64_metadata(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_legacy_direct_url(venv)
    lib64_metadata = (
        venv
        / "lib64"
        / "python3.12"
        / "site-packages"
        / "vq-0.0.dist-info"
        / "direct_url.json"
    )
    lib64_metadata.parent.mkdir(parents=True)
    lib64_metadata.write_text(
        json.dumps({"dir_info": {}, "url": PROJECT.resolve().as_uri()}),
        encoding="utf-8",
    )

    proc = _run_script(
        "reinstall.sh",
        *_preview_args("reinstall.sh", venv, adopt=True),
    )

    assert proc.returncode != 0
    assert "does not prove this venv came from" in proc.stderr
    assert not (venv / OWNER_MARKER).exists()


@pytest.mark.parametrize("pep610_kind", ["foreign", "malformed", "symlinked"])
def test_legacy_adoption_requires_exact_regular_pep610_metadata(
    tmp_path: Path,
    pep610_kind: str,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    metadata = _write_legacy_direct_url(venv)
    if pep610_kind == "foreign":
        metadata.write_text(
            json.dumps({"url": (tmp_path / "foreign").resolve().as_uri()}),
            encoding="utf-8",
        )
    elif pep610_kind == "malformed":
        metadata.write_text("not-json\n", encoding="utf-8")
    else:
        payload = tmp_path / "direct-url-payload"
        payload.write_text(
            json.dumps({"url": PROJECT.resolve().as_uri()}),
            encoding="utf-8",
        )
        metadata.unlink()
        metadata.symlink_to(payload)

    proc = _run_script(
        "reinstall.sh",
        *_preview_args("reinstall.sh", venv, adopt=True),
    )

    assert proc.returncode != 0
    assert "does not prove this venv came from" in proc.stderr
    assert not (venv / OWNER_MARKER).exists()


def test_legacy_adoption_stamps_and_verifies_the_checkout_marker(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_legacy_direct_url(venv)
    proc = _run_helper(
        'vq_require_venv_ownership "$1" 1; vq_ownership_marker_status "$1"',
        str(venv),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert (venv / OWNER_MARKER).read_text(encoding="utf-8") == (
        f"version=1\nproject={PROJECT.resolve()}\n"
    )


def test_foreign_dry_run_never_executes_target_python_or_vq(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "foreign-venv"
    (venv / "bin").mkdir(parents=True)
    sentinel = tmp_path / "executed"
    payload = f'#!/bin/sh\nprintf ran >> "{sentinel!s}"\nexit 0\n'
    for executable in ("python", "vq"):
        path = venv / "bin" / executable
        path.write_text(payload, encoding="utf-8")
        path.chmod(0o755)
    (venv / "pyvenv.cfg").write_text(
        f"home = foreign\nexecutable = {sys.executable}\n",
        encoding="utf-8",
    )

    proc = _run_script(
        "install.sh",
        "--force",
        "--python",
        str(venv / "bin" / "python"),
        "--venv",
        str(venv),
        "--dry-run",
    )

    assert proc.returncode != 0
    assert "unowned virtualenv" in proc.stderr
    assert not sentinel.exists()


def test_adoption_with_malicious_target_binaries_uses_external_inspection_only(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "legacy-venv"
    (venv / "bin").mkdir(parents=True)
    sentinel = tmp_path / "executed"
    payload = f'#!/bin/sh\nprintf ran >> "{sentinel!s}"\nexit 0\n'
    for executable in ("python", "vq"):
        path = venv / "bin" / executable
        path.write_text(payload, encoding="utf-8")
        path.chmod(0o755)
    (venv / "pyvenv.cfg").write_text(
        f"home = foreign\nexecutable = {sys.executable}\n",
        encoding="utf-8",
    )
    _write_legacy_direct_url(venv)

    proc = _run_script(
        "install.sh",
        "--force",
        "--adopt-legacy",
        "--python",
        str(venv / "bin" / "python"),
        "--venv",
        str(venv),
        "--dry-run",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not sentinel.exists()
    assert not (venv / OWNER_MARKER).exists()


@pytest.mark.parametrize(
    ("flag", "environment"),
    [
        ("--purge-state", {"VQ_STATE_DIR": str(REPO)}),
        ("--purge-config", {"VQ_CONFIG_DIR": str(REPO)}),
        ("--purge-state", {"VQ_STATE_DIR": str(PROJECT)}),
    ],
)
def test_uninstall_refuses_to_purge_the_checkout(
    tmp_path: Path,
    flag: str,
    environment: dict[str, str],
) -> None:
    environment.update(
        {
            "HOME": str(tmp_path / "home"),
            "VQ_STATE_DIR": environment.get("VQ_STATE_DIR", str(tmp_path / "state")),
            "VQ_CONFIG_DIR": environment.get("VQ_CONFIG_DIR", str(tmp_path / "config")),
        }
    )
    (tmp_path / "home").mkdir()

    proc = _run_script("uninstall.sh", "--keep-venv", flag, "--yes", "--dry-run", env=environment)

    assert proc.returncode != 0
    assert "vibe-qc checkout" in proc.stderr


@pytest.mark.parametrize("target", ["relative-state", "/var/tmp", "/var/lib/vq"])
def test_uninstall_refuses_relative_or_broad_purge_targets(
    tmp_path: Path,
    target: str,
) -> None:
    home = tmp_path / "home"
    home.mkdir()

    proc = _run_script(
        "uninstall.sh",
        "--keep-venv",
        "--purge-state",
        "--yes",
        "--dry-run",
        env={
            "HOME": str(home),
            "VQ_STATE_DIR": target,
            "VQ_CONFIG_DIR": str(tmp_path / "config"),
        },
    )

    assert proc.returncode != 0
    assert "refusing" in proc.stderr


def test_uninstall_purges_only_explicit_safe_custom_directories(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    state = tmp_path / "runtime" / "state"
    config = tmp_path / "runtime" / "config"
    venv = tmp_path / "runtime" / "venv"
    home.mkdir()
    state.mkdir(parents=True)
    config.mkdir(parents=True)
    _make_venv_shape(venv)
    _stamp_owner(venv)
    (state / "history.json").write_text("{}\n", encoding="utf-8")
    (config / "config.toml").write_text("# test\n", encoding="utf-8")

    proc = _run_script(
        "uninstall.sh",
        "--keep-venv",
        "--venv",
        str(venv),
        "--all",
        "--yes",
        env={
            "HOME": str(home),
            "VQ_STATE_DIR": str(state),
            "VQ_CONFIG_DIR": str(config),
        },
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not state.exists()
    assert not config.exists()
    assert venv.is_dir()
    assert home.exists()


def test_uninstall_never_purges_state_under_a_live_daemon(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    state = tmp_path / "runtime" / "state"
    config = tmp_path / "runtime" / "config"
    home.mkdir()
    state.mkdir(parents=True)
    config.mkdir(parents=True)
    sleeper = subprocess.Popen(["sleep", "30"])
    try:
        (state / "daemon.pid").write_text(f"{sleeper.pid}\n", encoding="utf-8")
        proc = _run_script(
            "uninstall.sh",
            "--keep-venv",
            "--purge-state",
            "--force",
            "--yes",
            env={
                "HOME": str(home),
                "VQ_STATE_DIR": str(state),
                "VQ_CONFIG_DIR": str(config),
            },
        )
    finally:
        sleeper.terminate()
        sleeper.wait(timeout=5)

    assert proc.returncode != 0
    assert "daemon pid" in proc.stderr
    assert state.exists()


def test_uninstall_removes_marker_owned_venv_with_missing_python(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    venv = tmp_path / "broken-venv"
    home.mkdir()
    _make_venv_shape(venv, with_python=False)
    _stamp_owner(venv)

    proc = _run_script(
        "uninstall.sh",
        "--venv",
        str(venv),
        "--yes",
        env={
            "HOME": str(home),
            "VQ_STATE_DIR": str(tmp_path / "runtime" / "state"),
            "VQ_CONFIG_DIR": str(tmp_path / "runtime" / "config"),
        },
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not venv.exists()


def test_uninstall_adopts_and_removes_legacy_venv_with_missing_python(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    venv = tmp_path / "broken-legacy-venv"
    home.mkdir()
    _make_venv_shape(venv, with_python=False)
    _write_legacy_direct_url(venv)

    proc = _run_script(
        "uninstall.sh",
        "--venv",
        str(venv),
        "--adopt-legacy",
        "--yes",
        env={
            "HOME": str(home),
            "VQ_STATE_DIR": str(tmp_path / "runtime" / "state"),
            "VQ_CONFIG_DIR": str(tmp_path / "runtime" / "config"),
        },
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert not venv.exists()


@pytest.mark.parametrize(
    ("helper", "target", "message"),
    [
        ("vq_assert_safe_venv_target", "/usr/local/share/vq-env", "protected system"),
        ("vq_assert_safe_venv_target", "/etc/some-app/vq-env", "protected system"),
        ("vq_assert_safe_data_target", "/usr/local/share/vq-state", "protected system"),
        ("vq_assert_safe_data_target", "/etc/some-app/vq-state", "protected system"),
    ],
)
def test_missing_system_descendants_are_refused(
    helper: str,
    target: str,
    message: str,
) -> None:
    body = f'{helper} "$1" state' if helper == "vq_assert_safe_data_target" else f'{helper} "$1"'
    proc = _run_helper(body, target)

    assert proc.returncode != 0
    assert message in proc.stderr


def test_fedora_atomic_current_owned_home_descendant_is_allowed(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    proc = _run_helper(
        'HOME="$1"; export HOME; '
        "cd() { return 0; }; "
        "pwd() { printf '/var/home/USER\\n'; }; "
        "stat() { id -u; }; "
        "vq_assert_not_protected_system_target "
        '"/var/home/USER/project/.venv" virtualenv',
        str(home),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr


@pytest.mark.parametrize(
    ("physical_home", "target", "owner"),
    [
        ("/var/home/USER", "/var/home/USER/project/.venv", "999999"),
        # Synthetic foreign-owner path; preserve the runtime fixture value.
        ("/var/home/USER", "/var/" "home/OTHER_USER/project/.venv", "current"),
        ("/etc", "/etc/project/.venv", "current"),
    ],
)
def test_atomic_home_exception_rejects_unowned_foreign_or_system_homes(
    tmp_path: Path,
    physical_home: str,
    target: str,
    owner: str,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    stat_body = "id -u" if owner == "current" else f"printf '{owner}\\n'"
    proc = _run_helper(
        'HOME="$1"; export HOME; '
        'physical_home="$2"; '
        "cd() { return 0; }; "
        "pwd() { printf '%s\\n' \"$physical_home\"; }; "
        f"stat() {{ {stat_body}; }}; "
        'vq_assert_not_protected_system_target "$3" virtualenv',
        str(home),
        physical_home,
        target,
    )

    assert proc.returncode != 0
    assert "protected system" in proc.stderr


@pytest.mark.parametrize(
    ("helper", "target"),
    [
        ("vq_assert_safe_venv_target", "/usr/local"),
        ("vq_assert_safe_data_target", "/usr/local"),
    ],
)
def test_existing_system_descendants_are_refused(helper: str, target: str) -> None:
    if not Path(target).exists():
        pytest.skip(f"platform has no {target}")
    body = f'{helper} "$1"' if helper.endswith("venv_target") else f'{helper} "$1" state'
    proc = _run_helper(body, target)

    assert proc.returncode != 0
    assert "protected system" in proc.stderr


@pytest.mark.parametrize(
    ("helper", "target"),
    [
        ("vq_assert_safe_venv_target", "/tmp/missing/../../etc/vq-env"),
        ("vq_assert_safe_data_target", "/tmp/missing/../../etc/vq-state"),
    ],
)
def test_missing_targets_cannot_traverse_into_system_paths(
    helper: str,
    target: str,
) -> None:
    body = f'{helper} "$1"' if helper.endswith("venv_target") else f'{helper} "$1" state'
    proc = _run_helper(body, target)

    assert proc.returncode != 0
    assert "'..' path component" in proc.stderr


@pytest.mark.parametrize(
    ("helper", "kind"),
    [
        ("vq_assert_safe_venv_target", "venv"),
        ("vq_assert_safe_data_target", "state"),
        ("vq_assert_safe_data_target", "config"),
    ],
)
@pytest.mark.parametrize("destination", ("git_metadata", "protected"))
def test_missing_suffix_through_symlinked_ancestor_is_safety_checked_physically(
    tmp_path: Path,
    helper: str,
    kind: str,
    destination: str,
) -> None:
    link = tmp_path / f"{destination}-alias"
    protected_destination = REPO / ".git" if destination == "git_metadata" else Path("/etc")
    if destination == "git_metadata" and not protected_destination.is_dir():
        pytest.skip("checkout uses a non-directory Git metadata indirection")
    link.symlink_to(protected_destination, target_is_directory=True)
    missing_parent = f"missing-{tmp_path.name}-{kind}-parent"
    target = link / missing_parent / f"vq-{kind}"
    body = f'{helper} "$1" {kind}' if helper.endswith("data_target") else f'{helper} "$1"'

    proc = _run_helper(body, str(target))

    assert proc.returncode != 0
    if destination == "git_metadata":
        assert "Git metadata" in proc.stderr
    else:
        assert "protected system" in proc.stderr
    assert not (protected_destination / missing_parent).exists()


@pytest.mark.parametrize(
    ("helper", "kind"),
    [
        ("vq_assert_safe_venv_target", "venv"),
        ("vq_assert_safe_data_target", "state"),
        ("vq_assert_safe_data_target", "config"),
    ],
)
def test_missing_suffix_below_existing_non_directory_fails_closed(
    tmp_path: Path,
    helper: str,
    kind: str,
) -> None:
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("user data\n", encoding="utf-8")
    target = blocker / "missing" / f"vq-{kind}"
    body = f'{helper} "$1" {kind}' if helper.endswith("data_target") else f'{helper} "$1"'

    proc = _run_helper(body, str(target))

    assert proc.returncode != 0
    assert blocker.read_text(encoding="utf-8") == "user data\n"


@pytest.mark.parametrize("target", ["/var/lib/vq", "/private/var/lib/vq"])
def test_source_lifecycle_refuses_multi_user_venv_roots(target: str) -> None:
    proc = _run_helper('vq_assert_safe_venv_target "$1"', target)

    assert proc.returncode != 0
    assert "multi-user" in proc.stderr


@pytest.mark.parametrize("exists", [False, True])
def test_venv_targets_inside_git_metadata_are_refused(
    tmp_path: Path,
    exists: bool,
) -> None:
    target = tmp_path / ".git" / "lifecycle-venv"
    if exists:
        target.mkdir(parents=True)

    proc = _run_helper('vq_assert_safe_venv_target "$1"', str(target))

    assert proc.returncode != 0
    assert "Git metadata" in proc.stderr


@pytest.mark.parametrize("suffix", ["", "/", "//", "/.", "//./"])
def test_uninstall_refuses_final_component_venv_symlink_spellings(
    tmp_path: Path,
    suffix: str,
) -> None:
    home = tmp_path / "home"
    real_venv = tmp_path / "real-venv"
    link = tmp_path / "venv-link"
    home.mkdir()
    (real_venv / "bin").mkdir(parents=True)
    (real_venv / "bin" / "python").symlink_to(sys.executable)
    (real_venv / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
    sentinel = real_venv / "must-survive"
    sentinel.write_text("still here\n", encoding="utf-8")
    link.symlink_to(real_venv, target_is_directory=True)

    proc = _run_script(
        "uninstall.sh",
        "--venv",
        f"{link}{suffix}",
        "--yes",
        env={
            "HOME": str(home),
            "VQ_STATE_DIR": str(tmp_path / "runtime" / "state"),
            "VQ_CONFIG_DIR": str(tmp_path / "runtime" / "config"),
        },
    )

    assert proc.returncode != 0
    assert "symlinked virtualenv target" in proc.stderr
    assert link.is_symlink()
    assert sentinel.read_text(encoding="utf-8") == "still here\n"


@pytest.mark.parametrize(
    ("flag", "variable", "kind"),
    [
        ("--purge-state", "VQ_STATE_DIR", "state"),
        ("--purge-config", "VQ_CONFIG_DIR", "config"),
    ],
)
@pytest.mark.parametrize("suffix", ["", "/", "//", "/.", "//./"])
def test_uninstall_refuses_final_component_data_symlink_spellings(
    tmp_path: Path,
    flag: str,
    variable: str,
    kind: str,
    suffix: str,
) -> None:
    home = tmp_path / "home"
    real_data = tmp_path / "runtime" / f"real-{kind}"
    link = tmp_path / "runtime" / f"{kind}-link"
    home.mkdir()
    real_data.mkdir(parents=True)
    sentinel = real_data / "must-survive"
    sentinel.write_text("still here\n", encoding="utf-8")
    link.symlink_to(real_data, target_is_directory=True)
    environment = {
        "HOME": str(home),
        "VQ_STATE_DIR": str(tmp_path / "runtime" / "state"),
        "VQ_CONFIG_DIR": str(tmp_path / "runtime" / "config"),
        variable: f"{link}{suffix}",
    }

    proc = _run_script(
        "uninstall.sh",
        "--keep-venv",
        flag,
        "--yes",
        env=environment,
    )

    assert proc.returncode != 0
    assert f"symlinked {kind} directory" in proc.stderr
    assert link.is_symlink()
    assert sentinel.read_text(encoding="utf-8") == "still here\n"


def test_venv_target_must_not_contain_the_checkout() -> None:
    proc = _run_helper('vq_assert_safe_venv_target "$1"', str(REPO.parent))

    assert proc.returncode != 0
    assert "contains the vibe-qc checkout" in proc.stderr


def test_venv_target_must_not_contain_home(tmp_path: Path) -> None:
    target = tmp_path / "target"
    home = target / "home"
    home.mkdir(parents=True)
    proc = _run_helper(
        'HOME="$1/home"; export HOME; vq_assert_safe_venv_target "$1"',
        str(target),
    )

    assert proc.returncode != 0
    assert "contains the home directory" in proc.stderr


def test_existing_venv_target_must_be_owned_by_the_current_user(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    target.mkdir()
    proc = _run_helper(
        "stat() { printf '999999\\n'; }; vq_assert_safe_venv_target \"$1\"",
        str(target),
    )

    assert proc.returncode != 0
    assert "unowned virtualenv target" in proc.stderr


def test_existing_data_target_must_be_owned_by_the_current_user(
    tmp_path: Path,
) -> None:
    target = tmp_path / "state"
    target.mkdir()
    proc = _run_helper(
        "stat() { printf '999999\\n'; }; vq_assert_safe_data_target \"$1\" state",
        str(target),
    )

    assert proc.returncode != 0
    assert "unowned state directory" in proc.stderr


def test_venv_replacement_revalidates_the_target_immediately_before_move(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    moved = tmp_path / "venv-moved"
    target.mkdir()
    (target / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
    proc = _run_helper(
        'vq_record_venv_ownership "$1"; vq_begin_venv_replacement "$1"; '
        'mv -- "$1" "$2"; ln -s "$2" "$1"; '
        'vq_start_venv_replacement "$1"',
        str(target),
        str(moved),
    )

    assert proc.returncode != 0
    assert "symlinked virtualenv target" in proc.stderr
    assert target.is_symlink()
    assert (moved / "pyvenv.cfg").is_file()


def test_venv_replacement_rechecks_checkout_owner_immediately_before_move(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    proc = _run_helper(
        'vq_begin_venv_replacement "$1"; '
        "printf 'version=1\\nproject=%s\\n' \"$2\" > "
        '"$1/$VQ_OWNERSHIP_MARKER_NAME"; '
        'vq_start_venv_replacement "$1"',
        str(target),
        str(tmp_path / "foreign-checkout"),
    )

    assert proc.returncode != 0
    assert "belongs to another checkout" in proc.stderr
    assert target.is_dir()
    assert (target / "pyvenv.cfg").is_file()


def _direct_replacement_receipt(target: Path) -> Path:
    return target.parent / f".{target.name}.vq-venv-replacement.json"


def _direct_receipt_update(target: Path, transaction: str) -> Path:
    receipt = _direct_replacement_receipt(target)
    return receipt.parent / f".{receipt.name}.next-{transaction}"


def _crash_direct_replacement(
    target: Path,
    *,
    after_start: bool,
) -> subprocess.CompletedProcess[str]:
    body = (
        'vq_acquire_lifecycle_lock "$1" test-crash; '
        'vq_begin_venv_replacement "$1" 0 1; '
    )
    if after_start:
        body += (
            'vq_start_venv_replacement "$1"; '
            'printf "partial candidate\\n" > "$1/partial"; '
        )
    body += "vq_release_lifecycle_lock"
    return _run_helper(body, str(target))


@pytest.mark.parametrize("after_start", [False, True])
def test_direct_replacement_crash_is_recovered_before_retry(
    tmp_path: Path,
    after_start: bool,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old exact bytes\n", encoding="utf-8")

    crashed = _crash_direct_replacement(target, after_start=after_start)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    assert receipt.is_file()
    assert stat.S_IMODE(receipt.stat().st_mode) == 0o600
    if after_start:
        assert not sentinel.exists()

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "Recovery completed" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "old exact bytes\n"
    assert not (target / "partial").exists()
    assert not receipt.exists()
    assert not list(tmp_path.glob(".venv.vq-venv-backup-*"))


def test_direct_replacement_recovery_rejects_tampered_receipt_without_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["checkout"] = str(tmp_path / "foreign-checkout")
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "receipt identity is invalid" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert receipt.is_file()


def test_direct_replacement_recovery_rejects_boolean_schema_without_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["schema"] = True
    receipt.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    receipt.chmod(0o600)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "receipt identity is invalid" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert receipt.is_file()


def test_direct_replacement_recovery_rejects_unsafe_receipt_mode(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    receipt.chmod(0o644)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "unsafe transaction file" in recovered.stderr
    assert receipt.is_file()
    assert target.is_dir()


def _set_direct_receipt_phase(target: Path, phase: str) -> dict[str, object]:
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    payload["phase"] = phase
    receipt.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    return payload


def _direct_tree_digest(target: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(target.rglob("*"), key=lambda item: item.as_posix()):
        relative = path.relative_to(target).as_posix()
        if relative == ".vq-venv-transaction":
            continue
        info = path.lstat()
        mode = stat.S_IMODE(info.st_mode)
        if stat.S_ISLNK(info.st_mode):
            kind = b"L"
            payload = os.readlink(path).encode("utf-8", "surrogateescape")
        elif stat.S_ISDIR(info.st_mode):
            kind = b"D"
            payload = b""
        else:
            kind = b"F"
            payload = path.read_bytes()
        digest.update(kind)
        digest.update(relative.encode("utf-8", "surrogateescape"))
        digest.update(b"\0")
        digest.update(f"{mode:o}".encode())
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()


def test_recovery_handles_building_checkpoint_before_original_move(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    _set_direct_receipt_phase(target, "building")

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "Recovery completed" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "old\n"
    assert not _direct_replacement_receipt(target).exists()


def test_recovery_handles_candidate_created_before_complete_marker(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    payload = _set_direct_receipt_phase(target, "building")
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    candidate_stage = target.parent / (
        f".{target.name}.vq-venv-candidate-{payload['transaction']}"
    )
    target.replace(backup)
    candidate_stage.replace(target)
    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "Recovery completed" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "old\n"
    assert not backup.exists()


def test_recovery_discards_partial_pending_marker_before_target_creation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    payload = json.loads(
        _direct_replacement_receipt(target).read_text(encoding="utf-8")
    )
    pending = target.parent / (
        f".{target.name}.vq-venv-marker-{payload['transaction']}"
    )
    pending.write_text("partial", encoding="utf-8")
    pending.chmod(0o600)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "Recovery completed" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "old\n"
    assert not pending.exists()


def test_recovery_discards_exact_transaction_receipt_update_scratch(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=False)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    pending = _direct_receipt_update(target, payload["transaction"])
    pending.write_text("partial receipt update", encoding="utf-8")
    pending.chmod(0o600)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "Recovery completed" in recovered.stderr
    assert sentinel.read_text(encoding="utf-8") == "old\n"
    assert not pending.exists()
    assert not receipt.exists()


def test_committed_recovery_tolerates_marker_unlink_before_receipt_clear(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    crashed = _crash_direct_replacement(target, after_start=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    payload = _set_direct_receipt_phase(target, "target_committed")
    payload["target_tree_sha256"] = _direct_tree_digest(target)
    receipt = _direct_replacement_receipt(target)
    receipt.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    (target / ".vq-venv-transaction").unlink()

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "committed" in recovered.stderr
    assert (target / "partial").read_text(encoding="utf-8") == "partial candidate\n"
    assert not backup.exists()
    assert not _direct_replacement_receipt(target).exists()


def test_stale_receipt_recovery_refuses_while_target_daemon_is_running(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={target}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert recovered.returncode != 0
    assert "a vq daemon is running" in recovered.stderr
    assert receipt.is_file()
    assert backup.is_dir()
    assert not sentinel.exists()
    assert (target / "partial").is_file()


def test_recovery_rejects_replaced_candidate_and_preserves_backup(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "old-sentinel"
    sentinel.write_text("old\n", encoding="utf-8")
    crashed = _crash_direct_replacement(target, after_start=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = json.loads(receipt.read_text(encoding="utf-8"))
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    original_candidate = tmp_path / "original-candidate"
    target.replace(original_candidate)
    target.mkdir(mode=0o700)
    foreign = target / "foreign"
    foreign.write_text("must survive\n", encoding="utf-8")

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "candidate identity changed" in recovered.stderr
    assert foreign.read_text(encoding="utf-8") == "must survive\n"
    assert backup.is_dir()
    assert receipt.is_file()


def test_committed_recovery_rejects_replaced_target_before_backup_cleanup(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    crashed = _crash_direct_replacement(target, after_start=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = _set_direct_receipt_phase(target, "target_committed")
    payload["target_tree_sha256"] = _direct_tree_digest(target)
    receipt.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    target.replace(tmp_path / "verified-candidate")
    target.mkdir(mode=0o700)
    foreign = target / "foreign"
    foreign.write_text("must survive\n", encoding="utf-8")

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "committed target identity changed" in recovered.stderr
    assert foreign.read_text(encoding="utf-8") == "must survive\n"
    assert backup.is_dir()
    assert receipt.is_file()


def test_committed_recovery_rejects_replaced_backup_before_recursive_cleanup(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    crashed = _crash_direct_replacement(target, after_start=True)
    assert crashed.returncode == 0, crashed.stdout + crashed.stderr
    receipt = _direct_replacement_receipt(target)
    payload = _set_direct_receipt_phase(target, "target_committed")
    payload["target_tree_sha256"] = _direct_tree_digest(target)
    receipt.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    receipt.chmod(0o600)
    backup = target.parent / (
        f".{target.name}.vq-venv-backup-{payload['transaction']}"
    )
    backup.replace(tmp_path / "real-backup")
    backup.mkdir(mode=0o700)
    foreign = backup / "foreign"
    foreign.write_text("must survive\n", encoding="utf-8")

    recovered = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
    )

    assert recovered.returncode != 0
    assert "rollback backup identity changed" in recovered.stderr
    assert foreign.read_text(encoding="utf-8") == "must survive\n"
    assert receipt.is_file()


def test_direct_durable_replacement_commits_a_real_virtualenv(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    subprocess.run(
        [sys.executable, "-m", "venv", str(target)],
        check=True,
        capture_output=True,
        text=True,
    )
    _stamp_owner(target)
    old_sentinel = target / "old-sentinel"
    old_sentinel.write_text("old\n", encoding="utf-8")

    committed = _run_helper(
        'vq_acquire_lifecycle_lock "$1" test-success; '
        'vq_begin_venv_replacement "$1" 0 1; '
        'vq_start_venv_replacement "$1"; '
        '"$2" -m venv "$1"; '
        'vq_commit_venv_replacement; '
        "vq_release_lifecycle_lock",
        str(target),
        sys.executable,
    )

    assert committed.returncode == 0, committed.stdout + committed.stderr
    assert (target / "pyvenv.cfg").is_file()
    assert (target / "bin" / "python").exists()
    assert not old_sentinel.exists()
    assert not _direct_replacement_receipt(target).exists()
    assert not (target / ".vq-venv-transaction").exists()
    assert not list(tmp_path.glob(".venv.vq-venv-backup-*"))


def test_direct_recreate_requires_source_stability_before_any_git_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    git_log = tmp_path / "git.log"
    git = fake_bin / "git"
    git.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {git_log!s}\n"
        "exec /usr/bin/git \"$@\"\n",
        encoding="utf-8",
    )
    git.chmod(0o755)

    proc = _run_script(
        "update.sh",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
        env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
    )

    assert proc.returncode != 0
    assert "requires --skip-git" in proc.stderr
    # Read-only selector discovery may inspect Git; fetch/checkout/pull/merge
    # must never begin before the source-stability refusal.
    observed = git_log.read_text(encoding="utf-8") if git_log.exists() else ""
    assert " fetch " not in f" {observed} "
    assert " checkout " not in f" {observed} "
    assert " pull " not in f" {observed} "
    assert " merge " not in f" {observed} "


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_direct_restart_daemon_is_retired_before_service_or_venv_mutation(
    tmp_path: Path,
    script: str,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    args = ["--venv", str(target), "--restart-daemon"]
    if script == "install.sh":
        args.extend(["--force", "--python", sys.executable])
    elif script == "update.sh":
        args.append("--skip-git")
    else:
        args.extend(["--python", sys.executable])

    proc = _run_script(script, *args)

    assert proc.returncode != 0
    assert "no longer stop and restart" in proc.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert not _direct_replacement_receipt(target).exists()


def test_direct_update_proves_daemon_stopped_before_replacement_admission(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _stamp_owner(target)
    sentinel = target / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={target}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    proc = _run_script(
        "update.sh",
        "--skip-git",
        "--recreate-venv",
        "--python",
        sys.executable,
        "--venv",
        str(target),
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert proc.returncode != 0
    assert "a vq daemon is running" in proc.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert not _direct_replacement_receipt(target).exists()
    assert not list(tmp_path.glob(".venv.vq-venv-candidate-*"))


@pytest.mark.parametrize("script", ["install.sh", "reinstall.sh"])
def test_direct_adoption_proves_daemon_stopped_before_writing_owner_marker(
    tmp_path: Path,
    script: str,
) -> None:
    target = tmp_path / "venv"
    _make_venv_shape(target)
    _write_legacy_direct_url(target)
    sentinel = target / "must-survive"
    sentinel.write_text("unchanged\n", encoding="utf-8")
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={target}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    args = ["--venv", str(target), "--adopt-legacy", "--python", sys.executable]
    if script == "install.sh":
        args.append("--force")

    proc = _run_script(
        script,
        *args,
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
        },
    )

    assert proc.returncode != 0
    assert "a vq daemon is running" in proc.stderr
    assert sentinel.read_text(encoding="utf-8") == "unchanged\n"
    assert not (target / OWNER_MARKER).exists()
    assert not _direct_replacement_receipt(target).exists()


def test_creation_python_resolves_an_activated_target_to_its_base(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(target)],
        check=True,
        capture_output=True,
        text=True,
    )
    selected = target / "bin" / "python"
    proc = _run_helper(
        'resolved=""; vq_resolve_creation_python resolved "$1" "$2"; printf \'%s\\n\' "$resolved"',
        str(selected),
        str(target),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    resolved = Path(proc.stdout.strip()).resolve()
    assert resolved.is_file()
    assert not resolved.is_relative_to(target.resolve())


def test_creation_python_rejects_a_base_executable_inside_the_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "venv"
    (target / "bin").mkdir(parents=True)
    fake_python = target / "bin" / "python"
    fake_python.write_text(
        "#!/bin/sh\n"
        'case "${2-}" in\n'
        "  *base_executable*) printf '%s\\n' \"$0\" ;;\n"
        "  *) exit 0 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    proc = _run_helper(
        'resolved=""; vq_resolve_creation_python resolved "$1" "$2"',
        str(fake_python),
        str(target),
    )

    assert proc.returncode != 0
    assert "refusing to execute Python from the unproven target" in proc.stderr


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_replacement_flows_resolve_the_creation_python_before_using_it(
    script: str,
) -> None:
    text = (SCRIPTS / script).read_text(encoding="utf-8")

    assert text.index("vq_resolve_creation_python") < text.index(
        '"$PYTHON_BIN" -m venv "$VENV_PATH"'
    )


def test_source_marker_write_failure_fails_the_lifecycle_operation(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    vq = venv / "bin" / "vq"
    vq.write_text("#!/bin/sh\nexit 23\n", encoding="utf-8")
    vq.chmod(0o755)
    proc = _run_helper(
        "git() { "
        '  case "$*" in '
        "    *'rev-parse --git-dir'*) return 0 ;; "
        "    *'rev-parse HEAD'*) printf '0123456789012345678901234567890123456789\\n'; return 0 ;; "
        "  esac; return 1; "
        "}; "
        'vq_record_source_marker "$1"',
        str(venv),
    )

    assert proc.returncode != 0
    assert "could not write the SOURCE-SHA marker" in proc.stderr


def test_same_target_lifecycle_operations_contend(tmp_path: Path) -> None:
    target = tmp_path / "venv"
    checkout_a = tmp_path / "checkout-a"
    checkout_b = tmp_path / "checkout-b"
    target.mkdir()
    checkout_a.mkdir()
    checkout_b.mkdir()
    proc = _run_helper(
        'VQ_REPO_ROOT="$2"; vq_acquire_lifecycle_lock "$1" holder; '
        "set +e; "
        f'output=$(bash -c \'set -euo pipefail; exec 194>&- 195>&-; . "{HELPER}"; '
        'VQ_REPO_ROOT="$2"; vq_acquire_lifecycle_lock "$1/." contender\' '
        '_ "$1" "$3" 2>&1); '
        "contender_status=$?; set -e; "
        "vq_release_lifecycle_lock; "
        "printf '%s\\n' \"$output\"; "
        '[ "$contender_status" -ne 0 ]; '
        'case "$output" in *"lifecycle operation already active"*) ;; *) exit 9 ;; esac',
        str(target),
        str(checkout_a),
        str(checkout_b),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "lifecycle operation already active" in proc.stdout


def test_same_checkout_lifecycle_operations_contend_across_targets(
    tmp_path: Path,
) -> None:
    target_a = tmp_path / "venv-a"
    target_b = tmp_path / "venv-b"
    target_a.mkdir()
    target_b.mkdir()
    proc = _run_helper(
        'vq_acquire_lifecycle_lock "$1" holder; '
        "set +e; "
        f'output=$(bash -c \'set -euo pipefail; exec 194>&- 195>&-; . "{HELPER}"; '
        'vq_acquire_lifecycle_lock "$1" contender\' _ "$2" 2>&1); '
        "contender_status=$?; set -e; "
        "vq_release_lifecycle_lock; "
        "printf '%s\\n' \"$output\"; "
        '[ "$contender_status" -ne 0 ]; '
        'case "$output" in *"lifecycle operation already active"*) ;; *) exit 9 ;; esac',
        str(target_a),
        str(target_b),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "lifecycle operation already active" in proc.stdout


@pytest.mark.parametrize(
    ("script", "action"),
    [
        ("install.sh", "install"),
        ("update.sh", "update"),
        ("reinstall.sh", "reinstall"),
        ("uninstall.sh", "uninstall"),
    ],
)
def test_every_lifecycle_script_acquires_and_releases_the_target_lock(
    script: str,
    action: str,
) -> None:
    text = (SCRIPTS / script).read_text(encoding="utf-8")

    assert f'vq_acquire_lifecycle_lock "$VENV_PATH" {action}' in text or (
        action == "uninstall" and 'vq_acquire_lifecycle_lock "$LIFECYCLE_TARGET" uninstall' in text
    )
    assert "vq_release_lifecycle_lock" in text


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_checkout_owner_is_stamped_only_after_verification(script: str) -> None:
    text = (SCRIPTS / script).read_text(encoding="utf-8")

    verify = text.index("vq_verify_environment")
    owner = text.index("vq_record_venv_ownership", verify)
    assert verify < owner


@pytest.mark.parametrize("script", ["install.sh", "reinstall.sh"])
def test_locked_inactive_proof_precedes_owner_adoption_and_target_mutation(
    script: str,
) -> None:
    text = (SCRIPTS / script).read_text(encoding="utf-8")
    # A stale durable receipt has its own exact-lock recovery branch. Inspect
    # the ordinary mutation branch, where the daemon must be inactive before
    # adoption can stamp the target or replacement can begin.
    locked = text.rsplit("vq_acquire_lifecycle_lock", maxsplit=1)[1]

    proof = locked.index("vq_assert_daemon_stopped_or_managed")
    owner = locked.index("vq_require_venv_ownership")
    begin = locked.index("vq_begin_venv_replacement")
    assert proof < owner < begin
    assert 'vq_daemon_stop "$VENV_PATH"' not in text
    assert 'vq_daemon_start "$VENV_PATH"' not in text


@pytest.mark.parametrize("script", ["install.sh", "update.sh", "reinstall.sh"])
def test_ordinary_mutation_rechecks_receipt_after_exact_lock(script: str) -> None:
    text = (SCRIPTS / script).read_text(encoding="utf-8")
    locked = text.rsplit("vq_acquire_lifecycle_lock", maxsplit=1)[1]

    receipt = locked.index("vq_venv_replacement_receipt_path")
    recovery = locked.index("vq_recover_pending_venv_replacement_before_use")
    if script == "update.sh":
        mutation = locked.index("prepare_venv_update")
    else:
        mutation = locked.index("vq_require_venv_ownership")
    assert receipt < recovery < mutation


def test_macos_process_ownership_uses_full_argv_not_base_python() -> None:
    proc = _run_helper(
        "venv=$1; "
        "kill() { return 0; }; "
        "ps() { printf '%s\\n' \"$venv/bin/python -m vq daemon run\"; }; "
        'vq_process_belongs_to_venv 999999 "$venv"',
        "/tmp/vq-test-venv",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_systemd_activity_is_scoped_to_the_matching_venv() -> None:
    proc = _run_helper(
        "owned=$1; "
        "kill() { return 0; }; "
        "ps() { printf '%s\\n' \"$owned/bin/vq daemon run\"; }; "
        "systemctl() { "
        '  case "$*" in '
        "    *is-active*) return 0 ;; "
        "    *MainPID*) printf '999999\\n' ;; "
        "    *ExecStart*) printf '{}\\n' ;; "
        "  esac; "
        "}; "
        'vq_systemd_daemon_belongs_to_venv "$owned"; '
        'if vq_systemd_daemon_belongs_to_venv "${owned}-other"; then exit 9; fi',
        "/tmp/vq-test-venv",
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_install_metadata_round_trip(tmp_path: Path) -> None:
    venv = tmp_path / "venv"
    proc = _run_helper(
        'venv=$1; mkdir -p "$venv"; '
        "before=$(umask); "
        'vq_record_install_metadata "$venv" web 1; '
        'after=$(umask); [ "$before" = "$after" ]; '
        "profile=''; editable=''; "
        'vq_load_install_metadata "$venv" profile editable; '
        'printf \'%s/%s\\n\' "$profile" "$editable"',
        str(venv),
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert proc.stdout.strip() == "web/1"


def test_update_preserves_recorded_capabilities_and_install_mode(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    (venv / "bin").mkdir(parents=True)
    (venv / "bin" / "python").symlink_to(sys.executable)
    (venv / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
    (venv / ".vq-install-metadata").write_text(
        "version=1\nextras=web\neditable=1\n", encoding="utf-8"
    )
    _stamp_owner(venv)

    proc = _run_script(
        "update.sh",
        "--skip-git",
        "--dry-run",
        "--venv",
        str(venv),
        env={
            "HOME": str(tmp_path / "home"),
            "VQ_STATE_DIR": str(tmp_path / "state"),
            "VQ_CONFIG_DIR": str(tmp_path / "config"),
        },
    )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "capabilities: web" in proc.stdout
    assert "mode:         editable (-e)" in proc.stdout


def test_update_requires_outer_daemon_ownership_before_any_git_mutation() -> None:
    script = (SCRIPTS / "update.sh").read_text(encoding="utf-8")

    lock = script.index('vq_acquire_lifecycle_lock "$VENV_PATH" update')
    proof = script.index(
        '"$VENV_PATH" 0 "Updating this environment under outer-admin control"'
    )
    direct_proof = script.index('"$VENV_PATH" 0 "Updating this environment"')
    first_git_mutation = script.index('git -C "$VQ_REPO_ROOT" fetch')
    assert lock < proof < first_git_mutation
    assert lock < direct_proof < first_git_mutation
    assert 'vq_daemon_stop "$VENV_PATH"' not in script


def test_update_rejects_forged_same_parent_outer_admin_restart_contract(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={venv}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    proc = _run_script(
        "update.sh",
        "--skip-git",
        "--dry-run",
        "--venv",
        str(venv),
        env={
            "HOME": str(tmp_path / "home"),
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "VQ_ADMIN_MANAGED_DAEMON_RESTART_PID": str(os.getpid()),
        },
    )

    assert proc.returncode != 0
    assert "lacks a validated lifecycle-lock capability" in proc.stderr


def test_update_honors_validated_outer_admin_restart_capability(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={venv}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    with _admin_lock_capability(REPO.resolve(), venv.resolve()) as (
        lock_env,
        pass_fds,
    ):
        proc = _run_script(
            "update.sh",
            "--skip-git",
            "--dry-run",
            "--venv",
            str(venv),
            env={
                "HOME": str(tmp_path / "home"),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "VQ_ADMIN_MANAGED_DAEMON_RESTART_PID": str(os.getpid()),
                **lock_env,
            },
            pass_fds=pass_fds,
        )

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "restart is owned by the outer vq admin update" in proc.stdout
    assert "would refuse without --restart-daemon" not in proc.stdout


def test_update_rejects_admin_restart_capability_for_a_different_target(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    other = tmp_path / "other-venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)

    with _admin_lock_capability(REPO.resolve(), other.resolve()) as (
        lock_env,
        pass_fds,
    ):
        proc = _run_script(
            "update.sh",
            "--skip-git",
            "--dry-run",
            "--venv",
            str(venv),
            env={
                "VQ_ADMIN_MANAGED_DAEMON_RESTART_PID": str(os.getpid()),
                **lock_env,
            },
            pass_fds=pass_fds,
        )

    assert proc.returncode != 0
    assert "does not cover this exact checkout and virtualenv" in proc.stderr


def test_update_valid_lock_capability_does_not_bypass_live_daemon_proof(
    tmp_path: Path,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        "case \"$*\" in\n"
        "  *is-active*) exit 0 ;;\n"
        "  *MainPID*) printf '0\\n' ;;\n"
        f"  *ExecStart*) printf '{{ path={venv}/bin/vq; }}\\n' ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)

    with _admin_lock_capability(REPO.resolve(), venv.resolve()) as (
        lock_env,
        pass_fds,
    ):
        proc = _run_script(
            "update.sh",
            "--skip-git",
            "--venv",
            str(venv),
            env={
                "HOME": str(tmp_path / "home"),
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "VQ_ADMIN_MANAGED_DAEMON_RESTART_PID": str(os.getpid()),
                **lock_env,
            },
            pass_fds=pass_fds,
        )

    assert proc.returncode != 0
    assert "a vq daemon is running" in proc.stderr
    assert "Updating pip" not in proc.stdout
    assert "outer vq admin transaction stopped" not in proc.stdout


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("not-a-pid", "must be a numeric parent PID"),
        ("999999", "does not match this script's parent"),
    ],
)
def test_update_rejects_invalid_outer_admin_restart_handshake(
    tmp_path: Path,
    value: str,
    message: str,
) -> None:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _stamp_owner(venv)

    proc = _run_script(
        "update.sh",
        "--skip-git",
        "--dry-run",
        "--venv",
        str(venv),
        env={"VQ_ADMIN_MANAGED_DAEMON_RESTART_PID": value},
    )

    assert proc.returncode != 0
    assert message in proc.stderr


def test_lifecycle_scripts_never_invoke_removed_daemon_start() -> None:
    scripts = "\n".join(
        (SCRIPTS / name).read_text(encoding="utf-8")
        for name in ("install.sh", "update.sh", "reinstall.sh", "uninstall.sh")
    )

    assert 'bin/vq" daemon start' not in scripts


# ---------------------------------------------------------------------------
# An editable controller install must survive an update.
#
# `update.sh` documents "By default the installed mode is preserved" and read
# that mode only from `.vq-install-metadata`, which is vq's own note and only
# exists in a venv install.sh built. The plain
# `python -m venv .venv && .venv/bin/pip install -e '.[test,web]'` that
# CONTRIBUTING.md documents leaves no such note, so the default fell through
# to *copied* and every routine update quietly converted the install.
#
# The cost is not cosmetic. `fleet_release.runtime_repo()` resolves the
# controller checkout from where vq is imported; a copied install lands in
# site-packages, and `vq admin rollout-latest` then refuses on that host with
# "vq runtime source .../lib/pythonX.Y is not a git checkout". A routine
# `vq admin update vibeqc-queue HOST` could therefore disable rollout there,
# with no warning at any point.


def _update_preview(tmp_path: Path, venv: Path, *extra: str) -> subprocess.CompletedProcess[str]:
    return _run_script(
        "update.sh",
        "--skip-git",
        "--dry-run",
        "--venv",
        str(venv),
        *extra,
        env={
            "HOME": str(tmp_path / "home"),
            "VQ_STATE_DIR": str(tmp_path / "state"),
            "VQ_CONFIG_DIR": str(tmp_path / "config"),
        },
    )


def _venv_with_pip_record(tmp_path: Path, *, editable: bool) -> Path:
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    _write_direct_url(venv, editable=editable)
    _stamp_owner(venv)
    return venv


def test_update_preserves_an_editable_install_pip_recorded(tmp_path: Path) -> None:
    """No `.vq-install-metadata`: exactly the hand-made dev venv."""
    venv = _venv_with_pip_record(tmp_path, editable=True)

    proc = _update_preview(tmp_path, venv)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         editable (-e)" in proc.stdout


def test_update_preserves_a_copied_install_pip_recorded(tmp_path: Path) -> None:
    venv = _venv_with_pip_record(tmp_path, editable=False)

    proc = _update_preview(tmp_path, venv)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         copied into the venv" in proc.stdout
    assert "rollout-latest" not in proc.stderr


def test_pip_metadata_outranks_a_disagreeing_note(tmp_path: Path) -> None:
    """The note is vq's own record of what it did; pip's is what happened."""
    venv = _venv_with_pip_record(tmp_path, editable=True)
    (venv / ".vq-install-metadata").write_text(
        "version=1\nextras=web\neditable=0\n", encoding="utf-8"
    )

    proc = _update_preview(tmp_path, venv)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         editable (-e)" in proc.stdout
    assert "Trusting pip" in proc.stderr


def test_an_unreadable_install_shape_still_falls_back_to_the_note(
    tmp_path: Path,
) -> None:
    """A wheel or VCS install records no `dir_info`, so the question has no
    answer from pip and the recorded note is all there is."""
    venv = tmp_path / "venv"
    _make_venv_shape(venv)
    metadata = (
        venv / "lib" / "python3.12" / "site-packages"
        / "vq-0.26.0.dist-info" / "direct_url.json"
    )
    metadata.parent.mkdir(parents=True, exist_ok=True)
    metadata.write_text(
        json.dumps({"url": "https://example.invalid/vq.git",
                    "vcs_info": {"vcs": "git"}}),
        encoding="utf-8",
    )
    (venv / ".vq-install-metadata").write_text(
        "version=1\nextras=web\neditable=1\n", encoding="utf-8"
    )
    _stamp_owner(venv)

    proc = _update_preview(tmp_path, venv)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         editable (-e)" in proc.stdout


def test_deliberately_copying_over_an_editable_install_warns(
    tmp_path: Path,
) -> None:
    """`--copied` stays the operator's call, but it stops being silent."""
    venv = _venv_with_pip_record(tmp_path, editable=True)

    proc = _update_preview(tmp_path, venv, "--copied")

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         copied into the venv" in proc.stdout
    assert "rollout-latest" in proc.stderr
    assert "is not a git checkout" in proc.stderr


def test_two_dist_info_payloads_are_not_guessed_between(tmp_path: Path) -> None:
    """Fail closed to the note rather than pick one of two installs."""
    venv = _venv_with_pip_record(tmp_path, editable=True)
    second = (
        venv / "lib64" / "python3.12" / "site-packages"
        / "vq-0.26.0.dist-info" / "direct_url.json"
    )
    second.parent.mkdir(parents=True, exist_ok=True)
    second.write_text(
        json.dumps({"url": PROJECT.resolve().as_uri(), "dir_info": {}}),
        encoding="utf-8",
    )
    (venv / ".vq-install-metadata").write_text(
        "version=1\nextras=core\neditable=0\n", encoding="utf-8"
    )
    _stamp_owner(venv)

    proc = _update_preview(tmp_path, venv)

    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "mode:         copied into the venv" in proc.stdout
