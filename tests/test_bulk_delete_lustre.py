"""Remote bulk deletion uses munlink on Lustre and plain rm elsewhere.

The helper runs on scheduler hosts. These tests execute the real shell
function with stand-in ``stat`` and ``munlink`` commands on PATH, so the Lustre
branch runs on any development machine.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from vq import bulk_delete

pytestmark = pytest.mark.skipif(
    shutil.which("find") is None or shutil.which("xargs") is None,
    reason="needs find and xargs",
)


def _fake_bin(tmp_path: Path, *, fs_type: str, munlink: bool = True) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "munlink.log"
    (bin_dir / "stat").write_text(
        "#!/bin/sh\n"
        '[ "$1" = -f ] && [ "$2" = -c ] && [ "$3" = %T ] || exit 64\n'
        f"echo {fs_type}\n"
    )
    if munlink:
        (bin_dir / "munlink").write_text(
            "#!/bin/sh\n"
            f'for p in "$@"; do printf "%s\\n" "$p" >> {log}; done\n'
            f'exec {sys.executable} -c '
            '"import os, sys; [os.unlink(p) for p in sys.argv[1:]]" "$@"\n'
        )
    for tool in bin_dir.iterdir():
        tool.chmod(0o755)
    return bin_dir


def _tree(tmp_path: Path) -> tuple[Path, Path]:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    tree = tmp_path / "jobs" / "abc123"
    for index in range(3):
        sub = tree / f"d{index}" / "deeper"
        sub.mkdir(parents=True)
        for number in range(5):
            (sub / f"f{number}.out").write_text("x")
    (tree / "link-out").symlink_to(outside)
    return tree, outside


def _run(bin_dir: Path, *paths: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}
    return subprocess.run(
        bulk_delete.rmtree_argv(*(str(p) for p in paths)),
        capture_output=True, text=True, env=env, check=False, timeout=60,
    )


def test_lustre_tree_is_unlinked_with_munlink_without_following_links(tmp_path) -> None:
    bin_dir = _fake_bin(tmp_path, fs_type="lustre")
    tree, outside = _tree(tmp_path)
    receipt = tmp_path / "jobs" / "abc123.submit-once.json"
    receipt.write_text("{}")

    result = _run(bin_dir, tree, receipt)

    assert result.returncode == 0, result.stderr
    assert not tree.exists() and not receipt.exists()
    assert (outside / "keep.txt").read_text() == "keep"
    unlinked = (tmp_path / "munlink.log").read_text().splitlines()
    assert len(unlinked) == 16  # 15 files and the symlink itself
    assert str(tree / "link-out") in unlinked


def test_other_filesystems_use_plain_rm(tmp_path) -> None:
    bin_dir = _fake_bin(tmp_path, fs_type="ext2/ext3")
    tree, outside = _tree(tmp_path)

    result = _run(bin_dir, tree)

    assert result.returncode == 0, result.stderr
    assert not tree.exists()
    assert (outside / "keep.txt").exists()
    assert not (tmp_path / "munlink.log").exists()


def test_lustre_without_munlink_falls_back_to_rm(tmp_path) -> None:
    bin_dir = _fake_bin(tmp_path, fs_type="lustre", munlink=False)
    tree, _outside = _tree(tmp_path)
    clean_path = os.pathsep.join(
        p for p in os.environ["PATH"].split(os.pathsep)
        if not (Path(p) / "munlink").exists()
    )
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{clean_path}"}

    result = subprocess.run(
        bulk_delete.rmtree_argv(str(tree)), capture_output=True, text=True,
        env=env, check=False, timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert not tree.exists()


def test_a_symlinked_root_is_removed_as_a_link_only(tmp_path) -> None:
    bin_dir = _fake_bin(tmp_path, fs_type="lustre")
    _tree_path, outside = _tree(tmp_path)
    link = tmp_path / "jobs" / "linked"
    link.symlink_to(outside)

    result = _run(bin_dir, link)

    assert result.returncode == 0, result.stderr
    assert not link.is_symlink()
    assert (outside / "keep.txt").exists()
    assert not (tmp_path / "munlink.log").exists()


def test_missing_paths_are_not_an_error(tmp_path) -> None:
    bin_dir = _fake_bin(tmp_path, fs_type="lustre")

    result = _run(bin_dir, tmp_path / "never-created")

    assert result.returncode == 0, result.stderr


def test_runtime_source_stage_reclaim_uses_the_helper(tmp_path) -> None:
    from vq import admin

    bin_dir = _fake_bin(tmp_path, fs_type="lustre")
    root = tmp_path / "stages"
    names = [f"{index:040x}-{index:032x}" for index in range(3)]
    for name in names:
        (root / name / "src").mkdir(parents=True)
        (root / name / "src" / "module.py").write_text("x")
    env = {**os.environ, "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}"}

    result = subprocess.run(
        ["sh", "-c", admin._RUNTIME_SOURCE_RECLAIM_SCRIPT, "reclaim", str(root), "1", ""],
        capture_output=True, text=True, env=env, check=False, timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "reclaimed=2 retained=1" in result.stdout
    assert len([p for p in root.iterdir()]) == 1
    assert len((tmp_path / "munlink.log").read_text().splitlines()) == 2
