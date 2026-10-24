"""Exercise the standalone observer and recorder against stale fd listings."""

from __future__ import annotations

import base64
import os
from pathlib import Path

import pytest

from tests.test_dirfd_btrfs_139 import btrfs_readdir  # noqa: F401
from vq import admin

pytestmark = pytest.mark.usefixtures("btrfs_readdir")


@pytest.fixture(params=["observer", "recorder"])
def helper(request: pytest.FixtureRequest):
    source = admin._DETACHED_REMOTE_HELPER_SOURCE
    if request.param == "recorder":
        encoded = source.split('\n__loader_source_b64__ = "', 1)[1].split('"', 1)[0]
        source = base64.b64decode(encoded).decode("utf-8")
    namespace = {}
    umask = os.umask(0o077)
    try:
        # Load the actual shipped functions without invoking the stdin protocol.
        exec(source.split("\n__loader_source_b64__ =", 1)[0], namespace)
        yield namespace
    finally:
        os.umask(umask)


@pytest.mark.parametrize("receipt", ["lease.json", "activation.json", "result.json"])
def test_late_publication_is_pending_then_readable(
    tmp_path: Path, helper, receipt: str,
) -> None:
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        temporary = "." + receipt + "." + "a" * 32 + ".tmp"
        (tmp_path / temporary).write_text('{"complete": true}')
        (tmp_path / temporary).chmod(0o600)
        os.link(temporary, receipt, src_dir_fd=fd, dst_dir_fd=fd)
        assert os.listdir(fd) == []  # The model really hides the new entries.
        assert helper["optional_json"](fd, receipt) is None
        os.unlink(temporary, dir_fd=fd)
        assert helper["optional_json"](fd, receipt) == {"complete": True}
    finally:
        os.close(fd)


@pytest.mark.parametrize("entry, error", [
    ("unexpected.txt", "unexpected run entry"),
    (".result.json." + "a" * 32 + ".tmp", "unsafe temporary receipt"),
])
def test_late_unsafe_entry_is_rejected(
    tmp_path: Path, helper, entry: str, error: str,
) -> None:
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        (tmp_path / entry).write_text("unsafe")
        (tmp_path / entry).chmod(0o644)
        assert os.listdir(fd) == []
        with pytest.raises(ValueError, match=error):
            helper["validate_entries"](fd)
    finally:
        os.close(fd)


def test_validation_stays_bound_to_the_renamed_directory(
    tmp_path: Path, helper,
) -> None:
    run = tmp_path / "run"
    run.mkdir()
    fd = os.open(run, os.O_RDONLY | os.O_DIRECTORY)
    try:
        moved = tmp_path / "moved"
        run.rename(moved)
        run.mkdir()  # A clean replacement must not hide the held inode's entry.
        (moved / "unexpected.txt").write_text("unsafe")
        with pytest.raises(ValueError, match="unexpected run entry"):
            helper["validate_entries"](fd)
    finally:
        os.close(fd)
