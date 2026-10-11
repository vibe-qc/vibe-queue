"""Directory descriptors that list only what existed when opened (#139).

On btrfs, readdir through one open file description stops at the last entry
present when it was opened, and ``os.listdir(fd)`` reads through a duplicate
of that description. A keyed directory submit opened its fresh workspace
empty, copied the payload in through that descriptor and then digested it
through the same one. It hashed an empty directory, so every keyed directory
submit on a btrfs jobs directory refused with "submission payload changed".

CI has no btrfs, so ``btrfs_readdir`` models that one behavior: listing an
integer descriptor returns only the entries that existed when the descriptor
was opened and still exist.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from vq import dirfd, submit
from vq.spec import JobSpec


@pytest.fixture
def btrfs_readdir(monkeypatch: pytest.MonkeyPatch) -> None:
    real_open = os.open
    real_listdir = os.listdir
    real_close = os.close
    at_open: dict[int, set[str]] = {}

    def recording_open(*args, **kwargs):
        fd = real_open(*args, **kwargs)
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            at_open[fd] = set(real_listdir(fd))
        else:
            at_open.pop(fd, None)
        return fd

    def stale_listdir(path=None):
        if isinstance(path, int) and path in at_open:
            return [name for name in real_listdir(path) if name in at_open[path]]
        return real_listdir(".") if path is None else real_listdir(path)

    def forgetting_close(fd):
        at_open.pop(fd, None)
        real_close(fd)

    monkeypatch.setattr(os, "open", recording_open)
    monkeypatch.setattr(os, "listdir", stale_listdir)
    monkeypatch.setattr(os, "close", forgetting_close)


def _open_dir(path: Path) -> int:
    return os.open(path, os.O_RDONLY | os.O_DIRECTORY)


def test_the_model_reproduces_the_stale_listing(
    tmp_path: Path, btrfs_readdir: None,
) -> None:
    fd = _open_dir(tmp_path)
    try:
        (tmp_path / "late.py").write_text("print(1)\n")
        assert os.listdir(fd) == []
        assert dirfd.listdir(fd) == ["late.py"]
    finally:
        os.close(fd)


def test_listdir_sees_entries_created_after_the_descriptor(tmp_path: Path) -> None:
    fd = _open_dir(tmp_path)
    try:
        (tmp_path / "a").write_text("")
        (tmp_path / "b").mkdir()
        assert sorted(dirfd.listdir(fd)) == ["a", "b"]
    finally:
        os.close(fd)


def test_listdir_follows_the_held_inode_not_the_path(tmp_path: Path) -> None:
    held = tmp_path / "held"
    held.mkdir()
    fd = _open_dir(held)
    try:
        held.rename(tmp_path / "moved")
        held.mkdir()
        (held / "impostor").write_text("")
        (tmp_path / "moved" / "mine").write_text("")
        assert dirfd.listdir(fd) == ["mine"]
    finally:
        os.close(fd)


def test_listdir_closes_the_descriptor_it_opens(tmp_path: Path) -> None:
    fd = _open_dir(tmp_path)
    try:
        before = len(os.listdir("/dev/fd")) if Path("/dev/fd").is_dir() else None
        for _ in range(50):
            dirfd.listdir(fd)
        if before is not None:
            assert len(os.listdir("/dev/fd")) == before
    finally:
        os.close(fd)


def test_the_staged_workspace_digests_as_the_snapshot(
    tmp_path: Path, btrfs_readdir: None,
) -> None:
    source = tmp_path / "source"
    (source / "sub").mkdir(parents=True)
    (source / "p.py").write_text("print(1)\n")
    (source / "sub" / "data.txt").write_text("x\n")
    expected = submit._payload_digest(
        source_file=None, source_directory=source, source_archive=None,
    )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    workspace_fd = _open_dir(workspace)  # opened empty, like the keyed stage
    source_fd = _open_dir(source)
    try:
        submit._copy_directory_snapshot_at(source_fd, workspace_fd)
        assert submit._directory_payload_digest_at(workspace_fd) == expected[1]
    finally:
        os.close(source_fd)
        os.close(workspace_fd)


def test_a_keyed_directory_submit_succeeds_and_replays(
    tmp_path: Path, btrfs_readdir: None,
) -> None:
    source = tmp_path / "payload"
    source.mkdir()
    (source / "p.py").write_text("print(1)\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    def keyed() -> str:
        return submit.submit_local(
            host="localhost",
            directory=str(source),
            command=["python3", "p.py"],
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            idempotency_key="probe-139",
        )

    first = keyed()

    assert (jobs_dir / first / "p.py").read_text() == "print(1)\n"
    assert JobSpec.read(queue_dir / f"{first}.json").idempotency_key_hash
    assert keyed() == first
    assert [path.name for path in queue_dir.glob("*.json")] == [f"{first}.json"]
