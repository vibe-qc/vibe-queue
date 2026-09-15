"""Named directory transfer contracts; format decoding belongs to the producer."""
from __future__ import annotations

import contextlib
import io
import os
import shutil
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests.test_fetch import _materialize_job
from tests.test_fetch import state as state
from vq import fetch, paths, transport
from vq.cli import main
from vq.config import HostConfig


def _tree(root: Path) -> None:
    root.mkdir(parents=True)
    (root / 'metadata.txt').write_text('opaque contents')
    (root / 'nested').mkdir()
    (root / 'nested' / 'data').write_bytes(b'\x00\x01\xff')
    (root / 'empty').mkdir()


@pytest.mark.parametrize('source', ['workspace', 'archive', 'workdir', 'subdir'])
@pytest.mark.parametrize('remote', [False, True])
def test_named_directory_round_trip(state, monkeypatch, source, remote):
    spec = _materialize_job(state, 'directory01', {'unrelated': 'leave behind'})
    root = Path(spec.cwd)
    workdir = source in {'workdir', 'subdir'}
    subdir = 'results/accepted' if source == 'subdir' else None
    if workdir:
        root = state / 'scratch'
        spec.workdir = str(root)
        root.mkdir()
        if subdir:
            root = root / subdir
    _tree(root / 'run.trexio')
    if source == 'archive':
        archive = state / 'job.tar.bz2'
        with tarfile.open(archive, 'w:bz2') as tf:
            tf.add(root, arcname=spec.dest_dirname)
        shutil.rmtree(root)
        spec.archive_path = str(archive)
        spec.archived_at = '2026-09-13T12:00:00+00:00'
    spec.write(paths.spec_path(spec.id))
    if remote:
        @contextlib.contextmanager
        def stream(host_cfg, *args):
            result = CliRunner().invoke(main, list(args))
            assert result.exit_code == 0, result.output
            yield SimpleNamespace(stdout=io.BytesIO(result.stdout_bytes), stderr_text='')
        monkeypatch.setattr(transport, 'stream_remote_vq', stream)
        dst = fetch.fetch_artifact_remote(HostConfig(ssh='remote.invalid'), spec.id,
                                         'run.trexio', state / 'out', workdir=workdir,
                                         subdir=subdir)
    else:
        dst = fetch.fetch_artifact_local(spec.id, 'run.trexio', state / 'out',
                                        workdir=workdir, subdir=subdir)
    assert (dst / 'nested' / 'data').read_bytes() == b'\x00\x01\xff'
    assert (dst / 'metadata.txt').read_text() == 'opaque contents'
    assert (dst / 'empty').is_dir()
    assert list(dst.parent.iterdir()) == [dst]


@pytest.mark.parametrize('kind', ['symlink', 'directory-link', 'fifo'])
@pytest.mark.parametrize('source', ['workspace', 'workdir'])
def test_directory_child_refusal_preserves_prior_snapshot(state, source, kind):
    spec = _materialize_job(state, 'directory02', {})
    spec.workdir = spec.cwd
    spec.write(paths.spec_path(spec.id))
    root = Path(spec.cwd) / 'run.trexio'
    _tree(root)
    out = state / 'out'
    dst = out / root.name
    _tree(dst)
    (dst / 'previous').write_text('accepted')
    child = root / 'forbidden'
    if kind == 'fifo':
        os.mkfifo(child)
    else:
        child.symlink_to(root.parent if kind == 'directory-link' else root / 'metadata.txt')
    with pytest.raises((OSError, ValueError)):
        fetch.fetch_artifact_local(spec.id, root.name, out, workdir=source == 'workdir')
    assert (dst / 'previous').read_text() == 'accepted'
    assert list(out.iterdir()) == [dst]


def test_directory_refresh_replaces_tree_and_idempotent_false_refuses(state):
    spec = _materialize_job(state, 'directory03', {})
    src = Path(spec.cwd) / 'run.trexio'
    _tree(src)
    dst = fetch.fetch_artifact_local(spec.id, src.name, state / 'out')
    (dst / 'obsolete').write_text('stale')
    (src / 'metadata.txt').write_text('new')
    with pytest.raises(FileExistsError):
        fetch.fetch_artifact_local(spec.id, src.name, state / 'out', idempotent=False)
    fetch.fetch_artifact_local(spec.id, src.name, state / 'out')
    assert not (dst / 'obsolete').exists()
    assert (dst / 'metadata.txt').read_text() == 'new'


@pytest.mark.parametrize('bad', ['symlink', 'hardlink', 'fifo', 'escape', 'absolute',
                                'duplicate', 'extra-root', 'file-parent'])
def test_remote_directory_rejects_malformed_tree(state, monkeypatch, bad):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as tf:
        root = tarfile.TarInfo('run.trexio')
        root.type = tarfile.DIRTYPE
        tf.addfile(root)
        item = tarfile.TarInfo('run.trexio/data')
        if bad in {'symlink', 'hardlink'}:
            item.type = tarfile.SYMTYPE if bad == 'symlink' else tarfile.LNKTYPE
            item.linkname = '../outside'
        elif bad == 'fifo':
            item.type = tarfile.FIFOTYPE
        elif bad == 'escape':
            item.name = 'run.trexio/../outside'
        elif bad == 'absolute':
            item.name = '/run.trexio/data'
        elif bad == 'extra-root':
            item.name = 'other/data'
        tf.addfile(item)
        if bad == 'duplicate':
            tf.addfile(item)
        elif bad == 'file-parent':
            tf.addfile(tarfile.TarInfo('run.trexio/data/child'))
    @contextlib.contextmanager
    def stream(*args):
        yield SimpleNamespace(stdout=io.BytesIO(buffer.getvalue()), stderr_text='')
    monkeypatch.setattr(transport, 'stream_remote_vq', stream)
    dst = state / 'out' / 'run.trexio'
    _tree(dst)
    with pytest.raises((transport.RemoteError, OSError)):
        fetch.fetch_artifact_remote(HostConfig(ssh='remote.invalid'), 'id', dst.name, dst.parent)
    assert (dst / 'metadata.txt').read_text() == 'opaque contents'
    assert list(dst.parent.iterdir()) == [dst]


def test_remote_failure_after_complete_directory_keeps_old_tree(state, monkeypatch):
    root = state / 'source' / 'run.trexio'
    _tree(root)
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as tf:
        tf.add(root, arcname=root.name)
    @contextlib.contextmanager
    def stream(*args):
        yield SimpleNamespace(stdout=io.BytesIO(buffer.getvalue()), stderr_text='late failure')
        raise transport.RemoteError('late failure')
    monkeypatch.setattr(transport, 'stream_remote_vq', stream)
    dst = state / 'out' / root.name
    _tree(dst)
    (dst / 'previous').write_text('accepted')
    with pytest.raises(transport.RemoteError, match='late failure'):
        fetch.fetch_artifact_remote(HostConfig(ssh='remote.invalid'), 'id', root.name, dst.parent)
    assert (dst / 'previous').read_text() == 'accepted'
    assert list(dst.parent.iterdir()) == [dst]


def test_open_directory_replacement_cannot_redirect_children(state, monkeypatch):
    spec = _materialize_job(state, 'directory04', {})
    source = Path(spec.cwd) / 'run.trexio'
    _tree(source)
    outside = state / 'outside'
    _tree(outside)
    (outside / 'metadata.txt').write_text('must not read')
    original_open = os.open
    def replace(path, flags, *args, **kwargs):
        if path == 'metadata.txt' and kwargs.get('dir_fd') is not None:
            source.rename(source.with_name('old'))
            source.symlink_to(outside, target_is_directory=True)
        return original_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(os, 'open', replace)
    dst = fetch.fetch_artifact_local(spec.id, source.name, state / 'out')
    assert (dst / 'metadata.txt').read_text() == 'opaque contents'


@pytest.mark.parametrize('kind', ['symlink', 'hardlink', 'fifo', 'traversal', 'duplicate'])
def test_archived_directory_rejects_unsafe_selected_members(state, kind):
    archive = state / 'archive.tar'
    with tarfile.open(archive, 'w') as tf:
        root = tarfile.TarInfo('job/run.trexio')
        root.type = tarfile.DIRTYPE
        tf.addfile(root)
        item = tarfile.TarInfo('job/run.trexio/child')
        if kind in {'symlink', 'hardlink'}:
            item.type = tarfile.SYMTYPE if kind == 'symlink' else tarfile.LNKTYPE
            item.linkname = '../outside'
        elif kind == 'fifo':
            item.type = tarfile.FIFOTYPE
        elif kind == 'traversal':
            item.name = 'job/run.trexio/../outside'
        tf.addfile(item)
        if kind == 'duplicate':
            tf.addfile(item)
    with pytest.raises(ValueError):
        fetch._copy_named_artifact_from_archive(archive, 'run.trexio', state / 'staging')
