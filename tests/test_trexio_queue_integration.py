"""Opt-in real producer gate: VQ_TREXIO_PYTHON must supply both TREXIO backends.

The normal queue suite needs no chemistry dependency. Once selected, this gate
fails on missing imports/backends; it never substitutes placeholder fixtures.
All execution uses a sandbox daemon; remote transport boundaries are loopbacks.
"""
from __future__ import annotations

import contextlib
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from tests.test_scheduler_dispatch import FakeRunner, make_dispatcher
from vq import cleanup, config, fetch, paths, submit, transport
from vq.cli import main
from vq.daemon import Daemon
from vq.scheduler_dispatch import SchedulerHandle
from vq.spec import JobSpec, JobState

WORKLOAD = '''
import hashlib, json, os, sys
from pathlib import Path
import vibeqc
import vibeqc._vibeqc_core as core
import trexio
from vibeqc.output.formats.trexio import read_trexio
mode, backend = sys.argv[1:3]
name = "run.trexio.h5" if backend == "hdf5" else "run.trexio"
if mode == "check":
    for path in sys.argv[3:]:
        data = read_trexio(path)
        assert data.n_electrons == 2
        assert abs(data.energy - float(Path("reference-energy").read_text())) < 1e-9
else:
    mol = vibeqc.Molecule([vibeqc.Atom(1, [0., 0., 0.]), vibeqc.Atom(1, [0., 0., 1.4])])
    kwargs = {"initial_guess": "read", "read_from": name} if mode == "read" else {}
    result = vibeqc.run_job(mol, basis="sto-3g", method="rhf",
                           output="restart" if kwargs else "run",
                           trexio=not kwargs, trexio_backend=backend,
                           verbose=False, progress=False, **kwargs)
    if result is not None:
        assert result.converged
        if kwargs:
            assert abs(result.energy - float(Path("reference-energy").read_text())) < 1e-9
        else:
            Path("reference-energy").write_text(str(result.energy))
        Path("receipt.json").write_text(json.dumps({"backend": backend, "mode": mode,
            "energy": result.energy, "producer_version": vibeqc.__version__,
            "producer_module": vibeqc.__file__, "trexio_version": trexio.__version__,
            "native_sha256": hashlib.sha256(Path(core.__file__).read_bytes()).hexdigest()}))
'''


@pytest.fixture
def producer(monkeypatch, tmp_path):
    python = os.environ.get('VQ_TREXIO_PYTHON')
    if not python:
        pytest.skip('select real producer gate with VQ_TREXIO_PYTHON')
    assert Path(python).is_file(), 'VQ_TREXIO_PYTHON must be an installed interpreter'
    monkeypatch.setenv('PYTHONDONTWRITEBYTECODE', '1')
    monkeypatch.setenv('VIBEQC_GFN2_CACHE_DIR', str(tmp_path / 'gfn-cache'))
    for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
                 'VECLIB_MAXIMUM_THREADS', 'NUMEXPR_NUM_THREADS'):
        monkeypatch.setenv(name, '1')
    probe = config.run_import_runtime_identity_probe(python, 'vibeqc', timeout=60)
    assert probe[0] == 0, probe
    trexio_probe = config.run_import_runtime_identity_probe(
        python, 'trexio', symbols=['File', 'TREXIO_HDF5', 'TREXIO_TEXT'], timeout=60,
    )
    assert trexio_probe[0] == 0, trexio_probe
    return python


def _drain_job(daemon, jobid):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        daemon.iterate()
        spec = JobSpec.read(paths.spec_path(jobid))
        if spec.is_terminal:
            assert spec.state == JobState.COMPLETED, (spec.state, spec.failure_reason,
                (Path(spec.cwd) / 'stderr.log').read_text())
            return spec
        time.sleep(0.02)
    pytest.fail(f'sandbox job {jobid} did not finish within 120 seconds')


@pytest.mark.parametrize('backend', ['hdf5', 'text'])
def test_queued_export_transfer_archive_and_read(
    producer, tmp_path, monkeypatch, backend, record_property,
):
    payload = tmp_path / 'payload'
    payload.mkdir()
    (payload / 'job.py').write_text(WORKLOAD)
    daemon = Daemon(max_cpus=1, max_jobs=1, poll_interval=0.02,
                    queue_dir=paths.queue_dir(), jobs_dir=paths.jobs_dir())
    try:
        jobid = submit.submit_local(host='localhost', directory=str(payload),
            command=[producer, 'job.py', 'produce', backend], cpus=1, vibeqc_preflight=True)
        planned = JobSpec.read(paths.spec_path(jobid))
        name = 'run.trexio.h5' if backend == 'hdf5' else 'run.trexio'
        assert name in planned.expected_outputs
        spec = _drain_job(daemon, jobid)
        workspace = Path(spec.cwd)
        receipt = json.loads((workspace / 'receipt.json').read_text())
        assert receipt['backend'] == backend and receipt['mode'] == 'produce'
        expected_core = os.environ.get('VQ_TREXIO_EXPECTED_CORE_SHA256')
        if expected_core:
            assert receipt['native_sha256'] == expected_core
        snapshots = []
        full = fetch.fetch_local(jobid, tmp_path / 'full')
        snapshots.append(full / name)
        snapshots.append(fetch.fetch_artifact_local(jobid, name, tmp_path / 'named'))

        @contextlib.contextmanager
        def stream(host_cfg, *args):
            result = CliRunner().invoke(main, list(args))
            assert result.exit_code == 0, result.output
            yield SimpleNamespace(stdout=io.BytesIO(result.stdout_bytes), stderr_text='')
        monkeypatch.setattr(transport, 'stream_remote_vq', stream)
        snapshots.append(fetch.fetch_artifact_remote(config.HostConfig(ssh='remote.invalid'),
            jobid, name, tmp_path / 'remote-named'))

        class WorkspaceRunner(FakeRunner):
            def download_file(self, remote_path, local_path):
                with tarfile.open(local_path, 'w') as tf:
                    tf.add(workspace, arcname='.')
        scheduler_dst = tmp_path / 'scheduler-fetch'
        make_dispatcher(WorkspaceRunner()).fetch_results(
            SchedulerHandle('123.cluster', '/scratch/job'), scheduler_dst)
        snapshots.append(scheduler_dst / name)
        cleanup.archive_workspace(spec)
        assert not workspace.exists()
        snapshots.append(fetch.fetch_local(jobid, tmp_path / 'archive-full') / name)
        archived = fetch.fetch_artifact_local(jobid, name, tmp_path / 'archive-named')
        snapshots.append(archived)
        snapshots.append(fetch.fetch_artifact_remote(config.HostConfig(ssh='remote.invalid'),
            jobid, name, tmp_path / 'archive-remote'))
        # Decode every transferred fixture with the real producer backend.
        checked = subprocess.run([producer, str(payload / 'job.py'), 'check', backend,
                                  *(str(path) for path in snapshots)], cwd=full,
                                 capture_output=True, text=True, timeout=60)
        assert checked.returncode == 0, checked.stderr
        def content_hashes(path):
            files = sorted(path.rglob('*')) if path.is_dir() else [path]
            return {str(p.relative_to(path)) if path.is_dir() else p.name:
                    hashlib.sha256(p.read_bytes()).hexdigest() for p in files if p.is_file()}
        assert all(content_hashes(path) == content_hashes(archived) for path in snapshots)
        restart = tmp_path / 'restart-payload'
        restart.mkdir()
        shutil.copyfile(payload / 'job.py', restart / 'job.py')
        shutil.copyfile(full / 'reference-energy', restart / 'reference-energy')
        if archived.is_dir():
            shutil.copytree(archived, restart / name)
        else:
            shutil.copyfile(archived, restart / name)
        read_id = submit.submit_local(host='localhost', directory=str(restart),
            command=[producer, 'job.py', 'read', backend], cpus=1)
        # Remove submit-side inputs so a mistaken absolute reference cannot pass.
        shutil.rmtree(restart)
        read_spec = _drain_job(daemon, read_id)
        read_receipt = json.loads((Path(read_spec.cwd) / 'receipt.json').read_text())
        assert read_receipt['mode'] == 'read'
        assert read_receipt['energy'] == pytest.approx(receipt['energy'], abs=1e-9)
        validation = {
            'producer': receipt, 'read': read_receipt, 'snapshot_count': len(snapshots),
            'expected_outputs': planned.expected_outputs,
            'artifact_hashes': content_hashes(archived),
        }
        (tmp_path / 'validation.json').write_text(json.dumps(validation, indent=2))
        record_property('trexio_queue_validation', json.dumps(validation, sort_keys=True))
    finally:
        for running in daemon._running.values():
            running.popen.kill()
            running.popen.wait(timeout=10)
            running.close_logs()
        daemon._queue_lock_fd.close()
