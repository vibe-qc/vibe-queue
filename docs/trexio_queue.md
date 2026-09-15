# TREXIO export and READ through vq

QVF remains the default calculation container. TREXIO is an opt-in workload
output/input format; vq transfers its contents without interpreting orbitals.
TREXIO HDF5 is one regular file (normally `run.trexio.h5`), while the text
backend is a directory (normally `run.trexio`). Keep that directory intact.

## Select and check the workload runtime

The execution host's selected vibe-qc interpreter must include vibe-qc's
optional TREXIO dependency and the requested backend. Installing vq alone does
not provide either. Use `vq programs HOST --json` and the existing managed
program import/runtime identity probe to establish the executable and source
identity. An import of vibe-qc alone does not prove TREXIO backend support:
probe `trexio` with the same interpreter, then run a small export/readback for
**each** backend you intend to use. A library built without HDF5 may import
successfully but cannot satisfy an HDF5 job.

The opt-in integration gate below uses vq's existing
`run_import_runtime_identity_probe` for `vibeqc` and `trexio`, followed by real
HDF5 and text writes, decoding of every transferred result and queued READ.
Missing imports or backends fail the selected gate; they are not accepted as
skips. No new dependency is added to vq itself.

## Submit the whole input directory

For example, put `job.py` inside `payload/`:

```python
import vibeqc

molecule = vibeqc.Molecule([
    vibeqc.Atom(1, [0.0, 0.0, 0.0]),
    vibeqc.Atom(1, [0.0, 0.0, 1.4]),
])
vibeqc.run_job(
    molecule, basis="sto-3g", method="rhf", output="run",
    trexio=True, trexio_backend="text",  # or "hdf5"
)
```

On a local execution host with a qualified interpreter:

```sh
vq submit localhost --dir payload --vibeqc-preflight -- /path/to/qualified/python job.py
```

Opt-in preflight reads the producer's `plan.files[].path` entries, including
TREXIO directories, into `expected_outputs`. It is best-effort metadata
collection, not a backend acceptance test. Check `vq show JOBID --json` for
those planned outputs and the eventual queue outcome.

For SSH or scheduler execution, select the managed program and the runtime
on the **execution host**, following the program/hook rules in
[agent interaction](agent_interaction.md). A path to an interpreter on the
submitting workstation is not a portable scheduler command. Input files must
be in the submitted `--dir` or `--compressed` payload; submitting only a Python
script does not stage adjacent restart artifacts.

After the job completes, full fetch includes the complete workspace. Named
fetch selects only the requested file or directory, without queue sidecars:

```sh
vq fetch HOST JOBID --workspace -o results
vq fetch HOST JOBID --name run.trexio -o selected-text
vq fetch HOST JOBID --name run.trexio.h5 -o selected-hdf5
```

Choose the artifact that the job actually produced. If it wrote beneath the
recorded scratch workdir, use `--workdir --name run.trexio`; for nested output
add `--subdir results`. Workdir selection never falls back to the workspace or
its archive. A full workspace archive preserves a workspace TREXIO directory;
a separately swept workdir does not become part of that archive.

For a READ job, copy the fetched HDF5 file **or the whole text directory** into
`restart-payload/`, alongside its Python script. Use a relative input path:

```python
vibeqc.run_job(
    molecule, basis="sto-3g", method="rhf", output="restart",
    initial_guess="read", read_from="run.trexio",  # or "run.trexio.h5"
)
```

Submit `restart-payload/` as a directory with the target's qualified command.
The source should be scientifically compatible with the requested calculation;
vibe-qc owns those checks. Queue transfer acceptance is not a claim that an
arbitrary wavefunction can restart an arbitrary calculation.

## Named directory contract

The source name is one basename. Directory children may be ordinary files or
subdirectories; symlinks, hardlink archive entries and special files are refused.
Every selected path must stay inside that artifact. Duplicate or unrelated tar
members are refused. Ownership checks and explicit source selection remain the
same as for named files.

A fetch stages the complete tree before publication. Transfer/validation
failure preserves the previous destination. Re-fetch replaces the entire old
directory, so removed output files do not linger. It refuses destination
symlinks and file/directory type changes. It does not guarantee a consistent
snapshot while a producer is writing; fetch a completed job for readback.

Both ends of SSH named-directory transfer need this feature. Older senders
refuse a directory and older receivers refuse a directory tar root; upgrade
through the normal release process, or use full workspace fetch in the interim.
Regular-file named transfer keeps its existing wire shape.

## Repeat the real integration gate

From a development checkout, set `VQ_TREXIO_PYTHON` to an already qualified
vibe-qc interpreter. Supply its required native-library and basis environment.
The gate runs one numerical process at a time, uses an isolated queue daemon,
and does not contact a fleet daemon or scheduler:

```sh
VQ_TREXIO_PYTHON=/path/to/qualified/python \
  PYTHONDONTWRITEBYTECODE=1 \
  env -u PYTEST_ADDOPTS PYTHONPATH="$PWD/src" \
  .venv/bin/python -m pytest tests/test_trexio_queue_integration.py \
  -v -p no:cacheprovider --junitxml=trexio-queue.xml -o junit_family=legacy
```

Optionally set `VQ_TREXIO_EXPECTED_CORE_SHA256` to pin the compiled producer.
Retain the exact producer commit/runtime qualification and queue commit beside
the XML receipts, which record backend, energy, native hash, expected outputs,
artifact hashes and READ results. Without the explicit interpreter the ordinary
queue suite skips this optional gate; that skip is **not** integration evidence.

The gate generates real HDF5/text fixtures, drives submission and daemon
execution, checks full and named fetch, loopback SSH tar transport, the actual
scheduler result extractor over a mocked download, archive/fetch, and queued
READ after removing the original submit-side input directory. Those transport
checks do not substitute for deployment or a live scheduler acceptance run.
