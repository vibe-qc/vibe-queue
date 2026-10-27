# Running vibe-qc calculations

**Audience: you write [vibe-qc](https://vibe-qc.com/docs/) inputs and want to
run them on another machine.** This page covers the vq features made for
vibe-qc. The vibe-qc site keeps its own page on the queue, which goes
further into CRYSTAL, ORCA and paper workflows.

Before you start, the host needs a registered vibe-qc environment, usually
`vibeqc-dev` (tracking `main`) and `vibeqc-release` (the latest tag). See
[adding programs](programs.md#venv-a-python-environment-with-a-git-checkout).

## Submit a script

A vibe-qc input is an ordinary Python file and runs unchanged under
`python water.py` on your laptop:

```python
# water.py
import vibeqc as vqc

mol = vqc.Molecule([
    vqc.Atom(8, [0.0,  0.00,  0.00]),
    vqc.Atom(1, [0.0,  1.43, -0.98]),
    vqc.Atom(1, [0.0, -1.43, -0.98]),
])
result = vqc.run_job(mol, basis="6-31g*", method="RHF", output="water")
print(f"E(SCF) = {result.energy:.6f} Ha")
```

Choose the environment, declare the resources, and submit:

```sh
vq submit workstation --branch main --cpus 4 --mem-mb 8000 --time-limit 01:00:00 water.py
```

`--branch` picks the interpreter through the host's `[hosts.X.branches]`
table; `--python PATH` names one directly. On a host with a `remote_python`
set, a bare `vq submit workstation water.py` uses that.

Add `--program` to record which registered environment ran the job, and
`--expected-sha` when the number matters:

```sh
vq submit workstation --program vibeqc-release --branch release \
    --expected-sha 0123456789abcdef0123456789abcdef01234567 water.py
```

The job then fails before it starts if the release checkout has moved since
you submitted. A job's environment carries `VQ_PROGRAM_GIT_SHA`, the
revision at dispatch, for your own result records.

For a cluster, give a cluster-side interpreter or submit a directory with an
explicit command; see
[what works differently on a scheduler host](../backends.md#what-works-differently-on-a-scheduler-host).

## Follow the calculation

```sh
vq output <jobid> -f      # stream the calculation's .out file: SCF trace, energies
vq progress <jobid> -f    # SCF iterations: energy, gradient, DIIS
vq status <jobid>         # state, resources, the tail of stdout
vq wait <jobid>
```

Both `-f` forms stop on their own once the job has ended and the file has
gone quiet. `vq progress` reads the `.scf.jsonl` structured log that
`run_job` writes.

`vq wait` returning means the job reached a terminal state, not that the SCF
converged. Its exit status is the job's exit code for `completed` and
`failed`, and 1 for any other terminal state. Check the `.out` file, or the
QVF's run status, for convergence.

## Get the results

```sh
vq fetch workstation <jobid> -o results/    # lands in results/<jobid>/
```

`run_job` writes `water.out`, `water.system`, `water.qvf` and its siblings
next to the script, in the workspace. Use `--workdir` only for files a job
wrote into `$VQ_WORKDIR`.

## Submit a QVF job container

A `.qvf` file can carry a pending calculation (the structure and a
`job.spec` with method, basis, charge and multiplicity) instead of a script.
[vibe-sketch](vibe_family.md) and [vibe-view](vibe_family.md) write these.
Submit it with the program that should run it:

```sh
vq submit workstation job.qvf --program vibeqc-dev
```

vq runs the registered vibe-qc on the container in place: the results, log,
citations and system manifest are written into the same file. No script is
executed, and `--python` and `--branch` do not apply. A container that has
already run is refused unless you pass `--qvf-force`.

Fetch just the container back:

```sh
vq fetch workstation <jobid> --name job.qvf -o results/
```

The file is published atomically as `results/job.qvf`, so a viewer never
reads a half-written container.

## Check what a calculation will write, and need

vibe-qc can plan a calculation without running the SCF. The dry run writes
`{output}.system` with its declared outputs and a memory estimate:

```sh
VIBEQC_DRY_RUN=1 python water.py          # writes water.system, runs no SCF
vq estimate water.system --input water.py
```

`vq estimate` is read-only. It reports what the manifest knows and says
plainly what it does not, rather than guessing. When the dry run recorded
which inputs it read, `vq estimate` refuses an `--input` that is not one of
them, so make a fresh dry run after editing `water.py`. When the dry run
could not model every allocation, the memory reads `unknown` instead of
falling back to a rougher figure; see
[evidence the dry run records](../resource_estimation.md#evidence-the-dry-run-records).
To keep the estimate with the
job, for later comparison against what the job really used:

```sh
vq estimate water.system --input water.py --json > water.estimate.json
vq submit workstation --estimate water.estimate.json --mem-mb 8000 water.py
```

Once an operator has fitted a calibration from completed jobs, a calibrated
estimate can set the limits for you with `--mem-from-estimate` and
`--time-from-estimate`. See [resource estimates](../resource_estimation.md).

For a local submit, `--vibeqc-preflight` runs the dry run for you at submit
time and records the declared outputs on the job:

```sh
vq submit localhost --vibeqc-preflight water.py
```

It runs your script, so it is opt-in, bounded to ten seconds and non-fatal.
It is not forwarded to remote submits, and is disabled for `--array` and
`--chain`.

## Sweeps and multi-step work

```sh
vq submit workstation --array 10 -d ./scan -- python point.py   # $VQ_ARRAY_INDEX = 0..9
vq submit workstation --depends-on <jobid> freq.py               # after an optimization
```

For a chain of calculations whose verified outputs feed the next step, such
as calculate on a cluster, validate, then render with vibe-view, use a
[campaign](../campaigns.md). [TREXIO through the queue](../trexio_queue.md)
covers exporting wavefunction data as a job output.
