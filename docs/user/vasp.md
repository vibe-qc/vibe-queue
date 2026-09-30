# Running VASP

**Audience: people who run jobs.** Part of [Running jobs](index.md).

VASP uses vq's ordinary directory, command, scheduler and fetch interfaces. No
VASP adapter or VASP installation is bundled with vq. Use your site's licensed
installation and authorized potentials; keep executable paths, modules and
launcher configuration in your private operator configuration.

## What is qualified, and what is not

**Qualified on real hosts, at two sites.** On 2026-09-29 an independent
verifier accepted live qualification (#110). A small Gamma-point silicon deck
was run through vq on a Torque site and on a Slurm site. One-rank and two-rank
runs agreed to the printed precision in free energy and in forces; the input
files and the VASP binary had identical hashes in both runs; the fetched trees
matched the remote inventories file for file; and a running two-rank job that
was cancelled through vq ended with exit code 143 and its partial output
retained. That deck is a parity control, not a benchmark.

**Not established.** That the MPI child processes were gone after the
cancellation: neither site allowed a per-process scan, so that rests on indirect
evidence. Anything for another deck, another VASP build or another site. Repeat
the one- and two-rank parity check and a confirmed cancellation on your own
site before you rely on the recipes below there.

**Qualified in vq's test suite, with a stand-in.**
`test_fixed_filename_binary_workspace_lifecycle` in
`tests/test_scheduler_dispatch.py` runs the real directory staging,
batch-script rendering, exit-marker and whole-workspace fetch code against a
synthetic binary that contains no VASP code. Torque's `qsub`, `qstat` and
`qdel` and GNU `time` are simulated on the local machine, and the simulated
`qdel` is what signals the process group. The test checks that fixed input file
names (`INCAR`, `POSCAR`, `KPOINTS`, `POTCAR`) are read from the working
directory; that `--cpus` reaches `ppn=` and `VQ_CPUS` and `--mem-mb` reaches the
batch script; that a program prologue can set the thread counts (it sets one per
rank, overriding vq's `--cpus`-based default); that exit code 0 completes a job
and a non-zero one fails it; that a cancelled job ends with exit code 143 and
its partial output is fetched; and that binary restart output such as `WAVECAR`
comes back byte for byte. An independent verifier reproduced this on 2026-09-27.
The Slurm allocation mapping described below (`--ntasks`, `--cpus-per-task`) is
covered by the dialect tests, not by this workflow test.

## Prepare the program and allocation

On a machine where the executable is available, it can be registered as a
`kind = "binary"` program. For a scheduler reached through a driver, use the
existing scheduler-only registry (`kind = "scheduler"`, see the
[operator reference](../operations.md)) and per-program `scheduler_program_hooks`
([details](../operations.md#scheduler-driver-caps-state-and-fetch-contract)).
A driver-local binary path is not the cluster's executable path.

`--program NAME` identifies and validates the program; it does **not** substitute
the executable into your command. Supply an explicit command after `--`, or use
a site-provided launcher. Such a launcher must load the verified environment,
select the intended executable, invoke the site's MPI launcher inside the
allocation, and propagate its exit code. Do not put a second MPI launcher around
one that already launches MPI.

Discover the installed version and variants before selecting one. VASP documents
`vasp_std`, `vasp_gam` and `vasp_ncl`, but their presence and MPI dependencies are
installation-specific. See [VASP's installation guide](https://vasp.at/wiki/Installing_VASP.6.X.X).

For Torque, `--cpus N` requests one node with `ppn=N`; it does not launch N MPI
ranks. For Slurm, `--scheduler-tasks R` requests R tasks and `--cpus T` supplies
the CPUs per task. Verify the rendered allocation and the actual launcher rank
count. Explicitly set OpenMP and BLAS thread counts in the site launcher or
program prologue: vq's default thread caps use `--cpus`, so blindly launching N
ranks with those defaults can oversubscribe the allocation. Start qualification
with one thread per rank. Multi-node Torque VASP is outside this recipe.

## Submit a complete input directory

The conventional input deck contains `INCAR`, `POSCAR`, `KPOINTS` and an
authorized `POTCAR`. VASP reads these from its current directory; do not submit
`INCAR` using vq's single-file Python form. See [VASP input files](https://vasp.at/wiki/Category:Input_files).

Prepare a private directory containing only the intended deck and any explicitly
selected restart files. Do not include the VASP executable, source, unrelated
results or credentials. The directory contents are staged together.

The following assumes an operator has registered `vasp` on the example
`cluster` target and supplied `site-vasp` on its batch PATH. Replace these
example names with the approved site configuration:

```sh
vq submit cluster --program vasp --cpus 2 --mem-mb 4096 \
  --time-limit 00:10:00 --retry 0 --dir ./deck -- site-vasp
vq status cluster JOBID --json
vq logs cluster JOBID
vq kill cluster JOBID
vq fetch cluster JOBID -o ./results
```

Name the host on `kill` and `fetch`: without it they go to `default_host`
first, which need not be `cluster`. `-o` is a parent directory, so the workspace lands in
`./results/JOBID/` (`./results/NAME-JOBID/` when the job has a `--job-name`).

`kill` is optional cancellation, not part of a successful calculation. Keep the
returned vq ID, scheduler ID, actual allocation, executable digest/version,
launcher/module provenance and input hashes. Do not use `--expected-sha` to
invent a Git identity for a licensed executable. Record vq's actual source/build
identity separately from the VASP binary's identity.

## Completion, cancellation and output

vq records the command's exit code and scheduler state. A nonzero MPI/VASP exit
is a failed job; scheduler cancellation or wall-time termination is not a
successful calculation. Exit zero alone does not prove electronic or ionic
convergence. Inspect `OUTCAR`, `OSZICAR` and, when present, `vasprun.xml` for the
requested calculation and compare against your numerical reference.

Fetch the whole workspace, including failed-job outputs. This retains `OUTCAR`,
`OSZICAR`, `CONTCAR`, `vasprun.xml`, `WAVECAR`, `CHGCAR`, other generated files,
stdout/stderr and `_vq` evidence when present; no VASP-specific extension filter
is applied. Check fetch exclusions, file counts and hashes before cleanup. The
fetched directory can contain the staged potential: it is private licensed data,
not a public fixture or a directory to commit wholesale.

For initial qualification, use a scheduler target with node-local scratch
disabled. On shared scratch, files already written remain available to fetch
after cancellation. With `node_scratch_dir` enabled, a signal can end the wrapper
before copy-back; only shared logs are guaranteed. SIGKILL or node loss can also
prevent an exit marker. Preserve partial evidence and do not assume a killed
job produced a usable restart file. Confirm scheduler termination, including MPI
children, before considering cancellation complete.

## Restart explicitly

Keep automatic retries disabled for the initial workflow. A queue retry reuses
job state; it is not a chemistry-aware VASP restart. Create a new input directory
and a new job for each restart, preserving the original output unchanged.

For a fresh calculation, set `ISTART = 0` and `ICHARG = 2` and omit old restart
files from that new directory. For a wavefunction or charge-density restart,
copy only verified, compatible `WAVECAR`/`CHGCAR` files and set `ISTART`/`ICHARG`
deliberately for that calculation. Record their hashes and parent job ID. Review
[ISTART](https://vasp.at/wiki/ISTART) and [ICHARG](https://vasp.at/wiki/ICHARG),
including fallback behavior when a restart file is invalid. vq neither rewrites
these tags nor deletes old VASP outputs for you.
