# Running ORCA and CRYSTAL

**Audience: you run ORCA or CRYSTAL inputs and want vq to queue them on a
workstation or a cluster.** vq ships a wrapper for each program in the
repository's `contrib/` directory:
[`run-orca.sh`](https://github.com/vibe-qc/vibe-queue/blob/main/contrib/run-orca.sh)
and
[`run-crystal.sh`](https://github.com/vibe-qc/vibe-queue/blob/main/contrib/run-crystal.sh).
`bash run-orca.sh --help` and `bash run-crystal.sh --help` print their full
usage.

## Why a wrapper

vq runs the command after `--` as an argument list, with no shell, so `<` and
`>` mean nothing to it. Both programs depend on them:

* **ORCA** writes its output to stdout and must be called by its absolute
  path, because it finds its sibling executables relative to that path.
  Parallel ORCA starts its own MPI ranks from a `%pal` block in the input.
* **CRYSTAL** reads its input from stdin in the serial binaries. The parallel
  `Pcrystal` and `Pproperties` read a file named `INPUT` in the working
  directory instead, and are started through `mpirun`.

The wrappers handle each convention, write `<input>.out` next to the input,
refuse an output path that would overwrite the input, and delete bulky
regenerable scratch after a successful run. A failed run keeps everything
for debugging.

The wrappers live in the vq checkout on the host that runs the job, for
example `/home/USER/vibe-queue/contrib/`. Give that absolute path in the
command; it is not copied with your workspace.

## Register the binaries

On the daemon host that has them, in that host's config:

```toml
[programs.orca]
kind = "binary"
binary = "/opt/orca/6.1.1/orca"
description = "ORCA 6.1.1"

[programs.crystal]
kind = "binary"
binary = "/opt/crystal/bin/crystal"
description = "CRYSTAL serial"

[programs.pcrystal]
kind = "binary"
binary = "/opt/crystal/bin/Pcrystal"
description = "CRYSTAL parallel"
```

`vq programs workstation` should list each as `OK`. A job submitted with
`--program orca` gets `VQ_PROGRAM_EXE` set to the registered binary. A job
inherits the daemon's environment, which under systemd or launchd usually
has a short system `PATH` without the program's directory, so pass that
variable to the wrapper rather than relying on `PATH`. See
[adding programs](programs.md) for the other kinds.

## ORCA on a machine with no queue

```sh
vq submit workstation --program orca --cpus 1 --mem-mb 4000 --time-limit 00:30:00 \
    -d ./h2o -- \
    sh -c 'ORCA_BIN="$VQ_PROGRAM_EXE" exec bash /home/USER/vibe-queue/contrib/run-orca.sh h2o.inp'
vq wait workstation <jobid>
vq fetch workstation <jobid> -o results/
```

`results/<jobid>/` then holds `h2o.out` and the artefacts later steps read:
`h2o.gbw` (orbitals for a restart or a property run), and `h2o.property.txt`,
`h2o.engrad`, `h2o.xyz`, `h2o.densities` and `h2o.hess` when the calculation
writes them. Add `--keep-scratch` after `run-orca.sh` to keep the temporary
files as well.

**Parallel ORCA.** Put the rank count in the input and claim the same number
of CPUs:

```text
%pal nprocs 4 end
%maxcore 2000
```

```sh
vq submit workstation --program orca --cpus 4 --mem-mb 10000 --time-limit 02:00:00 \
    -d ./h2o -- \
    sh -c 'ORCA_BIN="$VQ_PROGRAM_EXE" exec bash /home/USER/vibe-queue/contrib/run-orca.sh h2o.inp'
```

The wrapper does not run `mpirun`; ORCA does. It puts ORCA's own directory
on `PATH` first, so an `mpirun` installed there is found. If the `mpirun`
ORCA was built against lives elsewhere, add it in the same command:
`sh -c 'PATH=/opt/openmpi/bin:$PATH ORCA_BIN=...'`. `%maxcore` is per
process, so `--mem-mb` should cover `nprocs` times `%maxcore` plus a margin.

## CRYSTAL on a machine with no queue

Serial SCF:

```sh
vq submit workstation --program crystal --cpus 1 --mem-mb 4000 --time-limit 01:00:00 \
    -d ./mgo -- \
    sh -c 'CRYSTAL_BIN="$VQ_PROGRAM_EXE" exec bash /home/USER/vibe-queue/contrib/run-crystal.sh --serial mgo.d12'
```

Parallel SCF, with the rank count given explicitly. The wrapper does not
guess one: a parallel run without `--np N` exits with an error. Match `--np`
to `--cpus`:

```sh
vq submit workstation --program pcrystal --cpus 8 --mem-mb 16000 --time-limit 04:00:00 \
    -d ./mgo -- \
    sh -c 'PCRYSTAL_BIN="$VQ_PROGRAM_EXE" exec bash /home/USER/vibe-queue/contrib/run-crystal.sh --np 8 mgo.d12'
```

`mpirun` must be on the job's `PATH`; add its directory in the `sh -c` line
if it is not. The parallel binaries write their log to stderr, so the
wrapper merges stderr into `mgo.out`.

A successful run keeps `mgo.out`, `fort.9`, `fort.98`, `fort.34`, `fort.87`
and `dffit3.dat`, and deletes the per-rank `fort.*.peN` scratch.
`--keep-scratch` keeps that too.

**PROPERTIES** reads the wavefunction an SCF left behind, so submit the
fetched SCF directory with the properties input added:

```sh
vq fetch workstation <scf-jobid> -o results/
cp mgo.d3 results/<scf-jobid>/
vq submit workstation --program properties --cpus 1 -d results/<scf-jobid> -- \
    sh -c 'PROPERTIES_BIN="$VQ_PROGRAM_EXE" exec bash /home/USER/vibe-queue/contrib/run-crystal.sh --serial --properties mgo.d3'
```

with a `[programs.properties]` entry registered like the others. The
parallel form uses `Pproperties`, `PPROPERTIES_BIN` and `--np N`.

**The CRYSTAL23 demo binaries.** `--demo` selects `crystal23demo` (or
`properties23demo` with `--properties`), through `CRYSTAL23DEMO_BIN` and
`PROPERTIES23DEMO_BIN`. The demo is serial only, so `--demo` with `--np` is
refused, and it is limited to ten atoms per primitive cell.

## On a PBS or Slurm cluster

A batch job runs on a compute node, where the driver's binary paths do not
exist and vq sets no `VQ_PROGRAM_EXE`. Load the program in a per-program hook
instead, in the **driver's** config, and register the name on the driver as
`kind = "scheduler"`. The wrapper must be at a path the compute nodes can
read, such as the vq checkout in a shared home directory.

ORCA, with the wrapper as the hook's command wrapper:

```toml
[programs.orca]
kind = "scheduler"

[hosts.cluster.scheduler_program_hooks.orca]
prologue = ["module load orca/6.1.1", "export ORCA_BIN=\"$(command -v orca)\""]
command_wrapper = ["bash", "/home/USER/vibe-queue/contrib/run-orca.sh"]
```

The prologue lines run in the job script's shell just before the command,
so the exported variable reaches the wrapper. The command is then only the
input file:

```sh
vq submit cluster --program orca --cpus 4 --mem-mb 10000 --time-limit 02:00:00 \
    -d ./h2o -- h2o.inp
```

On Slurm, an input with `%pal nprocs 4 end` is usually submitted as
`--ntasks 4 --cpus 1`, so the allocation holds four tasks for ORCA's ranks;
on Torque `--cpus 4` requests `nodes=1:ppn=4`. Follow your site's guidance
for MPI programs.

CRYSTAL's wrapper takes options before the input, and vq refuses a command
that begins with an option. So keep the wrapper in the command and use the
hook only to load the program:

```toml
[programs.pcrystal]
kind = "scheduler"

[hosts.cluster.scheduler_program_hooks.pcrystal]
prologue = ["module load crystal/23 openmpi"]
```

```sh
vq submit cluster --program pcrystal --ntasks 16 --cpus 1 --mem-mb 32000 \
    --time-limit 08:00:00 -d ./mgo -- \
    bash /home/USER/vibe-queue/contrib/run-crystal.sh --np 16 mgo.d12
```

`vq doctor cluster` reports a hook whose name has no `[programs]` entry.
[Machines with and without a batch scheduler](../backends.md) lists how each
resource option becomes a scheduler directive.

## When a run fails

* **Exit 127 from the wrapper:** it did not find the binary, or `mpirun` for
  a parallel CRYSTAL run. `vq logs <jobid>` shows its message, and for a
  missing binary the `PATH` it searched.
* **Exit 2 from the wrapper:** a usage error, such as a missing input file, a
  parallel CRYSTAL run without `--np`, or `--demo` with `--np`. The message
  is in `vq logs <jobid>`.
* **Any other non-zero exit** is the program's own. The scratch is kept, and
  `vq fetch` brings it back with the output.

For queue-side causes, such as a job that never starts or one ended by a
limit, see [troubleshooting](troubleshooting.md).
