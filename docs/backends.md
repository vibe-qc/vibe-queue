# Machines with and without a batch scheduler

**Audience: you submit jobs and want to know what differs between a plain
machine and a PBS or Slurm cluster.** Setting the machines up is covered in
[installing vq on machines and clusters](cluster_install.md).

The commands are the same everywhere: `vq submit`, `vq queue`, `vq status`,
`vq logs`, `vq wait`, `vq kill` and `vq fetch`. What differs is who decides
when a job starts, where it runs and which limits it is held to.

| | Daemon host (no queue) | PBS/Torque | Slurm |
|---|---|---|---|
| `scheduler` in config | `local` (the default) | `pbs` | `slurm` |
| Who starts the job | the vq daemon on that host | PBS, on a compute node | Slurm, on a compute node |
| Where vq waits for room | vq's CPU, memory and job caps | the PBS queue | the Slurm partition |
| `--cpus N` | N of the daemon's CPU slots | `nodes=1:ppn=N` | `--cpus-per-task=N` |
| `--ntasks R` | not used | not used by Torque | `--ntasks=R` (MPI ranks) |
| `--gpus N` | N of the daemon's GPU devices | `nodes=1:ppn=CPUS:gpus=N` | `--gpus=N` |
| `--mem-mb M` | charged against the host's budget, capped by cgroup where available | `-l mem=` (or omitted, see below) | `--mem=M` |
| `--time-limit` | enforced by vq's watchdog | `-l walltime=` | `--time=` |
| Job scratch | workspace plus `$VQ_WORKDIR` | the staged workspace only | the staged workspace, plus optional `node_scratch_dir` |
| `--chain`, `--rerun-until` | from that host itself only | yes | yes |
| `vq pause` | SIGSTOP of the running job | a scheduler hold on a queued job | a scheduler hold on a queued job |

## A machine with no queue

A workstation or server running the vq daemon is the simplest target. vq is
the scheduler: a job starts as soon as the host's CPU, memory and concurrency
budgets all allow it, and `--priority` moves it ahead of other waiting jobs.

```sh
vq submit workstation --cpus 4 --mem-mb 8000 --time-limit 02:00:00 -d . -- python run.py
vq status <jobid>      # names the budget a pending job is waiting for
vq top                 # live CPU and memory per running job
vq pause <jobid>       # SIGSTOP; vq resume <jobid> continues it
```

Things only a daemon host does:

* **Two output locations.** The job runs in its workspace and also gets an
  empty scratch directory as `$VQ_WORKDIR`. A plain `vq fetch` copies the
  workspace; `vq fetch --workdir <jobid>` copies the scratch directory.
* **Pause, resume and throttle.** The daemon owns the process group, so it can
  stop and continue it, or lower its CPU weight; see [throttling](throttle.md).
* **Program paths in the environment.** A job submitted with
  `--program NAME` gets `VQ_PROGRAM_BIN`, `VQ_PROGRAM_PYTHON` or
  `VQ_PROGRAM_EXE`, so the payload need not hard-code paths; see
  [adding programs](user/programs.md).
* **Undeclared jobs pay a default.** A job without `--mem-mb` is charged the
  host's `default_job_mem_mb`.

To keep a busy desktop usable, [drain](drain.md) the host or cap it with the
`[daemon]` settings in its config.

## A PBS/Torque cluster

You submit to the scheduler host by name. The client sends the request to
the driver daemon, which stages the workspace under the cluster's
`scratch_root`, writes the batch script and runs `qsub` on the login node:

```sh
vq submit cluster --cpus 8 --mem-mb 16000 --time-limit 12:00:00 \
    -d ./input -- ./run.sh
vq queue cluster
vq status cluster <jobid>       # includes the PBS job id and execution host
vq logs cluster <jobid>
vq fetch cluster <jobid> -o results/
```

Name the host on `status`, `logs`, `kill` and `fetch`. Without it they go to
your `default_host`.

* **Queue and resources beyond the basics** go through `submit_extra` in the
  host config, for example `["-q", "long"]`, or a PBS Pro `ngpus` request.
* **Torque memory limits.** Some Torque installations turn `-l mem=` into a
  hard per-process limit that kills tightly sized jobs. Set
  `scheduler_mem_directive = "omit"` on such a host; vq still records the
  declared memory.
* **Wall time is binding.** The scheduler kills a job at its walltime, and vq
  reports it as `time_exceeded`. Declare a limit your queue allows.
* **Setup lines.** `scheduler_prologue` and `scheduler_epilogue` add shell
  lines around every job, for example `module load` lines.

## A Slurm cluster

Slurm works the same way, through `sbatch`:

```sh
vq submit slurm --cpus 16 --mem-mb 32000 --time-limit 24:00:00 \
    -d ./input -- ./run.sh
```

* **Accounts and partitions** go in `submit_extra`:
  `["--account", "my-group", "--partition", "standard"]`.
* **MPI ranks.** `--ntasks R` requests R tasks and `--cpus T` sets the CPUs
  for each, so an MPI program with 8 single-threaded ranks is
  `--ntasks 8 --cpus 1`.
* **Lane limits.** When `scheduler_max_wall_time_seconds` is set for the
  host, vq rejects a longer request before staging anything, and
  `scheduler_max_cpus` warns about a request wider than the partition's
  nodes. An unset limit means unknown, not unlimited.
* **Node-local scratch.** `node_scratch_dir` (for example `/tmp/$USER`) gives
  a job fast local disk; the job script copies results back to the shared
  workspace when the payload ends, and also when the scheduler signals it.

## What works differently on a scheduler host

* **Use paths that exist on the cluster.** vq writes your command into the
  batch script unchanged. A single-file submit would default to the driver's
  own Python, which does not exist on a compute node, so vq refuses it. Pass
  `--python` with a cluster interpreter, or use `-d` / `-c` with an explicit
  command:

  ```sh
  vq submit cluster --python /home/USER/envs/calc/bin/python input.py
  vq submit cluster -d ./input -- /home/USER/envs/calc/bin/python input.py
  ```

  A bare `python` is looked up on the cluster's `PATH` and is accepted.
* **No driver paths in the environment.** A scheduler job gets `VQ_PROGRAM`
  and, for a managed runtime, `VQ_PROGRAM_GIT_SHA`, but not the driver-local
  `VQ_PROGRAM_BIN`. A program's cluster-side launcher comes from a
  `command_wrapper` in the host config; see [adding programs](user/programs.md).
* **One output location.** There is no separate `$VQ_WORKDIR`, so a plain
  `vq fetch` brings back everything.
* **Pause holds, it does not stop.** On a scheduler host `vq pause` places a
  scheduler hold (`qhold`) that keeps a queued job from starting, and
  `vq resume` releases it. A job that is already running is the scheduler's;
  `vq kill` cancels the batch job. Throttling is a daemon-host feature.
* **Check before a long run.** `vq doctor cluster` verifies the driver, the
  scheduler commands on the login node and the program hooks, without
  submitting anything.

## Choosing a host automatically

With several hosts configured, `vq placement` explains where a job would run
by predicted completion time, and the optional
[fleet broker](operations.md#the-fleet-broker) holds work until a daemon host
or cluster lane has room. See [explainable placement plans](placement_policy.md).
