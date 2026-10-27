# Asking for CPUs, memory and time

**Audience: you are submitting real work and want it scheduled, and stopped,
the way you expect.** Three options say what a job needs:

```sh
vq submit HOST --cpus 8 --mem-mb 16000 --time-limit 04:00:00 -d . -- python run.py
```

| Option | Means | Default |
|---|---|---|
| `--cpus N` | CPU slots the job claims | 1 |
| `--mem-mb N` | memory the job claims, in MB | the host's default charge, if it has one |
| `--time-limit HH:MM:SS` | wall-clock limit (`MM:SS` and plain seconds work too; `--time` and `--wall-time-seconds` are the same limit) | none |

What they do depends on the kind of host. A PBS or Slurm host turns them
into the scheduler's own directives, and the scheduler enforces them
([how each option is rendered](../backends.md)). The rest of this page is
about a host where the vq daemon runs the job itself.

## What the job sees

Each job gets its claim in its environment:

| Variable | Value |
|---|---|
| `VQ_CPUS` | `--cpus` |
| `VQ_MEM_MB` | `--mem-mb`, only when given |
| `VQ_WALL_TIME_SECONDS` | the time limit in seconds, only when given |

vq also sets the usual thread-pool variables to the CPU claim, unless the
daemon's own environment already sets them: `OMP_NUM_THREADS`,
`MKL_NUM_THREADS`, `VECLIB_MAXIMUM_THREADS`, `NUMEXPR_NUM_THREADS` and
`BLIS_NUM_THREADS` to `N`, and `OPENBLAS_NUM_THREADS` to 1. Without that, many
numerical libraries start one thread per core of the whole machine, and an
8-CPU job would compete with everything else on it. OpenBLAS stays at one
thread because it runs inside OpenMP regions, where threads of its own would
oversubscribe the cores. A program that starts its own processes, such as MPI
ranks, should use `VQ_CPUS` to size them.

## When the job starts

The daemon starts a pending job when its claim fits the host's budgets, which
the operator sets: `max_cpus`, `max_mem_mb` and `max_jobs`. Running jobs hold
what they claimed until they end. While a job waits, `vq status` gives its
place in line and what is holding it:

```text
state:        pending
queue position: 1 of 1 pending in local lane (dispatch order: priority, then submit time)
pending:      memory admission blocked: requested 1600 MB, 1500 MB available of 2000 MB local cap
```

A job whose claim can never fit, because it is larger than the cap itself,
says so instead. [Troubleshooting](troubleshooting.md#the-job-stays-pending)
lists every reason.

**An undeclared memory claim.** If the operator set `default_job_mem_mb`, a
job without `--mem-mb` is charged that much, and on a Linux host with cgroup
delegation it is also capped at it. Without that setting, an undeclared job
is charged nothing and can use whatever is free. Either way it gets no
`VQ_MEM_MB`. Declaring is how a large job gets the room it needs, and how
small jobs pack alongside each other.

**Priority.** Pending jobs start in order of `--priority` (default 0, higher
first), then of submission time:

```sh
vq submit HOST --priority 5 -d . -- python urgent.py
```

A higher priority moves a job ahead of others still waiting. It never stops a
job that is already running. Negative values put a job behind ordinary work.

## When the job goes over

| Limit | On Linux | On macOS |
|---|---|---|
| Time limit | stopped: `time_exceeded` | stopped: `time_exceeded` |
| `--mem-mb` | stopped: `oom_killed` | **not enforced** |
| Host memory ceiling | stopped: `oom_killed` | not enforced |
| No CPU activity | stopped: `starved` | not enforced |

**Time** is enforced everywhere. At the limit, vq sends the job's whole
process group SIGTERM, then SIGKILL after a grace period (10 seconds by
default). A program that checkpoints on SIGTERM can use that grace period.
`vq wait` exits 1 for a `time_exceeded` job.

**Memory** is enforced on Linux. With cgroup v2 delegation the kernel
enforces `--mem-mb` for every process the job started. Without it, vq's
watchdog sums the memory of the job's process group every few seconds and
stops the job once the sum is over. Separately, the watchdog stops any job
that holds more than 90% of the host's memory, declared or not. Both end as
`oom_killed`.

**Starvation**: on Linux a job whose processes used under 5% of one CPU for
300 seconds is stopped as `starved`, because it is almost certainly stuck.
[Troubleshooting](troubleshooting.md#the-job-ended-but-not-as-you-expected)
explains when a busy job can still look idle.

**On macOS** vq cannot read a job's processes, so it enforces only the time
limit. A job that claims `--mem-mb 100` and uses 400 MB still completes, and
no job is ever stopped as starved. `--mem-mb` still counts against the
memory budget, so declaring it still schedules the job honestly. The job's
peak use is still recorded: `vq status` shows it on the `usage:` line once
the job has finished.

The operator's [cgroup setup](../cgroup-setup.md) explains delegation, and
how to check that it is working on a host.

## Choosing the numbers

* **CPUs:** what the program actually uses. A Python script that calls
  NumPy uses as many threads as `OMP_NUM_THREADS` allows, so `--cpus 4` also
  limits it to four.
* **Memory:** the peak, plus a margin. The `usage:` line of a similar job
  that has finished gives its peak (the largest single process), and
  [`vq estimate`](../resource_estimation.md) reads vibe-qc's own estimate
  before the run.
* **Time:** long enough to finish, short enough that a hung run is caught.
  A job stopped at its limit keeps what it wrote:
  `vq fetch HOST JOBID --workspace` brings back its working directory, such
  as a checkpoint, and `vq fetch HOST JOBID --workdir` its `$VQ_WORKDIR`.

`vq submit auto` picks a host whose free memory fits `--mem-mb`
([placement](../placement_policy.md)). For a vibe-qc job, a calibrated
estimate can set both limits for you ([applying a calibration](../resource_estimation.md#applying-a-calibration)).
