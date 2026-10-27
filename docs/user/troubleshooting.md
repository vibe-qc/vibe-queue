# Troubleshooting

**Audience: a job did not do what you expected, and you want to know where to
look.** Each section starts from what you see. The quoted messages are vq's
own wording.

Three commands answer most questions:

```sh
vq status HOST <jobid>     # state, resources, every gate holding a pending job
vq events HOST <jobid>     # the job's timeline: submit, dispatch, transitions
vq doctor HOST             # SSH, the remote vq, the daemon or scheduler, programs
```

## vq cannot reach the host

Check the hop that vq uses, with no prompt, from the machine that submits:

```sh
ssh HOST hostname
ssh HOST /home/USER/vibe-queue/.venv/bin/vq --version   # the remote_vq path
```

* A password or host-key prompt means vq's own SSH calls will fail. Fix the
  key or the `~/.ssh/config` entry; vq has no SSH settings of its own.
* If the second line fails, `remote_vq` names the wrong path. Give the
  absolute path of the venv's `vq`: a non-interactive SSH shell rarely reads
  your login profile, so a bare `vq` is usually not on its `PATH`.
* For a PBS or Slurm host, the submission goes through its `scheduler_driver`
  first. Check that hop from the driver too.

`vq queue --all` reports an unreachable host in place and lists the others.
[Installing vq on machines and clusters](../cluster_install.md) has the
whole SSH layout.

## vq refuses the submission

* **The command begins with an option.** vq treats everything after `--` as
  the command, and a leading option is not an executable, so it refuses the
  submission rather than queue a malformed command. Put every vq option
  before `--`.
* **"--chain is local-only; host ... is remote"**, and the same for
  `--rerun-until`. An ordinary remote daemon never receives the chain or the
  flag. Submit on that host itself, or to a PBS or Slurm host, where both
  work. `--array` works everywhere.
* **A program or estimate refusal.** `--expected-sha` refuses a checkout
  that is not at that revision, and `--estimate` refuses a receipt made for
  other input files. Both are deliberate: the job would not compute what
  you asked for.

## The job stays pending

`vq status` lists every gate it can prove is holding the job, and several
can hold it at once. The most common:

| `vq status` says | What to do |
|---|---|
| "CPU request exceeds configured cap: requested N CPUs, cap is M" | The job can never fit this host. Resubmit with fewer `--cpus`, or ask for a larger `max_cpus`. |
| "memory request exceeds configured cap: charged N MB (daemon default for an undeclared job)" | Declare `--mem-mb`; the default charge for an undeclared job is larger than the cap. |
| "memory request exceeds configured cap: requested N MB" | Resubmit with less memory, or on a larger host. |
| "waiting on N dependenc(y/ies): ..." | A `--depends-on` job has not completed yet. |
| "held until ... (scheduled submit)" or "(retry backoff)" | An `--at` time, or the pause between `--retry` attempts. |
| "operator drain is active" or "paused for update" | The host's operator stopped dispatching; the job runs when that ends. |

A job that only fits once others finish shows the busy budget instead. An
empty list does not mean the job is ready: some of the daemon's own gates,
such as host-pressure pauses, cannot be proven from outside.

**On a PBS or Slurm host**, `submitting` means the driver is handing the job
to `qsub` or `sbatch`, and `vq status` shows the scheduler's job id once it
is accepted. `submit_outcome_unknown` means the driver lost the scheduler's
answer: vq does not know whether the job was accepted, so it never submits it
a second time on its own. Do not resubmit it by hand either; the operator
reconciles it (see the scheduler-driver section of
[operations](../operations.md#scheduler-driver-caps-state-and-fetch-contract)).

## The job ended, but not as you expected

`vq wait` returns once the job has reached a terminal state. Its exit status
is the job's own exit code for `completed` and `failed`, and 1 for every
other terminal state, so check the state, not only the status.

| State | What happened | What to do |
|---|---|---|
| `completed` | The command returned zero. | Check the program's own output for convergence: vq does not read it. |
| `failed` | A non-zero return, after any `--retry` attempts, or a validation or dispatch failure. | `vq logs` and `vq status` give the reason. |
| `oom_killed` | Over `--mem-mb` or the host's memory ceiling (on Linux), or the scheduler's out-of-memory accounting. | Raise `--mem-mb`. |
| `time_exceeded` | The wall-time limit. | Raise `--time-limit`, or restart from a checkpoint. |
| `starved` | The local watchdog saw too little CPU activity for too long. | See below. |
| `killed` | `vq kill`. | Nothing; that was a decision. |
| `aborted_by_queue` | The queue could not establish how the job ended, for example after the daemon was down. | Read `stdout.log` and `stderr.log` with `vq logs`, then resubmit. |

`--retry` re-queues only a plain `failed`; watchdog kills and `vq kill` are
not retried. [Aborting a job](../abort.md) and [retrying](../retry.md) go
further.

**`starved`** is decided from the job's CPU use, on Linux only. With cgroup
v2 delegation, vq counts every process the job started. Without it, vq
follows the process group and can miss workers that move to a group of
their own, as some build tools and `mpirun` setups do. A command that also
holds back its output for minutes, such as a long build piped through
`| tail`, can then be ended as `starved` while it works. Let the output
stream, or run such jobs on a host with
[cgroup delegation](../cgroup-setup.md). On macOS vq cannot read a job's CPU
or memory use, so no job there ends as `starved` or `oom_killed`; see
[asking for CPUs, memory and time](resources.md#when-the-job-goes-over).

## Fetch exits non-zero

* **"this local-daemon workspace does not establish retrieval of the job's
  separate output artifacts"**. A job run by a daemon has a workspace and a
  separate scratch directory, `$VQ_WORKDIR`. The workspace was copied, but
  the host could not prove the scratch directory empty. Fetch it with
  `--workdir`, or pass `--workspace` if only the workspace matters. PBS and
  Slurm jobs have no separate scratch directory.
* **`PARTIAL`**. An entry vq could not read, or a link it must not follow,
  was left out. The rest of the workspace arrived, and
  `_vq/fetch-exclusions.json` in the fetched tree names what is missing.

## A cluster job cannot find its program

A compute node has neither the driver's paths nor its environment. An
interpreter or binary named in the command must exist on the cluster, and a
module must be loaded in the host's `scheduler_prologue` or a per-program
hook. `vq programs` on the driver lists a cluster-only program as
`SCHEDULER_ONLY`; that is expected. See
[machines with and without a batch scheduler](../backends.md#what-works-differently-on-a-scheduler-host)
and [adding programs](programs.md#programs-on-a-pbs-or-slurm-cluster).

`vq programs HOST` reports `MISSING` for a registered program that a job
would fail to find, and `UNHEALTHY` for one that is installed but fails its
health check.

## Reporting a problem

Include the job id, its host, the output of `vq doctor HOST` and
`vq events HOST <jobid> --json`, and `vq --version` on both ends. Report
security issues privately, as described in the
[security policy](https://github.com/vibe-qc/vibe-queue/blob/main/SECURITY.md).
