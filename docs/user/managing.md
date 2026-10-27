# Managing jobs: names, pausing, stopping, rerunning and cleaning up

**Audience: you have jobs in the queue and want to keep them in order.**
This page follows a job from submission to disk cleanup.

## Name and tag what you submit

```sh
vq submit HOST --job-name h2o-scan-1 --tag scan-12 --tag sto3g -d . -- python run.py
```

* `--job-name` is a label for people. `vq queue` shows it in the `NAME`
  column, and `vq fetch` puts it in front of the job id in the directory it
  writes, such as `results/h2o-scan-1-e0c19d5b1cc3/`. Names may use letters,
  digits, `-`, `_` and `.`, up to 50 characters. vq rewrites a friendlier
  name and says so: `vq: sanitized --job-name 'H2O scan #1' -> 'H2O-scan-1'`.
* `--tag` groups jobs, and repeats. `vq queue HOST --tag scan-12` lists only
  the jobs that have every tag you name, and `vq usage` totals by tag.

A name is not unique and is not an id. Every command that acts on a job
takes the job id.

## Pause and resume

```sh
vq pause HOST JOBID          # SIGSTOP: the job stops using CPU
vq resume HOST JOBID         # SIGCONT: it carries on
vq pause HOST --all          # every running job on HOST
vq resume HOST --all
```

A paused job shows `suspended`. Its processes stay in memory, and it keeps
its claim on the host's CPUs and memory, so the queue does not start other
jobs in its place. Pausing frees the processor for something outside vq,
such as an interactive session on your workstation. To free the claim as
well, kill the job and resubmit it later. Time spent paused does not count
towards the job's time limit; `vq status` shows the total as
`paused_total:`.

A script that pauses the queue can tag what it paused, and later resume only
those jobs, leaving anything you paused by hand alone:

```sh
vq pause HOST --all --paused-by nightly-build
vq resume HOST --all --paused-by nightly-build
```

## Stop a job

```sh
vq kill HOST JOBID
vq kill HOST JOBID --reason "wrong basis set"
```

A pending job is marked `killed` at once. A running job's process group gets
SIGTERM, and the job ends as `killed`. `vq wait` then exits 1. `--reason` is
recorded and shown by `vq status`, as
`failure: killed by vq/operator request: wrong basis set`. A job waiting on
a killed job with `--depends-on` fails as well
([dependencies](workflows.md#dependencies)). [Aborting a job](../abort.md)
covers what a program can do on SIGTERM, and stopping work that vq did not
start.

## Run it again

```sh
vq resubmit HOST JOBID
vq resubmit HOST JOBID --cpus 4 --mem-mb 16000 --tag rerun
vq resubmit HOST --state oom_killed --mem-mb 32000
```

`vq resubmit` starts a new job from a finished one, whatever its end state:
completed, failed, killed or stopped at a limit. The new job gets a fresh id,
and a copy of the old job's workspace as it was left, including files it
wrote. It inherits the command and resources unless you override them with
`--cpus`, `--mem-mb`, `--wall-time-seconds`, `--priority`, `--retry`,
`--job-name` or `--tag` (`--clear-tags` drops the old tags). The old job is
not changed. `vq status` on the new one shows where it came from:

```text
parent_jobid: 840f687adb66 (resubmitted from)
```

`--state` resubmits every job in that state at once and prints one new id
per line, which suits a batch that ran out of memory. A job that is still
pending or running cannot be resubmitted: kill it first, or use
`vq kill --resubmit`, which does both.

For failures that may not recur, `--retry N` at submit time reruns the job
automatically ([retrying](../retry.md)).

## Clean up

A finished job keeps its workspace and scratch directory on the host until
they are cleaned up. `vq cleanup` shows what is there, then archives or
deletes it:

```sh
vq cleanup HOST                                  # list finished jobs and their sizes
vq cleanup HOST --archive --older-than 30d       # preview
vq cleanup HOST --archive --older-than 30d -x    # do it
vq cleanup HOST --delete --older-than 90d -x
vq cleanup HOST --restore JOBID -x
```

Nothing happens without `-x`: without it, each command only prints what it
would do. `--older-than` counts from when the job finished, in `s`, `m`,
`h`, `d` or `w`, and must be positive.

* **Archive** packs the workspace into a compressed tarball and removes the
  directory. The job stays known: `vq queue` hides it, and
  `vq queue --show-archived` lists it as `completed (archived)`. `vq fetch`
  and `vq resubmit` still work on an archived job, and `--restore` unpacks
  it back in place.
* **Delete** removes the job's record, workspace, scratch directory and
  archive. It cannot be undone.

The host's operator may also have set up automatic cleanup on a schedule
([automatic cleanup](../auto-cleanup.md)). Fetch what you need before then.

## How much you used

```sh
vq usage HOST                  # CPU-hours and wall-hours per tag
vq usage HOST --by submitter
vq usage HOST --by none        # one total
vq usage HOST --include-active # count running jobs so far
```

CPU-hours are each job's CPU claim times its run time, not measured CPU use.
A job with several tags is counted under each of them, and the table says
so. Jobs that never started are skipped and counted at the bottom. On a PBS
or Slurm host, vq uses the scheduler's own wall time, so time spent queued
in the scheduler is not charged.
