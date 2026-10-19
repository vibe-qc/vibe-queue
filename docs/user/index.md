# Running jobs

**Audience: you have work to run and a machine to run it on.** Nothing here
assumes you administer that machine.

## The shape of it

vq has four verbs you will use constantly and about forty you will not.

```sh
vq submit compute -- python optimise.py    # queue it
vq queue                                   # see what is happening
vq status <jobid>                          # see one job in detail
vq fetch <jobid> -o results/              # bring the results back
```

`compute` there is an **SSH alias**, not a hostname. vq resolves it through
your `~/.ssh/config`, so a job goes to the same place `ssh compute` does. Set
`default_host` in `~/.config/vq/config.toml` and you can drop it entirely.

Submitting does not run anything. It writes a durable job spec, uploads your
workspace, and returns a job id. The daemon on the far side dispatches it when
the host's CPU, memory and concurrency budgets all allow. That gap between
"submitted" and "running" is the point of a queue, and `vq status` will tell
you which budget you are waiting on.

## Submitting

The form depends on what you are running:

```sh
vq submit HOST input.py                       # implies: python input.py
vq submit HOST --python /path/to/py input.py  # a specific interpreter
vq submit HOST -d DIR -- python run.py        # a whole directory as workspace
vq submit HOST -c ARCHIVE -- bash run.sh      # a tarball as workspace
```

Everything vq needs goes **before** `--`; everything after it is the command
to run. The separator is required whenever your own arguments could look like
vq options. vq refuses a payload that begins with an option rather than
queueing something malformed.

Declare what the job needs, and it will be scheduled honestly:

```sh
vq submit HOST --cpus 8 --mem-mb 16000 --time-limit 04:00:00 -- python run.py
```

An undeclared job is not free: the daemon charges it an assumed footprint, so
declaring is how you get scheduled sensibly rather than conservatively.

[Resource estimation](../resource_estimation.md) explains `vq estimate`, a
read-only preview of an existing vibe-qc dry-run manifest with explicit unknowns
and memory aggregation semantics.

[Calculation campaigns](../campaigns.md) describes chains of jobs whose verified
outputs feed each other (calculate, validate, render): the campaign file,
artifact manifests, how `vq campaign status` decides what may run next, and
`vq campaign run`, which runs a campaign on this machine and can be stopped
and restarted at any point, and `vq campaign cleanup`, which frees what a
run directory no longer needs.

## Watching

```sh
vq queue                 # the live queue, archived jobs hidden
vq queue --all           # every configured host, failures shown inline
vq queue --active        # everything not yet terminal
vq queue -s running      # one state
vq status <jobid>        # one job, with the tail of its output
vq logs <jobid>          # stdout and stderr
vq tail <jobid> FILE     # live-tail any file in the workspace
vq top                   # live CPU and memory per job
vq wait <jobid>          # block until terminal
```

To cap how long you wait, use `vq wait JOBID --timeout 30m`. The timeout accepts
seconds (`90` or `0.5`) or a number with one lowercase suffix: `s` for seconds,
`m` for minutes, `h` for hours, or `d` for days. Fractions such as `1.5h` work;
the minimum is 0.1 seconds. If the time runs out, the command exits 124 and
leaves the job running. Without `--timeout`, it waits until the job ends.

`vq queue --all` walks every host you have configured. A host that is
unreachable is reported in place rather than taking the whole listing down.

`vq events JOBID` shows the job's lifecycle timeline: submission, dispatch,
state transitions and kill requests. To focus on recent transitions:

```sh
vq events JOBID --kind state_transition --tail 10
vq events HOST JOBID --kind kill_requested --kind watchdog_kill --json
```

Repeat `--kind` to include several exact event names. `-n` / `--tail` selects
the last N matching events and keeps them in timeline order; `0` (the default)
shows all matches. The same selection works with `--json`. For a remote job,
the host serving its events must support these options; scheduler jobs use
their driver's event timeline.

## Getting results back

```sh
vq fetch <jobid> -o results/               # the workspace
vq fetch --workdir <jobid>                 # the per-job scratch directory
vq fetch-all HOST -o results/              # every terminal job on a host
```

On a host that runs its own vq daemon, a job has two places it may have written
to: the **workspace**, where it runs and where your script's relative paths
land, and a separate **scratch directory**, `$VQ_WORKDIR`. A plain `vq fetch`
copies the workspace and succeeds when the host proves the scratch directory
is empty. If that directory contains files, or its contents cannot be checked,
the command copies the workspace but exits with a hint to choose explicitly:
`--workdir` for scratch results, or `--workspace` when only the workspace is
wanted. A PBS or Slurm job has no separate scratch directory, so a plain
`vq fetch` needs no flag for it.

A file vq cannot read, or a symlink it must not follow, costs that entry and
not the job: the rest of the workspace still arrives, the fetch prints
`PARTIAL`, and the tree's `_vq/fetch-exclusions.json` names what is missing.
`vq fetch-all` also writes `<output_dir>/_vq/fetch-all-report.json` listing
every job it swept and how each one went, so you can check the coverage of a
batch without re-reading the command's output.

## When something goes wrong

* A job that will not start: `vq status` names the gate it is waiting on.
  If it is a budget, either the host is busy or your declaration is larger
  than the host's cap.
* A job you want to stop: [aborting a job](../abort.md).
* A job that failed for a reason that will not recur: [retrying](../retry.md).
* A job that is running but should yield: `vq pause` / `vq resume`, and
  [throttling](../throttle.md).

If you are reporting a bug, `vq doctor` output plus the job id is what makes a
report actionable.

## Beyond one job

```sh
vq submit HOST --array 25 -- python sweep.py       # 25 parallel siblings
vq submit HOST --chain 5 -- python neb.py          # 5 in strict sequence
vq submit HOST --depends-on JOBID -- python next.py
vq submit HOST --rerun-until CONVERGED -- ./iterate.sh
```

`--array` is parallel siblings; `--chain` is a strict sequence where each step
waits for the previous to succeed. `--rerun-until` re-submits until your script
writes a flag file, with a cap so a non-converging loop cannot run forever.

`--chain` and `--rerun-until` are accepted for a local submit and for a PBS or
Slurm host. vq refuses them for an ordinary remote host, because that host's
daemon never receives the chain or the flag; submit from the machine that will
run the jobs instead. `--array` works everywhere.

To hold a job until a time, give `--at` an ISO 8601 timestamp with an explicit
timezone:

```sh
vq submit HOST --at 2026-05-20T22:00:00Z -- python run.py    # not before 22:00 UTC
vq submit HOST --at 2026-05-20T17:00:00-05:00 -- python run.py
```

The daemon dispatches the job no earlier than that time, and the host's usual
budgets still apply. vq rejects a timestamp with no timezone rather than guess
which clock you meant, and a time in the past means ready now.

## If you know SLURM

Four verbs have SLURM-style names. Each is the same command under a second
name, so its flags stay vq's own:

| SLURM-style | vq |
|---|---|
| `vq sbatch` | `vq submit` |
| `vq squeue` | `vq queue` |
| `vq scancel` | `vq kill` |
| `vq sacct` | `vq status` |

## Particular programs

* [Running VASP](vasp.md): a complete input directory through a scheduler, the
  whole workspace fetched back, restarts done explicitly. It says plainly what
  has been qualified (on real hosts at two sites, and against a stand-in binary
  in the test suite) and what has not been established.
* [TREXIO through the queue](../trexio_queue.md): opt-in export and READ of
  TREXIO data as a job's output or input format.

```{toctree}
:hidden:

../abort
../retry
../throttle
../trexio_queue
../resource_estimation
../campaigns
vasp
```
