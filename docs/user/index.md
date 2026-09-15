# Running jobs

**Audience: you have work to run and a machine to run it on.** Nothing here
assumes you administer that machine.

## The shape of it

vq has four verbs you will use constantly and about forty you will not.

```sh
vq submit compute -- python optimise.py    # queue it
vq queue                                   # see what is happening
vq status <jobid>                          # see one job in detail
vq fetch <jobid> -o results/               # bring the results back
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

`vq queue --all` walks every host you have configured. A host that is
unreachable is reported in place rather than taking the whole listing down.

## Getting results back

```sh
vq fetch <jobid> -o results/       # the workspace
vq fetch --workdir <jobid>         # the per-job scratch directory
vq fetch-all HOST -o results/      # every terminal job on a host
```

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

```{toctree}
:hidden:

../abort
../retry
../throttle
```
