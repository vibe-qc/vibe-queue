# Arrays, dependencies, chains and reruns

**Audience: one job is not enough.** You want a parameter sweep, a job that
waits for others, a fixed sequence of steps, or a calculation that repeats
until it converges. vq has one submit flag for each:

| You want | Flag | Runs |
|---|---|---|
| N independent variations, in parallel | `--array N` | N jobs at once, as budgets allow |
| A job that starts only after others succeed | `--depends-on JOBID` | when every named job completed |
| A job that starts after others end, however they end | `--depends-on-any JOBID` | when every named job is terminal |
| N steps in strict order | `--chain N` | one after another, each after the previous succeeded |
| Repeat until the job writes a flag file | `--rerun-until PATH` | again and again, up to `--rerun-max` |

Every job vq starts is an ordinary job: it has its own id, its own copy of
the submitted directory, and its own scratch directory, `$VQ_WORKDIR`.
`vq status`, `vq logs`, `vq kill` and `vq fetch` work on each one.

**The rule that shapes everything below:** a job sees only its own
workspace. It does not see the files another job wrote there, not even
those of the array element beside it or the chain step before it. To hand
results from one job to another, either write them to a directory both can
reach on that host, or fetch them to your machine and work on them there.
The worked example does both.

## A sweep and its analysis, worked through

Five points of a scan, then one job that picks the best. `point.py` runs
once per array element and branches on its index:

```python
import os
import pathlib
import sys

i = int(os.environ["VQ_ARRAY_INDEX"])          # 0 .. N-1
scale = 0.90 + 0.05 * i
energy = -76.0 + 0.1 * (i - 2) ** 2            # stands in for a real calculation
out = pathlib.Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
(out / f"point-{i}.txt").write_text(f"{scale:.2f} {energy:.4f}\n")
```

`collect.py` reads what the five wrote:

```python
import pathlib
import sys

rows = [p.read_text().split() for p in pathlib.Path(sys.argv[1]).glob("point-*.txt")]
scale, energy = min(rows, key=lambda row: float(row[1]))
print(f"{len(rows)} points; lowest energy {energy} at scale {scale}")
```

Submit the array, then the analysis with one `--depends-on` per element. The
results directory is an absolute path on the host that runs the jobs, so
every job finds the same one:

```sh
ids=$(vq submit workstation --array 5 -d . -- python point.py /home/USER/scans/h2o)
deps=""
for id in $ids; do deps="$deps --depends-on $id"; done
last=$(vq submit workstation $deps -d . -- python collect.py /home/USER/scans/h2o)
vq wait workstation $last
vq logs workstation $last
```

`vq submit --array 5` prints the five job ids, one per line, and the loop
turns them into `--depends-on` flags. Leave `$ids` and `$deps` unquoted so
the shell splits them. (In zsh, which does not split, write `${=ids}`.)
`vq status` on the analysis job lists its dependencies and whether they are
ready, and its output ends with the answer:

```text
5 points; lowest energy -76.0000 at scale 1.00
```

To work on the results on your own machine instead, fetch each element.
Each one lands in its own directory under `results/`:

```sh
for id in $ids; do vq fetch workstation $id -o results/; done
```

`vq queue workstation --array-group GID` lists the elements again later.
`GID` is the group id that `vq status` shows on each element's `array:` line.

## Arrays

```sh
vq submit HOST --array 25 -d . -- python sweep.py
```

Each element gets:

| Variable | Value |
|---|---|
| `VQ_ARRAY_INDEX` | its position, `0` to `N-1` |
| `VQ_ARRAY_TOTAL` | `N` |
| `VQ_ARRAY_GROUP_ID` | the id shared by the whole array |

There is no gang scheduling: elements start independently as the host's CPU
and memory budgets allow, and one element failing does not stop the others.
`--array` works on every kind of host, including PBS and Slurm, where each
element is its own batch job. Every element copies the whole submitted
directory, so a large `-d` costs N copies.

## Dependencies

```sh
A=$(vq submit HOST -d . -- python prepare.py)
vq submit HOST --depends-on $A -d . -- python run.py
vq submit HOST --depends-on-any $A -d . -- python cleanup.py
```

* `--depends-on JOBID` waits until that job has **completed**. If it ends any
  other way (failed, killed, out of memory, out of time, starved), the waiting
  job is marked `failed` without running, with the failure
  `predecessor JOBID failed (state=...)`. This is Slurm's `afterok`.
* `--depends-on-any JOBID` waits until that job has ended, however it ended,
  and then runs. Use it for cleanup or reporting that must happen even after
  a failure. This is Slurm's `afterany`.

Both repeat, and all named jobs must satisfy them. They combine:
`--depends-on A --depends-on-any B` waits for A to complete and for B to
end. The jobs named must already exist in your queue on that host, so a
typo is refused at submit time. `vq status` shows a waiting job's
`depends_on:` line, and the pending reason `waiting on N dependenc(y/ies)`.

## Chains

```sh
vq submit HOST --chain 5 -d . -- python step.py
```

`--chain N` submits N copies that run strictly one after another: step `k`
starts only after step `k-1` completed. If a step fails, every later step is
marked `failed` without running. Each step gets `VQ_CHAIN_INDEX` (`0` to
`N-1`), `VQ_CHAIN_TOTAL` and `VQ_CHAIN_GROUP_ID`.

Each step starts from a fresh copy of what you submitted, not from the
previous step's workspace. A step that needs the previous step's output must
read it from a shared directory, as in the worked example. For iterations
that should carry their own files forward, use `--rerun-until` instead.
`--chain` and `--array` cannot be combined.

## Rerun until converged

```sh
vq submit HOST --rerun-until '$VQ_WORKDIR/CONVERGED' --rerun-max 10 -d . -- python iterate.py
```

When the job completes, the daemon looks for the flag file. If it is not
there, it queues the next iteration, up to `--rerun-max` reruns (default 10).
Keep the single quotes: the daemon, not your shell, replaces `$VQ_WORKDIR`
with each iteration's own scratch directory. So the script signals that it
has converged by creating that file:

```python
import os
import pathlib

iteration = int(os.environ["VQ_RERUN_COUNT"])   # 0 on the first run
state = pathlib.Path("state.txt")
previous = state.read_text() if state.exists() else None
# ... one more iteration, starting from `previous` ...
converged = iteration >= 2                       # your own convergence test
state.write_text(f"after iteration {iteration}")
if converged:
    pathlib.Path(os.environ["VQ_WORKDIR"], "CONVERGED").touch()
```

Unlike chain steps, each rerun starts from a copy of the **previous
iteration's** workspace, so files the job wrote there carry forward, as
`state.txt` does here. Its `stdout.log` carries forward too and grows by one
iteration each time. Each iteration is a new job with its own id, which
depends on the one before; `VQ_RERUN_COUNT` and `VQ_RERUN_MAX` tell the
script where it is.

A failed iteration is not rerun: a failure is a different signal from "not
converged yet". Use `--retry` for failures that may not recur
([retrying](../retry.md)). When the cap is reached without the flag, the
daemon stops and logs a warning. Resubmit with a higher `--rerun-max` if
more iterations are needed.

## Where each works

`--array`, `--depends-on` and `--depends-on-any` work on every host. vq
accepts `--chain` and `--rerun-until` for a local submit and for a PBS or
Slurm host, and refuses them for an ordinary remote host, whose daemon never
receives them. Run `vq submit` on that host itself instead.

For longer pipelines whose steps must be checked before the next one runs,
see [calculation campaigns](../campaigns.md). To hold a job until a set time,
use `--at`, described in [running jobs](index.md#beyond-one-job).
