# Explainable placement plans

**Audience: operators** evaluating where a calculation should run across
daemon hosts and PBS or Slurm lanes.

`vq placement explain` applies the completion-time placement policy to one
job and shows the whole decision: every target it rejected and why, and for
each target it would accept, the predicted completion time with every
component and its source. It is local and read-only. It reads two files,
and it submits, reserves and contacts nothing. `vq placement fleet` builds
the fleet file from live daemon evidence (below). The broker keeps its strict
FIFO pass and capacity ranking unless its operator opts in to ordering by
this policy ([opt-in completion-time ordering](design_broker.md)).
`vq submit auto` and manual target selection are unchanged.

```sh
vq placement explain fleet.json request.json
vq placement explain fleet.json request.json --json
vq placement explain fleet.json request.json --now 2026-10-12T12:00:00+00:00 --max-age 600
```

## The fleet: `vq.placement-fleet/1`

One record per target: a daemon host, or a scheduler lane (a partition or
queue with one node shape). Each record describes what a job can actually
get.

```json
{
  "schema_name": "vq.placement-fleet/1",
  "targets": [
    {"name": "cluster-a/compute", "kind": "scheduler",
     "observed_at": "2026-10-12T11:59:00+00:00", "reachable": true,
     "os": "Linux", "arch": "x86_64", "isa_levels": ["x86-64-v2", "x86-64-v3", "x86-64-v4"],
     "programs": {"vibe-qc": ["<runtime sha256>", "<another runtime sha256>"]},
     "accounts": ["chem"], "queue": "compute",
     "data_access": ["shared-inputs", "results"],
     "node": {"cpus": 128, "mem_mb": 512000, "scratch_mb": 1000000}, "max_nodes": 16,
     "max_walltime_seconds": 172800,
     "queue_wait_seconds": {"low": 14400, "high": 28800, "source": "history"},
     "speed_factor": 2.0, "speed_source": "benchmark",
     "staging_mb_per_second": 200, "transfer_mb_per_second": 200},
    {"name": "workstation", "kind": "daemon",
     "observed_at": "2026-10-12T11:59:00+00:00", "reachable": true,
     "os": "Linux",
     "programs": {"vibe-qc": ["<runtime sha256>"]},
     "walltime_unlimited": true,
     "node": {"cpus": 32, "mem_mb": 128000, "scratch_mb": 500000},
     "free": {"cpus": 32, "mem_mb": 120000, "scratch_mb": 250000}}
  ]
}
```

* **`node`** is one compute node's CPUs, memory and scratch: never the cluster's
  total, and never the login node. A daemon host is one node.
  Optional **`scratch_mb`** is the scratch capacity in MB available to one job
  on each node, after any per-job or per-user quota. Zero means no scratch;
  an omitted value is unknown. For shared storage, record only the capacity
  each node's job allocation can use, rather than the whole filesystem's size.
  Optional **`gpus`** is the whole GPUs on one node. Zero means none; an
  omitted value is unknown.
* **`programs`** lists runtime digests per program; the first is the lane's
  current runtime.
* **`os`** is the compute node's operating-system label, such as `Linux` or
  `Darwin`. Use the same label as the workload's runtime profile. An omitted
  value is unknown; the planner does not infer it from the client or login node.
* **`accounts`** lists the accounts permitted for a job on this target.
  An omitted or `null` list is unknown; an empty list permits no account.
  An explicit request must match a listed account exactly, including case.
  Program availability and free capacity do not establish account permission.
* **`queue`** identifies this scheduler lane's PBS queue or Slurm partition.
  It is one case-sensitive label using the same bounded syntax as program
  names. Missing or `null` means unknown; the target's name does not establish
  its queue. A daemon host cannot declare a queue. This field identifies the
  lane; its permitted accounts, runtime and node shape still need their own
  capability evidence.
* **`data_access`** lists logical data-access labels available to a job's
  execution account on the target's compute nodes, for example `shared-inputs`
  and `results`. Record a label only when access is established for the whole
  lane's node shape and execution identity. Missing or `null` means unknown;
  an empty list records no access. Use these labels for datasets or storage
  capabilities, keeping actual paths, endpoints and credentials in external
  configuration. Labels are case-sensitive and each list holds at most 256.
* **`licenses`** lists license-eligibility labels established for the job's
  execution account on this target. For example, a workload may need both
  `solver-base` and `solver-gradients`. Missing or `null` means unknown; an
  empty list establishes no eligibility. Installing a program or recording
  its runtime digest does not establish license permission. Use logical,
  case-sensitive labels (at most 256), keeping license-server addresses,
  credentials and terms in external configuration. These declarations describe
  eligibility, not free token counts or a reservation of a floating license.
* **`max_walltime_seconds`** is the maximum walltime one job may request.
  Use **`walltime_unlimited: true`** only when evidence establishes that the
  target has no walltime cap. It cannot be combined with a numeric limit.
  An omitted limit without this flag is unknown and cannot establish that
  an explicit walltime request fits.
* **`free`** is a daemon host's capacity right now. A scheduler lane has none;
  model its wait with **`queue_wait_seconds`** instead. Its `source` is
  `history` (bounded past waits), `declared` (an administrator's figure) or
  `scheduler-estimate`. A scheduler's start estimate is advisory: Slurm needs
  suitable backfill configuration to give one, PBS may give none, and neither
  is a guarantee before submission. Nothing submits a probe job to find out.
  A daemon's optional **`free.scratch_mb`** is its unreserved scratch headroom
  now. It is separate from the job's scratch capacity in `node.scratch_mb`.
  Its optional **`free.gpus`** is its devices not assigned to a job now: the
  `gpu_devices` count minus `used_gpus` in its admission snapshot (see
  [GPU admission on daemon hosts](SPEC.md)).
* **`speed_factor`** is execution speed relative to the request's reference
  estimate, with its `speed_source`: `benchmark` (for example a
  [runtime benchmark campaign](benchmark_campaigns.md)), `declared` or
  `assumed` (the default 1.0).
* Use logical names: do not put private host names in a shared file.

## The request: `vq.placement-request/1`

```json
{
  "schema_name": "vq.placement-request/1",
  "program": "vibe-qc",
  "runtime_digest": "<runtime sha256>",
  "os": "Linux",
  "isa_level": "x86-64-v4",
  "account": "chem",
  "queue": "compute",
  "required_data_access": ["shared-inputs", "results"],
  "cpus": 16, "nodes": 1, "mem_mb_per_node": 64000,
  "mpi_ranks": 2, "threads_per_rank": 8,
  "scratch_mb_per_node": 100000,
  "wall_time_seconds": 14400,
  "runtime_seconds": {"low": 6000, "high": 8400},
  "input_mb": 100, "output_mb": 500
}
```

`runtime_digest`, when given, is never substituted by another runtime.
Optional `os` requires an exact, case-sensitive match with the target's
operating-system label. A different or unknown OS rejects the target even if
its runtime digest and architecture match. Omitting `os` adds no OS constraint.
Text and JSON explanations preserve the requested OS.
Optional `account` requires a recorded match in the target's `accounts` list.
Unknown permissions reject that target; if no target has a permitted match,
the request is held with the reasons. Omitting `account` adds no account
constraint. Text and JSON explanations preserve the requested account.

Optional `queue` requires an exact match with a scheduler target's recorded
PBS queue or Slurm partition before completion-time ranking. An unknown or
different queue, or a daemon target, is rejected; no eligible target produces
a hold with the reasons. The value is one label, rather than a comma-separated
list or scheduler expression. A matching target name is insufficient evidence.
The same queue name may match lanes on several clusters; each still undergoes
all other eligibility checks. Omitting `queue` or setting it to `null` adds no
queue constraint. Text and JSON preserve the requested queue. The planner
reads these declarations locally without contacting a scheduler or changing
its submission configuration.

Optional `required_data_access` lists every access label the job needs. All
must appear exactly in the target's `data_access` list before it can be ranked.
Missing labels are reported separately; unknown access rejects the target.
If none qualifies, the explanation reports a hold. Omitting the requirement
or using an empty list adds no access constraint. Both lists use the same
bounded labels as program names, rather than paths or URLs. Text and JSON
preserve the requirements. These are observed capability declarations; the
planner reads them locally without probing storage or granting permissions.

Optional `required_licenses` lists every license-eligibility label the job
needs. All must match the target's `licenses` exactly before ranking. Unknown
eligibility or any missing label rejects the target; no eligible target
produces a hold with the reasons. Omitting the requirement or using an empty
list adds no license constraint. Text and JSON preserve the requirements.
Both lists use the same bounded labels as program names. The planner reads
the declarations locally; it does not contact a license server, grant rights
or acquire tokens. Record eligibility for the actual execution identity and
node scope, and let the program's normal license checks govern execution.

Optional `mpi_ranks` and `threads_per_rank` declare the CPU layout. Supply
both as positive integers, with `cpus = mpi_ranks * threads_per_rank` and at
least one rank per requested node. The planner distributes whole ranks as
evenly as possible across the nodes, keeping each rank's threads together.
Each node must have at least `ceil(mpi_ranks / nodes) * threads_per_rank`
CPUs. For example, three 8-thread ranks across two nodes require 16 CPUs on
one node, even though the total is only 24. A lane of 12-CPU nodes is rejected;
a lane of 16-CPU nodes fits. A daemon's free-now check still needs all
requested CPUs on its single node. Text and JSON preserve the layout.
Omitting both fields retains the existing `ceil(cpus / nodes)` check.
These are placement requirements; the planner does not launch MPI, set
thread counts or configure affinity.

`runtime_seconds` is the execution time on a speed-1.0 target. An explicit
user limit stays authoritative wherever the job is finally submitted.
The optional `wall_time_seconds` is the positive integer limit requested at
submission, not an execution estimate. It is checked even when no runtime
estimate is available, and is never divided by the target's speed factor or
replaced by a shorter estimate. Queue wait, staging and transfer time do not
consume this limit. The text explanation displays it, and JSON preserves it
in `request`.

Optional `scratch_mb_per_node` is a positive integer requirement in MB on
**each** requested node. It must fit that node's recorded scratch capacity;
multiple nodes do not combine their scratch to meet a per-node requirement.
A daemon gets a zero queue-wait estimate only when its free CPUs, memory and
requested scratch all fit now. Unknown or insufficient free scratch uses its
wait model, or leaves the queue wait unknown. Omitting the requirement keeps
the existing behavior. Text and JSON explanations preserve the requirement.
These records describe capacity; this command does not allocate scratch or
measure a filesystem.

Optional `gpus_per_node` is a positive integer of whole GPUs on **each**
requested node. For vq's single-node GPU jobs it equals `vq submit --gpus`.
It must fit the target's recorded `node.gpus`; an unknown count rejects. A
daemon gets a zero queue-wait estimate only when its free GPUs fit as well;
unknown or insufficient free GPUs use its wait model or leave the wait
unknown. Omitting the requirement keeps the existing behavior. Text and JSON
explanations preserve it. This is capacity, not a GPU type or model.

## Hard requirements first

A target is rejected, with **every** reason that applies, when it:

* was unreachable or in maintenance when observed;
* was observed longer ago than `--max-age` (default 300 s), or more than
  60 s in the future (clock skew);
* lacks the program, or the requested runtime digest;
* has a different or unknown operating system when `os` is requested;
* has another architecture, or does not support the requested ISA level;
* does not permit the requested account, or its permitted accounts are unknown;
* is a daemon host, or has a different or unknown queue/partition, when `queue`
  is requested;
* lacks any required data-access label, or its data access is unknown;
* lacks any required license-eligibility label, or its license eligibility is unknown;
* needs more nodes than one job may have there;
* has nodes with too few CPUs for the whole-rank layout when declared
  (otherwise `ceil(cpus / nodes)`), or too little memory per node, or does not
  record that number when the request needs it;
* has too little scratch per node, or unknown scratch capacity when
  `scratch_mb_per_node` is requested;
* has too few GPUs per node, or an unknown GPU count, when `gpus_per_node`
  is requested;
* cannot accept the explicit `wall_time_seconds` request, or has no known
  walltime limit and no positive evidence of unlimited walltime;
* would exceed either its own walltime limit or the job's requested
  `wall_time_seconds` at the upper end of the predicted execution time.

The requested walltime and estimated execution time are independent checks.
A fast estimate does not make an oversized walltime request admissible.
An admissible reservation also does not make a slow target fit: the reference
execution upper bound, divided by the target's speed factor, must fit the requested
walltime. Equality fits. This can select a faster target with a longer queue
wait, or hold the job when none can finish within its requested limit.
Requests that omit `wall_time_seconds` retain the existing estimate-based check.

Stale or unknown essential capability is a rejection, never a fallback.
With no eligible target, the job is reported **held** with those reasons.

## Then rank by predicted completion

```text
completion = staging + queue wait + execution + result transfer
```

Each component is an interval with its source, or `unknown` with the reason.

| Component | From |
|---|---|
| staging | `input_mb / staging_mb_per_second`; zero with no input |
| queue wait | 0 for a daemon host whose free capacity fits the job now; otherwise the target's `queue_wait_seconds` |
| execution | `runtime_seconds / speed_factor` |
| transfer | `output_mb / transfer_mb_per_second`; zero with no output |

Targets whose every component is known rank first, then by the midpoint of
the predicted completion, its upper bound, the faster target, and the name.
A target with an unknown component is still listed and ranked, after the
complete predictions, and its completion is shown as `... + unknown`: the
sum of what is known is not passed off as a prediction. The explanation
names the selected target and runtime digest, and gives each target's data
age. The same files and `--now` always give the same explanation.

`--json` prints `vq.placement-explanation/1`: `selected`, `held`, the
ranked `candidates` with their `components`, and `rejected` with `reasons`.

## A request from a calibrated estimate

A request can leave its runtime and per-node memory to a #159 calibrated
estimate, from `vq estimate --calibration ... --json`:

```sh
vq placement explain fleet.json request.json --estimate calibrated.json
```

Only fields the request leaves unset are filled. An explicit value always
wins and is reported as `kept`.

| Field | Filled with |
|---|---|
| `runtime_seconds` | The receipt's lower bound and the **calibrated** upper bound, when the estimate's runtime is calibrated. |
| `mem_mb_per_node` | The calibrated peak rounded up to MiB, when it describes one node: a `node`-scope figure, or a `job`-scope one on a single-node request. A `rank`-scope or multi-node `job`-scope figure is not per-node memory and is not used. |

The explanation gains an `estimate` section, also shown in text form. It has
the estimate's digest, calibration id and hardware class, and says per field
`filled` (with the value), `kept`, or `not filled` and why, for example the
calibration's own reason for leaving the figure uncalibrated. Only a
`vq.calibrated-estimate/1` is accepted, and it is checked as `vq submit` checks
one. A raw receipt is refused. Without `--estimate`, explanations are unchanged.

## Live daemon evidence: `vq placement fleet`

A fleet file can carry the daemons' live capacity. Collect an answer on each
daemon host with `vq broker capacity --json` (the broker already does this
over SSH), then overlay the answers on your base record:

```sh
vq placement fleet base-fleet.json \
  --capacity workstation=workstation-capacity.json -o fleet.json
vq placement explain fleet.json request.json
```

Each `--capacity NAME=ANSWER.json` sets only these fields of the daemon
target `NAME`:

| Field | From the answer |
|---|---|
| `observed_at` | When the daemon published its figures: `sampled_at` less `snapshot_age_seconds`. |
| `reachable` | True only when the daemon answered and was shown to use its state root. |
| `free` | `cpus`, `mem_mb` and `gpus` (`free_cpus`, `free_mem_mb`, `free_gpus`), only when its admission is open and its queue ready. Otherwise `free` is removed, so nothing counts as free now. |
| `node.gpus` | The answer's `gpus`, when known. |
| `programs` | A program the answer reports with any status other than `OK` is removed. |

Everything else is the operator's record: node shape, runtime digests,
accounts, speed and transfer rates. Scheduler lanes, and daemons without an
answer, keep their base record. Their observation time is not refreshed, so
they still reject once it is older than `--max-age`. The command refuses:
- an answer for an unknown target or a scheduler lane;
- two answers for one target;
- a malformed answer;
- writing over the base file.

With only `--capacity`, it contacts no host.

To collect the answers in the same step, name the daemon targets with
`--collect`. Each must be a configured `[hosts.NAME]`:

```sh
vq placement fleet base-fleet.json --collect workstation --collect lab-node -o fleet.json
```

Each is asked `vq broker capacity --json` over SSH, as the broker asks: the
same transport, a bounded output, `--timeout` seconds (default 60), and only
the programs its base record lists.
- **Dating.** An answer is dated by this host's clock at the call, less the
  daemon's snapshot age, never by the target's own clock.
- **No answer.** A target that gives none is recorded `reachable: false`,
  observed now, with nothing free. The failure is reported on stderr, and the
  build continues.
- **Before contact.** Collecting a scheduler lane, an unknown target or a host
  that is not configured is refused before any host is contacted. So is naming
  a target with both `--capacity` and `--collect`.

Only the `--collect` hosts are contacted.

### Scheduler lanes: queue waits from history

The driver daemon records, for each scheduler job:
- `scheduler_submitted_at`: when qsub/sbatch accepted it;
- `scheduler_started_at`: when its poll first saw the cluster run it, so to
  the poll's granularity.

The difference is the job's queue wait, without staging. A job whose
acceptance was recovered from remote evidence has no
`scheduler_submitted_at`, and a retry clears both times.

`--queue-history NAME[=TARGET]` sets the scheduler lane `NAME`'s
`queue_wait_seconds` from the waits this host's queue recorded for scheduler
target `TARGET` (default `NAME`):

```sh
vq placement fleet base-fleet.json --queue-history cluster-a/compute=cluster-a -o fleet.json
```

- **Which jobs.** Of the most recent `--history-jobs` (default 50) jobs that
  started within `--history-days` (default 14), the interval is the
  nearest-rank 10th to 90th percentile, with `source: "history"`.
- **Too few.** With fewer than `--history-min` (default 5) recorded waits, the
  base record is kept. stderr reports each lane's outcome.
- **Excluded.** Jobs whose times are missing, unparsable, backwards or in the
  future are left out.
- **Refused.** Naming a target that is not a scheduler lane of the fleet.

This reads only this host's queue.

### Slurm lanes: node shape and walltime limit from `sinfo`

`--scheduler-shape NAME[=HOST]` asks the lane's Slurm host for the
partition named by the lane's `queue`:

```sh
vq placement fleet base-fleet.json --scheduler-shape cluster-a/compute=cluster-a -o fleet.json
```

HOST, which defaults to NAME, must be a configured `[hosts.HOST]` with
`scheduler = "slurm"`. It runs
`sinfo --noheader --partition QUEUE --format '%c|%m|%G|%l|%s'` over that
host's SSH, read-only and bounded: an owned process group, capped output,
`--timeout` and one replay of a transport failure.

What it sets on the lane:
- **Node shape.** `node.cpus`, `node.mem_mb` and `node.gpus` come from the
  partition's **smallest** node group, so a job sized by them fits any node.
  A `+` that sinfo adds to a folded row is read as a lower bound. GPUs are
  the `gpu:` GRES counts; other GRES and `(null)` count none. Other `node`
  fields, such as `scratch_mb`, are kept.
- **Walltime.** `max_walltime_seconds` is the tightest finite time limit, or
  `walltime_unlimited` when every group is `infinite`/`UNLIMITED`.
- **Job size.** `max_nodes` is set when the job-size limit is finite;
  otherwise the base record is kept.
- **Freshness.** `observed_at` is now, and `reachable` is true.

A host that gives no parseable answer leaves the lane `reachable: false`,
observed now, and says why on stderr. These are refused before any host is
contacted:
- a lane that is not a scheduler lane;
- a lane without a `queue`;
- a host that is not a configured Slurm host;
- the same lane named twice.

A partition that mixes node types is described by its smallest group: a GPU
job is then refused if any group lacks GPUs. Give such node types separate
lanes.

### Torque/PBS lanes: queue limits and the nodes the queue routes to

On a `scheduler = "pbs"` host, the same `--scheduler-shape NAME[=HOST]` reads
the lane's queue with `qstat -Qf QUEUE`. Continuation lines that `qstat`
folds onto a tab are rejoined first.

**Queue limits:**
- `resources_max.walltime` sets `max_walltime_seconds`, as
  `[[[days:]hours:]minutes:]seconds`. Without it, the base record's walltime
  is kept, because an absent maximum does not prove the walltime unlimited.
- `resources_max.nodect` sets `max_nodes`.
- `enabled` or `started` false sets `maintenance: true`.

**Node shape.** It comes from `pbsnodes -a`, using only the usable nodes the
queue routes to:
- **Torque:** the nodes carrying every property named by the queue's
  `resources_default.neednodes`, the attribute vq treats as authoritative for
  PBS lane membership.
- **PBS Pro:** the nodes whose `queue` attribute is QUEUE.

Of those nodes, the smallest gives `node.cpus` (`np` or
`resources_available.ncpus`), `node.mem_mb` (`resources_available.mem`, or
`physmem` in `status`, with its unit) and `node.gpus` (`gpus` or
`resources_available.ngpus`; absent is 0).

A queue that routes to no usable node, or a failed `pbsnodes`, keeps the
base node record, and stderr says why: vq never infers a queue's nodes from
properties alone. A `qstat` that fails or cannot be parsed leaves the lane
unreachable.

### Daemon targets: runtimes from a #161 deployment root

A target's `programs` are the runtime digests it can run. For a portable
runtime they are the deployed generations' artifact IDs, which
`--runtime-artifact` binds. `--runtime-root NAME=ROOT` reads them from daemon
target `NAME`, a configured `[hosts.NAME]`:

```sh
vq placement fleet base-fleet.json --runtime-root workstation=/opt/portable-runtimes/vibe-qc -o fleet.json
```

- **The call.** It runs `vq runtime-bundle generations --root ROOT --json` over
  that host's SSH (see [portable runtime bundles](portable_runtime_bundles.md)),
  bounded like `--collect`.
- **What it sets.** For each program with **verified** generations, the
  program's list becomes those generations: the root's current selection
  first when it is one of them, then the rest in identity order. A program
  the listing does not mention keeps its base record.
- **What it skips.** Unverified generations never count, nor does a program
  name a placement record cannot hold. A skipped program is reported.
- **No listing.** A target that gives no listing keeps its programs, and stderr
  says why. A runtime listing is not reachability evidence.
- **Refusals.** These are refused before any host is contacted: a target that
  is not a daemon target of the fleet, a host that is not configured, a
  relative ROOT, and the same target named twice.

## Comparing the broker's orderings on recorded jobs

Before enabling `[broker] policy = "completion-time"`, see what it would
change on this host's own work:

```sh
vq placement compare fleet.json [--jobs 50] [--now TIME] [--max-age 300] [--json]
```

- **Jobs.** The `--jobs` most recent finished local jobs in this host's queue,
  each with its CPUs, declared memory, walltime and program. Jobs without a
  declared `--mem-mb` are skipped and counted, since a broker needs one.
  Scheduler jobs are left out, because the broker places only on daemon hosts.
- **Two placements.** Each job is placed twice, over one capacity snapshot per
  daemon target of the fleet file. A snapshot uses the target's `free` CPUs
  and memory, its observation time and its walltime limit. It is unreachable
  or closed when the record says so, and a missing `free` is unknown.
  1. The **capacity** ordering is the broker's admission and ranking (B2),
     exactly as the broker calls it.
  2. The **completion-time** ordering is the broker's own re-ranker, applied to
     that decision.
- **Report.** Per job: both choices, whether they differ, and each one's
  blockers. Totals: compared, same, different, held by either, and skipped.
  It is read-only: it places nothing and contacts no host.

## Not in this increment

* Live acceptance of these evidence sources and of the opt-in broker policy
  on real daemon hosts and clusters. The tests use recorded output and
  stand-ins for SSH.
