# Explainable placement plans

**Audience: operators** evaluating where a calculation should run across
daemon hosts and PBS or Slurm lanes.

`vq placement explain` applies the completion-time placement policy to one
job and shows the whole decision: every target it rejected and why, and for
each target it would accept, the predicted completion time with every
component and its source. It is local and read-only. It reads two files,
and it submits, reserves and contacts nothing. Broker placement keeps its
strict FIFO pass and its existing ranking, and `vq submit auto` and manual
target selection are unchanged. This is the first increment of #160; feeding
this policy live capability records and offering it as an opt-in broker
policy are separate work.

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
     "arch": "x86_64", "isa_levels": ["x86-64-v2", "x86-64-v3", "x86-64-v4"],
     "programs": {"vibe-qc": ["<runtime sha256>", "<another runtime sha256>"]},
     "accounts": ["chem"],
     "node": {"cpus": 128, "mem_mb": 512000, "scratch_mb": 1000000}, "max_nodes": 16,
     "max_walltime_seconds": 172800,
     "queue_wait_seconds": {"low": 14400, "high": 28800, "source": "history"},
     "speed_factor": 2.0, "speed_source": "benchmark",
     "staging_mb_per_second": 200, "transfer_mb_per_second": 200},
    {"name": "workstation", "kind": "daemon",
     "observed_at": "2026-10-12T11:59:00+00:00", "reachable": true,
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
* **`programs`** lists runtime digests per program; the first is the lane's
  current runtime.
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
  "isa_level": "x86-64-v4",
  "account": "chem",
  "cpus": 16, "nodes": 1, "mem_mb_per_node": 64000,
  "scratch_mb_per_node": 100000,
  "wall_time_seconds": 14400,
  "runtime_seconds": {"low": 6000, "high": 8400},
  "input_mb": 100, "output_mb": 500
}
```

`runtime_digest`, when given, is never substituted by another runtime.
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

## Hard requirements first

A target is rejected, with **every** reason that applies, when it:

* was unreachable or in maintenance when observed;
* was observed longer ago than `--max-age` (default 300 s), or more than
  60 s in the future (clock skew);
* lacks the program, or the requested runtime digest;
* has another architecture, or does not support the requested ISA level;
* does not permit the requested account;
* needs more nodes than one job may have there;
* has nodes with too few CPUs (`ceil(cpus / nodes)`), or too little memory per
  node, or does not record that number when the request needs it;
* has too little scratch per node, or unknown scratch capacity when
  `scratch_mb_per_node` is requested;
* cannot accept the explicit `wall_time_seconds` request, or has no known
  walltime limit and no positive evidence of unlimited walltime;
* would exceed its walltime limit at the upper end of the predicted
  execution time.

The requested walltime and estimated execution time are independent checks.
A fast estimate does not make an oversized walltime request admissible.
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

## Not in this increment

* Building the fleet file from live evidence: the daemons' `vq broker
  capacity`, the scheduler drivers, #159 resource estimates and #161 runtime
  compatibility.
* An opt-in broker policy that uses this ranking, with reservations counted
  once across concurrent submissions and re-evaluated while work is held.
* Console candidate comparisons, and a comparison against the existing
  picker on recorded workloads.
