# Read-only resource estimates

`vq estimate` consumes an existing vibe-qc dry-run `.system` manifest. It does
not import or execute a submitted Python file, call a remote host, change a
calculation, or submit a job. This is the first increment of #159; automatic
resource selection and scheduler balancing (#160) are separate work.

```sh
vq estimate calculation.system --input calculation.py
vq estimate calculation.system --input calculation.py --json
vq estimate calculation.system --features features.json --memory-mb 4096 --json
```

Obtain the manifest using vibe-qc's existing dry-run/estimate workflow in an
execution context where that calculation is authorized. Dry-run environment
variables are not a sandbox. This command deliberately provides no interpreter
or executable-preflight option. Each `--input` names a regular file, with at
most 64 files of 64 MiB each; directory and QVF discovery are not implemented.
The manifest is limited to 1 MiB and must have `[outputs].status = "dry_run"`.

The versioned `vq.resource-estimate/1` JSON receipt contains the program
version, observed features, input/evidence SHA256 digests, memory semantics,
an explicit override, unknown fields and limitations. An absent estimate stays
unknown. Invalid values, contradictory evidence and non-dry-run manifests are
errors. Existing submission/preflight and host-picking behavior is unchanged.

| Quantity | First-increment behavior |
|---|---|
| Memory bytes | Preserve `[memory].estimate_bytes` exactly |
| Safety margin | Already included by vibe-qc; never multiply by 1.2 again |
| Raw memory and margin factor | Unknown: the legacy manifest does not report them |
| Memory scope | Unknown unless explicitly declared as job, node or rank |
| MPI memory | No inferred multiplication/division by rank count |
| Basis-function count | Only explicit evidence; never guessed from atoms or a basis name |
| Runtime, scratch and recommended CPU layouts | Unknown until appropriate models are available |
| Explicit `--memory-mb` | Authoritative job limit in MiB, retained even below a job estimate |
| Calibration | Uncalibrated; no measured confidence or guaranteed upper bound |

Optional `features.json` supplies missing operator evidence. It is attributed
separately in the receipt and cannot contradict the method, basis or task type
reported by the manifest. For example:

```json
{
  "schema_version": 1,
  "nbf": 240,
  "basis_representation": "spherical",
  "algorithm": "direct",
  "mpi_ranks": 1,
  "threads_per_rank": 8,
  "memory_scope": "job"
}
```

These numbers illustrate the format, not a predicted calculation. Supplying
an algorithm or Nbf does not create a new analytical model or modify the
calculation. The program's route-specific estimate remains authoritative.
For a rank/node estimate, `effective_job_memory_bytes` stays unknown unless
the user supplies a job limit. For a job estimate it uses the program value,
unless overridden.

The input digest changes when the manifest, feature evidence or supplied input
bytes change. Legacy manifests do not cryptographically bind their estimate
to the calculation's inputs: the caller supplies that association. Do not use
this receipt as an automatic admission decision or a validated cache hit.
The receipt projects known scientific fields and hashes; it does not copy
the manifest's host/environment sections into the output.

## Evaluating estimates on a held-out set

`vq estimate-evaluate` measures how estimates held up against what jobs
actually used. Freeze the evaluation set before looking at the results. It
pairs each estimate receipt with the job's `resource-usage.json` receipt,
its outcome and its hardware class:

```sh
vq estimate calculation.system --input calculation.py --json > case-01.estimate.json
# after the job ran: copy its _vq/resource-usage.json next to the set file
vq estimate-evaluate held-out.json
vq estimate-evaluate held-out.json --json
```

```json
{
  "schema_name": "vq.estimate-evaluation-set/1",
  "set_id": "held-out-2026-10",
  "cases": [
    {"case_id": "case-01", "estimate": "case-01.estimate.json",
     "usage": "case-01.resource-usage.json", "outcome": "completed",
     "hardware_class": "class-a"},
    {"case_id": "case-02", "estimate": "case-02.estimate.json", "usage": null,
     "outcome": "oom_killed", "hardware_class": "class-a",
     "memory_limit_bytes": 8589934592, "memory_limit_scope": "node"}
  ]
}
```

Paths are relative to the set file. `outcome` is one of `completed`,
`failed`, `oom_killed`, `time_exceeded` and `killed`. Give the job's memory
or wall-time limit when it was killed for exceeding it. Both direct
(`vq.direct-resource-usage.v1`) and scheduler (`vq.scheduler-resource-usage.v1`)
receipts are read.

`memory_limit_scope` declares whether `memory_limit_bytes` applies to the
whole `job`, one `node` or one `rank`; use `unspecified` when unknown.
Omitting it retains the existing total-job meaning. An OOM kill at a limit
at or above an estimate refutes that estimate only when the limit covers
the same or a narrower scope. For example, four ranks each using 600 MiB
can exceed a 2 GiB job limit while staying below a 1 GiB per-rank estimate.
That kill alone leaves the per-rank estimate `censored`. A rank limit can
refute a rank, node or job estimate; a node limit can refute a node or job
estimate. Unknown scopes remain censored unless measured peak RSS exceeds
the estimate. No multiplication or division by ranks or nodes is inferred.

For a receipt with collector `status: "ok"`, `command_status` must be
`succeeded` or `failed`, and `command_exit_code` must be an integer. Success
requires exit code zero; failure requires a nonzero code. A case marked
`completed` cannot use a failed-command receipt: the evaluator reports the
case and receipt error before producing a report. Correct the case's outcome
or supply its matching receipt instead of treating a failed run as successful
calibration evidence. Nonzero exits remain valid observations for OOM,
time-limit and kill outcomes, with the censored semantics below. A successful
command can still have a `failed` case outcome when workload acceptance fails.
Collector errors remain excluded.

Measurement fields must be finite, non-negative numbers (or `null` when
unknown). `NaN`, infinities and values that overflow numeric conversion are
receipt errors, never measurements or successful coverage. Runtime estimate
intervals likewise require finite, non-negative bounds in increasing order;
equal bounds, including zero, are allowed. Errors identify the case, receipt
type and offending field before any evaluation report is produced.

A receipt's peak RSS is the **largest single process** (`ru_maxrss`), not a
sum over concurrent processes. Each case's memory verdict follows from that:

| Verdict | When |
|---|---|
| `underpredicted` | the measured peak exceeds the estimate; or an OOM kill at a same- or narrower-scope limit at or above the estimate |
| `within` | at or below the estimate, for a `rank`-scope estimate or a single-process run |
| `not_refuted` | at or below a job-, node- or unspecified-scope estimate, but the run had several processes or did not record how many |
| `censored` | an OOM kill, time limit or kill whose measurement does not decide it |
| `unobserved` | a completed job without a usage receipt |
| `excluded` | a failed run, or a collector error: not a measurement |
| `no_prediction` | the estimate has no memory figure |

Runtime intervals are checked the same way against the measured wall time:
`covered`, `above` (including a time-limited run whose limit exceeded the
interval), `below`, `censored`, `unobserved`, `excluded`, `no_prediction`.
Current vibe-qc manifests carry no runtime, so expect `no_prediction` there.

Per method and hardware class, and overall, the report gives the sample size,
every verdict count, the measured/predicted ratio (median, maximum) and the
**underprediction frequency as a range**. The lower bound counts decided
underpredictions only. The upper bound adds every case that could still be
one (not refuted, censored or unobserved). Runtime coverage is reported the
same way. There is no tunable threshold. The report records the set's digest
and every receipt's digest, so a published number names exactly the data
behind it. It claims nothing beyond those estimators, programs, methods and
hardware classes.

## Next increments

Still open: a product-produced feature/input binding, bounded
directory/QVF adapters, method-specific analytical components, and calibration
from compatible completed-job resource receipts. Runtime prediction must
exclude queue delay. Basis count alone is insufficient to compare direct,
dense, fitted and periodic algorithms. No predictive accuracy or optimization
speedup is claimed.
