# Read-only resource estimates

`vq estimate` consumes an existing vibe-qc dry-run `.system` manifest. It does
not import or execute a submitted Python file, call a remote host, change a
calculation, or submit a job. This is the first increment of #159; automatic
resource selection and scheduler balancing (#160) are separate work.

```sh
vq estimate calculation.system --input calculation.py
vq estimate calculation.system --input calculation.py --json
vq estimate calculation.system --input calculation-dir --json
vq estimate calculation.system --features features.json --memory-mb 4096 --json
```

Obtain the manifest using vibe-qc's existing dry-run/estimate workflow in an
execution context where that calculation is authorized. Dry-run environment
variables are not a sandbox. This command deliberately provides no interpreter
or executable-preflight option. Each `--input` names a regular file or a
directory. A directory stands for every regular file under it, in
relative-path order, each opened without following a link. Links and special
files under it are left out and counted in a warning, because a `vq submit -d`
payload carries a link as a link. At most 64 files are fingerprinted after
expansion, each at most 64 MiB, and a directory walk visits at most 10,000
entries. A directory with no regular file, or one that cannot be read
completely, is an error rather than a smaller fingerprint. QVF members are
not discovered: a `.qvf` input is fingerprinted as one file.
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

## Attaching an estimate to a job

`vq submit --estimate RECEIPT` keeps a `vq estimate --json` receipt with the
job as its `resource_estimate`, so the estimate and what the job measurably
used can be compared once it finishes:

```sh
vq estimate calculation.system --input calculation.py --json > calculation.estimate.json
vq submit localhost --estimate calculation.estimate.json -d . -- python calculation.py
```

Every input file the receipt fingerprinted (`vq estimate --input`) must be a
file of the submission, by content: the submitted input file, a regular file
anywhere under `--dir`, or a member of a `--compressed` archive. A link to it
does not count. For a `--dir` submission, estimate with `--input` on the same
directory. Otherwise the submission is refused before anything is
written, because the estimate was made for other inputs. A receipt without
`--input` fingerprints only its manifest, so nothing can be checked and it is
attached as given. The search is bounded (20,000 files, 16 GiB) and a receipt
is at most 256 KiB.

An attached estimate changes no resource, placement or admission: `--mem-mb`,
`--time` and host picking behave as before. `vq status` shows an `estimate:`
line, `vq status --json` the `resource_estimate` record, and `vq resubmit` or
a reboot recovery keeps it. A remote host checks the receipt again against
the payload it receives. A broker refuses `--estimate`. The submission's
idempotency intent includes the receipt only when one is attached.

`vq estimate-collect OUT_DIR --hardware-class LABEL` then builds the held-out
set below from the local queue. Each finished job with an estimate becomes one
case: `estimates/<job>.json`, the `_vq/resource-usage.json` its collector left
as `usage/<job>.json` (no entry after a kill), its outcome from the job state,
and its memory limit for an OOM kill or its wall limit for a time-limited run.
An interrupted, starved or queue-aborted job has no evaluable outcome, and a scheduler array's
per-element receipts are not collected; both are listed as skipped. OUT_DIR
must not exist or be empty, and is written whole or not at all. `--set-id`
names the set (default: the directory's name) and `--json` prints the summary.

```sh
vq estimate-collect held-out-2026-10 --hardware-class class-a
vq estimate-evaluate held-out-2026-10/set.json
```

Collect only after the set is frozen: jobs chosen by looking at their results
are not held out.

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

## Calibrating on a training set

A calibration is fitted on one set and judged on another that it never saw:

```sh
vq estimate-collect training-2026-10 --hardware-class class-a
vq estimate-calibrate training-2026-10/set.json -o calibration.json --min-cases 5
vq estimate-evaluate held-out-2026-11/set.json --calibration calibration.json
```

`vq estimate-calibrate` writes a `vq.estimate-calibration/1` document. It
groups cases by estimator, program, program version, method, algorithm,
memory scope and hardware class, so a calibration only ever applies to the
identity it was fitted on. Only decided evidence counts:

| Evidence | When |
|---|---|
| exact ratio | observed/predicted peak where the measurement compares: a rank-scope estimate, or a single-process run (verdict `within` or a measured `underpredicted`) |
| lower bound | a multi-process run that measurably exceeded a job- or node-scope estimate (the job held at least its largest process), or an OOM kill at a comparable limit at or above the estimate (limit/predicted) |

Not-refuted, censored, unobserved, excluded and no-prediction cases are
counted and never used. A group's **multiplier** is its worst ratio, never
below 1, so a calibration never lowers a program's estimate. When that worst
case is only a lower bound, the document says so, and the true factor may be
larger. A group with fewer decided cases than `--min-cases` (default 5)
stays uncalibrated. Choose that number before fitting, never after looking
at an evaluation. The document records the training set's digest and every
training case's receipt digests. Its `calibration_id` is the digest of its own
content, and a document whose content no longer matches it is refused. An
existing output file is never overwritten.

### Runtime groups

The same document also calibrates runtime, in `runtime_groups` (method
`worst-observed-over-predicted-runtime-upper/1`). These are grouped like the
memory groups but without the memory scope, since wall time does not depend on
it. The evidence is the measured wall time against the **upper bound** of the
estimate's runtime interval:

| Evidence | When |
|---|---|
| exact ratio | a completed run with a measured wall time (wall/upper), whether it was covered, below or above the interval |
| lower bound | a run stopped at its time limit that ran past the upper bound: the larger of its wall time and `wall_limit_seconds`, over the upper bound; or a run stopped another way (killed, OOM) after it ran past the upper bound (wall/upper) |

Failed, excluded and unobserved runs, runs stopped before reaching the upper
bound, estimates without a runtime interval, and intervals whose upper bound
is zero are counted and never used. As for memory, the multiplier is the worst
ratio, never below 1, flagged when it is only a lower bound, and `--min-cases`
applies. Only the upper bound is scaled; the lower bound is left as it is.
A calibration fitted before runtime groups existed loads with none.

### Judging on held-out cases

`vq estimate-evaluate --calibration` keeps every raw verdict and adds, per
case:
- `calibrated_memory`, the verdict for the estimate times its group's
  multiplier;
- `calibrated_peak_bytes`;
- `calibration_multiplier`;
- `calibrated_runtime`, the runtime verdict for the interval with its upper
  bound times the case's runtime-group multiplier;
- `calibrated_runtime_upper_seconds`;
- `runtime_calibration_multiplier`.

A case whose group is uncalibrated, or absent, reads `uncalibrated`. Each
summary gains `memory.calibrated` with its underprediction bounds over the
calibrated cases and the number of uncalibrated ones, and `runtime.calibrated`
with its coverage bounds in the same way. The report gains a `calibration`
section naming the document. The text table adds `CALIBRATED UNDER`,
`UNCALIBRATED` (memory) and `CALIBRATED COVERED` columns. The evaluation refuses the calibration's training
set itself, and any case whose receipts are also a training case, so a
calibration is always judged on held-out cases. Without `--calibration` the
report is unchanged.

A multiplier is the worst ratio seen in training, not a confidence bound. A
calibration changes no receipt, admission or placement. Only
`--mem-from-estimate` and `--time-from-estimate` (below) turn a calibrated
figure into a limit, and only when asked to.

## Applying a calibration

Once a calibration has held out well, apply it to new estimates:

```sh
vq estimate calculation.system --input calculation.py \
    --calibration calibration.json --hardware-class class-a --json > calibrated.json
vq submit localhost --estimate calibrated.json -d . -- python calculation.py
```

With `--calibration` (and the `--hardware-class` the job will run on),
`vq estimate --json` prints a `vq.calibrated-estimate/1` instead of the bare
receipt. It holds:

- the `vq.resource-estimate/1` receipt, unchanged, and its digest;
- the calibration's id, method and training set, and the hardware class;
- the matching group's multiplier, and whether that is a lower bound;
- `calibrated_peak_bytes`, the raw peak times the multiplier, rounded up;
- the runtime part, from the matching runtime group: `runtime_status`,
  `runtime_multiplier` and whether it is a lower bound, and
  `calibrated_runtime_upper_seconds`, the raw upper bound times that
  multiplier. The raw interval in the receipt is unchanged.

When nothing applies it says `status: uncalibrated` with the reason: the
estimate has no memory figure, no group matches it, or its group had too few
cases. The runtime part is likewise `uncalibrated` with a `runtime_reason`:
no runtime interval, a zero upper bound, no matching runtime group, or too
few cases. A document written before the runtime part existed still loads
without it. The text output adds `Calibrated memory:` and
`Calibrated runtime` lines.

`vq submit --estimate` accepts either document. For a calibrated one:

- the document is checked: its receipt's digest, the peak and multiplier
  arithmetic, and a status consistent with them;
- the receipt is checked against the payload exactly as before and recorded
  as `resource_estimate`;
- the rest is recorded as `resource_estimate_calibration`.

`vq status` adds the calibrated figure to the `estimate:` line (or why there
is none). Resubmission and reboot recovery keep it, and the idempotency
intent includes it. `vq estimate-collect` still writes the raw receipt, so a
later calibration is fitted on raw estimates. On its own, a calibrated figure,
like a raw one, sets no `--mem-mb`, admission or placement.

### A memory limit from a calibrated estimate

`--mem-from-estimate` opts one submission in to taking its memory limit from
its calibrated estimate:

```sh
vq submit localhost --estimate calibrated.json --mem-from-estimate -d . -- python calculation.py
```

The limit is `calibrated_peak_bytes` rounded up to MiB, exactly as if it had
been given with `--mem-mb`. It is derived before the target is resolved, so
`vq submit auto` places the job by it. It is refused unless:

- the estimate is a `vq.calibrated-estimate/1` whose status is `calibrated`;
- its memory scope is `job`, because `--mem-mb` is a job total; under
  Slurm's per-node `--mem` a job-scope limit is conservative;
- no `--mem-mb` is given. An explicit limit stays authoritative, and giving
  both is a usage error.

The job records `mem_mb_source: "calibrated-estimate"`. Where the job is
created (on a remote host too), the limit is checked against the recorded
calibrated peak. `vq status` shows a `mem_mb:` line naming the source.
Resubmission keeps the source unless `--mem-mb` overrides the limit, and
reboot recovery keeps it. Without `--mem-from-estimate`, no estimate changes
a limit.

### A wall-time limit from a calibrated estimate

`--time-from-estimate` does the same for the wall-time limit:

```sh
vq submit localhost --estimate calibrated.json --time-from-estimate -d . -- python calculation.py
```

The limit is `calibrated_runtime_upper_seconds` rounded up to whole minutes,
exactly as if it had been given with `--time-limit`. It is refused unless:

- the estimate is a `vq.calibrated-estimate/1` whose `runtime_status` is
  `calibrated`;
- neither `--time-limit` nor `--wall-time-seconds` is given. An explicit
  limit stays authoritative.

The job records `wall_time_source: "calibrated-estimate"`, which is checked
against the recorded calibrated bound where the job is created (on a remote
host too). `vq status` shows a `wall_time:` line naming the source.
Resubmission keeps the source unless `--wall-time-seconds` overrides the
limit, and reboot recovery keeps it. The two flags are independent, and one
estimate can set both limits. A calibrated upper bound is still the worst ratio seen
in training, so a run can exceed it; the job is then stopped as
`TIME_EXCEEDED` like any other limit.

## Next increments

Still open: a product-produced feature/input binding, QVF member adapters,
method-specific analytical components, and letting a calibrated estimate
inform placement beyond its memory limit. Runtime prediction must
exclude queue delay. Basis count alone is insufficient to compare direct,
dense, fitted and periodic algorithms. No predictive accuracy or optimization
speedup is claimed.
