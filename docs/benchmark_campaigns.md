# Runtime benchmark campaigns

**Audience: operators** deciding whether a target-optimized build of a program
earns its place next to the portable generic one.

`vq benchmark` plans a generic-versus-optimized comparison and analyzes the
recorded runs. Both commands are local and read-only: they validate files,
print a run order and compute a report. They submit no job, measure nothing
and contact no host. This is the first increment of #162. Submitting the
matrix through vq and collecting its receipts, and the real pilot
measurements, are separate work; until then the runs are executed and
recorded by the operator.

```sh
vq benchmark plan plan.json                       # validate; print digest and run order
vq benchmark plan plan.json --json                # the same, for a runner
vq benchmark analyze plan.json results.json       # the report as a table
vq benchmark analyze plan.json results.json --json --csv cells.csv
vq benchmark analyze plan.json results.json --svg speedups.svg
```

## The plan: `vq.benchmark-plan/1`

A plan holds everything that must stay fixed for a comparison to mean
anything, and the analysis refuses results recorded against any other plan.

```json
{
  "schema_name": "vq.benchmark-plan/1",
  "plan_id": "isa-pilot",
  "baseline": "generic",
  "variants": [
    {"name": "generic", "runtime_digest": "<sha256>", "build_flags": "-O2 -march=x86-64-v2"},
    {"name": "tuned", "runtime_digest": "<sha256>", "build_flags": "-O2 -march=sapphirerapids",
     "libraries": {"openblas": "0.3.28"},
     "build_cost_core_seconds": 36000, "deployment_cost_core_seconds": 1800,
     "storage_bytes": 400000000}
  ],
  "workloads": [
    {"name": "h2o-ccsd", "input_digest": "<sha256>", "method": "CCSD",
     "algorithm": "conventional", "nbf": 58,
     "checks": [{"quantity": "total_energy", "abs_tolerance": 1e-8}]}
  ],
  "node_classes": ["class-a"],
  "layout": {"mpi_ranks": 1, "threads_per_rank": 8},
  "warmup": 1,
  "repeats": 5,
  "seed": 20261012,
  "confidence": 0.95,
  "margin": 0.02
}
```

* **Variants** are identified by runtime digest; two variants with one digest
  are refused. Record build flags and loaded numerical libraries. Compare one
  factor at a time: a different BLAS or compiler is its own plan, not a third
  variant mixed into an ISA comparison. Keep fast-math or reduced-precision
  builds out of the primary comparison; they need their own scientific
  validation first.
* **Workloads** are identified by input digest and need at least one
  correctness check. Record Nbf, the auxiliary basis size, method and
  algorithm where they apply.
* **Node classes** are operator-chosen labels for one compute-node type, not
  host names. Do not put private host names in a plan or results file.
* **`repeats`** is at least 5. **`warmup`** blocks run first and never count.
* **`margin`** is the practical-significance band: a ratio within `1 ± margin`
  is no difference worth a separate artifact.

`vq benchmark plan` prints the plan's `plan_sha256`. The digest covers the
plan's content, not its formatting or key order. It also prints the run order.
For each node class the blocks run in sequence, and each block contains
every workload/variant pair once, shuffled by `seed`, so no variant always
runs first or last. The same plan always gives the same order.

## The results: `vq.benchmark-results/1`

```json
{
  "schema_name": "vq.benchmark-results/1",
  "plan_sha256": "<from vq benchmark plan>",
  "runs": [
    {"workload": "h2o-ccsd", "variant": "tuned", "node_class": "class-a", "block": 1,
     "status": "completed", "runtime_digest": "<sha256>",
     "hardware": {"cpu_model": "Example CPU 8480", "mpi_ranks": 1, "threads_per_rank": 8},
     "steady_seconds": 80.2, "startup_seconds": 2.1, "cpu_seconds": 640.9,
     "peak_rss_mb": 1012.0, "scf_iterations": 12,
     "values": {"total_energy": -76.241123456}}
  ]
}
```

One run per workload, variant, node class and block; a duplicate, or a run
the plan does not contain, is an error. `status` is one of `completed`,
`failed`, `unconverged`, `time_exceeded`, `oom_killed` and `killed`.
`steady_seconds` is the execution time after startup, and the comparison is
made on it. Cold startup, CPU time, peak memory and SCF iterations are
reported separately. Keep scheduler waiting, build and transfer time out of
all of them.

## The analysis

For each node class, workload and non-baseline variant, each block's variant
run is paired with the baseline run of the same block. A pair counts only
when:

* both runs completed, with their planned runtime digest and the planned
  layout, and recorded `steady_seconds`;
* both ran on the same CPU model, when it is recorded;
* every declared check agrees: `|variant - baseline| <= abs_tolerance +
  rel_tolerance * |baseline|`. A fast run that fails a check is not a
  speedup.

Every pair that does not count is listed under `excluded` with its reason,
and a run that is missing is listed under `missing`. Nothing is dropped
silently.

| Reported | Meaning |
|---|---|
| ratio | `T_baseline / T_variant` per pair; above 1 means the variant is faster. Geometric mean, median, minimum and maximum over the retained pairs. |
| interval | Student t interval at `confidence` on the mean log ratio, transformed back to a ratio. |
| verdict | `faster` if the interval lies above `1 + margin`; `slower` if it lies below `1 / (1 + margin)`; `equivalent` if it lies inside that band; otherwise `inconclusive`. Also `inconclusive` with fewer retained pairs than `repeats`, and `incomparable` when one node class ran on several CPU models. A variant is never chosen on a single best run. |
| break-even | Only for `faster`. The variant's build plus deployment cost divided by the mean paired saving per run; cost and saving are both allocated core-seconds (time times the layout's cores). The conservative count uses the interval's lower bound instead. Storage bytes are reported, not converted. |
| aggregate | Per node class and variant, the equal-weight geometric mean of the per-workload ratios. Only workloads with all planned pairs and a comparable node class are included; the others are listed as left out. No interval is given, because workloads are not samples of one population. |

The JSON report (`vq.benchmark-report/1`) repeats the plan's identities under
`applies_to`: runtime digests, build flags, libraries, node classes and
layout. Its results lose applicability when any of these change. The same
plan and results always give the same report, byte for byte, whatever order
the runs were recorded in. It contains no host names. `--csv` writes one row
per node class, workload and variant.

The text report prints the same table. Where a cell's break-even is shown as
`237 (244)`, the second number is the conservative count.

## Exporting a plot

`analyze --svg speedups.svg` also writes a standalone vector figure, suitable
for a browser or a report, without requiring a plotting package. It can be
combined with `--json` and `--csv`. Export destinations must differ from the
input files and from each other, including symbolic links and hard links.

The figure groups workloads by node class and variant on a shared logarithmic
axis. Each point is the reported geometric mean of `T_baseline / T_variant`;
its bar is the configured Student-t confidence interval. The reference line
at 1 means equal steady execution time. The shaded equivalence band has the
same bounds as the analysis, `1 / (1 + margin)` through `1 + margin`.

Every row includes retained/planned pairs, verdict, and excluded and missing
pair counts. Inconclusive points are hollow. A single retained pair has no
interval; absent or unplottable ratios read `Unavailable`. An incomparable
cell has no plotted estimate even when individual ratios exist. Unknowns
never become zero-speedup points. Warm-ups remain excluded from the analysis.

Row tooltips retain the reasons for exclusions and missing pairs. The figure
embeds the full JSON report as inert SVG metadata, including input/runtime
identities and analysis settings; the plan and results digests are also
printed below the plot. Sharing the figure shares that report. The same
inputs produce identical SVG bytes, independent of recorded run order.

## Not in this increment

* Submitting the run order through `vq submit` or a scheduler and collecting
  the job receipts into a results file.
* Any measurement. The pilot on a real compute-node class is a separately
  admitted execution plan, and nothing here claims a speedup.
