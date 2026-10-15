# Runtime benchmark campaigns

**Audience: operators** deciding whether a target-optimized build of a program
earns its place next to the portable generic one.

`vq benchmark` plans a generic-versus-optimized comparison, runs it through
the queue, and analyzes the recorded runs. `plan` and `analyze` are local and
read-only. `run` submits the plan's runs, one at a time per node class, and
collects their results; nothing runs unless an operator starts it. The real
pilot measurements are separate, admitted work, and nothing here claims a
speedup.

```sh
vq benchmark plan plan.json                       # validate; print digest and run order
vq benchmark plan plan.json --json                # the same, for a runner
vq benchmark run plan.json binding.json RUN_DIR   # submit and collect; safe to stop and rerun
vq benchmark analyze plan.json RUN_DIR/results.json   # the report as a table
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

## Running a plan: `vq benchmark run`

```sh
vq benchmark run plan.json binding.json ~/benchmarks/isa-pilot
```

The plan says what is compared; the **binding** says where and how, and stays
with the operator, so the plan and its results can be shared without a host
name in them.

```json
{
  "schema_name": "vq.benchmark-binding/1",
  "plan_sha256": "<from vq benchmark plan>",
  "node_classes": {"class-a": {"host": "cluster-a"}},
  "variants": {
    "generic": {"command": ["/opt/vibeqc/generic/bin/python", "run.py"]},
    "tuned": {"command": ["/opt/vibeqc/spr/bin/python", "run.py"]}
  },
  "workloads": {"h2o-ccsd": {"sources": "inputs/h2o-ccsd", "args": []}},
  "mem_mb": 16000,
  "wall_time_seconds": 7200
}
```

* **`node_classes`** maps each node class to a configured host or scheduler
  lane; without `host`, the class runs on this machine. Every node class,
  variant and workload of the plan must be bound, and nothing else.
* **`variants`** gives each variant's command (and optionally a registered
  `program`). A run's command is the variant's command followed by the
  workload's `args`.
* **`workloads`** names each workload's input directory, relative to the
  binding file. Its content must match the plan's `input_digest`, the same
  digest `vq campaign validate` computes for node sources: every entry's
  path, type, size and SHA256, with symbolic links and special files refused.
  It is copied into each run's payload, so it arrives in the job's workspace.
* Each run asks for the layout's cores (`mpi_ranks × threads_per_rank`) and
  the binding's `mem_mb` and `wall_time_seconds`.

Every problem with the binding is listed before anything is submitted: an
unbound or extra name, a host that is neither this machine nor configured,
or an input whose content does not match its pin.

**Order and isolation.** Runs follow `vq benchmark plan`'s seeded order. On
one node class they never overlap: the next run is submitted only after the
previous one finished, so no run competes with another for its node.
Different node classes run side by side.

**What the job reports.** A benchmark job writes `vq-benchmark-run.json` in
its workspace (`vq.benchmark-run-report/1`):

```json
{"schema_name": "vq.benchmark-run-report/1", "runtime_digest": "<sha256>",
 "converged": true, "steady_seconds": 80.2, "startup_seconds": 2.1,
 "scf_iterations": 12, "values": {"total_energy": -76.241123456},
 "cpu_model": "Example CPU 8480", "mpi_ranks": 1, "threads_per_rank": 8}
```

Only the job can time its own steady state and know its result, and it
reports the runtime it actually ran: a portable runtime's artifact ID, for
example. vq does not vouch for these values. The analysis excludes a run
whose runtime or layout differs from the plan, and `converged: false` records
the run as `unconverged`. The layout defaults to the planned one when the
report leaves it out.

**`vq benchmark measure` writes the report for you** for vibe-qc and ORCA.
Use it as the variant's command:

```json
"variants": {
  "generic": {"command": ["vq", "benchmark", "measure", "--program", "vibe-qc",
                          "--runtime-root", "/opt/runtimes/vibeqc-generic",
                          "--", "{runtime}/bin/python", "run.py"]},
  "tuned": {"command": ["vq", "benchmark", "measure", "--program", "vibe-qc",
                        "--runtime-root", "/opt/runtimes/vibeqc-spr",
                        "--", "{runtime}/bin/python", "run.py"]}
}
```

It runs the command, passes its output through to the job's log unchanged,
keeps its exit code, and writes `vq-benchmark-run.json` in the workspace:

* **Startup and steady time.** Startup is the time to the first SCF
  iteration line of the output; steady time is the rest of the run. A
  program that prints no iteration line is timed as steady throughout and
  reports no startup, and is not counted as converged.
* **Values and convergence.** The total energy (as `total_energy`), the gap
  where printed (`gap_ev`), and the SCF iteration count are read by the same
  parsers as the console's calculation preview. A `FATAL` line, an
  optimisation that did not converge, a missing SCF result or a non-zero
  exit makes the run `converged: false`. The wrapper must observe an SCF
  iteration and the final cycle's result (`SCF converged ...` for vibe-qc,
  `FINAL SINGLE POINT ENERGY` for ORCA). A new SCF header, geometry cycle
  or iteration 1 clears the previous cycle's completion. An energy value
  alone or an earlier successful cycle cannot prove convergence. Completion
  and fatal-error evidence are tracked across the output stream, even when
  those lines scroll out of the bounded preview tail. Unconverged runs stay
  in the results with their timings and are excluded from speedup pairs.
* **Runtime.** `--runtime-root` names a portable runtime deployment
  (`vq runtime-bundle deploy`). The wrapper reads its verified current
  selection, expands `{runtime}` in the command to that generation's
  payload, and reports the selection's artifact ID. The digest therefore
  names the code that ran, and it is what the plan's variant
  `runtime_digest` should be. A selection that changes during the run is
  refused, and no report is written. For a runtime not deployed as a
  bundle, `--runtime-digest SHA256` records an identity you declare
  instead.
* **CPU model**, from the platform where it exposes one.

The command needs `vq` where the job runs, as on any vq host or scheduler
cluster with the vq helper.

**What the runner records.** For each finished run the runner brings the
report back with `vq fetch HOST JOB --name vq-benchmark-run.json`, and takes
peak RSS and CPU time from the job's measured `resource_usage` in
`vq status --json`. It then writes one row to `RUN_DIR/results.json`
(`vq.benchmark-results/1`), rewritten after every run.

* A completed job without a valid report is **left missing**, and the
  analysis lists it as missing. vq never fills in a timing.
* A job that failed, ran out of memory or time, or was killed is recorded
  with that status. Its `runtime_digest` is the planned one, which the
  analysis never compares for a run that did not complete.

**Stopping and restarting.** Each run is submitted with
`vq submit HOST --idempotency-key`, its key derived from the plan digest and
the run's place in the order, and is recorded before the request is sent.
The run directory is locked. Rerunning the same command after a crash or a
lost answer resends under the same key and the queue returns the job it
already accepted. A run's staged input is checked against its pin before
every send, and a changed one is never sent. `--once` makes one pass and
exits. The exit status is 0 when every run is recorded (or, with `--once`,
while runs remain without a problem), and 1 if a run was left missing or
hit a problem.

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

* Any measurement. The pilot on a real compute-node class is a separately
  admitted execution plan, and nothing here claims a speedup.
* Measurement wrappers for programs other than vibe-qc and ORCA; their jobs
  write `vq-benchmark-run.json` themselves.
