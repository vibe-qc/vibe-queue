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

Next increments need a product-produced feature/input binding, bounded
directory/QVF adapters, method-specific analytical components, and calibration
from compatible completed-job resource receipts. Runtime prediction must
exclude queue delay and distinguish OOM/time-limit observations from completed
runs. Basis count alone is insufficient to compare direct, dense, fitted and
periodic algorithms. No predictive accuracy or optimization speedup is claimed
by this first increment.
