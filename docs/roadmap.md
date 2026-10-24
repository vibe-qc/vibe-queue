# vq roadmap

Baseline reconciled on **2026-10-19** against release **v0.26.45** (2026-10-19),
current source including the post-release estimate workflow, the source
documentation and the live tracker.
The 1.0 target and acceptance boundaries were updated on **2026-10-22**.
This is a delivery plan, not a deployment ledger or a claim that every feature
in a release has completed its broader qualification.

## Where each kind of fact lives

| Question | Authority |
|---|---|
| What shipped, and in which release? | [Changelog](../CHANGELOG.md), with [unreleased fragments](../changelog.d/README.md) for main-only changes |
| What is open, with what evidence and ownership? | Canonical issue tracker, selected through external operator configuration |
| What comes next, and what depends on it? | This roadmap |
| What does an interface implement? | Its feature guide and classified sections of [SPEC](SPEC.md) |
| What is deployed? | External operator records; see [release records](../releases/README.md) |
| Historical plans | [Archived v0.1–v0.9 roadmap](roadmap_history.md), [version history](version_compatibility.md), and explicitly historical design documents |
| Release names | [Codenames](codenames.md); names do not impose feature scope |

Development owners check and land source. Release owners qualify and cut a
candidate under the [release process](release_process.md). Functional acceptance
can be performed by the implementing owner; no second reviewer or fleet-wide
convergence is required. A real missing host observation holds its own feature
claim, not unrelated development, releases or calculations on healthy runtimes.

## 1. Current baseline

The latest tag checked is v0.26.45. The v0.26 line already includes local queues,
Torque/PBS and Slurm dispatch, immutable runtime slots and recovery, retention,
branch-tracking dev lanes, a durable daemon-host broker, the authenticated fleet
console, public static snapshots and calculation previews. QVF protocol checks
use toolkit 0.5.0; format version 1 and the established lifecycle are retained.
See the changelog for individual release boundaries rather than duplicating its
patch-by-patch history here.

Several substantial features have already landed incrementally in v0.26:
resource-estimate receipts and evaluation, read-only placement explanations,
portable runtime bundles, benchmark and artifact campaign controllers, and the
HTTPS submission gateway. Their full requested outcomes remain separate from
those delivered increments; section 2 records the boundaries.

v0.26.45 adds the outbound gateway connector, daemon-job and campaign
binding to portable runtime generations, scheduler GPU requests and
node-allocation display, console refresh-cost measurements, benchmark
recovery fixes, QVF scheduler launcher repair and macOS app packaging.
Main additionally carries directory-input fingerprints, estimates attached
at submission and collected for evaluation. Those estimate changes follow
v0.26.45; source or release status does not establish deployment on a host.
The tag also does not establish real site TLS or packaged-app acceptance.

### Completed work no longer treated as a gate

The broader feature records are: **#153, #159, #160, #161, #162,
#163 and #165 (verified closed), and #168**; release coordination is tracked separately in **#189**.
The earlier roadmap's September host-validation table,
migration dependencies and decision queue are obsolete as an open-work list.
Closed records retain their actual evidence and limitations; closure does not
invent measurements absent from those records.

In particular, #64's daemon-host broker has bounded two-target acceptance;
#66's snapshot work, #72/#104 runtime migration, #73 self-update, #74 retention,
#87 scheduler registrations, #109 branch tracking and #110 VASP workflow are
closed records, not prerequisites to restart. #25 extras acceptance also
completed. No old campaign-drain, independent-review or update-only hold is a
standing release gate. Diagnose a newly observed defect on current source.

#164's outbound connector is also closed after its restart/interruption
acceptance harness. #163 separately passed a real verified-TLS calculation,
including logs and checksum-verified results, within its trusted-group boundary.
Publication visibility and deployment remain separately authorized
operations; this roadmap does not change either.

## 2. Open work and the next useful increment

Tracker labels such as `workflow::intake` can lag substantial implementation.
The descriptions below distinguish source delivery from the remaining requested
outcome. Technical guides carry exact command/schema contracts.

| Issue / guide | Implemented now | Remaining work or acceptance |
|---|---|---|
| **#159 [Resource estimates](resource_estimation.md)** | Read-only `vq estimate` with file/directory fingerprints, versioned evidence/features and explicit memory scope, evaluation with censored failed/OOM observations; on main, `submit --estimate` binds input content and preserves the receipt, and `estimate-collect` exports completed-job evaluation sets; `estimate-calibrate` fits peak-memory and runtime-upper-bound multipliers on a training set, judged by `estimate-evaluate --calibration` on held-out cases, and applied by `estimate --calibration` to the estimates jobs are submitted with; `submit --mem-from-estimate` and `--time-from-estimate` opt in to a calibrated estimate's memory and wall-time limits. | Product-produced feature/input binding, QVF discovery, method-specific analytical components, runtime/scratch/layout models, and letting estimates inform placement beyond those opt-in limits. Estimates do not automatically change resource limits, admission or placement; the broker refuses attached estimates. |
| **#160 [Placement](placement_policy.md)** | Read-only `vq placement explain` filters fit/compatibility and ranks completion-time components. Checks include OS/ISA/runtime, account, license/data access, memory/scratch, wall time and MPI layout. | Live capability ingestion, opt-in durable broker integration for daemon/PBS/Slurm targets, reservation/recovery and fairness comparison, followed by bounded live acceptance. The FIFO broker now also has an initial local-driver PBS/Slurm adapter; remote-driver broker placement remains pending. |
| **#161 [Portable runtimes](portable_runtime_bundles.md)** | Authenticated manifests, package/publish/verify/deploy/rollback, file/HTTPS/SSH transport, bearer/mTLS HTTPS credentials and, in v0.26.45, immutable generation binding for daemon jobs; on main, scheduler jobs bind a named generation that their job script verifies on the node, `runtime-bundle prune` removes generations no selection or live job keeps, and a `vq.runtime-trust/2` policy can trust publisher OpenSSH signatures (`runtime-bundle sign`) in place of per-digest pins, and `runtime-bundle build` / `build-finish` run a product builder as a local vq job and pack its completed output. | Product-owned relocation/ABI/native/data qualification on intended node classes and vendor-specific import qualification. Queue transport does not make an arbitrary chemistry build portable. |
| **#162 [Benchmark campaigns](benchmark_campaigns.md)** | Plans, resumable submission/collection, paired analysis and uncertainty, CSV/JSON/plot export, isolation and checkpoint recovery. | A bounded real generic-versus-optimized pilot with qualified runtime identities, numerical agreement, repeats and measured cost/benefit. No optimization speedup or default variant is established by synthetic analysis tests. |
| **#163 [HTTPS gateway](gateway.md)** | Scoped client keys, policy, resumable verified uploads, idempotent submit, status/logs/cancel, checksummed results, quotas/retention and `vq remote`. In v0.26.45, #164 adds an outbound-only connector with fencing, recovery and spool backpressure. | Verified closed after a real bounded calculation from a client without fleet access through a verified TLS endpoint, with logs and checksummed result retrieval. This remains a trusted-group interface; per-project OS-account isolation is outside the accepted MVP. |
| **#165 [Artifact campaigns](campaigns.md)** | Static DAG, manifests, durable controller/retry lineage, cross-host artifact relay, cleanup and console view. Main records executed runtimes and binds declared portable runtime digests on daemon hosts and scheduler targets; a scheduler job's generation counts as run only once the job completed with exit code 0 after its node verified it. | Verified closed after a real PBS calculate → Slurm validate → local render campaign across owning hosts, with a directory/QVF artifact and node-verified portable generation. Explicit scientific validation remains necessary for each new campaign; job exit zero alone is not chemistry acceptance. |
| **#168 [Fleet calculation view](fleet_console.md)** | Elapsed time, declared versus observed resources, final usage, freshness/unknowns, filters/pagination and trends; v0.26.45 adds scheduler node details, whole-GPU allocation and measured synthetic refresh overhead; on main, an optional `nvidia-smi` adapter reports a scheduler GPU job's per-device memory and utilisation, live while it runs and summarised once it has finished. | Real direct/PBS/Slurm/GPU collector observations and allocated-device telemetry are preserved. Live browser acceptance and a redacted article screenshot remain pending; the observations do not establish simultaneous execution of all four kinds or GPU compute performance. Requested GPU count is allocation, not utilization; on main, daemon hosts with `--gpu-devices` admit and assign whole devices and report a job's GPU use live and once finished, and a multi-user daemon can opt in to isolating devices. |
| **#153 [Native-install reuse](native_install_seeding.md)** | Opt-in product adapter and provenance/semantic-input checks for candidate-prefix native/data verification. | Product qualification and a receipt from the next authorized live deployment. No default chemistry adapter or universal cache-reuse claim; missing provenance remains a scoped hold. |

### Dependencies that actually matter

- #159 calibration and #162 measurements can improve #160's models; they are
  not prerequisites for using the existing read-only placement explanation.
- #165 campaigns that demand a portable runtime digest on a scheduler target
  run on #161's scheduler binding; the accepted campaign exercised a real
  scheduler node and verified the exact executed portable generation.
- #163 live TLS acceptance can exercise the completed #164 relay, without
  reopening its already accepted restart/recovery implementation.
- #153 native-install seeding and #161 complete-runtime distribution solve
  different problems. Neither permits skipping product provenance or ABI checks.
- #168 observation remains read-only and can qualify independently of #160
  automatic placement. Unsupported telemetry must stay unknown, not zero.

## 3. Next feature release and article scope

**v1.0 is the maintainer's next publication target** (2026-10-22),
superseding the v0.27 planning target. This is a stability milestone for the
existing queue interfaces, not a requirement to complete every enhancement.
The [1.0 compatibility boundary](version_compatibility.md#10-compatibility-boundary)
defines what the release commits to preserving. No version, candidate or tag
is established by this planning update.

For an article-oriented cut, assemble the already delivered queue, runtime,
broker and calculation-observation workflows, then include only the new
capabilities whose claimed behavior has the relevant acceptance. Freeze exact
versions and supported interfaces for reproducibility. A paper may describe
implemented opt-in previews with explicit limits; it must not describe pending
calibration, scheduler federation, portable chemistry qualification or GPU
utilization as completed results.

The shortest remaining demonstrations to consider for the feature narrative
are #163's TLS submit-to-results path, #168's mixed-fleet view and #165's bounded
artifact campaign. They are independent qualification tasks, not an all-or-
nothing release chain. Leave unfinished increments in section 2 and narrow the
release headline if a demonstration is unavailable at the article freeze.

The release owner chooses the exact candidate and records accepted behavior
and known limits, then runs the prescribed candidate CI and release checks.
Ordinary documentation/source landings do not trigger full CI. Other apps'
release numbers, whole-fleet rollout and completion of all eight issues are
not prerequisites for cutting Queue's next release.

Later v0.28–v0.33 names remain reservations, not assigned milestones. In
particular, retention and broker functionality already exist; their old
association with v0.31/v0.33 artwork does not defer them to those versions.

### GitLab release milestone

The existing milestone **1**, currently titled **v1.0 - Article-ready queue
workflows**, and acceptance issue **#189** remain the release records. The
release owner must reconcile the candidate and checklist with the
v1.0 target; this roadmap does not mark that publication work complete.

Before freezing the candidate, record the exact included claims and resolve
these outstanding demonstrations:

| Workflow | Accepted evidence and remaining acceptance |
|---|---|
| #163 HTTPS submit-to-results | Verified closed: real TLS submission, placement, logs and fetched results, plus held status/cancel acceptance. Preserve the documented trusted-group identity boundary. |
| #165 artifact campaign | Verified closed: a real PBS calculate → Slurm validate → local render campaign consumed the exact validated QVF and verified a portable generation on its scheduler node. Original failure and retry lineage were retained; supported cleanup preserved the final artifact and referenced failures. Existing controller/restart/idempotency checks are reused. |
| #168 fleet calculation view | Real direct/PBS/Slurm/GPU histories and allocated GPU device telemetry are preserved. Browser acceptance and the redacted article screenshot remain pending. Device readings are not per-job utilization or a GPU-compute benchmark; the snapshots do not show all four job kinds simultaneously running. #197 is verified closed: the current CLI reads the original historical job with unavailable scheduler CPU as null while retaining its separate valid process measurement. Deployment to other readers remains a rollout task. GPU isolation has not been verified on real GPU hardware/systemd; MIG is not modelled. |
| #194 portable command through gateway | Verified closed: real HTTPS bare `python3 run.py` imports passed on a direct daemon and on both PBS and Slurm, with persisted runtime pins and checksum-verified result fetching. #195 supplies local-driver scheduler placement, scoped admission holds and shared alias reservation accounting. Remote-driver gateway placement is not supported. |

The release owner records acceptance for included behavior and obtains an
explicit scope decision for any unresolved demonstration. They are **not
silently deferred by changing the version to 1.0**. Broad enhancement requests
remain separately tracked; a preview must retain its documented limitations.
Close the milestone only after the selected criteria, prescribed release checks
and published tag are recorded. Exact runtime pins and live job bindings remain
unchanged during qualification.

## 4. Future options and scope boundaries

The archived web-console M2–M6 labels are no longer a second release plan.
The current console, gateway and campaign guides define delivered behavior;
#168 owns the outstanding unified resource-view work. OIDC, broader
organization hardening and any additional clear-failed workflow need a current
bounded requirement before they become release commitments.

Historical RECOV-4 recycled-process-group protection, ISO-1 cross-user read
isolation and privileged root-daemon deployment proposals have no current
open tracker entry. Keep their historical context; recheck current source and
requirements before filing new work. They are not silently accepted features
or inherited release blockers.

Current scope includes multiple hosts and schedulers, trusted-group scoped API
identities, small artifact DAGs and whole-device scheduler GPU requests.
Consequently the old blanket exclusions of distributed queues, all workflow
DAGs and all GPU scheduling no longer describe the product. Boundaries remain:

- No adversarial multi-tenant sandbox or unrestricted cross-organization service.
- No general dynamic scientific workflow language, distributed compute graph
  engine or container orchestrator.
- No migration of running calculations or scheduler-bypassing resource allocation.
- No fractional GPU sharing, daemon GPU assignment or universal GPU telemetry.
- No automatic claim of chemistry correctness, predictive accuracy or binary
  portability from a successful queue operation alone.

## Maintaining this roadmap

Reconcile this roadmap **every week** against current source, release tags,
open and recently closed issues, and relevant acceptance evidence. Also update
it when material evidence changes between reviews. Date the reconciliation and
keep implemented, accepted, released and deployed states distinct. Use the
GitLab milestone for the release scope and criteria, and issues for concrete
work and evidence; do not duplicate the entire roadmap as an issue checklist.

When the release milestone is achieved, record its released version and advance
the next feature-release target to the next justified version. A review may mark
criteria satisfied or correct a planning target; actual package-version changes
and tags follow the existing release process and candidate checks. Feature
milestones do not automatically justify a SemVer major increment or require
other apps to use the same number. Preserve incomplete work with its precise
remaining criteria instead of declaring it done when a version changes.

Use the tracker for detailed criteria and ownership, and changelog fragments for shipped behavior. Keep
release inclusion, functional acceptance and deployment distinct. Do not
recreate closed issues as a parallel host-validation ledger. Preserve historical
designs as history, and update their live cross-references when an interface
changes. Keep private deployment identities, evidence and configuration outside
this product repository.
