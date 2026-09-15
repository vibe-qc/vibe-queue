# vq Long-Term Design Specification

**Status:** Living design document; May 2026 draft under section-by-section reconciliation
**Original anchor:** `main` @ `f402de4` (vibe-queue)
**Companion document:** `docs/roadmap.md`
**Author:** Drafted with M. Peintinger
**Audience:** vq core developers, future cluster admins deploying vq, integrators (vibe-qc, ORCA, custom codes), AI agents (Claude Code) submitting jobs

---

## 0. Document purpose

This file began as the May 2026 long-term design proposal for vq. It now also
records the staged reconciliation of that proposal with the architecture that
landed on `main`. It is not yet a whole-document description of current code.
`docs/roadmap.md` remains the sequencing source of truth.

In a section explicitly classified as current, "MUST" states an invariant,
"SHOULD" states the default with documented escape hatches, and "MAY" states an
option. In a future section, those terms constrain the proposed design but do
not describe shipped behavior or promise a release. They have no normative
force in retired or still-unclassified prose.

If something here disagrees with what is on `main`, `main` wins and this doc is wrong, and the fix is a PR to this doc, not to the code, unless the code is also wrong.

### 0.1 Classification labels

Reconciled sections use these labels:

A label applies to the smallest enclosing section or explicitly labelled
paragraph, so a narrow classification overrides a broader one.

- **Current architecture:** verified against current code and tests. It may be
  treated as an implementation contract within the section's stated scope.
- **Retired proposal:** an earlier design that did not land or was superseded.
  Do not change current code merely to make it true.
- **Future consideration:** an unimplemented option, not a release promise. It
  requires an ordinary design and approval before implementation.
- **Mixed:** the section explicitly separates current, retired, and future
  parts.

Any section without a classification label remains part of the original May
2026 proposal. Treat it as historical or prospective until it is reconciled;
use `main` and the current operational documentation for present behavior.

---

## 1. Mission statement and non-negotiables

**Classification: mixed.** Current architecture and scope boundaries are
distinguished from retired or future clauses below.

### 1.1 What vq is

vq is a cross-machine scientific job queue and scheduler frontend. A daemon owns
durable state for its queue and runs local child processes or stages
scheduler-targeted jobs over SSH to daemonless Torque and Slurm hosts. Remote
vq daemons, single-user and multi-user layouts, fleet routing, the web UI, and
versioned API surfaces are current.

Its primary user remains a quantum-chemistry developer. vq is designed to run
SCF, DFT, AIMD, and basis-set-fitting workloads, with declared resources and
configured enforcement protecting execution hosts where the platform supports
it, durable job state across daemon restarts, and no ad hoc
terminal-multiplexer tracking. The original future swappable-dispatcher
proposal is retired: external schedulers use a current parallel execution path.

### 1.2 Non-negotiables across all versions

1. **One versioned JobSpec model with explicit backend fields.** Command,
   resource, provenance, and lifecycle fields are shared. `scheduler_target`
   selects the scheduler path; `scheduler_job_id`, scheduler status, and remote
   detail fields are meaningful only there, while `pid` and `pgid` describe the
   local path. A missing target preserves local execution. The model is
   declarative, but it does not pretend backend-specific observations are
   interchangeable.
2. **Explicit restart semantics.** A daemon restart never silently re-runs a
   job.
   * A local RUNNING or SUSPENDED job with a live process group and no
     contradiction from any available PID-fingerprint or cgroup-ownership
     check is reattached. If the process is gone, a durable exit marker can
     still recover its return code; otherwise it becomes `ABORTED_BY_QUEUE`
     with a recorded reason.
   * A scheduler job is reattached from its recorded scheduler ID or the ID
     written in its remote workspace. A failed reattach remains nonterminal
     with `scheduler_state=reattach_failed` and is retried; it is not resubmitted
     as a new scheduler job.
   * `INTERRUPTED` remains readable for old specs but is no longer written by
     the daemon.
   * `--auto-resume` is an explicit policy for a local RUNNING job lost across
     reboot. When eligible, it emits a sibling spec in the same workspace with
     lineage recorded; it is never an implicit default.
3. **Durable record before execution.** The workspace and JobSpec exist before
   local process launch or remote scheduler submission. A crash in an
   acknowledgement window can leave an unowned process or scheduler job, but
   it must not erase the pre-existing job record under the documented durable
   filesystem assumptions.
4. **Single-language vq-owned core.** The CLI, daemon, web service, scheduler,
   and program support are Python. vq has no Go/Rust core, compiled vq
   extension, or Node build toolchain; third-party dependencies are outside
   this ownership statement.
5. **Linux first, macOS development only.** Production execution hosts are Linux. macOS is a supported submit and dev target. Anything macOS-specific (no cgroups or `/proc`) is a feature gate, not a code path duplication.
6. **No mandatory service manager for single-user operation.** systemd-user is
   the recommended production supervisor, but `vq daemon run` in a terminal or
   tmux remains a valid single-user deployment.
7. **Versioned, read-tolerant JobSpec evolution.** Current readers accept old
   supported specs through defaults and ignore unknown same-version fields;
   rewriting does not preserve those unknown fields. Readers reject a spec
   version newer than the running build. Required-field or meaning changes
   require a version bump and compatibility plan. The proposed automatic
   migration and `vq migrate` command did not ship.
8. **No hidden telemetry or phone-home path.** SSH transport, scheduler
   commands, fleet/recovery probes, Git update workflows, and config-gated
   terminal webhooks are current outbound paths. SSH endpoints may come from
   SSH config and Git endpoints from repository remotes; submitted commands
   can make their own connections. There is no vendor telemetry, global egress
   allowlist, `vq config show`, or promise that every endpoint appears in vq
   TOML.

### 1.3 What vq is not, and is not trying to become

- A general workflow DAG engine. vq has job-level dependencies, arrays, and
  chains, but orchestration suites such as Snakemake, Nextflow, or AiiDA remain
  the owner of larger workflows.
- A distributed compute graph runtime. That is Dask's job.
- A container orchestrator. A submitted command may invoke an OCI/HPC container
  runtime, but vq does not schedule pods.
- A multi-tenant SaaS. vq is research-group infrastructure across a configured
  fleet, not an organization-wide hosted service.
- A replacement for an external scheduler at scale. vq is the frontend and
  durable workflow boundary; Torque or Slurm owns cluster placement.
- A GPU fractional-sharing system. First-class GPU requests remain a future
  consideration. Trusted scheduler extras can carry site-specific accelerator
  directives, but vq does not model, account, or enforce GPUs. If first-class
  GPU support is approved, the proposed boundary is whole devices rather than
  fractional sharing.

---

## 2. System architecture

### 2.1 Component model

**Classification: current architecture.** vq has six cooperating areas; several
share a process rather than defining deployable services.

1. **CLI** (`vq.cli`). The Click command surface reads durable queue state,
   invokes local domain services, and reaches remote hosts through SSH. It also
   uses the daemon's Unix-socket RPC surface for ping/provenance/method
   discovery, config reload, canonical admin-status and drain state, and
   persistent-throttle state. Ordinary submit, status, kill, pause, and resume
   paths remain disk/domain-service or SSH operations. The old claim that the
   CLI never uses a socket is retired.
2. **Daemon and control socket** (`vq.daemon`, `vq.rpc`). One supervisor owns a
   state root, admits local and scheduler-targeted jobs, reconciles running
   work, records state transitions, and serves a line-delimited JSON RPC socket.
   Durable state is on disk; in-memory local/scheduler handles are reconstructed
   or conservatively parked after restart.
3. **Execution mechanisms** (`vq.dispatch`, `vq.scheduler_dispatch`,
   `vq.scheduler_dialect`). `LocalDispatcher` owns the in-memory `Popen`
   lifecycle while the daemon retains local cgroup/process-group policy. The
   separate `SchedulerDispatcher` path owns remote staging, scheduler commands,
   polling, cancellation, and result retrieval for Torque and Slurm.
4. **Watchdog** (`vq.watchdog`). The local resource/liveness policy samples
   `/proc` or cgroup data and returns enforcement verdicts to the daemon. It
   does not supervise scheduler-targeted jobs.
5. **Web UI and HTTP API** (`vq.web`). One FastAPI app serves HTML and versioned
   `/api/v1/` routes, with single-host and optional fleet route modules. It can
   run as a standalone Uvicorn process or as the child sidecar owned by
   `vq daemon run --web`. Single-host actions call disk-backed domain services;
   fleet mode adds SSH fan-out. Current writes cover kill with optional
   resubmit, pause/resume, queue controls, and failed-row cleanup; there is no
   HTTP submit/upload/watch API.
6. **Durable domain services.** `vq.spec`, `vq.paths`, `vq.events`, listing,
   submission, kill/pause/resume, cleanup, program configuration, and preflight
   modules provide the shared on-disk and workflow operations used by the CLI,
   daemon, and web app.

The proposed pluggable `vq.adapters` package never shipped and is retired for
this workstream. Program-specific behavior lives in concrete program/config and
preflight modules rather than a Python entry-point plugin layer.

JobSpecs and workspace metadata remain the durable queue contract. The daemon,
CLI, and web actions can all mutate state through their owning services; every
read-modify-write of an existing spec is serialized by `spec_lock`. Fresh
submission is currently a CLI/SSH-to-CLI path, not a web POST endpoint.

### 2.2 Data flow

**Classification: current architecture.** Fresh user submission begins at the
CLI; the HTTP API and web UI do not accept job submissions or uploads. The CLI
resolves the requested host to a local queue, an ordinary remote daemon, or the
fixed driver for a scheduler target. The receiving submit path validates the
payload shape, dependencies, and applicable program/runtime authority, then
materializes a workspace and atomically writes a `PENDING` JobSpec under its
own queue directory. An opt-in local `--vibeqc-preflight` may run a non-fatal
vibe-qc dry run to discover output hints. There is no generic adapter preflight
or post-flight output validator.

Each receiving submit appends a best-effort `submitted` record to
`<workspace>/_vq/events.jsonl` after the spec write. The JobSpec, not this
observability log, remains the authoritative mutable state.

The execution paths then diverge:

```text
Local queue
    -> daemon applies dependency, time, drain, quota, and local capacity gates
    -> dispatch-time runtime, multi-user, and cgroup-scope checks
    -> under the spec lock, claim RUNNING before process launch
    -> LocalDispatcher launches the wrapped Popen child
    -> under the spec lock, record pid/pgid; append dispatch/state events
    -> local watchdog samples/enforces; Popen supplies the normal return code
    -> exit marker supports restart and orphan recovery
    -> retry may return the job to PENDING, otherwise record a terminal state
    -> normal terminal bookkeeping may run webhook/rerun/workdir-cleanup policy

Ordinary remote daemon
    -> client CLI uploads the payload over SSH and invokes remote vq submit
    -> remote queue owns the workspace, JobSpec, events, daemon, and local flow
    -> status, logs, controls, wait, and an explicit fetch cross SSH as needed

Scheduler target
    -> target resolves to its configured driver, reached over SSH when remote
    -> driver owns a PENDING spec/workspace carrying scheduler_target
    -> daemon applies scheduler drain, quota, and max-scheduler-jobs gates
    -> claim RUNNING before SchedulerDispatcher stages and runs qsub/sbatch
    -> persist scheduler_job_id; append dispatch/state events
    -> batch-poll qstat/squeue while scheduler_state records cluster detail
    -> exit marker fences result copy-back and is the primary return-code source
    -> make bounded attempts to fetch the scheduler workspace to the driver
    -> record terminal state, marking artifacts unavailable if fetch is exhausted
    -> normal terminal bookkeeping may run the optional state-filtered webhook
```

Hostless per-job discovery does not make a configured remote default the queue
authority. `vq status JOBID` reads each configured, non-down authority's
durable queue listing, including archived specs, and selects the unique row
whose job ID and scheduler lane match. A scheduler handle is the tuple of its
driver-owned queue and persisted `scheduler_target`; two logical lanes on one
driver therefore remain distinct, while the driver listing is read only once.
Ordinary daemon and built-in local aliases collapse to their canonical
authority. Invalid or unavailable listings provide no negative ownership
evidence; multiple matching searched authorities and matching unconfigured
scheduler targets fail closed. An explicit `HOST` remains authoritative, and a
local default retains its direct no-fan-out path. Other hostless per-job verbs
retain the Baran's Detour ownership rule. An administratively down default
triggers fleet-wide durable discovery. An unmarked remote default remains the
selected target, but before invoking the verb `vq` performs one bounded,
read-only durable queue probe against that default's exact action queue
authority. A trusted listing, whether the job is found, absent, or duplicated,
preserves direct default routing. An unavailable or invalid listing supplies no
reachability evidence, so `vq` refuses with a diagnostic naming the inferred
default and asks for an explicit host. The probe never broadcasts the requested
action.

Current terminal states include `COMPLETED`, `FAILED`, `KILLED`,
`OOM_KILLED`, `STARVED`, `TIME_EXCEEDED`, and `ABORTED_BY_QUEUE`.
`INTERRUPTED` remains readable for compatibility. There is no email backend or
automatic local/remote-daemon copy back to the submitting client. Explicit
fetch and web browsing are later read/transport surfaces; scheduler copy-back
is specifically from the cluster to the owning driver workspace. Webhooks are
fire-and-forget and are not wired to every exceptional terminal path.

### 2.3 Storage model and its evolution

**Classification: mixed.** The current filesystem contract and retired database
proposal are separated below.

**Current architecture.** A JobSpec JSON file under a queue directory is the
canonical mutable record for each job. Listing remains a filesystem scan and
parse; there is no database index. The append-only JSONL file at
`<workspace>/_vq/events.jsonl` is best-effort observability history rather than
a transactional journal or alternate source of truth.

Single-user state resolves from `VQ_STATE_DIR`, otherwise
`XDG_DATA_HOME/vq` with `~/.local/share/vq` as the default. Multi-user mode
uses `VQ_MULTI_USER_ROOT` or `/var/lib/vq`, puts user-owned queue/workspace/
archive/workdir trees under `users/<uid>/`, and keeps daemon-wide control files
at the system root. Configuration is separate: `VQ_CONFIG_DIR` or the XDG
config root for ordinary use, and `/etc/vq/config.toml` for the standard system
deployment. Moving from a single-user tree to multi-user mode is an explicit
stopped-daemon filesystem migration; vq does not move or reinterpret the old
tree automatically.

**Retired proposal.** The May 2026 plan for `index.sqlite`, a global event-log
mirror, `vq reindex`, and a system-wide SQLite view never shipped. Current code
has no SQLite or Postgres persistence layer and no `vq migrate` or `vq reindex`
command. A future database-backed index or multi-host control plane would need
a new design and compatibility plan; the retired proposal does not reserve a
particular database or make that change a one-module swap.

**Current concurrency model.** The JSON spec store has many writers - the
daemon main loop, the daemon's in-process RPC thread, and write-capable CLI
verbs running as separate processes. Two layers keep them consistent:

1. `JobSpec.write()` uses the config-free `vq._storage.atomic_write_text`
   primitive, also compatibility-exported by `vq.paths`. It makes each write
   atomic against *readers* (unique `mkstemp` temp → `fsync` → rename, followed
   by a best-effort parent-directory `fsync`), so a reader never sees a torn or
   zero-length spec.
2. `paths.spec_lock(spec_path)` is a per-spec sidecar `<spec>.lock` advisory
   `flock`. It serializes a whole read → mutate → write against other
   *writers*, so two writers cannot lose an update. The canonical case is
   `vq kill` writing KILLED while the daemon writes COMPLETED, where the
   daemon's stale-read write would otherwise clobber the kill.

**Invariant for all future code: any read → mutate → write of a spec MUST hold `spec_lock` for that spec across the read and the write.** The lock never nests (no writer holds two spec locks at once, so there is no lock ordering and no deadlock against the daemon-singleton lock) and is scoped tightly - never held across a `Popen`, a workspace/workdir copy, a webhook, or a tarball write - so it never serializes slow I/O. This is the serialization half of the 2026-05-31 reliability audit's root-cause fix (`HANDOVER_VQ_AUDIT.md`); wired across the writers in v0.8.11-v0.8.13.

### 2.4 Filesystem layout

**Classification: current architecture.** The state and config roots are
independent. These trees pin the durable job boundaries and the principal
daemon files; feature modules also own root-level control, audit, rollout, and
admin state files.

The default single-user layout is:

```text
<state-root>/
  queue/
    <jobid>.json               # authoritative JobSpec
    <jobid>.json.lock          # advisory writer lock, created as needed
    .vq-daemon.lock            # one-daemon lock for this queue
  jobs/<jobid>/                # submitted workspace and results
    stdout.log                 # default captured-output path; spec-configurable
    stderr.log                 # default captured-output path; spec-configurable
    _vq/                       # reserved vq lifecycle metadata
      events.jsonl             # best-effort lifecycle history
      exit-code                # wrapper return code, when produced
      samples.jsonl            # optional local-watchdog samples
      scheduler-job-id         # scheduler-only recovery marker, when copied back
      resource-usage.json      # direct or scheduler terminal resource receipt
    <submitted files and results>
  workdirs/<jobid>/            # local-job VQ_WORKDIR scratch
  archive/<dest-dirname>.tar.bz2  # jobid, or job-name-jobid when named
  daemon.pid
  daemon.log
  daemon.sock
```

`VQ_ARCHIVE_DIR` and cleanup policy can move archives without moving the rest
of the state root. The CLI's `client.log` and files such as `drain.json`,
`throttle.json`, `daemon_capacity.json`, `rpc-audit.jsonl`, and admin/update
records are additional current state-root surfaces, not children of a database
or global event directory.

The default multi-user layout is:

```text
<system-root>/
  .vq-daemon.lock              # one system daemon
  daemon.pid
  daemon.log
  daemon.sock
  <system control, audit, and admin state>
  users/<uid>/                 # provisioned and owned by that uid/gid
    queue/<jobid>.json
    queue/<jobid>.json.lock
    jobs/<jobid>/              # same workspace shape as above
    workdirs/<jobid>/
    archive/<dest-dirname>.tar.bz2
```

Configuration and web authentication live outside those trees. Ordinary use
resolves `<config-root>/config.toml` and `<config-root>/web-token`; the standard
multi-user deployment uses `/etc/vq/config.toml` and `/etc/vq/web-token`.
There is no top-level `events/`, `index.sqlite`, or `tokens/` state directory,
and `adapter.json` is not a current workspace artifact.

Public job identifiers remain bare for compatibility, but the multi-user
storage authority is the owner-qualified spec path. Discovery first groups all
per-user records by job ID. If an ID occurs in more than one owner tree, every
record with that ID is quarantined from dispatch while unrelated unique jobs
continue; the colliding specs are not mutated. A public bare lookup fails as
ambiguous, while an explicit UID lookup remains available. Once a local or
scheduler job is admitted, its in-memory record retains the trusted owner and
exact spec path so a collision introduced later cannot redirect reconciliation
or terminal writes.

Within a workspace, `_vq/` is reserved for vq lifecycle metadata. Captured
logs, submitted payloads, and results coexist outside it, so the retired claim
that vq never writes elsewhere after dispatch was not a current invariant.
Metadata files are created lazily; their absence alone is not proof that a job
record is corrupt. A scheduler target also has an execution copy below its
configured remote `scratch_root` (currently
`.vibeqc-cluster/jobs/<jobid>/`). That copy is staged and later merged back into
the driver's workspace; it is not a separate authoritative queue store.

---

## 3. Job model

### 3.1 JobSpec, conceptually

**Classification: current architecture.** A JobSpec is the versioned, mutable
on-disk lifecycle record for one job. Submission writes its identity, command,
workspace, requested resources, dependencies/grouping, retry and cleanup
policy, program/runtime provenance, and initial `PENDING` state. The daemon and
control paths later add lifecycle timestamps, local process or scheduler
identity, cluster observations, pause/recovery data, failure evidence, fetch/
archive observations, and the terminal outcome.

A JobSpec is central but is not a self-contained portable launch bundle or the
only execution contract. The submitted workspace, markers, and events are
separate workspace artifacts. Host configuration, the program registry, runtime
deployment, and (for a scheduler target) dialect and site hooks remain external
inputs. Consequently, copying a JSON spec to another host is not sufficient to
dispatch it there. Local and scheduler fields intentionally share one model
while retaining backend-specific meaning, as described in §1.2.

Schema evolution is read-tolerant rather than migration-driven. Fresh specs
default to schema v2. Current readers accept supported older specs through
defaults, ignore unknown fields, and reject a `spec_version` newer than the
running build. Rewriting a spec emits the current known model and therefore
does not preserve ignored unknown fields. Required-field or meaning changes
need a schema-version and compatibility plan; ordinary optional additions use
defaults.

### 3.2 Current JobSpec field inventory

**Classification: current architecture.** The source model is authoritative;
this inventory groups every current top-level field by ownership rather than
repeating its Pydantic type declaration.

- **Identity and submission:** `spec_version`, `id`, `job_name`, `tags`,
  `submitter`, `workspace_source`, `submitted_at`.
- **Payload and captured I/O:** `command`, `cwd`, `stdout_path`, `stderr_path`,
  `qvf_artifact_name`.
- **Resources and admission:** `cpus`, `scheduler_tasks`, `mem_mb`,
  `wall_time_seconds`, `priority`, `not_before`. CPU count is required;
  memory, wall time, and scheduler task count remain optional.
- **Lifecycle and outcome:** `state`, `started_at`, `finished_at`, `exit_code`,
  `failure_reason`, `failure_tail`, `last_heartbeat_at`. Current local
  watchdog sampling does not write `last_heartbeat_at`; scheduler
  reconciliation does.
- **Local execution, recovery, and pause:** `pid`, `pgid`, `pid_start_time`,
  `workdir`, `recover_on_reboot`, `parent_jobid`, `paused_at`,
  `paused_monotonic_at`, `paused_seconds_total`, `paused_by`.
- **Scheduler execution:** `scheduler_target`, `scheduler_job_id`,
  `scheduler_state`, `scheduler_remote_workspace_cleaned_at`,
  `scheduler_exec_host`, `scheduler_walltime_used`,
  `scheduler_walltime_limit`. These remain `None` or inapplicable for local
  execution; scheduler jobs do not populate the local PID fields.
- **Dependencies, retries, and grouping:** `depends_on`, `depends_on_any`,
  `retry_max`, `retry_count`, `array_index`, `array_total`, `array_group_id`,
  `chain_index`, `chain_total`, `chain_group_id`,
  `rerun_until_file_exists`, `rerun_max`, `rerun_count`. Array elements are
  independent JobSpecs rather than native scheduler arrays.
- **Runtime and build provenance:** `branch`, `program`,
  `program_runtime_pin`, `refresh_before`, `build_env`. The optional nested
  runtime pin captures the applicable Git/import expectations or scheduler
  artifact identity at submission.
- **Cleanup and observation:** `clean_workdir_on_terminal`,
  `workdir_swept_at`, `last_status_at`, `last_fetched_at`, `archived_at`,
  `archive_path`.
- **vibe-qc output hints:** `expected_outputs`, `output_stem`,
  `last_output_status`. These are optional discovery/status hints, not a
  generic post-flight output contract.

The retired proposal's `adapter`, `adapter_config`, declared `env`, checksum
`inputs`, GPU/scratch fields, relative `working_dir`, `state_history`,
per-job notification fields, and nested platform-wide `reproducibility` object
did not ship. Notifications remain daemon configuration, and the daemon builds
the child environment from process state, JobSpec fields, and the program
registry. Any first-class GPU request, secret reference, or portable input
manifest remains a future design rather than a reserved field.

### 3.3 Job states

**Classification: current architecture.** `JobState` has three writable
nonterminal states and seven writable terminal states. `INTERRUPTED` is an
eighth terminal enum value retained only so old specs remain readable.

| Nonterminal state | Current meaning |
|---|---|
| `PENDING` | Durable spec exists and is waiting for its time/dependency/drain/admission gates. Validation and workspace materialization have already happened. |
| `RUNNING` | vq has claimed execution ownership. For a local job this covers the Popen lifecycle; for a scheduler job it starts before qsub/sbatch and includes cluster-queued time. `scheduler_state` carries the finer cluster phase. |
| `SUSPENDED` | A running local process group is SIGSTOP'd or a scheduler job is held. It remains nonterminal and continues to reserve vq capacity. |

The principal writable transitions are:

```text
submit -> PENDING
PENDING -> RUNNING
RUNNING <-> SUSPENDED
RUNNING -> PENDING              # plain nonzero exit with retry budget left
PENDING/RUNNING/SUSPENDED -> terminal state, as applicable
```

A retry happens before `FAILED` becomes durable: the same spec returns to
`PENDING`, increments `retry_count`, and receives a `not_before` backoff. A
convergence rerun or reboot auto-resume instead creates a sibling JobSpec with
lineage; it does not reopen the original terminal record.

| Terminal state | Current meaning |
|---|---|
| `COMPLETED` | The wrapped command returned zero. There is no adapter post-flight acceptance step. |
| `FAILED` | A nonzero return exhausted retries, or validation, dependency, build, admission, or dispatch bookkeeping failed the job. |
| `KILLED` | An operator/admin kill ended a pending, running, or suspended job. |
| `OOM_KILLED` | The local watchdog attributed termination to a memory limit, or scheduler accounting reported an out-of-memory kill (e.g. SLURM `OUT_OF_MEMORY`). |
| `STARVED` | The local watchdog attributed termination to sustained low CPU activity. |
| `TIME_EXCEEDED` | Local wall-time enforcement fired, or scheduler evidence established a wall-time overrun. |
| `ABORTED_BY_QUEUE` | Recovery or scheduler reconciliation could not establish a normal return code/liveness outcome and the queue conservatively ended the record. |
| `INTERRUPTED` | Legacy terminal value accepted on read. Current daemon paths do not write it. |

Terminal labels are preserved when late process or scheduler evidence arrives;
that evidence may fill fields such as `exit_code` without replacing the chosen
state. The May 2026 `PENDING_VALIDATION`, `QUEUED`, `STAGING`, `FINALIZING`,
`DONE`, `LOST`, and `REJECTED` ladder did not ship and is retired. Those
concepts are represented by gates, diagnostic fields, and the states above,
not by hidden enum values.

The JobSpec is the state authority. `events.jsonl` receives best-effort
submitted/dispatch/transition records, but it is not a transaction journal and
some exceptional or unsafe-workspace paths intentionally cannot append to it.
Do not reconstruct authoritative state by taking its last line.

### 3.4 Program integration boundaries

**Classification: mixed.** The current program registry, runtime-identity,
preflight, and artifact boundaries are described first. The original adapter
and plugin design is explicitly retired below.

**Current architecture: registry and launch.** Each vq configuration may define
a strict `[programs.NAME]` registry. A program has one of three concrete kinds:

- `binary`: an executable path, with existence and executable-bit probes;
  recognized serial CRYSTAL frontends also receive a bounded no-input startup
  probe in an isolated temporary directory;
- `venv`: a Python interpreter plus Git checkout, update metadata, import and
  health checks, and optional Git/import-version pins. Import checks remain
  optional for generic environments. A checkout with the managed vibe-qc
  source layout and its checkout-local `.venv` implicitly probes `vibeqc` even
  when a legacy registration omitted `import_check`. Its package must import
  from that checkout, its interpreter-reported compiled core must be readable,
  its filename must be an extension suffix supported by that interpreter, and
  that core must not predate the newest native compilation input. Explicit
  `import_check = "vibeqc"`
  registrations receive the same identity and freshness check when they use
  an alternate venv. When such a structural vibe-qc registration also has an
  `update_script`, its armed transaction requires a clean checkout with no Git
  operation in progress, a recoverable core snapshot, and native-source
  freshness evidence before Git mutation.
  A failed armed transaction or post-build import/freshness check restores the
  pre-update checkout, detached/local-branch state, source timestamps, and
  every snapshotted native artifact, including the live core in venv
  `site-packages`; transaction-created loader candidates are removed and
  baseline symlink identity is restored. Fresh immutable runtime slots apply
  the same identity/freshness gate before activation, require the core to stay
  inside the slot generation, and revalidate legacy verified slots before
  reuse;
- `import`: a Python interpreter plus a module and optional required symbols.

`vq programs` probes and reports those entries. `vq doctor` also checks that
scheduler program-hook and managed-runtime keys refer to registered names.
These are readiness and operator-diagnostic surfaces; ordinary binary and
import availability is not re-probed as a universal per-job submit gate.

At the JobSpec-owning submit boundary, `--program NAME` must name a registered
program. For a local `venv` program, configured runtime-pin mismatches are also
rejected before queueing. The name is stored as `JobSpec.program` and exported
as `VQ_PROGRAM`. Local `venv` jobs additionally receive
`VQ_PROGRAM_BIN`, `VQ_PROGRAM_PYTHON`, `VQ_PROGRAM_GIT_DIR`, and, when
configured, `VQ_PROGRAM_BRANCH`; local `binary` jobs receive
`VQ_PROGRAM_EXE`, the validated executable path, so payloads can
`exec "$VQ_PROGRAM_EXE"` without host-conditional logic. Scheduler jobs do not
receive driver-local paths. Once dispatch has authenticated or observed the
runtime, Git-backed managed jobs also receive the full dispatch-authoritative
SHA as `VQ_PROGRAM_GIT_SHA`. The same SHA is persisted as
`ProgramRuntimePin.resolved_git_sha`; vq refuses to launch a managed `venv`
whose SHA cannot be read, so absence cannot silently masquerade as provenance.

For ordinary payloads, `--program` is identity and routing metadata. It does
not select an executable, parse or rewrite an input, or replace the submitted
`JobSpec.command`. Trusted scheduler-host configuration may add a matching
prologue, epilogue, or argv `command_wrapper` through
`[hosts.HOST.scheduler_program_hooks.NAME]`; that is a concrete site hook, not
a plugin interface.

A positional `.qvf` is the narrow current exception to the no-rewrite rule. It
requires a named managed runtime. Local submission constructs
`<registered-python> -m vibeqc._cli run <artifact>`, while scheduler submission
requires the matching configured compute-side `command_wrapper`.
`qvf_artifact_name` records that the same container is both input and result.
This concrete QVF protocol does not create a general code-adapter seam.

**Current architecture: runtime identity.** `ProgramRuntimePin` is an optional
additive JobSpec snapshot, not an adapter contract. For local `venv` programs,
an explicit `--expected-sha`, configured `expected_git_sha`, or configured
`expected_import_version` is enforced at submit and checked again immediately
before dispatch against the submitted snapshot. A Git SHA discovered when no
pin was requested is retained as provenance with `enforce_git_sha = false`; it
does not prevent a queued job from crossing a runtime rollout. Immediately
before dispatch, vq separately records the SHA that will actually run in
`resolved_git_sha` and exports it to the payload; the submit-time observation
is never reported as the execution identity after a rollover.

When a scheduler-target submit names a program with a managed runtime
deployment, the target runtime is authoritative rather than a same-named
checkout on the driver, regardless of whether the staged entrypoint is Python,
`bash`, or another wrapper. An unavailable target identity rejects the submit
before any spec exists; vq never downgrades such a request to a driver-local
observational pin.
Resolution uses an immutable wrapper already present in submitted argv when
applicable, then the target program registry, then the target's last verified
managed-runtime deployment record. The resulting pin records the scheduler
host, executable/artifact identity, version, and SHA. At dispatch the daemon
rejects a pin associated with a different scheduler target and deliberately
does not validate it against the driver-local checkout. The stored identity
does not by itself rewrite an ordinary command; launcher selection remains the
submitted command plus trusted scheduler hooks.

**Current architecture: preflight and artifacts.** Generic source-shape,
dependency, resource, ownership, runtime-pin, cgroup, and scheduler-launch
checks belong to submission or dispatch services. The only calculation-specific
submit preflight is the opt-in `--vibeqc-preflight`. It executes the submitted
command once with `VIBEQC_DRY_RUN=1` and may record `expected_outputs` and
`output_stem` from exactly one contained `.system` manifest created or changed
by that run. Manifest discovery has explicit time, entry, and candidate bounds.
Timeout, nonzero exit, missing or malformed output, ambiguous manifests, or a
discovery bound is non-fatal and submission continues. These fields guide the
specialized artifact readers described below; they are not a generic collection
or acceptance contract.

vq preserves the job workspace rather than collecting code-specific output
globs. Local jobs have a separate `$VQ_WORKDIR`; scheduler jobs stage and copy
back the shared scheduler workspace as a whole. Specialized readers may inspect
or follow lexically safe relative artifact paths already visible in that
workspace through the driver. When `node_scratch_dir` is unset, relative
calculation artifacts are written there and can be observed live. When it is
set, only the wrapper's shared stdout/stderr redirections are live; relative
calculation artifacts stay in compute-node-local scratch until successful
normal copy-back. Signal traps write the shared exit marker but bypass that
copy-back and scratch cleanup. The specialized readers never reach node-local
scratch.

Artifact path checks reject absolute and parent-traversal paths but retain the
existing scheduler-job trust boundary for job-created symlinks. `vq fetch`
retrieves the shared workspace,
`vq fetch --workdir` retrieves local scratch when present, and
`vq fetch --name BASENAME` retrieves one regular workspace file. Specialized
QVF, `.out`, `.system`, and structured-progress readers inspect those artifacts
after the fact, but they do not change the queue's process outcome. A wrapped
command returning zero becomes `COMPLETED`; there is no generic post-flight
output validator.

The serial `crystal`, `crystal23`, and `crystal23demo` frontend names receive a
bounded `vq programs` startup probe with stdin closed and an isolated temporary
working directory. Dynamic-loader diagnostics, exit 126/127, inability to
start, timeout, nonzero/signal exit, or output without CRYSTAL v1.0.1's
measured `ERROR **** INPUT ****  END OF DATA IN INPUT DECK` diagnostic make the
entry unavailable. The exact zero-exit diagnostic proves only that the frontend
reached its own startup handling. The executable is resolved before switching
directories so legacy relative registry paths remain stable. Parallel CRYSTAL
launchers are not run outside their scheduler/MPI context and retain the
generic executable-path check.

The only ORCA-specific production logic is a best-effort `vq programs`
availability probe for the sibling `orca_startup_mpi` loader. A loader failure
is reported as serial-only detail while the main binary remains available. vq
does not parse or rewrite `%pal` or `%maxcore`, enforce their agreement with
JobSpec resources, manage ORCA restart files or scratch, collect ORCA output
globs, delete ORCA temporary files, inspect the normal-termination marker, or
perform an ORCA license-allowlist check.

**Retired proposal.** The `Adapter` protocol, the `raw`, `vibeqc`, `orca`, and
`python` adapters, the proposed Gaussian/NWChem/Q-Chem/Psi4/CP2K stubs,
`adapter_config`, Python entry-point discovery under `vq.adapters`, adapter
validation/staging/environment/post-flight/output-collection hooks, and the
ORCA input-rewrite design never shipped. No `vq.adapters` package or entry-point
group exists, and this proposal reserves no future plugin interface. Any future
program-specific behavior requires a separately reviewed concrete design.

### 3.5 Submission interfaces

**Classification: current architecture plus retired proposal.** `vq submit` is
the only shipped interface for submitting new source material. It accepts one
local file, directory, or compressed archive and routes the normalized request
through `vq.submit`. A local target writes a `PENDING` JobSpec into the owning
daemon's queue; an ordinary remote target delegates the same CLI request over
SSH to that host; a scheduler target delegates to its configured driver, whose
daemon persists the JobSpec before submitting it to PBS or Slurm. The
scheduler host itself runs no vq daemon.

`vq resubmit` is a separate lifecycle operation: it clones an existing
terminal JobSpec with a fresh job ID rather than accepting a new source
payload. Daemon-created retry, reboot-resume, rerun-until-file, and build-job
siblings are likewise internal lifecycle transitions, not alternative public
submission transports.

The FastAPI application serves authenticated reads and selected operator
mutations, but it does not accept job submissions or file uploads. There is no
watch-directory consumer, upload-token service, HTTP idempotency-key store, or
email submission path.

**Retired proposal.** The proposed HTTP `/api/v1/jobs` submission endpoint,
web submission form, multipart upload service, and `~/.vq-inbox/` watch
directory never shipped. They define no current compatibility contract. Email
submission remains out of scope.

---

## 4. Reliability and failure handling

This is the section that earns the user's "we need a lot of fail checking" requirement.

### 4.1 Failure taxonomy

**Classification: current architecture.** vq classifies a failure by the
authority that owns the outcome and by how far the request progressed. There is
no adapter-attributed failure class and no `REJECTED` state.

1. **Synchronous request rejection.** CLI parsing, configuration, target and
   payload-shape resolution, source checks, program/runtime identity, or
   dependency-reference validation may reject a submit before an authoritative
   JobSpec is written. The caller receives a nonzero result and the lifecycle
   state machine is not entered.
2. **Admission and dependency failure.** Most recoverable gates leave an
   existing spec in `PENDING`. A failed required predecessor instead
   cascade-fails its dependent to `FAILED`. An automatically generated runtime
   build job whose request can never fit the daemon's configured caps also
   becomes `FAILED`; an ordinary operator job over those caps deliberately
   remains `PENDING`.
3. **Dispatch failure.** A failed multi-user trust check, enforced runtime-pin
   check, local privilege/cgroup/process launch, or scheduler
   configuration/staging/submission step becomes `FAILED` with a
   `failure_reason`. A terminal state written concurrently by an operator or
   watchdog remains sticky.
4. **Payload outcome.** A nonzero command return may consume retry budget and
   return the same spec to `PENDING`; after the budget is exhausted it becomes
   `FAILED`. A zero return becomes `COMPLETED`. vq does not inspect
   calculation output to promote or demote either outcome.
5. **Operator and resource termination.** Manual control writes `KILLED`.
   Watchdog evidence, scheduler wall-time evidence, or a scheduler-attributed
   abnormal accounting state (`OUT_OF_MEMORY`, `TIMEOUT`, `DEADLINE`; other
   scheduler-ended states record `ABORTED_BY_QUEUE`) uses `OOM_KILLED`,
   `STARVED`, or `TIME_EXCEEDED` as applicable, even when an exit-marker
   read 0 for the killed run.
6. **Recovery and infrastructure uncertainty.** Restart and scheduler
   reconciliation first use process liveness and durable exit-marker evidence.
   When vq cannot establish a normal outcome, it uses `ABORTED_BY_QUEUE`.
   `recover_on_reboot` may create a sibling JobSpec, but it does not reopen the
   original terminal record.

`FAILED` therefore covers several queue-owned failures as well as an exhausted
nonzero process return. `failure_reason`, `failure_tail`, `exit_code`, scheduler
diagnostics, and preserved logs/artifacts distinguish those cases. Configured
terminal notifications are side effects of specific transition paths, not a
separate failure class and not a substitute for the JobSpec.

### 4.2 Submission validation and materialization

**Classification: current architecture plus retired proposal.** A successful
submit is a synchronous pipeline owned by the host that will store the JobSpec:

1. The CLI and configuration models validate option types and ranges, source
   existence, mutually exclusive payload forms, host/branch resolution,
   timestamps, array/chain combinations, and remote or scheduler routing
   constraints.
2. The JobSpec-owning boundary validates the program name, local registry
   membership, configured or explicit runtime pins, scheduler-target runtime
   identity when required, and the QVF managed-runtime contract.
3. `depends_on` and `depends_on_any` references are deduplicated and checked
   against the submitter's own queue. Cross-user dependencies are not accepted.
4. vq creates the workspace, copies a single file or directory, or extracts a
   tar archive with Python's `data` filter, then resolves the stored command.
5. If explicitly requested, `--vibeqc-preflight` runs the command once with
   `VIBEQC_DRY_RUN=1` and may add output hints from one new or changed contained
   manifest. Timeout, nonzero exit, missing or malformed output, ambiguity, or
   bounded-discovery exhaustion is non-fatal and does not reject the submit.
6. Pydantic constructs the JobSpec, and vq atomically writes it in `PENDING`.
   The following `submitted` event is best-effort; the spec write is the
   authoritative acceptance boundary.

An update drain with `reject_submits = true` may reject a local receiving
submit before materialization. A submitter-side administrative-down mark may
also refuse routing to a named remote host. Ordinary resource requests are not
compared with daemon capacity as a universal acceptance rule: when capacity
metadata is available, an oversized local operator request is warned about and
still queued. The default stdout contract remains the bare accepted job ID.
`vq submit --json` carries the same local or forwarded remote warnings in its
additive `capacity_warnings` list so automation cannot lose the condition by
discarding stderr.

The proposed asynchronous adapter validation worker, validation timeout,
`PENDING_VALIDATION`, `QUEUED`, and `REJECTED` transitions never shipped. vq
does not parse chemistry input or contact a code license service during generic
submission.

### 4.3 Admission gates and dispatch checks

**Classification: current architecture plus retired proposal.** A durable
`PENDING` spec is considered in priority/FIFO order only after its current
admission gates permit it. Recoverable holds do not consume a dispatch-attempt
counter and do not change the lifecycle state.

Global or scoped holds include an active admin update, a changed configuration
that is missing or invalid, full or scheduler-target drain, host memory
pressure, and an in-progress managed-runtime build. Per-job gates include
`not_before`, dependency readiness, local process/CPU/effective-memory
capacity, scheduler `max_scheduler_jobs`, and multi-user job/CPU quotas.
Required predecessor failure is the exception that cascade-fails a dependent;
`depends_on_any` waits for terminal predecessors without propagating their
outcome.

Local and scheduler jobs use different admission budgets. Local jobs consume
the driver's process, CPU, and effective-memory capacity. Scheduler jobs skip
those local CPU/memory gates, carry their resource request into the batch
script, and consume the driver's scheduler-job cap. An ordinary local operator
job that exceeds configured CPU or memory capacity stays `PENDING` until the
daemon capacity changes; only an automatically generated build job with an
impossible request is failed immediately. The operator job is nevertheless
classified as over cap on read: `vq list` labels its pending row, queue and
status JSON expose `pending_over_capacity` plus structured
`configured_capacity_overages`, and `vq status` reports its queue ETA as
unavailable rather than applying dispatch-history timing to a job that cannot
currently dispatch. Missing capacity metadata is unknown, and scheduler-target
jobs never inherit the driver's local CPU or memory classification. The JSON
boolean is therefore tri-state: `true` means a known configured-cap overage,
`false` means an applicable pending local job is known to fit the advertised
base caps, and `null` means capacity is unavailable or the job is not an
applicable pending local job. `null` also preserves uncertainty when a
mixed-version snapshot constrains memory but predates advertisement of the
daemon's undeclared-memory charge. The structured overage list is empty for
both the `false` and `null` cases.

Immediately before launch, the daemon re-reads and claims the spec under its
per-spec lock. Common fatal checks cover the multi-user submitter/workspace/log
trust boundary and enforced program runtime pins. The local path additionally
checks leaked cgroup scope state, UID/GID privilege-drop requirements, creates
`VQ_WORKDIR`, and starts the wrapped process. The scheduler path resolves its
configured dialect and hooks, refuses a named Python payload with no launcher,
stages the workspace, submits with qsub or sbatch, and parses the scheduler job
identifier. A fatal check or launch error becomes `FAILED`; a concurrent sticky
terminal state is preserved.

This is declared-resource admission, not a live free-memory or generic scratch
placement probe. vq has no `scratch_gb`, `dispatch_attempts`, or
`max_dispatch_attempts` contract, and it does not ping license servers, validate
restart files, or run adapter staging. Poll, marker, and result-fetch failures
after a scheduler job has been accepted belong to scheduler reconciliation,
not this pre-dispatch gate.

### 4.4 Runtime supervision

**Classification: current architecture plus retired proposal.**

The daemon invokes one `Watchdog` for local children and reattached local
orphans. Every newly dispatched direct local command also runs through a POSIX
`wait4` collector that writes `<workspace>/_vq/resource-usage.json` before the
existing exit marker. The receipt exists even when the command finishes before
the watchdog's first five-second sample. It reports monotonic wall time, user
and system CPU and their active-CPU sum, `ru_maxrss`, and the propagated command
exit code. In a dedicated cgroup-v2 job scope it also reports `pids.peak` minus
the collector's pre-command task baseline; its recorded semantics state that
threads count as tasks. Without that kernel counter, `process_count` is null.
The receipt records the source and aggregation semantics for every metric.

Scheduler-targeted jobs are not sampled by the local watchdog. Live scheduler
wall-time evidence is handled by scheduler reconciliation, while the generated
PBS/SLURM script records terminal command usage in the same receipt path through
GNU Time. The executable defaults to `/usr/bin/time`; a scheduler host may set
the absolute, compute-node-visible `scheduler_gnu_time_command` path when the
site installs it elsewhere. The scheduler schema and behavior are unchanged:
it reports wall seconds, user and system CPU seconds, their active-CPU sum,
peak RSS, and the propagated command exit code. Its `status` describes
collection health, not command success. The scheduler script fails with exit
125 before launching the payload when the configured GNU Time executable or
the receipt formatter is unavailable or incompatible. The daemon calls the
watchdog on every loop, while metric sampling is rate-limited by the watchdog's
five-second default. A `SUSPENDED` job is not sampled or subject to a watchdog
kill until it resumes.

At terminal fetch, daemonless Slurm jobs also project bounded authoritative
`sacct` CPU time and MaxRSS into the established `_vq/samples.jsonl` schema
when no usable complete GNU Time receipt is available, including absent,
malformed, incomplete-array, non-finite, or overflowing receipts. Native-array
cardinality and task/step coverage must be complete before a sample is
authoritative. Complete positive CPU and MaxRSS evidence records `ok`; missing
or zero metrics record explicit `partial`; accounting lag, malformed or
nonzero output, and arithmetic overflow record explicit `unavailable`, with
redacted diagnostics. Compatible process-tree samples are never overwritten.
A completed scheduler sample makes repeated fetch a no-op; retryable partial
or unavailable scheduler records may be refreshed without appending duplicate
samples. Telemetry availability never delays the terminal lifecycle transition
or capacity release.

For each metric sample, vq first tries the named systemd cgroup scope, then the
cgroup of the recorded process, then a process-group walk, and finally the
single process when no process group is available. Available RSS and CPU-time
values are appended to `<workspace>/_vq/samples.jsonl`. Missing metrics are
best-effort and do not fail a job.

On a single-user host where the `systemd-run --user --scope` probe succeeds,
each local job runs in a named cgroup. `MemoryMax` and `MemoryHigh` enforce an
effective memory cap when one exists, while `CPUQuota` throttles the job to its
declared CPU count. Multi-user dispatch instead uses a mandatory system-mode
scope to drop to the submitter's uid and applies the same resource properties.
Without usable cgroup enforcement, single-user dispatch runs directly and the
watchdog uses sampled RSS for its per-job memory check. vq does not inspect
cgroup `memory.events`, so a `MemoryMax` termination is not by itself
classified as `OOM_KILLED`.

The local watchdog remains an enforcement component when cgroups are active.
It checks `wall_time_seconds` independently of the metric-sampling cadence,
subtracts recorded pause time, retains the sampled host-RSS ceiling, and
retains the CPU-starvation check. A watchdog limit first records `OOM_KILLED`,
`STARVED`, or `TIME_EXCEEDED` and sends `SIGTERM` to the process group. A
reattached local orphan receives a fresh monotonic wall-time anchor; Section
4.7 describes restart recovery.

A `vq kill` of a running job signals the same process group the same way, and
both kills owe it the same grace, 10 seconds at the current internal default,
before `SIGKILL`. Neither grace is keyed on the wrapper that leads the group.
That wrapper installs no signal handlers, so it dies to the `SIGTERM` at once
while a command that ignores `SIGTERM` keeps running. vq therefore keeps a
record of a terminal job whose process group still answers after it has reaped
that wrapper, charges the job against the host's job, cpu and memory budgets
for as long as it does, and sends `SIGCONT` and then `SIGKILL` to the group
once the grace expires. The job's terminal state and exit code are already
recorded and are never revised by this. A group that exits within the grace
releases its budget as soon as vq observes that, with no `SIGKILL`. Nothing
waits for the group to become empty, because unreaped zombie members answer a
liveness probe and a container's PID 1 need not reap them: the `SIGKILL` is
the last thing vq does for the job, and the budget is released with it. Across
a daemon restart the equivalent reap is the startup scan of Section 4.7.

A separate host-pressure pass reads Linux memory pressure. At the current
internal defaults it pauses local running jobs at 85 percent pressure, holds
new dispatch, and resumes only the jobs it paused after pressure falls below
70 percent. An unavailable pressure reading is a no-op. Operator-paused jobs
remain paused.

The collector leads the job's process group, forks the command into it, and
waits for that command before it writes the receipt and the exit marker. The
daemon ends the attempt when it reaps the collector. If the collector was
killed by a signal while the spec was still nonterminal, for example by
`kill -9` on the pid `vq status` shows or by an OOM kill, the command's outcome
can no longer be recorded. vq then sends `SIGCONT` and then `SIGKILL` to what
remains of the process group before it records `FAILED` or the retry, so the
command cannot run on with its capacity released, or beside its own retry in
the same workspace. A paused job's group is reaped the same way however its
collector exited, because nothing could continue it once the spec leaves
`SUSPENDED`; a spec that still carries a pause intent counts as paused. This
reap does not apply once `vq kill` or the watchdog has written a terminal
state: that group is owed the grace their `SIGTERM` opened, and is held and
escalated as described above instead. A group already reaped here as paused is
not also held for a grace that has just been spent.

A collector that exits normally has waited for its command, so on a host
without a job scope vq does not signal processes the command left running in
the background. Where a scope exists, vq stops it at the end of every attempt,
including one that is retried.

The watchdog does not update `JobSpec.last_heartbeat_at`. Local sample
staleness is derived from `_vq/samples.jsonl`; scheduler reconciliation uses
`last_heartbeat_at` for scheduler jobs. `vq top` reads the latest local sample,
and the fleet job detail view reads a bounded recent sample history. The sample
file has no independent retention timer; it follows the workspace's archive
and deletion lifecycle.

Both resource receipt variants follow the workspace fetch, archive, and
deletion lifecycle. Resubmit removes an inherited receipt so a retry cannot be
mistaken for the source run. An immediate SIGKILL, node loss, or storage failure
can prevent the wrapper from publishing it; consumers must reject a missing or
`status: "error"` receipt when process telemetry is required.

**Retired proposal.** There are no `watchdog_interval_seconds` or
`watchdog_grace_seconds` configuration keys, no independent configurable
seven-day sample retention, and no remaining unimplemented local time-limit
gap. The earlier local heartbeat-stamping design and systemd `RuntimeMaxSec`
enforcement did not ship in the current architecture.

### 4.5 Process outcome and artifact inspection

**Classification: current architecture plus retired proposal.** vq has no
code-specific post-flight hook. For a local child or scheduler wrapper, a
reported return code of zero becomes `COMPLETED`; a nonzero return code becomes
`FAILED` after retry policy is considered. If manual control, watchdog
enforcement, wall-time handling, or infrastructure recovery already wrote a
terminal state, that label remains sticky, although a later return code may be
recorded as forensic evidence.

Scheduler result copy-back and process classification are independent. Once a
durable scheduler exit marker provides the return code, bounded copy-back
failures may leave the terminal job marked
`scheduler_state = "artifacts_unavailable"` without discarding that code. QVF,
`.out`, `.system`, and structured-progress readers inspect preserved artifacts;
they do not change the queue state.

**Retired proposal.** The adapter `post_flight` hook, `PostFlightOK`,
`PostFlightFail`, `PostFlightDegraded`, ORCA normal-termination-marker gate,
`DONE` state, and adapter-attributed recovery classification never shipped.

### 4.6 Crash diagnostics

**Classification: partial current implementation plus retired proposal.** The
local command wrapper normally writes its return code to `_vq/exit-code`, and
the scheduler wrapper uses the same durable-marker role. This includes the
usual `128 + signal` value when a command dies from a signal. If a wrapper is
killed before writing its marker, recovery may have no return code. Status,
fetch, and notification summaries decode a portable subset of signal numbers;
an unknown platform-specific number remains a generic signal value.

On a non-`COMPLETED` terminal or reap path, the daemon best-effort reads only
the job's stderr file. It stores at most the last 20 lines from the trailing
approximately 4 KB as `JobSpec.failure_tail`. Missing, empty, binary, or
unreadable stderr never prevents the terminal spec write. `vq status` renders
the stored tail, and fetch metadata plus bulk-fetch results can expose a short
hint from it. A configured terminal notification names a recognized signal in
its summary, but its payload does not copy `failure_tail`.

Fetched workspaces or workdirs, and streamed workspace archives, receive a
generated `_vq/terminal-diagnosis.json` sidecar. It contains the JobSpec
outcome, failure fields, scheduler metadata, and a structured diagnosis. This
is fetch metadata, not a runtime crash report. The full stdout and stderr files
remain ordinary workspace artifacts and can be inspected or fetched through
the normal interfaces. vq does not synthesize a separate crash bundle.

Fetched workspaces and workdirs also receive `_vq/fetch-manifest.json`
(`vq.fetch-manifest.v1`), written by the receiving side. It records
`fetched_at`, `refresh_attempted_at`, the source host, source kind, source
path, and transport, plus `stale` and `refresh_error`. A fetch into a
destination that already holds a previous fetch of the same job replaces that
snapshot and advances `fetched_at`; a fetch that cannot refresh exits non-zero
and stamps `stale: true` with the failure onto the existing tree. A consumer
can therefore age-check a fetched tree without trusting the CLI's exit code.
Artifact-only fetches (`vq fetch --name`) write no sidecars by contract and so
carry no manifest.

**Retired proposal.** vq does not capture `siginfo`, a 256 KB stdout window,
`dmesg`, `kernel.core_pattern`, or a discovered core-file path. It does not
write `_vq/crash.json`, and no such record is mirrored into notifications. A
calculation that writes its own diagnostic or core artifact may leave it in the
preserved workspace, but that is outside the queue's crash-diagnostic contract.

### 4.7 Daemon restart recovery

**Classification: current architecture plus retired proposal.** At startup the
daemon scans persisted specs. `PENDING` specs remain pending. Specs that were
already terminal remain terminal; if a local process group still appears to
survive behind one, vq verifies its recorded process identity and attempts to
reap that leaked group with `SIGKILL`.

For a local `RUNNING` or `SUSPENDED` spec, the stored process group is the
first liveness boundary. A live group is accepted only after the recorded
process-start fingerprint matches when that evidence is available. When a
cgroup scope is active, a conflicting scope `MainPID` also prevents reattach.
An accepted process keeps its lifecycle state, is tracked as a reattached
orphan, and rejoins watchdog supervision. The watchdog starts a fresh
monotonic wall-time epoch because the prior in-memory clock cannot be
reconstructed; recorded pause totals are still subtracted.

When the local process group is gone, vq first reads the durable workspace exit
marker. A valid marker recovers the return code and applies normal retry or
`COMPLETED` or `FAILED` classification, subject to an already-sticky terminal
label. Without a valid marker, the job becomes `ABORTED_BY_QUEUE` with a
failure reason because its outcome is unknown. A missing recorded process
group follows the same abort path.

`--auto-resume` applies only when this startup pass moves a spec that entered
as `RUNNING` to `ABORTED_BY_QUEUE`. It creates a fresh `PENDING` sibling with a
new identifier, the same workspace, and `parent_jobid` pointing to the dead
job so application-level restart files can be reused. It does not resume a
spec that entered as `SUSPENDED` or one that was already terminal before this
startup.

Scheduler-targeted `RUNNING` and `SUSPENDED` specs use a separate recovery
path. The driver rebuilds a scheduler handle from persisted target and job
identity and resumes reconciliation; if the scheduler identifier was not
persisted before a driver failure, it may recover the identifier from the
remote job record. If configuration or identity is temporarily insufficient,
vq keeps the spec nonterminal, records
`scheduler_state = "reattach_failed"`, reserves scheduler capacity, and retries
later rather than declaring a possibly live batch job aborted.

**Retired proposal.** `STAGING` and `FINALIZING` are not JobSpec lifecycle
states. The daemon claims a scheduler spec from `PENDING` to `RUNNING` before
staging and submission; a fatal error there becomes `FAILED`. Marker, fetch,
and reattachment detail is represented by `scheduler_state` and diagnostic
fields. Startup does not translate hypothetical `STAGING` or `FINALIZING`
instances, and there is no pending future-state promise attached to this
recovery contract.

### 4.8 Submission idempotency

**Classification: opt-in CLI contract plus retired HTTP proposal.** An
unkeyed fresh submission retains the historical behavior: it mints a new
12-character identifier, and repeating the command creates another job.
Resubmit, daemon retry, rerun-until, and auto-resume likewise create a new
logical attempt and do not inherit a prior submit key.

For one single-job submission, `vq submit --idempotency-key KEY ...` binds the
authenticated owner and key to a canonical intent at the queue authority. The
authority is scoped by the exact execution state store, authenticated owner,
and key hash. The canonical intent covers the scheduler target, program and
runtime pin, immutable payload digest, name, argv, resources, dependencies,
tags, and execution flags. The same owner, store, key, and intent return the
original job ID. Reusing the key for different intent is a hard conflict.
Arrays and chains do not yet accept this option.

The queue publishes the spec first and an immutable owner-scoped claim second,
under a per-key lock. A retry repairs a crash between those writes by scanning
bounded, no-follow spec records. The claim survives ordinary job deletion, so
deleting a job does not make its key reusable. Remote submission forwards the
same key once; ambiguous timeout, transport loss, or signal outcomes are not
replayed automatically and preserve staging for reconciliation.

The current web application has controls for existing jobs but no route for a
fresh HTTP submission.

**Retired proposal.** The fresh-submit HTTP API and its mandatory idempotency
key contract never shipped.

### 4.9 Backpressure

**Classification: current multi-user active-work gate plus retired submission
proposal.** Fresh submission does not inspect queue depth or quota configuration.
There is no single-user default of 1000, no submit-time refusal, and no
429-equivalent CLI or HTTP response. An accepted job receives its workspace and
durable `PENDING` spec even when it will be held by a later dispatch gate.

In multi-user mode only, `quotas.default_max_pending_jobs` and per-UID overrides
may be configured; their default is unlimited. The daemon currently treats the
value as a dispatch-time active-holder cap rather than a count of accepted
pending specs. Reaching it leaves another job `PENDING` and does not reject or
delete that job.

The active-holder projection is owner-qualified and deduplicated by owner and
job ID. It includes tracked local children, tracked scheduler jobs across later
ticks, reattached local orphans, `SUSPENDED` records that have no separate live
handle, and scheduler records whose durable identity is still reserving a
deferred reattachment. A job represented both in memory and as `SUSPENDED` on
disk is charged once. Waiting `PENDING` records are candidates, not holders.
Successful local or scheduler starts increment the same per-owner totals for
the remainder of that tick.

`default_max_concurrent_cpus` and its per-UID overrides use exactly the same
holder set, summing each holder's declared `cpus`. These quotas control daemon
dispatch only. They are not a durable admission count and do not make accepted
queue depth bounded.

**Retired proposal.** The global queue-depth threshold, default-1000 policy,
and submit-time 429 behavior never shipped. A future admission-backpressure
contract would need atomic workspace/spec refusal and an explicit policy for
already-admitted jobs; it is separate from the current active-work dispatch
quota.

### 4.10 Managed self-update and legacy rollout recovery

**Classification: current operator lifecycle.** `vq self-update` is the
first-class repair path for the managed vq environment that owns the running
user daemon. It accepts exactly one immutable selector: a full 40-hex source
SHA or an explicitly named accepted fleet report. The selected report is
validated by the same release-evidence contract used by fleet rollout. The
command has no host, force, environment, or restart-suppression option.

Self-update and fleet rollout share one global operation lock. Self-update
also uses the existing admin update marker, scoped queue pause/resume,
managed-script install and rollback, bounded daemon restart, and exact
source/tree provenance verification. The outer admin transaction owns daemon
restart timing. A managed update script accepts that coordination only with
inherited, validated file descriptors for the exact checkout and virtualenv
lifecycle locks, bound to the invoking parent and target resources. The child
also independently proves the serving daemon is stopped before any checkout or
virtualenv mutation; a parent-PID environment value alone conveys no
capability. Branch-mode auto-update rejects the running daemon's own
environment before submitting a build job because a mutable branch target
cannot satisfy this exact-restart contract.

The managed marker is also the crash-recovery receipt. Pause intent is durable
before SIGSTOP, the exact old checkout and virtualenv remain available until
terminal proof, and the marker's resource scope continues to hold dispatch if
the updater disappears. The daemon auto-reaps only ordinary stale markers; it
retains a managed-daemon or pause receipt. `vq admin recover-update` reconciles
the exact durable phase: it restores and verifies the pre-update checkout,
virtualenv, and daemon unless the receipt already records a committed target,
in which case it re-attests that exact target and completes cleanup. It never
promotes an unverified target by inference. Recovery then resumes and proves
only the receipt's pause-token scope before clearing the marker. A live updater
or malformed, changed, or ambiguous receipt remains blocked. The ordinary
clear and force interfaces cannot discard a managed receipt. A
pause-only receipt may use `clear-update-marker`, but that command must prove
the exact pause scope resumed before its terminal unlink.

A direct host whose installed vq predates a required recovery-parser fix may
use `recover-update --marker-id ID --with-driver-runtime`. The current driver
stages its exact package under the host's `~/vqscratch`, verifies the uploaded
archive digest, and invokes the configured absolute old `remote_vq` once with
that package first on its import path. The staged process must verify that its
loader names the exact archive before it observes recovery state. The command
does not install the package, requires an exact marker ID, never retries the
mutation, and retains the stage after an unknown SSH outcome. It is not valid
for scheduler-driver receipts or orphan quarantine.

An exceptional terminal receipt can outlive every checkout, virtualenv, and
backup asset needed by ordinary recovery.  The only supported escape hatch is
the explicit `recover-update --quarantine-orphaned-receipt` mode.  It admits
exactly one schema-v1 `target_committed` receipt with `backup_moved=false`, a
stopped writer, no surviving receipt assets, no paused jobs in the receipt's
token scope, a secure matching admin-status row, and an independently attested
current runtime at the operator-supplied full source SHA.  Admission binds the
physical state and lock directory inodes, serializes every marker writer, and
rechecks the original marker inode and bytes immediately before its sole
terminal move.  The marker is moved into an owner-only quarantine containing
a hash-bound status record and recovery receipt; quarantine evidence is never
deleted automatically.  Dry-run executes the same observation and validation
contract without moving the marker or changing jobs, services, update assets,
or pause state.  Accounting lag, an unreadable or mutable status file, a live
writer, a surviving asset, a nonempty pause scope, runtime disagreement, or
partial evidence that cannot be authenticated remains an unresolved fence.
This path proves that one obsolete fence can be retained safely; it does not
authorize a rollout, daemon restart, job submission, or fleet recovery.

The ownership lock and marker-registry lock are owner-only, no-follow inode
locks.  A transaction binds their physical namespace once; re-entry is limited
to the same process, thread, and binding.  Forked children discard inherited
lock descriptors and local ownership depth.  All marker rewrites, including
low-level heartbeat writes, take the marker-registry lock so quarantine cannot
race an otherwise cooperating writer.

Pre-recorder rollout journals can contain an action saved as `running` without
a durable operation receipt. All ordinary rollout modes fail closed on that
state. An operator may explicitly request `--reconcile-legacy` only as an
unscoped recovery from the configured driver. Under the global lock, vq first
authenticates every journal's exact historical report digest, deterministic
rollout ID, original argv, hold shape, and absence of a durable orphan before
it observes or changes any legacy claim. A healthy current same-lane LAST OK
proof permits the historical action to be recorded as `superseded` with
unknown observed outcome, never as successful, failed, or not run. Missing,
unreachable, blocked, marker-overlapped, or otherwise unproven current lanes
are retained instead. Their historical action and hold subtrees remain
byte-identical and strict top-level receipts bind their hashes to the current
accepted report. If one action in a historical rollout is retained, every
pre-recorder action and active owned hold in that rollout is retained as one
group.

Legacy holds are reconciled independently. This includes an obsolete
pre-owner `scheduler-target` claim even when its historical actions are all
terminal. Closed-shape obsolete full holds are first inventoried without host
controls so one unreachable host cannot abort global admission before another
host becomes eligible. Every claim must bind its committed historical report;
a scheduler claim also requires a unique canonical target/control relationship
in the current plan. A provenance-bound read-only snapshot with complete legacy
and lease coverage can settle an inactive target in the journal without
issuing a release. Each authoritative host observation is followed by its own
atomic journal write, after the current accepted-report identity is rechecked.
A committed settlement is not rolled back if a later host is unavailable or
the controller exits, and restart never probes released history again.
When a pending terminal operation failure keeps the global durable pass from
releasing an obsolete exact full hold, explicit recovery still inventories
that hold for host-local observation. After an authoritative inactive
snapshot, recovery atomically writes the current report journal's exact
three-field no-launch action acknowledgements, failed-host reason, and closed
forward intent before altering any historical journal. Each historical source
then receives its explicit released full-hold row and exact action backlinks;
only after every backlink is durable may the existing verified consumer mark
the corresponding harvested failure fences handled with retry authorization
still false. The current intent is cleared last. Restart reuses the persisted
plan projection and receipt context rather than rebuilding launch authority,
and no recovery invocation deploys or retries the acknowledged update.

Every such mutation refreshes the accepted report before the final supported
hold-status observation. With that status as the last external callback, vq
compares a non-fetching local `origin/main` and report epoch, then validates
the complete journal version vector immediately before authorization and the
single atomic write. A remote report published after the fetched epoch is
handled by the next invocation. An active or unavailable hold remains retained
under the existing exact release-authority rules.

An active legacy target is conditionally released only when its live reason
and `set_at` match, the current scheduler lane is a healthy LAST OK skip, and
no admin-update marker overlaps it; the release is `--release-legacy-only` and
never removes owner-scoped leases. Exit zero is not release proof: vq repeats
the supported authoritative snapshot and settles the journal only after the
exact legacy component is absent. Pairless full holds use the same rule with
their exact reason/`set_at` identity only when an authenticated same-host
action supersession and a fresh healthy current same-lane proof authorize that
release. A sibling host's proof never
authorizes live release; an independently inventoried full hold that is active,
unavailable, or still active after release remains retained with a coherent
receipt. Unreachable, incomplete, externally owned, or mismatched claims remain
retained and untouched. Missing or ambiguous plan bindings and malformed
historical evidence fail before the first mutation. None of these cases justify
a broad or pairless release.

`--reconcile-legacy` is recovery-only and exits before any rollout action.
Later unscoped invocations, and every selected host in a scoped invocation,
accept only a complete, untampered retention receipt bound to the same current
report. A scoped invocation may carry an exact receipt from an older accepted
report only for a configured host it explicitly excludes. The fleet-global
preflight still validates every receipt's closed shape and historical hashes;
the receipt exception itself neither reauthenticates nor authorizes legacy
recovery or release control on the excluded host. Ordinary read-only fleet
snapshots may still probe it. The receipt remains an explicit deferred legacy
fence while the selected hosts are planned. Unknown hosts, malformed receipts,
selected-host mismatches, and unscoped mismatches remain hard failures before
planning. A retained driver is a hard block. The operator must next run
ordinary report-pinned `--dry-run`, then invoke the ordinary rollout separately.
Unrelated hosts may proceed while all retained hosts remain fenced and visible
in JSON and text output.

---

## 5. Resource management

### 5.1 The resource budget

**Classification: current declared-resource admission plus retired proposal.**
Local dispatch is bounded by `max_jobs`, `max_cpus`, and `max_mem_mb`.
`max_cpus` defaults to the detected logical CPU count. When `max_mem_mb` is
omitted, vq uses the detected host total when available; an unavailable total
leaves that aggregate gate unset. A job with no `mem_mb` declaration reserves
`default_job_mem_mb` when configured and otherwise contributes zero to the
aggregate memory tally.

The daemon reserves declared resources for tracked local children and
reattached local orphans. `SUSPENDED` work keeps its reservation because it can
resume into the same process tree. Candidates are ordered by descending
priority and then submission time. A candidate that does not fit is skipped,
so a smaller later candidate can dispatch without changing the blocked job's
durable `PENDING` state. Section 4.3 describes impossible generated-build jobs;
ordinary operator jobs that do not currently fit remain pending.

The configured-cap classification compares the request only with the daemon's
advertised base `max_cpus` and `max_mem_mb`. Effective memory is explicit
`mem_mb`, or `default_job_mem_mb` for an undeclared request. Current usage,
temporary drain reductions, live free RAM, and per-user quotas remain ordinary
admission blockers rather than configured-cap impossibility. `vq overview`
keeps the compatible pending count and adds `over_capacity_pending_jobs`; its
text form renders a host-level alarm when that count is nonzero. The JSON
count is `null` when either the queue census or configured-cap snapshot is
unavailable, and is zero only after both inputs were read successfully.

Scheduler-targeted jobs do not consume the driver's local CPU, memory, or job
budget. They use the separate `max_scheduler_jobs` count, including a live
reattachment reservation when scheduler identity is known but tracking is
temporarily unavailable. Drain policy and host pressure can further restrict
dispatch. The multi-user per-owner active-work quota is a separate gate
described in Section 4.9.

These totals are admission reservations, not measurements of actual use.
Section 4.4 owns cgroup and watchdog enforcement. An undeclared memory request
can be uncharged when no default is configured, and CPU enforcement depends on
the cgroup mode available on that host.

**Retired proposal.** vq has no `total_gpus`, `total_scratch_gb`, `spec.gpus`,
or scratch-capacity allocator. The earlier formulas and cross-reference to a
future GPU budget did not ship.

### 5.2 cgroups v2 strategy

**Classification: current architecture plus retired claims.** vq does not
infer cgroup support from kernel or systemd version numbers. In single-user
mode it runs a bounded `systemd-run --user --scope --collect` probe with a
small `MemoryMax`. A successful probe enables named per-job scopes. A failed
probe leaves the daemon usable: local jobs run directly and the watchdog uses
its process and process-group sampling fallback. Delegation setup and the
operator probe are documented in `docs/cgroup-setup.md`.

A single-user scope applies `MemoryMax`, `MemoryHigh` at 90 percent of the hard
limit, and `CPUQuota` from the declared CPU count. Multi-user dispatch is a
different, mandatory system-mode scope that drops to the trusted submitter uid
and gid and applies the same resource properties. Multi-user dispatch fails
closed if that scope cannot be created. Before it forks the submitted command,
the direct resource collector also requires its own cgroup-v2 path to end in
the exact named `vq-job-<id>.scope`. Missing cgroup evidence or inheritance of
the root daemon's service cgroup exits 125 with `command_status=not_run`; a
declared `VQ_MEM_MB` never overrides that kernel placement or its effective
ancestor limits. Scheduler-targeted jobs do not use the driver's local scope
wrapper; scheduler directives and site policy own their resource enforcement.

The watchdog remains active in cgroup mode for pause-aware wall time, host
memory pressure, starvation, and metrics. vq does not currently inspect
cgroup `memory.events`, so a kernel `MemoryMax` termination is not by itself
classified as `OOM_KILLED`.

**Retired claims.** There are no probed minimum kernel or systemd versions, no
`TasksMax` or `RuntimeMaxSec` property in the wrapper, and no claim that a
scheduler backend uses the same local systemd mechanism.

### 5.3 Scratch and disk

**Classification: current managed workdirs and scheduler scratch plus retired
proposal.** A local dispatch receives one deterministic workdir at
`<state-root>/workdirs/<jobid>` in single-user mode or
`<system-root>/users/<uid>/workdirs/<jobid>` in multi-user mode. vq exports
that absolute path as `VQ_WORKDIR`; it is separate from the submitted workspace
used as the command's current directory.

Immediate terminal cleanup is opt-in through the job's clean-workdir setting.
In multi-user mode the root daemon never deletes the mutable `spec.workdir`
target. It retains the trusted owner and owner-qualified spec path at admission,
derives the sole permissible managed owner/job workdir, requires the recorded
path to match it exactly, and performs descriptor-relative no-follow checks.
Path mismatch, traversal, symlink, non-directory, and metadata failures skip
deletion and append an operator-visible diagnostic without changing the
terminal state. Cleanup occurs only after the local child or reattached orphan
has actually exited. Scheduler jobs legitimately have no local workdir and are
a no-op on this path.

The separate age-based cleanup pass enumerates managed roots. It is opt-in and
disabled by default; 14 days is operational guidance rather than an implicit
retention promise.

For a scheduler target, the configured absolute `scratch_root` is the shared
staged workspace. An optional trusted `node_scratch_dir` template makes the
batch script create a node-local temporary directory, seed it from the shared
workspace, and run there. On normal fallthrough it attempts to copy files back,
writes the shared exit marker, and removes the temporary directory. Copy errors
are suppressed by the wrapper. `TERM`, `INT`, or `HUP` traps write the shared
marker but bypass copy-back and scratch cleanup, so relative calculation
artifacts may be unavailable.

JobSpec persistence uses a unique same-directory temporary file, flush and
`fsync`, `os.replace`, and a best-effort parent-directory `fsync`. This atomic
contract applies to the spec file, not to arbitrary payload output or every
queue state file.

**Retired proposal.** vq has no configured list of capacity-ranked scratch
volumes, free-space allocator, adapter-based `PYSCF_TMPDIR` or ORCA scratch
injection, unconditional cleanup promise, or watchdog/dmesg ENOSPC classifier.
An application's own disk-full text can appear only through ordinary retained
stderr and failure-tail handling.

### 5.4 GPU scheduling (retired proposal)

**Classification: retired proposal.** GPU scheduling did not ship. `JobSpec`
has no `gpus` field, the daemon has no device inventory or allocator, and vq
does not set `CUDA_VISIBLE_DEVICES`, model MIG devices, or attach NVIDIA device
policy to its cgroup scopes. A payload can use GPUs made visible by its launch
environment, but vq does not reserve or isolate them. Any future GPU contract
requires a separately reviewed schema, compatibility, placement, and
enforcement design.

---

## 6. Notifications and observability

### 6.1 Per-job event timeline

**Classification: mixed.** The current per-workspace JSONL timeline is
described first. The proposal that it be the canonical observability and audit
backbone is retired below.

**Current architecture.** Since v0.4, ordinary lifecycle paths append
structured JSON records to `<workspace>/_vq/events.jsonl`. A writer-created
record contains `ts`, `kind`, and `jobid`, plus kind-specific fields. The
current event kinds are `submitted`, `dispatched`, `state_transition`,
`kill_requested`, and `watchdog_kill`. Ordinary submission, local and scheduler
dispatch, state changes, retries, kill, pause and resume, throttle, and watchdog
paths use these records.

```json
{"ts": "2026-05-09T19:42:01.234000+00:00", "kind": "state_transition", "jobid": "1a2b3c4d5e6f", "from": "running", "to": "oom_killed", "reason": "watchdog: rss exceeded", "evidence": {"rss_mb": 130000, "limit_mb": 122880}}
```

The file is appended rather than rewritten, but event writes are best-effort.
An append failure is logged and swallowed, a missing file reads as an empty
timeline, and malformed JSON lines are skipped. The JobSpec remains the
authoritative mutable state. A spec write and its event append are separate
operations, and exceptional paths may intentionally update a trusted spec
without touching an unsafe or unavailable workspace. Do not reconstruct
authoritative state from the final event or treat the timeline as a complete
transaction journal.

Current readers include `vq events JOBID`, its `--json` form, single-host and
fleet web job detail, and a limited scheduler diagnostic in `vq status`.
Scheduler-job events live in the driver-owned workspace. The versioned
single-host job-detail API returns the JobSpec-derived payload, not an event
tail.

**Retired proposal.** There is no global `events/<jobid>.jsonl` tree,
event-based Prometheus exporter, notification-delivery event stream, fetch
event kind, or guarantee that every mutation is mirrored into this file.
Webhook failures are logged and dropped. A successful terminal workspace fetch
records `last_fetched_at` in the authoritative JobSpec after the fetched tree
has been fully received and published. Local fetch binds the stamp to the
initial spec identity. An SSH-backed fetch derives the exact `submitted_at` and
terminal state from the staged diagnosis plus its non-stale workspace manifest,
then the hidden remote acknowledgement reauthorizes and re-reads that identity
under the spec lock before writing. Missing or invalid workspace protocol
metadata, and failed or ambiguous acknowledgement, make the command nonzero but
do not mark the already published tree stale;
`fetch-all` may replay the idempotent acknowledgement only from that exact
receipt. Remote workdir and artifact-only fetches do not use this workspace
mark-back protocol. A legacy raw spec without a persisted `submitted_at` cannot
be identity-bound and therefore fails closed rather than receiving an inferred
stamp. Any future durable audit, delivery, or metrics backbone requires a
separately reviewed persistence and compatibility design.

### 6.2 Terminal webhook notifications

**Classification: mixed.** The shipped daemon-wide webhook is described first.
The original generic, per-job, multi-channel delivery framework is retired
below.

**Current architecture.** Webhook notifications shipped in v0.5.35 and are
optional and disabled by default. Each daemon host may configure one destination
and an optional terminal-state filter:

```toml
[notifications]
webhook_url = "https://hooks.example.invalid/vq"
notify_on_states = ["failed", "oom_killed", "starved",
                    "time_exceeded", "killed", "aborted_by_queue"]
```

An absent or empty `webhook_url` disables notification network activity. An
empty or absent `notify_on_states` list applies no state filter. Configured state
names are normalized to lower case and deduplicated; unknown or nonterminal
states make configuration loading fail. These settings are daemon-wide and
there are no notification fields or per-job overrides in JobSpec. The running
daemon copies the values at startup. The current config-reload path does not
replace them, so a notification-config change requires a daemon restart.

The POST body is derived from the terminal JobSpec, not from an event-log
record. It contains equal `text` and `content` summaries plus a structured
`job` object with the job ID, name, state, exit code, failure reason,
timestamps, command, and daemon hostname. This shape is intended for Slack,
Discord, and Mattermost incoming webhooks and custom HTTP receivers. Microsoft
Teams is not supported by the generic payload.

Notification is a side effect of explicit daemon terminal paths rather than a
centralized lifecycle guarantee. Current call sites cover normal local and
scheduler reaping, orphan recovery and queue abort, scheduler wall-time
attribution, dependency cascade failure, impossible generated build jobs, and
refresh failure. A retry that returns to `PENDING` does not notify. Direct
`PENDING` kill, dispatch-time failure, and some scheduler reconciliation paths
can produce a terminal JobSpec without invoking the notifier. An empty state
filter therefore means no filtering among invoked notification paths, not that
every terminal transition is covered.

Production delivery is fire-and-forget on a daemon thread with a five-second
HTTP timeout. URL errors, unexpected exceptions, and HTTP status values of 400
or greater are logged at warning level and dropped. There is no retry, buffer,
persistent delivery record, event-log outcome, dead-letter queue, or delivery
receipt. A webhook failure cannot roll back or block the already-written
terminal JobSpec.

**Retired proposal.** vq has no notification-channel registry, general event
filter, per-job target or opt-out, email or SMTP backend, web notification feed,
`/notifications` route, TCP/IP message channel, desktop notifier, channel
plugin, retry policy, or dead-letter store. Generic webhook payload
compatibility does not create dedicated Slack, Discord, or Mattermost
integrations. Any future channel or durable-delivery contract requires a
separately reviewed configuration, security, persistence, and compatibility
design.

### 6.3 Metrics

**Classification: mixed.** vq ships file-backed resource observations and
structured operator snapshots. The original Prometheus endpoint, named metric
set, and Grafana dashboard are retired proposals.

**Current architecture.** Section 4.4 defines per-job resource collection. The
local daemon watchdog samples locally supervised jobs at a five-second default
and appends best-effort JSON records to `<workspace>/_vq/samples.jsonl`. Each
record carries `ts`, `elapsed_seconds`, `rss_mb`, `cpu_percent`,
`cpu_time_seconds`, `cpu_time_source`, `cgroup_lookup`, `cgroup_path`,
`sample_pid`, and `sample_pgid`; unavailable readings may be null. Direct jobs
also write one terminal `<workspace>/_vq/resource-usage.json` receipt with wall
time, active CPU, peak RSS, optional cgroup peak process count, command outcome,
and explicit source and aggregation semantics. Scheduler-targeted jobs are
outside the local watchdog and retain their GNU-time terminal receipt at the
same path. Both files follow the workspace retention lifecycle.

Operator surfaces consume these records without turning them into a
time-series service. `vq top [HOST]` reads the latest sample for each `RUNNING`
spec, derives memory and active-wall percentages from declared limits, flags
samples older than 30 seconds, and offers text, `--watch`, and `--json` forms.
Fleet job detail reads at most the last 60 well-formed sample records. Missing,
malformed, unreadable, or unavailable sample data yields absent readings or an
empty sample list rather than failing the job or page.

Queue and accounting snapshots are separate on-demand surfaces.
`vq overview [HOST] --json` and its fleet form expose per-host queue counts,
recent terminal counts, confirmed-running and capacity-reserved pending CPU
totals, daemon health, and capacity data. On scheduler hosts, only an exact
last scheduler phase of `running` is confirmed execution. Failure, fence,
finishing, reattachment, and unknown phases remain in the compatible pending
totals; `unconfirmed_scheduler_jobs` and `unconfirmed_scheduler_cpus` expose
that subset and suppress idle inference. The subset CPU field is informational
and MUST NOT be added to `pending_cpus` again. Overview also reports
`over_capacity_pending_jobs` for pending local specs that exceed the configured
base caps. `GET /api/v1/queue` returns a
JobSpec-derived cockpit summary
with total, active, pending, state-count, active-CPU, and declared-memory
values. `vq usage [HOST] --json` groups retained-job wall and CPU-hour estimates
by tag, submitter, host, or total. Its CPU amount is runtime multiplied by
declared CPU count, with scheduler-reported wall time preferred when available,
so it is accounting data rather than sampled CPU consumption.

Scheduler deployment scripts may also emit `VQ-DEPLOY-METRIC key=value`
transcript lines. vq parses those values into the canonical scheduler-runtime
record and carries them into rollout JSON as per-attempt deployment evidence
such as cache decisions, compiler-cache hit rate, phase durations, and
native-rebuild status. They are not runtime counters and are not exposed
through a scrape endpoint.

**Retired proposal.** The web application has no Prometheus `/metrics` route,
metric registry, scraper configuration, or exporter. None of `vq_queue_depth`,
`vq_running_jobs`, `vq_watchdog_kills_total`, `vq_dispatch_latency_seconds`,
`vq_job_duration_seconds`, `vq_daemon_up`, or `vq_resource_utilization` is
implemented. vq publishes no counter or histogram series, adapter-labelled
series, or CPU, memory, and scratch utilization gauge, and it ships no Grafana
dashboard JSON. The health endpoints and JSON commands above are snapshot
interfaces, not Prometheus metrics. Any future scrape contract requires a
separately reviewed exposure, authentication, label-cardinality, aggregation,
retention, and scheduler-telemetry design.

### 6.4 Tracing

**Classification: mixed.** vq has coarse per-job timestamps and resource
timing, but the proposed OpenTelemetry exporter and span seams did not ship and
are retired.

**Current architecture.** There is no distributed tracing subsystem. JobSpec
records lifecycle timestamps such as `submitted_at`, `started_at`,
`finished_at`, `last_heartbeat_at`, and `last_fetched_at`. `vq status --json`
derives wall and pause-adjusted active elapsed time from those fields. The
best-effort event timeline in section 6.1 timestamps its five event kinds,
while the resource surfaces in section 6.3 record watchdog elapsed time,
scheduler command wall and CPU time, and deployment phase metrics. These
records are independent observations, not spans: they carry no trace or span
ID, parent relationship, propagated context, sampling decision, or exporter
outcome.

Submission validation, the optional vibe-qc dry-run preflight, scheduler
staging, local or scheduler dispatch, watchdog evaluation, terminal
bookkeeping, and result fetch are ordinary direct calls. The local Dispatcher
protocol and scheduler dispatcher are execution boundaries only; neither
exposes an instrumentation hook or carries trace context. There is no generic
post-flight phase. Ordinary logs can name a job or operation, but they are not
correlated into a cross-process or cross-host trace. The web setting
`log_level = "trace"` selects Uvicorn verbosity only.

**Retired proposal.** vq has no OpenTelemetry dependency, tracing
configuration section, SDK or provider setup, exporter, collector integration,
or spans named `submit`, `validate`, `stage`, `dispatch`, `watchdog sample`,
`post-flight`, or `fetch`. Because top-level configuration rejects unknown
fields, tracing cannot currently be enabled by configuration. The May 2026
roadmap snapshot's v0.8 OpenTelemetry item in section 13 is already classified
as retired. Any future tracing work requires a separately reviewed design for
dependency footprint, attribute redaction, trace-context propagation across
the CLI, daemon, SSH, and scheduler job scripts, sampling, exporter failure
isolation, and compatibility.

### 6.5 Health endpoints

**Classification: mixed.** The two lightweight web-console endpoints are
current. The proposed daemon-owned liveness endpoint, expanded readiness
contract, and deep self-test are retired.

**Current architecture.** The optional FastAPI web console registers two
unauthenticated GET routes in both single-host and fleet modes. These are HTTP
checks for the web process and its local view of vq state. The daemon itself
serves Unix-socket RPC rather than HTTP.

- `GET /health/live` returns `ok` with HTTP 200 whenever the web application can
  answer. It proves web-console process liveness only; it does not prove that
  the vq daemon is running or responsive.
- `GET /health/ready` returns `ok` with HTTP 200 when the daemon pidfile for the
  detected single-user or multi-user mode points at a live PID and the selected
  queue root exists. It returns a generic HTTP 503 otherwise.

The daemon check is the pidfile-based `os.kill(pid, 0)` probe. It does not make
an RPC call, verify daemon process identity, confirm a recent queue poll, or
test dispatch progress. It also does not inspect scratch capacity, cgroup
delegation, configured resource limits, or scheduler health. Operators that
need proof that the daemon is responsive use `vq daemon ping`; the heavier
`vq daemon health` checks the broader lifecycle contract.

Both routes bypass the optional fleet-console login guard so supervisors and
uptime monitors can call them without a session. The console binds to loopback
by default, but the health routes are exposed wherever the operator binds or
reverse-proxies the console. Their responses contain no job data, tokens, or
filesystem paths.

**Retired proposal.** There is no `GET /health/deep`, no dispatching no-op
self-test, and no self-test cleanup contract. Readiness does not use
`poll_interval`, a last-poll timestamp, scratch free space, or cgroup checks.
There is no daemon-owned HTTP `/health/live` endpoint. Any future deep or
side-effecting health check requires a separately reviewed timeout, cleanup,
authentication, information-disclosure, and load policy.

---

## 7. Multi-user, security, and authorization

### 7.1 Threat model

**Classification: mixed.** The cooperative research-group and non-public
deployment assumptions remain current. The old claim that multi-user mode
only prevents accidental interference understates the shipped uid/gid
execution boundary. The old semi-trusted web-user privacy model is retired.

**Current trust boundaries:**

- **Single-user local mode.** One OS account owns the daemon, queue, workspaces,
  and clients. Ownership checks are deliberately no-ops in this mode. vq does
  not isolate processes that already share that account.
- **System multi-user mode.** The root daemon treats every user-writable queue
  spec as untrusted. The numeric state-directory name is the authoritative uid;
  the spec's submitter must match it. Job ids are safe path components and the
  inner id must match the queue filename. The daemon owns each structural
  `users/<uid>` directory as `root:<primary-gid>` mode `0750`; its writable
  `queue`, `jobs`, `archive`, and `workdirs` children remain owned by the target
  uid/gid, retain existing read/execute bits, and lose group/world write bits.
  The state/control root is real and daemon-owned; its documented
  `root:vq-admins` mode `2775` makes the trusted admin group part of this
  boundary. Its `users` child is real, daemon-owned, and not group/world
  writable. Startup migrates existing numeric trees to that structure and
  fails closed on managed symlinks, non-directories, non-canonical uid names,
  or ownership it cannot establish.
  A daemon whose effective uid is root starts only after the complete
  configuration validates and explicitly sets `[multi_user] enabled = true`.
  Missing, disabled, or malformed policy refuses before client or daemon
  logging, pidfile creation, web-sidecar startup, or `Daemon` construction.
  Single-user execution is never a root fallback.
  Local jobs must cross the mandatory `systemd-run --uid/--gid` privilege drop;
  there is no run-as-root fallback. Quotas and owner/admin checks constrain vq
  operations, but do not stop a shell user from consuming resources outside vq.
- **Privileged control.** The root-owned installation, system configuration,
  admin group, and administrators holding the shared admin bearer token are
  trusted. The daemon RPC socket is owner-only (`0600`) in single-user mode and
  admin-group accessible (`0660`) in multi-user mode. Multi-user RPC writes
  require the admin token. Linux peer uid is audit metadata, not a per-user RPC
  identity or authorization principal.
- **SSH and schedulers.** SSH aliases, host keys, the configured remote account,
  scheduler commands, site prologues, and scheduler hooks are trusted. A
  scheduler job runs as the account selected by that SSH/site configuration;
  vq does not propagate the local submitter uid into a second remote identity or
  add an isolation boundary inside that account.
- **Web and API.** These are operator consoles, not per-queue-user portals. A
  plain single-host sidecar leaves reads open and gates mutations with the
  shared bearer token. Fleet mode can add local accounts with viewer, operator,
  and admin roles; a viewer can read fleet-wide job data, and the bearer token
  is admin-equivalent. With no fleet accounts configured, authentication is
  off and fleet writes fail closed. The HTTP API reads jobs and supports the
  documented control mutations; it does not submit/upload jobs or implement
  the CLI wait protocol.
- **Network.** Loopback is the default. A non-loopback bind is allowed after a
  warning or explicit acknowledgement; it is not prohibited by an invariant.
  TLS, reverse-proxy authentication, and network ACLs are operator duties.
  Health routes remain unauthenticated wherever the console is exposed.

**Path and artifact limits.** Admission checks declared local paths against the
owner's real `jobs` directory. Provisioning refuses managed symlinks and wrong
file types. Terminal workdir deletion uses descriptor-relative no-follow checks,
and fetched scheduler archives use safe extraction. Incremental reads from a
live scheduler workspace intentionally retain that remote account's symlink
trust model. Local dispatch still validates paths and then uses ordinary
pathnames for workspace creation, log opening, and recursive chown. That
resolve-then-use sequence is not race-free confinement against a hostile tenant.
Initial tree migration is likewise pathname-based and relies on the validated
daemon-owned state/control root, its trusted admin group, the non-writable
`users` root, and trusted operator-selected ancestors; it is not
descriptor-relative, race-free traversal of an arbitrary filesystem.
Directory modes and shared primary groups can also expose state bytes, so vq
does not promise cross-user result confidentiality.

**Retired assumptions.** Web accounts are not quota principals, web roles are
not queue ownership, and there are no per-user web tokens, OIDC, or PAM identity
bindings in the current implementation. Public-interface refusal is also not a
current invariant.

**Out of scope.** vq is not a public SaaS boundary, identity provider, container
sandbox, or hostile-tenant security monitor. Sites that need adversarial
multi-tenancy or distinct remote users must supply OS accounts, private groups
and modes, per-user SSH/scheduler identities, containers or VMs, and site
policy. vq coordinates those mechanisms; it does not replace them.

### 7.2 Authentication evolution

**Classification: mixed.** The shared bearer-token path shipped, but the
projected per-user bearer-token registry, OIDC and PAM providers, token
rotation command, and 90-day bearer lifetime did not. Current source has two
separate authentication systems: one shared administrative bearer secret and
local fleet-console accounts with signed browser sessions.

**What actually shipped:**

- **v0.1 through v0.4.** There was no authenticated HTTP mutation surface.
  Local filesystem and process ownership were the local boundary, and SSH was
  the remote access boundary.
- **v0.5.0.** The first web dashboard was read-only, loopback-bound by default,
  and unauthenticated.
- **v0.5.1.** `vq web init-token` introduced one shared bearer token for the
  single-host HTTP mutation endpoints. Read endpoints stayed open. The token
  is a random 256-bit value stored in cleartext at the resolved `web-token`
  path; vq refuses to read a file with group or other permission bits.
- **v0.6.x.** Multi-user admin commands and daemon RPC mutations reused that
  same shared token as an administrator credential. This did not create a
  per-user token identity and did not map a token to a queue uid.
- **Fleet-console M2.** Local username/password accounts added the `viewer`,
  `operator`, and `admin` roles. Passwords are stored as salted scrypt hashes
  in `web-users.json`. Successful login issues a signed HMAC-SHA256 cookie
  carrying the user, role, and a 12-hour expiry. The shared bearer token is
  accepted as admin-equivalent for scripted fleet API access.

Creating the first local account enables the account/session gate for the
fleet surface and, in fleet mode, for the single-host read routes served on
the same port. With no accounts, those reads remain open in the documented
loopback or SSH-tunnel posture and fleet write actions fail closed. A plain
single-host sidecar has no account login route, so its reads remain open and
its mutation API continues to use the shared bearer token.

Bearer-token rotation is manual: `vq web init-token --force` replaces the
stored secret and invalidates the old value. Bearer tokens have no encoded
expiry, automatic rotation, per-user attribution, or revocation registry.
Local account passwords and roles are replaced with
`vq web user add USER --role ROLE --force`; accounts are removed with
`vq web user remove USER`.

**Retired proposals.** There is no per-user bearer-token store, no
`tokens/{token_hash}.json`, no `vq token rotate`, no 90-day bearer-token
lifetime, and no OIDC or PAM authentication provider. Fleet-console accounts
are console identities only; they are not OS accounts, queue ownership
principals, or quota identities.

### 7.3 Authorization

**Classification: mixed.** vq has several authorization planes rather than
one global ownership-plus-admin policy. Direct CLI job access, daemon control,
the web console, and operating-system access use different principals.

**Direct CLI job access.** Ownership checks are no-ops in single-user mode.
In multi-user mode, selected direct job operations call the shared ownership
helper: status, logs, events, output, progress, tail, local wait, kill, local
and scheduler pause/resume, bulk pause/resume, direct local workspace,
artifact, and workdir fetch, and resubmit. The internal
`tar-workspace`, `tar-artifact`, and `tar-workdir` emitters use the same helper
before opening their stdout tar stream, so SSH-backed fetch checks the effective
uid of the configured SSH process. A shared service account can cross owners
only when it is root or belongs to the configured admin group. Bulk fetch
records an ownership denial as an error for that job and continues the sweep.

The helper permits the caller when its effective uid matches the spec
submitter, when it is root, or when it is a member of the configured admin
group. When system multi-user mode is selected, vq fully validates the system
config and treats its enabled policy and `admin_group` as authoritative over a
personal config. A selected multi-user operation fails closed when no valid
enabled policy can be established. For compatibility with old specs, the
helper also permits a missing or unresolvable submitter. Each command must opt
into this helper; it is not global middleware.

Pause/resume applies that effective policy inside the selected spec's lock,
before classifying the job state or tag and before sending `SIGSTOP`,
`SIGCONT`, `qhold`, or `qrls`. Bulk helpers authorize each candidate under its
own lock before inspecting its local or scheduler state. A foreign candidate
is isolated as an error and the sweep continues without exposing that job's
state or `paused_by` value. The single-job helper repeats locked authorization
before its mutation, so bulk classification is not treated as mutation
authority. A failure to establish one valid effective multi-user policy fails
the command closed rather than becoming a per-job row. Single-user ownership
checks remain no-ops.

Scheduler pause alone releases its first authorized lock for one read-only
scheduler poll. Scheduler resume has no corresponding poll. Both verbs
reacquire the spec lock and re-read ownership, exact handle, state, and any
`--paused-by` filter immediately before the scheduler mutation. The lock is
held across the remote mutation, final owner and snapshot check, durable spec
write, and commit. A failure after `qhold` or `qrls` attempts the exact inverse.
When the inverse succeeds, the original spec remains untouched, or vq restores
and byte-verifies it when a local write was attempted. When the inverse fails,
vq attempts to persist the intended post-state with
`hold_outcome_unknown` or `release_outcome_unknown`. Only a verified marker
names the opposite ordinary resume or pause command as the explicit inverse
recovery route; marker or restore uncertainty remains a compound fail-closed
error. A rollback write can be retried only after an authorized read proves the
durable record is still the byte-exact post-state. Event append is best-effort
after commit and cannot roll back an already consistent scheduler/spec pair.

Submission stamps the caller's numeric effective uid into `JobSpec.submitter`
in multi-user mode, but that field is not immutable or authoritative. The
daemon treats the numeric `users/<uid>` directory containing the queue record
as the trusted owner and rejects a new multi-user spec whose submitter is
missing, non-numeric, or different from that directory uid.

**Discovery and operator surfaces.** `vq queue`, overview views, cleanup
candidate discovery, and the web consoles aggregate jobs visible to their
process across the per-user trees. They do not filter rows through
`check_owner`. `vq queue --all` means every configured host, not every user,
and it has no admin-group gate. Manual cleanup is an operator-oriented,
terminal-job surface: it is a dry run unless `--execute` is supplied and
relies on filesystem or root authority rather than the per-job ownership
helper.

Membership in the configured admin group has limited, surface-specific
effects. It bypasses `check_owner` and can grant access to the multi-user RPC
socket. It does not itself grant filesystem traversal through every
root-owned `users/<uid>` directory, make root-required operations
unprivileged, or create a universal act-on-any-job API. Multi-user RPC writes
also require the shared admin bearer token; read-only RPC methods require
socket access but not that token.

Fleet-console roles are console-wide, not queue-owner identities. A viewer can
read console-visible fleet data, an operator can request kill, pause, and
resume actions for console-visible jobs, and an admin inherits those powers
and can read the fleet audit view. The shared bearer token is admin-equivalent
for the documented API. Action execution remains subject to the service or SSH
account and the underlying host-side job operation.

**Known coverage gaps.**

- An issued fleet browser session continues to trust its signed embedded role
  until its 12-hour expiry even if the account is removed or demoted.
- The admin-group ownership bypass does not solve cross-user filesystem
  traversal. Root, additional operating-system access, or a future
  daemon-mediated owner-qualified API may still be required.

**Audit records.** There is no single actor event stream. Per-job
`events.jsonl` is a best-effort lifecycle timeline and has no universal
`actor_uid` field or complete-mutation guarantee. `rpc-audit.jsonl` separately
records registered `set_*` RPC calls with a best-effort, nullable peer `uid`.
`fleet-audit.jsonl` separately records the console user, role, action, host,
job, and outcome. Other CLI mutations do not gain a universal actor record
from any of these files.

**Retired proposals.** There is no custom authorization-policy entry point,
no admin requirement attached to `vq queue --all`, no immutable submitter
field, no blanket guarantee that an admin-group member can act on every job,
and no guarantee that every mutation records `actor_uid` in the per-job event
timeline.

### 7.4 Secrets

License keys, SMTP passwords, webhook tokens: never live in `config.toml`. References do, values come from:

1. `os.environ` at daemon start (recommended for systemd-managed daemons).
2. systemd `LoadCredential` mechanism (preferred when available).
3. Files referenced by absolute path with mode 0600 enforced (vq refuses to read otherwise).

The CLI `vq config show` redacts values from secret references. `vq config audit` checks that referenced secrets exist and have safe permissions.

### 7.5 Network policy

Outbound destinations are declared in config under a `[network]` block:

```toml
[network]
allowed_outbound = [
    "https://hooks.slack.com/*",
    "smtp://mail.example.edu:587",
    "https://license.faccts.de/*",
]
```

vq logs a warning if an adapter or notification attempts an outbound connection not in the allowlist. Off by default in single-user mode (no allowlist enforcement), required at multi-user from v0.6.

---

## 8. Cluster integration

**Classification: current architecture.** This section supersedes the original
May 2026 proposal to defer a single swappable scheduler dispatcher until v1.0.

### 8.1 Posture

vq now has parallel local-process and external-scheduler paths. A spec with no
`scheduler_target` uses `dispatch.LocalDispatcher` for its in-memory `Popen`
lifecycle. A spec with a scheduler target uses
`scheduler_dispatch.SchedulerDispatcher`, which stages and supervises the job
through a configured scheduler host. The daemon owns admission, durable state,
and terminal bookkeeping for both paths.

These are deliberately separate interfaces, not interchangeable implementations
of one protocol. Local process-group, cgroup, watchdog, and restart handling
remain in the daemon. Scheduler submission, polling, cancellation, workspace
transfer, and result retrieval use the scheduler-specific interface. The old
`daemon.dispatch_one` named seam never existed in the landed architecture.

### 8.2 Scheduler backends

Torque and Slurm are implemented as dialects over the same
`SchedulerDispatcher` transport workflow:

- `JobSpec.cpus` and `wall_time_seconds` map to both implemented dialects'
  script directives. `mem_mb` is requested by default; a scheduler host may
  set `scheduler_mem_directive = "omit"` when the site's Torque configuration
  interprets `-l mem=` as a restrictive process RLIMIT rather than an
  allocation request. Slurm also maps `scheduler_tasks` to `--ntasks`; Torque
  currently ignores that field. There is no first-class JobSpec GPU request
  yet.
- The fixed driver daemon keeps the authoritative JobSpec and local workspace
  path. The dispatcher stages a remote working copy over SSH, renders a
  scheduler script, and submits with `qsub` or `sbatch`.
- Reconciliation batches `qstat` or `squeue` by scheduler host. `qstat -f` or
  `sacct` adds scheduler state, execution-host, and walltime detail. The vq
  lifecycle remains RUNNING after submission while `scheduler_state` carries
  queued, running, held, or retry detail. Production keeps at most one bounded,
  read-only observation in flight per scheduler host; completed observations
  are applied by the daemon thread, so a slow host cannot stop local dispatch
  or another scheduler host. A Slurm batch that returns its exact invalid-job-ID
  diagnostic is split only for the missing candidates: live siblings retain
  their observed phase, and only a handle independently rejected by Slurm is
  treated as absent. Every other nonzero, malformed, or transport outcome is
  unknown and keeps the prior nonterminal reservation. Raw operating-system or
  subprocess failures at the scheduler runner boundary follow the same
  host-local rule: they cannot abort reconciliation of another host or the
  daemon dispatch pass.
- The generated exit marker is the return-code authority and the completion
  fence. Scheduler accounting supplies diagnostics and walltime evidence when
  the marker is missing.
- The local `/proc` watchdog does not sample scheduler-target jobs. Resource
  requests are enforced by the external scheduler. Detail polling reports
  scheduler state, execution host, and elapsed/limit walltime; it does not
  replace local CPU/RSS telemetry.
- Terminal workspaces are fetched back to the driver when available; bounded
  retrieval failure is recorded without hiding an authoritative return code.
  The state root does not move to a scheduler-side shared filesystem.
- `scheduler_job_id` and the deterministic remote workspace let the driver
  reattach after restart; cancellation routes through `qdel` or `scancel`.

`pbspro` and `sge` remain accepted `scheduler_dialect` vocabulary but have no
implementation; selecting either dialect fails closed instead of guessing flags.
Shared-state-root operation remains a future design, not a current scheduler
requirement.

### 8.3 Site portability

**Classification: current scheduler boundaries plus retired proposals.** Current
site portability comes from explicit host configuration:

- `scheduler = "local"` is the default; a scheduler host must declare its
  dialect, absolute scratch root, SSH target, and always-on `scheduler_driver`.
- The single-user state root comes from `VQ_STATE_DIR`, otherwise the XDG/default
  layout; multi-user mode has its separate system root. It is not a TOML host
  setting, and only fields that need stable host-wide locations are validated
  as absolute.
- Queue/account directives, node scratch, trusted prologue/epilogue lines, and
  per-program scheduler hooks are configured per host rather than hard-coded
  into a dialect.
- Torque and Slurm share the pure `SchedulerDialect` protocol. An unknown or
  unimplemented dialect fails before submission.

The original draft's Python entry-point adapter system, `disabled_adapters`,
and web-brand settings are not implemented scheduler requirements. Plugin-based
adapter discovery is a retired proposal for this workstream; site branding is a
future consideration, not part of the scheduler contract.

---

## 9. Configurability

### 9.1 Layered config

Three layers, last-wins:

1. Built-in defaults (in code).
2. Site config at `/etc/vq/config.toml`, root-installed, multi-user.
3. User config at `~/.config/vq/config.toml`, single-user overrides.

The daemon reports the effective merged config at startup and via `vq config show`. Schema validated with pydantic; malformed config refuses to start the daemon with a clear error.

### 9.2 Code-specific profiles

A "profile" is a named adapter-config preset. Sites ship them, users invoke them.

```toml
[profile.orca-fast]
adapter = "orca"
cpus = 8
mem_mb = 32000
wall_time_seconds = 3600
adapter_config = { scratch_root = "/scratch/local", license_check = true }

[profile.orca-aimd-overnight]
adapter = "orca"
cpus = 16
mem_mb = 96000
wall_time_seconds = 86400
```

`vq submit --profile orca-fast input.tar.gz` is the daily-driver UX. Profiles are a core ergonomic feature, not an afterthought.

### 9.3 Container option (later)

A `container` field in JobSpec, opt-in:

```toml
[adapter.orca]
container_image = "docker://faccts/orca:6.1.1"
container_runtime = "apptainer"
```

Apptainer (formerly Singularity) is the target, not Docker, because HPC sites prefer it and rootless execution is cleaner. Off by default through v1.0. ORCA in particular benefits from containerization given the bundled-OpenMPI quirk and the per-version dance university clusters do.

---

## 10. API for AI agents (Claude Code and similar)

### 10.1 Why this is its own section

Michael called this out specifically: Claude Code chats need to submit test jobs. Agent-to-vq integration has different requirements than human-driven submission, and they affect the API design.

### 10.2 Submission contract

The HTTP API endpoint for submission accepts a JSON body with the JobSpec fields plus an `idempotency_key`. Response is 201 with the jobid, or 200 with the existing jobid if the key matches a recent submission.

For workspace ingest, two modes:

1. **Inline.** Files are sent as base64 in the JSON body. Simple, fits a single tool call, capped at a configurable size (default 5 MB total). Suitable for short ORCA inputs and vibe-qc test cases.
2. **Multipart upload.** `POST /api/v1/uploads` returns an upload token. Agent streams the tar.gz to that endpoint, then submits with `inputs.upload_token` in the spec. For larger inputs.

The agent-friendly default is inline. Most test jobs from a coding chat are tiny.

### 10.3 Polling and result retrieval

Three retrieval modes:

- **Poll status:** `GET /api/v1/jobs/{jobid}` returns the current spec plus event log tail.
- **Long-poll:** `GET /api/v1/jobs/{jobid}/wait?timeout=60` blocks up to 60 seconds for a state transition. Cheap when the job is short. Returns even if no transition occurred so the caller knows the job is alive.
- **Output download:** `GET /api/v1/jobs/{jobid}/outputs.tar.gz` once terminal. Streams the same artifact `vq fetch` would produce.

Webhooks are not used for agent flows. Webhooks require the agent to host a callback endpoint, which Claude Code chats cannot do. Polling is the right primitive here.

### 10.4 Token scoping

Tokens for agents SHOULD be scoped (v0.7): a token may be limited to specific adapters or profiles, may have per-day quota, may be valid for a window. A "Claude Code dev token" with submit-and-read rights to the `vibeqc` and `orca` adapters, capped at 50 jobs/day, is the intended shape.

### 10.5 Stable schema, slow deprecation

The `/api/v1/...` namespace is stable. JobSpec field additions are additive and optional. A v2 namespace ships only when a backwards-incompatible change is unavoidable, with a 12-month overlap. AI agents in the wild cannot easily be retrained on schema changes; the API treats them as a long-tail integrator, not as the developer who can read a CHANGELOG.

---

## 11. Testing strategy

### 11.1 Layers

Already strong at 204 tests. The spec adds requirements for sustained quality:

1. **Unit tests** for every adapter, schema, validator. Per-component, fast.
2. **Integration tests** that exercise CLI -> daemon -> dispatch -> exit -> fetch on a real subprocess. The existing detached-daemon e2e is the prototype, expand to cover SSH transport, web API, and watchdog kills.
3. **State-machine property tests** (Hypothesis). Generate random sequences of (submit, kill, daemon-restart, watchdog-tick) and assert state-machine invariants: terminal states are sticky, every RUNNING has a process or transitions to LOST, etc.
4. **Chaos tests.** Inject failures: kill -9 the daemon mid-dispatch, fill the disk during a write, drop the SSH connection mid-submit. Run nightly.
5. **Compatibility tests.** Old JobSpec versions on disk must still load and migrate. A `tests/fixtures/specs_v0_1.json` directory grows over time and never shrinks.

### 11.2 Continuous regression

The vibe-qc regression suite (5-10 quick jobs) doubles as vq's smoke test from v0.4 onward. CI submits the suite to a vq daemon on every PR and asserts all jobs reach DONE within a budget. This catches integration regressions that pure unit tests miss.

### 11.3 Test data isolation

Every test that touches state uses a per-test `$VQ_STATE_DIR` under `tmp_path`. The state-dir override is already a first-class config knob, this just makes it a testing invariant.

---

## 12. Migration and upgrade

### 12.1 Spec versioning

Every JobSpec carries `version`. Breaking changes bump the version. The daemon refuses to load specs newer than its own version (forward compat is not free), and migrates older specs on read via `vq.migrate.v{N}_to_v{N+1}` functions.

`vq migrate` is a CLI command from v0.4. It is also called automatically by the daemon at startup, with a config flag to disable for sites that prefer manual control.

### 12.2 SQLite schema

From v0.5, SQLite schema is managed with a migration tool (Alembic-light, custom is fine). Migrations are forward-only. SQLite is derived state, so the safe rollback path is "rebuild from JSON" via `vq reindex`.

### 12.3 Config schema

Config schema versions parallel JobSpec versions but evolve more slowly. Deprecated keys log a warning for one minor version, fail in the next.

---

## 13. Revised roadmap

**Classification: retired proposal.** The entries below are the May 2026 draft's
proposed diff against the roadmap at that time. They are retained for design
rationale, not as current status or sequencing, and must not be implemented just
to make this snapshot true. `docs/roadmap.md` is the live sequencing source of
truth.

### v0.3 - resource watchdog (in flight)

Stays as planned. Add to the scope:

- **`wall_time_seconds` enforcement.** Currently absent from the roadmap. This is the single missing safety net for AIMD jobs and a small lift on top of the watchdog. Inclusion in v0.3 raises the urgency of `wall_time_seconds` becoming a JobSpec field, which means the migration story starts here.
- **Distinct terminal states for OOM/STARVED/TIME_EXCEEDED.** Already implied; spec it explicitly.

### v0.4 - read-only web UI + event log + spec v2

Expanded scope from the existing roadmap entry:

- Read-only FastAPI as planned.
- **Add the immutable per-job event log** (`_vq/events.jsonl`). This is a structural change that everything from v0.5 forward depends on. Doing it now, before the web UI ships write actions, means write actions can write events natively rather than retrofit.
- **Spec v2** with `wall_time_seconds`, `pgid`, `events_path`, `version` field promoted to required. Migration v1 -> v2 ships here.
- **Improved daemon recovery**: distinguish "process gone" from "process alive" at restart, INTERRUPTED only when truly orphaned. Requires v2 spec for `pgid`.

### v0.5 - write actions + metrics + HTTP API

Expanded scope:

- POST kill, POST resubmit as planned.
- **Versioned HTTP API at `/api/v1/`**. This is currently implicit in "write actions" but deserves first-class billing because Claude Code submission is a stated requirement.
- **Idempotency keys** on API submit, retention 24h.
- **SQLite index** for query performance. Background reindexer.
- **Prometheus `/metrics`** as planned.

### v0.6 - multi-user + security + adapters + reproducibility

Expanded from the existing v0.6:

- Per-user state dirs and ownership checks as planned.
- **Code adapter system** lands here. `raw` (extracts current behavior), `vibeqc`, `orca`, `python`. Adapter discovery via entry points. Profiles in config.
- **Per-user tokens** replace the single shared token. Token rotation.
- **Reproducibility metadata** captured at submit time.
- **Secrets handling** via env / systemd `LoadCredential` / 0600 files.

This is the largest version on the roadmap. It should not be a single PR. Suggested sequencing: adapters first as a pure addition (raw is a no-op wrapper around current behavior), then auth, then multi-user state.

### v0.7 - priority + quotas + retries + notifications + restart-from + dependencies

Existing roadmap plus:

- **`restart_from`** support, adapter-defined semantics. ORCA: stage `.gbw`/`.hess`. vibe-qc: stage prior workspace. Useful when a long job hits TIME_EXCEEDED.
- **Job dependencies** (`depends_on`) and **array jobs** (`array_index`/`array_size`). Basis-set sweeps and conformer searches benefit hugely. Both are queue-side features, no dispatcher changes needed.
- **Watch-dir submission**: drop a tar.gz in `~/.vq-inbox/`, daemon picks it up. Useful for `rsync`-driven workflows from a remote machine.
- **Email and webhook notifications** as planned.

### v0.8 - observability + container option

- **OpenTelemetry tracing** for end-to-end visibility.
- **Container adapter option** via Apptainer.
- **Grafana dashboard ship.**
- **TCP/IP notification channel** for lab integrations.

### v0.9 - hardening for cluster

- **Shared-filesystem state root** support (NFS/Lustre with locking).
- **OIDC and PAM auth options.**
- **Custom policy modules** via entry point.
- **Backup and restore tooling** for the state root (`vq backup`, `vq restore`).

### v1.0 - SLURM backend evaluation

Decision point: is the workload now multi-host enough to need SLURM under the hood? If yes, SLURM backend ships. If no, v1.0 is a feature-freeze and stabilization release with the existing single-host dispatcher and a "ready for the lab to depend on" stamp.

### Beyond v1.0

- GPU scheduling (whole-device).
- Distributed control plane (Postgres backend).
- A vq-aware Snakemake/Nextflow integration (vq as a "remote executor" target).
- vq-as-SLURM-frontend at scale: the user types `vq` commands, vq translates to `sbatch`, lab does not need to learn SLURM.

---

## 14. Open questions for Michael

**Classification: retired question snapshot.** These questions were open when
the May 2026 draft was written. They are retained as historical design context,
not as the current approval queue, and their presence here does not mean they
remain unanswered. Current decisions belong in `docs/roadmap.md`, the relevant
design document, or an active handover.

1. **v0.4 scope: is the event log + spec v2 + daemon recovery rework too much for one release?** The case for bundling is that they depend on each other and a half-done version is awkward. The case for splitting is that v0.4 was meant to be a small, fun ship after the heavy v0.3 watchdog work. My lean: bundle, because the alternative is shipping a web UI that has to be rewritten in v0.5 to use events.
2. **ORCA license enforcement: required from day one or opt-in?** The spec says opt-in (`license_required = false` by default), which keeps v0.6 simpler. If you have a site (a university cluster) that needs hard enforcement before they will deploy, this becomes default-on for that site only and the work is bigger.
3. **Watch-dir submission in v0.7: actually wanted, or YAGNI?** The use case is rsync-and-forget from a remote workstation. Concrete value, but it adds a third submission interface to maintain. Cut if not actually used in your workflow.
4. **Dependencies and array jobs: v0.7 or v0.8?** Dependencies are the bigger feature. Array jobs are smaller but very common in basis-set work. They could split: array jobs in v0.7 (small), dependencies in v0.8 (designed against real use cases that emerge in the meantime).
5. **Container support timing.** The spec puts it in v0.8. If a university cluster commits to running vq before then and they want Apptainer, this moves up. If not, it might slip past v1.0 entirely as the field has not converged on a clear winner for QC code distribution.
6. **TCP/IP notification: what shape?** The user request was broad. Concrete options: ZeroMQ pub/sub, plain TCP line protocol to a configured host:port, MQTT. I have spec'd nothing concrete. Tell me which existing lab system this needs to integrate with and I will pin it down.

---

## 15. Out of scope, with rationale

These are stated explicitly so they do not get re-litigated every six months.

- **GPU fractional sharing.** Whole-device only. Fractional sharing is an inference-fleet feature, not a chemistry feature.
- **DAG workflow engine.** Snakemake, Nextflow, AiiDA exist and are good. vq is the dispatcher beneath them, not a competitor.
- **Multi-tenant adversarial isolation.** Containers + SLURM is the right answer for this. vq's audience is a research group, not a cloud provider.
- **A web UI build pipeline.** htmx + vanilla HTML is the chosen stack. No React, no TypeScript, no bundler. The web UI is an internal tool, not a product.
- **Runtime scheduling of GPU memory or CPU pinning.** Outside the scope of a userland queue. Use cgroups, use SLURM cgroup integration, use NUMA tooling.
- **Email submission.** Operationally awful, security nightmare, and the use case (Claude Code) is solved cleanly by the HTTP API.
- **Real-time streaming of stdout to the web UI.** Tail-on-refresh is enough. WebSockets for log streaming is a feature factory, not a need.

---

## 16. Glossary

- **Adapter:** Code-specific Python module that knows how to validate, stage, run, and post-process a job for a given QC code.
- **Dispatcher:** A job-launch mechanism. Local jobs use `LocalDispatcher` for the `Popen` lifecycle; scheduler-target jobs use the separate `SchedulerDispatcher` interface for staging, submit, polling, cancellation, and result fetch.
- **Event log:** Per-job append-only JSONL of state transitions and notable events. Source of truth for "what happened."
- **JobSpec:** Declarative description of a job, on disk as JSON. Backend-agnostic.
- **Profile:** Named bundle of adapter + resource defaults + adapter config, invoked by name at submit.
- **State root:** The directory holding all vq state for a daemon. Defaults to `~/.local/share/vq/`, override via `$VQ_STATE_DIR`.
- **Watchdog:** The supervision component that samples running jobs and enforces resource limits.
- **Workspace:** Per-job directory holding inputs, outputs, and `_vq/` metadata.

---

## 17. How to update this document

This doc lives at `vibe-queue/docs/SPEC.md` alongside `roadmap.md`. The roadmap is reordered freely as priorities shift. This doc changes only when:

1. A non-negotiable in section 1.2 needs to change. (Treat that PR with proportional care.)
2. A scope or rationale for a roadmap entry changes. Update both this doc and the roadmap in the same PR.
3. A feature ships and the "future" framing in the spec is now historical. Update past tense.
4. An open question in section 14 is resolved. Move the resolution into the relevant section, delete the question.

Discussions about *whether* to do something belong in GitHub issues or design docs. This doc records *what was decided.*
