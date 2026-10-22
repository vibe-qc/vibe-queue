# Durable broker design (#64)

Status: approved scope, implemented in increments; the user-facing surface is
not yet declared stable, so this page stays a design record. Implemented: B1's
state-root diagnostic (v0.26.15); B2's pure placement decision, B3's durable
held store, B4a's per-job placement and recovery, B4b's strict-FIFO placement
pass, and B5 and B6, the client relay and the `[broker]` role with its
daemon-run pass, target capacity evidence and the `SECURITY.md` entry
(v0.26.17). `vq wait` and `vq logs --follow` for broker jobs followed in
v0.26.18. The role is off until a host enables it. B7, the console view, lists
the broker's jobs on `/fleet/jobs`, metadata only (decided 2026-09-29).
Bounded live acceptance on two daemon hosts passed with v0.26.26: held
placement reasons, client relay, keyed retries, recovery after a lost transfer
acknowledgement, streaming logs, result fetch, cancellation and resubmission,
and the read-only console. The broker used an isolated store and manual
placement passes; this does not enable the role on any production host.
The maintainer's decisions on the canonical issue govern this record.

## Scope and authority

One durable broker runs on the coordinator that hosts the fleet console.
Clients relay payloads through that broker over existing SSH as the operator
account. Only the broker needs the fleet inventory and target credentials.
There is no new network listener, per-client authorization model, HA, or second
writer. Losing the broker delays placement and broker-addressed access; it
does not discard held payloads or authorize another writer to take over.

The first contract targets daemon hosts. PBS and Slurm lanes are explicitly
ineligible. Existing scheduler-driver queues remain separate. The broker is a
submission frontend, not a replacement for cluster scheduling or the target
daemon's admission, FIFO, quotas and resource enforcement.

Placement provenance (selected host, time, snapshot inputs and ranking) lives
only in the broker's durable record. No JobSpec field or existing status/queue
wire schema changes in this contract. Broker-addressed status and the later
console view read that record, and join target state through its recorded job
identity. The console must not reconstruct broker state with a second sweep.

## Durable held record (B3)

A held job is outside the broker's local execution queue: placing it must never
also execute it on the broker. Before acknowledging submission, durably publish
the complete payload and a broker-owned record with at least:

- Broker job ID, record revision, creation time and lifecycle state.
- Canonical submission intent and content identity, declared CPU/memory/time
  requirements, program/runtime selection, and optional pool constraint.
- Broker-local payload reference and digest, with no client path needed for
  restart or delivery. A partially received upload is not an accepted job.
- Placement attempts: target authority, reservation identity and resource
  charge, target state-root/daemon identity, snapshot time, eligibility results
  and ranking inputs, and the recorded decision time.
- Transfer intent: broker job ID as the target submission idempotency key,
  fixed payload/request fingerprint, attempt phase, acknowledgements and
  target job ID when known. Include failure/uncertainty diagnostics.
- Cancellation intent and reconciliation results, independently durable from
  the transfer phase. Retain terminal mapping/tombstone evidence after ordinary
  result cleanup so a retry cannot create a second logical job.

The serialized schema is internal broker state, not a published wire
format; see "B3 source interface" below for the first version and its upgrade
policy. Records and
payloads use the existing owner-only state discipline: validated paths,
a per-transition writer lock, atomic replacement and durable directory publication.
Record revisions must prevent stale writers from applying transitions. A
failed write/fsync means no acknowledgement or next side effect.

## Placement from a snapshot (B2)

For one held job, compute eligibility and ranking without I/O or mutation.
The caller supplies the observation time, current time and maximum acceptable
snapshot age; the function never reads the clock or gathers hosts. A candidate
must have positive evidence of a healthy reachable daemon, matching state root,
open admission and a queue that can admit this job now. Unknown, stale or
future-dated observations are not capacity. Administrative down/drain/update
holds, program availability, quota and throttle restrictions must be resolved
for the job before admission is considered open.

Inputs carry effective free CPU, memory and job-slot headroom. They must already
account for running work, work ahead in the target FIFO, broker reservations
not yet visible in target state, and any tighter daemon/throttle/submitter
limits. Memory headroom is the minimum of live available RAM and remaining
configured/admission budgets, after outstanding broker charges. Count a
reservation once: retain it until a refreshed target snapshot demonstrably
includes the accepted job, then transfer accounting to that snapshot. Unknown
memory or concurrency accounting must not be treated as unlimited. An
unlimited job cap can be represented by positive admission headroom for this
one decision once the other gates are known; zero means no slot.

The request must supply positive CPU and memory demand. A future collector may
resolve memory from a declaration, estimator or explicit configured default;
B2 invents no memory estimate. Optional wall-time/pool restrictions also apply.
Snapshot construction and proof of those inputs belong to B4; today's coarse
`HostOverview` alone does not establish them.

Filter out scheduler lanes and candidates that cannot fit the whole request
now. Among eligible hosts, reuse `recommend_host()`'s ordering: RAM fit, CPU
fit, most free RAM, most free CPUs, fewest queued CPUs, longest idle, then host
name for deterministic ties. Unlike client-side `auto`, lack of a fit means
hold, not a recommendation to queue on an overloaded host. Keep a rejection
reason per candidate so the broker can persist why a job stayed held.

The result is advice, not a reservation. B4 must serialize decisions, recheck
freshness and authority before committing a reservation, and debit that charge
before evaluating another held job. Direct submissions can race a broker
snapshot; the target daemon remains the final admission authority. The broker
does not promise an immediate start after transfer.

### B2 source interface

`vq.overview.BrokerPlacementRequest` carries resolved CPU/memory demand and
optional pool/wall time. `BrokerCapacitySnapshot` carries job-specific effective
headroom, timestamp and positive admission/authority evidence; its default gates
are closed. These frozen Python values are internal inputs, not a public JSON
schema. `broker_host_blocker()` answers eligibility for one snapshot.
`place_broker_job()` returns the selected host (or `None`), deterministic
per-host blocker reasons and the eligible ranking tuples. The caller supplies
`now` and `max_age_seconds`. It rejects duplicate host keys and invalid policy
inputs rather than selecting an arbitrary snapshot. The B4 placement
transaction uses this decision with evidence collected by B6 from each
target daemon's admission snapshot.

### B3 source interface

`vq.broker_store.BrokerStore.open(root)` creates or opens
`<state root>/broker/` with `records/`, `payloads/` and `staging/`, all
owner-only `0700` directories that must not be symlinks.

Several processes may have the store open at once: a placement pass, a
client submission, a cancellation. Minutes of SSH work in a pass must not
block a submission, so the store holds no lock for its lifetime. Instead:

- **Transitions:** every read-modify-write transition takes the exclusive
  lock on `broker/.lock` only for its own duration. The wait is bounded
  (`lock_timeout`, 30 s by default, then `BrokerStoreBusy`). The transition
  applies only if the record is still at the caller's revision, so a stale
  view fails instead of overwriting.
- **New records** need no lock: each is published with an exclusive
  `link()`.
- **Passes:** placement passes exclude each other with the separate
  non-blocking `broker/.pass.lock`, and so does a direct `place()`, so the
  charges one placement debits cannot change before it reserves. A second
  pass gets `BrokerStoreBusy`. Both lock files must be owner-only regular
  files.
- **Failed creation:** a new record's publication, and its withdrawal if
  the directory fsync fails, happen under the writer lock. No other
  process's transition can act on a record that is about to be withdrawn.
- **Concurrent cancellation:** a job that another process cancels during
  a pass is reported in `PassReport.skipped`, not as an error. A conflict
  during recovery or charge settlement is re-read and applied to the
  current revision.
- **Evidence that must not be lost:** a pass records a target's acceptance
  or a transfer diagnostic on the current revision when a concurrent writer
  (usually a cancellation) moved the record on while the transfer ran. This
  happens only while the record is still that same transfer, identified by
  its fingerprint. An accepted job whose cancellation arrived mid-submit is
  therefore `placed` with its kill still owed. The store's directories, records and
payloads are opened no-follow, relative to directory descriptors, and non-blocking,
so a FIFO in place of a file is refused rather than waited on. The
state-root parent is opened by path. Archive validation reads the staged copy
by path inside the owner-only staging directory, and `submit_remote` reads the
verified payload by path. These are windows open only to the same user.
Records are `0600` JSON files
named `<broker job id>.json`; payloads are `0600` archives named
`<broker job id>.tar`. Broker job IDs are `b-` plus 12 hex characters. Each is
a valid job ID and submission idempotency key, and cannot be mistaken for a
target's 12-hex job ID.

`accept(payload, intent, now=...)` copies the client's archive into
`staging/`, fsyncs it and records its SHA-256. It refuses a non-tar archive or
a Python entrypoint missing from it, using the same check as remote submit.
Only then does it publish the payload, then the `held` record, each by rename
and a strict directory fsync. It returns only after both are durable. Any
failure withdraws both, so a partial upload is never an accepted job. The
payload is published with a no-replace link, so an ID collision is refused
instead of replacing another job's payload. The
payload is a snapshot: later changes to the client's file do not reach it.

`BrokerIntent` is the canonical submission: command, CPUs, **mandatory**
memory, wall time, priority, retry, auto-resume, name, program and expected
SHA, tags, clean-tmp, rerun-until and an optional pool. It is validated by
the remote submit wire's own field checks. Some inputs are deliberately
absent, because they name one target or one queue: scheduler target, branch
interpreter, dependencies, arrays and chains, `--at` and `--refresh`.

Every transition takes the caller's `expected_revision` and applies only if
the stored record is still at that revision (`BrokerRecordConflict`
otherwise). Each one increments the revision and atomically replaces the
record:

| call | from → to |
|---|---|
| `record_held_blockers(blockers=)` | `held` remains `held`; save the latest reasons without a reservation or charge |
| `reserve(decision=, snapshot=, target_state_root=)` | `held` → `reserved`, a new attempt with target, charge (CPUs, memory, one job slot), snapshot and B2 ranking/blockers |
| `release_reservation(reason=)` | `reserved` → `returned`, charge released |
| `resume_holding()` | `returned` → `held`, or → `cancelled` if cancellation was requested |
| `begin_transfer()` | `reserved` → `transferring`, freezing the transfer fingerprint; refused once cancellation is requested |
| `record_transfer_diagnostic(message=)` | stays `transferring`, keeps the uncertainty evidence |
| `record_placed(request_fingerprint=, target_job_id=)` | `transferring` → `placed`, only for the frozen fingerprint |
| `settle_charge()` | `placed`, releases the charge once target state includes the job |
| `request_cancel(reason=)` | `held` → `cancelled`; any other open state records intent only |

Only two calls leave `transferring`: `record_placed`, and `fence_transfer`,
which needs a target fence (below). Loading a record that shows a started
transfer as returned is refused unless its return reason carries that fence
proof (`fenced on target: …`). `cancelled` is
terminal and is the "honor cancellation" outcome in the table below. A
cancellation of a transferring or placed job stays recorded until B4/B5
reconciles acceptance and kills the target job. Nothing in B3 reports a
target job as killed.

The transfer fingerprint is the SHA-256 of the target host, target state root,
idempotency key (the broker job ID), intent digest and payload digest.
`transfer_request(record)` builds the keyed submit for a `transferring`
record. It re-reads the record and refuses a stale view, a fingerprint that
no longer matches, or payload bytes that changed. It returns the fixed target
and the `submit_remote` keyword arguments, including `idempotency_key`. B4
must confirm that the target's host configuration still resolves to the
recorded state root before replaying.

`scan()` returns every record plus a refusal reason for each unusable one.
`outstanding_charges()` sums unreleased charges per target host, and refuses
while any record is unusable. Loading is strict: exact field sets, the schema
string, the intent digest, and consistency between the state and its attempts
are all checked. Records and payloads are never deleted by B3; placed and
cancelled records are the tombstones.

**Upgrade policy:** the schema string is `vq.broker-record.v1`. A reader
refuses any other version rather than guessing. A later format adds a new
version with an explicit, reviewed migration.

## Reservation and transfer state machine (B3/B4)

| state | durable meaning | permitted next action |
|---|---|---|
| `held` | Payload accepted on broker; no target can have accepted it | Re-evaluate snapshots or cancel locally |
| `reserved` | Target and charge recorded; transfer has not started | Recheck bindings/capacity, then durably enter `transferring`; or release the unstarted reservation |
| `transferring` | Fixed target and idempotent request may have reached it | Reconcile or replay the same intent to the same authority; never choose another host on uncertainty |
| `placed` | Verified target acceptance receipt and target job ID recorded | Proxy observation/control to that target and reconcile lifecycle |
| `returned` | Attempt ended with proof that target acceptance cannot occur | Release its charge, retain attempt evidence, then return to `held` or honor cancellation |

Write `transferring` **before** invoking remote submission. A crash after that
write but before sending is intentionally treated as uncertain. Replay uses
the existing single-job idempotent submit path (SPEC §4.8), with the same broker
job ID, target authority, intent and content identity. Changed intent is a
conflict, not a new submission. Arrays/chains need an explicitly reviewed
extension because the current idempotency interface accepts one logical job.

Idempotency is scoped to one target's durable store. Using the same key on a
different host does **not** prevent duplicates. An SSH timeout, missing job in
an observation, drain, daemon restart or expired reservation is never proof of
nonacceptance. Once transfer might have started, freeze the target binding and
retain the resource charge until reconciliation. A renamed host alias or
changed state root must not silently redirect a replay.

`returned` is safe before any transfer attempt. After an attempt starts, it
requires a durable rejection/fencing proof that no old request can later accept.
Without one, an ambiguous attempt stays pinned for same-target replay; there is
no automatic return.

**Operator reconciliation (maintainer decision, 2026-09-29).** `vq broker
reconcile b-…` supplies that proof from the target itself. Under the pass lock
it runs `vq admin fence-idempotency-key KEY` on the attempt's target over SSH as
the operator. The target takes the same per-key lock every keyed submit holds:

- A claim, or a spec that a crash left without its claim, means the submission
  was accepted. The claim is repaired and the answer is `accepted` with the job
  id, and the broker records `placed`.
- Otherwise the target durably writes a fence record at the claim's name, and
  any later keyed submit under the key is refused. The answer is `fenced`, and
  the broker records the attempt `returned` (charge released, proof in the
  return reason), then holds the job again, or cancels it with `--cancel` or an
  earlier cancellation.

The answer must name the attempt's recorded state root, because idempotency is
scoped to one state authority. The command adds no listener or daemon RPC.
After acceptance, target drain/down changes do not authorize resubmission
elsewhere. Completed or killed work is not a returned transfer.

### B4a source interface

`vq.broker_placement.BrokerPlacer(store, submit=, state_root_of=,
refuse_target=)` runs the per-job transaction over one open store.
`place(broker_job_id, snapshots, max_age_seconds=)` places one `held` job:

1. Every outstanding broker charge is subtracted from its host's snapshot
   (`debit_snapshots`). Unknown headroom stays unknown, and a negative
   result counts as insufficient.
2. B2 decides. With no eligible host, the job stays `held` and its latest
   per-host blockers are saved with a revision compare-and-swap, then returned.
   No attempt or capacity charge is created. A later decision replaces these
   reasons; reserving a target clears them in favor of the new attempt evidence.
3. B2's ranking is walked in order, and the first eligible host that is an
   allowed target with a known state root is chosen (maintainer decision,
   2026-09-27). A refused target (`remote_target_refusal`: unconfigured, or
   a scheduler lane) or an unknown state root is skipped. A probe that
   raises counts as unknown. The skipped hosts and their reasons
   (`target-refused: …`, `state-root-unknown…`) are added to the attempt's
   blockers. Only when every eligible host is skipped does the job stay
   `held`, with every reason listed. No attempt starts without an exact
   state authority to pin a replay to, and the chosen root is probed once.
4. The payload is verified against its recorded digest. A missing or
   changed payload raises here, before anything irreversible happens.
   `reserve` then records the debited snapshot the decision saw, and
   `begin_transfer` writes `transferring` before the submit.
5. `submit` delivers the keyed request (`remote_submitter` wraps
   `submit_remote` and requires exactly one returned job ID). Acceptance
   records `placed`, and the charge stays until `settle_charge`.

Any submit failure, including an error exit, a timeout or an invalid job ID,
is recorded as a diagnostic, and the job stays `transferring`, pinned to its
target.

`recover()` runs before new placement. An unstarted `reserved` attempt is
released and resumed; a job cancelled while reserved becomes `cancelled`. A
`returned` job resumes holding. A `transferring` job is replayed with the
same key and the same request, and only when the target's current state root
equals the recorded one. Otherwise, including when the probe raises, it
records why and stays uncertain.

A cancellation recorded on a `transferring` job wins over replay (maintainer
decision, 2026-09-27). Neither recovery nor a pass ever resends it, because
a replay could create the job the user cancelled. It reports
`cancel-pending`, naming the target and the idempotency key, and stays
`transferring` with its charge, because the first send may have landed.
The cancellation is checked before the request is built, so a lost payload
cannot hide the kill instruction. It is checked again immediately before
sending, so a cancellation committed during the state-root probe still wins.
The
cancellation stays incomplete until `vq broker reconcile` settles the
transfer: a fenced one ends `cancelled`, and an accepted one is placed with
the cancellation still recorded, for `vq kill` to kill on the target.

An unusable record is reported and left alone, and while one exists `place`
refuses, because its charge cannot be counted.

Deliberately absent from B4a: which held job is placed next and retry
pacing, both of which B4b adds (below). Still absent after B4b: persisting
why a job stayed held. A target that definitively refuses the keyed submit,
for example over a program pin its registry lacks, leaves the job
`transferring` with the target's error as a diagnostic until an operator
runs `vq broker reconcile` (above).
The relay and `vq broker accept` run the same payload checks as a direct
submit, so only target-side refusals remain.

### B4b source interface: one strict-FIFO pass

`BrokerPlacer.run_once(snapshots, max_age_seconds=, target_jobs=,
retry_interval_seconds=300)` is one placement pass. It is a function; the
`[broker]` role's daemon-run pass (B6) is what schedules it. In order:

1. **Unusable records stop the pass** before any placement, since their
   charge and queue position are unknown.
2. **Interrupted transitions finish:** an interrupted reservation or return
   is completed, exactly as in `recover()`.
3. **Charges settle:** a placed job's charge is settled once
   `target_jobs[host]` lists its target job ID and that host's snapshot was
   observed after the acceptance. The snapshot must also be one placement
   would accept: not future-dated and within `max_age_seconds`. From then on
   the target's own headroom counts the job, so it is never counted twice or
   dropped early.

   **Collector contract:** `target_jobs[host]` must list every job the
   target still has a record of, terminal ones included, not only active
   jobs. Otherwise a job that finished before a qualifying snapshot would
   never settle. `observed_at` must not be later than when the target
   sampled its state. The capacity collector below meets both: it lists
   every spec ID and stamps the call's start.
4. **Uncertain transfers replay:** each is replayed at most once per
   `retry_interval_seconds` (measured from the record's last update), and
   only to its fixed target. A replay that cannot even be built, for example
   because its payload is gone, is reported in `refused` and does not stop
   the pass. The record is pinned, not held, so it never blocks the queue.
5. **Held jobs are placed in strict FIFO order,** by acceptance time and
   then ID (maintainer decision, 2026-09-27).
   - The first job that cannot be placed now becomes the `head`, and every
     later job waits behind it without being evaluated. There is no
     backfill, even for a later job that would fit.
   - A head that cannot be placed at all, for example because its payload
     is gone, stops the pass with its reason and needs an operator.
   - A job whose transfer became uncertain is no longer held, so it does not
     block the queue. The same applies to a job whose placement failed after
     its transfer began: it is reported in `refused` and later jobs move up.
   - A refused or unreachable top-ranked host does not hold the queue, since
     placement falls back to the next-ranked eligible host (B4a, step 3).

Invalid inputs are refused before any step writes. These are a non-finite
or negative retry interval, an invalid `max_age_seconds`, and duplicate
snapshot hosts. The returned `PassReport` lists what was recovered,
settled, replayed and placed, plus the `head`, the `waiting` jobs, any
`stopped` reason and the `refused` non-held records.

### Capacity evidence (B5/B6)

B2 needs positive evidence, and no existing remote command reports it. A
first version derived admission from outside the daemon. An independent
review proved three cases where it said "open" while the daemon would hold
the job:

- killed jobs whose processes survive;
- the in-memory host-pressure flag;
- a pending refresh that holds local dispatch.

So the daemon is now the authority for its own decision. After each
dispatch pass its main loop publishes one immutable admission snapshot
(`Daemon._publish_admission_snapshot`). The snapshot carries:

- its holds: an admin update holding local dispatch, a changed config not
  yet applied, a drain touching local work (a lease that holds only
  scheduler targets does not), host memory pressure, a pending refresh, a
  running build job;
- its caps;
- its local usage, including orphans and surviving killed jobs, counted
  exactly as `_dispatch_pending` counts them, with undeclared jobs at the
  default memory charge or unknown;
- its count of pending local jobs.

A failure to compute the snapshot withdraws it. It is published even when
the dispatch pass raised, and queue facts from an earlier tick are never
reused: an unevaluated queue is a hold. The read-only RPC method
`get_admission_snapshot` only returns that published value.

`vq broker capacity --json` (`vq.broker_capacity.sample_local`) is
read-only. It requires the snapshot to be whole, well-typed and at most
30 s old on the host's own clock, and passes its holds through as closed
gates. It adds:

- positive state-root evidence, meaning a running daemon uses the root this
  vq resolves;
- the refusal of multi-user hosts;
- live free memory;
- the status of the named programs (`--program`, which the collector sets
  to the held jobs' programs);
- every job ID the host records.

Admission is open only when no gate is closed. `queue_ready` requires both
that the daemon counts no pending local job and that the live spec list
shows none. The snapshot can be older than a job accepted since it was
published, so the answer reports `snapshot_age_seconds` (the host's own
clock), and the broker backdates `observed_at` by it. A charge therefore
never settles on figures older than its job. Headroom:

- `free_cpus` is the daemon's CPU cap minus its usage.
- `free_jobs` is the job cap minus active jobs, or a large value when the
  daemon has no cap.
- `free_mem_mb` is the smaller of live `MemAvailable` and the memory
  budget's remainder. The remainder is unknown when an undeclared job's
  charge is unknown, and live memory alone never stands in for a budget it
  cannot prove.
- `gpus` and `free_gpus` (#168) are the daemon's GPU devices and those not
  assigned to a job now, from the same snapshot. A daemon that lists no
  devices reports `0`. A snapshot without GPU keys, from a daemon older than
  GPU admission, reports `null`: unknown, not none. They are evidence for
  [placement plans](placement_policy.md) (`node.gpus`, `free.gpus`). The
  broker's own placement does not use them, and it still refuses `--gpus`.

**Collection:** `vq.broker_capacity.collect()` runs the command on every
target over SSH, in parallel and with a bound. `evidence_from_payload`
converts each answer.

- **Unusable answers:** a malformed, older or unreachable answer is no
  evidence: every gate is closed and capacity is unknown.
- **Timestamps:** `observed_at` is the broker's own clock at the start of
  the call, a lower bound of when the target sampled. Clock skew can
  therefore neither make stale evidence fresh nor settle a charge early.
- **Pools** come from the broker's own `[pools]` configuration.
- **Program check:** `program_refusal` refuses a host whose registry does
  not report the job's program `OK`. The placer applies it per job in its
  fallback walk (`job-refused: …`).

### The broker role (B6)

A host becomes a broker only through its own config:

```toml
[broker]
enabled = true
targets = ["alpha", "beta"]   # [hosts.*] keys with scheduler = "local"
interval_seconds = 60         # how often its daemon starts a pass
max_age_seconds = 180         # oldest capacity evidence a placement may use
retry_interval_seconds = 300  # least time between replays of one transfer
collect_timeout_seconds = 60  # bound on each target's capacity call
policy = "capacity"           # or "completion-time" (#160, below)
```

The section is off by default. Every target must be a configured daemon
host, listed once by its canonical entry: alias and excluded entries are
refused, and two targets that report the same state root are both skipped
until the config is fixed, because one machine's capacity would otherwise be
promised twice. Nothing outside `targets` is ever placed on, even if a
collector answers for it. A multi-user host cannot hold the role. On a
broker host:

- `vq broker accept PAYLOAD --cpus N --mem-mb M [...] -- CMD...` durably
  holds a relayed submission and prints its broker job ID. It is the
  endpoint for the client relay (B5). `--mem-mb` is mandatory, and a
  `--pool` that is unknown, or that contains no target, is refused. Under
  strict FIFO such a job would hold every later one forever.
- `vq broker cancel ID` cancels a held job at once. A transfer in doubt
  becomes `cancel-pending`, and a placed job keeps the cancellation until
  its target job is killed.
- `vq broker pass [--json]` runs one pass. It collects evidence from every
  target (programs limited to those the held jobs name), then runs
  `BrokerPlacer.run_once` with the keyed remote submit, the target and
  program refusals, the evidence's state roots and job lists.
- `vq broker list` and `vq broker show ID` report records. `list` flags a
  job that needs an operator; `show` and `--json` give the kill
  instruction for a `cancel-pending` job.
- The daemon starts `vq broker pass --json` as its own session every
  `interval_seconds` (at least 5 s), logging to
  `<state root>/broker-pass.log`. The log keeps one previous generation
  past 8 MiB. The daemon keeps at most one pass, re-reads `[broker]` at most
  every 30 s, and never blocks its loop. A pass takes the pass lock before
  its SSH round, so a second pass, including one started by hand, stops
  at once.

Every broker command except `vq broker capacity` (which targets answer)
refuses on a host without the role.

### Opt-in completion-time ordering (#160)

`[broker] policy` decides how the hosts that may run a job now are ordered:

```toml
[broker]
policy = "completion-time"                      # default: "capacity"
placement_fleet = "/etc/vq/placement-fleet.json" # required with completion-time
```

**Admission is unchanged.** B2 still admits only hosts with positive
admission evidence and room for the job now, after debiting every
outstanding charge. The policy can only reorder, or further refuse, those
hosts. It never places a job where `capacity` would refuse it, so FIFO
holding, charges and transfer recovery are exactly as above.

`placement_fleet` is the operator's `vq.placement-fleet/1` record of the
daemon targets: node shape, programs, accounts, speed and transfer rates.
See [placement plans](placement_policy.md). Each placement:
1. Reads `placement_fleet` again.
2. Lays this pass's debited evidence over each admitted host: its
   observation time, `reachable`, and free CPUs and memory.
3. Asks the placement planner for the job's CPUs, memory and walltime, with
   its program, or a stand-in every target can run for a plain command.

The planner's order replaces B2's. The usual walk then takes the first
allowed target with a known state root.

Each refusal becomes a held blocker prefixed `completion-time:`:
- an admitted host missing from the record, or recorded as a scheduler lane;
- a host the planner rejects, for example too few CPUs per node, or no known
  walltime limit for a job that sets `--time-limit`;
- an unreadable record, which never falls back silently to `capacity`.

With no host left, the job stays held with every reason. Broker intents
carry no runtime estimate, so execution time is unknown. The order follows
the known components and the speed factor.

### The client relay (B5)

A client `[hosts.<name>]` entry with `broker = true` (a daemon host entry)
makes `vq submit <name> ...` relay instead of submit
(`vq.broker_relay.submit_via_broker`):

1. It packs the payload (`--dir`, `-c`, or a single file with an explicit
   `--python`).
2. It uploads the payload to the broker.
3. It runs `vq broker accept` there and prints the broker job ID.
4. It removes the upload after an answer, including a refusal. When the
   answer is lost, it keeps the upload and says the job may have been
   accepted.

Options a broker job cannot carry are refused by name before anything is
sent.

`--idempotency-key KEY` is relayed to `vq broker accept`. The broker job ID is
`b-` plus the first 12 hex digits of a domain-separated SHA-256 of the key, so
the key never reaches the record and the record schema is unchanged. Under the
writer lock, an existing record for that ID is returned when its intent and
payload digest are identical; anything else is a hard conflict (SPEC §4.8). A
payload left without a record by a crash mid-accept is adopted only when its
bytes are identical. A lost answer can then be retried safely with the same
key.

Every other verb uses the ordinary remote routing. `vq status|logs|kill|fetch
<name> b-...` runs the same verb on the broker, whose local verb recognises
a broker ID:

- **`status`** gives the broker record's view. Once the job is placed, it
  adds the target's own `status --json`.
- **`logs`** is proxied to the target job. With `--follow`, the broker
  waits, re-reading the record, until the job is placed, then streams the
  target's own `vq logs -f` through as it arrives. A job cancelled before
  placement is an error.
- **`kill`** records the cancellation with its `--reason`. It cancels a
  held job at once, kills the target job once placed (a target job that
  already ended is reported, not an error), and reports the operator
  action for a transfer in doubt. `--resubmit` then accepts a new held job
  from the retained payload and intent, but only once the old one provably
  stopped: cancelled, never able to begin a transfer, or its target job
  killed or already ended. A transfer in doubt is not resubmitted.
- **`fetch`** proxies `tar-workspace`, `tar-workdir`, `tar-artifact` and
  `mark-fetched` byte for byte to the target, so the fetch protocol,
  including its acknowledgement, stays end to end. The stream is the
  target job's, so the client asks the broker which target job the broker
  job became, then verifies the stream and its manifest against that ID.
  The tree lands under the target job's ID.
- **`wait`** polls the broker's `status`, never the target, so the client
  still needs only the broker. The job is pending while held, reserved,
  returned or transferring. Once placed, the target job's state decides and
  its exit code is passed through. A job cancelled before placement ends as
  killed. A poll that fails, or a placed job whose target status is
  momentarily unavailable, is retried on the next tick.

The broker uses its own SSH credentials. Clients need none for the target,
though `status` names it.
`vq status b-...` without a host goes to the one `broker = true` entry. A
broker ID on a host without the role is refused.

## Recovery and control

On startup, acquire the pass lock and load/validate all records before
placement. Reconstruct outstanding charges before placing new work. Corrupt,
unknown-version or contradictory records stop the affected placement/recovery;
never infer that missing evidence means a request was not delivered.

- `held`: retain payload and resume normal eligibility checks.
- `reserved`: if transfer never began, revalidate the target and either start
  the recorded attempt or durably release it.
- `transferring`: reconcile/replay only the recorded keyed intent against the
  same state authority. An unreachable target stays uncertain. Persist the
  acceptance receipt before releasing the broker's separate reservation.
- `placed`: use the recorded target job ID; a missing/cleaned target spec does
  not erase the target's durable idempotency tombstone or authorize a new job.
- `returned`: finish the documented charge release exactly once, then return
  to holding or complete a previously recorded cancellation.

A broker outage leaves target jobs under their target daemons and retains held
work on disk. Recovery does not restart accepted jobs. Loss of the broker disk
has no automatic failover guarantee; any restore requires reconciliation of
all previously attempted keys before new placement, not replay from an older
backup as if it were a fresh queue.

`status`, `logs`, `fetch` and `kill` are broker-addressed (B5). Status distinguishes
held, transferring/uncertain, placed and target-unreachable; unreachable is not
terminal. Logs/fetch before placement report that no target result exists.
After placement, proxy using the recorded authority and job ID, preserving
existing ownership and transfer validation. A held/unstarted cancellation is
local and durable. A cancellation during transfer records intent and reconciles
acceptance before issuing target kill; an unreachable target must not be
reported as successfully killed. Serialize cancellation with placement.

## Trust and rollout gates

Compromising the broker compromises the fleet accessible through its operator
SSH credentials. The broker record contains private inventory and workload
metadata; it is not part of the anonymous fleet snapshot. The broker entry in
`SECURITY.md` is final with B6, and the operator instructions, including
recovery, are in `docs/operations.md`, "The fleet broker".

B0 is documentation. B1 checks remote-vq/daemon state-root agreement. B2 is pure
placement. B3 adds durable records/idempotent transfer support; B4 adds the
recovery/placement loop; B5 adds client routing; B6 enables the role; B7 reads
broker records into the authenticated console. Each increment requires focused
tests and a changelog fragment. B6 makes the role available; it stays off
until a host's own config enables it.

Held-job ordering is decided: strict broker FIFO, which matches current
queue expectations (maintainer, 2026-09-27). Backfill would be a separate,
explicitly approved policy. B2 ranks hosts for one job; B4b's pass decides
the order.

Cancellation of a job whose transfer is uncertain is decided as well: the
job is held as `cancel-pending` and never replayed (see B4a).

Acceptance before enablement includes crash injection around every durable
transition, simultaneous admission/cancellation, stale snapshots, repeated and
ambiguous keyed transfer, changed target bindings, retained tombstones, output
fetch through the broker, broker outage/recovery, and scheduler-target refusal.
Source tests and CI are separate from authorized host evidence.
