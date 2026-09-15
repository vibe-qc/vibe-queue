# Agent interaction protocol -- compute-d / compute-a (and any other vq-managed host)

This page is the contract for **other dev chats** (basis-opt,
periodic-SCF, molecular methods, basissetdev, vqfetch, docs, …)
that need to run things on compute-d or compute-a. The queue chat owns
this doc; updates are coordinated through the queue chat.

If you are a chat that wants to do work on compute-d or compute-a, **read
this whole page before your first action.** It's short.

## TL;DR

* **Do not write to `/home/USER/gitlab/vibeqc-{dev,release,queue}/`
  on compute-d or compute-a.** Those checkouts are managed by
  `vq admin update`; uncommitted modifications or untracked files
  there break the next deployment.
* **Submit work via `vq`** with a payload (input file, dir, or
  archive). The daemon stages your payload into a per-job workspace
  and runs your command inside it.
* **Use `$VQ_WORKDIR` as the job result root, not as proof of a distinct
  scratch directory.** Local daemon jobs receive a separate managed workdir.
  Scheduler jobs use the shared staged workspace; it is also the payload cwd
  when `node_scratch_dir` is unset.
* **Never write onto a payload file.** Create a dedicated output subdirectory
  under `$VQ_WORKDIR`, prove it is writable with a real write, and fail loudly
  if any destination aliases a submitted source.
* **To request a code / example landing in the repo**, submit with
  `--tag pr-request` and include a clearly-named payload + a
  description in the spec's `--job-name`. The maintainer reviews
  terminal jobs with that tag and lands the worthwhile ones.
* **Size every calculation for one full node**: request the target
  node's entire core count with `--cpus` (vibe-qc parallelizes with
  OpenMP, not MPI), a *generous* walltime, and a *generous*
  `--mem-mb`. See § "Sizing a calculation" for the per-host table.

## Release updates are one driver-owned operation

Dev chats do not choose fleet versions and do not update shared hosts
individually after a release. Once the release chat has committed an accepted
machine report, the updater runs this from the configured scheduler driver:

```sh
vq admin rollout-latest --dry-run
vq admin rollout-latest
```

There is deliberately no version, tag, branch, or SHA argument. The accepted
report supplies exact component pins internally -- it is the sole source of
every deployed identity, including the driver's vq and the scheduler-side
helpers (staged from the pinned commit, never from whatever tree the live
driver checkout happens to hold). This gives the operator the
"latest released ecosystem" action without turning deployments into
moving-branch tracking. A deployed vq that has moved ahead of the accepted
pin is release drift: the rollout fails closed with the exact recovery
(normally: cut the next release so the report pins the deployed vq). The
command updates the driver vq first, re-enters
through it, then handles canonical scheduler helpers/runtimes and managed venv
hosts serially. Queue/campaign aliases do not duplicate builds, vq-only
coordinators never receive chemistry runtimes, and a temporary host or
scheduler hold is deferred without blocking independent hosts. Re-running
first takes the global fleet fence and reconciles durable operations and
journals from every report and host before collecting fresh live state. It
adopts an exact local supervisor after controller death and is a no-op for
lanes already at the accepted identity with `LAST OK=true`.

Multi-user update outcomes are authoritative only after the authenticated
daemon RPC records them. If that canonical write fails, `admin update` fails
and retains a failed marker; a per-user state-file fallback is not accepted as
fleet evidence. Diagnose daemon/token access and follow the runbook's marker
recovery path rather than using `mark-ok`.

When one canonical host has more than one pending non-driver action,
`rollout-latest` owns one outer dispatch bracket across the host sequence.
Ordinary venv hosts use a full update drain. For scheduler lanes, source
commit `7f65b8e58` protects the canonical exact target plus every transitively
resolved nonlocal alias, using each target's own configured
`scheduler_driver` control key. Excluded, unresolved, and local aliases are
not inferred. Direct `vq admin update` and a one-action scheduler rollout are
unchanged.

The complete exact target/control group is durable before the first control
call, and no protected action starts until all members are confirmed. Each
record binds target, canonical action host, control key, and deterministic
owner. Controller loss, outcome-unknown work, and a verified failed action
retain every group member until explicit reconciliation. Partial cleanup is
reported per exact target; never broad-release it to make the queue move.
`--only <canonical>` keeps the group, while selecting only an actionless alias
still fails closed.

Do not rename or repoint a target key, its canonical relationship, its control
key, or that control key's SSH routing while any rollout hold is pending.
Restore the recorded mapping and reconcile first. Semantic drift or malformed
journal identity fails before any control mutation, and an obsolete report's
plan-bound group raises an actionable fence rather than being guessed under a
new plan.

If a terminal failed run reaches finalization, `rollout-latest` emits exactly
one valid `vq.fleet.rollout_result/2` before exiting 1. Its additive
`retained_rollout_holds` block projects owned journal claims recorded as
`active` or `cleanup-failed`. Text mode sanitizes and promotes the same
evidence as `FAILED HOST` and `RETAINED ROLLOUT HOLD`, explicitly stating that
current liveness is not asserted. Verify live state before releasing anything.
The separate `preserved_external_holds` / `PRESERVED EXTERNAL HOLD` contract
is unchanged.

Source commit `8869ca33e` adds a separate final-only
`drain_liveness` observation to executing rollout results. It runs after the
durable journal is finalized and while the global rollout fence is still held.
Each distinct daemon endpoint is queried at most once, then projected onto the
exact configured targets. Read `inactive_hosts`, `active_holds`, and
`unknown_hosts` separately from the historical preserved-hold and
journal-retained-hold blocks. An `OBSERVED ACTIVE ... (final sweep as of
<time>)` line is true only at that timestamp.

The sweep uses a versioned, non-writing daemon snapshot of the legacy and
scheduler-lease stores under both locks. Old daemons, incomplete or malformed
data, busy locks, timeouts, and unsupported provenance become unknown; there
is no direct state-file fallback. Exact journal identity determines rollout
ownership before text sanitization, and unreadable scheduler-lease state is
reported as a safety-fail-closed hold. The sweep never releases a hold, does
not run for dry-run or verify-only, and cannot change rollout status, verdict,
or exit code. The rollout plan/result/verify schema IDs remain `/2`, `/2`, and
`/3`.

The final-observation commit itself did not repair acquisition. Later source
commit `7f65b8e58` closes the bounded outer-bracket alias gap above while
leaving plan/result/verify schemas, accepted-report checks, and final
observation semantics unchanged. It remains source-only until an accepted
report carries it. No live host, root daemon, scheduler, drain, or fleet
operation exercised it. Verification passed 243 owned tests, an independent
509-test matrix, and full vq with `5,861 passed, 12 skipped in 263.29s`; full
Ruff, compileall, and diff checks were clean, and the independent security
review was CLEAR.

The durable local path uses a launcher plus a fresh-session execution recorder.
Its `python -m vq ...` outer child inherits the recorder's isolated process
group. The recorder stores at most 4 MiB of combined output while continuously
draining later bytes; `vq admin logs` remains the complete transcript. If the
recorder or child launch fails after activation, the missing local result is
outcome-unknown and non-replayable.

R4b.1 commit `1483c980e` additionally binds a scheduler-runtime rollout
action that uses `detached_build = true` with a fixed `update_host`. It proves
the exact live outer operation, persists one create-once local command binding,
and uses a fixed embedded Python helper to execute exact argv without shell
interpretation in a random owner-only remote namespace. Bounded receipts and
at most 4 MiB of retained output stay on the build host. A lost launch or poll
response can observe only that exact run and cannot replay it.

This does not make a remote return code of zero the rollout verdict. It means
only `deploy-completed-unverified`; independent login-host verification, LAST
OK, and marker finalization remain authoritative. If the recorder or outer
child dies, global reconciliation still blocks and does not positively query
the remote receipt. Scheduler-helper actions, Slurm allocation builds, and
manual commands without the validated outer context remain outside this
increment. It is source-only: no accepted report carries it, and no live host,
root daemon, scheduler, drain, or fleet operation exercised it. Focused
evidence: `107 focused and 373 adjacent tests passed; independent review passed 384 focused/adjacent tests`. Complete vq suite:
`5,776 passed and 12 skipped in 181.79s`. Static, diff, and independent review:
`full Ruff, Python compilation, git diff checks passed; independent review CLEAR`.

On failure, read the transcript named by the error before retrying:

```sh
vq admin logs TARGET --host HOST
```

Do not use `--force` to work around active jobs, a live updater, an external
PBS/SLURM stop, or a topology error. The fleet runbook owns those recovery
paths.

Scheduler runtime cache reuse is expected to skip vendored Libint, Libxc,
spglib, FFTW, and libecpint when the prior healthy runtime has a compatible
build stamp and toolchain. On Linux, the deployer bridges validated
`lib64/cmake/ecpint` metadata to the legacy `lib/cmake/ecpint` lookup used by
older immutable releases. A transcript that says the cache was reused and
then starts configuring libecpint is therefore a cache-handoff defect, not an
ordinary long build; stop at the next action boundary and diagnose it before
allowing another native lane to rebuild.

## Why this exists

On 2026-05-25 a deployment of `vibeqc-dev` to compute-d and compute-a
failed because chats had checked experimental work directly into
`/home/USER/gitlab/vibeqc-dev/`:

* compute-d had untracked `examples/periodic/` (108 files) and
  `examples/experimental_regression/` shadowing upstream paths,
  blocking `git pull` with "untracked working tree files would be
  overwritten."
* compute-a had 141 modified `python/vibeqc/basis_library/basis/*.g94`
  files (basissetdev citation comments) and a deleted `vdzp.ecp`,
  causing `scripts/update.sh` to abort with "working tree has
  uncommitted changes."

Both cost an hour of manual git-archaeology to untangle. v0.6.54
ships the **mechanism** (per-job workdir, payload submit, request
convention) so this doesn't recur. This doc is the **policy**
that goes with it.

### GFN2 cache staging for offline runtimes (#127)

Treat parameter-cache staging as a deployment prerequisite. The operator uses
`agentic-loop/fleet/seed-gfn2-cache.py` with a privately fetched source cache,
its reviewed file SHA256 and upstream source SHA256. The default is a plan;
`--apply` writes a separate directory named by the hash and never replaces
the existing default cache. Keep the JSON receipt with rollout evidence.
The full command is documented in `docs/user_guide/reference_data.md`.

New GFN2 jobs must set `VIBEQC_GFN2_CACHE_DIR` to the receipt's directory
before importing vibeqc. A running job retains its original environment and
cache. The staging command changes no program configuration and performs no
calculation; automatic wiring into every runtime roll remains an open #127
gate. Offline molecular and periodic compute-node results and their manifest
lineage are required for independent acceptance. Do not put parameter data
in the source repository or enable a fallback to an unpinned mutable cache.

## Submitting work

### Single-file Python script

```sh
vq submit compute-d my_experiment.py
```

The daemon copies `my_experiment.py` into a fresh workspace at
`/var/lib/vq/users/<your-uid>/jobs/<jobid>/`, runs it with the
host's default Python (override with `--python /path/to/python`
or `--branch <branch-name>`), and captures stdout/stderr to
`stdout.log` / `stderr.log` next to it. On a daemonless **scheduler**
host (pbs-cluster, slurm-cluster) there is no such default: `--python` is required
for a single-file submit, because the only interpreter vq could infer
is the driver's own. See the scheduler note further down.

### Single-file QVF calculation container

```sh
vq submit HOST job.qvf \
  --program vibeqc-dev
```

A `.qvf` positional input is not a Python script. vq records it as a
first-class QVF payload, resolves the named managed program, and invokes the
installed vibe-qc CLI as `python -m vibeqc._cli run job.qvf`, which is the
module form of `vibeqc run job.qvf`. It never falls through to `python job.qvf`
and never executes `run.record.input`.

For a local managed runtime, vq snapshots the full Git SHA automatically for
provenance. When neither `--expected-sha` nor a configured
`expected_git_sha` is present, that observed SHA is not enforced and the queued
job may cross a runtime rollout. Use `--expected-sha FULL_SHA`, or configure
`expected_git_sha`, when the local checkout must match at both submission and
dispatch. For a scheduler-target submit, the target-side runtime identity is
authoritative rather than any same-named checkout on the driver; the wrapper
contract is described below.

On a daemon host, the exact interpreter from `[programs.NAME]` is used. On a
scheduler host, the target's
`scheduler_program_hooks.NAME.command_wrapper` is required and selects the
immutable compute-node runtime; vq invokes `vibeqc._cli` through that Python
wrapper. Do not pass `--python` or `--branch` for a QVF submit. A settled
container is refused by default; use `--qvf-force` only when you deliberately
want another sequenced `run.record`.

Fetch only the updated container, leaving queue logs and metadata server-side:

```sh
vq fetch HOST JOBID --name job.qvf -o results/
```

This publishes `results/job.qvf` atomically. A retry is idempotent only when
the existing file is byte-identical; vq never overwrites a different result.

### Directory of files

```sh
vq submit compute-d -d my-experiment-dir/ -- python run.py --opt 12
```

Whole directory tree copied into the workspace, then your explicit
command runs from there.

For the compatibility form of a QVF job, keep generated sidecars in the
per-job scratch directory so the submitted workspace's only user artifact is
the updated container:

```sh
vq submit HOST -d jobdir/ --program vibeqc-dev \
  --expected-sha 0123456789abcdef0123456789abcdef01234567 \
  -- bash -lc 'exec "$VQ_PROGRAM_BIN/vibeqc" run job.qvf \
      --output "$VQ_WORKDIR/job"'
```

Use the first-class single-file form above when possible. Scheduler hosts do
not expose driver-local `VQ_PROGRAM_BIN`; their configured program wrapper
must launch the equivalent `-m vibeqc run` command.

### Tarball

```sh
vq submit compute-d -c my-experiment.tar.gz -- bash run.sh
```

Tarball extracted into the workspace (via `tarfile` data filter,
no symlink escape). Useful for large multi-file payloads where the
directory copy would be slow.

### Rebuild the env before your job runs (`--refresh`, v0.11.0)

```sh
vq submit private-host --refresh vibeqc-dev my_experiment.py
```

On a private or explicitly isolated host, `--refresh <env>` names a
`[programs.<env>]` venv-env (the same name `vq admin update <env>`
takes, e.g. `vibeqc-dev`). When this job reaches the front of the
queue, the daemon **drains the host** (lets running jobs finish, holds
new dispatch), runs `git pull` + the env's update_script, then
dispatches your job against the freshly-rebuilt venv.

* **Drop-proof:** the daemon owns the rebuild, so your client can
  disconnect the moment the submit returns -- unlike a client-connected
  `vq admin update`, a flaky link can't interrupt it.
* **Safe by construction:** if the rebuild fails, your job lands in
  `FAILED` with a `failure_reason` naming the env -- it never runs
  against a half-built env, and a failing refresh can't wedge the queue.
* **You own the choice on an isolated host:** the rebuild only happens
  because you asked for it on this job.
* Single-file submit only in v1 -- not `--array` / `--chain` (submit a
  standalone `--refresh` job first, then the array/chain).

Do not use `--refresh` against the shared release fleet. Shared hosts settle at
accepted releases so calculations have one reproducible identity; a dev chat
pulling one host to a moving tip recreates helper/runtime drift. If a landed
fix is needed for calculations, cut the next release and let
`rollout-latest` reconcile the fleet, then resubmit the failed calculation.

### Auto-place by memory (`vq submit auto`, v0.11.0)

```sh
vq submit auto my_experiment.py            # queue picks the host
vq submit auto --mem-mb 32000 big_run.py   # needs a host with >=32 GB free
vq submit auto --pool compute my.py        # restrict to the 'compute' pool
```

Don't care which host runs it? `auto` in the host slot lets the queue
choose. QC jobs are **memory-bound** -- an idle-CPU box with too little free
RAM will OOM a big calculation -- so it sweeps every configured host's `vq
overview` and matches your job's memory requirement against each host's
**live free RAM** first (the OOM guard), then prefers a host where your
`--cpus` also fit (so the job dispatches now instead of queueing), and
submits there -- printing `vq submit auto → <host>` to stderr. The memory
target comes from `--mem-mb`, or -- for a single-file vibe-qc `.py` job when
`estimate_python` is set in the config -- vibe-qc's own peak-memory estimate
(the submit host runs the job's dry-run once to read it, printing `→ <host>
(≈N MB est.)`); with neither it falls back to core headroom.

* Skips hosts that can't take the job: unreachable, `vq host down`,
  drained, or dead-daemon. Errors clearly (pointing at `vq overview
  --all`) if none qualify.
* Best-effort: a snapshot at submit time, not a reservation -- free RAM can
  shift after. For a hard requirement, name the host yourself.
* Scope placement with `--pool <name>` (a `[pools.<name>]` group in the
  config), or set `default_pool` so a bare `vq submit auto` already
  excludes the daily-driver / gaming boxes. Ad-hoc, `vq host down <host>`
  also drops a box until `vq host up`.

Equivalent to `vq submit $(vq overview --recommend) my.py`, in one
command and passing your job's cpus + memory.

### Sizing a calculation: one node, all its cores, generous walltime + memory (maintainer policy, 2026-08-06)

Undersized requests are the leading cause of walltime kills and OOM
kills in the fleet, and they waste the OpenMP parallelism the code is
built around. The standing policy for every chat submitting
calculations:

* **One node, max cores per node.** vibe-qc parallelizes with OpenMP
  inside one shared-memory node -- it does not span nodes with MPI.
  Every calculation therefore runs on exactly one node and should
  request that node's **full core count** with `--cpus` so OpenMP
  usage is maximized. vq propagates the declared count into
  `OMP_NUM_THREADS` and the BLAS siblings automatically (see
  § "Resource environment"). Do not submit deliberately small jobs
  onto big nodes "to be polite" -- a 4-core request on a 96-core
  slurm-cluster node strands 92 cores behind your allocation.

* **Walltime: generous, never tight.** vibe-qc is currently slow and
  inefficient relative to the mature codes it is compared against --
  do not scale wall estimates from CRYSTAL / ORCA experience, and do
  not trust an optimistic first guess. Take your best estimate and
  multiply by 3-5x. An over-declared wall costs a little backfill
  priority; an under-declared one kills the job near the end and
  wastes the entire allocation (`terminal_diagnosis` category
  `scheduler_walltime`, hint `increase_walltime`, exists because this
  keeps happening). On slurm-cluster, a submit without `--time` defaults to
  the partition maximum (24 h on `intelsr_medium`) -- that default is
  fine for production jobs; cap tighter only for smoke tests.

* **Memory: declare generously -- the nodes are big.** QC jobs are
  memory-bound. The fleet has RAM to spare: pbs-cluster's big-memory nodes
  carry 252-504 GB and slurm-cluster's nodes ~1 TB each. Once you hold all
  cores of a node its RAM is yours anyway, so declare what the
  calculation could realistically peak at, not a hopeful minimum --
  in-core algorithm paths beat disk-thrashing ones. (`--mem-mb`
  remains mandatory on compute-a; see the cap rule below.)

* **Dev chats: OpenMP scaling is a deliverable.** The code must be
  optimized for OpenMP throughout, and periodic calculations must
  parallelize over k-points -- k-point work distributes almost
  perfectly and full-node jobs are expected to exploit it. If a
  full-node run shows poor OpenMP scaling, that is a performance bug:
  report it to the owning dev chat (or file it) -- do not shrink the
  core request to hide it.

Per-target sizing table (nodes probed live 2026-08-06 via
`pbsnodes` on pbs-cluster and `sinfo`/`scontrol` on slurm-cluster -- re-probe if
this looks stale):

| Target | `--cpus` | Node RAM | Notes |
|---|---|---|---|
| compute-d | 32 | 125 GB | daemon host, no scheduler wall |
| compute-a | 12 | 62 GB | daemon host; 48 GB job-memory cap |
| compute-b | 16 | 49,340 MB admission cap | daemon configuration rechecked 2026-09-07; route larger jobs elsewhere |
| compute-c | 6 | 31 GB | daemon host |
| pbs-cluster (= `atokat` queue) | 20 | ~94 GB | Torque, ~24 nodes |
| pbs-cluster-itwin | 12 | ~94 GB | 8 nodes |
| pbs-cluster-jtwin | 12 | 252 GB | 8 nodes |
| pbs-cluster-big | 48-64 | 504 GB | 2 nodes (64c + 48c); request 48 unless you need a specific node |
| pbs-cluster-amd | 64-128 | 504 GB | mixed 64c/128c nodes; request 64 unless you target the 128c node |
| slurm-cluster / slurm-cluster-campaign | 96 | ~1000 GB | SLURM `intelsr_medium`, 192 nodes, wall default = max = 24 h |

compute-b's physical memory is not its job admission limit. Its live daemon caps
the lane at 16 CPUs and 49,340 MB; the previous 32-core table entry was stale
(#695). Select a destination that can satisfy the calculation's actual resource
requirement before submitting. Do not shrink a periodic leg's request to fit
compute-b: a leg needing more memory belongs on a ready larger-memory target, or
must remain unsubmitted until one is available. In `vq queue --active --json`,
`pending_over_capacity: true` means the existing job cannot fit the configured
target; it is not evidence that the job is running or will eventually start.

pbs-cluster's Torque queues publish no queue-level walltime maximum --
declare a generous `--time` yourself rather than relying on a
scheduler default.

### Declare your memory -- undeclared jobs are capped (compute-a, 2026-07-25)

Always pass a realistic `--mem-mb` with your submit. On compute-a's
multi-user daemon a job that does not declare memory is **assumed to
need 4000 MB and is cgroup-capped there** -- a heavyweight calculation
(periodic GDF, large correlated runs) submitted without `--mem-mb`
will be OOM-killed at 4 GB instead of swamping the box. This is
deliberate: the 2026-06-21 compute-a swap-death came from ~1100
memory-undeclared jobs running uncapped. Declared memory is honored
up to the host cap (compute-a: 48 000 MB across all concurrent jobs, 12
CPUs). If your job genuinely needs more than the host cap, it belongs
on a bigger host (`vq submit auto --mem-mb ...`).

### Knowing whether your submit landed (`--json`, v0.12.1)

`vq submit` prints the bare 12-hex jobid on stdout and nothing else -- that
contract is unchanged, and every wrapper in the fleet depends on it. But a bare
id cannot tell you whether the job is about to run or parked indefinitely, so
for scripted / agent use ask for the receipt instead:

```sh
vq submit compute-a --json my_experiment.py
```

```json
{
  "jobids": ["a1b2c3d4e5f6"],
  "host": "compute-a",
  "acceptance_scope": "queue",
  "execution_status": "not_observed",
  "capacity_warnings": [],
  "dispatch_holds": ["a full drain is active (`vq drain --status`)"],
  "next": ["vq status compute-a a1b2c3d4e5f6", "vq logs compute-a a1b2c3d4e5f6 -f"]
}
```

`dispatch_holds` is the useful part: it names, **at submit time**, anything
that will keep your job PENDING -- an active drain, a scheduler lane holding
that target, an admin update mid-rebuild. Previously you had to poll and guess.
An empty list means nothing queue-side is holding it, and PENDING is just
capacity or dependencies.

The receipt proves queue acceptance. It does not observe subsequent workspace
staging, scheduler acceptance or execution. `acceptance_scope` and
`execution_status` make this distinction explicit for automated callers. Use
`--wait-submitted` on scheduler targets when the submit command must report
staging or scheduler-submission failures without waiting for the calculation:

```bash
vq submit slurm-cluster -d payload --wait-submitted --submission-timeout 120 --json -- bash run.sh
```

Success then carries `acceptance_scope: scheduler`,
`execution_status: scheduler_accepted` and one `scheduler_acceptance` observation
per job, including its PBS/SLURM ID. This proves submission, not that a compute
node has started the job or that its calculation succeeded. Local `running`
during staging is insufficient. A recorded terminal failure exits 1 and names
its failure reason. The client polls the configured queue driver, including
remote drivers; it does not replay qsub/sbatch.

The timeout is **one budget for the whole array or chain**, default 60 seconds.
Exit 124 means acceptance was not observed within that budget; exit 130 means
the wait was interrupted. In either case the receipt retains all job IDs and
`retry_safe: false`: inspect those IDs, do not submit duplicates. A drain,
capacity limit or dependency can legitimately keep a job queued past the
deadline. Accepted elements of a mixed batch remain identified in its receipt.
Plain stdout remains just the job IDs, with diagnostics on stderr.

`--enqueue-only` explicitly selects the immediate queue-acceptance behavior
(currently the default). Use `--wait` for a terminal calculation verdict;
it can follow successful `--wait-submitted` but has its own unbounded wait.
`--fetch-on-done` likewise waits for completion. The acceptance timeout never
cancels, removes, or automatically retries any queued job.

Scheduler archive uploads retry transient transport failures twice, with
backoff and fresh SSH connections, before failing with an attempt count.
These retries resend the same archive before unpacking; they never replay a
scheduler submission command.

`capacity_warnings` is independent of `dispatch_holds`. A nonempty list means
the accepted local job exceeds the receiving daemon's configured CPU or
effective-memory cap and will remain PENDING until that cap changes or the job
is resubmitted smaller. The same field carries warnings forwarded from an
ordinary remote daemon. Treat it as part of acceptance: do not record an
unqualified `OK` while dropping it. `vq list --json` and `vq status --json`
then expose `pending_over_capacity` and structured
`configured_capacity_overages`; `vq overview --json` reports the aggregate
`over_capacity_pending_jobs` alarm count. The first field is tri-state:
`true` means a known overage, `false` means a pending local request is known
to fit the advertised base caps, and `null` means the snapshot is unavailable
or classification does not apply. An older constrained-memory snapshot that
cannot report the daemon's undeclared-memory charge also stays `null`. The
overview count is likewise `null` when the queue or capacity snapshot is
unavailable or incomplete.

Capacity warnings always print to stderr, with or without `--json`. The
unflagged receipt's courtesy host, dispatch-hold, and next-command narration
prints only at an interactive TTY; non-interactive callers use `--json` for
that context. Stdout without the flag remains exactly the clean job ID.

### Why is my job still PENDING?

`vq status HOST JOBID` on a pending job now names every dispatch gate it can
prove is holding it -- a full or scheduler-lane drain, an in-flight admin
update, an unmet `--depends-on`, a `--refresh` build still running, a
scheduled-submit `not_before`, or a capacity cap (jobs / CPUs / memory). They
**stack**: a job can be behind several at once, and clearing one leaves it
parked on the next, so all of them are reported.

```sh
vq status compute-a a1b2c3d4e5f6 --json | jq .pending_blockers
```

An **empty** list means "nothing provable is holding it" -- not "about to run".
A few gates are daemon-internal (host-memory-pressure pause, a per-user quota
under a queue dir this client cannot read) and cannot be seen from outside the
daemon; those are omitted rather than guessed. If the list is empty and the job
still will not run, it is either one of those or simply waiting its turn -- check
`queue position` in the same output.

### Reading results back

```sh
vq status compute-d <jobid>                       # spec + tail of stdout/stderr
vq status compute-d <jobid> --json                # machine-readable monitor state
vq logs compute-d <jobid>                         # full stdout + stderr (both streams)
vq logs compute-d <jobid> --tail 200 --json       # machine-readable log tail
vq logs compute-d <jobid> -f                      # tail-follow until terminal
vq output compute-d <jobid>                       # canonical vibe-qc .out tail
vq output compute-d <jobid> -f                    # follow the .out file
vq progress compute-d <jobid>                     # compact SCF iteration table
vq progress compute-d <jobid> -f                  # follow structured SCF rows
vq tail compute-d <jobid> --name calc.out -f      # follow an arbitrary workspace file
vq tail compute-d <jobid> --name calc.out -n 200 --json  # machine-readable file tail
vq fetch compute-d <jobid> -o ~/Downloads/        # workspace back to laptop
vq fetch compute-d <jobid> -o ~/Downloads/ --json # machine-readable fetch result
vq fetch compute-d <jobid> --name job.qvf -o ~/Downloads/ # one artifact only
vq fetch <local-daemon-host> <jobid> --workdir -o ~/Downloads/  # managed workdir only
vq fetch-all compute-d -o ~/Downloads/            # v0.12.0: ALL terminal jobs at once
vq fetch-all --all-hosts -o ~/Downloads/        # v0.12.0: every configured host at once
```

`vq fetch` tars and copies the **workspace** back. If a local daemon job has
a separate workdir, this implicit selection exits with an explicit
`--workdir` hint after copying the workspace, so an inputs-only tree is not
reported as artifact retrieval. Use `--workspace` to explicitly request only
the submitted workspace and queue logs. Terminal local-job receipts from
older remotes without workdir metadata also require this explicit selection.
`--workspace`, `--workdir`, and `--name` are mutually exclusive.

For local daemon jobs, the
separately managed **workdir** is retrieved via `vq fetch --workdir` (v0.7.7
*Cerf's Datagram*). It uses the same streaming-tar shape and lands at
`<output-dir>/<jobname>-<jobid>-workdir/`, so workspace and workdir fetches of
the same job can coexist under one `-o DIR` without name collision. Scheduler
jobs have no separately managed workdir to fetch; their runtime `$VQ_WORKDIR`
is the shared workspace returned by ordinary `vq fetch`.

An older remote without `mark-fetched` does not turn a successful explicit
workspace transfer into failure: vq warns that `last_fetched_at` was not
recorded and requests a remote update. Other acknowledgement failures still
exit nonzero while retaining the successfully copied tree and its freshness
manifest.

`vq fetch --name BASENAME` is the artifact-only path. It copies one regular
workspace file or complete directory directly into `-o DIR`. It adds no queue
logs, `_vq` metadata, sibling calculation sidecars, or diagnosis sidecar.
Names are one basename, not paths, so traversal and symlink escapes are
rejected. JSON results use `kind: "artifact"` and include the requested
`name`.

For opt-in TREXIO HDF5/text output, runtime checks and staged READ inputs, see
[TREXIO export and READ through vq](trexio_queue.md).

For one file in the recorded scratch workdir, combine `--workdir` and `--name`.
If it is nested, pass the directory separately with `--subdir`:

```sh
vq fetch compute-b JOBID --workdir --subdir results --name kernel.json -o evidence/
vq fetch compute-b JOBID --workdir --subdir results --name pair-tests.xml -o evidence/
vq fetch compute-b JOBID --workdir --subdir results --name provenance.json -o evidence/
```

These commands copy only the selected file or directory directly into `evidence/`.
They do not transfer sibling source/build trees or refresh a scheduler workspace.
`--name` still accepts only a basename. `--subdir` requires `--workdir --name`
and accepts relative directory components; absolute paths, `.`/`..`, and
symlinks in the selected directory/file chain are refused. A missing or swept
workdir never falls back to a workspace or its archive. JSON receipts record
`source_kind` and `source_subdir`; the caller must retain the job identity and
fetch any additional provenance files explicitly. Both client and remote queue
host need this capability. An older remote refuses the new options, preserving
any existing destination artifact; there is no fallback to a full-tree transfer.

With `--json`, `vq fetch` prints a single object instead of the human
`fetched -> ...` line:

```json
{
  "destination": "/tmp/results/abc123",
  "fetched_at": "2026-08-17T14:03:11+00:00",
  "jobid": "abc123",
  "kind": "workspace",
  "stale": false,
  "queue_handle": {
    "job_id": "abc123",
    "host": "compute-d",
    "submitted_at": "2026-07-02T10:00:00+00:00"
  }
}
```

`kind` is `"workspace"` or `"workdir"`. `queue_handle` uses the same shape as
status, queue, logs, tail, wait, and top JSON; remote fetches may report
`submitted_at: null` when the client did not read a local spec.
`fetched_at` / `stale` mirror the fetch manifest described below; both are
`null` / `false` for `--name` artifact fetches, which write no sidecars.

### Fetch freshness: `_vq/fetch-manifest.json` (issues #111 / #114 / #294)

Every fetched workspace and workdir carries
`_vq/fetch-manifest.json`, so a consumer can age-check the payload
**without trusting the CLI's exit code or its `fetched -> ...` line**:

```json
{
  "schema": "vq.fetch-manifest.v1",
  "jobid": "abc123",
  "job_name": "rp218-bipole",
  "fetched_at": "2026-08-17T14:03:11+00:00",
  "refresh_attempted_at": "2026-08-17T14:03:11+00:00",
  "source_host": "pbs-cluster",
  "source_kind": "workspace",
  "source_path": null,
  "transport": "ssh-stream",
  "stale": false,
  "refresh_error": null
}
```

The contract:

* **A re-fetch refreshes.** `vq fetch -o DIR` into a directory that already
  holds a previous fetch of the *same* job replaces its contents with the
  current snapshot and advances `fetched_at`. It no longer returns the old
  bytes while printing `fetched`. Files deleted at the source disappear from
  the destination.
* **A fetch that cannot refresh fails loudly.** A transport failure exits
  non-zero, and additionally stamps the previous snapshot with
  `stale: true` plus `refresh_error`, because some other process will read
  those bytes later without ever having seen the exit code.
* **A fetch without valid freshness metadata fails loudly.** `vq fetch`, each
  `vq fetch-all` row, and `vq submit --fetch-on-done` refuse to report a
  workspace or workdir as fetched unless its `vq.fetch-manifest.v1` sidecar
  matches the requested job and tree kind, has aware timestamps and transport
  provenance, and is explicitly fresh. A single artifact fetched with
  `--name` is the documented sidecar-free exception.
* **`stale: true` means do not reason about this tree.** Re-fetch it. A
  successful refresh clears the flag.

Automated watchers should treat `fetched_at` as the age of the evidence:
results-presence is the loop's mandated terminal-detection method precisely
because scheduler state is unreliable, so a frozen file must be
distinguishable from a stale read.

Fetched workspace and workdir directories also include
`_vq/terminal-diagnosis.json`, generated from the queue spec at fetch time.
It records the raw state/exit/scheduler fields plus the same nullable
`terminal_diagnosis` object exposed by `vq status --json`, so archived
artifacts remain self-describing even when the calculation produced only a
partial stdout and no `.system` or `.qvf`.

If the spec lacks a workdir (pre-v0.6.54 jobs) or the workdir was
swept by `--clean-tmp` + terminal state, `vq fetch --workdir` errors
with a named cause so you know whether to re-submit without
`--clean-tmp` next time.

Scheduler jobs do not persist a local `JobSpec.workdir`, so
`vq fetch HOST JOBID --workdir` reports that no separately managed local
workdir exists. Their runtime `$VQ_WORKDIR` is the shared scheduler workspace.
When `node_scratch_dir` is unset, the command runs in that same directory.
When it is set, the command runs in a temporary node-local copy and copies its
result tree back to the shared workspace on normal exit. In both cases,
`vq fetch HOST JOBID -o DIR` is the intended artifact fetch.
After the host's cleanup policy reaches archive age, vq removes the duplicate
remote scheduler workspace and stamps `scheduler_remote_workspace_cleaned_at`
on the spec. If that remote cleanup fails, later cleanup sweeps retry it, and
final delete keeps the spec until the remote path has either been cleaned or is
confirmed harmlessly absent.

For a whole batch, **`vq fetch-all HOST`** (v0.12.0 *Hollerith's
Return*) returns every terminal job's workspace in one command, the
bulk companion to per-job `vq fetch`. Run it from your submitting
folder (the default `-o` is the current dir) and the outputs land
there, one `<jobname>-<jobid>/` subdir per job. It is idempotent: a
job whose destination already exists is skipped, so re-running pulls
only what finished since the last run. `-s STATE` narrows to specific
terminal states (e.g. `-s completed`). This is the fleet answer to
"PBS copies everything to scratch, how do I get my files back": one
sweep brings the whole `--array` or AICCM batch home.

Per-job `vq fetch HOST JOBID` retries **refresh** the destination when it is a
prior vq fetch of that same job, as proven by its
`_vq/terminal-diagnosis.json` sidecar; see the fetch-freshness contract
above. An unrelated directory, a partial or legacy unmarked directory, a
symlink, or a destination marked for a different job remains a hard collision;
vq does not overwrite it. `vq fetch-all` keeps the skip-if-present sweep
semantics instead, and reports each skip explicitly.

For a single job you can fold the return into submission:
`vq submit HOST job.py --fetch-on-done` implies `--wait` and pulls
that job back when it finishes. It blocks until then, so it suits an
interactive one-off, not a long batch (for those, submit normally and
`vq fetch-all` later).

### vq never reads your stdin (issue #118)

Every vq subcommand runs its ssh children with stdin closed, so a shell
read-loop over a job list is safe:

```sh
while IFS=$'\t' read -r host jobid rest; do
    vq status "$host" "$jobid" --json > "status-$jobid.json"
done < jobs.tsv
```

Before this was fixed, `ssh` inherited the loop's stdin, which *was*
`jobs.tsv`, and swallowed the remaining rows: the loop processed the FIRST
row and exited 0, looking complete. A measured 28-row poll produced 9 files.
The `< /dev/null` workaround on each vq call is no longer needed (it remains
harmless). Verify a poll's output count against its input count anyway; that
is cheap and catches the next variant of this.

### Live monitoring contract for cockpit clients

Tools such as vibe-view can discover jobs with `vq queue HOST --json` and
then poll `vq status HOST JOBID --json`. Queue JSON rows include every
`JobSpec` field plus `effective_state`, `scheduler_running_confirmed`,
`queue_handle`, and `terminal_diagnosis`; status JSON carries the same state
projection and adds the live monitor fields below.

For an interactive lookup that has only a job ID, hostless `vq status JOBID`
can discover a unique owner from durable queue rows even when the configured
remote default is dead but not yet marked down. That discovery includes
archived rows and scheduler lanes and never uses status itself as the probe.
Automation that already has a queue row or ledger record should continue to
poll `vq status HOST JOBID --json` with its stored `queue_handle.host`: the
explicit owner avoids fleet discovery latency and remains authoritative if
other hosts are unreachable or a duplicate ID makes hostless discovery
ambiguous.

Other hostless per-job verbs intentionally do not inherit `status`'s
fleet-wide owner discovery while a remote default is merely unmarked. They
first read only that default's exact action queue authority with a bounded
probe. If no trusted listing returns, `vq` refuses before invoking logs,
control, wait, or fetch and asks for an explicit `HOST`. A trusted listing is
reachability evidence only: found, absent, and duplicate results all leave the
configured default selected. An administratively down default still uses
Baran's Detour; an explicit `HOST` and a local default retain their direct
paths.

For fleet-wide discovery, `vq queue --all --json` returns one top-level object
keyed by host name, with each value holding that host's queue rows or an
`error` object for an unreachable host.

The status JSON object is additive and keeps every `JobSpec` field at top
level; the monitor-relevant stable fields are:

```json
{
  "id": "abc123",
  "state": "running",
  "effective_state": "running",
  "scheduler_running_confirmed": null,
  "submitted_at": "2026-07-02T10:00:00+00:00",
  "started_at": "2026-07-02T10:01:00+00:00",
  "wall_elapsed_seconds": 120.0,
  "active_elapsed_seconds": 95.0,
  "paused_current_seconds": null,
  "paused_effective_seconds": 25.0,
  "active_walltime_percent": 3,
  "scheduler_walltime_used_seconds": null,
  "scheduler_walltime_limit_seconds": null,
  "scheduler_walltime_percent": null,
  "scheduler_walltime_remaining_seconds": null,
  "terminal_diagnosis": null,
  "queue_handle": {
    "job_id": "abc123",
    "host": "compute-d",
    "submitted_at": "2026-07-02T10:00:00+00:00"
  },
  "progress": {
    "source": "checkpoint_qvf",
    "run_status": "running",
    "seq": 7,
    "wall_time_s": 12.5,
    "written_at": "2026-07-02T10:03:00Z",
    "scf_iteration": 4,
    "energy_eh": -75.98
  },
  "qvf_lifecycle": {
    "artifact_name": "job.qvf",
    "run_status": "converged",
    "sequence": 0,
    "run_record_complete": true,
    "terminal_complete": true,
    "queue_terminal": true,
    "chemistry_failed": false,
    "queue_process_failed": false,
    "done": true,
    "outcome": "converged"
  },
  "runtime_workdir": "/var/lib/vq/users/UID/workdirs/abc123",
  "workdir": "/var/lib/vq/users/UID/workdirs/abc123",
  "checkpoint_qvf_filename": "checkpoint.qvf",
  "checkpoint_qvf_path": "/var/lib/vq/users/UID/workdirs/abc123/checkpoint.qvf",
  "checkpoint_qvf_exists": false,
  "stdout": "...",
  "stderr": "..."
}
```

`state`, `submitted_at`, and `started_at` are the raw vq lifecycle fields.
For scheduler-backed jobs, `state: "running"` means vq has handed the job to
the batch scheduler. `effective_state` is the monitor-facing state: it remains
`"queued"`, `"held"`, or `"unpolled"` while the scheduler has not reported
execution, becomes `"running"` once the scheduler reports execution, and
reports failure/fence phases such as `"poll_failed"`, `"finishing"`,
`"marker_probe_failed"`, or `"fetch_failed"` exactly. An unrecognized raw
phase projects to `"scheduler_unknown"`; the original remains in
`scheduler_state`.

A live raw `state: "running"` or `"suspended"` row with exact
`scheduler_state: "held"` projects to `effective_state: "held"`, matches
`vq queue -s held`, and remains in `--active`. A vq-issued hold normally has
raw `state: "suspended"` and `scheduler_running_confirmed: null`. A hold first
observed outside vq can retain raw `state: "running"`; it also remains in
`-s running`, reports `scheduler_running_confirmed: false`, and must continue
to reserve capacity. A terminal row instead keeps its terminal effective state,
even when `held` was its last scheduler observation.

`scheduler_running_confirmed` is tri-state. `true` means the exact last stored
scheduler phase was `running`; `false` means the row is a scheduler-owned raw
`RUNNING` reservation without that confirmation and must not be treated as
idle; `null` means the predicate does not apply. A true value is not a
freshness guarantee. For the same fail-closed reason, `vq queue -s running`
retains all raw scheduler lifecycle `RUNNING` reservations and qualifies the
text total as confirmed versus owned. Query exact phases with filters such as
`-s poll_failed`; use `--active` for all non-terminal rows.
`queue_handle` is the stable back-reference a cockpit can cache or
copy into result metadata; for scheduler-backed jobs the `host` is the
scheduler target rather than the driver. Remote queue listings rewrite
ordinary `localhost` handles to the host alias the operator requested, so
clients do not need to infer where a row came from. `progress` is populated from
`$VQ_WORKDIR/checkpoint.qvf` when that
QVF's `manifest.json` carries `provenance.checkpoint`; otherwise it is `null`.
The first live producer is vibe-qc's QVF checkpointer, which reports checkpoint
sequence, wall time, optional SCF iteration, optional energy, and
`provenance.run_status`. Clients should compute elapsed wall time from
`started_at` when no checkpoint progress is available and must tolerate
`progress: null`.

For a first-class single-QVF job, `qvf_lifecycle` is non-null. `done` requires
both a terminal queue state and QVF `converged`/`failed` provenance with a
complete sequenced `run.record`; a terminal QVF without its input and full
(possibly empty) log is not done. `chemistry_failed: true` means the
application produced a complete failed container. `queue_process_failed:
true` means vq saw a process/transport/protocol failure without such a
complete chemistry record. This distinction lets an agent retry queue
infrastructure failures without blindly retrying deterministic chemistry.

`wall_elapsed_seconds` is clock time from vq start/handoff to finish or the
current poll. `active_elapsed_seconds` subtracts vq pause/admin-update time;
`paused_current_seconds` and `paused_effective_seconds` expose that pause time
separately. `active_walltime_percent` compares active elapsed time to the vq
`wall_time_seconds` request when one exists. Scheduler-backed jobs also expose
parsed scheduler walltime numbers from detailed scheduler telemetry (PBS/Torque
`qstat -f`, SLURM `sacct`, or the host dialect equivalent):
`scheduler_walltime_used_seconds`, `scheduler_walltime_limit_seconds`,
`scheduler_walltime_percent`, and `scheduler_walltime_remaining_seconds`.
These fields are nullable when the underlying timestamps or scheduler detail
are not known yet. vq accepts both `HH:MM:SS` and SLURM-style `D-HH:MM:SS`
walltime strings before converting them to seconds.

For terminal jobs, `terminal_diagnosis` is a derived object that packages the
release-paper interpretation rules without changing the raw `state`. It
includes `category`, `action_hint`, `summary`, and when available `signal`,
`reason`, and `exit_code_description`. Examples: scheduler walltime kills use
`category="scheduler_walltime"` and `action_hint="increase_walltime"`;
`failed` with exit `137` uses `category="sigkill"` and
`action_hint="increase_memory_or_check_external_kill"`; scheduler jobs that
finish without an exit marker use `category="scheduler_missing_exit_marker"`
and `action_hint="queue_diagnostics"`. Non-terminal jobs report
`terminal_diagnosis: null`.

For bulk triage, use `vq queue HOST --json` (or `vq list HOST --json`).
Each row is the normal `JobSpec` JSON plus the same nullable
`terminal_diagnosis`, bounded `effective_state`, and tri-state
`scheduler_running_confirmed` fields, so supervisors can classify terminal and
scheduler-reservation rows without polling `vq status --json` once per job.
These fields have the same shape for local, remote, scheduler-driver, `--all`,
and fleet-web queue aggregation.

For fleet load, use `vq overview HOST --json`. Scheduler hosts report
`queue_counts` as the effective load used by placement. Only an exact last
scheduler phase of `running` counts as confirmed running; queued, held, and
unpolled work counts as pending. Failure, fence, finishing, reattachment, and
unknown phases are also capacity-reserved in the compatible pending totals,
with `unconfirmed_scheduler_jobs` and `unconfirmed_scheduler_cpus` identifying
that subset. Do not add those subset CPUs to `pending_cpus` a second time.
`scheduler_queue_counts` preserves the raw phases seen after qsub handoff, and
the text overview prints both sections when scheduler phase data exists. An
unconfirmed scheduler reservation prevents the host from being reported idle.
Queue and status rows expose the same distinction through `effective_state`
and `scheduler_running_confirmed`.

For live resource panels, use `vq top HOST --json`. Each running-job row
includes CPU/RSS/walltime fields plus the same `queue_handle` object as queue,
status, logs, tail, and wait JSON. Delegated remote top rewrites ordinary
`localhost` handles to the requested host alias.

Before onboarding a daemonless scheduler host such as pbs-cluster or slurm-cluster, run
`vq doctor HOST --json`. The required checks include the scheduler driver
daemon, scheduler clients/liveness, configured program hooks, and
`scheduler_remote_vq`. A missing scheduler-side `remote_vq` is a host install
or config failure: fix `[hosts.HOST].remote_vq` or update the queue install
before submitting production jobs.

Doctor runs a **local leg** first, so an unreachable host is diagnosed without
guessing. Read these checks before concluding anything about the remote side:

* `ssh_route` names what the alias resolves to and whether a `ProxyJump` or
  `ProxyCommand` sits in front of it.
* `ssh_first_hop` reports a bare TCP probe of the endpoint this machine dials
  first. When it fails, the remote checks are **absent from the payload**, not
  failed: doctor stopped because the SSH session they all need is impossible.
  Treat a missing `remote_vq` key alongside a failed `ssh_first_hop` as
  "not attempted", never as "remote vq is broken".
* `ssh_transport` appears only when the first hop answered and ssh still failed
  at the transport layer. Its message carries a named verdict plus the `ssh -v`
  lines that identify the failing hop.

An agent that hits any of these should fix the link or the gateway, not the
host's vq install, and must not mark the host administratively down for what is
a local routing problem.

For the browser dashboard, run `vq web run --host 127.0.0.1 --port 8765`.
The read-only JSON API mirrors the cockpit fields:
`GET /api/v1/queue` returns `{host, summary, jobs}` where `summary` carries
the same aggregate counts as the dashboard cards (`total`, `active`,
`pending`, `terminal_attention`, `active_cpus`, `declared_mem_mb`,
`scheduler_hosts`, `state_counts`) and each job carries `queue_handle`
and `terminal_diagnosis`; `GET /api/v1/jobs/<jobid>`
returns the same per-job payload. Write endpoints remain bearer-token gated.

Blocking supervisors that want to wait for the final state without scraping
stderr can use:

```sh
vq wait HOST JOBID --json
```

Terminal success or failure prints a single JSON object on stdout:

```json
{
  "jobid": "abc123",
  "state": "failed",
  "exit_code": 137,
  "cli_exit_code": 137,
  "queue_handle": {
    "job_id": "abc123",
    "host": "compute-d",
    "submitted_at": "2026-07-02T10:00:00+00:00"
  },
  "terminal_diagnosis": {
    "category": "sigkill",
    "action_hint": "increase_memory_or_check_external_kill",
    "summary": "Command died from SIGKILL; treat as resource or external process-manager kill until logs prove otherwise."
  }
}
```

The process exits with `cli_exit_code`, matching plain `vq wait`. If
`--timeout` elapses first, the job keeps running and `vq wait --json` exits
124 with:

```json
{
  "jobid": "abc123",
  "state": "suspended",
  "timed_out": true,
  "detail": "paused_by=admin-update-xyz789; paused_total=3m18s",
  "queue_handle": {
    "job_id": "abc123",
    "host": "compute-d",
    "submitted_at": "2026-07-02T10:00:00+00:00"
  },
  "cli_exit_code": 124
}
```

`runtime_workdir` is the path the job sees as `$VQ_WORKDIR`. For ordinary
local/daemon jobs it matches `workdir`. For scheduler-backed jobs, `workdir`
can remain `null` because there is no local vq scratch directory, while
`runtime_workdir` is the scheduler remote workspace path when the driver
config can resolve it. `checkpoint_qvf_exists` is `true` or `false` only for
local paths vq can stat; it is `null` for remote scheduler paths or unknown
workdirs.

`vq status --json` keeps `progress` for the rolling checkpoint-QVF contract.
The separate `calculation_progress` object comes from the selected vibe-qc
`.system` manifest and exposes normalized `iteration`, `energy_eh`,
`gradient_norm`, and `diis_subspace` keys when those values are available.
This avoids making clients guess whether live short keys such as `iter` and
`energy` or final manifest keys are present.

For calculation-native text, `vq output` selects the `.out` family declared by
the job's `expected_outputs` / `output_stem` metadata, with safe manifest
discovery for older jobs. `vq progress` reads the declared structured-log path
or matching `.scf.jsonl`, skips malformed or non-SCF records, and applies
`--tail` to SCF iterations rather than physical log lines. Both commands stamp
terminal reads so a recently inspected result remains protected by cleanup
policy.

While a scheduler job is nonterminal, `vq output`, `vq progress`, and the
calculation-progress portion of `vq status` read the shared scheduler workspace
through the driver rather than a stale staged-local file. When
`node_scratch_dir` is unset, relative calculation artifacts are written in that
workspace and these readers can observe them live. When it is set, the command
runs in compute-node-local scratch: stdout and stderr remain live because the
wrapper redirects them to the shared workspace, but `.out`, `.system`, and
structured-log updates normally appear there only after the command returns
and copy-back succeeds. The specialized readers cannot reach node-local
scratch.

The follow modes retain byte offsets, recover from truncation and split
structured records, and fall back to the fetched local workspace if scheduler
cleanup wins the final poll. Once the job is terminal, snapshots use whatever
artifacts copy-back and fetch recovered. A TERM, INT, or HUP trap records the
shared exit marker but bypasses node-scratch copy-back, so calculation artifacts
from that path may remain unavailable.

`vq progress -f` follows SCF records when the calculation backend writes them.
The molecular C++ SCF path currently publishes its structured trace after the
blocking kernel returns, so those rows can arrive as a batch rather than one
iteration at a time. `vq status` reads the callback-backed `.system` snapshot
for the latest live molecular phase and values. Geometry-optimization and
post-HF phase entries are snapshots, not guaranteed per-step counters.

For log panels, use:

```sh
vq logs HOST JOBID --tail 200 --json
vq logs HOST JOBID --stdout --tail 200 --json
vq logs HOST JOBID --stderr --tail 200 --json
```

The log JSON includes `jobid`, `state`, `queue_handle`, `stream`, `tail`,
resolved `stdout_path` / `stderr_path`, and the requested text. `queue_handle`
has the same shape and host semantics as `vq status --json`. `vq logs HOST
JOBID -f` is the supported live text stream for terminals; `--follow` and
`--json` are intentionally mutually exclusive because there is no streaming
JSON shape yet.

For engine-native files or other arbitrary workspace artifacts, use
`vq tail HOST JOBID --name FILE -n 200 --json` for polling and
`vq tail HOST JOBID --name FILE -f` for human live follow. The tail JSON has
this additive shape:

```json
{
  "jobid": "abc123",
  "host": "compute-d",
  "state": "running",
  "queue_handle": {
    "job_id": "abc123",
    "host": "compute-d",
    "submitted_at": "2026-07-02T10:00:00+00:00"
  },
  "filename": "calc.out",
  "tail": 200,
  "path": "/var/lib/vq/jobs/abc123/calc.out",
  "text": "..."
}
```

For live scheduler jobs, `path` points at the scheduler workspace path and the
payload also includes `scheduler_target`, `scheduler_job_id`,
`remote_workspace`, and `live_scheduler_workspace: true`. A `tail` value of
`null` means the caller requested `-n 0` / the whole file. `vq tail --json` is
one-shot only; clients that need updates should poll it or use the text follow
command for an operator-facing terminal.

`vq status HOST JOBID --json` uses `scheduler_job_id` as its canonical
scheduler handle and also emits the compatibility alias `scheduler_id` with the
same value. Clients may consume either key, but should not infer that an absent
legacy alias means the scheduler allocation was unrecorded.
Delegated remote tail JSON rewrites both the top-level `host` and
`queue_handle.host` from ordinary `localhost` aliases to the host the operator
requested; scheduler targets remain the scheduler host.

Checkpoint-producing jobs should write one rolling QVF at:

```text
$VQ_WORKDIR/checkpoint.qvf
```

Write it atomically: render to a temporary sibling such as
`checkpoint.qvf.tmp`, then replace `checkpoint.qvf` with `os.replace`. This
keeps viewers from opening a half-written archive. The final result can still
write any job-specific filename; `checkpoint.qvf` is only the live-monitoring
convention.

Checkpoint cadence should be gentle on shared filesystems. As a default,
write no more often than once per completed outer iteration and no faster
than every 10 to 30 seconds. If a checkpoint is large, roughly above
100 MB, prefer 60 seconds or longer and keep heavy volumetric data sparse.
Use a rolling file rather than an ever-growing numbered series unless the
workflow explicitly needs history. Always write a final checkpoint or final
QVF on normal termination if the job has enough state to do so.

If a result file wants to link back to the queue entry, copy the status
`queue_handle` object:

```json
{"job_id": "abc123", "host": "compute-d", "submitted_at": "2026-07-02T10:00:00+00:00"}
```

The queue does not require this to live inside QVF. A client-side cache is
fine, but producers that already write QVF metadata may include this object so
an opened result can jump back to `vq status HOST JOBID`.

## The workdir mechanism (v0.6.54+)

Every dispatched job receives `$VQ_WORKDIR`, but its relationship to the
payload cwd depends on the execution path.

### Local daemon jobs

The daemon creates a per-job **workdir** at dispatch, distinct from the
workspace (cwd, which holds your submitted source):

* **Path**: `/var/lib/vq/users/<your-uid>/workdirs/<jobid>/`
  (multi-user) or `~/.local/share/vq/workdirs/<jobid>/`
  (single-user). The path is injected into your job's environment
  as `$VQ_WORKDIR`.
* **Permissions**: owned by your uid (root-daemon-chowned), so
  your script can read/write freely.
* **Lifetime**: by default the workdir lingers after the job
  terminates so you can read results back. The daemon's
  auto-cleanup sweep removes workdirs older than
  `workdir_max_age_seconds` (operator-configurable, recommended
  14 days).
* **Opt-in immediate cleanup**: pass `vq submit --clean-tmp` and
  the daemon rmtrees the workdir as soon as the job hits a
  terminal state. Use this when your result is captured fully in
  stdout / events and the workdir bytes have no further value.

### Scheduler jobs

For a scheduler target, `$VQ_WORKDIR` is the shared staged workspace under the
configured `scratch_root`; scheduler jobs do not get the managed local workdir
described above.

* When `node_scratch_dir` is unset, `$VQ_WORKDIR` resolves to the staged
  workspace and is also the payload cwd. A write to a submitted filename is
  therefore an in-place overwrite.
* When `node_scratch_dir` is set, vq copies the payload to a temporary
  node-local cwd before launch while `$VQ_WORKDIR` remains the distinct shared
  workspace. vq retains a private node-local seed copy and compares files by
  content and permissions before publishing relative outputs on normal exit.
  Unchanged seed files never overwrite shared checkpoints; equal changes on
  both sides need no copy. New or changed node-local files publish only when
  the shared destination has not changed independently. Live logs and `_vq`
  records are excluded from the seed and from output publication.

  Conflicting changes or copy failures produce a failed wrapper exit (125),
  with a diagnostic in `stderr.log`, and preserve node-local output under
  `_vq/scratch-recovery-*` for ordinary `vq fetch`. If that recovery copy also
  fails, the diagnostic names the retained node-local staging directory.
  Nonconflicting files may already have published before a conflict is found;
  the failed marker must not be treated as a complete result. Payload resource
  telemetry retains the payload's own exit code. Array copy-backs serialize
  their comparison and publication; independently writing payloads must still
  use distinct output paths. Replacing a shared destination symlink fails
  closed. Removing an input in scratch does not remove the shared input.

  Budget node-local space for both the seed and the working copy. A staging
  copy failure stops before launching the payload. Signal termination can
  still bypass copy-back, as before.

The environment-independent rule is the same on every host: never write onto
a payload file. Create a dedicated output subdirectory under `$VQ_WORKDIR`,
prove it is writable with a real write, and fail loudly if any destination
aliases a submitted source.

### Inside your script

```python
import json
import os
from pathlib import Path

workdir = Path(os.environ["VQ_WORKDIR"]).resolve()
output_dir = workdir / "results"

# `exist_ok=False` is deliberate: if the submitted payload already contains
# this path (including as a symlink), fail instead of reusing or corrupting it.
output_dir.mkdir(exist_ok=False)
output_dir = output_dir.resolve()

# Prove this exact destination is writable. Do not silently fall back to the
# payload directory when the probe fails.
probe = output_dir / ".write-probe"
probe.write_text("ok\n")
probe.unlink()

# Scratch downloads, intermediate files, large arrays:
scratch_file = output_dir / "intermediate.npz"
# ... do work, write under output_dir ...

# Final result: write inside the newly reserved directory, then emit a
# one-line summary to stdout for the operator's `vq logs` view.
result = {"converged": True, "energy_au": -1.1726}
result_file = output_dir / "result.json"
result_file.write_text(json.dumps(result, indent=2))
print(json.dumps(result))
```

### Rules

* **Write large and intermediate files under a dedicated child of
  `$VQ_WORKDIR`.** Do not write them directly at its root: on scheduler targets
  without `node_scratch_dir`, that root already contains the submitted source.
* **Never overwrite a submitted file.** Reserve a fresh output child with
  `exist_ok=False`, prove it with a real write, and fail loudly instead of
  silently relocating output. When deriving a destination from a source name,
  resolve both paths and reject equality before copying.
* Local managed workdirs and scheduler shared workspaces have different
  cleanup and fetch lifecycles; use `vq fetch --workdir` only for the former
  and ordinary `vq fetch` for scheduler results.
* **Never write to `/home/USER/gitlab/...`** on the host. The git
  checkouts there are managed by `vq admin update`.
* **Never write to `/tmp/`** for anything you want to read back --
  `/tmp` is OS-managed and may vanish on reboot. Use `$VQ_WORKDIR`.
* If your script needs to know how much disk is available, check
  `shutil.disk_usage(workdir)` from inside Python.

## Resource environment

Every dispatched job receives resource metadata:

```sh
VQ_JOB_ID=<jobid>
VQ_CPUS=<declared --cpus>
VQ_SCHEDULER_TASKS=<declared --scheduler-tasks/--ntasks, when set>
VQ_MEM_MB=<declared --mem-mb, when set>
VQ_WALL_TIME_SECONDS=<declared --time/--wall-time-seconds, when set>
```

Every direct local job writes `_vq/resource-usage.json`, including jobs that
finish before the watchdog's first five-second sample. Its
`vq.direct-resource-usage.v1` receipt reports monotonic wall time, POSIX
`wait4` user/system/active CPU, `ru_maxrss`, process outcome, and a
`process_count` field. Process count is the dedicated cgroup-v2 `pids.peak`
task count above the collector baseline when that counter is available, and is
otherwise null. The receipt's `metric_sources` and `aggregation_semantics`
objects make those distinctions explicit, including that the RSS value is a
maximum rather than a concurrent-process sum and that cgroup threads count as
tasks. Collection does not replace the command outcome: a failed command keeps
its exit code and writes `command_status: "failed"` with `status: "ok"` when
the measurement completed.

Every PBS or SLURM job rendered by the current scheduler driver also writes
`_vq/resource-usage.json`. vq wraps the already-composed effective command
(including any registered program command wrapper) with GNU Time. The
compute-node executable defaults to `/usr/bin/time` and can be set per host
with the absolute `scheduler_gnu_time_command` path; site and program
prologues/epilogues remain outside the measurement. The command's argv,
stdout, stderr, and return code are unchanged.
A normal receipt has this shape:

```json
{
  "schema": "vq.scheduler-resource-usage.v1",
  "status": "ok",
  "collector": "gnu-time",
  "scope": "effective-command",
  "command_status": "succeeded",
  "command_exit_code": 0,
  "wall_seconds": 12.34,
  "user_cpu_seconds": 45.67,
  "system_cpu_seconds": 1.23,
  "active_cpu_seconds": 46.9,
  "peak_rss_kb": 262144,
  "peak_rss_mb": 256.0
}
```

`active_cpu_seconds` is user plus system CPU time. `peak_rss_kb` is GNU
time's Linux `%M` value; `peak_rss_mb` is that value divided by 1024. A
nonzero or signal-derived command exit still has `status: "ok"` when the
measurement completed, with `command_status: "failed"` and the same exit code
as `_vq/exit-code`.

The job checks that the configured executable identifies itself as GNU Time,
can produce the required format, and that `awk` is available before running
any payload or site hook. A failed check writes a `status: "error"` receipt
with null metrics, writes exit code 125, and does not start the command. TERM,
INT, and HUP paths attempt to finalize the receipt before writing their
existing exit marker. An immediate SIGKILL, compute-node loss, or unavailable
shared filesystem cannot run shell cleanup; the missing receipt remains
fail-closed evidence for the consumer.

The receipt is wrapper metadata, so ordinary workspace fetches carry it
automatically under `_vq/`. Jobs already queued with an older generated
script do not gain it retroactively; submit them again after the scheduler
driver update when the receipt is required.

For OpenMP/BLAS-style workloads, wrappers should size OpenMP work from
`VQ_CPUS` (or scheduler-provided values such as `PBS_NP` when running under a
site scheduler). vq also defaults `OMP_NUM_THREADS`, `MKL_NUM_THREADS`,
`VECLIB_MAXIMUM_THREADS`, `NUMEXPR_NUM_THREADS`, and `BLIS_NUM_THREADS` to the
declared CPU count when the daemon or generated scheduler script has not
already set them. `OPENBLAS_NUM_THREADS` defaults to `1` because vibe-qc uses
OpenMP-led parallelism, and pthreaded OpenBLAS with more than one BLAS thread
inside OpenMP regions can hang with OpenBLAS' "Detect OpenMP Loop" warning.
Explicit values supplied by the operator still win.

For SLURM MPI-style workloads, `--scheduler-tasks N` (alias `--ntasks N`) is
the rank/task count rendered as `#SBATCH --ntasks=N`; `--cpus M` remains
`#SBATCH --cpus-per-task=M` and the vq CPU/thread-accounting value. For ORCA
PAL jobs whose input says `%pal nprocs N end`, submit with `--ntasks N --cpus 1`
unless a site wrapper/profile provides that mapping.
`vq status` reports this as `sched_tasks`, `vq status --json` exposes the
`scheduler_tasks` field, and queue tables add a `TASKS` column only when at
least one listed job uses the split.

`vq admin update` may briefly suspend running local jobs while it rebuilds a
managed environment. `vq status` reports the pauser tag (`paused_by`), the
current paused interval (`paused_now`), and accumulated paused time
(`paused_total`). If a suspended job is killed, vq first accounts the current
paused interval so the terminal spec does not under-report admin-update delay.
`vq wait --timeout` keeps the job running but exits 124; when the last status
poll included live context, the timeout line includes pause, PBS/fetch, and
walltime details so monitoring scripts can tell admin-update downtime from
calculation runtime pressure.

## Requesting a code / example landing

Two scenarios:

### 1. "I have a new example that should ship in vibe-qc"

Submit a job whose payload contains the example, tagged
`pr-request`, with a descriptive `--job-name`:

```sh
vq submit compute-d \
    --tag pr-request \
    --job-name "add-basis-opt-NaCl-example" \
    -d examples/basis-opt-NaCl/ \
    -- python run.py
```

For documentation/example regeneration payloads that depend on a mutable
registered checkout, pin the checkout explicitly:

```sh
vq submit compute-d \
    --program vibeqc-dev \
    --expected-sha "$(git rev-parse --short=12 HEAD)" \
    -d docs-artifact-payload/ \
    -- bash regenerate_docs.sh
```

`--expected-sha` works with single-file, `--dir`, and `--compressed` payloads.
Use at least 7 hex characters. The submit fails before queueing if the target
host's `[programs.vibeqc-dev]` checkout is older or otherwise different, and
the host-validated canonical 12-character SHA is stored on the spec so
dispatch fails if the checkout drifts while the job is pending.

For docs or screenshot regeneration jobs that need vibe-view, use the managed
`[programs.vibeview-dev]` entry rather than assuming `vibe-view` is on the
host PATH:

```sh
vq submit compute-a \
    --program vibeview-dev \
    -d docs-artifact-payload/ \
    -- bash -lc '"$VQ_PROGRAM_BIN/vibe-view" capture-selftest && bash regenerate_docs.sh'
```

When `--program` names a `kind = "venv"` program on a daemon host, vq injects
`VQ_PROGRAM_BIN`, `VQ_PROGRAM_PYTHON`, `VQ_PROGRAM_GIT_DIR`, and, when set,
`VQ_PROGRAM_BRANCH` into the job environment. That is the queue-wide
vibe-view handle; do not write into or manually repair the managed checkout.
Scheduler jobs only receive portable program identity (`VQ_PROGRAM` and the
optional branch). They deliberately do not receive driver-local path variables;
the scheduler runtime must be selected with the target's
`scheduler_program_hooks.NAME.command_wrapper`.

The same rule applies to the job *command*, which the dispatcher ships into the
generated batch script verbatim (a `command_wrapper` only prepends to it). A
single-file `vq submit <scheduler-host> my.py` would otherwise default to the
driver's own interpreter, which is a path on the driver and normally does not
exist on the cluster, so that submit **fails closed** with an error naming the
host. Either pass `--python` with a cluster-side interpreter, or use
`--dir` / `--compressed` with an explicit cluster-side command:

```sh
# rejected: interpreter would be the driver's own
vq submit pbs-cluster my.py
# accepted: interpreter is valid on pbs-cluster
vq submit pbs-cluster --python /home/USER/bin/vibeqc-release-python my.py
# accepted: explicit cluster-side command
vq submit pbs-cluster --program vibeqc-release -d payload/ -- vibeqc-release-python run.py
```

**Do not name the launcher twice.** If the host's `vibeqc-release` hook already
sets `command_wrapper = ["/home/USER/bin/vibeqc-release-python"]`, then a
submit that *also* starts its command with that launcher (either spelling -- bare
name, `~/bin/...`, or the absolute path) is asking for it to run twice. vq
de-duplicates and logs a WARNING, so the job still runs, but the two forms are
alternatives: either let the hook supply the launcher

```sh
vq submit pbs-cluster --program vibeqc-release -d payload/ -- run.py
```

or name it yourself against a host whose hook does not set one. Check with
`vq doctor pbs-cluster` -- a `scheduler_command_wrapper` failure means the host's
config has the launcher configured on both sides. See `docs/operations.md`
§ "The `command_wrapper` composition contract" for why: before v0.12.1 the
double-wrap handed a bash launcher to its own python and killed ~250 campaign
jobs with `SyntaxError: set -euo pipefail`.

Use `vq programs --all --json` to audit which hosts currently expose that
handle; the output is one JSON object keyed by host and includes each venv
program's current SHA, actual branch, and dirty checkout flag. Use
`vq programs --all --require vibeview-dev` when you need a simple pass/fail
gate for a queue-wide capture install. During a short migration where some
hosts still expose `vibe-view`, use
`vq programs --all --require-any vibeview-dev,vibe-view`, then standardize the
fleet back to `vibeview-dev`. For git-backed runtime checks, combine
`--require NAME` with `--require-sha NAME=SHA`,
`--require-version NAME=VERSION`, `--require-branch NAME=BRANCH`, and
`--require-clean NAME` so stale, wrong-version, wrong-branch, or dirty managed
checkouts fail before a docs artifact or release-paper job trusts them.

For registered serial CRYSTAL frontends (`crystal`, `crystal23`, and
`crystal23demo`), `vq programs` goes beyond the executable bit: it runs a
bounded no-input startup probe in an empty temporary directory. A missing
shared-library runtime, wrapper exit 126/127, launch failure, timeout, or output
without the exact zero-exit CRYSTAL no-input diagnostic is `NOT OK`, so do not
submit a CRYSTAL wave until the program gate is green. Parallel CRYSTAL
launchers still require a scheduler-context smoke test because the readiness
command does not start MPI outside an allocation.

The job runs (proving the example works on the target host) and
its workdir + stdout become the artefact the maintainer reviews:

```sh
vq queue compute-d --tag pr-request -s completed   # what's ready to review
vq status compute-d <jobid>                         # spec + outputs
ssh compute-d ls $(vq status compute-d <jobid> --json | jq -r .workdir)
```

The maintainer reviews and, if accepted, copies the example into
the appropriate vibe-qc subtree and commits via the normal repo
flow.

### 2. "I have a code change I want landed in vibe-qc"

Same convention: submit a tarball containing a patch + a script
that verifies it on the host:

```sh
vq submit compute-d \
    --tag pr-request \
    --job-name "fix-COSX-screening-tile-size" \
    -c my-patch-and-test.tar.gz \
    -- bash verify.sh
```

The maintainer reads the spec, the diff, the test output. If it
looks right, they apply it on the laptop, commit, push, and let
the next accepted release plus `vq admin rollout-latest` refresh the
managed fleet.

### 3. "I need to debug something that requires interactive shell"

Don't. Submit a script that prints what you need. If you genuinely
need an interactive shell, ask the maintainer.

## Forbidden actions (won't be reverted without notice)

* Writing to `/home/USER/gitlab/vibeqc-*/` on compute-d or compute-a.
* Running `git pull` / `git checkout` / `git stash` on those repos.
* Running `bash scripts/update.sh` by hand on those repos.
* Running `pip install` / `make` / build commands inside those
  repos.
* Modifying anything under `/etc/vq/` or `/opt/vq/`. The one sanctioned way to
  change `/opt/vq` is `sudo /opt/vq/bin/vq-multi-user-refresh`, and that is a
  **maintainer** action during a release rollout, not a job-submitting chat's.
  Do not run it.
* Modifying anything in another user's `/var/lib/vq/users/<uid>/`.

All of these are the queue chat's or the operator's responsibility.
If your work *seems* to need any of them, you're holding the wrong
end of the stick -- submit a job with the right payload and let the
mechanism handle it.

## Updating compute-d / compute-a yourself

You don't. After the release chat commits an accepted report, the maintainer or
updater chat runs `vq admin rollout-latest` from the configured driver. It is
serial, exact-report-pinned internally, resumable, and idempotent. There is no
enabled unattended timer yet; one is explicitly gated on green tests and a
real-fleet dry run.

If you submit a job whose script depends on a vibe-qc feature
that hasn't landed on the host yet, the job will fail at runtime
(import error, etc). Land the fix, cut the next release, ask the updater to run
`rollout-latest`, and resubmit the calculation against the newly registered
immutable runtime. For an urgent operational fix that cannot wait for a
release, ask the updater for a named mini-cycle; do not improvise a host-local
pull.

Do not bypass the queue's update flow by hand-pulling on the host.

## Quick recipe table

| Task | Command |
|------|---------|
| Run a Python script on compute-d | `vq submit compute-d my.py` |
| Size a full-node slurm-cluster job | `vq submit slurm-cluster job.qvf --program vibeqc-release --cpus 96 --mem-mb 900000 --time 24:00:00` |
| Size a full-node pbs-cluster job | `vq submit pbs-cluster --program vibeqc-release --cpus 20 --mem-mb 90000 --time 72:00:00 -d payload/ -- run.py` |
| Run one QVF container | `vq submit HOST job.qvf --program vibeqc-dev` (runtime identity is snapshotted automatically) |
| Run with a specific Python | `vq submit compute-d --python /path/to/python my.py` |
| Run from a dir of files | `vq submit compute-d -d mydir/ -- python run.py` |
| Run with array sweep | `vq submit compute-d --array 30 my.py` (use `$VQ_ARRAY_INDEX`) |
| Run after another job completes | `vq submit compute-d --depends-on <jobid> my.py` |
| Rebuild an explicitly isolated private env (v0.11.0) | `vq submit HOST --refresh vibeqc-dev my.py` (not for the shared fleet) |
| Auto-place by memory (v0.11.0) | `vq submit auto my.py` (queue picks the host) |
| Run with auto-cleanup of scratch | `vq submit compute-d --clean-tmp my.py` |
| Run a script on a scheduler host | `vq submit pbs-cluster --python /cluster/path/python my.py` (`--python` is required there) |
| Submit and learn holds/capacity warnings | `vq submit compute-d --json my.py` |
| Request a repo landing | `vq submit compute-d --tag pr-request --job-name "what-it-does" -d mydir/ -- ...` |
| Check on a job | `vq status compute-d <jobid>` |
| Watch live output | `vq logs compute-d <jobid> -f` |
| Pull results back | `vq fetch compute-d <jobid> -o ~/Downloads/` |
| Pull only the updated QVF | `vq fetch HOST <jobid> --name job.qvf -o DIR` |
| Pull ALL results back (v0.12.0) | `vq fetch-all compute-d -o ~/Downloads/` |
| List all nonterminal work | `vq queue compute-d --active` |
| List jobs in an array | `vq queue compute-d --array-group <group_id>` |
| List jobs awaiting review | `vq queue compute-d --tag pr-request -s completed` |

## When this doc is wrong

If you encounter a situation this doc doesn't cover (or covers
wrong), **submit it as a `pr-request` to the queue chat** with a
clear description of what's missing. The queue chat updates this
doc and the broader vq mechanism so the gap closes for everyone.

Do **not** invent your own workflow on compute-d / compute-a when the
documented one is ambiguous. Ambiguity is a bug in this doc, not
a license to improvise.
