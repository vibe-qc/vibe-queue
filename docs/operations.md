# Operations runbook

Recovery procedures for the rough edges between vq, systemd-user, and the
hosts vq runs on. Most of this exists because we hit it once on
2026-05-16 (see the post-mortem chapter at the end). The
audit-driven hardening sweep (v0.5.42 → v0.6.2, 2026-05-17)
added the diagnostic verbs and recovery markers referenced below.

If your problem isn't here, the order of escalation is:

1. `vq doctor [HOST]` (v0.12.0+) - client-side preflight for config,
   `vq host down` marks, SSH/remote-vq reachability, daemon RPC ping, and
   scheduler-driver routing. Use `vq doctor --all` before a cold fleet submit.
2. `vq daemon health` (v0.5.49+) -- single command that
   cross-checks loginctl + pgrep + systemctl + pidfile and prints a
   verdict. Usually tells you which of the recipes below applies.
3. `vq queue` / `vq daemon status` -- is the daemon alive and seeing
   jobs?
4. `vq admin status` -- is an admin update stuck in state=failed
   (v0.6.0 state machine; see "Admin update stuck" below)?
5. `systemctl --user status vq-daemon` -- is systemd seeing the
   daemon? Recent journal lines?
6. `journalctl --user -u vq-daemon -n 100` -- what did the daemon say
   before it stopped? Look for WARNING "daemon running X, on-disk
   source says Y" (v0.6.2 version-drift probe) or "scope collision
   detected" (v0.5.50 pre-flight).
7. The recipes below.

---

## Host-pressure auto-pause (v0.6.20+)

When `/proc/meminfo` reports `(MemTotal - MemAvailable) / MemTotal >=
85%`, the watchdog SIGSTOPs every currently-RUNNING job -- freezing
their RAM at the current footprint so the kernel OOM-killer
preferentially picks non-vq cgroups if it fires. When pressure drops
below 70% (hysteresis margin), the watchdog SIGCONTs exactly the jobs
it paused.

**Signs in the daemon log**:

```
WARNING host-pressure pause: host memory pressure 87.3% >=
  pause threshold 85.0%; SIGSTOPping 2 running job(s) to avoid
  OOM cascade
INFO    host-pressure resume: host memory pressure 65.1% <=
  resume threshold 70.0%; SIGCONTing 2 previously-paused job(s)
```

**Visible in `vq queue --active`**: paused jobs appear in `suspended`
state during the pressure window. Operator pause/resume work normally
during this -- operator-paused jobs stay paused after the auto-resume
fires (the watchdog only resumes what IT paused).

**Why this exists**: 2026-05-18 07:50 EDT compute-d wedge. Combined load
from desktop (Steam) + a vq job + Nextcloud sync + GNOME shell crossed
the 125 GB cliff; kernel OOM-killer cascaded through vq-daemon itself.
Per-job cgroup MemoryMax was correct but didn't see the aggregate.
This pass watches global pressure and reacts BEFORE the kernel does.

**Disable** (rarely needed): the watchdog accepts
`enforce_host_pressure_pause=False`. No CLI flag yet -- restart the
daemon with a modified launch if you really need to bypass.

---

## Client-side log file (v0.6.16+)

Every `vq` CLI invocation appends to a rotating log file at
`<state_root>/client.log` (typically
`~/.local/share/vq/client.log`). One INFO line per invocation
records the full argv plus the vq version; sub-operations
(SSH calls, file ops, state transitions) log to the same file
via the standard `logging.getLogger()` machinery.

Useful for:

* **Crash debug** -- when a `vq` invocation dies mid-flight or
  prints a confusing error, the log captures what it was doing
  immediately before. Includes `[pid=N]` so concurrent
  invocations from parallel shell pipelines are disambiguable.
* **Operational forensics** -- "did anyone touch compute-d
  yesterday?" answerable by `grep $(date -d yesterday +%Y-%m-%d)
  client.log`.
* **Bug reports** -- paste the relevant log slice + the argv
  line and the maintainer has half the reproduction info
  already.

Rotation: 10 MB per file × 3 backups (`client.log.1` / `.2` /
`.3`); 40 MB max footprint.

Level: INFO by default. Override with `VQ_LOG_LEVEL=DEBUG`
(case-insensitive; invalid values silently fall back to INFO).
DEBUG is verbose -- useful when reproducing a specific issue,
overkill for steady-state.

Disable: `VQ_LOG_DISABLED=1` skips log-file setup entirely.
Useful in test fixtures and as the emergency escape hatch if
the log file write itself becomes a failure mode (read-only
filesystem, permission denied). The CLI keeps working
regardless -- logging is best-effort.

Daemon log is separate (`<state_root>/daemon.log`); both can
exist side-by-side on a host that runs both client commands
and a daemon.

---

## `vq doctor` (v0.12.0+) - client preflight

Scheduler status refreshes wait for a new main-loop observation in a separate
bounded RPC worker. Up to four refresh readers can wait concurrently; additional
readers receive `scheduler refresh busy or server stopping`. A busy or timed-out
refresh is unavailable evidence, not a terminal scheduler state. These waits
do not block daemon ping, method discovery or serialized admin calls. A slow
main-loop operation can still exceed the requested freshness deadline; inspect
the reported observation time and direct scheduler state before resubmitting.

Use this when a fresh shell or chat needs to know whether `vq` is ready to
submit before it sends real work. It is read-only and checks the client-side
pieces first: host config, local `vq host down` marks, SSH/remote-vq
reachability, and a daemon RPC ping.

```sh
vq doctor                 # default_host, or implicit localhost without config
vq doctor compute-a            # one configured host
vq doctor --all           # every configured host
vq doctor --all --json    # monitor/script-friendly envelope
vq doctor pbs-cluster --admin-update
```

If a fleet command reports `remote vq failed (exit 127)`, the configured
`remote_vq` path is missing on that host. Run `vq doctor HOST --verbose` to
confirm the configured path, then repair the install or update
`[hosts.HOST].remote_vq`. If the host should be ignored while it is being
repaired, mark it down with
`vq host down HOST --reason "remote_vq missing"`; restore it later with
`vq host up HOST`.

Scheduler hosts are daemonless by design. For those, `vq doctor` validates the
configured `scheduler_driver` and pings the driver daemon instead of trying to
contact a daemon on the cluster login node. It also runs the read-only scheduler
probe from the driver side, so a `pbs-cluster` preflight catches missing `qsub`,
`qstat`, `qdel`, `qhold`, or `qrls` before a job reaches dispatch or a queued
job hold/release operation.

For PBS/Torque scheduler hosts, the same probe also reports scheduler dispatch
liveness. `vq doctor HOST --json` includes a `scheduler_liveness` check derived
from `qstat -Bf`, `qstat -Qf`, and a read-only `pbs_sched` process probe. A
failure such as `pbs_sched is not running` or `queue(s) enabled but not started`
means vq can submit and poll, but the cluster scheduler is not dispatching
work; pause new production submits and ask the cluster administrator to restart
or re-enable PBS scheduling.

For scheduler hosts, `vq doctor HOST` also reports `scheduler_program_hooks`.
Every `[hosts.HOST.scheduler_program_hooks.NAME]` key must have a matching
`[programs.NAME]` entry; missing program registrations are failures so typoed
per-program qsub hooks do not silently sit unused.

Scheduler configuration is fail-fast at config-load time. Fields that only make
sense on daemonless scheduler hosts, such as `scratch_root`,
`node_scratch_dir`, `remote_scheduler_host`, `scheduler_driver`, and
`submit_extra`, are rejected on normal `scheduler = "local"` hosts. For
scheduler hosts, `scratch_root` must be an absolute path on the scheduler host,
and scheduler path/command fields must be non-empty single-line strings. Treat
these as operator config errors and fix them before submitting production work.

For each scheduler host whose exact lane limit is known, set
`scheduler_max_wall_time_seconds` to that lane's strict positive maximum in
seconds. The value is bound to the effective partition parsed from that host's
`submit_extra`; `vq doctor HOST --json` and `vq programs HOST --json` expose
the additive `scheduler_lane` partition, maximum, and metadata source. An
unset maximum means unknown, never unlimited. Generic vq continues to admit an
unknown limit, but paper tooling must fail closed unless the exact requested
host reports a bounded canonical partition. A request above a configured
maximum is rejected before staging and revalidated by the daemon before
dispatch; vq never silently clamps it.

Use `--admin-update` before a maintenance run such as `vq admin update pbs-cluster`.
For scheduler hosts this adds a check for `scheduler_update_command` and notes
whether `scheduler_install_command` is also configured, so a missing cluster
provisioning command is caught before the update attempt. It also runs the
scheduler host's configured `remote_vq --version`, which is the quickest way to
verify the cluster-side helper after `vq admin update HOST`.

### Scheduler-side vq refresh for daemonless clusters

Daemonless scheduler hosts do not have a normal `vq-daemon` checkout to refresh.
The scheduler login host still needs a small vq install for operator probes and
cluster-side helper commands, usually configured as an absolute `remote_vq`.
Register that install through the scheduler host block:

```toml
[hosts.cluster]
ssh = "cluster-login"
remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"
scheduler = "pbs"
scheduler_dialect = "torque"
scheduler_driver = "driver"
scratch_root = "/home/USER"
scheduler_gnu_time_command = "/usr/bin/time"
scheduler_prologue = ["module purge", "source /home/USER/cluster-env.sh"]
scheduler_epilogue = ["rm -f scratch.tmp"]
scheduler_update_command = "/home/USER/vibe-queue/contrib/update-scheduler-vq.sh"
scheduler_update_host = "cluster-build"  # optional: build-capable SSH target
scheduler_update_stage = "/shared/USER/vq-admin/cluster"  # optional shared base
```

`scheduler_gnu_time_command` is the absolute path to GNU Time as seen by a
compute-node batch shell. It defaults to `/usr/bin/time`; set it explicitly
when the site installs GNU Time elsewhere. The scheduler script verifies that
the path is executable, identifies itself as GNU Time, and produces the
required telemetry format before it starts the payload.

`vq admin update cluster` now creates a fresh immutable stage generation from
the driver's exact, clean git revision before it invokes the configured command.
The command receives `VQ_SCHEDULER_STAGE`,
`VQ_SCHEDULER_EXPECTED_SOURCE_SHA`, and
`VQ_SCHEDULER_EXPECTED_TREE_SHA256`. The default stage base is
`$HOME/.cache/vq-admin/cluster` on the update SSH target; configure
`scheduler_update_stage` when the update runs on a build node and needs an
explicit shared path.

The shipped `contrib/update-scheduler-vq.sh` verifies the generation metadata
and archive checksum, extracts the archive, and verifies the staged `src/vq`
digest before creating a backup or touching the live tree. It then copies by
content checksum, reinstalls the helper, verifies the installed package digest,
and only then writes the `SOURCE-SHA` marker. A matching marker by itself is
never accepted as proof of installed code. After verification, vq retains the
five newest recognized stage generations and removes older ones. The active
generation is always
preserved; symlinks and unrecognized files or directories are left untouched
for operator forensics. A cleanup problem is reported as a maintenance warning
in text and JSON output, without misreporting a verified deployment as failed.
The normal update is therefore simply:

```sh
vq admin update cluster --show-output
```

The script backs up the current scheduler-side tree before replacing it. Its
failure cleanup removes only its private extraction directory; it never moves
the live helper directory, including when an already-current helper fails a
provenance check. Standalone `--source DIR` remains available for manual local
maintenance, but managed updates should use the staged contract above. Fresh
provisioning can use a site-local wrapper as `scheduler_install_command` once
the site has decided how the initial venv is created.

Site-local scheduler wrappers must consume the same three environment values.
They must verify the staged archive before publication, verify the installed
`vq source-tree-sha256` before writing `SOURCE-SHA`, and ensure failure cleanup
can only remove a newly-created staging root. A wrapper that ignores the staged
generation now fails the driver's post-update content check instead of reporting
a false success.

Managed helper updates retain staging generations. Successful activation does
not authorize cleanup: another deployment may still be using an older stage.
The hidden `vq source-stage-prune` command remains a separate maintenance
primitive. Use it only after establishing that no deployment is in flight and
identifying every generation that must be retained. Never prune queue state,
active/previous runtimes or protected sync directories as part of staging
maintenance. A routine update no longer requires moving forensic evidence out
of `generations/` to protect it from automatic retention cleanup.

Immutable venv updates likewise retain all other runtime generations after
both new activation and verified-slot reuse. The low-level slot reclamation
primitive is not an update step. Any separate retention operation needs a
fleet-wide no-deployment check and a fresh, complete ownership/liveness census.

For a new immutable slot, vq first reuses objects from the live checkout. If
the requested commit is absent there, it fetches that exact commit from the
checkout's configured origin into the unpublished slot. Relative filesystem
origins are resolved relative to the live checkout. Neither the live working
tree nor its refs are refreshed; no manual fetch inside a managed checkout is
needed. An unreachable origin or unavailable commit leaves the current runtime
unchanged. A locally available commit still needs no network for materialization.

Scheduler result collection runs with at most two transfer workers per daemon.
A slow completed-job archive no longer holds the main dispatch loop while it
downloads. The main loop still owns the exit-marker, accounting, retry and
terminal-state decisions; pending transfers retain their scheduler reservation.
Dispatcher groups receive transfer turns in service order: an actual admission
moves that group behind existing waiters, and new groups join at the back.
Slots released during a reconciliation pass become available on the next pass,
so an earlier busy group cannot continually overtake another ready group.
This bounds overtaking when polls and transfers progress; it does not impose
a wall-clock bound on scheduler polling, marker probes or filesystem work.
Each transfer uses distinct local and remote scratch archives, so an overlapping
fetch or transport left behind by a stopped daemon cannot remove another
transfer's archive. A restart re-proves terminal evidence before completing a
job. This does not remove queue-directory scan cost or make scheduler submit
and exit-marker SSH calls asynchronous.

Scheduler submission bursts yield to a fresh admission pass after five seconds
plus at most one in-flight submission. The next pass reloads pending priorities,
drain/update holds and resource reservations. A new higher-priority job therefore
does not wait behind the remainder of an old, lower-priority queue snapshot.
This is an admission checkpoint, not a submission latency guarantee: individual
SSH calls and queue-directory scans still contribute to elapsed time.

If compilation is only allowed on a specific cluster node, set
`scheduler_update_host` to that SSH target. Job submission and qstat polling
still use the scheduler host's normal `ssh` / `remote_scheduler_host`; only the
update/install shell command moves to the build-capable host. `vq doctor HOST
--admin-update` reports the configured update host so an operator can catch a
missing build-node override before running maintenance. Doctor also compares
the helper's content-derived `source-tree-sha256` and declarative `SOURCE-SHA`
with the driver, in that order. A helper newer than 0.26.0 answers both, plus
its version, through one `vq source-identity` call; an older helper is asked
each separately, which on a slow login node costs three python start-ups
inside the check's single `--check-timeout` budget. `[fleet]
check_timeout_seconds` widens the rollout sweep's budget for exactly that
reason (see `docs/orchestration.md`), and a probe that runs out of it is
reported as a timeout — retryable, never a wrong SHA.

Fleet venv refreshes are intentionally separate from scheduler-host refreshes.
`vq admin update ENV --all-hosts` and `vq admin auto-update ENV --all-hosts`
update hosts that run vq daemons. Daemonless scheduler targets stay visible in
the text or JSON fan-out result, but they are marked as skipped with a pointer to
`vq admin update HOST`. This keeps pbs-cluster from being probed as a normal remote vq
daemon while still reminding the operator that the cluster-side vq install has a
separate maintenance step.

`vq scheduler-probe HOST --json` emits the same scheduler-client probe in a
monitor-friendly shape. `vq programs HOST` and `vq admin status HOST` render
daemonless scheduler hosts as scheduler targets rather than SSHing to them for a
remote `vq` daemon. `vq daemon ping HOST` and `vq daemon health HOST` do the
same lower in the stack: the scheduler host is represented as a wrapper around
the configured driver daemon.

The same driver ownership applies after dispatch. Use `vq status HOST JOBID`,
`vq logs HOST JOBID`, `vq tail HOST JOBID --name FILE`, `vq fetch HOST JOBID`,
`vq fetch-all HOST`,
`vq wait HOST JOBID`, `vq kill HOST JOBID`, and `vq resubmit HOST JOBID`
against the scheduler host name; the client routes them through the configured
driver and filters bulk operations to that scheduler target. `vq tail` can read
an arbitrary file already visible in the shared scheduler workspace; add `-f`
to follow it by remote byte offset until the driver spec is terminal. With
`node_scratch_dir` configured, a relative file such as `calc.out` normally
appears only after successful copy-back at command return. Use
`vq logs HOST JOBID -f` for stdout/stderr panels because the wrapper always
redirects those streams to shared files.
`vq top HOST`
follows the same rule and renders only driver specs tagged for that scheduler
host. Its `ACTIVE` column subtracts paused time from the wall-clock `ELAPSED`
age, matching the watchdog wall-time accounting; `MEM%` and `WALL%` show current
resource pressure against the job's declared limits. `vq drain HOST` reports the
driver-level drain gate because the driver daemon owns scheduler dispatch.
For update windows, prefer an explicit update drain:

```sh
vq drain HOST --update-mode accept --reason "fleet upgrade"
vq drain HOST --update-mode deny --reason "fleet upgrade"
vq drain HOST --status
vq drain HOST --release
```

`accept` means "paused for update, accepting jobs for later": new submissions
become pending, but dispatch stays paused. `deny` means "paused for update,
denying new submissions": `vq submit` fails fast until the drain is released.
Use `deny` when jobs would otherwise be accepted against a stale or inconsistent
runtime during a fleet update.

When only one scheduler backend is unsafe, use a scheduler-target drain instead
of a global drain:

```sh
vq drain --scheduler-host pbs-cluster --reason "PBS scheduler idle"
vq drain --status
vq drain --release --scheduler-host pbs-cluster
```

This is daemon-enforced on already-pending scheduler rows. Matching jobs stay
PENDING with their original job IDs and workspaces, no qsub/sbatch is issued for
that target, and unrelated scheduler targets such as slurm-cluster can continue to
dispatch.

For a safe handoff from a global emergency stop to one held scheduler lane, keep
the full drain active while adding the lane, verify status, then release only
the full gate:

```sh
vq drain --update-mode accept --duration 12h --reason "fleet stop"
vq drain --scheduler-host pbs-cluster --reason "PBS scheduler idle"
vq drain --status
vq drain --release-full
vq drain --status
```

The first status should report `mode: full + scheduler-target (held: pbs-cluster)`.
During that state no scheduler target dispatches. `--release-full` then clears
only the global hold and leaves the pbs-cluster lane active, so slurm-cluster can dispatch
while already-pending pbs-cluster rows remain unchanged.
`vq throttle HOST` is intentionally not a
scheduler control: CPUWeight/cgroup throttling applies to local driver
processes, not jobs already handed to a batch scheduler.

### Scheduler-driver caps, state, and fetch contract

For daemonless scheduler hosts, the driver daemon owns the vq specs but the
work runs under the site scheduler. Size the daemon caps accordingly:

```sh
vq daemon run --max-cpus 18 --max-jobs 4 --max-scheduler-jobs 24
```

`--max-jobs` caps local child processes plus reattached local orphans on the
driver. Scheduler-backed jobs do not consume that local cap, CPU budget, or
memory budget because they run on the cluster. Use `--max-scheduler-jobs` only
when you want a separate limit on the number of scheduler jobs the driver keeps
submitted, queued, or running at once. If unset, scheduler submissions are
unlimited from vq's side and are governed by the batch scheduler. A full
`vq drain HOST` still blocks all new dispatch owned by the driver; partial
`vq drain --max-jobs N` is a local-process cap and is not a runtime scheduler
submission throttle.

`vq status HOST JOBID` separates the three phases operators need to read:

* `state` is the local vq lifecycle, such as pending, running, or completed.
* `sched_state` is the last scheduler observation, such as queued, running,
  finishing, or unpolled. JSON still carries the legacy `pbs_state_label`
  compatibility key, plus `scheduler_state` and `scheduler_status_label`.
* `fetch_state` says whether the scheduler workspace is still live remotely,
  waiting for the exit-marker and copy-back fence, or already staged locally.

`vq overview HOST` treats only an exact last `sched_state=running` as
confirmed execution. Queued, held, and unpolled jobs remain effective pending
load. Finishing, poll/marker/fetch failures, reattachment failures, and unknown
phases are reported as unconfirmed scheduler reservations: their CPUs remain
inside `pending_cpus` so placement stays fail-closed, while
`unconfirmed_scheduler_jobs` and `unconfirmed_scheduler_cpus` identify that
subset in JSON and text. A host with any such reservation is never described
as idle.

`vq queue HOST` uses the same split in compact form:
`HOST:vq=running,sched=queued` means vq already submitted the job, while the
scheduler has not started it on a compute node yet. Terminal scheduler rows use
`sched_last` because the vq state is local and final, while the scheduler value
is only the last observation before local completion.

The queue `STATE` column is the bounded monitor projection. For a scheduler job
whose durable vq lifecycle is `running`, it shows the exact last recognized
scheduler phase: `running`, `queued`, `held`, `unpolled`, `poll_failed`, a
copy-back fence such as `finishing` or `fetch_failed`, or
`scheduler_unknown`. The raw lifecycle remains the ownership record. Therefore
`vq queue -s running` deliberately retains every scheduler-owned lifecycle
`RUNNING` job so a telemetry outage cannot look like free capacity. The footer
qualifies that reservation count as `confirmed/owned` and lists the phases that
were not last confirmed running. Use an exact phase filter such as
`vq queue -s poll_failed` for incident triage; `--active` continues to include
every non-terminal lifecycle state. Collapsed arrays use the same phase
projection and show a mixed scheduler summary instead of borrowing the first
element's phase.

A live scheduler hold whose raw state is `RUNNING` or `SUSPENDED` and whose
exact scheduler phase is `held` projects to queue/status
`effective_state=held`, matches `-s held`, and remains under `--active`. A
vq-issued `qhold` normally has raw vq `suspended` plus `sched_state=held`, so its
`scheduler_running_confirmed` value is `null`. A hold first observed outside
vq can instead retain raw lifecycle `RUNNING`; that row also remains in
`-s running`, reports confirmation `false`, and continues to reserve capacity.

Queue and status JSON retain raw `state` and `scheduler_state` and add
`effective_state` plus `scheduler_running_confirmed`. The confirmation field is
tri-state: `true` means the last stored scheduler observation was exactly
`running`; `false` means a scheduler-owned lifecycle `RUNNING` job lacks that
confirmation and is not evidence of idle capacity; `null` means the predicate
does not apply. It is last-observation evidence, not a freshness guarantee.
Unrecognized or unsafe raw phase strings remain available in
`scheduler_state` JSON for diagnosis but project to the bounded
`scheduler_unknown` token in text and filters.

Scheduler submission is a submit-once transaction. Before `qsub` or `sbatch`,
the driver persists a `submitting` phase and uses a versioned remote receipt.
A valid scheduler ID or exact job-start marker proves acceptance; a committed
ordinary scheduler return code from 1 through 127 proves rejection. Timeout,
signal status, SSH 255, missing, conflicting, truncated, or otherwise ambiguous
evidence leaves the job nonterminal as
`submit_outcome_unknown`, reserves scheduler capacity, retains the remote
workspace, and is reconciled without submitting again. Treat that state as an
accepted-or-unknown scheduler mutation: inspect its bound receipt and marker;
do not manually replay the submit command.

For client-side response-loss protection on a single logical submission, use
an application-generated key:

```sh
vq submit input.py HOST \
  --idempotency-key paper-wave-0042 \
  --program vibeqc-release \
  --expected-sha <40-hex>
```

Repeating the exact keyed intent returns its original vq job ID. Changing the
payload, scheduler target, runtime pin, argv, resources, dependencies, or tags
with the same key is a hard conflict. Do not reuse keys across logical jobs.
`KEY` is 1 to 128 characters, starts with an ASCII alphanumeric, and otherwise
uses only ASCII alphanumerics, `.`, `_`, `:`, or `-`; vq redacts it from
delegated argv diagnostics.
Arrays and chains remain unkeyed; daemon retry, resubmit, rerun-until, and
auto-resume create fresh attempts and never inherit the original key.

Scheduler directory jobs are workspace-only in vq. Use
`vq fetch HOST JOBID -o DIR` to copy the preserved scheduler workspace,
including stdout, stderr, `_vq` markers, and generated output files that reached
the shared workspace. With `node_scratch_dir`, relative generated files arrive
only after successful normal copy-back; signal-trapped termination can leave
only the shared logs and marker available. `vq fetch --workdir` is for local
per-job scratch directories and intentionally reports that scheduler jobs have
no separate workdir unless a future scheduler template explicitly preserves
one. For a non-terminal scheduler row such as `poll_failed`, the single-job
fetch command attempts a best-effort live snapshot and fails loudly if the
workspace is unavailable. It does not prove terminal state and does not stamp a
terminal fetch time. `fetch-all` remains terminal-only.

Cleanup handles the duplicate scheduler-side workspace after results have been
copied back. At the auto-cleanup archive threshold, vq removes the remote
scheduler workspace and stamps `scheduler_remote_workspace_cleaned_at` on the
spec. Archived specs whose remote cleanup failed are retried on later sweeps,
and final `--delete` skips a scheduler spec if the remote workspace cannot be
cleaned, preserving the deterministic path for a later retry. See
`docs/auto-cleanup.md` for the retention contract.

For long release-paper jobs, read terminal states as follows:

* `time_exceeded` on a scheduler host means scheduler walltime accounting reached
  the requested limit. If the exit marker is missing, vq records the missing
  marker in `_vq/events.jsonl` but keeps the terminal state as `time_exceeded`
  when qstat detail, the persisted spec, or the final diagnostic probe supplies
  enough walltime evidence.
* `failed` with `exit_code: 137` means the wrapped command died from SIGKILL.
  Treat it as a resource or external process-manager kill until stdout, stderr,
  `_vq/events.jsonl`, and host logs prove otherwise. A bare `Killed: 9` line is
  not a scientific failure.
* `aborted_by_queue` after scheduler completion means vq could not recover an
  exit marker and had no scheduler walltime evidence to classify the cause. Use
  the event evidence, fetched workspace, and scheduler/accounting logs before
  promoting or resubmitting results.
* `vq kill` records `killed` plus the recovered exit code, for example `-15`
  for a SIGTERM accepted by the wrapper. Admin-update `pause`/`resume` events
  are recorded separately and should be accounted as queue/operator downtime,
  not calculation time.
* `vq wait HOST JOBID --timeout SECONDS` cancels only the client-side wait. The
  job keeps running, and the exit-124 message includes last-seen details when
  available: `paused_by`, `paused_now`, `paused_total`, active elapsed versus
  requested walltime, PBS/fetch labels, scheduler walltime usage, and any
  queue-attributed reason.

Scheduler hosts accept the existing vq-managed `--array N` and `--chain N`
submit modes. The driver creates N ordinary specs tagged with
`scheduler_target=HOST`; each spec becomes its own qsub when it reaches the
driver dispatch turn. This is intentionally not PBS-native `qsub -t` array
submission, so `vq queue`, `vq status`, `vq wait`, `vq fetch`, dependencies,
and per-spec retry/cleanup semantics stay the same as local arrays/chains.
`--rerun-until` composes with both modes: every generated spec carries the same
flag path and rerun cap, then stops independently when its own flag appears.

Use `vq submit --program NAME ...` when the job corresponds to a configured
`[programs.NAME]` entry. The flag is metadata only: it does not rewrite the
command. Local submits validate `NAME` against the local registry; remote and
scheduler-driver submits forward it so the receiving host validates against its
own registry. Jobs see `VQ_PROGRAM=NAME` in the environment, and scheduler jobs
also receive the same `VQ_ARRAY_*`, `VQ_CHAIN_*`, and `VQ_RERUN_*` metadata as
local jobs. Scheduler hosts can also define
`[hosts.HOST.scheduler_program_hooks.NAME]` to add trusted prologue/epilogue
lines only for jobs submitted with that program name.

For `kind = "venv"` programs that run on the daemon host, job payloads also
receive:

* `VQ_PROGRAM_BIN` -- directory containing the configured program Python and
  console scripts.
* `VQ_PROGRAM_PYTHON` -- the configured interpreter path.
* `VQ_PROGRAM_GIT_DIR` -- the managed checkout path.
* `VQ_PROGRAM_BRANCH` -- the configured branch, when set.
* `VQ_PROGRAM_GIT_SHA` -- the full Git SHA resolved immediately before
  dispatch. This is execution provenance, not the possibly older submit-time
  observation in `expected_git_sha`.

For `kind = "binary"` programs that run on the daemon host, job payloads also
receive:

* `VQ_PROGRAM_EXE` -- the validated executable path, so payloads can
  `exec "$VQ_PROGRAM_EXE"` without host-conditional logic. (venv programs keep
  `VQ_PROGRAM_BIN` as a *directory*, which is why binary programs use a
  separate variable.)

vq still does not rewrite `PATH` or the command. Directory payloads running on
that daemon host should call tools explicitly, for example
`"$VQ_PROGRAM_BIN/vibe-view" capture ...`.

Scheduler jobs receive the portable `VQ_PROGRAM` identity and optional
`VQ_PROGRAM_BRANCH`, plus `VQ_PROGRAM_GIT_SHA` for a resolved managed runtime,
but not these driver-local path variables. A scheduler
target can have a different operating system and filesystem layout; injecting
the driver's checkout or interpreter path there is invalid. Configure the
scheduler-side executable with
`[hosts.HOST.scheduler_program_hooks.NAME].command_wrapper` instead.

The job command is subject to the same rule: the dispatcher ships `spec.command`
into the generated batch script verbatim, and a `command_wrapper` only prepends
to it, so every absolute path in the command must be valid on the cluster. A
single-file submit normally defaults its interpreter to the driver's own
(`sys.executable`, or the driver host's `remote_python`), so vq **rejects that
submit for a scheduler target** rather than queueing a job that can only fail on
the compute node with `FileNotFoundError` naming a driver-local path. Pass
`--python` with a cluster-side interpreter, or submit `--dir` / `--compressed`
with an explicit cluster-side command. An interpreter given as a bare name
(`python`) is a `PATH` lookup on the cluster, not a driver-local path, and stays
accepted.

For git-backed venv programs with `expected_git_sha` or
`expected_import_version`, vq validates the pin at `vq submit --program NAME`
and snapshots the configured expected values onto the job spec. Immediately
before daemon dispatch, vq compares the live program runtime to that submitted
snapshot, not to whatever the registry currently expects. This catches a job
that sat pending while a mutable checkout advanced, including the case where an
operator also updated the registry pin. The job is marked failed with a
`runtime pin mismatch before dispatch` reason instead of starting an off-pin
calculation.

For one-off docs/example artifact jobs, pass `--expected-sha SHA` with
`--program NAME`. The CLI accepts at least 7 hex characters, validates the
program checkout before queueing, and records the host-validated canonical
12-character SHA on the spec. That makes later dispatch failures and fetched
diagnostics say exactly which checkout the job was meant to use, even when the
operator typed a shorter prefix.

Scientific payloads that write their own producer/result JSON should copy
`VQ_PROGRAM_GIT_SHA` into that record and treat its absence as fatal when exact
build attribution is required. Do not reconstruct the SHA by scanning native
extension filenames or by assuming the submit-time checkout remained active;
vq has already authenticated the scheduler wrapper or observed the local
checkout at the dispatch boundary.

For release-paper scheduler runs, prefer versioned release roots in the program
registry, for example `~/vibeqc-release-<sha>-<date>/.venv311/bin/python`, over
generic paths such as `~/vibeqc-release/.venv311/bin/python`. `vq programs
HOST --json` exposes the configured `python`, `git_dir`, current checkout SHA,
`git describe`, actual branch, dirty checkout flag, and the imported module
version when `import_check` is set. Check those fields before submitting
production paper jobs, and keep the fetched `.system` manifests as the final
promotion authority.

### Queue-wide vibe-view capture install

Yes: install vibe-view as a queue-managed venv program on each daemon host,
not as an unmanaged system-global Python package. The standard program entry is
`vibeview-dev`:

```toml
[programs.vibeview-dev]
kind = "venv"
python = "/home/USER/vibeqc-dev/.venv-vibeview/bin/python"
git_dir = "/home/USER/vibeqc-dev"
branch = "main"
update_script = "scripts/update_vibeview_capture_env.sh"
import_check = "vibeview"
healthcheck_command = "xvfb-run -a vibe-view capture-selftest"
description = "vibe-view headless capture environment (main branch)"
```

Then run `vq admin update vibeview-dev HOST` for one host, or
`vq admin update vibeview-dev --all-hosts` after the entry is present on the
fleet. `vq programs HOST` should show `vibeview-dev` OK with the
`capture-selftest` healthcheck before docs artifact jobs use it. Submit those
jobs with `--program vibeview-dev` and invoke
`"$VQ_PROGRAM_BIN/vibe-view"` from the payload command. This gives the queue a
global, healthchecked vibe-view handle while keeping the actual install scoped
to each managed checkout/venv.

For a scriptable fleet audit, use `vq programs --all --json`. The output is a
single JSON object keyed by host, so management chats can check whether
`vibeview-dev` exists and reports `status = "OK"` on every target without
scraping the human `==== HOST ====` banners.

For a pass/fail gate, use:

```sh
vq programs --all --require vibeview-dev
```

This prints the normal per-host listing and exits non-zero if any configured
host lacks `vibeview-dev` or reports it as `MISSING`. Use
`vq programs --all --json --require vibeview-dev` when automation needs the
host-keyed inventory on stdout and the failure list on stderr.

During a temporary rename/migration window, accept either the standard
`vibeview-dev` handle or the older `vibe-view` handle with:

```sh
vq programs --all --require-any vibeview-dev,vibe-view
```

Treat that as transitional. New docs artifact jobs should still submit with
`--program vibeview-dev` once the fleet config has converged.

For tools that are only meaningful beside a primary program, gate the
relationship without requiring the primary on every host:

```toml
[programs.orca_2mkl]
kind = "binary"
binary = "/opt/orca-6.1.1/orca_2mkl"
description = "ORCA 6.1.1 Molden converter"
```

The path must be target-local and absolute. Register a host-owned wrapper
instead when module setup is required; do not derive the converter from the
primary ORCA path because the primary may itself be a wrapper.

```sh
vq programs --all --require-companion orca=orca_2mkl
```

This ignores hosts that do not register `orca`. Every host that does
register it must report a healthy `orca` record and expose a separate,
healthy `orca_2mkl` record. An unavailable host inventory fails closed
because the relationship cannot be verified.

For git-backed venv programs, add `--require-clean NAME` when local checkout
modifications would make an update or artifact run untrustworthy:

```sh
vq programs --all \
    --require vibeqc-dev \
    --require-sha vibeqc-dev=<expected-sha> \
    --require-version vibeqc-dev=<expected-version> \
    --require-branch vibeqc-dev=main \
    --require-clean vibeqc-dev
```

This uses the `current_git_dirty` field from the program records and fails if
the checkout is dirty or the dirty state cannot be read. The branch gate uses
`current_git_branch` and fails if the live checkout is not on the expected
branch. The SHA gate accepts short SHA prefixes, which is useful after
`vq admin update` should have moved the managed checkout to a specific pushed
commit. A 40-character requirement uses `current_git_sha_full` and requires
exact equality; a matching display prefix is not full evidence. The version
gate uses `import_version` and requires an exact match, which catches a rebuilt
checkout whose importable package is still stale or broken.

Scheduler hosts can add trusted shell hooks to every generated qsub script:
`scheduler_prologue = ["..."]` runs after the script enters the job working
directory and before the user command, while `scheduler_epilogue = ["..."]`
runs after the user command rc is captured and before node-scratch copyback and
the vq exit marker. Use these for site setup such as `module load`, sourcing a
cluster environment, or small cleanup/copyback glue. Program-specific hooks use
the same line contract under `[hosts.HOST.scheduler_program_hooks.NAME]`; the
host-level prologue runs first, then the matching program prologue, the user
command, the matching program epilogue, and finally the host-level epilogue. A
program hook can also set `command_wrapper = ["/site/bin/orcasub", "..."]`.
These hooks and wrappers are host-maintainer configuration, not per-submit user
input. Site-specific wrapper binaries such as `vibeqcsub`, `orcasub`, or
`crystalsub` remain host-managed future work. Run `vq doctor HOST` after hook or
wrapper edits; it fails if a hook key has no matching `[programs.NAME]` registry
entry, and it fails if a wrapper collides with the interpreter (below).

### The two scheduler submit modes

vq enforces two **mutually exclusive** ways to get an executable onto a
scheduler host. Until v0.12.1 this contract was stated nowhere except in one
error message, which is how the two ended up mixed in production.

| | Mode A -- explicit interpreter | Mode B -- site wrapper |
|---|---|---|
| Submit | `vq submit pbs-cluster --python <cluster-path> my.py` | `vq submit pbs-cluster --program NAME -d payload/ -- <cluster-side command>` |
| Launcher comes from | `--python` (or `[hosts.HOST.branches]`) | `scheduler_program_hooks.NAME.command_wrapper` |
| That program's hook | must have **no** `command_wrapper` | supplies the launcher |
| Payload | single file | `--dir` / `--compressed` |

**Mixing them is the double-wrap bug below.** In Mode A the command already
*is* `[interpreter, script]`; a `command_wrapper` on the same program prepends
the launcher a second time.

Note that Mode A's `--python` is effectively mandatory: vq does **not** fall
back to `remote_python` for a single-file scheduler submit. It refuses a
driver-local interpreter outright, because `spec.command` is shipped into the
batch script verbatim and a driver path generally does not exist on the
cluster. The refusal is immediate and names the host.

The two modes can coexist on one host *per program* -- ORCA in Mode B with an
`orcasub` wrapper while vibeqc is in Mode A -- and that is a working
configuration. It is also a footgun, because which mode a program is in is
invisible at the submit site. `vq doctor HOST` reports the wrapper/interpreter
collision that indicates a program has been put in both.

### The `command_wrapper` composition contract

Read this before configuring a wrapper. Getting it wrong cost ~250 pbs-cluster
campaign jobs on 2026-07-20, and it is **not** vibe-qc-specific -- the same
shape was reproduced deliberately against ORCA on 2026-07-23
(`/home/USER/bin/orca /home/USER/bin/orca smoke_h2.inp` → *expect a '$', '!',
'%', '*' or '[' in the input*). It applies to every program that keeps a hook.

**The rule: the wrapper and the job's interpreter are alternatives, never a
pair.** Configure a program's launcher *either* as the submitted interpreter
(`--python`, or a `[hosts.HOST.branches]` entry) *or* as `command_wrapper` --
not both.

Why: a single-file submit's command is **always** `[interpreter, script]`, and
`command_wrapper` is an argv prefix prepended to that command. Point both at
the same launcher and the launcher receives its own path as the script
argument, so its python parses its shell source:

```
# branches.release   = "/home/USER/bin/vibeqc-release-python"   (a bash script)
# command_wrapper    = ["/home/USER/bin/vibeqc-release-python"]
# submitted command  = [vibeqc-release-python, run_batch.py]
# rendered job line  -> /home/USER/bin/vibeqc-release-python vibeqc-release-python run_batch.py
# job dies at        -> SyntaxError: set -euo pipefail
```

vq de-duplicates this at render time: the wrapper is injected **only if** the
command does not already start with it, matched by exact argv prefix *or* by
resolved program head (so `vibeqc-release-python`, `~/bin/vibeqc-release-python`
and `/home/USER/bin/vibeqc-release-python` all count as the same program). A
skipped injection is logged as a WARNING in the daemon log naming the job,
program, wrapper, and command head.

The dedup keeps jobs running, but it is resolving an ambiguity by guess, so
`vq doctor HOST` reports the collision as a failing `scheduler_command_wrapper`
check. Fix the config; do not rely on the dedup.

A wrapper around a genuinely different program (`orcasub` wrapping `orca`) is
unaffected and still wraps normally.

Scheduler polling and result collection are retryable operational probes. A
temporary SSH, `qstat`, or remote `tar` timeout should leave the prior vq/PBS
state in place and retry on the next reconcile pass, not infer that the job
finished. The production runner gives `qstat` a longer bounded timeout than
generic remote shell commands and gives workspace tar creation the same large
budget as the download that follows it. Live and detail observations run as one
bounded read-only flight per scheduler host, outside the daemon's dispatch
thread; only the daemon thread applies their results. A slow host therefore
cannot delay local dispatch or another scheduler host, and repeated ticks do not
start duplicate polls for the slow host. If Slurm returns its exact invalid-job
diagnostic for a mixed batch, vq probes only the missing candidates separately.
An independently rejected handle enters the normal exit-marker and final-fetch
terminal fence, while live siblings continue and ambiguous failures remain
nonterminal. Operating-system and subprocess failures raised by the scheduler
runner are contained by the same host-local observation boundary, so another
host and local dispatch still make progress. The first warning names both the
vq job ID and scheduler handle.

Use `vq usage HOST` when you need a retained-history accounting summary for a
host or scheduler target. It reports CPU-hours and wall-hours by tag by default;
`--by submitter`, `--by host`, and `--by none` switch the grouping, and `--json`
emits the same data for dashboards. Active jobs are excluded unless
`--include-active` is passed. For scheduler-backed jobs, vq prefers the final
scheduler walltime field when available, so pbs-cluster usage does not charge time a
qsub job spent queued before compute started.

For pending jobs, `vq status HOST JOBID` also shows a best-effort queue ETA when
retained completed-job history can estimate the jobs ahead of it. The estimate
uses jobs in the same local queue or scheduler target, matching by tag, command,
and CPU count with broader fallbacks. Treat it as a dispatch-turn estimate: it
does not include currently running jobs, dependency gates, future resource-fit
decisions, or site scheduler policy.

A local request that already exceeds the daemon's configured base `max_cpus`
or effective `max_mem_mb` is different: `vq list` labels it
`pending (over cap)`, `vq status` labels the state and reports its queue ETA as
unavailable, and `vq overview` raises an `over-cap pending` host alarm. The job
remains durably `PENDING`, because increasing the daemon cap is a supported
operator recovery. Queue/status JSON include `pending_over_capacity` and
`configured_capacity_overages`; overview JSON includes
`over_capacity_pending_jobs`. These fields compare only configured base caps
(`mem_mb`, or `default_job_mem_mb` when memory is undeclared), not current load,
drains, live free RAM, quotas, or scheduler-target resources.
`pending_over_capacity` is `true` for a known overage, `false` for an
applicable pending local job known to fit, and `null` when capacity is unknown
or classification does not apply. This includes an older constrained-memory
snapshot that cannot report the daemon's undeclared-memory charge. The
overview count likewise uses `null` when its queue or capacity snapshot is
unavailable or incomplete; zero is a known empty alarm count.

For scripted acceptance, use `vq submit HOST --json ...`. Its
`capacity_warnings` list carries these warnings on stdout for both local and
forwarded remote submits, including keyed idempotent replays of a still-pending
job, while the normal no-flag stdout remains exactly the bare job ID. Do not
treat an empty `dispatch_holds` list as proof that the job fits; inspect
`capacity_warnings` as well.

`vq pause HOST JOBID` and `vq pause HOST --all` on a scheduler host mean
scheduler hold, not compute-node suspension. The driver polls qstat and applies
`qhold` only while the job is still queued by the scheduler; if the job is
already running on a compute node, vq refuses instead of pretending the
calculation was frozen. `vq resume HOST JOBID` and `vq resume HOST --all` release
those holds with `qrls` and preserve the usual `paused_by` / held-time
accounting. To stop an already-running scheduler job, use `vq kill HOST JOBID`
and resubmit.

---

## ORCA MPI runtime for `%pal` jobs

Symptom: an ORCA input with `%pal nprocs N end` fails at startup with a dynamic
loader message like:

```text
Library not loaded: libmpi.40.dylib
Referenced from: .../orca_startup_mpi
```

or:

```text
orca_startup_mpi: error while loading shared libraries: libmpi.so.40:
cannot open shared object file: No such file or directory
```

Serial ORCA can still run in this state. The failure is the ORCA MPI launcher,
not a DLPNO or input-deck numerical problem.

`vq programs` probes this now. A healthy ORCA install reports:

```text
ORCA MPI startup loads
```

If it reports `ORCA MPI startup cannot load runtime`, treat ORCA as serial-only
on that host even though the program status remains `OK` for serial jobs. Do not
submit multi-core ORCA inputs there. Either run the ORCA reference serially
(omit `%pal` or use `nprocs 1`) or restart the vq daemon from an environment
that exposes the MPI library directory. On the workstation/Homebrew layout, the
required setting is:

```sh
export DYLD_LIBRARY_PATH=/opt/homebrew/opt/open-mpi/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}
vq programs localhost --json   # confirm ORCA reports "ORCA MPI startup loads"
```

On Linux ORCA builds that report a missing `libmpi.so.*`, add the directory
containing that shared library to the daemon environment's `LD_LIBRARY_PATH`
instead, then rerun the same `vq programs` probe.

Important: queued jobs inherit the already-running daemon's environment. Setting
this in a new shell does not fix a daemon that was started earlier without it;
restart the daemon only when it is safe to interrupt/recover active local jobs.

---

## `vq daemon health` (v0.5.49+) -- host lifecycle diagnostic

When `vq doctor` says the client can reach remote `vq` but a host still
"feels off", this is the deeper lifecycle verifier. It contacts the host and
cross-checks the four sources of truth on the daemon's existence:

* `loginctl show-user $USER` -- does PAM think the user has a
  session manager? Is `Linger=yes`?
* `pgrep -f 'systemd --user'` -- is the user-systemd manager actually
  alive? Defunct (zombie)?
* `systemctl --user show vq-daemon` -- does systemd see the unit?
  What's `ActiveState` / `MainPID` / `ExecStart`?
* `<state_root>/daemon.pid` -- does the pidfile reference a live PID
  whose `/proc/<pid>/cmdline` matches vq?

```sh
vq daemon health compute-d          # text verdict
vq daemon health compute-d --json   # machine-readable, for CI
```

Output is a short verdict line + per-source details. A healthy
daemon prints `verdict: OK`; degraded states (`zombie_user_systemd`,
`pidfile_stale`, `cmdline_mismatch`, `unit_inactive`, …) each map
to one of the recipes below. The `--json` form is what
`tests/integration_smoke.py` and any fleet-wide health probe
should parse.

Why this verb exists: pre-v0.5.49 diagnosing a sick daemon meant
manually running four shell commands and reasoning about their
combinations. Three of the four had non-obvious failure modes --
e.g. `systemctl --user` returns "Connection refused" when the
user-systemd manager is a zombie, which looks like "daemon not
installed" but isn't. The contract verifier consolidates the
reasoning and surfaces a single verdict.

---

## "Failed to connect to user scope bus via local transport: Connection refused"

You see this on `systemctl --user <anything>`. The user-mode systemd
manager is gone or in a degraded state -- either it never started
(no PAM session has touched the user yet) or it died and wasn't
cleaned up.

### Diagnose

```sh
loginctl show-user $USER | grep -E 'State|Linger'
pgrep -laf 'systemd --user'   # should show one process, not "<defunct>"
ps -o pid,ppid,stat,cmd $(pgrep -f 'systemd --user' | head -1) 2>/dev/null
```

If you see a zombie (`Zs` in STAT, `[systemd] <defunct>`), the user
manager died and its parent (PID 1) didn't reap it. Two scenarios:

* **You're logged in via non-interactive SSH** (e.g. `ssh host
  "command"`). On some hosts PAM doesn't spawn a fresh user manager
  for non-TTY sessions, and `systemctl --user` can't find one. Try
  again with `ssh -t host "command"`, or just log in interactively.
* **The user manager was killed by global OOM** (most common cause
  on compute-d / compute-a). It died mid-flight, the runtime dir is stale,
  and re-login via PAM hasn't fixed it. → use the *Force-revive*
  recipe below.

### Force-revive

A graceful `restart` will hang waiting for the zombie to ack SIGTERM
(it can't; it's already dead). Skip the graceful path. Run as
sudo:

```sh
sudo systemctl kill --signal=SIGKILL user@$(id -u).service
sudo systemctl reset-failed user@$(id -u).service
sudo systemctl start user@$(id -u).service
sleep 3
systemctl --user --no-pager status vq-daemon | head
```

The `kill` reaches only `user@$UID.service` and its child units
(your services like `vq-daemon`) -- **not** the `session-N.scope`
your SSH lives in, so your terminal stays alive. With `Linger=yes`
set and `vq-daemon.service` `enabled`, the daemon auto-launches as
soon as the fresh user manager comes up.

### Don't try

* **`systemctl --user restart vq-daemon`** when user-systemd is a
  zombie -- connection-refused, no progress.
* **`sudo systemctl restart user@$UID.service`** when the old
  manager is zombie -- hangs on TimeoutStopSec (90s default), and on
  a host that's already under memory pressure this can be enough to
  push the box past the edge into needing a hard reset. Today's
  incident #2 was exactly this.
* **`pkill -9 -f 'systemd --user'`** -- zombies are already dead;
  signals to them are no-ops. Only the parent's `wait()` reaps a
  zombie, and the user-side parent is PID 1, which should reap
  automatically but sometimes doesn't.

---

## Daemon running stale code after an on-disk reinstall

vq may be editable-installed or copied into its virtualenv. Moving the checkout
or reinstalling the package changes bytes on disk, but a *running* daemon has
already imported its modules; that in-memory code is frozen until the process
restarts.

### Current managed path (v0.25.0)

For the vq daemon's own managed environment, use the first-class exact-pin
entry point:

```sh
vq self-update --expected-sha <full-40-hex>
# or select one immutable accepted fleet report explicitly:
vq self-update --accepted-report vX.Y.Z
```

The command discovers the target environment from the loaded systemd-user or
launchd service executable. It shares the fleet rollout lock, preserves the
admin update marker and scoped pause/resume transaction, and requires a
successful service restart plus daemon source/tree provenance before exiting
zero. It has no host, environment, `--force`, or `--no-restart-daemon` option.
In multi-user mode it enforces the normal admin token gate. A prior update
marker or concurrent rollout must be reconciled through its owning workflow;
the self-update command never overwrites or replays it.

When the selected program uses `vibe-queue/scripts/update.sh`, the outer admin
transaction passes a parent-bound restart-coordination handshake. The script still
owns its venv and build locks, install, marker, and rollback work, but leaves
the daemon stopped after the outer transaction has quiesced it, until that
outer transaction restores or starts and verifies the one exact service.
Stale inherited handshake values are scrubbed or rejected; this is
lifecycle coordination within the same operator authority, not authentication.

Before the recovery receipt is written or the service is stopped, the managed
self-update resolves Git's active index and checks its sibling lock path. Any
present `index.lock` is a fail-closed admission error: the current daemon keeps
serving, the error names the exact lock, and vq leaves it untouched. Confirm
that no Git process is using the checkout before removing a stale lock and
retrying. File size or age is not ownership evidence; a newly created lock can
be empty while its writer is live. Vq's own read-only status and update-script
cleanliness probes disable optional Git index refreshes, while checkout and
other required mutations retain their normal locks.

An older accepted report or SHA is rejected. This first-class managed surface
only permits an exact descendant (or the already-installed commit), because an
older checkout can predate the inherited lifecycle transaction protocol.
Emergency rollback therefore remains an operator recovery procedure outside
`vq self-update`; divergent or ancestry-unknown history is also rejected.

`vq admin update <env>` detects when `<env>` is the venv from which
the running `vq-daemon` was launched and finishes with
`systemctl --user restart vq-daemon` so the freshly-installed code
takes effect. The output makes the restart explicit:

```text
== OK ==

==> vq self-update detected — restarting vq-daemon
   systemctl --user restart vq-daemon ... done (PID 1234 -> 5678)
```

Detection uses `systemctl --user show vq-daemon -p ExecStart --value`
to read the path the daemon was launched from, and compares it
against the env's venv bin dir (derived from `prog.python` in the
`[programs.X]` registry). When the env is **not** vq's venv (the
common case: `vq admin update vibeqc-dev`, etc.), the daemon is
left alone. See `admin._detect_vq_self_update` /
`admin._maybe_restart_daemon`.

`vq admin update <env> --no-restart-daemon` is accepted only when the service
manager proves that `<env>` is not the serving daemon environment. A serving
self-target is rejected before pause, fetch, or checkout mutation; use
`vq self-update`, whose success contract includes the exact restart.

**When `systemctl --user` is unreachable** (zombie user-systemd --
see § "Failed to connect..." above) AND the env is vq's venv, the
update exits non-zero with the recovery recipe pointer rather than
silently leaving the daemon on stale code. Recover user-systemd
first, then `systemctl --user restart vq-daemon` manually.

**Post-restart readiness window scales with the state dir.** After
the restart, the update polls the daemon's RPC until it answers
with the expected source SHA. The window is 30 s base plus 10 ms
per queued job spec, capped at 600 s -- a restarted daemon scans
every `queue/*.json` before its RPC socket answers, so a driver
carrying thousands of jobs legitimately needs minutes, not seconds
(2026-07-25: ~12.6k specs on the workstation driver put readiness
past a flat 30 s window, and two healthy restarts were reported as
`daemon restart FAILED` and needed `vq admin mark-ok`). Override
the computed window with `VQ_DAEMON_HEALTH_TIMEOUT` (seconds) in
the client environment if a host still needs longer.

### Historical pre-v0.5.42 manual recipe

An old, hand-managed vq install required this after every
`git pull && pip install -e .`:

```sh
systemctl --user restart vq-daemon
```

…to actually run the new code. This is historical incident guidance, not the
current serving-environment update path. Current vq rejects restart suppression
for its serving self-target; use `vq self-update` or the managed `vq admin
update` transaction above.

### How to tell if you're affected

* `vq --version` reports the *installed* wheel version (reads from
  the on-disk metadata).
* `vq daemon ping --json` reports the running daemon's version and the
  source/tree identity it captured at startup.

Process uptime predating the last reinstall is a useful supplemental signal:

```sh
ps -o pid,etime,cmd -p $(pgrep -f 'vq.*daemon' | head -1)
```

If `ELAPSED` is older than your last upgrade, compare the direct identities
below; do not infer a healthy restart from installed metadata alone.

**Authoritative direct signal (v0.24.x+): the daemon's own tree digest.**

```sh
vq daemon ping --json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["source_sha"], d["source_tree_sha256"])'
vq source-tree-sha256    # what the installed package on disk digests to
```

`source_sha` is a *declaration*: it comes from a `SOURCE-SHA` marker or from a
git checkout that happens to enclose the package, and either can describe a
commit whose code is not what the process imported. `source_tree_sha256` is
derived from the package bytes themselves, and like `source_sha` it is captured
once at daemon start -- so a daemon that never restarted keeps reporting the old
digest even after the files on disk change. That is what makes the comparison
meaningful:

| ping digest vs installed | ping SHA vs checkout | reading |
|---|---|---|
| equal | equal | healthy |
| equal | differs | the code is current; the **declaration** is stale. On a non-editable install, restamp: `vq source-sha --write-marker <40-hex>`. On an editable one there is no marker in play -- the checkout moved after the daemon started; just re-run the update |
| differs | differs | the **daemon** is stale. It did not restart, or it restarted from a different install |
| differs | equal | not treated as a failure -- see below |

`vq admin update` applies this rule after a self-update restart, so a
correct-code / stale-declaration host now verifies instead of failing, and the
failure message names which of the two halves is wrong.

**The asymmetry in the last row is deliberate.** The digest can *rescue* a
verification the SHA comparison would have failed; it cannot *fail* one the
SHA passes. Making it authoritative would tighten availability rather than
precedence: any host whose admin CLI and daemon resolve different installs
would start failing updates that are correct today, and on a multi-user host
that split is the designed arrangement, not a fault. So a matching SHA still
verifies on its own, and the digest is reported alongside for diagnosis.

The pair is persisted as `last_daemon_expected_source_tree_sha256` /
`last_daemon_actual_source_tree_sha256` in `vq admin status --json`.

Backward compatible on purpose: a daemon older than the `source_tree_sha256`
ping key reports nothing there, and verification falls back to the SHA
comparison rather than failing. Requiring the digest would have failed every
host on the release that introduced it.

### Why this matters

Pre-v0.5.42 case (2026-05-16): v0.5.40's parallelism cap was on
disk, but the daemon in memory was still on v0.5.39 with no cap at
all. The next `vq admin update` would have built with `nproc`
workers -- the very bug v0.5.40 was meant to fix. v0.5.42 closes
this gap by making the restart automatic.

---

## `systemctl --user restart vq-daemon` hangs

Two known causes:

1. **Zombie user-systemd** -- see above, use the force-revive
   recipe.
2. **Daemon has running jobs that aren't terminating.** Default
   `TimeoutStopSec` for `vq-daemon.service` is 90s; if any spawned
   `*.scope` job ignores SIGTERM, systemd waits the full timeout
   before escalating to SIGKILL. Watch with
   `journalctl --user -u vq-daemon -f` in another terminal.

If you need to force-stop immediately:

```sh
systemctl --user kill --signal=SIGKILL vq-daemon
systemctl --user start vq-daemon
```

This drops any in-flight jobs to terminal state on next daemon
poll. Jobs that were running in their own `vq-job-*.scope` cgroups
are *not* killed by this -- they keep running independently and
will be picked up by the daemon's reaper on restart.

---

## Host hangs / hard reset

If the box becomes completely unresponsive (SSH refuses, console
frozen, load average climbing past nproc, swap thrashing), assume
memory pressure. Most common cause on the fleet has been
unbounded `ninja` parallelism during a `vibe-qc` rebuild -- see the
2026-05-16 post-mortem.

### Hard-reset recovery checklist

After the box boots, in order:

```sh
# 1. Network — NordVPN sometimes re-engages killswitch + autoconnect
#    on reboot, blocking inbound SSH on the public route. (compute-d)
nordvpn set autoconnect off
nordvpn status

# 2. User-systemd healthy? No zombies?
pgrep -laf 'systemd --user'

# 3. Daemon auto-launched from disk?
systemctl --user --no-pager status vq-daemon | head
~/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq --version

# 4. Inspect what got orphaned at the reset
~/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq queue
```

Jobs that were running at the moment of reset land in `killed` or
`failed` after the daemon comes back up -- that's the normal
recovery path, not data loss in the queue sense (the queue state
file survives reboots). The *job's* output is lost if it wasn't
checkpointed.

`vq cleanup <jobid> --archive -x` archives + removes the orphans
once you've inspected them (archiving is a `vq cleanup` flag, not a
standalone `vq archive` command). `vq resubmit <jobid>` (v0.6.8+)
rerun the ones you want to
retry -- fresh jobid + fresh workspace (deep copy of the source's),
spec inherits cpus / mem_mb / wall_time / priority / retry_max /
tags / job_name / branch / recover_on_reboot from the source.
Override any of those per-flag at resubmit time
(`vq resubmit <jobid> --cpus 16 --wall-time-seconds 14400`).

---

## Preventing the next hard reset

These all landed on 2026-05-16 as direct responses to the
post-mortem. If a host on the fleet doesn't have them, it's at
risk.

* **vq daemon path** (v0.5.41): admin update injects
  `CMAKE_BUILD_PARALLEL_LEVEL` capped at 6 + `nice -n 19 ionice -c
  3` argv prefix. See `admin._safe_build_parallelism` /
  `admin._build_niceness_prefix`.
* **Interactive path** (2026-05-16, vibe-qc side):
  `scripts/update.sh` self-re-execs under `nice -n 19 ionice -c 3`
  and exports `CMAKE_BUILD_PARALLEL_LEVEL` capped at 8. Same
  formula as the daemon path but slightly looser cap because the
  user is watching.
* **Per-job cgroup MemoryMax** (v0.5.x): each `vq submit`'d job
  runs in its own transient `vq-job-*.scope` with a memory cap.
  This is the protection layer that's *always* on, and it's why
  the queue path has never caused an OOM -- only interactive
  builds and admin-update have.

If you ever find a build path that ISN'T capped, file it: there
should be no unbounded `ninja -j` invocation anywhere in the
fleet's surface area.

---

## Keeping `vibeqc-release` current with `vq admin auto-update` (v0.6.11+)

After a release chat tags `vX.Y.Z` and pushes it, the fleet's
`vibeqc-release` envs need a `git pull + rebuild` to actually
serve the new tag. Manual one-liner:

```sh
vq admin update vibeqc-release compute-d --tag vX.Y.Z
vq admin update vibeqc-release compute-a --tag vX.Y.Z
```

For unattended polling, `vq admin auto-update` is the safer
single-verb form (v0.6.11+):

```sh
# Check if compute-d's vibeqc-release is behind the latest tag.
# Probe-only: no apply, exit code reflects "drift" vs "current".
vq admin auto-update vibeqc-release compute-d --dry-run

# Apply if there's drift. Exit code 0 on success or no-drift,
# non-zero on apply failure or git-probe failure.
vq admin auto-update vibeqc-release compute-d
```

The verb queries `git ls-remote --tags origin` on the env's
clone, filters to semver-shaped tags (`vMAJOR.MINOR.PATCH`),
picks the newest by SemVer precedence (so a final release outranks its release
candidate and `rc.10 > rc.2`), refuses to move a newer installed tag backward,
and
enumerates every strict SemVer tag pointing at `HEAD`, selects the highest
unambiguous local precedence, and binds both the selected remote tag and its
peeled full commit SHA. On drift it calls
`update_env(env, expected_tag=newest, expected_sha=peeled_sha)` -- so named
tag and commit verification both fire and a bad pull
(rebase, branch divergence) fails the apply.

**Tag-mode vs branch-mode**. With the default
`auto_update_policy = "tag"` the verb tracks the newest semver
tag only (right for `vibeqc-release`); it applies inline via
`update_env(env, expected_tag=newest, expected_sha=peeled_sha)`. With
`auto_update_policy = "branch"` (v0.7.4+, right for `vibeqc-dev`)
it tracks `origin/<branch>` and -- since v0.12.x -- applies by
**submitting a capped `vq build-env` JOB** to the local daemon
rather than rebuilding inline in the timer process (see *build-env
wedge hardening* below). If that branch target is the vq daemon's own managed
environment, auto-update rejects it before job submission. Use `vq self-update`
with an exact immutable selector so the daemon restart and provenance gate
cannot be skipped.

### Wiring an unattended timer

The verb is the hard part; if you want it polled hourly via
systemd-user, create two unit files in
`~/.config/systemd/user/`:

```ini
# vq-auto-update.service
[Unit]
Description=vq admin auto-update vibeqc-release (latest tag)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=%h/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq admin auto-update vibeqc-release
Nice=10
IOSchedulingClass=idle
```

```ini
# vq-auto-update.timer
[Unit]
Description=hourly vq admin auto-update for vibeqc-release

[Timer]
OnUnitActiveSec=1h
RandomizedDelaySec=10min
Persistent=true

[Install]
WantedBy=timers.target
```

Then:

```sh
systemctl --user daemon-reload
systemctl --user enable --now vq-auto-update.timer
systemctl --user list-timers vq-auto-update
```

Tail the journal to see what each tick decided:

```sh
journalctl --user -u vq-auto-update.service -f
```

Three opt-ins keep a tag-mode timer safe: the env name (so a
fleet-wide misfire requires explicit setup per host),
latest-tag-only (no dev-tip auto-deploy), and the timer itself
(disabled by default).

---

## `build-env` wedge hardening + dev-HEAD routing (v0.12.x)

The 2026-06-26 fleet incident: the dev-HEAD auto-update timer
wedged compute-c/compute-b/compute-a. A `vq build-env vibeqc-dev` rebuild ran
12h+ with empty stdout while holding 6 CPUs, a duplicate stacked
behind it `starved`, and a half-landed rebuild left an
importable-but-ABI-broken env (newer Python tree against an
un-rebuilt `_vibeqc_core.so`). The manual reset was `vq admin
update vibeqc-dev <host>` per host; the three hardening changes
below stop the timer re-wedging.

**Supervised build (no silent wedge).** The `update_script` runs
in its own process group under wall-clock + stall + heartbeat
supervision:

* **Stall cap** `VQ_BUILD_STALL_TIMEOUT` (default 3600 s) -- a finite,
  non-negative interval that reaps the build if it emits *no output at all*
  for that long. A live
  l=6-enabled libint build can be quiet during a large generation or
  compilation step, so the one-hour default avoids reaping healthy work
  while still catching the historical multi-hour wedge. `0` disables.
* **Wall cap** `VQ_UPDATE_SCRIPT_TIMEOUT` (default 14400 s) -- the finite,
  strictly positive hard upper bound for the full update script. Raise it only
  for a host whose measured cold build needs more than four hours.
* **Heartbeat** `VQ_BUILD_HEARTBEAT_INTERVAL` (default 120 s) --
  log/stdout progress (`still running, Ns elapsed, Ms since last
  output`) so a running build is never opaque. `0` disables.

Direct venv delegation validates both build caps before SSH and always sends
their effective values, including the 14400/3600 defaults, to the remote CLI
through an exact two-name allowlist. Invalid, non-finite, or out-of-domain
values fail locally before an SSH process starts. No ambient variable,
credential, token, heartbeat setting, or outer timeout is forwarded by this
path.

A single delegated venv target has a separate local SSH observer,
`VQ_REMOTE_ADMIN_UPDATE_TIMEOUT`. Its unset default has a 15000 s floor and
rises when needed to remain at least `VQ_UPDATE_SCRIPT_TIMEOUT + 600`. An
explicit value must be finite and positive and may be lower than 15000 s when
it still covers that 600 s margin; otherwise it refuses before SSH. Delegated
venv `--all` and `--all-hosts` batches have no one finite aggregate observer
over all
environments or hosts; the per-environment wall/stall watchdogs and SSH
liveness checks remain the bounded failure controls. Scheduler helper and
runtime actions retain their profile-owned deadlines and do not inherit these
venv values.

On either build watchdog cap the **whole** process group is SIGTERM→SIGKILL'd
(grandchildren ninja/cc1plus reaped, CPUs released) -- the pre-fix
`subprocess.run` timeout killed only the `bash` wrapper and
orphaned the compilers. Build jobs also carry a
`wall_time_seconds` so the watchdog reaps a wedged build even if
the in-process guard is missed.

**Dev-HEAD routing (no duplicate / retry storm).** A branch-mode
(`auto_update_policy = "branch"`) drift no longer rebuilds inline
in the timer process -- it submits a `vq build-env <env>` JOB to
the local daemon, which is cgroup-capped, watchdog-reaped, and
visible in `vq status`. The submit front door
(`vq.build_job.submit_build_env_job`) **dedupes** (skip if a build
for the env is already queued/running) and **backs off** after a
failure (15 min, doubling per consecutive failure, capped 6 h).
The backoff state lives at `<state_root>/build-backoff/<env>.json`;
a successful build clears it.

**Retention never outranks reconciliation.** Daemon-side auto-cleanup is a
synchronous retention pass and can be expensive on a driver with many retained
specs. It is therefore deferred while any local, reattached, or scheduler job
is active. This guarantees that a child which exits during a large queue sweep
reaches the next `poll()` / `waitpid()` promptly instead of remaining a zombie
with a falsely live spec. The elapsed cleanup policy remains due and runs when
the daemon is quiescent; an operator-requested `vq cleanup` remains explicit
and is not subject to this daemon-side deferral.

**Atomic rebuild (no ABI skew).** Set `import_check` on the env so
`vq admin update` can verify + roll back atomically:

```toml
[programs.vibeqc-dev]
kind = "venv"
python = "/home/USER/vibeqc-dev/.venv/bin/python"
git_dir = "/home/USER/vibeqc-dev"
branch = "main"
update_script = "scripts/update.sh --dev"
auto_update_policy = "branch"
import_check = "vibeqc"        # arms the atomic snapshot/restore + gate
```

When `import_check` is set (together with `update_script`), the
update snapshots HEAD + the package's `.so` under
`<git_dir>/python/<import_check>/` before the build, runs
an isolated import probe after it, and on a failed
build *or* a failed import probe git-resets the checkout and
restores the `.so` -- so the live env is only ever a consistent
`{Python tree, .so}` pair. Unset (the default) disables both the
probe and the rollback (right for pure-git envs with no native
extension). `vq admin status` / the auto-update JSON surface
`rolled_back` + `import_check_rc` for forensics.

For `import_check = "vibeqc"`, the inventory/update probe disables mpi4py
auto-initialization before importing vibe-qc. If mpi4py is installed it still
loads `mpi4py.MPI`, requires that the serial probe remains uninitialized, and
queries the native vendor/library. This avoids turning a health inventory into
an unsupported singleton `MPI_Init` on cluster login nodes. Site-specific MPI
identity remains a per-program `healthcheck_command` or deployment gate; do
not encode Intel/Open MPI policy into the portable import probe.

**Opt-in immutable per-SHA runtime slots.** A venv program with an absolute
`runtime_slot_root` uses `<root>/releases/<sha>` generations and requires an
exact `--expected-sha`. Each venv is created directly at its final absolute
generation path because editable-install files and console shebangs embed that
path; a temporary-path build cannot safely be renamed into place. The update
content-seals the source and venv, checks the interpreter is executable, writes
`.vq-immutable-runtime`, and only then atomically switches `current` while
retaining `previous`.

A failed build never moves `current`. Its durable sibling and in-generation
`building` receipts make the same SHA retryable, but cleanup is allowed only
for that exact incomplete transaction after both pointers and a complete
non-terminal spec census prove it unused. Direct `scripts/update.sh` use against
a released generation is refused; create and publish a new generation through
`vq admin update` instead.

Reclamation fails closed. It always keeps `current` and `previous`, aborts with
zero deletions if any visible spec cannot be read consistently, and keeps exact
per-SHA generations named by live specs. If a non-terminal spec stores only a
stable `current` or wrapper command, its exec-time SHA cannot be reconstructed
after later flips, so all generations are retained until that spec is terminal.
This may temporarily use extra disk, but cannot delete a runtime still held by
a running process.

---

## Admin update stuck -- `state=failed` marker (v0.6.0+)

`vq admin update <env>` admits a scoped marker lease at start and removes its
own lease only after terminal proof. The first lease may use the legacy-
compatible `<state_root>/admin-update-in-progress` path; additional disjoint
leases live under `<state_root>/admin-update-markers/`, with admission and
mutation serialized by a stable lock. Pre-v0.6.0 the marker was a boolean
("in progress") that left the recovery story ambiguous: was the env half-built?
did pause-all run? did resume-all finish? v0.6.0 turned the marker
into a state machine that records exactly where the update broke:

```
PAUSING -> PAUSED -> PULLING -> TAG_CHECKING -> BUILDING ->
RESUMING -> RESTARTING_DAEMON -> VERIFYING -> (removed = IDLE)
                                            \-> FAILED (sticky)
```

`vq admin status` shows the state + `phase_started_at` +
`failure_reason` when a marker is present. Current markers also include
`last_heartbeat_at` and a short heartbeat message. Phase transitions stamp
that field, and long update scripts refresh it on the normal build heartbeat
cadence. Scheduler-host update/provisioning commands (`vq admin update
<scheduler-host>`) refresh the same heartbeat while their remote SSH command
is still running. A delegated update that is still compiling therefore reads
as a live marker with a recent heartbeat instead of a stale/failed marker:

Fleet views use the same host-side diagnosis. `vq overview` text output shows
`marker_status`, PID status, summary, and heartbeat when a marker is present;
`vq overview --json` includes `admin_marker_status`,
`admin_marker_pid_status`, `admin_marker_summary`, `admin_marker_action`,
`admin_marker_heartbeat_status`, and
`admin_marker_heartbeat_age_seconds`. These fields are computed on the host
that owns the marker, so a fleet aggregator does not need to re-probe a remote
PID.

```text
$ vq admin status
admin update in progress (marker present)
   state:            failed
   phase_started_at: 2026-05-18T09:14:22+00:00
   last_heartbeat:   2026-05-18T09:14:24+00:00
   failure_reason:   git pull rc=128: fatal: unable to access ...
   started_at:       2026-05-18T09:14:01+00:00
   pid:              412318
   env:              vibeqc-dev
```

### Ordinary stale markers are auto-reaped; durable receipts are retained

Before durable recovery receipts, a marker was intended to gate dispatch only
while its update was in flight. The marker lives in the state dir, so one left
by a killed update -- or one whose host **rebooted mid-update** -- used to
persist and silently park every job at `pending`, while the host still
reported plain `up`/`OK`. compute-c and compute-b both wedged this way on
2026-06-18 (a `pid=66408` marker, `started_at` 5 days stale, survived a
reboot and held the queue idle until `vq admin clear-update-marker -y`).

Since v0.11.0 the daemon detects an **ordinary stale** marker -- one whose
writing `vq admin update` process is gone -- and reaps it automatically
(deletes it and logs a loud `WARNING`) at startup and on every dispatch tick.
The current v0.25.0 contract deliberately excludes markers that own a durable
managed-daemon transaction or paused-job scope: those receipts retain their
exact dispatch hold until explicit recovery. Staleness is diagnosed when:

* its recorded `pid` is no longer in the process table; **or**
* `pid` is alive but its `/proc` start-time no longer matches the
  `pid_start_time` fingerprint recorded at write -- the kernel recycled
  the slot to an unrelated process (classic after a reboot); **or**
* the marker is older than 24h (`ADMIN_UPDATE_MARKER_MAX_AGE_SECONDS`) --
  no real update runs that long, so it's a corpse the liveness probe
  couldn't otherwise settle (pre-v0.11.0 marker, or no `/proc`).

A *live* update -- pid alive and young, including the brief window where the
update process orchestrates a daemon restart -- is never reaped. A stale
durable receipt is also never reaped: `vq admin status --verbose` diagnoses it
and directs a managed receipt to `vq admin recover-update`, or a pause-only
receipt to `vq admin clear-update-marker`, which must prove that exact pause
scope resumed before unlinking. Only an ordinary marker may disappear through
the automatic reaper.

Heartbeat age is diagnostic only. A quiet heartbeat does not by itself mark a
marker stale, because some update phases are legitimately quiet and
`VQ_BUILD_HEARTBEAT_INTERVAL=0` disables build heartbeats. PID liveness,
`pid_start_time`, and the 24h age backstop remain the stale-marker authority.
For a delegated remote update, a fresh heartbeat plus a live PID means the
remote compile is still in flight; wait or poll `vq admin status HOST` instead
of clearing or recovering the marker.

### A delegated update runs detached; a dropped session no longer kills it

`vq admin update ENV HOST` delegates to `ssh HOST ... vq admin update ENV
localhost`. Until 2026-09-11 the remote updater ran inside that ssh session.
compute-b, compute-c and compute-d run systemd-logind with `KillUserProcesses=yes`, so
when the last ssh session ended, logind stopped the session's scope and killed
the build along with everything else in it (the fleet handover records the
evidence). The atomic rollback never ran, and each host was left at the
requested tag with a marker reading `building`, a dead pid, and a venv whose
`import vibeqc` failed on a half-built `libint2.so`. `nohup` and `setsid` do not
help on such a host: a new session is still inside the scope.

The remote updater now starts outside the session. Where `systemctl --user`
answers, it runs as a transient user service unit named
`vq-admin-update-<run id>`, which survives the session for as long as the user
manager does. That needs lingering (`loginctl enable-linger`), and the launch
warns when it is disabled. On a host with no user manager it runs as a session
of its own, and on Linux the launch warns that logind can still kill it. Look
at a running unit with `systemctl --user status vq-admin-update-<run id>`; its
output is appended to the run's `child.log`.

The SSH call only launches the updater and waits for it to activate, then
returns; the driver follows the build with short read-only polls of
`vq admin observe-update`. A dropped connection costs one poll. The driver
re-attaches for up to 30 minutes and says so on stderr while it waits:

```
lost contact with compute-b (...); the detached update keeps running there.
Re-attaching to 4f2c... for up to 1800s
```

The run id is chosen by the driver *before* the launch, so even a launch whose
response is lost is adopted by observation rather than reported as unknown.

To look at a run yourself, on the driver or on the host:

```sh
vq admin observe-update RUN_ID --host compute-b
```

The run id is in the marker: `vq admin status HOST --json` reports it as
`detached_run_id`, and `in_flight` is true for a detached build exactly as it
is for an attached one. The full build output stays on the host; read it with
`vq admin logs ENV --host HOST` as before. Only the phase narration is echoed
to the driver's terminal, which is what the attached command showed too.

The run record outlives the marker. A marker whose updater was killed is an
*ordinary stale* marker and the daemon reaps it (see above), but the run
directory is separate state and is not reaped with it — so
`vq admin observe-update RUN_ID` still says how that update ended, or that it
ended without saying, long after the marker is gone. Records are pruned to the
newest 20 per host, and a run that is still live is never pruned.

Set `VQ_ADMIN_NO_DETACH=1` to force the old attached delegation. It exists for
bisecting a transport problem, not for normal use. A target whose vq is too
old to understand `--detach` falls back to the attached path automatically,
with a warning naming the risk; that fallback is what lets a host be upgraded
by the very command being upgraded.

`vq admin auto-update ENV HOST` delegates the same way, and the drift apply
it runs on the host is the same real rebuild, so it had the same exposure and
gets the same treatment. The driver sends `vq admin auto-update ENV --detach
--detach-run-id RUN_ID localhost`, follows the run with
`vq admin observe-update`, and prints the auto-update report the attached
command would have printed. `--all` and `--all-hosts` detach per host, and
the old-remote fallback and `VQ_ADMIN_NO_DETACH=1` apply unchanged. Only
`auto-update --dry-run` stays attached: it changes nothing, so there is
nothing for a dropped session to interrupt.

### Lost SSH response: reconcile before another update

A mutating remote admin command is attempted once. SSH exit 255, an observer
timeout, or a local SSH helper terminated by signal can happen after the remote
command started. The CLI therefore reports the remote outcome as unknown and
does not replay the command. Do not immediately run the same update or use
`--force`.

Detaching narrows this case sharply but does not retire it. A delegated venv
update or auto-update still reports an unknown outcome when the detached run died without
publishing a terminal receipt (`state: lost`), when the host stayed
unreachable past the re-attach window, or when an adopted launch left no run
on the target at all. The advice below is unchanged for those.

First inspect the target named by the error:

```sh
vq admin status HOST --json
```

For a scheduler helper or runtime, `HOST` is the scheduler target; vq follows
it to the driver that owns the marker. Inspect the marker state and heartbeat,
the canonical LAST OK record, the admin log, and the exact live SHA or tag. If
the marker is live, wait and poll. If the exact target is already healthy, no
second update is needed. Start another update only after proving that the prior
command is terminal and that the requested target was not completed.

A failure that says the local SSH process was not started is different: no
remote mutation was attempted. A normal remote nonzero exit is also an
authoritative result and retains its specific error. Read-only auto-update
dry-runs do not receive mutation guidance.

### Interrupted `rollout-latest`: adopt the operation, do not replay the lane

`rollout-latest` adds a durable controller-loss boundary around those ordinary
admin commands. Before an action can run, it journals an immutable operation
identity bound to the accepted report, attempt, action, target, and canonical
argv. A detached local supervisor publishes ready; the controller journals the
operation before persisting authorization. The supervisor then acts as a local
launcher: it consumes that authorization and transfers its already-held
lifetime lease, without an unlock gap, to a fresh-session execution recorder.
The recorder validates the same immutable authorization, records durable
launch intent before child `Popen`, continuously drains a bounded
combined-output spool, and writes one terminal result. The child inherits the
recorder's isolated process group.

If the controller dies before run authorization, no action mutation is
authorized. The detached launcher waits only for the bounded authorization
window and can then commit the immutable abort/result decision. A later
controller reconciles that terminal non-execution. Run authorization, external
abort, and timeout-abort compete through the same create-once decision.

If only the rollout controller dies, run `vq admin rollout-latest` again. The
new invocation takes the global fleet fence, inspects operations and journals
from every report and host before its first live snapshot, and adopts the exact
authorized operation. If authorization exists but activation does not and the
lifetime lease is free, vq resumes that same operation and attempt through a
new launcher. It does not launch a second action or mint a replacement attempt.
Any outcome-unknown operation anywhere in that global preflight blocks every
otherwise safe pre-activation relaunch before journal mutation.

Host selection does not turn that global integrity pass into a filtered read.
Malformed receipts, unknown hosts, and orphaned operations on any host still
fail closed. The one host-local liveness case is an already-retained, closed
receipt whose accepted-report identity is older than the current report: with
`--only` or `--skip`, vq may carry it forward only when that configured host is
explicitly outside the requested scope. The journal remains byte-identical, the
host remains visibly fenced and deferred, and no legacy recovery or drain
release control is sent to it. Ordinary fleet snapshots can still make
read-only probes. The same mismatch on an in-scope host or in an unscoped
invocation still requires an explicit fleet-global `--reconcile-legacy`.

If the first launcher has already handed off the lease, the recorder survives
controller or launcher death and continues to activation, child execution, and
result. Driver success still forces fresh-interpreter re-entry. Scoped `--only`
or `--skip` selection cannot hide an older operation because selection happens
after global reconciliation.

A busy pre-activation lease is treated only as evidence that some process owns
the operation, not as proof that activation will appear. One bounded deadline
covers that state. vq can terminate and reap a launcher handle it owns, but it
never signals an adopted lease owner or the detached recorder. It re-reads
activation and result after cleanup to close exit and publication races. A
free lease permits only explicit same-operation resumption; an unidentified
busy owner fails closed rather than starting another launcher indefinitely.

The execution recorder stores at most 4 MiB while continuing to drain and
discard later bytes so the child cannot block. Adopted bytes go only to stderr,
keeping JSON stdout clean; `vq admin logs` is the complete canonical
transcript. A verified nonzero result fences that host against replay in the
same controller pass while independent hosts may finish. Only after a fresh
plan durably records that host-local skip can a later explicit invocation
authorize a new attempt.

Owned rollout holds follow the operation. A local full drain is confirmed by
its exact `reason` and `set_at`; a scheduler claim is keyed by its exact owner.
Live, outcome-unknown, and verified failed actions retain that protection.
Release requires trustworthy reconciliation proving that no protected action
remains in one of those states and targets only the exact generation. An older
report's scheduler hold that lacks a complete control-host/release identity
fails closed under a newer report instead of being guessed or migrated.

For a canonical scheduler host with more than one pending non-driver action,
source commit `7f65b8e58` makes that outer protection exact across aliases.
The group contains the canonical target and every transitively resolved
nonlocal alias. Each target is bound to its own configured `scheduler_driver`
control key; excluded, unresolved, and local aliases are not inferred. The
complete group is journaled before the first control call, and no protected
update runs until every exact target is confirmed. Direct `vq admin update`
and a rollout with only one pending scheduler action are unchanged.

Each journal record carries its exact target, canonical `action_host`, stable
`control_host`, and deterministic per-target owner. Acquire and release
failures remain target-local, but durable child retention follows the
canonical action host so controller loss or a verified failed child cannot
unlock one alias early. `--only <canonical>` retains the complete group;
selection naming only an actionless alias still fails closed.

Plan-bound alias records are semantically checked before any control
mutation. Alias removal, reparenting, malformed ownership, or a changed
control binding produces a zero-control failure. Fleet-global reconciliation
defers those records until the matching plan can validate them, and an
obsolete report produces an actionable fence. Historical canonical and
legacy journal shapes keep their compatibility paths.

**Do not rename or repoint a target key, its canonical relationship, its
`scheduler_driver` control key, or that control key's SSH routing while a
rollout hold is pending.** Restore the recorded mapping and reconcile first.
Do not try to repair semantic drift by broad-releasing a scheduler target.

When a terminal failed run reaches finalization, `rollout-latest` emits exactly
one valid `vq.fleet.rollout_result/2` before exit 1. The additive
`retained_rollout_holds` block projects owned journal claims recorded as
`active` or `cleanup-failed`; text mode sanitizes and promotes `FAILED HOST`
and `RETAINED ROLLOUT HOLD`. This is persisted journal evidence, so the text
explicitly says current liveness is not asserted. Check the live drain before
releasing the exact generation. Existing `preserved_external_holds` and
`PRESERVED EXTERNAL HOLD` reporting remain unchanged.

Source commit `8869ca33e` adds a distinct final-only observation after
that journal finalization. While the global rollout fence is still held, an
executing run queries each daemon observation endpoint at most once and adds a
read-tolerant `drain_liveness` block to the same
`vq.fleet.rollout_result/2`. The block projects exact configured targets into
`inactive_hosts`, `active_holds`, and `unknown_hosts`. Text says
`OBSERVED ACTIVE ... (final sweep as of <time>)`; it asserts activity only at
that recorded observation time. `preserved_external_holds` remains historical
non-interference evidence, and `retained_rollout_holds` remains journal
evidence. Neither is replaced or reinterpreted.

The hidden observation path requires a supported daemon
`vq.drain.read_only_snapshot/1` response. The daemon holds the stable legacy
and scheduler-lease locks together in legacy-then-lease order, copies bounded
regular JSON files, and projects timed expiry only in memory. It never creates
or rewrites drain state. The caller bounds the process group, timeout, combined
capture, UTF-8 decode, JSON depth, fields, and provenance. A missing old-daemon
method, busy or unreadable lock, malformed or incomplete response, or timeout
becomes explicit unknown coverage; it never falls back to a direct state-file
read. Scheduler-lease parsing is shared with dispatch, so unreadable lease
state produces an observed safety-fail-closed full hold rather than a false
inactive result. The lease writer refuses output above the same 1 MiB store
limit.

Rollout ownership requires exact unsanitized journal identity: full holds match
the control host, reason, and `set_at`, while scheduler leases match the exact
target and owner. Display sanitization happens only after classification. The
sweep makes no mutation, does not run for `--dry-run` or `--verify-only`, and
cannot alter result status, selection verdict, or exit code if observation is
unavailable. Plan `/2`, result `/2`, and verify `/3` remain unchanged. The
final-liveness increment itself did not change acquisition; later source
commit `7f65b8e58` closes the bounded outer-bracket alias gap described above
without changing those schemas, accepted-report checks, or observation
semantics.

The alias-hold closure is source-only. No accepted report, live host, root
daemon, scheduler, drain, or fleet operation exercised it. Verification
passed 243 owned tests, an independent 509-test matrix, and full vq with
`5,861 passed, 12 skipped in 263.29s`; full Ruff, compileall, and diff checks
were clean, and the independent security review was CLEAR.

This is local controller and launcher recovery, not general remote-child
adoption. If the execution recorder dies or child launch fails after
activation, the missing result is outcome-unknown and non-replayable.

R4b.1 commit `1483c980e` narrows one remote response-loss window. A
scheduler-runtime rollout action using `detached_build = true` with a fixed
`update_host` exports an exact operation id, request digest, and authorization
nonce to its outer child. The runtime path revalidates the live outer phase,
action, host, program, SHA, and tag before the admin marker or SSH, then writes
one owner-only, create-once `scheduler-command.json` binding before the remote
launch. That binding pins the fixed target, program and mode, random full run
id, normalized private run directory, exact argv digest, and canonical remote
request digest.

The build host runs a fixed embedded Python helper with a bounded canonical
request on stdin. The deployment argv is executed without shell
interpretation, in a fresh session. Its random owner-only namespace retains
create-once request, lease, activation, and result receipts and at most 4 MiB
of combined output while draining later bytes. A lost or ambiguous launch or
poll response can only read that exact bound run. It never scans a legacy
directory and never launches a replacement. Invalid or unavailable evidence
leaves the remote outcome unknown.

Remote return code zero is only `deploy-completed-unverified`. The same live
outer admin action must still pass the independent login-host verification
before LAST OK advances or the marker clears. Remote evidence does not create
the outer operation's `result.json`, release a hold, or make recorder death
replayable. If the recorder or outer child dies, global reconciliation still
blocks on the local outcome-unknown action and does not positively query the
remote receipt. Inspect the local binding, retained remote evidence, marker,
logs, exact live identity, and drain; do not replay.

This source increment excludes scheduler-helper actions, Slurm
`update_allocation`, and manual commands without the validated outer context;
those retain their previous protocols. No accepted report carries it, and no
live host, root daemon, scheduler, drain, or fleet operation exercised it.
Focused evidence: `107 focused and 373 adjacent tests passed; independent review passed 384 focused/adjacent tests`. Complete vq suite:
`5,776 passed and 12 skipped in 181.79s`. Static, diff, and independent review:
`full Ruff, Python compilation, git diff checks passed; independent review CLEAR`.

Commits `4683a1d3b` and `01583c6d1` are source-only. No accepted report carries
them, and no live host, root, scheduler, drain, or fleet operation exercised
them. Their combined focused matrix passed 482 tests with one skip; the
complete source suite passed `5732 passed, 12 skipped in 182.01s`; Ruff,
Python compilation, and diff checks passed, and independent review is CLEAR.

### Recovery flow

1. **Read `failure_reason`.** It captures the specific diagnostic
   (e.g. `"git pull rc=128"`, `"daemon restart failed: systemctl
   --user is unreachable"`, `"--tag mismatch: HEAD is v0.7.4, not
   v0.8.0"`). Fix the underlying cause first.
2. **Follow the marker's recovery class.** A normal caught failure attempts its
   token-scoped resume, but a SIGKILL, reboot, or other hard interruption can
   leave a durable receipt and paused jobs. Do not assume a `finally` block ran.
   `vq admin status <host> --verbose` identifies the required path:

   ```sh
   # Managed receipt: reconcile and prove exact files/service/jobs.
   vq admin recover-update <host>

   # Ordinary marker, or pause-only receipt after inspecting the environment.
   vq admin clear-update-marker <host>
   ```
   `recover-update` is the only path for a managed receipt. It restores the old
   runtime for an uncommitted interrupted target, or re-attests an exact target
   the receipt already records as committed; it never infers that an unverified
   target landed. For a pause-only receipt, `clear-update-marker` resumes and
   proves the exact recorded token scope before unlinking. An ordinary marker
   has no rollback receipt; inspect the environment and updater liveness before
   acknowledging it. When status
   reports more than one managed receipt, pass the exact printed
   `--marker-id ID` to `recover-update`.

   A direct host can be trapped when its installed vq predates a landed
   recovery-parser fix and that same managed receipt prevents updating vq.
   After authenticating the receipt and confirming that the current driver
   contains the required fix, use the explicit compatibility path with the
   diagnosed marker ID:

   ```sh
   vq admin recover-update <host> --marker-id <id> --with-driver-runtime --json
   ```

   This does not install code on the host. The driver uploads its exact running
   `vq` package to a unique directory under `~/vqscratch`, verifies the archive
   digest remotely, and invokes the configured absolute `remote_vq` executable
   once with that package first on its import path. The staged process verifies
   its own archive before reading recovery state. A known result removes the
   stage; an unknown SSH outcome retains it and names the exact path so the
   marker can be reconciled before any retry. The mutating recovery command is
   never retried automatically. This mode is intentionally unavailable for
   scheduler-driver hosts and orphan-receipt quarantine.

   Recovery alone cannot update a console whose old update code recreates the
   same failure (#677). Once recovery has cleared the receipt, use the current
   verified driver for one pinned update:

   ```sh
   vq admin update vibeqc-queue <host> --with-driver-runtime \
     --tag <accepted-tag> --expected-sha <full-40-hex-sha> --json
   ```

   The temporary package runs the ordinary managed update transaction, including
   its service, checkout, installation and rollback checks. It is removed after
   a known result and retained with its exact path after an unknown outcome or
   interruption. Never retry an ambiguous update before reconciling its marker.
   The driver rechecks configuration after staging and does not retry the update
   command. This mode requires one nonlocal direct host and one exact SHA; it
   rejects batch, force, restart suppression and scheduler update modes.
   A `vq-only` target accepts only `vibeqc-queue`. Release acceptance and fleet
   verification remain separate gates; staged execution does not assert rollout
   convergence.

   A pre-marker daemon install also needs explicit adoption (#577). After
   inspecting its installed capabilities, declare the matching profile and
   PEP 610 mode on a single managed update, for example a web-enabled editable
   install:

   ```sh
   vq admin update vibeqc-queue <host> --with-driver-runtime \
     --expected-sha <full-40-hex-sha> --json \
     --update-script-arg=--adopt-legacy --update-script-arg=--extras \
     --update-script-arg=web --update-script-arg=--editable
   ```

   This requires both ownership and install-profile markers to be absent.
   Exactly one regular PEP 610 record must name the configured checkout and
   match the declared editable/copied mode. A normal `lib64 -> lib` alias is
   accepted only when every metadata path names the same file. The proof is
   repeated before stopping the service. The old venv stays unmodified in
   the rollback transaction; the canonical installer creates and records the
   replacement at the original path. Adoption never invents metadata for the
   old environment and is unavailable for batch updates. Existing drains and
   independent fleet acceptance still apply.

3. **Name the correct host.** For a venv env, this is the machine you updated.
   For a **scheduler host** (pbs-cluster, slurm-cluster) the update runs on that host's
   `scheduler_driver`, and the marker lives *there* with
   `envs=['scheduler:<host>']` or `envs=['scheduler-runtime:<host>:<program>']`
   -- `host=<cluster>` in the marker names what was being updated, not where the
   file is. Name the cluster and the command follows the driver for you, the
   same way `vq admin status HOST` does. Before v0.12.1 it did not: it SSHed to
   the cluster login node, found no marker, printed "no marker present" and
   exited 0 while the real marker kept blocking every update. The message now
   always names the host it looked at.

   The hostless form resolves to `default_host`, which is usually some other
   machine. If a marker is present locally and no HOST was given, the command
   refuses and tells you both candidates rather than clearing the wrong one.

   Clear refuses a marker with a live writer without `--force-live`. That flag
   only overrides the writer-liveness admission: it cannot raw-unlink a
   managed receipt or bypass a pause-only receipt's exact resume proof. A
   durable receipt keeps only its parsed dispatch scope held until its proof
   completes. Stale ordinary markers may be auto-reaped as described above.
4. **Start another update only when safe.** With the cause fixed, the prior
   command proven terminal, the exact live target checked, and any stale marker
   safely cleared, `vq admin update <env>` is the right next step. A lost SSH
   response must first follow the reconciliation flow above.

### Ordinary-marker force shortcut

If every present marker is ordinary, you have fixed the cause, and you have
independently proved the prior updater is gone, `vq admin update <env> --force`
acknowledges those ordinary markers and starts a new attempt. It refuses while
any durable managed-daemon or paused-job receipt is present; use the diagnosed
recovery path instead.

For ordinary markers the daemon's poll-loop reaper is more careful than
`--force`: it deletes one only when the PID is provably gone/recycled or the
marker is older than 24 hours. `--force` has no ordinary-marker PID guard, so
it remains an exceptional operator acknowledgement, not routine recovery.

**What `--force` does and does not bypass.** It overrides the *marker* guard
only. It does **not** bypass the active-scheduler-job guard or the provenance
verification -- those are unconditional on both scheduler update paths. What it
uniquely skips for an ordinary marker is the live-PID protection that
`clear-update-marker` enforces via `--force-live`. It cannot discard durable
rollback or pause state. If you are reaching for it because a scheduler host
is busy, you want `--drain-wait` instead (see "Updating a busy scheduler host"
below).

Also note: on a daemonless scheduler host the marker lives on the driver. If
that driver runs no vq daemon (a laptop, say), nothing auto-reaps it -- manual
clearing is the only path there.

### Admin update pause/resume scoping (v0.11.1)

`vq admin update` records pause intent, SIGSTOPs the affected jobs, pulls and
builds, then normally SIGCONTs them during cleanup. The resume is **scoped to
exactly the jobs that THIS invocation paused** -- it is not a blanket "resume
everything". A hard interruption retains that token scope in the marker for
explicit, proven recovery.

Mechanically, each update mints a fresh per-invocation tag
(`admin-update-<12 hex>`, `admin._new_pause_token`), pauses with
`pause_all(paused_by=<token>)`, and resumes with
`resume_all(paused_by_filter=<token>)`. A SUSPENDED job whose
`paused_by` doesn't match the running update's token is left paused.
That covers three cases the old blanket `resume_all` got wrong:

* **A prior *interrupted* update's stragglers.** If an earlier update
  was SIGKILLed / OOM-killed / lost its host to a reboot *after* it
  paused but *before* its `finally` resume ran, those jobs sit
  SUSPENDED under the earlier run's token. A later update no longer
  wakes them -- its filter doesn't match. (Before v0.11.1 it woke every
  SUSPENDED job at once: the **2026-06-22 compute-b incident**, where a
  `vq admin update vibeqc-queue` resumed 22 stuck-paused jobs and
  spiked the box to load 88 on btrfs+LUKS I/O.)
* **Operator pauses.** A job you paused by hand (`vq pause JOBID`,
  `paused_by=None`) is not collateral-resumed by an unrelated env
  rebuild -- an untagged pause can't be claimed by a tagged resume.
* **Build-script cooperative pauses.** The `scripts/_vq_cooperation.sh`
  pause that fires *inside* the update window resumes its own jobs by
  its own tag; it never adopts the admin token, and the admin resume
  never adopts its jobs.

The token is per-invocation **on purpose** -- a shared constant tag
(e.g. a fixed `update-script`) would let a *later* run's filtered
resume re-match and re-wake an *earlier* interrupted run's stragglers,
reintroducing the compute-b failure mode. If you find a straggler still
SUSPENDED after an interrupted update (`vq queue --state suspended`
shows a job with an old `admin-update-…` or `update-script`
`paused_by`), resume it explicitly: `vq resume JOBID`, or
`vq resume --all` to drain every leftover pause regardless of tag.

The surgical (`provides_branches`) path was already scoped this way via
its explicit paused-jobid list; v0.11.1 brings the queue-wide path to
the same guarantee.

Current multi-user pause/resume authorizes every control candidate under its
spec lock before reporting its state or `paused_by`, and before sending a local
signal or a scheduler hold/release. Bulk scans first omit readable terminal
history with no pause token or pending pause intent, after a read-side ownership
check. Those rows take no control lock and do not contribute to the summary's
skipped count. Their files and historical evidence stay in place. Unreadable or
unauthorized hints still go through the ordinary locked path; a foreign job is
an isolated error without its state being revealed. Failure to establish a
valid effective multi-user policy stops the command. Exact admin admission and
token-reconciliation proofs still scan every row under its lock, including
terminal rows; a bulk summary is never proof of recovery.
Scheduler pause/resume keeps the final mutation spec lock across the exact
handle check, `qhold`/`qrls`, owner recheck, and spec commit. If an exact
inverse fails, vq attempts to persist
`hold_outcome_unknown` or `release_outcome_unknown`. Use the opposite ordinary
verb only when the error confirms that marker as durable; otherwise the
compound outcome remains fail-closed. Do not edit the marker or assume the
remote scheduler state from the ordinary vq lifecycle state.

---

## Updating a busy scheduler host -- `--drain-wait` (v0.12.1+)

Both scheduler update paths refuse to run while the target has active
submitted jobs:

```
43 active scheduler job(s); wait for them to finish before rebuilding
the cluster environment
```

**That guard is correct and must not be removed.** It is what stops a rebuild
landing under live jobs, and it has already protected running paper jobs on
pbs-cluster. The problem was that on a shared production node that is rarely idle
there was no supported way to ever *satisfy* it: the only route through was a
hand-built drain window, and if you didn't build one, `--force` -- which is the
wrong tool, because it does not bypass this guard at all (it only overrides the
marker) while it *does* bypass the live-updater protection. pbs-cluster sat digest-red
for exactly this reason.

`--drain-wait DUR` is the supported maintenance window:

```sh
vq admin update pbs-cluster --drain-wait 4h
vq admin update vibeqc-release slurm-cluster --expected-sha <40-hex> --drain-wait 90m
```

**vq's own job state is not a statement about the cluster.** The daemon stamps
`state=RUNNING` before the `qsub` even runs and keeps the scheduler's real
phase in a separate `scheduler_state` field, so "vq says RUNNING while `qstat`
says QUEUED" is by design -- and "vq says RUNNING while `qstat` has never heard
of it" is what a daemon death or a failed reattach leaves behind. Six such
entries blocked pbs-cluster maintenance for days and read to a human as multi-day
production runs.

Both the refusal and the wait therefore **reconcile against the scheduler**
before counting. The rules, which cannot weaken the guard:

* Only a job the scheduler positively reports **finished** is discounted. A
  merely *queued* job keeps blocking -- it can start at any moment, including
  mid-rebuild.
* A probe failure (SSH down, `qstat` unparseable) leaves **everything**
  blocking, and the refusal says so. A broken probe must never be the thing
  that green-lights a rebuild under live work.
* A spec with no recorded scheduler job id cannot be observed, so it keeps
  blocking -- an untracked batch job may still be running -- but it is named, and
  a `--drain-wait` fails fast rather than waiting out a job it can never see
  finish. Confirm on the cluster, then `vq kill HOST <jobid>`.

What `--drain-wait` does, in order:

1. **Holds a scheduler drain lane** for the target (the same lane
   `vq drain --scheduler-host HOST` sets), so the daemon stops dispatching
   *new* work there. Without this the wait would race an actively-fed queue
   and might never converge.
2. **Waits** for the jobs already on the cluster to reach terminal, re-counting
   every 30 s and refreshing the admin-update marker heartbeat each time -- so
   `vq admin status` shows `drain-wait on pbs-cluster: N active scheduler job(s)
   after Ms of Ds` instead of going silent. Without the heartbeat the marker
   diagnostics would start recommending `--force` against a wait that is
   working exactly as intended.
3. **Proceeds** with the normal update once the target is genuinely quiet, or
   **refuses** with the remaining job list if the budget runs out.
4. **Releases the lane** it added -- and only that one. A lane you set yourself
   beforehand is left exactly as you left it, including its `reason`.

Notes:

* The default is unchanged. Without `--drain-wait`, the refusal is immediate,
  exactly as before; the message now points at this flag.
* A **held** job (`vq pause`, i.e. `qhold`) stays non-terminal and keeps its
  scheduler job id, so it counts as active forever. If every remaining job is
  SUSPENDED the wait fails fast and says so, instead of burning the whole
  deadline and then reporting a bare timeout.
* The lane drain holds *dispatch*, not *submission*: new rows still land as
  PENDING and the backlog visibly grows during the window. That is expected.
  Add `vq drain HOST --update-mode deny` beforehand if you want submissions
  rejected too.
* The wait budget is added to the outer SSH timeout on the delegated path, so
  a legitimate wait cannot look like a hung connection.
* `--drain-wait` is scheduler-hosts-only; a venv env update pauses the local
  queue instead and rejects the flag.

---

## Daemon picked up a config change -- `vq daemon reload` (v0.12.1+)

The daemon caches config-derived state, most consequentially one
`SchedulerDispatcher` per scheduler host which snapshots that host's
`scheduler_program_hooks` when first built. Before v0.12.1 that cache lived for
the daemon's whole life and there was no reload path at all -- `vq daemon start`
is removed and nothing handled SIGHUP -- so a config fix on disk was ignored
until a full restart. On 2026-07-22 a `command_wrapper` fix landed at ~17:50
and the daemon kept rendering the old job scripts for every dispatch from 17:57
to 18:26, working through the released backlog with config nobody was running
anymore.

Three mechanisms now, in increasing order of explicitness:

1. **Automatic.** Each cached dispatcher records the config file's fingerprint
   (mtime+size). If the file has moved since that dispatcher was built, the
   next dispatch rebuilds it. Edit `config.toml` and the change takes effect
   without any operator action. In-flight jobs keep the dispatcher they were
   submitted with.
2. **`kill -HUP <daemon-pid>`** -- requests a full reload on the next loop
   iteration: clears the whole dispatcher cache and re-reads config.
3. **`vq daemon reload`** -- the same thing over the daemon's RPC socket, with a
   confirmation. Audited (the method is `set_config_reload`).

If the config on disk has changed and no longer parses, the daemon **holds all
dispatch** and logs the parse error once, rather than continuing on state the
operator has already replaced. It resumes by itself once the file is valid
again. A reload that hits an invalid config is refused and keeps the previously
loaded config -- adopting a broken config would be worse than staleness.

`multi_user` is deliberately *not* reloadable in place: it selects the state
layout, queue lock, RPC socket, pidfile, and log file. A change there is logged
and requires a restart; every other change from that reload still applies.

---

## Daemon log: "scope collision detected" (v0.5.50+)

Symptom in `journalctl --user -u vq-daemon`:

```
WARNING: scope collision: vq-job-abc12345defg.scope already exists
   stopping stale scope before dispatch
```

This means a previous daemon run created a transient cgroup scope
for a job, the daemon process died, and the new daemon found the
old scope still on the cgroup tree. Pre-v0.5.50 the dispatch would
have failed with "Unit already exists"; the v0.5.50 pre-flight
check stops the stale scope and re-creates it cleanly.

**No operator action needed** -- this is informational. If you see
the warning repeatedly for the same scope, that's a bug; capture
the journal context and file it (the daemon should clean up after
itself).

---

## Daemon log: WARNING "daemon running X, on-disk source says Y" (v0.6.2+)

The daemon checks its own version against `src/vq/__init__.py` on
disk once per minute (`Daemon._maybe_check_version_drift`). If
they don't match it logs:

```
WARNING: daemon running 0.6.5, on-disk source says 0.6.7
   restart vq-daemon to pick up the new code:
   systemctl --user restart vq-daemon
```

### Cause

Someone ran `git pull && pip install -e .` directly (not via `vq
admin update vibeqc-queue`), bypassing the v0.5.42+ auto-restart.
The on-disk code is new; the running daemon is on stale bytecode.

### Fix

The recipe is in the warning itself:

```sh
systemctl --user restart vq-daemon
```

After restart the warning clears (with one INFO line: "drift
cleared: now running 0.6.7"). The standard `vq admin update
vibeqc-queue` path avoids this warning entirely -- the auto-restart
fires at the end of the update.

### Why a warning, not refuse-to-dispatch

Operator bypass is rare and usually intentional (mid-debug, mid-
release-rehearsal). Blocking dispatch on version drift would
escalate a misconfiguration into a queue outage; a loud warning
strikes the right balance. The probe is rate-limited to once per
60s and stamped per-version, so the WARNING fires once per
distinct drift state, not every minute.

---

## ABORTED_BY_QUEUE with reason `pid_recycled` or `cgroup_scope_mismatch` (v0.5.50+)

When you `vq status <jobid>` shows `state: aborted_by_queue` and
the `terminal_reason:` field reads `pid_recycled` or
`cgroup_scope_mismatch`, that's a new flavor of abort introduced
by the audit § 1 / § 2 hardening sweep. Both are protective --
they're the queue refusing to trust ambiguous state rather than
risking a wrong-job kill or a misattributed exit code.

| `terminal_reason` | What happened | What to do |
|---|---|---|
| `pid_recycled` | Spec recorded `pid=X` + `pid_start_time=T` at dispatch. On the next reattach attempt, `/proc/X/stat` field 22 had a different start time -- the OS recycled PID X to an unrelated process. The daemon won't claim the new process is our job. | Same as any ABORTED_BY_QUEUE: read stdout.log to see if useful output was produced; `vq resubmit` if needed. |
| `cgroup_scope_mismatch` | Spec recorded that the job runs in `vq-job-<id>.scope` with a specific MainPID. On reattach `systemctl --user show vq-job-<id>.scope -p MainPID` returned a PID that doesn't match the spec. Same protective stance as pid_recycled, but at the cgroup layer. | Same recovery as above. Check `journalctl --user -u vq-daemon` for the cross-check log line -- sometimes it points at a scope-leak bug worth filing. |

Both reasons land alongside the legacy reasons (`daemon_restart`,
`pgid_missing`, …) in the terminal-states cheat sheet in
[`handover.md`](handover.md). Pre-v0.5.50 these cases would have
been silent -- the daemon would have re-attached to whatever PID
was at `spec.pid` (which is wrong if it got recycled) and either
killed the wrong process or misattributed the exit code.

---

## Refreshing the root-owned `/opt/vq` install (v0.24.x+)

Applies to multi-user hosts (`compute-d`, `compute-a`). They run **two** vq installs:
the user daemon from a home checkout, and a root-owned multi-user daemon from
`/opt/vq/venv`. `vq admin update` reaches only the first. `/opt/vq` is a
non-editable install on purpose -- it is the privilege boundary -- so refreshing
it is a maintainer action on a privileged path.

```sh
ssh -t <host> 'sudo /opt/vq/bin/vq-multi-user-refresh \
  --checkout ~/gitlab/vibeqc-queue --expected-sha <40-hex>'
```

Installed root-owned by `contrib/deploy-multi-user.sh`. Authenticated `sudo` is
mandatory. The former passwordless `vq-admins` fragment is retired and the
deploy script removes it: letting a group member choose source whose build
backend root executes is a root grant regardless of the helper's absolute
path. Privileged activation follows an independently accepted release report
and always names its full 40-hex SHA.

If the installed helper predates the durable transaction, install the accepted
root-owned refresh surface without touching the runtime, then perform the
required helper dry-run:

```sh
bash vibe-queue/contrib/deploy-multi-user.sh \
  --expected-sha <accepted-40-hex-sha> --prepare-only
sudo /opt/vq/bin/vq-multi-user-refresh --checkout <checkout> \
  --expected-sha <accepted-40-hex-sha> --dry-run
```

The first-class vq-only user-install rollout lane is unrelated to this
privileged path. It updates a target-side user
`[programs.vibeqc-queue] kind = "venv"` registration and never substitutes for
or invokes `/opt/vq/bin/vq-multi-user-refresh`. `--dry-run` validates the exact
source, no-downgrade ancestry, shared lifecycle locks, and unit contract
without building or mutating. Dirty and downgrade bypasses do not exist.

**Why a helper and not three commands.** The hand sequence was `pip install`,
`vq source-sha --write-marker <sha>`, `systemctl restart`, and it had a
silent-corruption ordering trap: run it before the host's checkout reaches the
pin and `pip install` builds the **old** code while `--write-marker` stamps the
**new** commit onto it. Verification trusts the marker, so `vq admin update`
then reports `== OK ==` and `source SHA ... verified` against a lie -- strictly
harder to notice than the honest failure it replaced. Nothing on the host could
catch it, because the host was being asked to believe a claim about a commit it
was never shown.

The helper removes that by construction. `--expected-sha` is mandatory and is
cross-checked against both checkout HEAD and the explicit commit archived by
Git. A checkout behind the pin aborts before anything is installed:

```
vq-multi-user-refresh: checkout HEAD is <old>, not accepted pin <new>
```

It acquires the shared checkout and target lifecycle locks, creates a sealed
root-owned archive as the sudo invoker with optional Git locks, hooks, ambient
configuration, and replacement objects disabled, and prebuilds a wheel without
touching the live runtime. It fsyncs a root-only recovery receipt before
stopping the exact unit. After quiescence it moves the old venv to a
same-filesystem backup and creates the new venv directly at `/opt/vq/venv`.
The installed package digest must equal the accepted commit's package digest.
The helper then reads back the bound marker and tree and requires the restarted
daemon's verbose RPC identity to report both, run as euid 0 from the exact
installed executable/argv, and carry the same PID as systemd `MainPID`. Before
commit or backup deletion it rejects writable payloads and unsafe symlinks and
recursively fsyncs the verified runtime plus `/opt/vq`. Any pre-commit failure
restores and re-proves the old exact runtime; the next mutating invocation
recovers an interrupted receipt before starting new work. `--dry-run` never
recovers: it reports an existing receipt and exits without touching systemd or
either venv.

Runtime dependencies are not floating resolver output. The accepted commit's
`contrib/vq-multi-user-runtime-requirements.txt` pins exact wheel hashes for the
supported glibc x86_64 CPython 3.12-3.14 path. Download runs unprivileged;
root independently checks and seals those wheels, and builds the vq wheel with
the trusted standard library instead of executing an unpinned build backend.

---

## Are the required rollout lanes converged? (v0.24.x+)

```sh
vq admin rollout-latest --verify-only          # required lanes: converged | degraded
vq admin rollout-latest --verify-only --json   # machine-checkable
```

Run it **on the scheduler driver**: it discovers the accepted report from the
driver's own runtime checkout and derives the topology relative to it, so it
refuses to run anywhere else. Read-only means more than no update argv: before
fleet discovery it inspects all durable rollout operations and journals, but
never authorizes, aborts, follows, harvests, mutates, consumes a known-failure
fence, or releases an owned hold. Reconciliation-required state fails closed
and directs an explicit rollout invocation. Exit 0 means the modeled managed
lanes, required vq-only user lanes, and read-only provenance records converged
or were explicitly not applicable; 2 means those records are degraded, and 1
means error. It does not assert whole-fleet or whole-toolset convergence. The
additive `coverage` block keeps `managed_lanes`, `vq_user_lanes`, and
`provenance_lanes` separate, names privileged root apply, operator-managed
vibe-basisopt, intentionally inapplicable chemistry/view components on
vq-only hosts, and explicitly excluded hosts. A local-scheduler vq-only host
gets exactly one report-pinned user-vq action after driver re-entry. Its own
`[programs.vibeqc-queue] kind = "venv"` registration is required: missing or
invalid registration blocks, while failed, omitted, malformed, or
contradictory discovery defers. The payload's `degraded_hosts` maps each host
to every reason it is not standing at the newest accepted release report:
pending lanes with the ancestry relation applied, failed doctor checks, and
topology errors, folded into one answer.

As of 2026-08-10 that vq-only user-lane behavior is a source contract only; no
accepted report or live-fleet rollout has yet exercised it. Do not infer live
deployment from this page, and do not report the observed historical 28/28
managed count as whole-host convergence.

This replaces reading `vq admin status --all --json`, `vq programs --all
--json` and `vq doctor --all` and then making a judgement call, plus a second
zero-action `--dry-run` to confirm the planner agreed. It is also stricter than
`--dry-run`, which exits 0 with pending `update` actions.

Compose with `--only <host>` to ask about one host -- useful straight after a
scoped recovery.

---

## The fleet console drifts silently (v0.25.0+)

```sh
vq web status      # is a console service installed here, and has it drifted?
vq web install     # (re)write the unit, pointing it at the vq running now
vq web config      # resolved [web] settings, and the layer each came from
```

The console (`vq web run --fleet`, normally a service on the coordinator) is
**not** deployed by `vq admin rollout-latest`, and nothing else restarts it: a
running console keeps serving the vq it imported at startup. Upgrading vq on
that host therefore leaves the dashboard rendering older code on pages that
look completely normal. Re-running `vq web install` is the fix; it is
idempotent. `vq web status` names the drift, and the console additionally
compares its own version against the local daemon on every page.

Setup, `[web]` configuration, accounts and troubleshooting:
[`fleet_console.md`](fleet_console.md).

---

## Verifying a host's provisioning preconditions (v0.24.x+)

```sh
vq admin provision <host>           # verify + print the remediation plan
vq admin provision <host> --check   # verify only, machine-oriented
vq admin provision --all --json
```

Read-only in both forms. It checks what `vq doctor` does not, and every check
is there because that condition once cost hours while doctor reported the host
green:

* `remote_vq` resolves to a **wrapper** exporting `VQ_CONFIG_DIR` and
  `VQ_STATE_DIR`, not straight to `/opt/vq/venv/bin/vq`. Without the wrapper
  the CLI reads the per-user store while canonical writes go to the system one,
  so updates report `success: True` with no work errors and `LAST OK` never
  advances.
* `admin_token_file` is configured, present, and mode 0600 (the loader refuses
  any group or other bits).
* `/var/lib/vq` is `root:<admin_group>` 2775. A `root:root 755` state root
  leaves the host fully provisioned, daemon running, and unable to perform any
  admin operation, because the update marker lives directly under it.
* The root-owned `/opt/vq` install and its refresh helper are present and
  root-owned.
* `systemd-run` is available for per-job privilege drop.
* The multi-user unit is active, and programs are registered.

Repairing any of these needs root on the target, so failures are reported with
the exact fixing command rather than executed. `--check` suppresses the
remediation prose and is the form to use for a rehearsal -- confirming a
driver-migration candidate's preconditions, for instance -- rather than
walking the list by hand.

---

## Post-mortem reference: 2026-05-16 (two hard resets, compute-d)

* **Incident #1, ~06:00 UTC.** Interactive
  `bash scripts/update-dev.sh --dev` on compute-d fired 32 cc1plus
  workers on libint headers; peak ≈ 290 GB on a 125 GB box;
  global OOM; kernel killed user-systemd (and left it as zombie
  PID 100128); SSH froze; hard reset.
* **vq v0.5.40** shipped same morning: cap formula `min(nproc,
  max(2, mem_mb // 10000))` injected into `_run_update_script`.
  This stopped the *crash* but left 12 workers on compute-d -- still
  enough resident heap to thrash the page cache.
* **Incident #2, ~12:40 UTC.** v0.5.40 was being verified by
  re-running the build on compute-d (mistake -- same host, see
  post-mortem). Box never crashed but went unresponsive; an
  attempt to `systemctl --user restart vq-daemon` hit the zombie
  manager and returned `Connection refused`; the
  `sudo systemctl restart user@$UID.service` recovery hung on
  TimeoutStopSec; the cumulative pressure finished the box; hard
  reset #2.
* **vq v0.5.41** + **scripts/update.sh** changes (this commit)
  closed both paths: tighter formula (15 GB / worker, hard caps 6
  and 8) + idle CPU/IO priority everywhere a build is invoked.

Two lessons that aren't already encoded in code:

1. Never verify a "this host wedges during builds" fix by
   triggering the same build on that host. Use compute-a or a fresh
   worktree.
2. `systemctl --user restart` waits politely for SIGTERM ack. On
   a zombie manager that wait never ends. Go straight to
   `systemctl kill --signal=SIGKILL` if the manager is sick.
