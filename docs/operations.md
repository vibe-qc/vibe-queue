# Operations runbook

**Audience: operators.** Part of [Running a host](operator/index.md).

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

## Where a host's disk went, and moving scratch (#63, v0.26.10+)

**First, look in the right place.** vq keeps two roots:

```sh
vq daemon status        # prints queue, scratch, jobs and workdirs
du -sh "$(vq daemon status | awk '/scratch:/{print $2}')"
```

The **state root** holds the queue, its locks, archives and the daemon's own
files, and is never pruned. The **scratch root** holds `jobs/` (each job's
working directory and results) and `workdirs/` (`$VQ_WORKDIR`), and is swept by
age. They are separate by default from v0.26.10: `$VQ_SCRATCH_DIR`, else the
`scratch_root` config key, else `<state root>-scratch`.

They were one root before that, and the classification was wrong where it
mattered: on two daemon hosts **over 99 % of the state root's footprint was job
workdirs** — 132 GB of 138 on one, 81 GB of 84 on the other, most of it in a
handful of directories — under a root the layout documents as durable. On a
host that has not been migrated, expect the same shape and look under
`<state root>/workdirs` before anything else.

**`vq cleanup` sees workdirs now.** The listing has a WORKDIR column,
`reclaimable` includes those bytes with the workdir total called out, and
`--delete` removes the workdir as well as the workspace and the archive. It
previously reported about 1 GB across ~1,200 terminal jobs on the host whose
workdirs held 81 GB. `--archive` still tars the workspace only, and says so.

**Turning the sweep on is usually the whole fix:**

```sh
vq cleanup HOST --auto-enable --workdir-max-age 14d --delete-after 90d
vq cleanup HOST --auto-status
```

It never touches a job that is not terminal, ages terminal jobs by
`finished_at` rather than by directory mtime, and visits every workdir root vq
manages.

**Moving the root.** Set it once, for the daemon and every CLI caller. The
config key is the safer of the two, because both read the same file:

```toml
min_vq_version = "0.26.10"
scratch_root = "/data/vq-scratch"
```

`min_vq_version` matters here: a vq that predates the key ignores unrecognized
top-level keys by design, so without the floor an older build on the same host
would keep resolving scratch under the state root while a newer one uses the
key — a split store. Restart the daemon and check `vq daemon status`.

Nothing moves when you change it. Every existing job keeps the `cwd` and
`workdir` it recorded, only new dispatches use the new root, and cleanup,
accounting and the sweep follow the records: a job's workspace and workdir are
measured and removed at the path that job recorded, whether that is the current
root, the pre-#63 root under the state root, or a root you have since
configured away from. The age sweep walks the configured roots plus every root
this host's own job records still name, so a second or third move does not
hide the bytes from it.

**What a changed root does leave behind** is any directory under a root you
have configured away from that no job record names *at that path*. The sweep
removes a directory there only when a record names exactly it, because the
root may since have been repurposed and a name alone proves nothing. That
leaves an *orphan* — one whose job record is already gone — and the directory
of a job's earlier attempt when a retry recorded a new one under the current
root. The sweep logs each at debug level and leaves it. `rm -rf` them yourself
once you have checked `vq cleanup` reports nothing for that root. Draining the
old root before moving again avoids the situation entirely.

That records-not-recomputation posture is deliberate, and it is why a root
moved *underneath* vq by other tooling needs `vq admin repair-state-paths`
(#69, below) rather than a silent fallback. The full migration runbook is
[the fleet layout](fleet_layout.md), phase 5.

**A workdir vq did not create is never removed.** The recorded path must still
be the per-job directory *named for that job* — `<root>/<jobid>`, where `cwd`
and `workdir` are both written by vq itself and by nothing else; the root is
opened `O_NOFOLLOW`; a symlinked entry, a non-directory, or an entry owned by
another uid is refused and reported. In multi-user mode the queued spec is
user-writable, so the record is evidence only and the deletion target is
derived from the trusted owner and the job id — there the managed per-uid roots
remain the authority. A refusal at terminal-cleanup time is recorded on the
spec as `workdir_cleanup_note`, so `vq status <jobid>` explains it rather than
only the daemon log; it is **not** a `failure:`, because a job that completed
did not fail by keeping its scratch.

**Scheduler hosts.** `hosts.<name>.scratch_root` is the same concept for the
cluster vq submits to, and it is now safe to change under jobs in flight: each
job records the workspace it was staged into before submission and every later
step uses that path. Three things to expect after an upgrade:

* the driver **refuses to adopt a changed `scratch_root`** for a host while a
  job on it that may already be staged still predates the binding. It logs the
  job ids and keeps using the root it last adopted, which it remembers across a
  restart and a config reload — those are the two moments the phase 5 runbook
  names, and the guard used to be silent on both. Such a job binds when the
  driver *reattaches* to it, which is a driver start and not a poll tick, so
  the refusal clears on the restart after the change rather than minutes later.
  A host still held after that restart has a job that did not reattach, and
  that job is the real problem.

  A job that is merely **queued** does not hold the host. Nothing is staged
  before dispatch, so a PENDING job on its first attempt has no workspace to
  strand; counting it meant a host with a steady queue never adopted the
  change at all.
* **the first start after the upgrade cannot check the refusal**, because the
  record it compares against is written by the driver itself and an older vq
  wrote none. Set the key in the same window as the upgrade and that start has
  nothing to compare to. It serves the configured root — there is no other
  candidate — but it does **not** bind that host's pre-upgrade jobs to it and
  does not record the root as adopted, so restoring the previous value in
  `config.toml` and restarting the driver still repairs them. The restart is
  what re-derives a running job's workspace; a reload does not. Once those
  jobs drain the driver settles the doubt at its next lookup of the host.
  Changing the key in a separate window from the upgrade avoids it entirely.
* remote cleanup accepts a workspace staged under a *former* `scratch_root`, so
  changing the key no longer leaves undeletable directories on the cluster.

`vq doctor HOST` reports all of this as `scheduler_scratch_root`, from the
driver's own record, the local config and the queue, with no SSH: `FAIL` while
the host is held at a previous root or unconfirmed, and in either case only
while a job that may already be staged still carries no recorded workspace,
with the remedy in the message. The record is written only when the driver
looks the host up, so a key changed on an idle host, or an unconfirmed host
whose jobs have drained, passes before the record catches up. Run it where the driver keeps its state; elsewhere it reports "no
record" rather than a verdict.

---

## Removing old runtime generations (#74)

A program with `runtime_slot_root` gains one `releases/<sha>` generation per
update, and no update ever deletes one. Reclaim them with a reviewed plan:

```sh
vq admin runtime-retention ENV --keep 2
vq admin runtime-retention ENV --keep 2 --apply-plan PLAN_ID
```

The first command is read-only JSON. It lists every generation with its size,
whether it is kept and why, the candidates with `reclaimable_bytes`, any
blockers, and a `plan_id`. The following are always kept:

- `current` and, by default, `previous`;
- every generation a started local job references, through its dispatch
  selection or command line;
- on Linux, every generation a live process references (see below);
- any generation named by the daemon's service definition or used by the
  running daemon;
- every `--retain SHA`, for example one named by a fixed-SHA launcher;
- the `--keep` newest others;
- incomplete generations, which are left to the build's own recovery.

A started local job whose generation cannot be proven (for example one that
went through a stable launcher before a later flip) keeps **every**
generation until it ends, unless its processes prove it. Queued and scheduler
jobs without explicit generation references do not hold generations of this
store. A queued local command naming `previous` protects its target too.

**Process evidence (Linux).** The plan reads `/proc`. For each of your
processes it reads the executable, working directory, command line,
environment (including the `VQ_RUNTIME_SLOT_SHA` the stable launcher exports),
memory maps and open files. For other users' processes it reads only the
command line. Any generation a live process names is kept with the reason
`live process`, whether or not it belongs to a job. A started job with no
recorded generation is proven by its process group, the `pgid` captured at
dispatch. This works only when that group is alive and every member was read
completely. The job then holds exactly what those processes name, so a busy
host can reclaim everything else.
A job whose group is gone, or has a member that could not be read (for
example another user's process, or a non-dumpable one), still keeps every
generation. The census reports
`process_proven_jobs`, `process_scan` (false without `/proc`, where nothing
changes), and `process_references`, a map from SHA to pids. The pids are
informational and not part of the plan id, so a restarted helper does not
invalidate a reviewed plan, while a new reference does. A service authority that cannot
be read, or an unresolved admin update marker, is a blocker.

`--apply-plan` repeats the plan under the admin-update ownership and the
program's lifecycle locks, refuses when the plan id no longer matches, removes
exactly the candidates, and writes a receipt to
`<state root>/runtime-retention/`. Nothing is removed by an update or an
activation. Scheduler-host packed publications are not covered by this command.

To keep only the current verified runtime and any referenced generations:

```sh
vq admin runtime-retention ENV --keep 0 --drop-previous
vq admin runtime-retention ENV --keep 0 --drop-previous --apply-plan PLAN_ID
```

`--drop-previous` makes an unused `previous` generation eligible for removal.
It still preserves live-job, process and service references, explicit `--retain`
holds, and incomplete generations. A service definition naming the `previous`
pointer preserves its target. The current generation must pass content
verification before this policy can be applied. Repeat the same options when
applying; a changed policy, pointer or reference invalidates the reviewed plan.

Before deleting the previous generation, apply clears and durably records the
removal of its rollback pointer. Rollback then has no target until a subsequent
activation establishes one. If deletion fails partway, the pointer stays cleared
so it cannot name a partial generation; the receipt reports `previous_cleared`
and any removal failures. A previous generation retained for another reason
keeps its rollback pointer. This option is for reviewed one-off runs only;
scheduled retention continues to protect `previous`.

To stop forgetting, opt a program in to retention on every auto-cleanup sweep:

```sh
vq admin runtime-retention ENV --schedule --keep 2
vq admin runtime-retention ENV --unschedule
```

Each sweep then plans and applies retention for every scheduled program with
the same revalidation. It skips a program with blockers, or while an update
owns the host, until the next sweep. The schedule lives in
`<state root>/runtime-retention/schedule.json`, separate from the auto-cleanup
policy file, so older vq versions keep reading that policy. It does nothing
unless auto-cleanup is enabled (`vq cleanup --auto-enable ...`).

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
`vq host up HOST`. `vq host list` shows every configured host and which of them
you have marked down, with the reason. The marks are your local view only and
never touch the remote daemon.

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

On a single-user daemon host, `vq doctor` also reports `state_root_agreement`:
whether the vq that `remote_vq` runs uses the same state root as the host's
running daemon. The verbose daemon ping lists this user's running
`vq daemon run` processes and the root each resolves from its environment
(`/proc` on Linux, `ps -E` elsewhere). The check **fails** when a daemon runs
and none uses the root vq resolves there. That happens on a host reached
through a bare `vq` while its service runs a wrapper that exports
`VQ_STATE_DIR`: status, overview and the console then read an empty second
store. Point `remote_vq` at the wrapper, or export the same `VQ_STATE_DIR`.
The check adds nothing on a multi-user host, when the host's vq predates it,
or when the process scan is not possible.

On the host it runs on, `vq doctor` also reports `declared_extras_routes`:
for every venv program that declares `extras`, which of its configured scripts
is vq's own, and therefore whether the declared floor has any route into that
venv. A declaration reaches pip only through the checkout's own
`scripts/install.sh` or `scripts/update.sh`, and config load cannot check
where a configured command points -- at load time there may be no checkout to
read. So a program whose installer is a site command loads clean and produces
the failure the key exists to prevent: a console venv built without uvicorn
whose unit fails on every start.

The check **fails** only where both scripts were resolved and neither is vq's
own, which is the state with no route at all. A program whose only vq script
is its installer is OK and told that nothing re-applies the declaration after
the provision. Where the checkout is absent or unreadable -- a host part-way
through provisioning -- it reports that it could not answer and stays OK.
Where no program declares `extras`, it says nothing at all.

It is a local check: the scripts are files in a checkout, so only the host
holding that checkout can answer, and `vq doctor REMOTE` leaves it out rather
than answering from the driver's own `[programs.X]` registry. Run `vq doctor`
on the host, or read `vq programs --json`, which carries `install_script` and
`extras` for a fleet-wide audit. `vq doctor HOST --json` carries the
per-program verdicts under the check's `programs` key.

For scheduler hosts, `vq doctor HOST` also reports `scheduler_program_hooks`.
Every `[hosts.HOST.scheduler_program_hooks.NAME]` key must have a matching
`[programs.NAME]` entry; missing program registrations are failures so typoed
per-program qsub hooks do not silently sit unused.

A driver that has no local installation can register a scheduler-only name:

```toml
[programs.orca]
kind = "scheduler"

[programs.vibe-view]
kind = "scheduler"
```

Keep the existing target-side `scheduler_program_hooks` and
`scheduler_runtime_deployments` entries. This registration supplies identity
only: it adds no executable, changes no wrapper, and claims no remote health.
`vq programs localhost --json` reports `SCHEDULER_ONLY`; `--require NAME`
does not accept that as a locally available program. Local submit and local
dispatch refuse these names. Scheduler submissions still resolve managed
runtime pins from the target registry or verified deployment record, and
still refuse missing or mismatched identities. Local admin install/update
requires a `venv` registration; manage scheduler runtimes through their
configured scheduler deployment interface.

Install a vq version supporting this kind on the driver before adopting the
configuration; older loaders reject it. Preserve the external configuration
backup and the original hook/wrapper bindings. Verify `vq doctor HOST --json`
from the actual driver for each scheduler authority, then submit and fetch a
bounded canary for each program through its existing command wrapper. Check
the recorded runtime/source identity and command. A passing registration
check alone is not per-program dispatch or fleet-migration acceptance.

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

Set `scheduler_max_cpus` alongside it for the widest job that lane can ever
run. It rides the same `scheduler_lane` metadata and is bound to the same
effective partition, and an unset value means unknown rather than unlimited.
A request above it is **warned about and still submitted**, unlike the wall
time: see the capacity section below for why the two differ.

Use `--admin-update` before a maintenance run such as `vq admin update pbs-cluster`.
For scheduler hosts this adds a check for `scheduler_update_command` and notes
whether `scheduler_install_command` is also configured, so a missing cluster
provisioning command is caught before the update attempt. It also runs the
scheduler host's configured `remote_vq --version`, which is the quickest way to
verify the cluster-side helper after `vq admin update HOST`.

### Scheduler-side vq refresh for daemonless clusters

HOME discovery, staged-source checksum reads, and helper readiness/provenance
reads allow one bounded fresh SSH retry after a recognizable connection failure
or observation timeout. Authentication and host-key refusals, unexplained exit
255 and ordinary command failures do not trigger that retry. A persistent
checksum transport failure remains unverified and does not restart the upload.
Stage creation, build launch and cleanup commands keep their existing
single-attempt behavior; this read recovery does not replay an uncertain write
or establish that an unreachable host has recovered.

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

**Source-upload staging is the one exception, and it is a different thing.**
A build host that cannot reach the source repository has the exact commit
uploaded to it, about 110 MB per deploy, under
`<scratch_root>/.vq-admin/runtime-source/<program>/<sha>-<uuid>/`. That
directory belongs to exactly one deploy: the driver re-archives the same SHA
from git on demand, and nothing reads a stage once the build has consumed the
archive. So a deploy that verifies removes its own, and a deploy that fails
keeps its own for forensics while older ones are trimmed to
`RUNTIME_SOURCE_STAGES_TO_KEEP`. The reasoning that retains a helper generation
-- another deployment may still be using it -- does not apply to these, and
nothing here prunes a helper generation or calls the prune verb on the host.
Before this was added, a SLURM host had accumulated 235 stages, 26 GB, one per
deploy since July, and the shared home went over quota.

`vq source-stage-prune --runtime-source <scratch_root>/.vq-admin/runtime-source`
is the operator-facing form, for what an older vq left behind or what failed
deploys kept. Without `--runtime-source` the verb still reads only
`STAGE_ROOT/generations/`, which is why it reported `removed=0` against these
stages and the disk stayed full. Both forms only ever remove a directory whose
name is exactly `<40 hex>-<32 hex>`; anything else under the root is reported
as skipped and left in place.

### Feeding the source mirror

Before feeding, vq inspects the configured mirror without changing it. If that
read times out or returns SSH exit 255, it retries once on a fresh connection
with multiplex reuse disabled for that attempt. The shared SSH master remains
untouched. This recovery applies to the read-only inspection; it does not
automatically replay a push or runtime build after an ambiguous failure.

A deployment profile with `feed_source_mirror` set pushes the accepted pin
into the target's push-fed bare mirror before preparation, replacing the
manual per-release feed step. **The feed never forces anything.** It is one
unforced, `--atomic` `git push` from the driver's configured source checkout,
carrying only:

* `refs/vq/pins/<sha>`, the immutable per-pin ref. Its name is the commit it
  names, so feeding it is a creation or an exact no-op. It can never require
  a force and can never come to mean a different tree, and it keeps the
  accepted pin addressable in the mirror however the branches later move.
* this lane's branch pair — `refs/heads/release` and
  `refs/remotes/origin/release` for `vibeqc-release`, or the `main` pair for a
  branch-tip program. Atomically, because a lane whose branch and
  remote-tracking ref disagree is exactly what the preparer fails closed on.
* the tags that name this pin in the driver's clone. Not the whole tag
  namespace: a deployment feeds one release identity.

Everything else in the mirror is left byte-identical. Unrelated branches,
unrelated tags, other pins' refs and notes are never named by a refspec, so
nothing can move them — including a tag the driver's clone happens to hold at
a different commit.

Anything that would need a force is refused **before** the push, so the
mirror is unchanged and the current runtime, the build and the canonical
LAST OK record are all untouched. An operator sees the mirror's own values:

```
mirror feed: cluster-login:<mirror> has refs/heads/release at 9c1d0a2f4b77,
the requested pin is 41b7e3c90ad2, and the mirror has already moved past this
pin, so feeding it would rewind that branch. vq never force-pushes, so the
feed is refused and the mirror is unchanged. Deploy the commit the mirror
already carries, or have the mirror's owner move that branch back
deliberately, then re-run. The pin itself stays addressable as
refs/vq/pins/41b7e3c90ad2... once a feed of it succeeds.
```

**A legitimate but older accepted pin is refused, not silently upgraded.**
That is the point: a forced feed used to resolve this case by rewinding the
mirror, and the alternative failure — quietly building whatever newer tree
the mirror has drifted to — would be worse. Deploy the commit the mirror
already carries if that is what you meant; otherwise the mirror's owner moves
the lane branch back deliberately, outside vq, and the deployment is re-run.
vq will not do it for them, and there is no flag that makes it.

The other refusals read the same way: a **diverged** lane branch, a mirror
commit the driver's clone does not contain (so the driver cannot tell what
feeding the pin would drop), and a **tag the mirror already holds at another
object** — a tag is an identity, so vq neither moves one nor forces one. A
requested `--tag` that does not name the requested commit in the driver's own
clone is refused before the mirror is read at all.

`vq admin update ... --json` reports the exact refspecs in
`mirror_feed_refspecs` and the per-pin ref in `mirror_pin_ref`; both also
appear in the update transcript under `mirror feed refspecs (unforced,
atomic)`. The atomic push needs git >= 2.4 on the mirror's host.

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

### What a queue-directory scan costs

The daemon re-walks its queue directory on every tick. Two of those passes —
the deferred scheduler-reattach retry and dispatch itself — go through one
shared scan, and from v0.26.10 that scan leaves out every **settled** row: one
in a terminal state with no scheduler transaction that could still be open
(#22). Such a row costs one `stat` per pass rather than an open, a JSON parse
and a model validation, so per-tick admission cost follows the live queue
rather than everything the host has ever run. The pass that first sees a row
still reads it in full — that is what proves it settled and records the on-disk
identity later passes compare — so the first tick after a daemon start, and the
startup walk that precedes it, still cost what they always did.

The third pass, the pause-intent sweep, globs the directory itself and so got
nothing from that scan. From v0.26.10 the daemon hands it the same verdict
(#82): a settled row whose bytes have not changed since a full read found it
settled **and** free of any durable pause intent costs the sweep one `stat`
too. So all three per-tick walks now follow the live queue. A settled row that
does carry an intent — what a `vq pause` killed before SIGSTOP leaves behind
when its job exits underneath it — is never skipped, because finishing it is
the sweep's purpose; arming an intent rewrites the row, so it also always
changes the identity and puts the row back on the very next sweep. Measured on
a synthetic 100 live + 16,000 retained queue
(`scripts/benchmark_pause_intent_sweep.py`), the sweep drops from 0.489 s to
0.122 s per tick, 4.0x, which takes it from the largest single term in a tick
to less than the admission scan beside it.

Consequences for an operator:

* **Neither `vq cleanup --archive` nor `--delete` is the lever for dispatch
  latency any more.** Archive to reclaim disk; delete to discard history. A
  driver that keeps every job it has ever run now pays for its live queue on
  each tick either way.
* **A row you touch comes straight back.** A `--restore`, a `vq fetch` read
  stamp or a hand edit rewrites the spec, which changes its identity, so the
  next pass reads it again before deciding anything.
* **Ambiguity is never aged out.** A terminal row whose scheduler submit is
  unresolved keeps its reservation and stays on the scan at any age, and the
  startup walk reads every row so a leaked process group is still reaped.
* **The pause-intent sweep is a separate walk**, and it is no longer
  proportional to the retained total either. It still globs the directory
  itself, but it is handed the settled-row verdict the daemon's own scan
  recorded, so a settled row costs it a `stat` as well. It stays *first* in
  the tick, ahead of polling and the watchdog, so the verdict it uses is the
  previous tick's — or, on the first tick of a daemon's life, the startup
  walk's. The cost of that ordering is bounded at one extra full read per row,
  once: a row that settles only after a tick's scan has passed it is read once
  more by the next sweep.
* **What still reads every row, deliberately.** The startup walk, and the exact
  proofs behind `vq admin update` — pause admission and token clearance — which
  scan every row under its lock, including terminal rows. The skip belongs to
  the repeating sweep alone and cannot be handed to a scan that has not asked
  for the unlocked hint: doing so is refused outright rather than honoured.

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

`vq source-identity` is read-only and needs no daemon. It prints one line of
JSON and exits 0 whenever it produced an answer, so a missing or stale marker is
a field, not an exit status. The keys are `version`; `source_tree_sha256`, a
digest of the installed vq package's files (bytecode and the two provenance
markers left out, so an editable and a regular install hash alike), or `null`
with the reason in `source_tree_sha256_error`; and `source_sha`, the
package-local `SOURCE-SHA` marker, or `null` with the reason in
`source_sha_error`. That reason is `no SOURCE-SHA marker installed`, or a
message that the marker's recorded tree no longer matches the package beside it.
`vq source-sha` and `vq source-tree-sha256` print one of the two values each.

Fleet venv refreshes are intentionally separate from scheduler-host refreshes.
`vq admin update ENV --all-hosts` and `vq admin auto-update ENV --all-hosts`
update hosts that run vq daemons. Daemonless scheduler targets stay visible in
the text or JSON fan-out result, but they are marked as skipped with a pointer to
`vq admin update HOST`. This keeps pbs-cluster from being probed as a normal remote vq
daemon while still reminding the operator that the cluster-side vq install has a
separate maintenance step.

`vq scheduler-probe HOST --json` emits the same scheduler-client probe in a
monitor-friendly shape.

The probe also reports **schedulable capacity** for a PBS or SLURM host, read
from `pbsnodes -a` or `sinfo -N`. It answers two different questions, and the
difference is what tells a job that is waiting from one that will wait forever:

- `max_cpus_now` — the largest request that could **start right now**. A wider
  request is admitted and then queues.
- `max_cpus_when_free` — the largest request the nodes the scheduler can still
  use could **ever** run. A wider request queues indefinitely, however long you
  wait, because no usable node is that wide even when idle.

Both are repeated per lane (`groups`), alongside a per-node census with each
node's width, free cores and state. A node that is merely busy stays `usable`;
only one the scheduler will not place work on at all (PBS `down`/`offline`,
SLURM `drain`/`fail`/`maint`) counts as unusable, and the report names those
nodes. If the census cannot be read, `capacity.error` is set and every figure
is `null`: an unreadable census never reads as "nothing is free".

A lane is a SLURM partition, or a PBS node property — and those are not the
same kind of fact, so each lane says which it is. On SLURM the partition *is*
the scheduling lane. On PBS, the authority for which property a queue routes
to is that queue's `resources_default.neednodes`, read from `qstat -Qf` on the
same read-only pass that already reads `enabled` / `started`:

```
  capacity: largest job that can start now: 64 cpu(s); largest the usable nodes could ever run: 128
  nodes: 3 usable, 1 unusable
    gpu: now=64 when-free=64 (1 usable, 0 unusable) [queue: gpu]
    narrow: now=18 when-free=20 (1 usable, 0 unusable) [inferred]
    wide: now=64 when-free=128 (2 usable, 1 unusable) [queue: batch]
  [inferred]: a node property no queue's resources_default.neednodes names
```

A lane marked `[queue: NAME]` is one a queue declares it routes to, and `NAME`
is the queue to submit to for that row's capacity. A lane marked `[inferred]`
is a node property nothing routes to as far as vq can see: its figures are
still true of the nodes carrying the property, but no queue has been shown to
send work there — set `resources_default.neednodes` on the queue if you want
that lane attributed. Every lane comes back `[inferred]` at a site that sets
no such defaults, and also when `qstat -Qf` could not be read at all. SLURM
partitions carry no marker; nothing about them is inferred.

`--json` spells the same distinction as `capacity.groups[].authority`
(`partition`, `queue_default` or `node_property`) with `capacity.groups[].queues[]`
naming the routing queues, and the probe's own `queues[].default_neednodes`
carries each queue's raw attribute so an attribution can be audited against
the scheduler's own words. A PBS node is listed in every lane it has a property
for — `capacity.nodes[].lanes[]` — because PBS lists a node's properties in no
meaningful order and singling one out invents a membership the scheduler never
stated.

Declare the figure you want enforced as `scheduler_max_cpus` on the scheduler
host. `vq submit` then **warns** when a request is wider than the lane can ever
run, naming both numbers, and submits anyway. It warns rather than refusing
because the declared width goes stale the moment a node returns to service, and
refusing on a stale number would reject work the cluster can now run. Leaving
it unset warns about nothing: unknown is not unlimited.

Sizing a wave against these figures is what prevents the failure this was
built for — a full-node request whose only node of that width was offline,
queued for six days while the local ledger recorded it as running. `vq programs HOST` and `vq admin status HOST` render
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
unlimited from vq's side and are governed by the batch scheduler. A job counts
against that limit until a successful poll reports its allocation finished.
From then on, while vq probes its exit marker and collects its artifacts
(`finishing`, `marker_probe_failed`, `fetch_failed`, `artifacts_unavailable`),
it holds no slot, so a slow or failing transfer does not block the next
submission. It stays tracked until collection completes, and a later poll that
finds the job live again takes the slot back. A failed poll records its error
without erasing the last collection phase, so a daemon restart keeps the slot
released. `vq status` reports the same count when it says scheduler admission
is blocked. A full
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

Result collection retries a dropped or stalled SSH connection twice on a fresh,
unmultiplexed connection, as the staging upload does. The remote result
archive's cleanup is best effort: if it cannot reach the host, the fetch still
succeeds and the leftover `<job>.result-<id>.tar` is found by the
transfer-archive sweep.

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

The remote wrapper also creates `<workspace>.submit-once.attempt` exclusively
before invoking the scheduler. It retains the attempt ID, queue job ID, workspace,
dialect, observation time and a digest of the scheduler argv. A concurrent call,
an existing receipt, or even an incomplete attempt file refuses replay as
**outcome unknown**, never as scheduler rejection. Proven-terminal command retry
rotates this exact file together with the old receipt and markers; ordinary
terminal workspace retention removes it too. Read retries and failed artifact
collection do not clear it.

This fence prevents a second scheduler invocation; its presence or absence does
not prove scheduler acceptance or non-admission, and its argv digest is not a
runtime attestation. Existing jobs without this file remain unknown when their
receipts and markers are missing. External accounting import and operator
non-admission reconciliation still require authoritative evidence tied to the
original attempt; an empty live queue or a missing file is insufficient.

#### Importing authoritative acceptance after a lost submit response

On the **original driver**, `vq admin reconcile-submit JOBID` reads the exact
original binding and its `binding_digest`. It does not contact the scheduler or
change a job. Multi-user drivers require `--owner-uid UID`, checked against the
daemon-owned submission authority. Changed commands, resources, workspace,
retry attempt or runtime pins invalidate the binding.

An authenticated operator can import one positive **Torque `qstat -f` record**
obtained from the scheduler authority (including an authoritative historical
record in the same format). Save this JSON privately, inserting the original
binding digest and the unmodified raw record:

```json
{
  "schema": "vq.scheduler-recovery-evidence/1",
  "decision": "accepted",
  "binding_digest": "<digest returned by the original driver>",
  "format": "torque-qstat-f",
  "record": "<complete raw single-job qstat -f record>",
  "record_sha256": "<SHA-256 of the UTF-8 record string>",
  "source": "<where, when and from whom the authoritative record was obtained>",
  "captured_at": "2026-10-14T12:00:00+00:00",
  "account": "<verified scheduler account>",
  "server": "<server suffix in the full scheduler job ID>",
  "scheduler_utc_offset": "+00:00",
  "attestation": "I verified this record with the authoritative scheduler operator."
}
```

The record must contain a full, non-array scheduler ID, matching `Job_Owner`,
`Job_Name`, exact `Output_Path` and `Error_Path` in the original workspace,
recognized `job_state`, and `qtime` within the original attempt's evidence
window. `scheduler_utc_offset` is the server's UTC offset **at admission** for
its local `qtime`; English ctime and epoch seconds are supported. The account,
server, historical offset and source authority are the operator's assertions.
The daemon checks their consistency and retains them; it does not independently
authenticate the scheduler or attest the remote runtime from a text record.
Verify those facts before making the explicit attestation.

```sh
vq admin reconcile-submit JOBID --evidence acceptance.json
vq admin reconcile-submit JOBID --evidence acceptance.json \
  --apply --plan-digest <preview-plan-digest> --attest-authoritative
```

Add `--owner-uid UID` on both commands for a multi-user driver and supply its
admin token through the supported environment, `--token-file` or `--token-stdin`
on apply. There is no direct-file fallback if the daemon is unavailable.
The daemon locks the original spec, revalidates the preview digest, and durably
records the raw evidence, original runtime binding, operator identity when
available, authentication mode and application time in its private
`scheduler-recovery/` journal. Ordinary audit entries contain only the job ID
and plan digest. Repeating an identical application is idempotent. A conflicting
application is refused; journals remain separate across proven-terminal retries.

`applied: true` means the journal is durable, not that collection is complete.
The normal reconciler attaches the positively identified existing allocation;
it never submits a replacement. Conflicting receipt/start-marker IDs, including
ones discovered later, retain an unknown reservation for operator resolution.
The journal survives a crash between recording evidence and attaching the job.
Keep journals with the driver state backup; they contain private job details.

Negative/non-admission imports are **not supported**: they need complete
scheduler accounting coverage and authoritative proof that no submit remains
in flight. A missing job, expired accounting window or retained attempt file
provides neither. Such jobs remain unknown with capacity and runtime retention
intact. Other scheduler dialects and array allocations are also refused.

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

A fetch is not all-or-nothing. A workspace file the fetching account cannot
read, and an archive member the tar extraction filter refuses (an absolute
symlink out of the workspace, for instance), are skipped and listed in the
fetched tree's `_vq/fetch-exclusions.json` instead of failing the job, and the
tree's `_vq/fetch-manifest.json` carries `partial: true` with the count. That
counts as a successful fetch — the alternative loses a whole workspace over one
file — but `vq fetch` prints a `PARTIAL` line and `vq fetch-all` tags the row
and counts partials in its summary. Each `vq fetch-all` run also writes
`<output_dir>/_vq/fetch-all-report.json` naming every selected job, its
outcome, and each partial tree's exclusions; `--report PATH` moves it and
`--report none` declines it. Use that report, not the directory count, when
stating the coverage of a fetched corpus.

Fetch reserves three generated control files: `_vq/terminal-diagnosis.json`,
`_vq/fetch-exclusions.json`, and `_vq/fetch-manifest.json`. A raw workspace or
archive containing one of these paths, or a non-directory `_vq`, fails with a
path-specific metadata-collision error. The source and any previous fetched
destination are preserved. Ordinary lifecycle files inside `_vq` still fetch.
Whole-tree remote fetch requires a negotiated provenance response: the receiver
passes a fresh token through the hidden `--fetch-protocol-token` option, and
the emitter binds the tar stream to it. Older remotes reject this option and
must be updated before a new client can fetch their workspace or workdir.
Missing, malformed or replayed responses fail explicitly without replacing a
previous destination; there is no silent partial fallback. This request binding
prevents a raw archived sidecar from being mistaken for generated metadata.
Artifact-only fetches are unchanged.

A negotiated remote stream may carry one canonical regular file for each emitter-generated
diagnosis and exclusions sidecar; duplicate, aliased, or non-file controls are
refused before the staged tree replaces a previous fetch. A previously fetched
tree used as a new raw workspace therefore needs an explicit operator decision
about its control files; fetch does not silently overwrite or exclude them.

Only a positively identified same-job destination is reported as an
already-present skip. Other destination collisions, per-job filesystem errors,
and malformed archives produce error rows and leave the rest of the bulk sweep
running. Malformed archives remain failed jobs, not partial successes.

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
calculation. Configured SHA pins longer than the 12-character display
abbreviation are checked against the full commit identity. Every expected digit
must match, case-insensitively; an extra suffix beyond the commit is a mismatch.

For one-off docs/example artifact jobs, pass `--expected-sha SHA` with
`--program NAME`. The CLI accepts 7–40 hex characters and validates the
program checkout before queueing. It records the host-validated canonical
12-character SHA for shorter prefixes and preserves longer submitted pins in
lower case, checking all their digits against the full commit SHA. That makes
later dispatch failures and fetched diagnostics identify the intended checkout
without discarding the precision of an explicit pin.

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
python = "/home/USER/vibe-view/.venv/bin/python"
git_dir = "/home/USER/vibe-view"
branch = "main"
update_script = "scripts/update.sh"
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

### A scheduler host that does not answer vq's polls

Some poll failures are not the scheduler's answer at all: ssh never completed a
connection. In the daemon log they read `Connection timed out during banner
exchange`, `kex_exchange_identification: Connection closed by remote host`, or a
connection refused, reset or unroutable — always `exit 255`, and always with the
same message for every job on that host, because one batched poll carries them
all. The symptom an operator sees is a host whose jobs all sit in
`vq queue -s poll_failed` while `ssh HOST qstat` from a shell still works: the
cluster is fine and only the daemon's own connections are being turned away.

A host under that load often fails a second way as well, and vq now treats the
two alike: the connection is accepted and then nothing comes back, so vq's own
120-second poll timeout expires and the attempt's outcome is never learned.
That is not evidence of a healthy host either, and it is just as
load-dependent.

vq answers both symptoms by leaving the host alone for a while instead of
re-contacting it as fast as the failures return. Each deferral is announced:

```
WARNING vq.daemon: scheduler poll to pbs-cluster did not complete its
connection (3 attempt(s) in a row without an answer from the host); deferring
its next observation by 120s. Jobs there keep their last known state and are
never terminalized by a failed poll.
```

An attempt whose outcome was lost rather than refused reads `did not come back
with an outcome vq could read` in the same line, and counts in the same run.

The wait starts at 30 seconds, doubles for each further attempt that does not
reach the host, and stops growing at 5 minutes; the first poll that reaches the
host clears it, and a poll that succeeds logs that polling has resumed. What
ends a run is evidence that the host **answered**, not that the failure changed
shape: a rejected key (`Permission denied (publickey)`), a missing `qstat` or
an unparseable table returns the host to the ordinary cadence at once, because
those are standing faults that waiting cannot fix and should stay visible. A
scheduler client that reports it could not reach its own server is handled the
same way as ssh reporting the same thing: in both cases nothing was observed
and another attempt this instant is worth no more than the one that just
failed.

The wait belongs to the **host** — not to a job, not to a dispatcher, and not
to the host's current queue:

* A job submitted in the meantime does not reopen a host that is not
  answering, and the wait never applies to another host.
* Editing `config.toml` does not reset it. A reload rebuilds a target's
  dispatcher while its in-flight jobs keep the generation they were submitted
  with; the wait, the single in-flight observation and the fetch turn are all
  keyed on the target's configured name, so one host stays one host.
* The host's jobs finishing does not reset it either. A campaign that keeps
  submitting would otherwise hand a silent host a fresh 30-second run every
  time its queue emptied for a tick. A host with no running job is forgotten
  only after a full 5-minute ceiling with no attempt made.

What the deferral does **not** do: it never cancels, terminalizes, fetches or
re-stamps anything. A deferred pass makes no observation and claims none, so a
job's lifecycle stays `RUNNING` and its `scheduler_state` stays `poll_failed`
with the diagnostic from the last real attempt. Submission and result fetch are
not deferred either — only the read-only poll is. The cost of the deferral is
observation latency on that host, bounded by the 5-minute ceiling, and the job
detail shown for it is stale for at most that long.

An explicit `vq show <id> --refresh` against a job on a waiting host does not
break the wait: vq's own connections are part of what the host is turning away,
so a refresh in a shell loop would be a way around the pacing that is
protecting it. The refresh is answered straight away and names the cause —
`scheduler_host_poll_deferred` — instead of reporting that the job was not
observed, or timing out against a wait vq itself chose. The remaining interval
is on the deferral warning above.

If a host stays in this state, the question to take to the site is what is
turning connections away: a login node's sshd `MaxStartups` throttle, a gateway
saturated by other traffic, or the driver host itself being too loaded to
complete a handshake within `ConnectTimeout`. `vq doctor HOST --verbose` probes
the same transport from a quiet context, and comparing its result with the
daemon's while recording the driver's load average is the measurement that
distinguishes those.

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

A kill is recorded from the driver's last poll, so it can reach a job the
scheduler has just finished. When the scheduler's accounting then reports
the job's own end (Slurm `COMPLETED` or `FAILED`, not `CANCELLED`), the
daemon keeps that outcome and exit code rather than `killed`, and records the
ignored request as an event (#126). Torque's qstat cannot prove this, so
there a late kill still reads `killed`.

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

When the selected program uses vibe-queue's `scripts/update.sh`, the outer admin
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

**Serving environment in an immutable runtime slot (#73).** When the daemon's
program has `runtime_slot_root`, `vq self-update` and single-environment
`vq admin update` prepare the new generation at its final path while the old
daemon keeps running, then rebind the registry and the user service in one
recoverable transaction (see `design_immutable_venv_runtimes.md`,
"Serving-daemon generations"). Two things to know before the first such update:

* **The registry file is rewritten as a whole.** Every `[programs.<alias>]`
  that shares the daemon's checkout or interpreter gets
  `runtime_slot_execution = true`, and `config.toml` is re-serialized from its
  parsed values. The values are preserved exactly (the transaction refuses if
  they are not), but comments and layout are not: keep a commented copy under
  version control if you want them.
* **The bound files must be plain, owner-controlled files.** `config.toml`, the
  `vq-daemon.service` unit or `com.vq.daemon.plist` agent, and the runtime
  root's `current`/`previous` pointers must be regular files or symlinks owned
  by you, in directories that only you can write, reached through no symlinked
  path component. A symlinked `config.toml` or unit file (a dotfiles checkout,
  say) or a group-writable config directory is refused before anything is
  prepared, with an error naming the file and the rule.

After moving the daemon to a slot, its registration can also set
`runtime_slot_build_input = "current"` (#142). Preparation then clones from the
active verified generation and keeps its upstream, install profile and exact
SHA checks. Restart, rollback and interrupted-update recovery use the retained
generations even when the registered checkout is absent. Upgrade the vq used
for updating and recovery to a version supporting this serving input before
setting it; earlier versions refuse it. This setting does not repoint existing
launchers, `remote_vq` commands, web services or shell entry points, and does
not authorize deleting the checkout. Review those consumers and the
`vq admin retire-checkout` plan first.

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

**Post-restart readiness no longer waits for the startup walk.** After
the restart, the update polls the daemon's RPC until it answers
with the expected source identity. From v0.26.10 a daemon publishes that
socket **before** it reconciles surviving jobs, so readiness and provenance
are answerable within seconds of process start whatever the queue holds
(#67: a driver supervising 175 live scheduler jobs over an 18,013-spec queue
took 9 min 51 s to finish reattaching -- each scheduler job is probed over
its own SSH round trip -- and a valid install was rolled back although
nothing was wrong with it). Dispatch still waits for the walk; only
readiness was decoupled from it.

**The readiness window still scales with the state dir**, because that
ordering fix is not always the code doing the waiting: `vq admin update`
never re-execs, so the window in force is whichever version is already in
memory, and a managed rollback restarts the *previous* install, which does
scan before it answers. The window is 60 s base, plus 25 ms per queued job
spec, plus 2.5 s per live scheduler job, capped at 1800 s -- such a daemon
walks every `queue/*.json` **and** rebuilds one handle per live cluster job,
each over its own connection to the scheduler host (#53: a 21,683-spec queue
exceeded the previous 247 s window under load, causing a valid install to
roll back; #67: 5 min 20 s went on reattachment alone, which the spec-only
model did not count at all, against a 600 s ceiling no formula could reach).
The scheduler count uses one directory inventory and at most 5 s of content
reads, checking the budget between records. If it reads `k` of `N` records and
finds `L` live scheduler jobs, it allows for `L + (N - k)` jobs while the rest
remain unread. This is a conservative bound for that inventory, **not** an
estimated live count: filesystem order need not be representative. A completed
scan uses its observed live count; a record whose content cannot be read adds
one unknown job instead of being called inactive. An unavailable directory inventory selects
the 1800 s cap and logs that no count was established; an absent queue is empty.
The content budget cannot interrupt a single filesystem call, does not bound the
metadata inventory, and does not predict jobs added after the snapshot.

Verification returns as soon as readiness and provenance pass. The allowance
does not guarantee a startup duration. Override it with
`VQ_DAEMON_HEALTH_TIMEOUT` (finite positive seconds) in the client environment
if the driver still needs longer. The local rollout driver action and fresh
driver re-entry inherit this variable. For example:

```sh
VQ_DAEMON_HEALTH_TIMEOUT=900 vq admin rollout-latest
```

This does not configure remote hosts' readiness windows. A timeout or an
identity mismatch still fails verification and invokes managed rollback;
the longer allowance changes neither rollback nor service-stop semantics.

**Liveness evidence controls the bounded extension.**
When the scaled window expires and the daemon has never answered RPC at all,
the update asks the service manager whether a daemon process exists (#67).
If one does, the wait continues to at most **three times that host's own
window**, never past an absolute 3600 s, re-asking the service manager ten
times along the way -- a startup that outran its allowance is not an
unhealthy daemon, and rolling it back costs the driver more downtime than
waiting. The extension is a multiple of the window rather than a flat number
of seconds on purpose: a host whose allowance is seconds has nothing to scan,
so an expiry there is real and is not stretched. If no daemon process exists,
verification fails immediately with a message saying so, rather than waiting
out the rest of the window: that is a failed start, and
`VQ_DAEMON_HEALTH_TIMEOUT` will not help it. Once the daemon has answered
even once, the window is never extended -- the socket is up, and a missing or
mismatched source identity is an answer, not a slow start. A service manager
that is unavailable or returns ambiguous state does **not** prove the daemon
stopped: it permits the same bounded extension, with a diagnostic preserving
that uncertainty. Liveness alone never proves startup progress. The ceiling
bounds scheduled waiting; a query already in progress has its own timeout and
can finish past the deadline. No further liveness query starts after that deadline.

An actual start, restart, or rollback retains the full identity settling window:
the outgoing daemon can still answer before the replacement starts. Checks
that only observe an existing daemon (pre-update identity recovery and terminal
receipt recovery) stop on the first answering mismatch. These calls have no
pending restart that could change the identity. Silence may still wait for a
first answer; strict SHA and tree checks remain required.

Because `vq admin update` runs in the interpreter that was loaded **before**
the install, a change to these allowances governs the *next* update on a
host, not the one that ships it. `VQ_DAEMON_HEALTH_TIMEOUT` is the only lever
that applies to the run in progress.

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
frozen, load average climbing past nproc, swap thrashing), check for
memory pressure, including excessive build parallelism and exhausted swap.

### Hard-reset recovery checklist

After the box boots, in order:

```sh
# 1. Verify network access using the site's private recovery instructions.

# 2. User-systemd healthy? No zombies?
pgrep -laf 'systemd --user'

# 3. Daemon auto-launched from disk?
systemctl --user --no-pager status vq-daemon | head
~/vibe-queue/.venv/bin/vq --version

# 4. Inspect what got orphaned at the reset
~/vibe-queue/.venv/bin/vq queue
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

Use these safeguards when configuring build and job execution:

* **vq daemon path**: admin update runs the script under a `nice -n 19
  ionice -c 3` argv prefix (`admin._build_niceness_prefix`). Since #133 it no
  longer injects `CMAKE_BUILD_PARALLEL_LEVEL`. Its MemTotal-based level
  overrode the product helper's sizing and OOM-killed a host's build whose
  MemAvailable was far below MemTotal. The product's build helper now sizes
  parallelism from available memory per build phase. An operator-set
  `CMAKE_BUILD_PARALLEL_LEVEL` passes through, and `vq admin update HOST`
  forwards the driver's value to the remote update, since ssh does not carry
  the operator's shell environment.
* **Interactive path** (vibe-qc side): `scripts/update.sh` self-re-execs
  under `nice -n 19 ionice -c 3` and sizes its own build parallelism.
* **Per-job cgroup MemoryMax** (v0.5.x): each `vq submit`'d job
  runs in its own transient `vq-job-*.scope` with a memory cap.
  Verify that cgroup limits are enabled for the selected execution path.

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
ExecStart=%h/vibe-queue/.venv/bin/vq admin auto-update vibeqc-release
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
An admin-update marker in `vq overview --json` (and in the fleet console's
`/api/v1/fleet`) carries its managed-update receipt without the file bytes an
immutable-slot transaction retains for recovery: `managed_transaction.
slot_transaction` is reduced to `root`, `sha` and `staged`. The full receipt
stays in the owner-only marker file on the host, where `vq admin
recover-update` reads it.

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

### Concurrent scheduler runtime builds

A scheduler runtime update releases driver-wide admin ownership while its
remote build runs. The source checkout fence covers mirror feed, archive
staging and tag resolution. It ends once the remote command is bound to its
exact source, before the build wait: report refresh and another target can
then use that checkout. During preparation, physical aliases still contend.
Caller-owned outer fences remain held, and the runtime marker continues to
exclude a competing update of the same target. No local source is read during
remote verification, staging cleanup or result persistence.

The running driver's checkout and interpreter environment retain shared read
locks during this window. Other readers can proceed, but a self-update or
direct lifecycle mutation of those files cannot. Report/self-update operations
that already hold outer admin ownership, and installations whose driver
resources cannot be identified, retain the broader serialization.

After the remote build returns, the updater reacquires admin ownership before
verification and result persistence, and revalidates the state registry's
identity. It keeps its exact marker lease throughout. A failed build retains
its recovery marker; a changed registry refuses finalization. Concurrent builds
do not imply that a newer runtime has been delivered or accepted: those claims
still require the corresponding verified delivery receipt.

### Lost SSH response: reconcile before another update

Detached scheduler observation and retained-request reads retry a timeout or
SSH exit 255 once with multiplex reuse disabled. The retry reads the same run
and output offset; it does not restart the build or change the shared SSH
master. Launch, termination and cleanup commands are not automatically replayed.

A legacy detached scheduler build that loses observation retains its run and
reports an unknown outcome, including when its wall deadline arrives before
the observation grace expires. A failed or malformed last poll must not trigger
remote termination or cleanup. Reconcile the retained run before retrying.

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

### A deferred host's leftover hold blocks every plan (#96)

A finished rollout that deferred a scheduler host keeps its owned plan-bound
hold there. Every later `rollout-latest` refuses with "retains an owned hold
on HOST". `--supersede-plan-hold` cannot help: it needs health evidence at the
new pins, and only a rollout of that host produces that. The owning rollout
can abandon the host instead:

```sh
vq admin rollout-latest --resume ROLLOUT --abandon-host HOST
```

This is allowed only while every action of ROLLOUT for HOST is still
`not-run` with no operation attempt, and the hold carries its exact control
binding. A supported read-only drain snapshot must show no claim on HOST other
than ROLLOUT's own lease. A legacy claim or another owner's lease refuses. The
journal then records the abandonment on the hold and those actions, and the
hold is released with an exact-owner `drain --release`. Nothing is executed on
HOST, and the next rollout plans it normally. Re-running the command reports
`already-abandoned` without contacting the host.

Since #144 a host whose started actions all failed verifiably can be abandoned
the same way. Each such action must be `failed`, every attempt harvested, and
its durable operation completed with an executed nonzero exit. A successful,
running or outcome-unknown action still refuses. This is the recovery when a
rollout stops after an update failed on a scheduler host and a newer report
has since been accepted: its plan-bound holds on the host and every alias
otherwise block every later plan. Abandon each held target, the canonical host
and each alias, one invocation per target. The failure fence is not consumed
by the abandonment. The next rollout that plans HOST, unscoped or with
`--only HOST`, skips its updates once and consumes the fence. The run after
that updates HOST from live state. Until then, `--dry-run` and
`--verify-only` refuse on the unconsumed failure.

A hold whose rollout did run actions on HOST is covered by
`--supersede-plan-hold ROLLOUT HOST` instead. Since #130 it accepts each
scheduler lane healthy (LAST OK, clean) at either the new report's pin or the
obsolete rollout's own pin. The second case is the one a new pin creates: only
a rollout can install it, and the old hold blocks that rollout. Old-pin
evidence also requires every operation of ROLLOUT on that host to be settled
(completed, harvested, no unacknowledged failure). The receipt marks those
lanes `"basis": "historical"`, and a later validation checks them against
ROLLOUT's own authenticated report.

Since #138 a third case counts: a non-vq lane healthy at a commit that
descends from the new pin, such as a dev lane deployed from `main`. It counts
only when the planner itself proved the descent and skipped the lane as
"newer descendant already deployed; no downgrade". The receipt marks the lane
`"basis": "descendant"` and records the live SHA beside the pin. A vq lane
ahead of its pin is release drift and never counts.

A dry-run or `--verify-only` that meets such a hold refuses even when HOST is
outside `--only`/`--skip`. The check is fleet-global on purpose, so a scoped
preview cannot hide the hold. The refusal names the supersede command for a
scheduler hold. Once the receipt is written, the preview no longer refuses.

Since #99 a rollout does this itself when it finishes. Finalization releases
every owned hold whose host still has only never-started actions, with the
same proof and the same journal rows. A hold that fails the proof, for example
because the snapshot shows another owner's lease on the host, is kept, and
its hold record says why under `auto_release_refused`. `--abandon-host`
remains the manual path for those holds and for journals written before this
rule.

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
blocks on the local outcome-unknown action and refuses replay.

**Settling it from the remote receipt (#115).** On the driver, the owner
reconciles such an operation explicitly:

```sh
vq admin reconcile-operation OPERATION_ID --dry-run   # evidence only
vq admin reconcile-operation OPERATION_ID
vq admin rollout-latest --only HOST                   # harvests it, releases the hold
```

The operation ID is the one the refusal names. The command never relaunches
or replays anything. It requires all of the following:

- the local operation is outcome-unknown, for a scheduler-runtime action,
  and has its create-once detached binding;
- the build host still holds that exact run, and the request it retained
  rebuilds byte for byte from the local binding, which a read-only helper
  action checks;
- the run's terminal receipt reports success (rc 0, not timed out), and its
  whole retained output matches the stored digest;
- no admin update for that runtime is still running, and its marker records
  no paused jobs or managed transaction;
- the same independent login-host verification as the update passes for the
  operation's own SHA and tag, and the retained build command carries that
  SHA and tag.

Only then does it record LAST OK and clear the dead update's marker. It then
writes `reconciliation.json` (the binding digest, the remote result and the
verification) and a terminal success with no child identity, which is
accepted only alongside that receipt. `--dry-run` runs every check,
including the verification, and writes nothing. A failed, running or missing
run, a mismatched binding and a failed verification all leave the operation
blocked, with the reason.

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

Exact pause admission, pause-intent reconciliation and token-clearance proofs
share a five-second **lock-wait admission deadline** across their nested scans
and per-job rechecks. After that deadline, an uncontended lock can still be
acquired immediately; a contended row is explicitly unresolved and adds no
further wait. This bounds lock contention, **not the total scan or cleanup
wall time**: reading and authorizing retained history can take longer.

If interruption leaves a lock reachable through the original exception,
cleanup can return without waiting forever for that same exception to unwind.
A failed single or batch update retains its pause token and transaction marker,
records both the cleanup failure and original exception, and unwinds. It never
clears recovery authority because a row timed out. Inspect the failed update,
then use `vq admin recover-update` for a managed receipt, or
`vq admin clear-update-marker` for a pause-only receipt; the retry gets a fresh
admission deadline and must prove the exact token clear. Other operators' paused
jobs remain paused. Repeated-daemon reconciliation also uses a bounded sweep;
unresolved intents remain available to its next pass.

The daemon's own per-tick pause-intent sweep is not a proof, and it is the one
caller that repeats forever, so it asks for the same saving in a narrower form:
it reads each row before locking it and skips the ones carrying no pause
intent. A row with an intent, and a row that cannot be read at all, still take
the lock and the ownership check. An intent armed between that read and the
lock is picked up by the next tick. The saving is off by default, so
`pause_token_scope_with_proof`, which reconciles before it captures, keeps the
exact locked scan.

From v0.26.10 that sweep does not even read a retained row it has a verdict on
(#82). The daemon's admission scan records which settled rows carry no durable
pause intent, keyed on the spec's own `(st_mtime_ns, st_size, st_ino, st_dev)`,
and the sweep skips exactly those paths whose bytes still match — one `stat`
each. Everything else is swept as before: a row no pass has read, a settled row
that already carries an intent, a row rewritten since (arming an intent is a
rewrite, and every spec write is an atomic rename onto a fresh inode), and a
row that cannot even be stat'ed. The skip is a negative hint about work to do
and never authorization to signal a process: a row it declines still goes
through the locked, authorized path. It is also inseparable from the unlocked
read above — handing the verdict to a scan that has not asked for that read
raises rather than skipping a row — so the exact admission and
token-reconciliation proofs cannot inherit it and keep scanning every row under
its lock, terminal rows included.

Every bulk verb resolves the **effective authorization policy once for the
operation** and checks each row against that. The policy is the half of an
ownership decision that does not depend on the job row: the personal and system
configs, and in multi-user mode the caller's group and passwd entries. Which
rows are checked, in what order, and with what verdict is unchanged, and the
final verdict on a row is still taken under that row's own lock. A policy that
cannot be established still stops the command, now before the first lock is
taken rather than on the first row. This is the per-operation boundary the
single-job verbs already use: a revocation between two operations takes effect
on the next one, and a revocation *during* one bulk scan no longer takes effect
part-way through it, which previously depended on where in the queue the scan
had reached.

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

When `vq status <jobid>` shows `state: aborted_by_queue` and its
`failure:` line (`failure_reason` in `--json`, and `reason` inside its
`terminal_diagnosis`) begins `pid_recycled:` or
`cgroup_scope_mismatch:`, that's a new flavor of abort introduced
by the audit § 1 / § 2 hardening sweep. Both are protective --
they're the queue refusing to trust ambiguous state rather than
risking a wrong-job kill or a misattributed exit code.

| Reason prefix | What happened | What to do |
|---|---|---|
| `pid_recycled` | Spec recorded `pid=X` + `pid_start_time=T` at dispatch. On the next reattach attempt, `/proc/X/stat` field 22 had a different start time -- the OS recycled PID X to an unrelated process. The daemon won't claim the new process is our job. | Same as any ABORTED_BY_QUEUE: read stdout.log to see if useful output was produced; `vq resubmit` if needed. |
| `cgroup_scope_mismatch` | Spec recorded that the job runs in `vq-job-<id>.scope` with a specific MainPID. On reattach `systemctl --user show vq-job-<id>.scope -p MainPID` returned a PID that doesn't match the spec. Same protective stance as pid_recycled, but at the cgroup layer. | Same recovery as above. Check `journalctl --user -u vq-daemon` for the cross-check log line -- sometimes it points at a scope-leak bug worth filing. |

The reason is free text on the same `failure:` line, as it is for the older
aborts: a pgid that is gone after a daemon restart, no pgid recorded, or an
orphan that exited without an exit-code marker. Before v0.5.50 the two cases in
the table would have been silent -- the daemon would have re-attached to
whatever PID was at `spec.pid` (which is wrong if it got recycled) and either
killed the wrong process or misattributed the exit code.

---

## Refreshing the root-owned `/opt/vq` install (v0.24.x+)

**Legacy-layout only:** the privileged helpers still assume the former nested
source tree. See the [split-layout limitation](multi_user_deployment.md#split-layout-limitation)
before using this section; these commands do not provision a fresh standalone clone.

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

## The fleet broker

One host, normally the coordinator, can hold submissions and place them on
daemon hosts by CPU, memory and queue availability. It places them strictly
first-in-first-out and does so late: a job waits on the broker until some
target can run it now. The role is off unless the broker's own config
enables it:

```toml
[broker]
enabled = true
targets = ["alpha", "beta"]   # daemon hosts, each machine once by its canonical entry
```

The other settings and their defaults are `interval_seconds = 60`,
`max_age_seconds = 180`, `retry_interval_seconds = 300` and
`collect_timeout_seconds = 60`. Every target needs a vq with
`vq broker capacity`, a single-user daemon, and SSH access from the broker as
the operator. Run `vq broker capacity` on a target to see what the broker
will see: it prints each closed gate. Admission is open only with positive
evidence:

- the daemon answers;
- a running daemon uses the state root this vq resolves;
- the daemon's own published admission snapshot holds nothing;
- no local job is waiting.

The daemon's snapshot holds for:

- a drain touching local work;
- an admin update;
- host memory pressure;
- a pending refresh;
- a running build job;
- a changed config.

**What the broker's daemon does.** Every `interval_seconds` it runs
`vq broker pass` as a separate process, which logs to
`<state root>/broker-pass.log`. The pass asks every target for its evidence,
then works through these steps in order:

1. It finishes anything a crash interrupted.
2. It settles the capacity promised to jobs that a target now lists.
3. It resends a transfer whose outcome is unknown, with the same key and
   only to the same host.
4. It places held jobs oldest first. The first job that cannot run now
   waits at the front, and nothing behind it goes first. A job skips a
   host that is refused, unreachable or lacks its program, and falls back
   to the next-ranked one.

Run `vq broker pass` by hand to see one pass; a second pass while one runs
is refused.

```sh
vq broker list                     # every broker job: state, target, target job
vq broker show b-0123456789ab      # attempts, blockers, diagnostics
vq broker cancel b-0123456789ab    # held: cancelled at once
```

When the fleet console runs on the broker, `/fleet/jobs` also lists these
jobs under "Broker jobs", metadata only: id, state, target, resources, name,
times, and blockers or operator action. Held jobs retain the latest placement
refusal reasons across restarts; these are observations from the last pass,
not a fresh capacity probe. For the command and diagnostics, use
`vq broker show`.

**Submitting through the broker (clients).** A client needs SSH to the
broker only. Add an entry for it with `broker = true`. Keep a separate
entry if you also submit to that machine's own queue.

```toml
[hosts.fleet]
ssh = "coordinator"
broker = true
```

```sh
vq submit fleet -d ./job --cpus 16 --mem-mb 64000 -- python run.py
vq submit fleet calc.py --python /opt/vibeqc/bin/python --cpus 8 --mem-mb 16000
vq status fleet b-0123456789ab        # broker record, then the target's own status
vq logs fleet b-0123456789ab -n 100   # the target job's output, once placed
vq fetch fleet b-0123456789ab -o out  # the workspace, streamed through the broker
vq kill fleet b-0123456789ab          # cancel, and kill the target job once placed
vq wait fleet b-0123456789ab          # block until the target job ends
vq logs -f fleet b-0123456789ab       # follow it, once placed
```

A broker job needs `--mem-mb`. A single-file job needs an explicit
`--python` that is valid on every target, because the broker picks the
target and the client cannot know its interpreter. `--dir` and `-c` jobs
need their command after `--`. `--pool` scopes placement to a `[pools]`
entry on the broker. A broker job cannot carry these options, each of which
is refused with its name:

- `--array` and `--chain`;
- dependencies;
- `--at`, `--refresh` and `--branch`;
- `--scheduler-target`;
- the wait and fetch flags;
- a `.qvf` input.

If the broker's answer to a submission is lost, the relay says so and keeps
the upload. Check `vq broker list` there before submitting again. A submission
with `--idempotency-key KEY` avoids that check: the key names the broker job
ID in advance, and submitting again with the same key and content returns
that same job, never a second one. Different content under the same key is
refused. With one
`broker = true` entry, `vq status b-…` without a host goes to it. A fetched
tree lands under the target job's ID.

`vq wait fleet b-…` polls the broker. The job counts as pending while it is
held or its transfer is in doubt. Once placed, it ends with the target job's
state and exit code. A job cancelled before placement ends as `killed`. A
timeout names the broker state, the blockers and any operator action.
`vq logs --follow fleet b-…` waits while the job is not placed yet, saying
so once, and then streams the target job's output until it ends. A job
cancelled before placement is an error, since it never ran. `vq kill
--resubmit fleet b-…` kills the job and then holds a fresh copy (a new `b-…`
with the same payload and intent) for the broker to place afresh. It
resubmits only once the old job provably stopped; a transfer in doubt is
refused with its operator action, since resubmitting could run the work
twice.

**Recovery.**

- **A job stuck at the front** (`vq broker pass` shows it as `head`): `show`
  lists each host's blocker. Fix the host, or cancel the job.
- **A pass that stops with a job's reason** (for example a payload that is
  gone): cancel that job; the queue then moves on.
- **An uncertain transfer** (`transferring` with diagnostics) is resent
  until the target answers. It is never sent to another host.
- **A job cancelled while its transfer was uncertain** shows `OPERATOR
  ACTION` in `list` and the kill instruction in `show`. It is never resent,
  because the first send may have landed.
- **Settling a transfer in doubt** (a target that keeps refusing, or a
  cancelled one): `vq broker reconcile b-…` on the broker asks the recorded
  target to fence the job's key (#64). If the first send landed, the job
  becomes `placed` on that target job. Otherwise the key is durably refused
  there, so no late copy can be accepted, and the job holds again for a fresh
  placement (`--cancel`, or an earlier cancellation, cancels it instead). The
  target must answer from the recorded state root; otherwise nothing changes.
- **Two targets with the same state root** are both skipped with `share one
  state root`. List that machine once.

The trust boundary, including that compromising the broker compromises
every listed target, is in `SECURITY.md`, "Broker boundary". The design is in
`docs/design_broker.md`.

---

## Dev lanes that track `main` instead of the release pin

```toml
# driver config.toml; also raise min_vq_version to the first release
# that reads this key, so an older vq refuses instead of ignoring it

[fleet_tracking_lanes]
"vibeqc-dev" = "main"
```

```sh
vq admin rollout-latest                  # release lanes to the pins, dev lanes to main's head
vq admin rollout-latest --only-tracking  # advance only the tracking lanes
vq admin rollout-latest --verify-only    # a tracking lane anywhere past its pin is converged
```

A report pins what was released. Which lanes follow a branch is fleet
policy, so it lives in the driver config rather than in the report. Only
`vibeqc-dev` and `vibe-view` can track a branch. `vibeqc-release` and vq
itself always stay on their pins, and naming either one is a config error.
`vibe-view` is the fleet's only viewer lane, so tracking it means no host runs
the released viewer.

**What a rollout does.** It fetches each tracked branch into the checkout its
report pin resolves in (`[pin_source_repos]`), under the same lifecycle fence
report discovery uses, and resolves the head to a full SHA **once**. The head
must descend from the report pin, or the rollout stops with `does not contain
the report's dev pin`; a tracked lane never moves behind what was released.
Every lane of that program then deploys that exact SHA through the usual
per-host path: `vq admin update vibeqc-dev HOST --expected-sha <head>`, meaning
an atomic per-SHA runtime on scheduler hosts and the normal pause and update on
direct hosts. The plan prints `tracking: vibeqc-dev follows main at <sha>`, and
`--json` payloads carry a `tracking` list. Each journaled attempt records its
own target SHA.

A lane already at the head is skipped. A lane on a newer descendant of the
head is also skipped, because nothing is ever downgraded. A lane on a SHA that
diverged from the head, or a head that does not descend from the pin, is
blocked. Release lanes are planned exactly as without tracking.

The rollout id is still derived from the report. A later rollout under the
same report, after `main` has moved, deploys the new head as a fresh attempt
for each tracked lane. That is allowed only when the new attempt differs from
the lane's previous one in nothing but the target: the same argv with the
old SHA replaced by the new one. The previous attempt must also have
succeeded. A failed attempt allows it only after its failure has been
reconciled and a retry authorized, the same condition as for a retry at the
old target. An unknown or unfinished attempt keeps its refusal, whatever
`main` did since. A driver self-update re-enters with the heads its parent resolved, so
the lanes deploy what the plan showed.

**`--only-tracking`** advances only the tracking lanes. Release lanes,
scheduler helpers, each host's own vq and untracked programs that would
update are reported as deferred (`out of rollout scope: --only-tracking
(...)`). Lanes that are skipped or blocked keep that decision, and blocks
there neither stop the refresh nor keep the host held afterwards. A host
whose own vq or scheduler helper is not on its report pin gets no tracked
update either: its tracked lanes are deferred with `own vq is not on its
report pin; run a full rollout before --only-tracking`, so a lane is never
deployed by a vq the report does not pin. The driver's own vq lane is still evaluated first, as in every
rollout, because the pinned vq is the one that stages and verifies everything
else; when the driver is already on its pin, that is a skip. A tracking
refresh composes with `--only`/`--skip`, keeps the same serialization,
markers, holds and drain bracketing, and never marks the rollout complete.

**`--verify-only`** judges a tracking lane against its report pin. Any healthy
SHA descending from the pin is converged, so a lane that has fallen behind
`main` but not behind the pin stays green. A lane behind the pin, or on a
divergent SHA, is degraded. The output adds one line per tracking lane:

```text
TRACKING host_d           vibeqc-dev       3f2a9c1e7b44  5h old, behind-head main 8e01d5aa2c90
```

This line gives the lane's SHA, the age of that commit, and whether the lane
is at, behind or ahead of the head. In `--json` the same data is the
`tracking` list. If the tracked branch cannot be fetched or no longer
contains the pin, `--verify-only` warns (`tracking heads unavailable`) and
still gives its verdict; an actual rollout refuses in that case.

Exact-pin submission is unchanged: `vq submit --program vibeqc-dev
--expected-sha <sha>` still selects one generation, and the job's
`program_runtime_pin.resolved_git_sha` still records what ran.

Since #138, `--supersede-plan-hold` also accepts a healthy lane on a later
commit that descends from the new pin, so a host whose tracked lane runs
ahead can be superseded. A vq older than `fleet_tracking_lanes` ignores the key with a
warning and keeps tracked lanes at or after their pin, which is why the
example raises `min_vq_version`. On direct hosts an in-place dev rebuild still pauses
dev-branch jobs until those lanes are runtime slots.

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

## Declared `extras`: where the floor is applied, and where it is not

```sh
vq admin install <env> <host> --expected-sha <40-hex>   # or --from-report
```

Clones the program's `upstream` at a pin and runs its own `install_script`.
A program that declares what its venv is for --

```toml
[programs.vibeqc-queue]
extras = ["web"]
```

-- has that floor applied **here too**, not only on the next managed update.
vq appends `--extras PROFILE` to the installer, resolved to the smallest
published profile covering both the declaration and whatever the configured
command already asked for, so `install_script = "scripts/install.sh --editable
--extras dev"` plus `extras = ["web"]` builds `all`. A configured profile that
already covers the declaration is left exactly as written.

It appends that only to vq's **own** `scripts/install.sh` or
`scripts/update.sh`, proved against the checkout the program points at -- the
same proof a managed update makes before it names an install target. Any other
installer is free text that has never heard of `--extras`, so the install is
**refused**:

```
env 'vibeqc-queue' declares extras ['web'], which a fresh install applies only
through vq's own scripts/install.sh or scripts/update.sh. Its install_script is
'bin/setup.sh', so this provision would build the venv without the runtime the
program declares -- refusing instead of installing it. Point install_script at
the checkout's own scripts/install.sh. If update_script is the checkout's own
scripts/update.sh, you can instead drop the declaration, provision, then restore
it and apply it with: vq admin update vibeqc-queue (scripts/update.sh refuses a
venv vq's own scripts did not build, so this route needs install_script to
build through the checkout's scripts/install.sh)
```

That is the refusal before the clone. It cannot yet read a configured
`update_script` to prove it, so it names the second route conditionally; a
program with no `update_script` is told instead that it has only the first.
Two conditions are part of the second route. The declaration has to be
restored: an ordinary update applies the floor the program declares, so an
update of a program that declares nothing appends no `--extras`. And
`scripts/update.sh` has to accept the venv, and it refuses one that none of
vq's own scripts built -- an unmarked virtualenv -- so the route is real for an
installer that wraps vq's `scripts/install.sh`, not for one that builds the venv
itself.

A refusal that comes only **after** the clone -- the installer names one of
vq's scripts but the checkout does not prove it, or an `--install-script-arg
--extras` asks for less than the declaration -- comes before anything is
built. It leaves the checkout in place, and a provision refuses an occupied
`git_dir`, so the refusal ends by naming the machine the provision ran on and
saying to empty `git_dir` there before any way out it names. "Empty" because
`git_dir` may be a mount point created for the provision, which it accepts as
long as it is empty. It clears its own admin-update marker rather than leaving
it failed, and says so. Earlier releases left that marker, and no one command
to clear it was right on every route: a forwarded install runs on the far side
as `localhost`, a scheduler cluster's install runs on the cluster while
`clear-update-marker CLUSTER` goes to its `scheduler_driver`, and
`clear-update-marker --yes` requires an unambiguous marker in that host scope. A
checkout that is not a vq tree at all is told that no route applies the
declaration there.

The refusal, not a notice, is the point: a venv provisioned without the runtime
it declares is what the key exists to prevent, and it is exactly what a notice
that nobody reads leaves behind -- a console unit failing on every start with
"vq web requires the 'web' extra" until some later managed update converges it.
When the `install_script` does not even name one of vq's scripts, the refusal
comes **before the clone**, so the fix is a config edit with nothing to clean
up first.

### A program with no `update_script` has one route, not two

The second way out above -- provision without the declaration, then set the
profile -- is `vq admin update`, and `vq admin update` runs the program's
`update_script` and nothing else. A program that configures only
`install_script` therefore has no route to rebuild the environment it already
has: the update pulls the checkout, reports success and rebuilds nothing. That
shape is loadable (`extras` asks for `install_script` **or** `update_script`,
because a provision really does apply the floor once), so vq words the refusal
for it differently -- pointing `install_script` at the checkout's own installer
is the only route it has, and adding `update_script = "scripts/update.sh"` is
what makes the second one real.

### An ordinary update applies it on the same proof

`vq admin update ENV` applies the declaration too, wherever the configured
`update_script` resolves to the checkout's own `scripts/install.sh` or
`scripts/update.sh`. That was the one build vq performs that ignored the key:
a program declaring `extras = ["web"]` that was not the daemon-serving env
could be rebuilt without uvicorn by the very verb an operator reaches for to
fix that, and the asymmetry had no reason an operator could predict from the
outside.

The profile is resolved over whatever that run was going to install anyway --
an operator's `--update-script-arg --extras PROFILE`, else the profile
`.vq-install-metadata` records, else `core` -- and appended last, so it can
only add capability. It asks for no `--recreate-venv`: the reinstall goes into
the environment that is already there, which is what puts the declared runtime
in it. A request that does not cover the declaration is refused in the same
words the install uses, and a newly raised floor is not satisfied by
`--expected-sha` idempotence: a checkout at the target commit says nothing
about what its venv carries, which is exactly the state a console host can sit
in for days.

The managed daemon transaction is unchanged -- what it accepts, what it plans
and what it refuses are all as before.

The same asymmetry decides what an ordinary `vq admin update` says about a
declaration it is *not* applying. With an `update_script` that is the
program's own script it names what it found and the config change that would
give the program a route; without an `update_script` at all it says that this
update rebuilds nothing and that only the provision ever applied the
declaration. Neither note offers `vq admin install` as a way out: that
environment's checkout exists, and a provision refuses to build over one.
Neither offers the managed transaction either, which requires the same
canonical script and refuses without it.

`vq web install` and `vq web status` word their runtime remedy on the same
rule. A console pointed at a managed venv that cannot import uvicorn is told to
declare `extras` and rebuild; when the program configures no `update_script`,
it is told to add one first, because `vq admin update PROGRAM` on that program
would exit 0 and leave the unit failing on every start.

A program that declares no `extras` is installed and updated exactly as it
always was, which is every vibe-qc and vibe-view program: they are provisioned
and rebuilt by their own scripts and declare nothing.

An operator's `--install-script-arg --extras PROFILE` sets the base, as the
per-update request does for an update; the declaration is the floor under it.
A request that does not cover the declaration is refused rather than silently
widened -- two instructions contradicting each other, named rather than
reconciled.

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

## Fetches fail after the state root moved — `vq admin repair-state-paths`

A spec retains the absolute `cwd`, `workdir` and `archive_path` recorded when
its job ran. Copying state does not repoint those fields, so a preserved
workspace under a new root can remain unreachable to fetch. This differs from
an unreadable member inside a workspace: per-entry skipping cannot locate an
absent workspace root.

Use the [state-path repair guide](state_path_repair.md) for a read-only plan,
field and residue counts, the current-referent preservation contract and the
exact-plan apply procedure. The tool requires explicit roots and preserves raw
originals before any record replacement. An absent old root is reported as
unproven and blocks application; matching job IDs alone do not establish
artifact correspondence. A compatibility alias must be separately approved and
created by the operator. The repair command never changes host configuration.

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
  and 8) + idle CPU/IO priority everywhere a build is invoked. (vq's
  own injected level was removed in #133; the product's build helper
  now sizes parallelism from available memory.)

Two lessons that aren't already encoded in code:

1. Never verify a "this host wedges during builds" fix by
   triggering the same build on that host. Use compute-a or a fresh
   worktree.
2. `systemctl --user restart` waits politely for SIGTERM ack. On
   a zombie manager that wait never ends. Go straight to
   `systemctl kill --signal=SIGKILL` if the manager is sick.


### Selecting a failed runtime update marker

`vq admin clear-update-marker HOST` includes local `runtime-slot:<env>`
markers as well as ordinary environment markers. If more than one marker
matches the requested host scope, it refuses to guess. Inspect
`vq admin status HOST --json`, then pass the exact `marker_id` with
`vq admin clear-update-marker HOST --marker-id ID --yes`.
The selector cannot cross into another scheduler host's scope. Live updater
and durable transaction recovery checks still apply; selecting an ID does not
bypass them.
