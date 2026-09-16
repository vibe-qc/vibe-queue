# vq daemon lifecycle contract

What vq requires of the host environment, what vq detects + refuses to
operate when, and what to do when the contract breaks. The 2026-05-17
compute-d incident (user-systemd orphaned, daemon gone, requiring
out-of-band sudo recovery) motivated this document; v0.5.49 added the
`vq daemon health` verb that surfaces every contract violation in one
verdict.

For the newcomer-facing command matrix shared by vibe-qc, vibe-view, vq, and
vibe-basis, start with the
[toolset lifecycle guide](https://vibe-qc.com/docs/toolset_lifecycle.html). This page covers
the additional daemon ownership and host-service contract specific to vq.

## Source-install lifecycle

First [clone vibe-queue](installation.md#clone). Run the supported
source-install commands from that repository root:

```sh
./scripts/install.sh --extras web
.venv/bin/vq self-update --accepted-report vX.Y.Z
./scripts/reinstall.sh   # only while no daemon runs
./scripts/uninstall.sh --dry-run
```

Each command uses the dedicated `.venv` unless `--venv` is
given. Install records the selected extras profile and whether the package
is editable; update and reinstall preserve both unless `--extras`,
`--editable`, or `--copied` explicitly changes them. Environment replacement
uses a rollback-safe transaction, and every successful install restamps source
provenance.

Every verified environment also receives a regular
`.vq-checkout-owner` file bound to the canonical `vibe-queue` checkout.
Install with `--force`, in-place update, environment replacement, and removal
all require that exact ownership marker. The scripts recheck it while holding
the checkout lifecycle lock immediately before a destructive operation.
Foreign, malformed, or symlinked markers fail closed, as do targets below
system directories or Git metadata.

An older vq environment without the marker is not adopted automatically. Pass
`--adopt-legacy` explicitly (`install.sh` also requires `--force`) for a
one-time migration. Adoption uses an isolated Python outside the target to
read the environment's regular PEP 610 `direct_url.json`; it succeeds only
when the recorded local source path exactly matches this checkout. The target
environment's Python and `vq` commands are never executed to establish trust.
After that proof, a non-dry-run command commits the one-time adoption by
writing the marker while it holds the lifecycle lock, before any daemon or
environment mutation. That marker remains valid even if a later precondition
or install step fails. `--dry-run` performs the proof but does not stamp the
target.

When the selected environment owns a running daemon, direct install, update,
and reinstall refuse. Use `vq self-update` or `vq admin update`: the managed
transaction binds Linux systemd-user MainPID/ExecStart or macOS launchd argv,
durably owns checkout and venv rollback, pauses the relevant queue scope, and
proves the restarted daemon's exact source and package tree. A daemon belonging
to another venv is left alone.

A managed transaction takes no `--extras` from `update_script`: what goes into
a serving venv is decided by vq, not by a command line it did not write. It
rebuilds with the profile `.vq-install-metadata` records, changed for one
environment by an explicit `--update-script-arg --recreate-venv
--update-script-arg --extras --update-script-arg PROFILE`, and raised on every
rebuild by whatever the program declares:

```toml
[programs.vibeqc-queue]
extras = ["web"]
```

The declaration is applied by the managed transaction, which is the one path
that has proved the updater is vq's own `scripts/update.sh`; an ordinary
`vq admin update` on some other program runs that program's own script and
says that it left the declaration alone rather than passing over it silently.

Use the per-update flags to fix a host now; use the declaration to say what
the venv is *for*. The declaration survives a venv rebuilt outside the managed
path — a fresh provision, `install.sh`, `reinstall.sh` — where the metadata
sidecar dies with the venv it described, and an operator request that does not
cover it is refused rather than silently widened.

This is how a host whose one venv serves both the daemon and the web console
keeps uvicorn across a rebuild — the only supported way, since pip inside a
vq-managed checkout is not one. The declaration only ever adds: the effective
profile is the smallest published one containing both the declaration and what
the venv already had, so `["web"]` on a `dev` environment rebuilds as `all`
rather than dropping its test tooling. `vq web install` makes the same check
from the other side and refuses to point a unit at a vq that cannot import
uvicorn. Add the key only after the host runs a vq that knows it; an older one
rejects the unknown key and then fails to load the config at all.

Direct whole-venv replacement is supported only for a provably inactive
environment and a source-stable checkout (`update.sh --skip-git
--recreate-venv`, `reinstall.sh`, or `install.sh --force`). Before moving the
old venv it fsyncs an owner-only receipt beside the target. A later invocation
under the same checkout+target lifecycle locks reconciles an interrupted
candidate or cleanup debt, performs no service action, and exits so the
operator must retry explicitly. Direct `--restart-daemon` is retained only as
a fail-closed compatibility spelling.

Update and reinstall require an unsupervised daemon to be stopped manually;
they will not promise a restart they cannot perform. `vq daemon start` was
removed because it created a second daemon identity. Install systemd-user or
launchd-user supervision first.

A separately supervised dashboard created by `vq web install` has its own
service definition and process. After update or reinstall, rerun
`.venv/bin/vq web install` and verify `vq web status` so the service
points at the current environment and provenance. Before uninstalling the
environment, run `vq web uninstall` while that command still exists. The macOS
daemon plist generated with `vq daemon launchd-plist` already owns a web
sidecar; do not install a second dashboard service for that route.

## What vq requires

### Mandatory

1. **A user service manager active and reachable.**
   Linux uses systemd-user. `systemctl --user` must work;
   `loginctl show-user $USER` must
   report `State=active`. With `Linger=yes` enabled
   (`loginctl enable-linger $USER`, once per host) the user manager
   auto-starts at boot and survives logout — required for the daemon
   to run unattended on a compute host.

   macOS uses the generated `com.vq.daemon` launchd agent. Create it with
   `vq daemon launchd-plist`, bootstrap it with `launchctl`, and keep it at
   `~/Library/LaunchAgents/com.vq.daemon.plist` so lifecycle scripts can
   unload and restore the same definition around an update.

2. **The platform daemon service installed and enabled.**
   Either copy `contrib/vq-daemon.service` into
   `~/.config/systemd/user/` or use a distro packaging path. Then:
   `systemctl --user enable vq-daemon` to make it auto-start. The
   unit's `Restart=on-failure` + `RestartSec=5` means a crash
   recovers automatically.

   **v0.6.0 removal**: `vq daemon start` was removed. The
   systemd-user unit is the supported Linux daemon launch path; a launchd
   user agent is the supported macOS path.
   Pre-v0.6.0 the CLI verb could spawn a detached daemon that
   wrote its own pidfile (a second identity source competing with
   systemd's MainPID); removing the verb closes that whole class
   of failure mode. The `daemon_control.start_daemon` Python API
   is retained for test purposes (used by the e2e test suite) but
   shouldn't be called from production.

3. **An on-disk vq venv** that the service definition's executable
   points at. Example Linux layout for a standalone clone:
   `/home/USER/vibe-queue/.venv/bin/vq`. The
   `prog.python` field of the `[programs.vibeqc-queue]` config
   entry (if registered) must point at the same venv's `bin/python`
   so the v0.5.42 self-update auto-restart detection works.

### Strongly recommended

* **cgroup-v2 delegation** for accurate per-job memory enforcement
  and CPU-starvation detection. Drop-in at
  `/etc/systemd/system/user@.service.d/delegate.conf`:

      [Service]
      Delegate=cpu cpuset io memory pids

  Without delegation, the watchdog falls back to `/proc`-only mode
  (CPU-only enforcement, memory enforced by host-percent ceiling
  rather than per-job cap). The fleet hosts (compute-a, compute-d) have
  delegation enabled; the daemon's `cgroup.available()` probe
  detects + caches the answer at startup.

* **`~/bin` on the systemd-user PATH** if you dispatch CRYSTAL /
  ORCA / etc. from there. The unit ships generic; per-host PATH
  overrides go in
  `~/.config/systemd/user/vq-daemon.service.d/path-override.conf`.

### Forbidden (configurations that violate the contract)

* **Two daemons against the same `$VQ_STATE_DIR`.** Result is
  undefined — both will try to dispatch, lock files race, specs get
  overwritten. `vq daemon start` was **removed in v0.6.0** (it now
  hard-errors and points to the systemd-user unit), so the systemd
  MainPID is the single source of daemon identity. `vq daemon
  health` still reports a `FAIL` if a stray daemon-pidfile and the
  systemd MainPID disagree.

* **Editing files in the vq venv while its daemon is running.** A
  manual `pip install` while a job is dispatching can mid-stream
  swap modules out from under the running process. For a local serving source
  install, use `vq self-update --accepted-report vX.Y.Z`. For a managed remote
  profile, use `vq admin update vibeqc-queue`; both use the durable marker,
  pause, rollback, and verified restart transaction.

## What vq detects + refuses on

### `vq daemon health` cross-checks (v0.5.49+)

The verb cross-checks four sources of truth:

| source                                          | healthy value           |
|-------------------------------------------------|-------------------------|
| `loginctl show-user $USER` → `State`            | `active`                |
| `pgrep -u $UID -f 'systemd --user'`             | one live PID            |
| `systemctl --user show vq-daemon -p ActiveState`| `active`                |
| `<state_root>/daemon.pid` vs systemd `MainPID`  | identical OR no pidfile |

`ok=True` only when all four agree and the recorded daemon PID
is alive (verified via `kill(pid, 0)`). Each disagreement adds a
`FAIL` finding to the verdict. The verb is read-only —
diagnoses, never repairs.

**Hosts without systemd-user (macOS, non-systemd boxes).** macOS production
daemons should run under the generated launchd user agent, which the source
lifecycle scripts can identify and cycle. The health command itself still
uses the following platform-neutral fallback. When
none of the three systemd signals is present — no `loginctl`, no
`systemd --user` in the process table, and `systemctl --user`
unreachable — there is no user manager to contract with, so the
four-source check is skipped entirely. The verdict instead falls
back to **RPC-socket liveness**: the daemon (started directly, e.g.
`vq daemon run` from a wrapper) is `ok=True` iff it answers `ping`
on `<state_root>/daemon.sock` — the same signal `vq queue` /
`vq programs` / job dispatch rely on — with the pidfile pid carried
through for display. This is why a healthy macOS guarded node now
reports `daemon: OK ... (pidfile)` from `vq overview` / `vq daemon
health` instead of the old `daemon: FAIL (pidfile)` false-negative.
A Linux host always has at least a `loginctl` State + a manager PID,
so it keeps the full four-source contract; the orphan/zombie
incidents (manager present but `systemctl --user` unreachable) still
take the systemd path and `FAIL` correctly.

### `vq admin update`

* **Refuses conflicting existing markers.** The first current lease may use the
  legacy-compatible `<state_root>/admin-update-in-progress` path; additional
  disjoint leases live under `<state_root>/admin-update-markers/`. A stable lock
  serializes admission, and independently scoped leases may coexist only when
  they touch disjoint mutable resources. A
  conflicting update is rejected with a diagnosis-specific recovery recipe.
  The refusal is a
  plain runtime error (`Error: …`, exit 1) — **not** a CLI usage error
  (`Usage: …`, exit 2): the argv was valid, a prior run just left the
  marker. This matters across hosts — `vq admin update --all <host>`
  delegates `admin update --all localhost`, and a marker on the remote
  must read as a state condition, not as a malformed `--all` command.

  **v0.6.0 state machine, v0.25.0 durable receipt.** Each marker is a
  multi-phase state file (pre-v0.6.0 markers read as
  `state="legacy_in_progress"`). The ordinary phase sequence is:

      PAUSING -> PAUSED -> PULLING -> TAG_CHECKING -> BUILDING -> RESUMING ->
      RESTARTING_DAEMON -> VERIFYING -> (file removed = IDLE)
                                     \-> FAILED (sticky)

  Each transition rewrites the file atomically. On failure the
  state becomes `FAILED` (sticky) with a `failure_reason` field
  (e.g. `"git pull rc=128"`, `"daemon restart failed: systemctl
  --user is unreachable"`). `vq admin status` shows the current
  state + phase_started timestamp + failure_reason, so an operator
  sees "stuck at PULLING for 47m" or "FAILED: update_script rc=2"
  in one place instead of correlating logs.

  A serving-daemon update additionally embeds a failure-atomic managed
  transaction binding the exact old checkout, virtualenv, service identity,
  and pause token. A hard interruption leaves that receipt on disk and keeps
  only its dispatch scope held. The daemon never auto-reaps it, and neither
  `clear-update-marker` nor `update --force` may discard it. Run
  `vq admin recover-update [HOST]` (and `--marker-id ID` when diagnosis names
  more than one) to reconcile and verify the receipt's exact durable identity
  (old runtime, or an already committed target), prove the exact paused jobs
  resumed, and clear the receipt. An unverified target is never promoted by
  inference. A pause-only receipt uses `clear-update-marker`, which performs
  the resume proof before unlinking.
  If a direct host's installed vq predates a landed recovery-parser fix, the
  explicit `--with-driver-runtime --marker-id ID` form may stage and
  self-authenticate the current driver's package for one recovery invocation.
  It does not install that package and never retries an unknown mutation.
  Clear or force remains available only for an ordinary marker after the
  operator has independently inspected the environment and updater liveness.

* **Refuses to restart the daemon onto a half-installed package.**
  When the on-disk work (git pull, update_script) fails,
  `_maybe_restart_daemon` is skipped — restarting onto stale
  bytecode is better than restarting into a broken venv.

* **Surfaces the recovery recipe.** When the daemon-restart step
  fails AFTER the on-disk work succeeded (systemctl unreachable,
  timeout, non-zero rc), the marker stays on disk (v0.5.48 Bug A
  fix) and the failure message includes a pointer to
  `operations.md` for the user-systemd revival recipe.

### Daemon startup (`vq daemon run`)

* **PID-fingerprint check (v0.5.50).** RUNNING/SUSPENDED specs are
  re-attached as orphans only if the recorded `spec.pgid` is alive
  AND the recorded `spec.pid_start_time` matches the current
  `/proc/<spec.pid>/stat` field 22. Mismatch → spec lands in
  `ABORTED_BY_QUEUE` with reason `pid_recycled` instead of being
  silently re-attached to whatever unrelated process the kernel
  reused the PID for. macOS / pre-v0.5.50 specs skip the check
  (fall back to pgid-only liveness).

* **cgroup-scope MainPID cross-check (v0.6.0).** When `cgroup_enabled`,
  startup recovery also queries `systemctl --user show
  vq-job-<id>.scope -p MainPID --value` and compares to spec.pid.
  If different (or unit not found while systemctl IS reachable),
  the spec lands `ABORTED_BY_QUEUE` with reason
  `cgroup_scope_mismatch`. Closes the gap where pgid liveness +
  PID-fingerprint both pass but systemd no longer associates this
  PID with our scope unit (= the scope was detached / collected).

* **`_start_job` race fix (v0.6.0).** The dispatch path now writes
  `spec.state=RUNNING` with `pid=None`/`pgid=None` BEFORE the
  `subprocess.Popen` call. A daemon crash in the narrow window
  between Popen.success and the post-Popen `spec.write` would
  previously leave a PENDING spec next to a running process — the
  next daemon loop would re-dispatch it (DOUBLE DISPATCH). Now the
  recovery path sees `RUNNING + pgid=None` and marks
  `ABORTED_BY_QUEUE` via the existing "no pgid recorded" branch;
  the unowned process (if any) stays within its cgroup scope and
  the operator can `vq resubmit` cleanly.

* **cgroup availability re-probe (v0.5.50).** The
  `cgroup.available()` `lru_cache` is cleared at startup so a
  daemon restart re-tests delegation from scratch. Stops the audit
  § 2j failure mode where a cached `True` survived a user-systemd
  flip and every subsequent `_start_job` failed via `systemd-run`.

* **Refuses to dispatch under an active marker.** v0.5.45 added a
  marker check in `_dispatch_pending`. If the daemon restarts
  while an admin update is in flight (or after the update was
  killed mid-way), the new daemon sees the marker and parks the
  pending queue until the operator clears it.

## When the contract breaks

### Symptom matrix

| symptom                                              | first command to run            |
|------------------------------------------------------|---------------------------------|
| `systemctl --user` returns "Transport endpoint…"     | `vq daemon health <host>`       |
| `vq queue` works but a job won't dispatch            | `vq admin status <host>` (look for marker banner) |
| `vq admin update` exits FAILED, daemon stale         | `vq daemon health <host>` then `vq admin status` |
| `vq --version` reports new code but daemon is old    | `vq daemon health <host>` (compare PIDs)         |
| Job killed STARVED right after `vq resume`           | v0.5.48 Bug B fix; upgrade.     |

### Recovery recipes

#### user-systemd orphan / zombie

If `vq daemon health` reports
`FAIL: systemctl --user cannot reach the user manager`:

```sh
# From an interactive shell on the host (sudo required):
sudo systemctl kill --signal=SIGKILL user@$(id -u).service
sudo systemctl reset-failed user@$(id -u).service
sudo systemctl start user@$(id -u).service
sleep 3
vq daemon health        # should report verdict: OK
```

If the user-systemd process is alive but `systemctl --user` can't
reach it (the 2026-05-17 compute-d incident), the `kill` won't
actually kill anything but it transitions the unit state so
`reset-failed` + `start` succeeds. If the PID is truly orphaned
(not in any cgroup systemd manages), use the explicit
`sudo kill -9 <pid>` first, then the three-step recipe.

#### Stale admin-update marker

```sh
# Inspect and follow the printed diagnosis:
vq admin status <host> --verbose

# Durable serving-daemon receipt:
vq admin recover-update <host>

# Direct old-vq compatibility, with the exact diagnosed receipt:
vq admin recover-update <host> --marker-id <id> --with-driver-runtime --json

# Ordinary or pause-only marker after inspection:
vq admin clear-update-marker <host>
```

Stale ordinary markers may be auto-reaped when their writer is provably gone.
Durable managed-daemon and pause receipts are deliberately retained. Never use
`--force` against either receipt; the command refuses them because clearing the
file would abandon rollback state or paused jobs.

#### Daemon running stale code after manual `pip install`

```sh
# v0.5.42+: this is automatic via `vq admin update vibeqc-queue`.
# Manual fallback:
ssh <host> 'systemctl --user restart vq-daemon'
```

#### PID-recycle false-positive at startup (v0.5.50+)

If a spec lands in `ABORTED_BY_QUEUE` with reason `pid_recycled`
and you're confident the job genuinely finished cleanly (rather
than being killed early), check `<workspace>/_vq/exit-code` — the
v0.5.9 exit-marker may show the real rc. The recycle-detection is
conservative: false-positives are preferable to silently
re-attaching to someone else's process.

## See also

* `operations.md` — runtime symptom troubleshooting + recipes.
* `roadmap.md` v0.5.42 → v0.5.50 entries — the incremental history
  of how the contract evolved.
* `audit_2026-05-17` (chat record) — the structural critique that
  motivated v0.5.49 and v0.5.50.
