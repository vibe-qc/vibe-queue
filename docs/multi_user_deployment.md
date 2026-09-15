# Multi-user deployment runbook

Multi-user mode lets several people share one host's vq queue with an
OS-account execution boundary: each user's local jobs run **as that
user**, each user has their own state directory and resource quota,
and only a job's owner (or an admin) can kill or fetch it through vq.
It is not a hostile-tenant sandbox or a cross-user confidentiality
guarantee; see the security limits below.

This is a **root-daemon** deployment. Read the whole page before
starting -- particularly the security section.

---

## How it works

* The vq daemon runs as **root** (system-level systemd unit). It is a
  scheduler, not a worker -- it never runs job code itself.
* Each job is dropped to its **submitter's uid/gid** before execution
  via `systemd-run --scope --uid=<uid> --gid=<gid>`. The kernel
  enforces that it does not run as root or another uid; access to files
  then follows ordinary Unix owner, group, and mode permissions.
* Per-user state lives under `/var/lib/vq/users/<uid>/`. The
  structural `<uid>` directory is `root:<primary-gid>` mode `0750`;
  the user's writable `queue/`, `jobs/`, `archive/`, and `workdirs/`
  children stay owned by that uid/gid. Provisioning retains existing
  read/execute bits but removes group/world write bits from those children.
* `[quotas]` caps each user's pending-job count and concurrent CPUs.
* `kill` / `fetch` check the caller's uid against the job's
  `submitter`; members of the `admin_group` (and root) bypass the
  check.
* Admin verbs (`vq admin update`) require a bearer token
  (`--token` / `$VQ_TOKEN`).
* The **CLI auto-detects multi-user mode** (since v0.6.30): `vq
  submit` / `queue` / `status` read this host's
  `/etc/vq/config.toml` and follow its `[multi_user]` setting, so
  every user's client and the daemon agree on where job state lives
  -- no per-user config edit needed.

If `systemd-run` is not on PATH, the daemon **refuses to start** in
multi-user mode -- there would be no way to drop privileges, and
running jobs as root is not an acceptable fallback.

The submitted command is not launched merely because `systemd-run` returned a
process. The trusted receipt collector runs first and verifies that its own
cgroup-v2 path is the exact named `vq-job-<id>.scope`. If it still belongs to
`vq-daemon-multi-user.service`, or the membership cannot be read, the collector
writes a `command_status=not_run` error receipt and exits 125 without forking
the payload. This check does not treat `VQ_MEM_MB` as permission to bypass the
daemon service's or any other kernel-enforced memory limit.

Root execution also requires the complete daemon configuration to validate and
to set `[multi_user] enabled = true` explicitly. A missing section,
`enabled = false`, or malformed configuration makes `vq daemon run` refuse
before it configures client or daemon logging, writes a pidfile, starts a web
sidecar, or constructs the daemon. Running the root-owned system unit with
single-user policy is never a supported fallback: it would bypass the uid/gid
drop and run submitted payloads as root.

`vq admin provision HOST --check` probes the canonical root unit independently
of that flag. An active `vq-daemon-multi-user.service` beside readable
single-user policy is reported as an unsafe contradiction. If the target
configuration cannot be read or parsed, applicability stays unknown rather
than inheriting the driver's unrelated policy.

---

## Security: run vq from a root-owned install

The daemon executes as root. If it ran `vq` from a user-writable
location (a developer's `~/…/vibe-queue` checkout), that user could
edit the code root executes -- a trivial privilege escalation.

Bootstrap and update only through the root-owned transaction helper. The full
accepted release SHA is mandatory:

```sh
sudo /opt/vq/bin/vq-multi-user-refresh --checkout /path/to/vibeqc-queue \
  --expected-sha <accepted-40-hex-sha>
```

`contrib/deploy-multi-user.sh --expected-sha <accepted-40-hex-sha>` installs
the helper and the shared lifecycle-lock code root-owned, then uses the same
helper for first activation. There is no supported direct `python -m venv`,
`pip install`, editable install, or hand-written marker fallback.

An existing host whose installed helper predates this transaction first
upgrades only the sealed root-owned refresh surface, then runs the required
read-only check before activation:

```sh
bash vibe-queue/contrib/deploy-multi-user.sh \
  --expected-sha <accepted-40-hex-sha> --prepare-only
sudo /opt/vq/bin/vq-multi-user-refresh --checkout /path/to/vibeqc-queue \
  --expected-sha <accepted-40-hex-sha> --dry-run
sudo /opt/vq/bin/vq-multi-user-refresh --checkout /path/to/vibeqc-queue \
  --expected-sha <accepted-40-hex-sha>
```

`--prepare-only` does not touch vq config, queue state, either vq daemon, or
`/opt/vq/venv`. It does disable the retired unsafe root auto-update timer,
remove its passwordless fragment, and install the accepted helper, lifecycle
helper, exact unit, and fail-closed timer tombstones.

The helper acquires the exact checkout and `/opt/vq/venv` lifecycle locks. Git
observation and `git archive` run as the sudo invoker with optional locks,
replacement objects, repository hooks, and ambient Git configuration disabled.
Root builds a wheel only from that explicit commit's sealed root-owned archive.
It also refuses a dirty checkout and any accepted SHA that is not a descendant
of the installed source SHA.

The accepted commit also carries
`contrib/vq-multi-user-runtime-requirements.txt`. The privileged path supports
CPython 3.12 through 3.14 on glibc x86_64: dependency wheels are downloaded as
the non-root invoker with `--require-hashes --only-binary`, then root
independently verifies their exact name, version, SHA-256, wheel metadata, and
absence of symlinks before sealing them. The vq wheel itself is assembled from
the sealed package with the trusted Python standard library, so no PEP 517
backend from the checkout or network runs as root. An unsupported ABI or a
missing/mismatched artifact fails before service quiescence.

The wheel is built while the old daemon remains intact. Before any destructive
mutation, the helper fsyncs a root-only receipt under
`/opt/vq/lifecycle/`. It then stops and proves the exact systemd unit is
quiescent, moves the old venv to a same-filesystem backup, creates the new venv
directly at `/opt/vq/venv`, installs only from the sealed wheelhouse, and
requires its package digest to equal the accepted commit's `src/vq` digest.
Only then does it write and read back the marker and recursively fsync the new
runtime plus `/opt/vq` before it can commit or delete the backup. After start,
verbose RPC must bind the SHA and tree to a root process whose PID equals
systemd `MainPID` and whose executable and argv are exactly the installed unit.
Any failure before the durable commit restores and re-proves the old exact
runtime; a later invocation recovers an interrupted receipt before considering
a new refresh. Recovery accepts only the exact receipt schema, phase, field
types, transaction-derived backup path, and coherent old SHA/tree pair. A
malformed receipt or symlinked transaction path stops recovery before systemd
or either runtime is touched.

`--dry-run` never performs receipt recovery. If an interrupted root receipt
exists, it reports the exact path and stops without moving a venv or touching
systemd. Re-run the normal helper with the same accepted SHA to execute its
locked, idempotent recovery, then repeat the dry-run if desired.

The former passwordless `vq-admins` sudoers rule is retired. It let a group
member choose source code whose build backend root would execute, which is a
root grant regardless of the helper's absolute path. Deployment removes the
old fragment. Use ordinary authenticated `sudo` only after accepting the exact
fleet release report.

**Run v0.6.35 or newer for the original spec gate.** Each user's
`queue/` directory is writable by that user (they need to submit jobs
into it), so a spec file there is untrusted input. v0.6.35 began
binding the job to the uid of the state directory it was read from
and checking declared workspace and log paths before dispatch.
Earlier multi-user builds trusted the spec's `submitter` / `cwd`
fields directly, which let a user run a job as root by hand-editing
their own spec. Treat any host on a pre-v0.6.35 multi-user daemon as
needing an immediate upgrade.

**Current source adds the structural and identifier hardening.** It
requires a safe inner job id that exactly matches the queue filename,
checks declared paths against the user's real `jobs/` directory, owns
the structural `users/<uid>` directory, removes group/world write from
managed children, and refuses a managed root or child that is a symlink
or non-directory. On startup it auto-hardens
existing numeric user trees; an unsafe tree stops daemon startup
instead of being scanned as root. Do not infer this expanded contract
from the v0.6.35 version floor alone; verify the installed source
provenance with `vq daemon ping`.

These checks are a bounded privilege boundary, not a claim of
race-free pathname confinement. Local dispatch validates and then
uses ordinary pathnames for workspace creation, log opening, and
recursive `chown`; a hostile user racing those operations is outside
the supported threat model. Existing child read/execute bits are retained
during provisioning while group/world write bits are removed; a shared
primary group may still expose state bytes. Sites needing confidential or
adversarial multi-tenancy
must enforce private groups and modes plus containers, VMs, or an
equivalent site isolation policy.

The startup migration is also pathname-based. It relies on a validated,
daemon-owned state/control root whose documented `root:vq-admins` mode `2775`
makes that admin group trusted, a daemon-owned `users/` root with no
group/world write, and trusted operator-selected ancestor directories. It is
not descriptor-relative, race-free traversal of an arbitrary filesystem tree.

---

## Prerequisites

* Linux with systemd; `systemd-run` on PATH.
* root / sudo on the host.
* For migration, the loaded single-user unit must be the supported canonical
  unit with no drop-ins, service hooks, environment overrides, or extra
  `ExecStart` arguments: its command is exactly
  `<checkout>/vibe-queue/.venv/bin/vq daemon run`. Move legacy `--max-cpus`,
  `--max-jobs`, `--max-mem-mb`, and `--default-job-mem-mb` arguments into the
  `[daemon]` section of that user's config first; run a web sidecar as its
  separate unit. The user systemd manager must retain the passwd home and must
  not set `VQ_CONFIG_DIR`, `VQ_STATE_DIR`, `XDG_CONFIG_HOME`, or
  `XDG_DATA_HOME`. The deploy script proves this contract before it creates
  the admission drain, so an alternate state root fails without fencing or
  stopping either daemon.
* An admin group, e.g. `vq-admins`:
  ```sh
  sudo groupadd vq-admins          # harmless if it already exists
  sudo usermod -aG vq-admins ALICE # add each operator
  ```

---

## Install

```sh
cd /path/to/vibeqc-queue
git rev-parse HEAD                         # must equal the accepted report SHA
bash vibe-queue/contrib/deploy-multi-user.sh \
  --expected-sha <accepted-40-hex-sha>

# Independent read-only checks after deployment:
sudo systemctl is-active vq-daemon-multi-user
sudo VQ_CONFIG_DIR=/etc/vq VQ_STATE_DIR=/var/lib/vq \
  /opt/vq/venv/bin/vq daemon ping --json localhost
vq admin provision <host> --check
```

Run the script as the ordinary checkout owner, never by putting the whole
script under sudo. It seals the explicit accepted commit before its bootstrap
sudo steps, creates config and state, installs the exact systemd unit plus
root-owned helper and lifecycle helper, retires any legacy root auto-update
timer and passwordless fragment, then delegates bootstrap to the transactional
helper. On an existing host, use `--prepare-only` followed by the helper's
documented dry-run and activation instead of asking deploy to activate directly.

When a single-user runtime exists, bootstrap does not rely on an early
empty-queue snapshot. It takes an exactly owned full drain that denies new
submissions, proves the old daemon observes it, waits for every nonterminal spec
to disappear, removes write permission from the exact existing queue directory
or atomically publishes a transaction-bound blocker when the queue was absent,
and rechecks zero before and after stopping the user unit. Root activation starts
only after `ActiveState=inactive` and `MainPID=0`. A normal pre-activation
failure restores the exact queue mode, restarts the old daemon only when it was
previously active, and releases only the drain with the recorded reason and
timestamp. Bootstrap requires the checkout's existing single-user `.venv` so
the user-owned drain and receipt implementation is itself the accepted code;
an empty fresh queue is fenced with the same transaction.

Every admission-migration intent and effect is durably journaled at
`<checkout>/.git/vq-multi-user-bootstrap.json` before the corresponding drain,
queue blocker/mode, service, config, or root-activation mutation. A separate
validated operation lock serializes one deploy or recovery from receipt adoption
through terminal proof and receipt removal. After SIGKILL or a reboot, run the
same deploy command with the same accepted SHA. It validates the exact schema,
uid, checkout, drain identity, queue/staged-blocker inodes, modes, and phase.
Pre-config phases roll the old user runtime back; config/root-intent and later
phases complete the exact accepted bootstrap forward. Recovery exits after
settling the interrupted transaction; explicitly run the command once more to
begin a new migration after a rollback. A replaced drain,
changed queue inode, changed prospective config, malformed receipt, or a retry
with another SHA fails closed and retains evidence for inspection.

Minimal `/etc/vq/config.toml`:

```toml
[multi_user]
enabled     = true
admin_group = "vq-admins"

[quotas]
default_max_pending_jobs    = 20
default_max_concurrent_cpus = 16

# Optional per-uid overrides:
# [quotas.per_user.1001]
# max_pending_jobs    = 50
# max_concurrent_cpus = 32
```

> **These quotas gate dispatch independently of the daemon's own `--max-cpus`
> and of the host's physical core count, and on a multi-user host the quota is
> usually the tightest of the three.** They can disagree: compute-d on 2026-07-27
> had 32 physical cores, `max_cpus = 32`, and
> `default_max_concurrent_cpus = 16`, so a pending 24-CPU job sat for hours
> against an idle queue. Nothing surfaced the 16, and the investigation went to
> daemon health before anyone found the quota.
>
> `vq overview --json` now reports `quota_max_concurrent_cpus` alongside
> `max_cpus` for exactly this case -- check it first when a job is pending on a
> host that looks idle. A raised quota needs no daemon restart, but a job
> already submitted above the cap keeps waiting: resubmit it at or below the
> limit, or raise the limit.

---

## Migrating from a single-user install

A single-user deployment keeps state under each user's
`~/.local/share/vq/` (`queue/`, `jobs/`, `archive/`). Multi-user mode
reads `/var/lib/vq/users/<uid>/`. The two layouts are not shared -- a
switch must migrate the old state, or that history disappears from
`vq queue`.

**Migrate per user. Do this with the single-user daemon stopped.**

The deploy script's owned deny drain and queue-directory fence close admission
while its root bootstrap runs; they do not copy terminal history. Perform the
copy below when that history must remain visible after the switch.

1. **Drain first.** Let pending jobs finish (or `vq kill` them) under
   the *old* single-user daemon, so migration only moves terminal-
   state history. Pre-multi-user specs store `submitter` as a
   username, not a uid; the multi-user daemon dispatches only numeric-
   uid specs, so a leftover PENDING old spec would fail to dispatch.
   Draining avoids that entirely.

2. **Stop the old daemon:**
   ```sh
   systemctl --user stop vq-daemon      # as the user
   ```

3. **Copy the state** into the per-uid tree (substitute the real uid,
   e.g. `id -u alice`):
   ```sh
   UID_N=$(id -u alice)
   GID_N=$(id -g alice)
   sudo mkdir -p /var/lib/vq/users/$UID_N
   sudo cp -a ~alice/.local/share/vq/queue   /var/lib/vq/users/$UID_N/
   sudo cp -a ~alice/.local/share/vq/jobs    /var/lib/vq/users/$UID_N/
   sudo cp -a ~alice/.local/share/vq/archive /var/lib/vq/users/$UID_N/ 2>/dev/null || true
   sudo chown -R "$UID_N:$GID_N" /var/lib/vq/users/$UID_N
   sudo chown root:"$GID_N" /var/lib/vq/users/$UID_N
   sudo chmod 0750 /var/lib/vq/users/$UID_N
   ```

4. **Disable the old user-level unit** so it cannot race the
   system daemon:
   ```sh
   systemctl --user disable vq-daemon   # as the user
   ```

5. Start (or restart) the multi-user daemon and confirm the migrated
   jobs appear:
   ```sh
   sudo systemctl restart vq-daemon-multi-user
   vq queue
   ```

If you do **not** need the old history, skip migration -- just leave
`~/.local/share/vq/` in place (untouched, harmless) and start fresh
under `/var/lib/vq/`.

---

## Adding users

Per-user state lives under `/var/lib/vq/users/<uid>/`. The
`users/<uid>` structural directory is root-owned, so an unprivileged
user cannot create or replace their own managed children. An
un-provisioned user's first `vq submit` fails with `PermissionError`.

* **Admins are automatic.** The daemon auto-provisions a state dir
  for every `admin_group` member at startup (v0.6.27). After adding
  a user to the admin group (`sudo usermod -aG vq-admins ALICE`),
  restart the daemon (`sudo systemctl restart vq-daemon-multi-user`)
  so the next startup picks them up.

* **Non-admin users -- one command.** Run once, as root, per user
  (v0.6.33):

  ```sh
  sudo /opt/vq/venv/bin/vq admin provision-user ALICE
  ```

  `ALICE` is a username or a numeric uid. It creates
  a root-owned `/var/lib/vq/users/<uid>` structural directory plus
  user-owned `{queue,jobs,archive,workdirs}` children; their `vq
  submit` works thereafter. Idempotent -- safe to re-run.

Each user also needs the **vq CLI** available (the client
auto-detects multi-user mode from `/etc/vq/config.toml` since
v0.6.30 -- no per-user config edit).

---

## Verifying the execution boundary

After deployment, confirm jobs actually run as their submitter:

```sh
# As an ordinary user — submit a job that records its identity:
vq submit -c 1 --wall-time-seconds 60 -- bash -c 'id > /tmp/vq-whoami.$$'
# Inspect the job's stdout / the file — uid must be the SUBMITTER's,
# never 0 (root).
```

Cross-user check: submit as user A, then as user B try
`vq kill <A's jobid>` -- it must fail with an ownership error.

---

## Rollback

To return to single-user operation:

```sh
sudo systemctl disable --now vq-daemon-multi-user
# Re-enable each user's own daemon:
systemctl --user enable --now vq-daemon   # as the user
```

Per-user state under `/var/lib/vq/` is left intact; the user-level
daemon reads `~/.local/share/vq/` again. If you migrated state in,
copy the relevant `queue/` + `jobs/` back, or accept that the
in-`/var/lib/vq` history is no longer visible.

---

## Environment tag probes and the retired root timer

The CLI verb `vq admin auto-update ENV` probes the environment according to
its configured policy. Tag mode selects the newest SemVer tag and refuses a
downgrade; branch mode compares with `origin/<branch>` and submits a capped
build job. Branch mode rejects the running vq daemon's own managed environment
before mutation. Repair that environment with `vq self-update` and an exact
full SHA or explicitly selected accepted report instead.

The old `vq-admin-auto-update@.service` root template and its timer are
retired. Even though `ExecStart` lived in `/opt/vq`, the command read each
environment's configured user-owned checkout and update script as root. That
made the timer a delayed root-code execution surface. The shipped files are
fail-closed tombstones, and `deploy-multi-user.sh` disables existing timer
instances. Do not enable or recreate them.

The CLI probe remains available for an authenticated operator and for
non-privileged environments. Privileged `/opt/vq` activation is instead tied
to an independently accepted release report and performed with
`vq-multi-user-refresh --expected-sha <full-sha>`.

**Auth note.** Every active venv `vq admin auto-update` shape enforces the
admin bearer token in multi-user mode -- the same gate as `vq admin update`.
The same `--token` / `--token-stdin` / `--token-file` / `$VQ_TOKEN`
precedence applies, and SSH delegation forwards via `--token-stdin` so the
bearer never lands on argv. Authentication does not make a root timer safe:
the retired unit's source and update-script trust boundary was the defect.
Manual invocations still need a token in multi-user mode.

`vq admin auto-update --scheduler-runtimes` is a retired, fail-closed
compatibility spelling. It performs no config, token, routing, ref, status, or
mutation work. Reconcile scheduler runtimes with `vq admin rollout-latest`;
that command consumes one accepted release report and binds every runtime lane
to the report's exact tag and SHA.

### Fleet-scale `--all` / `--all-hosts` (v0.6.49)

If you have several non-privileged envs to inspect (e.g. vibeqc-release,
crystal-stable, properties-stable), one explicit `--all` probe can cover them:

```sh
# Every venv env on this host:
vq admin auto-update --all --dry-run   (probe only; no apply)
vq admin auto-update --all             (probe + apply on drift)

# Every venv env on every host in [hosts.*]:
vq admin auto-update --all --all-hosts

# One env, every host:
vq admin auto-update vibeqc-release --all-hosts
```

`--all` iterates the registry in sorted-by-name order with per-env
failure isolation: one env's `git ls-remote` error or apply failure
doesn't abort the sweep. `--all-hosts` delegates sequentially with
per-host failure isolation. Exit code is non-zero if any env or
host hit an error or apply-failure; the operator still gets the
full per-env / per-host report. The SSH delegate carries the
v0.6.48 `--token-stdin` forwarding, so the bearer never lands on
argv on either side of the tunnel regardless of `--all` shape.

Do not schedule the command as root against user-owned checkouts. A read-only
non-root dry-run can still report drift for operator review.

---

## Known limitations

* **The multi-user daemon is not self-updating.** Updating vq means running the
  exact-SHA transactional helper after accepting the fleet release report.
* Multi-user mode requires `systemd-run`; there is no non-systemd
  fallback (by design -- the fallback would run jobs as root).
