# vq state-file location audit (current through v0.25.0)

> **What this is:** the authoritative catalogue of every file and
> directory vq reads or writes, with the precedence rules that
> resolve each one. Maintained as the contract surface for
> operators answering "where does my X live?" without grepping
> the source.
>
> **What this is not:** a proposal to change anything. The v0.7.1
> design doc flagged the user-XDG vs daemon-XDG split as a
> footgun (see § Known gotchas below); resolving it touches the
> cleanup sweep, per-user state, and daemon RPC in ways that
> need maintainer-approved design work. This document audits the
> *current* surface so a future ship can talk about deltas
> against a stable baseline.

## Quick reference

```
Single-user (laptop, dev workstation):
  ~/.config/vq/                       config_dir()       - config + tokens
  ~/.local/share/vq/                  state_root()       - everything else

Multi-user system deployment (daemon as root):
  /etc/vq/                            SYSTEM_CONFIG_PATH - system config
  /var/lib/vq/                        multi_user_root()  - daemon state
  /var/lib/vq/users/<uid>/            user_dir(uid)      - per-user state
```

The two roots are independent - moving one with its env var
doesn't move the other. That is the v0.7.1 footgun in one line.

## The XDG split

vq follows the [XDG Base Directory
spec](https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html)
strictly:

* **Config** (read at startup, rarely written) → ``XDG_CONFIG_HOME``
  (default ``~/.config``) + ``/vq``.
* **State / data** (written constantly during normal operation) →
  ``XDG_DATA_HOME`` (default ``~/.local/share``) + ``/vq``.

The split is correct per XDG and is what every modern Linux app
does. The footgun for operators is purely *cognitive*: the
operator types ``~/.config/vq/`` looking for ``client.log`` (it
isn't there - that's state) or ``~/.local/share/vq/`` looking
for ``config.toml`` (it isn't there - that's config).

## The two env-var overrides

| Env var | Overrides | Default | Affects |
|---------|-----------|---------|---------|
| ``VQ_CONFIG_DIR`` | ``config_dir()`` | ``$XDG_CONFIG_HOME/vq`` | `config.toml`, `web-token`, `web-users.json`, `web-session-secret` |
| ``VQ_STATE_DIR`` | ``state_root()`` | ``$XDG_DATA_HOME/vq`` | everything below the "State files" table |
| ``VQ_ARCHIVE_DIR`` | ``archive_dir()`` | ``$state_root/archive`` | only ``vq cleanup --archive`` tarballs |
| ``VQ_MULTI_USER_ROOT`` | ``multi_user_root()`` | ``/var/lib/vq`` | every multi-user daemon path |

Two important non-interactions:

1. **``VQ_STATE_DIR`` and ``VQ_CONFIG_DIR`` are independent.**
   Setting ``VQ_STATE_DIR=/tmp/vq-test`` for a test run does
   NOT relocate the config. The CLI will still read
   ``~/.config/vq/config.toml`` (the laptop's real config),
   which is almost always wrong for an isolated test
   environment. Tests in the suite set BOTH variables; ad-hoc
   operator invocations usually want both set too.

2. **``VQ_STATE_DIR`` is ignored in multi-user mode.** When the
   daemon runs as root with ``[multi_user] enabled = true``, the
   per-user state lives under ``/var/lib/vq/users/<uid>/``
   regardless of any env var the user sets. The user's local
   client uses ``VQ_STATE_DIR`` for its own ``client.log`` but
   resolves spec/workspace/workdir paths via the multi-user
   root.

## Config files

These live under ``config_dir()`` = ``~/.config/vq/`` by default. On a host
whose system config enables multi-user mode, ``web-token`` instead resolves to
``/etc/vq/web-token`` so clients and the root daemon share the same admin
credential; the account and session files remain under ``config_dir()``.

| File | Owner | Purpose |
|------|-------|---------|
| ``config.toml`` | operator | hosts, programs, multi-user, quotas, notifications, drain, throttle |
| ``web-token`` | operator (created by ``vq web init-token``; replaced with ``--force``) | shared bearer token for web mutations and multi-user admin verbs |
| ``web-users.json`` | operator via ``vq web user`` | fleet-console local-account scrypt hashes and roles |
| ``web-session-secret`` | vq web process | HMAC key for fleet-console session cookies |

The system-wide multi-user-mode config is at
``/etc/vq/config.toml`` (constant ``SYSTEM_CONFIG_PATH``, not
overridable). The single-user CLI consults it via
``system_multi_user_enabled()`` so users on a multi-user host
auto-detect mode without mirroring ``[multi_user]`` into their
personal config.

## State files (single-user)

All under ``state_root()`` = ``~/.local/share/vq/`` by default.

| Path | Function | Owner / who writes | Purpose |
|------|----------|---------------------|---------|
| ``queue/<jobid>.json`` | ``spec_path(jobid)`` | daemon (lifecycle), CLI (submit, kill, cleanup) | per-job spec |
| ``jobs/<jobid>/`` | ``workspace_dir(jobid)`` | daemon (creates at dispatch), user job process (writes), CLI (fetch reads) | submitted workspace |
| ``workdirs/<jobid>/`` | ``workdir_for(jobid)`` | daemon (creates at dispatch), user job process (writes), CLI (``fetch --workdir`` reads) | per-job scratch (v0.6.54) |
| ``archive/<jobid>.tar.bz2`` | ``archive_path(jobid)`` | ``vq cleanup --archive`` | workspace tarball |
| ``admin-status.json`` | ``admin_status_path()`` | ``vq admin update`` / ``mark-ok`` / ``reset-branch`` / ``auto-update`` | per-env last-update outcome |
| ``scheduler-runtime-status.json`` | ``scheduler_runtime_status_path()`` | ``vq admin update PROGRAM HOST`` | per-host, per-program runtime LAST OK + verified identity |
| ``admin-updates/<target>/<ts>.log`` | ``admin_update_logfile()`` | ``vq admin update`` (all four lanes) | v0.12.1 per-update transcript: phase narration, heartbeats, and the **full** build output. One directory per target, so a lookup or a retention sweep can only ever match that target. Pruned to the newest ``ADMIN_UPDATE_LOGS_TO_KEEP`` (20) by ``prune_admin_update_logs()``. Read with ``vq admin logs`` |
| ``admin-detached/run-<run-id>/`` | ``admin_detached.detached_run_dir()`` | a delegated ``vq admin update`` or ``vq admin auto-update`` launched with ``--detach`` | Launch intent, activation (pid + start-time fingerprint + transcript path) and the terminal receipt (outcome, exit code, exact stdout) of one detached update. The receipt is what lets a driver learn the outcome of a build it stopped watching: a successful update *removes* its marker, so the marker alone cannot distinguish "finished" from "never started". A launch also leaves ``child.log``, and a systemd-spawned launch holds the bearer token in an owner-only ``token`` file until the updater activates. Read with ``vq admin observe-update``; pruned to the newest ``DETACHED_RUNS_TO_KEEP`` (20), never pruning a run that is still live |
| ``client.log`` | ``setup_cli_logging`` arg | every ``vq`` CLI invocation | one INFO line per invocation; rotates at 10 MB × 3 |
| ``daemon.pid`` | ``daemon_pidfile()`` | daemon at startup | pidfile (single-user only - multi-user uses ``/var/lib/vq/daemon.pid``) |
| ``daemon.log`` | ``daemon_logfile()`` | daemon | daemon's own structured log. v0.12.1: rotates at 20 MB × 3 (previously an unrotated ``FileHandler`` that grew forever) and honours ``VQ_LOG_LEVEL`` (previously hardcoded INFO) |
| ``daemon-state.json`` | ``daemon_state_path()`` | daemon (lifecycle persistence) | running-jobs index, suspended-jobs ledger, retry state |
| ``daemon.sock`` | ``rpc.socket_path()`` | daemon (RPC server) | Unix socket for ``get_*``/``set_*`` RPC (admin-status, drain, throttle, config reload) |
| ``rpc-audit.jsonl`` | ``audit.audit_log_path()`` | daemon RPC server | append-only record of **mutating** (``set_*``) RPC calls; read with ``vq audit``. No rotation |
| ``drain.json`` | ``drain.drain_state_path()`` | ``vq drain`` | drain marker (suppresses dispatch) |
| ``.drain.lock`` | adjacent to ``drain.json`` | daemon and direct drain readers/writers | stable advisory lock for legacy-state expiry and read/modify/write transactions; existing root-owned 0644 locks remain readable by daemon-down status fallback |
| ``scheduler-drain-leases.json`` | ``drain.scheduler_drain_leases_path()`` | daemon RPC scheduler-lease transaction | versioned, independently owned scheduler-target dispatch holds; composed with legacy ``drain.json`` |
| ``.scheduler-drain-leases.lock`` | adjacent to the lease store | daemon RPC scheduler-lease transaction | stable advisory lock across atomic sidecar replacement |
| ``throttle.json`` | ``throttle.throttle_state_path()`` | ``vq throttle`` | throttle marker (caps active CPUs) |
| ``admin-update-markers/<marker-id>.json`` | ``admin.admin_update_marker_dir()`` | ``vq admin update`` / ``vq admin recover-update`` | Additional independently scoped update leases when another lease already occupies the compatible singleton path. A serving-daemon marker embeds the durable checkout/venv/service/pause recovery receipt and is retained after updater loss until explicit recovery. Scheduler-target markers live on the configured ``scheduler_driver``. |
| ``admin-update-marker.lock`` | internal admin marker lock | every marker admission, transition, clear, and recovery | Stable advisory lock that serializes receipt mutation and prevents conflicting scopes from both passing admission. |
| ``admin-update-in-progress`` | ``admin.admin_update_marker_path()`` | legacy-compatible marker reader/writer | Original singleton, extensionless marker path. The first current lease still uses it for compatibility; additional concurrent leases use ``admin-update-markers/``. Its parsed resource scope controls dispatch, with unreadable or unrecognized state holding everything fail-closed. |
| ``<workspace>/_vq/events.jsonl`` | in ``events.append_event`` | daemon, kill/pause/throttle paths | append-only per-job event log (lifecycle transitions, daemon-side notes). Lives **inside the job workspace**, not a top-level ``events/`` dir |
| ``<workspace>/_vq/samples.jsonl`` | in ``watchdog`` | daemon (per-tick resource samples) | watchdog input; retained with the workspace |
| ``<workspace>/_vq/resource-usage.json`` | local terminal collector or generated PBS/SLURM job script | direct or scheduler command wrapper | wall, user/system/active CPU, peak-RSS, optional process-count, source/aggregation, and command-exit receipt; retained and fetched with the workspace |

## State files (multi-user)

When the daemon runs as root with multi-user enabled, system-
level files move to ``multi_user_root()`` = ``/var/lib/vq/`` and
per-user files move under ``users/<uid>/``.

```
/var/lib/vq/
├── daemon.pid                     daemon_pidfile(multi_user=True)
├── daemon.log                     daemon_logfile(multi_user=True)
├── daemon-state.json              system-wide lifecycle persistence
├── admin-status.json              daemon's per-env update outcomes
├── scheduler-runtime-status.json  per-host/per-program runtime LAST OK
├── admin-update-in-progress       first/legacy-compatible scoped receipt
├── admin-update-markers/          additional scoped update receipts
├── admin-update-marker.lock       stable receipt-mutation lock
├── admin-updates/                 admin_update_log_dir(multi_user=True)
├── admin-detached/                detached_run_root(multi_user=True)
├── rpc-audit.jsonl                mutating-RPC audit trail
├── drain.json
├── .drain.lock
├── scheduler-drain-leases.json
├── .scheduler-drain-leases.lock
├── throttle.json
└── users/
    └── <uid>/                     user_dir(uid)
        ├── queue/                 user_queue_dir(uid)
        ├── jobs/                  user_jobs_dir(uid)
        ├── workdirs/              user_workdir_root(uid)
        └── archive/               user_archive_dir(uid)
```

Per-user dirs are owned by ``<uid>:<gid>`` (chown'd by
``provision_user_state`` at daemon startup for every admin-group
member). The root-owned ``/var/lib/vq/`` itself prevents
unprivileged users from materializing their own subtree.

## Known gotchas

### 1. `admin-status.json` lives in two places (the v0.7.1 footgun)

The split is real and explicit:

* **User-side** (``vq admin status``, ``vq admin mark-ok``,
  ``vq admin reset-branch``) writes to
  ``state_root()/admin-status.json`` - i.e.
  ``~/.local/share/vq/admin-status.json`` for the operator's
  XDG (or whatever ``$VQ_STATE_DIR`` points at).
* **Daemon-side** (``vq admin auto-update`` running under the
  daemon's cron / scheduled trigger) writes to the daemon's
  ``state_root()/admin-status.json`` - i.e. root's XDG
  (``/root/.local/share/vq/``) for a single-user daemon
  running under sudo, or
  ``/var/lib/vq/admin-status.json`` for the multi-user
  system daemon.

Consequence: ``vq admin status`` from the operator and
``vq admin status`` from the daemon's cron can show *different
last-update records* for the same env. Both are real - they
just describe different invocations.

Workaround until unification ships: when investigating a
divergence, point both at the same root explicitly:

```sh
sudo -u queue_operator VQ_STATE_DIR=/var/lib/vq vq admin status
sudo VQ_STATE_DIR=/var/lib/vq vq admin status
```

Both then read the same file. (This is the "fix it manually
when it bites" pattern the v0.7.1 doc preserved as known and
deferred.)

**Caveat - don't carry that override into `vq admin update`.**
`VQ_STATE_DIR=/var/lib/vq` moves *every* state-root lookup in
that process, including the socket the post-restart provenance
ping uses. The self-update lane restarts the **user** daemon
(`systemctl --user`), so pointing the state root at the
multi-user root makes the verification ping land on the *root*
daemon instead - a different process, with its own
`source_sha`. On compute-d (2026-08-02) that reported the root
daemon's `/opt/vq` commit and failed two restarts that had
actually succeeded. Since v0.24.x the verification resolves the
user daemon explicitly (`rpc.user_socket_path`) and refuses to
grade an envelope that reports `multi_user`, so the override no
longer misleads it - but scope the workaround to the `vq admin
status` invocation you are actually comparing rather than
exporting it for the session.

### 2. ``VQ_STATE_DIR`` doesn't move config

Setting ``VQ_STATE_DIR=/tmp/test`` for a quick run does NOT
isolate the config. ``vq`` will still read your real
``~/.config/vq/config.toml`` - including hosts, default_host,
multi_user toggle, etc. For an isolated environment, set BOTH:

```sh
VQ_STATE_DIR=/tmp/test VQ_CONFIG_DIR=/tmp/test/cfg vq queue
```

Tests pin both via the ``cli_state`` fixture pattern.

### 3. ``daemon.pid`` location varies by mode

Single-user: ``state_root()/daemon.pid`` (under XDG).
Multi-user: ``multi_user_root()/daemon.pid`` (under
``/var/lib/vq/``). A multi-user user trying to find the
daemon pidfile in their XDG dir will not find it; the systemd
unit owns the canonical location.

### 4. ``client.log`` is per-CLI-invocation, not per-daemon

``setup_cli_logging`` writes to
``state_root()/client.log`` from every ``vq`` invocation. In
multi-user mode this means the user's XDG ``client.log`` (NOT
``/var/lib/vq/client.log``) - useful when investigating "why
did MY ``vq submit`` hang?" but easily confused with the
daemon's ``daemon.log`` which lives at the multi-user root.

### 5. ``$VQ_ARCHIVE_DIR`` is a deep override

The env var is consulted inside ``archive_dir()`` and per-
policy via ``AutoCleanupPolicy.archive_dir`` (which trumps the
env var). Changing it affects: manual ``vq cleanup --archive``,
auto-cleanup sweeps, archive-aware ``vq fetch``, and the
``vq queue (archived)`` annotation - all consistently pick up
the same location. The override exists because the most common
archive sizes (per-job ~10-100 MB) can swamp small ``~``
partitions on a host with a big secondary disk.

## What would unification look like?

Out of scope for this doc - left for a future maintainer-
approved design ship. The first decision is the *direction*:

* **Unified XDG**: everything under ``~/.local/share/vq/`` with
  a ``config/`` subdirectory. Wins on "one root to find" but
  breaks XDG.
* **Unified config-style**: everything under ``~/.config/vq/``
  with a ``data/`` subdirectory. Wins on "config dir is where
  to look" but XDG forbids putting state under config.
* **Symlinks**: keep both XDG roots but symlink hot files
  (``admin-status.json``, ``client.log``) into the config dir
  for convenience. No XDG break; no semantic change for callers.
* **Daemon-side RPC**: ``vq admin status`` always proxies
  through the daemon (which holds the canonical
  ``admin-status.json`` regardless of who invokes it).
  Closes the §1 footgun cleanly but adds a daemon-socket
  dependency to a verb that's currently read-only against the
  filesystem.

The right call depends on operator preferences and
multi-user/single-user trade-offs that haven't been
discussed yet. v0.7.12 documents the surface; a future
v0.7.x or v0.8.x ship picks the direction.

## Maintenance

This document tracks the **actual** path resolution in
``vq/paths.py``. When you add or relocate a state file:

1. Add the helper function in ``paths.py``.
2. Add a row to the appropriate § State files table here.
3. If the new file is hot-read by an operator-facing verb,
   add a § Known gotchas entry if its location differs from
   what the operator would naively guess.

A future test could pin every path returned by ``paths.py``
against this doc's tables; not built today, low priority.
