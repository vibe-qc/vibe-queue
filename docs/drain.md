# vq drain - temporary dispatch gate

`vq drain` lets you temporarily reduce or block new job dispatches
without killing running jobs. Think of it as a "pause the queue" button
with fine-grained control.

## Quick reference

```bash
# Full drain: no new jobs dispatched at all
vq drain

# Partial drain: allow only 1 concurrent job
vq drain --max-jobs 1

# Partial drain: allow only 8 CPU slots
vq drain --max-cpus 8

# Auto-release after 2 hours
vq drain --duration 7200

# Set a reason (recorded in drain state)
vq drain --reason "interactive work for 2 hours"

# Check current drain state
vq drain --status

# Release every legacy drain and every owner-scoped scheduler lease
vq drain --release

# Release only a full/global drain, keeping scheduler-host lanes
vq drain --release-full

# Multi-user mutation without putting the token in argv
printf '%s\n' "$VQ_TOKEN" | vq drain --max-jobs 1 --token-stdin
```

## How it works

Full and partial policy remains in `<state_root>/drain.json`. Independently
owned scheduler-target holds live in the versioned
`<state_root>/scheduler-drain-leases.json` sidecar. The daemon composes both
stores on every dispatch tick (default 1s), so either form takes effect within
one `poll_interval`.

Scheduler leases use a locked daemon-side transaction and atomic replacement.
Each claim has its own ID, host, owner, reason, and optional process identity,
so one updater can release its claim without removing another updater's or an
operator's overlapping hold. Legacy whole-object `drain.json` writers cannot
erase the sidecar.

Three modes:

| Command | Mode | Effect |
|---|---|---|
| `vq drain` | Full drain | No new jobs dispatched. Running jobs continue. |
| `vq drain --max-jobs N` | Partial (job cap) | New jobs limited to N concurrent. Running jobs continue. |
| `vq drain --max-cpus N` | Partial (CPU cap) | New jobs limited to N CPU slots. Running jobs continue. |
| `vq drain --max-jobs N --max-cpus M` | Partial (both) | Both caps apply simultaneously. |
| `vq drain --scheduler-host HOST` | Scheduler target | Hold qsub/sbatch for one scheduler target while other targets can dispatch. |
| `vq drain --release-full` | Full gate released | Clear only the full/global hold and preserve scheduler-target lanes. |
| `vq drain --release` | Released | Clear all legacy policy and every owner-scoped scheduler lease on this daemon. |

### Effective caps

Effective cap = `min(daemon.configured_cap, drain_override)`. For
example:

- Daemon configured with `--max-cpus 32`, drain set to `--max-cpus 8`
  → effective cap is 8.
- Daemon configured with `--max-jobs 2`, drain set to `--max-jobs 4`
  → effective cap is 2 (daemon's lower cap wins).

### Auto-release

Use `--duration SECONDS` for a bounded drain that auto-releases after
the specified time:

```bash
vq drain --max-jobs 1 --duration 7200 --reason "lunch break"
# → after 2 hours, drain is silently cleared
```

The auto-release timer starts when a legacy drain is set (`set_at` timestamp in
drain state). Expired drains are silently cleared on the next read
(drain, status, or dispatch tick). If the state also contains
scheduler-target lanes, expiry releases only the full/global gate and preserves
those scheduler lanes. Scheduler leases never inherit this timer;
`--drain-wait` is a deadline for waiting on quiescence, not a persisted lease
TTL.

### Scheduler-target handoff

Use `--scheduler-host HOST` when one scheduler backend is unsafe but the driver
daemon should still be able to dispatch unrelated scheduler targets:

```bash
vq drain --scheduler-host pbs-cluster --reason "PBS scheduler idle"
vq drain --release --scheduler-host pbs-cluster --lease-owner operator
```

For emergency maintenance where a full drain is already protecting the queue,
add the target lane before releasing the global gate:

```bash
vq drain --update-mode accept --duration 12h --reason "fleet stop"
vq drain --scheduler-host pbs-cluster --reason "PBS scheduler idle"
vq drain --status      # mode: full + scheduler-target (held: pbs-cluster)
vq drain --release-full
vq drain --status      # mode: scheduler-target (held: pbs-cluster)
```

This sequence prevents a dispatch window between the global stop and the
target-specific hold.

Ordinary manual holds use the owner key `operator`, so the owner-qualified
release above removes that hold without affecting automated work. A manual
scheduler-target release without an owner is an explicit operator override and
clears all claims for that target. Automated admin and fleet operations use
stable private owner keys and release only their own claims.

A bare `vq drain --release` is broader still: it clears every owner-scoped
scheduler lease on every target owned by that daemon, then clears legacy drain
policy. Use it only as a daemon-wide operator override. To release only a
legacy full/global gate while preserving scheduler-target claims, use
`vq drain --release-full`.

### Drain-and-wait for a scheduler-host update (v0.12.1)

You usually do **not** need to build this window by hand. `vq admin update
HOST --drain-wait DUR` does the whole sequence for you: it adds the lane, waits
for the target's already-submitted jobs to finish, runs the update, and then
releases only the lane it added.

```bash
vq admin update pbs-cluster --drain-wait 4h
```

Use the manual sequence above when you want the lane held across *several*
operations, or when you want it to outlive the update. `--drain-wait` acquires
its own independently owned lease, so a manual hold and multiple concurrent
updaters compose. Each updater releases only its own lease in `finally`; the
manual hold remains.

A caveat that surprises people: a lane drain holds **dispatch**, not
submission. New rows keep landing as PENDING during the wait, so the backlog
visibly grows. Add `--update-mode deny` (which sets `reject_submits`) if you
want submissions refused for the window too.

See `docs/operations.md` § "Updating a busy scheduler host" for the full
runbook.

### What drain does NOT affect

- **Already-running jobs** - they continue to completion.
- **The watchdog** - RSS / wall-time / starvation kills still fire.
- **`vq submit`** - submissions still land as PENDING specs. They just
  don't dispatch until drain releases or partial-drain caps allow.
- **`vq kill / pause / resume`** - these are job-level operations.

### Persistence

Drain state survives daemon restarts and host reboots (it's on disk).
Use `vq drain --release-full` to remove only a full/global legacy gate, or the
daemon-wide `vq drain --release` override to remove all legacy policy and every
owner-scoped scheduler lease.

### Authentication and mixed-version operation

Drain status is read-only and remains open. Mutations on a multi-user daemon
require the normal admin token. Use `VQ_TOKEN`, `--token-stdin`, or a mode-0600
`--token-file`; `--token` is supported but exposes the secret in shell history
and process listings. Remote and `--all` mutations forward a supplied token on
stdin, never in remote argv. A host's configured `admin_token_file` remains a
remote-host path.

A new client capability-probes the running daemon before changing scheduler
leases. If the daemon is old, unavailable, or returns a malformed protocol
response, the scoped mutation fails closed instead of writing the sidecar
directly. Deployment order is therefore readers and daemons first, then
writers. Do not downgrade a daemon while its scheduler lease sidecar is
nonempty: an older daemon does not know that sidecar and cannot enforce those
claims. Release the claims or restore the lease-aware daemon before resuming
dispatch.

## When to use drain

- **Need the box for interactive work.** Full drain blocks new jobs;
  partial drain (`--max-jobs 1`) lets a small test job through.
- **Temporary unavailability.** Use `--duration` for bounded windows
  (meetings, gaming, etc.).
- **Before `vq admin update`.** Admin update pauses the queue during
  the update, but drain provides a preemptive gate.
- **Disk pressure.** Reduce `--max-cpus` to let I/O-heavy jobs (crystal
  scratch writes) complete faster.

## When NOT to use drain

- **The queue is already empty.** Drain has no effect on an empty queue.
- **You want to stop running jobs.** Use `vq pause --all` or `vq kill`.
- **Long-term capacity planning.** For sustained load reduction, adjust
  the daemon's `--max-cpus` / `--max-jobs` startup flags.

## Status output

```
drain: ACTIVE since 2026-05-20T14:30:00+00:00 |
mode: full (no new dispatches) |
auto-release in 5400s |
reason: interactive work
```

`vq drain --status --json` includes the additive `scheduler_leases` list,
`legacy_scheduler_hosts`, and a per-lease `orphaned_scheduler_leases`
diagnostic. Text status also names an independently owned lease whose recorded
process is gone. Detection is informational and never auto-releases a hold.

## See also

- `vq throttle` - throttle per-job CPU priority (soft, doesn't block dispatch)
- `vq pause` - pause individual or all jobs
- `vq drain --status` - check current drain state
