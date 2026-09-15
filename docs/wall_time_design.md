# Wall-time enforcement: design notes and future options

**Audience:** the next vq dev chat that picks up wall-time
enforcement as a real architectural item (probably driven by
`vq admin update` in v0.6, or multi-host in v0.7+).

**Status as of v0.5.8 (2026-05-10):** wall-time enforcement lives
in the Python watchdog (`vq.watchdog`). Cgroup-level
`RuntimeMaxSec` was tried in v0.4 → v0.5.7 and dropped in v0.5.8.
This doc explains why the cgroup approach failed and what to
consider if/when wall-time becomes a hard architectural item again.

---

## TL;DR

* The watchdog owns wall-time. It already subtracts
  `paused_seconds_total` from elapsed, so pause/resume is
  naturally pause-aware.
* The previous design (cgroup `RuntimeMaxSec` for kernel-mediated
  wall-time + watchdog as belt-and-suspenders) cannot be fixed
  in-place: systemd refuses runtime mutation of
  time-based properties, and the timer is wall-clock, not
  active-time.
* If the daemon-down enforcement gap ever actually bites, three
  alternatives are sketched at the bottom of this doc. None are
  worth the complexity at vq's current scale (`--max-jobs=1`,
  laptop+compute-d, `Restart=on-failure` covers ≪1 s gaps).

---

## Timeline

| Version | Wall-time enforcement |
|---|---|
| v0.3 | Python watchdog only. /proc polling, SIGTERM → grace → SIGKILL on the pgid. |
| v0.4 | Cgroup `--property=RuntimeMaxSec=N` on the systemd-run scope. Watchdog flipped to `enforce_wall_time=False` (delegate to kernel). |
| v0.5.1 | Pause/resume added (SIGSTOP/SIGCONT). Watchdog learned to subtract `paused_seconds_total` from elapsed, but the watchdog wasn't *enforcing* wall-time when cgroup was on, so this was dormant. The bug was already shipped here, just hidden. |
| v0.5.7 | Tried to fix pause/resume by mutating `RuntimeMaxSec` at runtime via `systemctl --user set-property`. **Did not actually work.** Tests passed because they mocked `subprocess.run` as success. Smoke-test on compute-d caught it. |
| v0.5.8 | Drop `RuntimeMaxSec` from the scope entirely. Watchdog is the single owner of wall-time, regardless of cgroup state. |

---

## What v0.5.7 attempted, and why systemd refused it

The v0.5.7 plan:

```
on pause:  systemctl --user set-property vq-job-<id>.scope RuntimeMaxSec=infinity
on resume: systemctl --user set-property vq-job-<id>.scope \
               RuntimeMaxSec=<wall_time + paused_seconds_total>
```

systemd silently rejected every call:

```
Failed to set unit properties on vq-job-…scope:
Cannot set property RuntimeMaxUSec, or unknown property.
```

This is a documented systemd design choice, not a Manjaro/compute-d
quirk and not a version regression. From `systemctl(1)`:

> The set-property verb sets the specified unit properties at
> runtime where this is supported. … The properties that may be
> set are limited to those that may be modified at runtime, as
> listed in **systemd.resource-control(5)** …

`systemd.resource-control(5)` covers cgroup knobs:
`CPUQuota`, `CPUWeight`, `MemoryMax`, `MemoryHigh`,
`IOReadBandwidthMax`, `TasksMax`, … Time-based properties
(`RuntimeMaxSec`, `TimeoutStopSec`, `RuntimeRandomizedExtraSec`)
live in `systemd.exec(5)` / `systemd.service(5)` and are *not*
on the runtime-mutable list by design, because they're not
just stored values but armed timers tied to the unit's
`active_enter_timestamp`. Mutating them mid-active would mean
re-arming, which systemd's unit state machine doesn't expose.

There's no `--reset` flag, no privileged escape; the constraint
is in systemd's property-metadata table.

---

## Why v0.5.7 shipped despite testing

Two test types existed:

* `test_cgroup.TestSetRuntimeMaxSec`: six unit tests of the
  `set_runtime_max_sec()` helper.
* `test_pause_resume.TestPauseResumeRuntimeMaxSec`: three
  integration tests of pause/resume calling the helper.

All nine **mocked `subprocess.run` to return `returncode=0`**, so
the tests asserted "we built the right argv and called systemctl",
not "systemctl actually did the thing." The test file even
documented the mocking as deliberate (macOS dev boxes have no
delegation). The gap: no test exercised real systemd, and the
helper logged warnings but the CLI swallowed them.

**Generalised test-design lesson** worth carrying to other vq
features that touch systemd / cgroup: subprocess-success mocks
are fine for argv-shape contracts but cannot prove the syscall
is *accepted* by systemd. Anything that depends on systemd-side
semantics needs (a) a smoke test on a real Linux host, OR (b)
a documented "this is argv-shape only; semantics verified by
manual test" header on the test class.

---

## v0.5.8: what we ship now

Wall-time enforcement is entirely in
[`vq.watchdog.evaluate()`](../src/vq/watchdog.py). Relevant logic:

```python
elapsed_active = (now_mono - st.started_monotonic) - spec.paused_seconds_total
if elapsed_active >= spec.wall_time_seconds:
    return self._escalate(..., terminal_state=JobState.TIME_EXCEEDED, ...)
```

`paused_seconds_total` is updated by `pause_resume.resume_job()`
each cycle, so this naturally handles arbitrarily many
pause/resume rounds. SIGSTOP'd jobs accrue zero active time
(they're SUSPENDED; the watchdog skips kill paths for them
entirely).

**Trade-off:** lose kernel-mediated wall-time enforcement when
the daemon is down. Concretely: if vq-daemon dies between
sample iterations, no wall-time check fires until restart. The
gap is bounded by daemon-restart latency.

Mitigation today:

* `vq-daemon.service` has `Restart=on-failure`. Realistic gap on
  compute-d: ≪1 s.
* The v0.4 orphan-reattach reconciler picks up wall-time
  accounting on next daemon start (started_monotonic is
  recovered from `started_at` on the spec).
* `--max-jobs=1` makes the blast radius small: at most one
  in-flight job to over-run, and the host can absorb that.

For vq's current scale this is fine. The trade-off becomes
material when (a) `--max-jobs > 1` with strict per-host wall-time
SLAs, or (b) multi-host vq, where a single daemon-restart blip
across N hosts compounds.

---

## Alternatives for a future version

When/if the daemon-availability gap becomes a real issue, three
architectures are worth weighing. None of them are urgent.

### Option A: per-job systemd timer unit, recreated on pause/resume

Replace the in-scope `RuntimeMaxSec` with a *separate*
transient timer unit:

```
# Dispatch:
systemd-run --user --on-active=N --unit=vq-walltime-<id> -- \
    /bin/kill -TERM -<pgid>

# Pause:
systemctl --user stop vq-walltime-<id>.timer

# Resume:
systemd-run --user --on-active=<remaining> --unit=vq-walltime-<id> -- \
    /bin/kill -TERM -<pgid>
```

`systemd-run` timers can be created/torn-down at will, so the
property-mutability problem goes away. Each pause/resume cycle
tears down + re-creates the timer with the remaining active
budget.

**Pros:** kernel-mediated; survives daemon down-time; matches
v0.4's original goal.

**Cons:** doubles the per-job systemd unit count (scope + timer);
cleanup paths must handle "scope died but timer still ticking"
and vice versa; bookkeeping (remaining budget, kill-target pgid)
moves to spec persistence; the timer's
`/bin/kill -TERM -<pgid>` is more hostile than the watchdog's
graceful SIGTERM → grace → SIGKILL escalation.

**Effort:** ~150 LoC + unit lifecycle tests. Probably a v0.7
item if it lands at all.

### Option B: thin Python supervisor inside the scope

Wrap every dispatched command in a small Python parent that
forks the actual command, monitors a pause/resume signal file
(or pipe), tracks active wall-time, and SIGKILLs the child when
it overruns:

```
systemd-run --scope -- python -m vq.supervisor \
    --jobid <id> --wall-time-seconds N -- <real_command>
```

The supervisor talks to the daemon via the existing event-log
file; on pause it stops counting active time, on resume it
resumes.

**Pros:** zero systemd dependency for wall-time; works on macOS
dev boxes too (uniform code path); supervisor can do other
things later (per-step timing, intermediate output capture).

**Cons:** every dispatched command picks up an extra Python
process in the cgroup (~15 MB RSS); supervisor must be
installed in the dispatched venv (or a vendored stub vq ships);
breaks the "vq doesn't touch the user's runtime" invariant the
SPEC (`SPEC.md` §3.2) is proud of.

**Effort:** ~200 LoC supervisor + ~50 LoC daemon integration +
tests. Most invasive of the three; only worth it if other
in-scope-supervisor features come along too.

### Option C: stay watchdog-only, harden daemon availability

Accept v0.5.8's design. Close the daemon-down gap by:

* `Restart=always` (currently `on-failure`)
* Watchdog-of-watchdog: a tiny systemd-user health-check unit
  that runs `vq daemon ping localhost` every 5 s and restarts the
  daemon on miss. The HTTP `/health/live` route belongs to the optional
  web console and cannot establish daemon liveness.
* SPEC commitment: "wall-time enforcement is best-effort within
  daemon uptime; an over-run during a daemon-restart blip is
  not a bug"

**Pros:** zero new code; matches vq's "Python first, kernel
second" stance.

**Cons:** doesn't help for genuine daemon crashes that take
> 10 s to restart (e.g., disk full → daemon can't write spec
→ crash loop). Won't satisfy a hard SLA.

**Effort:** ~10 LoC of unit-file tweaks + a SPEC paragraph.

---

## Recommendation

For v0.6: **Option C.** vq's scale doesn't justify A or B yet,
and the v0.5.8 watchdog-only design is the right primitive for
the rest of v0.5.x and v0.6.

Revisit when one of these triggers:

1. `vq admin update` (v0.6) needs to pause-rebuild-resume across
   long maintenance windows where the daemon may be intentionally
   restarted multiple times, not a hard problem (orphan reattach
   handles it), but worth re-validating end-to-end.
2. Multi-host vq (v0.7+) where a daemon blip on one host
   compounds across the fleet.
3. A real over-run incident in production traceable to a
   daemon-down gap.

Until then, the watchdog-only approach buys simplicity that the
project should keep banking.

---

## See also

* [`SPEC.md`](SPEC.md) §3.4: watchdog as the only kill source.
* [`roadmap.md`](roadmap.md): v0.5.8 entry and v0.6 plan.
* `git log --grep "RuntimeMaxSec"`: full history of the
  attempt-and-revert.
