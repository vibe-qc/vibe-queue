# vq roadmap history, v0.1 to v0.9 (archived)

> **Archived on 2026-09-13.** Until then this file was `docs/roadmap.md`. By
> that date it had stopped being a plan: it held per-version release notes for
> v0.1.0 through v0.9.2 and a few mid-2026 status snapshots, and it had no
> forward section at all. Its content below is unchanged.
>
> * **What is planned next:** [`roadmap.md`](roadmap.md).
> * **What shipped from v0.26.0:** `CHANGELOG.md` at the repository root.
> * **v0.10 to v0.25** shipped before the public repository was split out at
>   0.25.7. Their per-version notes are not in this repository;
>   [`version_compatibility.md`](version_compatibility.md) keeps the version
>   map and the codename series.
> * **Issue numbers** below, such as `#125` or `#136`, belong to the pre-split
>   monorepo tracker, not to this project's GitLab issues.

Original preamble: "Internal development plan. Order is by priority, not
strict sequencing -- v0.4 might land before v0.3 if a need overtakes the
queue."

> **See also: [`SPEC.md`](SPEC.md)** -- the long-term design spec, which
> records *why* each version's scope is what it is, the non-negotiable
> invariants across releases, and what vq is explicitly not trying to
> become. When a bullet here is one line and you need the rationale, the
> spec section number to read is in parens beside the entry. The
> roadmap covers **sequencing**; the spec covers **design**.

## Done

### v0.1.0 -- local job queue
* CLI: `submit / queue / status / kill / daemon {start,stop,status,run}`
* JobSpec on disk (pydantic, version-checked)
* Daemon main loop with CPU budgeting
* Reboot semantics: RUNNING -> INTERRUPTED on daemon restart, no auto-recovery
* User systemd unit template at `contrib/vq-daemon.service`
* 130 tests

### v0.1.1 -- daemon-log dedup fix (`16a3efa`)
* `setup_daemon_logging` only attaches stderr StreamHandler when stderr is a TTY,
  preventing every record from being written twice when the parent has
  redirected stderr into the log file.

### v0.2.0 -- cross-machine submit + config + fetch
* `~/.config/vq/config.toml` with `default_host` + per-host
  `ssh / remote_vq / remote_python`
* SSH transport (`vq.transport`): scp upload, ssh exec, RemoteError surfacing
* `submit_remote`: tar workspace -> scp -> remote `vq submit localhost -c ... -- ...`
* `--python PATH` flag on submit (single-file mode)
* `vq fetch [HOST] JOBID -o DIR` -- workspace streamed back via
  ssh + tar pipe
* Hidden `vq tar-workspace JOBID` internal verb (used by `fetch_remote`)
* Default-host resolution for queue / status / kill / fetch -- `HOST` is
  now optional everywhere
* 204 tests; verified end-to-end laptop -> compute-d with vibe-qc importable

### v0.2.x -- `--max-jobs` cap (e6a2c88)
* Independent of `--max-cpus`; lets you set "one job at a time, but
  each may use the full box" without forcing every job to declare
  `--cpus = host_total`. Replaces the broken `--max-cpus 1` workaround
  for serial dispatch.

### v0.3.0 -- resource watchdog (2026-05-09)

* **JobSpec v2** with `mem_mb`, `wall_time_seconds`, `pgid`,
  `last_heartbeat_at` fields. v1 specs read into v2 cleanly (missing
  fields default to None).
* **`--mem-mb N` and `--wall-time-seconds N` flags** on submit
  (single-file and via `--`-form for `-d`/`-c`); plumbed through
  `submit_local` / `submit_remote` / remote-vq invocation.
* **Daemon memory-budget gate**: `--max-mem-mb N` sits alongside
  `--max-cpus` and `--max-jobs`. All three caps must allow a dispatch.
  Defaults to host total RAM from `/proc/meminfo` on Linux.
  `--default-job-mem-mb N` (2026-06-22) charges a job that declares no
  `--mem-mb` an assumed footprint, so an undeclared job is gated and
  cgroup-capped rather than dispatched unbounded.
* **Watchdog module** (`vq.watchdog`):
  * Per-job sampling of RSS via `/proc/<pid>/status` and CPU% via
    `/proc/<pid>/stat`, every `interval_seconds` (default 5).
  * Samples persisted at `<workspace>/_vq/samples.jsonl` (jsonl,
    one record per sample).
  * Three kill paths: per-job `mem_mb` exceeded -> OOM_KILLED;
    host RSS percent ceiling exceeded -> OOM_KILLED;
    `wall_time_seconds` elapsed -> TIME_EXCEEDED;
    CPU < starve_threshold for starve_window -> STARVED.
  * SIGTERM -> grace (default 10s) -> SIGKILL escalation, targets the
    process group (`os.killpg`) so OMP/MPI children die together.
  * Three new terminal states: `OOM_KILLED`, `STARVED`, `TIME_EXCEEDED`,
    distinguished from manual `KILLED` (so retry policies can target them).
  * `WATCHDOG_TERMINAL_STATES` constant + `JobSpec.is_watchdog_killed`
    property for downstream tooling.
* `pgid` captured at dispatch via `os.getpgid(popen.pid)` (works because
  `start_new_session=True` makes the child a session leader, so
  pgid == pid). Used for both watchdog kills and v0.4's daemon-recovery
  refinement.
* **252 tests + 2 skipped** (skipped = Linux-only `/proc` self-tests on
  macOS); +43 new in test_spec.py / test_daemon.py / test_watchdog.py
  including a real-subprocess wall-time-kill integration test.

**Notes for v0.4 carry-over.** v0.3 is monitoring-and-signal: the
watchdog samples and kills, but the kernel does not enforce. A job
that allocates faster than `interval_seconds` can still race the
sampler. v0.4 closes this with `systemd-run --user --scope
--property=MemoryMax=...` so the cgroup hierarchy enforces. Setup
prerequisite: `Delegate=cpu cpuset io memory pids` on
`/etc/systemd/system/user@.service.d/delegate.conf`. Documented in
`docs/install.md` (to ship with v0.4) as a pre-req; v0.4 falls back to
v0.3 `/proc` polling on hosts without delegation.

### v0.4.0 -- production hardening (2026-05-09)

* **cgroups v2 enforcement** via new ``vq.cgroup`` module.
  ``systemd-run --user --scope --collect --property=MemoryMax/MemoryHigh/CPUQuota/RuntimeMaxSec``
  wraps each dispatched job when delegation is detected at daemon
  startup. Host without delegation -> graceful fallback to v0.3 watchdog.
* **Daemon recovery using ``pgid``.** Restart no longer kills
  in-flight jobs. ``_reattach_or_interrupt_at_startup()`` checks
  ``killpg(pgid, 0)`` for each RUNNING spec; alive jobs are tracked
  as orphans (``self._orphans``) and re-sampled by the watchdog;
  exits are reconciled via periodic ``killpg(pgid, 0)`` polling
  (exit code unrecoverable -> INTERRUPTED).
* **Per-job event log** at ``<workspace>/_vq/events.jsonl`` -- new
  ``vq.events`` module with ``EventKind`` enum (SUBMITTED, DISPATCHED,
  STATE_TRANSITION, KILL_REQUESTED, WATCHDOG_KILL). Wired into submit /
  daemon / kill / watchdog state transitions. Append-only, best-effort
  writes (filesystem errors swallowed; never break dispatch).
* **Watchdog telemetry-only mode**: when cgroup enforcement is active,
  ``Watchdog.enforce_memory`` flips to False so the kernel handles
  memory and the watchdog avoids a duplicate kill. CPU-starvation
  detection stays on regardless. (Wall-time was originally also
  delegated to systemd via ``RuntimeMaxSec``; v0.5.8 reverted that
  — see the v0.5.8 entry below for why.)
* **+24 tests** (8 events + 2 orphan-recovery net + 13 cgroup +1 cli-version),
  275 passed / 3 skipped total on macOS Python 3.14.

**Web UI deferred to v0.5** (originally scoped here per SPEC sec 13,
moved out to keep v0.4 a coherent hardening release). The event log
that v0.5's UI needs as its read source is in place from v0.4.

### v0.5.0 -- read-only web dashboard (2026-05-09)

* FastAPI app at `vq.web` reading the existing JSON state + the v0.4
  event log (no new state). Routes: `/queue` (HTML table, htmx
  3 s polling), `/jobs/<jobid>` (full spec + last 200 lines
  stdout/stderr + event log timeline), `/health/{live,ready}`.
* `vq web run` CLI verb -- foreground uvicorn launcher, defaults to
  `127.0.0.1:8080`.
* `contrib/vq-web.service` systemd-user unit (parallels
  `vq-daemon.service`). Web service is independent of the daemon.
* Optional install footprint: `pip install -e '.[web]'`.
* +13 tests via FastAPI TestClient.

### v0.5.1 — pause/resume + bearer-token auth + write API (2026-05-09)

* `vq pause` / `vq resume` (SIGSTOP/SIGCONT a job's pgid). New
  non-terminal state `SUSPENDED`. Wall-time enforcement excludes
  paused intervals (`paused_seconds_total` on the spec).
* Bearer-token auth (`vq.auth`): `vq web init-token` writes
  `~/.config/vq/web-token` mode 0600; `hmac.compare_digest`;
  read endpoints stay unauthenticated.
* `POST /api/v1/jobs/<id>/{kill,pause,resume}` (bearer-gated).
* Default web port: 8765 (was 8080 — too crowded).
* `docs/web.md` with SSH-tunnel access + caddy reverse-proxy sketch.

### v0.5.2 — queue-wide --all on pause/resume (2026-05-09)

* `vq pause --all` / `vq resume --all` plus `POST
  /api/v1/queue/{pause,resume}` for whole-queue control.
  Mixed-state queues handled idempotently (jobs in the wrong state
  are skipped, counted in the summary).

### v0.5.3 — CRYSTAL14 + PROPERTIES14 on compute-d (2026-05-09)

* The user's `~/bin` added to the daemon's PATH so `crystal`
  and `properties` resolve from dispatched jobs.
* `contrib/run-crystal.sh` wrapper for the standard `crystal < INPUT
  > OUTPUT` IO-redirection idiom that vq's `--`-form argv
  pass-through can't express.

### v0.5.4 — parallel CRYSTAL14 + clean systemd PATH drop-in (2026-05-09)

* `run-crystal.sh` defaults to `mpirun -np 14 Pcrystal`; flags
  `--serial`, `--np N`, `--properties` cover all four binary
  variants (CRYSTAL ser/par × PROPERTIES ser/par).
* Daemon PATH customization moved into a clean systemd drop-in at
  `~/.config/systemd/user/vq-daemon.service.d/path-override.conf`,
  so `contrib/vq-daemon.service` stays generic across hosts.
* `web_state` test fixture isolates `ENV_CONFIG_DIR` +
  `ENV_WEB_TOKEN_FILE` (pre-existing test-isolation bug exposed
  when the suite ran on compute-d for the first time).

### v0.5.5 — Pcrystal INPUT-file convention fix (2026-05-10)

* `run-crystal.sh` parallel path stages user input as `./INPUT` and
  runs `mpirun -np N Pcrystal > out.out` instead of relying on
  stdin-via-mpirun (which is unreliable: rank-distribution
  semantics differ across OpenMPI versions). EXIT trap restores
  any pre-existing `INPUT` from a `.bak.$$` backup.
* Handover gains a "Why vq exists" scope statement: vq dispatches
  vibe-qc (preferred) + CRYSTAL/ORCA/PySCF (validation references);
  Psi4 still on the install list.

## Pending — v0.6.x followups for operator controls

> v0.5.13 shipped ``vq throttle``, v0.5.14 shipped ``vq drain``,
> v0.5.15 shipped persistent throttle, v0.5.16 shipped
> ``--duration`` auto-release for both, **v0.5.21 shipped the
> ``renice`` fallback for non-cgroup hosts**. The operator-controls
> cluster is now feature-complete for v0.5.x.

> **Original v0.5.11 design pin (shipped v0.5.13 + v0.5.14):** the operator's
> 2026-05-10 ask was that ``vq pause --all`` is too coarse for the
> interactive-use / host-is-busy cases. The htop screenshot showed
> ~37% saturation on a 24-thread box during a Steam game session —
> plenty of headroom for vq jobs to coexist *if* they get out of the
> way under contention. Soft throttle and drain mode were the
> missing tools. ``vq throttle`` shipped in v0.5.13;
> ``vq drain`` shipped in v0.5.14; see the Done section for both.

---

## Pending — `vq cleanup` followups

v0.5.10 shipped the manual ``vq cleanup`` verb. v0.5.17 closed the
"auto-policy daemon-loop integration" item. v0.5.22 closed the
configurable-archive-dir item. **v0.5.23 closed the per-state
retention overrides item** (``--archive-after-state STATE:DUR``,
symmetric ``--delete-after-state``, ``AutoCleanupPolicy.archive_after_by_state``
+ ``.delete_after_by_state`` fields). **The cleanup cluster is now
feature-complete for v0.5.x.**

### `vq cleanup` CLI verb (manual) — landed in v0.5.10

```bash
vq cleanup [HOST]                                    # list terminal jobs
vq cleanup [HOST] --archive --older-than 30d         # dry-run preview
vq cleanup [HOST] --archive --older-than 30d -x      # actually archive
vq cleanup [HOST] --delete  --older-than 90d -x      # actually delete
vq cleanup [HOST] --restore JOBID -x                 # un-archive
```

* Eligibility: spec is in a terminal state (completed / failed /
  killed / oom_killed / time_exceeded / starved / aborted_by_queue),
  AND `finished_at` is older than `--older-than`.
* Archive: workspace tarred to ``<state_root>/archive/<jobid>.tar.bz2``
  (under ``$VQ_STATE_DIR`` if set, default ``~/.local/share/vq/archive/``;
  the original roadmap pinned ``~/vq-archive/`` but nesting under the
  state root is more test-friendly and keeps "all vq state lives under
  VQ_STATE_DIR" as one mental model). Spec gets ``archived_at`` and
  ``archive_path`` populated; ``vq queue`` annotates the row with
  ``(archived)``; ``vq fetch <jobid>`` knows to un-tar from the
  archive instead of copying the (gone) workspace.
* Delete: spec + workspace + archive (whichever exist) all gone.
  Job vanishes from ``vq queue``. Idempotent; safe to call again
  after a partial failure.
* ``--dry-run`` is the default: actions are gated behind ``-x`` /
  ``--execute`` so a typo in ``--older-than`` can't nuke six months
  of work.

### Spec field additions for cleanup tracking — landed in v0.5.10

* ``last_status_at: str | None`` — touched by ``vq status <jobid>``
  on terminal specs only (avoids racing the daemon, which is the
  only writer for non-terminal specs). Lets auto-cleanup say "skip
  jobs the user has looked at recently."
* ``last_fetched_at: str | None`` — touched by ``vq fetch <jobid>``
  on terminal specs only. A fetched job is redundant on the daemon
  host (laptop has the data), so it can be cleaned more aggressively.
* ``archived_at: str | None`` + ``archive_path: str | None`` — set
  when ``vq cleanup --archive`` runs, cleared when ``--restore``
  un-tars.

### v0.6.x — auto-cleanup policy via config

```toml
[cleanup]
enabled            = true
archive_after_days = 30      # archive terminal jobs older than this
delete_after_days  = 90      # delete archived jobs older than this
archive_dir        = "~/vq-archive"
keep_if_status_within_days = 7   # spare jobs the user looked at recently
```

Daemon main loop runs cleanup once a day (cheap; just stat'ing terminal
specs). Same archive/delete logic as the manual verb. Strictly opt-in.

---

## v0.9.2 — *Kleinrock's Queue* — `vq status` shows a PENDING job's queue position (2026-06-05)

Answers the documented "my job sits in `pending`, where am I in line?"
question (the onboarding doc literally says *"if your job sits in pending
longer than you expect, check `vq queue`"*). `vq status JOBID` on a PENDING
job now adds a line:

```
queue position: 3 of 12 pending (dispatch order: priority, then submit time)
```

The rank is computed by the **exact key the daemon dispatches on** —
`(-priority, submitted_at)` (higher priority first, then FIFO by submission) —
via a new `listing.pending_queue_position(spec, pending) -> (rank, total)`.
Rank 1 means "first in line by sort order", explicitly **not** a guaranteed
next-to-run: a job ahead may be held by an unmet dependency, a scheduled
`not_before`, or a resource budget — hence the "dispatch order" wording rather
than an ETA promise. Best-effort: a listing hiccup never breaks `vq status`,
and the line only appears for PENDING jobs.

**Tests:** 4 new — the helper ranks by priority-then-submit-time and handles a
lone pending job (`test_listing.py`); `vq status` shows the position for a
PENDING job and omits it for a RUNNING one (`test_status.py`). Suite: 2222
passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. A true *ETA* (not just position) would need job
duration history — a separate, larger piece deliberately not attempted here.

---

## v0.9.1 — *Strachey's Monitor* — `vq top --watch` + `--json` (2026-06-05)

Rounds out v0.9.0's `vq top`:

* **`vq top --watch [N]`** (`-w`) — auto-refresh every N seconds (default 2;
  `-w` alone uses 2, `-w 5` for five) until Ctrl-C, clearing the screen
  between frames with a `host — every Ns — <timestamp> — Ctrl-C to exit`
  banner. Turns the one-shot snapshot into a live monitor (no more wrapping
  it in `watch(1)`). For a remote host each refresh is a fresh SSH —
  acceptable for the convenience, but `-w 5`+ keeps the handshake rate sane.
* **`vq top --json`** — emit the rows as a JSON array (`jobid`, `name`,
  `cpus`, `cpu_percent`, `rss_mb`, `mem_mb`, `elapsed_seconds`,
  `wall_time_seconds`, `sample_stale`) for dashboards / scripting. Mutually
  exclusive with `--watch`.

The refresh loop is `top.watch_loop(render, *, interval, host, sleep, write,
clock)` with `sleep` / `write` / `clock` injected, so it's unit-tested
without real time or a terminal. Remote `--json` / `--watch` reuse the
remote's own rendering via `_delegate_to_remote`.

**Tests:** 5 new in `tests/test_top.py` (JSON field shape, `show_top_local(
as_json=True)`, the watch loop renders exactly one frame + clears the screen
then exits on Ctrl-C, the `--json` CLI path, the `--watch`/`--json` mutex).
Suite: 2218 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. Remaining `vq top` idea: an active-elapsed column
that subtracts `paused_seconds_total`.

---

## v0.9.0 — *Tukey's Window* — `vq top`, a live per-job resource view (2026-06-05)

First **feature** ship after the v0.8.x reliability arc (which closed the
reliability audit). `vq top [HOST]` renders a top(1)-style table of the
RUNNING jobs' live resource usage, read from the watchdog's existing
`<workspace>/_vq/samples.jsonl` — nothing else consumed those samples for
display before, so this is pure new surface over data we already collect.

```
JOBID         NAME     CPU%    RSS    MEM  ELAPSED     WALL
abc123def456  mgo-pbe  785%  12.1G  15.6G  0:02:30  6:00:00
```

* **CPU%** is the whole process group (an 8-core CRYSTAL run saturating its
  cores reads ~800%); a trailing `*` flags a stale sample (>30 s old — the
  daemon stopped sampling or the job is wedged).
* **RSS / MEM** — current resident memory vs the job's `--mem-mb` ceiling.
* **ELAPSED / WALL** — wall-clock since dispatch (from `started_at`, so it
  shows even before the first sample) vs `--wall-time-seconds`.

Rows sort by CPU% descending (busiest on top). It's the per-job complement to
`vq overview` (host-level) and `vq status` (one job, no resource curve), and
answers the compute-d/compute-a operator's "is my job actually using the cores? how
close to the memory cap / wall-time deadline?" at a glance. Local reads the
samples directly; remote delegates `vq top localhost` over SSH via
`_delegate_to_remote`, exactly like `vq queue` / `vq status`. New module
`vq/top.py` (`gather_top_rows` + `format_top_table` + `show_top_local`).

**Tests:** 11 in `tests/test_top.py` (latest-sample read incl. last-line
selection, RUNNING-only filter, missing-sample → resources `-` but ELAPSED
still shown, CPU%-descending sort, stale-sample flag, table rendering +
footer, remote raises locally, CLI `--help` + local-empty). Suite: 2213
passed / 11 skipped on macOS.

**Follow-ups (not in this ship):** `vq top --watch [N]` auto-refresh; `--json`
for machine consumers; an active-elapsed column that subtracts
`paused_seconds_total`.

`Patch-candidate: v0.10.x`. Minor bump — first user-facing feature of the v0.9
line.

---

## v0.8.25 — *Denning's Lattice* — `vq resubmit` ownership gate (ISO-2) (2026-06-05)

Closes audit **ISO-2**. On a multi-user host `vq resubmit` had **no ownership
check**, so user A could re-run user B's job — and since resubmit carries the
source spec's `submitter`, the new job would run **as B**. `resubmit_local`
now calls `ownership.check_owner(source)` right after reading the source spec,
mirroring the `fetch.py` / `kill.py` / status gates: a no-op in single-user
mode, and in multi-user mode it allows the owner (and admin-group members)
but raises `OwnershipError` for anyone else.

**Tests:** 2 new in `tests/test_multi_user.py` — a cross-user resubmit raises
`OwnershipError`; single-user resubmit is unaffected (the check is a no-op
there, so the vestigial `submitter` field is ignored). Suite: 2202 passed /
11 skipped on macOS.

`Patch-candidate: v0.10.x` (security fix — same tier as the v0.6.35 / v0.6.44
multi-user hardening; the release chat may target it more broadly).

> **ISO-1 (the cross-user *read* isolation) is deliberately NOT in this
> ship.** Its two halves are interdependent and one needs a maintainer
> design decision: the ownership *gates* on `vq logs` / `vq status` are
> bypassable on their own (a user can `cat` another user's
> world-readable spec/log directly), so they must land *with* the directory
> permission tightening (`users_root` 0711, per-uid subtrees 0700) — but
> `0700` per-user dirs **break admin cross-user access via the CLI** (an
> admin's `vq status <other-job>` reads the spec file *as the admin's own
> uid*, which `0700` denies), so the admin path needs a decided model
> (sudo-as-root? a group-readable `0750` with `group=vq-admins`? route the
> read through the root daemon's RPC?). That decision + real multi-user
> (Linux) validation can't be made on the macOS dev box. Full analysis is in
> `HANDOVER_VQ_AUDIT.md` § B.

---

## v0.8.24 — *Jacobson's Backpressure* — host-pressure scheduling hardening (HP-1/2/3) (2026-06-05)

First of the maintainer-approved (2026-06-01) § B items. The v0.6.20
host-pressure auto-pause (SIGSTOP running jobs at ≥85 % host-memory pressure,
SIGCONT once it recedes below 70 %) had three holes the audit found; all
three close here. The watchdog gains a `host_pressure_active` property and a
`reseed_host_pressure_paused()` method; the daemon does the rest.

* **HP-1 — restart survival.** The watchdog's record of *which* jobs it
  auto-paused lived only in memory, so a daemon restart stranded those jobs
  SUSPENDED **forever** (the resume tick only SIGCONTs jobs it remembers
  pausing). `run()` now re-seeds at startup from the SUSPENDED specs tagged
  `watchdog_host_pressure`, so they resume when pressure recedes as if the
  daemon never bounced. **Operator-paused jobs (any other `paused_by`,
  including a bare `vq pause`) are deliberately excluded** — only `vq resume`
  brings those back. Skipped (with a warning) when host-pressure enforcement
  is off, so a re-seed can't gate dispatch forever.
* **HP-2 — dispatch gating.** The daemon kept dispatching NEW jobs onto a
  host it had just auto-paused for pressure — re-loading the very host it was
  relieving. `_dispatch_pending` now holds new dispatch while
  `watchdog.host_pressure_active`, exactly like a full drain (running /
  reattached jobs continue). It **composes with** operator `vq drain` —
  neither overrides the other; both are independent "hold new dispatch"
  gates.
* **HP-3 — orphan coverage.** The auto-pause candidate list was `_running`
  only, so a **reattached orphan** (a job still alive from a previous daemon)
  was immune to the pressure pause and kept loading the host. The candidate
  set is now `_running ∪ _orphans`.

**Tests:** 7 new in `tests/test_host_pressure_hp123.py` — the watchdog
re-seed re-arms RESUME for exactly the re-seeded jobs; the startup scan
re-seeds watchdog pauses but **not** operator pauses, and is skipped when
enforcement is off; dispatch is gated while pressure-active and resumes after
(the operator-pause/drain interaction the sign-off asked for); a reattached
orphan is a pressure candidate. Suite: 2200 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. Behaviour change (scheduling semantics), shipped
per the 2026-06-01 maintainer sign-off.

---

## v0.8.23 — *Bush's Memex* — the workdir-missing fetch hint names the real cause (CLEAN-3) (2026-06-05)

Closes audit **CLEAN-3**. When `vq fetch --workdir` finds the workdir
directory gone, it raised "workdir not found" with a hint that **always
blamed `--clean-tmp`** — misdirecting an operator whose workdir was simply
removed by the daemon's auto-cleanup **age-sweep** (a job that ran fine, sat
terminal past `workdir_max_age`, and got its scratch reclaimed; the operator
never passed `--clean-tmp`).

The age-sweep now leaves a breadcrumb: when it rmtrees a terminal job's
workdir it stamps `spec.workdir_swept_at` (new additive spec field), under
the per-spec lock so it can't clobber a concurrent `vq status` / `vq fetch`
stamp. A new `fetch._workdir_missing_hint(spec)` — shared by
`fetch_workdir_local` and the `tar-workdir` emitter, which had duplicated the
old hint — names the real cause: the age-sweep (with its timestamp) if
`workdir_swept_at` is set, else `--clean-tmp` if that was the opt-in, else a
bare not-found.

**Tests:** 2 new — the age-sweep hint fires (and doesn't mention
`--clean-tmp`) when `workdir_swept_at` is set (`test_fetch.py`); the sweep
stamps `workdir_swept_at` on a terminal job's spec when it reclaims the
workdir (`test_workdir_v0_6_54.py`). Suite: 2193 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. New additive spec field `workdir_swept_at` (old
specs read it as None — backward-compatible).

---

## v0.8.22 — *Mills's Clock* — measure the pause interval with a monotonic clock (PA-1) (2026-06-05)

Closes audit **PA-1**. Pause/resume bills `paused_seconds_total` (subtracted
from elapsed for wall-time enforcement) by diffing the wall-clock `paused_at`
ISO string against now. A wall-clock **step** during the pause — an NTP slew,
a manual `date`, a DST jump — distorts that diff, so a job paused across such
a step could be mis-billed (over- or under-counting its pause, and thus its
wall-time budget). `vq admin update`'s pause-the-queue-for-a-rebuild flow can
hold a pause for minutes; a long pause is exactly when a clock step is most
likely to land.

`vq pause` now also stamps `spec.paused_monotonic_at = time.monotonic()` at
SIGSTOP, and `vq resume` measures the interval from it
(`monotonic() - paused_monotonic_at`), which no wall-clock step can move.
`paused_at` stays for human display. `CLOCK_MONOTONIC` is system-wide on
Linux/macOS so it's comparable across the separate `vq pause` / `vq resume`
processes, and a SIGSTOP'd job can't survive a reboot, so the two readings
are always same-boot. A spec paused before PA-1 shipped (no anchor on disk)
falls back to the old wall-clock diff.

**Tests:** 2 new in `tests/test_pause_resume.py` (resume bills the monotonic
interval even when `paused_at` is corrupted to look years old; the wall-clock
fallback still works without an anchor). Suite: 2191 passed / 11 skipped on
macOS.

`Patch-candidate: v0.10.x`. New additive spec field `paused_monotonic_at`
(old specs read it as None — backward-compatible).

---

## v0.8.21 — *Nyquist's Sample* — the watchdog discards a garbage CPU% delta (WD-1) (2026-06-04)

Closes audit **WD-1**. The watchdog computes CPU% as a delta of cumulative
cputime over the wall-clock interval between samples. Two things make that
delta meaningless:

* the cputime reader **switched** between samples — the cgroup
  (`cpu.stat`), pgid-walk, and pid readers have different baselines (the
  cgroup is the scope's cumulative; the walks sum *live* processes), so a
  delta across a switch is noise;
* a child **exited** between samples — it takes its accumulated cputime out
  of the pgid / cgroup aggregate, so the total *drops* and the raw delta goes
  **negative**.

Either produced a bogus `cpu_percent` in `samples.jsonl` (a negative number,
or a huge one on a source switch) and risked a spurious STARVED reading. The
sampler now tags each cputime read with its source and only computes a CPU%
when this sample's source matches the last one's **and** the delta is
non-negative; otherwise it logs `cpu_percent = null` and re-baselines from
the new value. `WatchdogJobState` gains `last_cputime_source`; the
resume/re-baseline path clears it too. Also corrects the
`read_cputime_seconds_pgid` docstring, which wrongly claimed an exiting child
"leaves a flat plateau" (it leaves a *drop*).

**Tests:** 2 new in `tests/test_watchdog.py` (a negative delta logs `null`,
not a negative cpu%; a cgroup→pgid source switch re-baselines instead of
logging a huge value). Suite: 2189 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`.

---

## v0.8.20 — *Baker's Collector* — honour `--clean-tmp` on a queue-abort (CLEAN-4) (2026-06-04)

Closes audit **CLEAN-4**. The opt-in immediate workdir cleanup
(`--clean-tmp` → `clean_workdir_on_terminal`) ran from `_record_finish` /
`_record_orphan_finish`, but **not** from `_mark_aborted_by_queue` — so a job
the queue itself ended (daemon restart with a dead pgid; an orphan gone with
no exit marker) left its `$VQ_WORKDIR` scratch behind even when the submitter
asked for it to be cleaned. `_mark_aborted_by_queue` now calls
`_maybe_cleanup_workdir` after its terminal write, like every other terminal
path. Idempotent: a no-op unless `clean_workdir_on_terminal` is set and the
workdir exists.

**Tests:** 2 new in `tests/test_daemon.py` (the `--clean-tmp` workdir is
removed on a queue-abort; a workdir without `--clean-tmp` is preserved).
Suite: 2187 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`.

---

## v0.8.19 — *Lampson's Confinement* — run the scope-collision pre-flight in multi-user mode (MU-1) (2026-06-04)

Closes audit **MU-1**. `_start_job`'s leaked-cgroup-scope pre-flight (v0.5.51
— detect a `vq-job-<id>.scope` that survived a prior dispatch's `--collect`
cleanup, stop it, or land the spec FAILED with a clear reason instead of
hitting systemd-run's cryptic "Unit already exists") was gated on
`self.cgroup_enabled` alone. But in **multi-user mode `systemd-run --scope`
is mandatory** — it's how the daemon drops privileges to the submitter —
*regardless of cgroup enforcement*. So a multi-user daemon with cgroup
enforcement off created scopes but never checked for collisions; a leaked
scope then surfaced only as the opaque systemd-run failure.

The gate is now `self.cgroup_enabled or self._multi_user` — the pre-flight
runs whenever a scope will actually be created. `cgroup.scope_exists` /
`stop_scope` already target the right systemd manager (system vs `--user`)
via their `multi_user` arg, so no other change was needed.

**Tests:** 1 new in `tests/test_daemon.py` — a multi-user daemon with
`cgroup_enabled=False` and an unstoppable leaked scope now lands the spec
FAILED at the pre-flight (pre-fix it sailed past the skipped check into the
systemd-run wrap). Suite: 2185 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`.

---

## v0.8.18 — *Chandra's Detector* — `vq status` / `vq queue` flag stale state when the daemon is down (STATUS-1) (2026-06-04)

First of the localized § A audit tail. Closes **STATUS-1**: `vq status` and
`vq queue` read `spec.state` straight off disk with no liveness cross-check,
so a job whose process died while the daemon was down keeps reading RUNNING
(or PENDING / SUSPENDED) until the daemon restarts and its recovery pass
reconciles it. The operator has no way to tell a genuinely-running job from a
stale label.

Both now consult `daemon_control.is_daemon_running()` (the pidfile + `kill
-0` probe `vq daemon status` already uses) and, when the daemon is down,
flag non-terminal rows as possibly-stale:

* **`vq status`** annotates the `state:` line —
  `state:        running  (⚠ daemon not running — may be stale)`. Terminal
  states (and archived, which implies terminal) are immutable, so they're
  never flagged.
* **`vq queue`** prints a one-line warning above the table when any listed
  row is non-terminal and the local daemon is down. The warning is
  per-host: a `--all` fleet listing shows it only for the hosts that are
  actually down (each host's own `vq queue` renders its own warning), and
  the JSON output (`--json`) is left untouched for machine consumers.

The check is local-only (both verbs already delegate remote queries over
SSH, so the remote side runs its own local probe) and multi-user-aware
(`is_daemon_running(multi_user=...)`).

**Tests:** 3 new in `tests/test_status.py` (a non-terminal spec warns when
the daemon is down, is silent when it's up, and a terminal spec never warns).
Suite: 2184 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`.

---

## v0.8.17 — *Saltzer's End-to-End* — harden the remote-fetch SSH transport (REMOTE-2/3/4/5/6) (2026-06-04)

Closes the last non-`spec_lock` item of audit § C. `fetch_remote` /
`fetch_workdir_remote` hand-rolled their own `ssh` Popen and bypassed the
hardened `transport._ssh_base`, regressing a cluster of bugs the rest of the
transport layer had already fixed. New `transport.stream_remote_vq` (a
streaming context manager, since a tarball must not be buffered into memory
the way `run_remote_vq` does) routes both through one hardened path; the two
fetch functions collapse to thin wrappers over a shared
`_stream_extract_remote_tar`.

The five fixes (end-to-end reliability checks belong at the endpoint, not
assumed of the transport — hence the codename):

* **REMOTE-2** — uses `_ssh_base`: `ConnectTimeout` / `BatchMode=yes` /
  `ServerAlive` heartbeats. A hung or key-rejecting host now fails fast
  instead of hanging the fetch (the 2026-05-26 compute-d-lockout class).
* **REMOTE-3** — `shlex.join` for the remote command instead of passing argv
  as separate `ssh` words. `ssh host a b c` lets the remote shell
  re-tokenise `a b c`; the v0.5.32 *word-split* bug had silently regressed
  here. The remote now receives one shell-quoted command.
* **REMOTE-4** — **temp-then-rename**: tar members extract into a hidden
  staging dir on the same filesystem as the destination, promoted by a
  single atomic `os.replace` only on full success. A mid-stream failure
  (remote dies / tar truncates / disk fills) leaves NO half-written
  `<dest>/` for the next fetch or `vq cleanup --restore` to trip over.
* **REMOTE-5** — stderr is drained **concurrently** by a daemon thread while
  the caller reads the stdout tar, so a chatty remote can't fill the stderr
  pipe buffer and deadlock against our blocked stdout read.
* **REMOTE-6** — ssh's own exit **255** (connection refused / host
  unreachable / key rejected) is surfaced as a distinct transport error,
  not conflated with a real `remote_vq` non-zero exit. And a caller-side
  abort (destination exists, truncated tar) kills the remote and propagates
  the caller's exception rather than masking it with the now-meaningless rc.

**Tests:** `test_fetch.py` updated (the mocks move to `transport.subprocess`;
the argv assertions assert the hardened `_ssh_base` shape + the single
shell-quoted remote command) + a new REMOTE-4 test (a truncated stream leaves
no partial destination and no staging dir). 5 new direct
`transport.stream_remote_vq` tests in `test_transport.py` (hardened argv,
ssh-255, remote-rc, caller-exception-not-masked, stderr-drain). Suite: 2181
passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. **With this ship § C of the reliability audit is
complete** (`spec_lock` wiring v0.8.11–13, STATE-2 v0.8.15, the fetch
rewrite here). The remaining audit work is § B (maintainer-approved
host-pressure scheduling + multi-user isolation) and the localized § A tail.

---

## v0.8.16 — *Chandy's Snapshot* — the `depends_on` cascade-fail is now observable (EVENT-2) (2026-06-04)

Closes audit **EVENT-2**, the sibling of v0.8.12's EVENT-1. When a
`depends_on` predecessor lands in a non-COMPLETED terminal state, the daemon
cascade-fails the dependent — but it wrote FAILED to the spec **silently**:
no `STATE_TRANSITION` event in the dependent's `events.jsonl` and no terminal
webhook, unlike every other terminal transition in vq. A user watching the
event log or a Slack/Discord webhook saw the predecessor fail and the
dependent flip to FAILED in `vq status` with no record of *why* or *when* in
between.

The cascade now emits the `STATE_TRANSITION` event (inside the spec lock,
atomic with the write — reason = the `failure_reason` that names the failing
predecessor) and fires `send_terminal_notification` (after the lock; no-op
when no webhook is configured).

Also fixes a long-standing comment drift the audit flagged: the cascade
docstring (`daemon.py`) and `spec.py` said the dependent fails "with a
`work_errors` entry" — the field is `failure_reason` (`work_errors` is an
`admin.py` field for `vq admin update`, unrelated).

**Tests:** 1 new in `tests/test_depends_on_v0_6_51.py` — after a cascade-fail
the dependent's `events.jsonl` carries a `state_transition` to `failed` from
`pending` naming the predecessor. Suite: 2175 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`.

---

## v0.8.15 — *Corbató's Daemon* — reap a terminal job's process group that survives a daemon restart (STATE-2) (2026-06-04)

Restart-time companion to v0.8.14. Closes audit **STATE-2**: the startup
reattach scan (`_reattach_or_interrupt_at_startup`) only processes
RUNNING/SUSPENDED specs. A spec that was already **terminal** (`vq kill` /
watchdog) when the previous daemon died — but whose **process group is still
alive** — is skipped entirely, so it leaks: untracked by `_running` /
`_orphans`, consuming CPU/RAM forever, while its spec reads KILLED.

The startup scan now SIGKILL-reaps these. The original killer already
SIGTERM'd the process (`kill.py` signals before writing the terminal label;
the watchdog SIGTERMs at the transition) and it ignored that for the entire
downtime — so the SIGTERM grace is long elapsed and a direct SIGKILL is
correct (SIGCONT first in case it was left stopped). This is the
restart-time analogue of STATE-3's grace-then-SIGKILL: there the grace is in
the future, here it already passed during the downtime.

**Recycled-pgid guard.** The reap is gated on `_pid_fingerprint_matches` —
the same `/proc/<pid>/stat` start-time cross-check the reattach path uses.
`False` ("the kernel recycled this PID to an unrelated process") → **skip**:
SIGKILLing someone else's process is worse than a rare leak. `None` (macOS /
pre-v0.5.50 spec, no fingerprint) falls back to pgid-only liveness, matching
the reattach path.

**Tests:** 2 new in `tests/test_terminal_reaping.py` — a terminal spec with a
live pgid is SIGKILL-reaped at startup (the spec stays KILLED, the job is not
re-tracked); a fingerprint-recycled pgid is left untouched. Suite: 2174
passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. STATE-2 + STATE-3 together close the
"terminal-but-alive process" leak in both regimes (normal operation and
across a restart).

---

## v0.8.14 — *Thompson's Reaper* — SIGKILL escalation for a `vq kill`'d job that ignores SIGTERM (STATE-3) (2026-06-04)

First of the post-`spec_lock` reliability-audit items. Closes audit
**STATE-3**: `vq kill` SIGTERMs the job's process group and writes a terminal
label, but it never escalates. A process that **ignores SIGTERM** then stays
alive-in-fact while its spec reads KILLED — and because the daemon's
`popen.poll()` never returns for it, the job never leaves `_running`, so its
cpu/mem claim is **pinned forever** (the daemon won't dispatch into the slot
it thinks is still busy).

The watchdog already escalates SIGTERM → grace → SIGKILL for its *own* kills
(OOM_KILLED / STARVED / TIME_EXCEEDED) because it tracks the SIGTERM deadline
in-process. `vq kill` runs in a separate process with no daemon IPC, so the
daemon only learns of the kill by reading the spec. v0.8.14 gives
`_reconcile_running` the matching escalation: for any `_running` job whose
on-disk spec has gone terminal but whose process is still alive, arm a
`KILL_ESCALATION_GRACE_SECONDS` (10s, mirroring the watchdog grace) deadline;
once it elapses, `killpg(pgid, SIGKILL)`. The next poll then reaps the dead
process via `_record_finish` (which preserves the terminal label and stashes
the exit code).

`_RunningJob` gains `term_deadline` (monotonic, armed on first notice) and
`term_sigkilled` (so the SIGKILL fires + logs exactly once, not every tick).
A non-killed RUNNING job never arms the clock.

**Tests:** 2 new in `tests/test_terminal_reaping.py` — a real
SIGTERM-ignoring child (installs `SIG_IGN`, signals readiness so the test
doesn't race interpreter startup) is escalated to SIGKILL and reaped out of
`_running`; a normal RUNNING job is left untouched (deadline stays un-armed).
Suite: 2172 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. Pairs with **STATE-2** (next ship: the same
terminal-but-alive process, but surviving a daemon *restart* — startup only
scans RUNNING/SUSPENDED, so a terminal spec with a live pgid is currently
never reaped).

---

## v0.8.13 — *Gray's Transaction* — `spec_lock` for operator-state writers + stamps + cleanup (2026-06-04)

Third and final `spec_lock`-wiring ship — completes the serialization half
of the audit's root cause (`HANDOVER_VQ_AUDIT.md` § C). v0.8.11 wired the
terminal-exit writers, v0.8.12 the dispatch window; this wires the remaining
read-modify-write sites so **every** spec writer now takes the per-spec lock.

### Sites wired

* **`pause_resume.py`** — `pause_job` (read → SIGSTOP → SUSPENDED write) and
  `resume_job` (read → SIGCONT → RUNNING write) now hold the lock across the
  whole body, so the state check sees the latest state and refuses instead of
  clobbering a job that went terminal under it. The bulk verbs (`pause_all`,
  `resume_all`, `pause_provides_branches`, `resume_jobs`) delegate to these,
  so they get per-spec locking for free.
* **`status.py` + `fetch.py` stamps (STATE-4)** — the `last_status_at` /
  `last_fetched_at` stamps on terminal specs now lock + **re-read fresh**
  before writing, instead of writing the function's stale top-of-call read.
  The lock is taken only for the stamp (at the end), never across the slow
  output build / tarball copy. This stops a stamp from clobbering a
  concurrent `cleanup`/`fetch`/`status` write.
* **`cleanup.py` (CLEAN-2)** — `archive_workspace` / `restore_workspace` apply
  their archive-field changes under the lock with a fresh re-read (after the
  slow tar), and `delete_job` removes the spec under the lock so a racing
  status/fetch stamp can't resurrect a half-deleted spec. `delete_job` also
  drops the `<spec>.lock` sidecar (new `paths.spec_lock_path` helper) rather
  than leaking one empty lock file per deleted job.

### Deliberately NOT wired

* **`throttle.py`** — reads the spec to check state but never writes it (it
  sets a cgroup CPUWeight / renice, not a spec field). No read-modify-write,
  no lock needed.
* **`resubmit.py`** — reads the source spec and writes a *new* sibling spec;
  the source is never mutated, so there's no lost-update to guard. (Its
  ownership concern is the separate ISO-2 security item.)

**Tests:** 1 new in `tests/test_spec_lock_wiring.py`
(`TestStatusStampVsCleanupDelete`: a `vq status` stamp racing
`vq cleanup --delete` over a widened window must not resurrect the deleted
spec). The existing pause/resume, status, fetch, and cleanup suites cover
the per-site behavior. Suite: 2170 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x`. **With this ship the `spec_lock` wiring (audit
§ C, "the most important remaining work") is complete** — the daemon loop,
the in-process RPC thread, and every CLI verb that does a spec
read-modify-write now serialize on the per-spec advisory lock. The residual
re-read-before-write guards from the v0.8.9 first audit pass (STATE-1,
CONC-3) are now fully race-free because both sides take the lock.

---

## v0.8.12 — *Peterson's Lock* — `spec_lock` for the `_start_job` dispatch window + EVENT-1 (2026-06-04)

Second `spec_lock`-wiring ship. v0.8.11 closed the *terminal-exit* race;
this one closes the *dispatch* race in `_start_job`, which is trickier
because a `subprocess.Popen` sits between the two spec writes and the lock
must never be held across it.

### Two windows, both closed

`_start_job` writes the spec twice: a **RUNNING-claim** write (pid/pgid
None) *before* Popen — so a daemon crash can't leave a PENDING spec next to
a live process — and a **pid-fill** write *after* Popen. A `vq kill` can land
in either window.

* **Pre-Popen window** (between the STATE-1 re-read and the RUNNING-claim
  write): now wrapped in `spec_lock`, so the re-read → write is atomic. The
  v0.8.9 STATE-1 re-assert narrowed this; the lock closes it. Released
  before Popen.
* **Post-Popen window** (between the RUNNING-claim write and the pid-fill
  write, while the spec carries pid=None/pgid=None): a `vq kill` here writes
  KILLED but *can't signal* the just-spawned process (pgid wasn't recorded
  yet), and the old pid-fill write then clobbered KILLED back to RUNNING —
  leaving an **unkillable, untracked job** running while the spec lied as
  RUNNING. Now the pid-fill write re-reads under the lock: if a terminal
  label was set, the daemon reaps the just-spawned process group itself
  (SIGKILL — the operator already chose to kill it, nothing else would
  track it to escalate later) and preserves the label instead of clobbering
  it.

### EVENT-1 — dispatch failures are now observable

All five of `_start_job`'s early-FAILED paths (multi-user spec-gate reject,
leaked cgroup scope, gid resolution, privilege-drop failure, Popen failure)
now route through a new `_fail_dispatch(spec, reason)` helper that (a) takes
the lock + re-reads so a racing `vq kill` isn't clobbered to FAILED, and (b)
emits a `STATE_TRANSITION` event + records `failure_reason` — closing audit
item **EVENT-1**, where these failures previously wrote FAILED silently
(invisible in `events.jsonl`). Security note: the multi-user spec-gate
reject runs *before* `workspace.mkdir` with an attacker-controlled,
unvalidated `cwd`, so it uses `emit_event=False` — the FAILED spec write (to
the trusted queue dir) still happens, but no `events.jsonl` is written under
the untrusted path as root.

**Tests:** 2 new in `tests/test_spec_lock_wiring.py` —
`TestKillDuringPopenWindow` (a `vq kill` injected synchronously inside a
faked Popen lands in the post-Popen window; the spec stays KILLED, the job
is not tracked, and the spawned process is reaped) and
`TestDispatchFailureObservability` (a Popen failure emits the EVENT-1
transition + records a reason). The existing STATE-1 test
(`test_daemon.py`) covers the pre-Popen window. Suite: 2169 passed / 11
skipped on macOS.

`Patch-candidate: v0.10.x` — reliability + observability, additive. **Still
to wire** (next ship): the operator-state writers (`pause`/`resume`/
`throttle`), the `last_status_at` / `last_fetched_at` stamps, `cleanup`, and
`resubmit`.

---

## v0.8.11 — *Dekker's Mutex* — wire `spec_lock` into the terminal-transition writers (2026-06-04)

First of the `spec_lock`-wiring ships. The audit (`HANDOVER_VQ_AUDIT.md`)
named one architectural root cause behind its most severe findings: vq's
"one JSON file per job" store has many unsynchronized writers (the daemon
loop, its in-process RPC thread, every CLI verb), and
`paths.atomic_write_text` makes each individual write atomic against
*readers* but not against other *writers* — two writers each doing
read → mutate → write still lose updates. The canonical case is the one in
the `spec_lock` docstring: `vq kill` writes KILLED while the daemon's
`_record_finish` writes COMPLETED, and the daemon clobbers the kill — so
`vq kill` reports success while the spec lies as COMPLETED.

The `spec_lock` primitive (a per-spec sidecar `flock`) landed earlier
(`360ebb78`) but was inert — zero call sites. v0.8.11 wires it into the
**terminal-transition writers** so the lost-update race is closed for the
highest-severity path.

### Sites wired

Each wraps *only* the read → mutate → write in `with paths.spec_lock(p):`;
the webhook (network), the rerun spawn (workspace copy), and the workdir
cleanup (rmtree) all run *after* the lock releases — holding the lock across
them would block every other writer (and `vq status`).

* **`kill.py`** — the whole read → signal → write body (the `os.killpg` /
  `os.kill` signals are fast syscalls, safe under the lock).
* **`daemon._record_finish`** — the in-process job-exit path.
* **`daemon._record_orphan_finish`** — the orphan-reaper path. Re-reading
  the spec *inside* the lock also closes audit item **STATE-5** (it
  previously mutated a caller-supplied, possibly-stale spec).
* **`daemon._mark_aborted_by_queue`** — lock + re-read; a new `is_terminal`
  bail means it can no longer clobber a terminal label a racing writer set
  between the caller's read and the write.
* **`daemon._watchdog_pass`** (SIGTERM branch) — the CONC-3 re-read → write
  is now atomic; the SIGTERM itself is delivered after the lock releases.
* **`daemon` depends_on cascade** — the PENDING re-check → FAILED write is
  now atomic against a racing `vq kill`.

### The no-nest rule

`spec_lock` must never nest — a second `flock` on the same path from the
same process would deadlock. `_maybe_retry` writes the spec (state →
PENDING) and is called *only* from the two finish methods, which already
hold the lock, so it does **not** take the lock itself. Its docstring now
records this contract for future callers.

**Tests:** 2 new interleave tests in `tests/test_spec_lock_wiring.py`
(`vq kill` racing `_record_finish` / `_record_orphan_finish`, 50
barrier-synchronized trials each with a widened read→write window) assert
exactly one terminal state wins with no lost update — deterministically
green with the lock, and verified to fail without it. The primitive's own
no-lost-update tests live in `test_paths.py`. Suite: 2167 passed / 11
skipped on macOS.

`Patch-candidate: v0.10.x` — pure reliability fix, strictly additive (the
lock only adds mutual exclusion among the wired sites; un-wired writers
behave exactly as before). **Still to wire** (follow-up ships): the
`_start_job` dispatch-window write (the Popen sits between the RUNNING-claim
write and the pid-fill write — needs careful scoping), and the
operator-state writers (`pause/resume`/`throttle`), the `last_status_at` /
`last_fetched_at` stamps, `cleanup`, and `resubmit`.

---

## v0.8.10 — *Tarjan's Bridge* — remote `--chain` + `--rerun-until` (2026-06-02)

Closes the v0.8.7/v0.8.8 local-only gap. Operators running NEB
or DFT+U workflows on compute-d / compute-a can now use the same
queue primitives the local recipes use:

```sh
# 5-image NEB with per-image force-tolerance convergence,
# dispatched to compute-d.
vq submit compute-d \
    --chain 5 \
    --rerun-until '$VQ_WORKDIR/NEB_CONVERGED' \
    --rerun-max 20 \
    --cpus 16 --mem-mb 32000 \
    neb_image.py
```

### Same single-roundtrip pattern as v0.7.11

v0.7.11 (*Stroustrup's Stencil*) established the invariant:
one source upload, one SSH call — N specs get minted
**locally on the remote host** by its own `vq submit`. v0.8.10
extends that pattern to `--chain` and the v0.8.8 rerun fields.
The remote `vq` mints the chain group id and links the
`depends_on` chain locally, just like a direct local submit.

### Argv forwarding

* `chain > 1` → `--chain N` on the remote argv.
* `rerun_until_file_exists is not None` → `--rerun-until PATH`
  on the remote argv. Path passes through verbatim; the
  remote daemon substitutes `$VQ_WORKDIR` using its own
  workdir layout at check time.
* `rerun_max != 10` → `--rerun-max N` on the remote argv. The
  default value is the same on both sides, so don't bloat
  the argv in the common case.

### Mutex validated client-side

`submit_remote(chain=3, array=2)` raises `ValueError` before
any tar is built so operators get a clear error fast. Same
mutex contract as the local-side CLI (v0.8.7 ship).

### Length-check generalization

The post-call jobid count check used to assume `len(jobids)
== array`. v0.8.10 generalizes to `len(jobids) ==
max(array, chain)` since chain produces N jobids the same way
array does. The error message picks the right flag name
(`--chain N` vs `--array N`) so the operator's debugging
diff is unambiguous.

**Tests:** 9 new in `tests/test_submit_remote.py`
(`TestRemoteChain`: chain-1-no-flag, chain-N-single-call,
chain-array-mutex, chain-zero-rejected; `TestRemoteRerunUntil`:
rerun-until-verbatim, default-rerun-max-omitted, custom-
rerun-max-forwarded, no-rerun-no-flag, chain+rerun composed).
SSH transport mocked via the same fakes as the v0.7.11 array
tests. Suite: 2165 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive. Pre-v0.8.10
remote vq daemons that don't recognise `--chain` /
`--rerun-until` will print a click error from the remote
submit, which surfaces locally as a `transport.RemoteError`.
Operators submitting to fleet daemons still on v0.8.6 or
earlier should bump the fleet first.

---

## v0.8.9 — *Cook's Hierarchy* — `vq audit` CLI verb (2026-05-29)

Operator-visible payoff of the v0.8.6 *Codd's Audit* trail.
v0.8.6 wrote the data; v0.8.9 lets operators read it without
manually `tail`-ing the JSONL file.

### Forms

* `vq audit` — text table of the last 100 entries.
* `vq audit --json` — raw JSONL (one envelope per line) for
  scripting + `jq` pipelines.
* `vq audit --since DUR` — filter to entries newer than DUR
  (`1h`, `30m`, `7d`, same format as `--duration` elsewhere).
* `vq audit --uid N` — caller-uid filter.
* `vq audit --method NAME` — method filter. Trailing `*`
  means prefix glob (`--method set_*` matches every set_
  method audited).
* `vq audit --tail N` — cap output after filters (default 100).
* `vq audit HOST` — SSH-delegate to HOST.
* `vq audit --all-hosts` — parallel fan-out via v0.7.6.

### Filters compose

The four predicates (`--since`, `--uid`, `--method`, `--tail`)
all apply in sequence, with `--tail` last so the operator
sees the most recent N entries that matched. Useful forensics:

* "Who drained the queue at 03:14?" →
  `vq audit --since 4h --method set_drain_state`
* "What's everyone been doing to throttle this week?" →
  `vq audit --since 7d --method set_throttle_state`
* "Show me the failed admin-status writes" →
  `vq audit --method set_admin_status --json | jq 'select(.ok == false)'`
* "Who's been clearing throttle most often?" →
  `vq audit --since 7d --method set_throttle_state --json | jq 'select(.args_summary == "clear") | .uid' | sort | uniq -c`

### Text vs JSON

Text mode renders one line per entry:
`<ts>  <ok|FAIL>  uid=<N>  <method:24>  <args_summary>  [| error: ...]`

JSON mode emits raw envelopes (one per line), schema stable
with the on-disk format: `{ts, method, uid, ok, args_summary,
error?}`. Empty trail in text mode prints `(no audit entries
match the filter)`; in JSON mode prints nothing (scripts can
detect with `wc -l`).

### Internals

No new daemon surface. The verb reads the existing
`rpc-audit.jsonl` via `vq.audit.read_audit_log` and filters
in-process. SSH delegation reuses `_delegate_to_remote`; the
`--all-hosts` path uses `_aggregate_per_host` (same plumbing
as `vq daemon ping --all`).

**Tests:** 16 new in `tests/test_audit_cli.py` —
`TestDefaultOutput`, `TestJsonOutput` (schema preservation),
`TestFilters` (uid, method exact + prefix glob, since,
compose), `TestTail` (cap behaviour, zero rejected),
`TestEmptyTrail` (friendly text vs empty JSON),
`TestAllHostsMutex`. Suite: 2156 passed / 11 skipped on
macOS.

`Patch-candidate: v0.10.x` — strictly additive new verb. No
spec changes, no daemon changes, no new on-disk surface.

---

## v0.8.8 — *Turing's Halt* — `vq submit --rerun-until FILE` (2026-05-29)

Companion to v0.8.7's `--chain N`. v0.8.7 lets the operator
spawn N iterations up front; v0.8.8 lets the operator iterate
until a *convergence flag* appears in the workdir, with a
safety cap. Together they cover the NEB + DFT+U reaction
workflow shape end to end.

### The primitive

`vq submit --rerun-until '$VQ_WORKDIR/CONVERGED' script.py` —
on every COMPLETED terminal transition, the daemon checks
the path. If the file exists, the loop is done. If it's
missing AND `rerun_count < rerun_max` (default 10), the
daemon spawns a fresh clone of the spec with `rerun_count++`,
`depends_on=[this jobid]`, and a fresh workspace copy.

The `$VQ_WORKDIR` token is substituted at check time from
the spec's actual workdir — so scripts can write the flag
file next to their other output without needing to know the
queue's path layout.

### Schema additions

* **Spec:** `rerun_until_file_exists: str | None`,
  `rerun_max: int = 10`, `rerun_count: int = 0`.
* **CLI:** `--rerun-until PATH`, `--rerun-max N`.
* **Daemon env at dispatch:** `VQ_RERUN_COUNT`, `VQ_RERUN_MAX`.

### How DFT+U uses it

A U-self-consistency loop where the script reads the prev
iteration's U value, refines, and writes the new one (plus
the convergence flag if `|U_new - U_old| < tol`):

```sh
vq submit \
    --rerun-until '$VQ_WORKDIR/CONVERGED' \
    --rerun-max 15 \
    --cpus 16 --mem-mb 32000 \
    dft_u_iter.py
```

Inside `dft_u_iter.py`:

```python
import os, pathlib, json
workdir = pathlib.Path(os.environ["VQ_WORKDIR"])
iter_n = int(os.environ.get("VQ_RERUN_COUNT", "0"))
if iter_n == 0:
    u_prev = SEED_U
else:
    # The prev iteration's clone wrote here — find via depends_on.
    u_prev = read_prev_u()
u_new = solve_scf_and_extract_u(u_prev)
(workdir / "u.json").write_text(json.dumps({"u": u_new}))
if abs(u_new - u_prev) < TOL:
    (workdir / "CONVERGED").touch()  # daemon stops respawning
```

### How NEB uses it

NEB image-by-image with per-image force-tolerance
convergence. Combine v0.8.7 `--chain N` for the image
sequence with v0.8.8 `--rerun-until` per image:

```sh
# Each chain element iterates until its NEB_CONVERGED appears.
vq submit \
    --chain 5 \
    --rerun-until '$VQ_WORKDIR/NEB_CONVERGED' \
    --rerun-max 20 \
    neb_image.py
```

The composition: image k waits for image k-1 (chain
semantic), and image k itself iterates until its force-
tolerance is met (rerun semantic).

### Safety: failure ≠ "try again"

FAILED terminal transitions do NOT trigger reruns. Failure
is the wrong-signal that the script's done something wrong;
the operator wants to read the logs and fix it, not have
the queue loop on a broken script. Operators wanting
genuine failure retries already have `--retry N` (v0.5.31).

When the cap is hit (`rerun_count == rerun_max`) the daemon
logs a WARNING ("rerun-until flag MISSING but rerun_count
== rerun_max; not respawning") and the chain ends in the
COMPLETED state of the last iteration. The operator can
bump `--rerun-max` and resubmit the last jobid via
`vq resubmit` if more iterations are needed.

### Internals

The daemon hook `_maybe_spawn_rerun(spec)` runs after a
COMPLETED transition, *before* `_maybe_cleanup_workdir`
(so the rerun spawn can still read the workdir if the flag
is checked there). The helper `_spawn_rerun_clone(spec)`
mints a new jobid, `shutil.copytree`s the source cwd to a
fresh workspace under `jobs_dir`, builds a new JobSpec
mirroring the original (with the rerun fields incremented),
and writes it to the queue.

Best-effort: any failure during the spawn (workspace copy
fails, queue dir unwritable) logs a WARNING but doesn't
propagate. The original spec's terminal record stands; the
operator can manually `vq resubmit JOBID` if they want.

**Tests:** 12 new in `tests/test_rerun_until.py`
(`TestSpecRerunFields`, `TestMaybeSpawnRerun` including
flag-present-no-spawn / flag-absent-spawn / cap-reached-no-
spawn / FAILED-no-spawn / $VQ_WORKDIR substitution / count
increment, `TestSubmitRerunCLI` for the flag parse). Suite:
2124 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive: pre-v0.8.8
specs read clean (rerun fields default safe), and only opt-in
when the operator passes `--rerun-until`.

---

## v0.8.7 — *Hoare's Triple* — `vq submit --chain N` (2026-05-29)

First of the chemistry-workflow primitives for v0.8.x: a verb
that supports NEB image-by-image and DFT+U self-consistency
iteration patterns directly. The operator framing
(2026-05-29): "make sure reactions with NEB and DFT+U are
covered." `--chain` is the foundational primitive both
workflows use; `--rerun-until-file` (v0.8.8 companion ship)
covers the convergence-flag termination side.

### The primitive

`vq submit --chain N script.py` spawns N near-identical
specs linked by `depends_on` so they run strictly in sequence.
Element k starts only after element k-1 has reached COMPLETED;
if k-1 fails, k..N-1 cascade-fail without dispatching
(inherits the v0.6.51 `--depends-on` semantic).

Each element gets:

* Its own jobid + workspace (full source copy per element —
  same trade-off as `--array`).
* `chain_index` 0..N-1, `chain_total` N, `chain_group_id`
  (shared 8-hex tag) on the spec.
* Daemon-injected env vars at dispatch:
  `VQ_CHAIN_INDEX`, `VQ_CHAIN_TOTAL`, `VQ_CHAIN_GROUP_ID`.

The script branches on `VQ_CHAIN_INDEX` to know its
iteration: for an NEB image-by-image script, k=0 reads the
initial geometry while k>0 reads the prev iteration's
relaxed output. For a DFT+U self-consistency loop, the
script reads the prev U estimate and writes a refined one.

### How NEB uses it

A 5-image NEB sweep where each image initialises from the
prev:

```sh
vq submit --chain 5 \
    --cpus 8 --mem-mb 16000 \
    neb_image.py
```

Inside `neb_image.py`:

```python
import os
k = int(os.environ["VQ_CHAIN_INDEX"])
N = int(os.environ["VQ_CHAIN_TOTAL"])
gid = os.environ["VQ_CHAIN_GROUP_ID"]
if k == 0:
    geom = load_initial()
else:
    # Read prev image's relaxed geometry from its workdir.
    prev_workdir = find_workdir(chain_index=k-1, gid=gid)
    geom = read_relaxed(prev_workdir)
relaxed = optimise(geom)
save_to_workdir(relaxed)
```

### How DFT+U uses it

A self-consistent U loop with chain=10 (enough to converge
the metal-oxide test case the chemistry chat reports):

```sh
vq submit --chain 10 \
    --cpus 16 --mem-mb 32000 \
    dft_u_iter.py
```

Inside `dft_u_iter.py`: read the prev iteration's U value,
solve the SCF, output the refined U. v0.8.8's
`--rerun-until-file` will give this the "stop when
converged" termination; today the chain runs all N
iterations and the operator checks the last workdir for
the convergence trace.

### Distinct from `--array`

| | `--array N` | `--chain N` |
|---|---|---|
| Dispatch | All N independent + parallel | Strict sequence, one at a time |
| Failure | k's failure doesn't affect others | k's failure cascade-fails k+1..N-1 |
| Use case | Independent parameter sweep | Iterative refinement |
| Env vars | `VQ_ARRAY_*` | `VQ_CHAIN_*` |

The CLI rejects `--array` combined with `--chain` because
they're orthogonal abstractions; pick one.

### Composition

Composes with `--depends-on`: the user-supplied predecessor
list propagates to **every** chain element (the user dep is
gated independently of the chain link). Element 0 carries
only the user dep; element k>0 carries the user dep plus
the chain[k-1] dep.

### Internals

New `submit_local_chain` factory mirrors `submit_local_array`
in shape. CLI dispatch in `cli.submit` checks `chain > 1`
before `array > 1`. New `new_chain_group_id()` helper
matches `new_array_group_id()`. The daemon's `_start_job`
env-injection block (`daemon.py` around L2076) adds the
three `VQ_CHAIN_*` vars next to the existing `VQ_ARRAY_*`
block.

### What this isn't (yet)

* **Not chain --resubmit:** if you want "re-run the chain
  from element k after fixing the input," `vq resubmit
  JOBID_k` works on a single element; resubmitting the
  whole chain isn't a single-verb operation today.
* **Not array-of-chains:** chain[0..N-1] is linear. The
  "parallel batches with chain between batches" shape
  (e.g. multi-replica NEB) needs separate orchestration —
  v0.11.0 could add `--chain-array M N` if a real use case
  surfaces.
* **Not remote --chain:** local-only first pass. Remote
  delegation would need a `submit_remote_chain` mirroring
  v0.7.11's `submit_remote_array` single-roundtrip; small
  ship for v0.8.9 if operators report wanting it.

**Tests:** 13 new in `tests/test_chain.py`
(`TestSpecChainFields`, `TestSubmitLocalChain`,
`TestSubmitChainCLI`, `TestChainEnvFieldsPresent`). Suite:
2112 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive new flag +
new spec fields. Pre-v0.8.7 specs read clean (all chain
fields default None).

---

## v0.8.6 — *Codd's Audit* — multi-user RPC audit-trail (2026-05-29)

Multi-user hardening capstone for the v0.8.x RPC arc.
Compliance + forensic value: every `set_*` RPC call leaves a
trail, so operators can answer "who drained the queue at 03:14?"
or "what was the last admin-status update for vibeqc-release,
by whom?". Append-only JSON-Lines.

New module `vq/audit.py` factored out of the RPC layer so
monitoring scripts can `from vq.audit import read_audit_log`
without pulling in the socket protocol code.

### What gets logged

Only **mutating** methods (anything starting with `set_`). Reads
(`ping`, `get_methods`, `get_admin_status`, `get_drain_state`,
`get_throttle_state`) are NOT logged — high-volume, not
sensitive, and would drown out the signal.

### File location

* single-user → `<state_root>/rpc-audit.jsonl`
* multi-user → `<multi_user_root>/rpc-audit.jsonl`

Same shape as the rest of the daemon's state files (mirrors the
v0.8.1 path-mapping policy).

### Schema

One JSON object per line. Stable fields:

* `ts` — ISO 8601 UTC timestamp.
* `method` — the RPC method name.
* `uid` — caller's Unix uid via `SO_PEERCRED`, or `null` on
  platforms without it (macOS dev boxes) or on lookup failure.
* `ok` — `true` if the handler succeeded.
* `args_summary` — short rendered summary (env name, set-vs-
  clear, weight value). **Tokens and full state dicts are
  NEVER included.**
* `error` — present only when `ok=false`.

### Failed calls still audit

A `set_drain_state` that errors mid-handler still leaves a
line with `ok=false` and the error message — the failure
attempt is itself forensically interesting (e.g. an
unauthorized caller trying to set state in multi-user mode).

### Token redaction

The whole point of having an audit trail is being able to
share it with security/audit teams without leaking
credentials. The args-summary writer explicitly omits the
`token` arg of every set_* call, and forward-compat
catch-all handling for unknown set_* methods also redacts
`token` keys.

### Rotation

None in v0.8.6 — append-only. Each entry is ~200 bytes; even
at 1000 writes/day the file grows ~70KB/year. If a future
deployment needs rotation, v0.5.16's cron-archive pattern
applies.

### Performance

The audit append happens after the handler returns, so it
doesn't block the RPC response. On a write failure (disk
full, permission error) the audit module logs a WARNING but
the RPC call still completes — the audit log is a forensic
aid, not a correctness gate.

**Tests:** 18 new in `tests/test_audit.py`
(`TestAuditPath`, `TestSetMethodsAreAudited`,
`TestReadMethodsAreNotAudited`, `TestFailedCallsLogged`,
`TestSchemaStable` including token-redaction proof,
`TestAuditModuleAPI` including corrupt-line resilience).
Suite: 2099 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive new file. The
audit module is best-effort (write failures don't break
existing semantics), so the surface is small.

---

## v0.8.5 — *Knuth's Concrete* — drain/throttle `--all` (2026-05-29)

Mirror of v0.8.3's fan-out pattern for the mutating drain +
throttle verbs. v0.8.3 gave `vq daemon ping` fleet-wide reach;
v0.8.5 does the same for the verbs that actually *change* state.

Operator-impactful: today a fleet-wide drain for a maintenance
window means ssh into each box and run `vq drain`. v0.8.5 does
it in one shell: `vq drain --all --reason "datacenter cooling"`.
The matching release is `vq drain --all --release`.

### `vq drain --all`

New `--all` flag (mutually exclusive with positional HOST).
Applies the same verb to every host in
`~/.config/vq/config.toml` in parallel via the v0.7.6 fan-out.
Forms:

* `vq drain --all` — full drain across the fleet.
* `vq drain --all --release` — clear drain on every host.
* `vq drain --all --status` — aggregate per-host drain views.
* `vq drain --all --max-jobs N` — fleet-wide partial drain.
* `vq drain --all --reason TEXT --duration DUR` — annotated
  + auto-released bounded window.

Per-host failures land inline as one-line FAIL banners stacked
under the host's banner — same shape as `vq queue --all` —
so partial-fleet outcomes are visible.

### `vq throttle --all-hosts`

Separate flag name (`--all-hosts`, not `--all`) because
`--all` already means "every running job on ONE host". The
fan-out applies only to the persistent-throttle subset:
`--status`, `--release-persist`, `--persist --weight N`.

Per-job throttle ops (`--weight N JOBID`, `--restore JOBID`)
are rejected with `--all-hosts` because jobids are host-local
— there's no meaningful "throttle job X across every host"
operation. The error surfaces upfront with the constraint
spelled out.

### Local-arm optimisation

For the local host the fan-out calls the `drain` /
`throttle` module primitives directly instead of round-
tripping through a `vq` subprocess. Saves a process spawn +
config reload + click parsing on every `--all` invocation;
the remote arm continues to use `_delegate_to_remote` over
SSH.

**Tests:** 13 new in `tests/test_drain_throttle_all_hosts.py`
(drain `--all`-and-HOST mutex; drain `--all` empty-config
graceful; drain `--all` set aggregates per-host + writes
local; drain `--all --release` clears local + delegates
remote; drain `--all --status` aggregates; drain `--all`
per-host failure isolation; throttle `--all-hosts`-and-HOST
mutex; throttle `--all-hosts` per-job-op rejected; throttle
`--all-hosts --status` aggregates; throttle `--all-hosts
--release-persist` clears local; throttle `--all-hosts
--persist` writes local state; throttle `--all-hosts`
per-host failure isolation; throttle `--all-hosts --persist`
requires `--weight`). SSH mocked via `monkeypatch.setattr`
on `transport.run_remote_vq`. Suite: 2081 passed / 11
skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive new flag on
existing verbs; no behaviour change for single-host calls.

---

## v0.8.4 — *Brooks's Mythical* — RPC method introspection (2026-05-29)

Forward-compat probe for the v0.8.x RPC: a new built-in
`get_methods()` RPC returns the set of registered method names
+ the daemon version + multi-user flag. Clients can ask the
daemon "what do you support?" before calling a method that
might not exist.

The use case is real client-server version skew: a v0.8.5
client (post-v0.8.5's drain-all RPC method, say) calling
`set_drain_all_state` against a v0.8.3 daemon would today get
back `RPCError: unknown method: 'set_drain_all_state'
(known: [...])`. That's recoverable by parsing the error
string, but fragile. With `get_methods()` the client can ask
upfront and either downgrade or fall back without the
error-string parsing.

CLI surface: `vq daemon ping --verbose` (`-v`) calls
`get_methods` after the ping. The envelope gains a `methods`
field:

* **Text:** appended as ``| methods=[ping, get_methods,
  get_admin_status, ...]``.
* **JSON:** `methods: [...]` as a sorted list, or `methods:
  null` if the get_methods call failed (e.g. pre-v0.8.4
  daemon that doesn't recognise the method).

Methods are sorted server-side so monitoring scripts see
stable diffs across daemon restarts.

The `get_methods` registration is built into the `RPCServer`
constructor next to `ping` — every server has both built-ins
regardless of which method-set bundles
(`register_admin_status_methods`, `register_drain_methods`,
`register_throttle_methods`) are wired in. No daemon code
change beyond the constructor registration line — the daemon
auto-inherits it.

**Tests:** 4 new in `tests/test_rpc.py`
(`TestGetMethodsIntrospection` — sorted list, version field,
default-registered, multi-user open) + 4 new in
`tests/test_daemon_ping.py` (`TestDaemonPingVerbose` — text
shows methods, JSON envelope has methods field, non-verbose
preserves v0.8.2 envelope shape, pre-v0.8.4 daemon graceful
fallback). Suite: 2068 passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — strictly additive new method +
new CLI flag, no behaviour change to existing surface.

---

## v0.8.3 — *Dijkstra's Shortest* — remote ping + `--all` (2026-05-30)

v0.8.2 shipped the local-only `vq daemon ping`. v0.8.3
extends the verb to remote hosts and parallel fleet-wide
queries, wiring it to the v0.7.6 *Tanenbaum's Mailbox* fan-out
plumbing that already powers `vq admin auto-update --all-hosts`
and `vq queue --all`.

Three new forms:

* `vq daemon ping HOST` — SSH-delegate to HOST and parse the
  remote's JSON envelope back. Exit code propagates: a remote
  envelope with `ok=false` exits the local CLI with 1, so CI
  scripts can gate on it.
* `vq daemon ping --all` — parallel fan-out over every host in
  `~/.config/vq/config.toml`. Always exits 0 — per-host
  failures land inline so monitoring scripts can parse the
  aggregate.
* `vq daemon ping --all --json` — top-level dict keyed by host.
  Per-host SSH failures land as `{"error": "..."}` (same shape
  the v0.7.6 `_aggregate_per_host_json` helper produces for
  the other `--all` verbs).

Internals: the v0.8.2 verb body was 80 lines of ping-then-render.
v0.8.3 factors that into `_local_daemon_ping(timeout) -> (exit,
envelope)` and `_format_ping_text(envelope) -> str`. The remote
path reuses `_delegate_to_remote` (which wraps
`transport.run_remote_vq`); the `--all` path reuses
`_aggregate_per_host` / `_aggregate_per_host_json`. No new
plumbing — Dijkstra's algorithm reduces to the cheapest path
that already exists.

**Tests:** 6 new in `tests/test_daemon_ping.py` —
HOST-and-`--all`-mutual-exclusion, `--all` with no hosts
configured, `--all` aggregating local+remote JSON, `--all`
isolating one bad host, remote single-host exit-code
propagation, remote text path. SSH transport mocked via
`monkeypatch.setattr(transport, "run_remote_vq", ...)` so
tests don't need real SSH. Suite: 2060 passed / 11 skipped on
macOS.

`Patch-candidate: v0.10.x` — same surface as v0.8.2, additive
on top.

---

## v0.8.2 — *Lamport's Logical* — `vq daemon ping` (2026-05-30)

The v0.8.0+v0.8.1 RPC layer needed a user-facing payoff: a
single command that proves the daemon is actually responsive
without parsing systemd output, without doing a full lifecycle
audit, and without confusion about what "running" means when
the pidfile lies. v0.8.2 ships that command.

New verb: `vq daemon ping`. Sends one `ping` RPC call to the
local daemon socket, prints `daemon RPC: ok | version=…
| multi_user=… | socket=… | latency=…ms` on success.

Exit codes are stable + scripted-friendly:

* **0** — daemon reachable; RPC responded.
* **1** — socket missing (daemon down or not started yet).
* **2** — socket present but daemon didn't respond cleanly
  (protocol error, timeout, crash mid-handshake).

`--json` emits the monitoring-script envelope
`{ok, version, multi_user, socket_path, latency_ms, error}`.
Schema is pinned by a coverage test so a future field
addition is intentional.

`--timeout SECONDS` (default 2.0) bounds the wait — a hung
daemon should fail loud, not block the caller. Short by
design.

Distinct from existing verbs:

* `vq daemon status` checks the pidfile. Reads cheap, but
  the pidfile can lie if the daemon crashed mid-process and
  systemd hasn't yet noticed.
* `vq daemon health` does the full lifecycle contract
  verification — systemd unit, scope, cgroup, etc. Heavy
  (multi-second on a busy host) and answers a different
  question.
* `vq daemon ping` is the smallest possible "is it actually
  responsive?" probe — uses the RPC the daemon's own clients
  use, so a successful ping proves the same thing those
  clients would see.

Local-only first pass. Remote-host ping (`vq daemon ping
HOST` via SSH transport) is a natural v0.8.3 follow-up
once monitoring-script use cases surface.

**Tests:** 7 new in `tests/test_daemon_ping.py` —
reachable-text, reachable-json, latency-present, exit-1 on
no-socket, json-envelope on no-socket, short-timeout still
fails fast on no-socket, json-schema pin. Suite: 2054
passed / 11 skipped on macOS.

`Patch-candidate: v0.10.x` — small surface, no backward
compat concern, immediately useful in operator workflow.

---

## v0.8.1 — *Karp's Reduction* — RPC for drain + throttle state (2026-05-30)

v0.8.0 *Dahl's Simula* shipped the daemon-side RPC for
`admin-status.json`. The v0.7.12 audit pinned three daemon-state
files affected by the same user-XDG vs daemon-XDG split:
`admin-status.json` (fixed in v0.8.0), `drain.json`, and
`throttle.json`. v0.8.1 brings the latter two onto the same RPC.

`drain.json` was the worst offender — pre-v0.8.1 it had no
multi-user path mapping at all (`drain_state_path()` returned
`state_root() / "drain.json"` unconditionally). In multi-user
mode an admin user running `vq drain` wrote
`~/.local/share/vq/drain.json` while the root daemon read
`/var/lib/vq/drain.json` — every `vq drain` call from the CLI
was a silent no-op against dispatch. v0.8.1 fixes the path
*and* routes through the daemon's RPC so the canonical file
gets written regardless of which admin-group caller invoked
the CLI.

`throttle.json` had the multi-user path mapping fix back in
v0.6.37 but the file was still root-owned in multi-user — the
CLI required `sudo` to write it. RPC routing now lets the
daemon's accept-loop do the write under root, while any
admin-group caller (no `sudo` required) can request it via the
RPC + admin-token gate.

New RPC methods (all registered on the same daemon-side
`RPCServer` as `admin-status`):

* `get_drain_state()` — returns the drain dict (the
  `DrainState.model_dump()` shape) or `None` if no drain.
  Open — operator-visible, not sensitive.
* `set_drain_state(state, token=None)` — write a drain state.
  `state=None` clears (same as `vq drain --release`).
  Multi-user requires the admin token.
* `get_throttle_state()` — returns the persistent throttle
  dict or `None`. Open.
* `set_throttle_state(state, token=None)` — write or clear
  (`state=None`). Multi-user token-gated.

Plumbing pattern mirrors v0.8.0's `read_admin_status`:
`drain.read_drain_state(via_rpc=True)` is the new default for
CLI callers; `drain.write_drain_state(state, via_rpc=True)`
and `drain.clear_drain(via_rpc=True)` likewise. Daemon-internal
callers (`Daemon._dispatch_pending` reading drain on every
iteration; `apply_persistent_throttle_if_set` reading throttle
from `_start_job`) pass `via_rpc=False` to skip the self-RPC
roundtrip. Same with throttle's read/write/clear.

`DrainState.model_validate` with extra=forbid would crash on
forward-compat fields written by a newer client; the RPC
`set_drain_state` handler strips unknown keys before validation
(same pattern as `set_admin_status`).

**Tests:** +27 in `tests/test_rpc.py` (`TestDrainStateRoundtrip`,
`TestDrainViaRpcPlumbing`, `TestDrainMultiUserAuth`, the three
throttle counterparts, plus `TestAllRegisteredTogether` pinning
that admin-status + drain + throttle handlers coexist on one
server without method-name collision). Suite: 2047 passed / 11
skipped on macOS.

**Compatibility:** strictly additive. Pre-v0.8.1 callers that
didn't pass `via_rpc=` get the new default but fall back to the
direct file path when the daemon is down — single-user mode
sees identical behaviour, multi-user mode picks up the
canonical-file fix on a daemon restart.

`Patch-candidate: v0.10.x` — closes the multi-user drain footgun
and the throttle-needs-sudo ergonomic both with the same
mechanism v0.10.1 already accepts (v0.8.0). Built on top of:

---

## v0.8.0 — *Dahl's Simula* — daemon-side RPC for admin-status (2026-05-30)

The v0.7.12 *Wirth's Modula* audit doc documented an explicit
v0.7.1 footgun: ``admin-status.json`` lives in two places when
the daemon runs in multi-user mode — the operator's user-side
``vq admin status`` reads ``~/.local/share/vq/admin-status.json``
while the daemon-side ``vq admin auto-update`` (running under
the multi-user systemd unit) writes
``/var/lib/vq/admin-status.json``. Two parallel views, easy
to silently diverge, hard to diagnose.

The user picked "Daemon-side RPC" as the long-term clean fix
on 2026-05-28. v0.8.0 ships it.

New module ``vq/rpc.py``. Wire protocol:

* Unix domain socket
  (``state_root/daemon.sock`` single-user;
   ``multi_user_root/daemon.sock`` multi-user)
* Line-delimited JSON: ``{"method": str, "args": dict}`` →
  ``{"ok": bool, "result"|"error": ...}``
* One connection = one request + response = close. No
  long-lived state, easy to reason about.

Permissions:

* Single-user: socket file is **0600** (owner-only). Kernel
  enforces; no other user can connect.
* Multi-user: socket file is **0660** owned by the
  ``admin_group`` (from ``[multi_user]`` config). Any admin
  can connect; write methods require the admin token via the
  ``token`` arg.

Methods shipped:

* ``ping()`` — health check returning vq version + mode.
* ``get_admin_status()`` — returns ``dict[env, record-dict]``.
  Open (read-only, not sensitive).
* ``set_admin_status(env, record, token=None)`` — write one
  env's record. Multi-user requires the admin token.

Daemon integration: ``Daemon.run()`` starts the RPC server
thread after the dispatch setup, stops it on shutdown. Failure
to start the RPC is logged but doesn't block daemon startup —
CLI clients fall back to direct file access in that case (with
a WARNING in multi-user mode about the divergence risk).

Client integration: ``admin.read_admin_status()`` and
``admin.write_admin_status()`` gain a ``via_rpc: bool`` kwarg
(default ``True``). User-facing CLI paths pick up the RPC
route automatically; internal daemon-side callers pass
``via_rpc=False`` to avoid recursing back into the RPC.

Fallback semantics:

* ``rpc.try_rpc_or_fallback()`` is the canonical helper.
  Tries RPC first; on any failure runs the supplied fallback.
* Single-user fallback: silent (the user's file IS the
  daemon's file — no divergence possible).
* Multi-user fallback: logs WARNING naming the socket path
  and explaining the stale-view risk. Operator can decide to
  restart the daemon.

Threading: server uses one accept-loop thread; each accepted
connection is handled inline (sequentially). For the
read-heavy admin-status workload this is plenty. If a future
method does long work, revisit with per-connection threads.

Why a minor-version bump instead of v0.7.19: the architectural
shift from "daemon owns nothing the CLI cares about" to
"daemon mediates state access" is a real fence in the codebase
contract. Future ships can add more RPC methods (queue
listing, status, anything that benefits from a canonical view)
without re-litigating "is there an RPC?". The bump marks the
fence.

Codename *Dahl's Simula*: Ole-Johan Dahl co-invented Simula
(the first OO language). Simula introduced the idea of
encapsulating state behind a process-like object boundary —
which is exactly what the RPC does for ``admin-status.json``.
Dahl's name on a ship that adds the first true IPC boundary
to vq is on-theme.

Test suite: **2020 passed** (+27 new in ``tests/test_rpc.py``:
socket path resolution, server start/stop + stale-socket
recovery + double-start idempotence, ping round-trip,
protocol error paths (unknown method, malformed JSON,
handler exception, bad args, missing socket), fallback
helper (single-user silent + multi-user WARNING),
admin-status round-trip with unknown-field stripping,
``via_rpc`` plumbing through ``read/write_admin_status``,
multi-user auth (write rejected without token, read open),
concurrent ping safety), 11 skipped.

---

## v0.7.18 — *Kay's Object* — `vq overview --recommend` (2026-05-28)

Fleet operators with multiple hosts wanted an answer to "which
host should I submit to?". v0.6.21's `vq overview` already
showed the per-host state, but the operator had to eyeball the
output and pick. v0.7.18 ships a compact ranking:

```sh
vq submit $(vq overview --recommend) my.py
```

`--recommend` ranks reachable + healthy + non-drained hosts by:

1. `running_cpus + pending_cpus` (total workload — least
   loaded wins). Smaller = more capacity for new work.
2. `pending_cpus` (waiting work — tiebreaker preferring
   hosts with empty queues).
3. `-idle_seconds` (idle hosts beat busy ones at the same
   workload — operator probably wants to wake them up).
4. Host name (deterministic tiebreaker).

Filters out:

* Unreachable hosts (no network).
* Drained hosts (`vq drain` is set — they won't dispatch).
* Dead-daemon hosts (`daemon_health.ok` is False).

Exits non-zero when no host qualifies — operator sees the
shell pipeline fail loudly rather than submitting to a
zombie host.

Two new HostOverview fields back the ranking:

* `running_cpus: int = 0` — sum `spec.cpus` across RUNNING
  specs.
* `pending_cpus: int = 0` — sum across PENDING specs.

(SUSPENDED specs hold no CPU per v0.6.20 watchdog semantics
— excluded from both tallies.)

Best-effort: the workload tally is a proxy, not true
remaining capacity (we don't surface `cpus_total` in the
overview path). Operator with strict per-job CPU requirements
should cross-check via `vq overview HOST`. A future ship can
extend this to `--recommend --cpus N` filtering once
`cpus_total` is in HostOverview.

JSON round-trip: the new fields land in
`format_overview_json` and load on the remote-overview path
(``gather_overview_remote``). Pre-v0.7.18 hosts return
missing keys → treated as 0 for backward compat.

Codename *Kay's Object*: Alan Kay, OO — "objects hide their
state and expose behavior." The recommend verb hides the
ranking algorithm behind a single value the operator can
compose with via shell substitution. The chooser doesn't
need to know what the daemon's load looks like; the verb
encapsulates that.

Test suite: **1993 passed** (+14 new in
`tests/test_overview_recommend.py`: load-field accounting
includes RUNNING + PENDING, excludes terminal + SUSPENDED;
empty fleet returns None; unreachable / drained / dead-daemon
hosts filtered; ranking honors workload-asc / pending-asc /
idle-desc / name-asc tiebreak chain; JSON round-trip carries
the load fields), 11 skipped.

---

## v0.7.17 — *Postel's Robustness* — notify_on_states filter (2026-05-28)

v0.5.35 shipped webhook notifications with one knob:
``webhook_url``. The doc explicitly said "Future versions may
add a state filter (e.g. notify only on FAILED / OOM_KILLED,
not COMPLETED)". For a busy queue that's exactly the gap —
30-element array submit + every-state fires = 30 Slack pings
in 5 minutes, and the operator can't tell from the channel
which (if any) failed.

v0.7.17 ships the filter. ``NotificationConfig`` gains
``notify_on_states: list[str]``. The canonical "alert only on
failure" config is now a 6-line TOML block:

```toml
[notifications]
webhook_url = "https://hooks.slack.com/..."
notify_on_states = ["failed", "oom_killed", "starved",
                    "time_exceeded", "killed",
                    "aborted_by_queue"]
```

Two design choices:

* **Empty list = no filter** (backward-compat default). Every
  pre-v0.7.17 config keeps working unchanged; opting in is
  additive only.
* **Validation at config load.** An entry that doesn't match
  a valid terminal-state name fails the config parse with
  ``ConfigError`` naming the bad value. Operator hears about
  typos immediately, not after silently dropping
  notifications for hours. Case-insensitive (``"FAILED"`` and
  ``"failed"`` both work, lowercased at parse); duplicates
  deduped; PENDING and other non-terminal states rejected
  (the filter is specifically about terminal-state
  notifications).

Implementation surface:

* ``vq/config.py`` — new field + ``field_validator``.
* ``vq/notify.py`` — ``send_terminal_notification`` gains
  ``notify_on_states: list[str] | None = None`` kwarg.
  ``None`` / empty preserves the no-filter path.
* ``vq/daemon.py`` — Daemon plumbs the config field through
  to every ``notify.send_terminal_notification`` call site
  (5 of them: ``_record_finish`` happy + already-terminal
  branches, ``_record_orphan_finish``,
  ``_mark_aborted_by_queue``, ``_emit_pending_notification``).
* ``vq/cli.py`` — ``vq daemon run`` loads the field from
  config and passes it to Daemon's constructor; logs
  ``notifications: webhook configured (filter: [...])`` at
  daemon-start so the operator can verify the filter loaded
  as expected.

Codename *Postel's Robustness*: Jon Postel's "be conservative
in what you send" half of the robustness principle (the
"liberal in what you accept" half doesn't fit here). The
filter implements exactly that — the operator narrows the
webhook stream to what they actually want to hear.

Test suite: **1979 passed** (+11 new in
``tests/test_notify.py``: 6 filter behavior tests +
5 config validation tests covering case normalization,
unknown-state rejection, dedup, empty-default,
non-terminal-state rejection), 11 skipped.

---

## v0.7.16 — *Codd's Tuple* — `vq submit --time-limit HH:MM:SS` (2026-05-28)

vq already had ``--wall-time-seconds`` from v0.4; the spec
field has always been ``JobSpec.wall_time_seconds``. But every
HPC operator (SLURM, PBS, LSF, …) has muscle memory for
``--time HH:MM:SS`` — typing the integer-seconds form requires
math the operator doesn't want to do at 11 PM on a Friday.

v0.7.16 adds the ergonomic flag:

```sh
vq submit ... --time-limit 01:30:00 ...
vq submit ... --time 90:00 ...      # alias matches SLURM sbatch
vq submit ... --time 5400 ...       # plain seconds also accepted
```

All three forms set the same spec field as
``--wall-time-seconds``; the two flags are mutually exclusive.
Accepted parses:

* **HH:MM:SS** — three colon-separated parts. MM and SS must
  be < 60 (the conventional clock form).
* **MM:SS** — two parts. SS must be < 60; MM may be any non-
  negative integer (so ``90:00`` = 1h30m works the SLURM way).
* **Plain integer seconds** — matches existing
  ``--wall-time-seconds`` semantics, useful when composing
  with shell arithmetic.

Rejected with precise error messages (no silent fallback):
empty value, four-part colon split, MM/SS ≥ 60 in HH:MM:SS
form, non-integer components, negative values, zero (must be
≥ 1 second).

No spec changes — the field is the existing
``wall_time_seconds``, unchanged. ``vq admin status``, ``vq
queue``, the watchdog's TIME_EXCEEDED enforcement all keep
working without modification.

Codename *Codd's Tuple*: Edgar Codd's relational model
distinguished the underlying tuple (the row) from any
particular view over it. Here ``wall_time_seconds`` is the
tuple field; ``--wall-time-seconds`` and ``--time-limit /
--time`` are two views over the same column. Codd would be
amused that the views differ only in human ergonomics.

Test suite: **1968 passed** (+30 new in
``tests/test_time_limit.py`` — parametrized over canonical
HH:MM:SS / MM:SS / integer forms, exhaustive rejection-case
coverage, end-to-end CLI tests for both flag spellings,
mutual-exclusion gate, backward-compat of the existing
``--wall-time-seconds``), 11 skipped.

---

## v0.7.15 — *Shannon's Entropy* — cross-user resource cap tripwire (2026-05-28)

User clarified the multi-user resource model on 2026-05-28:
*"The resources need to be managed by the queue. Jobs from
different users shall not run in parallel if that exceeds total
available resource."* The daemon already enforces this (the
dispatch loop's ``used_cpus + spec.cpus > effective_max_cpus``
check sums across all users via the global ``_running`` /
``_orphans`` dicts), but nothing tested the invariant
explicitly. The v0.6.29 orphan-budget tests covered same-user
cases; the v0.6.34 per-user quota tests covered the
per-user-restrict-tighter path. The cross-user "two users can't
oversubscribe the host" case had no coverage.

v0.7.15 ships the tripwire. 6 new tests in
``tests/test_cross_user_resource_cap.py``:

* **Same-user baseline** — 1 user × 2 specs × 3 CPUs each
  vs cap=4 → only 1 dispatches.
* **2-user cross-user** — user A's 3-CPU job + user B's
  3-CPU job vs cap=4 → exactly 1 dispatches (which one is
  priority + submitted_at; the invariant is "not both").
* **3-user cross-user** — 3 users × 2 CPUs each vs cap=5 →
  exactly 2 dispatch (the third is held).
* **Single-job saturation** — user A's 16-CPU job
  saturates cap=16; user B's 1-CPU job stays PENDING.
* **Orphan from one user blocks another's dispatch** — A's
  orphan (alive from a previous daemon) holding 6 CPUs
  vs cap=8 means B's 3-CPU spec stays PENDING. This is the
  v0.6.29 invariant verified cross-user.
* **Structural test of dispatch-loop ordering** — the
  global ``cpus_total`` gate runs BEFORE the per-user
  quota gate. Pinned via a string-grep on daemon.py so a
  future refactor that swaps the order (which would let a
  per-user quota allocation 'unlock' a globally-rejected
  spec) trips this test.

No production code changes. Audit found the existing
behaviour correct; the tests pin the verified behaviour as
tripwires for future refactors.

Codename *Shannon's Entropy*: Claude Shannon's information
theory — the channel capacity is a hard upper bound on
throughput regardless of who's transmitting. The host's
``cpus_total`` is vq's channel capacity; the global cap
enforces that parallel dispatch across users can never
exceed it.

Test suite: **1938 passed** (+6 new), 11 skipped.

---

## v0.7.14 — *Hamming's Code* — multi-user test coverage audit (2026-05-27)

v0.7.13 audited the general v0.6.18 → v0.7.12 ships but
stopped short of the explicit "multi-user test coverage
audit" half from the roadmap. v0.7.14 fills it.

8 new tests in ``tests/test_round3_multi_user_audit.py``,
organized by introducing ship:

* **v0.7.7 fetch --workdir** —
  ``fetch_workdir_local(..., multi_user=True)`` resolves the
  spec via ``resolve_spec_path`` (cross-user search), not
  the single-user ``queue_dir()``. Plus: destination dir
  honours the multi-user spec's ``job_name``. Plus: unknown
  jobid in multi-user fails with the same ``FileNotFoundError``
  shape as single-user (caller doesn't have to branch on
  mode).
* **v0.7.8 depends_on_any** — the validation pass in
  ``submit_local`` checks ``--depends-on-any`` predecessors
  against the SAME multi-user ``queue_dir`` as ``--depends-on``
  (submitter's per-user dir; cross-user predecessor rejected).
  Cross-user dependency mode remains a future feature; this
  test pins the current single-user-scoped behaviour.
* **v0.7.9 reset-branch / v0.7.12 audit doc** —
  ``admin_status_path()`` resolution baseline pinned. The
  v0.7.12 audit doc flagged the user-XDG vs daemon-XDG split
  on this file; pinning the current resolution here gives the
  future unification ship a known starting point and catches
  an unintentional path move.
* **v0.7.10 collapse-arrays** — ``list_jobs(host,
  multi_user=True)`` aggregates across every user dir; the
  ``--collapse-arrays`` fold then folds each user's array
  group independently (the group_id is per-submit, so groups
  never span users — but the aggregation order across users
  could surprise the fold).
* **v0.7.11 remote --array** — the wire shape carries no
  multi-user hint (no ``--multi-user`` / ``--uid`` flag
  forwarded). The remote vq alone decides mode from its own
  ``/etc/vq/config.toml``. Pinned: only ``--array N`` lands
  on the remote argv.

No production code changes. The multi-user paths were all
working correctly — the audit's value is the tripwires for
future refactors.

Codename *Hamming's Code*: Richard Hamming's error-correcting
codes — the parity bits that catch silent corruption in
seldom-exercised codepaths. The multi-user tests are vq's
equivalent: the seldom-exercised paths the operator touches
only when the daemon's running as root with [multi_user]
enabled, exactly the scenario where a quiet regression would
go unnoticed longest.

Test suite: **1932 passed** (+8 new), 11 skipped.

---

## v0.7.13 — *Backus's Form* — round-3 hardening + coverage audit (2026-05-27)

v0.6.17 ran a round-2 hardening audit (watchdog, cgroup,
transport). v0.6.18 audited test-coverage gaps. Since then,
30+ ships have landed (v0.6.18 → v0.7.12). v0.7.13 walks the
new surface area and fills coverage gaps that the per-module
test files left implicit, on the theory that "an operator
triggering an uncovered branch after-hours is the worst time
to discover the gap."

9 new tests in `tests/test_round3_audit.py`, organized by the
ship that introduced the covered code:

* **v0.7.5 recovery_audit** — TCP probe handles
  ``socket.gaierror`` (DNS resolution failure) cleanly, not
  just ``ConnectionRefusedError`` / ``socket.timeout``.
* **v0.7.6 fanout** — ``_safe_per_host`` catches
  ``Exception`` but lets ``KeyboardInterrupt`` propagate so
  Ctrl-C still aborts a sweep.
* **v0.7.6 fanout JSON** —
  ``_aggregate_per_host_json`` preserves a per-host result
  that's a JSON list (not a dict) — vq queue --json returns a
  list, for example.
* **v0.7.7 fetch workdir** — works on a spec with
  ``archived_at`` set (archives only touch the workspace, not
  the workdir).
* **v0.7.8 depends_on_any** — tripwire that
  ``JobState.INTERRUPTED in TERMINAL_STATES`` (so the afterany
  predicate treats it as ready). Future refactor that removes
  INTERRUPTED from TERMINAL_STATES would silently break
  afterany; this test catches it.
* **v0.7.9 reset-branch** — failed ``git fetch`` cleanly
  skips ``git reset --hard`` and preserves the prior
  ``last_sha`` in admin-status (does NOT clobber to None,
  which would erase useful diagnostic state).
* **v0.7.10 collapse-arrays** — single-element group renders
  as ``ARRAY 1/1 done`` (boundary case); all-failed group
  renders as ``ARRAY N/N F`` (single-state form, instantly
  recognisable as "whole sweep broke").
* **v0.7.11 remote --array** — ``array=1`` produces a wire
  shape byte-identical to the pre-v0.7.11 single-call path
  (no ``--array`` flag on the remote argv). Guards against a
  future refactor accidentally routing single submits through
  the array-spawn codepath.

No production code changes. The audit found no actual
behavioural bugs — every probed case already worked
correctly; the tests pin the behaviour so a future regression
fails loudly.

Codename *Backus's Form*: John Backus + BNF (Backus-Naur
Form). BNF made grammar explicit — every production rule
written down, no implicit fallbacks. The round-3 audit makes
each module's contract surface explicit by pinning the
edge-case branches the regular test files take for granted.

Test suite: **1924 passed** (+9 new), 11 skipped.

---

## v0.7.12 — *Wirth's Modula* — state-file location audit (2026-05-27)

v0.7.1's design doc flagged "state-file location unification"
as a known slip — the user-XDG vs daemon-XDG split means
``vq admin status`` from the operator and ``vq admin
auto-update`` from the daemon's cron can write to (and read
from) two different ``admin-status.json`` files. Resolving
the split touches the cleanup sweep, per-user state, and
daemon RPC in ways that need maintainer-approved design work
— so v0.7.12 ships only the *audit* step: document the
current surface comprehensively, so a future ship can talk
about deltas against a stable baseline.

New doc: ``docs/state_file_audit.md``. Contains:

* The quick-reference layout (single-user XDG + multi-user
  ``/var/lib/vq/`` roots, with env-var overrides).
* The XDG split rationale (vq follows the spec strictly;
  the operator footgun is cognitive).
* The four env-var overrides table — ``VQ_CONFIG_DIR``,
  ``VQ_STATE_DIR``, ``VQ_ARCHIVE_DIR``,
  ``VQ_MULTI_USER_ROOT`` — with their precedence + scope.
* Two non-interaction warnings:
  ``VQ_STATE_DIR`` does NOT move config (tests pin both);
  ``VQ_STATE_DIR`` is ignored in multi-user mode.
* Per-file tables for config (3 entries), single-user state
  (~14 entries), multi-user state (per-user + system-level).
* Five known gotchas with workarounds, including the v0.7.1
  ``admin-status.json`` split with a concrete
  ``sudo -u ... VQ_STATE_DIR=...`` workaround.
* "What would unification look like?" deferred section that
  catalogues four candidate directions (unified XDG, unified
  config-style, symlinks, daemon-side RPC) without picking
  one — the future ship's first decision.
* Maintenance instructions: how to keep the doc in sync when
  adding or relocating a state file.

This is a docs-only ship. No code changes; no test changes.
v0.7.12 exists as a separate version (rather than a docs
slipstream) so operators can cite a stable version when
investigating "where does my X live?" — and so the future
unification ship has a baseline tag to diff against.

Codename *Wirth's Modula*: Niklaus Wirth's Modula language
made "explicitly export every interface" foundational. The
audit doc explicitly enumerates every state-file path vq
touches — it's the export interface for the path layout,
the way each ``MODULE`` in Modula explicitly listed what it
exposed. Picking it for a docs-shaping ship is on theme.

Test suite: **1915 passed / 11 skipped**, unchanged from
v0.7.11.

---

## v0.7.11 — *Stroustrup's Stencil* — remote --array single-roundtrip (2026-05-27)

v0.6.52 shipped ``vq submit --array N`` but explicitly punted
on the remote-host path: the CLI looped ``submit_remote`` N
times, paying N SSH handshakes + N source-tar uploads. For a
30-element array, that's ~30× the wall-time of the local case
plus correspondingly multiplied network load.

v0.7.11 closes the gap. The remote ``vq`` already supports
``--array N`` natively (v0.6.52 ``submit_local_array``); the
v0.7.11 change is just to forward the count over SSH instead
of looping on the laptop:

* ``submit_remote(..., array=N)`` adds an ``array`` kwarg
  (default 1) that emits ``--array N`` on the remote argv.
* The local side reads N jobids from the remote's multi-line
  stdout (the remote vq already prints one jobid per line
  per array element).
* ``submit_remote`` return type changes ``str`` → ``list[str]``
  uniformly. The single-element case returns a length-1 list
  rather than the bare string, so callers don't branch on
  ``array`` at all — they just iterate.
* The CLI dispatch collapses to a single ``submit_remote(...,
  array=array)`` call, eliminating the pre-v0.7.11 ``for _ in
  range(array): submit_remote(...)`` loop.

Operationally most visible on the canonical "30 elements
chemistry sweep against vibe-qc" use case — wall time for a
30-element array submit drops from ``30 × (ssh handshake +
30-KB tar upload)`` to ``1 × (ssh handshake + 30-KB tar
upload + remote spawn loop)``. On a 100-ms-RTT laptop-to-host
link that's ~9 seconds saved per array submit; on a 1-Gbps
link with a 10-MB ``--dir`` source it's about ~3 seconds of
upload time per element avoided.

Bonus: remote array elements now share an
``array_group_id`` (the remote's ``submit_local_array`` mints
it on the host side). Pre-v0.7.11 the CLI logged this
limitation explicitly — "Remote elements share no group id".
That comment goes away with v0.7.11; remote arrays are
finally indistinguishable from local arrays, including for
``vq queue --array-group <gid>`` and ``vq queue
--collapse-arrays`` (v0.7.10).

Validation:

* ``array < 1`` rejected at the laptop edge with a clear
  ``ValueError``.
* Remote stdout length-check: if the remote returns fewer
  jobids than ``array`` (truncated stream, remote error
  printed mid-batch), raise ``RemoteError`` rather than
  silently returning a short list.
* Each jobid validated as 12-hex; a single malformed line
  surfaces as ``RemoteError`` naming the offending value.

Existing ``submit_remote`` callers (one in the CLI, one in
``test_submit_remote.py``) updated for the new return shape.
No other call sites — the API surface is small.

Codename *Stroustrup's Stencil*: Bjarne Stroustrup, C++. A
"stencil" in C++ idiom is a template overlaid N times — which
is exactly what ``--array N`` does at submission: one
template (the source upload), N stamped specs. The
single-roundtrip optimization makes the stenciling actually
look like stenciling at the wire level.

Test suite: **1915 passed** (+6 new in
``tests/test_submit_remote.py::TestRemoteArray`` — array=1
backward compat, array=N does one upload + one ssh call,
returned jobids all 12-hex, mismatched count raises, array<1
rejected, malformed jobid in stream raises), 11 skipped.

---

## v0.7.10 — *McCarthy's List* — collapsed array-row display (2026-05-27)

v0.6.52 added ``vq submit --array N``; v0.6.53 added the
``--array-group GID`` filter. But for the operator surveying
the queue at a glance, ``vq queue`` floods one row per array
element — a 30-element submit takes 30 lines, and finding the
non-array jobs in among them is exactly the wrong default.

v0.7.10 adds a deliberate fold:

```sh
vq queue --collapse-arrays
```

renders one row per ``array_group_id`` with a compact per-state
breakdown in the state column:

```
ID         STATE              CPUS  SUBMITTED            COMMAND
solo00...  running            2     2026-05-27 10:00:00  python solo.py
grp00001   ARRAY 5P/25C/30    2     2026-05-27 10:00:00  python sweep.py
grp00002   ARRAY 3/3 done     4     2026-05-27 09:45:00  python qc.py
```

The single-letter state codes (P/R/S/C/F/K/O/V/T/I/A) cover
every JobState — a pinned test ensures the mapping stays
exhaustive across future state additions. Single-state groups
get the compact ``N/M LETTER`` form; mixed groups get the
breakdown ordered active-states-first.

Three composition properties:

* Combines with the existing filters (``-s``, ``--tag``,
  ``--array-group``). The per-group fold runs AFTER state
  filtering, so ``vq queue --collapse-arrays -s failed``
  shows only the array groups with at least one failed
  element, and the breakdown reflects just that subset.
* Non-array specs render unchanged in the same table.
* Forwarded over SSH for remote hosts — the wire carries the
  already-folded summary, not the full per-element table.

The fold is opt-in (no flag → per-element behaviour, same as
pre-v0.7.10) so existing scripts that parse ``vq queue`` output
don't break.

Codename *McCarthy's List*: John McCarthy invented LISP and
made the list the fundamental data structure of programming.
The collapsed array row IS a list rendered as one row — and
naming the predicate ``collapse_arrays=True`` makes the fold's
homoiconicity explicit (the input list, the per-group list,
and the rendered summary are all just views over the same
sequence).

Test suite: **1909 passed** (+13 new in
`tests/test_collapse_arrays.py` — state-summary helper
including exhaustive-letter-coverage gate, format_table fold
under collapse vs no-collapse, mixed array + non-array, multi-
group, NAME-column preservation, CLI flag end-to-end including
filter composition), 11 skipped.

---

## v0.7.9 — *Liskov's Substitution* — `vq admin reset-branch` (2026-05-27)

v0.7.1's post-update branch validation finally surfaced silent
branch drift loudly (the 2026-05-25 compute-d vibeqc-dev on
``release`` despite config), but it left the operator to fix
the drift by hand: ssh in, git fetch, git reset --hard. v0.7.9
closes that slip with an auto-fix verb.

```sh
vq admin reset-branch ENV [HOST] --yes
```

Sequence on the env's ``git_dir``:

1. ``git fetch origin``
2. ``git reset --hard origin/<configured-branch>``
3. Capture new SHA + branch.
4. Update admin-status: ``last_sha`` + ``last_branch_actual``
   + ``last_dirty_after_update=False`` (the reset always
   produces a clean tree by definition).

Three deliberate design choices:

* ``--yes`` is required. The reset is destructive (any
  uncommitted local changes are discarded) — that's exactly
  the intent (operators run this to throw away stray hand-
  edits after a cross-chat collision on compute-d / compute-a), but
  not what we want muscle-memory to land. Without ``--yes`` the
  verb prints the planned operation + exits non-zero.
* ``last_success`` is NOT flipped True. A reset-branch fixes
  the *branch* but doesn't prove the *build* is healthy.
  Operator follows with ``vq admin update`` (to rebuild) or
  ``vq admin mark-ok`` (if they've independently verified) to
  flip ``last_success``.
* No reset is attempted if the fetch fails. A failed fetch
  could mean we'd reset to a stale local ref, which is worse
  than just bailing out — operator can re-try.

Validation mirrors ``vq admin mark-ok``: unknown env → fast
error; env without a configured ``branch =`` → fast error
naming the missing config (the reset target is undefined
without it).

Codename *Liskov's Substitution*: Barbara Liskov, LSP. The
verb literally substitutes the working tree with the canonical
upstream form — anything a downstream consumer (the daemon /
the operator / the post-update branch check) expects of "the
env on branch X" continues to hold after the substitution.

Test suite: **1896 passed** (+10 new in
`tests/test_admin_reset_branch.py` — real-git roundtrip with
``_make_repo_with_remote`` fixture covering drift-forward,
drift-backward, no-op-when-aligned; validation including
unknown-env + env-without-branch; persistence including the
last_success-not-flipped invariant; CLI safety guardrail), 11
skipped.

---

## v0.7.8 — *Knuth's Schedule* — `--depends-on-any` (afterany) (2026-05-27)

v0.6.51's `--depends-on` shipped SLURM `afterok` semantics —
the dependent waits for predecessors to *succeed*, and a
predecessor failure cascade-fails the dependent. That's the
right default for build pipelines (don't run downstream if
upstream broke). But it's the wrong predicate for *cleanup*
or *post-processing* workloads: a results-collection job
typically wants to fire whether the upstream succeeded, hit a
walltime, OOM'd, or got killed — the entire point is to
gather what artefacts exist.

v0.7.8 adds the second predicate. New flag:

```sh
vq submit ... --depends-on-any JOBID  # repeatable
```

Semantics: the dependent holds PENDING until every
``depends_on_any`` predecessor reaches a terminal state (any
state — COMPLETED, FAILED, KILLED, OOM_KILLED, STARVED,
TIME_EXCEEDED, TIMEOUT, ABORTED_BY_QUEUE, INTERRUPTED). On
the next dispatch tick it then runs *regardless of the
predecessor's outcome*. Predecessor failure does NOT cascade
to the dependent — that asymmetry vs. ``--depends-on`` is the
whole point.

Combines additively with ``--depends-on`` on the same submit:

```sh
vq submit \
    --depends-on $A \         # afterok: A must succeed
    --depends-on-any $B \     # afterany: B must terminate
    cleanup.py
```

Dispatch gate becomes
``all(depends_on COMPLETED) AND all(depends_on_any TERMINAL)``.

Spec model gains ``depends_on_any: list[str]`` alongside the
existing ``depends_on``. Pre-v0.7.8 specs read clean (the
Pydantic default-factory empty list). Status text + JSON
render a ``depends_on_any_status`` per-list readiness
annotation; unlike the ``depends_on`` annotation, there is no
``"failed: …"`` variant (a failed predecessor in afterany is
"ready," not "failed").

Submit-time validation mirrors the afterok path: dedupe per
list, reject self-dependency, fail-fast on a nonexistent
predecessor. Cross-list dedup is the operator's call — an id
in both lists effectively degrades the afterany predicate to
afterok for that pred (harmless redundancy; no warning).

Codename *Knuth's Schedule*: Donald Knuth's *The Art of
Computer Programming* devotes Volume 1 to coroutines and
Volume 3 to sorting/searching, with extensive treatment of
scheduling problems throughout. The dispatch gate's
"all-predecessors-terminal AND all-predecessors-success"
conjunction is a textbook job-scheduling predicate; v0.7.8
adds the second half of the literal SLURM scheduler's
predicate vocabulary.

Test suite: **1886 passed** (+20 new in
`tests/test_depends_on_any.py` — spec roundtrip, submit
validation, dispatch-gate semantics including FAILED /
KILLED / OOM_KILLED predecessor → READY transitions, status
text + JSON display, the "no failed-variant" invariant), 11
skipped.

---

## v0.7.7 — *Cerf's Datagram* — `vq fetch --workdir` (2026-05-27)

v0.6.54 gave every job a per-job scratch directory at
``$VQ_WORKDIR`` and made it the right place for intermediate /
large artefacts (agent-protocol convention). What v0.6.54 did
NOT give operators was a way to *pull that content back* — to
read it they had to `vq status JOBID`, parse the workdir path,
ssh in, and `scp -r`. v0.7.7 closes that gap.

New verb:

```sh
vq fetch [HOST] JOBID --workdir [-o DIR]
```

* `--workdir` switches the fetch payload from `spec.cwd`
  (workspace) to `spec.workdir` (scratch). Same shape as the
  existing fetch otherwise — streaming ssh + tar for remote
  hosts, direct copy for local.
* Destination directory name has ``-workdir`` appended
  (``<jobname>-<jobid>-workdir/`` when ``--job-name`` was used;
  ``<jobid>-workdir/`` otherwise). Workspace fetch and workdir
  fetch of the same job can therefore coexist under one
  ``-o DIR`` without a name collision.
* Errors cleanly in two pre-empt-able failure modes:
  * Spec has no ``workdir`` field (pre-v0.6.54 spec or
    ``--no-workdir`` submit). Message: "no workdir; only the
    workspace is fetchable."
  * Workdir was swept by the daemon's terminal cleanup
    (``--clean-tmp`` at submit + job hit terminal). Message
    names the cause so the operator knows the fix is "don't
    pass ``--clean-tmp`` next time."

Two new internal pieces back the verb:

1. `emit_workdir_tar(jobid)` / `fetch_workdir_local` /
   `fetch_workdir_remote` in `vq/fetch.py`. Mirrors the
   existing workspace triplet (`emit_workspace_tar` /
   `fetch_local` / `fetch_remote`) closely — same streaming-tar
   pipeline, same peek-first-member receive logic — but with no
   archive-aware path (workdirs aren't archived by `vq cleanup
   --archive`, which only touches the workspace).
2. `vq tar-workdir JOBID` hidden internal verb in `vq/cli.py`.
   Mirrors `tar-workspace`. Excluded from the
   `setup_cli_logging` initialisation (alongside
   `tar-workspace`) so its binary stdout stream isn't
   contaminated by log lines.

Codename *Cerf's Datagram*: Vint Cerf co-invented TCP/IP, the
foundation of "reliably transfer arbitrary data between two
machines." A `vq fetch --workdir` is the highest-level
operator-facing instance of that contract in the queue's
surface — pull bytes off a remote scratch dir, land them
locally as a faithful copy, regardless of which side the job
ran on.

Test suite: **1866 passed** (+17 new — 14 in `tests/test_fetch.py`,
3 in `tests/test_cli.py`'s `TestFetchCLI` /
`TestTarWorkdirInternalVerb`), 11 skipped.

---

## v0.7.6 — *Tanenbaum's Mailbox* — parallel fleet fan-out (2026-05-27)

With v0.7.3's `BatchMode=yes` making unreachable hosts fail fast
on the SSH transport layer, the serial per-host fan-out in every
`--all` / `--all-hosts` verb has become unnecessarily expensive.
A fleet of N hosts paid `N × per_host_cost` wall time even when
most hosts were healthy; on the v0.7.5 `audit-recovery --all`
codepath each host costs up to ~15 s (3 tiers × 5 s timeouts),
so the cost was specifically painful there.

v0.7.6 parallelizes every per-host fan-out via a
`ThreadPoolExecutor` in `vq.cli`:

* `vq admin status --all` (existing read-only sweep)
* `vq admin update --all-hosts` (write sweep)
* `vq admin auto-update --all-hosts` (drift sweep)
* `vq admin audit-recovery --all` (recovery contract sweep)
* `vq queue --all` / `vq programs --all` (existing
  read-only sweeps via `_aggregate_per_host`)

Three invariants the implementation pins:

1. **Output ordering stays deterministic alphabetical.** A host
   that finishes last is still rendered first if its name sorts
   first; banner sequence is identical to the pre-v0.7.6 serial
   render. Operators don't have to relearn anything.
2. **Per-host failures stay isolated.** One ssh timeout / config
   error / parse failure still surfaces inline as the same
   `(error querying <host>: …)` line; the rest of the fan-out
   completes normally.
3. **JSON shapes are unchanged.** `--json` payloads still come
   back as the same top-level dict keyed by host name; per-host
   errors still surface as `{"error": "…"}`.

Two escape hatches for operators who need predictable serial
ordering (debug logs, pathological host that confuses the pool):

* `VQ_FANOUT_SERIAL=1` — env var; forces serial dispatch.
* `VQ_FANOUT_WORKERS=N` — env var; caps the thread pool size
  (default cap is 8, enough for realistic fleets without
  swamping the local ssh multiplexer on a 20-host config).

Codename *Tanenbaum's Mailbox*: Andrew S. Tanenbaum's distributed
systems / operating systems textbooks made "mailbox" the canonical
primitive for message-passing concurrency. Each thread sends to
its host's "mailbox" (the ssh transport) and waits for the reply;
`as_completed()` rendezvous picks results up as they land.

Test suite: **1849 passed** (+31 new in
`tests/test_fanout.py`), 11 skipped.

---

## v0.7.5 — *Hopper's Compiler* — host recovery channels contract (2026-05-26)

The 2026-05-26 compute-d ssh-trust lockout exposed an architectural
gap: every fleet host had **exactly one** path in from the laptop
(primary sshd + the laptop's pubkey), so when *something* (cron /
unattended-upgrade / config-mgmt) wiped `~/.ssh/authorized_keys`,
every administrative path closed simultaneously. Recovery cost
ran a month (waiting on physical access). For the next machine
that joins the fleet, this needs to not happen.

The fix is defense-in-depth: **multiple independent channels** so
losing one doesn't lock you out. v0.7.5 codifies the contract:

* **Tier 1 — Hardware management (BMC / IPMI / iDRAC)**.
  Independent of OS state entirely. Informational from vq's
  side: the `bmc_url` field in `[hosts.X.recovery]` is shown
  in `audit-recovery` output as a click-through. Not probed
  (BMCs are designed to resist unauthenticated requests).
* **Tier 2 — Cockpit web admin on :9090**. Separate daemon
  (`cockpit.socket`), PAM auth (not SSH keys). When sshd is
  broken / locked out, Cockpit usually still works. The verb
  TCP-probes :9090 for liveness.
* **Tier 3 — Recovery sshd on an alternate port (default
  22222) with `/etc/ssh/recovery_authorized_keys`** —
  root-owned, mode 0600. No user-level process can wipe it.
  Carries a *separate* recovery keypair the operator keeps
  in cold storage, distinct from the day-to-day laptop key.
  The verb actually attempts ssh auth on this port to verify
  end-to-end.

Three pieces ship together:

1. **`docs/host_recovery_channels.md`** — the contract spec.
   Per-tier setup instructions, pre-flight checklist for new
   hosts, failure-mode table (which tier saves you in which
   scenario), operational rules.
2. **`contrib/setup-recovery-channels.sh`** — idempotent
   bootstrap. Installs Cockpit (apt / dnf / pacman aware),
   writes `/etc/ssh/sshd_config.d/recovery.conf` with the
   tier-3 Match block, drops the recovery key with correct
   perms, reloads sshd, verifies listeners. Safe to re-run.
3. **`vq admin audit-recovery HOST [--all] [--json]`** — the
   audit verb. Probes each tier, reports green/yellow/red.
   Exit code non-zero if any host is RED (CI-wireable).

Why "Hopper's Compiler": Grace Hopper's compilers introduced
"check the source before you commit to running it" as a
discipline. v0.7.5 applies the same to host provisioning —
audit the recovery channels before you trust a host to the
fleet. The audit catches gaps at provisioning time, not at
3am when you've already lost the primary path.

New `RecoveryConfig` pydantic model on `HostConfig` carries
per-host customisation (ports, key paths, BMC URL); defaults
match the values produced by the bootstrap script, so most
hosts need zero `[hosts.X.recovery]` lines in config.

**Files touched**:
- `vibe-queue/docs/host_recovery_channels.md` (new — the contract)
- `vibe-queue/contrib/setup-recovery-channels.sh` (new — bootstrap)
- `vibe-queue/src/vq/recovery_audit.py` (new — probe + report)
- `vibe-queue/src/vq/config.py` — `RecoveryConfig` model, added
  to `HostConfig.recovery`
- `vibe-queue/src/vq/cli.py` — `vq admin audit-recovery` verb
- `vibe-queue/tests/test_recovery_audit.py` (new — 18 tests
  covering all three tiers, audit composition, text/JSON
  rendering, RecoveryConfig defaults)

Suite: **1818 passed / 11 skipped** on macOS (+18 new tests).

---

## v0.7.4 — *Ritchie's Pipe* — dev-branch tracker for auto-update (2026-05-26)

Reserves the `Ritchie's Pipe` codename slotted at the v0.7.1 design
phase — Unix pipes are the metaphor: dev-tracking auto-update is a
pipe feeding the local env whatever's on `origin/<branch>`.

The deferred-since-v0.6.11 feature. `vq admin auto-update` until now
only ever moved the env to a newer semver-shaped tag — sensible for
release-tracking (vibeqc-release should only deploy at tags), wrong
for dev-tracking (vibeqc-dev should follow main as it advances,
without waiting for a tag). v0.6.11's design refused branch-mode
because the dev tree could be dirty (chats writing scratch into the
checkout) or drifted (wrong branch checked out). Three later ships
closed those failure modes one at a time:

* **v0.6.54** agent protocol — chats write to `$VQ_WORKDIR`, not
  into the git checkout
* **v0.7.1 item 1** post-pull branch validation — silent branch
  drift now fails loudly with `branch_mismatch`
* **v0.7.1 item 5** `fail_on_dirty` opt-in — operators can pin
  "this env should always be clean"

With those guards in place, dev-tip auto-update is safe.

The mechanism: new per-env config field
`auto_update_policy: Literal["tag", "branch"] = "tag"`. When
`"branch"`, `check_env_drift` does:

1. `git fetch origin` (bounded by the existing `_LS_REMOTE_TIMEOUT_SECONDS`)
2. `git rev-parse HEAD` vs `git rev-parse origin/<branch>`
3. SHAs differ → `action="update"`, `target_sha=<origin sha>`
4. SHAs match → `action="skip"`, reason `"already at origin/main (sha)"`

`auto_update_env` apply branches on `decision.policy`: tag-mode
keeps the v0.5.24 `expected_tag` verification; branch-mode calls
`update_env` without `expected_tag` and lets v0.7.1's branch
validation pin the post-pull HEAD to `prog.branch`.

**Why "Ritchie's Pipe"**: Dennis Ritchie's Unix pipes turn streams
into composable feeds — the dev-mode auto-update is exactly that
shape: origin/main is the source, the local env is the sink, the
auto-update timer is the pipe character. v0.6.11 implemented half
the abstraction (tag drift); v0.7.4 closes the loop.

**Files touched**:
- `vibe-queue/src/vq/config.py` — `auto_update_policy` field on
  `VenvProgram`
- `vibe-queue/src/vq/auto_update.py` — new `_fetch_origin`,
  `_rev_parse`, `_check_branch_drift` helpers; `check_env_drift`
  branches on policy; `AutoUpdateDecision` gains `policy`,
  `current_sha`, `target_sha`; `auto_update_env` apply path
  splits by policy
- `vibe-queue/tests/test_auto_update_branch_mode.py` — 9 new
  tests covering config defaults, branch-mode success / drift /
  error paths, and tag-mode back-compat

Suite: **1800 passed / 11 skipped** on macOS (+9 new tests).

---

## v0.7.3 — *Dijkstra's Semaphore* — bounded wait on unreachable hosts (2026-05-26)

The 2026-05-26 compute-d ssh-trust lockout exposed a long-latent gap:
when a fleet host's `authorized_keys` loses the laptop's key, ssh
falls back to keyboard-interactive / password prompts. In a
`subprocess.run` context, the prompt reads from the inherited tty
stdin and **never returns** — `vq admin status --all` and every
other `--all` aggregation hung indefinitely on the unreachable host
instead of failing fast and continuing with the rest of the fleet.

The fix is a one-line addition to `transport._ssh_base` /
`transport._scp_base`: `-o BatchMode=yes`. With it set, ssh refuses
to prompt for any non-key credentials and exits non-zero
immediately on auth failure. The v0.5.36 per-host aggregation
(`_aggregate_per_host` in `cli.py`) already catches the resulting
`RemoteError`/`ClickException` and renders it inline as
`(error querying <host>: ...)` — so one bad host no longer blocks
the listing for any of the others.

Why "Dijkstra's Semaphore": Edsger Dijkstra's foundational work on
synchronization primitives codified the discipline of bounding
every wait in a concurrent system. v0.7.3 applies that discipline
to vq's network calls — every ssh / scp invocation now has a
finite, predictable failure mode regardless of the remote's state.

**Files touched** — `vibe-queue/src/vq/transport.py` (added one
`-o BatchMode=yes` to each of `_ssh_base` / `_scp_base`);
`tests/test_transport.py` (new `TestSshBatchMode` class with three
assertions: `run_remote_vq`, `run_remote_shell`, `upload_file` all
carry BatchMode=yes).

Suite: **1791 passed / 11 skipped** on macOS.

---

## v0.7.2 — *Engelbart's Demo* — VERSION column from pyproject.toml (2026-05-25)

Same-day patch on v0.7.1 *Lamport's Clock*. The 2026-05-25 fleet
status output was actively misleading: vibe-qc main is at
`0.9.2.dev0` (per `pyproject.toml [project] version`), but
`vq admin status` displayed `v0.7.5-983-g11b4f7af` for the dev
clone because `git describe --tags --always` walks back to the
nearest annotated tag — and v0.7.5 is the most recent annotated
one on the lineage; v0.8.x and v0.9.x release tags are either
lightweight or off-lineage and don't surface. Operators reading
the status saw "0.7.5" and worried about a regression that wasn't
there.

The fix: read `pyproject.toml`'s `[project] version` directly
via a new `_query_pyproject_version()` helper. The text formatter
renames the column header `DESCRIBE` → `VERSION` and shows the
pyproject value preferentially, falling back to `git describe`
only when no pyproject is present (e.g. envs that don't follow
the standard layout).

JSON output adds a new `current_version` field (always present;
null when no pyproject is found). The pre-v0.7.2 `current_describe`
field is preserved for back-compat.

**Why "Engelbart's Demo"**: Doug Engelbart's 1968 *Mother of All
Demos* introduced interactive computing's defining UX: *show the
human what's actually on the machine*. v0.7.2 makes
`vq admin status` finally honest about what's on the machine.

**Files touched** — `vibe-queue/src/vq/admin.py` (helper +
EnvStatus field + text/JSON formatters);
`tests/test_admin_pyproject_version.py` (new, 11 tests);
`tests/test_admin.py` (one assertion updated for the column
rename). Suite: 1788 passed / 11 skipped.

---

## v0.7.1 — *Lamport's Clock* — operator-visibility hardening (2026-05-25)

vq's first per-feature minor on the 0.7.x line. Six-item
additive ship on `vq admin update` provoked by the same-day
fleet-update incident: compute-d vibeqc-dev silently checked out
the `release` branch despite config saying `main` (root cause
was a vibe-qc-side argv loss in `scripts/_safe_build_env.sh`,
fixed in vibe-qc `ea195796`). Recovery cost ~5 hours because
`vq admin status` reported `LAST OK=False` and nothing else
visible — the operator had to SSH to the host, hunt logs, and
reverse-engineer "what went wrong" three times before
discovering the venv-hybrid + branch-drift combination.

**Why "Lamport's Clock"**: Lamport's 1978 paper on distributed
event ordering established that a single instantaneous reading
(`LAST OK=False`) is insufficient — you need the causal chain
(branch ⇒ pull ⇒ script ⇒ outcome) to make sense of what
happened. v0.7.1 records the causal chain.

**The six items** (all additive — no breaking schema changes;
old admin-status.json files load via dataclass defaults; new
files load on older clients via the v0.7.1 forward-compat
unknown-key strip):

1. **Post-update branch validation.** After git pull, run
   `git rev-parse --abbrev-ref HEAD` and compare to
   `VenvProgram.branch`. Mismatch fails the update with
   explicit `branch_mismatch` reason and skips the 10-30 min
   build entirely. `vq admin status` renders
   `main -> release` arrow notation on drift.
2. **Persist `update_script_output` tail.** Last 80 lines
   (env-tunable via `VQ_ADMIN_UPDATE_OUTPUT_LINES`) of the
   failed update script's stdout+stderr land in admin-
   status.json. `vq admin status --verbose` surfaces them
   per-failing-env so the operator's first command after a
   failed update is the answer, not the start of an SSH-hunt
   cycle.
3. **`--update-script-arg FLAG`** (repeatable). Forwards
   flags like `--recreate-venv` or `--dev` to the bash
   update script without an SSH+heredoc workaround.
   Forwarded transparently to remote hosts via
   `--all-hosts`. The 2026-05-25 incident hit this gap
   three times on compute-d; this verb closes it.
4. **`vq admin mark-ok ENV --note "REASON"`** operator
   escape hatch. Flip `LAST OK=True` cleanly + with an
   audit trail (`last_marked_ok_at` + `last_marked_ok_note`,
   surfaced as `True*` in status with the note under
   `--verbose`). Replaces the surgical Python-edit of
   admin-status.json we used on 2026-05-25. Admin-token
   gated in multi-user mode.
5. **`fail_on_dirty = true`** opt-in `VenvProgram` config
   knob. When set, post-update dirty tree flips
   `LAST OK=False` with the explicit reason
   `dirty_tree_after_update`. Recommended for vibeqc-queue
   and vibeqc-release; default `false` for vibeqc-dev where
   dirty is expected (basissetdev artifacts, in-flight
   research edits).
6. **`vq admin update --show-output`**. Emits the failure
   tail header + script output to stderr after the standard
   summary. Especially useful in `--json` mode where the
   output lives only in a JSON field — `--show-output`
   provides the human-readable bridge.

**Forward-compatibility fix**: `read_admin_status` now
silently strips unknown keys before constructing
`AdminUpdateRecord`. Pre-v0.7.1 the constructor's `TypeError`
caused the whole entry to be dropped — meaning a newer daemon
could write a record an older client treated as "no last
update at all". Mixed-version operation now works cleanly in
both directions.

**Out of scope (deliberate slips)**: `vq admin reset-branch
ENV HOST` auto-fix (operator should see drift via item 1
first; slipped to v0.7.2+); `--stash-dirty` (papers over
basis-chat's populate bug; slipped indefinitely); state-file
location unification (touches too many subsystems for an
operator-visibility ship; documented as a known footgun).

**Files touched** — `vibe-queue/src/vq/admin.py`,
`vibe-queue/src/vq/cli.py`, `vibe-queue/src/vq/config.py`;
new tests: `tests/test_admin_branch_validation.py`,
`test_admin_output_capture.py`, `test_admin_update_script_args.py`,
`test_admin_mark_ok.py`, `test_admin_fail_on_dirty.py`,
`test_admin_show_output.py`; design doc at
`docs/v0_7_1_lamports_clock_design.md`; conftest stub at
`tests/conftest.py` so legacy `subprocess.run` mocks survive
the new branch-check call site.

**Suite**: 1777 passed, 11 skipped on macOS (+58 new tests).

---

## v0.7.0 — *Hoare's Pipeline* — agent-protocol era (2026-05-25)

vq's minor-version jump after a long v0.6.x arc. The substantive
code shipped in v0.6.54 (commit `0b0ddb1f` —
[entry below](#v0654--per-job-workdir--agent-interaction-protocol-2026-05-25));
v0.7.0 is the **marker** that says "this is where vq's contract
with other dev chats became explicit." Convention: vq codenames
draw from computer-science pioneers (Hoare, Ritchie, Lamport,
Dijkstra, Hopper, Engelbart, Knuth, ...), distinct from
vibe-qc's chemistry/physics scientists (Löwdin, Pulay, Grimme,
Knowles, ...). Same `[surname]'s [object]` shape so the two
ship-note streams read culturally adjacent.

**Why "Hoare's Pipeline"**: Tony Hoare's CSP (Communicating
Sequential Processes, 1978) is the academic foundation for
"async processes coordinating through queues" — exactly the
model vq has been converging on across the v0.6.x arc and made
explicit in v0.6.54's agent protocol. The "Pipeline" object
evokes the chat → payload → queue → result flow operationally.

**What the v0.7.0 marker captures** (all already shipped in the
v0.6.34–v0.6.54 series — see entries below):

* **Multi-user backbone** + per-user quotas + bearer-token admin
  auth (v0.6.34–v0.6.47).
* **Fleet auto-update**: `vq admin auto-update [--all] [--all-hosts]`
  + systemd-timer template (v0.6.11, v0.6.47, v0.6.49).
* **Job-coordination surface**: `vq submit --depends-on JOBID`
  (v0.6.51), `--array N` (v0.6.52), `vq queue --array-group`
  filter (v0.6.53).
* **Operator visibility**: `vq logs JOBID` verb with
  `--follow` (v0.6.50), expanded `vq status` (depends_on
  annotation, array context, workdir, failure_reason),
  `vq overview` / `vq summary` fleet view (v0.6.21, v0.6.24).
* **Agent-protocol foundation** (v0.6.54): per-job `$VQ_WORKDIR`
  scratch + `docs/agent_interaction.md` + CLAUDE.md § 15. The
  shift from "vq is a job runner" to "vq is the coordination
  substrate between dev chats."

**v0.7.x roadmap** (codenames TBD at ship time except where named):

* **v0.7.1 *Lamport's Clock*** — shipped 2026-05-25. Six-item
  operator-visibility hardening pass on `vq admin update`
  provoked by the same-day fleet-update incident. See per-
  version entry below; design + postmortem in
  [`v0_7_1_lamports_clock_design.md`](v0_7_1_lamports_clock_design.md).
* **v0.7.2 *Engelbart's Demo*** — shipped 2026-05-25 (same-day
  patch on v0.7.1). `vq admin status` surfaces the project's
  semver from `pyproject.toml [project] version` in the new
  VERSION column, replacing the misleading `DESCRIBE` column
  that could lag by entire minor versions when annotated tags
  were stale.
* **v0.7.3 *Dijkstra's Semaphore*** — shipped 2026-05-26.
  `ssh BatchMode=yes` on every transport call so a host whose
  authorized_keys lost the laptop's key fails auth immediately
  instead of hanging on a password prompt waiting on inherited
  tty stdin. `vq admin status --all` (and every other `--all`
  aggregation) renders the bad host inline as an error and
  carries on with the rest of the fleet.
* **v0.7.4 *Ritchie's Pipe*** — shipped 2026-05-26. Per-env
  `auto_update_policy = "branch"` config knob makes
  `vq admin auto-update` track `origin/<branch>` SHA drift
  (alongside the existing `"tag"` policy). Reverses v0.6.11's
  no-dev-tip decision now that v0.6.54 / v0.7.1 / v0.7.3 have
  collectively closed the original footgun. See per-version
  entry below.
* **v0.7.5 *Hopper's Compiler*** — shipped 2026-05-26. 3-tier
  host recovery channels contract (BMC + Cockpit + recovery
  sshd) + `vq admin audit-recovery` verb + idempotent bootstrap
  script. Motivated by the 2026-05-26 compute-d lockout. See
  per-version entry below.
* **v0.7.6 *Tanenbaum's Mailbox*** — shipped 2026-05-27.
  Parallel fleet fan-out: every `--all` / `--all-hosts` verb
  dispatches per-host calls via `ThreadPoolExecutor` (safe on
  top of v0.7.3's `BatchMode=yes`). Output ordering remains
  deterministic alphabetical; `VQ_FANOUT_SERIAL=1` escape
  hatch + `VQ_FANOUT_WORKERS=N` cap (default 8). See per-version
  entry below.
* **v0.7.7 *Cerf's Datagram*** — shipped 2026-05-27.
  `vq fetch --workdir JOBID` pulls the v0.6.54 scratch workdir
  (`$VQ_WORKDIR`) back to the laptop. Closes the v0.6.54 gap
  where operators could *write* to the workdir but had to ssh
  in manually to read content back. Destination dir gets a
  `-workdir` suffix so workspace + workdir fetches coexist
  under one `-o DIR`. See per-version entry below.
* **v0.7.8 *Knuth's Schedule*** — shipped 2026-05-27.
  `vq submit --depends-on-any JOBID` adds SLURM afterany
  semantics — the dependent dispatches once every predecessor
  reaches a terminal state regardless of success/failure.
  Complements `--depends-on` (afterok). Critical asymmetry:
  predecessor failure does NOT cascade-fail an afterany
  dependent. See per-version entry below.
* **v0.7.9 *Liskov's Substitution*** — shipped 2026-05-27.
  `vq admin reset-branch ENV [HOST] --yes` auto-fix verb for
  silent branch drift surfaced by v0.7.1's post-update branch
  validation. Runs `git fetch origin && git reset --hard
  origin/<configured-branch>`; updates admin-status with new
  SHA + branch_actual but conservatively does NOT flip
  `last_success` (operator follows with `vq admin update` or
  `mark-ok`). `--yes` required because reset --hard is
  destructive. See per-version entry below.
* **v0.7.10 *McCarthy's List*** — shipped 2026-05-27.
  `vq queue --collapse-arrays` folds every `--array N` group
  into a single row keyed on `array_group_id` with a compact
  per-state breakdown (P/R/S/C/F/...). Opt-in; non-array specs
  render unchanged. Composes with existing filters; the fold
  runs AFTER state filtering so the breakdown reflects only
  the filtered subset. See per-version entry below.
* **v0.7.11 *Stroustrup's Stencil*** — shipped 2026-05-27.
  Remote `vq submit --array N` collapses N SSH roundtrips +
  N source-tar uploads into ONE upload + ONE remote vq call
  forwarding `--array N`. Remote elements now share an
  `array_group_id`, closing the v0.6.52 "no remote group id"
  limitation. `submit_remote` return type changed `str → list[str]`
  uniformly. See per-version entry below.
* **v0.7.12 *Wirth's Modula*** — shipped 2026-05-27. Docs-only
  ship: `docs/state_file_audit.md` catalogues every file vq
  reads or writes, the env-var precedence rules, and the
  known gotchas (including the v0.7.1-flagged user-XDG vs
  daemon-XDG split for `admin-status.json`). Contract surface
  for a future maintainer-approved unification ship to refactor
  against. See per-version entry below.
* **v0.7.13 *Backus's Form*** — shipped 2026-05-27. Round-3
  hardening + test-coverage audit. 9 targeted tests filling
  edge-case gaps across the v0.6.18 → v0.7.12 ships (recovery
  DNS failure, fanout KeyboardInterrupt, JSON non-dict
  results, workdir fetch on archived spec, afterany
  INTERRUPTED tripwire, reset-branch fetch-failure, collapse
  edge cases, remote --array wire-shape). No production code
  changes — audit found no bugs; tests pin behaviour to catch
  future regressions. See per-version entry below.
* **v0.7.14 *Hamming's Code*** — shipped 2026-05-27.
  Multi-user test coverage audit (secondary half of v0.7.13).
  8 targeted tests filling multi-user gaps: fetch --workdir
  cross-user spec resolution + named-job destination + unknown-
  jobid error path; --depends-on-any validation respects
  submitter's queue; admin-status path baseline pinned for
  future unification; list_jobs aggregates across users +
  collapse-arrays folds each group independently; remote
  --array wire shape carries no multi-user hint. No production
  code changes. See per-version entry below.
* **v0.7.15 *Shannon's Entropy*** — shipped 2026-05-28.
  Cross-user resource cap tripwire. Pins the operator-stated
  invariant ("jobs from different users shall not run in
  parallel if that exceeds total available resource") with
  6 tests: global `cpus_total` cap sums across submitters,
  orphan accounting works cross-user, dispatch-loop ordering
  runs global gate before per-user gate. No production code
  changes — existing enforcement verified correct. See
  per-version entry below.
* **v0.7.16 *Codd's Tuple*** — shipped 2026-05-28.
  `vq submit --time-limit HH:MM:SS` (alias `--time`) SLURM-
  ergonomic flag for the existing `wall_time_seconds` spec
  field. Mutually exclusive with `--wall-time-seconds`.
  Accepts HH:MM:SS, MM:SS, or plain integer seconds. See
  per-version entry below.
* **v0.7.17 *Postel's Robustness*** — shipped 2026-05-28.
  Webhook `notify_on_states` filter. NotificationConfig
  gains a state filter that defaults to "fire on every
  terminal" (backward-compat) but lets the operator narrow
  to e.g. "alert only on failure" with one TOML list. Closes
  the v0.5.35 explicit gap. See per-version entry below.
* **v0.7.18 *Kay's Object*** — shipped 2026-05-28.
  `vq overview --recommend` ranks healthy hosts by workload
  + idle-time, prints best single host name. Composes with
  shell: `vq submit $(vq overview --recommend) my.py`. Adds
  `running_cpus` / `pending_cpus` fields to HostOverview.
  See per-version entry below.
* **v0.8.0 *Dahl's Simula*** — shipped 2026-05-30.
  Daemon-side RPC for admin-status. Closes the v0.7.12
  user-XDG vs daemon-XDG split footgun: reads + writes now
  route through the daemon's Unix-socket RPC so there's ONE
  canonical `admin-status.json`. New `vq/rpc.py` module.
  Minor-version bump marking the architectural fence.
  See per-version entry below.
* **v0.8.1+** — `vq web` UI audit + improvements (C1)
  user-XDG vs daemon-XDG split documented as a known footgun
  in the v0.7.1 design doc).

Beyond v0.7.2, planned themes for the v0.7.x arc — feature
richness + maturity for HPC research workflows:

* ✅ `vq fetch --workdir` — shipped v0.7.7.
* ✅ Collapsed array-row display in `vq queue` — shipped v0.7.10
  via `--collapse-arrays`.
* ✅ Remote `--array` single-roundtrip optimization — shipped
  v0.7.11.
* Cross-user dependency mode (if requested).
* ✅ Round-3 hardening audit — shipped v0.7.13.
* ✅ Multi-user test coverage audit — shipped v0.7.14.
* ✅ `vq submit --depends-on-any` (afterany semantics) — shipped
  v0.7.8.

**Compatibility matrix** mapping vq versions to vibe-qc tags
lives in [`version_compatibility.md`](version_compatibility.md);
ship continues unchanged inside the vibe-qc monorepo (no
separate git repo).

---

### v0.6.54 — per-job workdir + agent interaction protocol (2026-05-25)

The "don't write to the git repo on compute-d/compute-a" ship. Provoked
by the 2026-05-25 deployment incident where untracked dirs on
compute-d (108 files in `examples/periodic/`,
`examples/experimental_regression/`) and modified basis files on
compute-a (141 `.g94` citation-comment additions, 1 deleted `.ecp`)
blocked the routine `vq admin update vibeqc-dev <host>` flow.
Each cost an hour of manual git-archaeology to untangle.

The mechanism: every job dispatched by the daemon gets its own
scratch **workdir** outside the git checkouts. The protocol:
chats submit work via `vq` with a payload, use `$VQ_WORKDIR` for
scratch, and never touch the host's git trees. A new
`docs/agent_interaction.md` documents the rules; a new CLAUDE.md
§ 15 points dev chats at the doc on their first read.

Implementation:

* **Spec**: additive `workdir: str | None` (set by daemon at
  dispatch) and `clean_workdir_on_terminal: bool = False` (set
  by `vq submit --clean-tmp`). Pre-v0.6.54 specs read clean.
* **Paths**: `workdir_root()` / `workdir_for(jobid)` (single-user
  at `<state>/workdirs/<jobid>/`); `user_workdir_root(uid)` /
  `user_workdir(uid, jobid)` (multi-user at
  `users/<uid>/workdirs/<jobid>/`). `provision_user_state` now
  also creates+chowns `workdirs/`.
* **Daemon `_start_job`**: creates the per-job workdir
  (chowned to run_uid in multi-user), persists `spec.workdir`,
  injects `VQ_WORKDIR` into the child process env alongside the
  v0.6.52 `VQ_ARRAY_*` triple. Non-array specs now also get a
  materialised env dict (changed from `env=None` inherit) so the
  `VQ_WORKDIR` injection always reaches the job.
* **Daemon terminal transitions**: new `_maybe_cleanup_workdir`
  helper called from `_record_finish` and `_recover_orphan`;
  rmtrees the workdir IFF `spec.clean_workdir_on_terminal` is set.
  Idempotent, errors logged but never propagated.
* **`cleanup.AutoCleanupPolicy`**: new
  `workdir_max_age_seconds: int | None = None` field (default:
  disabled). When set, the daemon auto-cleanup pass runs a
  workdir sweep: rmtrees per-job workdirs whose top-level mtime
  is older than the cutoff. Skips young workdirs (so actively-
  written ones survive). Counts surfaced in the pass log
  (`workdirs_swept` / `workdir_errors`).
* **CLI**: `vq submit --clean-tmp` flag → sets
  `clean_workdir_on_terminal=True`. Forwarded to remote via the
  SSH delegate. Composes with all other submit flags
  (--depends-on, --array, --wait, --tag).
* **`vq status`**: shows
  `workdir: <path> (clean-on-terminal | lingers until cleanup-sweep)`
  when set. Hidden when None (pre-v0.6.54 specs, or jobs that
  haven't dispatched yet).

Docs:

* New [`vibe-queue/docs/agent_interaction.md`](agent_interaction.md):
  protocol for dev chats running on compute-d / compute-a (TL;DR,
  submission shapes, workdir rules, request convention,
  forbidden actions, recipe table).
* New CLAUDE.md § 15 amendment — short pointer at the new doc
  with the most-likely-missed rule (don't write to
  `/home/USER/gitlab/vibeqc-*/` on the fleet hosts).

New test file: `tests/test_workdir_v0_6_54.py` (15 tests):

* `TestSpecRoundtrip` — fields default sensibly, roundtrip,
  pre-v0.6.54 reads clean.
* `TestPaths` — single-user + multi-user workdir layout.
* `TestDaemonDispatch` — workdir created, env injected.
* `TestTerminalCleanup` — clean_workdir_on_terminal works in
  both directions + idempotent on missing workdir.
* `TestStaleSweep` — old workdir swept; young survives;
  disabled-by-default (None) is a no-op.
* `TestStatusDisplay` — workdir line + cleanup-mode annotation;
  hidden when unset.
* `TestCLI` — `--clean-tmp` sets the spec field end-to-end.

Plus one regression fix in `tests/test_array_v0_6_52.py`: the
non-array Popen no longer passes `env=None` (now passes a
materialised env dict so VQ_WORKDIR can be injected). Updated
test asserts that VQ_WORKDIR is present and VQ_ARRAY_* keys are
absent for non-array specs.

No security boundary changes; not patch-candidate. Sets up the
v0.6.55 / v0.6.56 follow-ups:

* **v0.6.55**: dev-tracking auto-update (`vq admin auto-update
  --track-branch`) — reverses v0.6.11's deliberate no-dev-tip
  decision now that the agent-interaction protocol means the
  dev branch's tree is reliably clean.
* **v0.6.56**: persist `update_script_output` in `vq admin
  status` so the next failed `vq admin update` surfaces its
  build error instead of dropping stdout (the gap that hid
  compute-a's dirty-tree error today).

---

### v0.6.53 — `vq queue --array-group GID` filter (2026-05-24)

Polish on the v0.6.52 ship. After `jobids=$(vq submit --array 30
sweep.py)` an operator wants to see just those 30 rows without
the rest of the queue scrolling past. `vq queue --array-group
<gid>` filters to elements of the named group (the 8-hex
`array_group_id` recorded on each element's spec).

Composes with all the other queue filters:

* `vq queue --array-group <gid>` — only that group.
* `vq queue --array-group <gid> -s failed` — which array
  elements failed (the natural triage flow).
* `vq queue --array-group <gid> --json` — machine-readable.
* `vq queue --all --array-group <gid>` — across every host.

Implementation mirrors the v0.6.6 `--tag` filter exactly:

* New `--array-group GID` option on the `vq queue` command
  (single-value, not repeatable — operators target one group
  at a time).
* Applied client-side after the existing `-s` / `--show-archived`
  / `--tag` filters: `[s for s in specs if s.array_group_id ==
  array_group]`.
* Forwarded over SSH for remote hosts (`--array-group <gid>`
  appended to `remote_args` so the filter runs on the host with
  the specs and the wire doesn't carry rows that get dropped
  anyway — same wire-economy rationale as v0.6.6).

No spec changes. No display changes (collapsed-row "ARRAY
12/30 COMPLETED" view is still a follow-up; this ship just adds
the filter).

New test class in `tests/test_cli.py::TestArrayGroupQueueFilter`
(5 tests): filter shows only matching group + excludes other
groups + excludes solo submits; unknown GID returns empty;
composes with `-s` state filter (AND); `--json` form filters
identically; remote delegate forwards `--array-group` over SSH.

No security boundary changes; not patch-candidate.

---

### v0.6.52 — `vq submit --array N` array jobs (2026-05-24)

SLURM-array analogue. One submit spawns N near-identical specs
sharing an 8-hex group id; each gets a sequential index 0..N-1
and a total of N. The daemon injects three environment variables
at dispatch (`VQ_ARRAY_INDEX` / `VQ_ARRAY_TOTAL` /
`VQ_ARRAY_GROUP_ID`) so the job's script can branch on its index
without parsing its own spec file off disk.

Forms:

* `vq submit --array N input.py` — N elements, each running
  `input.py`.
* `vq submit --array N --depends-on $A input.py` — every element
  depends on `$A` (single shared predecessor).
* `vq submit --array N --wait input.py` — wait for ALL elements
  sequentially; exit code is the WORST per-element verdict.
* `vq submit --array 1 ...` — silently treated as a regular
  submit (single jobid, NO array fields set — `--array 1` is a
  no-op).

Implementation:

* Spec: additive `array_index: int | None`, `array_total: int |
  None`, `array_group_id: str | None`. All three are None for the
  common non-array case; all three are set together for array
  elements. No new dispatch semantics — the daemon treats each
  element as an ordinary independent spec (no gang scheduling,
  no array-completion notion, per-user budgets/quotas apply per
  element).
* Helper: `new_array_group_id()` in `vq.submit` returns 8 hex
  chars (shorter than jobid's 12; the namespace is "groups in
  this user's session," much smaller pressure than jobids face).
* Submit module: new `submit_local_array(array=N, **kwargs) ->
  list[str]` wrapper that loops `submit_local` N times with
  sequential `array_index`, shared `array_group_id`, and
  identical other fields. Each element gets its own jobid +
  workspace + full source copy. `vibeqc_preflight` is disabled
  inside the loop (running the user's script N times before
  queue entry is the opposite of what array operators want).
* Daemon `_start_job`: when `spec.array_index is not None`,
  build a child env dict from `os.environ` + the three VQ_ARRAY_*
  variables and pass it as `Popen(env=...)`. Non-array specs
  keep the pre-v0.6.52 inherit behavior (`env=None`). systemd-run
  `--scope` (both single-user `--user` and multi-user `--uid=U`
  paths) inherits the caller's environment, so the env reaches
  the job through the privilege-drop wrap.
* CLI: new `--array N` flag with `click.IntRange(min=1)`.
  Dispatches to `submit_local_array` when N > 1 (local host) or
  loops `submit_remote` N times (remote host — N SSH roundtrips
  + N source tar uploads, slow for large N but correct; remote
  --array forwarding is a future optimisation that changes the
  remote stdout contract). Prints all N jobids one per line.
  `--wait + --array` waits on every element in submission order;
  exit code is the WORST per-element `cli_exit_code`.
* `vq status`: new `array: <index>/<total> (group=<group_id>)`
  line when set (conditional like priority/tags).

Known limitations (documented):

* Remote `--array` doesn't share a group id across elements
  (each remote submit_remote call is independent and the remote
  vq's submit_local generates its own jobid; the laptop side
  doesn't thread a group id through). Local `--array` does set a
  shared group id. A future refactor could push --array N to the
  remote in one shot.
* Workspace duplication: each element gets a full source copy.
  For small input.py the cost is trivial; for big `--dir` /
  `--compressed` submits with large N, the operator pays N ×
  source-size disk + setup time. Single-workspace-per-job is a
  deep daemon invariant (chown, archive, cleanup); keeping it
  intact is the right v0.6.52 trade-off.
* No `vq queue --array-group GID` filter; no collapsed display
  in `vq queue` (every element shows as its own row). Polish
  follow-up.

New test file: `tests/test_array_v0_6_52.py` (18 tests):

* `TestSpecRoundtrip` — defaults None; roundtrip; pre-v0.6.52
  reads clean; array_index >= 0 + array_total >= 1 validators.
* `TestGroupIdHelper` — 8 hex chars; distinct per call.
* `TestSubmitLocalArray` — N specs created with sequential
  indexes + shared group_id; per-element workspaces (no
  collision); array < 1 rejected; array=1 still uses the
  array path (returns 1-element list with array fields set).
* `TestDaemonEnv` — array spec → Popen receives `env=` with
  the three VQ_ARRAY_* keys + inherited PATH; non-array spec
  → `env=None` (legacy inherit).
* `TestStatusDisplay` — array line renders when set, omitted
  otherwise.
* `TestCLI` — `--array N` prints N jobids; `--array 1` falls
  back to single submit (no array fields set); `--array 0`
  rejected by Click's `IntRange`.

No security boundary changes; not patch-candidate.

---

### v0.6.51 — `vq submit --depends-on JOBID` dispatch gate (2026-05-24)

Job dependencies. SLURM-style `afterok` semantics: the daemon
holds a job PENDING until every predecessor reaches COMPLETED;
if any predecessor lands in a non-COMPLETED terminal state
(FAILED / KILLED / OOM_KILLED / STARVED / TIME_EXCEEDED /
INTERRUPTED / ABORTED_BY_QUEUE), the dependent cascade-fails
to FAILED with a `failure_reason` naming the failing
predecessor + its state.

Forms:

* `vq submit --depends-on J1 input.py` — one predecessor.
* `vq submit --depends-on J1 --depends-on J2 input.py` —
  multiple (AND semantics — all must succeed).
* `vq submit --depends-on $A --wait input.py` — synchronous
  A-then-B chain in shell.

Implementation:

* Spec: additive `depends_on: list[str] = []` field + additive
  `failure_reason: str | None` field (the latter is also a
  re-usable hook for other daemon-attributed failure modes —
  currently only the cascade-fail path writes it).
* Submit: `submit_local` / `submit_remote` accept `depends_on`;
  submit-time validation walks each predecessor against the
  submitter's queue dir and rejects unknown jobids with a clear
  message. Self-dependency rejected. Order is preserved
  (operator's stated intent); duplicates deduped. In multi-user
  mode the validation scope is the SUBMITTER's per-user queue
  only — cross-user dependencies are deliberately out of scope
  (the depender would otherwise need read access into a
  different user's state tree to even validate).
* Daemon dispatch loop (`_dispatch_pending`):
  * Builds `specs_by_id` from the v0.6.36 single-materialise
    `all_specs` list (no extra I/O).
  * Pre-pass over PENDING + non-empty-depends_on: if ANY
    predecessor is non-COMPLETED terminal → transition
    dependent to FAILED with `failure_reason` set + `finished_at`
    stamped; persist atomically.
  * `_deps_ready(s)` predicate joins the pending filter: a
    PENDING spec is dispatchable iff `_backoff_ready(s)` AND
    `_deps_ready(s)`.
  * Missing predecessor (cleanup'd, never submitted) is treated
    conservatively as "still waiting" so a misclick on
    `vq cleanup --delete` doesn't silently fail the dependent.
    Operator can `vq kill` to unblock.
* Status: `vq status` shows `depends_on: J1, J2 (ready /
  waiting: J1 (running) / failed: J1 / unresolved: J1)` when
  the field is set. `--json` adds a `depends_on_status` key with
  the same annotation. `failure_reason` surfaces as
  `failure: …` when set (cascade-fail visible at a glance).
* CLI: `submit` gains `--depends-on JOBID` (repeatable). SSH
  delegate forwards `--depends-on` per element; remote
  validation lives at the remote vq's submit layer.

Edge cases / non-goals:

* Cycles are NOT detected. Two jobs depending on each other
  manifest as "both stay PENDING forever" — surfaces in
  `vq queue`; operator's responsibility.
* Cross-user dependencies (multi-user mode, depending on
  another user's job) are unsupported. Would need a privileged
  validation path.
* Cascade is one-hop only: a chain A → B → C with B failing
  cascades to C as "predecessor B failed (state=FAILED)", not
  "predecessor A failed via B". Once C lands in FAILED, any
  D that depends on C cascades on C's failure too.

New test file: `tests/test_depends_on_v0_6_51.py` (27 tests):

* `TestSpecRoundtrip` — default empty list, roundtrip, pre-v0.6.51
  specs read clean.
* `TestSubmitValidation` — unknown predecessor rejected; valid
  predecessor accepted; duplicates deduped in order.
* `TestDispatchGate` — PENDING predecessor → still PENDING;
  COMPLETED predecessor → dispatches; multi-predecessor needs
  all; missing predecessor stays PENDING (does NOT cascade-fail).
* `TestCascadeFail` — parametrised over every non-COMPLETED
  terminal state; one-failed-one-pending cascades immediately;
  COMPLETED does not cascade.
* `TestStatusAnnotation` — ready / waiting / failed / unresolved
  one-token annotations; text + JSON renderers include
  `depends_on` + `depends_on_status`.
* `TestCLI` — `vq submit --depends-on` writes spec; unknown
  predecessor surfaces a clean error.

No security boundary changes; not patch-candidate.

---

### v0.6.50 — `vq logs JOBID` verb with `--follow` (2026-05-24)

QoL companion to `vq status` and `vq tail`. `vq status` shows
metadata + a small log tail but doesn't follow; `vq tail` is a
generic `exec tail(1)` against an arbitrary file in the workspace.
`vq logs` is the spec-aware "show me this job's output" view that
operators reach for in the common case.

Forms:

* `vq logs JOBID` — both streams, last 100 lines each,
  banner-separated (`--- stdout ---` / `--- stderr ---`).
* `vq logs HOST JOBID` — explicit host.
* `vq logs JOBID --stdout | grep …` — stdout only, no banner
  (so piped greps don't pick up the banner line).
* `vq logs JOBID --stderr` — stderr only, no banner.
* `vq logs JOBID -n 0` — whole output (no tail).
* `vq logs JOBID -f` — follow until the job is terminal AND log
  files have been idle for two consecutive polls. No Ctrl-C
  needed — chain in shell scripts safely.
* `vq logs JOBID --json` — machine-readable: `{jobid, state,
  stream, tail, stdout_path, stderr_path, stdout, stderr}`
  (stream-specific keys present only when requested). `--follow
  --json` rejected (no agreed streaming-JSON shape).

Implementation:

* New module `src/vq/logs.py`: `tail_file`, `show_logs`,
  `show_logs_json`, `follow_logs` (generator). Multi-user aware
  via `paths.resolve_spec_path(jobid, multi_user=True)`. Archived
  jobs surface the `vq cleanup --restore` hint instead of an
  empty tail. Terminal specs get `last_status_at` stamped (same
  v0.5.10 auto-cleanup hint pattern that `vq status` already
  uses).
* `follow_logs` is a generator (`Iterator[str]`) so the CLI layer
  streams each chunk directly to stdout and tests drive the loop
  via injected `sleep` / `now` callbacks without wall-clock
  waits. Per-stream cursors track file size; new bytes are
  decoded and (for `stream="both"`) prefixed with `[stderr]` on
  lines from stderr so the operator can tell them apart in the
  merged stream. Truncation / rotation detected by a size shrink
  → cursor reset.
* CLI verb `logs_cmd` in `cli.py`, sitting next to `status`.
  Remote-host case delegates `vq logs localhost JOBID ...` via
  the existing `_delegate_to_remote` plumbing; `-f` works over
  SSH because the remote vq runs `follow_logs` and stdout streams
  back through the pipe.
* `vq/status.py`'s `_tail_file` is now a thin re-export of
  `logs.tail_file` (same helper, shared between the two verbs;
  `__all__` declared so the long-standing import name keeps
  working for any external caller).

New test file: `tests/test_logs_v0_6_50.py` (30 tests):

* `TestTailFile` — the shared tail helper sentinels + truncation.
* `TestShowLogs` — banner-separated default, --stdout / --stderr
  single-stream, archived state, missing log file, custom tail,
  unknown jobid, remote-host rejection, terminal `last_status_at`
  stamping.
* `TestShowLogsJSON` — JSON shape + stream-filtered keys +
  `state` included.
* `TestFollowLogs` — initial tail yielded first; terminates on
  terminal+idle; streams new bytes as files grow (driver appends
  between polls); `[stderr]` prefix on the merged stream;
  archived returns hint and stops.
* `TestLogsCLI` — end-to-end via CliRunner: default form, --tail
  0 = whole output, --stdout / --stderr mutual exclusion, --follow
  + --json rejection, --json shape, unknown jobid clean error,
  remote delegate carries the `logs` verb + flags, `-f` doesn't
  hang on a terminal job (run in a thread with a 10-second
  watchdog).

No security boundary changes; not patch-candidate.

---

### v0.6.49 — `vq admin auto-update --all` + `--all-hosts` (2026-05-24)

Fleet-scale entry point for the auto-update verb. v0.6.11 shipped
the per-env CLI; v0.6.47 shipped the per-env-per-host systemd-timer
template; v0.6.48 closed the auth gap. The remaining shape gap:
an operator with N envs × M hosts needed N×M cron entries / timer
instances to drive the verb. v0.6.49 collapses both dimensions.

Forms (new):

* `vq admin auto-update --all` — every venv env on default_host.
* `vq admin auto-update --all HOST` — every venv env on HOST.
* `vq admin auto-update ENV --all-hosts` — one env, every host.
* `vq admin auto-update --all --all-hosts` — the full matrix.

Surface mirrors `vq admin update --all` / `--all-hosts` exactly:

* New module helper `auto_update.auto_update_all(cfg, *, host,
  dry_run) -> list[AutoUpdateOutcome]` iterates `kind="venv"`
  programs in sorted-by-name order. Raises `AdminError` only on
  an empty registry.
* Per-env failure isolation: one env's `ls-remote` error or apply
  failure does NOT abort the sweep. Each env's outcome is collected
  and rendered; exit code non-zero if any env hit an error or
  apply-failure.
* Per-host failure isolation on `--all-hosts`: matches the
  v0.5.37 `update --all-hosts` pattern (sequential delegation,
  per-host failures tracked in a closure list, exit code non-zero
  if any host failed).
* Pause/resume is per-env, NOT batch-bracketed: each env's apply
  reuses `auto_update_env` → `admin.update_env`, which pauses,
  pulls, runs the update script, then resumes. Batch-wide bracketing
  would keep the queue paused across N envs' (network-bounded)
  `ls-remote` probes; per-env bracketing is the right granularity.
* SSH delegate (`--all-hosts` path) carries the v0.6.48
  `--token-stdin` forwarding fix verbatim — bearer never lands on
  argv on either side of the SSH tunnel, regardless of `--all` /
  single-env shape.
* JSON output: `--all` emits an array of per-env objects;
  `--all-hosts` emits a top-level object keyed by host (matches the
  v0.5.46 `update --all` shape).

Caveat caught by these tests: the pre-v0.6.49 single-env body
referenced `outcome.update_result.errors`, but the dataclass field
is `.work_errors`. Only worked by accident because the old body
echoed "apply: FAILED" *before* hitting the AttributeError on the
for-loop. The new shared formatter builds the whole text block
first, which surfaced the typo — fixed to `.work_errors` in
`_format_outcome_text`.

New file: `tests/test_auto_update_all_v0_6_49.py` (13 tests):
module-level iteration order + per-env failure isolation + empty-
registry raise; CLI `--all` happy path / mixed outcomes / JSON;
positional + flag mutual exclusion; `--all-hosts` per-host
delegation + token-via-stdin verification + per-host failure
isolation; the v0.6.48 multi-user gate firing ONCE up-front
before any iteration begins.

No security boundary changes; not patch-candidate.

---

### v0.6.48 — `vq admin auto-update` admin-token gate (SECURITY, 2026-05-24)

**Audit observation surfaced by v0.6.47 (see drop-box); fixed
here.** Pre-v0.6.48 the auto-update CLI verb skipped the v0.6.44
admin-token check. The gate lived only on `vq admin update`;
`vq admin auto-update` called `admin.update_env` via the Python
layer (`auto_update.auto_update_env`) without first running the
`cfg.multi_user.enabled and verify_admin_token(...)` check at the
CLI layer. On a multi-user host that meant any local-shell user
could trigger a privileged env refresh on the root-owned venv. Blast
radius bounded — the verb only fast-forwards to the newest semver
tag on the configured remote — but same auth-bypass *class* as
v0.6.44 on a sibling verb. The v0.6.47 systemd-timer ship made
this surface easier to reach (one timer per env), which is why the
v0.6.47 drop-box explicitly flagged it for the next pass.

Fix mirrors the v0.6.44 + v0.6.46 surface on the auto-update CLI
command:

* `--token TOKEN` / `--token-stdin` / `--token-file PATH` flags
  added (matching `vq admin update`); mutually exclusive.
* `resolve_token` precedence: CLI argv (with the v0.6.46 argv-
  exposure stderr warning) → stdin → file → `$VQ_TOKEN`.
* Gate: `cfg.multi_user.enabled and verify_admin_token(...)` —
  rejects with `"admin auto-update: token required in multi-user
  mode."` when the token is missing or wrong.
* SSH-delegate path forwards the token via `--token-stdin` (mirrors
  v0.6.46 for `vq admin update`) — keeps it off the local ssh argv
  AND the remote `sh -c` argv.

Single-user mode unchanged. The systemd-timer template (v0.6.47)
runs as root with `VQ_CONFIG_DIR=/etc/vq` and reads the
`/etc/vq/web-token` file the daemon already requires — no operator
action needed for the timer path.

New file: `tests/test_admin_auto_update_token_v0_6_48.py` (11
tests): multi-user rejects no token / wrong token / missing token
file; multi-user accepts via $VQ_TOKEN / --token-stdin / --token-
file / --token (with argv warning); single-user passes without a
token; mutually-exclusive flag combos rejected; SSH delegate uses
`--token-stdin` rather than a `--token VALUE` argv element.

`Patch-candidate: v0.8.x, v0.9.0` (same severity tier as v0.6.44 +
f4104db4 — multi-user auth bypass on a privileged verb).

---

### v0.6.47 — ship systemd-timer template units for `vq admin auto-update` (2026-05-24)

**Deferred-by-design item, taken with explicit maintainer
approval.** The original v0.6.x decision was to ship only the
CLI verb (`vq admin auto-update ENV`, v0.6.11) and let operators
wire their own systemd-timer / cron entry around it — keep the
auto-deploy surface small. The maintainer reversed that for
v0.6.47 so a multi-user operator can opt in with two `cp`s + one
`systemctl enable`.

* `contrib/vq-admin-auto-update@.service` — template unit (one
  instance per env). `ExecStart=/opt/vq/venv/bin/vq admin
  auto-update %i`, `User=root`, `Type=oneshot`,
  `Environment=VQ_CONFIG_DIR=/etc/vq` (matches the daemon's unit
  so both find the system config).
* `contrib/vq-admin-auto-update@.timer` — fires the matching
  `.service` instance daily with `RandomizedDelaySec=1h`
  (avoids the cross-fleet `git ls-remote` thundering herd) and
  `Persistent=true` (catches up a missed run on next boot).
* `docs/multi_user_deployment.md` — new "Auto-updating env tags"
  section with the install commands + cadence-editing examples.

Single-user mode unchanged. The original three opt-ins (operator
types the env, latest-tag-only, optional `--dry-run`) still
gate the verb itself — this ship just hands operators a ready
systemd integration instead of asking them to copy/paste one.

**Flagged for the audit chat (pre-existing, not introduced
here).** `vq admin auto-update` does NOT currently require the
v0.6.44 admin bearer token; only `vq admin update` does. The
timer runs as root (system manager → trusted scheduler), but a
local user with shell access can also trigger `vq admin
auto-update` manually. Surfaced explicitly in the
`.service` header comment + the deployment doc + the v0.9.0
drop-box so the next audit pass can decide whether to gate the
verb.

**Tests** (+8 in `test_admin_auto_update_units_v0_6_47.py`):
both unit files exist; the `.service` runs the right CLI verb
with the multi-user config dir set + `User=root` + `Type=oneshot`;
the `.timer` references the `.service` template instance
explicitly, has an `OnCalendar` schedule, and installs into
`timers.target`. Sanity-level by design — the units are config,
not Python; the behaviour the `.service` triggers is covered by
the v0.6.11 auto-update tests.

1599 passed / 11 skipped on macOS (+8 from v0.6.46's 1591).

---

### v0.6.46 — reduce admin-token exposure on argv (2026-05-24)

**Audit-driven hardening (security review #3, 2026-05-24 pass).**
`vq admin update --token TOKEN` put the bearer onto two argv
surfaces: (1) the laptop's `ps -ef` — both the user's vq
invocation and the follow-up ssh forwarding — and (2) the remote
host's `ps -ef`, since SSH unwraps the argv into a sh -c command
line. Shell `HISTFILE` was a third surface.

* New `--token-stdin` flag — reads one line from stdin. The
  intended scripted-caller pattern is
  `printf '%s\n' "$tok" | vq admin update --token-stdin ...`.
* New `--token-file PATH` flag — reads from a 0600-mode file with
  the same perm enforcement as `~/.config/vq/web-token`.
* `$VQ_TOKEN` env var (already supported pre-v0.6.46) is now the
  documented default for interactive use.
* The three CLI input flags are mutually exclusive; ambiguity is
  a hard `UsageError` rather than silent precedence.
* `--token TOKEN` stays for backwards compat but emits a loud
  stderr warning at use, listing the safer channels. Suppress
  with `VQ_SUPPRESS_TOKEN_ARGV_WARNING=1` for callers who have
  weighed the trade-off.
* The remote-dispatch closure in `admin update` (the multi-host
  forwarding via `_delegate_to_remote`) now sends `--token-stdin`
  + a stdin pipe instead of `--token TOKEN` on argv, so the
  bearer never appears on either side of the SSH tunnel.
  `transport.run_remote_vq` gained an optional `stdin_data`
  kwarg to carry it.
* `auth.redact_token_args` scrubs `--token VALUE` pairs from the
  argv passed to the debug log line in `transport.run_remote_vq`,
  so the token isn't preserved verbatim in journal entries / log
  captures (the live ssh argv is unchanged — argv exposure is a
  separate mitigation, addressed by the stdin pipe above).

Single-user mode is untouched. The argv warning is intentionally
noisy because the safer channels exist and the user just hasn't
noticed yet.

**Tests** (+23 in `test_token_argv_exposure_v0_6_46.py`):
`resolve_token` with stdin / file / env-var / perm-refusal / empty
input; `warn_argv_token_exposure` emits to stderr and respects the
suppression env var; `redact_token_args` redacts `--token VALUE`
pairs without touching `--token-stdin` or other args; CLI-level
mutual-exclusivity of the three input flags; `--token` emits the
audit warning; `transport.run_remote_vq` plumbs `stdin_data` into
`subprocess.input` and keeps the token off the ssh argv; the
debug log line redacts the token value.

---

### v0.6.45 — `vq web run` warns on non-loopback bind (2026-05-24)

**Audit-driven hardening (security review #2, 2026-05-24 pass).**
The read-only HTML pages and the OpenAPI `/docs` endpoint have no
auth — only the write endpoints carry the bearer-token gate
(`require_token`). The default bind is `127.0.0.1`, so a stock
deployment is fine, but the operator who passes `--host 0.0.0.0`
(or any LAN IP) on a host without a fronting TLS reverse proxy
exposes every job's name, working directory, stdout/stderr tail,
host metadata, and queue state to anyone who can reach the port.

* `vq web run` now classifies its `--host` against
  `_is_loopback_bind` (literal `"localhost"`, 127.0.0.0/8, `::1`).
  Any non-loopback bind triggers a loud stderr warning at startup
  that names the exposure surface and points at the silence flag.
* New `--i-understand-public-bind` flag suppresses the warning
  once the operator has confirmed a reverse-proxy ACL is in
  place. No-op for loopback binds (no double-warning, no spurious
  output).
* `--help` text + `docs/web.md` audit-note paragraph updated to
  match.

Single-user-laptop usage is unchanged (default bind is
loopback → silent). The warning is intentionally noisy because
the audit risk is real — production deployments behind caddy /
nginx with their own ACL hit it exactly once and pass the flag.

**Tests** (+16 in `test_web_bind_warning_v0_6_45.py`): classifier
coverage (IPv4 loopback range, `::1`, `localhost`, wildcards,
LAN, public IPs, unresolved hostnames); CLI-level warning
emission on `0.0.0.0` / LAN; ack-flag silences non-loopback;
ack-flag is a no-op on loopback; uvicorn receives the chosen
host/port unchanged.

---

### v0.6.44 — SECURITY: missing admin-token file rejects `vq admin update` (2026-05-21)

**Multi-user admin-auth bypass.** `auth.verify_admin_token()`
returned `True` when no token file existed, justified as a
"single-user compat" path. But the only caller — `vq admin
update` in `cli.py` — already gates the call on
`cfg.multi_user.enabled`, so the compat branch never fired in
single-user mode. In multi-user mode it instead **silently
bypassed the admin gate** on any host that lacked a token file
(e.g. before `vq web init-token` had run, or after a
misconfigured token-file wipe) — anyone with shell access on the
host could run admin verbs unauthenticated.

The pre-fix `test_multi_user.py::test_verify_admin_token_no_token_file`
*locked in this bug* by asserting `verify_admin_token("any") is
True` against a missing file. That assertion is now inverted.

Fixed: `verify_admin_token` returns `False` when no token file
exists (matches what the web API's `require_token` has always
done — FastAPI raises 503 on missing token). The CLI's "token
required in multi-user mode" error now also fires for the
missing-file case.

Carries `Patch-candidate: v0.8.x, v0.9.0` (security backport,
mirrors the v0.6.35 privesc fix).

**Fleet status:** compute-d + compute-a both have a token file
(rotated 2026-05-21), so the bypass condition does not currently
apply on the fleet. A future deployment that omits or wipes the
token file would have been exposed under the old code.

**Tests** (+4 in `test_admin_token_security_v0_6_44.py`):
`verify_admin_token("")` and `verify_admin_token("anything")`
return `False` for a missing file (the CLI call shape); the happy
path (correct token accepted, wrong token rejected) still works;
end-to-end `vq admin update` in multi-user mode with no token
file fails with the token-required ClickException. The existing
buggy assertion in `test_multi_user.py` was inverted.

1552 passed / 11 skipped on macOS (+4 from v0.6.43's 1548).

---

### v0.6.43 — the manual `vq cleanup` CLI verb works in multi-user mode (2026-05-21)

**Round-2 hardening audit — final follow-up.** The daemon's
*auto*-cleanup was fixed in v0.6.39; this is the operator-facing
manual verb. `vq cleanup`'s candidate discovery
(`find_candidates` / `find_candidates_by_jobid`) resolved
terminal jobs from the single-user `paths.queue_dir()`, so on a
multi-user host `vq cleanup` listed nothing and could
archive/delete/restore nothing.

* `find_candidates` gains a `multi_user` flag: it sweeps every
  per-user queue dir (`paths._all_user_dirs()`).
* `find_candidates_by_jobid` resolves each id via
  `paths.resolve_spec_path`.
* `Candidate` gains a `uid` field — both discovery functions stamp
  it with the owning uid (the per-user dir the spec came from).
* The `vq cleanup` CLI threads the v0.6.30 multi-user autodetect:
  discovery runs multi-user, and the archive / delete / restore
  actions use each candidate's own `user_queue_dir` /
  `user_archive_dir` (an explicit `--archive-dir` still
  overrides). The action functions (`archive_workspace` /
  `delete_job` / `restore_workspace`) were already parameterised
  on `queue_dir` / `archive_dir`, so no signature change there.

Single-user mode is untouched.

**This closes the round-2 multi-user hardening audit and both of
its follow-ups.** Every queue-reading / job-acting code path —
dispatch, the daemon's safety + maintenance passes, and every
`vq` verb — is now multi-user-correct.

**Tests** (+4 in `test_cleanup_cli_multi_user_v0_6_43.py`):
`find_candidates` sweeps per-user dirs and stamps `uid`;
`find_candidates_by_jobid` resolves per-user; the `vq cleanup`
CLI lists and deletes a per-user job.

1548 passed / 11 skipped on macOS (+4 from v0.6.42's 1544).

---

### v0.6.42 — `vq admin update`'s surgical pause works in multi-user mode (2026-05-21)

**Round-2 hardening audit follow-up.** `admin.update_env` /
`admin.update_all` quiesce the queue around an env rebuild —
`pause_all` / `pause_provides_branches` before the work,
`resume_all` / `resume_jobs` after. None of those four calls
threaded `multi_user`, so on a multi-user host the env-update
pause was a no-op: a job from any user could dispatch mid-rebuild
and see a half-updated venv.

Resolved with the maintainer's confirmation that **`vq admin
update` runs as root on a multi-user host** — so the pause/resume
helpers (root) can signal every user's process group. Both
`update_env` and `update_all` now compute `multi_user`
(`cfg.multi_user.enabled or config.system_multi_user_enabled()`)
and pass it to all four pause/resume calls; the v0.6.38
`pause_resume` machinery then resolves specs across the per-user
state dirs.

This clears the last of the two round-2-audit follow-ups
documented in v0.6.40. The remaining item — the *manual* `vq
cleanup` CLI verb (the daemon's auto-cleanup was fixed in
v0.6.39) — is still tracked.

**Tests** (+3 in `test_admin.py::TestMultiUserUpdatePause`, and
the existing `pause_all`/`resume_all`/surgical-pause spies in
`test_admin.py` updated to assert the `multi_user=` kwarg): with
a `[multi_user]` config, `update_env` (pause-all + surgical
paths) and `update_all` thread `multi_user=True`.

1544 passed / 11 skipped on macOS (+3 from v0.6.41's 1541).

---

### v0.6.41 — web dashboard + `vq overview` work in multi-user mode (2026-05-21)

**Round-2 hardening audit, milestone 4 (final audited module).**
The read-only web dashboard (`vq.web`) and `vq overview` /
`vq summary` read job state from the single-user
`paths.queue_dir()`. In multi-user mode jobs live under
`/var/lib/vq/users/<uid>/queue/`, so on a multi-user host the web
`/queue` page rendered empty, `/jobs/<id>` 404'd, and
`vq overview` reported 0 jobs everywhere.

* `create_app()` detects multi-user mode once (from
  `[multi_user] enabled` in the system or loaded config) and
  threads it into every queue read: the `/queue` +
  `/queue/_table` pages and the host-pressure badge
  (`list_jobs(..., multi_user=...)`), the `/jobs/<id>` +
  `/jobs/<id>/_log` detail routes (a new `_resolve_job_spec_path`
  helper searches the per-user dirs), the `/health/ready` probe
  (checks the multi-user root), and the `/api/v1/` kill / pause /
  resume / queue-pause / queue-resume write actions.
* `overview.gather_overview_local` gains a `multi_user` flag
  forwarded to `list_jobs`; the `vq overview` CLI passes it via
  the v0.6.30 autodetect. The remote (SSH `vq overview --json`)
  path already runs the CLI on the target host, which resolves
  its own mode.

Single-user mode is untouched.

This completes the round-2 multi-user hardening audit: the audit
covered `pause_resume` (v0.6.38), `cleanup` (v0.6.39), `resubmit`
+ `wait` (v0.6.40), and `web` + `overview` (this) — every queue-
reading module is now multi-user-correct. Two operator-visible
follow-ups remain documented: `admin.py`'s `vq admin update`
surgical pause, and the *manual* `vq cleanup` CLI verb.

**Tests** (+5 in `test_web_overview_multi_user_v0_6_41.py`): the
web `/queue` page + `/jobs/<id>` route resolve per-user jobs, an
unknown job 404s, and `gather_overview_local` counts per-user
jobs only with `multi_user=True`.

1541 passed / 11 skipped on macOS (+5 from v0.6.40's 1536).

---

### v0.6.40 — `vq wait` + `vq resubmit` work in multi-user mode (2026-05-21)

**Round-2 hardening audit, milestone 3.** Both verbs resolved
job specs from the single-user `paths.queue_dir()`. In multi-user
mode specs live under `/var/lib/vq/users/<uid>/queue/`, so on the
multi-user fleet:

* `vq wait JOBID` failed with "no such job" — and `vq submit
  --wait` (which shares the wait path) could never block on a
  multi-user submission.
* `vq resubmit JOBID` could not find the source. Worse, even if
  it had: `resubmit_local` wrote the new spec with a `user@host`
  `submitter`, which the **v0.6.35 dispatch gate rejects** inside
  a per-user queue dir — so a resubmitted job would land `FAILED`
  immediately.

Fixes (all behind a `multi_user` flag, default `False`):

* `wait_for_terminal_local` / `wait_for_terminal` resolve the
  spec via `paths.resolve_spec_path(jobid, multi_user=True)`. The
  remote (SSH `vq status`) path is mode-agnostic and unchanged.
* `resubmit_local` resolves the source from the per-user dir,
  derives the **owning uid** from the resolved path (the trusted
  signal — `users/` is root-owned), and lands the new job — spec,
  deep-copied workspace, and **numeric-uid `submitter`** — in
  that same user's tree. The daemon's v0.6.35 `_chown_tree` fixes
  workspace ownership at dispatch, so a resubmit run by root or
  by the owner both work.
* `resubmit_state` (bulk `--state`) sweeps every per-user queue
  dir.
* `vq wait`, `vq submit --wait`, `vq resubmit`, and `vq resubmit
  --state` (CLI) route through the v0.6.30 multi-user autodetect.

Single-user mode is untouched.

**Tests** (+5 in `test_wait_resubmit_multi_user_v0_6_40.py`):
`wait` resolves a per-user spec; `resubmit_local` lands the new
job in the owner's tree with a numeric-uid submitter;
`resubmit_state` sweeps per-user dirs; single-user mode does not
see per-user jobs.

1536 passed / 11 skipped on macOS (+5 from v0.6.39's 1531).

---

### v0.6.39 — daemon auto-cleanup sweeps the per-user state trees (2026-05-21)

**Round-2 hardening audit, milestone 2.** The daemon's opt-in
auto-cleanup pass (`run_auto_cleanup_pass`) resolved terminal jobs
from the single-user `paths.queue_dir()`. In multi-user mode job
state lives under `/var/lib/vq/users/<uid>/`, so on a multi-user
host the pass **silently never touched any job** — once
auto-cleanup was enabled, terminal jobs (and their workspaces)
would accumulate on disk forever.

* `run_auto_cleanup_pass` gains a `multi_user` flag: when set it
  sweeps every per-user state tree (`paths._all_user_dirs()`),
  archiving each user's workspaces into **their own**
  `user_archive_dir`. The per-pass archive+delete logic was
  factored into `_archive_and_delete_pass` so the single-user and
  per-user paths share one implementation.
* `auto_cleanup_policy_path()` is multi-user-aware — the
  `auto-cleanup.json` policy file is daemon-wide, so in multi-user
  mode it lives at the system root (`multi_user_root()`),
  alongside `daemon.pid` / `throttle.json` (mirrors the v0.6.37
  `throttle_state_path()` fix).
* The daemon's `_maybe_auto_cleanup` passes
  `multi_user=self._multi_user`.

Single-user mode is untouched (flag defaults `False`). The
per-pass building blocks (`find_candidates`, `archive_workspace`,
`delete_job`) already took `queue_dir` / `archive_dir` params, so
no per-job-function signature changes were needed.

**Known follow-up (documented, not a discovered gap):** the
*manual* `vq cleanup` CLI verb still resolves from the single-user
queue dir — on a multi-user host it lists nothing. The daemon's
auto-cleanup (the silent, cumulative failure) is the one this
milestone fixes; the CLI verb is operator-visible and tracked for
a later milestone.

**Tests** (+5 in `test_cleanup_multi_user_v0_6_39.py`): the pass
archives / deletes / spares per-user jobs by age; the policy file
path follows the mode; a single-user pass does not touch per-user
dirs.

1531 passed / 11 skipped on macOS (+5 from v0.6.38's 1526).

---

### v0.6.38 — pause/resume work in multi-user mode (2026-05-21)

**From the round-2 hardening audit. Fixes a silent safety-feature
failure on the multi-user fleet.** Every `pause_resume.py` entry
point resolved job specs from the single-user `paths.queue_dir()`.
In multi-user mode specs live under `/var/lib/vq/users/<uid>/
queue/`, so on compute-d + compute-a:

* `vq pause` / `vq resume` failed with "no such job"; and —
  worse —
* the daemon's **v0.6.20 host-pressure auto-pause** (the
  OOM-prevention safety feature) **silently no-op'd**:
  `_host_pressure_pass` calls `pause_job`, which raised
  `FileNotFoundError` for every per-user job and was swallowed by
  the best-effort `except`. Under a memory spike the daemon would
  log "failed to pause" and the OOM cascade the feature exists to
  prevent could proceed.

All six entry points now take a `multi_user` flag:

* `pause_job` / `resume_job` resolve via `paths.resolve_spec_path(
  jobid, multi_user=True)` — searches the per-user dirs. They also
  now translate a cross-user `os.killpg` `PermissionError` into a
  clear `PauseError` (only root or the job's owner can signal it).
* `pause_all` / `resume_all` / `pause_provides_branches` sweep
  every per-user queue dir; `resume_jobs` resolves each id there.
* The daemon's `_host_pressure_pass` passes
  `multi_user=self._multi_user` to `pause_job` / `resume_job`, so
  the safety feature works on the fleet again.
* `vq pause` / `vq resume` (CLI) route through the v0.6.30
  multi-user autodetect.

Single-user mode is untouched (the flag defaults to `False`).

**Known follow-up (documented, not a discovered gap):** `vq admin
update`'s surgical pause (`admin.py` → `pause_all` /
`pause_provides_branches` / `resume_jobs` / `resume_all`) does not
yet pass `multi_user`, so the env-update pause is still
single-user-scoped. Tracked for a follow-up milestone — it needs
the "does `vq admin update` run as root on a multi-user host"
question resolved first.

**Tests** (+7 in `test_pause_resume_multi_user_v0_6_38.py`): all
six entry points resolve / sweep the per-user dirs; single-user
mode unaffected.

1526 passed / 11 skipped on macOS (+7 from v0.6.37's 1519).

---

### v0.6.37 — `vq throttle` works in multi-user mode (2026-05-21)

**From the v0.6.35 hardening audit (finding B).** `vq throttle`
was doubly broken on a multi-user host:

* `cgroup.set_cpu_weight` issued `systemctl --user set-property` —
  the wrong manager for a job's root-owned *system* scope — and
  was gated behind `available()`, which probes `--user` scope
  delegation. Both made it silently no-op. (v0.6.28 fixed the
  same `--user` bug for `scope_main_pid` / `scope_exists` /
  `stop_scope` but missed this one helper.)
* `throttle_job` read the spec from the single-user
  `paths.queue_dir()`; in multi-user mode the spec lives under
  `/var/lib/vq/users/<uid>/queue/`, so it failed with "no such
  job" before reaching the cgroup call.

Fixed, model **root-only / any job** (a job's scope is a
root-owned system scope — adjusting it needs root):

* `cgroup.set_cpu_weight` takes a `multi_user` flag → targets the
  system manager (no `--user`) and skips the `available()` gate.
* `throttle.py` (`throttle_job` / `throttle_all` / `restore_job`
  / `restore_all` / `_apply_throttle` / `apply_persistent_throttle_
  if_set`) threads `multi_user` through: specs resolve from the
  per-user dirs, `throttle --all` sweeps every user's queue, and
  a non-root caller gets a clear "run as root (sudo)" error
  instead of a raw permission failure. `throttle_state_path()`
  is multi-user-aware (the persistent-throttle file lives at the
  system root, shared by the root daemon and the root CLI).
* The daemon's `_start_job` passes `multi_user` into
  `apply_persistent_throttle_if_set`, so persistent throttle
  applies to newly-dispatched system scopes.
* `vq throttle` CLI computes multi-user via the v0.6.30
  autodetect and routes accordingly.

**Tests** (+11 in `test_throttle_multi_user_v0_6_37.py`):
`set_cpu_weight` manager selection + gate behaviour; `_apply_
throttle` multi-user path (no renice fallback); root requirement
on `throttle_job` / `throttle_all` (and single-user *not* gated);
`throttle_job` resolves the per-user spec and drives its system
scope; `apply_persistent_throttle_if_set` multi-user path.

1519 passed / 11 skipped on macOS (+11 from v0.6.36's 1508).

With this, all three v0.6.35-audit findings are closed (A = the
v0.6.35 privesc fix, B = this, C = v0.6.36).

---

### v0.6.36 — `_dispatch_pending` scans the queue dirs once per tick (2026-05-21)

**Multi-user dispatch hot-path cleanup, from the v0.6.35
hardening audit (finding C).** Pre-v0.6.36 the multi-user branch
of `_dispatch_pending` called `_iter_specs()` **three times** per
poll tick:

1. sort the PENDING list;
2. count SUSPENDED jobs against per-user quota;
3. re-sort PENDING (`_iter_specs` clears + repopulates `_job_uid`
   as a side effect, so the first `pending` list had to be
   rebuilt).

`_iter_specs()` globs and JSON-reads every per-user queue dir, so
on a busy multi-user host that tripled the per-tick stat + read
I/O for no behavioural gain. v0.6.36 materialises the spec list
**once** (`all_specs = list(self._iter_specs())`) and derives the
PENDING sort and the SUSPENDED tally from it; the single call
leaves `_job_uid` fully and stably populated for the rest of the
pass, so the rebuild-and-re-sort step is gone.

Pure performance refactor — no behaviour change. Single-user mode
already did one scan and is untouched.

**Tests** (+3 in `test_daemon_single_scan_v0_6_36.py`): a
SUSPENDED job still counts against `default_max_pending_jobs` and
`default_max_concurrent_cpus`; a SUSPENDED job + an orphan both
count in the same pass (the materialised-list path and the
`_orphans` path agree).

1508 passed / 11 skipped on macOS (+3 from v0.6.35's 1505).

---

### v0.6.35 — SECURITY: multi-user daemon vets the spec before acting as root (2026-05-21)

**Critical privilege-escalation fix.** In multi-user mode each
user **owns** their `/var/lib/vq/users/<uid>/queue/` directory —
they can drop a hand-crafted spec JSON straight in, bypassing
`vq submit`. The root daemon's `_start_job` trusted three
attacker-controlled fields:

* **`submitter`** picked the uid the job runs as — forge `"0"`
  and the job runs **as root**: arbitrary code execution as root
  for any provisioned user.
* **`cwd`** is the workspace the daemon `chown -R`s — point it at
  `/etc` (or `/`) and the daemon hands ownership to the attacker.
* **`stdout_path` / `stderr_path`** are joined onto `cwd` and
  opened `"ab"` as root — an absolute / `../` path escapes and
  creates a root-owned file anywhere.

The trustworthy uid is the per-user state directory the spec was
read from: `users/` is root-owned, so a user cannot forge a
`<uid>/` entry. `_iter_specs` already recorded it as `_job_uid` —
the per-user *quota* path used it, but the *privilege-drop* path
did not. The inconsistency was the bug.

New gate `Daemon._validate_multi_user_spec`, run at the top of
`_start_job` **before any filesystem op**:

1. `submitter` must equal the owning directory's uid (forged or
   misplaced specs are rejected, not dispatched).
2. `cwd` must resolve inside the submitter's own `jobs/` tree.
3. `stdout_path` / `stderr_path` must resolve inside the
   workspace.

A spec that fails the gate is landed `FAILED` with a logged
reason. The privilege drop now derives the run-uid from the
trusted `_job_uid` directly; `spec.submitter` is only ever a
consistency check. Orphan reattach (`_OrphanJob.uid`, v0.6.34)
likewise switched from `spec.submitter` to the directory uid.
Honest `vq submit` is unaffected — it already sets
`submitter = geteuid()` and a `cwd` inside the user's tree.

**Tests** (+11 in `test_multi_user_spec_validation_v0_6_35.py`):
honest spec + auto-resume sibling pass; forged `submitter` (root
and other-user), missing `submitter`, unknown owning dir, `cwd`
outside the jobs tree / in another user's tree, `stdout`/`stderr`
path escapes all rejected; `_start_job` rejects a forged spec
without creating its forged `cwd`.

1505 passed / 11 skipped on macOS (+11 from v0.6.34's 1494).

Fleet impact: compute-d + compute-a both run multi-user and need this —
re-install from a root-owned venv and restart the unit.

---

### v0.6.34 — per-user `[quotas]` count reattached orphans (2026-05-21)

**Finishes the v0.6.29 orphan-accounting fix.** v0.6.29 made the
*global* dispatch budgets (`max_jobs` / `max_cpus` / `max_mem`)
count reattached orphans — jobs still alive from a previous daemon
process — but deferred the per-user `[quotas]` analogue. Until this
fix, a daemon restart let a user exceed their `max_pending_jobs` /
`max_concurrent_cpus` by the count + CPU footprint of their own
orphaned jobs: the per-user gate counted only `_running`, never
`_orphans`.

* `_OrphanJob` gains a `uid` field — the job's `submitter` (a
  numeric-uid string in multi-user mode, `None` single-user). The
  reattach site fills it from the spec.
* `_dispatch_pending`'s per-user quota tally now folds in
  `_orphans` alongside `_running`, keyed by that `uid`, before the
  per-user gate runs — so a user's reattached orphans hold their
  quota across a restart exactly as their in-process jobs do.

**Tests** (+3 in `test_multi_user_quota_orphans_v0_6_34.py`): an
orphan's CPUs count against `default_max_concurrent_cpus`; an
orphan counts against `default_max_pending_jobs`; control — with
no orphan the per-user gate does not hold the job.

Also fixes a latent flaky test: `test_subcommand_invocation_logged`
asserted `"queue"` in the logged `sys.argv`, which passed only by
accident when pytest's cwd path contained the substring (a
`vibe-queue/` path). Pinned `sys.argv` so the assertion is
deterministic.

1494 passed / 11 skipped on macOS (+3 from v0.6.33's 1491).

---

### v0.6.33 — `vq admin provision-user` (non-admin bootstrap) (2026-05-21)

**Closes the last multi-user submit-side gap.** On a multi-user
host per-user job state lives under `/var/lib/vq/users/<uid>/`, and
that tree is root-owned — an unprivileged user cannot create their
own subdir, so their first `vq submit` fails with `PermissionError`.
v0.6.27 had the daemon auto-provision a state dir for every
`admin_group` member at startup, but a **non-admin** user still hit
the wall (the runbook's standing "Known limitation").

New verb: **`vq admin provision-user USER`** — creates
`/var/lib/vq/users/<uid>/{queue,jobs,archive}`, chowned to the
user. `USER` is a numeric uid or a username. Run once, as root, for
any user not in the admin group; their `vq submit` works
thereafter.

* Multi-user mode only — a `UsageError` in single-user mode (state
  there lives under `~/.local/share/vq/`, which `vq submit`
  creates itself). Uses the v0.6.30 autodetect, so it works under
  `sudo` with no `VQ_CONFIG_DIR`.
* Root-only — the chown to the target uid requires it; a clear
  `UsageError` with the `sudo …` re-run line otherwise.
* Idempotent — re-running keeps existing dirs and re-applies
  ownership (built on the v0.6.27 `paths.provision_user_state`).
* Local-only — no SSH delegation; the operator runs it as root on
  the multi-user host.

**Tests** (+7 in `test_admin_provision_user_v0_6_33.py`):
single-user rejected, non-root rejected, unknown user rejected,
provisions by uid, provisions by username, idempotent re-run, verb
listed in `vq admin --help`.

1491 passed / 11 skipped on macOS (+7 from v0.6.32's 1484).

With this, the multi-user feature has no remaining submit-side
bootstrap gap — admins are auto-provisioned by the daemon,
non-admins by this one-off verb.

---

### v0.6.32 — `web_token_path()` is multi-user aware (2026-05-21)

**Fixes a token-rotation footgun hit live on compute-a.** Rotating the
compute-a admin token with `sudo /opt/vq/venv/bin/vq web init-token
--force` wrote the new token to **`/root/.config/vq/web-token`** —
root's home — because `sudo` runs as root and no `VQ_CONFIG_DIR`
was set. But the daemon reads `/etc/vq/web-token` (its unit sets
`VQ_CONFIG_DIR=/etc/vq`). The "rotation" silently left the old,
leaked token live and dropped the new one where nothing reads it.

**Fix.** `auth.web_token_path()` resolution is now:

1. `$VQ_WEB_TOKEN_FILE` — explicit override.
2. `$VQ_CONFIG_DIR/web-token` — when a config dir is set explicitly
   (the daemon unit; an operator).
3. `/etc/vq/web-token` — **new**: on a multi-user host (system-wide
   `/etc/vq/config.toml` with `[multi_user] enabled = true`) with no
   explicit config dir, the token resolves next to the system
   config, where the root daemon reads it.
4. `~/.config/vq/web-token` — single-user default.

So `sudo vq web init-token` on a multi-user host now writes the
token where the daemon will actually read it — no `VQ_CONFIG_DIR`
needed. Mirrors the v0.6.30 client-side multi-user autodetect.

**Tests** (+5 in `test_web_token_path_v0_6_32.py`): `$VQ_WEB_TOKEN_FILE`
wins; explicit `$VQ_CONFIG_DIR` wins over multi-user; multi-user
host with no config dir → `/etc/vq/web-token`; single-user → the
config-dir default; a present-but-single-user `/etc/vq/config.toml`
does not divert the path.

1484 passed / 11 skipped on macOS (+5 from v0.6.31's 1479).

**Operational note:** when rotating a fleet token on a host that
predates v0.6.32, still pass `VQ_CONFIG_DIR=/etc/vq` explicitly:
`sudo VQ_CONFIG_DIR=/etc/vq /opt/vq/venv/bin/vq web init-token
--force`. Once the host runs v0.6.32 the bare command resolves
correctly on its own.

---

### v0.6.31 — `vq web init-token --quiet` (credential hygiene) (2026-05-21)

**Stops the deploy script leaking the admin bearer token.** `vq web
init-token` echoes the freshly generated token to stdout so an
interactive operator can copy it. The multi-user deploy script
(`contrib/deploy-multi-user.sh`) called it unqualified — so every
multi-user deployment printed the admin token into terminal
scrollback. During the compute-a bring-up that output was pasted into a
chat, putting a live credential in a transcript.

**Fix.** New `vq web init-token --quiet` flag: writes the 0600
token file but does **not** echo the token — it prints only the
file path. The deploy script now uses `--quiet` and tells the
operator to retrieve the token with `sudo cat /etc/vq/web-token`
when needed. Interactive (non-`--quiet`) behaviour is unchanged.

Scripted callers should always pass `--quiet` so the credential
never lands in scrollback, CI logs, or pasted output.

**Tests** (+5 in `test_web_init_token_quiet_v0_6_31.py`): default
still echoes the token; `--quiet` writes the file but the token
value is absent from stdout; `--quiet` still reports the path;
`--quiet` composes with `--force` (rotate quietly); the flag is
in `--help`.

1479 passed / 11 skipped on macOS (+5 from v0.6.30's 1474).

**Operational note:** tokens generated by the pre-v0.6.31 deploy
script (compute-d, compute-a) were surfaced and should be rotated —
`sudo /opt/vq/venv/bin/vq web init-token --force` on each host.

---

### v0.6.30 — client-side multi-user autodetect (2026-05-21)

**Removes the per-user config edit from multi-user deployment.**
On a multi-user host the root daemon reads `/etc/vq/config.toml`
(its unit sets `VQ_CONFIG_DIR=/etc/vq`), but the CLI reads each
user's `~/.config/vq/config.toml`. The two decided single- vs
multi-user independently — so until a user mirrored `[multi_user]
enabled = true` into their *own* config, `vq submit` wrote job
state to the single-user `~/.local/share/vq/` while the daemon
only ever looked at `/var/lib/vq/users/<uid>/`. The job simply
never dispatched. The compute-d bring-up hit exactly this.

**Fix.** `config.system_multi_user_enabled()` reads the canonical
system config (`config.SYSTEM_CONFIG_PATH = /etc/vq/config.toml`)
and reports whether it has `[multi_user] enabled = true`.
`cli._multi_user_active()` now returns
`cfg.multi_user.enabled OR system_multi_user_enabled()` — so the
client follows the *host's* mode with no per-user config edit.

`system_multi_user_enabled()` is best-effort: a missing file, a
parse error, or an unreadable file all return False — a broken
`/etc/vq/config.toml` can never break the single-user CLI.

**Tests** (+9 in `test_multi_user_autodetect_v0_6_30.py`):
`system_multi_user_enabled` — missing / enabled=true / enabled=false
/ no `[multi_user]` section / malformed TOML; `_multi_user_active`
— user-cfg opt-in, system-config override of a single-user user
cfg, both-single-user, cfg=None falling back to the system check.

1474 passed / 11 skipped on macOS (+9 from v0.6.29's 1465).

**Operational impact.** The "client must know it is multi-user"
known-limitation in `docs/multi_user_deployment.md` is resolved:
deploying multi-user no longer needs a `[multi_user]` stanza
appended to every user's personal config. (compute-d and compute-a
already have the manual stanza from earlier bring-up — harmless,
the OR just makes it redundant.)

---

### v0.6.29 — reattached orphans count against the dispatch budgets (2026-05-21)

**Fixes a real resource over-subscription bug**, caught while
updating compute-a after it came back online: `vq summary compute-a` showed
**4 jobs running against a `--max-jobs 2` cap** (4 live job scopes
confirmed it was real, not a display glitch).

**Root cause.** When a daemon restarts (admin-update, crash
recovery), jobs still alive from the previous daemon are
*reattached* — tracked in `Daemon._orphans`, not `_running`,
because the new daemon has no `Popen` handle for them. The dispatch
gate counted only `len(self._running)`:

```python
if effective_max_jobs is not None and len(self._running) >= effective_max_jobs:
```

So a restarted daemon saw `len(_running) == 0`, ignored N
reattached orphans, and dispatched a *fresh* `max_jobs` on top of
them — running **N + max_jobs** jobs. On compute-a: 2 orphans (from the
v0.6.x admin-update restart) + 2 fresh = 4. The same gap left the
`max_cpus` and `max_mem_mb` budgets short by the orphans' footprint
— exactly the over-commit that drives the OOM cascades this fleet
has fought.

**Fix.** `_orphans` was a bare `dict[str, int]` (jobid → pgid). It
is now `dict[str, _OrphanJob]`, a small dataclass carrying
`pgid` + `cpus` + `mem_mb` (read from the spec at reattach time).
The dispatch gate now counts orphans in all three budgets:

* job count — `len(self._running) + len(self._orphans)`
* CPU — orphan `cpus` summed into `used_cpus`
* memory — orphan `mem_mb` summed into `used_mem`

Both the pre-loop gate and the in-loop gate were corrected.

**Tests** (+4 in `test_daemon_orphan_budget_v0_6_29.py`): orphans
fill `--max-jobs` → PENDING job held; an orphan's cpus fill
`--max-cpus` → held; 1 orphan + `--max-jobs 2` allows exactly one
fresh dispatch; control — no orphans, free budget → dispatch
proceeds. Six existing orphan-recovery tests updated for the
`_OrphanJob` shape.

1465 passed / 11 skipped on macOS (+4 from v0.6.28's 1461).

**Affects every host, not just multi-user** — any daemon restart
with surviving jobs (the normal `vq admin update` path) hit this.
The fleet should pick up v0.6.29 promptly; until then a restart
can transiently double the running-job count.

---

### v0.6.28 — cgroup scope helpers target the right systemd manager (2026-05-21)

**Watchdog ↔ multi-user audit + fix.** With multi-user live on
compute-d, this audited whether the watchdog still protects jobs that
run in system-mode `systemd-run --scope --uid` scopes (not `--user`
scopes).

**Core enforcement: verified sound.** The watchdog's CPU/memory
sampling goes through `cgroup.cgroup_path_for_pid()` — a `/proc/
<pid>/cgroup` lookup that is path-based and manager-agnostic — and
the root daemon can read any cgroup's `cpu.stat` / `memory.current`.
OOM, STARVED, wall-time, and the v0.6.20 host-pressure pause are all
signal-based (SIGKILL / SIGTERM / SIGSTOP / SIGCONT), and root can
signal a process of any uid. So the safety net works for multi-user
jobs unchanged — nothing to fix there.

**Gap found + fixed: scope-management helpers.** Three helpers in
`cgroup.py` hard-coded `systemctl --user`:

* `scope_main_pid` — the v0.6.0 recovery cross-check.
* `scope_exists` — the v0.5.51 dispatch collision pre-flight.
* `stop_scope` — the leaked-scope recovery path.

A multi-user job scope is a *system* scope; `systemctl --user`
cannot see or stop it, so all three silently misfired (returned
None / False / failed) for multi-user jobs. They degraded
gracefully — `scope_main_pid` → PID-fingerprint fallback,
`scope_exists` → best-effort, `stop_scope` → spec lands FAILED with
a clear message — so no job was mis-handled, but the cross-checks
were dead weight in multi-user.

Each now takes `multi_user: bool = False`; a new
`_systemctl_scope_argv()` helper picks `[systemctl]` (system) or
`[systemctl, --user]`. The daemon passes `self._multi_user` at all
three call sites. Single-user behaviour is byte-identical.

**Not changed — throttle.** `cgroup.set_cpu_weight` is also
`--user`-bound, but `apply_persistent_throttle_if_set` gates on
`cgroup.available()` (a `--user` probe that fails for the root
daemon), so multi-user throttle already takes the v0.5.21 `renice`
fallback — which works (root renices any uid). Left as-is; the
renice path is correct, just not cgroup-CPUWeight.

**Tests** (+9 in `test_cgroup_scope_manager_v0_6_28.py`):
`_systemctl_scope_argv` user vs system; `scope_exists` /
`stop_scope` / `scope_main_pid` each query the matching manager
under `multi_user` True/False, with the verdict logic unchanged.

1461 passed / 11 skipped on macOS (+9 from v0.6.27's 1452).

---

### v0.6.27 — multi-user: auto-provision admin-group state dirs (2026-05-21)

**Closes the submit-side bootstrap gap.** With multi-user mode
live on compute-d, the first real submit surfaced a hole: per-user
state lives under `/var/lib/vq/users/<uid>/`, but `users/` is
root-owned, so an unprivileged user cannot create their own
`<uid>/` subtree — their first `vq submit` failed with
`PermissionError: [Errno 13] … /var/lib/vq/users/<uid>`. The
deployment runbook listed this as a "Known limitation" needing a
per-user `sudo mkdir`; that is too sharp an edge for routine use.

The fix uses the fact that the daemon already runs as root:

* **`paths.provision_user_state(uid, gid)`** — creates a user's
  `<users_root>/<uid>/` + `queue/` `jobs/` `archive/` tree and
  chowns every level to `uid:gid`. Idempotent.
* **`ownership.admin_group_uids(cfg)`** — resolves every member of
  the configured `admin_group` to a uid: supplementary members
  (`grp.gr_mem`) plus users whose *primary* gid is the group.
  Stale member names with no passwd entry are skipped.
* **`Daemon._provision_admin_user_dirs()`** — at multi-user
  startup, after `users/` is created, provisions a state dir for
  every admin-group uid. Best-effort per uid; logs a one-line
  summary (`provisioned N/M admin-group user state dir(s)`).

The model: **being in the `vq-admins` group gets you a working
queue automatically** — no per-user `sudo` step. A non-admin user
still needs a one-off provisioning step (a future `vq admin
provision-user` verb, or daemon-side lazy creation, would close
that too; deferred — admins are the common operator case).

**Tests** (+10 in `test_multi_user_provision_v0_6_27.py`):
`TestProvisionUserState` (3) — full tree created, idempotent,
chowned to target; `TestAdminGroupUids` (5) — single-user empty,
missing group empty, supplementary + primary-gid members
resolved, stale member skipped; `TestDaemonProvisionsAdminDirs`
(2) — daemon provisions reported uids, empty admin set is a
no-op.

1452 passed / 11 skipped on macOS (+10 from v0.6.26's 1442).

**Verified live:** multi-user is deployed on compute-d — a test job
submitted by uid 1000 dispatched and ran as **uid 1000, not
root**, confirming the v0.6.25 privilege-drop end-to-end. This
release removes the manual dir-bootstrap step that bring-up hit.

---

### v0.6.26 — fix `show_status_json` multi-user signature gap (2026-05-21)

**Unbreak `vq status --json`.** Commit `d8f7efb` ("make CLI verbs
multi-user aware") wired the multi-user `multi_user=` kwarg through
`cli.py` → `list_jobs` / `kill_job` / `show_status` /
`show_status_json`, and landed the `listing.py` / `kill.py`
callees. But `status.py`'s `show_status_json` got the `multi_user`
kwarg in its *body* and at its *call site* — and never in its own
*signature*. The result: `vq status --json` and `vq sacct --json`
crashed on `origin/main` with `TypeError: show_status_json() got
an unexpected keyword argument 'multi_user'`.

This release adds the missing `multi_user: bool = False` parameter
to `show_status_json`'s signature, so the call site, body, and
signature finally agree. It also bumps the version — the multi-user
CLI-wiring commits (`d8f7efb` and the earlier backbone) shipped
unversioned.

**Process note:** the break came from a partial cross-file change —
a kwarg added to a function's body + callers but not its own
signature. A green local suite is not proof a commit ships green
when the working tree carries uncommitted siblings; stage complete
cross-file changes together, or verify against a clean tree.

1442 passed / 11 skipped on macOS (unchanged from v0.6.25).

---

### v0.6.25 — multi-user privilege drop + flock-guard catch-up (2026-05-21)

**Makes the multi-user feature actually safe to deploy.** The
multi-user backbone landed in `0952d47` ("v0.6.x: multi-user
backbone, per-user quotas, bearer-token auth") with per-user state
dirs, ownership checks on kill/fetch, and admin-token auth — but a
**critical gap**: it never dropped privileges when spawning jobs.
`cgroup.wrap_command` hard-coded `systemd-run --user --scope`, so a
root multi-user daemon ran **every submitted job as root**. The
per-user state dirs and ownership checks were a façade with no
isolation behind them.

**Privilege drop — `cgroup.wrap_command(run_as_uid=, run_as_gid=)`.**
When a uid is given the wrap builds a **system-mode** transient
scope — `systemd-run --scope --uid=U --gid=G` (no `--user`) — so the
kernel runs the job as its submitter. In this mode the wrap is
**mandatory**: applied even with no resource caps, and a missing
`systemd-run` **raises** rather than returning a command that would
run as root. The single-user path is byte-for-byte unchanged.

**Daemon dispatch.** In multi-user mode the daemon resolves the
submitter's uid/gid, `chown`s the job workspace to them (so the
job — running as the user — can write its exit-code marker +
outputs), and dispatches through the mandatory privilege-drop wrap.
A spec whose `submitter` doesn't resolve to a uid/gid is failed
cleanly instead of falling through to a root run.

**Startup guard.** A daemon constructed with `multi_user=True` on a
host without `systemd-run` now **refuses to start** (raises in
`__init__`, before the queue lock is claimed) — there is no safe
privilege-drop without it.

**`ownership._caller_is_admin` fix.** The admin-group check compared
an integer uid against `grp.gr_mem` (a list of *name* strings) and
never matched — every non-root admin-group member was wrongly
denied. Now resolves the uid to a username and also accepts the
admin group as the caller's primary group.

**`vq admin update --all-hosts` exit-code fix.** `0952d47`'s
`--token` rework referenced `token` inside the per-host closure
before it was assigned (the assignment sat *after* the `--all-hosts`
block). For `--all-hosts` this raised `NameError` — not a
`ClickException` — so a failed host never landed in the `failures`
list and the command exited 0 despite the failure. Token resolution
moved ahead of the `--all-hosts` block.

**Hardened `contrib/vq-daemon-multi-user.service`.** The shipped
unit had `ExecStart=vq daemon run` (bare `vq` — systemd has no shell
PATH → 203/EXEC) and no `VQ_CONFIG_DIR`, so it could never have
started. Now: absolute `ExecStart` from a **root-owned** install
(`/opt/vq/venv/bin/vq`, not a user-writable checkout — that would be
a root-escalation path), `Environment=VQ_CONFIG_DIR=/etc/vq`, and an
in-file security note.

**New `docs/multi_user_deployment.md`** — full runbook: root-owned
install, `/etc/vq` config, the systemd unit, isolation verification,
rollback, and the **state-migration step** from a single-user
`~/.local/share/vq/` install to per-uid `/var/lib/vq/users/<uid>/`.

**Flock-guard catch-up.** `af02532` added the daemon queue-directory
`fcntl` lock (the two-daemon split-brain fix) but shipped no version
bump or roadmap entry; this release records it.

**Tests** (+19 in `test_multi_user_privdrop_v0_6_25.py`):
`TestWrapCommandPrivDrop` (6) — system-mode argv, mandatory wrap,
raise-not-root-run, caps still applied, optional gid, single-user
unchanged; `TestSystemdRunOnPath` (2); `TestDaemonMultiUserStartupGuard`
(3); `TestDispatchHelpers` (3) — `_gid_for_uid`, `_chown_tree`;
`TestCallerIsAdmin` (5) — root, supplementary member, primary-group
member, non-member, missing group. Plus the 3 `TestAdminUpdateAllHosts`
tests now pass.

1442 passed / 11 skipped on macOS (+19 from v0.6.24's 1423).

**Deployment note:** the fleet daemons stay **single-user** — this
release makes multi-user *safe to deploy*, it does not flip the
fleet. Switching a host to multi-user is a deliberate, runbook-
driven operator action (see `docs/multi_user_deployment.md`).

**Still not done (multi-user follow-ups):** automatic submit-side
directory bootstrap for a brand-new user's first submit; watchdog
cgroup accounting verified against system-mode (vs `--user`) job
scopes. Both noted in the deployment doc's "Known limitations".

---

### v0.6.24 — `idle_seconds` + `vq summary` alias (2026-05-19)

**The "is this host quiet right now?" question.** v0.6.21 added
`vq overview` with queue counts + recent-terminal counts + daemon
health, which covers most of the "what's going on across the
fleet" story. The gap that surfaced today: an operator looking at
overview sees "running 0, pending 0" and wants to know *how long*
the host has been quiet — useful for deciding "is it safe to
update vibeqc-release on compute-d now?" or "did this overnight
batch actually finish at 6am like I expected?".

**`HostOverview.idle_seconds: int | None`** populated in
`gather_overview_local`:

* If any job is currently `running` → `None` (host is busy; idle
  isn't meaningful).
* If no terminal job has a parseable `finished_at` → `None` (no
  history to measure against).
* Otherwise → `now - max(finished_at)` in integer seconds. The
  max walks the FULL terminal history, not just the `--since-hours`
  window, so a host that ran nothing in the last day but finished
  a job 36 h ago idle-reports as `~129600` (not "unknown").

**Text rendering** picks one of three states per host:

* `running:       2 job(s)` — host is busy, idle isn't relevant.
* `idle:          3m 12s`   — host is quiet, last finish was N ago.
* (line absent)             — quiet but no terminal history.

Duration formatter `_format_duration(seconds)` keeps the line
compact regardless of magnitude — emits the two highest-order
non-zero units (`d/h/m/s`), so "1d 1h", "2h 5m", "3m 12s", "45s".

**JSON output** gains `idle_seconds` (int or null). The remote-
forwarding path (`_overview_from_json`) tolerates pre-v0.6.24
remotes that omit the key (treats as None) and ignores negative
values from a misbehaving remote (clock skew, schema bug).

**`vq summary` Click alias** added via
`main.add_command(overview, name="summary")` — same command
behind both verb names, same flags, same JSON schema. Recognizes
the operator habit of reaching for "summary" when asking for a
fleet rundown.

**Tests** (+21 in `test_overview_idle_v0_6_24.py`):

* `TestFormatDuration` (5) — 0/negative clamp, seconds-only,
  minutes+seconds, hours+minutes, days+hours-compact (top-2
  units).
* `TestCountSpecsLastTerminalAt` (4) — no terminal returns None,
  None when finished_at missing, picks max across full history
  not just window, unparseable finished_at skipped.
* `TestGatherOverviewIdle` (3) — idle populated when quiet with
  history, None when running > 0, None when no terminal history.
* `TestFormatOverviewIdle` (5) — idle line when quiet, running
  line when busy, both omitted when quiet-no-history, JSON
  includes the key, None serializes as null.
* `TestOverviewJsonRoundtrip` (3) — idle_seconds round-trips
  faithfully, missing key tolerated (pre-v0.6.24), negative
  idle ignored.
* `TestSummaryAlias` (1) — both verb names produce identical
  help body (modulo the Usage line).

1386 passed / 10 skipped on macOS (+21 from v0.6.23's 1365).

**What was already in `vq overview` and didn't need rebuilding**:
running/pending/completed/failed counts (v0.6.21), 24h recent-
terminal window (v0.6.21 default), daemon health + memory
pressure (v0.6.21), env versions (v0.6.21), admin-update marker
(v0.6.21), drain + throttle state (v0.6.23). The five-line "what
shall be next is the docker vq summary command that gives an
overview of …" spec landed as a single-field additive ship plus
a verb alias because the heavy lifting was already on disk.

---

### v0.6.23 — five-item polish ship (2026-05-18)

**Picks up the small follow-ons that v0.6.20–v0.6.22 left
hanging.** All five items are tiny per-feature wins; together
they tighten the operator surface around the watchdog +
overview + cleanup verbs that landed in the previous three
releases.

**Item 1 — watchdog auto-pause tagged `paused_by="watchdog_host_pressure"`.**
The v0.6.20 host-pressure auto-pause now records *who* paused
the job, leveraging the v0.6.22 `paused_by` infrastructure. An
operator running `vq status JOBID` on a watchdog-paused job sees
`paused_by: watchdog_host_pressure` instead of an opaque
"paused" with no context. The watchdog's matching resume side
already keys off this tag implicitly (it only SIGCONTs jobs
*it* paused).

**Item 2 — `vq overview` surfaces drain + throttle state.**
`HostOverview` gains two new optional fields, `drain_state`
and `throttle_state`, populated locally by reading the
respective state files and forwarded over JSON when querying
remotes. The text format adds a `drain:` line and a `throttle:`
line when either is active; absent when both are at defaults.
Closes the gap where `vq overview` showed running/queued
counts but not *why* nothing was dispatching (drain active).

**Item 3 — `vq daemon health --json` exposes `drain_active` +
`drain_reason`.** `ContractVerdict` gets the two new fields,
populated in `verify_user_systemd_contract()` by reading drain
state. An INFO finding is added to the verdict's findings list
when drain is active. JSON consumers (monitoring scripts) can
key off `drain_active: true` to suppress "daemon idle"
warnings during a planned drain.

**Item 4 — web dashboard surfaces host-pressure banner.**
`/queue` now renders a yellow banner when host memory pressure
is ≥80% (matches the `vq daemon health` WARN threshold) OR
when the watchdog has currently auto-paused at least one job.
Inline-styled (no `base.html` edit needed). The banner shows
the current pressure %, the count of watchdog-paused jobs, and
what the watchdog will do (SIGCONT at the resume threshold).

**Item 5 — `vq cleanup --jobid X` for single-job
archive/delete.** The pre-v0.6.23 `vq cleanup` verb required
an `--older-than` cutoff, which prevented "I know this exact
jobid is done; clean it up now" workflows. The new
`--jobid JOBID` flag (repeatable) bypasses the cutoff and
targets explicit jobids. Mutex with `--older-than`. Rejected
with `--restore` (no restore-by-jobid use-case so far).
Unknown jobids surface inline `unknown jobid: <id>` errors;
the rest of the request continues.

**Tests** (+21 in `test_polish_v0_6_23.py`):

* `TestDaemonHealthDrainFields` (3) — verdict has fields,
  populated when drain set, JSON includes both keys.
* `TestOverviewDrainThrottle` (5) — None defaults, drain
  populated from drain.json, text format shows drain/throttle
  lines, JSON exposes both keys.
* `TestCleanupByJobid` (8) — `find_candidates_by_jobid`
  selects terminal specs, rejects running/unknown/already-
  archived, CLI mutex with `--older-than`, `--jobid+--restore`
  rejected, dry-run lists candidate, unknown jobid surfaces
  inline error.
* `TestWatchdogPausedByTag` (1) — `_host_pressure_pass`
  passes `paused_by="watchdog_host_pressure"` to `pause_job`.
* `TestWebHostPressureBanner` (3) — no banner at low
  pressure + no paused jobs, banner at ≥80%, banner counts
  watchdog-paused jobs.

1365 passed / 10 skipped on macOS (+21 from v0.6.22's 1344).

---

### v0.6.22 — `vq pause --paused-by TAG` / `vq resume --paused-by TAG` (2026-05-18)

**Closes the `scripts/update.sh` handover's edge case #5.** A
script can now pause the queue with a tag, do work, and resume
only what IT paused — operator-paused (untagged) jobs stay
paused. Pairs with the v0.6.20 host-pressure auto-pause and the
v0.6.0 admin-update pause/resume bracket, completing the
cooperative-pause model.

**New `JobSpec.paused_by: str | None`** — free-form actor tag,
same charset as `job_name` (alnum + `-` `_` `.`, ≤50). Set when
pause is invoked with `--paused-by TAG`; cleared on resume.
Additive schema; pre-v0.6.22 specs read clean (default None).

**First-pauser-wins semantics**: if a job is already SUSPENDED
when a second pause arrives, the new `paused_by` is NOT applied
— matches the existing idempotent-pause behavior. The implication
is that operator-paused jobs (paused_by=None) can't be "claimed"
by a later tagged pause; `resume --paused-by TAG` correctly
leaves them alone.

**CLI changes**:

* `vq pause [JOBID] --paused-by TAG` — records the tag on the
  spec.
* `vq pause --all --paused-by TAG` — same, applied to every
  newly-paused job.
* `vq resume --all --paused-by TAG` — scopes the resume to
  jobs whose `paused_by` matches.
* `vq resume JOBID --paused-by TAG` — strict: errors if the
  spec's `paused_by` doesn't match (catches script logic bugs).
* `vq status JOBID` shows `paused_by:` when set.

**Bulk-summary additions**: `vq resume --all --paused-by TAG`
now reports the filter-skipped count, e.g. `resumed 1 job [2
paused by other tag, left paused]`.

**Tests** (+11 in `test_pause_resume.py`):

* `TestPausedByTag` (4) — field set on pause / None default /
  cleared on resume / invalid charset rejected.
* `TestPauseAllWithTag` (3) — pause_all forwards tag / resume_all
  filter scopes to matches / resume_all no-filter resumes all.
* `TestPausedByCLI` (4) — CLI tag recorded / single-job filter
  mismatch errors / filter match resumes / invalid charset
  rejected at CLI.

1344 passed / 10 skipped on macOS (+11 from v0.6.21's 1333).

**Operator-facing impact for the scripts/update.sh handover**:
the "precise variant" recipe in
`docs/handover-update-script-cooperative-pause.md` is now the
one-liner:

```bash
trap _vq_resume_queue EXIT
"$_VQ_BIN" pause --all --paused-by update-script
# ... build ...
# trap: "$_VQ_BIN" resume --all --paused-by update-script
```

vs. the pre-v0.6.22 alternative (capture jobid list, loop per-
job, hope nothing races). Operator-paused jobs no longer get
accidentally resumed by the script's trap.

**Why first-pauser-wins not reference-counted**: a reference-
counting model (track every actor's pause as a separate vote;
only resume when the count drops to zero) would handle the case
where operator + script both pause the same job, both want to
resume independently. But the operational reality is simpler:
the script paused→build→resume cycle is a narrow window, the
overlap with manual operator pause is rare, and when it does
happen the operator can re-pause after the script's resume.
Reference-counting also requires schema for the set, more
complex state-machine, more edge cases. First-pauser-wins is a
30-line change with clear semantics.

### v0.6.21 — `vq overview` fleet summary verb (2026-05-18)

**User-requested ship.** One verb that answers "what's the state
of my fleet?" — version, daemon health, queue, recent activity,
env drift, admin marker — for every host in the config or just
one specifically.

#### CLI shape

```
vq overview                 # fleet — all hosts in config
vq overview HOST            # single host
vq overview --json          # machine-readable
vq overview --since-hours N # widen recent-terminal window (default 24)
```

#### Per-host content

* **Header**: `==== HOST (vq VERSION) ====`
* **Daemon**: verdict (OK / FAIL) + pid; **memory pressure** %
  if the host has /proc/meminfo
* **Queue**: counts by state — `running`, `pending`, `suspended`,
  `completed`, `failed`, … (only states with `count > 0` shown)
* **Recent (terminal, in window)**: terminal-state counts whose
  `finished_at` is within `--since-hours` of now
* **Envs**: `name  branch  describe  [DIRTY]` per `kind=venv`
  program — same data as `vq admin status` but compact
* **Admin update marker**: only when present; shows `state` /
  `envs` / `started_at` / failure reason

Unreachable hosts render `==== HOST ====\n  ERROR: <reason>` and
the sweep continues — one broken host can't block the rest.

#### Implementation

* New module `src/vq/overview.py` (~290 LoC):
  * `HostOverview` dataclass — structured per-host snapshot
  * `_count_specs(specs, recent_window, now)` — pure partition
    into current-state counts and recent-terminal counts
  * `gather_overview_local(host, cfg, *, recent_window)` —
    calls existing `list_jobs` / `lifecycle.verify_user_systemd_contract`
    / `admin.query_env_status` / `admin.read_admin_update_marker`
  * `gather_overview_remote(host, host_cfg, *, recent_window)` —
    one SSH call to `<remote_vq> overview localhost --json
    --since-hours N`; remote does the gather, emits one JSON
    blob; local rebuilds the HostOverview from it. Transport
    failures → `reachable=False` + error string (never raise).
  * `_overview_from_json` — defensive rebuild (drops unknown
    fields; falls back to None on schema drift so a v0.6.21
    laptop reading a future v0.7's overview output doesn't crash)
  * `format_overview_text` / `format_overview_json` —
    single-host renderers
  * `format_fleet_overview_text` / `format_fleet_overview_json`
    — stack per-host

* New CLI `vq overview [HOST] [--json] [--since-hours N]` in
  `src/vq/cli.py` (~80 LoC). Single-host invocation gathers
  the named host; no-arg invocation walks every host in the
  config.

#### Prerequisites shipped in the same ship

* **`vq queue --json`** flag — emits a JSON array of JobSpec
  records (same filters apply: `-s`, `--active`,
  `--show-archived`, `--tag`). Used by overview's local gather
  for queue counts, but also useful standalone for any
  monitoring tooling. Forwarded over SSH.
* **`lifecycle.ContractVerdict.memory_pressure_pct`** field —
  snapshotted via `read_host_memory_pressure_pct()` during
  `verify_user_systemd_contract`. Surfaces in both `vq daemon
  health` text output (with an explicit WARN when ≥80% since
  the v0.6.20 watchdog pauses at 85%) and the JSON output.
  Saves a second SSH call from monitoring tools that wanted
  both `vq daemon health` and the pressure number.

#### Mixed-version fleet behavior

If the laptop is v0.6.21 but a remote host is older (e.g.
v0.6.20), `vq overview REMOTE` errors with the clean
`No such command 'overview'` message inline as the host's
error — the rest of the fleet still renders. After deploying
v0.6.21 to every host, the verb works end-to-end.

#### Tests (+19)

* `TestCountSpecs` (4) — current-state partition / recent-window
  filter / no-finished_at-skipped / unparseable-finished_at-skipped.
* `TestGatherOverviewLocal` (2) — minimal smoke / queue spec
  counting.
* `TestFormatOverviewText` (3) — unreachable host one-liner /
  reachable host sections / memory pressure surfaced.
* `TestFormatOverviewJson` (2) — schema completeness / fleet
  wrapper.
* `TestOverviewCLI` (3) — text mode / JSON mode / help mentions
  capabilities.
* `TestQueueJsonFlag` (3) — array shape / state filter /
  empty case.
* `TestDaemonHealthMemoryPressure` (2) — verdict field present /
  JSON exposes it.

1333 passed / 10 skipped on macOS (+19 from v0.6.20's 1314).

#### Operator example

```
$ vq overview
==== compute-d (vq 0.6.21) ====
  daemon:        OK, daemon pid=12308
  memory pressure: 2.4%
  queue:
    running               1
    pending               0
  recent (terminal, in window):
    completed             47
    failed                2
  envs:
    vibeqc-dev         main      v0.7.5-246-g85120a9
    vibeqc-queue       main      v0.7.5-302-gd1d511d
    vibeqc-release     release   v0.8.1

==== compute-a (vq 0.6.21) ====
  daemon:        OK, daemon pid=48338
  memory pressure: 19.8%
  queue:
    running               1
  recent (terminal, in window):
    completed             3
  envs:
    vibeqc-dev         main      v0.7.5-292-g9cc518d
    vibeqc-queue       main      v0.7.5-302-gd1d511d
    vibeqc-release     release   v0.8.1
```

### v0.6.20 — host-pressure auto-pause in the watchdog (2026-05-18)

**Crash-driven ship.** Forensics on the 2026-05-18 07:50 EDT
compute-d wedge revealed an OOM cascade that took out vq-daemon
itself (Steam + a vq job + Nextcloud + GNOME shell collectively
crossed the 125 GB cliff). Per-job cgroup MemoryMax worked as
designed but didn't watch the AGGREGATE pressure. This ship adds
the missing layer.

**New behaviour**: every daemon iterate() tick, before the
per-job watchdog pass, the daemon checks global memory pressure
via `/proc/meminfo`:

```
pressure = 100 * (MemTotal - MemAvailable) / MemTotal
```

If `pressure >= host_pressure_pause_pct` (default 85%) AND there
are running jobs, the daemon SIGSTOPs every running job via
`pause_job(jid)` and records the jobid set. The watchdog enters
`_host_pressure_active=True`.

When pressure drops below `host_pressure_resume_pct` (default
70%, hysteresis margin of 15%), the daemon SIGCONTs exactly the
jobids it recorded (not operator-paused jobs, not jobs newly
dispatched while paused). The watchdog clears the active flag.

**Why pause not kill**: a long-running calculation shouldn't be
murdered because Steam started up. Pausing freezes the job's RAM
at the current footprint (RAM stays allocated, doesn't grow); the
kernel preferentially OOM-kills cgroups that are still
allocating, so frozen jobs become low-priority OOM-kill targets.
When pressure drops, SIGCONT picks them back up. Solvers don't
need to tolerate kernel SIGKILL; they only need to tolerate
SIGSTOP/SIGCONT, which the v0.5.1 `vq pause` machinery has
required from every supported solver for ~12 months.

**Why hysteresis (85 / 70 not 85 / 85)**: a single threshold
would flap every iterate tick if pressure hovered near 85% —
pause, immediately resume (because pressure dropped a tiny bit
when we paused), then pause again on the next tick. The 15%
gap means once paused, pressure has to drop substantially
before resume — typically meaning the competing workload
(Steam, build, etc.) has actually finished or freed memory.

**Why thresholds default to 85 / 70**: typical Linux box runs at
40-60% memory usage from page cache + small processes; 85% is
the operator-visible "things are getting hot" mark with enough
headroom to react before the kernel OOM-killer wakes up at ~95%.
70% is conservative for resume — leaves clear margin before
re-entering the danger zone.

**Disable / tune**: configurable per Watchdog construction:

```python
Watchdog(
    host_pressure_pause_pct=85.0,    # raise for noisier hosts
    host_pressure_resume_pct=70.0,
    enforce_host_pressure_pause=False,  # bypass entirely
)
```

The daemon CLI doesn't expose these yet — operators who want
tighter / looser thresholds will need a future
`vq daemon run --host-pressure-pause-pct N` flag. Default
behavior is the right call for now; tighten as needed.

**No-/proc-/no-MemAvailable handling**: hosts without
`/proc/meminfo` (macOS dev box, BSD, certain container setups)
produce `None` from the pressure reader; the pass becomes a
clean no-op. The fleet's actual production hosts (compute-d +
compute-a, both Linux) get the protection.

**New API**:

* `vq.watchdog.read_host_memory_pressure_pct() -> float | None`
  — pure /proc/meminfo parser, returns the
  `(MemTotal - MemAvailable) / MemTotal` percentage. None on
  any failure.
* `vq.watchdog.HostPressureAction` enum — NO_OP / PAUSE / RESUME.
* `vq.watchdog.HostPressureVerdict` dataclass — `action`,
  `jobids`, `pressure_pct`, `reason`.
* `Watchdog.check_host_pressure(running_jobids) -> HostPressureVerdict`
  — hysteresis state machine. Pure decision; daemon does the
  pause/resume side effects.

**Daemon integration** (`Daemon._host_pressure_pass`):

* Runs once per `iterate()` tick, BEFORE `_watchdog_pass`, so
  a pause-decision is reflected in `spec.state` (= SUSPENDED)
  by the time the per-job watchdog runs and correctly skips
  kill paths for suspended jobs.
* All side effects wrapped in `try/except` — a partial pressure
  intervention must not crash the dispatch loop. Errors log,
  the next tick retries.

**Tests (+13 in `tests/test_watchdog.py`)**:

* `TestReadHostMemoryPressurePct` (4) — parser correct on
  well-formed meminfo, returns None on missing file / missing
  MemAvailable field / garbled values.
* `TestCheckHostPressure` (9) — low pressure → NO_OP / high
  pressure + running jobs → PAUSE / high pressure no jobs →
  NO_OP (avoid stranded-active state) / already active stays
  paused / drop below resume → RESUME / hysteresis prevents
  flapping in the mid-range / None pressure → NO_OP /
  `enforce_host_pressure_pause=False` → NO_OP / RESUME only
  targets jobs WE paused (not newly-dispatched ones).

1314 passed / 10 skipped on macOS (+13 from v0.6.19's 1301).

**Companion: handover for the scripts/update.sh chat**:
`docs/handover-update-script-cooperative-pause.md` documents the
complementary script-side fix (call `vq pause --all` before the
cmake build, `trap _vq_resume_queue EXIT`). That chat owns the
vibe-qc side; this ship owns the vq side. Both layers stack:
the script-side pause covers "interactive update script kills a
running job"; the watchdog auto-pause covers the broader
"aggregate desktop + vq + sync load crosses the cliff" case
that fired on 2026-05-18.

### v0.6.19 — docs deep-audit per CLAUDE.md § 5 (2026-05-18)

**Pure docs ship.** Per-quarter cadence audit (CLAUDE.md § 5)
across every Markdown file under `docs/`. Audit agent
identified ~15 findings; this ship fixes the CRITICAL + HIGH
ones (concentrated in `handover.md` and `chat-onboarding.md`).

#### chat-onboarding.md

The file is meant to bring a fresh chat up to speed. The
audit found it was anchored at vq 0.5.40 (~30 releases stale)
and missing every v0.6.x verb a current chat would want to
reach for. Updates:

* Version anchor `0.5.40` → `0.6.18`.
* Added `--at ISO8601` and `--wait` rows to the
  "Useful submit flags" table.
* Added `vq wait` + `vq status --json` to the "Watching jobs"
  section.
* New top-level sections:
  - "Rerunning + recovery (v0.6.8 / v0.6.10)" — `vq resubmit
    JOBID`, `vq resubmit --state aborted_by_queue` for
    post-reboot, compose with `vq wait`.
  - "SLURM-style verb aliases (v0.6.15)" — `vq sbatch` /
    `squeue` / `scancel` / `sacct` mapping table.
  - "Latest-tag auto-update for vibeqc-release (v0.6.11)" —
    `--dry-run` + apply recipes.
  - "Client-side log file (v0.6.16)" — `client.log` location,
    `VQ_LOG_LEVEL` / `VQ_LOG_DISABLED` env overrides.

#### handover.md

Concentrated stale-claim cluster the audit flagged:

* Smoke-test polling recipe (line ~209) said "no notifications,
  no email, no Slack hook in v0.2." Replaced with the
  `vq wait` / `vq submit --wait` (v0.6.14) options plus
  webhook notifications (v0.5.35) for terminal-state push.
* "Watching jobs" prelude (line ~519) said "no `wait`-style
  blocking call. The user is responsible for polling.
  Notification channels are on the v0.7 roadmap." Replaced
  with the actual current shape: webhook notifications +
  `vq wait` synchronous mode; polling stays a third option.
* "Limitations (v0.5.25)" block (line ~831) listed four
  limitations of which three have shipped (`--all` v0.5.28,
  self-update v0.5.42, marker file v0.5.44 + v0.6.0 state
  machine). Rewrote as "Current limitations" + "Previously-
  listed limitations that have shipped" with the canonical
  cross-references.
* "Still on the v0.6.x roadmap" block (line ~987) listed the
  same three shipped items. Rewrote as "Still on the v0.7+
  roadmap" pointing only at the multi-user item that remains.
* One-time-setup verification (line ~323) said
  `vq --version should print vq, version 0.3.0`. Bumped to
  `0.6.18`.
* Two references to vibeqc-release `v0.7.3 as of 2026-05-10`
  refreshed to vibe-qc `v0.8.0` as of 2026-05-18 (vibe-qc has
  since tagged v0.8.0).

#### web.md

Two "v0.5.2 will ship" promises (TLS reverse-proxy recipe; HTTP
submit / status / wait API) were ~17 releases stale. Neither
shipped at v0.5.2; both remain unshipped at v0.6.18. Reframed
as "still not shipped" with current context: the HTTP submit/
wait/status API is largely subsumed by the v0.6.14 CLI verbs
(`vq status --json`, `vq wait --timeout`, `vq submit --wait`)
that handle the same shell-script needs via SSH, so the HTTP
side stays parked.

#### Findings deliberately punted

The audit flagged ~5 MEDIUM/LOW items not addressed this ship:

* `handover.md` § "What's NEW in v0.5.X" sediment (~440
  lines of release history that duplicates roadmap.md). A
  future ship could collapse this into a one-line-per-
  version table or delete entirely.
* `SPEC.md` + `roadmap.md` reference `docs/install.md`
  which doesn't exist. Content actually lives in
  `lifecycle.md` + `handover.md` § "One-time setup".
* Cross-link gap between `handover.md` and the
  `operations.md` / `lifecycle.md` / `wall_time_design.md`
  trio. handover.md still self-contains recovery recipes
  that have canonical homes elsewhere.

Marked `Patch-candidate: v0.7.x` so the release chat sees the
deferred items.

Patch-candidate: v0.7.x

No code change, no test additions. 1301 passed / 10 skipped
(unchanged from v0.6.18).

### v0.6.18 — test coverage audit + cgroup scope-helper fill (2026-05-18)

**Coverage-audit follow-on.** Ran `python -m coverage` against
the suite, found that `src/vq/cgroup.py` was at 67% (49 missed
lines) — by far the largest gap among the non-CLI modules. The
gap was concentrated in three helpers added in v0.5.50/v0.5.51:
`scope_main_pid`, `scope_exists`, `stop_scope`. These functions
are exercised integration-style during daemon startup-recovery
but had no direct unit tests, so a macOS dev-box run (where
systemctl doesn't exist) skipped them entirely.

Coverage baseline (before / after this ship):

| module | before | after |
|---|---|---|
| `cgroup.py` | 67% | **97%** |
| overall | 85% | **86%** |

**Tests added** (+19, all in `tests/test_cgroup.py`):

* `TestScopeMainPid` (8): success returns int / .scope suffix
  auto-appended / no systemctl / systemctl nonzero / PID=0 /
  unparseable PID / empty stdout / OSError → None.
* `TestScopeExists` (6): loaded → True / not-found → False /
  empty state → None / no systemctl → None / nonzero rc →
  None (uncertain, not False) / TimeoutExpired → None.
* `TestStopScope` (5): success → True / failure → False /
  no systemctl → False / OSError → False / .scope suffix
  auto-appended.

All three helpers have the same shape (probe systemctl, parse
output, return on rc / stdout) — the tests mock `_systemctl_path`
+ `subprocess.run` and cover every branch of the return logic.

**Other modules in the coverage report**:

* `cli.py` at 72% (362 missed lines) — biggest absolute gap,
  but mostly error-handling branches for unusual config /
  remote-failure cases. Not a useful coverage target: the
  branches are already exercised via the integration tests +
  smoke tests on the fleet. Drilling further would chase
  metric rather than value.
* `pause_resume.py` at 76% — gap is concentrated in the
  whole-queue + provides_branches pause paths' error-recovery
  branches. Each one has a clear "user-visible failure mode" —
  worth filling but not in this ship.
* `daemon.py` at 83% (87 missed) — large module; the gap is
  spread across many branches with no concentrated cluster.
* `lifecycle.py` at 82% — similar shape.

Punted to future ships:

* **pause_resume.py coverage fill** (Patch-candidate: v0.7.x) —
  ~30 LoC of tests would push it from 76% → 90%+. The error-
  recovery branches are exactly what we'd want to assert on
  when a pause/resume cycle goes wrong mid-flight; worth a
  follow-up ship.
* **CLI coverage** — deliberately NOT targeted. The Click
  framework + integration tests already exercise the common
  paths; the gap is mostly defensive error rendering that's
  easier to verify by reading than by patching pytest fixtures
  for every config edge case.

1301 passed / 10 skipped on macOS (+19 from v0.6.17's 1282).

### v0.6.17 — hardening audit on watchdog / cgroup / transport (2026-05-18)

**Audit-driven follow-on.** Deep-read of `watchdog.py`, `cgroup.py`,
`transport.py` produced three actionable findings. Each
addressed with code change + test; lower-severity findings
documented as known trade-offs.

#### Fix #1 (HIGH) — transport: missing subprocess.run timeout

The audit's most production-relevant finding. Pre-v0.6.17,
`run_remote_vq` / `run_remote_shell` / `upload_file` called
`subprocess.run` without a `timeout=` argument. A momentary
network glitch or a half-broken remote sshd (NFS-stuck
homedir, fork-bombed remote) could hang the daemon main loop
for the OS TCP timeout (minutes) or indefinitely. Unlike the
`cgroup.py` helpers which pass `timeout=5/10`, transport had
no protection.

Fix:

* Every `subprocess.run` now takes `timeout=` (default 600s
  for `run_remote_vq`, 30s for `run_remote_shell`, 1200s for
  `upload_file`).
* `subprocess.TimeoutExpired` translates to `RemoteError` so
  callers don't need a second except-clause for the timeout case.
* `_ssh_base()` adds explicit SSH options:
  `ConnectTimeout=10`, `ServerAliveInterval=30`,
  `ServerAliveCountMax=3`. Catches dead routes / wrong-port
  misconfigs quickly + detects half-broken sshds without
  waiting for the OS TCP keepalive (~2 hours on Linux).
* `_scp_base()` mirrors the same options.

#### Fix #2 (MEDIUM) — watchdog: SIGKILL re-fire spam for stuck processes

Pre-v0.6.17, once SIGTERM grace expired the "already-escalated"
branch returned `WatchdogAction.SIGKILL` on every subsequent
`evaluate()` tick. Harmless against a dying process
(`killpg` is idempotent) but for a truly stuck process
(D-state on broken NFS, kernel hang), the daemon would emit
"SIGKILL emitted" log lines forever with elapsed grace just
growing.

Fix: new `WatchdogJobState.sigkill_emitted` boolean. Flipped
True on the SIGKILL emission; subsequent evaluations return
`OK`. The daemon's `_reap_finished_jobs` loop owns the actual
termination detection via `popen.poll()` from here.

#### Fix #3 (MEDIUM) — watchdog: `host_total_mem_mb=0` would OOM every job

Pre-v0.6.17, `if self.host_total_mem_mb is not None:` was the
only guard. A misconfigured `/proc/meminfo` parser, a future
psutil-free fallback returning 0 on parse failure, or a CLI
passing `--host-total-mem-mb 0` would set `host_cap=0` and
OOM-kill any job with `rss > 0` — with the confusing operator-
facing reason "host cap 90% of 0 MB (0 MB)".

Fix: tighten the guard to `is not None and > 0`.

#### Documented trade-offs (low severity, not patched)

* **watchdog `samples.jsonl` is not fsync'd** — POSIX-guarantees
  `O_APPEND` atomic-up-to-PIPE_BUF (~4 KiB); typical sample JSON
  is well under that. A daemon SIGKILL or OS crash mid-write
  could leave a truncated line, but the test-suite for any
  future jsonl reader will need to tolerate that anyway. Adding
  `fsync()` per 5-second sample across every running job is
  non-trivial steady-state I/O overhead. Trade-off accepted.

* **`cgroup_path_for_pid` doesn't validate cgroup namespace
  remapping** — assumes "no namespace shenanigans," which holds
  for the current fleet (not running vq in containers). If a
  future deployment runs vq itself inside a container with a
  different cgroup namespace than the host's `/sys/fs/cgroup`,
  the constructed path could point at an unrelated cgroup. Out
  of scope for now; documented as a deployment caveat.

* **watchdog auto-register-via-evaluate uses fresh
  `monotonic()`** — if the daemon crashes mid-job and restarts,
  `evaluate()` will auto-register the job (since it's missing
  from `_states`) with a fresh `started_monotonic`. The wall-
  time clock effectively resets. The audit flagged this as
  latent — the daemon explicitly calls `register()` during
  startup-recovery for all RUNNING specs (see
  `_reattach_or_interrupt_at_startup`), so the auto-register
  fallback in `evaluate` should never trigger in practice.
  Marked as `Patch-candidate: v0.7.x` so the release chat
  considers asserting on the missing-state case instead.

#### Tests (+10)

* `tests/test_transport.py::TestSshTimeoutOptions` (3) —
  ConnectTimeout + ServerAlive* options present on every
  transport entry point.
* `tests/test_transport.py::TestSubprocessTimeoutTranslation`
  (4) — `subprocess.TimeoutExpired` raised by each entry
  point translates to `RemoteError`; the `timeout=` kwarg
  flows through to `subprocess.run`.
* `tests/test_watchdog.py::TestKillEscalation::test_sigkill_emitted_only_once`
  — after the first SIGKILL emission, every subsequent
  `evaluate` returns OK.
* `tests/test_watchdog.py::TestKillEscalation::test_within_grace_then_sigkill_marks_emitted`
  — sigkill_emitted only flips True at the actual SIGKILL
  emission, not during grace.
* `tests/test_watchdog.py::TestRSSCheck::test_host_total_zero_does_not_trigger_oom`
  — the v0.6.17 guard explicitly verified against 999 MB rss
  with `host_total_mem_mb=0`.

#### Test-file adjustments

The v0.6.17 SSH-options change inserted `-o KEY=VAL` pairs
between `ssh` and the host in `_ssh_base`. Existing tests pinned
`cmd[:2] == ["ssh", host]` (host at position 1) — now host is
at `cmd[-2]` (penultimate, right before the joined remote-cmd
string). Updated:

* `tests/test_transport.py` — `cmd[:2]` → `cmd[0]==ssh` +
  `cmd[-2]==host`; `cmd[2]` → `cmd[-1]`.
* `tests/test_cli.py::TestRemoteDispatch::_remote_argv` helper
  — same shape change.
* `tests/test_cli.py` host-extraction in `fake_run` callbacks
  — `cmd[1]` → `cmd[-2]`, `cmd[2]` → `cmd[-1]`.

This is the kind of test-fragility lesson worth remembering:
pinning specific argv positions makes future invariant changes
require touching tests too. Future tests should match by shape
("first arg is ssh, last arg is the joined cmd") rather than
by position when the SSH argv shape may evolve.

1282 passed / 10 skipped on macOS (+10 from v0.6.16's 1272).

### v0.6.16 — client-side log file at `<state_root>/client.log` (2026-05-18)

**First item from the post-v0.6.15 audit-focus phase.** The
daemon already had `daemon.log` (via `setup_daemon_logging`);
the CLI side had `logging.getLogger()` calls scattered through
the codebase but no handler installed, so they silently
dropped. v0.6.16 wires up the client side.

**New behavior**: every `vq` CLI invocation appends to
`<state_root>/client.log`. One INFO line per invocation
captures the argv + vq version; sub-operations log naturally
via the existing scattered `log.debug/info/warning/error`
calls.

**Rotating storage**: `RotatingFileHandler` with 10 MB per
file × 3 backups → 40 MB worst-case disk footprint. Plenty
of history for forensic debug; bounded so a host with high
`vq` traffic doesn't fill its disk.

**Format**:

```
2026-05-18T15:42:09-0400 [pid=42654] INFO vq.cli: cli invocation: argv=[…] vq_version=0.6.16
```

The `[pid=N]` field is the discriminator for concurrent
invocations from parallel shell pipelines — without it, two
simultaneous `vq submit` calls would interleave indistinguishably.

**Configuration**:

* `VQ_LOG_LEVEL=DEBUG` — override level (case-insensitive;
  invalid values default to INFO).
* `VQ_LOG_DISABLED=1` — skip log-file setup entirely. Useful
  for test fixtures (the test suite installs this in its
  own state_dir fixtures where the log would just pollute
  tmp dirs). Also the emergency escape hatch if the log file
  write itself becomes a failure mode (read-only filesystem).

**Best-effort, never fatal**: if the log file open fails
(unwriteable parent dir, permission denied, fs full),
`setup_cli_logging` returns None and the CLI keeps running
normally. A CLI invocation must NOT die because the log is
unwriteable.

**Plumbing**:

* `src/vq/log.py` extended with `setup_cli_logging(log_file)`,
  `ENV_LOG_LEVEL` / `ENV_LOG_DISABLED` constants, format /
  rotation constants. Existing `setup_daemon_logging` unchanged.
* `src/vq/cli.py` main() callback (now `@click.pass_context`)
  installs the handler + emits the invocation line. Skipped
  for the hidden `tar-workspace` verb out of an abundance of
  caution (that verb streams binary to stdout).
* Idempotent: re-calling `setup_cli_logging` removes the
  prior handler before installing a new one — repeated CLI
  invocations in the same Python interpreter (test runs,
  web import path) don't pile up handlers.

**Tests** (+22 in `test_log.py`):

* `TestLevelResolution` (10, parametrized): default INFO,
  valid name parsing (DEBUG/info/Info/WARNING/ERROR/CRITICAL),
  invalid values fall back (`""`, `garbage`, `INFO `, `VERBOSE`,
  `42`).
* `TestSetupCliLogging` (8): file creation, env-disabled
  skip, env-not-`1` does NOT skip, idempotency (no duplicate
  handlers), level gating, DEBUG mode shows debug lines,
  unwriteable path returns None, pid appears in format.
* `TestCLIInvocationLine` (2): `--version` exits early (no
  invocation log), subcommand `--help` does log through
  group body.

1272 passed / 10 skipped on macOS (+22 from v0.6.15's 1250).

**Doc updates**: operations.md gets a "Client-side log file"
section right above the `vq daemon health` block.

### v0.6.15 — SLURM-style verb aliases (2026-05-18)

**Seventh post-audit feature ship.** Closes the last
long-term-idea on the roadmap: SLURM-compatible CLI subset.

**Four aliases**, each registered via `main.add_command(canonical,
name=alias)` so the alias IS the same Click command object — no
duplicate definitions to drift over time:

| SLURM-style alias | Canonical vq verb |
|---|---|
| `vq sbatch`  | `vq submit`  |
| `vq squeue`  | `vq queue`   |
| `vq scancel` | `vq kill`    |
| `vq sacct`   | `vq status`  |

The alias only renames the verb — **flag names stay vq-style**.
`vq sbatch --cpus 4 --mem-mb 1024` works; `vq sbatch --cpus-per-task=4
--mem=1G` does NOT. Reason: SLURM-flag translation has too much
surface area for a sugar layer, and partial translation would
mislead operators about which SLURM semantics actually work. Help
text on each alias surfaces the canonical's flag set; an operator
typing `vq sbatch --help` sees the full `vq submit` flag list.

**Why this scope** (verb-only, not full flag translation):

* Same Click command instance under both names → no risk of
  feature drift between alias + canonical.
* Operators with SLURM muscle memory get to keep typing `sbatch`
  / `squeue` / `scancel` / `sacct`.
* `--help` on the alias shows what flags actually work — no
  silent acceptance of unsupported SLURM flags.
* Cheap to implement, easy to reason about, easy to remove if
  the project decides aliases were the wrong call.

**Tests** (+14 in `test_slurm_aliases.py`):

* `TestAliasRegistration` (4, parametrized): each alias is
  registered + points at the same Click command object as its
  canonical.
* `TestSbatchAlias` (3): submits a single-file job / help text
  matches submit's body / vq flags reach through alias.
* `TestSqueueAlias` (2): lists jobs / `--state` filter reaches
  through.
* `TestScancelAlias` (2): kills a pending job / unknown jobid
  errors.
* `TestSacctAlias` (2): shows status / `--json` flag works
  through alias.
* `TestAliasVisibility` (1): all four aliases listed in top-
  level `vq --help`.

1250 passed / 10 skipped on macOS (+14 from v0.6.14 + minor
test-count drift from earlier fixtures).

**Doc updates**: handover.md TL;DR mentions the aliases.

### v0.6.14 — `vq wait JOBID` + `vq submit --wait` synchronous mode (2026-05-18)

**Sixth post-audit feature ship.** Closes the "resource
reservation" long-term-idea but with the more-useful "wait for
completion" semantics rather than "wait for dispatch only" —
shell scripts almost always want the former.

**New standalone verb** `vq wait [HOST] JOBID`:

* Polls until the job reaches a terminal state. Local jobs:
  re-read the spec each tick (the daemon writes atomically via
  tempfile-then-rename, so partial-read races are impossible).
  Remote jobs: shell out to `vq status HOST JOBID --json`
  (also new in this ship).
* Exit code maps to the job's outcome:
  - `0` — COMPLETED (the spec's `exit_code`, normally 0)
  - non-zero — any other terminal state (FAILED, KILLED,
    OOM_KILLED, STARVED, TIME_EXCEEDED, ABORTED_BY_QUEUE,
    INTERRUPTED). FAILED propagates the spec's actual
    `exit_code` when known; else 1.
  - `124` — `--timeout` elapsed (matches GNU coreutils
    `timeout(1)`). Job keeps running; only the wait is
    canceled.
  - `130` — SIGINT (Ctrl-C). Same: job keeps running.
* Wait loop is laptop-side. Transient SSH/network blips on
  remote polling are caught + retried per `--poll-interval`,
  so a 5s blip doesn't fail a multi-hour wait. Persistent
  errors (auth failure, host unreachable) retry until the
  `--timeout` fires.
* Default `--poll-interval` is 5s (matches watchdog cadence).
  Default `--timeout` is unlimited.

**New flag on submit**: `vq submit ... --wait` is sugar:
submit normally, then wait for terminal, exit with the job's
exit code. Composable with every existing submit shape
(single-file, `-d`, `-c`, `--at`, `--tag`, ...).

**New `vq status --json`** (~30 LoC, supporting infrastructure
for `vq wait` over SSH but useful for any automation):
emits the JobSpec as a JSON object plus `stdout` / `stderr`
tailed strings. Same `last_status_at` side-effect as the text
path. Forwarded over SSH for remote hosts.

**Shell-script use cases this unlocks**:

```bash
# Wait for a job, fetch when done, fail the script on non-zero
JID=$(vq submit input.py)
vq wait "$JID" && vq fetch "$JID" -o ./out

# One-liner with submit --wait
vq submit input.py --wait && vq fetch "$JID" -o ./out
#                       ^^ exits with the job's actual exit code

# Bounded wait
vq wait "$JID" --timeout 7200 || echo "took longer than 2h"
```

**Plumbing**:

* New module `src/vq/wait.py` (~190 LoC):
  - `WaitResult` dataclass with `cli_exit_code` property
  - `WaitTimeout` exception carrying jobid + last-seen state
  - `wait_for_terminal_local` (spec re-read per tick)
  - `wait_for_terminal_remote` (transport.run_remote_vq +
    JSON parse; transient-error retry)
  - `wait_for_terminal` dispatcher (local-or-remote)
  - All polling functions accept injectable `_now` / `_sleep`
    for fast deterministic tests
* `show_status_json` added to `src/vq/status.py` — emits the
  full JobSpec + tailed output as JSON.
* CLI verb `vq wait` and submit's `--wait` flag both use the
  shared `wait_for_terminal` dispatcher.

**Tests** (+28 in `test_wait.py`):

* `TestWaitResult` (11) — exit-code mapping for each terminal
  state (parametrized over the watchdog/operator/queue
  variants).
* `TestWaitForTerminalLocal` (5) — already-terminal short-
  circuit / poll-until-flip / RUNNING-not-terminal / timeout
  raises / missing spec.
* `TestWaitForTerminalRemote` (5) — immediate terminal /
  poll-through-running / transient-error retry / invalid-JSON
  retry / timeout under persistent error.
* `TestWaitCLI` (5) — COMPLETED exits 0 / FAILED propagates
  exit code / timeout exits 124 / missing jobid UsageError /
  help mentions exit codes.
* `TestSubmitWaitCLI` (2) — flag plumbing in help.

1215 passed / 10 skipped on macOS (+28 from v0.6.13's 1187).

**Why laptop-side wait loop**: pushing it to the remote would
require either (a) a long-lived SSH session per wait (limited
by the laptop's `MaxSessions`), or (b) a daemon-side "register
this callback" protocol. Both are heavier than just polling.
Laptop-side polling also keeps Ctrl-C semantics clean: the
operator's SIGINT cancels the wait without touching the job.

### v0.6.13 — `vq status` label polish: scheduled-submit vs retry-backoff (2026-05-18)

**Polish on v0.6.12.** End-to-end smoke test on compute-a caught it:
the `not_before` line in `vq status` was hardcoded to label the
field as `(retry backoff)`, which is wrong for v0.6.12's
scheduled-submit jobs (where retry_count is 0 and the field came
from `--at`, not from a daemon-driven backoff).

Fix: disambiguate by `spec.retry_count` (already part of v0.5.31's
retry-tracking).

* `retry_count > 0` → label as `(retry backoff)` (legacy behavior
  for actual retry-after-FAILED dispatches).
* `retry_count == 0` → label as `(scheduled submit)` (the v0.6.12
  --at case, where the field was set at submit time without any
  retry attempt yet).

A scheduled-submit job that subsequently fails + retries will
flip from "scheduled submit" → "retry backoff" the moment
retry_count increments — same wire field, different operator-facing
label per phase.

**Test** (+1): `test_cli.TestScheduledSubmitCLI.test_status_labels_scheduled_submit_not_retry_backoff`
asserts a --at submission's status output contains "scheduled
submit" and not "retry backoff".

1187 passed / 10 skipped on macOS (+1 from v0.6.12).

### v0.6.12 — `vq submit --at ISO8601` scheduled submits (2026-05-18)

**Fifth post-audit feature ship.** Closes the "scheduled submits"
long-term-idea item. The daemon's dispatch loop already gates
PENDING specs on `spec.not_before` (v0.5.31's retry-backoff path
uses the same field); v0.6.12 exposes that field at submit time.

**New flag**: `vq submit --at "ISO8601"`. Format requires an
explicit timezone — `2026-05-20T22:00:00Z` (UTC) or
`2026-05-20T17:00:00-05:00` (offset). **Naive timestamps are
rejected at the CLI boundary** to dodge the laptop-vs-server
timezone-confusion footgun. The parsed timestamp is re-emitted
in canonical ISO 8601 form (`+00:00` for UTC; explicit offsets
preserved) so the daemon sees a consistent representation
regardless of input variant.

Past timestamps are **accepted** — the dispatch loop already
treats an elapsed `not_before` as ready-now (matches the v0.5.31
retry-backoff semantics where a stale backoff is harmless). This
keeps the verb idempotent under clock skew.

Composes cleanly with `--retry` and `--priority`:

* `--retry`: if the job fails after dispatch, the retry-backoff
  path overwrites `not_before` per its own schedule. No
  interaction needed in the v0.6.12 code.
* `--priority`: the dispatch loop's sort is
  `(-priority, submitted_at)`; the not-before-ready filter runs
  before the sort. A scheduled high-priority job still jumps the
  queue once its time arrives.

**Plumbing**:

* `submit_local` accepts a new `not_before: str | None` kwarg
  that lands on the JobSpec.
* `submit_remote` forwards it as `--at <ISO8601>` over SSH; the
  remote vq's `--at` flag re-parses (so a version mismatch where
  the remote can't read our format fails fast at the remote
  rather than later in the dispatch loop with the "treated as
  ready now" fallback).
* CLI parses + validates at the boundary; spec/daemon code
  unchanged.

**Tests** (+12 across `test_submit.py`, `test_cli.py`,
`test_submit_remote.py`):

* `TestNotBeforeCapture` (3) — kwarg lands on spec / default
  None / past timestamps accepted at the API.
* `TestScheduledSubmitCLI` (7) — UTC `Z` accepted / explicit
  offset accepted / naive timestamps rejected / malformed ISO
  rejected / no flag leaves None / past timestamps accepted
  at the CLI / help text mentions the flag.
* `TestSingleFileRemote` (2) — not_before forwarded as `--at`
  flag / omitted flag when not_before is None.

1186 passed / 10 skipped on macOS (+12 from v0.6.11).

**Why naive-timestamp rejection is non-negotiable**: the
operator on a laptop in `America/New_York` typing
`2026-05-20T22:00:00` and shipping it to a daemon in `UTC` would
otherwise get a job that fires four hours off. The CLI's
strict-tz requirement makes that misuse impossible — the
operator must explicitly state which clock they meant. Cheaper
than retrofitting a "what tz did you mean?" prompt; eliminates
the entire class of bug.

### v0.6.11 — `vq admin auto-update ENV` latest-tag drift verb (2026-05-18)

**Fourth post-audit feature ship.** Closes the "systemd-timer
auto-update" item from the v0.6 deferred list, but as a CLI verb
rather than a daemon-side timer. The CLI is the smaller, safer
surface: operators wire their own systemd-timer / cron entry if
they want unattended polling; the verb itself is single-env,
explicit, and refuses anything other than latest-tag drift.

**New verb `vq admin auto-update ENV [HOST]`**:

* Queries the env's git remote: `git ls-remote --tags origin`
* Filters to semver-shaped tags (`vMAJOR.MINOR.PATCH[suffix]`)
* Picks the newest by `(major, minor, patch)` tuple — handles
  double-digit minors correctly (`v0.10.0 > v0.9.0`, which pure
  lex sort gets wrong)
* Compares to env's `git describe --exact-match --tags HEAD`
* On drift: calls `admin.update_env(env, expected_tag=newest)` —
  re-uses the v0.5.24 tag verification so a pull-then-build that
  doesn't land the expected tag fails the apply rather than
  building against the wrong commit

**Latest-tag only by design.** Track-branch / dev-tip HEAD-tracking
is INTENTIONALLY out of scope:

* Auto-deploying a dev-branch tip would silently ship half-baked
  commits to a live queue.
* The operator already vetted the *tag* before pushing it; the
  dev branch HEAD is by definition unvetted.
* For dev envs: run `vq admin update vibeqc-dev` directly from
  shell / cron / systemd-timer. The auto-update verb stays
  narrowly scoped to its safe use case.

**CLI shape**:

```
vq admin auto-update ENV              # default_host
vq admin auto-update ENV HOST         # explicit host
vq admin auto-update ENV --dry-run    # probe only
vq admin auto-update ENV --json       # machine-readable
```

Three opt-ins keep the surface safe:

1. Operator types the env name explicitly (no fleet-wide misfire).
2. The verb only acts on semver-tag drift (no silent dev-tip
   deploys).
3. `--dry-run` is the cheapest way to ask "is there drift?"
   without applying.

**Exit codes**:

* 0 — drift applied OK, or no drift (`action=skip`)
* non-zero — apply failed, or git probe failed (`action=error`)

`--dry-run` exits 0 even when drift is detected (the operator
asked "would you?"; the answer "yes" isn't a failure).

**New module `src/vq/auto_update.py`** (~230 LoC):

* `_parse_semver_tag(tag)` — regex-based tuple parser.
* `_newest_semver_tag(tags)` — tuple-sort newest picker.
* `_list_remote_tags(git_dir)` — `git ls-remote --tags origin`
  with peeled `^{}` ref handling + dedupe.
* `check_env_drift(env, cfg) -> AutoUpdateDecision` — pure
  function; no side effects.
* `auto_update_env(env, cfg, *, host, dry_run)` →
  `AutoUpdateOutcome` (decision + optional update_result).
* `AutoUpdateDecision` / `AutoUpdateOutcome` dataclasses.

**Tests** (+39 in `test_auto_update.py`):

* `TestSemverHelpers` (8) — parse / newest / double-digit minor /
  ignore non-semver / empty input.
* `TestListRemoteTags` (4) — simple output / peeled-suffix dedupe
  / malformed line tolerance / CalledProcessError propagation.
* `TestCheckEnvDrift` (7) — drift detected / no-drift / local
  has no tag / ls-remote error / timeout / no-semver-tags / unknown
  env raises AdminError.
* `TestAutoUpdateEnv` (4) — dry-run never applies / skip path
  doesn't call update_env / error path doesn't call update_env /
  drift apply calls update_env with expected_tag.
* `TestAutoUpdateCLI` (8) — text mode displays decision / apply
  calls update_env / apply failure exits non-zero / skip exits
  zero / error exits non-zero / unknown env at CLI / JSON mode
  shape / help mentions latest-tag-only.

1174 passed / 10 skipped on macOS (+39 from v0.6.10).

**Doc updates**: handover.md and operations.md mention the new
verb. roadmap.md v0.6 deferred list updated.

**Fleet deploy**: standard
`vq admin update vibeqc-queue compute-d compute-a`.

**Why no systemd-timer unit files this ship**: the verb is the
hard part; wrapping it in a `.service` + `.timer` is a one-time
operator setup. Shipping the units risks them landing on hosts
without the corresponding cron-schedule decision, which the
operator should make. Docs explain the wiring; if a fleet-wide
unit becomes common, a follow-up ship can templatize it.

### v0.6.10 — bulk resubmit via `--state STATE` (2026-05-18)

**Third post-audit feature ship.** Closes the v0.7 carryover item
the v0.6.5 roadmap rewrite flagged as
"`vq queue --resubmit-aborted-by-queue` — refresh everything that
died in the reboot." After v0.6.8 + v0.6.9 hardened the per-job
verb, the bulk form is a thin layer on top.

**New CLI shape**:

```
# Single (existing):
vq resubmit HOST JOBID
vq resubmit JOBID                          # default_host

# Bulk (new):
vq resubmit --state STATE [--state STATE2 ...]    # default_host
vq resubmit HOST --state STATE                    # explicit host
vq resubmit --state aborted_by_queue              # post-reboot recovery
```

`--state` is repeatable; only terminal states are valid choices
(running / pending / suspended are excluded by Click). Mutually
exclusive with the positional JOBID: passing both is rejected
with a UsageError pointing operators at the right form for their
intent. Per-flag overrides (`--cpus`, `--mem-mb`,
`--wall-time-seconds`, `--priority`, `--retry`, `--job-name`,
`--tag`, `--clear-tags`) apply uniformly to every resubmitted
job in the batch.

**Output format**:

* **stdout**: one new jobid per line. Scriptable —
  `vq resubmit --state aborted_by_queue | xargs -I{} vq tail {} -f`
  works without any text wrangling.
* **stderr**: per-source mapping (`  abc... -> def...`),
  per-failure error lines, final summary
  (`resubmitted N jobs (M errors)`).

Two failure modes that don't abort the batch (each lands in
`result.errors`):

* Source workspace gone (cleaned up after the source job
  finished) — `FileNotFoundError`.
* Source spec corrupt (unparseable JSON) — caught at the glob
  level so the daemon doesn't crash mid-batch on bad on-disk
  state.

If every source fails (no successful resubmits) the CLI exits
non-zero with the summary so wrapper scripts notice; mixed
success/failure exits zero and the operator reads stderr.

**New API**:

* `resubmit_state(states, *, overrides) -> BulkResubmitResult` —
  local-side helper.
* `BulkResubmitResult` dataclass: `pairs: list[(src, new)]`,
  `errors: list[(src, msg)]`.
* `resubmit_state_remote(host_cfg, states, *, overrides) ->
  (new_ids, stderr_text)` — SSH-forwarder; captures the remote's
  stdout (list of new jobids) and stderr (per-source mapping +
  summary) and routes them back to the local CLI's streams.
* `_build_override_argv(overrides)` — extracted helper, shared
  by `resubmit_remote` (single) and `resubmit_state_remote`
  (bulk) so both forward overrides identically.

**Tests** (+16, total 54 in `test_resubmit.py`):

* `TestResubmitState` (8): empty-states is a no-op; filters by
  state; multiple states compose (OR-semantics); non-matching
  yields empty; overrides apply to every match; one failure
  doesn't abort the batch; corrupt spec is skipped with an
  error entry; RUNNING source is not matched even if FAILED is
  requested.
* `TestBulkResubmitCLI` (6): bulk mode prints jobids on stdout;
  summary on stderr; `--state` + positional JOBID mutually
  exclusive; running/pending/suspended rejected by Click's
  choice type; overrides applied; invalid state name rejected.
* `TestBulkResubmitRemote` (2): argv includes every state flag
  + overrides; bad remote stdout (non-jobid line) raises
  RemoteError.

1135 passed / 10 skipped on macOS (+16 from v0.6.9).

**Why the choice list excludes the non-terminal states**: by
the time an operator's invoking bulk resubmit, they've already
seen the matching jobs in `vq queue` and are choosing to rerun
them. Letting `--state running` through would route into
`resubmit_local` which would refuse each job individually
(non-terminal source rejected) — but emit a confusing summary
of "0 resubmitted, N errors." Refusing at the CLI boundary
gives operators a clearer "this isn't the verb for that" before
any work happens.

**Fleet deploy**: standard `vq admin update vibeqc-queue
compute-d compute-a` picks up the new flag via the editable install
path; the daemon's behavior doesn't change.

### v0.6.9 — `vq resubmit` cleans inherited daemon artifacts (2026-05-18)

**Polish fix on v0.6.8.** End-to-end smoke test on compute-d after
the v0.6.8 ship revealed an interleaving issue: the new
workspace's deep-copy carried the source's `stdout.log`,
`stderr.log`, `_vq/events.jsonl`, `_vq/exit-code`, and
`_vq/samples.jsonl`. The new run's bash wrap then APPENDED into
those files, so `vq status` on the resubmit showed both runs'
output concatenated, the watchdog samples mixed two jobs' RSS
curves, and the SUBMITTED event for the new job landed at the
bottom of the source's history.

Fix: new `_clean_for_resubmit(workspace)` helper unlinks
`stdout.log`, `stderr.log`, `_vq/events.jsonl`, `_vq/exit-code`,
`_vq/samples.jsonl` from the new workspace after the deep-copy
(or archive-extract), before the SUBMITTED event is appended.
Best-effort: missing files are not an error; OSError on unlink
logs a warning and continues. Inputs are NEVER touched — only
the daemon-managed artifacts listed in
`_RESUBMIT_CLEAN_RELPATHS`.

**Why not pre-emptively purge all of `_vq/`**: that directory
may someday hold per-job markers / sidecar metadata the operator
cares about. The cleanup is a narrow allowlist, not a blanket
wipe.

**Test** (+1, `test_resubmit.TestResubmitLocal.test_clean_inherited_daemon_artifacts`):
pre-stamps each artifact in the source's workspace, resubmits,
asserts the new workspace has them gone (or in events.jsonl's
case, contains exactly one line — the new SUBMITTED entry).

1119 passed / 10 skipped on macOS (+1 from v0.6.8).

**Fleet deploy**: standard
`vq admin update vibeqc-queue compute-d compute-a`.

### v0.6.8 — `vq resubmit <jobid>` operator-driven rerun verb (2026-05-18)

**Second post-audit feature ship.** Closes the long-standing docs-
lie: `vq resubmit <jobid>` has been mentioned in three docs
(handover.md, operations.md, lifecycle.md) as "the one-step
recovery" since v0.5.x, but the verb itself never existed —
operators had to do the manual `vq fetch + vq submit -d` dance.

**New verb `vq resubmit [HOST] JOBID`**:

* Reads source spec from the queue.
* Refuses non-terminal source states (RUNNING / PENDING /
  SUSPENDED) with a UsageError that points the operator at
  `vq kill` first if they want to abort+rerun.
* Builds a fresh workspace: deep-copies `source.cwd` (or extracts
  `source.archive_path` when the source is archived via
  `vq cleanup --archive`).
* Writes a new JobSpec with a fresh jobid +
  `parent_jobid = source.id`, state PENDING. The new spec
  inherits `cpus / mem_mb / wall_time_seconds / priority /
  retry_max / tags / job_name / branch / recover_on_reboot`
  from the source; `retry_count` resets to 0 (fresh budget);
  `submitter` is stamped with the current user.
* Appends a SUBMITTED event to the NEW workspace's events.jsonl
  (the source workspace is left untouched).
* Prints the new jobid on stdout, same shape as `vq submit`.

**Per-flag overrides** (each replaces the inherited value only on
the new spec):

* `--cpus N`, `--mem-mb N`, `--wall-time-seconds N`,
  `--priority N`, `--retry N` (sets `retry_max`)
* `--job-name NAME`
* `--tag TAG` (repeatable) — replaces tags wholesale; mutually
  exclusive with…
* `--clear-tags` — explicit wipe of inherited tags.

**Remote-host path**: `vq resubmit compute-d JOBID` forwards over
ssh as `<remote_vq> resubmit localhost JOBID [overrides]`; the
remote vq does the local-side work and returns the new jobid.
The flag set matches the local CLI 1:1; `--clear-tags` is the
sentinel that the remote uses to distinguish "inherit" from
"override-with-empty."

**Why fresh-workspace as the default** (not the daemon's
`_auto_resume` same-workspace semantics):

* Matches the handover.md / operations.md manual recipe
  operators were already using.
* Idempotent — the new run doesn't inherit half-written outputs
  from the source run.
* Concurrency-safe — no race against any process still holding
  the source's `cwd`.
* Solvers that genuinely want restart-from-disk (CRYSTAL fort.20,
  PySCF chkfile, ORCA .gbw) already have `vq submit --auto-resume`
  for that purpose, and the daemon handles it.

**Tests** (+37 in `test_resubmit.py`):

* `TestResubmitLocal` (18): inherit semantics for every field
  (cpus, mem_mb, wall_time_seconds, priority, retry_max,
  recover_on_reboot, branch, tags, job_name); override semantics
  (each flag replaces); retry_count resets; submitter stamps the
  current user; tag override with `[]` clears; non-terminal
  source rejection (parameterized over RUNNING / PENDING /
  SUSPENDED); every TERMINAL_STATES value accepted
  (parameterized); missing source raises FileNotFoundError;
  workspace gone (post-cleanup) raises; archived source extracts
  the tarball; SUBMITTED event appended to NEW workspace with
  `parent_jobid`.
* `TestResubmitCLI` (9): local resubmit prints new jobid; default
  host resolution; non-terminal rejection surfaces as UsageError;
  missing source surfaces as UsageError; `--tag` override;
  `--clear-tags`; `--tag` + `--clear-tags` mutually exclusive;
  `--cpus` override; help text mentions the terminal-state rule.
* `TestResubmitRemote` (3): argv construction with every override
  flag forwarded; `--clear-tags` sentinel for `tags=[]`;
  bad-output detection raises RemoteError.

1118 passed / 10 skipped on macOS (+37 from v0.6.7).

**Docs updates**:

* `docs/handover.md` — replaced the "(`vq resubmit <jobid>` —
  which would do this in one step — is on the v0.6+ list.)" hedge
  with the actual recipe. Also corrected the adjacent
  `--auto-resume` paragraph that claimed the flag was planned for
  v0.7+ (it shipped in v0.5.30).
* `docs/operations.md` — the existing "vq resubmit is also an
  option" mention now points to the real verb with a one-line
  override example. The audit-table mentions of `vq resubmit`
  next to `pid_recycled` / `cgroup_scope_mismatch` ABORTED_BY_QUEUE
  reasons are now accurate.
* `docs/lifecycle.md` — the "the operator can `vq resubmit`
  cleanly" reference is unchanged (the verb now exists).

**Fleet deploy**: standard `vq admin update vibeqc-queue` on
compute-d + compute-a picks up the new verb via the editable
`pip install -e .` path (no native-deps build required).

### v0.6.7 — docs catch-up sweep for the audit-driven hardening (2026-05-18)

**Pure docs ship — no code change.** Closes the documentation
gaps the post-v0.6.6 audit ("Is the documentation updated to
reflect all recent feature additions?") flagged across the
v0.5.42 → v0.6.6 hardening sweep. The features themselves all
shipped between 2026-05-16 and 2026-05-17; the user-facing docs
hadn't caught up.

Four files touched:

* **`docs/operations.md`** — added four new sections:
  * `vq daemon health` (v0.5.49+) promoted to the diagnostic
    entry point at the top + added as step 1 in the escalation
    list, displacing the four-shell-command manual reasoning it
    consolidates.
  * Admin update stuck — `state=failed` marker (v0.6.0+):
    documents the new PAUSING → PAUSED → PULLING → TAG_CHECKING
    → BUILDING → RESUMING → RESTARTING_DAEMON → VERIFYING /
    FAILED state machine + the recovery flow (read
    `failure_reason`, check jobs resumed, `vq admin
    clear-update-marker`, re-run). Notes `--force` as the
    operator escape hatch.
  * Daemon log: "scope collision detected" (v0.5.50+) — what
    the warning means + that no operator action is required.
  * Daemon log: "daemon running X, on-disk source says Y"
    (v0.6.2+) — version-drift warning + recipe (restart
    vq-daemon) + rationale for warn-not-block.
  * ABORTED_BY_QUEUE with reason `pid_recycled` or
    `cgroup_scope_mismatch` (v0.5.50+) — table of the two new
    terminal reasons + recovery (same as legacy
    aborted_by_queue).

* **`docs/config.toml.example`** — added the v0.5.47
  `provides_branches` field to the `vibeqc-dev` and
  `vibeqc-release` `[programs.X]` blocks (surgical pause/resume
  scope when admin-updating a single env). Added commented-out
  `[programs.crystal23demo]` + `[programs.properties23demo]`
  blocks matching the v0.6.3 fleet config so a new host can
  copy-paste them.

* **`docs/hosts.md`** — added `crystal23demo` and
  `properties23demo` rows to the "Engines registered"
  tables for both compute-d and compute-a; added them to the
  at-a-glance Engines summary row. New host-bin paths use
  `~/bin/...` placeholders per CLAUDE.md § 12 (the file's
  existing absolute-path entries are pre-existing tech debt,
  not propagated to the new rows).

* **`src/vq/__init__.py` + `pyproject.toml`** — version bump to
  0.6.7.

No code change, no test additions. Pre-commit hook passes
(personal-info patterns in new lines all use `~/` placeholders).
No fleet deploy needed for a docs-only ship.

### v0.6.6 — job tags / metadata (2026-05-17)

**First post-audit feature ship.** The smallest of the long-term
items the v0.6.5 roadmap bookkeeping flagged as "smallest useful
next item." Closed in one session.

**New `JobSpec.tags: list[str]`** — additive field with default
`[]`. Pre-v0.6.6 specs on disk read clean (missing key →
empty list). Validator dedupes + sorts at validate time so two
submits with `--tag foo --tag bar` vs `--tag bar --tag foo`
round-trip to identical specs. Same strict charset as `job_name`
(alnum + `-_.`, ≤50 chars) so tags flow into CLI argv + ssh-shipped
argv + filenames without quoting.

**CLI: `vq submit --tag X` (repeatable)** — sets `spec.tags`.
Plumbed through both `submit_local` and `submit_remote`; the
SSH-delegated path forwards each `--tag` separately so the remote
vq applies the same dedup + validation.

**CLI: `vq queue --tag X` (repeatable)** — filter the listing
with AND-semantics. A row shows iff `required_tags ⊆ spec.tags`.
Composes with `--active` / `-s STATE` / `--show-archived` /
`--all` / `HOST`. Filter forwarded over SSH so the wire doesn't
carry rows that get dropped client-side.

**`vq status` display** — `tags:` line appears when spec has any
tags. Stable sort order (matches the validator's normalization).

**Tests** (+12):
* `test_spec.TestTagsField` (5): default empty, dedup+sort,
  invalid charset rejected, JSON round-trip, pre-v0.6.6 reads
  clean.
* `test_submit.TestTagsCapture` (3): tags kwarg lands on spec,
  default empty, None defaults to empty.
* `test_cli.TestTagsCLI` (4): submit stores tags + shows in
  status, queue --tag filter AND-semantics, invalid charset
  rejected at CLI, help mentions --tag on both submit + queue.

1081 passed / 10 skipped on macOS (+12 from v0.6.5).

**Why this scope.** Tags are pure operator metadata — they don't
affect dispatch order, scheduling, or resource accounting (those
remain `priority` / `cpus` / `mem_mb` / wall-time territory). The
audit's discipline ("hardening over features") makes the small
additive case the right shape: ~50 LoC of code + 12 tests, no
new failure surface, no schema-versioned migration story.

`docs/chat-onboarding.md` updated with a row in the submit-flags
table.

### v0.6.5 — roadmap bookkeeping: rewrite v0.6 + v0.7 sections to reflect what shipped (2026-05-17)

**Pure docs ship — no code change.** The v0.6 and v0.7 sections
of `docs/roadmap.md` were multi-paragraph 2026-05-10 planning
documents; most of what they described shipped during the v0.5.x
sweep + the v0.6.0 lifecycle cut, but the sections still read as
"future work" which confused readers about what's actually done
vs deferred.

Rewrote both sections as tables mapping each original-design
item to the release that shipped it, plus a short "what remains
for v0.6.x+ / v0.7" list. For v0.6: ~22 items shipped across
v0.5.x → v0.6.4; 6 items remain (multi-user, per-user quotas,
bearer-token admin, `VQ_FORCE_NATIVE_DEPS_CHECK`,
systemd-timer auto-update, vq-clone consolidation). For v0.7: 4
of 5 original items shipped (priority, retry, notifications,
auto-resume); per-user quotas remains gated on multi-user.

Long-term-ideas section also tightened with concrete "smallest
useful next item" notes so the next chat continuing the roadmap
doesn't have to re-derive the scoping. Out-of-scope section
unchanged.

1069 passed / 10 skipped (unchanged from v0.6.4). No fleet deploy
needed.

### v0.6.4 — CRYSTAL23 demo reference systems (2026-05-17)

**Fleet test-fixture addition.** v0.6.3 enabled the wrapper +
fleet config for `crystal23demo`; v0.6.4 mirrors the 15 official
demo input files from the canonical CRYSTAL distribution
(`crystal.unito.it/test_demo/inputs/`, linked from
`https://www.crystalsolutions.eu/try-it.html`) into the repo so
chats have an in-repo set of known-good inputs to smoke-test
against.

**New directory: `examples/crystal23_demo/`**

15 `.d12` SCF inputs covering bulk (Be / MgO / NiO / Si / C
diamond / urea), slab (graphite monolayer), polymer ((SN)x),
and surface chemistry (MgO 001 ± CO). All sized for the demo's
10-atom-per-primitive-cell cap; asymmetric-unit atom counts
documented in the per-system table. README covers:

* The 15 systems with atom counts + dimensionality + basis;
* The demo binary's capability + 10-atom limit semantics;
* The `vq submit ... --demo input.d12` recipe (matches the
  v0.6.3 wrapper + chat-onboarding.md idiom);
* Provenance + license / redistribution note (CLAUDE.md § 1
  on-demand-fetch fallback documented).

**Fleet smoke from v0.6.3** validated end-to-end: compute-d
completed the LiH/STO-3G demo job in 6m17s, exit 0 — proves the
wrapper's `--demo` path + `CRYSTAL23DEMO_BIN` env override work
through the daemon + the new `[programs.crystal23demo]` registry
entry.

**No code change.** Pure example additions + a docs README. No
tests required.

### v0.6.3 — CRYSTAL23 demo support (2026-05-17)

**Fleet capability addition.** Both compute-a and compute-d have
``crystal23demo`` and ``properties23demo`` binaries in ``~/bin``
(full CRYSTAL23 feature set capped at 10 atoms per primitive cell;
the only thing distinguishing the demo from a paid v23 license).
No parallel demo binary ships; the demo is serial-only.

**Wrapper changes** (``contrib/run-crystal.sh``):

* New ``--demo`` flag selects ``crystal23demo`` (or
  ``properties23demo`` when composed with ``--properties``). Implies
  serial; combining with ``--np`` is rejected loudly rather than
  silently dropping the parallelism request.
* New env-var overrides ``CRYSTAL23DEMO_BIN`` and
  ``PROPERTIES23DEMO_BIN`` (parallel the v0.5.19
  ``CRYSTAL_BIN`` / ``PCRYSTAL_BIN`` / ``PROPERTIES_BIN`` /
  ``PPROPERTIES_BIN`` pattern).
* Status label updated to call out the 10-atom limit so it appears
  in the daemon's stderr log next to the job's "input: …" line.

**Fleet config** (applied to ``~/.config/vq/config.toml`` on both
hosts; ``.bak-pre-0.6.3`` backups saved):

```toml
[programs.crystal23demo]
kind        = "binary"
binary      = "/home/USER/bin/crystal23demo"
description = "CRYSTAL23 demo SCF (serial; 10-atom cell limit)"

[programs.properties23demo]
kind        = "binary"
binary      = "/home/USER/bin/properties23demo"
description = "PROPERTIES23 demo post-processing (serial; 10-atom cell limit)"
```

Both entries surface in ``vq programs --all`` with KIND=binary and
STATUS=OK.

**Submission idiom** (added to ``docs/chat-onboarding.md``):

```sh
# CRYSTAL23 demo SCF
vq submit -d ./calc --cpus 1 --wall-time-seconds 7200 -- \
    env CRYSTAL23DEMO_BIN=/home/USER/bin/crystal23demo \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --demo input.d12

# CRYSTAL23 demo + PROPERTIES23 demo post-processing
vq submit -d ./calc --cpus 1 --wall-time-seconds 3600 -- \
    env PROPERTIES23DEMO_BIN=/home/USER/bin/properties23demo \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --demo --properties propinput.d3
```

**Why no test in the suite for the demo binaries**: the wrapper's
binary-selection logic is shell, not Python; the unit test
``tests/test_run_crystal_wrapper.py`` (if added) would have to
shell out to bash to exercise it. Existing strategy is to run the
wrapper end-to-end via ``tests/integration_smoke.py`` against
``vq programs --json``; that path will pick up the new binaries
automatically once a smoke run targets them. v0.6.3 ships the
wrapper change + the fleet config addition; explicit unit tests
for the shell control flow can land in a follow-up if the
wrapper grows past its current size.

**Deploy**: standard ``vq admin update vibeqc-queue <host>``
picks up the wrapper change (lives in ``vibe-queue/contrib/`` so
it travels with the queue venv update). The config additions on
each host took effect immediately (``vq programs`` reads config
fresh per invocation; no daemon restart required for the
registry-side change).

### v0.6.2 — daemon-side version-drift probe (audit § 1c) (2026-05-17)

**Closes the last meaningful audit § 1 invariant gap.** The audit
flagged "the daemon has no way to notice it's running stale code
after a manual `git pull` that didn't go through `vq admin update`"
— a real operator-bypass class. v0.5.42+'s auto-restart handles
the standard `vq admin update vibeqc-queue` path; this v0.6.2
ship is the safety net.

Implementation:

* New module-level `_read_vq_version_from_source()` reads
  `vq/__init__.py` from disk and parses out `__version__` via
  regex. Returns None on any failure (best-effort).
* New `Daemon._maybe_check_version_drift()` called once per
  `iterate()` tick. Rate-limited to once per 60 seconds via
  `self._last_version_drift_check_monotonic` (cheap probe even
  if uncached: one stat + one read + one regex).
* On drift detected: WARNING with "daemon running X, on-disk
  source says Y" + the recovery recipe. Stamped against
  `self._version_drift_seen_at_version` so the warning fires
  once per distinct drift state, not every 60s.
* On drift cleared (operator restarted, versions match again):
  INFO "drift cleared" + the now-current version. Resets the
  per-state stamp.
* Unreadable source: silent no-op (the probe is best-effort).

**Why this is the right shape, not e.g. refuse-to-dispatch:**
the auto-restart path already gives us the strong guarantee
for the standard case. Operator bypass is rare and the operator
generally knows what they're doing — a loud warning in the daemon
log is the right balance vs blocking dispatch (which would
escalate a misconfiguration into a queue outage).

**Tests** (+5 in `test_daemon.py::TestVersionDriftProbe`):

* `_read_vq_version_from_source` returns the live `__version__`
  for the actual install (sanity).
* drift detected → WARNING fires once; second call within the
  rate-limit window is a no-op.
* matching versions → no warning.
* drift cleared after a previous drift → INFO "drift cleared"
  fires.
* unreadable source → silent no-op.

1069 passed / 10 skipped on macOS (+5 from v0.6.1).

**Audit closure.** With v0.6.2 every audit § 1 invariant and
every audit § 2 failure mode has either shipped, documented as
a known trade-off, or been genuinely superseded. The audit-driven
hardening sweep is complete.

### v0.6.1 — state machine polish: phase split + restart-attempted-only transition (2026-05-17)

**Two follow-on improvements to v0.6.0's state machine.** Pure
polish — no new failure-mode coverage, just better diagnostic
accuracy in the state banner.

#### 1. PULLING phase splits into PULLING / TAG_CHECKING / BUILDING

Pre-v0.6.1 a single PULLING transition fired at the start of
`_do_update_work` and stayed until RESUMING. A stuck update during
the heavy `update_script` step (e.g. a ninja build hanging at
30 min in) showed `state=pulling` in the status banner — confusing
because git pull finished seconds in.

v0.6.1 adds two interior transitions inside `_do_update_work`:

* `TAG_CHECKING` fires before `_run_git_tag_check` (only when
  `--tag` is given AND git pull succeeded).
* `BUILDING` fires before `_run_update_script` (only when
  `update_script` is configured AND prerequisites passed).

Operator now sees `state=building` (with `phase_started_at`
showing when ninja kicked off) for the long-tail case, and
`state=tag_checking` for the brief tag-verify step. PULLING stays
as the cover for `git pull` itself.

The transitions are no-ops when no marker exists (e.g. direct
`_do_update_work` test invocation outside `update_env`) — kept
the helper tolerant for unit-test friendliness.

#### 2. RESTARTING_DAEMON transitions only when restart is attempted

Pre-v0.6.1 `update_env` / `update_all` unconditionally transitioned
to RESTARTING_DAEMON immediately before calling
`_maybe_restart_daemon`, regardless of whether a restart actually
happened. Updates of non-self-update envs (vibeqc-dev,
vibeqc-release) misleadingly showed `state=restarting_daemon` in
the banner for a moment even though the helper's first action was
to skip with "not a vq self-update — daemon untouched."

v0.6.1 moves the transition INSIDE `_maybe_restart_daemon`, right
before `_restart_vq_daemon` is called. Non-self-update envs skip
straight from PULLING/BUILDING to VERIFYING. Self-update envs
(vibeqc-queue) hit RESTARTING_DAEMON as the proper signal.

#### Tests (+5)

* `TestStateMachinePhaseSplits` (3) — TAG_CHECKING fires when
  `--tag` is given; BUILDING fires when update_script is
  configured; neither fires when both are absent.
* `TestRestartingDaemonTransitionAttemptedOnly` (2) —
  non-self-update env doesn't fire RESTARTING_DAEMON (probe
  returns `is_self_update=False`); self-update env DOES fire it
  (probe returns True with mocked restart success).

1064 passed / 10 skipped on macOS (+5 from v0.6.0).

#### Deferred

Same as v0.6.0's deferred list (multi-user, finer state splits
like PAUSING/PAUSED breakdown if that ever matters). No new
deferrals from v0.6.1.

### v0.6.0 — lifecycle backbone: state machine, race fix, cgroup-scope cross-check, `vq daemon start` removal (2026-05-17)

**The audit-blessed v0.6.0 cut.** Four pieces that together
complete the lifecycle-hardening arc the audit proposed; the
breaking change (`vq daemon start` removal) is the moment that
stops the dual-identity-source class of failure mode permanently.
Batched into one ship to honor the daemon-restart-churn discipline.

#### 1. Admin-update state machine (audit § 4b)

The boolean marker file becomes a multi-phase state machine
extension (additive schema; pre-v0.6.0 markers read as
`state="legacy_in_progress"` for backward compat). Sequence:

```
PAUSING -> PAUSED -> PULLING -> RESUMING ->
RESTARTING_DAEMON -> VERIFYING -> (file removed = IDLE)
                               \-> FAILED (sticky)
```

* `acquire_admin_update_marker` writes `state=PAUSING` initially.
* New `transition_admin_update_state(new_state,
  failure_reason=None)` rewrites the file atomically.
* `update_env` / `update_all` drive the state machine through
  each phase.
* Success → file removed (IDLE).
* Failure → `transition_admin_update_state(FAILED, failure_reason=...)`
  with a one-line `failure_reason` capturing the specific
  diagnostic (e.g. `"git pull rc=128"`, `"daemon restart failed:
  systemctl --user is unreachable"`).

New `AdminUpdateMarker` fields: `state`, `phase_started_at`,
`failure_reason`. `format_admin_status` banner now shows
state + phase_started_at + failure_reason when present.

Why this matters: pre-v0.6.0 the marker just said "in progress"
— operator had no way to tell "stuck at git pull for 47m" vs
"died during update_script". Now the state file IS the
diagnostic.

#### 2. `_start_job` race fix (audit § 2b)

The dispatch path now writes `spec.state=RUNNING` with
`pid=None`/`pgid=None` BEFORE `subprocess.Popen` instead of
after. Pre-v0.6.0 a daemon crash in the narrow window between
Popen.success and the post-Popen `spec.write` would leave a
PENDING spec next to a running process — the next daemon loop
would re-dispatch it (DOUBLE DISPATCH). Now the recovery path
sees `RUNNING + pgid=None` and marks `ABORTED_BY_QUEUE` via the
existing "no pgid recorded" branch.

The window is microseconds-narrow in normal operation; the fix
eliminates the failure mode entirely regardless of timing.

#### 3. cgroup-scope MainPID cross-check at startup (audit § 4c.2)

New `cgroup.scope_main_pid(unit_name)` queries
`systemctl --user show <scope> -p MainPID --value`. The
`_reattach_or_interrupt_at_startup` recovery path, when
`cgroup_enabled` is True, cross-checks the scope's MainPID
against `spec.pid` after the pgid liveness + PID-fingerprint
checks pass. Mismatch (or unit not found while systemctl IS
reachable) → spec lands `ABORTED_BY_QUEUE` with reason
`cgroup_scope_mismatch`.

This complements v0.5.50's PID-fingerprint: PID-fingerprint
detects kernel PID-recycle; cgroup-scope cross-check detects
the case where the scope unit was detached / collected even
though the PID stayed alive (e.g. operator manually stopped
the scope while the process kept running).

#### 4. `vq daemon start` REMOVED (was warn-only in v0.5.50)

**Breaking change.** The CLI verb now raises `UsageError` with
the migration recipe instead of spawning a daemon. The
underlying `daemon_control.start_daemon` Python API is
retained for the e2e test suite, but production callers must
use the systemd-user unit (`contrib/vq-daemon.service`).

Closes the entire dual-identity-source class (pidfile vs systemd
MainPID) that `vq daemon health` (v0.5.49) flagged. The recipe
in the error message:

```sh
cp contrib/vq-daemon.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now vq-daemon
vq daemon health        # verify
```

#### docs/lifecycle.md updates

Documents the state machine, the cgroup-scope cross-check, the
`_start_job` race fix, and the `vq daemon start` removal — so
the file remains the canonical contract reference.

#### Tests (+10)

* `test_admin.py::TestAdminUpdateStateMachine` (6) — initial
  state=pausing, transition updates state + phase_started_at,
  transition with no marker is no-op, failure transition records
  reason, legacy marker default, status banner shows state +
  failure_reason.
* `test_admin.py::TestUpdateEnvDrivesStateMachine` (2) — success
  path removes marker; failure path transitions to sticky FAILED
  with specific reason.
* `test_daemon.py::TestStartJobRaceFix` (1) — at the moment
  Popen would have been called, the spec on disk is already
  RUNNING + pid=None (race-fix invariant).
* `test_daemon.py::TestCgroupScopeMainPidCrossCheck` (2,
  Linux-only) — scope mismatch marks ABORTED_BY_QUEUE; scope_main_pid
  returning None falls back to pgid+fingerprint verdict.
* `test_e2e.py::TestDaemonProcessE2E::test_cli_start_was_removed`
  (1) — CLI verb errors with migration recipe.
* Two existing e2e tests (`test_full_lifecycle`,
  `test_double_start_rejected`) updated to use the Python API
  directly instead of the removed CLI verb.

1059 passed / 10 skipped on macOS (+10 from v0.5.51).

#### Migration notes for operators

* **CLI**: `vq daemon start` now exits non-zero with the
  migration recipe. Scripts calling it must switch to managing
  the systemd-user unit. The error message has the exact
  commands.
* **Marker file**: existing markers on disk (from v0.5.49 or
  v0.5.50 / v0.5.51) read clean; the `state` field defaults to
  `legacy_in_progress` so the guard still fires. v0.5.x daemons
  reading a v0.6.0 marker also work — they don't know about the
  state field but see file-present = blocked.
* **Specs on disk**: `pid_start_time` field (v0.5.50) is now
  joined by the v0.6.0 RUNNING+pid=None transition during
  dispatch — recovery handles all cases.

#### Deferred

* The phased state machine still treats `PULLING` as one
  conceptual phase (covers git pull + tag check + update_script).
  A future v0.6.x could split into PULLING / TAG_CHECKING /
  BUILDING for finer-grained "where did it die" visibility.
* `_maybe_restart_daemon` could also drive the state machine
  through RESTARTING_DAEMON → VERIFYING with daemon-restart
  result baked into the state file. Currently the transitions
  fire in `update_env` / `update_all` around the
  `_maybe_restart_daemon` call.
* Multi-user (v0.6.1+ per the original v0.6 roadmap).

### v0.5.51 — diagnostic-richness hardening + scope-collision guard (2026-05-17)

**Three small additive hardenings.** Picks up the lifecycle wiring,
the marker-PID liveness signal, and the cgroup scope-collision
edge case that the audit listed but v0.5.50's strict scope discipline
deferred. All additive; existing fleet behavior unchanged on the
happy path.

#### 1. `_maybe_restart_daemon` includes contract findings on failure

When the self-update auto-restart path detects "systemctl
unreachable on a vq self-update" (the worst case: stale daemon code
AND no way to fix it), the failure message now includes the
`lifecycle.verify_user_systemd_contract()` findings. Pre-v0.5.51
the operator saw one diagnostic line; now they see all four sources
of truth (loginctl / pgrep / systemctl / pidfile) cross-checked,
with explicit `FAIL` lines pointing at exactly what's broken.

Also adds a pointer to `docs/lifecycle.md § 'user-systemd
orphan / zombie'` alongside the existing `operations.md` pointer
— the lifecycle doc shipped in v0.5.50 has the canonical recovery
recipe now.

Backward compat: the original probe diagnostic line is still
present in the message; no callers reading `daemon_restart_message`
need to change.

#### 2. Marker-PID liveness signal at daemon-tick

`Daemon._poll_admin_update_marker` now probes
`kill(marker.pid, 0)` when it first sees a marker. Dead PID
escalates the log entry from `INFO` → `WARNING` with explicit
"previous `vq admin update` was killed mid-flight; venv state
may be inconsistent" — distinguishing the genuine mid-flight
crash from "operator started an update and forgot to clear the
marker" (alive PID case). The operator sees the failure-mode
classification in the daemon log without needing to manually
correlate the marker's pid against running processes.

New module-level helper `daemon._pid_is_alive(pid) -> bool | None`
covers the standard `kill(pid, 0)` cases (ProcessLookupError →
False, PermissionError → True, other OSError → None).

#### 3. cgroup scope-name collision detection in `_start_job`

Audit § 2e: when a previous job's transient scope leaked past
`--collect` cleanup, `systemd-run --unit=vq-job-<id>.scope`
fails cryptically with "Unit already exists" and the daemon
re-dispatches the same broken spec forever. Now pre-flight
checked.

New cgroup helpers:
* `cgroup.scope_exists(unit_name) -> bool | None` — probes
  `systemctl --user show <unit> -p LoadState --value`. Suffix-
  tolerant: callers can pass either `vq-job-X` or `vq-job-X.scope`.
* `cgroup.stop_scope(unit_name) -> bool` — best-effort
  `systemctl --user stop <unit>` for the recovery path.

`_start_job` now:
1. Computes `scope_name = f"vq-job-{spec.id}"` once (refactor).
2. If `cgroup_enabled` AND `scope_exists(scope_name) is True`,
   try `stop_scope`; log success/failure.
3. If stop fails AND scope still exists on the re-probe, land
   the spec FAILED with `exit_code=-1` and a log line pointing
   at `systemctl --user reset-failed <scope>` / `kill <scope>`
   as the operator's recovery path. Returns False from
   `_start_job` so the dispatch loop moves to the next spec
   rather than retrying the same broken one.

#### Tests (+5)

`test_admin.py`:
* `TestMaybeRestartDaemonContractWiring` (1) — fake probe +
  fake contract verdict; assert the failure message contains
  the verdict's `FAIL` finding lines AND the lifecycle.md
  pointer.

`test_daemon.py`:
* `TestMarkerPidLivenessCheck` (2) — write a marker with a
  guaranteed-dead PID (`2**31 - 1`); assert WARNING with
  "killed mid-flight" fires. Write a marker with the test's
  own PID (alive); assert INFO with "still running" fires
  instead.
* `TestScopeCollisionDetection` (2) — collision + stop-fails
  path lands spec FAILED; no-collision happy path proceeds
  normally.

1049 passed / 8 skipped on macOS (+5 from v0.5.50).

#### Deferred to v0.6.0

Same list as v0.5.50: state machine replacing the marker file,
remove `vq daemon start` (vs current warn-only), `_start_job`
race fix (audit § 2b), cgroup-scope MainPID cross-check at
startup (the static structural fix; the v0.5.51 work above is
the runtime collision guard, not the post-restart re-attach
check).

### v0.5.50 — daemon-lifecycle hardening cluster (2026-05-17)

**Batched hardening release.** Four additive fixes plus a
deprecation warning plus the documented user-systemd contract.
All audit-flagged items that can ship without breaking changes;
the breaking restructure (state machine replacing the marker
file, `vq daemon start` removal) stays parked for the actual
v0.6.0 cut. Built as one ship to minimise daemon-restart churn
on the fleet (lesson from the v0.5.42→48 cadence).

#### 1. PID-fingerprint anti-recycle check at daemon startup (audit § 2d)

`_reattach_or_interrupt_at_startup` previously decided "still
alive, treat as orphan" purely on `killpg(spec.pgid, 0)`. The
kernel can recycle a PID to an unrelated process after the
daemon dies — `killpg` succeeds against the recycled process,
and the orphan reconciler then tracks an unrelated user
process as if it were the original job. Eventually that
process exits, vq writes ABORTED_BY_QUEUE with a bogus exit
marker, or worse, the watchdog samples `/proc/<pid>` and
records nonsense.

Fix: capture `/proc/<spec.pid>/stat` field 22 (process start
time in clock ticks since boot) at dispatch into a new
`JobSpec.pid_start_time` field, and cross-check it at startup
recovery. Mismatch → spec lands in ABORTED_BY_QUEUE with
reason `pid_recycled`. macOS / pre-v0.5.50 specs return None
from the helper and the check is skipped (falls back to pgid-
only liveness).

New module-level helpers in `daemon.py`:
* `_read_pid_start_time(pid) -> int | None`
* `_pid_fingerprint_matches(spec) -> bool | None`

Recovery path at `daemon.py:_reattach_or_interrupt_at_startup`
now consults the fingerprint when both spec.pid and
spec.pid_start_time are set.

#### 2. Atomic admin-update marker write (audit § 2c)

Pre-v0.5.50 `_guard_admin_update_marker` (existence check)
and `write_admin_update_marker` (the writer) were two separate
calls. Two concurrent `vq admin update` invocations could
both pass the guard, both write the marker — the second
silently overwrote the first.

Fix: new `acquire_admin_update_marker(envs, host, force=False)`
that does the check-and-write as one `os.open(O_CREAT|O_EXCL)`
call. `FileExistsError` from the open is converted to
`AdminError` with the same recovery-recipe message
`_guard_admin_update_marker` produces. `force=True` keeps the
pre-v0.5.50 overwrite semantics so the `--force` flag's
documented behaviour is preserved.

`update_env` and `update_all` rewired to use `acquire_…` (the
old pre-pause `_guard_…` call stays as the fast-path "fail
before pausing" check; the authoritative race-free claim is
in `acquire_…`).

#### 3. cgroup availability re-probe at daemon startup (audit § 2j)

`cgroup.available()` is `@lru_cache`d for the daemon's
lifetime. Pre-v0.5.50, a daemon restart inherited the
previous daemon's cached True even when user-systemd had lost
its delegated controllers between restarts (e.g. user-systemd
killed by OOM and respawned without the Delegate= drop-in,
or the 2026-05-17 OOM cascade perturbed the hierarchy). Every
subsequent `_start_job` would then fail at the `systemd-run`
call with no recovery — the daemon would re-dispatch the
same failed spec indefinitely.

Fix: `Daemon.__init__` now calls
`cgroup.reset_availability_cache()` before `cgroup.available()`,
re-probing from scratch. `reset_availability_cache()` itself
is defensive: tolerates `available` being monkeypatched to a
non-cached function (test paths) by `getattr(available,
"cache_clear", None)`.

#### 4. `vq daemon start` deprecation warning

Documented in `docs/lifecycle.md` and warned at runtime: the
daemon should be managed by systemd-user (`vq-daemon.service`),
not spawned via `vq daemon start`. Pre-v0.5.50 both
identity-source paths coexisted silently; their PIDs can
disagree, which `vq daemon health` (v0.5.49) reports as FAIL.

Not removed in v0.5.50 — only a stderr warning is added. v0.6.0
proper is likely the removal moment.

#### 5. `docs/lifecycle.md` — the documented user-systemd contract

New permanent reference: what vq requires (mandatory + strongly
recommended + forbidden configurations), what vq detects + refuses
on (via `vq daemon health` + `vq admin update` guards + daemon
startup checks), what to do when the contract breaks (the symptom
matrix and recovery recipes). Cross-references operations.md for
runtime troubleshooting and points at `vq daemon health` (v0.5.49)
as the diagnostic entry point.

#### Tests (+12)

`test_daemon.py` adds:
* `TestPidFingerprint` (5): `_read_pid_start_time` for self,
  for nonexistent PID, fingerprint `matches` returns None when
  field absent (Linux+macOS), True for live PID, False for
  recycled PID (Linux-only).
* `TestStartupRecoveryPidRecycleDetection` (1, Linux-only):
  end-to-end — write a RUNNING spec with the wrong pid_start_time,
  call `_reattach_or_interrupt_at_startup`, assert
  ABORTED_BY_QUEUE.
* `TestCgroupReprobeAtStartup` (1): Daemon ctor calls
  `reset_availability_cache` before `available`.

`test_admin.py` adds:
* `TestAcquireAdminUpdateMarker` (5): first acquire succeeds,
  second without force raises, force overwrites, O_EXCL
  atomicity (race-correctness), unreadable marker blocks.
* `TestUpdateEnvUsesAcquire` (1): concurrent-update integration.

1044 passed / 8 skipped on macOS (+9 from v0.5.49; the
fingerprint tests skip on macOS, +4 to the skip count).

#### Self-discovered bug fix

While auditing the cgroup change, found that
`cgroup.reset_availability_cache()` would crash with
`AttributeError: 'function' object has no attribute
'cache_clear'` when tests monkeypatch `cgroup.available` with a
plain lambda (existing pattern in `test_drain.py`,
`test_throttle.py`). Defensive `getattr` fix shipped as part of
v0.5.50; no Patch-candidate trailer needed since this code path
only fires when the v0.5.50 daemon constructor calls it.

#### Deferred to v0.6.0 proper

* Wire `_maybe_restart_daemon` to use
  `lifecycle.verify_user_systemd_contract` (replaces the
  message-sniffing `_systemctl_user_available` probe).
* State machine replacing the admin-update-in-progress marker
  file (audit § 4b). Current marker is doing the safety job
  correctly post-v0.5.48 + v0.5.50; the state machine is an
  improvement but not a fix for any current bug — restructure
  belongs in v0.6.0.
* Remove `vq daemon start` (vs current warn-only deprecation).
* cgroup-scope MainPID cross-check at startup recovery (audit
  § 4c.2). Lower priority than PID-fingerprint; deferred.

### v0.5.49 — `vq daemon health` lifecycle contract verifier (2026-05-17)

**First piece of the v0.6.0 lifecycle backbone, shipped as additive
v0.5.49.** Pure read-only diagnostic — never restarts, kills, or
reconfigures anything. Would have caught today's 2026-05-17 compute-d
incident (user-systemd PID 121866 orphaned from `user@1000.service`,
`systemctl --user` returning "Transport endpoint is not connected",
`vq --version` reporting the on-disk version while no daemon was
actually running) in seconds.

**The four sources of truth, cross-checked in one verdict:**

1. **`loginctl show-user $USER`** — PAM/login-layer view of the user
   session (`State`, `RuntimePath`).
2. **`pgrep 'systemd --user'`** — does a user-manager process actually
   exist in the process table? The authoritative "is the user manager
   actually running" signal.
3. **`systemctl --user show vq-daemon.service`** — systemd's view of
   the daemon: `ActiveState` and `MainPID`.
4. **`<state_root>/daemon.pid`** — the pidfile written by
   `vq daemon start`.

`ok=True` only when all four agree and the recorded daemon PID is
alive. Each disagreement adds a `FAIL` finding and flips `ok` to
False; `WARN` / `INFO` cover degraded-but-recoverable states.

**New module + verb:**

* `src/vq/lifecycle.py` — `verify_user_systemd_contract()` returning
  `ContractVerdict` (dataclass with `ok` + the four sources'
  values + a `findings` list of tagged human-readable lines).
* `vq daemon health [HOST | --all] [--json]` — runs the verifier
  locally or via SSH delegation. Same per-host JSON-aggregation
  shape as `vq admin status --all --json` (top-level dict keyed by
  host, per-host failures as `{"error": ...}`). On local mode a
  non-OK verdict exits non-zero so scripts can react.

**Today's compute-d incident, replayed through this verb:**

```text
== daemon lifecycle health ==
verdict: FAILED

findings:
  WARN: loginctl reports State='active'  (manager dead but session entry stale)
  OK: `systemd --user` running at pid=121866   (zombie; PID exists)
  FAIL: `systemctl --user` cannot reach the user manager (orphan / zombie
         — see operations.md § 'Failed to connect to user scope bus...')
```

Operator sees in 3 seconds: user-systemd is orphaned. Go straight to
the force-revive recipe.

**Tests** (+12 in new `test_lifecycle.py`):

* `TestVerifyContractHealthy` (1) — all-green path with all four
  sources agreeing.
* `TestVerifyContractNoSystemdUser` (1) — pgrep returns nothing
  (no user-manager process).
* `TestVerifyContractOrphanSystemdUser` (1) — pgrep returns a PID
  but systemctl can't reach it (today's compute-d symptom).
* `TestVerifyContractPidfileMismatch` (1) — pidfile pid disagrees
  with systemd MainPID (two competing identity sources).
* `TestVerifyContractDaemonProcessGone` (1) — systemd reports
  MainPID but `kill(pid, 0)` says the process is gone.
* `TestVerifyContractLoginctlMissing` (1) — loginctl unavailable
  is WARN not FAIL (some minimal containers).
* `TestFormatVerdictJson` (1) — JSON shape stable, every documented
  field present.
* `TestDaemonHealthCLI` (5) — local text + local JSON + non-OK
  verdict exits non-zero + help mentions all four sources + --all
  is mutually exclusive with HOST.

1035 passed / 4 skipped on macOS (+12 from v0.5.48).

**Scope discipline.** This release deliberately doesn't change ANY
existing behavior — adding a verb and a diagnostic module. The
v0.6.0 backbone proposal (lifecycle module owns the contract, real
state machine replacing the marker file, daemon-startup invariant
checks, deprecate `vq daemon start`) builds on this foundation in
later releases. The audit's strict "no new features in v0.6.0" rule
still applies to the v0.6.0 cut itself; v0.5.49 is the additive
first step, not the breaking restructure.

**Deferred:**

* Make `_maybe_restart_daemon` (admin.py) use
  `verify_user_systemd_contract` instead of its current
  message-sniffing `_systemctl_user_available` probe. Will let it
  produce a richer "why didn't the restart take" message.
* Cross-check the daemon's `sys.executable` against the env's
  `prog.python` (audit § 1c) — currently only the venv bin dir is
  compared.
* Daemon-startup invariant checks (PID-fingerprint via
  `/proc/<pid>/stat[22]` start-time; cgroup-scope MainPID
  cross-check).

### v0.5.48 — two concrete bugs in shipped code from the v0.5.47 audit (2026-05-17)

**Two fixes, both in shipped code, both surfaced by a focused
codebase audit run after the 2026-05-17 compute-d incident.** Not
features — corrections to wiring that was wrong on first commit.

#### Bug A — `update_env` / `update_all` cleared the marker before the daemon-restart attempt

`vq admin update` writes an admin-update-in-progress marker, does
the on-disk work, runs the optional daemon self-update restart,
then clears the marker. Pre-v0.5.48, the marker-clear ran in the
same `finally` block as the queue resume — **before**
`_maybe_restart_daemon`. A failed daemon restart (systemctl
unreachable, timeout, non-zero rc) flipped `result.success` to
False after the marker had already been cleared.

Symptom: operator sees "FAILED" exit from `vq admin update
vibeqc-queue`, plus the "vq-daemon restart failed" reason line,
but `vq admin status` shows no marker. Next operator (or the
same operator next morning) sees a clean state — and re-runs
`vq admin update`, which passes the marker guard, briefly pauses
the queue, runs git pull (no-op, already up to date), tries to
restart again, fails again, again clears nothing. The daemon
silently runs stale code the whole time. The v0.5.44 marker
design specifically intended to prevent this; the wiring was
ordered wrong on first commit.

Fix at `src/vq/admin.py:341-362` (single-env) and
`src/vq/admin.py:432-453` (`--all`): move the conditional
`clear_admin_update_marker()` out of the `finally` block,
**after** `_maybe_restart_daemon` has had a chance to mutate
`result.daemon_restart_succeeded`. `result.success` (the
property) already incorporates a failed restart via the check at
`src/vq/admin.py:172-175` — so the post-restart success check
correctly leaves the marker on disk when the restart failed.

#### Bug B — `Watchdog.reset_sampling_for_resume` was defined but never called

`watchdog.py:138-149` defines `reset_sampling_for_resume(jobid)`
with a docstring documenting exactly the failure mode it's there
to prevent: a long-paused CPU-bound job, on resume, gets a near-
zero CPU% reading (CPU delta computed against pre-pause cputime
over a wall window that includes the entire pause) and a stale
`starve_since_monotonic` from before the pause — both wiring up
to STARVED + SIGTERM on the first post-resume sample.

Verified via `grep -r reset_sampling_for_resume src/vq/`: the
function was defined and documented but never invoked from any
caller. Latent silent-job-kill since v0.5.1.

Fix: wire the transition detector inside `Watchdog.evaluate`
itself rather than depending on the daemon's main loop to track
spec-state history. Added
`WatchdogJobState.last_observed_spec_state: JobState | None`
(additive field). At the entry of `evaluate`, after `st` is
bound, the watchdog checks for a SUSPENDED → RUNNING transition
and calls `reset_sampling_for_resume(jobid)` automatically.
`last_observed_spec_state` is updated unconditionally so a
SUSPENDED job taking the early-OK return at `watchdog.py:204`
still records "we saw it suspended" for the next evaluate to
detect the resume.

#### Tests

* `test_admin.py::TestMarkerClearOrderingAfterDaemonRestart` (3) —
  marker stays when `_maybe_restart_daemon` flips
  `result.success=False`; marker cleared on the happy path with
  explicit successful restart; same shape for `update_all`.
* `test_watchdog.py::TestSuspendedToRunningTransition` (3) — the
  bug-prevention property (stale `starve_since_monotonic`
  cleared on transition); no reset on two consecutive RUNNING
  evaluations (so CPU% deltas keep working); no reset on the
  first evaluate ever for a job.

1023 passed / 4 skipped on macOS (+6 from v0.5.47).

#### Why these slipped past v0.5.44–v0.5.47

Both bugs come from a v0.5.x development pattern the audit
flagged in its trajectory critique. Bug A is a tests-against-
fiction issue: the v0.5.44 test suite for the marker covered
"marker stays on git-pull failure / update-script failure /
work_errors" but did not cover "marker stays on daemon-restart
failure" — the test fixtures bypassed `_maybe_restart_daemon`
entirely with a no-op mock, so the ordering never mattered. Bug
B is dead-code that shipped because no test ever exercised the
pause-then-resume-while-watchdog-tracking path; the docstring
sat as documentation for absent behavior since v0.5.1.

The audit chat that surfaced both is summarised in this
release's commit message; the full report sits in chat history
rather than the repo for v0.5.48 (a permanent record can be
landed as `docs/audit_2026-05-17.md` in a future docs ship if
helpful). Both fixes are independent of the v0.6.0 lifecycle
work the audit recommended.

### v0.5.47 — `provides_branches` surgical pause for `vq admin update <env>` (2026-05-17)

**Operator quality-of-life.** Until v0.5.47, every
`vq admin update <env>` call paused the WHOLE queue: a rebuild
of vibeqc-dev (which only affects main-branch jobs) also paused
release-branch jobs that don't import a single byte of dev-env
code. On compute-d with 30+ release-branch jobs running, this
meant a multi-minute interruption for jobs that had no
correctness exposure to the update.

**Fix.** Opt-in surgical pause via a new `provides_branches`
config field on `VenvProgram`. When set and non-empty,
`vq admin update <env>` pauses ONLY jobs whose `spec.branch` is
in that list; everything else keeps running.

```toml
[programs.vibeqc-dev]
kind              = "venv"
python            = "/home/USER/vibeqc-dev/.venv/bin/python"
git_dir           = "/home/USER/vibeqc-dev"
branch            = "main"
update_script     = "scripts/update.sh --dev"
provides_branches = ["main", "dev", "development"]

[programs.vibeqc-release]
...
provides_branches = ["release", "latest"]
```

Leaving `provides_branches` unset (or empty) preserves the
pre-v0.5.47 queue-wide-pause behavior — safe default. Opt in
when you have a clean branch/env mapping.

**Wiring.**

* `JobSpec.branch: str | None = None` — additive field, captured at
  submit time when `vq submit --branch X` is used. Stored verbatim
  (canonical name OR alias — whatever the user typed) so the
  match against `provides_branches` is direct.
* `vq submit --branch X` — existing flag, now also stamps
  `spec.branch = X` in addition to its existing role of resolving
  X through `[hosts.X.branches]` to a python path.
* `vq submit --branch-name X` — new hidden flag used only by
  `submit_remote` when forwarding a laptop-side `--branch` over
  SSH. Sets `spec.branch` WITHOUT triggering python resolution
  (the python is already resolved on the laptop and passed via
  `--python`). The two flags are mutually exclusive at the CLI
  boundary.
* `pause_resume.pause_provides_branches(host, branches)` — new
  surgical pause; returns `(summary, paused_jobids)`. Jobs with
  `spec.branch = None` (the explicit "untagged" case) are skipped
  by design.
* `pause_resume.resume_jobs(host, jobids)` — symmetric counterpart
  that resumes a specific list. Distinct from `resume_all`
  (which resumes EVERY suspended job): targeting the explicit
  list means operator-paused jobs outside the update's scope
  stay suspended.
* `admin.update_env` — picks the surgical path when
  `prog.provides_branches` is truthy (non-None and non-empty);
  falls back to `pause_all` + `resume_all` otherwise. Both paths
  run their resume in the same try/finally so SIGINT doesn't
  strand jobs.

**Scope: single-env only in v0.5.47.** `vq admin update --all`
still uses queue-wide `pause_all` because the multi-env case
needs the union of all envs' `provides_branches` (and a fallback
to `pause_all` when any env in the batch lacks the declaration).
That's a small follow-on; deferred to v0.5.48.

**Tests** (+19 new, distributed):

* `test_config.py::TestProvidesBranches` (3) — field defaults to
  None, parses list-of-strings, accepts empty list.
* `test_spec.py::TestBranchField` (3) — default None, round-trip,
  pre-v0.5.47 spec (no `branch` key on disk) reads clean.
* `test_submit.py::TestBranchCapture` (3) — submit_local stamps
  branch on spec, default None when unset, alias preserved
  verbatim.
* `test_pause_resume.py::TestPauseProvidesBranches` (3) — filters
  by branch (untagged + other-branch jobs left alone), alias-name
  in provides_branches matches alias-name on spec, empty
  branches list is a 0-pause.
* `test_pause_resume.py::TestResumeJobs` (3) — resumes only the
  listed jobids (others stay suspended), empty list is 0-resume,
  missing jobid surfaces as error.
* `test_admin.py::TestProvidesBranchesIntegration` (4) — update_env
  uses surgical pause when configured, falls back when not, empty
  list also falls back (bool([]) check), failure path still calls
  resume_jobs with the originally-paused list.

1017 passed / 4 skipped on macOS (+19 from v0.5.46).

**Deploy.** Daemon-side. `vq admin update vibeqc-queue <host>`
pulls + auto-restarts. After deploy, to opt in: add
`provides_branches = [...]` to each host's `[programs.vibeqc-dev]`
and `[programs.vibeqc-release]` sections, then submit future jobs
with `vq submit --branch ...` so they get the spec.branch tag.
Pre-v0.5.47 jobs (`spec.branch = None`) are conservatively
SKIPPED by surgical pause — they stay running through dev-env
updates, which is exactly the v0.5.47 ergonomic win.

**Edge case: untagged-and-skipped vs untagged-and-paused.** When
provides_branches IS configured, jobs without a branch tag stay
running through the update. That's an explicit trade-off:
selectivity over total safety. If you have shell-wrapper jobs
that internally call dev-env python, they'll see the rebuild
mid-flight. Either tag those jobs (resubmit with `--branch
main`) or leave `provides_branches` unset on the affected env to
preserve pause-everything behavior.

### v0.5.46 — `--json` output for `vq admin status / update / clear-update-marker` (2026-05-17)

**Scripting affordance.** v0.5.20–v0.5.45 built up the admin verbs
with human-readable text formatters. The chat-orchestration
workflow (and any CI / automation around `vq admin update`) wants
to parse outcomes programmatically — grepping the text formatters
is fragile (column widths shift when an env is added, banner
appears/disappears depending on marker state, etc.). v0.5.46 adds
`--json` to all three admin verbs with stable schemas.

**New formatters** in `admin.py`:

* `format_admin_status_json(cfg)` — `{"marker": {...} | null, "envs": [...]}`.
  Per-env record flattens `EnvStatus` (name, git_dir, branch,
  current_sha, current_describe, is_dirty, error) and
  `AdminUpdateRecord` (last_updated_at, last_success, last_sha,
  last_tag, last_expected_tag, last_git_pull_rc,
  last_update_script_rc). Missing fields are explicit `null` —
  stable schema is more useful to scripts than a compact one.
* `format_update_result_json(result)` — `UpdateResult.asdict()` plus
  computed `success` and `tag_matches` (the dataclass properties,
  materialised so they're part of the wire schema).
* `format_update_all_results_json(results)` — `{"results": [...],
  "n_ok": N, "n_total": M, "failed_envs": [...],
  "batch_success": bool}`. Empty input gives a clean zero-batch
  result (`batch_success: true`).
* `_marker_to_json_block()` — used by status; returns `null` when
  no marker, `{...fields, "readable": true}` when parseable,
  `{"readable": false}` when present but corrupt. Lets consumers
  distinguish "no marker" from "marker exists but unparseable"
  without ambiguity.

**CLI surface.**

* `vq admin status [--json] [HOST | --all]` — single-host emits one
  JSON object. `--all` aggregates into a top-level dict keyed by
  host; per-host failures surface as `{"error": "..."}` so one bad
  host doesn't break parsing of the rest.
* `vq admin update [--json] [...]` — single-env returns
  `UpdateResult`-shaped object; `--all` returns the batch object;
  `--all-hosts` returns a host-keyed dict (same shape as status).
  Plumbs through SSH delegation: `vq admin update <env> HOST --json`
  forwards `--json` to the remote vq.
* `vq admin clear-update-marker [--json] [HOST]` — returns
  `{"cleared": bool, "marker": {...} | null, "readable": bool}`.
  `--json` implies `--yes` (no prompt in script mode — the
  absence of a TTY is itself signal that scripting is in play).

**Stable-schema commitment.** Every JSON output includes every
documented field with explicit `null` rather than dropping the
key. This matters for `jq` consumers writing
`.envs[] | .last_success` — they get `null` for unrecorded envs,
not an error. Pretty-printed (indent=2, sort_keys=True) so a
human reading the JSON output still gets a readable result.

**Tests** (+16 across 4 new classes in test_admin.py):

* `TestAdminStatusJsonFormatter` (4) — empty registry, env-record
  flattening, marker block present, marker block unreadable.
* `TestAdminStatusJsonCLI` (2) — CLI emits valid JSON; help
  mentions --json.
* `TestAdminUpdateJsonFormatter` (4) — single-result includes
  success+tag_matches, failure propagates, batch summary fields,
  empty input.
* `TestAdminUpdateJsonCLI` (2) — single-env CLI parses, --all CLI
  parses.
* `TestClearUpdateMarkerJsonCLI` (4) — no marker → cleared=False,
  --json clears without prompting, unreadable marker handled,
  help mentions --json.

998 passed / 4 skipped on macOS (+16 from v0.5.45).

**Deploy.** Daemon-side fix. `vq admin update vibeqc-queue
<host>` pulls and auto-restarts. Post-deploy verification:
`vq admin status --json <host> | jq .` should pretty-print
cleanly.

**Deferred to v0.6.x (remaining v0.6.0 design items):**

* `provides_branches` for surgical pause scoping — closed in
  v0.5.47 (single-env update_env path). update_all surgical
  scoping (union of envs' provides_branches with fallback to
  pause_all) deferred to v0.5.48.
* Bearer-token auth on admin verbs (parity with web write
  endpoints). Currently relies on local-shell access.
* `VQ_FORCE_NATIVE_DEPS_CHECK=1` env injection into the
  update_script — one-liner here, but needs vibe-qc-side honor
  in `scripts/update.sh`.

### v0.5.45 — daemon honors the admin-update-in-progress marker (2026-05-17)

**Closes the loop on v0.5.44.** v0.5.44 added the marker file and
made `vq admin update` write/clear/refuse-on-presence. But the
daemon's dispatch loop didn't know about it: if a `vq admin update`
left a marker behind (interrupted run, parent process killed), the
daemon kept dispatching new jobs into a possibly-half-installed
venv. The marker was paperwork; the daemon was still trusting.

**Fix.** A new `Daemon._poll_admin_update_marker()` runs at the
top of `_dispatch_pending` each tick. When the marker is on disk,
the function returns True and the dispatch loop early-returns —
running jobs continue (their process images are already mapped;
the venv mutation can't reach a live process), reconcile /
orphan / watchdog passes also continue. Only NEW dispatches are
gated.

```python
def _poll_admin_update_marker(self) -> bool:
    present = admin.admin_update_marker_exists()
    # ... transition-only logging ...
    return present
```

The check is a cheap `path.exists()` — same cost as the existing
drain-state read on the next line. Adds nothing measurable to
each dispatch tick.

**Transition-only logging.** A naïve "log when blocked" would
spam the daemon log once per tick (~1/sec at default
`poll_interval`). Instead, `_admin_update_marker_present` tracks
the previous tick's state; the daemon logs:

* "admin-update-in-progress marker detected — pausing new
  dispatches (envs=..., host=..., started=..., pid=...,
  vq_version=...); running jobs unaffected. Recover via
  `vq admin clear-update-marker`." — once, on the
  False → True transition.
* "admin-update-in-progress marker cleared — resuming new
  dispatches." — once, on the True → False transition.

Initial `_admin_update_marker_present = False` at `__init__` means
the *first* tick after a daemon start with a marker already
present logs the detection — exactly what an operator wants when
the daemon comes up after a crash that left a marker behind.

**Why this matters in practice.** v0.5.44 introduced the marker
as the "previous update was interrupted; venv may be inconsistent"
signal. Pre-v0.5.45, that signal only mattered when *someone ran
`vq admin update` again*. Now it matters every dispatch tick. The
specific scenario closed:

1. `vq admin update vibeqc-dev` is in progress. Marker is on disk.
2. Update fails (rc != 0 from update_script: e.g. a transient
   pip-install network error mid-pip-install leaves the venv
   half-installed; or the build OOM'd; or the user hit Ctrl-C).
3. v0.5.44's `update_env` finally block: resume_all() runs (queue
   unblocked) → marker stays (success=False, conditional clear
   skipped).
4. Pre-v0.5.45 daemon: queue is unblocked, dispatches resume into
   the broken venv. Jobs fail or run against mismatched modules.
5. v0.5.45 daemon: marker check fires → skip dispatch → log the
   reason → operator sees the log line and acts.

The pre-v0.5.45 mitigation was "the SUSPENDED state is sticky;
the operator notices via `vq queue`." But SUSPENDED only persists
if jobs were paused at update time; freshly-submitted PENDING
jobs after the failed update would dispatch normally. v0.5.45
closes that gap too.

**Already-running jobs.** Not affected. The marker means "the
venv on disk may be inconsistent," but a process whose modules
are already loaded keeps using its in-memory copies — POSIX file
semantics: open inode survives unlink/replace. Reconcile +
orphan + watchdog passes run unchanged. This is deliberate: we
don't want a transient marker to kill long-running jobs.

**Tests** (+6 in `tests/test_daemon.py::TestAdminUpdateMarkerEnforcement`):

* `test_no_marker_dispatches_normally` — sanity: absence of
  marker → normal dispatch.
* `test_marker_present_blocks_new_dispatch` — pending job stays
  PENDING across multiple iterate() calls when marker is on disk.
* `test_marker_clearing_resumes_dispatch` — clearing the marker
  unblocks pending dispatch within one tick.
* `test_running_jobs_continue_through_marker_appearance` — job
  that started before the marker appeared runs through to
  completion.
* `test_state_transition_logs_once_per_change` — exactly one
  "detected" log entry across N ticks with the marker present;
  exactly one "cleared" entry on transition.
* `test_malformed_marker_still_blocks_with_unreadable_log` —
  a marker file present but unparseable still blocks, with
  diagnostic text including "unreadable".

A new `isolated_daemon` fixture redirects `VQ_STATE_DIR` into
`tmp_path` so the marker writes / reads stay isolated from the
developer's real state root (the existing `daemon` fixture
doesn't monkey-patch state_root, so the marker would otherwise
collide with a live developer install).

982 passed / 4 skipped on macOS (+6 from v0.5.44).

**Deploy.** Same flow as v0.5.44: `vq admin update vibeqc-queue
<host>` pulls v0.5.45, the v0.5.43 self-update auto-restart
brings the daemon up on the new code. No manual restarts needed.
Post-deploy, `vq admin status <host>` should still show no
marker (the successful update cleared it).

### v0.5.44 — admin-update-in-progress marker file (2026-05-17)

**Closes the last v0.6.0 design gap on `vq admin update`.** Builds
on the v0.5.42/43 self-update cluster: the auto-restart fix made
the daemon pick up new code automatically, but said nothing about
what happens if the *update itself* is interrupted (Ctrl-C, ssh
dropped, SIGKILL, kernel OOM mid-build). v0.5.20–v0.5.43 left the
queue in a known-clean state via `try/finally`-bracketed
pause/resume — but the *env* could be half-installed, and the next
`vq admin update` would happily run against it.

**Fix.** Write `<state_root>/admin-update-in-progress` immediately
after pause_all (before any git pull / update_script work) and
clear it in the same finally block iff the update succeeded. A
failed or interrupted update leaves the marker on disk; subsequent
`vq admin update` invocations check for the marker and refuse to
proceed.

**Marker contents** (JSON, atomic tmpfile-rename write):

```json
{
  "envs":        ["vibeqc-dev"],
  "host":        "compute-d",
  "started_at":  "2026-05-17T05:42:01+00:00",
  "pid":         93346,
  "vq_version":  "0.5.44"
}
```

`envs` is a list so `vq admin update --all` records the full
batch scope. `vq_version` lets a future vq reading an old marker
diagnose schema drift.

**Recovery flow (operator-facing).**

```sh
# 1. See the marker.
vq admin status HOST
# →  !! admin-update-in-progress marker present !!
#       envs:        vibeqc-dev
#       host:        compute-d
#       started_at:  2026-05-17T05:42:01+00:00
#       pid:         93346
#       vq_version:  0.5.44
#       A prior admin update did not complete cleanly...

# 2. Verify the env (re-run the build, smoke tests, git state).

# 3a. Acknowledge and clear:
vq admin clear-update-marker HOST

# 3b. Or one-shot recovery (overwrite + re-run):
vq admin update vibeqc-dev HOST --force
```

**API surface.**

* `admin.AdminUpdateMarker` dataclass + `admin_update_marker_path()` /
  `admin_update_marker_exists()` / `read_admin_update_marker()` /
  `write_admin_update_marker()` / `clear_admin_update_marker()`.
* `admin._guard_admin_update_marker(force=False)` — raises
  `AdminError` if marker present and not force; used by both
  `update_env` and `update_all`.
* `update_env(env, cfg, *, host, expected_tag=None, restart_daemon=True, force=False)`
  — new `force` kwarg plumbs through CLI's `--force`.
* `update_all(cfg, *, host, restart_daemon=True, force=False)` —
  marker records the full env list; clears iff ALL envs succeed
  (partial batch keeps marker for operator inspection).

**CLI surface.**

* `vq admin update [ENV] [HOST] [--all] [--all-hosts] [--tag TAG] [--no-restart-daemon] [--force]`
  — `--force` added. Plumbs to `update_env` / `update_all` and to
  the remote-delegation argv.
* `vq admin clear-update-marker [HOST] [-y/--yes]` — new verb.
  Shows the marker contents, prompts for confirmation (unless
  `-y`), removes the file. Quiet no-op when no marker present.
  Same local-vs-SSH-delegation shape as `vq admin status`.
* `vq admin status` — prepends a multi-line banner ("!!
  admin-update-in-progress marker present !!" plus envs / host /
  started_at / pid / vq_version) when a marker is on disk. ASCII
  only per project style.

**Edge cases handled** (each has a dedicated test):

| Case                                       | Behavior                                                |
|--------------------------------------------|---------------------------------------------------------|
| No marker present                          | update proceeds normally                                |
| Marker present, --force=False              | update_env raises AdminError with recovery recipe       |
| Marker present, --force=True               | overwrites marker, proceeds; clears on success          |
| Marker present but malformed JSON          | guard still fires (cheap stat check); diagnostic says "(unreadable)" |
| Update succeeds end-to-end                 | marker cleared in finally                               |
| git pull fails                             | marker stays (success=False)                            |
| update_script fails                        | marker stays                                            |
| tag mismatch with --tag                    | marker stays                                            |
| `--all` partial success                    | marker stays, records full env list                     |
| `--all` full success                       | marker cleared                                          |
| `clear-update-marker` with no marker       | quiet no-op, exit 0                                     |
| `clear-update-marker` prompt aborted (n)   | marker untouched, exit non-zero                         |

**Why "marker stays on failure" instead of "marker only on crash".**
The naive read is "marker tracks crashes only; a clean rc=1 is
caught by the result, no marker needed." But the failure modes
that motivate the marker — half-installed venv after `pip install
-e .` died mid-write — present as rc != 0 outcomes too. Treating
all non-success identically is conservative and avoids the bug
where a soft failure leaves the venv inconsistent without the
loud signal. Cost: one extra `vq admin clear-update-marker` call
after every failed update — acceptable for the "always know"
property.

**Tests** (+26 in test_admin.py):

* `TestAdminUpdateMarker` (7) — read/write/clear roundtrip,
  malformed JSON handling, schema drift, idempotent clear.
* `TestAdminUpdateMarkerGuard` (8) — write-after-pause +
  clear-on-success in update_env, marker-stays on git pull /
  update_script / tag mismatch failure, guard rejects without
  force, guard fires on malformed marker, force overwrites,
  update_all records full env list, update_all clears iff all
  succeed.
* `TestFormatMarkerBanner` (3) — no banner without marker,
  banner shows details when present, banner says "unreadable" on
  malformed marker.
* `TestClearUpdateMarkerCLI` (5) — quiet no-op, --yes skips
  prompt, prompt-y confirms and clears, prompt-n aborts and keeps
  marker, help text mentions the recovery flow.
* `TestAdminUpdateForceFlagCLI` (3) — --force in help, force lets
  update proceed past existing marker, no-force rejects with
  recovery recipe text.

976 passed / 4 skipped on macOS (+26 from v0.5.43).

**Deploy.** First v0.5.x version where the v0.5.43 auto-restart
fires end-to-end on a routine `vq admin update vibeqc-queue
<host>` call — that path now writes + clears the marker, and the
daemon-restart-after-success step still triggers. Each host:

```sh
vq admin update vibeqc-queue <host>
# Expect:
#   == admin update vibeqc-queue ==
#      ...
#   ==> vq self-update detected — restarting vq-daemon
#      systemctl --user restart vq-daemon ... done (PID OLD -> NEW)
#   == OK ==
```

Verify post-deploy: `vq admin status <host>` should show no
marker banner (the successful run cleared it).

**Deferred to v0.6.x:**

* Daemon-side enforcement of the marker — closed in v0.5.45.
* `--json` output for admin verbs — closed in v0.5.46.
* Bearer-token auth on admin verbs (parity with web write
  endpoints). Currently relies on local-shell access.

### v0.5.43 — fix v0.5.42 self-update detection through the venv python symlink (2026-05-17)

**Bug fix.** v0.5.42 shipped the auto-restart-vq-daemon-on-self-update
feature but it never fired in production. Root cause discovered on the
v0.5.42 deploy attempt to compute-a (the chat that wrote v0.5.42 handed off
before deploying; the deploy chat caught the bug on the first
``vq admin update vibeqc-queue compute-a`` call — no banner, no PID
transition, daemon stayed on the old code).

``admin._detect_vq_self_update`` compared
``Path(prog.python).resolve().parent`` against the daemon ExecStart's
parent dir. In a real venv, ``bin/python`` is a **symlink** to the
system interpreter (``/usr/bin/python3.14`` on compute-a / compute-d).
``Path.resolve()`` dereferences the symlink, so ``.parent`` returns
``/usr/bin`` instead of the venv's ``bin/``. The daemon's vq
ExecStart is a regular file, so its ``.resolve().parent`` correctly
returned the venv's bin. The two bin dirs never matched → detection
always reported "different venv" → restart never fired.

Same bug applied on the systemctl-unavailable fallback path
(``sys.executable`` IS the venv python symlink). On macOS dev hosts,
fallback detection would also silently say "not a self-update".

**Fix.** ``.parent`` before ``.resolve()``: keep the venv bin dir as
the comparison target, then resolve any directory-level symlinks.

```python
# v0.5.42 (wrong — dereferences the python symlink):
venv_bin = Path(prog.python).resolve().parent

# v0.5.43 (right — stays in the venv's bin dir):
venv_bin = Path(prog.python).parent.resolve()
```

Applied symmetrically to ``daemon_bin`` in the systemctl path and
``sys_bin`` in the fallback path. The existing docstring already
described the intended behaviour as ``Path(prog.python).parent``;
the code had drifted from the doc.

**Why the v0.5.42 tests missed it.** Every fixture in
``TestSelfUpdateRestart`` wrote ``python_path.write_text("")`` to
create a regular file at the venv python location, never a symlink.
The bug surfaces only when the python entry is a real symlink to a
file outside the venv — i.e. every actual venv.

**Tests** (+2 in ``TestSelfUpdateRestart``):

* ``test_detect_treats_venv_python_symlink_as_belonging_to_venv``:
  creates a venv-shaped tmp_path where ``bin/python`` is a real
  symlink to an out-of-venv interpreter, asserts the systemctl-OK
  detection returns ``is_self_update=True``.
* ``test_detect_fallback_handles_venv_python_symlink``: same shape
  but exercises the fallback path (``_systemctl_user_available`` →
  False, ``sys.executable`` pointed at the symlink). Asserts
  fallback also recognises self-update.

**Deploy.** Daemon-side. The v0.5.42 daemon's broken detection means
the v0.5.42 → v0.5.43 bump itself still needs a one-time manual
``systemctl --user restart vq-daemon`` per host (same as the v0.5.42
bootstrap step). After v0.5.43 is the running daemon, future
``vq admin update vibeqc-queue`` calls finally trigger the auto-restart
the v0.5.42 entry promised.

```sh
ssh HOST 'cd ~/gitlab/vibeqc-queue && git pull && cd vibe-queue && \
   .venv/bin/pip install -e . && systemctl --user restart vq-daemon'
ssh HOST '~/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq --version'  # → 0.5.43
vq admin update vibeqc-queue HOST  # banner should now fire
```

### v0.5.42 — `vq admin update` auto-restarts vq-daemon on a self-update (2026-05-16)

**Closes the stale-code gap surfaced by 2026-05-16's compute-d
incident.** vq is editable-installed (`pip install -e .`); on-disk
changes only take effect once the running daemon process restarts.
v0.5.40's parallelism cap landed on disk via `vq admin update
vibeqc-queue`, but the compute-d daemon kept running v0.5.39 in
memory for ~30 minutes until the operator noticed and restarted manually.
Any admin operation during that window would have used the
un-capped logic — the exact bug the patch was trying to fix. See
operations.md § "Daemon running stale code after `pip install -e .`"
for the full incident write-up.

**Fix.** At the end of a successful `vq admin update <env>`, detect
whether `<env>` is the venv from which `vq-daemon` was launched. If
yes, `systemctl --user restart vq-daemon` automatically; otherwise
leave the daemon alone. Concrete behavior:

```text
$ vq admin update vibeqc-queue
== admin update vibeqc-queue ==
   ...

   resumed:       resumed 0 jobs (1 not SUSPENDED)

==> vq self-update detected — restarting vq-daemon
   systemctl --user restart vq-daemon ... done (PID 1234 -> 5678)

== OK ==
```

**Detection signal.** Primary: `systemctl --user show vq-daemon -p
ExecStart --value` parses the `path=` field out of systemd's raw
ExecStart value and compares its parent dir to the env's venv bin
(`Path(prog.python).parent`). Fallback when systemctl is
unreachable (macOS, zombie user-systemd): compare `sys.executable`'s
bin dir to the env's venv bin — if they match, the running
`vq admin update` command itself is inside this venv, so a daemon
launched from the same venv is the likely-correct verdict. See
`admin._detect_vq_self_update`.

**Edge cases handled** (each has a dedicated test in
`TestSelfUpdateRestart`):

| Case                                           | Behavior                                         |
|------------------------------------------------|--------------------------------------------------|
| Non-vq env (vibeqc-dev, …)                     | Daemon untouched, quiet                          |
| vq env, daemon running                         | Restart, report PID transition                   |
| vq env, daemon stopped                         | Skip with note; next `start` picks up new code  |
| vq env, systemctl unreachable                  | Surface recovery recipe + exit non-zero          |
| Update itself failed (rc != 0)                 | Never restart (would land on a half-built env)   |
| `--no-restart-daemon` flag passed              | Skip detection entirely, never restart           |
| macOS (no systemctl --user)                    | Treated as "systemctl unavailable", fallback     |

**Opt-out flag.** `vq admin update <env> --no-restart-daemon`
forwards through the CLI to `update_env(..., restart_daemon=False)`
and to remote vq invocations via `_delegate_to_remote`. Mid-debug
sessions where you want to keep the running daemon's state across
the update.

**Failure surface.** When detection believes it IS a self-update
but the restart fails (systemctl unreachable / timeout / non-zero
rc), `UpdateResult.success` flips to False so the CLI exits
non-zero. The user sees the recovery recipe pointer rather than
silently continuing on stale code. This is the inverse of the
2026-05-16 silent failure: now the bad state is loud.

**Tests** (+18 new in `test_admin.py::TestSelfUpdateRestart`):

* `_parse_execstart_path` — handles systemd's raw ExecStart format
  (`{ path=...; argv[]=...; ... }`) and the empty/malformed case.
* `_detect_vq_self_update` — five paths (systemctl unavailable +
  match, systemctl unavailable + mismatch, ExecStart in env's venv,
  ExecStart in different venv, unit missing).
* `_restart_vq_daemon` — happy PID transition, rc!=0 with
  recovery-recipe pointer, timeout.
* `_maybe_restart_daemon` — six end-to-end paths (flag disabled,
  update failed, not a self-update, happy restart, systemctl
  unreachable on a self-update, daemon not running on a
  self-update).
* CLI: `--no-restart-daemon` plumbs through to `update_env`;
  default is `restart_daemon=True`.
* Formatter: self-update success banner + failed-restart reason
  line.

**Deploy.** This *is* the deploy fix — once v0.5.42 is the running
daemon version on each host, future `vq admin update vibeqc-queue`
calls handle their own daemon restart. The one-time bootstrap is
the same as for any vq update:

```sh
git pull && pip install -e . && systemctl --user restart vq-daemon
```

After that, you're done with manual restarts.

### v0.5.41 — tighten the v0.5.40 cap + idle CPU/IO priority for the update_script (2026-05-16)

**Responsiveness fix.** v0.5.40 prevented the global-OOM hang on
compute-d (commit `285e9a9`) but the user reported the box still went
"quite unresponsive and then it dies" during a re-run later the same
day. Forensics:

* v0.5.40 budgeted **10 GB / worker** with no hard cap. On compute-d
  (125 GB, 32 threads) that's 12 cc1plus workers, peaking at ~100 GB
  resident.
* No OOM — but the kernel was thrashing the page cache hard enough
  to freeze the interactive shell for 10+ seconds at a time, and
  builds were back-to-back enough that the box looked dead from the
  outside.
* The OOM was the headline failure; this is the long-tail failure
  the cap was supposed to also prevent.

**Fix #1 — tighter formula.** ``_safe_build_parallelism`` now uses:

```
cap = min(nproc, max(2, mem_mb // 15000), 6)
```

Two changes vs v0.5.40:

1. **15 GB / worker** (up from 10): peak cc1plus on the heaviest TUs
   (libint integral headers, ``periodic_*.cpp``, ``gradient.cpp``)
   was closer to 8–10 GB than the v0.5.40 doc assumed. 15 GB gives a
   clean 50% margin.
2. **Hard cap of 6 workers** even on monster machines: above 6,
   ninja's bottleneck stops being CPU and becomes link-step
   serialization + filesystem buffer cache pressure. Extra
   parallelism just thrashes — not a build-time win, and definitely
   a responsiveness loss.

Concrete fleet numbers under v0.5.41:

| Host    | nproc | RAM     | cap | peak    | host headroom |
|---------|------:|--------:|----:|--------:|--------------:|
| compute-d |    32 | 125 GB  |  6  | ~60 GB  | ~65 GB        |
| compute-a    |    16 |  62 GB  |  4  | ~40 GB  | ~22 GB        |
| macbook |    10 |  32 GB  |  2  | ~20 GB  | ~12 GB        |

**Fix #2 — argv-level niceness prefix.** A new
``admin._build_niceness_prefix()`` returns ``["nice", "-n", "19",
"ionice", "-c", "3"]`` on Linux hosts where both binaries are on
PATH. ``_run_update_script`` prepends this to the bash invocation so
the build runs at:

* **CPU**: lowest POSIX priority (``nice -n 19``) — preempted by any
  foreground process the user types into a shell.
* **IO**: Linux idle class (``ionice -c 3``) — disk IO only happens
  when no other process wants it.

The cap stays as the **hard correctness guarantee** (memory cannot
exceed); niceness is the **soft latency guarantee** (responsiveness
cannot collapse).

**Partial-degrade tolerance.** ``shutil.which`` gates each binary —
if ``ionice`` is missing (minimal Linux images), the prefix is just
``nice -n 19``. If both are missing, the prefix is empty and argv is
unchanged. macOS skips the whole thing via the ``/proc/meminfo``
gate (no Linux → no Linux-specific tuning).

**Env-override precedence — unchanged.** v0.5.40's rule still holds:
if the caller's env already sets ``CMAKE_BUILD_PARALLEL_LEVEL``, vq
does not override.

**Tests** (+5 new in ``test_admin.py``):
* ``test_safe_build_parallelism_formula`` updated for the new
  formula. Now covers 5 regimes — RAM-bound, nproc-bound, floor
  minimum, hard cap on a 256 GB machine, plus the compute-d baseline.
* ``test_build_niceness_prefix_on_linux_with_both_tools`` — full
  prefix when both binaries are on PATH.
* ``test_build_niceness_prefix_on_linux_missing_ionice`` — partial
  degrade keeps just ``nice``.
* ``test_build_niceness_prefix_off_on_macos`` — /proc/meminfo gate
  short-circuits the whole helper.
* ``test_update_script_prepends_niceness_argv`` — end-to-end argv
  shape check including the prefix.
* ``test_update_script_no_niceness_argv_on_macos`` — when prefix is
  empty, argv starts with ``bash``.

**Deploy.** Daemon-side. Each host: ``git pull && pip install -e .
&& systemctl --user restart vq-daemon``. After deploy, the next
``vq admin update <env>`` will use the new cap automatically.

### v0.5.40 — `vq admin update` caps ninja parallelism (`CMAKE_BUILD_PARALLEL_LEVEL` env) (2026-05-16)

**Durability fix.** Direct follow-up to the compute-d global-OOM hang
on 2026-05-16 (see commit history around `c537bb8` /
`scripts/update.sh` interactive build retries that pushed ~30
concurrent cc1plus past 125 GB RAM and global-OOM'd the host).

vq's per-job cgroup MemoryMax already protects dispatched jobs; the
gap was the **daemon-side `update_script` invocation**, which runs
outside any per-job cgroup and inherits the caller's env. Default
ninja parallelism is `nproc` — 32 on compute-d — and each cc1plus on
vibe-qc's template-heavy translation units (``periodic_*.cpp``,
``guess.cpp``, ``gradient.cpp``) peaks at 5-8 GB. 32 × ~6 GB > 125 GB
physical → global OOM → host hang.

**Fix.** ``admin._run_update_script`` now injects
``CMAKE_BUILD_PARALLEL_LEVEL`` into the subprocess env using a
RAM-aware heuristic:

```
cap = min(nproc, max(2, mem_mb // 10000))
```

10 GB per worker (not 8) leaves a ~20% safety margin against ninja's
peak overshoot plus any tenant pressure on the host. Concrete fleet
numbers:

| Host    | nproc | RAM     | cap | peak     | host headroom |
|---------|------:|--------:|----:|---------:|--------------:|
| compute-d |    32 | 125 GB  |  12 | ~120 GB  | ~5 GB         |
| compute-a    |    16 |  62 GB  |   6 |  ~60 GB  | ~2 GB         |

**Env-override precedence.** If the caller's environment already
sets ``CMAKE_BUILD_PARALLEL_LEVEL``, vq does NOT override —
explicit user intent wins. Useful when an operator deliberately
wants a different cap (e.g. testing a build under tighter or
looser parallelism).

**macOS / hosts without /proc/meminfo.** ``_safe_build_parallelism``
returns ``None``, and vq leaves the env untouched so the system /
user default wins.

**Helper exposed.** ``admin._safe_build_parallelism()`` is the
single source of truth for the formula — tests assert it directly
across 4 regimes (RAM-bound, nproc-bound, floor minimum, no-meminfo).

**Tests** (+5 in ``test_admin.py``):
* ``test_safe_build_parallelism_formula`` — checks the 4 regimes.
* ``test_safe_build_parallelism_no_proc_meminfo`` — macOS dev path.
* ``test_update_script_injects_parallelism_cap_when_unset`` — env
  injection happens when CMAKE_BUILD_PARALLEL_LEVEL is unset.
* ``test_update_script_honors_pre_existing_parallelism_env`` —
  user override is preserved.
* ``test_update_script_no_cap_when_meminfo_unreadable`` — on
  macOS the env is left clean.
923 passed / 4 skipped on macOS (+5 from v0.5.39).

**Deploy.** Daemon-side fix. Each host needs the usual ``git pull
&& pip install -e . && systemctl --user restart vq-daemon``.

**Belt-and-braces follow-up (vibe-qc side, separate chat).** Patch
``scripts/update.sh`` to default to the same formula so the cap
applies even when the script is run interactively outside vq. Until
that lands, interactive runs still need ``CMAKE_BUILD_PARALLEL_LEVEL=N``
prefix to be safe.

### v0.5.39 — `update_script` accepts args via shlex; vibe-qc consolidation drops `update-dev.sh` (2026-05-16)

Two coupled changes — one in vq, one in vibe-qc, shipped together
because they're the only sensible way to retire a redundant shell
wrapper.

**vq side.** ``admin._run_update_script`` now :func:`shlex.split`'s
the ``[programs.X].update_script`` field, so a value like
``"scripts/update.sh --dev"`` invokes ``bash <git_dir>/scripts/update.sh
--dev``. The first token is still the script path (resolved relative
to ``git_dir``); everything after is forwarded as argv to bash. The
legacy single-path form (``"scripts/update-dev.sh"``) keeps working
without change — shlex.split of a one-token string is just that
token. Whitespace-only / empty values now record a clear work_error
instead of silently shlex'ing to ``[]``.

**vibe-qc side.** the operator's call: ``scripts/update-dev.sh`` was a
4-line wrapper around ``scripts/update.sh --dev``. Two scripts to
maintain for the same effect; the wrapper deleted. Refs scrubbed in
``scripts/update.sh`` header, ``docs/installation.md``,
``docs/updating.md``. ``CHANGELOG.md``'s historical line stays
(it's a date-stamped record of past usage). The vq ``update_script``
fields in both compute-d's and compute-a's ``~/.config/vq/config.toml``
flipped to ``"scripts/update.sh --dev"`` for the vibeqc-dev entries;
backups saved as ``config.toml.bak-pre-0.5.39``.

**Why ship both at once.** The doc/config consolidation depends on
vq supporting args in ``update_script`` — without v0.5.39's
shlex-split, the deletion would break ``vq admin update vibeqc-dev``
because the new ``"scripts/update.sh --dev"`` value would be
treated as one literal path with a space in it and fail to find
the file. Done in lockstep.

**Tests** (+4 in ``test_admin.py::TestUpdateEnvExecution``):
* ``update_script = "scripts/update.sh --dev"`` → bash gets
  ``["bash", "<abs path>", "--dev"]`` argv.
* Multi-arg form forwards each token separately.
* Legacy single-path form still produces 2-element bash argv.
* Whitespace-only ``update_script`` records "empty" work_error
  instead of silently no-op'ing.
918 passed / 4 skipped on macOS (+4 from v0.5.38).

**Deploy.** Daemon-side fix — each host needs ``git pull && pip
install -e . && systemctl --user restart vq-daemon``. Doing it via
``vq admin update --all --all-hosts`` would chicken-and-egg (uses
the old update_script value through the v0.5.38 daemon), so deploy
through the standard ssh route.

### v0.5.38 — watchdog: cgroup-v2 cpu.stat / memory.current as primary sampler (2026-05-16)

**Bug fix.** the operator's report 2026-05-16: a vibe-qc rebuild via
``pip install -e .`` got STARVED-killed at the 5-minute starve
window even though 32 cc1plus instances were saturating the box.

**Root cause** (per the operator's trace; matches the watchdog code's
``read_cputime_seconds_pgid`` design from v0.5.12): the pgid-walk
CPU-activity sampler iterates ``_pgid_pids(pgid)`` to find PIDs in
the job's dispatch pgroup, then sums ``/proc/<pid>/stat`` across
them. ninja's build orchestration calls ``setsid`` / ``PR_SET_PGID``
to put cc1plus instances in their OWN session/pgid, escaping the
parent pgroup. From the watchdog's vantage the original pgid sees
~0% CPU for the whole multi-minute build → STARVED kill. The
behaviour is silent in stdout because the user wrapped pip output
in a ``| tail -3`` pipe that buffers until pip exits, so they
couldn't see "is the build alive?" via tail-of-output either.

**Fix.** When ``cgroup.available()`` is True (every modern vq host:
the daemon already wraps each dispatch in a transient
``vq-job-<id>.scope`` cgroup-v2 scope for CPU/memory enforcement),
the watchdog prefers cgroup counters:

* ``cgroup_path_for_pid(pid)`` parses ``/proc/<pid>/cgroup``
  (single-line ``0::<path>``) and returns the absolute fs path.
* ``read_cpu_usage_seconds(cgroup_path)`` reads ``cpu.stat``'s
  ``usage_usec`` field, converts to seconds — cgroup-wide,
  immune to pgid/session escapes.
* ``read_memory_current_mb(cgroup_path)`` reads ``memory.current``,
  returns MB. Bonus: avoids the shared-page double-count problem
  of the pid-sum approach.

The pgid-walk readers remain as a fallback for macOS dev hosts and
Linux hosts without cgroup-v2 delegation. Partial-data path
(cgroup has CPU but not memory, or vice versa) fills the remaining
field from the pgid walk — defensive against hosts with selective
controller delegation.

**Doc warning.** ``vq submit --help`` gains a "Watchdog notes"
block describing the pgid-walk fallback failure mode and the two
workarounds (drop top-level stdout-buffering pipes; target
cgroup-v2 hosts).

**Tests** (+15 in test_cgroup.py + test_watchdog.py):
* ``TestCgroupPathForPid``: typical v2 line, missing entry, v1-only
  format returns None, root cgroup edge case.
* ``TestReadCpuUsageSeconds``: typical parse, missing file, malformed
  value, "aggregates descendants" intent assertion.
* ``TestReadMemoryCurrentMb``: typical parse, missing, malformed,
  floor-division to whole MB.
* ``TestCgroupPreferredSampling``: cgroup-available → cgroup readers
  used (verified by asserting that pgid readers' kill-trigger values
  are NOT consulted); cgroup-None → pgid-walk fallback engages;
  partial-data path fills the missing field from pgid-walk.

914 passed / 4 skipped on macOS (+15 from v0.5.37).

**Deploy note.** The fix is daemon-side (watchdog runs in the
daemon process). Each compute host needs:

```
git pull && pip install -e . --quiet && systemctl --user restart vq-daemon
```

The orphan-reattach path in the daemon's startup recovery means
running jobs survive the restart cleanly (we proved this across
v0.5.32 / v0.5.35 deploys).

### v0.5.37 — `vq admin update --all-hosts` (fleet-wide env refresh) (2026-05-15)

Predicted-and-shipped right after v0.5.36: the new `vq admin status
--all` made it obvious that compute-a's vibeqc-dev was 7 commits behind
compute-d's. Refreshing both via one command is the natural follow-on.

**Naming.** ``--all`` was already taken for "all envs on one host"
(v0.5.28). Added ``--all-hosts`` for the cross-host dimension. The
two are composable:

```
vq admin update ENV                 # one env, default_host
vq admin update ENV HOST            # one env, one host
vq admin update --all               # all envs, default_host
vq admin update --all HOST          # all envs, one host
vq admin update ENV --all-hosts     # one env, every host (NEW)
vq admin update --all --all-hosts   # all envs, every host (NEW)
```

**Behaviour.** Sequential per-host loop. Each host pauses its OWN
jobs, runs git pull + tag-verify + update_script, resumes — the pause
window is bounded to that host's update. No cross-host interactions,
no shared lock, no global pause. If you have many hosts and want
faster wall-clock, a future v0.5.x could parallelize across hosts;
sequential is correct and simple for v0.5.37.

**Tag composes with --all-hosts.**
``vq admin update vibeqc-release --all-hosts --tag v0.8.0`` verifies
the same tag landed on every host. This is the right shape for
release-day fleet sweeps. (``--all`` still forbids ``--tag`` — different
envs track different tags; that constraint is unchanged.)

**Failure model.** Unlike the v0.5.36 read commands (where errors
render inline and exit 0), writes need failure visibility. One host
failing does NOT abort the rest of the loop — the healthy hosts
still update — but the batch exit code is non-zero with a summary
``admin update --all-hosts: N host(s) failed (<hostnames>)``. Scripts
calling this need to notice and react.

**Mutual exclusions.**
* ``--all-hosts`` + positional ``HOST`` → ``UsageError`` ("contradictory").
* ``--all --all-hosts`` + ANY positional → ``UsageError`` (every-env
  every-host needs no further selection).

**Implementation.** ~70 LoC, reuses the v0.5.36
``cli._aggregate_per_host`` helper. The per-host closure captures
``failures: list[str]`` so it can record which hosts failed while
the helper handles output stacking + error rendering.

**Tests** (+7, in `test_cli.py::TestAdminUpdateAllHosts`): one-env
across hosts, all-envs across hosts, --tag forwarding, positional-
HOST rejection, --all combo rejection, one-host-failure-doesn't-
abort-others + exits non-zero, help text. 899 passed / 4 skipped on
macOS (+7 from v0.5.36).

### v0.5.36 — cross-host aggregation: `--all` on queue / programs / admin status (2026-05-15)

Predicted-and-shipped item: once compute-a came online as a second compute
host (2026-05-15), typing `vq queue compute-d; vq queue compute-a` for routine
fleet checks felt obviously redundant within hours. Adds a `--all`
flag to the three "show me state" verbs.

**Behaviour.** Walks `cfg.hosts` in sorted alpha order, calls the
existing per-host implementation for each, stacks the outputs under
``==== <host> ====`` banners. Per-host failures (ssh timeout, host
down, config error) are caught and rendered inline as
``(error querying <host>: ...)`` — one bad host never hides the rest
of the fleet.

```
$ vq queue --all
==== compute-a ====
ID            STATE     CPUS  SUBMITTED (UTC)      COMMAND
(no jobs)

==== compute-d ====
ID            NAME           STATE    CPUS  SUBMITTED (UTC)      COMMAND
9922b4f16aba                 running  16    2026-05-15 11:10:30  ...
```

**Design choice: stacked, not merged.** The existing per-host renderer
already does column-conditional logic (NAME column only when at least
one spec has a name; PRI only when non-default priority appears).
Stacking preserves that per-host. A single merged table with HOST as
a column would have required either a shared schema (--json everywhere,
new merge code) OR collapsing the conditional columns (lose the
zero-noise default). Stacked also reads cleanly when one host has 20
jobs and the other has 0 — no padding distortion. Re-evaluate when 3+
hosts become normal.

**Flags forwarded into each per-host call:**
* `vq queue --all -s STATE / --active / --show-archived` — each
  delegated invocation includes the filter, so filtering happens on
  the host with the specs (no full listing streamed over SSH only to
  drop most).
* `vq programs --all --json` — current main emits a single top-level
  JSON object keyed by host, matching the other fleet monitor commands.
  Text `vq programs --all` remains the bannered human view.
* `vq programs --all --require NAME` — current main exits non-zero when
  any configured host lacks `NAME` or reports it as not `OK`, giving
  fleet managers a pass/fail gate for managed tools such as
  `vibeview-dev`.
* `vq programs --all --require-any A,B` — current main accepts at least
  one OK program from a comma-separated alias group on every host. This
  is for temporary managed-program renames, not a substitute for
  converging the fleet on one registry name.
* `vq programs --all --require-clean NAME` — current main exits non-zero
  when a venv program's git checkout is dirty or the dirty state cannot
  be read. Pair with `--require NAME` before trusting release-paper or
  docs artifact jobs on managed checkouts.
* `vq programs --all --require-branch NAME=BRANCH` — current main exits
  non-zero when a venv program's actual git branch does not match the
  expected branch. Pair with `--require-clean` for update preflights.
* `vq programs --all --require-sha NAME=SHA` — current main exits
  non-zero when a venv program's current git SHA does not match the
  requested full or short SHA prefix.
* `vq programs --all --require-version NAME=VERSION` — current main
  exits non-zero when a program's reported `import_version` does not
  exactly match the expected runtime version.
* `vq admin status --all` — no extra flags forwarded today.

**`--all` is mutually exclusive with positional `HOST`**: passing
both is a contradiction (which host did you mean?), errored at the
CLI boundary with a clear message rather than silent precedence.

**Implementation.** One shared helper ``cli._aggregate_per_host(cfg,
per_host_fn)`` owns the iteration + banner + error-catching shape.
Each command supplies a small ``_query_one(host)`` closure that does
its existing local-or-SSH-delegate work. ~80 LoC for the helper +
three closures.

**Empty config** (no `[hosts.X]` blocks) returns
`(no hosts configured — add [hosts.X] sections to ...)` rather than
silent empty output, mirroring the `(no jobs)` pattern elsewhere.

**Tests** (+11, in `test_cli.py::TestAllHostsAggregation`): per-host
banner sorting, positional-HOST rejection, one-failing-host doesn't
break the others, state filter forwarded, --json forwarded, empty-
config message, help text mentions --all on all three verbs. 892
passed / 4 skipped on macOS (+11 from v0.5.35).

### v0.5.35 — terminal-state webhook notifications (Slack / Discord / Mattermost) (2026-05-15)

Fourth v0.7 item closed. When a job hits a terminal state — COMPLETED,
FAILED, KILLED, INTERRUPTED, OOM_KILLED, STARVED, TIME_EXCEEDED,
ABORTED_BY_QUEUE — the daemon POSTs a small JSON payload to a
user-configured URL. Designed to plug into Slack / Discord /
Mattermost incoming webhooks without per-platform integration code.

**Config.** New `[notifications]` section in
``~/.config/vq/config.toml`` (daemon-side):

```toml
[notifications]
webhook_url = "https://hooks.slack.com/services/T.../B.../X..."
```

Absent section = disabled (default). Empty / missing URL = same.
``extra="forbid"`` on the section so a typo (`webhook_urls = ...`)
fails loudly at load time instead of silently leaving notifications
disabled.

**Payload.** Dual-key human summary + structured ``job`` block:

```json
{
  "text":    "vq: job mgo-pbe-abc123def456 finished as completed (rc=0) on compute-d",
  "content": "<same>",
  "job": {
    "id":            "abc123def456",
    "name":          "mgo-pbe",
    "state":         "completed",
    "exit_code":     0,
    "submitted_at":  "2026-05-15T10:00:00+00:00",
    "started_at":    "2026-05-15T10:00:05+00:00",
    "finished_at":   "2026-05-15T11:00:00+00:00",
    "command":       "python run.py",
    "host":          "compute-d"
  }
}
```

Slack reads ``text``, Discord reads ``content``, Mattermost reads
``text`` (Slack-compatible). Each platform ignores the keys it
doesn't recognise — one URL works against any of them. Custom HTTP
receivers (your own endpoint / a relay) consume the ``job`` block.

**Microsoft Teams is NOT supported by this generic POST** — Teams
expects an Adaptive Card / MessageCard shape rather than plain
``text``/``content``. A Teams integration would need its own
payload format; not in scope for v0.5.35.

The summary string uses the v0.5.34 ``dest_dirname`` (``<name>-<jobid>``
when ``job_name`` is set, else ``<jobid>``) so one consistent label
appears in queue / status / fetch / archive / notification.

**Where it fires.** Three call sites in ``daemon.py``, all of which
cover the ground:

* ``Daemon._record_finish`` — the "process exited" hook. Both the
  natural-COMPLETED/FAILED branch AND the already-terminal-reap
  branch (where the watchdog or ``vq kill`` had stamped the state
  earlier) fire the notification. This means OOM_KILLED / STARVED /
  TIME_EXCEEDED / KILLED-while-running all notify uniformly when
  the process is finally reaped.
* ``Daemon._record_orphan_finish`` — the daemon-restart orphan
  reaper. A job that exited during daemon downtime gets one
  notification on the next daemon startup.
* ``Daemon._mark_aborted_by_queue`` — the queue-side terminal
  sink for orphans we can't recover (pgid gone, no exit marker).

**Failure model.** Fire-and-forget on a daemon thread.
``send_terminal_notification`` returns immediately so the daemon's
tick loop is never blocked on HTTP. The actual POST has a 5-second
timeout; URL errors, unexpected exceptions, and HTTP >= 400
responses are logged at WARNING level and dropped. No retry. No
buffering. No persistence. If you need delivery guarantees, point
the webhook at something that owns retry policy.

**Not covered in v0.5.35.**
* ``vq kill`` on a PENDING job: ``kill.py`` sets KILLED directly
  without daemon involvement (the job never ran). Rare enough that
  v1 accepts the gap.
* Per-state filter (e.g. notify only on FAILED + watchdog kills).
  Coming later if anyone asks; v1 fires for every terminal state.
* Per-job opt-out / per-job webhook URL override. Same: trivial to
  add when the use case shows up.
* Microsoft Teams.

**Tests** (+23, all in ``test_notify.py``, ``test_config.py`` +
``test_daemon.py::TestNotificationsOnTerminalTransition``):
payload shape (text+content+job), Slack/Discord compatibility,
no-op-when-url-unset (urlopen MUST NOT be called),
no-op-when-state-not-terminal, fire-and-forget timing
(``blocking=False`` returns under 0.5s when the post would sleep 2s),
URLError / unexpected-exception / 4xx all swallowed + logged,
``[notifications]`` parsing, extra-field rejection, pre-v0.5.35
configs still load, terminal-transition integration (COMPLETED /
FAILED / ABORTED_BY_QUEUE all fire), retry-re-enqueue does NOT
fire, default daemon has no webhook URL. 881 passed / 4 skipped on
macOS (+23 from v0.5.34).

### v0.5.34 — `vq submit --job-name NAME` (human-readable job label) (2026-05-15)

Optional decorative label that gives jobs a human-readable identity
in listings and on disk. Followed the SLURM/PBS/AWS-Batch convention
after a quick survey of comparable systems: jobid stays canonical as
the addressing key (no `vq status NAME` resolution, no uniqueness
constraints, no namespacing); the name is purely for humans and for
user-visible artifact filenames.

**Surface.**
* ``vq submit --job-name NAME``: optional, validated against
  ``^[A-Za-z0-9._-]{1,50}$``. Strict charset so the name flows into
  archive filenames + fetch destination dirs + ssh-shipped argv
  without any quoting concerns.
* **JobSpec.job_name**: ``str | None = None``. Additive field with a
  pydantic validator enforcing the charset; pre-v0.5.34 specs read
  clean (default None).
* **JobSpec.dest_dirname** property: returns ``f"{job_name}-{id}"``
  when name is set, else ``id``. Single source of truth used by
  fetch / archive / emit_workspace_tar.

**Where it shows up.**
* **``vq queue``**: new ``NAME`` column between ID and STATE, surfaced
  only when at least one spec has a name set (same zero-noise policy
  as v0.5.29's ``PRI`` column). Truncated to 20 chars with ``...``
  ellipsis to keep total row width readable on a 132-col terminal.
* **``vq status``**: ``name:`` line right after ``id:`` when set.
  Omitted entirely otherwise — common case status block unchanged.
* **``vq fetch JOBID -o DIR``**: destination lands at
  ``DIR/<name>-<jobid>/`` instead of ``DIR/<jobid>/`` when the spec
  has a name. The ``-<jobid>`` suffix is non-optional even with a
  name — two jobs can legitimately share a name; the suffix is what
  guarantees a fetch never silently clobbers another.
* **``vq cleanup --archive``**: archive at
  ``<archive_dir>/<name>-<jobid>.tar.bz2`` instead of
  ``<archive_dir>/<jobid>.tar.bz2``. Tarball internal top-level dir
  matches (``<name>-<jobid>``). Pre-v0.5.34 archives untouched —
  ``vq cleanup --restore`` reads either layout cleanly via the
  ``dest_dirname`` property.

**Not done by design.**
* No ``vq status NAME`` / ``vq fetch NAME`` resolution. Names aren't
  unique; the moment two jobs share one, name-as-identifier becomes
  ambiguous. We may add ``vq find NAME`` later (returning matching
  jobids) if the demand materialises.
* On-disk workspace path is unchanged: still
  ``<jobs_dir>/<jobid>/``. The daemon and watchdog address jobs by
  jobid; the name only shapes user-visible artifacts.

**``fetch_remote`` design note.** Pre-v0.5.34 could pre-check
``output_dir/<jobid>`` for existence locally before the SSH call,
because the dest dirname was fully predictable from the jobid alone.
With job_name in play, the dest dirname depends on remote spec state
the laptop doesn't have. We could pre-fetch the spec (extra SSH
round-trip on every fetch) OR peek the first tar member to learn the
top-level (one round-trip total, fast-path unchanged). Chose the
peek — the collision case pays one round-trip we'd have paid for
the fetch anyway; the common case (no collision) stays at one SSH.

**Tests**: +43 across spec / submit / listing / status / fetch /
cleanup. 858 passed / 4 skipped on macOS (+43 from v0.5.33).
Highlights:
* ``test_spec.py::TestJobName``: charset acceptance + rejection,
  round-trip, dest_dirname behaviour, old-spec compatibility.
* ``test_listing.py::TestFormatTableNameColumn``: conditional
  column, truncation, column ordering (ID NAME STATE).
* ``test_cleanup.py::TestArchiveWithJobName``: archive filename uses
  name prefix; tarball top-level matches; restore lands at original
  jobid-only workspace dir (not at name-prefixed location).
* ``test_fetch.py::TestFetchLocalWithJobName`` +
  ``TestEmitWorkspaceTarWithJobName``: end-to-end including an
  archive+fetch round trip with a named job.

### v0.5.33 — `vq queue` hides archived jobs by default (`--show-archived` opts back in) (2026-05-15)

**UX fix.** Once you've archived a job (``vq cleanup --archive`` tars
the workspace, removes it, stamps the spec with ``archived_at`` +
``archive_path``), you usually don't want to see it in the day-to-day
queue listing. Pre-v0.5.33 the archived spec stayed visible —
annotated ``(archived)`` — so a few weeks of work piled up dead rows
that crowded out the live queue.

**Behaviour.** ``vq queue`` now filters out specs with ``archived_at``
set. ``--show-archived`` reinstates the old behaviour for when you do
want to see the historical bin. Other verbs are unchanged:
``vq status JOBID``, ``vq fetch JOBID`` (un-tars the archive on the
fly), and ``vq cleanup --restore JOBID`` (un-tars permanently) all
still work on archived jobs — only the listing filters.

**Composes with v0.5.27 state filters.** ``vq queue -s completed``
shows live COMPLETED jobs only (archived COMPLETED still hidden);
``vq queue --show-archived -s completed`` shows both live and archived
COMPLETED. The state filter is "which states do I want"; the archived
filter is "do I want the historical bin too."

**Remote delegation forwarded.** The laptop ``vq queue HOST`` passes
``--show-archived`` through to the remote ``vq`` so filtering happens
on the host that holds the specs (consistent with how ``-s`` is
already plumbed).

**Tests.** +5 in ``test_listing.py::TestQueueArchivedFilter``:
default-hides-archived, ``--show-archived`` includes archived, state
filter alone doesn't bypass the archived hide, ``--show-archived``
composes with ``-s``, help mentions the flag.

### v0.5.32 — fix: shlex-quote argv across the laptop → ssh → remote-shell boundary (2026-05-15)

**User-reported bug.** Two symptoms, same root cause: `vq submit` with
a `bash -c '… > out'` command shape returned an empty stdout to the
laptop (parser raised "expected 12-hex jobid, got ''") even though the
job ran fine; and the redirection tail was *stripped* from
`bash -c`'s argument, so `bash` interpreted the wrong tokens. Caught
by the operator running 89 single-ORCA-job submits in a script and losing every
jobid the script tried to record.

**Root cause** (pre-v0.5.32 misconception, in the `run_remote_vq`
docstring): `ssh host cmd a b c` does *not* pass `[a, b, c]` as argv
to a remote process. ssh always pipes whatever follows the host to a
shell on the remote (`sh -c` style), and the remote shell re-tokenises
the joined string and re-interprets unquoted shell metacharacters.

So `vq submit … -- bash -c 'echo X > /tmp/y'` shipped (post argv
flattening by ssh) the line `vq submit … -- bash -c echo X > /tmp/y`
to the remote shell. The unquoted `>` was applied by the *outer*
remote shell, redirecting the *whole* `vq submit` command's stdout
(the printed jobid!) into `/tmp/y` on the remote — leaving local
`proc.stdout` empty (Bug 1) and stripping the redirect tail from
`bash -c`'s string argument (Bug 2).

**Diagnosis verified live**: compute-d had a stray `/home/USER/output.out`
file at exactly 13 bytes (12-hex jobid + newline) — that's where the operator's
89 submits had been printing their jobids. Reproduced fresh on the
laptop: `bash -c 'echo MARKER > /tmp/marker.txt'` landed on compute-d,
not the laptop. After fix: jobid prints cleanly; `marker.txt` lands
inside the job's workspace as intended.

**Fix.** Three sites, same one-line change: shlex-join the argv into a
single shell-safe command string before handing to ssh.

* ``transport.run_remote_vq``: `cmd = [*ssh_base, shlex.join([remote_vq,
  *vq_args])]` instead of `[*ssh_base, remote_vq, *vq_args]`. Affects
  every remote verb (`vq queue`, `vq status`, `vq submit`, `vq fetch`,
  `vq kill`, `vq pause/resume`, `vq throttle`, `vq drain`,
  `vq cleanup`, `vq programs --json`, `vq admin update / status`).
* ``transport.run_remote_shell``: same fix. One caller in
  `submit_remote` (`("rm", "-f", remote_tar)` cleanup) — no functional
  change today since `remote_tar` is a tmpfile path without
  metacharacters, but defensive parity with `run_remote_vq` and
  protects future callers.
* ``cli.py:_exec_tail_remote``: same fix. The `os.execvp("ssh", ...)`
  path for `vq tail HOST JOBID` had the same shape. `filename` is
  upstream-validated against `..` and absolute paths, but a name with
  a space (`my report.log`) would have broken without quoting.

The function docstrings now explain the model explicitly (the old
docstring claimed "the remote shell sees them as argv directly" — that
was wrong and was the source of the bug).

**Regression guards** (in the test suite, would catch a future revert):
* `test_transport.py::test_shell_metacharacters_preserved_through_quoting`
  — round-trips an argv containing `>`, `;`, and spaces through
  `run_remote_vq` and `shlex.split`s the captured ssh command string,
  asserting equality with the input argv. Reproduces the operator's exact bug
  shape (`bash -c "echo HELLO > /tmp/y.txt; cat /tmp/y.txt"`).
* `test_transport.py::test_paths_with_spaces_preserved` — same for
  `run_remote_shell`.
* `test_tail.py::test_remote_filename_with_spaces_preserved` — same
  for the `vq tail` direct-ssh path.

**Existing tests updated**: the unit tests that asserted on the
broken-up-argv pattern (`assert captured == [[..., "vq", "queue",
"localhost"]]`) now check the joined-string pattern via
`shlex.split(cmd[2])` — same invariant, correctly modelling what the
remote shell actually parses. Five `test_cli.py::TestRemoteDispatch`
tests + two `test_tail.py` tests refactored to use a helper.

**Deploy note.** Fix is laptop-side only (the LAPTOP's `vq` ships the
command to the remote; the remote's `vq` is unchanged). `pip install
-e .` on the laptop picks it up; on compute-d, pulling + reinstalling
is enough — no daemon restart needed (the daemon code is untouched).

+3 tests; 810 / 4 skipped on macOS (+3 from v0.5.31).

### v0.5.31 — retry-on-failure (`vq submit --retry N`) (2026-05-14)

Third v0.7 item. Re-enqueues a job that exits non-zero, up to N times,
with exponential backoff.

* **New JobSpec fields** (all additive — pre-v0.5.31 specs read clean):
  * ``retry_max: int = 0`` (``ge=0``) — budget set at submit via
    ``--retry N``.
  * ``retry_count: int = 0`` (``ge=0``) — retries spent so far.
  * ``not_before: str | None = None`` — earliest dispatch time (ISO);
    the dispatch loop skips a PENDING job whose ``not_before`` is in
    the future. A corrupt value is treated as "ready now" so a bad
    timestamp can't permanently trap a job.

* **``vq submit --retry N``** flag (``IntRange(min=0)``), plumbed
  through ``submit_local`` / ``submit_remote``.

* **Re-enqueue model (not sibling-resubmit).** When a job's command
  exits non-zero, the daemon's new ``_maybe_retry`` flips the SAME
  spec back to PENDING, increments ``retry_count``, sets
  ``not_before = now + backoff``, and clears the run-instance fields
  (pid / pgid / started_at / exit_code). One jobid throughout — the
  user submitted one job; the workspace (and its events.jsonl /
  stdout.log) is reused, so a retry continues the same history. (This
  is deliberately different from ``--auto-resume``'s sibling model:
  auto-resume preserves a real ABORTED_BY_QUEUE terminal spec; retry
  is "try the same thing again," conceptually one flaky job.)

* **Exponential backoff**: ``RETRY_BACKOFF_BASE_SECONDS=10``, doubling
  per retry, capped at ``RETRY_BACKOFF_MAX_SECONDS=600`` — 10s, 20s,
  40s, 80s, ... ≥640s → 600s.

* **Only the plain non-zero-exit FAILED transition is retryable.**
  Both daemon FAILED-sites (``_record_finish`` in-process,
  ``_record_orphan_finish`` orphan-via-marker) gate on
  ``rc != 0 and self._maybe_retry(...)``. Watchdog kills (OOM_KILLED /
  STARVED / TIME_EXCEEDED) and ``vq kill`` (KILLED) are caught by the
  ``is_terminal`` precedence check *upstream* of both sites — they
  never reach ``_maybe_retry``. A job the watchdog or the user killed
  must not silently come back.

* **Honest about determinism**: ``--retry`` is for *transient*
  failures (a flaky import, a filesystem hiccup, resource contention).
  A deterministic failure — bad input, SCF non-convergence — just
  burns all N attempts. The help text says this.

* **Composes with ``--auto-resume``**: ``_auto_resume``'s sibling now
  carries ``retry_max`` + ``retry_count`` forward (``not_before`` is
  intentionally NOT carried — a resume should dispatch promptly, not
  sit in a stale backoff window). A job with both flags that spent
  2/3 retries before a reboot resumes with 2/3 still spent.

* **Surfaced**: ``vq status`` shows ``retries: N/M used`` +
  ``not_before:`` lines (only when ``--retry`` was used); ``vq queue``
  annotates the state column ``pending (retry N/M)`` once
  ``retry_count > 0`` — so a PENDING job sitting in backoff is
  distinguishable from a fresh PENDING.

* +31 tests in new ``test_retry.py``: spec fields, backoff helper
  (doubling + cap), submit + CLI flag, ``_maybe_retry`` (no-budget /
  re-enqueue / exhausted / backoff-grows / event-logged), both
  FAILED-site integrations (re-enqueue / exhausted→FAILED /
  rc=0-never-retried / no-flag-unchanged / watchdog-kill-not-retried),
  the not_before dispatch gate (future-skipped / past-dispatched /
  none-dispatched / corrupt-treated-as-ready), and retry+auto-resume
  composition. 807 / 4 skipped on macOS (+31 from v0.5.30).

### v0.5.30 — opt-in auto-resume after host reboot (`--auto-resume`) (2026-05-14)

Second v0.7 item pulled forward — the operator's explicit 2026-05-10 request.
compute-d is a shared workstation (interactive workloads, occasional reboots) now
also internet-reachable; multi-hour vibe-qc sweeps that die in a reboot
shouldn't need a manual resubmit.

* **New JobSpec fields** (both additive — pre-v0.5.30 specs read clean,
  no SPEC_VERSION bump):
  * ``recover_on_reboot: bool = False`` — opt-in marker.
  * ``parent_jobid: str | None = None`` — set on a resumed sibling to
    the jobid it was resubmitted from; records the lineage chain.

* **``vq submit --auto-resume``** flag, plumbed through ``submit_local``
  / ``submit_remote`` (the latter forwards ``--auto-resume`` in the
  remote argv only when set).

* **Daemon startup auto-resume pass.** ``_reattach_or_interrupt_at_startup``
  now collects the specs it moves to ABORTED_BY_QUEUE **from a RUNNING
  entry state** in this pass, then — for those with
  ``recover_on_reboot=True`` — calls the new ``_auto_resume`` to emit a
  sibling resubmit: fresh jobid, SAME command + SAME workspace (so the
  job's own restart-from-disk logic — CRYSTAL GUESSP=fort.20, PySCF
  chkfile, ORCA .gbw — picks up the partial state), ``parent_jobid``
  linking the chain, ``recover_on_reboot`` propagated so a sibling that
  dies in a *later* reboot is itself resumed.

* **Deliberate exclusions** (each pinned by a test):
  * **Resubmit-storm guard** — only jobs RUNNING-at-entry to *this*
    startup pass are eligible; a spec already ABORTED_BY_QUEUE from a
    previous run is never re-resumed.
  * **SUSPENDED jobs** — a paused job whose pgid is gone (user paused,
    then host died) is NOT auto-resumed; un-pausing via resubmit
    contradicts the pause intent.
  * **Cleanly-finished jobs** — if the v0.5.9 exit marker is present
    (job actually completed during the daemon-down gap),
    ``_record_orphan_finish`` classifies it COMPLETED/FAILED and no
    resubmit happens.
  * **Live pgid** — a daemon-only restart (host didn't reboot)
    re-attaches the orphan; nothing is resubmitted.

* **vq submit doesn't checkpoint.** It re-runs the same command and
  trusts the user's wrapper to resume from disk. The ``--auto-resume``
  help text + the spec-field comment both say this loudly. Off by
  default — silent resume after a thermal trip is the wrong thing
  unless asked for.

* **``vq status``** surfaces ``auto-resume: on`` and ``parent_jobid:``
  lines, both only when set (uncluttered common case).

* +18 tests in new ``test_auto_resume.py``: spec fields (defaults,
  old-spec-reads-clean, disk roundtrip), submit + CLI flag, and the
  daemon startup pass — core resume, no-flag-no-sibling,
  resubmit-storm guard, SUSPENDED excluded, exit-marker-completes,
  live-pgid-reattach, sibling-dispatchable-and-resumable-again
  (two-generation lineage), mixed-flags, events.jsonl logging. 776 / 4
  skipped on macOS (+18 from v0.5.29).

* **Note on compute-d specifically**: auto-resume fires when the daemon
  *next starts*. compute-d's daemon currently runs via ``nohup`` (the
  systemd-user manager is broken), so it does NOT auto-start on
  reboot — auto-resume there is "resumes once you relaunch the
  daemon", not "resumes unattended". Fixing the systemd-user unit so
  the daemon auto-starts is the orthogonal compute-d-infra follow-up
  that makes auto-resume fully hands-off.

### v0.5.29 — job priority (`vq submit --priority N`) (2026-05-14)

First v0.7 item pulled forward — the roadmap's own rule ("order by
priority, not strict sequencing; a need overtaking the queue jumps
the line") sanctioned it. The need: a critical-path job (the GDF
sweep, on 2026-05-14) was stuck behind a lower-value job in a
``--max-jobs 1`` queue, with no way to make it jump ahead short of
killing the job in front or restarting the daemon.

* **New JobSpec field ``priority: int = 0``**. Additive — v1/v2 specs
  read into v0.5.29 cleanly, no SPEC_VERSION bump. Higher = more
  urgent; negative = "run after the default-priority work".

* **Daemon dispatch order** is now ``(-priority, submitted_at)``:
  higher priority dispatches first; within one priority level it
  stays FIFO by submission time. The all-default-priority case is
  byte-identical to the pre-v0.5.29 pure-FIFO ordering — a
  regression-guard test pins that.

* **``vq submit --priority N``** flag, plumbed through
  ``submit_local`` and ``submit_remote`` (the latter forwards
  ``--priority N`` in the remote argv only when non-zero, keeping
  the common-case command line clean).

* **Does NOT preempt RUNNING jobs.** Priority only reorders what
  dispatches *next*. A high-priority submit jumps ahead of everything
  still PENDING, but a job already running keeps running. Documented
  in the ``--priority`` help text so nobody expects preemption.

* **Surfaced conditionally**: ``vq queue`` grows a ``PRI`` column
  *only when* at least one job in the listing has a non-zero
  priority — zero noise in the overwhelmingly-common all-default
  case. ``vq status`` adds a ``priority:`` line, again only when
  non-zero.

* **Use case**:
  ```
  # GDF sweep is critical-path; jump it ahead of queued work:
  vq submit gdf_sweep.py --branch main --priority 10 --cpus 16
  ```

* +20 tests in new ``test_priority.py``: spec field (default/positive/
  negative/old-spec-reads-clean/disk-roundtrip), dispatch order
  (higher-first, FIFO-tiebreak, negative-runs-last, all-default-is-
  pure-FIFO regression guard), submit_local writes priority, CLI
  ``--priority`` flag (positive/default/negative/help), conditional
  PRI column (hidden when all-default, shown on any non-zero,
  negative rendered, empty-listing unaffected). 758 / 4 skipped on
  macOS (+20 from v0.5.28).

### v0.5.28 — `vq admin update --all` multi-env refresh (2026-05-14)

Third v0.6.0 admin-update item pulled forward (after ``--tag`` in
v0.5.24 and ``vq admin status`` in v0.5.25). Refreshes every
registered ``kind = "venv"`` program in one verb instead of one
``vq admin update <env>`` call per env.

* **New ``--all`` flag** on ``vq admin update``. Iterates every venv
  program in the registry (sorted by name), pulls + builds each.
  Binary and import programs are skipped (not git-backed).

* **Pause/resume bracket the WHOLE batch**, not per-env. The queue is
  paused once, every env is pulled+built, then resumed once. Rationale:
  a job dispatched mid-batch could otherwise see env A on the new
  commit but env B still on the old — pausing the batch closes that
  window. ``resume_all`` runs in a ``finally`` so a crash mid-batch
  still brings the queue back.

* **Validate-before-pause**: every env's git_dir is checked before
  the queue is paused. A typo'd ``git_dir`` in one env fails fast
  (``AdminError``) without ever pausing the queue.

* **One env's failure doesn't abort the rest**: every env is
  attempted, results collected. Batch verdict is all-or-nothing
  (``== BATCH OK: N/N ==`` vs ``== BATCH FAILED: M/N (failed: ...) ==``)
  but a failed vibeqc-dev still lets vibeqc-release run.

* **``--all`` is mutually exclusive with ``--tag``** (different envs
  track different tags — vibeqc-dev=main, vibeqc-release=a release
  tag — so a single ``--tag`` value can't apply to all) and with a
  positional ENV. With ``--all`` the first positional is HOST:
  ``vq admin update --all`` (default_host) or
  ``vq admin update --all compute-d``.

* **Each env's outcome is recorded** via ``record_update_outcome`` so
  ``vq admin status`` reflects the batch.

* **Internal refactor**: ``update_env``'s body split into
  ``_resolve_venv_program`` (validation, raises ``AdminError``) +
  ``_do_update_work`` (git pull + tag check + update_script, no
  pause/resume, no persist). ``update_env`` and ``update_all`` both
  compose those. Fixed a latent finally-masks-exception bug in
  ``update_env`` along the way (pre-bind ``result = None`` so the
  resume in ``finally`` can't shadow the original exception).

* **Empty registry** (zero venv programs) raises ``AdminError`` rather
  than silently returning an empty list — "nothing to do" is surfaced.

* +16 tests across ``TestUpdateAll`` (6: updates-every-venv,
  pause/resume-once, one-env-failure-continues, empty-registry,
  bad-git_dir-rejected-before-pause, records-each-outcome),
  ``TestFormatUpdateAllResults`` (3), ``TestUpdateAllCLI`` (7:
  happy path, failure exit code, --all+ENV mutex, --all+--tag mutex,
  no-env-no-all error, single-env regression guard, --help). 738 / 4
  skipped on macOS (+16 from v0.5.27).

* **Remaining v0.6.0 admin items**: self-update (``vq admin update
  vq``) with daemon restart, admin-update-in-progress marker file
  for crash recovery, multi-user / per-uid.

### v0.5.27 — `vq queue --state` filter + `--active` shortcut (2026-05-13)

User-driven ask alongside the v0.5.26 ``vq tail`` work: "how can I
only get running and pending jobs from vq queue?"  Pre-v0.5.27 the
verb dumped every state and chats relied on ``vq queue | awk
'NR==1 || /running|pending/'``. v0.5.27 makes it a first-class CLI
flag.

* **``-s / --state STATE`` flag** (repeatable Click multiple option).
  Examples:
  * ``vq queue -s running`` — only RUNNING
  * ``vq queue -s running -s pending`` — both
  * ``vq queue -s failed -s killed`` — terminal-failure forensics

* **``--active`` shortcut** — equivalent to
  ``-s running -s pending -s suspended`` (the non-terminal states,
  which is what users usually want when watching the queue).
  Composes with explicit ``-s``: ``--active -s completed`` shows
  the non-terminal states PLUS completed.

* **Server-side filtering on remote** — the resolved state set is
  forwarded to the remote vq as ``-s STATE`` flags. The remote vq
  drops non-matching specs before formatting; only the filtered
  table comes back over SSH. Cheaper than streaming the whole list.

* **Unknown state errors cleanly** — typos like ``-s faild`` error
  at CLI time with the list of valid states, rather than silently
  returning an empty listing (which would be misleading: "no failed
  jobs!" when really the filter just didn't match).

* **No-filter behaviour unchanged** — ``vq queue`` without ``-s`` /
  ``--active`` still lists every state. Backward-compatible.

* **Valid state names**: ``pending``, ``running``, ``suspended``,
  ``completed``, ``failed``, ``killed``, ``interrupted``,
  ``oom_killed``, ``starved``, ``time_exceeded``, ``aborted_by_queue``.

* +9 tests in ``test_listing.py::TestQueueStateFilter`` covering:
  no-filter shows-all-states (regression guard), single -s,
  multiple -s, --active alone, --active + explicit -s, unknown
  state error, empty filter result, --help mentions both flags +
  enumerates state names. 722 / 4 skipped on macOS (+9 from v0.5.26).

### v0.5.26 — `vq tail` verb for live log inspection (2026-05-13)

User-driven ask: "how can I print the output file vibe-qc writes when
its logger is configured?" Previously there was no built-in way; users
had to ssh to the host and run ``tail -f`` on the workspace path by
hand. v0.5.26 adds the verb.

* **New CLI verb ``vq tail [HOST] JOBID``** with three flags:
  * ``-f / --follow`` — stream new output (like `tail -f`)
  * ``-n / --lines N`` — initial lines (default 50; 0 = whole file)
  * ``--name FILENAME`` — file to tail in the workspace (default
    ``stdout.log``)

* **Default filename ``stdout.log``** matches what the daemon
  captures from the dispatched process's stdout. ``--name`` opens up
  any file the job wrote — including the canonical use cases:
  * **vibe-qc logger output** — ``--name vibeqc.log`` for chats that
    do ``logging.basicConfig(filename='vibeqc.log')`` in their script
  * **engine native outputs** — ``--name mgo.out`` for CRYSTAL,
    ``--name h2.out`` for ORCA / Psi4, ``--name stderr.log`` for the
    daemon-captured stderr

* **Implementation: ``execvp`` directly into `tail`** (locally) or
  into `ssh HOST vq tail localhost JOBID ...` (remotely). The Python
  CLI process is replaced; SIGINT, output streaming, and exit-code
  propagation all flow through `tail` itself. No buffering layer
  between vibe-qc's logger and the user's terminal.

* **Remote case** ``vq tail compute-d JOBID -f --name vibeqc.log``
  delegates as ``ssh compute-d vq tail localhost JOBID -f --name
  vibeqc.log`` — the remote vq resolves the workspace path against
  the daemon's actual ``$VQ_STATE_DIR``, not a guess from the laptop.

* **Path-traversal guarded**: ``--name`` rejects absolute paths and
  any ``..`` components. The intent is "tail a file IN the workspace,"
  not arbitrary filesystem read.

* **Helpful errors**: missing jobid (unknown workspace), missing file
  (lists what IS in the workspace, max 10 names, so the user can
  spot the right file).

* **File-must-exist caveat**: ``tail -f`` errors if the file doesn't
  exist when the verb starts. If the job just dispatched and the
  logger hasn't flushed yet, run ``vq status`` first to confirm
  RUNNING, then ``vq tail`` a moment later.

* **Chat workflow**:
  ```
  vq submit my_long_run.py --branch main      # dispatch
  vq queue                                    # find the jobid
  vq tail <jobid> --name vibeqc.log -f         # watch SCF converge live
  ```

* +14 tests in new ``test_tail.py`` covering local + remote argv
  shape, default vs explicit filename, --follow / --lines plumbing,
  missing-workspace error, missing-file error with hint,
  absolute-path + ``..`` rejection, --help discoverability.
  713 / 4 skipped on macOS (+14 from v0.5.25).

### v0.5.25 — `vq admin status` (2026-05-13)

Second v0.6.0 admin-update item pulled forward. Answers the
chat-debug question: "is compute-d at the commit I just pushed?"
without having to ssh in to ``git rev-parse HEAD``.

* **New verb ``vq admin status [HOST]``** (under the existing ``admin``
  click group). Lists every registered ``kind = "venv"`` program with
  columns: NAME, BRANCH, SHA, DESCRIBE, DIRTY, LAST_UPDATED_AT,
  LAST OK.

* **Live git queries** per env:
  * ``git rev-parse --short=12 HEAD`` → SHA column
  * ``git describe --tags --always`` → DESCRIBE column (exact tag if
    HEAD is at one, otherwise ``<tag>-<ahead>-g<sha>``)
  * ``git status --porcelain`` → DIRTY (yes/no/-)
  All wrapped in best-effort exception catches; a broken git just
  shows "-" rather than crashing the whole table.

* **Persisted history** in ``<state_root>/admin-status.json`` (one
  ``AdminUpdateRecord`` per env, keyed by env name). Written by
  ``record_update_outcome`` at the end of every ``update_env``
  invocation (success or failure — failures are forensics-useful).
  Schema: ``last_updated_at`` (ISO ts), ``last_success`` (bool),
  ``last_sha``, ``last_tag``, ``last_expected_tag``,
  ``last_git_pull_rc``, ``last_update_script_rc``.

* **Read-side resilience**: corrupt JSON returns ``{}``; entries with
  unknown fields (schema drift from a future vq version) are quietly
  dropped, not crashed on.

* **Delegation**: ``vq admin status`` on the laptop SSH-forwards to
  ``ssh compute-d vq admin status localhost``; the remote vq's git
  queries run where the checkouts actually live.

* **Binary and import programs excluded** from the table — they're
  not git-backed, the columns wouldn't make sense.

* +15 tests across ``TestAdminStatusPersistence`` (3),
  ``TestAdminStatusFormat`` (4), ``TestAdminStatusCLI`` (4),
  ``TestAdminStatusPersistenceFunctions`` (4). 699 / 4 skipped on
  macOS (+15 from v0.5.24).

* Test-isolation fix: ``_query_git_sha`` / ``_describe`` / ``_dirty``
  now catch ``Exception`` broadly because they're called from the
  end of ``update_env`` (after the explicit subprocess.run mocks have
  been consumed). Production behaviour unchanged — the broad catch is
  defensive against the existing OSError/TimeoutExpired plus any
  future surprise from subprocess.

### v0.5.24 — `--tag` verification for `vq admin update` (2026-05-13)

First v0.6.0 admin-update item pulled forward. The release-chat
coordination doc named this as the catch for the "libint vanishing"
bug class: pull succeeded, but the checkout didn't land on the
expected tag (rebase, branch divergence, force-push, race with a
human). v0.5.24 adds a single ``git describe --exact-match`` check
between the pull and the build.

* **New ``--tag TAG`` option** on ``vq admin update <env>``. When set,
  after ``git pull`` succeeds the verb runs ``git -C <git_dir>
  describe --exact-match --tags HEAD`` and asserts the result matches.

* **Mismatch handling**: the update_script is SKIPPED — running the
  build against a wrongly-tagged checkout would produce a wrongly-
  tagged venv, which is exactly the failure class we're guarding
  against. ``UpdateResult.success`` becomes False; CLI exits non-zero;
  the formatted report shows "MISMATCH (got 'v0.7.3')" for forensics.

* **New ``UpdateResult`` fields**: ``expected_tag``, ``actual_tag``,
  ``tag_check_rc``. Plus computed ``tag_verification_attempted`` /
  ``tag_matches`` properties. ``success`` extended to fail when
  ``--tag`` was given and the tag didn't match.

* **Delegation**: ``vq admin update vibeqc-release --tag v0.8.0``
  from the laptop SSH-forwards through to
  ``ssh compute-d vq admin update vibeqc-release localhost --tag v0.8.0``;
  the remote vq runs the verification locally where the git
  checkout actually lives.

* **No --tag = no behaviour change** vs v0.5.20 minimal. Regression
  guard test asserts the describe subprocess never fires when ``--tag``
  is unset.

* **Release-chat workflow**:
  ```
  git push --tags
  vq admin update vibeqc-release --tag v0.8.0
  vq submit smoke_test.py --branch release
  ```

* +10 tests across ``TestTagVerification`` (7) + ``TestTagVerificationCLI``
  (3). 684 / 4 skipped on macOS (+10 from v0.5.23).

### v0.5.23 — per-state retention overrides for auto-cleanup (2026-05-13)

Closes the last ``vq cleanup`` followup. Pre-v0.5.23 every terminal
state shared one archive threshold and one delete threshold; you
couldn't keep failed-job workspaces around longer than completed jobs
without rewriting the policy by hand.

* **New ``AutoCleanupPolicy`` fields**:
  * ``archive_after_by_state: dict[str, int]`` — per-state archive
    threshold (seconds). Empty by default.
  * ``delete_after_by_state: dict[str, int]`` — symmetric for delete.
  Keys are JobState values (``"completed"``, ``"failed"``,
  ``"killed"``, ``"oom_killed"``, ``"time_exceeded"``, ``"starved"``,
  ``"interrupted"``, ``"aborted_by_queue"``).

* **Resolution helper** ``_cutoff_for(policy, kind, state)``: per-state
  override wins; otherwise the global ``<kind>_after_seconds`` is
  used; if both unset, returns ``None`` (state not covered → skipped).

* **``run_auto_cleanup_pass`` refactor**: instead of a single
  ``find_candidates(older_than=...)`` call per kind, walks ALL
  candidates and age-checks each against its state's specific
  cutoff. Same primitives (``archive_workspace`` /
  ``delete_job``) under the hood; the per-spec age check moved into
  the new ``_spec_older_than`` helper.

* **New ``parse_state_age("STATE:DUR")``** helper in ``cleanup.py``
  for CLI parsing. Validates the state name against
  ``VALID_STATE_NAMES`` so a typo'd "faild:7d" errors at CLI time
  rather than persisting silently.

* **CLI** ``vq cleanup --auto-enable`` gains:
  * ``--archive-after-state STATE:DUR`` (repeatable; multiple Click
    option)
  * ``--delete-after-state STATE:DUR`` (repeatable)

  Either kind alone is sufficient to satisfy the ``--auto-enable``
  "at least one threshold" requirement (you can run the policy on a
  single state if you want).

* **Reporting**: ``--auto-status`` lists each per-state override
  inline; the ``vq cleanup --auto-enable`` confirmation message
  mirrors the same lines so a typo in STATE shows up immediately.

* **Typical use case** — keep failed-job forensics around 4× longer
  than completed-job records:
  ```
  vq cleanup --auto-enable --archive-after 30d \
             --archive-after-state failed:90d --delete-after 180d
  ```

* +18 tests across ``TestParseStateAge`` (5),
  ``TestPolicyPerStateFields`` (2), ``TestCutoffResolution`` (4),
  ``TestPerStateAutoPass`` (3), ``TestPerStateCLI`` (4). 674 / 4
  skipped on macOS (+18 from v0.5.22).

### v0.5.22 — configurable archive_dir for `vq cleanup` (2026-05-13)

Closes the second-to-last ``vq cleanup`` followup. Pre-v0.5.22 the
archive directory was hard-coded to ``<state_root>/archive/``; compute-d
users with a small ``~`` partition and a roomy secondary disk had no
way to redirect short of moving the entire state root.

* **Env var override** ``$VQ_ARCHIVE_DIR`` (``paths.ENV_ARCHIVE_DIR``)
  takes precedence over the default. Applies to every code path that
  reads ``paths.archive_dir()``: manual ``--archive``, auto-cleanup
  sweeps, archive-aware ``vq fetch``, the ``(archived)`` annotation in
  ``vq queue``. One env var per host.

* **Per-policy override** ``AutoCleanupPolicy.archive_dir`` (Optional
  absolute-path string). Stored in the policy JSON; persists across
  daemon restarts. Trumps both ``$VQ_ARCHIVE_DIR`` and the default —
  the policy author wins so different policies can target different
  volumes.

* **CLI flag** ``--archive-dir DIR`` on ``vq cleanup``:
  * With ``--auto-enable``: stored in the policy.
  * With one-shot ``--archive``: used for that invocation only (no
    policy file written).
  * Tilde expansion applied before persistence so the on-disk JSON is
    unambiguous about which directory it means.

* ``vq cleanup --auto-status`` reports the active ``archive_dir`` when
  the policy has one set; absent line means "default."

* +11 tests across ``TestArchiveDirEnvOverride`` (env var
  resolution + archive_workspace round-trip), ``TestPolicyArchiveDirField``
  (JSON persistence + auto-pass override + env-var fallback when policy
  unset), ``TestArchiveDirCLI`` (--auto-enable persistence, tilde
  expansion, --auto-status reporting, one-shot --archive override).
  656 / 4 skipped on macOS (+11 from v0.5.21).

### v0.5.21 — `renice` fallback for `vq throttle` on non-cgroup hosts (2026-05-13)

Closes the last operator-controls pending item. Pre-v0.5.21, ``vq
throttle`` raised ``ThrottleError`` on hosts without ``systemd-run
--user --scope`` delegation; the renice fallback was sketched in the
v0.5.13 roadmap entry but punted because compute-d was always
cgroup-enforced. That assumption broke in this session: systemd-user
on compute-d hung, daemon now runs via nohup with ``cgroup=disabled``.
The fallback is suddenly the only path to throttle on compute-d.

* **New helpers in ``throttle.py``**:
  * ``_weight_to_nice(weight) -> int`` — coarse-banded heuristic
    mapping cgroup ``CPUWeight`` [1-10000] to POSIX nice [-20, 19].
    Default 100 -> 0; typical throttle 20 -> 10; deep throttle 1 -> 19;
    boost 200 -> -5 (root-only territory). The mapping table is in the
    function docstring and the test file pins the key bands.
  * ``_renice_pgid(pgid, nice) -> (bool, str)`` — wraps
    ``renice -n N -g <pgid>``, captures stderr on failure, catches
    timeout + OSError defensively.
  * ``_apply_throttle(scope, pgid, weight) -> (path, detail)`` —
    shared cgroup-or-renice helper. Tries cgroup first; falls back to
    renice when ``cgroup.available()`` returns False. Raises
    ``ThrottleError`` if both paths are unusable (cgroup off AND no
    pgid on spec).

* **``throttle_job`` now uses ``_apply_throttle``** under the hood.
  Success message tells the user which path ran: "throttled job X to
  CPUWeight=20" (cgroup) vs "throttled job X via renice (nice=10
  ...); cgroup unavailable on this host" (fallback).

* **``apply_persistent_throttle_if_set``** takes a new ``pgid: int |
  None`` parameter so the daemon can hand in the freshly-started
  job's pgid for the renice path. Cgroup path unchanged; the new
  parameter has a default of ``None`` so existing callers (none
  outside the daemon) still work.

* **Daemon ``_start_job``**: the ``if self.cgroup_enabled:`` gate
  around the persistent-throttle apply is gone. ``apply_persistent
  _throttle_if_set`` is called unconditionally; it handles
  cgroup-vs-renice dispatch internally. Log message includes which
  path ran.

* +20 tests across ``test_throttle.py``:
  ``TestWeightToNiceMapping`` (7), ``TestRenicePgid`` (5),
  ``TestThrottleJobReniceFallback`` (2),
  ``TestApplyPersistentThrottleReniceFallback`` (4),
  ``TestThrottleAllReniceFallback`` (1), plus the
  ``test_throttle_no_cgroup_*`` rewrites (1). 645 / 4 skipped on
  macOS (+20 from v0.5.20).

* **What still requires cgroup**: bound memory caps via
  ``MemoryMax/MemoryHigh``, CPU quota via ``CPUQuota``, and the
  kernel-enforcement-of-watchdog-decision story. Renice gives you
  soft-priority only. For deep precision the cgroup path remains the
  right setup; renice covers the "step aside under contention"
  workflow.

### v0.5.20 — `vq admin update <env>` minimal (2026-05-13)

Closes the chat-feature-test gap the operator raised when discussing the v0.8.0
tag: a chat that pushes a commit then submits a job needs compute-d to
have that commit. Before v0.5.20 the chat had to either ask the operator to
``ssh compute-d && git pull && bash scripts/update-dev.sh`` manually, or
submit-as-exec hack the update inline. v0.5.20 ships the dedicated
verb so it's one line:

```bash
git push
vq admin update vibeqc-dev
vq submit my_feature_test.py --branch main
```

* **New verb ``vq admin update <env> [HOST]``** (new ``admin`` click
  group). Reads ``<env>`` from the host's ``[programs.X]`` registry —
  must be ``kind = "venv"`` (the binary/import kinds aren't
  git-backed and the verb errors out for them). Pipeline:
  1. ``pause_all(host)`` so the rebuild can't race a running job's
     loaded modules.
  2. ``git -C <git_dir> pull`` (capture stdout+stderr; rc=N → skip
     update_script).
  3. If ``update_script`` is configured AND git pull succeeded:
     ``bash <git_dir>/<update_script>`` (e.g.
     ``scripts/update-dev.sh``).
  4. ``resume_all(host)`` — in a ``finally`` block, so a Ctrl-C
     between pause and pull doesn't strand jobs in SUSPENDED.

* **Bounded subprocess timeouts**: ``git pull`` capped at 300s,
  ``update_script`` capped at 1800s (30 min — covers a full vibe-qc
  rebuild with libint + libxc + the C++ extensions). Timeout records
  a work_error and falls through to resume_all; no infinite hang.

* **Exit code** is 0 only if both git pull and update_script (if any)
  finished rc=0 and no work_errors were recorded. Non-zero exit
  prints a "FAILED" line with reasons (which subprocess failed, or
  which work errors hit).

* **Delegation pattern** matches every other vq verb: ``vq admin
  update vibeqc-dev`` on the laptop SSH-forwards to ``ssh compute-d vq
  admin update vibeqc-dev localhost``. The registry lives where the
  binaries do (compute-d); the laptop just dispatches the request.

* **v0.5.20 minimal scope deliberately excludes** (these stay as the
  v0.6.0 roadmap entry below):
  * ``--tag v0.X.Y`` verification post-pull
  * ``--all`` multi-env mode
  * Admin-update-in-progress marker file for crash recovery
  * ``vq admin status`` (last-update times, tip SHAs)
  * Self-update (``vq admin update vq``) with daemon restart
  * Multi-user / per-uid

* +23 tests in ``test_admin.py``: input-validation (unknown env, wrong
  kind, missing git_dir, not a checkout), execution paths (happy,
  no update_script, git pull failure skips script, script failure,
  missing script file, timeout), resume-always-runs invariant via
  ``finally`` (subprocess.run raises → pause_all called, resume_all
  still called), format_update_result output shape (OK/FAILED/work
  errors/no-script-block), CLI verb (exit codes, help text). Full
  suite: 625 passed / 4 skipped on macOS (+23 from v0.5.19).

### v0.5.19 — smoke test consumes absolute paths from `vq programs --json` (2026-05-13)

Closes the PATH-fragility class of failure the v0.5.18 smoke test hit
on its first run: ``crystal-serial``, ``crystal-parallel``, ``psi4`` all
failed in ~2.7 s because the nohup'd daemon's PATH didn't include
``/home/USER/bin`` or ``/home/USER/psi4conda/bin``. With v0.5.19 the
smoke test reads the registry directly and submits with absolute paths
— the daemon's PATH stops mattering for any binary engine.

* **``vq programs --json``** emits the registry as a JSON array. Stable
  schema: common fields (``name``, ``kind``, ``status``, ``reason``,
  ``description``) plus kind-specific fields (``binary`` for binary
  programs; ``python``+``git_dir``+``branch``+``update_script`` plus
  runtime identity such as ``current_git_sha``, ``current_git_branch``,
  and ``current_git_dirty`` for venv; ``python``+``import_check`` for
  import). Sorted by name so consecutive snapshots diff cleanly. Same
  delegation logic as the human table — ``vq programs HOST --json``
  forwards the flag through SSH.

* **``contrib/run-crystal.sh``** honors four env-var binary overrides:
  ``CRYSTAL_BIN``, ``PCRYSTAL_BIN``, ``PROPERTIES_BIN``,
  ``PPROPERTIES_BIN``. Each falls back to ``command -v <name>`` when
  unset, preserving pre-v0.5.19 behaviour. mpirun is still PATH-resolved
  (system /usr/bin/mpirun on compute-d; non-issue).

* **``tests/integration_smoke.py``** now reads ``vq programs --json``
  once at startup, gates each engine on ``status="OK"`` AND the
  expected ``kind``, then hands the matching record into each
  ``submit()`` callable. Binary engines (orca, psi4) submit with the
  absolute path directly; CRYSTAL engines submit via
  ``env CRYSTAL_BIN=… bash run-crystal.sh …`` /
  ``env PCRYSTAL_BIN=… bash run-crystal.sh …``. Header now prints which
  binary each engine resolved to before the run, for transparency.

* +16 tests (8 JSON-output in ``test_programs.py``, 8 wrapper-env-var
  in new ``test_run_crystal_sh.py``); 602 / 4 skipped on macOS.

### v0.5.18 — program registry + `vq programs` verb + integration smoke (2026-05-10)

New top-level verb ``vq programs [HOST]`` lists registered programs
and probes availability. Three program kinds with pydantic
discriminator on ``kind``:
* ``binary`` (CRYSTAL, ORCA, Psi4) — exists + executable bit
* ``venv`` (vibeqc-dev, vibeqc-release) — python + git_dir present
* ``import`` (pyscf) — ``python -c 'import name'`` returns 0

Per-host registry (lives in the host's own config.toml, not the
laptop's). ``vq programs`` from the laptop delegates via SSH to
``vq programs localhost`` so probes run where files actually exist.

Foundation for v0.6.0's ``vq admin update <name>`` — same
``[programs.X]`` table will drive the venv-refresh logic.

New integration smoke script at ``tests/integration_smoke.py`` (not
pytest-collected; runs against live host). Exercises vibe-qc-dev,
vibe-qc-release, ORCA, CRYSTAL serial, Pcrystal, PySCF, Psi4
end-to-end: submit → wait → fetch → assert artifacts present. The
visualisation files (CRYSTAL fort.9/fort.34/fort.87; ORCA .gbw)
are explicitly checked.

+23 unit tests; 586 / 4 skipped on macOS.

### v0.5.17 — auto-cleanup policy (daemon main-loop integration) (2026-05-10)

Closes the v0.5.10-deferred "auto-policy" item. New CLI surface on
``vq cleanup``: ``--auto-enable`` (with ``--archive-after`` /
``--delete-after`` / ``--interval`` / ``--reason``),
``--auto-disable``, ``--auto-status``. Policy persisted to
``<state_root>/auto-cleanup.json``; daemon's ``iterate()`` reads it
each loop and runs the archive + delete passes when interval has
elapsed since last_run_at.

Implementation: ``AutoCleanupPolicy`` pydantic model + persistence
functions + ``run_auto_cleanup_pass`` (which composes the existing
``find_candidates`` / ``archive_workspace`` / ``delete_job``
primitives), all in cleanup.py. Daemon's ``_maybe_auto_cleanup``
hook in iterate(); exception-safe (sweep failure logs but never
propagates).

Atomic tmpfile-rename writes, corrupt-file tolerance, last_run_at
stamping (even on zero-candidate sweeps so the daemon doesn't churn
on every iteration). Mutex with one-shot ``--archive`` / ``--delete``
/ ``--restore`` modes (the auto-policy uses those primitives
internally; mixing them in one invocation would be ambiguous).

+30 tests; 563 / 4 skipped on macOS.

### v0.5.16 — `--duration` auto-release for drain + persistent throttle (2026-05-10)

Both ``DrainState`` and ``ThrottleState`` grow ``duration_seconds:
int | None``. CLI: ``--duration 2h`` / ``30m`` / ``600s`` etc.
(reuses ``cleanup.parse_age``). Centralised expiry check inside
``read_drain_state()`` and ``read_throttle_state()``: expired state
files are silently cleared on the next read, so every caller
(daemon dispatch loop, CLI ``--status``, persistent-throttle apply
in ``_start_job``) gets the same "expired = no state" view.

``--status`` output gains an ``auto-release in Ns`` countdown for
both verbs. Bad ``set_at`` timestamps are conservatively treated as
no-expire.

+14 tests; 533 / 4 skipped on macOS. Closes the
"drain/throttle auto-release after duration" item.

### v0.5.15 — persistent throttle across new dispatches (2026-05-10)

Closes the v0.5.13-documented limitation that
``vq throttle --all --weight 20`` didn't carry to newly-dispatched
jobs. New ``--persist`` flag (requires ``--all``) writes
``throttle.json`` next to ``drain.json``; daemon ``_start_job``
reads it after creating the systemd-run scope and applies CPUWeight
to the new scope.

New CLI surface on ``vq throttle``:
* ``--persist`` (with ``--all``) — also write throttle.json
* ``--release-persist`` — clear state file without touching running scopes
* ``--status`` — read-only snapshot of persistent state
* ``--reason TEXT`` — optional label

``--restore --all`` now also clears the persistent state file (so
the interactive-work-finished flow stays single-command).

Best-effort: a failed ``systemctl set-property`` on the new scope
leaves the job at default CPUWeight and logs the failure; doesn't
block dispatch. Same atomic-tmpfile-rename write + corrupt-file
tolerance as drain.json.

+17 tests; 519 / 4 skipped on macOS.

### v0.5.14 — `vq drain` (daemon dispatch gate) (2026-05-10)

New verb that gates the daemon's dispatch loop on a persisted state
file (``<state_root>/drain.json``). Three modes: full drain (no new
dispatches), partial drain (lower the effective ``max_jobs`` /
``max_cpus`` cap below daemon's configured value), or released
(remove the file, daemon goes back to configured caps).

Implementation: ``DrainState`` pydantic model + atomic
tmpfile-rename writes; daemon ``_dispatch_pending`` reads the state
at the top of every iteration (cheap stat + json parse, takes effect
within ``poll_interval``). ``effective_max_jobs(daemon_cap)`` and
``effective_max_cpus(daemon_cap)`` helpers compute the
``min(daemon, drain)`` cap when both are set.

Survives daemon restarts and host reboots (state on disk).
Affects only NEW dispatches; running jobs continue. Composes with
``vq pause --all`` and ``vq throttle --all`` for the full
operator-control story.

+27 tests (DrainState model, persistence + atomicity +
corruption-tolerance, daemon dispatch integration, CLI verb).
502 / 4 skipped on macOS.

Together with v0.5.13 ``vq throttle``, this closes the original
v0.5.11 "operator controls" roadmap pin.

### v0.5.13 — `vq throttle` (soft CPU priority) (2026-05-10)

New verb that adjusts a running job's cgroup ``CPUWeight`` at runtime
without killing or pausing it. Sibling to ``vq pause`` / ``vq resume``:
same per-job + ``--all`` API shape, same local/remote dispatch
routing. Under contention with a default-priority process, the
throttled job's share of disputed cores is roughly
``weight / (weight + 100)``; when nothing else wants CPU, the
throttled job still uses everything available.

The actual systemd mutation goes through a new
``cgroup.set_cpu_weight(scope, weight)`` helper. Unlike
``RuntimeMaxSec`` (the v0.5.7 misadventure), ``CPUWeight`` IS
runtime-mutable per ``systemd.resource-control(5)``; this works.

cgroup-only in v0.5.13. Hosts without ``systemd-run --user --scope``
delegation get a ``ThrottleError``. Doesn't persist across new
dispatches; persistent throttle would need daemon-level state,
deferred to v0.6.x.

+32 tests (26 throttle + 6 cgroup); 475 / 4 skipped on macOS.

### v0.5.12 — watchdog samples pgid descendants (2026-05-10)

Closes the STARVED-regression that v0.5.9's bash-wrap introduced.
Per-pid /proc sampling was reading bash (sleeping in ``wait()``) at
~0% CPU, false-positive-killing every >5min single-process job. New
pgid-aware readers (`_pgid_pids`, `read_rss_mb_pgid`,
`read_cputime_seconds_pgid`) walk /proc once per sample, sum across
the pgroup. Side benefit: pre-v0.5.9 watchdog under-reported MPI/OMP
forks too; pgid aggregation captures those.

Watchdog STARVED workaround (always declare `--wall-time-seconds N`)
is no longer needed once daemon is on v0.5.12+. Existing recipes
that include it are still fine — the wall-time path uses different
watchdog logic that doesn't conflict.

+9 tests; 443/4 skipped on macOS.

### v0.5.11 — archive-aware remote fetch (2026-05-10)

Closes a v0.5.10 contract that wasn't actually held end-to-end:
``vq fetch <archived-jobid>`` worked locally on the daemon host but
failed remotely from the laptop because `emit_workspace_tar` (the
internal `vq tar-workspace` verb that `fetch_remote` invokes over
SSH) only knew about live workspaces. Confirmed live on compute-d
2026-05-10 with job `c99c97cad7c3` (archived MgO PBE/POB-TZVP run).

Fix: `emit_workspace_tar` checks `spec.is_archived` and streams the
.tar.bz2 archive bytes directly to stdout when archived. The
receiving end's `tarfile.open(mode="r|")` autodetects bz2 so
`fetch_remote` is unchanged.

+2 tests; 435/3 skipped on macOS.

### v0.5.10 — `vq cleanup` verb (2026-05-10)

Closes the workspace-accumulation pain point the operator hit on 2026-05-10
(77 specs + 78 workspaces / 457 MB hand-deleted out of
``~/.local/share/vq/`` after a queue cleanup). The manual verb lands
exactly the surface the v0.6 design notes pinned (see "Pending —
v0.6.x auto-cleanup policy" above): three actions on terminal-state
jobs, ``--older-than`` age filter, dry-run by default with ``-x`` to
execute. Auto-policy + config block stay in v0.6.x.

* New CLI verb ``vq cleanup [HOST] [--archive | --delete | --restore JOBID]
  [--older-than DUR] [-x]``. Same delegation pattern as ``vq queue`` /
  ``vq status`` (local: act on local state; remote: ssh'd through to
  the remote vq).
* Archive: tar.bz2 the workspace, remove it, stamp ``archived_at`` +
  ``archive_path`` on the spec. Spec stays in the queue (annotated
  ``(archived)``); ``vq fetch`` knows to un-tar instead of copy.
  Tempfile-then-rename for the tarball write so a crash mid-tar
  can't leave a half-written ``.tar.bz2`` in the archive dir.
* Delete: idempotent removal of spec + workspace + archive
  (whichever exist). Refuses non-terminal jobs (the daemon could
  still be writing).
* Restore: un-tar the archive back into the workspace, clear
  ``archived_at`` + ``archive_path``, remove the archive file.
* Dry-run is the default (the verb prints a "would archive: N jobs;
  reclaimable: M MB" preview); ``-x`` / ``--execute`` flips it to
  actually act. Mode flags (``--archive`` / ``--delete`` /
  ``--restore``) are mutually exclusive.
* New optional spec fields: ``last_status_at``, ``last_fetched_at``
  (touched by the corresponding verbs on terminal specs only),
  ``archived_at``, ``archive_path``. Additive (no ``SPEC_VERSION``
  bump per the v0.3 convention) so v0.3+ specs read into v0.5.10
  cleanly.
* ``vq queue`` annotates ``(archived)`` next to the state column;
  ``vq status`` adds an ``archive:`` line + a "vq cleanup --restore"
  hint where the stdout/stderr tails would normally go.
* +50 tests across ``test_cleanup.py`` (parse_age, eligibility
  filters, archive/restore round-trip, delete idempotence),
  ``test_cli.py::TestCleanupCLI`` (mode mutex, ``--older-than``
  required, dry-run safety net, archive + restore round-trip),
  ``test_status.py`` / ``test_listing.py`` / ``test_fetch.py`` (the
  annotation / touch / archive-fetch hooks). Full suite: 433
  passed / 3 skipped on macOS.

### v0.5.9 — orphan exit-code recovery (2026-05-10)

Closes the second of the two v0.5.1 design gaps surfaced during
v0.5.6 smoke testing on compute-d (jobs `dd4fef514889`,
`6660f47568b6`). With v0.5.8's wall-time correction (which prevents
systemd from killing paused jobs) plus v0.5.9's exit-code recovery,
the full pause-restart-resume-complete cycle is now correctness-clean
end-to-end — both blockers for v0.6's `vq admin update` are clear.

**Bug:** A job that survives a daemon restart (re-attached as an
orphan via `pgid` in `_reattach_or_interrupt_at_startup`) AND then
completes normally got marked `ABORTED_BY_QUEUE` instead of
`COMPLETED`. The v0.4 reconciler polled `killpg(pgid, 0)` and, when
the pgid disappeared, marked the spec aborted with reason "orphan
process exited (exit code unrecoverable)". That was correct when we
genuinely didn't know the rc — but wrong when the inner command
exited cleanly and we just lacked the `Popen` handle to read it.

**Fix:** wrap every dispatched command in a tiny bash shim that
captures the inner rc to `<workspace>/_vq/exit-code` before exit.
The orphan reconciler reads the marker on pgid disappearance:

* marker present → `_record_orphan_finish` classifies as `COMPLETED`
  (rc=0) or `FAILED` (rc!=0; bash's 128+sig is also recorded).
* marker absent → `ABORTED_BY_QUEUE` (the v0.4 path; reserved now
  for genuinely unknown-rc cases: SIGKILL of bash itself, host
  crash, pre-v0.5.9 spec on disk).

`_reattach_or_interrupt_at_startup` follows the same logic, so a
spec stuck in `RUNNING` with a gone pgid + a written marker also
recovers correctly at daemon startup (the exact symptom of the
reported bug).

**Wrap composition:** `systemd-run --scope … -- bash -c '"$@"; rc=$?;
echo "$rc" > /abs/path/to/_vq/exit-code; exit "$rc"' vq-exit-wrap
<original argv>`. The bash wrap goes *inside* the cgroup wrap so
both bash and the user cmd are accounted to the per-job cgroup; a
runaway bash gets killed by the kernel like any other in-scope
process. `$0` is `vq-exit-wrap` so `ps` shows the wrapper's role.
The marker path is shell-quoted at wrap time.

**Stale-marker hygiene:** `_start_job` unlinks any pre-existing
marker before dispatch. Workspaces are jobid-keyed and freshly
created by `submit`, so a stale marker can only appear on a
re-dispatch of the same workspace (not currently a supported
flow); the unlink keeps the invariant "marker exists ⇒ *this*
dispatch wrote it."

**Known caveat:** in non-cgroup mode (Linux without
`Delegate=memory` on the user manager), the watchdog samples
`/proc/<popen.pid>/status` which after wrapping is bash's RSS
(~5MB) rather than the user cmd's. Per-job memory caps (`spec.mem_mb`)
won't fire in that mode. compute-d uses cgroup mode (kernel
enforces, watchdog telemetry-only — unaffected); macOS has no
`/proc` (watchdog already no-ops — unaffected). A v0.6.x follow-up
can switch the watchdog to walk `/proc/<pgid>/task/<pgid>/children`
and sample the heaviest descendant; deferred until the
non-cgroup-Linux case is actually in scope.

* +20 tests in `test_daemon.py` (TestExitMarkerHelpers,
  TestExitMarkerIntegration, TestOrphanRecoveryViaMarker)
  covering: marker round-trip with rc=0/non-zero/signal exits, path
  with spaces, stale marker cleanup, stdout-flushed-before-marker,
  startup-reattach with marker, orphan-with-terminal-state
  precedence, and the full pause-restart-resume-complete lifecycle.

### v0.5.8 — wall-time enforcement returns to the watchdog (2026-05-10)

**Why:** v0.5.7 attempted to make pause/resume pause-aware at the
cgroup level by mutating `RuntimeMaxSec` via
`systemctl --user set-property`. Live smoke-test on compute-d
(`b1a71b4ddea4`, `804e1b11a6a9`) showed systemd silently rejects
those calls — `set-property` only mutates properties listed in
`systemd.resource-control(5)` (cgroup knobs); time-based properties
from `systemd.exec(5)` are not runtime-mutable by design. v0.5.7's
unit + integration tests all passed because they mocked
`subprocess.run` as success, never exercising real systemd.

**v0.5.8 fix:**

* `cgroup.wrap_command` no longer emits `--property=RuntimeMaxSec=…`.
  The `wall_time_seconds` parameter is removed.
* `cgroup.set_runtime_max_sec()` helper deleted (was the v0.5.7
  addition); 9 associated tests removed.
* Daemon flips `enforce_wall_time=True` always — the Python
  watchdog (which has subtracted `paused_seconds_total` from
  elapsed since v0.5.1) is now the single owner of wall-time
  enforcement. Pause is naturally pause-aware: SIGSTOP'd jobs
  accrue zero active time.
* No new tests required — the existing watchdog pause-aware tests
  (`test_pause_resume.TestWatchdogSuspendedHandling`) already
  cover the contract.

**Trade-off:** lose kernel-mediated wall-time enforcement during
daemon-down windows. Mitigated by `vq-daemon.service`'s
`Restart=on-failure` and the v0.4 orphan-reattach reconciler;
remaining gap is bounded by daemon-restart latency, ≪1 s in
practice.

**Future architecture options** (not v0.5.8 work — captured in
[`docs/wall_time_design.md`](wall_time_design.md)): per-job
systemd timer unit recreated on pause/resume; or thin Python
supervisor inside the scope; or stay watchdog-only and keep
hardening daemon availability. Worth revisiting when
`vq admin update` (v0.6) or multi-host (v0.7+) actually need
something stronger.

### v0.5.7 — CRYSTAL scratch cleanup [+ failed RuntimeMaxSec pause-aware] (2026-05-10)

**CRYSTAL wrapper cleanup:** *(this part works)*

* `contrib/run-crystal.sh` deletes per-rank `fort.<N>.pe<RANK>`
  scratch on successful (`rc == 0`) parallel runs. Real CRYSTAL
  jobs leave 200+ scratch files (~14 ranks × ~16 file types),
  each MB-to-GB sized -- without cleanup, the queue fills disk
  fast. Canonical files (`fort.9`, `fort.98`, `fort.34`,
  `fort.87`, `dffit3.dat`, `<input>.out`) preserved.
* Failed runs (`rc != 0`) keep scratch + warn user with the
  cleanup one-liner. New `--keep-scratch` flag preserves
  everything regardless. Serial mode is a no-op.
* Smoke-tested locally with a fake CRYSTAL binary that mimics
  real per-rank scratch generation.

**RuntimeMaxSec pause-aware:** *(intended fix, shipped broken;
corrected in v0.5.8 — see entry above)*

* Intent: `pause_job` sets `RuntimeMaxSec=infinity` on the job's
  systemd scope; `resume_job` sets it to
  `wall_time_seconds + paused_seconds_total`. Implementation
  invoked `systemctl --user set-property`, which silently fails
  for `RuntimeMaxSec` (not in the runtime-mutable property
  table). v0.5.8 dropped the helper + the cgroup wall-time path
  entirely.

### v0.5.6 — multi-venv routing via `--branch` (2026-05-10)

* `vq submit --branch NAME` routes to a named Python interpreter
  via two new per-host config tables:
  * `[hosts.X.branches]` — canonical branch name → interpreter
    path (e.g. `main`, `release`)
  * `[hosts.X.branch_aliases]` — alias name → canonical branch
    name (e.g. `dev` → `main`, `latest` → `release`)
  Mutually exclusive with `--python`. Single-file submit only.
  Validation at config-load time: alias targets must exist in
  `branches`; alias names cannot collide with canonical branches.
* compute-d config wires up both vibe-qc clones: `--branch main`
  (default, dev venv) and `--branch release` (release venv,
  tracking the latest tag).
* Single-line recipe table in handover for vibe-qc dev/release ×
  CRYSTAL ser/par × PROPERTIES ser/par × ORCA × PySCF × Psi4
  (placeholder).
* Manual venv-refresh procedure documented (`vq pause --all` →
  ssh compute-d → `git pull && bash scripts/update.sh` → `vq resume
  --all`) for the post-release case until v0.6's `vq admin
  update` lands.
* +16 tests (9 in test_config.py, 7 in test_cli.py).

## v0.6 — queue-managed environment refresh + multi-user

> **Status (post v0.6.4, 2026-05-17): most of the original v0.6
> design shipped during the v0.5.x sweep + the v0.6.0 lifecycle cut.**
> Only multi-user remains. The original design pin (see git history
> for the prior multi-paragraph spec) recorded what the audit-driven
> trajectory produced anyway, just in a different version-number
> shape than originally projected.

### What landed (each item points at the actual release that shipped it)

| original v0.6 design item                       | shipped in     |
|-------------------------------------------------|----------------|
| Single-env single-host `vq admin update <env>`  | v0.5.20        |
| `vq admin update --all` multi-env               | v0.5.28        |
| `--tag` verification post-pull                  | v0.5.24        |
| `vq admin update --all-hosts` fleet sweep       | v0.5.37        |
| `vq admin status` + persisted history           | v0.5.25        |
| admin-update-in-progress marker file            | v0.5.44        |
| Daemon honors marker (dispatch gate)            | v0.5.45        |
| `vq admin clear-update-marker` recovery verb    | v0.5.44        |
| `--force` flag overrides marker                 | v0.5.44        |
| Atomic marker via `O_CREAT|O_EXCL`              | v0.5.50        |
| `provides_branches` surgical pause              | v0.5.47        |
| Self-update auto-restart of vq-daemon           | v0.5.42 + .43  |
| State machine (PAUSING→…→IDLE/FAILED)           | v0.6.0         |
| State machine phase split (TAG_CHECKING/BUILDING) | v0.6.1       |
| `vq daemon start` removed (systemd-user only)   | v0.6.0         |
| `--json` output for admin verbs                 | v0.5.46        |
| `vq daemon health` lifecycle contract verifier  | v0.5.49        |
| docs/lifecycle.md (user-systemd contract)       | v0.5.50        |
| Daemon-side version-drift probe                 | v0.6.2         |
| PID-fingerprint anti-recycle at startup         | v0.5.50        |
| cgroup-scope MainPID cross-check at startup     | v0.6.0         |
| `_start_job` race fix (RUNNING before Popen)    | v0.6.0         |
| CRYSTAL23 demo support + reference systems      | v0.6.3 + .4    |

### What remains for v0.6.x+

> **Status (2026-05-21):** Multi-user backbone, per-user quotas, and
> bearer-token auth on admin verbs shipped in May 2026. The remaining
> three items are deferred or deliberately not done.


* **Multi-user backbone** — shipped in v0.6.x (May 2026).
  under `/var/lib/vq/users/<uid>/`; ownership checks on kill /
  fetch (UID matches submitter, or user in `vq-admins` group);
  system-level systemd unit + slice. Requires root on the
  target host (compute-d confirmed willing).
* **Per-user quotas** — shipped in v0.6.x (May 2026).
  per submitter. Gated on multi-user (no point until there are
  multiple users to quota).
* **Bearer-token auth on admin verbs** — shipped in v0.6.x (May 2026).
  endpoints. Today's model: if you have shell access on the
  host, you can run `vq admin update`. Fine for single-user;
  multi-user requires an explicit auth layer.
* **`VQ_FORCE_NATIVE_DEPS_CHECK=1`** env injection into the
  update_script — one-liner on the vq side; needs vibe-qc-side
  honor in `scripts/update.sh`. Deferred until release chat
  v0.8.0 prep clears.
* **systemd-timer auto-update** — shipped in v0.6.11 as the
  CLI verb `vq admin auto-update ENV` (latest-tag only).
  Operators wire their own systemd-timer or cron entry around
  the CLI; the `[admin.auto_update]` config-block + .timer unit
  ship was deliberately not done — three opt-ins (operator
  types the env, latest-tag-only, optional --dry-run) keep the
  auto-deploy surface small.
* **vq own-clone consolidation into vibeqc-dev** — long-term
  layout cleanup; deferred until vibe-qc v0.8.0 ships and
  `vq admin update` is the canonical refresh path (now true,
  so this is unblocked but still low priority).

## v0.7 — original feature roadmap (mostly shipped during the v0.5.x sweep)

> The 2026-05-10 v0.7 plan listed five features. By 2026-05-17
> four had shipped in v0.5.x because the chat that needed them
> next was the chat doing the work — no point waiting for an
> arbitrary version-number boundary. Listed here for
> traceability; only the one unshipped item remains for a real
> v0.7.

| original v0.7 item                              | shipped in     |
|-------------------------------------------------|----------------|
| Job priority (`--priority N`)                   | v0.5.29        |
| Retry on failure (`--retry N`)                  | v0.5.31        |
| Notifications (`notify_webhook`)                | v0.5.35        |
| Opt-in auto-resume after host reboot            | v0.5.30        |
| Per-user quotas                                 | **not yet** (gated on multi-user) |

### Bulk auto-resume opt-in — shipped as `vq resubmit --state STATE`

The original 2026-05-10 v0.7 spec pitched
`vq queue --resubmit-aborted-by-queue` as a flag on the
listing verb. v0.6.10 (2026-05-18) shipped this as
`vq resubmit --state STATE` instead — the verb is a write
operation, not a listing operation, so it belongs on
`vq resubmit`, not on `vq queue`. Same effect, cleaner
verb-object pairing. Composes with the v0.6.8/v0.6.9
single-job resubmit verb that this builds on.

---

## Historical roadmap snapshot (2026-06-30)

This section is retained as the v0.12-era record; it is not the current work
queue. The live package and operator-driven status are summarized immediately
below it and in `handovers/HANDOVER_VQ_DEV.md` plus the GitLab issue broker.

The **reliability audit is closed** — the v0.8.11–v0.8.25 arc landed all of
`HANDOVER_VQ_AUDIT.md` § C, every high/medium finding, 6 of 7 § A items, and
the host-pressure + resubmit-ownership parts of § B. The only audit items
left are **deferred to a Linux host**: § A RECOV-4 (pgid-recycle reattach
guard) and § B ISO-1 (cross-user read isolation, which also needs a maintainer
decision on the admin access model). Both have full analyses in the audit
handover.

With the audit done, the **v0.9 feature line opened**: `vq top` — a live
per-job CPU% / RSS / wall-time view over the watchdog samples (v0.9.0
*Tukey's Window* snapshot, v0.9.1 *Strachey's Monitor* `--watch`/`--json`) —
and v0.9.2 *Kleinrock's Queue* (a PENDING job's queue position in
`vq status`).

The **v0.12.0 scheduler-hardening line** now covers daemonless scheduler hosts
across submit/status/queue/logs/tail/fetch/fetch-all/wait/kill/resubmit/
admin-update/overview/auto-placement/top/drain/pause/resume, and adds
`vq doctor` as the read-only client preflight for config, admin-down marks,
SSH/remote-vq reachability, daemon RPC health, and scheduler-driver routing.
`vq doctor --admin-update` adds the maintenance-specific check that a
scheduler host actually has `scheduler_update_command` configured before an
operator tries to refresh it. The same maintenance preflight checks
`remote_vq --version` on the scheduler host, so pbs-cluster onboarding can verify the
scheduler-side helper after update.
Scheduler-host updates can optionally run their update/install command on a
dedicated build node via `scheduler_update_host`, while scheduler dispatch and
qstat polling stay on the normal scheduler SSH targets.
Venv fleet fan-outs (`vq admin update ENV --all-hosts` and
`vq admin auto-update ENV --all-hosts`) now skip daemonless scheduler hosts in
text and JSON instead of treating them as remote vq daemons; operators use the
explicit `vq admin update HOST` scheduler-maintenance path for those targets.
Scheduler-host `vq tail HOST JOBID --name FILE` can now one-shot arbitrary
files from a live cluster workspace through the driver; terminal scheduler jobs
fall back to the staged-back local workspace, and live arbitrary-file following
is still left to the stdout/stderr `vq logs HOST JOBID -f` path.
Scheduler-host `vq pause` / `vq resume` now use qhold/qrls for queued cluster
jobs and refuse live compute-node jobs honestly; `vq throttle` remains
not-applicable on scheduler hosts until there is a scheduler-native throttling
semantics to expose. `vq top` now reports ACTIVE elapsed runtime separately from
wall-clock ELAPSED age, subtracting `paused_seconds_total` so live monitoring
matches watchdog wall-time accounting, and adds MEM% / WALL% pressure columns
for quick limit scanning.
`vq usage HOST` now summarizes retained job CPU-hours and wall-hours by tag,
submitter, host, or total; scheduler-backed jobs prefer scheduler-reported
walltime, so pbs-cluster accounting avoids charging qsub queue wait as compute time.
`vq status` now turns v0.9.2's pending position into a best-effort dispatch-turn
ETA when retained completed-job history can estimate jobs ahead by tag, command,
and CPU count; this remains intentionally separate from scheduler-capacity or
dependency-aware prediction.
The scheduler driver now separates local-process concurrency from scheduler
submission concurrency: local `--max-jobs`, CPU, and memory gates cover only
driver-host child processes, while `--max-scheduler-jobs` optionally caps how
many cluster jobs the driver keeps submitted, queued, or running. Scheduler
polling/fetch transport also gets bounded retry and longer qstat/tar timeouts,
and status/queue output separates vq state, PBS state, and fetch state.
Scheduler hosts now accept the existing vq-managed `--array N` and `--chain N`
submit modes: the driver mints N ordinary specs tagged for the scheduler target,
so pbs-cluster batches can use array metadata and chain dependencies without depending
on PBS-native array support. Rerun metadata (`--rerun-until` / `--rerun-max`)
is preserved on every generated spec. `vq submit --program NAME` now carries a
validated `[programs.NAME]` identity into specs, remote/scheduler-driver submit
argv, `VQ_PROGRAM`, and `vq status`; scheduler jobs also receive the same
`VQ_ARRAY_*`, `VQ_CHAIN_*`, and `VQ_RERUN_*` env metadata as local jobs.
Scheduler qsub scripts now also accept host-level `scheduler_prologue` and
`scheduler_epilogue` hooks for trusted site setup/cleanup lines; full
per-program wrappers were future work at this snapshot and later shipped as
`scheduler_program_hooks.command_wrapper`.

## Current operator-driven reliability status (2026-08-20)

vq is now 0.25.2, with accepted fleet reports through v0.15.137. Feature work
is selected from live operator evidence rather than the historical candidate
list. Issue #125 closes one such reliability gap: an ordinary local job whose
CPU request or effective memory exceeds the daemon's configured base caps
retains the supported durable `PENDING` recovery path, but `vq list`, status,
and overview distinguish it from normally queued work; status does not invent a
finite dispatch-turn ETA; and `vq submit --json` carries local or forwarded
capacity warnings on the machine-readable acceptance receipt.

Issue #136 exposed a stricter trust-boundary gap on compute-d: the root-owned
system unit was active while its configuration omitted explicit multi-user
enablement, so the daemon selected the single-user dispatch path and submitted
payloads ran as root. Current source refuses every effective-uid-0 daemon
before side effects unless the full configuration validates and explicitly
enables multi-user mode. Provisioning also probes the root unit independently
of that flag and treats active-root plus readable single-user policy as an
unsafe contradiction. This is a source prevention milestone only; choosing a
supported compute-d deployment model and repairing exact legacy workspaces still
requires separate operational authority.

The separately designed privileged `/opt/vq` root-daemon apply lane remains a
review/authorization milestone, not an implied continuation from this roadmap.
Until that boundary is approved, read-only provenance and the authenticated
manual refresh contract remain authoritative.

## Candidate next features (no version assigned, maintainer-directed)

The original "long-term ideas" below all shipped. New candidates should come
from live vibe-qc workflow needs rather than this historical list.

**September 2026 console work:** the concrete remaining product milestones
are maintained in [`fleet_dashboard_design.md`](fleet_dashboard_design.md#6-roadmap).
The session/audit tranche now implements persistent session revocation,
account-change invalidation, bounded login admission, trusted HTTPS cookies
and single-host bearer-write audit receipts. OIDC, fleet-wide clear-failed,
public endpoint deployment, results/QVF browsing and live analytics remain
separate milestones. Old v0.6/v0.7 "pending" labels above are historical,
including per-user quotas (shipped) and clone consolidation (superseded by the
repository split).
## Long-term ideas — now shipped (kept for the history)

* **Job tags / metadata** — shipped v0.6.6 (`vq submit --tag`, `vq queue
  --tag`, shown in `vq status`).
* **Scheduled submits** — shipped v0.6.12 (`vq submit --at ISO8601`; daemon
  honors `spec.not_before`, added v0.5.31 for retry-backoff and reused here).
* **Resource reservation / synchronous submit** — shipped v0.6.14 (`vq wait
  JOBID` + `vq submit --wait`). Shipped "wait until terminal" (chain on the
  exit code) rather than the originally-specced "wait until dispatched".
* **SLURM-compatible CLI subset** — shipped v0.6.15 (`vq sbatch` / `squeue` /
  `scancel` / `sacct` verb aliases). Flag names stay vq-style; full SLURM-flag
  translation was deliberately skipped to avoid a partial-compatibility
  footgun.

## Out of scope (forever, probably)

* **Distributed queue across multiple hosts** — SLURM exists. vq's niche is
  single-host informal queues; distributing across hosts is a different
  product.

* GPU scheduling -- runtime's job.
* Container orchestration -- not what vq is.
* Multi-tenant / cross-organization queues.

---

## How to update this file

When a feature ships:
1. Move its bullet from "Next" / "v0.X" up to **Done** with the version
   tag and commit hash.
2. If it changed scope mid-flight, leave a note explaining what
   shipped vs. what was originally planned.

When something is deferred:
1. Note it in the relevant section with a one-line "moved to vY.Z
   because ...".
