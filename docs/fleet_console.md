# The vq fleet console

**Audience: operators.** Part of [Running a host](operator/index.md).

A read-only web dashboard for a vq fleet: one card per configured host,
a cross-host job table, and a doctor board. It runs on one machine -- the
*coordinator* -- and reaches the rest of the fleet over the SSH you have
already configured. No agent on the other hosts, no new protocol, no new
port anywhere but the coordinator.

This page is for someone setting it up on **their own** fleet. If you
want to know how the console is put together, see
[`fleet_dashboard_design.md`](https://github.com/vibe-qc/vibe-queue/blob/main/docs/fleet_dashboard_design.md).

---

## What it shows

| | |
|---|---|
| `/fleet` | One card per configured host: daemon health, whether dispatch is open, queue counts, CPU and memory, deployed program versions, and the machine's own name for itself. Plus a fleet roll-up -- configured targets and the execution resources behind them, how many running, how many unreachable, degraded daemons, and **how many distinct vq versions**, which is the number you watch during a rolling update -- and an execution-readiness summary: where each program can run now, which updates are building or failed, and which jobs the queue refused. |
| `/fleet/jobs` | Every job on every host in one filterable, sortable table, read as a calculation overview: running jobs first, how long each has run and waited, declared versus measured CPU and memory, where it executes, its program version, and how fresh each measurement is. Totals above the table keep allocation and measurement apart and say when they are partial. |
| `/fleet/jobs/<id>` | Drill-down: status, event log, output tail, resource curve, and for a finished job its final resource usage (measured wall, CPU time and average cores, peak RSS against the declared memory). For a scheduler job, a "nodes" section lists each allocated node and its cores, as the scheduler reported them: PBS/Torque `exec_host` (`node07/0-7+node08/0-7`, one entry per core, or PBS Pro's `node07/0*8`) or a Slurm node list (`node[01-04,07]`), whose cores are "not reported". It is the allocation, not a per-node measurement. All of this is fetched live, not from the cached snapshot. For vibe-qc and ORCA jobs, a "calculation" section: system, method/basis, energies, optimisation state and an SCF convergence plot, parsed from the same output tail. |
| `/fleet/campaigns` | Each calculation campaign whose controller runs on the console host, drawn as a graph: stages coloured by status, success and completion edges, each stage's latest job linked to its drill-down, which outputs are published and verified, and why a stage is waiting or blocked. See [Campaigns in the console](#campaigns-in-the-console). |
| `/fleet/doctor` | `vq doctor` across the fleet, on demand. |
| `/fleet/audit` | Who did what through the console (admins only). |
| `/api/v1/fleet` | The whole snapshot as JSON. |
| `/api/v1/fleet/jobs/<id>/preview?host=H` | A calculation preview parsed from the job's stdout tail: formula, method/basis, the latest SCF series, per-step energies, total energy and gap, optimisation summary, fatal error. vibe-qc and ORCA jobs; `preview` is `null` for other programs. The job's stdout is read, except that an ORCA job run through `contrib/run-orca.sh` is read from the one output file the wrapper writes (its output argument, or `<input>.out`); `source` in the response names which. |

The host grid is served from a snapshot that a background thread
refreshes on a timer (30 s by default). The job drill-down is not -- it
is read live each time, so logs and events are exactly current.

---

## Requirements

* **One coordinator host** that can `ssh` to every fleet host
  non-interactively (`BatchMode=yes` must work -- key auth, no prompt).
  Verify with `vq doctor` before you start.
* **vq installed with the web extra** on that host.
  [Clone vibe-queue](installation.md#clone), then run from its root:

  ```bash
  ./scripts/install.sh --extras web
  ```

  Without it, `vq web run` exits with `vq web requires the 'web' extra`.
  This is worth getting right at install time: a fleet that discovers
  the extra is missing tends to fix it by building a *second* vq tree
  just for the console, which is how consoles drift (see
  [Keeping it current](#keeping-it-current)).
* **A service manager**: systemd (user or system) or launchd. The
  installer detects which.

The console needs no daemon of its own. A coordinator that runs no jobs
is a perfectly normal deployment.

---

## Install

```bash
vq web install --fleet --bind 127.0.0.1 --port 8765
```

That writes a service unit, enables it, starts it, and records which vq
installed it. Add `--dry-run` first to see every file and command it
would run and change nothing:

```bash
vq web install --fleet --dry-run
```

The default is a per-user service, which needs no root privileges. If a
site deliberately uses a system-level unit, name the unprivileged account
that will run the console. Give that account dedicated config and state
directories, then pass their absolute paths during installation. The generated
unit preserves these variables; root execution by the service is refused:

```bash
# If the account does not already exist:
sudo useradd --system --create-home --home-dir /var/lib/vq-console vq-console
sudo install -d -o vq-console -g vq-console \
  /var/lib/vq-console/config /var/lib/vq-console/state
sudo env VQ_CONFIG_DIR=/var/lib/vq-console/config \
  VQ_STATE_DIR=/var/lib/vq-console/state \
  /absolute/path/to/vq web install \
  --manager systemd-system --service-user vq-console \
  --fleet --bind 127.0.0.1 --port 8765
```

Use the same `VQ_CONFIG_DIR` and `VQ_STATE_DIR` values for later `vq web`
administration and uninstall commands. If `VQ_MULTI_USER_ROOT` is set during
installation, it is preserved in the unit too. The selected account must be
able to traverse and execute `/absolute/path/to/vq`.

The settings you pass are written to the `[web]` section of your config
file, **not** baked into the service unit's command line. That is
deliberate -- see [Configuration](#configuration). The unit's `ExecStart`
is just `<path-to-vq> web run`.

### Options

| Flag | Persists to `[web]` | Meaning |
|---|---|---|
| `--bind` | `bind` | Listen address. Default `127.0.0.1`. |
| `--port` | `port` | Default 8765. |
| `--fleet` / `--no-fleet` | `fleet` | Fleet mode. Off by default. |
| `--interval` | `fleet_interval_seconds` | Seconds between sweeps. Default 30, floor 5. |
| `--title` | `title` | Header brand. Set it per fleet so two open consoles are distinguishable. |
| `--i-understand-public-bind` | `public_bind_ack` | Silence the non-loopback warning. |
| `--manager` | -- | `systemd-user`, `systemd-system`, `launchd-user`. Default: detected. |
| `--unit-name` | -- | Default `vq-web`. Change it to run two consoles on one host. |
| `--service-user` | -- | Required with `systemd-system`; must name an existing non-root account. |
| `--no-start` | -- | Install and enable, don't start yet. |

If your config already has a `[web]` section, the installer leaves it
alone and prints what it would have written, so it can never silently
overwrite a value you set on purpose.

### Removing it

```bash
vq web uninstall            # stop, disable, remove the unit
vq web uninstall --purge    # also drop the provenance marker
```

Use the same privilege level as installation for a system unit. A service
manager failure returns non-zero and leaves the unit file in place for
inspection; uninstall never reports success after a failed stop.

Your `[web]` settings and any accounts survive both -- this removes the
service, not the configuration.

---

## Session storage, login limits and API audit

Accounts and the signing secret keep their existing paths. Revocable sessions
and login counters use `web-auth/sessions.sqlite3` beside `web-users.json`:
under `$VQ_CONFIG_DIR` when set, otherwise `~/.config/vq`. The directory must
be owned by the console account and mode 0700; the database must be an owned
regular file at mode 0600. Both are created automatically. All workers of one
console must use the same local configuration directory. Separate replicas
with separate disks do not share revocation or login budgets.

SQLite serializes writers, so on a busy console the workers contend for the
store: they are separate processes, and the most contended moment is several of
them racing to create the store's tables on first use. A statement waits up to
**15 seconds** for another worker before giving up with `database is locked`.
That budget was 2 seconds, which a loaded CI runner exceeded, failing a request
that would have succeeded a moment later. Raising it costs latency only while
the lock is actually held, and the work runs in a worker thread rather than on
the event loop. A store that is genuinely wedged still surfaces an error rather
than hanging. `scripts/measure_session_lock_budget.py` measures what a given
host needs if the default ever has to be revisited.

Upgrading from stateless sessions signs every browser out once. Log in again;
the existing account format remains valid. Login and account creation limit
usernames to 128 characters and passwords to 1024 characters; an existing
account exceeding these bounds must be replaced through `vq web user`.
Forms are limited to 16 KiB. Logout then invalidates a
copied cookie across restarts, while other browser sessions remain active.
Password or role changes invalidate the account's sessions. Each account can
hold 32 sessions; a new one evicts the oldest when that limit is reached.
The maximum lifetime remains 12 hours. An unavailable session store denies
access; a failed revocation returns 503 rather than claiming logout succeeded.

Within each 60-second window, login admits at most 10 attempts per account,
20 per client address and 200 across the console. Successful logins count too.
HTTP 429 includes `Retry-After`; wait that interval before retrying. Restarting
the service does not clear a limit. Behind a proxy, client addresses must come
from the ASGI server's explicitly trusted proxy configuration. An untrusted
`X-Forwarded-For` or `X-Forwarded-Proto` header is not used by the application.
Requests served as HTTPS set Secure cookies; see the TLS sketch in `web.md`.

Single-host bearer-token write routes now append `started` and outcome entries
to `fleet-audit.jsonl`, correlated by `request_id`. The identity is the shared
`bearer-token`, not an individual account. Records cover kill (including its
resubmit option), pause/resume and queue pause/resume/clear-failed. `ok` means
the handler returned normally; clear-failed can still report skipped jobs.
HTTP errors and unexpected exceptions receive failure outcomes without their
potentially sensitive text. Credentials and query values are not retained.
If the start record cannot be written, the request returns 503 before acting.
If only the outcome append fails, the operation's response is preserved and
the server logs the request ID. A start without an outcome requires checking
job state before considering a retry.

### Upgrade, rollback and backups

The SQLite store is console-local authentication state, not queue job state.
Old builds ignore it and still accept signed stateless cookies. Before a
rollback, stop all console workers and rotate `web-session-secret` so a
previously revoked cookie cannot become valid on the old build. That signs
out every browser. Never mix old and new web workers behind the same endpoint.

Keep account-store backups private. Restoring an old session database can
restore an old session, so stop all workers, omit the session database and
rotate the signing secret when restoring accounts. Every browser must then
log in again. Removing the database during recovery also resets rate counters;
it is not a routine way to get around a login limit. A full supported
backup/restore command remains a separate roadmap item.

## Configuration

Everything lives in the `[web]` section of vq's config file
(`~/.config/vq/config.toml`, or `$VQ_CONFIG_DIR/config.toml`):

```toml
[web]
bind                   = "<overlay-ip>"   # e.g. your WireGuard address
port                   = 8765
fleet                  = true
fleet_interval_seconds = 30
title                  = "acme fleet"
log_level              = "info"
public_bind_ack        = true
```

**Precedence, highest first:**

1. a command-line flag on `vq web run`
2. an environment variable
3. the `[web]` section
4. the built-in default

Every layer is validated by the same rules, so a value the config file
would reject cannot sneak in through the environment either.

To see what is actually in effect, and *which layer set each value*:

```bash
vq web config
```

```
vq web — resolved configuration
  (precedence: CLI flag > env > [web] in config > default)

  bind                    <overlay-ip>             config [web]
  port                    8765                     default
  fleet                   True                     config [web]
  fleet_interval_seconds  30                       default
  log_level               info                     default
  title                   acme fleet               config [web]
  public_bind_ack         True                     config [web]

  URL: http://<overlay-ip>:8765/
```

### Environment variables

`VQ_WEB_BIND`, `VQ_WEB_PORT`, `VQ_WEB_FLEET`, `VQ_WEB_FLEET_INTERVAL`,
`VQ_WEB_LOG_LEVEL`, `VQ_WEB_TITLE`, `VQ_WEB_PUBLIC_BIND_ACK`. Useful for
a container or a one-off; prefer the config file for a real deployment,
because `vq web config` can show it to you and the file is validated.

An unparseable value (`VQ_WEB_PORT=notanint`) is ignored and the next
layer applies -- `vq web config` will show the value's real source, not
blame the variable.

---

## Accounts and access

**With no accounts configured, the console is open.** Every page is
readable by anyone who can reach the port, and write actions (kill,
pause, resume) are disabled. That is the intended posture for a console
bound to loopback and reached through an SSH tunnel.

Creating the first account turns authentication on for **the entire
port** -- the fleet pages *and* the single-host pages beside them:

```bash
vq web user add alice --role admin
```

Roles: `viewer` (read), `operator` (+ kill / pause / resume), `admin`
(+ the audit trail).

Scripted readers can use the bearer token instead of a session:

```bash
vq web init-token
curl -H "Authorization: Bearer $(cat ~/.config/vq/web-token)" \
     http://127.0.0.1:8765/api/v1/fleet
```

### Binding beyond loopback

The read surface exposes job names, working directories, stdout/stderr
tails, host metadata and queue state. Before binding anywhere but
loopback, do **both** of:

1. **Create at least one account.** Otherwise the bind is an
   unauthenticated read surface for everyone who can route to it.
2. **Put TLS in front of it,** or keep the bind on a private overlay
   (WireGuard, Tailscale) that is itself the perimeter. The console
   speaks plain HTTP and does not terminate TLS.

`vq web run` warns loudly on a non-loopback bind until you pass
`--i-understand-public-bind` or set `public_bind_ack = true`. The
warning is about exposure, not about the bind being unsupported.

**To show fleet status to the public, do not bind the console publicly.**
Write a static snapshot with `vq web public-snapshot --out DIR`, and serve
`DIR` from a separate unprivileged account. Only hosts you give a public
display name appear, and only calculations submitted with a `public.<alias>`
tag. The console stays on loopback. See
[`web.md`, "Public fleet snapshot (#66)"](web.md#public-fleet-snapshot-66)
and the [operator deployment runbook](public_snapshot_deployment.md).

---

## Keeping it current

**A stale console is the failure mode this deployment path exists to
prevent, and it is invisible unless you look for it.** Every other vq
surface runs whatever is installed right now: a CLI verb cannot drift,
and a drifted daemon eventually misbehaves in a way somebody notices. A
console that is running old code just serves the past, in the present
tense, on pages that look completely normal.

Three things guard against it:

1. **`vq web install` points the unit at the vq that ran it** and
   records the version, path and time in
   `<config-dir>/web-console-install.json`.

2. **`vq web status`** compares that record against the vq installed
   now:

   ```
   service:      vq-web (systemd-user)
   installed by: vq 0.24.0
   running vq:   0.25.0
   active:       yes

   ⚠️  drift: this console service was installed by vq 0.24.0, but vq
       0.25.0 is installed now. The running console is serving the older
       code.
       Fix with:  vq web install
   ```

3. **The console audits itself.** On every page it compares its own
   version against the vq daemon on the same host and shows a banner
   when they disagree. It never guesses: no daemon reachable means no
   comparison and no warning.

**So: after upgrading vq on the coordinator, re-run `vq web install`.**
It is idempotent. Nothing else restarts the console for you -- a fleet
rollout updates the *code*, and a running console keeps serving the copy
it imported at startup.

---

## Reading the fleet page

A few things on the card are worth explaining, because they encode
distinctions that are easy to miss.

**The version chip is never blank.** It shows one of:

* `vq 0.24.0` -- the version that host's vq reported.
* `vq helper c1f568717` -- a daemonless scheduler host (PBS, SLURM). It
  has no vq of its own, but it runs a helper, and the helper's skew from
  its driver is worth seeing. This used to render as nothing at all,
  which was indistinguishable from agreement.
* `vq version unknown` -- genuinely unknown. Said out loud rather than
  left blank.

**The header chip says `console v0.24.0`.** That is the version of the
process rendering the page, not a fleet fact. Hover for the interpreter
path and start time.

**"calls itself &lt;name&gt;"** appears when a host's own hostname differs
from the `[hosts.<key>]` name you gave it. Usually harmless -- an alias,
a short name vs an FQDN. Sometimes it is the symptom of a config key
that means different machines on different hosts (`localhost` is the
classic), or of the same machine enrolled twice.

**A duplicate-enrolment banner is a real defect, not cosmetic.** Two
keys naming one machine means both are swept, both render, and every job
on that machine is counted twice in the jobs table, the totals and the
JSON API. vq will not silently collapse them, because it cannot know
which key you meant to keep.

**"n vq versions"** is the rolling-update number. One means converged.

**A stale-snapshot banner means the sweep has stopped.** The page polls
every 10 s regardless, so it keeps animating even when the background
sweep has died -- age is the only honest signal. The threshold follows
your configured interval.

## Reading execution readiness

The panel under the tiles answers "can calculations run now, and where?"
from the same snapshot as the cards. It adds no probe. Each number keeps
apart things the cards alone blur together.

**Configured targets are not machines.** A target is a `[hosts.<key>]`
entry. *Execution resources* counts what is behind them: a daemon host is
the machine that answered, by the name it reports for itself, so two keys
for one machine count once; a scheduler lane is its cluster, and lanes
that log in through the same SSH destination are one cluster. A target
that did not answer cannot be identified (it may be a second key for a
machine that did), so the tile shows `+?` and the panel says how many.

**Daemon counts are per daemon.** A scheduler lane has no daemon of its
own; its card shows its driver's health as `driver daemon ok` or
`driver daemon degraded`. The `daemons degraded` tile counts the driver
once, not once per lane. `daemon health unknown` counts daemons that
did not answer or did not report health.

**Dispatch is open, held or unknown, per target.** It is held when the
snapshot proves a hold the daemon obeys: `vq host down`, a drain that
covers the target, or an admin-update lease whose scope holds it. It is
unknown when the target was not observed, its daemon is degraded, or its
health was not reported; vq does not guess either way. `open` means
nothing in the snapshot holds the target. It is not a promise that a
particular job will start: quotas, memory pressure and each job's runtime
pin are checked only at dispatch.

**Updates sit beside the current runtime, not in place of it.** For each
program the table lists where its current runtime is eligible (with the
version the host reports), and separately any update lease naming it:

* `building` with *current runtime unaffected* -- a replacement runtime
  slot, or a scheduler runtime, is being built beside the one in use.
  Jobs keep dispatching on the current runtime.
* `update failed` or `needs recovery` with *current runtime unaffected* --
  the replacement did not finish. The current runtime is still eligible;
  the row carries the action the updater recorded (for example
  `vq admin recover-update`).
* any state with *dispatch held* -- an update that does hold the target,
  such as a venv rebuild on the host itself. The program is listed as
  held there, with the lease as the reason.

Mixed vq versions across the fleet are a rollout state. They never make
a target held or a program ineligible on their own.

**Admission failures name the program, target and reason.** A job is
listed when the queue refused it and nothing of the same program has been
admitted on that target since:

* `dispatch refused` -- the daemon failed it at dispatch, before it
  started (for example a runtime pin it would not run). A job that ran
  and exited non-zero, or failed because a dependency failed, is not an
  admission failure.
* `scheduler rejected` -- the scheduler refused the submission before
  assigning a job ID.
* `over capacity` -- a pending job that requests more than the target's
  configured caps, so it cannot dispatch as configured.

The program shows as `refused` on that target; the target itself stays
open for other programs.

**Partial means unknown, not idle.** A target that was not observed, is
administratively down, or whose job listing failed is named in a
*Partial* banner. Its calculations are missing from every total and its
programs are listed as unknown or held there, never as eligible. *Live
calculations* counts each job once per owning daemon, so a job on a
scheduler lane is not charged to its driver as well, and a duplicate
enrolment does not double it. A job still queued in the cluster counts as
queued, never as running.

## Reading the jobs table

`/fleet/jobs` answers which calculations run where, for how long, and with
what resources. Three kinds of value never share a cell: what a job
**declared** (`cpus`, `mem MB`, its time limit), what was **measured**, and
what is **unknown**. A missing measurement shows `-` with the reason in the
sample column; it is never a zero and never the declared value.

| Column | Meaning |
|---|---|
| runtime | Execution time. A direct job: since dispatch, minus paused time. A scheduler job: the walltime the scheduler reported, unknown until it reports one, because vq's start time for it is the hand-off to the scheduler and can include the cluster's queue wait. A finished job prefers its final command receipt's measured walltime (the largest element's for an array); without one it keeps the direct or scheduler duration. After a `/` the limit: the scheduler's, else the declared `wall_time_seconds`. |
| wait | Queue wait, never counted as runtime. `waiting` and growing while the job is queued; submission to dispatch for a started direct job; unknown for a started scheduler job, whose wait inside the cluster queue is not recorded. |
| cpus / cpu used | Declared cpus, then measured core-equivalents: the job's whole process group CPU% over the last watchdog interval, divided by 100. An 8-core job at full use reads about 8. A finished job shows its final receipt's average busy cores, marked `avg`; for an array this is the per-element average, not all elements' concurrent use. |
| mem MB / rss MB | Declared memory limit, then the resident memory of the job's processes in the latest watchdog sample. |
| peak MB | The highest resident memory the watchdog's samples have shown for this run; hover for when tracking began. It is a *sampled* peak: a spike shorter than the sampling interval (about 5 s) is not in it. It survives a daemon restart, but a retry starts a new peak. For a running Slurm job the column instead shows `sstat`'s peak RSS of the job's largest task, marked `task`: one task's peak since the job started, not the job's memory. A finished job shows its final receipt's largest single-process peak, marked `process`; for an array it is the largest peak over the elements. This is not total job memory or current RSS. Peaks are never added up, since different jobs peak at different times, and a host whose vq does not record peaks shows `-`. |
| nodes | Where it executes: a direct job's host, or the compute node(s) the scheduler reported (unknown until it starts). A job spread over several nodes reads `N nodes`; hover for each node and, for PBS/Torque, its allocated cores. Slurm's node list names nodes only, so their cores are not shown, and a string that does not parse is shown as reported. `via` names the scheduler driver that owns the job, which is not where it runs. |
| software | The program, with its recorded version or build when the job pinned one. |
| sample | The measurement's age, or why there is none: `stale` (over 30 s old at display time, or already marked stale by the collector), `age unknown` (missing/invalid timestamp or one ahead of the console clock), `no sample` (none yet, or the host cannot sample, e.g. no `/proc`), `scheduler, <age> ago` (a running scheduler job: for PBS/Torque, memory and CPU from the scheduler's own `resources_used`; for Slurm, CPU from `sstat` and the largest task's peak in the peak column, with no current memory; CPU is shown as average busy cores over the run so far rather than a current reading, and none of it is added to the fresh-sample totals), `not sampled` (a scheduler job whose scheduler has reported no usage yet, or whose Slurm accounting is disabled; its accounting arrives with the results), `collector error`, `unknown` (an older snapshot without measurements). |

Default order is running jobs first, then live scheduler jobs whose phase is
unconfirmed, then held or paused, then queued, then finished; newest first
within each. A scheduler job vq has handed off but the cluster still queues
counts as queued, not running. Filters (host, state, program, project, tag,
owner, text) and sorting are in the URL, so they survive the refresh.

The program selector includes live and last-known jobs. A selected program
stays available when its last matching job disappears or the snapshot is
empty, so applying another filter preserves the program selection.

The state column, state filter and state sorting use the snapshot's effective
execution state. A scheduler job waiting in the cluster queue is `queued`,
and an unavailable scheduler observation is `unpolled`, `poll_failed` or
`scheduler_unknown`; none matches `state=running`. Hover over a state to see
the underlying vq lifecycle. Older rows without an effective state retain
their lifecycle state. Last-known rows use the same selection, and a chosen
state stays selected when its last matching job disappears. The jobs API
uses the same filter and preserves both raw `state` and `effective_state`.

Projects use exact, case-sensitive `project.NAME` tags attached at submission,
for example `vq submit localhost --tag project.water-scan calculation.py`.
The project selector lists names from live and last-known jobs. A job can
belong to several projects by carrying several such tags; ordinary tags do
not imply a project. Jobs without a project tag still appear with all projects
selected. These labels use the existing submission metadata, including on
older hosts.

The owner field matches the recorded submitter exactly and suggests known
values, including from last-known jobs. That value can be a numeric UID in
multi-user mode or a `user@host` label; it is not a console account. Project,
owner and the other filters combine with AND. They also apply to the
authenticated jobs API (`?project=water-scan&submitter=alice`) and the
last-known table, whose rows stay out of live totals. A project selection is
retained even if its host disappears from the snapshot.

The **submitted from** and **submitted through** filters bound submission dates
in UTC, with both dates inclusive. Either bound may be omitted. For example,
`?state=failed&submitted_from=2026-10-01&submitted_to=2026-10-15` selects failed
jobs submitted during that interval. Offset timestamps are converted to UTC;
legacy timestamps without an offset are treated as UTC. Jobs with missing or
invalid submission times match only when no date bound is set. Invalid dates
and reversed ranges return HTTP 400 rather than dropping the filter. Date
bounds combine with the other filters on the page, refresh fragment, jobs API
and last-known rows. They select from the existing cached snapshot; they do
not fetch expired or archived history from hosts.

**Previous** and **Next** page through all matching jobs in the cached snapshot,
including finished history. The rows-per-page selector bounds each page; the
page links preserve the filters, sort and row limit. The URL's one-based `page`
also survives the background table refresh. Applying filters or changing sort
starts at page 1. If a newer snapshot has fewer pages, the table falls back to
the last available page. New or removed jobs can shift page boundaries between
sweeps. Counts and resource totals cover all matches, and last-known jobs stay
in their separate table. This pagination applies to the HTML table; the jobs
API continues to return all cached jobs matching its filters.

Finished rows show `final` in the sample column (with the element count for
an array), using the same validated receipts as the job detail page. `no receipt`,
`invalid receipt` and `collector error` keep unavailable values as `-`. An older
host that does not report final usage shows `unknown`. Final measurements never
age into stale live samples, and never contribute to live totals or trends.
The background sweep receives summaries in its existing batched queue listing;
rendering and refreshing the table make no individual job requests. Receipt
reads run on the owning host with the existing file-size and array-count bounds.

The totals cover every job matching the filters, not just the visible
page. Allocated sums what running jobs declared. Used sums only fresh
samples, marked `*` and itemised when some running job contributed none.
RSS totals add each job's resident memory, so pages shared between jobs
count once per job. GPUs are allocation only: the `gpus allocated` tile sums
what running scheduler jobs requested with `--gpus`, and the cpus column
shows a job's `+N gpu`. vq has no GPU collector yet, so their use, VRAM and
model are not shown.

Freshness is checked on every page/table refresh, even if the background sweep
has stopped. Last-known numbers remain visible for stale or undated samples,
but neither contributes to fresh usage totals. A recent collection timestamp
does not establish the age of an undated sample from an older remote version.
The authenticated `/api/v1/fleet/jobs` response applies the same rules to
`resource_observation.status` and includes `age_seconds` (unknown is `null`).
Its original `sample_at` and `collected_at` timestamps remain unchanged;
`age_unknown` explicitly identifies readings whose freshness cannot be proved.

A red banner names each host whose queue could not be read this sweep
(unreachable, failed listing, or administratively down). Its jobs are
missing from the live table and every total until a sweep reads it again.
An amber banner names a driver whose resource samples could not be read;
its jobs are listed with `collector error`.

**Trends and per-host load.** Under the totals, three sparklines draw this
console's recent sweeps: running jobs; CPU, with measured cores (solid)
against what running jobs declared (dashed); and measured RSS in GiB.
They cover up to the last 240 sweeps, about two hours at the default
interval, kept in the console's memory only, so a restart starts them
afresh. Measured values come from samples fresh at each sweep. A dot under
a point marks a partial sweep: a running job had no fresh sample, or a host
was not listed. A dip there is missing data, not idle work, and a sweep
with no value at all breaks the line rather than drawing a zero. GPUs have
no trend yet.

Below them, one row per host gives:

* running jobs;
* the CPUs they declared against the daemon's configured budget;
* the cores measured by fresh samples, marked `*` with the coverage when
  some running job had none;
* measured RSS against the host's total and currently free memory.

A scheduler lane reads `not sampled`, since its jobs run on compute nodes the
watchdog does not see. A host whose queue was not listed reads
`not listed in this snapshot: unknown, not idle`. The host filter narrows
this table; the other filters do not, since it describes hosts rather than
jobs.

**Last known.** When an unlisted host was listed by an earlier sweep of this
console, the jobs it showed then appear in a separate *Last known* table
under the live one, with the time of that listing. They are not current:
they may have finished, failed or been requeued since. They are in no
count or total, their runtime is as of the listing rather than growing,
and their measured usage is not shown. Another failed sweep keeps the
original listing time; a successful listing replaces them; after 24 hours
they are dropped. The filters apply to them too. They are kept in the
console's memory only, so a console restart forgets them, and keeping them
costs no request to any host. `/api/v1/fleet/jobs` returns them under
`last_known` (per host: `host`, `observed_at`, `reason`, `jobs`), outside
`jobs` and `count`; `/api/v1/fleet` gives each host's `last_known_count`
and `last_known_at`.

Measurements come from the same rows as `vq top --json`, read once per
driver per sweep, and only from a driver that runs a direct job: locally on
the console host, `vq top localhost --json` over SSH elsewhere. There is no
request per job. The watchdog samples about every 5 s on the daemon host;
the displayed age is relative to the page render, not to the sweep.

---

## What a refresh costs

The background sweep reads each host once per refresh. For a daemon host it
makes one queue listing, one overview, and one `vq top` if the host runs a
direct job. A scheduler target shares its driver's reading. Page views and
their filters, sorting and paging read the cached snapshot and contact no
host. The number of remote calls therefore follows the number of hosts, never
the number of jobs.

`scripts/measure_fleet_console.py` measures this on a synthetic fleet. It runs
the console's real sweep and its real jobs table. Only the SSH transport is
replaced: each remote call is answered with the JSON the real command prints,
after a simulated round trip. It reports the calls per sweep, the sweep
durations, the render times of a few table requests, the machine, and its
load:

```sh
PYTHONPATH=src .venv/bin/python scripts/measure_fleet_console.py \
    --hosts 8 --jobs 250 --latency-ms 80 --sweeps 5
```

Measured with vq 0.26.44 on a 12-core arm64 macOS workstation, at a 1-minute
load average of about 6 from other work. Every workload had 80 ms of
simulated latency per remote call, 30 % of jobs running directly, and 5
sweeps:

| Workload | Rows | Remote calls per sweep | Sweep (median) | Default table, 200 rows | `state=running`, 50 rows | Search, page 2 of 100 | 2000 rows |
|---|---|---|---|---|---|---|---|
| `synthetic-8h-250j-80ms` | 2000 | 24 (8 + 8 + 8) | 0.29 s | 13 ms, 0.26 MB | 7 ms | 13 ms | 66 ms, 2.5 MB |
| `synthetic-8h-1000j-80ms` | 8000 | 24 (8 + 8 + 8) | 0.42 s | 34 ms | 25 ms | 45 ms | 100 ms, 2.6 MB |
| `synthetic-16h-250j-80ms` | 4000 | 48 (16 + 16 + 16) | 0.56 s | 20 ms | 12 ms | 23 ms | 77 ms, 2.5 MB |

Read it as follows:

* Four times the jobs per host left the calls unchanged. The sweep grew by
  the time taken to decode and project the larger listings.
* Twice the hosts doubled the calls. The sweep took about two rounds, because
  it reads at most 8 hosts at once and three sequential 80 ms calls set each
  host's floor.
* Rendering made no remote call in any run.

These numbers describe these workloads on that machine and nothing more. A
real fleet's SSH round trips, its remote overview gathers and its hosts' load
set its own refresh time. Run the script with your own host count, job count
and a measured round trip before relying on a figure.

## Campaigns in the console

`/fleet/campaigns` shows the [calculation campaigns](campaigns.md) that
`vq campaign run` drives on the console host as the console's own user.
Each run registers its run directory under that user's vq state
(`campaigns/<key>.json`: the campaign id and digest, and the absolute
paths of the campaign file and run directory). The page reads each
registered run's journal through the same evaluation as
`vq campaign status`, so it never disagrees with the command line. It is
read-only: retries and cleanup stay with `vq campaign retry` and
`vq campaign cleanup`.

Each campaign is drawn left to right, one column per dependency depth:

* **Stages** are boxes coloured by status: `ready`, `waiting`, `running`,
  `uncertain` (a submission whose answer was lost; the controller resends
  it under its key), `succeeded`, `failed` and `blocked`.
* **Edges** are solid for `success` and dashed for `completion`. An edge
  that skips a column runs in the lane above the boxes, so it is not
  mistaken for one through the stage in between.
* **The table below the graph** gives each stage's latest attempt, linked
  to its job's drill-down (logs and results), with the runtime the job's
  queue recorded at dispatch ("ran vibeqc-release 0.26.0 @ <sha>"), and each
  output's
  publication: `verified`, `not published`, or the verification's verdict
  (`digest_mismatch`, `missing`, …). It also lists why a stage is waiting
  or blocked and the controller's next step, and which producer outputs
  are kept for unfinished consumers.

A campaign whose file has changed since the run was registered, whose
journal cannot be read, or whose run directory is gone shows the reason
instead of a graph. Paths are shown only as the run directory's name.
Deleting a registration hides its campaign; the next `vq campaign run` for
it registers it again. `/api/v1/fleet/campaigns` serves the same as JSON,
behind the same login.

---

## Troubleshooting

**`vq web requires the 'web' extra`** -- change the managed environment's
recorded profile, then reinstall the service with `vq web install`. For an
already marked environment serving the daemon, pin the current full source
SHA to change only its profile. Replace `CURRENT_FULL_SOURCE_SHA` with that
40-character commit and use your configured environment name:

```sh
vq admin update vibeqc-queue --expected-sha CURRENT_FULL_SOURCE_SHA \
  --update-script-arg=--recreate-venv \
  --update-script-arg=--extras --update-script-arg=web
vq web install
```

The admin transaction stops the proven serving daemon, rebuilds the same
virtualenv with the requested extras, verifies provenance and restores the
old environment if the update fails. It preserves the recorded editable or
copied mode; a configured `scripts/update.sh --editable` is accepted when it
matches that record. An unmarked legacy environment still requires explicit
legacy adoption. `vq self-update` preserves the installed profile by default.
For an inactive environment, use `scripts/update.sh --skip-git
--recreate-venv --extras web --venv .venv`; the direct updater still refuses
active-environment mutation. Do not build a second tree for the console.

On macOS, service installation waits up to 15 seconds for launchd to unload
the previous label and retries transient bootstrap I/O errors within that
window. Permission and unknown service-state errors stop the install; the
provenance marker is written only after all manager commands succeed.

**The service restart-loops.** `journalctl --user -u vq-web -n 50`, or
for launchd the `.out`/`.err` files named in the plist. The usual causes
are the missing web extra and a port already in use.

**Every host shows `unreachable`.** The console's SSH is not your
shell's SSH: a service unit has a different environment and often no
agent. Confirm with `vq doctor` run *as the service's user*, and prefer
key auth with an explicit `IdentityFile` in `~/.ssh/config`.

**One host shows `unreachable` and the rest are fine.** The error text
is on the card. If it mentions `Host key verification failed`, the
service user has never accepted that host's key.

**Nothing on the page updates.** The grid refreshes via htmx, which vq
serves locally from `/static/htmx.min.js` -- it needs no internet. If the
page is static, check the browser console; if fragments 401, your
session expired (12 h) and reloading will send you to the login form.

**The snapshot is old but the sweep looks alive.** A sweep that fails
keeps the previous snapshot and logs a warning rather than blanking the
page. Check the console's logs for SSH timeouts; a single hung host
slows the whole fan-out.

**`vq web status` says no console is installed, but one is running.** It
was installed by hand, before `vq web install` existed. Re-run
`vq web install` to adopt it -- the unit is rewritten and recorded.

---

## See also

* [`fleet_dashboard_design.md`](https://github.com/vibe-qc/vibe-queue/blob/main/docs/fleet_dashboard_design.md) -- what a console must preserve.
* [`web.md`](web.md) -- the single-host dashboard, and the public fleet snapshot.
* [`hosts.md`](hosts.md) -- configuring the fleet the console displays.
* [`operations.md`](operations.md) -- day-to-day fleet operation.


## Fleet observation and ownership

Each sweep reads a driver's queue history once and shares that observation
across its scheduler aliases. Local specs are serialized once; remote queue
rows are validated once and retain fields supplied by newer drivers. Jobs keep
their owning queue handles, so equal job IDs on different drivers remain
separate jobs. Invalid rows remain diagnostic and raise a listing warning.

A daemon host's execution totals include only jobs without a scheduler target.
Scheduler cards project their own target's jobs from the shared driver
observation. Only an observed scheduler `running` phase counts as running or
busy CPUs. Queued, held, unpolled and unconfirmed reservations remain pending
capacity; unconfirmed reservations also retain their separate diagnostic count.
These projections change presentation only and never rewrite queue state.

The snapshot timestamp records the start of its observation, preserving the
age of displayed data even when a sweep is slow. The cache reports an active
refresh and its elapsed time separately from the last refresh failure. Automatic
sweeps wait the configured interval after completion; manual requests cannot
overlap an active automatic sweep. A completed slow sweep gets its duration
plus the normal missed-sweep allowance before the stale warning appears.
