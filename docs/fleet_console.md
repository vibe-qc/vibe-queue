# The vq fleet console

A read-only web dashboard for a vq fleet: one card per configured host,
a cross-host job table, and a doctor board. It runs on one machine -- the
*coordinator* -- and reaches the rest of the fleet over the SSH you have
already configured. No agent on the other hosts, no new protocol, no new
port anywhere but the coordinator.

This page is for someone setting it up on **their own** fleet. If you
want to know how the console is put together, see
[`fleet_dashboard_design.md`](fleet_dashboard_design.md).

---

## What it shows

| | |
|---|---|
| `/fleet` | One card per configured host: daemon health, queue counts, CPU and memory, deployed program versions, and the machine's own name for itself. Plus a fleet roll-up -- how many hosts, how many running, how many unreachable, and **how many distinct vq versions**, which is the number you watch during a rolling update. |
| `/fleet/jobs` | Every job on every host in one filterable, sortable table. |
| `/fleet/jobs/<id>` | Drill-down: status, event log, output tail, resource curve -- fetched live, not from the cached snapshot. |
| `/fleet/doctor` | `vq doctor` across the fleet, on demand. |
| `/fleet/audit` | Who did what through the console (admins only). |
| `/api/v1/fleet` | The whole snapshot as JSON. |

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

* [`fleet_dashboard_design.md`](fleet_dashboard_design.md) -- architecture and milestones.
* [`web.md`](web.md) -- the single-host dashboard.
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
