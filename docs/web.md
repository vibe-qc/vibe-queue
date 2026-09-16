# vq web dashboard -- access + auth

## Quick reference

| Endpoint | Auth | Default access |
|---|---|---|
| `GET /queue` | none † | localhost-only by default |
| `GET /jobs/<jobid>` | none † | localhost-only by default |
| `GET /health/{live,ready}` | none | localhost-only by default |
| `POST /api/v1/jobs/<jobid>/{kill,pause,resume}` | bearer token | localhost-only by default |
| `GET /docs` (OpenAPI) | none † | localhost-only by default |
| `GET /fleet`, `/fleet/jobs`, `/fleet/jobs/<jobid>` (fleet mode only) | none, or a login once accounts exist | localhost-only by default |
| `GET /api/v1/fleet`, `/api/v1/fleet/jobs` (fleet mode only) | none, or a session/bearer once accounts exist | localhost-only by default |

† **Since v0.25.0**, when the app runs in fleet mode *and* at least one
console account exists, these single-host routes need the same session
cookie (or the bearer token) as the fleet pages. Outside fleet mode, or
with no accounts configured, they stay open -- the documented tunnel
posture. `/static` and `/health/*` are never challenged, so the login
page can render and supervisors can keep polling.

## Fleet console mode (`--fleet`)

`vq web run --fleet` adds the **fleet console** on top of the single-host
dashboard. Installing, configuring and operating it is
[`fleet_console.md`](fleet_console.md); design + roadmap are
`fleet_dashboard_design.md`. What it adds:

* `/fleet` -- one card per configured host: reachability, daemon health,
  vq version, queue counts, cpu/memory capacity, drain / throttle /
  admin-update badges. Administratively-down hosts (`vq host down`)
  render as unprobed `admin down` cards, matching `vq overview`.
* `/fleet/jobs` -- every job across the fleet in one table, filterable
  by host, state, tag, submitter, and free text.
* `/fleet/jobs/<jobid>` -- per-job drill-down (spec, decoded exit,
  stdout/stderr tails, event timeline, recent watchdog resource
  samples), fetched **live** from the host that owns the spec at
  request time -- scheduler jobs are read on their `scheduler_driver`,
  exactly like `vq status` / `vq events`.
* `/fleet/doctor` -- the `vq doctor --all` checks as a status board.
  A sweep dials every host, so it runs only on demand (button,
  30 s debounce), never on a timer.
* `/api/v1/fleet` + `/api/v1/fleet/jobs` + `/api/v1/fleet/doctor` --
  the same data as JSON.

The data comes from a **background sweep thread** in the web process:
per configured host, the `vq overview` gather plus the queue listing
(local reads on the console host, `vq queue localhost --json` over SSH
elsewhere, scheduler hosts read on their `scheduler_driver`). Pages
serve from the cached snapshot (its `gathered_at` is shown), so a slow
or hung SSH fan-out never blocks a request; `VQ_WEB_FLEET_INTERVAL`
(seconds, default 30, floor 5) sets the sweep cadence.

Run fleet mode **only on a host with SSH reach to the fleet** (normally
the coordinator) -- not in every per-host sidecar. Without `--fleet`
(or `VQ_WEB_FLEET=1` for service units), the app is exactly the
single-host dashboard: no fleet routes, no SSH fan-out.

### Fleet console accounts + roles (M2)

By default the fleet pages are open like the single-host dashboard
(loopback bind + SSH tunnel posture) and **write actions are
disabled**. Creating the first account turns login on for every fleet
surface:

```sh
vq web user add example_admin --role admin       # interactive hidden prompt
vq web user list
vq web user remove example_admin
```

Roles: `viewer` (read everything), `operator` (kill / pause / resume
from the job detail page), `admin` (operator + the `/fleet/audit`
trail). Accounts live in `web-users.json` (scrypt hashes, mode 0600)
next to the web token; sessions are signed HMAC cookies (12 h) with the
secret auto-created at `web-session-secret`. JSON APIs accept the
session cookie **or** the existing bearer token (admin-equivalent) so
agents keep working.

Every login attempt and write action appends one record to
`fleet-audit.jsonl` under the console host's state dir: ts, user, role,
action, jobid, host, outcome. `/fleet/audit` (admin) renders the recent
trail; `/api/v1/fleet/audit` serves it as JSON.

Write actions are executed on the host that owns the spec (scheduler
jobs on their `scheduler_driver`), via the same code paths as
`vq kill` / `vq pause` / `vq resume`, with the operator's reason
persisted as the job's `failure_reason`.

**Since v0.25.0 the login covers the whole port.** Creating the first
account also gates the single-host routes registered beside the fleet
ones -- `/queue`, `/jobs/<id>` and its stdout/stderr tails, `/api/v1/*`,
`/docs` -- with the same session or bearer token. They were open, which
on a console bound to a private overlay handed them to every peer on
that overlay uncredentialed while `/fleet` next door asked for a
password. `/static` and `/health/*` stay open. Two limits, both
deliberate: with no accounts configured nothing changes (the documented
open tunnel posture), and a plain single-host sidecar started without
`--fleet` is never gated, because it registers no login route and
gating it would lock the operator out with no way back in.

Fleet reads remain safe to expose only behind a tunnel or a
TLS-terminating reverse proxy; account gating is a step toward the
public deployment (M3), not a license to bind publicly without TLS.

Default bind is `127.0.0.1:8765`. The port moved from 8080 in v0.5.0 to
8765 in v0.5.1 because 8080 is too crowded; pick any free port via
`vq web run --port N` if 8765 also clashes for you.

For a dashboard that lives with the queue daemon instead of a shell session,
start the daemon with the web sidecar:

```sh
vq daemon run --web --web-host 127.0.0.1 --web-port 8765
```

The sidecar runs the same `vq web run` service as a child of the daemon and is
checked on every daemon loop. If the web child exits while the daemon stays
alive, the daemon logs the exit code and starts a fresh sidecar. The sidecar is
stopped when the daemon exits. Keep `--web-host` loopback-only unless a reverse
proxy owns authentication and TLS.

On macOS, run the launchd template from the vibe-queue checkout root when
the dashboard should survive shell or Codex task exits:

```sh
mkdir -p ~/Library/LaunchAgents
vq daemon launchd-plist \
  --python "$PWD/.venv/bin/python" \
  --working-directory "$PWD" \
  --max-cpus 18 --max-jobs 4 --max-mem-mb 104858 \
  --web-port 8768 \
  --output ~/Library/LaunchAgents/com.vq.daemon.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.vq.daemon.plist
launchctl enable gui/$(id -u)/com.vq.daemon
launchctl kickstart -k gui/$(id -u)/com.vq.daemon
```

The template starts `vq daemon run --web --web-host 127.0.0.1 --web-port 8768`,
so the queue and dashboard are restarted together by launchd.

> **Audit note -- non-loopback binds leak metadata.** Unless the console
> runs in fleet mode with accounts configured (see above), the read-only
> HTML pages and the OpenAPI `/docs` endpoint have *no auth* (the bearer
> token only gates the write endpoints). Binding to a non-loopback
> address (`0.0.0.0`, a LAN IP, a public IP) without a fronting TLS
> reverse proxy exposes every job's name, working directory, stdout /
> stderr tails, host metadata, and queue state to anyone who can reach
> the port. Since v0.6.45, `vq web run` prints a loud stderr warning on
> any non-loopback bind; pass `--i-understand-public-bind` to silence
> it once you've confirmed there's a reverse-proxy ACL in front. The
> default localhost bind + SSH tunnel pattern below remains the
> recommended deployment.

## Reach the dashboard from your laptop (SSH tunnel)

If the daemon + web service run on a remote box (e.g. compute), the
read-only HTML pages are bound to localhost on that box and aren't
visible to your laptop unless you tunnel the port:

```bash
ssh -L 8765:localhost:8765 compute
# then in a browser on the laptop:
# http://localhost:8765/queue
```

This is the recommended path: no firewall changes on compute, no public
exposure of the unauthenticated read-only pages. Tunnel stays open as
long as the ssh session does.

If 8765 is taken on the laptop, pick a different *local* side:

```bash
ssh -L 9876:localhost:8765 compute
# browser: http://localhost:9876/queue
```

The remote port stays 8765 (or wherever the service is bound on
compute); the local side is whatever's free for you.

## Set up bearer-token auth (required before write actions work)

The first time you want to use the write endpoints (`vq kill` /
`vq pause` / `vq resume` *over HTTP* -- the local CLI doesn't need
this), generate a token on the host running `vq-web`:

```bash
ssh compute
vq web init-token
# wrote token to /home/USER/.config/vq/web-token (mode 0600)
#
# Use this header in API calls:
#   Authorization: Bearer <your-token>
#
# Example (kill a job):
#   curl -X POST -H 'Authorization: Bearer <your-token>' \
#     http://localhost:8765/api/v1/jobs/<jobid>/kill
```

The token file is mode 0600. vq refuses to read it if the mode is
wider; fix with `chmod 600 ~/.config/vq/web-token`.

To rotate: `vq web init-token --force` (invalidates anyone holding the
old token).

Use the token from your laptop via the same ssh tunnel:

```bash
TOKEN=$(ssh compute 'cat ~/.config/vq/web-token')
curl -X POST -H "Authorization: Bearer $TOKEN" \
  http://localhost:8765/api/v1/jobs/abc123def456/kill
# -> "killed running job abc123def456 (SIGCONT+SIGTERM to pgid 12345)"
```

Without a token file present, write endpoints return 503 ("auth not
configured"). Read-only endpoints work without any token, unless the
console is in fleet mode with accounts configured -- then they take the
session cookie or this same bearer token (see the fleet console section
above).

### Token input channels for `vq admin update` (multi-user mode)

Multi-user `vq` deployments require a bearer token for `vq admin
update`. The token can arrive through any of four channels --
listed best-to-worst for credential hygiene:

| Channel | Argv exposure? | Best for |
|---|---|---|
| `$VQ_TOKEN` env var | none | interactive shells |
| `--token-stdin` (v0.6.46+) | none | scripted callers, automation |
| `--token-file PATH` (v0.6.46+) | path leaks, token doesn't | systemd units, CI |
| `--token TOKEN` | **token in `ps -ef` + shell history** | one-off use only |

The three CLI flags are mutually exclusive. `--token TOKEN` emits
a loud stderr warning at every use; silence with
`VQ_SUPPRESS_TOKEN_ARGV_WARNING=1` if you've weighed the trade-off.
The remote-dispatch path (when `vq admin update` forwards to
another host via SSH) automatically uses `--token-stdin` over the
SSH tunnel so the bearer never appears on argv on either side --
no operator action needed.

```bash
# Recommended (interactive):
export VQ_TOKEN=$(cat ~/.config/vq/web-token)
vq admin update vibeqc-dev --all-hosts

# Recommended (scripted):
printf '%s\n' "$VQ_TOKEN" | vq admin update --token-stdin vibeqc-dev --all-hosts

# OK (file with mode 0600):
vq admin update --token-file ~/.config/vq/web-token vibeqc-dev --all-hosts

# Discouraged (loud warning at use):
vq admin update --token "$VQ_TOKEN" vibeqc-dev --all-hosts
```

## Public exposure -- caddy reverse proxy + TLS (still not shipped as of v0.6.18)

If you want the dashboard reachable from a domain (e.g.
`vq.example.com`), put a TLS-terminating reverse proxy in front. vq
does NOT bundle this; the recipe below was originally pitched for
v0.5.2 but the priority kept being deferred for higher-impact verbs
(resubmit, wait, auto-update, scheduled submits). It's documented in
advance for operators who want it now.

### Sketch (caddy on the same host as vq-web)

```caddyfile
vq.example.com {
    reverse_proxy 127.0.0.1:8765
    header Strict-Transport-Security "max-age=31536000"

    # Run vq with fleet mode and configured console accounts, or put
    # an independent access-control layer here. Plain sidecar reads
    # are not authenticated by multi-user mode alone.

    # Logs to journal:
    log {
        output stderr
    }
}
```

What this gets you:
* Let's Encrypt certificate auto-renewed by caddy.
* TLS reachability; access still depends on the configured authentication.
* The bearer token still gates write actions, transmitted over TLS.

What this does NOT get you (yet):
* Per-user identity from the bearer token. It remains one shared
  admin-equivalent secret. Fleet mode can add separate local console
  accounts with viewer, operator, and admin roles.
* OIDC, SSO, or PAM authentication. None is implemented.

The app sets Secure session cookies when the ASGI server reports HTTPS.
Uvicorn's proxy handling must trust only the actual fronting proxy: for the
loopback sketch, keep the default `127.0.0.1` trust, or explicitly set
`FORWARDED_ALLOW_IPS=127.0.0.1` in the service environment. For a remote proxy,
configure its exact address and restrict backend traffic to it. Never use
`FORWARDED_ALLOW_IPS=*` on a reachable backend. The app does not parse raw
forwarded headers itself. Verify the resulting cookie has `Secure` through
the real HTTPS endpoint before publishing DNS. Edge connection/request limits
remain necessary; the console's persistent login budgets only bound admitted
password work. HSTS should be enabled only on the intended HTTPS hostname.

### Why bind localhost on compute behind the proxy

Two reasons: on a plain single-host dashboard `vq web` has no auth on
read endpoints (a public bind would expose every job's stdout/stderr),
and the bearer token is a single shared secret in v0.5.1 -- not enough
to call it "authenticated for public consumption." Fleet-console
accounts do gate the read endpoints from v0.25.0 on, but they are not a
substitute for TLS in front.

Multi-user mode adds OS-account ownership checks and reuses the shared
bearer token for admin operations; it does not add per-user bearer
tokens. Fleet-console accounts can gate the read side when the process
runs in fleet mode. A plain single-host sidecar still has open reads, so
the tunnel or TLS-proxy boundary remains required.

## Programmatic access for AI agents (Claude Code)

`/api/v1/...` is the agent-friendly namespace. The current shape:

```text
POST /api/v1/jobs/{jobid}/kill
POST /api/v1/jobs/{jobid}/pause
POST /api/v1/jobs/{jobid}/resume
POST /api/v1/queue/clear-failed?older_than=7d
Authorization: Bearer <token>

-> 200 with a one-line text body
-> 401 if missing/wrong token
-> 404 if no such job
-> 409 if state precludes the action (e.g. already terminal)
-> 503 if the host has no token configured yet
```

`POST /api/v1/jobs/{jobid}/kill` accepts `reason=TEXT` and
`resubmit=true` query parameters for upgrade workflows that need to terminate
an obsolete-runtime job and immediately queue a fresh pending replacement.

An HTTP submit / status / wait API was originally pitched for v0.5.2
(`POST /api/v1/jobs` with idempotency keys, `GET /api/v1/jobs/{jobid}`
status as JSON, `/api/v1/jobs/{jobid}/wait?timeout=N` long-poll).
**Still not shipped at v0.6.18** -- the priority kept slipping in
favor of higher-impact CLI verbs that scratch the same itch via
SSH:

* `vq status JOBID --json` (v0.6.14) -- machine-readable spec +
  tailed stdout/stderr.
* `vq wait JOBID --timeout SECONDS` (v0.6.14) -- synchronous wait
  with exit code reflecting the job's outcome.
* `vq submit ... --wait` (v0.6.14) -- submit + wait sugar.

For agents (e.g. Claude Code) on the laptop, the SSH path is the
canonical submit route: it inherits the laptop's SSH key auth +
the host's standard sshd hardening, and the network surface is
exactly one ssh session, not two TLS endpoints. The HTTP API
remains kill / pause / resume for the read-only dashboard's
inline action buttons.

## Health checks for monitoring

* `GET /health/live` -- always 200 when the web-console process can
  answer. It does not probe the vq daemon.
* `GET /health/ready` -- 200 only if the mode-specific daemon pidfile
  points at a live PID and the queue root exists. 503 otherwise. This
  is a lightweight pidfile check, not proof that the daemon RPC is
  responsive; use `vq daemon ping localhost` for that stronger probe.

Neither route requires auth. Their responses expose no job data, tokens,
or filesystem paths. There is no `/health/deep` self-test endpoint.
