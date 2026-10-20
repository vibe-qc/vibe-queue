# Operating the public fleet snapshot

**Audience: operators.** Part of [Running a host](operator/index.md).

This is the P3 operator runbook for #66. It deploys the existing static writer;
it adds no vq server or listener. The
[public field envelope and opt-in tags](web.md#public-fleet-snapshot-66) are
unchanged. The authenticated console remains a separate private application.
The commands below are examples for an operator-authorized deployment, not
steps performed by a source update.

## Separate the writer from the public server

Use the existing fleet operator account for the writer, with its established
vq entry point, configuration, state roots and SSH credentials. A fresh empty
state directory would make a local coordinator appear to have an empty queue;
do not create one just for this timer. Verify the service sees the same fleet
as an interactive `vq overview` before publishing its output.

Run the static server under a different unprivileged account. Give it read and
traverse access to the publication directory, and no write access. It must have
no access to the operator's home, configuration, SSH agent, keys, daemon socket,
queue state, console authentication files or private logs. An account boundary
alone does not prevent network access to a loopback console. Use a separately
isolated network environment with no route to the console or fleet, or a
separate frontend host receiving only the two public artifacts. Do not add a
proxy route from this public endpoint to the authenticated console.

The examples use `/srv/vq-public` as the publication root. Substitute locally
chosen account/group names for the uppercase placeholders; keep those choices
in private operator configuration. An administrator can provision it once:

```bash
sudo install -d -o WRITER_USER -g PUBLIC_GROUP -m 0750 /srv/vq-public
```

The serving account belongs to `PUBLIC_GROUP`; it cannot write that directory.
Check traversal permissions on every parent directory as that account. Do not
make the operator's home readable to solve a publication permission problem.
The writer creates final files at mode 0644 regardless of its umask, but does
not set the directory's serving permissions. Provision the directory before the
first run: if `--out` does not exist, the writer creates it itself, and under the
unit's `UMask=0077` that directory is 0700, which the serving account cannot
enter. The symptom is a 403 from the static server, not a vq error. Keep configuration, logs, backups
and temporary operational evidence outside this root.

Prefer a local filesystem with atomic same-directory rename. The writer
replaces `fleet.json` first and `index.html` second; the pair is not an atomic
generation. A write failure can leave a new JSON file beside an older HTML
page. Each file is complete and carries its own observation time. A second-host
publisher must copy only these exact two files after successful generation,
stage them beside their destination and rename them there. Do not copy the
whole operator directory or use in-place writes. The same mixed-generation
limitation applies unless the site adds a reviewed generation switch.

## Run one bounded writer on a timer

The following Linux user units run as the fleet operator. Replace `ExecStart`
with the installed stable vq launcher or wrapper that already selects the
correct operator configuration/state; `%h/.local/bin/vq` is only an example.
Create `~/.config/vq/public-snapshot.env` as a private file (mode 0600), holding
any environment overrides that launcher needs, including the established state
and config roots. It may be empty when the launcher supplies them. Do not copy
secrets into a unit file or this repository. A user manager does not inherit
all environment variables from the shell where a unit file was written.

Save as `~/.config/systemd/user/vq-public-snapshot.service`:

```ini
[Unit]
Description=Write the public vq fleet snapshot

[Service]
Type=oneshot
EnvironmentFile=%h/.config/vq/public-snapshot.env
ExecStart=%h/.local/bin/vq web public-snapshot --out /srv/vq-public
UMask=0077
TimeoutStartSec=5min
StandardOutput=journal
StandardError=journal
```

Save as `~/.config/systemd/user/vq-public-snapshot.timer`:

```ini
[Unit]
Description=Refresh the public vq fleet snapshot

[Timer]
OnStartupSec=1min
OnUnitInactiveSec=5min
AccuracySec=1s
RandomizedDelaySec=15s
Unit=vq-public-snapshot.service

[Install]
WantedBy=timers.target
```

The timer schedules the next run after the previous service becomes inactive.
An already-active service is not started again, so this unit has one writer.
Do not combine it with a second cron job or direct concurrent CLI invocations.
Trigger manual refreshes through the same service. These are standard
[systemd timer semantics](https://www.freedesktop.org/software/systemd/man/latest/systemd.timer.html).
The user manager must run while the operator is logged out; arranging that
(for example, with an administrator-approved lingering user manager) is a
separate host setup step.

Validate the edited units, run one writer, and inspect both files before
enabling the timer:

```bash
systemd-analyze --user verify ~/.config/systemd/user/vq-public-snapshot.service \
    ~/.config/systemd/user/vq-public-snapshot.timer
systemctl --user daemon-reload
systemctl --user start vq-public-snapshot.service
systemctl --user status vq-public-snapshot.service
journalctl --user -u vq-public-snapshot.service -n 50
# After the first output and serving-boundary checks pass:
systemctl --user enable --now vq-public-snapshot.timer
systemctl --user list-timers vq-public-snapshot.timer
```

Five minutes is a starting interval, not a measured fleet requirement. Measure
sweep duration with slow/unreachable targets and set the timeout, interval and
`stale_after_seconds` together. The freshness window must cover sweep time,
interval, jitter and any publication transfer, with room for retries. A killed
or timed-out writer may leave a temporary file or a mixed-generation pair;
serve only the exact final filenames. Keep writer failures in private logs and
alert on them. Do not replace failed output with an invented healthy snapshot.

## Serve exact files behind TLS

Use a dedicated static virtual host, distinct from the console hostname. The
following Nginx fragment belongs in its `http` context; it assumes the separate
serving account/network isolation above is already configured. Domain and
certificate paths are reserved examples and must be replaced by the operator.
Do not include application proxy locations or a broader document root here.

```nginx
limit_req_zone $binary_remote_addr zone=vq_public_rate:10m rate=5r/s;

server {
    listen 443 ssl;
    server_name status.example.org;
    ssl_certificate /etc/ssl/example/fullchain.pem;
    ssl_certificate_key /etc/ssl/example/privkey.pem;
    root /srv/vq-public;
    types { text/html html; application/json json; }
    default_type application/octet-stream;
    autoindex off;
    disable_symlinks on;
    server_tokens off;
    limit_req zone=vq_public_rate burst=10 nodelay;
    limit_req_status 429;

    add_header Cache-Control "no-store" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header Referrer-Policy "no-referrer" always;
    add_header Content-Security-Policy "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'" always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    location = / {
        limit_except GET { deny all; }
        try_files /index.html =404;
    }
    location = /index.html {
        limit_except GET { deny all; }
        try_files /index.html =404;
    }
    location = /fleet.json {
        limit_except GET { deny all; }
        try_files /fleet.json =404;
    }
    location / { return 404; }
}
```

Exact [Nginx locations](https://nginx.org/en/docs/http/ngx_http_core_module.html#location)
limit the published paths; `limit_except GET` also permits HEAD. Other paths,
including `/api/`, `/login`, dotfiles and directory contents, have no route.
The page needs inline CSS but no scripts, network resources or forms. The
[headers](https://nginx.org/en/docs/http/ngx_http_headers_module.html#add_header)
apply to error responses too. The example's
[request limit](https://nginx.org/en/docs/http/ngx_http_limit_req_module.html)
is an initial operator setting; size edge connection and resource limits for
the expected audience. Configure certificate renewal and validate with
`nginx -t` before reloading the dedicated frontend. HSTS belongs only on a
hostname that will remain HTTPS. DNS, certificates and firewall rules remain
operator changes, not vq installation effects.

## Watch freshness, not just HTTP success

An HTTP 200 can serve an old file indefinitely. The HTML prints observation
and stale-after times; it contains no code that changes appearance as time
passes. Monitor the **served** `fleet.json`, require schema `vq.public-fleet/1`,
parse its timezone-aware `generated_at` and `stale_after`, and alert when the
current time reaches the latter. Also alert on unreadable/invalid output,
future observation times, writer failures and stalled transfer to a separate
frontend. Check that the HTML timestamp advances too: a fresh JSON file alone
does not prove the second rename succeeded. A previous schema or malformed
response is a failed monitor, not a healthy empty fleet.

The public states are coarse: `up` means the host answered the sweep, not that
its daemon is ready; a busy scheduler with no known CPU budget can show
`active`. A named but unsampled host shows `unknown`. None of these cards is
an admission or scientific-health verdict. Use authenticated operator tools
for diagnostics. A single inaccessible host can become a down card; a failed
whole sweep leaves the previous output to age. Do not put the private error
text into a public fallback page.

## Upgrade, rollback and backup

Stop the timer and let an active oneshot finish before changing its launcher,
configuration or publication path. Record the installed source/version and
keep the prior public pair with its original timestamps in external operator
storage. Validate a new writer in a separate operator-owned directory, review
its schema/allowlist and render both pages, then run the production writer and
resume the timer. Use the supported vq update process for the installed runtime;
this runbook does not authorize changing the daemon or fleet.

A writer rollback must still understand `vq.public-fleet/1`. Restore the known
launcher/configuration, regenerate a snapshot and resume the timer. If the
writer cannot run, retaining an old pair is an explicitly stale fallback;
never edit its timestamps to make it current. If publication must be withdrawn,
disable the public virtual host rather than relying on a stopped timer: it
leaves already-published files available. Copies already fetched cannot be
recalled by removing a job's opt-in later.

Public artifacts are reproducible output. Back up private writer configuration,
units and deployment settings separately; SSH credentials stay in the existing
secret/credential backup system, never in the publication root. Keep service
and proxy logs private too. For the authenticated console, preserve its live
account store, signing secret, session database and audit records in their
existing locations during a normal compatible upgrade. This timer does not
read or rewrite those stores. Follow the
[console backup and rollback rules](fleet_console.md#upgrade-rollback-and-backups):
restoring old authentication state is not a session-preserving upgrade and
requires invalidating sessions as documented. Do not publish a console backup
beside `fleet.json` or promise session preservation when restoring old state.

## Operator acceptance before publication

Record this evidence privately after deployment is separately authorized:

1. The timer runs as the intended operator with the correct fleet/config/state;
   two consecutive successful runs advance both files and survive logout/reboot.
2. The serving identity can read the two final files but cannot write their
   directory or read fleet credentials, queue state, console auth or logs. Its
   network boundary prevents access to the console and fleet SSH endpoints.
3. The real HTTPS endpoint returns the intended content types, headers and
   timestamps. `/api/v1/fleet`, `/login`, a nonexistent path and temporary-file
   names are refused; POST cannot perform a write. The static server has no
   console upstream. Inspect the rendered page and alias/display-name choices.
4. A disposable failure exercise preserves old output, triggers the freshness
   alert and recovers on the next success. Exercise JSON/HTML write failure
   separately because the pair is not atomic. Do not deliberately break live
   credentials or interrupt production work to test this.
5. Rehearse rollback with retained fixtures, verify monitoring notices stale
   output, and confirm ordinary writer upgrades leave authenticated console
   sessions and audit storage untouched.

Source tests cover projection and writer behavior. They do not prove a site's
account isolation, rename semantics, TLS configuration or end-to-end freshness.
