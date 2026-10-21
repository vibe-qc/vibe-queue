# Submission gateway (#163)

The gateway lets an authorized laptop, collaborator or automation service
outside the fleet network submit and follow calculations over HTTPS with an
API key, without fleet SSH access. It is a separate, opt-in service on the
[broker](design_broker.md) host. It submits through the broker's intake, so it
holds no SSH keys or fleet inventory of its own. Read its trust boundary in
`SECURITY.md` ("Submission gateway boundary") before enabling it.

**Status:** complete MVP, slices 1 to 5:
- identity: keys, policy, authentication and audit;
- resumable uploads and submission through the broker, with status and
  listing;
- paged logs and cancellation;
- result collection and downloads;
- the `vq remote` client.

## The `vq remote` client

On the client machine, put the gateway URL and your key in an owner-only file:

```sh
mkdir -p ~/.config/vq
cat > ~/.config/vq/gateway-credentials.toml <<'TOML'
url = "https://gateway.example"
key = "vqk_..."
# ca_file = "/path/to/private-ca.pem"   # if the gateway uses a private CA
TOML
chmod 600 ~/.config/vq/gateway-credentials.toml
```

The client refuses a file others can read, and plain `http://` to anything but
loopback. The key is never accepted on the command line. `--credentials FILE`
or `$VQ_GATEWAY_CREDENTIALS` selects another file.

```sh
vq remote submit ./h2o --program vibeqc-release --cpus 4 --mem-mb 8000 \
  --wall-time 3600 --idempotency-key paper-run-0042 -- python3 run.py
vq remote status b-0123456789ab
vq remote logs b-0123456789ab --follow
vq remote fetch b-0123456789ab ./results
vq remote cancel b-0123456789ab
```

- **`submit`** packs a directory, or takes a `.tar`, and uploads it in 8 MiB
  chunks, resuming after an interruption. It then submits. Network failures
  and gateway 502/503/504 are retried with backoff. Retrying, or rerunning the
  command with the same `--idempotency-key`, returns the original job: a
  dropped response never creates a second one. Directories containing links
  or special files are refused.
- **`logs --follow`** polls until the job is cancelled or its results are
  ready.
- **`fetch`** downloads every manifest file and checks each against its
  SHA-256. It refuses to overwrite files unless given `--force`.

The HTTP API below is what the client uses; any HTTP client works the same
way.

## Trusted group only

A key selects a **project** and its policy; it does not select an OS account.
Every gateway job runs under the broker's execution account and its targets'
configured accounts, and uploaded scripts can run arbitrary code there. Issue
keys only to people and services you would let run jobs as that account.

## Configure

On the broker host, enable the broker first, then add the gateway and one
section per project:

```toml
[gateway]
enabled = true
bind = "127.0.0.1"      # behind a TLS reverse proxy (default)
port = 8770
# tls_cert = "/etc/vq/gateway.crt"   # or terminate TLS here
# tls_key  = "/etc/vq/gateway.key"

[gateway.projects.group-a]
programs = ["vibeqc-release"]
max_cpus = 16
max_mem_mb = 65536
max_wall_time_seconds = 86400
max_active_jobs = 10     # per principal
storage_quota_mb = 1024  # per principal
result_retention_days = 14
```

`vq gateway serve` refuses to start without `[gateway] enabled` and `[broker]
enabled`. It also refuses a plaintext bind on a non-loopback address: bind
loopback behind a reverse proxy that terminates TLS, or set both `tls_cert`
and `tls_key` (absolute paths).

## Keys

```sh
vq gateway key create --principal alice --project group-a \
  --scope submit --scope read --scope cancel --scope download
vq gateway key list
vq gateway key rotate <key-id>
vq gateway key revoke <key-id>
```

`create` and `rotate` print the key, `vqk_<id>_<secret>`, **once**. Store it in
the client's credential file. vq keeps only an HMAC of the secret under a
private random pepper, in `<state root>/gateway/` (owner-only). Keys expire
after 90 days by default (`--expires-days`). Every request re-reads the store,
so a revoked or expired key fails on its next request. Rotating issues a key
with the same grant and remaining lifetime, then revokes the old one.

Scopes are `submit`, `read`, `cancel` and `download`. Limits apply per
principal and project, so issuing a second key does not raise them.

## Use

Clients send `Authorization: Bearer vqk_...`; keys in URLs are never read, and
the console's admin token is not accepted.

```sh
curl -fsS https://gateway.example/v1/healthz
curl -fsS -H "Authorization: Bearer $VQ_GATEWAY_KEY" https://gateway.example/v1/whoami
```

`/v1/healthz` is unauthenticated and returns only `{"ok": true}`.
`/v1/whoami` returns the presented key's own ID, principal, project, scopes and
expiry.

## Submit a calculation

A job's payload is an uncompressed tar. vq runs its command in the extracted
directory on whichever target the broker places it on. Upload it first: declare
its size and SHA-256, send it in chunks, then finalize it.

```sh
H="Authorization: Bearer $VQ_GATEWAY_KEY"
size=$(stat -c %s payload.tar); sum=$(sha256sum payload.tar | cut -d' ' -f1)
id=$(curl -fsS -H "$H" -H 'Content-Type: application/json' \
  -d "{\"size\": $size, \"sha256\": \"$sum\"}" \
  https://gateway.example/v1/uploads | jq -r .upload_id)
curl -fsS -H "$H" -X PUT --data-binary @payload.tar \
  -H "Content-Range: bytes 0-$((size - 1))/$size" https://gateway.example/v1/uploads/$id
curl -fsS -H "$H" -X POST https://gateway.example/v1/uploads/$id/finalize
```

A chunk must start at the byte count the gateway reports
(`GET /v1/uploads/{id}` → `received`), so an interrupted upload resumes where
it stopped. Each chunk is at most 64 MiB. Finalizing checks the digest and
accepts only plain files and directories, with no absolute paths, `..` or
links, within the project's storage quota.

Then submit it under your own idempotency key:

```sh
curl -fsS -H "$H" -H 'Content-Type: application/json' https://gateway.example/v1/jobs -d '{
  "upload_id": "'$id'", "command": ["python3", "run.py"],
  "program": "vibeqc-release", "cpus": 4, "mem_mb": 8000,
  "wall_time_seconds": 3600, "idempotency_key": "paper-run-0042"}'
```

**Responses:**
- 202 with the job ID once the broker has durably accepted the job. It is
  queued at the broker, not yet running.
- Repeating the identical request returns the same job with 200 and
  `"replayed": true`.
- Reusing the key for different content is a 409.

**Refusals:**
- 403: a program or resource outside the project policy.
- 400: unknown fields such as `uid` or a host.
- 429: too many of your jobs are active (`max_active_jobs`). A job counts
  while it waits at the broker, and once placed until its results are
  collected.

`GET /v1/jobs` (cursor `?after=`) and `GET /v1/jobs/{id}` show your jobs:
state, timestamps, program, resources, attempts, whether the job is placed,
and short waiting reasons. They never show the command, payload or the fleet
host it went to. Another principal's job IDs read as not found.

## Logs and cancellation

```sh
curl -fsS -H "$H" "https://gateway.example/v1/jobs/$job/logs?stream=stdout&offset=0"
curl -fsS -H "$H" -X POST https://gateway.example/v1/jobs/$job/cancel
```

Logs (`read` scope) return one window of stdout or stderr: `data`,
`next_offset` and the stream's current `size` in bytes. Pass `next_offset` back
as `offset` to continue, at most 1 MiB per call (`limit`). A job still waiting
at the broker returns `"placed": false` and no data. Output is read on the
job's host each time, up to 64 MiB per stream.

Cancel (`cancel` scope) can be repeated. A job not yet placed is cancelled at
once. A placed job is also killed on its host. `"stopped": true` means it can
no longer run. `"stopped": false` means its transfer was in doubt: it may have
reached a host. In that case the broker's operator resolves it; the gateway
never resubmits.

## Results

```sh
curl -fsS -H "$H" https://gateway.example/v1/jobs/$job/results
curl -fsS -H "$H" -o out.txt https://gateway.example/v1/jobs/$job/results/out.txt
```

With the `download` scope, `GET /v1/jobs/{id}/results` answers
`"ready": false` (with the job's state) until the job's host reports it
finished. The first call after that collects the job's workspace once. Later
calls return the same manifest: each file's `path`, `size` and `sha256`,
plus any `skipped` entries.

Only regular files are kept. Symbolic links and unsafe paths are skipped and
listed by name. Download a file by its exact manifest path. The response
carries its SHA-256 in `X-Content-SHA256`: compare it before using the file.
Collected results count against your storage quota, and outputs larger than
what remains of it are refused (413).

On the gateway host, `vq gateway prune` (or `--dry-run`) removes collected
results and uploads older than their project's `result_retention_days`. Run
it from a timer.

## Relay mode: a gateway outside, a connector inside (#164)

When the broker host cannot accept inbound connections, run the gateway in
relay mode on a host clients can reach, and a connector on the broker host
that dials out to it. The relay gateway needs no broker, SSH keys or fleet
inventory.

On the reachable host:

```toml
[gateway]
enabled = true
mode = "relay"
# plus [gateway.projects.NAME] sections, as above
```

```sh
vq gateway connector enroll --project group-a > token   # one use, 10 minutes
```

On the broker host, with `[broker]` enabled:

```toml
[connector]
enabled = true
gateway_url = "https://gateway.example"
# ca_file = "/etc/vq/gateway-ca.pem"
```

```sh
vq connector enroll https://gateway.example < token
vq connector run          # long-polls the gateway; outbound HTTPS only
```

**What clients see in relay mode:**
- **Job IDs:** jobs get `g-` IDs, derived from the idempotency key.
- **`transfer`:** `queued` until the connector claims the job, then
  `claimed`, then `accepted` once it is admitted to the broker. `refused`
  carries a `refused_reason`, for example a payload that failed its digest.
  `uncertain` means the connector will retry it.
- **`state` and `placed`:** come from the connector's latest report, with
  its time in `status_as_of`.
- **`connector_fresh`:** whether the connector reached the gateway in the
  last 90 seconds.

A connector that goes offline leaves admitted work running. Nothing is assumed
finished, and nothing is resubmitted.

`POST /v1/jobs/{id}/cancel` works in relay mode too:
- A job not yet claimed by the connector is cancelled at once and never
  delivered.
- For an admitted job, the answer's `cancellation.state` is `requested` until
  the connector has run the kill on the broker host. It then becomes
  `stopped`, or `not-stopped` if the transfer was in doubt.
- `"stopped": true` appears only once that is settled.

Logs in relay mode come from the connector. Your first
`GET /v1/jobs/{id}/logs` asks for them. Within about five seconds the
connector reads the job's output on the broker host and pushes the new bytes.
Later calls page through what has arrived: `size` is what the gateway holds,
and `as_of` is when it last received output. Keep calling, or use
`vq remote logs --follow`, and the gateway keeps asking for 10 minutes after
each request. At most 16 MiB per stream is buffered at the gateway.

Results in relay mode are collected by the connector:
- Your first `GET /v1/jobs/{id}/results` asks for them, and `collection.state`
  shows `requested`.
- If the job is still running, the connector reports `not-finished`. Ask
  again later.
- Once it has finished, the connector pushes each file and the gateway checks
  it against the manifest. Then `ready` is true and downloads work exactly as
  in local mode.
- Results larger than your remaining storage quota end in `failed`.

`vq remote fetch` handles all of this.

The connector stages payloads and collected results in at most
`[connector] spool_mb` (default 4096) of the broker host's disk. Work that
would not fit waits at the gateway, and job views show
`"connector_backpressure": true` until space frees up.

One connector serves a project. To replace it, run `vq gateway connector
revoke ID`, then enroll a new one. `vq connector rotate` replaces its key
without downtime.

Run `vq connector run` under a supervisor on the broker host, for example a
user systemd unit or launchd agent with restart-on-failure. It backs off while
the gateway is unreachable. A connector restart never loses or duplicates
work: unacknowledged items are claimed again, and the broker returns jobs it
already admitted.

## Audit

`<state root>/gateway/audit.jsonl` records one line per request: time, key ID,
principal, project, method, path and status. It never records keys, headers
or payloads.
