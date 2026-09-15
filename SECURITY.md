# Security Policy

vq takes security seriously. This document describes how to report a
vulnerability and what is in scope.

See also: [CONTRIBUTING.md](CONTRIBUTING.md) for non-security bugs.

## Supported versions

vq is pre-1.0. Only the latest commit on `main` receives security fixes, and
nothing is backported. A supported-version table will appear here once the
release line is declared stable.

Note that a **fleet** runs pinned versions: `releases/` holds fleet release
reports and host runtimes reference them. A fix on `main` is not a fix on a
deployed host until that host is rolled. If you are reporting something that
affects deployed hosts, say so, and we will treat the rollout as part of the
fix rather than as follow-up work.

## Reporting a vulnerability

Please email **mpei@vibe-qc.com** directly. Do not open a public GitLab issue
for security-relevant reports — that includes any bug you believe could be
exploited for code execution, privilege escalation, data leakage, or resource
exhaustion beyond what the test suite would surface.

What to include in your report:

- A description of the issue and its potential impact.
- Steps to reproduce, ideally with a minimal job spec or config fragment.
- The version and commit you observed it on (`vq --version` and
  `git rev-parse HEAD`), and whether the deployment was single-user,
  multi-user (`/opt/vq`), or scheduler-backed.

Please **redact host names, addresses and account names** from anything you
send. We can reproduce from a shape; we do not need your topology.

We aim to acknowledge your report within **72 hours** and will coordinate a
disclosure timeline with you privately. Public disclosure happens after a fix
is available on `main`, unless the reporter requests otherwise.

### Encrypting your report (optional, recommended for sensitive details)

If your report contains exploit details, proof-of-concept code, or anything
you would rather not transmit in cleartext, encrypt it to the project author's
PGP key.

**Fingerprint** — `CC6D 30BB DF96 F694 C615  FBDE 4CD5 65CF 26B1 E7E5`

(no-space form for `gpg` and URLs:
`CC6D30BBDF96F694C615FBDE4CD565CF26B1E7E5`)

**Get the key:**

```sh
curl -O https://vibe-qc.com/docs/_static/pgp/mpei.asc
gpg --import mpei.asc
```

```sh
gpg --keyserver hkps://keys.openpgp.org \
    --recv-keys CC6D30BBDF96F694C615FBDE4CD565CF26B1E7E5
```

After importing, **always verify the fingerprint matches the canonical value
above** before trusting the key — paste-jacking and MITM at fetch time are
real concerns. The fingerprint is a hash; a tampered-with key produces a
different one.

```sh
gpg --fingerprint CC6D30BBDF96F694C615FBDE4CD565CF26B1E7E5
# Should print: CC6D 30BB DF96 F694 C615  FBDE 4CD5 65CF 26B1 E7E5
```

**Encrypt and send:**

```sh
gpg --encrypt --armor --recipient mpei@vibe-qc.com \
    --output report.asc report.txt
# Then attach report.asc to an email to mpei@vibe-qc.com
```

The same fingerprint is published in vibe-qc's and vibe-view's `SECURITY.md`.
If they ever disagree, that is itself a security signal worth flagging — email
the address above (unencrypted is fine for that meta-report).

## Threat model

vq's threat model is **execution by design**. Unlike a parser or a viewer,
vq's entire purpose is to take a command a user supplied and run it, often on
another machine, sometimes as another user. So the interesting questions are
never "can input cause execution" — the answer is yes, that is the product.
They are:

1. **Whose** command runs, and **as whom**?
2. Can a caller reach a job, a workspace, or a control operation that is not
   theirs?
3. Can a supervised daemon be made to run something its operator did not
   install?

A finding is in scope if it answers one of those the wrong way.

**In scope** — code under this repository:

- **Authentication and authorization.** `src/vq/auth.py` (the shared bearer
  token, its 0600 file-mode refusal, the constant-time compare),
  `src/vq/web/authn.py` (scrypt password hashes, the HMAC-signed session
  cookie, the `viewer < operator < admin` role order),
  `src/vq/ownership.py` (multi-user uid and admin-group checks on
  kill / fetch / resubmit).
  - Note the documented default: with **no users configured, fleet-console
    authentication is off** and write actions answer 503. That is the
    intended Phase A tunnel posture, not a vulnerability. A way to reach a
    *write* action in that state is.
- **The web dashboard.** `src/vq/web/` routes and templates: template
  injection, missing auth on a mutating route, a read-only endpoint that
  leaks another user's spec or log, a bind address that escapes loopback
  without the warning firing.
- **Dispatch and transport.** `src/vq/transport.py`, `ssh_probe.py`,
  `dispatch.py`, `scheduler_dispatch.py`, `scheduler_dialect.py`: argument
  and path quoting into remote shells, anything that lets a job spec inject
  into an `ssh` or scheduler submission command line.
- **Job specs and workspaces.** `src/vq/spec.py`, `spec_access.py`,
  `build_job.py`, `fetch.py`, `paths.py`: path traversal out of a workspace,
  a fetch that writes outside its destination, a spec field that escapes its
  serialization.
- **Privileged lifecycle.** `src/vq/admin.py`, `lifecycle.py`,
  `provision.py`, `auto_update.py`, `scripts/*.sh`,
  `contrib/deploy-multi-user.sh`, `contrib/vq-multi-user-refresh*` and the
  shipped systemd units and sudoers fragment: privilege boundaries, the
  lifecycle lock, ownership and mode checks, and any path by which
  `vq self-update` could be steered to install something other than the
  accepted release report.
- **Resource containment.** `src/vq/cgroup.py`, `capacity.py`, `throttle.py`,
  `watchdog.py`: an escape from a configured cgroup, or a job that can starve
  a host past its declared caps.

**Out of scope:**

- **The workload itself.** If a user submits a malicious command, vq runs it.
  That is the contract. Sandboxing user workloads is not a vq feature and is
  not claimed anywhere.
- **A trusted operator's configuration.** `config.toml` is a trusted input:
  it names interpreters, hooks and command wrappers by design. Someone who
  can write it can already run code as that user.
- **Bugs in upstream dependencies** — report to those projects directly:
  [click](https://github.com/pallets/click),
  [pydantic](https://github.com/pydantic/pydantic),
  [FastAPI](https://github.com/fastapi/fastapi),
  [Starlette](https://github.com/encode/starlette),
  [uvicorn](https://github.com/encode/uvicorn),
  [Jinja2](https://github.com/pallets/jinja).
- **OpenSSH and the cluster schedulers** vq talks to. vq's *use* of them is in
  scope; their own defects are not.

Not yet implemented, and therefore not a finding: per-user bearer tokens,
OIDC, and PAM authentication. `src/vq/auth.py` documents the shared-secret
posture deliberately. A report that the shared bearer is not a per-user
identity tells us something we have written down; a report that it can be
*bypassed* does not.

### Console session and write-audit boundary

Fleet local-account cookies are signed and require a matching record in the
private `web-auth/sessions.sqlite3` database beside the account store. Logout
removes that record; changing a password or role invalidates its account
fingerprint. Missing/corrupt state denies sessions. Old stateless cookies are
not migrated. Sessions expire after 12 hours and are capped at 32 per account.

Login admission is shared across workers and restarts: within a 60-second
window, at most 10 attempts per account, 20 per client address and 200 for the
console reach password hashing. Successful logins also count. HTTP 429 supplies
`Retry-After`; rejected attempts do not extend the window. This bounds password
work, not denial of service: an attacker can temporarily exhaust a shared
account/address budget. A fronting proxy must bound connections and request
traffic too. Raw forwarded headers never establish trust in application code.

Secure cookies follow the ASGI request scheme. Configure only trusted proxy
addresses at the ASGI server, restrict direct backend access and use TLS at the
edge. Do not trust forwarded headers from arbitrary clients.

Authenticated single-host API writes retain start and outcome audit records
under one request ID, with the shared identity `bearer-token`. A start-record
failure refuses execution. A missing outcome after a crash or append failure
means the result needs investigation, not that replay is safe. These records
do not identify a person or provide the future tamper-evident audit chain.

If you are unsure whether a finding is in scope, email it to the address above
and we will triage together.
