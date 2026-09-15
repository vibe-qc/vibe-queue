# Compute hosts — what to record about one, and why

**Audience: operator.** This page is about the *shape* of a vq compute host:
what you need to know about a machine before you point a queue at it, and
what changes about vq's behaviour when each of those facts changes.

For HOW to submit / monitor / fetch, see
[`chat-onboarding.md`](chat-onboarding.md). For the SSH setup story (key
generation, sshd hardening, fail2ban, router port-forwards) see
[`remote-access.md`](remote-access.md). For the design contract see
[`SPEC.md`](SPEC.md).

## Where the real inventory lives

The filled-in table for this project's own fleet is **not in this
repository**, and must not come back into it. It lives in the private
`mpei/scripts` project, as `hosts.md`.

That is not tidiness. A populated host inventory is an attack surface
document: it pairs a resolvable public hostname with an external SSH port,
a login account name, a static LAN address, and — in this project's case —
the `fail2ban` `ignoreip` range, which is precisely the netblock that is
*exempt* from brute-force banning. Published together, those turn "somebody
scanned my port" into "somebody knows which port, as whom, and from where
they would not be banned".

`.githooks/pre-commit` blocks private IPv4 literals from re-entering this
tree, and `tests/test_no_maintainer_paths.py` re-checks the whole tree in
CI. Neither can block a hostname. That part is on you.

**If you are writing an example here, invent one.** `compute.example.com`,
`myuser`, `~/.ssh/compute_ed25519` — the convention
[`config.toml.example`](config.toml.example) already uses.

## What a host record contains

Each row below is a fact vq's scheduling actually consumes. A record that
omits one is a record that cannot answer "why did that job go there?".

### Identity and reachability

| Field | Why vq cares |
|---|---|
| SSH alias | The only name vq knows. `[hosts.NAME] ssh = "alias"` is resolved by `~/.ssh/config`, never by vq. |
| Reachability route | Whether the alias goes over the LAN, over the internet, or through a jump host. vq does not model this; `~/.ssh/config` does. |
| Break-glass route | A second alias that bypasses the normal path, for when the normal path is what broke. |
| Login account | Job ownership, workspace paths, and every multi-user check key off it. |

vq stays transport-agnostic on purpose: ports, hostnames and key paths live
in `~/.ssh/config`, so nothing in vq changes when a host moves onto the
internet. See [`remote-access.md`](remote-access.md).

### Capacity

| Field | Why vq cares |
|---|---|
| Cores / threads | The `--max-cpus` ceiling. Set it to what you are willing to give away, not to what `nproc` reports. |
| RAM, and swap | Admission refuses a job whose declared footprint does not fit. Swap is not RAM; a job that swaps has already lost. |
| L3 cache | Not consumed by vq, but it is the usual reason two nominally similar hosts differ by 2x on the same job. |
| Daemon caps | `--max-cpus` and `--max-jobs` as actually deployed. `--max-jobs` is a soft cap so one bad job cannot starve the rest; the CPU budget alone usually allows more. |
| Interactive use | A workstation someone is sitting at needs headroom the numbers do not show. |

### Accelerators

| Field | Why vq cares |
|---|---|
| GPU model and VRAM | The binding constraint for GPU workloads is almost always VRAM, not compute capability. |
| Compute capability | Whether a prebuilt wheel will run at all. |
| Driver and toolkit version | Prebuilt CUDA wheels bundle their own runtime and need only the driver; anything that compiles needs the toolkit. |

A caveat worth writing down once per host: **an interactive shell may not
have the toolkit on `PATH`** even when the toolkit is installed, because a
non-login or non-`/etc/profile`-sourcing shell skips the profile drop-in
that adds it. vq builds remote commands as non-login shells, so a toolchain
that works when you SSH in by hand can still be missing under `vq admin
update`. This has bitten this fleet more than once.

### Engine inventory

The registered programs (`vq programs HOST`) and their paths. Two rules:

- **Record what is missing and why.** An engine absent by design and an
  engine absent by accident produce the same dispatch failure and want
  opposite responses. `vq submit HOST engine` fails at dispatch either way.
- **Pre-registered entries are fine.** An entry whose path does not exist
  yet flips to OK on its own once the build lands; that is better than
  forgetting to register it.

### Asymmetries

The section people actually read. For every pair of hosts, the three or four
sentences that decide which one a job goes to: which has the engine, which
has the memory, which has the accelerator worth using, and which one has a
known operational trap.

## Regenerating a record

Run on the host:

```bash
lscpu | grep -E "Model name|Core|Thread|MHz|cache:"
free -h | head -2
lspci | grep -iE "vga|3d|nvidia|amd|radeon"
nvidia-smi --query-gpu=name,driver_version,memory.total,compute_cap --format=csv
vq programs                                       # engine inventory
systemctl --user cat vq-daemon | grep ExecStart   # daemon caps
```

## When a record needs updating

Whenever hardware changes, an engine is added or removed, daemon caps
change, an SSH alias or port-forward changes, or a host joins or leaves the
fleet.

A retired host is the case most often missed. It does not stop appearing in
a config, a runbook or an inventory by itself, and a stale row for a machine
that no longer answers costs a debugging session every time somebody trusts
it. Retire the row in the same change that retires the host.

If an inventory is more than about three months stale, regenerate it rather
than patching it.


## Retiring a host with retained rollout evidence

Remove a permanently retired machine from `[hosts]`, pools, rollout groups,
and SSH configuration. Its historical jobs, reports and rollout journals stay
available. A retained host fence needs a separate explicit audit declaration;
it does not need a placeholder active host.

First print the bindings for the local retained evidence:

```sh
vq host retirement-audit retired-worker
```

This reads local journals and prints each rollout ID and its host evidence
digest. It does not authorize retirement. After the maintainer's retirement
decision, record that decision and the exact returned bindings in the VQ config:

```toml
[fleet.retired_hosts.retired-worker]
retired_at = "2026-09-12T17:00:00+00:00"
reason = "Permanently retired after hardware failure"
authorization_reference = "Maintainer retirement decision in the issue tracker"

[fleet.retired_hosts.retired-worker.retained_receipts]
"ROLLOUT_ID_FROM_AUDIT" = "SHA256_FROM_AUDIT"
```

Replace both placeholders with the command's output. Every affected rollout
needs its own binding. The timestamp must include a timezone, the reason and
authorization reference must be nonempty, and every digest must be a full
lowercase SHA-256 value. Older VQ builds reject this new `[fleet]` field;
install code supporting it before distributing the configuration.

A declaration binds the host's retained action/hold receipts and their original
journal subtrees. Planning still authenticates the historical report and each
receipt's frozen accepted-report context from the report repository. An older
context can remain historical for this explicitly retired host; live hosts
continue to require the current report or the existing explicit exclusion
rules. Edited receipts, wrong digests, unknown hosts, incomplete evidence and
unverifiable reports remain errors.

Retirement preserves unknown outcomes and host fences. It neither marks a hold
released nor marks a rollout complete. Automatic recovery and explicit legacy
reconciliation exclude authenticated retired hosts from host probes and retry
lists. Uncovered running actions and durable-operation failures still block;
a mixed live/retired legacy action group cannot be rewritten around its frozen
retirement evidence. A name cannot be both retired and active in the config.
Re-enrolment therefore requires an explicit review of that declaration and the
still-retained fences.
