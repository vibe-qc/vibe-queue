# vq — chat onboarding

For a chat new to this project. Self-contained: drop a link to this
file into a fresh chat's first message and they have everything they
need to submit + monitor + fetch jobs across the supported engines (vibe-qc,
CRYSTAL, ORCA, PySCF, Psi4) without further context.

For deeper / in-flight context — current branch, design invariants,
release coordination — see [`handover.md`](handover.md) (the chat-facing
reference for chats already in the loop) and [`SPEC.md`](SPEC.md) (the
long-term design contract). For the compute hosts'
hardware / engine inventory / network topology — which one to send a
job to and why — see [`hosts.md`](hosts.md).

---

## TL;DR

`vq` is a per-host and scheduler-backed job queue at `vibe-queue/` in this
monorepo. The CLI runs locally and delegates remote work over SSH; configured
PBS/Torque and Slurm hosts route through their scheduler driver. Topology is
configuration, not a frozen list in this page: run `vq doctor --all` and use
[`hosts.md`](hosts.md) for the current hardware and engine inventory. Engines
dispatched today include:

| Engine | What |
|---|---|
| **vibe-qc** | The in-house Python+C++ quantum-chemistry code. |
| **CRYSTAL14** | External periodic-DFT code; serial + parallel via `Pcrystal`. |
| **ORCA** | External molecular-QC code, 6.1.1. |
| **PySCF** | Inside the vibeqc-dev venv (`import pyscf`). |
| **Psi4** | External, via psi4conda. |

vq is independently versioned; the current source stamp is `vq 0.25.7`.
Sibling work is recorded in the parent CHANGELOG, and
[`version_compatibility.md`](version_compatibility.md) maps each released
vibe-qc tag to the vq version in that tagged tree.

## The chat workflow (90% of what you'll do)

```bash
vq doctor --all                                           # config/connectivity preflight
git push                                                  # your edits to vibe-qc, on main
vq admin update vibeqc-dev                                # configured default host
JOBID=$(vq submit my_test.py --branch main \
                  --cpus 4 --wall-time-seconds 3600)
vq tail $JOBID --name vibeqc.log -f                       # live-watch the SCF (Ctrl-C to stop)
vq fetch $JOBID -o ./results                              # workspace back to laptop
```

Always declare `--wall-time-seconds` (the watchdog can't bound runaway
jobs without it). Always declare `--cpus N` matching what you'll
actually use (the daemon budgets against `--max-cpus`).

## Submit recipes per engine

```bash
# vibe-qc (Python; --branch picks the venv)
vq submit run.py --branch main    --cpus 4 --wall-time-seconds 3600   # dev clone, latest commit
vq submit run.py --branch release --cpus 4 --wall-time-seconds 3600   # release-tagged clone

# PySCF (lives inside the vibeqc-dev venv — same as vibe-qc)
vq submit pyscf_run.py --branch main --cpus 4 --wall-time-seconds 1800

# ORCA (use the wrapper so ORCA stdout becomes input.out and scratch is cleaned)
vq submit -d ./calc --cpus 1 --wall-time-seconds 3600 -- \
    env ORCA_BIN=/home/USER/bin/orca_6_1_1_linux_x86-64_shared_openmpi418_nodmrg/orca \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-orca.sh input.inp

# CRYSTAL14 (use the wrapper — handles INPUT-file convention + scratch cleanup)
# NOTE: prefix `env CRYSTAL_BIN=... PCRYSTAL_BIN=...` because the
# service daemon's PATH may not include ~/bin/ (where the binaries live).
# The wrapper's env-var overrides (v0.5.19) take an absolute path and
# bypass the PATH lookup.
# Serial:
vq submit -d ./calc --cpus 1 --wall-time-seconds 7200 -- \
    env CRYSTAL_BIN=/home/USER/bin/crystal \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --serial input.d12
# Parallel example (match --np to the requested --cpus allocation):
vq submit -d ./calc --cpus 14 --wall-time-seconds 7200 -- \
    env PCRYSTAL_BIN=/home/USER/bin/Pcrystal \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --np 14 input.d12

# CRYSTAL23 demo (v0.6.3; use when the selected host registers
# crystal23demo + properties23demo). Full v23 feature set capped at 10 atoms per
# primitive cell — the ceiling is the only thing distinguishing the
# demo from a paid v23 license. Serial-only (no Pcrystal23demo ships);
# --demo + --np is rejected with a clear error.
vq submit -d ./calc --cpus 1 --wall-time-seconds 7200 -- \
    env CRYSTAL23DEMO_BIN=/home/USER/bin/crystal23demo \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --demo input.d12
# CRYSTAL23 demo + PROPERTIES23 demo (post-processing on the same cap):
vq submit -d ./calc --cpus 1 --wall-time-seconds 3600 -- \
    env PROPERTIES23DEMO_BIN=/home/USER/bin/properties23demo \
    bash /home/USER/gitlab/vibeqc-queue/vibe-queue/contrib/run-crystal.sh \
    --demo --properties propinput.d3

# Psi4
vq submit -d ./calc --cpus 1 --wall-time-seconds 3600 -- \
    /home/USER/psi4conda/bin/psi4 input.in
```

Run `vq programs` to see registered binaries + their absolute paths
(use those if the daemon's PATH is uncertain).

## Useful submit flags

| flag | what |
|---|---|
| `--cpus N` | CPU slots claimed (required practice) |
| `--wall-time-seconds N` | hard kill at N seconds; required practice |
| `--mem-mb N` | memory budget (bookkeeping + cgroup if available) |
| `--branch main\|release` | named vibe-qc venv |
| `--priority N` | dispatch ordering: higher = first (v0.5.29). Ties broken by FIFO by submission time. Does *not* preempt RUNNING jobs |
| `--auto-resume` | resubmit after a host reboot (v0.5.30). Uses the same workspace so restart-from-disk logic can pick up partial state. Your job must restart-from-disk (CRYSTAL `GUESSP=fort.20`, PySCF chkfile, ORCA `.gbw`). `recover_on_reboot` propagates so successive reboots keep resubmitting. |
| `--retry N` | re-enqueue on non-zero exit, exp backoff 10 to 20 to 40 capped 600s (v0.5.31). For TRANSIENT failures only. **Only non-zero-exit FAILED is retried** — watchdog kills (OOM_KILLED / STARVED / TIME_EXCEEDED) and vq kill (KILLED) are NOT retried. |
| `--job-name NAME` | human-readable label (v0.5.34). Shows up in `vq queue` (NAME column) + `vq status` + drives `vq fetch` dest dir (`DIR/<name>-<jobid>/`) + archive filename (`<name>-<jobid>.tar.bz2`). Charset: alnum + `-_.` only, ≤50 chars |
| `--tag TAG` | free-form label, repeatable (v0.6.6). `vq submit ... --tag experiment-12 --tag basisset-dev` attaches both. Shown in `vq status`; filter with `vq queue --tag X` (AND-semantics across multiple `--tag`). Same charset as `--job-name`. Pure operator metadata — does not affect dispatch order, scheduling, or resource accounting |
| `--at ISO8601` | scheduled submit (v0.6.12). `vq submit foo.py --at 2026-05-20T22:00:00Z` holds the job in PENDING until the timestamp. Format **requires** an explicit timezone (`Z` or `+HH:MM`) — naive timestamps are rejected to dodge laptop-vs-server tz confusion. Past timestamps treated as "ready now" |
| `--wait` | synchronous mode (v0.6.14). `vq submit foo.py --wait` blocks until the job is terminal; exit code reflects the job's outcome (0 on COMPLETED, FAILED's exit_code on FAILED, 124 on `--timeout`, 130 on Ctrl-C). Ctrl-C cancels the wait but the job keeps running |

## Watching jobs / getting output

```bash
vq queue                        # all states (archived hidden by default since v0.5.33)
vq queue --active               # every nonterminal lifecycle row
vq queue -s failed              # forensics filter
vq queue --show-archived        # include archived jobs (annotated "(archived)")
vq queue --all                  # stacked listing across every configured host (v0.5.36)
vq programs --all               # what's installed where, all hosts (v0.5.36)
vq admin status --all           # tip SHAs + update times, all hosts (v0.5.36)

vq status <jobid>               # metadata + last 50 stdout/stderr lines
vq status <jobid> -n 0          # full output

vq tail <jobid> -f                          # follow stdout.log
vq tail <jobid> --name vibeqc.log -f        # vibe-qc's Python logger
vq tail <jobid> --name mgo.out -f           # CRYSTAL output
vq tail <jobid> --name h2.out -f            # ORCA / Psi4 output

vq fetch <jobid> -o ./out       # tar the workspace back to laptop

vq wait <jobid>                 # block until terminal; exit-code = job's outcome (v0.6.14)
vq wait <jobid> --timeout 7200  # bounded wait; exit 124 if exceeded
vq status <jobid> --json        # machine-readable spec + tailed stdout/stderr (v0.6.14)
```

## Managing jobs / queue

```bash
vq kill <jobid>                       # SIGTERM the process group
vq pause <jobid> | vq pause --all     # SIGSTOP (RAM stays; not a checkpoint)
vq resume <jobid> | vq resume --all
vq throttle <jobid> --weight 20       # soft CPU priority (cgroup or renice fallback)
vq drain --max-jobs 1                 # temporarily lower the dispatch cap
```

## Rerunning + recovery (v0.6.8 / v0.6.10)

```bash
vq resubmit <jobid>                   # fresh jobid, FRESH workspace (deep-copy)
vq resubmit <jobid> --cpus 16         # per-flag override on the new job
vq resubmit <jobid> --tag retry-1     # replace tags on the new job

# Post-reboot recovery one-liner: resubmit everything the queue
# protectively aborted. stdout = new jobids (scriptable); stderr =
# per-source mapping + summary.
vq resubmit --state aborted_by_queue

# Compose with vq wait for sync rerun:
NEW=$(vq resubmit <jobid>) && vq wait "$NEW"
```

`vq resubmit` refuses non-terminal source states (use `vq kill` first).
Source spec is unchanged; only a new sibling is written.

## SLURM-style verb aliases (v0.6.15)

For chats with SLURM muscle memory, four name-level aliases (same
Click command under both names, flags stay vq-style):

| SLURM-style | Canonical vq verb |
|---|---|
| `vq sbatch`  | `vq submit`  |
| `vq squeue`  | `vq queue`   |
| `vq scancel` | `vq kill`    |
| `vq sacct`   | `vq status`  |

## Policy-driven auto-update for managed environments

```bash
# Probe drift only:
vq admin auto-update vibeqc-release HOST --dry-run

# Apply if tag policy finds a newer SemVer tag than the env's HEAD:
vq admin auto-update vibeqc-release HOST
```

Tag policy refuses to move a newer install backward. Environments explicitly
configured for branch policy track `origin/<branch>` through capped build
jobs. Branch policy rejects the running vq daemon's own environment before
mutation; repair that environment with `vq self-update --expected-sha
<full-40-hex>` or an explicitly selected accepted report.

## Fleet summary at a glance (v0.6.21)

```bash
vq overview                  # every configured host, multi-section
vq overview HOST             # just one configured host
vq overview --json           # machine-readable
vq overview --since-hours 72 # widen the recent-terminal window
```

Per host: vq version / daemon health + memory pressure / queue
counts by state / recent terminal counts / env versions + drift /
admin-update marker if present. Unreachable hosts render their
error inline so one broken host can't block the rest of the
sweep.

Useful as the first thing to run when "is anything off?" —
collapses what `vq queue --all` + `vq admin status --all` +
`vq daemon health` + `vq programs --all` give piecewise.

## Client-side log file (v0.6.16)

Every `vq` CLI invocation appends to `<state_root>/client.log`
(typically `~/.local/share/vq/client.log`). One line per
invocation captures argv + vq version; sub-operations log via
`logging.getLogger()`. Rotates at 10 MB × 3 backups.

Override level: `VQ_LOG_LEVEL=DEBUG vq ...`
Disable: `VQ_LOG_DISABLED=1 vq ...`

Useful when a `vq` command dies mid-flight or prints a confusing
error — `tail -30 ~/.local/share/vq/client.log` shows what was
happening.

## Webhook notifications on terminal state (v0.5.35)

Opt in via `~/.config/vq/config.toml` on the **daemon host**:

```toml
[notifications]
webhook_url = "https://hooks.slack.com/services/T.../B.../X..."
```

Works for Slack / Discord / Mattermost incoming webhooks (one URL,
either platform — the payload carries both `text` and `content` keys).
Microsoft Teams needs a different format; not supported by the
generic POST.

The daemon fires one POST per terminal transition: COMPLETED, FAILED,
KILLED, OOM_KILLED, STARVED, TIME_EXCEEDED, ABORTED_BY_QUEUE. Fire-
and-forget, 5s timeout, no retry. To pick up the config change, restart
the daemon (`systemctl --user restart vq-daemon`).

## Refreshing a managed environment after your push

This is the piece chats often forget:

```bash
# After you push to vibe-qc's main:
vq admin update vibeqc-dev          # default host; name HOST explicitly if needed
# After a release tag:
vq admin update vibeqc-release --tag v0.8.0   # fetch + verify the exact named tag
# Refresh every env on one host:
vq admin update --all
# Refresh one env on every host (v0.5.37):
vq admin update vibeqc-dev --all-hosts
# Refresh every env on every host — the post-release fleet sweep (v0.5.37):
vq admin update --all --all-hosts
# Check the fleet (v0.5.36):
vq admin status --all
```

`vq admin update` records its exact affected scope before pausing, refreshes
and rebuilds the environment, then resumes and proves that scope. A hard
interruption of a serving-daemon update retains a durable receipt for `vq
admin recover-update`. `--tag` resolves the explicitly named tag ref to its
peeled commit and verifies that exact identity before and after the update
script.

## Gotchas worth knowing

1. **vq doesn't checkpoint.** `--retry` and `--auto-resume` re-run the
   same command in the same workspace; your script must resume from
   partial state on disk (CRYSTAL `GUESSP=fort.20`, PySCF chkfile,
   ORCA `.gbw`).
2. **CRYSTAL `fort.34`** (final geometry) is only written by OPTGEOM
   jobs. A plain SCF input doesn't generate it. Don't assert on it for
   an SCF check; assert on `fort.9` (final wave function) + the
   `SCF ENDED - CONVERGENCE ON ENERGY` log line instead.
3. **Two FAILED-classes**: a plain non-zero command exit is `FAILED`
   (retryable with `--retry`). Watchdog kills (`OOM_KILLED` / `STARVED`
   / `TIME_EXCEEDED`) and `vq kill` (`KILLED`) are NOT retried — by
   design; a job the watchdog or the user killed must not silently
   come back. `ABORTED_BY_QUEUE` is a third class: the queue lifecycle
   (not the watchdog, not the user) ended the job. It is also NOT
   retried — check the spec reason and either resubmit (`vq resubmit
   JOBID`) or investigate the workspace.
4. **`--branch` is mutually exclusive with `--python`.** Only one may
   be used per submit. `--branch` resolves through `[hosts.X.branches]`;
   `--python` takes a literal path. Both work for `-d` (directory) /
   `-c` (tarball) submits too, as of 2026-08-01: the resolved
   interpreter is prepended to the explicit command, so
   `vq submit pbs-cluster -d ./sweep --program vibeqc-release --branch release
   -- run.py` runs through pbs-cluster's pinned release wrapper without naming
   a cluster path. Omit both when the payload already carries its own
   interpreter.
5. **The daemon's PATH** may not include `/home/USER/bin` etc. Service
   configuration, an interactive shell, and a scheduler job can all have
   different environments. When in doubt, use absolute binary paths in `--`
   commands; the smoke test
   in `tests/integration_smoke.py` reads them from `vq programs
   --json` to bypass PATH entirely.
6. **`ssh -p PORT host`**, never `host:port`. Put non-default ports in
   `~/.ssh/config`; vq uses configured SSH aliases transparently. The
   `host:port` syntax some tools
   accept (URLs, scp) is *not* ssh syntax.

## Where managed state and programs live

| | path |
|---|---|
| single-user vq state | `$VQ_STATE_DIR`, default `~/.local/share/vq/` |
| single-user config | `$VQ_CONFIG_DIR/config.toml`, default `~/.config/vq/config.toml` |
| multi-user daemon state | `/var/lib/vq/` |
| multi-user system config | `/etc/vq/config.toml` |
| registered runtime/checkouts | inspect `vq programs HOST --json` |
| engine wrappers | `vibe-queue/contrib/` in the deployed checkout |

## When something looks wrong

- **Job not dispatching?** `vq queue --active` shows it as `pending` —
  could be (a) cpu/memory budget full, (b) `--max-jobs` cap reached,
  (c) in retry-backoff (the row will say `pending (retry N/M)`), or
  (d) drained (`vq drain --status`).
- **Job died fast?** `vq status <jobid>` shows the terminal state and
  last lines; `vq fetch` pulls the full workspace for forensics.
- **Daemon stopped?** Run `vq daemon health HOST`; follow the lifecycle owner
  and recovery recipe it reports. Orphan reattachment preserves safely
  identifiable running jobs; details are in [`lifecycle.md`](lifecycle.md).
- **Output missing a file you expected?** `vq fetch` brings the
  *whole* workspace; if the file isn't there, the job didn't write it
  (or wrote it elsewhere — check `cwd` in `vq status` to confirm
  where the job was running).

## Where to read more (in the repo)

- [`agent_prompts.md`](agent_prompts.md) — copy-paste prompts for the
  release + upgrade chats, the ecosystem module/version inventory
  (vq is versioned independently of vibe-qc and carries no tag), and
  the standing release-process decisions.
- [`fleet_update_runbook.md`](fleet_update_runbook.md) — **read this
  before running any `vq admin update`.** Order of operations, the
  no-builds-on-login-nodes routing for pbs-cluster/slurm-cluster, how to watch an
  update live, how to read its transcript afterwards, and how to get
  past a busy node without `--force`.
- [`agent_interaction.md`](agent_interaction.md) — the protocol for
  chats *submitting* work (as opposed to updating hosts).
- [`operations.md`](operations.md) — troubleshooting: marker recovery,
  the `command_wrapper` composition contract, daemon config reload.
- [`handover.md`](handover.md) — canonical chat-facing reference,
  deeper; covers in-flight design state and what shipped recently.
- [`SPEC.md`](SPEC.md) — long-term design invariants and what vq is
  explicitly not trying to become.
- [`roadmap.md`](roadmap.md) — what is being worked on next, and what is
  released but not yet deployed or validated.
- [`roadmap_history.md`](roadmap_history.md) — version-by-version what
  shipped through v0.9, with rationale; `CHANGELOG.md` from v0.26.0.
- [`remote-access.md`](remote-access.md) — internet-reachable SSH
  setup (if you're submitting from off-LAN).
- [`config.toml.example`](config.toml.example) — annotated config
  template.
- Parent repo: [`docs/user_guide/queue.md`](../../docs/user_guide/queue.md)
  — user-facing entry point (more polished, less internals).

When this doc gets stale, the version number at the top is the
canary; bring an issue or a PR.
