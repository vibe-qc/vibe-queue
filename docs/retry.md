# vq retry — automatic re-enqueue on failure

`vq submit --retry N` lets a job automatically re-run up to N times
when it exits non-zero. Use it for jobs that may fail transiently
(network hiccups, license server timeout, intermittent OOM).

## Quick reference

```bash
# Retry up to 3 times on failure
vq submit run.py --retry 3

# Combined with auto-resume (for reboot + failure recovery)
vq submit run.py --retry 3 --auto-resume

# Check retry status
vq queue -s failed    # see "(retry N/M)" annotations
vq status JOBID       # see retry_count in detail
```

## How it works

### Backoff formula

Retry uses **exponential backoff** with a fixed base:

```
delay = min(10 * 2^(retry_count - 1), 600)  # seconds
```

| Retry # | Delay before dispatch |
|---|---|
| 1 | 10 seconds |
| 2 | 20 seconds |
| 3 | 40 seconds |
| 4 | 80 seconds |
| 5 | 160 seconds |
| 6 | 320 seconds |
| 7+ | 600 seconds (capped) |

The delay is recorded as `not_before` on the spec; the daemon skips
the job until that time passes.

### What IS retried

Only **non-zero command exit code** failures:

```bash
# These trigger retry:
vq submit run.py --retry 3    # exit code 1, 2, 42, etc.

# These also retry (from orphan recovery):
# Job exits while daemon is down, exit marker shows non-zero rc
```

### What is NOT retried (by design)

| State | Why not retried |
|---|---|
| `OOM_KILLED` | Watchdog killed — likely not transient |
| `STARVED` | Watchdog killed — CPU starvation is systemic |
| `TIME_EXCEEDED` | Watchdog killed — wall-time overran |
| `KILLED` | User killed it — user decision |
| `ABORTED_BY_QUEUE` | Queue lifecycle ended it — investigate before retrying |
| `COMPLETED` (exit 0) | Job succeeded — nothing to retry |

**Rationale:** a job the watchdog or the user killed should not
silently come back. If it's truly transient, the user can resubmit
manually. Retry is for *program* failures, not *infrastructure*
failures.

### Retry budget

- `retry_max` = N (set at submit time via `--retry`)
- `retry_count` = how many retries have been spent (starts at 0)
- When `retry_count >= retry_max` → job lands in `FAILED`

The retry budget **persists across reboots**. If a job uses 2 of 3
retries, then the host reboots, the auto-resume sibling inherits
`retry_count=2` — it has only 1 retry left.

### Workspace reuse

Retried jobs use the **same workspace directory**. The daemon:

1. Leaves the existing workspace on disk (output files, partial state)
2. Unlinks the old exit-code marker before the new dispatch
3. Opens stdout.log / stderr.log in **append** mode

This means:
- **Partial state accumulates** — your script must handle restarting
  from partial outputs (CRYSTAL `GUESSP=fort.20`, PySCF chkfile, ORCA
  `.gbw`).
- **Logs accumulate** — `vq status JOBID` shows all retries;
  `vq fetch` brings the entire workspace.

### State transitions

```
PENDING  →  RUNNING  →  (non-zero exit)  →  PENDING (retry 1/3)
                                              →  RUNNING  →  (non-zero exit)  →  PENDING (retry 2/3)
                                              →  RUNNING  →  (non-zero exit)  →  FAILED (3/3 exhausted)
                                              →  RUNNING  →  (exit 0)  →  COMPLETED
```

## When to use retry

- **Network-dependent jobs** (license check, remote data fetch)
- **License server timeouts** (rare, intermittent)
- **Intermittent OOM** (if the job sometimes exceeds memory by a
  small margin and the next run might fit)
- **External service calls** (REST API, S3 upload)

## When NOT to use retry

- **Wrong input / bad configuration** — retry won't fix it
- **Algorithmic failure** — convergence failure, wrong parameters
- **Persistent OOM** — the job will OOM again; use `--mem-mb`
- **Data corruption** — retry may mask the real problem

## Manual retry

If a job lands in `FAILED` after exhausting retries, resubmit manually:

```bash
vq resubmit JOBID    # re-queue with same workspace
# or
vq submit ...         # fresh submission
```

## See also

- `--auto-resume` — retry after host reboot (same workspace)
- `docs/auto-cleanup.md` — clean up failed/retried jobs
- `vq queue -s failed` — see retried jobs in the queue listing
