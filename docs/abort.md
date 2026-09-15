# vq ABORTED_BY_QUEUE — when the queue ends a job

`ABORTED_BY_QUEUE` is a terminal state meaning: **the queue lifecycle
ended this job, not the user and not the watchdog.** It tells you to
investigate the reason rather than assuming the job failed.

## When a job gets ABORTED_BY_QUEUE

| Reason | When | What it means |
|---|---|---|
| `pid_recycled` | Daemon startup (v0.5.50+) | The recorded `spec.pid` is alive on disk but `/proc/<pid>/stat` field 22 (start time in jiffies) doesn't match the value captured at dispatch. The kernel reused this PID for an unrelated process. Conservative: don't silently re-attach to someone else's work. |
| `no pgid recorded` | Daemon startup | The spec was submitted before v0.3 and has no `pgid`. The daemon cannot check liveness. |
| `daemon_restart_orphan` | Daemon startup | The process group is gone (killed by init/SIGKILL during host reboot) and no exit-code marker was written. |
| `orphan process exited (no marker)` | Orphan reconciliation | The daemon detected an orphan job whose pgid disappeared, but there's no exit-code marker (bash wrap didn't write it — possibly SIGKILL of bash itself). |

## Interpreting the state

### The spec reason

Check the spec's `exit_code` field and the reason in the event log:

```bash
vq status JOBID
# Look for:
#   state: ABORTED_BY_QUEUE
#   reason: pid_recycled (or daemon_restart_orphan, etc.)
```

Or check the event log directly:

```bash
vq status JOBID -n 0   # full output including events
# Look for state_transition events with reason field
```

### The workspace

The workspace is usually **still on disk**:

```bash
ls ~/.local/share/vq/jobs/JOBID/
# Should show: stdout.log, stderr.log, _vq/, user files
```

Check the logs:

```bash
cat ~/.local/share/vq/jobs/JOBID/stdout.log
cat ~/.local/share/vq/jobs/JOBID/stderr.log
```

If the job was writing output before it ended, the logs will show
where it got.

### The exit-code marker

If the job ended gracefully (bash wrap wrote its exit code):

```bash
cat ~/.local/share/vq/jobs/JOBID/_vq/exit-code
# Shows the inner command's exit code (or 128+signal for signals)
```

This is useful when the daemon couldn't capture it via popen (the
job ran while the daemon was down).

## Recovery actions

### Resubmit (most common)

```bash
vq resubmit JOBID     # re-queue with the same workspace
# or
vq submit ...         # fresh submission
```

### Check the workspace

If the job had partial output:

```bash
vq fetch JOBID -o ./forensics   # copy workspace for inspection
```

### Auto-resume sibling

If the job was submitted with `--auto-resume`, the daemon has already
created a sibling resubmit:

```bash
vq queue -s pending   # look for the sibling job
# The sibling's spec has parent_jobid = JOBID
```

## ABORTED_BY_QUEUE vs other states

| State | Who ended it | Retriable? |
|---|---|---|
| `FAILED` (non-zero exit) | User's program | Yes, with `--retry` |
| `OOM_KILLED` | Watchdog (memory) or scheduler accounting (e.g. SLURM `OUT_OF_MEMORY`) | No — investigate memory budget |
| `STARVED` | Watchdog (CPU) | No — investigate CPU budget |
| `TIME_EXCEEDED` | Watchdog (wall-time) or scheduler evidence (walltime, `TIMEOUT`/`DEADLINE`) | No — investigate wall-time budget |
| `KILLED` | User (`vq kill`) | No — user decision |
| **`ABORTED_BY_QUEUE`** | **Queue lifecycle** | **No — check reason, then resubmit** |

## Prevention

- **Enable cgroup delegation** on the queue host — it prevents OOM
  kills and gives the kernel a chance to contain resource violations.
- **Use `--auto-resume`** for long-running jobs that benefit from
  restarting from partial state on reboot.
- **Use `--retry N`** for jobs that may fail transiently (not for
  infrastructure failures).
- **Enable `vq daemon health`** monitoring — detect PID-recycle and
  orphan issues early.

## See also

- `docs/remote-access.md` — host reboot scenarios
- `docs/lifecycle.md` — daemon startup recovery
- `vq queue -s aborted_by_queue` — filter for aborted jobs
