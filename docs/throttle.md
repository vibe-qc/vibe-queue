# vq throttle — soft CPU priority control

`vq throttle` lets you adjust a running job's CPU weight without
killing or pausing it. The job continues running but gets less CPU
under contention.

## Quick reference

```bash
# Set a running job to CPUWeight=20 (very low priority)
vq throttle JOBID --weight 20

# Restore default CPUWeight=100
vq throttle JOBID --restore

# Apply to every running job
vq throttle --all --weight 20

# Restore every throttled job
vq throttle --all --restore

# Check current throttle settings
vq throttle --status
```

## How it works

### Cgroup path (preferred)

When the host has cgroup-v2 delegation enabled (the default on compute-d
and compute-a), throttle sets the `CPUWeight` property on the job's transient
systemd scope (`vq-job-<jobid>.scope`):

```bash
systemctl --user set-property vq-job-<jobid>.scope CPUWeight=20
```

`CPUWeight` ranges from **1 to 10,000**. Default systemd value is **100**.
Lower values get less CPU under contention; higher values get more.

**Key property of CPUWeight:** when no other process wants CPU, the
scope still uses all available cores — it's only under contention that
the weight matters. This is the "soft" in soft throttle.

### Renice fallback (non-cgroup hosts)

On hosts without cgroup delegation (e.g. macOS dev machines, or Linux
hosts without the `Delegate=` drop-in), vq falls back to `renice` on
the job's process group:

```bash
renice -n <value> -p <pgid>
```

The nice value is derived from the CPUWeight by mapping 1-10,000 →
19 to -20 (inverted: lower weight = higher nice = lower priority).
This is a best-effort approximation and may not have the same fairness
properties as cgroup CPUWeight.

## When to use throttle

- **Start a heavy job, then remember you need the desktop.** Throttle
  to CPUWeight=20 or lower rather than pausing (which preserves RAM).
- **Run multiple jobs and want to prioritize one.** Keep the priority
  job at CPUWeight=100 and throttle others.
- **Night batch:** set everything to low weights so nothing blocks
  daytime interactive use.

## When NOT to use throttle

- **The job is OOM-ing.** Throttle does not reduce memory usage; use
  `vq pause` or `vq kill` instead.
- **The job is CPU-starved by the watchdog.** A STARVED kill means the
  watchdog saw no CPU activity for the starvation window. Throttle makes
  this worse, not better. Use `--auto-resume` or add more CPU budget.

## Persistence

Throttle state is persisted in `<state_root>/throttle.json`. When the
daemon restarts, it re-applies the throttle to new dispatches automatically.

**Important:** throttle does NOT affect already-running jobs when
applied via `vq admin update`. It only applies to jobs dispatched after
the throttle is set. To affect running jobs, use `vq throttle <jobid>`
directly.

## CPUWeight vs nice mapping (renice fallback)

| CPUWeight | renice value | Relative priority |
|---|---|---|
| 10000 | -20 | Highest |
| 1000 | -10 | Above default |
| 100 | 0 | Default |
| 10 | 5 | Below default |
| 1 | 19 | Lowest |

## See also

- `docs/drain.md` — harder gate that limits concurrent job count
- `vq drain` — throttle per-job; drain limits the fleet
- `vq pause` — hard freeze (preserves RAM but uses zero CPU)
