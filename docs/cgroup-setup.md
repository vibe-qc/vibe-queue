# vq cgroup setup - delegation and fallback detection

Cgroups v2 provide **kernel-level enforcement** of per-job CPU and
memory limits. When configured, each dispatched job runs inside a
transient systemd scope (`vq-job-<jobid>.scope`) that the kernel
enforces - not just a watchdog watching from outside.

## How cgroup enforcement works

When cgroup delegation is available, vq dispatches each job through:

```bash
systemd-run --user --scope --quiet --collect \
    --property=MemoryMax=<N>M \
    --property=MemoryHigh=<int(N*0.9)>M \
    --property=CPUQuota=<N*100>% \
    -- \
    /path/to/python script.py
```

- **`MemoryMax`** - hard cap. Kernel kills the job in-cgroup when
  exceeded (not the host's OOM-killer).
- **`MemoryHigh`** - soft cap at 90% of MemoryMax. Triggers memory
  reclaim before the hard kill fires (smoother behavior).
- **`CPUQuota`** - CPU percentage of one core. 4 cpus → 400%.
- **`--collect`** - scope is automatically cleaned up after the job
  finishes.

The watchdog still samples these jobs for RSS, CPU, and wall-time telemetry.
It also retains pause-aware wall-time enforcement, host-memory pressure
handling, and CPU-starvation checks. The per-job memory hard cap and CPU quota
are enforced by the kernel cgroup.

## Prerequisite: delegation

Cgroup delegation must be enabled on the **user systemd manager**:

```bash
# On the queue host, as root or with sudo:
sudo mkdir -p /etc/systemd/system/user@.service.d
cat <<EOF | sudo tee /etc/systemd/system/user@.service.d/delegate.conf
[Service]
Delegate=cpu cpuset io memory pids
EOF

sudo systemctl daemon-reload
```

After this change, **restart the user manager**:

```bash
# As the queue user:
systemctl --user daemon-reload
systemctl --user restart vq-daemon
```

If the user manager was started before the drop-in, it won't pick up
the delegation until it's restarted.

## Verify delegation is working

### Quick probe

```bash
systemd-run --user --scope --quiet --collect \
    --property=MemoryMax=10M -- /bin/true
echo $?
# 0 = delegation works
# non-zero = delegation not available
```

The daemon startup log records `cgroup=enforced` or `cgroup=disabled`. Use the
quick probe above to confirm the host capability directly.

### Check a running job's cgroup

```bash
systemctl --user list-units --type=scope | grep 'vq-job-'
# Should show active scopes for running jobs
```

## Fallback: /proc polling

When delegation is **not** available, vq falls back to:

1. **Watchdog-only enforcement** - the daemon polls `/proc/<pid>/stat`
   and `/proc/<pid>/status` directly for each job's RSS and CPU time.
2. **Signal-based kill** - when the watchdog detects a violation, it
   sends SIGTERM → grace period → SIGKILL to the job's process group.
3. **No per-job kernel memory cap** - the daemon tracks memory usage
   (RSS sum of all processes in the pgid) and kills when the
   host-total ceiling is exceeded, but there is no per-job kernel
   memory limit.

This fallback is functional but less protective: a job that allocates
memory faster than the watchdog's sampling interval (default 5s) can
briefly overshoot. On compute-d and compute-a, delegation is enabled.

## What happens when delegation flips

Delegation can flip if:

1. The drop-in is removed or modified.
2. The user systemd manager is restarted without the drop-in.
3. A kernel update changes cgroup controller support.

**v0.5.50+**: the daemon clears its cgroup availability cache at
startup, so a restart re-tests delegation from scratch. If delegation
is lost, the daemon logs a warning and falls back to /proc polling.

To detect a flip, rerun the quick probe after restart and compare the daemon's
startup `cgroup=` log field.

## Troubleshooting

### "cgroup probe failed: PermissionError"

Delegation is not configured. Install the drop-in and restart the
user manager (see above).

### "cgroup probe failed: Failed to set unit properties"

The user systemd manager doesn't support the property. This can happen
on older systemd versions (< 244). Upgrade or use the /proc fallback.

### "cgroup probe failed: timeout"

systemd-run is taking too long. Check that `systemctl --user` works
interactively (not just from the daemon). Zombie user-systemd can
cause this - use the force-revive recipe in `docs/operations.md`.

## See also

- `docs/lifecycle.md` - the systemd-user contract
- `docs/wall_time_design.md` - why wall-time stays in the watchdog
- `docs/operations.md` - zombie user-systemd recovery
