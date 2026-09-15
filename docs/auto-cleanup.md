# vq auto-cleanup

`vq cleanup` manages retained job artifacts on each queue host. It only acts on
terminal jobs. Running, pending, and suspended jobs are never archived or
deleted by cleanup.

## Manual Cleanup

Dry-run is the default:

```bash
vq cleanup HOST
vq cleanup HOST --archive --older-than 30d
vq cleanup HOST --delete --older-than 180d
```

Add `-x` or `--execute` to perform the action:

```bash
vq cleanup HOST --archive --older-than 30d -x
vq cleanup HOST --delete --older-than 180d -x
vq cleanup HOST --archive --jobid <jobid> -x
vq cleanup HOST --delete --jobid <jobid> -x
vq cleanup HOST --restore <jobid> -x
```

Archive mode creates `<archive_dir>/<job-name-or-jobid>.tar.bz2`, removes the
workspace directory, and leaves the spec in the queue with `archived_at` and
`archive_path`. `vq fetch HOST JOBID` transparently fetches archived jobs from
that tarball.

Delete mode removes the spec, workspace, and archive. It is irreversible.

## Daemon Policy

Enable host-side auto-cleanup with the CLI rather than hand-editing the policy
JSON:

```bash
vq cleanup HOST --auto-enable \
    --archive-after 30d \
    --archive-after-state failed:90d \
    --delete-after 180d \
    --workdir-max-age 14d \
    --interval 24h \
    --reason "fleet retention policy"
```

Check or disable it with:

```bash
vq cleanup HOST --auto-status
vq cleanup HOST --auto-disable
```

`--auto-status` reports whether the next daemon sweep is already due, paused,
or scheduled for a specific `next_run_at` timestamp. Use that before manual
disk cleanup so you can tell whether the daemon is waiting on the interval or
whether a due sweep should run on the next daemon loop.

The policy lives at `<state_root>/auto-cleanup.json`. In multi-user mode it is a
daemon-wide policy under the system vq root, while each user's jobs archive into
that user's archive directory.

## Retention Rules

`--archive-after DUR` archives terminal jobs older than `DUR`.
`--delete-after DUR` deletes terminal jobs older than `DUR`.

Per-state overrides use `STATE:DUR` and win over the global value for that
state:

```bash
vq cleanup HOST --auto-enable \
    --archive-after 30d \
    --archive-after-state failed:90d \
    --delete-after 180d
```

Valid states are `completed`, `failed`, `killed`, `interrupted`,
`oom_killed`, `starved`, `time_exceeded`, and `aborted_by_queue`.

Use `--archive-dir DIR` or `$VQ_ARCHIVE_DIR` when archive tarballs should live
on a larger filesystem than the queue state root.

## Scheduler Workspaces

Scheduler-backed jobs, such as PBS/Torque jobs, stage a remote workspace under
the scheduler host's configured `scratch_root` and copy the full result tree
back into the local vq workspace when the job reaches a terminal state.

Cleanup now reclaims that duplicate remote scheduler workspace too:

- At archive age, the auto-cleanup sweep removes the scheduler-side workspace
  and stamps `scheduler_remote_workspace_cleaned_at` on the spec.
- Archived specs whose remote workspace cleanup previously failed are retried
  on later sweeps until they are stamped clean.
- Before deleting a scheduler spec, cleanup tries the remote workspace cleanup
  one last time. If the scheduler-side cleanup fails, the delete is skipped so
  the spec keeps the deterministic remote path for a later retry.

This remote cleanup uses the same scheduler dispatcher configuration as submit,
status, and fetch. It is idempotent: an already-missing remote directory counts
as a successful `rm -rf`.

Node-local scratch created by scheduler scripts is separate. When
`node_scratch_dir` is configured, the generated job script copies outputs back
to the scheduler workspace, writes the exit marker, and removes its temporary
node-local directory on normal script completion.

## Workdirs

Local per-job workdirs, exposed to payloads as `$VQ_WORKDIR`, are distinct from
the workspace. Jobs can opt into immediate cleanup with:

```bash
vq submit HOST --clean-tmp ...
```

The auto-cleanup model also supports aged workdir sweeping through
`--workdir-max-age DUR`. Use it conservatively because workdirs may contain
operator-readable scratch from long debugging runs.
When configured, `vq cleanup HOST --auto-status` reports it as
`workdir_max_age=<seconds>s` so fleet audits can confirm temporary workdir
cleanup is active.

## Schedule And Safety

The daemon checks the policy on each loop and runs a sweep only after
`interval_seconds` has elapsed since `last_run_at`. Cleanup errors are counted
and logged; they do not crash the daemon. `last_run_at` is stamped after each
pass so a chronically failing cleanup item does not retry every loop tick.
