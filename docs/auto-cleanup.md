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

Delete mode removes the spec, workspace, **workdir** and archive. It is
irreversible.

### What the sizes mean

The listing has three size columns, and #63 added the middle one:

| column | tree | removed by |
|---|---|---|
| WORKSPACE | the job's `cwd`, under `<scratch root>/jobs` | `--archive` (into a tarball), `--delete` |
| WORKDIR | the job's `$VQ_WORKDIR`, under `<scratch root>/workdirs` | `--delete`, or the age sweep |
| ARCHIVE | the tarball, under the state root's `archive/` | `--delete` |

`reclaimable` is their sum, with the workdir total called out separately.

**Before #63 the workdir column did not exist and its bytes were not counted.**
On the host that prompted the issue, `vq cleanup` reported about 1 GB across
~1,200 terminal jobs while `workdirs/` held 81 GB — a verb that looked like
cleanup and could not see where the growth was.

Each job is measured and cleaned **at the path it recorded**, so a host part-way
through the [phase 5 scratch migration](fleet_layout.md) sees both roots: jobs
dispatched before the split are accounted and deleted under the state root,
jobs dispatched after it under the scratch root. A host that has since been
pointed at a third root is no different — the record is what is followed, and
the age sweep walks every root the queue's own records still name on top of the
configured ones. A workdir whose job record is already gone, under a root
nothing names any more, is the exception: remove it by hand. `--archive` tars the workspace
only and says so, because the workdir it leaves behind is usually the larger
number.

A workdir is removed only when the recorded path is still the per-job directory
vq created under a root vq manages. One pointed somewhere else — by hand, or by
a record from a host whose layout was rearranged outside vq — is reported and
left alone.

Both are all-or-nothing on purpose. An archive whose workspace holds an entry
that cannot be read fails and leaves the workspace in place, because the
archive is followed by deleting that workspace, and a tarball with the entry
missing would turn an unreadable file into a lost one; a half-written tarball
is removed. Fix the permission and archive again. In the daemon's sweep the
failure is counted and logged and the job is retried on the next sweep.
`--restore` fails the same way on a tarball it cannot read in full and leaves
the archive untouched. This differs deliberately from `vq fetch`, which copies
what it can and records what it skipped: a fetch leaves the workspace where it
was, an archive does not (#62).

### What cleanup does and does not cost the daemon

**Retention is not what the daemon's per-tick scan costs, and cleanup is not
how you bound it.** As of v0.26.10 the scan leaves out every *settled* row —
one that has reached a terminal state and carries no scheduler transaction that
could still be open — whether or not anybody ever archived it. Such a row costs
one `stat` per pass instead of an open, a JSON parse and a model validation
(#22). The first pass after a daemon start still reads every row once; that is
what proves each one settled, and records the on-disk identity (mtime, size,
inode, device) the later passes compare against. Any rewrite of the file — an
archive, a `--restore`, a `vq fetch` read stamp, a hand edit — changes that
identity and the row is read again on the very next pass.

So plan retention on the things retention actually governs:

* **Archive to reclaim disk, not to speed up dispatch.** Archiving tars a
  workspace away, which is usually where the bytes are. It does not change
  what the daemon's scan costs, because an unarchived terminal row is already
  off that scan.
* **Delete only to discard history.** `--delete` is still the only verb that
  removes a spec, and it is still irreversible. It is no longer the way to keep
  a busy driver dispatching promptly.
* **Nothing about a retained job becomes less retrievable.** `vq queue`,
  `vq status`, `vq show`, `vq fetch`, `vq cleanup` and `--restore` resolve a
  job by id or run their own listing pass; none of them go through the daemon's
  scan. Two kinds of row stay on that scan however old they are: one whose
  scheduler submit is still unresolved — it must keep reserving its slot — and
  every row at daemon startup, where a terminal job's leaked process group is
  reaped.

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
on a larger filesystem than the queue state root. `scratch_root` (or
`$VQ_SCRATCH_DIR`) does the same for the disposable trees, and is the bigger
lever: on two measured hosts those were over 99 % of the footprint.

## Scheduler Workspaces

Scheduler-backed jobs, such as PBS/Torque jobs, stage a remote workspace under
the scheduler host's configured `scratch_root` and copy the full result tree
back into the local vq workspace when the job reaches a terminal state. The
workspace each job was staged into is recorded on its spec before submission
(#63), so cleanup reclaims it at that exact path even if `scratch_root` has
changed since — which used to make it permanently un-reapable.

Cleanup now reclaims that duplicate remote scheduler workspace too:

- At archive age, the auto-cleanup sweep removes the scheduler-side workspace
  and stamps `scheduler_remote_workspace_cleaned_at` on the spec.
- Archived specs whose remote workspace cleanup previously failed are retried
  on later sweeps until they are stamped clean.
- Before deleting a scheduler spec, cleanup tries the remote workspace cleanup
  one last time. If the scheduler-side cleanup fails, the delete is skipped so
  the spec keeps the recorded remote path for a later retry.

This remote cleanup uses the same scheduler dispatcher configuration as submit,
status, and fetch. It is idempotent: an already-missing remote directory counts
as a successful removal.

On a Lustre filesystem, `rm -r` stats every file before unlinking it, one
metadata-server round trip per file. Remote tree removal therefore checks each
tree first. When `stat -f` reports `lustre` and `munlink` is installed, vq
unlinks files and symlinks with `find -P TREE \( -type f -o -type l \) -print0
| xargs -0 munlink`, removes the emptied directories with `find -P TREE -depth
-type d -empty -delete`, and then runs `rm -rf` on whatever remains. Links are
never followed. Every other filesystem uses `rm -rf` directly. The same rule
applies to node-local scratch removal in generated job scripts and to
runtime-source stage reclaim. It needs no configuration.

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

The sweep visits **every** workdir root vq manages — the scratch root and, until
it is empty, the pre-#63 one under the state root — and within each it is
gated hard:

- a workdir whose job is not terminal is never removed, whatever its age or
  its directory mtime (a long SCF loop writing into files that already exist
  leaves that mtime frozen at dispatch, which is how an mtime-only heuristic
  would delete live scratch);
- a terminal job's workdir ages by its `finished_at`, not by the directory;
- the root itself is opened `O_NOFOLLOW`, so a symlinked root fails closed
  instead of redirecting the removal somewhere else;
- an entry that is a symlink, is not a directory, or is owned by another uid is
  refused and counted as an error rather than removed;
- nothing outside those per-job directories is reachable: the queue, archives,
  and anything else under either root are not sweep candidates.
When configured, `vq cleanup HOST --auto-status` reports it as
`workdir_max_age=<seconds>s` so fleet audits can confirm temporary workdir
cleanup is active.

## Schedule And Safety

The daemon checks the policy on each loop and runs a sweep only after
`interval_seconds` has elapsed since `last_run_at`. Cleanup errors are counted
and logged; they do not crash the daemon. `last_run_at` is stamped after each
pass so a chronically failing cleanup item does not retry every loop tick.
