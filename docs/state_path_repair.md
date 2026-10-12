# Repair recorded paths after a state-root move

**Audience: operators.** Part of [Running a host](operator/index.md).

A copied queue record still contains its original absolute `cwd`, `workdir`
and `archive_path`. Moving the containing state directory does not update
those fields. Fetch can therefore fail even when the corresponding artifacts
are present under the new root.

`vq admin repair-state-paths` plans an explicit root substitution for terminal
records. It defaults to read-only JSON. It changes neither artifacts nor
configuration, starts no daemon, and contacts no other host.

## Supported preservation contract

This first repair supports an existing, explicitly approved compatibility
symlink whose **direct absolute target is the selected new root**. The old and
new root must open the same current directory. Every affected artifact is
independently inventoried through both views; identities and content digests
must agree. Internal artifact symlinks are inventoried as link text without
following their targets. Artifact roots, record roots, queue directories and
receipt parents cannot be symlinks.

This proves preservation of the artifact that fetch reads **now**. It does
not establish the contents of a deleted historical copy. If the old root is
absent, points elsewhere, or is a separate copy, the plan reports a blocker.
There is no override that turns matching job IDs or path existence into a
content-equality claim. Repair from a prior trusted migration manifest is a
separate recovery workflow, not supported by this command.

The mapped paths must follow the existing managed layout: `jobs/<id>`,
`workdirs/<id>`, or `archive/<name>-<id>.tar.bz2` (also the unnamed archive),
optionally below `users/<current-uid>/`. External scheduler workspaces and
scratch-root policy changes are outside this operation. Run it as the owner
of the selected records; it refuses another user's queue and files or
directories writable by another principal.

The actual fetch source must exist: a workspace directory, or an archive for
an archived record. An already removed workdir, or the removed workspace of
an archived job, may remain absent through both names. The plan reports that
absence; it does not recreate missing bytes.

## Review a small plan first

The paths below are examples; select the actual canonical directories. The
command does not create the compatibility symlink for you.

```sh
vq admin repair-state-paths \
  --state-root /srv/queue-current \
  --from-root /srv/queue-previous \
  --to-root /srv/queue-current \
  --job job123 --dry-run > repair-plan.json
```

The plan lists exact record paths, original record digests, every field's old
and new value, current artifact identities/digests, matching absences, and
blockers. `summary` distinguishes scanned and affected records, candidate
records from actually applicable records (zero when any blocker exists), and
residue and alias-dependent link counts. `inventory` includes affected records
even when their fields are blocked. `residue` lists JSON pointers to lexical
old-root mentions in other fields, including nested extensions and commands;
these are review warnings, not path proofs, and are never rewritten.
`--json` explicitly selects the default output. `plan_id` binds that complete
snapshot, including census counts and residue. Without `--job`, even an unrelated
new queue record changes the scanned count and requires a new reviewed plan.
Use exact `--job` selections to keep a canary's approval scope independent of
unselected job arrivals. A blocked plan exits with
status 1 and writes no queue record or lock. Without `--job`, the plan scans
`queue/` and the current owner's `users/<uid>/queue/`, preserving both copies
when they agree. Duplicate job IDs with conflicting path bindings are refused.
Artifact hashing can take time for large results; start with explicit job IDs.

## Apply only the reviewed plan

```sh
vq admin repair-state-paths \
  --state-root /srv/queue-current \
  --from-root /srv/queue-previous \
  --to-root /srv/queue-current \
  --job job123 \
  --apply-plan REVIEWED_SHA256 \
  --receipt-dir /srv/repair-receipts/first-job
```

The receipt parent must already exist, be owned by the operator, and have no
symlink component. The final receipt directory must be new and outside the
record and artifact roots. Apply reconstructs the plan, rejects changed
records, artifacts or aliases, and preserves **all exact original JSON bytes**
before changing the first record. Unknown JSON fields survive the update;
record mode, owner and group are preserved.

Each record is reread under its existing per-spec lock, checked terminal,
and replaced atomically through a pinned directory descriptor. A busy lock,
active job, ambiguous path, invalid record, or stale plan fails closed. Normal
queue writers must obey that same lock; do not run external scripts that edit
records or move their directories concurrently. This is an explicit repair
operation, not automatic path resolution during fetch.

## Retain the receipt, including interrupted runs

The receipt contains the reviewed `plan.json`, original record files named by
the digest of their relative path, a durable intent before each replacement,
and an applied marker afterwards. Only `result.json` with status `complete`
means the whole selected plan finished. Artifact bytes are never moved,
rewritten or deleted.

A multi-record operation is atomic **per record**, not across the entire
queue. If interrupted, retain its originals, intents and any named temporary
record; inspect current hashes against the intent's `before` and `after`
digests. Do not blindly restore an original over a newer queue record. A new
dry run identifies the remaining old-root references and requires a newly
reviewed digest and a new receipt directory. Existing receipt directories
are never reused or overwritten.

## Alias retirement is a separate check

A successful record repair does not authorize removing the compatibility alias.
`alias_dependencies` lists absolute internal artifact symlinks still targeting
OLD. Their link text is preserved, not rewritten; removing OLD may make fetch
skip those entries. Other residue (commands, convergence paths or extensions)
may also depend on OLD. Resolve such dependencies under a separate reviewed
operation and verify fetched content before considering alias retirement. This
command neither creates nor removes the alias. It cannot discover references
outside the selected records and artifacts.
