# Runtime slots: moving a program to verified generations

**Audience: operators.** Part of [Running a host](operator/index.md).

**Status.** The local flow on this page, `vq admin migrate-runtime` for a program
registered on this host, has been accepted on one direct host: on 2026-09-28, at
vq 0.26.16, a first generation was prepared and applied, rolled back and applied
again for a development, a release and a viewer registration. No other host is
recorded as having run it. The `blocked` plan status and the blocker for an
installer outside the source tree arrived in 0.26.17, after that acceptance, and
have not been run on a host. This page does not cover a scheduler host whose
runtimes are packed releases, the `participants` form of the manifest: that kind of transaction has
not yet moved a binding on a real host, and it is described only in
`design_immutable_venv_runtimes.md` in the source tree.

## What a runtime slot is

A program registered with `kind = "venv"` is normally updated in place: a
`git pull` and an install into the venv it already has. That rewrites files a
running interpreter may still be importing.

With `runtime_slot_root` set, an update builds a new **generation** at
`<root>/releases/<sha>/`, with its own virtual environment created at that final
path. It verifies and seals the generation, and only then points `<root>/current`
at it; `<root>/previous` keeps the last one. Once the registration also has
`runtime_slot_execution` (below), a job that is running keeps the generation it
started with, and a job dispatched afterwards gets the new one.

Three keys on the program control how far a registration goes:

| key | what it does |
|---|---|
| `runtime_slot_root` | The absolute path of the slot store. Alone, it changes only how updates are built; jobs still run from the registered `python` and `git_dir`. |
| `runtime_slot_execution = true` | Local dispatch of a `--program` job, and the QVF commands vq generates, run the selected verified generation, and the job records its source, interpreter and SHA (`resolved_runtime_*`). A command that names the old interpreter still runs it. Needs `runtime_slot_root`. There is no fallback to the old environment: a missing or invalid current generation refuses. Enable it for every alias that shares the checkout or the interpreter together, after the first generation is verified. |
| `runtime_slot_build_input = "current"` | Clone the next generation from the active one instead of the registered `git_dir`, which may then be retired. Needs `runtime_slot_execution`. Setting it retires nothing. |

**Introduce these keys one host at a time, after that host runs a vq that knows
them.** A vq that predates a key does not ignore it: program entries reject
unknown keys, so it fails to load the config, and the commands that read the
config stop working on that host.

`vq programs HOST --json` reports the selected generation's paths and identity
for a program with `runtime_slot_execution`. The `build_python` and
`build_git_dir` beside them are the inputs the next generation is built from: the
registered ones by default, the active generation's with
`runtime_slot_build_input = "current"`.

None of this installs a wrapper or moves an existing binding by itself.
`migrate-runtime` does that for a local registration, and it refuses one whose
`runtime_slot_execution` is already true, so do not enable the key by hand first.
A registration that serves the vq daemon is not moved by this command at all (a
serving daemon blocks it); see the serving-daemon update in the
[operator reference](operations.md). Updating a slot program takes an exact
`--expected-sha`, and old generations are removed with the
[reviewed retention command](operations.md#removing-old-runtime-generations-74).

## Moving a registration: `vq admin migrate-runtime`

The command migrates one local component, including every alias that shares its
checkout or interpreter. It rewrites the config files you name, using candidates
you have reviewed, and creates new stable launchers. It leaves every legacy path
and both generations in place, and deletes nothing. It does not reconfigure a
service, contact another driver, run a private packed publisher, or prune.

### What blocks a plan

A plan lists its blockers, and prepare and apply refuse a plan that has any:

* an entry in `external_consumers`;
* an unresolved admin update or recovery marker;
* a target SHA that disagrees with the program's configured fixed SHA;
* a serving daemon, an unknown service identity, or no service manager at all;
* no verified generation exists yet and the registration has no `install_script`,
  or its installer lies outside the source tree.

Other conditions are refusals, not blockers: the command stops with
`Error: ...` and exit status 1 and prints no plan.

* Every path is absolute, has no `..`, and no component of it is a symlink. Every
  file you name must be owned by you, not group- or world-writable, and have a
  single hard link. Its directory, and the legacy source directory, must be
  owned by you and not group- or world-writable too. A shared group-writable
  checkout therefore fails.
* The config files, their candidates and the receipt directory must lie outside
  the legacy source tree and the runtime store. Keep the manifest and the
  receipts out of product checkouts as well: they hold private deployment paths,
  and a candidate holds complete config bytes.
* The legacy interpreter has to stay executable, because rollback restores it.
* The registered `python` must be `git_dir/<one directory>/bin/python`, and the
  update, post-update and install scripts must lie inside the source tree. An
  external or nested environment needs a reviewed build adapter.
* Aliases that share the checkout or the interpreter must have the same build
  profile in every registry, and none may already have `runtime_slot_execution`
  or name a different `runtime_slot_root`.

### The manifest

```json
{
  "schema_version": "vq.runtime-migration/1",
  "component": "chemistry",
  "program": "chemistry",
  "source_sha": "0123456789abcdef0123456789abcdef01234567",
  "runtime_root": "/opt/example/runtimes/chemistry",
  "registries": [
    {
      "path": "/opt/example/config/config.toml",
      "candidate": "/opt/example/review/config.candidate.toml"
    }
  ],
  "launchers": [
    {
      "path": "/opt/example/bin/chemistry-python",
      "bootstrap": "/opt/example/retained-vq/bin/vq",
      "arguments": []
    }
  ],
  "external_consumers": []
}
```

| field | meaning |
|---|---|
| `component` | A short name: a lower-case letter, then lower case, digits, `-` and `_`. |
| `program` | The registered venv program. |
| `source_sha` | The full 40-character lower-case commit to build as the generation. |
| `runtime_root` | The slot store, in its canonical spelling. Required for a local migration. |
| `registries` | Each config file that holds the registration, with a `candidate`: your edited copy. |
| `launchers` | New stable launchers. `path` must not exist. `bootstrap` is a vq executable that stays installed independently and lies outside the store: a regular file you own, not a symlink, with an execute bit. Its identity is part of the plan, so reinstalling it before the transaction finishes makes the plan stale. `arguments` are fixed Python arguments, for example `["-m", "vq.cli"]` for a daemonless source-local vq helper. |
| `external_consumers` | **Required.** Every remote driver, packed publisher or other consumer you know of. Any entry blocks the plan. Leave it empty only when you have looked. |

The manifest is an inventory you declare. vq does not discover other consumers,
and it rejects a key it does not know.

A candidate must parse as a complete vq config and differ from the original in
exactly one way: `runtime_slot_root` and `runtime_slot_execution = true` on
**every** alias sharing the program's checkout or interpreter. It may also set
`runtime_slot_build_input = "current"`, but then on every such alias in every
registry. Keep `python`, `git_dir`, update arguments, extras, import checks and
every unrelated field as they were. Formatting may differ; rollback restores the
original bytes, comments included.

### Plan, prepare, apply

```sh
vq admin migrate-runtime --manifest /opt/example/review/migration.json
vq admin migrate-runtime --manifest /opt/example/review/migration.json \
  --prepare-plan REVIEWED_PLAN_SHA256
vq admin migrate-runtime --manifest /opt/example/review/migration.json \
  --apply-plan REVIEWED_READY_PLAN_SHA256 \
  --receipt-dir /opt/example/review/transaction-001
```

Each step prints JSON. The first is a read-only plan: the exact source and
generation paths, every alias and file it would change, `current` and `previous`,
the generations that jobs still reference, the blockers, and a `plan_id`, which
is a digest of the whole plan. Its `status` names the next step:

| `status` | next step |
|---|---|
| `preparation_required` | run `--prepare-plan` with the `plan_id` |
| `ready` | a verified generation exists; review the plan, then run `--apply-plan` |
| `blocked` | there are blockers; fix them and make the plan again |

`blocked` wins whenever `blockers` is not empty, and a plan whose result has
blockers exits 1. Prepare and apply refuse a plan that has blockers with `Error:
plan has unresolved blockers` and do not list them; make the plan again to see
them.

**Prepare** builds the generation at its final path without activating it and
returns a new plan, now `ready`. Nothing that jobs use changes, but it does run
the registration's install or update script and its post-update script in the new
tree, and it fetches the commit from the configured origin when the checkout
does not have it yet. The build transcript is kept under `<state root>/admin-updates/<program>/`; a failed
preparation names that file and quotes the last lines of the failing script.

**Apply** takes the digest of the ready plan and a receipt directory that does
not yet exist, whose parent you own. It re-checks the plan under the admin and
lifecycle locks, runs the program's availability probes against the prepared
generation, stages the new bytes and the exact rollback bytes durably, and only
then replaces the bindings one at a time, journalling each step. A stale config,
candidate, pointer, source identity or generation changes the digest and is
refused. With several registries the switch is recoverable, in sequence: it is
**not** an atomic commit across them. Apply also records the receipt directory in
an index under `<state root>/runtime-migration/receipts/`.

### Resume and roll back

```sh
vq admin migrate-runtime --receipt-dir /opt/example/review/transaction-001 --resume
vq admin migrate-runtime --receipt-dir /opt/example/review/transaction-001 --rollback
```

These take only `--receipt-dir`, and both are idempotent. They print the
transaction record, whose top-level `state` is `initializing`, `prepared`,
`applying`, `applied`, `rolling_back` or `rolled_back`. Run them as the account
and state root the plan was made under: recovery refuses a different queue state
root, takes the admin and lifecycle locks, and re-checks the service blockers, so
a daemon that has appeared since refuses a rollback.

Once a rollback has started, finish it before making a new plan. A rolled-back
transaction cannot be applied again: make a new plan and a new receipt. Recovery
refuses to overwrite an edit you made after the apply, so restore or reconcile
that edit first. Rollback also re-verifies, content hash included, the
generations the old `current` and `previous` pointers named, so do not prune
either before you are sure. It restores the old registry bytes and pointers and
removes only a launcher this transaction created. No source, venv, job or archive
moves. The staged files, `.vq-migrate-*` beside each bound file, are kept.

A transaction stuck in `initializing` has changed no live binding: both commands
refuse it with "initialization interrupted; original bindings are intact;
replan". Make a new plan and a new receipt.

### Afterwards

A migration does not prove that the component works. Submit a small job with
`--program`, confirm `vq programs HOST --json` names the generation you expect,
and check a launcher that a scheduled or scripted caller uses. Processes that
were running before the flip keep importing from the old generation, so check
those late imports before you delete anything.

## `vq runtime-python`

```sh
vq runtime-python --root /absolute/runtime/root [--expected-sha FULL_SHA] -- ARGS...
```

This is the primitive a stable launcher calls: it runs Python from the store's
current verified generation. The launchers `migrate-runtime` writes are exactly
`exec BOOTSTRAP runtime-python --root ROOT -- ARGUMENTS "$@"`, mode 0700. You
rarely type it yourself.

It reads `current` once and requires that generation's verified markers, then
executes `releases/<sha>/source/<venv>/bin/python`, where `<venv>` is `.venv`
unless the program registers a source-local name such as `.venv-viewer`. It
replaces itself with the interpreter, so the working directory, arguments,
streams, signals and exit status are the interpreter's. Python's own flags go
after the `--`. It exports `VQ_RUNTIME_SLOT_SHA`, `VQ_RUNTIME_SLOT_SOURCE` and
`VQ_RUNTIME_SLOT_PYTHON`, naming what it selected.

`--expected-sha` refuses any other current generation. The launch checks markers
and structure; it does not hash the generation again, which belongs to sealing.
A flip of `current` after the launcher has selected a generation affects the
next launch, not the running process.

It refuses, with `Error: ...` on standard error and exit status 1, when:

* `--root` is not absolute, contains a symlink, is owned by someone else (root may
  use any owner) or is group- or world-writable;
* there is no active generation (`no active runtime slot at ROOT`), `current` is a
  real directory rather than a link into `releases/`, or it names a generation
  that is missing, unfinished or malformed;
* `--expected-sha` is not a full 40-character lowercase SHA, or does not match.

There is no fallback to a legacy environment, by design. The command launches an
already activated runtime; it installs no wrapper and migrates no registration.
