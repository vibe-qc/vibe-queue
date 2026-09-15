# v0.7.1 *Lamport's Clock* — operator-visibility hardening for `vq admin update`

**Audience:** the next vq dev chat (or operator) that wants to
understand why `vq admin update` got a multi-item hardening pass
in v0.7.1 and what each piece does.

**Status as of v0.7.0 (2026-05-25):** `vq admin update` records
a single `last_success: bool` per env per host and nothing else
visible to the operator. The 2026-05-25 fleet-update incident
proved that single bool is not enough: when an update fails, the
operator has to SSH into the host, hunt for logs, and reverse-
engineer "what went wrong" from scratch. v0.7.1 closes that gap.

**Why "Lamport's Clock"**: Leslie Lamport's 1978 paper *"Time,
Clocks, and the Ordering of Events in a Distributed System"*
established that in a distributed system you cannot trust a
single instantaneous reading — you need ordered events with
causal links to make sense of what happened. `vq admin status`
is exactly that problem: today it reads as if `LAST OK=False` is
a single fact, but the fact has a *cause* (the script's stderr),
a *prior cause* (the branch the env is on), and a *prior-prior
cause* (the args the script was invoked with). v0.7.1 records
the causal chain so the operator can read it.

---

## The 2026-05-25 incident — postmortem

The incident that motivated v0.7.1. Real chronology, sanitized.

### Symptoms

1. **compute-d vibeqc-dev** silently checked out `release` despite
   `config.toml` saying `branch = "main"`. `vq admin status
   compute-d` showed `BRANCH=main` (config field, not actual HEAD)
   so the drift wasn't visible.
2. **compute-a vibeqc-dev** had 141 modified basis-set `.g94` files
   from a basissetdev populate-bug, blocking the next `git pull`.
3. **Both hosts** had `vibeqc-dev` venvs in a hybrid state after
   the host Python upgraded from 3.14.4 to 3.14.5 (pyvenv.cfg
   pointed at the old interpreter; venv binaries were the new
   ABI).
4. `vq admin update vibeqc-dev <host>` flipped `LAST OK` to False
   but the operator had **no way to see why** without SSHing in
   and reading `~/.local/share/vq/client.log` or the daemon log.

### Root causes

* **A1 — vibe-qc-side argv loss across niced re-exec.** The
  `scripts/_safe_build_env.sh` helper re-execs the calling
  script under `nice -n 19 ionice -c 3` for build-pressure
  safety. It was passing `"$@"` to the re-execed bash — but
  `update.sh` had already parsed `--dev` / `--branch X` and
  shifted the args out by the time the helper sourced. So the
  re-exec dropped every flag. `update.sh` then fell through to
  its default branch (`release`), silently switching the dev
  clone to the release branch. Fixed vibe-qc-side in commit
  `ea195796` (`_VIBEQC_UPDATE_ORIG_ARGS` snapshot before
  parser; helper uses snapshot if set).
* **A2 — basissetdev populate-bug on compute-a.** Out of scope for
  v0.7.1 (basis chat's territory; see § "Slips" below).
* **A3 — venv hybrid after host Python upgrade.** Recovered by
  `bash scripts/update.sh --dev --recreate-venv` run manually
  on each host. Catchable in v0.7.1 by `--update-script-arg
  --recreate-venv` (item 3 below).
* **B — vq-side operator visibility hole.** Even with A1 fixed
  upstream, the *next* time the build helper drops argv (or
  any other script-side failure happens), vq's response is the
  same single `LAST OK=False` bool. The operator's recovery
  loop is "SSH → hunt logs → guess → re-run". v0.7.1's scope
  is **shrinking that loop to seconds and making it
  client-side**.

### Recovery cost

| Phase | Wall time |
|---|---|
| Diagnose compute-a dirty-tree drift | ~45 min |
| Rescue compute-a basis artifacts to branch | ~10 min |
| Re-run compute-a update | ~25 min |
| Diagnose compute-d branch drift | ~30 min (because BRANCH column showed config not actual) |
| Re-run compute-d (3 failed cycles before --recreate-venv was tried) | ~3 hours |
| Fix vibe-qc `_safe_build_env.sh` argv loss | ~20 min |
| **Total** | **~5 hours** for what should have been a 30-min routine fleet update |

The single biggest time sink was **compute-d's three failed
cycles** — each one a `vq admin update` that flipped LAST OK
to False with no surfaced explanation, prompting another SSH-
and-guess cycle. v0.7.1 items 2 + 6 (persist + show
`update_script_output`) are specifically aimed at this.

---

## v0.7.1 scope — six additive items

All six are **additive** — no breaking changes to JobSpec,
AdminUpdateRecord, RPC protocol, or config. Old clients keep
working against new daemons; new clients reading old records
see defaults.

### Item 1 — Post-update branch validation

**Problem.** Config says `branch = "main"` but the checkout is
silently on `release`. The current update flow runs `git pull`
inside whatever branch HEAD points at, so the pull "succeeds"
but the env is on the wrong branch.

**Fix.** After `git pull` and before `record_update_outcome`,
read `git rev-parse --abbrev-ref HEAD` and compare to
`VenvProgram.branch`. Mismatch → update fails with explicit
reason `branch_mismatch: expected=main, actual=release`.

**New field.** `AdminUpdateRecord.last_branch_actual: str |
None`. Surfaced in `vq admin status` as a new column when the
mismatch is present (else hidden to avoid column bloat).

**Files.**
- `vibe-queue/src/vq/admin.py` — `update_env` adds the check;
  `AdminUpdateRecord` gets the field; `format_admin_status`
  surfaces mismatch.
- `vibe-queue/tests/test_admin_branch_validation.py` (new) —
  mismatch fails, match passes, missing-config-branch skips
  (back-compat).

**Edge cases.**
- Detached HEAD: `git rev-parse --abbrev-ref HEAD` returns
  `HEAD`. Treat as mismatch unless `branch` is unset.
- Empty `branch` in config (legacy envs): skip the check,
  same behavior as today.

### Item 2 — Persist update_script_output tail

**Problem.** When `update.sh` fails (e.g., dirty tree, pip
install error, build segfault), its stdout/stderr go to the
client log file but not into `admin-status.json`. Operator
sees `LAST OK=False` and nothing else.

**Fix.** Capture last N lines (default 80, configurable via
`VQ_ADMIN_UPDATE_OUTPUT_LINES`) of update script output into
`AdminUpdateRecord.last_update_script_output: str | None`.
Surface via:
- `vq admin status` (text): unchanged by default; add
  `--verbose` to print the tail below each row that has
  `LAST OK=False`.
- `vq admin status --json`: always include the field.

**Why bound the tail.** A full build log can be 30+ MB; we
don't want admin-status.json to balloon. 80 lines (~6 KB
typical) captures the failure mode for every observed case
and stays comfortably under any state-file size concern.

**Files.**
- `vibe-queue/src/vq/admin.py` — `update_env` captures stdout
  via a tail-truncating buffer; `AdminUpdateRecord` gets the
  field; `format_admin_status` learns `--verbose`.
- `vibe-queue/src/vq/cli.py` — `admin status` gains
  `--verbose` flag.
- Tests: tail truncates at N, --verbose renders, JSON always
  includes, success runs leave the field empty (don't store
  successful build noise).

### Item 3 — `--update-script-arg` pass-through

**Problem.** No way to ask `vq admin update` to forward
`--recreate-venv` (or `--dev`, or any other update.sh flag).
Operators currently SSH + run `bash scripts/update.sh
--recreate-venv` manually, bypassing the marker/record
machinery entirely. The 2026-05-25 incident hit this three
times on compute-d.

**Fix.** Repeatable `--update-script-arg <flag>` on `vq admin
update` (and `vq admin auto-update`) that appends to the
update.sh invocation. Passes through unchanged across the
multi-user RPC boundary.

**Example.**
```sh
vq admin update vibeqc-dev compute-d \
    --update-script-arg --recreate-venv \
    --update-script-arg --dev
```

**Files.**
- `vibe-queue/src/vq/cli.py` — flag on `admin update` +
  `admin auto-update`.
- `vibe-queue/src/vq/admin.py` — `update_env` accepts
  `update_script_args: list[str] = []`; `run_update_script`
  appends them to the bash invocation.
- RPC: `admin_update_remote` forwards the list (single arg
  is fine; list serializes through the existing transport).
- Tests: single flag, multiple flags, multi-user token still
  works alongside --update-script-arg, flags are
  shell-quoted (defense against `--update-script-arg "; rm
  -rf /"` though this requires admin-token-gated access).

**Security note.** This is admin-token-gated in multi-user
mode (`vq admin update` already requires the token). The
flags execute as the user running the script (not root via
the daemon), same trust boundary as the rest of the update
pipeline.

### Item 4 — `vq admin mark-ok` operator escape hatch

**Problem.** When the operator verifies an env is healthy
out-of-band (e.g., manually rebuilt via SSH + heredoc, like
2026-05-25), the only way to flip `last_success: false →
true` is a surgical Python edit of `~/.local/share/vq/admin-
status.json`. This is fragile, undocumented, and leaves no
audit trail of why the record was flipped.

**Fix.** New verb:
```sh
vq admin mark-ok ENV HOST --note "REASON"
```

Writes `last_success: true`, `last_marked_ok_at: <iso>`,
`last_marked_ok_note: "REASON"` to the record. `--note` is
required (no silent flips). Admin-token gated in multi-user
mode (matches `update` / `auto-update`).

**Display.** `vq admin status` shows a `*` after `LAST OK`
when the True came from a `mark-ok` rather than a real
update. `--verbose` shows the note. JSON always includes
both fields when present.

**Files.**
- `vibe-queue/src/vq/admin.py` — `mark_env_ok(env, note)`
  helper; `AdminUpdateRecord` gets two new optional fields.
- `vibe-queue/src/vq/cli.py` — `admin mark-ok` command.
- Tests: flip works, --note required, gate fires, audit
  trail in JSON, format_admin_status shows `*` marker.

### Item 5 — Dirty-tree `LAST OK` policy

**Problem.** Today `LAST OK=True` for a dirty tree is fine
because dirty is expected for some envs (vibeqc-dev with
basissetdev artifacts). But for envs that *should always be
clean* (vibeqc-queue, vibeqc-release), a dirty tree is a red
flag — typically an aborted update or a fleet-cleanup that
went wrong.

**Fix.** Config knob on `VenvProgram`:
```toml
[programs.vibeqc-queue]
kind = "venv"
branch = "main"
fail_on_dirty = true   # NEW; default false (back-compat)
```

When set, post-update dirty tree flips `LAST OK=False` with
reason `dirty_tree_after_update: <N> files modified`. Envs
that expect dirty (dev clones in active research) leave it
false.

**Files.**
- `vibe-queue/src/vq/config.py` — `VenvProgram.fail_on_dirty:
  bool = False`.
- `vibe-queue/src/vq/admin.py` — `update_env` checks after
  pull+build, respects the flag.
- `vibe-queue/config.toml.example` — annotated example with
  recommendation: `true` for queue/release, `false` for dev.
- Tests: opt-in flips False, opt-out tolerates, default
  behavior unchanged.

### Item 6 — `vq admin update --show-output`

**Problem.** Even with item 2 (persisted output tail), the
operator has to run a second command (`vq admin status
--verbose`) to see *why* the update they just ran failed.
For interactive use, that's friction.

**Fix.** `vq admin update ENV HOST --show-output` prints the
captured output tail to stderr immediately on failure (in
addition to persisting). Default off (preserves current quiet
UX); flag opts in. Honored across `--all` and `--all-hosts`
too — useful for "I just ran the whole fleet, which host
failed and why?".

**Files.**
- `vibe-queue/src/vq/cli.py` — flag on `admin update` +
  `admin auto-update`.
- `vibe-queue/src/vq/admin.py` — `update_env` returns the
  captured output in `UpdateResult.script_output`; CLI emits
  on failure when flag set.
- Tests: flag emits on failure, suppressed on success, --json
  mode unaffected (output goes to stderr, JSON stays clean
  on stdout).

---

## Out of scope — explicit slips

These were considered for v0.7.1 and deliberately deferred:

* **`vq admin reset-branch ENV HOST`** — auto-fix branch drift
  by `git checkout <config-branch>`. Decided this is too
  aggressive for v0.7.1: the operator should see the drift
  (item 1 surfaces it), then make the explicit decision about
  whether to switch branches or fix the config. Slipped to
  v0.7.2 once we have feedback on how often item 1 fires.
* **`vq admin update --stash-dirty`** — git stash before pull,
  restore after. Tempting for the compute-a basis-artifact case but
  the right fix is for the basis chat to clean up its
  populate-bug; vq encouraging "just stash it" papers over
  that. Slipped indefinitely.
* **State-file location unification.** `admin-status.json`
  currently lives at `~/.local/share/vq/admin-status.json`
  when invoked by a user, but the daemon's auto-update writes
  to a separate state root (root's XDG, or `$VQ_STATE_DIR` if
  set). v0.7.1 leaves this split alone — fixing it touches
  the cleanup sweep + per-user state + daemon RPC in ways
  that are out of scope for an operator-visibility ship.
  Documented as a known footgun in `docs/operations.md`.
* **Dev-branch tracker for `vq admin auto-update`** (was the
  *Ritchie's Pipe* candidate). Still valid as a future ship,
  bumped behind v0.7.1 because operator visibility is the
  more-acute pain after today's incident.

---

## File touch list (preview)

```
vibe-queue/src/vq/admin.py            # items 1, 2, 4, 5, 6
vibe-queue/src/vq/cli.py              # items 2, 3, 4, 6
vibe-queue/src/vq/config.py           # item 5
vibe-queue/config.toml.example        # item 5
vibe-queue/tests/test_admin_branch_validation.py  # item 1 (new)
vibe-queue/tests/test_admin_output_capture.py     # item 2 (new)
vibe-queue/tests/test_admin_update_script_args.py # item 3 (new)
vibe-queue/tests/test_admin_mark_ok.py            # item 4 (new)
vibe-queue/tests/test_admin_fail_on_dirty.py      # item 5 (new)
vibe-queue/tests/test_admin_show_output.py        # item 6 (new)
vibe-queue/docs/roadmap.md            # promote 0.7.1 to shipped
vibe-queue/docs/STATUS.md             # current ship line
vibe-queue/docs/version_compatibility.md  # vq 0.7.1 row
vibe-queue/docs/operations.md         # mark-ok + state-file footgun
vibe-queue/pyproject.toml             # version bump
vibe-queue/src/vq/__init__.py         # version bump
CHANGELOG.md                          # vq block update
```

Estimated 350–500 LOC including tests. One coherent ship per
the v0.7.x "feature rich and mature" guidance (CLAUDE.md
discussion 2026-05-25). Ships as v0.7.1 *Lamport's Clock*.

---

## Migration / rollback

* **Migration:** none required. All record-schema additions
  are optional. Old admin-status.json files load cleanly
  (TypeError in `read_admin_status` was already handled by
  the AdminUpdateRecord(**rec) constructor — extra keys
  ignored, missing keys take dataclass defaults).
* **Rollback:** an older vq client reading a v0.7.1-written
  record sees the new fields and ignores them (dict
  superset). A v0.7.1 client reading an old record sees
  `None` for the new fields, renders accordingly.
* **Deploy order:** ship to laptop client first, then fleet.
  The fleet update will write enriched records that old
  clients can still read. Reverse (fleet first, laptop
  client later) also works.

---

## Related docs

* `docs/wall_time_design.md` — precedent for vq design docs.
* `docs/STATUS.md` § "Recent ships" — v0.7.0 *Hoare's
  Pipeline* and the incident link.
* `docs/roadmap.md` § "v0.7.x roadmap" — Lamport's Clock
  entry to promote from queued → shipped on tag.
* CLAUDE.md § 15 — agent interaction protocol (where the
  "don't write to git checkouts" lesson lives).
