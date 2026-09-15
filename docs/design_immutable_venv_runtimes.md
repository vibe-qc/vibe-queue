# Immutable runtimes on venv hosts

**Status: APPROVED 2026-07-26 (maintainer: "We can properly refactor, as
needed"). Scope narrowed by the maintainer in the same exchange — see §0.
Implementation gated only on fleet quiescence, not on approval.**

## 0. The requirement, as finally stated

> "We are in a phase where we constantly rolling release. But this should not
> halt everything all the time. They can build and update in the background,
> everything newly submitted gets the latest version. period"
>
> "The only thing we want is that we do not have to drain all queued
> calculations… the actively running calculations runs with the current version,
> but meanwhile the updater runs and as soon as it is ready newly submitted
> calculations run with the new version."
>
> "The wait is just too long… the new patch release might not even concern what
> is running… otherwise updating takes forever and the queue has calculations
> running."

So: **build in the background, never halt the queue, running jobs keep their
version, newly submitted work gets the new one.** No drain, no pause.

This is narrower than the original proposal and **drops** three things it
carried: a deep rollback archive, retention/GC pressure, and hoisting
`third_party/` out of the checkout (which would have changed the human developer
workflow). None are needed.

On *"the patch release might not even concern what is running"* — true, and it is
the argument **for** this design. Today an operator has to judge whether a patch
touched a running job's code path, and a wrong judgement produces rows that look
identical to good ones. Keeping a running job on its own runtime removes the need
to judge at all.

### Disk: measured, and a non-issue

| host | checkout | `third_party` | free |
|---|---|---|---|
| compute-d | 7.0 G | 5.1 G | **776 G** |
| compute-c | 6.8 G | 5.1 G | **206 G** |
| compute-b | 6.8 G | 5.1 G | **1.8 T** |
| compute-a | 5.6 G | 3.9 G | **339 G** |

A duplicate runtime costs ~7 GB against 30–250× headroom. That is what kills the
deps-hoisting complexity: each slot can simply carry its own `third_party/` and
`build/`, which also keeps its builds warm with no shared-cache machinery.

~~Cheaper still: seed a new slot by hardlink-cloning (`cp -al`) the previous
slot's `third_party/` and `build/`.~~ **CORRECTED 2026-07-28 — a venv cannot be
seeded by copy or hardlink at all.**

The editable install is a `.pth` holding an **absolute** path into its checkout:

```
$ cat <venv>/lib/python3.14/site-packages/_editable_impl_vq.pth
/home/USER/vq/vibeqc-queue/vibe-queue/src
```

So a venv is bound by absolute path to the slot it was created in. Copying
`releases/<shaA>` to `releases/<shaB>` produces a venv still importing
`<shaA>`'s source — a slot that silently runs the wrong code, which is worse
than the mutation this design exists to prevent. **Each slot must create its own
venv in place.**

The second consequence is sharper, and it constrains the layout rather than just
the seeding. **A slot's venv must never point through `current`.** If the `.pth`
read `<root>/current/src`, flipping `current` would change a *running* job's
source out from under it — reintroducing precisely the in-place mutation §1
documents. The `.pth` must name the slot directly
(`<root>/releases/<sha>/src`), and `current` must be resolved by the **wrapper at
exec time**, choosing which slot's `bin/python` to run.

Absolute-path binding also rules out building a venv in a uniquely named
staging directory and renaming it into `releases/<sha>`. Console shebangs and
editable-install path files would still name the staging directory after the
rename. The implementation therefore builds directly at the final
`releases/<sha>` path. A durable sibling build receipt and matching in-slot
`building` marker identify an incomplete transaction. A retry may remove only
that exact incomplete generation, and only after `current`, `previous`, and a
complete non-terminal job-spec census prove it is unused. Verification writes
the content seal and `.vq-immutable-runtime` marker before activation; direct
lifecycle scripts refuse a marked generation.

That is exactly slurm-cluster's per-SHA-wrapper pattern, and it independently confirms
§2 Q3: the venv hosts need slurm-cluster's mechanism, not a new one.

The first update must also work when the live checkout has never fetched the
requested commit. Materialization reuses its existing Git objects, then fetches
an absent exact commit from its configured origin into the unpublished slot.
The live checkout's working tree and refs remain untouched. A relative local
origin is resolved against the live checkout before use from the slot. Missing
or unreachable source remains a failure before activation; the updater never
asks an operator to fetch manually inside the live managed checkout.

What this costs: a new slot pays a real venv creation plus an extension build.
What it does not cost is the native dependency tree — `third_party/` install
prefixes are consumed at build time rather than imported at run time, so they can
still be shared or copied between slots, and ccache still covers the compile.
The §1c question (does the build replace artifacts or rewrite them in place?)
therefore no longer gates this design; it remains worth answering for its own
sake.

### Shared launcher (issue #577)

`vq runtime-python --root /absolute/runtime/root -- "$@"` is the execution
boundary for a stable Python wrapper. It reads `current` once, requires that
exact generation's verified markers, checks its directory and interpreter
shape, and execs `releases/<sha>/source/.venv/bin/python`. It does not realpath
the interpreter itself: ordinary venv Python symlinks point at the base
interpreter, and executing that target would lose the venv. A concurrent
activation affects the next launch. It cannot retarget the already selected
generation or the late imports of an existing process.

An optional `--expected-sha FULL_SHA` refuses a different current generation.
Python arguments follow the `--` separator and retain their exact boundaries.
The launcher preserves cwd, streams, process identity, signals and exit status;
it records the selected paths in `VQ_RUNTIME_SLOT_SHA`,
`VQ_RUNTIME_SLOT_SOURCE` and `VQ_RUNTIME_SLOT_PYTHON`. Missing, unfinished or
malformed generations fail without falling back to a legacy environment.

Full content hashing belongs to sealing and activation. Launch checks the
verified markers and structural paths; it does not repeat a multi-gigabyte
integrity audit for every Python call. Owner edits inside a verified generation
remain forbidden. Retention must preserve every generation referenced by a job,
including one selected before a subsequent pointer flip.

This command is only the launcher primitive. Configuring `runtime_slot_root`
alone does not install a wrapper or redirect a program's registered `python`
and `git_dir`. Host migration must still bind registration, runtime provenance
and job retention to the exact slot, prove the old and new launch paths, and
preserve existing legacy jobs. No host migration is implied by this command's
availability.

## 0b. Origin

Raised by the maintainer 2026-07-26 via the fleet updater chat; written by the
vq dev chat. The maintainer's question was:

> "Why do we have to drain the queue? Can't we pause the queue and then run the
> queued calculations that were paused simply with the latest release version
> after the update?"

Short answer: pending rows already behave that way, and the pause is not
protecting them — it is failing to protect the *running* ones. The instinct is
right, and the fix is to stop mutating a runtime that a live process is
importing from.

## 1. Verification of the load-bearing claim

The proposal's justification — that a resumed job mixes old and new code in one
process — was flagged as read from the docs rather than reproduced. **It is
confirmed**, and the mechanism is worse than described.

### 1a. Reproduction

A minimal fixture: a process imports one module up front, is `SIGSTOP`ped, has
both modules overwritten in place, is `SIGCONT`ed, then imports a second module
lazily. One process, two versions:

```
early_at_start=v1
early_after_resume=v1     <- served from sys.modules, cached pre-update
late_after_resume=v2      <- read from disk after resume
```

No exception, no warning. `sys.modules` pins what was already imported; any
later import reads current bytes off disk. This is ordinary CPython behaviour,
not a vq bug — but the update path walks straight into it.

Confirmed against the tree, not inferred:

* `vq admin update` pauses with `SIGSTOP` (`pause_resume.py`, via `pause_all`).
* The documented semantics are verbatim at `cli.py:9428-9429`: *"Paused jobs
  continue with whatever bytecode they already imported; only NEW dispatches
  after this call see the fresh build."*

### 1b. Why it is worse than "stale files get overwritten"

`scripts/update.sh` installs **editable** —
`vibeqc_pip_install_editable "$VENV" "$EXTRAS_SPEC" "."`. The venv does not hold
a copy of vibe-qc; it resolves straight into the git checkout. So the mutating
step is not `pip` rewriting `site-packages`, it is **`git pull` rewriting the
exact files the live interpreter imports from**. There is no copy anywhere in
the path insulating a running process. Any lazy import after resume gets
post-pull source.

This matters for reachability: vibe-qc genuinely imports late. Post-SCF
property, population, QVF and output modules are reached only after the SCF
converges, which is precisely the window a long job is paused in. This is a
reachable silent-wrong-numbers path, not a theoretical one.

### 1c. A second, distinct hazard — NOT yet verified

The compiled extension is a different failure mode from the Python mixing. A
running process has the `.so` **mmap'd**. If the rebuild writes the file in
place (truncate + write) rather than replacing it by rename, the mapped pages
change under the live process — that is a `SIGBUS` or torn code, not clean
mixing. If the build renames into place, the running process keeps its old
inode and is safe.

Which one happens depends on how scikit-build/ninja emits the artifact into the
stable `build/{wheel_tag}` tree. **I have not verified this** and it should not
be asserted either way in the runbook until someone does. It does not change the
proposal's direction — both hazards are cured by not mutating a live runtime.

### 1d. Honest scope of the exposure

A job is only affected if it is **both** paused across an update **and** imports
something for the first time after resume. A job that has already imported
everything it needs finishes correctly. So this is not "every paused job is
corrupt" — it is "some paused jobs are silently corrupt and nothing tells you
which". That is still disqualifying for release-paper evidence, because the
affected rows are indistinguishable from good ones.

## 2. Answers to the five design questions

### Q1 — Disk and retention

**CORRECTION (2026-07-26).** An earlier revision of this section claimed the
existing pruner could delete a bundle a running job holds. That is **wrong for
release bundles.** `admin.prune_scheduler_stage_generations` operates only on
`stage_root/generations/`, matching `_SCHEDULER_STAGE_GENERATION_RE`, and
**nothing in `admin.py` prunes `releases/<sha>` at all** — which is exactly why
pbs-cluster has accumulated 11. No job ever executes from a staging generation, so the
mtime-rank retention there is harmless.

The real consequence is the opposite of what that revision implied: because
release bundles are never reclaimed, **a live job's runtime is already safe from
deletion today**, and the drain on the scheduler-runtime path is protecting
nothing. The work is to *add* liveness-aware cleanup for the new venv-host slots
so they do not accumulate forever — not to fix a pruner that is about to eat a
running job.

Retention always keeps `current`, `previous`, and every generation proven by an
exact per-SHA command in a running or suspended spec. Normal specs can retain a
stable `current` or wrapper command even though the process resolved a specific
generation at exec time. Such a spec cannot prove its resolved SHA after later
flips, so reclamation conservatively keeps **all** generations until every such
spec is terminal. An unreadable or changing spec census aborts the entire
reclamation pass without deleting anything. With ~7 GB per slot against
206 GB–1.8 TB free (§0), this deliberate temporary leak is the safe tradeoff.

Sizes, measured on a real checkout:

| tree | size | nature |
|---|---|---|
| `third_party/` (5 native dep install prefixes) | **4.9 GB** | gitignored build product, **inside the checkout** |
| `build/` (extension build tree) | **551 MB** | gitignored build product, inside the checkout |

A naive "bundle = copy of the checkout" is therefore **~5.5 GB per release**.
pbs-cluster's 11 retained generations would be **~60 GB per program**, times three
programs, on hosts that are not provisioned for that.

**Requirement:** retention must be **liveness-aware** (refcount bundles against
running + suspended specs, never a bare mtime rank), and a bundle must be small
enough that keeping a few is cheap — which forces Q2's answer.

### Q2 — Native dependency reuse

> **Superseded by §0.** The shared key-identified `deps/` prefix below is no longer the plan: measured disk headroom makes a self-contained per-slot `third_party/` simpler, and avoids changing the developer-facing layout. Kept for the evidence about where native deps live and what a naive whole-checkout copy would cost.

Native deps install to `third_party/<dep>/install/` **inside the checkout** and
are gitignored. That is why a per-SHA copy of the checkout both duplicates
4.9 GB and forfeits `dependency_cache=reused`, turning every release into
slurm-cluster's ~21-minute cold build instead of ~8 warm.

**Answer: split the layers.** The immutable unit should be the **Python layer
only**. Native dep prefixes and the compiler cache are *content-identified
shared state*, not part of the release identity:

```
<runtime-root>/
  deps/<key>/                  shared, one copy per toolchain key
  ccache/<key>/                shared, persistent
  releases/<sha>/              immutable, Python layer only (small)
  current   -> releases/<sha>
  previous  -> releases/<sha>
```

where `<key>` covers what the existing ccache scope already keys on: dep
versions, compiler content, Python ABI, architecture, CMake option generation.
Two releases with the same key share `deps/` and `ccache/`; a dep bump mints a
new key and rebuilds once.

This keeps warm builds warm *and* makes bundles small enough that liveness-aware
retention of several is affordable.

### Q3 — Dispatch-time resolution

The interpreter is resolved at **submit** time, not dispatch:
`submit.py:843` — `interp = python or host_cfg.remote_python or "python"` — and
baked into the spec's command. Genuine dispatch-time resolution would mean
changing the spec model.

**It should not be changed, because slurm-cluster already solves this without
touching vq at all.** slurm-cluster uses per-SHA wrapper scripts
(`vibeqc-release-0.15.63-<sha>-python`) behind a stable
`vibeqc-release-python` that is flipped atomically. Resolution therefore happens
at **exec** time:

* a **pending** row execs the stable wrapper *after* the flip → new bundle;
* a **running** row already execed → holds its old bundle for its whole life.

That is exactly the semantics the proposal asks for, with **zero change to vq's
spec model, submit path, or dispatch loop**. The venv hosts do not need a new
mechanism; they need slurm-cluster's mechanism.

Pinned submits keep working: per-SHA wrappers carry an in-wrapper
`expected_sha`, and the BUG 101 fix (`e5fc87ce` + `39de4a874`) already resolves a
scheduler-target's identity from the immutable wrapper filename rather than the
driver-local program. `remote_python` and `branches.{main,release}` point at the
stable wrapper — the same repoint already performed for slurm-cluster.

### Q4 — Migration

> **Superseded by §0, and see the 2026-07-28 correction there.** There is no dep hoisting to order first. Hardlink-seeding does NOT apply to the venv, which is absolute-path-bound and must be created per slot; it still applies to `third_party/`, which is consumed at build time rather than imported.

**No fleet-wide cold rebuild is required, if the increments are ordered
correctly.** Hoisting `third_party/` to the shared `deps/<key>/` prefix is a
behaviour-preserving change that can land *first*, on its own. Once every host
has a warm shared dep prefix, the first immutable bundle is a warm Python-layer
build, not a cold one.

Ordering it the other way round — bundles first — is what forces one cold
rebuild per host per program, and on slurm-cluster that is ~21 minutes inside a SLURM
allocation. Recommend against.

Caveat needing a decision: `scripts/update.sh` is also the **human developer**
entry point, so hoisting `third_party` out of the checkout changes local dev
workflow, not just the fleet. See §4.

### Q5 — Does the drain become unnecessary?

**Partly, and not yet.** The active-job guard is applied in two distinct places:

* `_update_scheduler_runtime_guarded`, refusing with
  `_active_jobs_refusal(host, drain_outcome, "replacing a runtime")`;
* `_update_scheduler_host_guarded`, for the **helper** update.

> **Renamed.** Both functions were `*_locked` when this was written. Line
> numbers are deliberately not quoted any more — they were stale within days.

For **runtime replacement** on an immutable-bundle host the guard protects
nothing: a running job holds its own bundle and an atomic `current` flip cannot
reach it. That guard, and `--drain-wait` on the runtime path, can go — **but only
after liveness-aware retention (Q1) lands.** Dropping the guard while the pruner
still ranks by mtime would simply move the breakage from "mutated under you" to
"deleted under you".

For the **helper** update the guard has an independent justification and should
stay: it replaces the vq that the scheduler side uses to poll and dispatch, which
is not covered by chemistry-runtime immutability. That reason is currently
undocumented and should be written down rather than left implicit — otherwise the
next reader deletes it as dead weight.

So: remove one guard with a stated reason, keep the other with a stated reason.
Do not leave a guard in place that no longer guards anything.

> ### CORRECTION 2026-08-01 — the helper justification above was wrong
>
> The paragraph beginning *"For the **helper** update the guard has an
> independent justification"* rests on a false premise, and it cost a real job:
> on 2026-08-01 a paper-critical GPW calculation four hours into a twelve-hour
> budget was killed to satisfy this wait, for a rebuild that could not have
> touched it.
>
> It says the helper "replaces the vq that the scheduler side uses to poll and
> dispatch". **The scheduler side does not poll or dispatch.** Verified against
> the tree:
>
> * pbs-cluster and slurm-cluster run **no vq daemon at all** — `scheduler_dispatch`'s own
>   docstring calls the architecture *"Arch 2 — off-cluster, SSH-driven,
>   stateless-on-cluster"*, and `HostConfig.scheduler_driver` says *"A scheduler
>   host (e.g. pbs-cluster) runs no vq daemon of its own"*. `HANDOVER_FLEET.md` has
>   called it "pbs-cluster's daemonless helper" for months.
> * `qsub` / `qstat` / `qdel` are **raw shell over SSH from the driver**
>   (`SshRemoteRunner.run` → `transport.run_remote_shell`), never
>   `run_remote_vq`. All live job state is the driver's `_scheduler_running`.
> * the helper is invoked as a short-lived process, one per call, for exactly
>   three things: `--version`, `source-sha` / `source-tree-sha256`, and
>   `source-stage-prune`. Provenance and stage GC.
>
> **Correction, same day:** an earlier revision of the bullet above said those
> calls run "over a fresh SSH connection with multiplexing explicitly
> disabled". That is wrong. `transport._multiplex_bypass_options()` sets
> `ControlMaster=no` / `ControlPath=none` only when `fresh_connection=True`,
> which defaults to `False` and which `run_remote_vq` never passes — so vq
> inherits the operator's `~/.ssh/config`, and the fleet's blocks use
> `ControlMaster auto` + `ControlPersist`. Multiplexing is the norm, not the
> exception; the bypass is reserved for retries after a poisoned master
> socket. The conclusion is unaffected — a per-invocation *process* still
> holds its own root's inode, whatever the connection does — but the claim
> was wrong and would have misled anyone costing SSH round trips.
> * the generated PBS/SLURM job script contains **no vq invocation**, so
>   compute-node jobs do not depend on the helper either.
>
> There is therefore no long-lived process to swap under, and no in-flight row
> on the scheduler host to lose.
>
> The second half of the claim — *"no per-SHA bundle and no atomic flip
> contract behind it"* — was also already false when written. Both hosts'
> `scheduler_update_command` entry points build an immutable per-SHA helper
> root and switch the stable path by rename. The PBS and Slurm updater
> entrypoints now live in private operations. Both install into the per-SHA
> root directly rather than through the stable link,
> which is exactly the absolute-path-binding requirement §0 establishes.
>
> **Resolved as of 2026-08-01.** The wait is now gated on the site's own
> activation receipt (`VQ-DEPLOY-METRIC helper_activation=atomic` plus the
> active path and the SHA), consumed by `helper_activation_is_proven`. A host
> that proves atomic activation skips the wait; a host on the in-place path
> (`contrib/update-scheduler-vq.sh`, which rsyncs over the live tree and
> installs editable) emits no receipt and keeps it. The proof is keyed on the
> configured deploy command, so repointing a host fails closed.
>
> The receipt is read from the *previous* run, because the wait happens before
> the deploy. That is the right question anyway: "does this host's deploy script
> stage and flip?" is a property of the installed script, and both hosts re-sync
> their scripts from the freshly verified archive on every update. Expect a
> one-cycle bootstrap lag — the first update after this change still runs the
> old script, emits no receipt, and waits.
>
> Lesson worth keeping: this comment was written as a conservative assumption
> and never rechecked against the scripts it described. A guard justified by an
> unverified claim is not conservative — it is an unmeasured cost, and here the
> cost was four hours of paper-critical compute.

## 4. Status of the four earlier questions

All four are now closed by the maintainer's narrowing (§0) plus measurement:

1. **Split-layer over shared deps — no longer needed.** Disk headroom makes a
   self-contained per-slot `third_party/` + `build/` the simpler answer, with
   sharing `third_party/` between slots to keep the build warm. NOT the venv:
   see the 2026-07-28 correction in §0 -- it is absolute-path-bound and must be
   created per slot.
2. **Increment order — no dep hoisting to order.** Dropped.
3. **`scripts/update.sh` developer impact — none.** The script runs verbatim
   inside whichever slot is being built. Its layout does not change, so the human
   developer workflow is untouched. This was the question that would have
   materially reshaped the work; it is now moot.
4. **Q5 asymmetry — confirmed, and stronger than stated.** Because release
   bundles are never reclaimed (§2 Q1 correction), the scheduler-runtime drain is
   already protecting nothing on pbs-cluster/slurm-cluster *today*. The helper guard keeps its
   independent reason and must be documented rather than deleted as dead weight.

## 4b. Implementation increments

Each independently shippable, `main` release-ready throughout (§14). **Gated on
fleet quiescence, not approval** — see §4c.

| # | Increment | Touches | Gate |
|---|---|---|---|
| 1 | Correct the `admin update --help` pause semantics; record the hazard | `cli.py` docstring only, no behaviour | landed 2026-07-26 |
| 2 | Drop the active-job guard + `--drain-wait` from the **scheduler-runtime** path (pbs-cluster/slurm-cluster); keep and document the **helper** guard | `admin.py` update path | one pbs-cluster/slurm-cluster runtime update with jobs running, uninterrupted |
| 3a | Slot layout primitive (`vq.runtime_slots`): `releases/<sha>/`, atomic `current` flip, retained `previous`, liveness-refusing reclamation | new module, unreachable | landed 2026-07-27 (`dc55446c8`), released |
| 3b | Opt-in config surface: `VenvProgram.runtime_slot_root` | `config.py`, inert | landed 2026-07-28 |
| 3c | Wire the update path: reserve the final per-SHA path with durable build receipts, build there, content-seal it, then atomically flip | `admin.py` update path (`_do_slot_update_work`) | landed 2026-07-29 (`00eed7247`), transaction hardening 2026-08-13 |
| 4 | Point that host's `remote_python` / `branches.*` at the stable wrapper | config only | pinned `--expected-sha` submit still fails closed correctly |
| 5 | Explicit maintenance only: keep both pointers and exact per-SHA references; keep all generations for unresolved stable-wrapper specs | `runtime_slots.reclaimable`/`reclaim`/`slots_in_use` | primitive landed 2026-07-29 (`216deab80`); automatic post-activation reclamation removed September 7 under #577 |
| 6 | Roll the flag across the remaining venv hosts; drop pause+drain there; update the runbook | config + runbook | no pause, no drain, running work untouched |

Increment 2 is first because it is small, needs no new machinery, and returns
real wait time on the two hosts that already support it. Increment 3c is where
the venv hosts stop halting.

**Ordering constraint on 3b, sharper than it looks.** `_ProgramBase` sets
`extra="forbid"` and `load_config` raises `ConfigError` on a validation failure,
so a vq predating `runtime_slot_root` does not ignore the key -- it fails to
load the config at all, and *every* command on that host breaks. Shipping the
field is safe; writing the key into a config a host reads is not, until that
host runs a vq carrying it. Convert per host, after that host is updated.

This was left strict on purpose. `DrainState` went the other way (unknown keys
now tolerated) because it is machine-written state where a strict reader
silently un-drained a host. Program config is hand-edited, where rejecting a
typo is worth the ordering cost.

## 4c. Why this is not being written right now

A sweep was in flight at the time of writing: an admin-update marker on slurm-cluster
(`vq admin update vibeqc-dev slurm-cluster --expected-sha 136f4909 --drain-wait 4h`,
pid 30419) plus a concurrent compute-d update. The driver self-updates its own vq
during a rollout, so landing a behaviour change to the update path mid-sweep
risks the running updater picking it up half-applied. Increments 2+ start once
`vq admin status --all` shows no marker and no updater pid.

That live sweep is also a fair illustration of the problem: a `--drain-wait 4h`
on a host with running calculations is precisely the halt this work removes.

## 5. Explicitly out of scope here

* The six items the updater chat filed separately (multi-user
  `/var/lib/vq` ownership, precondition-before-build ordering, `fleet_rollout.py`
  phase grouping vs `fleet_rollout_order`, `admin logs` targeting, actor
  attribution, `admin status` column order). Context in
  `vibe-queue/HANDOVER_FLEET.md` and `docs/fleet_update_runbook.md` §3b–3d.
* The build-job memory dimension
  (`default_job_mem_mb` vs `max_mem_mb`), filed under the compute-a refresh item in
  `handovers/HANDOVER_OPEN_BUGS_V015.md`.
* Verifying §1c (mmap'd `.so` replacement semantics). Worth doing, does not
  block this decision.

## 6. Interim guidance until this lands

While venv hosts still mutate in place, a paused-and-resumed job on
`compute-d` / `compute-a` / `compute-b` / `compute-c` / `localhost` **cannot be trusted as
release-paper evidence** if the update crossed its lifetime. The conservative
operational rule is the one already in force — drain rather than pause on venv
hosts — which is exactly the cost this proposal removes.

## 7. The second halt: the driver-global update mutex

> "We just constantly run, find bugs, fix bugs, commit and release and continue
> running calculations. But there is no need that one of these steps keeps the
> other from working. It can all happen in parallel from many chats and users and
> developers."

Runtime slots (§4b) remove the halt between **running calculations** and **an
update**. They do not remove the halt between **one update and another**, which
is a separate serialization point and blocks the multi-actor half of the
requirement.

`_guard_admin_update_marker` refuses *any* `vq admin update` while *any*
marker file exists on the driver, and the marker is a single driver-global file
(`<state_root>/admin-update-in-progress`). So one update anywhere in the fleet
blocks every other update everywhere, regardless of host or program.

Evidenced this session, not inferred: a `rollout-latest --dry-run` observed a
marker scoped to `scheduler-runtime:slurm-cluster:vibeqc-dev` deferring **slurm-cluster's other
two lanes, all three of pbs-cluster's, and all three of localhost's** — eight lanes held
by one unrelated slurm-cluster build. With several chats and the auto-update timer all
wanting to update different hosts, that mutex is the bottleneck the maintainer is
describing.

The fix direction already exists in the tree. `admin_update_marker_scope()`
parses a marker's `envs` into the set of hosts it actually protects
(`scheduler:<host>`, `scheduler-runtime:<host>:<program>`, or local) precisely
because the blanket hold over-blocked **dispatch** — its docstring cites a pbs-cluster
rebuild holding slurm-cluster SLURM handoffs on an idle cluster. The *dispatch* side was
scoped; the *mutex* side never was.

Proposed: make the marker per-host (or per-host-per-program) so concurrent
updates to independent hosts proceed, reusing `admin_update_marker_scope`'s
existing parsing rather than inventing a second notion of scope. Two properties
must survive:

* the stale/interrupted-marker recovery contract (a marker still blocks its own
  host until acknowledged — that is what makes an interrupted update safe);
* single-flight per host, so two actors cannot build the same program on the same
  host simultaneously.

This is a distinct workstream from the runtime slots and should land as its own
increment; it is what turns "no halt between run and update" into "no halt
between any two steps". Sequencing note: while slots are still opt-in per host,
scoping the mutex is independently useful, because it unblocks the
already-immutable scheduler hosts from each other.

## 8. Why this matters for the agentic loop specifically

The autonomous loop holds standing release authority (CLAUDE.md §13,
`agentic-loop/campaigns/release-paper-molecular.toml`
`autonomous_release_authorization`). That waives the human *trigger* and nothing
else: every evidence gate still stands, including **per-profile LAST OK and
quiescence**. Its job is precisely the loop the maintainer describes — run, find,
fix, commit, release, roll, keep running.

Both halts above are therefore worse for the loop than for a human operator:

* A **drain** it cannot satisfy is indefinite. A human can judge "this patch does
  not touch what is running" and take the risk; the loop cannot, and must not
  learn to, because §1d says the affected rows are indistinguishable from good
  ones. Runtime slots make the judgement unnecessary, which is the only form in
  which the loop can proceed safely.
* A **driver-global marker** left by any other actor blocks the loop's update
  step, and its own quiescence gate ("no marker remains") then cannot be
  satisfied by anything the loop is permitted to do — it must not clear another
  actor's marker. Under many parallel chats that is a livelock, not a delay.

Consequence for §7's design: per-host scoping is not merely a throughput
optimisation. It is what lets the loop's quiescence gate mean "this host is
settled" rather than "the entire fleet is idle", which is the only version of
that gate a continuously-rolling fleet can ever satisfy. Keep the
stale-marker-needs-acknowledgement contract for the *owning* host, since that is
what makes an interrupted update safe, but scope it so an unrelated host's marker
cannot wedge the loop.
