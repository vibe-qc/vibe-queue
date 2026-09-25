# One directory layout for every host

**Status: phases 1–4 have tooling and tests; no phase is recorded here as
applied to any host.** Whether a given host has been migrated is a fact about
that host. Read it from the host (`survey`, below), not from this page.

Tracked as vibe-qc#1. The filled-in, per-host plans and receipts are operator
records and live outside this repository, like the rest of the
[host inventory](hosts.md).

## The principle

Sort a host's home directory by **lifecycle, not by content type**. For every
directory the question is: *can this be regenerated, and if not, is it backed
up?* Mixing lifecycles in one place is what makes cleanup dangerous:
rebuildable build trees beside irreplaceable queue state, deployment versions
beside a checkout with unpushed commits, scratch output beside cited evidence.

| directory | holds | lifecycle | prunable? |
|---|---|---|---|
| `~/gitlab` | checkouts, one per lane | churns freely | when clean and idle |
| `~/vq` | everything vq runs: `runtimes/` and `programs/` | versioned releases | yes: keep current + previous + N |
| `~/vqstate` | queue database, leases, audit trail, detached run records | **durable** | **never** |
| `~/vqscratch` | submitted calculations, fetched results not yet judged | disposable | yes, by age |
| | *vq's own default since v0.26.10 is `<state root>-scratch`; point `scratch_root` here* | | |
| `~/vqruns` | staging before promotion into a calculations repository | **durable** | no; back up |
| `~/bin` | wrappers and symlinks **only**, pointing into `~/vq` | stable interface | contents yes, **paths no** |
| sync targets | whatever a file-sync client mirrors off the machine | **off-limits** | **never touched** |

```
~/vq/runtimes/<component>/releases/<sha>/
~/vq/runtimes/<component>/current  -> releases/<sha>
~/vq/runtimes/<component>/previous -> releases/<sha>
~/vq/runtimes/<component>/staging/      in-flight deploy
~/vq/runtimes/<component>/quarantine/   failed deploys, kept for forensics
~/vq/programs/<name>-<version>/         external programs
~/vq/programs/<name> -> <name>-<version>
~/vq/installers/<name>-<version>/       installer archives, kept, with SHA256SUMS
~/vq/.layout/                           layout record and migration receipts
```

`runtimes/<component>/` is the slot layout of [immutable
runtimes](design_immutable_venv_runtimes.md); this page only says where it
lives.

`~/vqruns` is staging, not a terminal store. Accepted results are promoted into
the owning paper's calculations repository; nothing is meant to live on a
compute host for good.

## Invariants

* **Never a sync target.** A local delete there propagates off the machine. The
  tool refuses them lexically and after resolving symlinks, because on real
  hosts a sync directory is often a symlink onto a data disk.
* **Managed checkouts are `vq admin update` territory.** No hand edits, no
  `git pull`, no `pip install`. The tool refuses everything under `~/gitlab`.
* **Never while a deploy is in flight.** A deploy writes a tree and flips a
  pointer; a change made mid-roll acts on a target that is about to move.
* **Keep `current` and `previous`.** Rollback must not need a rebuild.
* **`~/bin` paths are provenance.** Job receipts record the interpreter and
  program paths they ran, so an old path that stops resolving breaks the audit
  trail of finished work. Contents may move; every path keeps resolving.
* **`~/vqstate` is never pruned.**

## The tool

`contrib/fleet_layout_migrate.py` covers the three phases that stop no daemon
and delete nothing, and phase 4 under stricter guards. It needs Python 3.6 and the standard library, reads nothing
from stdin, and is meant to be piped, so a host needs nothing installed:

```sh
ssh HOST python3 - survey          < contrib/fleet_layout_migrate.py
ssh HOST python3 - plan --phase 1  < contrib/fleet_layout_migrate.py
```

### Review, then apply

1. `plan` is **read-only**. It prints every operation and a **plan id**: a
   SHA-256 over the operations, the host state they were computed from (inode
   numbers included) and the host name.
2. A person reviews that output.
3. `apply --approve <plan id>` recomputes the plan and runs only on an exact
   match. If the host changed since the review, or the id came from another
   host, it refuses and prints the new plan instead.
4. Every apply writes a receipt under `~/vq/.layout/receipts/`. `undo
   --receipt FILE` shows the reversal; adding `--approve <plan id>` runs it.
   If the host cannot store the receipt (a home over quota, say), the tool
   prints it and exits non-zero rather than leave operations without a record.

A blocker makes both `plan` and `apply` exit non-zero, and `plan` still prints
everything it could work out, so all three phases can be read before the first
one runs. On a cluster with several login nodes behind one name, plan and apply
through the same node: the host name is part of the id on purpose.

### Phase 1: the roots

Creates whichever of the four roots are missing, `~/vqstate` with mode 0700,
plus `~/vq/programs` and `~/vq/.layout`. On a scheduler login host, which runs
no daemon and so has no queue state, pass `--daemonless`: it gets no
`~/vqstate`, because an empty directory that looks durable is a small version
of the dead layer this layout exists to remove. A root that already exists is
**adopted**: recorded, never modified, never removed by an undo. Expect this. A
host may already keep live queue state in `~/vqstate`, or another lane's files
in `~/vq`; neither is touched.

### Phase 2: `~/vq/runtimes`

* **One existing runtime store** (`~/.local/libexec/vq-*/runtimes`):
  `~/vq/runtimes` becomes a symlink onto it. The store does not move, so venv
  shebangs, in-flight jobs and old receipts keep resolving.
* **No store, in-place checkouts:** `~/vq/runtimes` is created empty.
* **Packed releases** (per-SHA wrappers and no unpacked `releases/` tree: a
  scheduler site that ships each runtime as one bundle, unpacked on the compute
  node by the wrapper that names its SHA). Such a host is not exempt; its
  releases just hold something else. The contract is `releases/<sha>`,
  `current`, `previous`, and it does not say a release must be an unpacked
  tree:

  ```text
  ~/vq/runtimes/<component>/releases/<sha>/
      <bundle>.tar.gz     durable copy of the bundle
      SHA256SUMS          verifies it: sha256sum -c SHA256SUMS
      manifest.json       what the site deploy recorded about it
      python              link to the per-SHA wrapper that runs it
  ~/vq/runtimes/<component>/current  -> releases/<sha>
  ~/vq/runtimes/<component>/previous -> releases/<sha>
  ```

  Publishing bundles is the site deploy tooling's job, so the tool only
  **checks** this store and never builds or repairs it: every release has its
  bundle and `SHA256SUMS`, every `python` link resolves, no pointer dangles.
  It does not hash bundles (a plan stays cheap). With no releases published
  yet it refuses rather than create an empty `~/vq/runtimes` beside runtimes
  it does not describe: one more dead layer that looks live.
* **More than one store:** refused; a person chooses.

Phase 2 gives the store a name. **It does not make anything deploy there.**
That is a configuration change (`runtime_slot_root` for a venv program, or the
site deploy script's store root for a scheduler host), it changes what the next
update does, and it is out of this tool's hands. Two things to check first:

* `runtime_slot_root` is rejected by a vq that predates it, which breaks every
  command on that host. Update the host, then write the key.
* A deploy or wrapper script that compares a **resolved** active path against an
  **unresolved** store root will refuse when the root is given as
  `~/vq/runtimes` while it is still a symlink. Read each such comparison before
  pointing a script at the new name.

### Phase 3: external programs

`~/bin` entries are classified as symlink, wrapper, backup, archive,
`payload-file` (ELF or at least 1 MiB) or `payload-dir`. You say which payloads
form which program; the tool never guesses a version:

```sh
plan --phase 3 \
  --program orca-6.1.1=orca_6_1_1_linux_x86-64_shared_openmpi418 \
  --program crystal23-1.0.1=crystal23,properties23 \
  --archive crystal23-1.0.1=crystal23_v1_0_1_dist.tar.gz
```

* A **directory** becomes `~/vq/programs/<name>-<version>/` and trades places
  with its replacement symlink in one `renameat2(RENAME_EXCHANGE)`.
* A **file** is hard-linked to `~/vq/programs/<name>-<version>/bin/`, then a
  symlink is renamed over the old name.
* An **archive** is moved to `~/vq/installers/<name>-<version>/` under its
  vendor file name, and a `SHA256SUMS` is written beside it (`sha256sum -c`
  checks it). Installers are **kept**: for a licensed program "re-downloadable"
  is a claim nobody should have to test during a rebuild, and a vendor file
  name is what a vendor checksum is published against, so it is not renamed.
  No link is left in `~/bin`, since no receipt names an installer; it is the
  one kind of `~/bin` path that stops resolving.
* Wrappers, backups and unmapped payloads stay where they are and are listed.

Nothing is copied: a payload on another filesystem than `~/vq` is refused.
Programs that already live outside `~/bin` are not this phase's business.

**Why apply is safe under running jobs.** On a scheduler host the compute nodes
are invisible from the login node, so "nothing is using this" cannot be
established, and the tool does not rely on it. During an apply the old path
resolves at every instant and an open handle keeps its inode: nothing can reach
the new location until the very operation that moves the payload creates the
route. `tests/test_fleet_layout_migrate.py` holds that to zero failed lookups
from a concurrent reader; a rename-then-symlink in place of the exchange fails
the same test with `ENOENT`.

**Undo is weaker, and you should know how.** Resolving a symlink takes two
steps: read its text, then walk that text. A lookup that has read the old
path's text is already heading for the new location when an undo takes it
away.

* Undoing a **file** move has no gap: the hard link at the new location is kept
  for a grace period (`--grace`, 5 s), so both names are valid throughout.
* Undoing a **directory** move has one. A directory has a single name, so
  between the exchange that restores it and the rename that leaves a back-link
  behind, two adjacent system calls, an in-flight lookup can see `ELOOP`.
  For scale: a *bare* swap-back with no back-link was caught 5 to 7 times in
  about 250 swaps by a saturating reader on Linux, on both btrfs and ext4. The
  back-link narrows that to the two-call gap; the tool's own undo has been
  exercised under a concurrent reader on Darwin only, not yet on Linux. Undo a
  directory move while its program is idle if you can, and treat undo as
  recovery, not as routine.

**What the claims rest on.** The file move is as atomic as `rename(2)` and the
directory move as `renameat2(RENAME_EXCHANGE)`. The two `..._on_linux` canary
tests measure exactly those primitives with none of the tool's logic. Darwin
does not deliver either: its name cache lets `open()` fail with a transient
`EINVAL` during a plain `rename`, which is why the suite tolerates `EINVAL`,
and only `EINVAL`, there. Measured so far, forward direction, zero failed lookups in
440,000 to 585,000 each: `rename` and the exchange on Linux tmpfs, btrfs and
ext4. Before the first phase 3 on any other filesystem type, run the canaries
**on that filesystem**; a result from `tmpfs` says what the kernel
does, not what the filesystem does, and a network filesystem's clients are a
further question the canaries cannot answer from one machine.

### Phase 4: the state root gets a name

```sh
plan --phase 4 --state-root DIR
```

`~/vqstate` becomes a **symlink to the live state root**. The state itself is
never moved, and the daemon is not touched and keeps running.

That restraint is a lesson, not a preference. An earlier version of this tool
moved the root to `~/vqstate` and left a symlink at the old path, reasoning that
every symlink refusal in vq guarded a final path component. That was wrong:
`vq admin update` takes its lock in the state root and requires it to be an
**owner-controlled real directory**, checked with `lstat`. With a symlink where
vq computes its root, every managed update on that host was refused (fail
closed, nothing damaged) until the move was undone. The state root stays a
real directory at the path vq computes; the layout gets its name by pointing
*at* it. If you ever want the real directory to be `~/vqstate`, that is a
`VQ_STATE_DIR` change for the daemon **and every caller at once**; a daemon
given it while a CLI is not is how a *split store* is made (the CLI reads one
store, the daemon writes another, and updates report success while nothing
advances).

You name the root, and you read it **from the running daemon**
(`VQ_STATE_DIR` in its environment, else `$XDG_DATA_HOME/vq`), never from a
directory that merely looks like state: hosts carry stale state-shaped
directories. The tool refuses if the running daemon uses a different root, if
`DIR` has no `queue/` and `jobs/`, if `DIR` is itself a symlink (that host
cannot take managed updates; fix that first), if the name would lead into a
protected tree, or if `~/vqstate` already holds anything. Two stores are
reconciled by a person, never merged by a tool.

Job workdirs lived under the state root until v0.26.10, so on a host that has not
been through phase 5 expect it to be large (over 100 GB was found where
megabytes were assumed). Separating durable state from disposable workdirs is
phase 5, below.

**If a state root was actually moved, the records do not follow it.** This
tool never moves state, for the reasons above; other tooling has. A job spec
stores absolute `cwd`, `workdir` and `archive_path` values, recorded when the
job ran and never recomputed, so a move that copies the bytes to a new root
leaves every terminal record naming the old one and `vq fetch` fails on a
workspace that is in fact present under the new root. The supported repair is
`vq admin repair-state-paths` (#69): read-only by default, plan-id gated,
originals kept, and refusing anything it cannot verify. See
[the operations runbook](operations.md).

## Phases that need the maintainer first

These move or delete durable things. The tool does not implement them, and an
agent does not run them on its own judgment.

| phase | what | before anyone runs it |
|---|---|---|
| 5 | Converge scratch under `~/vqscratch`, add an age sweep | **vq supports this since v0.26.10 (#63); the runbook is below.** Scratch is a root of its own by default, `jobs/` and `workdirs/` hang off it, `vq cleanup` accounts for and removes workdir bytes, and a scheduler host's workspace is recorded per job rather than recomputed from the current `scratch_root`. What still needs a person: `~/vqruns` must exist and be backed up first, otherwise an age sweep eats results nobody has judged. |
| 6 | Retire legacy release families and checkouts | No deploy in flight. **"Old" is not "dead":** a legacy-named checkout was found carrying a live dashboard unit on one host, the driver's `remote_vq` on another and a `~/bin` symlink on a third, and its newer replacement lacked an extra the unit needs. So census every candidate first, against: symlinks; wrapper and deploy scripts; service units; the host's own config *and* the driver's; the scripts of every job the scheduler still knows; and the checkout's local-only git refs and stashes, which are source rather than re-runnable results (bundle them into `~/vqruns` first). Old `$HOME` release trees go through the site's cleanup script and its forbidden-path guard, which keeps the active target and the previous one. A runtimes store is pruned by liveness, never by mtime rank: keep `current`, `previous`, every release a live job names and N more; re-derive that set immediately before each deletion, so a deploy that flips a pointer mid-run loses nothing; leave `quarantine/` alone. Approval to retire is not evidence that removal happened. |
| 7 | Replace the cleanup script's per-host cases with one rule | Every host is actually on the layout. Until then the special cases are what keep the script off managed checkouts and live state. |

Installer archives under `~/vq/installers/` are not retirement candidates: the
maintainer's decision is that they are kept.

## Phase 5 in detail: moving a host's scratch

**A default changed.** A vq from v0.26.10 on puts `jobs/` and `workdirs/` under a
scratch root separate from the state root, defaulting to `<state root>-scratch`
— `~/.local/share/vq-scratch` on a stock install, `/var/lib/vq-scratch` in
multi-user mode. On a host you never touch, the effect of upgrading is that
**new** jobs land there while every existing job stays exactly where it is.

**Nothing moves on upgrade, and vq will not move it.** A job spec records
`cwd`, `workdir` and `archive_path` as absolute paths and no read path
recomputes them (phase 4's lesson, #69). So the old root keeps its jobs, and
vq keeps looking there: `vq fetch`, `vq cleanup` (listing, accounting and
`--delete`) and the age sweep iterate **both** roots until the old one is
empty. Relocating bytes is a person's decision, taken with the daemon stopped;
there is no upgrade path that does it for you, on purpose.

### The ordinary case: let it drain

The cheapest migration is none. Point the host at `~/vqscratch`, let the jobs
already on disk age out through the sweep, and the old `workdirs/` empties
itself.

1. **Measure first.** `du -sh` the state root and its `workdirs/` child. If
   `workdirs/` is not most of it, the host is not the shape this phase is for.
2. **Check `~/vqruns` exists and is backed up.** An age sweep with nowhere to
   promote results to eats work nobody has judged.
3. **Set the root, for the daemon and every caller at once.** The config key is
   the safer of the two ways, because both read the same file:

   ```toml
   min_vq_version = "0.26.10"
   scratch_root = "/home/USER/vqscratch"
   ```

   Set `min_vq_version` when you set `scratch_root`. A vq that predates the key
   ignores unrecognized top-level keys by design, so without the floor an older
   vq on that host keeps resolving scratch under the state root while a newer
   one uses this key — a split store, the same failure phase 4 warns about for
   `$VQ_STATE_DIR`. `$VQ_SCRATCH_DIR` overrides the key and has the same
   whole-host discipline as `$VQ_STATE_DIR`: give it to the daemon *and* every
   CLI caller, or don't use it.
4. **Restart the daemon** and confirm with `vq daemon status`, which prints
   `scratch`, `jobs` and `workdirs`.
5. **Turn the sweep on** if it is not already:
   `vq cleanup --auto-enable --workdir-max-age 14d --delete-after 90d`.
   It visits both roots.
6. **Watch the old root empty.** `vq cleanup` without a flag lists terminal
   jobs with a WORKDIR column; the old root is drained when nothing is left
   under `<state root>/workdirs` and `<state root>/jobs`.

### If you need the space back sooner

**Moving the bytes by hand is not supported, and there is no verb that makes
it supported.** A job spec records absolute `cwd` and `workdir` values and no
read path recomputes them, so a copy leaves every record naming the old
location. `vq admin repair-state-paths` does not close that gap: it repairs a
**state-root alias** (#69), it requires the old root to be the reviewed
compatibility *symlink* that directly names the new root — which a `cp -a`
never produces — and it rewrites `archive_path` along with `cwd` and
`workdir`, while archives stay on the state root by design. Leaving a symlink
at the old scratch path does not work either: vq opens a workdir root
`O_RDONLY | O_DIRECTORY | O_NOFOLLOW`, so a symlinked root fails closed and
switches that root's cleanup off — the exact defect #63 was opened about.

What is supported is reclaiming the space with vq's own verbs, which keep the
records and the bytes in agreement:

1. **Keep what is worth keeping.** `vq fetch` the results you have not judged
   yet into `~/vqruns`.
2. **Reclaim the rest.** Dry-run first — without `-x` the verb only prints:

   ```sh
   vq cleanup HOST --delete --older-than 30d
   vq cleanup HOST --delete --older-than 30d -x
   ```

   `--delete` removes the spec, the workspace, the workdir and the archive at
   the paths each job recorded, including under a root you have already
   configured away from, so it reclaims the same bytes a hand-move would have
   freed and leaves nothing pointing at a directory that is gone.
3. **Let the sweep keep it down** (previous section, step 5).

If you copy the trees anyway — with the daemon stopped, and only for jobs you
have already fetched — expect `vq fetch` and `vq cleanup` to address the old
path for those jobs, because that is what their records say. Delete the old
copies only after you no longer need those records at all.

A host where the state root is small and the sweep keeps up does not need any
of this.

### Scheduler hosts

`scratch_root` on a `[hosts.<name>]` entry is the same concept for the machine
vq submits *to*, and changing it is safe under jobs in flight since #63: a job
records the workspace it was staged into before the submission boundary, and
reattachment, retry, polling, fetch and remote cleanup all use that recorded
path. Three consequences worth knowing:

* A job dispatched by an older vq carries no such record. The driver binds it
  when it **reattaches** to it — a driver start, not a poll tick — and
  **refuses to adopt a changed `scratch_root` for that host while a job on it
  that may already be staged is still unbound**: it logs the job ids and keeps
  using the previous root, which it remembers durably, so once it has adopted
  a root neither a restart nor a `vq daemon reload` walks past the refusal.
  The change lands on the restart after you make it, once those jobs have
  bound. If a host is still held after that, one of its jobs did not reattach
  and is the real problem.

  An ordinary queued job is **not** one of those jobs. Nothing is staged
  before dispatch, so a job still PENDING on its first attempt cannot be
  stranded and does not hold the host; before v0.26.10 it did, and a
  continuously fed host never took the change at all.
* **The first start after the upgrade is the one case the refusal cannot
  check.** The driver compares against the root it last adopted, and a vq from
  before this feature recorded none — so if you upgrade and change the key in
  the same maintenance window, the first start has nothing to compare to. It
  does not pretend otherwise: it serves the configured root, because no other
  root is known, but it **does not bind** that host's pre-upgrade jobs to it
  and does not record the root as adopted. Those jobs then behave exactly as
  they did under the vq that dispatched them, and restoring the previous value
  in `config.toml` and restarting the driver repairs them — which is the
  point, because a workspace written onto a spec cannot be taken back. The
  restart is part of it: a running job's handle keeps the workspace derived
  when the driver reattached to it, and a config reload does not re-derive it.
  Once those jobs drain the doubt is settled at the driver's next lookup of
  the host (a dispatch to it, a reattachment, a cleanup), and `vq doctor`
  stops failing as soon as they drain. Set the key in a *separate* window
  from the upgrade and none of this applies.
* Remote cleanup accepts a workspace staged under a *former* `scratch_root`, so
  changing the key no longer leaves undeletable directories on the cluster.

**How to see it.** `vq doctor HOST`, run where the driver keeps its state,
reports `scheduler_scratch_root`: whether the value in `config.toml` is the
one in force, and if not, whether the host is held at a previous root or
serving an unconfirmed one. It reads the queue as well as the driver's record,
because the record is written only when the driver looks the host up: a key
changed on an idle host reads as moved but not held, and passes, when nothing
on the host could be stranded. It costs no SSH. A host the local driver has no
record of reports "no record" rather than a verdict.
