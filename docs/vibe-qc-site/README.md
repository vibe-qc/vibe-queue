# vq documentation written for vibe-qc.com

**For the vibe-qc documentation chat.** vq authors these pages and vibe-qc
copies them into the `mpei/vibe-qc` repository. vq's own docs build publishes
nothing from this directory, because `docs/conf.py` excludes it.

This directory is the canonical copy, as decided on vibe-queue#39 on
2026-09-13. A correction found on the vibe-qc side comes back as an issue on
vibe-queue. It is not made only in vibe-qc.

Written 2026-09-09 against vq 0.26.0 and vibe-qc `5f95d0870`. Reconciled
2026-09-13 with vibe-qc `880b1ff`: each page is vibe-qc's published page plus
the `starved` fix described on vibe-qc#242, and nothing else.

---

## Why these exist

`.gitlab-ci.yml` records the boundary: the vibe-qc documentation chat owns
`/`, `/docs/` and `/preview/` on vibe-qc.com, and vq owns
`/vibe-queue/docs/` and nothing above it.

That split leaves a gap. vq's own documentation is deliberately
product-neutral, because vq is a general job queue and, in its own words,
"does not care whether that is what you run." So the material that is
specifically about *queueing vibe-qc calculations* has no home on vq's site:
the program registry pointing at `vibeqc-dev` and `vibeqc-release`,
`--branch` routing, `--vibeqc-preflight`, QVF container submission, and the
CRYSTAL and ORCA wrappers.

That material belongs on vibe-qc.com. vq should write it, because vq is what
changes underneath it. Hence this directory.

The corresponding rule going the other way: **these pages state no vq version
number and duplicate no vq reference material.** Everything general links to
<https://vibe-qc.com/vibe-queue/docs/>. That is what keeps two sites on two
release cadences from drifting.

## Syncing to vibe-qc

On each vq release, and whenever a page here changes, copy each page over the
published file unchanged:

| File | Destination in `mpei/vibe-qc` | Action |
| --- | --- | --- |
| `user_guide/queue.md` | `docs/user_guide/queue.md` | Copy over the published file |
| `tutorial/vq_queue_remote_job.md` | `docs/tutorial/vq_queue_remote_job.md` | Copy over the published file |
| `toolset_lifecycle-vq-edits.md` | none | A record only. vibe-qc's own rewrite of `docs/toolset_lifecycle.md` superseded it; do not apply or copy it |

A straight copy is the whole procedure, with one check first: diff the
published page against the version it last received from here. A difference
you did not expect is an edit made only on the vibe-qc side. The copy would
overwrite it, so bring it here as a vibe-queue issue before copying. Edits made
only in vibe-qc are how the two copies diverged in both directions the first
time (vibe-queue#39).

Both pages keep vibe-qc's existing paths and filenames, so:

* the `queue` entry in `docs/user_guide/index.md`'s toctree still resolves;
* the `vq_queue_remote_job` entry in `docs/tutorial/index.md` still resolves;
* every existing inbound link keeps working, and **no `redirects` entry is
  needed** in `conf.py`.

## What only renders in vibe-qc

The pages carry markup that only means something inside vibe-qc's Sphinx tree.
It stays here verbatim so that the copy remains a straight copy. This
repository does not build these pages, so nothing here checks it:

* the `{figure}` at the top of `queue.md`, which points at
  `../_static/logo/vibe-queue-social.svg`, an asset in vibe-qc's tree;
* the `og:image` and `twitter:*` metadata in `queue.md`'s front matter;
* the MyST labels, such as `(step-1-dry-run-pre-flight-locally)=`, which keep
  the pre-split pages' anchors resolving. Some are named after old vq versions,
  such as `(multi-venv-branch-routing-v0-5-6)=`. Those are anchor names, not
  version statements;
* the tutorial's `{ref}` to `vq-core-preflight`, a label in `queue.md`.

## House style

vibe-qc's pre-commit hook rejects em and en dashes in `docs/**.md` prose, so
the pages here must stay free of them for a straight copy to pass
`.githooks/check_no_em_dashes.py`. They use `/home/USER/` for home paths, which
both repositories' pre-commit hooks allow.

The example host alias is `compute` throughout, with `localhost` where a page
means a local submit. The pages they replaced used a real fleet hostname in
sample output; a neutral alias keeps hostname review off these pages
permanently.

## What was cut, and why

The rewrite took `user_guide/queue.md` from 1053 lines to 409. vibe-qc's
integration brought it to 478, mostly anchor labels, the figure and the
corrections below.

Removed, because it is vq's own reference and now lives at
`/vibe-queue/docs/`: the installation walkthrough for both sides, the daemon
architecture diagram, systemd unit setup, the web dashboard, cgroup
enforcement detail, workspace cleanup, operator controls, daemon admin, the
reboot story, concurrency, troubleshooting, the version-history list and
vq's roadmap.

Kept and rewritten, because it is about vibe-qc and has nowhere else to go:
when to reach for the queue, pointing vq at your vibe-qc environments (hosts,
branch routing, the program registry), a first calculation, choosing which
vibe-qc build runs a job, QVF submission, `--vibeqc-preflight`, the CRYSTAL
and ORCA wrappers, and result readback.

The tutorial stays close to its original shape, because a worked
submit-and-fetch cycle for a real periodic calculation is exactly the kind of
page that should live on vibe-qc.com.

## Corrections carried into the rewrite

The pages being replaced had drifted from the code. These were found by
reading the current vq source, not by inspection of the prose:

* **The job lifecycle states were wrong.** The tutorial documented
  `queued -> starting -> running -> done | failed | timed_out | cancelled`.
  The real states, from `vq.spec.JobState`, are `pending`, `submitting`,
  `submit_outcome_unknown`, `running`, `suspended`, and the terminal set
  `completed`, `failed`, `killed`, `interrupted`, `oom_killed`, `starved`,
  `time_exceeded`, `aborted_by_queue`. `vq kill` produces `killed`, not
  `cancelled`.
* **`vq throttle --max-jobs 1` does not exist.** `--max-jobs` belongs to
  `vq drain`. `vq throttle` adjusts one job's CPU weight.
* **`vq cleanup --older-than 14d` does nothing on its own.** Cleanup verbs
  are dry-run by default and need `-x` to execute, plus one of `--archive`,
  `--delete` or `--restore`.
* **"No job-array primitive yet" is stale.** `--array N` and `--chain N` both
  exist, as does `--rerun-until`.
* **"vq is single-host by design, use SLURM for a cluster" is stale.** vq
  ships PBS and SLURM backends for registered hosts, with durable
  submission, monitoring, control and artefact fetch. The honest limitation
  is narrower and is stated as such: unregistered clusters, interactive
  allocations, and workflows outside the declared-resource model.
* **`--vibeqc-preflight` is narrower than described.** It runs on the
  submitting machine and is not forwarded to a remote submit
  (`submit_remote` does not take it), and it is disabled inside `--array` and
  `--chain`.
* **The CRYSTAL wrapper gained `--demo`**, selecting the CRYSTAL23 demo
  binaries: serial only, so it rejects `--np`, and capped at ten atoms per
  primitive cell.
* **Every `vibe-queue/`-relative path is dead**, along with the `See also`
  block's five links into `vibeqc/-/tree/main/vibe-queue`.

## Corrections made during vibe-qc's integration

The vibe-qc documentation chat integrated the pages under vibe-qc#189
(`f10cc197`, then `e38437a4` for the figure). It corrected errors that this
repository could not check without running vibe-qc. This copy carries all of
them since the reconciliation on vibe-queue#39:

* The tutorial's MgO cell was `np.eye(3) * a`, which with these atom
  positions is the CsCl structure. It is now the FCC primitive cell.
* `run_periodic_job` takes `vq.BasisSet(...)`, `method="RKS"`,
  `jk_method="gdf"` and `kpoints=`. `queue.md`'s first example calls
  `run_job(..., output="water")`, which `--vibeqc-preflight` needs.
* The tutorial no longer shows `--vibeqc-preflight` on a remote submit. Step 1
  runs the dry run by hand and step 2 submits without the flag. `queue.md`
  makes the local submit explicit with `localhost`.
* The carried-over dry-run transcript, the submit receipt, the MgO energy and
  iteration count, and every size and timing claim are gone. The tutorial now
  says no reference energy is asserted.
* `vibe-view capture -s vol_dens_0` became `vibe-view info` followed by
  `capture --section ID`, and the fetched-file listing no longer promises
  sidecars that depend on output settings.

## Two things the split broke that were not documentation

Both are fixed on vibe-qc `main` (checked at `2e7179e`):

* `website/src/data/components.mjs` no longer reads `vibe-queue/pyproject.toml`.
  vq's entry carries `version: null` and a link to its releases page
  (`b935488b`), so the website build no longer depends on a file the vibe-qc
  checkout does not have.
* `docs/conf.py`'s `_SIBLING_PYPROJECTS` now lists only vibe-basis, and no page
  uses `{{vq_version}}`. The docs link to vq's releases instead of stating a
  version.

## Keeping it current

The coupling that matters is small and worth stating, since it is the whole
reason these pages are authored in vq's repository:

| If this changes in vq | Update |
| --- | --- |
| `[programs.*]` schema, or `--program` / `--expected-sha` | `queue.md` § Point vq at your vibe-qc environments, § Choosing which vibe-qc runs the job |
| `--branch` routing or `[hosts.X.branches]` | `queue.md`, same two sections |
| `--vibeqc-preflight` scope, or the QVF submit contract | `queue.md`, the tutorial's steps 1 and 2 |
| `contrib/run-crystal.sh`, `contrib/run-orca.sh` | `queue.md` § CRYSTAL, ORCA and other external programs |
| `vq.spec.JobState` | `tutorial/vq_queue_remote_job.md` step 3, and the terminal list in `queue.md`. Enforced by `tests/test_vibeqc_site_job_states.py`, so this row is a pointer rather than the only defence |

Everything else in vq can move without touching these pages, which is the
point of linking out rather than copying.

Only the `JobState` row is machine-checked, by
`tests/test_vibeqc_site_job_states.py`. It asserts three things:

* every backticked name in a prose sentence about states is a real state;
* the tutorial's lifecycle block lists exactly `TERMINAL_STATES`;
* the "are all terminal" note in `queue.md` lists every terminal state
  except success.

The first check replaced one that could never fail. That version kept only
names already in the enum, so a renamed state dropped out of what it checked
instead of failing it (vibe-queue#39). The block check was written after the
block was found to be missing `starved`: every outcome it showed was real, so a
reader who handled all of them still missed one vq produces. The pattern is
`tests/test_admin_outcomes.py`'s, which pins `docs/orchestration.md` against
`admin.ADMIN_OUTCOMES`.

The other rows are still prose against prose. They are the cheaper half of the
practice and the reason a drift is recoverable at all, but they are not a
guard.
