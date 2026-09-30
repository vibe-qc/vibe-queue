# vq documentation written for vibe-qc.com

**For the vibe-qc documentation chat.** vq authors these pages and vibe-qc
copies them into the `mpei/vibe-qc` repository. vq's own docs build publishes
nothing from this directory, because `docs/conf.py` excludes it.

This directory is the canonical copy, as decided on vibe-queue#39 on
2026-09-13. A correction found on the vibe-qc side comes back as an issue on
vibe-queue. It is not made only in vibe-qc.

Written 2026-09-09 against vq 0.26.0 and vibe-qc `5f95d0870`. Reconciled
2026-09-13 with vibe-qc `880b1ff`: each page is vibe-qc's published page plus
the `starved` fix described on vibe-qc#242, and nothing else. Audited against
the vq source again on 2026-09-18, the first re-check since that reconciliation;
what it changed is under "Corrections from the 2026-09-18 audit" below. Audited a
third time on 2026-09-29 at vq `ad223b5` (0.26.16), this time for exit statuses,
what fetch brings back and which submit variants a remote host accepts; see
"Corrections from the 2026-09-29 audit". The same day the two pages were compared
with vibe-qc `da915e93`; see "Last sync check".

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

## Corrections from the 2026-09-18 audit

The reconciliation matched the two copies to each other. It did not re-read
either against the code, and vq had moved on. This pass walked both pages
claim by claim against the Click command tree, `src/vq/config.py`, the fetch
and cleanup modules and the `contrib/` wrappers (vibe-queue#39):

* **The parallel CRYSTAL examples no longer ran.** `contrib/run-crystal.sh`
  dropped its built-in rank count in `707232b`, two days after the
  reconciliation: a parallel run with no `--np N` now exits rather than
  choosing one. Both parallel examples on the page omitted it. They now pass
  `--np`, and the page says the wrapper will not guess.
* **A named fetch was described as refusing to overwrite.** The page said
  re-fetching is "idempotent only when the existing file is byte-identical:
  vq will not overwrite a different result". `fetch._publish_artifact` does
  the opposite: an existing file is replaced with the newly fetched bytes, and
  only an identical one short-circuits. It refuses a destination it cannot
  safely replace, such as a symlink. Rewritten to what the code does.
* **`vq pause` was documented as the dispatch gate.** "Holding the queue back"
  showed `vq pause` for "stop dispatching; running jobs continue" and
  `vq resume` for "dispatch again". That is `vq drain` and `vq drain
  --release`. `vq pause` SIGSTOPs one running job's process group into
  `suspended`, and with no job id and no `--all` it is a usage error. The
  tutorial's own step 3 already said so, so the page contradicted itself. The
  section now separates the dispatch gate from the running-job verbs.
* **The `vq queue` sample output was invented.** It showed columns `JOBID`,
  `ELAPSED` and `SCRIPT`. `listing.py` renders `ID`, `STATE`, `CPUS`,
  `SUBMITTED (UTC)` and `COMMAND`, with `NAME` only once a listed job has one,
  which that job did not. Replaced with the column list, and no fabricated
  row. This is the same fault the integration removed elsewhere on the page.
* **The preflight's cost was a number nobody measured.** "It costs a second or
  two at submit time" is unverifiable and was carried over from the pre-split
  page. What the code guarantees is a bound: `_DEFAULT_TIMEOUT_S` is ten
  seconds, after which the preflight is abandoned and the submit proceeds.
  The page states the bound instead.
* **A link labelled GitLab pointed at GitHub.** `queue.md`'s See also read
  "vibe-queue on GitLab" over the GitHub mirror URL. Relabelled.

Everything else both pages claim was checked and is correct as written: every
verb and flag resolves in the Click tree, every configuration key is a real
`Config`, `HostConfig`, `VenvProgram` or `BinaryProgram` field, and the
behaviour claims about `--expected-sha`, `--branch`, `--qvf-force`, the QVF
command, `--vibeqc-preflight`'s scope, `--array` and `--chain`, cleanup's
dry-run default, `vq drain --max-jobs`, `vq throttle`, `vq kill`'s escalation,
`vq resubmit`, the fetch destination layout, `vq status` naming the budget a
pending job waits on, and the cgroup v2 caps all match the source. The ORCA
wrapper's flags and kept artefacts match `contrib/run-orca.sh`.

## Corrections from the 2026-09-29 audit

The 2026-09-18 audit read the Click tree, the config models and the wrappers. It
did not look at exit statuses, at what a fetch brings back, or at which submit
variants a remote host accepts. This pass did, at `ad223b5`
(vibe-queue#39). Almost none of it is new code: what it found was in the
initial commit, and the earlier passes had not looked.

* **A plain `vq fetch` of a job that a local daemon ran exits 1 after copying
  the tree.** Every locally dispatched job has a scratch directory separate from
  its workspace, and `require_workspace_artifact_selection` (`src/vq/fetch.py`)
  refuses to report an implicit workspace fetch as success. `vq fetch --help`
  documents it and `tests/test_cli.py::test_fetch_separate_local_workdir_requires_selection`
  pins it. Every fetch example on both pages was a bare `vq fetch`. They now pass
  `--workspace` and say why. PBS and Slurm jobs, `--name` fetches and
  `vq fetch-all` are unaffected. Whether a bare fetch should keep failing for a
  job that never used its scratch directory was decided on vibe-queue#120
  (2026-09-29): it should not. A fetch now refuses only when the host cannot
  prove the scratch directory empty, and older releases still refuse every time.
  The examples keep `--workspace`, which is correct on every release; drop it
  from the quickstarts once the hosts run a release that contains the change.
* **`[outputs].status` was credited to vq.** The tutorial said vq uses it to
  tell a crashed run from an orphaned one. vq reads that field in one place, to
  find the dry-run manifest. It tells an orphan from a finished job by the exit
  code in `_vq/exit-code`. The sentence now says what vq does. What vibe-qc
  writes into the field is vibe-qc's to say, and is not checked here.
* **"Fetch can be selective" was false.** `vq fetch` never reads the declared
  artefacts. A comment in `spec.py` described a `--outputs-only` flag that does
  not exist; vibe-queue#121 corrected it. `vq output` and `vq progress` are what
  follow the declared artefacts.
* **`--chain` was offered for a default remote host.** `submit.py` refuses
  `--chain` and `--rerun-until` for an ordinary remote host, because that host's
  daemon never receives the linkage. `--array` is forwarded and works.
* **What a fetch brings back was understated.** Every workspace fetch carries a
  `_vq/` directory, and a `PARTIAL` fetch still delivers the rest of the tree.
  Both are now stated.
* **The preflight's interpreter was unstated.** For a local submit the dry run
  and the job use vq's own Python unless `--python` or `--branch` is given, so a
  vq venv without vibe-qc makes the dry run a silent no-op and fails the job on
  import.
* **`--program` was presented as a way to choose the vibe-qc.** For a single-file
  submit the interpreter comes from `--python`, `--branch` or the host's
  `remote_python`; `--program` records the program's identity and, with
  `--expected-sha`, verifies its checkout (`vq submit --help`: it "does not
  rewrite the command"). The page listed it as a third mechanism, so the example
  could stamp the release checkout while running the development interpreter. It
  now pairs `--program` with `--branch`.
* **`--branch` "single-file only" was stale.** `_payload_command` puts the
  interpreter in front of a `-d` or `-c` command. `vq submit --help` said
  single-file until vibe-queue#121 corrected it; a test now covers `--branch`
  with `-d`.
* **`provides_branches` matches the `--branch` name only.** A job submitted with
  `--program` or `--python` alone carries no branch name and is not paused by
  the narrower scope of `vq admin update`.
* **`VQ_PROGRAM_BIN`, `VQ_PROGRAM_PYTHON` and `VQ_PROGRAM_GIT_DIR` reach local
  dispatch only** (`VQ_PROGRAM` and `VQ_PROGRAM_BRANCH` also reach scheduler
  jobs), and `--program` never rewrites the command. The "everything else"
  example now names the executable, and says `--program` checks that the name is
  registered, not that the binary exists.
* **`vq wait` has an exit status.** It exits with the job's own code for a job
  that completed or failed, with 1 for every other terminal outcome, and with 124
  on `--timeout`. The pages said only "check the state".
* **`vq queue` lists every job that is not archived**, not only "what is
  happening right now", and its columns depend on the jobs listed.
* **cgroup enforcement needs delegation** on the user manager; without it vq
  polls and signals.

Blunt, as in the previous section: the guard passed on the pages as they stood
before this audit (18 of 18, run against `HEAD` before any edit) and passes after
it. Not one finding was of the class it catches. A wrong exit status, a claim
about what vq reads, and a variant that a remote host refuses are all
sentences about behaviour. Reading is still the only check for them.

Not checked, and left as written: the vibe-qc side of every claim about
`[outputs].status`, the `.system` schema, and the settled-container refusal.

## Last sync check

Compared on 2026-09-29 with vibe-qc `main` `da915e93` (vibe-queue `main`
`ad223b5`):

* vibe-qc's copies are a straight copy of this directory at `092c4bd`, the
  version vibe-qc#242 asked for (`d65f4532`, byte-identical for both pages). They
  carry `starved` and `interrupted`, so the point that started vibe-queue#39
  agrees. vibe-qc#242 itself is still open.
* Everything else that differs is a vq-side change vibe-qc has not received: the
  six corrections of the 2026-09-18 audit, the source-clone paragraph from the
  public-source work, and the corrections above.
* vibe-qc edited `queue.md` twice after `d65f4532`: `8e4e1559` (SSH port in the
  clone recipes) and `11a44ce4` (source links). Together they change four link
  targets from the private GitLab URL to the GitHub mirror. Each of those four is
  already the same here, so a straight copy loses none of them.
* No vibe-qc-only edit exists that this copy lacks, so no correction needs to
  come back as an issue.

The next copy is therefore safe to make as a straight copy over both published
files, from a vibe-queue commit that contains this section.

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
| `contrib/run-crystal.sh`, `contrib/run-orca.sh` | `queue.md` § CRYSTAL, ORCA and other external programs. The flags are checked against the scripts; what the wrapper *does* with them is not |
| `vq.spec.JobState` | `tutorial/vq_queue_remote_job.md` step 3, and the terminal list in `queue.md`. Checked |

Everything else in vq can move without touching these pages, which is the
point of linking out rather than copying.

`tests/test_vibeqc_site_claims.py` is the guard. It reads the two published
pages, and the code rather than a transcription of it, and asserts:

* **Job states.** Every backticked name in a prose sentence about states is a
  real state; the tutorial's lifecycle block lists exactly `TERMINAL_STATES`;
  the "are all terminal" note in `queue.md` lists every terminal state except
  success; and every state these pages name is covered by at least one of
  those three, so none is named where nothing would notice it going stale.
* **Verbs.** Every `vq <verb>` and `vq <verb> <subverb>` the pages show
  resolves in the Click tree under `vq.cli.main`, by introspection. A verb
  that exists as a function but is not registered, or is registered under
  another name, fails.
* **Flags.** An option on a `vq` command line must be an option of *that*
  command. The whole-tree version of this check would pass `vq submit
  --serial`, because `--serial` exists on `vq admin update`. An option named
  in prose, where no command is named, is held to the whole tree instead.
* **Wrapper flags.** Options passed after `--` to `contrib/run-crystal.sh` or
  `contrib/run-orca.sh` are checked against the options those scripts parse.
  This checks the flags that are present, not whether required flags are
  missing. It would not have caught the omitted `--np` in the rank-count
  drift above; that still needs a behaviour review.
* **Configuration keys.** Every key in a `toml` fence is a field of the model
  that owns it: `Config`, `HostConfig`, or the `kind`-selected program model.

Three of those are a constraint on how you write, so they are worth stating
plainly.

**Say "state" in a sentence that names one.** The first check reads only sentences that mention a *state*, and the
other two read only the lifecycle block and the terminal list. So if you name
a state in prose, either say "state" in the same sentence, or be naming one
the block or the list already pins. Writing "the job goes `suspended` while
the host is under pressure" otherwise leaves that name with nothing watching
it, and a later rename in `vq.spec.JobState` would go unnoticed. The check
fails with the states it found and what to do about them.

The narrower alternative was deliberate. This file itself writes
"`vq kill` produces `killed`, not `cancelled`", and vq does not produce
`cancelled` at all. A check that read every backticked name on the page would
need a list of every other identifier the prose uses, twenty of them today,
and would then fail for reasons that have nothing to do with the enum.

**Put a command in a fence, or in backticks with its verb.** The command
lines are read out of `sh` fences and out of indented blocks inside a MyST
directive, and separately out of inline spans that begin `vq `. Unbackticked
prose is read as English, so "vq delegates over SSH to the remote" is safe
and needs no escaping. What is not safe is a bare option in a fence attached
to no command; write the verb with it.

**Name the tool when an option is not vq's.** The pages document other
programs too. Options after `--` belong to whatever is being run, which is
why the checks split the line there, and the two shipped wrappers are checked
against `contrib/`. An option belonging to a tool this repository does not
ship has nowhere to be checked, so it goes in the guard's `_FOREIGN_OPTIONS`
with the tool that owns it. There are three today: `--section` is
`vibe-view capture`'s, `-m` is python's and `-a` is `xvfb-run`'s. Keep that
list short: a long one is how a guard stops guarding. Keep it reachable, too.
The first version of that check only read spans that looked like a bare
option, so `capture --section ID` never reached it and the exemption was
decorative; removing the entry changed nothing.

Configuration keys are read only from `toml` fences, not from prose. The
prose also writes bracketed names that are not vq configuration at all, such
as `[outputs].status` and `[plan]`, which are sections of vibe-qc's `.system`
manifest. Every table the prose refers to appears in a fence as well, so
nothing is lost by the narrower rule.

The first check replaced one that could never fail. That version kept only
names already in the enum, so a renamed state dropped out of what it checked
instead of failing it (vibe-queue#39). The block check was written after the
block was found to be missing `starved`: every outcome it showed was real, so a
reader who handled all of them still missed one vq produces. The pattern is
`tests/test_admin_outcomes.py`'s, which pins `docs/orchestration.md` against
`admin.ADMIN_OUTCOMES`.

What is still prose against prose is the *behaviour* half of every row: that
`--expected-sha` refuses a moved checkout, that `--vibeqc-preflight` is not
forwarded to a remote submit, that cleanup is dry-run until `-x`. The guard
can tell you a flag exists and is spelled onto the right command. It cannot
tell you the sentence around it is still true.

Be blunt about what that costs, because the number is the useful part: the
guard was run against the pages as they stood before the 2026-09-18 audit and
**passed**. Not one of the six corrections above was of the kind it catches.
A missing `--np` is an absence, not a wrong token; a verb used for the wrong
job is a real verb; an invented sample output, an unmeasured timing claim and
a mislabelled link are all prose. What the guard covers is the class the
original vibe-queue#39 report was about, `vq throttle --max-jobs 1` and
`[programs.*]` keys that never existed, and that class is now closed. The
class this audit found is still yours to catch by reading.

So when a release touches submit, fetch, cleanup, the lifecycle or the
wrappers, re-read the behaviour claims against the source. That is the
cheaper half of the practice and the reason a drift is recoverable at all,
but it is not a guard.
