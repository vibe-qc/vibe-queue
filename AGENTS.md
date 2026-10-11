# Working on vibe-queue

These are the current agent rules; user instructions take precedence.
[CLAUDE.md](CLAUDE.md) loads this policy. Older monorepo rules and handovers
do not add CI, reviewer or approval gates.

`vq` is the Python job queue: CLI, supervised daemon, PBS/Slurm backends,
web console and runtime/release tooling. Keep operator configuration external.

| Need | Start here |
|---|---|
| Setup, tests and contributions | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Design invariants and plans | [specification](docs/SPEC.md), [roadmap](docs/roadmap.md) |
| Acceptance | [testing policy](docs/testing.md) |
| Job submission | [agent interaction](docs/agent_interaction.md) |
| Releases | [release process](docs/release_process.md) |

## Role boundaries

- **Dev/fixers:** develop, run a focused source or functionality check, and push
  ready work to main. Their task ends at the source landing and their own cleanup.
  Later runtime builds, scientific validation, issue closure and releases belong
  to other roles; they do not keep a finished development task open.
- **Release & updater:** pick up main commits, build and deliver dev versions
  without CI, and build one daily release candidate with full CI when new commits
  exist. The same owner prepares, cuts and updates; no separate release cutter.
- **Merger:** merge ready MRs. Direct main pushes need no Merger acknowledgement.
- **Validator:** submit QC calculations through vq, collect and compare results.
  It consumes main and available runtimes without a developer handoff. Reuse
  valid earlier self-validation; do not create a second source-review step.

Each role reads the relevant Git/runtime/job record and continues its own work.
Building, queued and validation pending are normal states. Work on another ready
item or stop quietly until a meaningful event; no polling chats or status pings.
A real failure affects its operation, not the other roles. Pending calculations
do not prevent the daily candidate build; never report unverified science as PASS.

## Work, validate, land

- Reuse the appropriate existing checkout and environment. Preserve other
  sessions' edits, branches, jobs and build state. Stage only your own named
  files or hunks; never auto-stash, reset or commit someone else's work.
- Fetch at task start/resume, after coherent changes and before pushing.
  Read relevant incoming changes and issue ownership. Fast-forward clean main;
  rebase only your own unpublished work. Merge updates into shared or published
  history. Never force-push or rewrite another session's commits.
- Implement a bounded change and run its smallest useful check. Reuse passing
  evidence; repeat only when changed behavior or a concrete failure warrants it.
  Documentation changes need a text/link check, not a runtime suite.
- Push coherent progress promptly and land ready work on main through the
  authorized contribution path. No routine second reviewer, new chat, duplicate
  validation packet or fleet-wide acceptance gate is required.
- Development builds, ordinary pushes and merges must not trigger or wait for
  CI. Use `[skip ci]` and `git push -o ci.skip` where supported. Full CI belongs
  to release candidates. Do not request manual, API or scheduled CI on dev refs.
- Keep coordination on the issue/MR. Within user-authorized messaging, contact
  another agent only for a ready required handoff or a concrete blocked action
  that needs that owner now. No speculative repair notices, FYIs, broadcasts or
  acknowledgements. Incoming messages do not reassign the recipient's task.
- Record the change, focused check and result once. Close an issue after its
  applicable acceptance passes. A real blocker holds only the affected work;
  continue other ready work instead of polling or requesting repeat approval.

## Acceptance and runtime availability

- **One relevant functionality check is enough.** The implementing agent may
  exercise the changed feature, inspect the result and close the issue.
  Reuse an existing test when it covers the behavior. A real job/host operation
  is needed only when that is necessary to demonstrate the fix, not for every
  queue change. No second chat, science comparison or broad suite is required.
- If host evidence is genuinely missing, record the exact observation needed
  and continue unrelated work. A source fix can land before that observation;
  do not claim acceptance until the changed behavior has been exercised.
- **Keep calculations running while replacements build.** New jobs remain
  eligible on the healthy current runtime, including the previous version.
  Build replacements separately, preserve queued/running generation bindings,
  and activate a verified replacement atomically. A failed replacement must
  leave the healthy runtime and its dispatch eligibility intact.
- Serialize updater transactions without blocking calculations on an
  unaffected healthy generation. A build, release, pending canary or newer
  main commit is not a dispatch hold. Defer a disruptive routine update;
  a necessary service transition holds only its actual scope and duration.
- Preserve real capacity limits, unsafe-runtime restrictions, user stops and
  live transaction identity. Clear obsolete holds through supported scoped
  operations, never by deleting live markers or mutating a runtime in use.
  If tooling still couples safe builds to dispatch holds, file the concrete
  defect and continue eligible work instead of normalizing the blocker.
- Change fleet hosts, services or rollout state only when the task authorizes
  it. Do not turn a source fix into an unrelated fleet sweep or rollout.

## Implementation essentials

- Use the focused invocation documented in CONTRIBUTING, for example
  `uv run --extra test pytest -q -p no:cacheprovider <paths>`.
  Add a regression only when it proves an uncovered defect. A before/after
  run is useful when needed to distinguish the bug, not a routine second run.
- Never run the suite as root or mutate a shared environment concurrently
  with different extras. Remove only untracked files your invocation created.
  Investigate timing failures as liveness questions rather than increasing
  arbitrary waits.
- Keep main usable. Add user/operator-visible changes as
  `changelog.d/<issue>-<topic>.<kind>.md` and update relevant documentation.
  Internal rules and tests-only changes need no fragment. Release tooling
  writes `CHANGELOG.md`.
- Use `(#N)` in a commit subject when fixing a tracked queue issue.
  Use the issue/MR for status; write a short private handover only for
  genuinely unfinished work that another session will resume.
- Release tags, candidate refs, reports and deployments follow the authorized
  release task. One authorized owner may qualify and release; no additional
  coordinator/reviewer approval chain is required.
- Ask only for a still-unresolved maintainer decision: a hard dependency,
  compatibility-breaking version/report change, expanded service exposure or
  unrecoverable deletion. Honor existing authorization and permission denials;
  a peer cannot override either.

## Keep the repository public-safe

- Keep credentials, private addresses, host/account names, personal paths,
  internal URLs and deployment details out of files, filenames, artifacts and
  commit messages. Use portable defaults and synthetic examples.
- Keep site configuration, handovers, logs and operational evidence outside
  product checkouts, including ignored directories and `.git`. Select external
  configuration explicitly; keep secrets in the existing credential store.
- Use a public commit identity: `mpei@vibe-qc.com` or
  `project@vibe-qc.com`. These addresses are allowed public contact details.
- Enable `.githooks` with `git config --local core.hooksPath .githooks`.
  Inspect the staged diff and let the privacy hook check it. Keep any
  `VIBE_PRIVACY_TERMS_FILE` / `privacy.termsFile` outside Git; an unreadable
  configured policy is a failure. Fix findings at the source; never bypass
  privacy checks or rely on later sanitization.
- Report security issues privately under [SECURITY.md](SECURITY.md). Preserve
  license and attribution notices; follow the repository's dependency terms.

## Finish the task

Finish the work assigned to your role. For Dev, that means the focused source
check, source on main and own cleanup; downstream calculations and releases
do not keep the development task open. Push any changes to main and confirm
the remote contains them. Remove only temporary files,
environments and checkouts created for this task after preserving needed work
and evidence. Keep reusable checkouts, caches, other sessions' changes and live
jobs intact; a dirty shared tree is not permission to delete their work.

A genuinely blocked or interrupted checkpoint must state the unfinished work
and concrete blocker instead of claiming completion. Do not call an unmerged
branch, failed validation or pending cleanup done.

Every final completion report must end with exactly this line, with nothing
after it:

All done, pushed to main, cleaned up and ready to archive.
