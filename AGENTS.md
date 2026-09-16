# AGENTS.md — working on vibe-queue

For any AI coding agent working in this repository, and for humans who want
the short version. Codex reads this file directly; Claude Code loads it
through [`CLAUDE.md`](CLAUDE.md).

**This replaces the pre-split rules.** The archived monorepo rules were
written for vibe-qc and are **not binding here**. Older handovers and docs cite it as "CLAUDE.md § N". Read
those citations as history; the rules that still apply to vq are below.

## Keep this repository public-safe

This product repository must remain ready for public mirroring at every commit.

- Never put private email addresses, real machine names or host aliases,
  internal hostnames or URLs, private IP addresses, account names, personal
  filesystem paths, credentials, tokens or site-specific deployment details
  in tracked files, filenames, generated artifacts or commit messages.
  This includes code, comments, tests, documentation and agent instructions.
- `project@vibe-qc.com` and `mpei@vibe-qc.com` are explicitly allowed public
  email addresses. Use generic placeholders and reserved example addresses
  for tests and documentation; do not copy real private values into fixtures.
- Keep private configuration separate from product code. Store it outside
  the product checkout on the local machine, or in the private agentic loop
  repository. Select it through environment variables, command-line options
  or an explicit external configuration path. Commit only portable defaults,
  schemas and examples without private values.
- Ignored files and custom folders under `.git` are not private configuration
  stores. Keep private operational logs, inventories and release evidence
  outside the product checkout too. Never commit secrets to the private loop
  repository; use the existing credential or secret store.
- Prevent contamination while making the change. Inspect the diff and use the
  existing automated privacy checks before committing. Fix a finding in the
  source; do not rely on a later sanitizer or create sanitation chats for
  routine releases. Never bypass a privacy failure to publish a release.

## What this repository is

vq is the cross-machine job queue of the vibe-qc toolset. It is pure Python
3.12 or newer: a CLI, a supervised per-host daemon, scheduler backends (PBS
and Slurm), a web console, and the fleet release and rollout tooling.

The repository is `vibe-queue`. Development and review use the canonical
GitLab tracker. Obtain its project ID and connection details from external
operator configuration; do not put site access details in tracked content.
Public reports and contributions use the GitHub forms described in
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## Start here

| read | for |
|---|---|
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Setup, running tests, code style, commit messages |
| [`docs/roadmap.md`](docs/roadmap.md) | What is next, and what is released but not yet validated |
| [`docs/SPEC.md`](docs/SPEC.md) | Design invariants |
| [`docs/release_process.md`](docs/release_process.md) | How a release is cut (usually not by you) |
| [`docs/agent_interaction.md`](docs/agent_interaction.md) | Submitting work to fleet hosts *through* vq, as opposed to changing vq |
| External operator handovers | Resume from the latest record for your workstream |

## The hard rules

These are the only rules that are not a matter of judgment.

1. **Never force-push `main` or `release`,** and never amend or rewrite a
   commit that has been pushed.
2. **Don't skip hooks.** The pre-commit hook guards privacy. If a reviewed
   exception really needs `--no-verify`, say why in the commit message.
3. **Nothing private in tracked content:** credentials and tokens,
   home-directory paths, private IP addresses, real host addresses, ports or
   account names, employer names. `.githooks/pre-commit` and
   `tests/test_no_maintainer_paths.py` check the mechanical part.
   - **Security bugs** go by email, per [`SECURITY.md`](SECURITY.md), not to a
     public issue.
   - **A confidential issue's details** stay out of commits, CHANGELOG and
     handovers until its fix is on `main`.
4. **Don't create release refs** unless you are the release coordinator for
   that cut: no `vX.Y.Z` tags, no push to `release` or `release-candidate/*`,
   and no `releases/*.json`.
5. **Don't change fleet hosts** unless the maintainer asked you to for the
   task at hand. That covers updates, rollouts, host configs and services.
   Reading fleet state is fine (`vq programs`, `vq admin status --json`,
   `vq doctor`). Routine rolls belong to the release coordinator.

Everything below is how work here usually goes. Treat it as defaults, use
judgment, and say what you did.

## Landing changes

- **Either way is fine:** a merge request, or rebase-then-push to `main`:

  ```sh
  git fetch origin
  git pull --rebase origin main
  git push origin HEAD:main
  ```

  A push rejected after a clean rebase means someone landed first; repeat.
  CI (`ruff`, `test`, `docs-build`) runs on `main` and on merge requests.
- **Keep `main` working.**
  - Run the tests your change touches and name them in the commit body.
  - Leave no half-finished code paths.
  - Add a `CHANGELOG.md` entry under `[Unreleased]` for anything a user or
    operator would notice.
- **Name the issue in the subject** when a commit fixes one, e.g.
  `fix(dispatch): retry a probe that raced the job id (#42)`. In a subject,
  `(#N)` means an issue in the canonical queue tracker and nothing else;
  refer to handover items as `§ N`.
- **Merged is not validated.** When a fix needs host or CI evidence, comment
  on the issue with the commit and how to check it. Close the issue on that
  evidence.

## Shared files and shared trees

- **Shared ledgers.** `CHANGELOG.md` and the handovers take entries from many
  sessions.
  - On a conflict, keep the union of both sides. Don't resolve with
    `--ours`/`--theirs`, which are inverted during a rebase anyway.
  - Before staging, `git diff :2:<file> <file>` and
    `git diff :3:<file> <file>` should show only removals you meant.
  - After a rebase, check that your CHANGELOG entry is still under
    `[Unreleased]`: a release cut can land underneath you.
- **Shared working trees.** Stage explicit paths rather than `git add -A`.
  Don't stash, reset or delete changes, branches or worktrees you did not
  create.

## Other agents work here too

- **Before starting on an issue:** fetch, read the issue and its comments,
  and check whether another session or worktree already has it.
  Uncommitted work in a sibling `.claude/worktrees/*` counts. Coordinate
  rather than build a second copy.
- **Decisions that belong to the maintainer** go on the tracker, as a
  "Needs a decision: …" issue or a note on an existing one, not a guess.
- **If your own permissions deny an action,** ask the maintainer. Don't hand
  it to another session to do instead.

## Tests, practically

- **Invocation.** The canonical one is in `CONTRIBUTING.md`. With uv:
  `uv run --extra test pytest -q -p no:cacheprovider <paths>`.
  - The full suite also needs `--extra web` and takes about 25 minutes.
  - Delete the untracked `uv.lock` that uv leaves behind.
  - Don't run two `uv run` commands with different extras at once in one
    tree.
- **Never run the suite as root.**
- **Flakes under load** are usually a wait sized as a timing claim. Bound a
  wait for liveness, and keep negative probes short.
- **Prove the test.** Where you can, show that a fix's new test fails on the
  code before the fix.

## Handovers

For a workstream that will outlive your session, keep
an external private `HANDOVER_<topic>.md` current: what landed (SHAs, tests), next
steps, open decisions, and the `main` SHA you last synced to. A defect is an
issue, not a handover.

## Ask the maintainer first about

- anything the hard rules cover;
- a new hard runtime dependency;
- a change to the version scheme, the release report schema, or
  `vq --version` output (these cross repositories);
- widening what the daemon or the web console exposes (it needs a matching
  `SECURITY.md` entry);
- deleting anything that may not be recoverable: job specs, results, host
  state.
