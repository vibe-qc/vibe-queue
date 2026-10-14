# Release process

**Audience: contributors and the release coordinator.** Listed under Reference on the [front page](index.md).

vibe-queue is independently versioned. Development changes land on the canonical
GitLab main branch through the existing contributor and review process.
Released tags are immutable. Do not rewrite published history or move a
release tag to repair documentation or packaging metadata.

## Contributor evidence

Follow the repository's CONTRIBUTING.md and developer test guidance. A change
needs the relevant tests and validation comparisons, with commands, selected
source revision and results recorded for review. A release blocker or a fix to
an incorrect numerical result requires an independent check before closure.
Describe a change a user or an operator would notice in a changelog fragment
under `changelog.d/` (see `CONTRIBUTING.md`, "Changelog fragments"); preserve
released changelog sections.

AI-assisted changes follow the same disclosure, attribution, licensing and
validation requirements as other contributions. Assistance does not replace
review or evidence. See the existing contributor policy for the full contract.

## Maintainer publication

The release owner selects scope and version only after the required gates
pass. The agentic loop coordinates the source tag and supported publication
jobs. Site-specific commands, accounts, destinations and gate-operation
runbooks are maintained in the private operations repository. Contributor
source archives do not contain or install those operational scripts.

A release candidate is proved on a `release-candidate/vX.Y.Z` branch, which
exists to carry the gate pipeline. Its test job always runs the full suite,
including web dependencies; the local smoke default does not apply to CI.
Manual, scheduled and API pipelines must also target a release ref. Development
refs never run CI, and development builds do not wait for a pipeline or release
report. The `release` branch builds documentation and offers the configured
manual publication job, without repeating the candidate suite.
**Delete the candidate branch once the tag exists and the
candidate is merged into `main`:** `git push origin --delete
release-candidate/vX.Y.Z`. The gate evidence does not live in the branch. The
pipelines API returns a deleted branch's pipelines by ref name,
`scripts/make_release_report.py` finds them by release SHA and ref name, and
the committed report records the ref string; nothing resolves the branch
through git afterwards. Keeping it would only leave a second, mutable name for
a commit the tag names immutably (#43).

The changelog for the release is assembled from fragments. Before the version
heading is written, run `python scripts/fold_changelog.py`: it moves every
`changelog.d/*.md` under `## [Unreleased]` into its Keep a Changelog section,
ordered by issue number, and deletes the fragments. Review the result, stage
`CHANGELOG.md` together with the removed fragments, and then turn
`[Unreleased]` into the release's section as before. The test suite runs the
script's `--check`, so a misnamed fragment fails before the cut, not at it.

A public source snapshot has its own commit identity and records the original
source revision and per-file checksums. It contains no private development
ancestry. A correction is published as a new snapshot or patch release;
existing source tags remain unchanged. Public availability is established by
successful anonymous access and the publication verification report.

## External project identities

Release report generation and validation share an operator-maintained JSON file
outside all Git checkouts. Select it with `VQ_SOURCE_PROJECTS_FILE`, or place it
at `<private-state-root>/vibe-queue/config/source-projects.json`. The private root
uses `VIBE_PRIVATE_ROOT`, then `$XDG_STATE_HOME/vibe-private`, then
`~/.local/state/vibe-private`. Keep the file owned by the operator with mode 0600.

The object contains `pins` with entries `release`, `dev`, `vq`, and `vibe_view`,
and a `legacy` entry for the retained pre-split archive. Every entry contains a
`repo` slug and its positive GitLab `project_id`. Release and dev identify the
same project. Obtain these values from the site's trusted project inventory;
never derive trust from the release report being checked.

Before upgrading an existing deployment, preserve its current source identities
in this file on every release driver. Both report generation and validation
refuse missing or invalid configuration; ordinary queue commands do not require
it. Existing report bytes and pin ancestry checks remain unchanged.

`pin_source_repos` continues to use the operator's full repository slugs. Source
staging finds each product by its final path component and refuses ambiguous
entries. It does not assume a personal or organizational namespace.
