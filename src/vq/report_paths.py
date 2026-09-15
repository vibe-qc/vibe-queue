"""Where an accepted release report may live, and when two paths are one report.

A leaf module with no vq imports, because :mod:`vq.legacy_failure_transition`
is deliberately dependency-free and still has to compare report paths.

The 2026-09-08 split left every report with two legitimate locations. Path
*equality* is therefore not report identity, and that was re-broken one
comparison at a time: `b2aef94` fixed one copy, `721bea7` five more, and each
failing check had been masking the next. :func:`same_report` is the one place
that answers the question; a test asserts nothing else does.
"""

from __future__ import annotations

# Deliberately still the monorepo-relative path.
#
# fleet_rollout validates that a rollout receipt's `source_path` starts with
# `REPORT_DIRECTORY/`, and every receipt already persisted on the fleet
# records "vibe-queue/releases/...". Changing this constant would make vq
# reject its own history ("cannot form a coherent persisted supersession
# record") on live hosts.
#
# The fleet still deploys from the pre-split monorepo, where this path is
# also correct. Migrating it belongs with the fleet repointing, alongside the
# release-report schema bump and a compatibility window for old receipts --
# not with the repository split, which does not change how the fleet runs.
REPORT_DIRECTORY = "vibe-queue/releases"

REPORT_DIRECTORIES = (REPORT_DIRECTORY, "releases")
"""Every prefix a persisted receipt's ``source_path`` may legitimately carry.

Reports are still *written* under :data:`REPORT_DIRECTORY`; this widens only
what is *read back*. Both prefixes are real during the split transition:
every receipt already persisted on pbs-cluster and slurm-cluster records
``vibe-queue/releases/...``, while vibe-queue's own repository holds its
reports at ``releases/...``.

Accepting both is what makes the fleet repointing a non-breaking change.
Flipping the constant instead would make vq reject its own history --
``cannot form a coherent persisted supersession record`` -- on every host
still holding a pre-split receipt.
"""


def is_report_path(value: object) -> bool:
    """True iff ``value`` names a release report under an accepted prefix."""
    return isinstance(value, str) and any(
        value.startswith(f"{directory}/") for directory in REPORT_DIRECTORIES
    )


def report_paths_for(tag: str) -> tuple[str, ...]:
    """Every path a report for ``tag`` may legitimately occupy.

    Both layouts are live: ``vibe-queue/releases/`` in the monorepo,
    ``releases/`` in vibe-queue's own repository. Discovery searches both
    rather than deriving one from the *installed* vq's layout -- that would
    be the wrong question, since the repository being searched need not be
    the one vq is running from (tests and `--repo` both do exactly that).
    """
    return tuple(f"{directory}/{tag}.json" for directory in REPORT_DIRECTORIES)


def same_report(left: object, right: object) -> bool:
    """Do two report paths name the same accepted report?

    ``vibe-queue/releases/v0.15.118.json`` and ``releases/v0.15.118.json`` are
    one report in two live layouts. Discovery returns whichever spelling it
    found, while a journal or receipt keeps the one it was written with, so a
    plain ``==`` between them rejects a report that authenticated perfectly.

    Anything string equality accepted is still accepted, and non-strings
    compare exactly as before. The only widening is two *legitimate*
    locations of the same tag. The path never bound content -- the digest
    does -- so this changes where a report may be found, not which report it
    is.
    """
    if not isinstance(left, str) or not isinstance(right, str):
        return left == right
    if left == right:
        return True
    tag = left.rsplit("/", 1)[-1].removesuffix(".json")
    candidates = report_paths_for(tag)
    return left in candidates and right in candidates
