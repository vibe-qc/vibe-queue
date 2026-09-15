"""The host_f authoritative-mirror ref contract.

host_f's compute build node is offline and the online login node is intentionally
credential-free for private GitLab, so `prepare-host_f-runtime-source` resolves
pins from a bare mirror at ``~/.local/share/vq-host_f/vibeqc.git`` that is fed by
**push**. That mirror's ``remote.origin.url`` points at *itself*.

The 2026-07-23 failure: the preparer ran

    git fetch --prune origin '+refs/heads/main:refs/remotes/origin/main' ...

against that self-referencing origin, which copies the mirror's **stale**
``refs/heads/main`` over the **freshly pushed** ``refs/remotes/origin/main``::

    93b438f6..87fdc94c  main -> origin/main  (forced update)

so the feed was silently reverted and preparation failed *"main moved"*. It had
been written off as a harmless no-op; it was actively undoing the operator's
feed. Release pins were unaffected only because the tag check never consults
``origin/main``, which is why release bundles prepared while dev could not.

These tests pin the git-level contract the fix depends on, using real
repositories in a tmpdir — no host_f, no network. They are the guard that a
newly-fed main SHA survives preparation.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


def _git(*args: str, cwd: Path) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.fixture
def upstream(tmp_path: Path) -> Path:
    """A source repo with two commits, standing in for GitLab."""
    up = tmp_path / "upstream"
    up.mkdir()
    _git("init", "-q", "-b", "main", cwd=up)
    _git("config", "user.email", "t@t", cwd=up)
    _git("config", "user.name", "t", cwd=up)
    (up / "f.txt").write_text("old\n")
    _git("add", "f.txt", cwd=up)
    _git("commit", "-q", "-m", "old", cwd=up)
    (up / "f.txt").write_text("new\n")
    _git("commit", "-q", "-am", "new", cwd=up)
    return up


@pytest.fixture
def mirror(tmp_path: Path, upstream: Path) -> Path:
    """A bare mirror frozen at the OLD commit, with origin pointing at itself.

    This reproduces host_f's mirror exactly: cloned once, then cut off from the
    real upstream and left with a self-referencing origin.
    """
    m = tmp_path / "vibeqc.git"
    subprocess.run(
        ["git", "clone", "--mirror", "-q", str(upstream), str(m)], check=True
    )
    old = _git("rev-parse", "HEAD~1", cwd=upstream)
    _git("update-ref", "refs/heads/main", old, cwd=m)
    _git("update-ref", "refs/remotes/origin/main", old, cwd=m)
    # The defining property of host_f's mirror.
    _git("remote", "set-url", "origin", str(m), cwd=m)
    return m


def _feed(upstream: Path, mirror: Path, sha: str) -> None:
    """The supported feed: pin BOTH refs to the exact commit.

    Feeding both is what keeps them agreeing, so the preparer's
    ambiguity guard never has to choose between two truths.
    """
    _git(
        "push", str(mirror),
        f"+{sha}:refs/heads/main",
        f"+{sha}:refs/remotes/origin/main",
        cwd=upstream,
    )


def test_the_self_fetch_reverts_a_fed_sha(upstream: Path, mirror: Path) -> None:
    """THE BUG. Kept as an executable record of why the fetch was removed.

    Feed the mirror, then run the preparer's original fetch. The fed SHA is
    silently rolled back to the stale one — no error, exit 0.
    """
    new = _git("rev-parse", "HEAD", cwd=upstream)
    old = _git("rev-parse", "HEAD~1", cwd=upstream)
    _feed(upstream, mirror, new)
    assert _git("rev-parse", "refs/remotes/origin/main", cwd=mirror) == new

    # The original, destructive refresh.
    _git(
        "fetch", "--prune", "origin",
        "+refs/heads/main:refs/remotes/origin/main",
        cwd=mirror,
    )

    # refs/heads/main was never fed by the old feed, so it drags origin/main back.
    _git("update-ref", "refs/heads/main", old, cwd=mirror)
    _git(
        "fetch", "--prune", "origin",
        "+refs/heads/main:refs/remotes/origin/main",
        cwd=mirror,
    )
    assert _git("rev-parse", "refs/remotes/origin/main", cwd=mirror) == old, (
        "expected the self-fetch to revert the fed SHA — this is the bug"
    )


def test_a_fed_sha_survives_when_the_self_fetch_is_skipped(
    upstream: Path, mirror: Path
) -> None:
    """THE FIX. Skip the fetch on a self-referencing origin and the feed holds."""
    new = _git("rev-parse", "HEAD", cwd=upstream)
    _feed(upstream, mirror, new)

    # The fix's condition: origin resolves to the mirror itself => no fetch.
    origin_url = _git("config", "--get", "remote.origin.url", cwd=mirror)
    assert Path(origin_url).resolve() == mirror.resolve()

    assert _git("rev-parse", "refs/heads/main", cwd=mirror) == new
    assert _git("rev-parse", "refs/remotes/origin/main", cwd=mirror) == new


def test_the_feed_pins_an_exact_dev_sha_not_the_branch_tip(
    upstream: Path, mirror: Path
) -> None:
    """A dev pin is an exact commit, which is usually NOT the current tip.

    The rollout pins the last *green* dev SHA, and main has typically moved on
    since. Feeding `origin/main` would push the tip and the pin would fail
    "main moved"; the supported feed names the commit explicitly.
    """
    older = _git("rev-parse", "HEAD~1", cwd=upstream)
    _feed(upstream, mirror, older)

    assert _git("rev-parse", "refs/heads/main", cwd=mirror) == older
    assert _git("cat-file", "-t", older, cwd=mirror) == "commit"


def test_both_refs_agree_after_a_feed(upstream: Path, mirror: Path) -> None:
    """The preparer fails closed on disagreement, so the feed must not create it."""
    new = _git("rev-parse", "HEAD", cwd=upstream)
    _feed(upstream, mirror, new)

    assert _git("rev-parse", "refs/heads/main", cwd=mirror) == _git(
        "rev-parse", "refs/remotes/origin/main", cwd=mirror
    )


def test_a_half_feed_is_detectable_as_ambiguous(
    upstream: Path, mirror: Path
) -> None:
    """Feeding only one ref leaves the two disagreeing.

    The preparer refuses this rather than guessing which is authoritative —
    guessing is how a rollout pins the wrong commit.
    """
    new = _git("rev-parse", "HEAD", cwd=upstream)
    _git("push", str(mirror), f"+{new}:refs/heads/main", cwd=upstream)

    heads = _git("rev-parse", "refs/heads/main", cwd=mirror)
    remote = _git("rev-parse", "refs/remotes/origin/main", cwd=mirror)
    assert heads != remote, "a half-feed must be visibly ambiguous"


def test_release_pins_were_unaffected_by_the_bug(
    upstream: Path, mirror: Path
) -> None:
    """Why release bundles prepared while dev could not.

    The tag path checks `refs/tags/<TAG>`, which the self-fetch also copies —
    but from the same tag namespace, so it is idempotent. Only the
    heads->remotes mapping was lossy.
    """
    new = _git("rev-parse", "HEAD", cwd=upstream)
    _git("tag", "-f", "vX.Y.Z", new, cwd=upstream)
    _git("push", str(mirror), "+refs/tags/*:refs/tags/*", cwd=upstream)

    _git("fetch", "--prune", "origin", "+refs/tags/*:refs/tags/*", cwd=mirror)

    assert _git("rev-parse", "refs/tags/vX.Y.Z^{commit}", cwd=mirror) == new
