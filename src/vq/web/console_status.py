"""What this console process actually is, and whether it is stale.

A console is a long-lived process serving pages *about* a vq deployment,
which makes it uniquely prone to a failure no other vq surface has: it
can be running code much older than the vq installed beside it, and every
page it renders will look completely normal. A CLI verb cannot drift —
you invoke it and it runs whatever is installed right now. A daemon that
drifts eventually misbehaves in a way somebody notices. A stale console
just quietly reports the past, in the present tense.

That is not hypothetical. The 2026-08-05 fleet audit found the reference
fleet's console pinned to a hand-staged tree 1081 commits behind the vq
that owned it, serving a version number ten times over as if it were a
property of the fleet. Nothing on the page, in the logs, or in any
convergence check said so.

So the console checks itself, and says so on its own pages:

* :func:`console_identity` — what is running: version, interpreter,
  start time, source SHA. Enough to tell two consoles apart, and enough
  to find the tree on disk.
* :func:`console_staleness` — whether that matches the vq the local
  daemon is running. Divergence is the signature of exactly this bug.

Both are best-effort and never raise: a console that cannot introspect
itself must still serve pages.
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from time import monotonic

from vq import __version__

log = logging.getLogger(__name__)

#: Set once, at import, so it reflects when this process actually began
#: rather than when somebody first asked.
_STARTED_AT = datetime.now(UTC)


def _tree_digest() -> str | None:
    """Content digest of the vq package on disk, or None if underivable."""
    try:
        from vq.admin import source_tree_sha256  # noqa: PLC0415

        return source_tree_sha256()
    except Exception:  # pragma: no cover — defensive
        return None


#: Digest of the source tree *as it was when this process imported vq*.
#:
#: The version comparison below catches a console left behind by an
#: upgrade. It does not catch the case that actually happened on
#: 2026-08-05: the files a console was running from were replaced under
#: it by a checkout move, while the version string stayed 0.24.0 on both
#: sides. Comparing the digest captured at import against the digest on
#: disk right now catches exactly that, because a running process keeps
#: executing what it already imported.
_STARTED_TREE_DIGEST = _tree_digest()


@dataclass(frozen=True)
class ConsoleIdentity:
    """Facts about the process serving these pages."""

    version: str
    """``vq.__version__`` of the code actually imported by this process.

    Note this is emphatically *not* a fact about any host in the fleet,
    which is precisely how it came to be misread as one."""

    executable: str
    """Absolute path to the interpreter. The fastest way to find which
    checkout a drifted console is serving from."""

    source_sha: str | None
    """Commit of the tree this vq was installed from, when derivable."""

    started_at: str
    """ISO-8601 UTC. A console that has been up for weeks is a console
    that has missed every update in those weeks."""

    pid: int

    def describe(self) -> str:
        """One line for a tooltip or a log."""
        parts = [f"vq {self.version}"]
        if self.source_sha:
            parts.append(self.source_sha[:9])
        parts.append(self.executable)
        parts.append(f"up since {self.started_at[:19].replace('T', ' ')} UTC")
        return " · ".join(parts)


@dataclass(frozen=True)
class ConsoleStaleness:
    """Verdict on whether this console is running current code."""

    stale: bool
    """True only on positive evidence of divergence. An unknown
    comparison is never reported as stale — a console that cannot reach
    the daemon must not cry wolf about its own version."""

    console_version: str
    daemon_version: str | None
    reason: str | None
    """Operator-facing explanation, including what to do. None when not
    stale."""

    @property
    def known(self) -> bool:
        """Whether the comparison could be made at all."""
        return self.daemon_version is not None


def console_identity() -> ConsoleIdentity:
    """Identity of this console process. Never raises."""
    source_sha: str | None = None
    try:
        from vq.admin import running_source_sha  # noqa: PLC0415

        source_sha = running_source_sha(Path(__file__))
    except Exception:  # pragma: no cover — defensive
        source_sha = None
    return ConsoleIdentity(
        version=__version__,
        executable=sys.executable,
        source_sha=source_sha,
        started_at=_STARTED_AT.isoformat(),
        pid=os.getpid(),
    )


#: The staleness probe dials the daemon's unix socket. That is cheap, but
#: not free, and the answer changes only when somebody restarts a service
#: — so cache it rather than probing on every page render.
STALENESS_TTL_SECONDS = 60.0

#: Keyed on ``multi_user``, because it selects which daemon socket the
#: probe dials. A single shared slot would let a single-user query serve
#: a multi-user caller the wrong daemon's version for up to the TTL, and
#: "the console reports the wrong daemon's version" is the exact class of
#: bug this module exists to catch.
_staleness_cache: dict[bool, tuple[float, ConsoleStaleness]] = {}
_staleness_lock = Lock()


def cached_staleness(*, multi_user: bool = False) -> ConsoleStaleness:
    """:func:`console_staleness` behind a short TTL, for template use.

    Safe to call from a request handler: at most one probe per
    :data:`STALENESS_TTL_SECONDS`, and a probe that raises still yields a
    usable "not stale" verdict.
    """
    with _staleness_lock:
        cached = _staleness_cache.get(multi_user)
        if cached is not None and monotonic() - cached[0] < STALENESS_TTL_SECONDS:
            return cached[1]

    # Probed outside the lock on purpose. A hung daemon socket would
    # otherwise block every page render behind one stalled probe, which
    # is a worse failure than the handful of redundant dials that
    # concurrent renderers can produce at expiry.
    verdict = console_staleness(multi_user=multi_user)

    with _staleness_lock:
        # Stamped with the clock reading from *after* the probe. Using the
        # pre-probe reading ages the entry by however long the probe
        # blocked -- a slow enough probe produced an entry that was
        # already expired when it was stored.
        _staleness_cache[multi_user] = (monotonic(), verdict)
    return verdict


def reset_staleness_cache() -> None:
    """Drop every cached verdict (tests)."""
    with _staleness_lock:
        _staleness_cache.clear()


def _source_drift_reason() -> str | None:
    """Explain how the on-disk source diverged from what is running, or None.

    Compares the digest captured when this process imported vq against
    the digest on disk now. Silent unless BOTH digests are derivable and
    they differ: an underivable digest means "cannot tell", and a console
    that cannot tell must not raise an alarm.
    """
    if _STARTED_TREE_DIGEST is None:
        return None
    current = _tree_digest()
    if current is None or current == _STARTED_TREE_DIGEST:
        return None
    return (
        f"The vq source under {sys.executable} has changed on disk since "
        f"this console started (tree digest {_STARTED_TREE_DIGEST[:12]} at "
        f"startup, {current[:12]} now). The running process is still "
        f"executing the code it imported, which no longer exists on disk, "
        f"so this page may not reflect the installed vq at all. Restart "
        f"the console service."
    )


def console_staleness(*, multi_user: bool = False) -> ConsoleStaleness:
    """Compare this console's vq against the local daemon's.

    The daemon is the right yardstick because both are installed by the
    same update path on the same machine. If they disagree, one of them
    was not restarted after an update — and the console is overwhelmingly
    the likelier of the two, since nothing in the rollout ever restarted
    it before v0.25.0.

    Never raises, and never guesses. No daemon, no answer, no warning.
    """
    from vq import rpc  # noqa: PLC0415 — avoid import cost off this path

    daemon_version: str | None = None
    try:
        payload = rpc.ping(multi_user=multi_user)
        if isinstance(payload, dict):
            raw = payload.get("version")
            if isinstance(raw, str) and raw.strip():
                daemon_version = raw.strip()
    except Exception as e:  # pragma: no cover — defensive
        log.debug("console staleness probe failed: %s", e)

    # The `is None` arm carries as much weight as the equality: with no
    # daemon reachable there is no comparison to make, and a console that
    # cannot check must not claim it is stale. Drop it and an unreachable
    # daemon renders a red banner reading "the daemon reports None".
    if daemon_version is None or daemon_version == __version__:
        # Versions agree (or cannot be compared) -- but the files this
        # process is running from may still have been replaced underneath
        # it. That is not hypothetical: on 2026-08-05 a checkout move
        # deleted a running console's source while both sides still said
        # 0.24.0, and no version comparison could have noticed.
        drifted = _source_drift_reason()
        if drifted is not None:
            return ConsoleStaleness(
                stale=True,
                console_version=__version__,
                daemon_version=daemon_version,
                reason=drifted,
            )
        return ConsoleStaleness(
            stale=False,
            console_version=__version__,
            daemon_version=daemon_version,
            reason=None,
        )

    return ConsoleStaleness(
        stale=True,
        console_version=__version__,
        daemon_version=daemon_version,
        reason=(
            f"This console is running vq {__version__} from "
            f"{sys.executable}, but the vq daemon on this host reports "
            f"{daemon_version}. The console was not restarted after an "
            f"update, so every page it serves may be missing fixes that "
            f"are already installed. Restart the console service."
        ),
    )
