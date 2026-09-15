"""vq's user-facing narration channel.

**Why this exists.** vq had exactly two ways to say anything: ``click.echo``
(241 call sites, all inside ``cli.py``, all after-the-fact) and ``logging``
(which on the CLI path installs a *file-only* handler, so ``VQ_LOG_LEVEL=DEBUG``
produced terminal silence). Nothing in between. The consequence, from the
2026-07-22 fleet update: a two-hour ``vq admin update`` printed nothing at all
until it finished, and against a remote driver it printed nothing even then
until the SSH call returned, because ``_delegate_to_remote`` buffers the whole
remote stdout. An agent chat driving a fleet update could not tell a working
build from a wedged one.

This module is the missing layer: a **narration channel** that non-CLI modules
can write progress to without importing click, without a file handle threaded
through their call chain, and without deciding for themselves whether anyone is
listening.

**The contract.**

* :func:`narrate` is the only way to emit progress from a non-CLI module. It is
  a deliberate no-op when no channel is installed, so a writer stays callable
  from a test, a library caller, or the daemon.
* The CLI installs a channel for the duration of a verb via :func:`channel`.
* Every narration line is *also* written to the active :class:`RunLog` when one
  is open, so the full narrative survives on disk even when nobody was watching
  the terminal.
* Verbosity is a band on the line, not a decision at the call site. The channel
  filters.

This is intentionally NOT a port of vibe-qc's ``vibeqc.output``. vq depends on
click and pydantic only; it must never import vibeqc (a queue manager runs on
hosts where vibe-qc is not installed, including the driver Mac and cluster login
nodes). The two packages share the *idea* — one place decides how output is
shaped — not the code.

**Scope, stated honestly.** The existing 241 ``click.echo`` result-rendering
call sites are NOT migrated here, and this module does not try to be a document
/ table / quantity layer. Those are a separate proposal (a repo-wide reshaping
of every verb's output is a grand refactor, and CLAUDE.md § 9 says to surface
that before starting it). What this owns today is *progress narration* and the
*run log* — the two things whose absence made the fleet incident opaque.
"""
from __future__ import annotations

import contextlib
import logging
import os
import sys
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from enum import IntEnum
from pathlib import Path
from typing import TextIO

log = logging.getLogger(__name__)

ENV_OUTPUT_LEVEL = "VQ_OUTPUT_LEVEL"
"""Override the narration verbosity band. Accepts QUIET / NORMAL / VERBOSE /
DEBUG (case-insensitive). Invalid values fall back to NORMAL rather than
raising: a typo in an env var must not break a fleet update."""


class Level(IntEnum):
    """Verbosity band for one narration line.

    Ordered so a channel keeps every line at or below its own threshold.
    """

    QUIET = 0
    """Milestones only: a phase started, a phase finished, the verdict."""

    NORMAL = 1
    """The default. Progress an operator wants without asking."""

    VERBOSE = 2
    """Detail an operator asks for when something looks wrong: resolved paths,
    argv, per-attempt retry lines."""

    DEBUG = 3
    """Detail only a developer wants. Never on by default."""


_LEVEL_NAMES = {level.name: level for level in Level}


def resolve_level(explicit: str | None = None) -> Level:
    """Resolve the active band from an explicit value or the environment."""
    raw = (explicit or os.environ.get(ENV_OUTPUT_LEVEL, "")).strip().upper()
    return _LEVEL_NAMES.get(raw, Level.NORMAL)


class RunLog:
    """An append-only transcript of one long-running operation.

    The fleet incident's most expensive gap: the full output of a build was
    accumulated in memory, truncated to its last 80 lines into
    ``admin-status.json``, and the rest discarded. A 30-minute libint rebuild
    left ~6 KB of evidence. This keeps the whole thing on disk next to the
    other state, so ``vq admin logs`` can hand it back afterwards.

    Best-effort by construction: if the file cannot be written, the operation
    continues and the failure is logged once. A deploy must never fail because
    its transcript could not be opened.
    """

    def __init__(self, path: Path, *, header: str = "") -> None:
        self.path = path
        self._fh: TextIO | None = None
        self._broken = False
        self.available = False
        """True only when the transcript was actually opened. Callers must not
        report a ``run_log_path`` for a transcript that does not exist — on a
        multi-user host the log dir can be root-owned while the update runs as
        a user, and pointing an operator at a file that was never written is
        worse than admitting there is none."""
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = path.open("a", encoding="utf-8")
            self.available = True
        except OSError as e:
            self._broken = True
            log.warning("could not open run log %s: %s", path, e)
            return
        if header:
            self.write(header)

    def write(self, text: str) -> None:
        """Append ``text`` (a line, newline optional) to the transcript."""
        if self._fh is None:
            return
        try:
            self._fh.write(text if text.endswith("\n") else text + "\n")
            self._fh.flush()
        except OSError as e:
            if not self._broken:
                self._broken = True
                log.warning("run log %s became unwritable: %s", self.path, e)

    def stamp(self, text: str) -> None:
        """Append ``text`` prefixed with a UTC timestamp.

        Timestamps are what turn a transcript into a timeline: "the build ran
        for 94 minutes and then the verify failed in 3 seconds" is a different
        diagnosis from "the whole thing failed in 4 seconds".
        """
        self.write(f"{datetime.now(UTC).isoformat(timespec='seconds')}  {text}")

    def close(self) -> None:
        if self._fh is not None:
            with contextlib.suppress(OSError):
                self._fh.close()
            self._fh = None


class Channel:
    """Where narration goes for the duration of one operation."""

    def __init__(
        self,
        *,
        level: Level = Level.NORMAL,
        sink: Callable[[str], None] | None = None,
        run_log: RunLog | None = None,
    ) -> None:
        self.level = level
        self.run_log = run_log
        # Terminal output is OPT-IN, and the opt-in belongs to the CLI layer.
        # A library module must not decide that something should appear on a
        # user's screen: admin.py is also called by the daemon, by tests, and
        # (via the RPC path) by processes with no terminal at all. Without an
        # explicit sink, narration goes to the run log and nowhere else.
        if sink is not None:
            self._sink: Callable[[str], None] = sink
        elif _terminal_enabled:
            self._sink = _stderr_sink
        else:
            self._sink = _null_sink

    def emit(self, text: str, level: Level) -> None:
        # The run log records everything up to VERBOSE regardless of the
        # terminal band: the transcript's whole job is to be readable after the
        # fact by someone who was not watching and cannot re-run the operation.
        if self.run_log is not None and level <= Level.VERBOSE:
            self.run_log.stamp(text)
        if level <= self.level:
            self._sink(text)


def _null_sink(text: str) -> None:
    """Narration with no terminal attached still reaches the run log."""


def _stderr_sink(text: str) -> None:
    # stderr, not stdout: narration must never contaminate the machine-readable
    # stdout of a `--json` verb, the bare jobid `vq submit` prints, or the tar
    # bytes `vq tar-workspace` streams.
    try:
        sys.stderr.write(text + "\n")
        sys.stderr.flush()
    except (OSError, ValueError):  # closed stream in a detached daemon
        pass


_terminal_enabled = False


def enable_terminal_narration(enabled: bool = True) -> None:
    """Let narration reach stderr for the rest of this process.

    Called by the CLI for the verbs where live progress is the point — today
    the ``vq admin update`` family, whose long silences are what this module
    exists to end. Deliberately process-global and CLI-owned: see
    :class:`Channel` for why a library module must not make this call.
    """
    global _terminal_enabled
    _terminal_enabled = enabled


_active: Channel | None = None


def active_channel() -> Channel | None:
    """The installed channel, or None when nobody is listening."""
    return _active


@contextlib.contextmanager
def channel(
    *,
    level: Level | None = None,
    sink: Callable[[str], None] | None = None,
    run_log: RunLog | None = None,
) -> Iterator[Channel]:
    """Install a narration channel for the duration of the block.

    Nested installs replace and then restore, so a verb that calls into another
    verb's helper does not lose its own channel.
    """
    global _active
    previous = _active
    _active = Channel(
        level=level if level is not None else resolve_level(),
        sink=sink,
        run_log=run_log,
    )
    try:
        yield _active
    finally:
        _active = previous


def narrate(text: str, level: Level = Level.NORMAL) -> None:
    """Emit one progress line, if anyone is listening.

    A no-op without an installed channel — that is the point. A module can
    narrate freely without knowing whether it is running under a CLI verb, the
    daemon, a test, or an import in a notebook.
    """
    ch = _active
    if ch is not None:
        ch.emit(text, level)


def run_log_write(text: str) -> None:
    """Append raw text to the active run log only (never the terminal).

    For bulk output — a build's stdout — which belongs in the transcript but
    would drown the terminal.
    """
    ch = _active
    if ch is not None and ch.run_log is not None:
        ch.run_log.write(text.rstrip("\n"))
