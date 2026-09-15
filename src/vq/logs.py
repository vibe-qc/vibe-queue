"""v0.6.50: `vq logs JOBID` — show stdout / stderr without typing the
full workspace path.

The QoL companion to ``vq status``. ``vq status`` already shows the
tail of stdout + stderr inline with the spec, but it's noisy for the
"just give me the output" case — and there's no ``--follow``. This
module owns the focused logs view:

* ``show_logs(host, jobid, *, tail, stream, multi_user) -> str`` —
  banner-separated text tail of stdout / stderr / both.
* ``show_logs_json(...)`` — same data as a structured JSON payload
  with the resolved paths included.
* ``follow_logs(...)`` — generator that yields the initial tail
  plus new bytes as the files grow; stops when the spec hits a
  terminal state AND no new bytes for a few consecutive ticks.

``vq output JOBID`` (v0.24) — tail the vibe-qc ``.out`` file:

* ``show_output(host, jobid, *, tail) -> str`` — last N lines of
  the calculation ``.out``.
* ``follow_output(host, jobid, ...)`` — stream the ``.out`` live.

Multi-user aware via :func:`paths.resolve_spec_path`. The
file-reading side is identical to single-user: workspace lives at
``Path(spec.cwd)`` regardless of which user submitted; the
multi-user split is purely about where the *spec* lives in the
per-user state tree.

``_tail_file`` is the shared tail helper; ``status.py`` calls in
here so both modules render the same tail format ("(no output)" /
"(empty)" / "... (N earlier lines)\\n<last-N>").
"""
from __future__ import annotations

import codecs
import datetime as dt
import json
import time
import tomllib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from vq import config, paths
from vq.host import is_local_host
from vq.listing import queue_handle_for_spec
from vq.scheduler_dispatch import (
    SchedulerDispatcher,
    SchedulerError,
    SchedulerHandle,
    scheduler_dispatcher_for,
    scheduler_handle_for_spec,
)
from vq.spec import JobSpec
from vq.spec_access import resolve_authorized_spec, stamp_terminal_status_read

Stream = Literal["both", "stdout", "stderr"]


@dataclass(frozen=True)
class _SchedulerWorkspaceSource:
    dispatcher: SchedulerDispatcher
    handle: SchedulerHandle


# ---------------------------------------------------------------------------
# Shared tail helper (used by both vq logs and vq status / vq status --json).
# ---------------------------------------------------------------------------


def tail_file(path: Path, n: int | None) -> str:
    """Return the last ``n`` lines of ``path`` (all if ``n is None``).

    Three sentinel returns surface in user-facing output:

    * ``"(no output)"`` — the file doesn't exist (job hasn't started
      writing yet, or never started).
    * ``"(empty)"`` — the file exists but is zero-byte.
    * ``"... (K earlier lines)\\n<last-n>"`` — truncated; the hint
      tells the operator how many lines were skipped so the
      "is this everything?" question has an answer.

    No external deps; reads the whole file rather than seeking from
    the end. Log files are small (job stdout, typically < 1 MB) and
    the seek-and-rewind path is fiddly to get right across line
    endings + encoding errors; the simpler full-read is honest about
    the cost.
    """
    if not path.exists():
        return "(no output)"
    text = path.read_text(errors="replace")
    return _tail_text_snapshot(text, n)


def _tail_text_snapshot(text: str, n: int | None) -> str:
    """Tail already-read text with the same sentinels as :func:`tail_file`."""
    if not text:
        return "(empty)"
    if n is None:
        return text.rstrip("\n")
    lines = text.splitlines()
    if len(lines) <= n:
        return text.rstrip("\n")
    skipped = len(lines) - n
    return f"... ({skipped} earlier lines)\n" + "\n".join(lines[-n:])


def _tail_follow_snapshot(text: str, n: int | None) -> str:
    """Select an initial follow tail without changing its line framing."""
    if not text:
        return "(empty)\n"
    if n is None:
        return text
    lines = text.splitlines(keepends=True)
    if len(lines) <= n:
        return text
    skipped = len(lines) - n
    return f"... ({skipped} earlier lines)\n{''.join(lines[-n:])}"


def _read_follow_file_snapshot(
    path: Path,
    n: int | None,
) -> tuple[str, int]:
    """Read one follow snapshot and bind its cursor to those exact bytes."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return "(no output)\n", 0
    return (
        _tail_follow_snapshot(data.decode("utf-8", errors="replace"), n),
        len(data),
    )


def _read_file_since(path: Path, cursor: int) -> tuple[bytes, int, bool]:
    """Read bytes appended after ``cursor`` and recover from truncation."""
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return b"", cursor, False
    truncated = size < cursor
    start = 0 if truncated else cursor
    if size == start:
        return b"", start, truncated
    with path.open("rb") as stream:
        stream.seek(start)
        data = stream.read()
    return data, start + len(data), truncated


# ---------------------------------------------------------------------------
# Spec-resolution helper shared by all three public entry points.
# ---------------------------------------------------------------------------


def _resolve_spec(
    jobid: str,
    *,
    multi_user: bool,
) -> tuple[Path, JobSpec]:
    """Find ``jobid``'s spec on disk and load it.

    Returns ``(spec_path, spec)`` so the caller can re-write the
    spec (e.g. to stamp ``last_status_at``) without re-resolving.
    Raises :class:`FileNotFoundError` with a clear message — same
    behaviour as :func:`status.show_status`.
    """
    # Resolve the single-user default here to preserve the long-standing
    # ``vq.logs.paths.queue_dir`` injection seam used by callers and tests.
    queue_dir = None if multi_user else paths.queue_dir()
    return resolve_authorized_spec(
        jobid,
        queue_dir=queue_dir,
        multi_user=multi_user,
    )


def _archived_note() -> str:
    return "(archived; vq cleanup --restore <jobid> to un-tar)"


def _log_paths(spec: JobSpec) -> tuple[Path, Path]:
    """Resolve the on-disk stdout / stderr paths for a spec.

    Spec stores ``stdout_path`` / ``stderr_path`` as either an
    absolute path or a path relative to the job's workspace (cwd).
    Mirrors the daemon's resolution in ``_start_job``.
    """
    workspace = Path(spec.cwd)
    return (
        workspace / spec.stdout_path,
        workspace / spec.stderr_path,
    )


def _scheduler_workspace_source(
    spec: JobSpec,
    cfg: config.Config | None,
) -> _SchedulerWorkspaceSource | None:
    """Live scheduler workspace source for a non-terminal cluster job."""
    if (
        spec.is_terminal
        or spec.scheduler_target is None
        or spec.scheduler_job_id is None
    ):
        return None
    cfg = cfg or config.load_config()
    try:
        host_cfg = cfg.host(spec.scheduler_target)
    except config.ConfigError as exc:
        raise FileNotFoundError(
            f"scheduler host {spec.scheduler_target!r} is not configured: {exc}"
        ) from exc
    dispatcher = scheduler_dispatcher_for(host_cfg)
    return _SchedulerWorkspaceSource(
        dispatcher=dispatcher,
        handle=scheduler_handle_for_spec(
            dispatcher,
            spec,
            job_id=spec.scheduler_job_id,
        ),
    )


def _scheduler_calculation_source(
    spec: JobSpec,
    cfg: config.Config | None,
) -> _SchedulerWorkspaceSource | None:
    """Opt-in live scheduler source for calculation-artifact APIs."""
    if cfg is None:
        return None
    return _scheduler_workspace_source(spec, cfg)


def _scheduler_log_paths(source: _SchedulerWorkspaceSource) -> tuple[str, str]:
    return (
        f"{source.handle.remote_workspace}/stdout.log",
        f"{source.handle.remote_workspace}/stderr.log",
    )


def _scheduler_tail(
    source: _SchedulerWorkspaceSource,
    *,
    stream: Literal["stdout", "stderr"],
    tail: int | None,
) -> str:
    return source.dispatcher.tail_log(source.handle, lines=tail, stream=stream)


def _display_log_text(text: str) -> str:
    return text.rstrip("\n") if text else "(no output)"


def _queue_handle_payload(spec: JobSpec, host: str) -> dict[str, str | None]:
    """Stable back-reference for cockpit clients and result metadata."""
    return queue_handle_for_spec(spec, host)


def _render_log_output(
    *,
    stream: Stream,
    stdout_text: str,
    stderr_text: str,
) -> str:
    if stream == "stdout":
        return stdout_text
    if stream == "stderr":
        return stderr_text
    return (
        "--- stdout ---\n"
        f"{stdout_text}\n"
        "\n"
        "--- stderr ---\n"
        f"{stderr_text}"
    )


def _render_follow_log_output(
    *,
    stream: Stream,
    stdout_text: str,
    stderr_text: str,
) -> str:
    """Render an initial follow snapshot without stripping stream framing."""
    if stream == "stdout":
        return stdout_text
    if stream == "stderr":
        return stderr_text
    stdout_separator = "" if stdout_text.endswith("\n") else "\n"
    return (
        "--- stdout ---\n"
        f"{stdout_text}{stdout_separator}\n"
        "--- stderr ---\n"
        f"{stderr_text}"
    )


# ---------------------------------------------------------------------------
# Text rendering
# ---------------------------------------------------------------------------


def show_logs(
    host: str,
    jobid: str,
    *,
    tail: int | None = 100,
    stream: Stream = "both",
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """Render the requested stream(s) for ``jobid`` as text.

    Default tail of 100 (deliberately larger than ``vq status``'s 50
    — operators reaching for ``vq logs`` want output, not the spec).
    ``tail=None`` means "show everything." Banner ``--- stdout ---``
    / ``--- stderr ---`` separators only appear when both streams
    are requested; single-stream output is the raw tail with no
    banner (so ``vq logs JOB --stderr | grep ...`` doesn't pick up
    the banner line).

    Stamps ``last_status_at`` on terminal specs as a side effect, so
    the v0.5.10 auto-cleanup "skip recently-looked-at jobs" gate
    sees the read.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote logs for {host!r} should be delegated by the CLI layer"
        )
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    stdout_path, stderr_path = _log_paths(spec)
    scheduler_source = _scheduler_workspace_source(spec, cfg)

    if spec.is_archived:
        stdout_text = stderr_text = _archived_note()
    elif scheduler_source is not None:
        stdout_text = (
            _display_log_text(
                _scheduler_tail(scheduler_source, stream="stdout", tail=tail)
            )
            if stream in ("both", "stdout")
            else ""
        )
        stderr_text = (
            _display_log_text(
                _scheduler_tail(scheduler_source, stream="stderr", tail=tail)
            )
            if stream in ("both", "stderr")
            else ""
        )
    else:
        stdout_text = (
            tail_file(stdout_path, tail) if stream in ("both", "stdout") else ""
        )
        stderr_text = (
            tail_file(stderr_path, tail) if stream in ("both", "stderr") else ""
        )

    out = _render_log_output(
        stream=stream,
        stdout_text=stdout_text,
        stderr_text=stderr_text,
    )

    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)

    return out


def show_logs_json(
    host: str,
    jobid: str,
    *,
    tail: int | None = 100,
    stream: Stream = "both",
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """Same data as :func:`show_logs`, structured for machine consumers.

    Shape:

    .. code-block:: json

       {
         "jobid": "...",
         "state": "RUNNING",
         "stream": "both",
         "tail": 100,
         "stdout_path": "/abs/path/stdout.log",
         "stderr_path": "/abs/path/stderr.log",
         "stdout": "...",
         "stderr": "..."
       }

    ``stdout`` / ``stderr`` keys are present only when the matching
    stream was requested. ``state`` lets scripted callers know
    whether the job is still RUNNING (and ``vq logs --follow`` would
    have made sense) without a second ``vq status --json`` roundtrip.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote logs for {host!r} should be delegated by the CLI layer"
        )
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    stdout_path, stderr_path = _log_paths(spec)
    scheduler_source = _scheduler_workspace_source(spec, cfg)
    scheduler_stdout_path: str | None = None
    scheduler_stderr_path: str | None = None
    if scheduler_source is not None:
        scheduler_stdout_path, scheduler_stderr_path = _scheduler_log_paths(
            scheduler_source
        )

    payload: dict[str, object] = {
        "jobid": spec.id,
        "state": spec.state.value,
        "stream": stream,
        "tail": tail,
        "queue_handle": _queue_handle_payload(spec, host),
    }
    if scheduler_source is not None:
        payload["scheduler_target"] = spec.scheduler_target
        payload["scheduler_job_id"] = spec.scheduler_job_id
    if stream in ("both", "stdout"):
        if scheduler_source is not None:
            assert scheduler_stdout_path is not None
            payload["stdout_path"] = scheduler_stdout_path
            payload["stdout"] = _display_log_text(
                _scheduler_tail(scheduler_source, stream="stdout", tail=tail)
            )
        else:
            payload["stdout_path"] = str(stdout_path)
            payload["stdout"] = (
                _archived_note() if spec.is_archived
                else tail_file(stdout_path, tail)
            )
    if stream in ("both", "stderr"):
        if scheduler_source is not None:
            assert scheduler_stderr_path is not None
            payload["stderr_path"] = scheduler_stderr_path
            payload["stderr"] = _display_log_text(
                _scheduler_tail(scheduler_source, stream="stderr", tail=tail)
            )
        else:
            payload["stderr_path"] = str(stderr_path)
            payload["stderr"] = (
                _archived_note() if spec.is_archived
                else tail_file(stderr_path, tail)
            )

    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)

    return json.dumps(payload, indent=2, sort_keys=True, default=str)


# ---------------------------------------------------------------------------
# --follow: incremental tail until terminal-and-idle
# ---------------------------------------------------------------------------


def follow_logs(
    host: str,
    jobid: str,
    *,
    stream: Stream = "both",
    multi_user: bool = False,
    initial_tail: int | None = 20,
    poll_interval: float = 0.5,
    idle_ticks_after_terminal: int = 2,
    sleep: Callable[[float], None] | None = None,
    now: Callable[[], float] | None = None,
    cfg: config.Config | None = None,
) -> Iterator[str]:
    """Yield text chunks (initial tail + new bytes) until terminal-idle.

    Generator shape so the CLI layer can stream each chunk to stdout
    immediately and tests can drive the loop without real sleeps.

    Termination: the spec is re-read each tick; once it hits a
    terminal state (COMPLETED/FAILED/...) AND no new bytes arrived
    for ``idle_ticks_after_terminal`` consecutive polls, the
    generator returns. The idle wait lets a job that finished
    *just* before we noticed flush its last buffered output to disk
    before we bail.

    Initial chunk is the most recent ``initial_tail`` lines per
    requested stream, or all existing content when ``initial_tail`` is
    ``None``, banner-separated for ``stream="both"``. Each subsequent
    chunk is exactly the bytes that arrived since the last tick, in
    stream order — no banner re-print (so the output reads as one
    continuous stream once tailing starts). When both streams are tailed,
    a stream-prefix is added on lines from stderr (``[stderr]``) so the
    operator can tell which file contributed; pure stdout-only / stderr-only
    modes leave the bytes unmodified.

    ``sleep`` / ``now`` are injectable so tests can drive the loop
    deterministically without wall-clock waits.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote follow for {host!r} should be delegated by the CLI layer"
        )
    sleep_fn = sleep if sleep is not None else time.sleep
    _ = now  # reserved for future deadline support; unused for now

    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    stdout_path, stderr_path = _log_paths(spec)

    if spec.is_archived:
        yield _archived_note()
        return
    scheduler_source = _scheduler_workspace_source(spec, cfg)
    if scheduler_source is not None:
        yield from _follow_scheduler_logs(
            spec_path,
            scheduler_source,
            stream=stream,
            initial_tail=initial_tail,
            poll_interval=poll_interval,
            idle_ticks_after_terminal=idle_ticks_after_terminal,
            sleep=sleep_fn,
        )
        return

    # Bind each cursor to the exact bytes used for its displayed snapshot.
    # Reading first and then calling stat can skip bytes appended in the gap.
    stdout_initial, stdout_cursor = (
        _read_follow_file_snapshot(stdout_path, initial_tail)
        if stream in ("both", "stdout")
        else ("", 0)
    )
    stderr_initial, stderr_cursor = (
        _read_follow_file_snapshot(stderr_path, initial_tail)
        if stream in ("both", "stderr")
        else ("", 0)
    )
    initial = _render_follow_log_output(
        stream=stream,
        stdout_text=stdout_initial,
        stderr_text=stderr_initial,
    )
    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)
    if initial:
        yield initial

    cursors = {
        "stdout": stdout_cursor,
        "stderr": stderr_cursor,
    }

    def _decode(data: bytes, label: str | None) -> str:
        text = data.decode("utf-8", errors="replace")
        if label is None:
            return text
        # Prefix each newline-terminated line with the label so a
        # mixed-stream tail tells the operator which file the line
        # came from. Trailing partial line (no \n) gets the label too.
        if not text:
            return text
        lines = text.split("\n")
        # split() puts a trailing "" if text ends with \n; preserve
        # that so we don't drop the final newline.
        out = []
        for i, line in enumerate(lines):
            if i == len(lines) - 1 and line == "":
                out.append("")
                continue
            out.append(f"[{label}] {line}")
        return "\n".join(out)

    idle_after_terminal = 0
    while True:
        chunks: list[str] = []
        if stream in ("both", "stdout"):
            data, cursors["stdout"], _ = _read_file_since(
                stdout_path,
                cursors["stdout"],
            )
            if data:
                chunks.append(_decode(data, None if stream == "stdout" else "stdout"))
        if stream in ("both", "stderr"):
            data, cursors["stderr"], _ = _read_file_since(
                stderr_path,
                cursors["stderr"],
            )
            if data:
                chunks.append(_decode(data, None if stream == "stderr" else "stderr"))

        any_bytes = bool(chunks)
        if any_bytes:
            yield "".join(chunks)

        # Re-read the spec; if it's terminal AND we've seen no new
        # bytes for the idle window, we're done.
        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            # Spec was deleted out from under us (vq cleanup --delete
            # ran mid-follow); bail rather than spin.
            return

        if spec.is_terminal:
            if any_bytes:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    return

        sleep_fn(poll_interval)


def _follow_scheduler_logs(
    spec_path: Path,
    source: _SchedulerWorkspaceSource,
    *,
    stream: Stream,
    initial_tail: int | None,
    poll_interval: float,
    idle_ticks_after_terminal: int,
    sleep: Callable[[float], None],
) -> Iterator[str]:
    """Poll scheduler-side logs until the driver spec reaches terminal+idle."""
    stdout_text = (
        _scheduler_tail(source, stream="stdout", tail=initial_tail)
        if stream in ("both", "stdout")
        else ""
    )
    stderr_text = (
        _scheduler_tail(source, stream="stderr", tail=initial_tail)
        if stream in ("both", "stderr")
        else ""
    )
    yield _render_follow_log_output(
        stream=stream,
        stdout_text=stdout_text or "(no output)\n",
        stderr_text=stderr_text or "(no output)\n",
    )

    stdout_full = (
        (
            stdout_text
            if initial_tail is None
            else _scheduler_tail(source, stream="stdout", tail=None)
        )
        if stream in ("both", "stdout")
        else ""
    )
    stderr_full = (
        (
            stderr_text
            if initial_tail is None
            else _scheduler_tail(source, stream="stderr", tail=None)
        )
        if stream in ("both", "stderr")
        else ""
    )
    idle_after_terminal = 0

    while True:
        chunks: list[str] = []
        if stream in ("both", "stdout"):
            latest = _scheduler_tail(source, stream="stdout", tail=None)
            delta = _tail_delta(stdout_full, latest)
            stdout_full = latest
            if delta:
                chunks.append(
                    _decode_text(delta, None if stream == "stdout" else "stdout")
                )
        if stream in ("both", "stderr"):
            latest = _scheduler_tail(source, stream="stderr", tail=None)
            delta = _tail_delta(stderr_full, latest)
            stderr_full = latest
            if delta:
                chunks.append(
                    _decode_text(delta, None if stream == "stderr" else "stderr")
                )

        any_text = bool(chunks)
        if any_text:
            yield "".join(chunks)

        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return
        if spec.is_terminal:
            if any_text:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    return
        sleep(poll_interval)


def _tail_delta(previous: str, latest: str) -> str:
    """Best-effort increment from repeated scheduler ``tail`` snapshots."""
    if not latest:
        return ""
    if latest.startswith(previous):
        return latest[len(previous):]
    if previous and latest == previous:
        return ""
    return latest


def _decode_text(text: str, label: str | None) -> str:
    if label is None or not text:
        return text
    lines = text.split("\n")
    out = []
    for i, line in enumerate(lines):
        if i == len(lines) - 1 and line == "":
            out.append("")
            continue
        out.append(f"[{label}] {line}")
    return "\n".join(out)


# ----------------------------------------------------------------------
# v0.12.1: `vq events JOBID` — the per-job timeline.
#
# The daemon writes a structured event per lifecycle transition to
# `<workspace>/_vq/events.jsonl` (submitted, dispatched, every state change,
# kill requests, watchdog kills, and — since the scheduler-qdel fix — cluster
# cancellations). Until now `vq status` pointed the user at that file path and
# nothing read it: the answer to "what actually happened to my job, and when?"
# lived in a file an agent had to ssh in and cat. This surfaces it as a verb,
# routed exactly like `vq logs` (a scheduler job's events live on the driver,
# where the workspace is, so the same host resolution applies).
# ----------------------------------------------------------------------


def _format_event(record: dict[str, object]) -> str:
    """One human-readable line for a single event record.

    ``ts  KIND  <salient fields>`` — the kind-specific payload is rendered
    compactly so a state transition reads ``running -> completed (exit 0)``
    rather than a raw JSON dump.
    """
    ts = str(record.get("ts", "?"))
    kind = str(record.get("kind", "?"))
    detail = ""
    if kind == "state_transition":
        frm = record.get("from", "?")
        to = record.get("to", "?")
        detail = f"{frm} -> {to}"
        exit_code = record.get("exit_code")
        if exit_code is not None:
            detail += f" (exit {exit_code})"
        reason = record.get("reason")
        if reason:
            detail += f": {reason}"
    elif kind in ("kill_requested", "watchdog_kill"):
        bits = [str(record[k]) for k in ("via", "reason") if record.get(k)]
        detail = "; ".join(bits)
    elif kind == "dispatched":
        bits = [
            f"{k}={record[k]}"
            for k in ("pid", "pgid", "scheduler_job_id", "host")
            if record.get(k) is not None
        ]
        detail = " ".join(bits)
    else:
        # Any kind we do not special-case (incl. future ones): show the
        # non-boilerplate keys so nothing is silently dropped.
        skip = {"ts", "kind", "jobid"}
        detail = " ".join(
            f"{k}={v}" for k, v in record.items() if k not in skip
        )
    return f"{ts}  {kind:<17}{('  ' + detail) if detail else ''}"


def show_events(
    host: str,
    jobid: str,
    *,
    multi_user: bool = False,
) -> str:
    """Render ``jobid``'s event timeline as text (one line per event)."""
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote events for {host!r} should be delegated by the CLI layer"
        )
    from vq.events import read_events  # noqa: PLC0415 — keep events.py light

    _spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    events = read_events(Path(spec.cwd))
    if not events:
        return (
            f"no events recorded for {jobid} "
            "(pre-v0.6.54 job, or the workspace was cleaned up)"
        )
    return "\n".join(_format_event(rec) for rec in events)


def show_events_json(
    host: str,
    jobid: str,
    *,
    multi_user: bool = False,
) -> str:
    """Emit ``jobid``'s event timeline as a JSON object."""
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote events for {host!r} should be delegated by the CLI layer"
        )
    from vq.events import read_events  # noqa: PLC0415

    _spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    return json.dumps(
        {"jobid": jobid, "events": read_events(Path(spec.cwd))},
        indent=2,
        sort_keys=True,
    )


# ----------------------------------------------------------------------
# v0.24: `vq output JOBID` — tail the vibe-qc calculation .out file.
#
# The .out file is written line-buffered by vibe-qc's OutputChannel
# and carries the full SCF trace, properties, and (at VERBOSE/DEBUG)
# C++ diagnostics.  Unlike stdout/stderr (which carry process-level
# output and the ProgressLogger live feed), the .out is the canonical
# calculation record.  This command streams it to the terminal.
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class ArtifactPaths:
    """One workspace-relative vibe-qc output family.

    ``relative_stem`` may include a safe subdirectory learned from
    ``JobSpec.expected_outputs``. ``system_relative_path`` is kept separate
    because a manifest's filename and its ``[run].basename`` need not match.
    Scheduler readers use the relative properties; local readers use the
    workspace-contained absolute properties.
    """

    workspace: Path
    relative_stem: Path
    system_relative_path: Path
    structured_relative_path: Path | None = None

    def relative_path(self, suffix: str) -> Path:
        return self.relative_stem.parent / f"{self.relative_stem.name}{suffix}"

    @property
    def out_relative_path(self) -> Path:
        return self.relative_path(".out")

    @property
    def progress_relative_path(self) -> Path:
        return self.structured_relative_path or self.relative_path(".scf.jsonl")

    @property
    def out_path(self) -> Path:
        return self.workspace / self.out_relative_path

    @property
    def progress_path(self) -> Path:
        return self.workspace / self.progress_relative_path

    @property
    def system_path(self) -> Path:
        return self.workspace / self.system_relative_path


def _safe_relative_path(value: str) -> Path | None:
    """Return a lexical workspace-relative path, or ``None`` if unsafe."""
    if not value or "\x00" in value:
        return None
    candidate = Path(value)
    if candidate.is_absolute() or candidate == Path("."):
        return None
    if any(part == ".." for part in candidate.parts):
        return None
    return candidate


def _relative_artifact_path(stem: Path, suffix: str) -> Path:
    return stem.parent / f"{stem.name}{suffix}"


def _contained_relative_path(workspace: Path, path: Path) -> Path | None:
    """Return ``path`` relative to workspace, rejecting symlink escapes."""
    try:
        path.resolve().relative_to(workspace.resolve())
        return path.relative_to(workspace)
    except (OSError, RuntimeError, ValueError):
        return None


def _declared_structured_path(
    declared: list[Path],
    relative_stem: Path,
) -> Path | None:
    """Select an unambiguous declared ``.scf.jsonl`` path."""
    candidates = [
        path for path in declared if path.name.endswith(".scf.jsonl")
    ]
    sibling = _relative_artifact_path(relative_stem, ".scf.jsonl")
    if sibling in candidates:
        return sibling
    return candidates[0] if len(candidates) == 1 else None


def _declared_absolute_suffix(
    path: PurePosixPath,
    declared: list[Path],
) -> Path | None:
    """Map a fetched remote absolute path back to declared metadata.

    Scheduler manifests can retain their remote absolute paths after the
    workspace is fetched. Only remap such a path to an already-safe
    ``JobSpec.expected_outputs`` entry whose complete component sequence is
    the path suffix. Prefer the most specific match if declarations overlap.
    """
    if "\x00" in path.as_posix() or ".." in path.parts:
        return None
    matches = {
        candidate
        for candidate in declared
        if len(candidate.parts) <= len(path.parts)
        and path.parts[-len(candidate.parts):] == candidate.parts
    }
    if not matches:
        return None
    deepest = max(len(candidate.parts) for candidate in matches)
    specific = [
        candidate
        for candidate in matches
        if len(candidate.parts) == deepest
    ]
    return specific[0] if len(specific) == 1 else None


def _fetched_scheduler_suffix(
    workspace: Path,
    path: PurePosixPath,
    job_id: str,
) -> Path | None:
    """Map a terminal scheduler path to an existing fetched file.

    Shared-workspace paths normally contain ``job_id``.  A configured
    ``node_scratch_dir`` instead records an absolute path below a random
    ``vq-XXXXXX`` directory in the fetched manifest.  Prefer the explicit
    job-id boundary when present, then accept the contained suffix below that
    exact generated scratch-directory shape.  This helper is called only for
    terminal scheduler specs with durable scheduler identity.
    """
    if "\x00" in path.as_posix() or ".." in path.parts:
        return None
    positions = [
        index for index, part in enumerate(path.parts) if part == job_id
    ]
    for index in reversed(positions):
        if index + 1 >= len(path.parts):
            continue
        relative = _safe_relative_path(
            PurePosixPath(*path.parts[index + 1 :]).as_posix()
        )
        if relative is None:
            continue
        target = workspace / relative
        if (
            target.is_file()
            and _contained_relative_path(workspace, target) is not None
        ):
            return relative

    # Historical/current node-local scratch paths do not contain the vq job
    # ID, but their mktemp component has the exact ``vq-XXXXXX`` shape.
    # Copyback strips that one component and preserves everything below it.
    # Requiring the generated boundary avoids mapping an arbitrary outside
    # path onto a stale same-basename file in the submitted workspace.
    scratch_positions = [
        index
        for index, part in enumerate(path.parts)
        if part.startswith("vq-")
        and len(part) == len("vq-") + 6
        and part[len("vq-") :].isascii()
        and part[len("vq-") :].isalnum()
    ]
    for index in reversed(scratch_positions):
        if index + 1 >= len(path.parts):
            continue
        relative = _safe_relative_path(
            PurePosixPath(*path.parts[index + 1 :]).as_posix()
        )
        if relative is None:
            continue
        target = workspace / relative
        if (
            target.is_file()
            and _contained_relative_path(workspace, target) is not None
        ):
            return relative
    return None


def _manifest_structured_path(
    workspace: Path,
    data: dict[str, object],
    relative_stem: Path,
    declared: list[Path],
    terminal_scheduler_job_id: str | None,
) -> Path | None:
    """Read the structured role from a manifest plan when unambiguous."""
    plan = data.get("plan")
    if not isinstance(plan, dict):
        return None
    rows = plan.get("files")
    if not isinstance(rows, list):
        return None
    candidates: list[Path] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("role") != "structured":
            continue
        raw = row.get("path")
        if not isinstance(raw, str):
            continue
        posix_path = PurePosixPath(raw)
        if posix_path.is_absolute():
            safe = _contained_relative_path(workspace, Path(raw))
            if safe is None and terminal_scheduler_job_id is not None:
                safe = _fetched_scheduler_suffix(
                    workspace,
                    posix_path,
                    terminal_scheduler_job_id,
                ) or _declared_absolute_suffix(posix_path, declared)
        else:
            safe = _safe_relative_path(raw)
        if safe is not None:
            candidates.append(safe)
    sibling = _relative_artifact_path(relative_stem, ".scf.jsonl")
    if sibling in candidates:
        return sibling
    return candidates[0] if len(candidates) == 1 else None


def _manifest_artifact_paths(
    workspace: Path,
    manifest_relative_path: Path,
    declared: list[Path],
    terminal_scheduler_job_id: str | None,
) -> ArtifactPaths | None:
    """Read one manifest without allowing its basename to escape workspace."""
    manifest_path = workspace / manifest_relative_path
    try:
        manifest_path.resolve().relative_to(workspace.resolve())
        data = tomllib.loads(manifest_path.read_text())
    except (OSError, RuntimeError, ValueError, tomllib.TOMLDecodeError):
        return None
    run = data.get("run")
    if not isinstance(run, dict):
        return None
    basename = run.get("basename")
    if not isinstance(basename, str):
        return None
    safe_basename = _safe_relative_path(basename)
    if safe_basename is None or len(safe_basename.parts) != 1:
        return None
    relative_stem = manifest_relative_path.parent / safe_basename
    return ArtifactPaths(
        workspace=workspace,
        relative_stem=relative_stem,
        system_relative_path=manifest_relative_path,
        structured_relative_path=_manifest_structured_path(
            workspace,
            data,
            relative_stem,
            declared,
            terminal_scheduler_job_id,
        ),
    )


def _with_declared_structured_path(
    artifacts: ArtifactPaths,
    declared: list[Path],
) -> ArtifactPaths:
    """Fill a missing manifest structured role from JobSpec metadata."""
    if artifacts.structured_relative_path is not None:
        return artifacts
    structured = _declared_structured_path(declared, artifacts.relative_stem)
    if structured is None:
        return artifacts
    return ArtifactPaths(
        artifacts.workspace,
        artifacts.relative_stem,
        artifacts.system_relative_path,
        structured,
    )


def _structured_path_from_manifest(
    workspace: Path,
    system_relative_path: Path,
    relative_stem: Path,
    declared: list[Path],
    terminal_scheduler_job_id: str | None,
) -> Path | None:
    parsed = _manifest_artifact_paths(
        workspace,
        system_relative_path,
        declared,
        terminal_scheduler_job_id,
    )
    if parsed is None or parsed.relative_stem != relative_stem:
        return None
    return parsed.structured_relative_path


def artifact_paths_for_spec(spec: JobSpec) -> ArtifactPaths:
    """Resolve the declared or discovered vibe-qc artifact family.

    JobSpec metadata is the submit-time contract and therefore wins over
    whatever unrelated manifests happen to share the workspace. Older specs
    fall back to a valid manifest, an existing ``.out`` file, and finally the
    conventional ``output`` family. Every accepted path is lexical-relative;
    manifest ``[run].basename`` values must be a single safe path component.
    """
    workspace = Path(spec.cwd)
    declared = [
        safe
        for value in spec.expected_outputs
        if (safe := _safe_relative_path(value)) is not None
    ]
    terminal_scheduler_job_id = (
        spec.id
        if spec.is_terminal
        and spec.scheduler_target is not None
        and spec.scheduler_job_id is not None
        else None
    )

    declared_outs = [path for path in declared if path.suffix == ".out"]
    if declared_outs:
        chosen = declared_outs[0]
        safe_stem = _safe_relative_path(spec.output_stem or "")
        if safe_stem is not None and len(safe_stem.parts) == 1:
            chosen = next(
                (path for path in declared_outs if path.stem == safe_stem.name),
                chosen,
            )
        relative_stem = chosen.with_suffix("")
        matching_system = next(
            (
                path
                for path in declared
                if path.suffix == ".system"
                and path.with_suffix("") == relative_stem
            ),
            _relative_artifact_path(relative_stem, ".system"),
        )
        structured = _structured_path_from_manifest(
            workspace,
            matching_system,
            relative_stem,
            declared,
            terminal_scheduler_job_id,
        ) or _declared_structured_path(declared, relative_stem)
        return ArtifactPaths(
            workspace,
            relative_stem,
            matching_system,
            structured,
        )

    safe_stem = _safe_relative_path(spec.output_stem or "")
    if safe_stem is not None and len(safe_stem.parts) == 1:
        system_relative_path = _relative_artifact_path(safe_stem, ".system")
        return ArtifactPaths(
            workspace,
            safe_stem,
            system_relative_path,
            _structured_path_from_manifest(
                workspace,
                system_relative_path,
                safe_stem,
                declared,
                terminal_scheduler_job_id,
            ) or _declared_structured_path(declared, safe_stem),
        )

    declared_systems = [path for path in declared if path.suffix == ".system"]
    for system_relative_path in declared_systems:
        parsed = _manifest_artifact_paths(
            workspace,
            system_relative_path,
            declared,
            terminal_scheduler_job_id,
        )
        if parsed is not None:
            return _with_declared_structured_path(parsed, declared)
    if declared_systems:
        system_relative_path = declared_systems[0]
        return ArtifactPaths(
            workspace,
            system_relative_path.with_suffix(""),
            system_relative_path,
            _declared_structured_path(
                declared,
                system_relative_path.with_suffix(""),
            ),
        )

    manifest_candidates: list[Path] = []
    if workspace.is_dir():
        manifest_candidates = sorted(
            relative
            for path in workspace.rglob("*.system")
            if path.is_file()
            if (relative := _contained_relative_path(workspace, path)) is not None
        )
    default_manifest = Path("output.system")
    if default_manifest in manifest_candidates:
        manifest_candidates.remove(default_manifest)
        manifest_candidates.insert(0, default_manifest)
    for system_relative_path in manifest_candidates:
        parsed = _manifest_artifact_paths(
            workspace,
            system_relative_path,
            declared,
            terminal_scheduler_job_id,
        )
        if parsed is not None:
            return _with_declared_structured_path(parsed, declared)

    out_candidates: list[Path] = []
    if workspace.is_dir():
        out_candidates = sorted(
            relative
            for path in workspace.rglob("*.out")
            if path.is_file()
            if (relative := _contained_relative_path(workspace, path)) is not None
        )
    default_out = Path("output.out")
    chosen_out = (
        default_out
        if default_out in out_candidates
        else out_candidates[0]
        if out_candidates
        else default_out
    )
    relative_stem = chosen_out.with_suffix("")
    return ArtifactPaths(
        workspace,
        relative_stem,
        _relative_artifact_path(relative_stem, ".system"),
        _declared_structured_path(declared, relative_stem),
    )


def _out_path(spec: JobSpec) -> Path:
    """Compatibility wrapper for the resolved vibe-qc ``.out`` path."""
    return artifact_paths_for_spec(spec).out_path


def _scheduler_artifact_path(
    source: _SchedulerWorkspaceSource,
    relative_path: Path,
) -> str:
    return f"{source.handle.remote_workspace}/{relative_path.as_posix()}"


def _scheduler_artifact_text(
    source: _SchedulerWorkspaceSource,
    relative_path: Path,
    *,
    lines: int | None,
) -> str:
    return source.dispatcher.tail_file(
        source.handle,
        filename=relative_path.as_posix(),
        lines=lines,
    )


def _scheduler_artifact_snapshot(
    source: _SchedulerWorkspaceSource,
    relative_path: Path,
) -> tuple[bool, str]:
    """Return existence plus a complete scheduler-side file snapshot."""
    snapshot = source.dispatcher.tail_file_since(
        source.handle,
        filename=relative_path.as_posix(),
        byte_offset=0,
    )
    if snapshot is None:
        return False, ""
    return True, snapshot.data.decode("utf-8", errors="replace")


def _scheduler_manifest_relative_path(
    source: _SchedulerWorkspaceSource,
    raw: str,
) -> Path | None:
    """Normalize one manifest path against its live remote workspace."""
    if not raw or "\x00" in raw:
        return None
    candidate = PurePosixPath(raw)
    if candidate.is_absolute():
        try:
            candidate = candidate.relative_to(
                PurePosixPath(source.handle.remote_workspace)
            )
        except ValueError:
            return None
    if candidate == PurePosixPath(".") or ".." in candidate.parts:
        return None
    return Path(*candidate.parts)


def _scheduler_progress_relative_path(
    source: _SchedulerWorkspaceSource,
    artifacts: ArtifactPaths,
) -> Path:
    """Resolve the structured role from the authoritative live manifest."""
    text = _scheduler_artifact_text(
        source,
        artifacts.system_relative_path,
        lines=None,
    )
    if not text:
        return artifacts.progress_relative_path
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return artifacts.progress_relative_path
    plan = data.get("plan")
    rows = plan.get("files") if isinstance(plan, dict) else None
    if not isinstance(rows, list):
        return artifacts.progress_relative_path
    candidates: list[Path] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("role") != "structured":
            continue
        raw = row.get("path")
        if not isinstance(raw, str):
            continue
        relative = _scheduler_manifest_relative_path(source, raw)
        if relative is not None:
            candidates.append(relative)
    if artifacts.progress_relative_path in candidates:
        return artifacts.progress_relative_path
    return (
        candidates[0]
        if len(candidates) == 1
        else artifacts.progress_relative_path
    )


def read_system_progress(
    spec: JobSpec,
    *,
    cfg: config.Config | None = None,
) -> dict[str, object] | None:
    """Read ``[progress]`` from the selected local or scheduler manifest."""
    artifacts = artifact_paths_for_spec(spec)
    # Direct status-library callers historically passed no Config and read the
    # staged local workspace. The CLI supplies its already-loaded Config to
    # opt into the live scheduler source without adding hidden config I/O here.
    try:
        scheduler_source = _scheduler_calculation_source(spec, cfg)
        if scheduler_source is not None:
            text = _scheduler_artifact_text(
                scheduler_source,
                artifacts.system_relative_path,
                lines=None,
            )
            if not text:
                return None
        else:
            text = artifacts.system_path.read_text()
    except (FileNotFoundError, OSError, SchedulerError):
        # Calculation progress is optional status enrichment. A missing
        # scheduler target or transient SSH failure must not hide the spec's
        # otherwise available status; do not fall back to a stale local copy.
        return None
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError:
        return None
    progress = data.get("progress")
    return progress if isinstance(progress, dict) else None


def _parse_iso_utc(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _output_header(spec: JobSpec, path: Path) -> str:
    state = spec.state.value if spec.state else "unknown"
    started = _parse_iso_utc(spec.started_at)
    finished = _parse_iso_utc(spec.finished_at)
    if started is None:
        return f"[{state}]  {path.name}"
    end = finished or dt.datetime.now(dt.UTC)
    wall_s = max(0.0, (end - started).total_seconds())
    return f"[{state}] wall={wall_s:.0f}s  {path.name}"


def show_output(
    host: str,
    jobid: str,
    *,
    tail: int | None = 100,
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """Render the last ``tail`` lines of the vibe-qc ``.out`` file.

    ``tail=None`` means "show everything."  Prepends a one-line status
    header with the job state and wall time so the operator knows what
    they're looking at.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote output for {host!r} should be delegated by the CLI layer"
        )
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    artifacts = artifact_paths_for_spec(spec)
    scheduler_source = _scheduler_calculation_source(spec, cfg)
    path = artifacts.out_path
    display_path = (
        Path(_scheduler_artifact_path(scheduler_source, artifacts.out_relative_path))
        if scheduler_source is not None
        else path
    )
    header = _output_header(spec, display_path)

    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)
    if spec.is_archived:
        return f"{header}\n{_archived_note()}"
    if scheduler_source is not None:
        exists, text = _scheduler_artifact_snapshot(
            scheduler_source,
            artifacts.out_relative_path,
        )
        if not exists:
            return f"{header}\n(no .out file — calculation may not have started yet)"
        rendered = _tail_text_snapshot(text, tail)
        return f"{header}\n{rendered}"

    if not path.exists():
        return f"{header}\n(no .out file — calculation may not have started yet)"

    result = tail_file(path, tail)
    return f"{header}\n{result}"


def show_output_json(
    host: str,
    jobid: str,
    *,
    tail: int | None = 100,
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    """Same data as :func:`show_output`, structured for machine consumers.

    Shape:

    .. code-block:: json

       {
         "jobid": "...",
         "state": "RUNNING",
         "out_path": "/abs/path/output.out",
         "tail": 100,
         "out": "..."
       }
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote output for {host!r} should be delegated by the CLI layer"
        )
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    artifacts = artifact_paths_for_spec(spec)
    scheduler_source = _scheduler_calculation_source(spec, cfg)
    path = artifacts.out_path
    display_path = (
        _scheduler_artifact_path(scheduler_source, artifacts.out_relative_path)
        if scheduler_source is not None
        else str(path)
    )

    payload: dict[str, object] = {
        "jobid": jobid,
        "state": spec.state.value if spec.state else "unknown",
        "out_path": display_path,
        "tail": tail,
    }
    if spec.is_archived:
        payload["out"] = _archived_note()
    elif scheduler_source is not None:
        exists, text = _scheduler_artifact_snapshot(
            scheduler_source,
            artifacts.out_relative_path,
        )
        payload["out"] = _tail_text_snapshot(text, tail) if exists else None
        payload["scheduler_target"] = spec.scheduler_target
        payload["scheduler_job_id"] = spec.scheduler_job_id
        payload["live_scheduler_workspace"] = True
    elif path.exists():
        payload["out"] = tail_file(path, tail)
    else:
        payload["out"] = None

    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)

    return json.dumps(payload, indent=2, sort_keys=True)


def follow_output(
    host: str,
    jobid: str,
    *,
    multi_user: bool = False,
    initial_tail: int = 20,
    poll_interval: float = 0.5,
    idle_ticks_after_terminal: int = 2,
    sleep: Callable[[float], None] | None = None,
    now: Callable[[], float] | None = None,
    cfg: config.Config | None = None,
) -> Iterator[str]:
    """Yield text chunks (initial tail + new bytes) of the .out file
    until the job is terminal and idle.

    Generator shape so the CLI layer can stream each chunk to stdout
    immediately and tests can drive the loop without real sleeps.

    Mirrors :func:`follow_logs` but targets the single ``.out`` file
    instead of the process stdout/stderr pair.
    """
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote output follow for {host!r} should be delegated by the CLI layer"
        )
    sleep_fn = sleep if sleep is not None else time.sleep
    _ = now

    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    artifacts = artifact_paths_for_spec(spec)
    path = artifacts.out_path

    if spec.is_archived:
        yield _archived_note()
        stamp_terminal_status_read(spec_path)
        return

    scheduler_source = _scheduler_calculation_source(spec, cfg)
    if scheduler_source is not None:
        yield from _follow_scheduler_output(
            spec_path,
            scheduler_source,
            artifacts,
            initial_tail=initial_tail,
            poll_interval=poll_interval,
            idle_ticks_after_terminal=idle_ticks_after_terminal,
            sleep=sleep_fn,
        )
        return

    if not path.exists():
        yield f"(waiting for {path.name} — calculation may not have started yet)\n"
        # Poll until the file appears or the job is terminal.
        while True:
            sleep_fn(poll_interval)
            try:
                spec = JobSpec.read(spec_path)
            except FileNotFoundError:
                return
            # A legacy spec may learn its non-default family only after the
            # manifest appears, so re-resolve while there is no file to tail.
            path = _out_path(spec)
            if path.exists():
                break
            if spec.is_terminal:
                yield "(job finished; no .out file was written)\n"
                stamp_terminal_status_read(spec_path)
                return

    # Read the initial snapshot once and establish the cursor from those exact
    # bytes. Calling ``tail_file`` and then ``stat`` can lose bytes appended in
    # the gap between the two operations.
    try:
        initial_bytes = path.read_bytes()
    except FileNotFoundError:
        initial_bytes = b""
    cursor = len(initial_bytes)
    initial = _tail_follow_snapshot(
        initial_bytes.decode("utf-8", errors="replace"),
        initial_tail,
    )
    if initial:
        yield initial

    def _read_new() -> bytes:
        nonlocal cursor
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return b""
        if size < cursor:
            cursor = 0
        if size == cursor:
            return b""
        start = cursor
        with path.open("rb") as f:
            f.seek(cursor)
            data = f.read()
        cursor = start + len(data)
        return data

    idle_after_terminal = 0
    while True:
        data = _read_new()
        if data:
            yield data.decode("utf-8", errors="replace")

        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return

        if spec.is_terminal:
            if data:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    stamp_terminal_status_read(spec_path)
                    return

        sleep_fn(poll_interval)


def _follow_scheduler_output(
    spec_path: Path,
    source: _SchedulerWorkspaceSource,
    artifacts: ArtifactPaths,
    *,
    initial_tail: int,
    poll_interval: float,
    idle_ticks_after_terminal: int,
    sleep: Callable[[float], None],
) -> Iterator[str]:
    """Follow a scheduler-side ``.out`` through incremental file tails."""
    filename = artifacts.out_relative_path.as_posix()
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    snapshot = source.dispatcher.tail_file_since(
        source.handle,
        filename=filename,
        byte_offset=0,
    )
    if snapshot is None:
        cursor = 0
        seen_file = False
        yield (
            f"(waiting for {artifacts.out_relative_path.name} — "
            "calculation may not have started yet)\n"
        )
    else:
        cursor = snapshot.end_offset
        initial_text = decoder.decode(snapshot.data, final=False)
        seen_file = True
        yield _tail_follow_snapshot(initial_text, initial_tail)

    idle_after_terminal = 0
    while True:
        snapshot = source.dispatcher.tail_file_since(
            source.handle,
            filename=filename,
            byte_offset=cursor,
        )
        if snapshot is None:
            chunk = ""
        else:
            if snapshot.reset:
                decoder = codecs.getincrementaldecoder("utf-8")(
                    errors="replace"
                )
            cursor = snapshot.end_offset
            chunk = decoder.decode(snapshot.data, final=False)
            if not seen_file:
                chunk = _tail_follow_snapshot(chunk, initial_tail)
                seen_file = True

        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return

        # Reconciliation may fetch and remove the remote workspace before the
        # follower's final poll. The fetched local file has identical byte
        # offsets, so recover any unseen suffix from it once terminal.
        if snapshot is None and spec.is_terminal:
            local_data, cursor, truncated = _read_file_since(
                artifacts.out_path,
                cursor,
            )
            if truncated:
                decoder = codecs.getincrementaldecoder("utf-8")(
                    errors="replace"
                )
            chunk = decoder.decode(local_data, final=False)
            if local_data and not seen_file:
                chunk = _tail_follow_snapshot(chunk, initial_tail)
                seen_file = True

        if chunk:
            yield chunk

        if spec.is_terminal:
            if chunk:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    final_text = decoder.decode(b"", final=True)
                    if final_text:
                        yield final_text
                    stamp_terminal_status_read(spec_path)
                    return
        sleep(poll_interval)


# ----------------------------------------------------------------------
# v0.24: `vq progress JOBID` — live SCF iteration table.
# ----------------------------------------------------------------------

_PROGRESS_HEADER = "  iter     energy (Ha)            dE          ||[F,DS]||   DIIS"
_PROGRESS_SEP = "  " + "-" * (len(_PROGRESS_HEADER) - 2)


def _format_progress_row(rec: dict[str, object]) -> str:
    it = int(rec["iter"])
    energy = float(rec["energy"])
    de = rec.get("dE")
    grad = rec.get("grad_norm")
    diis = rec.get("diis_subspace", 0)
    de_str = f"{float(de):+.3e}" if de is not None else "     --   "
    grad_str = f"{float(grad):.3e}" if grad is not None else "-"
    diis_str = f"{int(diis):2d}" if int(diis) > 0 else " -"
    return (
        f"  {it:4d}   {energy:18.10f}  {de_str}  "
        f"{grad_str}       {diis_str}"
    )


def _parse_progress_lines(text: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(rec, dict) or rec.get("event") != "scf_iter":
            continue
        try:
            normalized = dict(rec)
            normalized["iter"] = int(rec["iter"])
            normalized["energy"] = float(rec["energy"])
            normalized["dE"] = (
                None if rec.get("dE") is None else float(rec["dE"])
            )
            normalized["grad_norm"] = (
                None
                if rec.get("grad_norm") is None
                else float(rec["grad_norm"])
            )
            normalized["diis_subspace"] = int(rec.get("diis_subspace", 0))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        records.append(normalized)
    return records


def _progress_path(spec: JobSpec) -> Path:
    return artifact_paths_for_spec(spec).progress_path


def _render_progress_records(
    records: list[dict[str, object]],
    tail: int | None,
) -> str:
    if not records:
        return "(no SCF iteration records yet)"
    if tail is not None and len(records) > tail:
        skipped = len(records) - tail
        records = records[-tail:]
    else:
        skipped = 0
    rows = [_PROGRESS_HEADER, _PROGRESS_SEP]
    if skipped:
        rows.append(f"  ... ({skipped} earlier iterations)")
    rows.extend(_format_progress_row(record) for record in records)
    rows.append(_PROGRESS_SEP)
    return "\n".join(rows)


def _consume_progress_bytes(
    pending: bytes,
    data: bytes,
    *,
    flush: bool = False,
) -> tuple[list[dict[str, object]], bytes]:
    """Frame appended NDJSON without dropping a split final record."""
    combined = pending + data
    newline = combined.rfind(b"\n")
    if newline >= 0:
        complete = combined[: newline + 1]
        remaining = combined[newline + 1 :]
    else:
        complete = b""
        remaining = combined
    if flush and remaining:
        complete += remaining
        remaining = b""
    records = _parse_progress_lines(complete.decode("utf-8", errors="replace"))
    return records, remaining


def show_progress(
    host: str,
    jobid: str,
    *,
    tail: int | None = 50,
    multi_user: bool = False,
    cfg: config.Config | None = None,
) -> str:
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote progress for {host!r} should be delegated by the CLI layer"
        )
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    artifacts = artifact_paths_for_spec(spec)
    path = artifacts.progress_path
    scheduler_source = _scheduler_calculation_source(spec, cfg)
    if spec.is_terminal:
        stamp_terminal_status_read(spec_path)
    if spec.is_archived:
        return _archived_note()
    if scheduler_source is not None:
        progress_relative_path = _scheduler_progress_relative_path(
            scheduler_source,
            artifacts,
        )
        text = _scheduler_artifact_text(
            scheduler_source,
            progress_relative_path,
            lines=None,
        )
        if not text:
            return "(no .scf.jsonl — structured log not enabled)"
        return _render_progress_records(_parse_progress_lines(text), tail)
    if not path.exists():
        return "(no .scf.jsonl — structured log not enabled)"
    text = path.read_text(errors="replace")
    records = _parse_progress_lines(text)
    return _render_progress_records(records, tail)


def follow_progress(
    host: str,
    jobid: str,
    *,
    multi_user: bool = False,
    initial_tail: int = 20,
    poll_interval: float = 0.5,
    idle_ticks_after_terminal: int = 2,
    sleep: Callable[[float], None] | None = None,
    now: Callable[[], float] | None = None,
    cfg: config.Config | None = None,
) -> Iterator[str]:
    if not is_local_host(host):
        raise NotImplementedError(
            f"remote progress follow for {host!r} should be delegated by the CLI layer"
        )
    sleep_fn = sleep if sleep is not None else time.sleep
    _ = now
    spec_path, spec = _resolve_spec(jobid, multi_user=multi_user)
    artifacts = artifact_paths_for_spec(spec)
    path = artifacts.progress_path
    if spec.is_archived:
        yield _archived_note() + "\n"
        stamp_terminal_status_read(spec_path)
        return

    scheduler_source = _scheduler_calculation_source(spec, cfg)
    if scheduler_source is not None:
        yield from _follow_scheduler_progress(
            spec_path,
            scheduler_source,
            artifacts,
            initial_tail=initial_tail,
            poll_interval=poll_interval,
            idle_ticks_after_terminal=idle_ticks_after_terminal,
            sleep=sleep_fn,
        )
        return

    try:
        initial_bytes = path.read_bytes()
        path_exists = True
    except FileNotFoundError:
        initial_bytes = b""
        path_exists = False
    cursor = len(initial_bytes)
    initial_records, pending = _consume_progress_bytes(b"", initial_bytes)
    if pending:
        trailing_records = _parse_progress_lines(
            pending.decode("utf-8", errors="replace")
        )
        if trailing_records:
            initial_records.extend(trailing_records)
            pending = b""
    initial = (
        _render_progress_records(initial_records, initial_tail)
        if path_exists
        else "(no .scf.jsonl — structured log not enabled)"
    )
    if initial:
        yield initial + "\n"

    def _read_new() -> tuple[bytes, bool]:
        nonlocal cursor
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            return b"", False
        truncated = size < cursor
        if truncated:
            cursor = 0
        if size == cursor:
            return b"", truncated
        start = cursor
        with path.open("rb") as stream:
            stream.seek(cursor)
            data = stream.read()
        cursor = start + len(data)
        return data, truncated

    idle_after_terminal = 0
    while True:
        data, truncated = _read_new()
        if truncated:
            pending = b""
        new_records, pending = _consume_progress_bytes(pending, data)
        if new_records:
            yield "\n".join(_format_progress_row(r) for r in new_records) + "\n"
        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return
        if not path.exists() and cursor == 0:
            path = _progress_path(spec)
        if spec.is_terminal:
            if data:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    final_records, pending = _consume_progress_bytes(
                        pending,
                        b"",
                        flush=True,
                    )
                    if final_records:
                        yield "\n".join(
                            _format_progress_row(record)
                            for record in final_records
                        ) + "\n"
                    stamp_terminal_status_read(spec_path)
                    return
        sleep_fn(poll_interval)


def _follow_scheduler_progress(
    spec_path: Path,
    source: _SchedulerWorkspaceSource,
    artifacts: ArtifactPaths,
    *,
    initial_tail: int,
    poll_interval: float,
    idle_ticks_after_terminal: int,
    sleep: Callable[[float], None],
) -> Iterator[str]:
    """Follow scheduler-side NDJSON while retaining split records."""
    filename = _scheduler_progress_relative_path(
        source,
        artifacts,
    ).as_posix()
    snapshot = source.dispatcher.tail_file_since(
        source.handle,
        filename=filename,
        byte_offset=0,
    )
    if snapshot is None:
        cursor = 0
        initial_bytes = b""
        path_exists = False
    else:
        cursor = snapshot.end_offset
        initial_bytes = snapshot.data
        path_exists = True
    initial_rendered = path_exists
    initial_records, pending = _consume_progress_bytes(b"", initial_bytes)
    if pending:
        trailing_records = _parse_progress_lines(
            pending.decode("utf-8", errors="replace")
        )
        if trailing_records:
            initial_records.extend(trailing_records)
            pending = b""
    initial = (
        _render_progress_records(initial_records, initial_tail)
        if path_exists
        else "(no .scf.jsonl — structured log not enabled)"
    )
    yield initial + "\n"

    idle_after_terminal = 0
    while True:
        snapshot = source.dispatcher.tail_file_since(
            source.handle,
            filename=filename,
            byte_offset=cursor,
        )
        if snapshot is None:
            resolved_filename = _scheduler_progress_relative_path(
                source,
                artifacts,
            ).as_posix()
            if resolved_filename != filename:
                filename = resolved_filename
                cursor = 0
                pending = b""
                initial_rendered = False
                snapshot = source.dispatcher.tail_file_since(
                    source.handle,
                    filename=filename,
                    byte_offset=0,
                )
        if snapshot is None:
            data = b""
            truncated = False
        else:
            truncated = snapshot.reset
            cursor = snapshot.end_offset
            data = snapshot.data

        try:
            spec = JobSpec.read(spec_path)
        except FileNotFoundError:
            return

        if snapshot is None and spec.is_terminal:
            data, cursor, truncated = _read_file_since(
                artifacts.progress_path,
                cursor,
            )
        if truncated:
            pending = b""
        new_records, pending = _consume_progress_bytes(pending, data)
        if new_records:
            if initial_rendered:
                yield "\n".join(
                    _format_progress_row(record) for record in new_records
                ) + "\n"
            else:
                yield _render_progress_records(new_records, initial_tail) + "\n"
                initial_rendered = True

        if spec.is_terminal:
            if data:
                idle_after_terminal = 0
            else:
                idle_after_terminal += 1
                if idle_after_terminal >= idle_ticks_after_terminal:
                    final_records, pending = _consume_progress_bytes(
                        pending,
                        b"",
                        flush=True,
                    )
                    if final_records:
                        if initial_rendered:
                            yield "\n".join(
                                _format_progress_row(record)
                                for record in final_records
                            ) + "\n"
                        else:
                            yield _render_progress_records(
                                final_records,
                                initial_tail,
                            ) + "\n"
                    stamp_terminal_status_read(spec_path)
                    return
        sleep(poll_interval)
