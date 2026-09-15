"""Durable run store for a detached ``vq admin update``.

The 2026-09-11 release lane lost three hosts to one dropped SSH session.
``vq admin update ENV HOST`` delegates to ``ssh HOST ... vq admin update ENV
localhost``, and the remote updater ran inside that ssh session. build-host, compute-b
and workstation run systemd-logind with ``KillUserProcesses=yes``: when the last
ssh session ends, logind stops the session's scope and every process in it.
The updater was killed between the checkout and the native rebuild, and the
atomic rollback, which lives in finalizers a killed process never reaches,
did not run. Each host was left at ``v0.17.1`` with a marker reading
``building``, a dead pid, and a venv whose ``import vibeqc`` failed on a
half-built ``libint2.so``.

This module is the durable half of the fix. The remote updater is started
outside the session -- as a transient systemd user unit where a user manager
answers, in a session of its own elsewhere -- and its progress and terminal
outcome are published *here* instead of being returned down the SSH pipe.
The driver polls this store over fresh read-only
connections and can re-attach to a run after a drop, because the run id is
chosen by the driver *before* launch -- an ambiguous launch is therefore still
observable rather than unknown.

One run directory, ``<state root>/admin-detached/run-<run id>/``, holds:

``launch.json``
    Written by the sshd-attached parent before it spawns anything. Its
    presence is what makes an ambiguous launch adoptable.
``activation.json``
    Written by the detached child once it is a session leader, carrying the
    pid and start-time fingerprint that prove *this* run is still alive, and
    later the path of the update transcript.
``result.json``
    Written by the child when the update is terminal. It carries the exact
    stdout payload and exit code the attached command would have produced, so
    the driver reports the same thing it always did.

The terminal receipt is the load-bearing part. A successful update *removes*
its marker, so polling the marker alone cannot tell "finished cleanly" from
"never started" -- which is the ambiguity this whole module exists to retire.
"""
from __future__ import annotations

import base64
import json
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path

from vq import paths
from vq.spec import utcnow_iso

DETACHED_RUN_DIRNAME = "admin-detached"
"""Directory of detached-update run records under the state root."""

DETACHED_LAUNCH_SCHEMA = "vq.admin.detached_update_launch/1"
DETACHED_ACTIVATION_SCHEMA = "vq.admin.detached_update_activation/1"
DETACHED_RESULT_SCHEMA = "vq.admin.detached_update_result/1"
DETACHED_OBSERVATION_SCHEMA = "vq.admin.detached_update_observation/1"

DETACHED_RUNS_TO_KEEP = 20
"""Run records retained before the oldest are pruned.

Matches the transcript bound in :data:`vq.paths.ADMIN_UPDATE_LOGS_TO_KEEP`:
these records point at those transcripts, so retaining more of one than the
other only produces receipts whose evidence has already been pruned away.
"""

MAX_TRANSCRIPT_CHUNK = 64 * 1024
"""Largest transcript slice one observation may carry.

A two-hour native rebuild writes megabytes. Bounding the chunk keeps a single
poll's response small and predictable; the driver drains a backlog by polling
again immediately at the new offset rather than by asking for more.
"""

MAX_PAYLOAD_BYTES = 4 * 1024 * 1024
"""Cap on the stored stdout payload, so a pathological result cannot make the
terminal receipt unreadable."""

STATE_MISSING = "missing"
"""No run directory: the launch never got far enough to record intent."""

STATE_LAUNCHING = "launching"
"""Intent recorded, but the child has not yet proved itself a live session."""

STATE_RUNNING = "running"
"""The detached updater is alive and owns the work."""

STATE_COMPLETED = "completed"
"""A terminal receipt is present; the outcome is known exactly."""

STATE_LOST = "lost"
"""Activated, not terminal, and its process is gone.

The genuinely-unknown case, and the only one that still earns the "outcome
unknown, do not retry" advice -- now with a retained transcript to read.
"""

_RUN_ID_RE = re.compile(r"\A[0-9a-f]{32}\Z")
_RUN_DIR_PREFIX = "run-"


class DetachedRunError(RuntimeError):
    """A detached-update run record is malformed, unsafe, or absent."""


def new_run_id() -> str:
    """Mint a run id.

    The *driver* calls this before launching, which is what makes a lost
    launch response recoverable: the identity of the run does not depend on
    the response that went missing.
    """
    return uuid.uuid4().hex


def validate_run_id(run_id: str) -> str:
    """Return ``run_id`` if it is exactly 32 lowercase hex digits.

    Run ids arrive over SSH argv and select a directory, so this is a path
    containment check, not a formatting nicety.
    """
    if not isinstance(run_id, str) or _RUN_ID_RE.fullmatch(run_id) is None:
        raise DetachedRunError(
            "detached run id must be exactly 32 lowercase hex digits"
        )
    return run_id


def detached_run_root(*, multi_user: bool = False) -> Path:
    """Directory holding every detached-update run record.

    Resolved through the same multi-user branch as the update transcripts:
    ``vq admin update`` runs as root on the multi-user hosts, so a naive
    ``state_root()`` would hide the record in root's XDG directory.
    """
    root = paths.multi_user_root() if multi_user else paths.state_root()
    return root / DETACHED_RUN_DIRNAME


def detached_run_dir(run_id: str, *, multi_user: bool = False) -> Path:
    """Directory holding one run's records."""
    return detached_run_root(multi_user=multi_user) / (
        _RUN_DIR_PREFIX + validate_run_id(run_id)
    )


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    """Atomically publish one owner-only record.

    tmpfile + ``os.replace`` so a poll never reads half a receipt, and 0600 so
    the record is no more readable than the marker it accompanies.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    fd = -1
    try:
        fd = os.open(
            str(tmp),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.write(fd, data)
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(tmp, path)
    finally:
        if fd >= 0:
            os.close(fd)
        tmp.unlink(missing_ok=True)
    dir_fd = os.open(str(path.parent), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _read_json(path: Path) -> dict[str, object] | None:
    """Parse one record, or ``None`` when it is absent or unreadable.

    Conservative on malformed input: a torn or hand-edited record reads as
    absent, which downgrades the observation rather than crashing the poll.
    """
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def write_launch(
    run_id: str,
    *,
    target: str,
    argv: list[str],
    multi_user: bool = False,
) -> Path:
    """Record launch intent before anything is spawned.

    Written by the sshd-attached parent. If the transport dies immediately
    after, this file is the evidence that lets the driver adopt the run
    instead of guessing.
    """
    run_dir = detached_run_dir(run_id, multi_user=multi_user)
    run_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(run_dir, 0o700)
    _write_private_json(
        run_dir / "launch.json",
        {
            "schema": DETACHED_LAUNCH_SCHEMA,
            "run_id": run_id,
            "target": target,
            "argv": list(argv),
            "launcher_pid": os.getpid(),
            "launched_at": utcnow_iso(),
        },
    )
    return run_dir


def write_activation(
    run_id: str,
    *,
    pid: int,
    pid_start_time: int,
    multi_user: bool = False,
) -> None:
    """Publish that the detached updater is alive and owns the work.

    The start-time fingerprint travels with the pid for the same reason the
    marker records one: after a reboot the kernel reuses pids, and a bare
    liveness probe would then read a stranger as "still building".
    """
    _write_private_json(
        detached_run_dir(run_id, multi_user=multi_user) / "activation.json",
        {
            "schema": DETACHED_ACTIVATION_SCHEMA,
            "run_id": run_id,
            "pid": pid,
            "pid_start_time": pid_start_time,
            "transcript": None,
            "activated_at": utcnow_iso(),
        },
    )


def publish_transcript(
    run_id: str, transcript: Path, *, multi_user: bool = False
) -> None:
    """Attach the update transcript's path to an activated run.

    Called once the run log is open, which is the first moment the path
    exists. Best-effort: a run whose transcript could not be recorded still
    reports state and outcome, it just cannot be followed live.
    """
    path = detached_run_dir(run_id, multi_user=multi_user) / "activation.json"
    activation = _read_json(path)
    if activation is None:
        return
    activation["transcript"] = str(transcript)
    _write_private_json(path, activation)


def write_result(
    run_id: str,
    *,
    outcome: str,
    exit_code: int,
    payload: str,
    error: str | None = None,
    multi_user: bool = False,
) -> None:
    """Publish the terminal receipt.

    ``payload`` is the exact stdout the attached command would have printed
    and ``exit_code`` the status it would have exited with, so the driver can
    reproduce the old behaviour verbatim rather than re-deriving it.
    """
    encoded = payload.encode("utf-8")[:MAX_PAYLOAD_BYTES]
    _write_private_json(
        detached_run_dir(run_id, multi_user=multi_user) / "result.json",
        {
            "schema": DETACHED_RESULT_SCHEMA,
            "run_id": run_id,
            "outcome": outcome,
            "exit_code": int(exit_code),
            "payload": encoded.decode("utf-8", "replace"),
            "payload_truncated": len(encoded) < len(payload.encode("utf-8")),
            "error": error,
            "completed_at": utcnow_iso(),
        },
    )


@dataclass(frozen=True)
class DetachedObservation:
    """One read-only look at a detached run."""

    run_id: str
    state: str
    detail: str
    target: str | None = None
    pid: int | None = None
    transcript: str | None = None
    transcript_offset: int = 0
    transcript_next_offset: int = 0
    transcript_size: int = 0
    transcript_base64: str = ""
    outcome: str | None = None
    exit_code: int | None = None
    payload: str | None = None
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state == STATE_COMPLETED

    def to_json(self) -> dict[str, object]:
        return {
            "schema": DETACHED_OBSERVATION_SCHEMA,
            "run_id": self.run_id,
            "state": self.state,
            "detail": self.detail,
            "target": self.target,
            "pid": self.pid,
            "transcript": self.transcript,
            "transcript_offset": self.transcript_offset,
            "transcript_next_offset": self.transcript_next_offset,
            "transcript_size": self.transcript_size,
            "transcript_base64": self.transcript_base64,
            "outcome": self.outcome,
            "exit_code": self.exit_code,
            "payload": self.payload,
            "error": self.error,
        }


def parse_observation(payload: object) -> DetachedObservation:
    """Rebuild an observation from a remote ``--json`` response.

    Fails closed on anything that is not exactly this schema: a driver that
    accepted a loosely-shaped object could read a stale or unrelated record as
    a terminal receipt, which is the one mistake this protocol must not make.
    """
    if not isinstance(payload, dict):
        raise DetachedRunError("detached observation must be a JSON object")
    if payload.get("schema") != DETACHED_OBSERVATION_SCHEMA:
        raise DetachedRunError(
            "detached observation carries an unknown schema "
            f"{payload.get('schema')!r}"
        )
    state = payload.get("state")
    if state not in {
        STATE_MISSING,
        STATE_LAUNCHING,
        STATE_RUNNING,
        STATE_COMPLETED,
        STATE_LOST,
    }:
        raise DetachedRunError(f"detached observation has unknown state {state!r}")
    run_id = payload.get("run_id")
    if not isinstance(run_id, str):
        raise DetachedRunError("detached observation is missing its run id")
    exit_code = payload.get("exit_code")
    if state == STATE_COMPLETED and not isinstance(exit_code, int):
        raise DetachedRunError(
            "a completed detached observation must carry an integer exit code"
        )

    def _text(name: str) -> str | None:
        value = payload.get(name)
        return value if isinstance(value, str) else None

    def _count(name: str) -> int:
        value = payload.get(name)
        return value if isinstance(value, int) and not isinstance(value, bool) else 0

    return DetachedObservation(
        run_id=run_id,
        state=state,
        detail=_text("detail") or "",
        target=_text("target"),
        pid=payload.get("pid") if isinstance(payload.get("pid"), int) else None,
        transcript=_text("transcript"),
        transcript_offset=_count("transcript_offset"),
        transcript_next_offset=_count("transcript_next_offset"),
        transcript_size=_count("transcript_size"),
        transcript_base64=_text("transcript_base64") or "",
        outcome=_text("outcome"),
        exit_code=exit_code if isinstance(exit_code, int) else None,
        payload=_text("payload"),
        error=_text("error"),
    )


def _transcript_slice(
    transcript: str | None,
    *,
    offset: int,
    max_bytes: int,
    multi_user: bool,
) -> tuple[bytes, int]:
    """Read ``max_bytes`` of the transcript from ``offset``; also its size.

    The path comes from a record this host wrote, but it is still confined to
    the update-transcript directory before being opened: a record is data, and
    a reader that dereferences data as a path without bounding it is one
    hand-edited file away from serving something else.
    """
    if not transcript:
        return b"", 0
    candidate = Path(transcript)
    allowed = paths.admin_update_log_dir(multi_user=multi_user).resolve(strict=False)
    resolved = candidate.resolve(strict=False)
    if resolved != allowed and allowed not in resolved.parents:
        return b"", 0
    try:
        size = resolved.stat().st_size
        with resolved.open("rb") as handle:
            handle.seek(min(offset, size))
            return handle.read(max(0, min(max_bytes, MAX_TRANSCRIPT_CHUNK))), size
    except OSError:
        return b"", 0


def observe(
    run_id: str,
    *,
    offset: int = 0,
    max_bytes: int = MAX_TRANSCRIPT_CHUNK,
    multi_user: bool = False,
) -> DetachedObservation:
    """Classify one run and return a slice of its transcript.

    Read-only and side-effect free, which is what lets the driver retry it
    freely after a transport drop. The records are read newest-first
    (result, then activation, then launch) so a poll that races the child's
    own writes can only ever see an older but internally consistent state.
    """
    from vq import admin  # noqa: PLC0415 — the liveness probe lives with the marker

    if offset < 0:
        raise DetachedRunError("transcript offset cannot be negative")
    run_dir = detached_run_dir(run_id, multi_user=multi_user)
    result = _read_json(run_dir / "result.json")
    activation = _read_json(run_dir / "activation.json")
    launch = _read_json(run_dir / "launch.json")

    target = launch.get("target") if launch is not None else None
    target = target if isinstance(target, str) else None
    transcript = activation.get("transcript") if activation is not None else None
    transcript = transcript if isinstance(transcript, str) else None
    chunk, size = _transcript_slice(
        transcript, offset=offset, max_bytes=max_bytes, multi_user=multi_user
    )
    pid = activation.get("pid") if activation is not None else None
    pid = pid if isinstance(pid, int) else None

    common: dict[str, object] = {
        "run_id": run_id,
        "target": target,
        "pid": pid,
        "transcript": transcript,
        "transcript_offset": offset,
        "transcript_next_offset": offset + len(chunk),
        "transcript_size": size,
        "transcript_base64": base64.b64encode(chunk).decode("ascii"),
    }

    if result is not None:
        outcome = result.get("outcome")
        exit_code = result.get("exit_code")
        payload = result.get("payload")
        error = result.get("error")
        return DetachedObservation(
            state=STATE_COMPLETED,
            detail=(
                f"detached update finished with outcome "
                f"{outcome if isinstance(outcome, str) else 'unknown'}"
            ),
            outcome=outcome if isinstance(outcome, str) else None,
            # A receipt whose exit code is unreadable is not a success: the
            # one thing this file exists to record is missing from it.
            exit_code=exit_code if isinstance(exit_code, int) else 1,
            payload=payload if isinstance(payload, str) else "",
            error=error if isinstance(error, str) else None,
            **common,  # type: ignore[arg-type]
        )

    if activation is None:
        if launch is None:
            return DetachedObservation(
                state=STATE_MISSING,
                detail=(
                    "no detached run record on this host; the launch never "
                    "recorded its intent"
                ),
                **common,  # type: ignore[arg-type]
            )
        return DetachedObservation(
            state=STATE_LAUNCHING,
            detail="detached updater is starting but has not activated yet",
            **common,  # type: ignore[arg-type]
        )

    recorded_start = activation.get("pid_start_time")
    recorded_start = recorded_start if isinstance(recorded_start, int) else 0
    alive = admin._pid_liveness(pid or 0)
    if alive is True and recorded_start:
        live_start = admin._pid_start_time(pid or 0)
        if live_start is not None and live_start != recorded_start:
            alive = False
    if alive is False:
        return DetachedObservation(
            state=STATE_LOST,
            detail=(
                f"the detached updater (pid={pid}) is gone and published no "
                "terminal receipt"
            ),
            **common,  # type: ignore[arg-type]
        )
    return DetachedObservation(
        state=STATE_RUNNING,
        detail=f"detached updater (pid={pid}) is running",
        **common,  # type: ignore[arg-type]
    )


def prune_detached_runs(
    *, keep: int = DETACHED_RUNS_TO_KEEP, multi_user: bool = False
) -> list[Path]:
    """Drop all but the newest ``keep`` run records.

    Best-effort, and never touches a run that is still live: a record without
    a terminal receipt whose process is alive is the one thing here that is
    load-bearing right now.
    """
    root = detached_run_root(multi_user=multi_user)
    try:
        candidates = [p for p in root.iterdir() if p.is_dir()]
    except OSError:
        return []
    ordered = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    removed: list[Path] = []
    for stale in ordered[max(0, keep):]:
        run_id = stale.name[len(_RUN_DIR_PREFIX):]
        if _RUN_ID_RE.fullmatch(run_id) is None:
            continue
        try:
            if observe(run_id, max_bytes=1, multi_user=multi_user).state in {
                STATE_LAUNCHING,
                STATE_RUNNING,
            }:
                continue
        except DetachedRunError:
            continue
        # Every file a run can leave, or the rmdir below fails and the record
        # is never pruned: child.log exists for every launched run.
        for name in ("launch.json", "activation.json", "result.json", "child.log", "token"):
            (stale / name).unlink(missing_ok=True)
        try:
            stale.rmdir()
        except OSError:
            continue
        removed.append(stale)
    return removed
