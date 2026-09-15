"""Crash feedback (v0.12.0): _read_stderr_tail capture + the failure_tail spec
field + its surfacing in ``vq status``.

On a FAILED terminal transition the daemon captures the tail of stderr.log into
``spec.failure_tail`` so ``vq status JOBID`` shows WHY a calculation died (the
Python traceback, the CRYSTAL error line, the ORCA abort) without a manual
fetch-and-grep. These cover the pure helper, the additive field, and the status
rendering; the daemon hook firing on a real exit is exercised by the daemon suite.
"""

from __future__ import annotations

import json
from pathlib import Path

from vq.daemon import (
    _FAILURE_TAIL_MAX_BYTES,
    _FAILURE_TAIL_MAX_LINES,
    _read_stderr_tail,
)
from vq.spec import JobSpec, JobState
from vq.status import show_status

# --------------------------------------------------------------------------- #
# _read_stderr_tail
# --------------------------------------------------------------------------- #


def test_read_stderr_tail_returns_last_lines(tmp_path: Path) -> None:
    (tmp_path / "stderr.log").write_text(
        'line 1\nline 2\nTraceback (most recent call last):\n'
        '  File "scf.py", line 9\nMemoryError\n'
    )
    tail = _read_stderr_tail(tmp_path, "stderr.log")
    assert tail is not None
    assert tail.endswith("MemoryError")
    assert "Traceback" in tail


def test_read_stderr_tail_caps_to_max_lines(tmp_path: Path) -> None:
    (tmp_path / "stderr.log").write_text("\n".join(f"line {i}" for i in range(200)) + "\n")
    tail = _read_stderr_tail(tmp_path, "stderr.log")
    assert tail is not None
    lines = tail.splitlines()
    assert len(lines) == _FAILURE_TAIL_MAX_LINES
    assert lines[-1] == "line 199"  # the tail, not the head


def test_read_stderr_tail_reads_only_trailing_bytes(tmp_path: Path) -> None:
    big = "HEAD_MARKER\n" + ("x" * (_FAILURE_TAIL_MAX_BYTES * 3)) + "\nTAIL_MARKER"
    (tmp_path / "stderr.log").write_text(big)
    tail = _read_stderr_tail(tmp_path, "stderr.log")
    assert tail is not None
    assert "TAIL_MARKER" in tail
    assert "HEAD_MARKER" not in tail


def test_read_stderr_tail_empty_is_none(tmp_path: Path) -> None:
    (tmp_path / "stderr.log").write_text("")
    assert _read_stderr_tail(tmp_path, "stderr.log") is None


def test_read_stderr_tail_missing_is_none(tmp_path: Path) -> None:
    assert _read_stderr_tail(tmp_path, "stderr.log") is None


def test_read_stderr_tail_whitespace_only_is_none(tmp_path: Path) -> None:
    (tmp_path / "stderr.log").write_text("   \n\n  \n")
    assert _read_stderr_tail(tmp_path, "stderr.log") is None


def test_read_stderr_tail_lossy_decodes_binary(tmp_path: Path) -> None:
    (tmp_path / "stderr.log").write_bytes(b"ok\n\xff\xfe bad bytes\nDONE")
    tail = _read_stderr_tail(tmp_path, "stderr.log")
    assert tail is not None
    assert tail.endswith("DONE")  # invalid bytes don't blow up the read


# --------------------------------------------------------------------------- #
# failure_tail spec field (additive)
# --------------------------------------------------------------------------- #


def test_failure_tail_defaults_none() -> None:
    spec = JobSpec(id="j1", command=["true"], cwd="/tmp", cpus=1)
    assert spec.failure_tail is None


def test_failure_tail_roundtrips(tmp_path: Path) -> None:
    spec = JobSpec(
        id="j2", command=["x"], cwd="/tmp", cpus=1,
        state=JobState.FAILED, exit_code=1,
        failure_tail="ValueError: basis set not found",
    )
    path = tmp_path / "j2.json"
    spec.write(path)
    assert JobSpec.read(path).failure_tail == "ValueError: basis set not found"


def test_pre_field_spec_reads_clean(tmp_path: Path) -> None:
    # A spec written before the field existed: failure_tail absent on disk.
    base = JobSpec(id="j3", command=["true"], cwd="/tmp", cpus=1)
    path = tmp_path / "j3.json"
    base.write(path)
    raw = json.loads(path.read_text())
    raw.pop("failure_tail", None)
    path.write_text(json.dumps(raw))
    assert JobSpec.read(path).failure_tail is None


# --------------------------------------------------------------------------- #
# vq status surfaces failure_tail
# --------------------------------------------------------------------------- #


def _failed_spec(queue: Path, ws: Path, tail: str | None) -> str:
    ws.mkdir(parents=True, exist_ok=True)
    (ws / "stdout.log").write_text("")
    (ws / "stderr.log").write_text("")
    jobid = "deadbeef0001"
    JobSpec(
        id=jobid, command=["scf.py"], cwd=str(ws), cpus=1,
        state=JobState.FAILED, exit_code=1,
        finished_at="2026-06-24T00:00:00+00:00", failure_tail=tail,
    ).write(queue / f"{jobid}.json")
    return jobid


def test_status_shows_failure_tail(tmp_path: Path) -> None:
    queue = tmp_path / "queue"
    queue.mkdir()
    jobid = _failed_spec(queue, tmp_path / "ws", "Traceback ...\nMemoryError")
    out = show_status("localhost", jobid, queue_dir=queue)
    assert "stderr tail:" in out
    assert "MemoryError" in out


def test_status_omits_tail_when_unset(tmp_path: Path) -> None:
    queue = tmp_path / "queue"
    queue.mkdir()
    jobid = _failed_spec(queue, tmp_path / "ws", None)
    out = show_status("localhost", jobid, queue_dir=queue)
    assert "stderr tail:" not in out
