"""Submit, observe, cancel, and fetch jobs through a scheduler reached by SSH.

Where :class:`~vq.dispatch.LocalDispatcher` runs a job as a local child process,
:class:`SchedulerDispatcher` runs it as a batch job on a cluster the vq daemon
does **not** live on: it stages the job's workspace to the cluster, submits a
rendered job script with ``qsub`` or ``sbatch``, observes jobs with batched
``qstat`` or ``squeue`` polls, cancels with ``qdel`` or ``scancel``, and fetches
the workspace artifacts and exit marker written by the job. The exit marker is
the primary return-code source and the daemon's completion fence; scheduler
accounting is a non-array fallback when the marker cannot be read. The cluster
holds no persistent vq process; only the scheduler job runs there.

This module is the **orchestration** layer. It owns *what* commands run in
*what* order and *how* their output is interpreted, but delegates two concerns:

* The **string dialect** — flag spellings, ``#PBS`` / ``#SBATCH`` directives,
  scheduler state letters, and id/exit-status parsing — to a
  :class:`~vq.scheduler_dialect.SchedulerDialect` (Torque and Slurm are
  implemented). Adding another scheduler is primarily a new dialect rather
  than a new orchestration path.
* The **SSH transport** — running scheduler commands and transferring workspace
  files — to a :class:`RemoteRunner`. The real :class:`SshRemoteRunner` wraps
  :mod:`vq.transport`; tests inject a fake that records argv and returns canned
  scheduler output, so submit, poll, terminal, and fetch paths run without a
  cluster or SSH.

The daemon routes scheduler-targeted specs through
``Daemon._start_scheduler_job`` and reconciles them through
``Daemon._reconcile_scheduler``. Other consumers include logs, status, fetch,
kill, pause/resume, CLI tail and cleanup, and admin census.
``scheduler = "local"`` remains the default and keeps ordinary hosts on the
local process path.
"""

from __future__ import annotations

import base64
import binascii
import contextlib
import json
import logging
import math
import os
import re
import shlex
import stat
import tarfile
import tempfile
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol

from vq import transport
from vq.config import HostConfig, SchedulerProgramHooks
from vq.scheduler_dialect import (
    DialectError,
    QstatDetail,
    ResourceRequest,
    SchedulerDialect,
    SchedulerPhase,
    SlurmDialect,
    dialect_for,
    enforce_scheduler_wall_time_limit,
    parse_submit_extra,
)
from vq.spec import JobSpec, JobState, utcnow_iso

log = logging.getLogger("vq.scheduler_dispatch")

# Run on the compute node, where the payload need not have Python or rsync.
# A private seed copy permits byte comparisons without relying on timestamp
# ordering (cp -a preserves old timestamps, and clocks can differ across hosts).
_NODE_SCRATCH_RECONCILE = r'''
__vq_reserved() {
  case "$1" in
    _vq|_vq/*|stdout.log|stdout.log.*|stderr.log|stderr.log.*|.pbs-spool.*) return 0 ;;
    *) return 1 ;;
  esac
}
__vq_same() {
  if [ -L "$1" ] && [ -L "$2" ]; then
    [ "$(readlink "$1")" = "$(readlink "$2")" ]
  elif [ -f "$1" ] && [ ! -L "$1" ] && [ -f "$2" ] && [ ! -L "$2" ]; then
    cmp -s "$1" "$2" &&
      [ "$(LC_ALL=C ls -ld "$1" | cut -c1-10)" = "$(LC_ALL=C ls -ld "$2" | cut -c1-10)" ]
  else
    return 1
  fi
}
__vq_publish_changed() {
  find "$__vq_scratch" -mindepth 1 -print0 > "$__vq_stage/paths" || return 1
  : > "$__vq_stage/directories" || return 1
  while IFS= read -r -d '' __vq_source; do
    __vq_relative=${__vq_source#"$__vq_scratch"/}
    __vq_reserved "$__vq_relative" && continue
    __vq_original="$__vq_seed/$__vq_relative"
    __vq_destination="$1/$__vq_relative"
    # An untouched seed cannot resurrect a shared path deleted by the payload.
    __vq_same "$__vq_source" "$__vq_original" && continue
    if [ -d "$__vq_source" ] && [ ! -L "$__vq_source" ] &&
       [ -d "$__vq_original" ] && [ ! -L "$__vq_original" ]; then
      continue
    fi
    # Do not traverse a shared symlink or a parent replaced by a file.
    __vq_parent=$(dirname "$__vq_destination")
    while [ "$__vq_parent" != "$1" ]; do
      if [ -L "$__vq_parent" ] || [ ! -d "$__vq_parent" ]; then
        printf 'vq: copy-back conflict at parent of %s\n' "$__vq_relative" >&2
        return 1
      fi
      __vq_parent=$(dirname "$__vq_parent")
    done
    if [ -d "$__vq_source" ] && [ ! -L "$__vq_source" ]; then
      if [ -L "$__vq_destination" ] ||
         { [ -e "$__vq_destination" ] && [ ! -d "$__vq_destination" ]; }; then
        printf 'vq: copy-back conflict at directory %s\n' "$__vq_relative" >&2
        return 1
      fi
      if [ ! -d "$__vq_destination" ]; then
        # Keep newly published directories private until their files arrive.
        mkdir -m 700 "$__vq_destination" || return 1
        printf '%s\0' "$__vq_source" >> "$__vq_stage/directories" || return 1
      fi
      continue
    fi
    if [ ! -f "$__vq_source" ] && [ ! -L "$__vq_source" ]; then
      printf 'vq: unsupported scratch output %s\n' "$__vq_relative" >&2
      return 1
    fi
    # Equal changes on both sides need no copy.
    __vq_same "$__vq_source" "$__vq_destination" && continue
    if [ -e "$__vq_original" ] || [ -L "$__vq_original" ]; then
      if ! __vq_same "$__vq_original" "$__vq_destination"; then
        printf 'vq: copy-back conflict at %s\n' "$__vq_relative" >&2
        return 1
      fi
    elif [ -e "$__vq_destination" ] || [ -L "$__vq_destination" ]; then
      printf 'vq: copy-back conflict at new output %s\n' "$__vq_relative" >&2
      return 1
    fi
    # Portable mv may follow a directory symlink supplied as its destination.
    if [ -L "$__vq_destination" ]; then
      printf 'vq: copy-back conflict at symlink %s\n' "$__vq_relative" >&2
      return 1
    fi
    # Replacing the directory entry avoids following a destination symlink.
    cp -a "$__vq_source" "$__vq_stage/publish" || return 1
    mv -f "$__vq_stage/publish" "$__vq_destination" || return 1
  done < "$__vq_stage/paths"
  while IFS= read -r -d '' __vq_source; do
    __vq_relative=${__vq_source#"$__vq_scratch"/}
    __vq_mode=$(stat -c %a "$__vq_source" 2>/dev/null || stat -f %Lp "$__vq_source") || return 1
    chmod "$__vq_mode" "$1/$__vq_relative" || return 1
  done < "$__vq_stage/directories"
}
__vq_reconcile() {
  __vq_lock="$1/_vq/scratch-copyback.lock"
  __vq_attempt=0
  while ! mkdir "$__vq_lock" 2>/dev/null; do
    __vq_attempt=$((__vq_attempt + 1))
    if [ "$__vq_attempt" -ge 100 ]; then
      printf 'vq: scratch copy-back lock unavailable; output retained\n' >&2
      return 1
    fi
    sleep 0.1
  done
  __vq_lock_held=1
  __vq_publish_changed "$1"
  __vq_copy_rc=$?
  rmdir "$__vq_lock" || __vq_copy_rc=1
  __vq_lock_held=0
  return "$__vq_copy_rc"
}
'''

# The exit-marker relpath, mirroring the local dispatch convention
# (daemon.EXIT_MARKER_RELPATH = "_vq/exit-code"). The job script writes the
# inner command's rc here; the dispatcher reads it back over SSH as the rc
# source of truth. For an array job each sub-job writes its own marker keyed by
# the scheduler's array index (review note 3: a single aggregate exit_status is
# first-match and cannot give per-sub-job rc).
EXIT_MARKER_RELDIR = "_vq"
EXIT_MARKER_BASENAME = "exit-code"

# Scheduler jobs run outside the local daemon's process tree, so the watchdog
# cannot provide terminal CPU/RSS accounting for them. Wrap the already-composed
# effective command with GNU time and publish one machine-readable receipt in
# the same shared _vq directory as the exit marker. The absolute path and a
# runtime probe deliberately fail closed on BSD time or a missing installation.
RESOURCE_USAGE_BASENAME = "resource-usage.json"
RESOURCE_USAGE_SCHEMA = "vq.scheduler-resource-usage.v1"
GNU_TIME_COMMAND = "/usr/bin/time"
RESOURCE_COLLECTOR_FAILURE_EXIT_CODE = 125
_GNU_TIME_SENTINEL = "__VQ_RESOURCE_USAGE_V1__"
_GNU_TIME_FORMAT = f"{_GNU_TIME_SENTINEL} %e %U %S %M"
_RESOURCE_USAGE_AWK = r"""
BEGIN {
    number = "^[0-9]+([.][0-9]+)?$"
    integer = "^[0-9]+$"
}
$1 == sentinel && NF == 5 {
    wall = $2
    user = $3
    sys_cpu = $4
    rss = $5
    found = 1
}
END {
    if (!found || wall !~ number || user !~ number || sys_cpu !~ number ||
            rss !~ integer || rc !~ integer) {
        exit 1
    }
    printf "{\n"
    printf "  \"schema\": \"%s\",\n", schema
    printf "  \"status\": \"ok\",\n"
    printf "  \"collector\": \"gnu-time\",\n"
    printf "  \"scope\": \"effective-command\",\n"
    printf "  \"command_status\": \"%s\",\n", command_status
    printf "  \"command_exit_code\": %s,\n", rc
    printf "  \"wall_seconds\": %s,\n", wall
    printf "  \"user_cpu_seconds\": %s,\n", user
    printf "  \"system_cpu_seconds\": %s,\n", sys_cpu
    printf "  \"active_cpu_seconds\": %.6f,\n", user + sys_cpu
    printf "  \"peak_rss_kb\": %s,\n", rss
    printf "  \"peak_rss_mb\": %.6f\n", rss / 1024.0
    printf "}\n"
}
""".strip()
_RESOURCE_USAGE_PROBE_AWK = r"""
BEGIN { number = "^[0-9]+([.][0-9]+)?$"; integer = "^[0-9]+$" }
$1 == sentinel && NF == 5 && $2 ~ number && $3 ~ number &&
        $4 ~ number && $5 ~ integer { found = 1 }
END { exit(found ? 0 : 1) }
""".strip()

# The job records its OWN scheduler id here, first thing, before running
# anything. It is the recovery path for a handle the driver never persisted:
# `_start_scheduler_job` claims RUNNING with `scheduler_job_id=None`, submits
# outside the spec lock, then records the id in a second write, so a driver
# death in that window strands a live cluster job the driver can no longer name.
# Written by the job means it exists whenever the job does, independent of
# whether the driver survived its own bookkeeping.
SCHEDULER_ID_BASENAME = "scheduler-job-id"

# stdout / stderr filenames inside the staged workspace, matching vq's local
# workspace convention so result-readback is uniform across dispatchers.
STDOUT_LOG = "stdout.log"
STDERR_LOG = "stderr.log"

# Scheduler operations cross a sometimes-busy login node. Keep qstat bounded but
# longer than the generic housekeeping timeout, and give result tar creation the
# same order-of-magnitude budget as the scp download that follows it.
SCHEDULER_POLL_TIMEOUT_SECONDS = 120.0
SCHEDULER_ARCHIVE_TIMEOUT_SECONDS = 1200.0
SCHEDULER_ACCOUNTING_TIMEOUT_SECONDS = 30.0
SCHEDULER_ACCOUNTING_MAX_BYTES = 256 * 1024
SCHEDULER_ACCOUNTING_MAX_ROWS = 4096
SCHEDULER_SAMPLES_MAX_BYTES = 1024 * 1024
SCHEDULER_SUBMIT_RECEIPT_SCHEMA = "vq.scheduler-submit-once.v1"
SCHEDULER_SUBMIT_RECEIPT_MAX_BYTES = 4096
SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES = 64 * 1024
SCHEDULER_JOB_ID_MAX_BYTES = 256
MISSING_MARKER_DIAGNOSTIC_TAIL_LINES = 120
MISSING_MARKER_DIAGNOSTIC_MAX_CHARS = 4096
MISSING_MARKER_DIAGNOSTIC_FILE_LIMIT = 200
TAIL_REWRITE_GUARD_BYTES = 4096

_SLURM_ACCOUNTING_JOB_ID_RE = re.compile(
    r"^(?P<root>[0-9]+)(?:_(?P<task>[0-9]+))?"
    r"(?:\.(?P<step>[A-Za-z0-9_-]+))?$"
)
_SLURM_MEMORY_RE = re.compile(
    r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>[KMGTPE]?)(?:I?B)?$",
    re.IGNORECASE,
)
_SLURM_DECIMAL_RE = re.compile(r"^[0-9]+(?:\.[0-9]{1,9})?$")
SCHEDULER_RESOURCE_RECEIPT_MAX_BYTES = 64 * 1024


@dataclass(frozen=True)
class _SlurmAccountingRow:
    root: str
    task: str | None
    step: str | None
    cpu_seconds: float | None
    rss_mb: float | None
    elapsed_seconds: float | None


def _parse_slurm_duration(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    try:
        if ":" not in value:
            if _SLURM_DECIMAL_RE.fullmatch(value) is None:
                raise ValueError
            seconds = float(value)
        else:
            day_text, separator, clock_text = value.partition("-")
            if separator:
                days = int(day_text)
            else:
                days = 0
                clock_text = day_text
            fields = clock_text.split(":")
            if len(fields) != 3:
                raise ValueError
            if any(not field.isdigit() for field in fields[:2]):
                raise ValueError
            if _SLURM_DECIMAL_RE.fullmatch(fields[2]) is None:
                raise ValueError
            hours, minutes = (int(field) for field in fields[:2])
            fractional_seconds = float(fields[2])
            if minutes >= 60 or fractional_seconds >= 60 or (separator and hours >= 24):
                raise ValueError
            seconds = days * 86400 + hours * 3600 + minutes * 60 + fractional_seconds
    except (ValueError, OverflowError) as exc:
        raise SchedulerError("malformed Slurm CPU/elapsed duration") from exc
    if not math.isfinite(seconds) or seconds < 0:
        raise SchedulerError("malformed Slurm CPU/elapsed duration")
    return seconds


def _parse_slurm_max_rss(value: str) -> float | None:
    value = value.strip()
    if not value:
        return None
    match = _SLURM_MEMORY_RE.fullmatch(value)
    if match is None:
        raise SchedulerError("malformed Slurm MaxRSS value")
    amount = float(match.group("value"))
    # The command pins --units=K. A suffix-free value is therefore KiB.
    unit = match.group("unit").upper() or "K"
    exponent = {"K": -1, "M": 0, "G": 1, "T": 2, "P": 3, "E": 4}[unit]
    rss_mb = amount * (1024.0**exponent)
    if not math.isfinite(rss_mb) or rss_mb < 0:
        raise SchedulerError("malformed Slurm MaxRSS value")
    return rss_mb


def _unavailable_scheduler_sample(
    scheduler_job_id: str,
    reason: str,
    *,
    observed_groups: int = 0,
) -> dict[str, object]:
    return {
        "ts": utcnow_iso(),
        "elapsed_seconds": None,
        "rss_mb": None,
        "cpu_percent": None,
        "cpu_time_seconds": None,
        "cpu_time_source": "slurm-sacct",
        "cgroup_lookup": None,
        "cgroup_path": None,
        "sample_pid": None,
        "sample_pgid": None,
        "scheduler_accounting_status": "unavailable",
        "scheduler_accounting_reason": reason,
        "scheduler_accounting_job_id": scheduler_job_id,
        "scheduler_accounting_groups": observed_groups,
    }


def _scheduler_accounting_sample(
    stdout: str,
    scheduler_job_id: str,
    *,
    expected_array_size: int | None = None,
) -> dict[str, object]:
    """Project bounded terminal sacct rows into the watchdog sample schema."""
    if len(stdout.encode("utf-8", "replace")) > SCHEDULER_ACCOUNTING_MAX_BYTES:
        raise SchedulerError("Slurm accounting output exceeded the bounded parser")
    requested = _SLURM_ACCOUNTING_JOB_ID_RE.fullmatch(scheduler_job_id)
    if requested is None or requested.group("step") is not None:
        raise SchedulerError("malformed Slurm scheduler job id")
    lines = [line for line in stdout.splitlines() if line.strip()]
    if len(lines) > SCHEDULER_ACCOUNTING_MAX_ROWS:
        raise SchedulerError("Slurm accounting output exceeded the bounded parser")
    rows: list[_SlurmAccountingRow] = []
    seen_job_ids: set[str] = set()
    saw_relevant_nonterminal = False
    for line in lines:
        fields = line.split("|")
        if len(fields) != 5:
            raise SchedulerError("malformed Slurm accounting output")
        job_text, state, cpu_text, rss_text, elapsed_text = (
            field.strip() for field in fields
        )
        match = _SLURM_ACCOUNTING_JOB_ID_RE.fullmatch(job_text)
        if match is None:
            raise SchedulerError("malformed Slurm accounting job id")
        if match.group("root") != requested.group("root"):
            continue
        requested_task = requested.group("task")
        if requested_task is not None and match.group("task") != requested_task:
            continue
        if job_text in seen_job_ids:
            raise SchedulerError("duplicate Slurm accounting job id")
        seen_job_ids.add(job_text)
        try:
            phase = SlurmDialect().phase_for_state(state)
        except DialectError as exc:
            raise SchedulerError("malformed Slurm accounting state") from exc
        if phase is not SchedulerPhase.FINISHED:
            saw_relevant_nonterminal = True
            continue
        rows.append(
            _SlurmAccountingRow(
                root=match.group("root"),
                task=match.group("task"),
                step=match.group("step"),
                cpu_seconds=_parse_slurm_duration(cpu_text),
                rss_mb=_parse_slurm_max_rss(rss_text),
                elapsed_seconds=_parse_slurm_duration(elapsed_text),
            )
        )
    if not rows:
        reason = "accounting_lag" if saw_relevant_nonterminal or not lines else "no_job_rows"
        return _unavailable_scheduler_sample(scheduler_job_id, reason)
    if saw_relevant_nonterminal:
        return _unavailable_scheduler_sample(scheduler_job_id, "accounting_lag")

    grouped: dict[tuple[str, str | None], list[_SlurmAccountingRow]] = {}
    for row in rows:
        grouped.setdefault((row.root, row.task), []).append(row)
    # A native array's parent is a bookkeeping aggregate. Task rows are the
    # independent allocations; including the parent would double count CPU.
    task_groups = {key: value for key, value in grouped.items() if key[1] is not None}
    if task_groups:
        grouped = task_groups
    if expected_array_size is not None:
        expected_tasks = {str(index) for index in range(expected_array_size)}
        observed_tasks = {
            task
            for _root, task in grouped
            if task is not None
        }
        if observed_tasks != expected_tasks:
            # sacct can expose terminal rows incrementally.  Until every exact
            # task from the submitted 0..N-1 native array is present, a sum or
            # maximum is not whole-job evidence and must remain retryable.
            return _unavailable_scheduler_sample(
                scheduler_job_id,
                "accounting_lag",
                observed_groups=len(grouped),
            )

    group_cpu: list[float | None] = []
    group_elapsed: list[float | None] = []
    group_rss: list[float | None] = []
    for group_rows in grouped.values():
        allocation = next((row for row in group_rows if row.step is None), None)
        # Without ``sacct -X``, the allocation row can report zero
        # utilization while .batch/.extern/application steps carry the real
        # CPU time. Prefer the sum of available step metrics; use allocation
        # TotalCPU only when the scheduler exposes no step CPU at all.
        step_rows = [row for row in group_rows if row.step is not None]
        step_cpu = [
            row.cpu_seconds
            for row in step_rows
            if row.cpu_seconds is not None
        ]
        cpu = (
            sum(step_cpu)
            if step_rows and len(step_cpu) == len(step_rows)
            else allocation.cpu_seconds
            if not step_rows
            and allocation is not None
            and allocation.cpu_seconds is not None
            else None
        )
        elapsed = allocation.elapsed_seconds if allocation is not None else None
        if elapsed is None:
            step_elapsed = [
                row.elapsed_seconds
                for row in step_rows
                if row.elapsed_seconds is not None
            ]
            elapsed = (
                max(step_elapsed)
                if step_rows and len(step_elapsed) == len(step_rows)
                else None
            )
        step_rss = [row.rss_mb for row in step_rows if row.rss_mb is not None]
        rss_candidates = [
            *step_rss,
            *(
                [allocation.rss_mb]
                if allocation is not None and allocation.rss_mb is not None
                else []
            ),
        ]
        rss = (
            max(rss_candidates)
            if step_rows and len(step_rss) == len(step_rows)
            else allocation.rss_mb
            if not step_rows and allocation is not None
            else None
        )
        group_cpu.append(cpu)
        group_elapsed.append(elapsed)
        group_rss.append(rss)

    try:
        cpu_seconds = (
            math.fsum(value for value in group_cpu if value is not None)
            if group_cpu and all(value is not None for value in group_cpu)
            else None
        )
        rss_mb = (
            max(value for value in group_rss if value is not None)
            if group_rss and all(value is not None for value in group_rss)
            else None
        )
        elapsed_seconds = (
            max(value for value in group_elapsed if value is not None)
            if group_elapsed and all(value is not None for value in group_elapsed)
            else None
        )
        cpu_denominator = (
            math.fsum(value for value in group_elapsed if value is not None)
            if group_elapsed and all(value is not None for value in group_elapsed)
            else 0.0
        )
        cpu_percent = (
            100.0 * cpu_seconds / cpu_denominator
            if cpu_seconds is not None and cpu_denominator > 0
            else None
        )
    except OverflowError as exc:
        raise SchedulerError("non-finite Slurm accounting aggregate") from exc
    numeric_values = (cpu_seconds, rss_mb, elapsed_seconds, cpu_percent)
    if any(value is not None and not math.isfinite(value) for value in numeric_values):
        raise SchedulerError("non-finite Slurm accounting aggregate")
    status = (
        "ok"
        if cpu_seconds is not None
        and cpu_seconds > 0
        and rss_mb is not None
        and rss_mb > 0
        and elapsed_seconds is not None
        else "partial"
    )
    reason = None if status == "ok" else (
        "zero_metrics_unverifiable"
        if cpu_seconds == 0 or rss_mb == 0
        else "metrics_missing"
    )
    return {
        "ts": utcnow_iso(),
        "elapsed_seconds": (
            round(elapsed_seconds, 3) if elapsed_seconds is not None else None
        ),
        "rss_mb": round(rss_mb, 6) if rss_mb is not None else None,
        "cpu_percent": round(cpu_percent, 1) if cpu_percent is not None else None,
        "cpu_time_seconds": (
            round(cpu_seconds, 3) if cpu_seconds is not None else None
        ),
        "cpu_time_source": "slurm-sacct",
        "cgroup_lookup": None,
        "cgroup_path": None,
        "sample_pid": None,
        "sample_pgid": None,
        "scheduler_accounting_status": status,
        "scheduler_accounting_reason": (
            reason
        ),
        "scheduler_accounting_job_id": scheduler_job_id,
        "scheduler_accounting_groups": len(grouped),
    }


def _existing_sample_disposition(
    path: Path,
    scheduler_job_id: str,
) -> str:
    """Return preserve, complete, retry, or missing."""
    try:
        raw = _read_bounded_regular_file(path, SCHEDULER_SAMPLES_MAX_BYTES)
        lines = raw.decode("utf-8").splitlines()
    except FileNotFoundError:
        return "missing"
    except (OSError, UnicodeError, SchedulerError):
        return "preserve"
    records: list[dict[str, object]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except (TypeError, ValueError):
            return "preserve"
        if not isinstance(record, dict):
            return "preserve"
        records.append(record)
    if not records:
        return "missing"
    for record in records:
        if (
            record.get("cpu_time_source") != "slurm-sacct"
            or record.get("scheduler_accounting_job_id") != scheduler_job_id
        ):
            return "preserve"
    if any(
        record.get("scheduler_accounting_status") == "ok"
        for record in records
    ):
        return "complete"
    return "retry"


def _write_scheduler_sample(path: Path, sample: dict[str, object]) -> None:
    """Publish one sample without following job-controlled filesystem links."""
    encoded = (json.dumps(sample, sort_keys=True, allow_nan=False) + "\n").encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    directory_fd = os.open(path.parent, directory_flags)
    temporary_name = f".{path.name}.tmp.{uuid.uuid4().hex}"
    file_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    file_flags |= getattr(os, "O_NOFOLLOW", 0)
    file_fd: int | None = None
    try:
        file_fd = os.open(temporary_name, file_flags, 0o600, dir_fd=directory_fd)
        with os.fdopen(file_fd, "wb") as stream:
            file_fd = None
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(
            temporary_name,
            path.name,
            src_dir_fd=directory_fd,
            dst_dir_fd=directory_fd,
        )
        os.fsync(directory_fd)
    finally:
        if file_fd is not None:
            os.close(file_fd)
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary_name, dir_fd=directory_fd)
        os.close(directory_fd)


def _read_bounded_regular_file(path: Path, max_bytes: int) -> bytes:
    """Read one file through no-follow directory/file descriptors."""
    if max_bytes <= 0:
        raise ValueError("bounded file size must be positive")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_flags |= getattr(os, "O_NOFOLLOW", 0)
    file_flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    file_flags |= getattr(os, "O_NONBLOCK", 0)
    directory_fd: int | None = None
    file_fd: int | None = None
    try:
        directory_fd = os.open(path.parent, directory_flags)
        file_fd = os.open(path.name, file_flags, dir_fd=directory_fd)
        metadata = os.fstat(file_fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > max_bytes:
            raise SchedulerError("bounded scheduler file is not a regular file")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(file_fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        if len(raw) > max_bytes:
            raise SchedulerError("bounded scheduler file exceeded its limit")
        return raw
    finally:
        if file_fd is not None:
            os.close(file_fd)
        if directory_fd is not None:
            os.close(directory_fd)


def _finite_nonnegative_number(payload: dict[str, object], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SchedulerError("malformed scheduler GNU Time receipt")
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise SchedulerError("malformed scheduler GNU Time receipt") from exc
    if not math.isfinite(result) or result < 0:
        raise SchedulerError("malformed scheduler GNU Time receipt")
    return result


def _read_scheduler_resource_receipt(path: Path) -> dict[str, object]:
    try:
        raw = _read_bounded_regular_file(
            path,
            SCHEDULER_RESOURCE_RECEIPT_MAX_BYTES,
        )
    except (OSError, SchedulerError) as exc:
        raise SchedulerError("scheduler GNU Time receipt unavailable") from exc
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, UnicodeError) as exc:
        raise SchedulerError("malformed scheduler GNU Time receipt") from exc
    if not isinstance(payload, dict):
        raise SchedulerError("malformed scheduler GNU Time receipt")
    expected = {
        "schema", "status", "collector", "scope", "command_status",
        "command_exit_code", "wall_seconds", "user_cpu_seconds",
        "system_cpu_seconds", "active_cpu_seconds", "peak_rss_kb",
        "peak_rss_mb",
    }
    if (
        set(payload) != expected
        or payload.get("schema") != RESOURCE_USAGE_SCHEMA
        or payload.get("status") != "ok"
        or payload.get("collector") != "gnu-time"
        or payload.get("scope") != "effective-command"
        or payload.get("command_status") not in {"succeeded", "failed"}
        or isinstance(payload.get("command_exit_code"), bool)
        or not isinstance(payload.get("command_exit_code"), int)
    ):
        raise SchedulerError("malformed scheduler GNU Time receipt")
    for key in (
        "wall_seconds", "user_cpu_seconds", "system_cpu_seconds",
        "active_cpu_seconds", "peak_rss_kb", "peak_rss_mb",
    ):
        _finite_nonnegative_number(payload, key)
    return payload


def _scheduler_resource_receipt_sample(
    local_dir: Path,
    handle: SchedulerHandle,
) -> dict[str, object] | None:
    marker_dir = local_dir / EXIT_MARKER_RELDIR
    base = marker_dir / RESOURCE_USAGE_BASENAME
    if handle.array_size is None:
        paths = [base] if base.exists() or base.is_symlink() else []
    else:
        paths = [base.with_name(f"{base.name}.{index}") for index in range(handle.array_size)]
        if not all(path.exists() or path.is_symlink() for path in paths):
            return None
    if not paths:
        return None
    try:
        receipts = [_read_scheduler_resource_receipt(path) for path in paths]
    except SchedulerError:
        return None
    try:
        cpu_seconds = math.fsum(
            _finite_nonnegative_number(receipt, "active_cpu_seconds")
            for receipt in receipts
        )
        wall_values = [
            _finite_nonnegative_number(receipt, "wall_seconds")
            for receipt in receipts
        ]
        rss_mb = max(
            _finite_nonnegative_number(receipt, "peak_rss_mb")
            for receipt in receipts
        )
        denominator = math.fsum(wall_values)
        cpu_percent = 100.0 * cpu_seconds / denominator if denominator > 0 else None
    except (OverflowError, SchedulerError):
        return None
    if not math.isfinite(cpu_seconds) or (
        cpu_percent is not None and not math.isfinite(cpu_percent)
    ):
        return None
    return {
        "ts": utcnow_iso(),
        "elapsed_seconds": round(max(wall_values), 3),
        "rss_mb": round(rss_mb, 6),
        "cpu_percent": round(cpu_percent, 1) if cpu_percent is not None else None,
        "cpu_time_seconds": round(cpu_seconds, 3),
        "cpu_time_source": "scheduler-gnu-time",
        "cgroup_lookup": None,
        "cgroup_path": None,
        "sample_pid": None,
        "sample_pgid": None,
        "scheduler_accounting_status": "ok",
        "scheduler_accounting_reason": None,
        "scheduler_accounting_job_id": handle.job_id,
        "scheduler_accounting_groups": len(receipts),
    }


class SchedulerError(RuntimeError):
    """A scheduler interaction failed at the transport or protocol level.

    Raised when ``qsub`` / ``qstat`` / ``qdel`` cannot be run or returns output
    the dialect cannot parse, or when staging fails. Distinct from
    :class:`~vq.scheduler_dialect.DialectError` (malformed *values*) only in that
    this one always crossed the SSH boundary; the daemon treats both as a
    dispatch failure and lands the spec FAILED.
    """


class SchedulerRemoteOutcomeUnknown(SchedulerError):
    """The local observer lost a scheduler-host command's outcome."""


class SchedulerSubmitOutcomeUnknown(SchedulerError):
    """A scheduler submit may have been accepted and must not be replayed."""


@dataclass(frozen=True)
class SchedulerPollEvidence:
    """One live scheduler observation plus exact absent-handle evidence.

    ``explicitly_absent_job_ids`` is populated only when the scheduler itself
    returned its narrow, recognized "invalid job id" diagnostic. Transport
    failures, malformed output, and every other non-zero result still raise and
    therefore remain unknown rather than being mistaken for completion.
    """

    phases: dict[str, SchedulerPhase]
    explicitly_absent_job_ids: frozenset[str] = frozenset()


@dataclass(frozen=True)
class RemoteResult:
    """The outcome of one remote command: its rc plus captured streams."""

    returncode: int
    stdout: str
    stderr: str


class RemoteRunner(Protocol):
    """The SSH boundary the dispatcher drives, injectable for tests.

    Command execution plus upload/download primitives carry the complete
    submit, observe, cancel, and result-fetch cycle. The real implementation is
    :class:`SshRemoteRunner`; the test suite supplies a fake that needs no
    network.
    """

    def run(
        self, argv: Sequence[str], *, stdin_data: str | None = None, check: bool = False
    ) -> RemoteResult:
        """Run ``argv`` on the scheduler host (ssh). ``check`` raises on nonzero."""
        ...

    def upload_tree(self, local_dir: Path, remote_dir: str) -> None:
        """Copy the contents of ``local_dir`` into the existing ``remote_dir``."""
        ...

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        """Copy one local file up to ``remote_path`` (scp), no unpacking.

        The staging primitive ``_stage_job`` uses: it lands the workspace
        tarball so a single follow-up call can unpack it, drop it, and write
        the job script together.
        """
        ...

    def download_file(self, remote_path: str, local_path: Path) -> None:
        """Copy a single remote file down to ``local_path`` (scp)."""
        ...


@dataclass(frozen=True)
class SchedulerHandle:
    """Opaque handle for a submitted batch job — the qsub analogue of a pgid.

    Carries everything the dispatcher needs to observe, cancel, and read the rc
    of the job after submission, so the daemon can persist it on the spec and
    reattach across a restart (design doc §5).
    """

    job_id: str
    """The scheduler job id from ``qsub`` (e.g. ``"12345.cluster"`` or, for an
    array, the master id ``"12345[].cluster"``)."""

    remote_workspace: str
    """Absolute path of the staged workspace on the scheduler host. Holds the
    job script, the exit-marker(s), and stdout/stderr logs."""

    array_size: int | None = None
    """Number of array sub-jobs, or ``None`` for a single job."""


@dataclass(frozen=True)
class SchedulerFileChunk:
    """One byte-exact incremental read from a scheduler workspace file."""

    end_offset: int
    data: bytes
    reset: bool = False


@dataclass(frozen=True)
class SchedulerSubmitReceipt:
    """Validated scheduler-side proof for one submit invocation."""

    status: str
    scheduler_job_id: str | None
    scheduler_returncode: int


def _submit_id_candidates(
    dialect: SchedulerDialect,
    stdout: str,
) -> list[str]:
    """Return every exact scheduler-id line in bounded stdout.

    Site wrappers may print a warning after qsub's accepted id. The accepted
    allocation remains real, so selecting only the final line can misclassify
    it as a rejection and replay live work. Parse each nonempty line in
    isolation; callers distinguish one proof from conflicting possible
    allocations.
    """
    matches: list[str] = []
    for line in stdout.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            matches.append(dialect.parse_submit_id(candidate))
        except DialectError:
            continue
    return matches


def _parse_unique_submit_id(
    dialect: SchedulerDialect,
    stdout: str,
) -> str:
    matches = _submit_id_candidates(dialect, stdout)
    if len(matches) != 1:
        raise DialectError("scheduler submit output lacks one unique job id")
    return matches[0]


#: ``tail`` stderr fragments that mean "the file is not there (yet)" — the one
#: non-zero exit that is a legitimate empty read rather than a failed one.
#: Matched case-insensitively against the remote's stderr.
_ABSENT_FILE_STDERR_FRAGMENTS = (
    "no such file or directory",
    "cannot open",
    "cannot stat",
)

_SLURM_INVALID_JOB_POLL_LINE = re.compile(
    r"(?:squeue:\s*error:\s*|slurm_load_jobs\s+error:\s*)?"
    r"invalid job id specified",
    re.IGNORECASE,
)


def _slurm_invalid_job_poll(stderr: str) -> bool:
    """Recognize only Slurm's exact, affirmative unknown-id diagnostic."""
    lines = [line.strip() for line in stderr.splitlines() if line.strip()]
    return bool(lines) and all(
        _SLURM_INVALID_JOB_POLL_LINE.fullmatch(line) is not None
        for line in lines
    )


def _is_absent_file_error(stderr: str) -> bool:
    """Whether a non-zero remote ``tail`` means the file simply is not there.

    Anything else — a permission denial, an I/O error, a stalled mount — is a
    failed read and must be surfaced, not rendered as an empty file.
    """
    lowered = (stderr or "").lower()
    return any(
        fragment in lowered for fragment in _ABSENT_FILE_STDERR_FRAGMENTS
    )


def _workspace_file_path(handle: SchedulerHandle, filename: str) -> str:
    """Join one lexically safe relative filename to a scheduler workspace.

    This blocks absolute and parent-traversal paths. It intentionally retains
    the scheduler workspace's existing trust model for symlinks created by the
    submitted job; the remote host does not expose a no-follow read primitive.
    """
    if not filename or "\x00" in filename:
        raise SchedulerError("scheduler workspace filename must be non-empty")
    relative = PurePosixPath(filename)
    if relative.is_absolute() or relative == PurePosixPath("."):
        raise SchedulerError(
            f"scheduler workspace filename must be relative: {filename!r}"
        )
    if ".." in relative.parts:
        raise SchedulerError(
            f"scheduler workspace filename cannot contain '..': {filename!r}"
        )
    return f"{handle.remote_workspace.rstrip('/')}/{relative.as_posix()}"


def scheduler_handle_for_spec(
    dispatcher: SchedulerDispatcher,
    spec: JobSpec,
    *,
    job_id: str,
) -> SchedulerHandle:
    """Project one persisted vq job into an ordinary scheduler handle.

    The caller supplies ``job_id`` so required, recovered, and cleanup-fallback
    policies stay at their owning call sites. A vq array is persisted as
    independent JobSpecs, so its metadata deliberately does not become a
    native scheduler ``array_size``.
    """
    return SchedulerHandle(
        job_id=job_id,
        remote_workspace=dispatcher.remote_workspace(spec.id),
        array_size=None,
    )


def _phase_for_polled_job(
    job_id: str,
    live: dict[str, SchedulerPhase],
    *,
    dialect_name: str,
) -> SchedulerPhase:
    """Return a phase for ``job_id`` from a possibly lossy scheduler table.

    Torque's default table can abbreviate long server-qualified ids
    (``6528.pbs.cluster`` for ``6528.pbs.cluster.example``). The dispatcher
    still polls by the full id from qsub, so accept a unique listed prefix as
    the same live job.

    SLURM arrays go the other direction: ``sbatch --array`` returns a master id
    such as ``12345``, while ``squeue`` may list element ids like ``12345_0``.
    Treat any listed element of the master as live, using the "most active"
    phase across the elements so one pending/running element keeps the master
    non-terminal. If no row exists, keep the existing "absent means finished"
    contract.
    """
    exact = live.get(job_id)
    if exact is not None:
        return exact
    if dialect_name == "torque":
        # Only a server-qualified Torque id can be a truncated table prefix.
        # Bare numeric prefixes are distinct jobs (123 is not 1234).
        matches = [
            (listed, phase)
            for listed, phase in live.items()
            if "." in listed and job_id.startswith(listed)
        ]
        if len(matches) == 1:
            return matches[0][1]
    elif dialect_name == "slurm":
        # Slurm ids are exact except for the documented master-to-array-row
        # relation. Do not apply Torque's lossy table-prefix compatibility to
        # numeric Slurm ids.
        array_row = re.compile(
            rf"^{re.escape(job_id)}_(?:\d+|\[[0-9,:%-]+\])$"
        )
        array_phases = [
            phase for listed, phase in live.items() if array_row.fullmatch(listed)
        ]
        if array_phases:
            if SchedulerPhase.RUNNING in array_phases:
                return SchedulerPhase.RUNNING
            if SchedulerPhase.PENDING in array_phases:
                return SchedulerPhase.PENDING
            return SchedulerPhase.FINISHED
    return SchedulerPhase.FINISHED


def _same_program(a: str, b: str) -> bool:
    """True when two argv heads name the same remote program.

    Compares the POSIX basename, so the equivalent spellings a site actually
    uses all collapse: ``vibeqc-release-python`` vs
    ``/home/USER/bin/vibeqc-release-python`` vs ``~/bin/vibeqc-release-python``
    vs ``./vibeqc-release-python``. Deliberately *not* ``os.path`` (these are
    remote cluster paths, resolved on a possibly-different OS from the driver)
    and deliberately no symlink resolution (the driver cannot stat the cluster's
    filesystem).
    """
    return bool(a) and bool(b) and PurePosixPath(a).name == PurePosixPath(b).name


def wrapper_already_applied(
    command_wrapper: Sequence[str], command: Sequence[str]
) -> bool:
    """True when ``command`` already launches through ``command_wrapper``.

    The composition contract for ``scheduler_program_hooks.NAME.command_wrapper``
    is documented in :meth:`SchedulerDispatcher.build_job_script` and
    ``docs/operations.md``: the wrapper is an argv prefix that is injected
    *only if the submitted command does not already start with it*.

    Two arms, in order of confidence:

    1. **Exact argv prefix** — ``command[:len(wrapper)] == wrapper``. The
       original guard; covers a payload that was built by prepending the very
       same argv.
    2. **Resolved head** — ``command[0]`` and ``command_wrapper[0]`` name the
       same program under different spellings. This is the arm that matters in
       practice, because a single-file submit's command is *always*
       ``[interpreter, script]``: when the site points both ``branches`` and
       ``command_wrapper`` at one stable launcher (which the config example used
       to recommend), the two spellings differ by an absolute-vs-bare path and
       arm 1 misses. Injecting again hands the launcher its own path as the
       script argument, so the wrapper's python parses the wrapper's shell
       source and dies at ``SyntaxError: set -euo pipefail``. That is the
       2026-07 pbs-cluster release-wrapper incident, ~250 campaign jobs.

    Arm 2 is a heuristic: two genuinely different programs that share a basename
    would be collapsed. That trade is deliberate — a legitimately nested
    same-basename wrapper is a configuration nobody has, while the double-wrap
    it prevents is a configuration the docs recommended — and every skip is
    logged at the call site so the decision is never silent.
    """
    if not command_wrapper or not command:
        return False
    if (
        len(command) >= len(command_wrapper)
        and list(command[: len(command_wrapper)]) == list(command_wrapper)
    ):
        return True
    if _same_program(command_wrapper[0], command[0]):
        return True
    # Arm 3: the wrapper's *last* element names the program. Covers an
    # env-style prefix (`["/usr/bin/env", "vibeqc-release-python"]`) where the
    # launcher is not the argv head. Only fires when that element looks like a
    # program rather than a flag, so `["/site/bin/orcasub", "--scheduler"]`
    # keeps wrapping normally.
    tail = command_wrapper[-1]
    return not tail.startswith("-") and _same_program(tail, command[0])


def _clip_diagnostic_text(text: str) -> str:
    """Bound text captured for event-log forensics."""
    if len(text) <= MISSING_MARKER_DIAGNOSTIC_MAX_CHARS:
        return text
    keep = MISSING_MARKER_DIAGNOSTIC_MAX_CHARS
    return f"...<truncated to last {keep} chars>...\n{text[-keep:]}"


def _skip_unsafe_members(
    member: tarfile.TarInfo, dest_path: str
) -> tarfile.TarInfo | None:
    """The tarfile ``data`` extraction filter, but *skip* any member it would
    reject instead of aborting the whole extraction.

    ``tarfile``'s ``data`` filter is the safe default we want when unpacking a
    fetched remote workspace: it refuses absolute paths, ``..`` traversal,
    absolute or escaping symlinks, and device nodes. But it enforces that by
    *raising* :class:`tarfile.FilterError` on the first offending member, which
    aborts :meth:`~tarfile.TarFile.extractall` entirely — the wrong trade-off
    here, where one stale entry should not cost us every real result file.

    Worse, in ``fetch_results`` that raised ``FilterError`` is not a
    :class:`SchedulerError`, so it escaped the method, propagated out of
    ``daemon._reconcile_scheduler``, and aborted the daemon's whole main-loop
    iteration *before dispatch ran* — starving every pending job on that host.
    (2026-07-16 incident: a killed job's stale ``third_party/*/install``
    build-tree symlinks pointed at an absolute node-local scratch path;
    ``filter="data"`` raised ``AbsoluteLinkError`` on the first one and wedged
    dispatch for the host until the symlinks were removed by hand.)

    Returning ``None`` tells ``extractall`` to drop the member: the unsafe entry
    is never written (the data filter's security property is preserved) and
    extraction continues with the rest of the archive.
    """
    try:
        return tarfile.data_filter(member, dest_path)
    except tarfile.FilterError:
        return None


def _merge_scheduler_workspace(staging: Path, local_dir: Path) -> None:
    """Overlay a safely extracted scheduler workspace onto its local source.

    Directory payloads may be hash-sealed before submission, leaving input
    files mode 0444 in ``local_dir``. Extracting the result archive directly
    over that tree fails on the first repeated input and drops every later
    result. Files are therefore extracted into ``staging`` first and then
    atomically replaced into the writable workspace directory.

    The driver-side event log is the sole exception. Its remote copy predates
    dispatch and status transitions, so replacing a newer local log with that
    staged snapshot would discard queue history. Other ``_vq`` files, including
    the terminal exit marker, remain scheduler-owned artifacts and are merged.
    """
    driver_owned = PurePosixPath("_vq/events.jsonl")
    sources = sorted(
        staging.rglob("*"),
        key=lambda path: (len(path.parts), str(path)),
    )
    for source in sources:
        relative = PurePosixPath(source.relative_to(staging).as_posix())
        destination = local_dir / relative
        if relative == driver_owned and destination.exists():
            continue
        if source.is_dir() and not source.is_symlink():
            if destination.exists() and (
                destination.is_symlink() or not destination.is_dir()
            ):
                raise OSError(
                    "scheduler result directory conflicts with local file: "
                    f"{relative}"
                )
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if (
            destination.exists()
            and destination.is_dir()
            and not destination.is_symlink()
        ):
            raise OSError(
                "scheduler result file conflicts with local directory: "
                f"{relative}"
            )
        source.replace(destination)


def _extract_scheduler_workspace(archive: Path, local_dir: Path) -> None:
    """Safely stage then merge one complete scheduler result archive."""
    with tempfile.TemporaryDirectory(
        dir=local_dir.parent,
        prefix=f".{local_dir.name}.vq-fetch-",
    ) as staging_name:
        staging = Path(staging_name)
        with tarfile.open(archive) as tf:
            tf.extractall(staging, filter=_skip_unsafe_members)
        _merge_scheduler_workspace(staging, local_dir)


_SCRIPT_SUFFIXES = (".py", ".pyw")


def _refuse_launcherless_script(
    composed: Sequence[str],
    *,
    program: str | None,
    job_id: str | None,
) -> None:
    """Fail closed when a Python payload would run with no interpreter.

    pbs-cluster, 2026-07-26 (job 04b5d4b0b46c, PBS 17614): vq pinned
    ``vibeqc-release`` to immutable v0.15.64 correctly, then generated a PBS
    script whose command was the bare payload::

        run.py

    Torque ran it directly -- ``line 31: run.py: command not found``, exit 127 --
    while the matched ORCA job (9e2f3dc15a78) completed normally. The cause is
    structural: :meth:`SchedulerDispatcher._compose_command` prepends
    ``scheduler_program_hooks.NAME.command_wrapper``, and pbs-cluster had wrappers
    registered for ORCA and vibe-view but none for ``vibeqc-release``. With no
    hook the wrapper is ``[]`` and the composed argv is the payload alone, so a
    script that cannot execute itself was submitted with no launcher and no
    complaint.

    A ``.py`` head can only run through an interpreter. Refusing here -- before
    any qsub -- turns a silent exit-127 an hour later into an immediate, named
    error, and cannot affect ORCA (a binary) or vibe-view (hook registered).

    The remedy is ``--branch`` / ``--python``, NOT a new ``command_wrapper``.
    pbs-cluster's ``[hosts.pbs-cluster.branches] release`` already maps to its pinned release
    wrapper; the failing submit simply named neither. pbs-cluster's
    ``vibeqc-release``/``-dev`` ``command_wrapper`` hooks were deliberately
    removed on 2026-07-21 because pointing both a branch/``--python`` and a
    wrapper at one launcher double-wraps it -- see
    :func:`wrapper_already_applied` on that same incident. A wrapper is right
    only for a payload carrying no interpreter of its own, such as a ``--dir``
    submit, which is why ORCA and vibe-view keep theirs.

    This is the safety half only: it makes an incomplete submit loud rather
    than supplying the interpreter itself. Whether vq should additionally derive
    a launcher from the scheduler-runtime record's verified ``active_path`` is
    tracked in the vq open-bug record -- but note it must not fight the
    branch/wrapper mechanisms that already exist.
    """
    # Scoped to a NAMED program, i.e. a job vq is supposed to be routing
    # through a managed runtime. A programless submit is the caller hand-rolling
    # its own command line -- a bare `.py` head there may legitimately rely on a
    # shebang plus the exec bit, and one supported SLURM directory-payload path
    # already does exactly that. Refusing it would break a working case for no
    # gain, since there is no pinned runtime to route through in the first place.
    if program is None or not composed:
        return
    head = composed[0]
    if not head.endswith(_SCRIPT_SUFFIXES):
        return
    raise SchedulerError(
        f"refusing to submit job {job_id or '(unassigned)'} for program "
        f"{program!r}: the composed command starts with {head!r}, which is a "
        "script and not an executable, so the compute node would run it with "
        "no interpreter (exit 127). Resubmit "
        "naming the runtime interpreter -- `--branch release` (or the alias "
        "`latest`), or an explicit `--python`; a scheduler host's "
        "`[hosts.<host>.branches]` maps those to its pinned per-program "
        "wrapper. Registering a `scheduler_program_hooks.<program>."
        "command_wrapper` is the alternative ONLY for a payload that supplies "
        "no interpreter of its own, such as a `--dir` submit: pointing both a "
        "branch/--python and a command_wrapper at the same launcher "
        "double-wraps it, which is why pbs-cluster's vibeqc-release/-dev hooks were "
        "removed on 2026-07-21. vq did not submit anything."
    )


class SchedulerDispatcher:
    """Submit, observe, and cancel jobs via an external scheduler over SSH.

    One dispatcher is bound to one scheduler host (its ``RemoteRunner``) and one
    :class:`SchedulerDialect`. The daemon constructs it per scheduler-host from
    the :class:`~vq.config.HostConfig` (``scratch_root`` + ``submit_extra``).
    """

    def __init__(
        self,
        dialect: SchedulerDialect,
        runner: RemoteRunner,
        *,
        scratch_root: str,
        submit_extra: Sequence[str] = (),
        job_root: str = ".vibeqc-cluster/jobs",
        node_scratch_dir: str | None = None,
        scheduler_prologue: Sequence[str] = (),
        scheduler_epilogue: Sequence[str] = (),
        scheduler_program_hooks: Mapping[str, SchedulerProgramHooks] | None = None,
        mem_directive: str = "request",
        gnu_time_command: str | None = None,
        max_wall_time_seconds: int | None = None,
    ) -> None:
        self.dialect = dialect
        self.runner = runner
        # "request" renders mem_mb as the dialect's memory directive; "omit"
        # drops the directive (keeping mem_mb for vq accounting + VQ_MEM_MB)
        # because Torque moms apply -l mem= as a hard per-process RLIMIT_DATA
        # that kills tightly-sized payloads with bad_alloc (BUG 118). See
        # HostConfig.scheduler_mem_directive.
        self.mem_directive = mem_directive
        # This remains a hard, fail-closed dependency of scheduler telemetry;
        # only its exact compute-node path is site-configurable.
        self.gnu_time_command = (
            GNU_TIME_COMMAND if gnu_time_command is None else gnu_time_command
        )
        self.max_wall_time_seconds = max_wall_time_seconds
        self.scratch_root = scratch_root.rstrip("/")
        self.job_root = job_root.strip("/")
        # Optional node-local scratch (e.g. ``/tmp1/$USER`` on pbs-cluster). When set,
        # a job runs in a fresh per-task dir on the compute node's local disk
        # and copies all output back to the shared /home workspace, sparing NFS
        # the heavy intermediate I/O (the pbs-cluster ``csub`` idiom). A trusted,
        # shell-expanded config string, NOT shlex-quoted, so ``$USER`` and the
        # like resolve on the node. None ⇒ run directly in the /home workspace.
        self.node_scratch_dir = node_scratch_dir
        # Trusted site hook lines rendered verbatim into every qsub script.
        # They run inside the job working directory; epilogue lines see
        # ``__vq_rc`` already captured so accidental command failures do not
        # overwrite the user's rc unless the hook deliberately changes it.
        self.scheduler_prologue = list(scheduler_prologue)
        self.scheduler_epilogue = list(scheduler_epilogue)
        self.scheduler_program_hooks = dict(scheduler_program_hooks or {})
        # Incremental workspace readers retain a bounded fingerprint of the
        # bytes immediately preceding their last cursor. It detects the common
        # copytruncate-and-regrow case even when the replacement reaches the
        # old size between polls, without hashing an ever-growing whole file.
        self._tail_boundary_tokens: dict[tuple[str, str], tuple[int, str]] = {}
        # Per-host, not per-job: see _ensure_job_root.
        self._job_root_ready = False
        # Resolve the site queue / account / extra directives ONCE, from
        # submit_extra, and render them as #PBS directives in every job script.
        # They are never also passed as qsub argv (review note 1).
        self._queue, self._account, self._extra_directives = parse_submit_extra(
            submit_extra
        )

    @property
    def accounting_required_for_absent(self) -> bool:
        """Whether a missing live-poll row needs accounting confirmation.

        Slurm's ``squeue`` reports only live jobs, so a successful empty result
        is not itself terminal proof. ``sacct -X`` must corroborate the state.
        Torque retains its historical qstat/exit-marker contract because its
        accounting availability and nonzero partial-result behaviour differ.
        """
        return self.dialect.name == "slurm"

    # -- paths --------------------------------------------------------------

    def remote_workspace(self, job_id: str) -> str:
        """The absolute remote workspace dir for ``job_id``."""
        return f"{self.scratch_root}/{self.job_root}/{job_id}"

    def _submit_receipt_path(self, job_id: str) -> str:
        """Return the staged-archive-external receipt adjacent to the workspace."""
        return f"{self.remote_workspace(job_id)}.submit-once.json"

    def _write_submit_receipt(
        self,
        *,
        job_id: str,
        status: str,
        scheduler_job_id: str | None,
        returncode: int,
    ) -> None:
        """Best-effort atomic publication of one versioned submit receipt.

        The scheduler result itself remains authoritative: a valid returned id
        proves acceptance and a committed ordinary nonzero return code proves
        rejection even if this secondary write is interrupted. The receipt is
        for restart reconciliation across the qsub/spec-write crash window.
        """
        receipt = {
            "schema": SCHEDULER_SUBMIT_RECEIPT_SCHEMA,
            "status": status,
            "vq_job_id": job_id,
            "scheduler_job_id": scheduler_job_id,
            "scheduler_returncode": returncode,
        }
        payload = json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n"
        path = self._submit_receipt_path(job_id)
        script = (
            "umask 077; path=$1; "
            'tmpdir=$(mktemp -d "${path}.publish.XXXXXXXX") || exit 1; '
            'trap \'rm -rf -- "$tmpdir"\' EXIT HUP INT TERM; '
            'tmp=${tmpdir}/receipt; cat > "$tmp" && mv -f -- "$tmp" "$path"'
        )
        try:
            result = self.runner.run(
                ["sh", "-c", script, "vq-submit-receipt", path],
                stdin_data=payload,
                check=False,
            )
        except Exception:  # noqa: BLE001 - scheduler proof above remains authoritative
            log.warning("job %s: could not publish scheduler submit receipt", job_id)
            return
        if result.returncode != 0:
            log.warning(
                "job %s: scheduler submit receipt publication exited %d",
                job_id,
                result.returncode,
            )

    def cleanup_remote_workspace(self, handle: SchedulerHandle) -> None:
        """Remove a terminal job's scheduler-side workspace.

        Result fetch leaves the remote workspace intact so transient copy-back
        failures can be retried without losing data. Retention cleanup calls
        this later, after the local workspace has been archived or the job is
        being deleted. The path guard keeps the reaper inside vq's configured
        scheduler job root even if an old/corrupt spec carries surprising
        metadata.
        """
        workspace = handle.remote_workspace.rstrip("/")
        jobs_root = PurePosixPath(self.scratch_root) / self.job_root
        workspace_path = PurePosixPath(workspace)
        if (
            workspace != str(workspace_path)
            or workspace_path.parent != jobs_root
            or workspace_path.name in {"", ".", ".."}
        ):
            raise SchedulerError(
                "refusing to clean scheduler workspace outside vq job root: "
                f"{handle.remote_workspace}"
            )
        # The submit-once receipt is workspace-adjacent and therefore excluded
        # from the staged archive. Retention still has to remove both exact
        # names; a wildcard would risk crossing job boundaries.
        receipt = f"{workspace}.submit-once.json"
        result = self.runner.run(
            ["rm", "-rf", "--", workspace, receipt],
            check=False,
        )
        if result.returncode != 0:
            raise SchedulerError(
                f"failed to clean remote workspace for {handle.job_id} "
                f"at {workspace} (exit {result.returncode}): "
                f"{result.stderr.strip() or '(empty stderr)'}"
            )

    def _marker_path(self, remote_workspace: str, array_index: int | None) -> str:
        """Exit-marker path for a job (or one array sub-job).

        Single job → ``<ws>/_vq/exit-code``. Array sub-job ``i`` →
        ``<ws>/_vq/exit-code.<i>`` (review note 3 — per-sub-job rc).
        """
        base = f"{remote_workspace}/{EXIT_MARKER_RELDIR}/{EXIT_MARKER_BASENAME}"
        return base if array_index is None else f"{base}.{array_index}"

    # -- script rendering ---------------------------------------------------

    @property
    def host_label(self) -> str:
        """Short name for this dispatcher's target, for log lines."""
        runner = self.runner
        host_cfg = getattr(runner, "host_cfg", None)
        return getattr(host_cfg, "ssh", None) or "scheduler"

    def effective_command(
        self,
        command: Sequence[str],
        program: str | None,
        *,
        job_id: str | None = None,
        warn: bool = True,
    ) -> list[str]:
        """Compose the argv the job will actually run.

        The single source of truth for the ``command_wrapper`` composition
        contract, shared by the script renderer and the pre-submit log line, so
        what gets logged can never drift from what gets run.

        See :func:`wrapper_already_applied`: a scheduler hook owns wrapper
        injection, but the payload command may already start with that same
        wrapper — a single-file submit's command is always
        ``[interpreter, script]``, and a site pointing both ``branches`` and
        ``command_wrapper`` at one launcher produces exactly that. Blindly
        prepending makes the launcher parse its own shell source as Python
        (the pbs-cluster release-wrapper incident), so injection is idempotent.
        """
        hooks = (
            self.scheduler_program_hooks.get(program) if program is not None else None
        )
        command_wrapper = hooks.command_wrapper if hooks is not None else []
        if wrapper_already_applied(command_wrapper, command):
            # `warn=False` for the pre-submit log line, which composes the argv
            # a second time purely to report it. Without this every skipped
            # wrapper logged twice, once naming `job None`.
            if not warn:
                return list(command)
            log.warning(
                "job %s (program %s): scheduler command_wrapper %r not injected "
                "— the submitted command already launches through %r. Configure "
                "either the interpreter or the wrapper, not both; run "
                "`vq doctor` for the full verdict.",
                job_id,
                program,
                command_wrapper[0],
                command[0],
            )
            return list(command)
        composed = [*command_wrapper, *command]
        _refuse_launcherless_script(composed, program=program, job_id=job_id)
        return composed

    def build_job_script(
        self,
        *,
        job_id: str,
        command: Sequence[str],
        remote_workspace: str,
        cpus: int,
        scheduler_tasks: int | None = None,
        mem_mb: int | None = None,
        wall_time_seconds: int | None = None,
        array_size: int | None = None,
        env: dict[str, str] | None = None,
        program: str | None = None,
    ) -> str:
        """Render the submittable ``#PBS`` job script for one submission.

        Exports any env, runs the user command, captures its rc, and writes the
        rc to the exit-marker -- array-aware via the scheduler's array index
        environment variable -- before exiting with it. The exit-marker always
        lives on the shared workspace (the rc source of truth, since scheduler
        accounting can linger only briefly and may be aggregate/first-match for
        arrays); this is the scheduler analogue of the daemon's local exit-wrap
        (``_build_wrapped_command``).

        Execution location depends on :attr:`node_scratch_dir`: unset ⇒ run in
        the staged ``/home`` workspace; set ⇒ run in a fresh per-task dir on the
        node's local disk and reconcile new/changed output against a private
        seed copy on normal command fallthrough before recording rc. Conflicting
        shared writes fail visibly with a retained scratch recovery copy. The
        latter spares NFS the heavy intermediate I/O. A
        TERM, INT, or HUP trap writes the shared exit marker and exits before
        copy-back and scratch cleanup; only the separately redirected shared
        stdout/stderr logs are guaranteed to have been visible in that path.

        **command_wrapper composition contract.** When ``program`` matches a
        ``scheduler_program_hooks`` entry with a ``command_wrapper``, that argv
        is the *outermost* launcher and is prepended to the submitted command —
        but only if the command does not already start with it, by exact argv
        prefix or by resolved program head (:func:`wrapper_already_applied`).
        The wrapper and the job's interpreter are therefore alternatives, never
        a pair: configure a program's launcher **either** as the submitted
        interpreter (``--python`` / ``[hosts.HOST.branches]``) **or** as
        ``command_wrapper``, not both. A skipped injection is logged as a
        warning and reported by ``vq doctor``.
        """
        req = ResourceRequest(
            cpus=cpus,
            scheduler_tasks=scheduler_tasks,
            mem_mb=None if self.mem_directive == "omit" else mem_mb,
            wall_time_seconds=wall_time_seconds,
            queue=self._queue,
            account=self._account,
            job_name=job_id,
            # #PBS -o/-e go to a throwaway job-level spool: Torque only writes
            # these at job END, so they are useless for live tailing. The job
            # script instead redirects the user command straight to
            # stdout.log/stderr.log on the NFS workspace (below), which IS
            # tailable mid-run (§18 item 5). Keeping the spool in the workspace
            # (not the default `~/<name>.o<id>`) avoids littering the home dir.
            stdout_path=f"{remote_workspace}/.pbs-spool.out",
            stderr_path=f"{remote_workspace}/.pbs-spool.err",
            array_size=array_size,
            extra_directives=self._extra_directives,
        )
        directives = self.dialect.resource_directives(req)
        program_hooks = (
            self.scheduler_program_hooks.get(program) if program is not None else None
        )
        program_prologue = program_hooks.prologue if program_hooks is not None else []
        program_epilogue = program_hooks.epilogue if program_hooks is not None else []
        effective_command = self.effective_command(command, program, job_id=job_id)

        ws_q = shlex.quote(remote_workspace)
        marker_dir = f"{remote_workspace}/{EXIT_MARKER_RELDIR}"
        single_marker = f"{marker_dir}/{EXIT_MARKER_BASENAME}"
        scheduler_id_path = f"{marker_dir}/{SCHEDULER_ID_BASENAME}"
        stdout_base = f"{remote_workspace}/{STDOUT_LOG}"
        stderr_base = f"{remote_workspace}/{STDERR_LOG}"
        # The user command's stdout/stderr are redirected straight to these NFS
        # files so a `vq logs --follow` can tail them mid-run (§18 item 5).
        run = (
            f'"$__vq_time" -f {shlex.quote(_GNU_TIME_FORMAT)} '
            f'-o "$__vq_resource_raw" -- {shlex.join(effective_command)} '
            '> "$__vq_stdout" 2> "$__vq_stderr"'
        )
        body: list[str] = [
            "set -u",
            f"mkdir -p {shlex.quote(marker_dir)}",
            (
                f"[ ! -L {shlex.quote(remote_workspace)} ] && "
                f"[ -d {shlex.quote(remote_workspace)} ] && "
                f"[ ! -L {shlex.quote(marker_dir)} ] && "
                f"[ -d {shlex.quote(marker_dir)} ] || exit 125"
            ),
        ]
        # Array sub-jobs land their rc + logs under per-index names; single jobs
        # use the bare names. The dialect tells us the scheduler's array-index
        # env var (PBS_ARRAYID for Torque, SLURM_ARRAY_TASK_ID for SLURM). All
        # three paths are on the shared workspace (the marker is the rc source
        # of truth; the logs are live-tailable).
        array_env = self.dialect.array_index_env
        body.append(
            f'if [ -n "${{{array_env}:-}}" ]; then '
            f"__vq_marker={shlex.quote(single_marker)}.${{{array_env}}}; "
            f"__vq_stdout={shlex.quote(stdout_base)}.${{{array_env}}}; "
            f"__vq_stderr={shlex.quote(stderr_base)}.${{{array_env}}}; "
            f"else __vq_marker={shlex.quote(single_marker)}; "
            f"__vq_stdout={shlex.quote(stdout_base)}; "
            f"__vq_stderr={shlex.quote(stderr_base)}; fi"
        )
        resource_usage_base = f"{marker_dir}/{RESOURCE_USAGE_BASENAME}"
        body.append(
            f'if [ -n "${{{array_env}:-}}" ]; then '
            f"__vq_resource_usage={shlex.quote(resource_usage_base)}.${{{array_env}}}; "
            f"else __vq_resource_usage={shlex.quote(resource_usage_base)}; fi"
        )
        body.append('__vq_resource_raw="$__vq_resource_usage.time.$$"')
        body.append('__vq_resource_tmp="$__vq_resource_usage.tmp.$$"')
        body.append(f"__vq_time={shlex.quote(self.gnu_time_command)}")
        body.append("__vq_resource_finalized=0")
        body.append(
            '__vq_write_marker() { printf "%s\\n" "$1" > "$__vq_marker" '
            "2>/dev/null || true; }"
        )
        # #414 (ask 2): the exit-marker must not record 0 for a killed
        # payload. GNU time 1.7 — still the /usr/bin/time on older cluster
        # nodes — exits with WEXITSTATUS(status) even when the command died
        # from a signal (which is 0), reporting the kill only as a "Command
        # terminated by signal N" line inside its -o output. A payload
        # SIGKILLed by the kernel OOM killer therefore reached the marker as
        # rc=0. Recover the signal from the raw collector output (a contract
        # stable across GNU time 1.7 and 1.9) and record the shell convention
        # 128+N instead. Strictly a false-zero corrector: any nonzero rc is
        # already truthful and passes through untouched.
        signal_line_awk = (
            "/^Command terminated by signal [0-9]+$/ { sig = $NF } "
            'END { if (sig != "") print sig }'
        )
        body.append(
            "__vq_recover_signal_rc() { "
            '[ "$__vq_rc" -eq 0 ] || return 0; '
            '[ -f "$__vq_resource_raw" ] || return 0; '
            f"__vq_kill_signal=$(LC_ALL=C awk {shlex.quote(signal_line_awk)} "
            '"$__vq_resource_raw" 2>/dev/null) || return 0; '
            '[ -n "$__vq_kill_signal" ] || return 0; '
            "__vq_rc=$((128 + __vq_kill_signal)); }"
        )
        error_format = (
            '{\\n  "schema": "'
            + RESOURCE_USAGE_SCHEMA
            + '",\\n  "status": "error",\\n  "collector": "gnu-time",'
            + '\\n  "scope": "effective-command",\\n  "error": "%s",'
            + '\\n  "command_status": "%s",\\n  "command_exit_code": %s,'
            + '\\n  "wall_seconds": null,\\n  "user_cpu_seconds": null,'
            + '\\n  "system_cpu_seconds": null,'
            + '\\n  "active_cpu_seconds": null,\\n  "peak_rss_kb": null,'
            + '\\n  "peak_rss_mb": null\\n}\\n'
        )
        body.extend(
            [
                "__vq_write_resource_error() {",
                '  __vq_resource_error="$1"',
                '  __vq_resource_command_status="$2"',
                '  __vq_resource_rc="$3"',
                (
                    f"  printf {shlex.quote(error_format)} "
                    '"$__vq_resource_error" "$__vq_resource_command_status" '
                    '"$__vq_resource_rc" > "$__vq_resource_tmp"'
                ),
                '  mv -f "$__vq_resource_tmp" "$__vq_resource_usage"',
                "}",
                "__vq_finalize_resource_usage() {",
                '  [ "$__vq_resource_finalized" -eq 0 ] || return 0',
                "  __vq_resource_finalized=1",
                '  __vq_resource_rc="$1"',
                "  __vq_resource_command_status=failed",
                (
                    '  [ "$__vq_resource_rc" -ne 0 ] || '
                    "__vq_resource_command_status=succeeded"
                ),
                (
                    "  if LC_ALL=C awk "
                    f"-v sentinel={shlex.quote(_GNU_TIME_SENTINEL)} "
                    f"-v schema={shlex.quote(RESOURCE_USAGE_SCHEMA)} "
                    '-v rc="$__vq_resource_rc" '
                    '-v command_status="$__vq_resource_command_status" '
                    f"{shlex.quote(_RESOURCE_USAGE_AWK)} "
                    '"$__vq_resource_raw" > "$__vq_resource_tmp" 2>/dev/null; then'
                ),
                '    mv -f "$__vq_resource_tmp" "$__vq_resource_usage"',
                "  else",
                '    rm -f "$__vq_resource_tmp"',
                (
                    "    __vq_write_resource_error invalid_gnu_time_output "
                    '"$__vq_resource_command_status" "$__vq_resource_rc" || true'
                ),
                "  fi",
                '  rm -f "$__vq_resource_raw"',
                "}",
                "__vq_fail_resource_collector() {",
                (
                    '  __vq_write_resource_error "$1" not_run null '
                    ">/dev/null 2>&1 || true"
                ),
                (
                    "  printf 'vq: scheduler resource telemetry unavailable (%s); "
                    "command not started\\n' \"$1\" > \"$__vq_stderr\""
                ),
                f"  __vq_write_marker {RESOURCE_COLLECTOR_FAILURE_EXIT_CODE}",
                f"  exit {RESOURCE_COLLECTOR_FAILURE_EXIT_CODE}",
                "}",
            ]
        )
        # Record the scheduler's own id for the driver to recover from. Only for
        # a single job: an array element's id carries its index (PBS
        # `18109[3].host`), which is not the parent handle vq holds, and every
        # sub-job would race to write the same file.
        id_env = self.dialect.job_id_env
        if array_size is None:
            body.extend(
                [
                "__vq_record_scheduler_id() {",
                (
                    f"  [ ! -L {shlex.quote(marker_dir)} ] && "
                    f"[ -d {shlex.quote(marker_dir)} ] || return 1"
                ),
                (
                    f"  [ ! -e {shlex.quote(scheduler_id_path)} ] && "
                    f"[ ! -L {shlex.quote(scheduler_id_path)} ] || return 1"
                ),
                (
                    "  __vq_scheduler_id_tmp="
                    f"{shlex.quote(marker_dir + '/.scheduler-job-id.')}$$"
                ),
                (
                    '  (set -C; : > "$__vq_scheduler_id_tmp") '
                    "2>/dev/null || return 1"
                ),
                '  chmod 600 "$__vq_scheduler_id_tmp" 2>/dev/null || true',
                (
                    f'  if printf "%s\\n" "${{{id_env}}}" > '
                    '"$__vq_scheduler_id_tmp" && '
                    f'ln "$__vq_scheduler_id_tmp" {shlex.quote(scheduler_id_path)} '
                    "2>/dev/null; then"
                ),
                '    rm -f -- "$__vq_scheduler_id_tmp"',
                "    return 0",
                "  fi",
                '  rm -f -- "$__vq_scheduler_id_tmp"',
                "  return 1",
                "}",
                (
                    f'if [ -n "${{{id_env}:-}}" ]; then '
                    "__vq_record_scheduler_id || true; fi"
                ),
                ]
            )
        body.append(
            '__vq_term() { __vq_rc="$1"; '
            'if [ "${__vq_lock_held:-0}" = 1 ]; then rmdir "$__vq_lock"; fi; '
            '__vq_finalize_resource_usage "$__vq_rc"; '
            '__vq_write_marker "$__vq_rc"; exit "$__vq_rc"; }'
        )
        body.append("trap '__vq_term 143' TERM")
        body.append("trap '__vq_term 130' INT")
        body.append("trap '__vq_term 129' HUP")
        body.extend(self._env_exports(env))
        body.append(
            'rm -f "$__vq_resource_usage" "$__vq_resource_raw" '
            '"$__vq_resource_tmp"'
        )
        body.append(
            '[ -x "$__vq_time" ] || '
            "__vq_fail_resource_collector gnu_time_missing"
        )
        body.append(
            '__vq_time_banner=$("$__vq_time" --version 2>/dev/null) || '
            "__vq_fail_resource_collector gnu_time_version_probe_failed"
        )
        body.append(
            'case "$__vq_time_banner" in *"GNU Time"*) ;; *) '
            "__vq_fail_resource_collector gnu_time_incompatible ;; esac"
        )
        body.append(
            "command -v awk >/dev/null 2>&1 || "
            "__vq_fail_resource_collector awk_missing"
        )
        body.append(
            f'if ! "$__vq_time" -f {shlex.quote(_GNU_TIME_FORMAT)} '
            '-o "$__vq_resource_raw" -- true >/dev/null 2>&1; then '
            "__vq_fail_resource_collector gnu_time_probe_failed; fi"
        )
        body.append(
            "if ! LC_ALL=C awk "
            f"-v sentinel={shlex.quote(_GNU_TIME_SENTINEL)} "
            f"{shlex.quote(_RESOURCE_USAGE_PROBE_AWK)} "
            '"$__vq_resource_raw" >/dev/null 2>&1; then '
            "__vq_fail_resource_collector gnu_time_output_incompatible; fi"
        )
        body.append('rm -f "$__vq_resource_raw"')

        if self.node_scratch_dir is None:
            body.append(f"cd {ws_q}")
            body.extend(self.scheduler_prologue)
            body.extend(program_prologue)
            body.append(run)
            body.append("__vq_rc=$?")
            body.append("__vq_recover_signal_rc")
            body.extend(program_epilogue)
            body.extend(self.scheduler_epilogue)
        else:
            # node_scratch_dir is a raw, shell-expanded snippet (e.g.
            # /tmp1/$USER) -- intentionally NOT shlex-quoted. mktemp -d is atomic
            # so concurrent array sub-jobs get distinct scratch dirs. Heavy
            # intermediate I/O stays node-local; only the small live logs and the
            # final copy-back touch NFS.
            nsd = self.node_scratch_dir
            body.extend(_NODE_SCRATCH_RECONCILE.strip().splitlines())
            body.append(f"mkdir -p {nsd} || __vq_term 125")
            body.append(f"__vq_stage=$(mktemp -d {nsd}/vq-XXXXXX) || __vq_term 125")
            body.append('__vq_scratch="$__vq_stage/work"; __vq_seed="$__vq_stage/seed"')
            body.append('mkdir "$__vq_scratch" "$__vq_seed" || __vq_term 125')
            # Exclude live wrapper-owned artifacts even when the payload was
            # submitted from a previously completed workspace.
            body.append(f'for __vq_item in {ws_q}/* {ws_q}/.[!.]* {ws_q}/..?*; do')
            body.append('  [ -e "$__vq_item" ] || [ -L "$__vq_item" ] || continue')
            body.append('  __vq_reserved "${__vq_item##*/}" && continue')
            body.append(
                '  cp -a "$__vq_item" "$__vq_seed"/ '
                '2>> "$__vq_stderr" || __vq_term 125'
            )
            body.append('done')
            body.append('cp -a "$__vq_seed"/. "$__vq_scratch"/ || __vq_term 125')
            body.append('cd "$__vq_scratch"')
            body.extend(self.scheduler_prologue)
            body.extend(program_prologue)
            body.append(run)
            body.append("__vq_rc=$?")
            body.append("__vq_recover_signal_rc")
            body.extend(program_epilogue)
            body.extend(self.scheduler_epilogue)
            # Telemetry describes the payload, even if output publication fails.
            body.append('__vq_finalize_resource_usage "$__vq_rc"')
            # Reconcile before recording completion. On conflict retain the
            # shared version and archive scratch for recovery, never success.
            body.append(f'if ! __vq_reconcile {ws_q} 2>> "$__vq_stderr"; then')
            body.append('  __vq_rc=125')
            body.append(f'  __vq_recovery=$(mktemp -d {ws_q}/_vq/scratch-recovery-XXXXXX)')
            body.append(
                '  if [ -n "$__vq_recovery" ] && '
                'cp -a "$__vq_scratch"/. "$__vq_recovery"/; then'
            )
            body.append(
                '    printf "vq: scratch output retained at %s\\n" '
                '"$__vq_recovery" >> "$__vq_stderr"'
            )
            body.append('  else')
            body.append(
                '    printf "vq: recovery copy failed; node scratch retained at %s\\n" '
                '"$__vq_stage" >> "$__vq_stderr"'
            )
            body.append('    __vq_keep_scratch=1')
            body.append('  fi')
            body.append('fi')

        body.append('__vq_finalize_resource_usage "$__vq_rc"')
        body.append('__vq_write_marker "$__vq_rc"')
        if self.node_scratch_dir is not None:
            body.append('[ "${__vq_keep_scratch:-0}" = 1 ] || rm -rf "$__vq_stage"')
        body.append('exit "$__vq_rc"')
        return self.dialect.render_job_script(directives, body)

    @staticmethod
    def _env_exports(env: dict[str, str] | None) -> list[str]:
        """``export K=V`` lines for the job script, sorted for determinism."""
        if not env:
            return []
        return [f"export {k}={shlex.quote(v)}" for k, v in sorted(env.items())]

    # -- submit -------------------------------------------------------------

    def submit(
        self,
        *,
        job_id: str,
        command: Sequence[str],
        cpus: int,
        scheduler_tasks: int | None = None,
        mem_mb: int | None = None,
        wall_time_seconds: int | None = None,
        array_size: int | None = None,
        env: dict[str, str] | None = None,
        local_workspace: Path | None = None,
        program: str | None = None,
        retry_attempt: bool = False,
    ) -> SchedulerHandle:
        """Stage, submit, and return a handle for one job.

        Steps: ensure the remote workspace dir; stage ``local_workspace`` into it
        if given; render + write the job script; ``qsub`` it; parse the scheduler
        id. Raises :class:`SchedulerError` on any transport or parse failure (the
        script never reaches the queue).
        """
        enforce_scheduler_wall_time_limit(
            wall_time_seconds,
            self.max_wall_time_seconds,
            scheduler_host=self.host_label,
            partition=self._queue,
        )
        if local_workspace is not None:
            # Defense in depth for legacy specs and non-CLI callers. New
            # submissions validate before creating their local spec/workspace;
            # this boundary still refuses an invalid staged Python command
            # before the first remote mkdir/upload or scheduler mutation.
            from vq.submit import (  # noqa: PLC0415
                PayloadValidationError,
                validate_staged_python_entrypoint,
            )

            try:
                effective_command = self.effective_command(
                    command,
                    program,
                    warn=False,
                )
                validate_staged_python_entrypoint(
                    command=command,
                    source_directory=local_workspace,
                )
                if list(command) != effective_command:
                    validate_staged_python_entrypoint(
                        command=effective_command,
                        source_directory=local_workspace,
                    )
            except PayloadValidationError as exc:
                raise SchedulerError(str(exc)) from exc
            except OSError as exc:
                raise SchedulerError(
                    "payload validation failed: staged workspace is "
                    "unavailable or unreadable before scheduler dispatch"
                ) from exc
        ws = self.remote_workspace(job_id)
        script = self.build_job_script(
            job_id=job_id,
            command=command,
            remote_workspace=ws,
            cpus=cpus,
            scheduler_tasks=scheduler_tasks,
            mem_mb=mem_mb,
            wall_time_seconds=wall_time_seconds,
            array_size=array_size,
            env=env,
            program=program,
        )
        script_path = f"{ws}/job.pbs"
        # What the job will actually run, at INFO, before it is submitted.
        # The rendered script is piped straight to the cluster and only comes
        # home via `fetch_results` after the job reaches a terminal state — so
        # a job that dies IN its launcher left nothing locally inspectable.
        # That is why the command_wrapper double-wrap stayed invisible for a
        # week while ~250 pbs-cluster jobs failed identically: the one line below
        # would have shown `…/vibeqc-release-python vibeqc-release-python
        # run.py` on the very first dispatch.
        log.info(
            "job %s: submitting to %s (program=%s) — run line: %s",
            job_id,
            self.host_label,
            program,
            shlex.join(self.effective_command(command, program, warn=False)),
        )
        # The full script at DEBUG. Not INFO: it carries the job's exported
        # environment, which is job-controlled content in a shared log.
        log.debug("job %s: rendered job script:\n%s", job_id, script)
        if retry_attempt:
            self._prepare_scheduler_retry_evidence(ws)
        # Staging crosses the SSH boundary and can raise a raw
        # transport.RemoteError. Convert any staging failure into
        # SchedulerError -- the dispatch-domain error _start_scheduler_job
        # already catches -- so ONE job's upload failure lands THAT spec FAILED
        # rather than the raw error escaping submit() and aborting the daemon's
        # whole reconcile+dispatch tick. Mirrors the fetch_results hardening.
        try:
            self._stage_job(
                job_id,
                ws,
                script_path,
                script,
                local_workspace,
                retry_attempt=retry_attempt,
            )
        except SchedulerError:
            raise
        except Exception as exc:  # noqa: BLE001 - uniform staging-failure surface
            raise SchedulerError(
                f"failed to stage workspace for {job_id} into {ws}: {exc}"
            ) from exc

        submit_argv = self.dialect.submit_command(script_path)
        submit_once = getattr(self.runner, "submit_once", None)
        remote_receipt_owned = callable(submit_once)
        try:
            if remote_receipt_owned:
                result = submit_once(
                    submit_argv,
                    receipt_path=self._submit_receipt_path(job_id),
                    vq_job_id=job_id,
                    dialect_name=self.dialect.name,
                    remote_workspace=ws,
                    array_size=array_size,
                )
            else:
                self._prepare_job_start_marker(ws)
                result = self.runner.run(submit_argv, check=False)
        except (transport.RemoteOutcomeUnknown, SchedulerRemoteOutcomeUnknown) as exc:
            raise SchedulerSubmitOutcomeUnknown(
                f"{submit_argv[0]} outcome is unknown for {job_id}; "
                "the scheduler command will not be replayed automatically"
            ) from exc
        submit_ids = _submit_id_candidates(self.dialect, result.stdout)
        scheduler_id = submit_ids[0] if len(submit_ids) == 1 else None
        # A syntactically exact scheduler id is direct acceptance evidence even
        # when the launcher also reports a contradictory nonzero status.  Some
        # scheduler wrappers enqueue successfully and then fail in a trailing
        # hook; treating that as committed rejection can duplicate live work.
        if scheduler_id is not None:
            if not remote_receipt_owned:
                self._write_submit_receipt(
                    job_id=job_id,
                    status="accepted",
                    scheduler_job_id=scheduler_id,
                    returncode=result.returncode,
                )
            log.debug("submitted %s -> scheduler id %s", job_id, scheduler_id)
            return SchedulerHandle(
                job_id=scheduler_id,
                remote_workspace=ws,
                array_size=array_size,
            )
        if submit_ids:
            if not remote_receipt_owned:
                self._write_submit_receipt(
                    job_id=job_id,
                    status="outcome_unknown",
                    scheduler_job_id=None,
                    returncode=result.returncode,
                )
            raise SchedulerSubmitOutcomeUnknown(
                f"{submit_argv[0]} outcome is unknown for {job_id}: multiple "
                "scheduler ids were reported; the command will not be replayed "
                "automatically"
            )
        if result.returncode < 0 or result.returncode >= 128:
            if not remote_receipt_owned:
                self._write_submit_receipt(
                    job_id=job_id,
                    status="outcome_unknown",
                    scheduler_job_id=None,
                    returncode=result.returncode,
                )
            raise SchedulerSubmitOutcomeUnknown(
                f"{submit_argv[0]} outcome is unknown for {job_id} after an "
                f"ambiguous process status ({result.returncode}); the scheduler "
                "command will not be replayed automatically"
            )
        if result.returncode != 0:
            if not remote_receipt_owned:
                self._write_submit_receipt(
                    job_id=job_id,
                    status="rejected",
                    scheduler_job_id=None,
                    returncode=result.returncode,
                )
            raise SchedulerError(
                f"{submit_argv[0]} failed (exit {result.returncode}) for {job_id}:\n"
                f"  stderr: {result.stderr.strip() or '(empty)'}"
            )
        try:
            _parse_unique_submit_id(self.dialect, result.stdout)
        except DialectError as exc:
            if not remote_receipt_owned:
                self._write_submit_receipt(
                    job_id=job_id,
                    status="outcome_unknown",
                    scheduler_job_id=None,
                    returncode=result.returncode,
                )
            raise SchedulerSubmitOutcomeUnknown(
                f"{submit_argv[0]} outcome is unknown for {job_id}: the command "
                "returned success without a valid scheduler id; it will not be "
                "replayed automatically"
            ) from exc
        raise AssertionError("valid scheduler id was not returned above")

    def _prepare_scheduler_retry_evidence(self, remote_workspace: str) -> None:
        """Remove exact, proven-terminal evidence before one explicit retry.

        A scheduler command retry intentionally reuses its vq job id and
        workspace.  The prior attempt is already terminal, so its start marker,
        exit marker, resource receipt, and adjacent submit-once receipt must not
        be mistaken for evidence from the new scheduler mutation.  Refuse
        symlinked parents and remove only these exact names.
        """
        marker_dir = f"{remote_workspace}/{EXIT_MARKER_RELDIR}"
        marker = f"{marker_dir}/{SCHEDULER_ID_BASENAME}"
        exit_marker = f"{marker_dir}/{EXIT_MARKER_BASENAME}"
        resource_receipt = f"{marker_dir}/{RESOURCE_USAGE_BASENAME}"
        submit_receipt = f"{remote_workspace}.submit-once.json"
        script = (
            'workspace="$1"; marker_dir="$2"; marker="$3"; receipt="$4"; '
            'exit_marker="$5"; resource_receipt="$6"; '
            '[ ! -L "$workspace" ] || exit 2; '
            'if [ -e "$workspace" ]; then '
            '  [ -d "$workspace" ] || exit 3; '
            '  [ ! -L "$marker_dir" ] || exit 4; '
            '  if [ -e "$marker_dir" ]; then '
            '    [ -d "$marker_dir" ] || exit 5; '
            '    rm -f -- "$marker" "$exit_marker" "$resource_receipt" '
            '      || exit 6; '
            '  fi; '
            'fi; '
            'rm -f -- "$receipt"'
        )
        try:
            result = self.runner.run(
                [
                    "sh",
                    "-c",
                    script,
                    "vq-scheduler-retry-evidence",
                    remote_workspace,
                    marker_dir,
                    marker,
                    submit_receipt,
                    exit_marker,
                    resource_receipt,
                ],
                check=False,
            )
        except Exception as exc:  # noqa: BLE001 - normalize runner boundary
            raise SchedulerError(
                "scheduler retry evidence could not be rotated safely"
            ) from exc
        if result.returncode != 0:
            raise SchedulerError(
                "scheduler retry evidence path is unsafe or could not be cleared"
            )

    def prepare_retry_attempt(self, job_id: str) -> None:
        """Rotate proven-terminal evidence before exposing a retry claim.

        The daemon calls this before it durably changes a retry from PENDING to
        SUBMITTING.  ``submit(..., retry_attempt=True)`` repeats the idempotent
        rotation immediately before staging, but that later defense cannot
        close the daemon-crash window between the phase-one write and submit.
        """
        self._prepare_scheduler_retry_evidence(self.remote_workspace(job_id))

    def _prepare_job_start_marker(self, remote_workspace: str) -> None:
        """Require an unforgeable start-marker location before qsub/sbatch.

        The uploaded payload controls the workspace bytes, including a staged
        ``_vq`` entry.  A pre-existing marker (or symlinked workspace/marker
        directory) must therefore fail before the scheduler mutation.  The
        production submit-once wrapper repeats this check in the same remote
        shell immediately before qsub; this helper covers injected/legacy
        runners that do not provide that atomic wrapper.
        """
        marker_dir = f"{remote_workspace}/{EXIT_MARKER_RELDIR}"
        marker = f"{marker_dir}/{SCHEDULER_ID_BASENAME}"
        script = (
            'workspace="$1"; marker_dir="$2"; path="$3"; '
            '[ ! -L "$workspace" ] && [ -d "$workspace" ] || exit 2; '
            '[ ! -L "$marker_dir" ] || exit 3; '
            'if [ ! -e "$marker_dir" ]; then mkdir -- "$marker_dir" || exit 4; fi; '
            '[ ! -L "$marker_dir" ] && [ -d "$marker_dir" ] || exit 5; '
            '[ ! -e "$path" ] && [ ! -L "$path" ] || exit 6'
        )
        try:
            result = self.runner.run(
                [
                    "sh",
                    "-c",
                    script,
                    "vq-job-start-marker-check",
                    remote_workspace,
                    marker_dir,
                    marker,
                ],
                check=False,
            )
        except Exception as exc:  # noqa: BLE001 - normalize runner boundary
            raise SchedulerError(
                "scheduler job-start marker could not be verified before submit"
            ) from exc
        if result.returncode != 0:
            raise SchedulerError(
                "scheduler job-start marker path is unsafe or already exists"
            )

    # -- poll ---------------------------------------------------------------

    def _stage_job(
        self,
        job_id: str,
        ws: str,
        script_path: str,
        script: str,
        local_workspace: Path | None,
        *,
        retry_attempt: bool = False,
    ) -> None:
        """Put the workspace and the job script on the cluster.

        One SSH round trip, not four. ``submit`` used to spend six remote
        connections per job -- mkdir, scp, untar, rm, write-script, qsub -- and
        four of those were plumbing. At 2-5 s each on a busy login node that
        cost is what capped campaign dispatch, so it is worth collapsing even
        though each individual call is cheap.

        What makes it collapsible: ``mkdir``, the untar, the tarball cleanup and
        the script write are all *setup* with no decision between them. They run
        as one ``sh -c``, with the script piped in on stdin -- so the same
        connection that unpacks the workspace also writes ``job.pbs``.

        The per-job ``mkdir`` moves inside that command, which leaves only the
        scp needing a directory to exist beforehand. The tarball lands beside
        the workspace (deliberately on the shared FS, not node-local ``/tmp``
        -- see `a01ad4fb1`), so only the *jobs root* has to pre-exist, and that
        is per host rather than per job: :meth:`_ensure_job_root` does it once
        per dispatcher.

        Net: scp + one setup call + qsub. Failure attribution is unchanged --
        every path still raises :class:`SchedulerError` for this one job, so a
        staging failure lands this spec FAILED and no other.
        """
        if local_workspace is None:
            self._run_checked(
                ["sh", "-c", f"mkdir -p {shlex.quote(ws)} && cat > {shlex.quote(script_path)}"],
                stdin_data=script,
                what="create remote workspace and write job script",
            )
            return

        self._ensure_job_root()
        remote_tar = f"{ws.rstrip('/')}.upload.tar"
        with tempfile.NamedTemporaryFile(suffix=".tar", delete=True) as tmp:
            local_tar = Path(tmp.name)
            with tarfile.open(local_tar, "w") as tf:
                # Contents (arcname=".") so they land directly under ws rather
                # than under a nested local-name directory.
                tf.add(
                    local_workspace,
                    arcname=".",
                    filter=(
                        self._retry_stage_filter
                        if retry_attempt
                        else None
                    ),
                )
            self.runner.upload_file(local_tar, remote_tar)

        # `rm -f` runs unconditionally via `;` rather than `&&`: a failed untar
        # must still not leave a tarball behind, and the explicit exit keeps
        # that cleanup from masking the real failure.
        setup = (
            f"mkdir -p {shlex.quote(ws)} && "
            f"{{ tar -xf {shlex.quote(remote_tar)} -C {shlex.quote(ws)}; "
            f"__rc=$?; rm -f {shlex.quote(remote_tar)}; "
            f'[ "$__rc" -eq 0 ] || exit "$__rc"; }} && '
            f"cat > {shlex.quote(script_path)}"
        )
        self._run_checked(
            ["sh", "-c", setup],
            stdin_data=script,
            what="stage workspace and write job script",
        )

    @staticmethod
    def _retry_stage_filter(member: tarfile.TarInfo) -> tarfile.TarInfo | None:
        """Exclude exact prior-attempt scheduler evidence from a retry upload."""
        parts = tuple(part for part in PurePosixPath(member.name).parts if part != ".")
        if parts in {
            (EXIT_MARKER_RELDIR, SCHEDULER_ID_BASENAME),
            (EXIT_MARKER_RELDIR, EXIT_MARKER_BASENAME),
            (EXIT_MARKER_RELDIR, RESOURCE_USAGE_BASENAME),
        }:
            return None
        return member

    def _ensure_job_root(self) -> None:
        """Create the shared per-host jobs root, once per dispatcher.

        The upload tarball is a sibling of the per-job workspace, so its parent
        must exist before the scp. That parent is shared by every job on the
        host, so making it per-job was a wasted round trip on all but the first.
        """
        if self._job_root_ready:
            return
        root = f"{self.scratch_root}/{self.job_root}"
        self._run_checked(["mkdir", "-p", root], what="create remote jobs root")
        self._job_root_ready = True

    def poll_with_evidence(
        self, handles: Iterable[SchedulerHandle]
    ) -> SchedulerPollEvidence:
        """Batched status poll for every handle → ``{job_id: phase}``.

        One ``qstat <id...>`` for all ids (design doc §4 — never per-job). A
        handle absent from the output has left the queue and is reported
        FINISHED; the caller then reads the exit-marker for its rc. The poll
        command is always the dialect's plain ``qstat`` — never ``qstat -f`` or a
        custom ``-format`` — because ``parse_poll`` keys off the default table's
        positional columns (review note 2).
        """
        ids = [h.job_id for h in handles]
        if not ids:
            return SchedulerPollEvidence({})
        # Torque qstat returns nonzero when some ids already left the queue while
        # still listing live ones on stdout. Slurm squeue does not provide that
        # proof: a nonzero result is an unavailable observation, never evidence
        # that every requested job is terminal.
        command = self.dialect.poll_command(ids)
        result = self.runner.run(command, check=False)
        live = self.dialect.parse_poll(result.stdout)
        phases = {
            jid: _phase_for_polled_job(
                jid,
                live,
                dialect_name=self.dialect.name,
            )
            for jid in ids
        }
        explicitly_absent: frozenset[str] = frozenset()
        if self.accounting_required_for_absent and result.returncode != 0:
            if self.dialect.name == "slurm" and _slurm_invalid_job_poll(
                result.stderr
            ):
                candidates = [
                    jid
                    for jid, phase in phases.items()
                    if phase is SchedulerPhase.FINISHED
                ]
                isolated: set[str] = set()
                if len(ids) == 1:
                    isolated.update(candidates)
                else:
                    # Slurm's batch diagnostic does not name the invalid id.
                    # Probe only the missing candidates individually so a
                    # live sibling omitted from partial stdout can never be
                    # retired on another handle's error. This rare fallback
                    # remains off the daemon thread and every call retains the
                    # runner's ordinary bounded timeout.
                    for jid in candidates:
                        single_command = self.dialect.poll_command([jid])
                        single_result = self.runner.run(
                            single_command,
                            check=False,
                        )
                        single_live = self.dialect.parse_poll(
                            single_result.stdout
                        )
                        phases[jid] = _phase_for_polled_job(
                            jid,
                            single_live,
                            dialect_name=self.dialect.name,
                        )
                        if single_result.returncode == 0:
                            continue
                        if (
                            _slurm_invalid_job_poll(single_result.stderr)
                            and phases[jid] is SchedulerPhase.FINISHED
                        ):
                            isolated.add(jid)
                            continue
                        raise SchedulerError(
                            f"{single_command[0]} poll failed "
                            f"(exit {single_result.returncode})"
                        )
                explicitly_absent = frozenset(isolated)
            if not explicitly_absent:
                raise SchedulerError(
                    f"{command[0]} poll failed (exit {result.returncode})"
                )
        return SchedulerPollEvidence(phases, explicitly_absent)

    def poll(self, handles: Iterable[SchedulerHandle]) -> dict[str, SchedulerPhase]:
        """Compatibility view of :meth:`poll_with_evidence`."""
        return self.poll_with_evidence(handles).phases

    def poll_detail(self, handles: Iterable[SchedulerHandle]) -> dict[str, QstatDetail]:
        """Batched ``qstat -f`` detail for live jobs → ``{job_id: QstatDetail}``.

        The telemetry companion to :meth:`poll` (§18 item 2): one detailed poll
        per host for the exec host + walltime-used/limit. Jobs that already left
        the queue are simply absent from the result (the caller knows them from
        :meth:`poll` as FINISHED). Tolerates the nonzero rc ``qstat`` returns for
        already-gone ids.
        """
        ids = [h.job_id for h in handles]
        if not ids:
            return {}
        command = self.dialect.poll_detail_command(ids)
        result = self.runner.run(command, check=False)
        if self.accounting_required_for_absent and result.returncode != 0:
            raise SchedulerError(
                f"{command[0]} accounting poll failed (exit {result.returncode})"
            )
        return self.dialect.parse_qstat_detail(result.stdout)

    def phase_from_detail(self, detail: QstatDetail) -> SchedulerPhase | None:
        """Coarse phase from a detailed scheduler record, if it has a state."""
        if not detail.raw_state:
            return None
        return self.dialect.phase_for_state(detail.raw_state)

    def abnormal_termination_from_detail(self, detail: QstatDetail) -> str | None:
        """Scheduler-attributed abnormal-end verdict from an accounting record.

        Returns the dialect's normalized abnormal terminal state (for example
        ``"OUT_OF_MEMORY"``) when the scheduler itself ended or invalidated
        the job, else ``None``. The daemon consults this before recording a
        terminal classification so a killed job whose exit-marker (mis)reads
        0 can never be reaped as ``completed`` (#414).
        """
        if not detail.raw_state:
            return None
        return self.dialect.abnormal_termination(detail.raw_state)

    def tail_log(
        self,
        handle: SchedulerHandle,
        *,
        lines: int | None = 200,
        stream: str = "stdout",
        array_index: int | None = None,
    ) -> str:
        """Return a live job's stdout/stderr log tail (§18 item 5).

        The user command's output is redirected to ``stdout.log`` / ``stderr.log``
        on the NFS workspace (see :meth:`build_job_script`), so this can ``tail``
        it over SSH *while the job runs* -- backing a future ``vq logs --follow``
        so a user can watch a long cluster calculation (an SCF converging).
        ``lines=None`` returns the whole file. Returns ``""`` when the log does
        not exist yet (the job is still queued).
        """
        path = self._log_path(handle, stream=stream, array_index=array_index)
        line_arg = "+1" if lines is None else str(lines)
        result = self.runner.run(["tail", "-n", line_arg, path], check=False)
        return result.stdout if result.returncode == 0 else ""

    def tail_file(
        self,
        handle: SchedulerHandle,
        *,
        filename: str,
        lines: int | None = 50,
    ) -> str:
        """Return a live scheduler workspace file tail.

        ``vq logs`` uses :meth:`tail_log` for the standard stdout/stderr files.
        Calculation-artifact readers and ``vq tail --name FILE`` use this
        method for workspace-relative files. Path validation is repeated here
        so non-CLI callers cannot supply absolute or parent-traversal paths.

        **A failed read is not an empty file.** This used to return ``""``
        for every non-zero ``tail`` exit, so a remote read that failed for any
        reason (a permission denial, an NFS stall, a truncated pipe) was
        indistinguishable from "the job has not written anything yet".
        ``vq progress`` rendered that as
        ``(no .scf.jsonl — structured log not enabled)`` and exited 0 — the
        same silent-success-on-a-failed-remote-read class as #111 / #114 /
        #118. Only a genuinely absent file still yields ``""``; anything else
        raises so the caller can report it.
        """
        line_arg = "+1" if lines is None else str(lines)
        path = _workspace_file_path(handle, filename)
        result = self.runner.run(["tail", "-n", line_arg, path], check=False)
        if result.returncode == 0:
            return result.stdout
        if _is_absent_file_error(result.stderr):
            return ""
        raise SchedulerError(
            f"failed to read {filename!r} from the scheduler workspace of "
            f"{handle.job_id} (tail exit {result.returncode}):\n"
            f"  path: {path}\n"
            f"  stderr: {result.stderr.strip() or '(empty)'}\n"
            "  next: this is a failed remote read, NOT an empty file — do not "
            "treat it as 'no output yet'."
        )

    def tail_file_since(
        self,
        handle: SchedulerHandle,
        *,
        filename: str,
        byte_offset: int,
    ) -> SchedulerFileChunk | None:
        """Return bytes after an offset from a scheduler workspace file.

        This backs live ``vq tail -f`` on scheduler workspaces. Unlike repeated
        full-file tails, each poll asks the remote host for the file size and
        only the appended bytes. The body is base64-wrapped across the
        text-decoding SSH transport, and capped to the size captured before the
        read so the returned cursor and bytes cannot race an append. If the file
        shrank between polls, or its preceding 4 KiB boundary fingerprint
        changed after a truncate-and-regrow, the remote shell resets the offset
        to zero and ``reset`` is true. A replacement whose size and boundary
        window are byte-identical is indistinguishable from an append. ``None``
        means the file is not readable yet.
        """
        path = _workspace_file_path(handle, filename)
        offset = max(0, byte_offset)
        cache_key = (handle.remote_workspace, filename)
        cached = self._tail_boundary_tokens.get(cache_key)
        expected_token = (
            cached[1]
            if cached is not None and cached[0] == offset
            else "-"
        )
        guard = TAIL_REWRITE_GUARD_BYTES
        script = (
            'path="$1"; offset="$2"; expected="$3"; '
            '[ -f "$path" ] || exit 1; '
            "size=$(wc -c < \"$path\" | tr -d '[:space:]') || exit 1; "
            '[ -n "$size" ] || exit 1; '
            'reset=0; '
            'if [ "$size" -lt "$offset" ]; then offset=0; reset=1; fi; '
            'if [ "$offset" -gt 0 ] && [ "$expected" != "-" ]; then '
            f'guard={guard}; '
            'if [ "$offset" -gt "$guard" ]; then '
            'guard_start=$((offset - guard)); else guard_start=0; fi; '
            'guard_count=$((offset - guard_start)); '
            'current=$(tail -c "+$((guard_start + 1))" "$path" | '
            'head -c "$guard_count" | cksum) || exit 1; '
            'set -- $current; current="$1:$2"; '
            'if [ "$current" != "$expected" ]; then offset=0; reset=1; fi; '
            'fi; '
            'count=$((size - offset)); '
            f'guard={guard}; '
            'if [ "$size" -gt "$guard" ]; then '
            'guard_start=$((size - guard)); else guard_start=0; fi; '
            'guard_count=$((size - guard_start)); '
            'token=$(tail -c "+$((guard_start + 1))" "$path" | '
            'head -c "$guard_count" | cksum) || exit 1; '
            'set -- $token; token="$1:$2"; '
            'printf "%s %s %s\\n" "$size" "$reset" "$token"; '
            'if [ "$count" -gt 0 ]; then '
            'tail -c "+$((offset + 1))" "$path" | head -c "$count" | base64; '
            "fi"
        )
        result = self.runner.run(
            [
                "sh",
                "-c",
                script,
                "vq-tail-file-since",
                path,
                str(offset),
                expected_token,
            ],
            check=False,
        )
        if result.returncode != 0:
            return None
        header, sep, encoded = result.stdout.partition("\n")
        if not sep:
            return None
        try:
            size_text, reset_text, boundary_token = header.split()
            size = int(size_text)
            reset = bool(int(reset_text))
            data = base64.b64decode(encoded, validate=False)
        except (ValueError, binascii.Error):
            log.warning(
                "unparseable remote file chunk for %s:%s: %r",
                handle.job_id,
                filename,
                result.stdout[:200],
            )
            return None
        start = 0 if reset else offset
        expected = max(0, size - start)
        if len(data) > expected:
            log.warning(
                "oversized remote file chunk for %s:%s: expected at most %d "
                "bytes, received %d",
                handle.job_id,
                filename,
                expected,
                len(data),
            )
            return None
        chunk = SchedulerFileChunk(
            end_offset=start + len(data),
            data=data,
            reset=reset,
        )
        if chunk.end_offset == size:
            self._tail_boundary_tokens[cache_key] = (
                chunk.end_offset,
                boundary_token,
            )
        else:
            self._tail_boundary_tokens.pop(cache_key, None)
        return chunk

    # -- exit code / terminal state -----------------------------------------

    def exit_marker_code(
        self, handle: SchedulerHandle, *, array_index: int | None = None
    ) -> int | None:
        """The job's marker rc, or ``None`` if the marker is absent/unreadable.

        This is the daemon's completion fence for scheduler jobs: the generated
        PBS script writes this marker only after the command exits and, when
        node-local scratch is enabled, after output has been copied back to the
        shared workspace. Callers that need a "safe to fetch the whole
        workspace" signal must use this marker-only method, not the qstat
        fallback in :meth:`exit_code`.
        """
        marker = self._marker_path(handle.remote_workspace, array_index)
        try:
            result = self.runner.run(["cat", marker], check=False)
        except Exception as exc:  # noqa: BLE001 - uniform transport-failure surface
            raise SchedulerError(
                f"failed to read exit-marker for {handle.job_id} at {marker}: {exc}"
            ) from exc
        if result.returncode == 0:
            text = result.stdout.strip()
            if text:
                try:
                    return int(text)
                except ValueError:
                    log.warning("unparseable exit-marker %s: %r", marker, text)
        return None

    def recorded_job_id(
        self,
        job_id: str,
        *,
        array_size: int | None = None,
    ) -> str | None:
        """The scheduler id the job recorded for itself, or ``None``.

        Recovery for a spec the driver dispatched but never got to name (see
        :data:`SCHEDULER_ID_BASENAME`). The workspace path is derived from vq's
        own job id, so this needs no scheduler handle -- which is the point,
        since the missing handle is what it exists to rebuild.

        ``None`` means "cannot say", never "no such job": a queued job has not
        run its script yet, and a transport failure is not evidence either. The
        caller must keep the spec live and retry rather than reap it.
        """
        if array_size is not None:
            # Native array elements carry element ids and deliberately never
            # publish the single-job marker; consuming a staged parent marker
            # would manufacture acceptance evidence that no array job wrote.
            return None
        workspace = self.remote_workspace(job_id)
        marker_dir = f"{workspace}/{EXIT_MARKER_RELDIR}"
        path = (
            f"{workspace}/"
            f"{EXIT_MARKER_RELDIR}/{SCHEDULER_ID_BASENAME}"
        )
        script = (
            'workspace="$1"; marker_dir="$2"; path="$3"; '
            '[ ! -L "$workspace" ] && [ -d "$workspace" ] || exit 1; '
            '[ ! -L "$marker_dir" ] && [ -d "$marker_dir" ] || exit 1; '
            '[ ! -L "$path" ] && [ -f "$path" ] || exit 1; '
            f'dd if="$path" bs={SCHEDULER_JOB_ID_MAX_BYTES + 1} '
            "count=1 2>/dev/null"
        )
        try:
            result = self.runner.run(
                [
                    "sh",
                    "-c",
                    script,
                    "vq-scheduler-job-id-read",
                    workspace,
                    marker_dir,
                    path,
                ],
                check=False,
            )
        except Exception:  # noqa: BLE001 - absence and transport failure are both "cannot say"
            log.warning("job %s: could not read recorded scheduler id", job_id)
            return None
        if result.returncode != 0 or len(
            result.stdout.encode("utf-8", "replace")
        ) > SCHEDULER_JOB_ID_MAX_BYTES:
            return None
        scheduler_job_id = result.stdout.strip()
        if not scheduler_job_id or len(result.stdout.splitlines()) != 1:
            return None
        parser_input = (
            f"Submitted batch job {scheduler_job_id}"
            if self.dialect.name == "slurm"
            else scheduler_job_id
        )
        try:
            parsed = self.dialect.parse_submit_id(parser_input)
        except DialectError:
            return None
        return scheduler_job_id if parsed == scheduler_job_id else None

    def submit_receipt(self, job_id: str) -> SchedulerSubmitReceipt | None:
        """Return one bounded, exact-job submit receipt when valid.

        Missing, oversized, malformed, cross-job, or dialect-invalid evidence
        is unknown rather than rejection. Native output and paths are never
        surfaced to the durable job record.
        """
        path = self._submit_receipt_path(job_id)
        script = (
            'path="$1"; [ ! -L "$path" ] && [ -f "$path" ] || exit 1; '
            f'dd if="$path" bs={SCHEDULER_SUBMIT_RECEIPT_MAX_BYTES + 1} '
            "count=1 2>/dev/null"
        )
        try:
            result = self.runner.run(
                ["sh", "-c", script, "vq-submit-receipt-read", path],
                check=False,
            )
        except Exception:  # noqa: BLE001 - absence and transport failure mean unknown
            return None
        if result.returncode != 0:
            return None
        if len(result.stdout.encode("utf-8", "replace")) > SCHEDULER_SUBMIT_RECEIPT_MAX_BYTES:
            return None
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict) or set(payload) != {
            "schema",
            "status",
            "vq_job_id",
            "scheduler_job_id",
            "scheduler_returncode",
        }:
            return None
        status = payload.get("status")
        scheduler_job_id = payload.get("scheduler_job_id")
        returncode = payload.get("scheduler_returncode")
        if (
            payload.get("schema") != SCHEDULER_SUBMIT_RECEIPT_SCHEMA
            or payload.get("vq_job_id") != job_id
            or status not in {"accepted", "rejected", "outcome_unknown"}
            or not isinstance(returncode, int)
            or isinstance(returncode, bool)
        ):
            return None
        if status == "accepted":
            if not isinstance(scheduler_job_id, str):
                return None
            parser_input = (
                f"Submitted batch job {scheduler_job_id}"
                if self.dialect.name == "slurm"
                else scheduler_job_id
            )
            try:
                parsed = self.dialect.parse_submit_id(parser_input)
            except DialectError:
                return None
            if parsed != scheduler_job_id:
                return None
        elif scheduler_job_id is not None:
            return None
        if status == "rejected" and not (0 < returncode < 128):
            return None
        if status == "outcome_unknown" and 0 < returncode < 128:
            return None
        return SchedulerSubmitReceipt(
            status=status,
            scheduler_job_id=scheduler_job_id,
            scheduler_returncode=returncode,
        )

    def exit_code(
        self, handle: SchedulerHandle, *, array_index: int | None = None
    ) -> int | None:
        """The job's (or one array sub-job's) rc, or ``None`` if unrecoverable.

        Primary source: the exit-marker file the job script wrote -- read in
        place over SSH (no need to stage the whole workspace back just for rc).
        Fallback (single jobs only): ``qstat -f`` ``exit_status`` while the job
        still lingers in the queue. ``None`` means neither was available.
        """
        marker_rc = self.exit_marker_code(handle, array_index=array_index)
        if marker_rc is not None:
            return marker_rc
        # Fallback: qstat -f exit_status. It is first-match/aggregate for arrays
        # (review note 3), so it is only trustworthy for a single job — arrays
        # must come from the per-index marker above.
        if array_index is None:
            detail = self.runner.run(
                self.dialect.detail_command(handle.job_id), check=False
            )
            if detail.returncode == 0:
                return self.dialect.parse_exit_status(detail.stdout)
        return None

    def missing_marker_diagnostics(
        self, handle: SchedulerHandle, *, array_index: int | None = None
    ) -> dict[str, object]:
        """Best-effort remote evidence for a FINISHED job with no marker.

        The daemon calls this only after the job disappeared from ``qstat``, the
        marker remained invisible for the grace window, and a final workspace
        fetch still did not recover ``_vq/exit-code``. Every probe is bounded
        and non-fatal: this method is for forensics, not terminal-state
        decision making.
        """
        marker = self._marker_path(handle.remote_workspace, array_index)
        evidence: dict[str, object] = {
            "scheduler_job_id": handle.job_id,
            "remote_workspace": handle.remote_workspace,
            "remote_exit_marker": marker,
        }
        if array_index is not None:
            evidence["array_index"] = array_index
        errors: list[str] = []

        def capture(label: str, argv: Sequence[str]) -> None:
            try:
                result = self.runner.run(argv, check=False)
            except Exception as exc:  # noqa: BLE001 - diagnostic path
                errors.append(f"{label}: {exc}")
                return
            evidence[f"{label}_rc"] = result.returncode
            if result.stdout:
                evidence[label] = _clip_diagnostic_text(result.stdout)
            if result.stderr:
                evidence[f"{label}_stderr"] = _clip_diagnostic_text(result.stderr)

        ws = handle.remote_workspace
        capture("remote_workspace_listing", ["ls", "-la", ws])
        capture("remote_vq_listing", ["ls", "-la", f"{ws}/{EXIT_MARKER_RELDIR}"])
        capture(
            "remote_file_sample",
            [
                "sh",
                "-c",
                "find "
                f"{shlex.quote(ws)} -maxdepth 2 -type f -print 2>&1 "
                f"| sort | sed -n '1,{MISSING_MARKER_DIAGNOSTIC_FILE_LIMIT}p'",
            ],
        )
        capture(
            "remote_exit_marker_probe",
            [
                "sh",
                "-c",
                "if [ -e "
                f"{shlex.quote(marker)} ]; then ls -l {shlex.quote(marker)}; "
                "printf '\\n-- contents --\\n'; "
                f"cat {shlex.quote(marker)}; else echo missing; fi",
            ],
        )
        capture(
            "remote_stdout_tail",
            [
                "tail",
                "-n",
                str(MISSING_MARKER_DIAGNOSTIC_TAIL_LINES),
                self._log_path(handle, stream="stdout", array_index=array_index),
            ],
        )
        capture(
            "remote_stderr_tail",
            [
                "tail",
                "-n",
                str(MISSING_MARKER_DIAGNOSTIC_TAIL_LINES),
                self._log_path(handle, stream="stderr", array_index=array_index),
            ],
        )
        capture("qstat_detail", self.dialect.detail_command(handle.job_id))
        if errors:
            evidence["diagnostic_errors"] = errors
        return evidence

    def final_state(
        self,
        handle: SchedulerHandle,
        *,
        array_index: int | None = None,
        walltime_exceeded: bool = False,
    ) -> JobState:
        """Classify a FINISHED job into a terminal :class:`JobState`.

        Convenience wrapper: read the rc, then bridge. ``walltime_exceeded`` is
        supplied by the caller from scheduler accounting when known (vq's
        ``/proc`` watchdog degrades to telemetry under a scheduler — design doc
        §10 — so wall-time enforcement is the scheduler's, surfaced here).
        """
        rc = self.exit_code(handle, array_index=array_index)
        return self.phase_to_state(
            SchedulerPhase.FINISHED, rc, walltime_exceeded=walltime_exceeded
        )

    @staticmethod
    def phase_to_state(
        phase: SchedulerPhase, rc: int | None, *, walltime_exceeded: bool = False
    ) -> JobState:
        """Bridge a coarse :class:`SchedulerPhase` (+ rc) to a vq :class:`JobState`.

        This dialect-independent conversion helper is kept out of the
        per-dialect layer so Torque, Slurm, and future dialects share it. The
        live daemon records scheduler phase separately and owns durable
        JobSpec lifecycle transitions; it does not move a submitted RUNNING
        spec back to PENDING when the cluster reports a queued phase::

            PENDING  -> PENDING
            RUNNING  -> RUNNING
            FINISHED -> walltime exceeded -> TIME_EXCEEDED
                        rc is None        -> ABORTED_BY_QUEUE  (gone, no marker)
                        rc == 0           -> COMPLETED
                        rc != 0           -> FAILED
        """
        if phase is SchedulerPhase.PENDING:
            return JobState.PENDING
        if phase is SchedulerPhase.RUNNING:
            return JobState.RUNNING
        # FINISHED
        if walltime_exceeded:
            return JobState.TIME_EXCEEDED
        if rc is None:
            return JobState.ABORTED_BY_QUEUE
        return JobState.COMPLETED if rc == 0 else JobState.FAILED

    def _log_path(
        self,
        handle: SchedulerHandle,
        *,
        stream: str,
        array_index: int | None,
    ) -> str:
        name = STDERR_LOG if stream == "stderr" else STDOUT_LOG
        if array_index is not None:
            name = f"{name}.{array_index}"
        return f"{handle.remote_workspace}/{name}"

    # -- cancel -------------------------------------------------------------

    def cancel(self, handle: SchedulerHandle) -> None:
        """Cancel a job with proof that it was accepted or already absent.

        ``qdel`` handles the SIGTERM→SIGKILL escalation itself per the queue's
        ``kill_delay``, so terminate and kill route here identically. Success
        proves the scheduler accepted the cancellation. The dialect's exact
        already-absent diagnostic is also idempotent success; every other
        nonzero result is ambiguous and must retain daemon ownership for retry.
        """
        command = self.dialect.cancel_command(handle.job_id)
        result = self.runner.run(command, check=False)
        if result.returncode != 0:
            diagnostic = " ".join(result.stderr.split())
            absent = (
                self.dialect.name == "torque"
                and re.fullmatch(
                    r"(?:qdel:\s*)?Unknown Job Id(?:\s+\S+)?",
                    diagnostic,
                )
                is not None
            ) or (
                self.dialect.name == "slurm"
                and re.fullmatch(
                    r"scancel:\s+error:\s+Invalid job id specified",
                    diagnostic,
                    flags=re.IGNORECASE,
                )
                is not None
            )
            if absent:
                log.debug(
                    "%s %s confirmed the scheduler job is already absent",
                    command[0],
                    handle.job_id,
                )
                return
            raise SchedulerError(
                f"scheduler cancellation could not be confirmed for "
                f"{handle.job_id} (exit {result.returncode})"
            )

    def hold(self, handle: SchedulerHandle) -> None:
        """Place a scheduler hold with ``qhold``.

        This is the batch-scheduler analogue of pausing a job before it starts:
        it prevents a queued job from being dispatched by the scheduler, but it
        is not a SIGSTOP-equivalent suspension for a job already running on a
        compute node. Callers should check the current scheduler phase before
        exposing this as ``vq pause``.
        """
        result = self.runner.run(self.dialect.hold_command(handle.job_id), check=False)
        if result.returncode != 0:
            raise SchedulerError(
                f"qhold failed (exit {result.returncode}) for {handle.job_id}:\n"
                f"  stderr: {result.stderr.strip() or '(empty)'}"
            )

    def release(self, handle: SchedulerHandle) -> None:
        """Release a scheduler hold with ``qrls``."""
        result = self.runner.run(
            self.dialect.release_command(handle.job_id), check=False
        )
        if result.returncode != 0:
            raise SchedulerError(
                f"qrls failed (exit {result.returncode}) for {handle.job_id}:\n"
                f"  stderr: {result.stderr.strip() or '(empty)'}"
            )

    # -- fetch results ------------------------------------------------------

    def fetch_results(
        self,
        handle: SchedulerHandle,
        local_dir: Path,
        *,
        terminal: bool = False,
    ) -> bool:
        """Copy the **entire** remote workspace back into ``local_dir``.

        The good-citizen / preserve-everything contract (no test jobs, full
        outputs kept): once a job reaches a terminal state — COMPLETED **or**
        FAILED — the daemon brings the whole workspace home, not just stdout, so
        every auxiliary file the run produced (wavefunctions, ``fort.*``,
        ``.gbw``, restart data, the exit-marker) is preserved on the submitting
        machine.

        Mechanism mirrors staging-up in reverse: tar the workspace remotely into
        one tarball, ``scp`` it down, untar into ``local_dir``, then remove the
        remote tarball. One scp regardless of file count, and the safe-extraction
        filter blocks any path-traversal entry. Raises :class:`SchedulerError`
        on any transport failure; the daemon then leaves the remote workspace in
        place so a retry can re-fetch rather than losing the data.
        """
        ws = handle.remote_workspace
        transfer_id = uuid.uuid4().hex
        remote_tar = f"{ws}.result-{transfer_id}.tar"
        local_dir.mkdir(parents=True, exist_ok=True)
        # A daemon restart or concurrent explicit fetch must not share a tar
        # with an older detached scp process or remove its remote archive.
        local_tar = local_dir / f".vq-fetch-{transfer_id}.tar"
        self._run_checked(
            ["tar", "-cf", remote_tar, "-C", ws, "."], what="archive remote workspace"
        )
        try:
            try:
                self.runner.download_file(remote_tar, local_tar)
            except Exception as exc:  # noqa: BLE001 - uniform transport-failure surface
                raise SchedulerError(
                    f"failed to download results for {handle.job_id} "
                    f"from {ws}: {exc}"
                ) from exc
            try:
                # Extract into a clean sibling first. Production payload inputs
                # can be hash-sealed mode 0444, so extracting directly over the
                # materialized source workspace raises EACCES before stdout,
                # stderr, and scientific outputs are reached in the tar.
                _extract_scheduler_workspace(local_tar, local_dir)
            except (tarfile.TarError, OSError) as exc:
                # Belt-and-suspenders: a truncated / unreadable tarball must
                # surface as SchedulerError so ``_reconcile_scheduler`` parks
                # THIS job as fetch_failed, rather than the raw error escaping
                # and wedging the daemon's whole reconcile-then-dispatch loop.
                raise SchedulerError(
                    f"failed to extract results for {handle.job_id} "
                    f"from {ws}: {exc}"
                ) from exc
        finally:
            local_tar.unlink(missing_ok=True)
            # Best-effort remote cleanup of the tarball; the workspace itself is
            # left intact (the daemon decides when to reap it).
            self.runner.run(["rm", "-f", remote_tar], check=False)
        if not terminal:
            return True
        return self.record_terminal_resource_sample(handle, local_dir)

    def record_terminal_resource_sample(
        self,
        handle: SchedulerHandle,
        local_dir: Path,
    ) -> bool:
        """Project terminal-only scheduler telemetry after artifact fetch."""
        return self._record_terminal_slurm_sample(handle, local_dir)

    def _record_terminal_slurm_sample(
        self,
        handle: SchedulerHandle,
        local_dir: Path,
    ) -> bool:
        """Populate missing daemonless-Slurm samples from bounded accounting."""
        if self.dialect.name != "slurm":
            return True
        samples_path = local_dir / EXIT_MARKER_RELDIR / "samples.jsonl"
        disposition = _existing_sample_disposition(samples_path, handle.job_id)
        if disposition in {"preserve", "complete"}:
            return True
        receipt_sample = _scheduler_resource_receipt_sample(local_dir, handle)
        if receipt_sample is not None:
            try:
                _write_scheduler_sample(samples_path, receipt_sample)
            except OSError:
                log.warning("could not persist bounded scheduler resource sample")
                return False
            return True
        try:
            result = self.runner.run(
                [
                    "sacct",
                    "--array",
                    "--units=K",
                    "-j",
                    handle.job_id,
                    "--parsable2",
                    "--noheader",
                    # JobID preserves ``<array>_<task>.<step>`` identity.
                    # JobIDRaw is a distinct numeric allocation id for each
                    # array task and cannot be grouped back to the parent.
                    "--format=JobID,State,TotalCPU,MaxRSS,ElapsedRaw",
                ],
                check=False,
            )
        except SchedulerError:
            sample = _unavailable_scheduler_sample(handle.job_id, "query_failed")
            try:
                _write_scheduler_sample(samples_path, sample)
            except OSError:
                log.warning("could not persist bounded scheduler resource sample")
            return False
        if result.returncode != 0:
            sample = _unavailable_scheduler_sample(handle.job_id, "command_failed")
        else:
            try:
                sample = _scheduler_accounting_sample(
                    result.stdout,
                    handle.job_id,
                    expected_array_size=handle.array_size,
                )
            except SchedulerError:
                sample = _unavailable_scheduler_sample(
                    handle.job_id,
                    "malformed_output",
                )
        try:
            _write_scheduler_sample(samples_path, sample)
        except OSError:
            log.warning("could not persist bounded scheduler resource sample")
            return False
        return sample.get("scheduler_accounting_status") == "ok"

    # -- helpers ------------------------------------------------------------

    def _run_checked(
        self, argv: Sequence[str], *, stdin_data: str | None = None, what: str
    ) -> RemoteResult:
        result = self.runner.run(argv, stdin_data=stdin_data, check=False)
        if result.returncode != 0:
            raise SchedulerError(
                f"failed to {what} (exit {result.returncode}):\n"
                f"  cmd: {shlex.join(argv)}\n"
                f"  stderr: {result.stderr.strip() or '(empty)'}"
            )
        return result


class SshRemoteRunner:
    """The production :class:`RemoteRunner` — a thin shell over :mod:`vq.transport`.

    Carries a :class:`~vq.config.HostConfig` whose ``ssh`` alias the 2-hop
    ProxyJump (gateway → cluster) is configured on, so every call is a single
    ``ssh <alias> …``. Deliberately logic-light: the orchestration and parsing
    that need testing live in :class:`SchedulerDispatcher`; this class is the
    un-unit-tested I/O edge (validated on the real cluster via the maintainer
    smoke recipe), the same split as :class:`~vq.dispatch.LocalDispatcher` vs
    the daemon tests.
    """

    def __init__(self, host_cfg: HostConfig) -> None:
        self._host_cfg = host_cfg

    def run(
        self, argv: Sequence[str], *, stdin_data: str | None = None, check: bool = False
    ) -> RemoteResult:
        timeout = _scheduler_remote_timeout(argv)
        retry_transient = _scheduler_remote_retries(argv)
        accounting_query = bool(argv) and argv[0] == "sacct"
        try:
            proc = transport.run_remote_shell(
                self._host_cfg,
                *argv,
                check=check,
                stdin_data=stdin_data,
                timeout=timeout,
                retry_transient=retry_transient,
                owned_process_group=accounting_query,
                max_stdout_bytes=(
                    SCHEDULER_ACCOUNTING_MAX_BYTES + 1 if accounting_query else None
                ),
                max_stderr_bytes=4096 if accounting_query else None,
            )
        except transport.RemoteOutcomeUnknown as exc:
            raise SchedulerRemoteOutcomeUnknown(
                f"remote scheduler command outcome is unknown on "
                f"{self._host_cfg.ssh}: {shlex.join(argv)}"
            ) from exc
        except transport.RemoteError as exc:
            raise SchedulerError(
                f"remote scheduler command failed on {self._host_cfg.ssh}: "
                f"{shlex.join(argv)}\n  {exc}"
            ) from exc
        if proc.returncode == 255:
            raise SchedulerError(
                f"ssh transport failed for scheduler command on {self._host_cfg.ssh}: "
                f"{shlex.join(argv)}\n"
                f"  stderr: {proc.stderr.strip() or '(empty)'}"
            )
        return RemoteResult(
            returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr
        )

    def submit_once(
        self,
        argv: Sequence[str],
        *,
        receipt_path: str,
        vq_job_id: str,
        dialect_name: str,
        remote_workspace: str,
        array_size: int | None,
    ) -> RemoteResult:
        """Run one scheduler submit and publish its receipt before replying.

        qsub/sbatch and receipt publication share one remote shell invocation,
        closing the observer crash gap between separate SSH calls. The wrapper
        captures scheduler output in owner-only temporary files, publishes a
        strict five-field JSON receipt atomically, then replays the original
        streams and status to the driver. If the local SSH observer is lost,
        the caller preserves outcome-unknown and later reads this receipt.
        """
        if dialect_name not in {"torque", "slurm"}:
            raise SchedulerError(f"unsupported submit-once dialect {dialect_name!r}")
        if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", vq_job_id):
            raise SchedulerError("invalid vq job id for scheduler submit receipt")
        if array_size is not None and array_size < 1:
            raise SchedulerError("scheduler array size must be positive")
        script = r'''
umask 077
receipt=$1
vq_job_id=$2
dialect=$3
workspace=$4
array_size=$5
shift 5
marker_dir=${workspace}/__VQ_MARKER_RELDIR__
job_start_marker=${marker_dir}/__VQ_MARKER_BASENAME__
marker_ready=1
if [ "$receipt" != "${workspace}.submit-once.json" ] || \
        [ -L "$workspace" ] || [ ! -d "$workspace" ] || \
        [ -L "$marker_dir" ] || [ -e "$receipt" ] || [ -L "$receipt" ]; then
    marker_ready=0
elif [ ! -e "$marker_dir" ] && ! mkdir -- "$marker_dir"; then
    marker_ready=0
elif [ -L "$marker_dir" ] || [ ! -d "$marker_dir" ] || \
        [ -e "$job_start_marker" ] || [ -L "$job_start_marker" ]; then
    marker_ready=0
fi
if [ "$marker_ready" -ne 1 ]; then
    printf '%s\n' \
        'vq: unsafe scheduler submit staging, receipt, or job-start marker' >&2
    exit 64
fi
# Capture through anonymous pipes into bounded shell memory. A filesystem temp
# path is discoverable and replaceable by another process under the shared
# remote account; reopening it after qsub would let that peer forge the direct
# acceptance id. Every command byte is prefixed by its original stream, while
# the unprefixed final status line proves the bounded pipeline reached EOF.
capture=$(
    {
        "$@" 2> >(LC_ALL=C sed 's/^/__VQ_STDERR__/')
        command_rc=$?
        stderr_filter_pid=$!
        [ -z "$stderr_filter_pid" ] || wait "$stderr_filter_pid"
        printf '__VQ_STATUS__%s\n' "$command_rc"
    } | head -c __VQ_OUTPUT_LIMIT__
)
status_lines=$(printf '%s\n' "$capture" | \
    sed -n 's/^__VQ_STATUS__\([0-9][0-9]*\)$/\1/p')
status_count=$(printf '%s\n' "$status_lines" | \
    awk 'NF { n++ } END { print n + 0 }')
output_overflow=0
if [ "$status_count" -eq 1 ]; then
    rc=$status_lines
else
    rc=200
    output_overflow=1
fi
out=$(printf '%s\n' "$capture" | \
    sed '/^__VQ_STDERR__/d; /^__VQ_STATUS__/d')
err=$(printf '%s\n' "$capture" | sed -n 's/^__VQ_STDERR__//p')
scheduler_id=
status=outcome_unknown
if [ "$dialect" = slurm ]; then
    matches=$(awk '
        /^[[:space:]]*Submitted[[:space:]]+batch[[:space:]]+job[[:space:]]+[0-9]+[[:space:]]*$/ {
            value=$0
            sub(/^[[:space:]]*Submitted[[:space:]]+batch[[:space:]]+job[[:space:]]+/, "", value)
            sub(/[[:space:]]*$/, "", value)
            print value
        }
    ' <<EOF
$out
EOF
)
    match_count=$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')
    [ "$match_count" -eq 1 ] && [ "$output_overflow" -eq 0 ] && \
        scheduler_id=$matches
else
    matches=$(awk '
        /^[[:space:]]*[0-9]+(\[[0-9]*\])?(\.[A-Za-z0-9._-]+)?[[:space:]]*$/ {
            value=$0
            sub(/^[[:space:]]*/, "", value)
            sub(/[[:space:]]*$/, "", value)
            print value
        }
    ' <<EOF
$out
EOF
)
    match_count=$(printf '%s\n' "$matches" | awk 'NF { n++ } END { print n + 0 }')
    [ "$match_count" -eq 1 ] && [ "$output_overflow" -eq 0 ] && \
        scheduler_id=$matches
fi
if [ -n "$scheduler_id" ]; then
    status=accepted
elif [ "$match_count" -eq 0 ] && [ "$output_overflow" -eq 0 ] && \
        [ "$rc" -gt 0 ] && [ "$rc" -lt 128 ]; then
    status=rejected
fi
if [ -n "$scheduler_id" ]; then
    scheduler_json="\"$scheduler_id\""
else
    scheduler_json=null
fi
receipt_format='{"schema":"%s","status":"%s","vq_job_id":"%s",'
receipt_format=${receipt_format}'"scheduler_job_id":%s,"scheduler_returncode":%s}\n'
# Noclobber maps the already-checked final name to an exclusive create. A
# racing same-account process can make publication fail, but cannot redirect
# this write through a precreated link. Missing/malformed receipt remains
# outcome-unknown on restart; direct stdout still proves this observed call.
( set -C; printf "$receipt_format" \
    'vq.scheduler-submit-once.v1' "$status" "$vq_job_id" \
    "$scheduler_json" "$rc" >"$receipt" ) 2>/dev/null || :
sync -f "$receipt" 2>/dev/null || :
sync -f "$(dirname -- "$receipt")" 2>/dev/null || :
[ -z "$out" ] || printf '%s' "$out"
[ -z "$err" ] || printf '%s' "$err" >&2
[ "$output_overflow" -eq 0 ] || exit 200
exit "$rc"
'''
        script = (
            script.replace("__VQ_OUTPUT_LIMIT__", str(SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES))
            .replace("__VQ_MARKER_RELDIR__", EXIT_MARKER_RELDIR)
            .replace("__VQ_MARKER_BASENAME__", SCHEDULER_ID_BASENAME)
        )
        try:
            proc = transport.run_remote_shell(
                self._host_cfg,
                "bash",
                "-c",
                script,
                "vq-scheduler-submit-once",
                receipt_path,
                vq_job_id,
                dialect_name,
                remote_workspace,
                "none" if array_size is None else str(array_size),
                *argv,
                check=False,
                timeout=_scheduler_remote_timeout(argv),
                retry_transient=0,
                owned_process_group=True,
                max_stdout_bytes=SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES + 1,
                max_stderr_bytes=SCHEDULER_SUBMIT_OUTPUT_MAX_BYTES + 1,
            )
        except transport.RemoteOutcomeUnknown as exc:
            raise SchedulerRemoteOutcomeUnknown(
                f"remote scheduler submit outcome is unknown on "
                f"{self._host_cfg.ssh}"
            ) from exc
        except transport.RemoteError as exc:
            raise SchedulerError(
                f"remote scheduler submit wrapper failed on {self._host_cfg.ssh}"
            ) from exc
        if proc.returncode in {200, 255} or proc.returncode < 0:
            raise SchedulerRemoteOutcomeUnknown(
                f"remote scheduler submit outcome is unknown on "
                f"{self._host_cfg.ssh}"
            )
        return RemoteResult(proc.returncode, proc.stdout, proc.stderr)

    def upload_tree(self, local_dir: Path, remote_dir: str) -> None:
        """Stage ``local_dir`` into ``remote_dir`` via tar → scp → untar.

        Mirrors the tar+scp+untar pattern vq already uses for remote submit. A
        single tarball is one scp round-trip regardless of file count, and the
        TORQUE login node's pure-ASCII / quoting quirks never touch the payload.

        The staging tarball lands **workspace-adjacent** (``<remote_dir>.upload.tar``,
        on the same shared cluster filesystem as ``remote_dir``), *not* under
        node-local ``/tmp``. The ``scp`` upload and the subsequent ``tar -xf``
        are two separate ssh connections, and a multi-login-node cluster
        (e.g. slurm-cluster's login01/login02) can route them to different nodes. With
        a node-local ``/tmp`` path the tarball scp'd onto one login node is then
        invisible to the untar on the other — ``tar: …: Cannot open: No such
        file or directory`` — which silently aborted the whole dispatch tick.
        A shared-FS path is visible from any login node, so node rotation
        between the two connections no longer matters. This is the same
        workspace-adjacent placement ``fetch_results`` already uses for the
        result tarball (``<ws>.result.tar``); ``remote_dir`` was created on the
        shared FS by ``submit``'s ``mkdir -p`` immediately before this call.
        """
        import tarfile  # noqa: PLC0415
        import tempfile  # noqa: PLC0415

        remote_tar = f"{remote_dir.rstrip('/')}.upload.tar"
        with tempfile.NamedTemporaryFile(suffix=".tar", delete=True) as tmp:
            local_tar = Path(tmp.name)
            with tarfile.open(local_tar, "w") as tf:
                # Archive the directory *contents* (arcname=".") so they land
                # directly under remote_dir, not under a nested local-name dir.
                tf.add(local_dir, arcname=".")
            transport.upload_file(self._host_cfg, local_tar, remote_tar)
        try:
            transport.run_remote_shell(
                self._host_cfg,
                "tar",
                "-xf",
                remote_tar,
                "-C",
                remote_dir,
                check=True,
                timeout=SCHEDULER_ARCHIVE_TIMEOUT_SECONDS,
                retry_transient=1,
            )
        finally:
            transport.run_remote_shell(
                self._host_cfg,
                "rm",
                "-f",
                remote_tar,
                check=False,
                retry_transient=1,
            )

    def upload_file(self, local_path: Path, remote_path: str) -> None:
        # #736: this replays only the same immutable staging archive before
        # unpacking or scheduler submission. Never retry qsub/sbatch here.
        transport.upload_file(
            self._host_cfg, local_path, remote_path, retry_transient=2,
        )

    def download_file(self, remote_path: str, local_path: Path) -> None:
        transport.download_file(self._host_cfg, remote_path, local_path)


def scheduler_dispatcher_for(host_cfg: HostConfig) -> SchedulerDispatcher:
    """Build the :class:`SchedulerDispatcher` for a scheduler-backend host.

    Reads the host's ``scheduler_dialect`` / ``scratch_root`` / ``submit_extra``
    / ``node_scratch_dir`` / ``scheduler_gnu_time_command`` / scheduler script
    hooks / program hooks from its :class:`~vq.config.HostConfig` and wraps an
    :class:`SshRemoteRunner` over its ``ssh`` alias. The daemon calls this once
    per scheduler-target host to get the dispatcher it routes that host's jobs
    through (design doc §17). Raises :class:`SchedulerError` if the host is not
    a scheduler host; HostConfig's own validation already enforces these fields
    when ``scheduler != "local"``, so this guards the misuse case.
    """
    if host_cfg.scheduler_dialect is None or host_cfg.scratch_root is None:
        raise SchedulerError(
            f"host {host_cfg.ssh!r} is not configured as a scheduler host "
            "(scheduler_dialect / scratch_root unset)"
        )
    return SchedulerDispatcher(
        dialect_for(host_cfg.scheduler_dialect),
        SshRemoteRunner(host_cfg),
        scratch_root=host_cfg.scratch_root,
        submit_extra=host_cfg.submit_extra,
        node_scratch_dir=host_cfg.node_scratch_dir,
        scheduler_prologue=host_cfg.scheduler_prologue,
        scheduler_epilogue=host_cfg.scheduler_epilogue,
        scheduler_program_hooks=host_cfg.scheduler_program_hooks,
        mem_directive=host_cfg.scheduler_mem_directive,
        gnu_time_command=host_cfg.scheduler_gnu_time_command,
        max_wall_time_seconds=host_cfg.scheduler_max_wall_time_seconds,
    )


def _scheduler_remote_timeout(argv: Sequence[str]) -> float | None:
    if not argv:
        return transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS
    if argv[0] == "qstat":
        return SCHEDULER_POLL_TIMEOUT_SECONDS
    if argv[0] == "sacct":
        return SCHEDULER_ACCOUNTING_TIMEOUT_SECONDS
    if argv[0] == "tar":
        return SCHEDULER_ARCHIVE_TIMEOUT_SECONDS
    return transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS


def _scheduler_remote_retries(argv: Sequence[str]) -> int:
    if not argv:
        return 0
    if argv[0] == "qstat":
        return 2
    # Workspace creation is the first scheduler-submit operation and is
    # idempotent.  Retry banner-exchange/connect failures here so a transient
    # login-node outage does not fail the job before qsub can assign an id.
    # qsub itself deliberately remains at zero retries: after an ambiguous
    # transport failure, resubmitting it could create a duplicate batch job.
    if argv[0] == "mkdir" and "-p" in argv[1:]:
        return 2
    if argv[0] in {"cat", "tail", "tar", "rm"}:
        return 1
    return 0
