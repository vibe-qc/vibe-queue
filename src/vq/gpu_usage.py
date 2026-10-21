"""What a scheduler job's GPUs measurably did, from ``nvidia-smi`` samples (#168).

A scheduler job that requests ``--gpus`` samples ``nvidia-smi`` on its batch
node while its command runs (:func:`job_script_lines`). The node only records
raw text in ``_vq/gpu-samples.txt``, size-capped: every sample has the devices
the node shows and the GPU processes descending from the job script. Parsing,
validation and attribution happen here, on the driver, once the job has
finished and its workspace has been fetched.

What can be attributed depends on what the node shows:

* exactly the allocated number of devices: they are the job's whole-GPU
  allocation, so their device-level figures are reported;
* more: only devices a process of the job was seen on are reported;
* fewer: the batch node's devices are reported as partial, since a
  multi-node job's other nodes are not observed.

Utilisation and device memory are device-level: the device as a whole, not
this job's share of it. Per-process memory is reported where a process of the
job was seen: one descending from the job script or, on Slurm, one in this
job's cgroup (an ``srun`` step's tasks are slurmstepd's children). ``[N/A]``
and ``[Not Supported]`` are unknown, never zero; GPU UUIDs are used only to
join the two queries and are never shown.
"""

from __future__ import annotations

import json
import math
import os
import re
import shlex
import stat
from pathlib import Path
from typing import Any

SAMPLES_BASENAME = "gpu-samples.txt"
SAMPLES_SCHEMA = "vq.gpu-samples/1"
#: The finished job's summary, written once after its workspace is fetched.
SUMMARY_BASENAME = "gpu-usage.json"
SUMMARY_SCHEMA = "vq.gpu-usage/1"
MAX_SUMMARY_BYTES = 1024 * 1024
#: A scheduler array is summarised up to this many elements and bytes.
MAX_ARRAY_ELEMENTS = 256
MAX_ARRAY_BYTES = 64 * 1024 * 1024
SAMPLE_INTERVAL_SECONDS = 30
#: Where the node's process table is read for cgroup membership.
PROC_ROOT = "/proc"
MAX_SAMPLES_BYTES = 8 * 1024 * 1024
MAX_DEVICES = 64

_UUID = re.compile(r"^(GPU|MIG)-[0-9A-Za-z-]{1,80}$")
_UNKNOWN = {"[N/A]", "[Not Supported]", "N/A", "[Unknown Error]", "[Insufficient Permissions]"}


def job_script_lines(samples_path: str) -> list[str]:
    """Shell functions the job script calls around its command.

    ``__vq_gpu_start`` begins sampling into the file named by the shell
    expression ``samples_path``; ``__vq_gpu_stop`` ends it, also from a trap.
    Without ``nvidia-smi`` the file records that and nothing is sampled.
    """
    gpu_query = ("__vq_gpu_smi --query-gpu=uuid,index,utilization.gpu,memory.used,"
                 "memory.total,name --format=csv,noheader,nounits")
    apps_query = ("__vq_gpu_smi --query-compute-apps=gpu_uuid,pid,used_memory "
                  "--format=csv,noheader,nounits")
    # Tag each compute app P (descends from the job script) or N PID (not).
    descendants = (
        'FNR == NR { parent[$1] = $2; next } '
        '{ split($0, f, ", "); pid = f[2] + 0; p = pid; '
        'for (depth = 0; depth < 64 && p > 1; depth++) { '
        'if (p == root) { print "P " $0; next } p = parent[p] + 0 } '
        'print "N " pid " " $0 }'
    )
    return [
        f"__vq_gpu_samples={samples_path}",
        "__vq_gpu_pid=",
        # A hung nvidia-smi must not hold up the job's end: bound each call
        # where the node has timeout(1).
        "__vq_gpu_smi() {",
        "  if command -v timeout >/dev/null 2>&1; then timeout 10 nvidia-smi \"$@\"; "
        "else nvidia-smi \"$@\"; fi",
        "}",
        "__vq_gpu_sample() {",
        '  printf "T %s\\n" "$(date +%s)"',
        f"  if ! __vq_gpu_out=$({gpu_query} 2>/dev/null); then",
        '    printf "E query-gpu failed\\n"; return 0',
        "  fi",
        "  printf '%s\\n' \"$__vq_gpu_out\" | sed 's/^/G /'",
        f"  if ! __vq_gpu_apps=$({apps_query} 2>/dev/null); then",
        '    printf "E query-compute-apps failed\\n"; return 0',
        "  fi",
        '  [ -n "$__vq_gpu_apps" ] || return 0',
        "  printf '%s\\n' \"$__vq_gpu_apps\" > \"$__vq_gpu_samples.apps\"",
        '  ps -eo pid=,ppid= > "$__vq_gpu_samples.ps" 2>/dev/null || '
        '{ printf "E ps failed\\n"; return 0; }',
        f'  LC_ALL=C awk -v root="$__vq_gpu_root" {shlex.quote(descendants)} '
        '"$__vq_gpu_samples.ps" "$__vq_gpu_samples.apps" | '
        "while IFS= read -r __vq_gpu_row; do",
        '    case "$__vq_gpu_row" in',
        "      'P '*) printf '%s\\n' \"$__vq_gpu_row\" ;;",
        '      *) __vq_gpu_rest=${__vq_gpu_row#N }; __vq_gpu_pid_of=${__vq_gpu_rest%% *}',
        '         __vq_gpu_in_job "$__vq_gpu_pid_of" && '
        "printf 'P %s\\n' \"${__vq_gpu_rest#* }\" ;;",
        "    esac",
        "  done",
        "}",
        # An srun step's processes are slurmstepd's children, not the script's,
        # but Slurm's cgroup path names this job: .../job_<SLURM_JOB_ID>/...
        "__vq_gpu_in_job() {",
        '  case "${SLURM_JOB_ID:-}" in ""|*[!0-9]*) return 1 ;; esac',
        f'  grep -qE "/job_${{SLURM_JOB_ID}}(/|\\$)" {shlex.quote(PROC_ROOT)}/"$1"/cgroup '
        "2>/dev/null",
        "}",
        "__vq_gpu_loop() {",
        "  __vq_gpu_sleep=",
        "  trap '[ -z \"$__vq_gpu_sleep\" ] || kill \"$__vq_gpu_sleep\" 2>/dev/null; exit 0' TERM",
        "  while :; do",
        '    __vq_gpu_size=$(wc -c < "$__vq_gpu_samples" 2>/dev/null) || __vq_gpu_size=0',
        f"    if [ $((__vq_gpu_size + 0)) -ge {MAX_SAMPLES_BYTES} ]; then",
        '      printf "X sample cap reached\\n" >> "$__vq_gpu_samples"; return 0',
        "    fi",
        '    __vq_gpu_sample >> "$__vq_gpu_samples" 2>/dev/null',
        f"    sleep {SAMPLE_INTERVAL_SECONDS} & __vq_gpu_sleep=$!",
        '    wait "$__vq_gpu_sleep"',
        "  done",
        "}",
        "__vq_gpu_start() {",
        "  __vq_gpu_root=$$",
        f'  printf "V {SAMPLES_SCHEMA} interval=%s\\n" {SAMPLE_INTERVAL_SECONDS} '
        '> "$__vq_gpu_samples" 2>/dev/null || return 0',
        "  if ! command -v nvidia-smi >/dev/null 2>&1; then",
        '    printf "U nvidia-smi not found\\n" >> "$__vq_gpu_samples"; return 0',
        "  fi",
        "  __vq_gpu_loop &",
        "  __vq_gpu_pid=$!",
        "}",
        "__vq_gpu_stop() {",
        '  [ -n "${__vq_gpu_pid:-}" ] || return 0',
        '  kill "$__vq_gpu_pid" 2>/dev/null; wait "$__vq_gpu_pid" 2>/dev/null',
        "  __vq_gpu_pid=",
        '  rm -f "$__vq_gpu_samples.apps" "$__vq_gpu_samples.ps"',
        '  printf "S %s\\n" "$(date +%s)" >> "$__vq_gpu_samples" 2>/dev/null || true',
        "}",
    ]


class _Invalid(ValueError):
    pass


def _number(value: str, what: str) -> float | None:
    value = value.strip()
    if value in _UNKNOWN:
        return None
    try:
        number = float(value)
    except ValueError:
        raise _Invalid(f"{what} is not a number") from None
    if not math.isfinite(number) or number < 0:
        raise _Invalid(f"{what} is not a finite non-negative number")
    return number


def _read(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise _Invalid("the samples are not a regular file")
        chunks, size = [], 0
        while size <= MAX_SAMPLES_BYTES + 64 * 1024:
            chunk = os.read(fd, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
    finally:
        os.close(fd)
    if size > MAX_SAMPLES_BYTES + 64 * 1024:
        raise _Invalid("the samples are larger than their cap")
    return b"".join(chunks)


def _parse(text: str) -> dict[str, Any]:
    lines = text.splitlines()
    if not lines or not re.fullmatch(rf"V {re.escape(SAMPLES_SCHEMA)} interval=(\d+)", lines[0]):
        raise _Invalid(f"not a {SAMPLES_SCHEMA} file")
    interval = int(lines[0].rsplit("=", 1)[1])
    samples, errors, truncated = _parse_rows(lines[1:], first_number=2)
    return {"interval": interval, "samples": samples, "errors": errors,
            "truncated": truncated}


def _parse_rows(lines: list[str], *, first_number: int,
                ) -> tuple[list[dict[str, Any]], list[str], bool]:
    """The samples, errors and cap marker in ``lines``, which follow a header."""
    samples: list[dict[str, Any]] = []
    errors: list[str] = []
    truncated = False
    for number, line in enumerate(lines, start=first_number):
        kind, _, rest = line.partition(" ")
        if kind == "T":
            samples.append({"devices": {}, "processes": []})
        elif kind in {"U", "E"}:  # U: nothing could be sampled; E: one query failed
            errors.append(rest)
        elif kind == "X":
            truncated = True
        elif kind == "S":
            continue
        elif kind in {"G", "P"} and samples:
            fields = rest.split(", ", 5 if kind == "G" else 2)
            if kind == "G" and len(fields) == 6 and _UUID.match(fields[0]):
                uuid, index, util, used, total, name = fields
                if not index.strip().isdigit():
                    raise _Invalid(f"line {number}: device index is not an integer")
                samples[-1]["devices"][uuid] = {
                    "index": int(index), "name": name.strip()[:128],
                    "utilization": _number(util, f"line {number}: utilization"),
                    "memory_used_mb": _number(used, f"line {number}: memory used"),
                    "memory_total_mb": _number(total, f"line {number}: memory total"),
                }
            elif kind == "P" and len(fields) == 3 and _UUID.match(fields[0]):
                samples[-1]["processes"].append(
                    (fields[0], _number(fields[2], f"line {number}: process memory")))
            else:
                raise _Invalid(f"line {number}: malformed {kind} row")
            if len(samples[-1]["devices"]) > MAX_DEVICES:
                raise _Invalid(f"more than {MAX_DEVICES} devices in one sample")
        else:
            raise _Invalid(f"line {number}: unexpected row")
    return samples, errors, truncated


def _device_summary(uuid: str, samples: list[dict[str, Any]]) -> dict[str, Any]:
    seen = [s["devices"][uuid] for s in samples if uuid in s["devices"]]
    utilization = [d["utilization"] for d in seen if d["utilization"] is not None]
    memory = [d["memory_used_mb"] for d in seen if d["memory_used_mb"] is not None]
    totals = [d["memory_total_mb"] for d in seen if d["memory_total_mb"] is not None]
    job_memory: list[float] = []
    for sample in samples:
        held = [mem for owner, mem in sample["processes"] if owner == uuid]
        if held:
            known = [mem for mem in held if mem is not None]
            if len(known) == len(held):
                job_memory.append(math.fsum(known))
    return {
        "index": seen[-1]["index"],
        "name": seen[-1]["name"],
        "memory_total_mb": max(totals) if totals else None,
        "device_peak_memory_mb": max(memory) if memory else None,
        "utilization_mean_percent": (round(math.fsum(utilization) / len(utilization), 1)
                                     if utilization else None),
        "utilization_peak_percent": max(utilization) if utilization else None,
        "job_peak_memory_mb": max(job_memory) if job_memory else None,
        "samples": len(seen),
    }


def _summarise_file(path: Path, allocated: int) -> dict[str, Any]:
    """One samples file's GPU use: ``ok``, ``unavailable`` or ``invalid``."""
    try:
        parsed = _parse(_read(path).decode("utf-8", errors="strict"))
    except (OSError, UnicodeError, _Invalid) as exc:
        return {"status": "invalid", "error": str(exc) or type(exc).__name__}
    base = {"collector": "nvidia-smi", "interval_seconds": parsed["interval"],
            "samples": len(parsed["samples"]), "truncated": parsed["truncated"],
            "errors": len(parsed["errors"]), "allocated": allocated}
    sampled = [s for s in parsed["samples"] if s["devices"]]
    if not sampled:
        reason = parsed["errors"][0] if parsed["errors"] else "no sample was taken"
        return {"status": "unavailable", "reason": reason, **base}
    visible = {uuid for s in sampled for uuid in s["devices"]}
    used = {owner for s in sampled for owner, _ in s["processes"]} & visible
    if len(visible) == allocated:
        reported, coverage = visible, "allocation"
    elif len(visible) < allocated:
        reported, coverage = visible, "partial"
    elif used:
        reported, coverage = used, "attributed"
    else:
        return {"status": "unavailable", **base, "visible": len(visible),
                "reason": (f"the node shows {len(visible)} GPUs for an allocation of "
                           f"{allocated}, and no process of this job was seen on one")}
    # By index, then (never shown) identity, so equal indices order the same every run.
    summaries = sorted(((_device_summary(uuid, sampled), uuid) for uuid in reported),
                       key=lambda pair: (pair[0]["index"], pair[1]))
    devices = [summary for summary, _uuid in summaries]
    return {"status": "ok", **base, "visible": len(visible), "coverage": coverage,
            "devices": devices}


def _sample_files(marker_dir: Path) -> list[tuple[int | None, Path]]:
    """The single job's samples, or an array's elements' in index order."""
    single = marker_dir / SAMPLES_BASENAME
    if os.path.lexists(single):
        return [(None, single)]
    try:
        names = os.listdir(marker_dir)
    except OSError:
        return []
    prefix = SAMPLES_BASENAME + "."
    return sorted((int(name[len(prefix):]), marker_dir / name) for name in names
                  if name.startswith(prefix) and name[len(prefix):].isdigit())


def _summarise_array(files: list[tuple[int | None, Path]], allocated: int) -> dict[str, Any]:
    """Each element on its own (elements may run on different nodes), then together."""
    if len(files) > MAX_ARRAY_ELEMENTS:
        return {"status": "unavailable", "elements": len(files),
                "reason": (f"a scheduler array of {len(files)} elements is not summarised "
                           f"(more than {MAX_ARRAY_ELEMENTS})")}
    try:
        size = sum(path.lstat().st_size for _index, path in files)
    except OSError as exc:
        return {"status": "invalid", "error": str(exc), "elements": len(files)}
    if size > MAX_ARRAY_BYTES:
        return {"status": "unavailable", "elements": len(files),
                "reason": (f"the array's GPU samples are larger than {MAX_ARRAY_BYTES} bytes "
                           "and are not summarised")}
    parts = [(index, _summarise_file(path, allocated)) for index, path in files]
    for index, part in parts:
        if part["status"] == "invalid":
            return {"status": "invalid", "error": f"element {index}: {part['error']}",
                    "elements": len(parts)}
    measured = [(index, part) for index, part in parts if part["status"] == "ok"]
    if not measured:
        return {"status": "unavailable", "elements": len(parts),
                "reason": f"element {parts[0][0]}: {parts[0][1]['reason']}"}
    coverages = {part["coverage"] for _index, part in measured}
    return {
        "status": "ok", "collector": "nvidia-smi",
        "interval_seconds": measured[0][1]["interval_seconds"],
        "samples": sum(part["samples"] for _index, part in parts),
        "truncated": any(part["truncated"] for _index, part in parts),
        "errors": sum(part["errors"] for _index, part in parts),
        "allocated": allocated,
        "visible": sum(part["visible"] for _index, part in measured),
        "coverage": coverages.pop() if len(coverages) == 1 else "mixed",
        "elements": [{"index": index, "status": part["status"],
                      **({"coverage": part["coverage"]} if part["status"] == "ok"
                         else {"reason": part["reason"]})} for index, part in parts],
        "devices": [{**device, "element": index}
                    for index, part in measured for device in part["devices"]],
    }


def summarise(workspace: Path, allocated: int) -> dict[str, Any] | None:
    """The finished job's GPU use from its raw samples, or None without any.

    ``status`` is ``ok`` with the attributed ``devices``; ``unavailable``
    when nothing could be sampled or attributed (``reason`` says why);
    ``invalid`` when the samples cannot be trusted (``error``). A scheduler
    array's elements are summarised each on its own and listed in
    ``elements``; its ``devices`` carry their ``element``.
    """
    files = _sample_files(workspace / "_vq")
    if not files:
        return None
    if files[0][0] is None:
        return _summarise_file(files[0][1], allocated)
    return _summarise_array(files, allocated)


def _source(files: list[tuple[int | None, Path]]) -> dict[str, list[int]]:
    stats = {path.name: path.lstat() for _index, path in files}
    return {name: [info.st_size, info.st_mtime_ns] for name, info in stats.items()}


def write_summary(workspace: Path, allocated: int) -> None:
    """Record the finished job's GPU use once, so listings never parse samples.

    Written next to the samples it summarises, with their sizes and mtimes; a
    reader ignores it once they change. Called after the driver fetches the
    workspace. Raises OSError when it cannot write.
    """
    marker_dir = workspace / "_vq"
    files = _sample_files(marker_dir)
    usage = summarise(workspace, allocated)
    if usage is None:
        return
    record = {"schema": SUMMARY_SCHEMA, "source": _source(files), "usage": usage}
    target = marker_dir / SUMMARY_BASENAME
    staging = marker_dir / f".{SUMMARY_BASENAME}.{os.getpid()}.tmp"
    staging.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
    os.replace(staging, target)


def _valid_number(value: object) -> bool:
    return value is None or (type(value) in {int, float} and math.isfinite(value)  # type: ignore[arg-type]
                             and value >= 0)  # type: ignore[operator]


def _valid_usage(usage: object) -> bool:
    """Whether a recorded summary has exactly the shape :func:`summarise` gives."""
    if not isinstance(usage, dict) or usage.get("status") not in {"ok", "unavailable",
                                                                   "invalid"}:
        return False
    if usage["status"] != "ok":
        return isinstance(usage.get("reason", usage.get("error")), str)
    devices = usage.get("devices")
    if not isinstance(devices, list) or len(devices) > MAX_DEVICES * MAX_ARRAY_ELEMENTS:
        return False
    numbers = ("memory_total_mb", "device_peak_memory_mb", "utilization_mean_percent",
               "utilization_peak_percent", "job_peak_memory_mb")
    for device in devices:
        if not (isinstance(device, dict) and type(device.get("index")) is int
                and device["index"] >= 0 and isinstance(device.get("name"), str)
                and len(device["name"]) <= 128
                and all(_valid_number(device.get(key)) for key in numbers)):
            return False
    return (usage.get("coverage") in {"allocation", "attributed", "partial", "mixed"}
            and all(type(usage.get(key)) is int and usage[key] >= 0
                    for key in ("samples", "errors", "interval_seconds")))


def _read_summary(workspace: Path) -> dict[str, Any] | None:
    """A recorded summary that still matches its samples, or None."""
    marker_dir = workspace / "_vq"
    try:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        fd = os.open(marker_dir / SUMMARY_BASENAME, flags)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                return None
            data = os.read(fd, MAX_SUMMARY_BYTES + 1)
        finally:
            os.close(fd)
        if len(data) > MAX_SUMMARY_BYTES:
            return None
        record = json.loads(data)
        if (not isinstance(record, dict) or record.get("schema") != SUMMARY_SCHEMA
                or record.get("source") != _source(_sample_files(marker_dir))
                or not _valid_usage(record.get("usage"))):
            return None
    except (OSError, ValueError):
        return None
    usage: dict[str, Any] = record["usage"]
    return usage


def read_gpu_usage(workspace: Path, allocated: int, *,
                   summary_only: bool = False) -> dict[str, Any] | None:
    """The finished job's GPU use, or None when it left no samples at all.

    A recorded summary that still matches the samples is used as it is.
    ``summary_only`` (listings) never parses raw samples: without such a
    summary it is None. Otherwise the samples are summarised now, as
    :func:`summarise` describes.
    """
    recorded = _read_summary(workspace)
    if recorded is not None or summary_only:
        return recorded
    return summarise(workspace, allocated)


def format_gpu_usage(usage: dict[str, Any] | None) -> str | None:
    """One line for `vq status`, or None when there are no samples."""
    if usage is None:
        return None
    if usage["status"] == "invalid":
        return f"not shown: {usage['error']}"
    if usage["status"] == "unavailable":
        return f"not measured: {usage['reason']}"

    def mb(value: float | None) -> str:
        return "unknown" if value is None else f"{value:.0f} MB"

    def percent(value: float | None) -> str:
        return "unknown" if value is None else f"{value:g}%"

    parts = []
    if "elements" in usage:
        # An array: one line over all elements' devices, never a sum of peaks.
        devices = usage["devices"]
        means = [d["utilization_mean_percent"] for d in devices
                 if d["utilization_mean_percent"] is not None]
        known = [d for d in devices if d["device_peak_memory_mb"] is not None]
        largest = max(known, key=lambda d: d["device_peak_memory_mb"], default=None)
        parts.append(
            f"{len(usage['elements'])} array elements, {len(devices)} devices: utilisation "
            f"mean {percent(round(math.fsum(means) / len(means), 1) if means else None)}, "
            "largest device memory peak "
            + (f"{mb(largest['device_peak_memory_mb'])} of {mb(largest['memory_total_mb'])} "
               f"(element {largest['element']}, GPU {largest['index']})"
               if largest is not None else "unknown"))
    for device in usage["devices"] if "elements" not in usage else []:
        job = (f"job processes peak {mb(device['job_peak_memory_mb'])}, "
               if device["job_peak_memory_mb"] is not None else "")
        parts.append(f"GPU {device['index']} {device['name']}: {job}device memory peak "
                     f"{mb(device['device_peak_memory_mb'])} of "
                     f"{mb(device['memory_total_mb'])}, utilisation mean "
                     f"{percent(device['utilization_mean_percent'])} peak "
                     f"{percent(device['utilization_peak_percent'])}")
    coverage = {"allocation": "the allocated devices",
                "attributed": "devices this job's processes used",
                "partial": "the batch node only; other nodes not observed",
                "mixed": "per element, which differs between elements"}[usage["coverage"]]
    notes = [f"device-level, {coverage}", f"{usage['samples']} samples every "
             f"{usage['interval_seconds']} s"]
    if usage["truncated"]:
        notes.append("sampling stopped at its size cap")
    return "; ".join(parts) + f" ({', '.join(notes)})"


# --- a running job's newest sample (#168) ------------------------------------

LIVE_SCHEMA = "vq.gpu-live/1"
#: How much of each running job's samples one poll reads, and how many jobs
#: share one remote command.
LIVE_TAIL_BYTES = 32 * 1024
LIVE_JOBS_PER_READ = 32


def live_read_command(paths: list[str]) -> list[str]:
    """One remote command that prints the end of each samples file in ``paths``.

    Each file's bytes follow a ``\\x1e``PATH line; a missing file prints
    nothing after its marker.
    """
    script = (f'for p do printf "\\036%s\\n" "$p"; tail -c {LIVE_TAIL_BYTES} -- "$p" '
              '2>/dev/null; printf "\\n"; done')
    return ["sh", "-c", script, "vq-gpu-live", *paths]


def latest_sample(text: str) -> dict[str, Any] | None:
    """The newest complete sample in the end of a samples file, or None.

    A sample is complete once a later sample, the stop line or the cap line
    follows it, so the one being written is never read half. The first line of
    a tail may be cut, so reading starts at the first sample. Invalid rows give
    None: a live figure is telemetry, never a reason to fail.
    """
    lines = text.splitlines()
    starts = [i for i, line in enumerate(lines) if line.startswith("T ")]
    ends = [i for i, line in enumerate(lines) if line[:2] in {"T ", "S ", "X "}]
    complete = [(start, next(end for end in ends if end > start))
                for start in starts if any(end > start for end in ends)]
    if not complete:
        return None
    start, end = complete[-1]
    try:
        observed_at = int(lines[start][2:])
        samples, _errors, _truncated = _parse_rows(lines[start:end], first_number=start + 1)
    except (ValueError, _Invalid):
        return None
    if len(samples) != 1 or not samples[0]["devices"]:
        return None
    sample = samples[0]
    devices = []
    for uuid, device in sorted(sample["devices"].items(),
                               key=lambda item: (item[1]["index"], item[0])):
        held = [mem for owner, mem in sample["processes"] if owner == uuid]
        known = [mem for mem in held if mem is not None]
        devices.append({
            "index": device["index"], "name": device["name"],
            "utilization_percent": device["utilization"],
            "memory_used_mb": device["memory_used_mb"],
            "memory_total_mb": device["memory_total_mb"],
            "job_process": bool(held),
            "job_memory_mb": math.fsum(known) if held and len(known) == len(held) else None,
        })
    return {"schema": LIVE_SCHEMA, "observed_at": observed_at, "devices": devices}


def parse_live_read(stdout: str) -> dict[str, dict[str, Any]]:
    """Each samples file's newest complete sample from :func:`live_read_command`."""
    found: dict[str, dict[str, Any]] = {}
    for section in stdout.split("\x1e")[1:]:
        path, _, body = section.partition("\n")
        sample = latest_sample(body)
        if sample is not None:
            found[path] = sample
    return found


def live_view(live: dict[str, Any] | None, allocated: int) -> dict[str, Any] | None:
    """What a running job's newest sample can say, under the finished-job rules."""
    if not isinstance(live, dict) or live.get("schema") != LIVE_SCHEMA:
        return None
    devices = live.get("devices")
    if not isinstance(devices, list) or not devices:
        return None
    if len(devices) == allocated:
        reported, coverage = devices, "allocation"
    elif len(devices) < allocated:
        reported, coverage = devices, "partial"
    else:
        reported = [d for d in devices if d.get("job_process")]
        coverage = "attributed"
        if not reported:
            return {"status": "unavailable", "observed_at": live.get("observed_at"),
                    "reason": (f"the node shows {len(devices)} GPUs for an allocation of "
                               f"{allocated}, and no process of this job is on one")}
    return {"status": "ok", "observed_at": live.get("observed_at"), "coverage": coverage,
            "visible": len(devices), "allocated": allocated, "devices": reported}


def format_gpu_live(live: dict[str, Any] | None, allocated: int) -> str | None:
    """One `vq status` line for a running job's newest sample, or None."""
    from datetime import UTC, datetime

    view = live_view(live, allocated)
    if view is None:
        return None
    observed = view.get("observed_at")
    when = (datetime.fromtimestamp(observed, UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            if isinstance(observed, int) else "an unknown time")
    if view["status"] == "unavailable":
        return f"not measured: {view['reason']} (sampled {when}, node clock)"

    def mb(value: object) -> str:
        return f"{value:.0f} MB" if isinstance(value, (int, float)) else "unknown"

    def percent(value: object) -> str:
        return f"{value:g}%" if isinstance(value, (int, float)) else "unknown"

    parts = []
    for device in view["devices"]:
        job = (f", job processes {mb(device.get('job_memory_mb'))}"
               if device.get("job_process") else "")
        parts.append(f"GPU {device.get('index')} {device.get('name')}: utilisation "
                     f"{percent(device.get('utilization_percent'))}, device memory "
                     f"{mb(device.get('memory_used_mb'))} of "
                     f"{mb(device.get('memory_total_mb'))}{job}")
    coverage = {"allocation": "the allocated devices",
                "attributed": "devices this job's processes use",
                "partial": "the batch node only; other nodes not observed"}[view["coverage"]]
    return "; ".join(parts) + f" (device-level, {coverage}; sampled {when}, node clock)"
