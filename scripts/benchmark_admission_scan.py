"""Compare the daemon's per-pass queue scan across the three #22 admission rules.

Run from this checkout::

    PYTHONPATH=src .venv/bin/python scripts/benchmark_admission_scan.py \\
        --live 100 --retained 16000 --archived 0 --repeats 5

Only synthetic spec files in a temporary directory are read. No fleet host is
contacted, no live job state is read, nothing is dispatched and no CPU load is
generated. The daemon it builds is a real ``Daemon`` over a temporary queue
directory -- so ``current`` times the shipped ``Daemon._iter_specs`` rather than
a re-implementation of it -- but the surrounding tick is not run, so this
measures the scan boundary the fix moved and is not a substitute for the driver
measurement the issue's third closure criterion asks for.

Three variants, all walking the same directory with the same glob:

``parent``
    Pre-#22. Opens, JSON-parses and model-validates every retained row.
``archived_only``
    What ``0d30c9e`` shipped: a row is left off the scan when it is *archived*,
    terminal and holds no open scheduler transaction. Archiving is opt-in, so a
    driver that never archives gets nothing from it.
``current``
    ``Daemon._iter_specs`` as it now stands: the archived condition is dropped,
    so every settled terminal row costs a ``stat`` instead of a read.

Two regimes are reported for each. ``first_pass_seconds`` is the cold pass that
records the on-disk identities -- the daemon pays it once per process, in the
startup walk. ``median_seconds`` is the steady state, which is what a tick
costs. ``pause_intent_sweep_seconds`` times ``reconcile_pause_intents``, the
third per-tick pass over the retained corpus, which globs the directory itself
and so takes no part in any of the three scans above. It is timed here as the
daemon ran it between #22 and #82 -- reading every row -- which is the *before*
number for #82; ``scripts/benchmark_pause_intent_sweep.py`` is the instrument
for that sweep and compares it with and without the settled-row verdict the
daemon now hands it.
"""
from __future__ import annotations

import argparse
import json
import platform
import statistics
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

from vq import __version__
from vq.daemon import Daemon, _SettledSpecIdentity, _stat_identity
from vq.pause_resume import reconcile_pause_intents
from vq.spec import JobSpec, JobState, utcnow_iso


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must not be negative")
    return parsed


def _write_queue(queue_dir: Path, *, live: int, retained: int, archived: int) -> None:
    queue_dir.mkdir(parents=True, exist_ok=True)
    for index in range(live):
        jobid = f"live{index:08d}"
        JobSpec(
            id=jobid,
            command=["true", "--synthetic", str(index)],
            cwd=str(queue_dir.parent / "jobs" / jobid),
            cpus=1,
            state=JobState.PENDING,
        ).write(queue_dir / f"{jobid}.json")
    for index in range(retained):
        jobid = f"kept{index:08d}"
        spec = JobSpec(
            id=jobid,
            command=["true", "--synthetic", str(index)],
            cwd=str(queue_dir.parent / "jobs" / jobid),
            cpus=1,
            state=JobState.COMPLETED,
        )
        spec.finished_at = utcnow_iso()
        spec.exit_code = 0
        spec.write(queue_dir / f"{jobid}.json")
    for index in range(archived):
        jobid = f"arch{index:08d}"
        spec = JobSpec(
            id=jobid,
            command=["true", "--synthetic", str(index)],
            cwd=str(queue_dir.parent / "jobs" / jobid),
            cpus=1,
            state=JobState.COMPLETED,
        )
        spec.finished_at = utcnow_iso()
        spec.exit_code = 0
        spec.archived_at = utcnow_iso()
        spec.archive_path = str(queue_dir.parent / "archive" / f"{jobid}.tar.bz2")
        spec.write(queue_dir / f"{jobid}.json")


def _parent_scan(
    queue_dir: Path, _identities: dict[Path, _SettledSpecIdentity]
) -> Iterator[JobSpec]:
    """The pre-#22 scan: open + parse + validate every retained row."""
    for path in sorted(queue_dir.glob("*.json")):
        try:
            yield JobSpec.read(path)
        except Exception:  # noqa: BLE001 - same convention as the daemon
            continue


def _archived_only_scan(
    queue_dir: Path, identities: dict[Path, _SettledSpecIdentity]
) -> Iterator[JobSpec]:
    """``0d30c9e``: only a row an operator archived costs a stat, not an open."""
    spec_paths = sorted(queue_dir.glob("*.json"))
    scanned = set(spec_paths)
    for stale in [p for p in identities if p not in scanned]:
        del identities[stale]
    for path in spec_paths:
        identity = (*_stat_identity(path), *_stat_identity(None))
        if identities.get(path) == identity:
            continue
        try:
            spec = JobSpec.read(path)
        except Exception:  # noqa: BLE001 - same convention as the daemon
            continue
        if (
            path.name == f"{spec.id}.json"
            and spec.is_archived
            and Daemon._spec_is_off_admission_path(spec)
        ):
            identities[path] = identity
            continue
        yield spec


def benchmark(*, live: int, retained: int, archived: int, repeats: int) -> dict:
    variants = ("parent", "archived_only", "current")
    samples: dict[str, list[float]] = {name: [] for name in variants}
    first_pass: dict[str, float] = {}
    yielded: dict[str, int] = {}
    with tempfile.TemporaryDirectory(prefix="vq-admission-scan-benchmark-") as tmp:
        root = Path(tmp)
        queue_dir = root / "queue"
        _write_queue(queue_dir, live=live, retained=retained, archived=archived)
        daemon = Daemon(
            max_cpus=8,
            max_jobs=8,
            poll_interval=1.0,
            queue_dir=queue_dir,
            jobs_dir=root / "jobs",
        )
        try:
            identities: dict[str, dict[Path, _SettledSpecIdentity]] = {
                name: {} for name in variants
            }

            def _scan(name: str) -> Iterator[JobSpec]:
                if name == "parent":
                    return _parent_scan(queue_dir, identities[name])
                if name == "archived_only":
                    return _archived_only_scan(queue_dir, identities[name])
                return daemon._iter_specs()

            # Warm the page cache for every variant, and let the two that keep
            # identities record them -- the daemon pays this once per process,
            # in the startup walk.
            for name in variants:
                start = time.perf_counter()
                yielded[name] = sum(1 for _ in _scan(name))
                first_pass[name] = time.perf_counter() - start
            for repeat in range(repeats):
                order = list(variants) if repeat % 2 == 0 else list(reversed(variants))
                for name in order:
                    start = time.perf_counter()
                    rows = sum(1 for _ in _scan(name))
                    samples[name].append(time.perf_counter() - start)
                    yielded[name] = rows
            sweep: list[float] = []
            for _ in range(repeats):
                start = time.perf_counter()
                reconcile_pause_intents(
                    "localhost",
                    queue_dir=queue_dir,
                    multi_user=False,
                    omit_rows_without_intent=True,
                )
                sweep.append(time.perf_counter() - start)
        finally:
            daemon._queue_lock_fd.close()
    medians = {name: statistics.median(values) for name, values in samples.items()}
    return {
        "schema": "vq.admission-scan-benchmark/2",
        "scope": (
            "synthetic single-user queue directory; scan boundary only, "
            "no dispatch, no fleet host, no generated load"
        ),
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "live_specs": live,
        "retained_terminal_specs": retained,
        "archived_specs": archived,
        "repeats": repeats,
        "rows_yielded": yielded,
        "seconds": samples,
        "first_pass_seconds": first_pass,
        "median_seconds": medians,
        "pause_intent_sweep_seconds": sweep,
        "pause_intent_sweep_median_seconds": statistics.median(sweep),
        "speedup_over_parent": {
            name: medians["parent"] / medians[name] for name in variants
        },
        "speedup_over_archived_only": (
            medians["archived_only"] / medians["current"]
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", type=_positive_int, default=100)
    parser.add_argument("--retained", type=_nonnegative_int, default=16000)
    parser.add_argument("--archived", type=_nonnegative_int, default=0)
    parser.add_argument("--repeats", type=_positive_int, default=5)
    args = parser.parse_args()
    print(
        json.dumps(
            benchmark(
                live=args.live,
                retained=args.retained,
                archived=args.archived,
                repeats=args.repeats,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
