"""Compare the daemon's per-tick pause-intent sweep across its three forms.

Run from this checkout::

    PYTHONPATH=src .venv/bin/python scripts/benchmark_pause_intent_sweep.py \\
        --live 100 --specs 16000 --repeats 3 --hosts 40

The daemon runs this sweep once per tick to finish any durable pause intent a
killed `vq pause` left behind. A queue retains its terminal jobs, so the sweep
is sized by everything the host has ever run rather than by what is live.

Three variants, all walking the same directory with the same glob:

``locked_scan``
    Pre-#22, and still the default the exact proofs take: every spec's lock is
    acquired and the authorization config loaded before the row is looked at.
``unlocked_read``
    What `7e80264` gave the daemon (#22): the row is read first and the lock and
    the ownership check are skipped when it carries no intent. Three file opens
    per retained row become one -- and that one is the cost this benchmark is
    now about, because the read itself remained.
``settled_skip``
    What the daemon now asks for (#82): the same sweep, handed the verdict a
    real ``Daemon._iter_specs`` pass already recorded, so a settled row whose
    bytes have not changed costs a ``stat`` and is never opened.

``settled_skip`` drives the shipped ``Daemon._pause_intent_sweep_may_skip``
over a real ``Daemon``, warmed by one real ``_iter_specs`` pass, so it measures
the code that ships rather than a re-implementation of it. The warming pass is
reported separately as ``warm_pass_seconds``: the daemon pays it in the startup
walk, and the per-tick passes behind it pay a stat.

Only synthetic specs in a temporary directory are written and read. This
measures the sweep alone, not a daemon tick, dispatch or scheduler polling; no
CPU load is generated, no fleet host is contacted, and live job state is
neither read nor changed.

`--hosts` sizes the synthetic config the authorization check parses on every
locked row, so ``locked_scan`` scales with it the way it does on a real driver.
`--live` writes PENDING rows, which every variant must read: no verdict is ever
recorded for a row that is not settled.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from vq import __version__, config, paths, pause_resume
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState, utcnow_iso

_VARIANTS = ("locked_scan", "unlocked_read", "settled_skip")


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


def _synthetic_config(hosts: int) -> str:
    return 'default_host = "cluster0"\n' + "".join(
        f'\n[hosts.cluster{index}]\nssh = "cluster{index}"\n'
        'scheduler = "slurm"\nscheduler_dialect = "slurm"\n'
        'scratch_root = "/scratch"\nscheduler_driver = "driver"\n'
        'submit_extra = ["--exclusive", "--export=ALL"]\n'
        f'[hosts.cluster{index}.branches]\n'
        'main = "/opt/example/dev/bin/python"\n'
        'release = "/opt/example/release/bin/python"\n'
        f'[hosts.cluster{index}.branch_aliases]\ndev = "main"\n'
        for index in range(hosts)
    )


def _write_corpus(queue_dir: Path, *, specs: int, live: int) -> None:
    for index in range(specs):
        jobid = f"benchmark{index:010d}"
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(queue_dir),
            cpus=1,
            state=JobState.COMPLETED,
            exit_code=0,
            finished_at=utcnow_iso(),
            submitter=str(os.geteuid()),
        ).write(queue_dir / f"{jobid}.json")
    for index in range(live):
        jobid = f"livebench{index:010d}"
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(queue_dir),
            cpus=1,
            state=JobState.PENDING,
            submitter=str(os.geteuid()),
        ).write(queue_dir / f"{jobid}.json")


def _sweep(queue_dir: Path, *, variant: str, daemon: Daemon) -> float:
    kwargs: dict[str, object] = {}
    if variant != "locked_scan":
        kwargs["omit_rows_without_intent"] = True
    if variant == "settled_skip":
        kwargs["rows_proven_without_intent"] = daemon._pause_intent_sweep_may_skip
    start = time.perf_counter()
    result = pause_resume.reconcile_pause_intents(
        "localhost", queue_dir=queue_dir, **kwargs,  # type: ignore[arg-type]
    )
    elapsed = time.perf_counter() - start
    if not result.success:
        raise RuntimeError(
            f"sweep reported {len(result.errors)} error(s) on a synthetic corpus"
        )
    return elapsed


def benchmark(*, specs: int, live: int, repeats: int, hosts: int) -> dict:
    if min(specs, repeats, hosts) <= 0 or live < 0:
        raise ValueError("specs, repeats and hosts must be positive")
    samples: dict[str, list[float]] = {name: [] for name in _VARIANTS}
    with tempfile.TemporaryDirectory(prefix="vq-pause-sweep-benchmark-") as tmp:
        root = Path(tmp)
        (root / "cfg").mkdir()
        (root / "cfg" / "config.toml").write_text(
            _synthetic_config(hosts), encoding="utf-8",
        )
        with patch.dict(
            os.environ,
            {paths.ENV_STATE_DIR: str(root / "state"),
             config.ENV_CONFIG_DIR: str(root / "cfg")},
        ):
            queue_dir = paths.queue_dir()
            queue_dir.mkdir(parents=True, exist_ok=True)
            jobs_dir = paths.jobs_dir()
            jobs_dir.mkdir(parents=True, exist_ok=True)
            _write_corpus(queue_dir, specs=specs, live=live)
            daemon = Daemon(
                max_cpus=8,
                max_jobs=8,
                poll_interval=1.0,
                queue_dir=queue_dir,
                jobs_dir=jobs_dir,
            )
            try:
                # One default sweep first, so every lock sidecar already exists
                # and no variant is timed while creating them.
                _sweep(queue_dir, variant="locked_scan", daemon=daemon)
                lock_sidecars = len(list(queue_dir.glob("*.json.lock")))
                # The pass that records the verdict. The daemon pays this in
                # its startup walk, once per process, and it reads every row.
                start = time.perf_counter()
                yielded = sum(1 for _ in daemon._iter_specs())
                warm_pass = time.perf_counter() - start
                proven = len(daemon._settled_spec_without_pause_intent)
                for repeat in range(repeats):
                    order = (
                        list(_VARIANTS)
                        if repeat % 2 == 0
                        else list(reversed(_VARIANTS))
                    )
                    for variant in order:
                        samples[variant].append(
                            _sweep(queue_dir, variant=variant, daemon=daemon)
                        )
            finally:
                daemon._queue_lock_fd.close()
    medians = {name: statistics.median(values) for name, values in samples.items()}
    return {
        "schema": "vq.pause-intent-sweep-benchmark/2",
        "scope": (
            "synthetic single-user pause-intent sweep over retained terminal "
            "specs; no daemon tick, dispatch or scheduler poll"
        ),
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "specs": specs,
        "live_specs": live,
        "hosts": hosts,
        "repeats": repeats,
        "lock_sidecars": lock_sidecars,
        "rows_yielded_by_warm_pass": yielded,
        "rows_proven_settled_without_intent": proven,
        "warm_pass_seconds": warm_pass,
        "seconds": samples,
        "median_seconds": medians,
        "speedup": {
            "settled_skip_over_unlocked_read": (
                medians["unlocked_read"] / medians["settled_skip"]
            ),
            "settled_skip_over_locked_scan": (
                medians["locked_scan"] / medians["settled_skip"]
            ),
            "unlocked_read_over_locked_scan": (
                medians["locked_scan"] / medians["unlocked_read"]
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specs", type=_positive_int, default=16000)
    parser.add_argument("--live", type=_nonnegative_int, default=100)
    parser.add_argument("--repeats", type=_positive_int, default=3)
    parser.add_argument("--hosts", type=_positive_int, default=40)
    args = parser.parse_args()
    print(
        json.dumps(
            benchmark(
                specs=args.specs,
                live=args.live,
                repeats=args.repeats,
                hosts=args.hosts,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
