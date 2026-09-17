"""Compare the daemon's per-tick pause-intent sweep with its pre-#22 form.

Run from this checkout: PYTHONPATH=src .venv/bin/python
scripts/benchmark_pause_intent_sweep.py --specs 20000 --repeats 3 --hosts 40

The daemon runs this sweep once per tick to finish any durable pause intent a
killed `vq pause` left behind. A queue retains its terminal jobs, so the sweep
is sized by everything the host has ever run. The `parent` variant is the
default, which takes every spec's lock and loads the authorization config
before looking at the row; `current` is what the daemon now asks for, which
reads the row first and stops there when there is no intent to finish.

Only synthetic specs in a temporary directory are written and read. This
measures the sweep alone, not a daemon tick, dispatch or scheduler polling. It
neither reads nor changes live job state and never starts a daemon.

`--hosts` sizes the synthetic config the authorization check parses on every
locked row, so the saving scales with it the way it does on a real driver.
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
from vq.spec import JobSpec, JobState


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
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


def _write_corpus(queue_dir: Path, specs: int) -> None:
    for index in range(specs):
        jobid = f"benchmark{index:010d}"
        JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(queue_dir),
            cpus=1,
            state=JobState.COMPLETED,
            exit_code=0,
            submitter=str(os.geteuid()),
        ).write(queue_dir / f"{jobid}.json")


def _sweep(queue_dir: Path, *, omit: bool) -> float:
    start = time.perf_counter()
    result = pause_resume.reconcile_pause_intents(
        "localhost", queue_dir=queue_dir, omit_rows_without_intent=omit,
    )
    elapsed = time.perf_counter() - start
    if not result.success:
        raise RuntimeError(
            f"sweep reported {len(result.errors)} error(s) on a synthetic corpus"
        )
    return elapsed


def benchmark(*, specs: int, repeats: int, hosts: int) -> dict:
    if min(specs, repeats, hosts) <= 0:
        raise ValueError("specs, repeats and hosts must be positive")
    samples: dict[str, list[float]] = {"parent": [], "current": []}
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
            _write_corpus(queue_dir, specs)
            # One default sweep first, so every lock sidecar already exists
            # and neither variant is timed while creating them.
            _sweep(queue_dir, omit=False)
            lock_sidecars = len(list(queue_dir.glob("*.json.lock")))
            for repeat in range(repeats):
                order = (
                    [("parent", False), ("current", True)]
                    if repeat % 2 == 0
                    else [("current", True), ("parent", False)]
                )
                for variant, omit in order:
                    samples[variant].append(_sweep(queue_dir, omit=omit))
    medians = {name: statistics.median(values) for name, values in samples.items()}
    return {
        "schema": "vq.pause-intent-sweep-benchmark/1",
        "scope": (
            "synthetic single-user pause-intent sweep over retained terminal "
            "specs; no daemon tick, dispatch or scheduler poll"
        ),
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "specs": specs,
        "hosts": hosts,
        "repeats": repeats,
        "lock_sidecars": lock_sidecars,
        "seconds": samples,
        "median_seconds": medians,
        "speedup": medians["parent"] / medians["current"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specs", type=_positive_int, default=20000)
    parser.add_argument("--repeats", type=_positive_int, default=3)
    parser.add_argument("--hosts", type=_positive_int, default=40)
    args = parser.parse_args()
    print(
        json.dumps(
            benchmark(specs=args.specs, repeats=args.repeats, hosts=args.hosts),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
