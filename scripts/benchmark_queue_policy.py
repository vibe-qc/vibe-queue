"""Compare queue ownership-policy cost with the pre-#111 TOML copier.

Run from this checkout: PYTHONPATH=src .venv/bin/python
scripts/benchmark_queue_policy.py --rows 17000 --repeats 3

Only synthetic config files in a temporary directory are read. This measures
repeated ownership checks, not a daemon scan, queue I/O, locking or dispatch.
It neither reads nor changes live job state and never starts a daemon.
"""
from __future__ import annotations

import argparse
import copy
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from vq import __version__, config, ownership
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


def _parent_read_config_data(path: Path) -> dict:
    # Exact previous read/copy boundary: both variants reread complete bytes,
    # share the same bounded TOML parse cache, and validate on every call.
    with path.open("rb") as stream:
        return copy.deepcopy(config._parse_config_bytes(stream.read()))


def benchmark(*, rows: int, repeats: int, hosts: int) -> dict:
    if min(rows, repeats, hosts) <= 0:
        raise ValueError("rows, repeats and hosts must be positive")
    samples: dict[str, list[float]] = {"parent": [], "current": []}
    current_reader = config._read_config_data
    with tempfile.TemporaryDirectory(prefix="vq-policy-benchmark-") as temporary:
        root = Path(temporary)
        (root / "config.toml").write_text(_synthetic_config(hosts), encoding="utf-8")
        spec = JobSpec(
            id="benchmark0001", command=["true"], cwd=str(root), cpus=1,
            state=JobState.COMPLETED, submitter=str(os.geteuid()),
        )
        with (
            patch.dict(os.environ, {config.ENV_CONFIG_DIR: str(root)}),
            patch.object(config, "SYSTEM_CONFIG_PATH", root / "absent-system.toml"),
        ):
            # Warm parsing and validation before either timed variant.
            ownership.check_owner(spec)
            for repeat in range(repeats):
                order = ["parent", "current"] if repeat % 2 == 0 else ["current", "parent"]
                for variant in order:
                    reader = _parent_read_config_data if variant == "parent" else current_reader
                    with patch.object(config, "_read_config_data", reader):
                        start = time.perf_counter()
                        for _ in range(rows):
                            ownership.check_owner(spec)
                        samples[variant].append(time.perf_counter() - start)
    medians = {name: statistics.median(values) for name, values in samples.items()}
    return {
        "schema": "vq.queue-policy-benchmark/1",
        "scope": "synthetic single-user ownership checks; no queue scan or dispatch",
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "rows": rows,
        "hosts": hosts,
        "repeats": repeats,
        "seconds": samples,
        "median_seconds": medians,
        "speedup": medians["parent"] / medians["current"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=_positive_int, default=17000)
    parser.add_argument("--repeats", type=_positive_int, default=3)
    parser.add_argument("--hosts", type=_positive_int, default=40)
    args = parser.parse_args()
    print(json.dumps(benchmark(rows=args.rows, repeats=args.repeats, hosts=args.hosts), indent=2))


if __name__ == "__main__":
    main()
