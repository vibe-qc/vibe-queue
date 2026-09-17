"""Compare a bulk pause/resume verb's authorization cost with its pre-#22 form.

Run from this checkout: PYTHONPATH=src .venv/bin/python
scripts/benchmark_bulk_control_authorization.py --specs 16000 --repeats 3

`vq pause --all`, `vq resume --all` and the pause-token proofs check ownership
for every row in the queue. A queue retains its terminal jobs, so that is sized
by everything the host has ever run. The expensive half of an ownership
decision does not depend on the row: it reads and validates the personal and
system configs, and in multi-user mode resolves the caller's group and passwd
entries. The `parent` variant pays that half per row, which is what the verbs
did; `current` resolves it once for the operation and checks every row against
it.

Only synthetic specs in a temporary directory are written and read. This
measures the bulk verb alone, not a daemon tick, dispatch or scheduler polling.
It neither reads nor changes live job state and never starts a daemon. Every
row is terminal, so the verbs signal nothing.

`--hosts` sizes the synthetic config the authorization check parses, so the
saving scales with it the way it does on a real driver.
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

from vq import __version__, config, ownership, paths, pause_resume
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


def _scan(spec_paths: list[Path], *, resolve_once: bool) -> float:
    """Time the verbs' per-row authorization filter over a retained queue.

    ``resolve_once`` false is the exact pre-#22 path: the filter resolves the
    policy inside every row's check, which is what passing no policy still
    does for a single-job caller.
    """
    policy = ownership.authorization_policy() if resolve_once else None
    start = time.perf_counter()
    yielded = sum(
        1
        for _ in pause_resume._bulk_control_candidates(
            spec_paths, multi_user=False, policy=policy,
        )
    )
    elapsed = time.perf_counter() - start
    if yielded:
        raise RuntimeError(
            f"{yielded} terminal row(s) survived the filter on a synthetic corpus"
        )
    return elapsed


def benchmark(*, specs: int, repeats: int, hosts: int) -> dict:
    if min(specs, repeats, hosts) <= 0:
        raise ValueError("specs, repeats and hosts must be positive")
    samples: dict[str, list[float]] = {"parent": [], "current": []}
    with tempfile.TemporaryDirectory(prefix="vq-bulk-authz-benchmark-") as tmp:
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
            spec_paths = sorted(queue_dir.glob("*.json"))
            # One untimed pass, so neither variant is timed while the page
            # cache is still cold on the corpus it just wrote.
            _scan(spec_paths, resolve_once=False)
            for repeat in range(repeats):
                order = (
                    [("parent", False), ("current", True)]
                    if repeat % 2 == 0
                    else [("current", True), ("parent", False)]
                )
                for variant, resolve_once in order:
                    samples[variant].append(
                        _scan(spec_paths, resolve_once=resolve_once)
                    )
    medians = {name: statistics.median(values) for name, values in samples.items()}
    return {
        "schema": "vq.bulk-control-authorization-benchmark/1",
        "scope": (
            "synthetic single-user bulk-control authorization filter over "
            "retained terminal specs; no daemon tick, dispatch or scheduler poll"
        ),
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "specs": specs,
        "hosts": hosts,
        "repeats": repeats,
        "seconds": samples,
        "median_seconds": medians,
        "speedup": medians["parent"] / medians["current"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specs", type=_positive_int, default=16000)
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
