"""Measure what lock budget the web session store actually needs (#60).

Run from this checkout: PYTHONPATH=src .venv/bin/python
scripts/measure_session_lock_budget.py --budget 15 --workers 32 --rounds 6

`vq web` workers are separate processes, so the session store's writers contend
across processes and are not smoothed out by the GIL. Every session operation
opens a fresh connection through `vq.web.authn._auth_db`, and the most contended
moment the store has is several workers racing to create the same two tables on
first use. This drives exactly that: N worker processes released from a barrier
together against one empty database.

It reports the worst single call and how many workers failed with
`database is locked`, so a budget can be chosen from a measurement rather than a
guess. Pass `--budget` to try a value other than the shipped one, which is how
you show that the budget rather than the workload decides the outcome.

Only a temporary state directory is written and read: no real session store, no
daemon, no host, no network. Run it under synthetic load to model a busy runner;
the numbers mean nothing without saying what the machine was doing.
"""
from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import platform
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

from vq import __version__


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _prepare(root: Path) -> None:
    """A fresh account and an absent database, so each round is a first use."""
    os.environ["VQ_STATE_DIR"] = str(root / "state")
    os.environ["VQ_CONFIG_DIR"] = str(root / "config")
    os.environ["VQ_WEB_FLEET"] = "1"
    os.environ.pop("VQ_WEB_TOKEN_FILE", None)
    for module in [name for name in list(sys.modules) if name.startswith("vq")]:
        del sys.modules[module]
    from vq import paths
    from vq.web import authn

    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    authn.add_user("alice", "correct", "operator")
    database = Path(authn._local_account_config_dir()) / "web-auth" / "sessions.sqlite3"
    database.unlink(missing_ok=True)


def _worker(root: str, budget: float | None, sessions: int, barrier, results) -> None:
    os.environ["VQ_STATE_DIR"] = str(Path(root) / "state")
    os.environ["VQ_CONFIG_DIR"] = str(Path(root) / "config")
    os.environ["VQ_WEB_FLEET"] = "1"
    if budget is not None:
        real_connect = sqlite3.connect

        def connect(path, *args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs["timeout"] = budget
            return real_connect(path, *args, **kwargs)

        sqlite3.connect = connect
    from vq.web import authn

    barrier.wait()
    worst = 0.0
    for _ in range(sessions):
        started = time.perf_counter()
        try:
            authn.issue_session("alice", "operator")
        except Exception as exc:  # noqa: BLE001 - the measurement is the failure
            results.put(("failed", f"{type(exc).__name__}: {exc}"))
            return
        worst = max(worst, time.perf_counter() - started)
    results.put(("ok", worst))


def measure(
    *, budget: float | None, workers: int, rounds: int, sessions: int,
) -> dict:
    failures: dict[str, int] = {}
    failed = 0
    worst_call = 0.0
    context = mp.get_context("spawn")
    for _ in range(rounds):
        with tempfile.TemporaryDirectory(prefix="vq-session-budget-") as raw:
            _prepare(Path(raw))
            barrier = context.Barrier(workers)
            results = context.Queue()
            processes = [
                context.Process(
                    target=_worker,
                    args=(raw, budget, sessions, barrier, results),
                )
                for _ in range(workers)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(300)
            while not results.empty():
                kind, payload = results.get()
                if kind == "failed":
                    failed += 1
                    failures[payload] = failures.get(payload, 0) + 1
                else:
                    worst_call = max(worst_call, float(payload))
    total = rounds * workers
    try:
        load_average = os.getloadavg()
    except OSError:  # pragma: no cover - not available everywhere
        load_average = None
    return {
        "schema": "vq.session-lock-budget/1",
        "scope": (
            "synthetic concurrent first sessions against a temporary session "
            "store; no daemon, host or network"
        ),
        "vq_version": __version__,
        "python": platform.python_version(),
        "platform": platform.system(),
        "cpu_count": os.cpu_count(),
        "load_average": load_average,
        "budget_seconds": budget,
        "workers": workers,
        "rounds": rounds,
        "sessions_per_worker": sessions,
        "workers_total": total,
        "workers_failed": failed,
        "failure_rate": failed / total if total else 0.0,
        "worst_single_call_seconds": worst_call,
        "failures": failures,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--budget",
        type=_positive_float,
        default=None,
        help="Busy timeout to force, in seconds. Default: the shipped value.",
    )
    parser.add_argument("--workers", type=_positive_int, default=32)
    parser.add_argument("--rounds", type=_positive_int, default=6)
    parser.add_argument("--sessions", type=_positive_int, default=6)
    args = parser.parse_args()
    print(
        json.dumps(
            measure(
                budget=args.budget,
                workers=args.workers,
                rounds=args.rounds,
                sessions=args.sessions,
            ),
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
