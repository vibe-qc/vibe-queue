"""Measure what the fleet console's refresh costs on a synthetic fleet (#168).

Run from this checkout:
    PYTHONPATH=src .venv/bin/python scripts/measure_fleet_console.py \
        --hosts 8 --jobs 250 --latency-ms 80 --sweeps 5

The console's own code runs unchanged: ``gather_fleet_snapshot`` sweeps every
configured host, and ``/fleet/jobs`` renders from the cached snapshot. Only the
SSH transport is replaced. Each remote ``vq`` call is answered with the JSON
the real command prints -- the queue listing, the overview, ``vq top`` -- after
a simulated round trip of ``--latency-ms``. Every host is a daemon host with
``--jobs`` jobs: a ``--running`` fraction running directly (so the sweep must
read their samples), a tenth queued, the rest finished.

It reports, as JSON:

* the remote calls one sweep makes, by command. They depend on the number of
  hosts, never on the number of jobs: one queue listing and one overview per
  host, and one ``vq top`` per host that runs a direct job;
* how long each sweep took, over ``--sweeps`` sweeps;
* how long filtered and paginated ``/fleet/jobs`` table requests take and how
  large they are, and that rendering made no remote call.

Only a temporary state directory is written: no daemon, no host, no network.
The numbers describe this workload on this machine and nothing else; a real
fleet's SSH round trips, overview gathers and host load differ, which is why
the workload and the machine are part of the report.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

#: Table requests a reader makes: the default view, a state filter, a
#: free-text search on a later page, and the largest page.
PAGE_REQUESTS = (
    "/fleet/jobs/_table",
    "/fleet/jobs/_table?state=running&limit=50",
    "/fleet/jobs/_table?q=scan&page=2&limit=100&sort=cpu_used",
    "/fleet/jobs/_table?limit=2000",
)


def _jobs(host_index: int, count: int, running: float, now: datetime) -> list[dict[str, Any]]:
    from vq.spec import JobSpec, JobState

    n_running = round(count * running)
    n_pending = count // 10
    jobs = []
    for n in range(count):
        state = (JobState.RUNNING if n < n_running
                 else JobState.PENDING if n < n_running + n_pending else JobState.COMPLETED)
        submitted = now - timedelta(minutes=5 + n)
        spec = JobSpec(
            id=f"{host_index:04x}{n:08x}", command=["python", "run.py"],
            cwd=f"/synthetic/jobs/{host_index}/{n}", cpus=1 + n % 8, mem_mb=1024 * (1 + n % 4),
            state=state, program="vibe-qc", job_name=f"scan-{n % 50}",
            submitted_at=submitted.isoformat(),
            started_at=(submitted + timedelta(minutes=1)).isoformat()
            if state != JobState.PENDING else None,
            finished_at=(submitted + timedelta(minutes=3)).isoformat()
            if state == JobState.COMPLETED else None,
            exit_code=0 if state == JobState.COMPLETED else None,
            pid=100000 + n if state == JobState.RUNNING else None,
        )
        jobs.append(json.loads(spec.model_dump_json()))
    return jobs


def _top_rows(jobs: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    stamp = now.isoformat()
    return [{
        "jobid": job["id"], "name": job["job_name"], "queue_handle": None,
        "cpus": job["cpus"], "cpu_percent": 95.0 * job["cpus"], "rss_mb": 800.0,
        "mem_mb": job["mem_mb"], "mem_percent": None, "active_elapsed_seconds": 240.0,
        "elapsed_seconds": 240.0, "wall_time_seconds": None, "wall_percent": None,
        "sample_stale": False, "sample_at": stamp, "rss_peak_mb": 900.0,
        "rss_peak_since": job["started_at"],
    } for job in jobs if job["state"] == "running"]


class SyntheticTransport:
    """Stands in for ``vq.transport.run_remote_vq``; counts calls by command."""

    def __init__(self, fleet: dict[str, list[dict[str, Any]]], latency: float) -> None:
        self.fleet = fleet
        self.latency = latency
        self.calls: Counter[str] = Counter()

    def __call__(self, host_cfg: Any, *args: str, **_kwargs: Any) -> Any:
        from vq import overview

        host = host_cfg.ssh.removeprefix("synthetic-")
        command = args[0] if args else ""
        self.calls[command] += 1
        time.sleep(self.latency)
        now = datetime.now(UTC)
        if command == "queue":
            stdout = json.dumps(self.fleet[host])
        elif command == "overview":
            stdout = overview.format_fleet_overview_json(
                [overview.HostOverview(host="localhost", reachable=True)])
        elif command == "top":
            stdout = json.dumps(_top_rows(self.fleet[host], now))
        else:
            raise AssertionError(f"the sweep made an unexpected remote call: vq {' '.join(args)}")
        return subprocess.CompletedProcess(["vq", *args], 0, stdout=stdout, stderr="")


def measure(*, hosts: int, jobs: int, latency_ms: float, sweeps: int,
            running: float = 0.3, repeats: int = 5) -> dict[str, Any]:
    """Sweep a synthetic fleet ``sweeps`` times and time the jobs page.

    The caller provides ``VQ_STATE_DIR`` and ``VQ_CONFIG_DIR``, and sets
    ``VQ_WEB_FLEET=1``; nothing outside those directories is written.
    """
    from fastapi.testclient import TestClient

    from vq import __version__, config, transport
    from vq.web import create_app
    from vq.web import fleet as fleet_mod

    now = datetime.now(UTC)
    names = [f"h{index:02d}" for index in range(hosts)]
    fleet = {name: _jobs(index, jobs, running, now) for index, name in enumerate(names)}
    cfg = config.Config(hosts={name: config.HostConfig(ssh=f"synthetic-{name}")
                               for name in names})
    fake = SyntheticTransport(fleet, latency_ms / 1000.0)
    real = transport.run_remote_vq
    transport.run_remote_vq = fake
    try:
        durations, per_sweep = [], []
        snapshot = None
        for _ in range(sweeps):
            fake.calls.clear()
            started = time.perf_counter()
            snapshot = fleet_mod.gather_fleet_snapshot(cfg)
            durations.append(time.perf_counter() - started)
            per_sweep.append(dict(fake.calls))
        assert snapshot is not None
        rows = sum(len(host.jobs) for host in snapshot.hosts)
        sampled = sum(1 for host in snapshot.hosts for job in host.jobs
                      if (job.get("resource_observation") or {}).get("status") == "observed")
        errors = sorted({host.jobs_error or host.usage_error or "" for host in snapshot.hosts}
                        - {""})

        app = create_app()
        app.state.fleet_cache.set_snapshot(snapshot)
        client = TestClient(app)
        fake.calls.clear()
        pages = []
        for path in PAGE_REQUESTS:
            times, size = [], 0
            for _ in range(repeats):
                started = time.perf_counter()
                response = client.get(path)
                times.append(time.perf_counter() - started)
                if response.status_code != 200:
                    raise RuntimeError(f"{path}: HTTP {response.status_code}")
                size = len(response.content)
            pages.append({"path": path, "median_seconds": round(statistics.median(times), 4),
                          "max_seconds": round(max(times), 4), "bytes": size})
        render_calls = sum(fake.calls.values())
    finally:
        transport.run_remote_vq = real

    return {
        "workload": f"synthetic-{hosts}h-{jobs}j-{latency_ms:g}ms",
        "parameters": {"hosts": hosts, "jobs_per_host": jobs, "running_fraction": running,
                       "simulated_latency_ms": latency_ms, "sweeps": sweeps,
                       "page_repeats": repeats},
        "machine": {"python": platform.python_version(), "system": platform.system(),
                    "machine": platform.machine(), "cpus": os.cpu_count(),
                    # What else the machine was doing: the numbers mean little without it.
                    "load_average_1m": (round(os.getloadavg()[0], 2)
                                        if hasattr(os, "getloadavg") else None),
                    "vq": __version__},
        "rows": rows,
        "rows_with_a_fresh_sample": sampled,
        "sweep_errors": errors,
        "remote_calls_per_sweep": per_sweep[-1],
        "remote_calls_stable": all(calls == per_sweep[0] for calls in per_sweep),
        "sweep_seconds": {"min": round(min(durations), 3),
                          "median": round(statistics.median(durations), 3),
                          "max": round(max(durations), 3)},
        "pages": pages,
        "remote_calls_while_rendering": render_calls,
        "note": "Synthetic: the SSH transport is simulated; only this workload on this "
                "machine is measured.",
    }


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--hosts", type=_positive_int, default=8)
    parser.add_argument("--jobs", type=_positive_int, default=250, help="jobs per host")
    parser.add_argument("--latency-ms", type=float, default=80.0,
                        help="simulated round trip of one remote vq call")
    parser.add_argument("--sweeps", type=_positive_int, default=5)
    parser.add_argument("--running", type=float, default=0.3,
                        help="fraction of each host's jobs running directly")
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="vq-measure-fleet-") as root:
        os.environ["VQ_STATE_DIR"] = str(Path(root) / "state")
        os.environ["VQ_CONFIG_DIR"] = str(Path(root) / "config")
        os.environ.pop("VQ_WEB_TOKEN_FILE", None)
        os.environ["VQ_WEB_FLEET"] = "1"
        report = measure(hosts=args.hosts, jobs=args.jobs, latency_ms=args.latency_ms,
                         sweeps=args.sweeps, running=args.running)
    json.dump(report, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
