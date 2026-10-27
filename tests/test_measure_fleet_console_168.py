"""#168 criterion 4: the console's refresh cost, measured on a synthetic fleet.

`scripts/measure_fleet_console.py` runs the real sweep and the real jobs page
against a fleet whose SSH transport answers with the JSON the real commands
print. These cases pin what its report shows: one queue listing and one
overview per host, one `vq top` per host running a direct job, no remote call
while the page renders, and the same calls whatever the number of jobs.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

from vq import auth, config, paths

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import measure_fleet_console as harness  # noqa: E402


@pytest.fixture(autouse=True)
def _state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")


def _measure(jobs: int, running: float = 0.3) -> dict:
    return harness.measure(hosts=3, jobs=jobs, latency_ms=0, sweeps=2, running=running,
                           repeats=1)


def test_remote_calls_follow_hosts_not_jobs() -> None:
    small, large = _measure(10), _measure(60)
    assert small["remote_calls_per_sweep"] == {"queue": 3, "overview": 3, "top": 3}
    assert large["remote_calls_per_sweep"] == small["remote_calls_per_sweep"]
    assert (small["rows"], large["rows"]) == (30, 180)
    # The samples were really read: every running direct job has one.
    assert (small["rows_with_a_fresh_sample"], large["rows_with_a_fresh_sample"]) == (9, 54)
    assert small["remote_calls_stable"] and small["sweep_errors"] == []


def test_rendering_makes_no_remote_call() -> None:
    report = _measure(20)
    assert report["remote_calls_while_rendering"] == 0
    assert [page["path"] for page in report["pages"]] == list(harness.PAGE_REQUESTS)
    assert all(page["bytes"] > 0 for page in report["pages"])


def test_a_host_running_no_direct_job_is_not_sampled() -> None:
    assert _measure(10, running=0.0)["remote_calls_per_sweep"] == {"queue": 3, "overview": 3}


def test_the_report_names_its_workload_and_machine() -> None:
    report = _measure(10)
    assert report["workload"] == "synthetic-3h-10j-0ms"
    assert {"python", "system", "machine", "cpus", "load_average_1m", "vq"} <= set(
        report["machine"])
    assert "simulated" in report["note"]


def test_an_unexpected_remote_call_is_refused() -> None:
    fake = harness.SyntheticTransport({"h00": []}, 0)
    with pytest.raises(AssertionError, match="unexpected remote call: vq status"):
        fake(config.HostConfig(ssh="synthetic-h00"), "status", "localhost")
