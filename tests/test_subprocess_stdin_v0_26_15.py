"""Four subprocess calls added after v0.26.14 close stdin (#101, the #118 invariant).

The AST guard in ``test_stdin_isolation`` catches a call site without
``stdin=``; these tests prove each reachable one hands the child DEVNULL at
run time, so a child that did read stdin could not consume the caller's.
"""
from __future__ import annotations

import json
import subprocess
import sys

from tests.test_web_fleet import web_state  # noqa: F401
from vq import config, daemon_control, runtime_participant
from vq.web import fleet as fleet_mod


def _recorder(calls: list[dict], stdout_for):
    def run(argv, **kwargs):
        calls.append({"argv": list(argv), **kwargs})
        return subprocess.CompletedProcess(argv, 0, stdout=stdout_for(argv), stderr="")
    return run


def test_both_ps_listings_close_stdin(monkeypatch) -> None:
    command = "/opt/venv/bin/python -m vq.cli daemon run"

    def stdout_for(argv):
        if "-E" in argv:
            return f"{command} VQ_STATE_DIR=/site/vq-state\n"
        return f"  4242 {command}\n"

    calls: list[dict] = []
    monkeypatch.setattr(daemon_control.subprocess, "run", _recorder(calls, stdout_for))

    found = daemon_control._ps_daemon_environments()

    assert found == [(4242, {"VQ_STATE_DIR": "/site/vq-state"})]
    assert [c["argv"][0] for c in calls] == ["ps", "ps"]
    assert all(c.get("stdin") is subprocess.DEVNULL for c in calls)


def test_the_migration_lease_drain_closes_stdin(monkeypatch) -> None:
    calls: list[dict] = []
    monkeypatch.setattr(subprocess, "run", _recorder(calls, lambda argv: ""))

    rc, _detail = runtime_participant._control(
        ["drain", "--release", "--scheduler-host", "hpc", "--lease-owner", "o", "localhost"])

    assert rc == 0
    [call] = calls
    assert call["argv"][:4] == [sys.executable, "-m", "vq.cli", "drain"]
    assert call.get("stdin") is subprocess.DEVNULL


def test_the_local_orca_output_tail_closes_stdin(web_state, monkeypatch) -> None:  # noqa: F811
    status = {"program": "orca", "state": "running", "stdout": "",
              "command": ["bash", "run-orca.sh", "water.inp"]}
    monkeypatch.setattr("vq.status.show_status_json", lambda *a, **k: json.dumps(status))
    calls: list[dict] = []
    monkeypatch.setattr(fleet_mod.subprocess, "run",
                        _recorder(calls, lambda argv: json.dumps({"text": "tail\n"})))

    payload, error = fleet_mod.fetch_job_preview(config.load_config(), "localhost", "abc")

    assert error is None and payload["source"] == "water.out"
    [call] = calls
    assert call["argv"][3:5] == ["tail", "localhost"]
    assert call.get("stdin") is subprocess.DEVNULL
