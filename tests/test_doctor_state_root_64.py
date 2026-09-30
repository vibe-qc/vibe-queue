"""`vq doctor` catches a remote_vq that reads another state root than the daemon (#64 B1).

A host reached through a bare ``vq`` while its daemon runs from a wrapper that
exports ``VQ_STATE_DIR`` resolves the default root instead: status, overview
and the console then read an empty second store (one entry where the live
store had thousands). A broker placing jobs from that view would place blind.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from vq import config, daemon_control, doctor, paths, transport


@pytest.mark.parametrize("env, root", [
    ({"VQ_STATE_DIR": "/site/vq-state", "HOME": "/home/USER"}, "/site/vq-state"),
    ({"VQ_STATE_DIR": "~/state", "HOME": "/home/USER"}, "/home/USER/state"),
    ({"XDG_DATA_HOME": "/data", "HOME": "/home/USER"}, "/data/vq"),
    ({"HOME": "/home/USER"}, "/home/USER/.local/share/vq"),
    ({}, None),
])
def test_a_process_environment_resolves_like_paths_state_root(env, root) -> None:
    assert daemon_control._state_root_for_environment(env) == root


def _envelope(client: str, *daemon_roots: str | None) -> dict:
    return {"ok": False, "error": "socket missing", "state_roots": {
        "client": client,
        "daemons": [{"pid": 100 + i, "state_root": r} for i, r in enumerate(daemon_roots)]}}


def test_a_daemon_on_another_root_fails_and_names_both() -> None:
    [result] = doctor.state_root_checks(_envelope("/home/USER/.local/share/vq", "/site/vq-state"))

    assert result["name"] == "state_root_agreement" and result["ok"] is False
    assert "/home/USER/.local/share/vq" in result["message"]
    assert "/site/vq-state" in result["message"]
    assert "remote_vq" in result["message"]


@pytest.mark.parametrize("roots", [
    ("/site/vq-state",),                    # the daemon uses this root
    ("/site/vq-state", "/tmp/test-root"),   # plus another daemon elsewhere
    (),                                     # no daemon running
    (None,),                                # a daemon whose root is unreadable
])
def test_agreement_or_no_evidence_passes(roots) -> None:
    [result] = doctor.state_root_checks(_envelope("/site/vq-state", *roots))

    assert result["ok"] is True


@pytest.mark.parametrize("envelope", [
    {"ok": True},                                            # older remote vq
    {"ok": True, "state_roots": None},                       # multi-user host
    {"ok": True, "state_roots": {"client": "/x", "daemons": None}},  # scan impossible
])
def test_nothing_is_added_without_evidence(envelope) -> None:
    assert doctor.state_root_checks(envelope) == []


def test_the_remote_doctor_reports_the_mismatch(monkeypatch) -> None:
    def remote(_host_cfg, *args, **kwargs):
        assert "--verbose" in args
        return subprocess.CompletedProcess(
            args, 1, stdout=json.dumps(_envelope("/home/USER/.local/share/vq", "/site/vq-state")),
            stderr="")

    monkeypatch.setattr(transport, "run_remote_vq", remote)

    checks, transport_failed = doctor.ping_remote(
        config.HostConfig(ssh="remote-host"), host_label="remote", timeout=2.0)

    assert transport_failed is False
    assert [c["name"] for c in checks] == ["remote_vq", "daemon_rpc", "state_root_agreement"]
    assert checks[-1]["ok"] is False


def test_the_scan_finds_a_running_daemon_and_its_root(tmp_path: Path) -> None:
    root = tmp_path / "wrapper-root"
    env = {**os.environ, paths.ENV_STATE_DIR: str(root)}
    proc = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)", "vq", "daemon", "run"], env=env)
    try:
        found = daemon_control.running_daemon_state_roots()
    finally:
        proc.kill()
        proc.wait(timeout=10)

    assert found is not None
    assert {"pid": proc.pid, "state_root": str(root)} in found


def test_the_verbose_ping_carries_the_evidence(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setattr(daemon_control, "running_daemon_state_roots",
                        lambda: [{"pid": 1, "state_root": "/site/vq-state"}])

    _, single = daemon_control.local_daemon_ping(0.2, multi_user=False, verbose=True)
    _, multi = daemon_control.local_daemon_ping(0.2, multi_user=True, verbose=True)

    assert single["state_roots"] == {"client": str(tmp_path / "state"),
                                     "daemons": [{"pid": 1, "state_root": "/site/vq-state"}]}
    assert multi["state_roots"] is None
