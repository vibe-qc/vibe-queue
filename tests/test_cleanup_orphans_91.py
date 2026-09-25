"""Orphaned remote scheduler workspaces (#91), against real shell scripts.

The runner executes the dispatcher's remote commands in a local shell, so the
listing script, the staging stamp and the guarded removal all really run. The
scheduler poll is the only fake.
"""
from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Sequence
from pathlib import Path

import pytest

from vq import cleanup, config, driver_identity, paths
from vq.scheduler_dialect import SchedulerPhase, TorqueDialect
from vq.scheduler_dispatch import RemoteResult, SchedulerDispatcher, SchedulerPollEvidence


class LocalShellRunner:
    def __init__(self) -> None:
        self.argvs: list[list[str]] = []

    def run(self, argv: Sequence[str], *, stdin_data: str | None = None,
            check: bool = False) -> RemoteResult:
        self.argvs.append(list(argv))
        proc = subprocess.run(list(argv), input=stdin_data, capture_output=True,
                              text=True, check=False, timeout=60)
        if check and proc.returncode:
            raise RuntimeError(proc.stderr)
        return RemoteResult(proc.returncode, proc.stdout, proc.stderr)

    def upload_file(self, local: Path, remote: str) -> None:
        Path(remote).write_bytes(local.read_bytes())


@pytest.fixture
def cluster(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    scratch = tmp_path / "scratch"
    runner = LocalShellRunner()
    dispatcher = SchedulerDispatcher(TorqueDialect(), runner, scratch_root=str(scratch))
    live: set[str] = set()

    def poll(handles):
        return SchedulerPollEvidence({
            h.job_id: SchedulerPhase.RUNNING if h.job_id in live else SchedulerPhase.FINISHED
            for h in handles
        })

    monkeypatch.setattr(dispatcher, "poll_with_evidence", poll)
    root = scratch / ".vibeqc-cluster" / "jobs"
    root.mkdir(parents=True)
    return dispatcher, runner, root, live


def _workspace(root: Path, job_id: str, *, owner: str | None, days_old: float,
               scheduler_id: str | None = None) -> Path:
    ws = root / job_id
    (ws / "_vq").mkdir(parents=True)
    (ws / "out.log").write_text("x\n")
    if owner is not None:
        (ws / "_vq" / "driver-id").write_text(owner + "\n")
    if scheduler_id is not None:
        (ws / "_vq" / "scheduler-job-id").write_text(scheduler_id + "\n")
    stamp = time.time() - days_old * 86400
    os.utime(ws, (stamp, stamp))
    return ws


def _find(dispatcher, **kwargs):
    return {o.job_id: o for o in cleanup.find_orphan_workspaces(
        dispatcher, driver_id=driver_identity.driver_id(), **kwargs)}


def test_only_this_drivers_old_unknown_workspaces_are_removable(cluster) -> None:
    dispatcher, _runner, root, _live = cluster
    me = driver_identity.driver_id()
    other = "f" * 32
    _workspace(root, "mineold00001", owner=me, days_old=30)
    _workspace(root, "mineyoung001", owner=me, days_old=1)
    _workspace(root, "otherold0001", owner=other, days_old=90)
    _workspace(root, "unstamped001", owner=None, days_old=90)
    _workspace(root, "stillknown01", owner=me, days_old=90)
    (root / "not a job id!").mkdir()
    (root / "linked").symlink_to(root / "mineold00001")

    found = _find(dispatcher, local_ids={"stillknown01"})

    assert set(found) == {"mineold00001", "mineyoung001", "otherold0001", "unstamped001"}
    assert found["mineold00001"].removable
    assert not found["mineyoung001"].removable
    assert not found["otherold0001"].removable
    assert found["otherold0001"].owner == "another driver"
    assert not found["unstamped001"].removable
    assert _find(dispatcher, local_ids=set(), include_unowned=True)["unstamped001"].removable
    assert not _find(dispatcher, local_ids=set(), include_unowned=True)[
        "otherold0001"].removable


def test_a_job_the_scheduler_still_runs_is_kept(cluster) -> None:
    dispatcher, _runner, root, live = cluster
    me = driver_identity.driver_id()
    _workspace(root, "running00001", owner=me, days_old=30, scheduler_id="42.cluster")
    live.add("42.cluster")

    found = _find(dispatcher, local_ids=set())

    assert not found["running00001"].removable
    assert "still has" in found["running00001"].reason


def test_removal_deletes_exactly_the_orphan_and_its_receipt(cluster) -> None:
    dispatcher, runner, root, _live = cluster
    me = driver_identity.driver_id()
    victim = _workspace(root, "mineold00001", owner=me, days_old=30)
    receipt = root / "mineold00001.submit-once.json"
    receipt.write_text('{"scheduler_job_id":"7.cluster"}')
    keep = _workspace(root, "otherold0001", owner="e" * 32, days_old=30)

    orphan = _find(dispatcher, local_ids=set())["mineold00001"]
    assert orphan.scheduler_job_ids == ("7.cluster",)
    cleanup.remove_orphan_workspace(dispatcher, orphan)

    assert not victim.exists() and not receipt.exists()
    assert keep.is_dir()
    assert "vq-rmtree" in runner.argvs[-1]


def test_staging_stamps_this_drivers_identity(cluster, tmp_path) -> None:
    dispatcher, _runner, root, _live = cluster
    local = tmp_path / "local-ws"
    local.mkdir()
    (local / "input.dat").write_text("payload")
    dispatcher._ensure_job_root()

    dispatcher._stage_job("stamped00001", str(root / "stamped00001"),
                          str(root / "stamped00001" / "job.pbs"), "#!/bin/sh\n", local)

    stamp = (root / "stamped00001" / "_vq" / "driver-id").read_text().strip()
    assert stamp == driver_identity.driver_id()
    assert (root / "stamped00001" / "input.dat").read_text() == "payload"


def test_driver_identity_is_stable_and_opaque(cluster) -> None:
    first = driver_identity.driver_id()

    assert first == driver_identity.driver_id()
    assert driver_identity.is_driver_id(first)
    assert (paths.state_root() / "driver-id").read_text().strip() == first


def test_cli_lists_by_default_and_removes_with_execute(cluster, monkeypatch) -> None:
    from types import SimpleNamespace

    from click.testing import CliRunner

    from vq import cli

    dispatcher, _runner, root, _live = cluster
    me = driver_identity.driver_id()
    victim = _workspace(root, "mineold00001", owner=me, days_old=30)
    _workspace(root, "unstamped001", owner=None, days_old=30)
    monkeypatch.setattr(cli.config, "load_config", lambda: SimpleNamespace(
        host=lambda name: SimpleNamespace(scheduler="pbs")))
    monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: False)
    monkeypatch.setattr(cli, "scheduler_dispatcher_for", lambda host_cfg: dispatcher)

    listed = CliRunner().invoke(cli.main, ["cleanup", "cluster", "--orphans"])
    assert listed.exit_code == 0, listed.output
    assert "remove mineold00001" in listed.output
    assert "keep   unstamped001" in listed.output
    assert "dry run: 1 removable" in listed.output
    assert victim.is_dir()

    removed = CliRunner().invoke(cli.main, ["cleanup", "cluster", "--orphans", "-x"])
    assert removed.exit_code == 0, removed.output
    assert "removed mineold00001" in removed.output
    assert not victim.exists()
    assert (root / "unstamped001").is_dir()
