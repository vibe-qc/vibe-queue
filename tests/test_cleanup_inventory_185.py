"""Bounded orphan inventory exercises the actual remote scripts and transport."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.test_cleanup_orphans_91 import (
    _archive,
    _find,
    _workspace,
    cluster,  # noqa: F401 -- shared isolated scratch fixture
)
from vq import cleanup, driver_identity, scheduler_dispatch, transport
from vq.config import HostConfig
from vq.scheduler_dispatch import (
    SchedulerError,
    SchedulerHandle,
    SchedulerRemoteOutcomeUnknown,
    SshRemoteRunner,
)


def test_large_inventory_uses_one_traversal_and_preserves_protection(
    request, monkeypatch, tmp_path: Path,
) -> None:
    dispatcher, runner, root, live = request.getfixturevalue("cluster")
    me = driver_identity.driver_id()
    for number in range(1320):
        _workspace(root, f"old{number:09d}", owner=me, days_old=30)
    _workspace(root, "running", owner=me, days_old=30, scheduler_id="42.cluster")
    live.add("42.cluster")
    _workspace(root, "foreign", owner="f" * 32, days_old=30)
    _workspace(root, "unknown", owner=None, days_old=30)
    _workspace(root, "local", owner=me, days_old=30)
    _archive(root, "local.upload.tar", 30)
    _archive(root, "gone.upload.tar", 30)

    # Any per-directory subprocess would hit these sentinels. Python remains
    # on PATH, so this proves the fast traversal rather than a larger timeout.
    sentinel = tmp_path / "external-command-used"
    tools = tmp_path / "tools"
    tools.mkdir()
    for name in ("stat", "head", "sed", "tr"):
        tool = tools / name
        tool.write_text('#!/bin/sh\n: > "$VQ_TEST_SENTINEL"\nexit 99\n')
        tool.chmod(0o755)
    monkeypatch.setenv("VQ_TEST_SENTINEL", str(sentinel))
    monkeypatch.setenv("PATH", str(tools) + os.pathsep + os.environ["PATH"])

    found = _find(dispatcher, local_ids={"local"})
    assert len(found) == 1323
    assert sum(item.removable for item in found.values()) == 1320
    assert not any(found[name].removable for name in ("running", "foreign", "unknown"))
    archives = cleanup.find_stray_archives(dispatcher, live_ids={"local"})
    assert {Path(item.path).name: item.removable for item in archives} == {
        "local.upload.tar": False, "gone.upload.tar": True,
    }
    assert not sentinel.exists()
    assert len(runner.argvs) == 2
    assert len(list(root.iterdir())) == 1326  # inventory changed nothing


def test_shell_fallback_without_python_preserves_existing_classification(
    request, monkeypatch, tmp_path: Path,
) -> None:
    dispatcher, _runner, root, _live = request.getfixturevalue("cluster")
    _workspace(root, "mine", owner=driver_identity.driver_id(), days_old=30)
    _workspace(root, "unknown", owner=None, days_old=30)
    _archive(root, "gone.upload.tar", 30)
    tools = tmp_path / "no-python-tools"
    tools.mkdir()
    for name in ("sh", "stat", "head", "tr", "sed"):
        source = shutil.which(name)
        assert source
        (tools / name).symlink_to(source)
    monkeypatch.setenv("PATH", str(tools))
    found = _find(dispatcher, local_ids=set())
    assert found["mine"].removable
    assert not found["unknown"].removable
    assert len(cleanup.find_stray_archives(dispatcher, live_ids=set())) == 1


@pytest.mark.parametrize("kind", ["workspaces", "archives"])
@pytest.mark.parametrize("failure", ["timeout", "output"])
def test_inventory_failure_reports_operation_limit_and_never_retries(
    monkeypatch, kind: str, failure: str,
) -> None:
    calls = []

    def remote(host, *argv, **kwargs):
        calls.append((argv, kwargs))
        try:
            if failure == "timeout":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            raise transport.SubprocessOutputLimitExceeded("test limit")
        except (subprocess.TimeoutExpired, transport.SubprocessOutputLimitExceeded) as exc:
            raise transport.RemoteOutcomeUnknown("untrusted command details") from exc

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    dispatcher = scheduler_dispatch.SchedulerDispatcher(
        scheduler_dispatch.dialect_for("torque"), SshRemoteRunner(HostConfig(ssh="cluster")),
        scratch_root="/scratch",
    )
    operation = (dispatcher.list_job_workspaces if kind == "workspaces"
                 else dispatcher.list_transfer_archives)
    with pytest.raises(SchedulerRemoteOutcomeUnknown) as caught:
        operation()
    assert len(calls) == 1
    contract = calls[0][1]
    assert contract["timeout"] == 180
    assert contract["retry_transient"] == 0
    assert contract["owned_process_group"]
    assert contract["max_stdout_bytes"] == 8 * 1024 * 1024
    assert contract["max_stderr_bytes"] == 4096
    message = str(caught.value)
    assert ("workspace" if kind == "workspaces" else "transfer archive") in message
    assert ("timeout after 180s" if failure == "timeout" else "capture limit") in message
    assert "no cleanup performed" in message
    assert "untrusted command details" not in message


def test_cleanup_mutations_keep_general_budget_without_replay(monkeypatch) -> None:
    calls = []

    def remote(host, *argv, **kwargs):
        calls.append(kwargs)
        raise transport.RemoteOutcomeUnknown("lost outcome")

    monkeypatch.setattr(transport, "run_remote_shell", remote)
    dispatcher = scheduler_dispatch.SchedulerDispatcher(
        scheduler_dispatch.dialect_for("torque"), SshRemoteRunner(HostConfig(ssh="cluster")),
        scratch_root="/scratch",
    )
    root = "/scratch/.vibeqc-cluster/jobs"
    with pytest.raises(SchedulerRemoteOutcomeUnknown):
        dispatcher.remove_transfer_archive(root + "/old.upload.tar")
    with pytest.raises(SchedulerRemoteOutcomeUnknown):
        dispatcher.cleanup_remote_workspace(SchedulerHandle("old", root + "/old"))
    assert len(calls) == 2
    assert all(call["retry_transient"] == 0 for call in calls)
    assert all(call["timeout"] == transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS for call in calls)
    assert scheduler_dispatch._scheduler_remote_timeout(
        ["sh", "-c", "arbitrary mutation", "vq-list-workspaces", root],
    ) == transport.DEFAULT_REMOTE_SHELL_TIMEOUT_SECONDS


@pytest.mark.parametrize("failure", ["metadata", "scheduler"])
def test_incomplete_inventory_aborts_execute_before_any_removal(
    request, monkeypatch, failure: str,
) -> None:
    from types import SimpleNamespace

    from click.testing import CliRunner

    from vq import cli

    dispatcher, _runner, root, _live = request.getfixturevalue("cluster")
    victim = _workspace(root, "candidate", owner=driver_identity.driver_id(), days_old=30,
                        scheduler_id="42.cluster")
    archive = _archive(root, "candidate.upload.tar", 30)
    if failure == "metadata":
        (victim / "_vq" / "driver-id").write_text("x" * 65)
    else:
        def failed_poll(handles):
            raise SchedulerError("scheduler state unknown")
        monkeypatch.setattr(dispatcher, "poll_with_evidence", failed_poll)
    monkeypatch.setattr(cli.config, "load_config", lambda: SimpleNamespace(
        host=lambda name: SimpleNamespace(scheduler="pbs")))
    monkeypatch.setattr(cli, "_multi_user_active", lambda cfg: False)
    monkeypatch.setattr(cli, "scheduler_dispatcher_for", lambda host_cfg: dispatcher)
    result = CliRunner().invoke(cli.main, ["cleanup", "cluster", "--orphans", "-x"])
    assert result.exit_code == 2
    assert victim.is_dir() and archive.is_file()
