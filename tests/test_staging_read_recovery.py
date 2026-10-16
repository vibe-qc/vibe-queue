"""Staging observations may retry; stage creation and uploads must not replay."""
from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

import pytest

from vq import admin, config, transport

SHA = "a" * 40
TREE = "b" * 64
BANNER = "Connection timed out during banner exchange\nConnection to UNKNOWN port 65535 timed out"


@pytest.fixture
def boundary(monkeypatch):
    calls = []
    monkeypatch.setattr(transport.time, "sleep", lambda _: None)
    def install(respond):
        def run(argv, **kwargs):
            assert argv[0] == "ssh"
            command = shlex.split(argv[-1])
            fresh = "ControlPath=none" in argv
            if fresh:
                assert "ControlMaster=no" in argv
            assert not any("StrictHostKeyChecking=no" in arg or arg == "-O" for arg in argv)
            calls.append((command, fresh))
            value = respond(command, fresh)
            if isinstance(value, Exception):
                raise value
            code, stdout, stderr = value
            return subprocess.CompletedProcess(argv, code, stdout, stderr)
        monkeypatch.setattr(transport.subprocess, "run", run)
        return calls
    return install


@pytest.mark.parametrize("failure", ["banner", "timeout"])
def test_home_discovery_recovers_once_on_fresh_connection(boundary, failure):
    def respond(command, fresh):
        if fresh:
            return 0, "/home/USER\n", ""
        if failure == "timeout":
            return subprocess.TimeoutExpired(command, 60)
        return 255, "", BANNER
    calls = boundary(respond)
    host = config.HostConfig(ssh="target.invalid")
    assert admin._scheduler_update_stage_path("target", host, host) == (
        "/home/USER/.cache/vq-admin/target"
    )
    assert len(calls) == 2
    assert calls[0][0] == calls[1][0]
    assert [fresh for _, fresh in calls] == [False, True]


@pytest.mark.parametrize("kind,code,stderr", [
    ("authentication", 255, "Permission denied (publickey). " + BANNER),
    ("host-key", 255, "Host key verification failed. " + BANNER),
    ("unknown", 255, "remote program exited unexpectedly"),
    ("ordinary", 1, "command failed: " + BANNER),
    ("missing", 127, "sh: unavailable"),
])
@pytest.mark.parametrize("probe", ["home", "source-sha"])
def test_permanent_or_unexplained_failure_is_not_retried(boundary, kind, code, stderr, probe):
    calls = boundary(lambda command, fresh: (code, "", stderr))
    host = config.HostConfig(ssh="target.invalid")
    if probe == "home":
        with pytest.raises(admin.AdminError, match="resolve scheduler update host HOME"):
            admin._scheduler_update_stage_path("target", host, host)
    else:
        result = admin._probe_scheduler_helper_digest(host, probe, admin._FULL_SHA_RE, SHA, probe)
        assert result.error is not None and not result.matched
    assert len(calls) == 1


@pytest.mark.parametrize("output", ["/home/USER\n", "relative\n", ""])
def test_healthy_and_malformed_home_are_not_retried(boundary, output):
    calls = boundary(lambda command, fresh: (0, output, ""))
    host = config.HostConfig(ssh="target.invalid")
    if output.startswith("/"):
        admin._scheduler_update_stage_path("target", host, host)
    else:
        with pytest.raises(admin.AdminError):
            admin._scheduler_update_stage_path("target", host, host)
    assert len(calls) == 1


def test_persistent_home_transport_failure_remains_failure_after_two_reads(boundary):
    calls = boundary(lambda command, fresh: (255, "", BANNER))
    host = config.HostConfig(ssh="target.invalid")
    with pytest.raises(admin.AdminError, match="banner exchange"):
        admin._scheduler_update_stage_path("target", host, host)
    assert len(calls) == 2


@pytest.mark.parametrize("verb,pattern,digest", [
    ("source-sha", admin._FULL_SHA_RE, SHA),
    ("source-tree-sha256", admin._SHA256_RE, TREE),
])
def test_helper_identity_probes_recover_but_keep_mismatch(boundary, verb, pattern, digest):
    calls = boundary(lambda command, fresh: (0, digest, "") if fresh else (255, "", BANNER))
    host = config.HostConfig(ssh="target.invalid")
    probe = admin._probe_scheduler_helper_digest(host, verb, pattern, "c" * len(digest), verb)
    assert probe.value == digest
    assert not probe.matched and probe.error is None
    assert len(calls) == 2
    assert all(command[-1] == verb for command, _ in calls)


def test_helper_readiness_reuses_transport_recovery_and_requires_two_successes(boundary):
    calls = boundary(lambda command, fresh: (0, "vq 0.0.0\n", "") if fresh else (255, "", BANNER))
    host = config.HostConfig(ssh="target.invalid")
    result = admin.SchedulerHostUpdateResult(host="target", ssh="target.invalid", scheduler="slurm",
                                           mode="update", command="not-executed")
    assert admin._wait_for_scheduler_helper_ready(host, result)
    assert len(calls) == 4
    assert len(result.helper_readiness_attempts) == 2
    assert all(command[-1] == "--version" for command, _ in calls)


@pytest.fixture
def source_stages(tmp_path, monkeypatch):
    project = tmp_path / "source-fixture"
    (project / "src" / "vq").mkdir(parents=True)
    (project / ".git").write_text("synthetic identity guard fixture\n")
    (project / "src" / "vq" / "__init__.py").write_text("__version__ = '0.0.0'\n")
    monkeypatch.setattr(admin, "_scheduler_helper_project_root", lambda: project)
    monkeypatch.setattr(admin, "current_source_sha", lambda: SHA)
    def archive(root, target, sha, **kwargs):
        target.write_bytes(b"synthetic source archive")
        return "d" * 64
    monkeypatch.setattr(admin, "_scheduler_helper_archive", archive)
    def local_git(argv, **kwargs):
        if "archive" in argv:
            Path(argv[argv.index("-o") + 1]).write_bytes(b"synthetic runtime source archive")
        return subprocess.CompletedProcess(argv, 0, "", "")
    monkeypatch.setattr(admin, "_mutating_git_run", local_git)
    uploads = []
    monkeypatch.setattr(
        transport, "upload_file", lambda host, local, remote, **kw: uploads.append(remote),
    )
    host = config.HostConfig(
        ssh="target.invalid", scratch_root="/scratch", scheduler="slurm",
        scheduler_dialect="slurm", scheduler_driver="localhost", scheduler_update_stage="/stages",
    )
    cfg = config.Config(scheduler_runtime_source_repo=str(project))
    return host, cfg, uploads


def run_stage(kind, host, cfg):
    if kind == "helper":
        result = admin.SchedulerHostUpdateResult(
            host="target", ssh="target.invalid", scheduler="slurm", mode="update", command="unused",
        )
        return admin._stage_scheduler_helper_source("target", host, host, result)
    result = admin.SchedulerRuntimeUpdateResult(
        host="target", program="vibeqc-dev", mode="update", command="unused",
        command_ssh="target.invalid", verify_command="unused", verify_ssh="target.invalid",
        expected_sha=SHA,
    )
    return admin._stage_scheduler_runtime_source("target", host, "vibeqc-dev", SHA, cfg, result)


@pytest.mark.parametrize("kind,upload_count", [("helper", 4), ("runtime", 3)])
def test_archive_verification_recovers_without_repeating_staging_writes(
    boundary, source_stages, kind, upload_count,
):
    host, cfg, uploads = source_stages
    def respond(command, fresh):
        if command[0] == "mkdir":
            return 0, "", ""
        assert command[:2] == ["sh", "-c"]
        return (0, "archive: OK\n", "") if fresh else (255, "", BANNER)
    calls = boundary(respond)
    assert run_stage(kind, host, cfg)
    assert len(uploads) == upload_count
    assert sum(command[0] == "mkdir" for command, _ in calls) == 1
    reads = [(command, fresh) for command, fresh in calls if command[0] == "sh"]
    assert len(reads) == 2 and reads[0][0] == reads[1][0]
    assert [fresh for _, fresh in reads] == [False, True]


@pytest.mark.parametrize("kind,upload_count", [("helper", 4), ("runtime", 3)])
def test_unobservable_verification_does_not_reupload(boundary, source_stages, kind, upload_count):
    host, cfg, uploads = source_stages
    calls = boundary(
        lambda command, fresh: (0, "", "") if command[0] == "mkdir" else (255, "", BANNER)
    )
    with pytest.raises(admin.AdminError, match="verification"):
        run_stage(kind, host, cfg)
    assert len(uploads) == upload_count
    assert len(calls) == 3  # one mkdir plus two read attempts


@pytest.mark.parametrize("kind", ["helper", "runtime"])
def test_stage_mkdir_is_never_replayed(boundary, source_stages, kind):
    host, cfg, uploads = source_stages
    calls = boundary(lambda command, fresh: (255, "", BANNER))
    with pytest.raises(admin.AdminError, match="could not create"):
        run_stage(kind, host, cfg)
    assert len(calls) == 1 and calls[0][0][0] == "mkdir"
    assert not calls[0][1]
    assert uploads == []
