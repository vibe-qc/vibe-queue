"""Source #11: an exact source pin does not make a profile request a no-op.

The real managed planner reads a temporary venv's metadata and probes its
external interpreter. Service, transaction and transport boundaries are doubles;
no updater, daemon, job or remote command is executed.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, cli, config

PROJECT = Path(__file__).resolve().parents[1]
SHA = "a" * 40


@pytest.fixture
def current_managed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    venv = tmp_path / "managed" / ".venv"
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(venv)],
        check=True, capture_output=True,
    )
    metadata = venv / ".vq-install-metadata"
    metadata.write_text("version=1\nextras=core\neditable=1\n")
    (venv / ".vq-checkout-owner").write_text(f"version=1\nproject={PROJECT}\n")
    prog = config.VenvProgram(
        kind="venv", python=str(venv / "bin" / "python"), git_dir=str(PROJECT),
        update_script="scripts/update.sh --editable",
    )
    cfg = config.Config(
        default_host="localhost", programs={"vibeqc-queue": prog},
        hosts={"remote": config.HostConfig(
            ssh="example.invalid", remote_vq="/managed/bin/vq",
        )},
    )
    monkeypatch.setattr(config.VenvProgram, "current_git_sha", lambda *a, **k: SHA)
    monkeypatch.setattr(config.VenvProgram, "current_git_dirty", lambda *a: False)
    monkeypatch.setattr(config.VenvProgram, "availability", lambda *a: (True, "core imports"))
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: admin._SelfUpdateProbe(
        is_self_update=True, daemon_running=True, service_manager="systemd",
        manager_available=True, diagnostic="fixture: exact serving environment",
    ))
    calls = []

    def transaction(env, cfg, **kwargs):
        calls.append(kwargs)
        return admin.UpdateResult(
            env=env, git_dir=prog.git_dir, branch=prog.branch,
            update_script=prog.update_script, git_pull_rc=0,
            expected_sha=kwargs["expected_sha"], actual_sha=SHA, sha_check_rc=0,
            update_script_rc=0,
        )

    monkeypatch.setattr(admin, "_update_env_logged", transaction)
    monkeypatch.setattr(cli.config, "load_config", lambda: cfg)
    monkeypatch.setattr(cli, "_resolve_admin_token", lambda *a, **k: None)
    monkeypatch.setattr(cli, "is_local_host", lambda host: host == "localhost")
    return prog, cfg, metadata, calls


@pytest.mark.parametrize(("recorded", "declared", "requested", "wanted"), [
    ("core", ["web"], None, "web"),
    ("dev", ["web"], None, "all"),
    ("core", [], ["--recreate-venv"], "core"),
    ("web", ["web"], ["--recreate-venv", "--extras", "web"], "web"),
    ("all", ["web"], ["--recreate-venv", "--extras", "web"], "web"),
])
def test_same_sha_still_executes_requested_rebuild_or_missing_floor(
    current_managed, recorded, declared, requested, wanted,
):
    prog, cfg, metadata, calls = current_managed
    metadata.write_text(f"version=1\nextras={recorded}\neditable=1\n")
    prog.extras = declared
    result = admin.update_env(
        "vibeqc-queue", cfg, host="localhost", expected_sha=SHA,
        update_script_args=requested,
    )
    assert result.already_current is False
    assert len(calls) == 1
    plan = calls[0]
    assert plan["managed_daemon_restart"] is True
    assert plan["expected_sha"] == SHA
    assert plan["managed_request_args"] == requested
    args = plan["update_script_args"]
    assert args[args.index("--extras") + 1] == wanted
    assert args[args.index("--venv") + 1] == str(metadata.parent)
    assert args.count("--recreate-venv") == 1
    # Config owns this mode, and the canonical updater must see it only once.
    assert "--editable" not in args
    assert metadata.read_text() == f"version=1\nextras={recorded}\neditable=1\n"


@pytest.mark.parametrize("requested", [
    ["--extras", "web"],
    ["--recreate-venv", "--extras", "core"],
    ["--recreate-venv", "--extras", "missing"],
    ["--recreate-venv", "--recreate-venv"],
    ["--recreate-venv", "--venv", "/other"],
    ["--recreate-venv", "--python", "/other/python"],
    ["--recreate-venv", "--copied"],
])
def test_same_sha_invalid_or_conflicting_requests_refuse_before_transaction(
    current_managed, requested,
):
    prog, cfg, metadata, calls = current_managed
    prog.extras = ["web"]
    with pytest.raises(admin.AdminError):
        admin.update_env(
            "vibeqc-queue", cfg, host="localhost", expected_sha=SHA,
            update_script_args=requested,
        )
    assert calls == []


@pytest.mark.parametrize(("recorded", "declared"), [
    ("core", []), ("web", ["web"]), ("all", ["web"]),
])
def test_valid_same_sha_without_request_or_missing_floor_is_a_true_noop(
    current_managed, recorded, declared,
):
    prog, cfg, metadata, calls = current_managed
    metadata.write_text(f"version=1\nextras={recorded}\neditable=1\n")
    prog.extras = declared
    result = admin.update_env("vibeqc-queue", cfg, host="localhost", expected_sha=SHA)
    assert result.success and result.already_current
    assert calls == []


def test_matching_profile_still_requires_actual_runtime_health(current_managed, monkeypatch):
    prog, cfg, metadata, calls = current_managed
    metadata.write_text("version=1\nextras=web\neditable=1\n")
    prog.extras = ["web"]
    monkeypatch.setattr(
        config.VenvProgram, "availability", lambda *a: (False, "web import failed"),
    )
    result = admin.update_env("vibeqc-queue", cfg, host="localhost", expected_sha=SHA)
    assert not result.already_current
    assert len(calls) == 1


def test_generic_explicit_updater_work_is_not_swallowed(current_managed, monkeypatch):
    prog, cfg, metadata, calls = current_managed
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: admin._SelfUpdateProbe(
        is_self_update=False, daemon_running=True, service_manager="systemd",
        manager_available=True, diagnostic="fixture: distinct service venv",
    ))
    result = admin.update_env(
        "vibeqc-queue", cfg, host="localhost", expected_sha=SHA,
        update_script_args=["--recreate-venv"],
    )
    assert not result.already_current
    assert len(calls) == 1
    assert not calls[0]["managed_daemon_restart"]
    assert calls[0]["update_script_args"] == ["--recreate-venv"]


@pytest.mark.parametrize("probe_available", [False, True])
def test_required_self_update_identity_is_checked_even_at_same_sha(
    current_managed, monkeypatch, probe_available,
):
    prog, cfg, metadata, calls = current_managed
    monkeypatch.setattr(admin, "_detect_vq_self_update", lambda unused: admin._SelfUpdateProbe(
        is_self_update=False, daemon_running=False, service_manager="systemd",
        manager_available=probe_available, diagnostic="fixture: no matching service proof",
    ))
    with pytest.raises(admin.AdminError):
        admin.update_env(
            "vibeqc-queue", cfg, host="localhost", expected_sha=SHA, require_self_update=True,
        )
    assert calls == []


@pytest.mark.parametrize("invalid", ["mode", "metadata", "script", "restart", "target"])
def test_same_sha_does_not_bypass_managed_preflight(current_managed, invalid):
    prog, cfg, metadata, calls = current_managed
    kwargs = {}
    if invalid == "mode":
        prog.update_script = "scripts/update.sh --copied"
    elif invalid == "metadata":
        metadata.write_text("version=1\nextras=web\neditable=1\nextras=core\n")
    elif invalid == "script":
        prog.post_update_script = "echo unowned"
    elif invalid == "restart":
        kwargs["restart_daemon"] = False
    elif invalid == "target":
        alias = metadata.parent.parent / "alias"
        alias.symlink_to(metadata.parent, target_is_directory=True)
        prog.python = str(alias / "bin" / "python")
    with pytest.raises(admin.AdminError):
        admin.update_env("vibeqc-queue", cfg, host="localhost", expected_sha=SHA, **kwargs)
    assert calls == []


@pytest.mark.parametrize("route", ["local", "delegated", "driver-runtime"])
@pytest.mark.parametrize("requested", [
    ["--recreate-venv", "--extras", "web"], ["--extras", "web"], [],
])
def test_cli_routes_share_the_same_sha_policy(current_managed, monkeypatch, route, requested):
    prog, cfg, metadata, calls = current_managed
    prog.extras = ["web"]
    runner = CliRunner()
    forwarded = []

    def invoke_remote(args):
        forwarded.append(args)
        return runner.invoke(cli.main, args)

    def forward(host, cfg, args, **kwargs):
        assert host == "remote"
        assert kwargs["append_localhost"] is True
        result = invoke_remote([*args, "localhost"])
        if result.exit_code:
            raise admin.transport.RemoteError(result.output)
        return result.stdout

    monkeypatch.setattr(cli, "_forward_admin_command", forward)

    def forward_venv(host, cfg, args, **kwargs):
        # A delegated managed-venv update now launches detached (#37) through
        # its own forwarder, which takes no append_localhost. What this test
        # pins is the target's same-SHA policy, so run the remote side
        # in-process exactly as the attached forwarder above does.
        assert host == "remote"
        result = invoke_remote([*args, "localhost"])
        if result.exit_code:
            raise admin.transport.RemoteError(result.output)
        return result.stdout

    monkeypatch.setattr(cli, "_forward_venv_admin_update", forward_venv)
    digest = "b" * 64

    def archive(path):
        path.write_bytes(b"fixture archive; no remote execution")
        return digest

    def remote_shell(host, *args, **kwargs):
        if args[0] in {"mkdir", "rmdir", "rm"}:
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[0] == "sha256sum":
            return subprocess.CompletedProcess(args, 0, f"{digest}  {args[1]}\n", "")
        assert args[0] == "/usr/bin/env"
        assert kwargs["retry_transient"] == 0
        result = invoke_remote(list(args[args.index("admin"):]))
        if result.exit_code:
            raise admin.transport.RemoteError(result.output)
        return subprocess.CompletedProcess(args, 0, result.stdout, "")

    monkeypatch.setattr(admin, "_write_driver_recovery_archive", archive)
    monkeypatch.setattr(admin.transport, "upload_file", lambda *a, **k: None)
    monkeypatch.setattr(admin.transport, "run_remote_shell", remote_shell)
    # Authentication of staged bytes is independently covered by the durable
    # recovery suite; this fixture exercises the unchanged CLI target parser.
    monkeypatch.setattr(admin, "require_staged_driver_recovery_archive", lambda value: None)
    argv = ["admin", "update", "vibeqc-queue",
            "localhost" if route == "local" else "remote",
            "--expected-sha", SHA, "--json"]
    if route == "driver-runtime":
        argv.append("--with-driver-runtime")
    for arg in requested:
        argv.extend(["--update-script-arg", arg])
    result = runner.invoke(cli.main, argv)
    if requested == ["--extras", "web"]:
        assert result.exit_code != 0
        assert "explicit --recreate-venv" in result.output + str(result.exception)
        assert calls == []
    else:
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["already_current"] is False
        assert len(calls) == 1
    assert len(forwarded) == (route != "local")
