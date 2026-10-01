"""An exception displayed in a fan-out must also fail the command."""

from __future__ import annotations

import json
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from vq import admin, cli, config


@pytest.fixture
def fanout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg_dir = tmp_path / "config"
    cfg_dir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    (cfg_dir / "config.toml").write_text('''
[hosts.alpha]
ssh = "alpha.example.invalid"
[hosts.beta]
ssh = "beta.example.invalid"
[hosts.gamma]
ssh = "gamma.example.invalid"
[programs.example]
kind = "venv"
python = "/opt/example/bin/python"
git_dir = "/opt/example/source"
''')
    return monkeypatch


@pytest.mark.parametrize("route", ["remote", "local-env", "local-all"])
@pytest.mark.parametrize("serial", [False, True])
@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("unexpected", [False, True])
def test_host_exception_fails_once_and_other_hosts_complete(
    fanout, route: str, serial: bool, as_json: bool, unexpected: bool,
) -> None:
    calls = []
    fanout.setattr(cli, "is_local_host", lambda host: host == "beta" and route != "remote")

    def outcome(host, *, remote):
        calls.append(host)
        if host == "beta":
            error = ValueError if unexpected else (
                click.ClickException if remote else admin.AdminError
            )
            raise error("fixture update failed")
        return json.dumps({"host": host, "success": True}) if as_json else "updated " + host

    def forward(host, *args, **kwargs):
        return outcome(host, remote=True)

    def local(*args, host, **kwargs):
        return outcome(host, remote=False)

    fanout.setattr(cli, "_forward_venv_admin_update", forward)
    fanout.setattr(admin, "update_env", local)
    fanout.setattr(admin, "update_all", local)
    args = ["admin", "update", "--all" if route == "local-all" else "example", "--all-hosts"]
    if serial:
        args.append("--serial")
    if as_json:
        args.append("--json")
    result = CliRunner().invoke(cli.main, args)

    assert sorted(calls) == ["alpha", "beta", "gamma"]
    assert result.exit_code == 1, result.output
    assert "admin update --all-hosts: 1 host(s) failed (beta)" in result.stderr
    if as_json:
        assert json.loads(result.stdout) == {
            "alpha": {"host": "alpha", "success": True},
            "beta": {"error": "fixture update failed"},
            "gamma": {"host": "gamma", "success": True},
        }
    else:
        assert "updated alpha" in result.stdout
        assert "updated gamma" in result.stdout
        assert "fixture update failed" in result.stdout


@pytest.mark.parametrize("as_json", [False, True])
def test_successful_fanout_still_succeeds(fanout, as_json: bool) -> None:
    fanout.setattr(cli, "is_local_host", lambda host: False)
    fanout.setattr(cli, "_forward_venv_admin_update", lambda *a, **kw: '{"success": true}')
    args = ["admin", "update", "example", "--all-hosts"]
    if as_json:
        args.append("--json")
    result = CliRunner().invoke(cli.main, args)
    assert result.exit_code == 0, result.output
    assert "host(s) failed" not in result.stderr


def test_per_host_usage_error_is_also_a_failed_update(fanout) -> None:
    fanout.setattr(cli, "is_local_host", lambda host: False)

    def forward(host, *args, **kwargs):
        if host == "beta":
            raise click.UsageError("fixture credential configuration error")
        return '{"success": true}'

    fanout.setattr(cli, "_forward_venv_admin_update", forward)
    result = CliRunner().invoke(
        cli.main, ["admin", "update", "example", "--all-hosts", "--json"],
    )
    assert result.exit_code == 1, result.output
    assert json.loads(result.stdout)["beta"] == {"error": "fixture credential configuration error"}
    assert "1 host(s) failed (beta)" in result.stderr
