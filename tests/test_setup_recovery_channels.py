"""Recovery setup validates explicit operator configuration before host access."""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "contrib/setup-recovery-channels.sh"


def run_setup(tmp_path: Path, user: str | None, *args: str):
    # Never allow these tests to enter privileged provisioning, even under root.
    binary = tmp_path / "id"
    binary.write_text("#!/bin/sh\necho 10001\n")
    binary.chmod(0o755)
    env = {"PATH": f"{tmp_path}:/usr/bin:/bin", "HOME": str(tmp_path)}
    if user is not None:
        env["RECOVERY_USER"] = user
    return subprocess.run(["bash", str(SCRIPT), *args], env=env,
                          capture_output=True, text=True, check=False)


@pytest.mark.parametrize("user", [None, "", "two users", "-option", "bad\nvalue"])
def test_missing_or_invalid_account_stops_before_provisioning(tmp_path, user):
    result = run_setup(tmp_path, user)
    assert result.returncode == 2
    assert "RECOVERY_USER must name one explicit local account" in result.stderr
    assert "Must run as root" not in result.stderr
    if user:
        assert user not in result.stderr


@pytest.mark.parametrize("user", ["queue_operator", "recovery-user", "service$"])
def test_explicit_account_passes_configuration_preflight(tmp_path, user):
    result = run_setup(tmp_path, user)
    assert result.returncode == 1
    assert "Must run as root" in result.stderr
    assert "RECOVERY_USER must" not in result.stderr


def test_skip_recovery_does_not_require_an_account(tmp_path):
    result = run_setup(tmp_path, None, "--skip-recovery-sshd")
    assert result.returncode == 1
    assert "Must run as root" in result.stderr


def test_help_requires_no_site_configuration(tmp_path):
    result = run_setup(tmp_path, None, "--help")
    assert result.returncode == 0
    assert "RECOVERY_USER" in result.stdout
