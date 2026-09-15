"""Sanity checks for macOS launchd templates."""
from __future__ import annotations

import plistlib
from pathlib import Path

from click.testing import CliRunner

from vq.cli import main

CONTRIB = Path(__file__).resolve().parent.parent / "contrib"
PLIST = CONTRIB / "com.vq.daemon.plist"


def _load_plist() -> dict[str, object]:
    with PLIST.open("rb") as f:
        return plistlib.load(f)


def test_launchd_daemon_plist_exists_and_parses() -> None:
    assert PLIST.is_file(), f"missing launchd template: {PLIST}"
    assert _load_plist()["Label"] == "com.vq.daemon"


def test_launchd_daemon_runs_daemon_with_web_sidecar() -> None:
    data = _load_plist()
    argv = data["ProgramArguments"]

    assert isinstance(argv, list)
    assert argv[1:5] == ["-m", "vq", "daemon", "run"]
    assert "--web" in argv
    assert "--web-host" in argv
    assert argv[argv.index("--web-host") + 1] == "127.0.0.1"
    assert "--web-port" in argv
    assert argv[argv.index("--web-port") + 1] == "8768"


def test_launchd_daemon_restarts_after_failure() -> None:
    data = _load_plist()

    assert data["RunAtLoad"] is True
    assert data["KeepAlive"] == {"SuccessfulExit": False}


def test_launchd_daemon_uses_placeholders_not_private_paths() -> None:
    body = PLIST.read_text()

    assert "/Users/" not in body
    assert "/home/" not in body
    assert "/absolute/path/to/" in body


def test_daemon_launchd_plist_command_renders_host_specific_plist(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        main,
        [
            "daemon",
            "launchd-plist",
            "--python",
            "/opt/vq/venv/bin/python",
            "--working-directory",
            str(tmp_path),
            "--max-cpus",
            "18",
            "--max-jobs",
            "4",
            "--max-mem-mb",
            "104858",
            "--web-port",
            "8768",
        ],
    )

    assert result.exit_code == 0, result.output
    data = plistlib.loads(result.output.encode())
    argv = data["ProgramArguments"]
    assert argv[:5] == ["/opt/vq/venv/bin/python", "-m", "vq", "daemon", "run"]
    assert "--web" in argv
    assert argv[argv.index("--web-port") + 1] == "8768"
    assert data["WorkingDirectory"] == str(tmp_path)
    assert data["RunAtLoad"] is True
    assert data["KeepAlive"] == {"SuccessfulExit": False}


def test_daemon_launchd_plist_command_rejects_relative_python(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        main,
        [
            "daemon",
            "launchd-plist",
            "--python",
            "relative-python",
            "--working-directory",
            str(tmp_path),
        ],
    )

    assert result.exit_code != 0
    assert "--python must be an absolute path" in result.output
