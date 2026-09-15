"""v0.16.0: [daemon] config section fills `vq daemon run` caps.

The config file — not hand-edited ExecStart lines — is the durable home
for per-host capacity caps. These tests pin the precedence contract:
an explicit CLI flag always wins; the [daemon] value fills in only when
the flag is absent; no section means no caps (legacy behaviour).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import cli, config
from vq.cli import main


@pytest.fixture
def captured_daemon(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub out Daemon + daemon plumbing; capture the ctor kwargs."""
    captured: dict[str, Any] = {}

    class FakeDaemon:
        def __init__(self, **kwargs: Any) -> None:
            captured.update(kwargs)

        def run(self) -> None:
            return None

    monkeypatch.setattr(cli, "Daemon", FakeDaemon)
    monkeypatch.setattr(cli, "setup_daemon_logging", lambda *_a, **_k: None)
    monkeypatch.setattr(cli, "write_pidfile", lambda **_k: None)
    monkeypatch.setattr(cli, "remove_pidfile", lambda **_k: None)
    monkeypatch.setattr(cli.os, "geteuid", lambda: 1000)
    return captured


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    return d


def test_config_fills_unset_caps(
    cfg_dir: Path, captured_daemon: dict[str, Any]
) -> None:
    (cfg_dir / "config.toml").write_text(
        "[daemon]\n"
        "max_cpus = 8\n"
        "max_jobs = 2\n"
        "max_mem_mb = 51218\n"
        "default_job_mem_mb = 4000\n"
    )
    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 0, result.output
    assert captured_daemon["max_cpus"] == 8
    assert captured_daemon["max_jobs"] == 2
    assert captured_daemon["max_mem_mb"] == 51218
    assert captured_daemon["default_job_mem_mb"] == 4000
    assert captured_daemon["max_scheduler_jobs"] is None


def test_cli_flag_beats_config(
    cfg_dir: Path, captured_daemon: dict[str, Any]
) -> None:
    (cfg_dir / "config.toml").write_text(
        "[daemon]\nmax_cpus = 8\nmax_mem_mb = 51218\n"
    )
    result = CliRunner().invoke(
        main, ["daemon", "run", "--max-cpus", "4", "--max-mem-mb", "20000"]
    )

    assert result.exit_code == 0, result.output
    assert captured_daemon["max_cpus"] == 4
    assert captured_daemon["max_mem_mb"] == 20000


def test_no_section_means_no_caps(
    cfg_dir: Path, captured_daemon: dict[str, Any]
) -> None:
    (cfg_dir / "config.toml").write_text('default_host = "x"\n')
    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 0, result.output
    assert captured_daemon["max_cpus"] is None
    assert captured_daemon["max_jobs"] is None
    assert captured_daemon["max_mem_mb"] is None
    assert captured_daemon["default_job_mem_mb"] is None


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("nan", "finite number greater than zero"),
        ("inf", "finite number greater than zero"),
        ("-inf", "not in the range"),
    ],
)
def test_poll_interval_rejects_nonfinite_before_command_work(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
    message: str,
) -> None:
    effects: list[str] = []

    def unexpected_config_load() -> None:
        effects.append("config")
        raise AssertionError("config loaded before poll-interval validation")

    monkeypatch.setattr(cli.config, "load_config", unexpected_config_load)

    result = CliRunner().invoke(
        main,
        ["daemon", "run", "--poll-interval", value],
    )

    assert result.exit_code == 2
    assert "Invalid value for '--poll-interval'" in result.output
    assert message in result.output
    assert effects == []
    assert result.exception is not None
    assert not isinstance(result.exception, AssertionError)


def test_poll_interval_minimum_reaches_daemon_unchanged(
    cfg_dir: Path,
    captured_daemon: dict[str, Any],
) -> None:
    (cfg_dir / "config.toml").write_text('default_host = "x"\n')

    result = CliRunner().invoke(
        main,
        ["daemon", "run", "--poll-interval", "0.01"],
    )

    assert result.exit_code == 0, result.output
    assert captured_daemon["poll_interval"] == 0.01


@pytest.mark.parametrize(
    "body",
    [
        "[multi_user]\nenabled = false\n",
        'default_host = "localhost"\n',
    ],
)
def test_root_daemon_refuses_single_user_config_before_side_effects(
    cfg_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    body: str,
) -> None:
    (cfg_dir / "config.toml").write_text(body)
    effects: list[str] = []
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        cli, "setup_daemon_logging", lambda *_a, **_k: effects.append("log")
    )
    monkeypatch.setattr(
        cli, "setup_cli_logging", lambda *_a, **_k: effects.append("client-log")
    )
    monkeypatch.setattr(
        cli, "write_pidfile", lambda **_k: effects.append("pidfile")
    )
    monkeypatch.setattr(
        cli, "_start_web_sidecar", lambda **_k: effects.append("web")
    )

    class ForbiddenDaemon:
        def __init__(self, **_kwargs: Any) -> None:
            effects.append("daemon")

    monkeypatch.setattr(cli, "Daemon", ForbiddenDaemon)

    result = CliRunner().invoke(main, ["daemon", "run", "--web"])

    assert result.exit_code == 1
    assert "refusing to run the daemon as root" in result.output
    assert "enabled = true" in result.output
    assert "no run-as-root single-user fallback" in result.output
    assert effects == []


def test_root_daemon_refuses_malformed_config_before_side_effects(
    cfg_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (cfg_dir / "config.toml").write_text("this is [not valid toml")
    effects: list[str] = []
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        cli, "setup_daemon_logging", lambda *_a, **_k: effects.append("log")
    )
    monkeypatch.setattr(
        cli, "setup_cli_logging", lambda *_a, **_k: effects.append("client-log")
    )
    monkeypatch.setattr(
        cli, "write_pidfile", lambda **_k: effects.append("pidfile")
    )

    class ForbiddenDaemon:
        def __init__(self, **_kwargs: Any) -> None:
            effects.append("daemon")

    monkeypatch.setattr(cli, "Daemon", ForbiddenDaemon)

    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 1
    assert "configuration is invalid" in result.output
    assert "[multi_user] enabled = true" in result.output
    assert "no single-user fallback" in result.output
    assert effects == []


def test_root_daemon_accepts_explicit_multi_user_config(
    cfg_dir: Path,
    captured_daemon: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (cfg_dir / "config.toml").write_text(
        "[multi_user]\nenabled = true\n"
    )
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)

    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 0, result.output
    assert captured_daemon["multi_user"] is True


@pytest.mark.parametrize("enabled", ['"yes"', "1"])
def test_root_daemon_rejects_non_boolean_multi_user_enablement(
    cfg_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    enabled: str,
) -> None:
    (cfg_dir / "config.toml").write_text(
        f"[multi_user]\nenabled = {enabled}\n"
    )
    effects: list[str] = []
    monkeypatch.setattr(cli.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        cli, "setup_cli_logging", lambda *_a, **_k: effects.append("client-log")
    )
    monkeypatch.setattr(
        cli, "setup_daemon_logging", lambda *_a, **_k: effects.append("log")
    )
    monkeypatch.setattr(
        cli, "write_pidfile", lambda **_k: effects.append("pidfile")
    )

    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 1
    assert "configuration is invalid" in result.output
    assert effects == []


def test_nonroot_daemon_keeps_malformed_config_compatibility(
    cfg_dir: Path,
    captured_daemon: dict[str, Any],
) -> None:
    (cfg_dir / "config.toml").write_text("this is [not valid toml")

    result = CliRunner().invoke(main, ["daemon", "run"])

    assert result.exit_code == 0, result.output
    assert captured_daemon["multi_user"] is False
