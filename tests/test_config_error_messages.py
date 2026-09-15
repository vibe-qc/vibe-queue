"""A config validation error names the key and the reason, and nothing else.

The rendering is built from pydantic's structured error list -- location and
message -- rather than from the exception's default string form. These tests
pin that property on every surface that prints a ``ConfigError``: the message
itself, which the daemon also logs, and the CLI's ``Error:`` line.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main

# Stands in for a credential at the end of a URL. Mixed case and no words, so
# no ordinary message can contain a run of it by accident.
SENTINEL = "Qm7Zx2Kw9Vp4Rt6Yb3Nc8Hd5Jf"
WINDOW = 5


def _fragments_of_sentinel_in(text: str) -> list[str]:
    return [
        SENTINEL[i:i + WINDOW]
        for i in range(len(SENTINEL) - WINDOW + 1)
        if SENTINEL[i:i + WINDOW] in text
    ]


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return d


# A model-level failure: the validator that rejects default_pool is handed the
# whole file, and the sentinel sits in an unrelated field, last.
MODEL_LEVEL = (
    'default_host = "example-host"\n'
    'default_pool = "nonexistent"\n'
    "\n"
    "[hosts.example-host]\n"
    'ssh = "example-host"\n'
    "\n"
    "[notifications]\n"
    f'webhook_url = "https://hooks.example.invalid/services/T0/B0/{SENTINEL}"\n'
)

# A field-level failure whose rejected value is the sentinel itself.
FIELD_LEVEL = (
    'default_host = "example-host"\n'
    "\n"
    "[hosts.example-host]\n"
    'ssh = "example-host"\n'
    "\n"
    "[notifications]\n"
    f'webhook_url = ["https://hooks.example.invalid/services/T0/B0/{SENTINEL}"]\n'
)


@pytest.mark.parametrize(
    "body", [MODEL_LEVEL, FIELD_LEVEL], ids=["model-level", "field-level"],
)
def test_the_message_carries_no_config_value(cfg_dir: Path, body: str) -> None:
    (cfg_dir / "config.toml").write_text(body, encoding="utf-8")

    with pytest.raises(config.ConfigError) as caught:
        config.load_config()

    message = str(caught.value)
    assert _fragments_of_sentinel_in(message) == [], message
    assert "example-host" not in message
    assert "errors.pydantic.dev" not in message


@pytest.mark.parametrize(
    "body", [MODEL_LEVEL, FIELD_LEVEL], ids=["model-level", "field-level"],
)
def test_the_cli_error_line_carries_no_config_value(
    cfg_dir: Path, body: str,
) -> None:
    (cfg_dir / "config.toml").write_text(body, encoding="utf-8")

    result = CliRunner().invoke(main, ["programs"])

    assert result.exit_code == 1
    assert result.stderr.startswith("Error: invalid config in ")
    assert _fragments_of_sentinel_in(result.output) == [], result.output


def test_the_message_still_says_what_is_wrong_and_where(cfg_dir: Path) -> None:
    (cfg_dir / "config.toml").write_text(MODEL_LEVEL, encoding="utf-8")

    with pytest.raises(config.ConfigError) as caught:
        config.load_config()

    message = str(caught.value)
    assert message.startswith("invalid config in ")
    assert "default_pool 'nonexistent' is not a defined pool" in message


def test_several_problems_are_one_line_each(cfg_dir: Path) -> None:
    (cfg_dir / "config.toml").write_text(
        "[hosts.example-host]\n"
        "ssh = 1\n"
        "max_concurrent = \"many\"\n",
        encoding="utf-8",
    )

    with pytest.raises(config.ConfigError) as caught:
        config.load_config()

    lines = str(caught.value).splitlines()
    assert len(lines) >= 3, lines
    assert any(line.strip().startswith("hosts.example-host.ssh:") for line in lines)
    assert any(
        line.strip().startswith("hosts.example-host.max_concurrent:")
        for line in lines
    )


def test_web_install_refuses_an_invalid_section_without_its_values(
    tmp_path: Path,
) -> None:
    from vq.web import install

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text('default_host = "localhost"\n', encoding="utf-8")

    with pytest.raises(install.InstallError) as caught:
        install.append_web_section(f'[web]\nport = "{SENTINEL}"\n', cfg_path)

    message = str(caught.value)
    assert _fragments_of_sentinel_in(message) == [], message
    assert "port" in message
    assert cfg_path.read_text(encoding="utf-8") == 'default_host = "localhost"\n'


def test_the_provisioning_probe_reports_no_config_value(tmp_path: Path) -> None:
    """Runs the probe's own Python, pointed at a file instead of /etc."""
    import subprocess
    import sys

    from vq import provision

    probe = provision._MULTI_USER_PROBE
    script = probe.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    target = tmp_path / "config.toml"
    target.write_text(MODEL_LEVEL, encoding="utf-8")
    assert 'Path("/etc/vq/config.toml")' in script
    script = script.replace('Path("/etc/vq/config.toml")', f"Path({str(target)!r})")

    completed = subprocess.run(
        [sys.executable, "-"], input=script, capture_output=True, text=True,
        timeout=60,
    )

    assert completed.returncode == 2, completed.stderr
    assert "invalid config in" in completed.stderr
    assert "default_pool" in completed.stderr
    assert _fragments_of_sentinel_in(completed.stderr + completed.stdout) == []
