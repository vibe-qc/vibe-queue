"""Characterize the shared credential boundary of admin write commands."""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import auth, config, paths
from vq.cli import main

_ADMIN_WRITERS = [
    pytest.param(
        ["admin", "update", "vibeqc-dev", "localhost"],
        "admin update",
        id="update",
    ),
    pytest.param(
        ["admin", "auto-update", "vibeqc-dev", "localhost"],
        "admin auto-update",
        id="auto-update",
    ),
    pytest.param(
        [
            "admin",
            "mark-ok",
            "vibeqc-dev",
            "localhost",
            "--note",
            "operator verified",
        ],
        "admin mark-ok",
        id="mark-ok",
    ),
    pytest.param(
        ["admin", "reset-branch", "vibeqc-dev", "localhost"],
        "admin reset-branch",
        id="reset-branch",
    ),
]


@pytest.fixture
def credential_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tmp_path / "missing-token"))
    monkeypatch.delenv("VQ_TOKEN", raising=False)
    (cfg_dir / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[multi_user]\n"
        "enabled = true\n"
        "\n"
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        'git_dir = "/fake/repo"\n'
        'branch = "main"\n'
    )
    return tmp_path


@pytest.mark.parametrize("argv,command_label", _ADMIN_WRITERS)
def test_missing_token_uses_each_writer_label(
    credential_env: Path,
    argv: list[str],
    command_label: str,
) -> None:
    result = CliRunner().invoke(main, argv)

    assert result.exit_code == 1
    assert (
        f"{command_label}: token required in multi-user mode. "
        "Generate with `vq web init-token`, then pass via $VQ_TOKEN env var, "
        "--token-stdin, --token-file PATH, or (discouraged) --token TOKEN."
    ) in result.output


@pytest.mark.parametrize("argv,_command_label", _ADMIN_WRITERS)
def test_explicit_token_inputs_are_mutually_exclusive_before_resolution(
    credential_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    _command_label: str,
) -> None:
    def must_not_resolve(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        raise AssertionError("resolve_token must not run for conflicting inputs")

    monkeypatch.setattr(auth, "resolve_token", must_not_resolve)

    result = CliRunner().invoke(
        main,
        [*argv, "--token", "argv-secret", "--token-stdin"],
        input="stdin-secret\n",
    )

    assert result.exit_code == 2
    assert (
        "--token, --token-stdin, and --token-file are mutually exclusive; "
        "choose one (or fall back to $VQ_TOKEN)."
    ) in result.output


@pytest.mark.parametrize("argv,command_label", _ADMIN_WRITERS)
def test_argv_token_warns_then_resolves_then_verifies(
    credential_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    command_label: str,
) -> None:
    events: list[tuple[object, ...]] = []

    monkeypatch.setattr(
        auth,
        "warn_argv_token_exposure",
        lambda: events.append(("warn",)),
    )

    def fake_resolve_token(  # type: ignore[no-untyped-def]
        cli_token,
        *,
        token_stdin,
        token_file,
    ):
        events.append(("resolve", cli_token, token_stdin, token_file))
        return "resolved-secret"

    def fake_verify_admin_token(token: str) -> bool:
        events.append(("verify", token))
        return False

    monkeypatch.setattr(auth, "resolve_token", fake_resolve_token)
    monkeypatch.setattr(auth, "verify_admin_token", fake_verify_admin_token)

    result = CliRunner().invoke(main, [*argv, "--token", "argv-secret"])

    assert result.exit_code == 1
    assert f"{command_label}: token required in multi-user mode" in result.output
    assert events == [
        ("warn",),
        ("resolve", "argv-secret", False, None),
        ("verify", "resolved-secret"),
    ]
