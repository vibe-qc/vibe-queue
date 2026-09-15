"""Canonical admin-status persistence for multi-user updates.

Regression for the host_d v0.15.61 rollout: ``vq admin update`` completed
successfully and wrote LAST OK=true only to the invoking user's XDG file.
The daemon-owned multi-user record stayed false, so the fleet rollout could
never become idempotent even though the runtime itself was healthy.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, auth, config, paths
from vq.cli import main


@pytest.fixture
def multi_user_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[config.Config, Path]:
    state = tmp_path / "state"
    cfg_dir = tmp_path / "cfg"
    repo = tmp_path / "repo"
    multi_user_root = tmp_path / "multi-user"
    token_file = tmp_path / "web-token"
    state.mkdir()
    cfg_dir.mkdir()
    (repo / ".git").mkdir(parents=True)
    multi_user_root.mkdir()
    token_file.write_text("canonical-secret\n", encoding="utf-8")
    token_file.chmod(0o600)
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
    monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(multi_user_root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(token_file))
    (cfg_dir / "config.toml").write_text(
        'default_host = "localhost"\n'
        "[multi_user]\n"
        "enabled = true\n"
        "[programs.vibe-view]\n"
        'kind = "venv"\n'
        'python = "/opt/vibe-view/bin/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n',
        encoding="utf-8",
    )
    return config.load_config(), token_file


def _stub_successful_update(
    monkeypatch: pytest.MonkeyPatch,
    repo: Path,
) -> None:
    class _Proof:
        summary = "proof complete"

        def require_quiescent(self) -> None:
            pass

        def require_clear(self) -> None:
            pass

    monkeypatch.setattr(
        admin,
        "_do_update_work",
        lambda env, prog, **kwargs: admin.UpdateResult(
            env=env,
            git_dir=str(repo),
            branch="main",
            update_script=None,
            git_pull_rc=0,
        ),
    )
    monkeypatch.setattr(admin, "_maybe_restart_daemon", lambda *args, **kwargs: None)
    monkeypatch.setattr(admin, "_query_git_sha", lambda path: "a" * 12)
    monkeypatch.setattr(
        admin,
        "pause_token_scope_with_proof",
        lambda *args, **kwargs: _Proof(),
    )
    monkeypatch.setattr(
        admin,
        "resume_token_scope_with_proof",
        lambda *args, **kwargs: _Proof(),
    )


def test_update_uses_authenticated_daemon_rpc_and_clears_marker(
    multi_user_env: tuple[config.Config, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _token_file = multi_user_env
    repo = Path(cfg.programs["vibe-view"].git_dir)
    _stub_successful_update(monkeypatch, repo)
    calls: list[tuple[str, dict[str, object] | None, bool]] = []

    def fake_rpc_call(
        method: str,
        args: dict[str, object] | None = None,
        *,
        multi_user: bool = False,
        **kwargs: object,
    ) -> object:
        calls.append((method, args, multi_user))
        if method == "get_admin_status":
            return {}
        assert method == "set_admin_status"
        return {"env": "vibe-view", "ok": True}

    monkeypatch.setattr("vq.rpc.call", fake_rpc_call)

    result = admin.update_env(
        "vibe-view",
        cfg,
        host="localhost",
        admin_token="canonical-secret",
    )

    assert result.success is True
    assert admin.read_admin_update_marker() is None
    set_call = next(call for call in calls if call[0] == "set_admin_status")
    assert set_call[1] is not None
    assert set_call[1]["token"] == "canonical-secret"
    assert set_call[2] is True
    # The RPC was authoritative; no per-user fallback record was written.
    assert not admin.admin_status_path().exists()


def test_rpc_failure_cannot_report_silent_success_or_write_fallback(
    multi_user_env: tuple[config.Config, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, _token_file = multi_user_env
    repo = Path(cfg.programs["vibe-view"].git_dir)
    _stub_successful_update(monkeypatch, repo)

    def unavailable_rpc(*args: object, **kwargs: object) -> object:
        raise ConnectionError("daemon unavailable")

    monkeypatch.setattr("vq.rpc.call", unavailable_rpc)

    result = admin.update_env(
        "vibe-view",
        cfg,
        host="localhost",
        admin_token="canonical-secret",
    )

    assert result.success is False
    assert any(
        "canonical admin status persistence failed" in error
        for error in result.work_errors
    )
    marker = admin.read_admin_update_marker()
    assert marker is not None
    assert marker.state == admin.ADMIN_UPDATE_STATE_FAILED
    assert not admin.admin_status_path().exists()


def test_cli_threads_token_file_credential_into_local_update(
    multi_user_env: tuple[config.Config, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg, token_file = multi_user_env
    captured: dict[str, object] = {}

    def fake_update_env(
        env: str,
        cfg_arg: config.Config,
        **kwargs: object,
    ) -> admin.UpdateResult:
        captured.update(kwargs)
        return admin.UpdateResult(
            env=env,
            git_dir=cfg_arg.programs[env].git_dir,
            branch="main",
            update_script=None,
            git_pull_rc=0,
        )

    monkeypatch.setattr("vq.cli.admin_module.update_env", fake_update_env)

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "update",
            "vibe-view",
            "localhost",
            "--token-file",
            str(token_file),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["admin_token"] == "canonical-secret"
    assert "canonical-secret" not in result.output
