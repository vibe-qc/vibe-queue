"""v0.6.48 (SECURITY): ``vq admin auto-update`` requires the admin
token in multi-user mode.

Pre-v0.6.48 the auto-update verb skipped the v0.6.44 admin-token
gate. The gate lived only on ``vq admin update``; ``auto-update``
called ``admin.update_env`` via the Python layer (through
``auto_update.auto_update_env``) without first running the
``cfg.multi_user.enabled and verify_admin_token(...)`` check at
the CLI layer.

On a multi-user host that meant any local-shell user could trigger
a privileged env refresh (the daemon runs as root; the pull +
update_script chain runs from a root-owned venv per the v0.6.47
systemd-timer ship). Blast radius is bounded — the verb only
fast-forwards to the newest semver tag on the configured remote —
but it is the same auth-bypass *class* as v0.6.44 on a sibling
verb. The v0.6.47 drop-box flagged it as an audit observation;
v0.6.48 closes it.

The fix mirrors the v0.6.44 + v0.6.46 surface on the venv auto-update
CLI command: --token / --token-stdin / --token-file flags
(mutually exclusive), ``resolve_token`` precedence, and the
``cfg.multi_user.enabled and verify_admin_token(...)`` reject when
missing / wrong. Single-user mode is unchanged (gate inert).

These tests pin the contract end-to-end via ``CliRunner``. The
inner decision/apply path (the v0.6.11 logic) is covered by
``test_auto_update.py`` and not re-asserted here.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, admin_detached, auth, cli, config, paths, transport
from vq.cli import main

REMOTE_SHA = "a" * 40

# Re-uses the shape of test_auto_update.py's `state_dir` /
# `_write_venv_cfg` helpers but tweaks them for the multi-user gate
# tests — we need explicit control over [multi_user] enabled and the
# VQ_WEB_TOKEN_FILE override.


def _write_cfg(
    cfgdir: Path,
    *,
    multi_user: bool,
    git_dir: Path,
) -> None:
    """Write a minimal config: one venv program + an opt-in
    [multi_user] block when requested."""
    body = (
        'default_host = "localhost"\n'
        '\n'
        '[programs.vibeqc-release]\n'
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{git_dir}"\n'
        'branch = "release"\n'
        'update_script = "scripts/update.sh"\n'
    )
    if multi_user:
        body += '\n[multi_user]\nenabled = true\n'
    (cfgdir / "config.toml").write_text(body)


def _write_scheduler_cfg(
    cfgdir: Path,
    *,
    multi_user: bool,
) -> None:
    """Write one managed scheduler runtime for the sibling auth path."""
    body = (
        'default_host = "localhost"\n'
        '\n'
        '[hosts.host_f]\n'
        'ssh = "host_f-login"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/home/USER"\n'
        'scheduler_driver = "localhost"\n'
        '\n'
        '[hosts.host_f.scheduler_runtime_deployments.vibeqc-release]\n'
        'update_command = "/site/bin/deploy-vibeqc-release"\n'
        'verify_command = "/site/bin/verify-vibeqc-release"\n'
    )
    if multi_user:
        body += '\n[multi_user]\nenabled = true\n'
    (cfgdir / "config.toml").write_text(body)


@pytest.fixture
def env_with_cfg(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Pristine state + cfg dir; caller writes config.toml shape.
    Also pins VQ_WEB_TOKEN_FILE to a nonexistent path by default so
    the host's real ~/.config/vq/web-token never bleeds into the
    test (mirrors the v0.6.44 fixture)."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    monkeypatch.setenv(
        auth.ENV_WEB_TOKEN_FILE, str(tmp_path / "no-token-here")
    )
    monkeypatch.delenv("VQ_TOKEN", raising=False)
    # Make the queue dirs the daemon helpers expect.
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    # Fake git_dir for the venv program.
    git_dir = tmp_path / "repo"
    git_dir.mkdir()
    (git_dir / ".git").mkdir()
    return tmp_path


def _ok_drift_mocks():
    """Mocks for the v0.6.11 happy path: ls-remote returns one
    semver tag, current is older → drift detected, update_env mocked
    to a success result. Composed with `with` by the caller — only
    fires when the gate lets the verb through."""
    fake_result = admin.UpdateResult(
        env="vibeqc-release",
        git_dir="/fake/git_dir",
        branch="release",
        update_script="scripts/update.sh",
        git_pull_rc=0,
        update_script_rc=0,
        expected_tag="v0.8.0",
        actual_tag="v0.8.0",
    )

    def git_probe(argv, **kwargs):
        if "rev-parse" in argv:
            return subprocess.CompletedProcess(
                args=argv, returncode=0, stdout=f"{REMOTE_SHA}\n", stderr=""
            )
        return subprocess.CompletedProcess(
            args=argv,
            returncode=0,
            stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
            stderr="",
        )

    return [
        patch(
            "vq.auto_update.subprocess.run",
            side_effect=git_probe,
        ),
        patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ),
        patch("vq.auto_update.admin.update_env", return_value=fake_result),
    ]


class TestMultiUserGateRejects:
    """The new gate fires before any work happens."""

    def test_multi_user_no_token_rejected(
        self, env_with_cfg: Path
    ) -> None:
        """No --token, no $VQ_TOKEN, no token file → rejected with
        the v0.6.48 'token required' message. The drift mocks would
        normally drive a successful apply; the gate must short-circuit
        before any of them fires."""
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=True,
            git_dir=env_with_cfg / "repo",
        )
        with patch("vq.auto_update.subprocess.run") as mock_run, patch(
            "vq.auto_update.admin.update_env"
        ) as mock_update:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "token required" in combined.lower()
        # Critical: gate ran BEFORE any drift work happened.
        mock_run.assert_not_called()
        mock_update.assert_not_called()

    def test_multi_user_wrong_token_rejected(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A token file exists but the supplied token doesn't match —
        gate rejects."""
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=True,
            git_dir=env_with_cfg / "repo",
        )
        tf = env_with_cfg / "web-token"
        tf.write_text("correct-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        monkeypatch.setenv("VQ_TOKEN", "wrong-token")

        with patch("vq.auto_update.subprocess.run") as mock_run:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "token required" in combined.lower()
        mock_run.assert_not_called()

    def test_multi_user_missing_token_file_rejected(
        self, env_with_cfg: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The v0.6.44-class scenario, but on the auto-update verb:
        token file missing entirely (e.g. before `vq web init-token`
        has run, or after a misconfigured wipe). With the operator
        supplying any token via $VQ_TOKEN, the verb must still
        reject — `verify_admin_token` returns False when no file
        exists (v0.6.44 behaviour)."""
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=True,
            git_dir=env_with_cfg / "repo",
        )
        # Token file path explicitly nonexistent.
        monkeypatch.setenv(
            auth.ENV_WEB_TOKEN_FILE,
            str(env_with_cfg / "definitely-missing"),
        )
        monkeypatch.setenv("VQ_TOKEN", "any-claimed-token")

        with patch("vq.auto_update.subprocess.run") as mock_run:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "token required" in combined.lower()
        mock_run.assert_not_called()

    def test_retired_scheduler_runtime_flag_precedes_token_gate(
        self, env_with_cfg: Path
    ) -> None:
        """The retired spelling cannot reach config or token inspection."""
        _write_scheduler_cfg(env_with_cfg / "cfg", multi_user=True)
        with patch("vq.cli.config.load_config") as load_config, patch(
            "vq.auto_update.auto_update_scheduler_runtimes"
        ) as mock_update:
            result = CliRunner().invoke(
                main,
                ["admin", "auto-update", "--scheduler-runtimes"],
            )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "rollout-latest" in combined
        assert "accepted release report" in combined
        load_config.assert_not_called()
        mock_update.assert_not_called()


class TestMultiUserGateAccepts:
    """Token resolution precedence — each input channel passes."""

    def _setup_good_token(
        self,
        tmp: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> Path:
        _write_cfg(
            tmp / "cfg",
            multi_user=True,
            git_dir=tmp / "repo",
        )
        tf = tmp / "web-token"
        tf.write_text("good-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        return tf

    def test_env_var_token_passes(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup_good_token(env_with_cfg, monkeypatch)
        monkeypatch.setenv("VQ_TOKEN", "good-token")
        m_run, m_tag, m_update = _ok_drift_mocks()
        with m_run, m_tag, m_update as mock_update:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code == 0, result.output
        assert "action:       update" in result.output
        mock_update.assert_called_once()

    def test_token_stdin_passes(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup_good_token(env_with_cfg, monkeypatch)
        m_run, m_tag, m_update = _ok_drift_mocks()
        with m_run, m_tag, m_update as mock_update:
            result = CliRunner().invoke(
                main,
                [
                    "admin",
                    "auto-update",
                    "vibeqc-release",
                    "--token-stdin",
                ],
                input="good-token\n",
            )
        assert result.exit_code == 0, result.output
        mock_update.assert_called_once()

    def test_token_file_passes(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Install the canonical token at the env-pinned path so the
        # daemon-side verify_admin_token finds it; ALSO supply a
        # separate --token-file that holds the same value.
        self._setup_good_token(env_with_cfg, monkeypatch)
        side = env_with_cfg / "side-token-file"
        side.write_text("good-token\n")
        side.chmod(0o600)
        m_run, m_tag, m_update = _ok_drift_mocks()
        with m_run, m_tag, m_update as mock_update:
            result = CliRunner().invoke(
                main,
                [
                    "admin",
                    "auto-update",
                    "vibeqc-release",
                    "--token-file",
                    str(side),
                ],
            )
        assert result.exit_code == 0, result.output
        mock_update.assert_called_once()

    def test_argv_token_passes_but_warns(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``--token TOKEN`` works but should emit the argv-exposure
        warning (audit recommendation #3 from v0.6.46)."""
        monkeypatch.delenv("VQ_SUPPRESS_TOKEN_ARGV_WARNING", raising=False)
        self._setup_good_token(env_with_cfg, monkeypatch)
        m_run, m_tag, m_update = _ok_drift_mocks()
        # Click ≤ 8.1 supports mix_stderr to split stdout/stderr;
        # Click ≥ 8.2 dropped the kwarg. The audit-nudge text shows up
        # in either stream in practice — accept either capture path.
        try:
            runner = CliRunner(mix_stderr=False)  # type: ignore[call-arg]
        except TypeError:
            runner = CliRunner()
        with m_run, m_tag, m_update:
            result = runner.invoke(
                main,
                [
                    "admin",
                    "auto-update",
                    "vibeqc-release",
                    "--token",
                    "good-token",
                ],
            )
        assert result.exit_code == 0, (result.output, result.exception)
        combined = result.output + (
            getattr(result, "stderr", "")
            if hasattr(result, "stderr") else ""
        )
        assert "shell history" in combined or "argv" in combined.lower()

    def test_retired_scheduler_runtime_flag_rejects_even_with_token(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_scheduler_cfg(env_with_cfg / "cfg", multi_user=True)
        tf = env_with_cfg / "web-token"
        tf.write_text("good-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        monkeypatch.setenv("VQ_TOKEN", "good-token")
        with patch(
            "vq.auto_update.auto_update_scheduler_runtimes",
            return_value=[],
        ) as mock_update:
            result = CliRunner().invoke(
                main,
                ["admin", "auto-update", "--scheduler-runtimes"],
            )
        assert result.exit_code != 0
        assert "rollout-latest" in result.output
        mock_update.assert_not_called()


class TestSingleUserBypass:
    """Single-user mode (no [multi_user] block) ignores the token —
    must keep working without one. v0.6.48 is strictly additive."""

    def test_single_user_no_token_passes(
        self, env_with_cfg: Path
    ) -> None:
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=False,
            git_dir=env_with_cfg / "repo",
        )
        m_run, m_tag, m_update = _ok_drift_mocks()
        with m_run, m_tag, m_update as mock_update:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code == 0, result.output
        assert "action:       update" in result.output
        mock_update.assert_called_once()


class TestMutuallyExclusiveTokenFlags:
    def test_token_plus_token_stdin_rejected(
        self,
        env_with_cfg: Path,
    ) -> None:
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=True,
            git_dir=env_with_cfg / "repo",
        )
        result = CliRunner().invoke(
            main,
            [
                "admin",
                "auto-update",
                "vibeqc-release",
                "--token",
                "x",
                "--token-stdin",
            ],
        )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "mutually exclusive" in combined.lower()

    def test_token_stdin_plus_token_file_rejected(
        self,
        env_with_cfg: Path,
    ) -> None:
        _write_cfg(
            env_with_cfg / "cfg",
            multi_user=True,
            git_dir=env_with_cfg / "repo",
        )
        side = env_with_cfg / "f"
        side.write_text("x\n")
        side.chmod(0o600)
        result = CliRunner().invoke(
            main,
            [
                "admin",
                "auto-update",
                "vibeqc-release",
                "--token-stdin",
                "--token-file",
                str(side),
            ],
        )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "mutually exclusive" in combined.lower()


class TestSSHDelegateForwardsTokenViaStdin:
    """The remote-dispatch path must forward the token to the
    target host via ``--token-stdin`` (not as a ``--token VAL`` argv
    element). Mirrors the v0.6.46 audit fix for ``admin update``:
    the local ``ssh`` argv + the remote ``sh -c`` argv must not
    carry the token verbatim."""

    def test_remote_dispatch_uses_token_stdin(
        self,
        env_with_cfg: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Multi-user cfg with a second [hosts.remote] entry so
        # `auto-update vibeqc-release remote` takes the SSH branch.
        tf = env_with_cfg / "web-token"
        tf.write_text("good-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        monkeypatch.setenv("VQ_TOKEN", "good-token")
        (env_with_cfg / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '\n'
            '[multi_user]\n'
            'enabled = true\n'
            '\n'
            '[hosts.remote]\n'
            'ssh = "remote.example.invalid"\n'
            '\n'
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{env_with_cfg / "repo"}"\n'
            'branch = "release"\n'
            'update_script = "scripts/update.sh"\n'
        )

        captured: dict[str, object] = {}

        def fake_run_remote_vq(host_cfg, *vq_args, stdin_data=None, **_kwargs):
            if tuple(vq_args[:2]) == ("admin", "observe-update"):
                return subprocess.CompletedProcess(
                    args=["ssh"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
                            "run_id": vq_args[2],
                            "state": admin_detached.STATE_COMPLETED,
                            "detail": "stub completed",
                            "target": "vibeqc-release",
                            "pid": 4242,
                            "transcript": None,
                            "transcript_offset": 0,
                            "transcript_next_offset": 0,
                            "transcript_size": 0,
                            "transcript_base64": "",
                            "outcome": admin.OUTCOME_OK,
                            "exit_code": 0,
                            "payload": "ok\n",
                            "error": None,
                        }
                    ),
                    stderr="",
                )
            captured["host"] = host_cfg.ssh
            captured["args"] = list(vq_args)
            captured["stdin_data"] = stdin_data
            return subprocess.CompletedProcess(
                args=["ssh"], returncode=0, stdout="{}", stderr=""
            )

        # Captured at the transport, because a real-write auto-update now
        # launches detached rather than going through _delegate_to_remote.
        # The property under test is unchanged: the launch carries the token
        # on stdin, never on argv.
        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
        monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(cli, "_DETACHED_POLL_INTERVAL_SECONDS", 0.0)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "vibeqc-release", "remote"]
        )
        assert result.exit_code == 0, result.output
        assert result.stdout == "ok\n"
        assert "--detach" in captured["args"]
        # The remote got the token via stdin, not on argv.
        assert captured["stdin_data"] == "good-token\n"
        assert "--token-stdin" in captured["args"]
        assert "--token" not in captured["args"]
        # And it included the leaf args we care about.
        assert captured["args"][:3] == ["admin", "auto-update", "vibeqc-release"]
