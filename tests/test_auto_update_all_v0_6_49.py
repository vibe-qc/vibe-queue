"""v0.6.49: `vq admin auto-update --all` / `--all-hosts` fleet sweep.

v0.6.11 shipped `vq admin auto-update ENV` (single env, single host).
v0.6.47 shipped the systemd-timer template (one timer per env per
host). The v0.6.48 ship closed the admin-token gap. The natural
follow-up: a fleet-scale entry point so an operator with N envs ×
M hosts doesn't need N×M cron entries / timer instances to drive
the verb.

`--all` iterates every ``kind="venv"`` program on one host, in
sorted-by-name order, with per-env failure isolation (one env's
ls-remote error doesn't abort the sweep). `--all-hosts` sequentially
delegates the same verb to every host in ``[hosts.*]``; per-host
failure isolated, exit code non-zero if any host failed.

Test coverage shape:

* `TestAutoUpdateAllModuleHelper` — `auto_update.auto_update_all`
  pure-function behaviour (iteration order, isolation, AdminError
  on empty registry).
* `TestAutoUpdateAllCLI` — end-to-end via CliRunner: --all happy
  path, mixed outcomes, exit-code follows worst per-env result,
  --all + ENV positional rejected.
* `TestAutoUpdateAllHostsCLI` — --all-hosts delegates per-host;
  remote SSH delegate forwards the token via --token-stdin
  (carries the v0.6.46 + v0.6.48 fix); per-host failure isolated.
* `TestMultiUserGateFiresBeforeIteration` — the v0.6.48 token gate
  runs ONCE up-front; with --all + bad token, no env is iterated
  and no drift probe runs.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, admin_detached, auth, auto_update, cli, config, paths, transport
from vq.cli import main

REMOTE_SHA = "a" * 40


@pytest.fixture(autouse=True)
def stable_local_head(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(auto_update, "_rev_parse", lambda *args: REMOTE_SHA)

# ----- shared fixtures -----


def _write_multi_venv_cfg(
    cfgdir: Path,
    *,
    git_dirs: dict[str, Path],
    multi_user: bool = False,
    hosts: dict[str, str] | None = None,
) -> None:
    """Write a config with multiple [programs.NAME] entries."""
    body = 'default_host = "localhost"\n\n'
    if hosts:
        for h, ssh in hosts.items():
            body += f'[hosts.{h}]\nssh = "{ssh}"\n\n'
    if multi_user:
        body += '[multi_user]\nenabled = true\n\n'
    for name, gd in git_dirs.items():
        body += (
            f'[programs.{name}]\n'
            f'kind = "venv"\n'
            f'python = "/fake/python"\n'
            f'git_dir = "{gd}"\n'
            f'branch = "release"\n'
            f'update_script = "scripts/update.sh"\n\n'
        )
    (cfgdir / "config.toml").write_text(body)


def _make_git_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / ".git").mkdir()
    return path


@pytest.fixture
def state_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    # By default, pin the token-file path to a nonexistent spot so the
    # host's real ~/.config/vq/web-token doesn't bleed in.
    monkeypatch.setenv(
        auth.ENV_WEB_TOKEN_FILE, str(tmp_path / "no-token-here")
    )
    monkeypatch.delenv("VQ_TOKEN", raising=False)
    return tmp_path


# ===========================================================================
# Module helper
# ===========================================================================


class TestAutoUpdateAllModuleHelper:
    def test_iterates_every_venv_program_in_sorted_order(
        self, state_dir: Path
    ) -> None:
        # Three venv envs; sorted iteration order is alphabetical.
        envs = {
            "vibeqc-release": _make_git_repo(state_dir / "rel"),
            "vibeqc-dev": _make_git_repo(state_dir / "dev"),
            "crystal-stable": _make_git_repo(state_dir / "cs"),
        }
        _write_multi_venv_cfg(state_dir / "cfg", git_dirs=envs)
        cfg = config.load_config()

        # ls-remote stub returns one newer semver tag; describe stub
        # returns an older one → every env drifts.
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ), patch(
            "vq.auto_update.admin.update_env",
            return_value=admin.UpdateResult(
                env="x", git_dir="g", branch="b",
                update_script="s", git_pull_rc=0,
                update_script_rc=0, expected_tag="v0.8.0",
                actual_tag="v0.8.0",
            ),
        ):
            outcomes = auto_update.auto_update_all(
                cfg, host="localhost", dry_run=False,
            )

        names = [o.decision.env_name for o in outcomes]
        assert names == sorted(envs.keys())

    def test_per_env_failure_does_not_abort_sweep(
        self, state_dir: Path
    ) -> None:
        envs = {
            "env-a": _make_git_repo(state_dir / "a"),
            "env-b": _make_git_repo(state_dir / "b"),
            "env-c": _make_git_repo(state_dir / "c"),
        }
        _write_multi_venv_cfg(state_dir / "cfg", git_dirs=envs)
        cfg = config.load_config()

        call_count = {"n": 0}

        def flaky_subprocess(*args, **kwargs):
            call_count["n"] += 1
            # Second env's ls-remote raises.
            if call_count["n"] == 2:
                raise subprocess.CalledProcessError(
                    returncode=128, cmd=["git"], stderr="fatal: net down",
                )
            return subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            )

        with patch(
            "vq.auto_update.subprocess.run", side_effect=flaky_subprocess,
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),  # no drift; skip
        ):
            outcomes = auto_update.auto_update_all(
                cfg, host="localhost", dry_run=True,
            )

        # All three envs were attempted.
        assert len(outcomes) == 3
        # Exactly one error outcome from env-b's ls-remote failure.
        n_error = sum(1 for o in outcomes if o.decision.action == "error")
        assert n_error == 1

    def test_empty_registry_raises(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="no .* programs"):
            auto_update.auto_update_all(
                cfg, host="localhost", dry_run=True,
            )


# ===========================================================================
# CLI: --all (single host)
# ===========================================================================


class TestAutoUpdateAllCLI:
    def _setup_two_envs(self, state_dir: Path) -> None:
        envs = {
            "env-alpha": _make_git_repo(state_dir / "alpha"),
            "env-beta": _make_git_repo(state_dir / "beta"),
        }
        _write_multi_venv_cfg(state_dir / "cfg", git_dirs=envs)

    def test_all_renders_per_env_blocks_and_summary(
        self, state_dir: Path
    ) -> None:
        self._setup_two_envs(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),  # skip everywhere
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "--all", "--dry-run"]
            )
        assert result.exit_code == 0, result.output
        assert "---- env-alpha ----" in result.output
        assert "---- env-beta ----" in result.output
        assert "2/2 envs OK" in result.output

    def test_all_exit_nonzero_when_any_env_failed(
        self, state_dir: Path
    ) -> None:
        self._setup_two_envs(state_dir)
        # First env: ls-remote OK + no drift (skip).
        # Second env: ls-remote raises → error outcome.
        call = {"n": 0}

        def flaky(*args, **kwargs):
            call["n"] += 1
            if call["n"] == 2:
                raise subprocess.CalledProcessError(
                    returncode=128, cmd=["git"], stderr="fatal",
                )
            return subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            )

        with patch(
            "vq.auto_update.subprocess.run", side_effect=flaky,
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "--all", "--dry-run"]
            )
        assert result.exit_code != 0
        # Both envs still got a per-env block — failure didn't abort.
        assert "---- env-alpha ----" in result.output
        assert "---- env-beta ----" in result.output
        assert "1/2 envs OK" in result.output

    def test_all_json_emits_array(self, state_dir: Path) -> None:
        self._setup_two_envs(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),
        ):
            result = CliRunner().invoke(
                main,
                ["admin", "auto-update", "--all", "--dry-run", "--json"],
            )
        assert result.exit_code == 0, result.output
        import json
        payload = json.loads(result.output)
        assert isinstance(payload, list)
        assert len(payload) == 2
        assert {p["decision"]["env_name"] for p in payload} == {
            "env-alpha", "env-beta",
        }

    def test_all_plus_env_positional_rejected(
        self, state_dir: Path
    ) -> None:
        self._setup_two_envs(state_dir)
        result = CliRunner().invoke(
            main,
            ["admin", "auto-update", "--all", "vibeqc-release", "localhost"],
        )
        assert result.exit_code != 0
        assert "ENV HOST" in result.output or "not " in result.output.lower()

    def test_neither_env_nor_all_rejected(
        self, state_dir: Path
    ) -> None:
        self._setup_two_envs(state_dir)
        result = CliRunner().invoke(main, ["admin", "auto-update"])
        assert result.exit_code != 0
        assert "ENV is required" in result.output or "--all" in result.output

    def test_single_env_path_unchanged(self, state_dir: Path) -> None:
        """v0.6.11 / v0.6.48 happy path: no --all, ENV positional,
        renders the v0.6.11 text shape (regression guard)."""
        self._setup_two_envs(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n", stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "env-alpha", "--dry-run"]
            )
        assert result.exit_code == 0, result.output
        assert "env:          env-alpha" in result.output
        assert "action:       skip" in result.output


# ===========================================================================
# CLI: --all-hosts (multi-host delegation)
# ===========================================================================


def _route_remote_admin(monkeypatch: pytest.MonkeyPatch, fake_delegate) -> None:  # type: ignore[no-untyped-def]
    """Answer both remote auto-update routes from one ``_delegate_to_remote`` fake.

    A real-write auto-update now launches its remote work detached, straight
    through the transport, and then polls for the run's terminal receipt; a
    ``--dry-run`` still goes through ``_delegate_to_remote``. The fake answers
    the launch under the configured host name, and the poll is handed a
    receipt carrying that answer as the run's stdout -- so each test keeps
    asserting on the one mutating call it was written about.
    """
    cfg = config.load_config()
    host_by_ssh = {h.ssh: name for name, h in cfg.hosts.items()}
    payloads: dict[str, str] = {}

    def fake_run_remote_vq(host_cfg, *vq_args, stdin_data=None, **_kwargs):  # type: ignore[no-untyped-def]
        args = list(vq_args)
        if args[:2] == ["admin", "observe-update"]:
            run_id = args[2]
            return subprocess.CompletedProcess(
                args=["ssh"],
                returncode=0,
                stdout=json.dumps(
                    {
                        "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
                        "run_id": run_id,
                        "state": admin_detached.STATE_COMPLETED,
                        "detail": "stub completed",
                        "target": None,
                        "pid": 4242,
                        "transcript": None,
                        "transcript_offset": 0,
                        "transcript_next_offset": 0,
                        "transcript_size": 0,
                        "transcript_base64": "",
                        "outcome": admin.OUTCOME_OK,
                        "exit_code": 0,
                        "payload": payloads[run_id],
                        "error": None,
                    }
                ),
                stderr="",
            )
        run_id = args[args.index("--detach-run-id") + 1]
        payloads[run_id] = fake_delegate(
            host_by_ssh[host_cfg.ssh], cfg, *args, stdin_data=stdin_data
        )
        return subprocess.CompletedProcess(
            args=["ssh"], returncode=0, stdout="{}", stderr=""
        )

    monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
    monkeypatch.setattr(cli, "_delegate_to_remote", fake_delegate)


class TestAutoUpdateAllHostsCLI:
    @pytest.fixture(autouse=True)
    def _fast_detached_polling(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Keep these fan-out tests off the detached poll loop's real clock."""
        monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(cli, "_DETACHED_POLL_INTERVAL_SECONDS", 0.0)
        monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
        monkeypatch.setattr(cli, "_DETACHED_OBSERVATION_GRACE_SECONDS", 0.0)

    def test_all_hosts_delegates_per_host_and_forwards_token_via_stdin(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """--all + --all-hosts: every env on every host. The remote
        delegate must forward the token via --token-stdin (carries the
        v0.6.46 + v0.6.48 fix) so the bearer never lands on argv on
        either side of the SSH tunnel."""
        envs = {
            "env-a": _make_git_repo(state_dir / "a"),
        }
        # Two remote hosts + the implicit localhost; the test relies
        # only on the remote-delegate path so we don't need to wire
        # the local handler.
        _write_multi_venv_cfg(
            state_dir / "cfg",
            git_dirs=envs,
            multi_user=True,
            hosts={
                "host_d": "host_d.example.invalid",
                "host_a": "host_a.example.invalid",
            },
        )
        tf = state_dir / "web-token"
        tf.write_text("good-token\n")
        tf.chmod(0o600)
        monkeypatch.setenv(auth.ENV_WEB_TOKEN_FILE, str(tf))
        monkeypatch.setenv("VQ_TOKEN", "good-token")

        captured: list[dict[str, object]] = []

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            captured.append(
                {"host": host, "args": list(args), "stdin": stdin_data}
            )
            return "ok\n"

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "--all", "--all-hosts"]
        )
        assert result.exit_code == 0, result.output
        # Both remote hosts got the verb (sorted: host_a, host_d).
        delegated_hosts = sorted(c["host"] for c in captured)
        assert delegated_hosts == ["host_a", "host_d"]
        # Each carried --all (env iteration delegated to the remote),
        # --token-stdin (not --token argv), and the token in stdin.
        for c in captured:
            assert "--all" in c["args"]
            assert "--token-stdin" in c["args"]
            assert "--token" not in c["args"]
            assert c["stdin"] == "good-token\n"

    def test_all_hosts_uses_distinct_remote_admin_token_files(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Each remote vq reads its configured host-local token file."""
        envs = {"env-a": _make_git_repo(state_dir / "a")}
        _write_multi_venv_cfg(
            state_dir / "cfg",
            git_dirs=envs,
            hosts={
                "host_d": "host_d.example.invalid",
                "host_a": "host_a.example.invalid",
            },
        )
        cfg_path = state_dir / "cfg" / "config.toml"
        cfg_text = cfg_path.read_text()
        cfg_text = cfg_text.replace(
            'ssh = "host_d.example.invalid"\n',
            'ssh = "host_d.example.invalid"\n'
            'admin_token_file = "/etc/vq/host_d-token"\n',
        )
        cfg_text = cfg_text.replace(
            'ssh = "host_a.example.invalid"\n',
            'ssh = "host_a.example.invalid"\n'
            'admin_token_file = "/etc/vq/host_a-token"\n',
        )
        cfg_path.write_text(cfg_text)

        captured: list[dict[str, object]] = []

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            captured.append(
                {"host": host, "args": list(args), "stdin": stdin_data}
            )
            return "ok\n"

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main,
            [
                "admin",
                "auto-update",
                "env-a",
                "--all-hosts",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0, result.output
        by_host = {str(call["host"]): call for call in captured}
        for host, path in (
            ("host_d", "/etc/vq/host_d-token"),
            ("host_a", "/etc/vq/host_a-token"),
        ):
            args = by_host[host]["args"]
            assert isinstance(args, list)
            assert args[args.index("--token-file") + 1] == path
            assert "--token-stdin" not in args
            assert by_host[host]["stdin"] is None

    def test_all_hosts_single_env_per_host(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ENV --all-hosts: one env, every host."""
        envs = {"env-a": _make_git_repo(state_dir / "a")}
        _write_multi_venv_cfg(
            state_dir / "cfg",
            git_dirs=envs,
            hosts={"host_d": "p.invalid"},
        )

        captured: list[dict[str, object]] = []

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            captured.append(
                {"host": host, "args": list(args), "stdin": stdin_data}
            )
            return "ok\n"

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "env-a", "--all-hosts"]
        )
        assert result.exit_code == 0, result.output
        assert len(captured) == 1
        # ENV positional carried; --all absent.
        assert "env-a" in captured[0]["args"]
        assert "--all" not in captured[0]["args"]
        # A real write rides the detached handshake, not an attached session.
        assert "--detach" in captured[0]["args"]

    def test_all_hosts_skips_daemonless_scheduler_host_json(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Auto-update fan-out skips scheduler-only hosts, just like update."""
        envs = {"env-a": _make_git_repo(state_dir / "a")}
        git_dir = envs["env-a"]
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "host_d"\n'
            '\n'
            "[hosts.host_d]\n"
            'ssh = "p.invalid"\n'
            '\n'
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "host_d"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n'
            '\n'
            "[programs.env-a]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "release"\n'
            'update_script = "scripts/update.sh"\n'
        )

        captured: list[dict[str, object]] = []

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            captured.append({"host": host, "args": list(args), "stdin": stdin_data})
            return '{"decision": {"action": "skip"}}\n'

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "env-a", "--all-hosts", "--json"]
        )

        assert result.exit_code == 0, result.output
        assert [c["host"] for c in captured] == ["host_d"]
        # stdout only: a detached delegation narrates its launch on stderr.
        payload = json.loads(result.stdout)
        assert payload["host_d"]["decision"]["action"] == "skip"
        assert payload["host_f"]["skipped"] is True
        assert payload["host_f"]["scheduler_host"] is True
        assert payload["host_f"]["next_command"] == "vq admin update host_f"

    def test_all_hosts_skips_vq_only_host_json(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A coordinator must not inherit driver-local runtime paths."""
        envs = {"env-a": _make_git_repo(state_dir / "a")}
        git_dir = envs["env-a"]
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "host_d"\n'
            '\n'
            "[hosts.host_d]\n"
            'ssh = "p.invalid"\n'
            '\n'
            "[hosts.coordinator]\n"
            'fleet_role = "vq-only"\n'
            'ssh = "coordinator.invalid"\n'
            '\n'
            "[programs.env-a]\n"
            'kind = "venv"\n'
            'python = "/driver-only/python"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "release"\n'
            'update_script = "scripts/update.sh"\n'
        )

        captured: list[str] = []

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            captured.append(host)
            return '{"decision": {"action": "skip"}}\n'

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "env-a", "--all-hosts", "--json"]
        )

        assert result.exit_code == 0, result.output
        assert captured == ["host_d"]
        # stdout only: a detached delegation narrates its launch on stderr.
        payload = json.loads(result.stdout)
        assert payload["coordinator"] == {
            "fleet_role": "vq-only",
            "reason": "host has no managed runtime lanes",
            "skipped": True,
        }

    def test_all_hosts_per_host_failure_isolated(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        envs = {"env-a": _make_git_repo(state_dir / "a")}
        _write_multi_venv_cfg(
            state_dir / "cfg",
            git_dirs=envs,
            hosts={
                "good": "g.invalid",
                "bad": "b.invalid",
            },
        )

        import click

        def fake_delegate(host, cfg, *args, stdin_data=None, **_kwargs):
            if host == "bad":
                raise click.ClickException("ssh blew up")
            return "ok on good\n"

        _route_remote_admin(monkeypatch, fake_delegate)

        result = CliRunner().invoke(
            main, ["admin", "auto-update", "--all", "--all-hosts"]
        )
        # Non-zero because one host failed.
        assert result.exit_code != 0
        # But both hosts are present in output (one failed, one OK) —
        # _aggregate_per_host caught the per-host exception.
        assert "good" in result.output
        assert "bad" in result.output
        assert "1 host(s) failed" in result.output


# ===========================================================================
# Multi-user gate fires before any iteration
# ===========================================================================


class TestMultiUserGateFiresBeforeIteration:
    def test_all_without_token_in_multi_user_rejects_before_iteration(
        self, state_dir: Path
    ) -> None:
        """v0.6.48 gate must fire ONCE up-front, before --all enters
        the per-env iteration. If it ran per-env, every env's drift
        probe would still execute (wastefully + leaking which envs
        exist to an unauthenticated caller)."""
        envs = {
            "env-a": _make_git_repo(state_dir / "a"),
            "env-b": _make_git_repo(state_dir / "b"),
        }
        _write_multi_venv_cfg(
            state_dir / "cfg", git_dirs=envs, multi_user=True,
        )
        with patch(
            "vq.auto_update.subprocess.run"
        ) as mock_run, patch(
            "vq.auto_update.admin.update_env"
        ) as mock_update:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "--all"]
            )
        assert result.exit_code != 0
        combined = (result.output or "") + (str(result.exception or ""))
        assert "token required" in combined.lower()
        # The gate ran once; no env was iterated.
        mock_run.assert_not_called()
        mock_update.assert_not_called()
