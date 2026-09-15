"""Regression: `vq admin update --all <remote-host>` delegation + the
marker-present error class (2026-06-26 host_c2→host_b report).

The reported failure: driving `vq admin update --all host_b` from one host
delegated a remote command to host_b that exited 2 with a click *usage*
error —

    Usage: vq admin update [OPTIONS] [ENV_OR_HOST] [HOST_IF_ENV]
    Try 'vq admin update --help' for help.

— and left an admin-update-in-progress marker stuck on host_b, blocking
every later `vq admin update` until it was cleared.

Two findings, both covered here:

1. The ``--all <host>`` remote-argv construction is correct: the local
   side delegates ``admin update --all [flags] localhost``, and that argv
   parses cleanly on the remote (no usage error, no positional-ENV/``--all``
   mutual-exclusion rejection). These tests mock the transport and assert
   both the *shape* of the delegated argv and that feeding it back through
   the CLI parses without a ``UsageError``.

2. The exit-2 ``Usage:`` banner was NOT an argv problem. It was the
   marker-guard refusal — a perfectly-valid argv whose *runtime/state*
   condition (a prior update left a marker) was mis-rendered as a
   ``click.UsageError`` (``Usage:`` banner + exit 2), which reads as
   "you mistyped the command". The guard now raises
   :class:`vq.admin.AdminUpdateInProgress` (an ``AdminError`` subclass)
   and the CLI renders it as a plain ``click.ClickException`` (``Error: …``,
   exit 1). A live marker tells the operator to wait/check status; failed
   or stale markers keep the actionable clear-update-marker / ``--force``
   recipe. Genuine input errors (unknown env, wrong kind) stay
   ``UsageError``.

   A failed/remote delegation never writes a marker on the *delegating*
   host (the marker is owned by the host that actually runs the rebuild),
   so the local tree is left clean.
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from vq import admin, admin_detached, cli, config, paths
from vq.cli import main

# ----------------------------------------------------------------------
# Fixtures / helpers
# ----------------------------------------------------------------------


@pytest.fixture
def env_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Isolated vq state + config dirs, plus one kind=venv program
    (``vibeqc-dev``) whose git_dir is a valid-enough checkout so
    update_env/update_all get *past* program validation and reach the
    admin-update-in-progress marker guard."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    (tmp_path / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.host_b]\n"
        'ssh = "host_b"\n'
        'remote_vq = "/home/USER/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq"\n'
        "\n"
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'branch = "main"\n'
    )
    return tmp_path


def _seed_marker(envs: list[str], host: str = "host_b") -> Path:
    """Drop an admin-update-in-progress marker, as a prior interrupted
    update would. Returns its path."""
    admin.acquire_admin_update_marker(envs=envs, host=host)
    path = admin.admin_update_marker_path()
    assert path.exists()
    return path


@pytest.fixture(autouse=True)
def _fast_detached_polling(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep these argv tests off the detached poll loop's real clock."""
    monkeypatch.setattr(cli.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(cli, "_DETACHED_POLL_INTERVAL_SECONDS", 0.0)
    monkeypatch.setattr(cli, "_DETACHED_UNCONFIRMED_GRACE_SECONDS", 0.0)
    monkeypatch.setattr(cli, "_DETACHED_OBSERVATION_GRACE_SECONDS", 0.0)


def _strip_detach_handshake(argv: list[str]) -> list[str]:
    """argv minus the detach handshake, validating the run id on the way."""
    out: list[str] = []
    index = 0
    while index < len(argv):
        if argv[index] == "--detach":
            index += 1
            continue
        if argv[index] == "--detach-run-id":
            admin_detached.validate_run_id(argv[index + 1])
            index += 2
            continue
        out.append(argv[index])
        index += 1
    return out


def _capture_delegation():
    """Patch the transport so the delegated remote argv is captured
    instead of ssh-ed. Returns (patcher_cm, captured_dict).

    A delegated venv update now launches the remote work detached and follows
    it by polling, so this captures the one mutating launch and answers the
    read-only observations with a terminal receipt carrying the remote stdout.
    """
    captured: dict = {}

    def fake_run_remote_vq(host_cfg, *vq_args, **kwargs):
        if tuple(vq_args[:2]) == ("admin", "observe-update"):
            return MagicMock(
                returncode=0,
                stdout=json.dumps(
                    {
                        "schema": admin_detached.DETACHED_OBSERVATION_SCHEMA,
                        "run_id": vq_args[2],
                        "state": admin_detached.STATE_COMPLETED,
                        "detail": "stub completed",
                        "target": "vibeqc-dev",
                        "pid": 4242,
                        "transcript": None,
                        "transcript_offset": 0,
                        "transcript_next_offset": 0,
                        "transcript_size": 0,
                        "transcript_base64": "",
                        "outcome": "ok",
                        "exit_code": 0,
                        "payload": "REMOTE-OK\n",
                        "error": None,
                    }
                ),
                stderr="",
            )
        captured["argv"] = list(vq_args)
        captured["kwargs"] = kwargs
        return MagicMock(returncode=0, stdout="REMOTE-OK\n", stderr="")

    cm = patch("vq.cli.transport.run_remote_vq", side_effect=fake_run_remote_vq)
    return cm, captured


# ----------------------------------------------------------------------
# 1. The delegated `--all <host>` remote argv is correct + parseable
# ----------------------------------------------------------------------


class TestAllRemoteDelegationArgv:
    def test_all_remote_delegates_all_localhost(self, env_root: Path) -> None:
        cm, captured = _capture_delegation()
        with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), cm:
            result = CliRunner().invoke(main, ["admin", "update", "--all", "host_b"])
        assert result.exit_code == 0, result.output
        # The remote command is `vq admin update --all localhost` — --all
        # is a flag, localhost is the (valid) HOST positional. No bare ENV.
        assert _strip_detach_handshake(captured["argv"]) == [
            "admin",
            "update",
            "--all",
            "localhost",
        ]
        # A lost SSH response is ambiguous after a mutating update starts.
        # Never replay it blindly; later reconciliation owns recovery. The
        # launch carries the short activation cap, because the build it starts
        # no longer runs underneath this connection.
        assert "retry_transient" not in captured["kwargs"]
        assert captured["kwargs"].get("timeout") == (
            cli._DETACHED_LAUNCH_TIMEOUT_SECONDS
        )
        assert captured["kwargs"].get("remote_env") == {
            "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
            "VQ_BUILD_STALL_TIMEOUT": "3600.0",
        }

    def test_all_show_output_remote_delegates_parseable_argv(
        self, env_root: Path
    ) -> None:
        # The exact form from the report: `--all --show-output`.
        cm, captured = _capture_delegation()
        with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), cm:
            result = CliRunner().invoke(
                main, ["admin", "update", "--all", "--show-output", "host_b"]
            )
        assert result.exit_code == 0, result.output
        assert _strip_detach_handshake(captured["argv"]) == [
            "admin", "update", "--all", "--show-output", "localhost",
        ]
        assert captured["kwargs"].get("timeout") == (
            cli._DETACHED_LAUNCH_TIMEOUT_SECONDS
        )

    def test_remote_single_env_forwards_the_watchdog_pair(
        self, env_root: Path
    ) -> None:
        """The build's own wall/stall caps still cross with the launch.

        They are what bounds the build now that no SSH session does: the
        launch call returns as soon as the updater is alive.
        """
        cm, captured = _capture_delegation()
        with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), cm:
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev", "host_b"]
            )
        assert result.exit_code == 0, result.output
        assert _strip_detach_handshake(captured["argv"]) == [
            "admin", "update", "vibeqc-dev", "localhost",
        ]
        assert (
            captured["kwargs"].get("timeout")
            == cli._DETACHED_LAUNCH_TIMEOUT_SECONDS
        )
        assert captured["kwargs"].get("remote_env") == {
            "VQ_UPDATE_SCRIPT_TIMEOUT": "14400.0",
            "VQ_BUILD_STALL_TIMEOUT": "3600.0",
        }

    def test_remote_admin_update_timeout_env_override(
        self, env_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "4321")
        cm, captured = _capture_delegation()
        with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), cm:
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev", "host_b"]
            )
        assert result.exit_code == 2
        assert "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT" in result.output
        assert "at least 15000" in result.output
        assert captured == {}

    @pytest.mark.parametrize(
        "argv",
        [
            ["admin", "update", "--all", "localhost"],
            ["admin", "update", "--all", "--show-output", "localhost"],
            ["admin", "update", "--all", "--json", "localhost"],
        ],
    )
    def test_delegated_argv_parses_on_remote_without_usage_error(
        self, env_root: Path, argv: list[str]
    ) -> None:
        """The crux of the report: the delegated command must PARSE on the
        receiving vq. `--all <HOST-positional>` is a documented valid form;
        it must not be misread as `--all <ENV>` and rejected."""
        with patch("vq.cli.is_local_host", return_value=True), patch(
            "vq.admin.update_all",
            return_value=[MagicMock(success=True, env="vibeqc-dev")],
        ), patch("vq.admin.format_update_all_results", return_value="ok"), patch(
            "vq.admin.format_update_all_results_json", return_value="{}"
        ):
            result = CliRunner().invoke(main, argv)
        assert result.exit_code == 0, result.output
        # The tell-tale of the reported failure was the usage banner.
        assert "Usage:" not in result.output
        assert "HOST_IF_ENV" not in result.output


# ----------------------------------------------------------------------
# 2. A delegated `--all <host>` leaves no marker on the delegating host
# ----------------------------------------------------------------------


class TestNoOrphanMarkerOnDelegatingHost:
    def test_all_remote_writes_no_local_marker(self, env_root: Path) -> None:
        cm, _ = _capture_delegation()
        assert not admin.admin_update_marker_path().exists()
        with patch("vq.cli.is_local_host", side_effect=lambda h: h == "localhost"), cm:
            result = CliRunner().invoke(main, ["admin", "update", "--all", "host_b"])
        assert result.exit_code == 0, result.output
        # The delegating host never runs update_all locally, so it must
        # never write a marker — the marker is owned by the host that runs
        # the rebuild. A regression that took the local path here would
        # strand a marker keyed to the wrong host's envs.
        assert not admin.admin_update_marker_path().exists()


# ----------------------------------------------------------------------
# 3. Marker-present => ClickException (exit 1), NOT the Usage banner
# ----------------------------------------------------------------------


class TestMarkerPresentIsNotAUsageError:
    def test_all_local_marker_present_renders_clean_error(
        self, env_root: Path
    ) -> None:
        marker = _seed_marker(["vibeqc-dev", "vibeqc-queue", "vibeqc-release"])
        result = CliRunner().invoke(main, ["admin", "update", "--all", "localhost"])
        # v0.26.1: exit 76 (`marker-present`), not exit 2 (UsageError). This
        # test's point -- "a marker is a runtime condition, not a mistyped
        # command" -- is unchanged and sharper: the classification now says so
        # outright instead of leaving a caller to infer it from exit 1 plus a
        # sentence. A batch classifies exactly as a single env does.
        assert result.exit_code == 76, result.output
        assert "Usage:" not in result.output
        assert "HOST_IF_ENV" not in result.output
        # A live marker is an in-flight updater, not a stale failure.
        assert "admin-update-in-progress marker present" in result.output
        assert "already running" in result.output
        assert "vq admin status --verbose" in result.output
        # Safety net intact: the guard refused, it did NOT paper over the
        # marker by clearing it.
        assert marker.exists()

    def test_single_env_local_marker_present_renders_clean_error(
        self, env_root: Path
    ) -> None:
        marker = _seed_marker(["vibeqc-dev"])
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "localhost"]
        )
        assert result.exit_code == 76, result.output
        assert "Usage:" not in result.output
        assert "already running" in result.output
        assert "vq admin status --verbose" in result.output
        assert marker.exists()

    def test_force_overrides_marker(self, env_root: Path) -> None:
        """Sanity: --force still bypasses the guard (so the clean-error
        path didn't accidentally swallow the escape hatch). update_env is
        stubbed so we only assert the guard didn't pre-empt the call."""
        _seed_marker(["vibeqc-dev"])
        with patch(
            "vq.admin.update_env",
            return_value=MagicMock(success=True, env="vibeqc-dev"),
        ) as upd, patch("vq.admin.format_update_result", return_value="ok"):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev", "localhost", "--force"]
            )
        assert result.exit_code == 0, result.output
        assert upd.call_args.kwargs.get("force") is True


# ----------------------------------------------------------------------
# 4. Boundary: genuine input errors stay UsageError; subclass wiring
# ----------------------------------------------------------------------


class TestUsageErrorBoundaryPreserved:
    def test_unknown_env_still_usage_error(self, env_root: Path) -> None:
        # A real input error — must keep the helpful Usage banner + exit 2.
        result = CliRunner().invoke(main, ["admin", "update", "ghost"])
        assert result.exit_code == 2
        assert "Usage:" in result.output
        assert "unknown env" in result.output

    def test_admin_update_in_progress_subclasses_admin_error(self) -> None:
        # Subclass so every existing `except AdminError` / pytest.raises
        # keeps catching the marker-present case.
        assert issubclass(admin.AdminUpdateInProgress, admin.AdminError)

    def test_guard_raises_the_subclass(self, env_root: Path) -> None:
        _seed_marker(["vibeqc-dev"])
        with pytest.raises(admin.AdminUpdateInProgress):
            admin._guard_admin_update_marker(force=False)

    def test_acquire_collision_raises_the_subclass(self, env_root: Path) -> None:
        admin.acquire_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
        with pytest.raises(admin.AdminUpdateInProgress):
            admin.acquire_admin_update_marker(envs=["vibeqc-dev"], host="localhost")
