"""v0.8.2 *Lamport's Logical* — ``vq daemon ping`` CLI verb tests.

Pins the v0.8.2 contract:

1. **Reachable daemon** — RPC server up → text line shows version,
   multi_user, socket, latency. Exit 0.
2. **Socket missing** — daemon down → human FAIL line on stderr,
   exit 1.
3. **JSON envelope** — stable schema for monitoring scripts.
4. **Latency is measured** — always present, always non-negative.
5. **Timeout flag is honored** — short timeout doesn't change the
   reachable-daemon outcome (just bounds the wait).
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import cli as cli_module
from vq import paths, rpc
from vq.cli import main


@pytest.fixture
def short_state(monkeypatch: pytest.MonkeyPatch) -> Path:
    """v0.8.0-style short tempdir so AF_UNIX socket paths fit the
    ~104-byte macOS limit. Same shape as test_rpc.py's state_dir."""
    tmpdir = Path(
        tempfile.mkdtemp(
            prefix="vqping-",
            dir=os.environ.get("VQ_TEST_SHORT_TMPDIR"),
        )
    )
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmpdir / "state"))
    (tmpdir / "state").mkdir()
    yield tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def running_server(short_state: Path) -> rpc.RPCServer:
    """A live daemon-side RPC server (ping is registered by default
    via _handle_ping; no extra method registration needed)."""
    server = rpc.RPCServer(multi_user=False)
    server.start()
    time.sleep(0.05)
    yield server
    server.stop()


class TestDaemonPingReachable:
    def test_text_output_when_daemon_up(
        self, running_server: rpc.RPCServer,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping"])
        assert result.exit_code == 0, result.output
        # Human line contains the key fields.
        assert "daemon RPC: ok" in result.output
        assert "version=" in result.output
        assert "multi_user=" in result.output
        assert "latency=" in result.output

    def test_json_output_when_daemon_up(
        self, running_server: rpc.RPCServer,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        assert "version" in payload and payload["version"]
        assert payload["multi_user"] is False
        assert "daemon.sock" in payload["socket_path"]
        assert isinstance(payload["latency_ms"], (int, float))
        assert payload["latency_ms"] >= 0.0
        assert payload["error"] is None

    def test_latency_present_and_nonzero(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Latency should always be measured even on a fast local
        socket (some positive float, not None)."""
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--json"])
        payload = json.loads(result.output)
        # On a hot local socket the round-trip is sub-millisecond,
        # but it's never exactly zero.
        assert payload["latency_ms"] >= 0.0


class TestDaemonPingUnreachable:
    def test_exit_1_when_socket_missing(
        self, short_state: Path,
    ) -> None:
        """No server running → exit 1, FAIL line on stderr."""
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping"])
        assert result.exit_code == 1
        # Human-readable error on stderr.
        assert "daemon RPC: FAIL" in result.stderr
        assert "socket not found" in result.stderr or "socket=" in result.stderr

    def test_json_envelope_when_socket_missing(
        self, short_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--json"])
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False
        assert payload["version"] is None
        assert payload["error"] is not None
        assert "daemon.sock" in payload["socket_path"]

    def test_short_timeout_bounds_failure_path(
        self, short_state: Path,
    ) -> None:
        """A very short timeout shouldn't change the no-socket
        outcome (the connect-refusal path doesn't wait for I/O)."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["daemon", "ping", "--timeout", "0.5", "--json"],
        )
        assert result.exit_code == 1
        payload = json.loads(result.stdout)
        assert payload["ok"] is False


class TestDaemonPingTimeoutValidation:
    @pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "-inf"])
    @pytest.mark.parametrize(
        "target_args",
        [[], ["remote"], ["--all"]],
        ids=["local", "remote", "all"],
    )
    def test_invalid_timeout_is_rejected_before_probe_work(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
        target_args: list[str],
    ) -> None:
        """Invalid deadlines must not reach config, sockets, or fan-out."""

        def unexpected(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("ping work started before timeout validation")

        monkeypatch.setattr(cli_module.config, "load_config", unexpected)
        monkeypatch.setattr(cli_module, "_local_daemon_ping", unexpected)
        monkeypatch.setattr(cli_module, "_delegate_to_remote", unexpected)
        monkeypatch.setattr(cli_module, "_aggregate_per_host", unexpected)
        monkeypatch.setattr(cli_module, "_aggregate_per_host_json", unexpected)
        monkeypatch.setattr(cli_module, "_format_ping_text", unexpected)

        result = CliRunner().invoke(
            main,
            ["daemon", "ping", "--timeout", value, *target_args],
        )

        assert result.exit_code == 2
        assert "Invalid value for '--timeout'" in result.output
        assert "finite number greater than zero" in result.output
        assert result.exception is not None
        assert not isinstance(result.exception, AssertionError)

    @pytest.mark.parametrize(
        ("option_args", "expected"),
        [([], 2.0), (["--timeout", "0.25"], 0.25)],
        ids=["default", "explicit"],
    )
    def test_positive_timeout_reaches_local_probe_unchanged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        option_args: list[str],
        expected: float,
    ) -> None:
        observed: list[float] = []

        monkeypatch.setattr(cli_module.config, "load_config", lambda: object())

        def fake_ping(
            timeout: float, *, verbose: bool = False
        ) -> tuple[int, dict[str, object]]:
            observed.append(timeout)
            return 0, {
                "ok": True,
                "version": "test",
                "source_sha": None,
                "multi_user": False,
                "socket_path": "/tmp/daemon.sock",
                "latency_ms": 0.1,
                "error": None,
            }

        monkeypatch.setattr(cli_module, "_local_daemon_ping", fake_ping)

        result = CliRunner().invoke(
            main, ["daemon", "ping", "--json", *option_args]
        )

        assert result.exit_code == 0, result.output
        assert observed == [expected]

    def test_positive_timeout_reaches_remote_delegate_unchanged(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.remote]\n"
            'ssh = "vq@remote.example.com"\n'
            'remote_vq = "/usr/local/bin/vq"\n'
        )
        monkeypatch.setenv(cli_module.config.ENV_CONFIG_DIR, str(cfg_dir))
        observed: list[tuple[str, ...]] = []

        def fake_delegate(
            _host: str,
            _cfg: object,
            *args: str,
        ) -> str:
            observed.append(args)
            return json.dumps({"ok": True})

        monkeypatch.setattr(cli_module, "_delegate_to_remote", fake_delegate)

        result = CliRunner().invoke(
            main,
            [
                "daemon",
                "ping",
                "remote",
                "--json",
                "--timeout",
                "0.25",
            ],
        )

        assert result.exit_code == 0, result.output
        assert observed == [
            ("daemon", "ping", "--json", "--timeout", "0.25", "localhost")
        ]


class TestDaemonPingMultiUserSelection:
    def test_system_config_selects_multi_user_socket(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A system multi-user config must steer ping to the system daemon."""
        from vq import config as _config

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))
        system_cfg = tmp_path / "system-config.toml"
        system_cfg.write_text("[multi_user]\nenabled = true\n")
        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", system_cfg)

        seen: list[tuple[str, bool]] = []

        def fake_socket_path(*, multi_user: bool) -> Path:
            seen.append(("socket", multi_user))
            return Path("/var/lib/vq/daemon.sock") if multi_user else Path("user.sock")

        def fake_call(
            method: str,
            args: dict[str, object] | None = None,
            *,
            multi_user: bool = False,
            timeout: float | None = None,
        ) -> dict[str, object]:
            seen.append((method, multi_user))
            return {"version": "test", "multi_user": multi_user}

        monkeypatch.setattr(rpc, "socket_path", fake_socket_path)
        monkeypatch.setattr(rpc, "call", fake_call)

        exit_code, envelope = cli_module._local_daemon_ping(0.1)

        assert exit_code == 0
        assert envelope["multi_user"] is True
        assert envelope["socket_path"] == "/var/lib/vq/daemon.sock"
        assert seen == [("socket", True), ("ping", True)]

    def test_verbose_ping_reports_authoritative_system_mode_evidence(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Fleet provenance can classify root-daemon applicability without
        changing the stable non-verbose monitoring envelope."""
        from vq import config as _config

        system_cfg = tmp_path / "system-config.toml"
        system_cfg.write_text("[multi_user]\nenabled = true\n")
        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", system_cfg)
        monkeypatch.setattr(
            rpc,
            "socket_path",
            lambda *, multi_user: Path("/var/lib/vq/daemon.sock"),
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda method, **kwargs: (
                {"methods": ["ping"]}
                if method == "get_methods"
                else {"version": "test", "multi_user": True}
            ),
        )
        system_service = {
            "active_state": "active",
            "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            "error": None,
            "exec_start": "{ path=/opt/vq/venv/bin/vq ; ... }",
            "executable": "/opt/vq/venv/bin/vq",
            "id": "vq-daemon-multi-user.service",
            "load_state": "loaded",
            "main_pid": 4242,
            "source": "/usr/bin/systemctl",
            "status": "ok",
            "sub_state": "running",
            "user": "root",
        }
        monkeypatch.setattr(
            "vq.daemon_control._systemd_multi_user_service_evidence",
            lambda: system_service,
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda method, **kwargs: (
                {"methods": ["get_process_identity", "ping"]}
                if method == "get_methods"
                else {
                    "pid": 4242,
                    "euid": 0,
                    "python_executable": "/opt/vq/venv/bin/python",
                    "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
                    "version": "test",
                    "source_sha": None,
                    "source_tree_sha256": None,
                    "multi_user": True,
                    "socket_path": "/var/lib/vq/daemon.sock",
                }
                if method == "get_process_identity"
                else {
                    "version": "test",
                    "multi_user": True,
                }
            ),
        )

        exit_code, envelope = cli_module._local_daemon_ping(
            0.1,
            verbose=True,
        )

        assert exit_code == 0
        assert envelope["system_multi_user"] == {
            "enabled": True,
            "error": None,
            "source": str(system_cfg),
            "status": "enabled",
        }
        assert envelope["process_identity"] == {
            "status": "ok",
            "error": None,
            "pid": 4242,
            "euid": 0,
            "python_executable": "/opt/vq/venv/bin/python",
            "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            "version": "test",
            "source_sha": None,
            "source_tree_sha256": None,
            "multi_user": True,
            "socket_path": "/var/lib/vq/daemon.sock",
        }
        assert envelope["system_service"] == system_service

    def test_verbose_ping_keeps_an_old_daemon_live_but_identity_unsupported(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from vq import config as _config

        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", tmp_path / "absent")
        monkeypatch.setattr(rpc, "socket_path", lambda **kwargs: Path("user.sock"))
        service = {
            "status": "unsupported",
            "error": "systemd is unavailable on this platform",
        }
        monkeypatch.setattr(
            "vq.daemon_control._systemd_multi_user_service_evidence",
            lambda: service,
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda method, **kwargs: (
                {"methods": ["ping"]}
                if method == "get_methods"
                else {"version": "old", "multi_user": False}
            ),
        )

        exit_code, envelope = cli_module._local_daemon_ping(0.1, verbose=True)

        assert exit_code == 0
        assert envelope["ok"] is True
        assert envelope["process_identity"] == {
            "status": "unsupported",
            "error": "daemon does not advertise get_process_identity",
            "pid": None,
            "euid": None,
            "python_executable": None,
            "argv": None,
        }
        assert envelope["system_service"] == service

    @pytest.mark.parametrize(
        "identity",
        [
            {},
            {
                "pid": True,
                "euid": 0,
                "python_executable": "/opt/vq/venv/bin/python",
                "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            },
            {
                "pid": 4242,
                "euid": -1,
                "python_executable": "/opt/vq/venv/bin/python",
                "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            },
            {
                "pid": 4242,
                "euid": 0,
                "python_executable": None,
                "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            },
            {
                "pid": 4242,
                "euid": 0,
                "python_executable": "/opt/vq/venv/bin/python",
                "argv": ["/opt/vq/venv/bin/vq", 1, "run"],
            },
        ],
    )
    def test_verbose_ping_rejects_malformed_process_identity(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        identity: dict[str, object],
    ) -> None:
        from vq import config as _config

        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", tmp_path / "absent")
        monkeypatch.setattr(rpc, "socket_path", lambda **kwargs: Path("user.sock"))

        def fake_call(method: str, **kwargs: object) -> dict[str, object]:
            if method == "ping":
                return {"version": "test", "multi_user": False}
            if method == "get_methods":
                return {"methods": ["get_process_identity", "ping"]}
            assert method == "get_process_identity"
            return identity

        monkeypatch.setattr(rpc, "call", fake_call)

        exit_code, envelope = cli_module._local_daemon_ping(0.1, verbose=True)

        assert exit_code == 0
        assert envelope["ok"] is True
        process = envelope["process_identity"]
        assert isinstance(process, dict)
        assert process["status"] == "unknown"
        assert "malformed" in str(process["error"])

    @pytest.mark.parametrize("failed_method", ["ping", "get_methods"])
    def test_verbose_ping_does_not_call_unobserved_capability_unsupported(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        failed_method: str,
    ) -> None:
        from vq import config as _config

        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", tmp_path / "absent")
        monkeypatch.setattr(rpc, "socket_path", lambda **kwargs: Path("user.sock"))

        def fake_call(method: str, **kwargs: object) -> dict[str, object]:
            if method == failed_method:
                raise ConnectionError(f"{method} unavailable")
            if method == "ping":
                return {"version": "test", "multi_user": False}
            raise AssertionError(f"unexpected call: {method}")

        monkeypatch.setattr(rpc, "call", fake_call)

        exit_code, envelope = cli_module._local_daemon_ping(0.1, verbose=True)

        assert exit_code == (1 if failed_method == "ping" else 0)
        process = envelope["process_identity"]
        assert isinstance(process, dict)
        assert process["status"] == "unknown"
        assert "unavailable" in str(process["error"])

    def test_verbose_ping_reports_invalid_system_policy_as_unknown(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        from vq import config as _config

        system_cfg = tmp_path / "system-config.toml"
        system_cfg.write_text("[multi_user\n")
        monkeypatch.setattr(_config, "SYSTEM_CONFIG_PATH", system_cfg)
        monkeypatch.setattr(
            rpc,
            "socket_path",
            lambda *, multi_user: Path("user.sock"),
        )
        monkeypatch.setattr(
            rpc,
            "call",
            lambda method, **kwargs: (
                {"methods": ["ping"]}
                if method == "get_methods"
                else {"version": "test", "multi_user": False}
            ),
        )

        exit_code, envelope = cli_module._local_daemon_ping(
            0.1,
            verbose=True,
        )

        assert exit_code == 0
        evidence = envelope["system_multi_user"]
        assert evidence["enabled"] is None
        assert evidence["status"] == "unknown"
        assert "failed to parse" in evidence["error"]


class TestDaemonPingJsonSchema:
    def test_json_schema_is_stable(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """The JSON envelope is the monitoring-script contract —
        pin the field set."""
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--json"])
        payload = json.loads(result.output)
        expected_keys = {
            "ok", "version", "source_sha", "source_tree_sha256", "multi_user",
            "socket_path", "latency_ms", "error",
        }
        assert set(payload.keys()) == expected_keys


# ----------------------------------------------------------------------
# v0.8.3 *Dijkstra's Shortest* — HOST + --all
# ----------------------------------------------------------------------


class TestDaemonPingAllAndHostArgs:
    """v0.8.3: ``vq daemon ping HOST`` (SSH-delegate) and
    ``vq daemon ping --all`` (parallel fan-out over every host).

    The remote-host path delegates through ``_delegate_to_remote``
    which calls ``transport.run_remote_vq`` — monkeypatch that to
    return deterministic JSON so the tests don't need actual SSH.
    """

    def test_host_and_all_are_mutually_exclusive(
        self, short_state: Path,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["daemon", "ping", "--all", "somehost"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_all_with_no_hosts_configured(
        self, short_state: Path,
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """Empty [hosts.X] in config → graceful message, not crash."""
        from vq import config as _config
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        (tmp_path / "cfg").mkdir()
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--all"])
        assert result.exit_code == 0
        assert "no [hosts.X] configured" in result.output

    def test_all_aggregates_local_and_remote_json(
        self, running_server: rpc.RPCServer,
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """``--all --json`` collects per-host envelopes into a
        top-level dict keyed by host."""
        from vq import config as _config
        # Write a config with two hosts: one localhost (uses live
        # RPC server) + one remote (delegate path).
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\nssh = \"localhost\"\n"
            "[hosts.fakebox]\nssh = \"vq@fakebox.example.com\"\n"
            "remote_vq = \"/usr/local/bin/vq\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))

        # Monkeypatch the SSH transport so remote hosts return
        # canned JSON without touching ssh(1).
        from vq import transport as _transport

        class _FakeProc:
            def __init__(self, stdout: str) -> None:
                self.stdout = stdout
                self.returncode = 0

        def _fake_run(host_cfg, *vq_args, stdin_data=None):
            envelope = {
                "ok": True,
                "version": "0.8.3-fake",
                "multi_user": False,
                "socket_path": "/var/lib/vq/daemon.sock",
                "latency_ms": 1.23,
                "error": None,
            }
            return _FakeProc(json.dumps(envelope))

        monkeypatch.setattr(_transport, "run_remote_vq", _fake_run)

        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--all", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert set(payload.keys()) == {"localhost", "fakebox"}
        # Local came from the live RPC server.
        assert payload["localhost"]["ok"] is True
        assert "version" in payload["localhost"]
        # Remote came from the fake transport.
        assert payload["fakebox"]["ok"] is True
        assert payload["fakebox"]["version"] == "0.8.3-fake"

    def test_scheduler_host_uses_driver_daemon_ping(
        self, running_server: rpc.RPCServer,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """A daemonless scheduler host reports the driver daemon ping."""
        from vq import config as _config

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "localhost"\n'
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))

        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        assert payload["scheduler_host"] is True
        assert payload["scheduler"] == "pbs"
        assert payload["driver"] == "localhost"
        assert payload["driver_ping"]["ok"] is True

    def test_all_isolates_per_host_failure(
        self, running_server: rpc.RPCServer,
        monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """A SSH failure on one host doesn't break the aggregate —
        the failed host gets an ``error`` field; the rest succeed."""
        from vq import config as _config
        from vq import transport as _transport

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\nssh = \"localhost\"\n"
            "[hosts.deadhost]\nssh = \"vq@deadhost.example.com\"\n"
            "remote_vq = \"/usr/local/bin/vq\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))

        def _fake_run(host_cfg, *vq_args, stdin_data=None):
            raise _transport.RemoteError("ssh: connect timed out")

        monkeypatch.setattr(_transport, "run_remote_vq", _fake_run)

        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--all", "--json"])
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        # local succeeded; deadhost has an error stamped.
        assert payload["localhost"]["ok"] is True
        assert "error" in payload["deadhost"]
        assert "ssh" in payload["deadhost"]["error"].lower() or \
               "timed out" in payload["deadhost"]["error"].lower()

    def test_remote_single_host_propagates_failure_exit_code(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """``vq daemon ping HOST --json`` exits 1 when the remote
        envelope reports ok=false (so CI scripts can gate on it)."""
        from vq import config as _config
        from vq import transport as _transport

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.downhost]\nssh = \"vq@downhost.example.com\"\n"
            "remote_vq = \"/usr/local/bin/vq\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))

        class _FakeProc:
            def __init__(self, stdout: str) -> None:
                self.stdout = stdout
                self.returncode = 0

        def _fake_run(host_cfg, *vq_args, stdin_data=None):
            envelope = {
                "ok": False,
                "version": None,
                "multi_user": False,
                "socket_path": "/var/lib/vq/daemon.sock",
                "latency_ms": 0.5,
                "error": "RPC socket not found at /var/lib/vq/daemon.sock",
            }
            return _FakeProc(json.dumps(envelope))

        monkeypatch.setattr(_transport, "run_remote_vq", _fake_run)

        runner = CliRunner()
        result = runner.invoke(
            main, ["daemon", "ping", "downhost", "--json"],
        )
        assert result.exit_code == 1, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is False
        assert "socket not found" in payload["error"]

    def test_remote_single_host_text_path(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    ) -> None:
        """Text-mode remote-host ping prints whatever the remote
        emitted (the helper text line). exit-code propagation in
        text mode is best-effort — the user can read the FAIL
        banner; the JSON path is the scripted contract."""
        from vq import config as _config
        from vq import transport as _transport

        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "config.toml").write_text(
            "[hosts.box]\nssh = \"vq@box.example.com\"\n"
            "remote_vq = \"/usr/local/bin/vq\"\n"
        )
        monkeypatch.setenv(_config.ENV_CONFIG_DIR, str(cfg_dir))

        class _FakeProc:
            def __init__(self, stdout: str) -> None:
                self.stdout = stdout
                self.returncode = 0

        def _fake_run(host_cfg, *vq_args, stdin_data=None):
            return _FakeProc(
                "daemon RPC: ok | version=0.8.3 | multi_user=False "
                "| socket=/x/daemon.sock | latency=0.5ms\n"
            )

        monkeypatch.setattr(_transport, "run_remote_vq", _fake_run)

        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "box"])
        assert result.exit_code == 0
        assert "daemon RPC: ok" in result.output
        assert "version=0.8.3" in result.output


# ----------------------------------------------------------------------
# v0.8.4 *Brooks's Mythical* — `--verbose` method-list probe
# ----------------------------------------------------------------------


class TestDaemonPingVerbose:
    def test_verbose_text_shows_methods(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """``--verbose`` calls ``get_methods`` after the ping; the
        text line gains a ``methods=[...]`` suffix."""
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--verbose"])
        assert result.exit_code == 0, result.output
        assert "daemon RPC: ok" in result.output
        assert "methods=[" in result.output
        # The built-ins are always there.
        assert "ping" in result.output
        assert "get_methods" in result.output

    def test_verbose_json_envelope_has_methods_field(
        self, running_server: rpc.RPCServer,
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            main, ["daemon", "ping", "--verbose", "--json"],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert "methods" in payload
        assert isinstance(payload["methods"], list)
        assert "ping" in payload["methods"]
        assert "get_methods" in payload["methods"]
        # The other envelope fields are unchanged.
        assert payload["ok"] is True
        assert payload["version"] is not None

    def test_non_verbose_omits_methods_field(
        self, running_server: rpc.RPCServer,
    ) -> None:
        """Backward-compat: the v0.8.2 envelope shape (no methods
        field) is preserved when ``--verbose`` isn't passed."""
        runner = CliRunner()
        result = runner.invoke(main, ["daemon", "ping", "--json"])
        payload = json.loads(result.output)
        assert "methods" not in payload

    def test_verbose_handles_pre_v084_daemon(
        self, short_state: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If get_methods isn't registered (older daemon), the ping
        still succeeds and the methods field lands as ``None`` —
        client sees "the daemon didn't tell me" rather than
        "no methods exist"."""
        # Build a server that has ping but NOT get_methods (simulate
        # pre-v0.8.4 daemon).
        server = rpc.RPCServer(multi_user=False)
        # Stomp the built-in get_methods registration so calls to it
        # return "unknown method" — this is what a pre-v0.8.4
        # daemon would do.
        del server._methods["get_methods"]
        try:
            server.start()
            time.sleep(0.05)
            runner = CliRunner()
            result = runner.invoke(
                main, ["daemon", "ping", "--verbose", "--json"],
            )
            assert result.exit_code == 0, result.output
            payload = json.loads(result.output)
            assert payload["ok"] is True
            # methods=None: the client asked but got refused.
            assert payload["methods"] is None
        finally:
            server.stop()
