"""Tests for v0.12.0 ``vq doctor`` preflight diagnostics."""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, host_status, paths
from vq import doctor as doctor_module
from vq.cli import main

DRIVER_SHA = "a" * 40
TREE_SHA256 = "12" * 32


@pytest.fixture
def doctor_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.setattr(
        doctor_module,
        "_driver_identity_probe",
        lambda kind, _deadline: (
            (DRIVER_SHA, None)
            if kind == "source_sha"
            else (TREE_SHA256, None)
        ),
    )
    (tmp_path / "cfg").mkdir()
    return tmp_path


def _write_config(root: Path, text: str) -> None:
    (root / "cfg" / "config.toml").write_text(text, encoding="utf-8")


def _write_two_host_config(root: Path) -> None:
    _write_config(
        root,
        'default_host = "goodhost"\n'
        '[hosts.goodhost]\nssh = "goodhost"\n'
        '[hosts.slowhost]\nssh = "slowhost"\n',
    )


def _remote_ping(stdout: dict[str, object], *, returncode: int = 0):
    return subprocess.CompletedProcess(
        args=["vq", "daemon", "ping"],
        returncode=returncode,
        stdout=json.dumps(stdout),
        stderr="",
    )


def _remote_source_sha(sha: str = DRIVER_SHA, *, returncode: int = 0):
    return subprocess.CompletedProcess(
        args=["vq", "source-sha"],
        returncode=returncode,
        stdout=f"{sha}\n" if returncode == 0 else "",
        stderr="" if returncode == 0 else "no SOURCE-SHA marker installed\n",
    )


def _remote_tree_sha(digest: str = TREE_SHA256, *, returncode: int = 0):
    return subprocess.CompletedProcess(
        args=["vq", "source-tree-sha256"],
        returncode=returncode,
        stdout=f"{digest}\n" if returncode == 0 else "",
        stderr="" if returncode == 0 else "source-tree digest unavailable\n",
    )


class TestDoctor:
    def test_cli_calls_public_doctor_service(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, object] = {}

        def fake_diagnose_host(
            cfg: config.Config,
            host: str,
            **kwargs: object,
        ) -> dict[str, object]:
            captured.update(cfg=cfg, host=host, kwargs=kwargs)
            return {
                "host": host,
                "ok": True,
                "scheduler": "local",
                "driver": None,
                "checks": [],
            }

        monkeypatch.setattr(doctor_module, "diagnose_host", fake_diagnose_host)

        result = CliRunner().invoke(main, ["doctor", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        assert json.loads(result.output) == {
            "checks": [],
            "driver": None,
            "host": "localhost",
            "ok": True,
            "scheduler": "local",
        }
        assert captured["host"] == "localhost"
        assert captured["kwargs"] == {
            "admin_update": False,
            "as_driver": None,
            "check_timeout": doctor_module.DEFAULT_CHECK_TIMEOUT_SECONDS,
            "timeout": 2.0,
        }

    def test_implicit_localhost_json(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "vq.doctor._local_daemon_probe",
            lambda timeout, **_kwargs: (
                0,
                {"ok": True, "version": "0.test"},
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["host"] == "localhost"
        assert payload["ok"] is True
        assert payload["checks"][0] == {
            "name": "config",
            "ok": True,
            "message": "implicit localhost",
        }
        assert payload["checks"][1]["name"] == "daemon_rpc"
        assert payload["checks"][1]["ok"] is True

    def test_remote_host_pings_remote_daemon(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.host_a]\n"
            'ssh = "host_a"\n'
            'remote_vq = "/opt/vq/bin/vq"\n',
        )
        captured: dict[str, object] = {}

        def fake_remote(host_cfg, *args, **kwargs):
            if args[:2] == ("web", "status"):
                # The console runtime check (#28) reads the host's own verdict.
                return _remote_ping({"installed": False})
            captured["ssh"] = host_cfg.ssh
            captured["args"] = args
            captured["kwargs"] = kwargs
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        assert captured["ssh"] == "host_a"
        assert captured["args"] == (
            "daemon",
            "ping",
            "--verbose",
            "--json",
            "--timeout",
            "2.0",
            "localhost",
        )
        assert captured["kwargs"]["check"] is False  # type: ignore[index]

    def test_remote_ping_retries_only_an_old_cli_without_verbose(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.host_a]\n" 'ssh = "host_a"\n')
        calls: list[tuple[str, ...]] = []

        def fake_remote(host_cfg, *args, **kwargs):
            if args[:2] == ("web", "status"):
                # The console runtime check (#28), not a ping retry.
                return _remote_ping({"installed": False})
            calls.append(args)
            if "--verbose" in args:
                return subprocess.CompletedProcess(
                    args,
                    2,
                    stdout="",
                    stderr="Error: No such option: --verbose\n",
                )
            return _remote_ping(
                {"ok": True, "version": "legacy", "multi_user": True}
            )

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        daemon_rpc = next(
            item for item in payload["checks"] if item["name"] == "daemon_rpc"
        )
        assert daemon_rpc["ok"] is True
        assert daemon_rpc["version"] == "legacy"
        assert "process_identity" not in daemon_rpc
        assert len(calls) == 2
        assert "--verbose" in calls[0]
        assert "--verbose" not in calls[1]

    def test_legacy_verbose_retry_accepts_clicks_exact_usage_envelope(
        self,
    ) -> None:
        proc = subprocess.CompletedProcess(
            args=["vq", "daemon", "ping"],
            returncode=2,
            stdout="",
            stderr=(
                "Usage: vq daemon ping [OPTIONS] [HOST]\n"
                "Try 'vq daemon ping --help' for help.\n\n"
                "Error: No such option: --verbose\n"
            ),
        )

        assert doctor_module._verbose_ping_is_unsupported(proc) is True

    def test_legacy_verbose_retry_accepts_current_click_diagnostic(
        self,
    ) -> None:
        proc = subprocess.CompletedProcess(
            args=["vq", "daemon", "ping"],
            returncode=2,
            stdout="",
            stderr=(
                "Usage: vq daemon ping [OPTIONS] [HOST]\n"
                "Try 'vq daemon ping --help' for help.\n\n"
                "Error: No such option '--verbose'.\n"
            ),
        )

        assert doctor_module._verbose_ping_is_unsupported(proc) is True

    @pytest.mark.parametrize(
        "stderr",
        [
            "configuration invalid\nNo such option: --verbose\nmore failures\n",
            "Error: No such option: --verbose-extra\n",
            "prefix Error: No such option: --verbose\n",
            (
                "Usage: vq daemon ping [OPTIONS] [HOST]\n"
                "Try 'another daemon ping --help' for help.\n\n"
                "Error: No such option: --verbose\n"
            ),
        ],
    )
    def test_legacy_verbose_retry_rejects_near_miss_errors(
        self, stderr: str
    ) -> None:
        proc = subprocess.CompletedProcess(
            args=["vq", "daemon", "ping"],
            returncode=2,
            stdout="",
            stderr=stderr,
        )

        assert doctor_module._verbose_ping_is_unsupported(proc) is False

    def test_remote_ping_does_not_retry_an_arbitrary_cli_failure(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.host_a]\n" 'ssh = "host_a"\n')
        calls = 0

        def fake_remote(host_cfg, *args, **kwargs):
            nonlocal calls
            calls += 1
            return subprocess.CompletedProcess(
                args,
                2,
                stdout="",
                stderr="configuration is invalid\n",
            )

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 1
        assert calls == 1

    def test_remote_daemon_provenance_survives_in_the_doctor_check(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.host_a]\n" 'ssh = "host_a"\n')
        system_mode = {
            "enabled": True,
            "error": None,
            "source": "/etc/vq/config.toml",
            "status": "enabled",
        }
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
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: _remote_ping(
                {
                    "ok": True,
                    "version": "0.24.0",
                    "source_sha": "a" * 40,
                    "source_tree_sha256": "b" * 64,
                    "multi_user": True,
                    "process_identity": {
                        "status": "ok",
                        "error": None,
                        "pid": 4242,
                        "euid": 0,
                        "python_executable": "/opt/vq/venv/bin/python",
                        "argv": [
                            "/opt/vq/venv/bin/vq",
                            "daemon",
                            "run",
                        ],
                        "version": "0.24.0",
                        "source_sha": "a" * 40,
                        "source_tree_sha256": "b" * 64,
                        "multi_user": True,
                        "socket_path": "/var/lib/vq/daemon.sock",
                    },
                    "socket_path": "/var/lib/vq/daemon.sock",
                    "system_service": system_service,
                    "system_multi_user": system_mode,
                }
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        daemon_rpc = next(
            item for item in payload["checks"] if item["name"] == "daemon_rpc"
        )
        assert daemon_rpc["source_sha"] == "a" * 40
        assert daemon_rpc["source_tree_sha256"] == "b" * 64
        assert daemon_rpc["multi_user"] is True
        assert daemon_rpc["process_identity"] == {
            "status": "ok",
            "error": None,
            "pid": 4242,
            "euid": 0,
            "python_executable": "/opt/vq/venv/bin/python",
            "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
            "version": "0.24.0",
            "source_sha": "a" * 40,
            "source_tree_sha256": "b" * 64,
            "multi_user": True,
            "socket_path": "/var/lib/vq/daemon.sock",
        }
        assert daemon_rpc["socket_path"] == "/var/lib/vq/daemon.sock"
        assert daemon_rpc["system_service"] == system_service
        assert daemon_rpc["system_multi_user"] == system_mode

    def test_remote_daemon_down_is_reported_after_remote_vq_runs(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.host_a]\n" 'ssh = "host_a"\n')
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: _remote_ping(
                {"ok": False, "error": "socket not found"},
                returncode=1,
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["remote_vq"]["ok"] is True
        assert checks["daemon_rpc"] == {
            "name": "daemon_rpc",
            "ok": False,
            "message": "socket not found",
        }

    def test_transport_failure_skips_daemon_json_parse(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.host_a]\n" 'ssh = "host_a"\n')
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["ssh", "host_a"],
                returncode=255,
                stdout="",
                stderr="Permission denied",
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["remote_vq"]["ok"] is False
        assert checks["remote_vq"]["message"] == "Permission denied"
        assert checks["daemon_rpc"] == {
            "name": "daemon_rpc",
            "ok": False,
            "message": "not checked; remote vq failed",
        }

    def test_missing_remote_vq_is_remote_vq_failure(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.host_e]\n"
            'ssh = "host_e"\n'
            'remote_vq = "/opt/vq/missing/bin/vq"\n',
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["ssh", "host_e"],
                returncode=127,
                stdout="",
                stderr="No such file or directory\n",
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_e", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["remote_vq"]["ok"] is False
        message = checks["remote_vq"]["message"]
        assert "/opt/vq/missing/bin/vq failed on host_e (exit 127)" in message
        assert "configured remote_vq command was not found" in message
        assert "configured remote_vq: /opt/vq/missing/bin/vq" in message
        assert "[hosts.host_e].remote_vq" in message
        assert "vq host down host_e --reason \"remote_vq missing\"" in message
        assert "vq host up host_e" in message
        assert checks["daemon_rpc"] == {
            "name": "daemon_rpc",
            "ok": False,
            "message": "not checked; remote vq failed",
        }

    def test_nonzero_remote_vq_with_traceback_is_remote_vq_failure(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.host_a]\n"
            'ssh = "host_a"\n'
            'remote_vq = "/opt/vq/bin/vq"\n',
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["ssh", "host_a"],
                returncode=1,
                stdout="Traceback (most recent call last):\nImportError: stale vq\n",
                stderr="",
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_a", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["remote_vq"]["ok"] is False
        message = checks["remote_vq"]["message"]
        assert "/opt/vq/bin/vq failed on host_a (exit 1)" in message
        assert "Traceback (most recent call last)" in message
        assert "remote vq returned non-JSON output" in message
        assert checks["daemon_rpc"] == {
            "name": "daemon_rpc",
            "ok": False,
            "message": "not checked; remote vq failed",
        }

    def test_text_output_indents_multiline_remote_vq_failure(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.host_e]\n"
            'ssh = "host_e"\n'
            'remote_vq = "/opt/vq/missing/bin/vq"\n',
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: subprocess.CompletedProcess(
                args=["ssh", "host_e"],
                returncode=127,
                stdout="",
                stderr="No such file or directory\n",
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_e"])

        assert result.exit_code == 1
        assert (
            "FAIL remote_vq: /opt/vq/missing/bin/vq failed on host_e "
            "(exit 127): No such file or directory"
        ) in result.output
        assert "\n  hint: the configured remote_vq command was not found" in result.output
        assert "\n  configured remote_vq: /opt/vq/missing/bin/vq" in result.output
        assert "\n  next: update the host's vq install" in result.output
        assert (
            "\n  optional: to keep fleet sweeps quiet until repaired, run "
            '`vq host down host_e --reason "remote_vq missing"`'
        ) in result.output
        assert "restore it with `vq host up host_e`." in result.output
        assert "FAIL daemon_rpc: not checked; remote vq failed" in result.output

    def test_scheduler_host_pings_driver_daemon(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'submit_extra = ["-q", "batch"]\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_max_wall_time_seconds = 28800\n',
        )
        captured: list[tuple[str, tuple[str, ...]]] = []

        def fake_remote(host_cfg, *args, **kwargs):
            captured.append((host_cfg.ssh, args))
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "server_state": "Active",
                            "pbs_sched_running": True,
                            "queues": [
                                {
                                    "name": "batch",
                                    "enabled": True,
                                    "started": True,
                                }
                            ],
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["scheduler"] == "pbs"
        assert payload["driver"] == "driver"
        assert payload["scheduler_lane"] == {
            "partition": "batch",
            "max_wall_time_seconds": 28_800,
            "source": "host-config",
        }
        assert captured == [
            ("host_f-login", ("--version",)),
            ("host_f-login", ("source-tree-sha256",)),
            ("host_f-login", ("source-sha",)),
            (
                "driver-ssh",
                (
                    "daemon",
                    "ping",
                    "--verbose",
                    "--json",
                    "--timeout",
                    "2.0",
                    "localhost",
                ),
            ),
            ("driver-ssh", ("scheduler-probe", "host_f", "--json")),
        ]
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler"]["ok"] is True
        assert checks["scheduler_driver"]["ok"] is True
        assert checks["scheduler_remote_vq"]["ok"] is True
        assert checks["daemon_rpc"]["ok"] is True
        assert checks["scheduler_clients"]["ok"] is True
        assert checks["scheduler_liveness"]["ok"] is True
        assert "pbs_sched running" in checks["scheduler_liveness"]["message"]

    def test_scheduler_host_default_doctor_reports_broken_scheduler_remote_vq(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_c]\n"
            'ssh = "host_c-login"\n'
            'remote_vq = "/opt/vq/missing/bin/vq"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if host_cfg.ssh == "host_c-login" and args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq", "--version"],
                    returncode=127,
                    stdout="",
                    stderr="No such file or directory\n",
                )
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_c"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_c", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "slurm",
                            "scheduler": "slurm",
                            "version": "24.05",
                            "confidence": "confirmed",
                            "binaries": {
                                "sbatch": True,
                                "squeue": True,
                                "scancel": True,
                                "sacct": True,
                                "scontrol": True,
                            },
                            "slurm_squeue_ok": True,
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_c", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_remote_vq"]["ok"] is False
        message = checks["scheduler_remote_vq"]["message"]
        assert "exit 127" in message
        assert "configured remote_vq command was not found" in message
        assert "configured remote_vq: /opt/vq/missing/bin/vq" in message
        assert "[hosts.host_c].remote_vq" in message

    def test_scheduler_host_reports_stopped_pbs_scheduler(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "server_state": "Idle",
                            "pbs_sched_running": False,
                            "queues": [
                                {
                                    "name": "host_f-big",
                                    "enabled": True,
                                    "started": False,
                                }
                            ],
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_clients"]["ok"] is True
        assert checks["scheduler_liveness"]["ok"] is False
        assert "pbs_sched is not running" in checks["scheduler_liveness"]["message"]
        assert "server_state=Idle" in checks["scheduler_liveness"]["message"]

    def test_slurm_scheduler_host_checks_slurm_clients(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_c]\n"
            'ssh = "host_c"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/workspace/USER"\n'
            'node_scratch_dir = "/tmp/$USER"\n'
            'submit_extra = ["--account", "<group-account>", "--partition", "debug"]\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_c"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_c", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "slurm",
                            "scheduler": "slurm",
                            "version": "24.05.3",
                            "confidence": "confirmed",
                            "binaries": {
                                "sbatch": True,
                                "squeue": True,
                                "scancel": True,
                                "sacct": True,
                                "scontrol": True,
                            },
                            "raw_version": "slurm 24.05.3",
                            "server_state": None,
                            "pbs_sched_running": None,
                            "queues": [],
                            "slurm_squeue_ok": True,
                            "slurm_squeue_error": None,
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_c", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_clients"]["ok"] is True
        assert "slurm clients available" in checks["scheduler_clients"]["message"]
        assert checks["scheduler_liveness"]["ok"] is True
        assert "SLURM squeue reachable" in checks["scheduler_liveness"]["message"]

    def test_slurm_scheduler_host_reports_squeue_liveness_failure(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_c]\n"
            'ssh = "host_c"\n'
            'scheduler = "slurm"\n'
            'scheduler_dialect = "slurm"\n'
            'scratch_root = "/workspace/USER"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_c"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_c", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "slurm",
                            "scheduler": "slurm",
                            "version": "24.05.3",
                            "confidence": "confirmed",
                            "binaries": {
                                "sbatch": True,
                                "squeue": True,
                                "scancel": True,
                                "sacct": True,
                                "scontrol": True,
                            },
                            "raw_version": "slurm 24.05.3",
                            "server_state": None,
                            "pbs_sched_running": None,
                            "queues": [],
                            "slurm_squeue_ok": False,
                            "slurm_squeue_error": "slurm_load_jobs error",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_c", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_clients"]["ok"] is True
        assert checks["scheduler_liveness"]["ok"] is False
        assert "SLURM squeue failed" in checks["scheduler_liveness"]["message"]
        assert "slurm_load_jobs error" in checks["scheduler_liveness"]["message"]

    def test_scheduler_host_reports_missing_scheduler_clients(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": None,
                            "scheduler": None,
                            "version": None,
                            "confidence": "unknown",
                            "binaries": {
                                "qsub": False,
                                "qstat": False,
                                "qdel": True,
                                "qhold": False,
                                "qrls": False,
                            },
                            "raw_version": "",
                            "notes": ["no qsub/qstat/qhold/qrls on PATH"],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["daemon_rpc"]["ok"] is True
        assert checks["scheduler_clients"]["ok"] is False
        assert "qsub" in checks["scheduler_clients"]["message"]
        assert "qstat" in checks["scheduler_clients"]["message"]
        assert "qhold" in checks["scheduler_clients"]["message"]
        assert "qrls" in checks["scheduler_clients"]["message"]

    def test_admin_update_mode_reports_missing_scheduler_update_command(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--admin-update", "--json"]
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_clients"]["ok"] is True
        assert checks["scheduler_admin_update"] == {
            "name": "scheduler_admin_update",
            "ok": False,
            "message": (
                "no scheduler_update_command configured; "
                "`vq admin update host_f` is unavailable"
            ),
        }

    def test_admin_update_mode_accepts_configured_scheduler_update_command(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_update_command = "/home/USER/vibeqc-dev/scripts/update_cluster.sh"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--admin-update", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_admin_update"] == {
            "name": "scheduler_admin_update",
            "ok": True,
            "message": (
                "scheduler_update_command configured; "
                "no scheduler_install_command; --cluster-install unavailable"
            ),
        }

    def test_admin_update_mode_checks_scheduler_remote_vq(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n',
        )
        captured: list[tuple[str, tuple[str, ...]]] = []

        def fake_remote(host_cfg, *args, **kwargs):
            captured.append((host_cfg.ssh, args))
            if host_cfg.ssh == "host_f-login" and args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq", "--version"],
                    returncode=0,
                    stdout="vq, version 0.12.0\n",
                    stderr="",
                )
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--admin-update", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_remote_vq"] == {
            "name": "scheduler_remote_vq",
            "ok": True,
            "version": "0.12.0",
            "source_sha": DRIVER_SHA,
            "source_tree_sha256": TREE_SHA256,
            "message": (
                "/home/USER/vibe-queue/.venv/bin/vq available on "
                "host_f-login: vq, version 0.12.0; "
                f"source-tree SHA-256 {TREE_SHA256} and SOURCE-SHA "
                f"{DRIVER_SHA} match driver"
            ),
        }
        assert ("host_f-login", ("--version",)) in captured
        assert ("host_f-login", ("source-tree-sha256",)) in captured
        assert ("host_f-login", ("source-sha",)) in captured

    def test_scheduler_remote_vq_reports_unmarked_helper(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if host_cfg.ssh == "host_f-login" and args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq", "--version"],
                    returncode=0,
                    stdout="vq, version 0.12.0\n",
                    stderr="",
                )
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha(returncode=1)
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_remote_vq"]["ok"] is False
        message = checks["scheduler_remote_vq"]["message"]
        assert "no SOURCE-SHA marker reported" in message
        assert "predates the provenance contract" in message

    def test_scheduler_remote_vq_reports_source_sha_mismatch(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n',
        )
        stale_sha = "b" * 40

        def fake_remote(host_cfg, *args, **kwargs):
            if host_cfg.ssh == "host_f-login" and args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq", "--version"],
                    returncode=0,
                    stdout="vq, version 0.12.0\n",
                    stderr="",
                )
            if args == ("source-tree-sha256",):
                return _remote_tree_sha()
            if args == ("source-sha",):
                return _remote_source_sha(stale_sha)
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_remote_vq"]["ok"] is False
        message = checks["scheduler_remote_vq"]["message"]
        assert "SOURCE-SHA mismatch" in message
        assert stale_sha in message
        assert DRIVER_SHA in message

    def test_admin_update_mode_reports_broken_scheduler_remote_vq(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(
            doctor_state,
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'remote_vq = "/home/USER/vibe-queue/.venv/bin/vq"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n',
        )

        def fake_remote(host_cfg, *args, **kwargs):
            if host_cfg.ssh == "host_f-login" and args == ("--version",):
                return subprocess.CompletedProcess(
                    args=["vq", "--version"],
                    returncode=127,
                    stdout="",
                    stderr="No such file or directory\n",
                )
            if args == ("source-sha",):
                return _remote_source_sha()
            if args[:2] == ("scheduler-probe", "host_f"):
                return subprocess.CompletedProcess(
                    args=["vq", "scheduler-probe", "host_f", "--json"],
                    returncode=0,
                    stdout=json.dumps(
                        {
                            "dialect": "torque",
                            "scheduler": "pbs",
                            "version": "2.5.12",
                            "confidence": "confirmed",
                            "binaries": {
                                "qsub": True,
                                "qstat": True,
                                "qdel": True,
                                "qhold": True,
                                "qrls": True,
                            },
                            "raw_version": "version: 2.5.12",
                            "notes": [],
                        }
                    ),
                    stderr="",
                )
            return _remote_ping({"ok": True, "version": "0.12.0"})

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", fake_remote)

        result = CliRunner().invoke(
            main, ["doctor", "host_f", "--admin-update", "--json"]
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        checks = {check["name"]: check for check in payload["checks"]}
        assert checks["scheduler_admin_update"]["ok"] is True
        assert checks["scheduler_remote_vq"]["ok"] is False
        message = checks["scheduler_remote_vq"]["message"]
        assert "exit 127" in message
        assert "No such file or directory" in message
        assert "configured remote_vq command was not found" in message
        assert "configured remote_vq: /home/USER/vibe-queue/.venv/bin/vq" in message
        assert "[hosts.host_f].remote_vq" in message
        assert "vq host down host_f --reason \"remote_vq missing\"" in message
        assert "vq host up host_f" in message

    def test_all_includes_admin_down_hosts(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_config(doctor_state, "[hosts.down]\n" 'ssh = "down-ssh"\n')
        host_status.mark_down("down", "maintenance")
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda *args, **kwargs: _remote_ping({"ok": True}),
        )

        result = CliRunner().invoke(main, ["doctor", "--all", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["down"]["ok"] is False
        assert payload["down"]["checks"][0]["name"] == "admin_down"
        assert "maintenance" in payload["down"]["checks"][0]["message"]


class TestSchedulerLivenessVerdict:
    """Unit coverage for the PBS liveness verdict on external-scheduler sites."""

    def test_maui_without_pbs_sched_passes(self) -> None:
        from vq.cli import _scheduler_liveness_check_from_payload

        check = _scheduler_liveness_check_from_payload(
            {
                "server_state": "Idle",
                "pbs_sched_running": False,
                "scheduler_daemons": ["maui"],
                "queues": [{"name": "atokat", "enabled": True, "started": True}],
            },
            host="host_f",
            driver_is_local=False,
        )
        assert check["ok"] is True
        assert "maui scheduler running" in str(check["message"])
        assert "server_state=Idle" in str(check["message"])

    def test_no_daemon_at_all_fails(self) -> None:
        from vq.cli import _scheduler_liveness_check_from_payload

        check = _scheduler_liveness_check_from_payload(
            {
                "server_state": "Idle",
                "pbs_sched_running": False,
                "scheduler_daemons": [],
                "queues": [],
            },
            host="host_f",
            driver_is_local=False,
        )
        assert check["ok"] is False
        assert "pbs_sched is not running" in str(check["message"])
        assert "maui/moab" in str(check["message"])

    def test_legacy_payload_without_daemons_field_keeps_old_behavior(self) -> None:
        from vq.cli import _scheduler_liveness_check_from_payload

        check = _scheduler_liveness_check_from_payload(
            {
                "server_state": "Idle",
                "pbs_sched_running": False,
                "queues": [],
            },
            host="host_f",
            driver_is_local=False,
        )
        assert check["ok"] is False

    def test_maui_with_stopped_queue_still_fails(self) -> None:
        from vq.cli import _scheduler_liveness_check_from_payload

        check = _scheduler_liveness_check_from_payload(
            {
                "server_state": "Idle",
                "pbs_sched_running": False,
                "scheduler_daemons": ["maui"],
                "queues": [{"name": "amd", "enabled": True, "started": False}],
            },
            host="host_f",
            driver_is_local=False,
        )
        assert check["ok"] is False
        assert "enabled but not started" in str(check["message"])


class TestDoctorAllRetriesATransportFailure:
    """A contended fan-out must not report a healthy host as failed.

    `vq doctor --all` probes up to eight hosts concurrently. That is enough to
    push a host on a slow ssh route past its probe timeout, and a timed-out or
    exit-255 probe becomes `remote_vq: ok=False`, which makes the whole host
    `ok: false` -- indistinguishable in the output from a real fault. host_0
    did exactly this on 2026-07-27: `verdict: failed` from one `--all` run,
    5/5 ok on its own moments later.

    It matters beyond the report: `fleet_rollout.doctor_failures` turns any
    not-ok in-scope host into `status="blocked"`, so a lost race blocks a whole
    rollout.
    """

    def test_transport_failure_is_reprobed_and_can_recover(
        self, monkeypatch: pytest.MonkeyPatch, doctor_state: Path
    ) -> None:
        _write_two_host_config(doctor_state)
        calls: list[str] = []

        def flaky(cfg, host_name, **kw):  # type: ignore[no-untyped-def]
            calls.append(host_name)
            if host_name == "slowhost" and calls.count("slowhost") == 1:
                return {
                    "host": host_name,
                    "ok": False,
                    "checks": [
                        {
                            "name": "remote_vq",
                            "ok": False,
                            "message": "remote vq failed (exit 255) on slowhost",
                        }
                    ],
                }
            return {
                "host": host_name,
                "ok": True,
                "checks": [{"name": "remote_vq", "ok": True, "message": "ok"}],
            }

        monkeypatch.setattr(doctor_module, "diagnose_host", flaky)
        result = CliRunner().invoke(main, ["doctor", "--all", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["slowhost"]["ok"] is True, (
            "a transport failure was reported as a host fault without a retry"
        )
        assert calls.count("slowhost") == 2, "expected exactly one serial re-probe"
        assert calls.count("goodhost") == 1, "a healthy host must not be re-probed"

    def test_a_genuinely_unreachable_host_still_fails(
        self, monkeypatch: pytest.MonkeyPatch, doctor_state: Path
    ) -> None:
        """The retry must not paper over a host that is really down -- a host
        vq cannot probe must never be silently skipped by a rollout."""
        _write_two_host_config(doctor_state)
        calls: list[str] = []

        def always_down(cfg, host_name, **kw):  # type: ignore[no-untyped-def]
            calls.append(host_name)
            if host_name == "slowhost":
                return {
                    "host": host_name,
                    "ok": False,
                    "checks": [
                        {
                            "name": "remote_vq",
                            "ok": False,
                            "message": "remote vq failed (exit 255) on slowhost",
                        }
                    ],
                }
            return {
                "host": host_name,
                "ok": True,
                "checks": [{"name": "remote_vq", "ok": True, "message": "ok"}],
            }

        monkeypatch.setattr(doctor_module, "diagnose_host", always_down)
        result = CliRunner().invoke(main, ["doctor", "--all", "--json"])

        payload = json.loads(result.output)
        assert payload["slowhost"]["ok"] is False
        assert calls.count("slowhost") == 2, "retried once, then reported honestly"

    def test_a_real_remote_side_fault_is_not_retried(
        self, monkeypatch: pytest.MonkeyPatch, doctor_state: Path
    ) -> None:
        """vq reached the host and found something wrong. Re-probing would
        just cost another round trip to reach the same verdict."""
        _write_two_host_config(doctor_state)
        calls: list[str] = []

        def remote_fault(cfg, host_name, **kw):  # type: ignore[no-untyped-def]
            calls.append(host_name)
            ok = host_name != "slowhost"
            return {
                "host": host_name,
                "ok": ok,
                "checks": [
                    {
                        "name": "scheduler_liveness",
                        "ok": ok,
                        "message": "pbs_sched is not running",
                    }
                ],
            }

        monkeypatch.setattr(doctor_module, "diagnose_host", remote_fault)
        result = CliRunner().invoke(main, ["doctor", "--all", "--json"])

        payload = json.loads(result.output)
        assert payload["slowhost"]["ok"] is False
        assert calls.count("slowhost") == 1, "a remote-side fault needs no retry"
