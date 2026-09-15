"""Regression coverage for BUG 70 doctor subprocess deadlines."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, doctor, paths, scheduler_probe, ssh_probe, transport
from vq.cli import main


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _SequenceClock:
    def __init__(self, *values: float) -> None:
        self._values = iter(values)
        self._last = values[-1]

    def monotonic(self) -> float:
        self._last = next(self._values, self._last)
        return self._last


def _timeout_with_private_output(timeout: float) -> None:
    try:
        raise subprocess.TimeoutExpired(["ssh", "private-target"], timeout)
    except subprocess.TimeoutExpired as exc:
        raise transport.RemoteOutcomeUnknown(
            "PRIVATE-RAW-REMOTE-OUTPUT must not reach doctor"
        ) from exc


def _completed(
    args: tuple[str, ...],
    stdout: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args, 0, stdout, "")


def _scheduler_host() -> config.HostConfig:
    return config.HostConfig(
        ssh="scheduler-login",
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/scratch",
        scheduler_driver="driver",
    )


def test_tiny_positive_timeout_metadata_is_not_rounded_to_zero() -> None:
    tiny = float.fromhex("0x0.0000000000001p-1022")
    result = doctor._timed_out_check(  # noqa: SLF001 - deadline contract
        "probe",
        doctor._DoctorCheckTimeout(  # noqa: SLF001 - deadline contract
            subprobe="tiny_probe",
            elapsed_seconds=tiny,
            timeout_seconds=tiny,
        ),
    )

    assert result["elapsed_seconds"] == tiny
    assert result["timeout_seconds"] == tiny
    assert result["elapsed_seconds"] > 0
    assert result["timeout_seconds"] > 0


def test_cli_help_and_forwarding_keep_rpc_and_check_timeouts_separate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    help_result = CliRunner().invoke(main, ["doctor", "--help"])
    assert help_result.exit_code == 0, help_result.output
    assert "--timeout" in help_result.output
    assert "Daemon RPC timeout" in help_result.output
    assert "--check-timeout" in help_result.output
    assert "outer deadline" in help_result.output

    captured: dict[str, object] = {}
    monkeypatch.setattr(config, "load_config", config.Config)

    def fake_diagnose_host(
        _cfg: config.Config,
        host: str,
        **kwargs: object,
    ) -> dict[str, object]:
        captured.update(host=host, kwargs=kwargs)
        return {
            "host": host,
            "ok": True,
            "scheduler": "local",
            "driver": None,
            "checks": [],
        }

    monkeypatch.setattr(doctor, "diagnose_host", fake_diagnose_host)
    result = CliRunner().invoke(
        main,
        [
            "doctor",
            "localhost",
            "--timeout",
            "0.25",
            "--check-timeout",
            "0.75",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["ok"] is True
    assert captured == {
        "host": "localhost",
        "kwargs": {
            "admin_update": False,
            "as_driver": None,
            "check_timeout": 0.75,
            "timeout": 0.25,
        },
    }


@pytest.mark.parametrize("option", ["--timeout", "--check-timeout"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1"])
def test_cli_rejects_nonfinite_or_nonpositive_timeouts_before_diagnosis(
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    value: str,
) -> None:
    monkeypatch.setattr(
        doctor,
        "diagnose_host",
        lambda *_args, **_kwargs: pytest.fail("diagnosis must not start"),
    )

    result = CliRunner().invoke(main, ["doctor", "localhost", option, value])

    assert result.exit_code == 2
    assert "finite number greater than zero" in result.output


def test_remote_daemon_outer_timeout_is_structured_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    seen: list[float] = []

    def stalled_remote_vq(
        _host_cfg: config.HostConfig,
        *_args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        assert kwargs["owned_process_group"] is True
        outer_timeout = float(kwargs["timeout"])
        seen.append(outer_timeout)
        clock.advance(outer_timeout)
        _timeout_with_private_output(outer_timeout)

    monkeypatch.setattr(transport, "run_remote_vq", stalled_remote_vq)
    checks, transport_failed = doctor.ping_remote(
        config.HostConfig(ssh="remote-host"),
        host_label="remote",
        timeout=7.0,
        check_timeout=0.25,
    )

    assert seen == [pytest.approx(0.25)]
    assert transport_failed is False
    assert checks[0]["name"] == "remote_vq"
    assert checks[0]["ok"] is False
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "daemon_ping"
    assert checks[0]["elapsed_seconds"] == pytest.approx(0.25)
    assert checks[0]["timeout_seconds"] == pytest.approx(0.25)
    assert "PRIVATE-RAW-REMOTE-OUTPUT" not in str(checks)
    assert checks[1] == {
        "name": "daemon_rpc",
        "ok": False,
        "message": "not checked; remote vq timed out",
    }


def test_delegated_scheduler_probe_timeout_is_structured_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    seen: list[float] = []

    def stalled_remote_vq(
        _host_cfg: config.HostConfig,
        *_args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        assert kwargs["owned_process_group"] is True
        outer_timeout = float(kwargs["timeout"])
        seen.append(outer_timeout)
        clock.advance(outer_timeout)
        _timeout_with_private_output(outer_timeout)

    monkeypatch.setattr(transport, "run_remote_vq", stalled_remote_vq)
    cfg = config.Config(hosts={"driver": config.HostConfig(ssh="driver")})
    checks = doctor.scheduler_probe_checks(
        cfg,
        "host_f",
        _scheduler_host(),
        driver="driver",
        check_timeout=0.4,
    )

    assert seen == [pytest.approx(0.4)]
    assert checks[0]["name"] == "scheduler_clients"
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "scheduler_probe"
    assert checks[0]["elapsed_seconds"] == pytest.approx(0.4)
    assert "PRIVATE-RAW-REMOTE-OUTPUT" not in str(checks)
    assert checks[1] == {
        "name": "scheduler_liveness",
        "ok": False,
        "message": "not checked; probe timed out",
    }


def test_local_driver_scheduler_probe_bounds_remote_shell(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    seen: list[float] = []

    def stalled_remote_shell(
        _host_cfg: config.HostConfig,
        *_args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        assert kwargs["owned_process_group"] is True
        outer_timeout = float(kwargs["timeout"])
        seen.append(outer_timeout)
        clock.advance(outer_timeout)
        _timeout_with_private_output(outer_timeout)

    monkeypatch.setattr(transport, "run_remote_shell", stalled_remote_shell)
    checks = doctor.scheduler_probe_checks(
        config.Config(),
        "host_f",
        _scheduler_host(),
        driver="localhost",
        check_timeout=0.3,
    )

    assert seen == [pytest.approx(0.3)]
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "scheduler_probe"
    assert checks[0]["elapsed_seconds"] == pytest.approx(0.3)
    assert "PRIVATE-RAW-REMOTE-OUTPUT" not in str(checks)
    assert checks[1]["message"] == "not checked; probe timed out"


def test_local_daemon_uses_check_budget_as_its_inner_rpc_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(config, "load_config", config.Config)
    seen: list[tuple[float, float]] = []

    def stalled_runner(argv: list[str], outer_timeout: float):
        inner_timeout = float(argv[-2])
        seen.append((inner_timeout, outer_timeout))
        clock.advance(outer_timeout)
        raise subprocess.TimeoutExpired(
            argv,
            outer_timeout,
            output="PRIVATE-RAW-LOCAL-RPC-TIMEOUT",
        )

    real_probe = doctor._local_daemon_probe  # noqa: SLF001 - bounded seam

    def injected_probe(
        timeout: float,
        *,
        multi_user: bool,
        deadline: doctor._CheckDeadline,  # noqa: SLF001 - bounded seam
    ):
        return real_probe(
            timeout,
            multi_user=multi_user,
            deadline=deadline,
            runner=stalled_runner,
        )

    monkeypatch.setattr(
        doctor,
        "_local_daemon_probe",
        injected_probe,
    )
    checks, transport_failed = doctor.ping_local(
        timeout=7.0,
        check_timeout=0.2,
    )

    assert seen == [(pytest.approx(0.2), pytest.approx(0.2))]
    assert transport_failed is False
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "daemon_rpc"
    assert checks[0]["elapsed_seconds"] == pytest.approx(0.2)
    assert "PRIVATE-RAW-LOCAL-RPC-TIMEOUT" not in str(checks)


def test_complete_local_verbose_daemon_probe_has_killable_outer_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(config, "load_config", config.Config)

    def stalled_verbose_runner(argv: list[str], timeout: float):
        worker_script = argv[2]
        worker_args = argv[3:]
        wrapper = f"""
import signal
import sys
import time
from vq import daemon_control

def stalled_local_daemon_ping(*_args, **_kwargs):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(30)

daemon_control.local_daemon_ping = stalled_local_daemon_ping
sys.argv = ["local-daemon-probe", *{worker_args!r}]
exec({worker_script!r})
""".strip()
        return transport.run_owned_subprocess(
            [sys.executable, "-c", wrapper],
            timeout=timeout,
            terminate_grace_seconds=0.05,
            kill_grace_seconds=0.5,
        )

    real_probe = doctor._local_daemon_probe  # noqa: SLF001 - bounded seam

    def injected_probe(
        timeout: float,
        *,
        multi_user: bool,
        deadline: doctor._CheckDeadline,  # noqa: SLF001 - bounded seam
    ):
        return real_probe(
            timeout,
            multi_user=multi_user,
            deadline=deadline,
            runner=stalled_verbose_runner,
        )

    monkeypatch.setattr(doctor, "_local_daemon_probe", injected_probe)
    started = time.monotonic()
    checks, transport_failed = doctor.ping_local(
        timeout=7.0,
        check_timeout=0.1,
    )
    elapsed = time.monotonic() - started

    assert elapsed < 2.0
    assert transport_failed is False
    assert checks[0]["ok"] is False
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "daemon_rpc"


def test_local_daemon_preprobe_budget_exhaustion_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)

    def delayed_config() -> config.Config:
        clock.advance(0.01)
        return config.Config()

    monkeypatch.setattr(config, "load_config", delayed_config)
    monkeypatch.setattr(
        transport,
        "run_owned_subprocess",
        lambda *_a, **_k: pytest.fail("expired daemon probe must not start"),
    )

    checks, transport_failed = doctor.ping_local(
        timeout=2.0,
        check_timeout=0.001,
    )

    assert transport_failed is False
    assert checks[0]["ok"] is False
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "daemon_rpc"


def _direct_route() -> ssh_probe.SshRoute:
    return ssh_probe.SshRoute(
        destination="remote",
        hostname="remote.example.invalid",
        port=22,
        user="operator",
        proxy_jump=None,
        proxy_command=None,
        identity_files=(),
        first_hop=ssh_probe.Hop("target", "remote.example.invalid", 22),
    )


def test_ssh_route_resolution_timeout_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)

    def stalled_route(_destination: str, *, timeout: float):
        clock.advance(timeout)
        try:
            raise subprocess.TimeoutExpired(["ssh", "-G"], timeout)
        except subprocess.TimeoutExpired as exc:
            raise ssh_probe.SshProbeTimeout("PRIVATE-SSH-CONFIG") from exc

    monkeypatch.setattr(ssh_probe, "resolve_route", stalled_route)
    checks, route, probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.25,
    )

    assert route is None
    assert probe is None
    assert blocked is True
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "ssh_route"
    assert checks[0]["elapsed_seconds"] == pytest.approx(0.25)
    assert "PRIVATE-SSH-CONFIG" not in str(checks)


def test_ssh_route_preprobe_budget_exhaustion_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _SequenceClock(100.0, 100.01, 100.01)
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        ssh_probe,
        "resolve_route",
        lambda *_a, **_k: pytest.fail("expired route probe must not start"),
    )

    checks, route, probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.001,
    )

    assert route is None
    assert probe is None
    assert blocked is True
    assert checks[0]["timed_out"] is True
    assert checks[0]["subprobe"] == "ssh_route"


def test_ssh_first_hop_timeout_uses_remaining_budget_and_stops_dependents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(ssh_probe, "resolve_route", lambda *_a, **_k: _direct_route())
    seen: list[float] = []

    def stalled_tcp(host: str, port: int, *, timeout: float):
        seen.append(timeout)
        clock.advance(timeout)
        return ssh_probe.TcpProbe(
            host,
            port,
            "timeout",
            "PRIVATE-TCP-DETAIL",
            timeout,
        )

    monkeypatch.setattr(ssh_probe, "probe_tcp", stalled_tcp)
    monkeypatch.setattr(
        ssh_probe,
        "control_master_active",
        lambda *_a, **_k: pytest.fail("dependent probe must not run"),
    )
    checks, _route, _probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.3,
    )

    assert seen == [pytest.approx(0.3)]
    assert blocked is True
    first_hop = checks[-1]
    assert first_hop["timed_out"] is True
    assert first_hop["subprobe"] == "ssh_first_hop"
    assert first_hop["elapsed_seconds"] == pytest.approx(0.3)
    assert "PRIVATE-TCP-DETAIL" not in str(first_hop)


def test_ssh_first_hop_just_late_success_is_structured_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(ssh_probe, "resolve_route", lambda *_a, **_k: _direct_route())

    def just_late_success(host: str, port: int, *, timeout: float):
        clock.advance(timeout)
        return ssh_probe.TcpProbe(
            host,
            port,
            "reachable",
            "accepted just after deadline",
            timeout,
        )

    monkeypatch.setattr(ssh_probe, "probe_tcp", just_late_success)

    checks, route, probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.2,
    )

    assert route is not None
    assert probe is not None
    assert blocked is True
    assert checks[-1]["ok"] is False
    assert checks[-1]["timed_out"] is True
    assert checks[-1]["subprobe"] == "ssh_first_hop"


def test_ssh_first_hop_preprobe_budget_exhaustion_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _SequenceClock(
        100.0,
        100.0,
        100.0,
        100.0,
        100.01,
        100.01,
    )
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(ssh_probe, "resolve_route", lambda *_a, **_k: _direct_route())
    monkeypatch.setattr(
        ssh_probe,
        "probe_tcp",
        lambda *_a, **_k: pytest.fail("expired first-hop probe must not start"),
    )

    checks, route, probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.001,
    )

    assert route is not None
    assert probe is None
    assert blocked is True
    assert checks[-1]["timed_out"] is True
    assert checks[-1]["subprobe"] == "ssh_first_hop"


def test_control_master_probe_cannot_extend_first_hop_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(ssh_probe, "resolve_route", lambda *_a, **_k: _direct_route())
    monkeypatch.setattr(
        ssh_probe,
        "probe_tcp",
        lambda host, port, **_kwargs: ssh_probe.TcpProbe(
            host,
            port,
            "refused",
            "connection refused",
            0.01,
        ),
    )
    seen: list[float] = []

    def stalled_master(_destination: str, *, timeout: float) -> bool:
        seen.append(timeout)
        clock.advance(timeout)
        return False

    monkeypatch.setattr(ssh_probe, "control_master_active", stalled_master)
    checks, _route, _probe, blocked = doctor.local_ssh_checks(
        config.HostConfig(ssh="remote"),
        probe_cache={},
        check_timeout=0.2,
    )

    assert seen == [pytest.approx(0.2)]
    assert blocked is True
    assert checks[-1]["timed_out"] is True
    assert checks[-1]["subprobe"] == "ssh_control_master"


def test_large_outer_budget_does_not_lengthen_existing_ssh_probe_caps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, float] = {}

    def resolve(_destination: str, *, timeout: float):
        seen["route"] = timeout
        return _direct_route()

    def tcp(host: str, port: int, *, timeout: float):
        seen["tcp"] = timeout
        return ssh_probe.TcpProbe(
            host,
            port,
            "refused",
            "connection refused",
            0.01,
        )

    def master(_destination: str, *, timeout: float) -> bool:
        seen["master"] = timeout
        return False

    def verbose(_destination: str, *, timeout: float):
        seen["verbose"] = timeout
        return ssh_probe.VerboseProbe(255, (), "connection failed")

    monkeypatch.setattr(ssh_probe, "resolve_route", resolve)
    monkeypatch.setattr(ssh_probe, "probe_tcp", tcp)
    monkeypatch.setattr(ssh_probe, "control_master_active", master)
    monkeypatch.setattr(ssh_probe, "verbose_probe", verbose)

    host_cfg = config.HostConfig(ssh="remote")
    doctor.local_ssh_checks(
        host_cfg,
        probe_cache={},
        check_timeout=99.0,
    )
    doctor.ssh_transport_check(
        host_cfg,
        _direct_route(),
        None,
        check_timeout=99.0,
    )

    assert seen == {
        "route": pytest.approx(ssh_probe.DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS),
        "tcp": pytest.approx(ssh_probe.DEFAULT_TCP_PROBE_TIMEOUT_SECONDS),
        "master": pytest.approx(ssh_probe.DEFAULT_CONFIG_DUMP_TIMEOUT_SECONDS),
        "verbose": pytest.approx(ssh_probe.DEFAULT_VERBOSE_PROBE_TIMEOUT_SECONDS),
    }


def test_verbose_ssh_probe_timeout_is_structured_and_redacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    seen: list[float] = []

    def stalled_verbose(_destination: str, *, timeout: float):
        seen.append(timeout)
        clock.advance(timeout)
        return ssh_probe.VerboseProbe(
            255,
            (),
            "PRIVATE-VERBOSE-SSH-OUTPUT",
            timed_out=True,
        )

    monkeypatch.setattr(ssh_probe, "verbose_probe", stalled_verbose)
    result = doctor.ssh_transport_check(
        config.HostConfig(ssh="remote"),
        _direct_route(),
        None,
        check_timeout=0.15,
    )

    assert seen == [pytest.approx(0.15)]
    assert result["timed_out"] is True
    assert result["subprobe"] == "ssh_transport"
    assert result["elapsed_seconds"] == pytest.approx(0.15)
    assert "PRIVATE-VERBOSE-SSH-OUTPUT" not in str(result)


def test_verbose_ssh_preprobe_budget_exhaustion_is_structured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _SequenceClock(100.0, 100.01, 100.01)
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        ssh_probe,
        "verbose_probe",
        lambda *_a, **_k: pytest.fail("expired verbose probe must not start"),
    )

    result = doctor.ssh_transport_check(
        config.HostConfig(ssh="remote"),
        _direct_route(),
        None,
        check_timeout=0.001,
    )

    assert result["ok"] is False
    assert result["timed_out"] is True
    assert result["subprobe"] == "ssh_transport"


@pytest.mark.parametrize(
    ("stalled_kind", "expected_subprobe"),
    [
        ("source_sha", "driver_source_sha"),
        ("source_tree_sha256", "driver_source_tree_sha256"),
    ],
)
def test_local_driver_identity_probe_is_owned_bounded_and_attributed(
    monkeypatch: pytest.MonkeyPatch,
    stalled_kind: str,
    expected_subprobe: str,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    owned_calls: list[str] = []

    def owned_process(argv, **kwargs):
        kind = argv[-1]
        owned_calls.append(kind)
        timeout = float(kwargs["timeout"])
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        if kind == stalled_kind:
            clock.advance(timeout)
            raise subprocess.TimeoutExpired(
                argv,
                timeout,
                output="PRIVATE-IDENTITY-STDOUT",
                stderr="PRIVATE-IDENTITY-STDERR",
            )
        clock.advance(0.05)
        return subprocess.CompletedProcess(argv, 0, f"{'a' * 40}\n", "")

    monkeypatch.setattr(transport, "run_owned_subprocess", owned_process)
    remote_calls: list[tuple[str, ...]] = []

    def remote_vq(
        _host_cfg: config.HostConfig,
        *args: str,
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        remote_calls.append(args)
        return _completed(args, "vq, version 0.17.0\n")

    monkeypatch.setattr(transport, "run_remote_vq", remote_vq)
    result = doctor.scheduler_remote_vq_check(
        config.HostConfig(ssh="scheduler-login"),
        host_label="host_f",
        check_timeout=0.3,
    )

    expected_owned = (
        ["source_sha"]
        if stalled_kind == "source_sha"
        else ["source_sha", "source_tree_sha256"]
    )
    assert owned_calls == expected_owned
    assert remote_calls == [("--version",)]
    assert result["timed_out"] is True
    assert result["subprobe"] == expected_subprobe
    assert result["elapsed_seconds"] == pytest.approx(0.3)
    assert "PRIVATE-IDENTITY" not in str(result)


@pytest.mark.parametrize("marker_active", [True, False])
def test_deadline_contract_survives_active_and_just_cleared_admin_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    marker_active: bool,
) -> None:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_f:vibeqc-release"],
        host="host_f",
    )
    if not marker_active:
        admin.clear_admin_update_marker()

    cfg = config.Config(
        hosts={
            "localhost": config.HostConfig(ssh="localhost"),
            "host_f": config.HostConfig(
                ssh="scheduler-login",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch",
                scheduler_driver="localhost",
            ),
        }
    )
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        doctor,
        "_local_daemon_probe",
        lambda *_args, **_kwargs: (0, {"ok": True, "version": "0.test"}),
    )
    monkeypatch.setattr(
        scheduler_probe,
        "probe",
        lambda _runner: scheduler_probe.ProbeResult(
            dialect="torque",
            scheduler="pbs",
            version="2.5.12",
            confidence="confirmed",
            binaries={
                "qsub": True,
                "qstat": True,
                "qdel": True,
                "qhold": True,
                "qrls": True,
            },
            raw_version="version: 2.5.12",
            pbs_sched_running=True,
            queues=(
                scheduler_probe.QueueLiveness(
                    name="batch",
                    enabled=True,
                    started=False,
                ),
            ),
        ),
    )
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)

    def stalled_remote_vq(
        _host_cfg: config.HostConfig,
        *_args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        outer_timeout = float(kwargs["timeout"])
        clock.advance(outer_timeout)
        _timeout_with_private_output(outer_timeout)

    monkeypatch.setattr(transport, "run_remote_vq", stalled_remote_vq)
    payload = doctor.diagnose_host(
        cfg,
        "host_f",
        timeout=2.0,
        check_timeout=0.2,
    )

    checks = {item["name"]: item for item in payload["checks"]}
    assert checks["scheduler_remote_vq"]["timed_out"] is True
    assert checks["scheduler_remote_vq"]["elapsed_seconds"] == pytest.approx(0.2)
    dispatch = checks["scheduler_liveness"]["scheduler_dispatch"]
    assert dispatch["admin_update_marker_active"] is marker_active
    assert "PRIVATE-RAW-REMOTE-OUTPUT" not in str(payload)


@pytest.mark.parametrize(
    ("stalled_subprobe", "expected_calls"),
    [
        ("helper_version", [("--version",)]),
        (
            "helper_source_tree_sha256",
            [("--version",), ("source-tree-sha256",)],
        ),
        (
            "helper_source_sha",
            [
                ("--version",),
                ("source-tree-sha256",),
                ("source-sha",),
            ],
        ),
    ],
)
def test_scheduler_helper_subprobes_share_one_deadline_and_stop_dependents(
    monkeypatch: pytest.MonkeyPatch,
    stalled_subprobe: str,
    expected_calls: list[tuple[str, ...]],
) -> None:
    clock = _Clock()
    monkeypatch.setattr(doctor.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(
        doctor,
        "_driver_identity_probe",
        lambda kind, _deadline: (
            ("a" * 40, None)
            if kind == "source_sha"
            else ("12" * 32, None)
        ),
    )
    calls: list[tuple[str, ...]] = []
    timeouts: list[float] = []
    names = {
        ("--version",): "helper_version",
        ("source-tree-sha256",): "helper_source_tree_sha256",
        ("source-sha",): "helper_source_sha",
    }

    def remote_vq(
        _host_cfg: config.HostConfig,
        *args: str,
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        assert kwargs["owned_process_group"] is True
        outer_timeout = float(kwargs["timeout"])
        timeouts.append(outer_timeout)
        if names[args] == stalled_subprobe:
            clock.advance(outer_timeout)
            _timeout_with_private_output(outer_timeout)
        clock.advance(0.05)
        if args == ("--version",):
            return _completed(args, "vq, version 0.17.0\n")
        if args == ("source-tree-sha256",):
            return _completed(args, f"{'12' * 32}\n")
        return _completed(args, f"{'a' * 40}\n")

    monkeypatch.setattr(transport, "run_remote_vq", remote_vq)
    result = doctor.scheduler_remote_vq_check(
        config.HostConfig(ssh="scheduler-login"),
        host_label="host_f",
        check_timeout=0.3,
    )

    assert calls == expected_calls
    assert timeouts[0] == pytest.approx(0.3)
    assert timeouts == sorted(timeouts, reverse=True)
    assert result["ok"] is False
    assert result["timed_out"] is True
    assert result["subprobe"] == stalled_subprobe
    assert result["elapsed_seconds"] == pytest.approx(0.3)
    assert "PRIVATE-RAW-REMOTE-OUTPUT" not in str(result)
