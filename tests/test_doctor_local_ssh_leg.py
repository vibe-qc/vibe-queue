"""``vq doctor``'s local leg: route resolution + first-hop probe + verdict.

Before this leg existed, all four of "VPN down", "jump host down", "target
down", and "key rejected" produced the same doctor output, because every check
doctor had was remote-side and therefore already presupposed the SSH session
that was broken. These tests pin the distinctions.

The autouse ``_autopatch_ssh_probe`` stub in ``conftest.py`` provides the
healthy default (direct route, first hop reachable); each test here overrides
it with the failure shape it is about.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, paths, ssh_probe
from vq.cli import main


@pytest.fixture
def doctor_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    (tmp_path / "cfg" / "config.toml").write_text(
        '[hosts.host_f]\nssh = "host_f"\n', encoding="utf-8"
    )
    return tmp_path


def _install_route(
    monkeypatch: pytest.MonkeyPatch,
    route: ssh_probe.SshRoute,
    probe: ssh_probe.TcpProbe | None = None,
) -> None:
    monkeypatch.setattr(
        "vq.ssh_probe.resolve_route", lambda destination, **_kw: route
    )
    if probe is not None:
        monkeypatch.setattr(
            "vq.ssh_probe.probe_tcp", lambda host, port, **_kw: probe
        )


def _proxied_route() -> ssh_probe.SshRoute:
    return ssh_probe.SshRoute(
        destination="host_f",
        hostname="t.example.invalid",
        port=22,
        user="tuser",
        proxy_jump="gateway",
        proxy_command=None,
        identity_files=("~/.ssh/host_f_ed25519",),
        first_hop=ssh_probe.Hop("jump host 'gateway'", "gw.example.invalid", 22),
    )


def _checks(output: str) -> dict[str, dict[str, object]]:
    payload = json.loads(output)
    return {str(check["name"]): check for check in payload["checks"]}


def _explode(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("remote checks must not run once the first hop is dead")


class TestFirstHopShortCircuit:
    def test_dead_jump_host_is_named_and_stops_the_remote_checks(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe(
                "gw.example.invalid",
                22,
                "refused",
                "connection refused (host is up, nothing listening on this port)",
                0.01,
            ),
        )
        monkeypatch.setattr("vq.cli.transport.run_remote_vq", _explode)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        checks = _checks(result.output)
        assert checks["ssh_route"]["ok"] is True
        assert checks["ssh_first_hop"]["ok"] is False
        message = str(checks["ssh_first_hop"]["message"])
        assert "jump host 'gateway' gw.example.invalid:22" in message
        assert "never contacted" in message
        # The remote checks would only have reproduced the same failure,
        # slower and with less information.
        assert "remote_vq" not in checks
        assert "daemon_rpc" not in checks

    def test_unreachable_first_hop_points_at_local_connectivity(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe(
                "gw.example.invalid", 22, "timeout", "no answer within 3s", 3.0
            ),
        )
        monkeypatch.setattr("vq.cli.transport.run_remote_vq", _explode)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        message = str(_checks(result.output)["ssh_first_hop"]["message"])
        assert "unreachable" in message
        assert "link, VPN, routing" in message

    def test_route_description_makes_the_bastion_visible_when_healthy(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda host_cfg, *a, **kw: subprocess.CompletedProcess(
                ["vq"], 0, json.dumps({"ok": True, "version": "0.12.0"}), ""
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        checks = _checks(result.output)
        assert "ProxyJump 'gateway' -> gw.example.invalid:22" in str(
            checks["ssh_route"]["message"]
        )
        assert checks["ssh_first_hop"]["ok"] is True
        assert checks["daemon_rpc"]["ok"] is True

    def test_proxy_command_route_is_not_probed_but_is_reported(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            ssh_probe.SshRoute(
                destination="host_f",
                hostname="t.example.invalid",
                port=22,
                user="tuser",
                proxy_jump=None,
                proxy_command="failover gw1 gw2",
                identity_files=(),
                first_hop=None,
                resolve_note="ProxyCommand is an opaque local program",
            ),
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda host_cfg, *a, **kw: subprocess.CompletedProcess(
                ["vq"], 0, json.dumps({"ok": True, "version": "0.12.0"}), ""
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        checks = _checks(result.output)
        assert checks["ssh_first_hop"]["ok"] is True
        assert "not probed" in str(checks["ssh_first_hop"]["message"])
        # An opaque proxy must not block the remote checks; it only means vq
        # cannot pre-empt them.
        assert checks["daemon_rpc"]["ok"] is True

    def test_broken_ssh_config_fails_the_route_check(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(destination: str, **_kw: object) -> ssh_probe.SshRoute:
            raise ssh_probe.SshProbeError(
                "`ssh -G host_f` failed: /home/user/.ssh/config line 4: Bad configuration"
            )

        monkeypatch.setattr("vq.ssh_probe.resolve_route", boom)
        monkeypatch.setattr("vq.cli.transport.run_remote_vq", _explode)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        checks = _checks(result.output)
        assert checks["ssh_route"]["ok"] is False
        assert "Bad configuration" in str(checks["ssh_route"]["message"])


class TestMultiplexedConnections:
    """A live ControlMaster outranks the socket layer.

    The fleet's ``~/.ssh/config`` uses ``ControlMaster auto`` +
    ``ControlPersist``, so ssh can still reach a host over an established
    master after the route underneath it has died. Short-circuiting on the TCP
    probe alone would then report a host as unusable that vq is still using.
    """

    def test_live_master_keeps_the_remote_checks_running(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe(
                "gw.example.invalid", 22, "refused", "connection refused", 0.01
            ),
        )
        monkeypatch.setattr(
            "vq.ssh_probe.control_master_active", lambda destination, **_kw: True
        )
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda host_cfg, *a, **kw: subprocess.CompletedProcess(
                ["vq"], 0, json.dumps({"ok": True, "version": "0.12.0"}), ""
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        checks = _checks(result.output)
        # The dead first hop is still reported: the master will expire.
        assert checks["ssh_first_hop"]["ok"] is False
        assert "multiplexed" in str(checks["ssh_first_hop"]["message"])
        # But reachability now comes from what actually worked.
        assert checks["daemon_rpc"]["ok"] is True

    def test_no_master_still_short_circuits(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe(
                "gw.example.invalid", 22, "refused", "connection refused", 0.01
            ),
        )
        monkeypatch.setattr(
            "vq.ssh_probe.control_master_active", lambda destination, **_kw: False
        )
        monkeypatch.setattr("vq.cli.transport.run_remote_vq", _explode)

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        assert "remote_vq" not in _checks(result.output)


class TestTransportVerdict:
    def _fail_remote_with(self, monkeypatch: pytest.MonkeyPatch, stderr: str) -> None:
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda host_cfg, *a, **kw: subprocess.CompletedProcess(
                ["vq"], 255, "", stderr
            ),
        )

    def test_reachable_first_hop_plus_exit_255_gets_classified(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )
        self._fail_remote_with(
            monkeypatch, "Connection closed by UNKNOWN port 65535\n"
        )
        monkeypatch.setattr(
            "vq.ssh_probe.verbose_probe",
            lambda destination, **_kw: ssh_probe.VerboseProbe(
                255,
                (
                    "connect to address 192.0.2.14 port 22: Connection refused",
                    "Connection closed by UNKNOWN port 65535",
                ),
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        checks = _checks(result.output)
        message = str(checks["ssh_transport"]["message"])
        assert "the target was never contacted" in message
        assert "ssh -v host_f" in message
        # The transcript line that names the real culprit is surfaced, which is
        # the thing the raw error message hides.
        assert "Connection refused" in message

    def test_key_rejection_is_separated_from_reachability(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )
        self._fail_remote_with(monkeypatch, "Permission denied (publickey).\n")
        monkeypatch.setattr(
            "vq.ssh_probe.verbose_probe",
            lambda destination, **_kw: ssh_probe.VerboseProbe(
                255, ("tuser@t.example.invalid: Permission denied (publickey).",)
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        message = str(_checks(result.output)["ssh_transport"]["message"])
        assert "rejected the key" in message
        assert "network path is fine" in message
        assert "~/.ssh/host_f_ed25519" in message

    def test_transient_failure_that_clears_is_reported_as_such(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )
        self._fail_remote_with(monkeypatch, "Connection closed by remote host\n")
        monkeypatch.setattr(
            "vq.ssh_probe.verbose_probe",
            lambda destination, **_kw: ssh_probe.VerboseProbe(0, ()),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        checks = _checks(result.output)
        assert checks["ssh_transport"]["ok"] is True
        assert "transient" in str(checks["ssh_transport"]["message"])

    def test_no_verdict_is_added_when_the_remote_answered(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )
        # A real remote-side failure (the daemon is down), not a transport one.
        monkeypatch.setattr(
            "vq.cli.transport.run_remote_vq",
            lambda host_cfg, *a, **kw: subprocess.CompletedProcess(
                ["vq"], 0, json.dumps({"ok": False, "error": "daemon not running"}), ""
            ),
        )
        monkeypatch.setattr(
            "vq.ssh_probe.verbose_probe",
            lambda destination, **_kw: pytest.fail(
                "ssh -v must not run when ssh itself worked"
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        checks = _checks(result.output)
        assert "ssh_transport" not in checks
        assert checks["daemon_rpc"]["ok"] is False


class TestVerdictScoping:
    def test_a_drivers_transport_failure_is_not_blamed_on_the_scheduler_host(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # host_f's own ssh is fine; its remote DRIVER is what is unreachable. The
        # ssh -v verdict runs against host_f's alias, so attributing the driver's
        # failure to host_f would describe the wrong destination entirely.
        (doctor_state / "cfg" / "config.toml").write_text(
            "[hosts.host_f]\n"
            'ssh = "host_f"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/home/USER"\n'
            'scheduler_driver = "boxen"\n'
            "\n[hosts.boxen]\n"
            'ssh = "boxen"\n',
            encoding="utf-8",
        )
        _install_route(
            monkeypatch,
            _proxied_route(),
            ssh_probe.TcpProbe("gw.example.invalid", 22, "reachable", "ok", 0.01),
        )

        def remote(host_cfg, *_a: object, **_kw: object):
            if host_cfg.ssh == "boxen":
                return subprocess.CompletedProcess(
                    ["vq"], 255, "", "Connection closed by UNKNOWN port 65535"
                )
            return subprocess.CompletedProcess(["vq"], 0, "vq, version 0.12.0\n", "")

        monkeypatch.setattr("vq.cli.transport.run_remote_vq", remote)
        monkeypatch.setattr(
            "vq.ssh_probe.verbose_probe",
            lambda destination, **_kw: pytest.fail(
                "the ssh -v verdict must not run for another host's transport"
            ),
        )

        result = CliRunner().invoke(main, ["doctor", "host_f", "--json"])

        assert result.exit_code == 1
        assert "ssh_transport" not in _checks(result.output)


class TestLocalHostAndFleet:
    def test_localhost_has_no_local_ssh_leg(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "vq.doctor._local_daemon_probe",
            lambda timeout, **_kwargs: (
                0,
                {"ok": True, "version": "0.test"},
            ),
        )
        monkeypatch.setattr(
            "vq.ssh_probe.resolve_route",
            lambda destination, **_kw: pytest.fail("localhost needs no ssh route"),
        )

        result = CliRunner().invoke(main, ["doctor", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        assert "ssh_route" not in _checks(result.output)

    def test_shared_bastion_is_probed_once_for_the_whole_fleet(
        self, doctor_state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (doctor_state / "cfg" / "config.toml").write_text(
            '[hosts.host_f]\nssh = "host_f"\n\n[hosts.host_c]\nssh = "host_c"\n',
            encoding="utf-8",
        )
        _install_route(monkeypatch, _proxied_route())
        probes: list[tuple[str, int]] = []

        def counting_probe(host: str, port: int, **_kw: object) -> ssh_probe.TcpProbe:
            probes.append((host, port))
            return ssh_probe.TcpProbe(host, port, "refused", "refused", 0.01)

        monkeypatch.setattr("vq.ssh_probe.probe_tcp", counting_probe)
        monkeypatch.setattr("vq.cli.transport.run_remote_vq", _explode)

        result = CliRunner().invoke(main, ["doctor", "--all", "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert set(payload) == {"host_f", "host_c"}
        assert probes == [("gw.example.invalid", 22)]
