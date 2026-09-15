"""Tests for the local-side SSH diagnostics behind the ``vq doctor`` local leg.

Route resolution is driven through an injected runner, so the real ``ssh -G``
is never invoked and nothing depends on the developer's ``~/.ssh/config``. The
TCP probe is exercised against sockets this test opens itself: reachable
against a live listener, refused against a port it just closed. Hostnames use
the reserved ``.invalid`` TLD (RFC 2606) so a stray resolution attempt cannot
reach anything real.
"""
from __future__ import annotations

import errno
import socket
import subprocess
import sys
import time
from collections.abc import Sequence

import pytest

from vq import ssh_probe

pytestmark = pytest.mark.no_autopatch_ssh_probe


def _dump(**fields: str) -> str:
    return "".join(f"{key} {value}\n" for key, value in fields.items())


def _runner_for(configs: dict[str, str], *, missing_rc: int = 255):
    """Build a runner that answers ``ssh -G <alias>`` from a canned table."""
    calls: list[list[str]] = []

    def runner(argv: Sequence[str], timeout: float):
        calls.append(list(argv))
        alias = argv[-1]
        if alias not in configs:
            return subprocess.CompletedProcess(
                list(argv),
                missing_rc,
                "",
                f"ssh: Could not resolve hostname {alias}\n",
            )
        return subprocess.CompletedProcess(list(argv), 0, configs[alias], "")

    runner.calls = calls  # type: ignore[attr-defined]
    return runner


class TestResolveRoute:
    def test_direct_route_first_hop_is_the_target(self) -> None:
        runner = _runner_for(
            {
                "host_a": _dump(
                    user="test_user",
                    hostname="host_a.example.invalid",
                    port="49999",
                    identityfile="~/.ssh/host_a_ed25519",
                )
            }
        )

        route = ssh_probe.resolve_route("host_a", runner=runner)

        assert route.proxied is False
        assert route.first_hop == ssh_probe.Hop("target", "host_a.example.invalid", 49999)
        assert route.identity_files == ("~/.ssh/host_a_ed25519",)
        assert "direct" in route.describe()

    def test_proxy_jump_first_hop_is_the_jump_host(self) -> None:
        runner = _runner_for(
            {
                "host_f": _dump(
                    user="tuser",
                    hostname="host_f.example.invalid",
                    port="22",
                    proxyjump="gateway",
                ),
                "gateway": _dump(
                    user="guser",
                    hostname="gw.example.invalid",
                    port="2222",
                ),
            }
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.proxy_jump == "gateway"
        assert route.first_hop == ssh_probe.Hop(
            "jump host 'gateway'", "gw.example.invalid", 2222
        )
        # The bastion is what the description makes visible; that alone is the
        # fact the old doctor could not report.
        assert "ProxyJump 'gateway' -> gw.example.invalid:2222" in route.describe()

    def test_port_in_the_jump_spec_wins_over_the_jump_alias_config(self) -> None:
        runner = _runner_for(
            {
                "host_f": _dump(hostname="host_f.example.invalid", proxyjump="gateway:2022"),
                "gateway": _dump(hostname="gw.example.invalid", port="2222"),
            }
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.first_hop is not None
        assert route.first_hop.port == 2022

    def test_nested_jump_chain_walks_to_the_endpoint_dialed_first(self) -> None:
        runner = _runner_for(
            {
                "target": _dump(hostname="t.example.invalid", proxyjump="inner"),
                "inner": _dump(hostname="i.example.invalid", proxyjump="outer"),
                "outer": _dump(hostname="o.example.invalid", port="443"),
            }
        )

        route = ssh_probe.resolve_route("target", runner=runner)

        assert route.first_hop == ssh_probe.Hop(
            "jump host 'outer'", "o.example.invalid", 443
        )

    def test_user_at_host_jump_spec_strips_the_user(self) -> None:
        runner = _runner_for(
            {
                "host_f": _dump(hostname="t.example.invalid", proxyjump="guser@gateway"),
                "gateway": _dump(hostname="gw.example.invalid", port="22"),
            }
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.first_hop is not None
        assert route.first_hop.host == "gw.example.invalid"

    def test_bracketed_ipv6_jump_spec_keeps_the_address_intact(self) -> None:
        runner = _runner_for(
            {
                "host_f": _dump(
                    hostname="t.example.invalid", proxyjump="[2001:db8::1]:2222"
                ),
                "2001:db8::1": _dump(hostname="2001:db8::1", port="22"),
            }
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.first_hop == ssh_probe.Hop("jump host '2001:db8::1'", "2001:db8::1", 2222)

    def test_proxy_command_is_reported_as_unprobeable(self) -> None:
        runner = _runner_for(
            {
                "host_f": _dump(
                    hostname="t.example.invalid",
                    proxycommand="/usr/bin/env sh -c 'failover gw1 gw2'",
                )
            }
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.proxied is True
        assert route.first_hop is None
        assert "opaque" in route.resolve_note
        assert "ProxyCommand" in route.describe()

    def test_openssh_none_sentinel_is_not_a_proxy(self) -> None:
        runner = _runner_for(
            {"host_a": _dump(hostname="m.example.invalid", proxycommand="none")}
        )

        route = ssh_probe.resolve_route("host_a", runner=runner)

        assert route.proxied is False
        assert route.first_hop is not None

    def test_unresolvable_jump_alias_still_yields_a_route(self) -> None:
        runner = _runner_for(
            {"host_f": _dump(hostname="t.example.invalid", proxyjump="ghost")}
        )

        route = ssh_probe.resolve_route("host_f", runner=runner)

        assert route.first_hop is None
        assert "could not resolve jump alias 'ghost'" in route.resolve_note

    def test_jump_loop_is_bounded(self) -> None:
        runner = _runner_for(
            {
                "a": _dump(hostname="a.example.invalid", proxyjump="b"),
                "b": _dump(hostname="b.example.invalid", proxyjump="a"),
            }
        )

        route = ssh_probe.resolve_route("a", runner=runner)

        assert route.first_hop is None
        assert "loops" in route.resolve_note

    def test_failing_ssh_g_on_the_destination_raises(self) -> None:
        runner = _runner_for({})

        with pytest.raises(ssh_probe.SshProbeError) as excinfo:
            ssh_probe.resolve_route("nowhere", runner=runner)

        assert "Could not resolve hostname" in str(excinfo.value)

    def test_timeout_is_reported_not_propagated(self) -> None:
        def runner(argv: Sequence[str], timeout: float):
            raise subprocess.TimeoutExpired(list(argv), timeout)

        with pytest.raises(ssh_probe.SshProbeError) as excinfo:
            ssh_probe.resolve_route("host_a", runner=runner)

        assert "timed out" in str(excinfo.value)

    def test_nested_jump_resolution_shares_one_total_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = 100.0
        seen_timeouts: list[float] = []
        configs = {
            "target": _dump(hostname="t.example.invalid", proxyjump="inner"),
            "inner": _dump(hostname="i.example.invalid", proxyjump="outer"),
            "outer": _dump(hostname="o.example.invalid", port="22"),
        }

        def monotonic() -> float:
            return now

        def runner(argv: Sequence[str], timeout: float):
            nonlocal now
            seen_timeouts.append(timeout)
            if timeout < 0.06:
                now += timeout
                raise subprocess.TimeoutExpired(list(argv), timeout)
            now += 0.06
            return subprocess.CompletedProcess(
                list(argv), 0, configs[argv[-1]], ""
            )

        monkeypatch.setattr(ssh_probe.time, "monotonic", monotonic)

        with pytest.raises(ssh_probe.SshProbeTimeout):
            ssh_probe.resolve_route("target", runner=runner, timeout=0.1)

        assert seen_timeouts == [pytest.approx(0.1), pytest.approx(0.04)]


class TestProbeTcp:
    def test_stalled_dns_is_killed_at_owned_process_boundary(self) -> None:
        stalled_dns = (
            "import socket,time; "
            "socket.getaddrinfo=lambda *_a,**_k: time.sleep(30); "
            "socket.create_connection(('stalled.invalid',22),timeout=30)"
        )

        def runner(_argv: Sequence[str], timeout: float):
            return ssh_probe._default_runner(  # noqa: SLF001 - real boundary
                [sys.executable, "-c", stalled_dns],
                timeout,
            )

        started = time.monotonic()
        probe = ssh_probe.probe_tcp(
            "stalled.invalid",
            22,
            timeout=0.1,
            runner=runner,
        )
        elapsed = time.monotonic() - started

        assert elapsed < 2.0
        assert probe.outcome == "timeout"
        assert probe.elapsed_seconds >= 0.1
        assert "stalled.invalid" in probe.detail

    def test_live_listener_is_reachable(self) -> None:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            _, port = server.getsockname()

            probe = ssh_probe.probe_tcp("127.0.0.1", port, timeout=2.0)

        assert probe.outcome == "reachable"
        assert probe.reachable is True

    def test_closed_port_is_refused(self) -> None:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            _, port = server.getsockname()
        # The socket is closed on exit, so nothing is listening on that port.

        probe = ssh_probe.probe_tcp("127.0.0.1", port, timeout=2.0)

        assert probe.outcome == "refused"
        assert probe.reachable is False

    def test_unresolvable_name_is_a_dns_outcome(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Faked rather than resolved for real: a resolver that hijacks NXDOMAIN
        # would hand back an address and turn this into a timeout.
        def boom(*_args: object, **_kwargs: object) -> None:
            raise socket.gaierror(-2, "Name or service not known")

        monkeypatch.setattr(ssh_probe.socket, "create_connection", boom)

        probe = ssh_probe._probe_tcp_in_process(  # noqa: SLF001
            "no-such-host.invalid", 22, timeout=2.0
        )

        assert probe.outcome == "dns"
        assert "no-such-host.invalid" in probe.detail

    def test_no_route_to_host_is_an_unreachable_outcome(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(*_args: object, **_kwargs: object) -> None:
            raise OSError(errno.EHOSTUNREACH, "No route to host")

        monkeypatch.setattr(ssh_probe.socket, "create_connection", boom)

        probe = ssh_probe._probe_tcp_in_process(  # noqa: SLF001
            "gw.example.invalid", 22, timeout=2.0
        )

        assert probe.outcome == "unreachable"

    def test_slow_endpoint_is_a_timeout_outcome(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom(*_args: object, **_kwargs: object) -> None:
            raise TimeoutError

        monkeypatch.setattr(ssh_probe.socket, "create_connection", boom)

        probe = ssh_probe._probe_tcp_in_process(  # noqa: SLF001
            "gw.example.invalid", 22, timeout=2.0
        )

        assert probe.outcome == "timeout"


class TestControlMasterActive:
    def test_running_master_is_detected(self) -> None:
        captured: dict[str, object] = {}

        def runner(argv: Sequence[str], timeout: float):
            captured["argv"] = list(argv)
            return subprocess.CompletedProcess(
                list(argv), 0, "", "Master running (pid=33466)\n"
            )

        assert ssh_probe.control_master_active("host_f", runner=runner) is True
        assert captured["argv"] == ["ssh", "-O", "check", "host_f"]

    def test_missing_socket_is_not_a_master(self) -> None:
        def runner(argv: Sequence[str], timeout: float):
            return subprocess.CompletedProcess(
                list(argv),
                255,
                "",
                "Control socket connect(~/.ssh/sockets/host_f.sock): No such file\n",
            )

        assert ssh_probe.control_master_active("host_f", runner=runner) is False

    def test_no_controlpath_configured_is_not_a_master(self) -> None:
        def runner(argv: Sequence[str], timeout: float):
            return subprocess.CompletedProcess(
                list(argv), 255, "", 'No ControlPath specified for "-O" command\n'
            )

        assert ssh_probe.control_master_active("host_a", runner=runner) is False

    def test_a_wedged_check_is_not_a_master(self) -> None:
        def runner(argv: Sequence[str], timeout: float):
            raise subprocess.TimeoutExpired(list(argv), timeout)

        assert ssh_probe.control_master_active("host_f", runner=runner) is False


def _route(**overrides: object) -> ssh_probe.SshRoute:
    fields: dict[str, object] = {
        "destination": "host_f",
        "hostname": "t.example.invalid",
        "port": 22,
        "user": "tuser",
        "proxy_jump": None,
        "proxy_command": None,
        "identity_files": ("~/.ssh/host_f_ed25519",),
        "first_hop": ssh_probe.Hop("target", "t.example.invalid", 22),
    }
    fields.update(overrides)
    return ssh_probe.SshRoute(**fields)  # type: ignore[arg-type]


class TestClassify:
    def test_unreachable_first_hop_beats_ssh_blaming_the_target(self) -> None:
        # The 2026-07-22 shape: the gateway is down, so OpenSSH reports
        # "Connection closed by UNKNOWN port 65535" against the TARGET. The
        # probe knows better.
        route = _route(
            proxy_jump="gateway",
            first_hop=ssh_probe.Hop("jump host 'gateway'", "gw.example.invalid", 22),
        )
        probe = ssh_probe.TcpProbe(
            "gw.example.invalid", 22, "refused", "connection refused", 0.01
        )

        verdict = ssh_probe.classify(
            route, probe, "Connection closed by UNKNOWN port 65535"
        )

        assert verdict.kind == "first_hop_refused"
        assert "gw.example.invalid" in verdict.summary
        assert "never contacted" in verdict.next_step

    def test_direct_route_names_the_target_not_a_hop(self) -> None:
        probe = ssh_probe.TcpProbe(
            "t.example.invalid", 22, "timeout", "no answer", 3.0
        )

        verdict = ssh_probe.classify(_route(), probe, "")

        assert verdict.kind == "target_unreachable"

    def test_reachable_first_hop_defers_to_ssh_stderr_for_auth(self) -> None:
        probe = ssh_probe.TcpProbe("t.example.invalid", 22, "reachable", "ok", 0.01)

        verdict = ssh_probe.classify(
            _route(), probe, "tuser@t.example.invalid: Permission denied (publickey)."
        )

        assert verdict.kind == "auth_rejected"
        assert "~/.ssh/host_f_ed25519" in verdict.next_step

    def test_changed_host_key_is_not_treated_as_a_reachability_problem(self) -> None:
        verdict = ssh_probe.classify(
            _route(),
            None,
            "@@@ WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED! @@@",
        )

        assert verdict.kind == "host_key_changed"
        assert "out-of-band" in verdict.next_step

    def test_proxied_route_with_collapsed_banner_blames_the_chain(self) -> None:
        route = _route(proxy_command="failover gw1 gw2", first_hop=None)

        verdict = ssh_probe.classify(
            route, None, "kex_exchange_identification: Connection closed by remote host"
        )

        assert verdict.kind == "proxy_failed"
        assert "ssh -v host_f" in verdict.next_step

    def test_same_stderr_on_a_direct_route_blames_the_target_not_a_proxy(self) -> None:
        verdict = ssh_probe.classify(
            _route(), None, "kex_exchange_identification: Connection closed by remote host"
        )

        assert verdict.kind == "target_closed"
        assert "t.example.invalid:22" in verdict.next_step

    def test_successful_recheck_is_reported_as_transient(self) -> None:
        verdict = ssh_probe.classify(_route(), None, "", returncode=0)

        assert verdict.kind == "ok"

    def test_unrecognised_failure_points_at_a_manual_run(self) -> None:
        verdict = ssh_probe.classify(_route(), None, "something entirely new")

        assert verdict.kind == "unknown"
        assert "ssh -vv host_f" in verdict.next_step


class TestDiagnosticLines:
    def test_keeps_the_causal_debug_line_and_drops_the_noise(self) -> None:
        transcript = "\n".join(
            [
                "OpenSSH_10.2p1, LibreSSL 3.3.6",
                "debug1: Reading configuration data /home/user/.ssh/config",
                "debug1: Executing proxy command: exec failover gw1 gw2",
                "debug1: connect to address 192.0.2.14 port 22: Connection refused",
                "debug1: identity file /home/user/.ssh/host_f_ed25519 type 3",
                "ssh: connect to host host_f port 22: Connection closed by UNKNOWN port 65535",
            ]
        )

        lines = ssh_probe.diagnostic_lines(transcript)

        assert "connect to address 192.0.2.14 port 22: Connection refused" in lines
        assert not any("identity file" in line for line in lines)
        assert not any(line.startswith("OpenSSH_") for line in lines)

    def test_unprefixed_proxy_command_stderr_is_always_kept(self) -> None:
        # A ProxyCommand's own stderr carries no debug prefix and no marker,
        # and is frequently the only honest description of what broke.
        lines = ssh_probe.diagnostic_lines(
            "vq-gateway-failover: both gw1 and gw2 are down\n"
            "debug1: pledge: filesystem\n"
        )

        assert lines == ("vq-gateway-failover: both gw1 and gw2 are down",)

    def test_output_is_bounded(self) -> None:
        transcript = "\n".join(
            f"failure line {index}" for index in range(50)
        )

        assert len(ssh_probe.diagnostic_lines(transcript)) <= 12


class TestVerboseProbe:
    def test_runs_batchmode_ssh_and_extracts_diagnostics(self) -> None:
        captured: dict[str, object] = {}

        def runner(argv: Sequence[str], timeout: float):
            captured["argv"] = list(argv)
            return subprocess.CompletedProcess(
                list(argv),
                255,
                "",
                "debug1: connect to address 192.0.2.14 port 22: Connection refused\n",
            )

        result = ssh_probe.verbose_probe("host_f", runner=runner)

        assert result.returncode == 255
        assert result.diagnostics == (
            "connect to address 192.0.2.14 port 22: Connection refused",
        )
        argv = captured["argv"]
        assert isinstance(argv, list)
        assert argv[:2] == ["ssh", "-v"]
        assert "BatchMode=yes" in argv
        assert argv[-2:] == ["host_f", "true"]

    def test_timeout_is_surfaced_not_raised(self) -> None:
        def runner(argv: Sequence[str], timeout: float):
            raise subprocess.TimeoutExpired(list(argv), timeout)

        result = ssh_probe.verbose_probe("host_f", runner=runner, timeout=1.0)

        assert result.returncode == 255
        assert "did not return within 1s" in result.error
