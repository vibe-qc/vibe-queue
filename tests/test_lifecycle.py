"""Tests for vq.lifecycle.verify_user_systemd_contract (v0.5.49).

The function cross-checks four real-system sources (loginctl, pgrep,
systemctl --user, the daemon pidfile). All four are mocked in tests
so the suite is portable across macOS / Linux / CI containers.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, lifecycle, paths
from vq.cli import main

# ----------------------------------------------------------------------
# Subprocess-mocking fixture
# ----------------------------------------------------------------------


def _ok(stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout=stdout, stderr=stderr,
    )


def _fail(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr=stderr,
    )


def _stub_subprocess_for(
    *,
    loginctl_state: str | None = "active",
    loginctl_runtime: str | None = "/run/user/1000",
    systemd_user_pid: int | None = 1000,
    systemctl_reachable: bool = True,
    daemon_active_state: str | None = "active",
    daemon_main_pid: int | None = 12345,
):
    """Build a subprocess.run side_effect that returns the configured
    values from each probe. Order doesn't matter — the mock dispatches
    by argv shape, not call order, so callers can compose freely."""

    def _dispatch(argv, *args, **kwargs):
        # pgrep -u UID -f 'systemd --user'
        if argv[0] == "pgrep":
            if systemd_user_pid is None:
                return _fail(1)
            return _ok(stdout=f"{systemd_user_pid}\n")
        # loginctl show-user UID -p State -p RuntimePath
        if argv[0] == "loginctl":
            if loginctl_state is None and loginctl_runtime is None:
                return _fail(1)
            out = ""
            if loginctl_state is not None:
                out += f"State={loginctl_state}\n"
            if loginctl_runtime is not None:
                out += f"RuntimePath={loginctl_runtime}\n"
            return _ok(stdout=out)
        # systemctl --user show <unit> -p <prop> --value
        if argv[0] == "systemctl" and argv[1] == "--user":
            if not systemctl_reachable:
                return _fail(1, stderr="Transport endpoint is not connected\n")
            # argv = ["systemctl","--user","show",unit,"-p",prop,"--value"]
            prop = argv[5]
            if prop == "ActiveState":
                return _ok(stdout=f"{daemon_active_state or ''}\n")
            if prop == "MainPID":
                return _ok(stdout=f"{daemon_main_pid or 0}\n")
            return _ok(stdout="\n")
        # Anything else - default ok
        return _ok()

    return _dispatch


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


# ----------------------------------------------------------------------
# verify_user_systemd_contract — direct module tests
# ----------------------------------------------------------------------


class TestVerifyContractHealthy:
    """All four sources agree + daemon process is alive → ok=True."""

    def test_all_green(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(systemd_user_pid=1000, daemon_main_pid=2000),
        )
        # Both candidate PIDs are alive in any test runner: use the
        # current PID for both (we mocked the values but pid-alive
        # uses real os.kill).
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is True
        assert v.manager_pid == 1000
        assert v.vq_daemon_main_pid == 2000
        assert v.daemon_process_alive is True
        assert any("OK: loginctl State=active" in f for f in v.findings)
        assert any("OK: `systemd --user` running" in f for f in v.findings)


class TestVerifyContractNoSystemdUser:
    """The 2026-05-17-pre-reboot symptom: no `systemd --user` process
    in the table at all. Must be a FAIL verdict."""

    def test_pgrep_returns_nothing(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(
                systemd_user_pid=None,
                systemctl_reachable=False,
                daemon_main_pid=None,
            ),
        )
        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is False
        assert v.manager_pid is None
        assert any(
            "FAIL: no `systemd --user` process" in f
            for f in v.findings
        )


class TestVerifyContractOrphanSystemdUser:
    """The 2026-05-17-orig symptom: pgrep returns a PID but systemctl
    --user can't reach it (orphan from its service unit)."""

    def test_systemctl_unreachable_with_live_pgrep(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(
                systemd_user_pid=121866,
                systemctl_reachable=False,
                daemon_main_pid=None,
            ),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is False
        assert v.manager_pid == 121866
        assert v.systemctl_user_reachable is False
        assert any("FAIL: `systemctl --user` cannot reach" in f for f in v.findings)


class TestVerifyContractPidfileMismatch:
    """daemon.pid and systemd MainPID disagree → FAIL (two competing
    identity sources)."""

    def test_pidfile_disagrees_with_systemd(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Write a pidfile with a different pid than systemd reports.
        from vq import daemon_control
        daemon_control.write_pidfile()  # writes os.getpid()
        # Override what's IN the pidfile so the test value differs
        # from daemon_main_pid below.
        paths.daemon_pidfile().write_text("99999\n")

        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(daemon_main_pid=12345),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is False
        assert v.daemon_pidfile_pid == 99999
        assert v.vq_daemon_main_pid == 12345
        assert any(
            "FAIL: daemon pidfile pid=99999 but systemd MainPID=12345" in f
            for f in v.findings
        )


class TestVerifyContractDaemonProcessGone:
    """systemd reports MainPID=X but kill(X, 0) says X doesn't
    exist → FAIL (the today's-incident class)."""

    def test_recorded_pid_not_alive(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(daemon_main_pid=12345),
        )
        # pid_alive returns True for the systemd-user manager PID (1000
        # in our stub) but False for the daemon's 12345.
        def _pid_alive(pid):
            if pid is None:
                return None
            return pid != 12345

        monkeypatch.setattr(lifecycle, "_pid_alive", _pid_alive)
        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is False
        assert v.vq_daemon_main_pid == 12345
        assert v.daemon_process_alive is False
        assert any(
            "FAIL: daemon pid=12345 recorded but the process is gone" in f
            for f in v.findings
        )


class TestVerifyContractLoginctlMissing:
    """No loginctl in PATH (some minimal containers) → WARN, not
    FAIL. The verdict can still be OK if the other three sources
    agree."""

    def test_loginctl_unavailable_does_not_fail(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(
                loginctl_state=None,
                loginctl_runtime=None,
                systemd_user_pid=1000,
                daemon_main_pid=2000,
            ),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        v = lifecycle.verify_user_systemd_contract()
        # WARN doesn't fail the verdict.
        assert v.ok is True
        assert any("WARN: loginctl unavailable" in f for f in v.findings)


class TestVerifyContractRpcFallback:
    """macOS / non-systemd hosts: when NONE of the three systemd-user
    signals is present (no loginctl, no `systemd --user` in the table,
    systemctl unreachable), the four-source contract can't apply. The
    verdict falls back to RPC-socket liveness — the signal that works off
    Linux — so a healthy manually-started daemon (`vq daemon run`) reports
    ok=True instead of the false-negative `daemon: FAIL (pidfile)` the
    systemd probes produced. This is the host_c2 symptom.
    """

    def _all_systemd_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Exactly what every probe reports on macOS. Forced via the
        # subprocess stub so the test is portable to Linux CI too.
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(
                loginctl_state=None,
                loginctl_runtime=None,
                systemd_user_pid=None,
                systemctl_reachable=False,
                daemon_main_pid=None,
            ),
        )

    def test_responsive_daemon_reports_ok(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import daemon_control
        daemon_control.write_pidfile()  # writes os.getpid() — a live pid
        self._all_systemd_absent(monkeypatch)
        # Daemon answers ping → responsive.
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})

        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is True
        # systemd-specific fields are cleanly None (no systemd here) ...
        assert v.vq_daemon_main_pid is None
        assert v.systemctl_user_reachable is False
        # ... but the pidfile pid is surfaced so the overview can render
        # `daemon: OK ... (pidfile)`.
        assert v.daemon_pidfile_pid == os.getpid()
        assert v.daemon_process_alive is True
        assert any("RPC socket responsive" in f for f in v.findings)
        # No misleading systemd FAILs leak into the macOS verdict.
        assert not any(f.startswith("FAIL") for f in v.findings)

    def test_unresponsive_daemon_reports_fail(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._all_systemd_absent(monkeypatch)
        # Socket missing / daemon down → ping returns None.
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: None)

        v = lifecycle.verify_user_systemd_contract()
        assert v.ok is False
        assert any(
            f.startswith("FAIL") and "RPC socket" in f for f in v.findings
        )

    def test_fallback_verdict_surface_is_complete(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The fall-back verdict carries the same fields the systemd path
        does (drain + memory pressure), so JSON consumers and
        `vq daemon health` see a stable schema regardless of platform."""
        self._all_systemd_absent(monkeypatch)
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})

        v = lifecycle.verify_user_systemd_contract()
        assert v.drain_active is False
        assert v.drain_reason is None
        payload = json.loads(lifecycle.format_contract_verdict_json(v))
        for key in (
            "ok", "memory_pressure_pct", "drain_active", "drain_reason",
            "daemon_pidfile_pid", "findings",
        ):
            assert key in payload

    def test_active_drain_surfaces_in_fallback(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An operator-set drain must still surface in the RPC-fallback
        verdict (drain.json is platform-independent)."""
        from vq import drain as _drain
        self._all_systemd_absent(monkeypatch)
        monkeypatch.setattr("vq.rpc.ping", lambda **kw: {"version": "9.9.9"})
        fake = _drain.DrainState(
            enabled=True, max_jobs=None, max_cpus=None, reason="interactive work",
        )
        monkeypatch.setattr("vq.drain.read_drain_state", lambda **kw: fake)

        v = lifecycle.verify_user_systemd_contract()
        assert v.drain_active is True
        assert v.drain_reason == "interactive work"


# ----------------------------------------------------------------------
# format_contract_verdict_json
# ----------------------------------------------------------------------


class TestFormatVerdictJson:
    def test_emits_valid_json_with_all_fields(self) -> None:
        v = lifecycle.ContractVerdict(
            ok=True,
            manager_pid=1000,
            loginctl_state="active",
            loginctl_runtime_path="/run/user/1000",
            systemctl_user_reachable=True,
            vq_daemon_state="active",
            vq_daemon_main_pid=2000,
            daemon_pidfile_pid=None,
            daemon_process_alive=True,
            findings=["OK: example"],
        )
        out = json.loads(lifecycle.format_contract_verdict_json(v))
        # Stable schema: every documented field present.
        for key in (
            "ok", "manager_pid", "loginctl_state",
            "loginctl_runtime_path", "systemctl_user_reachable",
            "vq_daemon_state", "vq_daemon_main_pid",
            "daemon_pidfile_pid", "daemon_process_alive", "findings",
        ):
            assert key in out
        assert out["ok"] is True
        assert out["findings"] == ["OK: example"]


# ----------------------------------------------------------------------
# vq daemon health CLI
# ----------------------------------------------------------------------


class TestDaemonHealthCLI:
    def test_local_text_output(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(systemd_user_pid=1000, daemon_main_pid=2000),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        result = CliRunner().invoke(main, ["daemon", "health"])
        assert result.exit_code == 0, result.output
        assert "== daemon lifecycle health ==" in result.output
        assert "verdict: OK" in result.output
        assert "OK: `systemd --user` running" in result.output

    def test_local_json_output(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(systemd_user_pid=1000, daemon_main_pid=2000),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )
        result = CliRunner().invoke(
            main, ["daemon", "health", "--json"],
        )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.output)
        assert parsed["ok"] is True
        assert parsed["manager_pid"] == 1000

    def test_scheduler_host_json_wraps_driver_health(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
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
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(systemd_user_pid=1000, daemon_main_pid=2000),
        )
        monkeypatch.setattr(
            lifecycle, "_pid_alive",
            lambda pid: True if pid is not None else None,
        )

        result = CliRunner().invoke(main, ["daemon", "health", "host_f", "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["ok"] is True
        assert payload["scheduler_host"] is True
        assert payload["scheduler"] == "pbs"
        assert payload["driver"] == "localhost"
        assert payload["driver_health"]["ok"] is True
        assert payload["driver_health"]["manager_pid"] == 1000

    def test_local_failed_verdict_exits_nonzero(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-OK verdict on local mode must exit non-zero so scripts
        can react to a bad health check without parsing the text."""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        monkeypatch.setattr(
            lifecycle.subprocess, "run",
            _stub_subprocess_for(
                systemd_user_pid=None,
                systemctl_reachable=False,
                daemon_main_pid=None,
            ),
        )
        result = CliRunner().invoke(main, ["daemon", "health"])
        assert result.exit_code != 0
        assert "verdict: FAILED" in result.output

    def test_help_lists_sources_cross_checked(self) -> None:
        result = CliRunner().invoke(main, ["daemon", "health", "--help"])
        assert result.exit_code == 0
        assert "loginctl" in result.output
        assert "pgrep" in result.output
        assert "systemctl" in result.output

    def test_all_and_host_mutually_exclusive(
        self, state_dir: Path
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        result = CliRunner().invoke(
            main, ["daemon", "health", "localhost", "--all"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output


class TestMultiUserDaemonIsNotJudgedByUserSystemd:
    """A system-wide daemon must not be failed by a user-systemd contract.

    On a multi-user host the vq daemon is a SYSTEM service, which systemd-user
    does not supervise. Judging it by the four-source user contract measures a
    different manager entirely and reports FAIL for a daemon serving normally.
    host_d and host_a did exactly that on 2026-07-27: `vq overview` showed
    `daemon: FAIL` while `vq doctor` was 5/5 on both and RPC answered. The false
    FAIL invited a repair that was never needed.

    Same class as the macOS false negative the RPC fallback was written for. It
    was never wired for multi-user because a multi-user Linux box *does* have a
    user manager -- one that is irrelevant to this daemon.
    """

    def test_multi_user_uses_rpc_liveness_not_the_user_contract(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
        monkeypatch.setattr(
            lifecycle,
            "_rpc_ping_liveness",
            lambda *, multi_user=False: (True, "0.21.0", "/var/lib/vq/vq.sock"),
        )
        # A user manager exists but supervises nothing relevant; under the old
        # behaviour these signals routed to the user contract and failed.
        monkeypatch.setattr(lifecycle, "_pgrep_systemd_user", lambda: 4242)
        monkeypatch.setattr(lifecycle, "_systemctl_user_reachable", lambda: False)
        monkeypatch.setattr(
            lifecycle, "_loginctl_user_state", lambda: ("active", "/run/user/1000")
        )

        verdict = lifecycle.verify_user_systemd_contract()

        assert verdict.ok is True, (
            "a healthy multi-user daemon was failed by the user-systemd contract"
        )
        assert any("multi-user daemon" in f for f in verdict.findings)
        assert not any(f.startswith("FAIL") for f in verdict.findings)

    def test_multi_user_still_fails_when_rpc_is_dead(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Routing around the user contract must not make the verdict
        toothless: a system daemon that is not serving is a real failure."""
        monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
        monkeypatch.setattr(
            lifecycle,
            "_rpc_ping_liveness",
            lambda *, multi_user=False: (False, None, "/var/lib/vq/vq.sock"),
        )

        assert lifecycle.verify_user_systemd_contract().ok is False

    def test_multi_user_pings_the_multi_user_socket(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """It must probe the system daemon's socket, not the per-user one --
        otherwise it would report on a daemon that is not the one running."""
        seen: list[bool] = []

        def spy(*, multi_user=False):  # type: ignore[no-untyped-def]
            seen.append(multi_user)
            return (True, "0.21.0", "/var/lib/vq/vq.sock")

        monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
        monkeypatch.setattr(lifecycle, "_rpc_ping_liveness", spy)

        lifecycle.verify_user_systemd_contract()

        assert seen == [True]

    def test_single_user_host_still_uses_the_user_contract(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The ordinary single-user Linux path is untouched: an unreachable
        user manager there is still a genuine fault."""
        monkeypatch.setattr(config, "system_multi_user_enabled", lambda: False)
        monkeypatch.setattr(lifecycle, "_pgrep_systemd_user", lambda: 4242)
        monkeypatch.setattr(lifecycle, "_systemctl_user_reachable", lambda: False)
        monkeypatch.setattr(
            lifecycle, "_loginctl_user_state", lambda: ("active", "/run/user/1000")
        )

        verdict = lifecycle.verify_user_systemd_contract()

        assert verdict.ok is False
        assert any("systemctl --user" in f for f in verdict.findings)
