"""v0.7.5 *Hopper's Compiler* — tests for the host-recovery audit.

Pins the 3-tier audit contract: BMC (informational), Cockpit on
:9090 (TCP probe), Recovery sshd on :22222 (real auth probe).

The probes live in :mod:`vq.recovery_audit` and are independent
of vq's normal SSH transport — these tests mock the network
calls so the suite runs offline.
"""
from __future__ import annotations

import socket
import subprocess
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from vq import config, recovery_audit


def _make_host_cfg(
    *,
    bmc_url: str | None = None,
    ssh_port: int = 22222,
    cockpit_port: int = 9090,
) -> config.HostConfig:
    rec = config.RecoveryConfig(
        bmc_url=bmc_url,
        ssh_port=ssh_port,
        cockpit_port=cockpit_port,
    )
    return config.HostConfig(ssh="testhost", recovery=rec)


def _proc(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr=stderr,
    )


# ----------------------------------------------------------------------
# Tier 1 — BMC (informational only)
# ----------------------------------------------------------------------


class TestTier1Bmc:
    def test_no_bmc_url_skipped(self) -> None:
        host_cfg = _make_host_cfg(bmc_url=None)
        result = recovery_audit._probe_bmc(host_cfg.recovery)
        assert result.tier == 1
        assert result.status == "skipped"
        assert "no bmc_url configured" in result.detail

    def test_configured_bmc_marked_green(self) -> None:
        host_cfg = _make_host_cfg(bmc_url="https://host-a-bmc.example.org/")
        result = recovery_audit._probe_bmc(host_cfg.recovery)
        assert result.tier == 1
        assert result.status == "green"
        assert "host-a-bmc.example.org" in result.detail


# ----------------------------------------------------------------------
# Tier 2 — Cockpit (TCP probe)
# ----------------------------------------------------------------------


class TestTier2Cockpit:
    def test_port_reachable_is_green(self) -> None:
        with patch(
            "vq.recovery_audit.socket.create_connection",
            return_value=socket.socket(),
        ):
            result = recovery_audit._probe_cockpit("testhost", 9090)
        assert result.tier == 2
        assert result.status == "green"
        assert "port 9090 reachable" in result.detail

    def test_connection_refused_is_red(self) -> None:
        with patch(
            "vq.recovery_audit.socket.create_connection",
            side_effect=ConnectionRefusedError(),
        ):
            result = recovery_audit._probe_cockpit("testhost", 9090)
        assert result.tier == 2
        assert result.status == "red"
        assert "connection refused" in result.detail

    def test_timeout_is_red(self) -> None:
        with patch(
            "vq.recovery_audit.socket.create_connection",
            side_effect=TimeoutError(),
        ):
            result = recovery_audit._probe_cockpit("testhost", 9090)
        assert result.tier == 2
        assert result.status == "red"
        assert "timed out" in result.detail


# ----------------------------------------------------------------------
# Tier 3 — Recovery SSH (real auth attempt)
# ----------------------------------------------------------------------


class TestTier3RecoverySsh:
    def test_missing_recovery_key_is_red(
        self, tmp_path, monkeypatch,
    ) -> None:
        # Point ssh_key_path at a non-existent file
        rec = config.RecoveryConfig(
            ssh_key_path=str(tmp_path / "does-not-exist"),
        )
        result = recovery_audit._probe_recovery_ssh("testhost", rec)
        assert result.tier == 3
        assert result.status == "red"
        assert "recovery key not found" in result.detail

    def test_successful_auth_is_green(self, tmp_path) -> None:
        # Create a real file at the key path so the existence check
        # passes; mock the ssh subprocess.
        key = tmp_path / "id_recovery"
        key.write_text("dummy key file")
        rec = config.RecoveryConfig(ssh_key_path=str(key))
        with patch(
            "vq.recovery_audit.subprocess.run",
            return_value=_proc(0),
        ):
            result = recovery_audit._probe_recovery_ssh("testhost", rec)
        assert result.tier == 3
        assert result.status == "green"
        assert "recovery key accepted" in result.detail

    def test_connection_refused_is_red(self, tmp_path) -> None:
        key = tmp_path / "id_recovery"
        key.write_text("dummy")
        rec = config.RecoveryConfig(ssh_key_path=str(key))
        with patch(
            "vq.recovery_audit.subprocess.run",
            return_value=_proc(
                255, stderr="ssh: connect to host testhost port 22222: Connection refused",
            ),
        ):
            result = recovery_audit._probe_recovery_ssh("testhost", rec)
        assert result.tier == 3
        assert result.status == "red"
        assert "recovery sshd not configured" in result.detail

    def test_permission_denied_is_red(self, tmp_path) -> None:
        """Recovery port is open, but the recovery key isn't in
        the server's recovery_authorized_keys."""
        key = tmp_path / "id_recovery"
        key.write_text("dummy")
        rec = config.RecoveryConfig(ssh_key_path=str(key))
        with patch(
            "vq.recovery_audit.subprocess.run",
            return_value=_proc(255, stderr="Permission denied (publickey)."),
        ):
            result = recovery_audit._probe_recovery_ssh("testhost", rec)
        assert result.tier == 3
        assert result.status == "red"
        assert "key rejected" in result.detail


# ----------------------------------------------------------------------
# audit_host — composes all three tiers + overall verdict
# ----------------------------------------------------------------------


class TestAuditHost:
    def _patches(
        self, tmp_path, *,
        cockpit_status: str = "green",
        recovery_status: str = "green",
        bmc_url: str | None = None,
    ):
        """Build a fully-mocked audit_host environment via three
        TierResult stubs."""
        key = tmp_path / "id_recovery"
        key.write_text("dummy")
        host_cfg = _make_host_cfg(bmc_url=bmc_url)
        host_cfg.recovery = config.RecoveryConfig(
            ssh_key_path=str(key), bmc_url=bmc_url,
        )

        def stub_cockpit(host, port):
            return recovery_audit.TierResult(
                tier=2, name="Cockpit",
                status=cockpit_status,  # type: ignore[arg-type]
                detail=f"stub: {cockpit_status}",
            )

        def stub_recovery(alias, rec):
            return recovery_audit.TierResult(
                tier=3, name="Recovery SSH",
                status=recovery_status,  # type: ignore[arg-type]
                detail=f"stub: {recovery_status}",
            )

        return host_cfg, stub_cockpit, stub_recovery

    def test_both_green_yields_green(self, tmp_path) -> None:
        host_cfg, stub_c, stub_r = self._patches(tmp_path)
        with patch("vq.recovery_audit._probe_cockpit", side_effect=stub_c), \
             patch("vq.recovery_audit._probe_recovery_ssh", side_effect=stub_r), \
             patch("vq.recovery_audit._resolve_host_address", return_value="testhost"):
            report = recovery_audit.audit_host("testhost", host_cfg)
        assert report.overall == "green"
        assert len(report.tiers) == 3

    def test_one_green_yields_yellow(self, tmp_path) -> None:
        host_cfg, stub_c, stub_r = self._patches(
            tmp_path, cockpit_status="green", recovery_status="red",
        )
        with patch("vq.recovery_audit._probe_cockpit", side_effect=stub_c), \
             patch("vq.recovery_audit._probe_recovery_ssh", side_effect=stub_r), \
             patch("vq.recovery_audit._resolve_host_address", return_value="testhost"):
            report = recovery_audit.audit_host("testhost", host_cfg)
        assert report.overall == "yellow"

    def test_no_green_yields_red(self, tmp_path) -> None:
        host_cfg, stub_c, stub_r = self._patches(
            tmp_path, cockpit_status="red", recovery_status="red",
        )
        with patch("vq.recovery_audit._probe_cockpit", side_effect=stub_c), \
             patch("vq.recovery_audit._probe_recovery_ssh", side_effect=stub_r), \
             patch("vq.recovery_audit._resolve_host_address", return_value="testhost"):
            report = recovery_audit.audit_host("testhost", host_cfg)
        assert report.overall == "red"

    def test_bmc_skipped_doesnt_block_green(self, tmp_path) -> None:
        """The BMC tier being 'skipped' (informational only) must
        NOT prevent overall=green when tiers 2+3 are both green."""
        host_cfg, stub_c, stub_r = self._patches(tmp_path, bmc_url=None)
        with patch("vq.recovery_audit._probe_cockpit", side_effect=stub_c), \
             patch("vq.recovery_audit._probe_recovery_ssh", side_effect=stub_r), \
             patch("vq.recovery_audit._resolve_host_address", return_value="testhost"):
            report = recovery_audit.audit_host("testhost", host_cfg)
        assert report.overall == "green"
        # Tier 1 still appears in the tier list, just as skipped.
        tier1 = next(t for t in report.tiers if t.tier == 1)
        assert tier1.status == "skipped"


# ----------------------------------------------------------------------
# Text + JSON rendering
# ----------------------------------------------------------------------


class TestRendering:
    def _green_report(self) -> recovery_audit.AuditReport:
        return recovery_audit.AuditReport(
            host_name="host_a",
            tiers=[
                recovery_audit.TierResult(1, "BMC / IPMI", "green", "configured at https://host-a-bmc.example.org/"),
                recovery_audit.TierResult(2, "Cockpit", "green", "port 9090 reachable"),
                recovery_audit.TierResult(
                    3, "Recovery SSH", "green", "port 22222 reachable; recovery key accepted"
                ),
            ],
            overall="green",
        )

    def test_text_rendering(self) -> None:
        text = recovery_audit.format_audit_text(self._green_report())
        assert "==== host_a ====" in text
        assert "Tier 1" in text and "Tier 2" in text and "Tier 3" in text
        assert "Overall: GREEN" in text

    def test_red_text_includes_action(self) -> None:
        red = recovery_audit.AuditReport(
            host_name="host_d",
            tiers=[
                recovery_audit.TierResult(1, "BMC / IPMI", "skipped", "no bmc_url"),
                recovery_audit.TierResult(2, "Cockpit", "red", "port closed"),
                recovery_audit.TierResult(3, "Recovery SSH", "red", "port closed"),
            ],
            overall="red",
        )
        text = recovery_audit.format_audit_text(red)
        assert "Overall: RED" in text
        assert "setup-recovery-channels.sh" in text

    def test_json_rendering(self) -> None:
        payload = recovery_audit.format_audit_json(self._green_report())
        assert payload["host_name"] == "host_a"
        assert payload["overall"] == "green"
        assert len(payload["tiers"]) == 3
        for t in payload["tiers"]:
            assert set(t.keys()) == {"tier", "name", "status", "detail"}


# ----------------------------------------------------------------------
# RecoveryConfig defaults / round-trip
# ----------------------------------------------------------------------


class TestRecoveryConfig:
    def test_defaults(self) -> None:
        rec = config.RecoveryConfig()
        assert rec.ssh_port == 22222
        assert rec.cockpit_port == 9090
        assert rec.ssh_key_path == "~/.ssh/id_ed25519_vibeqc-recovery"
        assert rec.bmc_url is None

    def test_host_cfg_default_recovery(self) -> None:
        host_cfg = config.HostConfig(ssh="testhost")
        assert host_cfg.recovery.ssh_port == 22222
        assert host_cfg.recovery.bmc_url is None

    @pytest.mark.parametrize("field", ["ssh_port", "cockpit_port"])
    @pytest.mark.parametrize(
        "value", [-1, 0, 65536, True, 1.0, "22222"]
    )
    def test_invalid_ports_fail_before_host_configuration(
        self, field: str, value: object
    ) -> None:
        recovery = {field: value}
        with pytest.raises(ValidationError):
            config.RecoveryConfig(**recovery)
        with pytest.raises(ValidationError):
            config.HostConfig(ssh="testhost", recovery=recovery)

    @pytest.mark.parametrize("field", ["ssh_port", "cockpit_port"])
    @pytest.mark.parametrize("value", [1, 65535])
    def test_tcp_port_boundaries_are_valid(
        self, field: str, value: int
    ) -> None:
        recovery = config.RecoveryConfig(**{field: value})
        assert getattr(recovery, field) == value
