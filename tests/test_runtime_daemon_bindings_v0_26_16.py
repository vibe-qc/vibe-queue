"""#73 review: unbindable files are admission errors, and receipts stay on the host.

Two findings from the independent review of the immutable serving-daemon
transaction (``afcc052``, ``155f850``):

* A symlinked ``config.toml`` or user service file, a group-writable config
  directory or a symlinked path component made the migration helpers raise
  their own ``MigrationError`` or a bare ``OSError``. Neither is an
  ``AdminError``, so ``vq self-update`` and ``vq admin update`` showed a
  traceback. They are now admission errors that name the file and the rule,
  raised before anything is prepared or stopped.
* ``vq overview --json`` (and through it the fleet console) serialized the
  admin-update marker with ``asdict``, which for the slot schema included the
  base64 bytes of ``config.toml`` and of the unit or plist file. The overview
  now carries only the slot transaction's ``root``, ``sha`` and ``staged``.
"""
from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path

import pytest

from tests.test_runtime_daemon import (  # noqa: F401 -- fixtures
    _plist,
    _unit,
    manager_sim,
    service_programs,
)
from tests.test_runtime_execution_binding import runtimes  # noqa: F401
from vq import admin, config, overview, runtime_daemon


def _symlink_config(tmp_path: Path) -> Path:
    real = tmp_path / "dotfiles" / "config.toml"
    real.parent.mkdir()
    real.write_bytes(config.config_path().read_bytes())
    config.config_path().unlink()
    config.config_path().symlink_to(real)
    return real


class TestAdmission:
    def test_a_symlinked_registry_is_an_admission_error(self, service_programs, tmp_path) -> None:  # noqa: F811
        registered, _old, _target = service_programs
        _symlink_config(tmp_path)
        with pytest.raises(admin.AdminError, match="serving registry") as info:
            runtime_daemon.registry_binding("queue", registered)
        assert "not a symlink" in str(info.value) and "Nothing was changed" in str(info.value)

    def test_a_group_writable_config_directory_is_an_admission_error(
        self, service_programs,  # noqa: F811
    ) -> None:
        if os.geteuid() == 0:
            pytest.skip("root is not refused by mode")
        registered, _old, _target = service_programs
        directory = config.config_path().parent
        directory.chmod(0o775)
        try:
            with pytest.raises(admin.AdminError, match="owner-controlled"):
                runtime_daemon.registry_binding("queue", registered)
        finally:
            directory.chmod(0o755)

    def test_a_symlinked_path_component_is_an_admission_error(
        self, service_programs, tmp_path, monkeypatch,  # noqa: F811
    ) -> None:
        registered, _old, _target = service_programs
        link = tmp_path / "linked-config-dir"
        link.symlink_to(config.config_path().parent, target_is_directory=True)
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(link))
        with pytest.raises(admin.AdminError, match="symlinked path component"):
            runtime_daemon.registry_binding("queue", registered)

    @pytest.mark.parametrize("manager", list(admin._DaemonServiceManager))
    def test_a_symlinked_service_file_is_an_admission_error(
        self, service_programs, tmp_path, monkeypatch, manager,  # noqa: F811
    ) -> None:
        _registered, old, target = service_programs
        real = tmp_path / "dotfiles-service"
        real.write_bytes(
            _unit(old) if manager is admin._DaemonServiceManager.SYSTEMD else _plist(old)
        )
        link = tmp_path / "service"
        link.symlink_to(real)
        monkeypatch.setattr(runtime_daemon, "service_path", lambda _manager: link)
        with pytest.raises(admin.AdminError, match="daemon service file"):
            runtime_daemon.service_binding(manager, old, target)

    @pytest.mark.parametrize("manager_sim", [admin._DaemonServiceManager.SYSTEMD], indirect=True)
    def test_the_update_refuses_before_touching_the_service(
        self, manager_sim, runtimes, tmp_path,  # noqa: F811
    ) -> None:
        sim = manager_sim
        _root, shas, _program, _cfg = runtimes
        real = _symlink_config(tmp_path)
        before = real.read_bytes()
        with pytest.raises(admin.AdminError, match="serving registry"):
            runtime_daemon.update(
                "queue", sim.cfg, sim.registered, sim.probe, host="localhost",
                expected_sha=shas[1], expected_tag=None, supplied=None,
                restart_daemon=True, admin_token=None, force=False,
            )
        assert sim.commands == []  # no stop, reload or start was issued
        assert sim.state["running"]
        assert real.read_bytes() == before
        assert admin.read_admin_update_marker() is None


class TestOverviewProjection:
    def _marker(self, service_programs) -> admin.AdminUpdateMarker:  # noqa: F811
        registered, _old, _target = service_programs
        registry = runtime_daemon.registry_binding("queue", registered)
        context = {"root": registered.runtime_slot_root, "sha": "b" * 40, "staged": True,
                   "registry": registry, "service": {"before": {"entry": {"data": "ZZLEAK"}}},
                   "actions": [{"before": registry["before"], "after": registry["after"]}]}
        return admin.AdminUpdateMarker(
            pid=1, pid_start_time=1.0, started_at="t", envs=["queue"], host="k",
            vq_version="0", managed_transaction={"schema": runtime_daemon.SCHEMA,
                                                 "phase": "armed", "slot_transaction": context},
        )

    def test_the_overview_json_carries_no_receipt_file_bytes(self, service_programs) -> None:  # noqa: F811
        marker = self._marker(service_programs)
        registry_bytes = config.config_path().read_bytes().decode()
        # Guard the guard: the plain dataclass dump really does carry them.
        assert "ZZLEAK" in str(asdict(marker))
        assert registry_bytes.splitlines()[0] in str(runtime_daemon.runtime_migration._bytes(
            marker.managed_transaction["slot_transaction"]["registry"]["before"]["entry"]))

        payload = overview.format_overview_json(
            overview.HostOverview(host="k", admin_marker=marker))

        text = str(payload)
        assert "ZZLEAK" not in text
        assert "data" not in str(payload["admin_marker"]["managed_transaction"])
        assert payload["admin_marker"]["managed_transaction"]["slot_transaction"] == {
            "root": marker.managed_transaction["slot_transaction"]["root"],
            "sha": "b" * 40, "staged": True,
        }
        assert payload["admin_marker"]["managed_transaction"]["phase"] == "armed"
        for snapshot in payload["admin_markers"]:
            assert "ZZLEAK" not in str(snapshot)

    def test_a_legacy_receipt_is_unchanged(self, service_programs) -> None:  # noqa: F811
        marker = admin.AdminUpdateMarker(
            pid=1, pid_start_time=1.0, started_at="t", envs=["queue"], host="k",
            vq_version="0", managed_transaction={"schema": "legacy", "phase": "armed"},
        )
        payload = overview.format_overview_json(
            overview.HostOverview(host="k", admin_marker=marker))
        assert payload["admin_marker"] == asdict(marker)

    def test_the_marker_survives_the_remote_round_trip(self, service_programs) -> None:  # noqa: F811
        marker = self._marker(service_programs)
        payload = overview.format_overview_json(
            overview.HostOverview(host="k", admin_marker=marker))
        rebuilt = overview._admin_marker_from_payload(payload["admin_marker"])
        assert rebuilt is not None
        assert rebuilt.managed_transaction["slot_transaction"]["sha"] == "b" * 40
