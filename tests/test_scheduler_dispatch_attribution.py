"""Attributing a scheduler dispatch stop: scheduler admin, vq, or both.

host_f 2026-07-25: PBS reported every queue ``enabled = True, started = False``
while `vq drain` was inactive and no marker existed. The doctor line named the
queues but not the authority, so establishing "PBS is holding this, not vq"
meant hand-correlating three commands. These pin the classification: explicit
scheduler-admin attribution when no vq mechanism is active, both named when
both hold, scoped markers only claim their own target, and nothing here ever
runs qstart/qstop or mutates scheduler state — the probe payloads are dicts
and the classification is pure read-only inspection.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from vq import admin, config, paths, scheduler_probe
from vq.cli import _scheduler_liveness_check_from_payload


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


def _pbs_payload(*, started: bool, extra: dict | None = None) -> dict:
    payload = {
        "pbs_sched_running": True,
        "server_state": "Active",
        "queues": [
            {"name": "amd", "enabled": True, "started": started},
            {"name": "big", "enabled": True, "started": started},
        ],
    }
    payload.update(extra or {})
    return payload


def _check(payload: dict, *, driver_is_local: bool = True) -> dict:
    return _scheduler_liveness_check_from_payload(
        payload, host="host_f", driver_is_local=driver_is_local
    )


class _StubDrain:
    def __init__(self, *, full: bool = False, targets: set[str] | None = None):
        self.is_full_drain = full
        self._targets = targets or set()

    def drains_scheduler_target(self, host: str) -> bool:
        return host in self._targets


def test_started_queues_are_healthy(state_dir: Path) -> None:
    check = _check(_pbs_payload(started=True))
    assert check["ok"] is True
    assert "scheduler_dispatch" not in check


def test_pbs_stop_without_vq_hold_names_the_scheduler_admin(
    state_dir: Path,
) -> None:
    """THE host_f CASE: attribution must be explicit, not left to correlation."""
    check = _check(_pbs_payload(started=False))

    assert check["ok"] is False
    dispatch = check["scheduler_dispatch"]
    assert dispatch["authority"] == "scheduler_admin"
    assert dispatch["mechanism"] == "pbs_queue_not_started"
    assert dispatch["vq_drain_active"] is False
    assert dispatch["admin_update_marker_active"] is False
    assert dispatch["running_jobs_continue"] is True
    assert dispatch["queued_jobs_wait"] is True
    assert dispatch["operator_action_required"] is True
    assert "NOT a vq hold" in check["message"]
    assert "Do not run qstart" in check["message"]
    assert "vq drain --release" in check["message"]


def test_pbs_stop_plus_vq_drain_names_both_mechanisms(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "vq.cli.drain_module.read_effective_drain_state",
        lambda *a, **k: _StubDrain(targets={"host_f"}),
    )

    check = _check(_pbs_payload(started=False))

    dispatch = check["scheduler_dispatch"]
    assert dispatch["authority"] == "scheduler_admin_and_vq"
    assert dispatch["vq_drain_active"] is True
    assert "scheduler drain lane holds host_f" in check["message"]
    assert "BOTH" in check["message"]
    # Both named, sole attribution claimed by neither.
    assert "NOT a vq hold" not in check["message"]


def test_a_scoped_marker_claims_only_its_own_target(state_dir: Path) -> None:
    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_c:vibeqc-dev"], host="host_c"
    )

    check = _check(_pbs_payload(started=False))

    dispatch = check["scheduler_dispatch"]
    assert dispatch["admin_update_marker_active"] is False, (
        "a host_c marker must not be blamed for a host_f PBS stop"
    )
    assert dispatch["authority"] == "scheduler_admin"


def test_a_live_marker_for_this_target_is_named(state_dir: Path) -> None:
    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_f:vibeqc-release"], host="host_f"
    )

    check = _check(_pbs_payload(started=False))

    dispatch = check["scheduler_dispatch"]
    assert dispatch["admin_update_marker_active"] is True
    assert dispatch["admin_update_marker_envs"] == [
        "scheduler-runtime:host_f:vibeqc-release"
    ]
    assert dispatch["admin_update_marker_started_at"]
    assert dispatch["authority"] == "scheduler_admin_and_vq"


def test_a_stale_marker_keeps_existing_stale_semantics(
    state_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admin.write_admin_update_marker(
        envs=["scheduler-runtime:host_f:vibeqc-release"], host="host_f"
    )
    monkeypatch.setattr(admin, "_pid_liveness", lambda pid: False)

    check = _check(_pbs_payload(started=False))

    assert check["scheduler_dispatch"]["admin_update_marker_active"] is False


def test_job_counts_do_not_change_the_classification(state_dir: Path) -> None:
    quiet = _check(_pbs_payload(started=False))
    busy = _check(
        _pbs_payload(
            started=False,
            extra={"jobs_running": 43, "jobs_queued": 17},
        )
    )
    assert quiet["scheduler_dispatch"] == busy["scheduler_dispatch"]


def test_remote_driver_reports_partial_attribution(state_dir: Path) -> None:
    """vq hold state lives on the driver; never guess it from elsewhere."""
    check = _check(_pbs_payload(started=False), driver_is_local=False)

    dispatch = check["scheduler_dispatch"]
    assert dispatch["vq_drain_active"] is None
    assert dispatch["admin_update_marker_active"] is None
    assert "run vq doctor there" in check["message"]


def test_slurm_partition_hold_is_classified(state_dir: Path) -> None:
    check = _check(
        {"slurm_squeue_ok": True, "slurm_partitions_not_up": ["intelsr"]}
    )
    assert check["ok"] is False
    dispatch = check["scheduler_dispatch"]
    assert dispatch["mechanism"] == "slurm_partition_not_up"
    assert dispatch["authority"] == "scheduler_admin"


def test_slurm_without_partition_state_claims_nothing(state_dir: Path) -> None:
    check = _check({"slurm_squeue_ok": True})
    assert check["ok"] is True
    assert "no attribution claimed" in check["message"]
    check_up = _check(
        {"slurm_squeue_ok": True, "slurm_partitions_not_up": []}
    )
    assert check_up["ok"] is True
    assert "all partitions up" in check_up["message"]


class _StubRunner:
    def __init__(self, rc: int, stdout: str, stderr: str = "") -> None:
        self._rc, self._stdout, self._stderr = rc, stdout, stderr
        self.commands: list[list[str]] = []

    def run(self, argv, check=False):  # type: ignore[no-untyped-def]
        self.commands.append(list(argv))

        class R:
            returncode = self._rc
            stdout = self._stdout
            stderr = self._stderr

        return R()


def test_partition_probe_reads_sinfo_and_never_mutates() -> None:
    runner = _StubRunner(0, "batch* up\nintelsr down\nlong inact\n")
    held, error = scheduler_probe._probe_slurm_partition_availability(runner)
    assert held == ("intelsr", "long")
    assert error is None
    assert all(cmd[0] == "sinfo" for cmd in runner.commands), (
        "the probe must be read-only sinfo, never scontrol update/qstart"
    )


def test_partition_probe_failure_yields_no_attribution() -> None:
    runner = _StubRunner(127, "", "sinfo: command not found")
    held, error = scheduler_probe._probe_slurm_partition_availability(runner)
    assert held is None
    assert "not found" in (error or "")
