"""Characterization for the doctor engine's CLI and web consumers.

These records are the boundary Milestone 2 extracts from ``vq.cli``. Keep the
payload shape, failure classification, host ordering, and defensive web error
mapping stable while the implementation moves to a public library module.
"""
from __future__ import annotations

import subprocess
import sys

import pytest

from vq import config, doctor, host_status
from vq.web import fleet as fleet_mod


def test_doctor_check_record_shape_and_metadata() -> None:
    assert doctor.check(
        "scheduler_liveness",
        False,
        "pbs_sched is not running",
        scheduler="pbs",
        driver="controller",
    ) == {
        "name": "scheduler_liveness",
        "ok": False,
        "message": "pbs_sched is not running",
        "scheduler": "pbs",
        "driver": "controller",
    }


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {
                "checks": [
                    {
                        "name": "remote_vq",
                        "ok": False,
                        "message": "remote vq failed (exit 255) on worker",
                    }
                ]
            },
            True,
        ),
        (
            {
                "checks": [
                    {
                        "name": "ssh_transport",
                        "ok": False,
                        "message": "ssh transport to worker failed",
                    }
                ]
            },
            True,
        ),
        (
            {
                "checks": [
                    {
                        "name": "scheduler_liveness",
                        "ok": False,
                        "message": "pbs_sched is not running",
                    }
                ]
            },
            False,
        ),
        (
            {
                "checks": [
                    {
                        "name": "remote_vq",
                        "ok": True,
                        "message": "recovered after exit 255",
                    }
                ]
            },
            False,
        ),
        ({"checks": "not-a-list"}, False),
        (None, False),
    ],
)
def test_doctor_transport_failure_classification(
    payload: object, expected: bool
) -> None:
    assert doctor.payload_hit_transport_failure(payload) is expected


def test_local_doctor_honors_system_multi_user_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[float, bool, float]] = []
    monkeypatch.setattr(host_status, "is_down", lambda _host: None)
    monkeypatch.setattr(config, "system_multi_user_enabled", lambda: True)
    system_multi_user = {
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
    process_identity = {
        "status": "ok",
        "error": None,
        "pid": 4242,
        "euid": 0,
        "python_executable": "/opt/vq/venv/bin/python",
        "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
        "version": "0.test",
        "source_sha": None,
        "source_tree_sha256": None,
        "multi_user": True,
        "socket_path": "/system/vq.sock",
    }

    def fake_local_probe(
        timeout: float,
        *,
        multi_user: bool,
        deadline: doctor._CheckDeadline,  # noqa: SLF001 - bounded seam
    ) -> tuple[int, dict[str, object]]:
        calls.append((timeout, multi_user, deadline.timeout_seconds))
        return 0, {
            "ok": True,
            "version": "0.test",
            "source_sha": None,
            "source_tree_sha256": None,
            "multi_user": multi_user,
            "process_identity": process_identity,
            "socket_path": "/system/vq.sock",
            "system_service": system_service,
            "system_multi_user": system_multi_user,
        }

    monkeypatch.setattr(doctor, "_local_daemon_probe", fake_local_probe)

    payload = doctor.diagnose_host(
        config.Config(), "localhost", timeout=0.25
    )
    assert payload["host"] == "localhost"
    assert payload["ok"] is True
    assert payload["scheduler"] == "local"
    assert payload["driver"] is None
    assert payload["checks"][0] == {
        "name": "config",
        "ok": True,
        "message": "implicit localhost",
    }
    assert payload["checks"][1] == {
        "name": "daemon_rpc",
        "ok": True,
        "message": "responsive (version=0.test)",
        "version": "0.test",
        "source_sha": None,
        "source_tree_sha256": None,
        "multi_user": True,
        "process_identity": process_identity,
        "socket_path": "/system/vq.sock",
        "system_service": system_service,
        "system_multi_user": system_multi_user,
    }
    assert calls == [(0.25, True, doctor.DEFAULT_CHECK_TIMEOUT_SECONDS)]


def test_web_doctor_sweep_preserves_order_payloads_and_error_shape(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(
        hosts={
            "alpha": config.HostConfig(ssh="alpha"),
            "beta": config.HostConfig(ssh="beta"),
        }
    )
    monkeypatch.setattr(fleet_mod.config, "load_config", lambda: cfg)

    def fake_is_down(host: str) -> host_status.DownEntry | None:
        if host == "beta":
            raise RuntimeError("probe exploded")
        return host_status.DownEntry(
            host=host,
            reason="maintenance",
            since="2026-08-02T12:00:00+00:00",
        )

    monkeypatch.setattr(host_status, "is_down", fake_is_down)

    assert fleet_mod.gather_doctor_results(timeout=2.5) == [
        {
            "host": "alpha",
            "ok": False,
            "scheduler": "local",
            "driver": None,
            "checks": [
                {
                    "name": "admin_down",
                    "ok": False,
                    "message": (
                        "marked down: maintenance; since "
                        "2026-08-02T12:00:00+00:00"
                    ),
                },
                {
                    "name": "config",
                    "ok": True,
                    "message": "configured ssh='alpha' scheduler='local'",
                },
            ],
        },
        {
            "host": "beta",
            "ok": False,
            "scheduler": None,
            "driver": None,
            "checks": [
                {"name": "doctor", "ok": False, "message": "probe exploded"}
            ],
        },
    ]


def test_web_doctor_call_does_not_import_click_cli() -> None:
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "from vq import config, host_status; "
                "from vq.web import fleet; "
                "fleet.config.load_config = lambda: config.Config("
                "hosts={'alpha': config.HostConfig(ssh='alpha')}); "
                "host_status.is_down = lambda host: host_status.DownEntry("
                "host=host, reason='maintenance', "
                "since='2026-08-02T12:00:00+00:00'); "
                "assert 'vq.cli' not in sys.modules; "
                "fleet.gather_doctor_results(timeout=0.01); "
                "raise SystemExit(1 if 'vq.cli' in sys.modules else 0)"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
