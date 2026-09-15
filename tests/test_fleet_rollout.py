"""Planning and resume coverage for ``vq admin rollout-latest``."""

from __future__ import annotations

import ast
import copy
import dataclasses
import fcntl
import inspect
import io
import json
import os
import shlex
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager, suppress
from pathlib import Path
from typing import Any

import pytest

from vq import (
    config,
    fleet_operation,
    fleet_release,
    fleet_rollout,
    legacy_failure_transition,
    paths,
)

OLD = "1" * 40
RELEASE = "2" * 40
DEV = "3" * 40
VQ = "4" * 40
VIEW = "5" * 40
FUTURE = "6" * 40
TARGET_TREE = "7" * 64


def test_driver_reentry_inherits_timeout_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {
        "VQ_UPDATE_SCRIPT_TIMEOUT": "21600",
        "VQ_BUILD_STALL_TIMEOUT": "7200",
        "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT": "24000",
    }
    for name, value in expected.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("PYTHONHOME", "/spoof/home")
    monkeypatch.setenv("PYTHONPATH", "/spoof/path")
    monkeypatch.setenv("PYTHONSTARTUP", "/spoof/startup.py")
    monkeypatch.setenv(
        fleet_rollout.ENV_ROLLOUT_REENTRY_HANDOFF,
        "ambient-capability-must-be-replaced",
    )
    observed: dict[str, str | None] = {}
    read_fd, write_fd = os.pipe()
    handoff = '{"schema":"vq.rollout.reentry_handoff/1"}'

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert argv[:8] == [
            "/managed/vq/bin/python",
            "-I",
            "-m",
            "vq",
            "admin",
            "rollout-latest",
            "--resume",
            "v0.15.60-timeout-contract",
        ]
        assert kwargs["pass_fds"] == (read_fd, write_fd)
        assert kwargs["close_fds"] is True
        assert kwargs["text"] is True
        assert kwargs["check"] is False
        environment = kwargs["env"]
        assert isinstance(environment, dict)
        observed.update({name: environment.get(name) for name in expected})
        assert environment[fleet_rollout.ENV_ROLLOUT_REENTRY_HANDOFF] == handoff
        assert "PYTHONHOME" not in environment
        assert "PYTHONPATH" not in environment
        assert "PYTHONSTARTUP" not in environment
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    try:
        rc = fleet_rollout.reenter_after_driver(
            "v0.15.60-timeout-contract",
            python="/managed/vq/bin/python",
            reentry_handoff=handoff,
            pass_fds=(read_fd, write_fd),
            as_json=False,
            runner=runner,
        )
    finally:
        os.close(read_fd)
        os.close(write_fd)

    assert rc == 0
    assert observed == expected


def test_rollout_action_inherits_timeout_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {
        "VQ_UPDATE_SCRIPT_TIMEOUT": "21600",
        "VQ_BUILD_STALL_TIMEOUT": "7200",
        "VQ_REMOTE_ADMIN_UPDATE_TIMEOUT": "24000",
    }
    for name, value in expected.items():
        monkeypatch.setenv(name, value)
    observed: dict[str, str | None] = {}

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        assert "env" not in kwargs
        probe = subprocess.run(
            [
                sys.executable,
                "-c",
                "import json, os; print(json.dumps({name: os.environ.get(name) "
                "for name in "
                "['VQ_UPDATE_SCRIPT_TIMEOUT', 'VQ_BUILD_STALL_TIMEOUT', "
                "'VQ_REMOTE_ADMIN_UPDATE_TIMEOUT']}))",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        observed.update(json.loads(probe.stdout))
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    action = fleet_rollout.RolloutAction(
        id="driver:localhost:vibeqc-queue",
        phase="driver",
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.15.60",
        target_tag=None,
        argv=["admin", "update", "vibeqc-queue", "localhost"],
        decision="update",
        reason="stale",
        before={},
    )

    proc = fleet_rollout.execute_action(action, runner=runner)

    assert proc.returncode == 0
    assert observed == expected


def _pin(
    name: str,
    sha: str,
    version: str,
    *,
    tag: str | None = None,
) -> fleet_release.FleetPin:
    flags = (
        ("--tag", tag, "--expected-sha", sha)
        if tag is not None
        else ("--expected-sha", sha)
    )
    return fleet_release.FleetPin(
        name=name,
        sha=sha,
        version=version,
        tag=tag,
        deploy_flags=flags,
        gating_job=fleet_release.PIN_GATING_JOBS[name],
        pipeline_id=4500,
        evidence_sha=sha,
        acceptance_rule="A",
    )


def _report() -> fleet_release.FleetReleaseReport:
    pins = {
        "release": _pin("release", RELEASE, "0.15.60", tag="v0.15.60"),
        "dev": _pin("dev", DEV, "0.15.61.dev0"),
        "vq": _pin("vq", VQ, "0.17.0"),
        "vibe_view": _pin("vibe_view", VIEW, "2.5.0"),
    }
    return fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.60.json",
        digest_sha256="a" * 64,
        generated_at="2026-07-25T12:00:00Z",
        release_version=(0, 15, 60),
        pins=pins,
        raw={"schema": "vq.fleet.release_report/2"},
    )


def _report_at(
    patch: int,
    *,
    digest: str,
) -> fleet_release.FleetReleaseReport:
    """Return an accepted-report fixture with a truthful tag/version pair."""
    release_version = f"0.15.{patch}"
    release_tag = f"v{release_version}"
    pins = {
        "release": _pin(
            "release",
            RELEASE,
            release_version,
            tag=release_tag,
        ),
        "dev": _pin("dev", DEV, f"0.15.{patch + 1}.dev0"),
        "vq": _pin("vq", VQ, "0.25.7"),
        "vibe_view": _pin("vibe_view", VIEW, "2.5.0"),
    }
    return fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path=f"vibe-queue/releases/{release_tag}.json",
        digest_sha256=digest,
        generated_at="2026-09-02T12:00:00Z",
        release_version=(0, 15, patch),
        pins=pins,
        raw={"schema": "vq.fleet.release_report/2"},
    )


def _runtime() -> config.SchedulerRuntimeDeployment:
    return config.SchedulerRuntimeDeployment(
        update_command="/site/deploy",
        verify_command="/site/verify",
    )


def _scheduler_host(
    *,
    ssh: str,
    role: str = "managed",
    canonical: str | None = None,
    deployments: bool = True,
) -> config.HostConfig:
    return config.HostConfig(
        ssh=ssh,
        scheduler="pbs",
        scheduler_dialect="torque",
        scratch_root="/scratch/user",
        scheduler_driver="localhost",
        scheduler_update_command=(
            "/site/update-helper" if role != "alias" else None
        ),
        scheduler_runtime_deployments=(
            {
                "vibeqc-release": _runtime(),
                "vibeqc-dev": _runtime(),
                "vibe-view": _runtime(),
            }
            if deployments
            else {}
        ),
        fleet_role=role,
        fleet_canonical_host=canonical,
    )


def _config(*, include_unresolved: bool = False) -> config.Config:
    hosts = {
        "localhost": config.HostConfig(ssh="localhost", fleet_role="managed"),
        "host_f": _scheduler_host(ssh="host_f"),
        "host_f-big": _scheduler_host(
            ssh="host_f",
            role="alias",
            canonical="host_f",
            # Legacy fleet configs copied deployment profiles into every PBS
            # queue alias. The explicit alias role must win during migration
            # so those copies cannot schedule duplicate builds.
            deployments=True,
        ),
        "host_d": config.HostConfig(ssh="host_d", fleet_role="managed"),
        "host_0": config.HostConfig(
            ssh="host_0",
            fleet_role="vq-only",
        ),
        "retired": config.HostConfig(ssh="retired", fleet_role="excluded"),
    }
    if include_unresolved:
        hosts["mystery"] = config.HostConfig(ssh="mystery")
    return config.Config(
        hosts=hosts,
        fleet_rollout_order=[
            "host_f",
            "host_f-big",
            "host_d",
            "localhost",
            "host_0",
            "retired",
            *(["mystery"] if include_unresolved else []),
        ],
    )


def _env(
    name: str,
    sha: str,
    *,
    tag: str | None = None,
    success: bool = True,
    marked: bool = False,
    installed_matches_checkout: bool | None = True,
) -> dict[str, Any]:
    return {
        "name": name,
        "error": None,
        "current_sha": sha[:12],
        "is_dirty": False,
        "last_success": success,
        "installed_sha_matches_checkout": installed_matches_checkout,
        "last_sha": sha[:12],
        "last_expected_sha": sha,
        "last_installed_sha": sha[:12],
        "last_tag": tag,
        "last_marked_ok_at": "2026-07-25T00:00:00Z" if marked else None,
    }


def _program(name: str, sha: str, version: str) -> dict[str, Any]:
    return {
        "name": name,
        "kind": "venv",
        "status": "OK",
        "current_git_sha_full": sha,
        "current_git_dirty": False,
        "import_version": version,
    }


def _deployment(
    sha: str,
    *,
    tag: str | None = None,
    success: bool = True,
) -> dict[str, Any]:
    return {
        "configured": True,
        "last": {
            "actual_sha": sha,
            "actual_tag": tag,
            "last_success": success,
            "healthy": success,
            "health_detail": "verification passed" if success else "failed",
        },
    }


def _snapshots(
    *,
    local_sha: str = OLD,
    release_tag: str = "v0.15.59",
    success: bool = True,
    marked: bool = False,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    admin: dict[str, Any] = {}
    programs: dict[str, Any] = {}
    for host in ("localhost", "host_d", "host_0"):
        admin[host] = {
            "envs": [
                _env("vibeqc-queue", local_sha, success=success, marked=marked),
                *(
                    [
                        _env(
                            "vibeqc-release",
                            local_sha,
                            tag=release_tag,
                            success=success,
                            marked=marked,
                        ),
                        _env(
                            "vibeqc-dev",
                            local_sha,
                            success=success,
                            marked=marked,
                        ),
                        _env(
                            "vibe-view",
                            local_sha,
                            success=success,
                            marked=marked,
                        ),
                    ]
                    if host != "host_0"
                    else []
                ),
            ],
            "marker": None,
        }
        programs[host] = [
            _program("vibeqc-queue", local_sha, "0.16.0"),
            *(
                [
                    _program(
                        "vibeqc-release",
                        local_sha,
                        release_tag.removeprefix("v"),
                    ),
                    _program("vibeqc-dev", local_sha, "0.15.59.dev0"),
                    _program("vibe-view", local_sha, "2.4.0"),
                ]
                if host != "host_0"
                else []
            ),
        ]
    admin["host_f"] = {
        "deployments": {
            "vibeqc-release": _deployment(
                local_sha,
                tag=release_tag,
                success=success,
            ),
            "vibeqc-dev": _deployment(local_sha, success=success),
            "vibe-view": _deployment(local_sha, success=success),
        },
        "marker": None,
    }
    doctor = {}
    for host in (
        "localhost",
        "host_d",
        "host_0",
        "retired",
        "host_f-big",
    ):
        doctor[host] = {
            "ok": True,
            "checks": [
                {
                    "name": "daemon_rpc",
                    "ok": True,
                    "message": "responsive",
                    "version": "0.17.0",
                    "source_sha": local_sha,
                    "source_tree_sha256": "c" * 64,
                    "multi_user": False,
                    "socket_path": "/tmp/vq/daemon.sock",
                    "system_multi_user": {
                        "enabled": False,
                        "error": None,
                        "source": "/etc/vq/config.toml",
                        "status": "absent",
                    },
                    "system_service": {
                        "status": "unsupported",
                        "error": "systemd is unavailable on this platform",
                    },
                }
            ],
        }
    doctor["host_f"] = {
        "ok": success,
        "checks": [
            {
                "name": "scheduler_remote_vq",
                "ok": success,
                "source_sha": local_sha,
                "message": f"SOURCE-SHA {local_sha} match driver",
            },
            {"name": "scheduler_liveness", "ok": True},
        ],
    }
    return admin, programs, doctor


def _ancestry(_pin_name: str, older: str, newer: str) -> bool | None:
    if older == newer:
        return True
    order = {OLD: 0, RELEASE: 1, DEV: 2, VQ: 3, VIEW: 4, FUTURE: 5}
    if older not in order or newer not in order:
        return None
    return order[older] < order[newer]


def _plan(
    *,
    cfg: config.Config | None = None,
    snapshots: tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None = None,
    target_vq_tree_sha256: str | None = None,
) -> fleet_rollout.RolloutPlan:
    admin, programs, doctor = snapshots or _snapshots()
    return fleet_rollout.build_plan(
        cfg or _config(),
        _report(),
        admin_status=admin,
        programs=programs,
        doctor=doctor,
        ancestry=_ancestry,
        target_vq_tree_sha256=target_vq_tree_sha256,
    )


def _exact_root_daemon_check() -> dict[str, Any]:
    process_identity = {
        "status": "ok",
        "error": None,
        "pid": 4242,
        "euid": 0,
        "python_executable": "/opt/vq/venv/bin/python3.14",
        "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
        "version": "0.17.0",
        "source_sha": VQ,
        "source_tree_sha256": TARGET_TREE,
        "multi_user": True,
        "socket_path": "/var/lib/vq/daemon.sock",
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
    return {
        "name": "daemon_rpc",
        "ok": True,
        "message": "responsive (version=0.17.0)",
        "version": "0.17.0",
        "source_sha": VQ,
        "source_tree_sha256": TARGET_TREE,
        "multi_user": True,
        "process_identity": process_identity,
        "socket_path": "/var/lib/vq/daemon.sock",
        "system_service": system_service,
        "system_multi_user": {
            "enabled": True,
            "error": None,
            "source": "/etc/vq/config.toml",
            "status": "enabled",
        },
    }


def _set_nested(record: dict[str, Any], path: str, value: object) -> None:
    parts = path.split(".")
    target = record
    for part in parts[:-1]:
        nested = target[part]
        assert isinstance(nested, dict)
        target = nested
    target[parts[-1]] = value


def test_plan_is_serial_driver_helper_scheduler_then_local() -> None:
    plan = _plan()

    assert plan.driver == "localhost"
    assert [action.id for action in plan.actions] == [
        "driver:localhost:vibeqc-queue",
        "local-runtime:host_0:vibeqc-queue",
        "helper:host_f",
        "scheduler-runtime:host_f:vibeqc-release",
        "scheduler-runtime:host_f:vibeqc-dev",
        "scheduler-runtime:host_f:vibe-view",
        "local-runtime:host_d:vibeqc-queue",
        "local-runtime:host_d:vibeqc-release",
        "local-runtime:host_d:vibeqc-dev",
        "local-runtime:host_d:vibe-view",
        "local-runtime:localhost:vibeqc-release",
        "local-runtime:localhost:vibeqc-dev",
        "local-runtime:localhost:vibe-view",
    ]
    assert all(action.decision == "update" for action in plan.actions)
    assert not any(
        action.host in {"host_f-big", "retired"} for action in plan.actions
    )
    host_0 = next(
        action for action in plan.actions if action.host == "host_0"
    )
    assert host_0.phase == "local-runtime"
    assert host_0.program == "vibeqc-queue"
    assert host_0.argv == [
        "admin",
        "update",
        "vibeqc-queue",
        "host_0",
        "--expected-sha",
        VQ,
    ]
    assert not any(
        action.host == "host_0" and action.program != "vibeqc-queue"
        for action in plan.actions
    )
    host_f_release = next(
        action
        for action in plan.actions
        if action.id == "scheduler-runtime:host_f:vibeqc-release"
    )
    assert host_f_release.argv == [
        "admin",
        "update",
        "vibeqc-release",
        "host_f",
        "--tag",
        "v0.15.60",
        "--expected-sha",
        RELEASE,
        "--drain-wait",
        "4h",
    ]
    assert "--force" not in host_f_release.argv


def test_plan_reports_managed_lane_scope_and_explicit_exclusions() -> None:
    plan = _plan()

    coverage = plan.as_dict()["coverage"]

    assert (
        coverage["scope"]
        == "managed-lanes+vq-user-lanes+read-only-provenance"
    )
    assert coverage["whole_fleet_convergence_asserted"] is False
    assert coverage["managed_lanes"] == {
        "total": len(plan.actions) - 1,
        "converged": 0,
    }
    assert coverage["vq_user_lanes"] == {
        "total": 1,
        "configured": 1,
        "converged": 0,
        "deferred": 0,
        "blocked": 0,
    }
    assert {
        (item["kind"], item.get("host"), item["component"])
        for item in coverage["exclusions"]
    } == {
        ("operator-managed", None, "vibe-basisopt"),
        ("topology-role", "host_0", "vibeqc-release"),
        ("topology-role", "host_0", "vibeqc-dev"),
        ("topology-role", "host_0", "vibe-view"),
        ("topology-role", "retired", "host"),
    }
    assert all(
        "vq-only host intentionally has no" in item["reason"]
        for item in coverage["exclusions"]
        if item.get("host") == "host_0"
    )


def test_vq_only_exact_user_install_requires_live_daemon_provenance() -> None:
    admin, programs, doctor = _snapshots(local_sha=VQ)
    programs["host_0"][0]["import_version"] = "0.17.0"
    doctor["host_0"]["checks"][0].update(
        {
            "version": "0.17.0",
            "source_sha": VQ,
            "source_tree_sha256": TARGET_TREE,
            "multi_user": False,
        }
    )

    action = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).actions
        if item.host == "host_0"
    )

    assert action.decision == "skip"
    assert action.reason == "already at target with LAST OK=true"
    assert action.before["required"] is True
    assert action.before["last_ok"] is True

    doctor["host_0"]["checks"][0]["source_sha"] = OLD
    stale_daemon = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).actions
        if item.host == "host_0"
    )
    assert stale_daemon.decision == "update"
    assert stale_daemon.before["last_ok"] is False
    assert "daemon.source_sha" in stale_daemon.before["detail"]


def test_vq_only_torn_admin_identity_defers_instead_of_converging() -> None:
    admin, programs, doctor = _snapshots(local_sha=VQ)
    programs["host_0"][0]["import_version"] = "0.17.0"
    doctor["host_0"]["checks"][0].update(
        {
            "version": "0.17.0",
            "source_sha": VQ,
            "source_tree_sha256": TARGET_TREE,
            "multi_user": False,
        }
    )
    admin_env = admin["host_0"]["envs"][0]
    admin_env.update(
        {
            "current_sha": OLD[:12],
            "last_sha": OLD[:12],
            "last_expected_sha": OLD,
            "last_installed_sha": OLD[:12],
        }
    )

    action = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).actions
        if item.host == "host_0"
    )

    assert action.decision == "defer"
    assert "admin and program snapshots disagree" in action.reason


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("installed_sha_matches_checkout", None),
        ("last_marked_ok_at", "2026-08-10T00:00:00Z"),
        ("current_sha", None),
        ("last_sha", None),
        ("last_expected_sha", None),
        ("last_installed_sha", None),
    ],
)
def test_vq_only_missing_or_acknowledged_install_proof_forces_update(
    field: str,
    value: object,
) -> None:
    admin, programs, doctor = _snapshots(local_sha=VQ)
    admin["host_0"]["envs"][0][field] = value
    programs["host_0"][0]["import_version"] = "0.17.0"
    doctor["host_0"]["checks"][0].update(
        {
            "version": "0.17.0",
            "source_sha": VQ,
            "source_tree_sha256": TARGET_TREE,
            "multi_user": False,
        }
    )

    action = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).actions
        if item.host == "host_0"
    )

    assert action.decision == "update"
    assert action.before["last_ok"] is False


def test_vq_only_driver_does_not_get_a_duplicate_local_action() -> None:
    cfg = _config()
    hosts = dict(cfg.hosts)
    hosts["localhost"] = hosts["localhost"].model_copy(
        update={"fleet_role": "managed"}
    )
    hosts["host_0"] = hosts["host_0"].model_copy(
        update={"fleet_role": "vq-only"}
    )
    hosts["host_f"] = hosts["host_f"].model_copy(
        update={"scheduler_driver": "host_0"}
    )
    cfg = cfg.model_copy(update={"hosts": hosts})

    plan = _plan(cfg=cfg)

    assert plan.driver == "host_0"
    assert [
        action.id for action in plan.actions if action.host == "host_0"
    ] == ["driver:host_0:vibeqc-queue"]


def test_nonlocal_vq_only_role_is_a_topology_block() -> None:
    cfg = _config()
    hosts = dict(cfg.hosts)
    hosts["host_0"] = _scheduler_host(
        ssh="host_0",
        role="vq-only",
        deployments=False,
    )
    cfg = cfg.model_copy(update={"hosts": hosts})

    plan = _plan(cfg=cfg)

    assert plan.has_blocks
    assert any(
        error.startswith("host_0: vq-only role requires scheduler=local")
        for error in plan.topology_errors
    )
    assert not any(action.host == "host_0" for action in plan.actions)


def test_coverage_counts_only_configured_managed_lanes() -> None:
    action = fleet_rollout.RolloutAction(
        id="local-runtime:localhost:vibe-view",
        phase="local-runtime",
        host="localhost",
        program="vibe-view",
        pin_name="vibe_view",
        target_sha=VIEW,
        target_version="2.5.0",
        target_tag=None,
        argv=["admin", "update", "vibe-view", "localhost"],
        decision="skip",
        reason="git_dir not a directory",
        before={"configured": False},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )

    assert plan.as_dict()["coverage"]["managed_lanes"] == {
        "total": 0,
        "converged": 0,
    }


def test_root_daemon_inventory_defers_an_enabled_multi_user_host() -> None:
    admin, programs, doctor = _snapshots()
    process_identity = {
        "status": "ok",
        "error": None,
        "pid": 4242,
        "euid": 0,
        "python_executable": "/opt/vq/venv/bin/python",
        "argv": ["/opt/vq/venv/bin/vq", "daemon", "run"],
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
    doctor["host_d"]["checks"] = [
        {
            "name": "daemon_rpc",
            "ok": True,
            "message": "responsive (version=0.15.112)",
            "version": "0.15.112",
            "source_sha": OLD,
            "source_tree_sha256": None,
            "multi_user": True,
            "process_identity": process_identity,
            "socket_path": "/var/lib/vq/daemon.sock",
            "system_service": system_service,
            "system_multi_user": {
                "enabled": True,
                "error": None,
                "source": "/etc/vq/config.toml",
                "status": "enabled",
            },
        }
    ]

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )

    lane = next(
        item
        for item in plan.provenance_lanes
        if item.id == "root-daemon:host_d:vibeqc-queue"
    )
    assert lane.decision == "defer"
    assert lane.applicable is True
    assert lane.evidence["source_sha"] == OLD
    assert lane.evidence["process_identity"] == process_identity
    assert lane.evidence["system_service"] == system_service
    assert lane.target_sha == VQ
    assert plan.has_deferred
    assert all(action.phase != "root-daemon" for action in plan.actions)
    assert not any("sudo" in action.argv for action in plan.actions)


def test_exact_root_daemon_provenance_converges_read_only() -> None:
    admin, programs, doctor = _snapshots()
    doctor["host_d"]["checks"] = [_exact_root_daemon_check()]

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    lane = next(item for item in plan.provenance_lanes if item.host == "host_d")

    assert lane.applicable is True
    assert lane.decision == "skip"
    assert lane.target_source_tree_sha256 == TARGET_TREE
    assert lane.evidence["comparison"] == {"status": "exact", "errors": []}
    assert "exact accepted root daemon" in lane.reason
    assert plan.as_dict()["coverage"]["provenance_lanes"]["converged"] == 1
    assert all(action.phase != "root-daemon" for action in plan.actions)
    assert not any("sudo" in action.argv for action in plan.actions)


@pytest.mark.parametrize(
    ("path", "value", "error_code"),
    [
        ("version", "0.16.9", "daemon.version.mismatch"),
        ("source_sha", OLD, "daemon.source_sha.mismatch"),
        (
            "source_tree_sha256",
            "8" * 64,
            "daemon.source_tree_sha256.mismatch",
        ),
        ("multi_user", False, "daemon.multi_user.mismatch"),
        ("socket_path", "/tmp/vq.sock", "daemon.socket_path.mismatch"),
        ("process_identity.pid", True, "process_identity.pid.invalid"),
        (
            "process_identity.status",
            [],
            "process_identity.status.unavailable",
        ),
        ("process_identity.euid", 1000, "process_identity.euid.mismatch"),
        (
            "process_identity.python_executable",
            "/usr/bin/python3",
            "process_identity.python_executable.mismatch",
        ),
        (
            "process_identity.argv",
            ["/opt/vq/venv/bin/vq", "daemon", "status"],
            "process_identity.argv.mismatch",
        ),
        (
            "process_identity.source_sha",
            OLD,
            "process_identity.source_sha.mismatch",
        ),
        (
            "process_identity.version",
            "0.16.9",
            "process_identity.version.mismatch",
        ),
        (
            "process_identity.source_tree_sha256",
            "8" * 64,
            "process_identity.source_tree_sha256.mismatch",
        ),
        (
            "process_identity.multi_user",
            False,
            "process_identity.multi_user.mismatch",
        ),
        (
            "process_identity.socket_path",
            "/tmp/vq.sock",
            "process_identity.socket_path.mismatch",
        ),
        (
            "system_service.main_pid",
            4243,
            "system_service.main_pid.mismatch",
        ),
        (
            "system_service.active_state",
            "inactive",
            "system_service.active_state.mismatch",
        ),
        (
            "system_service.executable",
            "/usr/local/bin/vq",
            "system_service.executable.mismatch",
        ),
        (
            "system_service.source",
            "/usr/local/bin/systemctl",
            "system_service.source.mismatch",
        ),
        (
            "system_service.source",
            [],
            "system_service.source.mismatch",
        ),
        (
            "system_service.id",
            "vq-daemon.service",
            "system_service.id.mismatch",
        ),
        (
            "system_service.load_state",
            "not-found",
            "system_service.load_state.mismatch",
        ),
        (
            "system_service.sub_state",
            "exited",
            "system_service.sub_state.mismatch",
        ),
        (
            "system_service.user",
            "vq",
            "system_service.user.mismatch",
        ),
        (
            "system_service.argv",
            ["/opt/vq/venv/bin/vq", "daemon", "status"],
            "system_service.argv.mismatch",
        ),
        (
            "system_service.exec_start",
            "bad\ncontrol",
            "system_service.exec_start.invalid",
        ),
    ],
)
def test_root_daemon_exact_identity_fails_closed_one_field_at_a_time(
    path: str,
    value: object,
    error_code: str,
) -> None:
    admin, programs, doctor = _snapshots()
    check = copy.deepcopy(_exact_root_daemon_check())
    _set_nested(check, path, value)
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is True
    assert lane.decision == "defer"
    assert lane.evidence["comparison"]["status"] == "mismatch"
    assert error_code in lane.evidence["comparison"]["errors"]


@pytest.mark.parametrize(
    "python_executable",
    [
        "/opt/vq/venv/bin/python",
        "/opt/vq/venv/bin/python3",
        "/opt/vq/venv/bin/python3.14",
    ],
)
def test_root_daemon_accepts_supported_venv_python_names(
    python_executable: str,
) -> None:
    admin, programs, doctor = _snapshots()
    check = _exact_root_daemon_check()
    check["process_identity"]["python_executable"] = python_executable
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.decision == "skip"


def test_root_daemon_missing_expected_digest_and_torn_snapshots_defer() -> None:
    admin, programs, doctor = _snapshots()
    doctor["host_d"]["checks"] = [_exact_root_daemon_check()]

    missing = next(
        lane
        for lane in _plan(snapshots=(admin, programs, doctor)).provenance_lanes
        if lane.host == "host_d"
    )
    assert missing.decision == "defer"
    assert (
        "target.source_tree_sha256.missing"
        in missing.evidence["comparison"]["errors"]
    )

    torn = copy.deepcopy(_exact_root_daemon_check())
    torn["process_identity"]["source_sha"] = OLD
    doctor["host_d"]["checks"] = [torn]
    mismatched = next(
        lane
        for lane in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if lane.host == "host_d"
    )
    assert mismatched.decision == "defer"
    assert (
        "process_identity.source_sha.mismatch"
        in mismatched.evidence["comparison"]["errors"]
    )


def test_active_system_unit_overrides_an_absent_config_claim() -> None:
    admin, programs, doctor = _snapshots()
    check = _exact_root_daemon_check()
    check["multi_user"] = False
    check["system_multi_user"] = {
        "enabled": False,
        "error": None,
        "source": "/etc/vq/config.toml",
        "status": "absent",
    }
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is True
    assert lane.decision == "defer"
    assert "system service is active" in lane.reason


def test_starting_system_unit_does_not_prove_root_daemon_is_inapplicable() -> None:
    admin, programs, doctor = _snapshots()
    check = _exact_root_daemon_check()
    check["multi_user"] = False
    check["system_multi_user"] = {
        "enabled": False,
        "error": None,
        "source": "/etc/vq/config.toml",
        "status": "absent",
    }
    check["system_service"] = {
        "status": "ok",
        "active_state": "activating",
        "sub_state": "start",
        "main_pid": 0,
    }
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is None
    assert lane.decision == "defer"


def test_contradictory_unsupported_service_does_not_prove_not_applicable() -> None:
    admin, programs, doctor = _snapshots()
    check = copy.deepcopy(doctor["host_d"]["checks"][0])
    check["system_service"] = {
        "status": "unsupported",
        "error": "probe unsupported",
        "sub_state": "running",
    }
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is None
    assert lane.decision == "defer"


def test_partial_inactive_service_does_not_prove_not_applicable() -> None:
    admin, programs, doctor = _snapshots()
    check = copy.deepcopy(doctor["host_d"]["checks"][0])
    check["system_service"] = {
        "status": "ok",
        "error": None,
        "source": "/usr/bin/systemctl",
        "id": "vq-daemon-multi-user.service",
        "load_state": "loaded",
        "active_state": "inactive",
        "sub_state": "dead",
        "main_pid": False,
    }
    doctor["host_d"]["checks"] = [check]

    lane = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is None
    assert lane.decision == "defer"


def test_root_daemon_inventory_distinguishes_not_applicable_and_unknown() -> None:
    admin, programs, doctor = _snapshots()
    doctor["localhost"]["checks"] = [
        {
            "name": "daemon_rpc",
            "ok": True,
            "message": "responsive",
                "system_multi_user": {
                    "enabled": False,
                    "error": None,
                    "source": "/etc/vq/config.toml",
                    "status": "absent",
                },
                "system_service": {
                    "status": "unsupported",
                    "error": "systemd is unavailable on this platform",
                },
            }
        ]
    # An old remote CLI has no target-config evidence. It must not make the
    # root lane disappear or look converged.
    doctor["host_d"]["checks"] = [
        {"name": "daemon_rpc", "ok": True, "message": "responsive"}
    ]

    plan = _plan(snapshots=(admin, programs, doctor))
    by_host = {lane.host: lane for lane in plan.provenance_lanes}

    assert by_host["localhost"].decision == "skip"
    assert by_host["localhost"].applicable is False
    assert by_host["host_d"].decision == "defer"
    assert by_host["host_d"].applicable is None
    assert "host_f" not in by_host  # scheduler hosts are daemonless
    assert "host_f-big" not in by_host  # aliases never duplicate inventory
    assert "retired" not in by_host


def test_live_multi_user_rpc_overrides_a_contradictory_absent_config_claim() -> None:
    admin, programs, doctor = _snapshots()
    doctor["host_d"]["checks"] = [
        {
            "name": "daemon_rpc",
            "ok": True,
            "message": "responsive",
            "multi_user": True,
            "system_multi_user": {
                "enabled": False,
                "error": None,
                "source": "/etc/vq/config.toml",
                "status": "absent",
            },
        }
    ]

    lane = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).provenance_lanes
        if item.host == "host_d"
    )

    assert lane.applicable is True
    assert lane.decision == "defer"
    assert "reports a live multi-user daemon" in lane.reason


def test_root_provenance_has_separate_coverage_and_no_action_count_churn() -> None:
    plan = _plan()

    coverage = plan.as_dict()["coverage"]

    assert coverage["managed_lanes"] == {
        "total": len(plan.actions) - 1,
        "converged": 0,
    }
    assert coverage["vq_user_lanes"] == {
        "total": 1,
        "configured": 1,
        "converged": 0,
        "deferred": 0,
        "blocked": 0,
    }
    assert coverage["provenance_lanes"] == {
        "total": len(plan.provenance_lanes),
        "converged": 0,
        "not_applicable": len(plan.provenance_lanes),
        "deferred": 0,
        "blocked": 0,
    }
    assert plan.as_dict()["summary"]["update"] == len(plan.actions)


def test_helper_action_is_pinned_to_the_report_vq_identity() -> None:
    """The helper deploys the accepted report's vq pin, never the live
    driver checkout — the report is the sole source of deployed identity."""
    plan = _plan()
    helper = next(
        action for action in plan.actions if action.id == "helper:host_f"
    )
    assert helper.argv == [
        "admin",
        "update",
        "host_f",
        "--expected-sha",
        VQ,
        "--drain-wait",
        "4h",
    ]
    assert helper.target_sha == VQ


def test_ahead_vq_lanes_block_as_release_drift() -> None:
    """A deployed vq ahead of the accepted pin is explicit release drift.

    'No downgrade' must never silently coexist with staging helpers from
    the ahead tree: the vq lanes fail closed with a precise recovery,
    while chemistry lanes keep the plain no-downgrade skip.
    """
    admin, programs, doctor = _snapshots(
        local_sha=FUTURE, release_tag="v0.15.61"
    )
    plan = _plan(snapshots=(admin, programs, doctor))

    driver = next(
        action
        for action in plan.actions
        if action.id == "driver:localhost:vibeqc-queue"
    )
    assert driver.decision == "block"
    assert "release drift" in driver.reason
    assert "cut the next release" in driver.reason
    assert f"--expected-sha {VQ}" in driver.reason

    helper = next(
        action for action in plan.actions if action.id == "helper:host_f"
    )
    assert helper.decision == "block"
    assert "release drift" in helper.reason
    assert f"vq admin update host_f --expected-sha {VQ}" in helper.reason

    local_vq = next(
        action
        for action in plan.actions
        if action.id == "local-runtime:host_d:vibeqc-queue"
    )
    assert local_vq.decision == "block"
    assert "release drift" in local_vq.reason

    chemistry = next(
        action
        for action in plan.actions
        if action.id == "local-runtime:host_d:vibeqc-release"
    )
    assert chemistry.decision == "skip"
    assert "no downgrade" in chemistry.reason
    assert plan.has_blocks


def test_helper_at_pin_with_canonical_record_is_a_noop() -> None:
    """A helper standing at the accepted pin must never look 'not
    deployed', even when the doctor's helper-vs-driver comparison fails
    because the driver checkout moved (the 2026-07-25 failure mode)."""
    admin, programs, doctor = _snapshots(local_sha=VQ)
    admin["host_f"]["helper"] = {
        "configured": True,
        "last": {
            "actual_sha": VQ,
            "last_success": True,
            "healthy": True,
            "metrics": {"dependency_cache": "not-applicable"},
        },
    }
    doctor["host_f"] = {
        "ok": False,
        "checks": [
            {
                "name": "scheduler_remote_vq",
                "ok": False,
                "source_sha": VQ,
                "message": (
                    "source-tree SHA-256 mismatch: helper aaaa, driver bbbb"
                ),
            },
            {"name": "scheduler_liveness", "ok": True},
        ],
    }
    plan = _plan(snapshots=(admin, programs, doctor))
    helper = next(
        action for action in plan.actions if action.id == "helper:host_f"
    )
    assert helper.decision == "skip"
    assert helper.reason == "already at target with LAST OK=true"
    assert helper.before["metrics"] == {"dependency_cache": "not-applicable"}


def test_helper_record_and_live_probe_disagreement_updates() -> None:
    """A canonical record at the pin is not sufficient when the live
    provenance probe reads a different identity."""
    admin, programs, doctor = _snapshots(local_sha=VQ)
    admin["host_f"]["helper"] = {
        "configured": True,
        "last": {"actual_sha": VQ, "last_success": True},
    }
    doctor["host_f"]["checks"][0] = {
        "name": "scheduler_remote_vq",
        "ok": False,
        "source_sha": OLD,
        "message": f"SOURCE-SHA mismatch: helper {OLD}, driver {VQ}",
    }
    plan = _plan(snapshots=(admin, programs, doctor))
    helper = next(
        action for action in plan.actions if action.id == "helper:host_f"
    )
    assert helper.decision == "update"


def test_result_payload_reports_metrics_and_durations() -> None:
    """Per-action evidence: deploy metrics ride the lane state and the
    wall-clock duration rides the journal."""
    admin, programs, doctor = _snapshots(local_sha=VQ)
    admin["host_f"]["deployments"]["vibeqc-release"]["last"]["metrics"] = {
        "dependency_cache": "reused",
        "compiler_cache_hit_rate_percent": "98.7",
        "native_deps_rebuilt": "no",
        "build_install_seconds": "212",
    }
    plan = _plan(snapshots=(admin, programs, doctor))
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-aaaaaaaaaaaa",
        report_digest_sha256="a" * 64,
        report_source_path="vibe-queue/releases/v0.15.60.json",
        actions={
            "scheduler-runtime:host_f:vibeqc-release": {
                "status": "success",
                "duration_seconds": 187.5,
            }
        },
    )
    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=doctor,
        run=run,
    )
    lane = next(
        item
        for item in payload["lanes"]
        if item["host"] == "host_f" and item["module"] == "vibeqc-release"
    )
    assert lane["metrics"]["dependency_cache"] == "reused"
    assert lane["metrics"]["compiler_cache_hit_rate_percent"] == "98.7"
    assert lane["metrics"]["native_deps_rebuilt"] == "no"
    assert lane["duration_seconds"] == 187.5


def test_target_and_last_ok_is_idempotent_but_mark_ok_is_not() -> None:
    admin, programs, doctor = _converged_snapshots()

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    assert all(action.decision == "skip" for action in plan.actions)
    assert plan.updates == []

    admin["host_d"]["envs"][0]["last_marked_ok_at"] = "2026-07-25T00:00:00Z"
    marked_plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    action = next(
        item
        for item in marked_plan.actions
        if item.id == "local-runtime:host_d:vibeqc-queue"
    )
    assert action.decision == "update"
    assert "human-acknowledged" in action.reason


def test_no_downgrade_and_divergence_fail_closed() -> None:
    admin, programs, doctor = _snapshots(local_sha=FUTURE, release_tag="v0.15.61")
    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    release = next(
        action
        for action in plan.actions
        if action.id == "local-runtime:host_d:vibeqc-release"
    )
    dev = next(
        action
        for action in plan.actions
        if action.id == "local-runtime:host_d:vibeqc-dev"
    )
    assert release.decision == "skip"
    assert "no downgrade" in release.reason
    assert dev.decision == "skip"

    programs["host_d"][1]["current_git_sha_full"] = "f" * 40
    admin["host_d"]["envs"][1]["last_tag"] = "v0.15.60"
    divergent = _plan(snapshots=(admin, programs, doctor))
    release = next(
        action
        for action in divergent.actions
        if action.id == "local-runtime:host_d:vibeqc-release"
    )
    assert release.decision == "block"
    assert "same release version" in release.reason


def test_failed_requested_tag_does_not_override_live_release_identity() -> None:
    """A rolled-back update's requested tag is not the checkout's tag."""
    admin, programs, doctor = _snapshots(
        local_sha=OLD,
        release_tag="v0.15.59",
    )
    release_env = next(
        env
        for env in admin["host_d"]["envs"]
        if env["name"] == "vibeqc-release"
    )
    release_program = next(
        program
        for program in programs["host_d"]
        if program["name"] == "vibeqc-release"
    )
    release_env["last_success"] = False
    release_env["last_tag"] = "v0.15.60"
    release_env["current_describe"] = "v0.15.59"
    release_program["current_git_describe"] = "v0.15.59"

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    action = next(
        item
        for item in plan.actions
        if item.id == "local-runtime:host_d:vibeqc-release"
    )

    assert action.before["current_tag"] == "v0.15.59"
    assert action.decision == "update"
    assert action.reason == "target is newer"


def test_verify_rejects_ahead_descendant_with_stale_installed_provenance(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A valid descendant SHA cannot hide an installed/checkout mismatch."""
    admin, programs, doctor = _converged_snapshots()
    dev_env = next(
        env
        for env in admin["host_d"]["envs"]
        if env["name"] == "vibeqc-dev"
    )
    dev_env["installed_sha_matches_checkout"] = False
    dev_program = next(
        program
        for program in programs["host_d"]
        if program["name"] == "vibeqc-dev"
    )
    dev_program["current_git_sha_full"] = FUTURE

    plan = _plan(snapshots=(admin, programs, doctor))
    action = next(
        item
        for item in plan.actions
        if item.id == "local-runtime:host_d:vibeqc-dev"
    )

    assert _ancestry("dev", DEV, FUTURE) is True
    assert action.before["last_ok"] is False
    assert action.decision == "update"
    assert "repair that descendant without downgrade" in action.reason
    assert action.target_sha == FUTURE
    assert action.target_tag is None
    assert action.target_version is None
    assert plan.report["pins"]["dev"] == {
        "sha": DEV,
        "version": "0.15.61.dev0",
        "tag": None,
        "gating_job": "build-test",
        "pipeline_id": 4500,
        "evidence_sha": DEV,
        "acceptance_rule": "A",
    }
    assert action.argv == [
        "admin",
        "update",
        "vibeqc-dev",
        "host_d",
        "--expected-sha",
        FUTURE,
    ]

    payload = fleet_rollout.verify_payload(
        plan=plan,
        doctor=doctor,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )
    assert payload["verdict"] == "degraded"
    reported_lane = next(
        lane
        for lane in payload["lanes"]
        if lane["host"] == "host_d" and lane["module"] == "vibeqc-dev"
    )
    assert reported_lane["target_sha"] == FUTURE
    assert reported_lane["target_tag"] is None
    assert reported_lane["target_version"] is None
    assert any(
        "local-runtime:host_d:vibeqc-dev: update" in reason
        for reason in payload["degraded_hosts"]["host_d"]
    )

    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run = fleet_rollout.execute_one(
        plan,
        action,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
        runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout="ok\n", stderr=""
        ),
    )
    journaled = run.actions[action.id]
    assert journaled["target_sha"] == FUTURE
    assert journaled["target_tag"] is None
    assert journaled["target_version"] is None

    dev_env["installed_sha_matches_checkout"] = True
    verification = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    verified_action = next(
        item
        for item in verification.actions
        if item.id == "local-runtime:host_d:vibeqc-dev"
    )
    assert verified_action.decision == "skip"
    assert verified_action.target_sha == DEV
    completed = fleet_rollout.result_payload(
        initial=plan,
        verification=verification,
        doctor=doctor,
        run=run,
    )
    completed_lane = next(
        lane
        for lane in completed["lanes"]
        if lane["host"] == "host_d" and lane["module"] == "vibeqc-dev"
    )
    assert completed_lane["changed"] is True
    assert completed_lane["target_sha"] == FUTURE
    assert completed_lane["target_tag"] is None
    assert completed_lane["target_version"] is None

    def unexpected_update(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        raise AssertionError("a healthy-ahead recovery must not execute an update")

    resumed = fleet_rollout.execute_plan(
        verification,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
        runner=unexpected_update,
    )
    resumed_entry = resumed.actions[action.id]
    assert resumed_entry["status"] == "success"
    assert resumed_entry["target_sha"] == FUTURE
    assert resumed_entry["verified_reason"] == (
        "newer descendant already deployed; no downgrade"
    )


def test_newer_release_with_false_canonical_last_ok_is_repaired() -> None:
    """Release semver no-downgrade cannot hide a failed deploy record."""
    admin, programs, doctor = _converged_snapshots()
    release_env = next(
        env
        for env in admin["host_d"]["envs"]
        if env["name"] == "vibeqc-release"
    )
    release_env["last_success"] = False
    release_env["last_tag"] = "v0.15.61"
    release_program = next(
        program
        for program in programs["host_d"]
        if program["name"] == "vibeqc-release"
    )
    release_program["current_git_sha_full"] = FUTURE
    release_program["current_git_describe"] = "v0.15.61"
    release_program["import_version"] = "0.15.61"
    release_env["current_describe"] = "v0.15.61"

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    action = next(
        item
        for item in plan.actions
        if item.id == "local-runtime:host_d:vibeqc-release"
    )

    assert action.before["last_ok"] is False
    assert action.decision == "update"
    assert "lacks verified deployment provenance" in action.reason
    assert action.target_sha == FUTURE
    assert action.target_tag == "v0.15.61"
    assert action.target_version == "0.15.61"
    assert plan.report["pins"]["release"]["sha"] == RELEASE
    assert plan.report["pins"]["release"]["tag"] == "v0.15.60"
    assert plan.report["pins"]["release"]["version"] == "0.15.60"
    assert action.argv == [
        "admin",
        "update",
        "vibeqc-release",
        "host_d",
        "--tag",
        "v0.15.61",
        "--expected-sha",
        FUTURE,
    ]


def test_healthy_ahead_descendant_keeps_the_no_downgrade_skip() -> None:
    """A proven healthy descendant remains converged and is never downgraded."""
    admin, programs, doctor = _converged_snapshots()
    dev_program = next(
        program
        for program in programs["host_d"]
        if program["name"] == "vibeqc-dev"
    )
    dev_program["current_git_sha_full"] = FUTURE

    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    action = next(
        item
        for item in plan.actions
        if item.id == "local-runtime:host_d:vibeqc-dev"
    )

    assert action.before["last_ok"] is True
    assert action.decision == "skip"
    assert action.reason == "newer descendant already deployed; no downgrade"
    assert fleet_rollout.verify_payload(
        plan=plan,
        doctor=doctor,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )["verdict"] == "converged"


def test_external_scheduler_hold_and_marker_are_host_scoped() -> None:
    admin, programs, doctor = _snapshots()
    doctor["host_f"]["ok"] = False
    doctor["host_f"]["checks"][1] = {
        "name": "scheduler_liveness",
        "ok": False,
        "message": "PBS server is administratively paused",
        "scheduler_dispatch": {"dispatching_new_jobs": False},
    }
    plan = _plan(snapshots=(admin, programs, doctor))

    assert all(
        action.decision == "defer"
        for action in plan.actions
        if action.host == "host_f"
    )
    assert all(
        action.decision == "update"
        for action in plan.actions
        if action.host == "host_d"
    )

    doctor["host_f"] = {
        "ok": False,
        "checks": [
            {
                "name": "scheduler_remote_vq",
                "ok": False,
                "source_sha": OLD,
            },
            {"name": "scheduler_liveness", "ok": True},
        ],
    }
    # A scheduler host has no daemon of its own, so its status payload carries
    # the DRIVER's marker -- and the driver is localhost, so the same marker
    # lands in localhost's payload too. Earlier fixtures put the marker in one
    # host only, which is not a shape collect_snapshots can produce.
    driver_marker = {
        "envs": ["scheduler-runtime:host_f:vibeqc-release"],
        "host": "host_f",
        "stale_reason": None,
    }
    admin["host_f"]["marker"] = driver_marker
    admin["localhost"]["marker"] = driver_marker
    marker_plan = _plan(snapshots=(admin, programs, doctor))
    host_f = {
        action.id: action.decision
        for action in marker_plan.actions
        if action.host == "host_f"
    }
    assert host_f["helper:host_f"] == "defer"
    assert host_f["scheduler-runtime:host_f:vibeqc-release"] == "defer"
    assert host_f["scheduler-runtime:host_f:vibeqc-dev"] == "update"
    assert host_f["scheduler-runtime:host_f:vibe-view"] == "update"
    # host_d delegates remotely and returns its own (absent) marker, so an
    # independent host keeps making progress.
    assert all(
        action.decision == "update"
        for action in marker_plan.actions
        if action.host == "host_d"
    )


def test_live_host_f_helper_marker_does_not_defer_local_or_other_host_lanes() -> None:
    """The live rollout shape: one driver marker with envs=scheduler:host_f."""
    admin, programs, doctor = _snapshots()
    marker = {
        "envs": ["scheduler:host_f"],
        "host": "host_f",
        "stale_reason": None,
    }
    admin["host_f"]["marker"] = marker
    admin["localhost"]["marker"] = marker

    plan = _plan(snapshots=(admin, programs, doctor))

    assert all(
        action.decision == "defer"
        for action in plan.actions
        if action.host == "host_f"
    )
    assert all(
        action.decision == "update"
        for action in plan.actions
        if action.host in {"localhost", "host_d"}
    )


def test_marker_taken_for_another_host_is_not_attributed_to_this_one() -> None:
    """Regression: the driver's marker must name its own target, not the host
    whose payload happened to carry it.

    Pre-fix this read "active update marker on host_f
    (envs=scheduler-runtime:othercluster:vibeqc-dev)" -- a different cluster's
    update reported against host_f, indistinguishable from a scope leak.
    """
    admin, programs, doctor = _snapshots()
    foreign_marker = {
        "envs": ["scheduler-runtime:othercluster:vibeqc-dev"],
        "host": "othercluster",
        "stale_reason": None,
    }
    admin["host_f"]["marker"] = foreign_marker
    admin["localhost"]["marker"] = foreign_marker

    plan = _plan(snapshots=(admin, programs, doctor))
    host_f = [action for action in plan.actions if action.host == "host_f"]
    assert host_f
    for action in host_f:
        assert action.decision == "update"
        assert "othercluster" not in action.reason


def test_stale_marker_defer_says_it_is_stale() -> None:
    admin, programs, doctor = _snapshots()
    admin["host_f"]["marker"] = {
        "envs": ["scheduler-runtime:host_f:vibeqc-dev"],
        "host": "host_f",
        "stale_reason": "pid 4242 is gone",
    }

    plan = _plan(snapshots=(admin, programs, doctor))
    host_f = {
        action.id: action
        for action in plan.actions
        if action.host == "host_f"
    }
    assert host_f["helper:host_f"].decision == "defer"
    assert "marker is stale (pid 4242 is gone)" in host_f["helper:host_f"].reason
    assert host_f["scheduler-runtime:host_f:vibeqc-dev"].decision == "defer"
    assert host_f["scheduler-runtime:host_f:vibeqc-release"].decision == "update"
    assert host_f["scheduler-runtime:host_f:vibe-view"].decision == "update"


PROBE_FAILURE = (
    "remote vq failed (exit 255) on host_d:\n  "
    "stderr: ssh: connect to host host_d port 22: Operation timed out"
)


def test_unreachable_local_host_defers_instead_of_reading_as_no_lane() -> None:
    """Regression: a failed status probe must not look like "no managed lane".

    ``vq admin status --all --json`` isolates a per-host failure as
    ``{"error": ...}``. Pre-fix every lane-state reader found no ``envs`` and
    returned ``configured=False``, so all of the host's lanes planned as
    ``skip`` "no managed local venv lane" -- a silent skip of a host that is
    merely unreachable, with ``--dry-run`` exiting 0 and never naming it.
    """
    admin, programs, doctor = _snapshots()
    admin["host_d"] = {"error": PROBE_FAILURE}

    plan = _plan(snapshots=(admin, programs, doctor))
    host_d = [action for action in plan.actions if action.host == "host_d"]
    assert len(host_d) == len(fleet_rollout.LOCAL_PROGRAM_ORDER)
    for action in host_d:
        assert action.decision == "defer"
        assert action.reason.startswith("host status probe failed:")
        assert "Operation timed out" in action.reason
        assert "no managed local venv lane" not in action.reason
    # A down host defers; it must not block the independent hosts.
    assert not plan.has_blocks
    assert plan.has_deferred
    assert all(
        action.decision == "update"
        for action in plan.actions
        if action.host == "host_f"
    )


def test_unreachable_scheduler_host_defers_helper_and_runtime_lanes() -> None:
    """The helper lane is the dangerous one: it takes ``configured`` from
    config, not from the payload, so pre-fix an unreachable scheduler host
    still planned a real helper *update* against it."""
    admin, programs, doctor = _snapshots()
    admin["host_f"] = {"error": PROBE_FAILURE.replace("host_d", "host_f")}

    plan = _plan(snapshots=(admin, programs, doctor))
    host_f = [action for action in plan.actions if action.host == "host_f"]
    assert {action.phase for action in host_f} == {"helper", "scheduler-runtime"}
    for action in host_f:
        assert action.decision == "defer"
        assert action.reason.startswith("host status probe failed:")
    assert not plan.has_blocks
    assert all(
        action.decision == "update"
        for action in plan.actions
        if action.host == "host_d"
    )


def test_probe_failure_keeps_a_real_rollout_from_reporting_complete() -> None:
    """A deferred lane must not read as a converged fleet.

    This already held pre-fix (``doctor_failures`` forces ``blocked``); it is
    pinned here so the safety net cannot regress alongside the reason strings.
    """
    admin, programs, doctor = _snapshots()
    admin["host_d"] = {"error": PROBE_FAILURE}

    plan = _plan(snapshots=(admin, programs, doctor))
    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=doctor,
        run=None,
    )
    assert payload["status"] != "complete"


def test_unresolved_legacy_host_blocks_execution_plan() -> None:
    plan = _plan(cfg=_config(include_unresolved=True))

    assert plan.has_blocks
    assert plan.topology_errors == [
        "mystery: auto: no managed lane found; declare vq-only/excluded"
    ]
    assert not any(action.host == "mystery" for action in plan.actions)


def _read_only_drain_status(
    *,
    policy: dict[str, Any] | None = None,
    leases: list[dict[str, Any]] | None = None,
    coverage: dict[str, bool] | None = None,
    safety_fail_closed: bool = False,
    observed_at: str = "2026-08-11T12:00:00+00:00",
) -> dict[str, Any]:
    resolved_coverage = coverage or {
        "legacy_state": True,
        "scheduler_leases": True,
    }
    resolved_leases = leases or []
    if policy is None and resolved_leases:
        policy = {
            "active": True,
            "is_full_drain": False,
            "max_jobs": None,
            "max_cpus": None,
            "reason": None,
            "set_at": min(str(item["set_at"]) for item in resolved_leases),
            "scheduler_hosts": sorted(
                {str(item["scheduler_host"]) for item in resolved_leases}
            ),
            "legacy_scheduler_hosts": [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    if safety_fail_closed:
        policy = {
            "active": True,
            "is_full_drain": True,
            "max_jobs": None,
            "max_cpus": None,
            "reason": "scheduler drain inventory unreadable; dispatch held safe",
            "set_at": observed_at,
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": True,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    return {
        "schema": "vq.drain.read_only_status/1",
        "observed_at": observed_at,
        "provenance": {
            "method": "get_drain_read_only_snapshot",
            "version": "0.25.0",
            "source_sha": "a" * 40,
            "source_tree_sha256": "b" * 64,
            "multi_user": False,
        },
        "coverage": resolved_coverage,
        "active": policy is not None,
        "policy": policy,
        "scheduler_leases": resolved_leases,
        "safety_fail_closed": safety_fail_closed,
    }


def test_final_drain_liveness_queries_each_control_once_and_projects_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(
        hosts={
            "driver": config.HostConfig(ssh="driver", fleet_role="managed"),
            "host_f": _scheduler_host(ssh="host_f"),
            "host_f-big": _scheduler_host(
                ssh="host_f",
                role="alias",
                canonical="host_f",
            ),
            "worker": config.HostConfig(ssh="worker", fleet_role="managed"),
        }
    )
    cfg.hosts["host_f"].scheduler_driver = "driver"
    cfg.hosts["host_f-big"].scheduler_driver = "driver"
    topology = {
        "driver": {
            "name": "driver",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
        "host_f": {
            "name": "host_f",
            "role": "managed",
            "canonical_host": None,
            "reason": "administratively down, but driver remains live",
        },
        "host_f-big": {
            "name": "host_f-big",
            "role": "alias",
            "canonical_host": "host_f",
            "reason": "explicit alias",
        },
        "worker": {
            "name": "worker",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
    }
    calls: list[list[str]] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        calls.append(args)
        control = args[-1]
        payload = (
            _read_only_drain_status(
                leases=[
                    {
                        "lease_id": "operator-host_f",
                        "scheduler_host": "host_f",
                        "owner": "operator",
                        "set_at": "2026-08-11T11:00:00+00:00",
                        "reason": "cooling maintenance",
                    }
                ]
            )
            if control == "driver"
            else _read_only_drain_status()
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    monkeypatch.setattr(
        fleet_rollout,
        "utcnow_iso",
        lambda: "2026-08-11T12:00:01+00:00",
        raising=False,
    )
    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert calls == [
        ["drain", "--status", "--json", "--read-only-snapshot", "driver"],
        ["drain", "--status", "--json", "--read-only-snapshot", "worker"],
    ]
    assert result["status"] == "complete"
    assert result["inactive_hosts"] == ["driver", "host_f-big", "worker"]
    assert [hold["host"] for hold in result["active_holds"]] == [
        "host_f",
    ]
    assert all(
        hold["kind"] == "scheduler-target"
        and hold["owner_class"] == "external"
        for hold in result["active_holds"]
    )
    assert result["unknown_hosts"] == []
    assert {item["host"] for item in result["observed_hosts"]} == {
        "driver",
        "host_f",
        "host_f-big",
        "worker",
    }


def test_final_drain_liveness_uses_each_scheduler_alias_own_driver() -> None:
    cfg = config.Config(
        hosts={
            "driver-a": config.HostConfig(ssh="driver-a", fleet_role="managed"),
            "driver-b": config.HostConfig(ssh="driver-b", fleet_role="managed"),
            "host_f": _scheduler_host(ssh="host_f"),
            "host_f-big": _scheduler_host(
                ssh="host_f",
                role="alias",
                canonical="host_f",
            ),
        }
    )
    cfg.hosts["host_f"].scheduler_driver = "driver-a"
    cfg.hosts["host_f-big"].scheduler_driver = "driver-b"
    topology = {
        name: {
            "name": name,
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
        for name in ("driver-a", "driver-b", "host_f")
    }
    topology["host_f-big"] = {
        "name": "host_f-big",
        "role": "alias",
        "canonical_host": "host_f",
        "reason": "explicit alias",
    }
    calls: list[str] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        control = argv[-1]
        calls.append(control)
        target = "host_f" if control == "driver-a" else "host_f-big"
        payload = _read_only_drain_status(
            leases=[
                {
                    "lease_id": f"lease-{target}",
                    "scheduler_host": target,
                    "owner": "operator",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "reason": "maintenance",
                }
            ]
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert calls == ["driver-a", "driver-b"]
    assert [hold["host"] for hold in result["active_holds"]] == [
        "host_f",
        "host_f-big",
    ]


def test_final_drain_liveness_projects_partial_to_local_alias_only() -> None:
    cfg = config.Config(
        hosts={
            "primary": config.HostConfig(ssh="primary", fleet_role="managed"),
            "local-alias": config.HostConfig(
                ssh="primary",
                fleet_role="alias",
                fleet_canonical_host="primary",
            ),
            "scheduler-alias": _scheduler_host(
                ssh="primary",
                role="alias",
                canonical="primary",
            ),
        }
    )
    cfg.hosts["scheduler-alias"].scheduler_driver = "primary"
    topology = {
        "primary": {
            "name": "primary",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
        "local-alias": {
            "name": "local-alias",
            "role": "alias",
            "canonical_host": "primary",
            "reason": "explicit alias",
        },
        "scheduler-alias": {
            "name": "scheduler-alias",
            "role": "alias",
            "canonical_host": "primary",
            "reason": "explicit alias",
        },
    }
    payload = _read_only_drain_status(
        policy={
            "active": True,
            "is_full_drain": False,
            "max_jobs": 0,
            "max_cpus": None,
            "reason": "local maintenance",
            "set_at": "2026-08-11T11:00:00+00:00",
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    )

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert [hold["host"] for hold in result["active_holds"]] == [
        "local-alias",
        "primary",
    ]
    assert result["inactive_hosts"] == ["scheduler-alias"]


def test_final_drain_liveness_queries_unresolved_and_excluded_scheduler_targets(
    ) -> None:
    cfg = config.Config(
        hosts={
            "driver": config.HostConfig(ssh="driver", fleet_role="managed"),
            "excluded": _scheduler_host(ssh="excluded", role="excluded"),
            "unresolved": _scheduler_host(ssh="unresolved", role="auto"),
        }
    )
    cfg.hosts["excluded"].scheduler_driver = "driver"
    cfg.hosts["unresolved"].scheduler_driver = "driver"
    topology = {
        "driver": {
            "name": "driver",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
        "excluded": {
            "name": "excluded",
            "role": "excluded",
            "canonical_host": None,
            "reason": "not a rollout lane",
        },
        "unresolved": {
            "name": "unresolved",
            "role": "unresolved",
            "canonical_host": None,
            "reason": "not classified for rollout",
        },
    }
    calls: list[str] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        calls.append(argv[-1])
        payload = _read_only_drain_status(
            leases=[
                {
                    "lease_id": f"lease-{host}",
                    "scheduler_host": host,
                    "owner": "operator",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "reason": "maintenance",
                }
                for host in ("excluded", "unresolved")
            ]
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert calls == ["driver"]
    assert [hold["host"] for hold in result["active_holds"]] == [
        "excluded",
        "unresolved",
    ]


def test_final_drain_liveness_supports_implicit_localhost_scheduler_driver() -> None:
    cfg = config.Config(
        hosts={"cluster": _scheduler_host(ssh="cluster")}
    )
    topology = {
        "cluster": {
            "name": "cluster",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
    }
    calls: list[str] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        calls.append(argv[-1])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_read_only_drain_status()),
            stderr="",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert calls == ["localhost"]
    assert result["inactive_hosts"] == ["cluster"]


def test_final_drain_liveness_queries_shared_local_endpoint_once() -> None:
    cfg = config.Config(
        hosts={
            "driver": config.HostConfig(ssh="localhost", fleet_role="managed"),
            "named-target": _scheduler_host(ssh="named-target"),
            "implicit-target": _scheduler_host(ssh="implicit-target"),
        }
    )
    cfg.hosts["named-target"].scheduler_driver = "driver"
    topology = {
        name: {
            "name": name,
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
        for name in ("driver", "named-target", "implicit-target")
    }
    calls: list[str] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        calls.append(argv[-1])
        payload = _read_only_drain_status(
            leases=[
                {
                    "lease_id": f"lease-{host}",
                    "scheduler_host": host,
                    "owner": "operator",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "reason": "maintenance",
                }
                for host in ("implicit-target", "named-target")
            ]
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert calls == ["localhost"]
    holds = {hold["host"]: hold for hold in result["active_holds"]}
    assert holds["named-target"]["control_host"] == "driver"
    assert holds["implicit-target"]["control_host"] == "localhost"


def test_final_drain_liveness_normalizes_policy_and_unknown_coverage() -> None:
    cfg = config.Config(
        hosts={
            "held": config.HostConfig(ssh="held", fleet_role="managed"),
            "unsafe": config.HostConfig(ssh="unsafe", fleet_role="managed"),
            "mystery": config.HostConfig(ssh="mystery", fleet_role="auto"),
        }
    )
    topology = {
        "held": {
            "name": "held",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
        "unsafe": {
            "name": "unsafe",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        },
        "mystery": {
            "name": "mystery",
            "role": "unresolved",
            "canonical_host": None,
            "reason": "ambiguous",
        },
    }

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        control = argv[-1]
        if control == "held":
            payload = _read_only_drain_status(
                policy={
                    "active": True,
                    "is_full_drain": False,
                    "max_jobs": 0,
                    "max_cpus": None,
                    "reason": "bounded maintenance",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "scheduler_hosts": [],
                    "legacy_scheduler_hosts": [],
                    "full_dispatch": False,
                    "reject_submits": True,
                    "update_mode": "deny",
                    "duration_seconds": None,
                }
            )
        elif control == "unsafe":
            payload = _read_only_drain_status(
                coverage={"legacy_state": True, "scheduler_leases": False},
                safety_fail_closed=True,
            )
        else:
            payload = _read_only_drain_status()
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="must not leak",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert result["status"] == "partial"
    assert [hold["kind"] for hold in result["active_holds"]] == [
        "partial",
        "submit-deny",
        "safety-fail-closed",
    ]
    assert result["active_holds"][-1]["owner_class"] == "safety-fail-closed"
    assert {item["host"] for item in result["unknown_hosts"]} == {"unsafe"}
    assert "mystery" in result["inactive_hosts"]
    assert "must not leak" not in json.dumps(result)


def test_final_drain_liveness_requires_supported_provenance_for_inactive() -> None:
    cfg = config.Config(
        hosts={"worker": config.HostConfig(ssh="worker", fleet_role="managed")}
    )
    topology = {
        "worker": {
            "name": "worker",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
    }
    payload = _read_only_drain_status()
    payload["provenance"]["source_sha"] = None

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="old daemon detail",
        )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert result["status"] == "unavailable"
    assert result["inactive_hosts"] == []
    assert result["observed_hosts"] == []
    assert result["unknown_hosts"] == [
        {
            "host": "worker",
            "control_host": "worker",
            "reason": "unsupported or malformed read-only drain snapshot",
        }
    ]
    assert "old daemon detail" not in json.dumps(result)


def test_final_drain_liveness_rejects_nonzero_and_malformed_observations() -> None:
    cfg = config.Config(
        hosts={"worker": config.HostConfig(ssh="worker", fleet_role="managed")}
    )
    topology = {
        "worker": {
            "name": "worker",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
    }
    contradictory = _read_only_drain_status(
        policy={
            "active": True,
            "is_full_drain": False,
            "max_jobs": None,
            "max_cpus": None,
            "reason": "forged",
            "set_at": "2026-08-11T11:00:00+00:00",
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": True,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    )
    cases = [
        (2, json.dumps(_read_only_drain_status())),
        (255, json.dumps(_read_only_drain_status())),
        (0, '{"error":"old daemon"}'),
        (0, '{"schema":"x","schema":"x"}'),
        (0, '{"schema":NaN}'),
        (0, '{"x":' + "[" * 40 + "0" + "]" * 40 + "}"),
        (0, json.dumps(contradictory)),
    ]

    for returncode, stdout in cases:
        def runner(
            argv: list[str],
            _returncode: int = returncode,
            _stdout: str = stdout,
            **kwargs: Any,
        ) -> subprocess.CompletedProcess[str]:
            del kwargs
            return subprocess.CompletedProcess(
                argv,
                _returncode,
                stdout=_stdout,
                stderr="secret transport detail",
            )

        result = fleet_rollout.collect_final_drain_liveness(
            cfg,
            topology=topology,
            run=None,
            runner=runner,
        )

        assert result["status"] == "unavailable"
        assert result["inactive_hosts"] == []
        assert "secret transport detail" not in json.dumps(result)


def test_final_drain_liveness_contains_runner_timeout_as_unknown() -> None:
    cfg = config.Config(
        hosts={"worker": config.HostConfig(ssh="worker", fleet_role="managed")}
    )
    topology = {
        "worker": {
            "name": "worker",
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
    }

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        raise subprocess.TimeoutExpired(argv, 0.01, stderr="secret")

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=runner,
    )

    assert result["status"] == "unavailable"
    assert result["inactive_hosts"] == []
    assert "secret" not in json.dumps(result)


def test_final_drain_liveness_classifies_exact_legacy_hold_as_rollout() -> None:
    set_at = "2026-08-11T11:00:00+00:00"
    reason = "vq rollout-latest v0.15.60-owned"
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-owned",
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "host_f": {
                "host": "host_f",
                "kind": "scheduler-target",
                "owned": True,
                "status": "active",
                "reason": reason,
                "control_host": "driver",
                "legacy_expected_set_at": set_at,
            }
        },
    )

    assert fleet_rollout._live_hold_owner_class(
        run,
        host="host_f",
        control_host="driver",
        kind="scheduler-target",
        owner=None,
        reason=reason,
        set_at=set_at,
    ) == "rollout"


@pytest.mark.parametrize("projected_kind", ["partial", "submit-deny"])
def test_final_drain_liveness_does_not_relabel_non_full_policy_from_full_journal(
    projected_kind: str,
) -> None:
    set_at = "2026-08-11T11:00:00+00:00"
    reason = "vq rollout-latest v0.15.60-owned"
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-owned",
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "driver": {
                "host": "driver",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": reason,
                "control_host": "driver",
                "set_at": set_at,
            }
        },
    )

    assert fleet_rollout._live_hold_owner_class(
        run,
        host="driver",
        control_host="driver",
        kind=projected_kind,
        owner=None,
        reason=reason,
        set_at=set_at,
    ) == "external"


def test_final_drain_liveness_projects_exact_driver_full_owner_to_all_targets(
    ) -> None:
    reason = "vq rollout-latest v0.15.60-owned"
    set_at = "2026-08-11T11:00:00+00:00"
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-owned",
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "driver": {
                "host": "driver",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": reason,
                "control_host": "driver",
                "set_at": set_at,
            }
        },
    )

    hold = fleet_rollout._final_liveness_hold(
        run,
        host="host_f",
        target="host_f",
        control_host="driver",
        remote_observed_at="2026-08-11T12:00:00+00:00",
        controller_observed_at="2026-08-11T12:00:01+00:00",
        kind="full",
        reason=reason,
        identity_reason=reason,
        set_at=set_at,
    )

    assert hold["owner_class"] == "rollout"


def test_final_drain_liveness_uses_unsanitized_reason_for_owner_identity() -> None:
    reason = "vq rollout-latest v0.15.60-owned"
    set_at = "2026-08-11T11:00:00+00:00"
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-owned",
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "driver": {
                "host": "driver",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": reason,
                "control_host": "driver",
                "set_at": set_at,
            }
        },
    )

    hold = fleet_rollout._final_liveness_hold(
        run,
        host="driver",
        target="driver",
        control_host="driver",
        remote_observed_at="2026-08-11T12:00:00+00:00",
        controller_observed_at="2026-08-11T12:00:01+00:00",
        kind="full",
        reason=reason,
        identity_reason="vq\nrollout-latest v0.15.60-owned",
        set_at=set_at,
    )

    assert hold["owner_class"] == "external"
    assert hold["reason"] == reason


def test_final_drain_liveness_accepts_bounded_long_reason_and_caps_display() -> None:
    reason = "r" * 600
    payload = _read_only_drain_status(
        policy={
            "active": True,
            "is_full_drain": False,
            "max_jobs": 0,
            "max_cpus": None,
            "reason": reason,
            "set_at": "2026-08-11T11:00:00+00:00",
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    )

    parsed = fleet_rollout._validate_final_drain_status(payload)

    assert parsed["policy"]["identity_reason"] == reason
    assert parsed["policy"]["reason"] == "r" * 512


def test_final_drain_liveness_accepts_producer_bounded_long_timestamp() -> None:
    set_at = "2026-08-11T11:00:00." + "1" * 600 + "+00:00"
    payload = _read_only_drain_status(
        policy={
            "active": True,
            "is_full_drain": False,
            "max_jobs": 0,
            "max_cpus": None,
            "reason": "maintenance",
            "set_at": set_at,
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    )

    parsed = fleet_rollout._validate_final_drain_status(payload)
    hold = fleet_rollout._final_liveness_hold(
        None,
        host="driver",
        target="driver",
        control_host="driver",
        remote_observed_at=set_at,
        controller_observed_at=set_at,
        kind="partial",
        reason="maintenance",
        identity_reason="maintenance",
        set_at=set_at,
    )

    assert parsed["policy"]["set_at"] == set_at
    assert len(hold["remote_observed_at"]) == 512


def test_final_drain_liveness_accepts_exact_leases_only_projection() -> None:
    leases = [
        {
            "lease_id": "lease-b",
            "scheduler_host": "b",
            "owner": "operator-b",
            "set_at": "2026-08-11T11:02:00+00:00",
            "reason": "b",
        },
        {
            "lease_id": "lease-a",
            "scheduler_host": "a",
            "owner": "operator-a",
            "set_at": "2026-08-11T11:01:00+00:00",
            "reason": "a",
        },
    ]
    payload = _read_only_drain_status(
        coverage={"legacy_state": False, "scheduler_leases": True},
        leases=leases,
        policy={
            "active": True,
            "is_full_drain": False,
            "max_jobs": None,
            "max_cpus": None,
            "reason": None,
            "set_at": "2026-08-11T11:01:00+00:00",
            "scheduler_hosts": ["a", "b"],
            "legacy_scheduler_hosts": [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        },
    )

    parsed = fleet_rollout._validate_final_drain_status(payload)

    assert [lease["scheduler_host"] for lease in parsed["scheduler_leases"]] == [
        "b",
        "a",
    ]


def test_final_drain_liveness_rejects_global_policy_without_legacy_coverage(
    ) -> None:
    payload = _read_only_drain_status(
        coverage={"legacy_state": False, "scheduler_leases": True},
        policy={
            "active": True,
            "is_full_drain": True,
            "max_jobs": None,
            "max_cpus": None,
            "reason": "forged",
            "set_at": "2026-08-11T11:00:00+00:00",
            "scheduler_hosts": [],
            "legacy_scheduler_hosts": [],
            "full_dispatch": True,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        },
    )

    with pytest.raises(ValueError, match="unavailable legacy coverage"):
        fleet_rollout._validate_final_drain_status(payload)


def test_final_drain_liveness_accepts_exact_both_unreadable_safety_gate() -> None:
    payload = _read_only_drain_status(
        coverage={"legacy_state": False, "scheduler_leases": False},
        safety_fail_closed=True,
    )

    parsed = fleet_rollout._validate_final_drain_status(payload)

    assert parsed["safety_fail_closed"] is True
    assert parsed["coverage"] == {
        "legacy_state": False,
        "scheduler_leases": False,
    }


def test_final_drain_liveness_rejects_noncanonical_lease_id() -> None:
    payload = _read_only_drain_status(
        leases=[
            {
                "lease_id": "forged\nlease",
                "scheduler_host": "host_f",
                "owner": "operator",
                "set_at": "2026-08-11T11:00:00+00:00",
                "reason": "maintenance",
            }
        ]
    )

    with pytest.raises(ValueError, match="lease is malformed"):
        fleet_rollout._validate_final_drain_status(payload)


def test_final_drain_observer_routes_loopback_config_key_locally() -> None:
    cfg = config.Config(
        hosts={"driver": config.HostConfig(ssh="localhost")}
    )

    assert fleet_rollout._drain_observation_argv(cfg, "driver") == [
        sys.executable,
        "-m",
        "vq",
        "drain",
        "--status",
        "--json",
        "--read-only-snapshot",
        "localhost",
    ]


def test_final_drain_observer_uses_direct_remote_ssh_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(
        hosts={
            "driver": config.HostConfig(
                ssh="driver-login",
                remote_vq="/opt/vq/bin/vq",
            )
        }
    )
    monkeypatch.setattr(
        fleet_rollout.transport,
        "_ssh_base",
        lambda host_cfg: ["ssh", host_cfg.ssh],
    )

    argv = fleet_rollout._drain_observation_argv(cfg, "driver")

    assert argv[:2] == ["ssh", "driver-login"]
    assert shlex.split(argv[-1]) == [
        "/opt/vq/bin/vq",
        "drain",
        "--status",
        "--json",
        "--read-only-snapshot",
        "localhost",
    ]


def test_final_drain_liveness_caps_production_fanout_and_queries_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import concurrent.futures

    host_names = [f"host-{index:02d}" for index in range(20)]
    cfg = config.Config(
        hosts={
            host: config.HostConfig(ssh=host, fleet_role="managed")
            for host in host_names
        }
    )
    topology = {
        host: {
            "name": host,
            "role": "managed",
            "canonical_host": None,
            "reason": "explicit config",
        }
        for host in host_names
    }
    workers: list[int] = []
    calls: list[str] = []

    class InlinePool:
        def __init__(self, *, max_workers: int) -> None:
            workers.append(max_workers)

        def __enter__(self) -> InlinePool:
            return self

        def __exit__(self, *args: object) -> None:
            del args

        def map(
            self,
            function: Any,
            values: list[str],
        ) -> list[object]:
            return [function(value) for value in values]

    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", InlinePool)
    monkeypatch.setattr(
        fleet_rollout,
        "_drain_observation_argv",
        lambda cfg, control: [control],
    )

    def bounded(argv: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        calls.append(argv[0])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_read_only_drain_status()),
            stderr="",
        )

    monkeypatch.setattr(
        fleet_rollout,
        "_run_bounded_observation_argv",
        bounded,
    )

    result = fleet_rollout.collect_final_drain_liveness(
        cfg,
        topology=topology,
        run=None,
        runner=subprocess.run,
    )

    assert workers == [fleet_rollout._DRAIN_LIVENESS_MAX_WORKERS]
    assert calls == host_names
    assert result["inactive_hosts"] == host_names


def test_final_drain_liveness_text_is_explicit_and_control_safe() -> None:
    payload = {
        "status": "complete",
        "driver": "driver",
        "report": {
            "release_tag": "v0.15.60",
            "digest_sha256": "a" * 64,
        },
        "drain_liveness": {
            "status": "partial",
            "observed_at": "2026-08-11T12:00:01+00:00",
            "observed_hosts": [],
            "inactive_hosts": [],
            "active_holds": [
                {
                    "host": "host_f\nforged",
                    "control_host": "driver",
                    "kind": "scheduler-target",
                    "owner_class": "external",
                    "owner": "operator",
                    "lease_id": "lease",
                    "reason": "manual\x00 cooling\nwindow",
                    "set_at": "2026-08-11T11:00:00+00:00",
                    "remote_observed_at": "2026-08-11T12:00:00+00:00",
                    "controller_observed_at": "2026-08-11T12:00:01+00:00",
                }
            ],
            "unknown_hosts": [
                {
                    "host": "worker\nforged",
                    "control_host": "worker",
                    "reason": "old\ndaemon",
                }
            ],
        },
    }

    text = fleet_rollout.render_result_text(payload)

    assert (
        "OBSERVED ACTIVE EXTERNAL HOLD host_f forged scheduler-target "
        "owner_class=external owner=operator reason=manual cooling window "
        "(final sweep as of 2026-08-11T12:00:00+00:00)"
        in text
    )
    assert "DRAIN LIVENESS UNKNOWN worker forged: old daemon" in text
    assert "\x00" not in text


def test_bounded_drain_observer_reaps_pipe_inheriting_grandchild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pid_path = tmp_path / "grandchild.pid"
    script = (
        "import os,subprocess,sys,time; "
        "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(60)'],"
        "stdout=sys.stdout,stderr=sys.stderr); "
        "tmp=sys.argv[1]+'.tmp'; open(tmp,'w').write(str(p.pid)); "
        "os.replace(tmp,sys.argv[1]); time.sleep(60)"
    )
    real_popen = subprocess.Popen
    ready_at: list[float] = []

    def ready_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        # The observer's deadline starts when Popen returns. Return only once
        # the grandchild holds the pipes (its pid is renamed into place, so
        # never read half-written), so the 0.2 s budget times the reap and
        # not two interpreter start-ups on a loaded machine.
        proc = real_popen(*args, **kwargs)
        deadline = time.monotonic() + 10
        while not pid_path.exists():
            if proc.poll() is not None or time.monotonic() >= deadline:
                # Best effort on a failure path: the group may be exiting,
                # which macOS answers with EPERM, not ESRCH (#27, #29).
                with suppress(OSError):
                    os.killpg(proc.pid, signal.SIGKILL)
                _stdout, stderr = proc.communicate(timeout=5)
                pytest.fail(f"grandchild fixture never wrote its pid: {stderr!r}")
            time.sleep(0.01)
        ready_at.append(time.monotonic())
        return proc

    monkeypatch.setattr(fleet_rollout.subprocess, "Popen", ready_popen)

    with pytest.raises(subprocess.TimeoutExpired):
        fleet_rollout._run_bounded_observation_argv(
            [sys.executable, "-c", script, str(pid_path)],
            timeout=0.2,
        )

    assert time.monotonic() - ready_at[0] < 3
    grandchild_pid = int(pid_path.read_text())
    deadline = time.monotonic() + 3
    while True:
        try:
            os.kill(grandchild_pid, 0)
        except ProcessLookupError:
            break
        if time.monotonic() >= deadline:
            pytest.fail("bounded observer left its pipe-inheriting child alive")
        time.sleep(0.01)


def test_bounded_drain_observer_caps_and_drains_output() -> None:
    script = (
        "import os; "
        f"os.write(1,b'x'*({fleet_rollout._DRAIN_LIVENESS_OUTPUT_LIMIT}+1))"
    )

    with pytest.raises(ValueError, match="output exceeds"):
        fleet_rollout._run_bounded_observation_argv(
            [sys.executable, "-c", script],
            timeout=3,
        )


def test_bounded_drain_observer_rejects_invalid_utf8() -> None:
    script = "import os; os.write(1,b'{\"value\":\"\\xff\"}')"

    with pytest.raises(UnicodeDecodeError):
        fleet_rollout._run_bounded_observation_argv(
            [sys.executable, "-c", script],
            timeout=3,
        )


def test_legacy_auto_topology_migrates_only_unambiguous_hosts() -> None:
    cfg = config.Config(
        hosts={
            "localhost": config.HostConfig(ssh="localhost"),
            "cluster": config.HostConfig(
                ssh="cluster-login",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                scheduler_runtime_deployments={
                    "vibeqc-release": _runtime(),
                },
            ),
            "campaign": config.HostConfig(
                ssh="cluster-login",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
            ),
            "unknown": config.HostConfig(ssh="unknown"),
        }
    )
    admin, _, _ = _snapshots()
    topology = fleet_rollout.resolve_topology(cfg, admin)

    assert topology["localhost"].role == "managed"
    assert topology["cluster"].role == "managed"
    assert topology["campaign"].role == "alias"
    assert topology["campaign"].canonical_host == "cluster"
    assert topology["unknown"].role == "unresolved"


def test_legacy_auto_topology_rejects_duplicate_scheduler_owners() -> None:
    deployments = {"vibeqc-release": _runtime()}
    cfg = config.Config(
        hosts={
            "localhost": config.HostConfig(ssh="localhost"),
            "cluster-a": config.HostConfig(
                ssh="shared-login",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                scheduler_runtime_deployments=deployments,
            ),
            "cluster-b": config.HostConfig(
                ssh="shared-login",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                scheduler_runtime_deployments=deployments,
            ),
        }
    )
    admin, _, _ = _snapshots()

    topology = fleet_rollout.resolve_topology(cfg, admin)

    assert topology["cluster-a"].role == "unresolved"
    assert topology["cluster-b"].role == "unresolved"
    assert "ambiguous" in topology["cluster-a"].reason


def test_interruption_resume_uses_new_live_state_not_stale_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    actions = [
        fleet_rollout.RolloutAction(
            id=f"a{index}",
            phase="local-runtime",
            host="host_d",
            program="vibeqc-dev",
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", "vibeqc-dev", "host_d"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for index in (1, 2)
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    calls: list[list[str]] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    held = False

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal held
        del kwargs
        args = argv[3:]
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": held,
                        "is_full_drain": held,
                        "scheduler_hosts": [],
                        "state": {
                            "reason": fleet_rollout._hold_reason(
                                "v0.15.60-test", "host_d"
                            ),
                            "set_at": "2026-08-10T20:00:00+00:00",
                        } if held else None,
                    }
                ),
                stderr="",
            )
        held = "--release-full" not in args
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(InterruptedError):
        fleet_rollout.execute_plan(
            plan,
            rollout_id="v0.15.60-test",
            runner=runner,
            control_runner=control_runner,
            stop_after=1,
        )
    assert len(calls) == 1

    resumed = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=plan.report,
        topology={},
        actions=[
            fleet_rollout.RolloutAction(
                **{
                    **actions[0].__dict__,
                    "decision": "skip",
                    "reason": "already at target with LAST OK=true",
                }
            ),
            actions[1],
        ],
    )
    run = fleet_rollout.execute_plan(
        resumed,
        rollout_id="v0.15.60-test",
        runner=runner,
        control_runner=control_runner,
    )
    assert len(calls) == 2
    assert run.complete is False
    assert run.actions["a1"]["status"] == "success"
    assert "LAST OK=true" in run.actions["a1"]["verified_reason"]
    assert run.actions["a2"]["status"] == "success"

    # If live state still asks for the first update, a stale journal success
    # cannot suppress it.
    fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-test",
        runner=runner,
        control_runner=control_runner,
    )
    assert len(calls) == 4


def _scheduler_runtime_action(
    program: str,
    *,
    decision: fleet_rollout.Decision = "update",
) -> fleet_rollout.RolloutAction:
    return fleet_rollout.RolloutAction(
        id=f"scheduler-runtime:host_f:{program}",
        phase="scheduler-runtime",
        host="host_f",
        program=program,
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", program, "host_f"],
        decision=decision,
        reason=(
            "target is newer"
            if decision == "update"
            else "already at target with LAST OK=true"
        ),
        before={},
    )


def _scheduler_alias_plan(
    *actions: fleet_rollout.RolloutAction,
) -> fleet_rollout.RolloutPlan:
    plan = fleet_rollout.RolloutPlan(
        driver="driver-a",
        report=fleet_release.report_summary(_report()),
        topology={
            "host_f": {"role": "managed", "canonical_host": None},
            "host_f-big": {"role": "alias", "canonical_host": "host_f"},
        },
        actions=list(actions),
    )
    plan._scheduler_hold_targets = {
        "host_f": (("host_f", "driver-a"), ("host_f-big", "driver-b")),
    }
    return plan


def test_final_scheduler_parity_accepts_the_exact_healthy_ahead_repair() -> None:
    attempted = _scheduler_runtime_action("program-1", decision="update")
    attempted.target_sha = FUTURE
    attempted.target_version = None
    attempted.target_tag = None
    final = _scheduler_runtime_action("program-1", decision="skip")
    final.reason = "newer descendant already deployed; no downgrade"
    final.before = {"current_sha": FUTURE}

    assert fleet_rollout._final_action_proves_scheduler_parity(
        attempted, final
    )
    final.before = {"current_sha": OLD}
    assert not fleet_rollout._final_action_proves_scheduler_parity(
        attempted, final
    )


def test_scheduler_hold_targets_cover_resolved_aliases_on_their_own_drivers(
) -> None:
    cfg = _config()
    cfg.hosts["driver-b"] = config.HostConfig(
        ssh="driver-b",
        fleet_role="managed",
    )
    cfg.hosts["host_f-big"].scheduler_driver = "driver-b"
    cfg.hosts["host_f-big"].scheduler_runtime_deployments = {}
    cfg.hosts["host_f-auto"] = _scheduler_host(
        ssh="host_f",
        role="auto",
        deployments=False,
    )
    cfg.hosts["host_f-auto"].scheduler_update_command = None
    cfg.hosts["host_f-auto"].scheduler_driver = "localhost"
    cfg.fleet_rollout_order.append("host_f-auto")
    cfg.fleet_rollout_order.append("driver-b")
    admin, programs, doctor = _snapshots()
    admin["driver-b"] = admin["localhost"]
    programs["driver-b"] = programs["localhost"]
    doctor["driver-b"] = doctor["localhost"]
    plan = _plan(cfg=cfg, snapshots=(admin, programs, doctor))

    assert plan._scheduler_hold_targets["host_f"] == (
        ("host_f", "localhost"),
        ("host_f-big", "driver-b"),
        ("host_f-auto", "localhost"),
    )
    assert "_scheduler_hold_targets" not in plan.as_dict()
    assert not any(action.host in {"host_f-big", "host_f-auto"} for action in plan.actions)


def test_scheduler_hold_targets_flatten_resolved_alias_chains() -> None:
    cfg = _config()
    cfg.hosts["host_f-outer"] = _scheduler_host(
        ssh="host_f",
        role="alias",
        canonical="host_f-big",
        deployments=False,
    )
    cfg.fleet_rollout_order.append("host_f-outer")

    plan = _plan(cfg=cfg)

    assert plan._scheduler_hold_targets["host_f"] == (
        ("host_f", "localhost"),
        ("host_f-big", "localhost"),
        ("host_f-outer", "localhost"),
    )


def test_scheduler_hold_target_alias_cycle_blocks_during_planning() -> None:
    cfg = _config()
    cfg.hosts["host_f-outer"] = _scheduler_host(
        ssh="host_f",
        role="alias",
        canonical="host_f-big",
        deployments=False,
    )
    cfg.hosts["host_f-big"].fleet_canonical_host = "host_f-outer"
    cfg.fleet_rollout_order.append("host_f-outer")

    with pytest.raises(fleet_rollout.FleetRolloutError, match="alias cycle"):
        _plan(cfg=cfg)


def test_scheduler_hold_group_is_complete_before_work_and_released_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("vibeqc-dev"),
        _scheduler_runtime_action("vibe-view"),
    )
    leases: dict[tuple[str, str], str] = {}
    events: list[str] = []
    controls: list[list[str]] = []

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        assert set(leases) == {
            ("driver-a", "host_f"),
            ("driver-b", "host_f-big"),
        }
        events.append(f"update:{argv[-2]}")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        control = args[-1]
        if "--status" in args:
            target_leases = [
                {
                    "scheduler_host": target,
                    "owner": owner,
                }
                for (owner_control, target), owner in leases.items()
                if owner_control == control
            ]
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": bool(target_leases),
                        "is_full_drain": False,
                        "scheduler_hosts": sorted(
                            item["scheduler_host"] for item in target_leases
                        ),
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": target_leases,
                        "state": None,
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        owner = args[args.index("--lease-owner") + 1]
        key = (control, target)
        if "--release" in args:
            assert leases.get(key) == owner
            leases.pop(key)
            events.append(f"release:{target}@{control}")
        else:
            leases[key] = owner
            events.append(f"hold:{target}@{control}")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-alias-group",
        runner=update_runner,
        control_runner=control_runner,
    )

    assert events[:2] == ["hold:host_f@driver-a", "hold:host_f-big@driver-b"]
    assert events[2:4] == ["update:vibeqc-dev", "update:vibe-view"]
    assert set(events[4:]) == {
        "release:host_f@driver-a",
        "release:host_f-big@driver-b",
    }
    assert leases == {}
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f"]["action_host"] == "host_f"
    assert run.holds["host_f-big"]["status"] == "released"
    assert run.holds["host_f-big"]["action_host"] == "host_f"
    assert run.holds["host_f-big"]["control_host"] == "driver-b"
    release_calls = [args for args in controls if "--release" in args]
    assert len(release_calls) == 2
    assert all("--lease-owner" in args for args in release_calls)


def test_scheduler_hold_group_survives_death_before_second_member_and_reparent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-alias-group-hard-death"
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
    )
    fleet_rollout.save_run(run)

    class SimulatedHardDeath(BaseException):
        pass

    calls: list[list[str]] = []

    def dying_control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        if not calls:
            persisted = fleet_rollout.load_run(rollout_id)
            assert persisted is not None
            assert set(persisted.holds) == {"host_f", "host_f-big"}
            assert {
                target: (
                    hold["action_host"],
                    hold["control_host"],
                    hold["acquire_unconfirmed"],
                )
                for target, hold in persisted.holds.items()
            } == {
                "host_f": ("host_f", "driver-a", True),
                "host_f-big": ("host_f", "driver-b", True),
            }
        calls.append(args)
        if "--status" in args:
            if args[-1] == "driver-b":
                raise SimulatedHardDeath
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": False,
                        "is_full_drain": False,
                        "scheduler_hosts": [],
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [],
                        "state": None,
                    }
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(SimulatedHardDeath):
        fleet_rollout._acquire_scheduler_rollout_hold_group(
            plan,
            run,
            action_host="host_f",
            actions=plan.actions,
            runner=dying_control,
            held={},
            validated=set(),
        )

    persisted = fleet_rollout.load_run(rollout_id)
    assert persisted is not None
    assert persisted.holds["host_f-big"]["acquire_unconfirmed"] is True
    before_holds = {
        target: dict(hold) for target, hold in persisted.holds.items()
    }

    changed = _scheduler_alias_plan(*plan.actions)
    changed._scheduler_hold_targets = {
        "host_f": (("host_f", "driver-a"),),
        "other": (("other", "driver-c"), ("host_f-big", "driver-b")),
    }
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="removed or reparented",
    ):
        fleet_rollout.execute_plan(
            changed,
            rollout_id=rollout_id,
            runner=lambda *args, **kwargs: pytest.fail(
                "reparented incomplete groups must block every update"
            ),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "semantic group drift must preserve every exact hold"
            ),
        )

    preserved = fleet_rollout.load_run(rollout_id)
    assert preserved is not None
    assert preserved.holds == before_holds


def test_scheduler_alias_acquire_failure_runs_no_work_and_cleans_exactly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    mutations: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": False,
                        "is_full_drain": False,
                        "scheduler_hosts": [],
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [],
                        "state": None,
                    }
                ),
                stderr="",
            )
        mutations.append(args)
        target = args[args.index("--scheduler-host") + 1]
        if target == "host_f-big" and "--release" not in args:
            return subprocess.CompletedProcess(
                argv, 9, stdout="", stderr="driver-b unavailable"
            )
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="could not acquire rollout hold for host_f-big",
    ):
        fleet_rollout.execute_plan(
            plan,
            rollout_id="v0.15.60-alias-acquire-failure",
            runner=lambda *args, **kwargs: pytest.fail(
                "no update may run before every alias hold is confirmed"
            ),
            control_runner=control_runner,
        )

    assert [
        args[args.index("--scheduler-host") + 1]
        for args in mutations
        if "--release" not in args
    ] == ["host_f", "host_f-big"]
    release_calls = [args for args in mutations if "--release" in args]
    assert {
        args[args.index("--scheduler-host") + 1] for args in release_calls
    } == {"host_f", "host_f-big"}
    assert all("--lease-owner" in args for args in release_calls)
    run = fleet_rollout.load_run("v0.15.60-alias-acquire-failure")
    assert run is not None
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f-big"]["status"] == "released"


def test_scheduler_alias_release_failure_is_target_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    held: set[tuple[str, str]] = set()
    releases: list[tuple[str, str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        control = args[-1]
        if "--status" in args:
            targets = sorted(target for owner, target in held if owner == control)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": bool(targets),
                        "is_full_drain": False,
                        "scheduler_hosts": targets,
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [
                            {
                                "scheduler_host": target,
                                "owner": fleet_rollout._hold_lease_owner(
                                    "v0.15.60-alias-release-failure", target
                                ),
                            }
                            for target in targets
                        ],
                        "state": None,
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        key = (control, target)
        if "--release" in args:
            releases.append(key)
            if target == "host_f-big":
                return subprocess.CompletedProcess(
                    argv, 9, stdout="", stderr="release failed on driver-b"
                )
            held.discard(key)
        else:
            held.add(key)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="release failed on driver-b",
    ):
        fleet_rollout.execute_plan(
            plan,
            rollout_id="v0.15.60-alias-release-failure",
            runner=lambda argv, **kwargs: subprocess.CompletedProcess(
                argv, 0, stdout="ok\n", stderr=""
            ),
            control_runner=control_runner,
        )

    run = fleet_rollout.load_run("v0.15.60-alias-release-failure")
    assert run is not None
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f-big"]["status"] == "cleanup-failed"
    assert releases.count(("driver-a", "host_f")) == 1
    assert releases.count(("driver-b", "host_f-big")) >= 1


@pytest.mark.parametrize("binding_case", ["wrong-control", "reparented"])
def test_existing_alias_binding_mismatch_blocks_work_and_preserves_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    binding_case: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = f"v0.15.60-alias-binding-{binding_case}"
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    stored_control = "driver-old" if binding_case == "wrong-control" else "driver-b"
    if binding_case == "reparented":
        plan._scheduler_hold_targets = {
            "host_f": (("host_f", "driver-a"),),
            "other": (("other", "driver-c"), ("host_f-big", "driver-b")),
        }
    hold = {
        "host": "host_f-big",
        "action_host": "host_f",
        "kind": "scheduler-target",
        "owned": True,
        "status": "active",
        "reason": fleet_rollout._hold_reason(rollout_id, "host_f-big"),
        "lease_owner": fleet_rollout._hold_lease_owner(
            rollout_id, "host_f-big"
        ),
        "control_host": stored_control,
    }
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={"host_f-big": hold},
        )
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="scheduler hold binding",
    ):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout_id,
            runner=lambda *args, **kwargs: pytest.fail(
                "binding drift must block every update"
            ),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "binding drift must preserve the exact hold"
            ),
        )

    run = fleet_rollout.load_run(rollout_id)
    assert run is not None
    assert run.holds["host_f-big"] == hold


@pytest.mark.parametrize(
    ("field", "value"),
    [("action_host", []), ("control_host", [])],
)
def test_malformed_active_alias_binding_is_not_overwritten_or_released(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = f"v0.15.60-malformed-alias-{field}"
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    hold: dict[str, Any] = {
        "host": "host_f-big",
        "action_host": "host_f",
        "kind": "scheduler-target",
        "owned": True,
        "status": "active",
        "reason": fleet_rollout._hold_reason(rollout_id, "host_f-big"),
        "lease_owner": fleet_rollout._hold_lease_owner(
            rollout_id, "host_f-big"
        ),
        "control_host": "driver-b",
    }
    hold[field] = value
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={"host_f-big": hold},
        )
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match=field):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout_id,
            runner=lambda *args, **kwargs: pytest.fail(
                "malformed bindings must block every update"
            ),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "malformed bindings have no safe control mutation"
            ),
        )

    run = fleet_rollout.load_run(rollout_id)
    assert run is not None
    assert run.holds["host_f-big"] == hold


@pytest.mark.parametrize("status", ["active", "cleanup-failed"])
@pytest.mark.parametrize(
    "ownership_case",
    ["owned-false", "wrong-owner", "unrecognized-ownerless"],
)
def test_invalid_scheduler_hold_ownership_blocks_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    status: str,
    ownership_case: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = f"v0.15.60-invalid-owner-{status}-{ownership_case}"
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    hold: dict[str, Any] = {
        "host": "host_f",
        "action_host": "host_f",
        "kind": "scheduler-target",
        "owned": ownership_case != "owned-false",
        "status": status,
        "reason": fleet_rollout._hold_reason(rollout_id, "host_f"),
        "lease_owner": fleet_rollout._hold_lease_owner(rollout_id, "host_f"),
        "control_host": "driver-a",
    }
    if ownership_case == "wrong-owner":
        hold["lease_owner"] = "fleet-rollout:other:host_f"
    elif ownership_case == "unrecognized-ownerless":
        hold["lease_owner"] = None
        hold["reason"] = "not this rollout"
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={"host_f": hold},
        )
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="ownership"):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout_id,
            runner=lambda *args, **kwargs: pytest.fail(
                "invalid hold ownership must block every update"
            ),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "invalid hold ownership must block every control mutation"
            ),
        )

    run = fleet_rollout.load_run(rollout_id)
    assert run is not None
    assert run.holds["host_f"] == hold


def test_one_scheduler_action_does_not_take_an_outer_alias_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _scheduler_alias_plan(_scheduler_runtime_action("vibeqc-dev"))

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-single-scheduler-action",
        runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout="ok\n", stderr=""
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "one scheduler action must not gain a new outer hold"
        ),
    )

    assert run.holds == {}


def test_skipped_canonical_scheduler_group_acquires_no_alias_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    scoped = fleet_rollout.select_hosts(
        plan,
        fleet_rollout.HostSelection(skip=("host_f",)),
    )

    run = fleet_rollout.execute_plan(
        scoped,
        rollout_id="v0.15.60-skipped-scheduler-group",
        runner=lambda *args, **kwargs: pytest.fail(
            "deferred scheduler actions must not execute"
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "deferred scheduler actions must not acquire holds"
        ),
    )

    assert run.holds == {}


def test_scoped_canonical_rollout_preserves_its_scheduler_hold_targets() -> None:
    plan = _plan()
    scoped = fleet_rollout.select_hosts(plan, _selection(only=["host_f"]))

    assert scoped._scheduler_hold_targets == plan._scheduler_hold_targets
    assert scoped._scheduler_hold_targets["host_f"] == (
        ("host_f", "localhost"),
        ("host_f-big", "localhost"),
    )
    assert all(
        action.decision == "update"
        for action in scoped.actions
        if action.host == "host_f"
    )


def test_scheduler_hold_spans_noncontiguous_actions_and_cleans_up(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)

    def action(host: str, phase: fleet_rollout.Phase, program: str):
        return fleet_rollout.RolloutAction(
            id=f"{phase}:{host}:{program}",
            phase=phase,
            host=host,
            program=program,
            pin_name="vq" if phase == "helper" else "dev",
            target_sha=VQ if phase == "helper" else DEV,
            target_version="0.17.0" if phase == "helper" else "0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", host] if phase == "helper" else [
                "admin", "update", program, host,
            ],
            decision="update",
            reason="target is newer",
            before={},
        )

    actions = [
        action("host_f", "helper", "vibeqc-queue"),
        action("host_c", "helper", "vibeqc-queue"),
        action("host_f", "scheduler-runtime", "vibeqc-dev"),
        action("host_c", "scheduler-runtime", "vibeqc-dev"),
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    events: list[str] = []
    held: set[str] = set()

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        events.append(f"update:{argv[-1]}:{argv[-2]}")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        if "--status" in args:
            payload = {
                "active": bool(held),
                "is_full_drain": False,
                "scheduler_hosts": sorted(held),
                "state": None,
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        host = args[args.index("--scheduler-host") + 1]
        if "--release" in args:
            held.discard(host)
            events.append(f"release:{host}")
        else:
            held.add(host)
            events.append(f"hold:{host}")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-holds",
        runner=update_runner,
        control_runner=control_runner,
    )

    assert events == [
        "hold:host_f",
        "update:host_f:update",
        "hold:host_c",
        "update:host_c:update",
        "update:host_f:vibeqc-dev",
        "release:host_f",
        "update:host_c:vibeqc-dev",
        "release:host_c",
    ]
    assert held == set()
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_c"]["status"] == "released"


def test_scheduler_hold_stays_active_until_final_parity_is_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful command is not permission to reopen a drifted lane."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-final-parity"
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1"),
        _scheduler_runtime_action("program-2"),
    )
    held: set[tuple[str, str]] = set()
    mutations: list[tuple[str, str, str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        control = args[-1]
        if "--status" in args:
            targets = sorted(target for owner, target in held if owner == control)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": bool(targets),
                        "is_full_drain": False,
                        "scheduler_hosts": targets,
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [
                            {
                                "scheduler_host": target,
                                "owner": fleet_rollout._hold_lease_owner(
                                    rollout_id, target
                                ),
                            }
                            for target in targets
                        ],
                        "state": None,
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        key = (control, target)
        if "--release" in args:
            held.discard(key)
            mutations.append(("release", control, target))
        else:
            held.add(key)
            mutations.append(("hold", control, target))
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout="ok\n", stderr=""
        ),
        control_runner=control_runner,
        retain_scheduler_holds=True,
    )

    assert held == {("driver-a", "host_f"), ("driver-b", "host_f-big")}
    assert all(
        run.holds[target]["status"] == "active"
        for target in ("host_f", "host_f-big")
    )
    assert not [item for item in mutations if item[0] == "release"]

    still_drifted = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="update"),
        _scheduler_runtime_action("program-2", decision="skip"),
    )
    run = fleet_rollout.reconcile_scheduler_parity_holds(
        plan,
        still_drifted,
        doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
        run=run,
        runner=control_runner,
    )

    assert held == {("driver-a", "host_f"), ("driver-b", "host_f-big")}
    assert all(
        run.holds[target]["status"] == "active"
        for target in ("host_f", "host_f-big")
    )
    assert not [item for item in mutations if item[0] == "release"]

    verified = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="skip"),
        _scheduler_runtime_action("program-2", decision="skip"),
    )
    run = fleet_rollout.reconcile_scheduler_parity_holds(
        plan,
        verified,
        doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
        run=run,
        runner=control_runner,
    )

    assert held == set()
    assert all(
        run.holds[target]["status"] == "released"
        for target in ("host_f", "host_f-big")
    )


def test_final_parity_mismatch_holds_a_single_scheduler_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The existing one-action deployment policy stays narrow until drift."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-single-final-parity"
    plan = _scheduler_alias_plan(_scheduler_runtime_action("program-1"))
    controls: list[list[str]] = []
    held: set[tuple[str, str]] = set()

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        control = args[-1]
        if "--status" in args:
            targets = sorted(target for owner, target in held if owner == control)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": bool(targets),
                        "is_full_drain": False,
                        "scheduler_hosts": targets,
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [],
                        "state": None,
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        held.add((control, target))
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout="ok\n", stderr=""
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "one action must not acquire an outer hold before deployment"
        ),
        retain_scheduler_holds=True,
    )
    assert run.holds == {}

    run = fleet_rollout.reconcile_scheduler_parity_holds(
        plan,
        plan,
        doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
        run=run,
        runner=control_runner,
    )

    assert held == {("driver-a", "host_f"), ("driver-b", "host_f-big")}
    assert all(
        run.holds[target]["status"] == "active"
        for target in ("host_f", "host_f-big")
    )
    assert not any("--release" in args for args in controls)


def test_final_parity_does_not_mutate_an_out_of_scope_scheduler_lane(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scoped run cannot hold a previously-converged excluded host."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-scoped-final-parity"
    initial = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="skip")
    )
    verification = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="update")
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(initial.report["digest_sha256"]),
        report_source_path=str(initial.report["source_path"]),
    )
    fleet_rollout.save_run(run)

    result = fleet_rollout.reconcile_scheduler_parity_holds(
        initial,
        verification,
        doctor={"host_f": {"ok": False}},
        run=run,
        runner=lambda *args, **kwargs: pytest.fail(
            "out-of-scope parity must not issue a drain control call"
        ),
        selection=fleet_rollout.HostSelection(skip=("host_f",)),
    )

    assert result.holds == {}


@pytest.mark.parametrize(
    "final_shape",
    ["missing-action", "shrunk-aliases", "extra-action"],
)
def test_final_parity_degradation_holds_the_complete_initial_alias_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    final_shape: str,
) -> None:
    """Missing/new lanes and degraded topology never discard known aliases."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = f"v0.15.60-final-{final_shape}"
    expected = _scheduler_runtime_action("program-1", decision="skip")
    initial = _scheduler_alias_plan(expected)
    if final_shape == "missing-action":
        verification = _scheduler_alias_plan()
    elif final_shape == "shrunk-aliases":
        verification = _scheduler_alias_plan(
            _scheduler_runtime_action("program-1", decision="update")
        )
        verification._scheduler_hold_targets = {
            "host_f": (("host_f", "driver-a"),),
        }
    else:
        extra = _scheduler_runtime_action("program-2", decision="update")
        verification = _scheduler_alias_plan(expected, extra)

    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(initial.report["digest_sha256"]),
        report_source_path=str(initial.report["source_path"]),
    )
    fleet_rollout.save_run(run)
    held: set[tuple[str, str]] = set()
    calls: list[tuple[str, str, str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        control = args[-1]
        if "--status" in args:
            targets = sorted(target for owner, target in held if owner == control)
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": bool(targets),
                        "is_full_drain": False,
                        "scheduler_hosts": targets,
                        "legacy_scheduler_hosts": [],
                        "scheduler_leases": [
                            {
                                "scheduler_host": target,
                                "owner": fleet_rollout._hold_lease_owner(
                                    rollout_id, target
                                ),
                            }
                            for target in targets
                        ],
                        "state": None,
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        operation = "release" if "--release" in args else "hold"
        calls.append((operation, control, target))
        if operation == "release":
            held.discard((control, target))
        else:
            held.add((control, target))
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    result = fleet_rollout.reconcile_scheduler_parity_holds(
        initial,
        verification,
        doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
        run=run,
        runner=control_runner,
    )

    assert held == {("driver-a", "host_f"), ("driver-b", "host_f-big")}
    assert not [call for call in calls if call[0] == "release"]
    assert set(result.holds) == {"host_f", "host_f-big"}


def test_final_parity_rejects_cross_report_plans_before_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    initial = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="skip")
    )
    verification = _scheduler_alias_plan(
        _scheduler_runtime_action("program-1", decision="skip")
    )
    verification.report = {**verification.report, "digest_sha256": "f" * 64}
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-cross-report-parity",
        report_digest_sha256=str(initial.report["digest_sha256"]),
        report_source_path=str(initial.report["source_path"]),
    )
    fleet_rollout.save_run(run)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="do not bind the current rollout report",
    ):
        fleet_rollout.reconcile_scheduler_parity_holds(
            initial,
            verification,
            doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
            run=run,
            runner=lambda *args, **kwargs: pytest.fail(
                "cross-report parity must not issue a control call"
            ),
            report_digest_resolver=lambda: "f" * 64,
        )


def test_rollout_hold_cleans_on_interruption_and_reentry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_d:{index}",
            phase="local-runtime",
            host="host_d",
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, "host_d"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for index, program in enumerate(
            ("vibeqc-queue", "vibeqc-dev", "vibe-view"), start=1
        )
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    held = False
    controls: list[str] = []
    updates: list[str] = []

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        updates.append(argv[-2])
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal held
        del kwargs
        args = argv[3:]
        if "--status" in args:
            payload = {
                "active": held,
                "is_full_drain": held,
                "scheduler_hosts": [],
                "state": {
                    "reason": fleet_rollout._hold_reason(
                        "v0.15.60-reentry", "host_d"
                    ),
                    "set_at": "2026-08-10T20:00:00+00:00",
                } if held else None,
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        if "--release-full" in args:
            held = False
            controls.append("release")
        else:
            held = True
            controls.append("hold")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(InterruptedError):
        fleet_rollout.execute_plan(
            plan,
            rollout_id="v0.15.60-reentry",
            runner=update_runner,
            control_runner=control_runner,
            stop_after=1,
        )
    assert held is False
    assert controls == ["hold", "release"]

    resumed_actions = [
        fleet_rollout.RolloutAction(
            **{
                **actions[0].__dict__,
                "decision": "skip",
                "reason": "already at target with LAST OK=true",
            }
        ),
        *actions[1:],
    ]
    resumed = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=plan.report,
        topology={},
        actions=resumed_actions,
    )
    run = fleet_rollout.execute_plan(
        resumed,
        rollout_id="v0.15.60-reentry",
        runner=update_runner,
        control_runner=control_runner,
    )

    assert held is False
    assert controls == ["hold", "release", "hold", "release"]
    assert updates == ["vibeqc-queue", "vibeqc-dev", "vibe-view"]
    assert run.holds["host_d"]["status"] == "released"
    assert run.actions["host_d:1"]["status"] == "success"
    assert "LAST OK=true" in run.actions["host_d:1"]["verified_reason"]


def test_local_rollout_hold_covers_every_serial_update_and_stays_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Four cold builds cannot outlive the outer full-drain deadline."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    monkeypatch.delenv("VQ_UPDATE_SCRIPT_TIMEOUT", raising=False)
    monkeypatch.delenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", raising=False)
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_e:{program}",
            phase="local-runtime",
            host="host_e",
            program=program,
            pin_name="vq" if program == "vibeqc-queue" else "dev",
            target_sha=VQ if program == "vibeqc-queue" else DEV,
            target_version="0.17.0",
            target_tag=None,
            argv=["admin", "update", program, "host_e"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for program in (
            "vibeqc-queue",
            "vibeqc-release",
            "vibeqc-dev",
            "vibe-view",
        )
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    now = 0.0
    expires_at: float | None = None
    acquired_duration: int | None = None

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal now
        del kwargs
        assert expires_at is not None
        assert now < expires_at
        now += float(
            fleet_rollout.transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS
        )
        assert now < expires_at
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal acquired_duration, expires_at
        del kwargs
        args = argv[3:]
        if "--status" in args:
            active = expires_at is not None and now < expires_at
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": active,
                        "is_full_drain": active,
                        "scheduler_hosts": [],
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    "v0.15.60-long-local", "host_e"
                                ),
                                "set_at": "2026-08-10T20:00:00+00:00",
                            }
                            if active
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        if "--release-full" in args:
            expires_at = None
            return subprocess.CompletedProcess(
                argv, 0, stdout="released\n", stderr=""
            )
        raw_duration = args[args.index("--duration") + 1]
        suffix_seconds = {"s": 1, "m": 60, "h": 3600}
        acquired_duration = (
            int(raw_duration[:-1]) * suffix_seconds[raw_duration[-1]]
        )
        expires_at = now + acquired_duration
        return subprocess.CompletedProcess(argv, 0, stdout="held\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-long-local",
        runner=update_runner,
        control_runner=control_runner,
    )

    assert (
        now
        == 4
        * fleet_rollout.transport.DEFAULT_REMOTE_ADMIN_UPDATE_TIMEOUT_SECONDS
    )
    assert acquired_duration is not None
    assert acquired_duration > now
    assert run.holds["host_e"]["duration_seconds"] == acquired_duration
    assert run.holds["host_e"]["status"] == "released"
    assert expires_at is None


def test_rollout_hold_cleans_after_terminal_action_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_f:{index}",
            phase="scheduler-runtime",
            host="host_f",
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, "host_f"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for index, program in enumerate(("vibeqc-dev", "vibe-view"), start=1)
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    held = False

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(argv, 9, stdout="", stderr="failed")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal held
        del kwargs
        args = argv[3:]
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": held,
                        "is_full_drain": False,
                        "scheduler_hosts": ["host_f"] if held else [],
                        "state": None,
                    }
                ),
                stderr="",
            )
        held = "--release" not in args
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    # A failed lane no longer aborts the invocation -- it is terminal for its
    # own host only -- but the rollout-owned drain it took must still be
    # released, which is what this test has always been protecting.
    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-failure-cleanup",
        runner=update_runner,
        control_runner=control_runner,
    )

    assert held is False
    assert run.holds["host_f"]["status"] == "released"
    # The host is recorded as degraded, and the run is not "complete".
    assert "host_f" in run.failed_hosts
    assert "failed with exit 9" in run.failed_hosts["host_f"]
    assert run.complete is False
    # Its second lane was not attempted: a host whose lane just failed is not a
    # host to keep installing onto.
    assert run.actions["host_f:2"]["status"] == "not-run"
    assert "an earlier lane on host_f failed" in run.actions["host_f:2"]["reason"]
    reloaded = fleet_rollout.load_run("v0.15.60-failure-cleanup")
    assert reloaded is not None
    assert reloaded.holds["host_f"]["status"] == "released"


def test_rollout_preserves_preexisting_scheduler_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_f:{program}",
            phase="scheduler-runtime",
            host="host_f",
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, "host_f"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for program in ("vibeqc-dev", "vibe-view")
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    controls: list[list[str]] = []

    def runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(
                {
                    "active": True,
                    "is_full_drain": False,
                    "scheduler_hosts": ["host_f"],
                    "scheduler_leases": [
                        {
                            "lease_id": "operator-host_f",
                            "scheduler_host": "host_f",
                            "owner": "operator",
                            "reason": "heatwave maintenance",
                        }
                    ],
                    "state": {"reason": "heatwave maintenance"},
                }
            ),
            stderr="",
        )

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-external-hold",
        runner=runner,
        control_runner=control_runner,
    )

    assert len(controls) == 3
    assert "--status" in controls[0]
    assert "--lease-owner" in controls[1]
    assert "--lease-owner" in controls[2]
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f"]["preexisting"] is True
    assert run.holds["host_f"]["external_holds"] == [
        {
            "host": "host_f",
            "kind": "scheduler-target",
            "lease_id": "operator-host_f",
            "owner": "operator",
            "reason": "heatwave maintenance",
        }
    ]

    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor={},
        run=run,
    )
    assert payload["preserved_external_holds"] == [
        {
            "host": "host_f",
            "kind": "scheduler-target",
            "lease_id": "operator-host_f",
            "owner": "operator",
            "reason": "heatwave maintenance",
            "status": "preserved",
        }
    ]
    assert (
        "PRESERVED EXTERNAL HOLD host_f scheduler-target owner=operator "
        "reason=heatwave maintenance"
        in fleet_rollout.render_result_text(payload)
    )


@pytest.mark.parametrize(
    ("external_reason", "rendered_reason"),
    [
        ("manual filesystem repair", "manual filesystem repair"),
        (None, "no reason recorded"),
    ],
)
def test_rollout_reports_a_borrowed_local_drain_with_its_original_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    external_reason: str | None,
    rendered_reason: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = fleet_rollout.RolloutAction(
        id="host_d:vibeqc-dev",
        phase="local-runtime",
        host="host_d",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_d"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={
            "host_d": {
                "name": "host_d",
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            }
        },
        actions=[action],
    )
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-borrowed-local",
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
    )
    controls: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": True,
                        "is_full_drain": True,
                        "scheduler_hosts": [],
                        "scheduler_leases": [],
                        "state": {"reason": external_reason},
                    }
                ),
                stderr="",
            )
        raise AssertionError(f"borrowed drain must not be mutated: {args}")

    hold = fleet_rollout.acquire_rollout_hold(
        plan,
        run,
        host="host_d",
        actions=[action],
        runner=control_runner,
    )
    fleet_rollout.release_rollout_hold(
        plan,
        run,
        hold=hold,
        runner=control_runner,
    )

    assert len(controls) == 1
    assert run.holds["host_d"]["status"] == "preserved"
    assert run.holds["host_d"].get("external_reason") == external_reason
    assert run.holds["host_d"]["external_holds"] == [
        {
            "host": "host_d",
            "kind": "full",
            "lease_id": None,
            "owner": None,
            "reason": external_reason,
        }
    ]
    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor={},
        run=run,
    )
    assert payload["preserved_external_holds"] == [
        {
            "host": "host_d",
            "kind": "full",
            "lease_id": None,
            "owner": None,
            "reason": external_reason,
            "status": "preserved",
        }
    ]
    assert (
        "PRESERVED EXTERNAL HOLD host_d full owner=external "
        f"reason={rendered_reason}"
        in fleet_rollout.render_result_text(payload)
    )


def test_old_borrowed_hold_does_not_relabel_the_rollout_reason_as_external() -> None:
    plan = _plan()
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-old-borrowed-local",
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        holds={
            "host_d": {
                "host": "host_d",
                "kind": "full",
                "owned": False,
                "status": "preserved",
                "reason": fleet_rollout._hold_reason(
                    "v0.15.60-old-borrowed-local", "host_d"
                ),
                "preexisting": True,
            }
        },
    )

    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor={},
        run=run,
    )

    assert payload["preserved_external_holds"][0]["reason"] is None
    assert "reason=no reason recorded" in fleet_rollout.render_result_text(payload)
    assert "vq rollout-latest" not in fleet_rollout.render_result_text(payload)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (
            {
                "scheduler_hosts": ["host_f"],
                "legacy_scheduler_hosts": ["host_f"],
                "scheduler_leases": [],
                "state": {"reason": "legacy operator hold"},
            },
            [
                {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "lease_id": None,
                    "owner": "legacy",
                    "reason": "legacy operator hold",
                }
            ],
        ),
        (
            {
                "is_full_drain": True,
                "scheduler_hosts": [],
                "scheduler_leases": [],
                "state": {"reason": "global maintenance"},
            },
            [
                {
                    "host": "host_f",
                    "kind": "full",
                    "lease_id": None,
                    "owner": None,
                    "reason": "global maintenance",
                }
            ],
        ),
        (
            {
                "is_full_drain": True,
                "scheduler_hosts": ["host_f"],
                "scheduler_leases": [],
                "state": {"reason": "combined maintenance"},
            },
            [
                {
                    "host": "host_f",
                    "kind": "full",
                    "lease_id": None,
                    "owner": None,
                    "reason": "combined maintenance",
                },
                {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "lease_id": None,
                    "owner": None,
                    "reason": "combined maintenance",
                },
            ],
        ),
        (
            {
                "scheduler_hosts": ["host_f"],
                "scheduler_leases": [
                    {
                        "lease_id": "operator-host_f",
                        "scheduler_host": "host_f",
                        "owner": "operator",
                        "reason": "operator maintenance",
                    },
                    {
                        "lease_id": "rollout-host_f",
                        "scheduler_host": "host_f",
                        "owner": "fleet-rollout:test:host_f",
                        "reason": "vq rollout-latest test host=host_f",
                    },
                ],
                "state": {"reason": "operator maintenance"},
            },
            [
                {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "lease_id": "operator-host_f",
                    "owner": "operator",
                    "reason": "operator maintenance",
                }
            ],
        ),
    ],
)
def test_external_scheduler_hold_projection_covers_every_store_shape(
    status: dict[str, Any],
    expected: list[dict[str, Any]],
) -> None:
    assert fleet_rollout._external_scheduler_holds(
        status,
        host="host_f",
        rollout_owner="fleet-rollout:test:host_f",
        rollout_owns_legacy=False,
    ) == expected


def test_resumed_rollout_migrates_its_legacy_scheduler_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-lease owned lane gains a lease before its old lane is removed."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = fleet_rollout.RolloutAction(
        id="host_f:vibeqc-dev",
        phase="scheduler-runtime",
        host="host_f",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_f"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    rollout_id = "v0.15.60-legacy-resume"
    reason = fleet_rollout._hold_reason(rollout_id, "host_f")
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        holds={
            "host_f": {
                "host": "host_f",
                "kind": "scheduler-target",
                "owned": True,
                "status": "cleanup-failed",
                "reason": reason,
            }
        },
    )
    controls: list[list[str]] = []
    lease_owner = f"fleet-rollout:{rollout_id}:host_f"
    lease_present = False
    fail_legacy_cleanup = True

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal lease_present
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": True,
                        "is_full_drain": False,
                        "scheduler_hosts": ["host_f"],
                        "legacy_scheduler_hosts": ["host_f"],
                        "scheduler_leases": (
                            [
                                {
                                    "scheduler_host": "host_f",
                                    "owner": lease_owner,
                                }
                            ]
                            if lease_present
                            else []
                        ),
                        "state": {
                            "reason": reason,
                            "set_at": "2026-08-08T12:00:00+00:00",
                        },
                    }
                ),
                stderr="",
            )
        if "--release-legacy-only" in args and fail_legacy_cleanup:
            return subprocess.CompletedProcess(
                argv, 9, stdout="", stderr="legacy cleanup failed"
            )
        if "--scheduler-host" in args and "--release" not in args:
            lease_present = True
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(fleet_rollout.FleetRolloutError, match="legacy cleanup"):
        fleet_rollout.acquire_rollout_hold(
            plan,
            run,
            host="host_f",
            actions=[action],
            runner=control_runner,
        )
    assert run.holds["host_f"]["lease_owner"] == lease_owner
    assert run.holds["host_f"]["legacy_migration_pending"] is True

    fail_legacy_cleanup = False
    hold = fleet_rollout.acquire_rollout_hold(
        plan,
        run,
        host="host_f",
        actions=[action],
        runner=control_runner,
    )

    assert "--lease-owner" in controls[1]
    assert "--release-legacy-only" in controls[2]
    assert "--status" in controls[3]
    assert "--release-legacy-only" in controls[4]
    assert hold["legacy_migrated"] is True
    fleet_rollout.release_rollout_hold(
        plan,
        run,
        hold=hold,
        runner=control_runner,
    )
    assert "--lease-owner" in controls[5]


def test_mixed_legacy_canonical_and_exact_alias_holds_release_narrowly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-mixed-alias-journal"
    reason = fleet_rollout._hold_reason(rollout_id, "host_f")
    plan = _scheduler_alias_plan(
        _scheduler_runtime_action("vibeqc-dev", decision="skip"),
        _scheduler_runtime_action("vibe-view", decision="skip"),
    )
    alias_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f-big")
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={
                # Pre-owner journal shape: missing action_host and lease_owner
                # means the key itself is the canonical protected host.
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "cleanup-failed",
                    "reason": reason,
                },
                "host_f-big": {
                    "host": "host_f-big",
                    "action_host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": reason,
                    "lease_owner": alias_owner,
                    "control_host": "driver-b",
                },
            },
        )
    )
    controls: list[list[str]] = []
    legacy_present = True
    owners: set[tuple[str, str, str]] = {
        ("driver-b", "host_f-big", alias_owner),
    }

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del argv, kwargs
        raise AssertionError("a skip-only mixed-journal resume must not update")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal legacy_present
        del kwargs
        args = argv[3:]
        controls.append(args)
        control = args[-1]
        if "--status" in args:
            canonical_owner = fleet_rollout._hold_lease_owner(
                rollout_id, "host_f"
            )
            has_owner = (control, "host_f", canonical_owner) in owners
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": legacy_present or has_owner,
                        "is_full_drain": False,
                        "scheduler_hosts": ["host_f"],
                        "legacy_scheduler_hosts": (
                            ["host_f"] if legacy_present else []
                        ),
                        "scheduler_leases": (
                            [
                                {
                                    "scheduler_host": "host_f",
                                    "owner": canonical_owner,
                                }
                            ]
                            if has_owner
                            else []
                        ),
                        "state": {
                            "reason": reason,
                            "set_at": "2026-08-08T12:00:00+00:00",
                        },
                    }
                ),
                stderr="",
            )
        target = args[args.index("--scheduler-host") + 1]
        if "--release-legacy-only" in args:
            assert target == "host_f"
            assert any(item[1] == "host_f" for item in owners)
            legacy_present = False
        else:
            owner = args[args.index("--lease-owner") + 1]
            key = (control, target, owner)
            if "--release" in args:
                owners.discard(key)
            else:
                owners.add(key)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=update_runner,
        control_runner=control_runner,
    )

    assert legacy_present is False
    assert owners == set()
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f-big"]["status"] == "released"
    mutation_calls = [args for args in controls if "--status" not in args]
    assert any("--release-legacy-only" in args for args in mutation_calls)
    assert all(
        "--lease-owner" in args or "--release-legacy-only" in args
        for args in mutation_calls
    )
    assert any(args[-1] == "driver-b" for args in mutation_calls)


def test_rollout_coordinator_lock_fences_a_second_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Different accepted reports still share one fleet mutation fence."""
    state_root = tmp_path / "state"
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_root))
    rollout_id = "v0.15.60-concurrent"
    script = """
from vq import fleet_rollout

try:
    with fleet_rollout._rollout_execution_lock(  # noqa: SLF001
        'v0.15.61-different-report'
    ):
        pass
except fleet_rollout.FleetRolloutError as exc:
    print(exc)
    raise SystemExit(73)
"""

    with fleet_rollout._rollout_execution_lock(rollout_id):  # noqa: SLF001
        blocked = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )

    admitted = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )

    assert blocked.returncode == 73
    assert "already executing" in blocked.stdout
    assert admitted.returncode == 0, admitted.stderr


def test_finalize_run_preserves_newer_journaled_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-verification save never overwrites another coordinator's hold."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-stale-finalizer"
    stale = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256="a" * 64,
        report_source_path="accepted.json",
        actions={"old": {"status": "success"}},
    )
    newest = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256="a" * 64,
        report_source_path="accepted.json",
        actions={"new": {"status": "running"}},
        holds={
            "host_f": {
                "host": "host_f",
                "kind": "scheduler-target",
                "owned": True,
                "status": "active",
                "lease_owner": "fleet-rollout:test:host_f",
            }
        },
    )
    fleet_rollout.save_run(newest)

    finalized = fleet_rollout.finalize_run(stale, complete=True)

    assert finalized.actions == newest.actions
    assert finalized.holds == newest.holds
    assert finalized.complete is False


def test_finalize_run_rejects_a_newer_accepted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run = fleet_rollout.RolloutRun(
        rollout_id="old-report",
        report_digest_sha256="a" * 64,
        report_source_path="accepted-old.json",
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="changed"):
        fleet_rollout.finalize_run(
            run,
            complete=True,
            report_digest_resolver=lambda: "b" * 64,
        )


def test_execute_plan_rechecks_report_before_each_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[
            fleet_rollout.RolloutAction(
                id=f"{host}:vibeqc-dev",
                phase="local-runtime",
                host=host,
                program="vibeqc-dev",
                pin_name="dev",
                target_sha=DEV,
                target_version="0.15.61.dev0",
                target_tag=None,
                argv=["admin", "update", "vibeqc-dev", host],
                decision="update",
                reason="target is newer",
                before={},
            )
            for host in ("alpha", "beta")
        ],
    )
    current_digest = [str(plan.report["digest_sha256"])]
    executed: list[str] = []

    def update_runner(
        argv: list[str], **_kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        executed.append(argv[-1])
        current_digest[0] = "b" * 64
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="report-switch-mid-run",
        runner=update_runner,
        report_digest_resolver=lambda: current_digest[0],
    )

    assert executed == ["alpha"]
    assert "beta" in run.failed_hosts
    assert "accepted fleet report changed" in run.failed_hosts["beta"]


def test_new_driver_attempt_clears_stale_completion_before_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[],
    )
    action = fleet_rollout.RolloutAction(
        id="driver",
        phase="driver",
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.17.0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-queue", "localhost"],
        decision="update",
        reason="target is newer",
        before={},
    )
    run = fleet_rollout.RolloutRun(
        rollout_id="completed-before-retry",
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        complete=True,
        failed_hosts={"old": "old failure"},
    )
    fleet_rollout.save_run(run)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="failed with"):
        fleet_rollout.execute_one(
            plan,
            action,
            rollout_id=run.rollout_id,
            runner=lambda argv, **_kwargs: subprocess.CompletedProcess(
                argv, 9, stdout="", stderr="failed"
            ),
        )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.complete is False
    assert persisted.failed_hosts == {}
    assert persisted.actions["driver"]["status"] == "failed"


def test_acquire_response_loss_is_journaled_before_legacy_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An uncertain new lease never strands or prematurely lifts the old one."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-acquire-response-loss"
    reason = fleet_rollout._hold_reason(rollout_id, "host_f")
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    action = fleet_rollout.RolloutAction(
        id="host_f:vibeqc-dev",
        phase="scheduler-runtime",
        host="host_f",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_f"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        holds={
            "host_f": {
                "host": "host_f",
                "kind": "scheduler-target",
                "owned": True,
                "status": "active",
                "reason": reason,
            }
        },
    )

    def interrupted_control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        if "--status" in args:
            payload = {
                "active": True,
                "is_full_drain": False,
                "scheduler_hosts": ["host_f"],
                "legacy_scheduler_hosts": ["host_f"],
                "scheduler_leases": [],
                "state": {
                    "reason": reason,
                    "set_at": "2026-08-08T12:00:00+00:00",
                },
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        persisted = fleet_rollout.load_run(rollout_id)
        assert persisted is not None
        pending = persisted.holds["host_f"]
        assert pending["lease_owner"] == lease_owner
        assert pending["acquire_unconfirmed"] is True
        assert pending["legacy_migration_pending"] is True
        raise KeyboardInterrupt("response lost after possible commit")

    with pytest.raises(KeyboardInterrupt, match="response lost"):
        fleet_rollout.acquire_rollout_hold(
            plan,
            run,
            host="host_f",
            actions=[action],
            runner=interrupted_control,
        )

    persisted = fleet_rollout.load_run(rollout_id)
    assert persisted is not None
    uncertain = persisted.holds["host_f"]
    cleanup_calls: list[list[str]] = []

    def cleanup_control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        cleanup_calls.append(argv[3:])
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    fleet_rollout.release_rollout_hold(
        plan,
        persisted,
        hold=uncertain,
        runner=cleanup_control,
    )

    assert len(cleanup_calls) == 1
    assert "--lease-owner" in cleanup_calls[0]
    assert "--release-legacy-only" not in cleanup_calls[0]
    restored = persisted.holds["host_f"]
    assert restored["status"] == "active"
    assert "lease_owner" not in restored
    assert "acquire_unconfirmed" not in restored


def test_execute_plan_cleans_unassigned_claim_after_acquire_interrupt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ctrl-C after a possible commit is recovered from the durable journal."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-interrupted-acquire"
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_f:{program}",
            phase="scheduler-runtime",
            host="host_f",
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, "host_f"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for program in ("vibeqc-dev", "vibe-view")
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    controls: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            payload = {
                "active": False,
                "is_full_drain": False,
                "scheduler_hosts": [],
                "legacy_scheduler_hosts": [],
                "scheduler_leases": [],
                "state": None,
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        if "--release" not in args:
            persisted = fleet_rollout.load_run(rollout_id)
            assert persisted is not None
            pending = persisted.holds["host_f"]
            assert pending["acquire_unconfirmed"] is True
            raise KeyboardInterrupt("response lost after possible commit")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(KeyboardInterrupt, match="response lost"):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout_id,
            runner=lambda *_args, **_kwargs: pytest.fail(
                "no update may start before the hold is confirmed"
            ),
            control_runner=control_runner,
        )

    assert len(controls) == 3
    assert "--status" in controls[0]
    assert "--release" not in controls[1]
    assert "--release" in controls[2]
    owner_index = controls[2].index("--lease-owner")
    assert controls[2][owner_index + 1] == lease_owner
    persisted = fleet_rollout.load_run(rollout_id)
    assert persisted is not None
    assert persisted.holds["host_f"]["status"] == "released"


@pytest.mark.parametrize("decision", ["skip", "update"])
def test_execute_plan_migrates_old_legacy_hold_with_at_most_one_update(
    decision: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Old rollout journals are migrated even below the multi-lane gate."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = f"v0.15.60-old-legacy-{decision}"
    reason = fleet_rollout._hold_reason(rollout_id, "host_f")
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    action = fleet_rollout.RolloutAction(
        id="host_f:vibeqc-dev",
        phase="scheduler-runtime",
        host="host_f",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_f"],
        decision=decision,
        reason=(
            "already at target with LAST OK=true"
            if decision == "skip"
            else "target is newer"
        ),
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    state_path = fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": reason,
                }
            },
        )
    )
    state_before_reconciliation = (
        state_path.read_bytes(),
        state_path.stat().st_mtime_ns,
    )
    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        inspect_only=False,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "global reconciliation must preserve a pre-owner scheduler hold "
            "for plan-aware migration"
        ),
    )
    assert (
        state_path.read_bytes(),
        state_path.stat().st_mtime_ns,
    ) == state_before_reconciliation
    events: list[str] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        if "--status" in args:
            events.append("status")
            payload = {
                "active": True,
                "is_full_drain": False,
                "scheduler_hosts": ["host_f"],
                "legacy_scheduler_hosts": ["host_f"],
                "scheduler_leases": [],
                "state": {
                    "reason": reason,
                    "set_at": "2026-08-08T12:00:00+00:00",
                },
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        if "--release-legacy-only" in args:
            events.append("legacy-release")
        elif "--release" in args:
            events.append("lease-release")
            owner_index = args.index("--lease-owner")
            assert args[owner_index + 1] == lease_owner
        else:
            events.append("lease-acquire")
            owner_index = args.index("--lease-owner")
            assert args[owner_index + 1] == lease_owner
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def fake_execute_one(
        _plan: fleet_rollout.RolloutPlan,
        _action: fleet_rollout.RolloutAction,
        *,
        rollout_id: str,
        runner: fleet_rollout.Runner,
        report_digest_resolver: fleet_rollout.ReportDigestResolver | None = None,
    ) -> fleet_rollout.RolloutRun:
        del runner, report_digest_resolver
        events.append("update")
        persisted = fleet_rollout.load_run(rollout_id)
        assert persisted is not None
        return persisted

    monkeypatch.setattr(fleet_rollout, "execute_one", fake_execute_one)
    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("the real update runner must not be called")
        ),
        control_runner=control_runner,
    )

    expected = ["status", "lease-acquire", "legacy-release"]
    if decision == "update":
        expected.append("update")
    expected.append("lease-release")
    assert events == expected
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f"]["legacy_migrated"] is True


def test_legacy_scheduler_hold_without_current_lane_blocks_actionably(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-orphan-legacy-scheduler"
    digest = "a" * 64
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=digest,
            report_source_path="report.json",
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": fleet_rollout._hold_reason(rollout, "host_f"),
                }
            },
        )
    )
    local = _safe_durable_action()
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[local],
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="legacy scheduler rollout hold.*no matching scheduler lane",
    ):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout,
            runner=lambda *args, **kwargs: pytest.fail(
                "no update may run while the legacy hold is unresolved"
            ),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "a legacy hold without a lane cannot be mutated blindly"
            ),
        )

    persisted = fleet_rollout.load_run(rollout)
    assert persisted is not None
    assert persisted.holds["host_f"]["status"] == "active"


def test_cross_report_legacy_scheduler_hold_blocks_global_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.59-obsolete-legacy-scheduler"
    old_digest = "a" * 64
    state_path = fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=old_digest,
            report_source_path="old-report.json",
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": fleet_rollout._hold_reason(rollout, "host_f"),
                }
            },
        )
    )
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="obsolete rollout.*scheduler hold.*cannot be.*reconciled",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "cross-report legacy ownership must not be mutated blindly"
            ),
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def _obsolete_preowner_scheduler_claim(
    tmp_path: Path,
    *,
    rollout_id: str = "v0.15.117-9c714aab5b75",
    host: str = "host_c",
) -> tuple[fleet_rollout.RolloutRun, Path]:
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=(
            "9c714aab5b759817dec874231eff3340395786ed4777d2b4b275326b74c1e260"
        ),
        report_source_path="vibe-queue/releases/v0.15.117.json",
        actions={
            "scheduler-runtime:host_c:vibeqc-dev": {
                "status": "success",
            },
            "scheduler-runtime:host_c:vibeqc-release": {
                "status": "not-run",
            },
        },
        holds={
            host: {
                "host": host,
                "kind": "scheduler-target",
                "owned": True,
                "preexisting": False,
                "reason": fleet_rollout._hold_reason(rollout_id, host),
                "status": "active",
            }
        },
        complete=True,
    )
    return run, fleet_rollout.save_run(run)


def _obsolete_scheduler_report() -> fleet_release.FleetReleaseReport:
    pins = dict(_report().pins)
    pins["release"] = _pin(
        "release",
        RELEASE,
        "0.15.117",
        tag="v0.15.117",
    )
    return fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.117.json",
        digest_sha256=(
            "9c714aab5b759817dec874231eff3340395786ed4777d2b4b275326b74c1e260"
        ),
        generated_at="2026-08-08T12:00:00Z",
        release_version=(0, 15, 117),
        pins=pins,
        raw={},
    )


def _authenticate_obsolete_scheduler_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    historical = _obsolete_scheduler_report()

    def discover(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        del repo
        assert fetch is False
        assert source_path == historical.source_path
        assert digest == historical.digest_sha256
        return historical

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        discover,
    )


def _legacy_scheduler_reconciliation_plan(
    *,
    host: str = "host_c",
    controls: tuple[tuple[str, str], ...] | None = None,
) -> fleet_rollout.RolloutPlan:
    action = fleet_rollout.RolloutAction(
        id=f"scheduler-runtime:{host}:vibeqc-dev",
        phase="scheduler-runtime",
        host=host,
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.133.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", host],
        decision="skip",
        reason="already at target with LAST OK=true",
        before={
            "configured": True,
            "last_ok": True,
            "current_sha": DEV,
        },
    )
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report={
            "digest_sha256": "b" * 64,
            "source_path": "vibe-queue/releases/v0.15.132.json",
        },
        topology={},
        actions=[action],
        _scheduler_hold_targets={
            host: controls or ((host, "localhost"),),
        },
    )


def _supported_drain_snapshot(
    *,
    host: str = "host_c",
    reason: str | None = None,
    set_at: str = "2026-08-08T12:00:00+00:00",
    legacy: bool = False,
    lease_owner: str | None = None,
) -> dict[str, Any]:
    leases = []
    if lease_owner is not None:
        leases.append(
            {
                "lease_id": "1" * 32,
                "scheduler_host": host,
                "owner": lease_owner,
                "set_at": set_at,
                "reason": reason,
            }
        )
    active = legacy or bool(leases)
    policy = None
    if active:
        policy = {
            "active": True,
            "is_full_drain": False,
            "max_jobs": None,
            "max_cpus": None,
            "reason": reason,
            "set_at": set_at,
            "scheduler_hosts": [host],
            "legacy_scheduler_hosts": [host] if legacy else [],
            "full_dispatch": False,
            "reject_submits": False,
            "update_mode": None,
            "duration_seconds": None,
        }
    return {
        "schema": "vq.drain.read_only_status/1",
        "observed_at": "2026-08-13T12:00:00+00:00",
        "provenance": {
            "method": "get_drain_read_only_snapshot",
            "version": "1",
            "source_sha": "1" * 40,
            "source_tree_sha256": "2" * 64,
            "multi_user": True,
        },
        "coverage": {"legacy_state": True, "scheduler_leases": True},
        "active": active,
        "policy": policy,
        "scheduler_leases": leases,
        "safety_fail_closed": False,
    }


def test_explicit_reconcile_inventories_obsolete_preowner_scheduler_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    before_hold = dict(run.holds["host_c"])

    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "global inventory must not inspect or release a scheduler claim"
        ),
    )

    assert result.legacy_scheduler_holds == ((run.rollout_id, "host_c"),)
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"] == before_hold
    assert not persisted.legacy_retained_holds
    assert state_path.is_file()


def test_obsolete_scheduler_claim_inactive_snapshot_settles_journal_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert controls == [
        [
            "drain",
            "--status",
            "--json",
            "--read-only-snapshot",
            "localhost",
        ]
    ]
    assert result.settled_inactive_holds == ((run.rollout_id, "host_c"),)
    assert result.released_live_holds == ()
    assert result.live_hold_state_changed is False
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"]["status"] == "released"


@pytest.mark.parametrize(
    "release_output",
    ["scheduler drain released for host_c\n", "scheduler drain was not set\n"],
)
def test_obsolete_scheduler_claim_exact_live_legacy_pair_settles_after_recheck(
    release_output: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    reason = fleet_rollout._hold_reason(run.rollout_id, "host_c")
    set_at = "2026-08-08T12:00:00+00:00"
    controls: list[list[str]] = []
    legacy_present = True

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal legacy_present
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    _supported_drain_snapshot(
                        reason=reason,
                        set_at=set_at,
                        legacy=legacy_present,
                    )
                ),
                stderr="",
            )
        legacy_present = False
        return subprocess.CompletedProcess(
            argv, 0, stdout=release_output, stderr=""
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert controls == [
        [
            "drain",
            "--status",
            "--json",
            "--read-only-snapshot",
            "localhost",
        ],
        [
            "drain",
            "--release",
            "--scheduler-host",
            "host_c",
            "--release-legacy-only",
            "--expected-legacy-set-at",
            set_at,
            "--expected-legacy-reason",
            reason,
            "localhost",
        ],
        [
            "drain",
            "--status",
            "--json",
            "--read-only-snapshot",
            "localhost",
        ],
    ]
    assert result.settled_inactive_holds == ((run.rollout_id, "host_c"),)
    assert result.released_live_holds == ()
    assert result.live_hold_state_changed is False
    assert result.live_hold_refresh_required is True
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"]["status"] == "released"


def test_scheduler_release_with_unobservable_post_state_persists_fence_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    reason = fleet_rollout._hold_reason(run.rollout_id, "host_c")
    calls = 0

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        del kwargs
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    _supported_drain_snapshot(reason=reason, legacy=True)
                ),
                stderr="",
            )
        if calls == 2:
            return subprocess.CompletedProcess(
                argv, 0, stdout="scheduler drain released\n", stderr=""
            )
        return subprocess.CompletedProcess(
            argv, 255, stdout="", stderr="network unavailable"
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert calls == 3
    assert result.live_hold_refresh_required is True
    assert result.settled_inactive_holds == ()
    assert "post-release legacy absence could not be proven" in (
        result.retained_holds[0][2]
    )
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"]["status"] == "active"
    assert persisted.legacy_retained_holds["host_c"]["source"] == (
        "scheduler-observation"
    )


@pytest.mark.parametrize("live_case", ["unreachable", "mismatched", "lease"])
def test_obsolete_scheduler_claim_unknown_or_mismatched_remains_fenced(
    live_case: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    before_hold = dict(run.holds["host_c"])
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        if live_case == "unreachable":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unreachable"
            )
        payload = _supported_drain_snapshot(
            reason="operator maintenance",
            legacy=live_case == "mismatched",
            lease_owner="operator" if live_case == "lease" else None,
        )
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(payload), stderr=""
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert len(controls) == 1
    assert "--release" not in controls[0]
    assert result.settled_inactive_holds == ()
    assert result.released_live_holds == ()
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_c")
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"] == before_hold
    assert "host_c" in persisted.legacy_retained_holds


@pytest.mark.parametrize("failure", ["malformed-snapshot", "ambiguous-plan"])
def test_obsolete_scheduler_claim_malformed_or_ambiguous_is_atomic(
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    controls = (
        (("host_c", "localhost"), ("host_c", "other-driver"))
        if failure == "ambiguous-plan"
        else None
    )
    plan = _legacy_scheduler_reconciliation_plan(controls=controls)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if failure == "ambiguous-plan":
            pytest.fail("ambiguous plan must fail before live inspection")
        return subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")

    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_scheduler_holds=((run.rollout_id, "host_c"),),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=control_runner,
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def test_obsolete_scheduler_claim_conditional_release_failure_is_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    before_hold = dict(run.holds["host_c"])
    reason = fleet_rollout._hold_reason(run.rollout_id, "host_c")
    calls = 0

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal calls
        del kwargs
        calls += 1
        if calls == 1:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    _supported_drain_snapshot(reason=reason, legacy=True)
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(
            argv,
            1,
            stdout="",
            stderr="legacy set_at changed",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert calls == 2
    assert result.released_live_holds == ()
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_c")
    assert "conditional legacy release failed" in result.retained_holds[0][2]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"] == before_hold
    assert "host_c" in persisted.legacy_retained_holds


def test_obsolete_scheduler_claim_active_release_respects_update_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    before_hold = dict(run.holds["host_c"])
    marker = {
        "host": "host_c",
        "envs": ["scheduler-runtime:host_c:vibeqc-dev"],
        "readable": True,
    }
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(
                _supported_drain_snapshot(
                    reason=fleet_rollout._hold_reason(
                        run.rollout_id, "host_c"
                    ),
                    legacy=True,
                )
            ),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={"host_c": {"marker": marker, "markers": [marker]}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert len(controls) == 1
    assert result.released_live_holds == ()
    assert "overlapping admin-update marker" in result.retained_holds[0][2]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"] == before_hold
    assert "host_c" in persisted.legacy_retained_holds


def test_obsolete_scheduler_claim_incomplete_read_only_coverage_is_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    before_hold = dict(run.holds["host_c"])
    snapshot = _supported_drain_snapshot()
    snapshot.update(
        {
            "coverage": {"legacy_state": False, "scheduler_leases": False},
            "active": True,
            "policy": {
                "active": True,
                "is_full_drain": True,
                "max_jobs": None,
                "max_cpus": None,
                "reason": (
                    "scheduler drain inventory unreadable; dispatch held safe"
                ),
                "set_at": snapshot["observed_at"],
                "scheduler_hosts": [],
                "legacy_scheduler_hosts": [],
                "full_dispatch": True,
                "reject_submits": False,
                "update_mode": None,
                "duration_seconds": None,
            },
            "safety_fail_closed": True,
        }
    )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(snapshot), stderr=""
        ),
    )

    assert result.released_live_holds == ()
    assert "coverage is incomplete" in result.retained_holds[0][2]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_c"] == before_hold
    assert "host_c" in persisted.legacy_retained_holds


def test_obsolete_scheduler_claim_batch_prevalidates_before_first_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    reason = fleet_rollout._hold_reason(run.rollout_id, "host_f")
    run.holds["host_f"] = {
        "host": "host_f",
        "kind": "scheduler-target",
        "owned": True,
        "preexisting": False,
        "reason": reason + "-tampered",
        "status": "active",
    }
    fleet_rollout.save_run(run)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)
    plan = _legacy_scheduler_reconciliation_plan()
    plan.actions.append(
        fleet_rollout.RolloutAction(
            id="scheduler-runtime:host_f:vibeqc-dev",
            phase="scheduler-runtime",
            host="host_f",
            program="vibeqc-dev",
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.133.dev0",
            target_tag=None,
            argv=["admin", "update", "vibeqc-dev", "host_f"],
            decision="skip",
            reason="already at target with LAST OK=true",
            before={
                "configured": True,
                "last_ok": True,
                "current_sha": DEV,
            },
        )
    )
    plan._scheduler_hold_targets["host_f"] = (("host_f", "localhost"),)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="malformed"):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_scheduler_holds=(
                (run.rollout_id, "host_c"),
                (run.rollout_id, "host_f"),
            ),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "every claim must prevalidate before the first live probe"
            ),
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


@pytest.mark.parametrize(
    "corruption",
    [
        "preexisting-missing",
        "preexisting-true",
        "failed-host",
        "active-cleanup-error",
        "cleanup-failed-without-error",
    ],
)
def test_obsolete_scheduler_claim_rejects_ambiguous_historical_shape(
    corruption: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    hold = run.holds["host_c"]
    if corruption == "preexisting-missing":
        hold.pop("preexisting")
    elif corruption == "preexisting-true":
        hold["preexisting"] = True
    elif corruption == "failed-host":
        run.failed_hosts["host_c"] = "historical lane failed"
    elif corruption == "active-cleanup-error":
        hold["cleanup_error"] = "unexpected"
    else:
        hold["status"] = "cleanup-failed"
    fleet_rollout.save_run(run)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    with pytest.raises(fleet_rollout.FleetRolloutError, match="malformed"):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_scheduler_holds=((run.rollout_id, "host_c"),),
            plan=_legacy_scheduler_reconciliation_plan(),
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "ambiguous historical shape must fail before observation"
            ),
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def test_released_obsolete_scheduler_claim_is_not_reprobed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _authenticate_obsolete_scheduler_report(monkeypatch)
    run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    plan = _legacy_scheduler_reconciliation_plan()
    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot()),
            stderr="",
        ),
    )

    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "released history must not inspect a later external drain"
        ),
    )

    assert result.legacy_scheduler_holds == ()


def test_real_legacy_batch_retains_host_b_and_reconciles_host_f_and_host_c(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The four actual source identities no longer form one global block."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_f_release_digest = (
        "308927e7256e46ca119149a105aa47f7816ef0abd56ac405f982fdf3bd1be9d5"
    )
    host_f_dev_digest = (
        "edaef4dec3e8193e9281cde9d31dd27efa583427d18aa601a38b6b9d235b4f96"
    )
    host_b_digest = (
        "b9ea64e7621418d52e452c8a38002783c2832bbb574e609210af1930aef16a1b"
    )
    release_60_sha = "86dc2ed623ddb54851f011842cb96e25a61db989"
    dev_60_sha = "bb0c17224260447b5ca8dad3765ae486b5db05ae"
    release_118_sha = "b11767e4872e7545737685b3d0b9bdc4dede8e8a"

    pins_60 = {
        **dict(_report().pins),
        "release": _pin(
            "release", release_60_sha, "0.15.60", tag="v0.15.60"
        ),
        "dev": _pin("dev", dev_60_sha, "0.15.61.dev0"),
    }
    pins_118 = {
        **dict(_report().pins),
        "release": _pin(
            "release", release_118_sha, "0.15.118", tag="v0.15.118"
        ),
    }

    def historical(
        *,
        version: tuple[int, int, int],
        digest: str,
        path: str,
        pins: Mapping[str, fleet_release.FleetPin],
    ) -> fleet_release.FleetReleaseReport:
        return fleet_release.FleetReleaseReport(
            source_ref="origin/main",
            source_path=path,
            digest_sha256=digest,
            generated_at="2026-08-08T12:00:00Z",
            release_version=version,
            pins=pins,
            raw={},
        )

    reports = {
        host_f_release_digest: historical(
            version=(0, 15, 60),
            digest=host_f_release_digest,
            path="vibe-queue/releases/v0.15.60.json",
            pins=pins_60,
        ),
        host_f_dev_digest: historical(
            version=(0, 15, 60),
            digest=host_f_dev_digest,
            path="vibe-queue/releases/v0.15.60.json",
            pins=pins_60,
        ),
        host_b_digest: historical(
            version=(0, 15, 118),
            digest=host_b_digest,
            path="vibe-queue/releases/v0.15.118.json",
            pins=pins_118,
        ),
        _obsolete_scheduler_report().digest_sha256: (
            _obsolete_scheduler_report()
        ),
    }

    def discover(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        del repo
        assert fetch is False
        report = reports[digest]
        assert report.source_path == source_path
        return report

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        discover,
    )

    action_rows = (
        (
            "v0.15.60-308927e7256e",
            host_f_release_digest,
            "scheduler-runtime:host_f:vibeqc-release",
            [
                "admin",
                "update",
                "vibeqc-release",
                "host_f",
                "--tag",
                "v0.15.60",
                "--expected-sha",
                release_60_sha,
                "--drain-wait",
                "4h",
            ],
            "vibe-queue/releases/v0.15.60.json",
        ),
        (
            "v0.15.60-edaef4dec3e8",
            host_f_dev_digest,
            "scheduler-runtime:host_f:vibeqc-dev",
            [
                "admin",
                "update",
                "vibeqc-dev",
                "host_f",
                "--expected-sha",
                dev_60_sha,
                "--drain-wait",
                "4h",
            ],
            "vibe-queue/releases/v0.15.60.json",
        ),
        (
            "v0.15.118-b9ea64e76214",
            host_b_digest,
            "local-runtime:host_b:vibeqc-release",
            [
                "admin",
                "update",
                "vibeqc-release",
                "host_b",
                "--tag",
                "v0.15.118",
                "--expected-sha",
                release_118_sha,
            ],
            "vibe-queue/releases/v0.15.118.json",
        ),
    )
    state_paths: dict[str, Path] = {}
    for rollout_id_value, digest, action_id, argv, source_path in action_rows:
        holds: dict[str, dict[str, Any]] = {}
        if action_id.startswith("local-runtime:host_b"):
            holds = {
                host: {
                    "host": host,
                    "kind": "full",
                    "owned": True,
                    "preexisting": False,
                    "reason": fleet_rollout._hold_reason(
                        rollout_id_value, host
                    ),
                    "status": "active",
                }
                for host in ("host_a", "host_b")
            }
        state_paths[rollout_id_value] = fleet_rollout.save_run(
            fleet_rollout.RolloutRun(
                rollout_id=rollout_id_value,
                report_digest_sha256=digest,
                report_source_path=source_path,
                actions={
                    action_id: {
                        "status": "running",
                        "decision": "update",
                        "reason": "target is newer",
                        "argv": argv,
                    }
                },
                holds=holds,
            )
        )
    host_c, host_c_path = _obsolete_preowner_scheduler_claim(tmp_path)
    state_paths[host_c.rollout_id] = host_c_path
    host_b_before = json.loads(
        state_paths["v0.15.118-b9ea64e76214"].read_text()
    )

    def current_action(
        action_id: str,
        *,
        phase: str,
        host: str,
        program: str,
        decision: str,
        reason: str,
    ) -> fleet_rollout.RolloutAction:
        return fleet_rollout.RolloutAction(
            id=action_id,
            phase=phase,  # type: ignore[arg-type]
            host=host,
            program=program,
            pin_name="release" if program == "vibeqc-release" else "dev",
            target_sha=FUTURE,
            target_version="0.15.132",
            target_tag=None,
            argv=["admin", "update", program, host],
            decision=decision,  # type: ignore[arg-type]
            reason=reason,
            before={
                "configured": True,
                "last_ok": decision == "skip",
                "current_sha": FUTURE,
            },
        )

    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={
            "source_path": "vibe-queue/releases/v0.15.132.json",
            "digest_sha256": "c" * 64,
        },
        topology={},
        actions=[
            current_action(
                "scheduler-runtime:host_f:vibeqc-release",
                phase="scheduler-runtime",
                host="host_f",
                program="vibeqc-release",
                decision="skip",
                reason="already at target with LAST OK=true",
            ),
            current_action(
                "scheduler-runtime:host_f:vibeqc-dev",
                phase="scheduler-runtime",
                host="host_f",
                program="vibeqc-dev",
                decision="skip",
                reason="already at target with LAST OK=true",
            ),
            current_action(
                "local-runtime:host_b:vibeqc-release",
                phase="local-runtime",
                host="host_b",
                program="vibeqc-release",
                decision="block",
                reason="host unreachable",
            ),
            current_action(
                "local-runtime:host_a:vibeqc-release",
                phase="local-runtime",
                host="host_a",
                program="vibeqc-release",
                decision="update",
                reason="target is newer",
            ),
            current_action(
                "local-runtime:host_d:vibeqc-release",
                phase="local-runtime",
                host="host_d",
                program="vibeqc-release",
                decision="update",
                reason="target is newer",
            ),
            _legacy_scheduler_reconciliation_plan().actions[0],
        ],
        _scheduler_hold_targets={"host_c": (("host_c", "localhost"),)},
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="c" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "global inventory must issue no legacy controls"
        ),
    )
    assert set(inventory.legacy_running_actions) == {
        (rollout_id_value, action_id)
        for rollout_id_value, _digest, action_id, _argv, _path in action_rows
    }
    assert inventory.legacy_scheduler_holds == (
        (host_c.rollout_id, "host_c"),
    )

    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        assert args[-1] == "localhost"
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=inventory.legacy_running_actions,
        legacy_scheduler_holds=inventory.legacy_scheduler_holds,
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "c" * 64,
        control_runner=control,
    )

    assert controls == [
        [
            "drain",
            "--status",
            "--json",
            "--read-only-snapshot",
            "localhost",
        ]
    ]
    assert set(result.superseded_actions) == {
        (
            "v0.15.60-308927e7256e",
            "scheduler-runtime:host_f:vibeqc-release",
        ),
        (
            "v0.15.60-edaef4dec3e8",
            "scheduler-runtime:host_f:vibeqc-dev",
        ),
    }
    assert result.retained_actions[0][:3] == (
        "v0.15.118-b9ea64e76214",
        "local-runtime:host_b:vibeqc-release",
        "host_b",
    )
    host_b_after = json.loads(
        state_paths["v0.15.118-b9ea64e76214"].read_text()
    )
    assert host_b_after["actions"] == host_b_before["actions"]
    assert host_b_after["holds"] == host_b_before["holds"]
    assert set(host_b_after["legacy_retained_holds"]) == {"host_a", "host_b"}
    assert host_b_after["legacy_retained_actions"]
    host_c_after = fleet_rollout.load_run(host_c.rollout_id)
    assert host_c_after is not None
    assert host_c_after.holds["host_c"]["status"] == "released"

    read_only = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="c" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "receipt reload must not issue controls"
        ),
    )
    fenced = fleet_rollout.fence_retained_legacy_holds(
        plan, read_only.retained_legacy_holds
    )
    decisions = {action.host: action.decision for action in fenced.actions}
    assert decisions["host_b"] == "defer"
    assert decisions["host_a"] == "defer"
    assert decisions["host_d"] == "update"


def test_global_reconcile_preserves_missing_control_host_until_plan_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A process-death journal is cleanup work even with no updates left."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-active-hold-resume"
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    action = fleet_rollout.RolloutAction(
        id="host_f:vibeqc-dev",
        phase="scheduler-runtime",
        host="host_f",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_f"],
        decision="skip",
        reason="already at target with LAST OK=true",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    state_path = fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": fleet_rollout._hold_reason(rollout_id, "host_f"),
                    "lease_owner": lease_owner,
                }
            },
        )
    )
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)
    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        inspect_only=False,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "global reconcile must not guess that the scheduler target owns "
            "the drain store"
        ),
    )
    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before
    controls: list[list[str]] = []

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del argv, kwargs
        raise AssertionError("a skip-only resume must not execute updates")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=update_runner,
        control_runner=control_runner,
    )

    assert len(controls) == 1
    assert controls[0][:3] == ["drain", "--release", "--scheduler-host"]
    assert controls[0][3] == "host_f"
    assert controls[0][4:6] == ["--lease-owner", lease_owner]
    assert controls[0][-1] == "localhost"
    assert run.holds["host_f"]["status"] == "released"
    assert run.actions[action.id]["status"] == "not-run"


def test_cross_report_exact_scheduler_hold_missing_control_host_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.59-old-exact-owner"
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256="a" * 64,
            report_source_path="old-report.json",
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "active",
                    "reason": fleet_rollout._hold_reason(rollout, "host_f"),
                    "lease_owner": fleet_rollout._hold_lease_owner(
                        rollout, "host_f"
                    ),
                }
            },
        )
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="obsolete rollout.*complete current-plan release identity",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "an obsolete release target must never be guessed"
            ),
        )


def test_cross_report_complete_alias_group_requires_matching_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.59-obsolete-explicit-alias-group"
    holds = {
        target: {
            "host": target,
            "action_host": "host_f",
            "kind": "scheduler-target",
            "owned": True,
            "status": "active",
            "reason": fleet_rollout._hold_reason(rollout, target),
            "lease_owner": fleet_rollout._hold_lease_owner(rollout, target),
            "control_host": control,
        }
        for target, control in (
            ("host_f", "driver-a"),
            ("host_f-big", "driver-b"),
        )
    }
    state_path = fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256="a" * 64,
            report_source_path="old-report.json",
            holds=holds,
        )
    )
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="matching report/config",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "obsolete explicit alias groups require plan validation"
            ),
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


_host_f_PLAN_HOLD_TARGETS = (
    "host_f",
    "host_f-amd",
    "host_f-big",
    "host_f-inf",
    "host_f-itwin",
    "host_f-jtwin",
)


def _healthy_plan_bound_supersede_action(
    *,
    report: fleet_release.FleetReleaseReport,
    phase: fleet_rollout.Phase,
    program: str,
) -> fleet_rollout.RolloutAction:
    pin_name = fleet_rollout.PROGRAM_PINS[program]
    pin = report.pins[pin_name]
    if phase == "helper":
        action_id = "helper:host_f"
        argv = [
            "admin",
            "update",
            "host_f",
            "--expected-sha",
            pin.sha,
            "--drain-wait",
            fleet_rollout.DEFAULT_DRAIN_WAIT,
        ]
    else:
        action_id = f"scheduler-runtime:host_f:{program}"
        argv = [
            "admin",
            "update",
            program,
            "host_f",
            *pin.deploy_flags,
            "--drain-wait",
            fleet_rollout.DEFAULT_DRAIN_WAIT,
        ]
    return fleet_rollout.RolloutAction(
        id=action_id,
        phase=phase,
        host="host_f",
        program=program,
        pin_name=pin_name,
        target_sha=pin.sha,
        target_version=pin.version,
        target_tag=pin.tag,
        argv=argv,
        decision="skip",
        reason="already at target with LAST OK=true",
        before={
            "configured": True,
            "current_sha": pin.sha,
            "current_version": pin.version,
            "current_tag": pin.tag,
            "dirty": False,
            "last_ok": True,
            "acknowledged": False,
            "detail": "exact managed scheduler lane",
            "metrics": None,
        },
    )


def _plan_bound_supersede_plan(
    report: fleet_release.FleetReleaseReport,
) -> fleet_rollout.RolloutPlan:
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(report),
        topology={},
        actions=[
            _healthy_plan_bound_supersede_action(
                report=report,
                phase="helper",
                program="vibeqc-queue",
            ),
            _healthy_plan_bound_supersede_action(
                report=report,
                phase="scheduler-runtime",
                program="vibeqc-release",
            ),
            _healthy_plan_bound_supersede_action(
                report=report,
                phase="scheduler-runtime",
                program="vibeqc-dev",
            ),
            _healthy_plan_bound_supersede_action(
                report=report,
                phase="scheduler-runtime",
                program="vibe-view",
            ),
        ],
        _scheduler_hold_targets={
            "host_f": tuple(
                (target, "localhost")
                for target in _host_f_PLAN_HOLD_TARGETS
            )
        },
    )


def _plan_bound_hold(
    rollout_id_value: str,
    *,
    host: str,
) -> dict[str, Any]:
    return {
        "host": host,
        "action_host": "host_f",
        "kind": "scheduler-target",
        "owned": True,
        "status": "active",
        "reason": fleet_rollout._hold_reason(rollout_id_value, host),
        "preexisting": False,
        "lease_owner": fleet_rollout._hold_lease_owner(
            rollout_id_value, host
        ),
        "control_host": "localhost",
        "legacy_migrated": False,
        "legacy_migration_pending": False,
    }


def test_plan_bound_hold_supersede_covers_host_f_six_lane_class(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every v0.15.155 host_f alias is covered, while one omission blocks."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report_at(155, digest="a" * 64)
    current = _report_at(157, digest="c" * 64)
    old_rollout = fleet_rollout.rollout_id(historical)
    holds = {
        host: _plan_bound_hold(old_rollout, host=host)
        for host in _host_f_PLAN_HOLD_TARGETS
    }
    # This is the exact durable crash cut written after a legacy scheduler
    # lane is migrated but before the replacement owner lease is released.
    # Its retained expected pair remains part of the authenticated evidence.
    holds["host_f"].update(
        {
            "legacy_migrated": True,
            "legacy_migration_pending": False,
            "legacy_expected_reason": holds["host_f"]["reason"],
            "legacy_expected_set_at": "2026-08-30T18:00:00+00:00",
        }
    )
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=old_rollout,
            report_digest_sha256=historical.digest_sha256,
            report_source_path=historical.source_path,
            holds=holds,
        )
    )
    plan = _plan_bound_supersede_plan(current)

    def historical_report(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        del repo
        assert fetch is False
        reports = {
            (historical.source_path, historical.digest_sha256): historical,
            (current.source_path, current.digest_sha256): current,
        }
        return reports[(source_path, digest)]

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical_report,
    )
    controls: list[list[str]] = []

    def inactive(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot(host=argv[-1])),
            stderr="",
        )

    for host in _host_f_PLAN_HOLD_TARGETS[:-1]:
        result = fleet_rollout.supersede_obsolete_plan_bound_hold(
            rollout_id_value=old_rollout,
            host=host,
            accepted_report=current,
            plan=plan,
            repo=tmp_path,
            current_report_digest_resolver=lambda: current.digest_sha256,
            control_runner=inactive,
        )
        assert result.replayed is False

    partially_covered = fleet_rollout._validate_running_journals(
        {},
        current_report_source_path=current.source_path,
        current_report_digest_sha256=current.digest_sha256,
        configured_retention_hosts=frozenset(_host_f_PLAN_HOLD_TARGETS),
        report_repo=tmp_path,
    )
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="host_f-jtwin.*matching report/config",
    ):
        fleet_rollout._inventory_legacy_scheduler_holds(
            fleet_rollout._all_rollout_runs(),
            current_report_digest_sha256=current.digest_sha256,
            allow_legacy_reconciliation=False,
            retained_hold_refs=set(
                partially_covered.retained_hold_refs
            ),
        )

    fleet_rollout.supersede_obsolete_plan_bound_hold(
        rollout_id_value=old_rollout,
        host=_host_f_PLAN_HOLD_TARGETS[-1],
        accepted_report=current,
        plan=plan,
        repo=tmp_path,
        current_report_digest_resolver=lambda: current.digest_sha256,
        control_runner=inactive,
    )
    state_path = fleet_rollout.rollout_state_path(old_rollout)
    before_replay = (state_path.read_bytes(), state_path.stat().st_mtime_ns)
    replayed = fleet_rollout.supersede_obsolete_plan_bound_hold(
        rollout_id_value=old_rollout,
        host="host_f",
        accepted_report=current,
        plan=plan,
        repo=tmp_path,
        current_report_digest_resolver=lambda: current.digest_sha256,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "an exact durable replay must not re-probe the fleet"
        ),
    )
    assert replayed.replayed is True
    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before_replay
    assert len(controls) == len(_host_f_PLAN_HOLD_TARGETS)
    assert all(
        args
        == [
            "drain",
            "--status",
            "--json",
            "--read-only-snapshot",
            "localhost",
        ]
        for args in controls
    )

    inventory = fleet_rollout._validate_running_journals(
        {},
        current_report_source_path=current.source_path,
        current_report_digest_sha256=current.digest_sha256,
        configured_retention_hosts=frozenset(_host_f_PLAN_HOLD_TARGETS),
        report_repo=tmp_path,
    )
    assert inventory.retained_fences == ()
    assert inventory.retained_hold_refs == frozenset(
        (old_rollout, host) for host in _host_f_PLAN_HOLD_TARGETS
    )
    assert inventory.superseded_plan_hold_refs == inventory.retained_hold_refs
    assert fleet_rollout._inventory_legacy_scheduler_holds(
        fleet_rollout._all_rollout_runs(),
        current_report_digest_sha256=current.digest_sha256,
        allow_legacy_reconciliation=False,
        retained_hold_refs=set(inventory.retained_hold_refs),
    ) == ()


@pytest.mark.parametrize(
    "tamper",
    ["partial-actions", "foreign-action", "arbitrary-sha"],
)
def test_plan_bound_hold_supersede_receipt_reauthenticates_current_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    tamper: str,
) -> None:
    """Persisted coverage cannot self-attest a partial or foreign plan."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report_at(155, digest="a" * 64)
    current = _report_at(157, digest="c" * 64)
    old_rollout = fleet_rollout.rollout_id(historical)
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=old_rollout,
            report_digest_sha256=historical.digest_sha256,
            report_source_path=historical.source_path,
            holds={"host_f": _plan_bound_hold(old_rollout, host="host_f")},
        )
    )
    reports = {
        (historical.source_path, historical.digest_sha256): historical,
        (current.source_path, current.digest_sha256): current,
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: reports[
            (source_path, digest)
        ],
    )
    fleet_rollout.supersede_obsolete_plan_bound_hold(
        rollout_id_value=old_rollout,
        host="host_f",
        accepted_report=current,
        plan=_plan_bound_supersede_plan(current),
        repo=tmp_path,
        current_report_digest_resolver=lambda: current.digest_sha256,
        control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot(host="host_f")),
            stderr="",
        ),
    )
    persisted = fleet_rollout.load_run(old_rollout)
    assert persisted is not None
    receipt = persisted.legacy_retained_holds["host_f"]
    actions = receipt["current_actions"]
    assert isinstance(actions, list)
    if tamper == "partial-actions":
        receipt["current_actions"] = actions[:1]
    elif tamper == "foreign-action":
        actions[0]["action_id"] = "helper:foreign"
    else:
        for action in actions:
            action["expected_sha"] = "f" * 40
            action["observed_sha"] = "f" * 40
    state_path = fleet_rollout.save_run(persisted)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="plan-bound hold supersede",
    ):
        fleet_rollout._validate_running_journals(
            {},
            current_report_source_path=current.source_path,
            current_report_digest_sha256=current.digest_sha256,
            configured_retention_hosts=frozenset({"host_f"}),
            report_repo=tmp_path,
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


@pytest.mark.parametrize(
    ("failure", "expected_controls", "match"),
    [
        ("malformed-hold", 0, "malformed or ambiguous"),
        ("acquire-unconfirmed-int", 0, "malformed or ambiguous"),
        ("acquire-unconfirmed-null", 0, "malformed or ambiguous"),
        ("migrated-and-pending", 0, "malformed or ambiguous"),
        ("migrated-and-unconfirmed", 0, "malformed or ambiguous"),
        ("migrated-partial-evidence", 0, "malformed or ambiguous"),
        ("empty-external-holds", 0, "malformed or ambiguous"),
        ("unhealthy-current", 0, "lacks strictly healthy"),
        ("wrong-current-pin", 0, "lacks strictly healthy"),
        ("missing-current-action", 0, "exact complete current"),
        ("extra-current-action", 0, "exact complete current"),
        ("static-action-tamper", 0, "lacks strictly healthy"),
        ("report-summary-tamper", 0, "exact accepted report"),
        ("same-version", 0, "not strictly older"),
        ("live-active", 1, "not proven inactive"),
    ],
)
def test_plan_bound_hold_supersede_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    expected_controls: int,
    match: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report_at(155, digest="a" * 64)
    current_patch = 155 if failure == "same-version" else 157
    current = _report_at(current_patch, digest="c" * 64)
    old_rollout = fleet_rollout.rollout_id(historical)
    hold = _plan_bound_hold(old_rollout, host="host_f")
    if failure == "malformed-hold":
        hold["lease_owner"] = "foreign-owner"
    elif failure == "acquire-unconfirmed-int":
        hold["acquire_unconfirmed"] = 1
    elif failure == "acquire-unconfirmed-null":
        hold["acquire_unconfirmed"] = None
    elif failure == "migrated-and-pending":
        hold.update(
            {
                "legacy_migrated": True,
                "legacy_migration_pending": True,
                "legacy_expected_reason": hold["reason"],
                "legacy_expected_set_at": "2026-08-30T18:00:00+00:00",
            }
        )
    elif failure == "migrated-and-unconfirmed":
        hold.update(
            {
                "legacy_migrated": True,
                "legacy_migration_pending": False,
                "acquire_unconfirmed": True,
            }
        )
    elif failure == "migrated-partial-evidence":
        hold.update(
            {
                "legacy_migrated": True,
                "legacy_migration_pending": False,
                "legacy_expected_reason": hold["reason"],
            }
        )
    elif failure == "empty-external-holds":
        hold["external_holds"] = []
    state_path = fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=old_rollout,
            report_digest_sha256=historical.digest_sha256,
            report_source_path=historical.source_path,
            holds={"host_f": hold},
        )
    )
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)
    plan = _plan_bound_supersede_plan(current)
    if failure == "unhealthy-current":
        plan.actions[0].before["last_ok"] = False
    elif failure == "wrong-current-pin":
        plan.actions[0].target_sha = "f" * 40
        plan.actions[0].before["current_sha"] = "f" * 40
    elif failure == "missing-current-action":
        plan.actions.pop()
    elif failure == "extra-current-action":
        plan.actions.append(
            fleet_rollout.RolloutAction(
                **{
                    **plan.actions[-1].__dict__,
                    "id": "scheduler-runtime:host_f:vibe-basis",
                    "program": "vibe-basis",
                }
            )
        )
    elif failure == "static-action-tamper":
        plan.actions[0].argv = [*plan.actions[0].argv, "--unexpected"]
    elif failure == "report-summary-tamper":
        plan.report["unexpected"] = True

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    controls: list[list[str]] = []

    def status(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        snapshot = (
            _supported_drain_snapshot(
                host="host_f",
                reason="external",
                lease_owner="another-rollout",
            )
            if failure == "live-active"
            else _supported_drain_snapshot(host="host_f")
        )
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(snapshot), stderr=""
        )

    with pytest.raises(fleet_rollout.FleetRolloutError, match=match):
        fleet_rollout.supersede_obsolete_plan_bound_hold(
            rollout_id_value=old_rollout,
            host="host_f",
            accepted_report=current,
            plan=plan,
            repo=tmp_path,
            current_report_digest_resolver=lambda: current.digest_sha256,
            control_runner=status,
        )

    assert len(controls) == expected_controls
    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def test_plan_bound_hold_supersede_cas_preserves_concurrent_journal_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report_at(155, digest="a" * 64)
    current = _report_at(157, digest="c" * 64)
    old_rollout = fleet_rollout.rollout_id(historical)
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=old_rollout,
            report_digest_sha256=historical.digest_sha256,
            report_source_path=historical.source_path,
            holds={"host_f": _plan_bound_hold(old_rollout, host="host_f")},
        )
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    def race(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        concurrent = fleet_rollout.load_run(old_rollout)
        assert concurrent is not None
        concurrent.failed_hosts["peer"] = "concurrent evidence"
        fleet_rollout.save_run(concurrent)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot(host="host_f")),
            stderr="",
        )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="changed during plan-bound hold observation",
    ):
        fleet_rollout.supersede_obsolete_plan_bound_hold(
            rollout_id_value=old_rollout,
            host="host_f",
            accepted_report=current,
            plan=_plan_bound_supersede_plan(current),
            repo=tmp_path,
            current_report_digest_resolver=lambda: current.digest_sha256,
            control_runner=race,
        )
    persisted = fleet_rollout.load_run(old_rollout)
    assert persisted is not None
    assert persisted.failed_hosts == {"peer": "concurrent evidence"}
    assert persisted.legacy_retained_holds == {}


def test_plan_bound_hold_supersede_stays_out_of_pending_failure_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unrelated failure transaction cannot resurrect permanent coverage."""
    superseded = ("v0.15.155-deadbeefdead", "host_f")
    retained = ("v0.15.118-old", "host_a")
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        fleet_rollout,
        "_all_rollout_run_snapshots",
        lambda: (),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "_pending_failure_transition_rows",
        lambda snapshots: (("v0.15.156-failure", "host_e", "failed"),),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "_validate_running_journals",
        lambda *args, **kwargs: fleet_rollout._LegacyJournalInventory(
            retained_hold_refs=frozenset({superseded, retained}),
            superseded_plan_hold_refs=frozenset({superseded}),
        ),
    )

    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="c" * 64,
        inspect_only=False,
        allow_legacy_reconciliation=True,
    )

    assert result.pending_failure_transitions == (
        ("v0.15.156-failure", "host_e", "failed"),
    )
    assert result.legacy_hold_retries == (retained,)


def test_skip_only_resume_migrates_legacy_lane_before_exact_owner_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pending legacy cleanup is journaled before the owner lease is released."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-pending-legacy-skip-resume"
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    action = fleet_rollout.RolloutAction(
        id="host_f:vibeqc-dev",
        phase="scheduler-runtime",
        host="host_f",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_f"],
        decision="skip",
        reason="already at target with LAST OK=true",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_digest_sha256=str(plan.report["digest_sha256"]),
            report_source_path=str(plan.report["source_path"]),
            holds={
                "host_f": {
                    "host": "host_f",
                    "kind": "scheduler-target",
                    "owned": True,
                    "status": "cleanup-failed",
                    "reason": fleet_rollout._hold_reason(rollout_id, "host_f"),
                    "lease_owner": lease_owner,
                    "legacy_migrated": False,
                    "legacy_migration_pending": True,
                }
            },
        )
    )
    controls: list[list[str]] = []
    legacy_present = True
    lease_present = False

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del argv, kwargs
        raise AssertionError("a skip-only resume must not execute updates")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal legacy_present, lease_present
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": True,
                        "is_full_drain": False,
                        "scheduler_hosts": ["host_f"],
                        "legacy_scheduler_hosts": ["host_f"],
                        "scheduler_leases": (
                            [
                                {
                                    "scheduler_host": "host_f",
                                    "owner": lease_owner,
                                }
                            ]
                            if lease_present
                            else []
                        ),
                        "state": {
                            "reason": fleet_rollout._hold_reason(
                                rollout_id, "host_f"
                            ),
                            "set_at": "2026-08-08T12:00:00+00:00",
                        },
                    }
                ),
                stderr="",
            )
        if "--release" not in args:
            assert legacy_present is True
            lease_present = True
            return subprocess.CompletedProcess(
                argv, 0, stdout="held\n", stderr=""
            )
        if "--release-legacy-only" in args:
            assert lease_present is True
            assert legacy_present is True
            legacy_present = False
        else:
            assert lease_present is True
            assert legacy_present is False
            persisted = fleet_rollout.load_run(rollout_id)
            assert persisted is not None
            assert persisted.holds["host_f"]["status"] == "active"
            assert persisted.holds["host_f"]["legacy_migrated"] is True
            assert (
                persisted.holds["host_f"]["legacy_migration_pending"] is False
            )
            lease_present = False
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout_id,
        runner=update_runner,
        control_runner=control_runner,
    )

    assert "--status" in controls[0]
    assert "--release" not in controls[1]
    assert "--release-legacy-only" in controls[2]
    assert controls[3][4:6] == ["--lease-owner", lease_owner]
    assert legacy_present is False
    assert lease_present is False
    assert run.holds["host_f"]["status"] == "released"
    assert run.holds["host_f"]["legacy_migrated"] is True
    assert run.holds["host_f"]["legacy_migration_pending"] is False


def test_release_response_loss_retries_and_skip_resume_reconciles_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A committed release with lost responses remains exactly recoverable."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-release-response-loss"
    lease_owner = fleet_rollout._hold_lease_owner(rollout_id, "host_f")
    actions = [
        fleet_rollout.RolloutAction(
            id=f"host_f:{program}",
            phase="scheduler-runtime",
            host="host_f",
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, "host_f"],
            decision="update",
            reason="target is newer",
            before={},
        )
        for program in ("vibeqc-dev", "vibe-view")
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    updates: list[str] = []
    controls: list[list[str]] = []
    lease_present = False
    lose_release_responses = True

    def update_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        updates.append(argv[-2])
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal lease_present
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            payload = {
                "active": lease_present,
                "is_full_drain": False,
                "scheduler_hosts": ["host_f"] if lease_present else [],
                "scheduler_leases": (
                    [
                        {
                            "scheduler_host": "host_f",
                            "owner": lease_owner,
                        }
                    ]
                    if lease_present
                    else []
                ),
                "state": None,
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        if "--release" in args:
            # The daemon committed the exact-owner release before the client
            # lost its response. Repeated releases are therefore idempotent.
            lease_present = False
            if lose_release_responses:
                return subprocess.CompletedProcess(
                    argv,
                    9,
                    stdout="",
                    stderr="response lost after commit",
                )
        else:
            lease_present = True
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    with pytest.raises(fleet_rollout.FleetRolloutError, match="response lost"):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=rollout_id,
            runner=update_runner,
            control_runner=control_runner,
        )

    release_controls = [args for args in controls if "--release" in args]
    assert len(release_controls) == 2
    assert all(lease_owner in args for args in release_controls)
    failed = fleet_rollout.load_run(rollout_id)
    assert failed is not None
    assert failed.holds["host_f"]["status"] == "cleanup-failed"
    assert lease_present is False

    lose_release_responses = False
    resumed = fleet_rollout.RolloutPlan(
        driver=plan.driver,
        report=plan.report,
        topology=plan.topology,
        actions=[
            fleet_rollout.RolloutAction(
                **{
                    **action.__dict__,
                    "decision": "skip",
                    "reason": "already at target with LAST OK=true",
                }
            )
            for action in actions
        ],
    )
    run = fleet_rollout.execute_plan(
        resumed,
        rollout_id=rollout_id,
        runner=update_runner,
        control_runner=control_runner,
    )

    release_controls = [args for args in controls if "--release" in args]
    assert len(release_controls) == 3
    assert release_controls[-1][4:6] == ["--lease-owner", lease_owner]
    assert updates == ["vibeqc-dev", "vibe-view"]
    assert run.holds["host_f"]["status"] == "released"
    assert run.complete is False


def test_failure_stops_and_names_scheduler_helper_log_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = fleet_rollout.RolloutAction(
        id="helper:host_f",
        phase="helper",
        host="host_f",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.17.0",
        target_tag=None,
        argv=["admin", "update", "host_f"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(argv, 7, stdout="", stderr="failed")

    # The log-target naming is what matters here, and it must survive the
    # failure no longer propagating: for a helper lane the transcript lives
    # under the HOST, not the program. It is now recorded per host instead of
    # raised.
    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="failure",
        runner=runner,
    )

    assert "vq admin logs host_f --host host_f" in run.failed_hosts["host_f"]


def test_one_host_failure_does_not_stop_the_others(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed host must not block convergence-checking for the rest.

    The host_d/host_a multi-user `/opt/vq` gap (runbook §3b) fails by design every
    release until a maintainer runs its sudo step, so aborting the whole run on
    the first failure meant `rollout-latest` was guaranteed to halt on every
    release with every host ordered behind host_d left unverified. The manual
    `--all-hosts` commands already continue past a single host's failure, so the
    automated path was the strict regression. Observed twice during the v0.15.75
    sweep, where the operator finished the remaining venv lanes by hand.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)

    def _action(host: str, program: str) -> fleet_rollout.RolloutAction:
        return fleet_rollout.RolloutAction(
            id=f"local-runtime:{host}:{program}",
            phase="local-runtime",
            host=host,
            program=program,
            pin_name="dev",
            target_sha=DEV,
            target_version="0.15.61.dev0",
            target_tag=None,
            argv=["admin", "update", program, host],
            decision="update",
            reason="target is newer",
            before={},
        )

    # host_d fails first in the ordering; host_b and host_e come after it.
    actions = [
        _action("host_d", "vibeqc-release"),
        _action("host_d", "vibeqc-dev"),
        _action("host_b", "vibeqc-release"),
        _action("host_e", "vibeqc-release"),
    ]
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=actions,
    )
    attempted: list[str] = []

    def runner(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        attempted.append(" ".join(argv[-2:]))
        # Only the host_d *update* fails; drain control calls are separate.
        if "host_d" in argv:
            return subprocess.CompletedProcess(argv, 1, stdout="", stderr="opt/vq")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    held = False

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal held
        del kwargs
        if "--status" in argv:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": held,
                        "is_full_drain": held,
                        "scheduler_hosts": [],
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    "degraded", "host_d"
                                ),
                                "set_at": "2026-08-10T20:00:00+00:00",
                            }
                            if held
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        held = "--release-full" not in argv
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="degraded",
        runner=runner,
        control_runner=control_runner,
    )

    # The hosts behind the failure were still attempted and succeeded.
    assert "vibeqc-release host_b" in attempted
    assert "vibeqc-release host_e" in attempted
    assert run.actions["local-runtime:host_b:vibeqc-release"]["status"] == "success"
    assert run.actions["local-runtime:host_e:vibeqc-release"]["status"] == "success"

    # The failure is confined to its own host, and its sibling lane is skipped.
    assert list(run.failed_hosts) == ["host_d"]
    assert "vibeqc-dev host_d" not in attempted
    assert run.actions["local-runtime:host_d:vibeqc-dev"]["status"] == "not-run"

    # A degraded sweep is still not a converged one.
    assert run.complete is False
    assert held is False


# --- host selection (--only / --skip) ---------------------------------------


def _selection(**kwargs: Any) -> fleet_rollout.HostSelection:
    return fleet_rollout.resolve_selection(_config(), **kwargs)


def test_only_narrows_execution_without_touching_the_reports_pins() -> None:
    """The whole point of `--only`: recovery stays on the pinned path.

    A host-scoped run must reach `vq admin update` with byte-identical argv to
    the unscoped run, because the alternative it replaces is an operator typing
    `--expected-sha <40-hex>` by hand.
    """
    plan = _plan()
    scoped = fleet_rollout.select_hosts(plan, _selection(only=["host_d"]))

    by_id = {action.id: action for action in scoped.actions}
    unscoped = {action.id: action for action in plan.actions}
    for action_id, action in by_id.items():
        assert action.argv == unscoped[action_id].argv
        assert action.target_sha == unscoped[action_id].target_sha

    assert all(
        action.decision == "update"
        for action in scoped.actions
        if action.host == "host_d"
    )
    assert all(
        action.decision == "defer"
        for action in scoped.actions
        if action.host == "host_f"
    )


def test_only_keeps_the_drivers_own_vq_lane() -> None:
    """A report-pinned rollout is driven by the pinned vq.

    Narrowing the driver's own lane out would verify the fleet with whatever vq
    the driver happens to be running, which is the thing the accepted report
    exists to stop.
    """
    scoped = fleet_rollout.select_hosts(_plan(), _selection(only=["host_d"]))

    driver = next(a for a in scoped.actions if a.phase == "driver")
    assert driver.host == "localhost"
    assert driver.decision == "update"


def test_skip_defers_the_named_host_but_spares_the_driver_lane() -> None:
    scoped = fleet_rollout.select_hosts(_plan(), _selection(skip=["localhost"]))

    driver = next(a for a in scoped.actions if a.phase == "driver")
    assert driver.decision == "update"
    assert all(
        action.decision == "defer"
        for action in scoped.actions
        if action.host == "localhost" and action.phase != "driver"
    )
    assert all(
        action.decision == "update"
        for action in scoped.actions
        if action.host == "host_f"
    )


def test_scope_exclusion_defers_rather_than_skips() -> None:
    """`skip` means "no work needed"; a scoped-out lane has not been looked at.

    Recording the narrowing as a skip would let `--only host_d` mark the whole
    fleet converged, which is the one thing a scoped run must never claim.
    """
    scoped = fleet_rollout.select_hosts(_plan(), _selection(only=["host_d"]))

    assert scoped.has_deferred
    assert not scoped.has_blocks


def test_selection_leaves_the_source_plan_untouched() -> None:
    plan = _plan()
    fleet_rollout.select_hosts(plan, _selection(only=["host_d"]))

    assert all(action.decision == "update" for action in plan.actions)


def test_only_and_skip_are_mutually_exclusive() -> None:
    with pytest.raises(fleet_rollout.FleetRolloutError, match="mutually exclusive"):
        _selection(only=["host_d"], skip=["host_f"])


def test_an_unknown_selection_name_fails_closed() -> None:
    """A typo must not quietly become "act on no hosts" and exit 0."""
    with pytest.raises(fleet_rollout.FleetRolloutError, match="unknown host"):
        _selection(only=["planetxx"])


def test_a_selection_naming_only_lane_less_hosts_fails_closed() -> None:
    """`retired` is configured but excluded, so it has no lane in any plan."""
    plan = _plan()
    with pytest.raises(fleet_rollout.FleetRolloutError, match="no lane in this plan"):
        fleet_rollout.assert_selection_in_plan(plan, _selection(only=["retired"]))


def test_vq_only_selection_uses_the_user_vq_lane_not_root_inventory() -> None:
    plan = _plan()
    selection = _selection(only=["host_0"])

    fleet_rollout.assert_selection_in_plan(plan, selection)
    restricted = fleet_rollout.restrict_plan(plan, selection)

    assert [action.id for action in restricted.actions] == [
        "local-runtime:host_0:vibeqc-queue"
    ]
    assert [lane.host for lane in restricted.provenance_lanes] == ["host_0"]


def test_exact_root_inventory_does_not_duplicate_the_vq_only_user_lane() -> None:
    admin, programs, doctor = _snapshots()
    doctor["host_0"]["checks"] = [_exact_root_daemon_check()]
    plan = _plan(
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )
    lane = next(item for item in plan.provenance_lanes if item.host == "host_0")
    assert lane.decision == "skip"
    assert lane.applicable is True

    selection = _selection(only=["host_0"])
    fleet_rollout.assert_selection_in_plan(plan, selection)
    assert [
        action.id for action in plan.actions if action.host == "host_0"
    ] == ["local-runtime:host_0:vibeqc-queue"]


def test_vq_only_missing_registration_blocks_instead_of_skipping() -> None:
    admin, programs, doctor = _snapshots()
    admin["host_0"]["envs"] = []
    programs["host_0"] = []

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.before["configured"] is False
    assert action.before["required"] is True
    assert action.decision == "block"
    assert "register [programs.vibeqc-queue]" in action.reason


def test_vq_only_probe_failure_defers_instead_of_looking_unconfigured() -> None:
    admin, programs, doctor = _snapshots()
    programs["host_0"] = {
        "error": "remote vq failed (exit 255): connection timed out"
    }

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.decision == "defer"
    assert "programs probe failed" in action.reason
    assert "connection timed out" in action.reason


def test_vq_only_torn_status_surfaces_defer() -> None:
    admin, programs, doctor = _snapshots()
    programs["host_0"] = []

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.decision == "defer"
    assert "status snapshots disagree" in action.reason


@pytest.mark.parametrize(
    ("surface", "replacement", "reason"),
    [
        ("admin", None, "admin status probe omitted"),
        ("admin", [], "malformed host record"),
        ("programs", None, "programs probe omitted"),
        ("programs", "bad", "malformed host record"),
    ],
)
def test_vq_only_missing_or_malformed_probe_defers(
    surface: str,
    replacement: object,
    reason: str,
) -> None:
    admin, programs, doctor = _snapshots()
    target = admin if surface == "admin" else programs
    if replacement is None:
        target.pop("host_0")
    else:
        target["host_0"] = replacement

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.decision == "defer"
    assert reason in action.reason


@pytest.mark.parametrize(
    ("replacement", "reason"),
    [
        (None, "doctor probe omitted"),
        ([], "malformed host record"),
        ({"error": "remote doctor timed out"}, "doctor probe failed"),
    ],
)
def test_vq_only_missing_malformed_or_failed_doctor_probe_defers(
    replacement: object,
    reason: str,
) -> None:
    admin, programs, doctor = _snapshots()
    if replacement is None:
        doctor.pop("host_0")
    else:
        doctor["host_0"] = replacement

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.decision == "defer"
    assert reason in action.reason


def test_vq_only_non_venv_registration_blocks() -> None:
    admin, programs, doctor = _snapshots()
    admin["host_0"]["envs"] = []
    programs["host_0"][0]["kind"] = "binary"

    action = next(
        item
        for item in _plan(snapshots=(admin, programs, doctor)).actions
        if item.host == "host_0"
    )

    assert action.decision == "block"
    assert 'must use kind="venv"' in action.reason


def test_vq_only_unhealthy_but_updateable_registration_repairs() -> None:
    admin, programs, doctor = _snapshots(local_sha=VQ)
    programs["host_0"][0].update(
        {
            "status": "MISSING",
            "reason": "vq import failed",
            "import_version": None,
        }
    )

    action = next(
        item
        for item in _plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ).actions
        if item.host == "host_0"
    )

    assert action.decision == "update"
    assert action.before["configured"] is True
    assert action.before["last_ok"] is False


def test_skipping_a_lane_less_host_fails_closed_too() -> None:
    """A --skip no-op is worse than a --only typo: the operator believes a host
    was excluded and it never was, so the run does the thing they were trying
    to prevent. A scheduler alias sharing its canonical host's SSH endpoint is
    the realistic case -- skipping the alias skips nothing."""
    plan = _plan()
    with pytest.raises(fleet_rollout.FleetRolloutError, match="no lane in this plan"):
        fleet_rollout.assert_selection_in_plan(plan, _selection(skip=["host_f-big"]))


def test_an_unscoped_selection_is_always_in_plan() -> None:
    fleet_rollout.assert_selection_in_plan(_plan(), fleet_rollout.HostSelection())


def test_an_out_of_scope_doctor_failure_does_not_fail_the_scoped_verdict() -> None:
    """The failure mode this guards is a scoped recovery that cannot be judged.

    `doctor_failures` walks the whole topology and forces `status="blocked"`, so
    without a scoped verdict a perfectly successful `--only host_d` would exit
    2 because host_f is unwell -- indistinguishable from host_d itself failing.
    """
    admin, programs, doctor = _snapshots()
    doctor["host_f"] = {
        "ok": False,
        "checks": [{"name": "scheduler_liveness", "ok": False}],
    }
    plan = _plan(snapshots=(admin, programs, doctor))
    selection = _selection(only=["host_d"])

    payload = fleet_rollout.result_payload(
        initial=None,
        verification=plan,
        doctor=doctor,
        run=None,
        selection=selection,
    )

    # Fleet-wide truth is preserved -- nothing is hidden ...
    assert payload["status"] == "blocked"
    assert "host_f" in payload["doctor_failures"]
    # ... but the scoped verdict answers the question that was asked.
    assert "host_f" not in payload["selection"]["degraded_hosts"]
    assert "host_d" in payload["selection"]["degraded_hosts"]
    # The driver's own vq lane is never narrowed out of the verdict: a scoped
    # run that exits 0 while its driver is behind the pin would be reporting a
    # rollout staged by a vq the accepted report does not pin.
    assert "localhost" in payload["selection"]["degraded_hosts"]


def test_driver_root_provenance_is_not_scope_exempt() -> None:
    admin, programs, doctor = _converged_snapshots()
    doctor["localhost"]["checks"][0]["system_multi_user"] = {
        "enabled": True,
        "error": None,
        "source": "/etc/vq/config.toml",
        "status": "enabled",
    }
    doctor["localhost"]["checks"][0]["multi_user"] = True
    plan = _plan(snapshots=(admin, programs, doctor))

    restricted = fleet_rollout.restrict_plan(
        plan,
        _selection(only=["host_d"]),
        keep_phases=fleet_rollout.SCOPE_EXEMPT_PHASES,
    )

    assert any(action.phase == "driver" for action in restricted.actions)
    assert all(lane.host == "host_d" for lane in restricted.provenance_lanes)


def test_restrict_plan_drops_out_of_scope_topology_errors() -> None:
    plan = _plan(cfg=_config(include_unresolved=True))
    assert plan.has_blocks

    restricted = fleet_rollout.restrict_plan(
        plan,
        fleet_rollout.HostSelection(only=("host_d",)),
    )

    assert restricted.topology_errors == []
    assert not restricted.has_blocks
    assert set(restricted.topology) == {"host_d"}


# --- one-verdict convergence check (--verify-only) --------------------------


def _converged_snapshots() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Live state in which every lane already stands at the accepted report."""
    admin, programs, doctor = _snapshots()
    for host in ("localhost", "host_d"):
        admin[host]["envs"] = [
            _env("vibeqc-queue", VQ),
            _env("vibeqc-release", RELEASE, tag="v0.15.60"),
            _env("vibeqc-dev", DEV),
            _env("vibe-view", VIEW),
        ]
        programs[host] = [
            _program("vibeqc-queue", VQ, "0.17.0"),
            _program("vibeqc-release", RELEASE, "0.15.60"),
            _program("vibeqc-dev", DEV, "0.15.61.dev0"),
            _program("vibe-view", VIEW, "2.5.0"),
        ]
    admin["host_0"]["envs"] = [_env("vibeqc-queue", VQ)]
    programs["host_0"] = [_program("vibeqc-queue", VQ, "0.17.0")]
    doctor["host_0"]["checks"][0].update(
        {
            "version": "0.17.0",
            "source_sha": VQ,
            "source_tree_sha256": TARGET_TREE,
            "multi_user": False,
        }
    )
    admin["host_f"]["deployments"] = {
        "vibeqc-release": _deployment(RELEASE, tag="v0.15.60"),
        "vibeqc-dev": _deployment(DEV),
        "vibe-view": _deployment(VIEW),
    }
    doctor["host_f"]["checks"][0]["source_sha"] = VQ
    doctor["host_f"]["checks"][0]["message"] = f"SOURCE-SHA {VQ} match driver"
    admin["host_f"]["helper"] = {
        "last": {"actual_sha": VQ, "last_success": True},
    }
    return admin, programs, doctor


def test_verify_payload_reports_converged_when_every_lane_is_at_the_pin() -> None:
    snapshots = _converged_snapshots()
    plan = _plan(
        snapshots=snapshots,
        target_vq_tree_sha256=TARGET_TREE,
    )
    assert not plan.updates, [a.id for a in plan.updates]

    payload = fleet_rollout.verify_payload(
        plan=plan,
        doctor=snapshots[2],
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    # /3 adds read-only provenance lanes and folds them into the verdict.
    assert payload["schema"] == "vq.fleet.rollout_verify/3"
    assert payload["verdict"] == "converged"
    assert payload["degraded_hosts"] == {}
    assert (
        payload["coverage"]["scope"]
        == "managed-lanes+vq-user-lanes+read-only-provenance"
    )
    assert payload["coverage"]["whole_fleet_convergence_asserted"] is False
    assert payload["coverage"]["managed_lanes"] == {
        "total": len(plan.actions) - 1,
        "converged": len(plan.actions) - 1,
    }
    assert payload["coverage"]["vq_user_lanes"] == {
        "total": 1,
        "configured": 1,
        "converged": 1,
        "deferred": 0,
        "blocked": 0,
    }


def test_verify_payload_names_every_degraded_host_with_its_reason() -> None:
    snapshots = _snapshots()
    plan = _plan(snapshots=snapshots)

    payload = fleet_rollout.verify_payload(
        plan=plan,
        doctor=snapshots[2],
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    assert payload["verdict"] == "degraded"
    assert set(payload["degraded_hosts"]) == {
        "localhost",
        "host_f",
        "host_d",
        "host_0",
    }
    assert any(
        "local-runtime:host_d:vibeqc-queue" in reason
        for reason in payload["degraded_hosts"]["host_d"]
    )


def test_verify_payload_folds_doctor_failures_into_the_verdict() -> None:
    """One verdict, not "read the plan, then also read doctor"."""
    admin, programs, doctor = _converged_snapshots()
    doctor["host_d"] = {
        "ok": False,
        "checks": [{"name": "daemon_rpc", "ok": False}],
    }

    payload = fleet_rollout.verify_payload(
        plan=_plan(
            snapshots=(admin, programs, doctor),
            target_vq_tree_sha256=TARGET_TREE,
        ),
        doctor=doctor,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    assert payload["verdict"] == "degraded"
    assert any(
        reason == "doctor: daemon_rpc"
        for reason in payload["degraded_hosts"]["host_d"]
    )
    assert any(
        reason.startswith("root-daemon:host_d:vibeqc-queue: defer")
        for reason in payload["degraded_hosts"]["host_d"]
    )


def test_verify_payload_reports_a_blocked_topology_rather_than_raising() -> None:
    """A verifier that cannot report on a blocked fleet is useless."""
    admin, programs, doctor = _converged_snapshots()
    doctor["mystery"] = {"ok": True, "checks": []}
    plan = _plan(
        cfg=_config(include_unresolved=True),
        snapshots=(admin, programs, doctor),
        target_vq_tree_sha256=TARGET_TREE,
    )

    payload = fleet_rollout.verify_payload(
        plan=plan,
        doctor=doctor,
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    assert payload["verdict"] == "degraded"
    assert payload["degraded_hosts"]["mystery"] == [
        "topology: auto: no managed lane found; declare vq-only/excluded"
    ]


def test_a_read_only_verdict_reports_no_lane_as_changed() -> None:
    """`initial=None` means nothing ran, so nothing changed.

    Passing the verification plan as its own `initial` reports every pending
    lane as `changed`, which is exactly backwards for a read-only verdict.
    """
    snapshots = _snapshots()
    plan = _plan(snapshots=snapshots)

    payload = fleet_rollout.result_payload(
        initial=None,
        verification=plan,
        doctor=snapshots[2],
        run=None,
    )

    assert payload["lanes"]
    assert not any(lane["changed"] for lane in payload["lanes"])


def test_render_verify_text_names_the_degraded_hosts_in_the_headline() -> None:
    snapshots = _snapshots()
    payload = fleet_rollout.verify_payload(
        plan=_plan(
            snapshots=snapshots,
            target_vq_tree_sha256=TARGET_TREE,
        ),
        doctor=snapshots[2],
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    text = fleet_rollout.render_verify_text(payload)

    assert text.splitlines()[0].startswith(
        "rollout-latest verify: modeled lanes degraded("
    )
    assert "host_d" in text.splitlines()[0]


def test_render_verify_text_does_not_claim_whole_fleet_convergence() -> None:
    snapshots = _converged_snapshots()
    payload = fleet_rollout.verify_payload(
        plan=_plan(
            snapshots=snapshots,
            target_vq_tree_sha256=TARGET_TREE,
        ),
        doctor=snapshots[2],
        rollout_id="v0.15.60-aaaaaaaaaaaa",
    )

    text = fleet_rollout.render_verify_text(payload)

    assert text.splitlines()[0] == "rollout-latest verify: modeled lanes converged"
    assert (
        "coverage: managed lanes, vq user lanes, plus read-only provenance; "
        "whole-fleet convergence not asserted" in text
    )
    assert "coverage counts: managed=" in text
    assert "vq-user=1/1" in text
    assert "EXCLUDED root-vq-daemon" not in text
    assert payload["provenance_lanes"]
    assert "EXCLUDED vibe-basisopt" in text
    assert "EXCLUDED host_0" in text


def test_render_result_text_neutralizes_external_hold_terminal_controls() -> None:
    snapshots = _converged_snapshots()
    plan = _plan(
        snapshots=snapshots,
        target_vq_tree_sha256=TARGET_TREE,
    )
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-control-text",
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        holds={
            "host_d": {
                "host": "host_d",
                "kind": "full",
                "owned": False,
                "status": "preserved",
                "external_holds": [
                    {
                        "host": "host_d",
                        "kind": "full",
                        "lease_id": None,
                        "owner": "operator\x9b0m",
                        "reason": "manual\nrepair\x1b[31m",
                    }
                ],
            }
        },
    )
    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=snapshots[2],
        run=run,
    )

    text = fleet_rollout.render_result_text(payload)

    assert "\x1b" not in text
    assert "\x9b" not in text
    assert "PRESERVED EXTERNAL HOLD host_d full" in text
    assert "owner=operator 0m reason=manual repair [31m" in text


def test_result_reports_retained_owned_holds_as_journal_evidence() -> None:
    snapshots = _converged_snapshots()
    plan = _plan(
        snapshots=snapshots,
        target_vq_tree_sha256=TARGET_TREE,
    )
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-retained-holds",
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        failed_hosts={"host_d": "build\nfailed\x1b[31m"},
        holds={
            "host_d": {
                "host": "host_d",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": 'vq rollout\nhost_d\x1b[0m',
            },
            "host_f": {
                "host": "host_f",
                "kind": "scheduler-target",
                "owned": True,
                "status": "cleanup-failed",
                "reason": "vq rollout host_f",
                "external_holds": [
                    {
                        "host": "host_f",
                        "kind": "scheduler-target",
                        "lease_id": "operator-host_f",
                        "owner": "operator",
                        "reason": "manual maintenance",
                    }
                ],
            },
            "host_a": {
                "host": "host_a",
                "kind": "full",
                "owned": True,
                "status": "released",
                "reason": "vq rollout host_a",
            },
            "malformed": {
                "host": "malformed",
                "kind": "full",
                "owned": True,
                "status": [],
                "reason": "invalid legacy journal value",
            },
            "borrowed": {
                "host": "borrowed",
                "kind": "full",
                "owned": False,
                "status": "preserved",
                "external_reason": "operator repair",
            },
        },
    )
    payload = fleet_rollout.result_payload(
        initial=plan,
        verification=plan,
        doctor=snapshots[2],
        run=run,
    )

    assert payload["retained_rollout_holds"] == [
        {
            "host": "host_d",
            "kind": "full",
            "reason": 'vq rollout\nhost_d\x1b[0m',
            "status": "active",
        },
        {
            "host": "host_f",
            "kind": "scheduler-target",
            "reason": "vq rollout host_f",
            "status": "cleanup-failed",
        },
    ]
    assert payload["preserved_external_holds"] == [
        {
            "host": "borrowed",
            "kind": "full",
            "lease_id": None,
            "owner": None,
            "reason": "operator repair",
            "status": "preserved",
        },
        {
            "host": "host_f",
            "kind": "scheduler-target",
            "lease_id": "operator-host_f",
            "owner": "operator",
            "reason": "manual maintenance",
            "status": "preserved",
        },
    ]

    text = fleet_rollout.render_result_text(payload)

    assert "\x1b" not in text
    assert "FAILED HOST host_d: build failed [31m" in text
    assert (
        "RETAINED ROLLOUT HOLD host_d full journal_status=active "
        "reason=vq rollout host_d [0m "
        "(journal evidence; current liveness not asserted)"
        in text
    )
    assert (
        "RETAINED ROLLOUT HOLD host_f scheduler-target "
        "journal_status=cleanup-failed reason=vq rollout host_f "
        "(journal evidence; current liveness not asserted)"
        in text
    )
    assert (
        "PRESERVED EXTERNAL HOLD host_f scheduler-target owner=operator "
        "reason=manual maintenance"
        in text
    )


def test_a_block_on_a_skipped_host_does_not_abort_the_scoped_run() -> None:
    """Routing around a blocked host is the main reason --skip exists.

    A blocked host that still aborts everything makes the flag useless exactly
    when it is needed.
    """
    admin, programs, doctor = _snapshots()
    # host_d's release lane goes dirty, which the planner blocks on.
    admin["host_d"]["envs"][1]["is_dirty"] = True
    for entry in programs["host_d"]:
        if entry["name"] == "vibeqc-release":
            entry["current_git_dirty"] = True
    plan = _plan(snapshots=(admin, programs, doctor))
    assert plan.has_blocks

    in_scope = fleet_rollout.restrict_plan(
        fleet_rollout.select_hosts(plan, _selection(skip=["host_d"])),
        _selection(skip=["host_d"]),
        keep_phases=fleet_rollout.SCOPE_EXEMPT_PHASES,
    )

    assert not in_scope.has_blocks
    assert not any(action.host == "host_d" for action in in_scope.actions)


def test_the_scoped_view_retains_the_driver_lane_for_the_execution_verdict() -> None:
    plan = _plan()
    selection = _selection(only=["host_d"])

    scoped = fleet_rollout.restrict_plan(
        plan,
        selection,
        keep_phases=fleet_rollout.SCOPE_EXEMPT_PHASES,
    )

    assert any(action.phase == "driver" for action in scoped.actions)
    assert {action.host for action in scoped.actions} == {"localhost", "host_d"}


def test_a_pure_verdict_drops_the_driver_lane_when_it_was_not_asked_about() -> None:
    """`--verify-only --only host_d` asks about host_d, not about the driver."""
    plan = _plan()

    scoped = fleet_rollout.restrict_plan(plan, _selection(only=["host_d"]))

    assert {action.host for action in scoped.actions} == {"host_d"}


def test_an_interrupted_scoped_run_never_persists_complete(tmp_path, monkeypatch) -> None:
    """`execute_plan` sets `complete` on every exit from its loop; the CLI's
    correction is only reached on the success path."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = fleet_rollout.select_hosts(_plan(), _selection(only=["host_d"]))
    for action in plan.actions:
        action.decision = "skip"

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id="v0.15.60-abcdef123456",
        runner=lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
        scoped=True,
    )

    assert run.complete is False


def test_a_scoped_run_preserves_an_out_of_scope_hosts_recorded_success(
    tmp_path, monkeypatch
) -> None:
    """The journal is per-rollout, so a scoped run shares it with the full run.

    Overwriting another host's recorded success with `not-run` erases the only
    evidence of a deploy that did happen under this rollout id.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-abcdef123456"
    plan = _plan()
    prior = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
        actions={
            "local-runtime:localhost:vibeqc-dev": {
                "decision": "update",
                "reason": "target is newer",
                "status": "success",
                "rc": 0,
                "duration_seconds": 91.2,
            }
        },
    )
    fleet_rollout.save_run(prior)

    scoped = fleet_rollout.select_hosts(plan, _selection(only=["host_d"]))
    for action in scoped.actions:
        if action.decision == "update":
            action.decision = "skip"

    run = fleet_rollout.execute_plan(
        scoped,
        rollout_id=rollout,
        runner=lambda *a, **k: subprocess.CompletedProcess([], 0, "", ""),
        scoped=True,
    )

    kept = run.actions["local-runtime:localhost:vibeqc-dev"]
    assert kept["status"] == "success"
    assert kept["duration_seconds"] == 91.2
    assert "out_of_scope_reason" in kept


def test_rollout_lock_refuses_a_symlink_without_touching_its_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The permanent fleet fence is a private no-follow state object."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_dir = tmp_path / "rollouts"
    rollout_dir.mkdir()
    target = tmp_path / "not-a-lock"
    target.write_text("operator data\n")
    (rollout_dir / ".fleet-rollout.lock").symlink_to(target)

    with (
        pytest.raises(fleet_rollout.FleetRolloutError, match="unsafe.*lock"),
        fleet_rollout.rollout_execution_lock("v0.15.60-lock-test"),
    ):
        pytest.fail("unsafe lock must never be acquired")

    assert target.read_text() == "operator data\n"


def test_rollout_lock_tightens_the_legacy_owner_mode_in_place(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_dir = tmp_path / "rollouts"
    rollout_dir.mkdir()
    lock = rollout_dir / ".fleet-rollout.lock"
    lock.write_text("")
    lock.chmod(0o644)

    with fleet_rollout.rollout_execution_lock("compatibility"):
        assert lock.stat().st_mode & 0o777 == 0o600

    assert lock.stat().st_mode & 0o777 == 0o600


def test_local_rollout_hold_captures_and_releases_the_exact_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A local full drain is releasable only by its reason + set_at identity."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-exact-local"
    action = fleet_rollout.RolloutAction(
        id="local-runtime:host_d:vibeqc-dev",
        phase="local-runtime",
        host="host_d",
        program="vibeqc-dev",
        pin_name="dev",
        target_sha=DEV,
        target_version="0.15.61.dev0",
        target_tag=None,
        argv=["admin", "update", "vibeqc-dev", "host_d"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
    )
    reason = fleet_rollout._hold_reason(rollout, "host_d")
    controls: list[list[str]] = []
    acquired = False

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal acquired
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            payload = {
                "active": acquired,
                "is_full_drain": acquired,
                "scheduler_hosts": [],
                "state": (
                    {"reason": reason, "set_at": "2026-08-10T20:00:00+00:00"}
                    if acquired
                    else None
                ),
            }
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        if "--release-full" in args:
            assert "--release" not in args
            assert args[args.index("--expected-full-reason") + 1] == reason
            assert (
                args[args.index("--expected-full-set-at") + 1]
                == "2026-08-10T20:00:00+00:00"
            )
            acquired = False
        else:
            acquired = True
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    hold = fleet_rollout.acquire_rollout_hold(
        plan,
        run,
        host="host_d",
        actions=[action],
        runner=control_runner,
    )
    assert hold["reason"] == reason
    assert hold["set_at"] == "2026-08-10T20:00:00+00:00"
    assert len([args for args in controls if "--status" in args]) == 2

    fleet_rollout.release_rollout_hold(
        plan,
        run,
        hold=hold,
        runner=control_runner,
    )

    assert acquired is False
    assert run.holds["host_d"]["status"] == "released"


def _scheduler_sidecar_only_status() -> dict[str, Any]:
    lease = {
        "lease_id": "rollout-host_c",
        "scheduler_host": "host_c",
        "owner": "fleet-rollout:v0.15.137-example:host_c",
        "set_at": "2026-08-19T20:00:00+00:00",
        "reason": "vq rollout-latest scheduler hold",
        "owner_pid": None,
        "owner_pid_start_time": 0,
    }
    state = {
        "enabled": True,
        "max_jobs": None,
        "max_cpus": None,
        "set_at": lease["set_at"],
        "reason": lease["reason"],
        "owner_pid": None,
        "owner_pid_start_time": 0,
        "scheduler_hosts": ["host_c"],
        "full_dispatch": False,
        "reject_submits": False,
        "update_mode": None,
        "duration_seconds": None,
    }
    return {
        **state,
        "active": True,
        "mode": "scheduler-target",
        "is_full_drain": False,
        "is_scheduler_target_drain": True,
        "scheduler_hosts": ["host_c"],
        "scheduler_leases": [lease],
        "orphaned_scheduler_leases": [],
        "scheduler_leases_error": None,
        "legacy_scheduler_hosts": [],
        "submit_policy": "accept_pending",
        "state": state,
    }


def _localhost_rollout_hold_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    rollout: str,
) -> tuple[
    fleet_rollout.RolloutPlan,
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutAction,
]:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = fleet_rollout.RolloutAction(
        id="local-runtime:localhost:vibeqc-queue",
        phase="local-runtime",
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.25.2",
        target_tag=None,
        argv=["admin", "update", "vibeqc-queue", "localhost"],
        decision="update",
        reason="target is newer",
        before={},
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=str(plan.report["digest_sha256"]),
        report_source_path=str(plan.report["source_path"]),
    )
    return plan, run, action


def test_local_full_rollout_hold_preserves_scheduler_sidecar_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A scheduler lease is an independent lane, not an operator cap."""
    plan, run, action = _localhost_rollout_hold_case(
        tmp_path,
        monkeypatch,
        rollout="v0.25.2-local-sidecar",
    )
    reason = fleet_rollout._hold_reason(run.rollout_id, "localhost")
    set_at = "2026-08-21T12:00:00+00:00"
    original_lease = _scheduler_sidecar_only_status()["scheduler_leases"][0]
    full_held = False
    mutations: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal full_held
        del kwargs
        args = argv[3:]
        if "--status" in args:
            payload = _scheduler_sidecar_only_status()
            if full_held:
                payload.update(
                    mode="full+scheduler-target",
                    is_full_drain=True,
                )
                payload["state"].update(
                    set_at=set_at,
                    reason=reason,
                    full_dispatch=True,
                    update_mode="accept",
                    duration_seconds=25_200,
                )
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        mutations.append(args)
        if "--release-full" in args:
            assert "--release" not in args
            assert "--scheduler-host" not in args
            assert args[args.index("--expected-full-reason") + 1] == reason
            assert args[args.index("--expected-full-set-at") + 1] == set_at
            full_held = False
        else:
            assert "--update-mode" in args
            assert "--scheduler-host" not in args
            full_held = True
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    hold = fleet_rollout.acquire_rollout_hold(
        plan,
        run,
        host="localhost",
        actions=[action],
        runner=control_runner,
    )

    assert full_held is True
    assert hold["owned"] is True
    assert _scheduler_sidecar_only_status()["scheduler_leases"][0] == original_lease

    fleet_rollout.release_rollout_hold(
        plan,
        run,
        hold=hold,
        runner=control_runner,
    )

    assert full_held is False
    assert _scheduler_sidecar_only_status()["scheduler_leases"][0] == original_lease
    assert ["--release-full" in args for args in mutations] == [False, True]
    assert run.holds["localhost"]["status"] == "released"


@pytest.mark.parametrize(("max_jobs", "max_cpus"), [(1, None), (None, 4)])
def test_local_rollout_hold_refuses_operator_cap_beside_scheduler_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    max_jobs: int | None,
    max_cpus: int | None,
) -> None:
    plan, run, action = _localhost_rollout_hold_case(
        tmp_path,
        monkeypatch,
        rollout=f"v0.25.2-local-cap-{max_jobs}-{max_cpus}",
    )
    controls: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        payload = _scheduler_sidecar_only_status()
        payload["mode"] = "partial+scheduler-target"
        payload["max_jobs"] = max_jobs
        payload["max_cpus"] = max_cpus
        payload["state"]["max_jobs"] = max_jobs
        payload["state"]["max_cpus"] = max_cpus
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(payload), stderr=""
        )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="partial drain"):
        fleet_rollout.acquire_rollout_hold(
            plan,
            run,
            host="localhost",
            actions=[action],
            runner=control_runner,
        )

    assert controls == [["drain", "--status", "--json", "localhost"]]
    assert run.holds == {}


def test_reconciliation_recovers_pre_exact_local_hold_before_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-set_at local journal is recovered only from its live exact pair."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-pre-exact-local"
    digest = "a" * 64
    reason = fleet_rollout._hold_reason(rollout, "host_d")
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=digest,
            report_source_path="report.json",
            holds={
                "host_d": {
                    "host": "host_d",
                    "kind": "full",
                    "owned": True,
                    "status": "active",
                    "reason": reason,
                }
            },
        )
    )
    controls: list[list[str]] = []

    def control_runner(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": True,
                        "mode": "full",
                        "is_full_drain": True,
                        "is_scheduler_target_drain": False,
                        "scheduler_hosts": [],
                        "scheduler_leases": [],
                        "orphaned_scheduler_leases": [],
                        "scheduler_leases_error": None,
                        "state": {
                            "reason": reason,
                            "set_at": "2026-08-10T20:00:00+00:00",
                        },
                        "submit_policy": "deny",
                    }
                ),
                stderr="",
            )
        assert "--release-full" in args
        assert "--release" not in args
        assert args[args.index("--expected-full-reason") + 1] == reason
        assert (
            args[args.index("--expected-full-set-at") + 1]
            == "2026-08-10T20:00:00+00:00"
        )
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
        control_runner=control_runner,
    )

    persisted = fleet_rollout.load_run(rollout)
    assert persisted is not None
    assert persisted.holds["host_d"]["set_at"] == (
        "2026-08-10T20:00:00+00:00"
    )
    assert persisted.holds["host_d"]["status"] == "released"
    assert ["--status" in args for args in controls] == [True, False]


def _safe_durable_action(*, phase: fleet_rollout.Phase = "local-runtime"):
    return fleet_rollout.RolloutAction(
        id=f"{phase}:localhost:vibeqc-queue",
        phase=phase,
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.17.0",
        target_tag=None,
        # This exercises the real detached supervisor without touching a
        # checkout, daemon, queue, or fleet host.
        argv=["--version"],
        decision="update",
        reason="test durable supervision",
        before={},
    )


def _journal_durable_action(
    *,
    action: fleet_rollout.RolloutAction,
    rollout: str,
    digest: str,
    lifecycle_resources: tuple[tuple[str, str], ...] = (),
) -> fleet_operation.OperationHandle:
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        action,
        attempt=1,
        lifecycle_resources=lifecycle_resources,
        require_rollout_lock=bool(lifecycle_resources),
    )
    handle = fleet_operation.prepare_operation(identity)
    prepared = fleet_operation.observe_operation(handle.operation_id)
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=digest,
            report_source_path="report.json",
            actions={
                action.id: {
                    "decision": "update",
                    "reason": action.reason,
                    "status": "running",
                    "argv": action.argv,
                    "target_sha": action.target_sha,
                    "target_version": action.target_version,
                    "target_tag": action.target_tag,
                    "operation_attempts": [
                        fleet_rollout._attempt_ref(
                            prepared,
                            identity,
                            request_sha256=handle.request_sha256,
                        )
                    ],
                }
            },
        )
    )
    return handle


@contextmanager
def _continuous_controller_fence(
    tmp_path: Path,
    rollout: str,
    *,
    enabled: bool = True,
) -> Iterator[tuple[str | None, tuple[tuple[str, str], ...]]]:
    """Model the CLI's continuous lifecycle and global rollout fences."""
    if not enabled:
        yield None, ()
        return
    resources = _continuous_controller_resources(tmp_path, rollout)
    resource_path = Path(resources[0][1])
    resource_path.mkdir()
    lock_path = (tmp_path / f"{rollout}-lifecycle.lock").resolve()
    lock_fd = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
        0o600,
    )
    os.fchmod(lock_fd, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    handoff = json.dumps(
        {
            "schema": "vq.toolset.lifecycle_handoff/1",
            "locks": [
                {
                    "scope": resources[0][0],
                    "resource": resources[0][1],
                    "fd": lock_fd,
                    "path": str(lock_path),
                }
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        with fleet_rollout.rollout_execution_lock(rollout):
            yield handoff, resources
    finally:
        # Do not issue LOCK_UN: a supervisor or action may still own a
        # borrowed descriptor for this open-file-description lock.
        os.close(lock_fd)


def _continuous_controller_resources(
    tmp_path: Path,
    rollout: str,
) -> tuple[tuple[str, str], ...]:
    return (("checkout", str((tmp_path / f"{rollout}-checkout").resolve())),)


def test_driver_reentry_requires_exact_receipt_and_mutating_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.24.0-exact-driver-reentry"
    digest = "a" * 64
    driver = _safe_durable_action(phase="driver")
    handle = _journal_durable_action(
        action=driver,
        rollout=rollout,
        digest=digest,
    )
    supervisor = fleet_operation.launch_supervisor(handle.operation_id)
    del supervisor
    fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=digest,
    )
    deadline = time.monotonic() + 10
    while fleet_operation.observe_operation(handle.operation_id).state != "completed":
        assert time.monotonic() < deadline
        time.sleep(0.01)

    harvested = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
    )
    assert harvested.driver_reentry_required is True
    capability = fleet_rollout.RolloutReentryCapability(
        rollout_id=str(harvested.driver_reentry_rollout_id),
        operation_id=str(harvested.driver_reentry_operation_id),
        request_sha256=str(harvested.driver_reentry_request_sha256),
        report_digest_sha256=str(
            harvested.driver_reentry_report_digest_sha256
        ),
    )
    journal = fleet_rollout.rollout_state_path(rollout)
    before_read_only = (journal.read_bytes(), journal.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="dry-run/verify-only will not acknowledge it",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=True,
            acknowledge_driver_reentry=rollout,
            authenticated_driver_reentry=capability,
        )
    assert (journal.read_bytes(), journal.stat().st_mtime_ns) == before_read_only

    wrong = fleet_rollout.RolloutReentryCapability(
        rollout_id=capability.rollout_id,
        operation_id=capability.operation_id,
        request_sha256="f" * 64,
        report_digest_sha256=capability.report_digest_sha256,
    )
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="does not match the exact terminal receipt",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=False,
            acknowledge_driver_reentry=rollout,
            authenticated_driver_reentry=wrong,
        )

    acknowledged = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
        acknowledge_driver_reentry=rollout,
        authenticated_driver_reentry=capability,
    )
    assert acknowledged.driver_reentry_required is False
    persisted = fleet_rollout.load_run(rollout)
    assert persisted is not None
    assert (
        persisted.actions[driver.id].get("driver_reentry_required") is None
    )


def _publish_authorization_without_activation(
    handle: fleet_operation.OperationHandle,
) -> None:
    output = handle.directory / "output.log"
    output.write_bytes(b"")
    output.chmod(0o600)
    nonce = "d" * 64
    fleet_operation._write_operation_receipt(
        handle,
        "ready.json",
        fleet_operation._ready_payload(handle, nonce=nonce),
    )
    fleet_operation._write_operation_receipt(
        handle,
        "authorization.json",
        fleet_operation._authorization_payload(handle, nonce=nonce),
    )
    assert fleet_operation.observe_operation(handle.operation_id).state == (
        "authorized-unactivated"
    )


class _FakeRelaunchSupervisor:
    """Minimal Popen stand-in which makes exit and reap behavior observable."""

    def __init__(
        self,
        *,
        returncode: int | None,
        lease_fd: int | None = None,
    ) -> None:
        self.returncode = returncode
        self.lease_fd = lease_fd
        self.poll_calls = 0
        self.wait_calls = 0
        self.terminate_calls = 0
        self.kill_calls = 0

    def close_lease(self) -> None:
        if self.lease_fd is not None:
            os.close(self.lease_fd)
            self.lease_fd = None

    def poll(self) -> int | None:
        self.poll_calls += 1
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_calls += 1
        if self.returncode is None:
            raise subprocess.TimeoutExpired("fake-supervisor", 0)
        return self.returncode

    def terminate(self) -> None:
        self.terminate_calls += 1
        self.close_lease()
        self.returncode = -signal.SIGTERM

    def kill(self) -> None:
        self.kill_calls += 1
        self.close_lease()
        self.returncode = -signal.SIGKILL


@pytest.mark.parametrize("entrypoint", ["reconcile", "direct"])
@pytest.mark.parametrize("failure", ["exit", "hang"])
def test_authorized_unactivated_relaunch_failure_is_bounded_and_reaped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
    failure: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    monkeypatch.setattr(
        fleet_rollout,
        "DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS",
        0.0,
    )
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = f"v0.15.60-{entrypoint}-{failure}-before-recorder"
    with _continuous_controller_fence(
        tmp_path,
        rollout,
        enabled=entrypoint == "reconcile",
    ) as (lifecycle_handoff, lifecycle_resources):
        handle = _journal_durable_action(
            action=action,
            rollout=rollout,
            digest=digest,
            lifecycle_resources=lifecycle_resources,
        )
        _publish_authorization_without_activation(handle)
        supervisor = _FakeRelaunchSupervisor(
            returncode=76 if failure == "exit" else None,
        )
        launched: list[str] = []

        def launch(operation: str, **kwargs: Any) -> Any:
            if entrypoint == "reconcile":
                assert kwargs["lifecycle_handoff"] is not None
            launched.append(operation)
            return supervisor

        monkeypatch.setattr(
            fleet_rollout.fleet_operation,
            "launch_supervisor",
            launch,
        )
        plan = fleet_rollout.RolloutPlan(
            driver="localhost",
            report={"digest_sha256": digest, "source_path": "report.json"},
            topology={},
            actions=[action],
        )

        match = (
            "exited with 76"
            if failure == "exit"
            else "did not publish durable activation"
        )
        with pytest.raises(fleet_rollout.FleetRolloutError, match=match):
            if entrypoint == "reconcile":
                fleet_rollout.reconcile_durable_operations(
                    current_report_digest_sha256=digest,
                    inspect_only=False,
                    lifecycle_handoff=lifecycle_handoff,
                )
            else:
                fleet_rollout.execute_one(
                    plan,
                    action,
                    rollout_id=rollout,
                    report_digest_resolver=lambda: digest,
                )

        assert launched == [handle.operation_id]
        assert supervisor.poll_calls >= 1
        if failure == "hang":
            assert supervisor.terminate_calls == 1
            assert supervisor.wait_calls == 1
        observed = fleet_operation.observe_operation(handle.operation_id)
        assert observed.state == "authorized-unactivated"
        assert observed.activation is None
        persisted = fleet_rollout.load_run(rollout)
        assert persisted is not None
        attempts = persisted.actions[action.id]["operation_attempts"]
        assert len(attempts) == 1
        assert attempts[0]["operation_id"] == handle.operation_id


@pytest.mark.parametrize("entrypoint", ["reconcile", "direct"])
@pytest.mark.parametrize("launcher", ["owned", "adopted"])
def test_busy_authorized_without_activation_keeps_a_bounded_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
    launcher: str,
) -> None:
    """Lease acquisition alone cannot turn a pre-activation hang unbounded."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    monkeypatch.setattr(
        fleet_rollout,
        "DURABLE_SUPERVISOR_ACQUIRE_TIMEOUT_SECONDS",
        0.0,
    )
    monkeypatch.setattr(
        fleet_rollout.time,
        "sleep",
        lambda _seconds: pytest.fail("pre-activation timeout must not sleep"),
    )
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = f"v0.15.60-{entrypoint}-{launcher}-busy-preactivation"
    lifecycle_resources = (
        _continuous_controller_resources(tmp_path, rollout)
        if entrypoint == "reconcile"
        else ()
    )
    handle = _journal_durable_action(
        action=action,
        rollout=rollout,
        digest=digest,
        lifecycle_resources=lifecycle_resources,
    )
    _publish_authorization_without_activation(handle)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    supervisor: _FakeRelaunchSupervisor | None = None
    adopted_lease_fd: int | None = None
    launched: list[str] = []
    if launcher == "adopted":
        adopted_lease_fd = os.open(
            handle.directory / "lease.lock",
            os.O_RDWR | os.O_NOFOLLOW,
        )
        fcntl.flock(adopted_lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monkeypatch.setattr(
            fleet_rollout.fleet_operation,
            "launch_supervisor",
            lambda *args, **kwargs: pytest.fail(
                "an adopted busy operation must not launch another supervisor"
            ),
        )
    else:

        def launch(operation: str, **kwargs: Any) -> _FakeRelaunchSupervisor:
            nonlocal supervisor
            del kwargs
            launched.append(operation)
            lease_fd = os.open(
                handle.directory / "lease.lock",
                os.O_RDWR | os.O_NOFOLLOW,
            )
            fcntl.flock(lease_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            supervisor = _FakeRelaunchSupervisor(
                returncode=None,
                lease_fd=lease_fd,
            )
            return supervisor

        monkeypatch.setattr(
            fleet_rollout.fleet_operation,
            "launch_supervisor",
            launch,
        )

    try:
        with pytest.raises(
            fleet_rollout.FleetRolloutError,
            match="durable activation",
        ) as raised:
            if entrypoint == "reconcile":
                with _continuous_controller_fence(
                    tmp_path,
                    rollout,
                ) as (lifecycle_handoff, active_resources):
                    assert active_resources == lifecycle_resources
                    fleet_rollout.reconcile_durable_operations(
                        current_report_digest_sha256=digest,
                        inspect_only=False,
                        lifecycle_handoff=lifecycle_handoff,
                    )
            else:
                fleet_rollout.execute_one(
                    plan,
                    action,
                    rollout_id=rollout,
                    report_digest_resolver=lambda: digest,
                )

        if launcher == "owned":
            assert "owned launcher was terminated and reaped" in str(raised.value)
            assert "same-operation resumption" in str(raised.value)
            assert launched == [handle.operation_id]
            assert supervisor is not None
            assert supervisor.poll_calls >= 1
            assert supervisor.terminate_calls == 1
            assert supervisor.wait_calls == 1
            assert fleet_operation.observe_operation(handle.operation_id).state == (
                "authorized-unactivated"
            )
        else:
            assert "adopted process still owns" in str(raised.value)
            assert "cannot be safely identified" in str(raised.value)
            assert adopted_lease_fd is not None
            assert fleet_operation.observe_operation(handle.operation_id).state == (
                "running-authorized"
            )
        persisted = fleet_rollout.load_run(rollout)
        assert persisted is not None
        attempts = persisted.actions[action.id]["operation_attempts"]
        assert len(attempts) == 1
        assert attempts[0]["operation_id"] == handle.operation_id
    finally:
        if supervisor is not None:
            supervisor.close_lease()
        if adopted_lease_fd is not None:
            os.close(adopted_lease_fd)


@pytest.mark.parametrize("entrypoint", ["reconcile", "direct"])
def test_internally_launched_supervisor_is_reaped_after_observation_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entrypoint: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = f"v0.15.60-{entrypoint}-post-launch-observe-error"
    lifecycle_resources = (
        _continuous_controller_resources(tmp_path, rollout)
        if entrypoint == "reconcile"
        else ()
    )
    handle = _journal_durable_action(
        action=action,
        rollout=rollout,
        digest=digest,
        lifecycle_resources=lifecycle_resources,
    )
    _publish_authorization_without_activation(handle)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    supervisor = _FakeRelaunchSupervisor(returncode=None)
    launched = False
    real_observe = fleet_operation.observe_operation

    def launch(operation: str, **kwargs: Any) -> _FakeRelaunchSupervisor:
        nonlocal launched
        del operation, kwargs
        launched = True
        return supervisor

    def observe(operation: str, **kwargs: Any):  # type: ignore[no-untyped-def]
        if launched:
            raise fleet_operation.OperationStateError(
                "injected post-launch observation failure"
            )
        return real_observe(operation, **kwargs)

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        observe,
    )
    monkeypatch.setattr(fleet_rollout.time, "sleep", lambda _seconds: None)

    with pytest.raises(Exception, match="post-launch observation failure"):
        if entrypoint == "reconcile":
            with _continuous_controller_fence(
                tmp_path,
                rollout,
            ) as (lifecycle_handoff, active_resources):
                assert active_resources == lifecycle_resources
                fleet_rollout.reconcile_durable_operations(
                    current_report_digest_sha256=digest,
                    inspect_only=False,
                    lifecycle_handoff=lifecycle_handoff,
                )
        else:
            fleet_rollout.execute_one(
                plan,
                action,
                rollout_id=rollout,
                report_digest_resolver=lambda: digest,
            )

    assert launched is True
    assert supervisor.poll_calls >= 1
    assert supervisor.terminate_calls == 1
    assert supervisor.wait_calls == 1


def test_passed_supervisor_is_reaped_after_activated_stream_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleaning the launcher PID must not depend on pre-activation state."""
    operation = "9" * 64
    supervisor = _FakeRelaunchSupervisor(returncode=None)

    class ActivatedObservation:
        state = "running-authorized"
        ready = {"nonce": "d" * 64}
        lease_busy = True
        activation = {"launch_intent": True}
        result = None

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda _operation: ActivatedObservation(),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "read_output_since",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("injected activated stream failure")
        ),
    )

    with pytest.raises(RuntimeError, match="activated stream failure"):
        fleet_rollout._stream_operation_until_terminal(
            operation,
            resume_authorized=True,
            launched_supervisor=supervisor,
        )

    assert supervisor.terminate_calls == 1
    assert supervisor.wait_calls == 1


def test_durable_supervisor_reap_tolerates_exit_before_signal() -> None:
    """A launcher exit between poll and SIGTERM is a successful reap race."""

    class RacingSupervisor(_FakeRelaunchSupervisor):
        def terminate(self) -> None:
            self.terminate_calls += 1
            self.returncode = 0
            raise ProcessLookupError("launcher exited before SIGTERM")

    supervisor = RacingSupervisor(returncode=None)

    assert (
        fleet_rollout._reap_durable_supervisor(
            supervisor,
            terminate=True,
        )
        == 0
    )
    assert supervisor.terminate_calls == 1
    assert supervisor.wait_calls == 1


def test_cross_report_reconciliation_relaunches_killed_pre_recorder_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """A committed run decision resumes its exact attempt, even when old."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    old_digest = "a" * 64
    rollout = "v0.15.60-killed-before-recorder"
    fence_stack = ExitStack()
    request.addfinalizer(fence_stack.close)
    lifecycle_handoff, lifecycle_resources = fence_stack.enter_context(
        _continuous_controller_fence(tmp_path, rollout)
    )
    assert lifecycle_handoff is not None
    handle = _journal_durable_action(
        action=action,
        rollout=rollout,
        digest=old_digest,
        lifecycle_resources=lifecycle_resources,
    )
    operation_handoff = fleet_rollout.attach_active_rollout_lock(
        lifecycle_handoff,
        rollout_id=rollout,
    )
    recorder_popen_entered = tmp_path / "recorder-popen-entered"

    def blocked_recorder_popen(
        argv: list[str],
        **kwargs: object,
    ) -> subprocess.Popen[bytes]:
        del argv, kwargs
        recorder_popen_entered.write_text("entered\n")
        time.sleep(30)
        raise AssertionError("killed supervisor unexpectedly returned")

    supervisor_pid = os.fork()
    if supervisor_pid == 0:  # pragma: no cover - assertions run in parent
        try:
            fleet_operation.run_supervisor(
                handle.operation_id,
                state_root=handle.state_root,
                authorization_timeout=5,
                poll_interval=0.005,
                recorder_popen=blocked_recorder_popen,
                lifecycle_handoff=operation_handoff,
            )
        except BaseException:
            os._exit(91)
        os._exit(0)
    try:
        fleet_operation.wait_for_ready(handle.operation_id, timeout=5)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=old_digest,
        )
        deadline = time.monotonic() + 5
        while not recorder_popen_entered.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
        os.kill(supervisor_pid, signal.SIGKILL)
        waited, status = os.waitpid(supervisor_pid, 0)
        assert waited == supervisor_pid
        assert os.WIFSIGNALED(status)
    finally:
        with suppress(ProcessLookupError, ChildProcessError):
            os.kill(supervisor_pid, signal.SIGKILL)
        with suppress(ChildProcessError):
            os.waitpid(supervisor_pid, 0)

    before = fleet_operation.observe_operation(handle.operation_id)
    assert before.state == "authorized-unactivated"
    ready_bytes = (handle.directory / "ready.json").read_bytes()
    authorization_bytes = (handle.directory / "authorization.json").read_bytes()
    real_launch = fleet_operation.launch_supervisor
    launched: list[str] = []

    def launch(operation: str, **kwargs: Any) -> subprocess.Popen[bytes]:
        launched.append(operation)
        return real_launch(operation, **kwargs)

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )

    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        inspect_only=False,
        lifecycle_handoff=lifecycle_handoff,
    )

    finished = fleet_rollout.load_run(rollout)
    assert finished is not None
    record = finished.actions[action.id]
    assert record["status"] == "success"
    assert launched == [handle.operation_id]
    assert len(record["operation_attempts"]) == 1
    assert record["operation_attempts"][0]["operation_id"] == handle.operation_id
    assert record["operation_attempts"][0]["harvested"] is True
    assert (handle.directory / "ready.json").read_bytes() == ready_bytes
    assert (
        handle.directory / "authorization.json"
    ).read_bytes() == authorization_bytes
    fence_stack.close()


def test_direct_execute_relaunches_authorized_unactivated_same_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-direct-authorized-unactivated"
    handle = _journal_durable_action(
        action=action,
        rollout=rollout,
        digest=digest,
    )
    _publish_authorization_without_activation(handle)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    real_launch = fleet_operation.launch_supervisor
    launched: list[str] = []

    def launch(operation: str, **kwargs: Any) -> subprocess.Popen[bytes]:
        launched.append(operation)
        return real_launch(operation, **kwargs)

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )

    finished = fleet_rollout.execute_one(
        plan,
        action,
        rollout_id=rollout,
        report_digest_resolver=lambda: digest,
    )

    record = finished.actions[action.id]
    assert record["status"] == "success"
    assert launched == [handle.operation_id]
    assert len(record["operation_attempts"]) == 1
    assert record["operation_attempts"][0]["attempt"] == 1
    assert record["operation_attempts"][0]["operation_id"] == handle.operation_id


def test_inspect_only_authorized_unactivated_is_byte_stable_and_never_relaunches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-inspect-authorized-unactivated"
    handle = _journal_durable_action(
        action=action,
        rollout=rollout,
        digest=digest,
    )
    _publish_authorization_without_activation(handle)
    journal = fleet_rollout.rollout_state_path(rollout)

    def snapshot() -> dict[str, tuple[bytes, int]]:
        files = [
            journal,
            *sorted(path for path in handle.directory.iterdir() if path.is_file()),
        ]
        return {
            str(path): (path.read_bytes(), path.stat().st_mtime_ns)
            for path in files
        }

    before = snapshot()
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail("inspect-only must not relaunch"),
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="authorized-unactivated.*will not reconcile",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=True,
        )

    assert snapshot() == before


def test_default_execute_one_uses_a_real_detached_durable_supervisor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(_report()),
        topology={},
        actions=[action],
    )
    rollout = "v0.15.60-real-supervisor"

    run = fleet_rollout.execute_one(
        plan,
        action,
        rollout_id=rollout,
        report_digest_resolver=lambda: str(plan.report["digest_sha256"]),
    )

    record = run.actions[action.id]
    assert record["status"] == "success"
    assert record["rc"] == 0
    assert len(record["operation_attempts"]) == 1
    attempt = record["operation_attempts"][0]
    assert attempt["attempt"] == 1
    assert attempt["state"] == "completed"
    assert attempt["harvested"] is True
    observed = fleet_operation.observe_operation(attempt["operation_id"])
    assert observed.result is not None
    assert observed.result["executed"] is True
    assert observed.result["returncode"] == 0


def test_terminal_attempt_requires_fresh_plan_attestation_before_attempt_two(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-explicit-attempt-two"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    fleet_rollout.execute_one(plan, action, rollout_id=rollout)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="fresh live plan",
    ):
        fleet_rollout.execute_one(plan, action, rollout_id=rollout)

    run = fleet_rollout.execute_one(
        plan,
        action,
        rollout_id=rollout,
        durable_reconciled=True,
    )

    attempts = run.actions[action.id]["operation_attempts"]
    assert [item["attempt"] for item in attempts] == [1, 2]
    assert all(item["harvested"] is True for item in attempts)


def test_inspect_only_pending_failure_is_byte_stable_and_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    action.argv = ["--not-a-real-option"]
    digest = "a" * 64
    rollout = "v0.15.60-inspect-pending-failure"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(plan, action, rollout_id=rollout)
    state_path = fleet_rollout.rollout_state_path(rollout)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="unacknowledged executed failure.*will not consume",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=True,
        )

    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def test_controller_death_after_authorization_is_adopted_and_harvested_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Dropping the first controller must not launch a second child."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    report_digest = "a" * 64
    rollout = "v0.15.60-controller-death"
    fence_stack = ExitStack()
    request.addfinalizer(fence_stack.close)
    lifecycle_handoff, lifecycle_resources = fence_stack.enter_context(
        _continuous_controller_fence(tmp_path, rollout)
    )
    assert lifecycle_handoff is not None
    identity = fleet_rollout._operation_identity(
        rollout,
        report_digest,
        action,
        attempt=1,
        lifecycle_resources=lifecycle_resources,
        require_rollout_lock=True,
    )
    handle = fleet_operation.prepare_operation(identity)
    prepared = fleet_operation.observe_operation(handle.operation_id)
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=report_digest,
        report_source_path="report.json",
        actions={
            action.id: {
                "decision": "update",
                "reason": action.reason,
                "status": "running",
                "argv": action.argv,
                "target_sha": action.target_sha,
                "target_version": action.target_version,
                "target_tag": action.target_tag,
                "operation_attempts": [
                    fleet_rollout._attempt_ref(
                        prepared,
                        identity,
                        request_sha256=handle.request_sha256,
                    )
                ],
            }
        },
    )
    fleet_rollout.save_run(run)
    operation_handoff = fleet_rollout.attach_active_rollout_lock(
        lifecycle_handoff,
        rollout_id=rollout,
    )
    supervisor = fleet_operation.launch_supervisor(
        handle.operation_id,
        lifecycle_handoff=operation_handoff,
    )
    fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=report_digest,
    )
    # The original controller disappears here: no wait, result read, or
    # journal transition. A new invocation adopts the lifetime lease/result.
    del supervisor

    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=report_digest,
        inspect_only=False,
        lifecycle_handoff=lifecycle_handoff,
    )
    first = fleet_rollout.load_run(rollout)
    assert first is not None
    assert first.actions[action.id]["status"] == "success"
    assert len(first.actions[action.id]["operation_attempts"]) == 1
    assert first.actions[action.id]["operation_attempts"][0]["harvested"] is True
    first_payload = first.as_dict()

    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=report_digest,
        inspect_only=False,
        lifecycle_handoff=lifecycle_handoff,
    )
    second = fleet_rollout.load_run(rollout)
    assert second is not None
    assert second.as_dict() == first_payload
    assert len(second.actions[action.id]["operation_attempts"]) == 1
    fence_stack.close()


def test_controller_death_nonzero_harvest_stays_host_local_without_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    rollout = "v0.15.60-controller-death-failed"
    failed = _safe_durable_action()
    failed.id = "local-runtime:host-a:vibeqc-queue"
    failed.host = "host-a"
    fence_stack = ExitStack()
    request.addfinalizer(fence_stack.close)
    lifecycle_handoff, lifecycle_resources = fence_stack.enter_context(
        _continuous_controller_fence(tmp_path, rollout)
    )
    assert lifecycle_handoff is not None
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        failed,
        attempt=1,
        lifecycle_resources=lifecycle_resources,
        require_rollout_lock=True,
    )
    handle = fleet_operation.prepare_operation(identity)
    prepared = fleet_operation.observe_operation(handle.operation_id)
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=digest,
            report_source_path="report.json",
            holds={
                "host-a": {
                    "host": "host-a",
                    "kind": "full",
                    "owned": True,
                    "status": "active",
                    "reason": fleet_rollout._hold_reason(rollout, "host-a"),
                    "set_at": "2026-08-10T20:00:00+00:00",
                    "control_host": "host-a",
                }
            },
            actions={
                failed.id: {
                    "status": "running",
                    "operation_attempts": [
                        fleet_rollout._attempt_ref(
                            prepared,
                            identity,
                            request_sha256=handle.request_sha256,
                        )
                    ],
                }
            },
        )
    )
    supervisor_errors: list[BaseException] = []
    operation_handoff = fleet_rollout.attach_active_rollout_lock(
        lifecycle_handoff,
        rollout_id=rollout,
    )

    def forced_failure_supervisor() -> None:
        try:
            fleet_operation.run_supervisor(
                handle.operation_id,
                authorization_timeout=10,
                poll_interval=0.01,
                child_popen=lambda _argv, **kwargs: subprocess.Popen(
                    [sys.executable, "-c", "raise SystemExit(9)"],
                    **kwargs,
                ),
                lifecycle_handoff=operation_handoff,
            )
        except BaseException as exc:
            supervisor_errors.append(exc)

    supervisor = threading.Thread(target=forced_failure_supervisor)
    supervisor.start()
    fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=digest,
    )
    supervisor.join(timeout=10)
    assert not supervisor.is_alive()
    assert supervisor_errors == []

    # Recovery harvests the known failed receipt but does not globally abort
    # discovery/planning for independent hosts.
    reconciliation = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
        lifecycle_handoff=lifecycle_handoff,
    )
    harvested = fleet_rollout.load_run(rollout)
    assert harvested is not None
    assert harvested.actions[failed.id]["status"] == "failed"
    assert len(harvested.actions[failed.id]["operation_attempts"]) == 1
    assert reconciliation.failed_operation_hosts == (
        (
            rollout,
            "host-a",
            "local-runtime:host-a:vibeqc-queue executed and failed with exit 9; "
            "it is not retry-safe",
        ),
    )
    assert harvested.holds["host-a"]["status"] == "active"

    sibling = _safe_durable_action()
    sibling.id = "local-runtime:host-a:vibeqc-release"
    sibling.host = "host-a"
    sibling.program = "vibeqc-release"
    sibling.pin_name = "release"
    healthy = _safe_durable_action()
    healthy.id = "local-runtime:host-b:vibeqc-queue"
    healthy.host = "host-b"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[failed, sibling, healthy],
    )

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout,
        durable_reconciled=True,
        reconciled_failures=reconciliation.failed_operation_hosts,
        lifecycle_handoff=lifecycle_handoff,
        lifecycle_resources=lifecycle_resources,
    )

    assert run.actions[failed.id]["status"] == "failed"
    assert len(run.actions[failed.id]["operation_attempts"]) == 1
    assert run.actions[sibling.id]["status"] == "not-run"
    assert run.actions[healthy.id]["status"] == "success"
    assert "host-a" in run.failed_hosts
    assert run.holds["host-a"]["status"] == "active"
    consumed = run.actions[failed.id]["operation_attempts"][0]
    assert consumed["failure_fence_consumed"] is True
    assert consumed["failure_retry_authorized"] is False

    controls: list[list[str]] = []
    hold_active = True

    def release_control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal hold_active
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": hold_active,
                        "is_full_drain": hold_active,
                        "scheduler_hosts": [],
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    rollout, "host-a"
                                ),
                                "set_at": "2026-08-10T20:00:00+00:00",
                            }
                            if hold_active
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        hold_active = "--release-full" not in args
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    next_invocation = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
        control_runner=release_control,
        lifecycle_handoff=lifecycle_handoff,
    )
    assert next_invocation.failed_operation_hosts == ()
    authorized = fleet_rollout.load_run(rollout)
    assert authorized is not None
    first_attempt = authorized.actions[failed.id]["operation_attempts"][0]
    assert first_attempt["failure_retry_authorized"] is True
    assert authorized.holds["host-a"]["status"] == "released"
    assert hold_active is False

    retried = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout,
        durable_reconciled=True,
        control_runner=release_control,
        lifecycle_handoff=lifecycle_handoff,
        lifecycle_resources=lifecycle_resources,
    )
    attempts = retried.actions[failed.id]["operation_attempts"]
    assert [item["attempt"] for item in attempts] == [1, 2]
    assert attempts[0]["failure_retry_authorized"] is True
    assert attempts[1]["harvested"] is True
    assert retried.actions[failed.id]["status"] == "success"
    assert retried.actions[sibling.id]["status"] == "success"
    assert retried.holds["host-a"]["status"] == "released"
    assert hold_active is False
    fence_stack.close()


def test_harvested_failure_after_driver_reentry_is_not_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The second interpreter cannot forget a failure harvested by the first."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    rollout = "v0.15.60-driver-and-remote-recovery"
    driver = _safe_durable_action(phase="driver")
    failed = _safe_durable_action()
    failed.id = "host-a:failed"
    failed.host = "host-a"
    failed.argv = ["--not-a-real-option"]
    action_records: dict[str, dict[str, Any]] = {}
    handles: list[fleet_operation.OperationHandle] = []
    for action in (driver, failed):
        identity = fleet_rollout._operation_identity(
            rollout,
            digest,
            action,
            attempt=1,
        )
        handle = fleet_operation.prepare_operation(identity)
        handles.append(handle)
        observed = fleet_operation.observe_operation(handle.operation_id)
        action_records[action.id] = {
            "decision": "update",
            "reason": action.reason,
            "status": "running",
            "operation_attempts": [
                fleet_rollout._attempt_ref(
                    observed,
                    identity,
                    request_sha256=handle.request_sha256,
                )
            ],
        }
    fleet_rollout.save_run(
        fleet_rollout.RolloutRun(
            rollout_id=rollout,
            report_digest_sha256=digest,
            report_source_path="report.json",
            actions=action_records,
        )
    )
    for handle in handles:
        supervisor = fleet_operation.launch_supervisor(handle.operation_id)
        del supervisor
        fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
        fleet_operation.authorize_operation(
            handle.operation_id,
            expected_report_digest_sha256=digest,
        )
        deadline = time.monotonic() + 10
        while fleet_operation.observe_operation(handle.operation_id).state != (
            "completed"
        ):
            assert time.monotonic() < deadline
            time.sleep(0.01)

    first = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
    )
    assert first.driver_reentry_rollout_id == rollout
    assert len(first.failed_operation_hosts) == 1
    capability = fleet_rollout.RolloutReentryCapability(
        rollout_id=str(first.driver_reentry_rollout_id),
        operation_id=str(first.driver_reentry_operation_id),
        request_sha256=str(first.driver_reentry_request_sha256),
        report_digest_sha256=str(
            first.driver_reentry_report_digest_sha256
        ),
    )

    # This is the fresh interpreter after the mandatory driver re-entry. The
    # pending marker is returned again until this process durably records the
    # host-local skip; merely harvesting in the old interpreter cannot lose it.
    second = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=digest,
        inspect_only=False,
        acknowledge_driver_reentry=rollout,
        authenticated_driver_reentry=capability,
    )
    assert second.driver_reentry_required is False
    assert second.failed_operation_hosts == first.failed_operation_hosts

    driver.decision = "skip"
    driver.reason = "already at target with LAST OK=true"
    sibling = _safe_durable_action()
    sibling.id = "host-a:sibling"
    sibling.host = "host-a"
    healthy = _safe_durable_action()
    healthy.id = "host-b:healthy"
    healthy.host = "host-b"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[driver, failed, sibling, healthy],
    )
    original_launch = fleet_operation.launch_supervisor
    launched: list[str] = []

    def launch(operation: str, **kwargs: Any):
        launched.append(operation)
        return original_launch(operation, **kwargs)

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )
    held: set[str] = set()

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        host = args[-1]
        if "--status" in args:
            active = host in held
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": active,
                        "is_full_drain": active,
                        "scheduler_hosts": [],
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    rollout, host
                                ),
                                "set_at": "2026-08-10T20:00:00+00:00",
                            }
                            if active
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        if "--release-full" in args:
            held.discard(host)
        else:
            held.add(host)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout,
        durable_reconciled=True,
        control_runner=control,
        reconciled_failures=second.failed_operation_hosts,
    )

    assert len(run.actions[failed.id]["operation_attempts"]) == 1
    assert run.actions[failed.id]["status"] == "failed"
    assert run.actions[sibling.id]["status"] == "not-run"
    assert run.actions[healthy.id]["status"] == "success"
    assert launched == [
        run.actions[healthy.id]["operation_attempts"][0]["operation_id"]
    ]
    consumed = run.actions[failed.id]["operation_attempts"][0]
    assert consumed["failure_fence_consumed"] is True
    assert consumed["failure_retry_authorized"] is False


def test_pending_cross_report_failure_without_plan_host_blocks_actionably(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    current_rollout = "v0.15.60-current"
    healthy = _safe_durable_action()
    healthy.id = "host-b:healthy"
    healthy.host = "host-b"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[healthy],
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="pending durable failure fence has no current-plan host lane",
    ):
        fleet_rollout.execute_plan(
            plan,
            rollout_id=current_rollout,
            runner=lambda *args, **kwargs: pytest.fail(
                "an unrelated host must not execute past the unresolved fence"
            ),
            reconciled_failures=(
                (
                    "v0.15.59-old",
                    "removed-host",
                    "old action executed and failed",
                ),
            ),
        )

    run = fleet_rollout.load_run(current_rollout)
    assert run is not None
    assert "removed-host" in run.failed_hosts


def test_legacy_running_action_without_operation_reference_requires_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-legacy-running",
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        actions={"remote": {"status": "running"}},
    )
    fleet_rollout.save_run(run)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="legacy running.*no durable operation reference",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
        )

    reconciliation = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        inspect_only=False,
        allow_legacy_reconciliation=True,
    )

    assert reconciliation.legacy_running_actions == (
        ("v0.15.60-legacy-running", "remote"),
    )
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.actions["remote"]["status"] == "running"


def test_legacy_running_action_with_unreferenced_durable_identity_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    rollout = "v0.15.60-unreferenced-durable"
    digest = "a" * 64
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={action.id: {"status": "running"}},
    )
    fleet_rollout.save_run(run)
    identity = fleet_rollout._operation_identity(
        rollout, digest, action, attempt=1,
    )
    handle = fleet_operation.prepare_operation(identity)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match=f"unreferenced durable operation {handle.operation_id}",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            allow_legacy_reconciliation=True,
        )


def _legacy_supersession_plan() -> fleet_rollout.RolloutPlan:
    report = _report()
    current = fleet_rollout.RolloutAction(
        id="local-runtime:host_b:vibeqc-release",
        phase="local-runtime",
        host="host_b",
        program="vibeqc-release",
        pin_name="release",
        target_sha=FUTURE,
        target_version="0.15.61",
        target_tag="v0.15.61",
        argv=[
            "admin",
            "update",
            "vibeqc-release",
            "host_b",
            "--tag",
            "v0.15.61",
            "--expected-sha",
            FUTURE,
        ],
        decision="skip",
        reason="already at target with LAST OK=true",
        before={
            "configured": True,
            "current_sha": FUTURE,
            "current_version": "0.15.61",
            "current_tag": "v0.15.61",
            "dirty": False,
            "last_ok": True,
            "acknowledged": False,
            "detail": "managed local venv",
        },
    )
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report={
            **fleet_release.report_summary(report),
            "digest_sha256": "b" * 64,
        },
        topology={
            "localhost": {
                "name": "localhost",
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            },
            "host_b": {
                "name": "host_b",
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            },
            "host_a": {
                "name": "host_a",
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            },
        },
        actions=[current],
    )


def _save_legacy_supersession_run(tmp_path: Path) -> fleet_rollout.RolloutRun:
    report = _report()
    rollout = fleet_rollout.rollout_id(report)
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
        actions={
            "local-runtime:host_b:vibeqc-release": {
                "argv": [
                    "admin",
                    "update",
                    "vibeqc-release",
                    "host_b",
                    "--tag",
                    "v0.15.60",
                    "--expected-sha",
                    RELEASE,
                ],
                "decision": "update",
                "reason": "target is newer",
                "status": "running",
            }
        },
        holds={
            host: {
                "host": host,
                "kind": "full",
                "owned": True,
                "preexisting": False,
                "reason": fleet_rollout._hold_reason(rollout, host),
                "status": "active",
            }
            for host in ("host_b", "host_a")
        },
    )
    fleet_rollout.save_run(run)
    return run


def test_legacy_action_is_superseded_unknown_and_holds_use_live_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        host = args[-1]
        if host == "host_a":
            if "--status" not in args:
                pytest.fail(
                    "a host_b action must not authorize release of host_a"
                )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": True,
                        "mode": "full",
                        "is_full_drain": True,
                        "is_scheduler_target_drain": False,
                        "scheduler_hosts": [],
                        "scheduler_leases": [],
                        "orphaned_scheduler_leases": [],
                        "scheduler_leases_error": None,
                        "submit_policy": "deny",
                        "state": {
                            "reason": fleet_rollout._hold_reason(
                                run.rollout_id, "host_a"
                            ),
                            "set_at": "2026-08-08T12:00:00+00:00",
                        },
                    }
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(
                {
                    "active": False,
                    "mode": "inactive",
                    "is_full_drain": False,
                    "is_scheduler_target_drain": False,
                    "scheduler_hosts": [],
                    "scheduler_leases": [],
                    "orphaned_scheduler_leases": [],
                    "scheduler_leases_error": None,
                    "submit_policy": "accept_pending",
                    "state": None,
                }
            ),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    action = persisted.actions["local-runtime:host_b:vibeqc-release"]
    assert action["status"] == "superseded"
    assert action["observed_outcome"] == "unknown"
    assert action["argv"][-1] == RELEASE
    assert "operation_attempts" not in action
    assert persisted.complete is False
    assert persisted.holds["host_b"]["status"] == "released"
    assert persisted.holds["host_a"]["status"] == "active"
    assert result.superseded_actions == (
        (run.rollout_id, "local-runtime:host_b:vibeqc-release"),
    )
    assert result.settled_inactive_holds == ((run.rollout_id, "host_b"),)
    assert result.released_live_holds == ()
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_a")
    assert all("--release" not in args for args in controls)


@pytest.mark.parametrize("decision", ["update", "defer", "block"])
def test_unproven_legacy_action_is_durably_retained_without_hold_probe(
    decision: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    plan.actions[0].decision = decision
    plan.actions[0].reason = "not proven current"
    run = _save_legacy_supersession_run(tmp_path)
    before_action = dict(run.actions["local-runtime:host_b:vibeqc-release"])
    before_holds = {host: dict(hold) for host, hold in run.holds.items()}
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "retained action group must not inspect or release its holds"
        ),
    )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.actions["local-runtime:host_b:vibeqc-release"] == before_action
    assert persisted.holds == before_holds
    assert set(persisted.legacy_retained_holds) == {"host_a", "host_b"}
    assert set(persisted.legacy_retained_actions) == {
        "local-runtime:host_b:vibeqc-release"
    }
    assert result.superseded_actions == ()
    assert result.retained_actions[0][:3] == (
        run.rollout_id,
        "local-runtime:host_b:vibeqc-release",
        "host_b",
    )


def test_legacy_action_with_update_marker_is_durably_retained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    before_action = dict(run.actions["local-runtime:host_b:vibeqc-release"])
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    marker = {
        "host": "host_b",
        "envs": ["vibeqc-release"],
        "readable": True,
    }

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": marker, "markers": [marker]}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "marker-retained action group must not inspect its holds"
        ),
    )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.actions["local-runtime:host_b:vibeqc-release"] == before_action
    assert "overlapping admin-update marker" in result.retained_actions[0][3]
    assert set(persisted.legacy_retained_holds) == {"host_a", "host_b"}


def _create_retained_legacy_action_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_holds: bool,
) -> tuple[fleet_rollout.RolloutRun, fleet_rollout.RolloutPlan]:
    historical = _report()
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    run = _save_legacy_supersession_run(tmp_path)
    if not with_holds:
        run.holds = {}
        fleet_rollout.save_run(run)
    plan = _legacy_supersession_plan()
    plan.actions[0].decision = "defer"
    plan.actions[0].reason = "host unreachable"
    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "retained action group must not inspect holds"
        ),
    )
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    return persisted, plan


def test_retained_action_receipt_allows_read_only_inventory_without_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "read-only retained receipt inventory must issue no control"
        ),
    )

    assert {host for _rollout, host, _reason in result.retained_legacy_holds} == {
        "host_a",
        "host_b",
    }
    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before


def test_corrupt_retained_action_receipt_fails_before_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=False
    )
    action_id = next(iter(run.legacy_retained_actions))
    run.legacy_retained_actions[action_id]["action_record_sha256"] = "0" * 64
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="incoherent retained legacy action receipt",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report["digest_sha256"]),
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=False,
            allowed_retention_identity_mismatch_hosts=frozenset({"host_b"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "corrupt receipt must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


def test_new_report_requires_explicit_retry_and_can_promote_retained_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=False
    )
    retry = _legacy_supersession_plan()
    retry.report["source_path"] = "vibe-queue/releases/v0.15.132.json"
    retry.report["digest_sha256"] = "c" * 64

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="different accepted report.*--reconcile-legacy",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path=str(retry.report["source_path"]),
            inspect_only=True,
        )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="c" * 64,
        current_report_source_path=str(retry.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "receipt reauthentication inventory must not issue controls"
        ),
    )
    assert inventory.legacy_running_actions == (
        (run.rollout_id, "local-runtime:host_b:vibeqc-release"),
    )
    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=inventory.legacy_running_actions,
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=retry,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "c" * 64,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "hold-free promotion must issue no controls"
        ),
    )

    assert result.superseded_actions == (
        (run.rollout_id, "local-runtime:host_b:vibeqc-release"),
    )
    promoted = fleet_rollout.load_run(run.rollout_id)
    assert promoted is not None
    assert promoted.actions["local-runtime:host_b:vibeqc-release"]["status"] == (
        "superseded"
    )
    assert promoted.legacy_retained_actions == {}


def test_new_report_can_keep_exact_receipts_for_explicitly_excluded_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, _plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="c" * 64,
        current_report_source_path="vibe-queue/releases/v0.15.132.json",
        inspect_only=True,
        allowed_retention_identity_mismatch_hosts=frozenset(
            {"host_a", "host_b"}
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "excluded retained receipts must issue no control"
        ),
    )

    assert {host for _rollout, host, _reason in inventory.retained_legacy_holds} == {
        "host_a",
        "host_b",
    }
    assert state_path.read_bytes() == before


def test_new_report_still_rejects_receipt_on_selected_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, _plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="different accepted report.*--reconcile-legacy",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path="vibe-queue/releases/v0.15.132.json",
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset({"host_a"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "selected stale receipt must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


def test_retained_receipt_rejects_unconfigured_host_before_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="unconfigured host 'host_b'",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report["digest_sha256"]),
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=True,
            configured_retention_hosts=frozenset({"host_a"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "unconfigured receipt host must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


def test_retained_action_group_rejects_divergent_report_contexts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, _plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    run.legacy_retained_holds["host_a"]["current_report_path"] = (
        "vibe-queue/releases/v0.15.132.json"
    )
    run.legacy_retained_holds["host_a"]["current_report_digest_sha256"] = "c" * 64
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="action group has incoherent receipt context",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path="vibe-queue/releases/v0.15.132.json",
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset(
                {"host_a", "host_b"}
            ),
            configured_retention_hosts=frozenset({"host_a", "host_b"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "divergent action-group context must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


def test_excluded_stale_receipt_rejects_invalid_accepted_report_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, _plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    run.legacy_retained_holds["host_a"]["current_report_path"] = ""
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="incoherent retained legacy hold receipt for 'host_a'",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path="vibe-queue/releases/v0.15.132.json",
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset(
                {"host_a", "host_b"}
            ),
            configured_retention_hosts=frozenset({"host_a", "host_b"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "invalid receipt path must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


@pytest.mark.parametrize(
    ("kind", "source", "control_host"),
    [
        ("full", "hold-observation", "host_f"),
        ("scheduler-target", "scheduler-observation", "rogue"),
    ],
)
def test_excluded_stale_receipt_rejects_incoherent_control_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    source: Any,
    control_host: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    hold = {
        "host": "host_a",
        "kind": kind,
        "owned": True,
        "status": "active",
        "reason": "legacy rollout hold",
    }
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.118-b9ea64e76214",
        report_digest_sha256="d" * 64,
        report_source_path="vibe-queue/releases/v0.15.118.json",
        holds={"host_a": hold},
    )
    receipt_plan = _legacy_supersession_plan()
    receipt_plan.report["source_path"] = "vibe-queue/releases/v0.15.147.json"
    receipt_plan.report["digest_sha256"] = "e" * 64
    run.legacy_retained_holds["host_a"] = fleet_rollout._legacy_retained_hold_receipt(
        run,
        host="host_a",
        hold=hold,
        plan=receipt_plan,
        source=source,
        control_host=control_host,
        retained_at="2026-08-27T12:00:00+00:00",
        reason="host_a is unreachable; exact hold remains retained",
    )
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="incoherent control host for 'host_a'",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path="vibe-queue/releases/v0.15.148.json",
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset({"host_a"}),
            configured_retention_hosts=frozenset({"host_a", "host_f"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "incoherent control host must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


@pytest.mark.parametrize("receipt_kind", ["action", "hold"])
@pytest.mark.parametrize(
    ("replacement_path", "replacement_digest"),
    [
        (None, "c" * 64),
        ("vibe-queue/releases/v0.15.132.json", None),
    ],
)
def test_excluded_receipt_rejects_split_current_report_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_kind: str,
    replacement_path: str | None,
    replacement_digest: str | None,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    receipt = (
        next(iter(run.legacy_retained_actions.values()))
        if receipt_kind == "action"
        else run.legacy_retained_holds["host_a"]
    )
    if replacement_path is not None:
        receipt["current_report_path"] = replacement_path
    if replacement_digest is not None:
        receipt["current_report_digest_sha256"] = replacement_digest
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="incoherent current report identity",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report["digest_sha256"]),
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset(
                {"host_a", "host_b"}
            ),
            configured_retention_hosts=frozenset({"host_a", "host_b"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "split report identity must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


@pytest.mark.parametrize(
    ("receipt_path", "error"),
    [
        (
            "vibe-queue/releases/v0.15.148.json",
            "incoherent current report identity",
        ),
        (
            "vibe-queue/releases/v99.0.0.json",
            "not from an older accepted report",
        ),
    ],
)
def test_excluded_receipt_must_bind_an_older_accepted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_path: str,
    error: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, _plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    for receipt in (
        *run.legacy_retained_actions.values(),
        *run.legacy_retained_holds.values(),
    ):
        receipt["current_report_path"] = receipt_path
        receipt["current_report_digest_sha256"] = "f" * 64
    state_path = fleet_rollout.save_run(run)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match=error,
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="c" * 64,
            current_report_source_path="vibe-queue/releases/v0.15.148.json",
            inspect_only=True,
            allowed_retention_identity_mismatch_hosts=frozenset(
                {"host_a", "host_b"}
            ),
            configured_retention_hosts=frozenset({"host_a", "host_b"}),
            control_runner=lambda *args, **kwargs: pytest.fail(
                "non-older report identity must fail before control"
            ),
        )

    assert state_path.read_bytes() == before


@pytest.mark.parametrize(
    "sibling",
    [None, {}, {"status": "authorized"}],
)
def test_legacy_supersession_rejects_malformed_or_unknown_sibling_state(
    sibling: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.actions["local-runtime:host_a:vibeqc-release"] = sibling
    fleet_rollout.save_run(run)
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="sibling state|malformed action|unknown action status",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "malformed sibling must block before hold inspection"
            ),
        )

    assert state_path.read_bytes() == before


def test_legacy_batch_prevalidates_every_action_before_first_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    first = _save_legacy_supersession_run(tmp_path)
    second = _save_legacy_supersession_run(tmp_path)
    second.rollout_id = "v0.15.60-second-legacy-running"
    for host, hold in second.holds.items():
        hold["reason"] = fleet_rollout._hold_reason(second.rollout_id, host)
    second.actions["local-runtime:host_b:vibeqc-release"]["argv"][-1] = "f" * 40
    fleet_rollout.save_run(second)
    first_path = fleet_rollout.rollout_state_path(first.rollout_id)
    second_path = fleet_rollout.rollout_state_path(second.rollout_id)
    first_before = first_path.read_bytes()
    second_before = second_path.read_bytes()
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="journal/report identity does not match",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(
                (first.rollout_id, "local-runtime:host_b:vibeqc-release"),
                (second.rollout_id, "local-runtime:host_b:vibeqc-release"),
            ),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "all action proofs precede hold inspection"
            ),
        )

    assert first_path.read_bytes() == first_before
    assert second_path.read_bytes() == second_before


def test_legacy_historical_digest_failure_leaves_journal_byte_identical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            fleet_release.FleetReleaseError("no exact report digest")
        ),
    )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="historical report could not be authenticated",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
        )

    assert state_path.read_bytes() == before


def test_legacy_malformed_hold_blocks_before_action_supersession(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds["host_b"] = "bad"  # type: ignore[assignment]
    fleet_rollout.save_run(run)
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="malformed hold record",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "malformed hold blocks before any control call"
            ),
        )

    assert state_path.read_bytes() == before


def _persisted_legacy_supersession_evidence(
    run: fleet_rollout.RolloutRun,
    *,
    relation: str = "equal",
) -> dict[str, Any]:
    target_sha = FUTURE
    observed_sha = FUTURE if relation == "equal" else "8" * 40
    reason = (
        "already at target with LAST OK=true"
        if relation == "equal"
        else "newer descendant already deployed; no downgrade"
    )
    return {
        "status": "superseded",
        "observed_outcome": "unknown",
        "superseded_at": "2026-08-13T12:00:00+00:00",
        "legacy_reconciliation": {
            "schema": "vq.fleet.legacy_action_reconciliation/1",
            "action_id": "local-runtime:host_b:vibeqc-release",
            "phase": "local-runtime",
            "host": "host_b",
            "program": "vibeqc-release",
            "historical_report_path": run.report_source_path,
            "historical_report_digest_sha256": run.report_digest_sha256,
            "durable_operation_present": False,
            "overlapping_admin_update_marker": False,
            "current_report_path": "vibe-queue/releases/v0.15.61.json",
            "current_report_digest_sha256": "b" * 64,
            "current_action_reason": reason,
            "current_identity_relation": relation,
            "current_target_sha": target_sha,
            "observed_current_sha": observed_sha,
            "outcome_claim": "not-observed",
        },
    }


@pytest.mark.parametrize(
    "tamper",
    [
        "historical_report_path",
        "historical_report_digest_sha256",
        "durable_operation_present",
        "current_identity_relation",
    ],
)
def test_tampered_supersession_evidence_never_isolates_unknown_hold(
    tamper: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-tampered-supersession",
        report_digest_sha256="a" * 64,
        report_source_path="vibe-queue/releases/v0.15.60.json",
        holds={
            "host_a": {
                "host": "host_a",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": "legacy hold",
            }
        },
    )
    action = _persisted_legacy_supersession_evidence(run)
    evidence = action["legacy_reconciliation"]
    assert isinstance(evidence, dict)
    evidence[tamper] = True
    run.actions["local-runtime:host_b:vibeqc-release"] = action
    fleet_rollout.save_run(run)
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
                argv, 1, stdout="", stderr="network unavailable"
            ),
        )

    assert state_path.read_bytes() == before


def test_coherent_ahead_supersession_requires_explicit_hold_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_b")
    run.actions["local-runtime:host_b:vibeqc-release"] = (
        _persisted_legacy_supersession_evidence(run, relation="ahead")
    )
    fleet_rollout.save_run(run)
    state_path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = state_path.read_bytes()

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="unreceipted active full hold.*--reconcile-legacy",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=False,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "ordinary reconciliation must issue no controls"
            ),
        )
    assert state_path.read_bytes() == before

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "explicit inventory must issue no controls"
        ),
    )
    assert inventory.legacy_hold_retries == ((run.rollout_id, "host_a"),)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    recovery = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 1, stdout="", stderr="network unavailable"
        ),
    )
    assert recovery.retained_holds[0][:2] == (run.rollout_id, "host_a")

    # The explicit recovery writes a report-bound fence receipt. Later normal
    # dry-run inventory reads it without controls and can show unrelated work.
    read_only = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "receipt inventory must remain read-only"
        ),
    )
    assert read_only.retained_legacy_holds[0][:2] == (run.rollout_id, "host_a")


def _obsolete_legacy_full_hold(
    *,
    host: str,
    version: tuple[int, int, int],
    digest: str,
) -> tuple[fleet_release.FleetReleaseReport, fleet_rollout.RolloutRun]:
    report = fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path=(
            "vibe-queue/releases/"
            f"v{version[0]}.{version[1]}.{version[2]}.json"
        ),
        digest_sha256=digest,
        generated_at="2026-08-08T12:00:00Z",
        release_version=version,
        pins=dict(_report().pins),
        raw={},
    )
    rollout = fleet_rollout.rollout_id(report)
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
        actions={
            f"local-runtime:{host}:vibeqc-release": {"status": "success"}
        },
        holds={
            host: {
                "host": host,
                "kind": "full",
                "owned": True,
                "preexisting": False,
                "reason": fleet_rollout._hold_reason(rollout, host),
                "status": "active",
            }
        },
        complete=True,
    )
    return report, run


def _legacy_full_hold_recovery_plan(
    hosts: tuple[str, ...],
) -> fleet_rollout.RolloutPlan:
    actions = []
    for host in hosts:
        actions.append(
            fleet_rollout.RolloutAction(
                id=f"local-runtime:{host}:vibeqc-release",
                phase="local-runtime",
                host=host,
                program="vibeqc-release",
                pin_name="release",
                target_sha=FUTURE,
                target_version="0.15.141",
                target_tag="v0.15.141",
                argv=["admin", "update", "vibeqc-release", host],
                decision="skip",
                reason="already at target with LAST OK=true",
                before={
                    "configured": True,
                    "last_ok": True,
                    "current_sha": FUTURE,
                },
            )
        )
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report={
            "source_path": "vibe-queue/releases/v0.15.141.json",
            "digest_sha256": "f" * 64,
        },
        topology={
            host: {
                "name": host,
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            }
            for host in hosts
        },
        actions=actions,
    )


def _inactive_full_drain_status() -> dict[str, Any]:
    return {
        "active": False,
        "mode": "inactive",
        "is_full_drain": False,
        "is_scheduler_target_drain": False,
        "scheduler_hosts": [],
        "scheduler_leases": [],
        "orphaned_scheduler_leases": [],
        "scheduler_leases_error": None,
        "state": None,
        "submit_policy": "accept_pending",
    }


def _install_obsolete_legacy_full_hold_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutPlan,
]:
    host_a_report, host_a = _obsolete_legacy_full_hold(
        host="host_a", version=(0, 15, 118), digest="a" * 64
    )
    host_e_report, host_e = _obsolete_legacy_full_hold(
        host="host_e", version=(0, 15, 137), digest="b" * 64
    )
    fleet_rollout.save_run(host_a)
    fleet_rollout.save_run(host_e)
    reports = {
        host_a_report.digest_sha256: host_a_report,
        host_e_report.digest_sha256: host_e_report,
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: reports[digest],
    )
    return host_a, host_e, _legacy_full_hold_recovery_plan(("host_a", "host_e"))


def _install_failed_exact_host_e_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutPlan,
    fleet_rollout.RolloutAction,
    fleet_operation.OperationObservation,
]:
    host_a, host_e, plan = _install_obsolete_legacy_full_hold_pair(
        tmp_path, monkeypatch
    )
    host_e_action = next(
        action for action in plan.actions if action.host == "host_e"
    )
    identity = fleet_rollout._operation_identity(
        host_e.rollout_id,
        host_e.report_digest_sha256,
        host_e_action,
        attempt=1,
    )
    operation_id = fleet_operation.operation_id(identity)
    observation = fleet_operation.OperationObservation(
        operation_id=operation_id,
        identity=identity,
        request_sha256="c" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={
            "status": "failed",
            "executed": True,
            "returncode": 9,
        },
    )
    host_e.actions[host_e_action.id] = {
        "status": "failed",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                observation,
                identity,
                request_sha256=observation.request_sha256,
                harvested=True,
            )
        ],
    }
    host_e.holds["host_e"].update(
        {
            "duration_seconds": 21600,
            "set_at": "2026-08-08T12:00:00+00:00",
            "control_host": "host_e",
        }
    )
    fleet_rollout.save_run(host_e)
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: [operation_id],
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observation,
    )
    return host_a, host_e, plan, host_e_action, observation


def _accepted_failure_recovery_report() -> fleet_release.FleetReleaseReport:
    pins = dict(_report().pins)
    pins["release"] = _pin("release", FUTURE, "0.15.141", tag="v0.15.141")
    pins["dev"] = _pin("dev", FUTURE, "0.15.141", tag="v0.15.141")
    return fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.141.json",
        digest_sha256="f" * 64,
        generated_at="2026-08-25T12:00:00Z",
        release_version=(0, 15, 141),
        pins=pins,
        raw={},
    )


def test_failure_report_epoch_observer_is_strictly_local_and_non_fetching(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    accepted = _accepted_failure_recovery_report()
    expected_argv = [
        "git",
        "-C",
        str(repo.resolve()),
        "rev-parse",
        "--verify",
        "origin/main^{commit}",
    ]
    discover_calls: list[tuple[Path, bool]] = []
    run_calls: list[list[str]] = []

    def discover(repo_arg: Path, *, fetch: bool) -> fleet_release.FleetReleaseReport:
        discover_calls.append((repo_arg, fetch))
        return accepted

    def run(
        argv: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        run_calls.append(argv)
        assert kwargs == {
            "capture_output": True,
            "text": True,
            "timeout": 120,
            "check": False,
            "stdin": subprocess.DEVNULL,
        }
        return subprocess.CompletedProcess(
            expected_argv,
            0,
            stdout=f"{'a' * 40}\n",
            stderr="",
        )

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_latest_report",
        discover,
    )
    monkeypatch.setattr(fleet_rollout.subprocess, "run", run)

    observed = fleet_rollout._observe_local_failure_report_epoch(repo)

    assert discover_calls == [(repo, False)]
    assert run_calls == [expected_argv]
    assert observed == fleet_rollout._FailureReportEpoch(
        origin_main_commit="a" * 40,
        report_source_path=accepted.source_path,
        report_digest_sha256=accepted.digest_sha256,
        rollout_id=fleet_rollout.rollout_id(accepted),
    )


def _call_failure_transition_v2(
    *,
    accepted_report: fleet_release.FleetReleaseReport,
    **kwargs: Any,
) -> fleet_rollout.LegacyRolloutReconciliation:
    return fleet_rollout.reconcile_legacy_rollout_state(
        accepted_report=accepted_report,
        **kwargs,
    )


def test_failure_transition_capability_is_optional_and_keyword_only() -> None:
    parameter = inspect.signature(fleet_rollout.reconcile_legacy_rollout_state).parameters[
        "accepted_report"
    ]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is None


def _isolated_failed_host_e_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    fleet_rollout.RolloutRun,
    fleet_rollout.RolloutPlan,
    fleet_rollout.RolloutAction,
    fleet_operation.OperationObservation,
    fleet_release.FleetReleaseReport,
]:
    host_a, host_e, plan, action, observation = _install_failed_exact_host_e_pair(
        tmp_path,
        monkeypatch,
    )
    fleet_rollout.rollout_state_path(host_a.rollout_id).unlink()
    accepted = _accepted_failure_recovery_report()
    action.decision = "update"
    action.reason = "target is newer"
    action.before = {
        "configured": True,
        "current_sha": OLD,
        "current_version": "0.15.137",
        "current_tag": "v0.15.137",
        "dirty": False,
        "last_ok": True,
        "acknowledged": True,
        "detail": "behind accepted report",
        "metrics": None,
    }
    historical_action = copy.deepcopy(action)
    historical_action.target_sha = RELEASE
    historical_action.target_version = str(_report().pins["release"].version)
    historical_action.target_tag = _report().pins["release"].tag
    historical_identity = fleet_rollout._operation_identity(
        host_e.rollout_id,
        host_e.report_digest_sha256,
        historical_action,
        attempt=1,
    )
    observation = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(historical_identity),
        identity=historical_identity,
        request_sha256="c" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "failed", "executed": True, "returncode": 9},
    )
    host_e.actions[action.id] = {
        "status": "failed",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                observation,
                historical_identity,
                request_sha256=observation.request_sha256,
                harvested=True,
            )
        ],
    }
    action_b = copy.deepcopy(action)
    action_b.id = "helper:host_e:vibeqc-dev"
    action_b.phase = "helper"
    action_b.program = "vibeqc-dev"
    action_b.pin_name = "dev"
    action_b.argv = ["admin", "update", "vibeqc-dev", "host_e"]
    plan.actions.append(action_b)
    historical_action_b = copy.deepcopy(action_b)
    historical_action_b.target_sha = DEV
    historical_action_b.target_version = str(_report().pins["dev"].version)
    historical_action_b.target_tag = _report().pins["dev"].tag
    identity_b = fleet_rollout._operation_identity(
        host_e.rollout_id,
        host_e.report_digest_sha256,
        historical_action_b,
        attempt=1,
    )
    observation_b = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(identity_b),
        identity=identity_b,
        request_sha256="8" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "failed", "executed": True, "returncode": 7},
    )
    host_e.actions[action_b.id] = {
        "status": "failed",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                observation_b,
                identity_b,
                request_sha256=observation_b.request_sha256,
                harvested=True,
            )
        ],
    }
    successful_action = copy.deepcopy(historical_action)
    successful_action.id = "local-runtime:host_e:unrelated-success"
    successful_action.program = "unrelated-success"
    successful_identity = fleet_rollout._operation_identity(
        host_e.rollout_id,
        host_e.report_digest_sha256,
        successful_action,
        attempt=1,
    )
    successful_observation = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(successful_identity),
        identity=successful_identity,
        request_sha256="9" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "success", "executed": True, "returncode": 0},
    )
    host_e.actions[successful_action.id] = {
        "status": "success",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                successful_observation,
                successful_identity,
                request_sha256=successful_observation.request_sha256,
                harvested=True,
            )
        ],
    }
    host_e.actions[action.id]["legacy_failure_skip"] = {
        "schema": "vq.fleet.legacy_failure_skip/1",
        "action_id": action.id,
        "host": "host_e",
        "failed_rollout_id": host_e.rollout_id,
        "operation_ids": [observation.operation_id],
        "historical_report_path": host_e.report_source_path,
        "historical_report_digest_sha256": host_e.report_digest_sha256,
        "current_report_path": "vibe-queue/releases/v0.15.141.json",
        "current_report_digest_sha256": "e" * 64,
        "current_target_sha": action.target_sha,
        "current_action_reason": "already at target with LAST OK=true",
        "observed_current_sha": action.target_sha,
        "recorded_at": "2026-08-25T11:58:00+00:00",
    }
    host_e.legacy_retained_holds["host_e"] = fleet_rollout._legacy_retained_hold_receipt(
        host_e,
        host="host_e",
        hold=host_e.holds["host_e"],
        plan=plan,
        source="hold-observation",
        control_host="host_e",
        retained_at="2026-08-25T11:59:00+00:00",
        reason="prior observation retained exact hold",
    )
    host_e.complete = False
    fleet_rollout.save_run(host_e)
    old_report, _unused = _obsolete_legacy_full_hold(
        host="host_e", version=(0, 15, 137), digest="b" * 64
    )
    reports = {
        old_report.digest_sha256: old_report,
        accepted.digest_sha256: accepted,
    }
    observations = {
        item.operation_id: item for item in (observation, observation_b, successful_observation)
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: reports[digest],
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: sorted(observations),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )

    def observe_local_epoch(repo: Path) -> fleet_rollout._FailureReportEpoch:
        del repo
        source_path = str(plan.report["source_path"])
        digest = str(plan.report["digest_sha256"])
        return fleet_rollout._FailureReportEpoch(
            origin_main_commit="1" * 40,
            report_source_path=source_path,
            report_digest_sha256=digest,
            rollout_id=f"{accepted.release.tag}-{digest[:12]}",
        )

    monkeypatch.setattr(
        fleet_rollout,
        "_observe_local_failure_report_epoch",
        observe_local_epoch,
    )
    return host_e, plan, action, observation, accepted


def _failure_transition_kwargs(
    *,
    host_e: fleet_rollout.RolloutRun,
    plan: fleet_rollout.RolloutPlan,
    tmp_path: Path,
    control_runner: fleet_rollout.Runner,
) -> dict[str, Any]:
    return {
        "legacy_running_actions": (),
        "legacy_hold_retries": ((host_e.rollout_id, "host_e"),),
        "failed_operation_hosts": ((host_e.rollout_id, "host_e", "release failed rc=9"),),
        "plan": plan,
        "admin_status": {},
        "repo": tmp_path,
        "current_report_digest_resolver": lambda: str(plan.report["digest_sha256"]),
        "control_runner": control_runner,
    }


def _inactive_failure_status_runner(
    *,
    hosts: tuple[str, ...] = ("host_e",),
    offline: tuple[str, ...] = (),
    calls: list[list[str]] | None = None,
) -> fleet_rollout.Runner:
    def control(argv: list[str], **unused: Any) -> subprocess.CompletedProcess[str]:
        args = argv[3:]
        host = args[-1]
        assert host in hosts
        assert args == ["drain", "--status", "--json", host]
        if calls is not None:
            calls.append(args)
        if host in offline:
            return subprocess.CompletedProcess(
                argv,
                255,
                stdout="",
                stderr="network unreachable",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    return control


def _failed_action_ids(run: fleet_rollout.RolloutRun) -> tuple[str, ...]:
    return tuple(
        sorted(action_id for action_id, row in run.actions.items() if row.get("status") == "failed")
    )


def _failed_operation_ids(run: fleet_rollout.RolloutRun) -> tuple[str, ...]:
    return tuple(
        sorted(
            str(attempt["operation_id"])
            for action_id in _failed_action_ids(run)
            for attempt in run.actions[action_id]["operation_attempts"]
        )
    )


def _forbid_failure_transition_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for owner, name in (
        (fleet_rollout, "execute_action"),
        (fleet_rollout, "consume_reconciled_failure_fences"),
        (fleet_rollout.fleet_operation, "prepare_operation"),
        (fleet_rollout.fleet_operation, "authorize_operation"),
        (fleet_rollout.fleet_operation, "launch_supervisor"),
    ):
        monkeypatch.setattr(
            owner,
            name,
            lambda *unused, _name=name, **kwargs: pytest.fail(
                f"failure-only recovery reached forbidden {_name} seam"
            ),
        )


def _assert_exact_failure_receipt(
    *,
    source: fleet_rollout.RolloutRun,
    current: fleet_rollout.RolloutRun,
    accepted: fleet_release.FleetReleaseReport,
    reason: str,
    hold_outcome: str = "observed-inactive",
) -> None:
    failed_ids = _failed_action_ids(source)
    expected_row = {
        "decision": "update",
        "reason": (f"skipped: an earlier lane on host_e failed ({reason})"),
        "status": "not-run",
    }
    assert current.actions == {action_id: expected_row for action_id in failed_ids}
    assert current.failed_hosts == {"host_e": reason}
    assert current.complete is False
    assert getattr(current, "legacy_failure_update_intent", None) is None
    assert "legacy_failure_update_intent" not in current.as_dict()
    backlinks = []
    for action_id in failed_ids:
        row = source.actions[action_id]
        assert "legacy_failure_skip" not in row
        backlink = row["legacy_failure_update_ack"]
        parsed = legacy_failure_transition.parse_failure_update_backlink(backlink)
        assert parsed.failed_action_ids == failed_ids
        assert parsed.failed_operation_ids == _failed_operation_ids(source)
        assert parsed.historical_report_path == source.report_source_path
        assert parsed.historical_report_digest_sha256 == source.report_digest_sha256
        assert parsed.current_report_path == accepted.source_path
        assert parsed.current_report_digest_sha256 == accepted.digest_sha256
        assert parsed.current_rollout_id == current.rollout_id
        assert parsed.current_action_ids == failed_ids
        backlinks.append(backlink)
        for attempt in row["operation_attempts"]:
            assert attempt["failure_fence_consumed"] is True
            assert attempt["failure_retry_authorized"] is False
    assert all(backlink == backlinks[0] for backlink in backlinks)
    success = source.actions["local-runtime:host_e:unrelated-success"]
    assert success["operation_attempts"][0]["failure_fence_consumed"] is False
    assert success["operation_attempts"][0]["failure_retry_authorized"] is False
    assert source.holds["host_e"] == {
        "host": "host_e",
        "kind": "full",
        "owned": True,
        "status": "released",
        "legacy_reconciliation": {"outcome": hold_outcome},
    }
    assert "host_e" not in source.legacy_retained_holds
    assert source.complete is False


def test_failure_transition_v2_report_mismatch_is_zero_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path, monkeypatch
    )
    mismatched = fleet_release.FleetReleaseReport(
        source_ref=accepted.source_ref,
        source_path="vibe-queue/releases/v0.15.141-other.json",
        digest_sha256="a" * 64,
        generated_at=accepted.generated_at,
        release_version=accepted.release_version,
        pins=accepted.pins,
        raw={},
    )
    before = fleet_rollout.rollout_state_path(host_e.rollout_id).read_bytes()
    callbacks: list[str] = []
    _forbid_failure_transition_execution(monkeypatch)

    def forbidden(name: str) -> Any:
        callbacks.append(name)
        pytest.fail(f"mismatched report reached forbidden {name} seam")

    monkeypatch.setattr(
        fleet_rollout.paths,
        "atomic_write_text",
        lambda *unused: callbacks.append("write"),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "load_run",
        lambda *unused: forbidden("journal-load"),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *unused, **kwargs: forbidden("historical-report"),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *unused, **kwargs: forbidden("operation-list"),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda *unused, **kwargs: forbidden("operation-observe"),
    )
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=lambda *unused, **ignored: forbidden("control"),
    )
    kwargs["current_report_digest_resolver"] = lambda: forbidden("resolver")
    with pytest.raises(fleet_rollout.FleetRolloutError, match="report"):
        _call_failure_transition_v2(
            accepted_report=mismatched,
            **kwargs,
        )
    assert callbacks == []
    assert fleet_rollout.rollout_state_path(host_e.rollout_id).read_bytes() == before


def test_failure_transition_v2_creates_ack_backlink_consumes_and_inspects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(),
        ),
    )

    old = fleet_rollout.load_run(host_e.rollout_id)
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert old is not None
    assert current is not None
    assert action.id in _failed_action_ids(host_e)
    _assert_exact_failure_receipt(
        source=old,
        current=current,
        accepted=accepted,
        reason="release failed rc=9",
    )

    inspected = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "post-transition inspection must remain read-only"
        ),
    )
    assert inspected.failed_operation_hosts == ()


def test_failure_transition_v2_conditionally_releases_exact_active_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path, monkeypatch
    )
    _forbid_failure_transition_execution(monkeypatch)
    exact = host_e.holds["host_e"]
    reason = str(exact["reason"])
    set_at = str(exact["set_at"])
    active = True
    controls: list[list[str]] = []

    def control(argv: list[str], **unused: Any) -> subprocess.CompletedProcess[str]:
        nonlocal active
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            assert args == ["drain", "--status", "--json", "host_e"]
            status = _inactive_full_drain_status()
            if active:
                status.update(
                    {
                        "active": True,
                        "mode": "full",
                        "is_full_drain": True,
                        "state": {"reason": reason, "set_at": set_at},
                        "submit_policy": "deny",
                    }
                )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(status),
                stderr="",
            )
        assert args == [
            "drain",
            "--release",
            "--scheduler-host",
            "host_e",
            "--release-legacy-only",
            "--expected-legacy-set-at",
            set_at,
            "--expected-legacy-reason",
            reason,
            "host_e",
        ]
        active = False
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=control,
        ),
    )
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert persisted is not None
    assert current is not None
    _assert_exact_failure_receipt(
        source=persisted,
        current=current,
        accepted=accepted,
        reason="release failed rc=9",
        hold_outcome="conditional-absence-confirmed",
    )
    assert controls == [
        ["drain", "--status", "--json", "host_e"],
        [
            "drain",
            "--release",
            "--scheduler-host",
            "host_e",
            "--release-legacy-only",
            "--expected-legacy-set-at",
            set_at,
            "--expected-legacy-reason",
            reason,
            "host_e",
        ],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
    ]


@pytest.mark.parametrize(
    "crash_phase",
    ("current-intent", "old-backlink", "old-consume", "current-clear"),
)
def test_failure_transition_v2_restarts_after_each_atomic_save_and_report_advance(
    crash_phase: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    expected_recorded_at = "2026-08-25T12:34:56+00:00"
    monkeypatch.setattr(
        fleet_rollout,
        "utcnow_iso",
        lambda: expected_recorded_at,
    )
    _forbid_failure_transition_execution(monkeypatch)
    receipt_report = accepted
    current_id = fleet_rollout.rollout_id(accepted)
    current_path = fleet_rollout.rollout_state_path(current_id)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    old_before = old_path.read_bytes()
    original_guarded = getattr(fleet_rollout, "_guarded_save_run", None)
    assert original_guarded is not None, "Phase2 requires one guarded journal CAS seam"
    seen_current_intent = False
    crashed = False
    stage_bytes: dict[str, tuple[bytes, bytes]] = {}

    def crashing_guard(*args: Any, **kwargs: Any) -> Any:
        nonlocal seen_current_intent, crashed
        target_rollout_id = args[0]
        candidate, effect = original_guarded(*args, **kwargs)
        assert candidate.rollout_id == target_rollout_id
        payload = candidate.as_dict()
        action_row = payload.get("actions", {}).get(action.id, {})
        intent = payload.get("legacy_failure_update_intent")
        if candidate.rollout_id == current_id and intent:
            seen_current_intent = True
            observed_phase = "current-intent"
        elif candidate.rollout_id == host_e.rollout_id and (
            "legacy_failure_update_ack" in action_row
        ):
            attempts = action_row.get("operation_attempts", [])
            observed_phase = (
                "old-consume"
                if attempts and attempts[0].get("failure_fence_consumed") is True
                else "old-backlink"
            )
        elif candidate.rollout_id == current_id and seen_current_intent and not intent:
            observed_phase = "current-clear"
        else:
            return candidate, effect
        stage_bytes[observed_phase] = (
            current_path.read_bytes(),
            old_path.read_bytes(),
        )
        if observed_phase == crash_phase and not crashed:
            crashed = True
            raise RuntimeError(f"crash after {crash_phase}")
        return candidate, effect

    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", crashing_guard)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    with pytest.raises(RuntimeError, match=crash_phase):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert crashed is True

    interrupted_old = fleet_rollout.load_run(host_e.rollout_id)
    interrupted_current = fleet_rollout.load_run(current_id)
    assert interrupted_old is not None
    assert interrupted_current is not None
    failed_ids = _failed_action_ids(host_e)
    failed_operations = _failed_operation_ids(host_e)
    failed_reason = "release failed rc=9"
    expected_row = {
        "decision": "update",
        "reason": f"skipped: an earlier lane on host_e failed ({failed_reason})",
        "status": "not-run",
    }
    expected_rows = {action_id: expected_row for action_id in failed_ids}
    assert failed_ids == (
        "helper:host_e:vibeqc-dev",
        "local-runtime:host_e:vibeqc-release",
    )
    assert failed_operations == (
        "2e8b39290030ff1e792c4b980d1ecc3c4410454789901457f78f8e361ce3ee4a",
        "975d6c0364abfee4e975263c75e09df24f161ed9efa5f31f90b98027f3b82871",
    )
    operations_by_action = {
        action_id: tuple(
            str(attempt["operation_id"])
            for attempt in host_e.actions[action_id]["operation_attempts"]
        )
        for action_id in failed_ids
    }
    assert operations_by_action == {
        "helper:host_e:vibeqc-dev": (
            "2e8b39290030ff1e792c4b980d1ecc3c4410454789901457f78f8e361ce3ee4a",
        ),
        "local-runtime:host_e:vibeqc-release": (
            "975d6c0364abfee4e975263c75e09df24f161ed9efa5f31f90b98027f3b82871",
        ),
    }
    released_hold = {
        "host": "host_e",
        "kind": "full",
        "owned": True,
        "status": "released",
        "legacy_reconciliation": {"outcome": "observed-inactive"},
    }
    expected_projection_actions: list[dict[str, Any]] = []
    for planned_action in sorted(
        (item for item in plan.actions if item.host == "host_e"),
        key=lambda item: item.id,
    ):
        expected_before = copy.deepcopy(planned_action.before)
        expected_before["required"] = expected_before.get("required", False)
        expected_projection_actions.append(
            {
                "action_id": planned_action.id,
                "phase": planned_action.phase,
                "host": planned_action.host,
                "program": planned_action.program,
                "decision": planned_action.decision,
                "reason": planned_action.reason,
                "pin_name": planned_action.pin_name,
                "target_sha": planned_action.target_sha,
                "target_version": planned_action.target_version,
                "target_tag": planned_action.target_tag,
                "argv": list(planned_action.argv),
                "before": expected_before,
            }
        )
    expected_projection = {
        "schema": legacy_failure_transition.FAILURE_UPDATE_ACTIONS_SCHEMA,
        "host": "host_e",
        "actions": expected_projection_actions,
    }
    expected_action_hash = legacy_failure_transition.canonical_json_sha256(expected_projection)
    assert expected_action_hash == (
        "8e6502dc9e0af93ca0feefb8fa8eb65ad52ff6fb07e8c74ee3fbd938eab55f8c"
    )
    expected_ack_hash = legacy_failure_transition.canonical_json_sha256(
        {
            "complete": False,
            "failed_hosts": {"host_e": failed_reason},
            "actions": expected_rows,
        }
    )
    assert expected_ack_hash == ("847e284b61c934d3211be061973ac6f2c42086f71b16406a358af428feac1427")
    expected_hold_hash = legacy_failure_transition.canonical_json_sha256(released_hold)
    assert expected_hold_hash == (
        "1bead9c5dec672eeb5b7ce30bd3811ea70af513aa7c38123b4c35bac49cd3173"
    )
    assert (accepted.source_path, accepted.digest_sha256, current_id) == (
        "vibe-queue/releases/v0.15.141.json",
        "f" * 64,
        "v0.15.141-ffffffffffff",
    )
    assert (
        str(plan.report["source_path"]),
        str(plan.report["digest_sha256"]),
        fleet_rollout.rollout_id(accepted),
    ) == (accepted.source_path, accepted.digest_sha256, current_id)
    assert (
        host_e.report_source_path,
        host_e.report_digest_sha256,
        host_e.rollout_id,
    ) == (
        "vibe-queue/releases/v0.15.137.json",
        "b" * 64,
        "v0.15.60-bbbbbbbbbbbb",
    )
    current_intent_payload = json.loads(stage_bytes["current-intent"][0])
    raw_intent = current_intent_payload["legacy_failure_update_intent"]
    parsed_intent = legacy_failure_transition.parse_failure_update_intent(raw_intent)
    expected_intent = {
        "schema": legacy_failure_transition.FAILURE_UPDATE_INTENT_SCHEMA,
        "host": "host_e",
        "current_report_path": accepted.source_path,
        "current_report_digest_sha256": accepted.digest_sha256,
        "current_rollout_id": current_id,
        "current_action_ids": list(failed_ids),
        "current_plan_projection": expected_projection,
        "current_actions_sha256": expected_action_hash,
        "current_ack_sha256": expected_ack_hash,
        "failed_host_reason": failed_reason,
        "sources": [
            {
                "historical_rollout_id": host_e.rollout_id,
                "historical_report_path": host_e.report_source_path,
                "historical_report_digest_sha256": host_e.report_digest_sha256,
                "failed_action_ids": list(failed_ids),
                "failed_operation_ids": list(failed_operations),
                "failure_reason": failed_reason,
                "settled_hold_sha256": expected_hold_hash,
            }
        ],
        "recorded_at": expected_recorded_at,
    }
    assert raw_intent == expected_intent
    assert parsed_intent.as_dict() == expected_intent
    assert len(parsed_intent.sources) == 1
    source_member = parsed_intent.sources[0]
    assert (
        parsed_intent.current_report_path,
        parsed_intent.current_report_digest_sha256,
        parsed_intent.current_rollout_id,
    ) == (accepted.source_path, accepted.digest_sha256, current_id)
    assert (
        source_member.historical_report_path,
        source_member.historical_report_digest_sha256,
        source_member.historical_rollout_id,
    ) == (
        host_e.report_source_path,
        host_e.report_digest_sha256,
        host_e.rollout_id,
    )
    assert parsed_intent.host == "host_e"
    assert parsed_intent.current_action_ids == source_member.failed_action_ids == failed_ids
    assert source_member.failed_operation_ids == failed_operations
    assert source_member.failure_reason == parsed_intent.failed_host_reason == failed_reason
    assert (
        legacy_failure_transition.thaw_json(parsed_intent.current_plan_projection)
        == expected_projection
    )
    assert parsed_intent.current_actions_sha256 == expected_action_hash
    assert parsed_intent.current_ack_sha256 == expected_ack_hash
    assert source_member.settled_hold_sha256 == expected_hold_hash
    expected_current_payload = {
        "rollout_id": current_id,
        "report_digest_sha256": accepted.digest_sha256,
        "report_source_path": accepted.source_path,
        "actions": expected_rows,
        "holds": {},
        "complete": False,
        "failed_hosts": {"host_e": failed_reason},
        "legacy_failure_update_intent": expected_intent,
    }
    expected_current_intent_bytes = (
        json.dumps(expected_current_payload, indent=2, sort_keys=True) + "\n"
    ).encode()
    assert current_intent_payload == expected_current_payload
    assert stage_bytes["current-intent"][0] == expected_current_intent_bytes
    assert interrupted_current.actions == expected_rows
    assert interrupted_current.failed_hosts == {"host_e": failed_reason}
    assert interrupted_current.complete is False
    intent = getattr(
        interrupted_current,
        "legacy_failure_update_intent",
        None,
    )
    backlinks_present = all(
        "legacy_failure_update_ack" in interrupted_old.actions[action_id]
        for action_id in failed_ids
    )
    consumed = all(
        attempt["failure_fence_consumed"] is True
        for action_id in failed_ids
        for attempt in interrupted_old.actions[action_id]["operation_attempts"]
    )
    if "old-backlink" in stage_bytes:
        expected_backlink = legacy_failure_transition.FailureUpdateBacklink(
            schema=legacy_failure_transition.FAILURE_UPDATE_BACKLINK_SCHEMA,
            host="host_e",
            failed_action_ids=source_member.failed_action_ids,
            failed_operation_ids=source_member.failed_operation_ids,
            historical_report_path=source_member.historical_report_path,
            historical_report_digest_sha256=(source_member.historical_report_digest_sha256),
            settled_hold_sha256=(legacy_failure_transition.canonical_json_sha256(released_hold)),
            current_report_path=parsed_intent.current_report_path,
            current_report_digest_sha256=(parsed_intent.current_report_digest_sha256),
            current_rollout_id=parsed_intent.current_rollout_id,
            current_action_ids=parsed_intent.current_action_ids,
            current_actions_sha256=parsed_intent.current_actions_sha256,
            recorded_at=expected_recorded_at,
        ).as_dict()
        expected_old_backlink = json.loads(old_before)
        expected_old_backlink["holds"]["host_e"] = released_hold
        expected_old_backlink.pop("legacy_retained_holds")
        expected_old_backlink["complete"] = False
        for action_id in failed_ids:
            expected_row_payload = expected_old_backlink["actions"][action_id]
            expected_row_payload.pop("legacy_failure_skip", None)
            expected_row_payload["legacy_failure_update_ack"] = expected_backlink
            assert all(
                attempt["failure_fence_consumed"] is False
                and attempt["failure_retry_authorized"] is False
                for attempt in expected_row_payload["operation_attempts"]
            )
        expected_old_backlink_bytes = (
            json.dumps(expected_old_backlink, indent=2, sort_keys=True) + "\n"
        ).encode()
        assert stage_bytes["old-backlink"][1] == expected_old_backlink_bytes
        assert stage_bytes["old-backlink"][0] == expected_current_intent_bytes
    if crash_phase == "current-intent":
        assert intent is not None
        assert backlinks_present is False
        assert consumed is False
        assert old_path.read_bytes() == old_before
    elif crash_phase == "old-backlink":
        assert intent is not None
        assert backlinks_present is True
        assert consumed is False
        assert interrupted_old.holds["host_e"] == {
            "host": "host_e",
            "kind": "full",
            "owned": True,
            "status": "released",
            "legacy_reconciliation": {"outcome": "observed-inactive"},
        }
        assert "host_e" not in interrupted_old.legacy_retained_holds
        for action_id in failed_ids:
            legacy_failure_transition.parse_failure_update_backlink(
                interrupted_old.actions[action_id]["legacy_failure_update_ack"]
            )
    elif crash_phase == "old-consume":
        assert intent is not None
        assert backlinks_present is True
        assert consumed is True
        before_consume = json.loads(stage_bytes["old-backlink"][1])
        after_consume = json.loads(stage_bytes["old-consume"][1])
        for action_id in failed_ids:
            before_attempts = before_consume["actions"][action_id]["operation_attempts"]
            after_attempts = after_consume["actions"][action_id]["operation_attempts"]
            assert all(
                attempt["failure_fence_consumed"] is False
                and attempt["failure_retry_authorized"] is False
                for attempt in before_attempts
            )
            assert all(
                attempt["failure_fence_consumed"] is True
                and attempt["failure_retry_authorized"] is False
                for attempt in after_attempts
            )
            for attempt in after_attempts:
                attempt["failure_fence_consumed"] = False
        assert after_consume == before_consume
        assert stage_bytes["old-consume"][0] == expected_current_intent_bytes
    else:
        assert intent is None
        assert "legacy_failure_update_intent" not in interrupted_current.as_dict()
        assert backlinks_present is True
        assert consumed is True
        assert stage_bytes["current-clear"][1] == stage_bytes["old-consume"][1]
        before_clear = json.loads(stage_bytes["old-consume"][0])
        after_clear = json.loads(stage_bytes["current-clear"][0])
        assert before_clear.pop("legacy_failure_update_intent") is not None
        assert after_clear == before_clear
    success_attempt = interrupted_old.actions["local-runtime:host_e:unrelated-success"][
        "operation_attempts"
    ][0]
    assert success_attempt["failure_fence_consumed"] is False
    assert success_attempt["failure_retry_authorized"] is False

    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)
    advanced: fleet_release.FleetReleaseReport | None = None
    if crash_phase in {"current-intent", "old-backlink"}:
        advanced = fleet_release.FleetReleaseReport(
            source_ref=accepted.source_ref,
            source_path="vibe-queue/releases/v0.15.142.json",
            digest_sha256="e" * 64,
            generated_at=accepted.generated_at,
            release_version=(0, 15, 142),
            pins=accepted.pins,
            raw={},
        )
        plan.report = fleet_release.report_summary(advanced)
        accepted = advanced
        original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
        original_input_init = legacy_failure_transition.HostRecoveryInput.__post_init__
        receipt_authentications: set[tuple[str, str]] = set()

        def historical(
            source_path: str,
            digest: str,
            repo: Path,
            *,
            fetch: bool,
        ) -> fleet_release.FleetReleaseReport:
            report = original_historical(source_path, digest, repo, fetch=fetch)
            if digest == receipt_report.digest_sha256:
                assert source_path == receipt_report.source_path
                assert report is receipt_report
                receipt_authentications.add((source_path, digest))
            return report

        def input_init(
            recovery: legacy_failure_transition.HostRecoveryInput,
        ) -> None:
            if recovery.current_snapshot is not None:
                assert receipt_authentications == {
                    (receipt_report.source_path, receipt_report.digest_sha256)
                }
                assert recovery.latest_report.ref.digest_sha256 == advanced.digest_sha256
                assert recovery.receipt_report is not None
                assert recovery.receipt_report.ref.digest_sha256 == receipt_report.digest_sha256
            original_input_init(recovery)
            receipt_authentications.clear()

        monkeypatch.setattr(
            fleet_rollout.fleet_release,
            "discover_historical_report_by_digest",
            historical,
        )
        monkeypatch.setattr(
            legacy_failure_transition.HostRecoveryInput,
            "__post_init__",
            input_init,
        )
    _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    old = fleet_rollout.load_run(host_e.rollout_id)
    assert old is not None
    assert old.actions[action.id]["operation_attempts"][0]["failure_fence_consumed"] is True
    receipt_current = fleet_rollout.load_run(current_id)
    assert receipt_current is not None
    assert getattr(receipt_current, "legacy_failure_update_intent", None) is None
    _assert_exact_failure_receipt(
        source=old,
        current=receipt_current,
        accepted=receipt_report,
        reason="release failed rc=9",
    )
    if advanced is not None:
        assert fleet_rollout.load_run(fleet_rollout.rollout_id(advanced)) is None


def test_failure_transition_v2_mixed_hosts_preserves_offline_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path, monkeypatch
    )
    _forbid_failure_transition_execution(monkeypatch)
    host_a_report, host_a = _obsolete_legacy_full_hold(
        host="host_a", version=(0, 15, 118), digest="a" * 64
    )
    fleet_rollout.save_run(host_a)
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: (
            host_a_report
            if digest == host_a_report.digest_sha256
            else original_historical(source_path, digest, repo, fetch=fetch)
        ),
    )
    controls: list[list[str]] = []
    control = _inactive_failure_status_runner(
        hosts=("host_a", "host_e"),
        offline=("host_a",),
        calls=controls,
    )

    result = _call_failure_transition_v2(
        accepted_report=accepted,
        **{
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=control,
            ),
            "legacy_hold_retries": (
                (host_a.rollout_id, "host_a"),
                (host_e.rollout_id, "host_e"),
            ),
        },
    )
    persisted_host_a = fleet_rollout.load_run(host_a.rollout_id)
    persisted_host_e = fleet_rollout.load_run(host_e.rollout_id)
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert persisted_host_a is not None
    assert persisted_host_e is not None
    assert current is not None
    assert persisted_host_a.holds["host_a"]["status"] == "active"
    assert (
        persisted_host_e.actions[action.id]["operation_attempts"][0]["failure_fence_consumed"]
        is True
    )
    assert result.retained_holds[0][:2] == (host_a.rollout_id, "host_a")
    assert set(current.actions) == set(_failed_action_ids(host_e))
    assert all("host_a" not in action_id for action_id in current.actions)
    assert current.failed_hosts == {"host_e": "release failed rc=9"}
    assert all("--release" not in argv and "update" not in argv for argv in controls)


def test_failure_transition_v2_groups_two_old_sources_for_one_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e_a, plan, action, _observation_a, accepted = _isolated_failed_host_e_recovery(
        tmp_path, monkeypatch
    )
    _forbid_failure_transition_execution(monkeypatch)
    existing_operations = fleet_rollout.fleet_operation.list_operation_ids(recover=True)
    observations = {
        operation_id: fleet_rollout.fleet_operation.observe_operation(
            operation_id,
            recover=True,
        )
        for operation_id in existing_operations
    }
    report_a, _unused = _obsolete_legacy_full_hold(
        host="host_e", version=(0, 15, 137), digest="b" * 64
    )
    report_b, host_e_b = _obsolete_legacy_full_hold(
        host="host_e", version=(0, 15, 136), digest="d" * 64
    )
    host_e_b.actions = {}
    for index, current_action in enumerate(item for item in plan.actions if item.host == "host_e"):
        historical_action = copy.deepcopy(current_action)
        historical_pin = report_b.pins[current_action.pin_name]
        historical_action.target_sha = historical_pin.sha
        historical_action.target_version = historical_pin.version
        historical_action.target_tag = historical_pin.tag
        identity_b = fleet_rollout._operation_identity(
            host_e_b.rollout_id,
            host_e_b.report_digest_sha256,
            historical_action,
            attempt=1,
        )
        observation_b = fleet_operation.OperationObservation(
            operation_id=fleet_operation.operation_id(identity_b),
            identity=identity_b,
            request_sha256=f"{index + 6:x}" * 64,
            state="completed",
            retry_safe=False,
            lease_busy=False,
            request={},
            ready={},
            authorization={},
            activation={},
            result={"status": "failed", "executed": True, "returncode": 5},
        )
        observations[observation_b.operation_id] = observation_b
        host_e_b.actions[current_action.id] = {
            "status": "failed",
            "operation_attempts": [
                fleet_rollout._attempt_ref(
                    observation_b,
                    identity_b,
                    request_sha256=observation_b.request_sha256,
                    harvested=True,
                )
            ],
        }
    host_e_b.holds["host_e"].update(
        {
            "duration_seconds": 21600,
            "set_at": "2026-08-08T12:00:00+00:00",
            "control_host": "host_e",
        }
    )
    host_e_b.complete = False
    fleet_rollout.save_run(host_e_b)
    reports = {
        report_a.digest_sha256: report_a,
        report_b.digest_sha256: report_b,
        accepted.digest_sha256: accepted,
    }
    expected_history_refs = {
        (report.source_path, report.digest_sha256) for report in reports.values()
    }
    expected_source_refs = {
        (host_e_a.rollout_id, "host_e"),
        (host_e_b.rollout_id, "host_e"),
    }
    expected_failed_operations = {
        *_failed_operation_ids(host_e_a),
        *_failed_operation_ids(host_e_b),
    }
    authenticated_history_refs: set[tuple[str, str]] = set()
    authenticated_journal_refs: set[tuple[str, str]] = set()
    authenticated_hold_refs: set[tuple[str, str]] = set()
    authenticated_failed_operations: set[str] = set()
    authenticated_status_hosts: set[str] = set()
    input_epochs = 0
    current_id = fleet_rollout.rollout_id(accepted)
    old_a, old_b = sorted(
        (host_e_a, host_e_b),
        key=lambda run: run.rollout_id,
    )
    required_vector_ids = {
        current_id,
        old_a.rollout_id,
        old_b.rollout_id,
    }
    expected_steps = (
        ("WriteCurrentAckAndIntent", current_id),
        ("WriteOldBacklink", old_a.rollout_id),
        ("WriteOldBacklink", old_b.rollout_id),
        ("ConsumeMember", old_a.rollout_id),
        ("ConsumeMember", old_b.rollout_id),
        ("ClearForwardIntent", current_id),
    )
    armed = False
    pending_input_vector: legacy_failure_transition.MutationVector | None = None
    active_guard_vector: legacy_failure_transition.MutationVector | None = None
    input_vectors: list[dict[str, str | None]] = []
    original_input_init = legacy_failure_transition.HostRecoveryInput.__post_init__
    original_load = fleet_rollout.load_run

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        del repo, fetch
        assert armed is False
        assert pending_input_vector is None
        assert active_guard_vector is None
        report = reports[digest]
        assert source_path == report.source_path
        authenticated_history_refs.add((source_path, digest))
        return report

    def observe(
        operation: str,
        *,
        recover: bool = True,
    ) -> fleet_operation.OperationObservation:
        del recover
        assert armed is False
        assert pending_input_vector is None
        assert active_guard_vector is None
        if operation in expected_failed_operations:
            authenticated_failed_operations.add(operation)
        return observations[operation]

    def load(rollout_id_value: str) -> fleet_rollout.RolloutRun | None:
        assert armed is False
        run = original_load(rollout_id_value)
        if pending_input_vector is None and active_guard_vector is None:
            for source_ref in expected_source_refs:
                if run is not None and source_ref[0] == rollout_id_value:
                    authenticated_journal_refs.add(source_ref)
                    hold = run.holds[source_ref[1]]
                    assert hold["host"] == source_ref[1]
                    assert hold["kind"] == "full"
                    assert hold["owned"] is True
                    authenticated_hold_refs.add(source_ref)
        return run

    def input_init(
        recovery: legacy_failure_transition.HostRecoveryInput,
    ) -> None:
        nonlocal input_epochs, pending_input_vector
        assert armed is False
        assert pending_input_vector is None
        assert authenticated_history_refs == expected_history_refs
        assert authenticated_journal_refs == expected_source_refs
        assert authenticated_hold_refs == expected_source_refs
        assert authenticated_failed_operations == expected_failed_operations
        assert authenticated_status_hosts == {"host_e"}
        assert {group.report.rollout_id for group in recovery.failures} == {
            host_e_a.rollout_id,
            host_e_b.rollout_id,
        }
        assert {
            token.rollout_id for token in recovery.mutation_vector.journals
        } == required_vector_ids
        original_input_init(recovery)
        input_epochs += 1
        pending_input_vector = recovery.mutation_vector
        input_vectors.append(
            {token.rollout_id: token.digest_sha256 for token in recovery.mutation_vector.journals}
        )
        authenticated_history_refs.clear()
        authenticated_journal_refs.clear()
        authenticated_hold_refs.clear()
        authenticated_failed_operations.clear()
        authenticated_status_hosts.clear()

    strict_group_control = _inactive_failure_status_runner()

    def group_control(
        argv: list[str],
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        assert armed is False
        assert (
            pending_input_vector is None
            or active_guard_vector is pending_input_vector
        )
        authenticated_status_hosts.add(argv[-1])
        return strict_group_control(argv, **kwargs)

    def list_group_operations(*, recover: bool) -> list[str]:
        del recover
        assert armed is False
        assert pending_input_vector is None
        assert active_guard_vector is None
        return sorted(observations)

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        list_group_operations,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        observe,
    )
    monkeypatch.setattr(fleet_rollout, "load_run", load)
    monkeypatch.setattr(
        legacy_failure_transition.HostRecoveryInput,
        "__post_init__",
        input_init,
    )
    reasons = {
        host_e_a.rollout_id: "alpha source failed rc=9",
        host_e_b.rollout_id: "zeta source failed rc=5",
    }
    expected_reason = reasons[sorted(reasons)[-1]]
    original_guarded = getattr(fleet_rollout, "_guarded_save_run", None)
    assert original_guarded is not None, "Phase2 requires one guarded journal CAS seam"
    original_authorize = legacy_failure_transition.HostTransition.authorized_effects
    original_read_bytes = Path.read_bytes
    original_read_text = Path.read_text
    original_is_file = Path.is_file
    original_atomic_write = fleet_rollout.paths.atomic_write_text
    checked_rollouts: set[str] = set()
    checked_cas_tokens: dict[str, str | None] = {}
    checked_vector_phases: list[frozenset[str]] = []
    checked_token_phases: list[dict[str, str | None]] = []
    effect_names: list[str] = []
    effect_targets: list[str] = []
    authorized_effects: list[legacy_failure_transition.RecoveryEffect] = []
    write_targets: list[str] = []
    phase_write_counts: list[int] = []
    events: list[tuple[str, str]] = []
    expected_write_path: Path | None = None
    actual_write_path: Path | None = None
    actual_write_text: str | None = None
    current_phase_writes = 0
    interrupted = False

    def note_state_read(path: Path) -> str | None:
        if active_guard_vector is None:
            return None
        assert armed is False, "journal read occurred after authorization"
        if path.parent != fleet_rollout.rollout_state_path(current_id).parent:
            return None
        for rollout_id_value in required_vector_ids:
            if path == fleet_rollout.rollout_state_path(rollout_id_value):
                checked_rollouts.add(rollout_id_value)
                events.append(("cas-read", rollout_id_value))
                return rollout_id_value
        return None

    def read_bytes(path: Path) -> bytes:
        rollout_id_value = note_state_read(path)
        try:
            payload = original_read_bytes(path)
        except FileNotFoundError:
            if rollout_id_value is not None:
                checked_cas_tokens[rollout_id_value] = None
            raise
        if rollout_id_value is not None:
            checked_cas_tokens[rollout_id_value] = legacy_failure_transition.canonical_json_sha256(
                json.loads(payload)
            )
        return payload

    def read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        rollout_id_value = note_state_read(path)
        try:
            payload = original_read_text(path, *args, **kwargs)
        except FileNotFoundError:
            if rollout_id_value is not None:
                checked_cas_tokens[rollout_id_value] = None
            raise
        if rollout_id_value is not None:
            checked_cas_tokens[rollout_id_value] = legacy_failure_transition.canonical_json_sha256(
                json.loads(payload)
            )
        return payload

    def is_file(path: Path) -> bool:
        rollout_id_value = note_state_read(path)
        present = original_is_file(path)
        if rollout_id_value is not None and not present:
            checked_cas_tokens[rollout_id_value] = None
        return present

    def authorize(
        planned: legacy_failure_transition.HostTransition,
    ) -> tuple[legacy_failure_transition.RecoveryEffect, ...]:
        nonlocal armed, expected_write_path, pending_input_vector
        assert armed is False
        assert active_guard_vector is not None
        assert pending_input_vector is active_guard_vector
        assert checked_rollouts == required_vector_ids
        expected_tokens = {
            token.rollout_id: token.digest_sha256 for token in active_guard_vector.journals
        }
        assert checked_cas_tokens == expected_tokens
        assert events[-1][0] == "cas-read"
        effects = original_authorize(planned)
        assert len(effects) == 1
        effect = effects[0]
        assert not isinstance(effect, legacy_failure_transition.Reject)
        assert effect.expected_vector is active_guard_vector
        assert {
            token.rollout_id for token in effect.expected_vector.journals
        } == required_vector_ids
        if isinstance(
            effect,
            legacy_failure_transition.WriteCurrentAckAndIntent
            | legacy_failure_transition.ClearForwardIntent,
        ):
            target = effect.report.rollout_id
        else:
            assert isinstance(
                effect,
                legacy_failure_transition.WriteOldBacklink
                | legacy_failure_transition.ConsumeMember,
            )
            target = effect.old_report.rollout_id
        checked_vector_phases.append(frozenset(checked_rollouts))
        checked_token_phases.append(dict(checked_cas_tokens))
        effect_names.append(type(effect).__name__)
        effect_targets.append(target)
        authorized_effects.append(effect)
        assert (type(effect).__name__, target) == expected_steps[len(effect_names) - 1]
        expected_write_path = fleet_rollout.rollout_state_path(target)
        events.append(("authorize", type(effect).__name__))
        pending_input_vector = None
        armed = True
        return effects

    def atomic_write(path: Path, text: str) -> None:
        nonlocal armed, current_phase_writes, actual_write_path, actual_write_text
        assert armed is True, "grouped journal write lacked fresh authorization"
        assert expected_write_path is not None
        assert path == expected_write_path
        assert events[-1][0] == "authorize"
        current_phase_writes += 1
        assert current_phase_writes == 1
        actual_write_path = path
        actual_write_text = text
        original_atomic_write(path, text)
        write_targets.append(effect_targets[-1])
        events.append(("write", effect_targets[-1]))
        armed = False

    def guarded_save(*args: Any, **kwargs: Any) -> Any:
        nonlocal active_guard_vector, current_phase_writes
        nonlocal expected_write_path, actual_write_path, actual_write_text, interrupted
        assert armed is False
        assert pending_input_vector is not None
        expected_vector = kwargs["expected_vector"]
        assert expected_vector is pending_input_vector
        _expected_name, expected_target = expected_steps[len(effect_names)]
        target_rollout_id = args[0]
        assert target_rollout_id == expected_target
        expected_token = expected_vector.token_for(target_rollout_id)
        assert expected_token.rollout_id == target_rollout_id
        checked_rollouts.clear()
        checked_cas_tokens.clear()
        current_phase_writes = 0
        expected_write_path = None
        actual_write_path = None
        actual_write_text = None
        active_guard_vector = pending_input_vector
        candidate, effect = original_guarded(*args, **kwargs)
        assert armed is False
        assert current_phase_writes == 1
        assert candidate.rollout_id == target_rollout_id
        assert effect is authorized_effects[-1]
        assert expected_token.rollout_id == effect_targets[-1]
        assert actual_write_path == fleet_rollout.rollout_state_path(target_rollout_id)
        assert actual_write_text == (
            json.dumps(candidate.as_dict(), indent=2, sort_keys=True) + "\n"
        )
        phase_write_counts.append(current_phase_writes)
        active_guard_vector = None
        payload = candidate.as_dict()
        if payload.get("legacy_failure_update_intent") and not interrupted:
            interrupted = True
            raise RuntimeError("crash after grouped intent")
        return candidate, effect

    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "is_file", is_file)
    monkeypatch.setattr(
        legacy_failure_transition.HostTransition,
        "authorized_effects",
        authorize,
    )
    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", atomic_write)
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", guarded_save)
    kwargs = {
        **_failure_transition_kwargs(
            host_e=host_e_a,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=group_control,
        ),
        "legacy_hold_retries": (
            (host_e_a.rollout_id, "host_e"),
            (host_e_b.rollout_id, "host_e"),
        ),
        "failed_operation_hosts": tuple(
            (rollout_id_value, "host_e", reason) for rollout_id_value, reason in reasons.items()
        ),
    }
    with pytest.raises(RuntimeError, match="grouped intent"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert interrupted is True
    interrupted_current = fleet_rollout.load_run(current_id)
    assert interrupted_current is not None
    raw_intent = interrupted_current.legacy_failure_update_intent
    parsed_intent = legacy_failure_transition.parse_failure_update_intent(raw_intent)
    assert tuple(member.historical_rollout_id for member in parsed_intent.sources) == tuple(
        sorted(reasons)
    )
    assert parsed_intent.failed_host_reason == expected_reason

    authenticated_history_refs.clear()
    authenticated_journal_refs.clear()
    authenticated_hold_refs.clear()
    authenticated_failed_operations.clear()
    authenticated_status_hosts.clear()
    _call_failure_transition_v2(
        accepted_report=accepted,
        **kwargs,
    )
    backlinks: list[dict[str, Any]] = []
    for source in (host_e_a, host_e_b):
        persisted = fleet_rollout.load_run(source.rollout_id)
        assert persisted is not None
        for action_id in _failed_action_ids(source):
            row = persisted.actions[action_id]
            assert row["operation_attempts"][0]["failure_fence_consumed"] is True
            parsed = legacy_failure_transition.parse_failure_update_backlink(
                row["legacy_failure_update_ack"]
            )
            assert parsed.historical_report_path == source.report_source_path
            assert parsed.historical_report_digest_sha256 == source.report_digest_sha256
            assert parsed.failed_action_ids == _failed_action_ids(source)
            assert parsed.failed_operation_ids == _failed_operation_ids(source)
            assert parsed.settled_hold_sha256 == (
                legacy_failure_transition.canonical_json_sha256(persisted.holds["host_e"])
            )
            backlinks.append(row["legacy_failure_update_ack"])
    common = (
        "current_report_path",
        "current_report_digest_sha256",
        "current_rollout_id",
        "current_action_ids",
        "current_actions_sha256",
        "recorded_at",
    )
    assert {
        json.dumps(
            {key: backlink[key] for key in common},
            separators=(",", ":"),
            sort_keys=True,
        )
        for backlink in backlinks
    } == {
        json.dumps(
            {key: backlinks[0][key] for key in common},
            separators=(",", ":"),
            sort_keys=True,
        )
    }
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    expected_effect_names = [
        "WriteCurrentAckAndIntent",
        "WriteOldBacklink",
        "WriteOldBacklink",
        "ConsumeMember",
        "ConsumeMember",
        "ClearForwardIntent",
    ]
    expected_targets = [
        current_id,
        old_a.rollout_id,
        old_b.rollout_id,
        old_a.rollout_id,
        old_b.rollout_id,
        current_id,
    ]
    assert input_epochs == 6
    assert effect_names == expected_effect_names
    assert effect_targets == expected_targets
    assert write_targets == expected_targets
    assert list(zip(effect_names, effect_targets, strict=True)) == list(expected_steps)
    assert checked_token_phases == input_vectors
    assert checked_vector_phases == [frozenset(required_vector_ids)] * 6
    assert phase_write_counts == [1, 1, 1, 1, 1, 1]
    for index, event in enumerate(events):
        if event[0] == "authorize":
            assert events[index + 1][0] == "write"
    for index in range(1, len(input_vectors)):
        changed = {
            rollout_id_value
            for rollout_id_value in required_vector_ids
            if input_vectors[index][rollout_id_value] != input_vectors[index - 1][rollout_id_value]
        }
        assert changed == {expected_targets[index - 1]}
    assert current.failed_hosts == {"host_e": expected_reason}
    assert set(current.actions) == set(_failed_action_ids(host_e_a))


@pytest.mark.parametrize("mutation_callback", ("resolver", "historical", "control"))
def test_failure_transition_v2_rejects_callback_cas_drift(
    mutation_callback: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
    inactive_control = _inactive_failure_status_runner()
    mutated = False

    def mutate() -> None:
        nonlocal mutated
        if mutated:
            return
        concurrent = fleet_rollout.load_run(host_e.rollout_id)
        assert concurrent is not None
        concurrent.failed_hosts["concurrent"] = "callback mutation"
        fleet_rollout.save_run(concurrent)
        mutated = True

    resolver_calls = 0

    def resolver() -> str:
        nonlocal resolver_calls
        resolver_calls += 1
        if mutation_callback == "resolver" and resolver_calls == 2:
            mutate()
        return str(plan.report["digest_sha256"])

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        report = original_historical(source_path, digest, repo, fetch=fetch)
        if mutation_callback == "historical":
            mutate()
        return report

    def control(argv: list[str], **unused: Any) -> subprocess.CompletedProcess[str]:
        if mutation_callback == "control":
            mutate()
        return inactive_control(argv, **unused)

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=control,
    )
    kwargs["current_report_digest_resolver"] = resolver
    with pytest.raises(fleet_rollout.FleetRolloutError, match="changed|stale|CAS"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert mutated is True
    assert fleet_rollout.load_run(fleet_rollout.rollout_id(accepted)) is None
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    assert persisted.failed_hosts["concurrent"] == "callback mutation"
    row = persisted.actions[action.id]
    assert "legacy_failure_update_ack" not in row
    assert row["operation_attempts"][0].get("failure_fence_consumed") is not True


@pytest.mark.parametrize("race", ("missing-current-create", "peer-token"))
def test_failure_transition_v2_guarded_cas_rejects_concurrent_create_or_peer(
    race: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path, monkeypatch
    )
    _forbid_failure_transition_execution(monkeypatch)
    peer_report, peer = _obsolete_legacy_full_hold(
        host="host_a", version=(0, 15, 118), digest="a" * 64
    )
    fleet_rollout.save_run(peer)
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: (
            peer_report
            if digest == peer_report.digest_sha256
            else original_historical(source_path, digest, repo, fetch=fetch)
        ),
    )
    guarded = getattr(fleet_rollout, "_guarded_save_run", None)
    assert guarded is not None, "Phase2 requires one guarded journal CAS seam"
    current_id = fleet_rollout.rollout_id(accepted)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    old_before = old_path.read_bytes()
    peer_path = fleet_rollout.rollout_state_path(peer.rollout_id)
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    raced_bytes: bytes | None = None
    current_before_race: bytes | None = None
    adapter_writes: list[Path] = []
    adapter_write_texts: list[str] = []
    authorized_effects: list[legacy_failure_transition.RecoveryEffect] = []
    invoked = False
    guard_calls = 0
    original_authorize = legacy_failure_transition.HostTransition.authorized_effects

    def fixture_write(run: fleet_rollout.RolloutRun) -> None:
        fixture_atomic_write(
            fleet_rollout.rollout_state_path(run.rollout_id),
            json.dumps(run.as_dict(), indent=2, sort_keys=True) + "\n",
        )

    def counted_adapter_write(path: Path, text: str) -> None:
        adapter_writes.append(path)
        adapter_write_texts.append(text)
        fixture_atomic_write(path, text)

    def authorize(
        transition: legacy_failure_transition.HostTransition,
    ) -> tuple[legacy_failure_transition.RecoveryEffect, ...]:
        effects = original_authorize(transition)
        authorized_effects.extend(effects)
        return effects

    def racing_guard(*args: Any, **kwargs: Any) -> Any:
        nonlocal current_before_race, guard_calls, invoked, raced_bytes
        guard_calls += 1
        trigger_call = 1 if race == "missing-current-create" else 2
        rejected_write_count: int | None = None
        target_rollout_id = args[0]
        expected_vector = kwargs["expected_vector"]
        expected_token = expected_vector.token_for(target_rollout_id)
        assert isinstance(
            expected_vector,
            legacy_failure_transition.MutationVector,
        )
        assert isinstance(
            expected_token,
            legacy_failure_transition.JournalToken,
        )
        if guard_calls == trigger_call:
            invoked = True
            rejected_write_count = len(adapter_writes)
            assert {token.rollout_id for token in expected_vector.journals} == {
                current_id,
                host_e.rollout_id,
                peer.rollout_id,
            }
            if race == "missing-current-create":
                assert expected_token.rollout_id == current_id
                assert expected_token.digest_sha256 is None
                competing = fleet_rollout.RolloutRun(
                    rollout_id=current_id,
                    report_digest_sha256=accepted.digest_sha256,
                    report_source_path=accepted.source_path,
                    actions={"foreign": {"status": "not-run"}},
                    failed_hosts={"foreign": "concurrent create"},
                    complete=False,
                )
                fixture_write(competing)
                raced_bytes = fleet_rollout.rollout_state_path(current_id).read_bytes()
            else:
                assert expected_token.rollout_id == host_e.rollout_id
                assert expected_token.digest_sha256 is not None
                current_before_race = fleet_rollout.rollout_state_path(current_id).read_bytes()
                concurrent_peer = fleet_rollout.load_run(peer.rollout_id)
                assert concurrent_peer is not None
                concurrent_peer.failed_hosts["host_a"] = "peer changed"
                fixture_write(concurrent_peer)
                raced_bytes = peer_path.read_bytes()
        try:
            candidate, effect = guarded(*args, **kwargs)
            assert candidate.rollout_id == target_rollout_id
            assert effect is authorized_effects[-1]
            assert adapter_writes[-1] == fleet_rollout.rollout_state_path(target_rollout_id)
            assert adapter_write_texts[-1] == (
                json.dumps(candidate.as_dict(), indent=2, sort_keys=True) + "\n"
            )
            return candidate, effect
        finally:
            if rejected_write_count is not None:
                assert len(adapter_writes) == rejected_write_count

    monkeypatch.setattr(
        fleet_rollout.paths,
        "atomic_write_text",
        counted_adapter_write,
    )
    monkeypatch.setattr(
        legacy_failure_transition.HostTransition,
        "authorized_effects",
        authorize,
    )
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", racing_guard)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="changed|stale|CAS"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **{
                **_failure_transition_kwargs(
                    host_e=host_e,
                    plan=plan,
                    tmp_path=tmp_path,
                    control_runner=_inactive_failure_status_runner(
                        hosts=("host_a", "host_e"),
                        offline=("host_a",),
                    ),
                ),
                "legacy_hold_retries": (
                    (peer.rollout_id, "host_a"),
                    (host_e.rollout_id, "host_e"),
                ),
            },
        )
    assert invoked is True
    assert old_path.read_bytes() == old_before
    persisted_old = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted_old is not None
    for action_id in _failed_action_ids(persisted_old):
        row = persisted_old.actions[action_id]
        assert "legacy_failure_update_ack" not in row
        assert row["operation_attempts"][0]["failure_fence_consumed"] is False
    if race == "missing-current-create":
        assert adapter_writes == []
        assert fleet_rollout.rollout_state_path(current_id).read_bytes() == raced_bytes
    else:
        assert adapter_writes == [fleet_rollout.rollout_state_path(current_id)]
        assert peer_path.read_bytes() == raced_bytes
        assert current_before_race is not None
        assert fleet_rollout.rollout_state_path(current_id).read_bytes() == current_before_race
        persisted_current = fleet_rollout.load_run(current_id)
        assert persisted_current is not None
        assert persisted_current.legacy_failure_update_intent is not None


@pytest.mark.parametrize(
    "tamper",
    (
        "foreign-host",
        "not-full",
        "not-owned",
        "bad-status",
        "wrong-reason",
        "missing-reason",
        "wrong-control",
        "missing-control",
        "bad-duration",
        "short-duration",
        "missing-duration",
        "wrong-set-at",
        "missing-set-at",
        "preexisting",
        "extra",
    ),
)
def test_failure_transition_v2_rejects_foreign_hold_before_control_or_save(
    tamper: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    if tamper == "foreign-host":
        host_e.holds["host_e"]["host"] = "host_a"
    elif tamper == "not-full":
        host_e.holds["host_e"]["kind"] = "scheduler-target"
    elif tamper == "not-owned":
        host_e.holds["host_e"]["owned"] = False
    elif tamper == "bad-status":
        host_e.holds["host_e"]["status"] = "released"
    elif tamper == "wrong-reason":
        host_e.holds["host_e"]["reason"] = "other owner"
    elif tamper == "missing-reason":
        host_e.holds["host_e"].pop("reason")
    elif tamper == "wrong-control":
        host_e.holds["host_e"]["control_host"] = "host_a"
    elif tamper == "missing-control":
        host_e.holds["host_e"].pop("control_host")
    elif tamper == "bad-duration":
        host_e.holds["host_e"]["duration_seconds"] = "21600"
    elif tamper == "short-duration":
        host_e.holds["host_e"]["duration_seconds"] = 21599
    elif tamper == "missing-duration":
        host_e.holds["host_e"].pop("duration_seconds")
    elif tamper == "wrong-set-at":
        host_e.holds["host_e"]["set_at"] = 17
    elif tamper == "missing-set-at":
        host_e.holds["host_e"].pop("set_at")
    elif tamper == "preexisting":
        host_e.holds["host_e"]["preexisting"] = True
    else:
        host_e.holds["host_e"]["unexpected"] = True
    host_e.legacy_retained_holds["host_e"]["hold_record_sha256"] = (
        fleet_rollout._legacy_subtree_sha256(host_e.holds["host_e"])
    )
    fleet_rollout.save_run(host_e)
    state_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = state_path.read_bytes()
    writes: list[Path] = []
    controls: list[list[str]] = []
    monkeypatch.setattr(
        fleet_rollout.paths,
        "atomic_write_text",
        lambda path, text: writes.append(path),
    )
    for owner, name in (
        (fleet_rollout, "execute_action"),
        (fleet_rollout, "release_rollout_hold"),
        (fleet_rollout.fleet_operation, "prepare_operation"),
        (fleet_rollout.fleet_operation, "authorize_operation"),
        (fleet_rollout.fleet_operation, "launch_supervisor"),
    ):
        monkeypatch.setattr(
            owner,
            name,
            lambda *unused, _name=name, **kwargs: pytest.fail(
                f"malformed hold reached forbidden {_name} seam"
            ),
        )
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="hold|full|owned|recovery candidate|malformed|ambiguous",
    ):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=lambda argv, **unused: (
                    controls.append(argv)
                    or subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")
                ),
            ),
        )
    assert controls == []
    assert writes == []
    assert state_path.read_bytes() == before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    for action_id in _failed_action_ids(persisted):
        for attempt in persisted.actions[action_id]["operation_attempts"]:
            assert attempt["failure_fence_consumed"] is False
            assert attempt["failure_retry_authorized"] is False


def test_failure_transition_authorization_is_immediately_before_every_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    peer_report, peer = _obsolete_legacy_full_hold(
        host="host_a", version=(0, 15, 118), digest="a" * 64
    )
    fleet_rollout.save_run(peer)
    current_id = fleet_rollout.rollout_id(accepted)
    required_vector_ids = {current_id, host_e.rollout_id, peer.rollout_id}
    original_authorize = legacy_failure_transition.HostTransition.authorized_effects
    original_guarded = getattr(fleet_rollout, "_guarded_save_run", None)
    assert original_guarded is not None, "Phase2 requires one guarded journal CAS seam"
    original_load = fleet_rollout.load_run
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
    original_list = fleet_rollout.fleet_operation.list_operation_ids
    original_observe = fleet_rollout.fleet_operation.observe_operation
    original_input_init = legacy_failure_transition.HostRecoveryInput.__post_init__
    original_read_bytes = Path.read_bytes
    original_read_text = Path.read_text
    original_is_file = Path.is_file
    original_atomic_write = fleet_rollout.paths.atomic_write_text
    armed = False
    events: list[tuple[str, str]] = []
    vectors: list[dict[str, str | None]] = []
    effect_targets: list[str] = []
    authorized_effects: list[legacy_failure_transition.RecoveryEffect] = []
    inputs = 0
    checked_rollouts: set[str] = set()
    checked_vector_phases: list[frozenset[str]] = []
    phase_write_counts: list[int] = []
    expected_write_path: Path | None = None
    actual_write_path: Path | None = None
    actual_write_text: str | None = None
    current_phase_writes = 0
    historical_refs: set[tuple[str, str]] = set()
    journal_refs: set[tuple[str, str]] = set()
    hold_refs: set[tuple[str, str]] = set()
    observed_failed_operations: set[str] = set()
    status_hosts: set[str] = set()
    expected_historical_refs = {
        (accepted.source_path, accepted.digest_sha256),
        (host_e.report_source_path, host_e.report_digest_sha256),
        (peer.report_source_path, peer.report_digest_sha256),
    }
    expected_old_refs = {
        (host_e.rollout_id, "host_e"),
        (peer.rollout_id, "host_a"),
    }
    expected_failed_operations = set(_failed_operation_ids(host_e))

    def callback(kind: str) -> None:
        assert armed is False, f"{kind} callback occurred after authorization"
        events.append(("callback", kind))

    def load(rollout_id_value: str) -> fleet_rollout.RolloutRun | None:
        callback("load")
        run = original_load(rollout_id_value)
        if run is not None:
            for rollout_ref, host in expected_old_refs:
                if rollout_id_value == rollout_ref:
                    journal_refs.add((rollout_ref, host))
                    hold = run.holds[host]
                    assert hold["host"] == host
                    assert hold["kind"] == "full"
                    assert hold["owned"] is True
                    hold_refs.add((rollout_ref, host))
        return run

    def note_state_read(path: Path) -> None:
        if path.parent == fleet_rollout.rollout_state_path(current_id).parent:
            for rollout_id_value in required_vector_ids:
                if path == fleet_rollout.rollout_state_path(rollout_id_value):
                    callback(f"cas-read:{rollout_id_value}")
                    checked_rollouts.add(rollout_id_value)
                    return

    def read_bytes(path: Path) -> bytes:
        note_state_read(path)
        return original_read_bytes(path)

    def read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        note_state_read(path)
        return original_read_text(path, *args, **kwargs)

    def is_file(path: Path) -> bool:
        note_state_read(path)
        return original_is_file(path)

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        callback("historical")
        historical_refs.add((source_path, digest))
        if digest == peer_report.digest_sha256:
            assert source_path == peer_report.source_path
            return peer_report
        return original_historical(source_path, digest, repo, fetch=fetch)

    def list_operations(*, recover: bool) -> list[str]:
        callback("operations-list")
        return original_list(recover=recover)

    def observe(
        operation: str,
        *,
        recover: bool = True,
    ) -> fleet_operation.OperationObservation:
        callback("operation-observe")
        observed = original_observe(operation, recover=recover)
        if operation in expected_failed_operations:
            observed_failed_operations.add(operation)
        return observed

    def input_init(
        recovery: legacy_failure_transition.HostRecoveryInput,
    ) -> None:
        nonlocal inputs
        callback("input")
        assert historical_refs == expected_historical_refs
        assert journal_refs == expected_old_refs
        assert hold_refs == expected_old_refs
        assert observed_failed_operations == expected_failed_operations
        assert status_hosts == {"host_a", "host_e"}
        assert recovery.latest_report.ref.digest_sha256 == accepted.digest_sha256
        assert {group.report.rollout_id for group in recovery.failures} == {host_e.rollout_id}
        assert {item.report.rollout_id for item in recovery.old_snapshots} == {host_e.rollout_id}
        assert all(
            proof.settled_hold is not None
            and proof.settled_hold["host"] == "host_e"
            and proof.settled_hold["kind"] == "full"
            and proof.settled_hold["owned"] is True
            and proof.settled_hold["status"] == "released"
            for proof in recovery.hold_proofs
        )
        assert {
            attempt.operation_id for group in recovery.failures for attempt in group.attempts
        } == set(_failed_operation_ids(host_e))
        assert {
            token.rollout_id for token in recovery.mutation_vector.journals
        } == required_vector_ids
        if recovery.current_snapshot is None:
            assert recovery.receipt_report is None
        else:
            assert recovery.receipt_report is not None
            assert recovery.receipt_report.ref.digest_sha256 == accepted.digest_sha256
        original_input_init(recovery)
        inputs += 1
        historical_refs.clear()
        journal_refs.clear()
        hold_refs.clear()
        observed_failed_operations.clear()
        status_hosts.clear()

    def authorize(
        planned: legacy_failure_transition.HostTransition,
    ) -> tuple[legacy_failure_transition.RecoveryEffect, ...]:
        nonlocal armed, expected_write_path
        assert armed is False
        assert checked_rollouts == required_vector_ids
        assert events[-1][0] == "callback"
        assert events[-1][1].startswith("cas-read:")
        effects = original_authorize(planned)
        assert len(effects) == 1
        effect = effects[0]
        if isinstance(
            effect,
            legacy_failure_transition.WriteCurrentAckAndIntent
            | legacy_failure_transition.ClearForwardIntent,
        ):
            target = effect.report.rollout_id
        else:
            assert isinstance(
                effect,
                legacy_failure_transition.WriteOldBacklink
                | legacy_failure_transition.ConsumeMember,
            )
            target = effect.old_report.rollout_id
        vector = {
            token.rollout_id: token.digest_sha256 for token in effect.expected_vector.journals
        }
        assert set(vector) == required_vector_ids
        vectors.append(vector)
        effect_targets.append(target)
        authorized_effects.append(effect)
        checked_vector_phases.append(frozenset(checked_rollouts))
        expected_write_path = fleet_rollout.rollout_state_path(target)
        events.append(("authorize", type(effect).__name__))
        armed = True
        return effects

    def guarded_save(*args: Any, **kwargs: Any) -> Any:
        nonlocal current_phase_writes, expected_write_path
        nonlocal actual_write_path, actual_write_text
        assert armed is False
        target_rollout_id = args[0]
        expected_vector = kwargs["expected_vector"]
        expected_token = expected_vector.token_for(target_rollout_id)
        assert expected_token.rollout_id == target_rollout_id
        checked_rollouts.clear()
        expected_write_path = None
        actual_write_path = None
        actual_write_text = None
        current_phase_writes = 0
        candidate, effect = original_guarded(*args, **kwargs)
        assert armed is False
        assert current_phase_writes == 1
        assert candidate.rollout_id == target_rollout_id
        assert effect is authorized_effects[-1]
        assert target_rollout_id == effect_targets[-1]
        assert actual_write_path == fleet_rollout.rollout_state_path(target_rollout_id)
        assert actual_write_text == (
            json.dumps(candidate.as_dict(), indent=2, sort_keys=True) + "\n"
        )
        phase_write_counts.append(current_phase_writes)
        return candidate, effect

    def atomic_write(path: Path, text: str) -> None:
        nonlocal armed, current_phase_writes, actual_write_path, actual_write_text
        assert armed is True, "journal write occurred without fresh authorization"
        assert expected_write_path is not None
        assert path == expected_write_path
        assert events[-1][0] == "authorize"
        current_phase_writes += 1
        assert current_phase_writes == 1
        actual_write_path = path
        actual_write_text = text
        original_atomic_write(path, text)
        events.append(("write", str(path)))
        armed = False

    def control(argv: list[str], **unused: Any) -> subprocess.CompletedProcess[str]:
        callback("control")
        host = argv[-1]
        assert argv[3:] == ["drain", "--status", "--json", host]
        status_hosts.add(host)
        if host == "host_e":
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(_inactive_full_drain_status()),
                stderr="",
            )
        assert host == "host_a"
        return subprocess.CompletedProcess(
            argv,
            255,
            stdout="",
            stderr="network unreachable",
        )

    def resolver() -> str:
        callback("resolver")
        return str(plan.report["digest_sha256"])

    monkeypatch.setattr(fleet_rollout, "load_run", load)
    monkeypatch.setattr(Path, "read_bytes", read_bytes)
    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(Path, "is_file", is_file)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        list_operations,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        observe,
    )
    monkeypatch.setattr(
        legacy_failure_transition.HostRecoveryInput,
        "__post_init__",
        input_init,
    )
    monkeypatch.setattr(
        legacy_failure_transition.HostTransition,
        "authorized_effects",
        authorize,
    )
    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", atomic_write)
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", guarded_save)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=control,
    )
    kwargs["current_report_digest_resolver"] = resolver
    kwargs["legacy_hold_retries"] = (
        (peer.rollout_id, "host_a"),
        (host_e.rollout_id, "host_e"),
    )
    _call_failure_transition_v2(
        accepted_report=accepted,
        **kwargs,
    )
    assert armed is False
    assert inputs >= 4
    assert checked_vector_phases == [frozenset(required_vector_ids)] * 4
    assert phase_write_counts == [1, 1, 1, 1]
    assert [kind for kind, _detail in events if kind == "authorize"] == ["authorize"] * 4
    assert [detail for kind, detail in events if kind == "authorize"] == [
        "WriteCurrentAckAndIntent",
        "WriteOldBacklink",
        "ConsumeMember",
        "ClearForwardIntent",
    ]
    for index, event in enumerate(events):
        if event[0] == "authorize":
            assert events[index + 1][0] == "write"
    assert effect_targets == [
        current_id,
        host_e.rollout_id,
        host_e.rollout_id,
        current_id,
    ]
    for index in range(1, len(vectors)):
        changed = {
            rollout_id_value
            for rollout_id_value in required_vector_ids
            if vectors[index][rollout_id_value] != vectors[index - 1][rollout_id_value]
        }
        assert changed == {effect_targets[index - 1]}
    assert len({vector[peer.rollout_id] for vector in vectors}) == 1


def _assert_failure_source_is_pending(run: fleet_rollout.RolloutRun) -> None:
    for action_id in _failed_action_ids(run):
        row = run.actions[action_id]
        assert "legacy_failure_update_ack" not in row
        for attempt in row["operation_attempts"]:
            assert attempt["failure_fence_consumed"] is False
            assert attempt["failure_retry_authorized"] is False


def _competing_failure_current(
    current: fleet_rollout.RolloutRun,
    accepted: fleet_release.FleetReleaseReport,
    *,
    digest: str = "d" * 64,
) -> tuple[fleet_rollout.RolloutRun, fleet_release.FleetReleaseReport]:
    assert current.legacy_failure_update_intent is not None
    report = fleet_release.FleetReleaseReport(
        source_ref=accepted.source_ref,
        source_path="vibe-queue/releases/v0.15.142.json",
        digest_sha256=digest,
        generated_at=accepted.generated_at,
        release_version=(0, 15, 142),
        pins=accepted.pins,
        raw={},
    )
    rollout_id_value = fleet_rollout.rollout_id(report)
    intent = copy.deepcopy(current.legacy_failure_update_intent)
    intent.update(
        {
            "current_report_path": report.source_path,
            "current_report_digest_sha256": report.digest_sha256,
            "current_rollout_id": rollout_id_value,
        }
    )
    competing = fleet_rollout.RolloutRun(
        rollout_id=rollout_id_value,
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
        actions=copy.deepcopy(current.actions),
        failed_hosts=copy.deepcopy(current.failed_hosts),
        complete=False,
        legacy_failure_update_intent=intent,
    )
    legacy_failure_transition.parse_failure_update_intent(intent)
    return competing, report


def _interrupt_failure_recovery_after(
    effect_type: type[legacy_failure_transition.RecoveryEffect],
    *,
    accepted: fleet_release.FleetReleaseReport,
    kwargs: Mapping[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_guarded = fleet_rollout._guarded_save_run

    def stop_after_effect(*args: Any, **guard_kwargs: Any) -> Any:
        candidate, effect = original_guarded(*args, **guard_kwargs)
        if type(effect) is effect_type:
            raise RuntimeError(f"fixture crash after {effect_type.__name__}")
        return candidate, effect

    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", stop_after_effect)
    with pytest.raises(RuntimeError, match=f"fixture crash after {effect_type.__name__}"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)


def test_failure_transition_v2_snapshot_and_token_share_one_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    original_read_text = Path.read_text
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    adapter_writes: list[Path] = []
    raced_bytes: bytes | None = None
    armed = True

    def read_text(path: Path, *args: Any, **kwargs: Any) -> str:
        nonlocal armed, raced_bytes
        text = original_read_text(path, *args, **kwargs)
        if armed and path == old_path:
            armed = False
            concurrent = json.loads(text)
            concurrent["failed_hosts"]["host_e"] = "changed after snapshot read"
            fixture_atomic_write(
                old_path,
                json.dumps(concurrent, indent=2, sort_keys=True) + "\n",
            )
            raced_bytes = old_path.read_bytes()
        return text

    def adapter_write(path: Path, text: str) -> None:
        adapter_writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(Path, "read_text", read_text)
    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="changed|stale|CAS"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(),
            ),
        )
    assert armed is False
    assert raced_bytes is not None
    assert adapter_writes == []
    assert old_path.read_bytes() == raced_bytes
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


def test_failure_transition_v2_rejects_competing_forward_intents(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    original_guarded = fleet_rollout._guarded_save_run

    def stop_after_intent(*args: Any, **kwargs: Any) -> Any:
        target_rollout_id = args[0]
        candidate, effect = original_guarded(*args, **kwargs)
        assert candidate.rollout_id == target_rollout_id
        if candidate.legacy_failure_update_intent is not None:
            raise RuntimeError("fixture crash after current intent")
        return candidate, effect

    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", stop_after_intent)
    with pytest.raises(RuntimeError, match="fixture crash"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(),
            ),
        )
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    assert current.legacy_failure_update_intent is not None
    competing_report = fleet_release.FleetReleaseReport(
        source_ref=accepted.source_ref,
        source_path="vibe-queue/releases/v0.15.142.json",
        digest_sha256="e" * 64,
        generated_at=accepted.generated_at,
        release_version=(0, 15, 142),
        pins=accepted.pins,
        raw={},
    )
    competing_id = fleet_rollout.rollout_id(competing_report)
    competing_intent = copy.deepcopy(current.legacy_failure_update_intent)
    competing_intent.update(
        {
            "current_report_path": competing_report.source_path,
            "current_report_digest_sha256": competing_report.digest_sha256,
            "current_rollout_id": competing_id,
        }
    )
    competing = fleet_rollout.RolloutRun(
        rollout_id=competing_id,
        report_digest_sha256=competing_report.digest_sha256,
        report_source_path=competing_report.source_path,
        actions=copy.deepcopy(current.actions),
        failed_hosts=copy.deepcopy(current.failed_hosts),
        complete=False,
        legacy_failure_update_intent=competing_intent,
    )
    original_parsed = legacy_failure_transition.parse_failure_update_intent(
        current.legacy_failure_update_intent
    )
    competing_parsed = legacy_failure_transition.parse_failure_update_intent(
        competing.legacy_failure_update_intent
    )
    assert competing_parsed.host == original_parsed.host
    assert competing_parsed.sources == original_parsed.sources
    assert competing_parsed.current_action_ids == original_parsed.current_action_ids
    assert competing_parsed.current_actions_sha256 == original_parsed.current_actions_sha256
    assert competing_parsed.current_ack_sha256 == original_parsed.current_ack_sha256
    assert (
        competing_parsed.current_rollout_id,
        competing_parsed.current_report_path,
        competing_parsed.current_report_digest_sha256,
    ) == (
        competing.rollout_id,
        competing.report_source_path,
        competing.report_digest_sha256,
    )
    fleet_rollout.save_run(competing)
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        if digest == competing_report.digest_sha256:
            assert source_path == competing_report.source_path
            return competing_report
        return original_historical(source_path, digest, repo, fetch=fetch)

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    tracked_paths = {
        fleet_rollout.rollout_state_path(item.rollout_id) for item in (host_e, current, competing)
    }
    before = {path: path.read_bytes() for path in tracked_paths}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="intent|context|current",
    ):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(calls=controls),
            ),
        )
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in tracked_paths} == before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


@pytest.mark.parametrize("advance_during", ("historical", "status"))
def test_failure_transition_v2_rechecks_latest_after_authentication(
    advance_during: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    latest_digest = accepted.digest_sha256
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest
    inactive = _inactive_failure_status_runner()

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        nonlocal latest_digest
        report = original_historical(source_path, digest, repo, fetch=fetch)
        if advance_during == "historical":
            latest_digest = "e" * 64
        return report

    def control(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal latest_digest
        result = inactive(argv, **kwargs)
        if advance_during == "status":
            latest_digest = "e" * 64
        return result

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    old_before = old_path.read_bytes()
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=control,
    )
    kwargs["current_report_digest_resolver"] = lambda: latest_digest
    with pytest.raises(fleet_rollout.FleetRolloutError, match="report|stale|changed"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert latest_digest == "e" * 64
    assert writes == []
    assert old_path.read_bytes() == old_before
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


@pytest.mark.parametrize("terminal", (False, True))
def test_failure_transition_v2_rechecks_latest_after_final_hold_status(
    terminal: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    if terminal:
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)

    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(
        fleet_rollout.rollout_id(accepted)
    )
    before = {
        path: path.read_bytes()
        for path in (old_path, current_path)
        if path.exists()
    }
    local_epoch_generation = 0
    status_calls = 0
    inactive = _inactive_failure_status_runner()
    writes: list[Path] = []
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text

    def observe_local_epoch(repo: Path) -> fleet_rollout._FailureReportEpoch:
        del repo
        return fleet_rollout._FailureReportEpoch(
            origin_main_commit=("1" if local_epoch_generation == 0 else "2") * 40,
            report_source_path=accepted.source_path,
            report_digest_sha256=accepted.digest_sha256,
            rollout_id=fleet_rollout.rollout_id(accepted),
        )

    def control(
        argv: list[str],
        **control_kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal local_epoch_generation, status_calls
        result = inactive(argv, **control_kwargs)
        status_calls += 1
        if status_calls == 2:
            local_epoch_generation = 1
        return result

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    monkeypatch.setattr(
        fleet_rollout,
        "_observe_local_failure_report_epoch",
        observe_local_epoch,
    )
    kwargs["control_runner"] = control
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="report|epoch|stale|changed",
    ):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert status_calls == 2
    assert local_epoch_generation == 1
    assert writes == []
    assert {
        path: path.read_bytes()
        for path in (old_path, current_path)
        if path.exists()
    } == before
    if terminal:
        assert current_path.exists()
    else:
        assert not current_path.exists()


@pytest.mark.parametrize("terminal", (False, True))
def test_failure_transition_v2_local_epoch_fences_post_status_resolver_side_effect(
    terminal: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    if terminal:
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)

    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(
        fleet_rollout.rollout_id(accepted)
    )
    before = {
        path: path.read_bytes()
        for path in (old_path, current_path)
        if path.exists()
    }
    status_calls = 0
    local_epoch_generation = 0
    final_status_seen = False
    resolver_after_status = 0
    simulated_hold_active = False
    writes: list[Path] = []
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text

    def observe_local_epoch(repo: Path) -> fleet_rollout._FailureReportEpoch:
        del repo
        return fleet_rollout._FailureReportEpoch(
            origin_main_commit=("1" if local_epoch_generation == 0 else "2") * 40,
            report_source_path=accepted.source_path,
            report_digest_sha256=accepted.digest_sha256,
            rollout_id=fleet_rollout.rollout_id(accepted),
        )

    def control(
        argv: list[str],
        **unused: Any,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal final_status_seen, local_epoch_generation, status_calls
        args = argv[3:]
        assert args == ["drain", "--status", "--json", "host_e"]
        status_calls += 1
        status = _inactive_full_drain_status()
        if simulated_hold_active:
            hold = host_e.holds["host_e"]
            status.update(
                {
                    "active": True,
                    "mode": "full",
                    "is_full_drain": True,
                    "state": {
                        "reason": hold["reason"],
                        "set_at": hold["set_at"],
                    },
                    "submit_policy": "deny",
                }
            )
        if status_calls == 2:
            final_status_seen = True
            local_epoch_generation = 1
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(status),
            stderr="",
        )

    def resolver() -> str:
        nonlocal resolver_after_status, simulated_hold_active
        if final_status_seen:
            resolver_after_status += 1
            simulated_hold_active = True
        return accepted.digest_sha256

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(
        fleet_rollout,
        "_observe_local_failure_report_epoch",
        observe_local_epoch,
    )
    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = control
    kwargs["current_report_digest_resolver"] = resolver
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="report|epoch|stale|changed",
    ):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert status_calls == 2
    assert local_epoch_generation == 1
    assert resolver_after_status == 0
    assert simulated_hold_active is False
    assert writes == []
    assert {
        path: path.read_bytes()
        for path in (old_path, current_path)
        if path.exists()
    } == before


def test_failure_transition_v2_success_never_resolves_fetchfully_after_final_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    epoch = fleet_rollout._FailureReportEpoch(
        origin_main_commit="1" * 40,
        report_source_path=accepted.source_path,
        report_digest_sha256=accepted.digest_sha256,
        rollout_id=fleet_rollout.rollout_id(accepted),
    )
    awaiting_status = False
    status_seen = False
    epoch_observations = 0

    def observe_local_epoch(repo: Path) -> fleet_rollout._FailureReportEpoch:
        nonlocal awaiting_status, epoch_observations, status_seen
        del repo
        epoch_observations += 1
        if not awaiting_status:
            awaiting_status = True
            status_seen = False
        else:
            assert status_seen is True
            awaiting_status = False
        return epoch

    inactive = _inactive_failure_status_runner()

    def control(
        argv: list[str],
        **control_kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal status_seen
        result = inactive(argv, **control_kwargs)
        if awaiting_status:
            status_seen = True
        return result

    def resolver() -> str:
        assert not (awaiting_status and status_seen), (
            "fetchful resolver ran after the final hold status"
        )
        return accepted.digest_sha256

    monkeypatch.setattr(
        fleet_rollout,
        "_observe_local_failure_report_epoch",
        observe_local_epoch,
    )
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=control,
    )
    kwargs["current_report_digest_resolver"] = resolver
    result = _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert result.settled_inactive_holds == ((host_e.rollout_id, "host_e"),)
    assert epoch_observations >= 2
    assert epoch_observations % 2 == 0
    assert awaiting_status is False


def _add_same_rollout_failed_host(
    source: fleet_rollout.RolloutRun,
    plan: fleet_rollout.RolloutPlan,
    *,
    host: str,
    request_sha256: str,
) -> fleet_operation.OperationObservation:
    template = next(action for action in plan.actions if action.host == "host_e")
    action = copy.deepcopy(template)
    action.id = f"local-runtime:{host}:vibeqc-release"
    action.host = host
    action.argv = ["admin", "update", "vibeqc-release", host]
    plan.actions = [item for item in plan.actions if item.host != host]
    plan.actions.append(action)
    historical = copy.deepcopy(action)
    historical.target_sha = RELEASE
    historical.target_version = str(_report().pins["release"].version)
    historical.target_tag = _report().pins["release"].tag
    identity = fleet_rollout._operation_identity(
        source.rollout_id,
        source.report_digest_sha256,
        historical,
        attempt=1,
    )
    observation = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(identity),
        identity=identity,
        request_sha256=request_sha256,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "failed", "executed": True, "returncode": 11},
    )
    source.actions[action.id] = {
        "status": "failed",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                observation,
                identity,
                request_sha256=observation.request_sha256,
                harvested=True,
            )
        ],
    }
    source.holds[host] = {
        "host": host,
        "kind": "full",
        "owned": True,
        "preexisting": False,
        "reason": fleet_rollout._hold_reason(source.rollout_id, host),
        "status": "active",
        "duration_seconds": 21600,
        "set_at": "2026-08-08T12:00:00+00:00",
        "control_host": host,
    }
    source.legacy_retained_holds[host] = (
        fleet_rollout._legacy_retained_hold_receipt(
            source,
            host=host,
            hold=source.holds[host],
            plan=plan,
            source="hold-observation",
            control_host=host,
            retained_at="2026-08-25T11:59:30+00:00",
            reason="prior observation retained exact hold",
        )
    )
    source.complete = False
    fleet_rollout.save_run(source)
    return observation


def test_failure_transition_same_rollout_multi_host_inventory_and_restart_are_local(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    source, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    observations = {
        operation_id: fleet_rollout.fleet_operation.observe_operation(
            operation_id,
            recover=True,
        )
        for operation_id in fleet_rollout.fleet_operation.list_operation_ids(
            recover=True
        )
    }
    host_a_observation = _add_same_rollout_failed_host(
        source,
        plan,
        host="host_a",
        request_sha256="6" * 64,
    )
    observations[host_a_observation.operation_id] = host_a_observation
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: tuple(sorted(observations)),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )
    current_id = fleet_rollout.rollout_id(accepted)
    host_a_action_id = host_a_observation.identity.action_id
    host_e_state = {
        "actions": copy.deepcopy(
            {
                action_id: row
                for action_id, row in source.actions.items()
                if any(
                    attempt["identity"]["host"] == "host_e"
                    for attempt in row.get("operation_attempts", [])
                )
            }
        ),
        "hold": copy.deepcopy(source.holds["host_e"]),
        "retained": copy.deepcopy(source.legacy_retained_holds["host_e"]),
    }
    original_guarded = fleet_rollout._guarded_save_run

    def crash_after_host_a_intent(*args: Any, **kwargs: Any) -> Any:
        candidate, effect = original_guarded(*args, **kwargs)
        if isinstance(
            effect,
            legacy_failure_transition.WriteCurrentAckAndIntent,
        ):
            raise RuntimeError("crash after host_a current intent")
        return candidate, effect

    monkeypatch.setattr(
        fleet_rollout,
        "_guarded_save_run",
        crash_after_host_a_intent,
    )
    with pytest.raises(RuntimeError, match="host_a current intent"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **{
                **_failure_transition_kwargs(
                    host_e=source,
                    plan=plan,
                    tmp_path=tmp_path,
                    control_runner=_inactive_failure_status_runner(
                        hosts=("host_a", "host_e")
                    ),
                ),
                "legacy_hold_retries": ((source.rollout_id, "host_a"),),
                "failed_operation_hosts": (
                    (source.rollout_id, "host_a", "host_a failed rc=11"),
                ),
            },
        )
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)

    pending = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "pending inventory must remain read-only"
        ),
    )
    assert pending.pending_failure_transitions == (
        (source.rollout_id, "host_a", "host_a failed rc=11"),
    )
    _call_failure_transition_v2(
        accepted_report=accepted,
        **{
            **_failure_transition_kwargs(
                host_e=source,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(
                    hosts=("host_a", "host_e")
                ),
            ),
            "legacy_hold_retries": ((source.rollout_id, "host_a"),),
            "failed_operation_hosts": pending.pending_failure_transitions,
        },
    )
    after_host_a = fleet_rollout.load_run(source.rollout_id)
    assert after_host_a is not None
    assert after_host_a.actions[host_a_action_id]["operation_attempts"][0][
        "failure_fence_consumed"
    ] is True
    assert {
        "actions": {
            action_id: row
            for action_id, row in after_host_a.actions.items()
            if any(
                attempt["identity"]["host"] == "host_e"
                for attempt in row.get("operation_attempts", [])
            )
        },
        "hold": after_host_a.holds["host_e"],
        "retained": after_host_a.legacy_retained_holds["host_e"],
    } == host_e_state

    host_e_inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "same-rollout inventory must not reach host control"
        ),
    )
    host_e_rows = tuple(
        row
        for row in host_e_inventory.failed_operation_hosts
        if row[1] == "host_e"
    )
    assert len(host_e_rows) == 1
    after_inventory = fleet_rollout.load_run(source.rollout_id)
    assert after_inventory is not None
    assert after_inventory.actions[host_a_action_id]["operation_attempts"][0][
        "failure_retry_authorized"
    ] is True
    host_a_state = {
        "action": copy.deepcopy(after_inventory.actions[host_a_action_id]),
        "hold": copy.deepcopy(after_inventory.holds["host_a"]),
    }

    def crash_after_host_e_intent(*args: Any, **kwargs: Any) -> Any:
        candidate, effect = original_guarded(*args, **kwargs)
        if isinstance(
            effect,
            legacy_failure_transition.WriteCurrentAckAndIntent,
        ):
            raise RuntimeError("crash after host_e current intent")
        return candidate, effect

    monkeypatch.setattr(
        fleet_rollout,
        "_guarded_save_run",
        crash_after_host_e_intent,
    )
    host_e_kwargs = {
        **_failure_transition_kwargs(
            host_e=source,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(
                hosts=("host_a", "host_e")
            ),
        ),
        "failed_operation_hosts": host_e_rows,
    }
    with pytest.raises(RuntimeError, match="host_e current intent"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **host_e_kwargs,
        )
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)
    host_e_pending = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "same-rollout pending inventory must remain read-only"
        ),
    )
    assert host_e_pending.pending_failure_transitions == host_e_rows
    _call_failure_transition_v2(
        accepted_report=accepted,
        **{
            **host_e_kwargs,
            "failed_operation_hosts": host_e_pending.pending_failure_transitions,
        },
    )
    final_source = fleet_rollout.load_run(source.rollout_id)
    current = fleet_rollout.load_run(current_id)
    assert final_source is not None
    assert current is not None
    for host in ("host_a", "host_e"):
        host_rows = [
            row
            for row in final_source.actions.values()
            if row.get("status") == "failed"
            and any(
                attempt["identity"]["host"] == host
                for attempt in row.get("operation_attempts", [])
            )
        ]
        assert host_rows
        assert all(
            attempt["failure_fence_consumed"] is True
            and attempt["failure_retry_authorized"] is (host == "host_a")
            for row in host_rows
            for attempt in row["operation_attempts"]
        )
    assert {
        "action": final_source.actions[host_a_action_id],
        "hold": final_source.holds["host_a"],
    } == host_a_state
    assert current.failed_hosts == {
        "host_a": "host_a failed rc=11",
        "host_e": host_e_rows[0][2],
    }


@pytest.mark.parametrize("stage", ("pending", "terminal"))
def test_failure_transition_rejects_extra_same_host_backlink_placement(
    stage: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    if stage == "pending":
        _interrupt_failure_recovery_after(
            legacy_failure_transition.WriteOldBacklink,
            accepted=accepted,
            kwargs=kwargs,
            monkeypatch=monkeypatch,
        )
    else:
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    source = fleet_rollout.load_run(host_e.rollout_id)
    assert source is not None
    original = source.actions[_failed_action_ids(source)[0]][
        "legacy_failure_update_ack"
    ]
    extra_action_id = "local-runtime:host_e:vibe-view"
    forged = copy.deepcopy(original)
    forged["failed_action_ids"] = [extra_action_id]
    legacy_failure_transition.parse_failure_update_backlink(forged)
    source.actions[extra_action_id] = {
        "status": "success",
        "legacy_failure_update_ack": forged,
    }
    source_path = fleet_rollout.save_run(source)
    current_path = fleet_rollout.rollout_state_path(
        fleet_rollout.rollout_id(accepted)
    )
    before = {
        source_path: source_path.read_bytes(),
        current_path: current_path.read_bytes(),
    }
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="backlink|placement|action",
    ):
        if stage == "pending":
            fleet_rollout.reconcile_durable_operations(
                current_report_digest_sha256=accepted.digest_sha256,
                current_report_source_path=accepted.source_path,
                inspect_only=False,
                allow_legacy_reconciliation=True,
                control_runner=lambda *args, **kwargs: pytest.fail(
                    "malformed pending backlink reached host control"
                ),
            )
        else:
            _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert writes == []
    assert {
        source_path: source_path.read_bytes(),
        current_path: current_path.read_bytes(),
    } == before


@pytest.mark.parametrize("mixed_ref", ("action", "scheduler-hold"))
def test_failure_transition_v2_rejects_mixed_legacy_references(
    mixed_ref: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    old_before = old_path.read_bytes()
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(calls=controls),
    )
    if mixed_ref == "action":
        kwargs["legacy_running_actions"] = ((host_e.rollout_id, "local-runtime:host_e:missing"),)
    else:
        kwargs["legacy_scheduler_holds"] = ((host_e.rollout_id, "host_e"),)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="mixed|legacy|failure"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert old_path.read_bytes() == old_before
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


def test_failure_transition_v2_uses_history_preserving_action_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    original_replace = fleet_rollout._replace_action_record
    replacements: list[tuple[str, str, dict[str, Any]]] = []

    def replace_action(
        run: fleet_rollout.RolloutRun,
        action_id: str,
        record: Mapping[str, Any],
    ) -> None:
        replacements.append((run.rollout_id, action_id, copy.deepcopy(dict(record))))
        original_replace(run, action_id, record)

    monkeypatch.setattr(fleet_rollout, "_replace_action_record", replace_action)
    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(),
        ),
    )
    failed_ids = set(_failed_action_ids(host_e))
    current_id = fleet_rollout.rollout_id(accepted)
    assert {
        action_id
        for rollout_id_value, action_id, row in replacements
        if rollout_id_value == current_id and set(row) == {"decision", "reason", "status"}
    } == failed_ids
    assert {
        action_id
        for rollout_id_value, action_id, row in replacements
        if rollout_id_value == host_e.rollout_id
        and "legacy_failure_update_ack" in row
    } == failed_ids


def test_failure_transition_v2_materializes_only_authorized_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    original_authorize = legacy_failure_transition.HostTransition.authorized_effects
    original_apply = fleet_rollout._apply_failure_effect
    authorized_effects: set[int] = set()

    def authorize(
        transition: legacy_failure_transition.HostTransition,
    ) -> tuple[legacy_failure_transition.RecoveryEffect, ...]:
        effects = original_authorize(transition)
        authorized_effects.update(id(effect) for effect in effects)
        return effects

    def apply_effect(
        effect: legacy_failure_transition.RecoveryEffect,
        **kwargs: Any,
    ) -> fleet_rollout.RolloutRun:
        assert id(effect) in authorized_effects
        return original_apply(effect, **kwargs)

    monkeypatch.setattr(
        legacy_failure_transition.HostTransition,
        "authorized_effects",
        authorize,
    )
    monkeypatch.setattr(fleet_rollout, "_apply_failure_effect", apply_effect)
    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(),
        ),
    )
    assert authorized_effects


@pytest.mark.parametrize(
    ("crash_effect", "expect_pending"),
    [
        (legacy_failure_transition.WriteOldBacklink, True),
        (legacy_failure_transition.ConsumeMember, False),
    ],
    ids=("before-consume", "before-clear"),
)
def test_failure_transition_v2_rejects_late_competing_context_before_each_effect(
    crash_effect: type[legacy_failure_transition.RecoveryEffect],
    expect_pending: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        crash_effect,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    interrupted = fleet_rollout.load_run(host_e.rollout_id)
    assert current is not None
    assert interrupted is not None
    competing, competing_report = _competing_failure_current(current, accepted)
    competing_path = fleet_rollout.rollout_state_path(competing.rollout_id)
    current_path = fleet_rollout.rollout_state_path(current_id)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_before = current_path.read_bytes()
    old_before = old_path.read_bytes()
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        if digest == competing_report.digest_sha256:
            assert source_path == competing_report.source_path
            return competing_report
        return original_historical(source_path, digest, repo, fetch=fetch)

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    injected = False
    controls: list[list[str]] = []
    adapter_writes: list[Path] = []
    inactive = _inactive_failure_status_runner(calls=controls)

    def control(argv: list[str], **control_kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal injected
        if not injected:
            fixture_atomic_write(
                competing_path,
                json.dumps(competing.as_dict(), indent=2, sort_keys=True) + "\n",
            )
            injected = True
        return inactive(argv, **control_kwargs)

    def adapter_write(path: Path, text: str) -> None:
        adapter_writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = control
    with pytest.raises(fleet_rollout.FleetRolloutError, match="intent|context|journal"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert injected is True
    assert controls
    assert adapter_writes == []
    assert current_path.read_bytes() == current_before
    assert old_path.read_bytes() == old_before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    assert all(
        attempt["failure_fence_consumed"] is not expect_pending
        for action_id in _failed_action_ids(persisted)
        for attempt in persisted.actions[action_id]["operation_attempts"]
    )


def test_failure_transition_v2_revalidates_context_before_terminal_completion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    original_guarded = fleet_rollout._guarded_save_run
    captured_intent: dict[str, Any] | None = None

    def capture_intent(*args: Any, **kwargs: Any) -> Any:
        nonlocal captured_intent
        candidate, effect = original_guarded(*args, **kwargs)
        if candidate.legacy_failure_update_intent is not None:
            captured_intent = copy.deepcopy(candidate.legacy_failure_update_intent)
        return candidate, effect

    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", capture_intent)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    monkeypatch.setattr(fleet_rollout, "_guarded_save_run", original_guarded)
    assert captured_intent is not None
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    intent_current = copy.deepcopy(current)
    intent_current.legacy_failure_update_intent = captured_intent
    competing, _competing_report = _competing_failure_current(intent_current, accepted)
    competing_path = fleet_rollout.rollout_state_path(competing.rollout_id)
    current_path = fleet_rollout.rollout_state_path(current_id)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = {path: path.read_bytes() for path in (current_path, old_path)}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    injected = False
    controls: list[list[str]] = []
    writes: list[Path] = []
    inactive = _inactive_failure_status_runner(calls=controls)

    def control(argv: list[str], **control_kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal injected
        if not injected:
            fixture_atomic_write(
                competing_path,
                json.dumps(competing.as_dict(), indent=2, sort_keys=True) + "\n",
            )
            injected = True
        return inactive(argv, **control_kwargs)

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = control
    with pytest.raises(fleet_rollout.FleetRolloutError, match="intent|context|journal"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert injected is True
    assert controls == [
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
    ]
    assert writes == []
    assert {path: path.read_bytes() for path in (current_path, old_path)} == before



@pytest.mark.parametrize("alias_mode", ("misnamed-only", "duplicate-alias"))
def test_failure_transition_v2_rejects_misnamed_or_aliased_intent_journal(
    alias_mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current_id = fleet_rollout.rollout_id(accepted)
    current_path = fleet_rollout.rollout_state_path(current_id)
    current_bytes = current_path.read_bytes()
    alias_path = fleet_rollout.rollout_state_dir() / "misnamed-current-alias.json"
    fleet_rollout.paths.atomic_write_text(alias_path, current_bytes.decode("utf-8"))
    if alias_mode == "misnamed-only":
        current_path.unlink()
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    tracked = {old_path, alias_path}
    if current_path.exists():
        tracked.add(current_path)
    before = {path: path.read_bytes() for path in tracked}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = _inactive_failure_status_runner(calls=controls)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="path|alias|journal|rollout_id"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in tracked} == before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


def test_failure_transition_v2_preserves_unrelated_current_sibling_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    current_id = fleet_rollout.rollout_id(accepted)
    sibling_action = "local-runtime:host_a:vibeqc-release"
    current = fleet_rollout.RolloutRun(
        rollout_id=current_id,
        report_digest_sha256=accepted.digest_sha256,
        report_source_path=accepted.source_path,
        actions={
            sibling_action: {
                "status": "not-run",
                "decision": "defer",
                "reason": "host_a is independently offline",
            }
        },
        holds={
            "host_a": {
                "host": "host_a",
                "kind": "full",
                "owned": True,
                "status": "active",
            }
        },
        failed_hosts={"host_a": "host_a independently failed"},
        complete=False,
    )
    fleet_rollout.save_run(current)
    before = copy.deepcopy(current.as_dict())
    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(),
        ),
    )
    persisted = fleet_rollout.load_run(current_id)
    assert persisted is not None
    assert persisted.failed_hosts == {
        "host_a": "host_a independently failed",
        "host_e": "release failed rc=9",
    }
    assert persisted.actions[sibling_action] == before["actions"][sibling_action]
    assert persisted.holds == before["holds"]
    assert persisted.complete is False
    assert persisted.legacy_failure_update_intent is None
    assert set(persisted.actions) == {sibling_action, *_failed_action_ids(host_e)}


@pytest.mark.parametrize("skip_case", ("schema", "host", "current"))
def test_failure_transition_v2_strictly_classifies_persisted_skip_evidence(
    skip_case: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    evidence = host_e.actions[action.id]["legacy_failure_skip"]
    if skip_case == "schema":
        evidence["schema"] = "vq.fleet.legacy_failure_skip/999"
    elif skip_case == "host":
        evidence["host"] = "host_a"
    else:
        action.decision = "skip"
        action.reason = "already at target with LAST OK=true"
        action.before["current_sha"] = action.target_sha
        action.before["last_ok"] = True
        evidence.update(
            {
                "current_report_path": accepted.source_path,
                "current_report_digest_sha256": accepted.digest_sha256,
                "current_target_sha": action.target_sha,
                "current_action_reason": action.reason,
                "observed_current_sha": action.target_sha,
            }
        )
    old_path = fleet_rollout.save_run(host_e)
    old_before = old_path.read_bytes()
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="skip|tampered|malformed"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(calls=controls),
            ),
        )
    assert controls == []
    assert writes == []
    assert old_path.read_bytes() == old_before
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


def test_failure_transition_v2_rejects_current_skip_before_active_hold_control(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    action.decision = "skip"
    action.reason = "already at target with LAST OK=true"
    action.before["current_sha"] = action.target_sha
    action.before["last_ok"] = True
    host_e.actions[action.id]["legacy_failure_skip"].update(
        {
            "current_report_path": accepted.source_path,
            "current_report_digest_sha256": accepted.digest_sha256,
            "current_target_sha": action.target_sha,
            "current_action_reason": action.reason,
            "observed_current_sha": action.target_sha,
        }
    )
    old_path = fleet_rollout.save_run(host_e)
    old_before = old_path.read_bytes()
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="current skip"):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=lambda argv, **unused: (
                    controls.append(argv[3:])
                    or pytest.fail("CURRENT skip must reject before hold control")
                ),
            ),
        )
    assert controls == []
    assert writes == []
    assert old_path.read_bytes() == old_before
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


def test_failure_transition_v2_rejects_malformed_overlapping_forward_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    competing, competing_report = _competing_failure_current(current, accepted)
    assert competing.legacy_failure_update_intent is not None
    competing.legacy_failure_update_intent["sources"] = {
        "historical_rollout_id": host_e.rollout_id,
    }
    competing_path = fleet_rollout.save_run(competing)
    original_historical = fleet_rollout.fleet_release.discover_historical_report_by_digest

    def historical(
        source_path: str,
        digest: str,
        repo: Path,
        *,
        fetch: bool,
    ) -> fleet_release.FleetReleaseReport:
        if digest == competing_report.digest_sha256:
            return competing_report
        return original_historical(source_path, digest, repo, fetch=fetch)

    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        historical,
    )
    paths = {
        fleet_rollout.rollout_state_path(host_e.rollout_id),
        fleet_rollout.rollout_state_path(current_id),
        competing_path,
    }
    before = {path: path.read_bytes() for path in paths}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = _inactive_failure_status_runner(calls=controls)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="intent|source|malformed"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in paths} == before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


@pytest.mark.parametrize(
    ("effect_type", "outstanding"),
    (
        (legacy_failure_transition.WriteCurrentAckAndIntent, True),
        (legacy_failure_transition.WriteOldBacklink, True),
        (legacy_failure_transition.ConsumeMember, True),
        (legacy_failure_transition.ClearForwardIntent, False),
    ),
)
def test_failure_transition_restart_inventory_routes_outstanding_intent(
    effect_type: type[legacy_failure_transition.RecoveryEffect],
    outstanding: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        effect_type,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "restart inventory must not inspect or mutate host control"
        ),
    )

    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    failed_attempts = [
        attempt
        for action_id in _failed_action_ids(persisted)
        for attempt in persisted.actions[action_id]["operation_attempts"]
    ]
    if outstanding:
        assert inventory.pending_failure_transitions == (
            (host_e.rollout_id, "host_e", "release failed rc=9"),
        )
        assert inventory.failed_operation_hosts == ()
        assert all(
            attempt["failure_retry_authorized"] is False
            for attempt in failed_attempts
        )
    else:
        assert inventory.pending_failure_transitions == ()
        assert inventory.failed_operation_hosts == ()
        assert all(
            attempt["failure_retry_authorized"] is True
            for attempt in failed_attempts
        )


def test_pending_failure_transition_never_resumes_an_authorized_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )

    failed_ids = tuple(fleet_rollout.fleet_operation.list_operation_ids(recover=True))
    failed_observations = {
        operation_id: fleet_rollout.fleet_operation.observe_operation(
            operation_id,
            recover=True,
        )
        for operation_id in failed_ids
    }
    authorized_identity = fleet_operation.OperationIdentity(
        rollout_id="v0.15.140-authorized-peer",
        report_digest_sha256="a" * 64,
        attempt=1,
        action_id="local-runtime:host_a:vibeqc-queue",
        phase="local-runtime",
        host="host_a",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=VQ,
        target_version="0.15.140",
        target_tag="v0.15.140",
        argv=("admin", "update", "vibeqc-queue", "host_a"),
        lifecycle_resources=(("checkout", "/managed/vq"),),
        rollout_lock_path="/managed/vq-rollout.lock",
    )
    authorized_id = fleet_operation.operation_id(authorized_identity)
    authorized = fleet_operation.OperationObservation(
        operation_id=authorized_id,
        identity=authorized_identity,
        request_sha256="e" * 64,
        state="authorized-unactivated",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation=None,
        result=None,
    )
    observations = {**failed_observations, authorized_id: authorized}
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: tuple(sorted(observations)),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail(
            "pending failure transition resumed an authorized operation"
        ),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "attach_active_rollout_lock",
        lambda lifecycle_handoff, *, rollout_id: (
            f"authenticated:{lifecycle_handoff}:{rollout_id}"
        ),
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        lifecycle_handoff="authenticated-lifecycle-handoff",
        control_runner=lambda *args, **kwargs: pytest.fail(
            "pending inventory must not reach host control"
        ),
    )

    assert inventory.pending_failure_transitions == (
        (host_e.rollout_id, "host_e", "release failed rc=9"),
    )
    assert not fleet_rollout.rollout_state_path(
        authorized_identity.rollout_id
    ).exists()


def test_pending_failure_inventory_also_carries_obsolete_scheduler_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    scheduler, scheduler_path = _obsolete_preowner_scheduler_claim(tmp_path)
    current_path = fleet_rollout.rollout_state_path(
        fleet_rollout.rollout_id(accepted)
    )
    source_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    paths = (source_path, current_path, scheduler_path)
    before = {path: path.read_bytes() for path in paths}

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=accepted.digest_sha256,
        current_report_source_path=accepted.source_path,
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "pending plus scheduler inventory must remain read-only"
        ),
    )

    assert inventory.pending_failure_transitions == (
        (host_e.rollout_id, "host_e", "release failed rc=9"),
    )
    assert inventory.legacy_scheduler_holds == (
        (scheduler.rollout_id, "host_c"),
    )
    assert inventory.legacy_hold_retries == ((host_e.rollout_id, "host_e"),)
    assert {path: path.read_bytes() for path in paths} == before


def test_pending_failure_cli_handoff_keeps_real_scheduler_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    scheduler, scheduler_path = _obsolete_preowner_scheduler_claim(tmp_path)
    current_path = fleet_rollout.rollout_state_path(
        fleet_rollout.rollout_id(accepted)
    )
    source_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    paths = (source_path, current_path, scheduler_path)
    before = {path: path.read_bytes() for path in paths}
    cfg = config.Config(
        hosts={
            name: config.HostConfig(ssh=name, fleet_role="managed")
            for name in ("localhost", "host_a", "host_e", "host_c")
        },
        programs={
            "vibeqc-queue": config.VenvProgram(
                kind="venv",
                python="/repo/.venv/bin/python",
                git_dir=str(tmp_path),
                branch="main",
                update_script="vibe-queue/scripts/update.sh",
            )
        },
        fleet_rollout_order=["localhost", "host_a", "host_e", "host_c"],
    )

    @contextmanager
    def fence(*unused: Any, **ignored: Any) -> Iterator[None]:
        yield

    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    monkeypatch.setattr("vq.cli.fleet_release.runtime_repo", lambda: tmp_path)
    monkeypatch.setattr(
        "vq.cli.fleet_release.discover_latest_report",
        lambda repo, **kwargs: accepted,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_release.git_is_ancestor",
        lambda repo, older, newer: True,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_checkout",
        lambda path: tmp_path,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_target",
        lambda path: Path("/managed/vq"),
    )
    monkeypatch.setattr("vq.cli.admin_module.toolset_lifecycle_lock", fence)
    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", fence)
    monkeypatch.setattr("vq.cli.fleet_rollout.adopt_rollout_reentry_handoff", fence)
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        lambda: ('{"schema":"vq.toolset.lifecycle_handoff/1","locks":[]}', ()),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        lambda: (),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module.source_tree_sha256_at_git_commit",
        lambda project_root, source_sha: "d" * 64,
        raising=False,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: ({}, {}, {}),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.build_plan",
        lambda *args, **kwargs: plan,
    )
    monkeypatch.setattr(
        "vq.cli.subprocess.run",
        lambda *args, **kwargs: pytest.fail(
            "real pending inventory handoff reached an unexpected control"
        ),
    )
    handoffs: list[dict[str, Any]] = []

    def legacy(**legacy_kwargs: Any) -> fleet_rollout.LegacyRolloutReconciliation:
        handoffs.append(legacy_kwargs)
        return fleet_rollout.LegacyRolloutReconciliation()

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_legacy_rollout_state",
        legacy,
    )
    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--reconcile-legacy", "--json"],
    )

    assert result.exit_code == 0, result.output
    assert len(handoffs) == 2
    assert handoffs[0]["failed_operation_hosts"] == (
        (host_e.rollout_id, "host_e", "release failed rc=9"),
    )
    assert handoffs[0]["legacy_scheduler_holds"] == ()
    assert handoffs[0]["accepted_report"] is accepted
    assert handoffs[1]["failed_operation_hosts"] == ()
    assert handoffs[1]["legacy_scheduler_holds"] == (
        (scheduler.rollout_id, "host_c"),
    )
    assert handoffs[1]["accepted_report"] is accepted
    assert {path: path.read_bytes() for path in paths} == before


def test_pending_failure_inventory_binds_attempt_host_before_retry_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.ConsumeMember,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    source = fleet_rollout.load_run(host_e.rollout_id)
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert source is not None
    assert current is not None
    assert current.legacy_failure_update_intent is not None
    action_id = _failed_action_ids(source)[0]
    attempt = source.actions[action_id]["operation_attempts"][0]
    old_operation_id = str(attempt["operation_id"])
    old_observation = fleet_rollout.fleet_operation.observe_operation(
        old_operation_id,
        recover=True,
    )
    foreign_identity = dataclasses.replace(old_observation.identity, host="host_a")
    foreign_operation_id = fleet_operation.operation_id(foreign_identity)
    attempt["identity"] = foreign_identity.as_dict()
    attempt["operation_id"] = foreign_operation_id
    source.actions[action_id]["legacy_failure_update_ack"][
        "failed_operation_ids"
    ] = sorted(
        foreign_operation_id if item == old_operation_id else item
        for item in source.actions[action_id]["legacy_failure_update_ack"][
            "failed_operation_ids"
        ]
    )
    for sibling_id in _failed_action_ids(source):
        source.actions[sibling_id]["legacy_failure_update_ack"][
            "failed_operation_ids"
        ] = copy.deepcopy(
            source.actions[action_id]["legacy_failure_update_ack"][
                "failed_operation_ids"
            ]
        )
    for member in current.legacy_failure_update_intent["sources"]:
        if member["historical_rollout_id"] == source.rollout_id:
            member["failed_operation_ids"] = copy.deepcopy(
                source.actions[action_id]["legacy_failure_update_ack"][
                    "failed_operation_ids"
                ]
            )
    legacy_failure_transition.parse_failure_update_intent(
        current.legacy_failure_update_intent
    )
    fleet_rollout.save_run(source)
    fleet_rollout.save_run(current)

    operation_ids = tuple(
        fleet_rollout.fleet_operation.list_operation_ids(recover=True)
    )
    observations = {
        operation_id: fleet_rollout.fleet_operation.observe_operation(
            operation_id,
            recover=True,
        )
        for operation_id in operation_ids
        if operation_id != old_operation_id
    }
    observations[foreign_operation_id] = dataclasses.replace(
        old_observation,
        operation_id=foreign_operation_id,
        identity=foreign_identity,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: tuple(sorted(observations)),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )
    paths = (
        fleet_rollout.rollout_state_path(source.rollout_id),
        fleet_rollout.rollout_state_path(current.rollout_id),
    )
    before = {path: path.read_bytes() for path in paths}
    fixture_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="attempt|identity|host|pending|transition",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=accepted.digest_sha256,
            current_report_source_path=accepted.source_path,
            inspect_only=False,
            allow_legacy_reconciliation=True,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "malformed pending inventory reached host control"
            ),
        )
    assert writes == []
    assert {path: path.read_bytes() for path in paths} == before


@pytest.mark.parametrize(
    "effect_type",
    (
        legacy_failure_transition.WriteCurrentAckAndIntent,
        legacy_failure_transition.WriteOldBacklink,
        legacy_failure_transition.ConsumeMember,
    ),
)
def test_failure_transition_restart_cli_cannot_launch_before_durable_clear(
    effect_type: type[legacy_failure_transition.RecoveryEffect],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from click.testing import CliRunner

    from vq.cli import main

    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    driver = fleet_rollout.RolloutAction(
        id="driver:localhost:vibeqc-queue",
        phase="driver",
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=accepted.pins["vq"].sha,
        target_version=str(accepted.pins["vq"].version),
        target_tag=accepted.pins["vq"].tag,
        argv=["admin", "update", "vibeqc-queue", "localhost"],
        decision="skip",
        reason="already at target with LAST OK=true",
        before={
            "configured": True,
            "current_sha": accepted.pins["vq"].sha,
            "current_version": str(accepted.pins["vq"].version),
            "current_tag": accepted.pins["vq"].tag,
            "dirty": False,
            "last_ok": True,
            "acknowledged": True,
            "detail": "already current",
            "metrics": None,
        },
    )
    plan.actions.insert(0, driver)
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        effect_type,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    assert current.legacy_failure_update_intent is not None

    cfg = config.Config(
        hosts={
            "localhost": config.HostConfig(ssh="localhost", fleet_role="managed"),
            "host_e": config.HostConfig(ssh="host_e", fleet_role="managed"),
        },
        programs={
            "vibeqc-queue": config.VenvProgram(
                kind="venv",
                python="/repo/.venv/bin/python",
                git_dir=str(tmp_path),
                branch="main",
                update_script="vibe-queue/scripts/update.sh",
            )
        },
        fleet_rollout_order=["localhost", "host_e"],
    )

    @contextmanager
    def fence(*unused: Any, **ignored: Any) -> Iterator[None]:
        yield

    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    monkeypatch.setattr("vq.cli.fleet_release.runtime_repo", lambda: tmp_path)
    monkeypatch.setattr(
        "vq.cli.fleet_release.discover_latest_report",
        lambda repo, **kwargs: accepted,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_release.git_is_ancestor",
        lambda repo, older, newer: True,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_checkout",
        lambda path: tmp_path,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_target",
        lambda path: Path("/managed/vq"),
    )
    monkeypatch.setattr("vq.cli.admin_module.toolset_lifecycle_lock", fence)
    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", fence)
    monkeypatch.setattr("vq.cli.fleet_rollout.adopt_rollout_reentry_handoff", fence)
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        lambda: ('{"schema":"vq.toolset.lifecycle_handoff/1","locks":[]}', ()),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        lambda: (),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module.source_tree_sha256_at_git_commit",
        lambda project_root, source_sha: "d" * 64,
        raising=False,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: ({}, {}, {}),
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.build_plan", lambda *args, **kwargs: plan)
    monkeypatch.setattr(
        "vq.cli.subprocess.run",
        _inactive_failure_status_runner(),
    )
    launches: list[str] = []

    def launch(*args: Any, **kwargs: Any) -> None:
        launches.append("ordinary-plan")
        raise fleet_rollout.FleetRolloutError("ordinary plan launched before clear")

    monkeypatch.setattr("vq.cli.fleet_rollout.execute_plan", launch)
    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--json"],
    )

    assert result.exit_code == 1, result.output
    assert "legacy failure transition" in result.output
    assert "--reconcile-legacy" in result.output
    assert launches == []
    persisted_current = fleet_rollout.load_run(current_id)
    assert persisted_current is not None
    assert persisted_current.legacy_failure_update_intent is not None
    persisted_old = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted_old is not None
    assert all(
        attempt["failure_retry_authorized"] is False
        for action_id in _failed_action_ids(persisted_old)
        for attempt in persisted_old.actions[action_id]["operation_attempts"]
    )

    recovered = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--reconcile-legacy", "--json"],
    )
    assert recovered.exit_code == 0, recovered.output
    assert launches == []
    persisted_current = fleet_rollout.load_run(current_id)
    persisted_old = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted_current is not None
    assert persisted_old is not None
    assert persisted_current.legacy_failure_update_intent is None
    assert all(
        attempt["failure_fence_consumed"] is True
        and attempt["failure_retry_authorized"] is False
        for action_id in _failed_action_ids(persisted_old)
        for attempt in persisted_old.actions[action_id]["operation_attempts"]
    )


@pytest.mark.parametrize(
    ("stage", "forbidden"),
    (
        ("fresh", "unclassified-action"),
        ("fresh", "same-host-running"),
        ("fresh", "same-host-failed"),
        ("fresh", "same-host-success"),
        ("fresh", "same-host-hold"),
        ("fresh", "same-host-retained-hold"),
        ("fresh", "same-host-retained-action"),
        ("existing-intent", "same-host-success"),
        ("terminal", "same-host-success"),
    ),
)
def test_failure_transition_v2_rejects_unclosed_recovering_host_current_state(
    stage: str,
    forbidden: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    current_id = fleet_rollout.rollout_id(accepted)
    if stage == "fresh":
        current = fleet_rollout.RolloutRun(
            rollout_id=current_id,
            report_digest_sha256=accepted.digest_sha256,
            report_source_path=accepted.source_path,
            complete=False,
        )
    elif stage == "existing-intent":
        _interrupt_failure_recovery_after(
            legacy_failure_transition.WriteCurrentAckAndIntent,
            accepted=accepted,
            kwargs=kwargs,
            monkeypatch=monkeypatch,
        )
        current = fleet_rollout.load_run(current_id)
        assert current is not None
    else:
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
        current = fleet_rollout.load_run(current_id)
        assert current is not None
        assert current.legacy_failure_update_intent is None

    extra_action_id = "local-runtime:host_e:vibe-view"
    if forbidden == "unclassified-action":
        current.actions["unclassified-current-row"] = {"status": "success"}
    elif forbidden.startswith("same-host-") and forbidden.rsplit("-", 1)[-1] in {
        "running",
        "failed",
        "success",
    }:
        current.actions[extra_action_id] = {
            "status": forbidden.rsplit("-", 1)[-1]
        }
    elif forbidden == "same-host-hold":
        current.holds["host_e"] = {
            "host": "host_e",
            "kind": "full",
            "owned": True,
            "status": "released",
        }
    elif forbidden == "same-host-retained-hold":
        current.legacy_retained_holds["host_e"] = {
            "host": "host_e",
            "reason": "must not survive into failure recovery",
        }
    else:
        current.actions[extra_action_id] = {"status": "running"}
        current.legacy_retained_actions[extra_action_id] = {
            "host": "host_e",
            "reason": "must not survive into failure recovery",
        }
    current_path = fleet_rollout.save_run(current)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = {path: path.read_bytes() for path in (old_path, current_path)}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = _inactive_failure_status_runner(calls=controls)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="current|journal|action|hold|retained|host",
    ):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in (old_path, current_path)} == before


def test_failure_transition_v2_classifies_allowed_intent_action_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert current is not None
    intent = current.legacy_failure_update_intent
    assert intent is not None
    old_action_id = str(intent["current_action_ids"][0])
    foreign_action_id = "local-runtime:host_a:vibeqc-queue"
    projection = intent["current_plan_projection"]
    assert isinstance(projection, dict)
    projection_rows = projection["actions"]
    assert isinstance(projection_rows, list)
    for row in projection_rows:
        if row["action_id"] == old_action_id:
            row["action_id"] = foreign_action_id
    projection_rows.sort(key=lambda row: row["action_id"])
    intent["current_action_ids"] = sorted(
        foreign_action_id if item == old_action_id else item
        for item in intent["current_action_ids"]
    )
    intent["current_actions_sha256"] = (
        legacy_failure_transition.canonical_json_sha256(projection)
    )
    reason = str(intent["failed_host_reason"])
    ack_reason = f"skipped: an earlier lane on host_e failed ({reason})"
    rows = {
        action_id: {
            "decision": "update",
            "reason": ack_reason,
            "status": "not-run",
        }
        for action_id in intent["current_action_ids"]
    }
    intent["current_ack_sha256"] = (
        legacy_failure_transition._current_ack_sha256(
            "host_e",
            reason,
            rows,
        )
    )
    current.actions = {
        (foreign_action_id if action_id == old_action_id else action_id): row
        for action_id, row in current.actions.items()
    }
    legacy_failure_transition.parse_failure_update_intent(intent)
    current_path = fleet_rollout.save_run(current)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = {path: path.read_bytes() for path in (old_path, current_path)}
    fixture_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = _inactive_failure_status_runner(calls=controls)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="current|action|host|journal",
    ):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in (old_path, current_path)} == before


@pytest.mark.parametrize(
    "malformed",
    ("allowed-attempts", "retained-holds", "retained-actions"),
)
def test_failure_transition_v2_rejects_malformed_full_current_substate(
    malformed: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    current = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(accepted),
        report_digest_sha256=accepted.digest_sha256,
        report_source_path=accepted.source_path,
        complete=False,
    )
    if malformed == "allowed-attempts":
        current.actions[action.id] = {
            "status": "success",
            "operation_attempts": [{"not": "a durable attempt"}],
        }
    elif malformed == "retained-holds":
        current.legacy_retained_holds = [{"not": "a retained hold map"}]  # type: ignore[assignment]
    else:
        current.legacy_retained_actions = [{"not": "a retained action map"}]  # type: ignore[assignment]
    current_path = fleet_rollout.save_run(current)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = {path: path.read_bytes() for path in (old_path, current_path)}
    fixture_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="current|journal|operation|retained|action|hold",
    ):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=_inactive_failure_status_runner(calls=controls),
            ),
        )
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in (old_path, current_path)} == before


def test_failure_transition_v2_rejects_source_disjoint_same_host_intent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=_inactive_failure_status_runner(),
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    current_id = fleet_rollout.rollout_id(accepted)
    current = fleet_rollout.load_run(current_id)
    assert current is not None
    competing, _report = _competing_failure_current(current, accepted)
    assert competing.legacy_failure_update_intent is not None
    competing.legacy_failure_update_intent["sources"][0].update(
        {
            "historical_rollout_id": "v0.15.1-cccccccccccc",
            "historical_report_path": "vibe-queue/releases/v0.15.1.json",
            "historical_report_digest_sha256": "c" * 64,
        }
    )
    legacy_failure_transition.parse_failure_update_intent(
        competing.legacy_failure_update_intent
    )
    competing_path = fleet_rollout.save_run(competing)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(current_id)
    paths = (old_path, current_path, competing_path)
    before = {path: path.read_bytes() for path in paths}
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []
    controls: list[list[str]] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = _inactive_failure_status_runner(calls=controls)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="intent|context|host"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert controls == []
    assert writes == []
    assert {path: path.read_bytes() for path in paths} == before


def test_failure_transition_v2_rechecks_released_live_hold_before_later_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    _forbid_failure_transition_execution(monkeypatch)
    hold = host_e.holds["host_e"]
    reason = str(hold["reason"])
    set_at = str(hold["set_at"])
    status_count = 0
    releases = 0
    controls: list[list[str]] = []

    def control(argv: list[str], **unused: Any) -> subprocess.CompletedProcess[str]:
        nonlocal releases, status_count
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            status_count += 1
            status = _inactive_full_drain_status()
            if status_count == 1 or status_count >= 3:
                status.update(
                    {
                        "active": True,
                        "mode": "full",
                        "is_full_drain": True,
                        "state": {"reason": reason, "set_at": set_at},
                        "submit_policy": "deny",
                    }
                )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(status),
                stderr="",
            )
        releases += 1
        assert releases == 1
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    old_before = old_path.read_bytes()
    fixture_atomic_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_atomic_write(path, text)

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="hold|active|absence|reappear",
    ):
        _call_failure_transition_v2(
            accepted_report=accepted,
            **_failure_transition_kwargs(
                host_e=host_e,
                plan=plan,
                tmp_path=tmp_path,
                control_runner=control,
            ),
        )
    assert controls == [
        ["drain", "--status", "--json", "host_e"],
        [
            "drain",
            "--release",
            "--scheduler-host",
            "host_e",
            "--release-legacy-only",
            "--expected-legacy-set-at",
            set_at,
            "--expected-legacy-reason",
            reason,
            "host_e",
        ],
        ["drain", "--status", "--json", "host_e"],
        ["drain", "--status", "--json", "host_e"],
    ]
    assert releases == 1
    assert writes == []
    assert old_path.read_bytes() == old_before
    assert not current_path.exists()
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    _assert_failure_source_is_pending(persisted)


@pytest.mark.parametrize("initially_active", (False, True))
def test_failure_transition_restart_never_releases_settled_hold_again(
    initially_active: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, _action, _observation, accepted = (
        _isolated_failed_host_e_recovery(tmp_path, monkeypatch)
    )
    hold = host_e.holds["host_e"]
    reason = str(hold["reason"])
    set_at = str(hold["set_at"])
    status_count = 0
    release_count = 0

    def first_control(
        argv: list[str],
        **unused: Any,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal release_count, status_count
        args = argv[3:]
        if "--status" in args:
            status_count += 1
            payload = _inactive_full_drain_status()
            if initially_active and status_count == 1:
                payload.update(
                    active=True,
                    mode="full",
                    is_full_drain=True,
                    state={"reason": reason, "set_at": set_at},
                    submit_policy="deny",
                )
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(payload),
                stderr="",
            )
        release_count += 1
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    kwargs = _failure_transition_kwargs(
        host_e=host_e,
        plan=plan,
        tmp_path=tmp_path,
        control_runner=first_control,
    )
    _interrupt_failure_recovery_after(
        legacy_failure_transition.WriteCurrentAckAndIntent,
        accepted=accepted,
        kwargs=kwargs,
        monkeypatch=monkeypatch,
    )
    assert release_count == int(initially_active)
    old_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    current_path = fleet_rollout.rollout_state_path(fleet_rollout.rollout_id(accepted))
    before = {path: path.read_bytes() for path in (old_path, current_path)}
    fixture_write = fleet_rollout.paths.atomic_write_text
    writes: list[Path] = []

    def adapter_write(path: Path, text: str) -> None:
        writes.append(path)
        fixture_write(path, text)

    def reappeared(
        argv: list[str],
        **unused: Any,
    ) -> subprocess.CompletedProcess[str]:
        args = argv[3:]
        if "--release" in args:
            pytest.fail("restart attempted a second conditional release")
        payload = _inactive_full_drain_status()
        payload.update(
            active=True,
            mode="full",
            is_full_drain=True,
            state={"reason": reason, "set_at": set_at},
            submit_policy="deny",
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    monkeypatch.setattr(fleet_rollout.paths, "atomic_write_text", adapter_write)
    kwargs["control_runner"] = reappeared
    with pytest.raises(fleet_rollout.FleetRolloutError, match="hold|active|absence"):
        _call_failure_transition_v2(accepted_report=accepted, **kwargs)
    assert release_count == int(initially_active)
    assert writes == []
    assert {path: path.read_bytes() for path in (old_path, current_path)} == before


@pytest.mark.parametrize(
    "offline_failure",
    ["nonzero", "oserror", "subprocess-error"],
)
def test_legacy_full_hold_batch_settles_safe_host_while_peer_is_offline(
    offline_failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan = _install_obsolete_legacy_full_hold_pair(tmp_path, monkeypatch)

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "explicit legacy inventory must not inspect one host before the "
            "other host is durably eligible"
        ),
    )

    assert set(inventory.legacy_hold_retries) == {
        (host_a.rollout_id, "host_a"),
        (host_e.rollout_id, "host_e"),
    }
    controls: list[list[str]] = []

    def control(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        if args[-1] == "host_a":
            if offline_failure == "oserror":
                raise OSError("network unreachable")
            if offline_failure == "subprocess-error":
                raise subprocess.TimeoutExpired(argv, 10)
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unreachable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((host_e.rollout_id, "host_e"),)
    assert result.retained_holds[0][:2] == (host_a.rollout_id, "host_a")
    assert all("--release" not in args for args in controls)
    persisted_host_a = fleet_rollout.load_run(host_a.rollout_id)
    persisted_host_e = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted_host_a is not None
    assert persisted_host_e is not None
    assert persisted_host_a.holds["host_a"]["status"] == "active"
    assert "host_a" in persisted_host_a.legacy_retained_holds
    assert persisted_host_e.holds["host_e"]["status"] == "released"
    assert "host_e" not in persisted_host_e.legacy_retained_holds


@pytest.mark.parametrize(
    "recovery_mode",
    [
        "normal",
        "crash",
        "report-change",
        "tamper",
        "stale-tamper-target",
        "stale-tamper-reason",
        "stale-tamper-observed",
    ],
)
def test_legacy_full_hold_batch_settles_inactive_failed_operation_host(
    recovery_mode: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A terminal failed sibling must not hide an obsolete inactive hold."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, observation = (
        _install_failed_exact_host_e_pair(
            tmp_path,
            monkeypatch,
        )
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "explicit legacy inventory must remain host-control free"
        ),
    )

    assert set(inventory.legacy_hold_retries) == {
        (host_a.rollout_id, "host_a"),
        (host_e.rollout_id, "host_e"),
    }
    assert inventory.failed_operation_hosts[0][:2] == (
        host_e.rollout_id,
        "host_e",
    )
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        if args[-1] == "host_a":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unreachable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    original_consume = fleet_rollout.consume_reconciled_failure_fences

    consume_calls = 0

    def consume_after_skip(
        failures: Sequence[tuple[str, str, str]], **kwargs: Any
    ) -> fleet_rollout.RolloutRun | None:
        nonlocal consume_calls
        persisted = fleet_rollout.load_run(host_e.rollout_id)
        assert persisted is not None
        action_record = persisted.actions[host_e_action.id]
        assert action_record["legacy_failure_skip"]["schema"] == (
            "vq.fleet.legacy_failure_skip/1"
        )
        attempt = action_record["operation_attempts"][0]
        assert attempt["failure_fence_consumed"] is False
        consume_calls += 1
        if recovery_mode != "normal" and consume_calls == 1:
            raise RuntimeError("simulated death after atomic skip journal")
        return original_consume(failures, **kwargs)

    monkeypatch.setattr(
        fleet_rollout,
        "consume_reconciled_failure_fences",
        consume_after_skip,
    )
    recovery_kwargs = {
        "legacy_running_actions": (),
        "legacy_hold_retries": inventory.legacy_hold_retries,
        "failed_operation_hosts": inventory.failed_operation_hosts,
        "plan": plan,
        "admin_status": {},
        "repo": tmp_path,
        "current_report_digest_resolver": lambda: str(
            plan.report["digest_sha256"]
        ),
        "control_runner": control,
    }
    if recovery_mode != "normal":
        with pytest.raises(
            RuntimeError, match="simulated death after atomic skip journal"
        ):
            fleet_rollout.reconcile_legacy_rollout_state(**recovery_kwargs)
        interrupted = fleet_rollout.load_run(host_e.rollout_id)
        assert interrupted is not None
        assert interrupted.holds["host_e"]["status"] == "released"
        interrupted_attempt = interrupted.actions[host_e_action.id][
            "operation_attempts"
        ][0]
        assert interrupted_attempt["failure_fence_consumed"] is False
        if recovery_mode == "tamper":
            interrupted.actions[host_e_action.id]["legacy_failure_skip"][
                "current_target_sha"
            ] = "0" * 40
            fleet_rollout.save_run(interrupted)
        elif recovery_mode.startswith("stale-tamper-"):
            evidence = interrupted.actions[host_e_action.id][
                "legacy_failure_skip"
            ]
            field = recovery_mode.removeprefix("stale-tamper-")
            if field == "target":
                evidence["current_target_sha"] = "0" * 40
            elif field == "reason":
                evidence["current_action_reason"] = "healthy enough"
            else:
                evidence["observed_current_sha"] = "0" * 40
            fleet_rollout.save_run(interrupted)
        if recovery_mode == "report-change" or recovery_mode.startswith(
            "stale-tamper-"
        ):
            plan = fleet_rollout.RolloutPlan(
                driver=plan.driver,
                report={
                    "source_path": "vibe-queue/releases/v0.15.142.json",
                    "digest_sha256": "e" * 64,
                },
                topology=plan.topology,
                actions=plan.actions,
            )
            recovery_kwargs["plan"] = plan
            recovery_kwargs["current_report_digest_resolver"] = lambda: str(
                plan.report["digest_sha256"]
            )
        restart_inventory = fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report["digest_sha256"]),
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=False,
            allow_legacy_reconciliation=True,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "restart inventory must remain host-control free"
            ),
        )
        assert (host_e.rollout_id, "host_e") not in (
            restart_inventory.legacy_hold_retries
        )
        recovery_kwargs["legacy_hold_retries"] = (
            restart_inventory.legacy_hold_retries
        )
        recovery_kwargs["failed_operation_hosts"] = (
            restart_inventory.failed_operation_hosts
        )
        if recovery_mode == "tamper" or recovery_mode.startswith(
            "stale-tamper-"
        ):
            with pytest.raises(
                fleet_rollout.FleetRolloutError,
                match="malformed or tampered persisted skip evidence",
            ):
                fleet_rollout.reconcile_legacy_rollout_state(
                    **recovery_kwargs
                )
            tampered = fleet_rollout.load_run(host_e.rollout_id)
            assert tampered is not None
            tampered_attempt = tampered.actions[host_e_action.id][
                "operation_attempts"
            ][0]
            assert tampered_attempt["failure_fence_consumed"] is False
            return
    result = fleet_rollout.reconcile_legacy_rollout_state(**recovery_kwargs)

    assert result.settled_inactive_holds == (() if recovery_mode != "normal" else (
        (host_e.rollout_id, "host_e"),
    ))
    assert result.retained_holds[0][:2] == (host_a.rollout_id, "host_a")
    assert all("--release" not in args for args in controls)
    persisted_host_e = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted_host_e is not None
    assert persisted_host_e.holds["host_e"]["status"] == "released"
    failed_attempt = persisted_host_e.actions[host_e_action.id][
        "operation_attempts"
    ][0]
    assert failed_attempt["failure_fence_consumed"] is True
    assert failed_attempt["failure_retry_authorized"] is False
    if recovery_mode == "report-change":
        assert persisted_host_e.actions[host_e_action.id][
            "legacy_failure_skip"
        ]["current_report_digest_sha256"] == "e" * 64

    dry_run = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "post-recovery dry-run must remain read-only"
        ),
    )
    assert dry_run.failed_operation_hosts == ()
    assert dry_run.retained_legacy_holds[0][:2] == (
        host_a.rollout_id,
        "host_a",
    )
    assert all(
        host != "host_e"
        for _rollout_id, host, _reason in dry_run.retained_legacy_holds
    )


@pytest.mark.parametrize(
    "tamper",
    [
        "wrong-control",
        "wrong-reason",
        "bad-duration",
        "short-duration",
        "missing-duration",
        "bad-preexisting",
        "preexisting-true",
        "missing-preexisting",
        "extra-field",
    ],
)
def test_failed_host_bridge_rejects_malformed_exact_full_hold(
    tamper: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    hold = host_e.holds["host_e"]
    if tamper == "wrong-control":
        hold["control_host"] = "host_a"
    elif tamper == "wrong-reason":
        hold["reason"] = "tampered-owner"
    elif tamper == "bad-duration":
        hold["duration_seconds"] = "21600"
    elif tamper == "short-duration":
        hold["duration_seconds"] = 21599
    elif tamper == "missing-duration":
        hold.pop("duration_seconds")
    elif tamper == "bad-preexisting":
        hold["preexisting"] = "false"
    elif tamper == "preexisting-true":
        hold["preexisting"] = True
    elif tamper == "missing-preexisting":
        hold.pop("preexisting")
    else:
        hold["unexpected"] = True
    fleet_rollout.save_run(host_e)
    state_paths = [
        fleet_rollout.rollout_state_path(run.rollout_id)
        for run in (host_a, host_e)
    ]
    before = [path.read_bytes() for path in state_paths]
    controls: list[list[str]] = []

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="malformed or ambiguous exact full hold on host_e",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report["digest_sha256"]),
            current_report_source_path=str(plan.report["source_path"]),
            inspect_only=False,
            allow_legacy_reconciliation=True,
            control_runner=lambda argv, **kwargs: (
                controls.append(argv)
                or subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")
            ),
        )

    assert controls == []
    assert [path.read_bytes() for path in state_paths] == before
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    attempt = persisted.actions[host_e_action.id]["operation_attempts"][0]
    assert attempt["failure_fence_consumed"] is False


@pytest.mark.parametrize("tamper", ["wrong-control", "short-duration"])
def test_receipted_exact_full_retry_validates_before_host_control(
    tamper: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _host_a, host_e, plan, _action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    hold = host_e.holds["host_e"]
    host_e.complete = False
    host_e.legacy_retained_holds["host_e"] = (
        fleet_rollout._legacy_retained_hold_receipt(
            host_e,
            host="host_e",
            hold=hold,
            plan=plan,
            source="hold-observation",
            control_host="host_e",
            retained_at="2026-08-08T12:01:00+00:00",
            reason="network unavailable",
        )
    )
    if tamper == "wrong-control":
        hold["control_host"] = "host_a"
    else:
        hold["duration_seconds"] = 1
    host_e.legacy_retained_holds["host_e"]["hold_record_sha256"] = (
        fleet_rollout._legacy_subtree_sha256(hold)
    )
    fleet_rollout.save_run(host_e)
    controls: list[list[str]] = []

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="malformed or ambiguous exact full hold on host_e",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=((host_e.rollout_id, "host_e"),),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: str(
                plan.report["digest_sha256"]
            ),
            control_runner=lambda argv, **kwargs: (
                controls.append(argv)
                or subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")
            ),
        )

    assert controls == []


def test_failed_host_bridge_keeps_recovered_legacy_full_classification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _host_a, host_e, plan, _action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    host_e.holds["host_e"].pop("duration_seconds")
    host_e.holds["host_e"].pop("control_host")
    fleet_rollout.save_run(host_e)

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "legacy inventory must not issue host controls"
        ),
    )

    assert (host_e.rollout_id, "host_e") in inventory.legacy_hold_retries


def test_failure_only_recovery_ignores_harvested_successful_sibling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, failed = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    success_identity = fleet_rollout._operation_identity(
        host_e.rollout_id,
        host_e.report_digest_sha256,
        host_e_action,
        attempt=2,
    )
    success = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(success_identity),
        identity=success_identity,
        request_sha256="d" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "success", "executed": True, "returncode": 0},
    )
    host_e.actions[host_e_action.id]["operation_attempts"].append(
        fleet_rollout._attempt_ref(
            success,
            success_identity,
            request_sha256=success.request_sha256,
            harvested=True,
        )
    )
    host_e.holds = {}
    fleet_rollout.save_run(host_e)
    fleet_rollout.rollout_state_path(host_a.rollout_id).unlink()
    observations = {failed.operation_id: failed, success.operation_id: success}
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "list_operation_ids",
        lambda *, recover: list(observations),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
    )
    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        failed_operation_hosts=inventory.failed_operation_hosts,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "failure-only recovery must issue no controls"
        ),
    )

    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    attempts = persisted.actions[host_e_action.id]["operation_attempts"]
    assert attempts[0]["failure_fence_consumed"] is True
    assert attempts[1]["failure_fence_consumed"] is False


def test_unhealthy_skip_still_settles_inactive_hold_beside_offline_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
    )
    host_e_action.decision = "block"
    host_e_action.reason = "current lane is not healthy"
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        if argv[-1] == "host_a":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unavailable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        failed_operation_hosts=inventory.failed_operation_hosts,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((host_e.rollout_id, "host_e"),)
    assert result.retained_holds[0][:2] == (host_a.rollout_id, "host_a")
    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_e"]["status"] == "released"
    record = persisted.actions[host_e_action.id]
    assert "legacy_failure_skip" not in record
    assert record["operation_attempts"][0]["failure_fence_consumed"] is False


def test_full_hold_observation_cas_rejects_concurrent_journal_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _host_a, host_e, plan, _action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        changed = fleet_rollout.load_run(host_e.rollout_id)
        assert changed is not None
        changed.failed_hosts["concurrent"] = "newer journal writer"
        fleet_rollout.save_run(changed)
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="changed during host-local recovery",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=((host_e.rollout_id, "host_e"),),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: str(
                plan.report["digest_sha256"]
            ),
            control_runner=control,
        )

    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    assert persisted.failed_hosts["concurrent"] == "newer journal writer"
    assert persisted.holds["host_e"]["status"] == "active"


@pytest.mark.parametrize(
    "retry_order",
    [("alpha", "beta"), ("beta", "alpha")],
)
def test_same_run_full_hold_candidates_commit_independently(
    retry_order: tuple[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical, run = _obsolete_legacy_full_hold(
        host="alpha",
        version=(0, 15, 137),
        digest="b" * 64,
    )
    run.actions["local-runtime:beta:vibeqc-release"] = {"status": "success"}
    run.holds["beta"] = {
        "host": "beta",
        "kind": "full",
        "owned": True,
        "preexisting": False,
        "reason": fleet_rollout._hold_reason(run.rollout_id, "beta"),
        "status": "active",
    }
    fleet_rollout.save_run(run)
    plan = _legacy_full_hold_recovery_plan(("alpha", "beta"))
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if argv[-1] == "alpha":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unavailable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=tuple(
            (run.rollout_id, host) for host in retry_order
        ),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((run.rollout_id, "beta"),)
    assert result.retained_holds[0][:2] == (run.rollout_id, "alpha")
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["alpha"]["status"] == "active"
    assert persisted.holds["beta"]["status"] == "released"
    assert set(persisted.legacy_retained_holds) == {"alpha"}


@pytest.mark.parametrize("invalid_failure", ["successful", "tampered"])
def test_failure_batch_prevalidates_all_rows_before_first_hold_control(
    invalid_failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, _host_e_action, failed = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    host_a_action = next(action for action in plan.actions if action.host == "host_a")
    host_a_identity = fleet_rollout._operation_identity(
        host_a.rollout_id,
        host_a.report_digest_sha256,
        host_a_action,
        attempt=1,
    )
    successful = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(host_a_identity),
        identity=host_a_identity,
        request_sha256="d" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "success", "executed": True, "returncode": 0},
    )
    host_a_attempt = fleet_rollout._attempt_ref(
        successful,
        host_a_identity,
        request_sha256=successful.request_sha256,
        harvested=True,
    )
    if invalid_failure == "tampered":
        host_a_attempt["request_sha256"] = "e" * 64
    host_a.actions[host_a_action.id] = {
        "status": "success",
        "operation_attempts": [host_a_attempt],
    }
    fleet_rollout.save_run(host_a)
    observations = {
        failed.operation_id: failed,
        successful.operation_id: successful,
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )
    state_paths = [
        fleet_rollout.rollout_state_path(run.rollout_id)
        for run in (host_a, host_e)
    ]
    before = [path.read_bytes() for path in state_paths]
    controls: list[list[str]] = []

    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=((host_e.rollout_id, "host_e"),),
            failed_operation_hosts=(
                (host_e.rollout_id, "host_e", "executed failure"),
                (host_a.rollout_id, "host_a", "not actually a failure"),
            ),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: str(
                plan.report["digest_sha256"]
            ),
            control_runner=lambda argv, **kwargs: (
                controls.append(argv)
                or subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")
            ),
        )

    assert controls == []
    assert [path.read_bytes() for path in state_paths] == before


@pytest.mark.parametrize("peer_change", ["report-identity", "remove-hold"])
def test_failure_candidate_rebinds_report_and_host_hold_after_peer_control(
    peer_change: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, _host_e_action, host_e_failed = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    host_a_action = next(action for action in plan.actions if action.host == "host_a")
    host_a_identity = fleet_rollout._operation_identity(
        host_a.rollout_id,
        host_a.report_digest_sha256,
        host_a_action,
        attempt=1,
    )
    host_a_failed = fleet_operation.OperationObservation(
        operation_id=fleet_operation.operation_id(host_a_identity),
        identity=host_a_identity,
        request_sha256="d" * 64,
        state="completed",
        retry_safe=False,
        lease_busy=False,
        request={},
        ready={},
        authorization={},
        activation={},
        result={"status": "failed", "executed": True, "returncode": 7},
    )
    host_a.actions[host_a_action.id] = {
        "status": "failed",
        "operation_attempts": [
            fleet_rollout._attempt_ref(
                host_a_failed,
                host_a_identity,
                request_sha256=host_a_failed.request_sha256,
                harvested=True,
            )
        ],
    }
    fleet_rollout.save_run(host_a)
    observations = {
        host_a_failed.operation_id: host_a_failed,
        host_e_failed.operation_id: host_e_failed,
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation, *, recover=True: observations[operation],
    )
    host_a_path = fleet_rollout.rollout_state_path(host_a.rollout_id)
    peer_bytes: bytes | None = None
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal peer_bytes
        del kwargs
        controls.append(argv[3:])
        changed = fleet_rollout.load_run(host_a.rollout_id)
        assert changed is not None
        if peer_change == "report-identity":
            changed.report_source_path = "vibe-queue/releases/v0.15.136.json"
        else:
            changed.holds.pop("host_a")
        fleet_rollout.save_run(changed)
        peer_bytes = host_a_path.read_bytes()
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="changed after global prevalidation",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=((host_e.rollout_id, "host_e"),),
            failed_operation_hosts=(
                (host_a.rollout_id, "host_a", "executed failure"),
            ),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: str(
                plan.report["digest_sha256"]
            ),
            control_runner=control,
        )

    assert controls == [["drain", "--status", "--json", "host_e"]]
    assert peer_bytes is not None
    assert host_a_path.read_bytes() == peer_bytes
    persisted = fleet_rollout.load_run(host_a.rollout_id)
    assert persisted is not None
    record = persisted.actions[host_a_action.id]
    assert "legacy_failure_skip" not in record
    assert record["operation_attempts"][0]["failure_fence_consumed"] is False


def test_failure_only_legacy_recovery_persists_skip_before_consuming_fence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    fleet_rollout.rollout_state_path(host_a.rollout_id).unlink()
    host_e.holds = {}
    fleet_rollout.save_run(host_e)

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "failure-only inventory must issue no controls"
        ),
    )
    assert inventory.legacy_hold_retries == ()
    assert inventory.failed_operation_hosts[0][:2] == (
        host_e.rollout_id,
        "host_e",
    )

    original_consume = fleet_rollout.consume_reconciled_failure_fences

    def consume_after_skip(
        failures: Sequence[tuple[str, str, str]], **kwargs: Any
    ) -> fleet_rollout.RolloutRun | None:
        persisted = fleet_rollout.load_run(host_e.rollout_id)
        assert persisted is not None
        record = persisted.actions[host_e_action.id]
        assert record["legacy_failure_skip"]["current_report_digest_sha256"] == (
            plan.report["digest_sha256"]
        )
        assert record["operation_attempts"][0]["failure_fence_consumed"] is False
        return original_consume(failures, **kwargs)

    monkeypatch.setattr(
        fleet_rollout,
        "consume_reconciled_failure_fences",
        consume_after_skip,
    )
    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        failed_operation_hosts=inventory.failed_operation_hosts,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "failure-only recovery must issue no controls"
        ),
    )

    persisted = fleet_rollout.load_run(host_e.rollout_id)
    assert persisted is not None
    attempt = persisted.actions[host_e_action.id]["operation_attempts"][0]
    assert attempt["failure_fence_consumed"] is True
    assert attempt["failure_retry_authorized"] is False
    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "post-recovery dry-run must remain read-only"
        ),
    )


def test_legacy_full_hold_settlement_survives_later_host_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan = _install_obsolete_legacy_full_hold_pair(
        tmp_path, monkeypatch
    )
    controls: list[str] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        host = argv[-1]
        controls.append(host)
        if host == "host_a":
            raise RuntimeError("simulated controller crash")
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    with pytest.raises(RuntimeError, match="simulated controller crash"):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=(
                (host_e.rollout_id, "host_e"),
                (host_a.rollout_id, "host_a"),
            ),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: str(
                plan.report["digest_sha256"]
            ),
            control_runner=control,
        )

    assert controls == ["host_e", "host_a"]
    persisted_host_e = fleet_rollout.load_run(host_e.rollout_id)
    persisted_host_a = fleet_rollout.load_run(host_a.rollout_id)
    assert persisted_host_e is not None
    assert persisted_host_a is not None
    assert persisted_host_e.holds["host_e"]["status"] == "released"
    assert persisted_host_a.holds["host_a"]["status"] == "active"


@pytest.mark.parametrize("active", [False, True])
def test_legacy_full_hold_rechecks_current_report_after_observation(
    active: bool,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    _host_a, host_e, plan = _install_obsolete_legacy_full_hold_pair(
        tmp_path, monkeypatch
    )
    state_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    before = state_path.read_bytes()
    current_digest = str(plan.report["digest_sha256"])
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal current_digest
        del kwargs
        args = argv[3:]
        controls.append(args)
        assert "--status" in args
        current_digest = "e" * 64
        payload = _inactive_full_drain_status()
        if active:
            payload = {
                "active": True,
                "mode": "full",
                "is_full_drain": True,
                "is_scheduler_target_drain": False,
                "scheduler_hosts": [],
                "scheduler_leases": [],
                "orphaned_scheduler_leases": [],
                "scheduler_leases_error": None,
                "submit_policy": "deny",
                "state": {
                    "reason": fleet_rollout._hold_reason(
                        host_e.rollout_id, "host_e"
                    ),
                    "set_at": "2026-08-08T12:00:00+00:00",
                },
            }
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(payload),
            stderr="",
        )

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="accepted fleet report changed after planning",
    ):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(),
            legacy_hold_retries=((host_e.rollout_id, "host_e"),),
            plan=plan,
            admin_status={},
            repo=tmp_path,
            current_report_digest_resolver=lambda: current_digest,
            control_runner=control,
        )

    assert state_path.read_bytes() == before
    assert controls == [["drain", "--status", "--json", "host_e"]]


def test_pairless_legacy_full_hold_with_recovered_set_at_reobserves_inactive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical, run = _obsolete_legacy_full_hold(
        host="host_e",
        version=(0, 15, 137),
        digest="b" * 64,
    )
    run.holds["host_e"]["set_at"] = "2026-08-08T12:00:00+00:00"
    fleet_rollout.save_run(run)
    plan = _legacy_full_hold_recovery_plan(("host_e",))
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        assert "--status" in args
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=((run.rollout_id, "host_e"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "f" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == (
        (run.rollout_id, "host_e"),
    )
    assert controls == [["drain", "--status", "--json", "host_e"]]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_e"]["status"] == "released"


def test_legacy_full_hold_restart_is_idempotent_and_scoped_to_retained_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan = _install_obsolete_legacy_full_hold_pair(
        tmp_path, monkeypatch
    )
    first_inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "inventory must remain host-control free"
        ),
    )

    def first_control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if argv[-1] == "host_a":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unreachable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=first_inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=first_control,
    )
    host_e_path = fleet_rollout.rollout_state_path(host_e.rollout_id)
    settled_host_e = host_e_path.read_bytes()

    restart_inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "restart inventory must remain host-control free"
        ),
    )
    assert restart_inventory.legacy_hold_retries == (
        (host_a.rollout_id, "host_a"),
    )
    restart_controls: list[str] = []
    restarted = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=restart_inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=lambda argv, **kwargs: (
            restart_controls.append(argv[-1])
            or subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unreachable"
            )
        ),
    )
    assert restarted.settled_inactive_holds == ()
    assert restarted.retained_holds[0][:2] == (host_a.rollout_id, "host_a")
    assert restart_controls == ["host_a"]
    assert host_e_path.read_bytes() == settled_host_e

    read_only = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "read-only scoped planning must issue no host controls"
        ),
    )
    assert read_only.retained_legacy_holds[0][:2] == (
        host_a.rollout_id,
        "host_a",
    )
    fenced = fleet_rollout.fence_retained_legacy_holds(
        plan, read_only.retained_legacy_holds
    )
    restricted = fleet_rollout.restrict_plan(
        fenced,
        fleet_rollout.HostSelection(only=("host_e",)),
    )
    assert [action.host for action in restricted.actions] == ["host_e"]
    assert restricted.actions[0].decision == "skip"
    assert restricted.retained_legacy_fences == []
    assert restricted.has_deferred is False

    refused = fleet_rollout.restrict_plan(
        fenced,
        fleet_rollout.HostSelection(only=("host_a",)),
    )
    assert [action.host for action in refused.actions] == ["host_a"]
    assert refused.actions[0].decision == "defer"
    assert [
        item["host"] for item in refused.retained_legacy_fences
    ] == ["host_a"]
    assert refused.has_deferred is True


def test_retained_legacy_hold_fences_only_affected_host_and_blocks_driver() -> None:
    report = _report()
    actions = [
        _safe_durable_action(),
        _safe_durable_action(),
        _safe_durable_action(),
    ]
    actions[0].id = "driver"
    actions[0].phase = "driver"
    actions[0].host = "localhost"
    actions[0].decision = "update"
    actions[1].id = "local-runtime:host_a:vibeqc-release"
    actions[1].host = "host_a"
    actions[1].decision = "block"
    actions[2].id = "local-runtime:host_b:vibeqc-release"
    actions[2].host = "host_b"
    actions[2].decision = "update"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(report),
        topology={"excluded": {"role": "excluded"}},
        actions=actions,
        provenance_lanes=[
            fleet_rollout.ProvenanceLane(
                id="root-daemon:host_a",
                host="host_a",
                component="vibeqc-queue-root",
                kind="root-daemon",
                target_sha=FUTURE,
                target_version="0.25.0",
                target_source_tree_sha256="a" * 64,
                current_sha=None,
                current_version=None,
                current_source_tree_sha256=None,
                decision="block",
                reason="unreachable",
                applicable=None,
                evidence={},
            )
        ],
        topology_errors=["host_a: network unavailable"],
    )

    fenced = fleet_rollout.fence_retained_legacy_holds(
        plan,
        (
            ("old-host_a", "host_a", "network unavailable"),
            ("old-driver", "localhost", "identity unknown"),
            ("old-excluded", "excluded", "not in current topology"),
        ),
    )

    by_id = {action.id: action for action in fenced.actions}
    assert by_id["driver"].decision == "block"
    assert by_id["local-runtime:host_a:vibeqc-release"].decision == "defer"
    assert by_id["local-runtime:host_b:vibeqc-release"].decision == "update"
    assert "old-host_a" in by_id["local-runtime:host_a:vibeqc-release"].reason
    assert fenced.provenance_lanes[0].decision == "defer"
    assert fenced.topology_errors == []
    assert {item["host"] for item in fenced.retained_legacy_fences} == {
        "localhost",
        "host_a",
        "excluded",
    }
    assert "old-excluded" in fleet_rollout.render_plan_text(
        fenced, title="dry run"
    )
    assert fenced.as_dict()["retained_legacy_fences"]
    restricted = fleet_rollout.restrict_plan(
        fenced,
        fleet_rollout.HostSelection(only=("excluded",)),
    )
    assert restricted.retained_legacy_fences[0]["host"] == "excluded"
    assert restricted.has_deferred is True
    verify = fleet_rollout.verify_payload(
        plan=restricted,
        doctor={},
        rollout_id="current",
    )
    assert verify["status"] == "deferred"
    assert verify["verdict"] == "degraded"
    assert verify["retained_legacy_fences"][0]["host"] == "excluded"
    assert "retained legacy rollout" in verify["degraded_hosts"]["excluded"][0]


@pytest.mark.parametrize(
    "bad_actions",
    [
        "not-an-object",
        {"local-runtime:host_b:vibeqc-release": "garbage"},
        {"local-runtime:host_b:vibeqc-release": {}},
        {"local-runtime:host_b:vibeqc-release": {"status": "unknown"}},
        {"local-runtime:host_b:vibeqc-release": {"status": 3}},
    ],
)
def test_malformed_action_journal_cannot_release_an_exact_live_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bad_actions: object,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-malformed-action",
        report_digest_sha256="b" * 64,
        report_source_path="report.json",
        actions={},
        holds={
            "host_b": {
                "host": "host_b",
                "kind": "full",
                "owned": True,
                "preexisting": False,
                "status": "active",
                "reason": fleet_rollout._hold_reason(
                    "v0.15.60-malformed-action", "host_b"
                ),
                "set_at": "2026-08-08T12:00:00+00:00",
            }
        },
    )
    path = fleet_rollout.save_run(run)
    payload = json.loads(path.read_text())
    payload["actions"] = bad_actions
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    before = path.read_bytes()
    controls: list[list[str]] = []

    def control(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv)
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    with pytest.raises(fleet_rollout.FleetRolloutError, match="action"):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            control_runner=control,
        )

    assert controls == []
    assert path.read_bytes() == before


def test_inactive_settled_hold_is_never_reprocessed_against_a_later_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-settled-inactive"
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256="b" * 64,
        report_source_path="report.json",
        actions={"local-runtime:host_b:vibeqc-release": {"status": "success"}},
        holds={
            "host_b": {
                "host": "host_b",
                "kind": "full",
                "owned": True,
                "preexisting": False,
                "status": "active",
                "reason": fleet_rollout._hold_reason(rollout, "host_b"),
            }
        },
    )
    fleet_rollout.save_run(run)
    inactive = {
        "active": False,
        "mode": "inactive",
        "is_full_drain": False,
        "is_scheduler_target_drain": False,
        "scheduler_hosts": [],
        "scheduler_leases": [],
        "orphaned_scheduler_leases": [],
        "scheduler_leases_error": None,
        "state": None,
        "submit_policy": "accept_pending",
    }
    controls: list[list[str]] = []

    def settle(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv)
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(inactive), stderr=""
        )

    fleet_rollout._release_reconciled_holds(run, {}, runner=settle)
    assert len(controls) == 1
    settled = fleet_rollout.load_run(rollout)
    assert settled is not None
    assert settled.holds["host_b"]["status"] == "released"
    state_path = fleet_rollout.rollout_state_path(rollout)
    before = state_path.read_bytes()

    fleet_rollout._release_reconciled_holds(
        settled,
        {},
        runner=lambda *args, **kwargs: pytest.fail(
            "terminal hold history must not inspect a later operator drain"
        ),
    )

    assert state_path.read_bytes() == before


def test_legacy_exact_active_full_hold_uses_only_conditional_pair_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_a")
    fleet_rollout.save_run(run)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    controls: list[list[str]] = []
    set_at = "2026-08-08T12:00:00+00:00"
    released = False

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal released
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": not released,
                        "mode": "full" if not released else "inactive",
                        "is_full_drain": not released,
                        "is_scheduler_target_drain": False,
                        "scheduler_hosts": [],
                        "scheduler_leases": [],
                        "orphaned_scheduler_leases": [],
                        "scheduler_leases_error": None,
                        "submit_policy": (
                            "deny" if not released else "accept_pending"
                        ),
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    run.rollout_id, "host_b"
                                ),
                                "set_at": set_at,
                            }
                            if not released
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        released = True
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert controls == [
        ["drain", "--status", "--json", "host_b"],
        [
            "drain",
            "--release-full",
            "--expected-full-reason",
            fleet_rollout._hold_reason(run.rollout_id, "host_b"),
            "--expected-full-set-at",
            set_at,
            "host_b",
        ],
        ["drain", "--status", "--json", "host_b"],
    ]
    assert result.live_hold_state_changed is False
    assert result.live_hold_refresh_required is True
    assert result.settled_inactive_holds == ((run.rollout_id, "host_b"),)
    assert result.released_live_holds == ()
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_b"]["status"] == "released"


@pytest.mark.parametrize("post_release", ["still-active", "unavailable"])
def test_legacy_full_hold_release_requires_authoritative_post_absence(
    post_release: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_a")
    fleet_rollout.save_run(run)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    set_at = "2026-08-08T12:00:00+00:00"
    status_calls = 0

    def active_status() -> dict[str, Any]:
        return {
            "active": True,
            "mode": "full",
            "is_full_drain": True,
            "is_scheduler_target_drain": False,
            "scheduler_hosts": [],
            "scheduler_leases": [],
            "orphaned_scheduler_leases": [],
            "scheduler_leases_error": None,
            "submit_policy": "deny",
            "state": {
                "reason": fleet_rollout._hold_reason(run.rollout_id, "host_b"),
                "set_at": set_at,
            },
        }

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal status_calls
        del kwargs
        args = argv[3:]
        if "--status" in args:
            status_calls += 1
            if status_calls == 2 and post_release == "unavailable":
                return subprocess.CompletedProcess(
                    argv, 1, stdout="", stderr="network unavailable"
                )
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(active_status()), stderr=""
            )
        assert "--release-full" in args
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert status_calls == 2
    assert result.settled_inactive_holds == ()
    assert result.released_live_holds == ()
    assert result.live_hold_state_changed is False
    assert result.live_hold_refresh_required is True
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_b")
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.complete is False
    assert persisted.holds["host_b"]["status"] == "active"
    receipt = persisted.legacy_retained_holds["host_b"]
    assert receipt["hold_record_sha256"] == fleet_rollout._legacy_subtree_sha256(
        persisted.holds["host_b"]
    )


def test_same_host_full_hold_release_authority_survives_controller_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_a")
    fleet_rollout.save_run(run)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    with pytest.raises(RuntimeError, match="controller stopped"):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=(
                (run.rollout_id, next(iter(run.actions))),
            ),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("controller stopped after action save")
            ),
        )

    interrupted = fleet_rollout.load_run(run.rollout_id)
    assert interrupted is not None
    action_id = "local-runtime:host_b:vibeqc-release"
    assert interrupted.actions[action_id]["status"] == "superseded"
    assert interrupted.holds["host_b"]["status"] == "active"

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "restart inventory must issue no host control"
        ),
    )
    assert inventory.legacy_running_actions == ()
    assert inventory.legacy_hold_retries == ((run.rollout_id, "host_b"),)
    set_at = "2026-08-08T12:00:00+00:00"

    def active_status() -> dict[str, Any]:
        return {
            "active": True,
            "mode": "full",
            "is_full_drain": True,
            "is_scheduler_target_drain": False,
            "scheduler_hosts": [],
            "scheduler_leases": [],
            "orphaned_scheduler_leases": [],
            "scheduler_leases_error": None,
            "submit_policy": "deny",
            "state": {
                "reason": fleet_rollout._hold_reason(
                    run.rollout_id, "host_b"
                ),
                "set_at": set_at,
            },
        }

    degraded_plan = copy.deepcopy(plan)
    degraded_plan.actions[0].decision = "update"
    degraded_plan.actions[0].reason = "target is newer"
    degraded_plan.actions[0].before = {
        "configured": True,
        "last_ok": False,
        "current_sha": RELEASE,
    }
    degraded = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=degraded_plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda argv, **kwargs: (
            subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(active_status()),
                stderr="",
            )
            if "--status" in argv
            else pytest.fail(
                "a degraded current lane must not release its full hold"
            )
        ),
    )
    assert degraded.settled_inactive_holds == ()
    assert degraded.retained_holds[0][:2] == (run.rollout_id, "host_b")

    released = False
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        nonlocal released
        del kwargs
        args = argv[3:]
        controls.append(args)
        if "--status" in args:
            payload = (
                _inactive_full_drain_status()
                if released
                else active_status()
            )
            return subprocess.CompletedProcess(
                argv, 0, stdout=json.dumps(payload), stderr=""
            )
        assert "--release-full" in args
        released = True
        return subprocess.CompletedProcess(
            argv, 0, stdout="released\n", stderr=""
        )

    recovered = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert recovered.settled_inactive_holds == (
        (run.rollout_id, "host_b"),
    )
    assert sum("--release-full" in args for args in controls) == 1
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_b"]["status"] == "released"


def test_full_hold_release_rejects_recreated_same_reason_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_a")
    original_set_at = "2026-08-08T12:00:00+00:00"
    run.holds["host_b"]["set_at"] = original_set_at
    action_id = "local-runtime:host_b:vibeqc-release"
    run.actions[action_id] = _persisted_legacy_supersession_evidence(run)
    fleet_rollout.save_run(run)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        assert "--status" in args
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(
                {
                    "active": True,
                    "mode": "full",
                    "is_full_drain": True,
                    "is_scheduler_target_drain": False,
                    "scheduler_hosts": [],
                    "scheduler_leases": [],
                    "orphaned_scheduler_leases": [],
                    "scheduler_leases_error": None,
                    "submit_policy": "deny",
                    "state": {
                        "reason": fleet_rollout._hold_reason(
                            run.rollout_id, "host_b"
                        ),
                        "set_at": "2026-08-09T12:00:00+00:00",
                    },
                }
            ),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=((run.rollout_id, "host_b"),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == ()
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_b")
    assert controls == [["drain", "--status", "--json", "host_b"]]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_b"]["set_at"] == original_set_at
    assert persisted.holds["host_b"]["status"] == "active"


def test_legacy_mixed_holds_retains_active_sibling_without_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )
    set_at = "2026-08-08T12:00:00+00:00"
    controls: list[list[str]] = []

    def status_payload(*, active: bool, host: str) -> dict[str, Any]:
        return {
            "active": active,
            "mode": "full" if active else "inactive",
            "is_full_drain": active,
            "is_scheduler_target_drain": False,
            "scheduler_hosts": [],
            "scheduler_leases": [],
            "orphaned_scheduler_leases": [],
            "scheduler_leases_error": None,
            "submit_policy": "deny" if active else "accept_pending",
            "state": (
                {
                    "reason": fleet_rollout._hold_reason(run.rollout_id, host),
                    "set_at": set_at,
                }
                if active
                else None
            ),
        }

    def control(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        controls.append(args)
        host = args[-1]
        if "--status" in args:
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    status_payload(
                        active=host == "host_a",
                        host=host,
                    )
                ),
                stderr="",
            )
        pytest.fail("a sibling action proof must not authorize live release")

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((run.rollout_id, "host_b"),)
    assert result.released_live_holds == ()
    assert result.live_hold_state_changed is False
    assert result.live_hold_refresh_required is False
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_a")
    release_controls = [args for args in controls if "--release-full" in args]
    assert release_controls == []


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"active": None},
        {
            "active": False,
            "mode": "inactive",
            "is_full_drain": True,
            "is_scheduler_target_drain": False,
            "scheduler_hosts": [],
            "scheduler_leases": [],
            "orphaned_scheduler_leases": [],
            "scheduler_leases_error": None,
            "submit_policy": "accept_pending",
            "state": None,
        },
    ],
)
def test_legacy_hold_requires_coherent_authoritative_inactive_status(
    payload: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.holds.pop("host_a")
    fleet_rollout.save_run(run)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda argv, **kwargs: subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(payload), stderr=""
        ),
    )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_b"]["status"] == "active"
    assert result.settled_inactive_holds == ()
    assert result.released_live_holds == ()
    assert result.retained_holds[0][:2] == (run.rollout_id, "host_b")


def test_legacy_mixed_durable_failure_refuses_before_hold_inspection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report()
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    run.actions["local-runtime:host_a:vibeqc-release"] = {
        "status": "failed",
        "operation_attempts": [{"malformed": "durable sibling"}],
    }
    fleet_rollout.save_run(run)
    before = fleet_rollout.rollout_state_path(run.rollout_id).read_bytes()
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: historical,
    )

    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.reconcile_legacy_rollout_state(
            legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
            plan=plan,
            admin_status={"host_b": {"marker": None, "markers": []}},
            repo=tmp_path,
            current_report_digest_resolver=lambda: "b" * 64,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "mixed durable sibling must block before hold inspection"
            ),
        )

    assert fleet_rollout.rollout_state_path(run.rollout_id).read_bytes() == before


def test_strict_attempt_history_rejects_duplicate_attempts_and_missing_ids() -> None:
    action = _safe_durable_action()
    identity = fleet_rollout._operation_identity(
        "rollout",
        "a" * 64,
        action,
        attempt=1,
    )
    base = {
        "operation_id": fleet_operation.operation_id(identity),
        "request_sha256": "2" * 64,
        "attempt": 1,
        "report_digest_sha256": "a" * 64,
        "identity": identity.as_dict(),
        "state": "prepared",
        "retry_safe": True,
        "harvested": False,
        "failure_fence_consumed": False,
        "failure_retry_authorized": False,
    }
    with pytest.raises(fleet_rollout.FleetRolloutError, match="unique, increasing"):
        fleet_rollout._operation_attempts(
            {"operation_attempts": [base, dict(base)]}
        )
    broken = dict(base)
    broken.pop("operation_id")
    with pytest.raises(fleet_rollout.FleetRolloutError, match="entry fields"):
        fleet_rollout._operation_attempts({"operation_attempts": [broken]})


def test_direct_execute_refuses_a_referenced_request_digest_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-request-mismatch"
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        action,
        attempt=1,
    )
    handle = fleet_operation.prepare_operation(identity)
    observed = fleet_operation.observe_operation(handle.operation_id)
    ref = fleet_rollout._attempt_ref(
        observed,
        identity,
        request_sha256="f" * 64,
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={action.id: {"status": "running", "operation_attempts": [ref]}},
    )
    fleet_rollout.save_run(run)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail("mismatched request must not launch"),
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="does not bind"):
        fleet_rollout.execute_one(plan, action, rollout_id=rollout)


def test_current_pre_authorized_attempt_is_adopted_without_a_second_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-preauth-adopt"
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        action,
        attempt=1,
    )
    handle = fleet_operation.prepare_operation(identity)
    prepared = fleet_operation.observe_operation(handle.operation_id)
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={
            action.id: {
                "decision": "update",
                "reason": action.reason,
                "status": "running",
                "operation_attempts": [
                    fleet_rollout._attempt_ref(
                        prepared,
                        identity,
                        request_sha256=handle.request_sha256,
                    )
                ],
            }
        },
    )
    fleet_rollout.save_run(run)
    launched = fleet_operation.launch_supervisor(handle.operation_id)
    del launched
    fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail("adoption must not launch again"),
    )

    finished = fleet_rollout.execute_one(
        plan,
        action,
        rollout_id=rollout,
        report_digest_resolver=lambda: digest,
    )

    assert finished.actions[action.id]["status"] == "success"
    assert len(finished.actions[action.id]["operation_attempts"]) == 1


def test_stale_report_after_ready_aborts_before_authorization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[action],
    )
    seen = 0

    def current_digest() -> str:
        nonlocal seen
        seen += 1
        return digest if seen < 3 else "b" * 64

    with pytest.raises(fleet_rollout.FleetRolloutError, match="stale report"):
        fleet_rollout.execute_one(
            plan,
            action,
            rollout_id="v0.15.60-stale-after-ready",
            report_digest_resolver=current_digest,
        )

    run = fleet_rollout.load_run("v0.15.60-stale-after-ready")
    assert run is not None
    operation = run.actions[action.id]["operation_attempts"][0]["operation_id"]
    deadline = time.monotonic() + 10
    while True:
        observed = fleet_operation.observe_operation(operation)
        if observed.state == "completed":
            break
        assert time.monotonic() < deadline
        time.sleep(0.01)
    assert observed.authorization is not None
    assert observed.authorization["decision"] == "abort"
    assert observed.activation is None
    assert observed.result is not None
    assert observed.result["executed"] is False


@pytest.mark.parametrize(
    ("journal_action_host", "journal_control_host"),
    [("host_f", "driver-b"), ("other", "wrong-driver")],
)
def test_global_reconcile_preserves_explicit_alias_hold_without_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    journal_action_host: str,
    journal_control_host: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-alias-controller-death"
    digest = "a" * 64
    action = _scheduler_runtime_action("vibeqc-dev")
    identity = fleet_rollout._operation_identity(
        rollout_id,
        digest,
        action,
        attempt=1,
    )
    operation_id = fleet_operation.operation_id(identity)
    observation = fleet_operation.OperationObservation(
        operation_id=operation_id,
        identity=identity,
        request_sha256="c" * 64,
        state="running-authorized",
        retry_safe=False,
        lease_busy=True,
        request={},
        ready={},
        authorization={},
        activation={},
        result=None,
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={
            action.id: {
                "status": "running",
                "operation_attempts": [
                    fleet_rollout._attempt_ref(
                        observation,
                        identity,
                        request_sha256=observation.request_sha256,
                    )
                ],
            }
        },
        holds={
            "host_f-big": {
                "host": "host_f-big",
                "action_host": journal_action_host,
                "kind": "scheduler-target",
                "owned": True,
                "status": "active",
                "reason": fleet_rollout._hold_reason(
                    rollout_id, "host_f-big"
                ),
                "lease_owner": fleet_rollout._hold_lease_owner(
                    rollout_id, "host_f-big"
                ),
                "control_host": journal_control_host,
            }
        },
    )

    fleet_rollout._release_reconciled_holds(
        run,
        {operation_id: observation},
        runner=lambda *args, **kwargs: pytest.fail(
            "global reconciliation must defer explicit alias bindings to a plan"
        ),
    )

    assert run.holds["host_f-big"]["status"] == "active"


def test_malformed_alias_action_host_fails_closed_before_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout_id = "v0.15.60-malformed-alias-action-host"
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout_id,
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "host_f-big": {
                "host": "host_f-big",
                "action_host": [],
                "kind": "scheduler-target",
                "owned": True,
                "status": "active",
                "reason": fleet_rollout._hold_reason(
                    rollout_id, "host_f-big"
                ),
                "lease_owner": fleet_rollout._hold_lease_owner(
                    rollout_id, "host_f-big"
                ),
                "control_host": "driver-b",
            }
        },
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="action_host"):
        fleet_rollout._release_reconciled_holds(
            run,
            {},
            runner=lambda *args, **kwargs: pytest.fail(
                "malformed alias ownership must not reach release"
            ),
        )


def test_reconciliation_releases_exact_hold_left_before_operation_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-hold-before-prepare"
    reason = fleet_rollout._hold_reason(rollout, "host_d")
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={
            "host_d": {
                "host": "host_d",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": reason,
                "set_at": "2026-08-10T20:00:00+00:00",
                "control_host": "host_d",
            }
        },
    )
    fleet_rollout.save_run(run)
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(argv, 0, stdout="released\n", stderr="")

    fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        inspect_only=False,
        control_runner=control,
    )

    assert controls == [
        [
            "drain",
            "--release-full",
            "--expected-full-reason",
            reason,
            "--expected-full-set-at",
            "2026-08-10T20:00:00+00:00",
            "host_d",
        ]
    ]
    recovered = fleet_rollout.load_run(rollout)
    assert recovered is not None
    assert recovered.holds["host_d"]["status"] == "released"


@pytest.mark.parametrize(
    "hold_kind",
    ["local-exact", "scheduler-exact", "scheduler-unconfirmed"],
)
def test_inspect_only_blocks_owned_hold_without_operation_and_changes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hold_kind: str,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-inspect-hold-before-prepare"
    if hold_kind == "local-exact":
        hold = {
            "host": "host_d",
            "kind": "full",
            "owned": True,
            "status": "active",
            "reason": fleet_rollout._hold_reason(rollout, "host_d"),
            "set_at": "2026-08-10T20:00:00+00:00",
            "control_host": "host_d",
        }
    else:
        hold = {
            "host": "host_d",
            "kind": "scheduler-target",
            "owned": True,
            "status": "active",
            "reason": fleet_rollout._hold_reason(rollout, "host_d"),
            "lease_owner": fleet_rollout._hold_lease_owner(
                rollout, "host_d"
            ),
            "control_host": "localhost",
            "legacy_migrated": False,
            "legacy_migration_pending": False,
        }
        if hold_kind == "scheduler-unconfirmed":
            hold["acquire_unconfirmed"] = True
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256="a" * 64,
        report_source_path="report.json",
        holds={"host_d": hold},
    )
    path = fleet_rollout.save_run(run)
    before = (path.read_bytes(), path.stat().st_mtime_ns)

    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="retains an owned hold.*will not release or reconcile",
    ):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="a" * 64,
            inspect_only=True,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "inspect-only must not issue hold control"
            ),
        )

    assert (path.read_bytes(), path.stat().st_mtime_ns) == before


def test_durable_terminal_failure_isolated_per_host_and_retains_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    rollout = "v0.15.60-two-host-failure"

    def action(action_id: str, host: str, argv: list[str]):
        value = _safe_durable_action()
        value.id = action_id
        value.host = host
        value.argv = argv
        return value

    failed = action(
        "local-runtime:host-a:vibeqc-queue",
        "host-a",
        ["--not-a-real-option"],
    )
    sibling = action(
        "local-runtime:host-a:vibeqc-release",
        "host-a",
        ["--version"],
    )
    sibling.program = "vibeqc-release"
    sibling.pin_name = "release"
    healthy = action(
        "local-runtime:host-b:vibeqc-queue",
        "host-b",
        ["--version"],
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": "a" * 64, "source_path": "report.json"},
        topology={},
        actions=[failed, sibling, healthy],
    )
    held: set[str] = set()

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        args = argv[3:]
        host = args[-1]
        if "--status" in args:
            active = host in held
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        "active": active,
                        "is_full_drain": active,
                        "scheduler_hosts": [],
                        "state": (
                            {
                                "reason": fleet_rollout._hold_reason(
                                    rollout, host
                                ),
                                "set_at": "2026-08-10T20:00:00+00:00",
                            }
                            if active
                            else None
                        ),
                    }
                ),
                stderr="",
            )
        if "--release-full" in args:
            held.discard(host)
        else:
            held.add(host)
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    run = fleet_rollout.execute_plan(
        plan,
        rollout_id=rollout,
        control_runner=control,
        report_digest_resolver=lambda: "a" * 64,
    )

    assert run.actions[failed.id]["status"] == "failed"
    assert run.actions[sibling.id]["status"] == "not-run"
    assert run.actions[healthy.id]["status"] == "success"
    assert run.holds["host-a"]["status"] == "active"
    assert held == {"host-a"}
    first_attempt = run.actions[failed.id]["operation_attempts"][0]
    assert first_attempt["failure_fence_consumed"] is True
    assert first_attempt["failure_retry_authorized"] is False

    authorized = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="a" * 64,
        inspect_only=False,
        control_runner=control,
    )
    assert authorized.failed_operation_hosts == ()
    reconciled = fleet_rollout.load_run(rollout)
    assert reconciled is not None
    assert reconciled.holds["host-a"]["status"] == "released"
    assert held == set()
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(
            plan,
            failed,
            rollout_id=rollout,
            durable_reconciled=True,
        )
    retried = fleet_rollout.load_run(rollout)
    assert retried is not None
    assert [
        item["attempt"]
        for item in retried.actions[failed.id]["operation_attempts"]
    ] == [1, 2]


def test_live_operation_output_drains_in_order_across_utf8_chunk_boundaries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Observed:
        def __init__(self, state: str):
            self.state = state
            self.ready = {"ready": True}
            self.activation = {"launch_intent": True}

    observations = iter((Observed("running-authorized"), Observed("completed")))
    chunks = iter(
        (
            fleet_operation.OperationOutputChunk(
                data=b"\xe2",
                offset=0,
                next_offset=1,
                stored_bytes=4,
                at_end=False,
                spool_full=False,
            ),
            fleet_operation.OperationOutputChunk(
                data=b"\x82\xac\n",
                offset=1,
                next_offset=4,
                stored_bytes=4,
                at_end=True,
                spool_full=False,
            ),
            fleet_operation.OperationOutputChunk(
                data=b"",
                offset=4,
                next_offset=4,
                stored_bytes=4,
                at_end=True,
                spool_full=False,
            ),
        )
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda operation: next(observations),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "read_output_since",
        lambda *args, **kwargs: next(chunks),
    )
    monkeypatch.setattr(fleet_rollout.time, "sleep", lambda seconds: None)
    stream = io.StringIO()

    result = fleet_rollout._stream_operation_until_terminal(
        "1" * 64,
        stream=stream,
    )

    assert result.state == "completed"
    assert stream.getvalue() == "€\n"


def test_running_authorized_regression_relaunches_the_exact_same_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = "1" * 64

    class Observed:
        def __init__(self, state: str, *, busy: bool) -> None:
            self.state = state
            self.ready = None
            self.lease_busy = busy
            self.activation = None

    observations = iter(
        (
            Observed("running-authorized", busy=True),
            Observed("authorized-unactivated", busy=False),
            Observed("completed", busy=False),
        )
    )
    supervisor = _FakeRelaunchSupervisor(returncode=None)
    launched: list[str] = []

    def observe(candidate: str) -> Observed:
        del candidate
        observed = next(observations)
        if observed.state == "completed":
            supervisor.returncode = 0
        return observed

    def launch(candidate: str, **kwargs: Any) -> _FakeRelaunchSupervisor:
        assert kwargs == {"lifecycle_handoff": None}
        launched.append(candidate)
        return supervisor

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        observe,
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )
    monkeypatch.setattr(fleet_rollout.time, "sleep", lambda seconds: None)

    result = fleet_rollout._stream_operation_until_terminal(
        operation,
        resume_authorized=True,
    )

    assert result.state == "completed"
    assert launched == [operation]
    assert supervisor.poll_calls >= 1


def test_fast_relaunch_exit_rechecks_receipt_before_reporting_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation = "2" * 64

    class Observed:
        def __init__(self, state: str) -> None:
            self.state = state
            self.ready = None
            self.lease_busy = False
            self.activation = None

    observations = iter(
        (
            Observed("authorized-unactivated"),
            Observed("completed"),
        )
    )
    supervisor = _FakeRelaunchSupervisor(returncode=0)

    def launch(candidate: str, **kwargs: Any) -> _FakeRelaunchSupervisor:
        assert candidate == operation
        assert kwargs == {"lifecycle_handoff": None}
        return supervisor

    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "observe_operation",
        lambda candidate: next(observations),
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        launch,
    )

    result = fleet_rollout._stream_operation_until_terminal(
        operation,
        resume_authorized=True,
    )

    assert result.state == "completed"


def test_global_unknown_outcome_preflight_precedes_any_safe_relaunch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fatal reconciliation state is global, not operation-hash ordered."""
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    handles: list[fleet_operation.OperationHandle] = []
    for suffix in ("one", "two"):
        action = _safe_durable_action()
        action.id = f"local-runtime:localhost:vibeqc-queue-{suffix}"
        handles.append(
            _journal_durable_action(
                action=action,
                rollout=f"v0.15.60-global-preflight-{suffix}",
                digest=digest,
            )
        )
    resumable, unknown = sorted(handles, key=lambda item: item.operation_id)
    _publish_authorization_without_activation(resumable)
    _publish_authorization_without_activation(unknown)
    unknown_ready = fleet_operation.observe_operation(unknown.operation_id).ready
    assert unknown_ready is not None
    fleet_operation._write_operation_receipt(
        unknown,
        "activation.json",
        fleet_operation._activation_payload(
            unknown,
            nonce=str(unknown_ready["nonce"]),
        ),
    )
    assert fleet_operation.observe_operation(unknown.operation_id).state == (
        "outcome-unknown"
    )
    journals = sorted(fleet_rollout.rollout_state_dir().glob("*.json"))
    before = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in journals
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail(
            "global unknown outcome must preflight before relaunch"
        ),
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="outcome unknown"):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=False,
        )

    assert {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in journals
    } == before
    assert fleet_operation.observe_operation(resumable.operation_id).state == (
        "authorized-unactivated"
    )


def test_activation_without_result_is_outcome_unknown_and_never_replayed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-outcome-unknown"
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        action,
        attempt=1,
    )
    handle = fleet_operation.prepare_operation(identity)
    prepared = fleet_operation.observe_operation(handle.operation_id)
    run = fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=digest,
        report_source_path="report.json",
        actions={
            action.id: {
                "status": "running",
                "operation_attempts": [
                    fleet_rollout._attempt_ref(
                        prepared,
                        identity,
                        request_sha256=handle.request_sha256,
                    )
                ],
            }
        },
    )
    fleet_rollout.save_run(run)
    failures: list[BaseException] = []

    def supervisor() -> None:
        try:
            fleet_operation.run_supervisor(
                handle.operation_id,
                authorization_timeout=10,
                poll_interval=0.01,
                child_popen=lambda *args, **kwargs: (_ for _ in ()).throw(
                    OSError("post-intent Popen failure")
                ),
            )
        except BaseException as exc:
            failures.append(exc)

    thread = threading.Thread(target=supervisor)
    thread.start()
    fleet_operation.wait_for_ready(handle.operation_id, timeout=10)
    fleet_operation.authorize_operation(
        handle.operation_id,
        expected_report_digest_sha256=digest,
    )
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert failures and isinstance(failures[0], OSError)
    assert fleet_operation.observe_operation(handle.operation_id).state == (
        "outcome-unknown"
    )
    state_path = fleet_rollout.rollout_state_path(rollout)
    journal_before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)
    monkeypatch.setattr(
        fleet_rollout.fleet_operation,
        "launch_supervisor",
        lambda *args, **kwargs: pytest.fail("unknown outcome must not replay"),
    )

    with pytest.raises(fleet_rollout.FleetRolloutError, match="outcome unknown"):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=digest,
            inspect_only=False,
        )

    persisted = fleet_rollout.load_run(rollout)
    assert persisted is not None
    attempt = persisted.actions[action.id]["operation_attempts"][0]
    assert attempt["state"] == "prepared"
    assert attempt["retry_safe"] is True
    assert attempt["harvested"] is False
    assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == journal_before


def test_all_action_record_replacements_flow_through_history_preserving_helper() -> None:
    tree = ast.parse(inspect.getsource(fleet_rollout))
    assignments: list[tuple[str | None, int]] = []

    class Visitor(ast.NodeVisitor):
        function: str | None = None

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            previous = self.function
            self.function = node.name
            self.generic_visit(node)
            self.function = previous

        def visit_Assign(self, node: ast.Assign) -> None:
            for target in node.targets:
                if (
                    isinstance(target, ast.Subscript)
                    and isinstance(target.value, ast.Attribute)
                    and isinstance(target.value.value, ast.Name)
                    and target.value.value.id == "run"
                    and target.value.attr == "actions"
                ):
                    assignments.append((self.function, node.lineno))
            self.generic_visit(node)

    Visitor().visit(tree)

    assert assignments == [("_replace_action_record", assignments[0][1])]


def test_unselected_current_pre_authorization_is_aborted_without_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    action = _safe_durable_action()
    digest = "a" * 64
    rollout = "v0.15.60-unselected"
    identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        action,
        attempt=1,
    )
    handle = fleet_operation.prepare_operation(identity)
    skipped = fleet_rollout.RolloutAction(
        **{
            **action.__dict__,
            "decision": "skip",
            "reason": "live planner no longer requests this mutation",
        }
    )
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest},
        topology={},
        actions=[skipped],
    )

    fleet_rollout.abort_unselected_pre_authorization_operations(plan)

    observed = fleet_operation.observe_operation(handle.operation_id)
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"
    assert observed.result["executed"] is False
    run = fleet_rollout.load_run(rollout)
    assert run is not None
    assert run.actions[action.id]["status"] == "not-run"


def test_pending_failure_aborts_exact_selected_preauth_and_retains_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    rollout = "v0.15.60-fenced-preauth"
    failed = _safe_durable_action()
    failed.id = "host-a:failed"
    failed.host = "host-a"
    failed.argv = ["--not-a-real-option"]
    sibling = _safe_durable_action()
    sibling.id = "host-a:prepared-sibling"
    sibling.host = "host-a"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[failed, sibling],
    )
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(plan, failed, rollout_id=rollout)
    run = fleet_rollout.load_run(rollout)
    assert run is not None
    run.holds["host-a"] = {
        "host": "host-a",
        "kind": "full",
        "owned": True,
        "status": "active",
        "reason": fleet_rollout._hold_reason(rollout, "host-a"),
        "set_at": "2026-08-10T20:00:00+00:00",
        "control_host": "host-a",
    }
    fleet_rollout.save_run(run)
    sibling_identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        sibling,
        attempt=1,
    )
    sibling_handle = fleet_operation.prepare_operation(sibling_identity)

    fleet_rollout.abort_unselected_pre_authorization_operations(
        plan,
        failed_hosts=frozenset({"host-a"}),
        control_runner=lambda *args, **kwargs: pytest.fail(
            "the pending failure host's exact hold must remain active"
        ),
    )

    sibling_observed = fleet_operation.observe_operation(
        sibling_handle.operation_id
    )
    assert sibling_observed.state == "completed"
    assert sibling_observed.result is not None
    assert sibling_observed.result["status"] == "aborted"
    persisted = fleet_rollout.load_run(rollout)
    assert persisted is not None
    assert persisted.holds["host-a"]["status"] == "active"


def test_pending_driver_failure_aborts_all_other_preauth_operations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    digest = "a" * 64
    rollout = "v0.15.60-driver-fenced-preauth"
    driver = _safe_durable_action(phase="driver")
    driver.argv = ["--not-a-real-option"]
    remote = _safe_durable_action()
    remote.id = "host-b:prepared"
    remote.host = "host-b"
    plan = fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": digest, "source_path": "report.json"},
        topology={},
        actions=[driver, remote],
    )
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(plan, driver, rollout_id=rollout)
    remote_identity = fleet_rollout._operation_identity(
        rollout,
        digest,
        remote,
        attempt=1,
    )
    remote_handle = fleet_operation.prepare_operation(remote_identity)

    fleet_rollout.abort_unselected_pre_authorization_operations(
        plan,
        failed_hosts=frozenset({"localhost"}),
        abort_all=True,
    )

    observed = fleet_operation.observe_operation(remote_handle.operation_id)
    assert observed.state == "completed"
    assert observed.result is not None
    assert observed.result["status"] == "aborted"


class TestLegacyReconciliationInventory:
    """`inventory_legacy_rollout_state` answers the question without writing.

    A durable, fleet-wide recovery that marks historical action outcomes
    permanently unknown had to be authorised blind: `--reconcile-legacy`
    refused `--dry-run` outright, so the operator could not see what it would
    rewrite. This is the read-only half.
    """

    def test_it_reports_the_same_obsolete_claim_the_mutating_pass_finds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
        run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
        _authenticate_obsolete_scheduler_report(monkeypatch)

        mutating = fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256="b" * 64,
            inspect_only=False,
            allow_legacy_reconciliation=True,
            control_runner=lambda *args, **kwargs: pytest.fail(
                "inventory must not inspect or release a scheduler claim"
            ),
        )
        preview = fleet_rollout.inventory_legacy_rollout_state(
            current_report_digest_sha256="b" * 64,
        )

        # The preview is only useful if it agrees with what would happen.
        assert mutating.legacy_scheduler_holds == ((run.rollout_id, "host_c"),)
        assert preview.scheduler_holds == mutating.legacy_scheduler_holds
        assert preview.empty is False

    def test_it_leaves_every_journal_byte_identical(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
        _run, state_path = _obsolete_preowner_scheduler_claim(tmp_path)
        _authenticate_obsolete_scheduler_report(monkeypatch)
        before = (state_path.read_bytes(), state_path.stat().st_mtime_ns)

        fleet_rollout.inventory_legacy_rollout_state(
            current_report_digest_sha256="b" * 64,
        )

        assert (state_path.read_bytes(), state_path.stat().st_mtime_ns) == before

    def test_it_observes_durable_operations_without_recovering_them(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Recovery is a write. The preview must not perform one to look."""
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
        _obsolete_preowner_scheduler_claim(tmp_path)
        _authenticate_obsolete_scheduler_report(monkeypatch)
        recover_flags: list[Any] = []

        def list_ids(*, recover: bool = True) -> tuple[str, ...]:
            recover_flags.append(recover)
            return ()

        monkeypatch.setattr(
            fleet_rollout.fleet_operation, "list_operation_ids", list_ids,
        )
        monkeypatch.setattr(
            fleet_rollout.fleet_operation,
            "observe_operation",
            lambda *a, **k: pytest.fail("no operation to observe"),
        )

        fleet_rollout.inventory_legacy_rollout_state(
            current_report_digest_sha256="b" * 64,
        )

        assert recover_flags == [False]

    def test_a_clean_fleet_inventories_to_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)

        preview = fleet_rollout.inventory_legacy_rollout_state(
            current_report_digest_sha256="b" * 64,
        )

        assert preview.empty is True
        assert preview.as_dict()["empty"] is True

    def test_it_binds_a_monorepo_journal_to_a_split_layout_checkout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The preview runs the mutating pass's binding check, and passes it.

        A journal recorded under the monorepo path, against a real checkout
        holding the byte-identical report under ``releases/``. The preview
        lists the hold because the real run re-observes it.
        """
        state = tmp_path / "state"
        state.mkdir()
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: state)
        repo, report = _split_layout_report_checkout(
            tmp_path, monkeypatch, tag="v0.15.118"
        )
        run = _monorepo_journal_for(report, host="host_a")
        fleet_rollout.save_run(run)

        preview = fleet_rollout.inventory_legacy_rollout_state(
            current_report_digest_sha256="f" * 64,
            current_report_source_path="releases/v0.17.0.json",
            report_repo=repo,
        )

        assert preview.hold_retries == ((run.rollout_id, "host_a"),)

    def test_it_refuses_a_hold_the_mutating_pass_would_refuse(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A preview that lists what the real run then rejects is no preview.

        The report authenticates -- the blob is committed -- but the journal
        claims a rollout id the report does not produce, so it does not bind.
        Both passes refuse it, in the same words, and neither writes.
        """
        state = tmp_path / "state"
        state.mkdir()
        monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: state)
        repo, report = _split_layout_report_checkout(
            tmp_path, monkeypatch, tag="v0.15.118"
        )
        run = _monorepo_journal_for(
            report, host="host_a", rollout_id="v0.15.118-000000000000"
        )
        state_path = fleet_rollout.save_run(run)
        before = state_path.read_bytes()

        with pytest.raises(
            fleet_rollout.FleetRolloutError,
            match="retained hold does not bind its committed accepted report",
        ) as previewed:
            fleet_rollout.inventory_legacy_rollout_state(
                current_report_digest_sha256="f" * 64,
                current_report_source_path="releases/v0.17.0.json",
                report_repo=repo,
            )
        with pytest.raises(fleet_rollout.FleetRolloutError) as mutated:
            fleet_rollout.reconcile_legacy_rollout_state(
                legacy_running_actions=(),
                legacy_hold_retries=((run.rollout_id, "host_a"),),
                plan=_legacy_full_hold_recovery_plan(("host_a",)),
                admin_status={},
                repo=repo,
                current_report_digest_resolver=lambda: "f" * 64,
                control_runner=lambda *args, **kwargs: pytest.fail(
                    "an unbound journal must fail before control"
                ),
            )

        assert str(mutated.value) == str(previewed.value)
        assert state_path.read_bytes() == before


def _split_layout(
    report: fleet_release.FleetReleaseReport,
) -> fleet_release.FleetReleaseReport:
    """The same report as vibe-queue's own repository holds it.

    A receipt persisted before the 2026-09-08 split records
    ``vibe-queue/releases/<tag>.json``. The byte-identical blob lives at
    ``releases/<tag>.json`` in vibe-queue's own history, so that is the
    spelling discovery returns from a split checkout -- for the very same
    report the journal is naming.
    """
    return dataclasses.replace(
        report,
        source_path=f"releases/{Path(report.source_path).name}",
    )


def test_split_layout_report_binds_a_retained_legacy_full_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical, run = _obsolete_legacy_full_hold(
        host="host_a",
        version=(0, 15, 118),
        digest="b" * 64,
    )
    run.holds["host_a"]["set_at"] = "2026-08-08T12:00:00+00:00"
    fleet_rollout.save_run(run)
    assert run.report_source_path == "vibe-queue/releases/v0.15.118.json"
    discovered = _split_layout(historical)
    assert discovered.source_path == "releases/v0.15.118.json"
    plan = _legacy_full_hold_recovery_plan(("host_a",))
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: discovered,
    )

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=((run.rollout_id, "host_a"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "f" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((run.rollout_id, "host_a"),)
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_a"]["status"] == "released"


def test_split_layout_report_binds_a_running_legacy_action(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    discovered = _split_layout(_report())
    plan = _legacy_supersession_plan()
    run = _save_legacy_supersession_run(tmp_path)
    assert run.report_source_path == "vibe-queue/releases/v0.15.60.json"
    assert discovered.source_path == "releases/v0.15.60.json"
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: discovered,
    )

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if argv[-1] == "host_a":
            return subprocess.CompletedProcess(
                argv,
                0,
                stdout=json.dumps(
                    {
                        **_inactive_full_drain_status(),
                        "active": True,
                        "mode": "full",
                        "is_full_drain": True,
                        "submit_policy": "deny",
                        "state": {
                            "reason": fleet_rollout._hold_reason(
                                run.rollout_id, "host_a"
                            ),
                            "set_at": "2026-08-08T12:00:00+00:00",
                        },
                    }
                ),
                stderr="",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert result.superseded_actions == (
        (run.rollout_id, "local-runtime:host_b:vibeqc-release"),
    )
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert (
        persisted.actions["local-runtime:host_b:vibeqc-release"]["status"]
        == "superseded"
    )


def test_split_layout_report_binds_an_obsolete_scheduler_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    discovered = _split_layout(_obsolete_scheduler_report())
    assert discovered.source_path == "releases/v0.15.117.json"
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: discovered,
    )
    run, _state_path = _obsolete_preowner_scheduler_claim(tmp_path)
    assert run.report_source_path == "vibe-queue/releases/v0.15.117.json"
    plan = _legacy_scheduler_reconciliation_plan()

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_supported_drain_snapshot()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_scheduler_holds=((run.rollout_id, "host_c"),),
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((run.rollout_id, "host_c"),)


def test_split_layout_report_binds_a_failed_legacy_rollout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_a, host_e, plan, host_e_action, _observation = (
        _install_failed_exact_host_e_pair(tmp_path, monkeypatch)
    )
    discovered = {
        run.report_digest_sha256: _split_layout(
            _obsolete_legacy_full_hold(
                host=host,
                version=version,
                digest=run.report_digest_sha256,
            )[0]
        )
        for run, host, version in (
            (host_a, "host_a", (0, 15, 118)),
            (host_e, "host_e", (0, 15, 137)),
        )
    }
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: discovered[digest],
    )
    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=False,
        allow_legacy_reconciliation=True,
    )
    assert inventory.failed_operation_hosts
    host_e_action.decision = "block"
    host_e_action.reason = "current lane is not healthy"

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        if argv[-1] == "host_a":
            return subprocess.CompletedProcess(
                argv, 255, stdout="", stderr="network unavailable"
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        failed_operation_hosts=inventory.failed_operation_hosts,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: str(
            plan.report["digest_sha256"]
        ),
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((host_e.rollout_id, "host_e"),)
    assert result.retained_holds[0][:2] == (host_a.rollout_id, "host_a")


def test_every_historical_report_binding_flows_through_one_helper() -> None:
    """`b2aef94` fixed one of five copies; the other four stayed broken.

    Path-spelling comparisons are therefore allowed in exactly one place.
    """
    tree = ast.parse(inspect.getsource(fleet_rollout))
    comparisons: list[tuple[str | None, int]] = []

    class Visitor(ast.NodeVisitor):
        function: str | None = None

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            previous = self.function
            self.function = node.name
            self.generic_visit(node)
            self.function = previous

        def visit_Compare(self, node: ast.Compare) -> None:
            for operand in (node.left, *node.comparators):
                if (
                    isinstance(operand, ast.Attribute)
                    and operand.attr == "source_path"
                    and isinstance(operand.value, ast.Name)
                    and operand.value.id == "historical"
                ):
                    comparisons.append((self.function, node.lineno))
            self.generic_visit(node)

    Visitor().visit(tree)

    assert [function for function, _ in comparisons] == [
        "_historical_report_binds_run"
    ]


def test_split_layout_retained_action_receipt_revalidates_next_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A receipt written from a split checkout must survive its own reader.

    The receipt records the report identity, and every reader validates that
    against the journal -- so recording the *discovered* spelling wrote a
    receipt that the very next `--reconcile-legacy` would reject.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    discovered = _split_layout(_report())
    plan = _legacy_supersession_plan()
    plan.actions[0].decision = "block"
    plan.actions[0].reason = "not proven current"
    run = _save_legacy_supersession_run(tmp_path)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: discovered,
    )

    fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=((run.rollout_id, next(iter(run.actions))),),
        plan=plan,
        admin_status={"host_b": {"marker": None, "markers": []}},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "b" * 64,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "retained action group must not inspect or release its holds"
        ),
    )

    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    receipt = persisted.legacy_retained_actions[
        "local-runtime:host_b:vibeqc-release"
    ]
    assert receipt["historical_report_path"] == run.report_source_path
    assert receipt["historical_report_digest_sha256"] == (
        run.report_digest_sha256
    )

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256="b" * 64,
        current_report_source_path=str(plan.report["source_path"]),
        inspect_only=True,
        allow_legacy_reconciliation=True,
    )

    assert inventory.legacy_running_actions == ()


def test_report_path_fields_are_never_compared_by_spelling() -> None:
    """Report-path fields are compared through `same_report`, never ``==``.

    Plain equality on report paths broke once per copy across the split:
    `b2aef94` fixed one, `721bea7` five more, and each failing check had been
    masking the next. This fails at the next copy instead of on a host.
    """
    fields = {
        "source_path",
        "report_source_path",
        "current_report_path",
        "historical_report_path",
    }

    def names_a_report_path(node: ast.expr) -> bool:
        if isinstance(node, ast.Attribute):
            return node.attr in fields
        if isinstance(node, ast.Subscript):
            return (
                isinstance(node.slice, ast.Constant)
                and node.slice.value in fields
            )
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            return node.args[0].value in fields
        return False

    offenders: list[str] = []
    for module in (fleet_rollout, legacy_failure_transition):
        for node in ast.walk(ast.parse(inspect.getsource(module))):
            if (
                isinstance(node, ast.Compare)
                and any(isinstance(op, (ast.Eq, ast.NotEq)) for op in node.ops)
                and any(
                    names_a_report_path(operand)
                    for operand in (node.left, *node.comparators)
                )
            ):
                offenders.append(
                    f"{module.__name__}:{node.lineno}: {ast.unparse(node)}"
                )

    assert offenders == []


def test_split_layout_current_report_is_not_a_changed_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A driver on a split checkout discovers the same current report.

    The retained receipt was written with ``vibe-queue/releases/v0.15.60.json``;
    ``discover_latest_report`` from vibe-queue's own repository returns
    ``releases/v0.15.60.json`` for the identical digest. Path-changed with
    digest-unchanged used to read as an incoherent receipt, a hard block with
    no operator workaround.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(
        tmp_path, monkeypatch, with_holds=True
    )
    receipt = next(iter(run.legacy_retained_actions.values()))
    assert receipt["current_report_path"] == "vibe-queue/releases/v0.15.60.json"
    split_current = "releases/v0.15.60.json"

    inventory = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report["digest_sha256"]),
        current_report_source_path=split_current,
        inspect_only=True,
        allow_legacy_reconciliation=True,
        control_runner=lambda *args, **kwargs: pytest.fail(
            "an unchanged report must not trigger any control"
        ),
    )

    assert inventory.legacy_running_actions == ()


def test_split_layout_failure_transition_authenticates_historical_reports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The v2 failure transition, run from a split checkout.

    Discovery by digest returns ``releases/...`` for reports every journal
    recorded as ``vibe-queue/releases/...``. `_authenticate_failure_report`
    compared the two with ``!=`` and refused the old journal as conflicting
    with its own report before any transition could start.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    host_e, plan, action, _observation, accepted = _isolated_failed_host_e_recovery(
        tmp_path,
        monkeypatch,
    )
    old_report, _unused = _obsolete_legacy_full_hold(
        host="host_e", version=(0, 15, 137), digest="b" * 64
    )
    split_reports = {
        report.digest_sha256: _split_layout(report)
        for report in (old_report, accepted)
    }
    assert host_e.report_source_path.startswith("vibe-queue/releases/")
    assert all(
        report.source_path.startswith("releases/")
        for report in split_reports.values()
    )
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda source_path, digest, repo, *, fetch: split_reports[digest],
    )
    _forbid_failure_transition_execution(monkeypatch)

    _call_failure_transition_v2(
        accepted_report=accepted,
        **_failure_transition_kwargs(
            host_e=host_e,
            plan=plan,
            tmp_path=tmp_path,
            control_runner=_inactive_failure_status_runner(),
        ),
    )

    old = fleet_rollout.load_run(host_e.rollout_id)
    current = fleet_rollout.load_run(fleet_rollout.rollout_id(accepted))
    assert old is not None
    assert current is not None
    assert action.id in _failed_action_ids(host_e)
    _assert_exact_failure_receipt(
        source=old,
        current=current,
        accepted=accepted,
        reason="release failed rc=9",
    )


def test_split_driver_unblocks_the_retained_host_a_hold_with_reconcile_legacy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The migration's blocker, end to end, driven the way the CLI drives it.

    host_a's v0.15.118 full hold was retained on 2026-08-30 against the
    then-accepted v0.15.155 report, and the driver now runs from a split
    checkout with a newer accepted report. ``--verify-only`` and a plain
    ``--dry-run`` refusing with "different accepted report" is by design. The
    defect was that ``--reconcile-legacy`` -- the only way past that refusal
    -- then refused the report it had just authenticated.
    """
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical, run = _obsolete_legacy_full_hold(
        host="host_a", version=(0, 15, 118), digest="b" * 64
    )
    run.holds["host_a"].update(
        {
            "duration_seconds": 21600,
            "set_at": "2026-08-08T12:00:00+00:00",
            "control_host": "host_a",
        }
    )
    retained_against = _legacy_full_hold_recovery_plan(("host_a",))
    retained_against.report["source_path"] = "vibe-queue/releases/v0.15.155.json"
    retained_against.report["digest_sha256"] = "e" * 64
    run.legacy_retained_holds["host_a"] = fleet_rollout._legacy_retained_hold_receipt(
        run,
        host="host_a",
        hold=run.holds["host_a"],
        plan=retained_against,
        source="hold-observation",
        control_host="host_a",
        retained_at="2026-08-30T12:00:00+00:00",
        reason="prior observation retained exact hold",
    )
    run.complete = False
    fleet_rollout.save_run(run)
    plan = _legacy_full_hold_recovery_plan(("host_a",))
    plan.report["source_path"] = "releases/v0.17.0.json"
    plan.report["digest_sha256"] = "f" * 64
    discovered = _split_layout(historical)
    monkeypatch.setattr(
        fleet_rollout.fleet_release,
        "discover_historical_report_by_digest",
        lambda *args, **kwargs: discovered,
    )
    scope = {
        "current_report_digest_sha256": "f" * 64,
        "current_report_source_path": "releases/v0.17.0.json",
        "allowed_retention_identity_mismatch_hosts": frozenset(),
        "configured_retention_hosts": frozenset({"host_a"}),
    }

    def no_control(*args: Any, **kwargs: Any) -> None:
        pytest.fail("inspection must issue no control")

    def inactive(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps(_inactive_full_drain_status()), stderr=""
        )

    # --verify-only, or --dry-run without --reconcile-legacy: the designed stop.
    with pytest.raises(
        fleet_rollout.FleetRolloutError,
        match="different accepted report.*--reconcile-legacy",
    ):
        fleet_rollout.reconcile_durable_operations(
            inspect_only=True, control_runner=no_control, **scope
        )

    # --reconcile-legacy.
    inventory = fleet_rollout.reconcile_durable_operations(
        inspect_only=False,
        allow_legacy_reconciliation=True,
        control_runner=inactive,
        **scope,
    )
    assert inventory.legacy_hold_retries == ((run.rollout_id, "host_a"),)
    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=inventory.legacy_hold_retries,
        plan=plan,
        admin_status={},
        repo=tmp_path,
        current_report_digest_resolver=lambda: "f" * 64,
        control_runner=inactive,
    )
    assert result.settled_inactive_holds == ((run.rollout_id, "host_a"),)

    # And afterwards the non-reconciling modes no longer stop.
    fleet_rollout.reconcile_durable_operations(
        inspect_only=True, control_runner=no_control, **scope
    )


def _split_layout_report_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tag: str,
) -> tuple[Path, fleet_release.FleetReleaseReport]:
    """A vibe-queue checkout holding ``tag``'s report at ``releases/`` only.

    The driver's checkout after the 2026-09-08 split: fresh history, the
    report committed under the split layout, no ``vibe-queue/`` prefix
    anywhere. Unlike :func:`_split_layout`, which rewrites a fixture's
    ``source_path`` and stubs discovery, this authenticates for real -- the
    pins resolve in the checkout's own history, as in test_fleet_release --
    so the split layout is exercised through the code that actually looks a
    report up. The pin repositories are pinned to that checkout so a driver
    config on the machine running the tests cannot leak in.
    """
    from tests import test_fleet_release as release_fixtures

    repo = release_fixtures._init_repo(tmp_path)
    sha = release_fixtures._git(repo, "rev-parse", "HEAD")
    release_fixtures._git(repo, "tag", tag, sha)
    path = repo / "releases" / f"{tag}.json"
    path.parent.mkdir(parents=True)
    payload = release_fixtures._report(tag, sha)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    release_fixtures._commit(repo, f"{tag} report in the split layout")
    release_fixtures._git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(
        fleet_release,
        "resolve_pin_repos",
        lambda explicit=None: release_fixtures._pin_repos(repo),
    )
    report = fleet_release.discover_latest_report(repo, fetch=False)
    assert report.source_path == f"releases/{tag}.json"
    return repo, report


def _monorepo_journal_for(
    report: fleet_release.FleetReleaseReport,
    *,
    host: str,
    rollout_id: str | None = None,
) -> fleet_rollout.RolloutRun:
    """A pre-split journal for ``report``: it names the monorepo path.

    Every journal persisted on the fleet before the split records
    ``vibe-queue/releases/<tag>.json``, because that is where the driver's
    checkout held the report at the time. The digest is the report's own;
    only the spelling of the path differs from where it is found now.
    """
    rollout = rollout_id or fleet_rollout.rollout_id(report)
    return fleet_rollout.RolloutRun(
        rollout_id=rollout,
        report_digest_sha256=report.digest_sha256,
        report_source_path=f"vibe-queue/releases/{report.release.tag}.json",
        actions={
            f"local-runtime:{host}:vibeqc-release": {"status": "success"}
        },
        holds={
            host: {
                "host": host,
                "kind": "full",
                "owned": True,
                "preexisting": False,
                "reason": fleet_rollout._hold_reason(rollout, host),
                "status": "active",
                "set_at": "2026-08-08T12:00:00+00:00",
            }
        },
        complete=True,
    )


def test_split_layout_checkout_binds_a_monorepo_journal_through_real_discovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The migration's blocker against a real checkout, nothing stubbed.

    The v0.15.118 journal records ``vibe-queue/releases/v0.15.118.json``; the
    driver's checkout holds that exact blob at ``releases/v0.15.118.json``.
    Every sibling test rewrites a fixture's path and stubs discovery, so none
    of them proves that the real lookup and the binding rule agree. This
    drives the retained hold through both.
    """
    state = tmp_path / "state"
    state.mkdir()
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: state)
    repo, report = _split_layout_report_checkout(
        tmp_path, monkeypatch, tag="v0.15.118"
    )
    run = _monorepo_journal_for(report, host="host_a")
    fleet_rollout.save_run(run)
    assert run.report_source_path != report.source_path
    controls: list[list[str]] = []

    def control(
        argv: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        del kwargs
        controls.append(argv[3:])
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps(_inactive_full_drain_status()),
            stderr="",
        )

    result = fleet_rollout.reconcile_legacy_rollout_state(
        legacy_running_actions=(),
        legacy_hold_retries=((run.rollout_id, "host_a"),),
        plan=_legacy_full_hold_recovery_plan(("host_a",)),
        admin_status={},
        repo=repo,
        current_report_digest_resolver=lambda: "f" * 64,
        control_runner=control,
    )

    assert result.settled_inactive_holds == ((run.rollout_id, "host_a"),)
    assert controls == [["drain", "--status", "--json", "host_a"]]
    persisted = fleet_rollout.load_run(run.rollout_id)
    assert persisted is not None
    assert persisted.holds["host_a"]["status"] == "released"
    # The journal keeps the identity it recorded; only the hold was settled.
    assert persisted.report_source_path == "vibe-queue/releases/v0.15.118.json"
    assert persisted.report_digest_sha256 == report.digest_sha256


def _retirement_case(tmp_path, monkeypatch):
    monkeypatch.setattr(fleet_rollout.paths, 'state_root', lambda: tmp_path)
    run, plan = _create_retained_legacy_action_receipt(tmp_path, monkeypatch, with_holds=True)

    def discover(path, digest, *args, **kwargs):
        assert kwargs.get('fetch') is False
        return dataclasses.replace(_report(), source_path=path, digest_sha256=digest)

    monkeypatch.setattr(fleet_rollout.fleet_release,
                        'discover_historical_report_by_digest', discover)
    declarations = {
        host: config.HostRetirement(
            retired_at='2026-09-12T17:00:00+00:00', reason='permanent hardware retirement',
            authorization_reference='maintainer decision in issue 13',
            retained_receipts={run.rollout_id: fleet_rollout.retained_host_audit_digest(run, host)},
        ) for host in ('host_a', 'host_b')
    }
    return run, plan, declarations


@pytest.mark.parametrize('inspect_only', [False, True])
@pytest.mark.parametrize('reconcile_legacy', [False, True])
def test_retired_receipts_preserve_unknown_fences_without_contact_or_rewrite(
    tmp_path, monkeypatch, inspect_only, reconcile_legacy,
):
    run, plan, declarations = _retirement_case(tmp_path, monkeypatch)
    path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = path.read_bytes()
    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256=str(plan.report['digest_sha256']),
        current_report_source_path=str(plan.report['source_path']),
        inspect_only=inspect_only, allow_legacy_reconciliation=reconcile_legacy,
        configured_retention_hosts=frozenset({'localhost'}),
        retired_hosts=declarations, report_repo=tmp_path,
        control_runner=lambda *a, **k: pytest.fail('retired hosts must not be contacted'),
    )
    assert result.legacy_hold_retries == ()
    assert result.legacy_running_actions == ()
    assert result.legacy_scheduler_holds == ()
    assert {host for _, host, _ in result.retained_legacy_holds} == {'host_a', 'host_b'}
    assert path.read_bytes() == before
    assert fleet_rollout.load_run(run.rollout_id).complete is False


def test_retirement_preview_excludes_retired_controls_but_preserves_audit(tmp_path, monkeypatch):
    run, plan, declarations = _retirement_case(tmp_path, monkeypatch)
    before = fleet_rollout.rollout_state_path(run.rollout_id).read_bytes()
    inventory = fleet_rollout.inventory_legacy_rollout_state(
        current_report_digest_sha256=str(plan.report['digest_sha256']),
        current_report_source_path=str(plan.report['source_path']),
        configured_retention_hosts=frozenset({'localhost'}),
        retired_hosts=declarations, report_repo=tmp_path,
    )
    assert inventory.running_actions == ()
    assert inventory.scheduler_holds == () and inventory.hold_retries == ()
    assert fleet_rollout.rollout_state_path(run.rollout_id).read_bytes() == before


@pytest.mark.parametrize('damage', [
    'missing-declaration', 'misspelled-host', 'wrong-digest', 'changed-hold',
    'changed-receipt', 'active-host', 'missing-repo', 'unauthenticated-report',
    'uncovered-action',
])
def test_retirement_cannot_hide_unknown_hosts_or_damaged_live_evidence(
    tmp_path, monkeypatch, damage,
):
    run, plan, declarations = _retirement_case(tmp_path, monkeypatch)
    configured = {'localhost'}
    repo = tmp_path
    if damage == 'missing-declaration':
        del declarations['host_b']
    elif damage == 'misspelled-host':
        declarations['maruu'] = declarations.pop('host_b')
    elif damage == 'wrong-digest':
        declarations['host_b'].retained_receipts[run.rollout_id] = '0' * 64
    elif damage == 'changed-hold':
        run.holds['host_b']['reason'] = 'changed after the retirement decision'
        fleet_rollout.save_run(run)
    elif damage == 'changed-receipt':
        run.legacy_retained_holds['host_b']['observed_outcome'] = 'success'
        fleet_rollout.save_run(run)
    elif damage == 'active-host':
        configured.add('host_b')
    elif damage == 'missing-repo':
        repo = None
    elif damage == 'unauthenticated-report':
        monkeypatch.setattr(fleet_rollout.fleet_release, 'discover_historical_report_by_digest',
                            lambda *a, **k: _report())
    else:
        key = next(iter(run.actions))
        run.actions['local-runtime:host_b:another-env'] = dict(run.actions[key])
        fleet_rollout.save_run(run)
    path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = path.read_bytes()
    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256=str(plan.report['digest_sha256']),
            current_report_source_path=str(plan.report['source_path']),
            inspect_only=False, allow_legacy_reconciliation=True,
            configured_retention_hosts=frozenset(configured),
            retired_hosts=declarations, report_repo=repo,
            control_runner=lambda *a, **k: pytest.fail('invalid evidence reached host control'),
        )
    assert path.read_bytes() == before


def test_live_host_receipt_still_requires_current_report_identity(tmp_path, monkeypatch):
    run, _, declarations = _retirement_case(tmp_path, monkeypatch)
    del declarations['host_b']
    with pytest.raises(fleet_rollout.FleetRolloutError, match='different accepted report'):
        fleet_rollout.reconcile_durable_operations(
            current_report_digest_sha256='c' * 64,
            current_report_source_path='releases/v0.15.132.json', inspect_only=True,
            configured_retention_hosts=frozenset({'localhost', 'host_b'}),
            retired_hosts=declarations, report_repo=tmp_path,
        )


def test_retirement_authenticates_frozen_older_report_without_rewriting_it(tmp_path, monkeypatch):
    run, _, declarations = _retirement_case(tmp_path, monkeypatch)
    path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = path.read_bytes()
    result = fleet_rollout.reconcile_durable_operations(
        current_report_digest_sha256='c' * 64,
        current_report_source_path='releases/v0.15.132.json', inspect_only=True,
        configured_retention_hosts=frozenset({'localhost'}),
        retired_hosts=declarations, report_repo=tmp_path,
    )
    assert {host for _, host, _ in result.retained_legacy_holds} == {'host_a', 'host_b'}
    assert path.read_bytes() == before


def test_retirement_leaves_live_peer_fence_and_rejects_mixed_group_rewrite(tmp_path, monkeypatch):
    run, plan, declarations = _retirement_case(tmp_path, monkeypatch)
    del declarations['host_b']
    kwargs = dict(
        current_report_digest_sha256=str(plan.report['digest_sha256']),
        current_report_source_path=str(plan.report['source_path']),
        configured_retention_hosts=frozenset({'localhost', 'host_b'}),
        retired_hosts=declarations, report_repo=tmp_path,
    )
    result = fleet_rollout.reconcile_durable_operations(inspect_only=True, **kwargs)
    assert {host for _, host, _ in result.retained_legacy_holds} == {'host_a', 'host_b'}
    before = fleet_rollout.rollout_state_path(run.rollout_id).read_bytes()
    with pytest.raises(fleet_rollout.FleetRolloutError, match='mixed live/retired'):
        fleet_rollout.reconcile_durable_operations(
            inspect_only=False, allow_legacy_reconciliation=True,
            control_runner=lambda *a, **k: pytest.fail('mixed group issued control'), **kwargs,
        )
    assert fleet_rollout.rollout_state_path(run.rollout_id).read_bytes() == before


def test_retirement_audit_command_only_prints_exact_bindings(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from vq.cli import main

    run, _, declarations = _retirement_case(tmp_path, monkeypatch)
    path = fleet_rollout.rollout_state_path(run.rollout_id)
    before = path.read_bytes()
    result = CliRunner().invoke(main, ['host', 'retirement-audit', 'host_a'])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        'host': 'host_a', 'retained_receipts': declarations['host_a'].retained_receipts,
    }
    assert path.read_bytes() == before
    missing = CliRunner().invoke(main, ['host', 'retirement-audit', 'misspelled-host'])
    assert missing.exit_code != 0 and 'no retained rollout evidence' in missing.output
