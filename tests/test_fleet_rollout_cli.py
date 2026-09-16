"""CLI contract for the input-free fleet rollout orchestrator."""

from __future__ import annotations

import json
import os
import subprocess
from contextlib import contextmanager, suppress
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import admin, config, fleet_operation, fleet_release, fleet_rollout
from vq.cli import main

SHA = "a" * 40


def _report(*, patch: int = 60) -> fleet_release.FleetReleaseReport:
    release_version = f"0.15.{patch}"
    release_tag = f"v{release_version}"
    pins = {
        name: fleet_release.FleetPin(
            name=name,
            sha=SHA,
            version=(
                release_version
                if name in {"release", "dev"}
                else "0.17.0" if name == "vq" else "2.5.0"
            ),
            tag=release_tag if name == "release" else None,
            deploy_flags=(
                ("--tag", release_tag, "--expected-sha", SHA)
                if name == "release"
                else ("--expected-sha", SHA)
            ),
            gating_job=fleet_release.PIN_GATING_JOBS[name],
            pipeline_id=4500,
            evidence_sha=SHA,
            acceptance_rule="A",
        )
        for name in fleet_release.PIN_NAMES
    }
    return fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path=f"vibe-queue/releases/{release_tag}.json",
        digest_sha256="b" * 64,
        generated_at="2026-07-25T12:00:00Z",
        release_version=(0, 15, patch),
        pins=pins,
        raw={"schema": "vq.fleet.release_report/2"},
    )


def _action(
    *,
    action_id: str,
    decision: fleet_rollout.Decision,
    phase: fleet_rollout.Phase = "driver",
    host: str = "localhost",
) -> fleet_rollout.RolloutAction:
    return fleet_rollout.RolloutAction(
        id=action_id,
        phase=phase,
        host=host,
        program="vibeqc-queue" if phase == "driver" else "vibeqc-dev",
        pin_name="vq" if phase == "driver" else "dev",
        target_sha=SHA,
        target_version="0.17.0" if phase == "driver" else "0.15.60",
        target_tag=None,
        argv=["admin", "update", "vibeqc-queue", "localhost"],
        decision=decision,
        reason=(
            "already at target with LAST OK=true"
            if decision == "skip"
            else "target is newer"
        ),
        before={
            "configured": True,
            "current_sha": SHA if decision == "skip" else "c" * 40,
            "current_version": "0.17.0",
            "current_tag": None,
            "dirty": False,
            "last_ok": decision == "skip",
            "acknowledged": False,
            "detail": "managed",
        },
    )


def _plan(
    report: fleet_release.FleetReleaseReport,
    actions: list[fleet_rollout.RolloutAction],
    *,
    extra_hosts: tuple[str, ...] = (),
    topology_errors: list[str] | None = None,
) -> fleet_rollout.RolloutPlan:
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report=fleet_release.report_summary(report),
        topology={
            name: {
                "name": name,
                "role": "managed",
                "canonical_host": None,
                "reason": "explicit config",
            }
            for name in ("localhost", *extra_hosts)
        },
        actions=actions,
        topology_errors=topology_errors or [],
    )


def _patch_discovery(
    monkeypatch,
    *,
    report: fleet_release.FleetReleaseReport,
    plans: list[fleet_rollout.RolloutPlan],
    extra_hosts: tuple[str, ...] = (),
    doctor: dict[str, Any] | None = None,
    build_kwargs: list[dict[str, Any]] | None = None,
) -> None:
    cfg = config.Config(
        fleet_report_repo="/repo",
        hosts={
            "localhost": config.HostConfig(
                ssh="localhost",
                fleet_role="managed",
            ),
            **{
                name: config.HostConfig(ssh=name, fleet_role="managed")
                for name in extra_hosts
            },
        },
        programs={
            "vibeqc-queue": config.VenvProgram(
                kind="venv",
                python="/repo/.venv/bin/python",
                git_dir="/repo",
                branch="main",
                update_script="vibe-queue/scripts/update.sh",
            ),
        },
    )
    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    monkeypatch.setattr("vq.cli.fleet_release.runtime_repo", lambda: Path("/repo"))
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_checkout",
        lambda path: Path("/repo"),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_target",
        lambda path: Path("/repo/.venv"),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        lambda: ('{"schema":"vq.toolset.lifecycle_handoff/1","locks":[]}', ()),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        lambda: (),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.driver_reentry_capability",
        lambda run, *, action_id: fleet_rollout.RolloutReentryCapability(
            rollout_id=fleet_rollout.rollout_id(report),
            operation_id="a" * 64,
            request_sha256="b" * 64,
            report_digest_sha256=str(report.digest_sha256),
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout._active_rollout_reentry_handoff",
        lambda capability, *, lifecycle_handoff: ("reentry-capability", (90, 91)),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_release.discover_latest_report",
        lambda repo, **kwargs: report,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_release.git_is_ancestor",
        lambda repo, older, newer: True,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module.source_tree_sha256_at_git_commit",
        lambda project_root, source_sha: "d" * 64,
        raising=False,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: (
            {},
            {},
            doctor
            or {
                name: {"ok": True, "checks": []}
                for name in ("localhost", *extra_hosts)
            },
        ),
    )
    remaining = iter(plans)

    def fake_build_plan(*args: Any, **kwargs: Any) -> fleet_rollout.RolloutPlan:
        if build_kwargs is not None:
            build_kwargs.append(dict(kwargs))
        return next(remaining)

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.build_plan",
        fake_build_plan,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_final_drain_liveness",
        lambda *args, **kwargs: {
            "status": "complete",
            "observed_at": "2026-08-11T12:00:01+00:00",
            "observed_hosts": [],
            "inactive_hosts": [],
            "active_holds": [],
            "unknown_hosts": [],
        },
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "garbage"),
        ("VQ_UPDATE_SCRIPT_TIMEOUT", "0"),
        ("VQ_BUILD_STALL_TIMEOUT", "-1"),
        ("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "garbage"),
        ("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "14999"),
    ],
)
def test_timeout_preflight_fails_before_rollout_discovery_ssh(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(name, value)
    touched: list[str] = []
    monkeypatch.setattr(
        "vq.cli.config.load_config",
        lambda: touched.append("config") or config.Config(),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: touched.append("snapshot") or ({}, {}, {}),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run"],
    )

    assert result.exit_code == 2
    assert name in result.output
    assert touched == []


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("mode", ["--dry-run", "--verify-only", "rollout"])
def test_fallback_report_is_visible_and_cannot_authorize_rollout_or_verification(
    monkeypatch: pytest.MonkeyPatch, as_json: bool, mode: str,
) -> None:
    report = replace(
        _report(),
        rejected_candidates=("releases/v0.15.61.json: pin ancestry failed",),
    )
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    if mode != "--dry-run":
        monkeypatch.setattr(
            fleet_rollout, "reconcile_durable_operations",
            lambda **kw: pytest.fail("fallback must stop before reconciliation"),
        )
        monkeypatch.setattr(
            fleet_rollout, "collect_snapshots",
            lambda **kw: pytest.fail("fallback must stop before host probes"),
        )
    result = CliRunner().invoke(main, [
        "admin", "rollout-latest",
        *([mode] if mode != "rollout" else []),
        *(["--json"] if as_json else []),
    ])
    assert result.exit_code == (0 if mode == "--dry-run" else 1), result.output
    assert "fallback v0.15.60" in result.output
    assert "v0.15.61.json: pin ancestry failed" in result.output
    if as_json:
        payload = json.loads(result.stdout)
        if mode == "--dry-run":
            assert payload["discovery_warnings"]
            assert payload["report"]["release_tag"] == "v0.15.60"
        else:
            assert payload["status"] == "error"
            assert "fallback cannot authorize" in payload["error"]
    elif mode == "--dry-run":
        assert "WARNING:" in result.output
        assert "rollout-latest dry run" in result.output


@pytest.mark.parametrize("as_json", [False, True])
def test_unfetched_newer_report_cannot_verify_older_fleet_as_converged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, as_json: bool,
) -> None:
    from tests.test_fleet_release import _split_discovery_repos

    driver, clones, _ = _split_discovery_repos(tmp_path)
    # Exercise actual report loading, including the newer report rejection.
    report = fleet_release.discover_latest_report(driver, fetch=False, pin_repos=clones)
    assert report.release.tag == "v0.15.59"
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    result = CliRunner().invoke(main, [
        "admin", "rollout-latest", "--verify-only", *(["--json"] if as_json else []),
    ])
    assert result.exit_code == 1, result.output
    assert "v0.15.60" in result.output
    assert "fallback v0.15.59" in result.output


def test_report_recheck_refuses_a_new_rejection_with_unchanged_selected_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    fallback = replace(report, rejected_candidates=("releases/v0.15.61.json: invalid pin",))
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    discoveries = iter((report, fallback))
    monkeypatch.setattr(
        fleet_release, "discover_latest_report", lambda *a, **kw: next(discoveries),
    )
    checked = []

    def execute(*args, report_digest_resolver, **kwargs):
        checked.append(True)
        report_digest_resolver()
        pytest.fail("same fallback digest must not authorize continued execution")

    monkeypatch.setattr(fleet_rollout, "execute_plan", execute)
    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])
    assert checked == [True]
    assert result.exit_code == 1, result.output
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert "fallback cannot authorize" in payload["error"]


def test_from_report_refuses_an_older_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import click

    from vq.cli import _pin_deploy_identity

    report = replace(_report(), rejected_candidates=("releases/v0.15.61.json: invalid pin",))
    reports = tmp_path / "private-reports"
    subprocess.run(["git", "init", "-q", str(reports)], check=True)
    monkeypatch.setattr(fleet_release, "report_repo", lambda cfg=None: reports)
    monkeypatch.setattr(fleet_release, "discover_latest_report", lambda *a, **kw: report)
    with pytest.raises(click.UsageError, match="fallback cannot authorize"):
        _pin_deploy_identity("vibeqc-queue", expected_sha=None, expected_tag=None)


def test_failure_epoch_refuses_fallback_before_reading_refs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = replace(_report(), rejected_candidates=("releases/v0.15.61.json: invalid pin",))
    monkeypatch.setattr(fleet_release, "discover_latest_report", lambda *a, **kw: report)
    monkeypatch.setattr(
        fleet_rollout.subprocess, "run", lambda *a, **kw: pytest.fail("must refuse fallback"),
    )
    with pytest.raises(fleet_rollout.FleetRolloutError, match="fallback cannot authorize"):
        fleet_rollout._observe_local_failure_report_epoch(Path("/repo"))


def test_dry_run_is_json_and_performs_no_update(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("mutated")),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_plan/2"
    assert payload["report"]["release_tag"] == "v0.15.60"
    assert payload["summary"]["update"] == 0


def test_rollout_derives_the_accepted_vq_tree_once_and_threads_it_to_planning(
    monkeypatch,
) -> None:
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(report, [_action(action_id="driver", decision="skip")])
    build_kwargs: list[dict[str, Any]] = []
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[initial, final],
        build_kwargs=build_kwargs,
    )
    calls: list[tuple[Path, str]] = []

    def digest(project_root: Path, source_sha: str) -> str:
        calls.append((project_root, source_sha))
        return "e" * 64

    monkeypatch.setattr(
        "vq.cli.admin_module.source_tree_sha256_at_git_commit",
        digest,
        raising=False,
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.execute_plan", lambda *a, **k: run)
    monkeypatch.setattr("vq.cli.fleet_rollout.save_run", lambda value: Path("/state"))

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    # The vq package root is DETECTED, not assumed to be <repo>/vibe-queue:
    # since the split the runtime checkout IS vibe-queue. This stub has
    # neither layout on disk, so resolution falls back to the root and lets
    # git report the real problem rather than a layout complaint.
    assert calls == [(Path("/repo"), SHA)]
    assert [item["target_vq_tree_sha256"] for item in build_kwargs] == [
        "e" * 64,
        "e" * 64,
    ]


def test_rollout_refuses_an_unhashable_accepted_vq_object_before_snapshots(
    monkeypatch,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.admin_module.source_tree_sha256_at_git_commit",
        lambda *args: (_ for _ in ()).throw(admin.AdminError("archive corrupt")),
        raising=False,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: (_ for _ in ()).throw(AssertionError("snapshot must not run")),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_result/2"
    assert "accepted vq package digest" in payload["error"]
    assert "archive corrupt" in payload["error"]


def test_driver_updates_first_then_reenters_fresh_vq(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="update")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.load_run",
        lambda rollout_id: fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_source_path=report.source_path,
            report_digest_sha256=report.digest_sha256,
        ),
    )
    calls: list[tuple[str, Any]] = []

    def execute_one(*args: Any, **kwargs: Any) -> None:
        calls.append(("update", args[1].id))

    def reenter(
        rollout_id: str,
        *,
        python: str | None = None,
        reentry_handoff: str | None = None,
        pass_fds: tuple[int, ...] = (),
        as_json: bool,
        selection: fleet_rollout.HostSelection | None = None,
        reconcile_legacy: bool = False,
    ) -> int:
        assert reconcile_legacy is False
        assert python == "/repo/.venv/bin/python"
        assert reentry_handoff == "reentry-capability"
        assert pass_fds == (90, 91)
        calls.append(("reenter", (rollout_id, as_json, selection)))
        return 7

    monkeypatch.setattr("vq.cli.fleet_rollout.execute_one", execute_one)
    monkeypatch.setattr("vq.cli.fleet_rollout.reenter_after_driver", reenter)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 7
    assert calls[0] == ("update", "driver")
    assert calls[1][0] == "reenter"
    assert calls[1][1][1] is True


def test_resume_without_inherited_capability_is_rejected(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="update")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    rollout_id = fleet_rollout.rollout_id(report)

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--resume", rollout_id, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_result/2"
    assert payload["status"] == "error"
    assert "requires the inherited rollout and lifecycle lock capability" in (
        payload["error"]
    )


@pytest.mark.parametrize("as_json", [False, True])
def test_malformed_reentry_lifecycle_capability_uses_rollout_error_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    as_json: bool,
) -> None:
    report = _report()
    rollout_id = fleet_rollout.rollout_id(report)
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    lock_path = tmp_path / ".fleet-rollout.lock"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    payload = {
        "schema": "vq.rollout.reentry_handoff/1",
        "rollout_id": rollout_id,
        "operation_id": "a" * 64,
        "request_sha256": "b" * 64,
        "report_digest_sha256": report.digest_sha256,
        "parent_pid": os.getppid(),
        "parent_start_ticks": fleet_operation._pid_start_ticks(os.getppid()),
        "parent_boot_id": fleet_operation._boot_id(),
        "rollout_lock": {"fd": lock_fd, "path": str(lock_path)},
        # The outer envelope is complete, but the lifecycle capability is not:
        # a controller successor must inherit both checkout and target locks.
        "lifecycle": {
            "schema": "vq.toolset.lifecycle_handoff/1",
            "locks": [],
        },
    }
    monkeypatch.setenv(
        fleet_rollout.ENV_ROLLOUT_REENTRY_HANDOFF,
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout._validate_inherited_rollout_lock",
        lambda fd, path: None,
    )
    argv = ["admin", "rollout-latest", "--resume", rollout_id]
    if as_json:
        argv.append("--json")

    try:
        result = CliRunner().invoke(main, argv)
    finally:
        # Adoption closes a valid inherited descriptor even when the nested
        # lifecycle envelope is rejected.
        with suppress(OSError):
            os.close(lock_fd)

    assert result.exit_code == 1
    if as_json:
        error = json.loads(result.output)["error"]
    else:
        assert result.output.startswith("Error: ")
        error = result.output
    assert "invalid rollout re-entry lifecycle handoff" in error
    assert "invalid fields" in error


def test_complete_run_rechecks_live_state_and_emits_result(monkeypatch) -> None:
    report = _report()
    initial = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime",
                decision="update",
                phase="local-runtime",
            ),
        ],
    )
    final = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime",
                decision="skip",
                phase="local-runtime",
            ),
        ],
    )
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan",
        lambda *args, **kwargs: run,
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.save_run", lambda value: Path("/state"))

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_result/2"
    assert payload["status"] == "complete"
    assert payload["retained_rollout_holds"] == []
    lanes = {lane["module"]: lane for lane in payload["lanes"]}
    assert lanes["vibeqc-dev"]["changed"] is True
    assert lanes["vibeqc-dev"]["sha"] == SHA
    assert payload["journal"]["complete"] is True
    assert payload["drain_liveness"]["status"] == "complete"


def test_final_drain_sweep_runs_after_finalize_and_is_not_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    order: list[str] = []
    saved: list[dict[str, Any]] = []
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan",
        lambda *args, **kwargs: run,
    )

    def finalize(value: fleet_rollout.RolloutRun, **kwargs: Any) -> fleet_rollout.RolloutRun:
        del kwargs
        order.append("finalize")
        value.complete = True
        return value

    def collect(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        order.append("sweep")
        return {
            "status": "complete",
            "observed_at": "2026-08-11T12:00:01+00:00",
            "observed_hosts": [
                {
                    "host": "localhost",
                    "control_host": "localhost",
                    "remote_observed_at": "2026-08-11T12:00:00+00:00",
                    "controller_observed_at": "2026-08-11T12:00:01+00:00",
                }
            ],
            "inactive_hosts": ["localhost"],
            "active_holds": [],
            "unknown_hosts": [],
        }

    monkeypatch.setattr("vq.cli.fleet_rollout.finalize_run", finalize)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_final_drain_liveness",
        collect,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.save_run",
        lambda value: saved.append(value.as_dict()) or Path("/state"),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert order == ["finalize", "sweep"]
    assert payload["drain_liveness"]["inactive_hosts"] == ["localhost"]
    assert all("drain_liveness" not in item for item in saved)


@pytest.mark.parametrize("mode", ["--dry-run", "--verify-only"])
def test_read_only_rollout_modes_never_run_final_drain_sweep(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_final_drain_liveness",
        lambda *args, **kwargs: pytest.fail("read-only mode ran final sweep"),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", mode, "--json"],
    )

    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(
    ("final_decision", "expected_exit"),
    [("skip", 0), ("update", 2)],
)
def test_final_drain_sweep_exception_is_additive_to_primary_exit(
    monkeypatch: pytest.MonkeyPatch,
    final_decision: fleet_rollout.Decision,
    expected_exit: int,
) -> None:
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(
        report,
        [_action(action_id="driver", decision=final_decision)],
    )
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan",
        lambda *args, **kwargs: run,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.save_run",
        lambda value: Path("/state"),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_final_drain_liveness",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("secret")),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == expected_exit
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_result/2"
    assert payload["drain_liveness"]["status"] == "unavailable"
    assert payload["drain_liveness"]["unknown_hosts"] == [
        {
            "host": "localhost",
            "control_host": "unknown",
            "reason": "final read-only drain sweep failed",
        }
    ]
    assert "secret" not in result.output


@pytest.mark.parametrize("as_json", [True, False])
def test_scoped_publish_then_failure_emits_result_before_exit_one(
    monkeypatch,
    as_json: bool,
) -> None:
    report = _report()
    initial = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="worker-runtime",
                decision="update",
                phase="local-runtime",
                host="worker",
            ),
        ],
        extra_hosts=("worker",),
    )
    final = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="worker-runtime",
                decision="skip",
                phase="local-runtime",
                host="worker",
            ),
        ],
        extra_hosts=("worker",),
    )
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[initial, final],
        extra_hosts=("worker",),
    )
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
        actions={"worker-runtime": {"status": "failed", "rc": 9}},
        holds={
            "worker": {
                "host": "worker",
                "kind": "full",
                "owned": True,
                "status": "active",
                "reason": "vq rollout-latest retained for worker",
                "set_at": "2026-08-10T12:00:00Z",
            }
        },
        failed_hosts={"worker": "command exited 9 after publishing target"},
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan", lambda *args, **kwargs: run
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.save_run", lambda value: Path("/state")
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_final_drain_liveness",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("secret")),
    )

    argv = ["admin", "rollout-latest", "--only", "worker"]
    if as_json:
        argv.append("--json")
    result = CliRunner().invoke(main, argv)

    assert result.exit_code == 1
    if as_json:
        payload = json.loads(result.output)
        assert payload["schema"] == "vq.fleet.rollout_result/2"
        assert payload["status"] == "deferred"
        assert payload["selection"]["verdict"] == "degraded"
        assert "error" not in payload
        assert payload["failed_hosts"] == {
            "worker": "command exited 9 after publishing target"
        }
        assert payload["retained_rollout_holds"] == [
            {
                "host": "worker",
                "kind": "full",
                "reason": "vq rollout-latest retained for worker",
                "status": "active",
            }
        ]
        assert payload["journal"]["complete"] is False
        assert payload["drain_liveness"]["status"] == "unavailable"
    else:
        assert "rollout-latest modeled lanes deferred" in result.output
        assert (
            "FAILED HOST worker: command exited 9 after publishing target"
            in result.output
        )
        assert (
            "RETAINED ROLLOUT HOLD worker full journal_status=active "
            "reason=vq rollout-latest retained for worker "
            "(journal evidence; current liveness not asserted)"
            in result.output
        )
        assert "Error: rollout action failed or remained unfinished" not in result.output


def test_owned_hold_finalization_mismatch_emits_result_before_exit_one(
    monkeypatch,
) -> None:
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
        holds={
            "localhost": {
                "host": "localhost",
                "kind": "scheduler-target",
                "owned": True,
                "status": "cleanup-failed",
                "reason": "vq rollout-latest retained for localhost",
            }
        },
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan", lambda *args, **kwargs: run
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.save_run", lambda value: Path("/state")
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["schema"] == "vq.fleet.rollout_result/2"
    assert payload["status"] == "deferred"
    assert payload["failed_hosts"] == {}
    assert "error" not in payload
    assert payload["retained_rollout_holds"] == [
        {
            "host": "localhost",
            "kind": "scheduler-target",
            "reason": "vq rollout-latest retained for localhost",
            "status": "cleanup-failed",
        }
    ]
    assert payload["journal"]["complete"] is False


def test_resume_rejects_a_report_switch(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    old_rollout = "v0.15.59-old"
    capability = fleet_rollout.RolloutReentryCapability(
        rollout_id=old_rollout,
        operation_id="a" * 64,
        request_sha256="b" * 64,
        report_digest_sha256="c" * 64,
    )

    @contextmanager
    def adopt(**kwargs: Any):
        assert kwargs["expected_rollout_id"] == old_rollout
        yield capability

    def reconcile(**kwargs: Any) -> fleet_rollout.OperationReconciliation:
        assert kwargs["acknowledge_driver_reentry"] == old_rollout
        assert kwargs["authenticated_driver_reentry"] == capability
        return fleet_rollout.OperationReconciliation()

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.adopt_rollout_reentry_handoff",
        adopt,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        reconcile,
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--resume", old_rollout, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert "accepted report changed" in payload["error"]


def test_real_report_discovery_config_migration_and_planner_dry_run(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Exercise the layers together; only fleet I/O is replaced by snapshots."""
    repo = tmp_path / "runtime"
    repo.mkdir()

    def git(*args: str) -> str:
        proc = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True,
            text=True,
            check=True,
        )
        return proc.stdout.strip()

    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.invalid")
    (repo / "README").write_text("runtime\n")
    package = repo / "vibe-queue" / "src" / "vq"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        '__version__ = "0.17.0"\n',
        encoding="utf-8",
    )
    # A real checkout carries a pyproject.toml beside src/vq, and that pair is
    # what identifies the vq package root in either layout. Without it the
    # fixture resembles no checkout that exists.
    (repo / "vibe-queue" / "pyproject.toml").write_text(
        '[project]\nname = "vq"\nversion = "0.17.0"\n', encoding="utf-8"
    )
    git("add", "README", "vibe-queue/src/vq/__init__.py",
        "vibe-queue/pyproject.toml")
    git("commit", "-m", "release")
    sha = git("rev-parse", "HEAD")
    git("tag", "v0.15.60")

    def raw_pin(name: str, version: str) -> dict[str, Any]:
        repo_slug, project_id = fleet_release.PIN_SOURCES[name]
        return {
            "repo": repo_slug,
            "project_id": project_id,
            "accepted": True,
            "acceptance_rule": "A",
            "sha": sha,
            "version": version,
            "tag": "v0.15.60" if name == "release" else None,
            "tag_object": sha if name == "release" else None,
            "deploy_flags": (
                ["--tag", "v0.15.60", "--expected-sha", sha]
                if name == "release"
                else ["--expected-sha", sha]
            ),
            "ci_evidence": {
                "pipeline_id": 4500,
                "pipeline_status": "success",
                # Release-gate ref: a main pipeline does not prove a release
                # tree and parse_report refuses one.
                "ref": "v0.15.60",
                "sha": sha,
                "gating_job": fleet_release.PIN_GATING_JOBS[name],
                "gating_job_status": "success",
                "web_url": "https://gitlab.example/pipelines/4500",
            },
        }

    raw = {
        "schema": fleet_release.REPORT_SCHEMA,
        "generated_at": "2026-07-25T12:00:00Z",
        "all_pins_accepted": True,
        "pins": {
            "release": raw_pin("release", "0.15.60"),
            "dev": raw_pin("dev", "0.15.60"),
            "vq": raw_pin("vq", "0.17.0"),
            "vibe_view": raw_pin("vibe_view", "2.5.0"),
        },
    }
    from tests.test_fleet_release import _commit, _git, _init_repo

    operations = tmp_path / "operations"
    operations.mkdir()
    reports = _init_repo(operations)
    report_path = reports / fleet_release.REPORT_DIRECTORY / "v0.15.60.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n")
    _commit(reports, "accepted report")
    _git(reports, "update-ref", "refs/remotes/origin/main", "HEAD")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    target_vq_tree_sha256 = admin.source_tree_sha256_at_git_commit(
        repo / "vibe-queue",
        sha,
    )

    cfg = config.Config(
        fleet_rollout_order=["host_f", "localhost"],
        # A /3 report pins four components across three repositories, so the
        # loader needs a checkout for each slug. The fixture keeps one repo,
        # so every slug points at it.
        pin_source_repos={
            slug: str(repo) for slug, _ in fleet_release.PIN_SOURCES.values()
        },
        hosts={
            "localhost": config.HostConfig(
                ssh="localhost",
                fleet_role="managed",
            ),
            "host_f": config.HostConfig(
                ssh="host_f",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                fleet_role="managed",
            ),
            "host_f-big": config.HostConfig(
                ssh="host_f",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                fleet_role="alias",
                fleet_canonical_host="host_f",
            ),
            "coordinator": config.HostConfig(
                ssh="coordinator",
                fleet_role="vq-only",
            ),
        },
        programs={
            "vibeqc-queue": config.VenvProgram(
                kind="venv",
                python=str(repo / ".venv" / "bin" / "python"),
                git_dir=str(repo),
                branch="main",
                update_script="vibe-queue/scripts/update.sh",
            ),
        },
    )
    envs = [
            {
                "name": name,
                "error": None,
                "current_sha": sha[:12],
                "is_dirty": False,
                "last_success": True,
                "last_sha": sha[:12],
                "last_expected_sha": sha,
                "last_installed_sha": sha[:12],
                "last_tag": "v0.15.60" if name == "vibeqc-release" else None,
            "last_marked_ok_at": None,
        }
        for name in (
            "vibeqc-queue",
            "vibeqc-release",
            "vibeqc-dev",
            "vibe-view",
        )
    ]
    programs = [
        {
            "name": name,
            "status": "OK",
            "current_git_sha_full": sha,
            "current_git_dirty": False,
            "import_version": (
                "0.17.0"
                if name == "vibeqc-queue"
                else "2.5.0" if name == "vibe-view" else "0.15.60"
            ),
        }
        for name in (
            "vibeqc-queue",
            "vibeqc-release",
            "vibeqc-dev",
            "vibe-view",
        )
    ]
    snapshots = (
        {
            "localhost": {"envs": envs, "marker": None},
            "host_f": {"deployments": {}, "marker": None},
            "coordinator": {
                "envs": [
                    {
                        **envs[0],
                        "installed_sha_matches_checkout": True,
                    }
                ],
                "marker": None,
            },
        },
        {
            "localhost": programs,
            "coordinator": [{**programs[0], "kind": "venv"}],
        },
        {
            "localhost": {"ok": True, "checks": []},
            "host_f": {
                "ok": True,
                "checks": [
                    {
                        "name": "scheduler_remote_vq",
                        "ok": True,
                        "source_sha": sha,
                    },
                    {"name": "scheduler_liveness", "ok": True},
                ],
            },
            "host_f-big": {"ok": True, "checks": []},
            "coordinator": {
                "ok": True,
                "checks": [
                    {
                        "name": "daemon_rpc",
                        "ok": True,
                        "version": "0.17.0",
                        "source_sha": sha,
                        "source_tree_sha256": target_vq_tree_sha256,
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
                            "error": "systemd unavailable",
                        },
                    }
                ],
            },
        },
    )
    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    cfg.fleet_report_repo = str(reports)
    monkeypatch.setattr("vq.cli.fleet_release.runtime_repo", lambda: repo)
    discover = fleet_release.discover_latest_report
    monkeypatch.setattr(
        "vq.cli.fleet_release.discover_latest_report",
        lambda runtime, **kwargs: discover(
            runtime,
            fetch=False,
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: snapshots,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._canonical_lifecycle_target",
        lambda path: repo / ".venv",
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        lambda: ('{"schema":"vq.toolset.lifecycle_handoff/1","locks":[]}', ()),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        lambda: (),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["report"]["release_tag"] == "v0.15.60"
    assert payload["summary"]["update"] == 0
    assert payload["topology"]["host_f-big"]["role"] == "alias"
    assert not any(action["host"] == "host_f-big" for action in payload["actions"])
    coordinator = [
        action for action in payload["actions"] if action["host"] == "coordinator"
    ]
    assert len(coordinator) == 1
    assert coordinator[0]["program"] == "vibeqc-queue"
    assert coordinator[0]["decision"] == "skip"
    assert payload["coverage"]["vq_user_lanes"]["converged"] == 1


# --- --verify-only: one machine-checkable convergence verdict ---------------


def test_verify_only_reports_converged_and_changes_nothing(monkeypatch) -> None:
    # Verification never delegates an update, so update-only settings cannot
    # make the read-only convergence verdict fail.
    monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "garbage")
    monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "-1")
    monkeypatch.setenv("VQ_REMOTE_ADMIN_UPDATE_TIMEOUT", "also-garbage")
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    for name in ("execute_plan", "execute_one", "save_run"):
        monkeypatch.setattr(
            f"vq.cli.fleet_rollout.{name}",
            lambda *a, **k: (_ for _ in ()).throw(AssertionError("mutated")),
        )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--verify-only", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    # /2 (2026-08-06): adds console_failures. A stale web console is a
    # real convergence failure -- nothing in the rollout knew the console
    # existed, so this verdict read "converged" for two weeks while the
    # coordinator served 1081-commit-stale pages.
    assert payload["schema"] == "vq.fleet.rollout_verify/3"
    assert payload["verdict"] == "converged"
    assert payload["degraded_hosts"] == {}


def test_verify_only_exits_2_and_names_the_degraded_hosts(monkeypatch) -> None:
    report = _report()
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime:host_d:vibeqc-dev",
                decision="update",
                phase="local-runtime",
                host="host_d",
            ),
        ],
        extra_hosts=("host_d",),
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan], extra_hosts=("host_d",))

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--verify-only", "--json"],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    assert payload["verdict"] == "degraded"
    assert list(payload["degraded_hosts"]) == ["host_d"]


def test_verify_only_reports_a_blocked_plan_rather_than_erroring(monkeypatch) -> None:
    """Placement guard: the branch must precede the blocked-plan raise.

    A verification command that cannot report on a blocked fleet is useless --
    "blocked" is exactly the state an operator reaches for it to describe.
    """
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
        topology_errors=["mystery: auto: no managed lane found"],
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan])

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--verify-only", "--json"],
    )

    assert result.exit_code == 2
    payload = json.loads(result.output)
    # /2 (2026-08-06): adds console_failures. A stale web console is a
    # real convergence failure -- nothing in the rollout knew the console
    # existed, so this verdict read "converged" for two weeks while the
    # coordinator served 1081-commit-stale pages.
    assert payload["schema"] == "vq.fleet.rollout_verify/3"
    assert payload["verdict"] == "degraded"
    assert payload["degraded_hosts"]["mystery"] == [
        "topology: auto: no managed lane found"
    ]


def test_verify_only_never_reenters_through_a_driver_self_update(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="update")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reenter_after_driver",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("reentered")),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--verify-only"])

    assert result.exit_code == 2
    assert "degraded" in result.output


def test_verify_only_and_dry_run_are_mutually_exclusive(monkeypatch) -> None:
    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--verify-only"],
    )

    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_verify_only_renders_one_verdict_line_in_text_mode(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--verify-only"])

    assert result.exit_code == 0, result.output
    assert result.output.splitlines()[0] == (
        "rollout-latest verify: modeled lanes converged"
    )
    assert "whole-fleet convergence not asserted" in result.output


# --- --only / --skip: host-scoped execution ---------------------------------


def test_only_and_skip_are_rejected_together(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--only", "localhost", "--skip", "localhost"],
    )

    assert result.exit_code == 1
    assert "mutually exclusive" in result.output


def test_an_unknown_only_host_fails_closed(monkeypatch) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--only", "nosuchhost", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert "unknown host" in payload["error"]


def test_a_scoped_dry_run_defers_the_unselected_lanes(monkeypatch) -> None:
    report = _report()
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime:host_d:vibeqc-dev",
                decision="update",
                phase="local-runtime",
                host="host_d",
            ),
            _action(
                action_id="local-runtime:localhost:vibeqc-dev",
                decision="update",
                phase="local-runtime",
            ),
        ],
        extra_hosts=("host_d",),
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan], extra_hosts=("host_d",))

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--only", "host_d", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["selection"] == {
        "only": ["host_d"],
        "skip": [],
        "scoped": True,
    }
    by_id = {action["id"]: action for action in payload["actions"]}
    assert by_id["local-runtime:host_d:vibeqc-dev"]["decision"] == "update"
    assert by_id["local-runtime:localhost:vibeqc-dev"]["decision"] == "defer"
    assert "out of rollout scope" in by_id["local-runtime:localhost:vibeqc-dev"]["reason"]
    # The pinned argv is untouched -- that is the point of staying on this path.
    assert by_id["local-runtime:host_d:vibeqc-dev"]["argv"] == [
        "admin",
        "update",
        "vibeqc-queue",
        "localhost",
    ]


@pytest.mark.parametrize(
    ("scope_args", "expected_selection"),
    [
        (
            ("--only", "host_f", "--only", "host_c"),
            {"only": ["host_f", "host_c"], "skip": [], "scoped": True},
        ),
        (
            ("--skip", "host_a"),
            {"only": [], "skip": ["host_a"], "scoped": True},
        ),
    ],
)
def test_scoped_dry_run_retains_stale_receipt_on_unselected_offline_host(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    scope_args: tuple[str, ...],
    expected_selection: dict[str, Any],
) -> None:
    report = _report(patch=148)
    hosts = ("host_f", "host_c", "host_a")
    actions = [_action(action_id="driver", decision="skip")]
    actions.extend(
        _action(
            action_id=f"local-runtime:{host}:vibeqc-dev",
            decision="update",
            phase="local-runtime",
            host=host,
        )
        for host in hosts
    )
    plan = _plan(report, actions, extra_hosts=hosts)
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[plan],
        extra_hosts=hosts,
    )
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)

    hold = {
        "host": "host_a",
        "kind": "full",
        "owned": True,
        "status": "active",
        "reason": "legacy rollout hold",
    }
    old = fleet_rollout.RolloutRun(
        rollout_id="v0.15.118-b9ea64e76214",
        report_digest_sha256="d" * 64,
        report_source_path="vibe-queue/releases/v0.15.118.json",
        holds={"host_a": hold},
    )
    receipt_plan = _plan(report, [])
    receipt_plan.report["source_path"] = "vibe-queue/releases/v0.15.147.json"
    receipt_plan.report["digest_sha256"] = "c" * 64
    old.legacy_retained_holds["host_a"] = (
        fleet_rollout._legacy_retained_hold_receipt(
            old,
            host="host_a",
            hold=hold,
            plan=receipt_plan,
            source="hold-observation",
            control_host="host_a",
            retained_at="2026-08-27T12:00:00+00:00",
            reason="host_a is unreachable; exact hold remains retained",
        )
    )
    state_path = fleet_rollout.save_run(old)
    before = state_path.read_bytes()

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", *scope_args, "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["selection"] == expected_selection
    assert payload["retained_legacy_fences"] == [
        {
            "host": "host_a",
            "reason": "host_a is unreachable; exact hold remains retained",
            "rollout_id": old.rollout_id,
        }
    ]
    by_host = {action["host"]: action for action in payload["actions"]}
    assert by_host["host_f"]["decision"] == "update"
    assert by_host["host_c"]["decision"] == "update"
    assert by_host["host_a"]["decision"] == "defer"
    assert state_path.read_bytes() == before


def test_scoped_receipt_exception_never_includes_scheduler_driver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    hosts = ("host_f", "host_a")
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime:host_f:vibeqc-dev",
                decision="update",
                phase="local-runtime",
                host="host_f",
            ),
            _action(
                action_id="local-runtime:host_a:vibeqc-dev",
                decision="update",
                phase="local-runtime",
                host="host_a",
            ),
        ],
        extra_hosts=hosts,
    )
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[plan],
        extra_hosts=hosts,
    )
    cfg = config.Config(
        fleet_report_repo="/repo",
        hosts={
            "localhost": config.HostConfig(
                ssh="localhost",
                fleet_role="managed",
            ),
            "host_f": config.HostConfig(
                ssh="host_f",
                scheduler="pbs",
                scheduler_dialect="torque",
                scratch_root="/scratch/user",
                scheduler_driver="localhost",
                scheduler_update_command="/site/update-helper",
                fleet_role="managed",
            ),
            "host_a": config.HostConfig(ssh="host_a", fleet_role="managed"),
        },
        programs={
            "vibeqc-queue": config.VenvProgram(
                kind="venv",
                python="/repo/.venv/bin/python",
                git_dir="/repo",
                branch="main",
                update_script="vibe-queue/scripts/update.sh",
            ),
        },
    )
    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)

    def reconcile(**kwargs: Any) -> fleet_rollout.OperationReconciliation:
        assert kwargs["allowed_retention_identity_mismatch_hosts"] == frozenset(
            {"host_a"}
        )
        assert kwargs["configured_retention_hosts"] == frozenset(
            {"localhost", "host_f", "host_a"}
        )
        return fleet_rollout.OperationReconciliation(
            retained_legacy_holds=(
                ("v0.15.118-old", "localhost", "driver receipt is stale"),
            )
        )

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        reconcile,
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--dry-run", "--only", "host_f", "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    driver = next(
        action for action in payload["actions"] if action["phase"] == "driver"
    )
    assert driver["decision"] == "block"


def test_plan_bound_hold_supersede_cli_is_exact_and_recovery_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(patch=157)
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="scheduler-runtime:host_f:vibeqc-release",
                decision="skip",
                phase="scheduler-runtime",
                host="host_f",
            ),
        ],
        extra_hosts=("host_f",),
    )
    plan._scheduler_hold_targets = {
        "host_f": (("host_f-big", "localhost"),)
    }
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[plan],
        extra_hosts=("host_f", "host_f-big"),
    )
    calls: list[dict[str, Any]] = []

    def supersede(**kwargs: Any) -> fleet_rollout.PlanBoundHoldSupersession:
        calls.append(kwargs)
        return fleet_rollout.PlanBoundHoldSupersession(
            rollout_id="v0.15.155-deadbeefdead",
            host="host_f-big",
            current_rollout_id=fleet_rollout.rollout_id(report),
            replayed=False,
        )

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.supersede_obsolete_plan_bound_hold",
        supersede,
        raising=False,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: pytest.fail(
            "targeted supersession must not run fleet-global reconciliation"
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_plan",
        lambda *args, **kwargs: pytest.fail(
            "targeted supersession must never execute a rollout action"
        ),
    )

    result = CliRunner().invoke(
        main,
        [
            "admin",
            "rollout-latest",
            "--supersede-plan-hold",
            "v0.15.155-deadbeefdead",
            "host_f-big",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert len(calls) == 1
    assert calls[0]["rollout_id_value"] == "v0.15.155-deadbeefdead"
    assert calls[0]["host"] == "host_f-big"
    assert calls[0]["accepted_report"] is report
    assert calls[0]["plan"] is plan
    payload = json.loads(result.output)
    assert payload == {
        "current_rollout_id": fleet_rollout.rollout_id(report),
        "host": "host_f-big",
        "replayed": False,
        "rollout_id": "v0.15.155-deadbeefdead",
        "schema": "vq.fleet.plan_hold_supersede_result/1",
        "status": "recorded",
    }


def test_a_selection_matching_no_lane_fails_closed(monkeypatch) -> None:
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
        extra_hosts=("host_d",),
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan], extra_hosts=("host_d",))

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--only", "host_d", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert "no lane in this plan" in payload["error"]


def test_selection_survives_the_driver_self_update_reexec(monkeypatch) -> None:
    """A scoped run whose driver self-updates must stay scoped in the child."""
    report = _report()
    plan = _plan(
        report,
        [
            _action(action_id="driver", decision="update"),
            _action(
                action_id="local-runtime:host_d:vibeqc-dev",
                decision="update",
                phase="local-runtime",
                host="host_d",
            ),
        ],
        extra_hosts=("host_d",),
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan], extra_hosts=("host_d",))
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.load_run",
        lambda rollout_id: fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_source_path=report.source_path,
            report_digest_sha256=report.digest_sha256,
        ),
    )
    seen: list[Any] = []

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_one",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reenter_after_driver",
        lambda rollout_id, **kwargs: (
            pytest.fail("missing exact re-entry handoff")
            if kwargs.get("reentry_handoff") != "reentry-capability"
            else None
        )
        or (
            pytest.fail("missing configured interpreter")
            if kwargs.get("python") != "/repo/.venv/bin/python"
            else None
        )
        or (
            pytest.fail("missing inherited descriptors")
            if kwargs.get("pass_fds") != (90, 91)
            else None
        )
        or (
            pytest.fail("unexpected legacy reconciliation")
            if kwargs.get("reconcile_legacy") is not False
            else None
        )
        or (
            pytest.fail("unexpected text-mode selection")
            if kwargs.get("as_json") is not False
            else None
        )
        or (
            seen.append(kwargs.get("selection")) or 0
        ),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--only", "host_d"],
    )

    assert result.exit_code == 0
    assert seen == [fleet_rollout.HostSelection(only=("host_d",))]


def test_reenter_after_driver_forwards_the_selection_flags() -> None:
    """The argv the child actually receives, not just the parameter."""
    recorded: list[list[str]] = []

    class _Proc:
        returncode = 0

    def runner(argv, **kwargs):
        recorded.append(list(argv))
        return _Proc()

    fleet_rollout.reenter_after_driver(
        "v0.15.60-abcdef123456",
        as_json=True,
        selection=fleet_rollout.HostSelection(only=("host_d", "host_a")),
        reconcile_legacy=True,
        runner=runner,
    )

    assert recorded[0][-6:] == [
        "--only",
        "host_d",
        "--only",
        "host_a",
        "--reconcile-legacy",
        "--json",
    ]
    assert recorded[0][-8:-6] == ["--resume", "v0.15.60-abcdef123456"]
    assert "--skip" not in recorded[0]


def test_global_rollout_lock_precedes_the_first_snapshot_and_spans_finalize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-report adoption, planning, mutation, and proof share one fence."""
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    events: list[str] = []
    locked = False

    @contextmanager
    def lock(rollout_id: str):
        nonlocal locked
        events.append(f"lock:{rollout_id}")
        locked = True
        try:
            yield
        finally:
            events.append("unlock")
            locked = False

    snapshots = iter((({}, {}, {"localhost": {"ok": True, "checks": []}}),) * 2)

    def collect_snapshots():
        assert locked
        events.append("snapshot")
        return next(snapshots)

    def reconcile(**kwargs: Any):
        assert locked
        events.append("reconcile")
        return fleet_rollout.OperationReconciliation()

    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )

    def execute_plan(*args: Any, **kwargs: Any):
        assert locked
        assert kwargs["retain_scheduler_holds"] is True
        events.append("execute")
        return run

    def reconcile_parity(*args: Any, **kwargs: Any):
        assert locked
        assert args == (initial, final)
        assert kwargs["run"] is run
        events.append("parity")
        return run

    def finalize_run(*args: Any, **kwargs: Any):
        assert locked
        events.append("finalize")
        run.complete = True
        return run

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", lock)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations", reconcile
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.collect_snapshots", collect_snapshots)
    monkeypatch.setattr("vq.cli.fleet_rollout.execute_plan", execute_plan)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_scheduler_parity_holds",
        reconcile_parity,
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.finalize_run", finalize_run)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    assert events == [
        "lock:discover-latest",
        "reconcile",
        "snapshot",
        "execute",
        "snapshot",
        "parity",
        "finalize",
        "unlock",
    ]


def test_controller_lifecycle_fence_and_handoff_span_execute_plan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    initial = _plan(report, [_action(action_id="driver", decision="skip")])
    final = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[initial, final])
    events: list[str] = []
    rollout_active = False
    lifecycle_depth = 0
    resources = (("checkout", "/repo"), ("target", "/repo/.venv"))
    handoff = json.dumps(
        {
            "schema": "vq.toolset.lifecycle_handoff/1",
            "locks": [
                {
                    "scope": scope,
                    "resource": resource,
                    "fd": fd,
                    "path": f"/tmp/test-controller-{scope}.lock",
                }
                for fd, (scope, resource) in zip((91, 92), resources, strict=True)
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    )

    @contextmanager
    def rollout_lock(rollout_id: str):
        nonlocal rollout_active
        assert rollout_id == "discover-latest"
        assert lifecycle_depth == 0
        rollout_active = True
        events.append("rollout-enter")
        try:
            yield
        finally:
            assert lifecycle_depth == 0
            events.append("rollout-exit")
            rollout_active = False

    @contextmanager
    def lifecycle_lock(*args: Any, **kwargs: Any):
        nonlocal lifecycle_depth
        del args, kwargs
        assert rollout_active
        if lifecycle_depth == 0:
            events.append("lifecycle-enter")
        lifecycle_depth += 1
        try:
            yield
        finally:
            lifecycle_depth -= 1
            if lifecycle_depth == 0:
                events.append("lifecycle-exit")

    def active_handoff() -> tuple[str, tuple[int, ...]]:
        assert lifecycle_depth > 0
        return handoff, (91, 92)

    def active_resources() -> tuple[tuple[str, str], ...]:
        assert lifecycle_depth > 0
        return resources

    def reconcile(**kwargs: Any) -> fleet_rollout.OperationReconciliation:
        assert rollout_active and lifecycle_depth > 0
        assert kwargs["lifecycle_handoff"] == handoff
        return fleet_rollout.OperationReconciliation()

    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )

    def execute_plan(*args: Any, **kwargs: Any) -> fleet_rollout.RolloutRun:
        del args
        assert rollout_active and lifecycle_depth > 0
        assert kwargs["lifecycle_handoff"] == handoff
        assert kwargs["lifecycle_resources"] == resources
        events.append("execute-plan")
        return run

    def finalize(*args: Any, **kwargs: Any) -> fleet_rollout.RolloutRun:
        del args, kwargs
        assert rollout_active and lifecycle_depth > 0
        events.append("finalize")
        run.complete = True
        return run

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", rollout_lock)
    monkeypatch.setattr("vq.cli.admin_module.toolset_lifecycle_lock", lifecycle_lock)
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        active_handoff,
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        active_resources,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        reconcile,
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.execute_plan", execute_plan)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_scheduler_parity_holds",
        lambda *args, **kwargs: run,
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.finalize_run", finalize)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    assert events == [
        "rollout-enter",
        "lifecycle-enter",
        "execute-plan",
        "finalize",
        "lifecycle-exit",
        "rollout-exit",
    ]


def test_driver_action_and_reentry_keep_controller_fences_until_child_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    driver = _action(action_id="driver", decision="update")
    plan = _plan(report, [driver])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.load_run",
        lambda rollout_id: fleet_rollout.RolloutRun(
            rollout_id=rollout_id,
            report_source_path=report.source_path,
            report_digest_sha256=report.digest_sha256,
        ),
    )
    events: list[str] = []
    rollout_active = False
    lifecycle_depth = 0
    resources = (("checkout", "/repo"), ("target", "/repo/.venv"))
    handoff = json.dumps(
        {
            "schema": "vq.toolset.lifecycle_handoff/1",
            "locks": [
                {
                    "scope": scope,
                    "resource": resource,
                    "fd": fd,
                    "path": f"/tmp/test-controller-{scope}.lock",
                }
                for fd, (scope, resource) in zip((91, 92), resources, strict=True)
            ],
        },
        separators=(",", ":"),
        sort_keys=True,
    )

    @contextmanager
    def rollout_lock(rollout_id: str):
        nonlocal rollout_active
        assert rollout_id == "discover-latest"
        rollout_active = True
        events.append("rollout-enter")
        try:
            yield
        finally:
            assert lifecycle_depth == 0
            events.append("rollout-exit")
            rollout_active = False

    @contextmanager
    def lifecycle_lock(*args: Any, **kwargs: Any):
        nonlocal lifecycle_depth
        del args, kwargs
        assert rollout_active
        if lifecycle_depth == 0:
            events.append("lifecycle-enter")
        lifecycle_depth += 1
        try:
            yield
        finally:
            lifecycle_depth -= 1
            if lifecycle_depth == 0:
                events.append("lifecycle-exit")

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", rollout_lock)
    monkeypatch.setattr("vq.cli.admin_module.toolset_lifecycle_lock", lifecycle_lock)
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_handoff",
        lambda: (handoff, (91, 92)),
    )
    monkeypatch.setattr(
        "vq.cli.admin_module._active_toolset_lifecycle_resources",
        lambda: resources,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.abort_unselected_pre_authorization_operations",
        lambda *args, **kwargs: None,
    )

    def execute_one(*args: Any, **kwargs: Any) -> fleet_rollout.RolloutRun:
        del args
        assert rollout_active and lifecycle_depth > 0
        assert kwargs["lifecycle_handoff"] == handoff
        assert kwargs["lifecycle_resources"] == resources
        events.append("driver-action")
        return fleet_rollout.RolloutRun(
            rollout_id=fleet_rollout.rollout_id(report),
            report_digest_sha256=report.digest_sha256,
            report_source_path=report.source_path,
        )

    def reenter(*args: Any, **kwargs: Any) -> int:
        del args, kwargs
        assert rollout_active
        assert lifecycle_depth > 0
        events.append("reenter")
        return 0

    monkeypatch.setattr("vq.cli.fleet_rollout.execute_one", execute_one)
    monkeypatch.setattr("vq.cli.fleet_rollout.reenter_after_driver", reenter)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    assert events == [
        "rollout-enter",
        "lifecycle-enter",
        "driver-action",
        "reenter",
        "lifecycle-exit",
        "rollout-exit",
    ]


@pytest.mark.parametrize("as_json", [False, True])
def test_explicit_legacy_reconciliation_uses_fresh_plan_inside_global_lock(
    monkeypatch: pytest.MonkeyPatch,
    as_json: bool,
) -> None:
    report = _report()
    initial = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime:host_a:vibeqc-release",
                decision="update",
                phase="local-runtime",
                host="host_a",
            ),
            _action(
                action_id="local-runtime:host_b:vibeqc-release",
                decision="update",
                phase="local-runtime",
                host="host_b",
            ),
        ],
        extra_hosts=("host_a", "host_b"),
    )
    final = _plan(
        report,
        [
            _action(action_id="driver", decision="skip"),
            _action(
                action_id="local-runtime:host_a:vibeqc-release",
                decision="update",
                phase="local-runtime",
                host="host_a",
            ),
            _action(
                action_id="local-runtime:host_b:vibeqc-release",
                decision="skip",
                phase="local-runtime",
                host="host_b",
            ),
        ],
        extra_hosts=("host_a", "host_b"),
    )
    _patch_discovery(
        monkeypatch,
        report=report,
        plans=[initial, initial, final],
        extra_hosts=("host_a", "host_b"),
    )
    events: list[str] = []
    locked = False

    @contextmanager
    def lock(unused_rollout_id: str):
        nonlocal locked
        locked = True
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")
            locked = False

    def reconcile(**kwargs: Any):
        assert locked
        assert kwargs["allow_legacy_reconciliation"] is True
        events.append("durable-reconcile")
        return fleet_rollout.OperationReconciliation(
            legacy_running_actions=(("old-rollout", "old-action"),)
        )

    def legacy(**kwargs: Any):
        assert locked
        assert kwargs["legacy_running_actions"] == (
            ("old-rollout", "old-action"),
        )
        assert kwargs["plan"] is initial
        events.append("legacy-reconcile")
        return fleet_rollout.LegacyRolloutReconciliation(
            superseded_actions=(("old-rollout", "old-action"),),
            settled_inactive_holds=(("old-rollout", "host_b"),),
            released_live_holds=(("old-rollout", "host_f"),),
            retained_holds=(("old-rollout", "host_a", "network unavailable"),),
            live_hold_state_changed=True,
        )

    healthy_doctor = {
        host: {"ok": True, "checks": []}
        for host in ("localhost", "host_a", "host_b")
    }
    snapshots = iter((({}, {}, healthy_doctor),) * 3)

    def snapshot():
        assert locked
        events.append("snapshot")
        return next(snapshots)

    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )

    def execute(plan: fleet_rollout.RolloutPlan, *args: Any, **kwargs: Any):
        assert locked
        decisions = {action.host: action.decision for action in plan.actions}
        assert decisions["host_a"] == "defer"
        assert decisions["host_b"] == "update"
        events.append("execute")
        return run

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", lock)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations", reconcile
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_legacy_rollout_state", legacy
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.collect_snapshots", snapshot)
    monkeypatch.setattr("vq.cli.fleet_rollout.execute_plan", execute)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_scheduler_parity_holds",
        lambda *args, **kwargs: run,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.finalize_run",
        lambda *args, **kwargs: setattr(run, "complete", True) or run,
    )

    argv = ["admin", "rollout-latest", "--reconcile-legacy"]
    if as_json:
        argv.append("--json")
    result = CliRunner().invoke(main, argv)

    assert result.exit_code == 2, result.output
    if as_json:
        payload = json.loads(result.output)
        assert payload["status"] == "retained"
        assert payload["retained_host_fences"] == [
            {
                "host": "host_a",
                "reason": "network unavailable",
                "rollout_id": "old-rollout",
            }
        ]
        legacy_payload = payload["legacy_reconciliation"]
        assert legacy_payload["superseded_actions"] == [
            {"action_id": "old-action", "rollout_id": "old-rollout"}
        ]
        assert legacy_payload["settled_inactive_holds"] == [
            {"host": "host_b", "rollout_id": "old-rollout"}
        ]
        assert legacy_payload["released_live_holds"] == [
            {"host": "host_f", "rollout_id": "old-rollout"}
        ]
    else:
        assert "LEGACY ACTION SUPERSEDED old-rollout old-action" in result.output
        assert "LEGACY HOLD SETTLED INACTIVE old-rollout host_b" in result.output
        assert "LEGACY LIVE HOLD RELEASED old-rollout host_f" in result.output
        assert "LEGACY HOLD RETAINED old-rollout host_a: network unavailable" in (
            result.output
        )
    assert events == [
        "lock",
        "durable-reconcile",
        "snapshot",
        "legacy-reconcile",
        "snapshot",
        "unlock",
    ]


def test_explicit_legacy_reconciliation_hands_off_a_claim_only_journal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    locked = False
    observed: dict[str, Any] = {}

    @contextmanager
    def lock(unused_rollout_id: str):
        nonlocal locked
        locked = True
        try:
            yield
        finally:
            locked = False

    def reconcile(**kwargs: Any) -> fleet_rollout.OperationReconciliation:
        assert locked
        assert kwargs["allow_legacy_reconciliation"] is True
        return fleet_rollout.OperationReconciliation(
            legacy_scheduler_holds=(("v0.15.117-old", "host_c"),)
        )

    def legacy(**kwargs: Any) -> fleet_rollout.LegacyRolloutReconciliation:
        assert locked
        observed.update(kwargs)
        raise fleet_rollout.FleetRolloutError("claim-only handoff observed")

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", lock)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        reconcile,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_legacy_rollout_state",
        legacy,
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--reconcile-legacy", "--json"],
    )

    assert result.exit_code == 1
    assert observed["legacy_running_actions"] == ()
    assert observed["legacy_scheduler_holds"] == (
        ("v0.15.117-old", "host_c"),
    )
    assert observed["plan"] is plan


def test_explicit_legacy_reconciliation_hands_off_failure_only_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    failure = (
        "v0.15.137-historical",
        "host_e",
        "terminal operation failed with exit 9",
    )
    observed: dict[str, Any] = {}

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            failed_operation_hosts=(failure,)
        ),
    )

    def legacy(**kwargs: Any) -> fleet_rollout.LegacyRolloutReconciliation:
        observed.update(kwargs)
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
    assert observed["failed_operation_hosts"] == (failure,)
    assert observed["legacy_hold_retries"] == ()
    assert observed["plan"] is plan
    assert observed["accepted_report"] is report


def test_explicit_legacy_reconciliation_hands_off_pending_failure_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    failure = (
        "v0.15.137-historical",
        "host_e",
        "terminal operation failed with exit 9",
    )
    observed: dict[str, Any] = {}
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            pending_failure_transitions=(failure,)
        ),
    )

    def legacy(**kwargs: Any) -> fleet_rollout.LegacyRolloutReconciliation:
        observed.update(kwargs)
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
    assert observed["failed_operation_hosts"] == (failure,)
    assert observed["accepted_report"] is report


def test_explicit_legacy_reconciliation_groups_pending_failures_by_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(
        report,
        [_action(action_id="driver", decision="skip")],
    )
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    failures = (
        (
            "v0.15.137-host_e-historical",
            "host_e",
            "host_e terminal operation failed with exit 9",
        ),
        (
            "v0.15.137-host_e-historical",
            "host_a",
            "host_a terminal operation failed with exit 7",
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            legacy_running_actions=(("v0.15.120-legacy", "helper:venus"),),
            legacy_scheduler_holds=(("v0.15.120-legacy", "venus"),),
            legacy_hold_retries=(
                (failures[0][0], failures[0][1]),
                (failures[1][0], failures[1][1]),
                ("v0.15.120-legacy", "venus"),
            ),
            pending_failure_transitions=failures,
        ),
    )
    observed: list[dict[str, Any]] = []

    def legacy(**kwargs: Any) -> fleet_rollout.LegacyRolloutReconciliation:
        observed.append(kwargs)
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
    assert [item["failed_operation_hosts"] for item in observed] == [
        (failures[1],),
        (failures[0],),
        (),
    ]
    assert [item["legacy_hold_retries"] for item in observed] == [
        ((failures[1][0], failures[1][1]),),
        ((failures[0][0], failures[0][1]),),
        (("v0.15.120-legacy", "venus"),),
    ]
    assert [item["legacy_running_actions"] for item in observed] == [
        (),
        (),
        (("v0.15.120-legacy", "helper:venus"),),
    ]
    assert [item["legacy_scheduler_holds"] for item in observed] == [
        (),
        (),
        (("v0.15.120-legacy", "venus"),),
    ]


def test_legacy_reconciliation_is_not_available_under_verify_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--verify-only answers "has the fleet converged", which is a different
    question. --dry-run previews the reconciliation; see the tests below."""
    monkeypatch.setattr(
        "vq.cli.config.load_config",
        lambda: pytest.fail("usage validation must precede discovery"),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--verify-only", "--reconcile-legacy"],
    )

    assert result.exit_code == 2
    assert "--reconcile-legacy" in result.output
    assert "--dry-run to preview" in result.output


@pytest.mark.parametrize(
    "scope_args",
    [["--only", "host_b"], ["--skip", "host_a"]],
)
def test_legacy_reconciliation_rejects_scoped_host_selection(
    monkeypatch: pytest.MonkeyPatch,
    scope_args: list[str],
) -> None:
    monkeypatch.setattr(
        "vq.cli.config.load_config",
        lambda: pytest.fail("scope validation must precede discovery"),
    )

    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--reconcile-legacy", *scope_args],
    )

    assert result.exit_code == 2
    assert "fleet-global recovery" in result.output


def test_retained_driver_legacy_hold_blocks_before_update_or_reentry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="update")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            retained_legacy_holds=(
                ("old-driver-rollout", "localhost", "hold identity unknown"),
            ),
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_one",
        lambda *args, **kwargs: pytest.fail("retained driver was updated"),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reenter_after_driver",
        lambda *args, **kwargs: pytest.fail("retained driver re-entered"),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["status"] == "error"
    assert "retained legacy rollout state" in payload["error"]
    assert "old-driver-rollout" in payload["error"]


def test_retained_driver_hold_blocks_recovered_fresh_interpreter_reentry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            driver_reentry_rollout_id="recovered-driver-rollout",
            driver_reentry_host="localhost",
            retained_legacy_holds=(
                ("old-driver-rollout", "localhost", "network unavailable"),
            ),
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reenter_after_driver",
        lambda *args, **kwargs: pytest.fail("retained driver re-entered"),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: pytest.fail("retained re-entry must fail before snapshot"),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert "fresh-interpreter re-entry is fenced" in payload["error"]


def test_recovered_driver_result_reenters_under_inherited_fences_before_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    events: list[str] = []

    @contextmanager
    def lock(rollout_id: str):
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")

    def reconcile(**kwargs: Any):
        events.append("reconcile")
        return fleet_rollout.OperationReconciliation(
            driver_reentry_rollout_id="v0.15.60-recovered",
            driver_reentry_host="localhost",
            driver_reentry_operation_id="a" * 64,
            driver_reentry_request_sha256="b" * 64,
            driver_reentry_report_digest_sha256="c" * 64,
        )

    def reenter(*args: Any, **kwargs: Any) -> int:
        assert events[-1] == "reconcile"
        assert kwargs["reentry_handoff"] == "reentry-capability"
        assert kwargs["pass_fds"] == (90, 91)
        events.append("reenter")
        return 0

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", lock)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations", reconcile
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.reenter_after_driver", reenter)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: pytest.fail("no stale-interpreter snapshot may run"),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 0, result.output
    assert events == ["lock", "reconcile", "reenter", "unlock"]


def test_recovered_failed_driver_is_not_replayed_in_the_same_invocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    driver = _action(action_id="driver", decision="update")
    plan = _plan(report, [driver])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    reason = "driver executed and failed with exit 9; it is not retry-safe"
    consumed: list[tuple[tuple[str, str, str], ...]] = []
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations",
        lambda **kwargs: fleet_rollout.OperationReconciliation(
            failed_operation_hosts=(
                (fleet_rollout.rollout_id(report), "localhost", reason),
            )
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.execute_one",
        lambda *args, **kwargs: pytest.fail(
            "a recovered failed driver must not launch again"
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reenter_after_driver",
        lambda *args, **kwargs: pytest.fail(
            "a failed driver receipt must not trigger re-entry"
        ),
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.consume_reconciled_failure_fences",
        lambda failures, **kwargs: consumed.append(tuple(failures)),
    )

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert result.exit_code == 1
    assert "recovered durable driver update failed" in result.output
    assert reason in result.output
    assert consumed == [
        ((fleet_rollout.rollout_id(report), "localhost", reason),)
    ]


def test_old_report_driver_receipt_reenters_once_with_exact_ack_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    old_rollout = "v0.15.59-old-driver"
    events: list[str] = []

    @contextmanager
    def lock(rollout_id: str):
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")

    def first_reconcile(**kwargs: Any):
        assert kwargs["acknowledge_driver_reentry"] is None
        assert kwargs["authenticated_driver_reentry"] is None
        events.append("reconcile-old")
        return fleet_rollout.OperationReconciliation(
            driver_reentry_rollout_id=old_rollout,
            driver_reentry_host="localhost",
            driver_reentry_operation_id="a" * 64,
            driver_reentry_request_sha256="b" * 64,
            driver_reentry_report_digest_sha256="c" * 64,
        )

    def reenter(token: str, **kwargs: Any) -> int:
        assert token == old_rollout
        assert events[-1] == "reconcile-old"
        assert kwargs["reentry_handoff"] == "reentry-capability"
        assert kwargs["pass_fds"] == (90, 91)
        events.append("reenter-old")
        return 0

    monkeypatch.setattr("vq.cli.fleet_rollout.rollout_execution_lock", lock)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations", first_reconcile
    )
    monkeypatch.setattr("vq.cli.fleet_rollout.reenter_after_driver", reenter)
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.collect_snapshots",
        lambda: pytest.fail("old interpreter must not snapshot"),
    )

    first = CliRunner().invoke(main, ["admin", "rollout-latest", "--json"])

    assert first.exit_code == 0, first.output
    assert events == ["lock", "reconcile-old", "reenter-old", "unlock"]

    capability = fleet_rollout.RolloutReentryCapability(
        rollout_id=old_rollout,
        operation_id="a" * 64,
        request_sha256="b" * 64,
        report_digest_sha256="c" * 64,
    )

    @contextmanager
    def adopt(**kwargs: Any):
        assert kwargs["expected_rollout_id"] == old_rollout
        yield capability

    def acknowledged(**kwargs: Any):
        assert kwargs["acknowledge_driver_reentry"] == old_rollout
        assert kwargs["authenticated_driver_reentry"] == capability
        events.append("ack-old")
        return fleet_rollout.OperationReconciliation()

    monkeypatch.setattr(
        "vq.cli.fleet_rollout.adopt_rollout_reentry_handoff",
        adopt,
    )
    monkeypatch.setattr(
        "vq.cli.fleet_rollout.reconcile_durable_operations", acknowledged
    )
    second = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", "--resume", old_rollout, "--json"],
    )

    assert second.exit_code == 1
    assert "exact recovered driver receipt was acknowledged" in second.output
    assert events[-2:] == ["ack-old", "unlock"]
    assert events.count("reenter-old") == 1


@pytest.mark.parametrize("mode", ["--dry-run", "--verify-only"])
def test_read_only_rollout_modes_do_not_create_lock_or_repair_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    report = _report()
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[plan])
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    identity = fleet_operation.OperationIdentity(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        attempt=1,
        action_id="prepared",
        phase="local-runtime",
        host="localhost",
        program="vibeqc-queue",
        pin_name="vq",
        target_sha=SHA,
        target_version="0.17.0",
        target_tag=None,
        argv=("--version",),
    )
    fleet_operation.prepare_operation(identity)

    def snapshot() -> dict[str, tuple[bytes, int]]:
        rollout_root = tmp_path / "rollouts"
        return {
            str(path.relative_to(rollout_root)): (
                path.read_bytes(),
                path.stat().st_mtime_ns,
            )
            for path in rollout_root.rglob("*")
            if path.is_file()
        }

    before = snapshot()
    result = CliRunner().invoke(
        main,
        ["admin", "rollout-latest", mode, "--json"],
    )

    assert result.exit_code == 1
    assert "will not reconcile" in result.output
    assert snapshot() == before
    assert not (tmp_path / "rollouts" / ".fleet-rollout.lock").exists()


class TestLegacyReconciliationPreview:
    """`--reconcile-legacy --dry-run` shows the plan and writes nothing.

    The mutating form writes durable supersession evidence fleet-wide and
    marks historical action outcomes permanently unknown. Until this existed
    it had to be authorised blind: the combination was refused outright, so
    the operator could not see what it would rewrite.
    """

    def _patched(self, monkeypatch: pytest.MonkeyPatch) -> list[str]:
        report = _report()
        _patch_discovery(
            monkeypatch,
            report=report,
            plans=[_plan(report, [_action(action_id="driver", decision="skip")])],
        )
        forbidden: list[str] = []

        def refuse(name: str):
            def call(*args: Any, **kwargs: Any):
                forbidden.append(name)
                raise AssertionError(f"{name} must not run under --dry-run")
            return call

        for target in (
            "reconcile_durable_operations",
            "reconcile_legacy_rollout_state",
            "collect_snapshots",
            "execute_plan",
        ):
            monkeypatch.setattr(
                f"vq.cli.fleet_rollout.{target}", refuse(target)
            )
        monkeypatch.setattr(
            "vq.cli.fleet_rollout.rollout_execution_lock",
            refuse("rollout_execution_lock"),
        )
        return forbidden

    def _inventory(
        self, monkeypatch: pytest.MonkeyPatch, inventory: Any,
    ) -> list[dict[str, Any]]:
        seen: list[dict[str, Any]] = []

        def probe(**kwargs: Any):
            seen.append(kwargs)
            return inventory

        monkeypatch.setattr(
            "vq.cli.fleet_rollout.inventory_legacy_rollout_state", probe
        )
        return seen

    def test_it_lists_what_would_be_reconciled_and_writes_nothing(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        forbidden = self._patched(monkeypatch)
        self._inventory(
            monkeypatch,
            fleet_rollout.LegacyRolloutInventory(
                running_actions=(("old-rollout", "scheduler-runtime:host_f:x"),),
                scheduler_holds=(("old-rollout", "host_f"),),
                hold_retries=(("old-rollout", "host_f-amd"),),
                pending_failure_transitions=(
                    ("old-rollout", "host_b", "update failed"),
                ),
            ),
        )

        result = CliRunner().invoke(
            main, ["admin", "rollout-latest", "--reconcile-legacy", "--dry-run"],
        )

        assert result.exit_code == 0, result.output
        assert forbidden == []
        assert "nothing was written" in result.output
        assert "old-rollout scheduler-runtime:host_f:x" in result.output
        assert "old-rollout host_f" in result.output
        assert "old-rollout host_f-amd" in result.output
        assert "host_b: update failed" in result.output
        assert "without --dry-run" in result.output

    def test_it_says_so_when_there_is_nothing_to_reconcile(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._patched(monkeypatch)
        self._inventory(monkeypatch, fleet_rollout.LegacyRolloutInventory())

        result = CliRunner().invoke(
            main, ["admin", "rollout-latest", "--reconcile-legacy", "--dry-run"],
        )

        assert result.exit_code == 0, result.output
        assert "no legacy rollout state to reconcile" in result.output

    def test_json_carries_the_same_rows(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._patched(monkeypatch)
        self._inventory(
            monkeypatch,
            fleet_rollout.LegacyRolloutInventory(
                running_actions=(("old-rollout", "old-action"),),
            ),
        )

        result = CliRunner().invoke(
            main,
            [
                "admin", "rollout-latest",
                "--reconcile-legacy", "--dry-run", "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["running_actions"] == [
            {"rollout_id": "old-rollout", "action_id": "old-action"}
        ]
        assert payload["empty"] is False

    def test_the_inventory_is_scoped_to_the_report_being_rolled(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A preview of the wrong report's state would be worse than none."""
        self._patched(monkeypatch)
        seen = self._inventory(
            monkeypatch, fleet_rollout.LegacyRolloutInventory(),
        )

        result = CliRunner().invoke(
            main, ["admin", "rollout-latest", "--reconcile-legacy", "--dry-run"],
        )

        assert result.exit_code == 0, result.output
        assert len(seen) == 1
        assert seen[0]["current_report_digest_sha256"] == _report().digest_sha256
        assert seen[0]["current_report_source_path"] == _report().source_path


class TestRepeatedPlanHoldSupersession:
    """One obsolete rollout can retain a hold per scheduler lane.

    host_f retained six -- host_f, host_f-amd, host_f-big, host_f-inf, host_f-itwin,
    host_f-jtwin -- from a single obsolete rollout, and clearing them took six
    near-identical invocations of a recovery-only command, each writing
    durable state. Repetition invites copy-paste error in exactly the place
    you least want it.
    """

    HOSTS = (
        "host_f", "host_f-amd", "host_f-big", "host_f-inf", "host_f-itwin", "host_f-jtwin",
    )
    ROLLOUT = "v0.15.155-deadbeefdead"

    def _setup(
        self, monkeypatch: pytest.MonkeyPatch, *, fail_on: str | None = None,
    ) -> tuple[Any, list[dict[str, Any]]]:
        report = _report()
        plan = _plan(
            report,
            [
                _action(action_id="driver", decision="skip"),
                _action(
                    action_id="scheduler-runtime:host_f:vibeqc-release",
                    decision="skip",
                    phase="scheduler-runtime",
                    host="host_f",
                ),
            ],
            extra_hosts=("host_f",),
        )
        plan._scheduler_hold_targets = {
            "host_f": tuple((host, "localhost") for host in self.HOSTS)
        }
        _patch_discovery(
            monkeypatch,
            report=report,
            plans=[plan],
            extra_hosts=("host_f", *self.HOSTS),
        )
        calls: list[dict[str, Any]] = []

        def supersede(**kwargs: Any) -> fleet_rollout.PlanBoundHoldSupersession:
            calls.append(kwargs)
            if kwargs["host"] == fail_on:
                raise fleet_rollout.FleetRolloutError(
                    f"{kwargs['host']} is not converged yet"
                )
            return fleet_rollout.PlanBoundHoldSupersession(
                rollout_id=kwargs["rollout_id_value"],
                host=kwargs["host"],
                current_rollout_id=fleet_rollout.rollout_id(report),
                replayed=False,
            )

        monkeypatch.setattr(
            "vq.cli.fleet_rollout.supersede_obsolete_plan_bound_hold",
            supersede,
            raising=False,
        )
        monkeypatch.setattr(
            "vq.cli.fleet_rollout.execute_plan",
            lambda *a, **k: pytest.fail("supersession executes no action"),
        )
        return plan, calls

    def _argv(self, *hosts: str, json_output: bool = False) -> list[str]:
        argv = ["admin", "rollout-latest"]
        for host in hosts:
            argv += ["--supersede-plan-hold", self.ROLLOUT, host]
        if json_output:
            argv.append("--json")
        return argv

    def test_one_rollouts_holds_clear_in_one_invocation(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        plan, calls = self._setup(monkeypatch)

        result = CliRunner().invoke(
            main, self._argv(*self.HOSTS, json_output=True),
        )

        assert result.exit_code == 0, result.output
        assert [call["host"] for call in calls] == list(self.HOSTS)
        # The evidence requirements are per host and unchanged; what is shared
        # is the one plan they are proved against.
        assert {id(call["plan"]) for call in calls} == {id(plan)}
        payload = json.loads(result.output)
        assert payload["schema"] == "vq.fleet.plan_hold_supersede_batch/1"
        assert payload["n_recorded"] == 6
        assert payload["n_replayed"] == 0
        assert [row["host"] for row in payload["results"]] == list(self.HOSTS)

    def test_a_single_pair_keeps_its_original_json_shape(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An existing --json consumer must not have to learn a new shape."""
        self._setup(monkeypatch)

        result = CliRunner().invoke(main, self._argv("host_f-big", json_output=True))

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["schema"] == "vq.fleet.plan_hold_supersede_result/1"
        assert payload["host"] == "host_f-big"

    def test_a_refusal_stops_and_still_reports_what_was_recorded(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The per-host evidence caught a genuine "host_f is not converged yet".
        What it caught must not be softened into a partial success, and what
        was already written is durable and must be reported."""
        _plan_obj, calls = self._setup(monkeypatch, fail_on="host_f-big")

        result = CliRunner().invoke(main, self._argv(*self.HOSTS))

        assert result.exit_code != 0
        assert [call["host"] for call in calls] == ["host_f", "host_f-amd", "host_f-big"]
        assert "host_f-big is not converged yet" in result.output
        assert "plan-bound hold supersede recorded: " in result.output
        assert "host_f-amd" in result.output
        assert "host_f-inf" not in result.output

    def test_text_output_names_every_hold(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        self._setup(monkeypatch)

        result = CliRunner().invoke(main, self._argv(*self.HOSTS))

        assert result.exit_code == 0, result.output
        for host in self.HOSTS:
            assert f"{self.ROLLOUT} {host} " in result.output

    def test_the_same_pair_twice_is_a_usage_error(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "vq.cli.config.load_config",
            lambda: pytest.fail("usage validation must precede discovery"),
        )

        result = CliRunner().invoke(main, self._argv("host_f-big", "host_f-big"))

        assert result.exit_code == 2
        assert "names the same ROLLOUT HOST pair twice" in result.output


@pytest.fixture
def component_histories(tmp_path: Path):
    """Independent object stores: a queue checkout cannot answer QC/view ancestry."""
    def git(repo: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), *args], check=True,
            capture_output=True, text=True,
        ).stdout.strip()

    histories = {}
    for name in ("qc", "view", "queue"):
        repo = tmp_path / name
        repo.mkdir()
        git(repo, "init", "-b", "main")
        git(repo, "config", "user.name", "Ancestry fixture")
        git(repo, "config", "user.email", "ancestry@example.invalid")
        commits = {}
        for stage in ("base", "target", "ahead"):
            (repo / "component").write_text(f"{name}: {stage}\n")
            git(repo, "add", ".")
            git(repo, "commit", "-m", stage)
            commits[stage] = git(repo, "rev-parse", "HEAD")
        git(repo, "checkout", "-b", "divergent", commits["base"])
        (repo / "component").write_text(f"{name}: divergent\n")
        git(repo, "commit", "-am", "divergent")
        commits["diverged"] = git(repo, "rev-parse", "HEAD")
        git(repo, "checkout", "main")
        git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
        histories[name] = {"repo": repo, **commits}
    tag = "v0.17.1"
    git(histories["qc"]["repo"], "tag", "-a", tag, "-m", tag,
        histories["qc"]["target"])
    pins = {}
    for name in fleet_release.PIN_NAMES:
        component = "qc" if name in {"release", "dev"} else (
            "queue" if name == "vq" else "view"
        )
        sha = histories[component]["target"]
        slug, project = fleet_release.PIN_SOURCES[name]
        pins[name] = {
            "accepted": True, "acceptance_rule": "A", "sha": sha,
            "repo": slug, "project_id": project,
            "version": "0.17.1" if component == "qc" else (
                "0.26.1" if component == "queue" else "2.16.2"
            ),
            "deploy_flags": (["--tag", tag] if name == "release" else [])
            + ["--expected-sha", sha],
            "ci_evidence": {
                "pipeline_id": 4400, "pipeline_status": "success",
                "ref": "release-candidate/ancestry-fixture", "sha": sha,
                "gating_job": fleet_release.PIN_GATING_JOBS[name],
                "gating_job_status": "success",
                "web_url": "https://gitlab.example/pipelines/4400",
            },
        }
        if name == "release":
            pins[name].update(tag=tag, tag_object=git(
                histories["qc"]["repo"], "rev-parse", tag,
            ))
    payload = {
        "schema": fleet_release.REPORT_SCHEMA,
        "all_pins_accepted": True, "pins": pins,
    }
    runtime = histories["queue"]["repo"]
    path = runtime / fleet_release.REPORT_DIRECTORY / f"{tag}.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload))
    git(runtime, "add", ".")
    git(runtime, "commit", "-m", "accepted split report")
    git(runtime, "update-ref", "refs/remotes/origin/main", "HEAD")
    repos = {
        "mpei/vibe-qc": histories["qc"]["repo"],
        "mpei/vibe-view": histories["view"]["repo"],
        "mpei/vibe-queue": runtime,
    }
    report = fleet_release.discover_latest_report(
        runtime, fetch=False, pin_repos=repos,
    )
    # Establish the production failure without teaching the runtime foreign objects.
    for component in ("qc", "view"):
        history = histories[component]
        assert fleet_release.git_is_ancestor(
            runtime, history["base"], history["target"],
        ) is None
        assert fleet_release.git_is_ancestor(
            history["repo"], history["base"], history["target"],
        ) is True
    return report, histories, repos


@pytest.mark.parametrize(
    ("mode", "expected_builds"),
    [("dry-run", 1), ("verify-only", 1), ("execute", 2),
     ("supersede-plan-hold", 1), ("reconcile-legacy", 2), ("resume", 2)],
)
def test_split_ancestry_at_every_cli_planning_boundary(
    monkeypatch: pytest.MonkeyPatch, component_histories,
    mode: str, expected_builds: int,
) -> None:
    report, histories, repos = component_histories
    real_ancestry = fleet_release.git_is_ancestor
    plan = _plan(report, [_action(action_id="driver", decision="skip")])
    _patch_discovery(monkeypatch, report=report, plans=[])
    cfg = config.load_config()
    cfg.pin_source_repos = {slug: str(path) for slug, path in repos.items()}
    monkeypatch.setattr(fleet_release, "git_is_ancestor", real_ancestry)
    monkeypatch.setattr(fleet_release, "runtime_repo", lambda: histories["queue"]["repo"])
    observed = []

    def build(*args: Any, **kwargs: Any):
        # Exercise real policy + real Git, leaving only fleet I/O stubbed.
        for name, component in (("release", "qc"), ("dev", "qc"),
                                ("vibe_view", "view"), ("vq", "queue")):
            pin = report.pins[name]
            for state, expected in (("base", "update"), ("target", "skip"),
                                    ("ahead", "block" if name == "vq" else "skip"),
                                    ("diverged", "block"), ("unknown", "block")):
                lane = fleet_rollout.LaneState(
                    configured=True, current_sha=histories[component].get(state, "f" * 40),
                    current_version=None, current_tag=None, dirty=False,
                    last_ok=True, acknowledged=False, detail="fixture",
                )
                decision, reason = fleet_rollout._lane_decision(
                    lane, pin=pin, ancestry=kwargs["ancestry"],
                    ahead_is_release_drift=name == "vq",
                )
                assert decision == expected, (mode, name, state, decision, reason)
                if state in {"unknown", "diverged"}:
                    assert state in reason
                if state == "ahead" and name != "vq":
                    assert "no downgrade" in reason
        observed.append(kwargs["ancestry"])
        return plan

    monkeypatch.setattr(fleet_rollout, "build_plan", build)
    run = fleet_rollout.RolloutRun(
        rollout_id=fleet_rollout.rollout_id(report),
        report_digest_sha256=report.digest_sha256,
        report_source_path=report.source_path,
    )
    monkeypatch.setattr(fleet_rollout, "execute_plan", lambda *a, **k: run)
    monkeypatch.setattr(fleet_rollout, "save_run", lambda value: Path("/state"))
    monkeypatch.setattr(
        fleet_rollout, "reconcile_durable_operations",
        lambda **kw: fleet_rollout.OperationReconciliation(
            legacy_running_actions=(("old-rollout", "old-action"),)
            if mode == "reconcile-legacy" else (),
        ),
    )
    monkeypatch.setattr(
        fleet_rollout, "reconcile_legacy_rollout_state",
        lambda **kw: fleet_rollout.LegacyRolloutReconciliation(
            live_hold_refresh_required=True,
        ),
    )
    monkeypatch.setattr(
        fleet_rollout, "supersede_obsolete_plan_bound_hold",
        lambda **kw: fleet_rollout.PlanBoundHoldSupersession(
            rollout_id="old-rollout", host="localhost",
            current_rollout_id=run.rollout_id, replayed=False,
        ),
    )
    argv = ["admin", "rollout-latest", "--json"]
    if mode == "supersede-plan-hold":
        argv += ["--supersede-plan-hold", "old-rollout", "localhost"]
    elif mode == "resume":
        @contextmanager
        def inherited(**kwargs: Any):
            assert kwargs["expected_rollout_id"] == run.rollout_id
            yield fleet_rollout.RolloutReentryCapability(
                rollout_id=run.rollout_id, operation_id="a" * 64,
                request_sha256="b" * 64,
                report_digest_sha256=report.digest_sha256,
            )
        monkeypatch.setattr(fleet_rollout, "adopt_rollout_reentry_handoff", inherited)
        argv += ["--resume", run.rollout_id]
    elif mode != "execute":
        argv += [f"--{mode}"]
    result = CliRunner().invoke(main, argv)
    assert result.exit_code == 0, (result.output, result.exception)
    assert len(observed) == expected_builds, result.output
    assert all(callback is observed[0] for callback in observed)


@pytest.mark.parametrize("configured_monorepo", [False, True])
def test_legacy_ancestry_uses_configured_monorepo_or_runtime_fallback(
    component_histories, configured_monorepo: bool,
) -> None:
    split_report, histories, _ = component_histories
    payload = json.loads(json.dumps(split_report.raw))
    payload.update(schema="vq.fleet.release_report/2", project_id=19)
    qc = histories["qc"]
    for name, pin in payload["pins"].items():
        pin.pop("repo")
        pin.pop("project_id")
        pin["sha"] = qc["target"]
        pin["deploy_flags"][-1] = qc["target"]
        pin["ci_evidence"]["sha"] = qc["target"]
        pin["ci_evidence"]["gating_job"] = fleet_release.LEGACY_PIN_GATING_JOBS[name]
    report = fleet_release.parse_report(
        payload, source_ref="origin/main", source_path=split_report.source_path,
        raw_bytes=json.dumps(payload).encode(),
    )
    runtime = histories["queue"]["repo"] if configured_monorepo else qc["repo"]
    mapping = {"mpei/vibeqc": qc["repo"]} if configured_monorepo else {}
    for name in fleet_release.PIN_NAMES:
        for older, newer, expected in (
            (qc["base"], qc["target"], True),
            (qc["target"], qc["base"], False),
            (qc["target"], qc["ahead"], True),
            (qc["ahead"], qc["target"], False),
            (qc["diverged"], qc["target"], False),
            (qc["target"], qc["diverged"], False),
            ("f" * 40, qc["target"], None),
        ):
            assert fleet_release.git_is_pin_ancestor(
                report, name, runtime, older, newer, pin_repos=mapping,
            ) is expected


def test_split_ancestry_fails_closed_without_searching_other_repositories(
    component_histories,
) -> None:
    report, histories, repos = component_histories
    qc = histories["qc"]
    runtime = histories["queue"]["repo"]
    with pytest.raises(fleet_release.FleetReleaseError, match="no local checkout"):
        fleet_release.git_is_pin_ancestor(
            report, "dev", runtime, qc["base"], qc["target"], pin_repos={},
        )
    with pytest.raises(fleet_release.FleetReleaseError, match="unknown ancestry pin"):
        fleet_release.git_is_pin_ancestor(
            report, "untrusted", runtime, qc["base"], qc["target"], pin_repos=repos,
        )
    # Even when the correct checkout is present under another slug, only the
    # authenticated pin's configured slug is eligible; no SHA-based rerouting.
    wrong = {**repos, "mpei/vibe-qc": runtime}
    assert fleet_release.git_is_pin_ancestor(
        report, "dev", runtime, qc["base"], qc["target"], pin_repos=wrong,
    ) is None
    assert fleet_release.git_is_pin_ancestor(
        report, "dev", runtime, report.pins["vibe_view"].sha, qc["target"],
        pin_repos=repos,
    ) is None


@pytest.mark.parametrize("name", ["release", "dev", "vibe_view"])
def test_split_descendant_repair_preserves_live_sha_and_safety_guards(
    component_histories, name: str,
) -> None:
    report, histories, repos = component_histories
    pin = report.pins[name]
    history = histories["view" if name == "vibe_view" else "qc"]

    def ancestry(pin_name: str, older: str, newer: str) -> bool | None:
        return fleet_release.git_is_pin_ancestor(
            report, pin_name, histories["queue"]["repo"], older, newer,
            pin_repos=repos,
        )

    lane = fleet_rollout.LaneState(
        configured=True, current_sha=history["ahead"], current_version=None,
        current_tag="v0.17.2" if name == "release" else None,
        dirty=False, last_ok=False, acknowledged=False, detail="fixture",
    )
    action = fleet_rollout._action(
        action_id="repair", phase="local-runtime", host="localhost",
        program=name, pin=pin,
        argv=["admin", "update", name, "localhost", *pin.deploy_flags],
        lane=lane, ancestry=ancestry, hold_reason=None, marker_reason=None,
    )
    assert action.decision == "update"
    assert action.target_sha == history["ahead"]
    assert action.argv[action.argv.index("--expected-sha") + 1] == history["ahead"]
    assert pin.sha not in action.argv
    assert fleet_rollout._lane_decision(
        replace(lane, dirty=True), pin=pin, ancestry=ancestry,
    )[0] == "block"
    if name == "release":
        assert action.target_tag == "v0.17.2"
        assert fleet_rollout._lane_decision(
            replace(lane, current_tag=pin.tag), pin=pin, ancestry=ancestry,
        ) == ("block", "same release version has a different SHA")
