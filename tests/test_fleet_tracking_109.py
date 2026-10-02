"""Rollout lanes that track a branch head instead of the report pin (#109)."""
from __future__ import annotations

import copy
import dataclasses
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from tests import test_fleet_rollout_cli as cli_fixtures
from tests.test_fleet_rollout import (
    DEV,
    FUTURE,
    OLD,
    RELEASE,
    VIEW,
    VQ,
    _config,
    _deployment,
    _env,
    _program,
    _report,
    _safe_durable_action,
    _scheduler_alias_plan,
    _scheduler_runtime_action,
    _snapshots,
)
from vq import config, fleet_release, fleet_rollout
from vq.cli import main

NEWER = "8" * 40
SIDE = "9" * 40
ORDER = {OLD: 0, RELEASE: 1, DEV: 2, VQ: 3, VIEW: 4, FUTURE: 5, NEWER: 6}


def _ancestry(_pin: str, older: str, newer: str) -> bool | None:
    """Linear history in ORDER; SIDE diverged from everything after OLD."""
    if older == newer:
        return True
    if SIDE in (older, newer):
        return older == OLD
    if older not in ORDER or newer not in ORDER:
        return None
    return ORDER[older] < ORDER[newer]


def _target(sha: str = FUTURE, program: str = "vibeqc-dev") -> fleet_rollout.TrackingTarget:
    return fleet_rollout.TrackingTarget(
        program=program,
        pin_name=fleet_rollout.PROGRAM_PINS[program],
        branch="main",
        sha=sha,
        committed_at=1_000,
    )


def _with_dev_lanes(snapshots, sha: str, *, success: bool = True):
    admin, programs, doctor = copy.deepcopy(snapshots)
    for host in ("localhost", "host_d"):
        admin[host]["envs"] = [
            _env("vibeqc-dev", sha, success=success) if env["name"] == "vibeqc-dev" else env
            for env in admin[host]["envs"]
        ]
        programs[host] = [
            _program("vibeqc-dev", sha, "0.15.61.dev5") if item["name"] == "vibeqc-dev" else item
            for item in programs[host]
        ]
    admin["host_f"]["deployments"]["vibeqc-dev"] = _deployment(sha, success=success)
    return admin, programs, doctor


def _build(snapshots=None, tracking=(), cfg=None):
    admin, programs, doctor = snapshots or _snapshots()
    return fleet_rollout.build_plan(
        cfg or _config(),
        _report(),
        admin_status=admin,
        programs=programs,
        doctor=doctor,
        ancestry=_ancestry,
        tracking=tracking,
    )


def _by_id(plan):
    return {action.id: action for action in plan.actions}


DEV_LANES = (
    "scheduler-runtime:host_f:vibeqc-dev",
    "local-runtime:localhost:vibeqc-dev",
    "local-runtime:host_d:vibeqc-dev",
)


# -- planning ---------------------------------------------------------------


def test_tracked_lanes_deploy_the_head_and_every_other_lane_is_unchanged():
    pinned = _build()
    tracked = _build(tracking=(_target(),))

    for action_id in DEV_LANES:
        action = _by_id(tracked)[action_id]
        assert action.decision == "update"
        assert action.target_sha == FUTURE
        assert (action.target_version, action.target_tag) == (None, None)
        assert action.argv[action.argv.index("--expected-sha") + 1] == FUTURE
        assert DEV not in action.argv
        assert action.pin_name == "dev"
    for action_id, action in _by_id(pinned).items():
        if action_id not in DEV_LANES:
            assert dataclasses.asdict(_by_id(tracked)[action_id]) == dataclasses.asdict(action)
    release = _by_id(tracked)["local-runtime:localhost:vibeqc-release"]
    assert (release.target_sha, release.target_tag) == (RELEASE, "v0.15.60")
    # The floor stays auditable in the report; the head is recorded beside it.
    assert tracked.report == pinned.report
    assert tracked.as_dict()["tracking"] == [_target().as_dict()]
    assert "tracking" not in pinned.as_dict()


def test_second_rollout_after_main_advances_deploys_the_new_head():
    at_first_head = _with_dev_lanes(_snapshots(), FUTURE)

    unchanged = _build(at_first_head, tracking=(_target(FUTURE),))
    advanced = _build(at_first_head, tracking=(_target(NEWER),))

    for action_id in DEV_LANES:
        assert _by_id(unchanged)[action_id].decision == "skip"
        assert _by_id(unchanged)[action_id].reason == "already at target with LAST OK=true"
        assert _by_id(advanced)[action_id].decision == "update"
        assert _by_id(advanced)[action_id].target_sha == NEWER


def test_a_lane_already_past_the_head_is_never_moved_back():
    ahead = _with_dev_lanes(_snapshots(), NEWER)
    plan = _build(ahead, tracking=(_target(FUTURE),))
    for action_id in DEV_LANES:
        action = _by_id(plan)[action_id]
        assert action.decision == "skip"
        assert action.reason.endswith("already deployed; no downgrade")


def test_a_head_that_does_not_descend_from_the_pin_blocks_only_tracked_lanes():
    plan = _build(tracking=(_target(OLD),))  # OLD is behind the dev pin
    for action_id in DEV_LANES:
        action = _by_id(plan)[action_id]
        assert action.decision == "block"
        assert "does not descend from the report dev pin" in action.reason
    assert plan.has_blocks
    assert _by_id(plan)["local-runtime:localhost:vibeqc-release"].decision == "update"


def test_a_tracked_lane_on_a_divergent_sha_blocks():
    plan = _build(_with_dev_lanes(_snapshots(), SIDE), tracking=(_target(),))
    for action_id in DEV_LANES:
        assert _by_id(plan)[action_id].decision == "block"
        assert "cannot prove safe ancestry" in _by_id(plan)[action_id].reason


def test_probe_failure_cannot_hide_a_head_behind_the_pin():
    admin, programs, doctor = _snapshots()
    admin["host_d"] = {"error": "ssh: timed out"}
    plan = _build((admin, programs, doctor), tracking=(_target(OLD),))
    assert _by_id(plan)["local-runtime:host_d:vibeqc-dev"].decision == "block"


@pytest.mark.parametrize(
    "target",
    [
        fleet_rollout.TrackingTarget("vibeqc-release", "release", "main", FUTURE),
        fleet_rollout.TrackingTarget("vibeqc-queue", "vq", "main", FUTURE),
        fleet_rollout.TrackingTarget("vibeqc-dev", "release", "main", FUTURE),
        fleet_rollout.TrackingTarget("vibeqc-dev", "dev", "main", "abc123"),
    ],
)
def test_invalid_tracking_targets_are_refused(target):
    with pytest.raises(fleet_rollout.FleetRolloutError, match="invalid tracking target"):
        _build(tracking=(target,))


def test_duplicate_tracking_targets_are_refused():
    with pytest.raises(fleet_rollout.FleetRolloutError, match="invalid tracking target"):
        _build(tracking=(_target(), _target(NEWER)))


# -- --only-tracking ------------------------------------------------------------


def _tracking_cfg(**lanes: str) -> config.Config:
    cfg = _config()
    return cfg.model_copy(update={"fleet_tracking_lanes": lanes or {"vibeqc-dev": "main"}})


def test_only_tracking_needs_configured_tracking_lanes():
    with pytest.raises(fleet_rollout.FleetRolloutError, match="fleet_tracking_lanes"):
        fleet_rollout.resolve_selection(_config(), only_tracking=True)


def _host_vq_converged(plan):
    plan.actions = [
        dataclasses.replace(action, decision="skip", reason="already at target with LAST OK=true")
        if action.phase == "helper" or action.program == "vibeqc-queue" else action
        for action in plan.actions
    ]
    return plan


def test_only_tracking_defers_every_untracked_lane_but_the_driver():
    selection = fleet_rollout.resolve_selection(_tracking_cfg(), only_tracking=True)
    plan = _host_vq_converged(_build(tracking=(_target(),)))
    scoped = fleet_rollout.select_hosts(plan, selection)

    assert selection.scoped and selection.tracking == ("vibeqc-dev",)
    for action in scoped.actions:
        original = _by_id(plan)[action.id]
        if action.id in DEV_LANES or action.phase == "driver":
            assert dataclasses.asdict(action) == dataclasses.asdict(original)
        elif original.decision == "update":
            assert action.decision == "defer"
            assert fleet_rollout.SCOPE_EXCLUDED_REASON in action.reason
            assert "--only-tracking" in action.reason
    assert scoped.tracking == plan.tracking
    assert selection.as_dict()["only_tracking"] == ["vibeqc-dev"]
    assert "only_tracking" not in fleet_rollout.HostSelection().as_dict()


def test_only_tracking_composes_with_host_selection():
    selection = fleet_rollout.resolve_selection(
        _tracking_cfg(), only=("host_d",), only_tracking=True
    )
    plan = _host_vq_converged(_build(tracking=(_target(),)))
    scoped = _by_id(fleet_rollout.select_hosts(plan, selection))
    assert scoped["local-runtime:host_d:vibeqc-dev"].decision == "update"
    assert scoped["local-runtime:localhost:vibeqc-dev"].decision == "defer"
    assert scoped["local-runtime:host_d:vibeqc-release"].decision == "defer"


def test_only_tracking_defers_tracked_lanes_on_a_host_whose_vq_lags():
    selection = fleet_rollout.resolve_selection(_tracking_cfg(), only_tracking=True)
    plan = _host_vq_converged(_build(tracking=(_target(),)))
    plan.actions = [  # host_d's own vq is behind its pin; host_f's helper too
        dataclasses.replace(action, decision="update", reason="target is newer")
        if action.id in ("local-runtime:host_d:vibeqc-queue", "helper:host_f") else action
        for action in plan.actions
    ]
    scoped = _by_id(fleet_rollout.select_hosts(plan, selection))

    for action_id in ("local-runtime:host_d:vibeqc-dev", "scheduler-runtime:host_f:vibeqc-dev"):
        assert scoped[action_id].decision == "defer"
        assert "own vq is not on its report pin" in scoped[action_id].reason
    assert scoped["local-runtime:localhost:vibeqc-dev"].decision == "update"
    # Without --only-tracking the full rollout updates the host vq first.
    full = _by_id(fleet_rollout.select_hosts(plan, fleet_rollout.HostSelection()))
    assert full["local-runtime:host_d:vibeqc-dev"].decision == "update"


def test_only_tracking_verdict_ignores_blocks_on_release_lanes():
    selection = fleet_rollout.resolve_selection(_tracking_cfg(), only_tracking=True)
    plan = _build(tracking=(_target(),))
    plan.actions = [
        dataclasses.replace(action, decision="block", reason="dirty")
        if action.program == "vibeqc-release" else action
        for action in plan.actions
    ]
    restricted = fleet_rollout.restrict_plan(
        plan, selection, keep_phases=fleet_rollout.SCOPE_EXEMPT_PHASES
    )
    assert not restricted.has_blocks
    assert {a.program for a in restricted.actions} == {"vibeqc-queue", "vibeqc-dev"}
    assert restricted.provenance_lanes == []


def test_only_tracking_with_no_tracked_lane_in_the_plan_is_refused():
    selection = fleet_rollout.resolve_selection(_tracking_cfg(), only_tracking=True)
    plan = _build()
    plan.actions = [a for a in plan.actions if a.program != "vibeqc-dev"]
    with pytest.raises(fleet_rollout.FleetRolloutError, match="no tracked lane"):
        fleet_rollout.assert_selection_in_plan(plan, selection)


def test_driver_reentry_carries_tracking_heads_and_scope():
    seen: list[list[str]] = []

    def runner(argv, **kwargs):
        seen.append(argv)
        return subprocess.CompletedProcess(argv, 0)

    fleet_rollout.reenter_after_driver(
        "v0.15.60-aaaaaaaaaaaa", as_json=False, runner=runner,
        selection=fleet_rollout.HostSelection(tracking=("vibeqc-dev",)),
        tracking=(_target(),),
    )
    fleet_rollout.reenter_after_driver("v0.15.60-aaaaaaaaaaaa", as_json=False, runner=runner)

    assert seen[0][-3:] == ["--only-tracking", "--tracking-sha", f"vibeqc-dev={FUTURE}"]
    assert "--only-tracking" not in seen[1] and "--tracking-sha" not in seen[1]


# -- verify-only annotation ----------------------------------------------------


def test_tracking_lane_report_names_sha_age_and_relation_to_head():
    snapshots = _with_dev_lanes(_snapshots(), FUTURE)
    admin, programs, doctor = snapshots
    admin["host_f"]["deployments"]["vibeqc-dev"] = _deployment(VIEW)  # behind head
    plan = _build((admin, programs, doctor))  # verify-only plans against the pin
    times = {FUTURE: 10_000, VIEW: 4_000}

    rows = fleet_rollout.tracking_lane_report(
        plan, (_target(NEWER),), ancestry=_ancestry,
        commit_time=lambda pin, sha: times.get(sha), now=13_600,
    )
    by_host = {row["host"]: row for row in rows}

    assert set(by_host) == {"host_f", "localhost", "host_d"}
    assert by_host["localhost"]["current_sha"] == FUTURE
    assert by_host["localhost"]["age_seconds"] == 3_600
    assert by_host["localhost"]["relation_to_head"] == "behind-head"
    # Descends from the pin, so the verdict is converged even behind the head.
    assert by_host["localhost"]["decision"] == "skip"
    assert by_host["host_f"]["age_seconds"] == 9_600
    at_head = fleet_rollout.tracking_lane_report(
        plan, (_target(FUTURE),), ancestry=_ancestry,
        commit_time=lambda pin, sha: None, now=0,
    )
    assert {row["relation_to_head"] for row in at_head if row["host"] != "host_f"} == {"at-head"}

    payload = fleet_rollout.verify_payload(
        plan=plan, doctor=doctor, rollout_id="r", tracking_lanes=rows,
    )
    text = fleet_rollout.render_verify_text(payload)
    assert payload["tracking"] == rows
    expected = f"TRACKING localhost        vibeqc-dev       {FUTURE[:12]}  1h old, behind-head main"
    assert expected in text
    assert "tracking" not in fleet_rollout.verify_payload(plan=plan, doctor=doctor, rollout_id="r")


def test_verify_only_judges_a_tracked_lane_against_the_pin():
    behind_pin = _with_dev_lanes(_snapshots(), RELEASE)
    descendant = _with_dev_lanes(_snapshots(), NEWER)
    assert _by_id(_build(behind_pin))["local-runtime:localhost:vibeqc-dev"].decision == "update"
    assert _by_id(_build(descendant))["local-runtime:localhost:vibeqc-dev"].decision == "skip"


# -- branch head resolution in real git ------------------------------------------


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, capture_output=True, text=True,
        env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
             "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
             "HOME": str(repo), "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"},
    ).stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "commit", "--allow-empty", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repos(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    _git(work, "init", "-q", "-b", "main")
    base = _commit(work, "base")
    pin = _commit(work, "dev pin")
    head = _commit(work, "main head")
    _git(work, "checkout", "-q", "-b", "side", base)
    side = _commit(work, "side")
    _git(work, "checkout", "-q", "main")
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "clone", "-q", "--bare", str(work), str(origin)], check=True)
    clone = tmp_path / "vibe-qc"
    subprocess.run(["git", "clone", "-q", str(origin), str(clone)], check=True)
    _git(work, "remote", "add", "origin", str(origin))
    return {"work": work, "clone": clone, "pin": pin, "head": head, "side": side}


def _git_report(pin_sha: str) -> fleet_release.FleetReleaseReport:
    report = _report()
    pins = {
        name: dataclasses.replace(pin, repo="mpei/vibe-qc")
        for name, pin in report.pins.items()
    }
    pins["dev"] = dataclasses.replace(
        pins["dev"], sha=pin_sha, deploy_flags=("--expected-sha", pin_sha)
    )
    return dataclasses.replace(report, pins=pins, raw={"schema": fleet_release.REPORT_SCHEMA})


def _resolve(repos, branch="main", pin=None):
    return fleet_release.resolve_branch_heads(
        _git_report(pin or repos["pin"]), {"dev": branch}, repos["clone"],
        pin_repos={"mpei/vibe-qc": repos["clone"]},
    )["dev"]


def test_resolves_and_refreshes_the_branch_head(repos):
    head = _resolve(repos)
    assert (head.sha, head.branch, head.pin_name) == (repos["head"], "main", "dev")
    assert isinstance(head.committed_at, int) and head.committed_at > 0

    newer = _commit(repos["work"], "main advances")
    _git(repos["work"], "push", "-q", "origin", "main")
    assert _resolve(repos).sha == newer


def test_a_branch_that_does_not_contain_the_pin_is_refused(repos):
    with pytest.raises(fleet_release.FleetReleaseError, match="does not contain"):
        _resolve(repos, branch="side")


def test_a_missing_branch_fails_instead_of_using_a_cached_head(repos):
    with pytest.raises(fleet_release.FleetReleaseError, match="could not resolve"):
        _resolve(repos, branch="no-such-branch")


# -- configuration ------------------------------------------------------------------


def test_tracking_lanes_config_accepts_trackable_programs():
    cfg = config.Config(fleet_tracking_lanes={"vibeqc-dev": "main", "vibe-view": "release/2.x"})
    assert fleet_rollout.tracking_branches(cfg) == {"dev": "main", "vibe_view": "release/2.x"}
    assert config.Config().fleet_tracking_lanes == {}


@pytest.mark.parametrize(
    "lanes",
    [
        {"vibeqc-release": "main"},
        {"vibeqc-queue": "main"},
        {"vibeqc-dev": "-main"},
        {"vibeqc-dev": "a..b"},
        {"vibeqc-dev": "x.lock"},
        {"vibeqc-dev": "x/"},
        {"vibeqc-dev": "a b"},
        {"vibeqc-dev": ""},
    ],
)
def test_tracking_lanes_config_refuses_pinned_programs_and_bad_branches(lanes):
    with pytest.raises(ValueError, match="fleet_tracking_lanes"):
        config.Config(fleet_tracking_lanes=lanes)


# -- durable execution ------------------------------------------------------------


def _tracked_action(sha: str, argv: list[str]) -> fleet_rollout.RolloutAction:
    action = _safe_durable_action()
    return dataclasses.replace(
        action, id="local-runtime:localhost:vibeqc-dev", program="vibeqc-dev",
        pin_name="dev", target_sha=sha, target_version=None, argv=argv,
    )


def _durable_plan(action, tracking=()):
    return fleet_rollout.RolloutPlan(
        driver="localhost",
        report={"digest_sha256": "a" * 64, "source_path": "report.json"},
        topology={},
        actions=[action],
        tracking=tracking,
    )


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    return tmp_path


def test_a_new_head_opens_a_new_attempt_after_a_settled_success(state):
    rollout = "v0.15.60-tracking-new-head"
    first = _tracked_action(FUTURE, ["--help", FUTURE])
    fleet_rollout.execute_one(_durable_plan(first, (_target(FUTURE),)), first, rollout_id=rollout)

    second = _tracked_action(NEWER, ["--help", NEWER])
    run = fleet_rollout.execute_one(
        _durable_plan(second, (_target(NEWER),)), second, rollout_id=rollout
    )

    record = run.actions[second.id]
    assert record["status"] == "success" and record["target_sha"] == NEWER
    attempts = record["operation_attempts"]
    assert [item["attempt"] for item in attempts] == [1, 2]
    assert [item["identity"]["target_sha"] for item in attempts] == [FUTURE, NEWER]


def test_the_same_head_twice_still_needs_fresh_plan_attestation(state):
    rollout = "v0.15.60-tracking-same-head"
    action = _tracked_action(FUTURE, ["--help", FUTURE])
    plan = _durable_plan(action, (_target(FUTURE),))
    fleet_rollout.execute_one(plan, action, rollout_id=rollout)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="fresh live plan"):
        fleet_rollout.execute_one(plan, action, rollout_id=rollout)


def test_an_untracked_lane_with_a_new_target_keeps_the_identity_refusal(state):
    rollout = "v0.15.60-untracked-new-target"
    first = _tracked_action(FUTURE, ["--help", FUTURE])
    fleet_rollout.execute_one(_durable_plan(first), first, rollout_id=rollout)
    second = _tracked_action(NEWER, ["--help", NEWER])
    with pytest.raises(fleet_rollout.FleetRolloutError, match="does not bind"):
        fleet_rollout.execute_one(_durable_plan(second), second, rollout_id=rollout)


def _fail_at_future(rollout):
    failed = _tracked_action(FUTURE, ["--not-a-real-option", FUTURE])
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(
            _durable_plan(failed, (_target(FUTURE),)), failed, rollout_id=rollout
        )
    return failed


def _reconcile_failure(rollout, action_id):
    run = fleet_rollout.load_run(rollout)
    (attempt,) = run.actions[action_id]["operation_attempts"]
    fleet_rollout._set_failure_fence_state(
        run, action_id=action_id, operation_id=attempt["operation_id"],
        consumed=True, retry_authorized=True,
    )
    fleet_rollout.save_run(run)


@pytest.mark.parametrize("reconciled_run", [False, True])
def test_an_unreconciled_failure_is_never_bypassed_by_a_new_head(state, reconciled_run):
    rollout = f"v0.15.60-tracking-after-failure-{reconciled_run}"
    failed = _fail_at_future(rollout)
    # The invocation may be a reconciled one; the fence itself was never consumed.
    second = _tracked_action(NEWER, ["--not-a-real-option", NEWER])
    with pytest.raises(fleet_rollout.FleetRolloutError, match="does not bind"):
        fleet_rollout.execute_one(
            _durable_plan(second, (_target(NEWER),)), second, rollout_id=rollout,
            durable_reconciled=reconciled_run,
        )
    attempts = fleet_rollout.load_run(rollout).actions[failed.id]["operation_attempts"]
    assert len(attempts) == 1


def test_a_reconciled_failure_may_retry_at_the_new_head(state):
    rollout = "v0.15.60-tracking-reconciled-retry"
    failed = _fail_at_future(rollout)
    _reconcile_failure(rollout, failed.id)
    retry = _tracked_action(NEWER, ["--not-a-real-option", NEWER])
    plan = _durable_plan(retry, (_target(NEWER),))

    # Only a reconciled invocation may retry, exactly as for the same target.
    with pytest.raises(fleet_rollout.FleetRolloutError):
        fleet_rollout.execute_one(plan, retry, rollout_id=rollout)
    retry.argv = ["--help", NEWER]
    with pytest.raises(fleet_rollout.FleetRolloutError, match="does not bind"):
        fleet_rollout.execute_one(plan, retry, rollout_id=rollout, durable_reconciled=True)

    retry.argv = ["--not-a-real-option", NEWER]
    with pytest.raises(fleet_rollout.FleetOperationFailed):
        fleet_rollout.execute_one(plan, retry, rollout_id=rollout, durable_reconciled=True)
    attempts = fleet_rollout.load_run(rollout).actions[failed.id]["operation_attempts"]
    assert [item["attempt"] for item in attempts] == [1, 2]
    assert [item["identity"]["target_sha"] for item in attempts] == [FUTURE, NEWER]


def test_a_new_head_with_any_other_argv_change_keeps_the_refusal(state):
    rollout = "v0.15.60-tracking-argv-change"
    first = _tracked_action(FUTURE, ["--help", FUTURE])
    fleet_rollout.execute_one(_durable_plan(first, (_target(FUTURE),)), first, rollout_id=rollout)
    second = _tracked_action(NEWER, ["--version", NEWER])
    with pytest.raises(fleet_rollout.FleetRolloutError, match="does not bind"):
        fleet_rollout.execute_one(
            _durable_plan(second, (_target(NEWER),)), second, rollout_id=rollout
        )


def test_only_tracking_parity_ignores_a_blocked_untracked_lane(state):
    tracked = _scheduler_runtime_action("vibeqc-dev")
    tracked.target_sha, tracked.target_version = FUTURE, None
    blocked = _scheduler_runtime_action("vibeqc-release", decision="block")
    blocked.reason = "managed checkout/runtime is dirty"
    initial = fleet_rollout.select_hosts(
        _scheduler_alias_plan(tracked, blocked),
        fleet_rollout.HostSelection(tracking=("vibeqc-dev",)),
    )
    final_dev = dataclasses.replace(tracked, decision="skip",
                                    reason="already at target with LAST OK=true")
    verification = _scheduler_alias_plan(final_dev, blocked)
    run = fleet_rollout.RolloutRun(
        rollout_id="v0.15.60-only-tracking-parity",
        report_digest_sha256=str(initial.report["digest_sha256"]),
        report_source_path=str(initial.report["source_path"]),
    )
    fleet_rollout.save_run(run)
    calls: list[list[str]] = []

    def control(argv, **kwargs):
        calls.append(argv)
        if "--status" in argv:
            return subprocess.CompletedProcess(argv, 0, stdout=json.dumps({
                "active": False, "is_full_drain": False, "scheduler_hosts": [],
                "legacy_scheduler_hosts": [], "scheduler_leases": [], "state": None,
            }), stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    result = fleet_rollout.reconcile_scheduler_parity_holds(
        initial, verification,
        doctor={"host_f": {"ok": True}, "host_f-big": {"ok": True}},
        run=run, runner=control,
        selection=fleet_rollout.HostSelection(tracking=("vibeqc-dev",)),
    )

    assert not any(h.get("status") == "active" for h in result.holds.values())
    assert not [argv for argv in calls if "--status" not in argv and "--release" not in argv]


# -- CLI --------------------------------------------------------------------------


def _patch_cli(monkeypatch, *, lanes=None, build_kwargs=None, plans=None):
    report = cli_fixtures._report()
    plan = cli_fixtures._plan(report, [cli_fixtures._action(action_id="driver", decision="skip")])
    cli_fixtures._patch_discovery(
        monkeypatch, report=report, plans=plans or [plan, plan], build_kwargs=build_kwargs,
    )
    cfg = config.load_config()  # the fixture's patched config
    lanes = lanes if lanes is not None else {"vibeqc-dev": "main"}
    cfg = cfg.model_copy(update={"fleet_tracking_lanes": lanes})
    monkeypatch.setattr("vq.cli.config.load_config", lambda: cfg)
    resolved: list[dict[str, str]] = []

    def resolve(report, branches, repo, **kwargs):
        resolved.append(dict(branches))
        return {
            pin: fleet_release.BranchHead(pin_name=pin, branch=branch, sha=FUTURE, committed_at=5)
            for pin, branch in branches.items()
        }

    monkeypatch.setattr("vq.cli.fleet_release.resolve_branch_heads", resolve)
    return resolved


def test_dry_run_resolves_heads_once_and_plans_with_them(monkeypatch):
    build_kwargs: list[dict[str, Any]] = []
    resolved = _patch_cli(monkeypatch, build_kwargs=build_kwargs)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run", "--json"])

    assert result.exit_code == 0, result.output
    assert resolved == [{"dev": "main"}]
    assert [kw["tracking"] for kw in build_kwargs] == [(_target(FUTURE).__class__(
        program="vibeqc-dev", pin_name="dev", branch="main", sha=FUTURE, committed_at=5),)]


def test_verify_only_plans_against_the_pin(monkeypatch):
    build_kwargs: list[dict[str, Any]] = []
    resolved = _patch_cli(monkeypatch, build_kwargs=build_kwargs)
    monkeypatch.setattr("vq.cli.fleet_rollout.collect_console_states", lambda cfg, hosts: {})

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--verify-only", "--json"])

    assert result.exit_code == 0, result.output
    assert resolved == [{"dev": "main"}]
    assert build_kwargs[0]["tracking"] == ()


def test_no_tracking_config_resolves_nothing(monkeypatch):
    build_kwargs: list[dict[str, Any]] = []
    resolved = _patch_cli(monkeypatch, lanes={}, build_kwargs=build_kwargs)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run", "--json"])

    assert result.exit_code == 0, result.output
    assert resolved == []
    assert build_kwargs[0]["tracking"] == ()
    assert "tracking" not in json.loads(result.output)


@pytest.mark.parametrize(
    "args",
    [
        ["--tracking-sha", f"vibeqc-dev={FUTURE}"],
        ["--only-tracking", "--reconcile-legacy"],
    ],
)
def test_protocol_and_recovery_combinations_are_usage_errors(args):
    result = CliRunner().invoke(main, ["admin", "rollout-latest", *args])
    assert result.exit_code == 2


def _unresolvable(monkeypatch):
    def resolve(*args, **kwargs):
        raise fleet_release.FleetReleaseError("could not resolve tracked branch 'main'")

    monkeypatch.setattr("vq.cli.fleet_release.resolve_branch_heads", resolve)


def test_verify_only_still_answers_when_the_heads_cannot_be_resolved(monkeypatch):
    _patch_cli(monkeypatch)
    _unresolvable(monkeypatch)
    monkeypatch.setattr("vq.cli.fleet_rollout.collect_console_states", lambda cfg, hosts: {})

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--verify-only", "--json"])

    assert result.exit_code == 0, result.output
    assert "tracking heads unavailable" in result.stderr


def test_a_rollout_refuses_when_the_heads_cannot_be_resolved(monkeypatch):
    _patch_cli(monkeypatch)
    _unresolvable(monkeypatch)

    result = CliRunner().invoke(main, ["admin", "rollout-latest", "--dry-run", "--json"])

    assert result.exit_code != 0
    assert "could not resolve tracked branch" in result.output


def test_reentry_argv_round_trips_to_the_same_targets():
    from vq import cli

    cfg = _tracking_cfg()
    parent = (_target(FUTURE),)
    argv: list[str] = []
    fleet_rollout.reenter_after_driver(
        "r", as_json=False, tracking=parent,
        runner=lambda a, **k: argv.extend(a) or subprocess.CompletedProcess(a, 0),
    )
    values = tuple(argv[i + 1] for i, a in enumerate(argv) if a == "--tracking-sha")

    child = cli._tracking_from_reentry(
        cfg, _report(), values, commit_time=lambda pin, sha: 1_000,
    )

    assert child == parent
    with pytest.raises(fleet_rollout.FleetRolloutError, match="do not match"):
        cli._tracking_from_reentry(cfg, _report(), (), commit_time=lambda p, s: None)
    with pytest.raises(fleet_rollout.FleetRolloutError, match="malformed"):
        cli._tracking_from_reentry(cfg, _report(), ("vibeqc-dev",), commit_time=lambda p, s: None)
