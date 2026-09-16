"""First-class, exact-SHA ``vq self-update`` lifecycle coverage."""

from __future__ import annotations

import json
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, cli, config, fleet_release, fleet_rollout

SHA = "a" * 40
CURRENT_SHA = "0" * 40


@pytest.fixture(autouse=True)
def _exact_target_ancestry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep command-shape tests focused beyond the real Git preflight."""
    monkeypatch.setattr(admin, "_run_git_fetch_sha", lambda *args: (0, "fetched"))
    monkeypatch.setattr(admin, "current_source_sha", lambda repo: CURRENT_SHA)
    monkeypatch.setattr(
        fleet_release,
        "git_is_ancestor",
        lambda repo, ancestor, descendant: (
            ancestor == CURRENT_SHA and descendant == SHA
        ),
    )


def _program(tmp_path: Path, python: str = "/managed/venv/bin/python") -> config.VenvProgram:
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    return config.VenvProgram(
        kind="venv",
        python=python,
        git_dir=str(repo),
        branch="main",
        update_script="vibe-queue/scripts/update.sh",
    )


def _probe(
    *,
    self_update: bool,
    manager: str = "systemd",
    available: bool = True,
) -> admin._SelfUpdateProbe:
    return admin._SelfUpdateProbe(
        is_self_update=self_update,
        daemon_running=True,
        service_manager=manager,
        manager_available=available,
        diagnostic=f"{manager} test probe",
    )


def _result(*, restart_ok: bool = True) -> admin.UpdateResult:
    return admin.UpdateResult(
        env="vibeqc-queue",
        git_dir="/managed/repo",
        branch="main",
        update_script=None,
        git_pull_rc=0,
        expected_sha=SHA,
        actual_sha=SHA,
        sha_check_rc=0,
        daemon_restart_attempted=True,
        daemon_restart_succeeded=restart_ok,
        daemon_health_verified=restart_ok,
        daemon_restart_message=(
            "RPC healthy; source SHA verified"
            if restart_ok
            else "systemctl --user restart timed out"
        ),
    )


@contextmanager
def _open_rollout_lock(unused_rollout_id: str):
    yield


@pytest.mark.parametrize("manager", ["systemd", "launchd"])
def test_resolve_self_update_target_uses_service_manager_provenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    manager: str,
) -> None:
    target = _program(tmp_path, "/managed/venv/bin/python")
    other = _program(tmp_path / "other", "/other/venv/bin/python")
    cfg = config.Config(
        programs={"vibeqc-queue": target, "vibeqc-dev": other},
    )

    def detect(prog: config.VenvProgram) -> admin._SelfUpdateProbe:
        return _probe(
            self_update=prog.python == target.python,
            manager=manager,
        )

    monkeypatch.setattr(admin, "_detect_vq_self_update", detect)

    env, resolved, probe = admin.resolve_vq_self_update_target(cfg)

    assert env == "vibeqc-queue"
    assert resolved is target
    assert probe.service_manager == manager
    assert probe.manager_available is True


def test_resolve_self_update_target_rejects_ambiguous_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = config.Config(
        programs={
            "vq-a": _program(tmp_path / "a", "/a/bin/python"),
            "vq-b": _program(tmp_path / "b", "/b/bin/python"),
        },
    )
    monkeypatch.setattr(
        admin,
        "_detect_vq_self_update",
        lambda unused: _probe(self_update=True),
    )

    with pytest.raises(admin.AdminError, match="multiple configured"):
        admin.resolve_vq_self_update_target(cfg)


def test_resolve_self_update_target_rejects_runtime_slot_before_ref_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = _program(tmp_path)
    values = base.model_dump()
    values["runtime_slot_root"] = str(tmp_path / "slots")
    target = config.VenvProgram(**values)
    cfg = config.Config(programs={"vibeqc-queue": target})
    monkeypatch.setattr(
        admin,
        "_detect_vq_self_update",
        lambda unused: _probe(self_update=True),
    )

    with pytest.raises(admin.AdminError, match="runtime_slot_root"):
        admin.resolve_vq_self_update_target(cfg)


@pytest.mark.parametrize(
    ("current", "forward", "backward", "expected"),
    [
        ("b" * 40, False, True, "older"),
        ("b" * 40, False, False, "divergent"),
    ],
)
def test_self_update_refuses_downgrade_or_divergence_before_apply(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    current: str,
    forward: bool,
    backward: bool,
    expected: str,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(admin, "current_source_sha", lambda repo: current)
    monkeypatch.setattr(
        fleet_release,
        "git_is_ancestor",
        lambda repo, ancestor, descendant: (
            forward if (ancestor, descendant) == (current, SHA) else backward
        ),
    )
    monkeypatch.setattr(
        fleet_rollout, "rollout_execution_lock", _open_rollout_lock,
    )
    monkeypatch.setattr(
        admin,
        "update_env",
        lambda *args, **kwargs: pytest.fail("unsafe target reached apply"),
    )
    argv = ["self-update", "--expected-sha", SHA]
    result = CliRunner().invoke(cli.main, argv)
    assert result.exit_code == 1
    assert expected in result.output


def test_self_update_exposes_no_rollback_bypass(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    current = "b" * 40
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(admin, "current_source_sha", lambda repo: current)
    monkeypatch.setattr(
        fleet_release,
        "git_is_ancestor",
        lambda repo, ancestor, descendant: (
            (ancestor, descendant) == (SHA, current)
        ),
    )
    monkeypatch.setattr(
        fleet_rollout, "rollout_execution_lock", _open_rollout_lock,
    )
    monkeypatch.setattr(
        admin,
        "update_env",
        lambda *args, **kwargs: pytest.fail("rollback reached apply"),
    )

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA, "--allow-rollback"],
    )
    assert result.exit_code == 2
    assert "No such option" in result.output


def test_self_update_is_discoverable_and_requires_a_full_sha() -> None:
    runner = CliRunner()

    help_result = runner.invoke(cli.main, ["--help"])
    missing = runner.invoke(cli.main, ["self-update"])
    abbreviated = runner.invoke(
        cli.main,
        ["self-update", "--expected-sha", "abc123"],
    )

    assert help_result.exit_code == 0
    assert "self-update" in help_result.output
    assert missing.exit_code == 2
    assert "--expected-sha" in missing.output
    assert abbreviated.exit_code == 2
    assert "full 40-character" in abbreviated.output


def test_self_update_delegates_to_managed_update_with_restart_required(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    seen: dict[str, object] = {}

    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )

    def update(env: str, unused_cfg: config.Config, **kwargs: object) -> admin.UpdateResult:
        seen["env"] = env
        seen.update(kwargs)
        return _result()

    monkeypatch.setattr(admin, "update_env", update)

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA],
    )

    assert result.exit_code == 0, result.output
    assert seen == {
        "env": "vibeqc-queue",
        "host": "localhost",
        "admin_token": None,
        "expected_sha": SHA,
        "restart_daemon": True,
        "require_self_update": True,
        "force": False,
    }
    assert "== OK ==" in result.output


def test_self_update_enforces_multi_user_admin_gate_before_target_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(
        programs={"vibeqc-queue": prog},
        multi_user=config.MultiUserConfig(enabled=True),
    )
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr("vq.auth.resolve_token", lambda *args, **kwargs: None)
    monkeypatch.setattr("vq.auth.verify_admin_token", lambda unused: False)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: pytest.fail("auth gate must precede target resolution"),
    )

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA],
    )

    assert result.exit_code == 1
    assert "token required" in result.output.lower()


def test_accepted_report_is_not_fetched_before_multi_user_auth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(
        programs={"vibeqc-queue": prog},
        multi_user=config.MultiUserConfig(enabled=True),
    )
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr("vq.auth.resolve_token", lambda *args, **kwargs: None)
    monkeypatch.setattr("vq.auth.verify_admin_token", lambda unused: False)
    monkeypatch.setattr(
        fleet_release,
        "discover_report",
        lambda *args, **kwargs: pytest.fail(
            "unauthorized accepted-report lookup must not fetch refs"
        ),
    )

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--accepted-report", "v0.15.131"],
    )

    assert result.exit_code == 1
    assert "token required" in result.output.lower()


def test_self_update_forwards_resolved_token_without_force(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    seen: dict[str, object] = {}
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        cli,
        "_resolve_admin_token",
        lambda *args, **kwargs: "resolved-secret",
    )
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )

    def update(*args: object, **kwargs: object) -> admin.UpdateResult:
        seen.update(kwargs)
        return _result()

    monkeypatch.setattr(admin, "update_env", update)

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA, "--token-stdin"],
        input="resolved-secret\n",
    )

    assert result.exit_code == 0, result.output
    assert seen["admin_token"] == "resolved-secret"
    assert seen["force"] is False


def test_self_update_refuses_concurrent_rollout_before_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )

    @contextmanager
    def blocked(unused_rollout_id: str):
        raise fleet_rollout.FleetRolloutError("fleet rollout already active")
        yield  # pragma: no cover

    monkeypatch.setattr(fleet_rollout, "rollout_execution_lock", blocked)
    called = False

    def update(*args: object, **kwargs: object) -> admin.UpdateResult:
        nonlocal called
        called = True
        return _result()

    monkeypatch.setattr(admin, "update_env", update)

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA],
    )

    assert result.exit_code == 1
    assert "fleet rollout already active" in result.output
    assert called is False


def test_self_update_preserves_marker_conflict_without_force(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )
    monkeypatch.setattr(
        admin,
        "update_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            admin.AdminUpdateInProgress("admin update marker belongs to another run")
        ),
    )

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA],
    )

    assert result.exit_code == 1
    assert "marker belongs to another run" in result.output
    assert "--force" not in result.output


def test_active_marker_blocks_accepted_report_before_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )
    admin.acquire_admin_update_marker(
        envs=["vibeqc-queue"],
        host="localhost",
    )
    admin._set_owned_admin_update_marker_path(None)
    monkeypatch.setattr(
        fleet_release,
        "discover_report",
        lambda *args, **kwargs: pytest.fail(
            "active marker must block before accepted-report fetch"
        ),
    )
    monkeypatch.setattr(
        admin,
        "update_env",
        lambda *args, **kwargs: pytest.fail("active marker reached update"),
    )

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--accepted-report", "v0.15.131"],
    )

    assert result.exit_code == 1
    assert "admin-update-in-progress marker" in result.output


def test_ordinary_update_ownership_blocks_report_fetch_and_self_update(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )
    entered = threading.Event()
    release = threading.Event()

    def held_update(*args: object, **kwargs: object) -> admin.UpdateResult:
        entered.set()
        assert release.wait(timeout=5)
        return _result()

    monkeypatch.setattr(admin, "_update_env_owned", held_update)
    failure: list[BaseException] = []

    def ordinary_update() -> None:
        try:
            admin.update_env("vibeqc-queue", cfg, host="localhost")
        except BaseException as exc:  # pragma: no cover - asserted below
            failure.append(exc)

    holder = threading.Thread(target=ordinary_update)
    holder.start()
    assert entered.wait(timeout=5)
    monkeypatch.setattr(
        fleet_release,
        "discover_report",
        lambda *args, **kwargs: pytest.fail(
            "contended self-update must not fetch accepted-report refs"
        ),
    )
    try:
        result = CliRunner().invoke(
            cli.main,
            ["self-update", "--accepted-report", "v0.15.131"],
        )
    finally:
        release.set()
        holder.join(timeout=5)

    assert not holder.is_alive()
    assert failure == []
    # v0.26.1: asserted on the classification, not the sentence. This test
    # used to pin "owns the local checkout mutation lock" -- the very string
    # the 2026-09 migration's orchestration grepped for to decide whether to
    # retry. That wording is deliberately changed in this same release to
    # prove the contract is the outcome and not the prose; a test pinning the
    # prose would have made the guarantee aspirational.
    assert result.exit_code == 1
    assert "contended" in result.output.lower() or "retry" in result.output.lower()


def test_self_update_restart_failure_is_machine_readable_and_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )
    monkeypatch.setattr(admin, "update_env", lambda *args, **kwargs: _result(restart_ok=False))

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA, "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["success"] is False
    assert payload["daemon_restart_attempted"] is True
    assert payload["daemon_restart_succeeded"] is False
    assert "timed out" in payload["daemon_restart_message"]


def test_self_update_selects_one_explicit_accepted_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prog = _program(tmp_path)
    reports = tmp_path / "private-reports"
    subprocess.run(["git", "init", "-q", str(reports)], check=True)
    cfg = config.Config(
        programs={"vibeqc-queue": prog}, fleet_report_repo=str(reports),
    )
    pin = fleet_release.FleetPin(
        name="vq",
        sha=SHA,
        version="0.25.0",
        deploy_flags=("--expected-sha", SHA),
        gating_job="test-vq",
        pipeline_id=1,
        evidence_sha=SHA,
        acceptance_rule="A",
    )
    report = fleet_release.FleetReleaseReport(
        source_ref="origin/main",
        source_path="vibe-queue/releases/v0.15.131.json",
        digest_sha256="b" * 64,
        generated_at="2026-08-13T12:00:00Z",
        release_version=(0, 15, 131),
        pins={"vq": pin},
        raw={},
    )
    events: list[str] = []
    updated: list[str] = []
    locked = False

    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        fleet_release,
        "discover_report",
        lambda identity, *, repo, runner: (
            pytest.fail("accepted report lookup must hold the fleet lock")
            if not locked
            else (
                pytest.fail("accepted report lookup used the wrong checkout")
                if repo != reports
                else events.append(f"discover:{identity}") or report
            )
        ),
    )
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    @contextmanager
    def ordered_lock(unused_rollout_id: str):
        nonlocal locked
        locked = True
        events.append("lock")
        try:
            yield
        finally:
            events.append("unlock")
            locked = False

    monkeypatch.setattr(fleet_rollout, "rollout_execution_lock", ordered_lock)

    def update(*args: object, **kwargs: object) -> admin.UpdateResult:
        assert locked
        events.append("update")
        updated.append(str(kwargs["expected_sha"]))
        return _result()

    monkeypatch.setattr(admin, "update_env", update)

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--accepted-report", "v0.15.131", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert events == ["lock", "discover:v0.15.131", "update", "unlock"]
    assert updated == [SHA]
    assert payload["self_update_selector"] == {
        "kind": "accepted-report",
        "identity": "v0.15.131",
    }
    assert payload["accepted_report_path"] == report.source_path


@pytest.mark.parametrize(
    ("failed", "message"),
    [
        (
            replace(
                _result(),
                rolled_back=True,
                rollback_summary="restored checkout and installed extension",
            ),
            "rolled back",
        ),
        (
            replace(
                _result(),
                daemon_restart_succeeded=True,
                daemon_health_verified=False,
                daemon_restart_message="RPC source SHA mismatch",
            ),
            "RPC source SHA mismatch",
        ),
    ],
)
def test_self_update_preserves_rollback_and_provenance_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failed: admin.UpdateResult,
    message: str,
) -> None:
    prog = _program(tmp_path)
    cfg = config.Config(programs={"vibeqc-queue": prog})
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(
        admin,
        "resolve_vq_self_update_target",
        lambda unused: ("vibeqc-queue", prog, _probe(self_update=True)),
    )
    monkeypatch.setattr(
        fleet_rollout,
        "rollout_execution_lock",
        _open_rollout_lock,
    )
    monkeypatch.setattr(admin, "update_env", lambda *args, **kwargs: failed)

    result = CliRunner().invoke(
        cli.main,
        ["self-update", "--expected-sha", SHA],
    )

    assert result.exit_code == 1
    assert message in result.output
