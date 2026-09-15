"""Tests for scheduler-runtime auto-update (v0.16.x).

Coverage shape:
* TestCheckSchedulerRuntimeDrift — decision function under each branch
    (no-repo, no-tags, no-record, drift, no-drift, tag-resolution-failure).
* TestRetiredSchedulerRuntimeAutoUpdate — direct Python entry points stop
    before config enumeration, discovery, or apply.
* TestSchedulerRuntimesCLI — the compatibility flag stops before config,
    routing, refs, status, or mutation and points at accepted-report rollout.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, auto_update, config, paths
from vq.cli import main
from vq.spec import utcnow_iso


def _fake_git_tag_output(tags: list[str]) -> subprocess.CompletedProcess[str]:
    """Return a CompletedProcess that looks like `git tag` output."""
    return subprocess.CompletedProcess(
        args=["git", "-C", "/fake", "tag"],
        returncode=0,
        stdout="\n".join(tags) + ("\n" if tags else ""),
        stderr="",
    )


def _fake_git_tag_failure() -> subprocess.CompletedProcess[str]:
    """Return a failed ``git tag`` probe with a useful diagnostic."""
    return subprocess.CompletedProcess(
        args=["git", "-C", "/fake", "tag"],
        returncode=128,
        stdout="",
        stderr="fatal: not a git repository",
    )


def _fake_rev_list_output(sha: str | None) -> subprocess.CompletedProcess[str]:
    """Return a CompletedProcess for explicit tag commit resolution."""
    if sha is None:
        return subprocess.CompletedProcess(
            args=["git", "-C", "/fake", "rev-parse", "--verify", "refs/tags/v0.15.50^{commit}"],
            returncode=1, stdout="", stderr="fatal: bad revision",
        )
    return subprocess.CompletedProcess(
        args=["git", "-C", "/fake", "rev-parse", "--verify", "refs/tags/v0.15.50^{commit}"],
        returncode=0, stdout=sha + "\n", stderr="",
    )


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_git_repo(path: Path, *, tags: list[str] | None = None) -> Path:
    """Create a fake git repo at ``path``, optionally with given tags."""
    path.mkdir(parents=True, exist_ok=True)
    (path / ".git").mkdir()
    if tags:
        # Write tags into a refs/tags/ structure so git tag can find them.
        tags_dir = path / ".git" / "refs" / "tags"
        tags_dir.mkdir(parents=True, exist_ok=True)
        for tag in tags:
            (tags_dir / tag).write_text("a" * 40 + "\n")
    return path


def _write_config(
    state: Path,
    *,
    scheduler_runtime_source_repo: str | None = None,
    host: str = "host_f",
    scheduler_driver: str = "localhost",
    driver_ssh: str | None = None,
) -> Path:
    """Write a minimal config with a scheduler host + runtime deployment."""
    lines = ['default_host = "localhost"', ""]
    if scheduler_runtime_source_repo:
        lines.append(
            f'scheduler_runtime_source_repo = "{scheduler_runtime_source_repo}"'
        )
        lines.append("")
    lines.extend(
        [
            f'[hosts.{host}]',
            f'ssh = "{host}-login"',
            'scheduler = "pbs"',
            'scheduler_dialect = "torque"',
            'scratch_root = "/home/USER"',
            f'scheduler_driver = "{scheduler_driver}"',
            'fleet_role = "managed"',
            "",
            f'[hosts.{host}.scheduler_runtime_deployments.vibeqc-release]',
            'update_command = "/site/bin/deploy-vibeqc-release"',
            'verify_command = "/site/bin/verify-vibeqc-release"',
            "",
        ]
    )
    if driver_ssh is not None:
        lines.extend(
            [
                f'[hosts.{scheduler_driver}]',
                f'ssh = "{driver_ssh}"',
                "",
            ]
        )
    cfg_path = state / "cfg" / "config.toml"
    cfg_path.write_text("\n".join(lines))
    return cfg_path


def _write_status_record(
    state: Path,
    host: str = "host_f",
    program: str = "vibeqc-release",
    *,
    actual_tag: str | None = None,
    actual_sha: str | None = None,
    last_ok_tag: str | None = None,
    last_ok_sha: str | None = None,
    last_success: bool = True,
) -> None:
    """Write a scheduler-runtime-status.json with one record."""
    status_dir = state / "state"
    status_dir.mkdir(parents=True, exist_ok=True)
    key = f"{host}:{program}"
    record = {
        "host": host,
        "program": program,
        "last_updated_at": utcnow_iso(),
        "last_success": last_success,
        "expected_sha": actual_sha or "-",
        "actual_sha": actual_sha or "-",
        "expected_tag": actual_tag,
        "actual_tag": actual_tag,
        "command_rc": 0,
        "verify_rc": 0,
        "healthy": True,
        "activation": "atomic",
        "active_path": "/home/USER/vibeqc-release/current",
        "health_detail": "ok",
        "quiescent": True,
        "updater_pid": None,
        "errors": [],
        "last_ok_sha": last_ok_sha or actual_sha,
        "last_ok_tag": last_ok_tag or actual_tag,
        "last_ok_at": utcnow_iso() if (last_ok_tag or actual_tag) else None,
        "last_ok_active_path": (
            "/home/USER/vibeqc-release/current"
            if (last_ok_tag or actual_tag)
            else None
        ),
    }
    (status_dir / "scheduler-runtime-status.json").write_text(
        json.dumps({key: record}, indent=2, sort_keys=True)
    )


def _mutate_status_record(state: Path, **changes: object) -> None:
    path = state / "state" / "scheduler-runtime-status.json"
    payload = json.loads(path.read_text())
    record = payload["host_f:vibeqc-release"]
    record.update(changes)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


# ------------------------------------------------------------------


class TestCheckSchedulerRuntimeDrift:
    """Decision-only tests — no apply, no side effects."""

    def test_no_source_repo_configured(self, state_dir: Path) -> None:
        _write_config(state_dir, scheduler_runtime_source_repo=None)
        cfg = config.load_config()
        d = auto_update.check_scheduler_runtime_drift(
            "host_f", "vibeqc-release", cfg,
        )
        assert d.action == "error"
        # v0.26.1: the remedy names the surviving spelling. Telling an
        # operator to set `scheduler_runtime_source_repo`, which is now
        # deprecated and warns on use, would be advice to undo a migration.
        assert "no vibe-qc checkout is configured" in d.reason
        assert "pin_source_repos" in d.reason
        assert "mpei/vibe-qc" in d.reason
        assert "scheduler_runtime_source_repo" not in d.reason

    def test_the_deprecated_spelling_still_satisfies_the_check(
        self, state_dir: Path,
    ) -> None:
        """An unmigrated driver must not be told its checkout is missing."""
        repo = str(state_dir / "not-a-repo")
        Path(repo).mkdir()
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()

        d = auto_update.check_scheduler_runtime_drift(
            "host_f", "vibeqc-release", cfg,
        )

        assert "no vibe-qc checkout is configured" not in d.reason

    def test_source_repo_not_a_git_checkout(self, state_dir: Path) -> None:
        repo = str(state_dir / "not-a-repo")
        Path(repo).mkdir()
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        d = auto_update.check_scheduler_runtime_drift(
            "host_f", "vibeqc-release", cfg,
        )
        assert d.action == "error"
        assert "not a git checkout" in d.reason

    def test_no_semver_tags_in_repo(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        with patch("subprocess.run", return_value=_fake_git_tag_output([])):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "skip"
        assert "no semver tags" in d.reason

    def test_git_tag_failure_is_an_error(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        with patch("subprocess.run", return_value=_fake_git_tag_failure()):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "git tag failed (rc=128)" in d.reason
        assert "not a git repository" in d.reason

    @pytest.mark.parametrize(
        ("error", "detail"),
        [
            (
                subprocess.TimeoutExpired(
                    cmd=["git", "-C", "/fake", "tag"], timeout=15,
                ),
                "timed out",
            ),
            (OSError("permission denied"), "failed to start: permission denied"),
        ],
    )
    def test_git_tag_probe_exception_is_an_error(
        self,
        state_dir: Path,
        error: BaseException,
        detail: str,
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        with patch("subprocess.run", side_effect=error):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert detail in d.reason

    def test_git_worktree_marker_file_is_accepted(self, state_dir: Path) -> None:
        repo_path = state_dir / "repo-worktree"
        repo_path.mkdir()
        (repo_path / ".git").write_text("gitdir: /fake/common/worktrees/repo\n")
        _write_config(
            state_dir, scheduler_runtime_source_repo=str(repo_path),
        )
        _write_status_record(
            state_dir, actual_tag="v0.15.49", actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "update"
        assert d.target_sha == "a" * 40

    def test_no_deployment_record(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert d.target_tag == "v0.15.50"
        assert "no complete LAST OK" in d.reason

    def test_invalid_last_ok_semver_fails_closed(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v01.2.3", actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v1.2.4"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "not valid SemVer" in d.reason

    def test_already_at_latest(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v0.15.50", actual_sha="a" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "skip"
        assert "already at latest semver tag" in d.reason
        assert d.current_sha == "a" * 40
        assert d.target_sha == "a" * 40

    def test_matching_tag_with_mismatched_sha_is_error(
        self, state_dir: Path,
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v0.15.50", actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "equal SemVer precedence" in d.reason
        assert d.current_tag == d.target_tag == "v0.15.50"
        assert d.current_sha == "b" * 40
        assert d.target_sha == "a" * 40

    def test_matching_tag_resolution_failure_is_an_error(
        self, state_dir: Path,
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v0.15.50", actual_sha="a" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output(None),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "could not resolve tag v0.15.50" in d.reason

    def test_drift_detected(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v0.15.50", actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50", "v0.15.51"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "update"
        assert d.target_tag == "v0.15.51"
        assert d.current_tag == "v0.15.50"

    def test_stale_driver_tags_never_propose_a_downgrade(
        self, state_dir: Path
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir, actual_tag="v0.15.51", actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "skip"
        assert d.current_tag == "v0.15.51"
        assert d.target_tag == "v0.15.50"
        assert "downgrade" in d.reason.lower()

    def test_missing_last_ok_tag_with_sha_fails_closed(
        self, state_dir: Path
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag=None,
            actual_sha="b" * 40,
            last_ok_tag=None,
            last_ok_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "LAST OK tag is not a string" in d.reason

    @pytest.mark.parametrize(
        "changes",
        [
            {"last_ok_sha": "garbage"},
            {"last_ok_sha": 123},
            {"last_ok_tag": 123},
            {"last_success": "false"},
            {"host": "host_c"},
            {"program": "vibeqc-dev"},
        ],
    )
    def test_malformed_or_wrong_lane_last_ok_record_fails_closed(
        self,
        state_dir: Path,
        changes: dict[str, object],
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag="v0.15.50",
            actual_sha="b" * 40,
        )
        _mutate_status_record(state_dir, **changes)
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.51"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            decision = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )

        assert decision.action == "error"
        assert "invalid scheduler runtime deployment record" in decision.reason

    @pytest.mark.parametrize(
        ("actual_tag", "actual_sha"),
        [("v0.15.50", "bad"), (123, "b" * 40), ("v0.15.50", 123)],
    )
    def test_malformed_legacy_success_identity_fails_closed(
        self,
        state_dir: Path,
        actual_tag: object,
        actual_sha: object,
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag="v0.15.50",
            actual_sha="b" * 40,
        )
        _mutate_status_record(
            state_dir,
            last_ok_tag=None,
            last_ok_sha=None,
            last_success=True,
            actual_tag=actual_tag,
            actual_sha=actual_sha,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.51"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            decision = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )

        assert decision.action == "error"
        assert "invalid scheduler runtime deployment record" in decision.reason

    def test_equal_precedence_build_metadata_refuses_sideways_move(
        self, state_dir: Path
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag="v1.2.3+build.1",
            actual_sha="b" * 40,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v1.2.3+build.2"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "error"
        assert "sideways move" in d.reason

    def test_equal_precedence_driver_targets_are_ambiguous(
        self, state_dir: Path
    ) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            return_value=_fake_git_tag_output(
                ["v1.2.3+build.1", "v1.2.3+build.2"]
            ),
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )

        assert d.action == "error"
        assert "ambiguous unattended target" in d.reason

    def test_last_ok_survives_failure(self, state_dir: Path) -> None:
        """A failed deploy doesn't overwrite last_ok_tag."""
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag="v0.15.49",       # most recent attempt failed
            actual_sha="c" * 40,
            last_ok_tag="v0.15.50",      # rollback target
            last_ok_sha="b" * 40,
            last_success=False,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50", "v0.15.51"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        # Should compare against last_ok_tag, not actual_tag
        assert d.action == "update"
        assert d.current_tag == "v0.15.50"
        assert d.current_sha == "b" * 40
        assert d.target_tag == "v0.15.51"

    def test_failed_latest_attempt_is_retried(self, state_dir: Path) -> None:
        repo = str(state_dir / "repo")
        _make_git_repo(Path(repo))
        _write_config(state_dir, scheduler_runtime_source_repo=repo)
        _write_status_record(
            state_dir,
            actual_tag="v0.15.50",
            actual_sha="a" * 40,
            last_ok_tag="v0.15.50",
            last_ok_sha="a" * 40,
            last_success=False,
        )
        cfg = config.load_config()
        with patch(
            "subprocess.run",
            side_effect=[
                _fake_git_tag_output(["v0.15.50"]),
                _fake_rev_list_output("a" * 40),
            ],
        ):
            d = auto_update.check_scheduler_runtime_drift(
                "host_f", "vibeqc-release", cfg,
            )
        assert d.action == "update"
        assert d.current_sha == d.target_sha == "a" * 40


class TestRetiredSchedulerRuntimeAutoUpdate:
    """Every public standalone entry point fails before discovery or apply."""

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_single_runtime_entry_fails_closed_without_touching_config(
        self,
        dry_run: bool,
    ) -> None:
        with patch(
            "vq.auto_update.check_scheduler_runtime_drift"
        ) as drift, patch.object(
            admin, "update_scheduler_runtime"
        ) as update:
            outcome = auto_update.auto_update_scheduler_runtime(
                "host_f",
                "vibeqc-release",
                object(),  # type: ignore[arg-type]
                dry_run=dry_run,
            )

        drift.assert_not_called()
        update.assert_not_called()
        assert outcome.decision.action == "error"
        assert "rollout-latest" in outcome.decision.reason
        assert "accepted release report" in outcome.decision.reason
        assert outcome.scheduler_runtime_result is None

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_aggregate_entry_fails_closed_without_enumerating_hosts(
        self,
        dry_run: bool,
    ) -> None:
        with patch(
            "vq.auto_update.scheduler_runtime_deployment_hosts"
        ) as enumerate_hosts, patch.object(
            admin, "update_scheduler_runtime"
        ) as update:
            outcomes = auto_update.auto_update_scheduler_runtimes(
                object(),  # type: ignore[arg-type]
                dry_run=dry_run,
            )

        enumerate_hosts.assert_not_called()
        update.assert_not_called()
        assert len(outcomes) == 1
        assert outcomes[0].decision.action == "error"
        assert "rollout-latest" in outcomes[0].decision.reason

    @pytest.mark.parametrize("role", ["auto", "vq-only", "alias", "excluded"])
    def test_deployment_host_resolver_rejects_every_non_managed_role(
        self,
        state_dir: Path,
        role: str,
    ) -> None:
        _write_config(state_dir)
        cfg = config.load_config()
        update: dict[str, object] = {"fleet_role": role}
        if role == "alias":
            update["fleet_canonical_host"] = "canonical-host_f"
        cfg.hosts["host_f"] = cfg.hosts["host_f"].model_copy(update=update)

        with pytest.raises(admin.AdminError, match="fleet_role='managed'"):
            auto_update.scheduler_runtime_deployment_hosts(cfg)


class TestSchedulerRuntimesCLI:
    """The compatibility flag fails before all stateful/routed work."""

    @pytest.mark.parametrize(
        "args",
        [
            ["--scheduler-runtimes"],
            ["--scheduler-runtimes", "--dry-run"],
            ["vibeqc-release", "--scheduler-runtimes"],
            ["--all", "--scheduler-runtimes"],
            ["--all-hosts", "--scheduler-runtimes"],
            [
                "--scheduler-runtimes",
                "--scheduler-driver-reentry",
                "coordinator",
            ],
        ],
    )
    def test_flag_is_retired_before_config_routing_refs_or_status(
        self,
        args: list[str],
    ) -> None:
        with patch(
            "vq.cli.config.load_config"
        ) as load_config, patch(
            "vq.cli._forward_admin_command"
        ) as forward, patch(
            "vq.auto_update.auto_update_scheduler_runtimes"
        ) as sweep, patch(
            "vq.auto_update.subprocess.run"
        ) as git_probe, patch.object(
            admin, "load_scheduler_runtime_status"
        ) as load_status:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", *args],
            )

        assert result.exit_code == 2
        assert "standalone scheduler-runtime auto-update is disabled" in (
            result.output
        )
        assert "rollout-latest" in result.output
        assert "accepted release report" in result.output
        load_config.assert_not_called()
        forward.assert_not_called()
        sweep.assert_not_called()
        git_probe.assert_not_called()
        load_status.assert_not_called()

    def test_help_identifies_accepted_report_replacement(self) -> None:
        result = CliRunner().invoke(main, ["admin", "auto-update", "--help"])

        assert result.exit_code == 0, result.output
        assert "Retired, fail-closed compatibility flag" in result.output
        assert "rollout-latest" in result.output
