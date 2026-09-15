"""Tests for v0.5.17 auto-cleanup policy + daemon main-loop hook.

Three layers (same as the operator-controls suite):
1. AutoCleanupPolicy model + persistence.
2. ``run_auto_cleanup_pass`` and ``should_run_auto_cleanup`` logic.
3. Daemon ``iterate()`` hook + CLI ``vq cleanup --auto-*`` flags.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from vq import cleanup, config, paths
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.state_root().mkdir(parents=True, exist_ok=True)
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _write_terminal_spec(
    jobid: str,
    *,
    state_val: JobState = JobState.COMPLETED,
    finished_at: str | None = None,
) -> JobSpec:
    """Write a fake terminal-state spec with a controllable finished_at."""
    workspace = paths.jobs_dir() / jobid
    workspace.mkdir(parents=True, exist_ok=True)
    (workspace / "marker.txt").write_text("hello\n")
    spec = JobSpec(
        id=jobid,
        command=["python", "x.py"],
        cwd=str(workspace),
        cpus=1,
        state=state_val,
        finished_at=finished_at,
        exit_code=0 if state_val == JobState.COMPLETED else 1,
    )
    spec.write(paths.spec_path(jobid))
    return spec


class TestAutoCleanupPolicyModel:
    def test_defaults(self) -> None:
        p = cleanup.AutoCleanupPolicy()
        assert p.enabled is True
        assert p.archive_after_seconds is None
        assert p.delete_after_seconds is None
        assert p.interval_seconds == 24 * 60 * 60
        assert p.last_run_at is None

    def test_extra_fields_rejected(self) -> None:
        with pytest.raises(ValidationError):
            cleanup.AutoCleanupPolicy(bogus_field="x")  # type: ignore[call-arg]

    @pytest.mark.parametrize(
        "field",
        [
            "archive_after_seconds",
            "delete_after_seconds",
            "interval_seconds",
            "workdir_max_age_seconds",
        ],
    )
    @pytest.mark.parametrize(
        "value", [True, False, 0, -1, 1.0, 1.5, "1"]
    )
    def test_duration_fields_require_strict_positive_integers(
        self, field: str, value: object
    ) -> None:
        with pytest.raises(ValidationError):
            cleanup.AutoCleanupPolicy.model_validate({field: value})

    @pytest.mark.parametrize(
        "field", ["archive_after_by_state", "delete_after_by_state"]
    )
    @pytest.mark.parametrize(
        "value", [True, False, 0, -1, 1.0, 1.5, "1"]
    )
    def test_per_state_durations_require_strict_positive_integers(
        self, field: str, value: object
    ) -> None:
        with pytest.raises(ValidationError):
            cleanup.AutoCleanupPolicy.model_validate(
                {field: {"failed": value}}
            )

    def test_one_second_durations_remain_valid(self) -> None:
        policy = cleanup.AutoCleanupPolicy(
            archive_after_seconds=1,
            delete_after_seconds=1,
            interval_seconds=1,
            workdir_max_age_seconds=1,
            archive_after_by_state={"failed": 1},
            delete_after_by_state={"failed": 1},
        )
        assert policy.archive_after_seconds == 1
        assert policy.delete_after_seconds == 1
        assert policy.interval_seconds == 1
        assert policy.workdir_max_age_seconds == 1
        assert policy.archive_after_by_state == {"failed": 1}
        assert policy.delete_after_by_state == {"failed": 1}
        optional = cleanup.AutoCleanupPolicy(
            archive_after_seconds=None,
            delete_after_seconds=None,
            workdir_max_age_seconds=None,
        )
        assert optional.archive_after_seconds is None
        assert optional.delete_after_seconds is None
        assert optional.workdir_max_age_seconds is None

    def test_interval_cannot_be_disabled_with_none(self) -> None:
        with pytest.raises(ValidationError):
            cleanup.AutoCleanupPolicy(interval_seconds=None)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("archive_after_seconds", 0),
            ("delete_after_seconds", -1),
            ("interval_seconds", True),
            ("workdir_max_age_seconds", "1"),
            ("archive_after_by_state", {"failed": -1}),
            ("delete_after_by_state", {"failed": True}),
        ],
    )
    def test_invalid_duration_assignment_is_rejected(
        self, field: str, value: object
    ) -> None:
        policy = cleanup.AutoCleanupPolicy()
        before = policy.model_dump()
        with pytest.raises(ValidationError):
            setattr(policy, field, value)
        assert policy.model_dump() == before


class TestAutoCleanupPersistence:
    def test_read_when_no_file(self, state: Path) -> None:
        assert cleanup.read_auto_cleanup_policy() is None

    def test_write_read_roundtrip(self, state: Path) -> None:
        p = cleanup.AutoCleanupPolicy(
            archive_after_seconds=86400 * 30,
            delete_after_seconds=86400 * 90,
            interval_seconds=3600,
            reason="testing",
        )
        cleanup.write_auto_cleanup_policy(p)
        back = cleanup.read_auto_cleanup_policy()
        assert back is not None
        assert back.archive_after_seconds == 86400 * 30
        assert back.delete_after_seconds == 86400 * 90
        assert back.interval_seconds == 3600
        assert back.reason == "testing"

    def test_clear_idempotent(self, state: Path) -> None:
        assert cleanup.clear_auto_cleanup_policy() is False
        assert cleanup.clear_auto_cleanup_policy() is False

    def test_corrupt_file_returns_none(self, state: Path) -> None:
        path = cleanup.auto_cleanup_policy_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not valid json {{{")
        assert cleanup.read_auto_cleanup_policy() is None

    @pytest.mark.parametrize(
        "payload",
        [
            {"archive_after_seconds": 0},
            {"delete_after_seconds": -1},
            {"interval_seconds": True},
            {"workdir_max_age_seconds": 1.5},
            {"archive_after_by_state": {"failed": "1"}},
            {"delete_after_by_state": {"failed": False}},
        ],
    )
    def test_invalid_duration_file_returns_none(
        self, state: Path, payload: dict[str, object]
    ) -> None:
        path = cleanup.auto_cleanup_policy_path()
        path.write_text(json.dumps(payload))
        assert cleanup.read_auto_cleanup_policy() is None


class TestShouldRunAutoCleanup:
    def test_disabled_returns_false(self) -> None:
        p = cleanup.AutoCleanupPolicy(enabled=False)
        assert cleanup.should_run_auto_cleanup(p) is False

    def test_never_run_returns_true(self) -> None:
        p = cleanup.AutoCleanupPolicy(interval_seconds=3600)
        assert cleanup.should_run_auto_cleanup(p) is True

    def test_last_run_recent_returns_false(self) -> None:
        recent = (datetime.now(UTC) - timedelta(seconds=60)).isoformat()
        p = cleanup.AutoCleanupPolicy(
            interval_seconds=3600,
            last_run_at=recent,
        )
        assert cleanup.should_run_auto_cleanup(p) is False

    def test_last_run_long_ago_returns_true(self) -> None:
        old = (datetime.now(UTC) - timedelta(hours=48)).isoformat()
        p = cleanup.AutoCleanupPolicy(
            interval_seconds=3600,
            last_run_at=old,
        )
        assert cleanup.should_run_auto_cleanup(p) is True

    def test_corrupt_last_run_at_returns_true(self) -> None:
        """Robust against a malformed timestamp on disk: better to run
        the sweep than to silently never run again."""
        p = cleanup.AutoCleanupPolicy(
            interval_seconds=3600,
            last_run_at="not a real timestamp",
        )
        assert cleanup.should_run_auto_cleanup(p) is True


class TestRunAutoCleanupPass:
    def test_archive_pass_archives_old_jobs(self, state: Path) -> None:
        """archive_after_seconds=60; spec finished 2 minutes ago → archived."""
        old = (datetime.now(UTC) - timedelta(seconds=120)).isoformat()
        _write_terminal_spec("oldjob000001", finished_at=old)

        p = cleanup.AutoCleanupPolicy(archive_after_seconds=60)
        counts = cleanup.run_auto_cleanup_pass(p)
        assert counts["archived"] == 1
        assert counts["deleted"] == 0
        # Spec should now be archived.
        recovered = JobSpec.read(paths.spec_path("oldjob000001"))
        assert recovered.is_archived

    def test_archive_pass_skips_recent_jobs(self, state: Path) -> None:
        recent = (datetime.now(UTC) - timedelta(seconds=30)).isoformat()
        _write_terminal_spec("recentjob001", finished_at=recent)
        p = cleanup.AutoCleanupPolicy(archive_after_seconds=3600)
        counts = cleanup.run_auto_cleanup_pass(p)
        assert counts["archived"] == 0

    def test_delete_pass_deletes_old_jobs(self, state: Path) -> None:
        very_old = (datetime.now(UTC) - timedelta(days=200)).isoformat()
        _write_terminal_spec("ancient00001", finished_at=very_old)
        p = cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 90)
        counts = cleanup.run_auto_cleanup_pass(p)
        assert counts["deleted"] == 1
        # Spec file gone.
        assert not paths.spec_path("ancient00001").exists()

    def test_archive_then_delete_in_one_pass(self, state: Path) -> None:
        """Archive-after 30d, delete-after 90d. A 100-day-old non-archived
        job goes archive (passes archive-after) → then delete (passes
        delete-after). End state: gone entirely."""
        very_old = (datetime.now(UTC) - timedelta(days=100)).isoformat()
        _write_terminal_spec("vacuum000001", finished_at=very_old)
        p = cleanup.AutoCleanupPolicy(
            archive_after_seconds=86400 * 30,
            delete_after_seconds=86400 * 90,
        )
        counts = cleanup.run_auto_cleanup_pass(p)
        # Both passes counted the job.
        assert counts["archived"] == 1
        assert counts["deleted"] == 1
        # No spec, no workspace.
        assert not paths.spec_path("vacuum000001").exists()

    def test_skips_non_terminal_jobs(self, state: Path) -> None:
        """Running / pending / suspended jobs must never be touched."""
        old = (datetime.now(UTC) - timedelta(days=100)).isoformat()
        _write_terminal_spec(
            "running00001", state_val=JobState.RUNNING, finished_at=old,
        )
        # The find_candidates logic already filters by is_terminal; this
        # test pins that auto-cleanup honours it too. A RUNNING spec with
        # finished_at set is unusual but harmless; we just verify it's
        # not touched.
        p = cleanup.AutoCleanupPolicy(delete_after_seconds=86400 * 30)
        counts = cleanup.run_auto_cleanup_pass(p)
        assert counts["deleted"] == 0
        assert paths.spec_path("running00001").exists()

    def test_run_updates_last_run_at_even_on_zero_candidates(
        self, state: Path
    ) -> None:
        """Stamping last_run_at on every pass prevents an empty queue from
        re-running the sweep on every iteration of the daemon loop."""
        p = cleanup.AutoCleanupPolicy(archive_after_seconds=3600)
        assert p.last_run_at is None
        cleanup.write_auto_cleanup_policy(p)
        cleanup.run_auto_cleanup_pass(p)
        # The state file on disk should have last_run_at stamped now.
        back = cleanup.read_auto_cleanup_policy()
        assert back is not None
        assert back.last_run_at is not None


class TestFormatAutoCleanupStatus:
    def test_no_policy(self, state: Path) -> None:
        out = cleanup.format_auto_cleanup_status()
        assert "disabled" in out

    def test_policy_with_caps(self, state: Path) -> None:
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            archive_after_seconds=86400 * 30,
            delete_after_seconds=86400 * 90,
            interval_seconds=3600,
        ))
        out = cleanup.format_auto_cleanup_status()
        assert "ENABLED" in out
        assert "archive_after=2592000s" in out
        assert "delete_after=7776000s" in out
        assert "interval=3600s" in out
        assert "last_run=never" in out
        assert "next_run=due" in out

    def test_status_reports_next_run_when_interval_not_elapsed(
        self, state: Path
    ) -> None:
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            archive_after_seconds=3600,
            interval_seconds=3600,
            last_run_at="2999-01-01T12:00:00+00:00",
        ))
        out = cleanup.format_auto_cleanup_status()
        assert "last_run_at=2999-01-01T12:00:00+00:00" in out
        assert "next_run_at=2999-01-01T13:00:00+00:00" in out

    def test_status_reports_due_when_interval_elapsed(self, state: Path) -> None:
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            archive_after_seconds=3600,
            interval_seconds=1,
            last_run_at="2000-01-01T00:00:00+00:00",
        ))
        out = cleanup.format_auto_cleanup_status()
        assert "next_run=due" in out

    def test_status_reports_paused_policy(self, state: Path) -> None:
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            enabled=False,
            archive_after_seconds=3600,
            last_run_at="2000-01-01T00:00:00+00:00",
        ))
        out = cleanup.format_auto_cleanup_status()
        assert "auto-cleanup: PAUSED" in out
        assert "next_run=paused" in out

    def test_status_reports_workdir_sweep_age(self, state: Path) -> None:
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            archive_after_seconds=3600,
            workdir_max_age_seconds=14 * 86400,
        ))
        out = cleanup.format_auto_cleanup_status()
        assert "workdir_max_age=1209600s" in out


class TestDaemonHook:
    def test_iterate_runs_cleanup_when_policy_present(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """iterate() should call _maybe_auto_cleanup, which runs the
        sweep when policy + interval-elapsed."""
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        very_old = (datetime.now(UTC) - timedelta(days=100)).isoformat()
        _write_terminal_spec("daemonkid001", finished_at=very_old)
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            delete_after_seconds=86400 * 30,
        ))

        d = Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
        d.iterate()

        # The terminal-state spec should have been deleted by the sweep.
        assert not paths.spec_path("daemonkid001").exists()

    def test_iterate_noop_when_no_policy(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        very_old = (datetime.now(UTC) - timedelta(days=100)).isoformat()
        _write_terminal_spec("survives0001", finished_at=very_old)

        d = Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
        d.iterate()

        # No policy → spec survives.
        assert paths.spec_path("survives0001").exists()

    def test_invalid_persisted_policy_never_reaches_cleanup_pass(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq.daemon import Daemon

        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        policy_path = cleanup.auto_cleanup_policy_path()
        policy_path.write_text(json.dumps({"delete_after_seconds": -1}))
        calls: list[bool] = []
        monkeypatch.setattr(
            "vq.cleanup.run_auto_cleanup_pass",
            lambda *args, **kwargs: calls.append(True),
        )

        daemon = Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
        daemon._maybe_auto_cleanup()

        assert cleanup.read_auto_cleanup_policy() is None
        assert calls == []

    def test_iterate_swallows_cleanup_exception(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A botched cleanup must not propagate out of iterate() and
        take down the daemon's dispatch loop."""
        from vq.daemon import Daemon
        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        # Patch run_auto_cleanup_pass to raise.
        def boom(*a, **kw):
            raise RuntimeError("simulated cleanup failure")
        monkeypatch.setattr("vq.cleanup.run_auto_cleanup_pass", boom)
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            delete_after_seconds=86400 * 30,
        ))

        d = Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
        # Must not raise:
        d.iterate()

    @pytest.mark.parametrize(
        "active_collection",
        ["_running", "_orphans", "_scheduler_running", "_terminal_survivors"],
    )
    def test_due_cleanup_defers_until_jobs_are_quiescent(
        self,
        state: Path,
        monkeypatch: pytest.MonkeyPatch,
        active_collection: str,
    ) -> None:
        """Retention must not starve child/scheduler reconciliation.

        Regression for the 2026-08-01 localhost incident: a build-env child
        exited successfully while a due cleanup synchronously scanned nearly
        20,000 specs, so the daemon did not reap it and its spec stayed
        RUNNING. Every active-job collection gates automatic cleanup; once it
        clears, the still-due pass runs normally.
        """
        from vq.daemon import Daemon

        monkeypatch.setattr("vq.cgroup.available", lambda: False)
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            delete_after_seconds=86400 * 30,
        ))
        calls: list[bool] = []
        monkeypatch.setattr(
            "vq.cleanup.run_auto_cleanup_pass",
            lambda *args, **kwargs: calls.append(True),
        )

        d = Daemon(max_cpus=4, max_jobs=1, max_mem_mb=None)
        active = {"live-job": object()}
        setattr(d, active_collection, active)

        d._maybe_auto_cleanup()
        assert calls == []

        active.clear()
        d._maybe_auto_cleanup()
        assert calls == [True]


class TestAutoCleanupCLI:
    def test_auto_status_when_no_policy(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-status"])
        assert result.exit_code == 0
        assert "disabled" in result.output

    def test_auto_enable_writes_policy(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--archive-after", "30d",
             "--delete-after", "90d", "--interval", "6h",
             "--reason", "daily housekeeping"],
        )
        assert result.exit_code == 0, result.output
        assert "auto-cleanup enabled" in result.output
        p = cleanup.read_auto_cleanup_policy()
        assert p is not None
        assert p.archive_after_seconds == 86400 * 30
        assert p.delete_after_seconds == 86400 * 90
        assert p.interval_seconds == 6 * 3600
        assert p.reason == "daily housekeeping"

    def test_auto_enable_requires_one_window(self, state: Path) -> None:
        """--auto-enable with neither --archive-after nor --delete-after
        is rejected (the policy would have nothing to do)."""
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-enable"])
        assert result.exit_code != 0
        assert "requires at least one" in result.output

    def test_auto_disable_clears_policy(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        cleanup.write_auto_cleanup_policy(cleanup.AutoCleanupPolicy(
            archive_after_seconds=86400,
        ))
        result = CliRunner().invoke(main, ["cleanup", "--auto-disable"])
        assert result.exit_code == 0
        assert "disabled" in result.output
        assert cleanup.read_auto_cleanup_policy() is None

    def test_auto_disable_noop_when_no_policy(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-disable"])
        assert result.exit_code == 0
        assert "no-op" in result.output or "not enabled" in result.output

    def test_auto_mutex_with_archive(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--archive", "--older-than", "30d"],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_auto_mutex_among_auto_flags(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main, ["cleanup", "--auto-enable", "--auto-status"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_auto_status_after_enable(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--delete-after", "30d",
             "--reason", "test"],
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-status"])
        assert result.exit_code == 0
        assert "ENABLED" in result.output
        assert "delete_after" in result.output
        assert "test" in result.output


# ----------------------------------------------------------------------
# v0.5.22: configurable archive_dir
# ----------------------------------------------------------------------


class TestArchiveDirEnvOverride:
    """``$VQ_ARCHIVE_DIR`` redirects every archive path away from the
    default ``<state_root>/archive/``. Useful for the small-~ /
    big-secondary-disk case on host_d."""

    def test_env_var_overrides_default(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        custom = state / "custom-archive-location"
        monkeypatch.setenv(paths.ENV_ARCHIVE_DIR, str(custom))
        assert paths.archive_dir() == custom

    def test_unset_env_var_returns_default(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(paths.ENV_ARCHIVE_DIR, raising=False)
        assert paths.archive_dir() == state / "state" / "archive"

    def test_archive_workspace_uses_env_var(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The whole archive-workspace path resolves through paths.archive_dir,
        so setting the env var redirects the tarball physically."""
        from vq.cleanup import archive_workspace
        custom = state / "elsewhere"
        monkeypatch.setenv(paths.ENV_ARCHIVE_DIR, str(custom))
        spec = _write_terminal_spec(
            "aaaa00000abc",
            finished_at=datetime.now(UTC).isoformat(),
        )
        archive = archive_workspace(spec)
        assert archive.parent == custom
        assert archive.exists()


class TestPolicyArchiveDirField:
    """``AutoCleanupPolicy.archive_dir`` is a per-policy override, set
    by the user via ``--archive-dir`` on ``--auto-enable``. Persisted
    in the policy JSON; the auto-cleanup pass reads it and passes
    through to ``archive_workspace``."""

    def test_archive_dir_persisted_in_policy_json(self, state: Path) -> None:
        from vq.cleanup import (
            AutoCleanupPolicy,
            read_auto_cleanup_policy,
            write_auto_cleanup_policy,
        )
        policy = AutoCleanupPolicy(
            archive_after_seconds=30 * 86400,
            archive_dir=str(state / "policy-archive"),
        )
        write_auto_cleanup_policy(policy)
        back = read_auto_cleanup_policy()
        assert back is not None
        assert back.archive_dir == str(state / "policy-archive")

    def test_archive_dir_none_by_default(self) -> None:
        from vq.cleanup import AutoCleanupPolicy
        p = AutoCleanupPolicy(archive_after_seconds=86400)
        assert p.archive_dir is None

    def test_auto_pass_uses_policy_archive_dir(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Policy.archive_dir overrides the default location. Also
        confirms it takes precedence over the env var (the policy
        author wins)."""
        from vq.cleanup import AutoCleanupPolicy, run_auto_cleanup_pass
        custom_policy = state / "policy-override"
        custom_env = state / "env-override"
        monkeypatch.setenv(paths.ENV_ARCHIVE_DIR, str(custom_env))

        old = (datetime.now(UTC) - timedelta(days=10)).isoformat()
        _write_terminal_spec("aaaa00000def", finished_at=old)

        policy = AutoCleanupPolicy(
            archive_after_seconds=7 * 86400,  # 7d threshold
            archive_dir=str(custom_policy),
        )
        counts = run_auto_cleanup_pass(policy)
        assert counts["archived"] == 1
        # Tarball lives where the POLICY said, not where env said.
        assert (custom_policy / "aaaa00000def.tar.bz2").exists()
        assert not custom_env.exists()

    def test_auto_pass_falls_back_to_env_when_policy_none(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Policy.archive_dir is None → env-var override applies."""
        from vq.cleanup import AutoCleanupPolicy, run_auto_cleanup_pass
        custom_env = state / "env-fallback"
        monkeypatch.setenv(paths.ENV_ARCHIVE_DIR, str(custom_env))

        old = (datetime.now(UTC) - timedelta(days=10)).isoformat()
        _write_terminal_spec("aaaa00000eee", finished_at=old)

        policy = AutoCleanupPolicy(
            archive_after_seconds=7 * 86400,
            archive_dir=None,
        )
        run_auto_cleanup_pass(policy)
        assert (custom_env / "aaaa00000eee.tar.bz2").exists()


class TestArchiveDirCLI:
    """End-to-end CLI: ``--archive-dir`` with ``--auto-enable`` stores
    in the policy; ``--archive-dir`` with one-shot ``--archive`` is
    used for that single invocation; ``--auto-status`` reports it."""

    def test_auto_enable_with_archive_dir(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        custom = state / "cli-archive-dir"
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--archive-after", "30d",
             "--archive-dir", str(custom)],
        )
        assert result.exit_code == 0, result.output
        assert f"archive_dir={custom}" in result.output
        policy = cleanup.read_auto_cleanup_policy()
        assert policy is not None
        assert policy.archive_dir == str(custom)

    def test_auto_enable_expands_tilde_in_archive_dir(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``--archive-dir ~/foo`` should expand to an absolute path
        before being persisted (otherwise the daemon would resolve it
        against its own HOME later, which is opaque)."""
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        monkeypatch.setenv("HOME", str(state / "fake-home"))
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--archive-after", "30d",
             "--archive-dir", "~/archives"],
        )
        assert result.exit_code == 0, result.output
        policy = cleanup.read_auto_cleanup_policy()
        assert policy is not None
        assert policy.archive_dir is not None
        # Tilde got expanded
        assert "~" not in policy.archive_dir
        assert policy.archive_dir.endswith("/archives")

    def test_auto_status_reports_archive_dir(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        custom = state / "report-dir"
        CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--archive-after", "30d",
             "--archive-dir", str(custom)],
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-status"])
        assert result.exit_code == 0
        assert f"archive_dir={custom}" in result.output

    def test_one_shot_archive_with_archive_dir(self, state: Path) -> None:
        """``vq cleanup --archive --archive-dir DIR -x`` for a single
        invocation. No policy file written."""
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        old = (datetime.now(UTC) - timedelta(days=40)).isoformat()
        _write_terminal_spec("aaaa00000fff", finished_at=old)
        custom = state / "one-shot-archive"
        result = CliRunner().invoke(
            main,
            ["cleanup", "--archive", "--older-than", "30d",
             "--archive-dir", str(custom), "-x"],
        )
        assert result.exit_code == 0, result.output
        assert (custom / "aaaa00000fff.tar.bz2").exists()
        # No policy file should have been written.
        assert cleanup.read_auto_cleanup_policy() is None


# ----------------------------------------------------------------------
# v0.5.23: per-state retention overrides
# ----------------------------------------------------------------------


class TestParseStateAge:
    def test_basic_state_age(self) -> None:
        from vq.cleanup import parse_state_age
        state, td = parse_state_age("failed:7d")
        assert state == "failed"
        assert td == timedelta(days=7)

    def test_state_with_whitespace(self) -> None:
        from vq.cleanup import parse_state_age
        state, td = parse_state_age("  completed :90d")
        assert state == "completed"
        assert td == timedelta(days=90)

    def test_missing_separator_errors(self) -> None:
        from vq.cleanup import parse_state_age
        with pytest.raises(ValueError, match="no ':' separator"):
            parse_state_age("failed7d")

    def test_unknown_state_errors(self) -> None:
        from vq.cleanup import parse_state_age
        with pytest.raises(ValueError, match="unknown state"):
            parse_state_age("nonsense:7d")

    def test_bad_duration_errors(self) -> None:
        from vq.cleanup import parse_state_age
        with pytest.raises(ValueError, match="cannot parse age"):
            parse_state_age("failed:7foo")


class TestPolicyPerStateFields:
    def test_per_state_fields_default_to_empty_dict(self) -> None:
        from vq.cleanup import AutoCleanupPolicy
        p = AutoCleanupPolicy(archive_after_seconds=86400)
        assert p.archive_after_by_state == {}
        assert p.delete_after_by_state == {}

    def test_per_state_fields_persist_roundtrip(self, state: Path) -> None:
        from vq.cleanup import (
            AutoCleanupPolicy,
            read_auto_cleanup_policy,
            write_auto_cleanup_policy,
        )
        p = AutoCleanupPolicy(
            archive_after_seconds=30 * 86400,
            archive_after_by_state={"failed": 7 * 86400, "completed": 90 * 86400},
            delete_after_by_state={"oom_killed": 14 * 86400},
        )
        write_auto_cleanup_policy(p)
        back = read_auto_cleanup_policy()
        assert back is not None
        assert back.archive_after_by_state == {
            "failed": 7 * 86400,
            "completed": 90 * 86400,
        }
        assert back.delete_after_by_state == {"oom_killed": 14 * 86400}


class TestCutoffResolution:
    """``_cutoff_for(policy, kind, state)`` is the dispatcher:
    per-state value wins; global is fallback; both unset = None."""

    def test_per_state_wins(self) -> None:
        from vq.cleanup import AutoCleanupPolicy, _cutoff_for
        p = AutoCleanupPolicy(
            archive_after_seconds=30 * 86400,
            archive_after_by_state={"failed": 7 * 86400},
        )
        assert _cutoff_for(p, "archive", "failed") == timedelta(days=7)
        # Other states fall back to global
        assert _cutoff_for(p, "archive", "completed") == timedelta(days=30)

    def test_no_per_state_uses_global(self) -> None:
        from vq.cleanup import AutoCleanupPolicy, _cutoff_for
        p = AutoCleanupPolicy(archive_after_seconds=30 * 86400)
        assert _cutoff_for(p, "archive", "completed") == timedelta(days=30)

    def test_both_unset_returns_none(self) -> None:
        from vq.cleanup import AutoCleanupPolicy, _cutoff_for
        p = AutoCleanupPolicy()
        assert _cutoff_for(p, "archive", "completed") is None
        assert _cutoff_for(p, "delete", "failed") is None

    def test_only_per_state_set_other_states_skipped(self) -> None:
        """If only per-state is set (no global), states not listed are
        intentionally skipped — the user is being targeted."""
        from vq.cleanup import AutoCleanupPolicy, _cutoff_for
        p = AutoCleanupPolicy(
            archive_after_by_state={"failed": 7 * 86400},
        )
        assert _cutoff_for(p, "archive", "failed") == timedelta(days=7)
        assert _cutoff_for(p, "archive", "completed") is None


class TestPerStateAutoPass:
    """``run_auto_cleanup_pass`` applies per-state cutoffs correctly."""

    def test_only_listed_state_archived_when_per_state_only(
        self, state: Path
    ) -> None:
        """Policy with archive_after_by_state={failed: 7d} but no
        global: only failed jobs get archived; completed jobs left
        alone even when older than 7d."""
        from vq.cleanup import AutoCleanupPolicy, run_auto_cleanup_pass
        old = (datetime.now(UTC) - timedelta(days=14)).isoformat()
        _write_terminal_spec(
            "perstate0001", state_val=JobState.FAILED, finished_at=old,
        )
        _write_terminal_spec(
            "perstate0002", state_val=JobState.COMPLETED, finished_at=old,
        )
        policy = AutoCleanupPolicy(
            archive_after_by_state={"failed": 7 * 86400},
        )
        counts = run_auto_cleanup_pass(policy)
        assert counts["archived"] == 1
        # Verify which one survived
        from vq.spec import JobSpec
        failed_spec = JobSpec.read(paths.spec_path("perstate0001"))
        completed_spec = JobSpec.read(paths.spec_path("perstate0002"))
        assert failed_spec.is_archived
        assert not completed_spec.is_archived

    def test_per_state_wins_over_global(self, state: Path) -> None:
        """Global archive_after=30d, but failed-state override=7d.
        A 14-day-old failed job should be archived; a 14-day-old
        completed job should NOT (within the 30d global window)."""
        from vq.cleanup import AutoCleanupPolicy, run_auto_cleanup_pass
        old = (datetime.now(UTC) - timedelta(days=14)).isoformat()
        _write_terminal_spec(
            "winperst0001", state_val=JobState.FAILED, finished_at=old,
        )
        _write_terminal_spec(
            "winperst0002", state_val=JobState.COMPLETED, finished_at=old,
        )
        policy = AutoCleanupPolicy(
            archive_after_seconds=30 * 86400,
            archive_after_by_state={"failed": 7 * 86400},
        )
        counts = run_auto_cleanup_pass(policy)
        assert counts["archived"] == 1

        from vq.spec import JobSpec
        assert JobSpec.read(paths.spec_path("winperst0001")).is_archived
        assert not JobSpec.read(paths.spec_path("winperst0002")).is_archived

    def test_per_state_keeps_failed_longer_than_completed(
        self, state: Path
    ) -> None:
        """Common use case: keep failed jobs around longer for
        forensics. global archive_after=7d, failed:30d.
        A 10-day-old completed job should be archived; a 10-day-old
        failed job should NOT (within the 30d failed window)."""
        from vq.cleanup import AutoCleanupPolicy, run_auto_cleanup_pass
        old = (datetime.now(UTC) - timedelta(days=10)).isoformat()
        _write_terminal_spec(
            "longfail0001", state_val=JobState.FAILED, finished_at=old,
        )
        _write_terminal_spec(
            "longfail0002", state_val=JobState.COMPLETED, finished_at=old,
        )
        policy = AutoCleanupPolicy(
            archive_after_seconds=7 * 86400,
            archive_after_by_state={"failed": 30 * 86400},
        )
        counts = run_auto_cleanup_pass(policy)
        assert counts["archived"] == 1

        from vq.spec import JobSpec
        assert not JobSpec.read(paths.spec_path("longfail0001")).is_archived
        assert JobSpec.read(paths.spec_path("longfail0002")).is_archived


class TestPerStateCLI:
    def test_cli_archive_after_state_persisted(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable",
             "--archive-after", "30d",
             "--archive-after-state", "failed:7d",
             "--archive-after-state", "completed:90d"],
        )
        assert result.exit_code == 0, result.output
        policy = cleanup.read_auto_cleanup_policy()
        assert policy is not None
        assert policy.archive_after_by_state == {
            "failed": 7 * 86400,
            "completed": 90 * 86400,
        }

    def test_cli_unknown_state_errors(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable",
             "--archive-after-state", "garbage:7d"],
        )
        assert result.exit_code != 0
        assert "unknown state" in result.output

    def test_cli_per_state_alone_satisfies_auto_enable_requirement(
        self, state: Path
    ) -> None:
        """Just --archive-after-state failed:7d is enough — caller
        doesn't need to also set the global --archive-after."""
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable",
             "--archive-after-state", "failed:7d"],
        )
        assert result.exit_code == 0, result.output
        policy = cleanup.read_auto_cleanup_policy()
        assert policy is not None
        assert policy.archive_after_seconds is None
        assert policy.archive_after_by_state == {"failed": 7 * 86400}

    def test_cli_workdir_max_age_persisted(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--workdir-max-age", "14d"],
        )
        assert result.exit_code == 0, result.output
        assert "workdir_max_age=1209600s" in result.output
        policy = cleanup.read_auto_cleanup_policy()
        assert policy is not None
        assert policy.archive_after_seconds is None
        assert policy.delete_after_seconds is None
        assert policy.workdir_max_age_seconds == 14 * 86400

    def test_cli_workdir_max_age_invalid_duration_errors(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        result = CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable", "--workdir-max-age", "two-weeks"],
        )
        assert result.exit_code != 0
        assert "cannot parse age" in result.output

    def test_cli_workdir_max_age_forwards_to_remote(
        self, state: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "host_a"\n'
            '[hosts.host_a]\n'
            'ssh = "host_a"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, tuple[str, ...]]] = []

        def fake_delegate(host: str, _cfg: object, *args: str) -> str:
            captured.append((host, args))
            return "remote ok\n"

        monkeypatch.setattr("vq.cli._delegate_to_remote", fake_delegate)
        result = CliRunner().invoke(
            main,
            [
                "cleanup",
                "host_a",
                "--auto-enable",
                "--archive-after",
                "30d",
                "--workdir-max-age",
                "14d",
            ],
        )
        assert result.exit_code == 0, result.output
        assert result.output == "remote ok\n"
        assert captured == [
            (
                "host_a",
                (
                    "cleanup",
                    "localhost",
                    "--auto-enable",
                    "--archive-after",
                    "30d",
                    "--workdir-max-age",
                    "14d",
                ),
            )
        ]

    def test_cli_auto_status_reports_per_state(self, state: Path) -> None:
        (state / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n[hosts.localhost]\nssh = "localhost"\n'
        )
        CliRunner().invoke(
            main,
            ["cleanup", "--auto-enable",
             "--archive-after-state", "failed:7d",
             "--delete-after-state", "completed:90d"],
        )
        result = CliRunner().invoke(main, ["cleanup", "--auto-status"])
        assert result.exit_code == 0
        assert "archive_after[failed]" in result.output
        assert "delete_after[completed]" in result.output
