"""Tests for v0.6.x multi-user mode: paths, config, ownership, quotas, auth."""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

from vq import config, ownership, paths
from vq.spec import JobSpec, JobState


def _resolve_production_default(expression: str, tmp_path: Path) -> Path:
    """Resolve a documented system default outside the pytest boundary."""
    env = dict(os.environ)
    for name in (
        paths.ENV_MULTI_USER_ROOT,
        paths.ENV_TEST_SANDBOX_ROOT,
        "PYTEST_ADDOPTS",
        "PYTEST_CURRENT_TEST",
        "PYTEST_VERSION",
    ):
        env.pop(name, None)
    home = tmp_path / "default-home"
    home.mkdir(exist_ok=True)
    env["HOME"] = str(home)
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [sys.executable, "-c", f"from vq import paths; print({expression})"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return Path(result.stdout.strip())


# ---------------------------------------------------------------------------
# paths — multi-user
# ---------------------------------------------------------------------------


class TestMultiUserPaths:
    def test_multi_user_root_default(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.multi_user_root()", tmp_path
        ) == Path("/var/lib/vq")

    def test_multi_user_root_env_override(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        custom = tmp_path / "custom-vq"
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(custom))
        assert paths.multi_user_root() == custom

    def test_users_root(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.users_root()", tmp_path
        ) == Path("/var/lib/vq/users")

    def test_user_dir(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_dir(1000)", tmp_path
        ) == Path("/var/lib/vq/users/1000")

    def test_user_queue_dir(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_queue_dir('1000')", tmp_path
        ) == Path("/var/lib/vq/users/1000/queue")

    def test_user_jobs_dir(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_jobs_dir(1000)", tmp_path
        ) == Path("/var/lib/vq/users/1000/jobs")

    def test_user_archive_dir(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_archive_dir(1000)", tmp_path
        ) == Path("/var/lib/vq/users/1000/archive")

    def test_user_spec_path(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_spec_path(1000, 'abc123')", tmp_path
        ) == Path(
            "/var/lib/vq/users/1000/queue/abc123.json"
        )

    def test_user_workspace_dir(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.user_workspace_dir(1000, 'abc123')", tmp_path
        ) == Path(
            "/var/lib/vq/users/1000/jobs/abc123"
        )

    def test_daemon_pidfile_multi_user(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.daemon_pidfile(multi_user=True)", tmp_path
        ) == Path("/var/lib/vq/daemon.pid")

    def test_daemon_pidfile_single_user(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        state = tmp_path / "state"
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        assert paths.daemon_pidfile(multi_user=False) == state / "daemon.pid"

    def test_daemon_logfile_multi_user(self, tmp_path: Path) -> None:
        assert _resolve_production_default(
            "paths.daemon_logfile(multi_user=True)", tmp_path
        ) == Path("/var/lib/vq/daemon.log")

    def test_all_user_dirs_empty(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setenv(
            paths.ENV_MULTI_USER_ROOT, str(tmp_path / "nonexistent-vq")
        )
        assert paths._all_user_dirs() == []

    def test_all_user_dirs_populated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        users = tmp_path / "users"
        (users / "1000" / "queue").mkdir(parents=True)
        (users / "1001" / "queue").mkdir(parents=True)
        # Non-digit dirs are ignored.
        (users / "lost+found").mkdir()
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))
        dirs = paths._all_user_dirs()
        assert len(dirs) == 2
        assert {d.name for d in dirs} == {"1000", "1001"}

    @pytest.mark.parametrize("name", ["001000", "١٠٠٠"])
    def test_all_user_dirs_rejects_noncanonical_numeric_spelling(
        self,
        name: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        (tmp_path / "users" / name).mkdir(parents=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))

        with pytest.raises(
            paths.UnsafeMultiUserStateError,
            match="canonical ASCII decimal spelling",
        ):
            paths._all_user_dirs()

    @pytest.mark.parametrize("entry_kind", ["file", "symlink"])
    def test_all_user_dirs_rejects_non_directory_canonical_uid_entry(
        self,
        entry_kind: str,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        users = tmp_path / "users"
        users.mkdir()
        entry = users / "1000"
        if entry_kind == "file":
            entry.write_text("unsafe", encoding="utf-8")
        else:
            outside = tmp_path / "outside"
            outside.mkdir()
            entry.symlink_to(outside, target_is_directory=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))

        with pytest.raises(paths.UnsafeMultiUserStateError, match="real directory"):
            paths._all_user_dirs()

    def test_all_user_dirs_rejects_world_writable_state_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "users").mkdir()
        tmp_path.chmod(0o777)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))

        with pytest.raises(paths.UnsafeMultiUserStateError, match="world writable"):
            paths._all_user_dirs()

        tmp_path.chmod(stat.S_IMODE(tmp_path.stat().st_mode) & ~0o022)

    def test_all_user_dirs_accepts_trusted_group_writable_state_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "users" / "1000").mkdir(parents=True)
        tmp_path.chmod(0o2775)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))

        assert [entry.name for entry in paths._all_user_dirs()] == ["1000"]

    def test_resolve_spec_path_single_user(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        state = tmp_path / "state"
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        assert paths.resolve_spec_path(
            "abc", multi_user=False
        ) == state / "queue" / "abc.json"

    def test_resolve_spec_path_multi_user_known_uid(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "users" / "1000" / "queue").mkdir(parents=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))
        expected = tmp_path / "users" / "1000" / "queue" / "abc.json"
        assert paths.resolve_spec_path("abc", multi_user=True, uid="1000") == expected

    def test_resolve_spec_path_multi_user_not_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "users").mkdir(parents=True)
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))
        with pytest.raises(FileNotFoundError, match="no such job"):
            paths.resolve_spec_path("nonexistent", multi_user=True)

    def test_resolve_spec_path_multi_user_search(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "users" / "1001" / "queue").mkdir(parents=True)
        spec = JobSpec(id="xyz", command=["echo", "hi"], cwd=str(tmp_path), cpus=1)
        spec.write(tmp_path / "users" / "1001" / "queue" / "xyz.json")
        monkeypatch.setenv(paths.ENV_MULTI_USER_ROOT, str(tmp_path))
        found = paths.resolve_spec_path("xyz", multi_user=True)
        assert found == tmp_path / "users" / "1001" / "queue" / "xyz.json"


# ---------------------------------------------------------------------------
# config — multi-user + quotas
# ---------------------------------------------------------------------------


class TestMultiUserConfig:
    def test_defaults(self) -> None:
        cfg = config.MultiUserConfig()
        assert cfg.enabled is False
        assert cfg.admin_group == "vq-admins"

    def test_parsed(self) -> None:
        cfg = config.MultiUserConfig.model_validate({"enabled": True, "admin_group": "ops"})
        assert cfg.enabled is True
        assert cfg.admin_group == "ops"


class TestQuotaConfig:
    def test_defaults(self) -> None:
        cfg = config.QuotaConfig()
        assert cfg.default_max_pending_jobs is None
        assert cfg.default_max_concurrent_cpus is None
        assert cfg.users == {}

    def test_effective_defaults(self) -> None:
        cfg = config.QuotaConfig(
            default_max_pending_jobs=10,
            default_max_concurrent_cpus=8,
        )
        assert cfg.effective_max_pending_jobs(1000) == 10
        assert cfg.effective_max_concurrent_cpus(1000) == 8

    def test_per_user_override(self) -> None:
        cfg = config.QuotaConfig(
            default_max_pending_jobs=10,
            users={
                "1000": config.PerUserQuotaConfig(max_pending_jobs=30),
            },
        )
        assert cfg.effective_max_pending_jobs(1000) == 30
        assert cfg.effective_max_pending_jobs(1001) == 10
        # CPU falls back to default (None).
        assert cfg.effective_max_concurrent_cpus(1000) is None

    def test_per_user_full_override(self) -> None:
        cfg = config.QuotaConfig(
            default_max_pending_jobs=10,
            default_max_concurrent_cpus=8,
            users={
                "1001": config.PerUserQuotaConfig(
                    max_pending_jobs=5,
                    max_concurrent_cpus=2,
                ),
            },
        )
        assert cfg.effective_max_pending_jobs(1001) == 5
        assert cfg.effective_max_concurrent_cpus(1001) == 2

    def test_uid_is_string_key(self) -> None:
        cfg = config.QuotaConfig(
            users={
                "1000": config.PerUserQuotaConfig(max_pending_jobs=7),
            },
        )
        # Both int and str uid work.
        assert cfg.effective_max_pending_jobs(1000) == 7
        assert cfg.effective_max_pending_jobs("1000") == 7

    @pytest.mark.parametrize(
        ("model", "field"),
        [
            (config.QuotaConfig, "default_max_pending_jobs"),
            (config.QuotaConfig, "default_max_concurrent_cpus"),
            (config.PerUserQuotaConfig, "max_pending_jobs"),
            (config.PerUserQuotaConfig, "max_concurrent_cpus"),
        ],
    )
    @pytest.mark.parametrize("value", [-1, True, "2", 2.0])
    def test_quota_caps_reject_invalid_values(
        self,
        model: type[config.QuotaConfig] | type[config.PerUserQuotaConfig],
        field: str,
        value: object,
    ) -> None:
        with pytest.raises(ValueError):
            model.model_validate({field: value})

    @pytest.mark.parametrize(
        ("model", "field"),
        [
            (config.QuotaConfig, "default_max_pending_jobs"),
            (config.QuotaConfig, "default_max_concurrent_cpus"),
            (config.PerUserQuotaConfig, "max_pending_jobs"),
            (config.PerUserQuotaConfig, "max_concurrent_cpus"),
        ],
    )
    @pytest.mark.parametrize("value", [None, 0, 1, 32])
    def test_quota_caps_accept_nonnegative_strict_integers(
        self,
        model: type[config.QuotaConfig] | type[config.PerUserQuotaConfig],
        field: str,
        value: int | None,
    ) -> None:
        parsed = model.model_validate({field: value})
        assert getattr(parsed, field) == value


class TestConfigIntegration:
    def test_multi_user_field_on_config(self) -> None:
        cfg = config.Config()
        assert cfg.multi_user.enabled is False
        assert cfg.quotas.default_max_pending_jobs is None

    def test_invalid_quota_cap_fails_config_load(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text(
            "[quotas]\n"
            "default_max_pending_jobs = -1\n"
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))

        with pytest.raises(config.ConfigError, match="default_max_pending_jobs"):
            config.load_config()


# ---------------------------------------------------------------------------
# ownership checks
# ---------------------------------------------------------------------------


class TestOwnership:
    def test_check_owner_noop_single_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """In single-user mode (multi_user.enabled=False), check_owner is a no-op."""
        cfg = config.Config()
        assert cfg.multi_user.enabled is False
        spec = JobSpec(id="abc", command=["echo"], cwd="/tmp", cpus=1)
        # Should not raise.
        ownership.check_owner(spec, cfg=cfg)

    def test_check_owner_no_submitter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Jobs with no submitter field pass ownership check."""
        cfg = config.MultiUserConfig(enabled=True)
        full_cfg = config.Config(multi_user=cfg)
        spec = JobSpec(id="abc", command=["echo"], cwd="/tmp", cpus=1, submitter=None)
        ownership.check_owner(spec, cfg=full_cfg)

    def test_check_owner_caller_is_root(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """root (euid=0) always passes ownership check."""
        if os.geteuid() != 0:
            pytest.skip("test requires root")
        cfg = config.MultiUserConfig(enabled=True)
        full_cfg = config.Config(multi_user=cfg)
        spec = JobSpec(id="abc", command=["echo"], cwd="/tmp", cpus=1, submitter="1000")
        ownership.check_owner(spec, cfg=full_cfg)

    def test_ownership_error_message(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-owner, non-admin caller gets OwnershipError."""
        cfg = config.MultiUserConfig(enabled=True, admin_group="nonexistent-group-xyz")
        full_cfg = config.Config(multi_user=cfg)
        spec = JobSpec(id="abc", command=["echo"], cwd="/tmp", cpus=1, submitter="99999")
        # We're not uid 99999, so this should raise.
        if os.geteuid() == 99999:
            pytest.skip("test runner is uid 99999")
        with pytest.raises(ownership.OwnershipError, match="belongs to uid 99999"):
            ownership.check_owner(spec, cfg=full_cfg)

    def test_check_spec_path_owner_noop_single_user(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = config.Config()
        spec = JobSpec(id="abc", command=["echo"], cwd=str(tmp_path), cpus=1)
        spec_path = tmp_path / "abc.json"
        spec.write(spec_path)
        # Should not raise in single-user mode.
        ownership.check_spec_path_owner(spec_path, cfg=cfg)


class TestSystemOwnershipPolicy:
    @staticmethod
    def _write_foreign_spec(tmp_path: Path) -> Path:
        spec_path = tmp_path / "foreign.json"
        JobSpec(
            id="foreign",
            command=["true"],
            cwd=str(tmp_path),
            cpus=1,
            submitter="2002",
        ).write(spec_path)
        return spec_path

    def test_system_mode_gates_when_personal_config_is_single_user(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        personal_dir = tmp_path / "personal"
        personal_dir.mkdir()
        (personal_dir / "config.toml").write_text(
            "[multi_user]\n"
            "enabled = false\n"
            'admin_group = "personal-admins"\n'
        )
        system_path = tmp_path / "system.toml"
        system_path.write_text(
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "system-admins"\n'
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(personal_dir))
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)

        with pytest.raises(
            ownership.OwnershipError, match="system-admins"
        ):
            ownership.check_spec_path_owner(
                self._write_foreign_spec(tmp_path), multi_user=True
            )

    def test_enabled_system_admin_group_overrides_personal_group(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        personal = config.Config(
            multi_user=config.MultiUserConfig(
                enabled=True, admin_group="personal-admins"
            )
        )
        system_path = tmp_path / "system.toml"
        system_path.write_text(
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "system-admins"\n'
        )
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(
            ownership,
            "_caller_is_admin",
            lambda cfg: cfg.multi_user.admin_group == "personal-admins",
        )

        with pytest.raises(
            ownership.OwnershipError, match="system-admins"
        ):
            ownership.check_owner(
                JobSpec(
                    id="foreign",
                    command=["true"],
                    cwd=str(tmp_path),
                    cpus=1,
                    submitter="2002",
                ),
                cfg=personal,
                multi_user=True,
            )

    def test_invalid_present_system_config_fails_closed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        system_path = tmp_path / "system.toml"
        system_path.write_text(
            'hosts = "not-a-table"\n'
            "[multi_user]\n"
            "enabled = true\n"
        )
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)
        personal = config.Config(
            multi_user=config.MultiUserConfig(enabled=True)
        )

        with pytest.raises(config.ConfigError, match="invalid config"):
            ownership.check_owner(
                JobSpec(
                    id="owned",
                    command=["true"],
                    cwd=str(tmp_path),
                    cpus=1,
                    submitter=str(os.geteuid()),
                ),
                cfg=personal,
                multi_user=True,
            )

    def test_single_user_ignores_malformed_system_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        system_path = tmp_path / "system.toml"
        system_path.write_text("this is [not valid toml")
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)

        ownership.check_owner(
            JobSpec(
                id="single-user",
                command=["true"],
                cwd=str(tmp_path),
                cpus=1,
                submitter="2002",
            ),
            cfg=config.Config(),
        )

    def test_enabled_system_policy_precedes_malformed_personal_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        personal_dir = tmp_path / "personal"
        personal_dir.mkdir()
        (personal_dir / "config.toml").write_text("this is [not valid toml")
        system_path = tmp_path / "system.toml"
        system_path.write_text(
            "[multi_user]\n"
            "enabled = true\n"
            'admin_group = "system-admins"\n'
        )
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(personal_dir))
        monkeypatch.setattr(config, "SYSTEM_CONFIG_PATH", system_path)
        monkeypatch.setattr(ownership, "_caller_uid", lambda: 1001)
        monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)

        ownership.check_owner(
            JobSpec(
                id="owned",
                command=["true"],
                cwd=str(tmp_path),
                cpus=1,
                submitter="1001",
            ),
            multi_user=True,
        )


# ---------------------------------------------------------------------------
# auth — admin token verification
# ---------------------------------------------------------------------------


class TestAdminTokenAuth:
    def test_resolve_token_cli_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VQ_TOKEN", "env-token")
        from vq import auth

        assert auth.resolve_token("cli-token") == "cli-token"

    def test_resolve_token_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VQ_TOKEN", "env-token")
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        from vq import auth

        # Re-apply after delenv.
        monkeypatch.setenv("VQ_TOKEN", "env-token")
        assert auth.resolve_token() == "env-token"

    def test_resolve_token_neither(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("VQ_TOKEN", raising=False)
        from vq import auth

        assert auth.resolve_token() is None

    def test_verify_admin_token_no_token_file_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.6.44 (security): when no token file exists, verification
        must FAIL. Pre-v0.6.44 returned True (the single-user-compat
        branch), which silently bypassed the multi-user admin gate
        on any host that lacked a token file."""
        monkeypatch.setenv("VQ_WEB_TOKEN_FILE", str(tmp_path / "nonexistent"))
        from vq import auth

        assert auth.verify_admin_token("any-token") is False

    def test_verify_admin_token_mismatch(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When a token file exists, wrong tokens fail."""
        token_file = tmp_path / "token"
        token_file.write_text("correct-token\n")
        token_file.chmod(0o600)
        monkeypatch.setenv("VQ_WEB_TOKEN_FILE", str(token_file))
        from vq import auth

        assert auth.verify_admin_token("correct-token") is True
        assert auth.verify_admin_token("wrong-token") is False


class TestResubmitOwnershipISO2:
    """ISO-2 (v0.8.25): `vq resubmit` must not let user A re-run user B's
    job (which would run AS B, since resubmit carries the submitter)."""

    def test_resubmit_denies_another_users_job(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from vq import resubmit

        if os.geteuid() == 99999:
            pytest.skip("test runner is uid 99999")
        full_cfg = config.Config(
            multi_user=config.MultiUserConfig(
                enabled=True, admin_group="nonexistent-group-xyz",
            )
        )
        monkeypatch.setattr(config, "load_config", lambda: full_cfg)

        queue = tmp_path / "queue"
        queue.mkdir(parents=True)
        jobs = tmp_path / "jobs"
        jobs.mkdir(parents=True)
        JobSpec(
            id="srcjob000001", command=["echo"], cwd=str(jobs), cpus=1,
            state=JobState.COMPLETED, finished_at="2026-01-01T00:00:00+00:00",
            submitter="99999",  # owned by a different user
        ).write(queue / "srcjob000001.json")

        with pytest.raises(ownership.OwnershipError):
            resubmit.resubmit_local(
                "srcjob000001", queue_dir=queue, jobs_dir=jobs,
            )

    def test_resubmit_single_user_ignores_submitter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In single-user mode the ownership check is a no-op, so resubmit
        works regardless of the (vestigial) submitter field."""
        from vq import resubmit

        monkeypatch.setattr(
            config, "load_config",
            lambda: config.Config(multi_user=config.MultiUserConfig(enabled=False)),
        )
        queue = tmp_path / "queue"
        queue.mkdir(parents=True)
        jobs = tmp_path / "jobs"
        ws = jobs / "srcjob000002"
        ws.mkdir(parents=True)
        JobSpec(
            id="srcjob000002", command=["echo"], cwd=str(ws), cpus=1,
            state=JobState.COMPLETED, finished_at="2026-01-01T00:00:00+00:00",
            submitter="99999",
        ).write(queue / "srcjob000002.json")

        new_id = resubmit.resubmit_local(
            "srcjob000002", queue_dir=queue, jobs_dir=jobs,
        )
        assert new_id and new_id != "srcjob000002"


class TestReadPathsAreOwnershipGated:
    """ISO-1, first half: a job's output is its owner's.

    `vq kill` has gated on ownership since v0.6.x. The READ paths never did, so
    on a multi-user host any local user could inspect another user's job -- and
    read its stdout/stderr -- through vq itself.

    This half does NOT close the hole alone: the per-user state directories are
    still world-readable, so a determined user reads the spec or log directly
    and bypasses vq. The directory modes are the other half, and because they
    change permissions on live multi-user hosts they need validating on a real
    host before landing.
    """

    def _foreign_spec(self, tmp_path: Path) -> Path:
        queue = tmp_path / "queue"
        queue.mkdir(parents=True, exist_ok=True)
        ws = tmp_path / "ws"
        ws.mkdir(exist_ok=True)
        path = queue / "foreign.json"
        JobSpec(
            id="foreign",
            command=["true"],
            cwd=str(ws),
            cpus=1,
            submitter="99999",
        ).write(path)
        return path

    def _multi_user(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cfg = config.Config(
            multi_user=config.MultiUserConfig(
                enabled=True, admin_group="nonexistent-group-xyz"
            )
        )
        monkeypatch.setattr(ownership.config_module, "load_config", lambda: cfg)

    def test_status_refuses_another_users_job(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if os.geteuid() == 0:
            pytest.skip("root passes every ownership check by design")
        from vq import status as status_mod

        spec_path = self._foreign_spec(tmp_path)
        self._multi_user(monkeypatch)

        with pytest.raises(ownership.OwnershipError):
            status_mod.show_status(
                "localhost", "foreign", queue_dir=spec_path.parent
            )

    def test_logs_refuses_another_users_job(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        if os.geteuid() == 0:
            pytest.skip("root passes every ownership check by design")
        from vq import logs as logs_mod

        spec_path = self._foreign_spec(tmp_path)
        self._multi_user(monkeypatch)
        monkeypatch.setattr(logs_mod.paths, "queue_dir", lambda: spec_path.parent)

        with pytest.raises(ownership.OwnershipError):
            logs_mod._resolve_spec("foreign", multi_user=False)

    def test_single_user_mode_is_unaffected(self, tmp_path: Path) -> None:
        """Inert wherever multi-user is off -- which is every host today, so
        this lands without changing any current behaviour."""
        from vq import status as status_mod

        spec_path = self._foreign_spec(tmp_path)

        out = status_mod.show_status(
            "localhost", "foreign", queue_dir=spec_path.parent
        )

        assert "foreign" in out


class TestProvisioningGrantsAdminGroupTheStateRoot:
    """A freshly provisioned multi-user host must be able to run admin ops.

    `sudo mkdir -p /var/lib/vq` leaves the state root root:root 0755. The
    admin-update marker is written directly under it, so every `vq admin`
    operation on a brand-new host failed at marker acquisition -- with the host
    otherwise fully provisioned and the daemon running, which reads as a vq bug
    rather than a missing permission.

    Grepping the script is the honest test here: it is shipped shell that the
    suite cannot execute (it sudo-installs system units), so this guards against
    the line being dropped in a future edit rather than proving the modes on a
    real host.
    """

    def _script(self) -> str:
        path = Path(__file__).parents[1] / "contrib" / "deploy-multi-user.sh"
        return path.read_text(encoding="utf-8")

    def test_state_root_is_chgrped_to_the_admin_group(self) -> None:
        assert 'chgrp "$ADMIN_GROUP" "$STATE_DIR"' in self._script()

    def test_state_root_is_group_writable_and_setgid(self) -> None:
        """setgid so files created there inherit the group, not the creator's
        primary group."""
        assert 'chmod 2775 "$STATE_DIR"' in self._script()

    def test_the_grant_happens_after_the_group_exists(self) -> None:
        """groupadd must precede the chgrp, or it fails on a fresh host."""
        script = self._script()
        assert script.index('groupadd -f "$ADMIN_GROUP"') < script.index(
            'chgrp "$ADMIN_GROUP" "$STATE_DIR"'
        )
