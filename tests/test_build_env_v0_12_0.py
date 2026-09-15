"""Tests for v0.12.0 build-as-job foundations: the ``JobSpec.build_env``
marker field and the internal ``vq build-env`` subcommand the daemon's build
job runs. The dispatch integration (build-job creation, exclusivity, dedup)
is tested separately once it lands.

``build_env`` marks a spec AS the build job for a venv env. The daemon runs
such a job through the normal cgroup-wrapped, full-host, exclusive job path
to rebuild the env, instead of the v0.11.0 uncapped inline rebuild. The
``build-env`` command is the job-command form of ``admin.update_env(<env>)``:
exit 0 on success, 1 on a failed build, 2 if it could not start.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from vq import admin
from vq.cli import main
from vq.spec import JobSpec

# ----------------------------------------------------------------------
# JobSpec.build_env field
# ----------------------------------------------------------------------


class TestBuildEnvSpecField:
    def test_default_is_none(self) -> None:
        spec = JobSpec(id="a" * 12, command=["true"], cwd="/tmp", cpus=1)
        assert spec.build_env is None

    def test_accepts_env_name(self) -> None:
        spec = JobSpec(
            id="b" * 12, command=["true"], cwd="/tmp", cpus=1,
            build_env="vibeqc-dev",
        )
        assert spec.build_env == "vibeqc-dev"

    def test_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(
            id="c" * 12, command=["true"], cwd="/tmp", cpus=1,
            build_env="vibeqc-dev",
        )
        path = tmp_path / "spec.json"
        spec.write(path)
        assert JobSpec.read(path).build_env == "vibeqc-dev"

    def test_none_roundtrips_through_disk(self, tmp_path: Path) -> None:
        spec = JobSpec(id="d" * 12, command=["true"], cwd="/tmp", cpus=1)
        path = tmp_path / "spec.json"
        spec.write(path)
        assert JobSpec.read(path).build_env is None

    def test_old_spec_without_build_env_reads_clean(
        self, tmp_path: Path
    ) -> None:
        """Additive field: a spec JSON written before v0.12.0 (no
        'build_env' key) reads into the current model with build_env
        defaulting to None, no SPEC_VERSION bump."""
        old_json = {
            "spec_version": 2,
            "id": "e" * 12,
            "command": ["true"],
            "cwd": "/tmp/e",
            "cpus": 1,
            "state": "pending",
            "submitted_at": "2026-06-10T12:00:00+00:00",
            # no "build_env" key: simulates a pre-v0.12.0 spec
        }
        path = tmp_path / "old.json"
        path.write_text(json.dumps(old_json))
        spec = JobSpec.read(path)
        assert spec.build_env is None
        assert spec.id == "e" * 12

    def test_build_env_and_refresh_before_are_independent(self) -> None:
        """The build job carries build_env. The --refresh jobs that wait on
        it carry refresh_before. The model must read either marker cleanly."""
        builder = JobSpec(
            id="f" * 12, command=["true"], cwd="/tmp", cpus=1,
            build_env="vibeqc-dev",
        )
        waiter = JobSpec(
            id="g" * 12, command=["true"], cwd="/tmp", cpus=1,
            refresh_before="vibeqc-dev",
        )
        assert builder.build_env == "vibeqc-dev"
        assert builder.refresh_before is None
        assert waiter.refresh_before == "vibeqc-dev"
        assert waiter.build_env is None


# ----------------------------------------------------------------------
# `vq build-env` internal subcommand
# ----------------------------------------------------------------------


class _FakeResult:
    def __init__(self, success: bool) -> None:
        self.success = success


class TestBuildEnvCommand:
    _BASELINE = "a" * 40
    _TARGET = "b" * 40

    def _patch(self, monkeypatch: pytest.MonkeyPatch, *, update_env) -> None:
        # The command loads config then calls admin.update_env. Stub both:
        # config so no real /etc/vq is needed, update_env to the behaviour
        # under test. cli references admin as admin_module, i.e. vq.admin.
        monkeypatch.setattr("vq.config.load_config", lambda: object())
        monkeypatch.setattr("vq.admin.update_env", update_env)
        monkeypatch.setattr(
            "vq.admin._resolve_venv_program",
            lambda _env, _cfg: SimpleNamespace(
                branch="main",
                git_dir="/fake/repo",
            ),
        )
        monkeypatch.setattr(
            "vq.admin.current_source_sha",
            lambda _repo: self._BASELINE,
        )
        monkeypatch.setattr(
            "vq.auto_update._fetch_origin",
            lambda _repo: (0, "fetched"),
        )
        monkeypatch.setattr(
            "vq.auto_update._rev_parse",
            lambda _repo, _ref: self._TARGET,
        )
        monkeypatch.setattr(
            "vq.auto_update._is_ancestor",
            lambda _repo, _old, _new: True,
        )

    def _argv(self, env: str) -> list[str]:
        return [
            "build-env",
            env,
            "--baseline-sha",
            self._BASELINE,
            "--expected-sha",
            self._TARGET,
        ]

    def test_success_exits_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self._patch(
            monkeypatch, update_env=lambda *a, **k: _FakeResult(True),
        )
        res = CliRunner().invoke(main, self._argv("vibeqc-dev"))
        assert res.exit_code == 0, res.output
        assert "rebuilt OK" in res.output

    def test_failed_build_exits_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self._patch(
            monkeypatch, update_env=lambda *a, **k: _FakeResult(False),
        )
        res = CliRunner().invoke(main, self._argv("vibeqc-dev"))
        assert res.exit_code == 1, res.output

    def test_admin_error_exits_two(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _raise(*a, **k):
            raise admin.AdminError("no such env")

        self._patch(monkeypatch, update_env=_raise)
        res = CliRunner().invoke(main, self._argv("bogus-env"))
        assert res.exit_code == 2, res.output

    def test_targets_localhost_without_restart(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The build job must rebuild THIS host and never bounce the daemon
        mid-dispatch (restart_daemon=False), matching the inline --refresh
        call it replaces."""
        seen: dict[str, object] = {}

        def _capture(env, cfg, **k):
            seen["env"] = env
            seen["host"] = k.get("host")
            seen["restart_daemon"] = k.get("restart_daemon")
            return _FakeResult(True)

        self._patch(monkeypatch, update_env=_capture)
        res = CliRunner().invoke(main, self._argv("vibeqc-dev"))
        assert res.exit_code == 0, res.output
        assert seen == {
            "env": "vibeqc-dev",
            "host": "localhost",
            "restart_daemon": False,
        }
