"""v0.7.1 *Lamport's Clock* — Item 5: dirty-tree LAST OK policy.

Pins the opt-in ``VenvProgram.fail_on_dirty`` config knob: when
True, ``vq admin update`` flips LAST OK=False on a post-update
dirty tree. Default False preserves pre-v0.7.1 behavior for dev
clones where dirty is normal (basissetdev artifacts, in-flight
experimental edits).

The host_a/host_d 2026-05-25 incident showed why: vibeqc-queue and
vibeqc-release SHOULD always be clean — a dirty tree on those
envs is typically an aborted deploy. vibeqc-dev tolerates dirty.
This knob lets each env make its own contract explicit.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 5.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config, paths


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_venv_program(
    git_dir: Path, *, branch: str | None = "main",
    fail_on_dirty: bool = False,
) -> config.VenvProgram:
    return config.VenvProgram(
        kind="venv",
        python="/fake/python",
        git_dir=str(git_dir),
        branch=branch,
        fail_on_dirty=fail_on_dirty,
        update_script=None,
    )


def _proc(rc: int, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr="",
    )


class TestFailOnDirtyConfig:
    def test_defaults_to_false(self) -> None:
        """Pre-existing config.toml files (without the new field)
        load with fail_on_dirty=False — back-compat."""
        prog = config.VenvProgram(
            kind="venv", python="/x", git_dir="/y",
        )
        assert prog.fail_on_dirty is False

    def test_explicit_true_round_trips(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-queue]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{state_dir / "repo"}"\n'
            'branch = "main"\n'
            'fail_on_dirty = true\n'
        )
        (state_dir / "repo" / ".git").mkdir(parents=True, exist_ok=True)
        cfg = config.load_config()
        assert cfg.programs["vibeqc-queue"].fail_on_dirty is True


class TestDirtyTreeSuccessGate:
    """Pins how dirty_after_update flows into success."""

    def _route_pull_branch_dirty(
        self, *, pull_rc: int = 0, branch_stdout: str = "main",
        dirty_stdout: str = "",
    ):
        """Route subprocess.run for: pull, branch check, dirty check.
        ``dirty_stdout`` is the porcelain output (empty=clean, any
        non-empty=dirty)."""
        def route(*args, **kwargs):
            argv = args[0]
            if len(argv) >= 4 and argv[3] == "pull":
                return _proc(pull_rc, stdout="Already up to date\n")
            if len(argv) >= 4 and argv[3] == "rev-parse":
                return _proc(0, stdout=branch_stdout + "\n")
            if len(argv) >= 4 and argv[3] == "status":
                return _proc(0, stdout=dirty_stdout)
            return _proc(0, stdout="")
        return route

    @pytest.mark.no_autopatch_branch_check
    def test_clean_tree_succeeds_regardless_of_opt_in(
        self, state_dir: Path,
    ) -> None:
        repo = state_dir / "repo"
        repo.mkdir(parents=True, exist_ok=True)
        (repo / ".git").mkdir()
        prog = _make_venv_program(repo, fail_on_dirty=True)
        router = self._route_pull_branch_dirty(dirty_stdout="")
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-queue", prog)
        assert result.dirty_after_update is False
        assert result.success is True

    @pytest.mark.no_autopatch_branch_check
    def test_dirty_tree_fails_when_opt_in_set(
        self, state_dir: Path,
    ) -> None:
        repo = state_dir / "repo"
        repo.mkdir(parents=True, exist_ok=True)
        (repo / ".git").mkdir()
        prog = _make_venv_program(repo, fail_on_dirty=True)
        router = self._route_pull_branch_dirty(dirty_stdout=" M file.py\n")
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-queue", prog)
        assert result.dirty_after_update is True
        assert result.fail_on_dirty_in_effect is True
        assert result.success is False
        rendered = admin.format_update_result(result)
        assert "dirty tree after update" in rendered

    @pytest.mark.no_autopatch_branch_check
    def test_dirty_tree_tolerated_by_default(
        self, state_dir: Path,
    ) -> None:
        """The dev-clone case: dirty is expected, so the default
        ``fail_on_dirty=False`` skips the dirty check entirely.
        The verdict stays True and ``dirty_after_update`` is None
        (the legacy ``vq admin status`` row still shows the
        live-current dirty bit via the separate _query_git_dirty
        call in query_env_status — operators don't lose that
        visibility)."""
        repo = state_dir / "repo"
        repo.mkdir(parents=True, exist_ok=True)
        (repo / ".git").mkdir()
        prog = _make_venv_program(repo, fail_on_dirty=False)
        router = self._route_pull_branch_dirty(dirty_stdout=" M many.g94\n")
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-dev", prog)
        # With fail_on_dirty=False, the check is skipped: None ⇒
        # "we didn't check", not "we checked and it was clean".
        assert result.dirty_after_update is None
        assert result.fail_on_dirty_in_effect is False
        assert result.success is True


class TestPersistAndSurface:
    def test_record_persists_dirty_flag(self, state_dir: Path) -> None:
        result = admin.UpdateResult(
            env="vibeqc-queue",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            dirty_after_update=True,
            fail_on_dirty_in_effect=True,
        )
        admin.record_update_outcome("vibeqc-queue", result)
        rec = admin.read_admin_status()["vibeqc-queue"]
        assert rec.last_dirty_after_update is True
        # And the success verdict propagates honestly.
        assert rec.last_success is False

    def test_json_includes_dirty_field(self, state_dir: Path) -> None:
        repo = state_dir / "repo"
        (repo / ".git").mkdir(parents=True, exist_ok=True)
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-queue]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'fail_on_dirty = true\n'
        )
        admin.record_update_outcome(
            "vibeqc-queue",
            admin.UpdateResult(
                env="vibeqc-queue", git_dir=str(repo), branch=None,
                update_script=None, git_pull_rc=0,
                dirty_after_update=True, fail_on_dirty_in_effect=True,
            ),
        )
        cfg = config.load_config()
        payload = json.loads(admin.format_admin_status_json(cfg))
        env = next(e for e in payload["envs"] if e["name"] == "vibeqc-queue")
        assert env["last_dirty_after_update"] is True
