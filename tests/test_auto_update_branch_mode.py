"""v0.7.4 *Ritchie's Pipe* — branch-mode dev-tip tracker for
``vq admin auto-update``.

Per-env ``auto_update_policy`` config selects whether drift detection
watches the newest semver tag on origin (``"tag"``, default and the
v0.6.11 behavior) or watches ``origin/<branch>`` SHA
(``"branch"``, new in v0.7.4). The branch-mode lets a dev-tracking
env auto-refresh whenever main advances, without requiring a tag.

See ``docs/roadmap.md`` v0.7.4 entry for the rationale (why this
wasn't here from v0.6.11 and what's changed since to make it safe).
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, auto_update, cli, config, paths


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_git_repo(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    (path / ".git").mkdir()
    return path


def _write_branch_cfg(
    state_dir: Path, repo: Path, branch: str | None = "main",
) -> None:
    body = (
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        'python = "/fake/python"\n'
        f'git_dir = "{repo}"\n'
        'auto_update_policy = "branch"\n'
    )
    if branch:
        body += f'branch = "{branch}"\n'
    (state_dir / "cfg" / "config.toml").write_text(body)


def _proc(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr=stderr,
    )


def _make_subprocess_router(
    *,
    fetch_rc: int = 0,
    head_sha: str = "abc1234567890abc1234567890abc1234567890a",
    origin_sha: str = "abc1234567890abc1234567890abc1234567890a",
    head_rc: int = 0,
    origin_rc: int = 0,
):
    """Side-effect router that dispatches by argv shape: git fetch /
    git rev-parse HEAD / git rev-parse origin/<branch>."""
    def route(*args, **kwargs):
        argv = args[0] if args else kwargs.get("args", [])
        if argv and argv[0] == "git":
            if "fetch" in argv:
                return _proc(fetch_rc, stderr="" if fetch_rc == 0 else "fetch error")
            if "rev-parse" in argv:
                ref = argv[-1]
                if ref == "HEAD":
                    return _proc(head_rc, stdout=head_sha + "\n" if head_rc == 0 else "")
                if ref.startswith("origin/"):
                    return _proc(origin_rc, stdout=origin_sha + "\n" if origin_rc == 0 else "")
        return _proc(0, stdout="")
    return route


# ----------------------------------------------------------------------
# Config field
# ----------------------------------------------------------------------


class TestAutoUpdatePolicyConfig:
    def test_defaults_to_tag(self) -> None:
        prog = config.VenvProgram(
            kind="venv", python="/x", git_dir="/y",
        )
        assert prog.auto_update_policy == "tag"

    def test_branch_policy_round_trips(self, state_dir: Path) -> None:
        _write_branch_cfg(state_dir, state_dir / "repo")
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]
        assert prog.auto_update_policy == "branch"
        assert prog.branch == "main"


# ----------------------------------------------------------------------
# check_env_drift — branch mode
# ----------------------------------------------------------------------


class TestBranchModeDriftCheck:
    def test_no_drift_returns_skip(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo)
        cfg = config.load_config()
        sha = "abc1234567890abc1234567890abc1234567890a"
        router = _make_subprocess_router(head_sha=sha, origin_sha=sha)
        with patch("vq.auto_update.subprocess.run", side_effect=router):
            decision = auto_update.check_env_drift("vibeqc-dev", cfg)
        assert decision.action == "skip"
        assert decision.policy == "branch"
        assert decision.current_sha == sha
        assert decision.target_sha == sha
        assert "already at origin/main" in decision.reason

    def test_drift_returns_update_with_target_sha(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo)
        cfg = config.load_config()
        old_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        new_sha = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        router = _make_subprocess_router(
            head_sha=old_sha, origin_sha=new_sha,
        )
        with patch("vq.auto_update.subprocess.run", side_effect=router):
            decision = auto_update.check_env_drift("vibeqc-dev", cfg)
        assert decision.action == "update"
        assert decision.policy == "branch"
        assert decision.current_sha == old_sha
        assert decision.target_sha == new_sha
        assert "branch drift" in decision.reason
        # No tag involved in branch mode:
        assert decision.target_tag is None
        assert decision.current_tag is None

    @pytest.mark.parametrize(
        ("forward", "reverse", "expected_action", "reason_fragment"),
        [
            (False, True, "skip", "refusing unattended downgrade"),
            (False, False, "error", "have diverged"),
            (None, None, "error", "could not prove ancestry"),
        ],
    )
    def test_non_forward_history_never_authorizes_branch_update(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
        forward: bool | None,
        reverse: bool | None,
        expected_action: str,
        reason_fragment: str,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo)
        cfg = config.load_config()
        old_sha = "a" * 40
        new_sha = "b" * 40
        router = _make_subprocess_router(
            head_sha=old_sha,
            origin_sha=new_sha,
        )

        def ancestry(
            unused_repo: Path,
            older: str,
            newer: str,
        ) -> bool | None:
            assert unused_repo == repo
            if (older, newer) == (old_sha, new_sha):
                return forward
            if (older, newer) == (new_sha, old_sha):
                return reverse
            pytest.fail(f"unexpected ancestry pair {(older, newer)}")

        monkeypatch.setattr(auto_update, "_is_ancestor", ancestry)
        with patch("vq.auto_update.subprocess.run", side_effect=router):
            decision = auto_update.check_env_drift("vibeqc-dev", cfg)

        assert decision.action == expected_action
        assert reason_fragment in decision.reason
        assert decision.current_sha == old_sha
        assert decision.target_sha == new_sha

    def test_branch_unset_returns_error(self, state_dir: Path) -> None:
        """Mis-configured: policy=branch but branch field empty."""
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo, branch=None)
        cfg = config.load_config()
        decision = auto_update.check_env_drift("vibeqc-dev", cfg)
        assert decision.action == "error"
        assert decision.policy == "branch"
        assert "requires `branch" in decision.reason

    def test_fetch_failure_returns_error(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo)
        cfg = config.load_config()
        router = _make_subprocess_router(fetch_rc=1)
        with patch("vq.auto_update.subprocess.run", side_effect=router):
            decision = auto_update.check_env_drift("vibeqc-dev", cfg)
        assert decision.action == "error"
        assert decision.policy == "branch"
        assert "git fetch origin failed" in decision.reason

    def test_missing_origin_branch_returns_error(
        self, state_dir: Path,
    ) -> None:
        """origin/<branch> doesn't exist (typo in branch field or
        branch never pushed). rev-parse returns rc!=0 → error."""
        repo = _make_git_repo(state_dir / "repo")
        _write_branch_cfg(state_dir, repo)
        cfg = config.load_config()
        router = _make_subprocess_router(origin_rc=1)
        with patch("vq.auto_update.subprocess.run", side_effect=router):
            decision = auto_update.check_env_drift("vibeqc-dev", cfg)
        assert decision.action == "error"
        assert decision.policy == "branch"
        assert "git rev-parse origin/main failed" in decision.reason


@pytest.mark.parametrize("ancestry", [False, None])
def test_queued_branch_worker_refuses_unproved_forward_move(
    state_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
    ancestry: bool | None,
) -> None:
    repo = _make_git_repo(state_dir / "repo")
    _write_branch_cfg(state_dir, repo)
    baseline = "a" * 40
    target = "b" * 40
    monkeypatch.setattr(admin, "current_source_sha", lambda unused: baseline)
    monkeypatch.setattr(auto_update, "_fetch_origin", lambda unused: (0, ""))
    monkeypatch.setattr(
        auto_update,
        "_rev_parse",
        lambda unused_repo, unused_ref: target,
    )
    monkeypatch.setattr(
        auto_update,
        "_is_ancestor",
        lambda unused_repo, unused_old, unused_new: ancestry,
    )
    monkeypatch.setattr(
        admin,
        "update_env",
        lambda *args, **kwargs: pytest.fail("unsafe queued move reached apply"),
    )

    result = CliRunner().invoke(
        cli.main,
        [
            "build-env",
            "vibeqc-dev",
            "--baseline-sha",
            baseline,
            "--expected-sha",
            target,
        ],
    )

    assert result.exit_code == 2
    assert "not a proven descendant" in result.output


# ----------------------------------------------------------------------
# check_env_drift — tag mode still works (back-compat)
# ----------------------------------------------------------------------


class TestTagModeBackCompat:
    def test_default_policy_still_tag(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-release]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "release"\n'
            # NO auto_update_policy → default "tag"
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-release"]
        assert prog.auto_update_policy == "tag"

    def test_tag_mode_decision_carries_policy_tag(
        self, state_dir: Path,
    ) -> None:
        """Existing tag-mode decisions still carry policy='tag'
        (default in the dataclass) so consumers can discriminate."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-release]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "release"\n'
        )
        cfg = config.load_config()
        # Stub the tag-mode helpers to a "no drift" outcome.
        def route(*args, **kwargs):
            argv = args[0] if args else kwargs.get("args", [])
            if argv and "ls-remote" in argv:
                return _proc(
                    0,
                    stdout=f"{'a' * 40}\trefs/tags/v0.9.1\n",
                )
            if argv and "describe" in argv:
                return _proc(0, stdout="v0.9.1\n")
            if argv and "rev-parse" in argv:
                return _proc(0, stdout=f"{'a' * 40}\n")
            return _proc(0, stdout="")
        with patch("vq.auto_update.subprocess.run", side_effect=route):  # noqa: SIM117
            # also mock admin._run_git_tag_check used inside the tag path
            with patch(
                "vq.admin._run_git_tag_check",
                return_value=(0, "v0.9.1"),
            ):
                decision = auto_update.check_env_drift(
                    "vibeqc-release", cfg,
                )
        assert decision.policy == "tag"
        assert decision.action == "skip"
        assert decision.target_tag == "v0.9.1"
