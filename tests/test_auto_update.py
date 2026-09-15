"""Tests for vq.auto_update (v0.6.11) — latest-tag drift + apply.

Coverage shape:
* TestSemverHelpers — _parse_semver_tag + _newest_semver_tag pure
  functions, no fixture needed.
* TestListRemoteTags — subprocess mock around git ls-remote stdout
  parsing (peeled ^{} suffix handling, malformed lines, errors).
* TestCheckEnvDrift — the decision function under each branch
  (drift / no-drift / git-error / no-semver-tags).
* TestAutoUpdateEnv — the apply path with admin.update_env mocked.
* TestAutoUpdateCLI — CliRunner on `vq admin auto-update`.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, auto_update, config, paths
from vq.cli import main

REMOTE_SHA = "a" * 40


@pytest.fixture(autouse=True)
def stable_local_head(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default tag tests use a coherent local/remote commit identity."""
    monkeypatch.setattr(auto_update, "_rev_parse", lambda *args: REMOTE_SHA)


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


def _write_venv_cfg(
    state: Path,
    *,
    name: str = "vibeqc-release",
    git_dir: str | None = None,
) -> Path:
    """Write a minimal config with one [programs.NAME] entry. Returns
    the git_dir path (which is also created as a fake repo)."""
    if git_dir is None:
        git_dir = str(state / "repo")
    _make_git_repo(Path(git_dir))
    (state / "cfg" / "config.toml").write_text(
        f'default_host = "localhost"\n\n'
        f'[programs.{name}]\n'
        f'kind = "venv"\n'
        f'python = "/fake/python"\n'
        f'git_dir = "{git_dir}"\n'
        f'branch = "release"\n'
        f'update_script = "scripts/update.sh"\n'
    )
    return Path(git_dir)


class TestSemverHelpers:
    @pytest.mark.parametrize(
        "tag,expected",
        [
            ("v0.7.3", (0, 7, 3)),
            ("v1.0.0", (1, 0, 0)),
            ("v0.10.0", (0, 10, 0)),
            ("v0.8.0-rc.1", (0, 8, 0)),
            ("v1.2.3+build.42", (1, 2, 3)),
        ],
    )
    def test_parse_semver_valid(
        self, tag: str, expected: tuple[int, int, int]
    ) -> None:
        assert auto_update._parse_semver_tag(tag) == expected

    @pytest.mark.parametrize(
        "tag",
        [
            "v0.7",
            "0.7.3",
            "v0.7.3.1",
            "release-2024",
            "v0.7.3rc1",
            "main",
            "v01.2.3",
            "v1.02.3",
            "v1.2.03",
            "v1.2.3-rc.01",
        ],
    )
    def test_parse_semver_invalid_returns_none(self, tag: str) -> None:
        assert auto_update._parse_semver_tag(tag) is None

    def test_newest_picks_highest_tuple(self) -> None:
        assert (
            auto_update._newest_semver_tag(
                ["v0.7.3", "v0.7.4", "v0.8.0", "v0.7.99"]
            )
            == "v0.8.0"
        )

    def test_newest_handles_double_digit_minor(self) -> None:
        """Pure lexicographic sort would put v0.9.0 > v0.10.0; the
        tuple sort correctly puts v0.10.0 > v0.9.0."""
        assert (
            auto_update._newest_semver_tag(["v0.9.0", "v0.10.0", "v0.8.5"])
            == "v0.10.0"
        )

    def test_newest_prefers_final_release_over_prerelease(self) -> None:
        assert (
            auto_update._newest_semver_tag(
                ["v1.2.3", "v1.2.3-rc.99", "v1.2.3-beta.1"]
            )
            == "v1.2.3"
        )

    def test_newest_compares_numeric_prerelease_identifiers_numerically(
        self,
    ) -> None:
        assert (
            auto_update._newest_semver_tag(
                ["v1.2.3-rc.2", "v1.2.3-rc.10"]
            )
            == "v1.2.3-rc.10"
        )

    def test_build_metadata_has_equal_precedence(self) -> None:
        assert auto_update._semver_precedence(
            "v1.2.3+build.1"
        ) == auto_update._semver_precedence("v1.2.3+build.2")

    def test_newest_ignores_non_semver(self) -> None:
        assert (
            auto_update._newest_semver_tag(
                ["v0.7.3", "release-2024", "v0.8.0", "main"]
            )
            == "v0.8.0"
        )

    def test_newest_returns_none_when_no_semver(self) -> None:
        assert auto_update._newest_semver_tag(["main", "release-2024"]) is None

    def test_newest_empty_input_returns_none(self) -> None:
        assert auto_update._newest_semver_tag([]) is None


class TestListRemoteTags:
    @staticmethod
    def _fake_ls_remote_stdout(
        tags: list[str], *, with_peeled: bool = False
    ) -> str:
        """Build a fake `git ls-remote --tags origin` stdout. Annotated
        tags appear twice — once as the tag object, once as the peeled
        commit (suffix `^{}`)."""
        lines = []
        for i, tag in enumerate(tags):
            sha = f"{i:040x}"
            lines.append(f"{sha}\trefs/tags/{tag}")
            if with_peeled:
                lines.append(f"{(i + 100):040x}\trefs/tags/{tag}^{{}}")
        return "\n".join(lines) + "\n"

    def test_parses_simple_output(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = self._fake_ls_remote_stdout(["v0.7.3", "v0.8.0"])
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ):
            tags = auto_update._list_remote_tags(git_dir)
        assert tags == ["v0.7.3", "v0.8.0"]

    def test_strips_peeled_suffix_and_deduplicates(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = self._fake_ls_remote_stdout(
            ["v0.7.3", "v0.8.0"], with_peeled=True
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ):
            tags = auto_update._list_remote_tags(git_dir)
        # Each tag appears once even though the raw stdout has it twice
        assert tags == ["v0.7.3", "v0.8.0"]

    def test_ignores_malformed_lines(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = (
            f"{REMOTE_SHA}\trefs/tags/v0.7.3\n"
            "\n"  # blank
            "garbage with no tab\n"
            "def456\trefs/heads/main\n"  # not a tag ref
            f"{REMOTE_SHA}\trefs/tags/v0.8.0\n"
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ):
            tags = auto_update._list_remote_tags(git_dir)
        assert tags == ["v0.7.3", "v0.8.0"]

    def test_rejects_invalid_sha_tag_rows(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = (
            "abc123\trefs/tags/v9.9.9\n"
            f"{REMOTE_SHA}\trefs/tags/v0.8.0\n"
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ), pytest.raises(ValueError, match="invalid object ID"):
            auto_update._list_remote_tag_refs(git_dir)

    def test_rejects_orphan_peeled_tag_row(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = f"{REMOTE_SHA}\trefs/tags/v9.9.9^{{}}\n"
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ), pytest.raises(ValueError, match="orphan peeled"):
            auto_update._list_remote_tag_refs(git_dir)

    def test_rejects_conflicting_duplicate_tag_identity(
        self, tmp_path: Path,
    ) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        stdout = (
            f"{'a' * 40}\trefs/tags/v9.9.9\n"
            f"{'b' * 40}\trefs/tags/v9.9.9\n"
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0, stdout=stdout, stderr=""
            ),
        ), pytest.raises(ValueError, match="conflicting direct"):
            auto_update._list_remote_tag_refs(git_dir)

    def test_propagates_called_process_error(self, tmp_path: Path) -> None:
        git_dir = _make_git_repo(tmp_path / "repo")
        with patch(
            "vq.auto_update.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=128, cmd=["git"], stderr="fatal: not a git repo"
            ),
        ), pytest.raises(subprocess.CalledProcessError):
            auto_update._list_remote_tags(git_dir)


class TestCheckEnvDrift:
    def test_multiple_exact_local_tags_choose_highest_semver(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        monkeypatch.setattr(
            auto_update,
            "_local_semver_tags_at_head",
            lambda _git: (["v1.0.0", "v3.0.0"], None),
        )
        monkeypatch.setattr(auto_update, "_rev_parse", lambda *_: REMOTE_SHA)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v2.0.0\n",
                stderr="",
            ),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)

        assert decision.action == "skip"
        assert decision.current_tag == "v3.0.0"
        assert "downgrade" in decision.reason

    def test_malformed_highest_remote_tag_fails_inventory(
        self, state_dir: Path,
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout=(
                    f"{REMOTE_SHA}\trefs/tags/v1.0.0\n"
                    "bad\trefs/tags/v99.0.0\n"
                ),
                stderr="",
            ),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "error"
        assert "invalid remote tag inventory" in decision.reason

    def test_drift_detected_when_local_lags_remote(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        # First mock call → ls-remote returns v0.7.3 + v0.8.0
        # Second mock call → describe returns v0.7.3 (local current)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=(
                    f"{REMOTE_SHA}\trefs/tags/v0.7.3\n"
                    f"{REMOTE_SHA}\trefs/tags/v0.8.0\n"
                ),
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "update"
        assert decision.current_tag == "v0.7.3"
        assert decision.target_tag == "v0.8.0"
        assert "drift" in decision.reason.lower()

    def test_no_drift_when_local_at_newest(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=(
                    f"{REMOTE_SHA}\trefs/tags/v0.7.3\n"
                    f"{REMOTE_SHA}\trefs/tags/v0.8.0\n"
                ),
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "skip"
        assert decision.current_tag == "v0.8.0"
        assert decision.target_tag == "v0.8.0"
        assert "already at latest" in decision.reason.lower()

    def test_remote_tag_deletion_never_causes_unattended_downgrade(
        self, state_dir: Path
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.9.0"),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "skip"
        assert decision.current_tag == "v0.9.0"
        assert decision.target_tag == "v0.8.0"
        assert "downgrade" in decision.reason.lower()

    def test_equal_precedence_build_metadata_refuses_sideways_move(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        current_sha = "b" * 40
        monkeypatch.setattr(auto_update, "_rev_parse", lambda *args: current_sha)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v1.2.3+build.2\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v1.2.3+build.1"),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)

        assert decision.action == "error"
        assert "sideways move" in decision.reason

    def test_equal_precedence_remote_targets_are_ambiguous(
        self, state_dir: Path
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=(
                    f"{'a' * 40}\trefs/tags/v1.2.3+build.1\n"
                    f"{'b' * 40}\trefs/tags/v1.2.3+build.2\n"
                ),
                stderr="",
            ),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)

        assert decision.action == "error"
        assert "ambiguous unattended target" in decision.reason

    def test_annotated_tag_uses_peeled_commit_and_rewrite_is_an_error(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        current_sha = "b" * 40
        tag_object = "c" * 40
        remote_commit = "d" * 40
        monkeypatch.setattr(auto_update, "_rev_parse", lambda *args: current_sha)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=(
                    f"{tag_object}\trefs/tags/v1.2.3\n"
                    f"{remote_commit}\trefs/tags/v1.2.3^{{}}\n"
                ),
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v1.2.3"),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)

        assert decision.action == "error"
        assert decision.target_sha == remote_commit
        assert "rewritten-tag" in decision.reason

    def test_local_no_tag_fails_closed(self, state_dir: Path) -> None:
        """An untagged HEAD has unknown downgrade direction."""
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(128, None),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "error"
        assert decision.current_tag is None
        assert decision.target_tag == "v0.8.0"
        assert "downgrade direction is unknown" in decision.reason

    def test_ls_remote_error_yields_error_decision(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=128, cmd=["git"], stderr="fatal: cannot reach remote"
            ),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "error"
        assert "ls-remote" in decision.reason.lower()
        assert decision.target_tag is None

    def test_ls_remote_timeout_yields_error_decision(
        self, state_dir: Path
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["git"], timeout=30),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "error"
        assert "timed out" in decision.reason.lower()

    def test_no_semver_tags_yields_error_decision(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/release-2024\n",
                stderr="",
            ),
        ):
            decision = auto_update.check_env_drift("vibeqc-release", cfg)
        assert decision.action == "error"
        assert "semver" in decision.reason.lower()

    def test_unknown_env_raises_admin_error(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="unknown env"):
            auto_update.check_env_drift("ghost", cfg)


class TestAutoUpdateEnv:
    """The apply path: combines drift detection with admin.update_env."""

    def test_nonlocal_direct_helper_refuses_before_drift_or_apply(
        self, state_dir: Path,
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch("vq.auto_update.check_env_drift") as drift, patch(
            "vq.auto_update.admin.update_env"
        ) as apply:
            outcome = auto_update.auto_update_env(
                "vibeqc-release", cfg, host="remote.example"
            )
        assert outcome.decision.action == "error"
        assert "only apply on the local host" in outcome.decision.reason
        drift.assert_not_called()
        apply.assert_not_called()

    def test_dry_run_never_applies(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ), patch(
            "vq.auto_update.admin.update_env"
        ) as mock_update:
            outcome = auto_update.auto_update_env(
                "vibeqc-release", cfg, host="localhost", dry_run=True
            )
        assert outcome.decision.action == "update"
        assert outcome.update_result is None
        # No apply attempted in dry-run mode
        mock_update.assert_not_called()

    def test_skip_does_not_call_update_env(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),  # already current
        ), patch(
            "vq.auto_update.admin.update_env"
        ) as mock_update:
            outcome = auto_update.auto_update_env(
                "vibeqc-release", cfg, host="localhost", dry_run=False
            )
        assert outcome.decision.action == "skip"
        assert outcome.update_result is None
        mock_update.assert_not_called()

    def test_error_decision_does_not_call_update_env(
        self, state_dir: Path
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.auto_update.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=128, cmd=["git"]
            ),
        ), patch(
            "vq.auto_update.admin.update_env"
        ) as mock_update:
            outcome = auto_update.auto_update_env(
                "vibeqc-release", cfg, host="localhost", dry_run=False
            )
        assert outcome.decision.action == "error"
        assert outcome.update_result is None
        mock_update.assert_not_called()

    def test_drift_apply_calls_update_env_with_target_tag(
        self, state_dir: Path
    ) -> None:
        _write_venv_cfg(state_dir)
        cfg = config.load_config()
        fake_result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir="/fake/git_dir",
            branch="release",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            expected_tag="v0.8.0",
            actual_tag="v0.8.0",
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ), patch(
            "vq.auto_update.admin.update_env", return_value=fake_result
        ) as mock_update:
            outcome = auto_update.auto_update_env(
                "vibeqc-release", cfg, host="localhost", dry_run=False
            )
        assert outcome.decision.action == "update"
        assert outcome.update_result is fake_result
        mock_update.assert_called_once()
        # The expected_tag kwarg is the target tag the drift check found
        _args, kwargs = mock_update.call_args
        assert kwargs.get("expected_tag") == "v0.8.0"
        assert kwargs.get("expected_sha") == REMOTE_SHA


class TestAutoUpdateCLI:
    """`vq admin auto-update` end-to-end with mocked git + update_env."""

    def test_dry_run_text_mode_shows_decision(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release", "--dry-run"]
            )
        assert result.exit_code == 0, result.output
        assert "action:       update" in result.output
        assert "current_tag:  v0.7.3" in result.output
        assert "target_tag:   v0.8.0" in result.output
        assert "dry-run" in result.output

    def test_apply_calls_update_env(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        fake_result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir="/fake/git_dir",
            branch="release",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            expected_tag="v0.8.0",
            actual_tag="v0.8.0",
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ), patch(
            "vq.auto_update.admin.update_env", return_value=fake_result
        ) as mock_update:
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code == 0, result.output
        assert "apply:        OK" in result.output
        mock_update.assert_called_once()

    def test_apply_failure_exits_non_zero(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        fake_result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir="/fake/git_dir",
            branch="release",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=1,  # build failed → success property = False
            expected_tag="v0.8.0",
            actual_tag="v0.8.0",
            work_errors=["update_script rc=1"],
        )
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ), patch(
            "vq.auto_update.admin.update_env", return_value=fake_result
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code != 0
        assert "apply:        FAILED" in result.output

    def test_skip_exits_zero(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.8.0"),
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code == 0
        assert "action:       skip" in result.output

    def test_error_exits_non_zero(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            side_effect=subprocess.CalledProcessError(
                returncode=128, cmd=["git"], stderr="fatal: no remote"
            ),
        ):
            result = CliRunner().invoke(
                main, ["admin", "auto-update", "vibeqc-release"]
            )
        assert result.exit_code != 0
        assert "action:       error" in result.output

    def test_unknown_env_rejected_at_cli(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        result = CliRunner().invoke(
            main, ["admin", "auto-update", "ghost-env"]
        )
        assert result.exit_code != 0
        assert "unknown env" in result.output.lower()

    def test_json_mode_emits_machine_readable(self, state_dir: Path) -> None:
        _write_venv_cfg(state_dir)
        with patch(
            "vq.auto_update.subprocess.run",
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=f"{REMOTE_SHA}\trefs/tags/v0.8.0\n",
                stderr="",
            ),
        ), patch(
            "vq.auto_update.admin._run_git_tag_check",
            return_value=(0, "v0.7.3"),
        ):
            result = CliRunner().invoke(
                main,
                ["admin", "auto-update", "vibeqc-release", "--dry-run", "--json"],
            )
        assert result.exit_code == 0
        import json as _json
        payload = _json.loads(result.output)
        assert payload["decision"]["action"] == "update"
        assert payload["decision"]["current_tag"] == "v0.7.3"
        assert payload["decision"]["target_tag"] == "v0.8.0"
        assert payload["dry_run"] is True
        assert payload["update_result"] is None

    def test_help_names_both_explicit_update_policies(self) -> None:
        result = CliRunner().invoke(main, ["admin", "auto-update", "--help"])
        assert result.exit_code == 0
        assert "Tag mode" in result.output
        assert "Branch mode" in result.output
        assert "vq self-update" in result.output
