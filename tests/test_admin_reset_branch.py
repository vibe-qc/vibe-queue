"""v0.7.9 *Liskov's Substitution* — `vq admin reset-branch` tests.

Pins the four invariants:

1. **Real git roundtrip** — given a real git repo with a branch
   that's diverged from origin, `reset_branch_env` actually
   restores the working tree to ``origin/<branch>``.
2. **Validation** — unknown env errors; env without a configured
   ``branch`` errors with the canonical message naming the missing
   config.
3. **Persistence** — the admin-status record reflects the new SHA
   + branch but conservatively preserves ``last_success`` from the
   prior record (reset-branch doesn't prove the build is healthy).
4. **CLI safety guardrail** — `vq admin reset-branch ENV` without
   ``--yes`` prints the planned operation and exits non-zero.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main


@pytest.fixture
def cli_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Hermetic vq state + config dir for CLI invocations."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    return tmp_path


def _git(cwd: Path, *args: str, check: bool = True) -> str:
    """Run a git command and return stdout. Pinned-config so the
    test runs the same on a CI box with no user .gitconfig."""
    env = {
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_AUTHOR_NAME": "vq-test",
        "GIT_AUTHOR_EMAIL": "vq-test@example.invalid",
        "GIT_COMMITTER_NAME": "vq-test",
        "GIT_COMMITTER_EMAIL": "vq-test@example.invalid",
        "PATH": "/usr/bin:/bin:/usr/local/bin",
    }
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True, text=True, env=env,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed: {proc.stderr}"
        )
    return proc.stdout.strip()


def _make_repo_with_remote(
    tmp_path: Path, branch: str = "main",
) -> tuple[Path, Path]:
    """Build a local 'origin' (bare) repo + a working clone with
    one commit on ``branch``. Returns (clone_dir, origin_dir)."""
    origin = tmp_path / "origin.git"
    origin.mkdir()
    _git(origin, "init", "--bare", "--initial-branch", branch)

    clone = tmp_path / "clone"
    clone.mkdir()
    _git(clone, "init", "--initial-branch", branch)
    _git(clone, "remote", "add", "origin", str(origin))
    (clone / "README.md").write_text("v1\n")
    _git(clone, "add", ".")
    _git(clone, "commit", "-m", "initial")
    _git(clone, "push", "-u", "origin", branch)
    return clone, origin


def _cfg_with_env(
    cli_state: Path, env: str, git_dir: Path, *, branch: str = "main",
) -> config.Config:
    """Write a config.toml carrying [programs.venv.<env>] and reload."""
    (cli_state / "cfg" / "config.toml").write_text(
        f"""default_host = "localhost"

[hosts.localhost]
ssh = "localhost"

[programs.{env}]
kind = "venv"
python = "/usr/bin/python3"
git_dir = "{git_dir}"
branch = "{branch}"
"""
    )
    return config.load_config()


# ----------------------------------------------------------------------
# 1. Real git roundtrip — drift forward, reset back, then drift back, reset forward
# ----------------------------------------------------------------------


class TestRealGitRoundtrip:
    def test_resets_drifted_local_to_origin(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """The canonical case: the local working tree has drifted
        ahead of origin (extra commits, modified files). reset_branch
        should snap back to origin's tip."""
        clone, origin = _make_repo_with_remote(tmp_path)
        # Capture the origin's SHA (what we expect to land on).
        origin_sha = _git(clone, "rev-parse", "HEAD")
        # Drift the clone: add an extra commit + a stray modified file.
        (clone / "stray.txt").write_text("not in origin")
        _git(clone, "add", "stray.txt")
        _git(clone, "commit", "-m", "drift commit")
        (clone / "README.md").write_text("locally modified\n")
        drifted_sha = _git(clone, "rev-parse", "HEAD")
        assert drifted_sha != origin_sha

        cfg = _cfg_with_env(cli_state, "myenv", clone)
        result = admin.reset_branch_env("myenv", cfg=cfg)

        assert result.success is True
        assert result.branch == "main"
        assert result.prior_sha is not None
        assert result.new_sha is not None
        # The new SHA should match origin's tip (12-char prefix).
        assert origin_sha.startswith(result.new_sha)
        # The drift commit is gone.
        assert not (clone / "stray.txt").exists()
        # The modified README is reverted to origin's content.
        assert (clone / "README.md").read_text() == "v1\n"

    def test_advances_local_to_new_origin(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """The other canonical case: origin has advanced (another
        clone pushed new commits) and the local clone is behind.
        reset_branch should fast-forward to origin."""
        clone, origin = _make_repo_with_remote(tmp_path)
        local_sha_before = _git(clone, "rev-parse", "HEAD")
        # Simulate origin advancing via a second clone.
        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "newfile.txt").write_text("v2 content")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "from elsewhere")
        _git(clone2, "push", "origin", "main")
        origin_sha = _git(clone2, "rev-parse", "HEAD")
        assert origin_sha != local_sha_before
        # The first clone hasn't fetched the new commit yet.

        cfg = _cfg_with_env(cli_state, "myenv", clone)
        result = admin.reset_branch_env("myenv", cfg=cfg)

        assert result.success is True
        assert origin_sha.startswith(result.new_sha or "")
        assert (clone / "newfile.txt").read_text() == "v2 content"

    def test_no_op_when_already_aligned(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """When the local clone is already aligned with origin, the
        reset is a no-op (rc=0, same SHA before and after)."""
        clone, _ = _make_repo_with_remote(tmp_path)
        sha = _git(clone, "rev-parse", "HEAD")

        cfg = _cfg_with_env(cli_state, "myenv", clone)
        result = admin.reset_branch_env("myenv", cfg=cfg)

        assert result.success is True
        assert sha.startswith(result.new_sha or "")
        assert sha.startswith(result.prior_sha or "")

    def test_reattaches_detached_tag_to_configured_branch(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """A managed release checkout may be detached at an immutable
        tag. reset-branch must leave it attached to the configured
        branch so the next admin update can pull normally."""
        clone, origin = _make_repo_with_remote(tmp_path, branch="release")
        _git(clone, "tag", "v0.15.42")
        _git(clone, "push", "origin", "v0.15.42")
        _git(clone, "checkout", "v0.15.42")
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("v2\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "release branch advances")
        _git(clone2, "push", "origin", "release")
        origin_sha = _git(clone2, "rev-parse", "HEAD")

        cfg = _cfg_with_env(
            cli_state, "myenv", clone, branch="release",
        )
        result = admin.reset_branch_env("myenv", cfg=cfg)

        assert result.success is True
        assert result.checkout_rc == 0
        assert origin_sha.startswith(result.new_sha or "")
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "release"
        upstream = _git(
            clone, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}",
        )
        assert upstream == "origin/release"
        assert (clone / "README.md").read_text() == "v2\n"

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_repairs_clean_detached_tag_before_pull(
        self, tmp_path: Path
    ) -> None:
        """Detached old tag -> newer tag is a supported managed update
        path. The update code should reattach the configured branch,
        fetch and check out the requested immutable tag even when the
        branch tip has moved past that tag."""
        clone, origin = _make_repo_with_remote(tmp_path, branch="release")
        _git(clone, "tag", "v0.15.42")
        _git(clone, "push", "origin", "v0.15.42")
        _git(clone, "checkout", "v0.15.42")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("v2\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "tagged release")
        _git(clone2, "tag", "v0.15.43")
        (clone2 / "README.md").write_text("branch tip after tag\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "branch advances past tag")
        _git(clone2, "push", "origin", "release")
        _git(clone2, "push", "origin", "v0.15.43")

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="release",
            update_script=None,
        )

        result = admin._do_update_work(
            "vibeqc-release", prog, expected_tag="v0.15.43",
        )

        assert result.success is True
        assert result.git_pull_rc == 0
        assert result.actual_branch is None
        assert result.actual_tag == "v0.15.43"
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
        assert (clone / "README.md").read_text() == "v2\n"

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_rejects_missing_immutable_tag(
        self, tmp_path: Path
    ) -> None:
        clone, _ = _make_repo_with_remote(tmp_path, branch="release")
        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="release",
            update_script=None,
        )

        result = admin._do_update_work(
            "vibeqc-release", prog, expected_tag="v0.15.99",
        )

        assert result.success is False
        assert result.git_pull_rc != 0
        assert result.actual_tag is None

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_rejects_moved_immutable_tag(
        self, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path, branch="release")
        _git(clone, "tag", "v0.15.43")
        _git(clone, "push", "origin", "v0.15.43")
        original_tag_sha = _git(clone, "rev-parse", "v0.15.43")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("retagged\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "move tag target")
        _git(clone2, "tag", "-f", "v0.15.43")
        _git(clone2, "push", "--force", "origin", "v0.15.43")
        assert _git(clone2, "rev-parse", "v0.15.43") != original_tag_sha

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="release",
            update_script=None,
        )

        result = admin._do_update_work(
            "vibeqc-release", prog, expected_tag="v0.15.43",
        )

        assert result.success is False
        assert result.git_pull_rc != 0
        assert result.actual_tag is None
        assert _git(clone, "rev-parse", "v0.15.43") == original_tag_sha

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_passes_tag_to_branch_defaulting_script(
        self, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path, branch="release")
        script = clone / "scripts" / "update.sh"
        script.parent.mkdir()
        script.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "target=release\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  case \"$1\" in\n"
            "    --branch) target=\"$2\"; shift 2 ;;\n"
            "    *) shift ;;\n"
            "  esac\n"
            "done\n"
            "git fetch origin release\n"
            "git checkout \"$target\"\n"
            "if [ \"$target\" = release ]; then\n"
            "  git reset --hard origin/release\n"
            "fi\n"
        )
        script.chmod(0o755)
        _git(clone, "add", ".")
        _git(clone, "commit", "-m", "add update script")
        _git(clone, "push", "origin", "release")
        _git(clone, "tag", "v0.15.42")
        _git(clone, "push", "origin", "v0.15.42")
        _git(clone, "checkout", "v0.15.42")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("tag payload\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "tag payload")
        _git(clone2, "tag", "v0.15.43")
        (clone2 / "README.md").write_text("moving branch tip\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "release moves on")
        _git(clone2, "push", "origin", "release")
        _git(clone2, "push", "origin", "v0.15.43")

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="release",
            update_script="scripts/update.sh",
        )

        result = admin._do_update_work(
            "vibeqc-release",
            prog,
            expected_tag="v0.15.43",
            update_script_args=["--recreate-venv"],
        )

        assert result.success is True
        assert result.update_script_rc == 0
        assert result.actual_tag == "v0.15.43"
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
        assert (clone / "README.md").read_text() == "tag payload\n"

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_fails_if_script_drifts_from_exact_tag(
        self, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path, branch="release")
        script = clone / "scripts" / "update.sh"
        script.parent.mkdir()
        script.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "git fetch origin release\n"
            "git checkout release\n"
            "git reset --hard origin/release\n"
        )
        script.chmod(0o755)
        _git(clone, "add", ".")
        _git(clone, "commit", "-m", "add update script")
        _git(clone, "push", "origin", "release")
        _git(clone, "tag", "v0.15.42")
        _git(clone, "push", "origin", "v0.15.42")
        _git(clone, "checkout", "v0.15.42")
        baseline_sha = _git(clone, "rev-parse", "HEAD")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("tag payload\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "tag payload")
        _git(clone2, "tag", "v0.15.43")
        (clone2 / "README.md").write_text("moving branch tip\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "release moves on")
        _git(clone2, "push", "origin", "release")
        _git(clone2, "push", "origin", "v0.15.43")

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="release",
            update_script="scripts/update.sh",
        )

        result = admin._do_update_work(
            "vibeqc-release", prog, expected_tag="v0.15.43",
        )

        assert result.success is False
        assert result.update_script_rc == 0
        assert result.actual_tag is None
        assert any(
            "post-update tag verification failed" in e
            for e in result.work_errors
        )
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
        assert _git(clone, "rev-parse", "HEAD") == baseline_sha
        assert (clone / "README.md").read_text() == "v1\n"
        assert result.rolled_back is True

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_expected_sha_stays_on_blessed_commit(
        self, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path, branch="main")
        script = clone / "scripts" / "update.sh"
        script.parent.mkdir()
        script.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "ref=''\n"
            "dev_seen=0\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  case \"$1\" in\n"
            "    --branch) ref=\"$2\"; shift 2 ;;\n"
            "    --ref) ref=\"$2\"; shift 2 ;;\n"
            "    --dev) dev_seen=1; shift ;;\n"
            "    *) shift ;;\n"
            "  esac\n"
            "done\n"
            "if [ \"$dev_seen\" = 1 ] && [ -n \"$ref\" ]; then\n"
            "  echo 'conflicting selectors' >&2\n"
            "  exit 22\n"
            "fi\n"
            "test -n \"$ref\"\n"
            "git checkout --detach \"$ref\"\n"
        )
        script.chmod(0o755)
        _git(clone, "add", ".")
        _git(clone, "commit", "-m", "add update script")
        _git(clone, "push", "origin", "main")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("blessed payload\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "blessed payload")
        blessed_sha = _git(clone2, "rev-parse", "HEAD")
        (clone2 / "README.md").write_text("moving branch tip\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "main moves on")
        _git(clone2, "push", "origin", "main")

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="main",
            update_script="scripts/update.sh --dev",
        )

        result = admin._do_update_work(
            "vibeqc-dev",
            prog,
            expected_sha=blessed_sha,
            update_script_args=["--recreate-venv"],
        )

        assert result.success is True
        assert result.update_script_rc == 0
        assert result.expected_sha == blessed_sha
        assert result.actual_sha == blessed_sha
        assert result.sha_matches is True
        assert _git(clone, "rev-parse", "HEAD") == blessed_sha
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
        assert (clone / "README.md").read_text() == "blessed payload\n"

    @pytest.mark.no_autopatch_branch_check
    def test_admin_update_fails_if_script_drifts_from_expected_sha(
        self, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path, branch="main")
        script = clone / "scripts" / "update.sh"
        script.parent.mkdir()
        script.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "git fetch origin main\n"
            "git checkout main\n"
            "git reset --hard origin/main\n"
        )
        script.chmod(0o755)
        _git(clone, "add", ".")
        _git(clone, "commit", "-m", "add update script")
        _git(clone, "push", "origin", "main")
        baseline_sha = _git(clone, "rev-parse", "HEAD")

        clone2 = tmp_path / "clone2"
        clone2.mkdir()
        _git(clone2, "clone", str(origin), ".")
        (clone2 / "README.md").write_text("blessed payload\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "blessed payload")
        blessed_sha = _git(clone2, "rev-parse", "HEAD")
        (clone2 / "README.md").write_text("moving branch tip\n")
        _git(clone2, "add", ".")
        _git(clone2, "commit", "-m", "main moves on")
        moving_sha = _git(clone2, "rev-parse", "HEAD")
        _git(clone2, "push", "origin", "main")

        prog = config.VenvProgram(
            kind="venv",
            python="/usr/bin/python3",
            git_dir=str(clone),
            branch="main",
            update_script="scripts/update.sh",
        )

        result = admin._do_update_work(
            "vibeqc-dev", prog, expected_sha=blessed_sha,
        )

        assert result.success is False
        assert result.update_script_rc == 0
        assert result.actual_sha == moving_sha
        assert any(
            "post-update SHA verification failed" in e
            for e in result.work_errors
        )
        assert _git(clone, "rev-parse", "HEAD") == baseline_sha
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "main"
        assert (clone / "README.md").read_text() == "v1\n"
        assert result.rolled_back is True

    @pytest.mark.no_autopatch_branch_check
    @pytest.mark.parametrize("detached", [False, True])
    def test_immutable_update_refuses_dirty_checkout_without_mutation(
        self, tmp_path: Path, detached: bool,
    ) -> None:
        clone, _origin = _make_repo_with_remote(tmp_path, branch="main")
        baseline_sha = _git(clone, "rev-parse", "HEAD")
        if detached:
            _git(clone, "checkout", "--detach", baseline_sha)
        (clone / "README.md").write_text("operator edit\n")
        before_branch = _git(clone, "rev-parse", "--abbrev-ref", "HEAD")

        result = admin._do_update_work(
            "vibeqc-dev",
            config.VenvProgram(
                kind="venv",
                python="/usr/bin/python3",
                git_dir=str(clone),
                branch="main",
            ),
            expected_sha=baseline_sha,
        )

        assert result.success is False
        assert "working tree is dirty" in "; ".join(result.work_errors)
        assert _git(clone, "rev-parse", "HEAD") == baseline_sha
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == before_branch
        assert (clone / "README.md").read_text() == "operator edit\n"

    @pytest.mark.no_autopatch_branch_check
    def test_tag_sha_mismatch_is_rejected_before_checkout(
        self, tmp_path: Path,
    ) -> None:
        clone, _origin = _make_repo_with_remote(tmp_path, branch="release")
        baseline_sha = _git(clone, "rev-parse", "HEAD")
        _git(clone, "tag", "v0.15.43")
        _git(clone, "push", "origin", "v0.15.43")

        result = admin._do_update_work(
            "vibeqc-release",
            config.VenvProgram(
                kind="venv",
                python="/usr/bin/python3",
                git_dir=str(clone),
                branch="release",
            ),
            expected_tag="v0.15.43",
            expected_sha="f" * 40,
        )

        assert result.success is False
        assert "checkout refused" in "; ".join(result.work_errors)
        assert _git(clone, "rev-parse", "HEAD") == baseline_sha
        assert _git(clone, "rev-parse", "--abbrev-ref", "HEAD") == "release"

    @pytest.mark.no_autopatch_branch_check
    def test_expected_tag_is_verified_when_multiple_tags_share_head(
        self, tmp_path: Path,
    ) -> None:
        clone, _origin = _make_repo_with_remote(tmp_path, branch="release")
        expected_sha = _git(clone, "rev-parse", "HEAD")
        _git(clone, "tag", "v0.15.43")
        _git(
            clone,
            "tag",
            "-a",
            "v0.15.43-build-metadata",
            "-m",
            "second tag on the accepted commit",
        )
        _git(clone, "push", "origin", "v0.15.43")
        _git(clone, "push", "origin", "v0.15.43-build-metadata")

        result = admin._do_update_work(
            "vibeqc-release",
            config.VenvProgram(
                kind="venv",
                python="/usr/bin/python3",
                git_dir=str(clone),
                branch="release",
            ),
            expected_tag="v0.15.43",
            expected_sha=expected_sha,
        )

        assert result.success is True, result.work_errors
        assert result.actual_tag == "v0.15.43"
        assert result.tag_matches is True
        assert _git(clone, "rev-parse", "HEAD") == expected_sha


# ----------------------------------------------------------------------
# 2. Validation
# ----------------------------------------------------------------------


class TestValidation:
    def test_unknown_env_errors(self, cli_state: Path) -> None:
        cfg = _cfg_with_env(cli_state, "myenv", cli_state)
        with pytest.raises(
            admin.AdminError, match="(?i)not in config|unknown|programs"
        ):
            admin.reset_branch_env("not-an-env", cfg=cfg)

    def test_env_without_branch_errors(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """The reset target is undefined without a configured
        branch. The error message names the missing config so the
        operator knows what to add."""
        clone, _ = _make_repo_with_remote(tmp_path)
        # Config that explicitly OMITS the branch line.
        (cli_state / "cfg" / "config.toml").write_text(
            f"""default_host = "localhost"

[hosts.localhost]
ssh = "localhost"

[programs.unbranched]
kind = "venv"
python = "/usr/bin/python3"
git_dir = "{clone}"
"""
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="branch ="):
            admin.reset_branch_env("unbranched", cfg=cfg)


# ----------------------------------------------------------------------
# 3. Persistence — admin-status record reflects the reset
# ----------------------------------------------------------------------


class TestAdminStatusPersistence:
    def test_record_carries_new_sha_and_branch(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        clone, origin = _make_repo_with_remote(tmp_path)
        (clone / "stray.txt").write_text("drift")
        _git(clone, "add", "stray.txt")
        _git(clone, "commit", "-m", "drift")
        origin_sha = _git(origin, "rev-parse", "main")

        cfg = _cfg_with_env(cli_state, "myenv", clone)
        admin.reset_branch_env("myenv", cfg=cfg)

        rec = admin.read_admin_status()["myenv"]
        assert origin_sha.startswith(rec.last_sha or "")
        assert rec.last_branch_actual == "main"
        assert rec.last_branch_expected == "main"
        # A reset-branch always produces a clean tree.
        assert rec.last_dirty_after_update is False

    def test_does_not_flip_last_success(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        """A reset-branch fixes the branch but doesn't prove the
        build is healthy. last_success stays whatever it was from
        the prior real update — typically False if the operator is
        reaching for reset-branch in the first place."""
        clone, _ = _make_repo_with_remote(tmp_path)

        # Pre-seed an admin-status record that says last_success=False.
        from vq.admin import AdminUpdateRecord
        records = {
            "myenv": AdminUpdateRecord(
                last_updated_at="2026-05-27T00:00:00+00:00",
                last_success=False,
                last_sha="oldsha000000",
                last_branch_expected="main",
                last_branch_actual="release",  # the drift
            ),
        }
        admin.write_admin_status(records)

        cfg = _cfg_with_env(cli_state, "myenv", clone)
        admin.reset_branch_env("myenv", cfg=cfg)

        rec = admin.read_admin_status()["myenv"]
        # last_success preserved from prior record (NOT flipped).
        assert rec.last_success is False
        # But the SHA + branch_actual now reflect reality.
        assert rec.last_branch_actual == "main"
        assert rec.last_sha != "oldsha000000"


# ----------------------------------------------------------------------
# 4. CLI safety guardrail
# ----------------------------------------------------------------------


class TestCliSafety:
    def test_without_yes_prints_plan_and_errors(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        clone, _ = _make_repo_with_remote(tmp_path)
        _cfg_with_env(cli_state, "myenv", clone)

        result = CliRunner().invoke(
            main,
            ["admin", "reset-branch", "myenv", "localhost"],
        )
        assert result.exit_code != 0
        assert "--yes" in result.output
        assert "DESTRUCTIVE" in result.output
        # And the verb did NOT run the reset.
        assert "myenv" not in admin.read_admin_status()

    def test_with_yes_runs_reset(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        clone, _ = _make_repo_with_remote(tmp_path)
        (clone / "stray.txt").write_text("drift")
        _git(clone, "add", "stray.txt")
        _git(clone, "commit", "-m", "drift")
        _cfg_with_env(cli_state, "myenv", clone)

        result = CliRunner().invoke(
            main,
            ["admin", "reset-branch", "myenv", "localhost", "--yes"],
        )
        assert result.exit_code == 0, result.output
        assert "OK" in result.output
        assert "main" in result.output
        # The drift is gone.
        assert not (clone / "stray.txt").exists()

    def test_json_output(
        self, cli_state: Path, tmp_path: Path
    ) -> None:
        clone, _ = _make_repo_with_remote(tmp_path)
        _cfg_with_env(cli_state, "myenv", clone)

        result = CliRunner().invoke(
            main,
            [
                "admin", "reset-branch", "myenv", "localhost",
                "--yes", "--json",
            ],
        )
        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["env"] == "myenv"
        assert payload["branch"] == "main"
        assert payload["success"] is True
        assert payload["fetch_rc"] == 0
        assert payload["reset_rc"] == 0
