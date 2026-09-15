"""v0.7.1 *Lamport's Clock* — Item 1: post-update branch validation.

Pins the contract added in admin.py: after a successful ``git pull``,
``_do_update_work`` runs ``git rev-parse --abbrev-ref HEAD`` and
compares the result to ``VenvProgram.branch`` (config). Mismatch ⇒
``UpdateResult.success`` flips to False with reason
``branch mismatch`` AND the update_script is skipped (don't burn
10-30 min building against the wrong branch).

The incident this prevents: 2026-05-25 host_d vibeqc-dev silently
sat on ``release`` despite config saying ``main`` — root cause was
vibe-qc's ``scripts/_safe_build_env.sh`` losing argv across a niced
re-exec (fixed in vibe-qc ``ea195796``). vq's defense-in-depth: the
next ``vq admin update`` would have failed loudly with a
``branch_mismatch`` reason instead of completing silently against
the wrong tree.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 1 for the full
postmortem + design rationale.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config, paths

# ----------------------------------------------------------------------
# Fixtures (mirror test_admin.py conventions)
# ----------------------------------------------------------------------


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


def _make_venv_program(
    git_dir: Path, *, branch: str | None = "main",
    update_script: str | None = "scripts/update.sh",
) -> config.VenvProgram:
    """Build a VenvProgram fixture and, when ``update_script`` is set,
    materialise the script file under ``git_dir/<update_script>`` so
    ``_run_update_script``'s pre-invocation existence check passes
    (the subprocess.run itself is mocked by the per-test router)."""
    if update_script:
        script_path = git_dir / update_script
        script_path.parent.mkdir(parents=True, exist_ok=True)
        script_path.write_text("#!/bin/bash\necho stub\n")
        script_path.chmod(0o755)
    return config.VenvProgram(
        kind="venv",
        python="/fake/python",
        git_dir=str(git_dir),
        branch=branch,
        update_script=update_script,
    )


def _proc(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr=stderr,
    )


def _make_subprocess_router(
    *,
    pull_rc: int = 0,
    branch_stdout: str = "main",
    branch_rc: int = 0,
    script_rc: int = 0,
):
    """Build a side_effect for ``subprocess.run`` that dispatches by
    the leading argv element. Mirrors the pattern in test_admin.py
    (which uses _bash_calls etc.) but parameterised so each test
    can pin its own (pull, branch-check, script) outcome triple."""
    def _route(*args, **kwargs):
        # subprocess.run(["git", "-C", <dir>, "pull"]) → git pull
        # subprocess.run(["git", "-C", <dir>, "rev-parse", ...]) → branch check
        # subprocess.run(["bash", <script>, ...]) → update_script
        # NB: ``*args`` capture (not ``args``); the cmd list is the
        # FIRST positional arg, not the args tuple itself.
        argv = args[0] if args else kwargs.get("args", [])
        if len(argv) >= 4 and argv[0] == "git" and argv[3] == "pull":
            return _proc(pull_rc, stdout="Already up to date.\n")
        if (
            len(argv) >= 4
            and argv[0] == "git"
            and argv[3] == "rev-parse"
        ):
            # The branch check; honor (branch_rc, branch_stdout)
            return _proc(
                branch_rc,
                stdout=branch_stdout + "\n" if branch_rc == 0 else "",
            )
        if argv and argv[0] == "bash":
            return _proc(script_rc, stdout="(build output)\n")
        # Unknown — return a benign success so tag-check etc. don't blow up
        return _proc(0, stdout="")
    return _route


# ----------------------------------------------------------------------
# Core: _do_update_work branch verification
# ----------------------------------------------------------------------


@pytest.mark.no_autopatch_branch_check
class TestBranchVerificationCore:
    """Pins ``_do_update_work``'s branch check: ran when (and only
    when) ``VenvProgram.branch`` is set AND git pull succeeded; sets
    ``actual_branch`` + ``branch_check_rc``; verdict flows through
    ``success``.

    Opts out of the conftest autopatch via the marker so that the
    real ``_run_git_branch_check`` runs against this file's
    per-test ``subprocess.run`` router."""

    def test_branch_match_yields_success(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        prog = _make_venv_program(repo, branch="main")
        router = _make_subprocess_router(branch_stdout="main")
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-dev", prog)
        assert result.branch_verification_attempted is True
        assert result.actual_branch == "main"
        assert result.branch_check_rc == 0
        assert result.branch_matches is True
        assert result.success is True

    def test_branch_mismatch_fails_and_skips_build(
        self, state_dir: Path,
    ) -> None:
        """The 2026-05-25 incident shape: config says main, HEAD is on
        release. Update fails with branch_mismatch, and crucially the
        bash update_script is NEVER invoked (we don't burn the build)."""
        repo = _make_git_repo(state_dir / "repo")
        prog = _make_venv_program(repo, branch="main")
        router = _make_subprocess_router(branch_stdout="release")
        with patch("vq.admin.subprocess.run", side_effect=router) as run:
            result = admin._do_update_work("vibeqc-dev", prog)
        assert result.branch_verification_attempted is True
        assert result.actual_branch == "release"
        assert result.branch_matches is False
        assert result.success is False
        # Crucial: no bash invocation. The build was correctly skipped.
        bash_calls = [
            c for c in run.call_args_list
            if c.args and c.args[0] and c.args[0][0] == "bash"
        ]
        assert bash_calls == [], (
            f"branch mismatch should skip the build; got: {bash_calls}"
        )

    def test_no_configured_branch_skips_check(
        self, state_dir: Path,
    ) -> None:
        """Legacy envs that leave ``branch`` unset (operator manages
        the branch by hand) get the pre-v0.7.1 behavior — no check,
        no field population, no success-impact."""
        repo = _make_git_repo(state_dir / "repo")
        prog = _make_venv_program(repo, branch=None)
        router = _make_subprocess_router()
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-dev", prog)
        assert result.branch_verification_attempted is False
        assert result.actual_branch is None
        assert result.branch_check_rc is None
        assert result.branch_matches is None
        assert result.success is True

    def test_git_pull_failure_skips_branch_check(
        self, state_dir: Path,
    ) -> None:
        """When git pull itself failed, the branch check shouldn't
        even run — we have no fresh tree to check. The check stays
        un-attempted (None) and the pull failure dominates."""
        repo = _make_git_repo(state_dir / "repo")
        prog = _make_venv_program(repo, branch="main")
        router = _make_subprocess_router(pull_rc=1)
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-dev", prog)
        assert result.git_pull_rc == 1
        assert result.branch_verification_attempted is False
        assert result.actual_branch is None
        assert result.branch_check_rc is None
        assert result.success is False

    def test_detached_head_treated_as_mismatch(
        self, state_dir: Path,
    ) -> None:
        """``git rev-parse --abbrev-ref HEAD`` returns the literal
        string ``HEAD`` for a detached checkout (e.g. ``git checkout
        <SHA>``). That is a mismatch against any real configured
        branch and surfaces as success=False."""
        repo = _make_git_repo(state_dir / "repo")
        prog = _make_venv_program(repo, branch="main")
        router = _make_subprocess_router(branch_stdout="HEAD")
        with patch("vq.admin.subprocess.run", side_effect=router):
            result = admin._do_update_work("vibeqc-dev", prog)
        assert result.actual_branch == "HEAD"
        assert result.branch_matches is False
        assert result.success is False


# ----------------------------------------------------------------------
# _run_git_branch_check — direct unit test
# ----------------------------------------------------------------------


@pytest.mark.no_autopatch_branch_check
class TestRunGitBranchCheck:
    """Pins the helper's contract: (rc, branch_or_none) from
    ``git rev-parse --abbrev-ref HEAD``. Opts out of the conftest
    autopatch so we're testing the real implementation, not the
    stub."""

    def test_normal_branch(self) -> None:
        with patch("vq.admin.subprocess.run") as run:
            run.return_value = _proc(0, stdout="main\n")
            rc, branch = admin._run_git_branch_check(Path("/fake"))
        assert rc == 0
        assert branch == "main"

    def test_detached_head_returns_literal(self) -> None:
        with patch("vq.admin.subprocess.run") as run:
            run.return_value = _proc(0, stdout="HEAD\n")
            rc, branch = admin._run_git_branch_check(Path("/fake"))
        assert rc == 0
        assert branch == "HEAD"

    def test_nonzero_rc_returns_none(self) -> None:
        """Broken .git / corrupt repo — rc non-zero, branch=None.
        Caller's comparison evaluates None != "main" ⇒ False ⇒
        success=False, the safe-failure path."""
        with patch("vq.admin.subprocess.run") as run:
            run.return_value = _proc(128, stderr="fatal: not a git repository\n")
            rc, branch = admin._run_git_branch_check(Path("/fake"))
        assert rc == 128
        assert branch is None

    def test_timeout_returns_safe_failure(self) -> None:
        """Timeout / OSError both surface as (1, None) — neutral
        failure signal that turns into success=False via comparison."""
        with patch("vq.admin.subprocess.run") as run:
            run.side_effect = subprocess.TimeoutExpired(cmd="git", timeout=30)
            rc, branch = admin._run_git_branch_check(Path("/fake"))
        assert rc == 1
        assert branch is None


# ----------------------------------------------------------------------
# AdminUpdateRecord persistence
# ----------------------------------------------------------------------


class TestAdminUpdateRecordBranchFields:
    """Pins that record_update_outcome serialises the new branch
    fields, and read_admin_status round-trips them. Also asserts
    pre-v0.7.1 record files (without the new keys) load cleanly via
    dataclass defaults."""

    def test_record_round_trip(self, state_dir: Path) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="main",
            branch_check_rc=0,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        records = admin.read_admin_status()
        rec = records["vibeqc-dev"]
        assert rec.last_branch_expected == "main"
        assert rec.last_branch_actual == "main"
        assert rec.last_success is True

    def test_record_round_trip_mismatch(self, state_dir: Path) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(state_dir / "repo"),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="release",
            branch_check_rc=0,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        rec = admin.read_admin_status()["vibeqc-dev"]
        assert rec.last_branch_expected == "main"
        assert rec.last_branch_actual == "release"
        assert rec.last_success is False  # branch_matches=False flows in

    def test_pre_v0_7_1_record_loads_via_defaults(
        self, state_dir: Path,
    ) -> None:
        """A v0.7.0-shaped record file (no last_branch_* keys) should
        load with both fields defaulted to None — not get dropped by
        the TypeError-skip path. Catches the back-compat contract."""
        path = admin.admin_status_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write a record using the v0.7.0 key set only.
        path.write_text(json.dumps({
            "vibeqc-dev": {
                "last_updated_at": "2026-05-25T00:00:00+00:00",
                "last_success": True,
                "last_sha": "abc123",
                "last_tag": None,
                "last_expected_tag": None,
                "last_git_pull_rc": 0,
                "last_update_script_rc": 0,
            },
        }))
        records = admin.read_admin_status()
        # Must NOT be dropped — that's the regression we're pinning.
        assert "vibeqc-dev" in records
        rec = records["vibeqc-dev"]
        assert rec.last_success is True
        # The new fields default to None.
        assert rec.last_branch_expected is None
        assert rec.last_branch_actual is None


# ----------------------------------------------------------------------
# Surface rendering: text + JSON
# ----------------------------------------------------------------------


class TestStatusRenderingBranchDrift:
    """Pins how the v0.7.1 branch-drift signal surfaces in operator-
    facing output."""

    def _write_cfg(self, state_dir: Path, repo: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )

    def test_text_renders_arrow_on_drift(self, state_dir: Path) -> None:
        """Operator-facing: when the LAST update detected drift, the
        BRANCH column shows ``main -> release`` instead of plain
        ``main``. Pre-v0.7.1 records (no actual_branch) fall through
        to the plain rendering."""
        repo = _make_git_repo(state_dir / "repo")
        self._write_cfg(state_dir, repo)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(repo),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="release",
            branch_check_rc=0,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        rendered = admin.format_admin_status(cfg)
        assert "main -> release" in rendered, rendered

    def test_text_plain_when_no_drift(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        self._write_cfg(state_dir, repo)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(repo),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="main",
            branch_check_rc=0,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        rendered = admin.format_admin_status(cfg)
        # No arrow when expected == actual.
        assert "main -> " not in rendered, rendered

    def test_json_always_includes_branch_fields(
        self, state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        self._write_cfg(state_dir, repo)
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir=str(repo),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="release",
            branch_check_rc=0,
        )
        admin.record_update_outcome("vibeqc-dev", result)
        cfg = config.load_config()
        payload = json.loads(admin.format_admin_status_json(cfg))
        env = next(e for e in payload["envs"] if e["name"] == "vibeqc-dev")
        assert env["last_branch_expected"] == "main"
        assert env["last_branch_actual"] == "release"

    def test_failure_summary_lists_branch_mismatch(self) -> None:
        """``format_update_result`` failure-reasons line includes the
        branch mismatch so the operator sees it next to the other
        failure modes (git pull rc, tag mismatch, script rc).
        """
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/fake/repo",
            branch="main",
            update_script=None,
            git_pull_rc=0,
            actual_branch="release",
            branch_check_rc=0,
        )
        rendered = admin.format_update_result(result)
        assert "branch mismatch" in rendered
        assert "'main'" in rendered
        assert "'release'" in rendered
        assert "== FAILED ==" in rendered
