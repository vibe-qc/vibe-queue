"""Tests for vq.admin / `vq admin update <env>` (v0.5.20)."""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import admin, config, paths
from vq.cli import main
from vq.pause_resume import PauseError


class _ProofStub:
    """Minimal pause/resume proof accepted by the admin lifecycle."""

    def __init__(
        self,
        summary: str,
        *,
        pause_error: BaseException | None = None,
        resume_error: BaseException | None = None,
    ) -> None:
        self.summary = summary
        self.pause_error = pause_error
        self.resume_error = resume_error

    def require_quiescent(self) -> None:
        if self.pause_error is not None:
            raise self.pause_error

    def require_clear(self) -> None:
        if self.resume_error is not None:
            raise self.resume_error

# ----------------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------------


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """vq state + config dirs in tmp_path. Pause/resume use this state
    via paths.queue_dir() to walk JobSpec files; the admin tests don't
    need real running jobs because pause_all/resume_all degrade
    cleanly to "0 jobs" when the queue dir is empty."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _make_git_repo(path: Path) -> Path:
    """Make ``path`` look like a git checkout (just enough for
    `git_dir` validation; we never actually run git on it because
    subprocess.run is mocked)."""
    path.mkdir(parents=True, exist_ok=True)
    (path / ".git").mkdir()
    return path


def _write_venv_program(
    cfg_dir: Path,
    name: str = "vibeqc-dev",
    *,
    python: str = "/fake/python",
    git_dir: str | None = None,
    update_script: str | None = "scripts/update-dev.sh",
    post_update_script: str | None = None,
    branch: str | None = "main",
) -> None:
    """Write a config.toml with a single [programs.NAME] entry of kind=venv."""
    if git_dir is None:
        git_dir = str(cfg_dir.parent / "repo")
    lines = [
        f'[programs.{name}]',
        'kind = "venv"',
        f'python = "{python}"',
        f'git_dir = "{git_dir}"',
    ]
    if branch:
        lines.append(f'branch = "{branch}"')
    if update_script:
        lines.append(f'update_script = "{update_script}"')
    if post_update_script:
        lines.append(f'post_update_script = "{post_update_script}"')
    (cfg_dir / "config.toml").write_text("\n".join(lines) + "\n")


def _ok_proc(stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=0, stdout=stdout, stderr=stderr,
    )


def _fail_proc(rc: int, stdout: str = "", stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr=stderr,
    )


def _bash_calls(mock_run: object) -> list[tuple]:
    """Return only the ``bash <script>`` invocations recorded by the
    subprocess mock. Filters out git commands so v0.5.25's added
    diagnostic queries (git rev-parse / describe / status) don't
    confuse "did we run the update_script?" assertions."""
    return [
        c for c in mock_run.call_args_list  # type: ignore[attr-defined]
        if c.args and c.args[0] and c.args[0][0] == "bash"
    ]


def _assert_no_bash_invoked(mock_run: object) -> None:
    assert not _bash_calls(mock_run), (
        f"expected no bash calls but got: {_bash_calls(mock_run)}"
    )


class TestSchedulerHostUpdate:
    def test_successful_scheduler_update_verifies_helper_provenance_read_only(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sha = "a" * 40
        tree_sha256 = "12" * 32
        (state_dir / "cfg" / "config.toml").write_text(
            "[hosts.driver]\n"
            'ssh = "driver-ssh"\n'
            "\n"
            "[hosts.host_f]\n"
            'ssh = "host_f-login"\n'
            'remote_vq = "/opt/vq/bin/vq"\n'
            'scheduler = "pbs"\n'
            'scheduler_dialect = "torque"\n'
            'scratch_root = "/scratch"\n'
            'scheduler_driver = "driver"\n'
            'fleet_role = "managed"\n'
            'scheduler_update_command = "/home/USER/update-vq.sh"\n',
            encoding="utf-8",
        )
        cfg = config.load_config()
        marker_calls: list[tuple[str, tuple[str, ...]]] = []

        monkeypatch.setattr(admin, "SCHEDULER_HELPER_READINESS_INTERVAL_SECONDS", 0)

        def fake_stage(host, host_cfg, command_host_cfg, result, *, expected_sha=None):
            result.stage_root = "/shared/vq-admin/host_f"
            result.stage_path = (
                f"{result.stage_root}/generations/{sha}-{'1' * 32}"
            )
            result.stage_uploaded = True
            result.expected_source_sha = sha
            result.expected_source_tree_sha256 = tree_sha256
            return result.stage_path

        monkeypatch.setattr(admin, "_stage_scheduler_helper_source", fake_stage)
        monkeypatch.setattr(
            admin.transport,
            "run_remote_shell",
            lambda *args, **kwargs: _ok_proc(stdout="updated\n"),
        )

        def fake_remote_vq(host_cfg, *args, **kwargs):
            marker_calls.append((host_cfg.ssh, args))
            if args == ("--version",):
                return _ok_proc(stdout="vq 0.12.0\n")
            if args == ("source-tree-sha256",):
                return _ok_proc(stdout=f"{tree_sha256}\n")
            if args == ("source-sha",):
                return _ok_proc(stdout=f"{sha}\n")
            assert args[0] == "source-stage-prune"
            return _ok_proc(stdout='{"removed": []}\n')

        monkeypatch.setattr(admin.transport, "run_remote_vq", fake_remote_vq)

        result = admin.update_scheduler_host("host_f", cfg)

        assert result.success is True
        assert result.expected_source_sha == sha
        assert result.remote_source_sha == sha
        assert result.remote_source_tree_sha256 == tree_sha256
        assert result.source_marker_rc == 0
        assert marker_calls == [
            ("host_f-login", ("--version",)),
            ("host_f-login", ("--version",)),
            ("host_f-login", ("source-tree-sha256",)),
            ("host_f-login", ("source-sha",)),
        ]


# ----------------------------------------------------------------------
# Input validation (AdminError paths)
# ----------------------------------------------------------------------


class TestUpdateEnvInputValidation:
    def test_unknown_env_raises(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="unknown env 'ghost'"):
            admin.update_env("ghost", cfg, host="localhost")

    def test_binary_program_rejected(self, state_dir: Path) -> None:
        """admin update only handles git-backed venv programs; a binary
        program (CRYSTAL, ORCA) has no git_dir to pull from."""
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/fake/crystal"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="only kind=\"venv\""):
            admin.update_env("crystal", cfg, host="localhost")

    def test_import_program_rejected(self, state_dir: Path) -> None:
        """import programs (pyscf etc.) live inside another venv; the
        parent venv is the right thing to refresh."""
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.pyscf]\n'
            'kind = "import"\n'
            'python = "/fake/python"\n'
            'import_check = "pyscf"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="only kind=\"venv\""):
            admin.update_env("pyscf", cfg, host="localhost")

    def test_missing_git_dir_rejected(self, state_dir: Path) -> None:
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(state_dir / "does-not-exist"),
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="not a directory"):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

    def test_not_a_git_checkout_rejected(self, state_dir: Path) -> None:
        plain_dir = state_dir / "plain"
        plain_dir.mkdir()  # exists but has no .git/
        _write_venv_program(state_dir / "cfg", git_dir=str(plain_dir))
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="not a git checkout"):
            admin.update_env("vibeqc-dev", cfg, host="localhost")


# ----------------------------------------------------------------------
# Happy path + failure modes (with subprocess mocking)
# ----------------------------------------------------------------------


class TestUpdateEnvExecution:
    @pytest.fixture(autouse=True)
    def _plain_update_argv(
        self,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
    ) -> None:
        """Keep generic subprocess-shape tests independent of host OS.

        Linux production hosts prepend nice/ionice; their dedicated tests
        below exercise that policy explicitly.
        """
        if "niceness" not in request.node.name:
            monkeypatch.setattr(admin, "_build_niceness_prefix", lambda: [])

    def test_happy_path_runs_pull_and_script(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        # Stage the update_script on disk (admin.py checks existence
        # before running bash).
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()
        monkeypatch.setenv("VQ_UPDATE_SCRIPT_TIMEOUT", "21600")
        monkeypatch.setenv("VQ_BUILD_STALL_TIMEOUT", "7200")

        responses = [
            _ok_proc(stdout="Already up to date.\n"),  # git pull
            _ok_proc(stdout="Build OK\n"),             # bash update_script
        ]
        with patch("vq.admin.subprocess.run", side_effect=responses) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success
        assert result.git_pull_rc == 0
        assert result.update_script_rc == 0
        assert "Already up to date" in result.git_pull_output
        assert "Build OK" in result.update_script_output
        assert result.run_log_path is not None
        assert (
            "# effective update timeouts: wall=21600s stall=7200s"
            in Path(result.run_log_path).read_text()
        )
        # First call was git pull
        first_call = m.call_args_list[0]
        assert first_call.args[0][0] == "git"
        assert first_call.args[0][1] == "-C"
        # Second was bash <script>
        second_call = m.call_args_list[1]
        assert second_call.args[0][0] == "bash"
        assert second_call.args[0][1].endswith("scripts/update-dev.sh")

    def test_no_update_script_runs_only_pull(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run", side_effect=[_ok_proc()]
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success
        assert result.update_script_rc is None  # never ran
        assert result.update_script_output == ""
        # 1 explicit call (git pull); the v0.5.25 admin-status SHA query
        # also runs but consumes the StopIteration silently via the
        # best-effort exception catch in _query_git_sha.
        _assert_no_bash_invoked(m)

    def test_git_pull_failure_skips_script(self, state_dir: Path) -> None:
        """If git pull rc != 0, the checkout is in a bad state; running
        the update_script against it would either fail or silently
        succeed against stale code. Skip the script."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1, stderr="fatal: ...")],
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.git_pull_rc == 1
        assert result.update_script_rc is None  # didn't run
        # update_script (bash) must not have been invoked.
        _assert_no_bash_invoked(m)

    def test_update_script_failure_records_rc(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(),
                _fail_proc(2, stderr="compilation failed"),
            ],
        ):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.git_pull_rc == 0
        assert result.update_script_rc == 2
        assert "compilation failed" in result.update_script_output

    def test_post_update_script_runs_after_update_script(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        (repo / "scripts" / "setup_basis_library.sh").write_text(
            "#!/bin/bash\n"
        )
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            post_update_script="scripts/setup_basis_library.sh",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="Already up to date.\n"),
                _ok_proc(stdout="Build OK\n"),
                _ok_proc(stdout="Basis library installed\n"),
            ],
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success
        assert result.update_script_rc == 0
        assert result.post_update_script_rc == 0
        assert "Basis library installed" in result.post_update_script_output
        bash_calls = _bash_calls(m)
        assert len(bash_calls) == 2
        assert bash_calls[0].args[0][1].endswith("scripts/update-dev.sh")
        assert bash_calls[1].args[0][1].endswith(
            "scripts/setup_basis_library.sh"
        )

    def test_post_update_script_runs_without_update_script(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "setup_basis_library.sh").write_text(
            "#!/bin/bash\n"
        )
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script=None,
            post_update_script="scripts/setup_basis_library.sh",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="Already up to date.\n"),
                _ok_proc(stdout="Basis library installed\n"),
            ],
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success
        assert result.update_script_rc is None
        assert result.post_update_script_rc == 0
        bash_calls = _bash_calls(m)
        assert len(bash_calls) == 1
        assert bash_calls[0].args[0][1].endswith(
            "scripts/setup_basis_library.sh"
        )

    def test_update_script_failure_skips_post_update_script(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        (repo / "scripts" / "setup_basis_library.sh").write_text(
            "#!/bin/bash\n"
        )
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            post_update_script="scripts/setup_basis_library.sh",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="Already up to date.\n"),
                _fail_proc(2, stderr="compilation failed"),
            ],
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.update_script_rc == 2
        assert result.post_update_script_rc is None
        bash_calls = _bash_calls(m)
        assert len(bash_calls) == 1
        assert bash_calls[0].args[0][1].endswith("scripts/update-dev.sh")

    def test_post_update_script_failure_fails_update(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        (repo / "scripts" / "setup_basis_library.sh").write_text(
            "#!/bin/bash\n"
        )
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            post_update_script="scripts/setup_basis_library.sh",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="Already up to date.\n"),
                _ok_proc(stdout="Build OK\n"),
                _fail_proc(3, stderr="basis install failed"),
            ],
        ):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.post_update_script_rc == 3
        assert "basis install failed" in result.post_update_script_output
        rendered = admin.format_update_result(result)
        assert "post_update_script rc=3" in rendered

    def test_update_script_missing_recorded_as_work_error(
        self, state_dir: Path
    ) -> None:
        """Update_script configured but the file doesn't exist on disk.
        Bash would print "No such file or directory"; we catch this in
        admin.py before invoking bash and record a work_error."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/never-existed.sh",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run", side_effect=[_ok_proc()]
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.git_pull_rc == 0
        assert result.update_script_rc is None
        assert len(result.work_errors) == 1
        assert "not found" in result.work_errors[0]
        # bash never invoked (we checked the path on disk first)
        _assert_no_bash_invoked(m)

    def test_git_pull_timeout_recorded(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="git pull", timeout=300),
        ):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.git_pull_rc is None
        assert any("timed out" in e for e in result.work_errors)

    # ------------------------------------------------------------------
    # v0.5.39: update_script accepts args via shlex.split. Lets a single
    # script (e.g. vibe-qc's scripts/update.sh) cover both `--dev` and
    # `--release` paths without each consumer needing a thin wrapper.
    # ------------------------------------------------------------------

    def test_update_script_with_args_invokes_bash_with_argv(
        self, state_dir: Path
    ) -> None:
        """``update_script = "scripts/update.sh --dev"`` → bash gets
        ["bash", "<git_dir>/scripts/update.sh", "--dev"] argv.

        Captures the actual argv passed to subprocess.run and asserts
        the args are forwarded as separate arguments (NOT as one big
        string that would have ended up as bash's $1)."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\necho ok\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh --dev",
        )
        cfg = config.load_config()

        captured_argvs: list[list[str]] = []

        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captured_argvs.append(list(argv))
            return _ok_proc(stdout="rebuilt OK\n")

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is True
        # Find the bash invocation (the git-pull invocation runs git, not bash)
        bash_calls = [a for a in captured_argvs if a and a[0] == "bash"]
        assert len(bash_calls) == 1, captured_argvs
        bash_argv = bash_calls[0]
        # bash, <abs path to script>, then args
        assert bash_argv[0] == "bash"
        assert bash_argv[1].endswith("/scripts/update.sh")
        assert bash_argv[2:] == ["--dev"]

    def test_update_script_multi_arg(self, state_dir: Path) -> None:
        """Multiple args after the script path forward as separate
        argv elements. Quoting via shlex rules works too."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh --dev --rebuild-native-deps",
        )
        cfg = config.load_config()

        captured_argvs: list[list[str]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captured_argvs.append(list(argv))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_calls = [a for a in captured_argvs if a and a[0] == "bash"]
        assert bash_calls[0][2:] == ["--dev", "--rebuild-native-deps"]

    def test_update_script_legacy_single_path_form_still_works(
        self, state_dir: Path
    ) -> None:
        """Belt-and-braces backward-compat: a bare path with no args
        (the legacy v0.5.20 form) still invokes bash with just the
        script path, no argv noise."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        captured_argvs: list[list[str]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captured_argvs.append(list(argv))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_calls = [a for a in captured_argvs if a and a[0] == "bash"]
        # No args after the script path
        assert len(bash_calls[0]) == 2
        assert bash_calls[0][1].endswith("/scripts/update.sh")

    def test_update_script_whitespace_only_is_recorded_as_work_error(
        self, state_dir: Path
    ) -> None:
        """Pathological config (``update_script = "   "``) shouldn't
        silently no-op or shlex-split into nothing. Record as a work
        error so the operator notices."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="   ",
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run", side_effect=[_ok_proc()]
        ):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is False
        assert result.update_script_rc is None
        assert any("empty" in e for e in result.work_errors)

    # ------------------------------------------------------------------
    # v0.5.40: CMAKE_BUILD_PARALLEL_LEVEL cap on the build env. Caps
    # ninja parallelism so vibe-qc's template-heavy translation units
    # don't peak past host RAM and trigger global OOM (concrete bug:
    # host_d hung 2026-05-16 from 32 concurrent cc1plus on 125 GB box).
    # ------------------------------------------------------------------

    def test_safe_build_parallelism_formula(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """v0.5.41 heuristic: min(nproc, max(2, mem_mb // 15000), 6).
        Verifies all four regimes — RAM-bound, nproc-bound, the
        2-worker minimum floor, and the v0.5.41 hard cap of 6 — by
        faking /proc/meminfo + os.cpu_count."""
        # Helper to fake the meminfo read
        def fake_meminfo(mem_kb: int) -> None:
            meminfo = tmp_path / "meminfo"
            meminfo.write_text(f"MemTotal:       {mem_kb} kB\n")
            import builtins
            orig_open = builtins.open
            def fake_open(path, *a, **kw):  # type: ignore[no-untyped-def]
                if str(path) == "/proc/meminfo":
                    return orig_open(meminfo, *a, **kw)
                return orig_open(path, *a, **kw)
            monkeypatch.setattr(builtins, "open", fake_open)

        # host_d-like: 32 threads, 125 GB → hard-cap at 6.
        # (125*1024 MB) // 15000 = 8, but min(..., 6) wins.
        fake_meminfo(125 * 1024 * 1024)
        monkeypatch.setattr(admin.os, "cpu_count", lambda: 32)
        assert admin._safe_build_parallelism() == 6

        # host_a-like: 16 threads, 62 GB → RAM-bound at 4.
        # (62*1024 MB) // 15000 = 4.
        fake_meminfo(62 * 1024 * 1024)
        monkeypatch.setattr(admin.os, "cpu_count", lambda: 16)
        assert admin._safe_build_parallelism() == 4

        # nproc-bound: 3 threads, 64 GB → nproc=3 wins over RAM=4 and cap=6.
        fake_meminfo(64 * 1024 * 1024)
        monkeypatch.setattr(admin.os, "cpu_count", lambda: 3)
        assert admin._safe_build_parallelism() == 3

        # Floor: 8 threads, 4 GB → max(2, 0) = 2 minimum.
        fake_meminfo(4 * 1024 * 1024)
        monkeypatch.setattr(admin.os, "cpu_count", lambda: 8)
        assert admin._safe_build_parallelism() == 2

        # v0.5.41 hard cap regression: 64 threads, 256 GB → still 6,
        # not 17 (256000//15000). The cap exists because above 6
        # workers ninja serializes on link/IO contention, not because
        # of any memory shortfall on a monster box.
        fake_meminfo(256 * 1024 * 1024)
        monkeypatch.setattr(admin.os, "cpu_count", lambda: 64)
        assert admin._safe_build_parallelism() == 6

    def test_safe_build_parallelism_no_proc_meminfo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """macOS / hosts without /proc/meminfo: return None so the
        caller leaves env unchanged (system default wins)."""
        import builtins
        orig_open = builtins.open
        def fake_open(path, *a, **kw):  # type: ignore[no-untyped-def]
            if str(path) == "/proc/meminfo":
                raise FileNotFoundError("/proc/meminfo")
            return orig_open(path, *a, **kw)
        monkeypatch.setattr(builtins, "open", fake_open)
        assert admin._safe_build_parallelism() is None

    @staticmethod
    def _bash_env(captures: list[tuple[list[str], dict[str, str] | None]]) -> dict[str, str]:
        """Find the env dict passed to the bash invocation among captured
        subprocess.run calls. Filters out the git pull / describe / SHA
        query side-channel calls that admin.update_env also makes.

        v0.5.41: ``bash`` may be preceded by ``nice``/``ionice`` in
        argv, so we search the whole argv list rather than just
        argv[0]."""
        for argv, env in captures:
            if argv and "bash" in argv:
                return dict(env or {})
        raise AssertionError(
            f"no bash invocation found among captured calls: {[a for a, _ in captures]}"
        )

    @staticmethod
    def _bash_argv(captures: list[tuple[list[str], dict[str, str] | None]]) -> list[str]:
        """v0.5.41: the full captured argv for the bash invocation, so
        tests can assert on the nice/ionice prefix shape."""
        for argv, _ in captures:
            if argv and "bash" in argv:
                return list(argv)
        raise AssertionError(
            f"no bash invocation found among captured calls: {[a for a, _ in captures]}"
        )

    def test_update_script_injects_parallelism_cap_when_unset(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the calling env has no CMAKE_BUILD_PARALLEL_LEVEL,
        vq computes one and injects it. Asserts the env dict passed
        to the bash subprocess.run carries the cap."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        monkeypatch.setattr(admin, "_safe_build_parallelism", lambda: 8)
        monkeypatch.delenv("CMAKE_BUILD_PARALLEL_LEVEL", raising=False)

        captures: list[tuple[list[str], dict[str, str] | None]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captures.append((list(argv), kw.get("env")))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_env = self._bash_env(captures)
        assert bash_env.get("CMAKE_BUILD_PARALLEL_LEVEL") == "8"
        assert bash_env.get("GIT_OPTIONAL_LOCKS") == "0"

    def test_update_script_honors_pre_existing_parallelism_env(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the caller's environment already has
        CMAKE_BUILD_PARALLEL_LEVEL, vq DOES NOT override — explicit
        user intent wins. Useful when an operator deliberately wants
        a different cap for their host."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        monkeypatch.setattr(admin, "_safe_build_parallelism", lambda: 8)
        monkeypatch.setenv("CMAKE_BUILD_PARALLEL_LEVEL", "4")

        captures: list[tuple[list[str], dict[str, str] | None]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captures.append((list(argv), kw.get("env")))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_env = self._bash_env(captures)
        # User's "4" preserved, NOT replaced with the computed "8".
        assert bash_env.get("CMAKE_BUILD_PARALLEL_LEVEL") == "4"

    def test_update_script_no_cap_when_meminfo_unreadable(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On macOS dev / hosts where _safe_build_parallelism returns
        None, vq leaves the env untouched and trusts the system
        default. Verified by asserting CMAKE_BUILD_PARALLEL_LEVEL is
        NOT in the bash subprocess env."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        monkeypatch.setattr(admin, "_safe_build_parallelism", lambda: None)
        monkeypatch.delenv("CMAKE_BUILD_PARALLEL_LEVEL", raising=False)

        captures: list[tuple[list[str], dict[str, str] | None]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captures.append((list(argv), kw.get("env")))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_env = self._bash_env(captures)
        assert "CMAKE_BUILD_PARALLEL_LEVEL" not in bash_env

    # ------------------------------------------------------------------
    # v0.5.41: nice + ionice argv prefix on Linux update_script invocations.
    # v0.5.40's cap stopped the global-OOM hang but 12 cc1plus workers still
    # made the box unresponsive. The argv prefix keeps the build at idle
    # CPU+IO priority so the foreground shell stays snappy during a rebuild.
    # ------------------------------------------------------------------

    def test_build_niceness_prefix_on_linux_with_both_tools(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """When /proc/meminfo exists AND both nice + ionice are on
        PATH, the prefix is the full ``nice -n 19 ionice -c 3``. This
        is the host_d / host_a baseline."""
        # Fake /proc/meminfo by intercepting Path.exists for that path
        def fake_exists(self: Path) -> bool:
            if str(self) == "/proc/meminfo":
                return True
            return (
                Path.exists.__wrapped__(self)
                if hasattr(Path.exists, "__wrapped__")
                else os.path.exists(str(self))
            )
        monkeypatch.setattr(
            admin.Path,
            "exists",
            lambda self: True if str(self) == "/proc/meminfo" else os.path.exists(str(self)),
        )
        monkeypatch.setattr(
            admin.shutil,
            "which",
            lambda name: f"/usr/bin/{name}" if name in ("nice", "ionice") else None,
        )
        assert admin._build_niceness_prefix() == ["nice", "-n", "19", "ionice", "-c", "3"]

    def test_build_niceness_prefix_on_linux_missing_ionice(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Some minimal Linux images ship ``nice`` (POSIX) but not
        ``ionice`` (util-linux). Skip the missing tool, keep the
        available one — partial degrade is fine."""
        monkeypatch.setattr(
            admin.Path,
            "exists",
            lambda self: True if str(self) == "/proc/meminfo" else os.path.exists(str(self)),
        )
        monkeypatch.setattr(
            admin.shutil,
            "which",
            lambda name: "/usr/bin/nice" if name == "nice" else None,
        )
        assert admin._build_niceness_prefix() == ["nice", "-n", "19"]

    def test_build_niceness_prefix_off_on_macos(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No /proc/meminfo → returns []. macOS dev boxes typically
        *want* the build to use full CPU, and ``ionice`` doesn't
        exist there anyway. Empty prefix = argv unchanged."""
        monkeypatch.setattr(
            admin.Path,
            "exists",
            lambda self: False if str(self) == "/proc/meminfo" else os.path.exists(str(self)),
        )
        # Even if shutil.which would find nice, we short-circuit on the
        # /proc/meminfo gate.
        monkeypatch.setattr(admin.shutil, "which", lambda name: "/usr/bin/nice")
        assert admin._build_niceness_prefix() == []

    def test_update_script_prepends_niceness_argv(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end: when the niceness prefix is non-empty,
        ``_run_update_script`` prepends it to the bash argv. Verified
        by capturing the actual argv passed to subprocess.run."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        monkeypatch.setattr(
            admin, "_build_niceness_prefix",
            lambda: ["nice", "-n", "19", "ionice", "-c", "3"],
        )

        captures: list[tuple[list[str], dict[str, str] | None]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captures.append((list(argv), kw.get("env")))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_argv = self._bash_argv(captures)
        # argv shape: [nice, -n, 19, ionice, -c, 3, bash, <script>]
        assert bash_argv[:6] == ["nice", "-n", "19", "ionice", "-c", "3"]
        assert bash_argv[6] == "bash"
        assert bash_argv[7].endswith("scripts/update.sh")

    def test_update_script_no_niceness_argv_on_macos(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When the niceness prefix is empty (macOS / missing tools),
        the bash invocation has no extra prefix — argv starts with
        ``bash``. Otherwise we'd regress macOS dev-loop perf for no
        gain."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()

        monkeypatch.setattr(admin, "_build_niceness_prefix", lambda: [])

        captures: list[tuple[list[str], dict[str, str] | None]] = []
        def fake_run(argv, *a, **kw):  # type: ignore[no-untyped-def]
            captures.append((list(argv), kw.get("env")))
            return _ok_proc()

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        bash_argv = self._bash_argv(captures)
        assert bash_argv[0] == "bash"


class TestResumeAlwaysRuns:
    """The try/finally invariant: pause_all happens before any work;
    resume_all MUST happen after, even if work raises. This is what
    keeps a Ctrl-C between pause and pull from stranding paused jobs."""

    def test_resume_runs_even_when_subprocess_raises(
        self, state_dir: Path
    ) -> None:
        """If subprocess.run raises an unhandled exception (something
        we don't catch — e.g. OSError that isn't FileNotFoundError),
        resume_all still gets called from the finally block."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        # Track that pause + resume both got hit. (We can't use the
        # actual pause_all/resume_all summaries because there are no
        # real jobs; they return "0 jobs" but the call still happened.)
        pause_calls: list[str] = []
        resume_calls: list[str] = []

        from vq import admin as admin_mod

        orig_pause = admin_mod.pause_token_scope_with_proof
        orig_resume = admin_mod.resume_token_scope_with_proof

        # v0.6.42: admin update threads multi_user= into pause/resume.
        def _spy_pause(host: str, token: str, **kw: object) -> object:
            pause_calls.append(host)
            return orig_pause(host, token, **kw)

        def _spy_resume(host: str, token: str, **kw: object) -> object:
            resume_calls.append(host)
            return orig_resume(host, token, **kw)

        with patch.object(
                 admin_mod, "pause_token_scope_with_proof", _spy_pause,
             ), \
             patch.object(
                 admin_mod, "resume_token_scope_with_proof", _spy_resume,
             ), \
             patch(
                 "vq.admin.subprocess.run",
                 side_effect=RuntimeError("boom"),
             ), \
             pytest.raises(RuntimeError, match="boom"):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        # Both got called, in order.
        assert pause_calls == ["localhost"]
        assert resume_calls == ["localhost"]


# ----------------------------------------------------------------------
# v0.5.42: vq self-update auto-restart of vq-daemon
#
# Cf. the 2026-05-16 stale-code post-mortem in operations.md: vq is
# editable-installed, so `pip install -e .` lands new code on disk but
# the running daemon keeps the old module objects in memory until it
# restarts. v0.5.42 makes that restart automatic when admin update IS
# updating vq's own venv.
# ----------------------------------------------------------------------


@pytest.mark.no_autopatch_self_update_probe
class TestSelfUpdateRestart:
    """Detection + restart wiring for `vq admin update <env>` when
    <env> is the venv vq-daemon was launched from."""

    # ------------------------------------------------------------------
    # _parse_execstart_path: extract `path=` from systemd's ExecStart
    # raw value
    # ------------------------------------------------------------------

    def test_parse_execstart_path_typical_value(self) -> None:
        """systemd's `systemctl show -p ExecStart --value` returns
        ``{ path=/...; argv[]=/...; ... ; pid=N ; ... }``. We pull the
        path field."""
        raw = (
            "{ path=/home/USER/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq ; "
            "argv[]=/home/USER/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq "
            "daemon run --max-cpus 12 --max-jobs 2 ; ignore_errors=no ; "
            "start_time=... ; pid=1234 ; code=0 ; status=0 }"
        )
        assert admin._parse_execstart_path(raw) == (
            "/home/USER/gitlab/vibeqc-queue/vibe-queue/.venv/bin/vq"
        )

    def test_parse_execstart_path_returns_none_on_empty(self) -> None:
        """No path= field (unit not loaded / unknown shape) → None,
        caller surfaces "could not parse" diagnostic."""
        assert admin._parse_execstart_path("") is None
        assert admin._parse_execstart_path("(no value)") is None

    # ------------------------------------------------------------------
    # _detect_vq_self_update: the verdict that drives the whole flow
    # ------------------------------------------------------------------

    def test_detect_systemctl_unavailable_sys_executable_matches(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """macOS / zombie user-systemd path: no systemctl. Fall back to
        comparing sys.executable to the env's venv bin dir. When they
        match, we BELIEVE it's a self-update but can't restart →
        manager_available=False, is_self_update=True. The caller surfaces
        the recovery recipe."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        python_path = venv_bin / "python"
        python_path.write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_path),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(admin, "_select_daemon_service_manager", lambda: None)
        monkeypatch.setattr(admin.sys, "executable", str(venv_bin / "python"))

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is True
        assert probe.manager_available is False
        assert "fallback to sys.executable" in probe.diagnostic

    def test_detect_systemctl_unavailable_sys_executable_mismatches(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """macOS path on a non-vq env (vibeqc-dev): sys.executable
        lives in vq's venv, not this env's venv → is_self_update=False.
        Daemon stays untouched."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        python_path = venv_bin / "python"
        python_path.write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_path),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(admin, "_select_daemon_service_manager", lambda: None)
        # sys.executable lives somewhere else entirely (vq's venv)
        monkeypatch.setattr(
            admin.sys, "executable",
            "/some/other/venv/bin/python",
        )

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is False
        assert probe.manager_available is False

    def test_detect_systemctl_ok_execstart_matches_venv(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The happy detection path on Linux: systemctl is reachable,
        ExecStart's path= field points at <env>/.venv/bin/vq → self-
        update verdict True, daemon_running True (MainPID > 0)."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        python_path = venv_bin / "python"
        python_path.write_text("")
        daemon_exe = venv_bin / "vq"
        daemon_exe.write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_path),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(
            admin,
            "_select_daemon_service_manager",
            lambda: admin._DaemonServiceManager.SYSTEMD,
        )
        execstart = (
            f"{{ path={daemon_exe} ; argv[]={daemon_exe} daemon run ; "
            f"ignore_errors=no }}"
        )
        monkeypatch.setattr(
            admin, "_query_daemon_execstart", lambda: (0, execstart),
        )
        monkeypatch.setattr(admin, "_query_daemon_mainpid", lambda: 1234)
        monkeypatch.setattr(
            admin,
            "_query_daemon_service_state",
            lambda manager: admin._DaemonServiceState(
                manager, True, 1234, str(daemon_exe), "test systemd service",
            ),
        )

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is True
        assert probe.daemon_running is True
        assert probe.service_manager == "systemd"
        assert probe.manager_available is True
        assert "self-update" in probe.diagnostic

    def test_detect_treats_venv_python_symlink_as_belonging_to_venv(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """v0.5.43 regression: in a real venv, ``bin/python`` is a
        symlink to the system interpreter (e.g.
        ``/usr/bin/python3.14``). v0.5.42's detection did
        ``Path(prog.python).resolve().parent`` which dereferenced the
        symlink and yielded ``/usr/bin`` — never matching the
        daemon's bin dir → auto-restart never fired on real fleet
        hosts. Fixed by taking ``.parent`` before ``.resolve()``.

        This test reproduces the real-venv shape: a python symlink
        whose target is OUTSIDE the venv, with vq as a regular
        sibling. Detection must still recognise the env as the one
        the daemon was launched from."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        # Mimic /usr/bin/python3.14 living outside the venv.
        sys_python = state_dir / "fake_sys" / "python3.14"
        sys_python.parent.mkdir(parents=True)
        sys_python.write_text("")
        python_link = venv_bin / "python"
        python_link.symlink_to(sys_python)
        daemon_exe = venv_bin / "vq"
        daemon_exe.write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_link),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(
            admin,
            "_select_daemon_service_manager",
            lambda: admin._DaemonServiceManager.SYSTEMD,
        )
        execstart = (
            f"{{ path={daemon_exe} ; argv[]={daemon_exe} daemon run ; "
            f"ignore_errors=no }}"
        )
        monkeypatch.setattr(
            admin, "_query_daemon_execstart", lambda: (0, execstart),
        )
        monkeypatch.setattr(admin, "_query_daemon_mainpid", lambda: 1234)
        monkeypatch.setattr(
            admin,
            "_query_daemon_service_state",
            lambda manager: admin._DaemonServiceState(
                manager, True, 1234, str(daemon_exe), "test systemd service",
            ),
        )

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is True, probe.diagnostic
        assert probe.manager_available is True
        assert probe.daemon_running is True

    def test_detect_fallback_handles_venv_python_symlink(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same v0.5.43 regression on the sys.executable fallback
        path (macOS / zombie user-systemd). ``sys.executable`` IS the
        venv python symlink, so the same resolve-then-parent bug
        applied. Verified by making ``sys.executable`` a symlink to
        an out-of-venv interpreter and asserting fallback detection
        still returns is_self_update=True."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        sys_python = state_dir / "fake_sys" / "python3.14"
        sys_python.parent.mkdir(parents=True)
        sys_python.write_text("")
        python_link = venv_bin / "python"
        python_link.symlink_to(sys_python)
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_link),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(admin, "_select_daemon_service_manager", lambda: None)
        monkeypatch.setattr(admin.sys, "executable", str(python_link))

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is True, probe.diagnostic
        assert probe.manager_available is False

    def test_detect_systemctl_ok_execstart_other_venv(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Updating vibeqc-dev while vq-daemon was launched from a
        different venv (vq's own venv) → is_self_update False, daemon
        untouched. The common case for non-self-update calls."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        python_path = venv_bin / "python"
        python_path.write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_path),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(
            admin,
            "_select_daemon_service_manager",
            lambda: admin._DaemonServiceManager.SYSTEMD,
        )
        other_execstart = (
            "{ path=/elsewhere/.venv/bin/vq ; "
            "argv[]=/elsewhere/.venv/bin/vq daemon run ; ignore_errors=no }"
        )
        monkeypatch.setattr(
            admin, "_query_daemon_execstart", lambda: (0, other_execstart),
        )
        monkeypatch.setattr(admin, "_query_daemon_mainpid", lambda: 4321)
        monkeypatch.setattr(
            admin,
            "_query_daemon_service_state",
            lambda manager: admin._DaemonServiceState(
                manager,
                True,
                4321,
                "/elsewhere/.venv/bin/vq",
                "test systemd service",
            ),
        )

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is False
        assert probe.manager_available is True
        assert "different venv" in probe.diagnostic

    def test_detect_systemctl_ok_unit_missing(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemctl is reachable but `show vq-daemon` returns rc!=0
        (unit not installed). Not a self-update we can act on; report
        the failed rc in the diagnostic for ops debugging."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        (venv_bin / "python").write_text("")
        _write_venv_program(
            state_dir / "cfg",
            python=str(venv_bin / "python"),
            git_dir=str(repo),
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]

        monkeypatch.setattr(
            admin,
            "_select_daemon_service_manager",
            lambda: admin._DaemonServiceManager.SYSTEMD,
        )
        monkeypatch.setattr(
            admin, "_query_daemon_execstart",
            lambda: (4, "Unit vq-daemon.service not found."),
        )
        monkeypatch.setattr(
            admin,
            "_query_daemon_service_state",
            lambda manager: admin._DaemonServiceState(
                manager,
                False,
                None,
                None,
                "systemctl show vq-daemon rc=4: unit not found",
            ),
        )

        probe = admin._detect_vq_self_update(prog)
        assert probe.is_self_update is False
        assert probe.daemon_running is False
        assert "rc=4" in probe.diagnostic

    # ------------------------------------------------------------------
    # _restart_vq_daemon: systemctl --user restart vq-daemon
    # ------------------------------------------------------------------

    def test_restart_success_reports_pid_transition(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Happy restart: subprocess.run returns rc=0, MainPID changes
        from 1234 → 5678. Message includes the PID transition so the
        user can confirm visually."""
        monkeypatch.setattr(admin, "_query_daemon_mainpid", lambda: 5678)
        with patch(
            "vq.admin.subprocess.run",
            return_value=_ok_proc(),
        ) as mock_run:
            ok, msg = admin._restart_vq_daemon(pre_pid=1234)
        assert ok is True
        assert "PID 1234 -> 5678" in msg
        # systemctl was actually invoked with restart
        assert mock_run.call_args.args[0] == [
            "systemctl", "--user", "restart", "vq-daemon",
        ]

    def test_restart_failure_points_at_recovery_recipe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemctl --user restart returns rc=1 (e.g. zombie user-
        systemd, connection refused). Message points at operations.md
        rather than dumping raw stderr — the recovery recipe is more
        actionable than the error string."""
        with patch(
            "vq.admin.subprocess.run",
            return_value=_fail_proc(
                1, stderr="Failed to connect to user scope bus...",
            ),
        ):
            ok, msg = admin._restart_vq_daemon(pre_pid=1234)
        assert ok is False
        assert "rc=1" in msg
        assert "operations.md" in msg

    def test_restart_timeout_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """systemctl --user restart hangs past TimeoutStopSec
        (e.g. a job's scope cgroup is ignoring SIGTERM). Surface the
        timeout + operations.md recipe pointer."""
        with patch(
            "vq.admin.subprocess.run",
            side_effect=subprocess.TimeoutExpired(
                cmd=["systemctl"], timeout=120,
            ),
        ):
            ok, msg = admin._restart_vq_daemon(pre_pid=1234)
        assert ok is False
        assert "timed out" in msg
        assert "operations.md" in msg

    # ------------------------------------------------------------------
    # _maybe_restart_daemon: end-to-end wiring from update_env
    # ------------------------------------------------------------------

    def _setup_self_update_env(
        self, state_dir: Path
    ) -> tuple[config.Config, config.VenvProgram, Path]:
        """Build a venv-program config where prog.python is inside the
        env's checkout/.venv/bin/. Returns (cfg, prog, venv_bin)."""
        repo = _make_git_repo(state_dir / "repo")
        venv_bin = repo / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        python_path = venv_bin / "python"
        python_path.write_text("")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update.sh").write_text("#!/bin/bash\n")
        _write_venv_program(
            state_dir / "cfg",
            python=str(python_path),
            git_dir=str(repo),
            update_script="scripts/update.sh",
        )
        cfg = config.load_config()
        prog = cfg.programs["vibeqc-dev"]
        return cfg, prog, venv_bin

    def test_no_restart_when_flag_disabled(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`--no-restart-daemon` short-circuits before detection. Even
        on a vq self-update the daemon stays untouched."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        )
        # Spy on detection — it should NOT be called at all.
        called: list[str] = []
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: (called.append("probed"), None)[1],  # type: ignore[func-returns-value]
        )

        admin._maybe_restart_daemon(prog, result, restart_daemon=False)

        assert called == []
        assert result.daemon_restart_attempted is False
        assert "--no-restart-daemon" in result.daemon_restart_message

    def test_no_restart_when_update_failed(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If the update itself failed (rc!=0 on git or script), never
        restart — restarting onto a half-installed package is worse
        than running stale code."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=128,  # failure
        )
        called: list[str] = []
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: (called.append("probed"), None)[1],  # type: ignore[func-returns-value]
        )

        admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        assert called == []
        assert result.daemon_restart_attempted is False
        assert "half-installed" in result.daemon_restart_message

    def test_no_restart_when_not_self_update(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Updating vibeqc-dev when daemon runs from a different venv
        → quiet no-op (no `daemon_restart_attempted` flag, no
        systemctl invocation)."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        )
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: admin._SelfUpdateProbe(
                is_self_update=False, daemon_running=True,
                service_manager="systemd", manager_available=True,
                diagnostic="diff venv",
            ),
        )

        with patch("vq.admin.subprocess.run") as mock_run:
            admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        # No restart was invoked
        assert mock_run.call_count == 0
        assert result.daemon_restart_attempted is False
        assert "not a vq self-update" in result.daemon_restart_message

    def test_restart_attempted_on_self_update_happy_path(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Self-update detected + systemctl reachable + daemon running
        → restart attempted, succeeds, fields populated. The end-to-end
        happy case."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        )
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: admin._SelfUpdateProbe(
                is_self_update=True, daemon_running=True,
                service_manager="systemd", manager_available=True,
                diagnostic="ok",
            ),
        )
        # MainPID query before + after restart (both succeed).
        pid_queries = iter([1234, 5678])
        monkeypatch.setattr(
            admin, "_query_daemon_service_pid",
            lambda manager: next(pid_queries),
        )
        expected_sha = "a" * 40
        monkeypatch.setattr(admin, "current_source_sha", lambda path=None: expected_sha)
        monkeypatch.setattr(
            admin,
            "_verify_restarted_daemon",
            lambda sha, *, expected_tree_sha256=None: admin.DaemonProvenance(
                verified=True,
                actual_sha=expected_sha,
                actual_tree_sha256=None,
                detail=f"RPC healthy; source SHA {expected_sha} verified",
            ),
        )
        with patch(
            "vq.admin.subprocess.run", return_value=_ok_proc(),
        ) as mock_run:
            admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        assert result.daemon_restart_attempted is True
        assert result.daemon_restart_succeeded is True
        assert "PID 1234 -> 5678" in result.daemon_restart_message
        # systemctl restart was actually called
        assert any(
            list(c.args[0])[:4] == [
                "systemctl", "--user", "restart", "vq-daemon",
            ]
            for c in mock_run.call_args_list
        )
        # And the update's own success now includes the daemon restart
        assert result.success is True

    def test_self_update_but_systemctl_unreachable_fails_loud(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Worst case: detection (via sys.executable fallback) says
        self-update, but systemctl can't reach the user manager. We
        can't restart, the daemon is stuck on stale code — surface
        non-zero with the recovery recipe pointer."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        )
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: admin._SelfUpdateProbe(
                is_self_update=True, daemon_running=None,
                service_manager=None, manager_available=False,
                diagnostic="systemctl --user unavailable",
            ),
        )

        admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        assert result.daemon_restart_attempted is True
        assert result.daemon_restart_succeeded is False
        assert "operations.md" in result.daemon_restart_message
        # success now flips to False so the CLI exits non-zero
        assert result.success is False

    def test_self_update_starts_and_verifies_inactive_service(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An inactive managed service must still restart and pass RPC/SHA."""
        _, prog, _ = self._setup_self_update_env(state_dir)
        result = admin.UpdateResult(
            env="vibeqc-dev", git_dir=str(prog.git_dir),
            branch="main", update_script="scripts/update.sh",
            git_pull_rc=0, update_script_rc=0,
        )
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda p: admin._SelfUpdateProbe(
                is_self_update=True, daemon_running=False,
                service_manager="systemd", manager_available=True,
                diagnostic="ok",
            ),
        )

        expected_sha = "a" * 40
        monkeypatch.setattr(admin, "_query_daemon_service_pid", lambda manager: None)
        monkeypatch.setattr(admin, "current_source_sha", lambda path=None: expected_sha)
        monkeypatch.setattr(
            admin,
            "_restart_vq_daemon",
            lambda **kwargs: (True, "service started"),
        )
        monkeypatch.setattr(
            admin,
            "_verify_restarted_daemon",
            lambda sha, *, expected_tree_sha256=None: admin.DaemonProvenance(
                verified=True,
                actual_sha=expected_sha,
                actual_tree_sha256=None,
                detail="RPC/SHA verified",
            ),
        )

        admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        assert result.daemon_restart_attempted is True
        assert result.daemon_health_verified is True
        assert result.success is True

    # ------------------------------------------------------------------
    # CLI: --no-restart-daemon flag plumbing
    # ------------------------------------------------------------------

    def test_cli_no_restart_daemon_flag_passes_through(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq admin update <env> --no-restart-daemon` reaches
        `update_env` with restart_daemon=False. Verified by capturing
        the kwargs passed to the admin module."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )

        captured: dict[str, object] = {}

        def fake_update_env(env, cfg, **kw):  # type: ignore[no-untyped-def]
            captured.update(kw)
            return admin.UpdateResult(
                env=env, git_dir=str(repo),
                branch="main", update_script=None,
                git_pull_rc=0,
            )

        from vq import cli as cli_mod
        monkeypatch.setattr(
            cli_mod.admin_module, "update_env", fake_update_env,
        )
        result = CliRunner().invoke(
            main,
            ["admin", "update", "vibeqc-dev", "--no-restart-daemon"],
        )
        assert result.exit_code == 0, result.output
        assert captured.get("restart_daemon") is False

    def test_cli_default_is_restart_daemon_true(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without the flag, restart_daemon=True is the default. The
        whole feature is opt-out, not opt-in — the bug it fixes is
        important enough to be on by default."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )

        captured: dict[str, object] = {}

        def fake_update_env(env, cfg, **kw):  # type: ignore[no-untyped-def]
            captured.update(kw)
            return admin.UpdateResult(
                env=env, git_dir=str(repo),
                branch="main", update_script=None,
                git_pull_rc=0,
            )

        from vq import cli as cli_mod
        monkeypatch.setattr(
            cli_mod.admin_module, "update_env", fake_update_env,
        )
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev"],
        )
        assert result.exit_code == 0, result.output
        assert captured.get("restart_daemon") is True

    def test_cli_expected_sha_passes_through_to_managed_env(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`vq admin update ENV --expected-sha` reaches update_env for
        ordinary managed venv updates, not only scheduler-runtime updates."""
        repo = _make_git_repo(state_dir / "repo")
        sha = "a" * 40
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )

        captured: dict[str, object] = {}

        def fake_update_env(env, cfg, **kw):  # type: ignore[no-untyped-def]
            captured.update(kw)
            return admin.UpdateResult(
                env=env, git_dir=str(repo),
                branch="main", update_script=None,
                git_pull_rc=0,
                expected_sha=sha,
                actual_sha=sha,
                sha_check_rc=0,
            )

        from vq import cli as cli_mod
        monkeypatch.setattr(
            cli_mod.admin_module, "update_env", fake_update_env,
        )
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "--expected-sha", sha],
        )
        assert result.exit_code == 0, result.output
        assert captured.get("expected_sha") == sha
        assert "expected_sha" in result.output

    def test_cli_expected_sha_rejects_all_envs(
        self, state_dir: Path,
    ) -> None:
        sha = "a" * 40
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{state_dir / "repo"}"\n'
            'branch = "main"\n'
        )
        result = CliRunner().invoke(
            main, ["admin", "update", "--all", "--expected-sha", sha],
        )
        assert result.exit_code != 0
        assert "--all is mutually exclusive with --expected-sha" in result.output

    def test_formatter_includes_restart_banner_on_self_update(
        self,
    ) -> None:
        """When the result records a successful daemon restart, the
        formatter emits the `==> vq self-update detected — restarting
        vq-daemon` banner + the message line. The user sees the
        restart actually happened."""
        result = admin.UpdateResult(
            env="vibeqc-queue",
            git_dir="/repo",
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            paused_summary="paused 0 jobs",
            resumed_summary="resumed 0 jobs",
            daemon_restart_attempted=True,
            daemon_restart_succeeded=True,
            daemon_health_verified=True,
            daemon_restart_message=(
                "systemctl --user restart vq-daemon ... done "
                "(PID 1234 -> 5678)"
            ),
        )
        text = admin.format_update_result(result)
        assert "vq self-update detected" in text
        assert "PID 1234 -> 5678" in text
        assert "== OK ==" in text

    def test_formatter_failed_restart_shows_reason(self) -> None:
        """When the restart failed, the FAILED block includes
        `vq-daemon restart failed` in the reasons list so the user
        knows what to fix."""
        result = admin.UpdateResult(
            env="vibeqc-queue",
            git_dir="/repo",
            branch="main",
            update_script="scripts/update.sh",
            git_pull_rc=0,
            update_script_rc=0,
            daemon_restart_attempted=True,
            daemon_restart_succeeded=False,
            daemon_restart_message="systemctl unreachable; see operations.md",
        )
        text = admin.format_update_result(result)
        assert "vq self-update detected — daemon restart FAILED" in text
        assert "== FAILED ==" in text
        assert "vq-daemon restart failed" in text


# ----------------------------------------------------------------------
# format_update_result
# ----------------------------------------------------------------------


class TestFormatUpdateResult:
    def test_ok_output_shape(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch="main",
            update_script="scripts/update-dev.sh",
            paused_summary="paused 0 jobs",
            resumed_summary="resumed 0 jobs",
            git_pull_rc=0,
            git_pull_output="Already up to date.\n",
            update_script_rc=0,
            update_script_output="Build OK\n",
        )
        text = admin.format_update_result(result)
        assert "== admin update vibeqc-dev ==" in text
        assert "git_dir:       /repo" in text
        assert "branch:        main" in text
        assert "update_script: scripts/update-dev.sh" in text
        assert "Already up to date" in text
        assert "Build OK" in text
        assert "== OK ==" in text

    def test_failed_output_includes_reasons(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch="main",
            update_script="scripts/update-dev.sh",
            git_pull_rc=128,
            git_pull_output="fatal: not a git repo",
        )
        text = admin.format_update_result(result)
        assert "== FAILED ==" in text
        assert "git pull rc=128" in text

    def test_no_script_skips_script_block(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch="main",
            update_script=None,  # no script configured
            git_pull_rc=0,
        )
        text = admin.format_update_result(result)
        # Should NOT include "-- <script> (rc=..) --" section
        assert "scripts/update" not in text
        assert "update_script: (none)" in text

    def test_work_errors_shown(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch=None,
            update_script="x.sh",
            work_errors=["git pull timed out after 300s"],
        )
        text = admin.format_update_result(result)
        assert "work errors" in text
        assert "timed out" in text
        assert "FAILED" in text


# ----------------------------------------------------------------------
# CLI verb
# ----------------------------------------------------------------------


class TestAdminUpdateCLI:
    def test_unknown_env_returns_usage_error(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["admin", "update", "ghost"])
        assert result.exit_code == 2  # click.UsageError
        assert "unknown env" in result.output

    def test_binary_env_rejected_via_cli(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/fake/crystal"\n'
        )
        result = CliRunner().invoke(main, ["admin", "update", "crystal"])
        assert result.exit_code == 2
        assert "only kind=\"venv\"" in result.output

    def test_happy_path_cli_exits_zero(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update-dev.sh"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok"), _ok_proc(stdout="built")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev"]
            )
        assert result.exit_code == 0, result.output
        assert "== OK ==" in result.output
        assert "vibeqc-dev" in result.output

    def test_failure_cli_exits_nonzero(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1, stderr="bad")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev"]
            )
        assert result.exit_code == 1
        assert "== FAILED ==" in result.output

    def test_admin_help_lists_update(self) -> None:
        result = CliRunner().invoke(main, ["admin", "--help"])
        assert result.exit_code == 0
        assert "update" in result.output

    def test_admin_update_help_describes_workflow(self) -> None:
        result = CliRunner().invoke(main, ["admin", "update", "--help"])
        assert result.exit_code == 0
        # Should mention the typical chat workflow
        assert "git push" in result.output
        assert "vq admin update" in result.output
        assert "vq submit" in result.output

    def test_top_level_help_includes_admin(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "admin" in result.output


# ----------------------------------------------------------------------
# v0.5.24: --tag verification
# ----------------------------------------------------------------------


class TestTagVerification:
    """``--tag v0.X.Y`` runs ``git describe --exact-match --tags HEAD``
    after fetching/checking out that tag and asserts the output matches.
    Mismatch = update_script SKIPPED + result.success=False."""

    @pytest.fixture(autouse=True)
    def _plain_update_argv(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Tag tests describe pinning, not Linux process priority."""
        monkeypatch.setattr(admin, "_build_niceness_prefix", lambda: [])

    def test_tag_match_marks_success(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        responses = [
            _ok_proc(stdout="tag fetched\n"),             # git fetch tag
            _ok_proc(stdout="detached\n"),                # git checkout tag
            _ok_proc(stdout="v0.8.0\n"),                  # git describe
            _ok_proc(stdout="Build OK\n"),                # update_script
            _ok_proc(stdout="v0.8.0\n"),                  # post-script describe
        ]
        with patch("vq.admin.subprocess.run", side_effect=responses) as m:
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                expected_tag="v0.8.0",
            )

        assert result.success is True
        assert result.tag_verification_attempted is True
        assert result.tag_matches is True
        assert result.actual_tag == "v0.8.0"
        assert result.expected_tag == "v0.8.0"
        # The explicit responses corresponded to:
        # fetch tag, checkout tag, describe, bash update_script.
        # v0.5.25 adds an extra SHA-query subprocess (best-effort,
        # silently falls through StopIteration); not what the test is
        # measuring. Verify the meaningful work happened:
        assert len(_bash_calls(m)) == 1  # update_script ran
        bash_call = _bash_calls(m)[0]
        assert bash_call.args[0][-2:] == ["--branch", "v0.8.0"]
        # Verify the describe call signature
        describe_call = m.call_args_list[2]
        assert describe_call.args[0][1] == "-C"
        assert "describe" in describe_call.args[0]
        assert "--exact-match" in describe_call.args[0]

    def test_tag_mismatch_skips_script_and_fails(
        self, state_dir: Path
    ) -> None:
        """Wrong tag = update_script NOT run + result.success=False."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        responses = [
            _ok_proc(stdout="tag fetched\n"),             # git fetch tag
            _ok_proc(stdout="detached\n"),                # git checkout tag
            _ok_proc(stdout="v0.7.3\n"),                  # got wrong tag
            # update_script should NOT be called
        ]
        with patch("vq.admin.subprocess.run", side_effect=responses) as m:
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                expected_tag="v0.8.0",
            )

        assert result.success is False
        assert result.tag_matches is False
        assert result.actual_tag == "v0.7.3"
        assert result.expected_tag == "v0.8.0"
        assert result.update_script_rc is None  # never ran
        # bash update_script must not have been invoked.
        _assert_no_bash_invoked(m)

    def test_no_tag_on_head_treated_as_mismatch(
        self, state_dir: Path
    ) -> None:
        """git describe rc=128 means no tag points at HEAD; we treat
        that as a verification failure if --tag was specified."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(),  # git fetch tag
                _ok_proc(),  # git checkout tag
                _fail_proc(
                    128, stdout="",
                    stderr="fatal: No tags can describe '...'",
                ),
            ],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                expected_tag="v0.8.0",
            )

        assert result.success is False
        assert result.tag_matches is False
        assert result.actual_tag is None
        assert result.tag_check_rc == 128

    def test_tag_check_skipped_when_git_pull_fails(
        self, state_dir: Path
    ) -> None:
        """If git pull failed, tag verification doesn't run; the failure
        is already failure enough."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1, stderr="network down")],
        ) as m:
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                expected_tag="v0.8.0",
            )

        assert result.success is False
        assert result.git_pull_rc == 1
        # tag check never ran
        assert result.tag_check_rc is None
        assert result.actual_tag is None
        # bash update_script must not have been invoked.
        _assert_no_bash_invoked(m)

    def test_no_expected_tag_keeps_v0_5_20_behavior(
        self, state_dir: Path
    ) -> None:
        """Without --tag, behaviour is identical to v0.5.20: pull +
        update_script, no describe call. Regression guard."""
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(), _ok_proc()],
        ) as m:
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")

        assert result.success is True
        assert result.tag_verification_attempted is False
        assert result.tag_matches is None
        # Critical regression guard: NO describe call when --tag unset.
        describe_calls = [
            c for c in m.call_args_list
            if c.args and c.args[0] and "describe" in c.args[0]
        ]
        # v0.5.25 added _query_git_describe inside admin status, but
        # update_env itself does not call describe unless --tag is set.
        # However record_update_outcome doesn't call describe (only
        # rev-parse), so there should be no describe calls here.
        assert describe_calls == []
        # And the update_script ran exactly once.
        assert len(_bash_calls(m)) == 1

    def test_format_shows_tag_block_on_attempt(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir="/repo",
            branch="release",
            update_script=None,
            git_pull_rc=0,
            expected_tag="v0.8.0",
            actual_tag="v0.8.0",
            tag_check_rc=0,
        )
        text = admin.format_update_result(result)
        assert "tag verification" in text
        assert "expected: 'v0.8.0'" in text
        assert "actual:   'v0.8.0'" in text
        assert "MATCH" in text
        assert "OK" in text

    def test_format_shows_mismatch_reason(self) -> None:
        result = admin.UpdateResult(
            env="vibeqc-release",
            git_dir="/repo",
            branch="release",
            update_script=None,
            git_pull_rc=0,
            expected_tag="v0.8.0",
            actual_tag="v0.7.3",
            tag_check_rc=0,
        )
        text = admin.format_update_result(result)
        assert "MISMATCH" in text
        assert "v0.7.3" in text
        assert "FAILED" in text
        assert "tag mismatch" in text


class TestTagVerificationCLI:
    def test_cli_tag_flag_threads_through(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update-dev.sh"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="tag fetched"),
                _ok_proc(stdout="detached"),
                _ok_proc(stdout="v0.8.0"),
                _ok_proc(stdout="built"),
                _ok_proc(stdout="v0.8.0"),
            ],
        ):
            result = CliRunner().invoke(
                main,
                ["admin", "update", "vibeqc-dev", "--tag", "v0.8.0"],
            )
        assert result.exit_code == 0, result.output
        assert "tag verification" in result.output
        assert "MATCH" in result.output

    def test_cli_tag_mismatch_exits_nonzero(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="tag fetched"),
                _ok_proc(stdout="detached"),
                _ok_proc(stdout="v0.7.3"),  # wrong tag
            ],
        ):
            result = CliRunner().invoke(
                main,
                ["admin", "update", "vibeqc-dev", "--tag", "v0.8.0"],
            )
        assert result.exit_code == 1
        assert "MISMATCH" in result.output
        assert "FAILED" in result.output

    def test_cli_help_mentions_tag(self) -> None:
        result = CliRunner().invoke(main, ["admin", "update", "--help"])
        assert result.exit_code == 0
        assert "--tag" in result.output
        assert "v0.X.Y" in result.output


# ----------------------------------------------------------------------
# v0.5.25: vq admin status
# ----------------------------------------------------------------------


class TestAdminStatusPersistence:
    """``vq admin update`` writes one row to admin-status.json per env;
    ``vq admin status`` reads it for the LAST_UPDATED_AT column."""

    def test_update_records_outcome(self, state_dir: Path) -> None:
        """A successful update writes an AdminUpdateRecord for the env."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="Already up to date")],
        ):
            result = admin.update_env("vibeqc-dev", cfg, host="localhost")
        assert result.success

        records = admin.read_admin_status()
        assert "vibeqc-dev" in records
        rec = records["vibeqc-dev"]
        assert rec.last_success is True
        assert rec.last_git_pull_rc == 0
        assert rec.last_updated_at  # ISO timestamp

    def test_failure_recorded_too(self, state_dir: Path) -> None:
        """Failed update is still recorded — forensics-useful."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1, stderr="network down")],
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        records = admin.read_admin_status()
        assert "vibeqc-dev" in records
        assert records["vibeqc-dev"].last_success is False
        assert records["vibeqc-dev"].last_git_pull_rc == 1
        assert records["vibeqc-dev"].last_failure_reason == "git pull rc=1"

    def test_daemon_restart_failure_reason_is_persisted(
        self, state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            name="vibeqc-queue",
            git_dir=str(repo),
            update_script=None,
        )
        cfg = config.load_config()
        result = admin.UpdateResult(
            env="vibeqc-queue",
            git_dir=str(repo),
            branch="main",
            update_script=None,
            git_pull_rc=0,
            branch_check_rc=0,
            actual_branch="main",
            daemon_restart_attempted=True,
            daemon_restart_succeeded=False,
            daemon_health_verified=False,
            daemon_expected_source_sha="a" * 40,
            daemon_actual_source_sha="b" * 40,
            daemon_restart_message="RPC source SHA mismatch",
        )

        admin.record_update_outcome("vibeqc-queue", result)

        records = admin.read_admin_status()
        rec = records["vibeqc-queue"]
        assert rec.last_success is False
        assert (
            rec.last_failure_reason
            == "daemon restart failed: RPC source SHA mismatch"
        )
        assert rec.last_daemon_restart_attempted is True
        assert rec.last_daemon_restart_succeeded is False
        assert rec.last_daemon_health_verified is False
        assert rec.last_daemon_expected_source_sha == "a" * 40
        assert rec.last_daemon_actual_source_sha == "b" * 40
        payload = json.loads(admin.format_admin_status_json(cfg))
        row = next(
            item for item in payload["envs"]
            if item["name"] == "vibeqc-queue"
        )
        assert row["last_failure_reason"] == rec.last_failure_reason
        assert row["last_daemon_restart_message"] == "RPC source SHA mismatch"
        text = admin.format_admin_status(cfg, verbose=True)
        assert "-- vibeqc-queue: failure reason --" in text
        assert "daemon restart failed: RPC source SHA mismatch" in text

    def test_multiple_envs_tracked_independently(
        self, state_dir: Path
    ) -> None:
        repo_a = _make_git_repo(state_dir / "repo-a")
        repo_b = _make_git_repo(state_dir / "repo-b")
        (state_dir / "cfg" / "config.toml").write_text(
            f'[programs.vibeqc-dev]\nkind = "venv"\n'
            f'python = "/x"\ngit_dir = "{repo_a}"\n\n'
            f'[programs.vibeqc-release]\nkind = "venv"\n'
            f'python = "/y"\ngit_dir = "{repo_b}"\n'
        )
        cfg = config.load_config()

        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc()],
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1)],
        ):
            admin.update_env("vibeqc-release", cfg, host="localhost")

        records = admin.read_admin_status()
        assert "vibeqc-dev" in records
        assert "vibeqc-release" in records
        assert records["vibeqc-dev"].last_success is True
        assert records["vibeqc-release"].last_success is False


class TestAdminStatusFormat:
    """``format_admin_status(cfg)`` builds the table."""

    def test_empty_registry_message(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        assert "no venv programs registered" in text

    def test_only_venv_kinds_appear(self, state_dir: Path) -> None:
        """Binary and import kinds are excluded; only venv shows up."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/fake/crystal"\n'
            '\n'
            f'[programs.vibeqc-dev]\nkind = "venv"\n'
            f'python = "/x"\ngit_dir = "{repo}"\n'
            '\n'
            '[programs.pyscf]\nkind = "import"\n'
            'python = "/y"\nimport_check = "pyscf"\n'
        )
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        assert "vibeqc-dev" in text
        assert "crystal" not in text
        assert "pyscf" not in text

    def test_missing_git_dir_shows_error(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            f'[programs.broken]\nkind = "venv"\n'
            f'python = "/x"\ngit_dir = "{state_dir}/missing"\n'
        )
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        assert "broken" in text
        assert "ERROR" in text

    def test_table_columns_present(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            f'[programs.vibeqc-dev]\nkind = "venv"\n'
            f'python = "/x"\ngit_dir = "{repo}"\nbranch = "main"\n'
        )
        cfg = config.load_config()
        text = admin.format_admin_status(cfg)
        # v0.7.2: DESCRIBE was renamed to VERSION (sources from
        # pyproject.toml's [project] version, falls back to git
        # describe when no pyproject is present). The other six
        # columns are unchanged.
        for col in ("NAME", "BRANCH", "SHA", "VERSION", "DIRTY",
                    "LAST_UPDATED_AT", "LAST OK"):
            assert col in text


class TestAdminStatusCLI:
    def test_status_cli_runs(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            f'[programs.vibeqc-dev]\nkind = "venv"\n'
            f'python = "/x"\ngit_dir = "{repo}"\nbranch = "main"\n'
        )
        result = CliRunner().invoke(main, ["admin", "status"])
        assert result.exit_code == 0, result.output
        assert "vibeqc-dev" in result.output
        assert "main" in result.output

    def test_status_empty_registry(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["admin", "status"])
        assert result.exit_code == 0
        assert "no venv programs registered" in result.output

    def test_status_help(self) -> None:
        result = CliRunner().invoke(main, ["admin", "status", "--help"])
        assert result.exit_code == 0
        # Should mention key columns and the "is host_d at my commit?" use case
        assert "SHA" in result.output
        assert "LAST_UPDATED_AT" in result.output

    def test_admin_help_lists_status(self) -> None:
        result = CliRunner().invoke(main, ["admin", "--help"])
        assert result.exit_code == 0
        assert "status" in result.output


class TestAdminStatusPersistenceFunctions:
    def test_read_empty_when_file_missing(self, state_dir: Path) -> None:
        assert admin.read_admin_status() == {}

    def test_corrupt_file_returns_empty(self, state_dir: Path) -> None:
        admin.admin_status_path().parent.mkdir(parents=True, exist_ok=True)
        admin.admin_status_path().write_text("not valid json {{{")
        assert admin.read_admin_status() == {}

    def test_roundtrip(self, state_dir: Path) -> None:
        records = {
            "alpha": admin.AdminUpdateRecord(
                last_updated_at="2026-05-13T12:00:00+00:00",
                last_success=True,
                last_sha="abcdef012345",
                last_tag="v0.8.0",
            ),
        }
        admin.write_admin_status(records)
        back = admin.read_admin_status()
        assert "alpha" in back
        assert back["alpha"].last_sha == "abcdef012345"
        assert back["alpha"].last_tag == "v0.8.0"

    def test_forward_compat_strips_unknown_fields(
        self, state_dir: Path,
    ) -> None:
        """v0.7.1 *Lamport's Clock* changed the schema-drift policy:
        unknown keys (likely from a NEWER vq version that added
        fields) are now silently stripped and the entry loads
        cleanly with what the local client does understand. The
        pre-v0.7.1 behavior was to drop the whole entry, which
        meant an older client treated a newer daemon's record as
        "no last update at all" — surprising and unhelpful for
        rollback / mixed-version operation."""
        admin.admin_status_path().parent.mkdir(parents=True, exist_ok=True)
        admin.admin_status_path().write_text(
            '{"future_env": {"unknown_field": "x", '
            '"last_updated_at": "now", "last_success": true}}'
        )
        records = admin.read_admin_status()
        assert "future_env" in records
        # The known fields loaded correctly; the unknown one is
        # silently absent (no extra attribute on the dataclass).
        assert records["future_env"].last_updated_at == "now"
        assert records["future_env"].last_success is True

    def test_truly_corrupt_entry_still_dropped(
        self, state_dir: Path,
    ) -> None:
        """The forward-compat strip applies to UNKNOWN keys; missing
        REQUIRED keys still drop the entry rather than crash the
        whole read. Pinning this so the strip doesn't go too far."""
        admin.admin_status_path().parent.mkdir(parents=True, exist_ok=True)
        # Missing required fields (last_updated_at + last_success).
        admin.admin_status_path().write_text(
            '{"broken_env": {"last_sha": "abc"}}'
        )
        records = admin.read_admin_status()
        assert "broken_env" not in records


# ----------------------------------------------------------------------
# v0.5.28: vq admin update --all (multi-env)
# ----------------------------------------------------------------------


def _write_two_venv_config(state_dir: Path) -> tuple[Path, Path]:
    """Config with two kind=venv programs + one binary (which --all
    must skip). Returns (repo_dev, repo_release)."""
    repo_dev = _make_git_repo(state_dir / "repo-dev")
    (repo_dev / "scripts").mkdir()
    (repo_dev / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
    repo_rel = _make_git_repo(state_dir / "repo-rel")
    (repo_rel / "scripts").mkdir()
    (repo_rel / "scripts" / "update.sh").write_text("#!/bin/bash\n")
    (state_dir / "cfg" / "config.toml").write_text(
        'default_host = "localhost"\n'
        '[programs.vibeqc-dev]\n'
        'kind = "venv"\n'
        'python = "/fake/dev/python"\n'
        f'git_dir = "{repo_dev}"\n'
        'branch = "main"\n'
        'update_script = "scripts/update-dev.sh"\n'
        '\n'
        '[programs.vibeqc-release]\n'
        'kind = "venv"\n'
        'python = "/fake/rel/python"\n'
        f'git_dir = "{repo_rel}"\n'
        'branch = "release"\n'
        'update_script = "scripts/update.sh"\n'
        '\n'
        '[programs.crystal]\n'
        'kind = "binary"\n'
        'binary = "/fake/crystal"\n'
    )
    return repo_dev, repo_rel


class TestUpdateAll:
    """``update_all`` refreshes every kind=venv program; binary/import
    programs are skipped. Pause/resume bracket the whole batch."""

    def test_updates_every_venv_skips_binary(
        self, state_dir: Path
    ) -> None:
        _write_two_venv_config(state_dir)
        cfg = config.load_config()
        # 2 envs × 2 calls each (git pull + update_script) = 4 responses.
        responses = [
            _ok_proc(stdout="Already up to date."),  # dev: git pull
            _ok_proc(stdout="dev built"),            # dev: update_script
            _ok_proc(stdout="Already up to date."),  # release: git pull
            _ok_proc(stdout="release built"),        # release: update_script
        ]
        with patch("vq.admin.subprocess.run", side_effect=responses):
            results = admin.update_all(cfg, host="localhost")

        assert len(results) == 2  # crystal (binary) skipped
        # Sorted by name: vibeqc-dev before vibeqc-release
        assert results[0].env == "vibeqc-dev"
        assert results[1].env == "vibeqc-release"
        assert all(r.success for r in results)

    def test_batch_defers_serving_daemon_restart_to_outer_transaction(
        self,
        state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            name="vibeqc-queue",
            git_dir=str(repo),
            update_script="vibe-queue/scripts/update.sh",
        )
        cfg = config.load_config()
        captured: list[bool] = []

        def work(name, prog, **kwargs):
            captured.append(kwargs["managed_daemon_restart"])
            return admin.UpdateResult(
                env=name,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                git_pull_rc=0,
                update_script_rc=0,
            )

        probe = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="test managed daemon",
        )
        lifecycle = SimpleNamespace(terminal_verified=False)

        def complete(prog, result, active_lifecycle):
            assert active_lifecycle is lifecycle
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = True
            result.daemon_health_verified = True
            active_lifecycle.terminal_verified = True

        with (
            patch("vq.admin._detect_vq_self_update", return_value=probe),
            patch("vq.admin._managed_update_script_args", return_value=[]),
            patch(
                "vq.admin._begin_managed_daemon_update",
                return_value=lifecycle,
            ),
            patch(
                "vq.admin._complete_managed_daemon_update",
                side_effect=complete,
            ),
            patch("vq.admin._do_update_work", side_effect=work),
            patch("vq.admin._maybe_restart_daemon"),
        ):
            results = admin.update_all(cfg, host="localhost")

        assert captured == [True]
        assert len(results) == 1

    def test_batch_rejects_ambiguous_serving_daemon_before_pause(
        self,
        state_dir: Path,
    ) -> None:
        repo_a = _make_git_repo(state_dir / "repo-a")
        repo_b = _make_git_repo(state_dir / "repo-b")
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vq-a]\n"
            'kind = "venv"\n'
            'python = "/same/venv/bin/python"\n'
            f'git_dir = "{repo_a}"\n'
            "[programs.vq-b]\n"
            'kind = "venv"\n'
            'python = "/same/venv/bin/python"\n'
            f'git_dir = "{repo_b}"\n'
        )
        cfg = config.load_config()
        probe = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="test duplicate",
        )
        with (
            patch("vq.admin._detect_vq_self_update", return_value=probe),
            patch("vq.admin.pause_token_scope_with_proof") as pause,
            pytest.raises(admin.AdminError, match="multiple configured venvs"),
        ):
            admin.update_all(cfg, host="localhost")

        pause.assert_not_called()

    def test_pause_resume_bracket_the_batch_once(
        self, state_dir: Path
    ) -> None:
        """pause_all + resume_all each called exactly ONCE for the whole
        batch, not once per env."""
        _write_two_venv_config(state_dir)
        cfg = config.load_config()

        from vq import admin as admin_mod
        pause_calls: list[str] = []
        resume_calls: list[str] = []
        orig_pause = admin_mod.pause_token_scope_with_proof
        orig_resume = admin_mod.resume_token_scope_with_proof

        # v0.6.42: admin update threads multi_user= into pause/resume.
        def _spy_pause(host: str, token: str, **kw: object) -> object:
            pause_calls.append(host)
            return orig_pause(host, token, **kw)

        def _spy_resume(host: str, token: str, **kw: object) -> object:
            resume_calls.append(host)
            return orig_resume(host, token, **kw)

        with patch.object(
                 admin_mod, "pause_token_scope_with_proof", _spy_pause,
             ), \
             patch.object(
                 admin_mod, "resume_token_scope_with_proof", _spy_resume,
             ), \
             patch(
                 "vq.admin.subprocess.run",
                 side_effect=[_ok_proc(), _ok_proc(), _ok_proc(), _ok_proc()],
             ):
            admin.update_all(cfg, host="localhost")

        # ONE pause, ONE resume — not 2+2.
        assert pause_calls == ["localhost"]
        assert resume_calls == ["localhost"]

    def test_one_env_failure_does_not_abort_the_rest(
        self, state_dir: Path
    ) -> None:
        """If vibeqc-dev's git pull fails, vibeqc-release is still
        attempted. Batch verdict is all-or-nothing but every env runs."""
        _write_two_venv_config(state_dir)
        cfg = config.load_config()
        responses = [
            _fail_proc(1, stderr="dev network down"),  # dev: git pull FAILS
            # dev update_script skipped (pull failed)
            _ok_proc(stdout="up to date"),             # release: git pull OK
            _ok_proc(stdout="release built"),          # release: update_script
        ]
        with patch("vq.admin.subprocess.run", side_effect=responses):
            results = admin.update_all(cfg, host="localhost")

        assert len(results) == 2
        assert results[0].env == "vibeqc-dev"
        assert results[0].success is False
        assert results[0].git_pull_rc == 1
        assert results[1].env == "vibeqc-release"
        assert results[1].success is True

    def test_empty_registry_raises_admin_error(
        self, state_dir: Path
    ) -> None:
        """No venv programs at all → AdminError, not a silent empty list."""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/fake/crystal"\n'
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError, match="no kind=\"venv\""):
            admin.update_all(cfg, host="localhost")

    def test_bad_git_dir_rejected_before_pause(
        self, state_dir: Path
    ) -> None:
        """A typo'd git_dir in one env must fail BEFORE the queue is
        paused — validate everything up front."""
        repo_dev = _make_git_repo(state_dir / "repo-dev")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo_dev}"\n'
            '\n'
            '[programs.vibeqc-broken]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{state_dir}/does-not-exist"\n'
        )
        cfg = config.load_config()

        from vq import admin as admin_mod
        pause_calls: list[str] = []
        with patch.object(
            admin_mod, "pause_token_scope_with_proof",
            lambda h, token, **kwargs: (
                pause_calls.append(h) or _ProofStub("paused")
            ),
        ), pytest.raises(admin.AdminError, match="not a directory"):
            admin.update_all(cfg, host="localhost")
        # Queue must NOT have been paused — validation failed first.
        assert pause_calls == []

    def test_records_outcome_for_each_env(
        self, state_dir: Path
    ) -> None:
        _write_two_venv_config(state_dir)
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(), _ok_proc(), _ok_proc(), _ok_proc()],
        ):
            admin.update_all(cfg, host="localhost")
        records = admin.read_admin_status()
        assert "vibeqc-dev" in records
        assert "vibeqc-release" in records
        assert records["vibeqc-dev"].last_success is True
        assert records["vibeqc-release"].last_success is True


class TestFormatUpdateAllResults:
    def test_batch_ok_verdict(self) -> None:
        results = [
            admin.UpdateResult(
                env="vibeqc-dev", git_dir="/d", branch="main",
                update_script=None, git_pull_rc=0,
            ),
            admin.UpdateResult(
                env="vibeqc-release", git_dir="/r", branch="release",
                update_script=None, git_pull_rc=0,
            ),
        ]
        text = admin.format_update_all_results(results)
        assert "BATCH OK: 2/2" in text
        assert "vibeqc-dev" in text
        assert "vibeqc-release" in text

    def test_batch_failed_lists_failing_envs(self) -> None:
        results = [
            admin.UpdateResult(
                env="vibeqc-dev", git_dir="/d", branch="main",
                update_script=None, git_pull_rc=0,
            ),
            admin.UpdateResult(
                env="vibeqc-release", git_dir="/r", branch="release",
                update_script=None, git_pull_rc=128,
            ),
        ]
        text = admin.format_update_all_results(results)
        assert "BATCH FAILED: 1/2" in text
        assert "failed: vibeqc-release" in text

    def test_empty_results(self) -> None:
        text = admin.format_update_all_results([])
        assert "no venv envs" in text


class TestUpdateAllCLI:
    def test_cli_all_happy_path(self, state_dir: Path) -> None:
        _write_two_venv_config(state_dir)
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(), _ok_proc(), _ok_proc(), _ok_proc()],
        ):
            result = CliRunner().invoke(main, ["admin", "update", "--all"])
        assert result.exit_code == 0, result.output
        assert "BATCH OK: 2/2" in result.output

    def test_cli_all_failure_exits_nonzero(self, state_dir: Path) -> None:
        _write_two_venv_config(state_dir)
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _fail_proc(1, stderr="bad"),  # dev pull fails
                _ok_proc(), _ok_proc(),       # release ok
            ],
        ):
            result = CliRunner().invoke(main, ["admin", "update", "--all"])
        assert result.exit_code == 1
        assert "BATCH FAILED: 1/2" in result.output

    def test_cli_all_and_env_mutually_exclusive(
        self, state_dir: Path
    ) -> None:
        """`vq admin update vibeqc-dev --all` → with --all the positional
        is treated as HOST; an extra positional ('ENV HOST'-style) errors."""
        _write_two_venv_config(state_dir)
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev", "extra-arg", "--all"]
        )
        assert result.exit_code != 0
        assert "at most a HOST positional" in result.output

    def test_cli_all_and_tag_mutually_exclusive(
        self, state_dir: Path
    ) -> None:
        _write_two_venv_config(state_dir)
        result = CliRunner().invoke(
            main, ["admin", "update", "--all", "--tag", "v0.8.0"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive with --tag" in result.output

    def test_cli_no_env_no_all_errors(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        result = CliRunner().invoke(main, ["admin", "update"])
        assert result.exit_code != 0
        assert "ENV is required" in result.output

    def test_cli_single_env_still_works(self, state_dir: Path) -> None:
        """Regression guard: the v0.5.20 single-env form is unchanged."""
        _write_two_venv_config(state_dir)
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok"), _ok_proc(stdout="built")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev"]
            )
        assert result.exit_code == 0, result.output
        assert "== OK ==" in result.output
        # Single-env mode does NOT print the batch verdict.
        assert "BATCH" not in result.output

    def test_cli_help_mentions_all(self) -> None:
        result = CliRunner().invoke(main, ["admin", "update", "--help"])
        assert result.exit_code == 0
        assert "--all" in result.output


# ----------------------------------------------------------------------
# v0.5.44: admin-update-in-progress marker file
# ----------------------------------------------------------------------


class TestAdminUpdateMarker:
    """Pure mechanics of the marker module: path, write, read, clear,
    and the exists vs read distinction (cheap stat vs parsed view)."""

    def test_marker_path_lives_under_state_root(
        self, state_dir: Path
    ) -> None:
        path = admin.admin_update_marker_path()
        assert path == paths.state_root() / admin.ADMIN_UPDATE_MARKER_FILENAME

    def test_exists_false_when_no_file(self, state_dir: Path) -> None:
        assert admin.admin_update_marker_exists() is False
        assert admin.read_admin_update_marker() is None

    def test_write_then_read_roundtrip(self, state_dir: Path) -> None:
        m = admin.write_admin_update_marker(
            envs=["vibeqc-dev", "vibeqc-release"], host="host_d",
        )
        assert admin.admin_update_marker_exists() is True
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.envs == ["vibeqc-dev", "vibeqc-release"]
        assert loaded.host == "host_d"
        assert loaded.pid == os.getpid()
        # started_at + vq_version are stamped at write time; just
        # assert non-empty + the version matches the running module.
        assert loaded.started_at
        assert loaded.last_heartbeat_at
        assert loaded.last_heartbeat_message == "state=pausing"
        from vq import __version__ as live_version
        assert loaded.vq_version == live_version == m.vq_version

    def test_read_returns_none_on_malformed_json(
        self, state_dir: Path
    ) -> None:
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not valid json {")
        # exists() still True — the safety guard must fire even on
        # corrupt markers; the parsed read is the one that returns None.
        assert admin.admin_update_marker_exists() is True
        assert admin.read_admin_update_marker() is None

    def test_read_returns_none_on_schema_drift(
        self, state_dir: Path
    ) -> None:
        """A marker missing required fields (e.g. written by a future
        vq version with extra fields, then read by an older vq) parses
        as None rather than half-populating the dataclass."""
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text('{"envs": ["x"]}')  # missing host/started_at/pid
        assert admin.admin_update_marker_exists() is True
        assert admin.read_admin_update_marker() is None

    def test_clear_returns_parsed_snapshot_and_removes_file(
        self, state_dir: Path
    ) -> None:
        admin.write_admin_update_marker(envs=["vibeqc-dev"], host="host_a")
        snapshot = admin.clear_admin_update_marker()
        assert snapshot is not None
        assert snapshot.envs == ["vibeqc-dev"]
        assert snapshot.host == "host_a"
        assert admin.admin_update_marker_exists() is False

    def test_clear_when_absent_is_quiet_noop(
        self, state_dir: Path
    ) -> None:
        assert admin.clear_admin_update_marker() is None
        assert admin.admin_update_marker_exists() is False

    def test_transition_refreshes_heartbeat(
        self, state_dir: Path
    ) -> None:
        marker = admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        updated = admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_BUILDING,
        )
        assert updated is not None
        assert updated.last_heartbeat_at
        assert updated.last_heartbeat_at != ""
        assert updated.last_heartbeat_message == "state=building"
        assert updated.last_heartbeat_at >= marker.last_heartbeat_at


class TestAdminUpdateMarkerGuard:
    """update_env / update_all marker-write + clear-on-success
    behavior plus the guard that rejects when a marker is already
    on disk."""

    def test_update_env_writes_marker_then_clears_on_success(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        # Marker was cleared by the successful finally branch.
        assert admin.admin_update_marker_exists() is False

    def test_update_env_keeps_marker_on_git_pull_failure(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(1, stderr="network down")],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert not result.success
        # git pull failed → success is False → marker stays.
        assert admin.admin_update_marker_exists() is True
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.envs == ["vibeqc-dev"]

    def test_update_env_keeps_marker_on_update_script_failure(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _ok_proc(stdout="ok"),
                _fail_proc(2, stderr="build failed"),
            ],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert not result.success
        assert admin.admin_update_marker_exists() is True

    def test_update_env_refuses_when_marker_present(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError) as exc_info:
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        msg = str(exc_info.value)
        assert "admin-update-in-progress marker present" in msg
        assert "marker_status=running" in msg
        assert "already running" in msg
        assert "vq admin status --verbose" in msg
        # Marker is unchanged.
        assert admin.admin_update_marker_exists() is True

    def test_update_env_refuses_when_marker_is_malformed(
        self, state_dir: Path
    ) -> None:
        """A corrupt marker still blocks new updates — the safety
        net should fire on the file's existence, not on whether we
        can parse it."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("garbage")
        cfg = config.load_config()
        with pytest.raises(admin.AdminError) as exc_info:
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        assert "unreadable" in str(exc_info.value)

    def test_update_env_force_overwrites_existing_marker(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost", force=True,
            )
        assert result.success
        # Force succeeded → marker cleared at end.
        assert admin.admin_update_marker_exists() is False

    def test_update_all_marker_records_full_env_list(
        self, state_dir: Path
    ) -> None:
        repo_a = _make_git_repo(state_dir / "repo_a")
        repo_b = _make_git_repo(state_dir / "repo_b")
        (state_dir / "cfg" / "config.toml").write_text(
            f'[programs.vibeqc-dev]\nkind = "venv"\n'
            f'python = "/fake/python"\ngit_dir = "{repo_a}"\n'
            f'branch = "main"\n'
            f'[programs.vibeqc-release]\nkind = "venv"\n'
            f'python = "/fake/python"\ngit_dir = "{repo_b}"\n'
            f'branch = "release"\n'
        )
        cfg = config.load_config()
        # Make git pull on the first env fail so the batch is not
        # fully successful — we want the marker to persist with both
        # envs recorded.
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[
                _fail_proc(1, stderr="boom"),  # repo_a git pull
                _ok_proc(stdout="ok"),         # repo_b git pull
            ],
        ):
            results = admin.update_all(cfg, host="localhost")
        assert any(not r.success for r in results)
        # Partial batch → marker remains, listing both envs.
        assert admin.admin_update_marker_exists() is True
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert set(loaded.envs) == {"vibeqc-dev", "vibeqc-release"}

    def test_update_all_clears_marker_only_on_full_success(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            results = admin.update_all(cfg, host="localhost")
        assert all(r.success for r in results)
        assert admin.admin_update_marker_exists() is False


class TestFormatMarkerBanner:
    """format_admin_status's marker-banner prepend behavior."""

    def test_no_banner_when_no_marker(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()
        with patch("vq.admin.subprocess.run", return_value=_ok_proc(stdout="abc")):
            text = admin.format_admin_status(cfg)
        assert "admin-update-in-progress marker" not in text

    def test_banner_shows_marker_details_when_present(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        cfg = config.load_config()
        with patch("vq.admin.subprocess.run", return_value=_ok_proc(stdout="abc")):
            text = admin.format_admin_status(cfg)
        assert "admin-update-in-progress marker present" in text
        assert "vibeqc-dev" in text
        assert "host_d" in text
        assert "marker_status: running" in text
        assert "last_heartbeat:" in text
        assert "heartbeat:   last heartbeat" in text
        assert "already running" in text
        assert "vq admin status --verbose" in text

    def test_banner_says_unreadable_on_malformed(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json")
        cfg = config.load_config()
        with patch("vq.admin.subprocess.run", return_value=_ok_proc(stdout="abc")):
            text = admin.format_admin_status(cfg)
        assert "admin-update-in-progress marker present" in text
        assert "unreadable" in text

    def test_banner_distinguishes_stale_marker_when_pid_is_gone(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "envs": ["vibeqc-dev"],
            "host": "host_d",
            "started_at": "2026-07-04T00:00:00+00:00",
            "pid": 999999999,
            "pid_start_time": 0,
            "vq_version": "0.12.0",
            "state": admin.ADMIN_UPDATE_STATE_BUILDING,
            "phase_started_at": "2026-07-04T00:00:00+00:00",
            "failure_reason": None,
        }))
        cfg = config.load_config()
        with patch("vq.admin.subprocess.run", return_value=_ok_proc(stdout="abc")):
            text = admin.format_admin_status(cfg)
        assert "marker_status: stale" in text
        assert "pid=999999999 is not running" in text
        assert "vq admin clear-update-marker" in text


class TestClearUpdateMarkerCLI:
    """`vq admin clear-update-marker` verb."""

    @staticmethod
    def _write_minimal_config(state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )

    def test_quiet_noop_when_no_marker(self, state_dir: Path) -> None:
        self._write_minimal_config(state_dir)
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker"],
        )
        assert result.exit_code == 0, result.output
        assert "no admin-update-in-progress marker" in result.output

    def test_skips_prompt_with_yes_flag(self, state_dir: Path) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--yes", "--force-live"],
        )
        assert result.exit_code == 0, result.output
        assert "marker_status: running" in result.output
        assert "pid_status:" in result.output
        assert "already running" in result.output
        assert "marker cleared" in result.output
        assert admin.admin_update_marker_exists() is False

    def test_prompts_without_yes_flag(self, state_dir: Path) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        # Answer 'y' to the click.confirm prompt.
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--force-live"], input="y\n",
        )
        assert result.exit_code == 0, result.output
        assert "marker_status: running" in result.output
        assert "already running" in result.output
        assert "marker cleared" in result.output
        assert admin.admin_update_marker_exists() is False

    def test_refuses_live_marker_without_force_live(
        self, state_dir: Path
    ) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--yes"],
        )
        assert result.exit_code == 1, result.output
        assert "marker_status: running" in result.output
        assert "refusing to clear live admin-update marker" in result.output
        assert "--force-live" in result.output
        assert admin.admin_update_marker_exists() is True

    def test_abort_at_prompt_keeps_marker(self, state_dir: Path) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED,
            failure_reason="test failed marker",
        )
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker"], input="n\n",
        )
        assert result.exit_code != 0  # click.confirm(abort=True) → non-zero
        assert admin.admin_update_marker_exists() is True

    def test_help_text_mentions_recovery_flow(self) -> None:
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--help"],
        )
        assert result.exit_code == 0
        assert "clear" in result.output.lower()
        assert "--force" in result.output
        assert "--force-live" in result.output

    def test_single_user_pause_receipt_survives_config_flip_to_multi_user(
        self, state_dir: Path,
    ) -> None:
        """Recovery uses the receipt namespace, not today's config mode."""
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[multi_user]\n'
            'enabled = true\n'
        )
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="localhost",
        )
        admin._record_admin_update_pause_scope(
            pause_token="admin-update-0123456789ab",
            paused_jobids=[],
            surgical=False,
            # Model a receipt written before the config flipped to true.
            multi_user=False,
        )
        with patch(
            "vq.admin.resume_token_scope_with_proof",
            return_value=_ProofStub("single-user token scope clear"),
        ) as resume:
            result = CliRunner().invoke(
                main,
                [
                    "admin", "clear-update-marker", "--yes", "--force-live",
                ],
            )

        assert result.exit_code == 0, result.output
        resume.assert_called_once_with(
            "localhost",
            "admin-update-0123456789ab",
            multi_user=False,
            queue_root=paths.state_root().resolve(),
        )
        assert not admin.admin_update_marker_exists()

    def test_legacy_scoped_marker_without_queue_namespace_fails_closed(
        self, state_dir: Path,
    ) -> None:
        marker = admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="localhost",
        )
        marker.pause_token = "admin-update-0123456789ab"
        admin._write_admin_update_marker_atomic(marker)
        marker_path = admin.admin_update_marker_path()
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
        payload.pop("pause_multi_user")
        payload.pop("pause_queue_root")
        marker_path.write_text(json.dumps(payload), encoding="utf-8")
        marker = admin.read_admin_update_marker()
        assert marker is not None

        with (
            patch("vq.admin.resume_token_scope_with_proof") as resume,
            pytest.raises(admin.AdminError, match="queue namespace"),
        ):
            admin.recover_pause_scope_and_clear_marker(marker)

        resume.assert_not_called()
        assert admin.admin_update_marker_exists()

    def test_pause_receipt_queue_root_drift_fails_before_scan(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        marker = admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="localhost",
        )
        admin._record_admin_update_pause_scope(
            pause_token="admin-update-fedcba987654",
            paused_jobids=[],
            surgical=False,
            multi_user=False,
        )
        marker = admin.read_admin_update_marker()
        assert marker is not None
        marker_path = admin.admin_update_marker_path()
        monkeypatch.setattr(
            paths, "state_root", lambda: state_dir / "different-state-root",
        )

        with (
            patch("vq.admin.resume_token_scope_with_proof") as resume,
            pytest.raises(admin.AdminError, match="no longer matches path policy"),
        ):
            admin.recover_pause_scope_and_clear_marker(marker)

        resume.assert_not_called()
        assert marker_path.exists()


class TestAdminUpdateForceFlagCLI:
    """`vq admin update --force` overrides the marker guard."""

    def test_help_includes_force(self) -> None:
        result = CliRunner().invoke(
            main, ["admin", "update", "--help"],
        )
        assert result.exit_code == 0
        assert "--force" in result.output
        assert "marker" in result.output.lower()

    def test_force_lets_update_proceed_with_existing_marker(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )
        admin.write_admin_update_marker(
            envs=["stale-env"], host="host_d",
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev", "--force"],
            )
        assert result.exit_code == 0, result.output
        # Marker cleared at end of the successful run.
        assert admin.admin_update_marker_exists() is False

    def test_no_force_rejects_with_recovery_recipe(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED,
            failure_reason="prior update rc=1",
        )
        result = CliRunner().invoke(
            main, ["admin", "update", "vibeqc-dev"],
        )
        assert result.exit_code != 0
        assert "marker" in result.output.lower()
        assert "marker_status=failed" in result.output
        assert "vq admin clear-update-marker" in result.output


# ----------------------------------------------------------------------
# v0.5.46: --json output for admin status / update / clear-update-marker
# ----------------------------------------------------------------------


class TestAdminStatusJsonFormatter:
    """format_admin_status_json — JSON shape + marker inclusion."""

    def test_empty_registry(self, state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        cfg = config.load_config()
        out = json.loads(admin.format_admin_status_json(cfg))
        # v0.26.1: `in_flight` / `last_outcome` / `last_outcome_at` are the
        # two sequencing questions a caller has, answered at the top level
        # instead of inferred across nested blocks. Additive, so a consumer
        # reading the keys it knows is unaffected -- but this assertion pins
        # the whole shape, so it names them.
        assert out == {
            "marker": None,
            "markers": [],
            "envs": [],
            "in_flight": False,
            "last_outcome": None,
            "last_outcome_at": None,
        }

    def test_envs_flatten_status_and_record(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            return_value=_ok_proc(stdout="abcdef123456"),
        ):
            out = json.loads(admin.format_admin_status_json(cfg))
        assert out["marker"] is None
        assert isinstance(out["envs"], list)
        assert len(out["envs"]) == 1
        env = out["envs"][0]
        # Every documented key is present (stable schema; nulls
        # rather than omissions).
        for key in (
            "name", "git_dir", "branch", "current_sha",
            "current_describe", "is_dirty", "error",
            "last_updated_at", "last_success", "last_sha",
            "last_tag", "last_expected_tag", "last_git_pull_rc",
            "last_update_script_rc",
        ):
            assert key in env, f"missing key {key!r} in env JSON"
        assert env["name"] == "vibeqc-dev"
        assert env["branch"] == "main"
        # No prior `vq admin update` recorded → last_* are null.
        assert env["last_updated_at"] is None
        assert env["last_success"] is None

    def test_marker_block_present_when_marker_exists(
        self, state_dir: Path
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        cfg = config.load_config()
        out = json.loads(admin.format_admin_status_json(cfg))
        assert out["marker"] is not None
        assert out["marker"]["envs"] == ["vibeqc-dev"]
        assert out["marker"]["host"] == "host_d"
        assert out["marker"]["readable"] is True
        assert out["marker"]["marker_status"] == "running"
        assert "already running" in out["marker"]["summary"]
        assert "vq admin status --verbose" in out["marker"]["action"]
        assert "pid=" in out["marker"]["pid_status"]
        assert out["marker"]["heartbeat_status"].startswith("last heartbeat")
        assert out["marker"]["heartbeat_age_seconds"] is not None

    def test_marker_block_unreadable_when_corrupt(
        self, state_dir: Path
    ) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json")
        cfg = config.load_config()
        out = json.loads(admin.format_admin_status_json(cfg))
        assert out["marker"]["readable"] is False
        assert out["marker"]["marker_status"] == "unreadable"
        assert "cannot be parsed" in out["marker"]["summary"]


class TestAdminStatusJsonCLI:
    """`vq admin status --json` plumbing."""

    def test_cli_emits_valid_json(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )
        with patch("vq.admin.subprocess.run", return_value=_ok_proc(stdout="abc")):
            result = CliRunner().invoke(
                main, ["admin", "status", "--json"],
            )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed["marker"] is None
        assert any(e["name"] == "vibeqc-dev" for e in parsed["envs"])

    def test_help_mentions_json(self) -> None:
        result = CliRunner().invoke(main, ["admin", "status", "--help"])
        assert result.exit_code == 0
        assert "--json" in result.output


class TestAdminUpdateJsonFormatter:
    """format_update_result_json + format_update_all_results_json."""

    def test_update_result_json_includes_success_and_tag_matches(
        self,
    ) -> None:
        r = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch="main",
            update_script=None,
            git_pull_rc=0,
            paused_summary="paused 0",
            resumed_summary="resumed 0",
        )
        out = json.loads(admin.format_update_result_json(r))
        assert out["env"] == "vibeqc-dev"
        assert out["success"] is True
        assert out["tag_matches"] is None  # no --tag given

    def test_update_result_json_propagates_failure(self) -> None:
        r = admin.UpdateResult(
            env="vibeqc-dev",
            git_dir="/repo",
            branch="main",
            update_script="scripts/update-dev.sh",
            git_pull_rc=1,
        )
        out = json.loads(admin.format_update_result_json(r))
        assert out["success"] is False
        assert out["git_pull_rc"] == 1

    def test_update_all_results_json_batch_summary(self) -> None:
        ok = admin.UpdateResult(
            env="a", git_dir="/r", branch="main",
            update_script=None, git_pull_rc=0,
        )
        bad = admin.UpdateResult(
            env="b", git_dir="/r", branch="main",
            update_script=None, git_pull_rc=1,
        )
        out = json.loads(admin.format_update_all_results_json([ok, bad]))
        assert out["n_total"] == 2
        assert out["n_ok"] == 1
        assert out["failed_envs"] == ["b"]
        assert out["batch_success"] is False
        assert len(out["results"]) == 2

    def test_update_all_results_json_empty_input(self) -> None:
        out = json.loads(admin.format_update_all_results_json([]))
        assert out == {
            "results": [],
            "n_ok": 0,
            "n_total": 0,
            "failed_envs": [],
            "batch_success": True,
        }


class TestAdminUpdateJsonCLI:
    """`vq admin update --json` plumbing."""

    def test_single_env_json(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "vibeqc-dev", "--json"],
            )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed["env"] == "vibeqc-dev"
        assert parsed["success"] is True

    def test_all_envs_json(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
        )
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = CliRunner().invoke(
                main, ["admin", "update", "--all", "--json"],
            )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed["batch_success"] is True
        assert parsed["n_total"] >= 1


class TestClearUpdateMarkerJsonCLI:
    """`vq admin clear-update-marker --json` plumbing."""

    @staticmethod
    def _write_minimal_config(state_dir: Path) -> None:
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )

    def test_json_no_marker_returns_cleared_false(
        self, state_dir: Path
    ) -> None:
        self._write_minimal_config(state_dir)
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--json"],
        )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed == {
            "cleared": False, "marker": None, "readable": False,
        }

    def test_json_clears_existing_marker_no_prompt(
        self, state_dir: Path
    ) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        # No `input=` provided — --json must NOT prompt (would
        # SystemExit on EOF if it did).
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--json", "--force-live"],
        )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed["cleared"] is True
        assert parsed["readable"] is True
        assert parsed["marker_status"] == "running"
        assert "already running" in parsed["summary"]
        assert parsed["marker"]["envs"] == ["vibeqc-dev"]
        assert parsed["marker"]["marker_status"] == "running"
        assert "pid=" in parsed["marker"]["pid_status"]
        assert parsed["marker"]["heartbeat_status"].startswith("last heartbeat")
        assert parsed["heartbeat_age_seconds"] is not None
        assert admin.admin_update_marker_exists() is False

    def test_json_refuses_live_marker_without_force_live(
        self, state_dir: Path
    ) -> None:
        self._write_minimal_config(state_dir)
        admin.write_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--json"],
        )
        assert result.exit_code == 1, result.output
        parsed = json.loads(result.stdout)
        assert parsed["cleared"] is False
        assert parsed["readable"] is True
        assert parsed["marker_status"] == "running"
        assert "--force-live" in parsed["error"]
        assert parsed["marker"]["envs"] == ["vibeqc-dev"]
        assert parsed["heartbeat_status"].startswith("last heartbeat")
        assert admin.admin_update_marker_exists() is True

    def test_json_unreadable_marker(self, state_dir: Path) -> None:
        self._write_minimal_config(state_dir)
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("garbage")
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--json"],
        )
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.stdout)
        assert parsed["cleared"] is True
        assert parsed["readable"] is False
        assert parsed["marker"] is None
        assert parsed["marker_status"] == "unreadable"
        assert "cannot be parsed" in parsed["summary"]
        assert admin.admin_update_marker_exists() is False

    def test_help_mentions_json(self) -> None:
        result = CliRunner().invoke(
            main, ["admin", "clear-update-marker", "--help"],
        )
        assert result.exit_code == 0
        assert "--json" in result.output


# ----------------------------------------------------------------------
# v0.5.47: surgical pause scoping via provides_branches
# ----------------------------------------------------------------------


class TestProvidesBranchesIntegration:
    """v0.5.47: when prog.provides_branches is set and non-empty,
    update_env routes pause/resume through pause_provides_branches +
    resume_jobs instead of pause_all + resume_all."""

    def test_update_env_uses_surgical_pause_when_configured(
        self, state_dir: Path
    ) -> None:
        """update_env should call pause_provides_branches with the
        env's configured branches and pass the returned list to
        resume_jobs."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'provides_branches = ["main", "dev"]\n'
        )
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        # v0.6.42: admin update threads multi_user= (False here — no
        # [multi_user] in the test config).
        mock_pause.assert_called_once()
        assert mock_pause.call_args.args[0] == "localhost"
        token = mock_pause.call_args.args[1]
        assert token.startswith("admin-update-")
        assert mock_pause.call_args.kwargs == {
            "branches": ["main", "dev"],
            "multi_user": False,
            "queue_root": paths.state_root().resolve(),
            "exclude_jobids": None,
        }
        mock_resume.assert_called_once_with(
            "localhost",
            token,
            multi_user=False,
            queue_root=paths.state_root().resolve(),
        )

    def test_update_env_falls_back_to_pause_all_without_provides_branches(
        self, state_dir: Path
    ) -> None:
        """No provides_branches → preserve pre-v0.5.47 behavior: pause
        the entire queue."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        # v0.11.1: the non-surgical path tags its pause with a
        # per-invocation token and resumes only jobs carrying it, so a
        # prior interrupted update's stragglers stay paused. Assert the
        # SAME token flows pause -> resume (the scoping is what matters).
        mock_pause.assert_called_once()
        assert mock_pause.call_args.args[0] == "localhost"
        token = mock_pause.call_args.args[1]
        assert token and token.startswith("admin-update-")
        assert mock_pause.call_args.kwargs == {
            "branches": None,
            "multi_user": False,
            "queue_root": paths.state_root().resolve(),
            "exclude_jobids": None,
        }
        mock_resume.assert_called_once_with(
            "localhost",
            token,
            multi_user=False,
            queue_root=paths.state_root().resolve(),
        )

    def test_empty_provides_branches_list_falls_back(
        self, state_dir: Path
    ) -> None:
        """`provides_branches = []` is empty (bool([]) is False), so
        treated the same as None — falls back to pause_all. Tests the
        truthy-check semantics rather than `is None`."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'provides_branches = []\n'
        )
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        mock_pause.assert_called_once()
        assert mock_pause.call_args.kwargs["branches"] is None
        mock_resume.assert_called_once()

    def test_resume_jobs_called_with_paused_list_on_failure_too(
        self, state_dir: Path
    ) -> None:
        """Even when the git pull fails, the finally block must still
        call resume_jobs with the originally-paused list — symmetry
        with pause_all/resume_all on the fallback path."""
        repo = _make_git_repo(state_dir / "repo")
        (state_dir / "cfg" / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{repo}"\n'
            'branch = "main"\n'
            'provides_branches = ["main"]\n'
        )
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 2 jobs"),
            ),
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 2 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_fail_proc(1, stderr="network down")],
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert not result.success
        # Even on failure, the invocation's durable token scope is resumed.
        mock_resume.assert_called_once()
        assert mock_resume.call_args.args[0] == "localhost"
        assert mock_resume.call_args.args[1].startswith("admin-update-")
        assert mock_resume.call_args.kwargs == {
            "multi_user": False,
            "queue_root": paths.state_root().resolve(),
        }


class TestAdminPauseResumeProofRetention:
    def test_single_pause_admission_failure_blocks_work_and_keeps_marker(
        self, state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        pause_failure = PauseError("eligible job remains RUNNING")
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub(
                    "pause admission NOT proved", pause_error=pause_failure,
                ),
            ),
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("token scope clear"),
            ) as resume,
            patch("vq.admin._do_update_work") as work,
            pytest.raises(PauseError, match="eligible job remains RUNNING"),
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        work.assert_not_called()
        resume.assert_called_once()
        marker = admin.read_admin_update_marker()
        assert marker is not None
        # Resume proof succeeded, so the failed-update marker remains while
        # its now-empty pause authority is safely disarmed.
        assert marker.pause_token is None

    def test_single_partial_resume_proof_keeps_marker(
        self, state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("pause admission proved"),
            ),
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub(
                    "token scope NOT clear",
                    resume_error=PauseError("job two still SUSPENDED"),
                ),
            ),
            patch(
                "vq.admin._do_update_work",
                return_value=admin.UpdateResult(
                    env="vibeqc-dev",
                    git_dir=str(repo),
                    branch="main",
                    update_script=None,
                    git_pull_rc=0,
                ),
            ),
            pytest.raises(PauseError, match="job two still SUSPENDED"),
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")

        marker = admin.read_admin_update_marker()
        assert marker is not None
        assert marker.pause_token is not None
        assert marker.pause_multi_user is False
        assert marker.pause_queue_root == str(paths.state_root().resolve())
        assert marker.managed_transaction is None

    def test_batch_partial_resume_proof_keeps_marker(
        self, state_dir: Path,
    ) -> None:
        _write_two_venv_config(state_dir)
        cfg = config.load_config()

        def work(name: str, prog: config.VenvProgram, **kwargs: object):
            del kwargs
            return admin.UpdateResult(
                env=name,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                git_pull_rc=0,
                update_script_rc=0 if prog.update_script else None,
            )

        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("batch pause admission proved"),
            ),
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub(
                    "batch token scope NOT clear",
                    resume_error=PauseError("one batch job remains"),
                ),
            ),
            patch("vq.admin._do_update_work", side_effect=work) as update_work,
            pytest.raises(PauseError, match="one batch job remains"),
        ):
            admin.update_all(cfg, host="localhost")

        assert update_work.call_count == 2
        marker = admin.read_admin_update_marker()
        assert marker is not None
        assert marker.pause_token is not None
        assert marker.pause_multi_user is False
        assert marker.pause_queue_root == str(paths.state_root().resolve())
        assert marker.managed_transaction is None


# ----------------------------------------------------------------------
# v0.5.48 Bug A regression: marker-clear must run AFTER
# _maybe_restart_daemon so a failed self-update restart (which flips
# result.success to False) leaves the marker on disk. Pre-v0.5.48 the
# clear was in the finally block before the restart attempt — a failed
# restart silently cleared the marker.
# ----------------------------------------------------------------------


class TestMarkerClearOrderingAfterDaemonRestart:
    """v0.5.48 (Bug A): if vq admin update succeeds for the on-disk
    work but the daemon-restart step fails (systemctl unreachable,
    timeout, non-zero rc), the marker must remain on disk."""

    def test_marker_stays_when_daemon_restart_fails(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        # Make _maybe_restart_daemon fail the result by patching it to
        # set daemon_restart_attempted=True + succeeded=False, which
        # flips result.success to False via the UpdateResult.success
        # property's daemon-restart check.
        def _fake_restart(
            prog,
            result,
            *,
            restart_daemon,
            require_self_update=False,
            multi_user=False,
        ):
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = False
            result.daemon_restart_message = "fake: systemctl unreachable"

        with (
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
            patch(
                "vq.admin._maybe_restart_daemon",
                side_effect=_fake_restart,
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        # The update_script (none) + git pull succeeded, but the daemon
        # restart failed — the UpdateResult.success property flips to
        # False because of the daemon-restart check.
        assert not result.success
        # CRITICAL: marker stayed on disk so the next vq admin update
        # is blocked by the guard with the recovery recipe.
        assert admin.admin_update_marker_exists() is True

    def test_marker_cleared_when_daemon_restart_succeeds(
        self, state_dir: Path
    ) -> None:
        """The happy path: on-disk work succeeded AND daemon restart
        succeeded → marker cleared. Same test shape as the older
        TestAdminUpdateMarkerGuard happy-path test, but with an
        explicit successful _maybe_restart_daemon side-effect to make
        the ordering contract explicit."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        def _fake_restart_ok(
            prog,
            result,
            *,
            restart_daemon,
            require_self_update=False,
            multi_user=False,
        ):
            # Self-update detected, restart succeeded. Doesn't flip
            # result.success.
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = True
            result.daemon_health_verified = True
            result.daemon_restart_message = "fake: restart ok"

        with (
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
            patch(
                "vq.admin._maybe_restart_daemon",
                side_effect=_fake_restart_ok,
            ),
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        assert admin.admin_update_marker_exists() is False

    def test_update_all_marker_stays_when_daemon_restart_fails(
        self, state_dir: Path
    ) -> None:
        """Same ordering contract for the batch path: failed restart
        on the vq self-update env in a batch must leave the marker
        on disk even if every other env succeeded."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()

        def _fake_restart_fail(
            prog,
            result,
            *,
            restart_daemon,
            require_self_update=False,
            multi_user=False,
        ):
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = False
            result.daemon_restart_message = "fake: systemctl unreachable"

        with (
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
            patch(
                "vq.admin._maybe_restart_daemon",
                side_effect=_fake_restart_fail,
            ),
        ):
            results = admin.update_all(cfg, host="localhost")
        # Single-env batch (only vibeqc-dev configured) → batch fails
        # because that env's restart failed.
        assert not all(r.success for r in results)
        assert admin.admin_update_marker_exists() is True

    def test_deferred_single_update_restart_identity_loss_is_failure(
        self,
        state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            name="vibeqc-queue",
            git_dir=str(repo),
            update_script="vibe-queue/scripts/update.sh",
        )
        cfg = config.load_config()
        initial = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="initial exact target",
        )
        managed: list[bool] = []
        lifecycle = SimpleNamespace(terminal_verified=False)

        def work(name, prog, **kwargs):
            managed.append(kwargs["managed_daemon_restart"])
            return admin.UpdateResult(
                env=name,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                git_pull_rc=0,
                update_script_rc=0,
            )

        def lose_identity(prog, result, active_lifecycle):
            assert active_lifecycle is lifecycle
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = False
            result.daemon_health_verified = False
            result.work_errors.append("lost daemon target identity")
            active_lifecycle.terminal_verified = True

        with (
            patch("vq.admin._detect_vq_self_update", return_value=initial),
            patch("vq.admin._guard_git_index_unlocked", return_value=None),
            patch("vq.admin._managed_update_script_args", return_value=[]),
            patch(
                "vq.admin._begin_managed_daemon_update",
                return_value=lifecycle,
            ),
            patch(
                "vq.admin._complete_managed_daemon_update",
                side_effect=lose_identity,
            ),
            patch("vq.admin._do_update_work", side_effect=work),
        ):
            result = admin.update_env(
                "vibeqc-queue",
                cfg,
                host="localhost",
            )

        assert managed == [True]
        assert result.success is False
        assert any("lost daemon target identity" in item for item in result.work_errors)
        assert admin.admin_update_marker_exists() is True

    def test_deferred_batch_restart_identity_loss_is_failure(
        self,
        state_dir: Path,
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            name="vibeqc-queue",
            git_dir=str(repo),
            update_script="vibe-queue/scripts/update.sh",
        )
        cfg = config.load_config()
        initial = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="initial exact target",
        )
        lifecycle = SimpleNamespace(terminal_verified=False)

        def work(name, prog, **kwargs):
            assert kwargs["managed_daemon_restart"] is True
            return admin.UpdateResult(
                env=name,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                git_pull_rc=0,
                update_script_rc=0,
            )

        def lose_identity(prog, result, active_lifecycle):
            assert active_lifecycle is lifecycle
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = False
            result.daemon_health_verified = False
            result.work_errors.append("lost daemon target identity")
            active_lifecycle.terminal_verified = True

        with (
            patch("vq.admin._detect_vq_self_update", return_value=initial),
            patch("vq.admin._managed_update_script_args", return_value=[]),
            patch(
                "vq.admin._begin_managed_daemon_update",
                return_value=lifecycle,
            ),
            patch(
                "vq.admin._complete_managed_daemon_update",
                side_effect=lose_identity,
            ),
            patch("vq.admin._do_update_work", side_effect=work),
        ):
            results = admin.update_all(cfg, host="localhost")

        assert len(results) == 1
        assert results[0].success is False
        assert any(
            "lost daemon target identity" in item
            for item in results[0].work_errors
        )
        assert admin.admin_update_marker_exists() is True


class TestAdminUpdateOwnershipLock:
    """The report-fetch/checkout lock is a stable owner-only inode."""

    def test_creates_owner_only_regular_lock(self, state_dir: Path) -> None:
        with admin.admin_update_ownership():
            path = admin._admin_update_ownership_lock_path()
            info = path.stat(follow_symlinks=False)
            assert info.st_nlink == 1
            assert info.st_mode & 0o777 == 0o600

        assert path.is_file()

    def test_refuses_symlink_lock_without_touching_target(
        self,
        state_dir: Path,
    ) -> None:
        path = admin._admin_update_ownership_lock_path()
        victim = state_dir / "victim"
        victim.write_text("unchanged")
        path.symlink_to(victim)

        with (
            pytest.raises(admin.AdminError, match="unsafe.*ownership lock"),
            admin.admin_update_ownership(),
        ):
            pytest.fail("symlink lock was accepted")

        assert victim.read_text() == "unchanged"

    def test_refuses_nonregular_and_hardlinked_lock(
        self,
        state_dir: Path,
    ) -> None:
        path = admin._admin_update_ownership_lock_path()
        path.mkdir()
        with (
            pytest.raises(admin.AdminError, match="unsafe.*ownership lock"),
            admin.admin_update_ownership(),
        ):
            pytest.fail("directory lock was accepted")
        path.rmdir()

        seed = state_dir / "seed-lock"
        seed.write_text("")
        seed.chmod(0o600)
        os.link(seed, path)
        with (
            pytest.raises(admin.AdminError, match="one link"),
            admin.admin_update_ownership(),
        ):
            pytest.fail("hardlinked lock was accepted")

    def test_nonroot_refuses_group_writable_lock_directory(
        self,
        state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root = paths.state_root()
        root.chmod(0o770)
        monkeypatch.setattr(admin.os, "geteuid", lambda: root.stat().st_uid + 1)

        with (
            pytest.raises(admin.AdminError, match="unsafe.*directory"),
            admin.admin_update_ownership(),
        ):
            pytest.fail("unsafe parent was accepted")


# ----------------------------------------------------------------------
# v0.5.50: atomic admin-update marker via acquire_admin_update_marker
# (O_CREAT|O_EXCL). Closes audit § 2c race: pre-v0.5.50 the guard
# (existence check) and the writer were two separate calls, so two
# concurrent admin updates could both pass the check then race the
# write — second overwrote the first silently.
# ----------------------------------------------------------------------


class TestAcquireAdminUpdateMarker:
    """v0.5.50: acquire_admin_update_marker(envs, host, force=False)
    is the atomic check-and-write replacement for the
    guard+write pair. force=True keeps the pre-v0.5.50 overwrite
    semantics so the --force flag still works."""

    def test_first_acquire_succeeds(self, state_dir: Path) -> None:
        marker = admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        assert marker.envs == ["vibeqc-dev"]
        assert admin.admin_update_marker_exists() is True

    def test_second_acquire_without_force_raises(
        self, state_dir: Path
    ) -> None:
        """The contention case: marker already on disk → AdminError
        with the recovery recipe. This is the race-free claim — pre-
        v0.5.50 a second writer would silently overwrite."""
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        with pytest.raises(admin.AdminError) as exc_info:
            admin.acquire_admin_update_marker(
                envs=["vibeqc-dev"], host="host_d",
            )
        msg = str(exc_info.value)
        assert "admin-update-in-progress marker present" in msg
        assert "vibeqc-dev" in msg  # original marker's env, not the new one
        assert "marker_status=running" in msg
        assert "already running" in msg
        assert "vq admin status --verbose" in msg

    def test_force_overwrites_existing(self, state_dir: Path) -> None:
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        new_marker = admin.acquire_admin_update_marker(
            envs=["vibeqc-release"], host="host_d", force=True,
        )
        assert new_marker.envs == ["vibeqc-release"]
        # The marker on disk reflects the force-write, not the
        # original.
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.envs == ["vibeqc-release"]

    def test_o_excl_atomicity(self, state_dir: Path) -> None:
        """Race-correctness: even if we manually create the marker
        file BETWEEN the (nonexistent) guard call and the write,
        the acquire fails. With pre-v0.5.50 write_admin_update_marker,
        the test's manual write would have been overwritten —
        because os.replace doesn't care if the destination exists."""
        # Manually create the marker via a low-level write (simulating
        # another process having beaten us to it).
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Use O_CREAT|O_EXCL to confirm the slot was empty before our
        # manual write.
        fd = os.open(
            str(path),
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o644,
        )
        try:
            os.write(fd, b'{"envs":["vibeqc-dev"],"host":"x",'
                         b'"started_at":"t","pid":1,"vq_version":"v"}')
        finally:
            os.close(fd)
        # Now acquire must fail.
        with pytest.raises(admin.AdminError):
            admin.acquire_admin_update_marker(
                envs=["vibeqc-dev"], host="host_d",
            )
        # And the original (sneaky) marker is untouched.
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.envs == ["vibeqc-dev"]

    def test_acquire_unreadable_marker_still_blocks(
        self, state_dir: Path
    ) -> None:
        """A corrupt marker file in the slot still blocks acquire
        (atomicity comes from the kernel, not from the JSON parse)."""
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("garbage not json")
        with pytest.raises(admin.AdminError) as exc_info:
            admin.acquire_admin_update_marker(
                envs=["vibeqc-dev"], host="host_d",
            )
        assert "unreadable" in str(exc_info.value)


class TestAdminUpdateStateMachine:
    """v0.6.0: state machine extension of the marker file. Initial
    acquire sets state=pausing; transition rewrites the file
    atomically; on success the file is removed; on failure the
    state transitions to FAILED (sticky)."""

    def test_acquire_sets_initial_state_pausing(
        self, state_dir: Path
    ) -> None:
        marker = admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        assert marker.state == admin.ADMIN_UPDATE_STATE_PAUSING
        assert marker.phase_started_at  # non-empty
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.state == admin.ADMIN_UPDATE_STATE_PAUSING

    def test_transition_updates_state_and_phase_started_at(
        self, state_dir: Path
    ) -> None:
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        initial = admin.read_admin_update_marker()
        assert initial is not None
        first_phase_ts = initial.phase_started_at

        # Sleep to ensure timestamps differ at second-precision.
        import time as _time
        _time.sleep(0.01)
        updated = admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_PULLING,
        )
        assert updated is not None
        assert updated.state == admin.ADMIN_UPDATE_STATE_PULLING
        assert updated.phase_started_at != first_phase_ts
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.state == admin.ADMIN_UPDATE_STATE_PULLING

    def test_transition_on_no_marker_returns_none(
        self, state_dir: Path
    ) -> None:
        out = admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_PULLING,
        )
        assert out is None

    def test_failure_transition_records_reason(
        self, state_dir: Path
    ) -> None:
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        updated = admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED,
            failure_reason="git pull rc=128",
        )
        assert updated is not None
        assert updated.state == admin.ADMIN_UPDATE_STATE_FAILED
        assert updated.failure_reason == "git pull rc=128"

    def test_legacy_marker_state_defaults_to_legacy(
        self, state_dir: Path
    ) -> None:
        """A pre-v0.6.0 marker on disk (no state field) reads as
        ADMIN_UPDATE_STATE_LEGACY so the guard still fires and the
        operator sees the marker is from a pre-state-machine vq."""
        import json as _json
        path = admin.admin_update_marker_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Legacy payload — no `state` field, no `phase_started_at`,
        # no `failure_reason`.
        path.write_text(_json.dumps({
            "envs": ["vibeqc-dev"],
            "host": "host_d",
            "started_at": "2026-05-17T00:00:00+00:00",
            "pid": 1234,
            "vq_version": "0.5.43",
        }))
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.state == admin.ADMIN_UPDATE_STATE_LEGACY

    def test_status_banner_includes_state_and_failure_reason(
        self, state_dir: Path
    ) -> None:
        from vq import config as cfg_mod
        (state_dir / "cfg" / "config.toml").write_text(
            'default_host = "localhost"\n'
        )
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="host_d",
        )
        admin.transition_admin_update_state(
            admin.ADMIN_UPDATE_STATE_FAILED,
            failure_reason="update_script rc=2",
        )
        cfg = cfg_mod.load_config()
        text = admin.format_admin_status(cfg)
        assert "marker_status: failed" in text
        assert "state:       failed" in text
        assert "failure:     update_script rc=2" in text
        assert "vq admin clear-update-marker" in text
        assert "phase_started" in text


class TestUpdateEnvDrivesStateMachine:
    """Integration: update_env transitions the marker through the
    full state sequence and ends with the file removed on success
    or the sticky FAILED state on failure."""

    def test_success_path_clears_marker(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_ok_proc(stdout="ok")],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert result.success
        # Marker file removed entirely on success.
        assert admin.admin_update_marker_exists() is False

    def test_failure_path_transitions_to_sticky_FAILED(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        with patch(
            "vq.admin.subprocess.run",
            side_effect=[_fail_proc(128, stderr="network down")],
        ):
            result = admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert not result.success
        assert admin.admin_update_marker_exists() is True
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.state == admin.ADMIN_UPDATE_STATE_FAILED
        # Failure reason carries the specific signal.
        assert loaded.failure_reason is not None
        assert "git pull rc=128" in loaded.failure_reason


class TestStateMachinePhaseSplits:
    """v0.6.1: TAG_CHECKING fires inside _do_update_work when --tag
    is given; BUILDING fires when update_script is configured.
    These are interior transitions of the PULLING phase from
    v0.6.0 — finer "stuck where" diagnosis in the status banner."""

    def test_tag_check_phase_transition_fires(
        self, state_dir: Path
    ) -> None:
        """Spy on transition_admin_update_state — when --tag is
        used, TAG_CHECKING must appear in the call sequence."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        seen_states: list[str] = []
        orig = admin.transition_admin_update_state

        def _spy(new_state, *, failure_reason=None):
            seen_states.append(new_state)
            return orig(new_state, failure_reason=failure_reason)

        with (
            patch("vq.admin.transition_admin_update_state", side_effect=_spy),
            patch(
                "vq.admin.subprocess.run",
                side_effect=[
                    _ok_proc(stdout="ok"),       # git fetch tag
                    _ok_proc(stdout="detached"),  # git checkout tag
                    _ok_proc(stdout="v0.7.13\n"),  # git describe
                ],
            ),
        ):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                expected_tag="v0.7.13",
            )
        # TAG_CHECKING must appear between PULLING and VERIFYING.
        assert admin.ADMIN_UPDATE_STATE_TAG_CHECKING in seen_states
        # And BUILDING must NOT appear since update_script is None.
        assert admin.ADMIN_UPDATE_STATE_BUILDING not in seen_states

    def test_building_phase_transition_fires_for_update_script(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        (repo / "scripts").mkdir()
        (repo / "scripts" / "update-dev.sh").write_text("#!/bin/bash\n")
        _write_venv_program(state_dir / "cfg", git_dir=str(repo))
        cfg = config.load_config()
        seen_states: list[str] = []
        orig = admin.transition_admin_update_state

        def _spy(new_state, *, failure_reason=None):
            seen_states.append(new_state)
            return orig(new_state, failure_reason=failure_reason)

        with (
            patch("vq.admin.transition_admin_update_state", side_effect=_spy),
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok"), _ok_proc(stdout="built")],
            ),
        ):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        # BUILDING fires when update_script is configured + git pull ok.
        assert admin.ADMIN_UPDATE_STATE_BUILDING in seen_states
        # No --tag → TAG_CHECKING does NOT appear.
        assert admin.ADMIN_UPDATE_STATE_TAG_CHECKING not in seen_states

    def test_no_tag_no_script_skips_both_subphases(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        seen_states: list[str] = []
        orig = admin.transition_admin_update_state

        def _spy(new_state, *, failure_reason=None):
            seen_states.append(new_state)
            return orig(new_state, failure_reason=failure_reason)

        with (
            patch("vq.admin.transition_admin_update_state", side_effect=_spy),
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        # Neither sub-phase appeared.
        assert admin.ADMIN_UPDATE_STATE_TAG_CHECKING not in seen_states
        assert admin.ADMIN_UPDATE_STATE_BUILDING not in seen_states


class TestRestartingDaemonTransitionAttemptedOnly:
    """v0.6.1: RESTARTING_DAEMON fires only when _maybe_restart_daemon
    actually issues a restart (a vq self-update env that detection
    confirms). Non-self-update envs skip the misleading transition."""

    def test_non_self_update_env_skips_restarting_daemon_transition(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        cfg = config.load_config()
        seen_states: list[str] = []
        orig = admin.transition_admin_update_state

        def _spy(new_state, *, failure_reason=None):
            seen_states.append(new_state)
            return orig(new_state, failure_reason=failure_reason)

        # Mock _detect_vq_self_update to return "not self-update"
        # so _maybe_restart_daemon takes the no-op branch — no
        # transition into RESTARTING_DAEMON expected.
        fake_probe = admin._SelfUpdateProbe(
            is_self_update=False,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="(test: not vq's venv)",
        )
        with (
            patch("vq.admin.transition_admin_update_state", side_effect=_spy),
            patch(
                "vq.admin._detect_vq_self_update",
                lambda prog: fake_probe,
            ),
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        assert admin.ADMIN_UPDATE_STATE_RESTARTING_DAEMON not in seen_states
        # But VERIFYING did fire (we still complete the run).
        assert admin.ADMIN_UPDATE_STATE_VERIFYING in seen_states

    def test_self_update_env_fires_restarting_daemon_transition(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg",
            name="vibeqc-queue",
            git_dir=str(repo),
            update_script="vibe-queue/scripts/update.sh",
        )
        cfg = config.load_config()
        seen_states: list[str] = []
        orig = admin.transition_admin_update_state

        def _spy(new_state, *, failure_reason=None):
            seen_states.append(new_state)
            return orig(new_state, failure_reason=failure_reason)

        # Self-update probe + mocked outer managed-daemon transaction.
        fake_probe = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=True,
            service_manager="systemd",
            manager_available=True,
            diagnostic="(test: vq's venv)",
        )
        lifecycle = SimpleNamespace(terminal_verified=False)

        def work(name, prog, **kwargs):
            assert kwargs["managed_daemon_restart"] is True
            return admin.UpdateResult(
                env=name,
                git_dir=prog.git_dir,
                branch=prog.branch,
                update_script=prog.update_script,
                git_pull_rc=0,
                update_script_rc=0,
            )

        def complete(prog, result, active_lifecycle):
            admin.transition_admin_update_state(
                admin.ADMIN_UPDATE_STATE_RESTARTING_DAEMON,
            )
            result.daemon_restart_attempted = True
            result.daemon_restart_succeeded = True
            result.daemon_health_verified = True
            active_lifecycle.terminal_verified = True

        with (
            patch("vq.admin.transition_admin_update_state", side_effect=_spy),
            patch(
                "vq.admin._detect_vq_self_update",
                lambda prog: fake_probe,
            ),
            patch("vq.admin._guard_git_index_unlocked", return_value=None),
            patch(
                "vq.admin._managed_update_script_args",
                return_value=[],
            ),
            patch(
                "vq.admin._begin_managed_daemon_update",
                return_value=lifecycle,
            ),
            patch(
                "vq.admin._complete_managed_daemon_update",
                side_effect=complete,
            ),
            patch("vq.admin._do_update_work", side_effect=work),
        ):
            admin.update_env(
                "vibeqc-queue", cfg, host="localhost",
            )
        # RESTARTING_DAEMON appeared because the self-update path
        # actually issued the restart.
        assert admin.ADMIN_UPDATE_STATE_RESTARTING_DAEMON in seen_states


class TestMaybeRestartDaemonContractWiring:
    """Unsupported service-manager paths fail closed with diagnostics."""

    def test_contract_findings_in_unreachable_message(
        self, state_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Build a probe that says "is a vq self-update, systemctl
        # not ok" so the unreachable branch fires.
        fake_probe = admin._SelfUpdateProbe(
            is_self_update=True,
            daemon_running=None,
            service_manager=None,
            manager_available=False,
            diagnostic="(fake probe diagnostic)",
        )
        monkeypatch.setattr(
            admin, "_detect_vq_self_update",
            lambda prog: fake_probe,
        )

        # Build a minimal venv program + result for _maybe_restart_daemon.
        from vq import config as config_mod
        prog = config_mod.VenvProgram(
            kind="venv",
            python="/fake/.venv/bin/python",
            git_dir="/fake/.venv",
        )
        result = admin.UpdateResult(
            env="vibeqc-queue",
            git_dir="/fake/.venv",
            branch="main",
            update_script=None,
            git_pull_rc=0,
        )
        admin._maybe_restart_daemon(prog, result, restart_daemon=True)

        msg = result.daemon_restart_message
        assert result.daemon_restart_attempted is True
        assert result.daemon_restart_succeeded is False
        # The probe diagnostic is still there for backward compat.
        assert "(fake probe diagnostic)" in msg
        assert "no verified supported service-manager" in msg
        assert "operations.md" in msg
        assert "lifecycle.md" in msg


class TestUpdateEnvUsesAcquire:
    """Integration check: update_env routes through
    acquire_admin_update_marker (the production path)."""

    def test_concurrent_update_blocked_by_acquire(
        self, state_dir: Path
    ) -> None:
        """Simulating the concurrent-update race: an existing marker
        on disk (placed before update_env starts pause) causes
        update_env to fail at the acquire step, with no queue
        disruption from a successful-then-overwritten pause."""
        repo = _make_git_repo(state_dir / "repo")
        _write_venv_program(
            state_dir / "cfg", git_dir=str(repo), update_script=None,
        )
        # Marker already on disk — another admin update is "in flight".
        admin.acquire_admin_update_marker(
            envs=["vibeqc-dev"], host="otherhost",
        )
        cfg = config.load_config()
        with pytest.raises(admin.AdminError):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
            )
        # The original marker survived — no overwrite.
        loaded = admin.read_admin_update_marker()
        assert loaded is not None
        assert loaded.envs == ["vibeqc-dev"]


class TestMultiUserUpdatePause:
    """v0.6.42: `vq admin update` threads ``multi_user=`` into the
    surgical pause/resume. On a multi-user host the update runs as
    root, so the queue-quiesce must reach every user's jobs — the
    pause/resume helpers resolve specs from the per-user state dirs
    only when told the mode."""

    _MU_HEADER = "[multi_user]\nenabled = true\n\n"

    def _write_mu_config(
        self, state_dir: Path, repo: Path, *, provides_branches: bool = False
    ) -> None:
        lines = [
            self._MU_HEADER,
            "[programs.vibeqc-dev]\n",
            'kind = "venv"\n',
            'python = "/fake/python"\n',
            f'git_dir = "{repo}"\n',
            'branch = "main"\n',
        ]
        if provides_branches:
            lines.append('provides_branches = ["main"]\n')
        (state_dir / "cfg" / "config.toml").write_text("".join(lines))

    def test_update_env_pause_all_threads_multi_user(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        self._write_mu_config(state_dir, repo)
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        # v0.11.1: non-surgical pause now carries a per-invocation token;
        # the resume filters on the SAME token. multi_user must still be
        # threaded into both (the point of this test).
        mock_pause.assert_called_once()
        assert mock_pause.call_args.args[0] == "localhost"
        token = mock_pause.call_args.args[1]
        assert token and token.startswith("admin-update-")
        assert mock_pause.call_args.kwargs["multi_user"] is True
        mock_resume.assert_called_once_with(
            "localhost",
            token,
            multi_user=True,
            queue_root=paths.multi_user_root().resolve(),
        )

    def test_update_env_surgical_pause_threads_multi_user(
        self, state_dir: Path
    ) -> None:
        repo = _make_git_repo(state_dir / "repo")
        self._write_mu_config(state_dir, repo, provides_branches=True)
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        mock_pause.assert_called_once()
        assert mock_pause.call_args.args[0] == "localhost"
        token = mock_pause.call_args.args[1]
        assert token.startswith("admin-update-")
        assert mock_pause.call_args.kwargs == {
            "branches": ["main"],
            "multi_user": True,
            "queue_root": paths.multi_user_root().resolve(),
            "exclude_jobids": None,
        }
        mock_resume.assert_called_once_with(
            "localhost",
            token,
            multi_user=True,
            queue_root=paths.multi_user_root().resolve(),
        )

    def test_update_all_threads_multi_user(self, state_dir: Path) -> None:
        repo = _make_git_repo(state_dir / "repo")
        self._write_mu_config(state_dir, repo)
        cfg = config.load_config()
        with (
            patch(
                "vq.admin.pause_token_scope_with_proof",
                return_value=_ProofStub("paused 0 jobs"),
            ) as mock_pause,
            patch(
                "vq.admin.resume_token_scope_with_proof",
                return_value=_ProofStub("resumed 0 jobs"),
            ) as mock_resume,
            patch(
                "vq.admin.subprocess.run",
                side_effect=[_ok_proc(stdout="ok")],
            ),
        ):
            admin.update_all(cfg, host="localhost")
        # v0.11.1: batch pause carries a per-invocation token; the batch
        # resume filters on the SAME token. multi_user still threaded.
        mock_pause.assert_called_once()
        assert mock_pause.call_args.args[0] == "localhost"
        token = mock_pause.call_args.args[1]
        assert token and token.startswith("admin-update-")
        assert mock_pause.call_args.kwargs["multi_user"] is True
        mock_resume.assert_called_once_with(
            "localhost",
            token,
            multi_user=True,
            queue_root=paths.multi_user_root().resolve(),
        )


class TestInstalledShaTracksTheInstallNotTheCheckout:
    """`last_installed_sha` answers "what commit is actually installed?".

    Distinct from `last_sha`, which is where the checkout ended up. They agree
    after a successful update and diverge after a rolled-back one: the rollback
    resets the tree but never re-runs the editable install, so `.dist-info`
    keeps describing the previous commit. Localhost, 2026-08-01: a failed build
    left the checkout months behind while `vibeqc.__version__` still read the
    newer version, and nothing on any surface distinguished that from health.
    """

    def test_a_successful_update_records_what_it_installed(self) -> None:
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-08-01T00:00:00Z",
            last_success=True,
            last_sha="a" * 40,
            last_installed_sha="a" * 40,
        )

        assert rec.last_installed_sha == rec.last_sha

    def test_mismatch_is_reported_when_the_checkout_moved_underneath(
        self,
    ) -> None:
        """The rolled-back shape: installed newer, checkout older."""
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-08-01T00:00:00Z",
            last_success=False,
            last_sha="b" * 40,
            last_installed_sha="a" * 40,
        )
        st = SimpleNamespace(current_sha="b" * 40)

        assert admin._installed_matches(rec, st) is False

    def test_agreement_is_reported_when_they_match(self) -> None:
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-08-01T00:00:00Z",
            last_success=True,
            last_installed_sha="a" * 40,
        )
        st = SimpleNamespace(current_sha="a" * 40)

        assert admin._installed_matches(rec, st) is True

    def test_short_form_checkout_sha_still_compares(self) -> None:
        """`current_sha` is short-form on some paths; a prefix match is not a
        mismatch."""
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-08-01T00:00:00Z",
            last_success=True,
            last_installed_sha="abcdef0123456789" + "0" * 24,
        )
        st = SimpleNamespace(current_sha="abcdef012345")

        assert admin._installed_matches(rec, st) is True

    def test_unknown_either_side_is_not_reported_as_agreement(self) -> None:
        """Absence of evidence must not read as health -- that is the exact
        failure this field exists to stop."""
        rec = admin.AdminUpdateRecord(
            last_updated_at="2026-08-01T00:00:00Z",
            last_success=True,
        )

        assert admin._installed_matches(rec, SimpleNamespace(current_sha="a" * 40)) is None
        assert admin._installed_matches(None, SimpleNamespace(current_sha="a" * 40)) is None


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    """Run git in ``repo`` with an identity, so these tests work on a box
    with no global git config (CI containers, fresh dev VMs)."""
    return subprocess.run(
        ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", *args],
        capture_output=True,
        text=True,
        check=True,
    )


class TestSourceShaMarkerCannotOutliveItsCode:
    """A ``SOURCE-SHA`` marker asserts "this package directory is commit X".

    Nothing bound the two. The marker is written *after* ``pip install``
    (`docs/multi_user_deployment.md`), so it is absent from the wheel's
    ``RECORD``; pip removes only files it tracks, so an upgrade leaves the
    previous marker orphaned inside the *new* package directory, and the new
    code then reports the *previous* commit through any number of reinstalls
    and daemon restarts. host_d, 2026-08-02: a frozen ``389190467553…``
    survived three update attempts across two hours.

    The fix records the tree digest the marker was written against, so a marker
    that no longer describes the code beside it reads as absent -- fail-closed,
    naming a real remedy -- instead of confidently naming a commit that is not
    installed.

    This does not close the runbook's *ordering* trap (`--write-marker` run
    before that host's checkout reaches the pin, stamping a pin onto an
    honestly-installed older build). That marker is self-consistent; catching
    it needs the SHA derived from the install source rather than passed in by
    hand.
    """

    @staticmethod
    def _package(tmp_path: Path) -> Path:
        pkg = tmp_path / "site-packages" / "vq"
        pkg.mkdir(parents=True)
        (pkg / "__init__.py").write_text("__version__ = '1'\n", encoding="utf-8")
        (pkg / "daemon.py").write_text("x = 1\n", encoding="utf-8")
        return pkg

    def test_a_fresh_marker_reads_back(self, tmp_path: Path) -> None:
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME

        admin.write_source_sha_marker("a" * 40, marker)

        assert admin.read_source_sha_marker(marker) == "a" * 40

    def test_a_marker_orphaned_by_an_upgrade_reads_as_absent(
        self, tmp_path: Path
    ) -> None:
        """The host_d shape: the code underneath the marker was replaced."""
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        admin.write_source_sha_marker("a" * 40, marker)

        # pip install of a newer vq: package files replaced, untracked marker
        # left behind untouched.
        (pkg / "daemon.py").write_text("x = 2  # the next release\n", encoding="utf-8")

        assert admin.read_source_sha_marker(marker) is None

    def test_a_rollback_under_a_restamped_marker_reads_as_absent(
        self, tmp_path: Path
    ) -> None:
        """A marker stays bound across restamps: roll the tree back after the
        marker was honestly rewritten and it goes stale again.

        Note the limit. This catches the tree moving *under* a marker. It does
        not catch `--write-marker <new-sha>` run against a genuinely-installed
        *old* tree -- the runbook's ordering trap, where the operator names a
        pin the checkout has not reached yet. That marker is self-consistent
        and still a lie; see `docs/fleet_update_runbook.md`.
        """
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        admin.write_source_sha_marker("a" * 40, marker)
        stale_body = (pkg / "daemon.py").read_text(encoding="utf-8")

        # Rebuild lands the new code, marker restamped honestly...
        (pkg / "daemon.py").write_text("x = 2\n", encoding="utf-8")
        admin.write_source_sha_marker("b" * 40, marker)
        assert admin.read_source_sha_marker(marker) == "b" * 40

        # ...then a rollback resets the tree without rewriting the marker.
        (pkg / "daemon.py").write_text(stale_body, encoding="utf-8")
        assert admin.read_source_sha_marker(marker) is None

    def test_a_legacy_bare_sha_marker_is_still_accepted(self, tmp_path: Path) -> None:
        """Every helper deployed before this change carries a bare 40-hex
        marker. Rejecting those flips the whole fleet to "no marker" at once,
        and `vq source-sha` is a scheduler-compat gate -- it would fail every
        lane simultaneously. Absent digest line = legacy = trusted.
        """
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        marker.write_text("c" * 40 + "\n", encoding="utf-8")

        assert admin.read_source_sha_marker(marker) == "c" * 40

    def test_the_provenance_markers_do_not_invalidate_each_other(
        self, tmp_path: Path
    ) -> None:
        """`admin.source_tree_sha256` excludes both marker names, and must keep
        doing so -- a digest that covered its own output would never be stable.
        The binding comes from *storing* the digest, not from digesting the
        marker.
        """
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        admin.write_source_sha_marker("a" * 40, marker)

        # Helper deploys land this one alongside the SHA marker.
        (pkg / admin.SOURCE_TREE_SHA256_NAME).write_text("f" * 64 + "\n", encoding="utf-8")

        assert admin.read_source_sha_marker(marker) == "a" * 40

    def test_bytecode_churn_does_not_invalidate_the_binding(
        self, tmp_path: Path
    ) -> None:
        """First run of the installed package writes ``__pycache__``. That must
        not read as "the code changed"."""
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        admin.write_source_sha_marker("a" * 40, marker)

        cache = pkg / "__pycache__"
        cache.mkdir()
        (cache / "daemon.cpython-313.pyc").write_bytes(b"\x00\x01compiled")

        assert admin.read_source_sha_marker(marker) == "a" * 40

    def test_an_undigestible_tree_is_tolerated_not_rejected(
        self, tmp_path: Path
    ) -> None:
        """Read side tolerant: if the current digest cannot be computed we have
        not *proven* the marker stale, so we do not claim it is."""
        lone = tmp_path / "vq"
        lone.mkdir()
        marker = lone / admin.SOURCE_SHA_MARKER_NAME
        marker.write_text(
            "d" * 40 + f"\n{admin.SOURCE_SHA_TREE_FIELD}=" + "e" * 64 + "\n",
            encoding="utf-8",
        )

        assert admin.read_source_sha_marker(marker) == "d" * 40

    def test_unknown_trailing_fields_are_ignored(self, tmp_path: Path) -> None:
        """Forward compat: a newer writer may add fields this reader predates."""
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        marker.write_text(
            "a" * 40 + "\nbuilt-by=some-future-vq\n", encoding="utf-8"
        )

        assert admin.read_source_sha_marker(marker) == "a" * 40

    def test_the_marker_is_readable_by_anyone_who_can_read_the_code(
        self, tmp_path: Path
    ) -> None:
        """host_d, 2026-08-02: `-rw-------` marker beside `-rw-r--r--` code.

        The atomic write stages through `tempfile.NamedTemporaryFile`, which
        creates at 0600, and `os.replace` preserves that mode. On a root-written
        `/opt/vq` install that makes the marker root-only -- and
        `read_source_sha_marker` swallows the `PermissionError` and answers
        `None`, so a non-root `vq source-sha` reports "no marker installed"
        while the file is sitting right there. That is a scheduler-compat gate
        failing closed on a lie. The marker is provenance for code that is
        itself world-readable; it must be too.
        """
        pkg = self._package(tmp_path)
        marker = admin.write_source_sha_marker("a" * 40, pkg / admin.SOURCE_SHA_MARKER_NAME)

        assert marker.stat().st_mode & 0o777 == 0o644

    def test_write_side_stays_strict(self, tmp_path: Path) -> None:
        pkg = self._package(tmp_path)
        with pytest.raises(admin.AdminError):
            admin.write_source_sha_marker("not-a-sha", pkg / admin.SOURCE_SHA_MARKER_NAME)

    def test_a_stale_marker_is_reported_as_stale_not_as_missing(
        self, tmp_path: Path
    ) -> None:
        """`read_source_sha_marker` collapses both to None, which is the right
        fail-closed contract. The operator still needs to know which one they
        have: "missing" and "stale" have different remedies.
        """
        pkg = self._package(tmp_path)
        marker = pkg / admin.SOURCE_SHA_MARKER_NAME
        admin.write_source_sha_marker("a" * 40, marker)
        (pkg / "daemon.py").write_text("x = 2\n", encoding="utf-8")

        status = admin.inspect_source_sha_marker(marker)
        assert status.present is True
        assert status.stale is True
        assert status.sha is None
        assert status.recorded_sha == "a" * 40

        missing = admin.inspect_source_sha_marker(tmp_path / "nope" / "SOURCE-SHA")
        assert missing.present is False
        assert missing.stale is False


class TestCheckoutShaMustDescribeTheRunningCode:
    """`current_source_sha` runs ``git rev-parse HEAD`` anchored at the
    *installed* package directory. Git discovers a repository by walking
    *up*, so a non-editable install that happens to sit anywhere inside some
    unrelated work tree reports that tree's HEAD as vq's own source SHA --
    a value derived from code that has nothing to do with the bytes running.

    It is a stable lie: it survives daemon restarts (re-derived identically
    each boot), survives the real checkout being current (different repo), and
    appears in no file under the vq state, config, or package directories, so
    grepping those cannot find it. That is the host_d report exactly.
    """

    def test_a_tracked_checkout_still_answers(self, tmp_path: Path) -> None:
        repo = tmp_path / "checkout"
        (repo / "src" / "vq").mkdir(parents=True)
        _git(repo.parent, "init", "-q", str(repo))
        (repo / "src" / "vq" / "__init__.py").write_text("x = 1\n", encoding="utf-8")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "init")
        head = _git(repo, "rev-parse", "HEAD").stdout.strip()

        got = admin.current_source_sha(repo / "src" / "vq" / "__init__.py")

        assert got == head
        assert admin.current_source_sha(
            repo / "src" / "vq" / "__init__.py", require_tracked=True
        ) == head

    def test_an_untracked_install_does_not_borrow_the_enclosing_repos_head(
        self, tmp_path: Path
    ) -> None:
        repo = tmp_path / "unrelated"
        repo.mkdir()
        _git(repo, "init", "-q", ".")
        (repo / "README.md").write_text("not vq\n", encoding="utf-8")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-q", "-m", "init")

        # `pip install` (non-editable) copies vq into a venv under that tree.
        pkg = repo / "venv" / "lib" / "python3.13" / "site-packages" / "vq"
        pkg.mkdir(parents=True)
        (pkg / "__init__.py").write_text("x = 1\n", encoding="utf-8")

        assert admin.current_source_sha(pkg / "__init__.py", require_tracked=True) is None

    def test_outside_any_repository_is_still_none(self, tmp_path: Path) -> None:
        pkg = tmp_path / "vq"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("x = 1\n", encoding="utf-8")

        assert admin.current_source_sha(pkg / "__init__.py", require_tracked=True) is None
