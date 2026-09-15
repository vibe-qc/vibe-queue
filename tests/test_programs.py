"""Tests for v0.5.18 program registry + `vq programs` verb."""
from __future__ import annotations

import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, host_status, paths, transport
from vq.cli import _program_requirement_failures, main


@pytest.fixture
def cfg_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    d = tmp_path / "cfg"
    d.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(d))
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    return d


def _write_python_wrapper(path: Path, module_dir: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/sh\n"
        f"PYTHONPATH={shlex.quote(str(module_dir))} "
        f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _init_git_repo(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    (path / "README.md").write_text("test repo\n", encoding="utf-8")
    subprocess.run(["git", "add", "README.md"], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            "seed",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


# ----------------------------------------------------------------------
# Schema parsing
# ----------------------------------------------------------------------


class TestBinaryProgramSchema:
    def test_minimal_binary_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/usr/bin/crystal"\n'
        )
        c = config.load_config()
        assert "crystal" in c.programs
        prog = c.programs["crystal"]
        assert prog.kind == "binary"
        assert prog.binary == "/usr/bin/crystal"
        assert prog.description is None

    def test_binary_with_description(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.orca]\n'
            'kind = "binary"\n'
            'binary = "/home/USER/bin/orca"\n'
            'description = "ORCA 6.1.1"\n'
        )
        c = config.load_config()
        assert c.programs["orca"].description == "ORCA 6.1.1"


class TestVenvProgramSchema:
    def test_minimal_venv_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-dev/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-dev"\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-dev"]
        assert prog.kind == "venv"
        assert prog.python == "/home/USER/vibeqc-dev/.venv/bin/python"
        assert prog.git_dir == "/home/USER/vibeqc-dev"
        assert prog.branch is None
        assert prog.update_script is None

    def test_venv_with_branch_and_script(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-release/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-release"\n'
            'branch = "release"\n'
            'update_script = "scripts/update.sh"\n'
            'healthcheck_command = "echo ok"\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-release"]
        assert prog.branch == "release"
        assert prog.update_script == "scripts/update.sh"
        assert prog.healthcheck_command == "echo ok"

    def test_venv_import_symbols_parse(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-release/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-release"\n'
            'import_check = "vibeqc"\n'
            'import_symbols = ["CosxVariant"]\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-release"]
        assert isinstance(prog, config.VenvProgram)
        assert prog.import_check == "vibeqc"
        assert prog.import_symbols == ["CosxVariant"]

    def test_venv_expected_runtime_pins_parse(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-release/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-release"\n'
            'import_check = "vibeqc"\n'
            'expected_git_sha = "2f62d9a"\n'
            'expected_import_version = "0.15.1"\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-release"]
        assert isinstance(prog, config.VenvProgram)
        assert prog.expected_git_sha == "2f62d9a"
        assert prog.expected_import_version == "0.15.1"


class TestImportProgramSchema:
    def test_import_parses(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.pyscf]\n'
            'kind = "import"\n'
            'python = "/home/USER/vibeqc-dev/.venv/bin/python"\n'
            'import_check = "pyscf"\n'
        )
        c = config.load_config()
        prog = c.programs["pyscf"]
        assert prog.kind == "import"
        assert prog.import_check == "pyscf"

    def test_import_symbols_parse(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-core]\n'
            'kind = "import"\n'
            'python = "/home/USER/vibeqc-dev/.venv/bin/python"\n'
            'import_check = "vibeqc"\n'
            'import_symbols = ["CosxVariant"]\n'
        )
        c = config.load_config()
        prog = c.programs["vibeqc-core"]
        assert isinstance(prog, config.ImportProgram)
        assert prog.import_symbols == ["CosxVariant"]


class TestRejectedSchemas:
    def test_unknown_kind_rejected(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.weird]\n'
            'kind = "magic"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_extra_field_on_binary_rejected(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.crystal]\n'
            'kind = "binary"\n'
            'binary = "/usr/bin/crystal"\n'
            'rogue_field = "x"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_binary_without_path_rejected(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.crystal]\n'
            'kind = "binary"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_venv_symbols_without_import_check_rejected(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-dev/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-dev"\n'
            'import_symbols = ["CosxVariant"]\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_expected_import_version_requires_import_check(
        self, cfg_dir: Path
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-release/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-release"\n'
            'expected_import_version = "0.15.1"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()

    def test_venv_healthcheck_rejects_multiline_command(
        self, cfg_dir: Path
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            'python = "/home/USER/vibeqc-release/.venv/bin/python"\n'
            'git_dir = "/home/USER/vibeqc-release"\n'
            'healthcheck_command = "echo ok\\necho nope"\n'
        )
        with pytest.raises(config.ConfigError, match="invalid config"):
            config.load_config()


# ----------------------------------------------------------------------
# Availability probes (with real on-disk files)
# ----------------------------------------------------------------------


class TestBinaryAvailability:
    def test_ok_when_executable_file_exists(self, tmp_path: Path) -> None:
        bin_path = tmp_path / "fake-crystal"
        bin_path.write_text("#!/bin/sh\necho fake\n")
        bin_path.chmod(0o755)
        prog = config.BinaryProgram(kind="binary", binary=str(bin_path))
        ok, reason = prog.availability()
        assert ok is True
        assert str(bin_path) in reason

    def test_missing_file_fails(self, tmp_path: Path) -> None:
        prog = config.BinaryProgram(
            kind="binary", binary=str(tmp_path / "does-not-exist"),
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "not found" in reason

    def test_directory_path_fails(self, tmp_path: Path) -> None:
        prog = config.BinaryProgram(kind="binary", binary=str(tmp_path))
        ok, reason = prog.availability()
        assert ok is False
        assert "not a file" in reason

    def test_non_executable_fails(self, tmp_path: Path) -> None:
        bin_path = tmp_path / "no-x-bit"
        bin_path.write_text("hi")
        bin_path.chmod(0o644)
        prog = config.BinaryProgram(kind="binary", binary=str(bin_path))
        ok, reason = prog.availability()
        assert ok is False
        assert "not executable" in reason


class TestVenvAvailability:
    def test_ok_when_python_and_git_present(self, tmp_path: Path) -> None:
        # Set up a fake venv: python file + git_dir with .git/
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\nexec python3 \"$@\"\n")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        prog = config.VenvProgram(
            kind="venv", python=str(venv_py), git_dir=str(git_dir),
        )
        ok, reason = prog.availability()
        assert ok is True

    @pytest.mark.parametrize("implicit", [True, False], ids=["implicit", "explicit"])
    def test_stale_core_in_managed_vibeqc_checkout_fails_closed(
        self, tmp_path: Path, implicit: bool
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        venv_root = git_dir / ".venv" if implicit else tmp_path / "external-venv"
        core = venv_root / "lib" / "vibeqc" / "_vibeqc_core.so"
        core.parent.mkdir(parents=True)
        core.write_text("stale test core\n", encoding="utf-8")
        (package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(core)!r}\n",
            encoding="utf-8",
        )
        cpp_source = git_dir / "cpp" / "src" / "bindings.cpp"
        cpp_source.parent.mkdir(parents=True)
        cpp_source.write_text("// newer source\n", encoding="utf-8")
        os.utime(core, ns=(1_000_000_000, 1_000_000_000))
        os.utime(cpp_source, ns=(2_000_000_000, 2_000_000_000))
        venv_py = venv_root / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
            import_check=None if implicit else "vibeqc",
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "stale compiled core" in reason.lower()
        assert "cpp/src/bindings.cpp" in reason

    def test_newer_included_native_data_marks_core_stale(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        core = git_dir / ".venv" / "lib" / "vibeqc" / "_vibeqc_core.so"
        core.parent.mkdir(parents=True)
        core.write_text("test core\n", encoding="utf-8")
        (package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(core)!r}\n",
            encoding="utf-8",
        )
        bindings = git_dir / "cpp" / "src" / "bindings.cpp"
        bindings.parent.mkdir(parents=True)
        bindings.write_text("// structural marker\n", encoding="utf-8")
        included_data = git_dir / "cpp" / "src" / "generated_data.inc"
        included_data.write_text("// newer included data\n", encoding="utf-8")
        os.utime(bindings, ns=(1_000_000_000, 1_000_000_000))
        os.utime(core, ns=(2_000_000_000, 2_000_000_000))
        os.utime(included_data, ns=(3_000_000_000, 3_000_000_000))
        venv_py = git_dir / ".venv" / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "stale compiled core" in reason.lower()
        assert "cpp/src/generated_data.inc" in reason

    def test_newer_native_build_spec_marks_core_stale(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        core = git_dir / ".venv" / "lib" / "vibeqc" / "_vibeqc_core.so"
        core.parent.mkdir(parents=True)
        core.write_text("test core\n", encoding="utf-8")
        (package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(core)!r}\n",
            encoding="utf-8",
        )
        bindings = git_dir / "cpp" / "src" / "bindings.cpp"
        bindings.parent.mkdir(parents=True)
        bindings.write_text("// structural marker\n", encoding="utf-8")
        build_spec = git_dir / "CMakeLists.txt"
        build_spec.write_text("add_subdirectory(cpp)\n", encoding="utf-8")
        os.utime(bindings, ns=(1_000_000_000, 1_000_000_000))
        os.utime(core, ns=(2_000_000_000, 2_000_000_000))
        os.utime(build_spec, ns=(3_000_000_000, 3_000_000_000))
        venv_py = git_dir / ".venv" / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "stale compiled core" in reason.lower()
        assert "CMakeLists.txt" in reason

    def test_current_core_in_managed_vibeqc_checkout_is_available(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        core = git_dir / ".venv" / "lib" / "vibeqc" / "_vibeqc_core.so"
        core.parent.mkdir(parents=True)
        core.write_text("current test core\n", encoding="utf-8")
        (package / "__init__.py").write_text(
            "class _Core:\n"
            "    pass\n"
            "_vibeqc_core = _Core()\n"
            f"_vibeqc_core.__file__ = {str(core)!r}\n",
            encoding="utf-8",
        )
        cpp_source = git_dir / "cpp" / "src" / "bindings.cpp"
        cpp_source.parent.mkdir(parents=True)
        cpp_source.write_text("// older source\n", encoding="utf-8")
        os.utime(cpp_source, ns=(2_000_000_000, 2_000_000_000))
        os.utime(core, ns=(3_000_000_000, 3_000_000_000))
        venv_py = git_dir / ".venv" / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
        )

        ok, reason = prog.availability()

        assert ok is True, reason
        assert "`import vibeqc`" in reason

    def test_pure_python_core_shim_fails_managed_readiness(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        core = package / "_vibeqc_core.py"
        core.write_text("# not a compiled extension\n", encoding="utf-8")
        (package / "__init__.py").write_text(
            "from . import _vibeqc_core\n",
            encoding="utf-8",
        )
        cpp_source = git_dir / "cpp" / "src" / "bindings.cpp"
        cpp_source.parent.mkdir(parents=True)
        cpp_source.write_text("// older source\n", encoding="utf-8")
        os.utime(cpp_source, ns=(1_000_000_000, 1_000_000_000))
        os.utime(core, ns=(2_000_000_000, 2_000_000_000))
        venv_py = git_dir / ".venv" / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "target-supported compiled-extension proof" in reason

    def test_colocated_generic_venv_does_not_infer_vibeqc(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("", encoding="utf-8")
        bindings = git_dir / "cpp" / "src" / "bindings.cpp"
        bindings.parent.mkdir(parents=True)
        bindings.write_text("// monorepo marker\n", encoding="utf-8")
        python = git_dir / "vibe-queue" / ".venv" / "bin" / "python"
        _write_python_wrapper(python, git_dir / "python")
        prog = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(git_dir),
            update_script="vibe-queue/scripts/update.sh",
        )

        ok, reason = prog.availability()

        assert prog.effective_import_check() is None
        assert ok is True, reason

    def test_missing_python_fails(self, tmp_path: Path) -> None:
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        prog = config.VenvProgram(
            kind="venv",
            python=str(tmp_path / "nope" / "python"),
            git_dir=str(git_dir),
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "python not found" in reason

    def test_missing_git_dir_fails(self, tmp_path: Path) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.touch()
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(tmp_path / "absent"),
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "git_dir missing" in reason

    def test_not_a_git_checkout_fails(self, tmp_path: Path) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.touch()
        plain_dir = tmp_path / "not-a-repo"
        plain_dir.mkdir()
        prog = config.VenvProgram(
            kind="venv", python=str(venv_py), git_dir=str(plain_dir),
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "not a git checkout" in reason

    def test_import_symbols_probe_for_venv(self, tmp_path: Path) -> None:
        import sys

        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            import_check="os",
            import_symbols=["path"],
        )

        ok, reason = prog.availability()

        assert ok is True
        assert "symbols [path]" in reason

    def test_expected_git_sha_accepts_configured_prefix(self, tmp_path: Path) -> None:
        git_dir = tmp_path / "repo"
        sha = _init_git_repo(git_dir)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            expected_git_sha=sha[:7],
        )

        ok, reason = prog.availability()

        assert ok is True, reason

    def test_expected_git_sha_mismatch_marks_venv_missing(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        sha = _init_git_repo(git_dir)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            expected_git_sha="deadbeef",
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "runtime pin mismatch" in reason
        assert "deadbeef" in reason
        assert sha in reason

    def test_expected_import_version_mismatch_marks_venv_missing(
        self, tmp_path: Path
    ) -> None:
        module_dir = tmp_path / "modules"
        module_dir.mkdir()
        (module_dir / "vibeqc.py").write_text('__version__ = "0.15.2"\n')
        venv_py = tmp_path / "venv" / "bin" / "python"
        _write_python_wrapper(venv_py, module_dir)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        prog = config.VenvProgram(
            kind="venv",
            python=str(venv_py),
            git_dir=str(git_dir),
            import_check="vibeqc",
            expected_import_version="0.15.1",
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "runtime pin mismatch" in reason
        assert "expected 0.15.1, got 0.15.2" in reason

    def test_healthcheck_success_marks_venv_ok(self, tmp_path: Path) -> None:
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        script = tmp_path / "ok.sh"
        script.write_text("#!/bin/sh\necho capture-selftest OK\n", encoding="utf-8")
        script.chmod(0o755)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            healthcheck_command=str(script),
        )

        ok, reason = prog.availability()

        assert ok is True, reason
        assert "healthcheck ok" in reason
        assert "capture-selftest OK" in reason

    def test_healthcheck_finds_commands_in_venv_bin(self, tmp_path: Path) -> None:
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        venv_bin = tmp_path / "venv" / "bin"
        venv_bin.mkdir(parents=True)
        python = venv_bin / "python"
        python.write_text("#!/bin/sh\n", encoding="utf-8")
        python.chmod(0o755)
        probe = venv_bin / "vibe-view"
        probe.write_text("#!/bin/sh\necho venv-bin health ok\n", encoding="utf-8")
        probe.chmod(0o755)
        prog = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(git_dir),
            healthcheck_command="vibe-view capture-selftest",
        )

        ok, reason = prog.availability()

        assert ok is True, reason
        assert "venv-bin health ok" in reason

    def test_healthcheck_supports_vibeview_xvfb_shim(
        self, tmp_path: Path
    ) -> None:
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        venv_bin = tmp_path / "venv" / "bin"
        venv_bin.mkdir(parents=True)
        python = venv_bin / "python"
        python.write_text("#!/bin/sh\n", encoding="utf-8")
        python.chmod(0o755)
        probe = venv_bin / "vibe-view"
        probe.write_text(
            "#!/bin/sh\n"
            'echo "capture via ${VIBE_TEST_WRAPPER:-missing} $0 $1"\n',
            encoding="utf-8",
        )
        probe.chmod(0o755)
        wrapper = venv_bin / "xvfb-run"
        wrapper.write_text(
            "#!/bin/sh\n"
            'test "$PYVISTA_OFF_SCREEN" = "True"\n'
            'test "$1" = "-a"\n'
            "shift\n"
            "export VIBE_TEST_WRAPPER=wrapper-ok\n"
            'exec "$@"\n',
            encoding="utf-8",
        )
        wrapper.chmod(0o755)
        prog = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(git_dir),
            healthcheck_command="xvfb-run -a vibe-view capture-selftest",
        )

        ok, reason = prog.availability()

        assert ok is True, reason
        assert "capture via wrapper-ok" in reason

    def test_healthcheck_failure_marks_venv_missing(self, tmp_path: Path) -> None:
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        script = tmp_path / "bad.sh"
        script.write_text(
            "#!/bin/sh\necho capture failed >&2\nexit 7\n",
            encoding="utf-8",
        )
        script.chmod(0o755)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            healthcheck_command=str(script),
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "healthcheck rc=7" in reason
        assert "capture failed" in reason

    def test_healthcheck_timeout_reports_last_probe_line(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        real_run = subprocess.run

        def fake_run(
            argv: list[str], *args: object, **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            if argv == ["slow-healthcheck"]:
                raise subprocess.TimeoutExpired(
                    argv,
                    timeout=60.0,
                    output=b"starting capture\n",
                    stderr=b"waiting for offscreen GL\n",
                )
            return real_run(argv, *args, **kwargs)

        monkeypatch.setattr(config.subprocess, "run", fake_run)
        prog = config.VenvProgram(
            kind="venv",
            python=sys.executable,
            git_dir=str(git_dir),
            healthcheck_command="slow-healthcheck",
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "healthcheck timed out after 60s" in reason
        assert "waiting for offscreen GL" in reason


class TestUnhealthyIsNotMissing:
    """#45: a runtime that imports but whose healthcheck does not pass is
    ``UNHEALTHY``, never the ``MISSING`` of a runtime that does not import.

    Reproduces the driver on 2026-09-12: ``vibeview-dev`` imported 2.16.1
    while its Linux-only ``xvfb-run`` healthcheck could not start on macOS,
    and ``vibeqc-dev`` could not import at all. Both read ``MISSING``.
    """

    def _venv(
        self, tmp_path: Path, name: str, module_source: str
    ) -> tuple[Path, Path]:
        root = tmp_path / name
        git_dir = root / "repo"
        (git_dir / ".git").mkdir(parents=True)
        site = root / "site"
        site.mkdir()
        (site / "vqprobe45.py").write_text(module_source, encoding="utf-8")
        python = root / "venv" / "bin" / "python"
        _write_python_wrapper(python, site)
        return python, git_dir

    def _write_config(self, cfg_dir: Path, tmp_path: Path) -> None:
        failing = tmp_path / "failing-healthcheck.sh"
        failing.write_text("#!/bin/sh\necho capture failed >&2\nexit 3\n")
        failing.chmod(0o755)
        sections = []
        for name, source, healthcheck in (
            (
                "imports-no-healthcheck-binary",
                "__version__ = '2.16.1'\n",
                "vq-no-such-healthcheck-45 capture-selftest",
            ),
            (
                "does-not-import",
                "raise ImportError('libint2.so: cannot open shared object')\n",
                "vq-no-such-healthcheck-45 capture-selftest",
            ),
            (
                "imports-healthcheck-fails",
                "__version__ = '2.16.1'\n",
                str(failing),
            ),
        ):
            python, git_dir = self._venv(tmp_path, name, source)
            sections.append(
                f"[programs.{name}]\n"
                'kind = "venv"\n'
                f'python = "{python}"\n'
                f'git_dir = "{git_dir}"\n'
                'import_check = "vqprobe45"\n'
                f'healthcheck_command = "{healthcheck}"\n'
            )
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n' + "\n".join(sections),
            encoding="utf-8",
        )

    def test_json_never_shares_a_status_between_the_two(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json

        self._write_config(cfg_dir, tmp_path)

        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        records = {r["name"]: r for r in _json.loads(result.stdout)}
        cannot_start = records["imports-no-healthcheck-binary"]
        broken = records["does-not-import"]
        ran_and_failed = records["imports-healthcheck-fails"]

        assert broken["status"] == "MISSING"
        assert broken["healthcheck_status"] == "not-run"
        assert "import vqprobe45 failed" in broken["reason"]

        assert cannot_start["status"] == "UNHEALTHY"
        assert cannot_start["healthcheck_status"] == "could-not-start"
        assert cannot_start["import_version"] == "2.16.1"
        assert "healthcheck failed to start" in cannot_start["reason"]

        assert ran_and_failed["status"] == "UNHEALTHY"
        assert ran_and_failed["healthcheck_status"] == "failed"
        assert "healthcheck rc=3" in ran_and_failed["reason"]

        assert cannot_start["status"] != broken["status"]

    def test_table_shows_unhealthy(self, cfg_dir: Path, tmp_path: Path) -> None:
        self._write_config(cfg_dir, tmp_path)

        result = CliRunner().invoke(main, ["programs", "localhost"])

        assert result.exit_code == 0, result.output
        rows = {
            line.split()[0]: line.split()[2]
            for line in result.stdout.splitlines()[1:]
            if line.strip()
        }
        assert rows["imports-no-healthcheck-binary"] == "UNHEALTHY"
        assert rows["imports-healthcheck-fails"] == "UNHEALTHY"
        assert rows["does-not-import"] == "MISSING"

    def test_require_still_refuses_an_unhealthy_program(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        self._write_config(cfg_dir, tmp_path)

        result = CliRunner().invoke(
            main, ["programs", "--require", "imports-no-healthcheck-binary"]
        )

        assert result.exit_code != 0, result.output
        assert "imports-no-healthcheck-binary is UNHEALTHY" in result.output

    def test_availability_bool_is_unchanged(self, tmp_path: Path) -> None:
        python, git_dir = self._venv(tmp_path, "direct", "__version__ = '1'\n")
        prog = config.VenvProgram(
            kind="venv",
            python=str(python),
            git_dir=str(git_dir),
            import_check="vqprobe45",
            healthcheck_command="vq-no-such-healthcheck-45",
        )

        ok, reason = prog.availability()

        assert ok is False
        assert "healthcheck failed to start" in reason
        assert prog.availability_status().status == config.PROGRAM_STATUS_UNHEALTHY


class TestImportAvailability:
    def test_ok_when_module_importable(self) -> None:
        """Use the system python to import 'os' (always available)."""
        import sys
        prog = config.ImportProgram(
            kind="import",
            python=sys.executable,
            import_check="os",
        )
        ok, reason = prog.availability()
        assert ok is True

    def test_fails_when_module_missing(self) -> None:
        import sys
        prog = config.ImportProgram(
            kind="import",
            python=sys.executable,
            import_check="this_module_does_not_exist_anywhere_123",
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "import" in reason and "failed" in reason

    def test_fails_when_symbol_missing(self) -> None:
        import sys
        prog = config.ImportProgram(
            kind="import",
            python=sys.executable,
            import_check="os",
            import_symbols=["definitely_not_an_os_symbol_xyz"],
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "definitely_not_an_os_symbol_xyz" in reason

    def test_fails_when_python_missing(self, tmp_path: Path) -> None:
        prog = config.ImportProgram(
            kind="import",
            python=str(tmp_path / "no-python"),
            import_check="os",
        )
        ok, reason = prog.availability()
        assert ok is False
        assert "python not found" in reason


# ----------------------------------------------------------------------
# CLI verb
# ----------------------------------------------------------------------


class TestProgramsCLI:
    def test_no_programs_registered_message(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text('default_host = "localhost"\n')
        result = CliRunner().invoke(main, ["programs", "localhost"])
        assert result.exit_code == 0
        assert "no programs registered" in result.output

    def test_managed_vibeqc_without_import_check_reports_broken_core(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json

        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        package = git_dir / "python" / "vibeqc"
        package.mkdir(parents=True)
        (package / "_vibeqc_core.py").write_text(
            "# stale core missing MOLECULAR_XC_GRID_MAX_WORKERS\n",
            encoding="utf-8",
        )
        (package / "__init__.py").write_text(
            "from ._vibeqc_core import MOLECULAR_XC_GRID_MAX_WORKERS\n",
            encoding="utf-8",
        )
        cpp_source = git_dir / "cpp" / "src" / "bindings.cpp"
        cpp_source.parent.mkdir(parents=True)
        cpp_source.write_text("// current binding\n", encoding="utf-8")
        venv_py = git_dir / ".venv" / "bin" / "python"
        _write_python_wrapper(venv_py, git_dir / "python")
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
            'update_script = "scripts/update.sh"\n',
            encoding="utf-8",
        )

        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        record = _json.loads(result.output)[0]
        assert record["status"] == "MISSING"
        assert record["import_check"] == "vibeqc"
        assert "MOLECULAR_XC_GRID_MAX_WORKERS" in record["reason"]

    def test_lists_registered_programs(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        bin_ok = tmp_path / "ok-bin"
        bin_ok.write_text("#!/bin/sh\n")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.alpha]\n'
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
            'description = "first program"\n'
            '\n'
            '[programs.beta]\n'
            'kind = "binary"\n'
            'binary = "/does/not/exist"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost"])
        assert result.exit_code == 0, result.output
        # Both names appear.
        assert "alpha" in result.output
        assert "beta" in result.output
        # Statuses differ.
        assert "OK" in result.output
        assert "MISSING" in result.output
        # Header is present.
        assert "NAME" in result.output
        assert "KIND" in result.output

    def test_default_remote_host_delegates_to_remote_vq(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        class FakeProc:
            stdout = "remote programs\n"

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            return FakeProc()

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["programs"])

        assert result.exit_code == 0, result.output
        assert result.output.strip() == "remote programs"
        assert captured == [("programs", "localhost")]

    def test_default_remote_transport_failure_falls_back_to_localhost(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise transport.RemoteError(
                "remote vq failed (exit 255) on remote-test: "
                "ssh: Network is unreachable"
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["programs"])

        assert result.exit_code == 0, result.output
        assert "no programs registered" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is unreachable" in combined
        assert "showing localhost programs" in combined
        assert captured == [("programs", "localhost")]

    def test_default_remote_marked_down_skips_remote_probe(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise AssertionError("down default_host should not be probed")

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)
        host_status.mark_down("remote-test", "off network")

        result = CliRunner().invoke(main, ["programs"])

        assert result.exit_code == 0, result.output
        assert "no programs registered" in result.output
        combined = result.output + result.stderr
        assert "default_host 'remote-test' is marked down" in combined
        assert "showing localhost programs" in combined
        assert captured == []

    def test_default_remote_json_stays_strict(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise transport.RemoteError(
                "remote vq failed (exit 255) on remote-test: "
                "ssh: Network is unreachable"
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["programs", "--json"])

        assert result.exit_code != 0
        assert "remote vq failed" in result.output
        assert captured == [("programs", "localhost", "--json")]

    def test_default_remote_require_stays_strict(
        self, cfg_dir: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (cfg_dir / "config.toml").write_text(
            'default_host = "remote-test"\n'
            "\n"
            "[hosts.remote-test]\n"
            'ssh = "remote.example.com"\n'
            'remote_vq = "vq"\n'
        )
        captured: list[tuple[str, ...]] = []

        def fake_run_remote_vq(host_cfg, *args, **kwargs):
            captured.append(tuple(args))
            raise transport.RemoteError(
                "remote vq failed (exit 255) on remote-test: "
                "ssh: Network is unreachable"
            )

        monkeypatch.setattr(transport, "run_remote_vq", fake_run_remote_vq)

        result = CliRunner().invoke(main, ["programs", "--require", "vibeqc-dev"])

        assert result.exit_code != 0
        assert "remote vq failed" in result.output
        assert captured == [("programs", "localhost")]

    def test_help_text(self) -> None:
        result = CliRunner().invoke(main, ["programs", "--help"])
        assert result.exit_code == 0
        # Should mention all three kinds.
        for k in ("binary", "venv", "import"):
            assert k in result.output

    def test_top_level_listing_includes_programs_verb(self) -> None:
        result = CliRunner().invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "programs" in result.output


# ----------------------------------------------------------------------
# v0.5.19: --json output
# ----------------------------------------------------------------------


class TestProgramsJsonOutput:
    """`vq programs --json` exposes the registry to scripts.

    Stable schema (documented in ``vq programs --help``):
      common:   name, kind, status, reason, description
      binary:   binary       (absolute path)
      venv:     python, git_dir, branch, update_script,
                import_check, import_symbols
      import:   python, import_check, import_symbols
    Empty registry -> ``[]``. status is "OK" or "MISSING".

    The integration smoke test consumes this to look up absolute
    binary paths (so the daemon's PATH doesn't matter); v0.6.0's
    `vq admin update` will consume the same schema.
    """

    def test_empty_registry_returns_empty_array(self, cfg_dir: Path) -> None:
        import json as _json
        (cfg_dir / "config.toml").write_text('default_host = "localhost"\n')
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0
        records = _json.loads(result.output)
        assert records == []

    def test_all_json_returns_host_keyed_object(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.alpha]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )

        result = CliRunner().invoke(main, ["programs", "--all", "--json"])

        assert result.exit_code == 0, result.output
        payload = _json.loads(result.output)
        assert list(payload) == ["localhost"]
        assert payload["localhost"][0]["name"] == "alpha"
        assert payload["localhost"][0]["status"] == "OK"

    def test_require_passes_when_program_is_ok(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeview-dev]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require", "vibeview-dev"],
        )

        assert result.exit_code == 0, result.output
        assert "vibeview-dev" in result.output

    def test_require_fails_when_program_is_missing(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.alpha]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require", "vibeview-dev"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "required program check failed" in combined
        assert "localhost: vibeview-dev is not registered" in combined

    def test_require_fails_when_all_has_no_hosts(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text("")

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require", "vibeview-dev"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "no hosts configured" in result.output
        assert "(no hosts): vibeview-dev unavailable" in combined

    def test_require_any_passes_when_alias_is_ok(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibe-view]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-any", "vibeview-dev,vibe-view"],
        )

        assert result.exit_code == 0, result.output
        assert "vibe-view" in result.output

    def test_require_any_fails_when_no_alias_is_ok(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.alpha]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-any", "vibeview-dev,vibe-view"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "none of vibeview-dev, vibe-view is OK" in combined
        assert "vibeview-dev=unregistered" in combined

    def test_require_companion_passes_for_two_registered_records(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        orca = tmp_path / "orca"
        orca_2mkl = tmp_path / "orca_2mkl"
        for binary in (orca, orca_2mkl):
            binary.write_text("#!/bin/sh\n", encoding="utf-8")
            binary.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.orca]\n"
            'kind = "binary"\n'
            f'binary = "{orca}"\n'
            "\n"
            "[programs.orca_2mkl]\n"
            'kind = "binary"\n'
            f'binary = "{orca_2mkl}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "programs",
                "--all",
                "--require-companion",
                "orca=orca_2mkl",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "orca_2mkl" in result.output

    def test_require_companion_is_conditional_per_host(self) -> None:
        payload = {
            "healthy": [
                {"name": "orca", "status": "OK"},
                {"name": "orca_2mkl", "status": "OK"},
            ],
            "missing-companion": [{"name": "orca", "status": "OK"}],
            "without-primary": [{"name": "alpha", "status": "OK"}],
        }

        failures = _program_requirement_failures(
            payload,
            (),
            companion_requirements=(("orca", "orca_2mkl"),),
        )

        assert failures == [
            "missing-companion: orca is registered but companion "
            "orca_2mkl is not registered"
        ]

    def test_require_companion_fails_when_companion_is_missing(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        orca = tmp_path / "orca"
        orca.write_text("#!/bin/sh\n", encoding="utf-8")
        orca.chmod(0o755)
        missing_converter = tmp_path / "missing-orca_2mkl"
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.orca]\n"
            'kind = "binary"\n'
            f'binary = "{orca}"\n'
            "\n"
            "[programs.orca_2mkl]\n"
            'kind = "binary"\n'
            f'binary = "{missing_converter}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "programs",
                "--all",
                "--require-companion",
                "orca=orca_2mkl",
            ],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "localhost: orca companion orca_2mkl is MISSING" in combined

    def test_require_companion_fails_when_registered_primary_is_missing(
        self,
    ) -> None:
        failures = _program_requirement_failures(
            {
                "host-a": [
                    {
                        "name": "orca",
                        "status": "MISSING",
                        "reason": "binary is not executable",
                    },
                    {"name": "orca_2mkl", "status": "OK"},
                ]
            },
            (),
            companion_requirements=(("orca", "orca_2mkl"),),
        )

        assert failures == [
            "host-a: primary orca is MISSING (binary is not executable)"
        ]

    def test_require_companion_rejects_bad_spec(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-companion", "orca"],
        )

        assert result.exit_code != 0
        assert "--require-companion expects NAME=VALUE" in result.output

    def test_require_clean_passes_for_clean_checkout(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "programs",
                "--all",
                "--require",
                "vibeqc-dev",
                "--require-clean",
                "vibeqc-dev",
            ],
        )

        assert result.exit_code == 0, result.output

    def test_require_clean_fails_for_dirty_checkout(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (git_dir / "README.md").write_text("changed\n", encoding="utf-8")
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-clean", "vibeqc-dev"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "localhost: vibeqc-dev checkout is dirty" in combined

    def test_require_branch_passes_for_matching_branch(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-branch", f"vibeqc-dev={branch}"],
        )

        assert result.exit_code == 0, result.output

    def test_require_branch_fails_for_wrong_branch(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-branch", "vibeqc-dev=release"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "localhost: vibeqc-dev branch is" in combined
        assert "(expected release)" in combined

    def test_require_branch_rejects_bad_spec(self, cfg_dir: Path) -> None:
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-branch", "vibeqc-dev"],
        )

        assert result.exit_code != 0
        assert "--require-branch expects NAME=VALUE" in result.output

    def test_require_sha_passes_for_matching_prefix(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        sha = _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-sha", f"vibeqc-dev={sha[:7]}"],
        )

        assert result.exit_code == 0, result.output

    def test_require_sha_fails_for_mismatch(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-sha", "vibeqc-dev=deadbeef"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "localhost: vibeqc-dev git SHA is" in combined
        assert "(expected deadbeef)" in combined

    def test_require_full_sha_rejects_matching_display_prefix(self) -> None:
        expected = "a" * 12 + "b" * 28
        actual = "a" * 12 + "c" * 28
        payload = {
            "host_f": [
                {
                    "name": "vibeqc-dev",
                    "status": "OK",
                    "current_git_sha": actual[:12],
                    "current_git_sha_full": actual,
                }
            ]
        }

        failures = _program_requirement_failures(
            payload,
            (),
            sha_requirements=(("vibeqc-dev", expected),),
        )

        assert failures == [
            f"host_f: vibeqc-dev git SHA is {actual} (expected {expected})"
        ]

    def test_require_full_sha_fails_without_full_evidence(self) -> None:
        expected = "a" * 40
        payload = {
            "host_f": [
                {
                    "name": "vibeqc-dev",
                    "status": "OK",
                    "current_git_sha": expected[:12],
                }
            ]
        }

        failures = _program_requirement_failures(
            payload,
            (),
            sha_requirements=(("vibeqc-dev", expected),),
        )

        assert failures == [
            f"host_f: vibeqc-dev git SHA is unknown (expected {expected})"
        ]

    def test_require_version_passes_for_matching_import_version(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        module_dir = tmp_path / "modules"
        module_dir.mkdir()
        (module_dir / "vibeqc.py").write_text(
            '__version__ = "0.15.29.dev0"\n',
            encoding="utf-8",
        )
        venv_py = tmp_path / "venv" / "bin" / "python"
        _write_python_wrapper(venv_py, module_dir)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
            'import_check = "vibeqc"\n'
        )

        result = CliRunner().invoke(
            main,
            [
                "programs",
                "--all",
                "--require-version",
                "vibeqc-dev=0.15.29.dev0",
            ],
        )

        assert result.exit_code == 0, result.output

    def test_require_version_fails_for_mismatch(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        module_dir = tmp_path / "modules"
        module_dir.mkdir()
        (module_dir / "vibeqc.py").write_text(
            '__version__ = "0.15.28"\n',
            encoding="utf-8",
        )
        venv_py = tmp_path / "venv" / "bin" / "python"
        _write_python_wrapper(venv_py, module_dir)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.vibeqc-release]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
            'import_check = "vibeqc"\n'
        )

        result = CliRunner().invoke(
            main,
            ["programs", "--all", "--require-version", "vibeqc-release=0.15.29"],
        )

        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert "localhost: vibeqc-release import version is 0.15.28" in combined
        assert "(expected 0.15.29)" in combined

    def test_require_keeps_json_stdout_parseable_on_failure(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n", encoding="utf-8")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            "[hosts.localhost]\n"
            'ssh = "localhost"\n'
            "\n"
            "[programs.alpha]\n"
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )
        try:
            runner = CliRunner(mix_stderr=False)  # type: ignore[call-arg]
        except TypeError:
            runner = CliRunner()

        result = runner.invoke(
            main,
            ["programs", "--all", "--json", "--require", "vibeview-dev"],
        )

        stdout = getattr(result, "stdout", result.output)
        payload = _json.loads(stdout)
        combined = result.output + getattr(result, "stderr", "")
        assert result.exit_code == 1
        assert payload["localhost"][0]["name"] == "alpha"
        assert "localhost: vibeview-dev is not registered" in combined

    def test_binary_program_record_has_absolute_path(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        """A binary entry must expose its `binary` field so callers can
        submit jobs with the absolute path. This is the field
        integration_smoke.py reads to bypass the daemon's PATH."""
        import json as _json
        bin_ok = tmp_path / "real-bin"
        bin_ok.write_text("#!/bin/sh\n")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.alpha]\n'
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
            'description = "fake program"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0, result.output
        records = _json.loads(result.output)
        assert len(records) == 1
        rec = records[0]
        assert rec["name"] == "alpha"
        assert rec["kind"] == "binary"
        assert rec["status"] == "OK"
        assert rec["binary"] == str(bin_ok)
        assert rec["description"] == "fake program"
        # Common fields always present.
        assert "reason" in rec

    def test_venv_program_record_exposes_python_and_git(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        (git_dir / ".git").mkdir(parents=True)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-dev]\n'
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update-dev.sh"\n'
            'healthcheck_command = "echo ok"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0
        records = _json.loads(result.output)
        rec = records[0]
        assert rec["kind"] == "venv"
        assert rec["python"] == str(venv_py)
        assert rec["git_dir"] == str(git_dir)
        assert rec["branch"] == "main"
        assert rec["update_script"] == "scripts/update-dev.sh"
        assert rec["healthcheck_command"] == "echo ok"
        assert rec["import_check"] is None
        assert rec["import_symbols"] == []
        assert rec["import_version"] is None
        assert rec["current_git_sha"] is None
        assert rec["current_git_describe"] is None
        assert rec["current_git_branch"] is None
        assert rec["current_git_dirty"] is None
        # binary field absent on venv kind
        assert "binary" not in rec

    def test_venv_program_record_exposes_runtime_identity(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        module_dir = tmp_path / "modules"
        module_dir.mkdir()
        (module_dir / "vibeqc.py").write_text(
            '__version__ = "0.15.7"\n'
            "class CosxVariant:\n"
            "    pass\n",
            encoding="utf-8",
        )
        venv_py = tmp_path / "venv" / "bin" / "python"
        _write_python_wrapper(venv_py, module_dir)
        git_dir = tmp_path / "repo"
        sha = _init_git_repo(git_dir)
        branch = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=git_dir,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.vibeqc-release]\n'
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "release"\n'
            'import_check = "vibeqc"\n'
            'import_symbols = ["CosxVariant"]\n'
        )

        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        rec = _json.loads(result.output)[0]
        assert rec["python"] == str(venv_py)
        assert rec["git_dir"] == str(git_dir)
        assert rec["import_version"] == "0.15.7"
        assert rec["current_git_sha"] == sha
        assert rec["current_git_describe"]
        assert rec["current_git_branch"] == branch
        assert rec["current_git_dirty"] is False
        assert rec["expected_git_sha"] is None
        assert rec["expected_import_version"] is None
        assert "__version__=0.15.7" in rec["reason"]

        text = CliRunner().invoke(main, ["programs", "localhost"])
        assert text.exit_code == 0, text.output
        assert str(venv_py) in text.output
        assert "git_sha=" + sha in text.output
        assert "git_branch=" + branch in text.output
        assert "__version__=0.15.7" in text.output

    def test_venv_program_record_reports_dirty_checkout(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        venv_py = tmp_path / "venv" / "bin" / "python"
        venv_py.parent.mkdir(parents=True)
        venv_py.write_text("#!/bin/sh\n", encoding="utf-8")
        venv_py.chmod(0o755)
        git_dir = tmp_path / "repo"
        _init_git_repo(git_dir)
        (git_dir / "README.md").write_text("changed\n", encoding="utf-8")
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            f'python = "{venv_py}"\n'
            f'git_dir = "{git_dir}"\n'
        )

        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])

        assert result.exit_code == 0, result.output
        rec = _json.loads(result.output)[0]
        assert rec["current_git_dirty"] is True
        text = CliRunner().invoke(main, ["programs", "localhost"])
        assert text.exit_code == 0, text.output
        assert "git_dirty=true" in text.output

    def test_import_program_record_exposes_python_and_module(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        import json as _json
        venv_py = tmp_path / "py"
        venv_py.write_text("#!/bin/sh\n")
        venv_py.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.pyscf]\n'
            'kind = "import"\n'
            f'python = "{venv_py}"\n'
            'import_check = "pyscf"\n'
            'import_symbols = ["Mole"]\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0
        records = _json.loads(result.output)
        rec = records[0]
        assert rec["kind"] == "import"
        assert rec["python"] == str(venv_py)
        assert rec["import_check"] == "pyscf"
        assert rec["import_symbols"] == ["Mole"]
        assert rec["import_version"] is None

    def test_status_missing_for_unavailable_binary(
        self, cfg_dir: Path
    ) -> None:
        """A binary that doesn't exist on disk shows up with
        status=MISSING. Smoke test skips engines whose program record
        isn't status=OK."""
        import json as _json
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.ghost]\n'
            'kind = "binary"\n'
            'binary = "/does/not/exist"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0
        rec = _json.loads(result.output)[0]
        assert rec["status"] == "MISSING"
        assert "not found" in rec["reason"]

    def test_records_are_sorted_by_name(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        """Deterministic order so scripts diffing successive snapshots
        get a stable stream."""
        import json as _json
        b1 = tmp_path / "b1"
        b1.write_text("#!/bin/sh\n")
        b1.chmod(0o755)
        b2 = tmp_path / "b2"
        b2.write_text("#!/bin/sh\n")
        b2.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.zulu]\n'
            'kind = "binary"\n'
            f'binary = "{b1}"\n'
            '\n'
            '[programs.alpha]\n'
            'kind = "binary"\n'
            f'binary = "{b2}"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        assert result.exit_code == 0
        names = [r["name"] for r in _json.loads(result.output)]
        assert names == ["alpha", "zulu"]

    def test_json_output_is_valid_json(
        self, cfg_dir: Path, tmp_path: Path
    ) -> None:
        """Defensive: nothing extra (no trailing prose, no markers)
        should leak in. The whole stdout must be parseable JSON."""
        import json as _json
        bin_ok = tmp_path / "b"
        bin_ok.write_text("#!/bin/sh\n")
        bin_ok.chmod(0o755)
        (cfg_dir / "config.toml").write_text(
            'default_host = "localhost"\n'
            '[programs.x]\n'
            'kind = "binary"\n'
            f'binary = "{bin_ok}"\n'
        )
        result = CliRunner().invoke(main, ["programs", "localhost", "--json"])
        # Must parse cleanly.
        _json.loads(result.output)

    def test_help_mentions_json_flag(self) -> None:
        result = CliRunner().invoke(main, ["programs", "--help"])
        assert result.exit_code == 0
        assert "--json" in result.output
