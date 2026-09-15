"""Old-code contracts for Python import identity probes.

The public model results and the legacy probe tuples are both consumed outside
the config parser.  These cases land before moving the cohesive import-probe
primitive family out of ``vq.config``.
"""
from __future__ import annotations

import importlib.machinery
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

from vq import _program_probe, config


def _write_python_wrapper(path: Path, module_dir: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "#!/bin/sh\n"
        f"PYTHONPATH={shlex.quote(str(module_dir))} "
        f"exec {shlex.quote(sys.executable)} -S \"$@\"\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_mpi4py(
    module_dir: Path,
    *,
    initialized: bool,
    vendor: str = "Intel MPI",
    library: str = "Intel(R) MPI Library test",
) -> None:
    package = module_dir / "mpi4py"
    package.mkdir()
    (package / "__init__.py").write_text(
        "class _RC:\n"
        "    initialize = True\n"
        "rc = _RC()\n",
        encoding="utf-8",
    )
    (package / "MPI.py").write_text(
        f"def Is_initialized():\n    return {initialized!r}\n"
        "def Get_version():\n    return (4, 1)\n"
        f"def get_vendor():\n    return ({vendor!r}, (1, 0, 0))\n"
        f"def Get_library_version():\n    return {library!r}\n",
        encoding="utf-8",
    )


def test_import_probe_family_uses_config_free_compatibility_aliases() -> None:
    from vq import _program_probe

    assert config._IMPORT_VERSION_PREFIX == _program_probe._IMPORT_VERSION_PREFIX
    assert config.import_probe_code is _program_probe.import_probe_code
    assert config._split_import_version is _program_probe._split_import_version
    assert (
        config.run_import_identity_probe
        is _program_probe.run_import_identity_probe
    )
    assert (
        config.run_import_runtime_identity_probe
        is _program_probe.run_import_runtime_identity_probe
    )
    assert config.run_import_probe is _program_probe.run_import_probe


def test_failed_vibeqc_import_reports_loaded_core_path(tmp_path: Path) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    core = package / "_vibeqc_core.py"
    core.write_text("# old core lacks Required\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "from ._vibeqc_core import Required\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, version, module_path, core_path = (
        config.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc != 0
    assert "cannot import name 'Required'" in output
    assert version is None
    assert module_path == str(package / "__init__.py")
    assert core_path == str(core)


def test_failed_vibeqc_import_finds_core_from_distribution_metadata(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "raise ImportError('broken package before core load')\n",
        encoding="utf-8",
    )
    core = module_dir / "installed" / "vibeqc" / "_vibeqc_core.so"
    core.parent.mkdir(parents=True)
    core.write_text("old installed core\n", encoding="utf-8")
    dist_info = module_dir / "vibe_qc-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 1.0\n",
        encoding="utf-8",
    )
    (dist_info / "RECORD").write_text(
        "installed/vibeqc/_vibeqc_core.so,,\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, _version, _module_path, core_path = (
        config.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc != 0
    assert "broken package before core load" in output
    assert core_path == str(core)


def test_timed_out_vibeqc_import_preserves_flushed_core_path(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "import time\ntime.sleep(60)\n",
        encoding="utf-8",
    )
    core = module_dir / "installed" / "vibeqc" / "_vibeqc_core.so"
    core.parent.mkdir(parents=True)
    core.write_text("old installed core\n", encoding="utf-8")
    dist_info = module_dir / "vibe_qc-1.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: vibe-qc\nVersion: 1.0\n",
        encoding="utf-8",
    )
    (dist_info / "RECORD").write_text(
        "installed/vibeqc/_vibeqc_core.so,,\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, version, module_path, core_path = (
        config.run_import_runtime_identity_probe(
            str(python), "vibeqc", timeout=1.0
        )
    )

    assert rc != 0
    assert "probe timed out" in output
    assert "__vq_import_native_core_path__" not in output
    assert version is None
    assert module_path == str(package / "__init__.py")
    assert core_path == str(core)


def test_failed_vibeqc_import_prefers_loader_priority_core(
    tmp_path: Path,
) -> None:
    specific_suffix = next(
        (
            suffix
            for suffix in importlib.machinery.EXTENSION_SUFFIXES
            if suffix != ".so"
        ),
        None,
    )
    if specific_suffix is None:
        pytest.skip("platform exposes no ABI-specific extension suffix")
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    preferred = package / f"_vibeqc_core{specific_suffix}"
    fallback = package / "_vibeqc_core.so"
    preferred.write_text("preferred core\n", encoding="utf-8")
    fallback.write_text("leftover generic core\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "raise ImportError('package failed before core load')\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, _version, _module_path, core_path = (
        config.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc != 0
    assert "package failed before core load" in output
    assert core_path == str(preferred)


def test_failed_vibeqc_import_prefers_meta_path_core_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    source_core = package / "_vibeqc_core.so"
    source_core.write_text("stale source core\n", encoding="utf-8")
    mapped_core = tmp_path / "build" / "vibeqc" / "_vibeqc_core.so"
    mapped_core.parent.mkdir(parents=True)
    mapped_core.write_text("editable finder core\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "raise ImportError('package failed before core load')\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    original = _program_probe.import_probe_code

    def finder_probe_code(module: str, symbols: list[str] | None = None) -> str:
        return (
            "import importlib.machinery, sys\n"
            "class _MappedCoreFinder:\n"
            "    def find_spec(self, fullname, path=None, target=None):\n"
            "        if fullname == 'vibeqc._vibeqc_core':\n"
            "            return importlib.machinery.ModuleSpec(\n"
            f"                fullname, loader=None, origin={str(mapped_core)!r}\n"
            "            )\n"
            "        return None\n"
            "sys.meta_path.insert(0, _MappedCoreFinder())\n"
            + original(module, symbols)
        )

    monkeypatch.setattr(_program_probe, "import_probe_code", finder_probe_code)

    rc, output, _version, _module_path, core_path = (
        _program_probe.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc != 0
    assert "package failed before core load" in output
    assert core_path == str(mapped_core)


def test_authoritative_unlocatable_core_spec_blocks_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    fallback = package / "_vibeqc_core.so"
    fallback.write_text("must not be selected\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "raise ImportError('package failed before core load')\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    original = _program_probe.import_probe_code

    def blocked_probe_code(module: str, symbols: list[str] | None = None) -> str:
        return (
            "import importlib.machinery, sys\n"
            "class _UnlocatableCoreFinder:\n"
            "    def find_spec(self, fullname, path=None, target=None):\n"
            "        if fullname == 'vibeqc._vibeqc_core':\n"
            "            return importlib.machinery.ModuleSpec(\n"
            "                fullname, loader=None, origin=None\n"
            "            )\n"
            "        return None\n"
            "sys.meta_path.insert(0, _UnlocatableCoreFinder())\n"
            + original(module, symbols)
        )

    monkeypatch.setattr(_program_probe, "import_probe_code", blocked_probe_code)

    rc, output, _version, _module_path, core_path = (
        _program_probe.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc != 0
    assert "package failed before core load" in output
    assert core_path is None


def test_vibeqc_probe_uses_registered_interpreter_extension_suffixes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module_dir = tmp_path / "modules"
    package = module_dir / "vibeqc"
    package.mkdir(parents=True)
    target_suffix = ".target-interpreter.so"
    core = package / f"_vibeqc_core{target_suffix}"
    core.write_text("target core\n", encoding="utf-8")
    (package / "__init__.py").write_text(
        "class _Core:\n"
        "    pass\n"
        "_vibeqc_core = _Core()\n"
        f"_vibeqc_core.__file__ = {str(core)!r}\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    original = _program_probe.import_probe_code

    def target_probe_code(module: str, symbols: list[str] | None = None) -> str:
        return (
            "import importlib.machinery\n"
            f"importlib.machinery.EXTENSION_SUFFIXES = [{target_suffix!r}]\n"
            + original(module, symbols)
        )

    monkeypatch.setattr(_program_probe, "import_probe_code", target_probe_code)

    rc, output, _version, _module_path, core_path = (
        _program_probe.run_import_runtime_identity_probe(str(python), "vibeqc")
    )

    assert rc == 0, output
    assert core_path == str(core)


def test_successful_managed_import_requires_loaded_core_not_disk_candidate(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "checkout"
    package = source_root / "python" / "vibeqc"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text(
        "__version__ = '1.2.3'\n",
        encoding="utf-8",
    )
    core = package / f"_vibeqc_core{importlib.machinery.EXTENSION_SUFFIXES[0]}"
    core.write_text("not a loaded extension\n", encoding="utf-8")
    native_source = source_root / "cpp" / "src" / "bindings.cpp"
    native_source.parent.mkdir(parents=True)
    native_source.write_text("// source\n", encoding="utf-8")
    os.utime(native_source, ns=(1_000_000_000, 1_000_000_000))
    os.utime(core, ns=(2_000_000_000, 2_000_000_000))
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, source_root / "python")

    rc, output, version, module_path, core_path = (
        config.run_import_runtime_identity_probe(
            str(python),
            "vibeqc",
            source_root=source_root,
        )
    )

    assert rc != 0
    assert "did not report a compiled core path" in output
    assert version == "1.2.3"
    assert module_path == str(package / "__init__.py")
    assert core_path is None


def test_unpaired_core_path_cannot_inherit_prior_extension_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix = _program_probe._IMPORT_NATIVE_CORE_PATH_PREFIX
    partial = f"{prefix}1:/old/_vibeqc_core.so\n{prefix}/new/_vibeqc_core.py\n"

    def time_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(
            cmd=["python", "-c", "probe"],
            timeout=1.0,
            output=partial,
        )

    monkeypatch.setattr(_program_probe.subprocess, "run", time_out)

    rc, output, _version, _module_path, core_path = (
        _program_probe.run_import_runtime_identity_probe(
            "python", "vibeqc", timeout=1.0
        )
    )

    assert rc != 0
    assert core_path == "/new/_vibeqc_core.py"
    assert "lacks target-supported compiled-extension proof" in output


@pytest.mark.parametrize(
    ("vendor", "library"),
    (
        ("Intel MPI", "Intel(R) MPI Library test"),
        ("Open MPI", "Open MPI v4 test"),
        ("MPICH", "MPICH Version test"),
    ),
)
def test_vibeqc_import_probe_disables_portable_singleton_mpi_init(
    tmp_path: Path,
    vendor: str,
    library: str,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    _write_fake_mpi4py(
        module_dir,
        initialized=False,
        vendor=vendor,
        library=library,
    )
    (module_dir / "vibeqc.py").write_text(
        "import mpi4py\n"
        "if mpi4py.rc.initialize:\n"
        "    raise RuntimeError('MPI auto-init was not disabled before vibeqc')\n"
        "Required = object()\n"
        "__version__ = '1.2.3'\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)
    program = config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(git_dir),
        import_check="vibeqc",
        import_symbols=["Required"],
    )

    ok, reason = program.availability()

    assert ok is True
    assert "`import vibeqc` (__version__=1.2.3)" in reason
    assert "+ symbols [Required] ok" in reason
    code = config.import_probe_code("vibeqc", ["Required"])
    assert code.index("_vq_mpi4py.rc.initialize = False") < code.index(
        "importlib.import_module(module_name)"
    )
    assert "_vq_mpi.Get_library_version()" in code
    assert "_vq_mpi.get_vendor()" in code
    assert "assert " not in code


def test_vibeqc_import_probe_allows_a_release_without_mpi4py(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    (module_dir / "vibeqc.py").write_text(
        "Required = object()\n__version__ = '1.2.3'\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    assert config.run_import_identity_probe(
        str(python), "vibeqc", symbols=["Required"]
    ) == (0, "", "1.2.3")


def test_vibeqc_import_probe_fails_if_mpi_initialized_despite_preactivation(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    _write_fake_mpi4py(module_dir, initialized=True)
    (module_dir / "vibeqc.py").write_text("value = 1\n", encoding="utf-8")
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, version = config.run_import_identity_probe(
        str(python), "vibeqc"
    )

    assert rc != 0
    assert output.splitlines()[-1] == (
        "RuntimeError: vibeqc import probe unexpectedly initialized MPI"
    )
    assert version is None


@pytest.mark.parametrize(
    ("module_source", "expected_version"),
    (
        (
            "Required = object()\n"
            "__version__ = 'fallback'\n"
            "def version_info():\n"
            "    return '1.2.3'\n",
            "1.2.3",
        ),
        (
            "Required = object()\n"
            "__version__ = 'fallback'\n"
            "def version_info():\n"
            "    return {'version': '2.3.4', 'vibeqc_version': 'ignored'}\n",
            "2.3.4",
        ),
        (
            "Required = object()\n"
            "def version_info():\n"
            "    return {'vibeqc_version': '3.4.5'}\n",
            "3.4.5",
        ),
        (
            "Required = object()\n"
            "class Info:\n"
            "    version = '4.5.6'\n"
            "def version_info():\n"
            "    return Info()\n",
            "4.5.6",
        ),
        (
            "Required = object()\n"
            "__version__ = '5.6.7'\n"
            "def version_info():\n"
            "    raise RuntimeError('identity unavailable')\n",
            "5.6.7",
        ),
        ("Required = object()\n", None),
    ),
)
def test_import_program_version_selection_and_symbol_result(
    module_source: str,
    expected_version: str | None,
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    (module_dir / "probe_target.py").write_text(module_source, encoding="utf-8")
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    program = config.ImportProgram(
        kind="import",
        python=str(python),
        import_check="probe_target",
        import_symbols=["Required"],
    )

    ok, reason = program.availability()

    assert ok is True
    assert "`import probe_target`" in reason
    assert "+ symbols [Required]" in reason
    if expected_version is None:
        assert "__version__=" not in reason
    else:
        assert f"__version__={expected_version}" in reason
    assert program.import_version() == expected_version


def test_import_identity_probe_preserves_tuple_and_subprocess_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            7,
            stdout="module noise\n__vq_import_version__=9.8.7\n",
            stderr="import warning\n",
        )

    monkeypatch.setattr(subprocess, "run", run)
    result = config.run_import_identity_probe(
        "/venv/bin/python",
        "probe_target",
        symbols=["Required"],
        timeout=2.5,
    )

    assert result == (7, "module noise\nimport warning", "9.8.7")
    assert calls == [
        (
            [
                "/venv/bin/python",
                "-c",
                config.import_probe_code("probe_target", ["Required"]),
            ],
            {
                "capture_output": True,
                "text": True,
                "timeout": 2.5,
                "stdin": subprocess.DEVNULL,
            },
        )
    ]


@pytest.mark.parametrize(
    ("output", "expected"),
    (
        (
            "before\n__vq_import_version__=1.0\nmiddle\n"
            "__vq_import_version__=2.0\nafter\n",
            ("before\nmiddle\nafter", "2.0"),
        ),
        ("__vq_import_version__=\n", ("", None)),
        ("plain output\n", ("plain output", None)),
    ),
)
def test_import_version_marker_split_contract(
    output: str, expected: tuple[str, str | None]
) -> None:
    assert config._split_import_version(output) == expected


def test_import_identity_concatenates_streams_without_inserting_separator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="stdout-without-newline",
            stderr="stderr-without-newline",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert config.run_import_identity_probe(
        "/venv/bin/python", "probe_target"
    ) == (0, "stdout-without-newlinestderr-without-newline", None)


def test_import_identity_uses_last_marker_across_stdout_then_stderr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout="stdout noise\n__vq_import_version__=1.0\n",
            stderr="stderr noise\n__vq_import_version__=2.0\n",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert config.run_import_identity_probe(
        "/venv/bin/python", "probe_target"
    ) == (0, "stdout noise\nstderr noise", "2.0")


def test_import_identity_timeout_result_is_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: list[str], **kwargs: object) -> None:
        raise subprocess.TimeoutExpired(
            argv,
            timeout=float(kwargs["timeout"]),
            output="partial import output",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert config.run_import_identity_probe(
        "/venv/bin/python", "probe_target", timeout=2.5
    ) == (
        1,
        "import probe_target: probe timed out after 2.5s",
        None,
    )


def test_import_identity_start_error_result_is_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise OSError("injected exec failure")

    monkeypatch.setattr(subprocess, "run", run)
    assert config.run_import_identity_probe(
        "/venv/bin/python", "probe_target"
    ) == (
        1,
        "import probe_target: probe failed to start: injected exec failure",
        None,
    )


def test_import_identity_unexpected_exception_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(_argv: list[str], **_kwargs: object) -> None:
        raise RuntimeError("unexpected import probe failure")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(RuntimeError, match="unexpected import probe failure"):
        config.run_import_identity_probe("/venv/bin/python", "probe_target")


def test_legacy_import_probe_drops_only_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(argv: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(
            argv,
            7,
            stdout="__vq_import_version__=1.0\nvisible output\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert config.run_import_probe(
        "/venv/bin/python",
        "probe_target",
        symbols=["Required"],
        timeout=120,
    ) == (7, "visible output")
    assert calls == [
        (
            [
                "/venv/bin/python",
                "-c",
                config.import_probe_code("probe_target", ["Required"]),
            ],
            {
                "capture_output": True,
                "text": True,
                "timeout": 120,
                "stdin": subprocess.DEVNULL,
            },
        )
    ]


def test_missing_symbols_preserve_requested_order_before_version_result(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    (module_dir / "probe_target.py").write_text(
        "__version__ = '9.9.9'\n",
        encoding="utf-8",
    )
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)

    rc, output, version = config.run_import_identity_probe(
        str(python),
        "probe_target",
        symbols=["Zulu", "Alpha"],
    )

    assert rc != 0
    assert output.splitlines()[-1] == (
        "ImportError: missing symbol(s) from probe_target: Zulu, Alpha"
    )
    assert version is None


@pytest.mark.parametrize("failure", ("timeout", "oserror"))
def test_import_program_fault_reason_and_version_result(
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    python = tmp_path / "python"
    python.touch()

    def run(argv: list[str], **kwargs: object) -> None:
        if failure == "timeout":
            raise subprocess.TimeoutExpired(argv, timeout=float(kwargs["timeout"]))
        raise OSError("injected exec failure")

    monkeypatch.setattr(subprocess, "run", run)
    program = config.ImportProgram(
        kind="import",
        python=str(python),
        import_check="probe_target",
    )

    ok, reason = program.availability()

    assert ok is False
    if failure == "timeout":
        assert reason == (
            "import probe_target failed: "
            "import probe_target: probe timed out after 15s"
        )
    else:
        assert reason == (
            "import probe_target failed: import probe_target: "
            "probe failed to start: injected exec failure"
        )
    assert program.import_version() is None


def test_venv_import_failure_preserves_probe_tail(tmp_path: Path) -> None:
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)
    program = config.VenvProgram(
        kind="venv",
        python=sys.executable,
        git_dir=str(git_dir),
        import_check="module_that_does_not_exist_vq_probe_contract",
    )

    ok, reason = program.availability()

    assert ok is False
    assert reason.startswith(
        "import module_that_does_not_exist_vq_probe_contract failed: "
    )
    assert reason.endswith(
        "No module named 'module_that_does_not_exist_vq_probe_contract'"
    )


def test_venv_expected_import_version_requires_reported_version(
    tmp_path: Path,
) -> None:
    module_dir = tmp_path / "modules"
    module_dir.mkdir()
    (module_dir / "probe_target.py").write_text("value = 1\n", encoding="utf-8")
    python = tmp_path / "venv" / "bin" / "python"
    _write_python_wrapper(python, module_dir)
    git_dir = tmp_path / "checkout"
    (git_dir / ".git").mkdir(parents=True)
    program = config.VenvProgram(
        kind="venv",
        python=str(python),
        git_dir=str(git_dir),
        import_check="probe_target",
        expected_import_version="1.2.3",
    )

    assert program.availability() == (
        False,
        "runtime pin mismatch: import probe_target version expected 1.2.3, "
        "got (not reported)",
    )


@pytest.mark.parametrize(
    ("probe_result", "expected"),
    (
        ((0, "", "1.2.3"), []),
        (
            (0, "", None),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
        (
            (0, "", "9.9.9"),
            ["import probe_target version expected 1.2.3, got 9.9.9"],
        ),
        (
            (1, "import failed", None),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
        (
            (1, "import failed after marker", "1.2.3"),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
    ),
)
def test_venv_runtime_pin_import_results(
    probe_result: tuple[int, str, str | None],
    expected: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, list[str] | None]] = []

    def probe(
        python: str,
        module: str,
        *,
        symbols: list[str] | None = None,
        timeout: float = 15.0,
    ) -> tuple[int, str, str | None]:
        assert timeout == 15.0
        calls.append((python, module, symbols))
        return probe_result

    monkeypatch.setattr(config, "run_import_identity_probe", probe)
    program = config.VenvProgram(
        kind="venv",
        python="/venv/bin/python",
        git_dir="/checkout",
        import_check="probe_target",
        import_symbols=["Required"],
        expected_import_version="1.2.3",
    )

    assert program.runtime_pin_mismatches() == expected
    assert calls == [("/venv/bin/python", "probe_target", ["Required"])]


def test_admin_import_gate_forwards_symbols_and_long_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import admin

    calls: list[tuple[str, str, list[str] | None, float]] = []

    def probe(
        python: str,
        module: str,
        *,
        symbols: list[str] | None = None,
        timeout: float = 15.0,
    ) -> tuple[int, str]:
        calls.append((python, module, symbols, timeout))
        return 7, "import failed"

    monkeypatch.setattr(config, "run_import_probe", probe)
    assert admin._run_import_check(
        "/venv/bin/python", "probe_target", symbols=["Required"]
    ) == (7, "import failed")
    assert calls == [
        ("/venv/bin/python", "probe_target", ["Required"], 120)
    ]


@pytest.mark.parametrize(
    ("probe_result", "expected"),
    (
        ((0, "", "1.2.3"), []),
        (
            (0, "", None),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
        (
            (0, "", "9.9.9"),
            ["import probe_target version expected 1.2.3, got 9.9.9"],
        ),
        (
            (1, "import failed", None),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
        (
            (1, "import failed after marker", "1.2.3"),
            [
                "import probe_target version expected 1.2.3, "
                "but current import version could not be read"
            ],
        ),
    ),
)
def test_daemon_import_pin_probe_forwards_identity_and_fails_closed(
    probe_result: tuple[int, str, str | None],
    expected: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from vq import daemon as daemon_module
    from vq.spec import ProgramRuntimePin

    calls: list[tuple[str, str, list[str] | None]] = []

    def probe(
        python: str,
        module: str,
        *,
        symbols: list[str] | None = None,
    ) -> tuple[int, str, str | None]:
        calls.append((python, module, symbols))
        return probe_result

    monkeypatch.setattr(daemon_module, "run_import_identity_probe", probe)
    program = config.VenvProgram(
        kind="venv",
        python="/venv/bin/python",
        git_dir="/checkout",
        import_check="configured_fallback",
        import_symbols=["ConfiguredSymbol"],
    )
    pin = ProgramRuntimePin(
        expected_import_version="1.2.3",
        import_check="probe_target",
        import_symbols=["Required"],
    )

    assert daemon_module._runtime_pin_snapshot_mismatches(program, pin) == expected
    assert calls == [("/venv/bin/python", "probe_target", ["Required"])]
