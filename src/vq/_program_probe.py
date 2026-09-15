"""Config-free runtime inspection helpers for registered programs."""
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

_IMPORT_VERSION_PREFIX = "__vq_import_version__="
_IMPORT_MODULE_PATH_PREFIX = "__vq_import_module_path__="
_IMPORT_NATIVE_CORE_PATH_PREFIX = "__vq_import_native_core_path__="
_VIBEQC_NATIVE_SOURCE_GLOBS = (
    "*.cpp",
    "*.hpp",
    "*.h",
    "*.cc",
    "*.cu",
    "*.inc",
    "*.in",
)
VIBEQC_UNSUPPORTED_CORE_ERROR = (
    "managed vibeqc core lacks target-supported compiled-extension proof"
)


def import_probe_code(module: str, symbols: list[str] | None = None) -> str:
    """Python snippet for a module import plus optional exported-symbol check."""
    wanted = list(symbols or [])
    return (
        "import importlib, importlib.machinery, importlib.metadata, "
        "importlib.util, os, sys\n"
        f"module_name = {module!r}\n"
        "def _vq_emit_core_path(_vq_path):\n"
        "    _vq_name = os.path.basename(str(_vq_path))\n"
        "    _vq_supported = any(\n"
        "        _vq_name == '_vibeqc_core' + _vq_suffix\n"
        "        for _vq_suffix in importlib.machinery.EXTENSION_SUFFIXES\n"
        "    )\n"
        f"    print({_IMPORT_NATIVE_CORE_PATH_PREFIX!r} + "
        "('1:' if _vq_supported else '0:') + str(_vq_path), flush=True)\n"
        "_vq_module_spec = importlib.util.find_spec(module_name)\n"
        "_vq_spec_origin = getattr(_vq_module_spec, 'origin', None)\n"
        "if _vq_spec_origin is not None:\n"
        f"    print({_IMPORT_MODULE_PATH_PREFIX!r} + str(_vq_spec_origin), "
        "flush=True)\n"
        "_vq_discovered_core_path = None\n"
        "if module_name == 'vibeqc' and _vq_module_spec is not None:\n"
        "    _vq_package_roots = list(\n"
        "        _vq_module_spec.submodule_search_locations or []\n"
        "    )\n"
        "    _vq_core_resolution_blocked = False\n"
        "    for _vq_finder in sys.meta_path:\n"
        "        _vq_find_spec = getattr(_vq_finder, 'find_spec', None)\n"
        "        if _vq_find_spec is None:\n"
        "            continue\n"
        "        try:\n"
        "            _vq_core_spec = _vq_find_spec(\n"
        "                'vibeqc._vibeqc_core', _vq_package_roots\n"
        "            )\n"
        "        except Exception:\n"
        "            _vq_core_resolution_blocked = True\n"
        "            break\n"
        "        if _vq_core_spec is None:\n"
        "            continue\n"
        "        _vq_core_origin = getattr(_vq_core_spec, 'origin', None)\n"
        "        if _vq_core_origin and os.path.isfile(_vq_core_origin):\n"
        "            _vq_discovered_core_path = str(_vq_core_origin)\n"
        "        else:\n"
        "            _vq_core_resolution_blocked = True\n"
        "        break\n"
        "    for _vq_package_root in (\n"
        "        [] if _vq_core_resolution_blocked else _vq_package_roots\n"
        "    ):\n"
        "        for _vq_suffix in importlib.machinery.EXTENSION_SUFFIXES:\n"
        "            if _vq_discovered_core_path is not None:\n"
        "                break\n"
        "            _vq_candidate = os.path.join(\n"
        "                _vq_package_root, '_vibeqc_core' + _vq_suffix\n"
        "            )\n"
        "            if os.path.isfile(_vq_candidate):\n"
        "                _vq_discovered_core_path = _vq_candidate\n"
        "    if (\n"
        "        _vq_discovered_core_path is None\n"
        "        and not _vq_core_resolution_blocked\n"
        "    ):\n"
        "        try:\n"
        "            _vq_distribution = importlib.metadata.distribution('vibe-qc')\n"
        "        except importlib.metadata.PackageNotFoundError:\n"
        "            _vq_distribution = None\n"
        "        _vq_dist_candidates = []\n"
        "        if _vq_distribution is not None:\n"
        "            for _vq_entry in (_vq_distribution.files or []):\n"
        "                if not _vq_entry.name.startswith('_vibeqc_core'):\n"
        "                    continue\n"
        "                _vq_candidate = str(\n"
        "                    _vq_distribution.locate_file(_vq_entry)\n"
        "                )\n"
        "                if os.path.isfile(_vq_candidate):\n"
        "                    _vq_dist_candidates.append(_vq_candidate)\n"
        "        for _vq_suffix in importlib.machinery.EXTENSION_SUFFIXES:\n"
        "            _vq_expected_name = '_vibeqc_core' + _vq_suffix\n"
        "            _vq_matches = sorted(\n"
        "                candidate for candidate in _vq_dist_candidates\n"
        "                if os.path.basename(candidate) == _vq_expected_name\n"
        "            )\n"
        "            if _vq_matches:\n"
        "                _vq_discovered_core_path = _vq_matches[0]\n"
        "                break\n"
        "    if _vq_discovered_core_path is not None:\n"
        "        _vq_emit_core_path(_vq_discovered_core_path)\n"
        "_vq_mpi4py = None\n"
        "_vq_mpi4py_spec = (\n"
        "    importlib.util.find_spec('mpi4py')\n"
        "    if module_name == 'vibeqc'\n"
        "    else None\n"
        ")\n"
        "if _vq_mpi4py_spec is not None:\n"
        "    import mpi4py as _vq_mpi4py\n"
        "    _vq_mpi4py.rc.initialize = False\n"
        "try:\n"
        "    module = importlib.import_module(module_name)\n"
        "except BaseException:\n"
        "    if module_name == 'vibeqc':\n"
        "        _vq_failed_core = sys.modules.get('vibeqc._vibeqc_core')\n"
        "        _vq_failed_core_path = getattr(_vq_failed_core, '__file__', None)\n"
        "        if _vq_failed_core_path is not None:\n"
        "            _vq_emit_core_path(_vq_failed_core_path)\n"
        "    raise\n"
        "_vq_module_path = getattr(module, '__file__', None)\n"
        "if _vq_module_path is not None:\n"
        f"    print({_IMPORT_MODULE_PATH_PREFIX!r} + str(_vq_module_path), flush=True)\n"
        "if module_name == 'vibeqc':\n"
        "    _vq_native_core = getattr(module, '_vibeqc_core', None)\n"
        "    _vq_native_core_path = getattr(_vq_native_core, '__file__', None)\n"
        "    if _vq_native_core_path is not None:\n"
        "        _vq_emit_core_path(_vq_native_core_path)\n"
        "    else:\n"
        f"        print({_IMPORT_NATIVE_CORE_PATH_PREFIX!r} + '0:', flush=True)\n"
        "if _vq_mpi4py is not None:\n"
        "    from mpi4py import MPI as _vq_mpi\n"
        "    if _vq_mpi.Is_initialized():\n"
        "        raise RuntimeError(\n"
        "            'vibeqc import probe unexpectedly initialized MPI'\n"
        "        )\n"
        "    _vq_mpi.Get_version()\n"
        "    _vq_mpi_vendor = _vq_mpi.get_vendor()\n"
        "    _vq_mpi_library = _vq_mpi.Get_library_version()\n"
        "    if not _vq_mpi_vendor or not _vq_mpi_library:\n"
        "        raise RuntimeError('vibeqc native MPI identity is missing')\n"
        f"wanted = {wanted!r}\n"
        "missing = [name for name in wanted if not hasattr(module, name)]\n"
        "if missing:\n"
        "    raise ImportError(\n"
        "        'missing symbol(s) from ' + module_name + ': ' + ', '.join(missing)\n"
        "    )\n"
        "version = None\n"
        "version_info = getattr(module, 'version_info', None)\n"
        "if callable(version_info):\n"
        "    try:\n"
        "        info = version_info()\n"
        "    except Exception:\n"
        "        info = None\n"
        "    if isinstance(info, str):\n"
        "        version = info\n"
        "    elif isinstance(info, dict):\n"
        "        version = info.get('version') or info.get('vibeqc_version')\n"
        "    elif info is not None:\n"
        "        version = getattr(info, 'version', None)\n"
        "if version is None:\n"
        "    version = getattr(module, '__version__', None)\n"
        "if version is not None:\n"
        f"    print({_IMPORT_VERSION_PREFIX!r} + str(version), flush=True)\n"
    )


def _split_import_version(output: str) -> tuple[str, str | None]:
    version: str | None = None
    lines: list[str] = []
    for line in output.splitlines():
        if line.startswith(_IMPORT_VERSION_PREFIX):
            version = line[len(_IMPORT_VERSION_PREFIX):] or None
        else:
            lines.append(line)
    return "\n".join(lines), version


def _split_import_runtime_paths(
    output: str,
) -> tuple[str, str | None, str | None, bool | None]:
    module_path: str | None = None
    native_core_path: str | None = None
    native_core_supported: bool | None = None
    lines: list[str] = []
    for line in output.splitlines():
        if line.startswith(_IMPORT_MODULE_PATH_PREFIX):
            module_path = line[len(_IMPORT_MODULE_PATH_PREFIX):] or None
        elif line.startswith(_IMPORT_NATIVE_CORE_PATH_PREFIX):
            value = line[len(_IMPORT_NATIVE_CORE_PATH_PREFIX):]
            if value.startswith(("1:", "0:")):
                native_core_supported = value[0] == "1"
                native_core_path = value[2:] or None
            else:
                native_core_path = value or None
                native_core_supported = None
        else:
            lines.append(line)
    return (
        "\n".join(lines),
        module_path,
        native_core_path,
        native_core_supported,
    )


def _managed_vibeqc_runtime_error(
    source_root: Path,
    *,
    module_path: str | None,
    native_core_path: str | None,
) -> str | None:
    """Return why a managed vibe-qc runtime cannot be trusted.

    Editable installs can import Python from the live checkout while retaining
    an older compiled extension.  Import success and package metadata therefore
    do not prove that the core matches the source.  This applies the same
    conservative file-mtime direction as vibe-qc's test-session stale-core
    guard, but readiness must fail closed because a stale core can silently
    compute wrong numbers.
    """
    expected_package = (source_root / "python" / "vibeqc").resolve()
    if module_path is None:
        return "managed vibeqc import did not report its package path"
    try:
        imported_module = Path(module_path).resolve(strict=True)
    except OSError as exc:
        return f"managed vibeqc package path is unreadable: {exc}"
    if imported_module.parent != expected_package:
        return (
            "managed vibeqc imported from the wrong checkout: "
            f"{imported_module} (expected {expected_package})"
        )
    if native_core_path is None:
        return "managed vibeqc import did not report a compiled core path"
    try:
        native_core = Path(native_core_path).resolve(strict=True)
    except OSError as exc:
        return f"managed vibeqc compiled core path is unreadable: {exc}"
    if not native_core.is_file():
        return f"managed vibeqc compiled core is not a file: {native_core}"
    try:
        source_mtimes = vibeqc_native_source_mtimes(source_root)
    except OSError as exc:
        return f"managed vibeqc native sources could not be inspected: {exc}"
    if not source_mtimes:
        return (
            "managed vibeqc native sources could not be inspected under "
            f"{source_root / 'cpp'}"
        )
    newest_source, newest_mtime = max(source_mtimes, key=lambda item: item[1])
    try:
        core_mtime = native_core.stat().st_mtime_ns
    except OSError as exc:
        return f"managed vibeqc compiled core could not be inspected: {exc}"
    if core_mtime < newest_mtime:
        return (
            "stale compiled core: "
            f"{native_core} predates "
            f"{newest_source.relative_to(source_root)}; rebuild required"
        )
    return None


def vibeqc_native_source_mtimes(
    source_root: Path,
) -> tuple[tuple[Path, int], ...]:
    """Capture native compilation-input mtimes for the stale-core contract."""
    cpp = source_root / "cpp"
    if not cpp.is_dir():
        return ()
    inputs: set[Path] = set()
    for pattern in _VIBEQC_NATIVE_SOURCE_GLOBS:
        inputs.update(cpp.rglob(pattern))
    inputs.update(cpp.rglob("CMakeLists.txt"))
    inputs.update(cpp.rglob("*.cmake"))
    inputs.update(cpp.rglob("*.cmake.in"))
    root_cmake = source_root / "CMakeLists.txt"
    if root_cmake.is_file():
        inputs.add(root_cmake)
    return tuple(
        (source, source.stat().st_mtime_ns)
        for source in sorted(inputs)
    )


def run_import_runtime_identity_probe(
    python: str,
    module: str,
    *,
    symbols: list[str] | None = None,
    timeout: float = 15.0,
    source_root: Path | None = None,
) -> tuple[int, str, str | None, str | None, str | None]:
    """Probe import identity, including package and compiled-core paths."""
    try:
        proc = subprocess.run(
            [python, "-c", import_probe_code(module, symbols)],
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as exc:
        partial = "\n".join(
            part
            for part in (
                _timeout_stream_text(exc.output),
                _timeout_stream_text(exc.stderr),
            )
            if part
        )
        (
            cleaned,
            module_path,
            native_core_path,
            native_core_supported,
        ) = _split_import_runtime_paths(partial)
        _cleaned, version = _split_import_version(cleaned)
        message = f"import {module}: probe timed out after {timeout:g}s"
        if (
            module == "vibeqc"
            and native_core_path is not None
            and native_core_supported is not True
        ):
            message += f"; {VIBEQC_UNSUPPORTED_CORE_ERROR}"
        return 1, message, version, module_path, native_core_path
    except OSError as exc:
        return (
            1,
            f"import {module}: probe failed to start: {exc}",
            None,
            None,
            None,
        )
    output = (proc.stdout or "") + (proc.stderr or "")
    (
        cleaned,
        module_path,
        native_core_path,
        native_core_supported,
    ) = _split_import_runtime_paths(output)
    cleaned, version = _split_import_version(cleaned)
    rc = proc.returncode
    if (
        module == "vibeqc"
        and native_core_path is not None
        and native_core_supported is not True
    ):
        rc = 1
        error = VIBEQC_UNSUPPORTED_CORE_ERROR
        cleaned = f"{error}\n{cleaned}" if cleaned else error
    if rc == 0 and module == "vibeqc" and source_root is not None:
        error = _managed_vibeqc_runtime_error(
            source_root,
            module_path=module_path,
            native_core_path=native_core_path,
        )
        if error is not None:
            rc = 1
            cleaned = f"{cleaned}\n{error}" if cleaned else error
    return rc, cleaned, version, module_path, native_core_path


def run_import_identity_probe(
    python: str,
    module: str,
    *,
    symbols: list[str] | None = None,
    timeout: float = 15.0,
) -> tuple[int, str, str | None]:
    """Run an import/symbol probe and return ``(rc, output, version)``."""
    rc, output, version, _module_path, _native_core_path = (
        run_import_runtime_identity_probe(
            python,
            module,
            symbols=symbols,
            timeout=timeout,
        )
    )
    return rc, output, version


def run_import_probe(
    python: str,
    module: str,
    *,
    symbols: list[str] | None = None,
    timeout: float = 15.0,
) -> tuple[int, str]:
    """Run an import/symbol probe and return ``(returncode, stdout+stderr)``."""
    rc, output, _version = run_import_identity_probe(
        python, module, symbols=symbols, timeout=timeout,
    )
    return rc, output


def _query_git(git_dir: str, *args: str) -> str | None:
    try:
        proc = subprocess.run(
            ["git", "-C", git_dir, *args],
            capture_output=True,
            text=True,
            timeout=5.0,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return None
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    value = proc.stdout.strip()
    return value or None


def _git_has_changes(git_dir: str) -> bool | None:
    try:
        proc = subprocess.run(
            ["git", "-C", git_dir, "status", "--porcelain"],
            capture_output=True,
            text=True,
            timeout=5.0,
            stdin=subprocess.DEVNULL,
            env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"},
        )
    except subprocess.TimeoutExpired:
        return None
    except OSError:
        return None
    if proc.returncode != 0:
        return None
    return bool(proc.stdout.strip())


def _last_nonempty_line(text: str) -> str | None:
    for line in reversed(text.splitlines()):
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def _timeout_stream_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return str(value)


HEALTHCHECK_OK = "ok"
HEALTHCHECK_NOT_CONFIGURED = "not-configured"
HEALTHCHECK_NOT_RUN = "not-run"
HEALTHCHECK_COULD_NOT_START = "could-not-start"
HEALTHCHECK_TIMED_OUT = "timed-out"
HEALTHCHECK_FAILED = "failed"


@dataclass(frozen=True)
class VenvHealthcheck:
    """A healthcheck result that keeps *why* it failed as a value.

    "Could not start" is a configuration problem -- a Linux-only
    ``xvfb-run`` configured on a macOS host -- while "ran and failed" is a
    finding about the runtime. The two call for different responses, and the
    detail string is prose, so the distinction lives in ``status``.
    """

    status: str
    detail: str | None

    @property
    def ok(self) -> bool:
        return self.status in (HEALTHCHECK_OK, HEALTHCHECK_NOT_CONFIGURED)


def run_venv_healthcheck(
    healthcheck_command: str | None,
    *,
    python: str,
    git_dir: Path,
) -> tuple[bool, str | None]:
    result = classify_venv_healthcheck(
        healthcheck_command, python=python, git_dir=git_dir,
    )
    return result.ok, result.detail


def classify_venv_healthcheck(
    healthcheck_command: str | None,
    *,
    python: str,
    git_dir: Path,
) -> VenvHealthcheck:
    if not healthcheck_command:
        return VenvHealthcheck(HEALTHCHECK_NOT_CONFIGURED, None)
    try:
        argv = shlex.split(healthcheck_command)
    except ValueError as exc:
        return VenvHealthcheck(
            HEALTHCHECK_COULD_NOT_START,
            f"healthcheck command parse failed: {exc}",
        )
    if not argv:
        return VenvHealthcheck(
            HEALTHCHECK_COULD_NOT_START, "healthcheck command is empty",
        )
    env = os.environ.copy()
    env.setdefault("PYVISTA_OFF_SCREEN", "True")
    venv_bin = str(Path(python).parent)
    system_path = env.get("PATH", "")
    env["PATH"] = f"{venv_bin}{os.pathsep}{system_path}"
    venv_only = _healthcheck_binary_is_venv_only(
        argv[0], venv_bin=venv_bin, system_path=system_path,
    )
    try:
        proc = subprocess.run(
            argv,
            cwd=git_dir,
            env=env,
            capture_output=True,
            text=True,
            timeout=60.0,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(
            part
            for part in (
                _timeout_stream_text(exc.output),
                _timeout_stream_text(exc.stderr),
            )
            if part
        )
        tail = _last_nonempty_line(output)
        detail = f": {tail}" if tail else ""
        return VenvHealthcheck(
            HEALTHCHECK_TIMED_OUT, f"healthcheck timed out after 60s{detail}",
        )
    except OSError as exc:
        return VenvHealthcheck(
            HEALTHCHECK_COULD_NOT_START,
            f"healthcheck failed to start: {exc}"
            f" (searched {venv_bin} then PATH)",
        )
    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part)
    tail = _last_nonempty_line(output)
    if proc.returncode != 0:
        detail = f": {tail}" if tail else ""
        return VenvHealthcheck(
            HEALTHCHECK_FAILED, f"healthcheck rc={proc.returncode}{detail}",
        )
    if venv_only is not None:
        # Reported while it still works, which is the only time it is
        # actionable. See :func:`_healthcheck_binary_is_venv_only`.
        return VenvHealthcheck(
            HEALTHCHECK_OK,
            f"{tail or 'ok'} (warning: {argv[0]} resolves only inside the "
            f"venv, at {venv_only}; a reprovision will not recreate it)",
        )
    return VenvHealthcheck(HEALTHCHECK_OK, tail or "ok")


def _healthcheck_binary_is_venv_only(
    argv0: str,
    *,
    venv_bin: str,
    system_path: str,
) -> str | None:
    """Path of a healthcheck binary that exists ONLY inside the venv, or None.

    vq prepends the venv's ``bin`` to PATH so a healthcheck can use the
    environment's own entry points, which is right. The failure mode is a
    binary that lives there and is not installed by anything: on two hosts
    ``vibeview-dev``'s ``xvfb-run`` was a hand-written shim inside the old
    venv's ``bin/``, present on no system path and reproduced by no reinstall.
    Migrating the venv broke every vibe-view healthcheck until it was copied
    across by hand.

    That is not vq's bug, but it is vq's blast radius, and it is invisible
    exactly while everything works. An absolute or relative argv[0] is not
    this case: it names a path rather than relying on the search.
    """
    if os.sep in argv0 or (os.altsep and os.altsep in argv0):
        return None
    inside = shutil.which(argv0, path=venv_bin)
    if inside is None:
        return None
    if system_path and shutil.which(argv0, path=system_path) is not None:
        return None
    return inside


def _is_orca_binary(path: Path) -> bool:
    """Best-effort detection for an ORCA frontend binary.

    ORCA serial runs can succeed while parallel ``%pal nprocs N`` runs fail at
    startup if ``orca_startup_mpi`` cannot load libmpi. The binary registry has
    no richer program type, so key off the conventional frontend name and
    sibling MPI launcher.
    """
    return path.name == "orca" and (path.parent / "orca_startup_mpi").exists()


def _orca_mpi_availability(path: Path) -> tuple[bool, str]:
    """Return whether ORCA's MPI startup binary can be loaded.

    Running ``orca_startup_mpi`` with no input is intentionally cheap: a healthy
    launcher reaches ORCA's own "no input files" fatal error, while a broken MPI
    runtime fails earlier in the dynamic loader, for example with
    "Library not loaded: libmpi.40.dylib" on macOS or "error while loading
    shared libraries: libmpi.so.40" on Linux.
    """
    launcher = path.parent / "orca_startup_mpi"
    if not launcher.is_file():
        return True, "ORCA MPI startup not present; serial executable only"
    if not os.access(launcher, os.X_OK):
        return False, f"ORCA MPI startup not executable: {launcher}"
    try:
        proc = subprocess.run(
            [str(launcher)],
            capture_output=True,
            text=True,
            timeout=10.0,
            stdin=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        return False, f"ORCA MPI startup probe timed out: {launcher}"
    except OSError as exc:
        return False, f"ORCA MPI startup probe failed: {exc}"
    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part)
    if _orca_mpi_loader_failure(output):
        detail = _first_nonempty_line(output) or f"exit {proc.returncode}"
        return False, f"ORCA MPI startup cannot load runtime: {detail}"
    return True, "ORCA MPI startup loads"


def _orca_mpi_loader_failure(output: str) -> bool:
    lower = output.lower()
    return (
        "library not loaded" in lower
        or "dyld[" in lower
        or "error while loading shared libraries" in lower
        or "cannot open shared object file" in lower
    )


_CRYSTAL_SERIAL_FRONTENDS = frozenset(
    {"crystal", "crystal23", "crystal23demo"}
)
# CRYSTAL v1.0.1 emits two spaces between the second marker and ``END``.
# Keep the byte-measured layout: this signature is an identity gate.
_CRYSTAL_NO_INPUT_SIGNATURE = "ERROR **** INPUT ****  END OF DATA IN INPUT DECK"


def _is_crystal_serial_binary(path: Path) -> bool:
    """Return whether *path* names a serial CRYSTAL frontend.

    Parallel CRYSTAL launchers need scheduler/MPI context and cannot be
    safely exercised by ``vq programs``. The serial frontends accept an empty
    stdin as a bounded loader/startup probe and fail before chemistry.
    """
    return path.name.casefold() in _CRYSTAL_SERIAL_FRONTENDS


def _crystal_loader_failure_detail(output: str) -> str | None:
    """Extract the useful soname detail from a dynamic-loader failure."""
    for line in output.splitlines():
        stripped = line.strip()
        lower = stripped.lower()
        marker = "error while loading shared libraries:"
        if marker in lower:
            offset = lower.index(marker) + len(marker)
            return stripped[offset:].strip() or stripped
        if (
            "library not loaded" in lower
            or "cannot open shared object file" in lower
            or "dyld[" in lower
        ):
            return stripped
    return None


def _crystal_serial_availability(path: Path) -> tuple[bool, str]:
    """Prove that a serial CRYSTAL frontend reaches program startup.

    The probe runs with closed stdin in an empty temporary directory so a
    healthy frontend takes its ordinary no-input error without touching the
    caller's workspace. Exit 126/127, loader diagnostics, inability to start,
    timeout, nonzero/signal exit, and output without CRYSTAL's measured
    no-input diagnostic are unavailable states. A chemistry calculation is
    deliberately not run.
    """
    executable = path.resolve()
    try:
        with tempfile.TemporaryDirectory(prefix="vq-crystal-probe-") as cwd:
            proc = subprocess.run(
                [str(executable)],
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=10.0,
                stdin=subprocess.DEVNULL,
            )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(
            part
            for part in (
                _timeout_stream_text(exc.output),
                _timeout_stream_text(exc.stderr),
            )
            if part
        )
        detail = _first_nonempty_line(output)
        suffix = f": {detail}" if detail else ""
        return False, (
            f"CRYSTAL startup probe timed out: {executable}{suffix}"
        )
    except OSError as exc:
        return False, f"CRYSTAL startup probe failed: {exc}"

    output = "\n".join(part for part in (proc.stdout, proc.stderr) if part)
    loader_detail = _crystal_loader_failure_detail(output)
    if loader_detail is not None:
        return False, f"CRYSTAL startup cannot load runtime: {loader_detail}"
    if proc.returncode in {126, 127}:
        detail = _first_nonempty_line(output)
        suffix = f": {detail}" if detail else ""
        return False, f"CRYSTAL startup failed (exit {proc.returncode}){suffix}"
    if proc.returncode < 0:
        detail = _first_nonempty_line(output)
        suffix = f": {detail}" if detail else ""
        return False, (
            f"CRYSTAL startup terminated by signal {-proc.returncode}{suffix}"
        )
    if proc.returncode != 0:
        detail = _first_nonempty_line(output)
        suffix = f": {detail}" if detail else ""
        return False, f"CRYSTAL startup failed (exit {proc.returncode}){suffix}"
    if _CRYSTAL_NO_INPUT_SIGNATURE not in output:
        detail = _first_nonempty_line(output)
        suffix = f": {detail}" if detail else ""
        return False, f"CRYSTAL no-input diagnostic missing{suffix}"
    return True, "CRYSTAL startup loads"


def _first_nonempty_line(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None
