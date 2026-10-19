"""Product-owned native-install qualification before an immutable slot build.

Queue authenticates paths, generations, copies and observations. The tracked
adapter owns build-input semantics and native/data probe isolation. This is not
a general build recipe, compiler cache, or permission to qualify old installs.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import signal
import stat
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Annotated, Any, Literal, NoReturn

from pydantic import BaseModel, ConfigDict, Field

from vq import runtime_slots

RECEIPT = ".vq-native-seed.json"
_MAX_OUTPUT = 1024 * 1024
_MAX_FILES = 4096
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$", strict=True)]


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class BuildInputs(_Record):
    sources: Digest
    patches: Digest
    recipe: Digest
    toolchain: Digest
    options: Digest
    abi: Digest


class FileIdentity(_Record):
    sha256: Digest
    mode: Literal[0o444, 0o644, 0o555, 0o755]


class SeedPlan(_Record):
    schema_: Literal["vq.native-seed-plan/1"] = Field(alias="schema")
    decision: Literal["qualified"]
    receipt: str
    receipt_sha256: Digest
    donor_inputs: BuildInputs
    candidate_inputs: BuildInputs
    installs: dict[str, dict[str, FileIdentity]]


class Observation(_Record):
    path: str
    sha256: Digest


class Verification(_Record):
    schema_: Literal["vq.native-seed-verification/1"] = Field(alias="schema")
    decision: Literal["qualified"]
    isolation: Literal["denied-donor-prefix"]
    operation: Annotated[str, Field(min_length=1, max_length=4096)]
    result: Any
    libraries: list[Observation]
    data: list[Observation]


def _fail(message: str) -> NoReturn:
    raise runtime_slots.RuntimeSlotError(f"native reuse held: {message}")


def _relative(value: str, venv: str) -> Path:
    path = Path(value)
    if (
        not value or value == "." or path.is_absolute() or path.as_posix() != value
        or any(part in {".", "..", ".git", venv, "__pycache__"} for part in path.parts)
        or any(c in value for c in ("\\", "\x00", "\n", "\r"))
        or path.name in {"pyvenv.cfg", "activate", runtime_slots.IMMUTABLE_RUNTIME_MARKER}
    ):
        _fail("install paths must be canonical, source-relative and outside the venv")
    return path


def _inside(root: Path, relative: Path) -> Path:
    path = root
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            _fail("symlinks are unsupported in native seed paths")
    if not path.resolve().is_relative_to(root.resolve()):
        _fail("native seed path escapes its source")
    return path


def _digest(path: Path) -> str:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        _fail("native seed inputs must be regular files without hardlinks")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _identity(path: Path) -> FileIdentity:
    return FileIdentity.model_validate({
        "sha256": _digest(path), "mode": stat.S_IMODE(path.stat().st_mode),
    })


def _adapter_call(
    adapter: Path, request: dict[str, Any], *, timeout: int, pass_fds: tuple[int, ...],
) -> dict[str, Any]:
    # File-backed input avoids pipe deadlocks; output is capped while the
    # process runs. Inherited physical locks survive a killed controller.
    env = {k: v for k, v in os.environ.items() if not k.startswith((
        "PYTHON", "LD_", "DYLD_", "VIBE_TOOLSET_ADMIN_LOCK_", "VIBE_TOOLSET_INHERITED_",
    ))}
    with (
        tempfile.TemporaryFile() as inp, tempfile.TemporaryFile() as out,
        tempfile.TemporaryFile() as err,
    ):
        inp.write(json.dumps(request, allow_nan=False).encode())
        inp.seek(0)
        child = subprocess.Popen(
            [str(adapter)], cwd=request["candidate"]["source"], env=env,
            stdin=inp, stdout=out, stderr=err, start_new_session=True, pass_fds=pass_fds,
        )
        deadline = time.monotonic() + timeout
        try:
            while child.poll() is None:
                if out.tell() > _MAX_OUTPUT or err.tell() > _MAX_OUTPUT:
                    _fail("adapter output exceeds 1 MiB")
                if time.monotonic() >= deadline:
                    _fail("adapter timed out")
                time.sleep(0.02)
            if child.returncode != 0:
                _fail(f"adapter {request['phase']} exited {child.returncode}")
            out.seek(0)
            raw = out.read(_MAX_OUTPUT + 1)
            if len(raw) > _MAX_OUTPUT or err.tell() > _MAX_OUTPUT:
                _fail("adapter output exceeds 1 MiB")
            result = json.loads(raw, parse_constant=lambda value: _fail(f"invalid JSON {value}"))
            if not isinstance(result, dict):
                _fail("adapter response must be an object")
            if result.get("decision") == "hold":
                _fail(str(result.get("reason", "product qualification unavailable"))[:4096])
            return result
        finally:
            # Also reap descendants after a successful adapter: it may not
            # leave a process writing after the phase or outliving its locks.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(child.pid, signal.SIGKILL)
            child.wait()


def check_installs(source: Path, plan: SeedPlan, venv: str) -> None:
    """Require complete manifests, not just selected library names."""
    for name, files in plan.installs.items():
        root = _inside(source, _relative(name, venv))
        actual: dict[str, FileIdentity] = {}
        for path in root.rglob("*"):
            if path.is_symlink():
                _fail("symlinks are unsupported in native installs")
            if not path.is_dir():
                relative = path.relative_to(root).as_posix()
                _relative(relative, venv)
                actual[relative] = _identity(path)
        if actual != files:
            _fail(f"native install manifest mismatch: {name}")


def prepare(
    root: str, sha: str, donor_sha: str, transaction: str, adapter_name: str,
    *, timeout: int, pass_fds: tuple[int, ...], record: dict[str, Any],
) -> SeedPlan:
    """Called with the store, donor and candidate physical lifecycle locks held."""
    paths = runtime_slots.layout(root)
    source = runtime_slots.slot_source(root, sha)
    donor = runtime_slots.slot_source(root, donor_sha)
    venv = runtime_slots._slot_venv_subdir(root, sha)
    if runtime_slots._slot_venv_subdir(root, donor_sha) != venv:
        _fail("donor and candidate virtualenv layouts differ")
    runtime_slots._assert_generation(paths, donor_sha, verify_content=True)
    donor_state = runtime_slots._read_verified_state(
        runtime_slots.slot_path(root, donor_sha) / runtime_slots.SLOT_STATE_MARKER, donor_sha,
    )
    state = runtime_slots._read_exact_marker(
        runtime_slots.slot_path(root, sha) / runtime_slots.SLOT_STATE_MARKER,
        expected_kind="vq-runtime-slot-build", expected_id=sha, expected_states={"building"},
    )
    if state["transaction"] != transaction or donor_sha == sha:
        _fail("candidate is not this unpublished transaction")
    adapter = _inside(source, _relative(adapter_name, venv))
    adapter_hash = _digest(adapter)
    tracked = subprocess.run(
        ["git", "-C", str(source), "show", f"{sha}:{adapter_name}"],
        capture_output=True, check=True, timeout=30, stdin=subprocess.DEVNULL,
    ).stdout
    if hashlib.sha256(tracked).hexdigest() != adapter_hash or not os.access(adapter, os.X_OK):
        _fail("adapter must be the executable tracked at the candidate commit")
    request: dict[str, Any] = {
        "schema": "vq.native-seed-request/1", "phase": "plan", "transaction": transaction,
        "donor": {"sha": donor_sha, "source": str(donor),
                  "source_content_sha256": donor_state["source_content_sha256"],
                  "venv_content_sha256": donor_state["content_sha256"]},
        "candidate": {"sha": sha, "source": str(source)},
    }
    record.update(request=request, adapter={"path": adapter_name, "sha256": adapter_hash})
    before = runtime_slots.source_content_sha256(root, sha)
    plan = SeedPlan.model_validate(
        _adapter_call(adapter, request, timeout=timeout, pass_fds=pass_fds),
    )
    record["plan"] = plan.model_dump(by_alias=True)
    if runtime_slots.source_content_sha256(root, sha) != before:
        _fail("plan phase changed the candidate")
    runtime_slots._assert_generation(paths, donor_sha, verify_content=True)
    if plan.donor_inputs != plan.candidate_inputs:
        _fail("candidate build inputs differ from the donor receipt")
    receipt = _inside(donor, _relative(plan.receipt, venv))
    if _digest(receipt) != plan.receipt_sha256:
        _fail("donor build receipt digest differs")
    if not plan.installs or not 0 < sum(map(len, plan.installs.values())) <= _MAX_FILES:
        _fail("require 1..4096 native install files")
    prefixes = [_relative(name, venv) for name in plan.installs]
    for name, files in plan.installs.items():
        prefix = _relative(name, venv)
        if not files or any(prefix != other and prefix.is_relative_to(other) for other in prefixes):
            _fail("install prefixes must be nonempty and disjoint")
        destination = _inside(source, prefix)
        if destination.exists():
            _fail("native install destination already exists")
        for relative in files:
            _relative(relative, venv)
    check_installs(donor, plan, venv)
    # All input, receipt, manifest and destination checks precede the first copy.
    for name, files in plan.installs.items():
        for relative, identity in files.items():
            destination = _inside(source, Path(name) / relative)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with (donor / name / relative).open("rb") as inp, destination.open("xb") as out:
                while block := inp.read(1024 * 1024):
                    out.write(block)
            destination.chmod(identity.mode)
    check_installs(source, plan, venv)
    request.update(phase="verify", plan=plan.model_dump(by_alias=True))
    before_probe = runtime_slots.source_content_sha256(root, sha)
    if _digest(adapter) != adapter_hash:
        _fail("adapter changed between phases")
    observed = Verification.model_validate(
        _adapter_call(adapter, request, timeout=timeout, pass_fds=pass_fds),
    )
    record["verification"] = observed.model_dump(by_alias=True)
    expected = {
        str(source / name / relative): identity.sha256
        for name, files in plan.installs.items() for relative, identity in files.items()
    }
    if not observed.libraries or not observed.data:
        _fail("native and data observations are both required")
    for observation in [*observed.libraries, *observed.data]:
        if expected.get(observation.path) != observation.sha256:
            _fail("observed library/data is not an exact candidate install file")
    if runtime_slots.source_content_sha256(root, sha) != before_probe:
        _fail("verification changed the candidate")
    runtime_slots._assert_generation(paths, donor_sha, verify_content=True)
    if runtime_slots.resolve_current(root) != donor_sha:
        _fail("current generation changed during seeding")
    return plan
