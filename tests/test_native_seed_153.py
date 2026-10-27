"""Required native reuse crosses the actual slot/installer boundary (#153)."""
from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pydantic
import pytest

from vq import admin, config, native_seed, runtime_slots

pytestmark = pytest.mark.no_autopatch_lifecycle_lock


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(repo), "-c", "user.name=Seed Test",
         "-c", "user.email=test@example.invalid", "-c", "commit.gpgsign=false", *args],
        text=True, stderr=subprocess.PIPE,
    ).strip()


_IDENTITY = '''import hashlib, json, platform, shutil
from pathlib import Path
def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def inputs(source):
    source = Path(source)
    return dict(sources=digest(source / "lookup.c"), patches=digest(source / "patches.txt"),
                recipe=digest(source / "build.py"), toolchain=digest(shutil.which("cc")),
                options=hashlib.sha256(b"-fPIC -dynamiclib").hexdigest(),
                abi=hashlib.sha256(platform.platform().encode()).hexdigest())
'''

_BUILDER = '''import json, os, subprocess, sys
from pathlib import Path
from identity import inputs, digest
root = Path.cwd()
install = root / "deps/tiny/install"
library = install / "liblookup.dylib"
with Path(os.environ["SEED_TEST_INSTALL_LOG"]).open("a") as log:
    log.write(str(root) + "\\n")
if not library.exists():
    if os.environ.get("SEED_TEST_NO_COMPILE"):
        raise SystemExit("second generation attempted compilation")
    install.mkdir(parents=True)
    subprocess.run(["cc", "-fPIC", "-dynamiclib", str(root / "lookup.c"),
                    "-o", str(library)], check=True)
    with Path(os.environ["SEED_TEST_COMPILE_LOG"]).open("a") as log:
        log.write("compile\\n")
    (install / "data.txt").write_text("42\\n")
    (install / "receipt.json").write_text(json.dumps(dict(inputs=inputs(root),
        outputs={p.name: digest(p) for p in install.iterdir()})))
if os.environ.get("SEED_TEST_INSTALL_MUTATION"):
    (install / "data.txt").write_text("99\\n")
python = root / ".venv/bin/python"
python.parent.mkdir(parents=True, exist_ok=True)
python.symlink_to(sys.executable)
'''

_ADAPTER = '''import ctypes, hashlib, json, os, subprocess, sys, time
from pathlib import Path
from identity import inputs, digest
if len(sys.argv) > 1:
    install, origin = Path(sys.argv[1]), Path(sys.argv[2])
    library = install / "liblookup.dylib"
    data = install / "data.txt"
    native = ctypes.CDLL(str(library))
    native.lookup.argtypes = [ctypes.c_char_p]
    native.lookup.restype = ctypes.c_int
    # This is a native fopen, not a reported Python-only path check.
    assert native.lookup(os.fsencode(origin / "deps/tiny/install/data.txt")) == -1
    assert native.lookup(os.fsencode(data)) == 42
    class Info(ctypes.Structure):
        _fields_ = [("name", ctypes.c_char_p), ("base", ctypes.c_void_p),
                    ("symbol", ctypes.c_char_p), ("address", ctypes.c_void_p)]
    info = Info()
    loader = ctypes.CDLL(None)
    loader.dladdr.argtypes = [ctypes.c_void_p, ctypes.POINTER(Info)]
    assert loader.dladdr(ctypes.cast(native.lookup, ctypes.c_void_p), ctypes.byref(info))
    loaded = Path(os.fsdecode(info.name)).resolve()
    assert loaded == library
    print(json.dumps(dict(schema="vq.native-seed-verification/1", decision="qualified",
        isolation="denied-donor-prefix", operation="native lookup", result=42,
        libraries=[dict(path=str(loaded), sha256=digest(loaded))],
        data=[dict(path=str(data), sha256=digest(data))])))
    raise SystemExit
request = json.load(sys.stdin)
for fd in request.get("test_lock_fds", []):
    os.fstat(fd)
source, donor = Path(request["candidate"]["source"]), Path(request["donor"]["source"])
mode = os.environ.get("SEED_TEST_ADAPTER_MODE")
if mode == "timeout":
    time.sleep(30)
if mode == "overflow":
    print("x" * (1024 * 1024 + 1))
    raise SystemExit
if mode == "hold":
    print(json.dumps(dict(decision="hold", reason="qualification missing")))
    raise SystemExit
if request["phase"] == "plan":
    install = donor / "deps/tiny/install"
    receipt = install / "receipt.json"
    prior = json.loads(receipt.read_text())
    files = {p.name: dict(sha256=digest(p), mode=p.stat().st_mode & 0o777)
             for p in install.iterdir()}
    assert prior["outputs"] == {k: v["sha256"] for k,v in files.items() if k != "receipt.json"}
    plan = dict(schema="vq.native-seed-plan/1", decision="qualified",
        receipt="deps/tiny/install/receipt.json", receipt_sha256=digest(receipt),
        donor_inputs=prior["inputs"], candidate_inputs=inputs(source),
        installs={"deps/tiny/install": files})
    if mode == "mismatch":
        plan["candidate_inputs"]["recipe"] = "f" * 64
    if mode == "receipt":
        plan["receipt_sha256"] = "f" * 64
    if mode == "escape":
        plan["installs"] = {"../escape": files}
    if mode == "missing-file":
        del files["data.txt"]
    if mode == "plan-write":
        (source / "unexpected.txt").write_text("not read only")
    print(json.dumps(plan))
else:
    profile = '(version 1)(allow default)(deny file-read* (subpath ' + json.dumps(str(donor)) + '))'
    profile += ('(deny file-write* (subpath ' + json.dumps(str(donor))
                + ')(subpath ' + json.dumps(str(source)) + '))')
    result = subprocess.check_output(["/usr/bin/sandbox-exec", "-p", profile,
        sys.executable, "-B", __file__, str(source / "deps/tiny/install"), str(donor)], text=True)
    value = json.loads(result)
    if mode == "wrong-image":
        value["libraries"][0]["path"] = str(donor / "deps/tiny/install/liblookup.dylib")
    print(json.dumps(value))
'''


@pytest.mark.skipif(
    sys.platform != "darwin" or shutil.which("cc") is None,
    reason="this fixture's product adapter uses macOS native donor-read denial",
)
def test_two_generations_reuse_native_files_and_hold_before_install(tmp_path, monkeypatch):
    repo = tmp_path / "product"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / ".gitignore").write_text(".venv/\ndeps/\n__pycache__/\n")
    (repo / "lookup.c").write_text(
        '#include <stdio.h>\nint lookup(const char *path) {\n'
        'FILE *f = fopen(path, "r"); int v = -1;\n'
        'if (f) { if (fscanf(f, "%d", &v) != 1) v = -1; fclose(f); } return v; }\n'
    )
    (repo / "patches.txt").write_text("")
    (repo / "identity.py").write_text(_IDENTITY)
    (repo / "build.py").write_text(_BUILDER)
    (repo / "seed").write_text(f"#!{sys.executable}\n" + _ADAPTER)
    (repo / "seed").chmod(0o755)
    (repo / "build.sh").write_text(
        "#!/bin/sh\nset -eu\n" + shlex.quote(sys.executable) + " -B build.py\n",
    )
    _git(repo, "add", ".gitignore", "lookup.c", "patches.txt", "identity.py",
         "build.py", "seed", "build.sh")
    _git(repo, "commit", "-qm", "first source")
    first = _git(repo, "rev-parse", "HEAD")
    root = tmp_path / "runtimes"
    prog = config.VenvProgram(
        kind="venv", python=str(repo / ".venv/bin/python"), git_dir=str(repo), branch="main",
        install_script="build.sh", update_script="build.sh", runtime_slot_root=str(root),
    )
    install_log, compile_log = tmp_path / "installs", tmp_path / "compiles"
    monkeypatch.setenv("SEED_TEST_INSTALL_LOG", str(install_log))
    monkeypatch.setenv("SEED_TEST_COMPILE_LOG", str(compile_log))
    # Own fixture job census only; no daemon, scheduler or external state.
    monkeypatch.setattr(admin, "_runtime_slot_in_use_snapshot", lambda _root: set())
    no_donor = admin._do_slot_update_work(
        "demo", prog.model_copy(update={"runtime_slot_seed_adapter": "seed"}),
        expected_sha=first,
    )
    assert not no_donor.success
    assert any("no retained current donor" in e for e in no_donor.work_errors)
    assert not install_log.exists() and not compile_log.exists()
    built = admin._do_slot_update_work("demo", prog, expected_sha=first)
    assert built.success, built.work_errors
    donor = runtime_slots.slot_source(root, first)
    before = runtime_slots.source_content_sha256(root, first)
    (repo / "second.txt").write_text("unrelated product change\n")
    _git(repo, "add", "second.txt")
    _git(repo, "commit", "-qm", "second source")
    second = _git(repo, "rev-parse", "HEAD")
    seeded = prog.model_copy(update={"runtime_slot_seed_adapter": "seed",
                                     "runtime_slot_seed_timeout_seconds": 10})
    monkeypatch.setenv("SEED_TEST_NO_COMPILE", "1")
    source = runtime_slots.slot_source(root, second)
    original_call = native_seed._adapter_call

    def call_under_locks(adapter, request, *, timeout, pass_fds):
        held = set(admin._active_toolset_lifecycle_resources())
        current_donor = Path(request["donor"]["source"])
        candidate = Path(request["candidate"]["source"])
        for scope, path in [("checkout", root), ("checkout", current_donor),
                            ("target", current_donor / ".venv"),
                            ("checkout", candidate), ("target", candidate / ".venv")]:
            assert (scope, str(path)) in held
        assert len(pass_fds) >= 5
        return original_call(adapter, {**request, "test_lock_fds": list(pass_fds)},
                             timeout=timeout, pass_fds=pass_fds)

    monkeypatch.setattr(native_seed, "_adapter_call", call_under_locks)

    for mode, reason in [
        ("mismatch", "build inputs differ"), ("receipt", "receipt digest differs"),
        ("escape", "source-relative"),
        ("missing-file", "manifest mismatch"), ("hold", "qualification missing"),
        ("plan-write", "changed the candidate"), ("overflow", "exceeds 1 MiB"),
    ]:
        monkeypatch.setenv("SEED_TEST_ADAPTER_MODE", mode)
        result = admin._do_slot_update_work("demo", seeded, expected_sha=second)
        assert not result.success
        assert any(reason in e for e in result.work_errors), result.work_errors
        assert not (source / "deps/tiny/install").exists()
        assert install_log.read_text().splitlines() == [str(donor)]
        assert runtime_slots.resolve_current(root) == first
        assert runtime_slots.source_content_sha256(root, first) == before
        receipt = json.loads((source.parent / native_seed.RECEIPT).read_text())
        assert receipt["decision"] == "hold" and receipt["candidate_sha"] == second

    monkeypatch.setenv("SEED_TEST_ADAPTER_MODE", "wrong-image")
    refused = admin._do_slot_update_work("demo", seeded, expected_sha=second)
    assert not refused.success, refused
    assert any("exact candidate install" in e for e in refused.work_errors), refused.work_errors
    assert install_log.read_text().splitlines() == [str(donor)]

    monkeypatch.delenv("SEED_TEST_ADAPTER_MODE")
    result = admin._do_slot_update_work("demo", seeded, expected_sha=second)
    assert result.success, result.work_errors
    assert compile_log.read_text().splitlines() == ["compile"]
    assert install_log.read_text().splitlines() == [str(donor), str(source)]
    assert runtime_slots.resolve_current(root) == second
    assert runtime_slots.source_content_sha256(root, first) == before
    receipt = json.loads((source.parent / native_seed.RECEIPT).read_text())
    assert receipt["decision"] == "qualified"
    assert receipt["verification"]["result"] == 42
    assert receipt["verification"]["libraries"][0]["path"].startswith(str(source) + "/")
    original = donor / "deps/tiny/install/liblookup.dylib"
    copied = source / "deps/tiny/install/liblookup.dylib"
    assert original.read_bytes() == copied.read_bytes()
    assert original.stat().st_ino != copied.stat().st_ino
    assert result.metrics["native_seed"] == "qualified"
    # An already sealed exact generation needs neither adapter nor installer.
    monkeypatch.setenv("SEED_TEST_ADAPTER_MODE", "hold")
    again = admin._do_slot_update_work("demo", seeded, expected_sha=second)
    assert again.success, again.work_errors
    assert len(install_log.read_text().splitlines()) == 2
    # The selected program remains serializable with the new opt-in cleared.
    selected = runtime_slots.slot_local_program(seeded, root, second)
    assert config.VenvProgram.model_validate(selected.model_dump()) == selected

    # An installer that alters the seed cannot activate its generation.
    monkeypatch.delenv("SEED_TEST_ADAPTER_MODE")
    (repo / "third.txt").write_text("another source change\n")
    _git(repo, "add", "third.txt")
    _git(repo, "commit", "-qm", "third source")
    third = _git(repo, "rev-parse", "HEAD")
    second_before = runtime_slots.source_content_sha256(root, second)
    monkeypatch.setenv("SEED_TEST_INSTALL_MUTATION", "1")
    altered = admin._do_slot_update_work("demo", seeded, expected_sha=third)
    assert not altered.success and any("manifest mismatch" in e for e in altered.work_errors)
    assert runtime_slots.resolve_current(root) == second
    assert runtime_slots.source_content_sha256(root, second) == second_before
    assert compile_log.read_text().splitlines() == ["compile"]

    # Broken donor content is refused before its adapter or installer is used.
    monkeypatch.delenv("SEED_TEST_INSTALL_MUTATION")
    copied.write_bytes(b"changed donor")
    corrupted = admin._do_slot_update_work("demo", seeded, expected_sha=third)
    assert not corrupted.success and any("content changed" in e for e in corrupted.work_errors)
    assert len(install_log.read_text().splitlines()) == 3
    assert not (runtime_slots.slot_source(root, third) / "deps/tiny/install").exists()


@pytest.mark.parametrize(
    "adapter", ["", ".", "../seed", "/external/seed", "a/../seed", "a//seed", "a\\seed"],
)
def test_seed_config_rejects_ambiguous_adapter_paths(tmp_path, adapter):
    with pytest.raises(pydantic.ValidationError):
        config.VenvProgram(
            kind="venv", python=str(tmp_path / ".venv/bin/python"), git_dir=str(tmp_path),
            runtime_slot_root=str(tmp_path / "rt"), runtime_slot_seed_adapter=adapter,
        )


def test_seed_requires_slot_configuration(tmp_path):
    with pytest.raises(pydantic.ValidationError, match="requires runtime_slot_root"):
        config.VenvProgram(kind="venv", python=str(tmp_path / ".venv/bin/python"),
                           git_dir=str(tmp_path), runtime_slot_seed_adapter="seed")


def test_adapter_timeout_reaps_its_process(tmp_path):
    adapter = tmp_path / "adapter"
    pidfile = tmp_path / "pid"
    adapter.write_text(f"#!{sys.executable}\nimport os,time\n"
                       f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep(30)\n")
    adapter.chmod(0o755)
    with pytest.raises(runtime_slots.RuntimeSlotError, match="timed out"):
        native_seed._adapter_call(
            adapter, {"candidate": {"source": str(tmp_path)}, "phase": "plan"},
            timeout=1, pass_fds=(),
        )
    with pytest.raises(ProcessLookupError):
        os.kill(int(pidfile.read_text()), 0)


def _alias_plan(files):
    inputs = dict.fromkeys(
        ('sources', 'patches', 'recipe', 'toolchain', 'options', 'abi'), 'a' * 64,
    )
    return native_seed.SeedPlan.model_validate({
        'schema': 'vq.native-seed-plan/1', 'decision': 'qualified',
        'receipt': 'receipt.json', 'receipt_sha256': 'b' * 64,
        'donor_inputs': inputs, 'candidate_inputs': inputs,
        'installs': {'deps/install': files},
    })


def test_native_sibling_alias_copy_preserves_bytes_and_targets(tmp_path):
    donor, candidate = tmp_path / 'donor', tmp_path / 'candidate'
    install = donor / 'deps/install'
    install.mkdir(parents=True)
    library = install / 'lib.so.3'
    library.write_bytes(b'unit fixture, not native acceptance')
    library.chmod(0o644)
    (install / 'lib.so.1').symlink_to('lib.so.3')
    (install / 'lib.so').symlink_to('lib.so.1')
    files = {'lib.so.3': native_seed._identity(library).model_dump(),
             'lib.so.1': {'symlink': 'lib.so.3'}, 'lib.so': {'symlink': 'lib.so.1'}}
    plan = _alias_plan(files)
    native_seed.copy_installs(donor, candidate, plan, '.venv')
    assert (candidate / 'deps/install/lib.so').read_bytes() == library.read_bytes()
    assert os.readlink(candidate / 'deps/install/lib.so') == 'lib.so.1'
    (install / 'lib.so').unlink()
    (install / 'lib.so').symlink_to('lib.so.3')
    with pytest.raises(runtime_slots.RuntimeSlotError, match='manifest mismatch'):
        native_seed.check_installs(donor, plan, '.venv')


@pytest.mark.parametrize('files', [
    {'link': {'symlink': '../outside'}},
    {'link': {'symlink': '/outside'}},
    {'link': {'symlink': 'sub/file'}},
    {'link': {'symlink': 'missing'}},
    {'link': {'symlink': 'other'}, 'other': {'symlink': 'link'}},
    {'link': {'symlink': 'other'}, 'other': {'sha256': 'a' * 64, 'mode': 0o644},
     'link/child': {'sha256': 'b' * 64, 'mode': 0o644}},
])
def test_native_unsafe_aliases_refused_before_copy(tmp_path, files):
    source = tmp_path / 'donor'
    (source / 'deps/install').mkdir(parents=True)
    destination = tmp_path / 'candidate'
    with pytest.raises(runtime_slots.RuntimeSlotError):
        native_seed.copy_installs(source, destination, _alias_plan(files), '.venv')
    assert not destination.exists()
