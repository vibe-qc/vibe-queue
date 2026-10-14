"""Build once, relocate twice; trust/compatibility/refusal/recovery contracts."""
from __future__ import annotations

import io
import json
import platform
import shutil
import subprocess
import tarfile
import threading
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import admin
from vq import runtime_bundle as rb
from vq.cli import main

pytestmark = pytest.mark.no_autopatch_lifecycle_lock


def _profile() -> rb.TargetProfile:
    return rb.TargetProfile(
        os=platform.system(), arch=platform.machine(), isa=[],
        abi={"libc": "fixture-system", "python": "none", "blas": "none",
             "mpi": "none", "launcher": "none"},
    )


def _description(tmp_path: Path, dependency: str = "b") -> Path:
    description = rb.BuildDescription(
        program="fixture", source_revision="a" * 40, recipe_sha256="c" * 64,
        dependency_sha256=dependency * 64, target=_profile(), variant="generic",
        compiler_flags=[], toolchain="fixture-cc", publisher="test-builder",
        build_receipt="fixture-build/1", qualification_receipt="fixture-smoke/1",
        smoke_argv=["bin/probe"],
    )
    path = tmp_path / f"description-{dependency}.json"
    path.write_text(description.model_dump_json())
    return path


def _trust(tmp_path: Path, *manifests: rb.BundleManifest) -> Path:
    path = tmp_path / "operator-trust.json"
    path.write_text(rb.TrustPolicy(publishers={
        "test-builder": [m.artifact_id for m in manifests],
    }).model_dump_json())
    return path


@pytest.fixture
def bundle(tmp_path: Path) -> tuple[Path, rb.BundleManifest, Path]:
    payload = tmp_path / "built"
    (payload / "bin").mkdir(parents=True)
    (payload / "bin/probe").write_text("#!/bin/sh\ntest -f data/value\n")
    (payload / "bin/probe").chmod(0o755)
    (payload / "data").mkdir()
    (payload / "data/value").write_text("42\n")
    output = tmp_path / "bundle"
    manifest = rb.pack(payload, _description(tmp_path), output)
    return output, manifest, _trust(tmp_path, manifest)


def test_compiled_library_and_data_relocate_twice_without_rebuild(
    tmp_path: Path, monkeypatch,
) -> None:
    cc = shutil.which("cc")
    if cc is None:
        pytest.skip("a C compiler is required for the bounded relocation fixture")
    source = tmp_path / "source"
    source.mkdir()
    payload = tmp_path / "build"
    for name in ("bin", "lib", "data"):
        (payload / name).mkdir(parents=True)
    library = source / "answer.c"
    library.write_text("int answer(void) { return 42; }\n")
    program = source / "main.c"
    program.write_text(
        '#include <stdio.h>\nextern int answer(void);\n'
        'int main(void) { int value=0; FILE *f=fopen("data/value", "r");\n'
        'if (!f) return 2; int n=fscanf(f,"%d", &value); fclose(f);\n'
        'return n != 1 || value != answer(); }\n'
    )
    darwin = platform.system() == "Darwin"
    lib = payload / "lib" / ("libanswer.dylib" if darwin else "libanswer.so")
    lib_flags = (["-dynamiclib", "-Wl,-install_name,@rpath/libanswer.dylib"]
                 if darwin else ["-shared"])
    subprocess.run([cc, "-fPIC", *lib_flags, str(library), "-o", str(lib)], check=True)
    rpath = "@loader_path/../lib" if darwin else "$ORIGIN/../lib"
    subprocess.run([cc, str(program), "-L" + str(payload / "lib"), "-lanswer",
                    "-Wl,-rpath," + rpath, "-o", str(payload / "bin/probe")], check=True)
    (payload / "data/value").write_text("42\n")
    output = tmp_path / "bundle"
    manifest = rb.pack(payload, _description(tmp_path), output)
    trust = _trust(tmp_path, manifest)
    store = tmp_path / "store"
    rb.publish(output, store, trust)
    # Source and build prefix no longer exist during either target operation.
    shutil.rmtree(source)
    shutil.rmtree(payload)
    launched = []
    real_popen = subprocess.Popen

    def observe(argv, **kwargs):
        launched.append(argv)
        assert len(kwargs["pass_fds"]) == 2  # Target/store locks survive controller exit.
        return real_popen(argv, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", observe)
    for name in ("target-one", "target-two"):
        receipt = rb.deploy(store, manifest.artifact_id, tmp_path / name, trust, _profile())
        assert receipt["verified"]
        assert receipt["target_builds"] == 0
        assert receipt["artifact_id"] == manifest.artifact_id
        assert Path(receipt["generation"], "lib", lib.name).is_file()
    assert len(launched) == 2
    assert all(len(argv) == 1 and argv[0].endswith("/bin/probe") for argv in launched)


def test_same_source_two_profiles_and_rollback_preserve_old_pin(tmp_path: Path, bundle) -> None:
    output, first, trust = bundle
    other = tmp_path / "other-bundle"
    (tmp_path / "built/data/value").write_text("43\n")
    second = rb.pack(tmp_path / "built", _description(tmp_path, "d"), other)
    assert first.build.source_revision == second.build.source_revision
    assert first.build.build_id != second.build.build_id
    assert first.artifact_id != second.artifact_id
    assert first.archive_sha256 != second.archive_sha256
    trust = _trust(tmp_path, first, second)
    store, target = tmp_path / "store", tmp_path / "target"
    for path, manifest in ((output, first), (other, second)):
        rb.publish(path, store, trust)
        rb.deploy(store, manifest.artifact_id, target, trust, _profile())
    pinned = target / "generations" / first.artifact_id / "payload"
    old_bytes = (pinned / "bin/probe").read_bytes()
    assert (pinned / "data/value").read_text() == "42\n"
    selected_data = target / "generations" / second.artifact_id / "payload/data/value"
    assert selected_data.read_text() == "43\n"
    assert rb._selection(target)["previous"] == first.artifact_id
    # Lost acknowledgement and a missing auxiliary receipt both replay safely.
    (target / "generations" / second.artifact_id / "verification.json").unlink()
    rb.deploy(store, second.artifact_id, target, trust, _profile())
    assert rb._selection(target)["previous"] == first.artifact_id
    rb.deploy(store, first.artifact_id, target, trust, _profile())
    assert rb._selection(target)["current"] == first.artifact_id
    assert (pinned / "bin/probe").read_bytes() == old_bytes
    subprocess.run([str(pinned / "bin/probe")], cwd=pinned, check=True)


def test_plan_is_read_only_and_cli_defaults_to_plan(tmp_path: Path, bundle) -> None:
    output, manifest, trust = bundle
    store, target = tmp_path / "store", tmp_path / "not-created"
    rb.publish(output, store, trust)
    profile = tmp_path / "profile.json"
    profile.write_text(_profile().model_dump_json())
    result = CliRunner().invoke(main, ["runtime-bundle", "deploy", manifest.artifact_id,
        "--store", str(store), "--root", str(target), "--trust", str(trust),
        "--target-profile", str(profile)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["blockers"] == []
    assert not target.exists()


@pytest.mark.parametrize("kind", ["arch", "isa", "abi"])
def test_incompatible_target_refused_before_mutation(tmp_path: Path, bundle, kind: str) -> None:
    output, manifest, trust = bundle
    if kind == "isa":
        data = manifest.model_dump()
        data["build"]["target"]["isa"] = ["fixture-special-isa"]
        manifest = rb.BundleManifest.model_validate(data)
        (output / "manifest.json").write_text(manifest.model_dump_json())
        trust = _trust(tmp_path, manifest)
    target = _profile().model_dump()
    if kind == "arch":
        target["arch"] = "other-arch"
    if kind == "abi":
        target["abi"]["libc"] = "other-libc"
    store, root = tmp_path / "store", tmp_path / "target"
    rb.publish(output, store, trust)
    with pytest.raises(rb.BundleError, match="mismatch|absent"):
        rb.deploy(store, manifest.artifact_id, root, trust, rb.TargetProfile.model_validate(target))
    assert not root.exists()


def test_tampering_and_unauthorized_publisher_never_publish(tmp_path: Path, bundle) -> None:
    output, manifest, trust = bundle
    original = (output / "manifest.json").read_bytes()
    data = manifest.model_dump()
    data["build"]["publisher"] = "untrusted-builder"
    (output / "manifest.json").write_text(json.dumps(data))
    with pytest.raises(rb.BundleError, match="not pinned"):
        rb.publish(output, tmp_path / "store", trust)
    (output / "manifest.json").write_bytes(original)
    with (output / "bundle.tar").open("ab") as handle:
        handle.write(b"modified")
    with pytest.raises(rb.BundleError, match="archive digest"):
        rb.publish(output, tmp_path / "store", trust)
    assert not (tmp_path / "store").exists()


@pytest.mark.parametrize("kind", ["traversal", "symlink", "missing-data", "duplicate"])
def test_authenticated_but_unsafe_archive_is_refused(tmp_path: Path, bundle, kind: str) -> None:
    output, manifest, _ = bundle
    archive = output / "bundle.tar"
    with tarfile.open(archive, "w") as tar:
        if kind == "traversal":
            member = tarfile.TarInfo("../escape")
            tar.addfile(member, io.BytesIO())
        elif kind == "symlink":
            member = tarfile.TarInfo("bin/probe")
            member.type = tarfile.SYMTYPE
            member.linkname = "/bin/sh"
            tar.addfile(member)
        else:
            for name in ("bin/probe", "bin/probe") if kind == "duplicate" else ("bin/probe",):
                data = (tmp_path / "built" / name).read_bytes()
                member = tarfile.TarInfo(name)
                member.size, member.mode = len(data), 0o755
                tar.addfile(member, io.BytesIO(data))
    data = manifest.model_dump()
    data["archive_sha256"] = rb._file_digest(archive)
    changed = rb.BundleManifest.model_validate(data)
    (output / "manifest.json").write_text(changed.model_dump_json())
    trust = _trust(tmp_path, changed)
    with pytest.raises(rb.BundleError):
        rb.publish(output, tmp_path / "store", trust)
    assert not (tmp_path / "store").exists()
    assert not (tmp_path / "escape").exists()


def test_interrupted_transfer_and_failed_smoke_preserve_selection(tmp_path: Path, bundle,
                                                               monkeypatch) -> None:
    output, manifest, trust = bundle
    store, target = tmp_path / "store", tmp_path / "target"
    real_copy = rb.shutil.copyfile

    def interrupted(source, destination):
        Path(destination).write_bytes(b"partial")
        raise OSError("interrupted transfer")

    monkeypatch.setattr(rb.shutil, "copyfile", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        rb.publish(output, store, trust)
    assert not (store / "artifacts" / manifest.artifact_id).exists()
    monkeypatch.setattr(rb.shutil, "copyfile", real_copy)
    rb.publish(output, store, trust)
    rb.deploy(store, manifest.artifact_id, target, trust, _profile())
    selection = (target / "selection.json").read_bytes()
    (tmp_path / "built/bin/probe").write_text("#!/bin/sh\nexit 1\n")
    bad_output = tmp_path / "bad-bundle"
    bad = rb.pack(tmp_path / "built", _description(tmp_path, "d"), bad_output)
    trust = _trust(tmp_path, manifest, bad)
    rb.publish(bad_output, store, trust)
    with pytest.raises(rb.BundleError, match="smoke check failed"):
        rb.deploy(store, bad.artifact_id, target, trust, _profile())
    assert (target / "selection.json").read_bytes() == selection


def test_store_lock_fences_concurrent_publication_and_retry(tmp_path: Path, bundle) -> None:
    output, manifest, trust = bundle
    store = tmp_path / "store"
    holding, release = threading.Event(), threading.Event()
    errors = []

    def holder():
        try:
            with rb._locks(store):
                holding.set()
                assert release.wait(20)
        except BaseException as exc:
            errors.append(exc)
            holding.set()

    thread = threading.Thread(target=holder)
    thread.start()
    try:
        assert holding.wait(20)
        assert not errors
        with pytest.raises(admin.AdminUpdateInProgress):
            rb.publish(output, store, trust)
    finally:
        release.set()
        thread.join(20)
    assert not errors
    assert rb.publish(output, store, trust) == manifest
    assert rb.publish(output, store, trust) == manifest


def test_legacy_root_is_never_reinterpreted(tmp_path: Path, bundle) -> None:
    output, manifest, trust = bundle
    store, target = tmp_path / "store", tmp_path / "legacy"
    rb.publish(output, store, trust)
    (target / "releases" / ("a" * 40)).mkdir(parents=True)
    with pytest.raises(rb.BundleError, match="unrecognized/legacy"):
        rb.deploy(store, manifest.artifact_id, target, trust, _profile())
    assert sorted(p.name for p in target.iterdir()) == ["releases"]


def test_generic_native_flag_is_rejected(tmp_path: Path) -> None:
    data = json.loads(_description(tmp_path).read_text())
    data["compiler_flags"] = ["-O3", "-march=native"]
    with pytest.raises(ValueError, match="generic artifact"):
        rb.BuildDescription.model_validate(data)


def test_unselected_generation_recovers_after_interruption(tmp_path: Path, bundle, monkeypatch):
    output, manifest, trust = bundle
    store, target = tmp_path / "store", tmp_path / "target"
    rb.publish(output, store, trust)
    real_smoke = rb._smoke

    def interrupted(*args):
        raise OSError("controller interrupted after generation publication")

    monkeypatch.setattr(rb, "_smoke", interrupted)
    with pytest.raises(OSError, match="controller interrupted"):
        rb.deploy(store, manifest.artifact_id, target, trust, _profile())
    assert not (target / "selection.json").exists()
    assert (target / "generations" / manifest.artifact_id / "payload").is_dir()
    monkeypatch.setattr(rb, "_smoke", real_smoke)
    assert rb.deploy(store, manifest.artifact_id, target, trust, _profile())["verified"]


def test_smoke_timeout_leaves_selection_unset(tmp_path: Path, bundle):
    _output, _manifest, _trust_file = bundle
    (tmp_path / "built/bin/probe").write_text("#!/bin/sh\nsleep 10\n")
    output = tmp_path / "slow-bundle"
    manifest = rb.pack(tmp_path / "built", _description(tmp_path), output)
    trust = _trust(tmp_path, manifest)
    store, target = tmp_path / "store", tmp_path / "target"
    rb.publish(output, store, trust)
    with pytest.raises(rb.BundleError, match="timed out"):
        rb.deploy(store, manifest.artifact_id, target, trust, _profile(), timeout=0.1)
    assert not (target / "selection.json").exists()
