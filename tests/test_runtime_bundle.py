"""Build once, relocate twice; trust/compatibility/refusal/recovery contracts."""
from __future__ import annotations

import contextlib
import io
import json
import platform
import shutil
import ssl
import subprocess
import tarfile
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


@dataclass
class _HTTPSStore:
    url: str
    ca_file: Path
    requests: list[str]
    responses: dict[str, tuple[int, dict[str, str], bytes]]


@pytest.fixture
def https_store(tmp_path: Path, bundle, monkeypatch):
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("OpenSSL is required for the local HTTPS transport fixture")
    certificate, key = tmp_path / "ca.pem", tmp_path / "key.pem"
    subprocess.run([
        openssl, "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes",
        "-keyout", str(key), "-out", str(certificate), "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
    ], check=True, capture_output=True)
    output, manifest, _ = bundle
    prefix = f"/store/artifacts/{manifest.artifact_id}/"
    responses = {}
    for name in ("manifest.json", "bundle.tar"):
        body = (output / name).read_bytes()
        responses[name] = (200, {"Content-Length": str(len(body))}, body)
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            name = self.path.removeprefix(prefix)
            status, headers, body = responses.get(name, (404, {}, b""))
            self.send_response(status)
            for field, value in headers.items():
                self.send_header(field, value)
            self.end_headers()
            # A refusing client may close before reading the response.
            with contextlib.suppress(ConnectionError, ssl.SSLError):
                self.wfile.write(body)

        def log_message(self, *_args):
            pass

    monkeypatch.setenv("no_proxy", "localhost,127.0.0.1")
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield _HTTPSStore(f"https://localhost:{server.server_port}/store/",
                         certificate, requests, responses)
    finally:
        server.shutdown()
        thread.join(5)
        server.server_close()


def test_https_fetch_cli_then_deploy_and_offline_reuse(
    tmp_path: Path, bundle, https_store, monkeypatch,
) -> None:
    _output, manifest, trust = bundle
    store = tmp_path / "downloaded"
    result = CliRunner().invoke(main, [
        "runtime-bundle", "fetch", manifest.artifact_id, "--url", https_store.url,
        "--store", str(store), "--trust", str(trust), "--ca-file", str(https_store.ca_file),
        "--timeout", "2", "--max-download-mib", "1",
    ])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == {
        "artifact_id": manifest.artifact_id, "build_id": manifest.build.build_id,
        "published": True,
    }
    assert https_store.requests == [
        f"/store/artifacts/{manifest.artifact_id}/{name}"
        for name in ("manifest.json", "bundle.tar")
    ]
    assert not (store / "selection.json").exists()
    assert not (store / "generations").exists()
    for name in ("first-target", "second-target"):
        receipt = rb.deploy(store, manifest.artifact_id, tmp_path / name, trust, _profile())
        assert receipt["verified"] and receipt["target_builds"] == 0
        assert Path(receipt["generation"], "data/value").read_text() == "42\n"

    def no_network(*_args, **_kwargs):
        pytest.fail("a published artifact must be reusable without contacting the server")

    monkeypatch.setattr(rb, "build_opener", no_network)
    assert rb.fetch(https_store.url, manifest.artifact_id, store, trust) == manifest
    # Offline reuse still verifies bytes and the operator's current trust policy.
    archive = store / "artifacts" / manifest.artifact_id / "bundle.tar"
    archive.write_bytes(b"damaged")
    with pytest.raises(rb.BundleError, match="archive digest"):
        rb.fetch(https_store.url, manifest.artifact_id, store, trust)
    _trust(tmp_path)
    with pytest.raises(rb.BundleError, match="not pinned"):
        rb.fetch(https_store.url, manifest.artifact_id, store, trust)


@pytest.mark.parametrize("url", [
    "http://artifacts.example.org/store", "https://operator:secret@artifacts.example.org",
    "https://artifacts.example.org/?token=secret", "https://artifacts.example.org/#fragment",
])
def test_fetch_refuses_unsafe_urls_before_network(tmp_path: Path, bundle, monkeypatch, url):
    _output, manifest, trust = bundle

    def no_network(*_args, **_kwargs):
        pytest.fail("invalid URLs must be refused before contacting any server")

    monkeypatch.setattr(rb, "build_opener", no_network)
    store = tmp_path / "store"
    with pytest.raises(rb.BundleError, match="requires HTTPS") as error:
        rb.fetch(url, manifest.artifact_id, store, trust)
    assert "secret" not in str(error.value)
    assert not store.exists()


def test_fetch_requires_out_of_band_pin_before_network(tmp_path: Path, bundle, monkeypatch):
    _output, manifest, _ = bundle
    trust = _trust(tmp_path)

    def no_network(*_args, **_kwargs):
        pytest.fail("untrusted artifacts must be refused before contacting any server")

    monkeypatch.setattr(rb, "build_opener", no_network)
    with pytest.raises(rb.BundleError, match="not pinned"):
        rb.fetch("https://artifacts.example.org", manifest.artifact_id, tmp_path / "store", trust)
    assert not (tmp_path / "store").exists()


def test_fetch_rejects_untrusted_tls_certificate(tmp_path: Path, bundle, https_store):
    _output, manifest, trust = bundle
    with pytest.raises(rb.BundleError, match="HTTPS artifact transfer failed") as error:
        rb.fetch(https_store.url, manifest.artifact_id, tmp_path / "store", trust, timeout=2)
    assert https_store.url not in str(error.value)
    assert https_store.requests == []
    assert not (tmp_path / "store").exists()


@pytest.mark.parametrize("trusted", [False, True])
def test_fetch_authenticates_exact_manifest_before_archive(
    tmp_path: Path, bundle, https_store, trusted: bool,
):
    _output, manifest, trust = bundle
    body = manifest.model_dump()
    body["build"]["dependency_sha256"] = "d" * 64
    changed = rb.BundleManifest.model_validate(body)
    if trusted:
        trust = _trust(tmp_path, manifest, changed)
    data = changed.model_dump_json().encode()
    https_store.responses["manifest.json"] = (200, {"Content-Length": str(len(data))}, data)
    with pytest.raises(rb.BundleError, match="requested artifact identity|not pinned"):
        rb.fetch(https_store.url, manifest.artifact_id, tmp_path / "store", trust,
                 ca_file=https_store.ca_file)
    assert https_store.requests == [f"/store/artifacts/{manifest.artifact_id}/manifest.json"]
    assert not (tmp_path / "store").exists()


def test_interrupted_https_fetch_retries_same_identity(tmp_path: Path, bundle, https_store):
    _output, manifest, trust = bundle
    store = tmp_path / "store"
    original = https_store.responses["bundle.tar"]
    status, headers, body = original
    https_store.responses["bundle.tar"] = (status, headers, body[:10])
    temporary_root = Path(rb.tempfile.gettempdir())
    before = set(temporary_root.glob("vq-bundle-fetch-*"))
    with pytest.raises(rb.BundleError, match="declared length|transfer failed"):
        rb.fetch(https_store.url, manifest.artifact_id, store, trust, ca_file=https_store.ca_file)
    assert not store.exists()
    assert set(temporary_root.glob("vq-bundle-fetch-*")) == before
    https_store.responses["bundle.tar"] = original
    assert rb.fetch(https_store.url, manifest.artifact_id, store, trust,
                    ca_file=https_store.ca_file) == manifest
    assert set(temporary_root.glob("vq-bundle-fetch-*")) == before


@pytest.mark.parametrize("declared_length", [False, True])
def test_fetch_bounds_archive_download(tmp_path: Path, bundle, https_store, declared_length):
    _output, manifest, trust = bundle
    status, headers, body = https_store.responses["bundle.tar"]
    if not declared_length:
        headers = {}
    https_store.responses["bundle.tar"] = (status, headers, body)
    with pytest.raises(rb.BundleError, match="byte limit"):
        rb.fetch(https_store.url, manifest.artifact_id, tmp_path / "store", trust,
                 ca_file=https_store.ca_file, max_archive_bytes=16)
    assert not (tmp_path / "store").exists()


def test_fetch_refuses_redirect_and_archive_tampering(tmp_path: Path, bundle, https_store):
    _output, manifest, trust = bundle
    store = tmp_path / "store"
    https_store.responses["bundle.tar"] = (302, {"Location": "/redirected"}, b"")
    with pytest.raises(rb.BundleError, match="transfer failed"):
        rb.fetch(https_store.url, manifest.artifact_id, store, trust, ca_file=https_store.ca_file)
    assert len(https_store.requests) == 2
    assert not any("redirected" in request for request in https_store.requests)
    assert not store.exists()
    https_store.responses["bundle.tar"] = (200, {"Content-Length": "7"}, b"damaged")
    with pytest.raises(rb.BundleError, match="archive digest"):
        rb.fetch(https_store.url, manifest.artifact_id, store, trust, ca_file=https_store.ca_file)
    assert not store.exists()


def test_fetch_rechecks_trust_after_download(tmp_path: Path, bundle, https_store, monkeypatch):
    _output, manifest, trust = bundle
    download = rb._download

    def revoke_after_archive(*args, **kwargs):
        download(*args, **kwargs)
        if args[2].name == "bundle.tar":
            _trust(tmp_path)

    monkeypatch.setattr(rb, "_download", revoke_after_archive)
    with pytest.raises(rb.BundleError, match="not pinned"):
        rb.fetch(https_store.url, manifest.artifact_id, tmp_path / "store", trust,
                 ca_file=https_store.ca_file)
    assert len(https_store.requests) == 2
    assert not (tmp_path / "store").exists()


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
