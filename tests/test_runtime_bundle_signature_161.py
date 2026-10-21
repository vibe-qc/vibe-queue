"""#161: trusting a runtime bundle by its publisher's OpenSSH signature.

A `vq.runtime-trust/2` policy may name publisher public keys beside its digest
pins. `vq runtime-bundle sign` writes `manifest.json.sig` over the canonical
manifest with `ssh-keygen -Y sign`, and publish, deploy and fetch then trust
an unpinned manifest only when `ssh-keygen -Y verify` confirms that signature
with a key listed for the manifest's publisher. Real keys and the real
`ssh-keygen` throughout; fetches use the SSH test transport's local stand-in.
"""
from __future__ import annotations

import base64
import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import runtime_bundle as rb
from vq.cli import main

pytestmark = [
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="needs OpenSSH ssh-keygen"),
]

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle import _description, _profile, bundle  # noqa: E402,F401
from test_runtime_bundle_ssh_161 import _commands, ssh  # noqa: E402,F401

Bundle = tuple[Path, rb.BundleManifest, Path]
Key = tuple[Path, str, str]  # private key, public key line, fingerprint


def _key(tmp_path: Path, name: str) -> Key:
    private = tmp_path / "keys" / name
    private.parent.mkdir(exist_ok=True)
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", f"{name} key",
                    "-f", str(private)], check=True, stdin=subprocess.DEVNULL)
    listed = subprocess.run(["ssh-keygen", "-l", "-f", f"{private}.pub"], check=True,
                            capture_output=True, text=True).stdout
    return private, Path(f"{private}.pub").read_text().strip(), listed.split()[1]


@pytest.fixture
def alice(tmp_path: Path) -> Key:
    return _key(tmp_path, "alice")


@pytest.fixture
def mallory(tmp_path: Path) -> Key:
    return _key(tmp_path, "mallory")


def _policy(tmp_path: Path, *, signers: dict[str, list[str]],
            pins: dict[str, list[str]] | None = None, name: str = "signed-trust.json") -> Path:
    path = tmp_path / name
    path.write_text(json.dumps({"schema_name": "vq.runtime-trust/2",
                                "publishers": pins or {}, "signers": signers}))
    return path


def _pack(tmp_path: Path, dependency: str) -> tuple[Path, rb.BundleManifest]:
    payload = tmp_path / f"built-{dependency}"
    (payload / "bin").mkdir(parents=True)
    (payload / "bin/probe").write_text("#!/bin/sh\ntest -f data/value\n")
    (payload / "bin/probe").chmod(0o755)
    (payload / "data").mkdir()
    (payload / "data/value").write_text(f"{dependency}\n")
    output = tmp_path / f"bundle-{dependency}"
    return output, rb.pack(payload, _description(tmp_path, dependency), output)


def _target(tmp_path: Path) -> Path:
    return tmp_path / "target"


class TestPolicy:
    def test_signers_need_schema_2_and_a_v1_policy_is_unchanged(self, alice: Key) -> None:
        with pytest.raises(ValueError, match="signers require schema_name vq.runtime-trust/2"):
            rb.TrustPolicy.model_validate_json(json.dumps(
                {"schema_name": "vq.runtime-trust/1", "publishers": {},
                 "signers": {"test-builder": [alice[1]]}}))
        v1 = rb.TrustPolicy.model_validate_json(
            json.dumps({"schema_name": "vq.runtime-trust/1", "publishers": {"p": ["d" * 64]}}))
        assert v1.signers == {}
        v2 = rb.TrustPolicy.model_validate_json(json.dumps(
            {"schema_name": "vq.runtime-trust/2", "publishers": {},
             "signers": {"test-builder": [alice[1]]}}))
        assert v2.signers == {"test-builder": [alice[1]]}

    ONE_LINE = "a signer is one OpenSSH public key line"

    @pytest.mark.parametrize(("make", "match"), [
        (lambda line: f"cert-authority {line}", ONE_LINE),  # an allowed-signers option
        (lambda line: line.replace(" ", "\n", 1), ONE_LINE),  # a second line
        (lambda line: line + "\nvq-publisher " + line, ONE_LINE),  # an injected entry
        (lambda line: line + "\r", ONE_LINE),  # a control character: a CRLF ending
        (lambda line: line.split()[0] + " AAAA!notbase64", ONE_LINE),
        (lambda line: line.replace("ssh-ed25519", "ssh-dss", 1), ONE_LINE),
        (lambda _line: "ssh-dss " + base64.b64encode(b"\0\0\0\x07ssh-dss" + b"\0" * 32)
         .decode(), ONE_LINE),  # a well-formed key of a type that is not accepted
        (lambda line: line.replace("ssh-ed25519", "ssh-rsa", 1), "match its declared type"),
        (lambda line: line.split()[0] + " " + line.split()[1][:-3], "not valid base64"),
    ])
    def test_a_signer_must_be_one_plain_public_key(
        self, alice: Key, make: Any, match: str,
    ) -> None:
        with pytest.raises(ValueError, match=match):
            rb.TrustPolicy(schema_name="vq.runtime-trust/2", publishers={},
                           signers={"test-builder": [make(alice[1])]})

    def test_a_key_without_a_comment_is_accepted(self, alice: Key) -> None:
        bare = " ".join(alice[1].split()[:2])
        assert rb.TrustPolicy(schema_name="vq.runtime-trust/2", publishers={},
                              signers={"p": [bare]}).signers == {"p": [bare]}


class TestSign:
    def test_it_signs_the_canonical_manifest_and_plain_openssh_verifies_it(
        self, tmp_path: Path, bundle: Bundle, alice: Key,  # noqa: F811
    ) -> None:
        output, manifest, _pins = bundle
        result = CliRunner().invoke(main, ["runtime-bundle", "sign", str(output),
                                           "--key", str(alice[0])])
        assert result.exit_code == 0, result.output
        signature = output / "manifest.json.sig"
        assert json.loads(result.output) == {"artifact_id": manifest.artifact_id,
                                             "publisher": "test-builder",
                                             "signature": str(signature)}
        # The manifest file is the canonical bytes, so an operator can check by hand.
        allowed = tmp_path / "allowed"
        allowed.write_text(f'builder namespaces="runtime-bundle@vibe-qc.com" {alice[1]}\n')
        with (output / "manifest.json").open("rb") as message:
            subprocess.run(["ssh-keygen", "-Y", "verify", "-f", str(allowed), "-I", "builder",
                            "-n", "runtime-bundle@vibe-qc.com", "-s", str(signature)],
                           stdin=message, check=True, capture_output=True)
        assert not any(path.name.startswith("alice") for path in output.iterdir())

    def test_a_modified_archive_is_not_signed(self, bundle: Bundle, alice: Key) -> None:  # noqa: F811
        output, _manifest, _pins = bundle
        with (output / "bundle.tar").open("ab") as archive:
            archive.write(b"\0" * 512)
        with pytest.raises(rb.BundleError, match="archive digest"):
            rb.sign(output, alice[0])
        assert not (output / "manifest.json.sig").exists()

    def test_an_unusable_key_is_reported_and_nothing_written(
        self, tmp_path: Path, bundle: Bundle,  # noqa: F811
    ) -> None:
        output, _manifest, _pins = bundle
        junk = tmp_path / "not-a-key"
        junk.write_text("not a key\n")
        with pytest.raises(rb.BundleError, match="ssh-keygen could not sign with that key: .+"):
            rb.sign(output, junk)
        assert not (output / "manifest.json.sig").exists()

    def test_signing_again_replaces_the_signature(
        self, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ) -> None:
        output, _manifest, _pins = bundle
        rb.sign(output, alice[0])
        first = (output / "manifest.json.sig").read_bytes()
        rb.sign(output, mallory[0])
        assert (output / "manifest.json.sig").read_bytes() != first


class TestTrust:
    def test_a_signed_unpinned_bundle_publishes_and_deploys_naming_its_signer(
        self, tmp_path: Path, bundle: Bundle, alice: Key,  # noqa: F811
    ) -> None:
        output, manifest, _pins = bundle
        rb.sign(output, alice[0])
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        store = tmp_path / "store"
        assert rb.publish(output, store, trust) == manifest
        stored = store / "artifacts" / manifest.artifact_id
        assert (stored / "manifest.json.sig").read_bytes() == (
            output / "manifest.json.sig").read_bytes()
        planned = CliRunner().invoke(main, [
            "runtime-bundle", "deploy", manifest.artifact_id, "--store", str(store),
            "--root", str(_target(tmp_path)), "--trust", str(trust),
            "--target-profile", str(_profile_file(tmp_path)),
        ])
        assert planned.exit_code == 0, planned.output
        plan = json.loads(planned.output)
        assert plan["signer"] == alice[2] and plan["blockers"] == []
        receipt = rb.deploy(store, manifest.artifact_id, _target(tmp_path), trust, _profile())
        assert receipt["verified"] and receipt["signer"] == alice[2]
        written = json.loads((_target(tmp_path) / "generations" / manifest.artifact_id
                              / "verification.json").read_text())
        assert written["signer"] == alice[2]

    def test_a_pin_needs_no_signature_and_records_no_signer(
        self, tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ) -> None:
        output, manifest, pins = bundle
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]},
                        pins={"test-builder": [manifest.artifact_id]})
        (output / "manifest.json.sig").write_text("garbage that would not verify\n")
        with pytest.raises(rb.BundleError, match="not an OpenSSH signature"):
            rb.publish(output, tmp_path / "store", trust)  # never published as given
        rb.sign(output, mallory[0])  # a signature nobody trusts does not matter
        for policy in (pins, trust):
            store = tmp_path / f"store-{policy.stem}"
            rb.publish(output, store, policy)
            receipt = rb.deploy(store, manifest.artifact_id, tmp_path / f"t-{policy.stem}",
                                policy, _profile())
            assert "signer" not in receipt

    def test_each_untrusted_signature_is_refused_and_nothing_published(
        self, tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ) -> None:
        output, manifest, _pins = bundle
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        store = tmp_path / "store"
        signature = output / "manifest.json.sig"

        def refused(match: str) -> None:
            with pytest.raises(rb.BundleError, match=match):
                rb.publish(output, store, trust)
            assert not (store / "artifacts" / manifest.artifact_id).exists()

        refused("not pinned for this publisher and has no signature")
        rb.sign(output, mallory[0])
        refused("does not verify with a key")
        other_output, _other = _pack(tmp_path, "d")
        rb.sign(other_output, alice[0])
        shutil.copyfile(other_output / "manifest.json.sig", signature)
        refused("does not verify with a key")  # alice signed a different manifest
        message = tmp_path / "manifest-copy.json"
        shutil.copyfile(output / "manifest.json", message)
        subprocess.run(["ssh-keygen", "-Y", "sign", "-f", str(alice[0]), "-n", "file",
                        str(message)], check=True, capture_output=True, stdin=subprocess.DEVNULL)
        shutil.copyfile(f"{message}.sig", signature)
        refused("does not verify with a key")  # the right key, another namespace
        signature.unlink()
        signature.symlink_to(other_output / "manifest.json.sig")
        refused("expected a regular file")
        signature.unlink()
        signature.symlink_to(tmp_path / "nothing-here")  # a dangling link is no absence
        refused("expected a regular file")
        signature.unlink()
        begin, end = "-----BEGIN SSH SIGNATURE-----\n", "\n-----END SSH SIGNATURE-----\n"
        oversized = begin + "A" * (64 * 1024 + 1 - len(begin) - len(end)) + end
        for malformed in (oversized, begin + "\u00e9" + end,
                          "A" * 70 + end, begin + "A" * 70 + "\n"):
            signature.write_text(malformed)
            refused("not an OpenSSH signature")

    def test_a_key_listed_for_another_publisher_does_not_count(
        self, tmp_path: Path, bundle: Bundle, alice: Key,  # noqa: F811
    ) -> None:
        output, _manifest, _pins = bundle
        rb.sign(output, alice[0])
        with pytest.raises(rb.BundleError, match="is not pinned for this publisher by"):
            rb.publish(output, tmp_path / "store",
                       _policy(tmp_path, signers={"other-builder": [alice[1]]}))

    def test_any_listed_key_verifies_and_dropping_it_stops_new_deploys(
        self, tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ) -> None:
        output, manifest, _pins = bundle
        rb.sign(output, alice[0])
        both = _policy(tmp_path, signers={"test-builder": [mallory[1], alice[1]]})
        store = tmp_path / "store"
        rb.publish(output, store, both)
        receipt = rb.deploy(store, manifest.artifact_id, _target(tmp_path), both, _profile())
        assert receipt["signer"] == alice[2]
        rotated = _policy(tmp_path, signers={"test-builder": [mallory[1]]}, name="rotated.json")
        with pytest.raises(rb.BundleError, match="does not verify with a key"):
            rb.deploy(store, manifest.artifact_id, _target(tmp_path), rotated, _profile())
        assert rb.verified_generation(_target(tmp_path))[0] == manifest.artifact_id

    def test_without_a_working_ssh_keygen_nothing_is_trusted_by_signature(
        self, tmp_path: Path, bundle: Bundle, alice: Key, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    ) -> None:
        output, _manifest, _pins = bundle
        rb.sign(output, alice[0])
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        old = tmp_path / "old-openssh"
        old.mkdir()
        (old / "ssh-keygen").write_text("#!/bin/sh\necho 'unknown option -- Y' >&2\nexit 1\n")
        (old / "ssh-keygen").chmod(0o755)
        monkeypatch.setenv("PATH", f"{old}{os.pathsep}{os.environ['PATH']}")
        with pytest.raises(rb.BundleError, match="does not verify with a key"):
            rb.publish(output, tmp_path / "store", trust)
        monkeypatch.setenv("PATH", str(tmp_path / "nowhere"))
        with pytest.raises(rb.BundleError, match=r"need OpenSSH ssh-keygen \(8.1 or later\)"):
            rb.publish(output, tmp_path / "store", trust)

    def test_a_verifier_that_hangs_or_cannot_start_refuses(
        self, tmp_path: Path, bundle: Bundle, alice: Key, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    ) -> None:
        output, _manifest, _pins = bundle
        rb.sign(output, alice[0])
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        for failure in (subprocess.TimeoutExpired("ssh-keygen", 30), PermissionError("denied")):
            def run(*_args: Any, error: Exception = failure, **kwargs: Any) -> None:
                assert 0 < kwargs["timeout"] <= 30  # a hung verifier is bounded
                raise error

            monkeypatch.setattr(subprocess, "run", run)
            with pytest.raises(rb.BundleError, match="ssh-keygen could not verify"):
                rb.publish(output, tmp_path / "store", trust)


    def test_ssh_keygen_says_nothing_on_success_or_failure(
        self, tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
        capfd: pytest.CaptureFixture[str],
    ) -> None:
        output, _manifest, _pins = bundle
        rb.sign(output, alice[0])
        capfd.readouterr()
        rb.publish(output, tmp_path / "store", _policy(
            tmp_path, signers={"test-builder": [mallory[1], alice[1]]}))
        with pytest.raises(rb.BundleError, match="does not verify"):
            rb.publish(output, tmp_path / "other-store", _policy(
                tmp_path, signers={"test-builder": [mallory[1]]}, name="mallory.json"))
        (output / "manifest.json.sig").write_text(  # well armoured, unparseable inside
            "-----BEGIN SSH SIGNATURE-----\nAAAA\n-----END SSH SIGNATURE-----\n")
        with pytest.raises(rb.BundleError, match="does not verify"):
            rb.publish(output, tmp_path / "other-store", _policy(
                tmp_path, signers={"test-builder": [alice[1]]}, name="alice.json"))
        assert capfd.readouterr() == ("", "")


def _profile_file(tmp_path: Path) -> Path:
    path = tmp_path / "profile.json"
    path.write_text(_profile().model_dump_json())
    return path


class TestStoreSignature:
    def test_a_verified_republish_attaches_the_signature_and_a_pinned_one_does_not(
        self, tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ) -> None:
        output, manifest, pins = bundle
        store = tmp_path / "store"
        rb.publish(output, store, pins)  # first published by pin, unsigned
        stored = store / "artifacts" / manifest.artifact_id / "manifest.json.sig"
        assert not stored.exists()
        signed = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        with pytest.raises(rb.BundleError, match="has no signature"):
            rb.deploy(store, manifest.artifact_id, _target(tmp_path), signed, _profile())
        rb.sign(output, alice[0])
        rb.publish(output, store, signed)
        assert stored.read_bytes() == (output / "manifest.json.sig").read_bytes()
        assert rb.deploy(store, manifest.artifact_id, _target(tmp_path), signed,
                         _profile())["signer"] == alice[2]
        alice_signature = stored.read_bytes()
        rb.sign(output, mallory[0])
        rb.publish(output, store, pins)  # trusted by its pin: the stored one stays
        assert stored.read_bytes() == alice_signature

    def test_a_pinned_bundle_with_a_dangling_signature_link_is_refused(
        self, tmp_path: Path, bundle: Bundle,  # noqa: F811
    ) -> None:
        output, manifest, pins = bundle
        (output / "manifest.json.sig").symlink_to(tmp_path / "nothing-here")
        with pytest.raises(rb.BundleError, match="expected a regular file"):
            rb.publish(output, tmp_path / "store", pins)
        assert not (tmp_path / "store" / "artifacts" / manifest.artifact_id).exists()

    def test_a_stored_manifest_that_differs_is_not_republished_over(
        self, tmp_path: Path, bundle: Bundle, alice: Key,  # noqa: F811
    ) -> None:
        output, manifest, _pins = bundle
        rb.sign(output, alice[0])
        trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
        store = tmp_path / "store"
        rb.publish(output, store, trust)
        other_output, _other = _pack(tmp_path, "d")
        stored = store / "artifacts" / manifest.artifact_id
        stored.chmod(0o755)
        shutil.copyfile(other_output / "manifest.json", stored / "manifest.json")
        with pytest.raises(rb.BundleError, match="published identity has different metadata"):
            rb.publish(output, store, trust)


def test_a_signed_artifact_is_fetched_by_signature_over_ssh(
    tmp_path: Path, bundle: Bundle, alice: Key, ssh: tuple[list[str], Path],  # noqa: F811
) -> None:
    argv, log = ssh
    output, manifest, _pins = bundle
    rb.sign(output, alice[0])
    trust = _policy(tmp_path, signers={"test-builder": [alice[1]]})
    remote = tmp_path / "builder store"
    rb.publish(output, remote, trust)
    local = tmp_path / "local-store"
    assert rb.fetch_over_ssh(argv, str(remote), manifest.artifact_id, local, trust,
                             max_archive_bytes=1024**2) == manifest
    base = f"{remote}/artifacts/{manifest.artifact_id}"
    assert [command.rsplit("/", 1)[-1].rstrip("'") for command in _commands(log)] == [
        "manifest.json", "manifest.json.sig", "bundle.tar"]
    assert f"head -c {64 * 1024 + 1} -- {shlex.quote(base + '/manifest.json.sig')}" in (
        _commands(log))
    assert (local / "artifacts" / manifest.artifact_id / "manifest.json.sig").is_file()
    assert rb.deploy(local, manifest.artifact_id, _target(tmp_path), trust,
                     _profile())["signer"] == alice[2]
    log.write_text("")  # reused offline, its stored signature verified again
    assert rb.fetch_over_ssh(argv, str(remote), manifest.artifact_id, local, trust) == manifest
    assert _commands(log) == []


def test_a_pinned_fetch_requests_no_signature(
    tmp_path: Path, bundle: Bundle, alice: Key, ssh: tuple[list[str], Path],  # noqa: F811
) -> None:
    argv, log = ssh
    output, manifest, _pins = bundle
    rb.sign(output, alice[0])
    trust = _policy(tmp_path, signers={"test-builder": [alice[1]]},
                    pins={"test-builder": [manifest.artifact_id]})
    remote = tmp_path / "builder store"
    rb.publish(output, remote, trust)
    rb.fetch_over_ssh(argv, str(remote), manifest.artifact_id, tmp_path / "local", trust)
    assert [command.rsplit("/", 1)[-1].rstrip("'") for command in _commands(log)] == [
        "manifest.json", "bundle.tar"]


def test_an_untrusted_fetch_stops_before_the_archive(
    tmp_path: Path, bundle: Bundle, alice: Key, mallory: Key,  # noqa: F811
    ssh: tuple[list[str], Path],  # noqa: F811
) -> None:
    argv, log = ssh
    output, manifest, pins = bundle
    rb.sign(output, mallory[0])
    remote = tmp_path / "builder store"
    rb.publish(output, remote, pins)
    local = tmp_path / "local"

    def fetched(trust: Path, match: str) -> list[str]:
        log.write_text("")
        with pytest.raises(rb.BundleError, match=match):
            rb.fetch_over_ssh(argv, str(remote), manifest.artifact_id, local, trust)
        assert not (local / "artifacts" / manifest.artifact_id).exists()
        return [command.rsplit("/", 1)[-1].rstrip("'") for command in _commands(log)]

    alice_only = _policy(tmp_path, signers={"test-builder": [alice[1]]})
    assert fetched(alice_only, "does not verify with a key") == [
        "manifest.json", "manifest.json.sig"]
    elsewhere = _policy(tmp_path, signers={"other-builder": [alice[1]]}, name="other.json")
    assert fetched(elsewhere, "is not pinned for this publisher by") == ["manifest.json"]
    nobody = _policy(tmp_path, signers={"test-builder": []}, name="nobody.json")
    assert fetched(nobody, "requested artifact is not pinned") == []
