"""Portable runtime artifacts, separate from legacy source-SHA venv slots.

Product builders own compilation and relocation. File, HTTPS and SSH transports
authenticate and publish their output for deployment without any build fallback.
Trust is an operator-maintained allowlist of exact manifest digests, obtained
out of band, not a checksum file shipped alongside an untrusted download. A
schema 2 policy may instead name publisher OpenSSH keys: a manifest is then
also trusted when ``ssh-keygen -Y verify`` confirms its detached signature.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import http.client
import json
import os
import re
import selectors
import shlex
import shutil
import signal
import ssl
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath
from typing import Annotated, Any, Literal
from urllib.error import URLError
from urllib.parse import urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, OpenerDirector, Request, build_opener

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator

from vq._storage import atomic_write_text
from vq.paths import require_test_path_within_sandbox
from vq.runtime_slots import layout

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Text = Annotated[str, Field(min_length=1, max_length=1024)]
_MAX_FILES = 10_000
_MAX_BYTES = 64 * 1024**3
_MAX_ARCHIVE_BYTES = _MAX_BYTES + 32 * 1024**2
_MAX_BEARER_TOKEN_BYTES = 16 * 1024


class BundleError(ValueError):
    """An artifact or target failed a required gate."""


class BundleModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class TargetProfile(BundleModel):
    """Operator-qualified compute-node facts; not login-node autodetection."""

    os: Text
    arch: Text
    isa: list[Text]
    # Exact ABI tokens are conservative: no undocumented version ordering.
    # Use explicit "none" for static/no-Python/no-MPI/no-BLAS requirements.
    abi: dict[Text, Text]
    modules: list[Text] = Field(default_factory=list)

    @model_validator(mode="after")
    def complete_abi(self) -> TargetProfile:
        required = {"libc", "python", "blas", "mpi", "launcher"}
        if not required.issubset(self.abi):
            raise ValueError(f"ABI profile must declare {sorted(required)} (or explicit 'none')")
        return self


class BuildDescription(BundleModel):
    schema_name: Literal["vq.runtime-build/1"] = "vq.runtime-build/1"
    program: Text
    source_revision: Annotated[str, Field(pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")]
    recipe_sha256: Digest
    dependency_sha256: Digest
    target: TargetProfile
    variant: Literal["generic", "optimized"]
    toolchain: Text
    compiler_flags: list[Text]
    publisher: Text
    build_receipt: Text
    qualification_receipt: Text
    # Explicit argv, invoked only after trust and compatibility checks.
    smoke_argv: Annotated[list[Text], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def validate_probe(self) -> BuildDescription:
        _relative(self.smoke_argv[0])
        if self.variant == "generic" and any(
            re.search(r"(?:-march|-mcpu)\s*=\s*native(?:\s|$)", flag)
            for flag in self.compiler_flags
        ):
            raise ValueError("native ISA flags cannot qualify a generic artifact")
        return self

    @property
    def build_id(self) -> str:
        return _digest(_json(self))


class FileRecord(BundleModel):
    sha256: Digest
    size: Annotated[int, Field(ge=0, le=_MAX_BYTES)]
    executable: bool


class BundleManifest(BundleModel):
    schema_name: Literal["vq.runtime-bundle/1"] = "vq.runtime-bundle/1"
    build: BuildDescription
    archive_sha256: Digest
    files: dict[str, FileRecord]

    @model_validator(mode="after")
    def validate_inventory(self) -> BundleManifest:
        if not 1 <= len(self.files) <= _MAX_FILES:
            raise ValueError("bundle requires 1..10000 files")
        if sum(item.size for item in self.files.values()) > _MAX_BYTES:
            raise ValueError("bundle exceeds the 64 GiB unpacked limit")
        folded = {name.casefold() for name in self.files}
        if len(folded) != len(self.files):
            raise ValueError("case-colliding bundle paths are unsupported")
        for name in self.files:
            _relative(name)
            if any(parent.as_posix().casefold() in folded
                   for parent in PurePosixPath(name).parents):
                raise ValueError("file/directory path collision")
        probe = self.files.get(self.build.smoke_argv[0])
        if probe is None or not probe.executable:
            raise ValueError("smoke executable must be an executable inventory member")
        return self

    @property
    def artifact_id(self) -> str:
        return _digest(_json(self))


#: The SSHSIG namespace of a bundle signature, so no other signature verifies.
SIGNATURE_NAMESPACE = "runtime-bundle@vibe-qc.com"
#: A detached signature over the canonical manifest, beside ``manifest.json``.
SIGNATURE_FILE = "manifest.json.sig"
_MAX_SIGNATURE_BYTES = 64 * 1024
# The one principal of each single-key allowed-signers list vq writes.
_SIGNER = "vq-publisher"
_SSH_KEY_TYPES = frozenset({
    "ssh-ed25519", "sk-ssh-ed25519@openssh.com", "ssh-rsa", "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384", "ecdsa-sha2-nistp521", "sk-ecdsa-sha2-nistp256@openssh.com",
})
_SSH_KEY = re.compile(r"([a-z0-9@.-]+) ([A-Za-z0-9+/]+={0,2})(?: [^\x00-\x1f\x7f]*)?")


def _ssh_key_blob(line: str) -> tuple[str, bytes]:
    """The type and key blob of one plain OpenSSH public key line (no options)."""
    match = _SSH_KEY.fullmatch(line)
    if match is None or match[1] not in _SSH_KEY_TYPES:
        raise ValueError("a signer is one OpenSSH public key line: type, base64 key, comment")
    try:
        blob = base64.b64decode(match[2])
    except ValueError:
        raise ValueError("a signer's key is not valid base64") from None
    declared = match[1].encode()
    if blob[:4 + len(declared)] != len(declared).to_bytes(4, "big") + declared:
        raise ValueError("a signer's key does not match its declared type")
    return match[1], blob


def _ssh_key(line: str) -> str:
    _ssh_key_blob(line)
    return line


SshPublicKey = Annotated[str, AfterValidator(_ssh_key)]


class TrustPolicy(BundleModel):
    schema_name: Literal["vq.runtime-trust/1", "vq.runtime-trust/2"] = "vq.runtime-trust/1"
    publishers: dict[str, list[Digest]]
    # Schema 2: keys whose signature trusts a publisher's manifest without a pin.
    signers: dict[Text, list[SshPublicKey]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def signers_need_schema_2(self) -> TrustPolicy:
        if self.signers and self.schema_name != "vq.runtime-trust/2":
            raise ValueError("signers require schema_name vq.runtime-trust/2")
        return self


def _json(value: BaseModel | dict[str, Any]) -> bytes:
    body = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _relative(name: str) -> str:
    path = PurePosixPath(name)
    if (not name or len(name) > 512 or "\\" in name or "\x00" in name
            or path.is_absolute() or path.as_posix() != name
            or any(part in {"", ".", ".."} for part in name.split("/"))):
        raise BundleError(f"unsafe bundle path: {name!r}")
    return name


def _read_json(path: Path) -> bytes:
    _regular(path)
    with path.open("rb") as handle:
        data = handle.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise BundleError("JSON document exceeds 2 MiB")
    return data


def _regular(path: Path) -> os.stat_result:
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise BundleError(f"expected a regular file without symlinks: {path}")
    return info


def _file_digest(path: Path) -> str:
    if _regular(path).st_size > _MAX_ARCHIVE_BYTES:
        raise BundleError("archive/file exceeds size limit")
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def load_manifest(directory: Path, trust: Path) -> BundleManifest:
    return _trusted(directory, trust)[0]


def _trusted(directory: Path, trust: Path) -> tuple[BundleManifest, str | None]:
    """The manifest in ``directory`` if the operator policy trusts it, and how.

    A pin for its publisher is enough (None). Otherwise its signature must
    verify with a key the policy names for that publisher, and the second
    value is that key's SHA256 fingerprint.
    """
    manifest = BundleManifest.model_validate_json(_read_json(directory / "manifest.json"))
    policy = TrustPolicy.model_validate_json(_read_json(trust))
    publisher = manifest.build.publisher
    if manifest.artifact_id in policy.publishers.get(publisher, []):
        return manifest, None
    keys = policy.signers.get(publisher)
    if not keys:
        raise BundleError("manifest is not pinned for this publisher by the operator trust policy")
    return manifest, _verified_signer(manifest, directory / SIGNATURE_FILE, keys)


def _ssh_keygen() -> str:
    found = shutil.which("ssh-keygen")
    if found is None:
        raise BundleError("bundle signatures need OpenSSH ssh-keygen (8.1 or later) on PATH")
    return found


def _read_signature(path: Path) -> bytes:
    if not path.exists() and not path.is_symlink():
        raise BundleError("manifest is not pinned for this publisher and has no signature "
                          f"({SIGNATURE_FILE})")
    _regular(path)
    with path.open("rb") as handle:
        data = handle.read(_MAX_SIGNATURE_BYTES + 1)
    if (len(data) > _MAX_SIGNATURE_BYTES or not data.isascii()
            or not data.startswith(b"-----BEGIN SSH SIGNATURE-----\n")
            or not data.endswith(b"\n-----END SSH SIGNATURE-----\n")):
        raise BundleError(f"{SIGNATURE_FILE} is not an OpenSSH signature")
    return data


def _fingerprint(blob: bytes) -> str:
    """A key's fingerprint as ``ssh-keygen -l`` prints it."""
    return "SHA256:" + base64.b64encode(hashlib.sha256(blob).digest()).decode().rstrip("=")


def _verified_signer(manifest: BundleManifest, path: Path, keys: list[str]) -> str:
    """The fingerprint of the key whose signature over ``manifest`` verifies.

    Each key is tried alone, so the result names the one that signed. The
    signature and allowed-signers list sit in private staging, and
    ssh-keygen's messages are not echoed. An ssh-keygen without ``-Y verify``
    (OpenSSH before 8.1) verifies nothing.
    """
    signature = _read_signature(path)
    keygen = _ssh_keygen()
    with tempfile.TemporaryDirectory(prefix="vq-bundle-verify-") as tmp:
        staged = Path(tmp) / SIGNATURE_FILE
        staged.write_bytes(signature)
        allowed = Path(tmp) / "allowed_signers"
        for key in keys:
            kind, blob = _ssh_key_blob(key)
            allowed.write_text(f"{_SIGNER} {kind} {base64.b64encode(blob).decode()}\n")
            try:
                result = subprocess.run(
                    [keygen, "-Y", "verify", "-f", str(allowed), "-I", _SIGNER,
                     "-n", SIGNATURE_NAMESPACE, "-s", str(staged)],
                    input=_json(manifest), stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, timeout=30, check=False,
                )
            except (OSError, subprocess.TimeoutExpired):
                raise BundleError("ssh-keygen could not verify the bundle signature") from None
            if result.returncode == 0:
                return _fingerprint(blob)
    raise BundleError("manifest signature does not verify with a key the operator trust "
                      "policy names for this publisher")


def sign(directory: Path, key: Path) -> BundleManifest:
    """Sign a bundle's canonical manifest with an OpenSSH key, which stays put.

    ``directory`` is a packed bundle or a published artifact; its archive is
    checked against the manifest first, since the signature vouches for both.
    ``key`` is a private key file, or a public key whose private half
    ssh-agent holds; ssh-keygen may ask for a passphrase on the terminal.
    The signature replaces any earlier one atomically.
    """
    _directory(directory)
    manifest = BundleManifest.model_validate_json(_read_json(directory / "manifest.json"))
    _archive(manifest, directory / "bundle.tar")
    keygen = _ssh_keygen()
    with tempfile.TemporaryDirectory(prefix="vq-bundle-sign-") as tmp:
        message = Path(tmp) / "manifest.json"
        message.write_bytes(_json(manifest))
        try:
            result = subprocess.run(
                [keygen, "-Y", "sign", "-f", str(key), "-n", SIGNATURE_NAMESPACE, str(message)],
                # A passphrase prompt uses the terminal, never vq's stdin (#118).
                stdin=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False,
            )
        except OSError:
            raise BundleError("ssh-keygen could not start to sign the bundle") from None
        if result.returncode != 0:
            reason = result.stderr.decode(errors="replace").strip().splitlines()
            raise BundleError("ssh-keygen could not sign with that key"
                              + (f": {reason[-1][:200]}" if reason else ""))
        signature = _read_signature(Path(tmp) / SIGNATURE_FILE)
    atomic_write_text(directory / SIGNATURE_FILE, signature.decode("ascii"))
    return manifest


def compatibility(manifest: BundleManifest, target: TargetProfile) -> list[str]:
    required = manifest.build.target
    blockers = []
    for field in ("os", "arch"):
        if getattr(required, field) != getattr(target, field):
            blockers.append(f"{field} mismatch")
    if not set(required.isa).issubset(target.isa):
        blockers.append("required ISA features are absent")
    for name, value in required.abi.items():
        if value != "none" and target.abi.get(name) != value:
            blockers.append(f"{name} ABI mismatch or unknown")
    if not set(required.modules).issubset(target.modules):
        blockers.append("required external modules are absent")
    return blockers


def _archive(manifest: BundleManifest, archive: Path, destination: Path | None = None) -> None:
    """Stream and validate every member; never call tar.extract/extractall."""
    if _file_digest(archive) != manifest.archive_sha256:
        raise BundleError("archive digest mismatch")
    seen: set[str] = set()
    files: set[str] = set()
    with tarfile.open(archive, "r|*") as stream:
        for member in stream:
            name = _relative(member.name)
            if name in seen or len(seen) >= 2 * _MAX_FILES:
                raise BundleError("duplicate archive member or member limit exceeded")
            seen.add(name)
            if member.isdir():
                if name in manifest.files or not any(
                    p.startswith(name + "/") for p in manifest.files
                ):
                    raise BundleError("unexpected archive directory")
                continue
            expected = manifest.files.get(name)
            if not member.isfile() or member.issparse() or expected is None:
                raise BundleError("links, special files and unlisted members are forbidden")
            if member.size != expected.size or member.mode & 0o7000:
                raise BundleError("archive member size or privileged mode mismatch")
            if bool(member.mode & 0o111) != expected.executable:
                raise BundleError("archive executable mode mismatch")
            source = stream.extractfile(member)
            if source is None:
                raise BundleError("unreadable archive member")
            digest = hashlib.sha256()
            with contextlib.ExitStack() as stack:
                stack.enter_context(source)
                output = None
                if destination is not None:
                    path = destination / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    output = stack.enter_context(path.open("xb"))
                while block := source.read(1024 * 1024):
                    digest.update(block)
                    if output is not None:
                        output.write(block)
                if output is not None:
                    output.flush()
                    os.fsync(output.fileno())
                    os.fchmod(output.fileno(), 0o755 if expected.executable else 0o644)
            if digest.hexdigest() != expected.sha256:
                raise BundleError("archive file digest mismatch")
            files.add(name)
    if files != manifest.files.keys():
        raise BundleError("archive is missing declared runtime files/data")
    if destination is not None:
        for directory in sorted(destination.rglob("*"), reverse=True):
            if directory.is_dir():
                _sync_directory(directory)
        _sync_directory(destination)


def _inventory(directory: Path) -> dict[str, FileRecord]:
    result = {}
    for path in sorted(directory.rglob("*")):
        info = path.lstat()
        if stat.S_ISDIR(info.st_mode):
            continue
        _regular(path)
        name = _relative(path.relative_to(directory).as_posix())
        result[name] = FileRecord(sha256=_file_digest(path), size=info.st_size,
                                  executable=bool(info.st_mode & 0o111))
        if len(result) > _MAX_FILES:
            raise BundleError("bundle file limit exceeded")
    return result


def _root(path: Path) -> Path:
    require_test_path_within_sandbox(path, "portable runtime store")
    # Reuse the existing physical path, ownership and non-writable-root gate.
    return layout(path).root


@contextlib.contextmanager
def _locks(*roots: Path) -> Iterator[None]:
    # Same physical/store fences as runtime lifecycle operations; no lock bypass.
    from vq.admin import toolset_lifecycle_lock

    with toolset_lifecycle_lock([], action="vq-runtime-bundle", extra_resources=tuple(
        ("checkout", str(root)) for root in roots
    )):
        yield


def _store(root: Path, kind: str, *, create: bool) -> None:
    _root(root)
    marker = root / "format.json"
    if marker.exists():
        if json.loads(_read_json(marker)) != {"schema": kind}:
            raise BundleError("incompatible runtime store format")
        return
    if root.exists() and any(root.iterdir()):
        raise BundleError("refusing a nonempty unrecognized/legacy runtime root")
    if create:
        root.mkdir(parents=True, exist_ok=True)
        atomic_write_text(marker, _json({"schema": kind}).decode())


def _directory(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise BundleError(f"expected an ordinary directory: {path}")
    info = path.stat()
    if info.st_mode & 0o022 or (os.geteuid() != 0 and info.st_uid != os.geteuid()):
        raise BundleError(f"directory must be operator-owned and not group/world writable: {path}")


def _sync_directory(path: Path) -> None:
    with contextlib.suppress(OSError):
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def pack(directory: Path, description: Path, output: Path) -> BundleManifest:
    """Package already-built files. No compiler, installer or build script runs."""
    _directory(directory)
    directory = directory.resolve()
    output = _root(output)
    if output.is_relative_to(directory):
        raise BundleError("bundle output must be outside the runtime input directory")
    build = BuildDescription.model_validate_json(_read_json(description))
    inventory = _inventory(directory)
    with _locks(output):
        if output.exists():
            raise BundleError("bundle output already exists; it will not be overwritten")
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".vq-pack-", dir=output.parent) as tmp:
            staging = Path(tmp) / "bundle"
            staging.mkdir()
            archive = staging / "bundle.tar"
            with tarfile.open(archive, "w") as tar:
                for name, record in sorted(inventory.items()):
                    member = tarfile.TarInfo(name)
                    member.size = record.size
                    member.mode = 0o755 if record.executable else 0o644
                    with (directory / name).open("rb") as handle:
                        tar.addfile(member, handle)
            manifest = BundleManifest(build=build, files=inventory,
                                      archive_sha256=_file_digest(archive))
            _archive(manifest, archive)
            atomic_write_text(staging / "manifest.json", _json(manifest).decode())
            with archive.open("rb") as handle:
                os.fsync(handle.fileno())
            staging.rename(output)
            _sync_directory(output.parent)
    return manifest


def publish(bundle: Path, store: Path, trust: Path) -> BundleManifest:
    """Publish a verified copy with an immutable content identity.

    A signature beside the bundle's manifest is published with it. Publishing
    an identity again changes nothing, except that a signature which trusted
    the bundle here replaces the stored one.
    """
    manifest, signer = _trusted(bundle, trust)
    _archive(manifest, bundle / "bundle.tar")
    signature = bundle / SIGNATURE_FILE
    store = _root(store)
    with _locks(store):
        _store(store, "vq.runtime-artifact-store/1", create=True)
        artifacts = store / "artifacts"
        artifacts.mkdir(exist_ok=True)
        _directory(artifacts)
        final = artifacts / manifest.artifact_id
        if final.exists() or final.is_symlink():
            _directory(final)
            stored = BundleManifest.model_validate_json(_read_json(final / "manifest.json"))
            if stored != manifest:
                raise BundleError("published identity has different metadata")
            _archive(manifest, final / "bundle.tar")
            if signer is not None:
                atomic_write_text(final / SIGNATURE_FILE,
                                  _read_signature(signature).decode("ascii"))
            return manifest
        with tempfile.TemporaryDirectory(prefix=".vq-publish-", dir=artifacts) as tmp:
            staging = Path(tmp) / "artifact"
            staging.mkdir()
            shutil.copyfile(bundle / "bundle.tar", staging / "bundle.tar")
            _archive(manifest, staging / "bundle.tar")
            atomic_write_text(staging / "manifest.json", _json(manifest).decode())
            if signature.exists() or signature.is_symlink():
                atomic_write_text(staging / SIGNATURE_FILE,
                                  _read_signature(signature).decode("ascii"))
            with (staging / "bundle.tar").open("rb") as handle:
                os.fsync(handle.fileno())
            staging.rename(final)
            _sync_directory(artifacts)
    return manifest


class _NoBundleRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Request, fp: Any, code: int, msg: str,
                         headers: Any, newurl: str) -> None:
        # Fetch only the operator's exact HTTPS store; no downgrade or redirect.
        return None


def _https_store_url(url: str) -> str:
    try:
        url.encode("ascii")
        parsed = urlsplit(url)
        port = parsed.port
    except (UnicodeError, ValueError):
        raise BundleError("artifact store URL must be an ASCII HTTPS URL") from None
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.query or parsed.fragment
            or port == 0 or "\\" in url or any(ord(c) <= 32 or ord(c) == 127 for c in url)):
        raise BundleError("artifact store requires HTTPS without credentials, query or fragment")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _https_bearer_token(path: Path) -> str:
    """Read a bounded external credential without exposing paths or contents."""
    try:
        _regular(path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        with os.fdopen(os.open(path, flags), "rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise ValueError
            token = handle.read(_MAX_BEARER_TOKEN_BYTES + 3)
        # Accept one conventional line ending, never embedded whitespace or
        # controls that could turn a credential into another HTTP header.
        if token.endswith(b"\r\n"):
            token = token[:-2]
        elif token.endswith(b"\n"):
            token = token[:-1]
        if (len(token) > _MAX_BEARER_TOKEN_BYTES
                or not re.fullmatch(rb"[A-Za-z0-9._~+/-]+=*", token)):
            raise ValueError
        return token.decode("ascii")
    except (OSError, ValueError):
        raise BundleError(
            "HTTPS bearer token could not be loaded; use an owner-only regular file "
            "with one ASCII bearer token (at most 16 KiB)",
        ) from None


def _download(opener: OpenerDirector, url: str, destination: Path, *,
              limit: int, timeout: float, bearer_token: str | None = None) -> None:
    """Stream a bounded, complete response to unpublished operator-owned staging."""
    request = Request(url, headers={"Accept-Encoding": "identity"})
    if bearer_token is not None:
        request.add_unredirected_header("Authorization", f"Bearer {bearer_token}")
    try:
        with opener.open(request, timeout=timeout) as response, destination.open("xb") as output:
            if response.status != 200:
                raise BundleError("artifact download requires an HTTP 200 response")
            if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                raise BundleError("encoded artifact responses are unsupported")
            length = response.headers.get("Content-Length")
            if length is not None and (not length.isascii() or not length.isdecimal()):
                raise BundleError("invalid artifact Content-Length")
            expected = int(length) if length is not None else None
            if expected is not None and expected > limit:
                raise BundleError("artifact download exceeds the configured byte limit")
            total = 0
            while block := response.read1(min(1024 * 1024, limit - total + 1)):
                total += len(block)
                if total > limit:
                    raise BundleError("artifact download exceeds the configured byte limit")
                output.write(block)
            if expected is not None and total != expected:
                raise BundleError("artifact download ended before its declared length")
            output.flush()
            os.fsync(output.fileno())
    except (URLError, OSError, http.client.HTTPException):
        # Do not echo remote responses, URLs or proxy credentials into diagnostics.
        raise BundleError("HTTPS artifact transfer failed; no artifact was published") from None


#: ``download(name, destination, limit)`` writes the store's
#: ``artifacts/<id>/<name>`` to ``destination``, or raises BundleError.
_Download = Callable[[str, Path, int], None]


def _fetch_trusted(
    artifact_id: str, store: Path, trust: Path, download: _Download, *, max_archive_bytes: int,
) -> BundleManifest:
    """The transport-independent part of a fetch: trust first, then stage and publish.

    Nothing is requested unless the artifact is pinned or the policy names
    signing keys. An artifact already published here is verified and reused
    without the transport. Otherwise the manifest is authenticated, by its pin
    or by the signature fetched beside it, before the archive is requested;
    all land in private staging, and the normal publication gates decide.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("artifact ID must be a full SHA256 digest")
    if type(max_archive_bytes) is not int or not 0 < max_archive_bytes <= _MAX_ARCHIVE_BYTES:
        raise BundleError("download byte limit must be positive and at most 64 GiB plus 32 MiB")
    policy = TrustPolicy.model_validate_json(_read_json(trust))
    if (not any(artifact_id in pins for pins in policy.publishers.values())
            and not any(policy.signers.values())):
        raise BundleError("requested artifact is not pinned by the operator trust policy")
    store = _root(store)
    _store(store, "vq.runtime-artifact-store/1", create=False)
    final = store / "artifacts" / artifact_id
    if final.exists() or final.is_symlink():
        with _locks(store):
            directory, manifest, _signer = _artifact(store, artifact_id, trust)
            _archive(manifest, directory / "bundle.tar")
            return manifest
    with tempfile.TemporaryDirectory(prefix="vq-bundle-fetch-") as tmp:
        staging = Path(tmp)
        download("manifest.json", staging / "manifest.json", 2 * 1024**2)
        manifest = BundleManifest.model_validate_json(_read_json(staging / "manifest.json"))
        if manifest.artifact_id != artifact_id:
            raise BundleError("downloaded manifest does not match the requested artifact identity")
        # Only an unpinned manifest needs its signature, and only signers can use it.
        publisher = manifest.build.publisher
        if (artifact_id not in policy.publishers.get(publisher, [])
                and policy.signers.get(publisher)):
            download(SIGNATURE_FILE, staging / SIGNATURE_FILE, _MAX_SIGNATURE_BYTES)
        load_manifest(staging, trust)
        download("bundle.tar", staging / "bundle.tar", max_archive_bytes)
        # Recheck current trust and archive contents, then publish under the
        # existing store lock. No target selection or smoke execution here.
        return publish(staging, store, trust)


def fetch(
    base_url: str, artifact_id: str, store: Path, trust: Path, *,
    ca_file: Path | None = None, client_cert: Path | None = None,
    client_key: Path | None = None, bearer_token_file: Path | None = None,
    timeout: float = 30.0,
    max_archive_bytes: int = 4 * 1024**3,
) -> BundleManifest:
    """Fetch a pinned artifact over verified HTTPS, then use normal publication.

    Download only into private temporary staging, outside lifecycle locks.
    Repeated calls verify and reuse an already published artifact, even when
    the server is offline. A retry after interruption fetches the same identity
    afresh; no partial response is admitted as a resumable or trusted artifact.
    Optional PEM client credentials stay external and are loaded only when a
    download is needed. The private key must be unencrypted, either in the
    certificate PEM or in a separate client_key file.
    An optional bearer token is read once from an external private file when
    downloading; it is never added to the URL or published artifact.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("artifact ID must be a full SHA256 digest")
    base_url = _https_store_url(base_url)
    if not 0 < timeout <= 120:
        raise BundleError("download socket timeout must be in (0, 120] seconds")
    if client_key is not None and client_cert is None:
        raise BundleError("--client-key requires --client-cert")
    remote = f"{base_url}/artifacts/{artifact_id}"
    opener: list[OpenerDirector] = []
    bearer_token: str | None = None

    def download(name: str, destination: Path, limit: int) -> None:
        nonlocal bearer_token
        if not opener:
            if bearer_token_file is not None:
                bearer_token = _https_bearer_token(bearer_token_file)
            try:
                context = ssl.create_default_context(cafile=ca_file)
                if client_cert is not None:
                    # An explicit password prevents OpenSSL from prompting on
                    # stdin or a terminal when given an encrypted private key.
                    context.load_cert_chain(client_cert, client_key, password="")
            except (OSError, ValueError):
                # Credential paths and OpenSSL diagnostics may be private.
                raise BundleError(
                    "HTTPS TLS configuration failed; check CA and client certificate/key "
                    "PEM files (private key must be unencrypted)",
                ) from None
            opener.append(build_opener(HTTPSHandler(context=context), _NoBundleRedirect()))
        _download(opener[0], f"{remote}/{name}", destination, limit=limit, timeout=timeout,
                  bearer_token=bearer_token)

    return _fetch_trusted(artifact_id, store, trust, download,
                          max_archive_bytes=max_archive_bytes)


def _remote_store_path(path: str) -> str:
    """An absolute, plain remote store path, with no way to reach elsewhere."""
    if (not path.startswith("/") or len(path) > 1024 or not path.isascii()
            or any(ord(c) < 32 or ord(c) == 127 for c in path)
            or any(part in {".", ".."} for part in path.split("/"))):
        raise BundleError("remote store must be an absolute ASCII path without control "
                          "characters or . and .. components")
    return path.rstrip("/") or "/"


def _ssh_download(ssh_argv: list[str], remote_path: str, destination: Path, *,
                  limit: int, timeout: float) -> None:
    """Stream one remote file over SSH into private staging, bounded at both ends.

    The remote side sends at most ``limit + 1`` bytes (``head -c``), so an
    oversized file is noticed without transferring it whole. A read that
    waits longer than ``timeout`` seconds is a stalled transfer. The whole
    ssh process group is killed on any failure, and the remote's messages
    are not echoed: they can name hosts and paths.
    """
    command = f"head -c {limit + 1} -- {shlex.quote(remote_path)}"
    try:
        proc = subprocess.Popen(
            [*ssh_argv, command], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )
    except OSError:
        raise BundleError("SSH artifact transfer could not start; no artifact was published") \
            from None
    assert proc.stdout is not None
    try:
        with destination.open("xb") as output, selectors.DefaultSelector() as selector:
            selector.register(proc.stdout, selectors.EVENT_READ)
            total = 0
            while True:
                if not selector.select(timeout):
                    raise BundleError("SSH artifact transfer stalled; no artifact was published")
                block = os.read(proc.stdout.fileno(), 1024 * 1024)
                if not block:
                    break
                total += len(block)
                if total > limit:
                    raise BundleError("artifact download exceeds the configured byte limit")
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
        try:
            returncode = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise BundleError("SSH artifact transfer did not finish; no artifact was "
                              "published") from None
        if returncode != 0:
            raise BundleError(f"SSH artifact transfer failed (exit {returncode}): the remote "
                              "file could not be read; no artifact was published")
    finally:
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(proc.pid, signal.SIGKILL)
            proc.wait()
        proc.stdout.close()


def fetch_over_ssh(
    ssh_argv: list[str], remote_store: str, artifact_id: str, store: Path, trust: Path, *,
    timeout: float = 30.0, max_archive_bytes: int = 4 * 1024**3,
) -> BundleManifest:
    """Fetch a pinned artifact from a store on another host over SSH.

    ``ssh_argv`` is vq's ssh command for a configured host (its connection,
    keep-alive and batch-mode options); the remote store holds
    ``artifacts/<id>/{manifest.json,bundle.tar}`` as ``publish`` lays it out.
    The same gates as an HTTPS fetch apply: the pin is checked before any
    connection, a published artifact is reused offline, the manifest is
    authenticated before the archive is requested, and publication rechecks
    everything. SSH adds no trust: the bytes are as untrusted as any download.
    """
    remote_store = _remote_store_path(remote_store)
    if not 0 < timeout <= 120:
        raise BundleError("transfer inactivity timeout must be in (0, 120] seconds")
    base = f"{remote_store.rstrip('/')}/artifacts/{artifact_id}"

    def download(name: str, destination: Path, limit: int) -> None:
        _ssh_download(ssh_argv, f"{base}/{name}", destination, limit=limit, timeout=timeout)

    return _fetch_trusted(artifact_id, store, trust, download,
                          max_archive_bytes=max_archive_bytes)


def _artifact(
    store: Path, artifact_id: str, trust: Path,
) -> tuple[Path, BundleManifest, str | None]:
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("artifact ID must be a full SHA256 digest")
    store = _root(store)
    _store(store, "vq.runtime-artifact-store/1", create=False)
    directory = store / "artifacts" / artifact_id
    _directory(directory.parent)
    _directory(directory)
    manifest, signer = _trusted(directory, trust)
    if manifest.artifact_id != artifact_id:
        raise BundleError("artifact identity mismatch")
    return directory, manifest, signer


def _selection(root: Path) -> dict[str, Any]:
    path = root / "selection.json"
    if not path.exists() and not path.is_symlink():
        return {"schema": "vq.runtime-selection/1", "current": None, "previous": None}
    body = json.loads(_read_json(path))
    if not isinstance(body, dict) or set(body) != {"schema", "current", "previous"}:
        raise BundleError("invalid selection receipt")
    if body["schema"] != "vq.runtime-selection/1":
        raise BundleError("unsupported selection schema")
    for name in ("current", "previous"):
        value = body[name]
        if value is not None and (
            not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value)
        ):
            raise BundleError("invalid selection identity")
    return body


def generation_payload(root: Path, artifact_id: str) -> Path:
    """Where a deployed generation's payload lives under ``root``."""
    return root / "generations" / artifact_id / "payload"


def node_check_lines(root: str, artifact_id: str, *, fail: str) -> list[str]:
    """Shell lines a scheduler job runs before its command, on its node.

    The same proof ``verified_generation`` asks for, from tools a compute node
    has: ``deploy`` writes the manifest as canonical JSON, so its SHA256 is
    the generation's identity, and the verification receipt as canonical JSON
    with ``"verified":true``. No part may be a link, and the payload must not
    be group or world writable. Its owner is not checked: the deploying
    account may not be the job's. On any failure ``fail`` is called with the
    reason, and it must not return.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("a runtime artifact is named by its full SHA256 identity")
    generation = f"{root}/generations/{artifact_id}"
    receipt = f"{generation}/verification.json"
    return [
        f"__vq_rt_gen={shlex.quote(generation)}",
        "__vq_rt_check() {",
        '  for __vq_rt_path in "$__vq_rt_gen" "$__vq_rt_gen/payload"; do',
        '    [ ! -L "$__vq_rt_path" ] && [ -d "$__vq_rt_path" ] || '
        '{ __vq_rt_why="$__vq_rt_path is not a directory"; return 1; }',
        "  done",
        '  for __vq_rt_path in "$__vq_rt_gen/manifest.json" "$__vq_rt_gen/verification.json"; do',
        '    [ ! -L "$__vq_rt_path" ] && [ -f "$__vq_rt_path" ] || '
        '{ __vq_rt_why="$__vq_rt_path is not a regular file"; return 1; }',
        "  done",
        "  if command -v sha256sum >/dev/null 2>&1; then",
        '    __vq_rt_sum=$(sha256sum < "$__vq_rt_gen/manifest.json") || __vq_rt_sum=',
        "  elif command -v shasum >/dev/null 2>&1; then",
        '    __vq_rt_sum=$(shasum -a 256 < "$__vq_rt_gen/manifest.json") || __vq_rt_sum=',
        "  else",
        '    __vq_rt_why="no sha256sum or shasum to check its manifest"; return 1',
        "  fi",
        f'  [ "${{__vq_rt_sum%% *}}" = {artifact_id} ] || '
        '{ __vq_rt_why="its manifest is not that generation"; return 1; }',
        f"  grep -qF '\"schema\":\"vq.runtime-deployment/1\"' {shlex.quote(receipt)} && "
        f"grep -qF '\"verified\":true' {shlex.quote(receipt)} && "
        f"grep -qF '\"artifact_id\":\"{artifact_id}\"' {shlex.quote(receipt)} || "
        '{ __vq_rt_why="its deployment receipt does not record it verified"; return 1; }',
        # ls -ld: characters 6 and 9 are the group and world write bits.
        '  case $(ls -ld "$__vq_rt_gen/payload" | cut -c6,9) in *w*) '
        '__vq_rt_why="its payload is group or world writable"; return 1 ;; esac',
        "}",
        f'__vq_rt_check || {fail} "$__vq_rt_why"',
    ]


def verified_generation(root: Path, artifact_id: str | None = None) -> tuple[str, Path]:
    """A deployed generation of ``root`` its own verification proves, and its payload.

    ``artifact_id`` names one generation; None means the current selection.
    The generation's manifest must have that identity and its verification
    receipt must say it verified, as ``deploy`` leaves them. Errors name the
    generation, never a file's contents: in multi-user mode the root daemon
    reads a path the job's owner chose.
    """
    if artifact_id is None:
        try:
            current = _selection(root).get("current")
        except (OSError, ValueError):
            raise BundleError(f"{root} has no readable runtime selection") from None
        if not isinstance(current, str):
            raise BundleError(f"{root} has no selected runtime")
        artifact_id = current
    elif not re.fullmatch(r"[0-9a-f]{64}", artifact_id):
        raise BundleError("a runtime artifact is named by its full SHA256 identity")
    generation = root / "generations" / artifact_id
    refused = BundleError(f"runtime {artifact_id[:12]} is not a verified deployment in {root}")
    try:
        manifest = BundleManifest.model_validate_json(_read_json(generation / "manifest.json"))
        receipt = json.loads(_read_json(generation / "verification.json"))
        _directory(generation / "payload")
    except (OSError, ValueError):
        raise refused from None
    if manifest.artifact_id != artifact_id or not (
            isinstance(receipt, dict) and receipt.get("verified") is True):
        raise refused
    return artifact_id, generation_payload(root, artifact_id)


def deploy_plan(
    store: Path, artifact_id: str, root: Path, trust: Path, target: TargetProfile,
) -> dict[str, Any]:
    store, root = _root(store), _root(root)
    if store == root or store.is_relative_to(root) or root.is_relative_to(store):
        raise BundleError("artifact store and target must be independent roots")
    directory, manifest, signer = _artifact(store, artifact_id, trust)
    _archive(manifest, directory / "bundle.tar")
    root = _root(root)
    _store(root, "vq.portable-runtime-target/1", create=False)
    selected = _selection(root)
    plan: dict[str, Any] = {
        "schema": "vq.runtime-deploy-plan/1", "artifact_id": artifact_id,
        "build_id": manifest.build.build_id, "source_revision": manifest.build.source_revision,
        "target_profile_sha256": _digest(_json(target)),
        "generation": str(root / "generations" / artifact_id / "payload"),
        "current": selected["current"], "blockers": compatibility(manifest, target),
        "target_builds": 0, "smoke_required": True,
    }
    if signer is not None:
        plan["signer"] = signer
    return plan


def _smoke(payload: Path, build: BuildDescription, timeout: float) -> None:
    # Trusted product code runs as the invoking operator. This is not a sandbox.
    from vq.admin import _active_lifecycle_pass_fds

    env = dict(os.environ)
    for key in ("PYTHONPATH", "PYTHONHOME", "LD_PRELOAD", "LD_LIBRARY_PATH",
                "DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH", "DYLD_INSERT_LIBRARIES"):
        env.pop(key, None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    with subprocess.Popen(
        [str(payload / build.smoke_argv[0]), *build.smoke_argv[1:]], cwd=payload,
        env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, start_new_session=True,
        pass_fds=_active_lifecycle_pass_fds(),
    ) as process:
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise BundleError("runtime smoke check timed out") from exc
    if code:
        raise BundleError(f"runtime smoke check failed (exit {code})")


def deploy(
    store: Path, artifact_id: str, root: Path, trust: Path, target: TargetProfile,
    *, timeout: float = 30.0,
) -> dict[str, Any]:
    """Verify, relocate, smoke-test, and atomically select; never compile."""
    if not 0 < timeout <= 120:
        raise BundleError("smoke timeout must be in (0, 120] seconds")
    store, root = _root(store), _root(root)
    if store == root or store.is_relative_to(root) or root.is_relative_to(store):
        raise BundleError("artifact store and target must be independent roots")
    # Preflight before any target mutation. Recheck under both physical locks.
    plan = deploy_plan(store, artifact_id, root, trust, target)
    if plan["blockers"]:
        raise BundleError("; ".join(plan["blockers"]))
    with _locks(store, root):
        plan = deploy_plan(store, artifact_id, root, trust, target)
        if plan["blockers"]:
            raise BundleError("; ".join(plan["blockers"]))
        directory, manifest, _signer = _artifact(store, artifact_id, trust)
        _store(root, "vq.portable-runtime-target/1", create=True)
        generations = root / "generations"
        generations.mkdir(exist_ok=True)
        _directory(generations)
        generation = generations / artifact_id
        if generation.exists() or generation.is_symlink():
            _directory(generation)
        else:
            with tempfile.TemporaryDirectory(prefix=".vq-deploy-", dir=generations) as tmp:
                staging = Path(tmp) / "generation"
                payload = staging / "payload"
                payload.mkdir(parents=True)
                _archive(manifest, directory / "bundle.tar", payload)
                atomic_write_text(staging / "manifest.json", _json(manifest).decode())
                staging.rename(generation)
                _sync_directory(generations)
        # A crash after publication or lost receipt can safely replay these gates.
        if BundleManifest.model_validate_json(_read_json(generation / "manifest.json")) != manifest:
            raise BundleError("retained generation metadata mismatch")
        payload = generation / "payload"
        _directory(payload)
        if _inventory(payload) != manifest.files:
            raise BundleError("retained generation content mismatch")
        _smoke(payload, manifest.build, timeout)
        if _inventory(payload) != manifest.files:
            raise BundleError("smoke check changed the immutable payload")
        # One atomic receipt commits current and previous together. Old paths
        # remain valid forever here: jobs may be bound to any generation
        # (JobSpec.runtime_bundle, #161), and retention is later.
        selected = _selection(root)
        if selected["current"] != artifact_id:
            selected["previous"], selected["current"] = selected["current"], artifact_id
        receipt = {**plan, "schema": "vq.runtime-deployment/1", "verified": True}
        atomic_write_text(generation / "verification.json", _json(receipt).decode())
        atomic_write_text(root / "selection.json", _json(selected).decode())
        return receipt


# --- retention (#161) ---------------------------------------------------------

PRUNE_PLAN_SCHEMA = "vq.runtime-prune-plan/1"
PRUNE_SCHEMA = "vq.runtime-prune/1"
_PRUNING_PREFIX = ".pruning-"
_GENERATION = re.compile(r"[0-9a-f]{64}")


def _tree_bytes(path: Path) -> int:
    """Bytes of the regular files under ``path``, never following a link."""
    total = 0
    for directory, _dirs, files in os.walk(path):
        for name in files:
            info = os.lstat(os.path.join(directory, name))
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def prune_plan(root: Path, *, bound: dict[str, list[str]],
               keep: frozenset[str] = frozenset()) -> dict[str, Any]:
    """Which deployed generations ``root`` keeps, and why, and which it can drop.

    ``bound`` maps a generation to the queued or running jobs bound to it.
    The current and previous selections, bound generations and ``keep`` are
    kept; every other generation is prunable. Read-only.
    """
    root = _root(root)
    _store(root, "vq.portable-runtime-target/1", create=False)
    selected = _selection(root)
    generations = root / "generations"
    entries: list[dict[str, Any]] = []
    interrupted: list[str] = []
    other: list[str] = []
    names = sorted(os.listdir(generations)) if generations.is_dir() else []
    for name in names:
        path = generations / name
        if name.startswith(_PRUNING_PREFIX) and path.is_dir() and not path.is_symlink():
            interrupted.append(name)
            continue
        if not _GENERATION.fullmatch(name) or path.is_symlink() or not path.is_dir():
            other.append(name)  # deploy staging or something unknown: left alone
            continue
        reasons = [label for label, identity in (("current", selected["current"]),
                                                 ("previous", selected["previous"]))
                   if identity == name]
        reasons += [f"bound by job {job}" for job in sorted(bound.get(name, []))]
        if name in keep:
            reasons.append("kept by --keep")
        entries.append({"artifact_id": name, "action": "keep" if reasons else "prune",
                        "reasons": reasons, "bytes": _tree_bytes(path)})
    return {
        "schema": PRUNE_PLAN_SCHEMA, "root": str(root),
        "current": selected["current"], "previous": selected["previous"],
        "generations": entries, "interrupted": interrupted, "left_alone": other,
        "reclaimable_bytes": sum(e["bytes"] for e in entries if e["action"] == "prune"),
    }


def _remove_tree(path: Path) -> None:
    """Remove a generation's directory tree, never following a link.

    Payload directories are read-only by design, so each directory is made
    writable for its owner first; links are removed as links.
    """
    for directory, _dirs, _files in os.walk(path):
        os.chmod(directory, 0o700)  # os.walk yields real directories only
    shutil.rmtree(path)


def prune(root: Path, *, bound: Callable[[], dict[str, list[str]]],
          keep: frozenset[str] = frozenset()) -> dict[str, Any]:
    """Remove the generations :func:`prune_plan` finds prunable, under the root lock.

    Bindings are read again under the lock (``bound`` is called there). Each
    generation is first renamed out of ``generations/<id>``, so anything that
    resolves it afterwards fails closed instead of reading a half-removed
    tree, then removed. An interrupted prune's leftovers go too. The
    artifact store is untouched: ``deploy`` brings a generation back.
    """
    root = _root(root)
    with _locks(root):
        plan = prune_plan(root, bound=bound(), keep=keep)
        generations = root / "generations"
        removed: list[str] = []
        for entry in plan["generations"]:
            if entry["action"] != "prune":
                continue
            staging = generations / f"{_PRUNING_PREFIX}{entry['artifact_id']}-{os.getpid()}"
            os.rename(generations / entry["artifact_id"], staging)
            _sync_directory(generations)
            _remove_tree(staging)
            removed.append(entry["artifact_id"])
        for name in plan["interrupted"]:
            _remove_tree(generations / name)
        _sync_directory(generations)
    return {**plan, "schema": PRUNE_SCHEMA, "removed": removed,
            "removed_interrupted": list(plan["interrupted"])}
