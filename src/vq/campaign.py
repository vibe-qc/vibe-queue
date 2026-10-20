"""Artifact-aware calculation campaigns: graph, artifacts and progression (#165).

The pieces a campaign controller sits on, each usable and testable alone:

* a **campaign file** (``vq.campaign/1``): a small static DAG of ordinary vq
  job nodes. A node's inputs name another node's declared output; that is a
  success edge, because an artifact exists only after its producer succeeded.
  Explicit ``after`` edges add ``success`` or ``completion`` ordering. The file
  is validated before anything runs (references, cycles, unsafe or
  overlapping paths) and gets a stable digest;
* **node sources**: a directory of files a node's job needs, pinned in the
  campaign file by a content digest (``vq.campaign-sources/1``);
* an **artifact manifest** (``vq.campaign-artifact/1``), published from a
  producer's real output (relative paths, types, sizes, SHA256; symlinks and
  special files refused), and the verification of a staged copy that reports
  what is missing, partial, different, unexpected or unsafe;
* a **progression engine** over the controller's journal
  (``vq.campaign-journal/1``). Per node it reports ready, waiting, blocked
  with the reason, uncertain, running, succeeded or failed, and what to do
  next. Idempotency keys derive from the campaign digest, node and attempt,
  so an attempt that may have reached its target is reconciled under its
  own key, never sent under a new one.

Nothing here submits, transfers or deletes; :mod:`vq.campaign_controller`
acts on what this module reports. Jobs stay owned by their daemon or
scheduler driver; a campaign only orders them and tracks their artifacts.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CAMPAIGN_SCHEMA = "vq.campaign/1"
JOURNAL_SCHEMA = "vq.campaign-journal/1"
ARTIFACT_SCHEMA = "vq.campaign-artifact/1"
SOURCES_SCHEMA = "vq.campaign-sources/1"
STATUS_SCHEMA = "vq.campaign-status/1"

MAX_NODES = 64
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_ARTIFACT_ENTRIES = 100_000
_CHUNK = 1024 * 1024

Label = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")]
HostName = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")]
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Reference = Annotated[
    str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}\.[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
]

AttemptState = Literal["submitting", "accepted", "running", "succeeded", "failed", "cancelled"]
ArtifactStatus = Literal["verified", "missing", "partial", "digest_mismatch", "unexpected",
                         "unsafe"]
_TERMINAL: frozenset[str] = frozenset({"succeeded", "failed", "cancelled"})
#: Worst first: the status a verification reports when it finds several problems.
_SEVERITY = ("unsafe", "missing", "partial", "digest_mismatch", "unexpected")


class CampaignError(ValueError):
    """The campaign, journal or artifact cannot be used as it stands."""


def safe_relative_path(value: str) -> str:
    """``value`` if it is a plain relative POSIX path that stays inside its root."""
    if not value or len(value) > 1024 or "\\" in value or "\0" in value:
        raise ValueError(f"path {value!r} must be 1-1024 characters without '\\' or NUL")
    if value.startswith("/"):
        raise ValueError(f"path {value!r} must be relative")
    if any(part in ("", ".", "..") for part in value.split("/")):
        raise ValueError(f"path {value!r} must not contain empty, '.' or '..' components")
    return value


def _overlaps(a: str, b: str) -> bool:
    """Whether one path is the other or lies inside it, component-wise."""
    pa, pb = a.split("/"), b.split("/")
    shorter = min(len(pa), len(pb))
    return pa[:shorter] == pb[:shorter]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class Output(_Model):
    """A file or directory the node leaves in its workspace, published on success."""

    name: Label
    path: str
    kind: Literal["file", "directory"]

    check_path = field_validator("path")(safe_relative_path)


class Input(_Model):
    """Another node's output, staged into this node's workspace before submission."""

    name: Label
    source: Reference
    path: str

    check_path = field_validator("path")(safe_relative_path)

    @property
    def producer(self) -> str:
        return self.source.split(".", 1)[0]

    @property
    def output(self) -> str:
        return self.source.split(".", 1)[1]


class After(_Model):
    node: Label
    on: Literal["success", "completion"]


class Sources(_Model):
    """Files a node's job needs, copied into its workspace before its inputs.

    ``path`` is a directory relative to the campaign file's own directory;
    its contents land at the workspace root. ``sha256`` pins those contents
    (:func:`source_digest`), so the campaign digest covers the exact bytes a
    node runs and a changed script is a changed campaign.
    """

    path: str
    sha256: Digest

    check_path = field_validator("path")(safe_relative_path)


class Node(_Model):
    id: Label
    #: ``validate`` nodes check a result scientifically; a ``render`` node
    #: publishes one and must follow a passed validation.
    kind: Literal["build", "compute", "validate", "render", "other"]
    #: The logical target; unset leaves the choice to submission time.
    host: HostName | None = None
    program: Label | None = None
    #: The portable runtime generation the node runs on (#161), and the
    #: ``vq runtime-bundle deploy`` target on its host that holds it. A root
    #: needs a digest; a digest without a root still describes the campaign,
    #: but the controller cannot run it.
    runtime_digest: Digest | None = None
    runtime_root: Annotated[str, Field(min_length=2, max_length=4096, pattern=r"^/")] | None = None
    command: list[Annotated[str, Field(max_length=4096)]] = Field(min_length=1, max_length=256)
    cpus: Annotated[int, Field(ge=1)]
    mem_mb: Annotated[int, Field(ge=1)] | None = None
    wall_time_seconds: Annotated[int, Field(ge=1)] | None = None
    sources: Sources | None = None
    inputs: list[Input] = Field(default_factory=list, max_length=32)
    outputs: list[Output] = Field(default_factory=list, max_length=32)
    after: list[After] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def _consistent(self) -> Node:
        if self.runtime_root is not None and self.runtime_digest is None:
            raise ValueError(
                f"node {self.id}: runtime_root needs runtime_digest, the generation to bind"
            )
        for kind, names in (("input", [i.name for i in self.inputs]),
                            ("output", [o.name for o in self.outputs]),
                            ("after", [a.node for a in self.after])):
            if len(set(names)) != len(names):
                raise ValueError(f"node {self.id}: each {kind} may appear once")
        paths = [i.path for i in self.inputs] + [o.path for o in self.outputs]
        for index, first in enumerate(paths):
            for second in paths[index + 1:]:
                if _overlaps(first, second):
                    raise ValueError(
                        f"node {self.id}: paths {first!r} and {second!r} overlap; "
                        "each input and output needs its own place in the workspace"
                    )
        return self

    def edges(self) -> dict[str, str]:
        """Predecessor -> ``success`` or ``completion``."""
        edges = {i.producer: "success" for i in self.inputs}
        for edge in self.after:
            edges.setdefault(edge.node, edge.on)
        return edges


class Campaign(_Model):
    schema_name: Literal["vq.campaign/1"]
    campaign_id: Label
    nodes: list[Node] = Field(min_length=1, max_length=MAX_NODES)

    @model_validator(mode="after")
    def _graph(self) -> Campaign:
        ids = [n.id for n in self.nodes]
        if len(set(ids)) != len(ids):
            raise ValueError("each node id may appear once")
        nodes = {n.id: n for n in self.nodes}
        for node in self.nodes:
            for item in node.inputs:
                producer = nodes.get(item.producer)
                if producer is None or producer.id == node.id:
                    raise ValueError(f"node {node.id}: input {item.name} names "
                                     f"{'itself' if producer else 'unknown node'} "
                                     f"{item.producer!r}")
                if item.output not in {o.name for o in producer.outputs}:
                    raise ValueError(f"node {node.id}: {item.producer} declares no "
                                     f"output {item.output!r}")
            for edge in node.after:
                if edge.node not in nodes or edge.node == node.id:
                    raise ValueError(f"node {node.id}: after names "
                                     f"{'itself' if edge.node == node.id else 'unknown node'} "
                                     f"{edge.node!r}")
                if edge.on == "completion" and edge.node in {i.producer for i in node.inputs}:
                    raise ValueError(
                        f"node {node.id}: its inputs need {edge.node} to succeed, which a "
                        f"completion edge on {edge.node} contradicts"
                    )
        cycle = _find_cycle(nodes)
        if cycle:
            raise ValueError(f"the campaign has a cycle: {' -> '.join(cycle)}")
        for node in self.nodes:
            if node.kind == "render" and not _validated_upstream(node.id, nodes):
                raise ValueError(
                    f"node {node.id}: a render node needs a validate node upstream along "
                    "success edges; a successful exit is not a validated result"
                )
        return self

    def node(self, node_id: str) -> Node:
        return next(n for n in self.nodes if n.id == node_id)

    def order(self) -> list[str]:
        """A topological order; among ready nodes, by id, so it is stable."""
        nodes = {n.id: n for n in self.nodes}
        remaining = {n.id: set(n.edges()) for n in self.nodes}
        order: list[str] = []
        while remaining:
            ready = sorted(i for i, deps in remaining.items() if not deps & set(remaining))
            order.extend(ready)
            for node_id in ready:
                del remaining[node_id]
        assert len(order) == len(nodes)
        return order


def _find_cycle(nodes: dict[str, Node]) -> list[str] | None:
    colour: dict[str, int] = {}
    stack: list[str] = []

    def visit(node_id: str) -> list[str] | None:
        colour[node_id] = 1
        stack.append(node_id)
        for dep in sorted(nodes[node_id].edges()):
            if colour.get(dep) == 1:
                return [*stack[stack.index(dep):], dep]
            if dep not in colour:
                found = visit(dep)
                if found:
                    return found
        stack.pop()
        colour[node_id] = 2
        return None

    for node_id in sorted(nodes):
        if node_id not in colour:
            found = visit(node_id)
            if found:
                return found
    return None


def _validated_upstream(node_id: str, nodes: dict[str, Node]) -> bool:
    seen: set[str] = set()
    todo = [dep for dep, on in nodes[node_id].edges().items() if on == "success"]
    while todo:
        current = todo.pop()
        if current in seen:
            continue
        seen.add(current)
        if nodes[current].kind == "validate":
            return True
        todo.extend(dep for dep, on in nodes[current].edges().items() if on == "success")
    return False


def _canonical_digest(payload: object) -> str:
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def campaign_digest(campaign: Campaign) -> str:
    payload = campaign.model_dump(mode="json")
    for node in payload["nodes"]:
        # A node without sources digests as it did before the field existed,
        # so journals and keys of such campaigns stay valid; likewise a node
        # without a runtime root.
        if node["sources"] is None:
            del node["sources"]
        if node["runtime_root"] is None:
            del node["runtime_root"]
    return _canonical_digest(payload)


def idempotency_key(campaign_sha256: str, node_id: str, attempt: int) -> str:
    """The key an attempt is submitted under; the same after any restart."""
    return hashlib.sha256(
        f"{CAMPAIGN_SCHEMA}\0{campaign_sha256}\0{node_id}\0{attempt}".encode()
    ).hexdigest()


def _read_json(path: Path) -> object:
    try:
        with path.open("rb") as handle:
            data = handle.read(MAX_FILE_BYTES + 1)
    except OSError as exc:
        raise CampaignError(f"{path}: {exc.strerror or exc}") from None
    if len(data) > MAX_FILE_BYTES:
        raise CampaignError(f"{path}: larger than {MAX_FILE_BYTES} bytes")
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CampaignError(f"{path}: not UTF-8 JSON: {exc}") from None


def load_campaign(path: Path) -> Campaign:
    return Campaign.model_validate(_read_json(path))


# --- Artifacts ---------------------------------------------------------------

class Entry(_Model):
    path: str
    type: Literal["file", "directory"]
    size: Annotated[int, Field(ge=0)] | None = None
    sha256: Digest | None = None

    check_path = field_validator("path")(safe_relative_path)

    @model_validator(mode="after")
    def _typed(self) -> Entry:
        if (self.type == "file") != (self.size is not None and self.sha256 is not None):
            raise ValueError(f"entry {self.path}: a file has size and sha256, a directory neither")
        return self


class ExecutedRuntime(_Model):
    """The code a job ran with, as its queue recorded it at dispatch (#165).

    Copied from the job's ``program_runtime_pin``, or its bound runtime
    generation once its record proves it ran on it: what the queue resolved
    and authenticated, never what the campaign declared. It is a record; a
    node's declared ``runtime_digest`` is enforced by comparing it with this.
    """

    program: Annotated[str, Field(min_length=1, max_length=128)]
    version: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    #: The full git SHA the queue authenticated immediately before dispatch.
    git_sha: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")] | None = None
    #: An immutable artifact, such as a scheduler deployment's per-SHA launcher.
    artifact_identity: Annotated[str, Field(min_length=1, max_length=4096)] | None = None
    #: The immutable runtime slot (generation) the job was dispatched into.
    runtime_root: Annotated[str, Field(min_length=1, max_length=4096)] | None = None


class ArtifactManifest(_Model):
    schema_name: Literal["vq.campaign-artifact/1"]
    campaign_sha256: Digest
    node: Label
    attempt: Annotated[int, Field(ge=1)]
    output: Label
    runtime_digest: Digest | None = None
    #: The producing attempt's executed runtime, when its queue recorded one.
    executed_runtime: ExecutedRuntime | None = None
    kind: Literal["file", "directory"]
    #: A file artifact's own size and digest; None for a directory.
    size: Annotated[int, Field(ge=0)] | None = None
    sha256: Digest | None = None
    #: A directory artifact's contents, relative to it; empty for a file.
    entries: list[Entry] = Field(default_factory=list, max_length=MAX_ARTIFACT_ENTRIES)
    total_bytes: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def _consistent(self) -> ArtifactManifest:
        if self.kind == "file":
            if self.size is None or self.sha256 is None or self.entries:
                raise ValueError("a file artifact has size and sha256 and no entries")
            if self.total_bytes != self.size:
                raise ValueError("total_bytes must equal the file's size")
            return self
        if self.size is not None or self.sha256 is not None:
            raise ValueError("a directory artifact lists entries instead of size and sha256")
        paths = {e.path: e.type for e in self.entries}
        if len(paths) != len(self.entries):
            raise ValueError("each entry path may appear once")
        for entry in self.entries:
            parent = entry.path.rpartition("/")[0]
            if parent and paths.get(parent) != "directory":
                raise ValueError(f"entry {entry.path}: its directory {parent} is not listed")
        if self.total_bytes != sum(e.size or 0 for e in self.entries):
            raise ValueError("total_bytes must equal the sum of the file sizes")
        return self


def manifest_payload(manifest: ArtifactManifest) -> dict[str, Any]:
    """The manifest as written and hashed. Without an executed runtime it is
    byte for byte what it was before that field existed, so existing digests
    stay valid and older controllers still read it."""
    payload = manifest.model_dump(mode="json")
    if payload["executed_runtime"] is None:
        del payload["executed_runtime"]
    return payload


def manifest_digest(manifest: ArtifactManifest) -> str:
    return _canonical_digest(manifest_payload(manifest))


def load_manifest(path: Path) -> ArtifactManifest:
    return ArtifactManifest.model_validate(_read_json(path))


def _hash_regular_file(path: Path) -> tuple[int, str]:
    """Size and SHA256 of a regular file, refusing to follow a swapped-in link."""
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise CampaignError(f"{path} is not a regular file")
        digest = hashlib.sha256()
        size = 0
        while chunk := os.read(fd, _CHUNK):
            digest.update(chunk)
            size += len(chunk)
        return size, digest.hexdigest()
    finally:
        os.close(fd)


def _real_directory(root: Path, relative: str) -> Path:
    """``root/relative``, refusing a symlink at any component on the way."""
    current = root
    for part in relative.split("/"):
        current = current / part
        mode = os.lstat(current).st_mode
        if stat.S_ISLNK(mode):
            raise CampaignError(f"{current} is a symbolic link")
    return current


def _walk(directory: Path, prefix: str = "") -> Iterator[tuple[str, os.stat_result, Path]]:
    with os.scandir(directory) as listing:
        children = sorted(listing, key=lambda e: e.name)
    for child in children:
        relative = f"{prefix}{child.name}"
        info = child.stat(follow_symlinks=False)
        yield relative, info, Path(child.path)
        if stat.S_ISDIR(info.st_mode):
            yield from _walk(Path(child.path), relative + "/")


def publish_artifact(
    root: Path, campaign: Campaign, node_id: str, attempt: int, output_name: str,
    *, runtime_digest: str | None = None, executed_runtime: ExecutedRuntime | None = None,
) -> ArtifactManifest:
    """The manifest of ``node_id``'s output ``output_name`` under workspace ``root``.

    Refuses symbolic links and special files anywhere in the artifact: what a
    consumer receives must be exactly the bytes listed, inside its root.
    """
    try:
        node = campaign.node(node_id)
    except StopIteration:
        raise CampaignError(f"the campaign has no node {node_id!r}") from None
    output = next((o for o in node.outputs if o.name == output_name), None)
    if output is None:
        raise CampaignError(f"node {node_id} declares no output {output_name!r}")
    if attempt < 1:
        raise CampaignError("attempts are numbered from 1")
    try:
        target = _real_directory(root, output.path)
        info = os.lstat(target)
    except FileNotFoundError:
        raise CampaignError(f"output {output_name} is missing: {root / output.path}") from None
    common: dict[str, Any] = {
        "schema_name": ARTIFACT_SCHEMA, "campaign_sha256": campaign_digest(campaign),
        "node": node_id, "attempt": attempt, "output": output_name,
        "runtime_digest": runtime_digest, "kind": output.kind,
        "executed_runtime": (executed_runtime.model_dump(mode="json")
                             if executed_runtime is not None else None),
    }
    if output.kind == "file":
        if not stat.S_ISREG(info.st_mode):
            raise CampaignError(f"output {output_name} at {target} is not a regular file")
        size, digest = _hash_regular_file(target)
        return ArtifactManifest.model_validate(
            {**common, "size": size, "sha256": digest, "total_bytes": size})
    if not stat.S_ISDIR(info.st_mode):
        raise CampaignError(f"output {output_name} at {target} is not a directory")
    entries: list[dict[str, Any]] = []
    for relative, child_info, path in _walk(target):
        if len(entries) >= MAX_ARTIFACT_ENTRIES:
            raise CampaignError(f"output {output_name} has more than "
                                f"{MAX_ARTIFACT_ENTRIES} entries")
        if stat.S_ISDIR(child_info.st_mode):
            entries.append({"path": relative, "type": "directory"})
        elif stat.S_ISREG(child_info.st_mode):
            size, digest = _hash_regular_file(path)
            entries.append({"path": relative, "type": "file", "size": size, "sha256": digest})
        else:
            raise CampaignError(f"output {output_name} contains {relative}, which is not a "
                                "regular file or directory (symbolic links are refused)")
    return ArtifactManifest.model_validate({
        **common, "entries": entries, "total_bytes": sum(e.get("size", 0) for e in entries),
    })


def verify_artifact(staged: Path, manifest: ArtifactManifest) -> dict[str, object]:
    """Compare a staged copy with its manifest; ``verified`` only when identical."""
    problems: list[dict[str, str]] = []

    def problem(category: str, path: str, detail: str) -> None:
        problems.append({"category": category, "path": path, "detail": detail})

    def check_file(path: Path, label: str, size: int, digest: str) -> None:
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            problem("missing", label, "not present")
            return
        if not stat.S_ISREG(info.st_mode):
            problem("unsafe", label, "not a regular file")
            return
        if info.st_size < size:
            problem("partial", label, f"{info.st_size} of {size} bytes")
            return
        actual_size, actual = _hash_regular_file(path)
        if actual_size != size or actual != digest:
            problem("digest_mismatch", label, f"sha256 {actual[:12]}, expected {digest[:12]}")

    try:
        root_info = os.lstat(staged)
    except FileNotFoundError:
        root_info = None
    if root_info is None:
        problem("missing", ".", "the staged artifact does not exist")
    elif manifest.kind == "file":
        assert manifest.size is not None and manifest.sha256 is not None
        check_file(staged, ".", manifest.size, manifest.sha256)
    elif not stat.S_ISDIR(root_info.st_mode):
        problem("unsafe", ".", "not a real directory")
    else:
        expected = {e.path: e for e in manifest.entries}
        present: set[str] = set()
        for relative, info, _ in _walk(staged):
            present.add(relative)
            entry = expected.get(relative)
            if stat.S_ISLNK(info.st_mode) or not (stat.S_ISDIR(info.st_mode)
                                                   or stat.S_ISREG(info.st_mode)):
                problem("unsafe", relative, "not a regular file or directory")
            elif entry is None:
                problem("unexpected", relative, "not in the manifest")
            elif entry.type == "directory" and not stat.S_ISDIR(info.st_mode):
                problem("unsafe", relative, "expected a directory")
            elif entry.type == "file":
                assert entry.size is not None and entry.sha256 is not None
                check_file(staged / relative, relative, entry.size, entry.sha256)
        for path in sorted(set(expected) - present):
            problem("missing", path, "not present")
    status = next((c for c in _SEVERITY if any(p["category"] == c for p in problems)),
                  "verified")
    return {"status": status, "manifest_sha256": manifest_digest(manifest),
            "node": manifest.node, "attempt": manifest.attempt, "output": manifest.output,
            "problems": sorted(problems, key=lambda p: (p["path"], p["category"]))}


# --- Node sources ------------------------------------------------------------

def source_digest(root: Path, *, exclude: frozenset[str] = frozenset()) -> str:
    """The pin of a sources directory: every entry's path, type, size and SHA256.

    Symbolic links and special files are refused, as in an artifact. Paths in
    ``exclude`` (and everything under them) are left out, which lets a staged
    payload be checked against its pin with its inputs already in place.
    """
    if not root.is_dir() or root.is_symlink():
        raise CampaignError(f"sources {root} is not a directory")
    entries: list[dict[str, Any]] = []
    for relative, info, path in _walk(root):
        if any(relative == skip or relative.startswith(skip + "/") for skip in exclude):
            continue
        if len(entries) >= MAX_ARTIFACT_ENTRIES:
            raise CampaignError(f"sources {root} has more than {MAX_ARTIFACT_ENTRIES} entries")
        if stat.S_ISDIR(info.st_mode):
            entries.append({"path": relative, "type": "directory"})
        elif stat.S_ISREG(info.st_mode):
            size, digest = _hash_regular_file(path)
            entries.append({"path": relative, "type": "file", "size": size, "sha256": digest})
        else:
            raise CampaignError(f"sources {root} contain {relative}, which is not a regular "
                                "file or directory (symbolic links are refused)")
    return _canonical_digest({"schema_name": SOURCES_SCHEMA, "entries": entries})


def sources_dir(base: Path, sources: Sources) -> Path:
    """Where a node's sources live, refusing a symbolic link on the way."""
    try:
        return _real_directory(base, sources.path)
    except FileNotFoundError:
        raise CampaignError(f"sources {base / sources.path} do not exist") from None


def check_sources(campaign: Campaign, base: Path) -> dict[str, dict[str, object]]:
    """Each node's sources under ``base``: the pin, the actual digest, and whether
    they match. A node without sources is absent."""
    checked: dict[str, dict[str, object]] = {}
    for node in campaign.nodes:
        if node.sources is None:
            continue
        try:
            actual: str | None = source_digest(sources_dir(base, node.sources))
            problem = None if actual == node.sources.sha256 else (
                f"sources {node.sources.path} have sha256 {actual}, the campaign pins "
                f"{node.sources.sha256}")
        except (CampaignError, OSError) as exc:
            actual, problem = None, str(exc)
        checked[node.id] = {"path": node.sources.path, "sha256": node.sources.sha256,
                            "actual": actual, "problem": problem}
    return checked


# --- Journal and progression -------------------------------------------------

class JobRef(_Model):
    host: HostName
    job_id: Annotated[str, Field(min_length=1, max_length=128)]


class ArtifactRecord(_Model):
    manifest_sha256: Digest
    status: ArtifactStatus


class InputBinding(_Model):
    """Which producer attempt and manifest an attempt was staged from."""

    producer_attempt: Annotated[int, Field(ge=1)]
    manifest_sha256: Digest


class Attempt(_Model):
    node: Label
    attempt: Annotated[int, Field(ge=1)]
    retry_of: Annotated[int, Field(ge=1)] | None = None
    idempotency_key: Digest
    state: AttemptState
    job: JobRef | None = None
    inputs: dict[Label, InputBinding] = Field(default_factory=dict)
    artifacts: dict[Label, ArtifactRecord] = Field(default_factory=dict)
    #: What the job ran with, once its queue dispatched it (#165).
    runtime: ExecutedRuntime | None = None


class Journal(_Model):
    schema_name: Literal["vq.campaign-journal/1"]
    campaign_sha256: Digest
    attempts: list[Attempt] = Field(default_factory=list, max_length=10_000)


def journal_payload(journal: Journal) -> dict[str, Any]:
    """The journal as written. An attempt without a recorded runtime is
    written as before that field existed, so older controllers still read it."""
    payload = journal.model_dump(mode="json")
    for attempt in payload["attempts"]:
        if attempt["runtime"] is None:
            del attempt["runtime"]
    return payload


def load_journal(path: Path) -> Journal:
    return Journal.model_validate(_read_json(path))


def _attempts_by_node(
    campaign: Campaign, journal: Journal, digest: str,
) -> dict[str, list[Attempt]]:
    """Group and check the journal; any inconsistency refuses the whole plan."""
    if journal.campaign_sha256 != digest:
        raise CampaignError(
            f"the journal belongs to campaign {journal.campaign_sha256[:12]}, not this one "
            f"({digest[:12]}); a changed campaign starts a new journal"
        )
    nodes = {n.id: n for n in campaign.nodes}
    grouped: dict[str, list[Attempt]] = {n.id: [] for n in campaign.nodes}
    for attempt in journal.attempts:
        if attempt.node not in nodes:
            raise CampaignError(f"the journal names unknown node {attempt.node!r}")
        grouped[attempt.node].append(attempt)
    for node_id, attempts in grouped.items():
        attempts.sort(key=lambda a: a.attempt)
        node = nodes[node_id]
        for index, attempt in enumerate(attempts, start=1):
            where = f"{node_id} attempt {attempt.attempt}"
            if attempt.attempt != index:
                raise CampaignError(f"{node_id}: attempts must be numbered 1, 2, ... "
                                    f"without gaps or repeats")
            if attempt.idempotency_key != idempotency_key(digest, node_id, index):
                raise CampaignError(f"{where}: idempotency key does not derive from this "
                                    "campaign, node and attempt")
            if index == 1 and attempt.retry_of is not None:
                raise CampaignError(f"{where}: the first attempt retries nothing")
            if index > 1:
                previous = attempts[index - 2]
                if attempt.retry_of != index - 1:
                    raise CampaignError(f"{where}: must record retry_of {index - 1}")
                if previous.state not in {"failed", "cancelled"}:
                    raise CampaignError(f"{where}: only a failed or cancelled attempt is "
                                        f"retried, and attempt {index - 1} is {previous.state}")
            if attempt.state in {"accepted", "running", "succeeded"} and attempt.job is None:
                raise CampaignError(f"{where}: an {attempt.state} attempt names its job")
            unknown = (set(attempt.artifacts) - {o.name for o in node.outputs}
                       or set(attempt.inputs) - {i.name for i in node.inputs})
            if unknown:
                raise CampaignError(f"{where}: names undeclared {', '.join(sorted(unknown))}")
    return grouped


def _succeeded(attempts: list[Attempt]) -> Attempt | None:
    return attempts[-1] if attempts and attempts[-1].state == "succeeded" else None


def campaign_status(campaign: Campaign, journal: Journal) -> dict[str, object]:
    """Where every node stands, why, and the next action for each."""
    digest = campaign_digest(campaign)
    grouped = _attempts_by_node(campaign, journal, digest)
    status: dict[str, str] = {}
    report: list[dict[str, object]] = []

    for node_id in campaign.order():
        node = campaign.node(node_id)
        attempts = grouped[node_id]
        latest = attempts[-1] if attempts else None
        reasons: list[str] = []
        # Each unmet edge: whether it can still be met ("waiting") or not.
        unmet: list[tuple[str, str]] = []
        for producer, on in sorted(node.edges().items()):
            state = status[producer]
            if on == "success" and state != "succeeded":
                if state == "failed":
                    unmet.append(("blocked", f"needs {producer} to succeed; it failed"))
                elif state == "blocked":
                    unmet.append(("blocked", f"needs {producer} to succeed; it is blocked"))
                else:
                    unmet.append(("waiting", f"waits for {producer} to succeed"))
            elif on == "completion" and state not in {"succeeded", "failed"}:
                if state == "blocked":
                    unmet.append(("blocked", f"{producer} will not run (it is blocked); "
                                             "a completion edge needs it to finish"))
                else:
                    unmet.append(("waiting", f"waits for {producer} to finish"))
        artifact_problems: list[str] = []
        for item in node.inputs:
            producer_attempt = _succeeded(grouped[item.producer])
            if producer_attempt is None:
                continue
            record = producer_attempt.artifacts.get(item.output)
            if record is None:
                artifact_problems.append(f"artifact {item.source} is not published")
            elif record.status != "verified":
                artifact_problems.append(f"artifact {item.source} is {record.status}")

        if latest is not None:
            # A started attempt must have had everything it needed. A
            # success is final (a succeeded attempt is never retried); a
            # completion edge needs only that its producer finished once,
            # since a failed producer may since have been retried.
            gaps = list(artifact_problems)
            for producer, on in node.edges().items():
                attempts_of = grouped[producer]
                if on == "success" and _succeeded(attempts_of) is None:
                    gaps.append(f"{producer} had not succeeded")
                elif on == "completion" and not any(a.state in _TERMINAL for a in attempts_of):
                    gaps.append(f"{producer} had not finished")
            if gaps:
                raise CampaignError(f"{node_id} attempt {latest.attempt} started although "
                                    f"{'; '.join(sorted(gaps))}")
            for item in node.inputs:
                binding = latest.inputs.get(item.name)
                producer_attempt = _succeeded(grouped[item.producer])
                assert producer_attempt is not None
                record = producer_attempt.artifacts[item.output]
                if binding is None or (binding.producer_attempt, binding.manifest_sha256) != (
                        producer_attempt.attempt, record.manifest_sha256):
                    raise CampaignError(
                        f"{node_id} attempt {latest.attempt}: input {item.name} must be "
                        f"staged from {item.producer} attempt {producer_attempt.attempt} "
                        f"manifest {record.manifest_sha256[:12]}"
                    )
            state = {"submitting": "uncertain", "accepted": "running",
                     "running": "running"}.get(latest.state, latest.state)
            if state == "cancelled":
                state = "failed"
            key = latest.idempotency_key
            if state == "uncertain":
                action = {"action": "reconcile", "attempt": latest.attempt,
                          "idempotency_key": key,
                          "detail": "the submission may have reached its target: resend the "
                                    "same payload under this key, which returns the job "
                                    "already accepted; never under another key"}
            elif state == "running":
                action = {"action": "wait", "attempt": latest.attempt}
            elif state == "failed":
                action = {"action": "none", "detail": f"attempt {latest.attempt} "
                          f"{latest.state}; a retry is an explicit attempt "
                          f"{latest.attempt + 1}"}
            else:
                action = {"action": "none"}
                missing = [o.name for o in node.outputs
                           if latest.artifacts.get(o.name) is None
                           or latest.artifacts[o.name].status != "verified"]
                if missing:
                    action = {"action": "publish", "outputs": missing,
                              "detail": "publish and verify these artifacts"}
        elif any(kind == "blocked" for kind, _ in unmet) or artifact_problems:
            state, action = "blocked", {"action": "none"}
            reasons.extend(message for kind, message in unmet if kind == "blocked")
            reasons.extend(artifact_problems)
        elif unmet:
            state, action = "waiting", {"action": "none"}
            reasons.extend(message for _, message in unmet)
        else:
            state = "ready"
            stage = []
            for item in node.inputs:
                producer_attempt = _succeeded(grouped[item.producer])
                assert producer_attempt is not None
                stage.append({
                    "input": item.name, "from": item.source, "to": item.path,
                    "producer_attempt": producer_attempt.attempt,
                    "manifest_sha256":
                        producer_attempt.artifacts[item.output].manifest_sha256,
                })
            action = {"action": "submit", "attempt": 1,
                      "idempotency_key": idempotency_key(digest, node_id, 1),
                      "stage": stage}
        status[node_id] = state
        report.append({
            "node": node_id, "kind": node.kind, "host": node.host, "status": state,
            "reasons": reasons, "next": action,
            "attempts": [
                {"attempt": a.attempt, "state": a.state, "retry_of": a.retry_of,
                 "job": a.job.model_dump(mode="json") if a.job else None,
                 "inputs": {k: v.model_dump(mode="json") for k, v in sorted(a.inputs.items())},
                 "artifacts": {k: v.model_dump(mode="json")
                               for k, v in sorted(a.artifacts.items())},
                 "runtime": a.runtime.model_dump(mode="json") if a.runtime else None}
                for a in attempts
            ],
        })

    retain = []
    for node in campaign.nodes:
        done = _succeeded(grouped[node.id])
        if done is None:
            continue
        needed_by = sorted({c.id for c in campaign.nodes for i in c.inputs
                            if i.producer == node.id and status[c.id] != "succeeded"})
        if needed_by:
            retain.append({"node": node.id, "attempt": done.attempt, "needed_by": needed_by})

    nodes_done = all(
        status[n.id] == "succeeded" and all(
            (rec := grouped[n.id][-1].artifacts.get(o.name)) is not None
            and rec.status == "verified" for o in n.outputs)
        for n in campaign.nodes
    )
    if nodes_done:
        overall = "succeeded"
    elif any(s == "failed" for s in status.values()):
        overall = "failed"
    elif any(s == "blocked" for s in status.values()):
        overall = "blocked"
    else:
        overall = "in_progress"
    return {
        "schema_name": STATUS_SCHEMA, "campaign_id": campaign.campaign_id,
        "campaign_sha256": digest, "status": overall, "nodes": report,
        "retain": sorted(retain, key=lambda r: str(r["node"])),
    }


Report = dict[str, Any]


def to_json(payload: Report) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"


def runtime_text(runtime: dict[str, Any]) -> str:
    """One line for an executed runtime: program, version and short SHA."""
    parts = [str(runtime["program"])]
    if runtime.get("version"):
        parts.append(str(runtime["version"]))
    if runtime.get("git_sha"):
        parts.append(f"@ {str(runtime['git_sha'])[:12]}")
    elif runtime.get("artifact_identity") or runtime.get("runtime_root"):
        parts.append("(immutable runtime, no git SHA recorded)")
    return " ".join(parts)


def status_text(report: Report) -> str:
    lines = [f"campaign {report['campaign_id']} ({report['campaign_sha256'][:12]}): "
             f"{report['status']}", ""]
    rows = [("NODE", "KIND", "HOST", "STATUS", "NEXT")]
    for node in report["nodes"]:
        rows.append((node["node"], node["kind"], node["host"] or "-", node["status"],
                     node["next"]["action"]))
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]))]
    lines.extend("  ".join(r[i].ljust(widths[i]) for i in range(len(r))).rstrip() for r in rows)
    notes = [f"  {n['node']}: {reason}" for n in report["nodes"] for reason in n["reasons"]]
    notes += [f"  {n['node']}: {n['next']['detail']}" for n in report["nodes"]
              if n["next"].get("detail")]
    if notes:
        lines += ["", "Notes:", *notes]
    ran = [f"  {n['node']} attempt {a['attempt']}: {runtime_text(a['runtime'])}"
           for n in report["nodes"] if (a := (n["attempts"] or [None])[-1]) and a["runtime"]]
    if ran:
        lines += ["", "Ran with (as each job's queue recorded it):", *ran]
    if report["retain"]:
        lines += ["", "Retain (artifacts still referenced):"]
        lines += [f"  {r['node']} attempt {r['attempt']}: needed by {', '.join(r['needed_by'])}"
                  for r in report["retain"]]
    return "\n".join(lines) + "\n"
