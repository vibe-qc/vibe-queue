"""The campaign controller (#165): run a campaign to its end, restartably.

``vq campaign run`` drives a campaign's nodes through the ordinary queue,
with nobody prompting between stages. Each pass, in the order the
progression engine (:func:`vq.campaign.campaign_status`) gives:

* **observe** every accepted attempt with ``vq status HOST`` and record how
  its job ended: ``completed`` is a success, ``killed`` a cancellation, any
  other terminal state a failure;
* **publish** each declared output of a succeeded attempt: bring it back
  with ``vq fetch HOST JOB --name`` into the run directory, hash it there,
  copy it into the artifact store and verify the copy against that
  manifest. Only a verified artifact unblocks a consumer;
* **stage and submit** each ready node: copy its inputs from the store into
  a fresh payload directory and verify them, record the attempt as
  ``submitting``, then ``vq submit --idempotency-key`` the payload;
* **reconcile** an attempt left ``submitting`` (a crash, a lost answer, or
  an attempt ``vq campaign retry`` authorized): send the same payload under
  the same key again. The queue returns the job it already accepted under
  that key, or accepts it once; a changed payload is a hard conflict.

It repeats until nothing more can run, sleeping between passes while jobs
run. The journal is rewritten atomically after every change and the run
directory is locked, so the controller can be stopped at any point and
started again with the same command.

Retries and scientific judgement stay explicit. The controller never
retries a failed attempt (``vq campaign retry`` authorizes the next one),
and a node succeeds by its job's exit; whether a result is right is what
the campaign's validate nodes decide.

A node's ``sources`` directory, pinned by digest in the campaign file, is
copied into its payload and verified before its inputs are staged, and
again before every resend.

A node runs on the host it names, or on this machine without one; the
host must be this machine or a configured one. The storage policy is
controller relay: every artifact passes through the run directory, so a
producer and its consumer need not reach each other, only the controller's
host must reach both. A node declaring a runtime digest is bound to that
runtime-bundle generation under its ``runtime_root`` (#161), on a daemon
host or a scheduler target, and nothing its job produced is published
until the job's own record confirms it ran on it: a daemon job once
started, a scheduler job, whose node verifies the generation, once it
completed with exit code 0.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from vq.campaign import (
    JOURNAL_SCHEMA,
    ArtifactManifest,
    ArtifactRecord,
    Attempt,
    Campaign,
    CampaignError,
    ExecutedRuntime,
    InputBinding,
    JobRef,
    Journal,
    Node,
    campaign_digest,
    campaign_status,
    check_sources,
    idempotency_key,
    journal_payload,
    load_journal,
    load_manifest,
    manifest_digest,
    manifest_payload,
    publish_artifact,
    runtime_text,
    source_digest,
    sources_dir,
    to_json,
    verify_artifact,
)
from vq.host import is_local_host
from vq.spec import JOB_NAME_MAX_LEN, JobState

JOURNAL_NAME = "journal.json"
LOCK_NAME = "controller.lock"
DEFAULT_INTERVAL_SECONDS = 30.0
SUBMIT_TIMEOUT_SECONDS = 600.0
STATUS_TIMEOUT_SECONDS = 120.0
FETCH_TIMEOUT_SECONDS = 3600.0

#: How a job's queue state reads as an attempt state. Every other terminal
#: state is a failure; a non-terminal state not listed leaves the attempt
#: as it is.
_ATTEMPT_STATES = {
    JobState.PENDING.value: "accepted",
    JobState.SUBMITTING.value: "accepted",
    JobState.SUBMIT_OUTCOME_UNKNOWN.value: "accepted",
    JobState.RUNNING.value: "running",
    JobState.SUSPENDED.value: "running",
    JobState.COMPLETED.value: "succeeded",
    JobState.KILLED.value: "cancelled",
}
_TERMINAL_JOB_STATES = frozenset({
    "completed", "failed", "killed", "interrupted", "oom_killed", "starved",
    "time_exceeded", "aborted_by_queue",
})
#: An attempt only moves forward: a job seen pending after it was seen
#: running (a resume) does not make the attempt "accepted" again.
_PROGRESS = {"submitting": 0, "accepted": 1, "running": 2,
             "succeeded": 3, "failed": 3, "cancelled": 3}
#: The resource receipt's error when a scheduler job's node refused the
#: runtime generation it is bound to, its command not started (#161).
_GENERATION_REFUSED = "runtime_generation_unavailable"


class Transient(Exception):
    """The queue could not answer this pass; the controller asks again next pass."""


class SubmitUncertain(Transient):
    """The submission may or may not have been accepted; resend under its key."""


class SubmitRefused(CampaignError):
    """The queue refused the submission before accepting it."""


class ObserveError(Transient):
    """The job's state could not be read this pass."""


@dataclass(frozen=True)
class Observation:
    """What the queue says about one job."""

    state: str
    workspace: Path | None
    detail: str | None = None
    #: The queue's own state (``completed``, ``oom_killed``, ...), and the
    #: job's ``resource_usage`` once it finished, when the queue reports one.
    job_state: str | None = None
    usage: dict[str, Any] | None = None
    #: The runtime the queue resolved for the job at dispatch (#165).
    runtime: ExecutedRuntime | None = None
    #: A scheduler job bound to a runtime generation whose node has not yet
    #: been seen to verify it: ``runtime`` leaves the generation out until
    #: the job completes with exit code 0.
    generation_unconfirmed: bool = False


class Queue(Protocol):
    """The three queue operations the controller needs, on any configured host."""

    def submit(self, node: Node, payload: Path, key: str, job_name: str) -> JobRef: ...

    def observe(self, job: JobRef) -> Observation: ...

    def fetch(self, job: JobRef, name: str, into: Path) -> Path:
        """Copy the top-level workspace entry ``name`` of a finished job into
        ``into``; return where it landed."""
        ...


class FetchMissing(CampaignError):
    """The job's workspace has no such entry."""


class FetchError(Transient):
    """The copy could not be made this pass."""


@dataclass
class CliQueue:
    """The queue on each node's host, through the `vq submit`, `vq status` and
    `vq fetch` a user runs. A node without a host runs on this machine."""

    vq: tuple[str, ...] = (sys.executable, "-m", "vq")
    retained_runtimes: bool = False

    def _run(self, argv: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [*self.vq, *argv], capture_output=True, text=True, timeout=timeout,
            stdin=subprocess.DEVNULL, check=False,
        )

    def submit(self, node: Node, payload: Path, key: str, job_name: str) -> JobRef:
        host = node.host or "localhost"
        argv = ["submit", host, "--dir", str(payload), "--cpus", str(node.cpus),
                "--job-name", job_name, "--idempotency-key", key, "--json"]
        if node.mem_mb is not None:
            argv += ["--mem-mb", str(node.mem_mb)]
        if node.wall_time_seconds is not None:
            argv += ["--wall-time-seconds", str(node.wall_time_seconds)]
        if node.program is not None:
            argv += ["--program", node.program]
        if node.program is not None and self.retained_runtimes:
            # An immutable command is a request, never its own attestation.
            # Submit authenticates its provider, native/content identity and
            # exact launcher, and reserves it before accepting this same key.
            launcher = Path(node.command[0])
            match = re.fullmatch(
                re.escape(node.program)
                + r"-.+-([0-9a-f]{40})(?:-sha256-[0-9a-f]{64})?(?:-store)?-python",
                launcher.name,
            )
            if launcher.is_absolute() and match is not None:
                argv += ["--managed-generation", match.group(1)]
            elif (launcher.is_absolute() and launcher.name == "python"
                  and launcher.parent.name == "bin"
                  and launcher.parent.parent.name == ".venv"
                  and launcher.parent.parent.parent.name == "source"
                  and launcher.parent.parent.parent.parent.parent.name == "releases"
                  and re.fullmatch(r"[0-9a-f]{40}", launcher.parent.parent.parent.parent.name)):
                argv += ["--managed-generation", launcher.parent.parent.parent.parent.name]
        if node.runtime_root is not None and node.runtime_digest is not None:
            # The queue binds exactly this generation, or refuses (#161).
            argv += ["--runtime-root", node.runtime_root,
                     "--runtime-artifact", node.runtime_digest]
        argv += ["--", *node.command]
        try:
            proc = self._run(argv, SUBMIT_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            raise SubmitUncertain(
                f"vq submit did not answer within {SUBMIT_TIMEOUT_SECONDS:.0f} s") from None
        except OSError as exc:
            raise SubmitUncertain(f"vq submit could not run: {exc}") from None
        receipt: Any = None
        with contextlib.suppress(ValueError):
            receipt = json.loads(proc.stdout or "null")
        if proc.returncode == 0:
            jobids = receipt.get("jobids") if isinstance(receipt, dict) else None
            if (not isinstance(jobids, list) or len(jobids) != 1
                    or not isinstance(jobids[0], str) or not jobids[0]):
                raise SubmitUncertain(f"vq submit answered without one job ID: {proc.stdout!r}")
            # The receipt names the host to ask about the job: the one the
            # node named, also for a scheduler lane that a driver serves.
            answered = receipt.get("host")
            try:
                return JobRef(host=answered if isinstance(answered, str) else host,
                              job_id=jobids[0])
            except ValueError:
                raise SubmitUncertain(f"vq submit answered an unusable host {answered!r}") \
                    from None
        message = _last_line(proc.stderr) or f"exit {proc.returncode}"
        if isinstance(receipt, dict) and receipt.get("outcome") == "unknown":
            raise SubmitUncertain(f"vq submit outcome unknown: {message}")
        if proc.returncode == 2:
            # Click's usage error: refused while checking the request, before
            # the queue accepted anything (a key reused for another intent
            # lands here too).
            raise SubmitRefused(f"vq submit refused: {message}")
        raise SubmitUncertain(f"vq submit exited {proc.returncode}: {message}")

    def observe(self, job: JobRef) -> Observation:
        where = f"vq status {job.host} {job.job_id}"
        try:
            proc = self._run(["status", job.host, job.job_id, "--json"], STATUS_TIMEOUT_SECONDS)
        except (subprocess.TimeoutExpired, OSError) as exc:
            raise ObserveError(f"{where}: {exc}") from None
        if proc.returncode != 0:
            raise ObserveError(f"{where}: {_last_line(proc.stderr) or proc.returncode}")
        try:
            payload = json.loads(proc.stdout)
        except ValueError:
            raise ObserveError(f"{where} printed no JSON") from None
        if not isinstance(payload, dict):
            raise ObserveError(f"{where} printed no JSON object")
        return observation_from_status(payload)

    def fetch(self, job: JobRef, name: str, into: Path) -> Path:
        where = f"vq fetch {job.host} {job.job_id} --name {name}"
        try:
            proc = self._run(["fetch", job.host, job.job_id, "--name", name, "-o", str(into),
                              "--json"], FETCH_TIMEOUT_SECONDS)
        except (subprocess.TimeoutExpired, OSError) as exc:
            raise FetchError(f"{where}: {exc}") from None
        if proc.returncode == 2:
            raise FetchMissing(f"{where}: {_last_line(proc.stderr) or 'refused'}")
        if proc.returncode != 0:
            raise FetchError(f"{where}: {_last_line(proc.stderr) or proc.returncode}")
        try:
            result = json.loads(proc.stdout)
        except ValueError:
            raise FetchError(f"{where} printed no JSON") from None
        landed = result.get("destination") if isinstance(result, dict) else None
        if not isinstance(landed, str) or Path(landed) != into / name:
            raise FetchError(f"{where} landed at {landed!r}, not {into / name}")
        return into / name


def _last_line(text: str | None) -> str:
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _generation_ran(payload: dict[str, Any]) -> bool:
    """Whether a bound job's ``vq status --json`` payload proves its command ran
    on the generation it is bound to (#161).

    A daemon verifies the generation before it starts the job, so a start is
    proof. A scheduler job's start is recorded when it is handed to the
    scheduler, before its node checks the generation; the job script exits
    126 without starting the command when the check fails, so only a
    completed job with exit code 0 proves the check passed.
    """
    if payload.get("scheduler_target") is not None:
        return payload.get("state") == JobState.COMPLETED.value and payload.get("exit_code") == 0
    return isinstance(payload.get("started_at"), str) and bool(payload["started_at"])


def executed_runtime(payload: dict[str, Any]) -> ExecutedRuntime | None:
    """The runtime a job ran with, from its ``vq status --json`` payload.

    Only what the queue resolved for a dispatched job counts: the git SHA it
    authenticated or the immutable slot it chose, or the portable runtime
    generation the job is bound to (#161) once its record proves the command
    ran on it (:func:`_generation_ran`); that generation's identity is then
    ``artifact_identity`` and its payload ``runtime_root``. A job with
    neither, one not dispatched yet, or a value that does not validate
    records nothing rather than a guess.
    """
    raw_pin = payload.get("program_runtime_pin")
    pin = raw_pin if isinstance(raw_pin, dict) else {}
    raw_bundle = payload.get("runtime_bundle")
    bundle = raw_bundle if isinstance(raw_bundle, dict) and _generation_ran(payload) else None
    if not (pin.get("resolved_git_sha") or pin.get("resolved_runtime_root") or bundle):
        return None
    record = {
        "program": payload.get("program"),
        "version": pin.get("program_version"),
        "git_sha": pin.get("resolved_git_sha"),
        "artifact_identity": pin.get("artifact_identity"),
        "runtime_root": pin.get("resolved_runtime_root"),
    }
    if bundle is not None:
        artifact, root = bundle.get("artifact_id"), bundle.get("root")
        if not isinstance(artifact, str) or not isinstance(root, str):
            return None
        record.update(program=record["program"] or "runtime-bundle",
                      artifact_identity=artifact,
                      runtime_root=f"{root.rstrip('/')}/generations/{artifact}/payload")
    try:
        return ExecutedRuntime.model_validate(record)
    except ValueError:
        return None


def observation_from_status(payload: dict[str, Any]) -> Observation:
    """An :class:`Observation` from a ``vq status --json`` payload."""
    state = payload.get("state")
    cwd = payload.get("cwd")
    workspace = Path(cwd) if isinstance(cwd, str) and cwd else None
    if not isinstance(state, str):
        return Observation("unknown", workspace, "vq status reported no state")
    raw_usage = payload.get("resource_usage")
    usage = raw_usage if isinstance(raw_usage, dict) else None
    runtime = executed_runtime(payload)
    unconfirmed = (payload.get("scheduler_target") is not None
                   and isinstance(payload.get("runtime_bundle"), dict)
                   and not _generation_ran(payload))

    def seen(attempt_state: str, detail: str | None = None) -> Observation:
        return Observation(attempt_state, workspace, detail, job_state=state, usage=usage,
                           runtime=runtime, generation_unconfirmed=unconfirmed)

    exit_code = payload.get("exit_code")
    if state == JobState.COMPLETED.value and exit_code not in (None, 0):
        return seen("failed", f"completed with exit code {exit_code}")
    if state in _ATTEMPT_STATES:
        return seen(_ATTEMPT_STATES[state])
    if state in _TERMINAL_JOB_STATES:
        reason = payload.get("failure_reason")
        if not isinstance(reason, str) and usage is not None and usage.get("error") == (
                _GENERATION_REFUSED):
            # Its node refused the bound generation, and the job script's one
            # stderr line says why (#161).
            stderr = payload.get("stderr")
            reason = (_last_line(stderr if isinstance(stderr, str) else None)
                      or "its node refused the bound runtime generation")
        return seen("failed", f"job {state}" + (f": {reason}" if isinstance(reason, str) else ""))
    return seen("unknown", f"unrecognised job state {state!r}")


def preflight(
    campaign: Campaign, sources_root: Path | None = None,
    hosts: frozenset[str] | None = None, scheduler_hosts: frozenset[str] = frozenset(),
) -> list[str]:
    """Why this controller cannot run the campaign; empty when it can.

    ``sources_root`` is the campaign file's directory, which node sources
    are relative to. Every pin is checked here, before anything is staged.
    ``hosts`` are the configured host names; a node's host must be one of
    them or this machine. ``scheduler_hosts`` are the configured scheduler
    targets, whose ``runtime_root`` is a path on the cluster: it is checked
    by the rule ``vq submit`` applies there, so a node that would be refused
    is refused before anything upstream of it runs.
    """
    from vq.submit import _validate_runtime_bundle_request

    problems = []
    if sources_root is not None:
        problems += [f"node {node_id}: {item['problem']}"
                     for node_id, item in check_sources(campaign, sources_root).items()
                     if item["problem"]]
    elif any(node.sources is not None for node in campaign.nodes):
        problems.append("the campaign declares sources but no directory to find them in")
    for node in campaign.nodes:
        if (node.host is not None and not is_local_host(node.host)
                and node.host not in (hosts or frozenset())):
            problems.append(
                f"node {node.id}: host {node.host} is neither this machine nor a configured "
                "host")
        if node.runtime_digest is not None and node.runtime_root is None:
            problems.append(
                f"node {node.id}: declares runtime_digest without runtime_root, the "
                "`vq runtime-bundle deploy` target on its host the queue binds it from")
        elif node.runtime_root is not None:
            try:
                _validate_runtime_bundle_request(
                    node.runtime_root, node.runtime_digest,
                    scheduler_target=node.host if node.host in scheduler_hosts else None)
            except ValueError as exc:
                problems.append(f"node {node.id}: vq submit would refuse its runtime_root: {exc}")
    return problems


def job_name(campaign_id: str, node_id: str, attempt: int) -> str:
    """The job name an attempt is submitted under; the same after any restart."""
    name = f"{campaign_id}.{node_id}.a{attempt}"
    if len(name) > JOB_NAME_MAX_LEN:
        name = f"{node_id}.a{attempt}"[:JOB_NAME_MAX_LEN]
    return name


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_atomic(path: Path, text: str) -> None:
    tmp = path.with_name(f".{path.name}.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path)
    elif path.exists() or path.is_symlink():
        path.unlink()


def _copy(source: Path, dest: Path) -> None:
    """Copy a file or directory without following any symbolic link.

    A link is copied as a link, so the verification that always follows
    reports it as unsafe instead of the copy reading through it.
    """
    if source.is_dir() and not source.is_symlink():
        shutil.copytree(source, dest, symlinks=True)
    else:
        shutil.copy2(source, dest, follow_symlinks=False)


def _size(path: Path) -> int:
    """Bytes under ``path``, without following a symbolic link."""
    if path.is_symlink() or not path.is_dir():
        return path.lstat().st_size
    return sum(child.lstat().st_size for child in path.rglob("*")
               if child.is_symlink() or child.is_file())


def apply_cleanup(plan: list[dict[str, Any]]) -> int:
    """Remove every path of a cleanup plan; return the bytes freed."""
    freed = 0
    for item in plan:
        path = Path(item["path"])
        if path.exists() or path.is_symlink():
            _remove(path)
            freed += int(item["bytes"])
    return freed


@contextlib.contextmanager
def locked(run_dir: Path) -> Iterator[None]:
    """Hold the run directory for one controller at a time."""
    run_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(run_dir / LOCK_NAME,
                 os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CampaignError(
                f"another controller is running this campaign ({run_dir / LOCK_NAME} is held)"
            ) from None
        yield
    finally:
        os.close(fd)


@dataclass
class Controller:
    """One campaign's controller over one run directory. Hold :func:`locked` around it."""

    campaign: Campaign
    run_dir: Path
    queue: Queue
    log: Callable[[str], None] = lambda _message: None
    #: The campaign file's directory; node sources are relative to it.
    sources_root: Path | None = None
    journal: Journal = field(init=False)
    digest: str = field(init=False)
    #: Problems that need someone to act, by node. A node with one is not
    #: tried again until the controller restarts.
    problems: dict[str, str] = field(default_factory=dict)
    #: Nodes whose queue call failed this pass; they are asked again next pass.
    transient: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.digest = campaign_digest(self.campaign)
        path = self.run_dir / JOURNAL_NAME
        if path.exists():
            self.journal = load_journal(path)
        else:
            self.journal = Journal.model_validate(
                {"schema_name": JOURNAL_SCHEMA, "campaign_sha256": self.digest})
        self.status()  # refuses a journal that does not fit this campaign

    # --- journal ----------------------------------------------------------

    def status(self) -> dict[str, Any]:
        return campaign_status(self.campaign, self.journal)

    def save(self) -> None:
        self.journal = Journal.model_validate(self.journal.model_dump(mode="json"))
        self.status()
        _write_atomic(self.run_dir / JOURNAL_NAME,
                      to_json(journal_payload(self.journal)))

    def _attempt(self, node_id: str, number: int) -> Attempt:
        return next(a for a in self.journal.attempts
                    if a.node == node_id and a.attempt == number)

    def _replace(self, attempt: Attempt, **changes: Any) -> Attempt:
        updated = Attempt.model_validate({**attempt.model_dump(mode="json"), **changes})
        self.journal.attempts[self.journal.attempts.index(attempt)] = updated
        self.save()
        return updated

    # --- run directory ----------------------------------------------------

    def payload_dir(self, node_id: str, attempt: int) -> Path:
        return self.run_dir / "payloads" / node_id / str(attempt)

    def artifact_path(self, node_id: str, attempt: int, output: str) -> Path:
        return self.run_dir / "artifacts" / node_id / str(attempt) / output

    def _manifest(self, node_id: str, attempt: int, output: str, sha256: str) -> ArtifactManifest:
        path = self.artifact_path(node_id, attempt, output).with_name(f"{output}.manifest.json")
        manifest = load_manifest(path)
        if manifest_digest(manifest) != sha256:
            raise CampaignError(f"{path} does not match the journal's manifest {sha256[:12]}")
        return manifest

    def _sources(self, node: Node) -> Path:
        assert node.sources is not None
        if self.sources_root is None:
            raise CampaignError(f"node {node.id} declares sources but the controller has no "
                                "directory to find them in")
        return sources_dir(self.sources_root, node.sources)

    def _verify_payload(self, node: Node, payload: Path,
                        bindings: dict[str, InputBinding]) -> None:
        """The payload is exactly the node's pinned sources plus its staged inputs."""
        for item in node.inputs:
            binding = bindings[item.name]
            manifest = self._manifest(item.producer, binding.producer_attempt, item.output,
                                      binding.manifest_sha256)
            result = verify_artifact(payload / item.path, manifest)
            if result["status"] != "verified":
                raise CampaignError(f"staged input {item.name} is {result['status']}")
        if node.sources is None:
            return
        # Leave out the inputs and the directories made only to hold them.
        source = self._sources(node)
        exclude = {item.path for item in node.inputs}
        for item in node.inputs:
            parts = item.path.split("/")
            exclude.update(ancestor for ancestor in ("/".join(parts[:k])
                                                     for k in range(1, len(parts)))
                           if not (source / ancestor).is_dir())
        if source_digest(payload, exclude=frozenset(exclude)) != node.sources.sha256:
            raise CampaignError(f"staged sources of {node.id} no longer match their pin "
                                f"{node.sources.sha256[:12]}")

    def stage(self, node: Node, attempt: int) -> dict[str, InputBinding]:
        """Copy and verify ``node``'s inputs into the payload of ``attempt``.

        Each input comes from its producer's succeeded attempt and verified
        manifest. A payload left by a crash before its attempt was recorded
        was never sent, so it is rebuilt.
        """
        bindings: dict[str, InputBinding] = {}
        for item in node.inputs:
            attempts = [a for a in self.journal.attempts if a.node == item.producer]
            done = attempts[-1] if attempts and attempts[-1].state == "succeeded" else None
            record = done.artifacts.get(item.output) if done else None
            if done is None or record is None or record.status != "verified":
                raise CampaignError(f"input {item.name}: {item.source} has no verified artifact")
            bindings[item.name] = InputBinding(producer_attempt=done.attempt,
                                               manifest_sha256=record.manifest_sha256)
        payload = self.payload_dir(node.id, attempt)
        staging = payload.with_name(f".{attempt}.staging")
        for leftover in (payload, staging):
            _remove(leftover)
        staging.mkdir(parents=True)
        if node.sources is not None:
            for child in sorted(self._sources(node).iterdir()):
                _copy(child, staging / child.name)
            if source_digest(staging) != node.sources.sha256:
                raise CampaignError(f"the copied sources of {node.id} do not match their pin "
                                    f"{node.sources.sha256[:12]}")
            for output in node.outputs:
                if os.path.lexists(staging / output.path):
                    raise CampaignError(
                        f"output {output.name} at {output.path} is already a source file; "
                        "what the job writes could not be told apart from it")
        for item in node.inputs:
            binding = bindings[item.name]
            source = self.artifact_path(item.producer, binding.producer_attempt, item.output)
            dest = staging / item.path
            if os.path.lexists(dest):
                raise CampaignError(f"input {item.name} at {item.path} collides with a "
                                    "source file")
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
            except (FileExistsError, NotADirectoryError):
                raise CampaignError(f"input {item.name} at {item.path} is under a source "
                                    "file") from None
            _copy(source, dest)
        self._verify_payload(node, staging, bindings)
        os.replace(staging, payload)
        _fsync_dir(payload.parent)
        return bindings

    # --- actions ----------------------------------------------------------

    def _submit_new(self, node_id: str, next_action: dict[str, Any]) -> None:
        node = self.campaign.node(node_id)
        number = int(next_action["attempt"])
        bindings = self.stage(node, number)
        attempt = Attempt(node=node_id, attempt=number,
                          idempotency_key=idempotency_key(self.digest, node_id, number),
                          state="submitting", inputs=bindings)
        # Recorded before the request is sent: a crash from here on leaves
        # an attempt that is resent under its key, never a second job.
        self.journal.attempts.append(attempt)
        self.save()
        self.log(f"{node_id}: staged attempt {number}"
                 + "".join(f"; {name} from {b.producer_attempt}:{b.manifest_sha256[:12]}"
                           for name, b in sorted(bindings.items())))
        self._send(self._attempt(node_id, number))

    def _send(self, attempt: Attempt) -> None:
        node = self.campaign.node(attempt.node)
        payload = self.payload_dir(attempt.node, attempt.attempt)
        if not payload.is_dir():
            raise CampaignError(f"payload {payload} is gone; it cannot be resent under its key")
        self._verify_payload(node, payload, attempt.inputs)
        job = self.queue.submit(
            node, payload, attempt.idempotency_key,
            job_name(self.campaign.campaign_id, attempt.node, attempt.attempt))
        self._replace(attempt, state="accepted", job=job.model_dump(mode="json"))
        self.log(f"{attempt.node}: attempt {attempt.attempt} accepted as job {job.job_id}"
                 + (f" on {job.host}" if job.host != "localhost" else ""))

    def _observe(self, attempt: Attempt) -> bool:
        assert attempt.job is not None
        seen = self.queue.observe(attempt.job)
        if seen.state not in _PROGRESS:
            raise CampaignError(f"job {attempt.job.job_id}: {seen.detail or seen.state}")
        declared = self.campaign.node(attempt.node).runtime_digest
        # A running scheduler job's node may not have checked its generation
        # yet; it is confirmed, or refused, once the job has exited.
        confirmable = seen.state == "succeeded" or (
            seen.state == "running" and not seen.generation_unconfirmed)
        if declared is not None and confirmable:
            # The queue binds the generation; this confirms it from the job's
            # own record before anything it produced is published.
            ran = seen.runtime.artifact_identity if seen.runtime is not None else None
            if ran != declared:
                raise CampaignError(
                    f"job {attempt.job.job_id} ran "
                    f"{'runtime ' + ran[:12] if ran else 'no recorded runtime generation'}, "
                    f"not the declared runtime {declared[:12]}; its outputs are not published")
        changes: dict[str, Any] = {}
        if _PROGRESS[seen.state] > _PROGRESS[attempt.state]:
            changes["state"] = seen.state
        if seen.runtime is not None and seen.runtime != attempt.runtime:
            changes["runtime"] = seen.runtime.model_dump(mode="json")
        if not changes:
            return False
        self._replace(attempt, **changes)
        if "state" in changes:
            detail = f" ({seen.detail})" if seen.detail else ""
            self.log(f"{attempt.node}: attempt {attempt.attempt} {seen.state}{detail}")
        if "runtime" in changes:
            self.log(f"{attempt.node}: attempt {attempt.attempt} runs "
                     f"{runtime_text(changes['runtime'])}")
        return True

    def _publish(self, attempt: Attempt) -> None:
        assert attempt.job is not None
        node = self.campaign.node(attempt.node)
        # Controller relay: each output comes back through `vq fetch` into
        # the run directory, wherever the job ran, and is hashed there.
        incoming = self.run_dir / "incoming" / node.id / str(attempt.attempt)
        _remove(incoming)
        incoming.mkdir(parents=True)
        fetched: set[str] = set()
        artifacts = dict(attempt.artifacts)
        for output in node.outputs:
            if (record := artifacts.get(output.name)) and record.status == "verified":
                continue
            top = output.path.split("/", 1)[0]
            if top not in fetched:
                try:
                    self.queue.fetch(attempt.job, top, incoming)
                except FetchMissing as exc:
                    raise CampaignError(f"output {output.name} is missing: {exc}") from None
                fetched.add(top)
            manifest = publish_artifact(incoming, self.campaign, node.id, attempt.attempt,
                                        output.name, runtime_digest=node.runtime_digest,
                                        executed_runtime=attempt.runtime)
            dest = self.artifact_path(node.id, attempt.attempt, output.name)
            dest.parent.mkdir(parents=True, exist_ok=True)
            copy = dest.with_name(f".{output.name}.copy")
            _remove(copy)
            _copy(incoming / output.path, copy)
            result = verify_artifact(copy, manifest)
            if result["status"] == "verified":
                _remove(dest)
                os.replace(copy, dest)
                _write_atomic(dest.with_name(f"{output.name}.manifest.json"),
                              to_json(manifest_payload(manifest)))
            else:
                _remove(copy)
            artifacts[output.name] = ArtifactRecord.model_validate(
                {"manifest_sha256": manifest_digest(manifest), "status": result["status"]})
            self.log(f"{node.id}: published {output.name} from attempt {attempt.attempt}: "
                     f"{result['status']} ({manifest_digest(manifest)[:12]})")
        self._replace(attempt, artifacts={k: v.model_dump(mode="json")
                                          for k, v in artifacts.items()})
        _remove(incoming)

    def step(self) -> bool:
        """One pass: act on every node that can move. True if anything changed."""
        self.transient.clear()
        progressed = False
        # Each node takes each kind of action at most once per pass, so a
        # publish that does not verify is retried next pass, not in a loop.
        done: set[tuple[str, str]] = set()
        while True:
            acted = False
            for entry in self.status()["nodes"]:
                node_id, action = entry["node"], entry["next"]
                kind = action["action"]
                if (kind not in {"submit", "reconcile", "wait", "publish"}
                        or node_id in self.problems or node_id in self.transient
                        or (node_id, kind) in done):
                    continue
                done.add((node_id, kind))
                try:
                    if kind == "submit":
                        self._submit_new(node_id, action)
                    elif kind == "reconcile":
                        self._send(self._attempt(node_id, int(action["attempt"])))
                    elif kind == "wait":
                        if not self._observe(self._attempt(node_id, int(action["attempt"]))):
                            continue
                    else:
                        self._publish(self._attempt(node_id, len(entry["attempts"])))
                except Transient as exc:
                    self.transient[node_id] = str(exc)
                    self.log(f"{node_id}: {exc}; asking again next pass")
                except (CampaignError, OSError, ValueError) as exc:
                    self.problems[node_id] = str(exc)
                    self.log(f"{node_id}: {exc}")
                else:
                    progressed = acted = True
                    break
            if not acted:
                return progressed

    def run(
        self, *, once: bool = False, interval: float = DEFAULT_INTERVAL_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> dict[str, Any]:
        """Pass until nothing more can run; sleep between passes while jobs run.

        Returns once the campaign succeeded, or when no job is in flight, no
        queue call is pending a retry and the last pass changed nothing:
        the remaining nodes are failed, blocked, or held by a problem that
        needs someone to act.
        """
        while True:
            progressed = self.step()
            report = self.status()
            running = any(n["status"] == "running" and n["node"] not in self.problems
                          for n in report["nodes"])
            if once or report["status"] == "succeeded" or not (
                    running or progressed or self.transient):
                return report
            if not progressed:
                sleep(interval)

    def cleanup_plan(self, *, intermediate: bool = False) -> list[dict[str, Any]]:
        """What the run directory can give up, with each path's size and reason.

        By default only copies the controller made: the payload of an attempt
        the queue accepted (only a ``submitting`` attempt is ever resent), and
        leftovers of an interrupted stage or publish. With ``intermediate``,
        also each published output whose every consumer has succeeded. Never
        an output on the retention list, an output no node consumes (a
        campaign result), or anything outside the run directory.
        """
        report = self.status()
        retained = {(str(r["node"]), int(r["attempt"])) for r in report["retain"]}
        consumers: dict[tuple[str, str], list[str]] = {}
        for node in self.campaign.nodes:
            for item in node.inputs:
                consumers.setdefault((item.producer, item.output), []).append(node.id)
        plan: list[dict[str, Any]] = []

        def add(path: Path, reason: str) -> None:
            if path.exists() or path.is_symlink():
                plan.append({"path": str(path), "bytes": _size(path), "reason": reason})

        for attempt in self.journal.attempts:
            if attempt.state != "submitting":
                add(self.payload_dir(attempt.node, attempt.attempt),
                    f"payload of {attempt.node} attempt {attempt.attempt}, which the queue "
                    "accepted; it is never resent")
        incoming = self.run_dir / "incoming"
        if incoming.is_dir():
            for leftover in sorted(incoming.glob("*/*")):
                add(leftover, "copy left by an interrupted publish")
        for area in ("payloads", "artifacts"):
            root = self.run_dir / area
            if root.is_dir():
                for leftover in sorted(root.rglob(".*")):
                    if leftover.name.endswith((".staging", ".copy")):
                        add(leftover, "left by an interrupted stage or publish")
        if intermediate:
            for node in self.campaign.nodes:
                attempts = [a for a in self.journal.attempts if a.node == node.id]
                if not attempts or attempts[-1].state != "succeeded":
                    continue
                done = attempts[-1]
                for output in node.outputs:
                    users = consumers.get((node.id, output.name), [])
                    # Retained while any consumer of any of its outputs has
                    # not succeeded; an output nothing consumes is a result.
                    if not users or (node.id, done.attempt) in retained:
                        continue
                    add(self.artifact_path(node.id, done.attempt, output.name),
                        f"{node.id}.{output.name} was consumed by {', '.join(sorted(users))}, "
                        "which all succeeded")
        return plan

    def authorize_retry(self, node_id: str) -> Attempt:
        """Record the next attempt of a failed or cancelled node, staged but unsent.

        The next pass sends it under the new attempt's own key. Its inputs
        are staged from the producers' current verified artifacts.
        """
        try:
            node = self.campaign.node(node_id)
        except StopIteration:
            raise CampaignError(f"the campaign has no node {node_id!r}") from None
        attempts = [a for a in self.journal.attempts if a.node == node_id]
        if not attempts or attempts[-1].state not in {"failed", "cancelled"}:
            state = attempts[-1].state if attempts else "never attempted"
            raise CampaignError(f"{node_id} is {state}; only a failed or cancelled attempt "
                                "is retried")
        number = attempts[-1].attempt + 1
        bindings = self.stage(node, number)
        attempt = Attempt(node=node_id, attempt=number, retry_of=number - 1,
                          idempotency_key=idempotency_key(self.digest, node_id, number),
                          state="submitting", inputs=bindings)
        self.journal.attempts.append(attempt)
        self.save()
        return attempt
