"""Old-code contracts for status/log resolution and terminal read stamps.

These tests intentionally land before the Milestone 7 helper extraction.  They
pin the complete normalized text/JSON outputs and the less visible transaction
semantics that a shared helper must preserve.
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from collections.abc import Callable
from pathlib import Path

import pytest

from vq import config, listing, logs, ownership, paths, spec_access
from vq import status as status_module
from vq.spec import TERMINAL_STATES, JobSpec, JobState

_CASES = (
    "pending",
    "running",
    "scheduler",
    "archived",
    "dependency",
    "qvf",
    "terminal",
)
_FORMATS = ("text", "json")

# The digest covers every byte of the normalized public return, including JSON
# indentation and key ordering. Scheduler snapshots intentionally include the
# additive bounded-refresh audit introduced for BUG107.
# To update one, temporarily replace that digest with zeros and run the focused
# case: the assertion prints the complete normalized output and new digest for
# review.  Never update a digest without reviewing that output diff.
_OUTPUT_SHA256 = {
    ("pending", "text"): "f81a81a2a0bd4e965f80640905a7e462f8a83d01ea54fb5e90d5b2daf6596716",
    ("running", "text"): "6e9df27e90f361162754582be3a8581c4958b7b61d9f9892ed9e43b78ba5e0d4",
    ("scheduler", "text"): "e526ce8b853be3b8dcea720aeda6686fa9f6f8cd62aca61b04ee2c190170d993",
    ("archived", "text"): "17fcceccfc0799b0d6e62c870d07f8c5fdac0463b5a4b321862d573f9142ca89",
    ("dependency", "text"): "fe02636c0c1059904f8327bb01cfef64adac9bc9b0ce9ab9ddb4ca51e2fb3cba",
    ("qvf", "text"): "206afbcf785783de46ee3ae4ec62a653a502d435a435dcc579d4e046a59cbf3d",
    ("terminal", "text"): "4f7442bf729216211947120167be895fee74a44c4144e394ca04f72ba7e59667",
    ("pending", "json"): "20866e675cd35b47e1e0fa41b28b77cf889175f2a08eee3e897075390619dfa2",
    ("running", "json"): "90c9bc4d6f45d879bbfe8a5bbb1b210f70f8ea36b557d697f1adbe47253d69f6",
    ("scheduler", "json"): "182bc0d73dd925ede0a9a6405306df389e04278038223f8faa6b064866ac1f36",
    ("archived", "json"): "7b67031e095a4f1903f982550f1e6eea56bb1e5a62a597e1bdf4c2a40e2880d5",
    ("dependency", "json"): "40c16ab38127b14b48b2a3786afe4e5613e295819c1016a2cb8e4f596c747601",
    ("qvf", "json"): "61f952e584f8058fd1957934e22ca7c2ab266ea6ae64bcbce3b2a296d482fc5e",
    ("terminal", "json"): "c672f5bc65472c990c6d0e6c651f61b6abeabe711edc222ca697cdf4e5c36f26",
}


def _configure_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    state_root = tmp_path / "state"
    queue_dir = state_root / "queue"
    queue_dir.mkdir(parents=True)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text('default_host = "localhost"\n')
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(status_module, "is_daemon_serving", lambda **_kwargs: True)
    return queue_dir


def _workspace(tmp_path: Path, jobid: str) -> Path:
    workspace = tmp_path / "workspaces" / jobid
    workspace.mkdir(parents=True)
    (workspace / "stdout.log").write_text("stdout line\n")
    (workspace / "stderr.log").write_text("stderr line\n")
    return workspace


def _write_complete_qvf(path: Path) -> None:
    input_bytes = b'{"method":"rhf"}\n'
    log_bytes = b"SCF complete\n"
    members = {
        "run_record/input.txt": input_bytes,
        "run_record/log.txt": log_bytes,
    }
    manifest = {
        "qvf_version": "0.1",
        "provenance": {"run_status": "converged"},
        "sections": [
            {
                "id": "run_record3",
                "kind": "run.record",
                "program": "vibe-qc",
                "sequence": 3,
                "members": {
                    role: {
                        "path": member_path,
                        "format": "binary",
                        "sha256": hashlib.sha256(data).hexdigest(),
                    }
                    for role, (member_path, data) in {
                        "input": ("run_record/input.txt", input_bytes),
                        "log": ("run_record/log.txt", log_bytes),
                    }.items()
                },
            }
        ],
    }
    members["manifest.json"] = json.dumps(manifest, sort_keys=True).encode()
    with zipfile.ZipFile(path, "w") as archive:
        for member_path, data in members.items():
            archive.writestr(member_path, data)


def _write_case(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> tuple[Path, JobSpec]:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    jobid = f"status-{case}"
    workspace = _workspace(tmp_path, jobid)
    fields: dict[str, object] = {
        "id": jobid,
        "command": ["python", "run.py"],
        "cwd": str(workspace),
        "cpus": 2,
        "submitted_at": "2026-08-01T10:00:00+00:00",
    }

    if case == "running":
        fields["state"] = JobState.RUNNING
    elif case == "scheduler":
        fields.update(
            state=JobState.RUNNING,
            scheduler_target="host_f",
            scheduler_job_id="1234.cluster",
            scheduler_state="running",
            scheduler_exec_host="node042",
            scheduler_walltime_used="00:30:00",
            scheduler_walltime_limit="01:00:00",
        )
    elif case == "archived":
        fields.update(
            state=JobState.COMPLETED,
            finished_at="2026-08-01T11:00:00+00:00",
            exit_code=0,
            archived_at="2026-08-01T12:00:00+00:00",
            archive_path=str(tmp_path / "archive" / f"{jobid}.tar.bz2"),
        )
    elif case == "dependency":
        predecessor_id = "dependency-source"
        predecessor_workspace = _workspace(tmp_path, predecessor_id)
        JobSpec(
            id=predecessor_id,
            command=["sleep", "10"],
            cwd=str(predecessor_workspace),
            cpus=1,
            state=JobState.RUNNING,
            submitted_at="2026-08-01T09:00:00+00:00",
        ).write(queue_dir / f"{predecessor_id}.json")
        fields["depends_on"] = [predecessor_id]
    elif case == "qvf":
        _write_complete_qvf(workspace / "result.qvf")
        fields.update(
            state=JobState.COMPLETED,
            finished_at="2026-08-01T11:00:00+00:00",
            exit_code=0,
            qvf_artifact_name="result.qvf",
        )
    elif case == "terminal":
        fields.update(
            state=JobState.FAILED,
            finished_at="2026-08-01T11:00:00+00:00",
            exit_code=2,
            failure_reason="command exited 2",
            last_status_at="2026-08-01T11:30:00+00:00",
        )
    elif case != "pending":
        raise AssertionError(f"unknown case: {case}")

    spec = JobSpec(**fields)
    spec_path = queue_dir / f"{jobid}.json"
    spec.write(spec_path)
    return spec_path, spec


def _render_status(
    output_format: str,
    spec: JobSpec,
    spec_path: Path,
) -> str:
    if output_format == "text":
        return status_module.show_status(
            "localhost",
            spec.id,
            queue_dir=spec_path.parent,
        )
    return status_module.show_status_json(
        "localhost",
        spec.id,
        queue_dir=spec_path.parent,
    )


def _normalized(output: str, tmp_path: Path) -> str:
    return output.replace(str(tmp_path), "<TMP>")


def _without_status_stamp(spec: JobSpec) -> dict[str, object]:
    payload = spec.model_dump(mode="json")
    payload.pop("last_status_at")
    return payload


@pytest.mark.parametrize("case", _CASES)
@pytest.mark.parametrize("output_format", _FORMATS)
def test_status_output_and_stamp_snapshot_on_old_code(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    output_format: str,
) -> None:
    spec_path, initial = _write_case(tmp_path, monkeypatch, case)
    original_bytes = spec_path.read_bytes()

    output = _render_status(output_format, initial, spec_path)

    normalized = _normalized(output, tmp_path)
    actual_digest = hashlib.sha256(normalized.encode()).hexdigest()
    assert actual_digest == _OUTPUT_SHA256[(case, output_format)], (
        f"{case}/{output_format}: {actual_digest}\n{normalized}"
    )

    after = JobSpec.read(spec_path)
    if initial.is_terminal:
        assert after.last_status_at is not None
        assert _without_status_stamp(after) == _without_status_stamp(initial)
    else:
        assert spec_path.read_bytes() == original_bytes
    if output_format == "json":
        # The payload is assembled before the best-effort disk stamp.
        assert json.loads(output)["last_status_at"] == initial.last_status_at


def _basic_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    state: JobState,
) -> tuple[Path, JobSpec]:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "race-job")
    spec = JobSpec(
        id="race-job",
        command=["python", "run.py"],
        cwd=str(workspace),
        cpus=1,
        state=state,
        submitted_at="2026-08-01T10:00:00+00:00",
        finished_at=(
            "2026-08-01T11:00:00+00:00" if state in TERMINAL_STATES else None
        ),
        exit_code=0 if state in TERMINAL_STATES else None,
    )
    spec_path = queue_dir / "race-job.json"
    spec.write(spec_path)
    return spec_path, spec


def _interleave_on_first_tail(
    monkeypatch: pytest.MonkeyPatch,
    spec_path: Path,
    mutate: Callable[[JobSpec], None],
) -> None:
    original_tail = status_module._tail_file
    interleaved = False

    def tail_with_writer(path: Path, count: int | None) -> str:
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            with paths.spec_lock(spec_path):
                fresh = JobSpec.read(spec_path)
                mutate(fresh)
                fresh.write(spec_path)
        return original_tail(path, count)

    monkeypatch.setattr(status_module, "_tail_file", tail_with_writer)


@pytest.mark.parametrize("output_format", _FORMATS)
def test_terminal_snapshot_stamps_fresh_concurrent_terminal_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    spec_path, initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.COMPLETED,
    )

    def concurrent_writer(fresh: JobSpec) -> None:
        fresh.state = JobState.KILLED
        fresh.exit_code = -15
        fresh.finished_at = "2026-08-01T11:01:00+00:00"
        fresh.last_fetched_at = "2026-08-01T11:02:00+00:00"

    _interleave_on_first_tail(monkeypatch, spec_path, concurrent_writer)

    output = _render_status(output_format, initial, spec_path)

    if output_format == "json":
        assert json.loads(output)["state"] == JobState.COMPLETED.value
    else:
        assert "state:        completed" in output
    after = JobSpec.read(spec_path)
    assert after.state == JobState.KILLED
    assert after.last_fetched_at == "2026-08-01T11:02:00+00:00"
    assert after.last_status_at is not None


@pytest.mark.parametrize("output_format", _FORMATS)
def test_nonterminal_snapshot_that_finishes_during_read_is_not_stamped(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    spec_path, initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.RUNNING,
    )

    def concurrent_writer(fresh: JobSpec) -> None:
        fresh.state = JobState.COMPLETED
        fresh.exit_code = 0
        fresh.finished_at = "2026-08-01T11:00:00+00:00"

    _interleave_on_first_tail(monkeypatch, spec_path, concurrent_writer)

    output = _render_status(output_format, initial, spec_path)

    if output_format == "json":
        assert json.loads(output)["state"] == JobState.RUNNING.value
    else:
        assert "state:        running" in output
    after = JobSpec.read(spec_path)
    assert after.state == JobState.COMPLETED
    assert after.last_status_at is None


@pytest.mark.parametrize("output_format", _FORMATS)
def test_terminal_snapshot_rechecks_fresh_nonterminal_state_before_stamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    spec_path, initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.COMPLETED,
    )

    def concurrent_writer(fresh: JobSpec) -> None:
        fresh.state = JobState.RUNNING
        fresh.exit_code = None
        fresh.finished_at = None

    _interleave_on_first_tail(monkeypatch, spec_path, concurrent_writer)

    output = _render_status(output_format, initial, spec_path)

    if output_format == "json":
        assert json.loads(output)["state"] == JobState.COMPLETED.value
    else:
        assert "state:        completed" in output
    after = JobSpec.read(spec_path)
    assert after.state == JobState.RUNNING
    assert after.last_status_at is None


def _public_reader(
    name: str,
    spec_path: Path,
    *,
    multi_user: bool,
    host: str = "localhost",
) -> str:
    if name == "status-text":
        return status_module.show_status(
            host,
            spec_path.stem,
            queue_dir=spec_path.parent,
            multi_user=multi_user,
        )
    if name == "status-json":
        return status_module.show_status_json(
            host,
            spec_path.stem,
            queue_dir=spec_path.parent,
            multi_user=multi_user,
        )
    if name == "logs-text":
        return logs.show_logs(
            host,
            spec_path.stem,
            multi_user=multi_user,
        )
    if name == "logs-json":
        return logs.show_logs_json(
            host,
            spec_path.stem,
            multi_user=multi_user,
        )
    if name == "logs-follow":
        generator = logs.follow_logs(
            host,
            spec_path.stem,
            multi_user=multi_user,
            poll_interval=0,
            sleep=lambda _seconds: None,
        )
        try:
            return next(generator)
        finally:
            generator.close()
    if name == "events-text":
        return logs.show_events(
            host,
            spec_path.stem,
            multi_user=multi_user,
        )
    if name == "events-json":
        return logs.show_events_json(
            host,
            spec_path.stem,
            multi_user=multi_user,
        )
    raise AssertionError(f"unknown reader: {name}")


_STAMPING_READERS = ("status-text", "status-json", "logs-text", "logs-json")
_PUBLIC_READERS = (
    *_STAMPING_READERS,
    "logs-follow",
    "events-text",
    "events-json",
)


@pytest.mark.parametrize("reader", _PUBLIC_READERS)
def test_public_readers_resolve_then_authorize_then_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "ordered")
    spec_path = queue_dir / "ordered.json"
    JobSpec(
        id="ordered",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
    ).write(spec_path)
    (tmp_path / "config" / "config.toml").write_text(
        'default_host = "localhost"\n'
        "[multi_user]\n"
        "enabled = true\n"
        'admin_group = "nonexistent-vq-test-group"\n'
    )
    calls: list[tuple[str, Path]] = []
    original_check = ownership.check_spec_path_owner
    original_read = JobSpec.read

    def resolve(jobid: str, *, multi_user: bool = False, uid=None) -> Path:
        assert jobid == "ordered"
        assert multi_user is True
        assert uid is None
        calls.append(("resolve", spec_path))
        return spec_path

    def authorize(path: Path, *, cfg=None, multi_user: bool = False) -> None:
        calls.append(("authorize", path))
        original_check(path, cfg=cfg, multi_user=multi_user)

    def read(_cls: type[JobSpec], path: Path) -> JobSpec:
        calls.append(("read", path))
        return original_read(path)

    # Module-qualified dependency lookup is an explicit M7 constraint.  The
    # existing test suite and fault-injection seams patch these shared modules;
    # the neutral helper must keep that observable injection boundary.
    monkeypatch.setattr(paths, "resolve_spec_path", resolve)
    monkeypatch.setattr(ownership, "check_spec_path_owner", authorize)
    monkeypatch.setattr(ownership, "_caller_is_admin", lambda _cfg: False)
    monkeypatch.setattr(JobSpec, "read", classmethod(read))

    _public_reader(reader, spec_path, multi_user=True)

    # Enabled multi-user authorization performs its historical ownership read,
    # followed by the caller's final load.  M7 deliberately does not collapse
    # that two-read/TOCTOU behavior into a different security transaction.
    assert calls[:4] == [
        ("resolve", spec_path),
        ("authorize", spec_path),
        ("read", spec_path),
        ("read", spec_path),
    ]


@pytest.mark.parametrize("reader", _PUBLIC_READERS)
def test_public_readers_keep_exact_missing_job_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    missing_path = queue_dir / "missing.json"

    with pytest.raises(FileNotFoundError) as exc_info:
        _public_reader(reader, missing_path, multi_user=False)

    assert str(exc_info.value) == "no such job: missing"


@pytest.mark.parametrize("reader", _PUBLIC_READERS)
def test_public_readers_authorize_before_final_spec_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "denied")
    spec_path = queue_dir / "denied.json"
    JobSpec(
        id="denied",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
    ).write(spec_path)

    class Denied(PermissionError):
        pass

    def deny(
        _path: Path, *, cfg=None, multi_user: bool = False
    ) -> None:
        del cfg, multi_user
        raise Denied("denied before read")

    def unexpected_read(_cls: type[JobSpec], _path: Path) -> JobSpec:
        raise AssertionError("final JobSpec.read ran before authorization")

    monkeypatch.setattr(ownership, "check_spec_path_owner", deny)
    monkeypatch.setattr(JobSpec, "read", classmethod(unexpected_read))

    with pytest.raises(Denied, match="denied before read"):
        _public_reader(reader, spec_path, multi_user=False)


def test_secure_reread_authorizes_the_exact_opened_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "opened-owner")
    spec_path = queue_dir / "opened-owner.json"
    JobSpec(
        id="opened-owner",
        command=["echo", "foreign"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
        submitter="2002",
    ).write(spec_path)
    observed: list[JobSpec] = []

    monkeypatch.setattr(
        paths,
        "resolve_spec_path",
        lambda *_args, **_kwargs: spec_path,
    )

    def deny_opened_snapshot(
        spec: JobSpec, *, cfg=None, multi_user: bool = False
    ) -> None:
        del cfg
        assert multi_user is True
        observed.append(spec)
        raise ownership.OwnershipError("foreign opened snapshot")

    monkeypatch.setattr(ownership, "check_owner", deny_opened_snapshot)

    with pytest.raises(ownership.OwnershipError, match="foreign opened snapshot"):
        spec_access.reread_authorized_spec(
            "opened-owner",
            expected_path=spec_path,
            multi_user=True,
        )

    assert [spec.submitter for spec in observed] == ["2002"]


@pytest.mark.parametrize("output_format", _FORMATS)
def test_status_honors_explicit_queue_dir_over_default_queue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    default_queue = _configure_state(tmp_path, monkeypatch)
    explicit_queue = tmp_path / "explicit-queue"
    explicit_queue.mkdir()
    target_workspace = _workspace(tmp_path, "explicit-target")
    decoy_workspace = _workspace(tmp_path, "default-decoy")
    target = JobSpec(
        id="same-id",
        command=["true"],
        cwd=str(target_workspace),
        cpus=1,
        state=JobState.RUNNING,
    )
    target_path = explicit_queue / "same-id.json"
    target.write(target_path)
    JobSpec(
        id="same-id",
        command=["false"],
        cwd=str(decoy_workspace),
        cpus=1,
        state=JobState.FAILED,
        exit_code=1,
        finished_at="2026-08-01T11:00:00+00:00",
    ).write(default_queue / "same-id.json")

    output = _render_status(output_format, target, target_path)

    if output_format == "json":
        payload = json.loads(output)
        assert payload["state"] == JobState.RUNNING.value
        assert payload["cwd"] == str(target_workspace)
    else:
        assert "state:        running" in output
        assert f"cwd:          {target_workspace}" in output


@pytest.mark.parametrize("output_format", _FORMATS)
def test_pending_status_resolves_default_queue_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output_format: str,
) -> None:
    _configure_state(tmp_path, monkeypatch)
    primary_queue = tmp_path / "primary-queue"
    changed_queue = tmp_path / "changed-queue"
    primary_queue.mkdir()
    changed_queue.mkdir()
    target_workspace = _workspace(tmp_path, "stable-target")
    ahead_workspace = _workspace(tmp_path, "changed-ahead")
    target = JobSpec(
        id="stable-target",
        command=["true"],
        cwd=str(target_workspace),
        cpus=1,
        state=JobState.PENDING,
        submitted_at="2026-08-01T10:00:00+00:00",
    )
    target.write(primary_queue / "stable-target.json")
    JobSpec(
        id="changed-ahead",
        command=["true"],
        cwd=str(ahead_workspace),
        cpus=1,
        state=JobState.PENDING,
        submitted_at="2026-08-01T09:00:00+00:00",
    ).write(changed_queue / "changed-ahead.json")
    calls = 0

    def moving_default() -> Path:
        nonlocal calls
        calls += 1
        return primary_queue if calls == 1 else changed_queue

    monkeypatch.setattr(paths, "queue_dir", moving_default)

    if output_format == "text":
        output = status_module.show_status("localhost", target.id)
        assert "queue position: 1 of 1 pending in local lane" in output
    else:
        payload = json.loads(status_module.show_status_json("localhost", target.id))
        assert payload["id"] == target.id
    assert calls == 1


@pytest.mark.parametrize("reader", _PUBLIC_READERS)
def test_remote_rejection_precedes_all_local_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    _configure_state(tmp_path, monkeypatch)
    unresolved = tmp_path / "never-read" / "remote-job.json"

    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("remote reader attempted local resolution")

    monkeypatch.setattr(paths, "resolve_spec_path", unexpected)
    monkeypatch.setattr(ownership, "check_spec_path_owner", unexpected)
    monkeypatch.setattr(JobSpec, "read", classmethod(unexpected))

    with pytest.raises(NotImplementedError):
        _public_reader(
            reader,
            unresolved,
            multi_user=True,
            host="remote.example",
        )


# Keep this patchable through ``paths.spec_lock`` for the same module-qualified
# fault-injection contract documented in the resolution-order test above.
class _FailingLock:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __enter__(self) -> None:
        raise self.error

    def __exit__(self, *_args: object) -> bool:
        return False


@pytest.mark.parametrize("reader", _STAMPING_READERS)
def test_terminal_read_returns_output_when_lock_acquisition_raises_oserror(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    spec_path, _initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.COMPLETED,
    )
    before = spec_path.read_bytes()
    monkeypatch.setattr(
        paths,
        "spec_lock",
        lambda _path: _FailingLock(OSError("lock unavailable")),
    )

    output = _public_reader(reader, spec_path, multi_user=False)

    assert output
    assert spec_path.read_bytes() == before


@pytest.mark.parametrize("reader", _STAMPING_READERS)
def test_terminal_read_does_not_suppress_non_oserror_from_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    spec_path, _initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.COMPLETED,
    )
    before = spec_path.read_bytes()
    monkeypatch.setattr(
        paths,
        "spec_lock",
        lambda _path: _FailingLock(RuntimeError("programming error")),
    )

    with pytest.raises(RuntimeError, match="programming error"):
        _public_reader(reader, spec_path, multi_user=False)
    assert spec_path.read_bytes() == before


def test_follow_archived_terminal_job_does_not_stamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "archived-follow")
    spec_path = queue_dir / "archived-follow.json"
    JobSpec(
        id="archived-follow",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.COMPLETED,
        archived_at="2026-08-01T12:00:00+00:00",
        archive_path=str(tmp_path / "archive" / "archived-follow.tar.bz2"),
    ).write(spec_path)

    chunks = list(logs.follow_logs("localhost", "archived-follow"))

    assert chunks == ["(archived; vq cleanup --restore <jobid> to un-tar)"]
    assert JobSpec.read(spec_path).last_status_at is None


def test_follow_unarchived_terminal_job_stamps_initial_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec_path, _initial = _basic_spec(
        tmp_path,
        monkeypatch,
        state=JobState.COMPLETED,
    )
    generator = logs.follow_logs(
        "localhost",
        spec_path.stem,
        stream="stdout",
        poll_interval=0,
        sleep=lambda _seconds: None,
    )

    try:
        assert next(generator) == "stdout line\n"
    finally:
        generator.close()

    assert JobSpec.read(spec_path).last_status_at is not None


def test_follow_job_that_becomes_terminal_does_not_stamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "running-follow")
    spec_path = queue_dir / "running-follow.json"
    JobSpec(
        id="running-follow",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.RUNNING,
    ).write(spec_path)
    generator = logs.follow_logs(
        "localhost",
        "running-follow",
        stream="stdout",
        poll_interval=0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
    )

    assert next(generator) == "stdout line\n"
    with paths.spec_lock(spec_path):
        fresh = JobSpec.read(spec_path)
        fresh.state = JobState.COMPLETED
        fresh.exit_code = 0
        fresh.finished_at = "2026-08-01T11:00:00+00:00"
        fresh.write(spec_path)
    assert list(generator) == []
    assert JobSpec.read(spec_path).last_status_at is None


def test_listing_terminal_job_is_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_dir = _configure_state(tmp_path, monkeypatch)
    workspace = _workspace(tmp_path, "listed-terminal")
    spec_path = queue_dir / "listed-terminal.json"
    JobSpec(
        id="listed-terminal",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        state=JobState.COMPLETED,
        finished_at="2026-08-01T11:00:00+00:00",
        exit_code=0,
    ).write(spec_path)
    before = spec_path.read_bytes()

    specs = listing.list_jobs("localhost", queue_dir=queue_dir)
    assert "listed-terminal" in listing.format_table(specs)

    assert spec_path.read_bytes() == before
