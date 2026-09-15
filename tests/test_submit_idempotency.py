"""Red-first contracts for queue-authority submit idempotency (BUG 104)."""

from __future__ import annotations

import errno
import inspect
import json
import multiprocessing
import os
import stat
import tarfile
from pathlib import Path

import pytest

from vq import auth, cleanup, drain, spec_access, submit
from vq.config import HostConfig
from vq.spec import JobSpec, JobState, ProgramRuntimePin
from vq.vibeqc_preflight import PreflightResult


def _submit(
    source: Path,
    queue_dir: Path,
    jobs_dir: Path,
    *,
    key: str | None,
    cpus: int = 1,
) -> str:
    return submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key=key,
        cpus=cpus,
    )


def _concurrent_submit(
    source: str,
    queue_dir: str,
    jobs_dir: str,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
) -> None:
    try:
        start.wait(timeout=10)
        jobid = _submit(
            Path(source),
            Path(queue_dir),
            Path(jobs_dir),
            key="campaign-round-7",
        )
    except BaseException as exc:  # pragma: no cover - asserted in parent
        results.put(("error", type(exc).__name__, str(exc)))
    else:
        results.put(("ok", jobid))


def _concurrent_submit_with_cpus(
    source: str,
    queue_dir: str,
    jobs_dir: str,
    start: multiprocessing.synchronize.Event,
    results: multiprocessing.queues.Queue,
    cpus: int,
) -> None:
    try:
        start.wait(timeout=10)
        jobid = _submit(
            Path(source),
            Path(queue_dir),
            Path(jobs_dir),
            key="different-intent-race",
            cpus=cpus,
        )
    except BaseException as exc:  # pragma: no cover - asserted in parent
        results.put(("error", type(exc).__name__, str(exc)))
    else:
        results.put(("ok", jobid))


def test_same_key_and_intent_returns_original_job_without_duplicate(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("print('once')\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    first = _submit(source, queue_dir, jobs_dir, key="paper-wave-0042")
    second = _submit(source, queue_dir, jobs_dir, key="paper-wave-0042")

    assert second == first
    assert [path.name for path in queue_dir.glob("*.json")] == [f"{first}.json"]
    assert [path.name for path in jobs_dir.iterdir()] == [first]
    spec = JobSpec.read(queue_dir / f"{first}.json")
    assert spec.idempotency_key_hash is not None
    assert spec.submission_intent_digest is not None
    assert spec.submission_owner_hash is not None
    public = spec.model_dump(mode="json")
    assert "idempotency_key_hash" not in public
    assert "submission_intent_digest" not in public
    assert "submission_owner_hash" not in public


def test_same_key_replay_redelivers_current_capacity_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="warning-replay")
    probed_jobids: list[str] = []

    def replay_warning(**kwargs: object) -> tuple[str, ...]:
        jobid = str(kwargs["jobid"])
        probed_jobids.append(jobid)
        return (f"current cap still parks job {jobid}",)

    monkeypatch.setattr(submit, "_impossible_capacity_warnings", replay_warning)
    warnings: list[str] = []

    replayed = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="warning-replay",
        warning_sink=warnings.append,
    )

    assert replayed == original
    assert probed_jobids == [original]
    assert warnings == [f"current cap still parks job {original}"]


def test_same_key_terminal_replay_does_not_emit_pending_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="terminal-replay")
    spec_path = queue_dir / f"{original}.json"
    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.exit_code = 0
    spec.write(spec_path)
    monkeypatch.setattr(
        submit,
        "_impossible_capacity_warnings",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("terminal replay must not probe a pending warning")
        ),
    )

    replayed = _submit(source, queue_dir, jobs_dir, key="terminal-replay")

    assert replayed == original


def test_under_lock_replay_rebinds_warning_to_durable_jobid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="locked-replay")
    monkeypatch.setattr(
        submit,
        "_replay_preexisting_idempotent_submission",
        lambda *_args, **_kwargs: None,
    )
    probed_jobids: list[str] = []

    def capacity_warning(**kwargs: object) -> tuple[str, ...]:
        jobid = str(kwargs["jobid"])
        probed_jobids.append(jobid)
        return (f"capacity warning for {jobid}",)

    monkeypatch.setattr(submit, "_impossible_capacity_warnings", capacity_warning)
    warnings: list[str] = []

    replayed = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="locked-replay",
        warning_sink=warnings.append,
    )

    assert replayed == original
    assert len(probed_jobids) == 2
    assert probed_jobids[0] != original
    assert probed_jobids[1] == original
    assert warnings == [f"capacity warning for {original}"]


def test_replay_capacity_probe_failure_does_not_mask_durable_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="warning-probe-failure")
    monkeypatch.setattr(
        submit,
        "_impossible_capacity_warnings",
        lambda **_kwargs: (_ for _ in ()).throw(
            KeyboardInterrupt("simulated replay probe interrupt")
        ),
    )

    replayed = _submit(
        source,
        queue_dir,
        jobs_dir,
        key="warning-probe-failure",
    )

    assert replayed == original


def test_same_key_with_changed_intent_is_hard_conflict(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    first = _submit(source, queue_dir, jobs_dir, key="same-key", cpus=1)

    with pytest.raises(submit.IdempotencyConflict, match="different submission intent"):
        _submit(source, queue_dir, jobs_dir, key="same-key", cpus=2)

    assert [path.stem for path in queue_dir.glob("*.json")] == [first]
    assert [path.name for path in jobs_dir.iterdir()] == [first]


def test_same_key_with_changed_payload_is_hard_conflict(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("print(1)\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    _submit(source, queue_dir, jobs_dir, key="payload-key")
    source.write_text("print(2)\n")

    with pytest.raises(submit.IdempotencyConflict):
        _submit(source, queue_dir, jobs_dir, key="payload-key")

    assert len(list(queue_dir.glob("*.json"))) == 1


def test_directory_payload_digest_has_unambiguous_entry_framing(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "a").write_bytes(b"X\0f\0b\0" + b"420" + b"\0Y")
    (second / "a").write_bytes(b"X")
    (second / "b").write_bytes(b"Y")

    assert submit._payload_digest(
        source_file=None,
        source_directory=first,
        source_archive=None,
    ) != submit._payload_digest(
        source_file=None,
        source_directory=second,
        source_archive=None,
    )


def test_directory_payload_symlink_target_has_unambiguous_framing(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first-link"
    second = tmp_path / "second-link"
    first.mkdir()
    second.mkdir()
    (first / "a").symlink_to("nested/target-with-newline\nend")
    (second / "a").symlink_to("X")
    (second / "b").write_bytes(b"Y")

    assert submit._payload_digest(
        source_file=None,
        source_directory=first,
        source_archive=None,
    ) != submit._payload_digest(
        source_file=None,
        source_directory=second,
        source_archive=None,
    )


def test_accepted_key_replays_while_new_submissions_are_drained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="accepted-before-drain")
    monkeypatch.setattr(
        drain,
        "read_drain_state",
        lambda: drain.DrainState(
            enabled=True,
            reject_submits=True,
            reason="rollout",
        ),
    )

    assert (
        _submit(source, queue_dir, jobs_dir, key="accepted-before-drain")
        == original
    )
    with pytest.raises(ValueError, match="paused for update"):
        _submit(source, queue_dir, jobs_dir, key="new-during-drain")
    assert len(list(queue_dir.glob("*.json"))) == 1


def test_accepted_key_replays_after_dependency_spec_is_deleted(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    jobs_dir = tmp_path / "jobs"
    predecessor = JobSpec(
        id="predecessor",
        command=["true"],
        cwd=str(tmp_path / "predecessor"),
        cpus=1,
    )
    predecessor.write(queue_dir / "predecessor.json")
    original = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="dependency-replay",
        depends_on=["predecessor"],
    )
    (queue_dir / "predecessor.json").unlink()

    replay = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="dependency-replay",
        depends_on=["predecessor"],
    )

    assert replay == original
    assert [path.stem for path in queue_dir.glob("*.json")] == [original]


@pytest.mark.parametrize("key", [None, "invalid-dependency"])
@pytest.mark.parametrize("dependency", ["../outside", "nested/job", ".", ".."])
def test_invalid_dependency_id_is_rejected_before_state_mutation(
    tmp_path: Path,
    key: str | None,
    dependency: str,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    (tmp_path / "outside.json").write_text("{}\n")

    with pytest.raises(ValueError, match="invalid job id"):
        submit.submit_local(
            host="localhost",
            input_file=str(source),
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            idempotency_key=key,
            depends_on=[dependency],
        )

    assert not queue_dir.exists()
    assert not jobs_dir.exists()


@pytest.mark.parametrize("payload_kind", ["file", "directory", "archive"])
def test_payload_race_stages_the_immutable_pre_mutation_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_kind: str,
) -> None:
    source_file = tmp_path / "input.py"
    source_dir = tmp_path / "payload"
    source_archive = tmp_path / "payload.tar"
    submit_fields: dict[str, object]
    if payload_kind == "file":
        source_file.write_text("print('before')\n")
        submit_fields = {"input_file": str(source_file)}
        staged_name = "input.py"

        def mutate() -> None:
            source_file.write_text("print('after')\n")

    elif payload_kind == "directory":
        source_dir.mkdir()
        (source_dir / "run.py").write_text("print('before')\n")
        submit_fields = {
            "directory": str(source_dir),
            "command": ["python", "run.py"],
        }
        staged_name = "run.py"

        def mutate() -> None:
            (source_dir / "run.py").write_text("print('after')\n")

    else:
        source_file.write_text("print('before')\n")
        with tarfile.open(source_archive, "w") as archive:
            archive.add(source_file, arcname="input.py")
        submit_fields = {
            "archive": str(source_archive),
            "command": ["python", "input.py"],
        }
        staged_name = "input.py"

        def mutate() -> None:
            source_file.write_text("print('after')\n")
            with tarfile.open(source_archive, "w") as archive:
                archive.add(source_file, arcname="input.py")

    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_digest = submit._payload_digest
    calls = 0

    def racing_digest(**kwargs: object) -> tuple[str, str]:
        nonlocal calls
        result = original_digest(**kwargs)  # type: ignore[arg-type]
        calls += 1
        if calls == 1:
            mutate()
        return result

    monkeypatch.setattr(submit, "_payload_digest", racing_digest)

    jobid = submit.submit_local(
        host="localhost",
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key=f"race-{payload_kind}",
        **submit_fields,
    )

    assert (jobs_dir / jobid / staged_name).read_text() == "print('before')\n"
    assert [path.stem for path in queue_dir.glob("*.json")] == [jobid]


@pytest.mark.parametrize("payload_kind", ["file", "directory", "archive"])
def test_post_snapshot_source_swap_cannot_change_staged_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_kind: str,
) -> None:
    source_file = tmp_path / "input.py"
    source_file.write_text("pass\n")
    source_dir = tmp_path / "payload"
    source_archive = tmp_path / "payload.tar"
    submit_fields: dict[str, object]
    if payload_kind == "file":
        outside = tmp_path / "outside.py"
        outside.write_text("pass\n")
        submit_fields = {"input_file": str(source_file)}
        staged_name = "input.py"

        def mutate() -> None:
            source_file.unlink()
            source_file.symlink_to(outside)

    elif payload_kind == "directory":
        source_dir.mkdir()
        run_file = source_dir / "run.py"
        run_file.write_text("pass\n")
        outside = tmp_path / "outside.py"
        outside.write_text("pass\n")
        submit_fields = {
            "directory": str(source_dir),
            "command": ["python", "run.py"],
        }
        staged_name = "run.py"

        def mutate() -> None:
            run_file.unlink()
            run_file.symlink_to(outside)

    else:
        with tarfile.open(source_archive, "w") as archive:
            archive.add(source_file, arcname="input.py")
        submit_fields = {
            "archive": str(source_archive),
            "command": ["python", "input.py"],
        }
        staged_name = "input.py"

        def mutate() -> None:
            with tarfile.open(source_archive, "w") as archive:
                unsafe = tarfile.TarInfo("../escape")
                unsafe.size = 0
                archive.addfile(unsafe)

    original_digest = submit._payload_digest
    mutated = False

    def racing_digest(**kwargs: object) -> tuple[str, str]:
        nonlocal mutated
        if not mutated:
            mutated = True
            mutate()
        return original_digest(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(submit, "_payload_digest", racing_digest)
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    jobid = submit.submit_local(
        host="localhost",
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key=f"pre-digest-{payload_kind}",
        **submit_fields,
    )

    staged = jobs_dir / jobid / staged_name
    assert staged.is_file()
    assert not staged.is_symlink()
    assert staged.read_text() == "pass\n"


@pytest.mark.parametrize("payload_kind", ["file", "directory", "archive"])
def test_keyed_payload_aba_race_stages_the_validated_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    payload_kind: str,
) -> None:
    source_file = tmp_path / "input.py"
    source_file.write_text("print('safe')\n")
    source_dir = tmp_path / "payload"
    source_archive = tmp_path / "payload.tar"
    outside = tmp_path / "outside.py"
    outside.write_text("print('unsafe')\n")
    submit_fields: dict[str, object]

    def set_safe() -> None:
        if payload_kind == "file":
            source_file.unlink(missing_ok=True)
            source_file.write_text("print('safe')\n")
        elif payload_kind == "directory":
            run_file = source_dir / "run.py"
            run_file.unlink(missing_ok=True)
            run_file.write_text("print('safe')\n")
        else:
            with tarfile.open(source_archive, "w") as archive_file:
                archive_file.add(source_file, arcname="input.py")

    def set_unsafe() -> None:
        if payload_kind == "file":
            source_file.unlink(missing_ok=True)
            source_file.symlink_to(outside)
        elif payload_kind == "directory":
            run_file = source_dir / "run.py"
            run_file.unlink(missing_ok=True)
            run_file.symlink_to(outside)
        else:
            with tarfile.open(source_archive, "w") as archive_file:
                empty = tarfile.TarInfo("data.txt")
                empty.size = 0
                archive_file.addfile(empty)

    if payload_kind == "file":
        submit_fields = {"input_file": str(source_file)}
        expected = "input.py"
    elif payload_kind == "directory":
        source_dir.mkdir()
        (source_dir / "run.py").write_text("print('safe')\n")
        submit_fields = {
            "directory": str(source_dir),
            "command": ["python", "run.py"],
        }
        expected = "run.py"
    else:
        set_safe()
        submit_fields = {
            "archive": str(source_archive),
            "command": ["python", "input.py"],
        }
        expected = "input.py"

    original_digest = submit._payload_digest
    first_digest = True

    def aba_digest(**kwargs: object) -> tuple[str, str]:
        nonlocal first_digest
        if first_digest:
            first_digest = False
            set_unsafe()
            result = original_digest(**kwargs)  # type: ignore[arg-type]
            set_safe()
            return result
        return original_digest(**kwargs)  # type: ignore[arg-type]

    original_commit = submit._commit_local_submission

    def aba_commit(**kwargs: object) -> str:
        set_unsafe()
        return original_commit(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(submit, "_payload_digest", aba_digest)
    monkeypatch.setattr(submit, "_commit_local_submission", aba_commit)
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    jobid = submit.submit_local(
        host="localhost",
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key=f"aba-{payload_kind}",
        **submit_fields,
    )

    staged = jobs_dir / jobid / expected
    assert staged.is_file()
    assert not staged.is_symlink()
    assert staged.read_text() == "print('safe')\n"


def test_keyed_preflight_cannot_mutate_dispatched_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("print('immutable')\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    def mutating_preflight(workspace: Path, _command: list[str]) -> PreflightResult:
        (workspace / "input.py").write_text("print('mutated')\n")
        return PreflightResult(expected_outputs=["result.out"], output_stem="result")

    monkeypatch.setattr(submit, "vibeqc_dry_run_preflight", mutating_preflight)
    jobid = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="preflight-isolated",
        vibeqc_preflight=True,
    )

    assert (jobs_dir / jobid / "input.py").read_text() == "print('immutable')\n"
    spec = JobSpec.read(queue_dir / f"{jobid}.json")
    assert spec.expected_outputs == ["result.out"]
    assert spec.output_stem == "result"


def test_workspace_fsync_failure_precedes_spec_and_claim_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    published: list[str] = []
    claimed: list[str] = []

    def fail_workspace_fsync(_workspace_fd: int) -> None:
        raise OSError("simulated staged-byte fsync failure")

    monkeypatch.setattr(
        submit,
        "_fsync_workspace_tree_at",
        fail_workspace_fsync,
    )
    monkeypatch.setattr(
        submit,
        "_write_spec_exclusive",
        lambda *_args, **_kwargs: published.append("spec"),
    )
    monkeypatch.setattr(
        submit,
        "_write_idempotency_claim",
        lambda *_args, **_kwargs: claimed.append("claim"),
    )

    with pytest.raises(OSError, match="staged-byte fsync"):
        _submit(source, queue_dir, jobs_dir, key="fsync-before-publication")

    assert published == []
    assert claimed == []
    assert list(queue_dir.glob("*.json")) == []
    [orphan] = list(jobs_dir.iterdir())
    assert (orphan / source.name).is_file()


def test_submitted_event_failure_does_not_mask_keyed_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(
        submit.events,
        "append_event",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("simulated submitted-event durability failure")
        ),
    )

    jobid = _submit(
        source,
        queue_dir,
        jobs_dir,
        key="event-after-acceptance",
    )

    assert JobSpec.read(queue_dir / f"{jobid}.json").id == jobid
    claims = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    assert len(claims) == 1
    assert json.loads(claims[0].read_text())["job_id"] == jobid
    assert (jobs_dir / jobid / "input.py").is_file()


def test_capacity_warning_probe_failure_precedes_keyed_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(
        submit,
        "_impossible_capacity_warnings",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("simulated capacity probe failure")
        ),
    )

    with pytest.raises(RuntimeError, match="capacity probe failure"):
        _submit(source, queue_dir, jobs_dir, key="capacity-before-acceptance")

    assert not queue_dir.exists()
    assert not jobs_dir.exists()


def test_crash_gap_scan_uses_held_queue_authority_during_path_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    key = "held-queue-authority"
    original_job = _submit(source, queue_dir, jobs_dir, key=key)
    [claim] = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    claim.unlink()
    empty_replacement = tmp_path / "empty-replacement"
    empty_replacement.mkdir()
    original_open = submit._open_real_directory

    def open_with_path_aba(path: Path) -> int:
        if Path(path) == queue_dir and any(
            frame.function == "_lookup_idempotent_submission"
            for frame in inspect.stack()
        ):
            return original_open(empty_replacement)
        return original_open(path)

    monkeypatch.setattr(submit, "_open_real_directory", open_with_path_aba)

    replayed = _submit(source, queue_dir, jobs_dir, key=key)

    assert replayed == original_job
    assert sorted(path.name for path in queue_dir.glob("*.json")) == [
        f"{original_job}.json"
    ]


def test_accepted_key_replays_before_drain_observation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="replay-before-drain")
    observed: list[bool] = []

    def fail_drain_observation():  # type: ignore[no-untyped-def]
        observed.append(True)
        raise RuntimeError("simulated unsafe drain fallback")

    monkeypatch.setattr(submit.drain, "read_drain_state", fail_drain_observation)

    assert (
        _submit(source, queue_dir, jobs_dir, key="replay-before-drain")
        == original
    )
    assert observed == []

    with pytest.raises(RuntimeError, match="unsafe drain fallback"):
        _submit(source, queue_dir, jobs_dir, key="new-key-observes-drain")
    assert observed == [True]
    assert sorted(path.stem for path in queue_dir.glob("*.json")) == [original]


def test_keyed_spec_publication_uses_held_queue_during_path_aba(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    detached_queue = tmp_path / "detached-queue"
    replacement_queue = tmp_path / "replacement-queue"
    original_publish = submit._write_spec_exclusive

    def publish_during_aba(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        queue_dir.rename(detached_queue)
        queue_dir.mkdir()
        try:
            original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]
        finally:
            queue_dir.rename(replacement_queue)
            detached_queue.rename(queue_dir)

    monkeypatch.setattr(submit, "_write_spec_exclusive", publish_during_aba)

    jobid = _submit(source, queue_dir, jobs_dir, key="queue-path-aba")

    assert JobSpec.read(queue_dir / f"{jobid}.json").id == jobid
    assert list(replacement_queue.glob("*.json")) == []
    [claim] = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    assert json.loads(claim.read_text())["job_id"] == jobid


def test_one_way_queue_detach_rolls_back_held_spec_and_exact_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    detached_queue = tmp_path / "detached-queue"
    original_publish = submit._write_spec_exclusive

    def detach_queue_before_publish(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        queue_dir.rename(detached_queue)
        queue_dir.mkdir()
        original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        submit,
        "_write_spec_exclusive",
        detach_queue_before_publish,
    )

    with pytest.raises(ValueError, match="idempotency authority was replaced"):
        _submit(source, queue_dir, jobs_dir, key="queue-one-way-detach")

    assert list(detached_queue.glob("*.json")) == []
    assert list(queue_dir.glob("*.json")) == []
    [orphan] = list(jobs_dir.iterdir())
    assert (orphan / source.name).is_file()
    assert list(detached_queue.rglob("*.json")) == []


def test_keyed_workspace_parent_detach_fails_without_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    detached_jobs = tmp_path / "detached-jobs"
    original_publish = submit._write_spec_exclusive

    def detach_jobs_before_publish(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        jobs_dir.rename(detached_jobs)
        jobs_dir.mkdir()
        original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        submit,
        "_write_spec_exclusive",
        detach_jobs_before_publish,
    )

    with pytest.raises(ValueError, match="workspace authority was replaced"):
        _submit(source, queue_dir, jobs_dir, key="jobs-parent-detach")

    assert list(queue_dir.glob("*.json")) == []
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []
    [orphan] = list(detached_jobs.iterdir())
    assert (orphan / source.name).is_file()


@pytest.mark.parametrize("rollback_failure", ["unlink", "queue-fsync"])
def test_workspace_authority_rollback_retains_payload_when_spec_removal_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rollback_failure: str,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_publish = submit._write_spec_exclusive
    original_unlink = os.unlink
    original_fsync = submit._fsync_durable_directory
    published_id: str | None = None

    def publish_then_detach(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        nonlocal published_id
        original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]
        published_id = spec.id
        detached = tmp_path / "detached-jobs-rollback"
        jobs_dir.rename(detached)
        jobs_dir.mkdir()

    def fail_spec_unlink(
        path: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        if kwargs.get("dir_fd") is not None and str(path).endswith(".json"):
            raise OSError(errno.EIO, "simulated spec unlink failure")
        original_unlink(path, *args, **kwargs)  # type: ignore[arg-type]

    def fail_queue_fsync(descriptor: int) -> None:
        if published_id is not None and os.fstat(descriptor).st_ino == queue_dir.stat().st_ino:
            raise OSError(errno.EIO, "simulated queue rollback fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(submit, "_write_spec_exclusive", publish_then_detach)
    if rollback_failure == "unlink":
        monkeypatch.setattr(submit.os, "unlink", fail_spec_unlink)
    else:
        monkeypatch.setattr(submit, "_fsync_durable_directory", fail_queue_fsync)

    with pytest.raises(ValueError, match="workspace authority was replaced"):
        _submit(source, queue_dir, jobs_dir, key=f"rollback-{rollback_failure}")

    assert published_id is not None
    detached_workspace = (
        tmp_path / "detached-jobs-rollback" / published_id
    )
    assert (detached_workspace / source.name).is_file()
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []
    if rollback_failure == "unlink":
        assert (queue_dir / f"{published_id}.json").is_file()


def test_keyed_workspace_parent_aba_retains_exact_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    detached_jobs = tmp_path / "detached-jobs"
    replacement_jobs = tmp_path / "replacement-jobs"
    original_publish = submit._write_spec_exclusive

    def aba_jobs_during_publish(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        jobs_dir.rename(detached_jobs)
        jobs_dir.mkdir()
        try:
            original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]
        finally:
            jobs_dir.rename(replacement_jobs)
            detached_jobs.rename(jobs_dir)

    monkeypatch.setattr(submit, "_write_spec_exclusive", aba_jobs_during_publish)

    jobid = _submit(source, queue_dir, jobs_dir, key="jobs-parent-aba")

    spec = JobSpec.read(queue_dir / f"{jobid}.json")
    assert Path(spec.cwd) == jobs_dir / jobid
    assert (jobs_dir / jobid / "input.py").read_text() == "pass\n"
    assert list(replacement_jobs.iterdir()) == []


def test_warning_consumer_failure_does_not_mask_local_acceptance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(
        submit,
        "_impossible_capacity_warnings",
        lambda **_kwargs: ("bounded warning",),
    )

    jobid = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="warning-after-acceptance",
        warning_sink=lambda _message: (_ for _ in ()).throw(
            RuntimeError("broken warning consumer")
        ),
    )

    assert JobSpec.read(queue_dir / f"{jobid}.json").id == jobid


def test_unkeyed_repeat_submissions_remain_distinct(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    first = _submit(source, queue_dir, jobs_dir, key=None)
    second = _submit(source, queue_dir, jobs_dir, key=None)

    assert first != second
    assert len(list(queue_dir.glob("*.json"))) == 2


@pytest.mark.parametrize(
    "key",
    ["", " leading", "trailing ", "slash/key", "line\nbreak", "x" * 129],
)
def test_invalid_key_is_rejected_before_state_mutation(
    tmp_path: Path,
    key: str,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    with pytest.raises(ValueError, match="idempotency key"):
        _submit(source, queue_dir, jobs_dir, key=key)

    assert not queue_dir.exists()
    assert not jobs_dir.exists()


def test_key_is_rejected_for_array_or_chain_metadata_before_mutation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    for kind, fields in (
        ("array", {"array_index": 0, "array_total": 2, "array_group_id": "group"}),
        ("chain", {"chain_index": 0, "chain_total": 2, "chain_group_id": "group"}),
    ):
        queue_dir = tmp_path / kind / "queue"
        jobs_dir = tmp_path / kind / "jobs"
        with pytest.raises(ValueError, match="single logical job"):
            submit.submit_local(
                host="localhost",
                input_file=str(source),
                queue_dir=queue_dir,
                jobs_dir=jobs_dir,
                idempotency_key="one-only",
                **fields,
            )
        assert not queue_dir.exists()
        assert not jobs_dir.exists()


def test_crash_gap_is_repaired_from_durable_spec(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._write_idempotency_claim
    attempts = 0

    def fail_first(*args: object, **kwargs: object) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("simulated crash after spec fsync")
        original(*args, **kwargs)

    monkeypatch.setattr(submit, "_write_idempotency_claim", fail_first)
    with pytest.raises(OSError, match="simulated crash"):
        _submit(source, queue_dir, jobs_dir, key="repair-me")

    (spec_path,) = list(queue_dir.glob("*.json"))
    original_jobid = spec_path.stem
    repaired = _submit(source, queue_dir, jobs_dir, key="repair-me")

    assert repaired == original_jobid
    assert len(list(queue_dir.glob("*.json"))) == 1
    assert attempts == 2


def test_claim_tombstone_survives_ordinary_job_deletion(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="durable-tombstone")
    (queue_dir / f"{original}.json").unlink()
    caplog.clear()

    replay = _submit(source, queue_dir, jobs_dir, key="durable-tombstone")

    assert replay == original
    assert list(queue_dir.glob("*.json")) == []
    assert "capacity warning probe failed" not in caplog.text


def test_cleanup_repairs_crash_gap_claim_before_deleting_bound_spec(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="cleanup-gap")
    (claim_path,) = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    claim_path.unlink()
    spec_path = queue_dir / f"{original}.json"
    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.exit_code = 0
    spec.write(spec_path)

    cleanup.delete_job(spec, queue_dir=queue_dir)
    replay = _submit(source, queue_dir, jobs_dir, key="cleanup-gap")

    assert replay == original
    assert list(queue_dir.glob("*.json")) == []
    assert claim_path.is_file()


def test_existing_claim_replays_during_drain_even_if_lock_file_was_lost(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="lost-lock")
    (claim_path,) = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    lock_path = claim_path.with_suffix(".lock")
    lock_path.unlink()
    monkeypatch.setattr(
        drain,
        "read_drain_state",
        lambda: drain.DrainState(enabled=True, reject_submits=True),
    )

    assert _submit(source, queue_dir, jobs_dir, key="lost-lock") == original


def test_bound_spec_repairs_lost_claim_and_lock_during_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    jobs_dir = tmp_path / "jobs"
    predecessor = JobSpec(
        id="predecessor",
        command=["true"],
        cwd=str(tmp_path / "predecessor"),
        cpus=1,
    )
    predecessor.write(queue_dir / "predecessor.json")
    original = submit.submit_local(
        host="localhost",
        input_file=str(source),
        queue_dir=queue_dir,
        jobs_dir=jobs_dir,
        idempotency_key="lost-claim-and-lock",
        depends_on=["predecessor"],
    )
    (claim_path,) = list(
        (queue_dir / ".submit-idempotency").rglob("*.json")
    )
    claim_path.unlink()
    claim_path.with_suffix(".lock").unlink()
    (queue_dir / "predecessor.json").unlink()
    monkeypatch.setattr(
        drain,
        "read_drain_state",
        lambda: drain.DrainState(enabled=True, reject_submits=True),
    )

    assert (
        submit.submit_local(
            host="localhost",
            input_file=str(source),
            queue_dir=queue_dir,
            jobs_dir=jobs_dir,
            idempotency_key="lost-claim-and-lock",
            depends_on=["predecessor"],
        )
        == original
    )
    assert claim_path.is_file()


def test_claim_temp_is_removed_when_claim_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_write = os.write
    failed = False

    def fail_claim_write(descriptor: int, data: bytes | memoryview) -> int:
        nonlocal failed
        if not failed and b"vq.submit-idempotency.v1" in bytes(data):
            failed = True
            raise OSError(errno.EIO, "simulated claim write failure")
        return original_write(descriptor, data)

    monkeypatch.setattr(os, "write", fail_claim_write)
    with pytest.raises(OSError, match="claim write failure"):
        _submit(source, queue_dir, jobs_dir, key="claim-temp-cleanup")

    assert failed
    assert list((queue_dir / ".submit-idempotency").rglob("*.tmp")) == []


def test_idempotency_owner_directory_fsync_io_error_is_not_suppressed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_fsync = os.fsync
    failed = False

    def fail_owner_directory(descriptor: int) -> None:
        nonlocal failed
        metadata = os.fstat(descriptor)
        namespace = queue_dir / ".submit-idempotency"
        owners = list(namespace.iterdir()) if namespace.is_dir() else []
        if (
            not failed
            and stat.S_ISDIR(metadata.st_mode)
            and owners
            and metadata.st_ino == owners[0].stat().st_ino
        ):
            failed = True
            raise OSError(errno.EIO, "simulated owner directory fsync failure")
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", fail_owner_directory)
    with pytest.raises(OSError, match="owner directory fsync failure"):
        _submit(source, queue_dir, jobs_dir, key="claim-directory-fsync")

    assert failed
    assert list(queue_dir.glob("*.json")) == []
    assert not jobs_dir.exists()


def test_unsupported_idempotency_directory_fsync_is_tolerated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_fsync = os.fsync

    def reject_directory_fsync(descriptor: int) -> None:
        if stat.S_ISDIR(os.fstat(descriptor).st_mode):
            raise OSError(errno.EINVAL, "directory fsync unsupported")
        original_fsync(descriptor)

    monkeypatch.setattr(os, "fsync", reject_directory_fsync)
    jobid = _submit(
        source,
        queue_dir,
        jobs_dir,
        key="unsupported-directory-fsync",
    )

    assert (queue_dir / f"{jobid}.json").is_file()
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_retry_repairs_transient_namespace_parent_fsync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._fsync_durable_directory
    failed = False
    successful_retry = False

    def fail_once(descriptor: int) -> None:
        nonlocal failed, successful_retry
        if not failed:
            failed = True
            raise OSError(errno.EIO, "simulated namespace parent fsync")
        successful_retry = True
        original(descriptor)

    monkeypatch.setattr(submit, "_fsync_durable_directory", fail_once)
    with pytest.raises(OSError, match="namespace parent fsync"):
        _submit(source, queue_dir, jobs_dir, key="namespace-fsync-repair")
    assert list(queue_dir.glob("*.json")) == []

    jobid = _submit(source, queue_dir, jobs_dir, key="namespace-fsync-repair")

    assert successful_retry
    assert (queue_dir / f"{jobid}.json").is_file()


def test_child_authority_fd_is_closed_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "parent"
    parent.mkdir()
    parent_fd = os.open(parent, os.O_RDONLY)
    original_open = os.open
    original_close = os.close
    child_descriptors: list[int] = []
    closed: list[int] = []

    def observe_open(path: str | bytes | os.PathLike[str], *args: object, **kwargs: object) -> int:
        descriptor = original_open(path, *args, **kwargs)  # type: ignore[arg-type]
        if path == "child":
            child_descriptors.append(descriptor)
        return descriptor

    def observe_close(descriptor: int) -> None:
        closed.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(submit.os, "open", observe_open)
    monkeypatch.setattr(submit.os, "close", observe_close)
    monkeypatch.setattr(
        submit,
        "_fsync_durable_directory",
        lambda _descriptor: (_ for _ in ()).throw(
            OSError(errno.EIO, "simulated parent fsync")
        ),
    )

    with pytest.raises(OSError, match="parent fsync"):
        parent_metadata = os.fstat(parent_fd)
        submit._open_child_authority_directory(
            parent_fd,
            "child",
            create=True,
            authority_uid=parent_metadata.st_uid,
            authority_gid=parent_metadata.st_gid,
        )

    assert len(child_descriptors) == 1
    assert child_descriptors[0] in closed
    original_close(parent_fd)


def test_retry_repairs_transient_claim_parent_fsync_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._fsync_durable_directory
    failed = False
    repaired = False

    def fail_claim_parent_once(descriptor: int) -> None:
        nonlocal failed, repaired
        names = os.listdir(descriptor)
        claim_present = any(name.endswith(".json") for name in names)
        if claim_present and not failed:
            failed = True
            raise OSError(errno.EIO, "simulated claim parent fsync")
        if claim_present:
            repaired = True
        original(descriptor)

    monkeypatch.setattr(
        submit,
        "_fsync_durable_directory",
        fail_claim_parent_once,
    )
    with pytest.raises(OSError, match="claim parent fsync"):
        _submit(source, queue_dir, jobs_dir, key="claim-fsync-repair")
    (spec_path,) = list(queue_dir.glob("*.json"))

    replay = _submit(source, queue_dir, jobs_dir, key="claim-fsync-repair")

    assert replay == spec_path.stem
    assert repaired
    assert len(list(queue_dir.glob("*.json"))) == 1


def test_spec_parent_fsync_failure_precedes_claim_and_retry_repairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._fsync_durable_directory
    failed = False
    repaired = False

    def fail_spec_parent_once(descriptor: int) -> None:
        nonlocal failed, repaired
        names = os.listdir(descriptor)
        spec_present = any(
            name.endswith(".json") and name[0] != "." for name in names
        )
        if spec_present and not failed:
            failed = True
            raise OSError(errno.EIO, "simulated spec parent fsync")
        if spec_present:
            repaired = True
        original(descriptor)

    monkeypatch.setattr(
        submit,
        "_fsync_durable_directory",
        fail_spec_parent_once,
    )
    with pytest.raises(OSError, match="spec parent fsync"):
        _submit(source, queue_dir, jobs_dir, key="spec-fsync-repair")
    (spec_path,) = list(queue_dir.glob("*.json"))
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []
    assert (jobs_dir / spec_path.stem).is_dir()

    replay = _submit(source, queue_dir, jobs_dir, key="spec-fsync-repair")

    assert replay == spec_path.stem
    assert repaired
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_idempotency_lock_symlink_is_rejected_without_touching_target(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = _submit(source, queue_dir, jobs_dir, key="symlink-lock")
    (claim_path,) = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    lock_path = claim_path.with_suffix(".lock")
    lock_path.unlink()
    target = tmp_path / "lock-target"
    target.write_text("do-not-touch\n")
    lock_path.symlink_to(target)

    with pytest.raises((OSError, ValueError)):
        _submit(source, queue_dir, jobs_dir, key="symlink-lock")

    assert target.read_text() == "do-not-touch\n"
    assert JobSpec.read(queue_dir / f"{original}.json").id == original


def test_initial_jobs_directory_fsync_failure_closes_fd_without_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._fsync_durable_directory
    opened_jobs_fds: list[int] = []
    closed_fds: list[int] = []

    def fail_jobs_fsync(descriptor: int) -> None:
        metadata = os.fstat(descriptor)
        if jobs_dir.exists() and metadata.st_ino == jobs_dir.stat().st_ino:
            opened_jobs_fds.append(descriptor)
            raise OSError(errno.EIO, "simulated jobs-directory fsync")
        original(descriptor)

    original_close = os.close

    def observe_close(descriptor: int) -> None:
        closed_fds.append(descriptor)
        original_close(descriptor)

    monkeypatch.setattr(submit, "_fsync_durable_directory", fail_jobs_fsync)
    monkeypatch.setattr(submit.os, "close", observe_close)

    with pytest.raises(OSError, match="jobs-directory fsync"):
        _submit(source, queue_dir, jobs_dir, key="jobs-fsync-failure")

    assert opened_jobs_fds
    assert all(descriptor in closed_fds for descriptor in opened_jobs_fds)
    assert list(queue_dir.glob("*.json")) == []
    assert list(jobs_dir.iterdir()) == []
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []


def test_initial_workspace_authority_failure_rolls_back_exact_reservation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"

    monkeypatch.setattr(
        submit,
        "_revalidate_workspace_authority",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            submit._AuthorityReplacedError("simulated workspace authority")
        ),
    )

    with pytest.raises(ValueError, match="workspace authority"):
        _submit(source, queue_dir, jobs_dir, key="initial-workspace-authority")

    assert list(queue_dir.glob("*.json")) == []
    [orphan] = list(jobs_dir.iterdir())
    assert list(orphan.iterdir()) == []
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []


def test_workspace_replacement_is_never_deleted_during_authority_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._revalidate_workspace_authority
    calls = 0
    detached: Path | None = None

    def replace_workspace(
        jobs_path: Path,
        jobs_fd: int,
        job_id: str,
        workspace_fd: int,
    ) -> None:
        nonlocal calls, detached
        calls += 1
        if calls == 2:
            detached = jobs_dir / f"{job_id}.detached"
            (jobs_dir / job_id).rename(detached)
            replacement = jobs_dir / job_id
            replacement.mkdir()
            (replacement / "sentinel").write_text("do-not-delete\n")
        original(jobs_path, jobs_fd, job_id, workspace_fd)

    monkeypatch.setattr(
        submit,
        "_revalidate_workspace_authority",
        replace_workspace,
    )

    with pytest.raises(ValueError, match="workspace authority"):
        _submit(source, queue_dir, jobs_dir, key="replacement-workspace")

    replacement = next(
        path for path in jobs_dir.iterdir() if path.name != detached.name
    )
    assert (replacement / "sentinel").read_text() == "do-not-delete\n"
    assert list(queue_dir.glob("*.json")) == []
    assert list((queue_dir / ".submit-idempotency").rglob("*.json")) == []


def test_owned_tree_removal_cannot_delete_close_triggered_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "parent"
    parent.mkdir()
    owned = parent / "owned"
    owned.mkdir()
    expected = owned.stat()
    parent_fd = os.open(parent, os.O_RDONLY)
    original_close = os.close
    swapped = False
    detached = parent / "owned-detached"

    def swap_when_owned_fd_closes(descriptor: int) -> None:
        nonlocal swapped
        try:
            metadata = os.fstat(descriptor)
        except OSError:
            metadata = None
        original_close(descriptor)
        if (
            not swapped
            and metadata is not None
            and (metadata.st_dev, metadata.st_ino)
            == (expected.st_dev, expected.st_ino)
            and owned.exists()
        ):
            swapped = True
            owned.rename(detached)
            owned.mkdir()

    monkeypatch.setattr(submit.os, "close", swap_when_owned_fd_closes)

    submit._remove_tree_at(parent_fd, "owned", expected_root=expected)

    assert swapped is False
    assert owned.is_dir()
    assert not detached.exists()
    original_close(parent_fd)


def test_owned_tree_removal_never_pathname_deletes_after_final_inode_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "parent"
    parent.mkdir()
    owned = parent / "owned"
    owned.mkdir()
    expected = owned.stat()
    parent_fd = os.open(parent, os.O_RDONLY)
    original_rmdir = os.rmdir
    rmdir_calls: list[str] = []

    def observe_rmdir(
        path: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        rmdir_calls.append(str(path))
        original_rmdir(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(submit.os, "rmdir", observe_rmdir)

    submit._remove_tree_at(parent_fd, "owned", expected_root=expected)

    assert rmdir_calls == []
    assert owned.is_dir()
    os.close(parent_fd)


def test_detached_owner_authority_fails_closed_and_replay_repairs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_write = submit._write_idempotency_claim
    detached: Path | None = None

    def detach_then_write(
        binding: object,
        job_id: str,
        store: object,
    ) -> None:
        nonlocal detached
        namespace = queue_dir / ".submit-idempotency"
        owner = next(path for path in namespace.iterdir() if path.is_dir())
        detached = namespace / "detached-owner"
        owner.rename(detached)
        owner.mkdir(mode=0o700)
        original_write(binding, job_id, store)  # type: ignore[arg-type]

    monkeypatch.setattr(submit, "_write_idempotency_claim", detach_then_write)
    with pytest.raises(ValueError, match="authority.*replaced"):
        _submit(source, queue_dir, jobs_dir, key="detached-authority")

    (spec_path,) = list(queue_dir.glob("*.json"))
    assert (jobs_dir / spec_path.stem).is_dir()
    assert detached is not None
    assert list((queue_dir / ".submit-idempotency").glob("*/*.json")) == []

    monkeypatch.setattr(submit, "_write_idempotency_claim", original_write)
    replay = _submit(source, queue_dir, jobs_dir, key="detached-authority")

    assert replay == spec_path.stem
    assert len(list(queue_dir.glob("*.json"))) == 1
    assert len(list((queue_dir / ".submit-idempotency").glob("*/*.json"))) == 1


def test_post_claim_workspace_detach_cannot_hide_accepted_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._write_idempotency_claim
    detached_jobs = tmp_path / "jobs-detached-after-claim"

    def claim_then_detach(
        binding: object,
        job_id: str,
        store: object,
    ) -> None:
        original(binding, job_id, store)  # type: ignore[arg-type]
        jobs_dir.rename(detached_jobs)
        jobs_dir.mkdir()

    monkeypatch.setattr(submit, "_write_idempotency_claim", claim_then_detach)

    jobid = _submit(source, queue_dir, jobs_dir, key="post-claim-detach")

    assert (queue_dir / f"{jobid}.json").is_file()
    assert (detached_jobs / jobid / source.name).is_file()
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1
    monkeypatch.setattr(submit, "_write_idempotency_claim", original)
    assert _submit(source, queue_dir, jobs_dir, key="post-claim-detach") == jobid


@pytest.mark.parametrize(
    "failure",
    [
        OSError("post-claim close failed"),
        KeyboardInterrupt("post-claim observer interrupted"),
    ],
)
def test_proven_claim_publication_failure_does_not_mask_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: BaseException,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original = submit._write_idempotency_claim

    def publish_then_fail(
        binding: object,
        job_id: str,
        store: object,
    ) -> None:
        original(binding, job_id, store)  # type: ignore[arg-type]
        raise failure

    monkeypatch.setattr(submit, "_write_idempotency_claim", publish_then_fail)

    jobid = _submit(source, queue_dir, jobs_dir, key="proven-post-claim")

    assert (queue_dir / f"{jobid}.json").is_file()
    assert (jobs_dir / jobid / source.name).is_file()
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_post_claim_store_cleanup_failure_does_not_mask_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_close = os.close
    injected = False

    def fail_first_post_claim_close(descriptor: int) -> None:
        nonlocal injected
        claims = list((queue_dir / ".submit-idempotency").rglob("*.json"))
        if not injected and claims and stat.S_ISREG(os.fstat(descriptor).st_mode):
            injected = True
            original_close(descriptor)
            raise OSError(errno.EIO, "simulated post-claim lock close failure")
        original_close(descriptor)

    monkeypatch.setattr(submit.os, "close", fail_first_post_claim_close)

    jobid = _submit(source, queue_dir, jobs_dir, key="post-claim-store-close")

    assert injected
    assert JobSpec.read(queue_dir / f"{jobid}.json").id == jobid
    assert (jobs_dir / jobid / source.name).is_file()
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_post_claim_snapshot_cleanup_interrupt_does_not_mask_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_rmtree = submit.shutil.rmtree
    injected = False

    def interrupt_snapshot_cleanup(
        path: str | bytes | os.PathLike[str],
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal injected
        claims = list((queue_dir / ".submit-idempotency").rglob("*.json"))
        if not injected and claims and "vq-submit-snapshot-" in str(path):
            injected = True
            raise KeyboardInterrupt("simulated snapshot cleanup interrupt")
        original_rmtree(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(submit.shutil, "rmtree", interrupt_snapshot_cleanup)

    jobid = _submit(source, queue_dir, jobs_dir, key="post-claim-snapshot-close")

    assert injected
    assert JobSpec.read(queue_dir / f"{jobid}.json").id == jobid
    assert (jobs_dir / jobid / source.name).is_file()
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_claim_repair_uses_queue_owner_for_root_daemon_created_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    jobid = _submit(source, queue_dir, jobs_dir, key="daemon-owner-repair")
    (claim_path,) = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    claim_path.unlink()
    spec = JobSpec.read(queue_dir / f"{jobid}.json")
    expected = queue_dir.stat()
    observed: list[tuple[int, int]] = []
    original = submit._align_authority_owner

    def observe_owner(
        descriptor: int,
        *,
        authority_uid: int,
        authority_gid: int,
    ) -> None:
        observed.append((authority_uid, authority_gid))
        original(
            descriptor,
            authority_uid=authority_uid,
            authority_gid=authority_gid,
        )

    monkeypatch.setattr(submit, "_align_authority_owner", observe_owner)
    monkeypatch.setattr(submit.os, "geteuid", lambda: 0)

    submit.ensure_idempotency_claim_for_spec(queue_dir, spec)

    assert (expected.st_uid, expected.st_gid) in observed
    repaired = claim_path.stat()
    assert repaired.st_uid == expected.st_uid
    assert stat.S_IMODE(repaired.st_mode) == 0o600


def test_cleanup_retains_bound_job_when_claim_repair_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    jobid = _submit(source, queue_dir, jobs_dir, key="cleanup-fail-closed")
    (claim_path,) = list((queue_dir / ".submit-idempotency").rglob("*.json"))
    claim_path.unlink()
    spec_path = queue_dir / f"{jobid}.json"
    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.exit_code = 0
    spec.write(spec_path)

    def fail_repair(*args: object, **kwargs: object) -> None:
        raise OSError("simulated tombstone durability failure")

    monkeypatch.setattr(submit, "_write_idempotency_claim", fail_repair)
    with pytest.raises(OSError, match="tombstone durability"):
        cleanup.delete_job(spec, queue_dir=queue_dir)

    assert spec_path.is_file()
    assert (jobs_dir / jobid).is_dir()


def test_post_publication_exception_preserves_workspace_and_repairs_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    original_publish = submit._write_spec_exclusive
    published_jobid: str | None = None

    def publish_then_interrupt(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        nonlocal published_jobid
        original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]
        published_jobid = spec.id
        raise KeyboardInterrupt("observer interrupted after spec publication")

    monkeypatch.setattr(submit, "_write_spec_exclusive", publish_then_interrupt)
    with pytest.raises(KeyboardInterrupt, match="after spec publication"):
        _submit(source, queue_dir, jobs_dir, key="post-publish-gap")

    assert published_jobid is not None
    spec_path = queue_dir / f"{published_jobid}.json"
    assert spec_path.is_file()
    assert (jobs_dir / published_jobid / source.name).is_file()

    monkeypatch.setattr(submit, "_write_spec_exclusive", original_publish)
    replay = _submit(source, queue_dir, jobs_dir, key="post-publish-gap")
    assert replay == published_jobid
    assert len(list(queue_dir.glob("*.json"))) == 1


def test_collision_winner_cannot_trigger_path_based_workspace_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    winner: JobSpec | None = None
    original_publish = submit._write_spec_exclusive

    def race_winner(
        spec: JobSpec,
        spec_path: Path,
        **kwargs: object,
    ) -> None:
        nonlocal winner
        winner = JobSpec(
            id=spec.id,
            command=["winner-command"],
            cwd=spec.cwd,
            cpus=9,
        )
        winner.write(spec_path)
        original_publish(spec, spec_path, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(submit, "_write_spec_exclusive", race_winner)
    with pytest.raises(FileExistsError, match="collision"):
        _submit(source, queue_dir, jobs_dir, key="publication-collision")

    assert winner is not None
    (spec_path,) = list(queue_dir.glob("*.json"))
    assert JobSpec.read(spec_path).command == ["winner-command"]
    assert (jobs_dir / winner.id / source.name).is_file()


@pytest.mark.parametrize("record_kind", ["fifo", "symlink", "oversize", "malformed"])
def test_claim_repair_scan_is_bounded_nofollow_and_fail_closed(
    tmp_path: Path,
    record_kind: str,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    queue_dir.mkdir()
    jobs_dir = tmp_path / "jobs"
    candidate = queue_dir / "unclassified.json"
    if record_kind == "fifo":
        os.mkfifo(candidate)
    elif record_kind == "symlink":
        target = tmp_path / "external.json"
        target.write_text("{}\n")
        candidate.symlink_to(target)
    elif record_kind == "oversize":
        with candidate.open("wb") as stream:
            stream.truncate(spec_access.SPEC_READ_MAX_BYTES + 1)
    else:
        candidate.write_text("{not-json\n")

    with pytest.raises((OSError, ValueError)):
        _submit(source, queue_dir, jobs_dir, key=f"unsafe-{record_kind}")

    assert [path.name for path in queue_dir.glob("*.json")] == [candidate.name]
    assert not jobs_dir.exists()


@pytest.mark.parametrize("record_kind", ["mismatched-id", "partial-binding"])
def test_claim_repair_rejects_misbound_spec_evidence(
    tmp_path: Path,
    record_kind: str,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    # Establish the owner's namespace without binding the key under test.
    _submit(source, queue_dir, jobs_dir, key="namespace-seed")
    if record_kind == "mismatched-id":
        bad = JobSpec(
            id="different-id",
            command=["true"],
            cwd=str(tmp_path / "bad"),
            cpus=1,
        )
    else:
        bad = JobSpec(
            id="partial",
            command=["true"],
            cwd=str(tmp_path / "bad"),
            cpus=1,
            idempotency_key_hash="a" * 64,
        )
    bad.write(queue_dir / "misbound.json")
    before = {path.name for path in queue_dir.glob("*.json")}

    with pytest.raises(ValueError):
        _submit(source, queue_dir, jobs_dir, key="new-key")

    assert {path.name for path in queue_dir.glob("*.json")} == before


def test_raw_key_is_never_persisted_in_paths_specs_or_claims(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    raw_key = "private-campaign-key-493"

    _submit(source, queue_dir, jobs_dir, key=raw_key)

    assert raw_key not in str(queue_dir)
    for path in queue_dir.rglob("*"):
        assert raw_key not in path.name
        if path.is_file():
            assert raw_key not in path.read_text(errors="replace")


def test_raw_key_is_redacted_from_logged_cli_argv() -> None:
    raw_key = "private-campaign-key-493"
    assert auth.redact_token_args(
        ["vq", "submit", "--idempotency-key", raw_key, "input.py"]
    ) == [
        "vq",
        "submit",
        "--idempotency-key",
        "<redacted>",
        "input.py",
    ]
    assert auth.redact_token_args(
        ["vq", "submit", f"--idempotency-key={raw_key}", "input.py"]
    ) == [
        "vq",
        "submit",
        "--idempotency-key=<redacted>",
        "input.py",
    ]


def test_same_key_is_scoped_by_authenticated_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    monkeypatch.setattr(os, "geteuid", lambda: 1001)
    first = _submit(source, queue_dir, jobs_dir, key="owner-scoped")
    monkeypatch.setattr(os, "geteuid", lambda: 1002)
    second = _submit(source, queue_dir, jobs_dir, key="owner-scoped")

    assert first != second
    assert len(list(queue_dir.glob("*.json"))) == 2


def test_same_key_is_scoped_by_execution_state_store(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    first = _submit(
        source,
        tmp_path / "store-a" / "queue",
        tmp_path / "store-a" / "jobs",
        key="store-scoped",
    )
    second = _submit(
        source,
        tmp_path / "store-b" / "queue",
        tmp_path / "store-b" / "jobs",
        key="store-scoped",
    )

    assert first != second
    assert (
        _submit(
            source,
            tmp_path / "store-a" / "queue",
            tmp_path / "store-a" / "jobs",
            key="store-scoped",
        )
        == first
    )
    assert (
        _submit(
            source,
            tmp_path / "store-b" / "queue",
            tmp_path / "store-b" / "jobs",
            key="store-scoped",
        )
        == second
    )


def test_concurrent_submitters_create_exactly_one_job(tmp_path: Path) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    context = multiprocessing.get_context("fork")
    start = context.Event()
    results = context.Queue()
    workers = [
        context.Process(
            target=_concurrent_submit,
            args=(str(source), str(queue_dir), str(jobs_dir), start, results),
        )
        for _ in range(6)
    ]
    for worker in workers:
        worker.start()
    start.set()
    observations = [results.get(timeout=20) for _ in workers]
    for worker in workers:
        worker.join(timeout=20)
        assert worker.exitcode == 0

    assert {observation[0] for observation in observations} == {"ok"}
    assert len({observation[1] for observation in observations}) == 1
    assert len(list(queue_dir.glob("*.json"))) == 1
    assert len(list(jobs_dir.iterdir())) == 1


def test_concurrent_same_key_different_intents_have_one_winner(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    queue_dir = tmp_path / "queue"
    jobs_dir = tmp_path / "jobs"
    context = multiprocessing.get_context("fork")
    start = context.Event()
    results = context.Queue()
    workers = [
        context.Process(
            target=_concurrent_submit_with_cpus,
            args=(
                str(source),
                str(queue_dir),
                str(jobs_dir),
                start,
                results,
                cpus,
            ),
        )
        for cpus in (1, 2)
    ]
    for worker in workers:
        worker.start()
    start.set()
    observations = [results.get(timeout=20) for _worker in workers]
    for worker in workers:
        worker.join(timeout=20)
        assert worker.exitcode == 0

    assert sorted(observation[0] for observation in observations) == [
        "error",
        "ok",
    ]
    error = next(item for item in observations if item[0] == "error")
    assert error[1] == "IdempotencyConflict"
    assert len(list(queue_dir.glob("*.json"))) == 1
    assert len(list(jobs_dir.iterdir())) == 1
    assert len(list((queue_dir / ".submit-idempotency").rglob("*.json"))) == 1


def test_canonical_intent_digest_covers_every_queue_authority_field() -> None:
    pin = ProgramRuntimePin(
        expected_git_sha="a" * 40,
        enforce_git_sha=True,
        expected_import_version="1.2.3",
        import_check="pkg",
        import_symbols=["symbol"],
        scheduler_host="cluster-a",
        resolved_executable="/managed/python",
        program_kind="scheduler-runtime",
        program_version="1.2.3",
        artifact_identity="artifact-a",
    )
    baseline = JobSpec(
        id="intent-base",
        command=["/managed/python", "run.py", "--x"],
        cwd="/queue/jobs/intent-base",
        cpus=2,
        scheduler_tasks=3,
        mem_mb=4096,
        wall_time_seconds=120,
        priority=4,
        recover_on_reboot=True,
        retry_max=2,
        job_name="paper-job",
        branch="release",
        program="vibeqc-release",
        program_runtime_pin=pin,
        tags=["paper", "wave1"],
        not_before="2026-08-12T00:00:00+00:00",
        depends_on=["dep-a"],
        depends_on_any=["dep-b"],
        rerun_until_file_exists="done.json",
        rerun_max=7,
        clean_workdir_on_terminal=True,
        scheduler_target="cluster-a",
        refresh_before="vibeqc-release",
        qvf_artifact_name="job.qvf",
    )

    def digest(candidate: JobSpec, *, preflight: bool = True) -> str:
        return submit._submission_intent_digest(
            candidate,
            payload_kind="file",
            payload_digest="f" * 64,
            vibeqc_preflight=preflight,
        )

    expected = digest(baseline)
    dispatched = baseline.model_copy(
        update={
            "program_runtime_pin": pin.model_copy(
                update={"resolved_git_sha": "b" * 40}
            )
        }
    )
    assert digest(dispatched) == expected
    variants = [
        baseline.model_copy(update={"scheduler_target": "cluster-b"}),
        baseline.model_copy(update={"program": "reference"}),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"expected_git_sha": "b" * 40}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"enforce_git_sha": False}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"expected_import_version": "2.0.0"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"import_check": "other_pkg"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"import_symbols": ["other_symbol"]}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"scheduler_host": "cluster-b"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"resolved_executable": "/other/python"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"program_kind": "other-runtime"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"program_version": "2.0.0"}
                )
            }
        ),
        baseline.model_copy(
            update={
                "program_runtime_pin": pin.model_copy(
                    update={"artifact_identity": "artifact-b"}
                )
            }
        ),
        baseline.model_copy(update={"job_name": "other-job"}),
        baseline.model_copy(update={"command": ["python", "other.py"]}),
        baseline.model_copy(update={"cpus": 3}),
        baseline.model_copy(update={"scheduler_tasks": 4}),
        baseline.model_copy(update={"mem_mb": 8192}),
        baseline.model_copy(update={"wall_time_seconds": 121}),
        baseline.model_copy(update={"priority": 5}),
        baseline.model_copy(update={"depends_on": ["dep-c"]}),
        baseline.model_copy(update={"depends_on_any": ["dep-d"]}),
        baseline.model_copy(update={"tags": ["other"]}),
        baseline.model_copy(update={"recover_on_reboot": False}),
        baseline.model_copy(update={"retry_max": 3}),
        baseline.model_copy(update={"branch": "dev"}),
        baseline.model_copy(update={"not_before": None}),
        baseline.model_copy(update={"rerun_until_file_exists": "other"}),
        baseline.model_copy(update={"rerun_max": 8}),
        baseline.model_copy(update={"clean_workdir_on_terminal": False}),
        baseline.model_copy(update={"refresh_before": None}),
        baseline.model_copy(update={"qvf_artifact_name": "other.qvf"}),
    ]
    assert all(digest(candidate) != expected for candidate in variants)
    assert digest(baseline, preflight=False) != expected
    equivalent_tags = JobSpec.model_validate(
        {**baseline.model_dump(mode="python"), "tags": ["wave1", "paper", "paper"]}
    )
    assert digest(equivalent_tags) == expected


def test_remote_key_is_forwarded_once_and_rejects_multi_job_variants(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "input.py"
    source.write_text("pass\n")
    host_cfg = HostConfig(ssh="example", remote_vq="vq", remote_python="python3")
    observed: list[tuple[str, ...]] = []

    monkeypatch.setattr(submit.transport, "upload_file", lambda *_args: None)
    monkeypatch.setattr(
        submit.transport,
        "run_remote_shell",
        lambda *_args, **_kwargs: None,
    )

    class Result:
        stdout = "abc123def456\n"
        stderr = ""

    def remote_vq(
        _cfg: HostConfig,
        *args: str,
        **_kwargs: object,
    ) -> Result:
        observed.append(args)
        return Result()

    monkeypatch.setattr(submit.transport, "run_remote_vq", remote_vq)
    assert submit.submit_remote(
        host="example",
        host_cfg=host_cfg,
        input_file=str(source),
        idempotency_key="remote-once",
    ) == ["abc123def456"]
    assert observed[0].count("--idempotency-key") == 1
    index = observed[0].index("--idempotency-key")
    assert observed[0][index + 1] == "remote-once"

    for fields in ({"array": 2}, {"chain": 2}):
        with pytest.raises(ValueError, match="single logical job"):
            submit.submit_remote(
                host="example",
                host_cfg=host_cfg,
                input_file=str(source),
                idempotency_key="remote-once",
                **fields,
            )
    assert len(observed) == 1
