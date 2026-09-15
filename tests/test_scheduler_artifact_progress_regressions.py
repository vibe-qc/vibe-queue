"""Scheduler-facing regression targets for calculation artifacts.

The scheduler workspace is authoritative while a cluster job is live.  These
tests keep that contract hermetic by replacing the dispatcher with an in-memory
fake; no SSH command or scheduler process is started.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from vq import config, logs, paths
from vq import status as status_module
from vq.scheduler_dialect import TorqueDialect
from vq.scheduler_dispatch import (
    RemoteResult,
    SchedulerDispatcher,
    SchedulerError,
    SchedulerFileChunk,
    SchedulerHandle,
)
from vq.spec import JobSpec, JobState

OUT_RELATIVE = "results/chemistry.out"
PROGRESS_RELATIVE = "results/chemistry.scf.jsonl"
SYSTEM_RELATIVE = "results/chemistry.system"


@pytest.fixture
def scheduler_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, config.Config]:
    state_root = tmp_path / "state"
    (state_root / "queue").mkdir(parents=True)
    config_root = tmp_path / "config"
    config_root.mkdir()
    (config_root / "config.toml").write_text(
        'default_host = "localhost"\n'
        "\n"
        "[hosts.host_f]\n"
        'ssh = "host_f"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/home/USER"\n'
        'scheduler_driver = "localhost"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_root))
    monkeypatch.setattr(status_module, "is_daemon_serving", lambda **_kw: True)
    return tmp_path, config.load_config()


def _write_scheduler_spec(
    root: Path,
    *,
    state: JobState = JobState.RUNNING,
    jobid: str = "scheduler-artifact-job",
) -> tuple[Path, Path]:
    workspace = root / "workspaces" / jobid
    (workspace / "results").mkdir(parents=True)
    (workspace / "stdout.log").write_text("", encoding="utf-8")
    (workspace / "stderr.log").write_text("", encoding="utf-8")
    fields: dict[str, object] = {
        "id": jobid,
        "command": ["python", "calculation.py"],
        "cwd": str(workspace),
        "cpus": 1,
        "state": state,
        "submitted_at": "2026-08-08T12:00:00+00:00",
        "started_at": "2026-08-08T12:01:00+00:00",
        "scheduler_target": "host_f",
        "scheduler_job_id": "555.cluster",
        "output_stem": "chemistry",
        "expected_outputs": [OUT_RELATIVE, SYSTEM_RELATIVE],
    }
    if state == JobState.COMPLETED:
        fields.update(
            finished_at="2026-08-08T12:02:00+00:00",
            exit_code=0,
        )
    spec_path = root / "state" / "queue" / f"{jobid}.json"
    JobSpec(**fields).write(spec_path)
    return spec_path, workspace


def _scf_row(iteration: int, energy: float) -> str:
    return json.dumps(
        {
            "event": "scf_iter",
            "iter": iteration,
            "energy": energy,
            "dE": -0.1 / iteration,
            "grad_norm": 10.0 ** (-iteration),
            "diis_subspace": iteration,
        }
    )


@dataclass
class _FakeArtifactDispatcher:
    files: dict[str, str] = field(default_factory=dict)
    tail_error: SchedulerError | None = None
    since_responses: dict[str, list[SchedulerFileChunk | None]] = field(
        default_factory=dict
    )
    calls: list[tuple[str, str, int | None]] = field(default_factory=list)
    handles: list[SchedulerHandle] = field(default_factory=list)
    _last_sizes: dict[str, int] = field(default_factory=dict)

    def remote_workspace(self, jobid: str) -> str:
        return f"/remote/ws/{jobid}"

    def tail_file(
        self,
        handle: SchedulerHandle,
        *,
        filename: str,
        lines: int | None = 50,
    ) -> str:
        self.handles.append(handle)
        self.calls.append(("tail_file", filename, lines))
        if self.tail_error is not None:
            raise self.tail_error
        text = self.files.get(filename, "")
        if lines is None or not text:
            return text
        split = text.splitlines(keepends=True)
        return "".join(split[-lines:])

    def tail_file_since(
        self,
        handle: SchedulerHandle,
        *,
        filename: str,
        byte_offset: int,
    ) -> SchedulerFileChunk | None:
        self.handles.append(handle)
        self.calls.append(("tail_file_since", filename, byte_offset))
        responses = self.since_responses.get(filename)
        if responses:
            response = responses.pop(0)
            if response is not None:
                self._last_sizes[filename] = response.end_offset
            return response
        if filename not in self.files:
            return None
        data = self.files[filename].encode("utf-8")
        size = self._last_sizes.get(filename, len(data))
        reset = size < byte_offset
        start = 0 if reset else byte_offset
        return SchedulerFileChunk(size, data[start:], reset=reset)

    def tail_log(
        self,
        handle: SchedulerHandle,
        *,
        lines: int | None = 200,
        stream: str = "stdout",
        array_index: int | None = None,
    ) -> str:
        del array_index
        self.handles.append(handle)
        self.calls.append(("tail_log", stream, lines))
        return self.files.get(f"{stream}.log", "")


def _install_dispatcher(
    monkeypatch: pytest.MonkeyPatch,
    fake: _FakeArtifactDispatcher,
) -> None:
    monkeypatch.setattr(logs, "scheduler_dispatcher_for", lambda _cfg: fake)
    monkeypatch.setattr(
        status_module,
        "scheduler_dispatcher_for",
        lambda _cfg: fake,
    )


def _mark_completed(spec_path: Path) -> None:
    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.finished_at = "2026-08-08T12:02:00+00:00"
    spec.exit_code = 0
    spec.write(spec_path)


def test_running_scheduler_output_and_json_use_remote_declared_artifact(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    _spec_path, workspace = _write_scheduler_spec(root)
    (workspace / OUT_RELATIVE).write_text("local stale\n", encoding="utf-8")
    fake = _FakeArtifactDispatcher(
        files={OUT_RELATIVE: "remote first\nremote second\nremote final\n"}
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = logs.show_output(
        "localhost",
        "scheduler-artifact-job",
        tail=1,
        cfg=cfg,
    )
    payload = json.loads(
        logs.show_output_json(
            "localhost",
            "scheduler-artifact-job",
            tail=1,
            cfg=cfg,
        )
    )

    assert "remote final" in rendered
    assert "local stale" not in rendered
    assert "... (2 earlier lines)" in rendered
    assert payload["out"] == "... (2 earlier lines)\nremote final"
    assert payload["out_path"] == (
        "/remote/ws/scheduler-artifact-job/results/chemistry.out"
    )
    assert fake.calls == [
        ("tail_file_since", OUT_RELATIVE, 0),
        ("tail_file_since", OUT_RELATIVE, 0),
    ]


@pytest.mark.parametrize(
    ("files", "text_marker", "json_value"),
    [
        ({OUT_RELATIVE: ""}, "(empty)", "(empty)"),
        ({}, "(no .out file", None),
    ],
    ids=["empty", "missing"],
)
def test_running_scheduler_output_distinguishes_empty_from_missing(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
    files: dict[str, str],
    text_marker: str,
    json_value: str | None,
) -> None:
    root, cfg = scheduler_state
    _write_scheduler_spec(root)
    fake = _FakeArtifactDispatcher(files=files)
    _install_dispatcher(monkeypatch, fake)

    rendered = logs.show_output(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )
    payload = json.loads(
        logs.show_output_json(
            "localhost",
            "scheduler-artifact-job",
            cfg=cfg,
        )
    )

    assert text_marker in rendered
    assert payload["out"] == json_value


def test_running_scheduler_progress_reads_full_remote_file_before_tail_filter(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    _write_scheduler_spec(root)
    remote_progress = "\n".join(_scf_row(i, -70.0 - i) for i in range(1, 5))
    fake = _FakeArtifactDispatcher(
        files={PROGRESS_RELATIVE: remote_progress + "\n"}
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = logs.show_progress(
        "localhost",
        "scheduler-artifact-job",
        tail=2,
        cfg=cfg,
    )

    assert "... (2 earlier iterations)" in rendered
    assert "-73.0000000000" in rendered
    assert "-74.0000000000" in rendered
    assert "-72.0000000000" not in rendered
    assert fake.calls == [
        ("tail_file", SYSTEM_RELATIVE, None),
        ("tail_file", PROGRESS_RELATIVE, None),
    ]


def test_scheduler_progress_uses_declared_non_sibling_structured_path(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    custom_path = "telemetry/live.scf.jsonl"
    spec = JobSpec.read(spec_path)
    spec.expected_outputs.append(custom_path)
    spec.write(spec_path)
    fake = _FakeArtifactDispatcher(
        files={custom_path: _scf_row(9, -79.0) + "\n"}
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = logs.show_progress(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )

    assert "-79.0000000000" in rendered
    assert fake.calls == [
        ("tail_file", SYSTEM_RELATIVE, None),
        ("tail_file", custom_path, None),
    ]


def test_scheduler_progress_uses_authoritative_remote_manifest_role(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(root)
    stale_path = "telemetry/stale.jsonl"
    custom_path = "telemetry/live.jsonl"
    spec = JobSpec.read(spec_path)
    spec.expected_outputs.append(stale_path)
    spec.write(spec_path)
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        f'path = "{stale_path}"\n',
        encoding="utf-8",
    )
    remote_manifest = (
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        'path = "/remote/ws/scheduler-artifact-job/telemetry/live.jsonl"\n'
    )
    fake = _FakeArtifactDispatcher(
        files={
            SYSTEM_RELATIVE: remote_manifest,
            custom_path: _scf_row(10, -80.0) + "\n",
        }
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = logs.show_progress(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )

    assert "-80.0000000000" in rendered
    assert fake.calls == [
        ("tail_file", SYSTEM_RELATIVE, None),
        ("tail_file", custom_path, None),
    ]


def test_terminal_scheduler_progress_remaps_fetched_custom_role_without_metadata(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(
        root,
        state=JobState.COMPLETED,
    )
    custom_path = "telemetry/live.jsonl"
    (workspace / "telemetry").mkdir()
    (workspace / custom_path).write_text(
        _scf_row(12, -82.0) + "\n",
        encoding="utf-8",
    )
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        "path = \"/cluster/node-scratch/vq-A1B2C3/"
        f'{custom_path}\"\n',
        encoding="utf-8",
    )

    def fail_dispatch(_cfg: config.HostConfig) -> _FakeArtifactDispatcher:
        pytest.fail("terminal scheduler progress must use fetched artifacts")

    monkeypatch.setattr(logs, "scheduler_dispatcher_for", fail_dispatch)

    rendered = logs.show_progress(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )

    resolved = logs.artifact_paths_for_spec(JobSpec.read(spec_path))
    assert resolved.progress_relative_path == Path(custom_path)
    assert "-82.0000000000" in rendered


@pytest.mark.parametrize("mode", ["local", "live-scheduler", "missing-id"])
def test_absolute_structured_suffix_is_not_remapped_outside_terminal_scheduler(
    scheduler_state: tuple[Path, config.Config],
    mode: str,
) -> None:
    root, _cfg = scheduler_state
    state = JobState.RUNNING if mode == "live-scheduler" else JobState.COMPLETED
    spec_path, workspace = _write_scheduler_spec(root, state=state)
    spec = JobSpec.read(spec_path)
    spec.expected_outputs.append("telemetry/live.jsonl")
    if mode == "local":
        spec.scheduler_target = None
        spec.scheduler_job_id = None
    elif mode == "missing-id":
        spec.scheduler_job_id = None
    spec.write(spec_path)
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        'path = "/outside/telemetry/live.jsonl"\n',
        encoding="utf-8",
    )

    resolved = logs.artifact_paths_for_spec(JobSpec.read(spec_path))

    assert resolved.progress_relative_path == Path(PROGRESS_RELATIVE)


def test_terminal_scheduler_absolute_suffix_prefers_deepest_declaration(
    scheduler_state: tuple[Path, config.Config],
) -> None:
    root, _cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(
        root,
        state=JobState.COMPLETED,
    )
    spec = JobSpec.read(spec_path)
    spec.expected_outputs.extend(["live.jsonl", "telemetry/live.jsonl"])
    spec.write(spec_path)
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        'path = "/different/root/telemetry/live.jsonl"\n',
        encoding="utf-8",
    )

    resolved = logs.artifact_paths_for_spec(JobSpec.read(spec_path))

    assert resolved.progress_relative_path == Path("telemetry/live.jsonl")


@pytest.mark.parametrize(
    "remote_path",
    [
        "/outside/not-copied/live.jsonl",
        "/outside/vq-åååååå/live.jsonl",
    ],
)
def test_terminal_scheduler_does_not_match_arbitrary_absolute_basename(
    scheduler_state: tuple[Path, config.Config],
    remote_path: str,
) -> None:
    root, _cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(
        root,
        state=JobState.COMPLETED,
    )
    (workspace / "live.jsonl").write_text(
        _scf_row(99, -99.0) + "\n",
        encoding="utf-8",
    )
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        f'path = "{remote_path}"\n',
        encoding="utf-8",
    )

    resolved = logs.artifact_paths_for_spec(JobSpec.read(spec_path))

    assert resolved.progress_relative_path == Path(PROGRESS_RELATIVE)


@pytest.mark.parametrize(
    "remote_path",
    [
        "/remote/ws/scheduler-artifact-job/../outside/live.jsonl",
        "/unmatched/root/live.jsonl",
    ],
)
def test_terminal_scheduler_rejects_unsafe_or_unmatched_absolute_role(
    scheduler_state: tuple[Path, config.Config],
    remote_path: str,
) -> None:
    root, _cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(
        root,
        state=JobState.COMPLETED,
    )
    (workspace / SYSTEM_RELATIVE).write_text(
        '[run]\nbasename = "chemistry"\n\n'
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        f'path = "{remote_path}"\n',
        encoding="utf-8",
    )

    resolved = logs.artifact_paths_for_spec(JobSpec.read(spec_path))

    assert resolved.progress_relative_path == Path(PROGRESS_RELATIVE)


def test_scheduler_status_text_and_json_use_remote_short_progress_keys(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    _spec_path, workspace = _write_scheduler_spec(root)
    (workspace / SYSTEM_RELATIVE).write_text(
        '[progress]\nphase = "local-stale"\niter = 99\nenergy = 1.0\n',
        encoding="utf-8",
    )
    fake = _FakeArtifactDispatcher(
        files={
            SYSTEM_RELATIVE: (
                '[progress]\nphase = "scf"\niter = 7\nenergy = -75.25\n'
                "grad = 0.0004\ndiis = 6\n"
            )
        }
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = status_module.show_status(
        "localhost",
        "scheduler-artifact-job",
        tail=1,
        cfg=cfg,
    )
    payload = json.loads(
        status_module.show_status_json(
            "localhost",
            "scheduler-artifact-job",
            tail=1,
            cfg=cfg,
        )
    )

    assert "progress:     scf  iter 7" in rendered
    assert "E=-75.2500000000 Ha" in rendered
    assert "|grad|=4.00e-04" in rendered
    assert "local-stale" not in rendered
    progress = payload["calculation_progress"]
    assert progress["phase"] == "scf"
    assert progress["iteration"] == 7
    assert progress["energy_eh"] == pytest.approx(-75.25)
    assert progress["gradient_norm"] == pytest.approx(0.0004)
    assert progress["diis_subspace"] == 6
    system_calls = [
        call
        for call in fake.calls
        if call[:2] == ("tail_file", SYSTEM_RELATIVE)
    ]
    assert system_calls == [
        ("tail_file", SYSTEM_RELATIVE, None),
        ("tail_file", SYSTEM_RELATIVE, None),
    ]


def test_scheduler_progress_outage_does_not_hide_status(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    _write_scheduler_spec(root)
    fake = _FakeArtifactDispatcher(
        tail_error=SchedulerError("scheduler login unavailable")
    )
    _install_dispatcher(monkeypatch, fake)

    rendered = status_module.show_status(
        "localhost",
        "scheduler-artifact-job",
        tail=1,
        cfg=cfg,
    )
    payload = json.loads(
        status_module.show_status_json(
            "localhost",
            "scheduler-artifact-job",
            tail=1,
            cfg=cfg,
        )
    )

    assert "state:        running" in rendered
    assert "progress:" not in rendered
    assert payload["state"] == "running"
    assert payload["calculation_progress"] is None


def test_direct_artifact_calls_without_config_remain_local_only(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, _cfg = scheduler_state
    _spec_path, workspace = _write_scheduler_spec(root)
    (workspace / OUT_RELATIVE).write_text("local output\n", encoding="utf-8")
    (workspace / PROGRESS_RELATIVE).write_text(
        _scf_row(11, -81.0) + "\n",
        encoding="utf-8",
    )
    (workspace / SYSTEM_RELATIVE).write_text(
        '[progress]\nphase = "local"\niteration = 11\n',
        encoding="utf-8",
    )

    def fail_scheduler_source(
        _spec: JobSpec,
        _cfg: config.Config | None,
    ) -> None:
        pytest.fail("direct artifact calls without cfg must remain local")

    monkeypatch.setattr(
        logs,
        "_scheduler_workspace_source",
        fail_scheduler_source,
    )

    output = logs.show_output("localhost", "scheduler-artifact-job")
    progress = logs.show_progress("localhost", "scheduler-artifact-job")
    status = status_module.show_status(
        "localhost",
        "scheduler-artifact-job",
        tail=1,
    )

    assert "local output" in output
    assert "-81.0000000000" in progress
    assert "progress:     local  iter 11" in status


def test_terminal_scheduler_artifacts_use_local_snapshot_without_dispatch(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(
        root,
        state=JobState.COMPLETED,
    )
    (workspace / OUT_RELATIVE).write_text("local final\n", encoding="utf-8")
    (workspace / PROGRESS_RELATIVE).write_text(
        _scf_row(8, -80.0) + "\n",
        encoding="utf-8",
    )

    def fail_dispatch(_cfg: config.HostConfig) -> _FakeArtifactDispatcher:
        pytest.fail("terminal scheduler artifacts must use the local snapshot")

    monkeypatch.setattr(logs, "scheduler_dispatcher_for", fail_dispatch)
    monkeypatch.setattr(status_module, "scheduler_dispatcher_for", fail_dispatch)

    output = logs.show_output(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )
    output_payload = json.loads(
        logs.show_output_json(
            "localhost",
            "scheduler-artifact-job",
            cfg=cfg,
        )
    )
    progress = logs.show_progress(
        "localhost",
        "scheduler-artifact-job",
        cfg=cfg,
    )

    assert "local final" in output
    assert output_payload["out"] == "local final"
    assert "-80.0000000000" in progress
    assert JobSpec.read(spec_path).last_status_at is not None


def test_follow_scheduler_output_keeps_final_bytes_after_remote_truncation(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    initial = "old content\n"
    replacement = "fresh one\nfresh two\nfresh three\nfresh four\n"
    fake = _FakeArtifactDispatcher(
        since_responses={
            OUT_RELATIVE: [
                SchedulerFileChunk(len(initial), initial.encode()),
                SchedulerFileChunk(
                    len(replacement),
                    replacement.encode(),
                    reset=True,
                ),
                SchedulerFileChunk(len(replacement), b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_output(
        "localhost",
        "scheduler-artifact-job",
        initial_tail=2,
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    first = next(chunks)
    _mark_completed(spec_path)
    rest = list(chunks)

    assert first + "".join(rest) == f"old content\n{replacement}"
    since_calls = [call for call in fake.calls if call[0] == "tail_file_since"]
    assert since_calls == [
        ("tail_file_since", OUT_RELATIVE, 0),
        ("tail_file_since", OUT_RELATIVE, len(initial)),
        ("tail_file_since", OUT_RELATIVE, len(replacement)),
    ]


def test_follow_scheduler_output_falls_back_to_terminal_local_fetch(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, workspace = _write_scheduler_spec(root)
    fake = _FakeArtifactDispatcher()
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_output(
        "localhost",
        "scheduler-artifact-job",
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    waiting = next(chunks)
    (workspace / OUT_RELATIVE).write_text("fetched final\n", encoding="utf-8")
    _mark_completed(spec_path)
    rest = "".join(chunks)

    assert "(waiting for chemistry.out" in waiting
    assert rest == "fetched final\n"
    assert JobSpec.read(spec_path).last_status_at is not None


def test_follow_scheduler_output_decodes_utf8_split_across_byte_chunks(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    first = b"start \xce"
    second = b"\xb1\n"
    fake = _FakeArtifactDispatcher(
        since_responses={
            OUT_RELATIVE: [
                SchedulerFileChunk(len(first), first),
                SchedulerFileChunk(len(first) + len(second), second),
                SchedulerFileChunk(len(first) + len(second), b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_output(
        "localhost",
        "scheduler-artifact-job",
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    initial = next(chunks)
    _mark_completed(spec_path)
    rendered = initial + "".join(chunks)

    assert rendered == "start α\n"


def test_follow_scheduler_output_applies_initial_tail_when_file_appears(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    body = b"one\ntwo\nthree\nfour\n"
    fake = _FakeArtifactDispatcher(
        since_responses={
            OUT_RELATIVE: [
                None,
                SchedulerFileChunk(len(body), body),
                SchedulerFileChunk(len(body), b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_output(
        "localhost",
        "scheduler-artifact-job",
        initial_tail=2,
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    waiting = next(chunks)
    _mark_completed(spec_path)
    rest = "".join(chunks)

    assert "(waiting for chemistry.out" in waiting
    assert rest == "... (2 earlier lines)\nthree\nfour\n"


def test_follow_scheduler_progress_preserves_split_row_after_terminal(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    first_row = _scf_row(1, -71.0) + "\n"
    final_row = _scf_row(2, -72.0) + "\n"
    split_at = len(final_row) // 2
    initial = first_row + final_row[:split_at]
    remote_size = len(first_row) + len(final_row)
    fake = _FakeArtifactDispatcher(
        since_responses={
            PROGRESS_RELATIVE: [
                SchedulerFileChunk(len(initial), initial.encode()),
                SchedulerFileChunk(
                    remote_size,
                    final_row[split_at:].encode(),
                ),
                SchedulerFileChunk(remote_size, b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_progress(
        "localhost",
        "scheduler-artifact-job",
        initial_tail=20,
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    first = next(chunks)
    _mark_completed(spec_path)
    rendered = first + "".join(chunks)

    assert rendered.count("-71.0000000000") == 1
    assert rendered.count("-72.0000000000") == 1
    since_calls = [call for call in fake.calls if call[0] == "tail_file_since"]
    assert since_calls == [
        ("tail_file_since", PROGRESS_RELATIVE, 0),
        ("tail_file_since", PROGRESS_RELATIVE, len(initial)),
        ("tail_file_since", PROGRESS_RELATIVE, remote_size),
    ]


def test_follow_scheduler_progress_emits_complete_replacement_after_reset(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    initial = _scf_row(1, -81.0) + "\n"
    replacement = "\n".join(
        _scf_row(iteration, -80.0 - iteration)
        for iteration in range(2, 6)
    ) + "\n"
    fake = _FakeArtifactDispatcher(
        since_responses={
            PROGRESS_RELATIVE: [
                SchedulerFileChunk(len(initial), initial.encode()),
                SchedulerFileChunk(
                    len(replacement),
                    replacement.encode(),
                    reset=True,
                ),
                SchedulerFileChunk(len(replacement), b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_progress(
        "localhost",
        "scheduler-artifact-job",
        initial_tail=2,
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    initial_rendered = next(chunks)
    _mark_completed(spec_path)
    rendered = initial_rendered + "".join(chunks)

    for energy in (-81.0, -82.0, -83.0, -84.0, -85.0):
        assert rendered.count(f"{energy:.10f}") == 1


def test_follow_scheduler_progress_preserves_split_utf8_record(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    row = (
        json.dumps(
            {
                "event": "scf_iter",
                "iter": 12,
                "energy": -82.0,
                "label": "α",
            },
            ensure_ascii=False,
        ).encode()
        + b"\n"
    )
    split_at = row.index("α".encode()) + 1
    fake = _FakeArtifactDispatcher(
        since_responses={
            PROGRESS_RELATIVE: [
                SchedulerFileChunk(split_at, row[:split_at]),
                SchedulerFileChunk(len(row), row[split_at:]),
                SchedulerFileChunk(len(row), b""),
            ]
        }
    )
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_progress(
        "localhost",
        "scheduler-artifact-job",
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    initial = next(chunks)
    _mark_completed(spec_path)
    rendered = initial + "".join(chunks)

    assert rendered.count("-82.0000000000") == 1


def test_follow_scheduler_progress_switches_to_late_remote_manifest_path(
    scheduler_state: tuple[Path, config.Config],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, cfg = scheduler_state
    spec_path, _workspace = _write_scheduler_spec(root)
    custom_path = "telemetry/late.jsonl"
    fake = _FakeArtifactDispatcher()
    _install_dispatcher(monkeypatch, fake)

    chunks = logs.follow_progress(
        "localhost",
        "scheduler-artifact-job",
        initial_tail=2,
        poll_interval=0.0,
        idle_ticks_after_terminal=1,
        sleep=lambda _seconds: None,
        cfg=cfg,
    )
    initial = next(chunks)
    fake.files[SYSTEM_RELATIVE] = (
        "[plan]\n"
        "[[plan.files]]\n"
        'role = "structured"\n'
        f'path = "{custom_path}"\n'
    )
    fake.files[custom_path] = "\n".join(
        _scf_row(iteration, -80.0 - iteration)
        for iteration in range(1, 5)
    ) + "\n"
    _mark_completed(spec_path)
    rendered = initial + "".join(chunks)

    assert "(no .scf.jsonl" in initial
    assert "... (2 earlier iterations)" in rendered
    assert "-83.0000000000" in rendered
    assert "-84.0000000000" in rendered
    assert "-82.0000000000" not in rendered


@dataclass
class _RecordingRunner:
    calls: list[list[str]] = field(default_factory=list)

    def run(
        self,
        argv: list[str] | tuple[str, ...],
        *,
        stdin_data: str | None = None,
        check: bool = False,
    ) -> RemoteResult:
        del stdin_data, check
        self.calls.append(list(argv))
        return RemoteResult(0, "", "")


@pytest.mark.parametrize(
    "filename",
    ["", "\x00", ".", "/etc/passwd", "../secret", "results/../../secret"],
)
@pytest.mark.parametrize(
    "operation",
    [
        lambda dispatcher, handle, filename: dispatcher.tail_file(
            handle,
            filename=filename,
        ),
        lambda dispatcher, handle, filename: dispatcher.tail_file_since(
            handle,
            filename=filename,
            byte_offset=0,
        ),
    ],
    ids=["tail-file", "tail-file-since"],
)
def test_scheduler_file_readers_reject_workspace_escape_paths(
    filename: str,
    operation: Callable[[SchedulerDispatcher, SchedulerHandle, str], object],
) -> None:
    runner = _RecordingRunner()
    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        runner,
        scratch_root="/home/USER",
    )
    handle = SchedulerHandle("555.cluster", "/remote/ws/job")

    with pytest.raises(SchedulerError):
        operation(dispatcher, handle, filename)

    assert runner.calls == []


def test_scheduler_file_reader_accepts_nested_relative_path() -> None:
    runner = _RecordingRunner()
    dispatcher = SchedulerDispatcher(
        TorqueDialect(),
        runner,
        scratch_root="/home/USER",
    )
    handle = SchedulerHandle("555.cluster", "/remote/ws/job/")

    dispatcher.tail_file(handle, filename="results/chemistry.out", lines=7)

    assert runner.calls == [
        ["tail", "-n", "7", "/remote/ws/job/results/chemistry.out"]
    ]
