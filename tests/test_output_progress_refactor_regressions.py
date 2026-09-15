"""Regression targets for the artifact/progress read-surface refactor.

This file describes the intended behavior at the known seams.  It is expected
to be RED until the corresponding production fixes land; the existing GREEN
characterization file continues to pin behavior that already works.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from vq import config, logs, paths
from vq import status as status_module
from vq.spec import JobSpec, JobState


@pytest.fixture
def queue_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    state_root = tmp_path / "state"
    (state_root / "queue").mkdir(parents=True)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.toml").write_text(
        'default_host = "localhost"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(state_root))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(config_dir))
    monkeypatch.setattr(status_module, "is_daemon_serving", lambda **_kw: True)
    return tmp_path


def _write_spec(
    root: Path,
    *,
    jobid: str = "artifact-job",
    state: JobState = JobState.RUNNING,
    output_stem: str | None = None,
    expected_outputs: list[str] | None = None,
    archived: bool = False,
    create_workspace: bool = True,
) -> tuple[Path, Path]:
    workspace = root / "workspaces" / jobid
    if create_workspace:
        workspace.mkdir(parents=True)
        (workspace / "stdout.log").write_text("", encoding="utf-8")
        (workspace / "stderr.log").write_text("", encoding="utf-8")
    fields: dict[str, object] = {
        "id": jobid,
        "command": ["python", "job.py"],
        "cwd": str(workspace),
        "cpus": 1,
        "state": state,
        "submitted_at": "2026-08-08T12:00:00+00:00",
        "output_stem": output_stem,
        "expected_outputs": expected_outputs or [],
    }
    if state != JobState.PENDING:
        fields["started_at"] = "2026-08-08T12:01:00+00:00"
    if state == JobState.COMPLETED:
        fields.update(
            finished_at="2026-08-08T12:02:00+00:00",
            exit_code=0,
        )
    if archived:
        fields.update(
            archived_at="2026-08-08T12:03:00+00:00",
            archive_path=str(root / "archive" / f"{jobid}.tar.bz2"),
        )
    spec_path = root / "state" / "queue" / f"{jobid}.json"
    JobSpec(**fields).write(spec_path)
    return spec_path, workspace


def _write_manifest(workspace: Path, filename: str, basename: str) -> Path:
    path = workspace / filename
    path.write_text(
        f'[run]\nbasename = "{basename}"\n',
        encoding="utf-8",
    )
    return path


def _iteration_row(
    iteration: int,
    energy: float,
    *,
    delta_e: float | None = None,
) -> str:
    return json.dumps(
        {
            "event": "scf_iter",
            "iter": iteration,
            "energy": energy,
            "dE": delta_e,
            "grad_norm": 10.0 ** (-iteration),
            "diis_subspace": iteration,
        }
    )


def _mark_completed(spec_path: Path) -> None:
    spec = JobSpec.read(spec_path)
    spec.state = JobState.COMPLETED
    spec.finished_at = "2026-08-08T12:02:00+00:00"
    spec.exit_code = 0
    spec.write(spec_path)


class TestDeclaredArtifactResolution:
    def test_output_stem_resolves_without_manifest(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_spec(
            queue_state,
            output_stem="chemistry",
        )

        payload = json.loads(logs.show_output_json("localhost", "artifact-job"))

        assert payload["out_path"] == str(workspace / "chemistry.out")
        assert payload["out"] is None

    def test_nested_expected_output_resolves_without_manifest(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_spec(
            queue_state,
            expected_outputs=["results/chemistry.out"],
        )
        nested = workspace / "results" / "chemistry.out"
        nested.parent.mkdir()
        nested.write_text("nested output\n", encoding="utf-8")

        payload = json.loads(logs.show_output_json("localhost", "artifact-job"))

        assert payload["out_path"] == str(nested)
        assert payload["out"] == "nested output"

    def test_declared_stem_wins_when_multiple_manifests_exist(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_spec(
            queue_state,
            output_stem="declared",
        )
        _write_manifest(workspace, "00-other.system", "other")
        _write_manifest(workspace, "99-declared.system", "declared")
        (workspace / "other.out").write_text("wrong family\n", encoding="utf-8")
        (workspace / "declared.out").write_text(
            "declared family\n",
            encoding="utf-8",
        )

        rendered = logs.show_output("localhost", "artifact-job")

        assert "declared.out" in rendered.splitlines()[0]
        assert "declared family" in rendered
        assert "wrong family" not in rendered

    def test_unsafe_manifest_basename_cannot_escape_workspace(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_spec(queue_state)
        _write_manifest(workspace, "00-unsafe.system", "../../outside/secret")
        spec = JobSpec.read(queue_state / "state" / "queue" / "artifact-job.json")

        resolved = logs._out_path(spec)

        assert resolved.resolve().is_relative_to(workspace.resolve())
        assert resolved == workspace / "output.out"


@pytest.mark.parametrize(
    "reader",
    [
        logs.show_output,
        logs.show_progress,
    ],
    ids=["output", "progress"],
)
def test_archived_missing_snapshot_has_restore_hint_and_read_stamp(
    queue_state: Path,
    reader: Callable[..., str],
) -> None:
    spec_path, _workspace = _write_spec(
        queue_state,
        state=JobState.COMPLETED,
        archived=True,
        create_workspace=False,
    )

    rendered = reader("localhost", "artifact-job")
    refreshed = JobSpec.read(spec_path)

    assert (
        "archived" in rendered.lower(),
        "cleanup --restore" in rendered,
        refreshed.last_status_at is not None,
    ) == (True, True, True)


def test_malformed_numeric_scf_rows_are_ignored(queue_state: Path) -> None:
    _spec_path, workspace = _write_spec(queue_state)
    (workspace / "output.scf.jsonl").write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event": "scf_iter",
                        "iter": "not-an-int",
                        "energy": -1.0,
                    }
                ),
                json.dumps(
                    {
                        "event": "scf_iter",
                        "iter": 2,
                        "energy": "not-a-float",
                    }
                ),
                json.dumps(
                    {
                        "event": "scf_iter",
                        "iter": 3,
                        "energy": -3.0,
                        "grad_norm": {"bad": "shape"},
                    }
                ),
                _iteration_row(4, -4.0, delta_e=-0.01),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rendered = logs.show_progress("localhost", "artifact-job")

    assert "-4.0000000000" in rendered
    assert rendered.count("\n") == 3


class TestFollowProgressFraming:
    def test_split_json_record_is_preserved_across_polls(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, workspace = _write_spec(queue_state)
        progress_path = workspace / "output.scf.jsonl"
        progress_path.write_text("", encoding="utf-8")
        record = _iteration_row(9, -9.0, delta_e=-0.09) + "\n"
        split_at = len(record) // 2
        ticks = 0

        def drive(_seconds: float) -> None:
            nonlocal ticks
            ticks += 1
            if ticks == 1:
                progress_path.write_text(record[:split_at], encoding="utf-8")
            elif ticks == 2:
                with progress_path.open("a", encoding="utf-8") as stream:
                    stream.write(record[split_at:])
                _mark_completed(spec_path)

        chunks = list(
            logs.follow_progress(
                "localhost",
                "artifact-job",
                poll_interval=0,
                idle_ticks_after_terminal=1,
                sleep=drive,
            )
        )

        assert "-9.0000000000" in "".join(chunks)

    def test_truncation_keeps_terminal_final_record(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, workspace = _write_spec(queue_state)
        progress_path = workspace / "output.scf.jsonl"
        progress_path.write_text(
            "\n".join(
                _iteration_row(i, -float(i), delta_e=-0.1)
                for i in range(1, 5)
            )
            + "\n",
            encoding="utf-8",
        )
        final_record = _iteration_row(12, -12.0, delta_e=-0.001) + "\n"
        ticks = 0

        def drive(_seconds: float) -> None:
            nonlocal ticks
            ticks += 1
            if ticks == 1:
                progress_path.write_text(final_record, encoding="utf-8")
                _mark_completed(spec_path)

        chunks = list(
            logs.follow_progress(
                "localhost",
                "artifact-job",
                initial_tail=1,
                poll_interval=0,
                idle_ticks_after_terminal=1,
                sleep=drive,
            )
        )

        assert "-12.0000000000" in "".join(chunks)

    def test_append_on_terminal_transition_emits_final_record(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, workspace = _write_spec(queue_state)
        progress_path = workspace / "output.scf.jsonl"
        progress_path.write_text(
            _iteration_row(1, -1.0) + "\n",
            encoding="utf-8",
        )
        ticks = 0

        def drive(_seconds: float) -> None:
            nonlocal ticks
            ticks += 1
            if ticks == 1:
                with progress_path.open("a", encoding="utf-8") as stream:
                    stream.write(_iteration_row(2, -2.0, delta_e=-1.0) + "\n")
                _mark_completed(spec_path)

        chunks = list(
            logs.follow_progress(
                "localhost",
                "artifact-job",
                initial_tail=1,
                poll_interval=0,
                idle_ticks_after_terminal=1,
                sleep=drive,
            )
        )

        assert "-2.0000000000" in "".join(chunks)


def test_status_renders_live_short_progress_keys(queue_state: Path) -> None:
    _spec_path, workspace = _write_spec(queue_state)
    (workspace / "output.system").write_text(
        "[run]\n"
        'basename = "output"\n'
        "\n[progress]\n"
        'phase = "scf"\n'
        "iter = 6\n"
        "energy = -73.4963596554\n"
        "grad = 3.3e-8\n"
        "diis = 6\n",
        encoding="utf-8",
    )

    rendered = status_module.show_status("localhost", "artifact-job")
    progress_line = next(
        line for line in rendered.splitlines() if line.startswith("progress:")
    )

    assert (
        "iter 6" in progress_line,
        "E=-73.4963596554 Ha" in progress_line,
        "|grad|=3.30e-08" in progress_line,
    ) == (True, True, True)

    payload = json.loads(status_module.show_status_json("localhost", "artifact-job"))
    calculation_progress = payload["calculation_progress"]
    assert calculation_progress["iteration"] == 6
    assert calculation_progress["energy_eh"] == pytest.approx(-73.4963596554)
    assert calculation_progress["gradient_norm"] == pytest.approx(3.3e-8)
    assert calculation_progress["diis_subspace"] == 6
    # The pre-existing field remains reserved for checkpoint-QVF progress.
    assert payload["progress"] is None
