"""Characterization coverage for the vq output/progress read surfaces.

These tests pin the working local rendering, manifest discovery, JSON, status
progress, and CLI delegation contracts. Scheduler-side file access remains a
separate focused increment.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import config, logs, paths
from vq import status as status_module
from vq.cli import main
from vq.spec import JobSpec, JobState


@pytest.fixture
def queue_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """Point vq at an isolated queue and localhost-only config."""
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


def _write_job(
    root: Path,
    *,
    jobid: str = "calc-job",
    state: JobState = JobState.RUNNING,
    output_stem: str | None = "calc",
    archived: bool = False,
) -> tuple[Path, Path]:
    workspace = root / "workspaces" / jobid
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
    }
    if state != JobState.PENDING:
        fields["started_at"] = "2026-08-08T12:01:00+00:00"
    if state in {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.KILLED,
        JobState.INTERRUPTED,
        JobState.OOM_KILLED,
        JobState.STARVED,
        JobState.TIME_EXCEEDED,
        JobState.ABORTED_BY_QUEUE,
    }:
        fields.update(
            finished_at="2026-08-08T12:02:00+00:00",
            exit_code=0 if state == JobState.COMPLETED else 1,
        )
    if archived:
        fields.update(
            archived_at="2026-08-08T12:03:00+00:00",
            archive_path=str(root / "archive" / f"{jobid}.tar.bz2"),
        )
    spec_path = root / "state" / "queue" / f"{jobid}.json"
    JobSpec(**fields).write(spec_path)
    return spec_path, workspace


def _write_manifest(
    workspace: Path,
    *,
    stem: str = "calc",
    progress: bool = False,
) -> Path:
    body = ["[run]", f'basename = "{stem}"']
    if progress:
        body.extend(
            [
                "",
                "[progress]",
                'phase = "scf"',
                "iteration = 7",
                "energy_eh = -75.123456789",
                "gradient_norm = 2.5e-7",
                "diis_subspace = 6",
            ]
        )
    path = workspace / f"{stem}.system"
    path.write_text("\n".join(body) + "\n", encoding="utf-8")
    return path


def _scf_iter(
    iteration: int,
    energy: float,
    *,
    delta_e: float | None,
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


class TestOutputReadSurface:
    def test_manifest_and_output_stem_family_resolve_locally(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state, output_stem="chemistry")
        _write_manifest(workspace, stem="chemistry")
        (workspace / "chemistry.out").write_text(
            "SCF start\nfinal energy = -75.0\n",
            encoding="utf-8",
        )

        rendered = logs.show_output("localhost", "calc-job")

        assert rendered.startswith("[running] wall=")
        assert "chemistry.out" in rendered.splitlines()[0]
        assert "final energy = -75.0" in rendered

    def test_tail_keeps_only_requested_output_lines(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        _write_manifest(workspace)
        (workspace / "calc.out").write_text(
            "line 1\nline 2\nline 3\nline 4\n",
            encoding="utf-8",
        )

        rendered = logs.show_output("localhost", "calc-job", tail=2)

        assert "... (2 earlier lines)" in rendered
        assert "line 1" not in rendered
        assert rendered.endswith("line 3\nline 4")

    def test_json_shape_includes_resolved_path_and_text(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        _write_manifest(workspace)
        (workspace / "calc.out").write_text("energy = -1\n", encoding="utf-8")

        payload = json.loads(
            logs.show_output_json("localhost", "calc-job", tail=12)
        )

        assert payload == {
            "jobid": "calc-job",
            "out": "energy = -1",
            "out_path": str(workspace / "calc.out"),
            "state": "running",
            "tail": 12,
        }

    def test_missing_output_is_a_readable_snapshot(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(
            queue_state,
            state=JobState.RUNNING,
        )
        (workspace / "stdout.log").unlink()
        (workspace / "stderr.log").unlink()
        workspace.rmdir()

        text = logs.show_output("localhost", "calc-job")
        payload = json.loads(logs.show_output_json("localhost", "calc-job"))

        assert "calc.out" in text
        assert "no .out file" in text
        assert payload["out"] is None
        assert payload["out_path"].endswith("/calc.out")

    def test_terminal_output_read_stamps_last_status_at(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, workspace = _write_job(
            queue_state,
            state=JobState.COMPLETED,
        )
        _write_manifest(workspace)
        (workspace / "calc.out").write_text("done\n", encoding="utf-8")

        logs.show_output("localhost", "calc-job")

        assert JobSpec.read(spec_path).last_status_at is not None

    def test_terminal_missing_output_json_stamps_last_status_at(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, _workspace = _write_job(
            queue_state,
            state=JobState.COMPLETED,
        )

        logs.show_output_json("localhost", "calc-job")

        assert JobSpec.read(spec_path).last_status_at is not None


class TestProgressReadSurface:
    def test_manifest_basename_selects_structured_log_and_tail(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state, output_stem="chemistry")
        _write_manifest(workspace, stem="chemistry")
        rows = [
            _scf_iter(i, -75.0 - i / 100, delta_e=None if i == 1 else -0.01)
            for i in range(1, 5)
        ]
        (workspace / "chemistry.scf.jsonl").write_text(
            "\n".join(rows) + "\n",
            encoding="utf-8",
        )

        rendered = logs.show_progress("localhost", "calc-job", tail=2)

        assert "... (2 earlier iterations)" in rendered
        assert "-75.0300000000" in rendered
        assert "-75.0400000000" in rendered
        assert "-75.0100000000" not in rendered

    def test_malformed_and_non_iteration_rows_are_ignored(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        _write_manifest(workspace)
        (workspace / "calc.scf.jsonl").write_text(
            "not-json\n"
            + json.dumps({"event": "warning", "message": "skip me"})
            + "\n"
            + _scf_iter(3, -74.5, delta_e=-0.1)
            + "\n{broken\n",
            encoding="utf-8",
        )

        rendered = logs.show_progress("localhost", "calc-job")

        assert "-74.5000000000" in rendered
        assert "skip me" not in rendered
        assert "not-json" not in rendered
        assert rendered.count("-74.5000000000") == 1

    def test_missing_progress_is_a_readable_snapshot(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(
            queue_state,
            state=JobState.RUNNING,
        )
        (workspace / "stdout.log").unlink()
        (workspace / "stderr.log").unlink()
        workspace.rmdir()

        rendered = logs.show_progress("localhost", "calc-job")

        assert "no .scf.jsonl" in rendered

    def test_terminal_progress_read_stamps_last_status_at(
        self,
        queue_state: Path,
    ) -> None:
        spec_path, workspace = _write_job(
            queue_state,
            state=JobState.COMPLETED,
        )
        _write_manifest(workspace)
        (workspace / "calc.scf.jsonl").write_text(
            _scf_iter(1, -1.0, delta_e=None) + "\n",
            encoding="utf-8",
        )

        logs.show_progress("localhost", "calc-job")

        assert JobSpec.read(spec_path).last_status_at is not None


class TestTextStatusProgress:
    def test_running_status_renders_manifest_progress(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        _write_manifest(workspace, progress=True)

        rendered = status_module.show_status("localhost", "calc-job")

        assert (
            "progress:     scf  iter 7  E=-75.1234567890 Ha  "
            "|grad|=2.50e-07"
        ) in rendered

    def test_malformed_manifest_is_skipped_before_valid_progress(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        (workspace / "00-broken.system").write_text(
            "[progress\n",
            encoding="utf-8",
        )
        _write_manifest(workspace, progress=True)

        rendered = status_module.show_status("localhost", "calc-job")

        assert "progress:     scf  iter 7" in rendered


class TestOutputProgressCLI:
    def test_local_output_and_progress_forms(
        self,
        queue_state: Path,
    ) -> None:
        _spec_path, workspace = _write_job(queue_state)
        _write_manifest(workspace)
        (workspace / "calc.out").write_text("local out\n", encoding="utf-8")
        (workspace / "calc.scf.jsonl").write_text(
            _scf_iter(2, -2.0, delta_e=-0.2) + "\n",
            encoding="utf-8",
        )

        output_result = CliRunner().invoke(main, ["output", "calc-job", "-n", "1"])
        progress_result = CliRunner().invoke(
            main,
            ["progress", "localhost", "calc-job", "-n", "1"],
        )

        assert output_result.exit_code == 0, output_result.output
        assert "local out" in output_result.output
        assert progress_result.exit_code == 0, progress_result.output
        assert "-2.0000000000" in progress_result.output

    @pytest.mark.parametrize(
        ("argv", "expected_remote_args", "remote_text"),
        [
            (
                ["output", "host_d", "calc-job", "-n", "7", "--json"],
                ["output", "localhost", "calc-job", "-n", "7", "--json"],
                '{"jobid":"calc-job","state":"running"}\n',
            ),
            (
                ["progress", "host_d", "calc-job", "-n", "8"],
                ["progress", "localhost", "calc-job", "-n", "8"],
                "remote progress\n",
            ),
        ],
    )
    def test_remote_forms_forward_stable_argv(
        self,
        queue_state: Path,
        monkeypatch: pytest.MonkeyPatch,
        argv: list[str],
        expected_remote_args: list[str],
        remote_text: str,
    ) -> None:
        config_path = queue_state / "config" / "config.toml"
        config_path.write_text(
            'default_host = "localhost"\n'
            "\n[hosts.host_d]\n"
            'ssh = "host_d.invalid"\n',
            encoding="utf-8",
        )
        captured: dict[str, object] = {}

        def fake_lookup(
            cfg: config.Config,
            host: str,
            *remote_args: str,
            **metadata: object,
        ) -> str:
            captured.update(
                cfg=cfg,
                host=host,
                args=list(remote_args),
                metadata=metadata,
            )
            return remote_text

        monkeypatch.setattr("vq.cli._delegate_job_lookup", fake_lookup)

        result = CliRunner().invoke(main, argv)

        assert result.exit_code == 0, result.output
        assert captured["host"] == "host_d"
        assert captured["args"] == expected_remote_args
        metadata = captured["metadata"]
        assert isinstance(metadata, dict)
        assert metadata["verb"] == argv[0]
        assert metadata["jobid"] == "calc-job"

    def test_output_follow_and_json_are_rejected_together(
        self,
        queue_state: Path,
    ) -> None:
        result = CliRunner().invoke(
            main,
            ["output", "calc-job", "--follow", "--json"],
        )

        assert result.exit_code != 0
        assert "--follow with --json is not supported" in result.output
