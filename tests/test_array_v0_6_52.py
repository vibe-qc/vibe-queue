"""v0.6.52: ``vq submit --array N`` array jobs.

SLURM-array analogue. One submit spawns N near-identical specs
sharing a group id; each gets a sequential index 0..N-1 and a
total of N. The daemon injects VQ_ARRAY_INDEX / VQ_ARRAY_TOTAL /
VQ_ARRAY_GROUP_ID env variables at dispatch so the job's script
can branch on its index without parsing its own spec file.

No gang scheduling — each element is an ordinary independent
spec; per-user budgets / quotas apply to each individually. The
shared group_id is operator metadata.

Coverage shape:

* TestSpecRoundtrip          array fields default None; roundtrip;
                             pre-v0.6.52 specs read clean.
* TestGroupIdHelper          new_array_group_id returns 8-hex,
                             distinct across calls.
* TestSubmitLocalArray       array=N creates N specs with
                             sequential indexes + shared group_id;
                             per-element workspace; rejects array<1;
                             N=1 still uses the per-element path
                             (returns 1-element list).
* TestDaemonEnv              spec.array_index → Popen receives
                             env with VQ_ARRAY_*; non-array spec →
                             env inherited from os.environ (None).
* TestStatusDisplay          show_status renders array line when set,
                             omits otherwise.
* TestCLI                    --array N prints N jobids one per line;
                             --array 1 falls back to single submit
                             (no array fields set); --array + N=0
                             rejected at the Click range layer.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq.cli import main
from vq.daemon import Daemon
from vq.spec import JobSpec
from vq.status import show_status
from vq.submit import (
    new_array_group_id,
    submit_local_array,
)

# ===========================================================================
# Spec roundtrip
# ===========================================================================


class TestSpecRoundtrip:
    def test_defaults_to_none(self) -> None:
        s = JobSpec(id="a", command=["true"], cwd=".", cpus=1)
        assert s.array_index is None
        assert s.array_total is None
        assert s.array_group_id is None

    def test_roundtrip_with_array_fields(self, tmp_path: Path) -> None:
        s = JobSpec(
            id="a", command=["true"], cwd=".", cpus=1,
            array_index=7, array_total=30, array_group_id="abcd1234",
        )
        p = tmp_path / "a.json"
        s.write(p)
        loaded = JobSpec.read(p)
        assert loaded.array_index == 7
        assert loaded.array_total == 30
        assert loaded.array_group_id == "abcd1234"

    def test_pre_v0_6_52_spec_reads_clean(self, tmp_path: Path) -> None:
        p = tmp_path / "old.json"
        p.write_text(json.dumps({
            "id": "old1",
            "command": ["true"],
            "cwd": str(tmp_path),
            "cpus": 1,
        }))
        loaded = JobSpec.read(p)
        assert loaded.array_index is None
        assert loaded.array_total is None
        assert loaded.array_group_id is None

    def test_array_index_must_be_non_negative(self) -> None:
        with pytest.raises(Exception):  # noqa: B017 — pydantic validation
            JobSpec(
                id="a", command=["true"], cwd=".", cpus=1,
                array_index=-1, array_total=10,
            )

    def test_array_total_must_be_positive(self) -> None:
        with pytest.raises(Exception):  # noqa: B017 — pydantic validation
            JobSpec(
                id="a", command=["true"], cwd=".", cpus=1,
                array_index=0, array_total=0,
            )


# ===========================================================================
# group_id helper
# ===========================================================================


class TestGroupIdHelper:
    def test_returns_8_hex_chars(self) -> None:
        gid = new_array_group_id()
        assert re.fullmatch(r"[0-9a-f]{8}", gid)

    def test_distinct_per_call(self) -> None:
        ids = {new_array_group_id() for _ in range(20)}
        assert len(ids) == 20  # vanishingly likely to collide


# ===========================================================================
# submit_local_array
# ===========================================================================


@pytest.fixture
def state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    cfgdir = tmp_path / "cfg"
    cfgdir.mkdir()
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfgdir))
    (cfgdir / "config.toml").write_text('default_host = "localhost"\n')
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


class TestSubmitLocalArray:
    def test_array_n_creates_n_specs(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        jobids = submit_local_array(
            array=5,
            host="localhost",
            input_file=str(src),
        )
        assert len(jobids) == 5
        assert len(set(jobids)) == 5  # unique
        group_ids: set[str] = set()
        for idx, jid in enumerate(jobids):
            spec = JobSpec.read(paths.queue_dir() / f"{jid}.json")
            assert spec.array_index == idx
            assert spec.array_total == 5
            assert spec.array_group_id is not None
            group_ids.add(spec.array_group_id)
        # All N elements share the same group_id.
        assert len(group_ids) == 1

    def test_each_element_has_its_own_workspace(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        jobids = submit_local_array(
            array=3, host="localhost", input_file=str(src),
        )
        workspaces: set[str] = set()
        for jid in jobids:
            spec = JobSpec.read(paths.queue_dir() / f"{jid}.json")
            workspaces.add(spec.cwd)
            assert (Path(spec.cwd) / "x.py").exists()
        assert len(workspaces) == 3

    def test_array_zero_rejected(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        with pytest.raises(ValueError, match="array must be >= 1"):
            submit_local_array(
                array=0, host="localhost", input_file=str(src),
            )

    def test_array_one_returns_single_element_list(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        jobids = submit_local_array(
            array=1, host="localhost", input_file=str(src),
        )
        assert len(jobids) == 1
        spec = JobSpec.read(paths.queue_dir() / f"{jobids[0]}.json")
        # array fields are SET even for array=1 — that's the
        # contract: if you called submit_local_array, you wanted
        # array semantics. Non-array submits go through submit_local
        # directly (CLI dispatches based on --array > 1).
        assert spec.array_index == 0
        assert spec.array_total == 1

    def test_scheduler_target_recorded_on_each_element(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        jobids = submit_local_array(
            array=3,
            host="localhost",
            input_file=str(src),
            # Cluster-side interpreter: a scheduler-target single-file submit
            # rejects the driver's own (see TestSchedulerTargetInterpreter in
            # tests/test_submit.py).
            python="/home/USER/venv/bin/python",
            scheduler_target="host_f",
        )

        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.scheduler_target for spec in specs} == {"host_f"}
        assert {spec.array_group_id for spec in specs if spec.array_group_id} == {
            specs[0].array_group_id
        }

    def test_rerun_until_recorded_on_each_element(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        jobids = submit_local_array(
            array=2,
            host="localhost",
            input_file=str(src),
            rerun_until_file_exists="$VQ_WORKDIR/DONE",
            rerun_max=4,
        )

        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.rerun_until_file_exists for spec in specs} == {
            "$VQ_WORKDIR/DONE"
        }
        assert {spec.rerun_max for spec in specs} == {4}


# ===========================================================================
# Daemon env injection
# ===========================================================================


class TestDaemonEnv:
    """Verify the daemon's _start_job Popen sees VQ_ARRAY_* env when
    spec.array_index is set, and inherits cleanly when not."""

    def _make_daemon(self, tmp_path: Path) -> Daemon:
        d = Daemon(
            max_cpus=4,
            poll_interval=0.05,
            queue_dir=tmp_path / "queue",
            jobs_dir=tmp_path / "jobs",
        )
        d.queue_dir.mkdir(parents=True, exist_ok=True)
        d.jobs_dir.mkdir(parents=True, exist_ok=True)
        return d

    def _write_spec(
        self,
        daemon: Daemon,
        jobid: str,
        *,
        array_index: int | None = None,
        array_total: int | None = None,
        array_group_id: str | None = None,
    ) -> JobSpec:
        ws = daemon.jobs_dir / jobid
        ws.mkdir(parents=True, exist_ok=True)
        spec = JobSpec(
            id=jobid,
            command=["true"],
            cwd=str(ws),
            cpus=1,
            array_index=array_index,
            array_total=array_total,
            array_group_id=array_group_id,
        )
        spec.write(daemon._spec_path(jobid))
        return spec

    def test_array_spec_dispatch_injects_vq_array_env(
        self, tmp_path: Path,
    ) -> None:
        d = self._make_daemon(tmp_path)
        spec = self._write_spec(
            d, "job123",
            array_index=7, array_total=30,
            array_group_id="abcd1234",
        )

        captured: dict[str, object] = {}

        # Stub Popen so we can capture the env without actually
        # running the process. Returning a quickly-exited fake is
        # enough — we don't need to follow the lifecycle.
        class FakePopen:
            def __init__(self, *args, **kwargs):  # noqa: ANN
                captured["args"] = args
                captured["kwargs"] = kwargs
                self.pid = 12345

            def poll(self):
                return 0

        with patch("vq.daemon.subprocess.Popen", FakePopen), patch(
            "vq.daemon.os.getpgid", return_value=12345,
        ), patch(
            "vq.daemon._read_pid_start_time", return_value=None,
        ):
            d._start_job(spec)

        env = captured["kwargs"]["env"]
        assert env is not None
        assert env["VQ_ARRAY_INDEX"] == "7"
        assert env["VQ_ARRAY_TOTAL"] == "30"
        assert env["VQ_ARRAY_GROUP_ID"] == "abcd1234"
        # Daemon's own env still present (PATH should be inherited).
        assert "PATH" in env

    def test_non_array_spec_dispatch_carries_workdir_env_no_array_keys(
        self, tmp_path: Path,
    ) -> None:
        """v0.6.54 changed this: non-array specs now also receive a
        materialised env dict (so VQ_WORKDIR can be injected). The
        v0.6.52 invariant — that VQ_ARRAY_* keys are NOT present —
        still holds; only that the env is no longer ``None``."""
        d = self._make_daemon(tmp_path)
        spec = self._write_spec(d, "job123")  # no array fields

        captured: dict[str, object] = {}

        class FakePopen:
            def __init__(self, *args, **kwargs):  # noqa: ANN
                captured["kwargs"] = kwargs
                self.pid = 12345

            def poll(self):
                return 0

        with patch("vq.daemon.subprocess.Popen", FakePopen), patch(
            "vq.daemon.os.getpgid", return_value=12345,
        ), patch(
            "vq.daemon._read_pid_start_time", return_value=None,
        ):
            d._start_job(spec)

        env = captured["kwargs"]["env"]
        assert env is not None
        # Daemon's env still inherited.
        assert "PATH" in env
        # VQ_WORKDIR is now always set (v0.6.54).
        assert "VQ_WORKDIR" in env
        # The VQ_ARRAY_* keys remain absent for non-array specs.
        assert "VQ_ARRAY_INDEX" not in env
        assert "VQ_ARRAY_TOTAL" not in env
        assert "VQ_ARRAY_GROUP_ID" not in env


# ===========================================================================
# Status display
# ===========================================================================


class TestStatusDisplay:
    def test_array_line_renders_when_set(self, state: Path) -> None:
        ws = state / "ws"
        ws.mkdir()
        JobSpec(
            id="a", command=["true"], cwd=str(ws), cpus=1,
            array_index=3, array_total=10, array_group_id="grp12345",
        ).write(paths.queue_dir() / "a.json")
        out = show_status("localhost", "a")
        assert "array:        3/10 (group=grp12345)" in out

    def test_array_line_omitted_when_unset(self, state: Path) -> None:
        ws = state / "ws"
        ws.mkdir()
        JobSpec(
            id="a", command=["true"], cwd=str(ws), cpus=1,
        ).write(paths.queue_dir() / "a.json")
        out = show_status("localhost", "a")
        assert "array:" not in out


# ===========================================================================
# CLI end-to-end
# ===========================================================================


class TestCLI:
    def test_array_n_prints_n_jobids(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", "--array", "4", str(src)],
        )
        assert result.exit_code == 0, result.output
        jobids = [line.strip() for line in result.output.splitlines() if line.strip()]
        assert len(jobids) == 4
        assert len({len(j) for j in jobids}) == 1  # all 12-hex

    def test_array_rerun_until_reaches_each_spec(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main,
            [
                "submit",
                "--array",
                "2",
                "--rerun-until",
                "$VQ_WORKDIR/DONE",
                "--rerun-max",
                "4",
                str(src),
            ],
        )
        assert result.exit_code == 0, result.output
        jobids = [line.strip() for line in result.output.splitlines() if line.strip()]
        assert len(jobids) == 2
        specs = [JobSpec.read(paths.queue_dir() / f"{jid}.json") for jid in jobids]
        assert {spec.rerun_until_file_exists for spec in specs} == {
            "$VQ_WORKDIR/DONE"
        }
        assert {spec.rerun_max for spec in specs} == {4}

    def test_array_one_falls_back_to_single_submit(
        self, state: Path,
    ) -> None:
        """--array 1 should NOT set array fields — that's reserved
        for actual array submissions. (submit_local_array always
        sets them; the CLI dispatches via submit_local when
        array == 1.)"""
        src = state / "x.py"
        src.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", "--array", "1", str(src)],
        )
        assert result.exit_code == 0, result.output
        jobid = result.output.strip()
        spec = JobSpec.read(paths.queue_dir() / f"{jobid}.json")
        assert spec.array_index is None
        assert spec.array_total is None
        assert spec.array_group_id is None

    def test_array_zero_rejected_by_click(self, state: Path) -> None:
        src = state / "x.py"
        src.write_text("print('hi')\n")
        result = CliRunner().invoke(
            main, ["submit", "--array", "0", str(src)],
        )
        assert result.exit_code != 0
        # Click's IntRange(min=1) error.
        assert "0" in result.output or "invalid" in result.output.lower()
