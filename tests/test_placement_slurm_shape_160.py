"""#160: a Slurm lane's node shape and walltime limit from `sinfo`.

`vq placement fleet --scheduler-shape NAME[=HOST]` asks the lane's Slurm
host for its partition's rows (`%c|%m|%G|%l|%s`) and records the smallest
node group's CPUs, memory and GPUs, the walltime limit and the job-size
limit. The output below is recorded in Slurm's documented formats and fed
through a stand-in for SSH; no real cluster was asked.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, transport
from vq import placement_policy as pp
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_placement_policy_160 import _fleet  # noqa: E402


class TestParse:
    def test_a_uniform_partition(self) -> None:
        assert pp.lane_shape_from_sinfo("64|256000|gpu:a100:4(S:0-1)|2-00:00:00|1-16\n") == {
            "node": {"cpus": 64, "mem_mb": 256000, "gpus": 4},
            "max_walltime_seconds": 172800, "walltime_unlimited": False, "max_nodes": 16}

    def test_several_node_groups_are_described_by_the_smallest(self) -> None:
        shape = pp.lane_shape_from_sinfo(
            "128+|512000|gpu:a100:2,gpu:v100:1,shard:8|12:00:00|1-4\n"
            "64|256000+|gpu:3|infinite|8\n")
        assert shape == {"node": {"cpus": 64, "mem_mb": 256000, "gpus": 3},
                         "max_walltime_seconds": 43200, "walltime_unlimited": False,
                         "max_nodes": 4}

    @pytest.mark.parametrize(("limit", "seconds"), [
        ("2-00:00:00", 172800), ("12:00:00", 43200), ("30:00", 1800), ("45", 2700),
        ("1-6", 86400 + 360), ("infinite", None), ("UNLIMITED", None)])
    def test_time_limits(self, limit: str, seconds: int | None) -> None:
        shape = pp.lane_shape_from_sinfo(f"8|1000|(null)|{limit}|1\n")
        assert shape["max_walltime_seconds"] == seconds
        assert shape["walltime_unlimited"] is (seconds is None)
        assert shape["node"]["gpus"] == 0

    def test_only_gpu_gres_counts_and_the_tightest_limit_wins(self) -> None:
        shape = pp.lane_shape_from_sinfo("8|1000|gpu:2,shard:8|1:00:00|1\n"
                                         "8|1000|gpu:a100:4|2:00:00|1\n")
        assert (shape["node"]["gpus"], shape["max_walltime_seconds"]) == (2, 3600)

    def test_an_unbounded_job_size_leaves_max_nodes_alone(self) -> None:
        assert "max_nodes" not in pp.lane_shape_from_sinfo("8|1000|(null)|1:00:00|1-infinite\n")

    @pytest.mark.parametrize(("text", "message"), [
        ("", "lists no nodes"),
        ("8|1000|(null)|1:00:00\n", "not in the expected format"),
        ("eight|1000|(null)|1:00:00|1\n", "CPUs is not a count"),
        ("8|lots|(null)|1:00:00|1\n", "memory is not a count"),
        ("8|1000|gpu:many|1:00:00|1\n", "no GPU count"),
        ("8|1000|(null)|soon|1\n", "not a duration"),
        ("8|1000|(null)|x-1:00:00|1\n", "not a duration"),
        ("8|1000|(null)|1:2:3:4|1\n", "not a duration"),
        ("8|1000|(null)|1:00:00|1-many\n", "job-size limit is not a node count"),
    ])
    def test_unparsable_output_is_refused(self, text: str, message: str) -> None:
        with pytest.raises(pp.PlacementError, match=message):
            pp.lane_shape_from_sinfo(text)


SLURM_HOSTS = ('[hosts.localhost]\nssh = "localhost"\n'
               '[hosts.cluster]\nssh = "cluster-login"\nscheduler = "slurm"\n'
               'scheduler_dialect = "slurm"\nscratch_root = "/scratch"\n'
               'scheduler_driver = "localhost"\n'
               '[hosts.daemonhost]\nssh = "daemonhost"\n')


@pytest.fixture
def ssh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(SLURM_HOSTS)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    state: dict[str, Any] = {"calls": [], "stdout": "64|256000|gpu:4|2-00:00:00|1-16\n",
                             "fail": False}

    def run_remote_shell(host_cfg: Any, *argv: str, **kwargs: Any) -> Any:
        state["calls"].append((host_cfg.ssh, argv))
        assert kwargs["owned_process_group"] is True and kwargs["check"] is True
        if state["fail"]:
            raise subprocess.CalledProcessError(1, argv, stderr="sinfo: error")
        return subprocess.CompletedProcess(argv, 0, stdout=state["stdout"], stderr="")

    monkeypatch.setattr(transport, "run_remote_shell", run_remote_shell)
    fleet = _fleet()
    for target in fleet["targets"]:
        if target["name"] == "fast-busy":
            target["queue"] = "compute"
            target["node"]["scratch_mb"] = 900000
    base = tmp_path / "base.json"
    base.write_text(json.dumps(fleet))
    state["base"] = base
    return state


def _fleet_cli(ssh: dict[str, Any], *args: str) -> Any:
    return CliRunner().invoke(main, ["placement", "fleet", str(ssh["base"]), *args])


def _lane(result: Any, name: str = "fast-busy") -> dict[str, Any]:
    lane: dict[str, Any] = next(
        t for t in json.loads(result.stdout)["targets"] if t["name"] == name)
    return lane


def test_a_lane_gets_its_partitions_shape(ssh: dict[str, Any]) -> None:
    before = time.time()
    result = _fleet_cli(ssh, "--scheduler-shape", "fast-busy=cluster")
    assert result.exit_code == 0, result.output
    lane = _lane(result)
    assert lane["node"] == {"cpus": 64, "mem_mb": 256000, "gpus": 4, "scratch_mb": 900000}
    assert (lane["max_walltime_seconds"], lane["walltime_unlimited"], lane["max_nodes"],
            lane["reachable"]) == (172800, False, 16, True)
    assert datetime.fromisoformat(lane["observed_at"]).timestamp() >= before - 1
    assert ssh["calls"] == [("cluster-login", (
        "sinfo", "--noheader", "--partition", "compute", "--format", "%c|%m|%G|%l|%s"))]
    assert "fast-busy: 64 CPUs, 256000 MB, 4 GPUs per node from cluster" in result.stderr


def test_an_unlimited_partition_replaces_a_recorded_limit(ssh: dict[str, Any]) -> None:
    ssh["stdout"] = "64|256000|(null)|infinite|1-infinite\n"
    lane = _lane(_fleet_cli(ssh, "--scheduler-shape", "fast-busy=cluster"))
    assert (lane["max_walltime_seconds"], lane["walltime_unlimited"], lane["max_nodes"]) == (
        None, True, 16)  # max_nodes stays the base record's


def test_a_host_that_gives_no_shape_leaves_the_lane_unreachable(ssh: dict[str, Any]) -> None:
    ssh["fail"] = True
    result = _fleet_cli(ssh, "--scheduler-shape", "fast-busy=cluster")
    assert result.exit_code == 0, result.output
    lane = _lane(result)
    assert lane["reachable"] is False
    assert lane["node"] == {"cpus": 128, "mem_mb": 512000, "scratch_mb": 900000}
    assert "fast-busy: unreachable (cluster gave no partition shape" in result.stderr
    ssh["fail"], ssh["stdout"] = False, ""
    assert _lane(_fleet_cli(ssh, "--scheduler-shape", "fast-busy=cluster"))[
        "reachable"] is False


@pytest.mark.parametrize(("args", "message"), [
    (["--scheduler-shape", "mem-rich=cluster"], "records no queue"),
    (["--scheduler-shape", "idle-slow=cluster"], "not a scheduler lane of the fleet"),
    (["--scheduler-shape", "fast-busy=daemonhost"],
     "daemonhost is not a configured Slurm or Torque/PBS host"),
    (["--scheduler-shape", "fast-busy"], "fast-busy is not a configured Slurm or Torque/PBS host"),
    (["--scheduler-shape", "fast-busy=cluster", "--scheduler-shape", "fast-busy=cluster"],
     "given twice"),
])
def test_misdirected_shapes_contact_no_host(ssh: dict[str, Any], args: list[str],
                                            message: str) -> None:
    result = _fleet_cli(ssh, *args)
    assert result.exit_code != 0 and message in result.output, result.output
    assert ssh["calls"] == []
