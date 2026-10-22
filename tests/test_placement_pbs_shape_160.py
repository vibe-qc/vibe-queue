"""#160: a Torque/PBS lane's shape from `qstat -Qf` and `pbsnodes -a`.

The lane's queue gives the walltime and node-count maxima and whether it
runs jobs. Its node shape is the smallest usable node it routes to: nodes
carrying every property its `resources_default.neednodes` names (Torque), or
nodes whose `queue` is the lane's (PBS Pro). Recorded output in each
dialect's format goes through a stand-in for SSH; no real cluster was asked.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, transport
from vq import placement_policy as pp
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_placement_policy_160 import _fleet  # noqa: E402

TORQUE_QUEUE = (
    "Queue: batch\n"
    "    queue_type = Execution\n"
    "    resources_max.walltime = 48:00:00\n"
    "    resources_max.nodect = 4\n"
    # Torque folds a long value mid-word onto a tab-indented line.
    "    resources_default.neednodes = 1:wide:pp\n"
    "\tn=64\n"
    "    enabled = True\n"
    "    started = True\n"
)
TORQUE_NODES = (
    "n1\n     state = free\n     np = 64\n     properties = wide,ib\n"
    "     status = opsys=linux,physmem=263852472kb,availmem=1kb\n     gpus = 2\n"
    "n2\n     state = job-exclusive\n     np = 128\n     properties = ib,wide\n"
    "     status = physmem=527704944kb\n     gpus = 4\n"
    "n3\n     state = free\n     np = 16\n     properties = narrow\n"
    "     status = physmem=1048576kb\n"
    "n4\n     state = down,offline\n     np = 8\n     properties = wide\n"
    "     status = physmem=1kb\n"
)
PBSPRO_QUEUE = ("Queue: workq\n    resources_max.walltime = 1:00:00:00\n"
                "    enabled = True\n    started = False\n")
PBSPRO_NODES = (
    "cn1\n     state = free\n     queue = workq\n     resources_available.ncpus = 48\n"
    "     resources_available.mem = 192gb\n     resources_available.ngpus = 4\n"
    "cn2\n     state = free\n     queue = other\n     resources_available.ncpus = 4\n"
    "     resources_available.mem = 1gb\n"
)


class TestParse:
    def test_a_torque_queue_and_the_nodes_its_neednodes_names(self) -> None:
        shape, note = pp.lane_shape_from_pbs("batch", TORQUE_QUEUE, TORQUE_NODES)
        assert note is None
        assert shape == {
            "max_walltime_seconds": 172800, "walltime_unlimited": False, "max_nodes": 4,
            "maintenance": False,
            # n1 and n2 carry "wide"; n3 does not, and n4 is down. The smallest wins.
            "node": {"cpus": 64, "mem_mb": 263852472 // 1024, "gpus": 2}}

    def test_a_pbs_pro_queue_and_the_nodes_in_it(self) -> None:
        shape, note = pp.lane_shape_from_pbs("workq", PBSPRO_QUEUE, PBSPRO_NODES)
        assert note is None
        assert shape == {"max_walltime_seconds": 86400, "walltime_unlimited": False,
                         "maintenance": True,  # started = False
                         "node": {"cpus": 48, "mem_mb": 196608, "gpus": 4}}

    def test_without_routed_nodes_the_shape_is_not_guessed(self) -> None:
        bare = "Queue: plain\n    enabled = True\n    started = True\n"
        shape, note = pp.lane_shape_from_pbs("plain", bare, TORQUE_NODES)
        assert shape == {"maintenance": False}  # no walltime maximum: none claimed
        assert note == "queue plain names no node property and no usable node is in it"
        _shape, note = pp.lane_shape_from_pbs(
            "batch", TORQUE_QUEUE.replace(":wide:", ":gpu:"), TORQUE_NODES)
        assert note == "no usable node carries gpu"
        _shape, note = pp.lane_shape_from_pbs("batch", TORQUE_QUEUE, None)
        assert note == "no pbsnodes reading"

    def test_a_node_needs_every_property_the_queue_names(self) -> None:
        queue = TORQUE_QUEUE.replace(":wide:", ":wide:ib:")
        nodes = TORQUE_NODES.replace("properties = wide,ib", "properties = wide")
        shape, _note = pp.lane_shape_from_pbs("batch", queue, nodes)
        assert shape["node"]["cpus"] == 128  # only n2 carries both wide and ib

    def test_only_queue_headers_name_a_queue(self) -> None:
        with pytest.raises(pp.PlacementError, match="does not describe queue q"):
            pp.lane_shape_from_pbs("q", "Note: q\n    enabled = True\n", None)

    @pytest.mark.parametrize(("walltime", "seconds"), [
        ("48:00:00", 172800), ("168:00:00", 604800), ("30:00", 1800), ("90", 90),
        ("1:12:00:00", 129600), ("01:00:00.500", 3600)])
    def test_pbs_durations(self, walltime: str, seconds: int) -> None:
        queue = f"Queue: q\n    resources_max.walltime = {walltime}\n"
        assert pp.lane_shape_from_pbs("q", queue, None)[0]["max_walltime_seconds"] == seconds

    @pytest.mark.parametrize(("size", "mb"), [
        ("263852472kb", 257668), ("192gb", 196608), ("2tb", 2097152), ("1048576mb", 1048576),
        ("1073741824", 1024), ("1073741824b", 1024)])
    def test_pbs_memory_sizes(self, size: str, mb: int) -> None:
        nodes = f"x\n  state = free\n  queue = q\n  np = 4\n  resources_available.mem = {size}\n"
        assert pp.lane_shape_from_pbs("q", "Queue: q\n", nodes)[0]["node"]["mem_mb"] == mb

    @pytest.mark.parametrize(("queue", "nodes", "message"), [
        ("Queue: other\n", None, "does not describe queue q"),
        ("Queue: q\n    resources_max.walltime = soon\n", None, "not a duration"),
        ("Queue: q\n    resources_max.nodect = many\n", None, "nodect is not a count"),
        ("Queue: q\n    resources_max.nodect = 0\n", None, "nodect is not a count"),
        ("Queue: q\n", "x\n  state = free\n  queue = q\n  np = ?\n", "CPUs are not a count"),
        ("Queue: q\n", "x\n  state = free\n  queue = q\n  np = 4\n", "no memory"),
        ("Queue: q\n", "x\n  state = free\n  queue = q\n  np = 4\n"
                       "  resources_available.mem = lots\n", "not a size"),
        ("Queue: q\n", "x\n  state = free\n  queue = q\n  np = 4\n"
                       "  resources_available.mem = 1gb\n  gpus = some\n",
         "GPUs are not a count"),
    ])
    def test_unparsable_output_is_refused(self, queue: str, nodes: str | None,
                                          message: str) -> None:
        with pytest.raises(pp.PlacementError, match=message):
            pp.lane_shape_from_pbs("q", queue, nodes)


PBS_HOSTS = ('[hosts.localhost]\nssh = "localhost"\n'
             '[hosts.torque]\nssh = "torque-login"\nscheduler = "pbs"\n'
             'scheduler_dialect = "torque"\nscratch_root = "/scratch"\n'
             'scheduler_driver = "localhost"\n')


@pytest.fixture
def ssh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(PBS_HOSTS)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    state: dict[str, Any] = {"calls": [], "fail": set(),
                             "out": {"qstat": TORQUE_QUEUE, "pbsnodes": TORQUE_NODES}}

    def run_remote_shell(host_cfg: Any, *argv: str, **kwargs: Any) -> Any:
        state["calls"].append(argv)
        if argv[0] in state["fail"]:
            raise subprocess.CalledProcessError(1, argv)
        return subprocess.CompletedProcess(argv, 0, stdout=state["out"][argv[0]], stderr="")

    monkeypatch.setattr(transport, "run_remote_shell", run_remote_shell)
    fleet = _fleet()
    for target in fleet["targets"]:
        if target["name"] == "fast-busy":
            target["queue"] = "batch"
    base = tmp_path / "base.json"
    base.write_text(json.dumps(fleet))
    state["base"] = base
    return state


def _lane(ssh: dict[str, Any]) -> tuple[dict[str, Any], str]:
    result = CliRunner().invoke(main, ["placement", "fleet", str(ssh["base"]),
                                       "--scheduler-shape", "fast-busy=torque"])
    assert result.exit_code == 0, result.output
    lane = next(t for t in json.loads(result.stdout)["targets"] if t["name"] == "fast-busy")
    return lane, result.stderr


def test_a_torque_lane_gets_its_queue_and_node_shape(ssh: dict[str, Any]) -> None:
    lane, stderr = _lane(ssh)
    assert lane["node"] == {"cpus": 64, "mem_mb": 257668, "gpus": 2}
    assert (lane["max_walltime_seconds"], lane["max_nodes"], lane["maintenance"],
            lane["reachable"]) == (172800, 4, False, True)
    assert ssh["calls"] == [("qstat", "-Qf", "batch"), ("pbsnodes", "-a")]
    assert "fast-busy: 64 CPUs, 257668 MB, 2 GPUs per node from torque" in stderr


def test_without_a_node_reading_the_queue_limits_still_apply(ssh: dict[str, Any]) -> None:
    ssh["fail"].add("pbsnodes")
    lane, stderr = _lane(ssh)
    assert lane["node"] == {"cpus": 128, "mem_mb": 512000}  # the base record
    assert lane["max_walltime_seconds"] == 172800 and lane["reachable"] is True
    assert "fast-busy: node shape kept (no pbsnodes reading) from torque" in stderr


def test_a_queue_that_cannot_be_read_leaves_the_lane_unreachable(ssh: dict[str, Any]) -> None:
    ssh["fail"].add("qstat")
    lane, stderr = _lane(ssh)
    assert lane["reachable"] is False and "unreachable" in stderr
    assert ssh["calls"] == [("qstat", "-Qf", "batch")]
