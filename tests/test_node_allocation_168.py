"""#168: per-node detail for distributed jobs, from the scheduler's allocation.

The execution host string the detail poll keeps verbatim is read as nodes
and their allocated cores: PBS/Torque ``exec_host`` in its three shapes and
Slurm hostlist expressions (node names only). The jobs table shows "N nodes"
with the list on hover and the drill-down a per-node table. Anything that
does not parse, or would expand too far, falls back to the raw string; no
per-node usage is ever implied.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from vq import node_allocation
from vq.node_allocation import NodeShare, parse_exec_host
from vq.spec import JobSpec, JobState
from vq.web import calculations, view_context


@pytest.mark.parametrize(("value", "expected"), [
    # PBS/Torque core ranges per node.
    ("node07/0-7+node08/0-7", [("node07", 8), ("node08", 8)]),
    ("node07/0,2-3+node08/1", [("node07", 3), ("node08", 1)]),
    # Torque's one entry per core, also when a node recurs out of order.
    ("node07/0+node07/1+node08/0+node07/2", [("node07", 3), ("node08", 1)]),
    # PBS Pro: a slot holding N CPUs.
    ("node07/0*8+node08/0*16", [("node07", 8), ("node08", 16)]),
    ("node07.cluster.example/0-3", [("node07.cluster.example", 4)]),
    # Slurm hostlists: names only, cores unknown.
    ("node[01-03,07]", [("node01", None), ("node02", None), ("node03", None),
                        ("node07", None)]),
    ("node[001-002],gpu[1-2]-ib", [("node001", None), ("node002", None),
                                   ("gpu1-ib", None), ("gpu2-ib", None)]),
    ("rack[1-2]-n[01-02]", [("rack1-n01", None), ("rack1-n02", None),
                            ("rack2-n01", None), ("rack2-n02", None)]),
    ("node[8-11]", [("node8", None), ("node9", None), ("node10", None), ("node11", None)]),
    ("node01", [("node01", None)]),
])
def test_allocations_parse(value: str, expected: list[tuple[str, int | None]]) -> None:
    assert parse_exec_host(value) == [NodeShare(node, cores) for node, cores in expected]


@pytest.mark.parametrize("value", [
    None, "", "None assigned", "node[01-03", "node]01[", "node07/", "node07/x",
    "node07/3-1", "node07/0*0", "/0-3", "node[03-01]", "node[03-01],node05", "node[a-c]",
    "n[1-3,1]", "a,a",
    "node07/0-3+", "node[1-100000]", "x" * 70000,
])
def test_anything_else_is_left_raw(value: str | None) -> None:
    assert parse_exec_host(value) is None


def test_expansion_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(node_allocation, "MAX_NODES", 4)
    assert parse_exec_host("n[1-4]") is not None
    assert parse_exec_host("n[1-5]") is None
    assert parse_exec_host("a[1-2]-b[1-3]") is None  # 6 names from two brackets
    assert parse_exec_host("n[1-3],m[1-2]") is None
    assert parse_exec_host("a,b,c,d") is not None
    assert parse_exec_host("a,b,c,d,e") is None
    assert parse_exec_host("a/0+b/0+c/0+d/0") is not None
    assert parse_exec_host("a/0+b/0+c/0+d/0+e/0") is None


def _scheduler_row(exec_host: str | None) -> dict[str, object]:
    return {"scheduler_target": "hpc_a", "scheduler_exec_host": exec_host,
            "queue_owner": "driver"}


class TestTable:
    def test_a_multi_node_job_reads_as_a_count_with_the_list_on_hover(self) -> None:
        view = calculations.location_view(_scheduler_row("node07/0-7+node08/0-7"))
        assert (view["label"], view["title"]) == (
            "2 nodes", "scheduler allocation: node07 (8 cores), node08 (8 cores)")
        assert view["nodes"] == "node07/0-7+node08/0-7"  # sorting keeps the raw string

    def test_a_single_node_reads_as_its_name(self) -> None:
        view = calculations.location_view(_scheduler_row("node07/0-3"))
        assert (view["label"], view["allocation"]) == ("node07", [{"node": "node07", "cores": 4}])

    def test_an_unparsed_string_and_a_direct_job_are_shown_as_before(self) -> None:
        view = calculations.location_view(_scheduler_row("None assigned"))
        assert (view["label"], view["title"], view["allocation"]) == (
            "None assigned", "None assigned", None)
        direct = calculations.location_view({"scheduler_target": None, "queue_host": "ws"})
        assert (direct["label"], direct["allocation"]) == ("ws", None)

    def test_the_rendered_table(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config, paths
        from vq.overview import HostOverview
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        row = {"id": "abc", "queue_host": "hpc_a", "queue_owner": "driver", "state": "running",
               "effective_state": "running", "scheduler_target": "hpc_a",
               "scheduler_state": "running", "cpus": 16,
               "scheduler_exec_host": "node07/0-7+node08/0-7",
               "submitted_at": (datetime.now(UTC) - timedelta(hours=1)).isoformat()}
        client = TestClient(create_app())
        client.app.state.fleet_cache.set_snapshot(fleet_mod.FleetSnapshot(
            gathered_at=datetime.now(UTC).isoformat(), duration_seconds=1.0,
            hosts=[fleet_mod.HostSnapshot(host="hpc_a", overview=HostOverview(host="hpc_a"),
                                          jobs=[row])]))
        html = client.get("/fleet/jobs/_table").text
        assert 'title="scheduler allocation: node07 (8 cores), node08 (8 cores)"' in html
        assert "2 nodes" in html


class TestDrillDown:
    def _detail(self, tmp_path: Path, **fields: object) -> dict[str, object]:
        status = JobSpec(id="abc", command=["run"], cwd=str(tmp_path), cpus=16,
                         state=JobState.RUNNING, **fields).model_dump(mode="json")
        return {"status": status, "events": [], "samples": [], "owner_host": "driver"}

    def test_a_scheduler_job_lists_its_nodes(self, tmp_path: Path) -> None:
        detail = self._detail(tmp_path, scheduler_target="hpc_a",
                              scheduler_exec_host="node[01-02]")
        assert view_context.node_allocation_view(detail) == [
            {"node": "node01", "cores": None}, {"node": "node02", "cores": None}]

    @pytest.mark.parametrize("fields", [
        {},  # a direct job
        {"scheduler_target": "hpc_a"},  # not started
        {"scheduler_target": "hpc_a", "scheduler_exec_host": "None assigned"},
    ])
    def test_otherwise_there_is_no_section(self, tmp_path: Path,
                                           fields: dict[str, object]) -> None:
        assert view_context.node_allocation_view(self._detail(tmp_path, **fields)) is None
        assert view_context.node_allocation_view(None) is None

    def test_the_rendered_section(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from fastapi.testclient import TestClient

        from vq import auth, config, paths
        from vq.web import create_app
        from vq.web import fleet as fleet_mod

        monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
        monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
        monkeypatch.setenv("VQ_WEB_FLEET", "1")
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        detail = self._detail(tmp_path, scheduler_target="hpc_a",
                              scheduler_exec_host="node07/0-7+node08/0-3")
        monkeypatch.setattr(fleet_mod, "fetch_job_detail", lambda *a, **k: (detail, None))
        monkeypatch.setattr(fleet_mod, "fetch_job_preview", lambda *a, **k: (None, None))
        html = TestClient(create_app()).get("/fleet/jobs/abc?host=localhost").text
        assert "<h3>nodes</h3>" in html
        assert '<td class="who">node08</td><td class="num">4</td>' in html
        assert "The scheduler's allocation: 2 nodes. Nothing is measured per node" in html
