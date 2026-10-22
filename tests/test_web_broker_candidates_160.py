"""#160: the console compares a broker job's candidate hosts.

A broker job with a placement attempt lists the hosts its decision ranked,
best first, with the target it went to marked. A completion-time refusal
reaches the console as its own `completion-time` code, never as the
generic `placement-blocked`, and never with its detail (which can name a
program). Jobs are placed by the real placer into the store the console
reads.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from tests.test_broker_placement_b4a_64 import Target
from tests.test_broker_store_64 import _archive, _intent, _snapshot
from tests.test_web_fleet_broker_64 import _broker_config, web_state  # noqa: F401
from vq import broker_service, broker_store
from vq.broker_completion import completion_time_reranker
from vq.broker_placement import BrokerPlacer
from vq.web import create_app


def _fleet(path: Path, beta_programs: dict[str, list[str]]) -> Path:
    def target(name: str, speed: float, programs: dict[str, list[str]]) -> dict[str, Any]:
        return {"name": name, "kind": "daemon", "observed_at": "2026-01-01T00:00:00+00:00",
                "reachable": True, "programs": programs,
                "node": {"cpus": 64, "mem_mb": 256000}, "walltime_unlimited": True,
                "speed_factor": speed, "speed_source": "benchmark"}

    path.write_text(json.dumps({"schema_name": "vq.placement-fleet/1", "targets": [
        target("alpha", 1.0, {"vibe-qc": ["a" * 64]}),
        target("beta", 2.0, beta_programs)]}))
    return path


def _place(web_state: Path, beta_programs: dict[str, list[str]],  # noqa: F811
           refused: str | None = None) -> str:
    cfg = _broker_config(web_state)
    record = broker_service.accept(cfg, _archive(web_state / "p.tar"),
                                   _intent(program="vibe-qc"), clock=lambda: 100.0)
    target = Target()
    target.roots["beta"] = "/srv/vq-state/beta"
    with broker_store.BrokerStore.open(broker_store.default_broker_root()) as store:
        placer = BrokerPlacer(
            store, submit=target.submit, state_root_of=target.state_root_of,
            clock=lambda: 106.0,
            refuse_target=lambda host: "not today" if host == refused else None,
            rerank=completion_time_reranker(_fleet(web_state / "fleet.json", beta_programs)))
        placer.place(record.broker_job_id, [
            _snapshot("alpha", free_cpus=32, free_mem_mb=128000), _snapshot("beta")],
            max_age_seconds=60.0)
    return record.broker_job_id


def test_a_placed_job_lists_its_ranked_candidates(web_state: Path) -> None:  # noqa: F811
    _place(web_state, {"vibe-qc": ["a" * 64]})
    view = broker_service.console_view(_broker_config(web_state))
    assert view is not None
    (row,) = view["jobs"]
    assert (row["state"], row["target_host"], row["candidates"]) == (
        "placed", "beta", ["beta", "alpha"])
    page = TestClient(create_app()).get("/fleet/jobs")
    assert page.status_code == 200, page.text
    assert "<strong>beta</strong>, alpha" in page.text


def test_the_target_is_marked_when_the_walk_skipped_a_better_one(
    web_state: Path,  # noqa: F811
) -> None:
    _place(web_state, {"vibe-qc": ["a" * 64]}, refused="beta")
    view = broker_service.console_view(_broker_config(web_state))
    assert view is not None
    (row,) = view["jobs"]
    assert (row["target_host"], row["candidates"]) == ("alpha", ["beta", "alpha"])
    assert row["blockers"] == {"beta": "target-refused"}
    assert "beta, <strong>alpha</strong>" in TestClient(create_app()).get("/fleet/jobs").text


def test_a_completion_time_refusal_keeps_its_code_not_its_detail(
    web_state: Path,  # noqa: F811
) -> None:
    _place(web_state, {})  # beta lacks the program; alpha takes it
    view = broker_service.console_view(_broker_config(web_state))
    assert view is not None
    (row,) = view["jobs"]
    assert (row["target_host"], row["candidates"]) == ("alpha", ["alpha"])
    assert row["blockers"] == {"beta": "completion-time"}
    page = TestClient(create_app()).get("/fleet/jobs").text
    assert "beta: completion-time" in page and "placement-blocked" not in page
    assert "vibe-qc" not in page  # the refusal's detail names the program


def test_a_held_job_has_no_candidates(web_state: Path) -> None:  # noqa: F811
    cfg = _broker_config(web_state)
    broker_service.accept(cfg, _archive(web_state / "p.tar"), _intent(), clock=lambda: 100.0)
    view = broker_service.console_view(cfg)
    assert view is not None and view["jobs"][0]["candidates"] == []
    assert "<td>—</td>" in TestClient(create_app()).get("/fleet/jobs").text
