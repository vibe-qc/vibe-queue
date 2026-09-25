"""Fleet console calculation previews (#65, V1): one authenticated JSON route."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_previews_65 import RKS
from tests.test_web_fleet import _write_spec, fleet_client, web_state  # noqa: F401
from vq import config
from vq.spec import JobState
from vq.web import fleet as fleet_mod


def _job(jobid: str, program: str | None, stdout: str) -> None:
    spec = _write_spec(jobid, state=JobState.RUNNING, program=program)
    workspace = Path(spec.cwd)
    (workspace / spec.stdout_path).parent.mkdir(parents=True, exist_ok=True)
    (workspace / spec.stdout_path).write_text(stdout)


def test_a_vibeqc_job_preview_parses_its_stdout_tail(fleet_client: TestClient) -> None:  # noqa: F811
    _job("preview00001", "vibeqc-release", "banner\n" + RKS)

    response = fleet_client.get("/api/v1/fleet/jobs/preview00001/preview?host=localhost")

    assert response.status_code == 200, response.text
    body = response.json()
    assert (body["jobid"], body["program"], body["state"]) == (
        "preview00001", "vibeqc-release", "running")
    assert body["preview"]["formula"] == "H2O"
    assert body["preview"]["headline"]["total_energy"] == -76.3298765432


def test_an_unknown_program_has_no_preview(fleet_client: TestClient) -> None:  # noqa: F811
    _job("preview00002", None, "hello\n")

    body = fleet_client.get("/api/v1/fleet/jobs/preview00002/preview?host=localhost").json()

    assert body["preview"] is None


def test_missing_host_and_missing_job_are_errors(fleet_client: TestClient) -> None:  # noqa: F811
    assert fleet_client.get("/api/v1/fleet/jobs/nosuchjob001/preview").status_code == 404
    missing = fleet_client.get("/api/v1/fleet/jobs/nosuchjob001/preview?host=localhost")
    assert missing.status_code == 502


def test_preview_reads_only_the_status_tail(web_state, monkeypatch) -> None:  # noqa: F811
    calls: list[tuple] = []

    def status(host, jobid, *, tail, multi_user):
        calls.append((host, jobid, tail))
        return '{"program": "vibeqc", "state": "running", "stdout": "  IUPAC name: water\\n"}'

    monkeypatch.setattr("vq.status.show_status_json", status)

    payload, error = fleet_mod.fetch_job_preview(config.load_config(), "localhost", "abc")

    assert error is None and payload["preview"]["name"] == "water"
    assert calls == [("localhost", "abc", fleet_mod.PREVIEW_TAIL_LINES)]


@pytest.mark.parametrize("program", ["vibeqc"])
def test_the_status_tail_is_bounded_before_parsing(web_state, monkeypatch, program) -> None:  # noqa: F811
    huge = "x" * (3 * 1024 * 1024) + "\n  IUPAC name: water\n"
    monkeypatch.setattr(
        "vq.status.show_status_json",
        lambda *a, **k: json.dumps({"program": program, "state": "running", "stdout": huge}),
    )

    payload, _error = fleet_mod.fetch_job_preview(config.load_config(), "localhost", "abc")

    assert payload["preview"]["name"] == "water"


def test_the_drill_down_renders_the_calculation_and_its_convergence(fleet_client: TestClient) -> None:  # noqa: F811,E501
    _job("preview00003", "vibeqc-release", RKS)

    page = fleet_client.get("/fleet/jobs/preview00003?host=localhost")

    assert page.status_code == 200, page.text
    assert "<h3>calculation</h3>" in page.text
    assert "water &middot; H2O (3 atoms)" in page.text
    assert "RKS / pbe / def2-svp" in page.text
    assert "-76.3298765432 Ha" in page.text
    assert "<polyline" in page.text and "log10 ||[F,DS]||" in page.text


def test_other_programs_render_no_calculation_section(fleet_client: TestClient) -> None:  # noqa: F811
    _job("preview00004", None, RKS)

    page = fleet_client.get("/fleet/jobs/preview00004?host=localhost")

    assert page.status_code == 200
    assert "<h3>calculation</h3>" not in page.text


def test_chart_geometry_is_bounded_and_monotone_in_x() -> None:
    from vq.web import view_context

    preview = {"scf": [{"iter": i, "residual": 10.0 ** -i, "energy": -1.0} for i in range(1, 6)]}

    chart = view_context.convergence_chart(preview, width=100, height=50)

    assert chart is not None
    xs = [float(p.split(",")[0]) for p in chart["points"].split()]
    ys = [float(p.split(",")[1]) for p in chart["points"].split()]
    assert xs == sorted(xs) and all(0 <= x <= 100 for x in xs)
    assert ys == sorted(ys) and all(0 <= y <= 50 for y in ys)
    assert (chart["log_top"], chart["log_bottom"]) == (-1, -5)
    assert view_context.convergence_chart({"scf": [{"iter": 1, "residual": 0.1}]}) is None
    assert view_context.convergence_chart(None) is None


def test_an_unparseable_vibeqc_tail_renders_no_empty_section(fleet_client: TestClient) -> None:  # noqa: F811,E501
    _job("preview00005", "vibeqc-dev", "still starting\n")

    page = fleet_client.get("/fleet/jobs/preview00005?host=localhost")

    assert page.status_code == 200
    assert "<h3>calculation</h3>" not in page.text
