"""The fleet console lists the broker's jobs, metadata only (#64 B7).

Maintainer decision (2026-09-29): id, state, target, resources, name, times
and blockers or operator action; never the command, program or payload. The
view is read-only and never creates broker state.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.test_broker_store_64 import _archive, _intent
from vq import auth, broker_service, broker_store, config, paths
from vq.web import create_app

BROKER_TOML = (
    '[hosts.alpha]\nssh = "alpha"\n\n'
    '[broker]\nenabled = true\ntargets = ["alpha"]\n'
)


@pytest.fixture
def web_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    monkeypatch.delenv(auth.ENV_WEB_TOKEN_FILE, raising=False)
    monkeypatch.setenv("VQ_WEB_FLEET", "1")
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _broker_config(web_state: Path) -> config.Config:
    (web_state / "cfg" / "config.toml").write_text(BROKER_TOML, encoding="utf-8")
    return config.load_config()


def test_broker_jobs_show_metadata_but_never_the_command(web_state: Path) -> None:
    cfg = _broker_config(web_state)
    intent = _intent(command=("secret-solver", "--deck", "private-deck.inp"),
                     job_name="scan-7")
    record = broker_service.accept(cfg, _archive(web_state / "p.tar"), intent,
                                   clock=lambda: 100.0)

    page = TestClient(create_app()).get("/fleet/jobs")

    assert page.status_code == 200, page.text
    assert "Broker jobs" in page.text
    assert record.broker_job_id in page.text and "scan-7" in page.text
    assert "held" in page.text
    assert "secret-solver" not in page.text and "private-deck.inp" not in page.text


def test_the_view_holds_only_the_decided_fields(web_state: Path) -> None:
    cfg = _broker_config(web_state)
    broker_service.accept(cfg, _archive(web_state / "p.tar"), _intent(), clock=lambda: 100.0)
    view = broker_service.console_view(cfg)
    assert view is not None and view["error"] is None
    (row,) = view["jobs"]
    assert set(row) == set(broker_service.CONSOLE_FIELDS)
    assert row["created_at"] == "1970-01-01T00:01:40+00:00"


def test_a_host_that_is_not_a_broker_shows_no_section(web_state: Path) -> None:
    page = TestClient(create_app()).get("/fleet/jobs")
    assert page.status_code == 200 and "Broker jobs" not in page.text


def test_the_console_never_creates_broker_state(web_state: Path) -> None:
    _broker_config(web_state)
    page = TestClient(create_app()).get("/fleet/jobs")
    assert "No broker jobs." in page.text
    assert not broker_store.default_broker_root().exists()
