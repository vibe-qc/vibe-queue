"""External site identities remain authoritative for release verification."""

from __future__ import annotations

import json

import pytest

from tests.test_fleet_release import _report
from vq import config, fleet_release, source_projects


def test_report_uses_external_identities_and_rejects_a_different_project(tmp_path, monkeypatch):
    settings = {
        "pins": {
            name: {"repo": "deployment/" + product, "project_id": project}
            for name, product, project in (
                ("release", "vibe-qc", 101),
                ("dev", "vibe-qc", 101),
                ("vq", "vibe-queue", 102),
                ("vibe_view", "vibe-view", 103),
            )
        },
        "legacy": {"repo": "deployment/archive", "project_id": 104},
    }
    path = tmp_path / "source-projects.json"
    path.write_text(json.dumps(settings))
    path.chmod(0o600)
    monkeypatch.setenv("VQ_SOURCE_PROJECTS_FILE", str(path))
    report = _report("v1.2.3", "a" * 40)

    def parse():
        return fleet_release.parse_report(
            report,
            source_ref="main",
            source_path="releases/v1.2.3.json",
            raw_bytes=json.dumps(report).encode(),
        )

    assert parse().pins["vq"].repo == "deployment/vibe-queue"
    report["pins"]["vq"]["project_id"] = 999
    with pytest.raises(fleet_release.FleetReleaseError, match="not"):
        parse()
    path.unlink()
    with pytest.raises(ValueError, match="external source-projects"):
        parse()


def test_source_lookup_preserves_external_namespaces_and_rejects_ambiguity():
    configured = {"deployment/vibe-qc": "/srv/qc"}
    assert config.Config(pin_source_repos=configured).vibeqc_source_repo == "/srv/qc"
    with pytest.raises(ValueError, match="multiple"):
        config.source_repo_key({**configured, "another/vibe-qc": "/srv/other"}, "vibe-qc")


def test_source_project_config_refuses_a_product_checkout(tmp_path, monkeypatch):
    (tmp_path / ".git").mkdir()
    path = tmp_path / "source-projects.json"
    path.write_text("{}")
    path.chmod(0o600)
    monkeypatch.setenv("VQ_SOURCE_PROJECTS_FILE", str(path))
    with pytest.raises(ValueError, match="outside Git"):
        source_projects.legacy_source()
