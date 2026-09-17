"""Exercise external report consumers with real Git and lifecycle locks."""

from __future__ import annotations

import json
import subprocess
import sys

import click
import pytest

from tests.test_external_reports import _stores
from tests.test_fleet_release import _commit, _git
from vq import admin, fleet_release, fleet_rollout
from vq.cli import _pin_deploy_identity

pytestmark = pytest.mark.no_autopatch_lifecycle_lock


def test_from_report_fetches_private_store_and_keeps_product_identity(tmp_path, monkeypatch):
    product, reports, _cfg, logical, digest = _stores(tmp_path, monkeypatch)
    for repo in (product, reports):
        _git(repo, "remote", "add", "origin", str(repo))
    (product / logical).unlink()
    _commit(product, "remove product report")
    before = {repo: _git(repo, "rev-parse", "HEAD") for repo in (product, reports)}
    real_discover = fleet_release.discover_latest_report
    seen = []

    def discover(repo, **kwargs):
        assert repo == reports
        assert ("checkout", str(reports)) in admin._active_toolset_lifecycle_resources()
        seen.append(repo)
        return real_discover(repo, **kwargs)

    monkeypatch.setattr(fleet_release, "discover_latest_report", discover)
    sha, tag, version, selected_digest = _pin_deploy_identity(
        "vibeqc-queue", expected_sha=None, expected_tag=None,
    )
    assert seen == [reports]
    assert sha == _git(product, "rev-parse", "v1.2.3^{commit}")
    assert tag is None
    assert version == "1.2.3"
    assert selected_digest == digest
    assert fleet_release.runtime_repo() == product
    assert {repo: _git(repo, "rev-parse", "HEAD") for repo in before} == before


def test_failure_epoch_uses_report_store_ref_and_exact_blob(tmp_path, monkeypatch):
    product, reports, _cfg, logical, digest = _stores(tmp_path, monkeypatch)
    (product / logical).unlink()
    _commit(product, "remove product report")
    _git(product, "update-ref", "refs/remotes/origin/main", "HEAD")
    report_head = _git(reports, "rev-parse", "origin/main")
    assert report_head != _git(product, "rev-parse", "origin/main")
    with admin.toolset_lifecycle_lock(
        [], action="test-report-epoch", extra_resources=(("checkout", str(reports)),),
    ):
        epoch = fleet_rollout._observe_local_failure_report_epoch(reports)
    assert epoch.origin_main_commit == report_head
    assert epoch.report_source_path == logical
    assert epoch.report_digest_sha256 == digest


@pytest.mark.parametrize("mapping", ["missing", "report-store"])
def test_private_store_cannot_replace_product_pin_provenance(tmp_path, monkeypatch, mapping):
    _product, reports, cfg, _logical, _digest = _stores(tmp_path, monkeypatch)
    cfg.pin_source_repos = (
        {} if mapping == "missing"
        else {slug: str(reports) for slug in cfg.pin_source_repos}
    )
    with pytest.raises(fleet_release.FleetReleaseError, match="no accepted"):
        fleet_release.discover_latest_report(fetch=False)


def test_external_legacy_report_requires_explicit_monorepo_mapping(tmp_path, monkeypatch):
    product, reports, cfg, logical, _digest = _stores(tmp_path, monkeypatch)
    path = reports / logical
    raw = json.loads(path.read_text())
    raw.update(schema="vq.fleet.release_report/2", project_id=19)
    for name, pin in raw["pins"].items():
        pin.pop("repo")
        pin.pop("project_id")
        pin["ci_evidence"]["gating_job"] = fleet_release.LEGACY_PIN_GATING_JOBS[name]
    path.write_text(json.dumps(raw))
    _commit(reports, "legacy report in external storage")
    _git(reports, "update-ref", "refs/remotes/origin/main", "HEAD")
    cfg.pin_source_repos = {}
    with pytest.raises(fleet_release.FleetReleaseError, match="explicit retained monorepo"):
        fleet_release.discover_latest_report(fetch=False)
    cfg.pin_source_repos = {fleet_release.MONOREPO_PIN_SOURCE[0]: str(product)}
    selected = fleet_release.discover_latest_report(fetch=False)
    assert selected.raw["schema"] == "vq.fleet.release_report/2"
    assert selected.pins["vq"].sha == _git(product, "rev-parse", "v1.2.3^{commit}")


@pytest.mark.parametrize("failure", ["fetch", "newest-rejected"])
def test_external_from_report_never_authorizes_stale_selection(tmp_path, monkeypatch, failure):
    product, reports, _cfg, logical, _digest = _stores(tmp_path, monkeypatch)
    for repo in (product, reports):
        _git(repo, "remote", "add", "origin", str(repo))
    if failure == "fetch":
        _git(reports, "remote", "set-url", "origin", str(tmp_path / "absent-origin"))
        expected = "could not refresh report source"
    else:
        raw = json.loads((reports / logical).read_text())
        raw["all_pins_accepted"] = False
        (reports / "vibe-queue/releases/v2.0.0.json").write_text(json.dumps(raw))
        _commit(reports, "newer rejected report")
        expected = "fallback cannot authorize"
    with pytest.raises(click.UsageError, match=expected):
        _pin_deploy_identity("vibeqc-queue", expected_sha=None, expected_tag=None)


@pytest.mark.parametrize("binding", ["exact", "missing-report", "different-report"])
def test_reentry_requires_the_exact_external_store_lock(tmp_path, monkeypatch, binding):
    product, reports, _cfg, _logical, _digest = _stores(tmp_path, monkeypatch)
    expected = [("checkout", str(product)), ("checkout", str(reports))]
    with admin.toolset_lifecycle_lock([], action="test-parent", extra_resources=tuple(expected)):
        handoff, fds = admin._active_toolset_lifecycle_handoff()
        if binding == "missing-report":
            expected.pop()
        elif binding == "different-report":
            expected[-1] = ("checkout", str(tmp_path / "other-reports"))
        child = subprocess.run(
            [sys.executable, "-c", """
import json, sys
from vq import admin
try:
    with admin._adopt_rollout_toolset_lifecycle_handoff(
        sys.argv[1], expected_resources=tuple(map(tuple, json.loads(sys.argv[2])))
    ):
        print("admitted")
except admin.AdminError as exc:
    print(str(exc))
    sys.exit(2)
""", handoff, json.dumps(expected)],
            pass_fds=fds, capture_output=True, text=True, timeout=30,
        )
        if binding == "exact":
            assert child.returncode == 0, child.stderr
            assert child.stdout.strip() == "admitted"
        else:
            assert child.returncode == 2, child.stderr
            assert "does not match the configured controller resources" in child.stdout
        # Closing the child's inherited copy must not unlock the parent.
        contender = subprocess.run(
            [sys.executable, "-c", """
import sys
from vq import admin
try:
    with admin.toolset_lifecycle_lock(
        [], action="test-contender", extra_resources=(("checkout", sys.argv[1]),)
    ):
        sys.exit(0)
except admin.AdminError as exc:
    print(str(exc))
    sys.exit(2)
""", str(reports)],
            capture_output=True, text=True, timeout=30,
        )
        assert contender.returncode == 2, contender.stderr
        assert "another toolset lifecycle operation owns checkout" in contender.stdout
