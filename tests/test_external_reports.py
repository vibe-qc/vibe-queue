"""Private report storage must not become controller or pin provenance."""

from __future__ import annotations

import hashlib
import json

import pytest

from tests.test_fleet_release import _commit, _git, _init_repo, _pin_repos, _report
from vq import config, fleet_release


@pytest.mark.parametrize("value", [None, "", "relative/reports"])
def test_report_storage_requires_explicit_absolute_configuration(value) -> None:
    with pytest.raises(fleet_release.FleetReleaseError, match="fleet_report_repo"):
        fleet_release.report_repo(config.Config(fleet_report_repo=value))


def test_private_report_store_cannot_hide_under_product_or_symlink_interface(tmp_path):
    product = tmp_path / "product"
    marker = product / "src/vq/__init__.py"
    marker.parent.mkdir(parents=True)
    marker.write_text("")
    outside = tmp_path / "operations"
    outside.mkdir()
    alias = product / "external"
    alias.symlink_to(outside, target_is_directory=True)
    for selected in (product, product / "private/reports", alias):
        with pytest.raises(fleet_release.FleetReleaseError, match="outside product"):
            fleet_release.report_repo(config.Config(fleet_report_repo=str(selected)))
    assert fleet_release.report_repo(config.Config(fleet_report_repo=str(outside))) == outside


def _stores(tmp_path, monkeypatch, *, legacy_evidence=False):
    for name in ("product", "operations"):
        (tmp_path / name).mkdir()
    product = _init_repo(tmp_path / "product")
    operations = _init_repo(tmp_path / "operations")
    (product / "README").write_text("distinct product source\n")
    _commit(product, "product implementation")
    sha = _git(product, "rev-parse", "HEAD")
    tag = "v1.2.3"
    _git(product, "tag", tag)
    payload = _report(tag, sha)
    if legacy_evidence:
        for pin in payload["pins"].values():
            pin["ci_evidence"]["ref"] = "main"
    logical = f"vibe-queue/releases/{tag}.json"
    raw = (json.dumps(payload, indent=2) + "\n").encode()
    for repo in (product, operations):
        path = repo / logical
        path.parent.mkdir(parents=True)
        path.write_bytes(raw)
        _commit(repo, "record report")
        _git(repo, "update-ref", "refs/remotes/origin/main", "HEAD")
    cfg = config.Config(
        fleet_report_repo=str(operations),
        fleet_report_history_repo=str(product),
        pin_source_repos={slug: str(path) for slug, path in _pin_repos(product).items()},
    )
    monkeypatch.setattr(config, "load_config", lambda: cfg)
    monkeypatch.setattr(fleet_release, "runtime_repo", lambda: product)
    return product, operations, cfg, logical, hashlib.sha256(raw).hexdigest()


def test_default_discovery_uses_external_store_but_authenticates_product_pins(
    tmp_path, monkeypatch,
) -> None:
    product, operations, _cfg, logical, digest = _stores(tmp_path, monkeypatch)
    (product / logical).unlink()
    _commit(product, "remove operational report from product")
    _git(product, "update-ref", "refs/remotes/origin/main", "HEAD")
    report = fleet_release.discover_latest_report(fetch=False)
    assert report.source_path == logical
    assert report.digest_sha256 == digest
    assert fleet_release.runtime_repo() == product
    assert fleet_release.report_repo() == operations
    assert fleet_release.git_is_ancestor(
        operations, report.release.sha, "origin/main",
    ) is None
    with pytest.raises(fleet_release.FleetReleaseError, match="no fleet release reports"):
        fleet_release.discover_latest_report(product, fetch=False)


def test_pre_hardening_digest_requires_original_history(tmp_path, monkeypatch) -> None:
    product, _operations, cfg, logical, digest = _stores(
        tmp_path, monkeypatch, legacy_evidence=True,
    )
    (product / "hardening").write_text("new acceptance policy\n")
    cutoff = _commit(product, "harden evidence")
    _git(product, "update-ref", "refs/remotes/origin/main", "HEAD")
    monkeypatch.setattr(fleet_release, "LEGACY_MAIN_EVIDENCE_CUTOFF", cutoff)

    # A new copy does not acquire historical acceptance on its own.
    cfg.fleet_report_history_repo = None
    with pytest.raises(fleet_release.FleetReleaseError, match="hardening"):
        fleet_release.discover_historical_report_by_digest(logical, digest, fetch=False)
    cfg.fleet_report_history_repo = str(product)
    selected = fleet_release.discover_historical_report_by_digest(
        logical, digest, fetch=False,
    )
    assert selected.digest_sha256 == digest
    assert selected.source_ref != "origin/main"

    # Historical recovery permission never authorizes current deployment.
    with pytest.raises(fleet_release.FleetReleaseError, match="no accepted"):
        fleet_release.discover_latest_report(fetch=False)
    with pytest.raises(fleet_release.FleetReleaseError, match="no blob"):
        fleet_release.discover_historical_report_by_digest(logical, "f" * 64, fetch=False)


def test_invalid_history_configuration_does_not_select_another_store(
    tmp_path, monkeypatch,
) -> None:
    _product, _operations, cfg, logical, digest = _stores(tmp_path, monkeypatch)
    cfg.fleet_report_history_repo = "relative/history"
    with pytest.raises(fleet_release.FleetReleaseError, match="must be absolute"):
        fleet_release.discover_historical_report_by_digest(logical, digest, fetch=False)
