"""A same-filename re-pin can supersede an obsolete plan-bound hold (#93).

A vq-only (or viewer-only) re-pin regenerates the same ``vX.Y.Z.json`` with a
new digest. The file name alone cannot order the two reports, so the re-pin is
newer only pin by pin: identical or strictly higher, at least one higher.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.test_fleet_rollout import (
    _host_f_PLAN_HOLD_TARGETS,
    _pin,
    _plan_bound_hold,
    _plan_bound_supersede_plan,
    _report_at,
    _supported_drain_snapshot,
)
from vq import fleet_release, fleet_rollout


def _repin(
    report: fleet_release.FleetReleaseReport, digest: str, **changes: tuple[str, str],
) -> fleet_release.FleetReleaseReport:
    pins = dict(report.pins)
    for name, (sha, version) in changes.items():
        pins[name] = _pin(name, sha, version, tag=pins[name].tag)
    return fleet_release.FleetReleaseReport(
        source_ref=report.source_ref, source_path=report.source_path,
        digest_sha256=digest, generated_at=report.generated_at,
        release_version=report.release_version, pins=pins, raw=report.raw,
    )


@pytest.fixture
def journal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fleet_rollout.paths, "state_root", lambda: tmp_path)
    historical = _report_at(155, digest="a" * 64)
    old_rollout = fleet_rollout.rollout_id(historical)
    fleet_rollout.save_run(fleet_rollout.RolloutRun(
        rollout_id=old_rollout,
        report_digest_sha256=historical.digest_sha256,
        report_source_path=historical.source_path,
        holds={host: _plan_bound_hold(old_rollout, host=host)
               for host in _host_f_PLAN_HOLD_TARGETS},
    ))
    reports: dict[tuple[str, str], fleet_release.FleetReleaseReport] = {
        (historical.source_path, historical.digest_sha256): historical,
    }

    def historical_report(source_path: str, digest: str, repo: Path, *, fetch: bool):
        del repo
        assert fetch is False
        try:
            return reports[(source_path, digest)]
        except KeyError:
            raise fleet_release.FleetReleaseError("unknown report") from None

    monkeypatch.setattr(fleet_rollout.fleet_release, "discover_historical_report_by_digest",
                        historical_report)
    return tmp_path, historical, old_rollout, reports


def _inactive(argv: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    del kwargs
    return subprocess.CompletedProcess(
        argv, 0, stdout=json.dumps(_supported_drain_snapshot(host=argv[-1])), stderr="")


def _supersede_all(tmp_path, old_rollout, current) -> None:
    plan = _plan_bound_supersede_plan(current)
    for host in _host_f_PLAN_HOLD_TARGETS:
        fleet_rollout.supersede_obsolete_plan_bound_hold(
            rollout_id_value=old_rollout, host=host, accepted_report=current, plan=plan,
            repo=tmp_path, current_report_digest_resolver=lambda: current.digest_sha256,
            control_runner=_inactive,
        )


def _inventory(tmp_path, current):
    return fleet_rollout._validate_running_journals(
        {},
        current_report_source_path=current.source_path,
        current_report_digest_sha256=current.digest_sha256,
        configured_retention_hosts=frozenset(_host_f_PLAN_HOLD_TARGETS),
        report_repo=tmp_path,
    )


def test_a_queue_only_repin_supersedes_the_obsolete_hold(journal) -> None:
    tmp_path, historical, old_rollout, reports = journal
    current = _repin(historical, "b" * 64, vq=("6" * 40, "0.25.8"))
    reports[(current.source_path, current.digest_sha256)] = current

    _supersede_all(tmp_path, old_rollout, current)

    inventory = _inventory(tmp_path, current)
    assert inventory.retained_hold_refs == frozenset(
        (old_rollout, host) for host in _host_f_PLAN_HOLD_TARGETS
    )
    replay = fleet_rollout.supersede_obsolete_plan_bound_hold(
        rollout_id_value=old_rollout, host="host_f", accepted_report=current,
        plan=_plan_bound_supersede_plan(current), repo=tmp_path,
        current_report_digest_resolver=lambda: current.digest_sha256,
        control_runner=lambda *a, **k: pytest.fail("a replay must not re-probe"),
    )
    assert replay.replayed is True


@pytest.mark.parametrize("change", ["downgrade", "unchanged", "garbled"])
def test_a_repin_that_is_not_pinwise_newer_refuses(journal, change) -> None:
    tmp_path, historical, old_rollout, reports = journal
    if change == "downgrade":
        current = _repin(historical, "b" * 64, vq=("6" * 40, "0.25.8"),
                         vibe_view=("7" * 40, "2.4.9"))
    elif change == "unchanged":
        current = _repin(historical, "b" * 64)
    else:
        current = _repin(historical, "b" * 64, vq=("6" * 40, "0.25.x"))
    reports[(current.source_path, current.digest_sha256)] = current

    with pytest.raises(fleet_rollout.FleetRolloutError, match="not strictly older"):
        fleet_rollout.supersede_obsolete_plan_bound_hold(
            rollout_id_value=old_rollout, host="host_f", accepted_report=current,
            plan=_plan_bound_supersede_plan(current), repo=tmp_path,
            current_report_digest_resolver=lambda: current.digest_sha256,
            control_runner=_inactive,
        )


def test_a_later_repin_does_not_make_existing_receipts_incoherent(journal) -> None:
    tmp_path, historical, old_rollout, reports = journal
    first = _repin(historical, "b" * 64, vq=("6" * 40, "0.25.8"))
    reports[(first.source_path, first.digest_sha256)] = first
    _supersede_all(tmp_path, old_rollout, first)
    second = _repin(first, "c" * 64, vq=("8" * 40, "0.25.9"))
    reports[(second.source_path, second.digest_sha256)] = second

    try:
        _inventory(tmp_path, second)
    except fleet_rollout.FleetRolloutError as exc:
        assert "incoherent current report identity" not in str(exc)
