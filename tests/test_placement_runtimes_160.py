"""#160/#161: a daemon's deployed runtimes as its placement runtime digests.

`vq runtime-bundle generations --root ROOT` lists a deployment root's
generations, each with its program and whether `verified_generation` proves
it deployed. `vq placement fleet --runtime-root NAME=ROOT` reads that
listing from daemon target NAME and records each program's verified
generations as its runtimes, current one first. The listing runs on real
pack/publish/deploy generations; the overlay uses a stand-in for SSH.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, transport
from vq import placement_policy as pp
from vq import runtime_bundle as rb
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_placement_policy_160 import _fleet  # noqa: E402
from test_runtime_prune_161 import Three, three  # noqa: E402,F401

A, B, C = "a" * 64, "b" * 64, "c" * 64


class TestListing:
    def test_every_generation_its_program_and_its_proof(self, three: Three) -> None:  # noqa: F811
        root, a, b, c, _trust = three
        (root / "generations" / a / "verification.json").unlink()  # no longer proven
        listing = rb.deployed_generations(root)
        assert (listing["schema"], listing["current"], listing["previous"]) == (
            "vq.runtime-generations/1", c, b)
        assert {g["artifact_id"]: (g["program"], g["verified"])
                for g in listing["generations"]} == {
            a: ("fixture", False), b: ("fixture", True), c: ("fixture", True)}

    def test_only_real_generation_directories_are_listed(
        self, three: Three, tmp_path: Path,  # noqa: F811
    ) -> None:
        root, a, b, c, _trust = three
        generations = root / "generations"
        (generations / ".vq-deploy-staging").mkdir()
        (generations / "notes.txt").write_text("x")
        (generations / ("d" * 64)).symlink_to(generations / a, target_is_directory=True)
        assert [g["artifact_id"] for g in rb.deployed_generations(root)["generations"]] == (
            sorted([a, b, c]))

    def test_the_cli_lists_them(self, three: Three) -> None:  # noqa: F811
        root, _a, _b, c, _trust = three
        as_json = CliRunner().invoke(main, ["runtime-bundle", "generations",
                                            "--root", str(root), "--json"])
        assert as_json.exit_code == 0, as_json.output
        assert json.loads(as_json.output)["current"] == c
        text = CliRunner().invoke(main, ["runtime-bundle", "generations", "--root", str(root)])
        assert text.exit_code == 0 and f"{c[:12]}  fixture" in text.output
        assert "verified  (current)" in text.output

    def test_a_root_that_is_not_a_deployment_target_is_refused(self, tmp_path: Path) -> None:
        other = tmp_path / "other"
        other.mkdir()
        (other / "x").write_text("x")
        with pytest.raises(rb.BundleError, match="unrecognized"):
            rb.deployed_generations(other)


def _listing(*generations: tuple[str, str | None, bool], current: str | None = B,
             ) -> dict[str, Any]:
    return {"schema": "vq.runtime-generations/1", "root": "/r", "current": current,
            "previous": None, "generations": [
                {"artifact_id": i, "program": p, "verified": v} for i, p, v in generations]}


class TestRuntimes:
    def test_verified_generations_per_program_current_first(self) -> None:
        runtimes, skipped = pp.runtimes_from_generations(_listing(
            (A, "vibe-qc", True), (B, "vibe-qc", True), (C, "vibe-qc", False),
            ("d" * 64, "orca", True), ("e" * 64, None, True)))
        assert runtimes == {"vibe-qc": [B, A], "orca": ["d" * 64]} and skipped == []
        assert pp.runtimes_from_generations(_listing(
            (A, "vibe-qc", True), (B, "vibe-qc", True), current=None))[0] == {
            "vibe-qc": [A, B]}

    def test_a_program_no_placement_record_can_name_is_skipped(self) -> None:
        runtimes, skipped = pp.runtimes_from_generations(_listing((A, "Vibe QC", True)))
        assert runtimes == {} and skipped == ["program 'Vibe QC' is not a placement label"]

    @pytest.mark.parametrize("listing", [
        {"schema": "other"}, [], {"schema": "vq.runtime-generations/1", "generations": {}},
        _listing(("short", "x", True)), _listing((A, "x", "yes")), _listing((A, 7, True)),
    ])
    def test_a_malformed_listing_is_refused(self, listing: object) -> None:
        with pytest.raises(pp.PlacementError):
            pp.runtimes_from_generations(listing)


@pytest.fixture
def ssh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        '[hosts.idle-slow]\nssh = "idle-slow"\n[hosts.stale]\nssh = "stale"\n'
        '[hosts.fast-busy]\nssh = "fast-busy"\n')
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    state: dict[str, Any] = {"calls": [], "fail": set(), "listing": _listing(
        (A, "vibe-qc", True), (B, "vibe-qc", True), (C, "orca", True))}

    def run_remote_vq(host_cfg: Any, *argv: str, **kwargs: Any) -> Any:
        state["calls"].append((host_cfg.ssh, argv))
        assert kwargs["owned_process_group"] is True
        if host_cfg.ssh in state["fail"]:
            raise TimeoutError("ssh timed out")
        return SimpleNamespace(stdout=json.dumps(state["listing"]))

    monkeypatch.setattr(transport, "run_remote_vq", run_remote_vq)
    fleet = _fleet()
    fleet["targets"][1]["programs"]["cp2k"] = ["f" * 64]  # not in the listing: kept
    base = tmp_path / "base.json"
    base.write_text(json.dumps(fleet))
    state["base"] = base
    return state


def _fleet_cli(ssh: dict[str, Any], *args: str) -> Any:
    return CliRunner().invoke(main, ["placement", "fleet", str(ssh["base"]), *args])


def test_a_daemons_runtimes_come_from_its_root(ssh: dict[str, Any]) -> None:
    result = _fleet_cli(ssh, "--runtime-root", "idle-slow=/opt/runtimes/vibe-qc")
    assert result.exit_code == 0, result.output
    idle = next(t for t in json.loads(result.stdout)["targets"] if t["name"] == "idle-slow")
    assert idle["programs"] == {"vibe-qc": [B, A], "orca": [C], "cp2k": ["f" * 64]}
    assert ssh["calls"] == [("idle-slow", ("runtime-bundle", "generations", "--root",
                                           "/opt/runtimes/vibe-qc", "--json"))]
    assert "idle-slow: orca 1 runtime(s), vibe-qc 2 runtime(s) at /opt/runtimes/vibe-qc" in (
        result.stderr)
    # A request pinned to a deployed generation is now planned there.
    request = {"schema_name": "vq.placement-request/1", "program": "vibe-qc",
               "runtime_digest": A, "cpus": 4}
    fleet = pp.Fleet.model_validate(json.loads(result.stdout))
    explained = pp.explain(fleet, pp.Request.model_validate(request),
                           now=pp._parse_time(fleet.targets[1].observed_at, "now"),
                           max_age_seconds=300)
    assert "idle-slow" in [c["target"] for c in explained["candidates"]]


def test_a_target_that_gives_no_listing_keeps_its_runtimes(ssh: dict[str, Any]) -> None:
    ssh["fail"].add("idle-slow")
    result = _fleet_cli(ssh, "--runtime-root", "idle-slow=/opt/runtimes/vibe-qc")
    assert result.exit_code == 0, result.output
    idle = next(t for t in json.loads(result.stdout)["targets"] if t["name"] == "idle-slow")
    assert idle["programs"] == {"vibe-qc": ["a" * 64], "cp2k": ["f" * 64]}
    assert idle["reachable"] is True  # a runtime listing is not reachability evidence
    assert "idle-slow: runtimes kept (idle-slow gave no generation listing" in result.stderr


@pytest.mark.parametrize(("args", "message"), [
    (["--runtime-root", "fast-busy=/r"], "not a daemon target of the fleet"),
    (["--runtime-root", "nowhere=/r"], "not a daemon target of the fleet"),
    (["--runtime-root", "down=/r"], "not a configured [hosts.down]"),
    (["--runtime-root", "idle-slow=relative/r"], "expected NAME=/absolute/root"),
    (["--runtime-root", "idle-slow"], "expected NAME=/absolute/root"),
    (["--runtime-root", "idle-slow=/r", "--runtime-root", "idle-slow=/s"], "given twice"),
])
def test_misdirected_roots_contact_no_host(ssh: dict[str, Any], args: list[str],
                                           message: str) -> None:
    result = _fleet_cli(ssh, *args)
    assert result.exit_code != 0 and message in result.output, result.output
    assert ssh["calls"] == []
