"""#162: a plan variant identified by a #161 portable runtime bundle.

`vq benchmark variant BUNDLE --name NAME` checks the bundle's archive against
its manifest and prints a plan `variants` entry. Its runtime digest is the
artifact ID, which `vq benchmark measure --runtime-root` reports for a
deployment of that bundle. Build flags, toolchain, dependency digest and
archive size come from the bundle. Real pack, publish and deploy throughout.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import benchmark as bm
from vq import benchmark_measure
from vq import runtime_bundle as rb
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_benchmark_162 import _plan  # noqa: E402
from test_runtime_bundle import _profile  # noqa: E402

FLAGS = ["-O2", "-march=x86-64-v3"]


def _pack(tmp_path: Path, name: str, flags: list[str], variant: str = "optimized",
          ) -> rb.BundleManifest:
    payload = tmp_path / f"built-{name}"
    (payload / "bin").mkdir(parents=True)
    (payload / "bin/probe").write_text("#!/bin/sh\ntest -f data/value\n")
    (payload / "bin/probe").chmod(0o755)
    (payload / "data").mkdir()
    (payload / "data/value").write_text(f"{name}\n")
    description = tmp_path / f"{name}.json"
    description.write_text(rb.BuildDescription(
        program="fixture", source_revision="a" * 40, recipe_sha256="c" * 64,
        dependency_sha256="d" * 64, target=_profile(), variant=variant,  # type: ignore[arg-type]
        compiler_flags=flags, toolchain="fixture-cc 13.2", publisher="test-builder",
        build_receipt="fixture-build/1", qualification_receipt="fixture-smoke/1",
        smoke_argv=["bin/probe"]).model_dump_json())
    return rb.pack(payload, description, tmp_path / f"bundle-{name}")


def _variant(bundle: Path, *args: str) -> Any:
    return CliRunner().invoke(main, ["benchmark", "variant", str(bundle), *args])


def test_a_bundle_gives_the_variant_its_identity(tmp_path: Path) -> None:
    manifest = _pack(tmp_path, "tuned", FLAGS)
    result = _variant(tmp_path / "bundle-tuned", "--name", "tuned",
                      "--library", "openblas=0.3.28", "--build-cost-core-seconds", "36000",
                      "--deployment-cost-core-seconds", "1800")
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "name": "tuned", "runtime_digest": manifest.artifact_id,
        "build_flags": "-O2 -march=x86-64-v3",
        "libraries": {"toolchain": "fixture-cc 13.2", "dependencies": "d" * 64,
                      "openblas": "0.3.28"},
        "build_cost_core_seconds": 36000.0, "deployment_cost_core_seconds": 1800.0,
        "storage_bytes": (tmp_path / "bundle-tuned" / "bundle.tar").stat().st_size}
    assert result.stderr == (f"fixture optimized build {manifest.build.build_id[:12]}, "
                             f"artifact {manifest.artifact_id[:12]}\n")


def test_variants_from_bundles_make_a_plan_measure_agrees_with(tmp_path: Path) -> None:
    generic = _pack(tmp_path, "generic", [], variant="generic")
    tuned = _pack(tmp_path, "tuned", FLAGS)
    trust = tmp_path / "trust.json"
    trust.write_text(rb.TrustPolicy(publishers={
        "test-builder": [generic.artifact_id, tuned.artifact_id]}).model_dump_json())
    store = tmp_path / "store"
    for name in ("generic", "tuned"):
        rb.publish(tmp_path / f"bundle-{name}", store, trust)
    variants = []
    for name, manifest in (("generic", generic), ("tuned", tuned)):
        # The published copy names the same variant as the packed one.
        published = store / "artifacts" / manifest.artifact_id
        result = _variant(published, "--name", name)
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == json.loads(
            _variant(tmp_path / f"bundle-{name}", "--name", name).stdout)
        variants.append(json.loads(result.stdout))
    assert variants[0]["build_flags"] is None  # a build without flags records none
    plan_file = tmp_path / "plan.json"
    plan_file.write_text(json.dumps(_plan(variants=variants)))
    plan = bm.load_plan(plan_file)
    assert [v.runtime_digest for v in plan.variants] == [generic.artifact_id,
                                                          tuned.artifact_id]
    root = tmp_path / "runtime"
    rb.deploy(store, tuned.artifact_id, root, trust, _profile())
    assert benchmark_measure.selected_runtime(root)[0] == plan.variants[1].runtime_digest


def test_a_tampered_archive_names_no_variant(tmp_path: Path) -> None:
    _pack(tmp_path, "tuned", FLAGS)
    archive = tmp_path / "bundle-tuned" / "bundle.tar"
    archive.write_bytes(archive.read_bytes() + b"\0" * 512)
    result = _variant(tmp_path / "bundle-tuned", "--name", "tuned")
    assert result.exit_code == 1 and "archive digest mismatch" in result.output
    assert result.stdout == ""


@pytest.mark.parametrize(("args", "message"), [
    (["--name", "tuned", "--library", "toolchain=gcc"], "library toolchain is derived"),
    (["--name", "tuned", "--library", "dependencies=x"], "library dependencies is derived"),
    (["--name", "tuned", "--library", "openblas"], "expected NAME=VALUE"),
    (["--name", "tuned", "--library", "=1"], "expected NAME=VALUE"),
    (["--name", "tuned", "--library", "openblas="], "expected NAME=VALUE"),
    (["--name", "tuned", "--library", "a=1", "--library", "a=2"], "a is given twice"),
    (["--name", "bad name"], "does not fit a plan variant"),
    (["--name", "tuned", "--library", "bad lib=1"], "does not fit a plan variant"),
])
def test_what_cannot_be_a_plan_variant_is_refused(tmp_path: Path, args: list[str],
                                                  message: str) -> None:
    _pack(tmp_path, "tuned", FLAGS)
    result = _variant(tmp_path / "bundle-tuned", *args)
    assert result.exit_code != 0 and message in result.output, result.output


def test_flags_longer_than_a_plan_records_are_refused(tmp_path: Path) -> None:
    _pack(tmp_path, "long", [f"-D{'X' * 250}{i}" for i in range(5)])
    result = _variant(tmp_path / "bundle-long", "--name", "long")
    assert result.exit_code == 1 and "does not fit a plan variant" in result.output


def test_a_directory_that_is_not_a_bundle_is_refused(tmp_path: Path) -> None:
    (tmp_path / "empty").mkdir()
    result = _variant(tmp_path / "empty", "--name", "x")
    assert result.exit_code == 1 and "manifest.json" in result.output
