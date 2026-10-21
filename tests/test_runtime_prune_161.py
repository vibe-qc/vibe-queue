"""#161: pruning deployed generations nothing keeps.

`vq runtime-bundle prune --root ROOT` plans, and with --apply removes, every
deployed generation that is neither the current nor the previous selection,
nor bound by a queued or running job in this host's queue, nor named with
--keep. Bindings are read again under the deployment lock, each generation is
renamed out of place before removal, and the artifact store keeps the bundle,
so `deploy` brings a pruned generation back. Real pack/publish/deploy
generations throughout.
"""
from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, paths
from vq import runtime_bundle as rb
from vq.cli import main
from vq.spec import JobSpec, JobState, RuntimeBundlePin

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_binding_161 import _bundle  # noqa: E402
from test_runtime_bundle import _profile, _trust  # noqa: E402

Three = tuple[Path, str, str, str, Path]


@pytest.fixture
def three(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Three:
    """A, B and C deployed in that order: C is current, B previous, A neither."""
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    manifests = [_bundle(tmp_path, value, dependency)
                 for value, dependency in (("41", "b"), ("42", "d"), ("43", "f"))]
    trust = _trust(tmp_path, *manifests)
    root = tmp_path / "runtime"
    for value, manifest in zip(("41", "42", "43"), manifests, strict=True):
        rb.publish(tmp_path / f"bundle-{value}", tmp_path / "store", trust)
        rb.deploy(tmp_path / "store", manifest.artifact_id, root, trust, _profile())
    a, b, c = (m.artifact_id for m in manifests)
    return root, a, b, c, trust


def _bind(jobid: str, root: Path, artifact: str, state: JobState) -> None:
    JobSpec(id=jobid, command=["true"], cwd=str(paths.queue_dir().parent / jobid), cpus=1,
            state=state, runtime_bundle=RuntimeBundlePin(root=str(root), artifact_id=artifact),
            ).write(paths.queue_dir() / f"{jobid}.json")


def _prune(*args: str) -> dict[str, Any]:
    result = CliRunner().invoke(main, ["runtime-bundle", "prune", *args])
    assert result.exit_code == 0, result.output
    payload: dict[str, Any] = json.loads(result.output)
    return payload


def _actions(plan: dict[str, Any]) -> dict[str, tuple[str, list[str]]]:
    return {g["artifact_id"]: (g["action"], g["reasons"]) for g in plan["generations"]}


class TestPlan:
    def test_only_an_unkept_generation_is_prunable_and_nothing_changes(
        self, three: Three,
    ) -> None:
        root, a, b, c, _trust_file = three
        plan = _prune("--root", str(root))
        assert plan["schema"] == "vq.runtime-prune-plan/1"
        assert _actions(plan) == {a: ("prune", []), b: ("keep", ["previous"]),
                                  c: ("keep", ["current"])}
        sizes = {g["artifact_id"]: g["bytes"] for g in plan["generations"]}
        assert sizes[a] > 0 and plan["reclaimable_bytes"] == sizes[a]
        assert (root / "generations" / a).is_dir()  # a plan removes nothing

    def test_a_live_job_keeps_its_generation_and_a_finished_one_does_not(
        self, three: Three,
    ) -> None:
        root, a, b, _c, _trust_file = three
        _bind("queued", root, a, JobState.PENDING)
        _bind("running", root, a, JobState.RUNNING)
        _bind("done", root, b, JobState.COMPLETED)
        _bind("elsewhere", root.parent / "other-root", a, JobState.PENDING)
        actions = _actions(_prune("--root", str(root)))
        assert actions[a] == ("keep", ["bound by job queued", "bound by job running"])
        assert actions[b] == ("keep", ["previous"])

    def test_a_binding_recorded_through_another_spelling_still_counts(
        self, three: Three,
    ) -> None:
        root, a, _b, _c, _trust_file = three
        alias = root.parent / "runtime-alias"
        alias.symlink_to(root, target_is_directory=True)
        _bind("via-alias", alias, a, JobState.PENDING)
        assert _actions(_prune("--root", str(root)))[a][0] == "keep"

    def test_keep_protects_a_named_generation(self, three: Three) -> None:
        root, a, _b, _c, _trust_file = three
        assert _actions(_prune("--root", str(root), "--keep", a))[a] == (
            "keep", ["kept by --keep"])
        refused = CliRunner().invoke(main, ["runtime-bundle", "prune", "--root", str(root),
                                            "--keep", "abc"])
        assert refused.exit_code != 0 and "full SHA256 identity" in refused.output

    def test_other_entries_are_left_alone_and_leftovers_reported(self, three: Three) -> None:
        root, a, _b, _c, _trust_file = three
        generations = root / "generations"
        (generations / ".vq-deploy-staging").mkdir()
        (generations / "notes.txt").write_text("operator notes\n")
        (generations / f".pruning-{a}-1").mkdir()
        plan = _prune("--root", str(root))
        assert plan["left_alone"] == [".vq-deploy-staging", "notes.txt"]
        assert plan["interrupted"] == [f".pruning-{a}-1"]

    def test_a_root_that_is_not_a_deployment_target_is_refused(self, tmp_path: Path) -> None:
        other = tmp_path / "other"
        other.mkdir()
        (other / "something").write_text("x")
        with pytest.raises(rb.BundleError, match="unrecognized"):
            rb.prune_plan(other, bound={})


class TestApply:
    def test_it_removes_only_the_unkept_generation_and_deploy_restores_it(
        self, tmp_path: Path, three: Three,
    ) -> None:
        root, a, b, c, trust = three
        result = _prune("--root", str(root), "--apply")
        assert result["schema"] == "vq.runtime-prune/1" and result["removed"] == [a]
        assert not (root / "generations" / a).exists()
        assert sorted(os.listdir(root / "generations")) == sorted([b, c])
        with pytest.raises(rb.BundleError, match="not a verified deployment"):
            rb.verified_generation(root, a)
        assert rb.verified_generation(root)[0] == c and rb.verified_generation(root, b)[0] == b
        # The store kept the bundle: a deploy brings it back (and selects it).
        rb.deploy(tmp_path / "store", a, root, trust, _profile())
        assert rb.verified_generation(root, a)[0] == a

    def test_bindings_are_read_again_under_the_lock(
        self, three: Three, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        root, a, _b, _c, _trust_file = three
        held: list[bool] = []
        inside = False

        @contextlib.contextmanager
        def lock(*roots: Path) -> Iterator[None]:
            nonlocal inside
            assert roots == (rb._root(root),)
            inside = True
            try:
                yield
            finally:
                inside = False

        def late_binding() -> dict[str, list[str]]:
            held.append(inside)
            return {a: ["submitted-after-the-plan"]}

        monkeypatch.setattr(rb, "_locks", lock)
        result = rb.prune(root, bound=late_binding)
        assert held == [True] and result["removed"] == []
        assert (root / "generations" / a).is_dir()

    def test_an_interrupted_prune_is_finished_and_others_stay(self, three: Three) -> None:
        root, _a, _b, _c, _trust_file = three
        generations = root / "generations"
        leftover = generations / f".pruning-{'e' * 64}-99"
        (leftover / "payload" / "bin").mkdir(parents=True)
        (leftover / "payload" / "bin" / "tool").write_text("x")
        for path in (leftover / "payload" / "bin", leftover / "payload"):
            path.chmod(0o555)
        (generations / ".vq-deploy-staging").mkdir()
        result = _prune("--root", str(root), "--apply")
        assert result["removed_interrupted"] == [leftover.name]
        assert not leftover.exists() and (generations / ".vq-deploy-staging").is_dir()

    def test_a_link_inside_a_generation_is_removed_not_followed(
        self, tmp_path: Path, three: Three,
    ) -> None:
        root, a, _b, _c, _trust_file = three
        outside = tmp_path / "outside"
        (outside / "keep-me").mkdir(parents=True)
        (outside / "keep-me" / "data").write_text("precious\n")
        generation = root / "generations" / a
        generation.chmod(0o755)
        (generation / "escape").symlink_to(outside / "keep-me", target_is_directory=True)
        (generation / "escape-file").symlink_to(outside / "keep-me" / "data")
        _prune("--root", str(root), "--apply")
        assert not generation.exists()
        assert (outside / "keep-me" / "data").read_text() == "precious\n"
        assert (outside / "keep-me").stat().st_mode & 0o777 != 0o700  # never chmod-ed


def test_a_linked_generation_name_is_left_alone_outside_untouched(
    tmp_path: Path, three: Three,
) -> None:
    root, _a, _b, _c, _trust_file = three
    outside = tmp_path / "elsewhere"
    (outside / "payload").mkdir(parents=True)
    (outside / "payload" / "data").write_text("not this root's\n")
    linked = root / "generations" / ("d" * 64)
    linked.symlink_to(outside, target_is_directory=True)
    plan = _prune("--root", str(root))
    assert linked.name in plan["left_alone"] and linked.name not in _actions(plan)
    _prune("--root", str(root), "--apply")
    assert linked.is_symlink() and (outside / "payload" / "data").exists()
    assert outside.stat().st_mode & 0o777 != 0o700


def test_removal_runs_on_the_renamed_copy_never_in_place(
    three: Three, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, a, _b, _c, _trust_file = three
    removed: list[str] = []
    real = rb._remove_tree

    def watch(path: Path) -> None:
        # By now the generation's own name resolves to nothing.
        assert not (root / "generations" / a).exists()
        removed.append(path.name)
        real(path)

    monkeypatch.setattr(rb, "_remove_tree", watch)
    rb.prune(root, bound=dict)
    assert removed == [f".pruning-{a}-{os.getpid()}"]
