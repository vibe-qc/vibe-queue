"""The split's two report locations are one report, not two."""

from __future__ import annotations

import pytest

from vq import fleet_release, report_paths

MONOREPO = "vibe-queue/releases/v0.15.118.json"
SPLIT = "releases/v0.15.118.json"


@pytest.mark.parametrize(
    ("left", "right", "expected"),
    [
        pytest.param(MONOREPO, SPLIT, True, id="journal-vs-discovered"),
        pytest.param(SPLIT, MONOREPO, True, id="discovered-vs-journal"),
        pytest.param(MONOREPO, MONOREPO, True, id="identical"),
        pytest.param(MONOREPO, "releases/v0.15.119.json", False, id="other-tag"),
        pytest.param(MONOREPO, "elsewhere/v0.15.118.json", False, id="illegitimate-dir"),
        pytest.param("not-a-path", "not-a-path", True, id="equal-non-path-unchanged"),
        pytest.param(None, None, True, id="none-unchanged"),
        pytest.param(None, SPLIT, False, id="missing-left"),
        pytest.param(SPLIT, None, False, id="missing-right"),
    ],
)
def test_same_report(left: object, right: object, expected: bool) -> None:
    assert report_paths.same_report(left, right) is expected


def test_same_report_never_rejects_what_equality_accepted() -> None:
    """The widening is one-way: no previously accepted pair can now fail."""
    samples = [MONOREPO, SPLIT, "releases/v0.15.119.json", "x", "", None, 0]
    for left in samples:
        for right in samples:
            if left == right:
                assert report_paths.same_report(left, right)


def test_fleet_release_reexports_are_the_same_objects() -> None:
    """Existing ``fleet_release.X`` callers must see exactly one definition."""
    for name in (
        "REPORT_DIRECTORY",
        "REPORT_DIRECTORIES",
        "is_report_path",
        "report_paths_for",
        "same_report",
    ):
        assert getattr(fleet_release, name) is getattr(report_paths, name)


def test_report_paths_imports_nothing_from_vq() -> None:
    """It exists so the dependency-free transition module can use it."""
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(report_paths))
    imported = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    assert not {name for name in imported if name == "vq" or name.startswith("vq.")}
