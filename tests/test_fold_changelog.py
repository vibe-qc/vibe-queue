"""`scripts/fold_changelog.py`: fragments in `changelog.d/` become `CHANGELOG.md` entries."""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "fold_changelog.py"

CHANGELOG = """# Changelog

## [Unreleased]

### Added

- An existing added entry (#1).

### Fixed

- An existing fixed entry (#2).

## [0.1.0] - 2026-01-01

### Fixed

- Old.
"""


def _module():
    spec = importlib.util.spec_from_file_location("fold_changelog", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _layout(tmp_path: Path) -> tuple[Path, Path]:
    fragments = tmp_path / "changelog.d"
    fragments.mkdir()
    (fragments / "README.md").write_text("how fragments are named\n", encoding="utf-8")
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(CHANGELOG, encoding="utf-8")
    return changelog, fragments


def test_fold_places_fragments_by_kind_and_issue(tmp_path: Path) -> None:
    fold = _module()
    changelog, fragments = _layout(tmp_path)
    (fragments / "40-later.fixed.md").write_text("**Later fix.** Body (#40).\n", encoding="utf-8")
    (fragments / "7-9-earlier.fixed.md").write_text(
        "**Earlier fix.**\nSecond line.\n\nSecond paragraph (#7, #9).\n", encoding="utf-8",
    )
    (fragments / "12-new-default.changed.md").write_text(
        "**A default changed** (#12).\n", encoding="utf-8",
    )
    (fragments / "2-nested.added.md").write_text(
        "**Nested list** inside an entry (#2).\n\n- one\n- two\n", encoding="utf-8",
    )

    rc = fold.main(["--changelog", str(changelog), "--fragments", str(fragments)])

    assert rc == 0
    assert changelog.read_text(encoding="utf-8") == """# Changelog

## [Unreleased]

### Added

- An existing added entry (#1).

- **Nested list** inside an entry (#2).

  - one
  - two

### Changed

- **A default changed** (#12).

### Fixed

- An existing fixed entry (#2).

- **Earlier fix.**
  Second line.

  Second paragraph (#7, #9).

- **Later fix.** Body (#40).

## [0.1.0] - 2026-01-01

### Fixed

- Old.
"""
    # The fragments are consumed; the README keeps the directory in git.
    assert sorted(path.name for path in fragments.iterdir()) == ["README.md"]


def test_check_reports_every_malformed_fragment_and_writes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    fold = _module()
    changelog, fragments = _layout(tmp_path)
    (fragments / "notes.md").write_text("no issue, no kind\n", encoding="utf-8")
    (fragments / "5-faster.perf.md").write_text("not a Keep a Changelog kind\n", encoding="utf-8")
    (fragments / "6-empty.fixed.md").write_text("\n", encoding="utf-8")
    (fragments / "8-marked.fixed.md").write_text("- already a bullet (#8)\n", encoding="utf-8")
    (fragments / "10-sliced.fixed.md").write_text(
        "### Fixed\n\n**A changelog slice, not an entry** (#10).\n", encoding="utf-8",
    )
    (fragments / "9-fine.fixed.md").write_text("**Fine** (#9).\n", encoding="utf-8")

    rc = fold.main(["--check", "--changelog", str(changelog), "--fragments", str(fragments)])

    assert rc == 1
    err = capsys.readouterr().err
    assert "notes.md: expected <issues>-<topic>.<kind>.md" in err
    assert "5-faster.perf.md: kind 'perf' is not one of" in err
    assert "6-empty.fixed.md: empty" in err
    assert "8-marked.fixed.md: starts with a list marker" in err
    assert "10-sliced.fixed.md: line 1 is a Markdown heading" in err
    assert "9-fine" not in err
    assert changelog.read_text(encoding="utf-8") == CHANGELOG
    assert (fragments / "9-fine.fixed.md").exists()


def test_fold_without_fragments_changes_nothing(tmp_path: Path) -> None:
    fold = _module()
    changelog, fragments = _layout(tmp_path)

    rc = fold.main(["--changelog", str(changelog), "--fragments", str(fragments)])

    assert rc == 0
    assert changelog.read_text(encoding="utf-8") == CHANGELOG


def test_fold_refuses_a_changelog_without_an_unreleased_section(tmp_path: Path) -> None:
    fold = _module()
    changelog, fragments = _layout(tmp_path)
    changelog.write_text("# Changelog\n\n## [0.1.0] - 2026-01-01\n", encoding="utf-8")
    (fragments / "3-thing.fixed.md").write_text("**Thing** (#3).\n", encoding="utf-8")

    rc = fold.main(["--changelog", str(changelog), "--fragments", str(fragments)])

    assert rc == 1
    assert (fragments / "3-thing.fixed.md").exists()


def test_fold_creates_missing_sections_in_keep_a_changelog_order(tmp_path: Path) -> None:
    fold = _module()
    changelog, fragments = _layout(tmp_path)
    changelog.write_text(
        "# Changelog\n\n## [Unreleased]\n\n## [0.1.0] - 2026-01-01\n", encoding="utf-8",
    )
    (fragments / "2-b.security.md").write_text("**S** (#2).\n", encoding="utf-8")
    (fragments / "1-a.added.md").write_text("**A** (#1).\n", encoding="utf-8")
    (fragments / "3-c.removed.md").write_text("**R** (#3).\n", encoding="utf-8")

    assert fold.main(["--changelog", str(changelog), "--fragments", str(fragments)]) == 0

    assert changelog.read_text(encoding="utf-8") == (
        "# Changelog\n\n## [Unreleased]\n\n"
        "### Added\n\n- **A** (#1).\n\n"
        "### Removed\n\n- **R** (#3).\n\n"
        "### Security\n\n- **S** (#2).\n\n"
        "## [0.1.0] - 2026-01-01\n"
    )


def test_the_repository_fragments_are_well_formed() -> None:
    """CI runs this: a misnamed fragment in `changelog.d/` fails here, not at the cut."""
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "all well-formed" in proc.stdout
