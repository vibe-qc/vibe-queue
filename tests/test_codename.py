"""The release codename catalog and its lookup.

The catalog is the single source of truth for `vq --version`, the CHANGELOG
headers and the annotated tag titles, so the contract it has to keep is small
but load-bearing: every minor resolves, patches inherit, dev builds resolve to
the release they are heading toward, and an unknown version resolves to
nothing rather than to a wrong name.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

import pytest

from vq import __version__
from vq.codename import (
    RELEASE_CODENAMES,
    codename_for_version,
    format_version,
)

_NAME = re.compile(r"^[A-Z][\w.À-ɏ-]*(?:'s)? [A-Z][\w-]*$")


def test_the_running_version_has_a_codename():
    """A release with no name would print a bare number where the series
    promises one. Bumping the version without adding a catalog entry is the
    way that happens, so pin it."""
    assert codename_for_version(__version__) is not None, (
        f"vq {__version__} has no entry in RELEASE_CODENAMES. Add it in the "
        "same commit that bumps the version, per the convention in "
        "docs/version_compatibility.md."
    )


def test_every_minor_line_is_registered():
    """Patches may inherit; a minor may not. An unregistered minor silently
    drags every patch on that line down with it."""
    minors = {
        f"{v.split('.')[0]}.{v.split('.')[1]}.0" for v in RELEASE_CODENAMES
    }
    missing = sorted(m for m in minors if m not in RELEASE_CODENAMES)
    assert not missing, f"minor lines with named patches but no own name: {missing}"


def _minors_by_major(versions: Iterable[str]) -> dict[int, list[int]]:
    """The minor lines present in each major, from every ``X.Y.0`` version."""
    lines: dict[int, list[int]] = {}
    for version in versions:
        major, minor, patch = version.split(".")[:3]
        if patch != "0":
            continue
        lines.setdefault(int(major), []).append(int(minor))
    return {major: sorted(minors) for major, minors in lines.items()}


def _contiguity_gaps(versions: Iterable[str]) -> dict[int, list[int]]:
    """Unnamed minor lines, per major.

    Contiguity is a property of a major line, not of the minor field on its
    own. The series began mid-line at v0.7.0, so the first major starts
    wherever it starts; every later major must start at ``.0``, because
    1.3.0 cannot ship without 1.0.0.
    """
    lines = _minors_by_major(versions)
    first_major = min(lines)
    gaps = {}
    for major, minors in lines.items():
        start = minors[0] if major == first_major else 0
        missing = sorted(set(range(start, minors[-1] + 1)) - set(minors))
        if missing:
            gaps[major] = missing
    return gaps


def test_the_series_is_contiguous_within_every_major_line():
    """A gap in the minor sequence means a release shipped unnamed, which is
    what the 2026-09 backfill existed to fix; this keeps it fixed.

    Grouped by major deliberately. Reading the minor field alone worked only
    while every version was 0.x: a 1.0.0 has minor 0, which collapsed the
    expected range onto 0 and reported 1 through 6 as missing from the 0.x
    line. The invariant was never about the minor field, so the first 1.0
    would have failed a test that had nothing to say about it.
    """
    gaps = _contiguity_gaps(RELEASE_CODENAMES)
    assert not gaps, f"unnamed minor lines, by major: {gaps}"


@pytest.mark.parametrize(
    ("versions", "expected"),
    [
        # The case the old shape got wrong: a 1.0.0 opens a new line, it does
        # not punch a hole in the 0.x one.
        (["0.7.0", "0.8.0", "1.0.0"], {}),
        (["0.7.0", "0.8.0", "1.0.0", "1.1.0"], {}),
        # Real gaps are still caught, on whichever line they fall.
        (["0.7.0", "0.9.0"], {0: [8]}),
        (["0.7.0", "1.0.0", "1.2.0"], {1: [1]}),
        # A later major has to start at .0; 2.1.0 alone is a missing 2.0.0.
        (["0.7.0", "2.1.0"], {2: [0]}),
        # Patch entries never participate.
        (["0.7.0", "0.7.3", "0.8.0"], {}),
    ],
)
def test_contiguity_is_checked_per_major_line(
    versions: list[str], expected: dict[int, list[int]]
):
    assert _contiguity_gaps(versions) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("0.7.0", "Hoare's Pipeline"),
        ("0.9.0", "Tukey's Window"),
        ("0.25.7", "Härder's Atomicity"),
    ],
)
def test_known_versions_resolve_exactly(version: str, expected: str):
    assert codename_for_version(version) == expected


@pytest.mark.parametrize(
    ("version", "inherits_from"),
    [
        ("0.9.5", "0.9.0"),      # unlisted patch
        ("0.10.4", "0.10.0"),
        ("0.25.99", "0.25.0"),
    ],
)
def test_an_unlisted_patch_inherits_its_minor(version: str, inherits_from: str):
    assert codename_for_version(version) == RELEASE_CODENAMES[inherits_from]


def test_a_listed_patch_overrides_its_minor():
    """The v0.7.x and v0.8.x lines named nearly every patch. An explicit entry
    has to win over the inheritance fallback, or those 40-odd names are dead
    weight."""
    assert RELEASE_CODENAMES["0.7.3"] != RELEASE_CODENAMES["0.7.0"]
    assert codename_for_version("0.7.3") == "Dijkstra's Semaphore"


@pytest.mark.parametrize(
    "version",
    ["0.25.0.dev0", "0.25.0.dev17", "0.25.0a1", "0.25.0b2", "0.25.0rc1"],
)
def test_prerelease_builds_inherit_the_release_they_head_toward(version: str):
    """PEP 440 suffixes strip, so a dev build's banner is right before the tag
    drops rather than after."""
    assert codename_for_version(version) == RELEASE_CODENAMES["0.25.0"]


@pytest.mark.parametrize("version", ["99.0.0", "0.6.39", "", "nonsense"])
def test_an_unregistered_version_resolves_to_nothing(version: str):
    """Not to a neighbour, and not to an exception. vq 0.6.39 predates the
    convention and must stay nameless."""
    assert codename_for_version(version) is None


def test_format_version_omits_empty_quotes_when_unnamed():
    assert format_version("0.25.7") == '0.25.7 "Härder\'s Atomicity"'
    assert format_version("0.6.39") == "0.6.39"


def test_no_codename_is_used_twice():
    """A codename identifies a release. Two releases sharing one is a defect.

    It is also an easy one to introduce and a hard one to notice: names are
    picked at ship time against the recent minors, and the v0.7.x/v0.8.x lines
    named nearly every *patch*, so a name can be free among the minors and
    still taken. Four forward entries were added that way in `4e020b5` --
    v0.27.0, v0.28.0, v0.29.0 and v0.33.0 each reused a patch's name -- and
    nothing caught it until the catalogue was read by hand.

    Rendered artwork is filed under a slug derived from the name, so a
    collision does not stay abstract: two releases resolve to one image.
    """
    seen: dict[str, list[str]] = {}
    for version, name in RELEASE_CODENAMES.items():
        seen.setdefault(name, []).append(version)
    clashes = {
        name: sorted(vs, key=lambda v: [int(p) for p in v.split(".")])
        for name, vs in seen.items()
        if len(vs) > 1
    }
    assert not clashes, "codenames used by more than one release: " + "; ".join(
        f"{name!r} -> {', '.join(vs)}" for name, vs in sorted(clashes.items())
    )


def test_every_name_follows_the_house_shape():
    """``[Surname]'s [Object]``. The shape is what keeps vq's series legible
    beside vibe-qc's; a name that breaks it reads as a typo."""
    offenders = [
        f"{v} -> {n}" for v, n in RELEASE_CODENAMES.items() if not _NAME.match(n)
    ]
    assert not offenders, f"codenames not in [Surname]'s [Object] form: {offenders}"


def _version_output() -> str:
    from click.testing import CliRunner

    from vq.cli import main

    result = CliRunner().invoke(main, ["--version"])
    assert result.exit_code == 0
    return result.output.strip()


def test_the_cli_surfaces_the_codename():
    """`vq --version` is the only place most people ever see one."""
    out = _version_output()
    assert __version__ in out
    assert codename_for_version(__version__) in out


# Mirrors doctor._scheduler_remote_vq_check. Keep the two identical.
_HELPER_VERSION = re.compile(r"\bversion\s+([^\s,;]+)")


def test_the_version_line_stays_machine_parseable():
    """A scheduler host's helper is identified by parsing this line.

    ``vq doctor`` runs ``vq --version`` on the remote helper and pulls the
    version out with the regex mirrored above. Adding the codename without
    keeping click's ``, version `` infix made that match fail, so
    ``helper_version`` came back None on every scheduler host -- silently,
    because the check reports on the return code and the SHA, not on whether
    the version parsed.

    A fleet is routinely mixed-version, so this is not only about the current
    driver: an older driver parses a newer helper's output during a rollout.
    """
    out = _version_output()
    match = _HELPER_VERSION.search(out)
    assert match is not None, (
        f"vq --version no longer matches the parser in doctor.py: {out!r}. "
        "Keep click's ', version ' infix and append to it."
    )
    assert match.group(1) == __version__


def test_the_version_line_keeps_clicks_default_prefix():
    """The prefix is a compatibility surface, not a style choice.

    Everything before the version number is exactly what click emits by
    default, so any consumer written against an older vq keeps working.
    """
    assert _version_output().startswith(f"vq, version {__version__}")
