"""The brand assets are hand-written SVG, and three of them share a glyph.

`docs/_static/logo/` holds a favicon, two wordmarks and a social card. The
queue glyph -- three tokens advancing along a rail in a rounded unit-cell
frame -- is duplicated into all four, because inlining it keeps each file
standalone and dependency-free, which is the same trade vibe-qc makes.

The cost of that trade is drift: someone adjusts the favicon, the wordmarks
keep the old glyph, and the sidebar stops matching the browser tab. Nothing
else would notice. This does.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

_LOGO = Path(__file__).resolve().parent.parent / "docs" / "_static" / "logo"

_WORDMARKS = ("vq-wordmark-light.svg", "vq-wordmark-dark.svg")
_ALL = ("vq-favicon.svg", *_WORDMARKS, "vq-social.svg")

# Teal is the shared family colour; vibe-qc's glyph uses the same value.
_TEAL = "#0F766E"


def _read(name: str) -> str:
    return (_LOGO / name).read_text(encoding="utf-8")


def _glyph_shapes(svg: str) -> list[str]:
    """The shapes inside the ``vq-glyph`` group, whitespace-normalised.

    Extracted by the explicit group rather than by colour: the social card has
    decorative teal elements of its own, and matching on the family colour
    swept those up too.
    """
    block = re.search(r'<g id="vq-glyph"[^>]*>(.*?)</g>', svg, re.S)
    assert block is not None, "no element with id=\"vq-glyph\""
    return [
        re.sub(r"\s+", " ", el).strip()
        for el in re.findall(r"<(?:rect|line)\b[^>]*/>", block.group(1))
    ]


@pytest.mark.parametrize("name", _ALL)
def test_every_asset_is_well_formed_svg(name: str):
    ET.fromstring(_read(name))


@pytest.mark.parametrize("name", _ALL)
def test_every_asset_is_labelled_for_screen_readers(name: str):
    """These ship as the site's logo and its social card; both are content."""
    svg = _read(name)
    assert 'role="img"' in svg, f"{name} has no role"
    assert "<title>" in svg, f"{name} has no <title>"
    assert "<desc>" in svg, f"{name} has no <desc>"


def test_the_glyph_has_not_drifted_between_copies():
    """Four files, one glyph. Edit one and this names the others."""
    reference = _glyph_shapes(_read("vq-favicon.svg"))
    assert len(reference) == 5, (
        "expected the frame, three tokens and the rail; got "
        f"{len(reference)} shapes"
    )
    for name in _WORDMARKS + ("vq-social.svg",):
        assert _glyph_shapes(_read(name)) == reference, (
            f"{name}'s glyph differs from vq-favicon.svg. The glyph is "
            "duplicated on purpose; update every copy together."
        )


def test_the_wordmarks_differ_only_in_ink_colour():
    """Light and dark are the same mark. Anything else is an accident."""
    light, dark = (_read(n) for n in _WORDMARKS)
    assert light.replace("#0F172A", "@") == dark.replace("#F1F5F9", "@")


def test_the_family_colour_is_shared_with_vibe_qc():
    """The glyph reading as a sibling of vibe-qc's is the whole point."""
    for name in _ALL:
        assert _TEAL in _read(name), f"{name} does not use the family teal"


def test_conf_points_at_assets_that_exist():
    conf = (_LOGO.parent.parent / "conf.py").read_text(encoding="utf-8")
    referenced = re.findall(r'"(?:_static/)?(logo/[A-Za-z0-9_.-]+\.svg)"', conf)
    assert referenced, "conf.py references no logo assets"
    for rel in referenced:
        assert (_LOGO.parent / rel).is_file(), f"conf.py points at missing {rel}"
