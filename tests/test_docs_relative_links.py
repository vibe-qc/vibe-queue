"""Every relative link in ``docs/**/*.md`` points at something that exists (#117).

``sphinx-build -W`` does not catch a link to a file that is not a document, so
four links to files removed in #54 stood on ``main`` with the build green
(#116). This test resolves each ``[text](target)`` against the page it is on.
A target may be a file or a directory, and it may leave ``docs/`` (``../AGENTS.md``,
``../src/vq/codename.py``), where it is checked as a file in the tree.

Deliberately not checked:

* ``docs/vibe-qc-site/``: those pages are authored here and copied into vibe-qc,
  where their links resolve against vibe-qc's tree, not this one.
* ``#anchor`` fragments: Sphinx checks the ones that are cross-references.
* Links with a scheme, and MyST roles such as ``{ref}``.
* Fenced code blocks and inline code spans, where ``](...)`` is an example.

A link from a published page to a page that ``docs/conf.py`` excludes from the
site still resolves as a file here, so it passes; the site renders it as plain
text.
"""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

import pytest

DOCS = Path(__file__).resolve().parent.parent / "docs"
EXEMPT_TOP_LEVEL = frozenset({"vibe-qc-site", "_build"})

_LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_INLINE_CODE = re.compile(r"`[^`]*`")
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _pages() -> list[Path]:
    return sorted(
        page for page in DOCS.rglob("*.md")
        if page.relative_to(DOCS).parts[0] not in EXEMPT_TOP_LEVEL
    )


def _prose_lines(text: str):
    """Yield ``(line number, line)`` for every line outside a fenced code block."""
    fenced = False
    for number, line in enumerate(text.splitlines(), 1):
        if line.lstrip().startswith(("```", "~~~")):
            fenced = not fenced
            continue
        if not fenced:
            yield number, line


def broken_links(page: Path, root: Path = DOCS) -> list[str]:
    """Return ``"line: target"`` for each relative link on ``page`` that does not resolve."""
    broken = []
    for number, line in _prose_lines(page.read_text(encoding="utf-8")):
        for match in _LINK.finditer(_INLINE_CODE.sub("", line)):
            target = match.group(1)
            if _SCHEME.match(target) or target.startswith(("#", "//", "{")):
                continue
            path = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path:
                continue
            base = root if path.startswith("/") else page.parent
            if not (base / path.lstrip("/")).exists():
                broken.append(f"{number}: {target}")
    return broken


@pytest.mark.parametrize("page", _pages(), ids=lambda p: p.relative_to(DOCS).as_posix())
def test_every_relative_link_resolves(page: Path) -> None:
    broken = broken_links(page)
    assert not broken, (
        f"{page.relative_to(DOCS.parent)} links to files that do not exist "
        "(line: target):\n  " + "\n  ".join(broken)
    )


def test_the_guard_finds_a_dangling_link_and_skips_what_it_should(tmp_path: Path) -> None:
    """A guard that cannot fail is not one: prove it fails, and that it skips."""
    (tmp_path / "there.md").write_text("# There\n")
    (tmp_path / "sub").mkdir()
    page = tmp_path / "page.md"
    page.write_text(
        "[ok](there.md) [ok with anchor](there.md#top) [dir](sub) [web](https://x.example/y)\n"
        "[anchor only](#top) [role]({ref}`x`)\n"
        "`[in a code span](gone.md)`\n"
        "```sh\n[in a fence](gone.md)\n```\n"
        "[missing](gone.md) and [also missing](sub/gone.md#section)\n"
    )
    assert broken_links(page, tmp_path) == ["7: gone.md", "7: sub/gone.md#section"]


def test_the_exemption_is_the_vibe_qc_site_only() -> None:
    top_level = {page.relative_to(DOCS).parts[0] for page in _pages()}
    assert "vibe-qc-site" not in top_level
    assert {"operator", "user", "agent"} <= top_level
