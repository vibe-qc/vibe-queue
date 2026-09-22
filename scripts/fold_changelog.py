"""Fold the changelog fragments in ``changelog.d/`` into ``CHANGELOG.md``.

Every change a user or an operator would notice is described in a file of its
own, ``changelog.d/<issues>-<topic>.<kind>.md``: ``<issues>`` is the tracker
issue number, or several joined with ``-``; ``<topic>`` is a short kebab-case
name; ``<kind>`` is a Keep a Changelog section in lower case -- ``added``,
``changed``, ``deprecated``, ``removed``, ``fixed`` or ``security``. The file
holds **one** entry, as prose: a bold lead sentence, the body, and the issue
number at the end. No list marker and no heading -- the fold adds the ``- ``
and indents the rest, so a nested list inside the entry is written flush left
and comes out nested. A change that needs an entry in two sections writes two
files, which is what makes each file's name true of its whole contents.

Sessions do not edit ``CHANGELOG.md``: a fragment is a file of its own, so two
sessions landing at once cannot conflict on the ledger. The release cut runs
this script, which moves every fragment under ``## [Unreleased]`` into the
right ``### Kind`` section -- sections in Keep a Changelog order, entries by
issue number -- and deletes the fragments; the cut then renames the section
as before.

    python scripts/fold_changelog.py          # fold, then review and stage
    python scripts/fold_changelog.py --check  # every fragment is well-formed

Nothing here touches git: review the diff, then stage ``CHANGELOG.md`` and
the removed fragments together.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
KINDS = ("added", "changed", "deprecated", "removed", "fixed", "security")
HEADINGS = {kind: f"### {kind.capitalize()}" for kind in KINDS}
KIND_OF_HEADING = {heading: kind for kind, heading in HEADINGS.items()}
FRAGMENT_NAME = re.compile(
    r"^(?P<issues>\d+(?:-\d+)*)-(?P<topic>[a-z0-9]+(?:-[a-z0-9]+)*)\.(?P<kind>[a-z]+)\.md$"
)
UNRELEASED = "## [Unreleased]"

Fragment = tuple[int, str, Path, str]


class FragmentError(ValueError):
    """A fragment, or the changelog, is not shaped as the fold expects."""


def parse_name(path: Path) -> tuple[int, str]:
    """Return the first issue number and the kind, or raise FragmentError."""
    match = FRAGMENT_NAME.match(path.name)
    if match is None:
        raise FragmentError(f"{path.name}: expected <issues>-<topic>.<kind>.md")
    kind = match.group("kind")
    if kind not in KINDS:
        raise FragmentError(f"{path.name}: kind {kind!r} is not one of {', '.join(KINDS)}")
    return int(match.group("issues").split("-")[0]), kind


def load_fragments(directory: Path) -> dict[str, list[Fragment]]:
    """Fragments by kind, ordered by issue number; every problem is reported at once."""
    problems: list[str] = []
    by_kind: dict[str, list[Fragment]] = {kind: [] for kind in KINDS}
    for path in sorted(directory.glob("*.md")) if directory.is_dir() else []:
        if path.name.lower() == "readme.md":
            continue
        try:
            issue, kind = parse_name(path)
        except FragmentError as exc:
            problems.append(str(exc))
            continue
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            problems.append(f"{path.name}: empty")
        elif (problem := shape_problem(text)) is not None:
            problems.append(f"{path.name}: {problem}")
        else:
            by_kind[kind].append((issue, path.name, path, text))
    if problems:
        raise FragmentError("\n".join(problems))
    for entries in by_kind.values():
        entries.sort(key=lambda entry: (entry[0], entry[1]))
    return by_kind


def shape_problem(text: str) -> str | None:
    """Why *text* is not one entry written as prose, or None.

    The fold makes the entry a list item, so a fragment that already carries
    the marker would nest one list inside another, and a heading inside a list
    item ends the item and orphans everything after it. Both mean the file is
    a slice of a changelog rather than one entry, which is what the ``<kind>``
    in its name promises. A ``- `` further down is a nested list within the
    entry and is fine.
    """
    if text.startswith("- "):
        return "starts with a list marker; write the entry as prose, the fold adds it"
    for number, line in enumerate(text.splitlines(), start=1):
        if line.startswith("#"):
            return (
                f"line {number} is a Markdown heading; one fragment is one entry, "
                "and its section comes from the file name"
            )
    return None


def as_bullet(text: str) -> list[str]:
    """One fragment as the lines of a Markdown list item."""
    lines = text.splitlines()
    return ["- " + lines[0]] + [("  " + line) if line.strip() else "" for line in lines[1:]]


def _append(body: list[str], bullets: list[list[str]]) -> None:
    while body and not body[-1].strip():
        body.pop()
    for bullet in bullets:
        body.append("")
        body.extend(bullet)
    body.append("")


def _insert_position(subsections: list[tuple[str | None, list[str]]], kind: str) -> int:
    position = 1
    for index, (heading, _body) in enumerate(subsections):
        other = KIND_OF_HEADING.get(heading or "")
        if heading is not None and (other is None or KINDS.index(other) < KINDS.index(kind)):
            position = index + 1
    return position


def fold(changelog: str, by_kind: dict[str, list[Fragment]]) -> str:
    """Return *changelog* with every fragment placed under ``[Unreleased]``."""
    lines = changelog.splitlines()
    try:
        start = lines.index(UNRELEASED)
    except ValueError:
        raise FragmentError(f"the changelog has no {UNRELEASED!r} section") from None
    end = next(
        (index for index in range(start + 1, len(lines)) if lines[index].startswith("## ")),
        len(lines),
    )
    subsections: list[tuple[str | None, list[str]]] = [(None, [])]
    for line in lines[start + 1:end]:
        if line.startswith("### "):
            subsections.append((line, []))
        else:
            subsections[-1][1].append(line)
    for kind in KINDS:
        entries = by_kind.get(kind) or []
        if not entries:
            continue
        bullets = [as_bullet(text) for _issue, _name, _path, text in entries]
        for heading, body in subsections:
            if heading == HEADINGS[kind]:
                _append(body, bullets)
                break
        else:
            body = [""]
            _append(body, bullets)
            subsections.insert(_insert_position(subsections, kind), (HEADINGS[kind], body))
    rebuilt: list[str] = []
    for heading, body in subsections:
        if heading is not None:
            rebuilt.append(heading)
        rebuilt.extend(body)
    folded = "\n".join(lines[: start + 1] + rebuilt + lines[end:])
    return folded + "\n" if changelog.endswith("\n") else folded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--check", action="store_true",
        help="only validate the fragments' names and contents; write nothing",
    )
    parser.add_argument("--changelog", type=Path, default=ROOT / "CHANGELOG.md")
    parser.add_argument("--fragments", type=Path, default=ROOT / "changelog.d")
    args = parser.parse_args(argv)
    try:
        by_kind = load_fragments(args.fragments)
    except FragmentError as exc:
        print(f"changelog fragments:\n{exc}", file=sys.stderr)
        return 1
    count = sum(len(entries) for entries in by_kind.values())
    if args.check:
        print(f"{count} changelog fragment(s), all well-formed")
        return 0
    if not count:
        print("no changelog fragments to fold")
        return 0
    try:
        folded = fold(args.changelog.read_text(encoding="utf-8"), by_kind)
    except FragmentError as exc:
        print(f"{args.changelog}: {exc}", file=sys.stderr)
        return 1
    args.changelog.write_text(folded, encoding="utf-8")
    for entries in by_kind.values():
        for _issue, _name, path, _text in entries:
            path.unlink()
    print(f"folded {count} changelog fragment(s) into {args.changelog} under {UNRELEASED}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
