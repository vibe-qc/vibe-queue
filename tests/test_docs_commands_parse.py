"""Every `vq ...` command shown in the docs parses against the real CLI.

Each shell block's `vq` lines are parsed with Click, as far as the command's
own options and arguments, without running anything. Placeholders such as
HOST and <jobid> become sample values. A path argument that does not exist
here is fine, since the docs name files on the reader's machine; any other
usage error means an example would fail when copied.
"""
from __future__ import annotations

import re
import shlex
from pathlib import Path

import click
import pytest

from vq.cli import main

ROOT = Path(__file__).resolve().parents[1]
# Design notes and history describe past or planned interfaces on purpose.
SKIP = ("SPEC", "history", "design", "v0_7")
PLACEHOLDERS = [
    (r"<jobid>|<scf-jobid>|<id>|\bJOBID\b|\$(A|B|C|last|j|id|JOB)\b", "abc123def456"),
    (r"\bGID\b", "376a5f2a"),
    (r"\$deps", "--depends-on abc123def456"),
    (r"\$\{=ids\}|\$ids", "abc123def456"),
    (r"\bHOST\b", "localhost"),
    (r"<[A-Za-z_-]+>", "x"),
    (r"\$\(.*?\)", "x"),
]


def _examples() -> list[tuple[str, str]]:
    found = []
    files = sorted((ROOT / "docs").glob("**/*.md")) + [ROOT / "README.md"]
    for path in files:
        if any(part in path.name for part in SKIP):
            continue
        text = path.read_text(encoding="utf-8")
        for block in re.findall(r"```(?:sh|bash|console|shell)?\n(.*?)```", text, re.S):
            for line in re.sub(r"\\\n\s*", " ", block).splitlines():
                line = line.split("  #")[0].strip().removeprefix("$ ")
                if line.startswith("vq "):
                    found.append((str(path.relative_to(ROOT)), line))
    return found


def _argv(line: str) -> list[str] | None:
    for pattern, value in PLACEHOLDERS:
        line = re.sub(pattern, value, line)
    line = re.split(r" \| | > | && | 2>", line)[0]
    try:
        argv = shlex.split(line)[1:]
    except ValueError:
        return None
    return argv if argv and argv[0] in main.commands else None


def _rest(ctx: click.Context) -> list[str]:
    return [*getattr(ctx, "_protected_args", []), *ctx.args]


def _parse(argv: list[str]) -> None:
    ctx = main.make_context("vq", list(argv))
    name, cmd, rest = main.resolve_command(ctx, _rest(ctx))
    assert cmd is not None and name is not None
    sub = cmd.make_context(name, rest, parent=ctx)
    while isinstance(cmd, click.Group) and _rest(sub):
        name, cmd, rest = cmd.resolve_command(sub, _rest(sub))
        assert cmd is not None and name is not None
        sub = cmd.make_context(name, rest, parent=sub)


def test_the_docs_show_commands() -> None:
    assert len([e for e in _examples() if _argv(e[1]) is not None]) > 100


@pytest.mark.parametrize(("path", "line"), _examples())
def test_a_documented_command_parses(path: str, line: str) -> None:
    argv = _argv(line)
    if argv is None:
        pytest.skip("not a single vq command line")
    try:
        _parse(argv)
    except click.exceptions.Exit:
        pass
    except click.ClickException as exc:
        message = exc.format_message()
        if "does not exist" in message:
            return
        pytest.fail(f"{path}: `{line}` does not parse: {message}")
