"""The vibe-qc.com pages make claims about vq; the code is the authority.

``docs/vibe-qc-site/`` holds the canonical copy of pages that vibe-qc copies
into ``mpei/vibe-qc`` (vibe-queue#39). They are excluded from this project's
Sphinx build, so no docs build touches them and nothing else here would notice
them drifting from the code they describe.

The pages transcribe three things vq owns: the job lifecycle, the command
tree, and the configuration schema. A transcribed contract is a copy, and a
copy rots. The original vibe-queue#39 report was mostly about the second and
third -- verbs, flags and described behaviours that did not exist -- while the
guard only ever checked the first. This module checks all three.

What it cannot check is prose about behaviour: whether ``--expected-sha``
really refuses a moved checkout is a claim only a reader can verify against
the code. Those rows in ``docs/vibe-qc-site/README.md``'s "Keeping it current"
table stay prose against prose.

Adapted from ``tests/test_admin_outcomes.py``'s documented-table check, which
pins ``docs/orchestration.md`` against ``admin.ADMIN_OUTCOMES``.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import click
import pytest

from vq.cli import main as vq_cli
from vq.config import BinaryProgram, Config, HostConfig, ImportProgram, VenvProgram
from vq.spec import TERMINAL_STATES, JobState

_ROOT = Path(__file__).resolve().parent.parent
_SITE = _ROOT / "docs" / "vibe-qc-site"
_CONTRIB = _ROOT / "contrib"
_TUTORIAL = _SITE / "tutorial" / "vq_queue_remote_job.md"
_QUEUE = _SITE / "user_guide" / "queue.md"

#: The pages vibe-qc copies verbatim, and the only ones whose vq syntax is
#: checked. ``README.md`` is the handoff contract and deliberately quotes
#: syntax that does *not* exist ("``vq throttle --max-jobs 1`` does not
#: exist"); ``toolset_lifecycle-vq-edits.md`` is a record of another
#: repository's page and quotes its "before" text. Scanning either would fail
#: on prose that is correct precisely because it names something wrong.
#: The job-state checks below still read every page, because naming a state
#: that vq does not produce is wrong even in a record.
_PUBLISHED = (_QUEUE, _TUTORIAL)

_ALL_STATES = {state.value for state in JobState}
_TERMINAL = {state.value for state in TERMINAL_STATES}

_FENCE = re.compile(r"^\s*`{3,}(\S*)")
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+(?=[A-Z`*(])")
_BARE_NAME = re.compile(r"`([a-z][a-z_]*)`")
_MENTIONS_A_STATE = re.compile(r"\bstates?\b")

#: An inline code span. The pages' claims about syntax are always inside one
#: or inside a fence; unbackticked prose ("vq delegates over SSH") is English,
#: not a command, and is not read as one.
_INLINE_CODE = re.compile(r"`([^`\n]+)`")

#: A long or short option. ``--`` alone is not an option: the pages write
#: "``--``-form argument pass-through", meaning the separator.
_OPTION = re.compile(r"(?<![\w-])(--[a-z][a-z0-9-]*|-[a-z])(?![\w-])")

#: A leading ``NAME=value`` environment prefix or ``NAME=$(`` capture, which
#: shell allows before the command word.
_SHELL_PREFIX = re.compile(
    r"^(?:[A-Za-z_][A-Za-z0-9_]*=(?:\$\()?\S*\s+|[A-Za-z_][A-Za-z0-9_]*=\$\()"
)

#: Options the pages name that belong to a tool this repository does not
#: ship, so nothing here can check them. Keep this list short and say which
#: tool owns each entry: an unexplained entry is how a guard stops guarding.
#: The ``contrib/`` wrappers are deliberately *not* here -- their options are
#: parsed from the scripts, so the pages are held to them.
#:
#: Every inline span is read, including one that names another command, so
#: these have to be declared rather than skipped. Filtering to spans that
#: *look* like a bare vq option was the first version of that rule and it was
#: wrong in the familiar way: ``capture --section ID`` never reached the
#: check, so removing this entry changed nothing and the exemption was not
#: load-bearing. A guard whose escape hatch is unreachable is not a guard.
_FOREIGN_OPTIONS = {
    "--section": "vibe-view capture, in the vibe-view repository",
    "-m": "python, in `python -m vibeqc._cli run`",
    "-a": "xvfb-run, in `xvfb-run -a`",
}


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------


def _walk_fences(text: str):
    """Yield ``(innermost_info, line)`` for every line of a page.

    ``innermost_info`` is ``None`` outside any fence, otherwise the info
    string of the innermost open fence (``"sh"``, ``"toml"``, ``"{note}"``).
    The fences are walked line by line with a stack: an opening fence carries
    an info string and a closing one does not. A regex that matched fence
    *pairs* loses step at a directive's bare closing fence and then pairs the
    wrong delimiters, which is why both the prose and the code extraction
    here share this walk.
    """
    stack: list[str] = []
    for line in text.splitlines():
        fence = _FENCE.match(line)
        if fence:
            if fence.group(1):
                stack.append(fence.group(1))
            elif stack:
                stack.pop()
            else:
                stack.append("")
            continue
        yield (stack[-1] if stack else None), line


def _prose_sentences(text: str) -> list[str]:
    """The page's prose, one whitespace-normalised sentence per entry.

    Code fences are dropped, because a Python or shell block's identifiers are
    not state names. MyST directives such as ``{note}`` are kept, because their
    body is prose.
    """
    kept = [
        line
        for info, line in _walk_fences(text)
        if info is None or info.startswith("{")
    ]
    return _SENTENCE_BREAK.split(" ".join(" ".join(kept).split()))


def _fenced_lines(text: str, info: str) -> list[str]:
    """Every line inside a fence whose info string is ``info``."""
    return [line for got, line in _walk_fences(text) if got == info]


def _fenced_blocks(text: str, info: str) -> list[str]:
    """Each ``info`` fence's body, as one string per fence."""
    blocks: list[str] = []
    current: list[str] | None = None
    for got, line in _walk_fences(text):
        if got == info:
            if current is None:
                current = []
            current.append(line)
        elif current is not None:
            blocks.append("\n".join(current))
            current = None
    if current is not None:
        blocks.append("\n".join(current))
    return blocks


def _command_lines(text: str) -> list[str]:
    """Every shell command line on the page whose command word is ``vq``.

    Two sources, and the difference matters:

    * every line of a ``sh`` fence, because a fence is code throughout;
    * only *indented* lines inside a MyST directive, because a directive's
      body is prose with an indented code block in it. Without the indent
      rule, prose that happens to begin with the word "vq" ("vq reads
      ``~/.config/vq/config.toml`` on the laptop") would be parsed as a
      command and would claim a verb named ``reads``.

    Backslash continuations are joined, a trailing ``#`` comment is dropped,
    and a leading ``NAME=value`` or ``JOBID=$(`` shell prefix is stripped so
    the command word is reached.
    """
    raw: list[str] = []
    for info, line in _walk_fences(text):
        in_shell_fence = info == "sh"
        in_directive_code = (
            info is not None and info.startswith("{") and line.startswith("    ")
        )
        if in_shell_fence or in_directive_code:
            raw.append(line)

    joined: list[str] = []
    pending = ""
    for line in raw:
        stripped = line.strip()
        if pending:
            stripped = f"{pending} {stripped}"
            pending = ""
        if stripped.endswith("\\"):
            pending = stripped[:-1].strip()
            continue
        joined.append(stripped)
    if pending:
        joined.append(pending)

    commands = []
    for line in joined:
        line = line.split("#", 1)[0].strip()
        while (match := _SHELL_PREFIX.match(line)) is not None:
            line = line[match.end() :].strip()
        if line.split()[:1] == ["vq"]:
            commands.append(line)
    return commands


def _split_at_separator(words: list[str]) -> tuple[list[str], list[str]]:
    """Split a vq command line at ``--``.

    Everything before belongs to vq; everything after is the literal command
    vq will run, so ``-- bash run-crystal.sh --np 14`` says nothing about vq's
    own options. This is vq's contract, not a convenience: ``--dir`` submits
    require an explicit command after ``--``.
    """
    if "--" in words:
        cut = words.index("--")
        return words[:cut], words[cut + 1 :]
    return words, []


def _resolve_verb(words: list[str]) -> tuple[str, click.Command | None]:
    """Resolve the longest ``vq verb [subverb]`` prefix of ``words``.

    Returns the path as written and the command it resolves to, or ``None``
    when it resolves to nothing. A second word is only consulted when the
    first resolved to a ``Group``; otherwise it is a positional (a host, a
    job id, a filename) and says nothing about the command tree.
    """
    cmd: click.Command = vq_cli
    path = ["vq"]
    for word in words[1:]:
        if not isinstance(cmd, click.Group):
            break
        if word.startswith("-"):
            break
        found = cmd.get_command(None, word)  # type: ignore[arg-type]
        path.append(word)
        if found is None:
            return " ".join(path), None
        cmd = found
        if not isinstance(cmd, click.Group):
            break
    if len(path) == 1:
        return "vq", vq_cli
    return " ".join(path), cmd


def _options_of(cmd: click.Command) -> set[str]:
    opts: set[str] = set()
    for param in cmd.params:
        if isinstance(param, click.Option):
            opts.update(param.opts)
            opts.update(param.secondary_opts)
    return opts


def _all_vq_options() -> set[str]:
    opts: set[str] = set()
    stack: list[click.Command] = [vq_cli]
    while stack:
        cmd = stack.pop()
        opts |= _options_of(cmd)
        if isinstance(cmd, click.Group):
            stack.extend(cmd.commands.values())
    return opts


_ALL_VQ_OPTIONS = _all_vq_options()

#: ``--flag)`` or ``--flag|-x)`` case arms in a ``contrib/`` wrapper.
_WRAPPER_ARM = re.compile(r"^\s*(--[a-z][a-z0-9-]*)[)|]", re.M)


def _wrapper_options() -> dict[str, set[str]]:
    """Each shipped wrapper's options, read from its own argument parser."""
    found = {}
    for script in sorted(_CONTRIB.glob("run-*.sh")):
        found[script.name] = set(_WRAPPER_ARM.findall(script.read_text()))
    return found


_WRAPPER_OPTIONS = _wrapper_options()


def _inline_code(text: str) -> list[str]:
    """Every inline code span in the page's prose, fenced blocks excluded."""
    spans: list[str] = []
    for info, line in _walk_fences(text):
        if info is not None and not info.startswith("{"):
            continue
        spans.extend(_INLINE_CODE.findall(line))
    return spans


# --------------------------------------------------------------------------
# Job states: the checks this file has carried since 4328c81 / b5953c2 / !14
# --------------------------------------------------------------------------


def _names_in_state_sentences(text: str) -> set[str]:
    """Every backticked bare identifier in a sentence that mentions a state.

    Deliberately not filtered by the enum. The first version of this check
    kept only names already in ``JobState`` and then asserted that they were
    in ``JobState``, which cannot fail: a renamed state dropped out of the
    result instead of failing the test (vibe-queue#39). The sentence is the
    filter instead, so a name the enum does not have is still collected.
    """
    return {
        name
        for sentence in _prose_sentences(text)
        if _MENTIONS_A_STATE.search(sentence)
        for name in _BARE_NAME.findall(sentence)
    }


def _state_mentions(text: str) -> dict[str, bool]:
    """Every current state the prose names, and whether the guard reads it."""
    mentions: dict[str, bool] = {}
    for sentence in _prose_sentences(text):
        readable = bool(_MENTIONS_A_STATE.search(sentence))
        for name in _BARE_NAME.findall(sentence):
            if name in _ALL_STATES:
                mentions[name] = mentions.get(name, False) or readable
    return mentions


def _lifecycle_block() -> str:
    """The tutorial's ``pending -> running -> ...`` fenced block."""
    blocks = [
        b
        for b in _fenced_blocks(_TUTORIAL.read_text(), "text")
        if "pending" in b and "->" in b
    ]
    assert len(blocks) == 1, (
        f"expected exactly one lifecycle block in {_TUTORIAL.name}, "
        f"found {len(blocks)}. Update this test if the page grew a second."
    )
    return blocks[0]


def _block_outcomes(block: str) -> set[str]:
    """The state each arrow chain ends at: the token after the final ``->``.

    Derived from the block's own structure rather than from a hand-written
    list of the states that are *not* outcomes. Reading the column also makes
    the check sharper than a whole-block scan: a state demoted out of
    ``TERMINAL_STATES`` but left in the outcome column still exists in
    ``JobState``, so the existence check passes it; this sees it in the wrong
    column and fails.
    """
    outcomes: set[str] = set()
    for line in block.splitlines():
        if "->" not in line:
            continue
        tail = line.rsplit("->", 1)[1].split()
        if tail:
            outcomes.add(tail[0])
    return outcomes


def _queue_terminal_list() -> set[str]:
    """The names in ``queue.md``'s "... are all terminal" sentence."""
    sentences = [
        s for s in _prose_sentences(_QUEUE.read_text()) if "are all terminal" in s
    ]
    assert len(sentences) == 1, (
        f"expected exactly one 'are all terminal' sentence in {_QUEUE.name}, "
        f"found {len(sentences)}. Update this test if the note was reworded."
    )
    return set(_BARE_NAME.findall(sentences[0]))


@pytest.mark.parametrize("page", sorted(_SITE.rglob("*.md")), ids=lambda p: str(p.name))
def test_no_page_names_a_state_that_does_not_exist(page: Path):
    """Catches a rename or a removal anywhere in the prose.

    This is the direction that actively misleads: a reader acts on a state the
    code no longer produces. Pages may legitimately name a subset, so this
    asserts nothing about completeness.
    """
    named = _names_in_state_sentences(page.read_text())
    assert named <= _ALL_STATES, (
        f"{page.name} names job states that are not in vq.spec.JobState: "
        f"{sorted(named - _ALL_STATES)}"
    )


def test_the_tutorials_lifecycle_block_lists_every_terminal_state():
    """Set equality, both directions.

    "Every documented state exists" would not have caught the bug this test was
    written for: the block omitted ``starved``, so a reader handling every
    outcome it showed still missed one vq produces.
    """
    documented_terminal = _block_outcomes(_lifecycle_block())
    assert documented_terminal == _TERMINAL, (
        "the tutorial's lifecycle block and vq.spec.TERMINAL_STATES disagree.\n"
        f"  documented but not terminal: {sorted(documented_terminal - _TERMINAL)}\n"
        f"  terminal but undocumented  : {sorted(_TERMINAL - documented_terminal)}"
    )


def test_queue_mds_terminal_list_names_every_terminal_state_but_success():
    """Set equality against the terminal states other than ``COMPLETED``.

    The note says that ``vq wait`` returning is not success, so it lists the
    terminal states a script has to treat as not having succeeded. Success is
    named through the enum rather than as a string, so a renamed ``COMPLETED``
    follows the code.
    """
    listed = _queue_terminal_list()
    expected = _TERMINAL - {JobState.COMPLETED.value}
    assert listed == expected, (
        "queue.md's 'are all terminal' list and vq.spec.TERMINAL_STATES "
        "(without completed) disagree.\n"
        f"  listed but not expected: {sorted(listed - expected)}\n"
        f"  expected but not listed: {sorted(expected - listed)}"
    )


def test_every_state_the_pages_name_is_pinned_by_some_check():
    """No state may be named where nothing would notice it going stale.

    The existence check reads only sentences that mention a state, and the two
    set-equality checks read only the lifecycle block and ``queue.md``'s
    terminal list. A *non-terminal* state named in a sentence that never says
    "state" is therefore invisible to all three, and a later rename would
    change nothing here. This closes that hole (vibe-queue#39, MR !14).
    """
    pinned = _block_outcomes(_lifecycle_block()) | _queue_terminal_list()
    unpinned: dict[str, list[str]] = {}
    for page in sorted(_SITE.rglob("*.md")):
        for name, readable in _state_mentions(page.read_text()).items():
            if readable or name in pinned:
                continue
            unpinned.setdefault(name, []).append(page.name)

    assert not unpinned, (
        "these job states are named in prose that no check reads, so a "
        "rename of them would not fail any test here:\n"
        + "\n".join(
            f"  {name}: {', '.join(sorted(pages))}"
            for name, pages in sorted(unpinned.items())
        )
        + "\n\nEither reword the sentence so it mentions a state, or name "
        "the state in the tutorial's lifecycle block or queue.md's terminal "
        "list, whichever is true of it."
    )


# --------------------------------------------------------------------------
# The command tree
# --------------------------------------------------------------------------


@pytest.mark.parametrize("page", _PUBLISHED, ids=lambda p: p.name)
def test_every_vq_verb_the_pages_name_exists(page: Path):
    """Every ``vq <verb> [<subverb>]`` on a published page is a real command.

    Read from the Click tree by introspection rather than by grepping the
    source, so a verb that is defined but not registered under ``main``, or
    registered under a different name than its function, is judged by what a
    reader would actually be able to type.
    """
    text = page.read_text()
    named: dict[str, click.Command | None] = {}
    for line in _command_lines(text):
        vq_words, _ = _split_at_separator(line.split())
        path, cmd = _resolve_verb(vq_words)
        named[path] = cmd
    for span in _inline_code(text):
        words = span.split()
        if words[:1] != ["vq"] or len(words) < 2:
            continue
        path, cmd = _resolve_verb(_split_at_separator(words)[0])
        named.setdefault(path, cmd)

    missing = sorted(path for path, cmd in named.items() if cmd is None)
    assert not missing, (
        f"{page.name} names vq commands that are not in the Click tree under "
        f"vq.cli.main: {missing}"
    )


@pytest.mark.parametrize("page", _PUBLISHED, ids=lambda p: p.name)
def test_every_flag_on_a_vq_command_line_belongs_to_that_command(page: Path):
    """An option shown on a ``vq`` command line must be that command's own.

    Checking against the whole tree instead would be far weaker, and wrongly
    passing: ``--serial`` exists on ``vq admin update``, so ``vq submit
    --serial`` would look fine. The command the line names is the one that has
    to accept it.
    """
    wrong: list[str] = []
    for line in _command_lines(page.read_text()):
        vq_words, _ = _split_at_separator(line.split())
        path, cmd = _resolve_verb(vq_words)
        if cmd is None:
            continue  # reported by the verb test
        allowed = _options_of(cmd) | _options_of(vq_cli)
        for word in vq_words:
            for opt in _OPTION.findall(word):
                if opt not in allowed:
                    wrong.append(f"{opt} on `{line}` ({path} has no such option)")
    assert not wrong, (
        f"{page.name} shows options the command does not accept:\n  "
        + "\n  ".join(sorted(set(wrong)))
    )


@pytest.mark.parametrize("page", _PUBLISHED, ids=lambda p: p.name)
def test_every_flag_the_prose_names_is_a_real_flag(page: Path):
    """A backticked option in prose must exist somewhere it can be typed.

    Prose names an option without naming a command ("It is disabled inside
    ``--array`` and ``--chain``"), so this is the weaker whole-tree check;
    the per-command one above covers the command lines. An option belonging
    to a shipped ``contrib/`` wrapper counts, because the pages document
    those and this repository ships them. Anything else must be declared in
    :data:`_FOREIGN_OPTIONS` with the tool that owns it.

    Every inline span is read, including one whose first word names another
    command. Skipping those would be tidier and would put a hole exactly
    where the pages talk about other tools.
    """
    wrapper_opts = set().union(*_WRAPPER_OPTIONS.values()) if _WRAPPER_OPTIONS else set()
    known = _ALL_VQ_OPTIONS | wrapper_opts | set(_FOREIGN_OPTIONS)
    unknown = set()
    for span in _inline_code(page.read_text()):
        unknown.update(opt for opt in _OPTION.findall(span) if opt not in known)
    assert not unknown, (
        f"{page.name} names options that are neither vq's, nor a contrib "
        f"wrapper's, nor declared foreign: {sorted(unknown)}. If one belongs "
        "to another tool, add it to _FOREIGN_OPTIONS with the tool's name."
    )


@pytest.mark.parametrize("page", _PUBLISHED, ids=lambda p: p.name)
def test_every_wrapper_flag_the_pages_show_exists_in_contrib(page: Path):
    """The options shown after ``--`` for a shipped wrapper are that
    wrapper's own.

    ``contrib/run-crystal.sh`` and ``contrib/run-orca.sh`` are in this
    repository, so the half of the "Keeping it current" row that is syntax
    can be machine-checked rather than left as prose against prose. Only
    flags that are present are checked; this does not detect a required
    ``--np`` omitted after the wrapper drops its default rank count.
    """
    wrong: list[str] = []
    for line in _command_lines(page.read_text()):
        _, payload = _split_at_separator(line.split())
        script = next(
            (
                Path(word).name
                for word in payload
                if Path(word).name in _WRAPPER_OPTIONS
            ),
            None,
        )
        if script is None:
            continue
        for word in payload:
            for opt in _OPTION.findall(word):
                if opt not in _WRAPPER_OPTIONS[script]:
                    wrong.append(f"{opt} passed to {script} on `{line}`")
    assert not wrong, (
        f"{page.name} passes options a contrib wrapper does not parse:\n  "
        + "\n  ".join(sorted(set(wrong)))
    )


# --------------------------------------------------------------------------
# The configuration schema
# --------------------------------------------------------------------------

_PROGRAM_MODELS = {
    "venv": VenvProgram,
    "binary": BinaryProgram,
    "import": ImportProgram,
}


@pytest.mark.parametrize("page", _PUBLISHED, ids=lambda p: p.name)
def test_every_config_key_the_pages_show_is_a_real_field(page: Path):
    """Every key in a ``toml`` fence is a field of the model that owns it.

    Checked only inside ``toml`` fences, on purpose. The prose also writes
    bracketed names that are *not* vq configuration -- ``[outputs].status``
    and ``[plan]`` are sections of vibe-qc's ``.system`` manifest -- so a
    scan of bracketed names in prose would fail on correct text. Every table
    the prose refers to (``[hosts.X.branches]``, ``[programs.X]``) appears in
    a fence too, so nothing is lost.

    Free-form mappings such as ``[hosts.X.branches]`` are not descended into:
    their keys are branch names the operator chooses, not schema.
    """
    bad: list[str] = []
    for block in _fenced_blocks(page.read_text(), "toml"):
        data = tomllib.loads(block)
        for key in data:
            if key in ("hosts", "programs"):
                continue
            if key not in Config.model_fields:
                bad.append(f"top-level `{key}` is not a vq.config.Config field")
        for name, host in data.get("hosts", {}).items():
            for key in host:
                if key not in HostConfig.model_fields:
                    bad.append(
                        f"[hosts.{name}] `{key}` is not a vq.config.HostConfig field"
                    )
        for name, program in data.get("programs", {}).items():
            kind = program.get("kind")
            model = _PROGRAM_MODELS.get(kind)
            if model is None:
                bad.append(
                    f"[programs.{name}] kind = {kind!r} is not one of "
                    f"{sorted(_PROGRAM_MODELS)}"
                )
                continue
            for key in program:
                if key not in model.model_fields:
                    bad.append(
                        f"[programs.{name}] `{key}` is not a "
                        f"vq.config.{model.__name__} field"
                    )
    assert not bad, f"{page.name} shows configuration keys vq does not read:\n  " + (
        "\n  ".join(bad)
    )


# --------------------------------------------------------------------------
# The handoff note
# --------------------------------------------------------------------------


def test_the_handoff_note_still_points_at_the_page_this_test_guards():
    """The "Keeping it current" table is what tells the next person where to
    look when the code changes. A guard on the page plus a stale pointer to it
    is a quieter failure than the drift itself, because nobody goes looking.
    """
    note = (_SITE / "README.md").read_text()

    # Every string below is derived, because this checks a document against
    # the code rather than pinning the code. A literal "vq.spec.JobState"
    # here would survive the enum being renamed or moved: the note would be
    # wrong and the assertion would still pass.
    symbol = f"{JobState.__module__}.{JobState.__qualname__}"
    assert symbol in note, (
        f"docs/vibe-qc-site/README.md no longer names {symbol} in its "
        "'Keeping it current' table; that row is how a change to the enum "
        "finds its way to the tutorial."
    )
    assert _TUTORIAL.name in note, (
        f"the 'Keeping it current' row no longer names {_TUTORIAL.name}, the "
        "page it governs."
    )
    assert Path(__file__).name in note, (
        f"the 'Keeping it current' row no longer names {Path(__file__).name}, "
        "so the note still claims the row is only a pointer while this guard "
        "exists under another name."
    )
