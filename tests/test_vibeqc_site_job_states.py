"""The vibe-qc.com pages name job states; the enum is the authority.

``docs/vibe-qc-site/`` holds the canonical copy of pages that vibe-qc copies
into ``mpei/vibe-qc`` (vibe-queue#39). They are excluded from this project's
Sphinx build, so no docs build touches them and nothing else here would notice
them drifting from the code they describe.

The pages transcribe the job lifecycle into prose. A transcribed enum is a
copy, and a copy rots: a renamed or removed state keeps reading as current, and
a reader writes a script against a state vq no longer produces.

Adapted from ``tests/test_admin_outcomes.py``'s documented-table check, which
pins ``docs/orchestration.md`` against ``admin.ADMIN_OUTCOMES``. That page
carries a Markdown table; these carry a fenced block and a sentence, so the
extraction differs while the rules do not.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from vq.spec import TERMINAL_STATES, JobState

_SITE = Path(__file__).resolve().parent.parent / "docs" / "vibe-qc-site"
_TUTORIAL = _SITE / "tutorial" / "vq_queue_remote_job.md"
_QUEUE = _SITE / "user_guide" / "queue.md"

_ALL_STATES = {state.value for state in JobState}
_TERMINAL = {state.value for state in TERMINAL_STATES}

_FENCE = re.compile(r"^\s*`{3,}(\S*)")
_SENTENCE_BREAK = re.compile(r"(?<=[.!?])\s+(?=[A-Z`*(])")
_BARE_NAME = re.compile(r"`([a-z][a-z_]*)`")
_MENTIONS_A_STATE = re.compile(r"\bstates?\b")


def _prose_sentences(text: str) -> list[str]:
    """The page's prose, one whitespace-normalised sentence per entry.

    Code fences are dropped, because a Python or shell block's identifiers are
    not state names. MyST directives such as ``{note}`` are kept, because their
    body is prose. The fences are walked line by line with a stack: an opening
    fence carries an info string and a closing one does not, and a regex that
    matched fence pairs lost step at a directive's bare closing fence, then
    swallowed the prose up to the next code block.
    """
    kept: list[str] = []
    stack: list[bool] = []  # True for a directive, False for code
    for line in text.splitlines():
        fence = _FENCE.match(line)
        if fence:
            if fence.group(1):
                stack.append(fence.group(1).startswith("{"))
            elif stack:
                stack.pop()
            else:
                stack.append(False)
            continue
        if all(stack):
            kept.append(line)
    return _SENTENCE_BREAK.split(" ".join(" ".join(kept).split()))


def _names_in_state_sentences(text: str) -> set[str]:
    """Every backticked bare identifier in a sentence that mentions a state.

    Deliberately not filtered by the enum. The first version of this file kept
    only names already in ``JobState`` and then asserted that they were in
    ``JobState``, which cannot fail: a renamed state dropped out of the result
    instead of failing the test (vibe-queue#39). The sentence is the filter
    instead, so a name the enum does not have is still collected. A sentence
    about states that backticks some other bare identifier will fail here too;
    reword it or write the identifier with its command, as in ``vq pause``.
    """
    return {
        name
        for sentence in _prose_sentences(text)
        if _MENTIONS_A_STATE.search(sentence)
        for name in _BARE_NAME.findall(sentence)
    }


def _state_mentions(text: str) -> dict[str, bool]:
    """Every current state the prose names, and whether the guard reads it.

    The value is True when at least one sentence naming that state also
    mentions a state, which is the filter
    :func:`_names_in_state_sentences` applies. A state named only in
    sentences that do not is invisible to the existence check.
    """
    mentions: dict[str, bool] = {}
    for sentence in _prose_sentences(text):
        readable = bool(_MENTIONS_A_STATE.search(sentence))
        for name in _BARE_NAME.findall(sentence):
            if name in _ALL_STATES:
                mentions[name] = mentions.get(name, False) or readable
    return mentions


def _lifecycle_block() -> str:
    """The tutorial's ``pending -> running -> ...`` fenced block.

    Anchored on the transition arrow rather than on a heading or an offset, so
    the block can move within the page.
    """
    blocks = re.findall(r"```text\n(.*?)```", _TUTORIAL.read_text(), re.S)
    matching = [b for b in blocks if "pending" in b and "->" in b]
    assert len(matching) == 1, (
        f"expected exactly one lifecycle block in {_TUTORIAL.name}, "
        f"found {len(matching)}. Update this test if the page grew a second."
    )
    return matching[0]


def _block_outcomes(block: str) -> set[str]:
    """The state each arrow chain ends at: the token after the final ``->``.

    Derived from the block's own structure rather than from a hand-written
    list of the states that are *not* outcomes. An earlier version subtracted
    ``{PENDING, RUNNING}`` to get here, which is the same defect this file
    exists to catch, one level up: a partial enumeration of the enum written
    out by hand, in a test, where it would go stale the first time the
    lifecycle gained another pre-running state.

    Reading the column also makes the check sharper than a whole-block scan.
    A state demoted out of ``TERMINAL_STATES`` but left in the outcome column
    still exists in ``JobState``, so the existence check passes it; this sees
    it in the wrong column and fails.
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
    """The names in ``queue.md``'s "... are all terminal" sentence.

    Anchored on the sentence's own claim rather than on a heading or on the
    names it lists, so the note can move and be reworded around it.
    """
    sentences = [
        s for s in _prose_sentences(_QUEUE.read_text()) if "are all terminal" in s
    ]
    assert len(sentences) == 1, (
        f"expected exactly one 'are all terminal' sentence in {_QUEUE.name}, "
        f"found {len(sentences)}. Update this test if the note was reworded."
    )
    return set(_BARE_NAME.findall(sentences[0]))


@pytest.mark.parametrize(
    "page", sorted(_SITE.rglob("*.md")), ids=lambda p: str(p.name)
)
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
    outcome it showed still missed one vq produces. The other direction matters
    just as much, and catches a removed state left behind in the prose.
    """
    documented_terminal = _block_outcomes(_lifecycle_block())
    assert documented_terminal == _TERMINAL, (
        "the tutorial's lifecycle block and vq.spec.TERMINAL_STATES disagree.\n"
        f"  documented but not terminal: {sorted(documented_terminal - _TERMINAL)}\n"
        f"  terminal but undocumented  : {sorted(_TERMINAL - documented_terminal)}"
    )


def test_queue_mds_terminal_list_names_every_terminal_state_but_success():
    """Set equality, both directions, against the terminal states other than
    ``COMPLETED``.

    The note says that ``vq wait`` returning is not success, so it lists the
    terminal states a script has to treat as not having succeeded. It omitted
    ``starved`` and ``interrupted`` until ``4328c81``, and vibe-qc#242 is the
    same omission on the published page. Success is named through the enum
    rather than as a string, so a renamed ``COMPLETED`` follows the code; a
    second successful terminal state, if vq ever gains one, fails here until
    someone decides which list it belongs in.
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

    The three checks in this file cover different ground.
    :func:`test_no_page_names_a_state_that_does_not_exist` reads only
    sentences that mention a state, because a sentence is the only filter
    that does not reduce to "names in the enum are in the enum". The two
    set-equality checks cover the lifecycle block and ``queue.md``'s
    terminal list, and between them every **terminal** state.

    That leaves a hole this test closes. A state named in a sentence that
    never says "state" is invisible to the existence check, and a
    *non-terminal* state -- ``pending``, ``running``, ``suspended``,
    ``submitting``, ``submit_outcome_unknown`` -- appears in neither of the
    set-equality checks. Write "the job goes ``suspended`` while the host is
    under pressure" and a later rename of that state changes nothing here:
    every test still passes and the page still reads as current.

    Verified against the pages as of this commit: all 13 states they name are
    covered, but three mentions (``killed`` and ``starved`` in ``README.md``,
    ``completed`` in the tutorial) already sit outside a readable sentence and
    survive only because those states are terminal and pinned elsewhere.

    Widening the existence check instead is not free, which is why this is a
    coverage check rather than a wider scan: ``README.md`` deliberately writes
    "``vq kill`` produces ``killed``, not ``cancelled``", and ``cancelled`` is
    a name vq does not produce. A whole-page scan would have to carry an
    allow-list of every backticked non-state identifier in the prose, 20 of
    them today, and would fail for reasons that have nothing to do with the
    enum.
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


def test_the_handoff_note_still_points_at_the_page_this_test_guards():
    """The "Keeping it current" table is what tells the next person where to
    look when the enum changes. A guard on the page plus a stale pointer to it
    is a quieter failure than the enum drifting, because nobody goes looking.
    """
    note = (_SITE / "README.md").read_text()

    # Every string below is derived, because this checks a document against
    # the code rather than pinning the code. A literal "vq.spec.JobState"
    # here would survive the enum being renamed or moved: the note would be
    # wrong and the assertion would still pass, since it only ever asked
    # whether some text appears, never whether it still names anything.
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
