"""Which nodes a scheduler job was given, from the scheduler's own record (#168).

The detail poll keeps the scheduler's execution host string verbatim
(``scheduler_exec_host``). This reads it as an allocation:

* **PBS/Torque** ``exec_host``: ``node07/0-7+node08/0-7`` (core ranges per
  node), Torque's older one-entry-per-core form ``node07/0+node07/1``, and
  PBS Pro's ``node07/0*8`` (a slot holding 8 CPUs). Each node keeps the
  cores listed for it.
* **Slurm** ``NodeList``: a hostlist expression such as
  ``node[01-04,07],gpu[1-2]-ib``. It names nodes only, so their cores stay
  unknown rather than being split evenly.

It is the allocation the scheduler reports, never a measurement of what each
node used. Anything that does not parse cleanly, or would expand beyond
:data:`MAX_NODES` nodes, gives None, and the caller keeps showing the raw
string.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

MAX_NODES = 4096
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,254}$")
_CORE_RANGE = re.compile(r"^(\d+)(?:-(\d+))?$")
_PBSPRO_SLOT = re.compile(r"^(\d+)\*(\d+)$")
_BRACKET = re.compile(r"\[([^\[\]]*)\]")


@dataclass(frozen=True)
class NodeShare:
    node: str
    #: Cores allocated on this node; None when the scheduler does not say.
    cores: int | None


def parse_exec_host(value: str | None) -> list[NodeShare] | None:
    """The nodes in a scheduler's execution host string, in order, or None."""
    if not value:
        return None
    text = value.strip()
    if "/" in text:
        return _pbs(text)
    return _slurm(text)


def _pbs(text: str) -> list[NodeShare] | None:
    cores: dict[str, int] = {}
    for entry in text.split("+"):
        node, sep, spec = entry.partition("/")
        if not sep or not _NAME.match(node):
            return None
        count = _pbs_cores(spec)
        if count is None:
            return None
        if node not in cores and len(cores) >= MAX_NODES:
            return None
        cores[node] = cores.get(node, 0) + count
    return [NodeShare(node, count) for node, count in cores.items()]


def _pbs_cores(spec: str) -> int | None:
    slot = _PBSPRO_SLOT.match(spec)
    if slot is not None:
        return int(slot.group(2)) or None
    total = 0
    for part in spec.split(","):
        match = _CORE_RANGE.match(part)
        if match is None:
            return None
        low = int(match.group(1))
        high = int(match.group(2)) if match.group(2) is not None else low
        if high < low:
            return None
        total += high - low + 1
    return total


def _slurm(text: str) -> list[NodeShare] | None:
    nodes: list[str] = []
    for expression in _split_top_level(text):
        expanded = _expand(expression, MAX_NODES - len(nodes))
        if expanded is None:
            return None
        nodes.extend(expanded)
    if not nodes or len(set(nodes)) != len(nodes):
        return None
    return [NodeShare(node, None) for node in nodes]


def _split_top_level(text: str) -> list[str]:
    """Split a hostlist on the commas outside brackets."""
    parts, depth, start = [], 0, 0
    for index, char in enumerate(text):
        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append(text[start:index])
            start = index + 1
    parts.append(text[start:])
    return parts


def _expand(expression: str, budget: int) -> list[str] | None:
    """Every name one bracketed expression stands for, or None.

    ``budget`` is how many more names may be produced; each name spends one,
    and running out makes the whole list unparsed.
    """
    match = _BRACKET.search(expression)
    if match is None:
        return [expression] if _NAME.match(expression) and budget >= 1 else None
    head, tail = expression[:match.start()], expression[match.end():]
    names: list[str] = []
    for part in match.group(1).split(","):
        bounds = _CORE_RANGE.match(part)
        if bounds is None:
            return None
        low, high = bounds.group(1), bounds.group(2) or bounds.group(1)
        if int(high) < int(low):
            return None
        # A zero-padded range keeps its width: node[01-10] -> node01..node10.
        width = len(low) if low.startswith("0") else 0
        for number in range(int(low), int(high) + 1):
            rest = _expand(f"{head}{number:0{width}d}{tail}", budget - len(names))
            if rest is None:
                return None
            names.extend(rest)
    return names
