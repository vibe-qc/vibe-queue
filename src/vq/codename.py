"""Release codename catalog and lookup.

Every **minor** release carries a "Pioneer's Concept" codename, drawn from
computer-science pioneers and deliberately distinct from vibe-qc's
chemistry and physics scientists. The second word names the concept the
person is known for, and it maps to what that release actually did:
Lamport's Clock for logical clocks, Dekker's Mutex for a lock, Chandy's
Snapshot for distributed snapshots. The shape is the same ``[surname]'s
[object]`` vibe-qc uses, so the two systems' release notes stay culturally
adjacent. The convention was declared at v0.7.0 and is documented in
``docs/version_compatibility.md``.

Patch releases inherit their parent minor's codename unless they carry a
distinct one of their own; the v0.7.x and v0.8.x lines named nearly every
patch, so most of them are listed here explicitly. Dev builds inherit the
codename of the release they are heading toward, after PEP 440 suffix
stripping.

Forward entries are provisional and must be re-confirmed against the delivered
concept when their release is cut. When cutting a new minor, add or confirm its
entry here in the same commit that bumps ``pyproject.toml`` and ``__init__.py``.
Picks happen at ship time and track
the thematic substance of the ship; they are not counted off a reserved list.
"""

from __future__ import annotations

import re

__all__ = ["RELEASE_CODENAMES", "codename_for_version", "format_version"]


#: Codename catalog, keyed by the X.Y.Z release version.
RELEASE_CODENAMES: dict[str, str] = {
    # --- Shipped ---------------------------------------------------------
    "0.7.0": "Hoare's Pipeline",
    "0.7.1": "Lamport's Clock",
    "0.7.2": "Engelbart's Demo",
    "0.7.3": "Dijkstra's Semaphore",
    "0.7.4": "Ritchie's Pipe",
    "0.7.5": "Hopper's Compiler",
    "0.7.6": "Tanenbaum's Mailbox",
    "0.7.7": "Cerf's Datagram",
    "0.7.8": "Knuth's Schedule",
    "0.7.9": "Liskov's Substitution",
    "0.7.10": "McCarthy's List",
    "0.7.11": "Stroustrup's Stencil",
    "0.7.12": "Wirth's Modula",
    "0.7.13": "Backus's Form",
    "0.7.14": "Hamming's Code",
    "0.7.15": "Shannon's Entropy",
    "0.7.16": "Codd's Tuple",
    "0.7.17": "Postel's Robustness",
    "0.7.18": "Kay's Object",

    "0.8.0": "Dahl's Simula",
    "0.8.1": "Karp's Reduction",
    "0.8.2": "Lamport's Logical",
    "0.8.3": "Dijkstra's Shortest",
    "0.8.4": "Brooks's Mythical",
    "0.8.5": "Knuth's Concrete",
    "0.8.6": "Codd's Audit",
    "0.8.7": "Hoare's Triple",
    "0.8.8": "Turing's Halt",
    "0.8.9": "Cook's Hierarchy",
    "0.8.10": "Tarjan's Bridge",
    "0.8.11": "Dekker's Mutex",
    "0.8.12": "Peterson's Lock",
    "0.8.13": "Gray's Transaction",
    "0.8.14": "Thompson's Reaper",
    "0.8.15": "Corbató's Daemon",
    "0.8.16": "Chandy's Snapshot",
    "0.8.17": "Saltzer's End-to-End",
    "0.8.18": "Chandra's Detector",
    "0.8.19": "Lampson's Confinement",
    "0.8.20": "Baker's Collector",
    "0.8.21": "Nyquist's Sample",
    "0.8.22": "Mills's Clock",
    "0.8.23": "Bush's Memex",
    "0.8.24": "Jacobson's Backpressure",
    "0.8.25": "Denning's Lattice",

    "0.9.0": "Tukey's Window",
    "0.9.1": "Strachey's Monitor",
    "0.9.2": "Kleinrock's Queue",

    "0.10.0": "Lampson's Hint",

    "0.11.0": "Eager's Sharing",

    "0.12.0": "Hopper's Bug",

    # --- Backfilled 2026-09-09, approved by the maintainer the same day ---
    #
    # The series lapsed after v0.12.0 and vq reached 0.25.7 unnamed. These
    # were reconstructed from the monorepo's own version-bump commits; see
    # .release-status/CODENAME-PROPOSAL.md for the substance behind each.
    #
    # These are now settled names, not drafts. Changing one after a tag
    # carries it is not a one-line edit any more: the tag is immutable, so
    # the name in it stands regardless of what this dict says.
    # Jim Gray, write-ahead logging: the durable ordered
    # record of every transition, which is what `vq events` exposes.
    "0.13.0": "Gray's Log",
    # Roger Needham, authentication. A principal is the
    # identity on whose behalf an action is checked; `doctor --as-driver`
    # runs the check as a different one.
    "0.14.0": "Needham's Principal",
    # Saltzer & Schroeder 1975, protection domains.
    # "Authority" is their vocabulary, and a dispatch stop is now
    # attributed to the authority that caused it.
    "0.15.0": "Schroeder's Authority",
    # Ivan Sutherland, Sketchpad (1963), the first
    # interactive graphical view of a system's state. The fleet console's
    # first cross-host drill-down.
    "0.16.0": "Sutherland's Sketchpad",
    # Fernando Corbató, CTSS, credited with the first
    # computer password. Accounts and sessions arrive on the console.
    "0.17.0": "Corbató's Password",
    # Martín Abadi, co-author of "Authentication in
    # Distributed Systems" (1992), the canonical delegation paper.
    # Per-host delegated admin is literally delegation.
    "0.18.0": "Abadi's Delegation",
    # Ralph Merkle, hash trees: the SHA is the evidence.
    # A rollout that stages nothing but the pinned commit is
    # content-addressed deployment.
    "0.19.0": "Merkle's Hash",
    # Maurice Herlihy, wait-free synchronization: an
    # operation completes without waiting on any other. The scheduler
    # runtime update stops draining and waiting for running work.
    "0.20.0": "Herlihy's Wait-Free",
    # Tom Kilburn, Atlas, virtual memory: one name maps to
    # different physical frames. One program name, one on-disk slot per SHA.
    "0.21.0": "Kilburn's Page",
    # J. D. C. Little, L = lambda W, the relation an operator
    # needs to reason about a queue. The quota that gates dispatch stops
    # being invisible.
    "0.22.0": "Little's Law",
    # Jerry Saltzer, "On the Naming and Binding of Network
    # Destinations" (1982). `runtime_slot_root` is a declared binding from a
    # program to its slot root, not an implicit one.
    "0.23.0": "Saltzer's Binding",
    # C. J. Cheney's copying collector: build the new space
    # beside the old, flip, reclaim what nothing references.
    "0.24.0": "Cheney's Semispace",
    # Härder & Reuter (1983) coined ACID and wrote the
    # transaction-recovery paper. A lifecycle transition either completes or
    # leaves a recoverable state.
    "0.25.0": "Härder's Atomicity",

    # --- Cut at ship time ------------------------------------------------
    # Eric S. Raymond, "The Cathedral and the Bazaar" (1997), the canonical
    # text on releasing a project to the public and what that demands of it.
    # v0.26.0 is where vq became publishable: the MPL text it had been
    # declaring since the split, a security policy, a contributor guide, a
    # changelog, a documentation site, this catalogue, and a release
    # procedure. The bazaar is the open half of the essay's pairing.
    "0.26.0": "Raymond's Bazaar",

    # --- Provisional: re-confirm the delivered concept before tagging ---
    "0.27.0": "Fidge's Timestamp",
    "0.28.0": "Mattern's Cut",
    "0.29.0": "Braden's Requirements",
    "0.30.0": "Bloom's Filter",
    "0.31.0": "Stonebraker's Vacuum",
    "0.32.0": "Brewer's Partition",
    "0.33.0": "Erlang's Blocking",
}


_PRERELEASE = re.compile(r"(?<=\d)(?:\.dev|a|b|rc)\d*.*$")


def _strip_prerelease(version: str) -> str:
    """Return the release a pre-release build is heading toward.

    Strips a PEP 440 ``.devN`` / ``aN`` / ``bN`` / ``rcN`` suffix, so
    ``"0.25.0.dev3"`` and ``"0.25.0rc1"`` both resolve to ``"0.25.0"``.
    """
    return _PRERELEASE.sub("", version.strip())


def codename_for_version(version: str) -> str | None:
    """Return the codename for *version*, or ``None`` if none is assigned.

    Resolution order:

    1. Strip any PEP 440 pre-release suffix.
    2. Look the resulting ``X.Y.Z`` up directly.
    3. Fall back to the parent minor, ``X.Y.0``, so an unlisted patch
       inherits its minor's codename.
    4. ``None`` when the minor is not registered either.

    >>> codename_for_version("0.25.7")
    "Härder's Atomicity"
    >>> codename_for_version("0.25.0.dev1")
    "Härder's Atomicity"
    >>> codename_for_version("99.0.0") is None
    True
    """
    stripped = _strip_prerelease(version)
    direct = RELEASE_CODENAMES.get(stripped)
    if direct is not None:
        return direct
    parts = stripped.split(".")
    if len(parts) >= 2:
        return RELEASE_CODENAMES.get(f"{parts[0]}.{parts[1]}.0")
    return None


def format_version(version: str) -> str:
    """Render a version for display, with its codename when one exists.

    ``0.25.7`` -> ``0.25.7 "Härder's Atomicity"``. A version with no
    codename renders bare, so an unreleased or unregistered build never
    prints an empty pair of quotes.
    """
    codename = codename_for_version(version)
    return f'{version} "{codename}"' if codename else version
