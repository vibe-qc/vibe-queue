"""Pure persisted-state predicates shared by dispatch and status."""

from __future__ import annotations

from collections.abc import Iterator, Mapping

from vq.spec import TERMINAL_STATES, JobSpec, JobState


def iter_unmet_dependencies(
    spec: JobSpec,
    specs_by_id: Mapping[str, JobSpec],
) -> Iterator[tuple[str, JobState | None]]:
    """Yield unmet dependency IDs and their observed states in field order.

    Required dependencies need ``COMPLETED``; after-any dependencies need any
    terminal state.  Missing IDs yield ``None``.  The caller owns the snapshot
    and decides whether to short-circuit, materialize every result, or format
    it for display.
    """
    for dependency_id in spec.depends_on or ():
        predecessor = specs_by_id.get(dependency_id)
        if predecessor is None or predecessor.state != JobState.COMPLETED:
            yield (
                dependency_id,
                predecessor.state if predecessor is not None else None,
            )
    for dependency_id in spec.depends_on_any or ():
        predecessor = specs_by_id.get(dependency_id)
        if predecessor is None or predecessor.state not in TERMINAL_STATES:
            yield (
                dependency_id,
                predecessor.state if predecessor is not None else None,
            )
