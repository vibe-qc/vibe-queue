"""Driver-owned scheduler budgets, shared by aliases of one target."""
from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from vq.config import Config


class SchedulerTargetAdmission:
    """One dispatch pass's capacity snapshot and cross-pass attempt pacing.

    Site/account quotas remain the scheduler's authority. These budgets count
    only this driver's admitted allocations, including uncertain submissions.
    """

    def __init__(
        self,
        config: Config,
        reservations: Iterable[tuple[str | None, int]],
        last_attempts: dict[str, float],
    ) -> None:
        self._canonical = {
            name: host.fleet_canonical_host or name
            for name, host in config.hosts.items()
        }
        self._caps: dict[str, int] = {}
        self._intervals: dict[str, float] = {}
        for name, host in config.hosts.items():
            target = self.canonical(name)
            if host.scheduler_max_jobs is not None:
                self._caps[target] = min(
                    self._caps.get(target, host.scheduler_max_jobs),
                    host.scheduler_max_jobs,
                )
            self._intervals[target] = max(
                self._intervals.get(target, 0.0),
                host.scheduler_submit_interval_seconds,
            )
        self._active: Counter[str | None] = Counter()
        for reserved_target, count in reservations:
            key = None if reserved_target is None else self.canonical(reserved_target)
            self._active[key] += count
        self._last_attempts = last_attempts

    def canonical(self, target: str) -> str:
        return self._canonical.get(target, target)

    def can_attempt(self, target: str, now: float) -> bool:
        canonical = self.canonical(target)
        cap = self._caps.get(canonical)
        # A corrupt/quarantined reservation whose target cannot be established
        # must not silently disappear from every target's budget.
        if cap is not None and self._active[canonical] + self._active[None] >= cap:
            return False
        previous = self._last_attempts.get(canonical)
        interval = self._intervals.get(canonical, 0.0)
        return previous is None or now - previous >= interval

    def attempted(self, target: str, now: float) -> None:
        self._last_attempts[self.canonical(target)] = now

    def reserve(self, target: str | None) -> None:
        self._active[None if target is None else self.canonical(target)] += 1
