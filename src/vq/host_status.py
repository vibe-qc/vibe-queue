"""Client-side administrative host availability ("down" marking).

A host can be marked administratively DOWN so vq skips probing it in
fan-out operations — ``vq overview`` / ``vq summary``, and the ``--all``
form of ``vq queue`` / ``vq programs`` / ``vq admin status`` — and refuses
new submits to it. This is the *submitter's* local view ("don't bother
with workstation from here for now"), NOT a statement about the host itself:
it never touches the remote daemon. Use it for a box that's unreachable,
on a flaky link, or deliberately offline, so a fleet sweep doesn't hang on
its ConnectTimeout every time.

Temporary by design — ``vq host up HOST`` clears it. The state is a small
JSON file at ``<config_dir>/hosts_down.json``, kept separate from
``config.toml`` so toggling it never rewrites (and never risks clobbering
the hand-maintained comments in) the user's config.

Robustness: the read path NEVER raises. A missing, empty, or corrupt
down-list yields ``{}`` (every host treated as up) rather than breaking
every vq command — an unreadable convenience marker must not take the CLI
down with it.
"""
from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass
from pathlib import Path

from vq import paths


@dataclass(frozen=True)
class DownEntry:
    """One administratively-down host."""

    host: str
    reason: str
    since: str  # ISO-8601 UTC timestamp, or "" if unknown.

    def describe(self) -> str:
        """Compact one-line description for listings: reason + since-when."""
        bits: list[str] = []
        if self.reason:
            bits.append(self.reason)
        if self.since:
            bits.append(f"since {self.since}")
        return "; ".join(bits) if bits else "(no reason given)"


def down_file() -> Path:
    """Path to the client-side down-list (``<config_dir>/hosts_down.json``)."""
    return paths.config_dir() / "hosts_down.json"


def load_down() -> dict[str, DownEntry]:
    """Return ``{host: DownEntry}`` for every administratively-down host.

    Never raises: a missing / empty / corrupt / wrong-shaped file yields
    ``{}`` so a broken convenience marker can't break every vq command.
    """
    p = down_file()
    if not p.is_file():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(data, dict):
        return {}
    out: dict[str, DownEntry] = {}
    for host, entry in data.items():
        if not isinstance(host, str):
            continue
        if isinstance(entry, dict):
            reason = str(entry.get("reason", "") or "")
            since = str(entry.get("since", "") or "")
        else:
            reason, since = "", ""
        out[host] = DownEntry(host=host, reason=reason, since=since)
    return out


def is_down(host: str) -> DownEntry | None:
    """Return the ``DownEntry`` if ``host`` is administratively down, else None."""
    return load_down().get(host)


def _save(entries: dict[str, DownEntry]) -> None:
    data = {
        host: {"reason": e.reason, "since": e.since}
        for host, e in entries.items()
    }
    paths.atomic_write_text(
        down_file(), json.dumps(data, indent=2, sort_keys=True) + "\n"
    )


def mark_down(
    host: str, reason: str = "", *, now: _dt.datetime | None = None
) -> DownEntry:
    """Mark ``host`` administratively down. Idempotent — updates the reason
    and refreshes the timestamp when the host is already down. Returns the
    new entry. ``now`` is injectable for deterministic tests.
    """
    stamp = (now or _dt.datetime.now(_dt.UTC)).isoformat(
        timespec="seconds"
    )
    entries = load_down()
    entry = DownEntry(host=host, reason=reason, since=stamp)
    entries[host] = entry
    _save(entries)
    return entry


def mark_up(host: str) -> bool:
    """Clear ``host``'s down mark. Return True if it was down (now cleared),
    False if it wasn't marked down in the first place."""
    entries = load_down()
    if host not in entries:
        return False
    del entries[host]
    _save(entries)
    return True
