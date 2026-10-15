"""The public, read-only fleet snapshot (#66): schema ``vq.public-fleet/1``.

The fleet console is an operator surface: it shows host keys, SSH targets,
job ids, argv, paths, logs and energies, and it stays on loopback behind
authentication. The public face is something else entirely. An
authenticated operator-side process writes a *static* snapshot, and a
separate unprivileged process with no fleet credentials serves the files.
vq never serves the public face itself.

This module owns what that snapshot may contain. It is default deny: the
schema below is the complete list of fields, every object in it forbids
additional properties, and nothing reaches the output except through it.

* **Hosts** appear only under a display name the operator configured in
  ``[public_snapshot.hosts]``, with a coarse availability and a coarse
  load bucket. A down or unreachable host keeps its card, with no reason.
* **Calculations** appear only when their submitter opted in with a
  ``public.<alias>`` tag (:data:`PUBLIC_TAG_PREFIX`), and then only as the
  alias, a coarse lifecycle state and the display name of the host, when
  it has one.

Everything else is excluded by construction: job ids, program names and
arguments, structures, energies, paths, logs, submitters, host keys, SSH
aliases, addresses and anything a host reports about itself.

Imports stay within the standard library, :mod:`vq.config` and
:mod:`vq.spec`, so the writer runs without the ``[web]`` extra.
"""
from __future__ import annotations

import contextlib
import html
import json
import os
import re
import tempfile
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from vq.config import PUBLIC_DISPLAY_NAME_MAX_LEN, PublicSnapshotConfig
from vq.spec import JOB_NAME_MAX_LEN

if TYPE_CHECKING:  # pragma: no cover — typing only
    from vq.overview import HostOverview
    from vq.web.fleet import FleetSnapshot

SCHEMA_ID = "vq.public-fleet/1"
"""Version tag of the public snapshot. A change to any field's meaning or
to the allowlist is a new version, never an edit of this one."""

PUBLIC_TAG_PREFIX = "public."
"""Submit-time opt-in: ``vq submit ... --tag public.<alias>``.

The maintainer's decision was a ``public=<alias>`` form. ``=`` is outside
the job-tag charset (``[A-Za-z0-9._-]``, :data:`vq.spec.JOB_NAME_PATTERN`),
and widening that validator would change what every tag accepts, so the
separator is ``.``, which the charset already has. The prefix is exact and
lowercase: ``Public.x`` or ``public-x`` is an ordinary tag, not an opt-in."""

PUBLIC_ALIAS_MAX_LEN = JOB_NAME_MAX_LEN - len(PUBLIC_TAG_PREFIX)
PUBLIC_ALIAS_PATTERN = re.compile(rf"^[A-Za-z0-9._-]{{1,{PUBLIC_ALIAS_MAX_LEN}}}$")

HOST_STATES: tuple[str, ...] = ("up", "down", "unknown")
"""``up``: the host answered the operator's sweep. ``down``: it is
administratively down or did not answer. ``unknown``: a display name is
configured for a key the sweep did not cover."""

HOST_LOADS: tuple[str, ...] = ("idle", "light", "busy", "full", "active", "unknown")
"""Coarse load. ``idle``: no running or waiting work. ``light``/``busy``/
``full``: running CPUs below half, below 90 % and at or above 90 % of the
host's configured CPU budget. ``active``: work is running or waiting but
the budget is not known. ``unknown``: the host is not ``up``."""

CALCULATION_STATES: tuple[str, ...] = (
    "queued", "running", "paused", "finished", "failed", "cancelled", "unknown",
)
"""Coarse lifecycle state of an opted-in calculation."""

_TIMESTAMP = {"type": "string", "format": "date-time"}

PUBLIC_SNAPSHOT_JSON_SCHEMA: dict[str, object] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": SCHEMA_ID,
    "title": "vq public fleet snapshot",
    "type": "object",
    "additionalProperties": False,
    "required": ["schema", "generated_at", "stale_after", "title", "hosts", "calculations"],
    "properties": {
        "schema": {"const": SCHEMA_ID},
        "generated_at": _TIMESTAMP,
        "stale_after": _TIMESTAMP,
        "title": {
            "type": ["string", "null"], "minLength": 1,
            "maxLength": PUBLIC_DISPLAY_NAME_MAX_LEN,
        },
        "hosts": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["name", "state", "load"],
                "properties": {
                    "name": {
                        "type": "string", "minLength": 1,
                        "maxLength": PUBLIC_DISPLAY_NAME_MAX_LEN,
                    },
                    "state": {"enum": list(HOST_STATES)},
                    "load": {"enum": list(HOST_LOADS)},
                },
            },
        },
        "calculations": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["alias", "state", "host"],
                "properties": {
                    "alias": {"type": "string", "pattern": PUBLIC_ALIAS_PATTERN.pattern},
                    "state": {"enum": list(CALCULATION_STATES)},
                    "host": {
                        "type": ["string", "null"], "minLength": 1,
                        "maxLength": PUBLIC_DISPLAY_NAME_MAX_LEN,
                    },
                },
            },
        },
    },
}
"""The complete field allowlist of ``vq.public-fleet/1``.

``generated_at`` is when the operator's sweep observed the fleet;
``stale_after`` is when a reader should stop trusting the page. ``host``
on a calculation is the display name of the host it is queued on, or
``null`` when that host has no display name."""


def public_alias(tags: object) -> str | None:
    """The public alias a job's tags opt it in under, or ``None``.

    Exactly one well-formed ``public.<alias>`` tag opts a job in. No such
    tag, a malformed alias, or two different aliases on one job all leave
    it out: an ambiguous opt-in is not an opt-in. ``tags`` is typed
    loosely because queue rows from a remote vq are plain JSON.
    """
    if not isinstance(tags, Iterable) or isinstance(tags, (str, bytes)):
        return None
    aliases = {
        tag[len(PUBLIC_TAG_PREFIX):]
        for tag in tags
        if isinstance(tag, str) and tag.startswith(PUBLIC_TAG_PREFIX)
    }
    if len(aliases) != 1:
        return None
    (alias,) = aliases
    return alias if PUBLIC_ALIAS_PATTERN.fullmatch(alias) else None


#: Monitor-facing job state (``effective_state``, else ``state``) to the
#: public lifecycle state. Anything not listed, including every scheduler
#: fault phase and any state a newer vq invents, is ``unknown``.
_CALCULATION_STATE = {
    "pending": "queued",
    "submitting": "queued",
    "queued": "queued",
    "held": "queued",
    "unpolled": "queued",
    "running": "running",
    "finishing": "running",
    "suspended": "paused",
    "completed": "finished",
    "failed": "failed",
    "oom_killed": "failed",
    "starved": "failed",
    "time_exceeded": "failed",
    "interrupted": "failed",
    "aborted_by_queue": "failed",
    "killed": "cancelled",
}


def _host_state(ov: HostOverview) -> str:
    return "down" if ov.admin_down or not ov.reachable else "up"


def _host_load(ov: HostOverview) -> str:
    running, pending = max(0, ov.running_cpus), max(0, ov.pending_cpus)
    if running == 0 and pending == 0:
        return "idle"
    if not ov.max_cpus or ov.max_cpus <= 0:
        return "active"
    ratio = running / ov.max_cpus
    if ratio < 0.5:
        return "light"
    return "busy" if ratio < 0.9 else "full"


def project_public_snapshot(
    snapshot: FleetSnapshot, public: PublicSnapshotConfig,
) -> dict[str, object]:
    """Project an operator's fleet snapshot to a ``vq.public-fleet/1`` document.

    A pure function: no I/O, no clock. Every output value is either a
    constant from this module, a coarse bucket computed here, an operator's
    display name or title from ``public``, a submitter's validated alias,
    or a timestamp derived from ``snapshot.gathered_at``. Nothing else in
    ``snapshot`` is copied, so a field the fleet grows later cannot reach
    the output without a change here (which, by the rule on
    :data:`SCHEMA_ID`, also means a new schema version).

    * A host whose key has no display name is left out entirely.
    * A host that is down or unreachable is a card with ``state: down``
      and ``load: unknown``; its reason is not carried.
    * A display name for a key the snapshot does not contain is a card
      with ``state: unknown``.
    * A job without exactly one valid ``public.<alias>`` tag is left out.
      A job listed twice (one machine enrolled under two keys) appears
      once, under the first host that listed it.
    """
    generated = datetime.fromisoformat(snapshot.gathered_at)
    names = public.hosts

    hosts: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for host in snapshot.hosts:
        seen_keys.add(host.host)
        name = names.get(host.host)
        if name is None:
            continue
        state = _host_state(host.overview)
        hosts.append({
            "name": name,
            "state": state,
            "load": _host_load(host.overview) if state == "up" else "unknown",
        })
    hosts.extend(
        {"name": name, "state": "unknown", "load": "unknown"}
        for key, name in names.items() if key not in seen_keys
    )

    calculations: list[dict[str, str | None]] = []
    seen_jobs: set[str] = set()
    for host in snapshot.hosts:
        for row in host.jobs:
            alias = public_alias(row.get("tags"))
            if alias is None:
                continue
            job_id = row.get("id")
            if isinstance(job_id, str):
                if job_id in seen_jobs:
                    continue
                seen_jobs.add(job_id)
            raw_state = row.get("effective_state") or row.get("state")
            calculations.append({
                "alias": alias,
                "state": _CALCULATION_STATE.get(raw_state, "unknown")
                if isinstance(raw_state, str) else "unknown",
                "host": names.get(host.host),
            })
    calculations.sort(key=lambda c: (c["alias"] or "", c["state"] or "", c["host"] or ""))

    return {
        "schema": SCHEMA_ID,
        "generated_at": generated.isoformat(),
        "stale_after": (generated + timedelta(seconds=public.stale_after_seconds)).isoformat(),
        "title": public.title,
        "hosts": hosts,
        "calculations": calculations,
    }


# --------------------------------------------------------------------------
# Writing the snapshot (P2). No server and no listener: the files are handed
# to whatever unprivileged process the operator chose to serve them.
# --------------------------------------------------------------------------

PUBLIC_JSON_NAME = "fleet.json"
PUBLIC_HTML_NAME = "index.html"

#: World-readable, so a separate serving account can read the files
#: without sharing a group with the operator.
_PUBLIC_FILE_MODE = 0o644


def check_public_document(doc: dict[str, object]) -> None:
    """Refuse a document that is not exactly a ``vq.public-fleet/1`` document.

    A re-check of the allowlist at the point of writing: the key set of
    every object, and every value's type, allowed values, pattern, length
    and timestamp format, as :data:`PUBLIC_SNAPSHOT_JSON_SCHEMA` states
    them. A future edit to :func:`project_public_snapshot` that adds a
    field, or that lets a raw value through an allowed field (a host's
    error string as its ``load``, say), fails here instead of being
    published. Needs no ``jsonschema`` at run time. The error names the
    path of the offending value, never the value itself.
    """
    props = PUBLIC_SNAPSHOT_JSON_SCHEMA["properties"]
    assert isinstance(props, dict)

    def _keys(obj: object, allowed: object, where: str) -> None:
        if not isinstance(obj, dict) or not isinstance(allowed, dict):
            raise ValueError(f"{where}: not an object")
        if set(obj) != set(allowed):
            raise ValueError(f"{where}: keys {sorted(obj)} are not {sorted(allowed)}")

    def _text(value: object, where: str, *, optional: bool = False) -> None:
        if value is None and optional:
            return
        if not isinstance(value, str):
            raise ValueError(f"{where}: not a string")
        if not 1 <= len(value) <= PUBLIC_DISPLAY_NAME_MAX_LEN:
            raise ValueError(f"{where}: not 1 to {PUBLIC_DISPLAY_NAME_MAX_LEN} characters")

    def _one_of(value: object, allowed: tuple[str, ...], where: str) -> None:
        if not isinstance(value, str) or value not in allowed:
            raise ValueError(f"{where}: not one of {', '.join(allowed)}")

    def _timestamp(value: object, where: str) -> None:
        if not isinstance(value, str):
            raise ValueError(f"{where}: not a string")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError(f"{where}: not an ISO 8601 timestamp") from None
        if parsed.tzinfo is None:
            raise ValueError(f"{where}: timestamp has no time zone")

    _keys(doc, props, "snapshot")
    if doc["schema"] != SCHEMA_ID:
        raise ValueError(f"snapshot: schema is not {SCHEMA_ID}")
    _timestamp(doc["generated_at"], "generated_at")
    _timestamp(doc["stale_after"], "stale_after")
    _text(doc["title"], "title", optional=True)
    for section in ("hosts", "calculations"):
        items = doc[section]
        if not isinstance(items, list):
            raise ValueError(f"{section}: not a list")
        allowed = props[section]["items"]["properties"]
        for index, item in enumerate(items):
            where = f"{section}[{index}]"
            _keys(item, allowed, where)
            assert isinstance(item, dict)
            if section == "hosts":
                _text(item["name"], f"{where}.name")
                _one_of(item["state"], HOST_STATES, f"{where}.state")
                _one_of(item["load"], HOST_LOADS, f"{where}.load")
            else:
                alias = item["alias"]
                if not isinstance(alias, str) or not PUBLIC_ALIAS_PATTERN.fullmatch(alias):
                    raise ValueError(f"{where}.alias: not a well-formed public alias")
                _one_of(item["state"], CALCULATION_STATES, f"{where}.state")
                _text(item["host"], f"{where}.host", optional=True)


def _atomic_write(path: Path, text: str) -> None:
    """Replace ``path`` with ``text`` so a reader sees the old file or the new one."""
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp, _PUBLIC_FILE_MODE)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp)
        raise


def write_public_snapshot(doc: dict[str, object], out_dir: Path) -> tuple[Path, Path]:
    """Write ``fleet.json`` and ``index.html`` into ``out_dir``; return both paths.

    Each file is replaced atomically (a temporary file in the same
    directory, fsynced, then renamed), so a server never hands out a torn
    file. The JSON goes first; the two files are replaced one after the
    other, not as a pair. The document is checked against the allowlist
    before anything is written, and nothing is written if it fails.
    """
    check_public_document(doc)
    body = json.dumps(doc, indent=2, sort_keys=True) + "\n"
    page = render_public_html(doc)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path, html_path = out_dir / PUBLIC_JSON_NAME, out_dir / PUBLIC_HTML_NAME
    _atomic_write(json_path, body)
    _atomic_write(html_path, page)
    with contextlib.suppress(OSError):  # directory fsync is best-effort
        dir_fd = os.open(out_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    return json_path, html_path


_HTML_STYLE = """
:root { color-scheme: light dark; --fg: #1d1d1f; --bg: #fbfbfd; --muted: #5f6368;
  --line: #d9dce1; --up: #1e7b34; --down: #b3261e; --unknown: #6b6f76; }
@media (prefers-color-scheme: dark) { :root { --fg: #e8eaed; --bg: #16171a;
  --muted: #a0a4ab; --line: #34373d; --up: #6fcf87; --down: #f28b82; --unknown: #a0a4ab; } }
body { margin: 0 auto; max-width: 60rem; padding: 1.5rem 1rem; font: 16px/1.5 system-ui,
  sans-serif; color: var(--fg); background: var(--bg); }
h1 { font-size: 1.6rem; margin: 0 0 .25rem; } h2 { font-size: 1.15rem; margin: 2rem 0 .5rem; }
.meta { color: var(--muted); margin: 0; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(12rem, 1fr));
  gap: .75rem; padding: 0; list-style: none; }
.card { border: 1px solid var(--line); border-radius: .5rem; padding: .75rem; }
.card strong { display: block; overflow-wrap: anywhere; }
.state-up { color: var(--up); } .state-down { color: var(--down); }
.state-unknown { color: var(--unknown); }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: .4rem .5rem; border-bottom: 1px solid var(--line);
  overflow-wrap: anywhere; }
"""


def render_public_html(doc: dict[str, object]) -> str:
    """A static page for a ``vq.public-fleet/1`` document: no script, no
    external resource, every value HTML-escaped.

    The page cannot compute staleness when it is viewed, so it states
    both times and tells the reader what a passed ``stale_after`` means.
    """
    esc = html.escape
    title = doc.get("title") or "Fleet status"
    hosts = doc.get("hosts") or []
    calcs = doc.get("calculations") or []
    assert isinstance(hosts, list) and isinstance(calcs, list)

    cards = "\n".join(
        f'<li class="card"><strong>{esc(str(h["name"]))}</strong>'
        f'<span class="state-{esc(str(h["state"]))}">{esc(str(h["state"]))}</span>'
        f' · load {esc(str(h["load"]))}</li>'
        for h in hosts
    ) or "<li>No machines are published.</li>"
    rows = "\n".join(
        f"<tr><td>{esc(str(c['alias']))}</td><td>{esc(str(c['state']))}</td>"
        f"<td>{esc(str(c['host'])) if c['host'] is not None else '—'}</td></tr>"
        for c in calcs
    )
    calc_block = (
        "<table><thead><tr><th>Calculation</th><th>State</th><th>Machine</th></tr>"
        f"</thead><tbody>\n{rows}\n</tbody></table>"
        if calcs else "<p>No calculations are published.</p>"
    )
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="generator" content="vq {esc(SCHEMA_ID)}">
<title>{esc(str(title))}</title>
<style>{_HTML_STYLE}</style>
</head>
<body>
<h1>{esc(str(title))}</h1>
<p class="meta">Fleet observed at <time datetime="{esc(str(doc["generated_at"]))}">\
{esc(str(doc["generated_at"]))}</time>.</p>
<p class="meta">Stale after <time datetime="{esc(str(doc["stale_after"]))}">\
{esc(str(doc["stale_after"]))}</time>: if that time has passed, this page is out of date \
and the machines and calculations below may no longer be accurate.</p>
<h2>Machines</h2>
<ul class="cards">
{cards}
</ul>
<h2>Calculations</h2>
{calc_block}
<p class="meta">Machine-readable: <a href="{PUBLIC_JSON_NAME}">{PUBLIC_JSON_NAME}</a> \
({esc(SCHEMA_ID)}).</p>
</body>
</html>
"""
