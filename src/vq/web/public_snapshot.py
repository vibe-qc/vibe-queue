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

Imports stay within the standard library and :mod:`vq.config`, so the
writer runs without the ``[web]`` extra.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime, timedelta
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
    the output without a change here and a new schema version.

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
