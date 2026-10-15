"""The fleet console's campaign graphs (#165).

Read-only. For each campaign a controller registered on this machine
(:mod:`vq.campaign_registry`), the run directory's journal goes through the
same :func:`vq.campaign.campaign_status` as ``vq campaign status``, and the
result is laid out as a compact left-to-right graph: one column per
dependency depth, nodes coloured by status, edges marked ``success`` or
``completion``. Each node's latest attempt links to its job's detail page,
where its logs and results are. Nothing here submits, retries or cleans.

A campaign whose file, journal or run directory cannot be read is shown
with the reason instead of a graph. Paths appear only as the run
directory's name: the registration's absolute paths stay on the server.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import quote

from vq import campaign_registry
from vq.campaign import (
    JOURNAL_SCHEMA,
    Campaign,
    CampaignError,
    Journal,
    campaign_digest,
    campaign_status,
    load_campaign,
    load_journal,
)

BOX_WIDTH = 190
BOX_HEIGHT = 58
COLUMN_GAP = 56
ROW_GAP = 14
PAD = 6

#: The journal's name in a run directory (``campaign_controller.JOURNAL_NAME``).
JOURNAL_NAME = "journal.json"


def campaigns_context() -> dict[str, object]:
    found, problems = campaign_registry.registrations()
    return {"campaigns": [campaign_view(r) for r in found], "registry_problems": problems}


def campaign_view(registration: campaign_registry.Registration) -> dict[str, Any]:
    run_dir = Path(registration.run_dir)
    view: dict[str, Any] = {
        "campaign_id": registration.campaign_id,
        "campaign_sha256": registration.campaign_sha256,
        "run_dir_name": run_dir.name,
        "registered_at": registration.registered_at,
        "status": "unknown",
        "error": None,
        "graph": None,
        "retain": [],
    }
    try:
        campaign = load_campaign(Path(registration.campaign_file))
        if campaign_digest(campaign) != registration.campaign_sha256:
            raise CampaignError("the campaign file has changed since this run was registered; "
                                "its new version is a different campaign")
        if not run_dir.is_dir():
            raise CampaignError("the run directory is gone")
        journal_path = run_dir / JOURNAL_NAME
        if journal_path.exists():
            journal = load_journal(journal_path)
        else:
            journal = Journal.model_validate({"schema_name": JOURNAL_SCHEMA,
                                              "campaign_sha256": registration.campaign_sha256})
        report = campaign_status(campaign, journal)
    except (CampaignError, OSError, ValueError) as exc:
        view["error"] = _without_paths(str(exc), registration)
        return view
    view.update(status=report["status"], graph=graph(campaign, report),
                retain=report["retain"])
    return view


def _without_paths(message: str, registration: campaign_registry.Registration) -> str:
    for path in (registration.run_dir, registration.campaign_file):
        message = message.replace(str(Path(path).parent), "…")
    return message


def graph(campaign: Campaign, report: dict[str, Any]) -> dict[str, Any]:
    """Node boxes, edges and per-node detail, with coordinates for the SVG."""
    nodes = {node.id: node for node in campaign.nodes}
    order = campaign.order()
    depth: dict[str, int] = {}
    for node_id in order:
        depth[node_id] = 1 + max((depth[p] for p in nodes[node_id].edges()), default=-1)
    # Within a column, nodes keep the order the campaign file lists them in.
    columns: dict[int, list[str]] = {}
    for node in campaign.nodes:
        columns.setdefault(depth[node.id], []).append(node.id)
    # ROW_GAP above the first row is the lane for an edge that skips columns.
    place = {
        node_id: (PAD + column * (BOX_WIDTH + COLUMN_GAP), ROW_GAP + row * (BOX_HEIGHT + ROW_GAP))
        for column, members in columns.items() for row, node_id in enumerate(members)
    }
    entries = {str(entry["node"]): entry for entry in report["nodes"]}
    boxes = [_box(node, entries[node.id], *place[node.id]) for node in campaign.nodes]
    edges = []
    for node_id in nodes:
        for producer, on in sorted(nodes[node_id].edges().items()):
            (px, py), (cx, cy) = place[producer], place[node_id]
            start, end = (px + BOX_WIDTH, py + BOX_HEIGHT // 2), (cx, cy + BOX_HEIGHT // 2)
            if depth[node_id] - depth[producer] > 1:
                # It would cross the boxes between, so it climbs into the gap
                # above the higher of its two rows, runs there, and drops in.
                lane = min(py, cy) - ROW_GAP // 2
                path = (f"M {start[0]} {start[1]} H {start[0] + COLUMN_GAP // 2} V {lane} "
                        f"H {end[0] - COLUMN_GAP // 2} V {end[1]} H {end[0]}")
            else:
                bend = (end[0] - start[0]) // 2
                path = (f"M {start[0]} {start[1]} C {start[0] + bend} {start[1]} "
                        f"{end[0] - bend} {end[1]} {end[0]} {end[1]}")
            edges.append({"from": producer, "to": node_id, "on": on, "path": path})
    return {
        "width": 2 * PAD + (max(columns) + 1) * BOX_WIDTH + max(columns) * COLUMN_GAP,
        "height": ROW_GAP + PAD + max(len(m) for m in columns.values()) * (BOX_HEIGHT + ROW_GAP)
        - ROW_GAP,
        "box_width": BOX_WIDTH, "box_height": BOX_HEIGHT,
        "nodes": boxes, "edges": edges,
    }


def _box(node: Any, entry: dict[str, Any], x: int, y: int) -> dict[str, Any]:
    attempts = entry.get("attempts") or []
    latest = attempts[-1] if attempts else None
    attempt = None
    if latest is not None:
        job = latest.get("job")
        attempt = {
            "number": latest["attempt"], "state": latest["state"],
            "retry_of": latest.get("retry_of"),
            "job_id": job["job_id"] if job else None, "host": job["host"] if job else None,
            "href": (f"/fleet/jobs/{quote(job['job_id'], safe='')}"
                     f"?host={quote(job['host'], safe='')}") if job else None,
        }
    records = (latest or {}).get("artifacts") or {}
    outputs = [
        {"name": output.name,
         "status": records[output.name]["status"] if output.name in records else "not published"}
        for output in node.outputs
    ]
    return {
        "id": node.id, "kind": node.kind, "host": node.host or "localhost",
        "status": entry["status"], "reasons": list(entry.get("reasons") or []),
        "next": next_step(entry.get("next") or {}), "attempt": attempt,
        "attempt_count": len(attempts), "outputs": outputs, "x": x, "y": y,
    }


def next_step(action: dict[str, Any]) -> str:
    """The controller's next action for a node, as a short phrase."""
    kind = action.get("action")
    if kind == "submit":
        staged = ", ".join(str(item["input"]) for item in action.get("stage") or [])
        return f"submit attempt {action['attempt']}" + (f" (stages {staged})" if staged else "")
    if kind == "wait":
        return f"wait for attempt {action['attempt']}"
    if kind == "publish":
        return "publish " + ", ".join(action.get("outputs") or [])
    if kind == "reconcile":
        return f"resend attempt {action['attempt']} under its own key"
    return str(action.get("detail") or "")
