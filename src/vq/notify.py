"""Best-effort HTTP webhook notifications from explicit terminal paths.

Single entry point: :func:`send_terminal_notification`. Daemon callers cover
normal local and scheduler reaping, orphan recovery and queue abort, scheduler
wall-time attribution, dependency cascade failure, impossible generated build
jobs, and refresh failure. Notification is not a centralized lifecycle hook:
direct ``PENDING`` kill, dispatch-time failure, and some scheduler
reconciliation paths can write a terminal JobSpec without invoking it.

**Payload shape** (Slack / Discord / Mattermost compatible)::

    {
      "text":    "vq: job NAME-JOBID finished as STATE (rc=N) on HOST",
      "content": "<same>",
      "job": {
        "id":             "<jobid>",
        "name":           "<job_name or null>",
        "state":          "<state>",
        "exit_code":      <int or null>,
        "failure_reason": "<reason or null>",
        "submitted_at":   "<iso>",
        "started_at":     "<iso or null>",
        "finished_at":    "<iso or null>",
        "command":        "<space-joined command>",
        "host":           "<gethostname()>"
      }
    }

* Slack incoming webhooks render ``text`` as the message body.
* Discord incoming webhooks render ``content`` as the message body.
* Mattermost incoming webhooks render ``text`` (Slack-compatible).
* Microsoft Teams uses a different payload (Adaptive Card / MessageCard)
  and is NOT supported by this generic POST — a Teams integration
  would need its own receiver.

The duplication is deliberate and cheap: each platform reads its
preferred key and ignores the other. One webhook URL → works
everywhere we care about.

**Failure model.** Fire-and-forget on a daemon thread by default
(:func:`send_terminal_notification` returns immediately so the
caller — typically the daemon's tick loop — isn't blocked on HTTP).
A POST that times out, errors, or returns >= 400 is logged at
WARNING level and dropped. There is no retry, no buffering, no
persistence. If you need delivery guarantees, point the webhook at
something that owns retry policy (a relay / queue / Lambda).
"""
from __future__ import annotations

import json
import logging
import socket
import threading
import urllib.error
import urllib.request
from typing import Any

from vq.spec import TERMINAL_STATES, JobSpec, signal_name_for_exit

log = logging.getLogger(__name__)

# HTTP timeout for the POST. Picked to be short enough that a hung
# webhook server can't stall a daemon thread for long, but long enough
# that a healthy webhook on a slow link still succeeds. The actual
# POST runs on a fire-and-forget background thread anyway, so the
# daemon's tick loop is never blocked even if a 5s timeout fires.
POST_TIMEOUT_SECONDS = 5.0


def build_payload(spec: JobSpec, *, hostname: str | None = None) -> dict[str, Any]:
    """Construct the webhook JSON payload for ``spec``.

    The summary string uses ``<job_name>-<jobid>`` when the job has a
    name (v0.5.34), else the bare jobid — same convention as fetch
    destinations and archive filenames so the user sees one consistent
    label everywhere.

    ``hostname`` defaults to :func:`socket.gethostname` if not passed
    (useful in tests for deterministic output).
    """
    host = hostname or socket.gethostname()
    label = spec.dest_dirname  # <name>-<jobid> if name, else <jobid>
    rc_str = ""
    if spec.exit_code is not None:
        sig = signal_name_for_exit(spec.exit_code)
        suffix = f", {sig}" if sig else ""
        rc_str = f" (rc={spec.exit_code}{suffix})"
    summary = f"vq: job {label} finished as {spec.state.value}{rc_str} on {host}"
    return {
        "text": summary,
        "content": summary,
        "job": {
            "id": spec.id,
            "name": spec.job_name,
            "state": spec.state.value,
            "exit_code": spec.exit_code,
            "failure_reason": spec.failure_reason,
            "submitted_at": spec.submitted_at,
            "started_at": spec.started_at,
            "finished_at": spec.finished_at,
            "command": " ".join(spec.command),
            "host": host,
        },
    }


def _do_post(url: str, payload: dict[str, Any], jobid: str) -> None:
    """Synchronous POST. Swallows all errors after logging.

    Separated out so :func:`send_terminal_notification` can wrap it in
    a thread for fire-and-forget AND tests can call it directly with
    a mocked ``urllib.request.urlopen`` for deterministic assertions.
    """
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=POST_TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            if status is not None and status >= 400:
                log.warning(
                    "webhook notification for %s returned HTTP %d",
                    jobid, status,
                )
    except urllib.error.URLError as e:
        log.warning(
            "webhook notification for %s failed: %s",
            jobid, e,
        )
    except Exception as e:
        # Defensive catch-all so an unexpected error from urllib or json
        # can't take down the daemon thread that called us. urllib's
        # exception surface isn't fully covered by URLError.
        log.warning(
            "webhook notification for %s raised %s: %s",
            jobid, type(e).__name__, e,
        )


def send_terminal_notification(
    spec: JobSpec,
    webhook_url: str | None,
    *,
    blocking: bool = False,
    notify_on_states: list[str] | None = None,
) -> None:
    """Send a webhook notification for ``spec``'s terminal state.

    No-ops when:

    * ``webhook_url`` is ``None`` or empty (= notifications disabled
      in config).
    * ``spec.state`` is not in :data:`vq.spec.TERMINAL_STATES`
      (defensive: callers should only invoke after a terminal
      transition, but a silent no-op is safer than firing a spurious
      notification on a misuse).
    * v0.7.17: ``notify_on_states`` is non-empty AND ``spec.state``
      is not in it. Empty / None preserves pre-v0.7.17 behaviour
      (do not filter calls made by terminal paths). The filter is checked with
      ``spec.state.value`` against the lower-case state names the
      config validator normalized.

    ``blocking=False`` (the default): spawn a daemon thread to do the
    POST and return immediately. The daemon tick loop is never
    blocked by HTTP.

    ``blocking=True``: synchronous POST. For tests, and for the rare
    CLI use case where you want to know the POST result inline.
    """
    if not webhook_url:
        return
    if spec.state not in TERMINAL_STATES:
        log.debug(
            "skipping notification for %s: state %s is not terminal",
            spec.id, spec.state,
        )
        return
    if notify_on_states and spec.state.value not in notify_on_states:
        log.debug(
            "skipping notification for %s: state %s not in "
            "notify_on_states filter %s",
            spec.id, spec.state.value, notify_on_states,
        )
        return
    payload = build_payload(spec)
    if blocking:
        _do_post(webhook_url, payload, spec.id)
        return
    threading.Thread(
        target=_do_post,
        args=(webhook_url, payload, spec.id),
        daemon=True,
        name=f"vq-notify-{spec.id}",
    ).start()
