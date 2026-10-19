"""The outbound connector (#164): claim relay work and drive the local broker.

It runs on the broker host and makes only outbound HTTPS requests to a
relay-mode gateway. Each pass:
- claims work items;
- for each submit item, downloads and checks the payload, admits it to the
  local broker under the item's scoped idempotency key, journals the
  gateway-job to broker-job mapping, and only then acknowledges;
- pushes the current view of every job it admitted, which is also its
  heartbeat.

A crash anywhere in a pass is safe:
- an unacknowledged item is claimed again once its lease expires, with a new
  epoch;
- the keyed broker accept returns the job already admitted, so nothing runs
  twice.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import stat
import tomllib
from pathlib import Path
from typing import Any

from vq import broker_service, broker_store, paths
from vq.gateway import jobs as gateway_jobs
from vq.gateway.client import ClientError, Credentials, GatewayClient, Send, check_url
from vq.gateway.keys import _private_dir, _read_private, _write_private


def connector_root() -> Path:
    return paths.state_root() / "connector"


def credentials_path(root: Path | None = None) -> Path:
    return (root or connector_root()) / "credentials.toml"


def save_credentials(url: str, key: str, ca_file: str | None, *, root: Path | None = None
                     ) -> Path:
    directory = _private_dir(root or connector_root())
    lines = [f"url = {json.dumps(url)}", f"key = {json.dumps(key)}"]
    if ca_file:
        lines.append(f"ca_file = {json.dumps(ca_file)}")
    path = directory / "credentials.toml"
    _write_private(path, ("\n".join(lines) + "\n").encode())
    return path


def load_credentials(root: Path | None = None) -> Credentials:
    path = credentials_path(root)
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ClientError("this host has no connector credentials; run "
                          "`vq connector enroll` first") from None
    if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
        raise ClientError(f"connector credentials {path} must be owner-only")
    data = tomllib.loads(path.read_text())
    return Credentials(url=data["url"].rstrip("/"), key=data["key"],
                       ca_file=data.get("ca_file"))


def enroll(url: str, token: str, *, ca_file: str | None = None, send: Send | None = None,
           root: Path | None = None) -> dict[str, Any]:
    """Exchange a one-use token for this host's connector key; store it privately."""
    check_url(url)
    bootstrap = GatewayClient(Credentials(url=url.rstrip("/"), key="", ca_file=ca_file),
                              send=send)
    answer = bootstrap._json("POST", "/v1/connector/enroll", payload={"token": token},
                             retry=False)
    save_credentials(url.rstrip("/"), answer["connector_key"], ca_file, root=root)
    return {"connector_id": answer["connector_id"], "project": answer["project"]}


class Connector:
    def __init__(self, cfg: Any, client: GatewayClient, *, broker_root: Path | None = None,
                 root: Path | None = None) -> None:
        self.cfg = cfg
        self.client = client
        self.broker_root = broker_root
        self.root = root or connector_root()

    def spool_free(self) -> int:
        """Bytes of [connector] spool_mb not used by staged payloads or results."""
        limit = int(getattr(self.cfg.connector, "spool_mb", 4096)) * 1024 * 1024
        used = 0
        for sub in ("spool", "results"):
            directory = self.root / sub
            if directory.exists():
                used += sum(p.stat().st_size for p in directory.rglob("*") if p.is_file())
        return max(0, limit - used)

    # -- journal: gateway job id -> broker job id, written before each ack ------
    def _journal_path(self) -> Path:
        return _private_dir(self.root) / "accepted.json"

    def journal(self) -> dict[str, str]:
        raw = _read_private(self._journal_path(), 16 * 1024 * 1024)
        return json.loads(raw) if raw else {}

    def _remember(self, gw_job_id: str, broker_job_id: str) -> None:
        journal = self.journal()
        if journal.get(gw_job_id) == broker_job_id:
            return
        journal[gw_job_id] = broker_job_id
        _write_private(self._journal_path(), json.dumps(journal, sort_keys=True).encode())

    def run_once(self, *, wait_seconds: float = 0.0) -> dict[str, Any]:
        claimed = self.client._json("POST", "/v1/connector/claim",
                                    payload={"wait_seconds": wait_seconds,
                                             "spool_free_bytes": self.spool_free()})
        items = claimed["items"]
        summary: dict[str, Any] = {"claimed": len(items), "accepted": [], "refused": [],
                                   "deferred": []}
        summary["cancelled"] = []
        for item in items:
            if item.get("kind") == "submit":
                outcome = self._submit(item)
                summary[outcome].append(item["job_id"])
            elif item.get("kind") == "cancel":
                if self._cancel(item):
                    summary["cancelled"].append(item["job_id"])
                else:
                    summary["deferred"].append(item["item_id"])
            elif item.get("kind") == "collect":
                outcome = self._collect(item)
                summary.setdefault("collected" if outcome == "collected" else outcome,
                                   []).append(item["job_id"])
            else:
                summary["deferred"].append(item.get("item_id"))
        summary["logs_pushed"] = self.push_logs(claimed.get("logs_wanted") or [])
        self.report()
        return summary

    LOG_CHUNK = 1024 * 1024

    def push_logs(self, wanted: list[dict[str, Any]]) -> int:
        """Push new output of placed jobs the gateway asked for; returns bytes sent."""
        from vq import broker_relay, transport

        sent = 0
        for entry in wanted:
            try:
                with broker_store.BrokerStore.open(self.broker_root) as store:
                    record = store.get(entry["broker_job_id"])
            except broker_store.BrokerStoreError:
                continue
            if record.state != broker_store.STATE_PLACED:
                continue
            for stream, have in entry["offsets"].items():
                try:
                    text = broker_relay.proxy_text(self.cfg, record, "logs", "localhost",
                                                   "{target}", f"--{stream}", "-n", "0")
                except (transport.RemoteError, broker_store.BrokerStoreError):
                    break
                data = text.encode()
                while have < len(data):
                    chunk = data[have:have + self.LOG_CHUNK]
                    try:
                        have = self.client._json(
                            "PUT", f"/v1/connector/jobs/{entry['job_id']}/logs/{stream}",
                            body=chunk,
                            headers={"Content-Range":
                                     f"bytes {have}-{have + len(chunk) - 1}/{len(data)}"},
                        )["size"]
                    except ClientError as exc:
                        if exc.status == 409:  # a lost response; the gateway already has it
                            break
                        if exc.status == 413:  # its buffer is full; visible to the client
                            break
                        raise
                    sent += len(chunk)
        return sent

    def _submit(self, item: dict[str, Any]) -> str:
        spool = _private_dir(self.root) / "spool"
        spool.mkdir(mode=0o700, exist_ok=True)
        payload = spool / f"{item['upload_id']}.tar"
        try:
            status, _headers, data = self.client._request(
                "GET", f"/v1/connector/uploads/{item['upload_id']}")
            if status != 200:
                return "deferred"  # the lease expires and the item is claimed again
            if len(data) != item["size"] or hashlib.sha256(data).hexdigest() != item["sha256"]:
                self._ack(item, "refused", reason="payload does not match its digest")
                return "refused"
            payload.write_bytes(data)
            os.chmod(payload, 0o600)
            try:
                intent = gateway_jobs.intent_from_request(item["request"])
                record = broker_service.accept(self.cfg, payload, intent,
                                               root=self.broker_root,
                                               idempotency_key=item["scoped_key"])
            except broker_store.BrokerStoreBusy:
                return "deferred"
            except (broker_store.BrokerStoreError, ValueError, KeyError) as exc:
                self._ack(item, "refused", reason=str(exc))
                return "refused"
            # Durable before the acknowledgement: after a crash the next pass
            # still reports this job, and a re-claim replays the keyed accept.
            self._remember(item["job_id"], record.broker_job_id)
            self._ack(item, "accepted", broker_job_id=record.broker_job_id)
            return "accepted"
        except ClientError:
            return "deferred"
        finally:
            with contextlib.suppress(OSError):
                payload.unlink()

    RESULT_CHUNK = 8 * 1024 * 1024

    def _collect(self, item: dict[str, Any]) -> str:
        """Collect locally, push every file by offset, then acknowledge the manifest.

        The local copy is removed once the gateway holds the verified files. A
        job still running is acknowledged as not-finished; an unreachable target
        leaves the item to be claimed again.
        """
        import shutil

        from vq import transport
        from vq.gateway.results import ResultError, ResultStore

        local = ResultStore(self.root)
        try:
            with broker_store.BrokerStore.open(self.broker_root) as store:
                record = store.get(item["broker_job_id"])
            manifest = local.collect(self.cfg, record, byte_budget=item["byte_budget"])
        except ResultError as exc:
            outcome = "not-finished" if exc.status == 409 else "failed"
            self._ack(item, outcome, reason=str(exc))
            return outcome
        except (transport.RemoteError, broker_store.BrokerStoreError, OSError, ValueError):
            return "deferred"
        try:
            for entry in manifest["files"]:
                self._push_file(item["job_id"], entry,
                                local._dir(record.broker_job_id) / "files" / entry["path"])
            body: dict[str, Any] = {"epoch": item["epoch"], "outcome": "collected",
                                    "manifest": {k: manifest[k] for k in
                                                 ("files", "skipped", "target_state")}}
            self.client._json("POST", f"/v1/connector/items/{item['item_id']}/ack",
                              payload=body)
        except ClientError as exc:
            if exc.status == 413:
                self._ack(item, "failed", reason=str(exc))
                shutil.rmtree(local._dir(record.broker_job_id), ignore_errors=True)
                return "failed"
            return "deferred"
        shutil.rmtree(local._dir(record.broker_job_id), ignore_errors=True)
        return "collected"

    def _push_file(self, gw_job_id: str, entry: dict[str, Any], source: Path) -> None:
        import re
        import urllib.parse

        size = entry["size"]
        path = "/".join(urllib.parse.quote(part) for part in entry["path"].split("/"))
        sent = 0
        with source.open("rb") as handle:
            while sent < size or size == 0:
                handle.seek(sent)
                chunk = handle.read(self.RESULT_CHUNK)
                end = sent + len(chunk) - 1
                try:
                    sent = self.client._json(
                        "PUT", f"/v1/connector/jobs/{gw_job_id}/results/{path}", body=chunk,
                        headers={"Content-Range": f"bytes {sent}-{max(end, sent)}/{size}"},
                    )["size"]
                except ClientError as exc:
                    resume = re.search(r"resume at byte (\d+)", str(exc))
                    if exc.status != 409 or resume is None:
                        raise
                    sent = int(resume.group(1))  # a lost response: continue where it is
                if size == 0:
                    return

    def _cancel(self, item: dict[str, Any]) -> bool:
        """Kill locally through the broker, then acknowledge stopped or not-stopped.

        A failure to reach the target leaves the item unacknowledged; its lease
        expires and it is tried again. Nothing is resubmitted.
        """
        from vq import broker_relay, transport

        try:
            _message, _record, stopped = broker_relay._kill(
                self.cfg, item["broker_job_id"], reason=item["reason"], root=self.broker_root)
        except (transport.RemoteError, broker_store.BrokerStoreError):
            return False
        self._ack(item, "stopped" if stopped else "not-stopped")
        return True

    def _ack(self, item: dict[str, Any], outcome: str, *, broker_job_id: str | None = None,
             reason: str | None = None) -> None:
        body: dict[str, Any] = {"epoch": item["epoch"], "outcome": outcome}
        if broker_job_id is not None:
            body["broker_job_id"] = broker_job_id
        if reason is not None:
            body["reason"] = reason
        self.client._json("POST", f"/v1/connector/items/{item['item_id']}/ack", payload=body)

    def report(self) -> int:
        """Push every admitted job's current view; also the connector's heartbeat."""
        views: dict[str, Any] = {}
        journal = self.journal()
        if journal:
            with broker_store.BrokerStore.open(self.broker_root) as store:
                for gw_job_id, broker_job_id in journal.items():
                    try:
                        views[gw_job_id] = gateway_jobs.client_view(store.get(broker_job_id))
                    except broker_store.BrokerStoreError:
                        continue
        return self.client._json("POST", "/v1/connector/report",
                                 payload={"views": views})["stored"]

    def rotate(self) -> None:
        answer = self.client._json("POST", "/v1/connector/rotate", retry=False)
        creds = self.client.credentials
        save_credentials(creds.url, answer["connector_key"], creds.ca_file, root=self.root)
