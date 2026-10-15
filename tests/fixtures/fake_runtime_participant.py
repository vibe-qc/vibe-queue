"""A scripted stand-in for a site's runtime participant adapter (#72 piece 2).

Usage: ``python fake_runtime_participant.py STATE_DIR``. It reads one request
from stdin and appends it to ``STATE_DIR/requests.jsonl``. It then answers
from ``STATE_DIR/table.json``, which maps an ``op`` to a list of scripted
answers consumed in order. Once a list is empty (or has no entry) the answer
is ``ok`` with an empty receipt. A scripted answer is a dict with any of:

* ``outcome``: ``ok``/``refused``/``pending``/``unknown``
* ``receipt``: the receipt object for ``ok``
* ``errors``: a list of strings
* ``adapter``: override the adapter digest
* ``op_id``: answer for another op_id
* ``raw``: emit this exact text instead of a document
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ADAPTER = "a" * 64


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def main() -> int:
    state = Path(sys.argv[1])
    request = json.loads(sys.stdin.read())
    with (state / "requests.jsonl").open("a") as log:
        log.write(json.dumps(request, sort_keys=True) + "\n")
    table_path = state / "table.json"
    table = json.loads(table_path.read_text()) if table_path.exists() else {}
    scripted = table.get(request["op"]) or []
    answer = scripted.pop(0) if scripted else {}
    table_path.write_text(json.dumps(table))
    if "raw" in answer:
        sys.stdout.write(answer["raw"])
        return 0
    outcome = answer.get("outcome", "ok")
    receipt = answer.get("receipt", {"op": request["op"]}) if outcome == "ok" else None
    document = {
        "schema": request["schema"],
        "op": request["op"],
        "op_id": answer.get("op_id", request["op_id"]),
        "outcome": outcome,
        "receipt": receipt,
        "receipt_sha256": hashlib.sha256(canonical(receipt)).hexdigest() if receipt else None,
        "errors": answer.get("errors", []),
        "participant": {
            "kind": "scheduler-packed",
            "adapter_sha256": answer.get("adapter", ADAPTER),
            "interpreter": "/site/bin/python3",
        },
    }
    sys.stdout.write(canonical(document).decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
