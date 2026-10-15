"""Measure one benchmark run and write its report (#162).

``vq benchmark measure`` wraps the program a benchmark job runs. It passes
the program's output through unchanged, keeps its exit code, and writes the
``vq-benchmark-run.json`` report (``vq.benchmark-run-report/1``) that
``vq benchmark run`` collects:

* **steady and startup time.** Startup is the time to the first SCF iteration
  line of the output, steady time the rest of the run. A program that prints
  no SCF iteration line is timed as steady throughout and reports no startup.
* **values, SCF iterations and convergence**, read from the output by the
  calculation-preview parsers (vibe-qc, ORCA): the total energy and, where
  printed, the gap.
* **the runtime it ran.** With ``--runtime-root`` the wrapper reads a portable
  runtime deployment's verified current selection, expands ``{runtime}`` in
  the command to that generation's payload, and reports the selection's
  artifact ID, so the digest names the code that ran. A selection that
  changes during the run is refused. ``--runtime-digest`` instead records an
  identity the operator declares, for a runtime not deployed as a bundle.
* **the CPU model**, where the platform exposes one.
"""

from __future__ import annotations

import json
import os
import platform
import re
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

from vq import previews
from vq import runtime_bundle as rb

REPORT_NAME = "vq-benchmark-run.json"
REPORT_SCHEMA = "vq.benchmark-run-report/1"
RUNTIME_PLACEHOLDER = "{runtime}"

#: The first SCF iteration line, per parser: where startup ends.
_FIRST_ITERATION = {
    "vibe-qc": previews._SCF_ROW,
    "orca": previews._ORCA_SCF_ROW,
}
_SCF_RESULT = {"vibe-qc": previews._SCF_DONE, "orca": previews._ORCA_FINAL}


class MeasureError(ValueError):
    """The run cannot be measured as asked."""


def selected_runtime(root: Path) -> tuple[str, Path]:
    """The verified current generation of a runtime deployment: its ID and payload."""
    selection = rb._selection(root)
    current = selection.get("current")
    if not isinstance(current, str):
        raise MeasureError(f"{root} has no selected runtime")
    generation = root / "generations" / current
    try:
        manifest = rb.BundleManifest.model_validate_json(
            rb._read_json(generation / "manifest.json"))
        receipt = json.loads(rb._read_json(generation / "verification.json"))
    except (OSError, ValueError) as exc:
        raise MeasureError(f"selected runtime {current[:12]} is not a verified deployment: "
                           f"{exc}") from None
    if manifest.artifact_id != current or not (isinstance(receipt, dict)
                                               and receipt.get("verified") is True):
        raise MeasureError(f"selected runtime {current[:12]} is not a verified deployment")
    return current, generation / "payload"


def cpu_model() -> str | None:
    """The CPU model name the platform reports, or None."""
    try:
        if sys.platform.startswith("linux"):
            for line in Path("/proc/cpuinfo").read_text(errors="replace").splitlines():
                key, _, value = line.partition(":")
                if key.strip() in {"model name", "Processor", "cpu model"} and value.strip():
                    return value.strip()[:256]
        elif sys.platform == "darwin":
            out = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                                 stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                 timeout=5, check=False)
            if out.returncode == 0 and out.stdout.strip():
                return out.stdout.strip()[:256]
    except (OSError, subprocess.SubprocessError):
        return None
    return platform.processor()[:256] or None


def _summary(program: str, text: str) -> dict[str, Any]:
    preview = previews.preview(program, text) or {}
    headline = preview.get("headline") or {}
    steps = preview.get("scf_energies") or []
    scf = preview.get("scf") or []
    optimisation = preview.get("optimisation")
    energy = headline.get("total_energy")
    if energy is None and steps:
        energy = steps[-1]
    values = {}
    if isinstance(energy, (int, float)):
        values["total_energy"] = float(energy)
    if isinstance(headline.get("gap_ev"), (int, float)):
        values["gap_ev"] = float(headline["gap_ev"])
    converged = bool(steps or energy is not None) and preview.get("error") is None
    if isinstance(optimisation, dict) and optimisation.get("converged") is False:
        converged = False
    return {
        "values": values,
        "converged": converged,
        "scf_iterations": scf[-1]["iter"] if scf else None,
    }


def measure(program: str, command: list[str], *, runtime_root: Path | None = None,
            runtime_digest: str | None = None, report: Path = Path(REPORT_NAME),
            tail_bytes: int = previews.MAX_TAIL_BYTES) -> int:
    """Run ``command``, write its report, and return its exit code."""
    parser = previews.parser_for(program)
    if parser is None:
        raise MeasureError(f"no output parser for program {program!r}; supported: "
                           "vibe-qc, orca")
    if (runtime_root is None) == (runtime_digest is None):
        raise MeasureError("give exactly one of --runtime-root and --runtime-digest")
    if runtime_digest is not None and not re.fullmatch(r"[0-9a-f]{64}", runtime_digest):
        raise MeasureError("--runtime-digest must be a full SHA256 digest")
    if runtime_root is not None:
        runtime_digest, payload = selected_runtime(runtime_root)
        command = [part.replace(RUNTIME_PLACEHOLDER, str(payload)) for part in command]
    elif any(RUNTIME_PLACEHOLDER in part for part in command):
        raise MeasureError(f"{RUNTIME_PLACEHOLDER} needs --runtime-root")
    first_iteration = _FIRST_ITERATION[parser]

    started = time.monotonic()
    first_seen: float | None = None
    scf_complete = False
    fatal_seen = False
    tail: deque[str] = deque()
    kept = 0
    try:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stdin=subprocess.DEVNULL,
                                text=True, errors="replace", bufsize=1)
    except OSError as exc:
        raise MeasureError(f"cannot run {command[0]}: {exc.strerror or exc}") from None
    assert proc.stdout is not None
    for line in proc.stdout:
        sys.stdout.write(line)
        iteration = first_iteration.match(line)
        if iteration is not None:
            if first_seen is None:
                first_seen = time.monotonic()
            if int(iteration["iter"]) == 1:
                scf_complete = False
        if ((parser == "vibe-qc" and previews._SCF_HEADER.match(line))
                or (parser == "orca" and previews._ORCA_CYCLE.search(line))):
            scf_complete = False
        if _SCF_RESULT[parser].search(line):
            scf_complete = True
        if ((parser == "vibe-qc" and previews._FATAL.match(line))
                or (parser == "orca" and previews._ORCA_ERROR.search(line))):
            fatal_seen = True
        tail.append(line)
        kept += len(line)
        while kept > tail_bytes and len(tail) > 1:
            kept -= len(tail.popleft())
    sys.stdout.flush()
    returncode = proc.wait()
    ended = time.monotonic()

    if runtime_root is not None and selected_runtime(runtime_root)[0] != runtime_digest:
        raise MeasureError("the runtime selection changed during the run; it is not "
                           "reported as either runtime")
    summary = _summary(program, "".join(tail))
    body: dict[str, Any] = {
        "schema_name": REPORT_SCHEMA,
        "runtime_digest": runtime_digest,
        # Keep stream evidence independently of the bounded preview tail:
        # an earlier result or a printed energy cannot finish a later SCF,
        # and scrolling a fatal error out of the tail cannot clear it.
        "converged": (summary["converged"] and returncode == 0 and first_seen is not None
                      and scf_complete and not fatal_seen),
        "steady_seconds": round(max(ended - (first_seen or started), 1e-6), 6),
        "values": summary["values"],
    }
    if first_seen is not None:
        body["startup_seconds"] = round(first_seen - started, 6)
    if summary["scf_iterations"] is not None:
        body["scf_iterations"] = summary["scf_iterations"]
    if (model := cpu_model()) is not None:
        body["cpu_model"] = model
    temporary = report.with_name(f".{report.name}.tmp")
    temporary.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, report)
    return returncode
