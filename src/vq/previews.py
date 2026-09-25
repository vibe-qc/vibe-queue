"""Calculation previews from a bounded output tail (#65, increment V0).

A preview answers "what is this job computing, and how is it going" without
touching the job's workdir beyond one bounded read of output it already
writes. This module is pure: no console, no daemon, no network.

* :func:`read_tail` reads at most ``max_bytes`` from the end of one file and
  drops the partial first line, so a parser never sees half a record.
* :func:`preview` picks a parser from the program name and returns a JSON-
  ready dict with the ``vq.calc-preview/1`` schema, or ``None`` when no parser
  applies. Every series is capped.

Parsers are program-specific, the envelope is not. Fields a parser cannot
find in the tail are ``None`` (a long job's header scrolls out of the tail);
nothing is guessed. Parsers: vibe-qc (V0) and ORCA (V3).
"""
from __future__ import annotations

import os
import re
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

SCHEMA = "vq.calc-preview/1"
MAX_TAIL_BYTES = 256 * 1024
MAX_SERIES = 200
"""Points kept per series: the most recent ones."""

_ELEMENTS = (
    "H", "He", "Li", "Be", "B", "C", "N", "O", "F", "Ne", "Na", "Mg", "Al", "Si", "P",
    "S", "Cl", "Ar", "K", "Ca", "Sc", "Ti", "V", "Cr", "Mn", "Fe", "Co", "Ni", "Cu",
    "Zn", "Ga", "Ge", "As", "Se", "Br", "Kr", "Rb", "Sr", "Y", "Zr", "Nb", "Mo", "Tc",
    "Ru", "Rh", "Pd", "Ag", "Cd", "In", "Sn", "Sb", "Te", "I", "Xe", "Cs", "Ba", "La",
    "Ce", "Pr", "Nd", "Pm", "Sm", "Eu", "Gd", "Tb", "Dy", "Ho", "Er", "Tm", "Yb", "Lu",
    "Hf", "Ta", "W", "Re", "Os", "Ir", "Pt", "Au", "Hg", "Tl", "Pb", "Bi", "Po", "At",
    "Rn", "Fr", "Ra", "Ac", "Th", "Pa", "U", "Np", "Pu", "Am", "Cm", "Bk", "Cf", "Es",
    "Fm", "Md", "No", "Lr",
)


def read_tail(path: Path, max_bytes: int = MAX_TAIL_BYTES) -> str:
    """The last ``max_bytes`` of ``path`` as text, without a partial first line."""
    with path.open("rb") as stream:
        size = os.fstat(stream.fileno()).st_size
        start = max(0, size - max_bytes)
        at_line_start = start == 0
        if not at_line_start:
            stream.seek(start - 1)
            at_line_start = stream.read(1) == b"\n"
        stream.seek(start)
        data = stream.read(max_bytes)
    text = data.decode("utf-8", "replace")
    if not at_line_start:
        newline = text.find("\n")
        text = text[newline + 1:] if newline >= 0 else ""
    return text


def hill_formula(atomic_numbers: list[int]) -> str | None:
    """Hill-order formula (C, H, then alphabetical; alphabetical without C)."""
    symbols: list[str] = []
    for z in atomic_numbers:
        if not 1 <= z <= len(_ELEMENTS):
            return None
        symbols.append(_ELEMENTS[z - 1])
    counts = Counter(symbols)
    if not counts:
        return None
    order = (["C", "H"] if "C" in counts else []) + sorted(
        s for s in counts if not ("C" in counts and s in {"C", "H"})
    )
    return "".join(f"{s}{counts[s] if counts[s] > 1 else ''}" for s in order)


_FLOAT = r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
_JOB = re.compile(r"^\s*Job:\s+(?P<method>.+?)\s+basis=(?P<basis>\S+)\s*$")
_IUPAC = re.compile(r"^\s*IUPAC name:\s*(?P<name>.+?)\s*$")
_ATOMS_HEADER = re.compile(r"^\s*Atoms \((?:bohr|angstrom|Angstrom)\)")
_ATOM_ROW = re.compile(rf"^\s*\d+\s+Z=\s*(?P<z>\d+)\s+{_FLOAT}\s+{_FLOAT}\s+{_FLOAT}\s*$")
_PERIODIC = re.compile(r"^\s*dimensionality\s*=\s*(?P<dim>[0-3])D\s*$")
_SCF_HEADER = re.compile(r"^\s*iter\s+energy \(Ha\)")
_SCF_ROW = re.compile(
    rf"^\s*(?P<iter>\d+)\s+(?P<energy>{_FLOAT})\s+(?P<de>--|{_FLOAT})\s+"
    rf"(?P<residual>{_FLOAT})\s+\d+\s*$"
)
_SCF_DONE = re.compile(rf"converged in (?P<n>\d+) iterations; E = (?P<energy>{_FLOAT}) Ha")
_TOTAL = re.compile(rf"^\s*Total energy\s+(?P<energy>{_FLOAT})\s*$")
_GAP = re.compile(rf"gap:\s*(?P<ha>{_FLOAT}) Ha \((?P<ev>{_FLOAT}) eV\)")
_FATAL = re.compile(r"^\s*FATAL:\s*(?P<message>.+?)\s*$")
MAX_MESSAGE = 300
_OPT_DONE = re.compile(
    rf"(?:Geometry optimization|Atomic relaxation):\s*(?P<n>\d+) iters, "
    rf"E = (?P<energy>{_FLOAT}) Ha, max\|grad\| = (?P<grad>{_FLOAT})"
    r".*?converged=(?P<conv>True|False)"
)


def _vibeqc(text: str) -> dict[str, Any]:
    method = basis = name = None
    periodic: int | None = None
    atoms: list[int] = []
    reading_atoms = False
    scf: list[dict[str, float | int | None]] = []
    steps: list[float] = []
    total = gap = None
    optimisation: dict[str, Any] | None = None
    error: str | None = None
    for line in text.splitlines():
        if (match := _JOB.match(line)) is not None:
            method, basis = match["method"].strip(), match["basis"]
            atoms, reading_atoms = [], False
            continue
        if (match := _IUPAC.match(line)) is not None:
            name = match["name"]
            continue
        if _ATOMS_HEADER.match(line):
            atoms, reading_atoms = [], True
            continue
        if reading_atoms:
            if (match := _ATOM_ROW.match(line)) is not None:
                atoms.append(int(match["z"]))
                continue
            if set(line.strip()) <= {"-"}:
                continue
            reading_atoms = False
        if (match := _PERIODIC.match(line)) is not None:
            periodic = int(match["dim"])
            continue
        if _SCF_HEADER.match(line):
            scf = []  # a new SCF (for example the next optimisation step)
            continue
        if (match := _SCF_ROW.match(line)) is not None:
            scf.append({
                "iter": int(match["iter"]),
                "energy": float(match["energy"]),
                "delta": None if match["de"] == "--" else float(match["de"]),
                "residual": float(match["residual"]),
            })
            continue
        if (match := _SCF_DONE.search(line)) is not None:
            steps.append(float(match["energy"]))
            continue
        if (match := _TOTAL.match(line)) is not None:
            total = float(match["energy"])
            continue
        if (match := _GAP.search(line)) is not None:
            gap = float(match["ev"])
            continue
        if (match := _FATAL.match(line)) is not None:
            error = match["message"][:MAX_MESSAGE]
            continue
        if (match := _OPT_DONE.search(line)) is not None:
            optimisation = {
                "iterations": int(match["n"]),
                "energy": float(match["energy"]),
                "max_gradient": float(match["grad"]),
                "converged": match["conv"] == "True",
            }
    return {
        "method": method,
        "basis": basis,
        "name": name,
        "formula": hill_formula(atoms) if atoms else None,
        "atoms": len(atoms) or None,
        "periodic_dimensions": periodic,
        "scf": scf[-MAX_SERIES:],
        "scf_energies": steps[-MAX_SERIES:],
        "headline": {"total_energy": total, "gap_ev": gap},
        "optimisation": optimisation,
        "error": error,
    }


_ORCA_KEYWORDS = re.compile(r"^\|\s*\d+>\s*!\s*(?P<keywords>.+?)\s*$")
_ORCA_BASIS = re.compile(r"Your calculation utilizes the basis:\s*(?P<basis>\S+)")
_ORCA_COORDS = re.compile(r"^\s*CARTESIAN COORDINATES \(ANGSTROEM\)\s*$")
_ORCA_COORD_ROW = re.compile(rf"^\s*(?P<sym>[A-Z][a-z]?)\s+{_FLOAT}\s+{_FLOAT}\s+{_FLOAT}\s*$")
_ORCA_SCF_ROW = re.compile(
    rf"^\s*(?P<iter>\d+)\s+(?P<energy>-\d+\.\d+)\s+(?P<de>{_FLOAT})\s+"
    rf"(?P<rmsdp>{_FLOAT})\s+{_FLOAT}\s+"
)
_ORCA_FINAL = re.compile(rf"^\s*FINAL SINGLE POINT ENERGY\s+(?P<energy>{_FLOAT})")
_ORCA_CYCLE = re.compile(r"GEOMETRY OPTIMIZATION CYCLE\s+(?P<n>\d+)")
_ORCA_MAX_GRAD = re.compile(rf"^\s*MAX gradient\s+(?P<grad>{_FLOAT})")
_ORCA_ERROR = re.compile(r"ORCA finished by error termination in (?P<where>.+?)\s*$")


def _orca(text: str) -> dict[str, Any]:
    keywords = basis = error = None
    symbols: list[str] = []
    reading = False
    scf: list[dict[str, float | int | None]] = []
    steps: list[float] = []
    cycle: int | None = None
    max_grad: float | None = None
    converged = False
    for line in text.splitlines():
        if keywords is None and (match := _ORCA_KEYWORDS.match(line)) is not None:
            keywords = match["keywords"]
            continue
        if (match := _ORCA_BASIS.search(line)) is not None:
            basis = match["basis"]
            continue
        if _ORCA_COORDS.match(line):
            symbols, reading = [], True
            continue
        if reading:
            if (match := _ORCA_COORD_ROW.match(line)) is not None:
                symbols.append(match["sym"])
                continue
            if set(line.strip()) <= {"-"} and line.strip():
                continue
            reading = False
        if (match := _ORCA_SCF_ROW.match(line)) is not None:
            if int(match["iter"]) == 1:
                scf = []  # a new SCF, e.g. the next geometry cycle
            scf.append({
                "iter": int(match["iter"]),
                "energy": float(match["energy"]),
                "delta": float(match["de"]),
                "residual": float(match["rmsdp"]),
            })
            continue
        if (match := _ORCA_FINAL.match(line)) is not None:
            steps.append(float(match["energy"]))
            continue
        if (match := _ORCA_CYCLE.search(line)) is not None:
            cycle = int(match["n"])
            continue
        if (match := _ORCA_MAX_GRAD.match(line)) is not None:
            max_grad = float(match["grad"])
            continue
        if "HURRAY" in line or "THE OPTIMIZATION HAS CONVERGED" in line:
            converged = True
            continue
        if (match := _ORCA_ERROR.search(line)) is not None:
            error = f"ORCA error termination in {match['where']}"[:MAX_MESSAGE]
    numbers = [_ELEMENTS.index(sym) + 1 for sym in symbols if sym in _ELEMENTS]
    return {
        "method": keywords,
        "basis": basis,
        "name": None,
        "formula": hill_formula(numbers) if numbers and len(numbers) == len(symbols) else None,
        "atoms": len(symbols) or None,
        "periodic_dimensions": None,
        "scf": scf[-MAX_SERIES:],
        "scf_energies": steps[-MAX_SERIES:],
        "headline": {"total_energy": steps[-1] if steps else None, "gap_ev": None},
        "optimisation": None if cycle is None else {
            "iterations": cycle,
            "energy": steps[-1] if steps else None,
            "max_gradient": max_grad,
            "converged": converged,
        },
        "error": error,
    }


_PARSERS: dict[str, Callable[[str], dict[str, Any]]] = {"vibe-qc": _vibeqc, "orca": _orca}


def parser_for(program: str | None) -> str | None:
    """The parser name for a registered program name, or ``None``."""
    if not program:
        return None
    key = program.lower().replace("_", "-")
    if key.startswith(("vibeqc", "vibe-qc")):
        return "vibe-qc"
    if key.startswith("orca"):
        return "orca"
    return None


def preview(program: str | None, text: str) -> dict[str, Any] | None:
    """A ``vq.calc-preview/1`` dict for ``text``, or ``None`` without a parser."""
    parser = parser_for(program)
    if parser is None:
        return None
    return {"schema": SCHEMA, "parser": parser, **_PARSERS[parser](text)}
