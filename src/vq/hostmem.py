"""Live host free-memory sampling for memory-aware ``vq submit auto``.

QC calculations are **memory-bound**: whether a job survives on a box is
decided by available RAM, not CPU load (an idle-CPU box with too little free
memory will OOM a big calculation). So ``vq submit auto`` places by matching
a job's *estimated peak memory* — from vibe-qc's own estimator, carried in
the dry-run ``.system`` manifest (see vibeqc_preflight.py) — against each
host's live free RAM, sampled here at overview-gather time.

Sampled *on* the host (locally, or via the remote forwarder's
``vq overview localhost --json``) so it reflects that box's real headroom,
fresh per gather. Linux ``/proc/meminfo`` (``MemAvailable`` — the kernel's
reclaim-aware estimate — and ``MemTotal``); graceful ``None`` elsewhere
(macOS has no ``/proc/meminfo``). Never raises: a sampling failure degrades
to ``None`` and placement falls back to the core-count proxy.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class HostMem:
    """A point-in-time sample of a host's RAM. Both fields ``None`` when the
    probe is unavailable (off Linux) or failed — consumers treat ``None`` as
    "no RAM signal"."""

    mem_total_mb: int | None = None
    mem_available_mb: int | None = None


def sample_host_mem(meminfo_path: str = "/proc/meminfo") -> HostMem:
    """Sample the local host's total + available RAM (MiB). Linux-only via
    ``/proc/meminfo``; returns an all-``None`` :class:`HostMem` off Linux or
    on any parse failure. Never raises. The path is a parameter only so tests
    can feed a fixture file."""
    out = HostMem()
    try:
        with open(meminfo_path, encoding="ascii") as fh:
            for line in fh:
                # Lines look like: "MemAvailable:   12345678 kB"
                if line.startswith("MemTotal:"):
                    out.mem_total_mb = int(line.split()[1]) // 1024
                elif line.startswith("MemAvailable:"):
                    out.mem_available_mb = int(line.split()[1]) // 1024
                if out.mem_total_mb is not None and out.mem_available_mb is not None:
                    break
    except (OSError, ValueError, IndexError):
        pass
    return out
