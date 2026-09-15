"""hostmem.py free-RAM sampling + memory-aware recommend_host + the dry-run
manifest estimate parse (v0.11.0 — the memory-aware half of `vq submit auto`,
replacing the load-based ranking: QC jobs are memory-bound, so RAM, not CPU
load, is the placement signal)."""
from __future__ import annotations

import subprocess

from vq import hostmem, overview, vibeqc_preflight

# --- sample_host_mem -----------------------------------------------------

def test_sample_host_mem_parses_meminfo(tmp_path):
    p = tmp_path / "meminfo"
    p.write_text(
        "MemTotal:       65854844 kB\n"
        "MemFree:         1000000 kB\n"
        "MemAvailable:   33554432 kB\n"     # exactly 32 GiB
        "Buffers:          123456 kB\n"
    )
    hm = hostmem.sample_host_mem(str(p))
    assert hm.mem_total_mb == 65854844 // 1024
    assert hm.mem_available_mb == 32768


def test_sample_host_mem_missing_available(tmp_path):
    p = tmp_path / "meminfo"
    p.write_text("MemTotal: 1048576 kB\nMemFree: 500000 kB\n")   # no MemAvailable
    hm = hostmem.sample_host_mem(str(p))
    assert hm.mem_total_mb == 1024
    assert hm.mem_available_mb is None


def test_sample_host_mem_absent_file(tmp_path):
    hm = hostmem.sample_host_mem(str(tmp_path / "nope"))
    assert hm.mem_total_mb is None and hm.mem_available_mb is None


# --- memory-aware recommend_host -----------------------------------------

def _ov(host, *, running=0, pending=0, max_cpus=None, idle=0,
        mem_avail=None, reachable=True, admin_down=False):
    o = overview.HostOverview(host=host)
    o.reachable = reachable
    o.admin_down = admin_down
    o.running_cpus = running
    o.pending_cpus = pending
    o.max_cpus = max_cpus
    o.idle_seconds = idle
    o.mem_available_mb = mem_avail
    return o


def test_recommend_picks_host_that_fits_ram():
    # 'big_cpu' has far more free cores but only 8 GiB free; 'roomy' has
    # fewer cores but 64 GiB. A 32 GiB job fits in RAM only on 'roomy'.
    big_cpu = _ov("big_cpu", max_cpus=64, mem_avail=8000)
    roomy = _ov("roomy", max_cpus=8, mem_avail=64000)
    assert overview.recommend_host(
        [big_cpu, roomy], job_cpus=2, job_mem_mb=32000
    ) == "roomy"


def test_ram_fit_beats_core_fit():
    # 'cores' fits cpus but not RAM (OOM risk); 'ram' fits RAM but its cores
    # are saturated (the job will queue). RAM-fit is primary → 'ram' wins:
    # a job that queues for cores beats one that OOMs.
    cores = _ov("cores", max_cpus=32, running=0, mem_avail=4000)
    ram = _ov("ram", max_cpus=4, running=4, mem_avail=64000)
    assert overview.recommend_host(
        [cores, ram], job_cpus=8, job_mem_mb=32000
    ) == "ram"


def test_recommend_prefers_most_free_ram_when_both_fit():
    a = _ov("a", max_cpus=16, mem_avail=40000)
    b = _ov("b", max_cpus=16, mem_avail=64000)
    assert overview.recommend_host([a, b], job_cpus=2, job_mem_mb=16000) == "b"


def test_unknown_ram_not_excluded_but_deprioritized():
    # 'known' reports 64 GiB free; 'blind' reports nothing. The known-fitting
    # host wins, but blind is NOT filtered out (it might fit) — just ranked
    # lower (0 headroom), and is still picked when it's the only option.
    known = _ov("known", max_cpus=8, mem_avail=64000)
    blind = _ov("blind", max_cpus=8, mem_avail=None)
    assert overview.recommend_host(
        [known, blind], job_cpus=2, job_mem_mb=16000
    ) == "known"
    assert overview.recommend_host(
        [blind], job_cpus=2, job_mem_mb=16000
    ) == "blind"


def test_no_estimate_falls_back_to_core_ranking():
    # job_mem_mb=None → memory imposes no constraint; most-free-cores wins
    # (the capacity picker, unchanged from pre-memory behaviour).
    small = _ov("small", max_cpus=4, mem_avail=8000)
    big = _ov("big", max_cpus=32, mem_avail=8000)
    assert overview.recommend_host([small, big], job_cpus=2) == "big"


# --- overview JSON round-trip + text -------------------------------------

def test_overview_json_round_trips_mem():
    ov = overview.HostOverview(host="host_a")
    ov.mem_total_mb, ov.mem_available_mb = 64000, 48000
    payload = overview.format_overview_json(ov)
    assert payload["mem_total_mb"] == 64000
    assert payload["mem_available_mb"] == 48000
    back = overview._overview_from_json("host_a", payload)
    assert (back.mem_total_mb, back.mem_available_mb) == (64000, 48000)


def test_overview_json_pre_sample_host_reads_none():
    # A host that never sampled (key absent) → None, existing fields parse.
    payload = {"host": "old", "running_cpus": 2, "pending_cpus": 0}
    back = overview._overview_from_json("old", payload)
    assert back.mem_total_mb is None
    assert back.mem_available_mb is None
    assert back.running_cpus == 2


def test_overview_text_shows_mem_line():
    ov = overview.HostOverview(host="host_b", vq_version="0.12.0")
    ov.mem_total_mb, ov.mem_available_mb = 32768, 16384    # 32 / 16 GiB
    text = overview.format_overview_text(ov)
    assert "mem free:" in text
    assert "16.0 / 32.0 GiB" in text
    assert "50%" in text


# --- preflight estimate parse + flag -------------------------------------

def _manifest(tmp_path, body: str):
    p = tmp_path / "out.system"
    p.write_text(body)
    return p


def test_preflight_parses_estimate_bytes(tmp_path):
    m = _manifest(
        tmp_path,
        '[outputs]\nstatus = "dry_run"\n'
        '[plan]\nmethod = "rks"\nbasis = "def2-svp"\nstem = "out"\nfiles = []\n'
        "[memory]\nestimate_bytes = 12884901888\n",   # 12 GiB
    )
    res = vibeqc_preflight._parse_manifest_for_outputs(tmp_path, m)
    assert res is not None
    assert res.estimate_bytes == 12884901888
    assert res.method == "rks"


def test_preflight_estimate_absent_is_none(tmp_path):
    m = _manifest(
        tmp_path,
        '[outputs]\nstatus = "dry_run"\n'
        '[plan]\nmethod = "rhf"\nbasis = "sto-3g"\nstem = "out"\nfiles = []\n',
    )
    res = vibeqc_preflight._parse_manifest_for_outputs(tmp_path, m)
    assert res is not None
    assert res.estimate_bytes is None


def test_preflight_with_estimate_sets_env(tmp_path, monkeypatch):
    captured = {}

    def _fake_run(cmd, **kw):
        captured["env"] = kw.get("env", {})
        raise subprocess.TimeoutExpired(cmd, 1)   # bail cleanly → returns None

    monkeypatch.setattr(vibeqc_preflight.subprocess, "run", _fake_run)
    vibeqc_preflight.vibeqc_dry_run_preflight(
        tmp_path, ["python", "x.py"], with_estimate=True
    )
    assert captured["env"].get("VIBEQC_DRY_RUN") == "1"
    assert captured["env"].get("VIBEQC_DRY_RUN_ESTIMATE") == "1"


def test_preflight_without_estimate_omits_flag(tmp_path, monkeypatch):
    captured = {}

    def _fake_run(cmd, **kw):
        captured["env"] = kw.get("env", {})
        raise subprocess.TimeoutExpired(cmd, 1)

    monkeypatch.setattr(vibeqc_preflight.subprocess, "run", _fake_run)
    vibeqc_preflight.vibeqc_dry_run_preflight(tmp_path, ["python", "x.py"])
    assert captured["env"].get("VIBEQC_DRY_RUN") == "1"
    assert "VIBEQC_DRY_RUN_ESTIMATE" not in captured["env"]
