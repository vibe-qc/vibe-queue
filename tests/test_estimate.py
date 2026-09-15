"""estimate_python — the automatic peak-memory estimate for `vq submit auto`
(v0.11.0 Inc 3): when no explicit --mem-mb is given, the submit host runs the
job's vibe-qc dry-run (which now emits [memory].estimate_bytes) via the
configured `estimate_python`, and places by RAM-fit. Best-effort: any miss
falls back to core-fit."""
from __future__ import annotations

from vq import cli, config
from vq.vibeqc_preflight import PreflightResult

_PRE = "vq.vibeqc_preflight.vibeqc_dry_run_preflight"   # lazily imported in the helper


def _cfg(estimate_python=None):
    return config.Config(estimate_python=estimate_python)


def _script(tmp_path):
    p = tmp_path / "job.py"
    p.write_text("import vibeqc\n")   # content irrelevant — the preflight is mocked
    return p


# --- config field --------------------------------------------------------

def test_estimate_python_defaults_none():
    assert config.Config().estimate_python is None


def test_estimate_python_parses_from_toml(tmp_path, monkeypatch):
    (tmp_path / "config.toml").write_text('estimate_python = "/venv/bin/python"\n')
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path))
    assert config.load_config().estimate_python == "/venv/bin/python"


# --- _auto_estimate_job_mem_mb: the no-op / fallback paths ----------------

def test_none_when_estimate_python_unset(tmp_path):
    assert cli._auto_estimate_job_mem_mb(_cfg(None), [str(_script(tmp_path))]) is None


def test_none_when_not_a_single_py(tmp_path):
    cfg = _cfg("/venv/bin/python")
    assert cli._auto_estimate_job_mem_mb(cfg, ["a.py", "b.py"]) is None        # not single
    assert cli._auto_estimate_job_mem_mb(cfg, ["just-a-command"]) is None      # not .py
    assert cli._auto_estimate_job_mem_mb(cfg, [str(tmp_path / "gone.py")]) is None  # missing


# --- _auto_estimate_job_mem_mb: the estimate path (preflight mocked) ------

def test_returns_mib_rounded_up(tmp_path, monkeypatch):
    cfg = _cfg("/venv/bin/python")
    script = _script(tmp_path)
    monkeypatch.setattr(
        _PRE,
        lambda ws, cmd, with_estimate=False: PreflightResult(
            estimate_bytes=12 * 1024**3 + 1   # 12 GiB + 1 byte → 12289 MiB
        ),
    )
    assert cli._auto_estimate_job_mem_mb(cfg, [str(script)]) == 12 * 1024 + 1


def test_none_when_no_estimate_emitted(tmp_path, monkeypatch):
    # dry-run ran but vibe-qc emitted no estimate (e.g. a semiempirical/MLIP
    # job) → estimate_bytes is None → fall back to core-fit.
    cfg = _cfg("/venv/bin/python")
    monkeypatch.setattr(
        _PRE, lambda ws, cmd, with_estimate=False: PreflightResult(estimate_bytes=None)
    )
    assert cli._auto_estimate_job_mem_mb(cfg, [str(_script(tmp_path))]) is None


def test_none_when_preflight_fails(tmp_path, monkeypatch):
    cfg = _cfg("/venv/bin/python")
    monkeypatch.setattr(_PRE, lambda ws, cmd, with_estimate=False: None)
    assert cli._auto_estimate_job_mem_mb(cfg, [str(_script(tmp_path))]) is None


def test_none_when_preflight_raises(tmp_path, monkeypatch):
    # never block the submit — an exception in the estimate path is swallowed.
    cfg = _cfg("/venv/bin/python")

    def _boom(ws, cmd, with_estimate=False):
        raise RuntimeError("interpreter exploded")

    monkeypatch.setattr(_PRE, _boom)
    assert cli._auto_estimate_job_mem_mb(cfg, [str(_script(tmp_path))]) is None


def test_runs_with_estimate_flag_on_a_temp_copy(tmp_path, monkeypatch):
    # the dry-run is invoked with `estimate_python`, the basename in a temp
    # workspace (NOT the user's source dir), and with_estimate=True.
    cfg = _cfg("/venv/bin/python")
    script = _script(tmp_path)
    seen = {}

    def _capture(ws, cmd, with_estimate=False):
        seen["ws"] = str(ws)
        seen["cmd"] = list(cmd)
        seen["with_estimate"] = with_estimate
        return PreflightResult(estimate_bytes=1024**3)   # 1 GiB

    monkeypatch.setattr(_PRE, _capture)
    assert cli._auto_estimate_job_mem_mb(cfg, [str(script)]) == 1024
    assert seen["with_estimate"] is True
    assert seen["cmd"] == ["/venv/bin/python", "job.py"]
    assert str(tmp_path) not in seen["ws"]   # ran on a temp copy, not the source dir
