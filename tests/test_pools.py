"""[pools] config + vq submit auto pool scoping (v0.11.0)."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from vq import config


def _hosts(*names):
    return {n: config.HostConfig(ssh=n) for n in names}


# --- config schema -------------------------------------------------------

def test_pool_resolves_to_its_hosts():
    cfg = config.Config(
        hosts=_hosts("host_a", "host_b", "host_e"),
        pools={"compute": config.PoolConfig(hosts=["host_a", "host_b"])},
    )
    assert cfg.resolve_pool_hosts("compute") == ["host_a", "host_b"]


def test_no_pool_returns_all_hosts():
    cfg = config.Config(hosts=_hosts("host_a", "host_b"))
    assert sorted(cfg.resolve_pool_hosts(None)) == ["host_a", "host_b"]


def test_default_pool_scopes_bare_auto():
    cfg = config.Config(
        hosts=_hosts("host_a", "host_b", "laptop"),
        pools={"compute": config.PoolConfig(hosts=["host_a", "host_b"])},
        default_pool="compute",
    )
    assert cfg.resolve_pool_hosts(None) == ["host_a", "host_b"]   # laptop excluded
    assert cfg.resolve_pool_hosts("compute") == ["host_a", "host_b"]


def test_unknown_pool_raises_configerror():
    cfg = config.Config(hosts=_hosts("host_a"))
    with pytest.raises(config.ConfigError):
        cfg.resolve_pool_hosts("nope")


def test_pool_with_unknown_host_rejected_at_load():
    with pytest.raises(ValidationError):
        config.Config(
            hosts=_hosts("host_a"),
            pools={"compute": config.PoolConfig(hosts=["host_a", "ghost"])},
        )


def test_empty_pool_rejected():
    with pytest.raises(ValidationError):
        config.Config(hosts=_hosts("host_a"), pools={"c": config.PoolConfig(hosts=[])})


def test_default_pool_must_be_defined():
    with pytest.raises(ValidationError):
        config.Config(hosts=_hosts("host_a"), default_pool="ghostpool")


def test_pools_parse_from_toml(tmp_path, monkeypatch):
    (tmp_path / "config.toml").write_text(
        'default_pool = "compute"\n'
        '[hosts.host_a]\nssh = "host_a"\n'
        '[hosts.host_b]\nssh = "host_b"\n'
        '[pools.compute]\nhosts = ["host_a", "host_b"]\n'
    )
    monkeypatch.setenv("VQ_CONFIG_DIR", str(tmp_path))
    cfg = config.load_config()
    assert cfg.default_pool == "compute"
    assert cfg.resolve_pool_hosts(None) == ["host_a", "host_b"]


# --- vq submit auto pool scoping (cli wiring) ----------------------------

def _ov(host, *, max_cpus=8, running=0):
    from vq import overview as ovmod
    o = ovmod.HostOverview(host=host)
    o.reachable = True
    o.max_cpus = max_cpus
    o.running_cpus = running
    return o


def test_pick_auto_host_respects_explicit_pool(monkeypatch):
    from vq import cli
    from vq import overview as ovmod
    cfg = config.Config(
        hosts=_hosts("host_a", "host_b", "laptop"),
        pools={"compute": config.PoolConfig(hosts=["host_a", "host_b"])},
    )
    # laptop has the MOST free capacity but is NOT in the pool → ignored.
    fakes = {"host_a": _ov("host_a", max_cpus=8), "host_b": _ov("host_b", max_cpus=4),
             "laptop": _ov("laptop", max_cpus=64)}
    monkeypatch.setattr(cli, "is_local_host", lambda h: False)
    monkeypatch.setattr(ovmod, "gather_overview_remote", lambda h, hc, **kw: fakes[h])
    monkeypatch.setattr("vq.host_status.load_down", lambda: {})
    assert cli._pick_auto_host(cfg, job_cpus=2, pool="compute") == "host_a"


def test_pick_auto_host_uses_default_pool(monkeypatch):
    from vq import cli
    from vq import overview as ovmod
    cfg = config.Config(
        hosts=_hosts("host_a", "laptop"),
        pools={"compute": config.PoolConfig(hosts=["host_a"])},
        default_pool="compute",
    )
    fakes = {"host_a": _ov("host_a", max_cpus=8), "laptop": _ov("laptop", max_cpus=64)}
    monkeypatch.setattr(cli, "is_local_host", lambda h: False)
    monkeypatch.setattr(ovmod, "gather_overview_remote", lambda h, hc, **kw: fakes[h])
    monkeypatch.setattr("vq.host_status.load_down", lambda: {})
    # bare auto (pool=None) → default_pool 'compute' → host_a only, laptop excluded
    assert cli._pick_auto_host(cfg, job_cpus=1, pool=None) == "host_a"


def test_pick_auto_host_uses_scheduler_overview_for_scheduler_pool(monkeypatch):
    from vq import cli
    from vq import overview as ovmod

    cfg = config.Config(
        hosts={
            "host_f": config.HostConfig(
                ssh="host_f",
                scheduler="pbs",
                scheduler_dialect="torque",
                scheduler_driver="driver",
                scratch_root="/scratch/USER",
            ),
            "driver": config.HostConfig(ssh="driver"),
        },
        pools={"clusters": config.PoolConfig(hosts=["host_f"])},
    )
    seen: list[str] = []

    def fake_scheduler_overview(host, host_cfg, cfg, **kw):
        seen.append(host)
        return _ov(host)

    def fail_remote(*args, **kwargs):
        raise AssertionError("scheduler host must not be probed as a remote daemon")

    monkeypatch.setattr(cli, "is_local_host", lambda h: False)
    monkeypatch.setattr(ovmod, "gather_scheduler_overview", fake_scheduler_overview)
    monkeypatch.setattr(ovmod, "gather_overview_remote", fail_remote)
    monkeypatch.setattr("vq.host_status.load_down", lambda: {})

    assert cli._pick_auto_host(cfg, job_cpus=1, pool="clusters") == "host_f"
    assert seen == ["host_f"]
