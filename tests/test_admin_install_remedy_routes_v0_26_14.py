"""A refused ``vq admin install`` followed over every route to success (#47).

A provision refused after the clone leaves the checkout, which the next
provision refuses. Three rounds of review found the remedy it printed for the
marker it also used to leave wrong on some route:

* ``vq admin clear-update-marker localhost``, printed by a forwarded install
  (which runs on the far side as ``localhost``) and typed on the driver, where
  it clears the driver's marker;
* "the same host argument you gave ``vq admin install``", wrong for a
  scheduler cluster: install sends the provision to the cluster itself, while
  ``clear-update-marker CLUSTER`` is routed to its ``scheduler_driver``;
* ``clear-update-marker localhost --yes`` on the right machine, which takes
  whichever failed marker is listed first -- another env's, where there is one.

So the refusal clears its own marker and the only remedy is to empty the
checkout on the machine it names. These run the CLI on a "driver" and forward
to separate in-process "hosts", each with its own state and config, through a
double of ``transport.run_remote_vq``, and follow that remedy to an install
that succeeds. No SSH and no network; host names are placeholders.
"""
from __future__ import annotations

import shutil
import socket
import subprocess
from pathlib import Path

import pytest
from click.testing import CliRunner

from tests.test_admin_install_extras_v0_26_10 import decoy_upstream  # noqa: F401
from vq import admin, config, paths, transport
from vq.cli import main

HOSTS = ("driver", "remote-a", "pbs-cluster")


def _config_text(tree: Path, upstream: Path, default_host: str) -> str:
    return (
        f'default_host = "{default_host}"\n\n'
        '[hosts.localhost]\nssh = "localhost"\n\n'
        '[hosts.remote-a]\nssh = "remote-a"\n\n'
        '[hosts.pbs-cluster]\nssh = "pbs-cluster"\nscheduler = "pbs"\n'
        'scheduler_dialect = "torque"\nscratch_root = "/scratch/USER"\n'
        'scheduler_driver = "localhost"\n\n'
        '[programs.demo]\nkind = "venv"\n'
        f'python = "{tree}/checkout/.venv/bin/python"\n'
        f'git_dir = "{tree}/checkout"\n'
        f'upstream = "{upstream}"\n'
        'install_script = "contrib/scripts/install.sh"\n'
        'update_script = "scripts/update.sh"\n'
        'extras = ["web"]\n'
    )


@pytest.fixture
def fleet(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, decoy_upstream):  # noqa: ANN001, ANN201, F811
    """A driver and two hosts, each its own state root and config."""
    upstream, sha = decoy_upstream
    roots: dict[str, tuple[Path, Path]] = {}

    def build(default_host: str) -> None:
        for name in HOSTS:
            root = tmp_path / name
            cfg_dir = root / "cfg"
            cfg_dir.mkdir(parents=True, exist_ok=True)
            (cfg_dir / "config.toml").write_text(
                _config_text(
                    root / "tree", upstream,
                    default_host if name == "driver" else "localhost",
                ),
                encoding="utf-8",
            )
            roots[name] = (root / "state", cfg_dir)

    def on(name: str) -> None:
        state, cfg_dir = roots[name]
        monkeypatch.setenv(paths.ENV_STATE_DIR, str(state))
        monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
        paths.queue_dir().mkdir(parents=True, exist_ok=True)
        paths.jobs_dir().mkdir(parents=True, exist_ok=True)

    ran_on: list[str] = []

    def remote(host_cfg, *args, stdin_data=None, **_kw):  # noqa: ANN001, ANN202
        ran_on.append(host_cfg.ssh)
        on(host_cfg.ssh)
        try:
            result = CliRunner().invoke(main, list(args), input=stdin_data)
        finally:
            on("driver")
        if result.exit_code != 0:
            raise transport.RemoteCommandError(
                f"remote vq failed (exit {result.exit_code}): {result.output}",
                returncode=result.exit_code,
                stderr=result.output,
            )
        return subprocess.CompletedProcess(args, 0, result.output, "")

    monkeypatch.setattr(transport, "run_remote_vq", remote)
    monkeypatch.setenv("VQ_ADMIN_TOKEN", "")

    def markers(name: str) -> list[list[str]]:
        on(name)
        try:
            return [m.envs for m in admin.read_admin_update_markers()]
        finally:
            on("driver")

    class Fleet:
        pass

    f = Fleet()
    f.build, f.on, f.ran_on, f.markers, f.sha = build, on, ran_on, markers, sha
    f.tree = lambda name: tmp_path / name / "tree"
    f.cfg = lambda name: roots[name][1]
    return f


def _rewrite_install_script(fleet, name: str, install_script: str) -> None:  # noqa: ANN001
    cfg = fleet.cfg(name) / "config.toml"
    text = cfg.read_text(encoding="utf-8")
    cfg.write_text(
        text.replace(
            'install_script = "contrib/scripts/install.sh"',
            f'install_script = "{install_script}"',
        ),
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    ("default_host", "install_host", "target"),
    [
        ("localhost", "remote-a", "remote-a"),
        ("remote-a", None, "remote-a"),
        ("localhost", "pbs-cluster", "pbs-cluster"),
    ],
    ids=["explicit-remote", "default-remote", "scheduler-cluster"],
)
def test_the_printed_remedy_leads_to_an_install_that_succeeds(
    fleet, default_host: str, install_host: str | None, target: str,  # noqa: ANN001
) -> None:
    fleet.build(default_host)
    fleet.on("driver")
    argv = ["admin", "install", "demo"]
    argv += [install_host] if install_host else []
    argv += ["--expected-sha", fleet.sha]

    refused = CliRunner().invoke(main, argv)

    assert refused.exit_code != 0
    assert fleet.ran_on == [target]
    said = " ".join(refused.output.split())
    checkout = fleet.tree(target) / "checkout"
    assert (
        f"cloned into {checkout} on {socket.gethostname()} when it refused"
    ) in said
    assert f"first empty {checkout} there" in said
    assert fleet.markers(target) == [], "no marker, on the machine it ran on"
    assert fleet.markers("driver") == []

    # Followed literally: empty the checkout where it says; then the first
    # way out the refusal names (point install_script at scripts/install.sh).
    for child in checkout.iterdir():
        if child.is_dir() and not child.is_symlink():
            shutil.rmtree(child)
        else:
            child.unlink()
    _rewrite_install_script(fleet, target, "scripts/install.sh")
    fleet.ran_on.clear()

    installed = CliRunner().invoke(main, argv)

    assert installed.exit_code == 0, installed.output
    assert fleet.ran_on == [target]
    assert (checkout / "installer-args").read_text(encoding="utf-8") == (
        "--extras web"
    )
