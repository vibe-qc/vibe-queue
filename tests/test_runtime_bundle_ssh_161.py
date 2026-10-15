"""#161: fetching a pinned runtime bundle from another host's store over SSH.

`vq runtime-bundle fetch --from-host HOST --remote-store PATH` reads the
published store layout on a configured host through vq's ssh command. The
gates are the HTTPS fetch's: the pin before any connection, offline reuse,
the manifest authenticated before the archive is requested, and the normal
publication checks. The tests stand in a local `ssh` that runs the remote
command with `sh -c`, so the quoting, the `head -c` cap and the streaming
are real.
"""
from __future__ import annotations

import json
import os
import shlex
import sys
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

from vq import runtime_bundle as rb
from vq import transport
from vq.cli import main

pytestmark = pytest.mark.no_autopatch_lifecycle_lock

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle import _profile, _trust, bundle  # noqa: E402,F401

STAND_IN = r'''
import json, os, subprocess, sys
with open(os.environ["VQ_SSH_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\n")
mode = os.environ.get("VQ_SSH_MODE", "ok")
if mode == "stall":
    with open(os.environ["VQ_SSH_PIDFILE"], "w") as handle:
        handle.write(str(os.getpid()))
    import time
    time.sleep(60)
sys.exit(subprocess.call(["sh", "-c", sys.argv[-1]]))
'''


@pytest.fixture
def ssh(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[list[str], Path]:
    script = tmp_path / "ssh_stand_in.py"
    script.write_text(STAND_IN)
    log = tmp_path / "ssh.jsonl"
    log.write_text("")
    monkeypatch.setenv("VQ_SSH_LOG", str(log))
    return [sys.executable, str(script)], log


def _commands(log: Path) -> list[str]:
    return [json.loads(line)[-1] for line in log.read_text().splitlines()]


@pytest.fixture
def remote(tmp_path: Path, bundle) -> tuple[Path, rb.BundleManifest, Path]:  # noqa: F811
    output, manifest, trust = bundle
    # A store path with a space and a quote, so the remote command's quoting
    # is exercised, not assumed.
    store = tmp_path / "builder store's root"
    rb.publish(output, store, trust)
    return store, manifest, trust


def test_a_published_store_is_fetched_over_ssh_and_deploys(
    tmp_path: Path, ssh: tuple[list[str], Path], remote,
) -> None:
    argv, log = ssh
    source, manifest, trust = remote
    local = tmp_path / "local-store"
    assert rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, local, trust,
                             max_archive_bytes=1024**2) == manifest
    base = f"{source}/artifacts/{manifest.artifact_id}"
    assert _commands(log) == [
        f"head -c {2 * 1024**2 + 1} -- {shlex.quote(base + '/manifest.json')}",
        f"head -c {1024**2 + 1} -- {shlex.quote(base + '/bundle.tar')}",
    ]
    receipt = rb.deploy(local, manifest.artifact_id, tmp_path / "target", trust, _profile())
    assert receipt["verified"] and receipt["target_builds"] == 0

    # Already published here: reused without any connection.
    log.write_text("")
    assert rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, local, trust) == manifest
    assert _commands(log) == []


def test_nothing_is_requested_for_an_unpinned_artifact_or_an_unsafe_path(
    tmp_path: Path, ssh: tuple[list[str], Path], remote,
) -> None:
    argv, log = ssh
    source, manifest, _ = remote
    with pytest.raises(rb.BundleError, match="not pinned"):
        rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, tmp_path / "local",
                          _trust(tmp_path))
    for path in ("relative/store", "/store/../etc", "/store\nrm -rf ~", "/störe"):
        with pytest.raises(rb.BundleError, match="remote store must be an absolute ASCII path"):
            rb.fetch_over_ssh(argv, path, manifest.artifact_id, tmp_path / "local",
                              remote[2])
    assert _commands(log) == [] and not (tmp_path / "local").exists()


def test_an_oversized_archive_is_cut_off_and_nothing_is_published(
    tmp_path: Path, ssh: tuple[list[str], Path], remote,
) -> None:
    argv, _ = ssh
    source, manifest, trust = remote
    size = (source / "artifacts" / manifest.artifact_id / "bundle.tar").stat().st_size
    with pytest.raises(rb.BundleError, match="exceeds the configured byte limit"):
        rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, tmp_path / "local", trust,
                          max_archive_bytes=size - 1)
    assert not (tmp_path / "local" / "artifacts" / manifest.artifact_id).exists()


def test_a_missing_or_tampered_remote_artifact_publishes_nothing(
    tmp_path: Path, ssh: tuple[list[str], Path], remote,
) -> None:
    argv, log = ssh
    source, manifest, trust = remote
    remote_dir = source / "artifacts" / manifest.artifact_id
    (remote_dir / "bundle.tar").rename(remote_dir / "bundle.tar.away")
    with pytest.raises(rb.BundleError, match="could not be read") as error:
        rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, tmp_path / "local", trust)
    assert str(source) not in str(error.value)  # the remote's path is not echoed
    (remote_dir / "bundle.tar.away").rename(remote_dir / "bundle.tar")
    with (remote_dir / "bundle.tar").open("ab") as handle:
        handle.write(b"tampered")
    with pytest.raises(rb.BundleError, match="archive digest"):
        rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, tmp_path / "local", trust)
    assert not (tmp_path / "local" / "artifacts" / manifest.artifact_id).exists()


def test_a_manifest_for_another_artifact_stops_before_the_archive(
    tmp_path: Path, ssh: tuple[list[str], Path], remote,
) -> None:
    argv, log = ssh
    source, manifest, trust = remote
    other = "f" * 64
    (source / "artifacts" / other).mkdir()
    (source / "artifacts" / other / "manifest.json").write_bytes(
        (source / "artifacts" / manifest.artifact_id / "manifest.json").read_bytes())
    policy = json.loads(trust.read_text())
    policy["publishers"]["test-builder"].append(other)
    trust.write_text(json.dumps(policy))
    with pytest.raises(rb.BundleError, match="does not match the requested artifact"):
        rb.fetch_over_ssh(argv, str(source), other, tmp_path / "local", trust)
    assert [c.rsplit("/", 1)[1].rstrip("'") for c in _commands(log)] == ["manifest.json"]


def test_a_stalled_transfer_is_stopped_and_its_process_group_killed(
    tmp_path: Path, ssh: tuple[list[str], Path], remote, monkeypatch: pytest.MonkeyPatch,
) -> None:
    argv, _ = ssh
    source, manifest, trust = remote
    pidfile = tmp_path / "ssh.pid"
    monkeypatch.setenv("VQ_SSH_MODE", "stall")
    monkeypatch.setenv("VQ_SSH_PIDFILE", str(pidfile))
    started = time.monotonic()
    with pytest.raises(rb.BundleError, match="stalled"):
        rb.fetch_over_ssh(argv, str(source), manifest.artifact_id, tmp_path / "local", trust,
                          timeout=0.5)
    assert time.monotonic() - started < 10
    pid = int(pidfile.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


class TestCli:
    def test_from_host_uses_the_configured_hosts_ssh(
        self, tmp_path: Path, ssh: tuple[list[str], Path], remote,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from vq import config

        argv, log = ssh
        source, manifest, trust = remote
        monkeypatch.setattr(config, "load_config", lambda: config.Config(
            hosts={"builder": config.HostConfig(ssh="builder.example.invalid")}))
        seen: list[str] = []

        def ssh_base(host_cfg: config.HostConfig) -> list[str]:
            seen.append(host_cfg.ssh)
            return argv

        monkeypatch.setattr(transport, "_ssh_base", ssh_base)
        result = CliRunner().invoke(main, [
            "runtime-bundle", "fetch", manifest.artifact_id, "--from-host", "builder",
            "--remote-store", str(source), "--store", str(tmp_path / "local"),
            "--trust", str(trust), "--max-download-mib", "1",
        ])
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["artifact_id"] == manifest.artifact_id
        assert seen == ["builder.example.invalid"] and len(_commands(log)) == 2

    @pytest.mark.parametrize(("args", "message"), [
        (["--url", "https://a.example.invalid", "--from-host", "b"], "exactly one of"),
        ([], "exactly one of"),
        (["--from-host", "b"], "go together"),
        (["--from-host", "b", "--remote-store", "/s", "--ca-file", "{trust}"],
         "--ca-file applies to --url only"),
    ])
    def test_transport_options_are_checked(self, tmp_path: Path, remote, args: list[str],
                                           message: str) -> None:
        _, manifest, trust = remote
        args = [a.replace("{trust}", str(trust)) for a in args]
        result = CliRunner().invoke(main, [
            "runtime-bundle", "fetch", manifest.artifact_id, *args,
            "--store", str(tmp_path / "local"), "--trust", str(trust)])
        assert result.exit_code == 2 and message in result.output
