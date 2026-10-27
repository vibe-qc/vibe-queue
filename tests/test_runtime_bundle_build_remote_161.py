"""#161: a product build run on a remote build host.

`vq runtime-bundle build --host NAME -d SRC ...` checks what it can here,
stages a tar of SRC and the description in the host's home and runs that
host's `vq runtime-bundle build -a TAR ...`; `build-finish --host NAME`
finishes it there. The stand-in for SSH and scp runs the real vq CLI in a
separate "remote" home, where a real daemon runs the real builder.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import config, transport
from vq import runtime_bundle as rb
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_runtime_bundle_build_161 import BUILDER, _run, host  # noqa: E402,F401


@pytest.fixture
def remote(host: dict[str, Any], tmp_path: Path,  # noqa: F811
           monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    (Path(config.config_dir()) / "config.toml").parent.mkdir(parents=True, exist_ok=True)
    (Path(config.config_dir()) / "config.toml").write_text('[hosts.buildhost]\nssh = "b"\n')
    home = tmp_path / "remote-home"
    home.mkdir()
    monkeypatch.chdir(home)
    state: dict[str, Any] = {"calls": [], "home": home, "fail": None}

    def upload_file(host_cfg: Any, local: Path, remote_path: str, **_kw: Any) -> None:
        state["calls"].append(("upload", remote_path))
        if state["fail"] == "upload":
            raise transport.RemoteOutcomeUnknown("scp stopped")
        (home / remote_path).write_bytes(local.read_bytes())

    def run_remote_vq(host_cfg: Any, *argv: str, **kw: Any) -> Any:
        assert host_cfg.ssh == "b" and kw["owned_process_group"] is True
        state["calls"].append(("vq", argv))
        if state["fail"] == "vq":
            raise transport.RemoteOutcomeUnknown("ssh timed out")
        result = CliRunner().invoke(main, list(argv))
        stdout = {"garbled": "not json", "foreign": "{}"}.get(state["fail"], result.stdout)
        if result.exit_code:
            raise transport.RemoteCommandError(
                f"remote vq failed: {result.output.strip()}", returncode=result.exit_code,
                stderr=result.stderr, stdout=stdout)
        return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

    def run_remote_shell(host_cfg: Any, *argv: str, **_kw: Any) -> Any:
        state["calls"].append(("shell", argv))
        assert argv[:2] == ("rm", "-f")
        for name in argv[2:]:
            (home / name).unlink(missing_ok=True)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    monkeypatch.setattr(transport, "upload_file", upload_file)
    monkeypatch.setattr(transport, "run_remote_vq", run_remote_vq)
    monkeypatch.setattr(transport, "run_remote_shell", run_remote_shell)
    return {**host, **state, "state": state}


def _build(remote: dict[str, Any], *extra: str, builder: tuple[str, ...] = ()) -> Any:
    return CliRunner().invoke(main, [
        "runtime-bundle", "build", "--host", "buildhost", "-d", str(remote["source"]),
        "--description", str(remote["description"]), "--build-dir", str(remote["build_dir"]),
        "--output", str(remote["output"]), "--cpus", "2", "--mem-mb", "512",
        "--time-limit", "600", "--name", "remote-build", *extra, "--",
        *(builder or BUILDER)])


def _finish(remote: dict[str, Any]) -> Any:
    return CliRunner().invoke(main, ["runtime-bundle", "build-finish", "--host", "buildhost",
                                     f"{remote['output']}.vq-build.json"])


def _staging(remote: dict[str, Any]) -> list[str]:
    return sorted(p.name for p in remote["home"].iterdir())


def test_a_remote_build_is_built_and_packed_there(remote: dict[str, Any], tmp_path: Path,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    local_tmp = tmp_path / "local-tmp"
    local_tmp.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(local_tmp))
    submitted = _build(remote)
    assert list(local_tmp.iterdir()) == []  # the local tar is gone too
    assert submitted.exit_code == 0, submitted.output
    document = json.loads(submitted.stdout)
    assert (document["schema"], document["host"]) == ("vq.runtime-build-record/1", "buildhost")
    uploads = [c[1] for c in remote["calls"] if c[0] == "upload"]
    tar, description = uploads
    assert tar.startswith(".vq-upload-") and tar.endswith(".tar") and "/" not in tar
    assert description == tar.removesuffix(".tar") + ".build.json"
    vq_call = next(c[1] for c in remote["calls"] if c[0] == "vq")
    assert vq_call == (
        "runtime-bundle", "build", "-a", tar, "--description", description,
        "--build-dir", str(remote["build_dir"]), "--output", str(remote["output"]),
        "--cpus", "2", "--mem-mb", "512", "--time-limit", "600", "--name", "remote-build",
        "--", *BUILDER)
    assert ("shell", ("rm", "-f", tar, description)) in remote["calls"]
    assert _staging(remote) == []  # nothing is left in the host's home

    waiting = _finish(remote)
    assert waiting.exit_code == 3, waiting.output
    assert (json.loads(waiting.stdout)["status"], json.loads(waiting.stdout)["host"]) == (
        "building", "buildhost")
    assert _run(remote, document["job_id"]).value == "completed"
    packed = _finish(remote)
    assert packed.exit_code == 0, packed.output
    manifest = rb.BundleManifest.model_validate_json(
        (remote["output"] / "manifest.json").read_bytes())
    assert json.loads(packed.stdout)["artifact_id"] == manifest.artifact_id
    assert set(manifest.files) == {"bin/probe", "data/value"}


def test_an_archive_build_runs_from_the_unpacked_tree(host: dict[str, Any],  # noqa: F811
                                                      tmp_path: Path) -> None:
    archive = tmp_path / "source.tar"
    with tarfile.open(archive, "w") as tar:
        for child in host["source"].iterdir():
            tar.add(child, arcname=child.name)
    result = CliRunner().invoke(main, [
        "runtime-bundle", "build", "-a", str(archive), "--description", str(host["description"]),
        "--build-dir", str(host["build_dir"]), "--output", str(host["output"]), "--", *BUILDER])
    assert result.exit_code == 0, result.output
    assert _run(host, json.loads(result.stdout)["job_id"]).value == "completed"
    assert (host["build_dir"] / "data" / "value").read_text() == "42\n"


@pytest.mark.parametrize(("args", "message"), [
    (["--host", "localhost"], "leave it out to build here"),
    (["--host", "elsewhere"], "not a configured [hosts.elsewhere]"),
    (["--build-dir", "relative"], "--build-dir must be an absolute path"),
    (["--output", "/abs/out/bundle"], "--output must be outside --build-dir"),
    (["--description", "{bad}"], "validation error"),
    (["--archive", "{tar}"], "exactly one of -d/--directory and -a/--archive"),
    (["--builder-none"], "Missing argument 'BUILDER...'"),
])
def test_a_refused_remote_build_contacts_no_host(remote: dict[str, Any], tmp_path: Path,
                                                 args: list[str], message: str) -> None:
    values = {"--host": "buildhost", "--build-dir": "/abs/out",
              "--output": "/abs/bundle", "--description": str(remote["description"])}
    extra: list[str] = []
    builder = list(BUILDER)
    if args == ["--builder-none"]:
        builder = []
    elif args[0] == "--archive":
        (tmp_path / "x.tar").write_bytes(b"")
        extra = ["-a", str(tmp_path / "x.tar")]
    elif args[1] == "{bad}":
        (tmp_path / "bad.json").write_text("{}")
        values["--description"] = str(tmp_path / "bad.json")
    else:
        values[args[0]] = args[1]
    argv = ["runtime-bundle", "build", "-d", str(remote["source"]), *extra]
    for flag, value in values.items():
        argv += [flag, value]
    result = CliRunner().invoke(main, [*argv, "--", *builder] if builder else argv)
    assert result.exit_code != 0 and message in result.output, result.output
    assert remote["calls"] == []


def test_a_remote_build_needs_a_builder(remote: dict[str, Any]) -> None:
    from vq.runtime_bundle_build import submit_remote_build

    with pytest.raises(rb.BundleError, match="needs the builder's command"):
        submit_remote_build("buildhost", config.load_config().hosts["buildhost"],
                            source=remote["source"], description=remote["description"],
                            build_dir="/abs/out", output="/abs/bundle", builder=[])
    assert remote["calls"] == []


def test_a_build_takes_one_source(host: dict[str, Any], tmp_path: Path) -> None:  # noqa: F811
    from vq.runtime_bundle_build import submit_build

    common = {"description": host["description"], "build_dir": host["build_dir"],
              "output": host["output"], "builder": BUILDER}
    for sources in ({}, {"source": host["source"], "archive": tmp_path / "x.tar"}):
        with pytest.raises(rb.BundleError, match="exactly one of a source directory"):
            submit_build(**common, **sources)


def test_an_archive_cannot_be_sent_to_a_host(remote: dict[str, Any], tmp_path: Path) -> None:
    (tmp_path / "x.tar").write_bytes(b"")
    result = CliRunner().invoke(main, [
        "runtime-bundle", "build", "--host", "buildhost", "-a", str(tmp_path / "x.tar"),
        "--description", str(remote["description"]), "--build-dir", "/abs/out",
        "--output", "/abs/bundle", "--", "true"])
    assert result.exit_code != 0 and "use -d/--directory" in result.output
    assert remote["calls"] == []


def test_a_refusal_on_the_host_is_reported_and_staging_removed(
    remote: dict[str, Any],
) -> None:
    remote["build_dir"].mkdir()  # the host already has this build dir
    result = _build(remote)
    assert result.exit_code == 1 and "--build-dir already exists" in result.output
    assert _staging(remote) == []


@pytest.mark.parametrize("fail", ["vq", "garbled", "foreign"])
def test_an_unknown_outcome_keeps_the_staging_and_a_rerun_is_refused(
    remote: dict[str, Any], fail: str,
) -> None:
    remote["state"]["fail"] = fail
    result = _build(remote)
    assert result.exit_code == 1
    assert "whether buildhost accepted the build is unknown" in result.output
    assert "was kept" in result.output
    tar, description = (c[1] for c in remote["calls"] if c[0] == "upload")
    assert _staging(remote) == sorted([tar, description])
    assert not any(c[0] == "shell" for c in remote["calls"])
    if fail != "vq":  # the build was accepted there: a rerun cannot queue another
        remote["state"]["fail"] = None
        again = _build(remote)
        assert again.exit_code == 1 and "or its build record already exists" in again.output


def test_an_interrupted_upload_removes_the_staging(remote: dict[str, Any]) -> None:
    remote["state"]["fail"] = "upload"
    result = _build(remote)
    assert result.exit_code == 1 and "scp stopped" in result.output
    assert "accepted the build" not in result.output
    assert any(c[0] == "shell" for c in remote["calls"])
    assert not any(c[0] == "vq" for c in remote["calls"])


def test_a_refused_remote_finish_is_an_error(remote: dict[str, Any]) -> None:
    document = json.loads(_build(remote, builder=("sh", "-c", "exit 1")).stdout)
    assert _run(remote, document["job_id"]).value == "failed"
    refused = _finish(remote)
    assert refused.exit_code == 1 and "ended failed; nothing to pack" in refused.output
