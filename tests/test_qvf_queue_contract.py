"""First-class complete-calculation QVF queue protocol tests."""
from __future__ import annotations

import hashlib
import json
import shlex
import subprocess
import sys
import time
import zipfile
from pathlib import Path

from click.testing import CliRunner

from vq import config, fetch, paths, submit, transport
from vq.cli import _validate_scheduler_target_expected_sha, main
from vq.config import HostConfig
from vq.daemon import Daemon
from vq.spec import JobSpec, JobState, ProgramRuntimePin
from vq.status import show_status_json
from vq.submit import submit_local


def _git_repo(path: Path) -> str:
    path.mkdir()
    subprocess.run(["git", "init"], cwd=path, check=True, capture_output=True)
    (path / "README").write_text("runtime\n")
    subprocess.run(["git", "add", "README"], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            "runtime",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit(path: Path, name: str) -> str:
    (path / name).write_text("drift\n")
    subprocess.run(["git", "add", name], cwd=path, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=vq tests",
            "-c",
            "user.email=vq-tests@example.invalid",
            "commit",
            "-m",
            "drift",
        ],
        cwd=path,
        check=True,
        capture_output=True,
    )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _write_qvf(
    path: Path,
    *,
    run_status: str = "pending",
    sequence: int | None = None,
    complete_record: bool = False,
) -> bytes:
    spec_bytes = b'{"basis":"sto-3g","job_type":"molecular","method":"rhf"}\n'
    sections: list[dict[str, object]] = [
        {
            "id": "job_spec0",
            "kind": "job.spec",
            "members": {
                "spec": {
                    "path": "job_spec/spec.json",
                    "format": "json",
                    "sha256": hashlib.sha256(spec_bytes).hexdigest(),
                }
            },
        }
    ]
    members: dict[str, bytes] = {"job_spec/spec.json": spec_bytes}
    if sequence is not None:
        record_members: dict[str, object] = {}
        if complete_record:
            for role, (member_path, data) in {
                "input": ("run_record/input.txt", spec_bytes),
                "log": ("run_record/log.txt", b"SCF complete\n"),
            }.items():
                members[member_path] = data
                record_members[role] = {
                    "path": member_path,
                    "format": "binary",
                    "sha256": hashlib.sha256(data).hexdigest(),
                }
        sections.append(
            {
                "id": f"run_record{sequence}",
                "kind": "run.record",
                "program": "vibe-qc",
                "sequence": sequence,
                "members": record_members,
            }
        )
    manifest = {
        "qvf_version": "0.1",
        "provenance": {"run_status": run_status},
        "sections": sections,
    }
    members["manifest.json"] = json.dumps(manifest).encode()
    with zipfile.ZipFile(path, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    return spec_bytes


def _write_fake_vibeqc(package_root: Path) -> None:
    package = package_root / "vibeqc"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "_cli.py").write_text(
        """\
import hashlib
import json
import os
import sys
import tempfile
import zipfile
from pathlib import Path

path = Path(sys.argv[2])
force = "--force" in sys.argv[3:]
with zipfile.ZipFile(path) as zf:
    names = {name: zf.read(name) for name in zf.namelist()}
manifest = json.loads(names["manifest.json"])
status = manifest["provenance"]["run_status"]
if status != "pending" and not force:
    raise SystemExit(2)
spec_section = next(s for s in manifest["sections"] if s["kind"] == "job.spec")
spec_bytes = names[spec_section["members"]["spec"]["path"]]
spec = json.loads(spec_bytes)
records = [s for s in manifest["sections"] if s["kind"] == "run.record"]
sequence = max([int(s["sequence"]) for s in records], default=-1) + 1
log = b"synthetic streamed log\\n"
base = f"run_record/{sequence}"
for role, data in (("input", spec_bytes), ("log", log)):
    names[f"{base}/{role}.txt"] = data
record = {
    "id": f"run_record{sequence}",
    "kind": "run.record",
    "program": "vibe-qc",
    "sequence": sequence,
    "exit_status": 1 if spec.get("options", {}).get("fail") else 0,
    "members": {
        role: {
            "path": f"{base}/{role}.txt",
            "format": "binary",
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        for role, data in (("input", spec_bytes), ("log", log))
    },
}
manifest["sections"].append(record)
failed = bool(spec.get("options", {}).get("fail"))
manifest["provenance"]["run_status"] = "failed" if failed else "converged"
names["manifest.json"] = json.dumps(manifest, sort_keys=True).encode()
fd, temp_name = tempfile.mkstemp(dir=path.parent, prefix=".qvf-", suffix=".tmp")
os.close(fd)
try:
    with zipfile.ZipFile(temp_name, "w") as zf:
        for name, data in names.items():
            zf.writestr(name, data)
    os.replace(temp_name, path)
finally:
    try:
        os.unlink(temp_name)
    except FileNotFoundError:
        pass
sys.stdout.buffer.write(log)
raise SystemExit(1 if failed else 0)
"""
    )


def _wait_terminal(daemon: Daemon, jobid: str) -> JobSpec:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        daemon.iterate()
        spec = JobSpec.read(paths.spec_path(jobid))
        if spec.is_terminal:
            return spec
        time.sleep(0.02)
    raise AssertionError(f"job {jobid} did not become terminal")


def _program_config(
    tmp_path: Path,
    sha: str,
    *,
    python: str | None = None,
) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir(exist_ok=True)
    git_dir = tmp_path / "runtime"
    (cfg_dir / "config.toml").write_text(
        "[programs.vibeqc-dev]\n"
        'kind = "venv"\n'
        f'python = "{python or sys.executable}"\n'
        f'git_dir = "{git_dir}"\n'
    )


def test_cli_single_qvf_resolves_managed_runtime_and_preserves_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    sha = _git_repo(tmp_path / "runtime")
    _program_config(tmp_path, sha)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    source = tmp_path / "job.qvf"
    original = _write_qvf(source)

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "localhost",
            "--program",
            "vibeqc-dev",
            str(source),
        ],
    )

    assert result.exit_code == 0, result.output
    spec = JobSpec.read(paths.spec_path(result.output.strip()))
    assert spec.command == [
        sys.executable,
        "-m",
        "vibeqc._cli",
        "run",
        "job.qvf",
    ]
    assert spec.qvf_artifact_name == "job.qvf"
    assert spec.program_runtime_pin is not None
    assert spec.program_runtime_pin.expected_git_sha == sha
    assert (Path(spec.cwd) / "job.qvf").read_bytes() == source.read_bytes()
    with zipfile.ZipFile(Path(spec.cwd) / "job.qvf") as zf:
        assert zf.read("job_spec/spec.json") == original


def test_qvf_full_sha_rejects_matching_prefix_with_wrong_tail(
    tmp_path: Path, monkeypatch
) -> None:
    sha = _git_repo(tmp_path / "runtime")
    _program_config(tmp_path, sha)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    source = tmp_path / "job.qvf"
    _write_qvf(source)
    replacement = "0" if sha[-1] != "0" else "1"
    wrong_full_sha = f"{sha[:-1]}{replacement}"

    result = CliRunner().invoke(
        main,
        [
            "submit",
            "localhost",
            "--program",
            "vibeqc-dev",
            "--expected-sha",
            wrong_full_sha,
            str(source),
        ],
    )

    assert result.exit_code != 0
    assert f"expected git SHA {wrong_full_sha}" in result.output
    assert not list(paths.queue_dir().glob("*.json"))


def test_qvf_scheduler_submit_uses_configured_compute_wrapper(
    tmp_path: Path, monkeypatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        "[hosts.host_f]\n"
        'ssh = "host_f.invalid"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/cluster"\n'
        'scheduler_driver = "localhost"\n'
        "[hosts.host_f.scheduler_program_hooks.vibeqc-dev]\n"
        'command_wrapper = ["/cluster/bin/vibeqc-dev-python"]\n'
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    source = tmp_path / "job.qvf"
    _write_qvf(source)
    queue = tmp_path / "queue"
    jobs = tmp_path / "jobs"
    sha = "a" * 40

    jobid = submit_local(
        host="localhost",
        input_file=str(source),
        program="vibeqc-dev",
        program_runtime_pin=ProgramRuntimePin(
            expected_git_sha=sha,
            scheduler_host="host_f",
        ),
        scheduler_target="host_f",
        queue_dir=queue,
        jobs_dir=jobs,
    )

    spec = JobSpec.read(queue / f"{jobid}.json")
    assert spec.command == [
        "/cluster/bin/vibeqc-dev-python",
        "-m",
        "vibeqc._cli",
        "run",
        "job.qvf",
    ]


def test_remote_single_qvf_is_forwarded_as_qvf_not_python(
    tmp_path: Path, monkeypatch
) -> None:
    source = tmp_path / "job.qvf"
    _write_qvf(source)
    calls: dict[str, object] = {"shell": []}

    def upload(_cfg, local: Path, remote: str) -> None:
        calls["upload"] = (local, remote)
        assert local == source
        assert remote.endswith("/job.qvf")

    def run_remote_vq(_cfg, *args: str, **_kwargs):
        calls["argv"] = args
        return subprocess.CompletedProcess(
            args=list(args),
            returncode=0,
            stdout="abc123def456\n",
            stderr="",
        )

    monkeypatch.setattr(transport, "upload_file", upload)
    monkeypatch.setattr(transport, "run_remote_vq", run_remote_vq)
    def run_remote_shell(_cfg, *args: str, **_kwargs):
        shell = calls["shell"]
        assert isinstance(shell, list)
        shell.append(args)
        return subprocess.CompletedProcess(
            args=[], returncode=0, stdout="", stderr=""
        )

    monkeypatch.setattr(transport, "run_remote_shell", run_remote_shell)
    result = submit.submit_remote(
        host="host_d",
        host_cfg=HostConfig(ssh="host_d.invalid", remote_vq="vq"),
        input_file=str(source),
        program="vibeqc-dev",
    )

    assert result == ["abc123def456"]
    argv = calls["argv"]
    assert isinstance(argv, tuple)
    assert "-c" not in argv
    assert "--" not in argv
    assert "python" not in argv
    assert argv[-1].endswith("/job.qvf")
    assert "--expected-sha" not in argv
    shell = calls["shell"]
    assert isinstance(shell, list)
    assert any(args[:2] == ("mkdir", "-p") for args in shell)
    assert any(args[0] == "rmdir" for args in shell)


def test_scheduler_qvf_auto_snapshots_registered_full_sha(
    tmp_path: Path, monkeypatch
) -> None:
    cfg_dir = tmp_path / "cfg"
    cfg_dir.mkdir()
    (cfg_dir / "config.toml").write_text(
        "[hosts.host_f]\n"
        'ssh = "host_f.invalid"\n'
        'scheduler = "pbs"\n'
        'scheduler_dialect = "torque"\n'
        'scratch_root = "/cluster"\n'
        'scheduler_driver = "localhost"\n'
    )
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(cfg_dir))
    sha = "a" * 40
    payload = json.dumps(
        [
            {
                "name": "vibeqc-dev",
                "kind": "venv",
                "python": "/cluster/bin/vibeqc-dev-python",
                "current_git_sha_full": sha,
                "import_version": "0.15.59",
            }
        ]
    )
    monkeypatch.setattr(
        transport,
        "run_remote_vq",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=payload, stderr=""
        ),
    )

    pin = _validate_scheduler_target_expected_sha(
        config.load_config(), "host_f", "vibeqc-dev", None
    )

    assert pin is not None
    assert pin.expected_git_sha == sha
    assert pin.resolved_executable == "/cluster/bin/vibeqc-dev-python"


def test_artifact_only_fetch_returns_no_queue_sidecars(tmp_path: Path) -> None:
    queue = paths.queue_dir()
    queue.mkdir(parents=True)
    workspace = paths.jobs_dir() / "abc123def456"
    workspace.mkdir(parents=True)
    qvf = workspace / "job.qvf"
    _write_qvf(qvf)
    (workspace / "stdout.log").write_text("operational log\n")
    (workspace / "_vq").mkdir()
    (workspace / "_vq" / "events.jsonl").write_text("{}\n")
    JobSpec(
        id="abc123def456",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        qvf_artifact_name="job.qvf",
    ).write(queue / "abc123def456.json")

    out = tmp_path / "result"
    result = fetch.fetch_artifact_local(
        "abc123def456", "job.qvf", out
    )

    assert result == out / "job.qvf"
    assert result.read_bytes() == qvf.read_bytes()
    assert sorted(path.name for path in out.iterdir()) == ["job.qvf"]


def test_qvf_status_requires_complete_sequenced_run_record(
    tmp_path: Path,
) -> None:
    queue = tmp_path / "queue"
    workspace = tmp_path / "workspace"
    queue.mkdir()
    workspace.mkdir()
    _write_qvf(
        workspace / "job.qvf",
        run_status="converged",
        sequence=0,
        complete_record=False,
    )
    JobSpec(
        id="qvfstatus001",
        command=["true"],
        cwd=str(workspace),
        cpus=1,
        qvf_artifact_name="job.qvf",
        state=JobState.COMPLETED,
        exit_code=0,
    ).write(queue / "qvfstatus001.json")

    payload = json.loads(
        show_status_json(
            "localhost", "qvfstatus001", queue_dir=queue, tail=0
        )
    )
    lifecycle = payload["qvf_lifecycle"]
    assert lifecycle["run_status"] == "converged"
    assert lifecycle["terminal_complete"] is False
    assert lifecycle["done"] is False
    assert lifecycle["queue_process_failed"] is True
    assert lifecycle["outcome"] == "queue_process_failed"


def test_qvf_status_distinguishes_chemistry_failure(tmp_path: Path) -> None:
    queue = tmp_path / "queue"
    workspace = tmp_path / "workspace"
    queue.mkdir()
    workspace.mkdir()
    _write_qvf(
        workspace / "job.qvf",
        run_status="failed",
        sequence=2,
        complete_record=True,
    )
    JobSpec(
        id="qvfstatus002",
        command=["false"],
        cwd=str(workspace),
        cpus=1,
        qvf_artifact_name="job.qvf",
        state=JobState.FAILED,
        exit_code=1,
    ).write(queue / "qvfstatus002.json")

    payload = json.loads(
        show_status_json(
            "localhost", "qvfstatus002", queue_dir=queue, tail=0
        )
    )
    lifecycle = payload["qvf_lifecycle"]
    assert lifecycle["sequence"] == 2
    assert lifecycle["terminal_complete"] is True
    assert lifecycle["done"] is True
    assert lifecycle["chemistry_failed"] is True
    assert lifecycle["queue_process_failed"] is False
    assert lifecycle["outcome"] == "chemistry_failed"


def test_daemon_qvf_round_trip_fetch_rerun_and_failed_container(
    tmp_path: Path, monkeypatch
) -> None:
    """One-file payload through the real daemon, including both outcomes."""
    sha = _git_repo(tmp_path / "runtime")
    isolated_python = tmp_path / "python-no-site"
    isolated_python.write_text(
        "#!/bin/sh\n"
        f"exec {shlex.quote(sys.executable)} -S \"$@\"\n"
    )
    isolated_python.chmod(0o755)
    _program_config(tmp_path, sha, python=str(isolated_python))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    fake_root = tmp_path / "fake-package"
    _write_fake_vibeqc(fake_root)
    monkeypatch.setenv("PYTHONPATH", str(fake_root))
    daemon = Daemon(max_cpus=1, poll_interval=0.01)
    daemon.queue_dir.mkdir(parents=True, exist_ok=True)
    daemon.jobs_dir.mkdir(parents=True, exist_ok=True)

    pending = tmp_path / "pending.qvf"
    original_spec = _write_qvf(pending)
    submit_result = CliRunner().invoke(
        main,
        [
            "submit",
            "localhost",
            "--program",
            "vibeqc-dev",
            "--expected-sha",
            sha,
            str(pending),
        ],
    )
    assert submit_result.exit_code == 0, submit_result.output
    first_id = submit_result.output.strip()
    first = _wait_terminal(daemon, first_id)
    assert first.state == JobState.COMPLETED
    fetched_dir = tmp_path / "fetched"
    fetched = fetch.fetch_artifact_local(
        first_id, "pending.qvf", fetched_dir
    )
    assert sorted(item.name for item in fetched_dir.iterdir()) == [
        "pending.qvf"
    ]
    with zipfile.ZipFile(fetched) as zf:
        manifest = json.loads(zf.read("manifest.json"))
        record = next(
            section
            for section in manifest["sections"]
            if section["kind"] == "run.record"
        )
        spec_section = next(
            section
            for section in manifest["sections"]
            if section["kind"] == "job.spec"
        )
        assert zf.read(spec_section["members"]["spec"]["path"]) == original_spec
        assert record["sequence"] == 0
        assert zf.read(record["members"]["input"]["path"]) == original_spec
        assert zf.read(record["members"]["log"]["path"]) == (
            Path(first.cwd) / first.stdout_path
        ).read_bytes()

    rerun_result = CliRunner().invoke(
        main,
        [
            "submit",
            "localhost",
            "--program",
            "vibeqc-dev",
            "--expected-sha",
            sha,
            "--qvf-force",
            str(fetched),
        ],
    )
    assert rerun_result.exit_code == 0, rerun_result.output
    rerun = _wait_terminal(daemon, rerun_result.output.strip())
    assert rerun.state == JobState.COMPLETED
    with zipfile.ZipFile(Path(rerun.cwd) / "pending.qvf") as zf:
        manifest = json.loads(zf.read("manifest.json"))
        assert sorted(
            section["sequence"]
            for section in manifest["sections"]
            if section["kind"] == "run.record"
        ) == [0, 1]

    failed_input = tmp_path / "failed.qvf"
    _write_qvf(failed_input)
    with zipfile.ZipFile(failed_input) as zf:
        members = {name: zf.read(name) for name in zf.namelist()}
    manifest = json.loads(members["manifest.json"])
    spec_section = next(
        section
        for section in manifest["sections"]
        if section["kind"] == "job.spec"
    )
    fail_spec = json.loads(members[spec_section["members"]["spec"]["path"]])
    fail_spec["options"] = {"fail": True}
    fail_bytes = json.dumps(fail_spec, sort_keys=True).encode()
    spec_path = spec_section["members"]["spec"]["path"]
    members[spec_path] = fail_bytes
    spec_section["members"]["spec"]["sha256"] = hashlib.sha256(
        fail_bytes
    ).hexdigest()
    members["manifest.json"] = json.dumps(manifest).encode()
    with zipfile.ZipFile(failed_input, "w") as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    failed_result = CliRunner().invoke(
        main,
        [
            "submit",
            "localhost",
            "--program",
            "vibeqc-dev",
            "--expected-sha",
            sha,
            str(failed_input),
        ],
    )
    assert failed_result.exit_code == 0, failed_result.output
    failed = _wait_terminal(daemon, failed_result.output.strip())
    assert failed.state == JobState.FAILED
    failed_status = json.loads(
        show_status_json(
            "localhost",
            failed.id,
            queue_dir=paths.queue_dir(),
            tail=10,
        )
    )
    assert failed_status["qvf_lifecycle"]["chemistry_failed"] is True
    assert failed_status["qvf_lifecycle"]["queue_process_failed"] is False


def test_qvf_stale_runtime_pin_fails_before_dispatch(
    tmp_path: Path, monkeypatch
) -> None:
    old_sha = _git_repo(tmp_path / "runtime")
    new_sha = _commit(tmp_path / "runtime", "NEW")
    _program_config(tmp_path, new_sha)
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    queue = paths.queue_dir()
    workspace = paths.jobs_dir() / "qvfstalepin1"
    queue.mkdir(parents=True)
    workspace.mkdir(parents=True)
    _write_qvf(workspace / "job.qvf")
    ran = workspace / "ran"
    JobSpec(
        id="qvfstalepin1",
        command=["sh", "-c", f"touch {ran}"],
        cwd=str(workspace),
        cpus=1,
        program="vibeqc-dev",
        program_runtime_pin=ProgramRuntimePin(expected_git_sha=old_sha),
        qvf_artifact_name="job.qvf",
    ).write(queue / "qvfstalepin1.json")
    daemon = Daemon(max_cpus=1, poll_interval=0.01)

    failed = _wait_terminal(daemon, "qvfstalepin1")

    assert failed.state == JobState.FAILED
    assert failed.failure_reason is not None
    assert "runtime pin mismatch before dispatch" in failed.failure_reason
    assert old_sha in failed.failure_reason
    assert not ran.exists()
