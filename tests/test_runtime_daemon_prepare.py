"""Execute a sandbox generation builder with the real managed profile contract."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import textwrap
import venv
from pathlib import Path

import pytest

from vq import admin, config, runtime_daemon, runtime_slots

pytestmark = [
    pytest.mark.no_autopatch_branch_check,
    pytest.mark.no_autopatch_lifecycle_lock,
    pytest.mark.no_autopatch_build_runner,
]


@pytest.mark.parametrize("editable", [False, True])
@pytest.mark.parametrize("current_input", [False, True])
def test_fresh_preparation_preserves_profile_at_final_path_without_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, editable: bool, current_input: bool,
):
    monkeypatch.delenv("PYTHONPATH", raising=False)
    repo = tmp_path / "legacy"
    package = repo / "src/vq"
    package.mkdir(parents=True)
    (repo / "pyproject.toml").write_text('[project]\nname="vibe-queue"\nversion="0.0"\n')
    (repo / ".gitignore").write_text('.venv/\n')
    (package / "__init__.py").write_text('VERSION = "retained"\n')
    (package / "__main__.py").write_text(textwrap.dedent('''\
        import hashlib
        from pathlib import Path
        root = Path(__file__).parent
        digest = hashlib.sha256()
        for path in sorted(root.rglob("*")):
            if not path.is_file() or "__pycache__" in path.parts:
                continue
            if path.name in {"SOURCE-SHA", "SOURCE-TREE-SHA256"}:
                continue
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(b"\\0")
            digest.update(path.read_bytes())
            digest.update(b"\\0")
        print(digest.hexdigest())
    '''))
    script = repo / "scripts/update.sh"
    script.parent.mkdir()
    product_scripts = Path(__file__).resolve().parents[1] / "scripts"
    for helper in ("_venv_helpers.sh", "_lifecycle_lock.sh"):
        shutil.copy2(product_scripts / helper, script.parent / helper)
    shutil.copy2(product_scripts / "update.sh", script.parent / "canonical-update.sh")
    binaries = tmp_path / "manager-stubs"
    binaries.mkdir()
    for manager in ("systemctl", "launchctl"):
        command = binaries / manager
        command.write_text("#!/bin/sh\nexit 3\n")
        command.chmod(0o700)
    monkeypatch.setenv("PATH", str(binaries) + os.pathsep + os.environ["PATH"])
    # Exercise the actual canonical argument/target preflight. Only network
    # package installation is replaced by a real local venv/package copy.
    script.write_text(textwrap.dedent('''\
        #!/bin/bash
        set -eu
        exec "$VQ_TEST_BUILDER_PYTHON" - "$@" <<'BUILD'
        import json, pathlib, shutil, sys, venv
        args = sys.argv[1:]
        path = pathlib.Path(args[args.index("--venv") + 1])
        assert path.is_absolute() and "releases" in path.parts
        assert "--skip-git" in args and "--recreate-venv" in args
        assert "--ref" not in args and "--branch" not in args
        assert "--adopt-legacy" not in args
        extras = args[args.index("--extras") + 1]
        editable = "--editable" in args
        venv.EnvBuilder(with_pip=False, symlinks=True).create(path)
        site = next((path / "lib").glob("python*/site-packages"))
        if editable:
            (site / "local.pth").write_text(str(path.parent / "src") + "\\n")
        else:
            shutil.copytree(path.parent / "src/vq", site / "vq")
        (path / ".vq-install-metadata").write_text(
            f"version=1\\nextras={extras}\\neditable={int(editable)}\\n")
        (path / "builder-argv.json").write_text(json.dumps(args))
        BUILD
    '''))
    monkeypatch.setenv("VQ_TEST_BUILDER_PYTHON", sys.executable)
    subprocess.run(["git", "init", "-q", "-b", "main", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=Runtime Test",
                    "-c", "user.email=test@example.invalid", "commit", "-qm", "builder"],
                   check=True)
    sha = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    upstream = tmp_path / "upstream"
    subprocess.run(["git", "clone", "-q", str(repo), str(upstream)], check=True)
    subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", str(upstream)],
                   check=True)
    old_venv = repo / ".venv"
    venv.EnvBuilder(with_pip=False, symlinks=True).create(old_venv)
    site = next((old_venv / "lib").glob("python*/site-packages"))
    shutil.copytree(package, site / "vq")
    (old_venv / ".vq-install-metadata").write_text(
        f"version=1\nextras=web\neditable={int(editable)}\n",
    )
    root = tmp_path / "store"
    registered = config.VenvProgram(
        kind="venv", python=str(old_venv / "bin/python"), git_dir=str(repo),
        update_script="scripts/update.sh", runtime_slot_root=str(root), extras=["web"],
    )
    serving = registered.model_copy(update={"runtime_slot_root": None})
    run_script = admin._run_update_script

    def canonical_preflight(git_dir, command, **kwargs):
        probe_options = {**kwargs, "extra_args": [*kwargs["extra_args"], "--dry-run"]}
        result = run_script(git_dir, "scripts/canonical-update.sh", **probe_options)
        assert result[0] == 0, result[1]
        return run_script(git_dir, command, **kwargs)

    monkeypatch.setattr(admin, "_run_update_script", canonical_preflight)
    before = admin._installed_tree_digest(serving.python)
    assert before == admin.source_tree_sha256_at_git_commit(repo, sha)
    with admin.toolset_lifecycle_lock(
        [registered], action="sandbox managed generation preparation",
        extra_resources=admin._runtime_slot_lifecycle_resources(registered, expected_sha=sha),
    ):
        result = runtime_daemon.prepare_generation(
            "queue", registered, serving, expected_sha=sha, expected_tag=None, supplied=None,
        )
    assert result.success, json.dumps(vars(result), indent=2)
    target = runtime_slots.slot_local_program(registered, root, sha)
    assert admin._installed_tree_digest(target.python) == before
    assert admin._installed_tree_digest(serving.python) == before
    assert not (root / "current").exists()
    assert admin._read_managed_install_metadata(Path(target.python).parent.parent) == {
        "version": "1", "extras": "web", "editable": str(int(editable)),
    }
    runtime_slots._assert_generation(runtime_slots.layout(root), sha, verify_content=True)

    if current_input:
        runtime_slots.activate(root, sha)
        registered = registered.model_copy(update={
            "runtime_slot_execution": True, "runtime_slot_build_input": "current",
        })
        serving = registered.execution_program()
        repo.rename(tmp_path / "retired-legacy")
        (upstream / "src/vq/__init__.py").write_text('VERSION = "new"\n')
        subprocess.run(["git", "-C", str(upstream), "add", "src/vq/__init__.py"], check=True)
        subprocess.run(["git", "-C", str(upstream), "-c", "user.name=Runtime Test",
                        "-c", "user.email=test@example.invalid", "commit", "-qm", "next"],
                       check=True)
        second = subprocess.check_output(
            ["git", "-C", str(upstream), "rev-parse", "HEAD"], text=True,
        ).strip()
        with admin.toolset_lifecycle_lock(
            [registered], action="sandbox managed current generation preparation",
            extra_resources=admin._runtime_slot_lifecycle_resources(
                registered, expected_sha=second,
            ),
        ):
            result = runtime_daemon.prepare_generation(
                "queue", registered, serving, expected_sha=second, expected_tag=None,
                supplied=None,
            )
        assert result.success, json.dumps(vars(result), indent=2)
        target = runtime_slots.slot_local_program(registered, root, second)
        assert admin._installed_tree_digest(target.python) == (
            admin.source_tree_sha256_at_git_commit(upstream, second)
        )
        assert admin._installed_tree_digest(serving.python) == before
        assert runtime_slots.resolve_current(root) == sha
        assert not repo.exists()
        assert admin._read_managed_install_metadata(Path(target.python).parent.parent) == {
            "version": "1", "extras": "web", "editable": str(int(editable)),
        }
        runtime_slots._assert_generation(runtime_slots.layout(root), second, verify_content=True)
