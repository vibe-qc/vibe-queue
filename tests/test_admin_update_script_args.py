"""v0.7.1 *Lamport's Clock* — Item 3: --update-script-arg pass-through.

``vq admin update`` (and ``--all-hosts``) gain a repeatable
``--update-script-arg FLAG`` option that gets appended to the
``bash <script>`` invocation. Lets operators ask the script for
``--recreate-venv``, ``--dev``, etc. without an SSH+heredoc dance.

The 2026-05-25 incident hit this gap three times on host_d —
each ``--recreate-venv`` request required a separate ssh + bash
heredoc that bypassed the marker/record machinery entirely.

See ``docs/v0_7_1_lamports_clock_design.md`` § Item 3.
"""
from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from vq import admin, config, paths


@pytest.fixture
def state_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv(paths.ENV_STATE_DIR, str(tmp_path / "state"))
    monkeypatch.setenv(config.ENV_CONFIG_DIR, str(tmp_path / "cfg"))
    (tmp_path / "cfg").mkdir()
    paths.queue_dir().mkdir(parents=True, exist_ok=True)
    paths.jobs_dir().mkdir(parents=True, exist_ok=True)
    return tmp_path


def _proc(rc: int = 0, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=[], returncode=rc, stdout=stdout, stderr="",
    )


def _make_script(git_dir: Path, name: str = "scripts/update.sh") -> None:
    script = git_dir / name
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text("#!/bin/bash\necho stub\n")
    script.chmod(0o755)


class TestRunUpdateScriptExtraArgs:
    """Direct unit test of _run_update_script's extra_args plumbing."""

    def test_extra_args_appended_to_bash_invocation(
        self, state_dir: Path,
    ) -> None:
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0, stdout="ok\n")

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir, "scripts/update.sh",
                work_errors=[],
                extra_args=["--recreate-venv", "--dev"],
            )
        # Find the bash call (niceness prefix may be present on Linux;
        # we look for the bash element)
        bash_argv = next(
            argv for argv in captured_argv if "bash" in argv
        )
        bi = bash_argv.index("bash")
        # argv after bash: [script_path, ...config_args, ...extra_args]
        tail = bash_argv[bi + 1:]
        # extra_args are the LAST elements (after script path + config args)
        assert tail[-2:] == ["--recreate-venv", "--dev"]

    def test_extra_args_appended_after_config_args(
        self, state_dir: Path,
    ) -> None:
        """Config-side script_args (from shlex.split(script_cmd)) come
        BEFORE the forwarded extra_args. ``script_cmd='scripts/update.sh
        --dev'`` + ``extra_args=['--recreate-venv']`` produces argv
        ``[bash, script, --dev, --recreate-venv]``."""
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0)

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir, "scripts/update.sh --dev",
                work_errors=[],
                extra_args=["--recreate-venv"],
            )
        bash_argv = next(argv for argv in captured_argv if "bash" in argv)
        bi = bash_argv.index("bash")
        tail = bash_argv[bi + 1:]
        # Last 2 elements: --dev (config), --recreate-venv (forwarded)
        assert tail[-2:] == ["--dev", "--recreate-venv"]

    def test_pinned_update_strips_config_ref_args(
        self, state_dir: Path,
    ) -> None:
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0)

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir, "scripts/update.sh --dev",
                work_errors=[],
                extra_args=["--recreate-venv", "--branch", "abc123"],
                strip_config_ref_args=True,
            )
        bash_argv = next(argv for argv in captured_argv if "bash" in argv)
        bi = bash_argv.index("bash")
        tail = bash_argv[bi + 1:]
        assert "--dev" not in tail
        assert tail[-3:] == ["--recreate-venv", "--branch", "abc123"]

    def test_no_extra_args_unchanged_behavior(
        self, state_dir: Path,
    ) -> None:
        """The default (extra_args=None) preserves pre-v0.7.1 behavior:
        only the config-side args are passed to bash."""
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0)

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir, "scripts/update.sh",
                work_errors=[],
                # extra_args omitted; defaults to None
            )
        bash_argv = next(argv for argv in captured_argv if "bash" in argv)
        bi = bash_argv.index("bash")
        # Just the script path, no extra args.
        assert bash_argv[bi + 1:] == [str(git_dir / "scripts/update.sh")]

    def test_managed_admin_update_exports_deferred_restart_contract(
        self, state_dir: Path,
    ) -> None:
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        captured_env: dict[str, str] = {}

        def fake_run(*args, **kwargs):
            captured_env.update(kwargs["env"])
            return _proc(rc=0)

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir,
                "scripts/update.sh",
                work_errors=[],
                managed_daemon_restart=True,
            )

        assert captured_env["VQ_ADMIN_MANAGED_DAEMON_RESTART_PID"] == str(
            os.getpid()
        )

    def test_unmanaged_script_does_not_inherit_deferred_restart_contract(
        self, state_dir: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        git_dir = state_dir / "repo"
        _make_script(git_dir)
        monkeypatch.setenv("VQ_ADMIN_MANAGED_DAEMON_RESTART_PID", "999999")
        captured_env: dict[str, str] = {}

        def fake_run(*args, **kwargs):
            captured_env.update(kwargs["env"])
            return _proc(rc=0)

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin._run_update_script(
                git_dir,
                "scripts/update.sh",
                work_errors=[],
            )

        assert "VQ_ADMIN_MANAGED_DAEMON_RESTART_PID" not in captured_env


class TestUpdateScriptRefPinArgs:
    def test_expected_sha_strips_conflicting_operator_selector(self) -> None:
        sha = "a" * 40
        args = admin._update_script_args_for_ref(
            ["--dev", "--recreate-venv"], None, sha,
        )

        assert args == ["--recreate-venv", "--ref", sha]

    def test_expected_tag_strips_branch_selector_forms(self) -> None:
        args = admin._update_script_args_for_ref(
            ["--branch", "main", "--ref=origin/main", "--flag"],
            "v0.15.43",
            None,
        )

        assert args == ["--flag", "--branch", "v0.15.43"]


class TestUpdateEnvThreading:
    """Pins that update_env threads the arg list down through
    _do_update_work -> _run_update_script."""

    def test_update_env_threads_extra_args(
        self, state_dir: Path,
    ) -> None:
        git_dir = state_dir / "repo"
        (git_dir / ".git").mkdir(parents=True, exist_ok=True)
        _make_script(git_dir)
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update.sh"\n'
        )
        cfg = config.load_config()
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0, stdout="")

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env(
                "vibeqc-dev", cfg, host="localhost",
                update_script_args=["--recreate-venv"],
            )
        bash_argv = next(
            argv for argv in captured_argv if "bash" in argv
        )
        assert "--recreate-venv" in bash_argv

    def test_update_env_default_no_extra_args(
        self, state_dir: Path,
    ) -> None:
        """update_env without update_script_args preserves the
        pre-v0.7.1 behavior: no extra flags pollute the bash call."""
        git_dir = state_dir / "repo"
        (git_dir / ".git").mkdir(parents=True, exist_ok=True)
        _make_script(git_dir)
        (state_dir / "cfg" / "config.toml").write_text(
            "[programs.vibeqc-dev]\n"
            'kind = "venv"\n'
            'python = "/fake/python"\n'
            f'git_dir = "{git_dir}"\n'
            'branch = "main"\n'
            'update_script = "scripts/update.sh"\n'
        )
        cfg = config.load_config()
        captured_argv: list[list[str]] = []

        def fake_run(*args, **kwargs):
            captured_argv.append(args[0])
            return _proc(rc=0, stdout="")

        with patch("vq.admin.subprocess.run", side_effect=fake_run):
            admin.update_env("vibeqc-dev", cfg, host="localhost")
        bash_argv = next(
            argv for argv in captured_argv if "bash" in argv
        )
        assert "--recreate-venv" not in bash_argv
        assert "--dev" not in bash_argv


@pytest.fixture
def legacy_install(tmp_path):
    import json
    import sys

    project = tmp_path / 'repo/vibe-queue'
    (project / 'src/vq').mkdir(parents=True)
    (project / 'pyproject.toml').write_text('')
    _make_script(project)
    venv = project / '.venv'
    (venv / 'bin').mkdir(parents=True)
    (venv / 'pyvenv.cfg').write_text('')
    (venv / 'bin/python').symlink_to(sys.executable)
    metadata = venv / 'lib/python3.13/site-packages/vq-0.11.0.dist-info/direct_url.json'
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({'url': project.as_uri(), 'dir_info': {'editable': True}}))
    prog = config.VenvProgram(
        kind='venv', python=str(venv / 'bin/python'), git_dir=str(project.parent),
        update_script='vibe-queue/scripts/update.sh',
    )
    return prog, project, venv, metadata


def test_managed_adoption_preserves_mode_and_web_without_marking_old_venv(legacy_install):
    prog, project, venv, _ = legacy_install
    (venv / 'lib64').symlink_to('lib', target_is_directory=True)
    args = admin._managed_update_script_args(
        prog, ['--adopt-legacy', '--extras', 'web', '--editable'],
    )
    assert args[args.index('--extras') + 1] == 'web'
    assert '--editable' in args and '--recreate-venv' in args
    # Outer transaction renames the old venv; the canonical installer creates
    # a new owned venv. Passing adoption to that absent target would fail.
    assert '--adopt-legacy' not in args
    assert not (venv / '.vq-install-metadata').exists()
    assert not (venv / '.vq-checkout-owner').exists()


@pytest.mark.parametrize('damage', [
    'different-origin', 'duplicate-record', 'symlink-only', 'different-alias',
    'mode-mismatch', 'marked', 'profile-present', 'relative-origin', 'bad-json',
])
def test_managed_adoption_refuses_ambiguous_provenance(legacy_install, damage):
    import json
    prog, project, venv, metadata = legacy_install
    if damage == 'different-origin':
        metadata.write_text(json.dumps({'url': project.parent.as_uri()}))
    elif damage == 'duplicate-record':
        other = metadata.parent.with_name('vq-0.12.0.dist-info')
        other.mkdir()
        (other / metadata.name).write_bytes(metadata.read_bytes())
    elif damage == 'symlink-only':
        data = metadata.read_bytes()
        metadata.unlink()
        other = project / 'external.json'
        other.write_bytes(data)
        metadata.symlink_to(other)
    elif damage == 'different-alias':
        other = venv / 'lib64/python3.13/site-packages/vq-0.11.0.dist-info'
        other.mkdir(parents=True)
        (other / metadata.name).symlink_to(metadata)
        metadata.unlink()
    elif damage == 'mode-mismatch':
        metadata.write_text(json.dumps({'url': project.as_uri(), 'dir_info': {'editable': False}}))
    elif damage == 'marked':
        (venv / '.vq-checkout-owner').write_text('version=1\n')
    elif damage == 'profile-present':
        (venv / '.vq-install-metadata').write_text('version=1\nextras=web\neditable=1\n')
    elif damage == 'relative-origin':
        metadata.write_text(json.dumps({'url': 'file:relative'}))
    else:
        metadata.write_text('{')
    with pytest.raises(admin.AdminError, match='legacy adoption'):
        admin._managed_update_script_args(prog, ['--adopt-legacy', '--extras', 'web', '--editable'])


@pytest.mark.parametrize('args', [None, ['--adopt-legacy'],
    ['--adopt-legacy', '--extras', 'web', '--editable', '--venv', '/tmp/other'],
    ['--adopt-legacy', '--extras', 'unknown', '--editable'],
])
def test_managed_adoption_requires_complete_explicit_declaration(legacy_install, args):
    prog, *_ = legacy_install
    with pytest.raises(admin.AdminError):
        admin._managed_update_script_args(prog, args)


@pytest.fixture
def marked_install(legacy_install):
    prog, project, venv, _ = legacy_install
    metadata = venv / '.vq-install-metadata'
    metadata.write_text('version=1\nextras=core\neditable=1\n')
    return prog, project, venv, metadata


@pytest.mark.parametrize('editable', [False, True])
@pytest.mark.parametrize('profile', ['core', 'web', 'test', 'dev', 'all'])
def test_managed_profile_change_preserves_target_mode_and_original_metadata(
    marked_install, editable, profile,
):
    prog, _, venv, metadata = marked_install
    mode = '--editable' if editable else '--copied'
    metadata.write_text(f'version=1\nextras=core\neditable={int(editable)}\n')
    before = metadata.read_bytes()
    prog = prog.model_copy(update={'update_script': prog.update_script + ' ' + mode})
    args = admin._managed_update_script_args(
        prog, ['--recreate-venv', '--extras', profile],
    )
    assert args[args.index('--extras') + 1] == profile
    assert args[args.index('--venv') + 1] == str(venv)
    assert (shlex.split(prog.update_script)[1:] + args).count(mode) == 1
    assert '--recreate-venv' in args
    assert metadata.read_bytes() == before


@pytest.mark.parametrize('editable', [False, True])
@pytest.mark.parametrize('configured_mode', [False, True])
def test_managed_mode_reaches_real_update_script_once(
    marked_install, editable, configured_mode,
):
    """Compose and execute the actual shell argv, including configured defaults."""
    prog, project, venv, metadata = marked_install
    mode = '--editable' if editable else '--copied'
    metadata.write_text(f'version=1\nextras=core\neditable={int(editable)}\n')
    source_scripts = Path(__file__).resolve().parents[1] / 'scripts'
    shutil.copytree(source_scripts, project / 'scripts', dirs_exist_ok=True)
    configured = prog.update_script + ' --dev' + (f' {mode}' if configured_mode else '')
    prog = prog.model_copy(update={'update_script': configured})
    planned = admin._managed_update_script_args(prog, ['--recreate-venv', '--extras', 'web'])
    pinned = admin._update_script_args_for_ref(planned, None, 'a' * 40)
    assert pinned is not None
    assert pinned[pinned.index('--venv') + 1] == str(venv)
    assert Path(pinned[pinned.index('--python') + 1]).is_file()
    assert pinned[pinned.index('--extras') + 1] == 'web'
    assert pinned[-2:] == ['--ref', 'a' * 40]
    errors = []
    # --help follows every planned argument: the real strict parser must
    # consume them all before exiting, without touching an environment.
    rc, output, _ = admin._run_update_script(
        Path(prog.git_dir), prog.update_script, work_errors=errors,
        extra_args=[*pinned, '--help'], strip_config_ref_args=True,
    )
    assert rc == 0, output
    assert errors == []
    assert 'USAGE' in output


@pytest.mark.parametrize('supplied', [None, ['--recreate-venv']])
def test_managed_default_still_preserves_profile(marked_install, supplied):
    prog, _, _, metadata = marked_install
    metadata.write_text('version=1\nextras=web\neditable=1\n')
    args = admin._managed_update_script_args(prog, supplied)
    assert args[args.index('--extras') + 1] == 'web'
    assert '--editable' in args


@pytest.mark.parametrize('supplied', [
    ['--extras', 'web'],
    ['--recreate-venv', '--extras'],
    ['--recreate-venv', '--extras', 'unknown'],
    ['--recreate-venv', '--extras', 'web', '--extras', 'core'],
    ['--recreate-venv', '--recreate-venv'],
    ['--recreate-venv', '--venv', '/tmp/other'],
    ['--recreate-venv', '--python', '/bin/python'],
    ['--recreate-venv', '--copied'],
    ['--recreate-venv', '--skip-git'],
    ['--adopt-legacy', '--extras', 'web', '--editable'],
])
def test_managed_profile_change_refuses_ambiguous_or_retargeting_args(marked_install, supplied):
    prog, _, _, _ = marked_install
    with pytest.raises(admin.AdminError):
        admin._managed_update_script_args(prog, supplied)


@pytest.mark.parametrize('argument', [
    '--copied', '--editable --copied', '--editable --editable', '--extras web',
])
def test_managed_config_cannot_override_recorded_mode_or_profile(marked_install, argument):
    prog, _, _, _ = marked_install
    prog = prog.model_copy(update={'update_script': prog.update_script + ' ' + argument})
    with pytest.raises(admin.AdminError):
        admin._managed_update_script_args(prog, ['--recreate-venv', '--extras', 'web'])


def test_unmarked_profile_change_still_requires_legacy_adoption(legacy_install):
    prog, *_ = legacy_install
    with pytest.raises(admin.AdminError, match='metadata'):
        admin._managed_update_script_args(prog, ['--recreate-venv', '--extras', 'web'])


def test_profile_request_is_revalidated_after_pause_before_service_stop(
    marked_install, monkeypatch,
):
    from types import SimpleNamespace

    prog, _, venv, metadata = marked_install
    supplied = ['--recreate-venv', '--extras', 'web']
    planned = admin._managed_update_script_args(prog, supplied)
    resumed = []

    def pause(*args, **kwargs):
        # Another actor changes the recorded mode after the initial plan.
        metadata.write_text('version=1\nextras=core\neditable=0\n')
        return SimpleNamespace(summary='paused', require_quiescent=lambda: None)

    def resume(*args, **kwargs):
        resumed.append(kwargs['pause_token'] if 'pause_token' in kwargs else args[1])
        return SimpleNamespace(summary='resumed', require_clear=lambda: None)

    monkeypatch.setattr(admin, 'pause_token_scope_with_proof', pause)
    monkeypatch.setattr(admin, 'resume_token_scope_with_proof', resume)
    monkeypatch.setattr(admin, '_begin_managed_daemon_update',
                        lambda *a, **k: pytest.fail('profile drift reached service stop'))
    with pytest.raises(admin.AdminError, match='declaration changed before service stop'):
        admin._update_env_logged(
            'vibeqc-queue', config.Config(programs={'vibeqc-queue': prog}),
            prog=prog, multi_user=False, host='localhost', admin_token=None,
            expected_tag=None, expected_sha='a' * 40, force=False,
            restart_daemon=True, require_self_update=True, managed_daemon_restart=True,
            initial_self_update_probe=admin._SelfUpdateProbe(
                is_self_update=True, daemon_running=True, service_manager='launchd',
                manager_available=True, diagnostic='test serving daemon',
            ),
            update_script_args=planned, managed_request_args=supplied,
        )
    assert len(resumed) == 1 and resumed[0].startswith('admin-update-')
    assert venv.is_dir()
