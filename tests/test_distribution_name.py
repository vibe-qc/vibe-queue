"""The distribution is `vibe-queue`; the command and the package stay `vq` (#68).

Three places spell the distribution's name, or the normalized ``.dist-info``
prefix pip derives from it, and cannot import one another: ``pyproject.toml``,
``vq.DISTRIBUTION_NAME`` and the PEP 610 readers in the shell installers. These
tests keep them in agreement and pin the transition rule for an environment
that a release before the rename installed.
"""
from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

import vq
from vq import admin

ROOT = Path(__file__).resolve().parents[1]


def _pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))


def test_pyproject_names_the_distribution_the_package_expects() -> None:
    project = _pyproject()["project"]
    assert project["name"] == vq.DISTRIBUTION_NAME == "vibe-queue"
    # Only the distribution is renamed: what an operator types is untouched.
    assert project["scripts"] == {"vq": "vq.cli:main"}


def test_dist_info_prefix_is_the_pep427_normalization() -> None:
    assert admin._dist_info_prefix("vibe-queue") == "vibe_queue"
    assert admin._dist_info_prefix("Vibe.Queue") == "vibe_queue"
    assert admin._dist_info_prefix(vq.LEGACY_DISTRIBUTION_NAME) == "vq"


def test_shell_installers_read_the_same_two_names() -> None:
    """Both PEP 610 readers in the installers read both names, neither preferred.

    ``records(current) or records(legacy)`` was a preference, and a preference
    is an answer: it settled cross-name ambiguity instead of refusing it, and
    turned an unreadable current record into a confident reading of a stale
    one. Both readers now take the pair and union it, and this pins that the
    spellings and the shape stay in step with the package.
    """
    helpers = (ROOT / "scripts" / "_venv_helpers.sh").read_text(encoding="utf-8")
    current = admin._dist_info_prefix(vq.DISTRIBUTION_NAME)
    legacy = admin._dist_info_prefix(vq.LEGACY_DISTRIBUTION_NAME)
    lookups = re.findall(
        r'matches = records\(\("([a-z0-9_]+)", "([a-z0-9_]+)"\)\)', helpers,
    )
    assert lookups == [(current, legacy)] * 2, lookups
    assert "records(current) or records(legacy)" not in helpers
    assert re.search(r'records\("[a-z0-9_]+"\) or records\(', helpers) is None
    assert 'pip uninstall --quiet --yes vq' in helpers


def test_privileged_refresh_derives_wheel_names_from_pyproject() -> None:
    script = (ROOT / "contrib" / "vq-multi-user-refresh").read_text(encoding="utf-8")
    assert 'name = metadata["project"]["name"]' in script
    assert 'wheel_name = f"{normalized}-{version}-py3-none-any.whl"' in script
    assert 'dist_info = f"{normalized}-{version}.dist-info"' in script


def test_the_refresh_shell_never_guesses_the_wheel_name() -> None:
    """The shell uses the name the builder reports, and derives none of its own.

    The rename (#68) changed the builder's output to `vibe_queue-*.whl` and
    left a `find -name 'vq-*.whl'` two hundred lines below matching nothing,
    which killed every privileged refresh at that gate. Checking the builder's
    spelling was what missed it, because the stale pattern was in the shell.
    So this asserts the coupling is gone rather than that both ends agree: the
    shell must learn the name from the build and must not contain a wheel glob
    naming any distribution.
    """
    script = (ROOT / "contrib" / "vq-multi-user-refresh").read_text(encoding="utf-8")
    assert "print(wheel_name)" in script, "the builder must report what it wrote"
    assert 'read -r vq_wheel_name < "$work_dir/vq-wheel-name"' in script
    assert 'vq_wheel="$wheelhouse/$vq_wheel_name"' in script
    # Any `-name '<something>-*.whl'` is a second, hand-maintained spelling of
    # the distribution's name. Dependency wheels are found by other means.
    globs = re.findall(r"-name\s+'([A-Za-z0-9_.]+)-\*\.whl'", script)
    assert globs == [], f"the shell still guesses a wheel name: {globs}"


def test_installed_distribution_resolves_under_the_new_name() -> None:
    try:
        installed = importlib.metadata.version(vq.DISTRIBUTION_NAME)
    except importlib.metadata.PackageNotFoundError:
        pytest.skip("vq is imported from source without an installed distribution")
    assert installed == vq.__version__


def test_the_privileged_refresh_builds_an_installable_wheel(tmp_path: Path) -> None:
    """The hand-built wheel carries the current name, and pip accepts it (#68).

    `contrib/vq-multi-user-refresh` builds vq's wheel with the standard library
    alone, to avoid running a caller-selected build backend as root, so it
    writes the distribution name, the wheel file name and the `.dist-info`
    directory itself. Nothing else checks that those three agree with
    `pyproject.toml`, and pip refuses a wheel whose file name and METADATA
    disagree -- on a privileged host, at the least convenient moment. This
    runs the builder's own code against this checkout and has pip read the
    result.
    """
    script = (ROOT / "contrib" / "vq-multi-user-refresh").read_text(encoding="utf-8")
    match = re.search(r"<<'PY_BUILD_VQ_WHEEL'\n(.*?)\nPY_BUILD_VQ_WHEEL\n", script, re.S)
    assert match is not None, "the wheel builder heredoc moved or was renamed"
    builder = tmp_path / "build_wheel.py"
    builder.write_text(match.group(1), encoding="utf-8")

    # The builder refuses a group- or other-writable source entry, which a
    # checkout may well have; copy the package and tighten the copy instead.
    snapshot = tmp_path / "snapshot"
    (snapshot / "src").mkdir(parents=True)
    shutil.copytree(ROOT / "src" / "vq", snapshot / "src" / "vq")
    shutil.copy2(ROOT / "pyproject.toml", snapshot / "pyproject.toml")
    for path in (snapshot / "src" / "vq").rglob("*"):
        path.chmod(path.stat().st_mode & ~0o022)
    out = tmp_path / "wheelhouse"
    out.mkdir()

    built = subprocess.run(
        [sys.executable, str(builder), str(snapshot), str(out)],
        capture_output=True, text=True, check=False,
    )
    assert built.returncode == 0, built.stdout + built.stderr

    normalized = admin._dist_info_prefix(vq.DISTRIBUTION_NAME)
    wheels = sorted(out.glob("*.whl"))
    assert [path.name for path in wheels] == [
        f"{normalized}-{vq.__version__}-py3-none-any.whl"
    ]
    # The shell reads this line and addresses the wheel by it; a builder that
    # writes one name and announces another strands the refresh at its gate.
    assert built.stdout.strip() == wheels[0].name
    with zipfile.ZipFile(wheels[0]) as archive:
        names = archive.namelist()
        dist_info = f"{normalized}-{vq.__version__}.dist-info"
        assert f"{dist_info}/METADATA" in names
        assert "vq/__init__.py" in names, "the import package is still vq"
        metadata = archive.read(f"{dist_info}/METADATA").decode()
    assert f"Name: {vq.DISTRIBUTION_NAME}\n" in metadata
    assert f"Version: {vq.__version__}\n" in metadata

    # pip is the arbiter of whether those names agree; ask it rather than
    # asserting the rule ourselves. --no-deps keeps this off the network.
    venv = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = venv / "bin" / "python"
    installed = subprocess.run(
        [str(python), "-m", "pip", "install", "--quiet", "--no-deps", str(wheels[0])],
        capture_output=True, text=True, check=False,
    )
    assert installed.returncode == 0, installed.stdout + installed.stderr
    resolved = subprocess.run(
        [str(python), "-c",
         "import importlib.metadata as m;"
         f"d = m.distribution({vq.DISTRIBUTION_NAME!r});"
         "print(d.metadata['Name'], d.version,"
         " [e.value for e in d.entry_points if e.name == 'vq'])"],
        capture_output=True, text=True, check=False,
    )
    assert resolved.returncode == 0, resolved.stdout + resolved.stderr
    assert resolved.stdout.strip() == (
        f"{vq.DISTRIBUTION_NAME} {vq.__version__} ['vq.cli:main']"
    )


def _record(venv: Path, prefix: str, *, editable: bool, project: Path) -> Path:
    metadata = (
        venv / "lib" / "python3.12" / "site-packages"
        / f"{prefix}-0.26.9.dist-info" / "direct_url.json"
    )
    metadata.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, object] = {"url": project.resolve().as_uri()}
    payload["dir_info"] = {"editable": True} if editable else {}
    metadata.write_text(json.dumps(payload), encoding="utf-8")
    return metadata


def _unmarked_venv(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "checkout"
    project.mkdir()
    venv = tmp_path / "venv"
    venv.mkdir()
    (venv / "pyvenv.cfg").write_text("home = test\n", encoding="utf-8")
    return venv, project


def test_records_are_read_under_either_name_and_neither_outranks_the_other(
    tmp_path: Path,
) -> None:
    venv, project = _unmarked_venv(tmp_path)
    assert admin._pep610_records(venv) == []
    legacy = _record(venv, "vq", editable=True, project=project)
    assert admin._pep610_records(venv) == [legacy]
    current = _record(venv, "vibe_queue", editable=False, project=project)
    assert admin._pep610_records(venv) == sorted([legacy, current])


def test_a_pre_rename_venv_still_proves_its_mode(tmp_path: Path) -> None:
    """Adoption of a venv last installed as `vq` reads the record that release left."""
    venv, project = _unmarked_venv(tmp_path)
    _record(venv, "vq", editable=True, project=project)
    assert admin._prove_legacy_managed_install(venv, project) is True


@pytest.mark.parametrize("order", [("vq", "vibe_queue"), ("vibe_queue", "vq")])
def test_a_venv_holding_both_records_is_refused(
    tmp_path: Path, order: tuple[str, str],
) -> None:
    """Two independent installs, two origins: refuse rather than pick one.

    Written in both orders on purpose. "The current name wins, because that is
    the install pip performed last" and "the most recent write wins" agree on
    one ordering and disagree on the other, so a test that wrote only one
    order could not tell the two rules apart -- and the one this replaces
    wrote the current record second, where both rules give the same answer.
    Neither rule survives: a venv vq installed never holds both (the install
    removes the old record first), so both present is a hand-built venv with
    two installs in it, which is exactly the case adoption exists for and
    exactly the case it must not guess at.
    """
    venv, project = _unmarked_venv(tmp_path)
    stale_checkout = tmp_path / "somewhere-else"
    stale_checkout.mkdir()
    origin = {"vibe_queue": project, "vq": stale_checkout}
    for prefix in order:
        _record(venv, prefix, editable=prefix == "vq", project=origin[prefix])

    with pytest.raises(admin.AdminError) as excinfo:
        admin._prove_legacy_managed_install(venv, project)

    message = str(excinfo.value)
    assert "one unambiguous PEP 610 record" in message
    # The operator's next move is deleting one .dist-info, so say which two
    # were found. The refusal this replaces named neither -- and it was not
    # this refusal they saw: the stale record answered, and the complaint was
    # that the origin did not match a checkout their live record does match.
    assert "vibe_queue-0.26.9.dist-info" in message
    assert "vq-0.26.9.dist-info" in message


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads an unreadable directory")
def test_an_unreadable_record_does_not_degrade_to_a_stale_one(
    tmp_path: Path,
) -> None:
    """Mode-0 on the current `.dist-info` must not promote the legacy record.

    `Path.glob` swallows the `PermissionError` and reports "no match", which
    under a name preference made the stale record the answer: the wrong
    checkout and the wrong install mode, reported with no sign that anything
    was unreadable. An unreadable `direct_url.json` already failed closed one
    level down, so this was the reader disagreeing with itself.
    """
    venv, project = _unmarked_venv(tmp_path)
    stale_checkout = tmp_path / "somewhere-else"
    stale_checkout.mkdir()
    _record(venv, "vq", editable=True, project=stale_checkout)
    current = _record(venv, "vibe_queue", editable=False, project=project)
    current.parent.chmod(0o000)
    try:
        with pytest.raises(admin.AdminError, match="cannot read"):
            admin._pep610_records(venv)
    finally:
        current.parent.chmod(0o755)


def _shell_readers() -> dict[str, str]:
    """The two PEP 610 readers embedded in `scripts/_venv_helpers.sh`.

    They run under an isolated external interpreter, never the venv's own, so
    they can be lifted out and run directly. Asserting on their source text
    alone is what let a name preference sit in both of them.
    """
    helpers = (ROOT / "scripts" / "_venv_helpers.sh").read_text(encoding="utf-8")
    bodies = re.findall(r"<<'PY'\n(.*?)\nPY\n", helpers, re.S)
    readers = {}
    for body in bodies:
        if "expected = os.path.realpath" in body:
            readers["matches_checkout"] = body
        elif "dir_info" in body:
            readers["detect_editable"] = body
    assert set(readers) == {"matches_checkout", "detect_editable"}, sorted(readers)
    return readers


def _run_shell_reader(
    which: str, tmp_path: Path, venv: Path, *args: str,
) -> subprocess.CompletedProcess[str]:
    script = tmp_path / f"{which}.py"
    script.write_text(_shell_readers()[which], encoding="utf-8")
    return subprocess.run(
        [sys.executable, "-I", "-S", str(script), str(venv), *args],
        capture_output=True, text=True, check=False,
    )


@pytest.mark.parametrize("prefix", ["vibe_queue", "vq"])
def test_the_shell_detector_answers_one_record_under_either_name(
    tmp_path: Path, prefix: str,
) -> None:
    """The positive control: one record still proves the venv's install mode."""
    venv, project = _unmarked_venv(tmp_path)
    _record(venv, prefix, editable=True, project=project)

    detected = _run_shell_reader("detect_editable", tmp_path, venv)
    matched = _run_shell_reader(
        "matches_checkout", tmp_path, venv, str(project),
    )

    assert (detected.returncode, detected.stdout) == (0, "1"), detected.stderr
    assert matched.returncode == 0, matched.stderr


def test_the_shell_readers_refuse_a_venv_holding_both_records(
    tmp_path: Path,
) -> None:
    """`detect_editable` went from refusing to guess to answering confidently.

    Two payloads under one name already refused. One under each name is the
    same ambiguity and got an answer, because the preference resolved it
    before the anti-ambiguity guard -- which is scoped to one spelling at a
    time -- ever saw both.
    """
    venv, project = _unmarked_venv(tmp_path)
    stale_checkout = tmp_path / "somewhere-else"
    stale_checkout.mkdir()
    _record(venv, "vibe_queue", editable=False, project=project)
    _record(venv, "vq", editable=True, project=stale_checkout)

    detected = _run_shell_reader("detect_editable", tmp_path, venv)
    matched = _run_shell_reader(
        "matches_checkout", tmp_path, venv, str(project),
    )

    assert detected.returncode != 0 and detected.stdout == ""
    assert matched.returncode != 0


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads an unreadable directory")
def test_the_shell_detector_does_not_answer_from_a_stale_record(
    tmp_path: Path,
) -> None:
    """An unreadable current record must not hand the question to the old one.

    This reader runs on the ordinary `scripts/update.sh` path to preserve the
    installed mode. Answering from a pre-rename record is how a copied install
    is reported editable, or an editable one copied -- the silent conversion
    the reader's own module comment exists to prevent.
    """
    venv, project = _unmarked_venv(tmp_path)
    stale_checkout = tmp_path / "somewhere-else"
    stale_checkout.mkdir()
    _record(venv, "vq", editable=True, project=stale_checkout)
    current = _record(venv, "vibe_queue", editable=False, project=project)
    current.parent.chmod(0o000)
    try:
        detected = _run_shell_reader("detect_editable", tmp_path, venv)
        matched = _run_shell_reader(
            "matches_checkout", tmp_path, venv, str(stale_checkout),
        )
    finally:
        current.parent.chmod(0o755)

    assert detected.returncode != 0 and detected.stdout == ""
    assert matched.returncode != 0
