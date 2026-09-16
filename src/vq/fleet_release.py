"""Accepted release-report discovery for driver-owned fleet rollouts.

The operator-facing command deliberately accepts no tag, version, or SHA.
Release automation writes immutable, evidence-carrying reports under
``releases/`` in an explicitly configured private operations checkout.
That checkout fetches ``origin/main`` and tags,
validates every candidate, and selects the newest semantic release.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

# These live in the dependency-free `vq.report_paths`, so that
# `legacy_failure_transition` can compare report paths without importing this
# module. Re-exported, so existing `fleet_release.X` callers are unchanged.
from vq.report_paths import REPORT_DIRECTORIES as REPORT_DIRECTORIES
from vq.report_paths import REPORT_DIRECTORY as REPORT_DIRECTORY
from vq.report_paths import is_report_path as is_report_path
from vq.report_paths import report_paths_for as report_paths_for
from vq.report_paths import same_report as same_report

REPORT_SCHEMA = "vq.fleet.release_report/3"
"""Schema newly generated reports carry.

``/3`` moves ``repo`` and ``project_id`` onto each pin and drops the
single top-level ``project_id``. Under ``/2`` one id covered all four
pins, which was only ever true while every component lived in one
repository; after the 2026-09-08 split the four pins resolve in three
different GitLab projects.
"""

LEGACY_REPORT_SCHEMAS = ("vq.fleet.release_report/2",)
"""Older schemas still accepted on read.

Reports already committed under ``/2`` stay deployable: a rollout that
could not read its own history could not prove a supersession chain.
``/2`` pins are read as monorepo pins -- see ``MONOREPO_PIN_SOURCE``.
"""

SUPPORTED_REPORT_SCHEMAS = (REPORT_SCHEMA, *LEGACY_REPORT_SCHEMAS)

_MAX_REPORTED_REJECTIONS = 3
"""How many rejection reasons a discovery failure quotes, newest first."""

_PIN_ANCESTRY_REF = "origin/main"
"""Ref a /3 pin's SHA must descend from, inside its own repository.

Every component tags its release from its own mainline, so ``origin/main``
is the one ref that means the same thing in all three repositories.
"""

MONOREPO_PIN_SOURCE = ("mpei/vibeqc", 19)
"""Where a ``/2`` pin came from: the pre-split monorepo, now the archive.

A ``/2`` report carries no per-pin origin, so every pin in one is
attributed here rather than guessed from the pin name. Project 19 is
frozen; this is a statement about history, not a deploy target.
"""

PIN_SOURCES = {
    "release": ("mpei/vibe-qc", 34),
    "dev": ("mpei/vibe-qc", 34),
    "vq": ("mpei/vibe-queue", 36),
    "vibe_view": ("mpei/vibe-view", 35),
}
"""Repository each ``/3`` pin resolves in, after the 2026-09-08 split.

``release`` and ``dev`` are two refs of one repository; ``vq`` and
``vibe_view`` are separate projects. Path-based GitLab lookups 404 on
this instance, so the numeric id -- not the slug -- is what callers use.
"""
def _detect_layout() -> tuple[Path, str]:
    """Return (repo root, repo-relative report directory) for this install.

    Both layouts are live during the 2026-09 split transition:

      * vibe-queue's own repository -- reports at ``releases/``;
      * the pre-split monorepo, which the fleet still deploys from --
        reports at ``vibe-queue/releases/``.

    Detected rather than assumed, because a wrong guess makes rollout-latest
    look in a directory that does not exist, and because rollout receipts
    already persisted on the fleet record the path they came from.
    """
    project = Path(__file__).resolve().parents[2]
    if (project / ".git").exists():
        return project, "releases"
    if (project.parent / ".git").exists():
        return project.parent, "vibe-queue/releases"
    return project, "releases"


PIN_NAMES = ("release", "dev", "vq", "vibe_view")

PIN_GATING_JOBS = {
    "release": "build-test",
    "dev": "build-test",
    "vq": "test",
    "vibe_view": "test",
}
"""Mandatory gate for each ``/3`` pin, in that pin's OWN repository.

The monorepo ran every component's gate in one pipeline, so the jobs needed
distinct names (``test-vq``, ``vibe-view-test``). After the split each
component's suite is simply ``test`` in its own project -- those old names do
not exist anywhere any more, so a ``/3`` pin cannot be evidenced by one.
"""

LEGACY_PIN_GATING_JOBS = {
    "release": "build-test",
    "dev": "build-test",
    "vq": "test-vq",
    "vibe_view": "vibe-view-test",
}
"""Gate names a ``/2`` report carries: the monorepo pipeline's job names.

Kept so previously committed reports still validate. Applying the ``/3``
names to a ``/2`` report would reject the fleet's entire history.
"""


def pin_gating_jobs(schema: object) -> Mapping[str, str]:
    """Gate names expected for a report of this schema."""
    if schema in LEGACY_REPORT_SCHEMAS:
        return LEGACY_PIN_GATING_JOBS
    return PIN_GATING_JOBS

_SHA40 = re.compile(r"^[0-9a-f]{40}$")
_TAG = re.compile(r"^v([0-9]+)\.([0-9]+)\.([0-9]+)$")

RELEASE_CANDIDATE_PREFIX = "release-candidate/"
"""Branch prefix that carries a release-gate pipeline before the tag exists."""

# Reports committed before this deploy-side hardening legitimately used
# successful ``main`` pipeline evidence under the then-current policy.  The
# legacy rollout reconciler may authenticate an exact historical blob from
# before this commit; normal report discovery never relaxes the current gate.
LEGACY_MAIN_EVIDENCE_CUTOFF = (
    "107ea759691c5c9dee6ab4c868b267a92494ed27"
)


def is_release_gate_ref(ref: object) -> bool:
    """True iff a pipeline on ``ref`` is a release-gate pipeline.

    Only two refs run the ONE CI pipeline of a release, which executes every
    component gate on the exact release tree: a ``vX.Y.Z`` tag, and the
    ``release-candidate/*`` branch used to obtain that evidence before the tag
    exists. Under the release-only policy an ordinary ``main`` push creates no
    pipeline at all, and a ``release`` branch pipeline runs docs jobs only.

    ``make_release_report.py`` enforces the same rule when it *generates* a
    report. This is the deploy-side half: a report is a committed JSON file,
    so one written by an older vq, hand-edited, or simply already on disk must
    not become deployable just because the generator has since been fixed.
    ``tests/test_fleet_release.py`` pins the two predicates to one behaviour.

    This is a consistency gate, not an authenticity one: ``ref`` is the
    report's own claim, and nothing here re-queries GitLab (parsing must stay
    deterministic and offline, from immutable git objects). It catches legacy,
    stale, and mistaken evidence — not a deliberate forgery.
    """
    if not isinstance(ref, str):
        return False
    return bool(_TAG.match(ref)) or ref.startswith(RELEASE_CANDIDATE_PREFIX)

Runner = Callable[..., subprocess.CompletedProcess[str]]


class FleetReleaseError(RuntimeError):
    """An accepted fleet release report could not be resolved safely."""


@dataclass(frozen=True)
class FleetPin:
    """One immutable module pin and its accepted CI evidence."""

    name: str
    sha: str
    version: str
    deploy_flags: tuple[str, ...]
    gating_job: str
    pipeline_id: int
    evidence_sha: str
    acceptance_rule: str
    tag: str | None = None
    repo: str | None = None
    project_id: int | None = None


@dataclass(frozen=True)
class FleetReleaseReport:
    """Validated report selected from the runtime clone."""

    source_ref: str
    source_path: str
    digest_sha256: str
    generated_at: str | None
    release_version: tuple[int, int, int]
    pins: Mapping[str, FleetPin]
    raw: Mapping[str, Any]
    # Discovery diagnostics only: never part of the immutable report or digest.
    rejected_candidates: tuple[str, ...] = ()

    @property
    def release(self) -> FleetPin:
        return self.pins["release"]


def runtime_repo() -> Path:
    """Return the checkout from which this installed vq is running.

    This was ``parents[3]`` -- the monorepo root, one level above
    ``vibe-queue/`` -- until the 2026-09 split. Both layouts are live during
    the transition, so the root is detected rather than assumed.

    This identity authenticates the controller and its installed source. It
    must never be replaced by the separately configured :func:`report_repo`.
    """
    repo = _detect_layout()[0]
    if not (repo / ".git").exists():
        if "site-packages" in Path(__file__).resolve().parts:
            # Name the install mode, not the directory. The directory is a
            # consequence nobody chose, and an operator reading only the path
            # has no way to tell that the fix is how vq was installed. A
            # routine `vq admin update vibeqc-queue HOST` used to convert an
            # editable controller to copied silently; see scripts/update.sh.
            raise FleetReleaseError(
                f"vq runtime source {repo} is not a git checkout: this vq is "
                "installed COPIED into a virtualenv, so it resolves into "
                "site-packages rather than the checkout it was built from. "
                "rollout-latest needs the controller installed editable -- "
                "reinstall it with scripts/update.sh --editable"
            )
        raise FleetReleaseError(
            f"vq runtime source {repo} is not a git checkout; "
            "rollout-latest requires the managed runtime clone"
        )
    return repo


def report_repo(cfg=None) -> Path:
    """Resolve explicit private report storage without changing runtime identity."""
    if cfg is None:
        from vq import config

        cfg = config.load_config()
    value = cfg.fleet_report_repo
    if not value or not Path(value).is_absolute():
        raise FleetReleaseError(
            "fleet_report_repo must name an absolute external private Git checkout; "
            "configure accepted-report storage before using report-based updates"
        )
    path = Path(value).resolve()
    for interface in (Path(value).absolute(), path):
        for parent in (interface, *interface.parents):
            if parent.name.casefold() == ".git" or any(
                (parent / marker).is_file() for marker in (
                    "src/vq/__init__.py", "vibe-queue/src/vq/__init__.py",
                    "python/vibeqc/__init__.py", "src/vibeview/__init__.py",
                    "conformance/run_conformance.py",
                )
            ):
                raise FleetReleaseError(
                    "fleet_report_repo must be outside product source checkouts; "
                    "use fleet_report_history_repo only for retained historical evidence"
                )
    return path


def _git(
    repo: Path,
    *args: str,
    runner: Runner = subprocess.run,
    timeout: float = 120,
    strip: bool = True,
) -> str:
    proc = runner(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "(no output)").strip()
        raise FleetReleaseError(f"git {' '.join(args)} failed: {detail}")
    return proc.stdout.strip() if strip else proc.stdout


def _require_sha(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _SHA40.fullmatch(value) is None:
        raise FleetReleaseError(f"{field} must be a full lowercase 40-hex SHA")
    return value


def _semver(tag: object, *, field: str) -> tuple[int, int, int]:
    if not isinstance(tag, str):
        raise FleetReleaseError(f"{field} must be a vX.Y.Z tag")
    match = _TAG.fullmatch(tag)
    if match is None:
        raise FleetReleaseError(f"{field} must be a vX.Y.Z tag")
    return tuple(int(part) for part in match.groups())


def _pin_source(
    pin_obj: Mapping[str, Any],
    *,
    name: str,
    schema: Any,
    source_path: str,
) -> tuple[str, int]:
    """Resolve which repository and GitLab project a pin resolves in.

    ``/3`` states this per pin. ``/2`` predates the split and states it
    nowhere, so its pins are attributed to the monorepo rather than
    guessed from the pin name -- a ``/2`` ``vq`` pin genuinely came from
    project 19, not from the vibe-queue repository that did not yet exist.

    A ``/3`` pin is required to carry both fields and to agree with
    ``PIN_SOURCES``. A report that named a different project for a pin
    would send the evidence lookup to a repository where that SHA either
    does not exist or, worse, names an unrelated commit.
    """
    if schema in LEGACY_REPORT_SCHEMAS:
        return MONOREPO_PIN_SOURCE

    expected_repo, expected_project_id = PIN_SOURCES[name]
    repo = pin_obj.get("repo")
    project_id = pin_obj.get("project_id")
    if not isinstance(repo, str) or not repo.strip():
        raise FleetReleaseError(
            f"{source_path}: pins.{name}.repo must be a non-empty string"
        )
    if not isinstance(project_id, int) or isinstance(project_id, bool):
        raise FleetReleaseError(
            f"{source_path}: pins.{name}.project_id must be an integer"
        )
    if (repo, project_id) != (expected_repo, expected_project_id):
        raise FleetReleaseError(
            f"{source_path}: pins.{name} resolves in {expected_repo!r} "
            f"(project {expected_project_id}), not {repo!r} (project {project_id})"
        )
    return repo, project_id


def parse_report(
    payload: Mapping[str, Any],
    *,
    source_ref: str,
    source_path: str,
    raw_bytes: bytes,
    allow_legacy_main_evidence: bool = False,
) -> FleetReleaseReport:
    """Validate a report without consulting moving CI state.

    Accepts every schema in ``SUPPORTED_REPORT_SCHEMAS``. The compatibility
    window is load-bearing rather than politeness: ``rollout-latest``
    reads previously committed reports to prove a supersession chain, so
    rejecting ``/2`` here would make vq unable to read its own history.
    """
    schema = payload.get("schema")
    if schema not in SUPPORTED_REPORT_SCHEMAS:
        expected = " or ".join(repr(name) for name in SUPPORTED_REPORT_SCHEMAS)
        raise FleetReleaseError(
            f"{source_path}: schema must be {expected}"
        )
    if payload.get("all_pins_accepted") is not True:
        raise FleetReleaseError(f"{source_path}: all_pins_accepted is not true")
    pins_obj = payload.get("pins")
    if not isinstance(pins_obj, dict):
        raise FleetReleaseError(f"{source_path}: pins must be an object")
    missing = [name for name in PIN_NAMES if name not in pins_obj]
    extra = [name for name in pins_obj if name not in PIN_NAMES]
    if missing or extra:
        raise FleetReleaseError(
            f"{source_path}: pins mismatch; missing={missing}, extra={extra}"
        )

    pins: dict[str, FleetPin] = {}
    for name in PIN_NAMES:
        pin_obj = pins_obj[name]
        if not isinstance(pin_obj, dict):
            raise FleetReleaseError(f"{source_path}: pins.{name} must be an object")
        if pin_obj.get("accepted") is not True:
            raise FleetReleaseError(f"{source_path}: pins.{name} is not accepted")
        acceptance_rule = pin_obj.get("acceptance_rule")
        if acceptance_rule not in {"A", "B", "B-prime"}:
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.acceptance_rule is not deployable"
            )
        if name != "release" and acceptance_rule != "A":
            raise FleetReleaseError(
                f"{source_path}: pins.{name} must use exact-SHA rule A"
            )
        pin_repo, pin_project_id = _pin_source(
            pin_obj, name=name, schema=schema, source_path=source_path
        )
        sha = _require_sha(pin_obj.get("sha"), field=f"pins.{name}.sha")
        version = pin_obj.get("version")
        if not isinstance(version, str) or not version.strip():
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.version must be non-empty"
            )
        evidence = pin_obj.get("ci_evidence")
        if not isinstance(evidence, dict):
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.ci_evidence must be an object"
            )
        expected_job = pin_gating_jobs(schema)[name]
        if evidence.get("gating_job") != expected_job:
            raise FleetReleaseError(
                f"{source_path}: pins.{name} requires {expected_job!r} evidence"
            )
        if evidence.get("gating_job_status") != "success":
            raise FleetReleaseError(
                f"{source_path}: pins.{name} gating job is not successful"
            )
        evidence_ref = evidence.get("ref")
        if not is_release_gate_ref(evidence_ref) and not (
            allow_legacy_main_evidence and evidence_ref == "main"
        ):
            # A green gate on an ordinary main pipeline does not prove a
            # release tree. v0.15.62 was the demonstration: its release and
            # dev pins were accepted from main pipeline 4630 while the
            # release-gate pipeline 4631 had FAILED build-test on the
            # identical tree, and `rollout-latest` deploys the newest
            # accepted report -- so an unevidencable release was deployable.
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.ci_evidence.ref "
                f"{evidence_ref!r} is not a release-gate ref "
                f"(a vX.Y.Z tag or {RELEASE_CANDIDATE_PREFIX}*); a main "
                "pipeline does not prove a release tree"
            )
        pipeline_id = evidence.get("pipeline_id")
        if not isinstance(pipeline_id, int) or pipeline_id <= 0:
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.ci_evidence.pipeline_id "
                "must be positive"
            )
        evidence_sha = _require_sha(
            evidence.get("sha"),
            field=f"pins.{name}.ci_evidence.sha",
        )
        if acceptance_rule == "A" and evidence_sha != sha:
            raise FleetReleaseError(
                f"{source_path}: pins.{name} rule A evidence SHA must equal pin SHA"
            )
        flags_obj = pin_obj.get("deploy_flags")
        if not isinstance(flags_obj, list) or not all(
            isinstance(value, str) for value in flags_obj
        ):
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.deploy_flags must be string argv"
            )
        tag = pin_obj.get("tag") if name == "release" else None
        if name == "release":
            release_version = _semver(tag, field="pins.release.tag")
            _require_sha(
                pin_obj.get("tag_object"),
                field="pins.release.tag_object",
            )
            expected_flags = ["--tag", tag, "--expected-sha", sha]
        else:
            expected_flags = ["--expected-sha", sha]
        if flags_obj != expected_flags:
            raise FleetReleaseError(
                f"{source_path}: pins.{name}.deploy_flags must be "
                f"{expected_flags!r}"
            )
        pins[name] = FleetPin(
            name=name,
            sha=sha,
            version=version,
            deploy_flags=tuple(flags_obj),
            gating_job=expected_job,
            pipeline_id=pipeline_id,
            evidence_sha=evidence_sha,
            acceptance_rule=acceptance_rule,
            tag=tag if isinstance(tag, str) else None,
            repo=pin_repo,
            project_id=pin_project_id,
        )

    filename = Path(source_path).name
    expected_filename = f"{pins['release'].tag}.json"
    if filename != expected_filename:
        raise FleetReleaseError(
            f"{source_path}: report filename must be {expected_filename!r}"
        )
    return FleetReleaseReport(
        source_ref=source_ref,
        source_path=source_path,
        digest_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        generated_at=(
            payload.get("generated_at")
            if isinstance(payload.get("generated_at"), str)
            else None
        ),
        release_version=release_version,
        pins=pins,
        raw=payload,
    )


def resolve_pin_repos(
    explicit: Mapping[str, Path | str] | None = None,
) -> dict[str, Path]:
    """Map each pin ``repo`` slug to a local checkout.

    ``explicit`` wins when given (tests and callers that already know the
    paths); otherwise the driver's ``pin_source_repos`` config is consulted.
    Config is imported lazily so this module keeps working without one --
    a ``/2`` report never needs a mapping at all.
    """
    if explicit is not None:
        return {slug: Path(path) for slug, path in explicit.items()}
    try:
        from vq import config as _config

        cfg = _config.load_config()
    except Exception:
        # No usable config is not an error here: it only matters if a /3
        # report actually asks for a slug, and that failure names the slug.
        return {}
    return {slug: Path(path) for slug, path in cfg.pin_source_repos.items()}


def _repo_for_pin(
    pin: FleetPin,
    *,
    schema: object,
    runtime_repo_path: Path,
    pin_repos: Mapping[str, Path],
    source_path: str,
) -> Path:
    """The checkout a pin's provenance must be verified in.

    A ``/2`` report predates the split: all four pins came from the monorepo,
    which is the runtime clone, so it keeps the historical behaviour exactly.

    A ``/3`` pin names its own repository, and checking it anywhere else is
    worse than not checking it -- the SHA either will not resolve, or will
    resolve to an unrelated commit that happens to exist there.
    """
    if schema in LEGACY_REPORT_SCHEMAS:
        # A /2 pin is a monorepo commit, and its tag is a monorepo tag. Before
        # the split the runtime clone WAS the monorepo, so it answered both.
        # It no longer does: vibe-queue has fresh history and carries neither.
        # A retained pre-split checkout does, which is a second reason to keep
        # one beyond rollback -- without it the fleet cannot authenticate its
        # own history. Falls back to the runtime clone when unconfigured, so a
        # driver still on the monorepo behaves exactly as before.
        legacy = pin_repos.get(MONOREPO_PIN_SOURCE[0])
        if legacy is None:
            from vq import config

            cfg = config.load_config()
            if cfg.fleet_report_repo and runtime_repo_path.resolve() == report_repo(cfg):
                raise FleetReleaseError(
                    f"{source_path}: historical pins require an explicit retained "
                    "monorepo under pin_source_repos; private report storage "
                    "cannot supply product ancestry"
                )
        return legacy if legacy is not None else runtime_repo_path
    slug = pin.repo
    if slug is None:
        raise FleetReleaseError(
            f"{source_path}: pins.{pin.name} carries no repo slug"
        )
    resolved = pin_repos.get(slug)
    if resolved is None:
        raise FleetReleaseError(
            f"{source_path}: pins.{pin.name} resolves in {slug!r}, but no "
            f"local checkout is configured for it. Add it under "
            f"[pin_source_repos] in the driver config, e.g.\n"
            f'    "{slug}" = "/path/to/checkout"'
        )
    if not (resolved / ".git").exists():
        raise FleetReleaseError(
            f"{source_path}: pin_source_repos[{slug!r}] = {resolved} is not a "
            "git checkout"
        )
    return resolved


def _load_report_from_git(
    repo: Path,
    *,
    ref: str,
    path: str,
    runner: Runner,
    allow_legacy_main_evidence: bool = False,
    pin_repos: Mapping[str, Path] | None = None,
) -> FleetReleaseReport:
    text = _git(
        repo,
        "show",
        f"{ref}:{path}",
        runner=runner,
        strip=False,
    )
    raw_bytes = text.encode()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise FleetReleaseError(f"{path}: invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise FleetReleaseError(f"{path}: report root must be an object")
    report = parse_report(
        payload,
        source_ref=ref,
        source_path=path,
        raw_bytes=raw_bytes,
        allow_legacy_main_evidence=allow_legacy_main_evidence,
    )
    schema = payload.get("schema")
    resolved_repos = resolve_pin_repos(pin_repos)
    # The anchor tag is vibe-qc's, so it must be resolved in vibe-qc -- not in
    # whichever repository happens to hold the reports. Post-split those are
    # different repositories, and the vibe-qc tag simply does not exist in
    # vibe-queue.
    release_repo = _repo_for_pin(
        report.release,
        schema=schema,
        runtime_repo_path=repo,
        pin_repos=resolved_repos,
        source_path=path,
    )
    peeled = _git(
        release_repo,
        "rev-parse",
        f"{report.release.tag}^{{commit}}",
        runner=runner,
    )
    if peeled != report.release.sha:
        raise FleetReleaseError(
            f"{path}: tag {report.release.tag} peels to {peeled}, "
            f"not report SHA {report.release.sha} (in {release_repo})"
        )
    tag_object = _git(
        release_repo,
        "rev-parse",
        str(report.release.tag),
        runner=runner,
    )
    reported_tag_object = report.raw["pins"]["release"]["tag_object"]
    if tag_object != reported_tag_object:
        raise FleetReleaseError(
            f"{path}: tag object is {tag_object}, "
            f"not reported {reported_tag_object}"
        )
    for name, pin in report.pins.items():
        pin_repo = _repo_for_pin(
            pin,
            schema=schema,
            runtime_repo_path=repo,
            pin_repos=resolved_repos,
            source_path=path,
        )
        # ``ref`` identifies a commit in the repository the REPORT came from.
        # That is the right ancestry ref only while the pins live there too --
        # the pre-split case, where one repository held reports and code alike.
        # Once a pin resolves elsewhere (a /3 pin in its own repository, or a
        # /2 pin in a retained monorepo checkout) that commit does not exist
        # there, or names something unrelated, so ancestry is checked against
        # that repository's own mainline instead.
        pin_ref = (
            ref
            if schema in LEGACY_REPORT_SCHEMAS and pin_repo == repo
            else _PIN_ANCESTRY_REF
        )
        for label, sha in (
            (f"pins.{name}.sha", pin.sha),
            (f"pins.{name} evidence SHA", pin.evidence_sha),
        ):
            proc = runner(
                [
                    "git",
                    "-C",
                    str(pin_repo),
                    "merge-base",
                    "--is-ancestor",
                    sha,
                    pin_ref,
                ],
                capture_output=True,
                text=True,
                timeout=120,
                check=False,
            )
            if proc.returncode != 0:
                where = "" if pin_repo == repo else f" in {pin_repo}"
                raise FleetReleaseError(
                    f"{path}: {label} {sha} is not an ancestor of "
                    f"{pin_ref}{where}. A stale checkout reports this for a "
                    "commit that genuinely landed -- fetch it and retry "
                    "before concluding the pin is bad."
                )
        if name == "release" and pin.acceptance_rule in {"B", "B-prime"}:
            _verify_historical_release_evidence(
                pin_repo,
                pin,
                path=path,
                runner=runner,
            )
    return report


def _verify_historical_release_evidence(
    repo: Path,
    pin: FleetPin,
    *,
    path: str,
    runner: Runner,
) -> None:
    """Recheck historical B/B-prime claims from immutable git objects."""
    pin_before_evidence = git_is_ancestor(
        repo,
        pin.sha,
        pin.evidence_sha,
        runner=runner,
    )
    evidence_before_pin = git_is_ancestor(
        repo,
        pin.evidence_sha,
        pin.sha,
        runner=runner,
    )
    if not (pin_before_evidence is True or evidence_before_pin is True):
        raise FleetReleaseError(
            f"{path}: release pin and evidence SHA are not comparable"
        )
    if pin.acceptance_rule == "B":
        pin_tree = _git(repo, "rev-parse", f"{pin.sha}^{{tree}}", runner=runner)
        evidence_tree = _git(
            repo,
            "rev-parse",
            f"{pin.evidence_sha}^{{tree}}",
            runner=runner,
        )
        if pin_tree != evidence_tree:
            raise FleetReleaseError(
                f"{path}: rule B release and evidence trees differ"
            )
        return

    base, head = (
        (pin.evidence_sha, pin.sha)
        if evidence_before_pin is True
        else (pin.sha, pin.evidence_sha)
    )
    changed = _git(
        repo,
        "diff",
        "--name-only",
        base,
        head,
        runner=runner,
    ).splitlines()
    forbidden = [
        name
        for name in changed
        if name not in {"CHANGELOG.md", "pyproject.toml"}
        and not name.startswith("docs/")
    ]
    if forbidden:
        raise FleetReleaseError(
            f"{path}: rule B-prime includes build-affecting paths {forbidden}"
        )
    if "pyproject.toml" not in changed:
        return
    diff = _git(
        repo,
        "diff",
        "--unified=0",
        base,
        head,
        "--",
        "pyproject.toml",
        runner=runner,
    )
    changed_lines = [
        line
        for line in diff.splitlines()
        if line.startswith(("+", "-"))
        and not line.startswith(("+++", "---"))
    ]
    if not changed_lines or any(
        re.fullmatch(r"[+-]version\s*=\s*.+", line) is None
        for line in changed_lines
    ):
        raise FleetReleaseError(
            f"{path}: rule B-prime pyproject diff is not version-only"
        )


def discover_latest_report(
    repo: Path | None = None,
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    series: tuple[int, int] | None = None,
    runner: Runner = subprocess.run,
    pin_repos: Mapping[str, Path] | None = None,
) -> FleetReleaseReport:
    """Fetch and select the newest accepted report committed on ``ref``.

    ``series`` restricts discovery to one major/minor line without pinning a
    patch release. This lets long-running consumers follow the newest accepted
    immutable patch in their supported series even after a newer series opens.
    """
    repo = (repo or report_repo()).resolve()
    pin_repos = resolve_pin_repos(pin_repos)
    if fetch:
        _fetch_report_sources(repo, pin_repos, runner=runner)
    names = _git(
        repo,
        "ls-tree",
        "-r",
        "--name-only",
        ref,
        "--",
        *REPORT_DIRECTORIES,
        runner=runner,
    ).splitlines()
    candidates = [
        name
        for name in names
        if is_report_path(name)
        and name.endswith(".json")
        and _TAG.fullmatch(Path(name).stem) is not None
        and (
            series is None
            or _semver(Path(name).stem, field="release report filename")[:2]
            == series
        )
    ]
    if not candidates:
        qualifier = f" for {series[0]}.{series[1]}.x" if series else ""
        raise FleetReleaseError(
            f"no fleet release reports{qualifier} found under "
            f"{' or '.join(d + '/' for d in REPORT_DIRECTORIES)} on {ref}"
        )

    # Newest-first, stop at the first accepted report. Only the newest
    # accepted report is ever deployed, so validating every historical
    # report (~11 git subprocesses each) added seconds per release to
    # every dry run for no decision value. A rejected newer candidate is
    # skipped exactly as before (an unaccepted or hand-damaged report
    # must not brick discovery of the older accepted one); its rejection
    # reasons travel with the selected report, so callers cannot mistake a
    # fallback for proof that the newest release has converged.
    def _release_key(path: str) -> tuple[int, int, int]:
        match = _TAG.fullmatch(Path(path).stem)
        assert match is not None  # candidates are pre-filtered
        return tuple(int(part) for part in match.groups())

    rejected: list[str] = []
    for path in sorted(candidates, key=_release_key, reverse=True):
        try:
            report = _load_report_from_git(
                repo,
                ref=ref,
                path=path,
                runner=runner,
                pin_repos=pin_repos,
            )
        except FleetReleaseError as exc:
            rejected.append(f"{path}: {exc}")
        else:
            return replace(report, rejected_candidates=tuple(rejected))
    # Report the newest few rejections, not all of them. Discovery searches
    # both layouts, so a vibe-queue checkout sees every monorepo-era report
    # and rejects all of them (their tags are not in this fresh history) --
    # concatenating 100+ multi-line git errors buries the one that matters.
    # Candidates are walked newest-first, so the head of this list is the
    # most relevant.
    if not rejected:
        detail = "(no candidates)"
    else:
        shown = rejected[:_MAX_REPORTED_REJECTIONS]
        detail = "; ".join(shown)
        remaining = len(rejected) - len(shown)
        if remaining > 0:
            detail += f"; (+{remaining} older candidate(s) also rejected)"
    raise FleetReleaseError(f"no accepted fleet release report: {detail}")


def _fetch_report_sources(
    repo: Path, pin_repos: Mapping[str, Path], *, runner: Runner,
) -> None:
    """Refresh each distinct checkout once, under its lifecycle fence.

    A failed fetch is fatal: validating cached refs after it would turn an
    unavailable current release into a successful stale selection. Fetch
    changes refs only, never the checkout, index or installed environment.
    """
    from vq import admin

    sources = sorted({repo.resolve(), *(path.resolve() for path in pin_repos.values())})
    try:
        resources = tuple(
            ("checkout", str(admin._canonical_lifecycle_checkout(source)))
            for source in sources
        )
        with admin.toolset_lifecycle_lock(
            [], action="vq-report-fetch", extra_resources=resources,
        ):
            for source in sources:
                try:
                    _git(source, "fetch", "origin", "main", "--tags", "--quiet", runner=runner)
                except (FleetReleaseError, OSError, subprocess.SubprocessError) as exc:
                    raise FleetReleaseError(
                        f"could not refresh report source {source}: {exc}"
                    ) from exc
    except admin.AdminError as exc:
        raise FleetReleaseError(f"could not fence report source fetch: {exc}") from exc


def discovery_warning(report: FleetReleaseReport) -> str | None:
    """Bounded explanation when discovery selected a fallback candidate."""
    rejected = report.rejected_candidates
    if not rejected:
        return None
    detail = "; ".join(rejected[:_MAX_REPORTED_REJECTIONS])
    remaining = len(rejected) - _MAX_REPORTED_REJECTIONS
    if remaining > 0:
        detail += f"; (+{remaining} other candidate(s) also rejected)"
    return (
        f"selected fallback {report.release.tag}; newer report candidate(s) "
        f"rejected: {detail}"
    )


def require_latest_report(report: FleetReleaseReport) -> None:
    """Do not authorize latest-release mutation or convergence from a fallback."""
    warning = discovery_warning(report)
    if warning is not None:
        raise FleetReleaseError(
            f"{warning}. Resolve the newer report rejection and retry; "
            "a fallback cannot authorize latest-report rollout or verification."
        )


def discover_report(
    tag: str,
    repo: Path | None = None,
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    runner: Runner = subprocess.run,
    pin_repos: Mapping[str, Path] | None = None,
) -> FleetReleaseReport:
    """Resolve one explicitly selected accepted-report identity.

    Unlike ``discover_latest_report``, this never falls through to an older
    candidate. The caller named an immutable identity, so a missing, damaged,
    or unaccepted report is a hard failure rather than permission to deploy a
    different release.
    """
    _semver(tag, field="accepted report identity")
    repo = (repo or report_repo()).resolve()
    pin_repos = resolve_pin_repos(pin_repos)
    if fetch:
        _fetch_report_sources(repo, pin_repos, runner=runner)
    candidates = report_paths_for(tag)
    path = candidates[0]
    last: FleetReleaseError | None = None
    for candidate in candidates:
        try:
            return _load_report_from_git(
                repo,
                ref=ref,
                path=candidate,
                runner=runner,
                pin_repos=pin_repos,
            )
        except FleetReleaseError as err:
            # Keep the first layout's error: it names the canonical path, so
            # the message stays stable regardless of which layout vq runs in.
            last = last or err
    try:
        raise last if last is not None else FleetReleaseError(
            f"{path}: report not found on {ref}"
        )
    except FleetReleaseError as exc:
        raise FleetReleaseError(
            f"accepted report {tag!r} could not be selected: {exc}"
        ) from exc


def discover_historical_report_by_digest(
    source_path: str,
    digest_sha256: str,
    repo: Path | None = None,
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    runner: Runner = subprocess.run,
    pin_repos: Mapping[str, Path] | None = None,
) -> FleetReleaseReport:
    """Authenticate a digest in current storage or its retained original history.

    The historical loader still proves the exact blob and original ancestry.
    Copying an old report into the operations repository does not grant it the
    pre-hardening exception: only its original historical commit can do that.
    """
    selected = (repo or report_repo()).resolve()
    sources = [selected]
    from vq import config

    cfg = config.load_config()
    if cfg.fleet_report_repo and selected == report_repo(cfg):
        history = cfg.fleet_report_history_repo
        if history:
            if not Path(history).is_absolute():
                raise FleetReleaseError("fleet_report_history_repo must be absolute")
            historical = Path(history).resolve()
            if historical not in sources:
                sources.append(historical)
    failures = []
    for source in sources:
        try:
            return _discover_historical_report_by_digest(
                source_path, digest_sha256, source, ref=ref, fetch=fetch,
                runner=runner, pin_repos=pin_repos,
            )
        except FleetReleaseError as exc:
            failures.append(str(exc))
    raise FleetReleaseError("; ".join(failures))


def _discover_historical_report_by_digest(
    source_path: str,
    digest_sha256: str,
    repo: Path | None = None,
    *,
    ref: str = "origin/main",
    fetch: bool = True,
    runner: Runner = subprocess.run,
    pin_repos: Mapping[str, Path] | None = None,
) -> FleetReleaseReport:
    """Authenticate one exact report blob from the mainline history.

    This is a migration-only primitive for pre-recorder rollout journals.  A
    report may since have been retracted or replaced at the same path, so its
    journaled SHA-256—not its filename—is the identity.  Normal deployment
    discovery remains current-tree-only and keeps the present release-gate
    policy.

    Two legacy reports predate the deploy-side ban on ordinary ``main`` CI
    evidence.  That one historical rule is accepted only when the exact blob's
    commit is an ancestor of the hardening commit's parent.  Every other report
    invariant, tag object, pin ancestry, evidence SHA, and deploy argv remains
    validated by the current loader.
    """
    tag = Path(source_path).stem
    _semver(tag, field="historical accepted report identity")
    # A persisted receipt records the path it came from, so both layouts are
    # legitimate here; every receipt on pbs-cluster and slurm-cluster says vibe-queue/.
    if source_path not in report_paths_for(tag):
        raise FleetReleaseError(
            "historical report path must be exactly one of "
            f"{report_paths_for(tag)!r}"
        )
    if re.fullmatch(r"[0-9a-f]{64}", digest_sha256) is None:
        raise FleetReleaseError(
            "historical report digest must be a full lowercase SHA-256"
        )

    repo = (repo or report_repo()).resolve()
    if fetch:
        _fetch_report_sources(repo, resolve_pin_repos(pin_repos), runner=runner)
    # A receipt persisted before the split records the monorepo path
    # ("vibe-queue/releases/..."), which never existed in vibe-queue's own
    # fresh history -- the identical blob lives at "releases/..." there. Search
    # every legitimate path for this tag, not just the one the receipt names,
    # and authenticate on the blob digest as before: the content, not its
    # location, is what the receipt attests to.
    candidate_paths = report_paths_for(tag)
    commits: list[tuple[str, str]] = []
    for candidate in candidate_paths:
        for commit in _git(
            repo,
            "log",
            "--format=%H",
            ref,
            "--",
            candidate,
            runner=runner,
        ).splitlines():
            commits.append((commit, candidate))
    for commit, found_path in commits:
        try:
            text = _git(
                repo,
                "show",
                f"{commit}:{found_path}",
                runner=runner,
                strip=False,
            )
        except FleetReleaseError:
            # A deletion commit is part of the path history but has no blob.
            continue
        if hashlib.sha256(text.encode()).hexdigest() != digest_sha256:
            continue
        try:
            return _load_report_from_git(
                repo,
                ref=commit,
                path=found_path,
                runner=runner,
                pin_repos=pin_repos,
            )
        except FleetReleaseError as strict_error:
            before_gate = git_is_ancestor(
                repo,
                commit,
                f"{LEGACY_MAIN_EVIDENCE_CUTOFF}^",
                runner=runner,
            )
            if before_gate is not True:
                raise FleetReleaseError(
                    f"{source_path}: exact historical blob fails current "
                    f"validation and is not proven to predate the evidence "
                    f"hardening: {strict_error}"
                ) from strict_error
            try:
                return _load_report_from_git(
                    repo,
                    ref=commit,
                    path=found_path,
                    runner=runner,
                    pin_repos=pin_repos,
                    allow_legacy_main_evidence=True,
                )
            except FleetReleaseError as legacy_error:
                raise FleetReleaseError(
                    f"{source_path}: exact pre-hardening historical blob is "
                    f"not valid under its legacy acceptance policy: "
                    f"{legacy_error}"
                ) from legacy_error
    raise FleetReleaseError(
        f"{source_path}: no blob with SHA-256 {digest_sha256} exists in "
        f"the {ref} path history under any of {candidate_paths!r}"
    )


def report_summary(report: FleetReleaseReport) -> dict[str, Any]:
    """JSON-safe compact identity used by plans and rollout state."""
    return {
        "source_ref": report.source_ref,
        "source_path": report.source_path,
        "digest_sha256": report.digest_sha256,
        "generated_at": report.generated_at,
        "release_tag": report.release.tag,
        "pins": {
            name: {
                "sha": pin.sha,
                "version": pin.version,
                "tag": pin.tag,
                "gating_job": pin.gating_job,
                "pipeline_id": pin.pipeline_id,
                "evidence_sha": pin.evidence_sha,
                "acceptance_rule": pin.acceptance_rule,
            }
            for name, pin in report.pins.items()
        },
    }


def semver_from_text(value: str | None) -> tuple[int, int, int] | None:
    """Parse ``X.Y.Z`` or ``vX.Y.Z`` for no-downgrade comparisons."""
    if value is None:
        return None
    match = re.fullmatch(r"v?([0-9]+)\.([0-9]+)\.([0-9]+)", value.strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def git_is_pin_ancestor(
    report: FleetReleaseReport,
    pin_name: str,
    runtime_repo_path: Path,
    older: str,
    newer: str,
    *,
    pin_repos: Mapping[str, Path | str] | None = None,
) -> bool | None:
    """Compare both directions in the authenticated pin's repository.

    Component identity comes from the report, never from either endpoint's
    SHA. Release and dev may share a pin, and a live descendant need not be a
    report pin at all. Reuse discovery's /3 mapping and /2 monorepo rules.
    """
    try:
        pin = report.pins[pin_name]
    except KeyError as exc:
        raise FleetReleaseError(
            f"{report.source_path}: unknown ancestry pin {pin_name!r}"
        ) from exc
    repo = _repo_for_pin(
        pin,
        schema=report.raw.get("schema"),
        runtime_repo_path=runtime_repo_path,
        pin_repos=resolve_pin_repos(pin_repos),
        source_path=report.source_path,
    )
    return git_is_ancestor(repo, older, newer)


def git_is_ancestor(
    repo: Path,
    older: str,
    newer: str,
    *,
    runner: Runner = subprocess.run,
) -> bool | None:
    """Return ancestry, or ``None`` when either commit cannot be resolved."""
    proc = runner(
        ["git", "-C", str(repo), "merge-base", "--is-ancestor", older, newer],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode == 0:
        return True
    if proc.returncode == 1:
        return False
    return None


def ordered_hosts(configured: Sequence[str], preferred: Sequence[str]) -> list[str]:
    """Apply the configured fleet order and append omitted hosts stably."""
    present = set(configured)
    return [name for name in preferred if name in present] + sorted(
        present - set(preferred)
    )
