#!/usr/bin/env python3
"""Emit the machine-readable release report a fleet rollout pins against.

The updater chat must never infer a deployment pin from a moving branch name.
This script resolves every pin to a 40-hex SHA, attaches the CI evidence that
makes it deployable, and **exits non-zero if any pin fails the acceptance
rule** — so a report that exists at all is a report whose pins are deployable.

CI policy (maintainer decision, 2026-07-26): CI runs exactly once per
release, on the exact release tree. Ordinary ``main`` pushes create no
pipeline, so there is no per-component "newest green main ancestor" to walk
any more. Every pin — release, dev, vq, vibe_view — is the release commit
itself, and every gate (``build-test``, ``test-vq``, ``vibe-view-test``) is
proven by the single ``release-candidate/*`` pipeline that ran on that exact
tree. The tag creates no duplicate pipeline.

``vibe-view-test`` includes both the source suite and the reproducible
artifact/clean-installed-wheel gate; keeping one evidence job prevents a
package failure from being ignored after a source-suite success.

That restriction is **enforced**, not assumed: :func:`gating_evidence`
discards any pipeline whose ref is not a release-gate ref. Pre-policy
``main`` pipelines are still returned by the API for old commits, and
trusting one is how a release whose release-gate pipeline *failed* can be
reported as accepted (see that function's docstring for the v0.15.62 case).

Usage:
    python3 vibe-queue/scripts/make_release_report.py --tag v0.15.54
    python3 vibe-queue/scripts/make_release_report.py --tag v0.15.54 -o report.json

Acceptance rules for the release pin (see docs/fleet_update_runbook.md
§ "Pinning a release: what counts as green"):

    A        exact-SHA pipeline with build-test success
    B        tree-identical to a build-test-green commit
    B-prime  differs from a build-test-green ancestor only in CHANGELOG.md,
             docs/**, and the pyproject.toml `version = ` line
    C        no evidence — push refs/heads/release-candidate/<tag> and wait

Component pins (dev, vq, vibe_view) accept rule A only: their gate must have
succeeded at the exact release SHA.

Rule C is reported as a FAILURE here on purpose: it is an instruction to go
get evidence, not a pin you may deploy.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

# Each pin resolves in its own GitLab project since the 2026-09-08 split.
# One id no longer covers all four: after the split each component's gating
# job runs in its own repository's pipeline, so the evidence lookup for a pin
# has to be aimed at that pin's project. Path-based lookups 404 on this
# instance (CLAUDE.md § 2), so the numeric id is what callers pass.
PIN_SOURCES = {
    "release": ("mpei/vibe-qc", 34),
    "dev": ("mpei/vibe-qc", 34),
    "vq": ("mpei/vibe-queue", 36),
    "vibe_view": ("mpei/vibe-view", 35),
}

# The frozen pre-split monorepo. Reports generated before the split named it
# once at top level; kept here only so historical reports remain readable.
MONOREPO_PROJECT_ID = 19
# Mandatory gate per pin, named as it is in that pin's OWN project. The
# monorepo ran every component's gate in one pipeline, so the jobs needed
# distinct names (`test-vq`, `vibe-view-test`); after the split each
# component's suite is simply `test` in its own repository, and those old
# names exist nowhere. Mirrors vq.fleet_release.PIN_GATING_JOBS.
GATING_JOBS = {
    "release": "build-test",
    "dev": "build-test",
    "vq": "test",
    "vibe_view": "test",
}

# New releases obtain the ONE CI run on ``release-candidate/*`` before the tag
# exists. Tag refs remain accepted here for historical reports created under
# the previous policy. Under the release-only policy an ordinary ``main``
# push creates no pipeline, and a ``release`` branch pipeline runs docs jobs
# only.
_RELEASE_TAG_REF = re.compile(r"^v\d+\.\d+\.\d+$")
_RELEASE_CANDIDATE_PREFIX = "release-candidate/"

# Paths whose contents cannot affect a build. Used only by rule B-prime.
NON_BUILD_EXACT = {"CHANGELOG.md", "pyproject.toml"}
NON_BUILD_PREFIXES = ("docs/",)

_VERSION_LINE = re.compile(r"^[+-]version\s*=", re.MULTILINE)
_DIFF_META = re.compile(r"^(diff |index |--- |\+\+\+ |@@ )")


def git(*args: str) -> str:
    """Run git in the anchor repository (the vibe-qc checkout, cwd)."""
    return git_in(None, *args)


def git_in(repo: str | None, *args: str) -> str:
    """Run git in ``repo``, or in cwd when ``repo`` is None.

    Sibling pins are resolved in their own checkouts after the split, so the
    anchor repository is no longer the only one this script reads.
    """
    argv = ["git"] + (["-C", repo] if repo else []) + list(args)
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        where = f" in {repo}" if repo else ""
        raise SystemExit(
            f"git {' '.join(args)}{where} failed: {proc.stderr.strip()}"
        )
    return proc.stdout.strip()


def newest_release_tag(repo: str) -> str:
    """The newest vX.Y.Z tag in ``repo``, by version order.

    Sibling repositories carry no `release` branch -- vibe-queue has only
    `main` plus its tags -- so the tag itself is the release identity.
    """
    tags = [
        line
        for line in git_in(repo, "tag", "--list", "v*").splitlines()
        if _RELEASE_TAG_REF.fullmatch(line.strip())
    ]
    if not tags:
        raise SystemExit(f"no vX.Y.Z release tag found in {repo}")
    def key(tag: str) -> tuple[int, int, int]:
        major, minor, patch = tag.lstrip("v").split(".")
        return int(major), int(minor), int(patch)
    return max(tags, key=key)


def sibling_pin(
    name: str,
    repo: str,
    *,
    tag: str | None = None,
) -> dict[str, Any]:
    """Resolve one sibling component's pin inside its own repository.

    The release is *anchored* on vibe-qc's tag, but a sibling's SHA cannot
    come from that tag -- it is a vibe-qc commit that does not exist here.
    Each sibling therefore resolves to its own current release tag, and is
    evidenced by its own project's pipeline.
    """
    repo_slug, project_id = PIN_SOURCES[name]
    tag = tag or newest_release_tag(repo)
    sha = git_in(repo, "rev-parse", f"{tag}^{{commit}}")
    version = version_at(sha, "pyproject.toml", repo=repo)
    evidence = gating_evidence(
        sha, gating_job=GATING_JOBS[name], project_id=project_id
    )
    pin: dict[str, Any] = {
        "repo": repo_slug,
        "project_id": project_id,
        "ref_resolved_from": tag,
        "sha": sha,
        "version": version,
        "accepted": evidence is not None,
        "acceptance_rule": "A" if evidence is not None else "C",
        "ci_evidence": evidence,
        "deploy_flags": ["--expected-sha", sha],
    }
    if evidence is None:
        pin["remediation"] = (
            f"no {GATING_JOBS[name]} success at {repo_slug} {tag} ({sha}). "
            f"That gate runs in project {project_id}, not in vibe-qc: check "
            f"that repository's release-gate pipeline, then re-run this script."
        )
    return pin


def glab_pipelines(*, project_id: int, **params: str) -> list[dict[str, Any]]:
    """Query one project's pipelines API. Path-based lookups 404 on this
    instance, so the numeric project id is mandatory (CLAUDE.md § 2)."""
    query = "&".join(f"{k}={v}" for k, v in params.items())
    proc = subprocess.run(
        ["glab", "api", f"projects/{project_id}/pipelines?{query}"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise SystemExit(f"glab api failed: {proc.stderr.strip()}")
    # Commit messages carry control characters; strict JSON rejects them.
    return json.loads(proc.stdout, strict=False)


def glab_jobs(pipeline_id: int, *, project_id: int) -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["glab", "api", f"projects/{project_id}/pipelines/{pipeline_id}/jobs"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if proc.returncode != 0:
        raise SystemExit(f"glab api failed: {proc.stderr.strip()}")
    return json.loads(proc.stdout, strict=False)


def _pipeline_job_evidence(
    pipeline: dict[str, Any],
    *,
    gating_job: str,
    project_id: int,
) -> dict[str, Any] | None:
    for job in glab_jobs(pipeline["id"], project_id=project_id):
        if job["name"] == gating_job and job["status"] == "success":
            return {
                "pipeline_id": pipeline["id"],
                "pipeline_status": pipeline["status"],
                "ref": pipeline["ref"],
                "sha": pipeline["sha"],
                "gating_job": gating_job,
                "gating_job_status": "success",
                "web_url": pipeline["web_url"],
            }
    return None


def is_release_gate_ref(ref: object) -> bool:
    """True iff a pipeline's ref makes it a release-gate pipeline."""
    if not isinstance(ref, str):
        return False
    return bool(_RELEASE_TAG_REF.match(ref)) or ref.startswith(
        _RELEASE_CANDIDATE_PREFIX
    )


def gating_evidence(
    sha: str,
    *,
    gating_job: str = "build-test",
    project_id: int = PIN_SOURCES["release"][1],
) -> dict[str, Any] | None:
    """Return successful ``gating_job`` evidence for an exact SHA, or None.

    Two independent filters apply, and both are load-bearing:

    * **Ref.** Only a release-gate pipeline counts. The pipelines API returns
      every pipeline at a SHA, and pre-policy ``main`` pipelines still exist
      in that history; accepting one would resurrect the retired "newest
      green main" reasoning. Worse, it can contradict a release-gate failure
      at the very same SHA — v0.15.62 had ``main`` pipeline 4630 green while
      release-gate pipeline 4631 failed ``build-test`` on the identical tree.
    * **Job.** A pipeline being green is not evidence: pipelines on the
      `release` branch run docs jobs only. Only the component's mandatory job
      with status ``success`` counts. Taking a green component gate from a
      release-gate pipeline that failed a *different* component's gate is
      correct and intended — each component is gated by its own job.
    """
    for pipeline in glab_pipelines(sha=sha, per_page="20", project_id=project_id):
        if not is_release_gate_ref(pipeline.get("ref")):
            continue
        evidence = _pipeline_job_evidence(
            pipeline, gating_job=gating_job, project_id=project_id
        )
        if evidence is not None:
            return evidence
    return None


def is_ancestor(a: str, b: str) -> bool:
    return (
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", a, b],
            capture_output=True,
        ).returncode
        == 0
    )


def non_build_only(base: str, head: str) -> bool:
    """True iff base..head touches only non-build paths, and pyproject.toml
    changes are confined to the `version = ` line."""
    files = [f for f in git("diff", "--name-only", base, head).splitlines() if f]
    if not files:
        return False
    for path in files:
        if path in NON_BUILD_EXACT:
            continue
        if path.startswith(NON_BUILD_PREFIXES):
            continue
        return False
    if "pyproject.toml" in files:
        diff = git("diff", "--unified=0", base, head, "--", "pyproject.toml")
        for line in diff.splitlines():
            if not line.startswith(("+", "-")) or _DIFF_META.match(line):
                continue
            if not _VERSION_LINE.match(line):
                return False
    return True


def classify_release_pin(peeled: str) -> dict[str, Any]:
    """Apply rules A / B / B-prime / C to a release tag's peeled SHA."""
    exact = gating_evidence(peeled, gating_job=GATING_JOBS["release"])
    if exact is not None:
        return {"rule": "A", "accepted": True, "ci_evidence": exact}

    # Search recent history both ways for a build-test-green relative.
    candidates = (
        git("rev-list", "--max-count=40", f"{peeled}~40..origin/main").splitlines()
        + git("rev-list", "--max-count=40", peeled).splitlines()
    )

    peeled_tree = git("rev-parse", f"{peeled}^{{tree}}")
    for cand in dict.fromkeys(c for c in candidates if c and c != peeled):
        if not (is_ancestor(cand, peeled) or is_ancestor(peeled, cand)):
            continue
        evidence = gating_evidence(cand, gating_job=GATING_JOBS["release"])
        if evidence is None:
            continue
        if git("rev-parse", f"{cand}^{{tree}}") == peeled_tree:
            return {
                "rule": "B",
                "accepted": True,
                "ci_evidence": evidence,
                "note": (
                    f"tree-identical to {cand}: both are {peeled_tree}; "
                    "CI tests a tree, so the evidence transfers exactly"
                ),
            }
        base, head = (cand, peeled) if is_ancestor(cand, peeled) else (peeled, cand)
        if non_build_only(base, head):
            return {
                "rule": "B-prime",
                "accepted": True,
                "ci_evidence": evidence,
                "note": (
                    f"differs from build-test-green {cand} only in "
                    "CHANGELOG.md / docs / the pyproject version line"
                ),
            }

    return {
        "rule": "C",
        "accepted": False,
        "ci_evidence": None,
        "remediation": (
            f"no build-test evidence for {peeled}. Get exact-SHA CI: "
            f"git push origin {peeled}:refs/heads/release-candidate/<tag>, "
            "wait for build-test green, then re-run this script."
        ),
    }


def version_at(commit: str, path: str, *, repo: str | None = None) -> str:
    """Read a PEP 621 ``version =`` value from ``path`` at ``commit``."""
    for line in git_in(repo, "show", f"{commit}:{path}").splitlines():
        if line.startswith("version"):
            return line.split("=", 1)[1].strip().strip('"')
    raise SystemExit(f"{path} at {commit} has no version field")


def _tracked_at_head(path: str) -> bool:
    """True iff ``path`` is tracked at HEAD. Own function so it is patchable."""
    return (
        subprocess.run(
            ["git", "cat-file", "-e", f"HEAD:{path}"],
            capture_output=True,
        ).returncode
        == 0
    )


def monorepo_layout() -> bool:
    """True iff this checkout still carries the sibling components.

    In the monorepo a pin's version was read as ``<release-sha>:<path>``
    for all four components. After the 2026-09-08 split ``vibe-queue/`` and
    ``vibe-view/`` are separate repositories, and -- more than a missing
    path -- the release SHA is a vibe-qc commit that does not exist in
    them, so there is no SHA there to read.

    A post-split release is therefore **anchored** on vibe-qc's tag while
    each sibling resolves to its own current release tag in its own
    repository, evidenced by its own project's pipeline. See
    :func:`sibling_pin`.
    """
    return all(
        _tracked_at_head(path)
        for path in ("vibe-queue/pyproject.toml", "vibe-view/pyproject.toml")
    )


def _output_path(value: str, *, source_repos: list[Path]) -> Path:
    """Keep operational report bytes outside all selected product trees."""
    path = Path(value).absolute()
    resolved = path.resolve()
    for supplied in (path, resolved):
        if any(part.casefold() == ".git" for part in supplied.parts):
            raise ValueError("release report output must be outside Git metadata")
        for source in source_repos:
            if supplied.is_relative_to(source.resolve()):
                raise ValueError("release report output must be outside product source trees")
        for parent in (supplied, *supplied.parents):
            if ((parent / "HEAD").is_file() and (parent / "objects").is_dir()
                    and (parent / "refs").is_dir()):
                raise ValueError("release report output must be outside Git object stores")
    if path.is_symlink():
        raise ValueError("release report output must not be a symlink")
    return resolved


def _write_private_report(path: Path, text: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("release report output must be an owned regular file with one link")
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as fh:
            fh.write(text + "\n")
            fh.flush()
            os.fsync(fd)
    finally:
        os.close(fd)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tag", required=True, help="release tag, e.g. v0.15.54")
    ap.add_argument("-o", "--output", help="write JSON here instead of stdout")
    ap.add_argument(
        "--generated-at",
        help="ISO timestamp to stamp the report with (default: unstamped)",
    )
    ap.add_argument(
        "--vq-repo",
        help="path to the mpei/vibe-queue checkout the vq pin resolves in",
    )
    ap.add_argument(
        "--vibe-view-repo",
        help="path to the mpei/vibe-view checkout the vibe_view pin resolves in",
    )
    ap.add_argument(
        "--vq-tag",
        help="pin vq at this tag instead of that repository's newest",
    )
    ap.add_argument(
        "--vibe-view-tag",
        help="pin vibe_view at this tag instead of that repository's newest",
    )
    args = ap.parse_args()

    split_layout = not monorepo_layout()
    if split_layout and not (args.vq_repo and args.vibe_view_repo):
        raise SystemExit(
            "this is a split (post-2026-09-08) vibe-qc checkout, so the vq and "
            "vibe_view pins cannot be read from it: their components left this "
            "repository, and the release SHA is a vibe-qc commit that does not "
            "exist in theirs.\n"
            "Pass --vq-repo and --vibe-view-repo pointing at those checkouts. "
            "Each sibling is pinned at its own newest release tag (override "
            "with --vq-tag / --vibe-view-tag) and evidenced by its own "
            "project's pipeline."
        )
    peeled = git("rev-parse", f"{args.tag}^{{commit}}")
    release_pin = classify_release_pin(peeled)

    # Release-only CI: every component pin IS the release commit, and its
    # gate must have succeeded at that exact SHA (rule A) in the single
    # release-gate pipeline. Ordinary main pipelines no longer exist and
    # are never acceptable evidence.
    component_paths = {
        "dev": "pyproject.toml",
        "vq": "vibe-queue/pyproject.toml",
        "vibe_view": "vibe-view/pyproject.toml",
    }
    component_pins: dict[str, dict[str, Any]] = {}
    if split_layout:
        # `dev` is still the anchor tree; the siblings are their own releases.
        component_paths = {"dev": "pyproject.toml"}
        component_pins["vq"] = sibling_pin("vq", args.vq_repo, tag=args.vq_tag)
        component_pins["vibe_view"] = sibling_pin(
            "vibe_view", args.vibe_view_repo, tag=args.vibe_view_tag
        )
    for name, path in component_paths.items():
        evidence = gating_evidence(
            peeled,
            gating_job=GATING_JOBS[name],
            project_id=PIN_SOURCES[name][1],
        )
        if evidence is None:
            component_pins[name] = {
                "ref_resolved_from": args.tag,
                "sha": peeled,
                "version": version_at(peeled, path),
                "accepted": False,
                "acceptance_rule": "C",
                "ci_evidence": None,
                "remediation": (
                    f"no {GATING_JOBS[name]} success at the exact release "
                    f"tree {peeled}. The release-gate pipeline "
                    f"refs/heads/release-candidate/{args.tag} "
                    "must run every component gate once; wait for it or "
                    "re-trigger it, then re-run this script."
                ),
                "deploy_flags": ["--expected-sha", peeled],
            }
            continue
        component_pins[name] = {
            "ref_resolved_from": args.tag,
            "sha": peeled,
            "version": version_at(peeled, path),
            "accepted": True,
            "acceptance_rule": "A",
            "ci_evidence": evidence,
            "deploy_flags": ["--expected-sha", peeled],
        }

    report: dict[str, Any] = {
        "schema": "vq.fleet.release_report/3",
        "generated_at": args.generated_at,
        "gating_jobs": GATING_JOBS,
        "pins": {
            "release": {
                "tag": args.tag,
                "sha": peeled,
                "version": version_at(peeled, "pyproject.toml"),
                "tag_object": git("rev-parse", args.tag),
                "accepted": release_pin["accepted"],
                "acceptance_rule": release_pin["rule"],
                "ci_evidence": release_pin["ci_evidence"],
                **{k: v for k, v in release_pin.items() if k in ("note", "remediation")},
                "deploy_flags": [
                    "--tag",
                    args.tag,
                    "--expected-sha",
                    peeled,
                ],
            },
            **component_pins,
        },
        # vibe-basis stayed inside vibe-qc, so it is still read at the
        # anchor SHA; the other two are read at their own pinned SHAs.
        "sibling_versions_at_release": {
            "vq": (
                component_pins["vq"]["version"]
                if split_layout
                else version_at(peeled, "vibe-queue/pyproject.toml")
            ),
            "vibe_view": (
                component_pins["vibe_view"]["version"]
                if split_layout
                else version_at(peeled, "vibe-view/pyproject.toml")
            ),
            "vibe_basis": version_at(peeled, "vibe-basis/pyproject.toml"),
        },
    }
    for name, pin in report["pins"].items():
        pin["repo"], pin["project_id"] = PIN_SOURCES[name]

    report["all_pins_accepted"] = all(p["accepted"] for p in report["pins"].values())

    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        sources = [Path.cwd(), Path(__file__).resolve().parents[1]]
        sources.extend(Path(value) for value in (args.vq_repo, args.vibe_view_repo) if value)
        try:
            output = _output_path(args.output, source_repos=sources)
        except ValueError as exc:
            ap.error(str(exc))
        _write_private_report(output, text)
    else:
        print(text)

    if not report["all_pins_accepted"]:
        rejected = [n for n, p in report["pins"].items() if not p["accepted"]]
        print(
            f"REJECTED: no acceptable CI evidence for pin(s): {', '.join(rejected)}. "
            "Do not deploy; see the remediation field.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
