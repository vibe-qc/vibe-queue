"""Sphinx configuration for the vq documentation.

Build locally from the repository root:

    sphinx-build -b html docs docs/_build/html

Naming
------
* The product is **vq**, the command users type. The repository and the
  GitLab project are **vibe-queue**; the distribution on PyPI-style
  metadata is ``vq``. Use "vq" in prose and "vibe-queue" only when the
  repository itself is meant.

Audience
--------
This corpus grew as one undifferentiated pile serving three readers at
once: someone submitting jobs, someone running a fleet host, and an agent
following a machine-readable protocol. The toctrees in ``index.md`` split
them. When you add a page, put it under the audience that would go looking
for it, not under the subsystem it happens to describe.
"""

from __future__ import annotations

import datetime
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))


# --- Project metadata ------------------------------------------------------

project = "vq"
author = "Michael F. Peintinger"
copyright = f"{datetime.date.today().year}, {author}"


def _read_release() -> str:
    """Version from the installed distribution, else from pyproject.toml.

    The CI docs build runs in a slim container with only Sphinx installed
    and no vq, so the pyproject fallback is what actually runs in
    production.
    """
    try:
        from importlib.metadata import PackageNotFoundError
        from importlib.metadata import version as _pkg_version

        try:
            return _pkg_version("vq")
        except PackageNotFoundError:
            pass
    except ImportError:
        pass
    try:
        import tomllib

        with open(_REPO_ROOT / "pyproject.toml", "rb") as f:
            return str(tomllib.load(f)["project"]["version"])
    except Exception:
        return "0.0.0"


release = _read_release()
version = ".".join(release.split(".")[:2])


def _read_codename(rel: str) -> str:
    """Codename for this build, from the catalog that ``vq --version`` reads.

    Imported from source rather than duplicated, so the site and the CLI can
    never disagree. Returns an empty string when the version has no entry, so
    a pre-convention or unreleased build renders without a dangling quote.
    """
    try:
        from vq.codename import codename_for_version

        return codename_for_version(rel) or ""
    except Exception:
        return ""


codename = _read_codename(release)


# --- Extensions ------------------------------------------------------------

extensions = [
    "myst_parser",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
    "sphinx_design",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "linkify",
    "substitution",
    "tasklist",
]
myst_heading_anchors = 3
myst_substitutions = {
    "release": release,
    "codename": codename,
    "version_display": f'{release} "{codename}"' if codename else release,
}
# The corpus is full of prose that looks like a bare URL or a mail address
# without being one. Keep linkify to explicit schemes.
myst_linkify_fuzzy_links = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}


# --- What does NOT get published -------------------------------------------
#
# docs/ is also this project's working memory: status files, handovers,
# per-cycle audits, design notes written to be superseded, and a 444 KB
# roadmap. Those are legitimate artefacts and they stay in the repository,
# but they are not product documentation and several of them would be the
# largest pages on the site.
#
# Two of these are excluded for a second reason: the fleet runbooks describe
# named real hosts. `docs/hosts.md` is sanitized and publishable; the
# runbooks are not, and are not worth sanitizing for an audience that does
# not exist.
exclude_patterns = [
    "_build",
    "README.md",
    # Working memory, not documentation.
    "STATUS.md",
    "roadmap.md",
    "handover.md",
    "handover-*.md",
    "audits/**",
    # Design notes: superseded by the code they proposed.
    "*_design.md",
    "design_*.md",
    "pbs_dispatcher_backend_design.md",
    "v0_7_1_lamports_clock_design.md",
    # Fleet-internal runbooks: name real hosts, no external audience.
    "fleet_update_runbook.md",
    "fleet_update_prompt.md",
    "state_file_audit.md",
    # Written for a different site. `vibe-qc-site/` holds the vq pages that
    # the vibe-qc documentation chat lands in mpei/vibe-qc, under /docs/ on
    # vibe-qc.com. They are authored here because vq is what changes
    # underneath them, but publishing them here too would put the same prose
    # at two URLs on one domain, which is the drift these pages exist to
    # avoid. See vibe-qc-site/README.md.
    "vibe-qc-site/**",
]

# Cross-references into excluded pages are expected: the published pages
# legitimately point at working-memory documents that live in the
# repository. Do not "fix" these by publishing the target.
suppress_warnings = ["myst.xref_missing"]


# --- HTML output -----------------------------------------------------------

html_theme = "furo"
html_title = f"vq {release}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

# --- Logo / favicon --------------------------------------------------------
#
# Assets are under ``docs/_static/logo/``, hand-written SVG rather than
# generated raster: the wordmark's lettering is drawn as geometric primitives,
# so it needs no font at render time and stays crisp at any size. The glyph is
# a queue -- three tokens advancing along a rail -- inside the same rounded
# unit-cell frame vibe-qc uses, so the two products read as one family.
#
# The glyph is duplicated into three files (favicon, both wordmarks) and into
# the social card. ``tests/test_logo_assets.py`` asserts the copies have not
# drifted; edit one and it will tell you about the others.
#
# Furo resolves light_logo / dark_logo from html_theme_options below; only the
# favicon is set explicitly here.
html_favicon = "_static/logo/vq-favicon.svg"

# vq owns exactly one subtree of vibe-qc.com. The docs-deploy job rsyncs
# into /web/vibe-queue/docs/ and nothing above it; keep the two in lockstep.
html_baseurl = "https://vibe-qc.com/vibe-queue/docs/"

# Furo's `source_repository` only understands the hosted forges it ships
# patterns for; a self-hosted GitLab makes it emit two warnings per page and
# render no link. Give it the URLs directly instead.
_SOURCE = "https://github.com/vibe-qc/vibe-queue"
html_theme_options = {
    # Furo picks a light or dark wordmark from the reader's theme.
    "light_logo": "logo/vq-wordmark-light.svg",
    "dark_logo": "logo/vq-wordmark-dark.svg",
    "source_view_link": f"{_SOURCE}/-/blob/main/docs/{{filename}}",
    "source_edit_link": f"{_SOURCE}/-/edit/main/docs/{{filename}}",
    "navigation_with_keys": True,
}

html_show_sourcelink = True
