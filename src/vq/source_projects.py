"""Deployment repository identities loaded from an external operator file.

The report parser and generator share this trust source. A report cannot
choose its own trusted project, and product source carries no site identities.
"""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Iterator, Mapping
from pathlib import Path

PIN_NAMES = ("release", "dev", "vq", "vibe_view")


def settings_path() -> Path:
    explicit = os.environ.get("VQ_SOURCE_PROJECTS_FILE")
    if explicit is not None:
        path = Path(explicit)
        if not explicit or not path.is_absolute():
            raise ValueError("VQ_SOURCE_PROJECTS_FILE must be an absolute external path")
        return path
    root = Path(
        os.environ.get("VIBE_PRIVATE_ROOT")
        or str(
            Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
            / "vibe-private"
        )
    )
    if not root.is_absolute():
        raise ValueError("private state root must be absolute")
    return root / "vibe-queue/config/source-projects.json"


def _load() -> dict:
    path = settings_path()
    try:
        resolved = path.resolve(strict=True)
        for candidate in (path, resolved):
            for parent in (candidate.parent, *candidate.parents):
                if (
                    (parent / ".git").exists()
                    or parent.name == ".git"
                    or ((parent / "HEAD").is_file() and (parent / "objects").is_dir())
                ):
                    raise ValueError(
                        "source project configuration must stay outside Git repositories"
                    )
        info = resolved.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("source project configuration must be an owner-only regular file")
        data = json.loads(resolved.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(
            "configure an external source-projects.json before release operations"
        ) from error
    if not isinstance(data, dict) or set(data) != {"pins", "legacy"}:
        raise ValueError("source project configuration needs pins and legacy entries")
    if not isinstance(data["pins"], dict) or set(data["pins"]) != set(PIN_NAMES):
        raise ValueError("source project configuration must name all four pins")
    for entry in (*data["pins"].values(), data["legacy"]):
        _identity(entry)
    if data["pins"]["release"] != data["pins"]["dev"]:
        raise ValueError("release and dev pins must name the same project")
    return data


def _identity(entry: object) -> tuple[str, int]:
    if not isinstance(entry, dict) or set(entry) != {"repo", "project_id"}:
        raise ValueError("each source identity needs repo and project_id")
    repo, project_id = entry["repo"], entry["project_id"]
    if (
        not isinstance(repo, str)
        or not repo.strip()
        or repo != repo.strip()
        or any(char.isspace() for char in repo)
        or not isinstance(project_id, int)
        or isinstance(project_id, bool)
        or project_id <= 0
    ):
        raise ValueError("source identity needs a repository slug and positive project ID")
    return repo, project_id


class PinSources(Mapping[str, tuple[str, int]]):
    """Resolve settings when used, so unrelated queue commands need no file."""

    def __getitem__(self, key: str) -> tuple[str, int]:
        return _identity(_load()["pins"][key])

    def __iter__(self) -> Iterator[str]:
        return iter(PIN_NAMES)

    def __len__(self) -> int:
        return len(PIN_NAMES)


PIN_SOURCES = PinSources()


def legacy_source() -> tuple[str, int]:
    return _identity(_load()["legacy"])
