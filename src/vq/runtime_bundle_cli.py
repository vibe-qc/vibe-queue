"""Opt-in local portable-artifact commands, independent of fleet updates."""

from __future__ import annotations

import json
import tarfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

_PATH = click.Path(path_type=Path)
_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)
_DIRECTORY = click.Path(exists=True, file_okay=False, path_type=Path)


def _run(action: Callable[[], dict[str, Any]]) -> None:
    from vq.admin import AdminError
    from vq.runtime_slots import RuntimeSlotError

    try:
        result = action()
    except (OSError, ValueError, tarfile.TarError, RuntimeSlotError, AdminError) as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@click.group("runtime-bundle")
def runtime_bundle() -> None:
    """Package and distribute already-built runtimes through local file stores.

    Uses dedicated artifact/target roots, independent of legacy runtime slots.
    Trust pins must come from an operator-controlled source. No fleet config,
    scheduler registration, compilation or existing job pins are changed.
    """


@runtime_bundle.command("pack")
@click.argument("directory", type=_DIRECTORY)
@click.option("--description", required=True, type=_FILE)
@click.option("--output", required=True, type=_PATH, help="New absolute bundle directory.")
def pack(directory: Path, description: Path, output: Path) -> None:
    """Package a product builder's qualified files and provenance."""
    from vq.runtime_bundle import pack as pack_bundle

    def action() -> dict[str, Any]:
        manifest = pack_bundle(directory, description, output)
        return {"artifact_id": manifest.artifact_id, "build_id": manifest.build.build_id,
                "archive_sha256": manifest.archive_sha256,
                "source_revision": manifest.build.source_revision,
                "authenticated": False}

    _run(action)


@runtime_bundle.command("publish")
@click.argument("bundle", type=_DIRECTORY)
@click.option("--store", required=True, type=_PATH, help="Absolute artifact store root.")
@click.option("--trust", required=True, type=_FILE, help="Operator-controlled manifest pins.")
def publish(bundle: Path, store: Path, trust: Path) -> None:
    """Authenticate and publish a content-addressed bundle; never overwrite."""
    from vq.runtime_bundle import publish as publish_bundle

    def action() -> dict[str, Any]:
        manifest = publish_bundle(bundle, store, trust)
        return {"artifact_id": manifest.artifact_id, "build_id": manifest.build.build_id,
                "published": True}

    _run(action)


@runtime_bundle.command("deploy")
@click.argument("artifact_id")
@click.option("--store", required=True, type=_DIRECTORY)
@click.option("--root", required=True, type=_PATH, help="Independent absolute target root.")
@click.option("--trust", required=True, type=_FILE)
@click.option("--target-profile", required=True, type=_FILE,
              help="Qualified compute-node profile JSON; not login-node autodetection.")
@click.option("--apply/--plan", default=False, help="Default: read-only compatibility plan.")
@click.option("--smoke-timeout", default=30.0, type=click.FloatRange(min=0, max=120, min_open=True))
def deploy(
    artifact_id: str, store: Path, root: Path, trust: Path,
    target_profile: Path, apply: bool, smoke_timeout: float,
) -> None:
    """Relocate and select an artifact without a target build.

    Apply runs the trusted bundle's smoke executable as the invoking operator.
    Repeating an apply reconciles the same immutable generation. To roll back,
    deploy the previous artifact ID; retained paths and job references survive.
    """
    from vq.runtime_bundle import TargetProfile, _read_json, deploy, deploy_plan

    def action() -> dict[str, Any]:
        profile = TargetProfile.model_validate_json(_read_json(target_profile))
        if apply:
            return deploy(store, artifact_id, root, trust, profile, timeout=smoke_timeout)
        return deploy_plan(store, artifact_id, root, trust, profile)

    _run(action)
