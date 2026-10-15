"""Opt-in portable-artifact commands, independent of fleet updates."""

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
    """Package and distribute already-built runtimes through file and HTTPS stores.

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


@runtime_bundle.command("fetch")
@click.argument("artifact_id")
@click.option("--url", "base_url", default=None, help="HTTPS artifact store root URL.")
@click.option("--from-host", "from_host", default=None, metavar="HOST",
              help="Fetch over SSH from this configured host instead of HTTPS; "
              "needs --remote-store.")
@click.option("--remote-store", default=None, metavar="PATH",
              help="With --from-host: the absolute artifact store root on that host.")
@click.option("--store", required=True, type=_PATH, help="Absolute local artifact store root.")
@click.option("--trust", required=True, type=_FILE, help="Operator-controlled manifest pins.")
@click.option("--ca-file", type=_FILE, help="Optional trusted CA PEM for a private HTTPS store.")
@click.option("--client-cert", type=_PATH,
              help="PEM client certificate/chain for HTTPS; may include its private key.")
@click.option("--client-key", type=_PATH,
              help="Separate unencrypted PEM private key for --client-cert (HTTPS only).")
# Defer credential checks so unreadable paths stay out of Click's diagnostics.
@click.option("--bearer-token-file", type=click.Path(path_type=Path, readable=False),
              help="Owner-only file containing the HTTPS store's bearer token; "
              "read only when a download is needed.")
@click.option("--timeout", default=30.0, show_default=True,
              type=click.FloatRange(min=0, max=120, min_open=True),
              help="HTTPS: connection/read socket timeout. SSH: the longest a transfer may "
              "go without data. Seconds; not a total transfer deadline.")
@click.option("--max-download-mib", default=4096, show_default=True,
              type=click.IntRange(min=1, max=65568), help="Maximum archive download size in MiB.")
def fetch(artifact_id: str, base_url: str | None, from_host: str | None,
          remote_store: str | None, store: Path, trust: Path, ca_file: Path | None,
          client_cert: Path | None, client_key: Path | None, bearer_token_file: Path | None,
          timeout: float, max_download_mib: int) -> None:
    """Fetch a pinned bundle over verified HTTPS or SSH and publish it locally.

    \b
    Over HTTPS (--url), the server serves artifacts/ARTIFACT_ID/
    {manifest.json,bundle.tar} below the URL. Over SSH (--from-host HOST
    --remote-store PATH), the same layout is read below PATH on a configured
    host, through vq's usual SSH connection to it; a store that
    `vq runtime-bundle publish` filled can be served that way as it is.
    HTTPS stores may require --bearer-token-file, --client-cert and optionally
    --client-key; server certificate and hostname verification remain mandatory.

    Either way the pin is checked before any connection, an artifact already
    published here is verified and reused without one, the manifest is
    authenticated before the archive is requested, and the archive is capped
    at --max-download-mib. An interrupted transfer can be retried under the
    same immutable identity. No deployment, build or runtime selection occurs.
    """
    from vq.runtime_bundle import fetch as fetch_bundle
    from vq.runtime_bundle import fetch_over_ssh

    if (base_url is None) == (from_host is None):
        raise click.UsageError("give exactly one of --url and --from-host")
    if (from_host is None) != (remote_store is None):
        raise click.UsageError("--from-host and --remote-store go together")
    for option, value in (("--ca-file", ca_file), ("--client-cert", client_cert),
                          ("--client-key", client_key), ("--bearer-token-file", bearer_token_file)):
        if from_host is not None and value is not None:
            raise click.UsageError(f"{option} applies to --url only")
    if client_key is not None and client_cert is None:
        raise click.UsageError("--client-key requires --client-cert")

    def action() -> dict[str, Any]:
        limit = max_download_mib * 1024**2
        if from_host is not None:
            from vq import config, transport

            try:
                host_cfg = config.load_config().host(from_host)
            except config.ConfigError as exc:
                raise click.UsageError(str(exc)) from None
            assert remote_store is not None
            manifest = fetch_over_ssh(transport._ssh_base(host_cfg), remote_store,
                                      artifact_id, store, trust, timeout=timeout,
                                      max_archive_bytes=limit)
        else:
            assert base_url is not None
            manifest = fetch_bundle(base_url, artifact_id, store, trust, ca_file=ca_file,
                                    client_cert=client_cert, client_key=client_key,
                                    bearer_token_file=bearer_token_file,
                                    timeout=timeout, max_archive_bytes=limit)
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
