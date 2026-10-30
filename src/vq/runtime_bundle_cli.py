"""Opt-in portable-artifact commands, independent of fleet updates."""

from __future__ import annotations

import json
import tarfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click

from vq.runtime_bundle import _MAX_ARCHIVE_BYTES

_PATH = click.Path(path_type=Path)
_FILE = click.Path(exists=True, dir_okay=False, path_type=Path)
_DIRECTORY = click.Path(exists=True, file_okay=False, path_type=Path)
_TRUST_HELP = "Operator trust policy: manifest pins and publisher signing keys."


def _run(action: Callable[[], dict[str, Any]]) -> None:
    from vq.admin import AdminError
    from vq.runtime_slots import RuntimeSlotError
    from vq.transport import RemoteError

    try:
        result = action()
    except (OSError, ValueError, tarfile.TarError, RuntimeSlotError, AdminError,
            RemoteError) as exc:
        raise click.ClickException(str(exc)) from None
    click.echo(json.dumps(result, indent=2, sort_keys=True))


@click.group("runtime-bundle")
def runtime_bundle() -> None:
    """Package and distribute already-built runtimes through file and HTTPS stores.

    Uses dedicated artifact/target roots, independent of legacy runtime slots.
    Trust pins and signer keys must come from an operator-controlled source.
    No fleet config, scheduler registration, compilation or existing job pins
    are changed.
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


@runtime_bundle.command("build", context_settings={"ignore_unknown_options": True})
@click.option("-d", "--directory", "source", type=_DIRECTORY, default=None,
              help="Product source tree, copied into the build job's workspace.")
@click.option("-a", "--archive", type=_FILE, default=None,
              help="A .tar/.tar.gz of the source tree instead of -d, unpacked into the "
                   "build job's workspace.")
@click.option("--description", required=True, type=_FILE)
@click.option("--build-dir", required=True, type=_PATH,
              help="New absolute directory the builder fills (given as $VQ_BUILD_DIR).")
@click.option("--output", required=True, type=_PATH, help="New absolute bundle directory.")
@click.option("--host", default=None,
              help="Build on this configured host's daemon; the paths are on that host.")
@click.option("--cpus", default=1, show_default=True, type=click.IntRange(min=1))
@click.option("--mem-mb", type=click.IntRange(min=1), default=None)
@click.option("--time-limit", type=click.IntRange(min=1), default=None,
              help="Build wall time in seconds.")
@click.option("--name", "job_name", default=None, help="Build job name.")
@click.argument("builder", nargs=-1, required=True, type=click.UNPROCESSED)
def build(source: Path | None, archive: Path | None, description: Path, build_dir: Path,
          output: Path, host: str | None, cpus: int, mem_mb: int | None,
          time_limit: int | None, job_name: str | None, builder: tuple[str, ...]) -> None:
    """Run a product builder as a queued job; pack its output with build-finish.

    BUILDER is the product's own build command, given after --. It runs in a
    copy of the source tree on this host's daemon, or with --host on that
    host's, and writes the build into $VQ_BUILD_DIR. The build record is
    written beside the bundle as OUTPUT.vq-build.json.
    """
    from vq.runtime_bundle_build import submit_build, submit_remote_build

    if (source is None) == (archive is None):
        raise click.UsageError("give exactly one of -d/--directory and -a/--archive")
    if host is None:
        _run(lambda: submit_build(
            source=source, archive=archive, description=description, build_dir=build_dir,
            output=output, builder=list(builder), cpus=cpus, mem_mb=mem_mb,
            wall_time_seconds=time_limit, job_name=job_name, multi_user=_multi_user()))
        return
    if source is None:
        raise click.UsageError("--host builds from a source tree: use -d/--directory")
    host_cfg = _remote_host(host)
    _run(lambda: submit_remote_build(
        host, host_cfg, source=source, description=description, build_dir=str(build_dir),
        output=str(output), builder=list(builder), cpus=cpus, mem_mb=mem_mb,
        wall_time_seconds=time_limit, job_name=job_name))


@runtime_bundle.command("build-finish")
@click.argument("record")
@click.option("--host", default=None,
              help="Finish on this configured host; RECORD is a path on that host.")
def build_finish(record: str, host: str | None) -> None:
    """Pack a completed build into its bundle; exit 3 while it is still building.

    A failed or killed build is refused. Finishing again reports the bundle
    already packed.
    """
    from vq.runtime_bundle_build import finish_build, finish_remote_build

    result: dict[str, Any] = {}

    def action() -> dict[str, Any]:
        if host is None:
            result.update(finish_build(Path(record), multi_user=_multi_user()))
        else:
            result.update(finish_remote_build(host, _remote_host(host), record))
        return result

    _run(action)
    if result["status"] == "building":
        raise SystemExit(3)


def _remote_host(name: str) -> Any:
    from vq import config
    from vq.host import is_local_host

    if is_local_host(name):
        raise click.UsageError("--host names a remote build host; leave it out to build here")
    hosts = config.load_config().hosts
    if name not in hosts:
        raise click.UsageError(f"--host {name}: not a configured [hosts.{name}]")
    return hosts[name]


def _multi_user() -> bool:
    from vq.cli import _multi_user_active

    return _multi_user_active(None)


@runtime_bundle.command("sign")
@click.argument("bundle", type=_DIRECTORY)
@click.option("--key", required=True, type=_FILE,
              help="OpenSSH private key, or a public key whose private half ssh-agent holds.")
def sign(bundle: Path, key: Path) -> None:
    """Sign a bundle's manifest for trust policies that name the signer's key.

    BUNDLE is a packed bundle or a published artifact directory. Its archive
    is checked first; then `ssh-keygen -Y sign` (OpenSSH 8.1+) writes
    manifest.json.sig beside the manifest, replacing an earlier signature.
    The key never enters the bundle.
    """
    from vq.runtime_bundle import SIGNATURE_FILE
    from vq.runtime_bundle import sign as sign_bundle

    def action() -> dict[str, Any]:
        manifest = sign_bundle(bundle, key)
        return {"artifact_id": manifest.artifact_id, "publisher": manifest.build.publisher,
                "signature": str(bundle / SIGNATURE_FILE)}

    _run(action)


@runtime_bundle.command("publish")
@click.argument("bundle", type=_DIRECTORY)
@click.option("--store", required=True, type=_PATH, help="Absolute artifact store root.")
@click.option("--trust", required=True, type=_FILE, help=_TRUST_HELP)
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
@click.option("--trust", required=True, type=_FILE, help=_TRUST_HELP)
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
              type=click.IntRange(min=1, max=_MAX_ARCHIVE_BYTES // 1024**2),
              help="Maximum archive download size in MiB.")
def fetch(artifact_id: str, base_url: str | None, from_host: str | None,
          remote_store: str | None, store: Path, trust: Path, ca_file: Path | None,
          client_cert: Path | None, client_key: Path | None, bearer_token_file: Path | None,
          timeout: float, max_download_mib: int) -> None:
    """Fetch a trusted bundle over verified HTTPS or SSH and publish it locally.

    \b
    Over HTTPS (--url), the server serves artifacts/ARTIFACT_ID/
    {manifest.json,bundle.tar} below the URL. Over SSH (--from-host HOST
    --remote-store PATH), the same layout is read below PATH on a configured
    host, through vq's usual SSH connection to it; a store that
    `vq runtime-bundle publish` filled can be served that way as it is.
    HTTPS stores may require --bearer-token-file, --client-cert and optionally
    --client-key; server certificate and hostname verification remain mandatory.

    Either way the ID must be pinned, or the trust policy must name signers,
    before any connection; an artifact already published here is verified and
    reused without one. The manifest is authenticated, by its pin or by the
    manifest.json.sig fetched beside it, before the archive is requested, and
    the archive is capped at --max-download-mib. An interrupted transfer can be
    retried under the same immutable identity. No deployment, build or runtime
    selection occurs.
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


def _bound_generations(root: Path) -> dict[str, list[str]]:
    """#161: generations of ``root`` that this host's queued or running jobs bind."""
    from vq import config
    from vq.listing import list_jobs

    wanted = {str(root), str(root.resolve())}
    bound: dict[str, list[str]] = {}
    for spec in list_jobs("localhost", multi_user=config.system_multi_user_enabled()):
        pin = spec.runtime_bundle
        if pin is None or spec.is_terminal:
            continue
        if pin.root in wanted or str(Path(pin.root).resolve()) in wanted:
            bound.setdefault(pin.artifact_id, []).append(spec.id)
    return bound


@runtime_bundle.command("generations")
@click.option("--root", required=True, type=_PATH, help="The deployment target root.")
@click.option("--json", "as_json", is_flag=True, help="Print the vq.runtime-generations/1 JSON.")
def generations(root: Path, as_json: bool) -> None:
    """List ROOT's deployed generations, their programs and which are verified.

    Read-only. A generation is verified when its manifest has its identity and
    its deployment receipt says it verified, the proof a job bound to it
    relies on. `vq placement fleet --runtime-root` reads this from a host.
    """
    from vq.runtime_bundle import deployed_generations

    if as_json:
        _run(lambda: deployed_generations(root))
        return
    try:
        listing = deployed_generations(root)
    except (OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from None
    for entry in listing["generations"]:
        marks = [label for label in ("current", "previous")
                 if listing[label] == entry["artifact_id"]]
        click.echo(f"{entry['artifact_id'][:12]}  {entry['program'] or '?':<20} "
                   f"{'verified' if entry['verified'] else 'NOT verified'}"
                   + (f"  ({', '.join(marks)})" if marks else ""))


@runtime_bundle.command("prune")
@click.option("--root", required=True, type=_PATH, help="The deployment target root.")
@click.option("--keep", "keep", multiple=True, metavar="ID",
              help="Also keep this generation (its full SHA256 identity); repeatable. Use it "
              "for generations that scheduler jobs bind from another driver's queue.")
@click.option("--apply/--plan", default=False, help="Default: read-only plan.")
def prune(root: Path, keep: tuple[str, ...], apply: bool) -> None:
    """Remove deployed generations nothing keeps, after a read-only plan.

    Kept: the current and previous selections, every generation a queued or
    running job in this host's queue is bound to, and --keep. Everything
    else is listed as prunable with its size; --apply removes it under the
    deployment lock, rereading the bindings there. The artifact store keeps
    the bundle, so `deploy` brings a pruned generation back. Jobs bound from
    another host's queue (scheduler jobs) are not visible here: --keep them.
    """
    import re

    from vq.runtime_bundle import BundleError, prune_plan
    from vq.runtime_bundle import prune as apply_prune

    def action() -> dict[str, Any]:
        for identity in keep:
            if not re.fullmatch(r"[0-9a-f]{64}", identity):
                raise BundleError(f"--keep {identity!r}: a generation is named by its "
                                  "full SHA256 identity")
        kept = frozenset(keep)
        if apply:
            return apply_prune(root, bound=lambda: _bound_generations(root), keep=kept)
        return prune_plan(root, bound=_bound_generations(root), keep=kept)

    _run(action)


@runtime_bundle.command("deploy")
@click.argument("artifact_id")
@click.option("--store", required=True, type=_DIRECTORY)
@click.option("--root", required=True, type=_PATH, help="Independent absolute target root.")
@click.option("--trust", required=True, type=_FILE, help=_TRUST_HELP)
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
    deploy the previous artifact ID; retained paths and job references survive
    until `prune` removes a generation nothing keeps.
    """
    from vq.runtime_bundle import TargetProfile, _read_json, deploy, deploy_plan

    def action() -> dict[str, Any]:
        profile = TargetProfile.model_validate_json(_read_json(target_profile))
        if apply:
            return deploy(store, artifact_id, root, trust, profile, timeout=smoke_timeout)
        return deploy_plan(store, artifact_id, root, trust, profile)

    _run(action)
