# Clone, download, and install

vq requires **Python 3.12 or newer**. Its repository is **vibe-queue** and
its Python distribution and command are **vq**.

## Source repositories

| Component | Public source | Checkout directory |
| --- | --- | --- |
| vq queue and daemon | [vibe-queue on GitHub](https://github.com/vibe-qc/vibe-queue) | `vibe-queue/` |
| vibe-qc calculation engine | [vibe-qc on GitHub](https://github.com/vibe-qc/vibe-qc) | `vibe-qc/` |
| vibe-view viewer | [vibe-view on GitHub](https://github.com/vibe-qc/vibe-view) | `vibe-view/` |

These are separate repositories with independent versions and environments.
Install vq from the root of its own checkout. The archived `mpei/vibeqc`
monorepo and its nested `vibe-queue/` directory are not the source for new
single-user installations. Installing or updating vq does not install or
update the calculation engine or viewer.

The **[GitHub mirror](https://github.com/vibe-qc/vibe-queue)** contains
selected public snapshots. GitLab remains canonical for development.
Check that the revision you need is present before using it. Public snapshot
commit IDs differ from private development IDs; see [publication](https://github.com/vibe-qc/vibe-queue/blob/main/PUBLICATION.md).

## Clone

Clone the public source snapshot over HTTPS:

```sh
git clone https://github.com/vibe-qc/vibe-queue.git
```

Developers with private GitLab access should use the clone URL supplied by
an administrator. Keep its host, port and SSH settings in external operator
configuration. GitLab remains the canonical development repository.

## Install

After cloning, enter the new directory and run the installer:

```sh
cd vibe-queue
./scripts/install.sh
.venv/bin/vq --version
```

The default copied installation creates `.venv` in that checkout. For the
web dashboard use `./scripts/install.sh --extras web`; for development use
`./scripts/install.sh --editable --extras dev`.

## Download source archives

Select the desired **vq** tag in
[GitHub mirror tags](https://github.com/vibe-qc/vibe-queue/tags) and download
that tag's source archive. Use this repository's archives, not an archive
of the calculation engine or the historical monorepo.

Extract the archive to inspect the source. Its root contains `pyproject.toml`,
`src/vq/`, and `scripts/install.sh`. Archive directory names include the
selected revision; there is no additional nested `vibe-queue/` directory.

**Use a Git clone for the supported lifecycle installer.** A plain source
archive has no Git history, and the default copied installation requires Git
to record its source commit. Downloading an archive does not provide that
provenance. Git-based updates and fleet workflows with accepted release pins
also require a clone.

## Next steps

* [Daemon lifecycle](lifecycle.md): supervision and safe updates.
* [Running a host](operator/index.md): configuration and operations.
* [Running jobs](user/index.md): submit, watch, and fetch results.
* [Multi-user deployment](multi_user_deployment.md): the privileged deployment
  helper still requires a legacy layout; read its split-layout limitation
  before attempting a new installation.
