# Clone, download, and install

**Audience: anyone installing vq,** on a laptop or on a host. Also listed under [Running a host](operator/index.md).

vq requires **Python 3.12 or newer**. Its repository is **vibe-queue** and
its Python distribution is **vibe-queue**, like the repository; the command
and the import package are **vq**.

Through v0.26.9 the distribution was named `vq`. An environment installed by
one of those releases keeps pip's record under that name until it is next
updated: `scripts/update.sh`, `scripts/reinstall.sh` and `vq admin update`
remove that record before installing `vibe-queue`, so `pip show vibe-queue`
then reports the installed version and `pip show vq` reports nothing. A
requirement or an extra that names vq, such as vibe-view's `[queue]`, spells
it `vibe-queue`.

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

## Where vq puts things

A fresh install uses two roots, sorted by lifecycle rather than by content
type, and creates each on first use:

| root | holds | default | override |
|---|---|---|---|
| state | the queue and its locks, archives, the daemon's pidfile, log, socket and control files | `$XDG_DATA_HOME/vq`, else `~/.local/share/vq` | `$VQ_STATE_DIR` |
| scratch | `jobs/` (each job's working directory and results) and `workdirs/` (`$VQ_WORKDIR`) | `<state root>-scratch`, so `~/.local/share/vq-scratch` | `$VQ_SCRATCH_DIR`, or the `scratch_root` config key |
| config | `config.toml`, `web-token` | `$XDG_CONFIG_HOME/vq`, else `~/.config/vq` | `$VQ_CONFIG_DIR` |

The split matters when the two live on different disks, which is the usual
reason to change it: the state root stays small and must be kept, while
scratch is where the volume is — on two measured compute hosts job workdirs
were over 99 % of what vq had on disk. Put the scratch root on the roomy
filesystem:

```toml
# ~/.config/vq/config.toml
min_vq_version = "0.26.10"
scratch_root = "/data/vq-scratch"
```

Set `min_vq_version` alongside it: a vq that predates the key ignores
unrecognized top-level keys by design, and you do not want one build writing
scratch in one place while another reads it somewhere else. `jobs_root` and
`workdir_root` split the two trees further if results and scratch belong on
different volumes; unset, they follow `scratch_root`.

`vq daemon status` prints the resolved roots. Changing a root later is safe —
existing jobs keep the paths they recorded, and accounting, `--delete` and the
age sweep follow those records into the root they name — but it must be changed
for the daemon **and** every CLI caller at once, which is why the config key is
preferable to the environment variable. In multi-user mode there is no config
key: `$VQ_MULTI_USER_SCRATCH_ROOT` is set once for the whole deployment, since
a per-caller key would let the root daemon and a user's CLI disagree. See
[the fleet layout](fleet_layout.md) for the migration runbook.

Multi-user deployments use `/var/lib/vq` and `/var/lib/vq-scratch`
(`$VQ_MULTI_USER_ROOT`, `$VQ_MULTI_USER_SCRATCH_ROOT`) with per-uid subtrees.

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
