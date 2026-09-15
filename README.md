<!-- attribution -->

_Created and maintained by Dr. Michael F. Peintinger._

# vq

**vq is a cross-machine job queue.** It takes a command, runs it on the
machine you point it at -- your laptop, a server over SSH, or a cluster behind
PBS or SLURM -- and tracks it until you fetch the results back. It was built
to queue [vibe-qc](https://github.com/vibe-qc/vibe-qc) calculations
and it does not care whether that is what you run.

```sh
vq submit compute -- python optimise.py
vq queue
vq fetch <jobid> -o results/
```

vq requires Python 3.12 or newer. It does not build or install vibe-qc, and
it installs nothing on the remote host but itself: its entire transport is
SSH, so if you can `ssh` to a machine, vq can queue work on it.

Full documentation: <https://vibe-qc.com/vibe-queue/docs/>

## Install

Run the source installer from the repository root:

```sh
./scripts/install.sh
.venv/bin/vq --version
```

The default `core` profile provides the CLI and daemon in the dedicated
`.venv` environment. Add the web dashboard or development tools when
needed:

| Profile | Contents |
| --- | --- |
| `core` | CLI and daemon; default |
| `web` | Core plus dashboard |
| `test` | Core plus tests |
| `dev` | Core, tests, lint, and typing tools |
| `all` | Web plus all development tooling |

```sh
./scripts/install.sh --extras web
./scripts/install.sh --editable --extras dev
```

Copied installation is the default and is recommended for a stable daemon.
Editable installation is intended for development.

## Test the source checkout

Run pytest from the repository root with this checkout's source first on the
import path. Clear inherited pytest options so a parent shell cannot disable
the conftest:

```sh
env -u PYTEST_ADDOPTS PYTHONPATH="$PWD/src" python -m pytest tests -q -p no:cacheprovider
```

The conftest installs session and per-test HOME, XDG, state, config, archive,
multi-user, and web-token sandboxes before collection, verifies that `vq` was
imported from this checkout, and fails the run if any default HOME/XDG
persistence sentinel changes. The production path resolver requires every
pytest process and inherited child to carry `VQ_TEST_SANDBOX_ROOT`, and every
persistent path must resolve beneath that root (including through symlinks).
Consequently `--noconftest`, inherited live overrides, and stale editable
installs fail closed instead of contacting a live daemon or writing live files.
Tests of documented production defaults run in a subprocess with a disposable
HOME and the inherited pytest marker removed.

## Maintain the source installation

```sh
.venv/bin/vq self-update --accepted-report vX.Y.Z
./scripts/reinstall.sh   # only while no daemon runs
./scripts/uninstall.sh --dry-run
./scripts/uninstall.sh
```

Update moves the whole vibe-qc Git checkout unless `--skip-git` is selected.
Use `vq self-update` (or `vq admin update`) when this environment owns a
running supervised daemon. Direct scripts never own service restart or Git +
venv rollback together. Reinstall keeps Git unchanged and durably restores the
previous inactive environment if the new one does not verify.

If `vq web install` created a separately supervised dashboard, refresh and
verify that service after an update or reinstall:

```sh
.venv/bin/vq web install
.venv/bin/vq web status
```

Run `vq web uninstall` before removing the environment, while its command still
exists. A daemon-owned web sidecar is maintained with the daemon instead and
must not be installed a second time as a separate service.

Uninstall removes only the dedicated environment by default. Queue state, job
history, workspaces, logs, configuration, systemd units, launchd agents, and
multi-user `/opt/vq` deployments are retained. State and configuration purge
flags are separate and irreversible, so always inspect `--dry-run` first.

The default user paths are `${XDG_DATA_HOME:-~/.local/share}/vq` for state and
`${XDG_CONFIG_HOME:-~/.config}/vq` for configuration. `VQ_STATE_DIR` and
`VQ_CONFIG_DIR` can override them. To remove data deliberately:

```sh
./scripts/uninstall.sh --purge-state --yes
./scripts/uninstall.sh --purge-config --yes
./scripts/uninstall.sh --all --yes
```

State purge is refused while queued work exists unless `--force` is also
supplied.

## Documentation

The site at <https://vibe-qc.com/vibe-queue/docs/> is split by audience,
because this corpus serves three readers who want almost nothing in common:

- **[Running jobs](docs/user/index.md)** -- submit, watch, fetch results.
  Dependencies, arrays, retries, and what to do when a job will not start.
- **[Running a host](docs/operator/index.md)** -- install and supervise the
  daemon, register engines, expose a host safely, cap what a job may consume,
  deploy for several users, recover a host that stopped answering.
- **[The agent contract](docs/agent/index.md)** -- stable JSON shapes, exit
  codes, lifecycle states, and the invariants that hold across versions.

Also: [cutting a release](docs/release_process.md),
[version compatibility](docs/version_compatibility.md),
[contributing](CONTRIBUTING.md), [security policy](SECURITY.md),
[changelog](CHANGELOG.md).


## History

This repository begins at `4899089` (2026-09-08), the first commit of the
split-out project. Development before that point took place in a private
monorepo, which is retained privately; the history was deliberately not
transferred. vq's version number did not restart with it -- the split
inherited `0.25.7`, the version the fleet was already running.

## License

Mozilla Public License 2.0. See [LICENSE](LICENSE).

Copyright (c) 2026 Michael F. Peintinger and vibe-qc contributors.
