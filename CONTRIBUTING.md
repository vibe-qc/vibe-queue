# Contributing to vq

vq is the cross-machine job queue of the vibe-qc toolset. It queues
[vibe-qc](https://github.com/vibe-qc/vibe-qc) calculations, or any
other command-line workload, onto a local machine, a remote host over SSH, or
a cluster scheduler backend. It is useful on its own and does not build or
install vibe-qc.

The distribution is named `vq`; the repository is named `vibe-queue`.

## Where to report what

| What | Where |
|---|---|
| A queue bug: a job that will not dispatch, wrong state, a daemon that will not start | [vibe-queue issues](https://github.com/vibe-qc/vibe-queue/issues) |
| A security-relevant bug | **Email mpei@vibe-qc.com** — see [SECURITY.md](SECURITY.md). Do not open a public issue. |
| A wrong number in a calculation vq merely ran | [vibe-qc issues](https://github.com/vibe-qc/vibe-qc/issues) |
| A viewer or `.qvf` rendering defect | [vibe-view issues](https://github.com/vibe-qc/vibe-view/issues) |
| An ambiguity in the QVF format itself | [qvf issues](https://github.com/vibe-qc/qvf/issues) |

When you cannot tell whether the queue or the workload is at fault, file it
here — moving an issue is cheap.

**A bug report that travels well** carries: `vq --version`, `vq doctor`
output, the job id and `vq show <id>`, the relevant slice of
`vq logs <id>`, and whether the job was local, SSH-dispatched, or
scheduler-dispatched. Redact host names and addresses; we do not need them
and this repository is public.

## Getting set up

Clone the public source snapshot over HTTPS:

```sh
git clone https://github.com/vibe-qc/vibe-queue.git
```

Developers with private GitLab access should use the clone URL supplied by
an administrator. Keep its host, port and SSH settings in external operator
configuration. GitLab remains the canonical development repository.

Then install from the repository root:

```sh
cd vibe-queue
./scripts/install.sh --editable --extras dev
```

See [clone and download options](docs/installation.md) for availability and
source archives. Development and the project tracker remain on canonical
GitLab; the mirror also provides GitHub issue forms and a pull request template.

Or, from the same repository root, for a plain development install:

```sh
python -m venv .venv
.venv/bin/pip install -e '.[test,web]'
```

vq requires **Python 3.12 or newer**. `[test]` pulls pytest, httpx and
jsonschema; `[web]` pulls the FastAPI dashboard stack, which several tests
need. `[dev]` adds ruff and mypy.

## Running the tests

Run pytest from the repository root with this checkout's source first on the
import path, and clear inherited pytest options so a parent shell cannot
disable the nested conftest:

```sh
env -u PYTEST_ADDOPTS PYTHONPATH="$PWD/src" python -m pytest tests -q -p no:cacheprovider
```

That invocation is not decoration. The conftest installs session-wide and
per-test HOME, XDG, state, config, archive, multi-user and web-token
sandboxes **before collection**, verifies that `vq` was imported from this
checkout, and fails the run if a default persistence sentinel changes. The
production path resolver requires every pytest process and every inherited
child to carry `VQ_TEST_SANDBOX_ROOT`, and every persistent path must resolve
beneath that root, symlinks included.

The consequence is deliberate: `--noconftest`, inherited live overrides and a
stale editable install **fail closed** rather than quietly contacting a live
daemon or writing live state. If you find yourself reaching for
`--noconftest`, you are about to run the suite against your own queue.

For ordinary work, run the lanes your change touches and name them in the
commit message.

### Known-red lanes

**There are none, as of 2026-09-09.** The suite is fully green: 8309 passed,
36 skipped, 0 failed locally, and green in CI on the v0.26.0 release
candidate (pipeline 5084).

Three lanes used to be listed here -- `test_scheduler_admin_update` (x2, CI),
`test_admin_lifecycle_transaction` and `test_multi_user_refresh_helper` (x2,
local). The first was retired by `670d255` / `dcd01dc`, which dropped a stale
stage-prune contract; the other two pass on a clean run. They were carried as
"known red" long enough that the label outlived the failures.

So a red suite is now a real signal. If something fails, it is either yours or
new -- do not assume it is background noise. Two things that look like
failures but are not:

* **Editing a source file while the suite runs.**
  `test_read_vq_version_from_source_for_live_install` compares the *imported*
  `vq.__version__` against a *fresh read* of `vq/__init__.py`, so bumping the
  version mid-run reports genuine drift. It is doing its job.
* **Two chats in one working tree.** A test file changing under a running
  suite produces failures that vanish on re-run. Check `git status`.

## The daemon is stateful, and the tests know it

vq owns a supervised long-running daemon, a lifecycle lock bound to the
owning uid, and on-disk state that outlives any single process. Three habits
follow:

- **Never run the suite as root.** vq refuses several operations for root by
  design, and the lifecycle lock binds a checkout to its owner. CI creates an
  unprivileged `vqtest` user for exactly this reason.
- **Reinstall and update are not the same operation.** `vq self-update` /
  `vq admin update` own service restart together with git and venv rollback.
  `scripts/reinstall.sh` deliberately owns neither and must not run while a
  daemon is up.
- **State-shape changes need a migration story.** `docs/version_compatibility.md`
  is the contract between a daemon and the state it inherits from an older
  one. A field added without a compatibility note is a field that breaks a
  rolling fleet update.

## Pre-commit hook (one-time setup)

After cloning, point git at the tracked `.githooks/` directory:

```sh
git config --local core.hooksPath .githooks
```

`pre-commit` refuses staged additions containing absolute paths into a
developer's home directory, the maintainer's day-job employer name, or
private IPv4 literals. For address examples, use the reserved documentation
ranges in [RFC 5737](https://www.rfc-editor.org/rfc/rfc5737):
`192.0.2.0/24`, `198.51.100.0/24` or `203.0.113.0/24`. Real hosts,
accounts, endpoints and addresses belong in external private configuration.

This matters more here than in the sibling repositories. vq is the fleet's
control plane, so real host names, real addresses, real ports and real
account names are the natural vocabulary of its documentation, its docstrings
and its fixtures. They arrive by default, not by accident, and once published
they cannot be unpublished.

Keep the path **relative**. An absolute path bakes the checkout location into
the config, and git skips a missing hooks directory **silently** — no warning,
no error — so the guard goes inert without anyone noticing. That is why
`tests/test_no_maintainer_paths.py` re-checks the whole tree in CI: the hook
is the fast half, the test is the half that always runs.

The hook checks each match separately and redacts matched values in its
diagnostics. Scan binary assets and filenames separately before publication;
this staged-text guard does not inspect them.

To confirm the hook runs without making a commit:

```sh
git hook run pre-commit
```

To bypass it for a reviewed exception, commit with `--no-verify` and explain
why in the commit message.

## Code style

- **Python 3.12+.** PEP 8, four-space indent. Start every module with
  `from __future__ import annotations`. Type hints on public API.
- `ruff check src/vq tests` with the config in `pyproject.toml`, line length
  100. CI runs it as a blocking job; keep the tree clean.
- `mypy` is configured `strict`. Not yet a CI gate; do not add new failures.
- Prefer editing existing modules over introducing new ones. `src/vq/` is
  already wide; a new file needs a reason beyond "this felt separate".
- Optional extras (`fastapi`, `uvicorn`, `jinja2`) are imported **lazily**,
  inside the function that needs them, with an `ImportError` branch naming
  the extra. A module-scope import of `[web]` makes the whole CLI need it.

## Commit messages

- Imperative mood, first line under 72 characters.
- **A commit that fixes a tracked issue carries its iid in the subject line**,
  e.g. `fix(dispatch): retry a scheduler probe that raced the job id (#42)`.
  Not in the body, not in a trailer — the subject, because that is what
  `git log --oneline` shows and what a triage sweep greps.
- Longer rationale in the body when the *what* does not explain itself.
- Co-author trailers are fine for pair work.
- Do not tag `vX.Y.Z` and do not push to `release`; releases are cut
  separately.

## Before you open a merge request

- Run the affected lanes locally and name them.
- Keep `main` release-ready: no half-finished code paths, docs in parity,
  `CHANGELOG.md` `[Unreleased]` reflecting what you actually landed.
- **vq versions are load-bearing for the fleet.** Private operations storage
  holds fleet release reports and runtime pins reference them by version. A change to the
  version scheme, the report schema, or `vq --version` output is a
  cross-repository change; raise it before you write it.

## What we will not accept (for now)

- New **hard** runtime dependencies without prior discussion. vq installs on
  compute hosts that nobody wants to debug; the dependency floor is a
  feature. A new optional extra with a lazy import is a much easier
  conversation.
- Changes that widen what the daemon or the web dashboard exposes without a
  matching entry in [SECURITY.md](SECURITY.md).
- Real host names, addresses, ports or account names in tracked content, even
  in a comment, even in a fixture.
- Changes that regress the suite without a stated rationale and a plan to
  restore.

## Licensing

By submitting a patch, merge request, or any other contribution to
vibe-queue, you agree that:

1. Your contribution is licensed under the Mozilla Public License 2.0 (the
   project license — see [`LICENSE`](LICENSE)).
2. You grant the project owner (Michael F. Peintinger) the right to relicense
   your contribution under alternative terms, including a future commercial
   license, alongside the MPL 2.0 public license. You retain copyright.

This is a lightweight alternative to a formal Contributor License Agreement.
If you are not comfortable with (2), please open an issue before contributing
so we can discuss.

vq is pure Python. Its runtime dependencies —
[click](https://click.palletsprojects.com/) (BSD-3) and
[pydantic](https://docs.pydantic.dev/) (MIT) — are MPL-compatible, as are the
optional [fastapi](https://fastapi.tiangolo.com/) (MIT),
[starlette](https://www.starlette.io/) (BSD-3),
[uvicorn](https://www.uvicorn.org/) (BSD-3) and
[jinja2](https://jinja.palletsprojects.com/) (BSD-3) `[web]` extras, and the
[pytest](https://docs.pytest.org/) (MIT),
[httpx](https://www.python-httpx.org/) (BSD-3) and
[jsonschema](https://python-jsonschema.readthedocs.io/) (MIT) test extras.

### Private privacy policy

The portable guard checks home paths without storing real user names. Operators
can add private literal terms through `VIBE_PRIVACY_TERMS_FILE` or the clone-local
`privacy.termsFile` Git setting. Use an absolute path to a UTF-8 file outside the
source checkout, with one literal term per line. Matching is case-insensitive;
a configured missing, empty or in-tree file blocks the check. Keep the private
terms file and resolved installation paths out of commits. The repository's
privacy tests load the same external policy when configured.

Site wrappers and deployment configuration are maintained separately in private
operations storage. Product changes must use portable examples and preserve the
existing generic installation and configuration APIs.
