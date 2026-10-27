# Adding programs

**Audience: you want a host to run a particular program (vibe-qc, ORCA,
CRYSTAL, PySCF, vibe-view, or your own code) and want jobs to name it
rather than hard-code its paths.**

vq runs any command, so registering a program is optional. Registering it
gives you three things:

* `vq programs HOST` checks that it is installed and working **before** a job
  fails on a missing binary.
* A job submitted with `--program NAME` records which program, and for a git
  checkout which revision, produced its results.
* On a daemon host the job finds the program through environment variables,
  so the same payload works on every host.

## Where the registration goes

A `[programs.NAME]` table goes in `~/.config/vq/config.toml` **on the host
that has the program**, not on your laptop. `vq programs HOST` run from the
laptop asks that host over SSH, so the checks happen where the files are.

## The four kinds

### `binary`: an executable

For a compiled program such as ORCA, CRYSTAL, xtb or Psi4:

```toml
[programs.orca]
kind = "binary"
binary = "/opt/orca/6.1.1/orca"      # absolute path
description = "ORCA 6.1.1"

[programs.xtb]
kind = "binary"
binary = "/home/USER/xtb-dist/bin/xtb"
description = "xtb GFN-xTB/GFN-FF"
```

vq checks that the file exists and is executable. A job submitted with
`--program orca` on that daemon host gets `VQ_PROGRAM_EXE`:

```sh
vq submit workstation --program orca --cpus 4 -d ./h2o -- \
    sh -c '"$VQ_PROGRAM_EXE" h2o.inp > h2o.out'
```

vq runs no shell of its own, so redirection needs `sh -c` or a wrapper.
The repository ships two wrappers that handle the calling conventions of
ORCA and CRYSTAL, [`contrib/run-orca.sh`](https://github.com/vibe-qc/vibe-queue/blob/main/contrib/run-orca.sh)
and [`contrib/run-crystal.sh`](https://github.com/vibe-qc/vibe-queue/blob/main/contrib/run-crystal.sh);
[running ORCA and CRYSTAL](orca_crystal.md) shows them in use.

### `venv`: a Python environment with a git checkout

For vibe-qc, vibe-view or any Python project installed in its own venv:

```toml
[programs.vibeqc-dev]
kind = "venv"
python = "/home/USER/vibe-qc/.venv/bin/python"
git_dir = "/home/USER/vibe-qc"
branch = "main"
import_check = "vibeqc"                 # the module that must import
update_script = "scripts/update.sh"     # lets `vq admin update vibeqc-dev` refresh it
description = "vibe-qc development (main branch)"
```

`vq programs` reports the checkout's revision, whether it is dirty, and
whether `import_check` imports. A job submitted with `--program vibeqc-dev`
gets `VQ_PROGRAM_PYTHON`, `VQ_PROGRAM_BIN` (the venv's `bin/` directory),
`VQ_PROGRAM_GIT_DIR`, `VQ_PROGRAM_BRANCH` and `VQ_PROGRAM_GIT_SHA`, the exact
revision at dispatch.

A bare `python` in such a job's command runs the venv's interpreter. This
form does so on every daemon host:

```sh
vq submit workstation --program vibeqc-dev -d . -- python water.py
```

The single-file form, `vq submit HOST --program vibeqc-dev water.py`, also
uses the venv's interpreter, including on a remote host with `remote_python`
set. On a remote host it needs a vq there that is as new as your own: an older
one refuses the submit, and the `-d . -- python` form works with any version
([version compatibility](../version_compatibility.md#single-file-program-interpreter-209)).
An explicit `--python`, or an absolute interpreter path in the command, is
always used as given.

A bare `python` with a `binary` or `import` program fails when the job
starts (`cannot supply a managed Python interpreter`), because only a `venv`
program has an interpreter of its own. Name the interpreter explicitly for
those.

Add `healthcheck_command` for a check that goes further than an import, for
example `xvfb-run -a vibe-view capture-selftest` for a headless viewer.

### `import`: a module inside another interpreter

For a package that lives in an environment you registered elsewhere:

```toml
[programs.pyscf]
kind = "import"
python = "/home/USER/vibe-qc/.venv/bin/python"
import_check = "pyscf"
description = "PySCF, in the vibe-qc venv"
```

### `scheduler`: a name for a program that exists only on a cluster

A driver that submits ORCA to a cluster but has no ORCA itself registers the
name only:

```toml
[programs.orca]
kind = "scheduler"
```

`vq programs` reports it as `SCHEDULER_ONLY`, and vq refuses to run it on the
driver. The cluster side is described below.

## Check it

```sh
vq programs workstation                    # NAME  KIND  STATUS  DETAIL
vq programs workstation --json
vq programs --all --require orca           # non-zero exit if any host lacks it
vq doctor workstation
```

`STATUS` is `OK`, `MISSING`, `UNHEALTHY` (it imports but the health check
failed) or `SCHEDULER_ONLY`. vq still accepts a job that names a `MISSING`
program; the job fails only when it calls the program. That is why the check
is worth running first. `--require NAME` exits 1 unless every host has NAME as
`OK`, so a `SCHEDULER_ONLY` registration does not satisfy it. `--all` asks
every `[hosts]` entry in your config. A daemon re-reads its config on
`vq daemon reload`.

## Programs on a PBS or Slurm cluster

A batch job runs on a compute node, where the driver's paths do not exist, so
vq does not inject `VQ_PROGRAM_BIN` or `VQ_PROGRAM_EXE` there. Instead, give
the scheduler host a per-program hook in the **driver's** config. It applies
to every job submitted with that `--program`:

```toml
[hosts.cluster.scheduler_program_hooks.orca]
prologue = ["module load orca/6.1.1"]           # shell lines before the command
epilogue = ["cp -f *.out artifacts/ 2>/dev/null || true"]
command_wrapper = ["/home/USER/bin/orca-site-wrapper"]   # argv prefix
```

```sh
vq submit cluster --program orca --ntasks 8 --cpus 1 -d ./h2o -- h2o.inp
```

The wrapper receives the submitted command as its arguments, so it must not
repeat its own name in the command. Every hook key must match a
`[programs.NAME]` entry on the driver, and `vq doctor cluster` reports one
that does not.

Python runtimes that vq builds and verifies on the cluster, such as a
vibe-qc release, are registered as `scheduler_runtime_deployments`; see
[scheduler runtime deployment](../scheduler_runtime_deployment.md).

## Pin the version a result depends on

For results you intend to publish, pin the program revision at submission:

```sh
vq submit workstation --program vibeqc-dev --expected-sha 0123456789ab input.py
```

The checkout must be at that revision when the job is queued, and the job
fails before it starts if the checkout has moved since. A registration can
also carry a standing pin with `expected_git_sha` or
`expected_import_version`.

## Several environments of one program

To choose between a development and a release environment by name, add
branch routing to the host block in the client's config:

```toml
[hosts.workstation.branches]
main    = "/home/USER/vibe-qc/.venv/bin/python"
release = "/home/USER/vibe-qc-release/.venv/bin/python"
```

`vq submit workstation --branch release input.py` then runs the release
interpreter. Set `provides_branches = ["release"]` on the matching program so
`vq admin update` pauses only the jobs that use that environment.

The complete annotated set of keys is in
[`config.toml.example`](https://github.com/vibe-qc/vibe-queue/blob/main/docs/config.toml.example).
