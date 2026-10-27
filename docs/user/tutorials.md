# Tutorials

Four walkthroughs, each building on the one before. They use example names:
`workstation` is a machine running the vq daemon, `cluster` is a PBS or Slurm
login node driven by `workstation`. Replace them with your own
`~/.ssh/config` aliases.

Setting the machines up is in
[installing vq on machines and clusters](../cluster_install.md).

## 1. A first job on a machine with no queue

Create a small workspace:

```sh
mkdir -p hello && cd hello
cat > count.py <<'EOF'
import os, time
for i in range(5):
    print("step", i, flush=True)
    time.sleep(2)
with open("result.txt", "w") as f:
    f.write(f"done on {os.uname().nodename}\n")
EOF
```

Submit it, watch it and fetch the result:

```sh
vq submit workstation --cpus 1 --mem-mb 500 --time-limit 00:05:00 -d . -- python3 count.py
vq queue workstation                 # pending, then running
vq logs workstation <jobid>          # the "step" lines so far
vq wait workstation <jobid>          # returns when the job has ended
vq fetch workstation <jobid> -o ../results/
cat ../results/<jobid>/result.txt
```

What happened: `submit` uploaded the directory and returned at once with a
job id; the daemon started the job when its CPU and memory budgets allowed;
`fetch` copied the workspace back into `results/<jobid>/`.

Try `vq status workstation <jobid>` while it runs, and `vq events workstation
<jobid>` afterwards for its timeline.

## 2. The same job on a PBS or Slurm cluster

A cluster runs jobs on compute nodes, so the interpreter must be one that
exists there. A bare `python3` is looked up on the cluster's `PATH`:

```sh
vq doctor cluster                    # driver, scheduler commands and helper OK?
vq submit cluster --cpus 1 --mem-mb 500 --time-limit 00:05:00 -d . -- python3 count.py
vq status cluster <jobid>            # shows the scheduler job id once accepted
vq wait cluster <jobid>
vq fetch cluster <jobid> -o ../results/
```

`result.txt` now names a compute node. If the scheduler needs an account or
partition, the operator adds it to the host's `submit_extra`; you do not pass
it on every submit. For an MPI program on Slurm add `--ntasks N --cpus 1`.
[Machines with and without a batch scheduler](../backends.md) lists every
difference.

## 3. A vibe-qc calculation, pinned and followed live

This needs a registered vibe-qc environment on the host; check with:

```sh
vq programs workstation              # vibeqc-dev should be OK
```

Write the input from [running vibe-qc calculations](vibe_qc.md#submit-a-script)
as `water.py`, then preview what it will write and need, without running
the SCF:

```sh
VIBEQC_DRY_RUN=1 python water.py     # needs vibe-qc on this machine; writes water.system
vq estimate water.system --input water.py
```

Submit it on the development environment, pinned to the revision it runs:

```sh
vq programs workstation --json       # read vibeqc-dev's current git SHA
vq submit workstation --program vibeqc-dev --branch main \
    --expected-sha <sha> --cpus 4 --mem-mb 8000 --time-limit 01:00:00 water.py
vq output workstation <jobid> -f     # the SCF trace as it is written
vq fetch workstation <jobid> -o results/
```

`results/<jobid>/` holds `water.out`, `water.system` and `water.qvf`. If the
checkout had moved between submission and start, the job would have failed
before running rather than compute with a different vibe-qc.

## 4. From a sketch to a picture

This needs vibe-sketch and vibe-view with their `queue` extras, and a vq
daemon on the same machine; see
[vibe-sketch and vibe-view with the queue](vibe_family.md#install-the-integration).

```sh
vibe-sketch convert CCO --from smiles -o ethanol.vsk
vibe-sketch submit ethanol.vsk --all --program vibeqc --task sp --method B3LYP --basis def2-SVP
vibe-sketch runs ethanol.vsk         # repeat until the run is completed
```

The run went to vq as a QVF job container. Open the result:

```sh
vibe-view from-vq <jobid>            # fetches the job and opens its .qvf
```

or render a figure headlessly as another queue job, using a registered
`vibeview-dev`:

```sh
vq fetch localhost <jobid> -o results/
vq submit localhost --program vibeview-dev -d results/<jobid> -- \
    sh -c 'xvfb-run -a "$VQ_PROGRAM_BIN/vibe-view" capture ethanol.qvf -o ethanol.png'
```

Substitute the container's file name as `vibe-sketch runs` reports it.

## Where to go next

* [Adding programs](programs.md): register ORCA, CRYSTAL, your own code, or a
  second vibe-qc environment.
* [Running ORCA and CRYSTAL](orca_crystal.md): the same cycle for the
  external codes vq ships wrappers for.
* [Arrays, dependencies, chains and reruns](workflows.md): a parameter sweep
  and the job that analyses it, worked through.
* [Managing jobs](managing.md): names, pausing, killing, resubmitting and
  cleaning up.
* [Troubleshooting](troubleshooting.md): when a job does not start, ends
  early or will not fetch.
* [Calculation campaigns](../campaigns.md): calculate, validate and render as
  one restartable chain.
* [Running jobs](index.md): arrays, chains, dependencies, retries and
  scheduled starts.
