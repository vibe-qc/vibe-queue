# vibe-sketch and vibe-view with the queue

**Audience: you prepare structures in vibe-sketch, compute them with
vibe-qc (or ORCA), and look at the results in vibe-view.** vq is the part in
the middle that runs the calculations and keeps track of them.

```text
vibe-sketch           vq                      vibe-qc / ORCA          vibe-view
draw, embed,   -->    submit, queue,   -->    compute on a     -->    fetch, open,
write inputs          track, fetch            daemon host or          watch live,
                                              cluster                 capture figures
```

Each product is installed on its own and works without the others. The
queue integration is an optional extra in vibe-sketch and vibe-view.
Their own manuals are the reference: [vibe-sketch](https://vibe-qc.com/vibe-sketch/docs/)
and [vibe-view](https://vibe-qc.com/vibe-view/docs/).

## Install the integration

vq is not on a package index, so install it into the other product's
environment from a vq checkout first, then add the `queue` extra:

```sh
git clone https://github.com/vibe-qc/vibe-queue.git ~/vibe-queue

# in the vibe-view checkout
.venv/bin/python -m pip install ~/vibe-queue
.venv/bin/python -m pip install -e '.[queue]'

# in the vibe-sketch checkout
.venv/bin/python -m pip install ~/vibe-queue
.venv/bin/python -m pip install -e '.[queue]'
```

Both talk to the **vq daemon on the same machine**, through vq's Python API,
so that machine needs a running daemon (`vq daemon install`; see
[installing vq](../cluster_install.md#3-a-daemon-host-a-machine-with-no-queue))
and a registered vibe-qc program, for example `vibeqc-dev`
([adding programs](programs.md)). Register `orca` too if you want ORCA
inputs to run. Check with:

```sh
vq daemon status
vq programs localhost
```

## From vibe-sketch: submit what you drew

vibe-sketch writes the inputs and submits them as one group, recording each
run in the document:

```sh
vibe-sketch convert CCO --from smiles -o ethanol.vsk
vibe-sketch submit ethanol.vsk --all --program vibeqc --task sp --method B3LYP --basis def2-SVP
vibe-sketch runs ethanol.vsk              # each run's state and where its result is
vibe-sketch cancel-group ethanol.vsk g1   # cancel a group that has not finished
```

A vibe-qc job goes to vq as a [QVF job container](vibe_qc.md#submit-a-qvf-job-container),
so the result is written into a file that still holds the sketch. Every
submission carries the `vibe-sketch` tag, so you can also follow it with vq
directly:

```sh
vq queue localhost --tag vibe-sketch
vq status localhost <jobid>
```

For staged work, such as an optimization followed by frequencies for every
species of a reaction scheme, vibe-sketch compiles a recipe into a
[vq campaign](../campaigns.md) and prints the command that runs it:

```sh
vibe-sketch campaign-compile scheme.vsk recipe1 --out campaign-dir --program vibeqc-dev
# ...prints the `vq campaign run ...` command for this campaign; run it as printed
vibe-sketch campaign-status scheme.vsk recipe1 recipe1-c1
```

`vq campaign run` can be stopped and started again at any point;
`vibe-sketch campaign-render` then turns the verified stages into aligned
QVF files and, with vibe-view installed, images.

## In vibe-view: watch, open and submit

`vibe-view open` and `vibe-view desktop` have a **vq Job Manager** panel. It
lists the local queue's jobs by state, shows a job's log tail, and opens a
completed job's QVF archives after fetching it. **Watch live** opens a
running job's checkpoint and reloads it as the calculation progresses.

From the viewer you can also submit the structure on screen: set the method,
basis, charge and multiplicity, then **Submit to vq cluster**. With
**Submit as QVF container** on, the payload is a pending QVF container that
the queue runs in place. The viewer picks the vibe-qc program registered with
vq. Set `VIBEVIEW_VQ_PROGRAM` to choose one when several are registered.

From the command line:

```sh
vibe-view from-vq <jobid>                  # fetch the job and open its .qvf files
vibe-view from-vq <jobid1> <jobid2> -o results/
vibe-view vq-features                      # which visualizations need what from vibe-qc
```

`from-vq` calls `vq fetch`, so it reaches whatever host your vq config
points at.

## vibe-view as a queue job: figures without a screen

vibe-view can render images headlessly, which makes it a program like any
other. Register it on the host that renders, with a health check that proves
capture works:

```toml
[programs.vibeview-dev]
kind = "venv"
python = "/home/USER/vibe-view/.venv/bin/python"
git_dir = "/home/USER/vibe-view"
branch = "main"
import_check = "vibeview"
healthcheck_command = "xvfb-run -a vibe-view capture-selftest"
description = "vibe-view headless capture"
```

Then submit a capture job next to the result it renders:

```sh
vq submit workstation --program vibeview-dev -d ./results/<jobid> -- \
    sh -c 'xvfb-run -a "$VQ_PROGRAM_BIN/vibe-view" capture water.qvf -o water.png'
vq fetch workstation <capture-jobid> --name water.png -o figures/
```

`vq programs workstation` should report `vibeview-dev` as `OK` first. In a
[campaign](../campaigns.md), the same command is a render node that runs
only after the calculation's results have been verified.
