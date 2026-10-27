`vq submit --help` now describes how `--mem-mb` is enforced: by cgroups or the watchdog on Linux, by the scheduler on PBS and Slurm, and not at all on macOS.
