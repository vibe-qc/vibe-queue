"""Entry point for `python -m vq`.

Used by `vq daemon start` to spawn the daemon subprocess in a way that doesn't
depend on the `vq` script being on PATH (it always uses the same interpreter
the parent process was running in).
"""
from vq.cli import main

if __name__ == "__main__":
    main()
