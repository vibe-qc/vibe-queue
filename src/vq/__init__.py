__version__ = "0.26.27"

# The distribution's name, as `pyproject.toml` declares it (#68). The command
# and this package are `vq`; only what an index or a requirement resolves is
# `vibe-queue`. `tests/test_distribution_name.py` keeps the three spellings --
# pyproject, this constant and the shell installers' glob -- in agreement.
DISTRIBUTION_NAME = "vibe-queue"
# `vq` was the distribution's name through v0.26.9. A venv last installed by
# one of those releases still carries a PEP 610 record under it until the next
# install removes it, so readers fall back to this name when no current one
# exists.
LEGACY_DISTRIBUTION_NAME = "vq"
