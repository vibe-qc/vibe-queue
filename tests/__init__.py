# Marker so cross-test imports (e.g. `from tests.test_scheduler_dispatch import ...`)
# work regardless of the working directory pytest is invoked from.
# The conftest + per-file sys.path manipulation puts the parent of this
# directory on sys.path; the `__init__.py` makes `tests` a real package.
