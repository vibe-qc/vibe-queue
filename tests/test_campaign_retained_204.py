"""Frozen campaign commands request authenticated retained generations explicitly."""

import copy
import subprocess

import pytest

from vq.campaign import Node
from vq.campaign_controller import CliQueue
from vq.runtime_admission import AdmissionError, ReservationStore, validate_identity

SHA = "a" * 40


def test_controller_keeps_command_key_and_default_behavior(tmp_path, monkeypatch):
    command = [f"/launchers/calc-1.0-{SHA}-python", "stage.py"]
    node = Node(
        id="check", kind="validate", host="cluster", program="calc", cpus=1, command=command
    )
    calls = []

    def run(self, argv, timeout):
        calls.append(argv)
        return subprocess.CompletedProcess(argv, 0, '{"jobids":["abc"],"host":"cluster"}', "")

    monkeypatch.setattr(CliQueue, "_run", run)
    CliQueue().submit(node, tmp_path, "same-key", "same-name")
    CliQueue(retained_runtimes=True).submit(node, tmp_path, "same-key", "same-name")
    assert "--managed-generation" not in calls[0]
    assert calls[1][calls[1].index("--managed-generation") + 1] == SHA
    assert calls[1][-2:] == command
    assert calls[1][calls[1].index("--idempotency-key") + 1] == "same-key"


def test_packed_reservation_binds_native_archive_and_launcher(tmp_path):
    generation = tmp_path / "releases" / SHA
    identity = dict(
        layout="packed-v1",
        program="calc",
        source_sha=SHA,
        generation=str(generation),
        source_root=str(generation),
        python=f"/launchers/calc-1.0-{SHA}-python",
        native_sha256="1" * 64,
        content_sha256="2" * 64,
        archive_sha256="3" * 64,
        launcher_sha256="4" * 64,
        healthy=True,
    )
    assert validate_identity(identity, "calc", SHA) == identity
    directory = tmp_path / "authority"
    directory.mkdir(mode=0o700)
    store = ReservationStore(directory, lambda *_: copy.deepcopy(identity))
    args = dict(
        key="5" * 64, owner="6" * 64, intent="7" * 64, program="calc", source=SHA, identity=identity
    )
    with store.transaction():
        row = store.reserve(**args)
        assert store.reserve(**args) == row
        for field in ("native_sha256", "content_sha256", "archive_sha256", "launcher_sha256"):
            bad = dict(identity, **{field: "8" * 64})
            with pytest.raises(AdmissionError):
                store.reserve(**dict(args, identity=bad))
        assert store.protects(str(generation)) == ["5" * 64]
    for change in (
        {"python": "/arbitrary/python"},
        {"source_root": "/outside"},
        {"archive_sha256": "bad"},
        {"healthy": False},
    ):
        with pytest.raises(AdmissionError):
            validate_identity(dict(identity, **change), "calc", SHA)
