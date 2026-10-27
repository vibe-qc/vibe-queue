"""#160/#168: `vq broker capacity` reports a daemon's GPUs and its free ones.

The figures come from the daemon's own admission snapshot: `gpus` is how many
devices it may hand out (0 when it lists none) and `free_gpus` is those not
assigned now. A snapshot from a daemon that predates GPU admission makes both
unknown (null), never zero. The broker-side conversion is unchanged.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from vq import broker_capacity, config
from vq.cli import main

sys.path.insert(0, str(Path(__file__).parent))
from test_broker_capacity_64 import _admission, _sample, host  # noqa: E402,F401


@pytest.mark.parametrize(("changes", "expected"), [
    ({"gpu_devices": ["0", "1", "2"], "used_gpus": 1}, (3, 2)),
    ({"gpu_devices": ["0"], "used_gpus": 1}, (1, 0)),
    ({"gpu_devices": None, "used_gpus": 0}, (0, 0)),  # a daemon with none
    ({}, (None, None)),  # a daemon from before GPU admission
    ({"gpu_devices": ["0"], "used_gpus": 2}, (None, None)),  # inconsistent
    ({"gpu_devices": ["0"], "used_gpus": -1}, (None, None)),
    ({"gpu_devices": ["0"]}, (None, None)),
    ({"gpu_devices": "0,1", "used_gpus": 0}, (None, None)),
    ({"gpu_devices": ["0"], "used_gpus": True}, (None, None)),
])
def test_the_gpu_figures_come_from_the_daemons_snapshot(
    host: Any, changes: dict[str, Any], expected: tuple[int | None, int | None],  # noqa: F811
) -> None:
    host.admission = _admission(**changes)
    payload = _sample()
    assert (payload["gpus"], payload["free_gpus"]) == expected
    json.dumps(payload)


def test_no_snapshot_means_unknown_gpus(host: Any) -> None:  # noqa: F811
    host.rpc_error = ConnectionError("no daemon")
    payload = _sample()
    assert (payload["gpus"], payload["free_gpus"]) == (None, None)


def test_the_broker_side_conversion_ignores_them(host: Any) -> None:  # noqa: F811
    host.admission = _admission(gpu_devices=["0", "1"], used_gpus=0)
    with_gpus = broker_capacity.evidence_from_payload("alpha", _sample(), observed_at=1_000.0)
    host.admission = _admission()
    without = broker_capacity.evidence_from_payload("alpha", _sample(), observed_at=1_000.0)
    assert with_gpus.error is None and with_gpus == without


def test_the_text_form_shows_them(host: Any, monkeypatch: pytest.MonkeyPatch) -> None:  # noqa: F811
    import time

    monkeypatch.setattr(config, "load_config", lambda: config.Config())
    host.admission = _admission(published_at=time.time(), gpu_devices=["0", "1"], used_gpus=1)
    text = CliRunner().invoke(main, ["broker", "capacity"])
    assert text.exit_code == 0, text.output
    assert "gpus=1 (of 2)" in text.output
