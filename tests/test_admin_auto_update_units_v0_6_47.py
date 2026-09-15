"""The legacy privileged root auto-update templates are retired safely."""

from __future__ import annotations

from pathlib import Path

CONTRIB = Path(__file__).resolve().parent.parent / "contrib"
SERVICE = CONTRIB / "vq-admin-auto-update@.service"
TIMER = CONTRIB / "vq-admin-auto-update@.timer"


def test_service_tombstone_executes_no_vq_or_checkout_code() -> None:
    body = SERVICE.read_text(encoding="utf-8")

    assert "ExecStart=/usr/bin/false" in body
    assert "admin auto-update" not in body
    assert "/home/" not in body
    assert "User=root" in body
    assert "RETIRED" in body


def test_timer_tombstone_has_no_schedule_or_install_target() -> None:
    body = TIMER.read_text(encoding="utf-8")

    assert "Unit=vq-admin-auto-update@%i.service" in body
    assert "OnCalendar=" not in body
    assert "OnUnitActiveSec=" not in body
    assert "WantedBy=" not in body
    assert "[Install]" not in body
    assert "RETIRED" in body
