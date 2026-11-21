"""C-SA-22 XNYS sessions calendar consumer (skeleton)."""

from __future__ import annotations


def load_spec(spec_dir: str) -> dict:
    """Load and verify the C-SA-22 specification package by digest."""
    raise NotImplementedError("calendar_consumer not implemented")


def resolve_session(venue: str, trade_date: str, spec: dict | None = None) -> dict:
    """Resolve session facts for (venue, trade_date) per C-SA-22."""
    raise NotImplementedError("calendar_consumer not implemented")
