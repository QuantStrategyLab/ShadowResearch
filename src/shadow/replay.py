"""Deterministic replay and verification CLI (skeleton)."""

from __future__ import annotations


def replay_window(bundle_dir: str, ledger_dir: str) -> dict:
    """Re-derive ledger records from retained inputs and compare bit-for-bit."""
    raise NotImplementedError("replay not implemented")


def main() -> int:
    """CLI entrypoint for replay."""
    raise NotImplementedError("replay not implemented")


if __name__ == "__main__":
    raise SystemExit(main())
