"""C-SH-LEDGER canonicalization, hash chain, and detached receipts (skeleton)."""

from __future__ import annotations


def canonical_json_bytes(obj: object) -> bytes:
    """Serialize obj per restricted RFC 8785 profile."""
    raise NotImplementedError("ledger not implemented")


class ShadowLedger:
    """Append-only SHA-256 hash-chained ledger."""

    def __init__(self, root_dir: str) -> None:
        self.root_dir = root_dir

    def append_record(self, record_body: dict, receipt: dict) -> dict:
        """Append one record and detached receipt."""
        raise NotImplementedError("ledger not implemented")

    def verify_chain(self) -> tuple[bool, int | None, str | None]:
        """Verify chain integrity and return (ok, first_bad_sequence, error)."""
        raise NotImplementedError("ledger not implemented")
