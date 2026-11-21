"""SH-1 §7 Design Demonstration runner (D1-D8)."""

from __future__ import annotations

import sys

DEMO_CASES = [
    ("D1", "Exact 3-tier sizing outputs (6-session synthetic fixture)"),
    ("D2", "Split invariance (2:1) and half-day invariance"),
    ("D3", "Standing Stage N assertions (kelly_applied=false, orders_placed=0)"),
    ("D4", "Bit-for-bit replay of D1 fixture window"),
    ("D5", "Hash-chain tamper rejection (mutation, fork, gap, duplicate)"),
    ("D6", "Y2-overlap 10-session reconstruction & forward-credit rejection"),
    ("D7", "O1 fail-closed output on withheld SOXL close"),
    ("D8", "C-SA-22 calendar vectors (2025 full year + error cases)"),
]


def main() -> int:
    """Execute D1-D8 and exit non-zero on any failure."""
    failures = 0
    for case_id, desc in DEMO_CASES:
        print(f"{case_id} FAIL (not implemented): {desc}")
        failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
