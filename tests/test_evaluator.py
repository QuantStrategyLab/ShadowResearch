"""Unit tests for C-FUSE-EVAL synthetic stub evaluator."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator
from shadow.sizing import policy_config_hash


def _valid_inputs() -> dict:
    return {
        "E": "50000",
        "E_star": "50000",
        "H": "0",
        "L": "0",
        "funds": {
            "TQQQ": {"s": "1", "close_price": "50", "q_pre": 0, "sigma_forecast": "0.05"},
            "SOXL": {"s": "1", "close_price": "40", "q_pre": 0, "sigma_forecast": "0.05"},
        },
    }


class EvaluatorStubTest(unittest.TestCase):
    """Verify exact 24 breaker IDs, O1 fail-closed, and zero ledger access."""

    def test_exact_24_breaker_ids_returned(self) -> None:
        ev = StubEvaluator()
        rows, scope, nxt = ev.evaluate(
            spec_version=ev.spec_version,
            engine_hash=ev.engine_hash,
            policy_hash=policy_config_hash(),
            session="2025-11-21",
            as_of="2025-11-21T21:00:00.000Z",
            mode="drill",
            validated_inputs=_valid_inputs(),
            prior_row_state={},
        )
        expected_ids = {
            "L0", "L1", "L2", "L3", "L4", "L5", "L6",
            "V1", "V2", "V3", "V4", "V5", "V6", "V7",
            "O1", "O2", "O3", "O4", "O5", "O6", "O7", "O8", "O9",
            "J1",
        }
        self.assertEqual(len(ALL_BREAKER_IDS), 24)
        self.assertEqual(set(rows.keys()), expected_ids)
        self.assertEqual(set(nxt.keys()), expected_ids)
        self.assertEqual(scope["aggregate"], "NORMAL")

    def test_o1_fail_closed_on_missing_soxl_close(self) -> None:
        ev = StubEvaluator()
        bad = _valid_inputs()
        bad["funds"]["SOXL"]["close_price"] = None
        rows, scope, _ = ev.evaluate(
            spec_version=ev.spec_version,
            engine_hash=ev.engine_hash,
            policy_hash=policy_config_hash(),
            session="2025-11-21",
            as_of="2025-11-21T21:00:00.000Z",
            mode="drill",
            validated_inputs=bad,
            prior_row_state={},
        )
        self.assertEqual(rows["O1"]["state"], "NO-NEW-RISK")
        self.assertTrue(rows["O1"]["would_have_tripped"])
        self.assertIn("SOXL_close", rows["O1"]["reason"])
        self.assertEqual(scope["aggregate"], "NO-NEW-RISK")

    def test_stub_structurally_has_no_ledger_handles(self) -> None:
        ev = StubEvaluator()
        self.assertFalse(hasattr(ev, "ledger"))
        self.assertFalse(hasattr(ev, "append_record"))
        self.assertFalse(hasattr(ev, "root_dir"))


if __name__ == "__main__":
    unittest.main()
