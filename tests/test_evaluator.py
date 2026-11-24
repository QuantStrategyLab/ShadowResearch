"""Unit tests for C-FUSE-EVAL synthetic stub evaluator."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.evaluator import (
    ALL_BREAKER_IDS,
    REQUIRED_MARKET_SERIES,
    StubEvaluator,
    StubProductionForbiddenError,
)
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
        self.assertFalse(ev.production_clock_capable)

    def test_stub_refuses_production_mode_structurally(self) -> None:
        ev = StubEvaluator()
        with self.assertRaises(StubProductionForbiddenError):
            ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=policy_config_hash(),
                session="2025-11-21",
                as_of="2025-11-21T21:00:00.000Z",
                mode="production",
                validated_inputs=_valid_inputs(),
                prior_row_state={},
            )

    def test_o1_fail_closed_on_each_withheld_or_ineligible_required_series(self) -> None:
        ev = StubEvaluator()
        spec = load_spec()
        cal = resolve_session("XNYS", "2025-11-21", spec=spec)
        complete_records = [
            {
                "instrument": inst,
                "calendar_spec_digest": cal["spec_digest"],
                "normalized_value": "100",
                "eligibility": {"eligible": True, "reason": None},
            }
            for inst in REQUIRED_MARKET_SERIES
        ]

        # Complete set passes O1/O2 cleanly
        rows_ok, scope_ok, _ = ev.evaluate(
            spec_version=ev.spec_version,
            engine_hash=ev.engine_hash,
            policy_hash=policy_config_hash(),
            session="2025-11-21",
            as_of="2025-11-21T21:00:00.000Z",
            mode="drill",
            validated_inputs={**_valid_inputs(), "calendar": cal, "market_records": complete_records},
            prior_row_state={},
        )
        self.assertEqual(scope_ok["aggregate"], "NORMAL")
        self.assertFalse(rows_ok["O1"]["would_have_tripped"])

        # Withholding each required series trips O1 NO-NEW-RISK
        for missing_inst in REQUIRED_MARKET_SERIES:
            partial = [
                (
                    r
                    if r["instrument"] != missing_inst
                    else {
                        "instrument": missing_inst,
                        "calendar_spec_digest": cal["spec_digest"],
                        "normalized_value": None,
                        "eligibility": {"eligible": False, "reason": f"missing:{missing_inst}"},
                    }
                )
                for r in complete_records
            ]
            rows, scope, _ = ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=policy_config_hash(),
                session="2025-11-21",
                as_of="2025-11-21T21:00:00.000Z",
                mode="drill",
                validated_inputs={**_valid_inputs(), "calendar": cal, "market_records": partial},
                prior_row_state={},
            )
            self.assertEqual(rows["O1"]["state"], "NO-NEW-RISK", missing_inst)
            self.assertTrue(rows["O1"]["would_have_tripped"], missing_inst)
            self.assertIn(missing_inst, rows["O1"]["reason"])
            self.assertEqual(scope["aggregate"], "NO-NEW-RISK")

    def test_o2_fail_closed_on_closed_unscheduled_unknown_and_disagreement(self) -> None:
        ev = StubEvaluator()
        spec = load_spec()
        bad_cals = [
            ("2025-11-27", resolve_session("XNYS", "2025-11-27", spec=spec)),
            ("2025-01-09", resolve_session("XNYS", "2025-01-09", spec=spec)),
            ("2025-11-21", resolve_session("XHKG", "2025-11-21", spec=spec)),
            ("2026-01-01", resolve_session("XNYS", "2026-01-01", spec=spec)),
            (
                "2025-11-21",
                resolve_session("XNYS", "2025-11-21", spec=spec, observed_venue_status="closed"),
            ),
        ]
        for sess, cal in bad_cals:
            rows, scope, _ = ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=policy_config_hash(),
                session=sess,
                as_of=f"{sess}T21:00:00.000Z",
                mode="drill",
                validated_inputs={**_valid_inputs(), "calendar": cal},
                prior_row_state={},
            )
            self.assertEqual(rows["O2"]["state"], "NO-NEW-RISK", cal)
            self.assertTrue(rows["O2"]["would_have_tripped"], cal)
            self.assertEqual(scope["aggregate"], "NO-NEW-RISK")

    def test_stub_override_cannot_clear_mandatory_o1_or_o2(self) -> None:
        # Finding #8 regression: stub_breaker_overrides may not clear mandatory O1/O2 fail-closed blocks
        ev = StubEvaluator()
        bad_o1 = _valid_inputs()
        bad_o1["funds"]["SOXL"]["close_price"] = None
        bad_o1["stub_breaker_overrides"] = {"O1": {"state": "NORMAL", "reason": "forced_bypass"}}
        with self.assertRaises(ValueError) as ctx_o1:
            ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=policy_config_hash(),
                session="2025-11-21",
                as_of="2025-11-21T21:00:00.000Z",
                mode="drill",
                validated_inputs=bad_o1,
                prior_row_state={},
            )
        self.assertIn("stub_override_cannot_clear_mandatory_breaker:O1", str(ctx_o1.exception))

        spec = load_spec()
        closed_cal = resolve_session("XNYS", "2025-11-27", spec=spec)
        bad_o2 = {
            **_valid_inputs(),
            "calendar": closed_cal,
            "stub_breaker_overrides": {"O2": {"state": "NORMAL", "reason": "forced_bypass"}},
        }
        with self.assertRaises(ValueError) as ctx_o2:
            ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=policy_config_hash(),
                session="2025-11-27",
                as_of="2025-11-27T21:00:00.000Z",
                mode="drill",
                validated_inputs=bad_o2,
                prior_row_state={},
            )
        self.assertIn("stub_override_cannot_clear_mandatory_breaker:O2", str(ctx_o2.exception))

        # Non-mandatory override (e.g. L1) still works when O1/O2 are clean
        ok_override = {
            **_valid_inputs(),
            "stub_breaker_overrides": {"L1": {"state": "DE-RISK", "reason": "fixture_test"}},
        }
        rows_ok, scope_ok, _ = ev.evaluate(
            spec_version=ev.spec_version,
            engine_hash=ev.engine_hash,
            policy_hash=policy_config_hash(),
            session="2025-11-21",
            as_of="2025-11-21T21:00:00.000Z",
            mode="drill",
            validated_inputs=ok_override,
            prior_row_state={},
        )
        self.assertEqual(rows_ok["L1"]["state"], "DE-RISK")
        self.assertEqual(scope_ok["aggregate"], "DE-RISK")


if __name__ == "__main__":
    unittest.main()
