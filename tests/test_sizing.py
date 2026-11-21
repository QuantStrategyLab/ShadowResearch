"""Unit tests for frozen policy_v0_pre 10-step sizing stack."""

from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.sizing import (
    SizingInputError,
    canonical_decimal_str,
    compute_session_sizing,
    quantize_6dp,
)


def _make_inputs(
    E: str = "50000",
    E_star: str = "50000",
    H: str = "0",
    L: str = "0",
    s_tqqq: str = "1",
    s_soxl: str = "1",
    p_tqqq: str = "50",
    p_soxl: str = "40",
    q_pre_tqqq: object = 0,
    q_pre_soxl: object = 0,
    sig_tqqq: str = "0.05",
    sig_soxl: str = "0.05",
) -> dict:
    return {
        "E": E,
        "E_star": E_star,
        "H": H,
        "L": L,
        "funds": {
            "TQQQ": {
                "s": s_tqqq,
                "close_price": p_tqqq,
                "q_pre": q_pre_tqqq,
                "sigma_forecast": sig_tqqq,
            },
            "SOXL": {
                "s": s_soxl,
                "close_price": p_soxl,
                "q_pre": q_pre_soxl,
                "sigma_forecast": sig_soxl,
            },
        },
    }


def _tier(res: dict, tid: str) -> dict:
    return next(t for t in res["tiers"] if t["tier_id"] == tid)


def _fund(tier_obj: dict, inst: str) -> dict:
    return next(f for f in tier_obj["funds"] if f["instrument"] == inst)


class SizingStackTest(unittest.TestCase):
    """Verify positive controls, 10-step stack, H>0 w_max, band & mandatory bypass."""

    def test_positive_control_full_cushion_m0_and_kelly_struct(self) -> None:
        res = compute_session_sizing(_make_inputs())
        expected_m0_2dp = {
            "conservative": Decimal("0.09"),
            "balanced": Decimal("0.20"),
            "aggressive": Decimal("0.39"),
        }
        expected_k_2dp = {
            "conservative": Decimal("0.11"),
            "balanced": Decimal("0.23"),
            "aggressive": Decimal("0.46"),
        }
        for tid in ("conservative", "balanced", "aggressive"):
            t = _tier(res, tid)
            self.assertEqual(Decimal(t["x_ML"]).quantize(Decimal("0.01")), expected_m0_2dp[tid])
            self.assertEqual(t["x_ML_binding_subbound"], "c")
            tqqq = _fund(t, "TQQQ")
            self.assertEqual(
                Decimal(tqqq["x_K_struct"]).quantize(Decimal("0.01")),
                expected_k_2dp[tid],
            )
            self.assertFalse(tqqq["kelly_applied"])
            self.assertEqual(t["orders_placed"], 0)

    def test_positive_control_balanced_cushion_table(self) -> None:
        cases = [
            ("50000", "0", Decimal("0.300"), Decimal("0.20")),
            ("45000", "5000", Decimal("0.222"), Decimal("0.14")),
            ("40000", "10000", Decimal("0.125"), Decimal("0.08")),
            ("37500", "12500", Decimal("0.067"), Decimal("0.04")),
        ]
        for eq, loss, exp_c_3dp, exp_xml_2dp in cases:
            res = compute_session_sizing(_make_inputs(E=eq, L=loss))
            bal = _tier(res, "balanced")
            self.assertEqual(Decimal(bal["c"]).quantize(Decimal("0.001")), exp_c_3dp)
            self.assertEqual(Decimal(bal["x_ML"]).quantize(Decimal("0.01")), exp_xml_2dp)

    def test_exhausted_cushion_yields_zero_exposure(self) -> None:
        res = compute_session_sizing(_make_inputs(E="42500", L="7500"))
        cons = _tier(res, "conservative")
        self.assertEqual(cons["c"], "0")
        self.assertEqual(cons["x_ML"], "0")
        for inst in ("TQQQ", "SOXL"):
            f = _fund(cons, inst)
            self.assertEqual(f["w_target"], "0")
            self.assertEqual(f["q_ideal"], 0)

    def test_h_positive_combined_w_max_ceiling_binds(self) -> None:
        # With H = 5 * E* (1 + H/E* = 6), aggressive raw sum w_target ~ 0.717 > w_max = 0.60
        res = compute_session_sizing(_make_inputs(H="250000"))
        agg = _tier(res, "aggressive")
        self.assertTrue(agg["w_max_bound"])
        w_sum = Decimal(_fund(agg, "TQQQ")["w_target"]) + Decimal(_fund(agg, "SOXL")["w_target"])
        self.assertEqual(quantize_6dp(w_sum), Decimal("0.600000"))
        self.assertEqual(_fund(agg, "TQQQ")["binding_constraint"], "w_max")

    def test_no_trade_band_suppresses_discretionary_rebalance_inclusive(self) -> None:
        # Balanced TQQQ w_target at full cushion ~ 0.05032218, q_ideal = 50.
        # Set q_pre = 45 -> w_cur = 45*50/50000 = 0.045; |0.045 - 0.050322| = 0.005322 <= 0.20*0.050322 = 0.010064
        res = compute_session_sizing(_make_inputs(q_pre_tqqq=45, q_pre_soxl=13))
        bal = _tier(res, "balanced")
        tqqq = _fund(bal, "TQQQ")
        self.assertEqual(tqqq["q_ideal"], 50)
        self.assertTrue(tqqq["no_trade_band_suppressed"])
        self.assertEqual(tqqq["delta_q_exec"], 0)
        self.assertFalse(tqqq["emergency_bypass_triggered"])

        # Exact inclusive <= boundary check: w_target = 0.05 (if s or exact ratio), construct exact 20% diff
        # Suppose E = 50000, P = 50, q_pre = 40 -> w_cur = 0.040. If w_target <= 0.05, |0.04-0.05|=0.01 == 0.20*0.05
        res_b = compute_session_sizing(_make_inputs(q_pre_tqqq=41, q_pre_soxl=13))
        tqqq_b = _fund(_tier(res_b, "balanced"), "TQQQ")
        self.assertTrue(tqqq_b["no_trade_band_suppressed"])

    def test_mandatory_cap_reduction_bypasses_no_trade_band(self) -> None:
        # Balanced at 10% drawdown: x_ML = 0.141829, w_target_TQQQ = 0.035457, q_ideal = 31 (at E=45000, P=50).
        # Hold q_pre = 35 -> w_cur = 35*50/45000 = 0.038889 (within 9.7% <= 20% of 0.035457!),
        # BUT x_cur = 3 * 0.038889 = 0.116667 while joint_cur = 0.116667 + 1.5*(3*0.008889) = 0.156667 > 0.141829!
        # Or make x_cur_TQQQ = 3 * (45*50/45000) = 0.15 > 0.141829:
        # Wait: 43*50/45000 = 0.047778 -> |0.047778-0.035457|/0.035457 = 34% > 20%.
        # With q_pre_tqqq = 36, q_pre_soxl = 9 (E=45000, P_soxl=40):
        # w_cur_TQQQ = 36*50/45000 = 0.04 (within 12.8% <= 20% of 0.035457),
        # w_cur_SOXL = 9*40/45000 = 0.008, joint x_cur = 3*(0.04 + 1.5*0.008) = 0.156 > 0.141829 (Step 7 joint cap!)
        res = compute_session_sizing(_make_inputs(E="45000", L="5000", q_pre_tqqq=36, q_pre_soxl=9))
        tqqq = _fund(_tier(res, "balanced"), "TQQQ")
        self.assertEqual(tqqq["q_ideal"], 31)
        self.assertFalse(tqqq["no_trade_band_suppressed"])
        self.assertEqual(tqqq["delta_q_exec"], -5)
        self.assertTrue(tqqq["emergency_bypass_triggered"])

    def test_canonical_decimal_str_formatting(self) -> None:
        self.assertEqual(canonical_decimal_str(Decimal("0.0000")), "0")
        self.assertEqual(canonical_decimal_str(Decimal("-0.00")), "0")
        self.assertEqual(canonical_decimal_str(Decimal("50000.00")), "50000")
        self.assertEqual(canonical_decimal_str(Decimal("100")), "100")
        self.assertEqual(canonical_decimal_str(Decimal("0.2000")), "0.2")
        with self.assertRaises(ValueError):
            canonical_decimal_str(Decimal("NaN"))

    def test_missing_input_raises_sizing_error(self) -> None:
        bad = _make_inputs()
        bad["funds"]["SOXL"]["close_price"] = None
        with self.assertRaises(SizingInputError) as ctx:
            compute_session_sizing(bad)
        self.assertEqual(ctx.exception.field, "SOXL_close")

    def test_kelly_would_have_bound_follows_counterfactual_vs_x_pre(self) -> None:
        # S4 high-vol case: x_K_cond < min(x_cap, x_ML) across all 6 rows, so
        # kelly_counterfactual_x < x_pre holds even when x_K_cond >= x_pre.
        s4_res = compute_session_sizing(_make_inputs(sig_tqqq="0.72", sig_soxl="0.80"))
        for tid in ("conservative", "balanced", "aggressive"):
            t = _tier(s4_res, tid)
            for inst in ("TQQQ", "SOXL"):
                f = _fund(t, inst)
                self.assertTrue(f["kelly_would_have_bound"], f"{tid}.{inst}")
                self.assertLess(Decimal(f["kelly_counterfactual_x"]), Decimal(f["x_pre"]))
                self.assertFalse(f["kelly_applied"])

        # S1 low-vol case: x_K_cond > x_ML so cf == x_pre -> equality yields False.
        s1_res = compute_session_sizing(_make_inputs())
        for tid in ("conservative", "balanced", "aggressive"):
            t = _tier(s1_res, tid)
            for inst in ("TQQQ", "SOXL"):
                f = _fund(t, inst)
                self.assertFalse(f["kelly_would_have_bound"], f"{tid}.{inst}")
                self.assertEqual(Decimal(f["kelly_counterfactual_x"]), Decimal(f["x_pre"]))

        # Zero signal (g == 0) and exhausted cushion (A == 0) yield False even at high vol.
        zero_sig = compute_session_sizing(
            _make_inputs(s_tqqq="0", s_soxl="0", sig_tqqq="0.72", sig_soxl="0.80")
        )
        for tid in ("conservative", "balanced", "aggressive"):
            t = _tier(zero_sig, tid)
            for inst in ("TQQQ", "SOXL"):
                f = _fund(t, inst)
                self.assertEqual(f["x_pre"], "0")
                self.assertEqual(f["kelly_counterfactual_x"], "0")
                self.assertFalse(f["kelly_would_have_bound"])

        exhausted = compute_session_sizing(
            _make_inputs(E="42500", L="7500", sig_tqqq="0.72", sig_soxl="0.80")
        )
        cons_ex = _tier(exhausted, "conservative")
        for inst in ("TQQQ", "SOXL"):
            f = _fund(cons_ex, inst)
            self.assertEqual(f["x_pre"], "0")
            self.assertEqual(f["kelly_counterfactual_x"], "0")
            self.assertFalse(f["kelly_would_have_bound"])


if __name__ == "__main__":
    unittest.main()
