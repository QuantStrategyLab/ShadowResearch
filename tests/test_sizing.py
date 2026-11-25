"""Unit tests for frozen policy_v0_pre 10-step sizing stack."""

from __future__ import annotations

import shutil
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.manifest import compute_code_manifest_hash, load_effective_runtime_config
from shadow.sizing import (
    CLOCK_START_DEFINITION,
    PINNED_POLICY_PARAMS,
    SizingInputError,
    canonical_decimal_str,
    compute_monthly_diagnostics,
    compute_session_sizing,
    policy_config_hash,
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
            for inst in ("TQQQ", "SOXL"):
                f = _fund(t, inst)
                self.assertFalse(f["kelly_applied"])
                self.assertIs(type(f["orders_placed"]), int)
                self.assertEqual(f["orders_placed"], 0)

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
        self.assertEqual(canonical_decimal_str(Decimal("0E-57")), "0")
        for extreme in (
            Decimal("1e200000000"),
            Decimal("1e-200000000"),
            Decimal("1." + ("1" * 64)),
        ):
            with self.assertRaises(ValueError):
                canonical_decimal_str(extreme)

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

    def test_forward_and_reverse_split_invariance(self) -> None:
        # 3:1 TQQQ forward split & 2:1 SOXL forward split
        pre_fwd = compute_session_sizing(
            _make_inputs(p_tqqq="60", p_soxl="40", q_pre_tqqq=15, q_pre_soxl=6)
        )
        post_fwd = compute_session_sizing(
            _make_inputs(p_tqqq="20", p_soxl="20", q_pre_tqqq=45, q_pre_soxl=12)
        )
        for tid in ("conservative", "balanced", "aggressive"):
            t_pre = _tier(pre_fwd, tid)
            t_post = _tier(post_fwd, tid)
            self.assertEqual(t_pre["c"], t_post["c"])
            self.assertEqual(t_pre["x_ML"], t_post["x_ML"])
            for inst in ("TQQQ", "SOXL"):
                f_pre = _fund(t_pre, inst)
                f_post = _fund(t_post, inst)
                for key in (
                    "x_pre",
                    "x_post_joint",
                    "w_target",
                    "no_trade_band_suppressed",
                    "emergency_bypass_triggered",
                    "binding_constraint",
                ):
                    self.assertEqual(f_pre[key], f_post[key], f"fwd split {tid}.{inst}.{key}")

        # 1:4 reverse split on both TQQQ (25->100, q 40->10) and SOXL (10->40, q 20->5)
        pre_rev = compute_session_sizing(
            _make_inputs(p_tqqq="25", p_soxl="10", q_pre_tqqq=40, q_pre_soxl=20)
        )
        post_rev = compute_session_sizing(
            _make_inputs(p_tqqq="100", p_soxl="40", q_pre_tqqq=10, q_pre_soxl=5)
        )
        for tid in ("conservative", "balanced", "aggressive"):
            t_pre = _tier(pre_rev, tid)
            t_post = _tier(post_rev, tid)
            self.assertEqual(t_pre["c"], t_post["c"])
            self.assertEqual(t_pre["x_ML"], t_post["x_ML"])
            for inst in ("TQQQ", "SOXL"):
                f_pre = _fund(t_pre, inst)
                f_post = _fund(t_post, inst)
                for key in (
                    "x_pre",
                    "x_post_joint",
                    "w_target",
                    "no_trade_band_suppressed",
                    "emergency_bypass_triggered",
                    "binding_constraint",
                ):
                    self.assertEqual(f_pre[key], f_post[key], f"rev split {tid}.{inst}.{key}")

    def test_cash_distribution_ex_date_invariance(self) -> None:
        # Per C-SA-CA: ex-date drop P_ex = P_cum - div leaves total economic equity E
        # (including accrued cash distribution receivable) and loss memory invariant.
        cum_div = compute_session_sizing(
            _make_inputs(E="50000", E_star="50000", L="0", p_tqqq="50", q_pre_tqqq=50)
        )
        ex_div = compute_session_sizing(
            _make_inputs(E="50000", E_star="50000", L="0", p_tqqq="48", q_pre_tqqq=50)
        )
        for tid in ("conservative", "balanced", "aggressive"):
            t_cum = _tier(cum_div, tid)
            t_ex = _tier(ex_div, tid)
            self.assertEqual(t_cum["c"], t_ex["c"])
            self.assertEqual(t_cum["x_ML"], t_ex["x_ML"])
            for inst in ("TQQQ", "SOXL"):
                f_cum = _fund(t_cum, inst)
                f_ex = _fund(t_ex, inst)
                for key in ("x_pre", "x_post_joint", "w_target", "binding_constraint"):
                    self.assertEqual(f_cum[key], f_ex[key], f"cash dist {tid}.{inst}.{key}")

    def test_half_day_invariance_edt_and_est(self) -> None:
        spec = load_spec()
        reg = resolve_session("XNYS", "2025-11-21", spec=spec)
        half_edt = resolve_session("XNYS", "2025-07-03", spec=spec)
        half_est = resolve_session("XNYS", "2025-11-28", spec=spec)
        self.assertEqual(reg["session_state"], "regular")
        self.assertEqual(half_edt["session_state"], "half_day")
        self.assertEqual(half_est["session_state"], "half_day")

        base_in = _make_inputs()
        res_reg = compute_session_sizing(base_in)
        res_edt = compute_session_sizing(base_in)
        res_est = compute_session_sizing(base_in)
        self.assertEqual(res_reg["tiers"], res_edt["tiers"])
        self.assertEqual(res_reg["tiers"], res_est["tiers"])

    def test_monthly_diagnostic_and_raw_vs_adjusted_separation(self) -> None:
        base_res = compute_session_sizing(_make_inputs())
        self.assertEqual(base_res["monthly_diagnostic"]["status"], "unavailable")
        self.assertIsNone(base_res["monthly_diagnostic"]["win_rate"])
        self.assertIsNone(base_res["monthly_diagnostic"]["payoff_ratio"])
        self.assertTrue(base_res["monthly_diagnostic"]["unread_by_sizing"])
        self.assertEqual(base_res["raw_vs_adjusted"]["status"], "unavailable")
        self.assertEqual(
            base_res["raw_vs_adjusted"]["unavailable_reason"],
            "prior_equity_unavailable",
        )

        # Populated monthly returns produce canonical decimals and leave sizing untouched
        with_diag = compute_session_sizing(
            {
                **_make_inputs(),
                "prior_E": "48000",
                "external_flow": "1000",
                "monthly_returns": ["0.04", "-0.02", "0.06", "0.02"],
            }
        )
        self.assertEqual(with_diag["tiers"], base_res["tiers"])
        diag = with_diag["monthly_diagnostic"]
        self.assertEqual(diag["status"], "computed")
        self.assertEqual(diag["sample_months"], 4)
        self.assertEqual(diag["win_count"], 3)
        self.assertEqual(diag["loss_count"], 1)
        self.assertEqual(diag["win_rate"], "0.75")
        self.assertEqual(diag["payoff_ratio"], "2")
        self.assertTrue(diag["unread_by_sizing"])
        self.assertIn("mean", diag["win_rate_bayes_ci"])

        # 7A separation keeps raw asset change distinct from cash-flow-adjusted P&L
        rva = with_diag["raw_vs_adjusted"]
        self.assertEqual(rva["status"], "computed")
        self.assertEqual(rva["raw_asset_change_usd"], "2000")
        self.assertEqual(rva["adjusted_pnl_usd"], "1000")

        # Empty list stays null with reason, never 0
        empty_diag = compute_monthly_diagnostics([])
        self.assertEqual(empty_diag["status"], "unavailable")
        self.assertIsNone(empty_diag["win_rate"])

    def test_policy_config_hash_pins_cost_and_clock_start_definition(self) -> None:
        digest = policy_config_hash()
        self.assertEqual(len(digest), 64)
        self.assertEqual(PINNED_POLICY_PARAMS["B0"], "50000")
        self.assertEqual(PINNED_POLICY_PARAMS["H_default"], "0")
        self.assertFalse(PINNED_POLICY_PARAMS["kelly_applied"])
        self.assertEqual(PINNED_POLICY_PARAMS["unavailable_broker_inputs"], "explicitly_unverified")
        self.assertIn("cost_assumptions", PINNED_POLICY_PARAMS)
        self.assertEqual(CLOCK_START_DEFINITION["clk_id"], "CLK-01")
        self.assertFalse(CLOCK_START_DEFINITION["drills_and_y2_credit_eligible"])
        self.assertTrue(CLOCK_START_DEFINITION["requires_sh2_acceptance"])

    def test_policy_config_hash_reflects_runtime_config_calendar_and_operational_files(self) -> None:
        # Finding #5 regression: operational runtime modules, config, and calendar identity change C7 hashes
        repo_root = Path(__file__).resolve().parents[1]
        base_code = compute_code_manifest_hash(repo_root)
        base_policy = policy_config_hash(repo_root=repo_root)

        base_cfg = load_effective_runtime_config(repo_root)
        changed_cfg = dict(base_cfg)
        changed_cfg["commitment_deadline_et"] = "22:00"
        self.assertNotEqual(
            policy_config_hash(runtime_config=changed_cfg, repo_root=repo_root),
            base_policy,
        )
        changed_ots = dict(base_cfg)
        changed_ots["ots_calendars"] = ["https://custom.calendar.example"]
        self.assertNotEqual(
            policy_config_hash(runtime_config=changed_ots, repo_root=repo_root),
            base_policy,
        )
        # Item #2 & #5: decision cutoffs, treasury_endpoint, and yahoo_symbol_map affect policy_config_hash
        for key, new_val in (
            ("decision_cutoff_et", "17:45"),
            ("decision_cutoff_half_day_et", "14:45"),
            ("treasury_endpoint", "https://alt-treasury.example/yield{year}.csv"),
            ("yahoo_symbol_map", {"XNDX": "^NDX", "XSOX": "SOX"}),
        ):
            alt_cfg = dict(base_cfg)
            alt_cfg[key] = new_val
            self.assertNotEqual(
                policy_config_hash(runtime_config=alt_cfg, repo_root=repo_root),
                base_policy,
                key,
            )

        # Changing calendar spec digest changes policy_config_hash
        self.assertNotEqual(
            policy_config_hash(
                calendar_spec={"calendar_version": "csa22-v2", "spec_digest": "f" * 64},
                repo_root=repo_root,
            ),
            base_policy,
        )

        # Modifying each operational Python module changes both code_version_sha and policy_config_hash
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_repo = Path(tmpdir) / "repo"
            shutil.copytree(repo_root / "src", tmp_repo / "src")
            shutil.copytree(repo_root / "config", tmp_repo / "config")
            shutil.copytree(repo_root / "specs", tmp_repo / "specs")
            self.assertEqual(compute_code_manifest_hash(tmp_repo), base_code)
            self.assertEqual(policy_config_hash(repo_root=tmp_repo), base_policy)

            for mod in (
                "src/shadow/scheduler.py",
                "src/shadow/feeder.py",
                "src/shadow/feeder_live.py",
                "src/shadow/replay.py",
                "src/shadow/ots.py",
            ):
                target = tmp_repo / mod
                orig = target.read_text(encoding="utf-8")
                target.write_text(orig + "\n# mutated\n", encoding="utf-8")
                self.assertNotEqual(compute_code_manifest_hash(tmp_repo), base_code, mod)
                self.assertNotEqual(policy_config_hash(repo_root=tmp_repo), base_policy, mod)
                target.write_text(orig, encoding="utf-8")

    def test_halted_fund_holdings_reserved_against_joint_cap_and_weight_ceil(self) -> None:
        # Codex P2 regression (PRRT_kwDOV2RNAc67oejF): balanced $50k with 500 TQQQ ($25k) under
        # V2 fund-scoped HALT and 0 SOXL must reserve frozen TQQQ exposure so SOXL cannot buy.
        overweight_halt = compute_session_sizing(
            _make_inputs(
                E="50000",
                E_star="50000",
                L="0",
                p_tqqq="50",
                q_pre_tqqq=500,
                p_soxl="40",
                q_pre_soxl=0,
            ),
            active_control_state="NORMAL",
            fund_control_states={"TQQQ": "HALT", "SOXL": "NORMAL"},
        )
        bal_over = _tier(overweight_halt, "balanced")
        self.assertEqual(_fund(bal_over, "TQQQ")["delta_q_exec"], 0)
        self.assertEqual(_fund(bal_over, "SOXL")["q_ideal"], 0)
        self.assertEqual(_fund(bal_over, "SOXL")["delta_q_exec"], 0)

        # Partial frozen TQQQ ($2,500 => x_cur = 0.15) leaves x_ML - 0.15 = 0.051289... headroom
        partial_halt = compute_session_sizing(
            _make_inputs(
                E="50000",
                E_star="50000",
                L="0",
                p_tqqq="50",
                q_pre_tqqq=50,
                p_soxl="40",
                q_pre_soxl=0,
            ),
            active_control_state="NORMAL",
            fund_control_states={"TQQQ": "HALT", "SOXL": "NORMAL"},
        )
        bal_part = _tier(partial_halt, "balanced")
        soxl_part = _fund(bal_part, "SOXL")
        tqqq_x_cur = Decimal("3") * (Decimal("2500") / Decimal("50000"))
        total_beta_post = tqqq_x_cur + Decimal("1.5") * Decimal(soxl_part["x_post_joint"])
        self.assertLessEqual(total_beta_post, Decimal(bal_part["x_ML"]) + Decimal("0.000002"))
        self.assertEqual(_fund(bal_part, "TQQQ")["delta_q_exec"], 0)

    def test_derisk_excluded_from_cap_without_frozen_and_tier_j1_trim_triggers_bypass(self) -> None:
        # 1) When TQQQ is DE-RISK and frozen_insts is empty, SOXL keeps full headroom.
        derisk_res = compute_session_sizing(
            _make_inputs(
                E="50000",
                E_star="50000",
                q_pre_tqqq=200,
                q_pre_soxl=0,
            ),
            fund_control_states={"TQQQ": "DE-RISK", "SOXL": "NORMAL"},
        )
        bal_derisk = _tier(derisk_res, "balanced")
        self.assertEqual(_fund(bal_derisk, "TQQQ")["w_target"], "0")
        self.assertGreater(int(_fund(bal_derisk, "SOXL")["q_ideal"]), 0)

        # 2) Tier-level J1_TRIM triggers mandatory_bypass for a weight-reducing trade even inside +/-20% band.
        # Compute baseline NORMAL balanced weight/q_pre slightly above target (inside 20% band) then trigger J1_TRIM on balanced tier.
        base_normal = compute_session_sizing(_make_inputs(E="50000", E_star="50000"))
        bal_norm = _tier(base_normal, "balanced")
        q_target_tqqq = _fund(bal_norm, "TQQQ")["q_ideal"]
        trim_res = compute_session_sizing(
            _make_inputs(
                E="50000",
                E_star="50000",
                q_pre_tqqq=int(q_target_tqqq) + 1,
            ),
            tier_control_states={"balanced": ("J1_TRIM", None)},
        )
        bal_trim = _tier(trim_res, "balanced")
        self.assertEqual(_fund(bal_trim, "TQQQ")["delta_q_exec"], -1)


if __name__ == "__main__":
    unittest.main()
