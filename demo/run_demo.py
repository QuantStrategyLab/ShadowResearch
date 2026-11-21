"""SH-1 §7 Design Demonstration runner (D1-D8)."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = str(REPO_ROOT / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator
from shadow.feeder import SOXL_PROXY_LABEL, VINTAGE_Y2, run_reconstruction_window
from shadow.ledger import (
    GENESIS_PREV_HASH,
    LedgerAppendError,
    ShadowLedger,
    build_record_body,
    check_forward_clock_credit,
    compute_code_manifest_hash,
)
from shadow.replay import replay_window
from shadow.sizing import compute_session_sizing, quantize_6dp

D1_FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "d1_synthetic_6sessions.json"
Y2_OVERLAP_DIR = REPO_ROOT / "tests" / "fixtures" / "y2overlap"

# Independent 6dp expectations frozen in demo/DEMO-CASES.md
D1_EXPECTED_6DP: dict[str, dict[str, dict[str, object]]] = {
    "S1": {
        "conservative": {
            "c": "0.150000", "x_ML": "0.091717", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.091717", "bind": False, "cf": "0.091717", "x_post": "0.068788", "w": "0.022929", "qi": 22, "dq": 22, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.061145", "bind": False, "cf": "0.061145", "x_post": "0.015286", "w": "0.005095", "qi": 6, "dq": 6, "sup": False},
        },
        "balanced": {
            "c": "0.300000", "x_ML": "0.201289", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.201289", "bind": False, "cf": "0.201289", "x_post": "0.150967", "w": "0.050322", "qi": 50, "dq": 50, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.134192", "bind": False, "cf": "0.134192", "x_post": "0.033548", "w": "0.011183", "qi": 13, "dq": 13, "sup": False},
        },
        "aggressive": {
            "c": "0.500000", "x_ML": "0.391176", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.391176", "bind": False, "cf": "0.391176", "x_post": "0.293382", "w": "0.097794", "qi": 97, "dq": 97, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.260784", "bind": False, "cf": "0.260784", "x_post": "0.065196", "w": "0.021732", "qi": 27, "dq": 27, "sup": False},
        },
    },
    "S2": {
        "conservative": {
            "c": "0.055556", "x_ML": "0.032257", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "0.500000", "x_Kc": "0.113932", "x_pre": "0.016129", "bind": False, "cf": "0.016129", "x_post": "0.016129", "w": "0.005376", "qi": 4, "dq": 4, "sup": False},
            "SOXL": {"v": "0.500000", "x_Kc": "0.113932", "x_pre": "0.010752", "bind": False, "cf": "0.010752", "x_post": "0.007168", "w": "0.002389", "qi": 2, "dq": 2, "sup": False},
        },
        "balanced": {
            "c": "0.222222", "x_ML": "0.141829", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.141829", "bind": False, "cf": "0.141829", "x_post": "0.106372", "w": "0.035457", "qi": 31, "dq": 31, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.094553", "bind": False, "cf": "0.094553", "x_post": "0.023638", "w": "0.007879", "qi": 8, "dq": 8, "sup": False},
        },
        "aggressive": {
            "c": "0.444444", "x_ML": "0.331716", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.331716", "bind": False, "cf": "0.331716", "x_post": "0.248787", "w": "0.082929", "qi": 74, "dq": 74, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.221144", "bind": False, "cf": "0.221144", "x_post": "0.055286", "w": "0.018429", "qi": 20, "dq": 20, "sup": False},
        },
    },
    "S3": {
        "conservative": {
            "c": "0.150000", "x_ML": "0.091717", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.077960", "bind": False, "cf": "0.077960", "x_post": "0.065881", "w": "0.021960", "qi": 21, "dq": 21, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.055030", "bind": False, "cf": "0.055030", "x_post": "0.017224", "w": "0.005741", "qi": 7, "dq": 7, "sup": False},
        },
        "balanced": {
            "c": "0.300000", "x_ML": "0.201289", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.171095", "bind": False, "cf": "0.171095", "x_post": "0.144588", "w": "0.048196", "qi": 48, "dq": 48, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.120773", "bind": False, "cf": "0.120773", "x_post": "0.037801", "w": "0.012600", "qi": 15, "dq": 15, "sup": False},
        },
        "aggressive": {
            "c": "0.500000", "x_ML": "0.391176", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.332500", "bind": False, "cf": "0.332500", "x_post": "0.280986", "w": "0.093662", "qi": 93, "dq": 93, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.234706", "bind": False, "cf": "0.234706", "x_post": "0.073460", "w": "0.024487", "qi": 30, "dq": 30, "sup": False},
        },
    },
    "S4": {
        "conservative": {
            "c": "0.150000", "x_ML": "0.091717", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "0.069444", "x_Kc": "0.012659", "x_pre": "0.006369", "bind": False, "cf": "0.000879", "x_post": "0.006369", "w": "0.002123", "qi": 2, "dq": 2, "sup": False},
            "SOXL": {"v": "0.062500", "x_Kc": "0.010254", "x_pre": "0.003822", "bind": False, "cf": "0.000427", "x_post": "0.003822", "w": "0.001274", "qi": 1, "dq": 1, "sup": False},
        },
        "balanced": {
            "c": "0.300000", "x_ML": "0.201289", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "0.138889", "x_Kc": "0.025318", "x_pre": "0.027957", "bind": True, "cf": "0.003516", "x_post": "0.027957", "w": "0.009319", "qi": 9, "dq": 9, "sup": False},
            "SOXL": {"v": "0.125000", "x_Kc": "0.020508", "x_pre": "0.016774", "bind": False, "cf": "0.001709", "x_post": "0.016774", "w": "0.005591", "qi": 6, "dq": 6, "sup": False},
        },
        "aggressive": {
            "c": "0.500000", "x_ML": "0.391176", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "0.263889", "x_Kc": "0.050637", "x_pre": "0.103227", "bind": True, "cf": "0.013362", "x_post": "0.103227", "w": "0.034409", "qi": 34, "dq": 34, "sup": False},
            "SOXL": {"v": "0.237500", "x_Kc": "0.041016", "x_pre": "0.061936", "bind": True, "cf": "0.006494", "x_post": "0.061936", "w": "0.020645", "qi": 25, "dq": 25, "sup": False},
        },
    },
    "S5": {
        "conservative": {
            "c": "0.150000", "x_ML": "0.091717", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.091717", "bind": False, "cf": "0.091717", "x_post": "0.068788", "w": "0.022929", "qi": 22, "dq": 22, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.061145", "bind": False, "cf": "0.061145", "x_post": "0.015286", "w": "0.005095", "qi": 6, "dq": 6, "sup": False},
        },
        "balanced": {
            "c": "0.300000", "x_ML": "0.201289", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.201289", "bind": False, "cf": "0.201289", "x_post": "0.150967", "w": "0.050322", "qi": 50, "dq": 50, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.134192", "bind": False, "cf": "0.134192", "x_post": "0.033548", "w": "0.011183", "qi": 13, "dq": 13, "sup": False},
        },
        "aggressive": {
            "c": "0.500000", "x_ML": "0.391176", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.391176", "bind": False, "cf": "0.391176", "x_post": "0.293382", "w": "0.097794", "qi": 97, "dq": 97, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.260784", "bind": False, "cf": "0.260784", "x_post": "0.065196", "w": "0.021732", "qi": 27, "dq": 27, "sup": False},
        },
    },
    "S6": {
        "conservative": {
            "c": "0.150000", "x_ML": "0.091717", "sub": "c", "x_Ks": "0.113932",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.091717", "bind": False, "cf": "0.091717", "x_post": "0.068788", "w": "0.022929", "qi": 45, "dq": 0, "sup": True},
            "SOXL": {"v": "1.000000", "x_Kc": "0.113932", "x_pre": "0.061145", "bind": False, "cf": "0.061145", "x_post": "0.015286", "w": "0.005095", "qi": 6, "dq": 0, "sup": False},
        },
        "balanced": {
            "c": "0.300000", "x_ML": "0.201289", "sub": "c", "x_Ks": "0.227865",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.201289", "bind": False, "cf": "0.201289", "x_post": "0.150967", "w": "0.050322", "qi": 100, "dq": 0, "sup": False},
            "SOXL": {"v": "1.000000", "x_Kc": "0.227865", "x_pre": "0.134192", "bind": False, "cf": "0.134192", "x_post": "0.033548", "w": "0.011183", "qi": 13, "dq": 0, "sup": False},
        },
        "aggressive": {
            "c": "0.500000", "x_ML": "0.391176", "sub": "c", "x_Ks": "0.455729",
            "TQQQ": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.391176", "bind": False, "cf": "0.391176", "x_post": "0.293382", "w": "0.097794", "qi": 195, "dq": 0, "sup": True},
            "SOXL": {"v": "1.000000", "x_Kc": "0.455729", "x_pre": "0.260784", "bind": False, "cf": "0.260784", "x_post": "0.065196", "w": "0.021732", "qi": 27, "dq": 0, "sup": False},
        },
    },
}


def _assert_6dp(actual_str: str, expected_6dp: str, label: str) -> None:
    actual_q = quantize_6dp(actual_str)
    exp_q = Decimal(expected_6dp)
    if actual_q != exp_q:
        raise AssertionError(f"{label}: expected {exp_q}, got {actual_q} (raw {actual_str})")


def _tier_map(tiers: list[dict]) -> dict[str, dict]:
    return {t["tier_id"]: t for t in tiers}


def _fund_map(tier_obj: dict) -> dict[str, dict]:
    return {f["instrument"]: f for f in tier_obj["funds"]}


def _check_positive_controls() -> None:
    base = {
        "E": "50000", "E_star": "50000", "H": "0", "L": "0",
        "funds": {
            "TQQQ": {"s": "1", "close_price": "50", "q_pre": 0, "sigma_forecast": "0.05"},
            "SOXL": {"s": "1", "close_price": "40", "q_pre": 0, "sigma_forecast": "0.05"},
        },
    }
    res0 = compute_session_sizing(base)
    tm0 = _tier_map(res0["tiers"])
    for tid, exp_m0, exp_k in (
        ("conservative", "0.09", "0.11"),
        ("balanced", "0.20", "0.23"),
        ("aggressive", "0.39", "0.46"),
    ):
        assert Decimal(tm0[tid]["x_ML"]).quantize(Decimal("0.01")) == Decimal(exp_m0)
        assert Decimal(_fund_map(tm0[tid])["TQQQ"]["x_K_struct"]).quantize(Decimal("0.01")) == Decimal(exp_k)

    for eq, loss, exp_c3, exp_ml2 in (
        ("50000", "0", "0.300", "0.20"),
        ("45000", "5000", "0.222", "0.14"),
        ("40000", "10000", "0.125", "0.08"),
        ("37500", "12500", "0.067", "0.04"),
    ):
        r = compute_session_sizing({**base, "E": eq, "L": loss})
        bal = _tier_map(r["tiers"])["balanced"]
        assert Decimal(bal["c"]).quantize(Decimal("0.001")) == Decimal(exp_c3)
        assert Decimal(bal["x_ML"]).quantize(Decimal("0.01")) == Decimal(exp_ml2)


def _check_standing_assertions(records: list[dict], source_tag: str) -> int:
    row_count = 0
    for env in records:
        body = env["record_body"]
        assert body["assertions"]["kelly_applied"] is False, f"{source_tag}: kelly_applied"
        assert body["assertions"]["orders_placed"] == 0, f"{source_tag}: orders_placed"
        assert body["bankroll"]["H"] == "0", f"{source_tag}: H != 0"
        assert body["assertions"]["B0"] == "50000", f"{source_tag}: B0 != 50000"
        breakers = body["evaluator"]["breakers"]
        assert [b["id"] for b in breakers] == list(ALL_BREAKER_IDS), f"{source_tag}: 24 breakers"
        for tier in body["tiers"]:
            assert tier["orders_placed"] == 0, f"{source_tag}: tier orders_placed"
            for fund in tier["funds"]:
                assert fund["kelly_applied"] is False, f"{source_tag}: fund kelly_applied"
                row_count += 1
    return row_count


def run_all_cases() -> list[tuple[str, bool, str]]:
    """Execute D1-D8 in temporary scratch space and return list of (id, ok, detail)."""
    results: list[tuple[str, bool, str]] = []
    d1_fixture = json.loads(D1_FIXTURE_PATH.read_text(encoding="utf-8"))
    spec = load_spec(DEFAULT_SPEC_DIR)
    ev = StubEvaluator()
    code_sha = compute_code_manifest_hash(REPO_ROOT)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        d1_ledger_dir = tmp / "d1_ledger"
        d1_bundle_dir = tmp / "d1_bundle"
        d1_bundle_dir.mkdir(parents=True, exist_ok=True)
        d1_ledger = ShadowLedger(d1_ledger_dir)

        prev = GENESIS_PREV_HASH
        prior_state: dict = {}
        d1_sizing_by_sid: dict[str, dict] = {}
        bundle_sessions: list[dict] = []

        # Build and execute D1 6-session window
        for seq, sess in enumerate(d1_fixture["sessions"]):
            sid = sess["session_id"]
            cal = resolve_session("XNYS", sess["session_date"], spec=spec)
            built = build_record_body(
                chain_id=d1_fixture["chain_id"],
                sequence=seq,
                prev_record_sha256=prev,
                mode=d1_fixture["mode"],
                session_date=sess["session_date"],
                decision_timestamp_utc=sess["decision_timestamp_utc"],
                session_inputs=sess["inputs"],
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state=prior_state,
            )
            assert built["appended"] is True
            env = d1_ledger.append_record(built["record_body"])
            prev = env["record_sha256"]
            prior_state = built["next_row_state"]
            d1_sizing_by_sid[sid] = built["record_body"]
            bundle_sessions.append(
                {
                    "session_date": sess["session_date"],
                    "decision_timestamp_utc": sess["decision_timestamp_utc"],
                    "inputs": sess["inputs"],
                }
            )

        (d1_bundle_dir / "replay_bundle.json").write_text(
            json.dumps(
                {
                    "chain_id": d1_fixture["chain_id"],
                    "mode": d1_fixture["mode"],
                    "calendar_spec_dir": str(DEFAULT_SPEC_DIR),
                    "calendar_spec_digest": spec["spec_digest"],
                    "code_version_sha": code_sha,
                    "sessions": bundle_sessions,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        # D1: Positive controls + exact 3-tier sizing outputs across 6 sessions
        _check_positive_controls()
        for sid, exp_tiers in D1_EXPECTED_6DP.items():
            tmap = _tier_map(d1_sizing_by_sid[sid]["tiers"])
            for tid, exp_t in exp_tiers.items():
                t = tmap[tid]
                _assert_6dp(t["c"], str(exp_t["c"]), f"{sid}.{tid}.c")
                _assert_6dp(t["x_ML"], str(exp_t["x_ML"]), f"{sid}.{tid}.x_ML")
                assert t["x_ML_binding_subbound"] == exp_t["sub"]
                fmap = _fund_map(t)
                _assert_6dp(fmap["TQQQ"]["x_K_struct"], str(exp_t["x_Ks"]), f"{sid}.{tid}.x_Ks")
                for inst in ("TQQQ", "SOXL"):
                    exp_f = exp_t[inst]
                    f = fmap[inst]
                    _assert_6dp(f["v"], str(exp_f["v"]), f"{sid}.{tid}.{inst}.v")
                    _assert_6dp(f["x_K_cond"], str(exp_f["x_Kc"]), f"{sid}.{tid}.{inst}.x_Kc")
                    _assert_6dp(f["x_pre"], str(exp_f["x_pre"]), f"{sid}.{tid}.{inst}.x_pre")
                    assert f["kelly_would_have_bound"] is exp_f["bind"], f"{sid}.{tid}.{inst}.bind"
                    _assert_6dp(f["kelly_counterfactual_x"], str(exp_f["cf"]), f"{sid}.{tid}.{inst}.cf")
                    _assert_6dp(f["x_post_joint"], str(exp_f["x_post"]), f"{sid}.{tid}.{inst}.x_post")
                    _assert_6dp(f["w_target"], str(exp_f["w"]), f"{sid}.{tid}.{inst}.w")
                    assert f["q_ideal"] == exp_f["qi"], f"{sid}.{tid}.{inst}.qi"
                    assert f["delta_q_exec"] == exp_f["dq"], f"{sid}.{tid}.{inst}.dq"
                    assert f["no_trade_band_suppressed"] is exp_f["sup"], f"{sid}.{tid}.{inst}.sup"
                    assert f["emergency_bypass_triggered"] is False
                    assert f["kelly_applied"] is False
            if sid == "S3":
                for tid in ("conservative", "balanced", "aggressive"):
                    assert Decimal(tmap[tid]["alpha_joint"]) < Decimal("1")

        results.append(("D1", True, "6 sessions x 3 tiers x 2 funds matched independent 6dp + positive controls"))

        # D2: Split invariance and half-day invariance
        s1_t = _tier_map(d1_sizing_by_sid["S1"]["tiers"])
        s5_t = _tier_map(d1_sizing_by_sid["S5"]["tiers"])
        s6_t = _tier_map(d1_sizing_by_sid["S6"]["tiers"])
        for tid in ("conservative", "balanced", "aggressive"):
            # Half-day invariance (S5 half-day vs S1 regular)
            assert s5_t[tid]["funds"] == s1_t[tid]["funds"]
            assert s5_t[tid]["c"] == s1_t[tid]["c"]
            assert s5_t[tid]["x_ML"] == s1_t[tid]["x_ML"]
            # Split invariance (S6 post-2:1 split vs S5 pre-split on x, w_target, evaluator)
            f5 = _fund_map(s5_t[tid])
            f6 = _fund_map(s6_t[tid])
            for inst in ("TQQQ", "SOXL"):
                assert f6[inst]["x_pre"] == f5[inst]["x_pre"]
                assert f6[inst]["x_post_joint"] == f5[inst]["x_post_joint"]
                assert f6[inst]["w_target"] == f5[inst]["w_target"]
        assert d1_sizing_by_sid["S6"]["evaluator"]["effective_state_by_scope"] == d1_sizing_by_sid["S5"]["evaluator"]["effective_state_by_scope"]

        # Clean 2:1 split pair with non-zero q_pre scaled 2x
        pre_split_in = {
            "E": "50000", "E_star": "50000", "H": "0", "L": "0",
            "funds": {
                "TQQQ": {"s": "1", "close_price": "50", "q_pre": 10, "sigma_forecast": "0.05"},
                "SOXL": {"s": "1", "close_price": "40", "q_pre": 5, "sigma_forecast": "0.05"},
            },
        }
        post_split_in = {
            "E": "50000", "E_star": "50000", "H": "0", "L": "0",
            "funds": {
                "TQQQ": {"s": "1", "close_price": "25", "q_pre": 20, "sigma_forecast": "0.05"},
                "SOXL": {"s": "1", "close_price": "40", "q_pre": 5, "sigma_forecast": "0.05"},
            },
        }
        pre_res = compute_session_sizing(pre_split_in)
        post_res = compute_session_sizing(post_split_in)
        for tid in ("conservative", "balanced", "aggressive"):
            pt_pre = _fund_map(_tier_map(pre_res["tiers"])[tid])
            pt_post = _fund_map(_tier_map(post_res["tiers"])[tid])
            for inst in ("TQQQ", "SOXL"):
                for k in ("x_pre", "x_post_joint", "w_target", "no_trade_band_suppressed", "binding_constraint"):
                    assert pt_pre[inst][k] == pt_post[inst][k]
        results.append(("D2", True, "split 2:1 invariance (x, w_target, decisions) and half-day invariance verified"))

        # D4: Bit-for-bit replay of D1
        replay_res = replay_window(d1_bundle_dir, d1_ledger_dir)
        assert replay_res["bit_for_bit_match"] is True
        assert replay_res["chain_ok"] is True
        assert replay_res["receipts_ok"] is True
        assert replay_res["replayed_count"] == 6
        results.append(("D4", True, "6/6 D1 records replayed bit-for-bit; chain + receipts verified separately"))

        # D5: Tamper rejection (1-byte mutation, fork, gap, duplicate)
        d1_records = d1_ledger.load_records()
        # 1. One-byte mutation at seq 2
        mut_dir = tmp / "d5_mut"
        mut_ledger = ShadowLedger(mut_dir)
        for env in d1_records:
            mut_ledger.append_record(env["record_body"])
        rec2 = mut_dir / "seq_000002.record.json"
        raw2 = bytearray(rec2.read_bytes())
        pos = raw2.index(b'"50000"') + 2
        raw2[pos] = ord("1") if raw2[pos] != ord("1") else ord("2")
        rec2.write_bytes(bytes(raw2))
        ok_m, bad_m, _ = mut_ledger.verify_chain()
        assert ok_m is False and bad_m == 2

        # 2. Forked previous hash rejected
        head_d1 = d1_ledger.head()
        assert head_d1 is not None
        fork_body = copy.deepcopy(head_d1["record_body"])
        fork_body["sequence"] = 6
        fork_body["session_date"] = "2025-12-02"
        fork_body["prev_record_sha256"] = "a" * 64
        fork_rejected = False
        try:
            d1_ledger.append_record(fork_body)
        except LedgerAppendError:
            fork_rejected = True
        assert fork_rejected

        # 3. Dropped record (gap at seq 1)
        gap_dir = tmp / "d5_gap"
        gap_ledger = ShadowLedger(gap_dir)
        for env in d1_records:
            gap_ledger.append_record(env["record_body"])
        (gap_dir / "seq_000001.record.json").unlink()
        ok_g, bad_g, _ = gap_ledger.verify_chain()
        assert ok_g is False and bad_g == 1

        # 4. Duplicate append rejected
        dup_body = copy.deepcopy(head_d1["record_body"])
        dup_body["sequence"] = 6
        dup_body["prev_record_sha256"] = head_d1["record_sha256"]
        dup_rejected = False
        try:
            d1_ledger.append_record(dup_body)
        except LedgerAppendError:
            dup_rejected = True
        assert dup_rejected
        results.append(("D5", True, "mutation(seq=2), fork, gap(seq=1), duplicate append all rejected"))

        # D6: Y2-overlap 10-session reconstruction + forward-clock rejection
        d6_res = run_reconstruction_window(
            y2_dir=Y2_OVERLAP_DIR,
            ledger_dir=tmp / "d6_ledger",
            archive_dir=tmp / "d6_archive",
            spec_dir=DEFAULT_SPEC_DIR,
        )
        d6_ledger: ShadowLedger = d6_res["ledger"]
        d6_records = d6_ledger.load_records()
        d6_receipts = d6_ledger.load_receipts()
        assert len(d6_records) == 10
        assert d6_ledger.verify_chain()[0] is True
        assert d6_ledger.verify_all_receipts()[0] is True
        for env in d6_records:
            b = env["record_body"]
            assert b["mode"] == "reconstruction"
            assert b["observation_class"] == "historical_reconstruction"
            lbl = b["discipline_labels"]
            assert lbl["vintage_qualification"] == VINTAGE_Y2
            assert lbl["soxl_basis"] == SOXL_PROXY_LABEL
            assert lbl["semi_substitution"] == "native_xsox_sub_for_icesemit"
            assert lbl["forward_clock_credit_eligible"] is False
            assert set(lbl["per_series_is_modelled"].keys()) == {"TQQQ", "SOXL", "XNDX", "SEMI", "SOFR", "TBILL_3M"}
        credit_ok, credit_reasons = check_forward_clock_credit(d6_records, d6_receipts)
        assert credit_ok is False and len(credit_reasons) >= 10
        results.append(("D6", True, "10-session Y2 reconstruction labelled; forward clock credit rejected"))

        # D3: Standing assertions across every row of every run (D1 + D6)
        rows_checked = _check_standing_assertions(d1_records, "D1") + _check_standing_assertions(d6_records, "D6")
        results.append(("D3", True, f"kelly_applied=false, orders_placed=0, 24 breakers across {rows_checked} rows"))

        # D7: Withheld SOXL close -> O1 fail-closed + zero ledger append
        count_before = len(d1_ledger.load_records())
        s1_missing = copy.deepcopy(d1_fixture["sessions"][0]["inputs"])
        s1_missing["funds"]["SOXL"]["close_price"] = None
        cal_next = resolve_session("XNYS", "2025-12-02", spec=spec)
        fail_res = build_record_body(
            chain_id=d1_fixture["chain_id"],
            sequence=count_before,
            prev_record_sha256=d1_ledger.head()["record_sha256"],  # type: ignore[index]
            mode="drill",
            session_date="2025-12-02",
            decision_timestamp_utc=cal_next["close_utc"],
            session_inputs=s1_missing,
            calendar_session=cal_next,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state=prior_state,
        )
        assert fail_res["appended"] is False and fail_res["fail_closed"] is True
        assert fail_res["o1_output"]["state"] == "NO-NEW-RISK"
        assert fail_res["o1_output"]["would_have_tripped"] is True
        assert "SOXL_close" in fail_res["o1_output"]["reason"]
        assert len(d1_ledger.load_records()) == count_before
        results.append(("D7", True, f"O1 fail-closed ({fail_res['o1_output']['reason']}); 0 records appended"))

        # D8: C-SA-22 full 2025 calendar vectors + boundary errors
        expected_2025 = json.loads((DEFAULT_SPEC_DIR / "expected-2025.json").read_text(encoding="utf-8"))
        assert len(expected_2025["sessions"]) == 365
        for ds, exp_entry in expected_2025["sessions"].items():
            assert resolve_session("XNYS", ds, spec=spec) == exp_entry, f"calendar mismatch on {ds}"
        unk_venue = resolve_session("XHKG", "2025-11-21", spec=spec)
        unk_before = resolve_session("XNYS", "2024-12-31", spec=spec)
        unk_after = resolve_session("XNYS", "2026-01-01", spec=spec)
        assert unk_venue["session_state"] == "unknown" and unk_venue["blocks_orders"] is True
        assert unk_before["session_state"] == "unknown" and unk_before["blocks_orders"] is True
        assert unk_after["session_state"] == "unknown" and unk_after["blocks_orders"] is True
        results.append(("D8", True, "365/365 2025 dates matched expected-2025.json; unknown venue/range blocked"))

    # Order output D1..D8 sequentially
    order = {"D1": 1, "D2": 2, "D3": 3, "D4": 4, "D5": 5, "D6": 6, "D7": 7, "D8": 8}
    results.sort(key=lambda item: order[item[0]])
    return results


def main() -> int:
    """Execute D1-D8 and print one line per row."""
    failures = 0
    for case_id, ok, detail in run_all_cases():
        status = "PASS" if ok else "FAIL"
        print(f"{case_id} {status}: {detail}")
        if not ok:
            failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
