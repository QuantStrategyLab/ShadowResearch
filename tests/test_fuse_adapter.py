"""Tests for digest-pinned vendored FUSE-1 adapter, tamper detection, V2 hwm_adjusted, and stub quarantine."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.evaluator import ALL_BREAKER_IDS, StubEvaluator, StubProductionForbiddenError
from shadow.fuse_adapter import (
    DEFAULT_VENDOR_ENGINE_ROOT,
    EngineIdentityError,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    V2PayloadSchemaError,
    VendoredFuseEvaluator,
    build_fuse_validated_inputs,
    resolve_aggregate_control_state,
    verify_f1_entry_gates,
    verify_vendored_engine_bundle,
)
from shadow.ledger import GENESIS_PREV_HASH, build_record_body
from shadow.manifest import compute_code_manifest_hash
from shadow.sizing import compute_session_sizing


def _valid_inputs() -> dict:
    return {
        "E": "50000",
        "E_star": "50000",
        "H": "0",
        "L": "0",
        "positions_source_record": {
            "as_of": "2026-01-05T22:00:00.000Z",
            "age_sessions": 0,
        },
        "broker_reconciliation_source_record": {
            "as_of": "2026-01-05T22:00:00.000Z",
            "age_sessions": 0,
        },
        "index_one_session_returns": {"NDX": 0.0, "SEMI": 0.0},
        "funds": {
            "TQQQ": {
                "s": "1",
                "close_price": "50",
                "q_pre": 100,
                "sigma_forecast": "0.24",
                "fund_return": 0.0,
                "index_return": 0.0,
            },
            "SOXL": {
                "s": "1",
                "close_price": "40",
                "q_pre": 100,
                "sigma_forecast": "0.22",
                "fund_return": 0.0,
                "index_return": 0.0,
            },
        },
    }


class FuseAdapterTest(unittest.TestCase):
    def test_entry_gates_and_read_only_vendoring_verified(self) -> None:
        gates = verify_f1_entry_gates(require_read_only=False)
        self.assertTrue(gates["ok"])
        self.assertTrue(gates["f1_a01_id_set_equal"])
        self.assertEqual(gates["f1_a03_base_cells"], "264/264")
        self.assertEqual(gates["f1_a04_tier_repetitions"], "132/132")
        self.assertEqual(gates["f1_a10_successor_cases"], "13/13")
        self.assertEqual(gates["f1_a11_unresolved_fail_closed"], ["O5", "V5", "V7"])
        self.assertEqual(gates["engine_hash"], PINNED_FUSE_ENGINE_HASH)
        self.assertEqual(gates["spec_version"], PINNED_FUSE_SPEC_VERSION)

        # Confirm zero __pycache__ polluted vendored dir
        pycache_dirs = list(DEFAULT_VENDOR_ENGINE_ROOT.rglob("__pycache__"))
        self.assertEqual(pycache_dirs, [])

    def test_one_byte_tamper_extra_file_and_corrupt_pyc_rejected_before_evaluate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            copy_dir = Path(tmpdir) / "vendor"
            shutil.copytree(DEFAULT_VENDOR_ENGINE_ROOT, copy_dir)

            # 1. Valid copy loads cleanly
            ev = VendoredFuseEvaluator(vendor_root=copy_dir)
            self.assertEqual(ev.engine_hash, PINNED_FUSE_ENGINE_HASH)

            # 2. Tamper 1 byte in existing .py AFTER load -> evaluate fails closed
            target = copy_dir / "fuse_engine" / "canonical.py"
            orig_bytes = target.read_bytes()
            os.chmod(target, 0o644)
            target.write_bytes(orig_bytes + b" ")
            spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
            cal = resolve_session("XNYS", "2026-01-05", spec=spec)
            vin = build_fuse_validated_inputs(
                _valid_inputs(),
                cal,
                policy_hash="p",
                session_date="2026-01-05",
                as_of="2026-01-05T22:00:00.000Z",
            )
            with self.assertRaises(EngineIdentityError):
                ev.evaluate(
                    PINNED_FUSE_SPEC_VERSION,
                    PINNED_FUSE_ENGINE_HASH,
                    "p",
                    "2026-01-05",
                    "2026-01-05T22:00:00.000Z",
                    "shadow",
                    vin,
                    {},
                    None,
                )

            # Restore file and verify clean
            target.write_bytes(orig_bytes)
            verify_vendored_engine_bundle(copy_dir)

            # 3. Extra file inside fuse_engine fails
            extra = copy_dir / "fuse_engine" / "shim.py"
            extra.write_text("# shim\n", encoding="utf-8")
            with self.assertRaises(EngineIdentityError):
                verify_vendored_engine_bundle(copy_dir)
            extra.unlink()

            # 4. Corrupt __pycache__/*.pyc fails before evaluation
            pyc_dir = copy_dir / "fuse_engine" / "__pycache__"
            pyc_dir.mkdir()
            tag = sys.implementation.cache_tag or "cpython-313"
            bogus_pyc = pyc_dir / f"canonical.{tag}.pyc"
            bogus_pyc.write_bytes(importlib.util.MAGIC_NUMBER + b"\x00" * 32)
            with self.assertRaises(EngineIdentityError):
                verify_vendored_engine_bundle(copy_dir)

    def test_zero_conformance_shims_and_hwm_adjusted_caller_contract(self) -> None:
        ev = VendoredFuseEvaluator()
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)

        # FUSE-1 rejects non-'shadow' mode directly without adapter coercion
        vin = build_fuse_validated_inputs(
            _valid_inputs(),
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        with self.assertRaises(ValueError):
            ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-05",
                "2026-01-05T22:00:00.000Z",
                "live",
                vin,
                {},
                None,
            )

        # Clean shadow run keeps unresolved V5/V7/O5 fail-closed without numeric shims
        rows, eff, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin,
            {},
            None,
        )
        self.assertEqual(set(rows.keys()), set(ALL_BREAKER_IDS))
        self.assertEqual(resolve_aggregate_control_state(eff), "NORMAL")
        for bid in ("V5", "V7", "O5"):
            self.assertEqual(rows[bid]["reason"], "unresolved_fail_closed")
            self.assertTrue(rows[bid]["would_have_tripped"])

        # Caller V2 schema check rejects missing or non-bool hwm_adjusted BEFORE evaluate()
        bad_missing_hwm = json.loads(json.dumps(vin))
        bad_missing_hwm["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split",
            "ratio": 2,
            "adjustment_complete": True,
            "exact_reconciliation": True,
        }
        with self.assertRaises(V2PayloadSchemaError):
            ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-05",
                "2026-01-05T22:00:00.000Z",
                "shadow",
                bad_missing_hwm,
                {},
                None,
            )

        bad_type_hwm = json.loads(json.dumps(vin))
        bad_type_hwm["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split",
            "ratio": 2,
            "adjustment_complete": True,
            "exact_reconciliation": True,
            "hwm_adjusted": "true",
        }
        with self.assertRaises(V2PayloadSchemaError):
            ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-05",
                "2026-01-05T22:00:00.000Z",
                "shadow",
                bad_type_hwm,
                {},
                None,
            )

        # Explicit hwm_adjusted=False passes schema gate and trips V2 HALT in FUSE-1
        explicit_false = json.loads(json.dumps(vin))
        explicit_false["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split",
            "ratio": 2,
            "adjustment_complete": True,
            "exact_reconciliation": True,
            "hwm_adjusted": False,
        }
        r_false, scope_false, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            explicit_false,
            {},
            None,
        )
        self.assertEqual(r_false["V2"]["state"], "HALT")
        self.assertEqual(r_false["V2"]["reason"], "reconciliation_mismatch")
        self.assertEqual(resolve_aggregate_control_state(scope_false), "HALT")

        # Explicit hwm_adjusted=True reconciles cleanly
        explicit_true = json.loads(json.dumps(vin))
        explicit_true["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split",
            "ratio": 2,
            "adjustment_complete": True,
            "exact_reconciliation": True,
            "hwm_adjusted": True,
        }
        r_true, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            explicit_true,
            {},
            None,
        )
        self.assertEqual(r_true["V2"]["state"], "NORMAL")
        self.assertFalse(r_true["V2"]["would_have_tripped"])

    def test_production_mode_quarantines_stub_evaluator_structurally(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        with self.assertRaises(StubProductionForbiddenError):
            build_record_body(
                chain_id="prod-stub-deny",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="production",
                session_date="2026-01-05",
                decision_timestamp_utc="2026-01-05T22:00:00.000Z",
                session_inputs=_valid_inputs(),
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=StubEvaluator(),
                calendar_spec=spec,
            )

    def test_no_silent_synthesis_of_sources_or_zero_returns(self) -> None:
        ev = VendoredFuseEvaluator()
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        bare_inputs = {
            "E": "50000",
            "E_star": "50000",
            "H": "0",
            "L": "0",
            "funds": {
                "TQQQ": {"s": "1", "close_price": "50", "q_pre": 100, "sigma_forecast": "0.24"},
                "SOXL": {"s": "1", "close_price": "40", "q_pre": 100, "sigma_forecast": "0.22"},
            },
        }
        vin = build_fuse_validated_inputs(
            bare_inputs,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertNotIn("positions", vin["source_records"])
        self.assertNotIn("broker_reconciliation", vin["source_records"])
        self.assertNotIn("index_returns", vin["source_records"])
        self.assertIsNone(vin["index_one_session_returns"])
        self.assertIsNone(vin["funds"]["TQQQ"]["fund_return"])
        self.assertIsNone(vin["funds"]["TQQQ"]["index_return"])

        rows, eff, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin,
            {},
            None,
        )
        self.assertEqual(rows["O1"]["state"], "NO-NEW-RISK")
        self.assertEqual(rows["O1"]["reason"], "stale_or_missing_input")
        self.assertEqual(resolve_aggregate_control_state(eff), "NO-NEW-RISK")

        # Real 1-session return (-11% on XNDX) computed from prior_closes + market_records trips L6
        crash_inputs = {
            "E": "50000",
            "E_star": "50000",
            "H": "0",
            "L": "0",
            "positions_source_record": {"as_of": "2026-01-05T22:00:00.000Z", "age_sessions": 0},
            "broker_reconciliation_source_record": {
                "as_of": "2026-01-05T22:00:00.000Z",
                "age_sessions": 0,
            },
            "prior_closes": {
                "TQQQ": "50",
                "SOXL": "40",
                "QQQ": "500",
                "XNDX": "100",
                "SEMI": "100",
            },
            "market_records": [
                {
                    "instrument": "TQQQ",
                    "normalized_value": "33.5",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "SOXL",
                    "normalized_value": "40",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "QQQ",
                    "normalized_value": "445",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "XNDX",
                    "normalized_value": "89",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "SEMI",
                    "normalized_value": "100",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "SOFR",
                    "normalized_value": "0.045",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "TBILL_3M",
                    "normalized_value": "0.044",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
            ],
            "funds": {
                "TQQQ": {"s": "1", "close_price": "33.5", "q_pre": 100, "sigma_forecast": "0.24"},
                "SOXL": {"s": "1", "close_price": "40", "q_pre": 100, "sigma_forecast": "0.22"},
            },
        }
        vin_crash = build_fuse_validated_inputs(
            crash_inputs,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertAlmostEqual(vin_crash["index_one_session_returns"]["NDX"], -0.11, places=6)
        rows_crash, eff_crash, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_crash,
            {},
            None,
        )
        self.assertEqual(rows_crash["L6"]["state"], "HALT")
        self.assertEqual(resolve_aggregate_control_state(eff_crash), "HALT")

    def test_market_record_corporate_action_wired_into_v2(self) -> None:
        ev = VendoredFuseEvaluator()
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)

        def _with_ca(split_ratio: str, cash: str) -> dict:
            inp = json.loads(json.dumps(_valid_inputs()))
            inp["market_records"] = [
                {
                    "instrument": "TQQQ",
                    "normalized_value": "50",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "corporate_action": {
                        "split_ratio": split_ratio,
                        "cash_per_share_usd": cash,
                        "ex_date": "2026-01-05",
                    },
                },
                {
                    "instrument": "SOXL",
                    "normalized_value": "40",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "corporate_action": {"split_ratio": "1", "cash_per_share_usd": "0", "ex_date": "2026-01-05"},
                },
                {
                    "instrument": "QQQ",
                    "normalized_value": "500",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "XNDX",
                    "normalized_value": "20000",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "SEMI",
                    "normalized_value": "5000",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "SOFR",
                    "normalized_value": "0.045",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
                {
                    "instrument": "TBILL_3M",
                    "normalized_value": "0.044",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                },
            ]
            return inp

        # Neutral split=1, cash=0 leaves V2 clean (None)
        vin_clean = build_fuse_validated_inputs(
            _with_ca("1", "0"),
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin_clean["funds"]["TQQQ"]["corporate_action"])
        rows_clean, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_clean,
            {},
            None,
        )
        self.assertEqual(rows_clean["V2"]["state"], "NORMAL")

        # Split factor 2 trips V2 HALT
        vin_split = build_fuse_validated_inputs(
            _with_ca("2", "0"),
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertEqual(vin_split["funds"]["TQQQ"]["corporate_action"]["type"], "split")
        self.assertEqual(vin_split["funds"]["TQQQ"]["corporate_action"]["ratio"], 2.0)
        rows_split, eff_split, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_split,
            {},
            None,
        )
        self.assertEqual(rows_split["V2"]["state"], "HALT")
        self.assertEqual(rows_split["V2"]["reason"], "pending_corporate_action_TQQQ")
        self.assertEqual(resolve_aggregate_control_state(eff_split), "HALT")

        # Cash distribution > 0 also trips V2 HALT
        vin_div = build_fuse_validated_inputs(
            _with_ca("1", "0.25"),
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertEqual(vin_div["funds"]["TQQQ"]["corporate_action"]["type"], "distribution")
        rows_div, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_div,
            {},
            None,
        )
        self.assertEqual(rows_div["V2"]["state"], "HALT")

    def test_no_new_risk_clamp_and_per_fund_isolation_in_sizing(self) -> None:
        # q_pre=0 under NO-NEW-RISK clamps increase to q_ideal=0, delta_q_exec=0
        s_inp_zero = json.loads(json.dumps(_valid_inputs()))
        s_inp_zero["funds"]["TQQQ"]["q_pre"] = 0
        s_inp_zero["funds"]["SOXL"]["q_pre"] = 0
        out_nnr = compute_session_sizing(s_inp_zero, active_control_state="NO-NEW-RISK")
        balanced_zero = next(t for t in out_nnr["tiers"] if t["tier_id"] == "balanced")
        for fdec in balanced_zero["funds"]:
            self.assertEqual(fdec["q_ideal"], 0)
            self.assertEqual(fdec["delta_q_exec"], 0)

        # NO-NEW-RISK allows risk reductions when q_pre exceeds target; ORDER-UNKNOWN blocks all orders
        s_inp_over = json.loads(json.dumps(_valid_inputs()))
        s_inp_over["funds"]["TQQQ"]["q_pre"] = 1000
        s_inp_over["funds"]["SOXL"]["q_pre"] = 1000
        out_reduce = compute_session_sizing(s_inp_over, active_control_state="NO-NEW-RISK")
        balanced_red = next(t for t in out_reduce["tiers"] if t["tier_id"] == "balanced")
        for fdec in balanced_red["funds"]:
            self.assertLess(fdec["q_ideal"], fdec["q_pre"])
            self.assertLess(fdec["delta_q_exec"], 0)

        out_ou = compute_session_sizing(s_inp_over, active_control_state="ORDER-UNKNOWN")
        balanced_ou = next(t for t in out_ou["tiers"] if t["tier_id"] == "balanced")
        for fdec in balanced_ou["funds"]:
            self.assertEqual(fdec["delta_q_exec"], 0)

        # End-to-end build_record_body isolates per-fund V4 NO-NEW-RISK on TQQQ without blocking SOXL
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        inp_per_fund = json.loads(json.dumps(_valid_inputs()))
        inp_per_fund["funds"]["TQQQ"]["q_pre"] = 0
        inp_per_fund["funds"]["TQQQ"]["sigma_forecast"] = "0.50"  # trips V4 NO-NEW-RISK on TQQQ only
        inp_per_fund["funds"]["SOXL"]["q_pre"] = 0
        inp_per_fund["funds"]["SOXL"]["sigma_forecast"] = "0.22"
        built = build_record_body(
            chain_id="prod-per-fund-v4",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=inp_per_fund,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=VendoredFuseEvaluator(),
            calendar_spec=spec,
        )
        self.assertTrue(built["appended"])
        rec = built["record_body"]
        balanced_rec = next(t for t in rec["tiers"] if t["tier_id"] == "balanced")
        by_sym = {f["instrument"]: f for f in balanced_rec["funds"]}
        self.assertEqual(by_sym["TQQQ"]["q_ideal"], 0)
        self.assertEqual(by_sym["TQQQ"]["delta_q_exec"], 0)
        self.assertGreater(by_sym["SOXL"]["q_ideal"], 0)
        self.assertGreater(by_sym["SOXL"]["delta_q_exec"], 0)


if __name__ == "__main__":
    unittest.main()
