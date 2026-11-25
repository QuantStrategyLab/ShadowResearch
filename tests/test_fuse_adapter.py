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
                "long_run_volatility": "0.24",
                "modelled_fee": 0.0,
                "modelled_financing": 0.0,
                "fund_return": 0.0,
                "index_return": 0.0,
            },
            "SOXL": {
                "s": "1",
                "close_price": "40",
                "q_pre": 100,
                "sigma_forecast": "0.22",
                "long_run_volatility": "0.24",
                "modelled_fee": 0.0,
                "modelled_financing": 0.0,
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

    def test_shadow_optional_hwm_and_source_records_translate_without_native_misclassification(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        ev = VendoredFuseEvaluator()

        shadow_opt = json.loads(json.dumps(_valid_inputs()))
        shadow_opt["high_water_mark"] = "50000"
        shadow_opt["source_records"] = {
            "positions": {"as_of": "2026-01-05T22:00:00.000Z", "age_sessions": 0},
            "broker_reconciliation": {"as_of": "2026-01-05T22:00:00.000Z", "age_sessions": 0},
        }
        del shadow_opt["positions_source_record"]
        del shadow_opt["broker_reconciliation_source_record"]

        vin = build_fuse_validated_inputs(
            shadow_opt,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        rows, _, _ = ev.evaluate(
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
        self.assertEqual(rows["O1"]["state"], "NORMAL")
        self.assertEqual(rows["O2"]["state"], "NORMAL")

        built = build_record_body(
            chain_id="prod-opt-fields",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=shadow_opt,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            calendar_spec=spec,
        )
        self.assertTrue(built["appended"])

        # Explicit native FUSE pass-through still works via fuse_inputs or native schema without E
        vin_native = build_fuse_validated_inputs(
            {"fuse_inputs": vin},
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertEqual(vin_native["bankroll"], 50000.0)
        direct_native = {k: v for k, v in vin.items() if k not in ("loaded_policy_hash", "registered_policy_hash")}
        vin_direct = build_fuse_validated_inputs(
            direct_native,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertEqual(vin_direct["loaded_policy_hash"], "p")

    def test_per_tier_q_pre_step3_max_loss_cap_and_fund_hwm_preserved(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        ev = VendoredFuseEvaluator()

        # 1. Per-tier q_pre mapping resolves selected tier cleanly (and fails closed if tier absent)
        tiered = json.loads(json.dumps(_valid_inputs()))
        tiered["funds"]["TQQQ"]["q_pre"] = {"conservative": 50, "balanced": 100, "aggressive": 150}
        tiered["funds"]["SOXL"]["q_pre"] = {"conservative": 50, "balanced": 100, "aggressive": 150}
        vin_tiered = build_fuse_validated_inputs(
            tiered,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertEqual(vin_tiered["funds"]["TQQQ"]["share_count"], 100)
        self.assertEqual(vin_tiered["funds"]["TQQQ"]["broker_position"], 100)
        rows_tiered, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_tiered,
            {},
            None,
        )
        self.assertEqual(rows_tiered["O1"]["state"], "NORMAL")
        self.assertEqual(rows_tiered["O2"]["state"], "NORMAL")
        built_tiered = build_record_body(
            chain_id="prod-tiered-qpre",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=tiered,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            calendar_spec=spec,
        )
        self.assertTrue(built_tiered["appended"])

        missing_tier = json.loads(json.dumps(tiered))
        missing_tier["funds"]["TQQQ"]["q_pre"] = {"conservative": 50}
        built_missing = build_record_body(
            chain_id="prod-missing-tier-qpre",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:00:00.000Z",
            session_inputs=missing_tier,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            calendar_spec=spec,
        )
        self.assertFalse(built_missing["appended"])
        self.assertTrue(built_missing["o1_output"]["would_have_tripped"])

        # 2. Default max_loss_cap comes from sizing Step 3 x_ML * E (~$10,064.44 for balanced $50k),
        # so $15,000 TQQQ exposure trips L0 even though it is below x_joint * E ($20,000).
        l0_case = json.loads(json.dumps(_valid_inputs()))
        l0_case["funds"]["TQQQ"]["q_pre"] = 300
        l0_case["funds"]["SOXL"]["q_pre"] = 0
        vin_l0 = build_fuse_validated_inputs(
            l0_case,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        sizing_bal = next(
            t for t in compute_session_sizing(_valid_inputs())["tiers"] if t["tier_id"] == "balanced"
        )
        self.assertAlmostEqual(
            vin_l0["max_loss_cap"],
            float(sizing_bal["x_ML"]) * 50000.0,
            places=2,
        )
        self.assertLess(vin_l0["max_loss_cap"], 15000.0)
        rows_l0, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_l0,
            {},
            None,
        )
        self.assertTrue(rows_l0["L0"]["outputs"]["cap_breached"])
        self.assertTrue(rows_l0["L0"]["would_have_tripped"])

        # 3. Fund-level hwm_adjusted=False is preserved even when corporate_action.hwm_adjusted=True
        ca_fund_false = json.loads(json.dumps(_valid_inputs()))
        ca_fund_false["funds"]["TQQQ"]["hwm_adjusted"] = False
        ca_fund_false["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split",
            "ratio": 2,
            "adjustment_complete": True,
            "exact_reconciliation": True,
            "hwm_adjusted": True,
        }
        vin_ca = build_fuse_validated_inputs(
            ca_fund_false,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIs(vin_ca["funds"]["TQQQ"]["hwm_adjusted"], False)
        rows_ca, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_ca,
            {},
            None,
        )
        self.assertEqual(rows_ca["V2"]["state"], "HALT")
        self.assertTrue(rows_ca["V2"]["would_have_tripped"])
        self.assertEqual(rows_ca["V2"]["reason"], "reconciliation_mismatch")

    def test_split_adjusted_fund_return_and_l4_one_session_loss_derivation(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        # 1. Reconciled 2-for-1 TQQQ split (50 -> 25) with omitted fund_return keeps V1 & V2 NORMAL
        split_in = json.loads(json.dumps(_valid_inputs()))
        split_in["prior_closes"] = {"TQQQ": "50", "SOXL": "40", "XNDX": "20000", "SEMI": "5000"}
        split_in["current_index_closes"] = {"XNDX": "20000", "SEMI": "5000"}
        split_in["funds"]["TQQQ"] = {
            "s": "1",
            "close_price": "25",
            "q_pre": 200,
            "sigma_forecast": "0.24",
            "long_run_volatility": "0.24",
            "modelled_fee": 0.0,
            "modelled_financing": 0.0,
            "hwm_adjusted": True,
            "corporate_action": {
                "type": "split",
                "ratio": 2,
                "adjustment_complete": True,
                "exact_reconciliation": True,
                "hwm_adjusted": True,
            },
        }
        del split_in["funds"]["SOXL"]["fund_return"]
        del split_in["funds"]["SOXL"]["index_return"]
        del split_in["index_one_session_returns"]
        vin_split = build_fuse_validated_inputs(
            split_in,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertAlmostEqual(vin_split["funds"]["TQQQ"]["fund_return"], 0.0)
        rows_split, _, _ = ev.evaluate(
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
        self.assertEqual(rows_split["V1"]["state"], "NORMAL")
        self.assertEqual(rows_split["V2"]["state"], "NORMAL")

        # 2. prior_E=50000 -> E=48000 (no external flow) derives one_session_loss=0.04 and trips L4
        l4_loss_in = json.loads(json.dumps(_valid_inputs()))
        l4_loss_in["prior_E"] = "50000"
        l4_loss_in["E"] = "48000"
        l4_loss_in["E_star"] = "50000"
        l4_loss_in["L"] = "2000"
        l4_loss_in["high_water_mark"] = "50000"
        vin_l4 = build_fuse_validated_inputs(
            l4_loss_in,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertAlmostEqual(vin_l4["one_session_loss"], 0.04)
        rows_l4, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_l4,
            {},
            None,
        )
        self.assertEqual(rows_l4["L4"]["state"], "NO-NEW-RISK")
        self.assertTrue(rows_l4["L4"]["latched"])

        # 3. Omitting prior_E when L > 0 (not provably zero-loss genesis) fails closed via missing_input
        missing_prior_in = json.loads(json.dumps(_valid_inputs()))
        missing_prior_in["E"] = "48000"
        missing_prior_in["E_star"] = "50000"
        missing_prior_in["L"] = "2000"
        vin_missing = build_fuse_validated_inputs(
            missing_prior_in,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin_missing["one_session_loss"])
        rows_missing, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin_missing,
            {},
            None,
        )
        self.assertEqual(rows_missing["L4"]["state"], "NO-NEW-RISK")
        self.assertEqual(rows_missing["L4"]["reason"], "missing_input")

    def test_o9_change_signals_forwarded_and_list_authorized_events_preserved(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        si_tier = _valid_inputs()
        si_tier["tier"] = "GUARD"
        si_tier["tier_change_event"] = {"from": "CORE", "to": "GUARD"}
        rows_tier, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_tier,
                cal,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            {},
            None,
        )
        self.assertEqual(rows_tier["O9"]["state"], "HALT")
        self.assertTrue(rows_tier["O9"]["would_have_tripped"])
        self.assertEqual(rows_tier["O9"]["reason"], "unauthorized_change")

        si_desc = _valid_inputs()
        si_desc["o9_change_descriptors"] = [
            {"param": "single_fund_cap", "to": 0.45, "scope": "fund:TQQQ"}
        ]
        rows_desc, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_desc,
                cal,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            {},
            None,
        )
        self.assertEqual(rows_desc["O9"]["state"], "HALT")
        self.assertTrue(rows_desc["O9"]["would_have_tripped"])
        self.assertEqual(rows_desc["O9"]["reason"], "unauthorized_change")

        si_clean = _valid_inputs()
        prior_v1_latched = {
            "V1": {
                "TQQQ": {
                    "latched": True,
                    "trip_session": "2026-01-05",
                    "deviation": 0.08,
                    "session_returns": [],
                }
            }
        }
        rows_no_rearm, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_clean,
                cal,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            prior_v1_latched,
            [],
        )
        self.assertEqual(rows_no_rearm["V1"]["state"], "NO-NEW-RISK")
        self.assertEqual(rows_no_rearm["V1"]["reason"], "latched_prior_trip")

        rows_list_rearm, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_clean,
                cal,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            prior_v1_latched,
            [
                {
                    "type": "rearm",
                    "row": "V1",
                    "fund": "TQQQ",
                    "scope": "fund:TQQQ",
                    "session": "2026-01-06",
                }
            ],
        )
        self.assertEqual(rows_list_rearm["V1"]["state"], "NORMAL")
        self.assertFalse(rows_list_rearm["V1"]["would_have_tripped"])

    def test_joint_exposure_scaling_o7_silence_flags_and_stale_calendar_rejected(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        # 1. Joint exposure is scaled into sizing-stack units (3x * beta / conv_scale)
        si_exp = _valid_inputs()
        vin_exp = build_fuse_validated_inputs(
            si_exp,
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertAlmostEqual(vin_exp["joint_exposure"], 33000.0, places=6)
        rows_exp, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            vin_exp,
            {},
            None,
        )
        self.assertTrue(rows_exp["J1"]["would_have_tripped"])
        self.assertEqual(rows_exp["J1"]["reason"], "joint_cap_exceeded")
        self.assertTrue(rows_exp["L0"]["would_have_tripped"])
        self.assertTrue(rows_exp["L0"]["outputs"]["cap_breached"])

        si_conv = _valid_inputs()
        si_conv["H"] = "10000"
        vin_conv = build_fuse_validated_inputs(
            si_conv,
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertAlmostEqual(vin_conv["joint_exposure"], 27500.0, places=6)

        # 2. Explicit O7 runtime/monitor silence signals trip O7 even with fresh heartbeat
        for flag in ("o7_runtime_silent", "o7_monitor_silent"):
            si_o7 = _valid_inputs()
            si_o7["last_heartbeat"] = "2026-01-06T22:00:00.000Z"
            si_o7[flag] = True
            vin_o7 = build_fuse_validated_inputs(
                si_o7,
                cal6,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            )
            rows_o7, _, _ = ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-06",
                "2026-01-06T22:00:00.000Z",
                "shadow",
                vin_o7,
                {},
                None,
            )
            self.assertEqual(rows_o7["O7"]["state"], "NO-NEW-RISK")
            self.assertTrue(rows_o7["O7"]["would_have_tripped"])
            self.assertEqual(rows_o7["O7"]["reason"], "dead_mans_switch")

        # 3. Stale calendar_session from another trade_date fails closed in O2 & O1
        cal_prev = resolve_session("XNYS", "2026-01-16", spec=spec)
        si_cal = _valid_inputs()
        vin_stale_cal = build_fuse_validated_inputs(
            si_cal,
            cal_prev,
            policy_hash="p",
            session_date="2026-01-19",
            as_of="2026-01-19T22:00:00.000Z",
        )
        self.assertIsNone(vin_stale_cal["session_calendar_status"])
        self.assertIsNone(vin_stale_cal["exchange_status"])
        self.assertNotIn("session_calendar", vin_stale_cal["source_records"])
        rows_stale_cal, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-19",
            "2026-01-19T22:00:00.000Z",
            "shadow",
            vin_stale_cal,
            {},
            None,
        )
        self.assertTrue(rows_stale_cal["O2"]["would_have_tripped"])
        self.assertTrue(rows_stale_cal["O1"]["would_have_tripped"])

    def test_o5_order_evidence_and_v1_five_session_history_forwarded(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        # 1. Pre-trade O5 order fields trigger violation when violating whitelist & pass when compliant
        si_o5_bad = _valid_inputs()
        si_o5_bad.update(
            {
                "o5_collar": "0.05",
                "o5_symbol": "UVXY",
                "o5_order_size": 10,
                "o5_side": "buy",
                "o5_account": "stage_n_shadow",
            }
        )
        rows_o5_bad, _, res_o5_bad = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_o5_bad,
                cal6,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            {},
            None,
        )
        self.assertTrue(rows_o5_bad["O5"]["would_have_tripped"])
        self.assertEqual(rows_o5_bad["O5"]["reason"], "pre_trade_limit_violation")
        self.assertEqual(res_o5_bad["O5"], {"rejection_count": 1, "last_rejection_session": "2026-01-06", "latched": False})

        si_o5_ok = _valid_inputs()
        si_o5_ok.update(
            {
                "o5_collar": "0.05",
                "o5_symbol": "TQQQ",
                "o5_order_size": 10,
                "o5_side": "buy",
                "o5_limit_price": "50",
                "o5_reference_price": "50",
                "o5_account": "stage_n_shadow",
            }
        )
        rows_o5_ok, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_o5_ok,
                cal6,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            {},
            None,
        )
        self.assertFalse(rows_o5_ok["O5"]["would_have_tripped"])

        # 2. Bootstrapped five_session_history is forwarded to V1 tracking deviation
        si_v1 = _valid_inputs()
        si_v1["funds"]["TQQQ"].update(
            {
                "fund_return": "0.005",
                "index_return": "0.0",
                "five_session_history": [
                    {"fund_return": 0.005, "index_return": 0.0} for _ in range(4)
                ],
            }
        )
        rows_v1, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            build_fuse_validated_inputs(
                si_v1,
                cal6,
                policy_hash="p",
                session_date="2026-01-06",
                as_of="2026-01-06T22:00:00.000Z",
            ),
            {},
            None,
        )
        self.assertEqual(rows_v1["V1"]["state"], "NO-NEW-RISK")
        self.assertTrue(rows_v1["V1"]["latched"])
        self.assertEqual(rows_v1["V1"]["reason"], "tracking_deviation_exceeds_2_0pp")

    def test_fractional_broker_position_and_market_data_validation_preserved(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        # 1. Fractional broker_position is preserved (not truncated to int) so O3 catches mismatch
        si_frac = _valid_inputs()
        si_frac["funds"]["TQQQ"]["broker_position"] = "100.9"
        vin_frac = build_fuse_validated_inputs(
            si_frac,
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertEqual(vin_frac["funds"]["TQQQ"]["share_count"], 100)
        self.assertAlmostEqual(vin_frac["funds"]["TQQQ"]["broker_position"], 100.9, places=6)
        rows_frac, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            vin_frac,
            {},
            None,
        )
        self.assertEqual(rows_frac["O3"]["state"], "HALT")
        self.assertEqual(rows_frac["O3"]["reason"], "reconciliation_mismatch")

        # 2. Missing required market_record fails O1 closed even when caller supplies source_records.market_data
        si_mkt = _valid_inputs()
        cal_digest = str(cal6["spec_digest"])
        si_mkt["market_records"] = [
            {
                "instrument": inst,
                "normalized_value": 50.0,
                "calendar_spec_digest": cal_digest,
                "eligibility": {"eligible": True},
            }
            for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR")
        ]
        si_mkt["source_records"] = {
            "market_data": {"as_of": "2026-01-06T22:00:00.000Z", "age_sessions": 0},
            "session_calendar": {"as_of": "2026-01-06T22:00:00.000Z", "age_sessions": 0},
        }
        vin_mkt = build_fuse_validated_inputs(
            si_mkt,
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertNotIn("market_data", vin_mkt["source_records"])
        rows_mkt, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            vin_mkt,
            {},
            None,
        )
        self.assertTrue(rows_mkt["O1"]["would_have_tripped"])

    def test_v6_missing_observations_not_fabricated_and_per_tier_controls_isolated(self) -> None:
        ev = VendoredFuseEvaluator()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec_v2)

        # 1. PRRT_kwDOV2RNAc67rRKk: omitting V6 observations keeps them missing_input (no fabricated 0.0 / 0.04)
        vin_missing_v6 = build_fuse_validated_inputs(
            _valid_inputs(),
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertIsNone(vin_missing_v6["funds"]["TQQQ"]["carrying_cost_measured"])
        self.assertIsNone(vin_missing_v6["funds"]["TQQQ"]["excess_return_shrunk"])
        rows_v6, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            vin_missing_v6,
            {},
            None,
        )
        self.assertEqual(rows_v6["V6"]["reason"], "missing_input")
        self.assertTrue(rows_v6["V6"]["would_have_tripped"])
        self.assertTrue(rows_v6["V6"]["outputs"]["notify"])

        # 2. PRRT_kwDOV2RNAc67rRKh: E=44000, E_star=50000, L=6000, prior_E=44000, q_pre=0
        # balanced stays NORMAL and buys, while conservative latches L2 (10% threshold) and blocks increases.
        si_tier = _valid_inputs()
        si_tier.update(
            {
                "E": "44000",
                "E_star": "50000",
                "H": "0",
                "L": "6000",
                "prior_E": "44000",
                "tier": "balanced",
            }
        )
        for sym in ("TQQQ", "SOXL"):
            si_tier["funds"][sym]["q_pre"] = 0
        si_tier["positions_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si_tier["broker_reconciliation_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si_tier["market_records"] = [
            {
                "instrument": inst,
                "normalized_value": 50.0,
                "calendar_spec_digest": str(cal6["spec_digest"]),
                "eligibility": {"eligible": True},
            }
            for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR", "TBILL_3M")
        ]

        code_sha = compute_code_manifest_hash()
        built = build_record_body(
            chain_id="drill-tier-iso-v1",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-06",
            decision_timestamp_utc="2026-01-06T22:00:00.000Z",
            session_inputs=si_tier,
            calendar_session=cal6,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
            calendar_spec=spec_v2,
        )
        self.assertTrue(built["appended"])
        rb = built["record_body"]
        by_tier_ev = rb["evaluator"]["by_tier"]
        self.assertEqual(by_tier_ev["balanced"]["effective_state_by_scope"]["account"], "NORMAL")
        self.assertEqual(by_tier_ev["conservative"]["effective_state_by_scope"]["account"], "NO-NEW-RISK")
        cons_l2 = next(b for b in by_tier_ev["conservative"]["breakers"] if b["id"] == "L2")
        self.assertEqual(cons_l2["state"], "NO-NEW-RISK")
        self.assertTrue(cons_l2["latched"])

        tiers_by_name = {t["tier_id"]: t for t in rb["tiers"]}
        bal_funds = {f["instrument"]: f for f in tiers_by_name["balanced"]["funds"]}
        con_funds = {f["instrument"]: f for f in tiers_by_name["conservative"]["funds"]}
        self.assertGreater(int(bal_funds["TQQQ"]["q_ideal"]), 0)
        self.assertEqual(int(con_funds["TQQQ"]["q_ideal"]), 0)
        self.assertEqual(int(con_funds["SOXL"]["q_ideal"]), 0)

    def test_explicit_fuse_inputs_evaluated_per_tier_for_sizing(self) -> None:
        ev = VendoredFuseEvaluator()
        spec_v2 = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec_v2)

        si_base = _valid_inputs()
        si_base.update(
            {
                "E": "44000",
                "E_star": "50000",
                "H": "0",
                "L": "6000",
                "prior_E": "44000",
                "tier": "balanced",
            }
        )
        for sym in ("TQQQ", "SOXL"):
            si_base["funds"][sym]["q_pre"] = 0
        si_base["positions_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si_base["broker_reconciliation_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si_base["market_records"] = [
            {
                "instrument": inst,
                "normalized_value": 50.0,
                "calendar_spec_digest": str(cal6["spec_digest"]),
                "eligibility": {"eligible": True},
            }
            for inst in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI", "SOFR", "TBILL_3M")
        ]
        balanced_native = build_fuse_validated_inputs(
            si_base,
            cal6,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        wrapped_inputs = dict(si_base)
        wrapped_inputs["fuse_inputs"] = balanced_native

        built = build_record_body(
            chain_id="drill-wrapped-tier-v1",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2026-01-06",
            decision_timestamp_utc="2026-01-06T22:00:00.000Z",
            session_inputs=wrapped_inputs,
            calendar_session=cal6,
            code_version_sha=compute_code_manifest_hash(),
            evaluator=ev,
            prior_row_state={},
            calendar_spec=spec_v2,
        )
        self.assertTrue(built["appended"])
        rb = built["record_body"]
        by_tier = rb["evaluator"]["by_tier"]
        self.assertEqual(by_tier["balanced"]["effective_state_by_scope"]["account"], "NORMAL")
        self.assertEqual(by_tier["conservative"]["effective_state_by_scope"]["account"], "NO-NEW-RISK")
        cons_l2 = next(b for b in by_tier["conservative"]["breakers"] if b["id"] == "L2")
        self.assertTrue(cons_l2["latched"])
        self.assertEqual(cons_l2["state"], "NO-NEW-RISK")

        tiers_by_name = {t["tier_id"]: t for t in rb["tiers"]}
        bal_funds = {f["instrument"]: f for f in tiers_by_name["balanced"]["funds"]}
        con_funds = {f["instrument"]: f for f in tiers_by_name["conservative"]["funds"]}
        self.assertGreater(int(bal_funds["TQQQ"]["q_ideal"]), 0)
        self.assertEqual(int(con_funds["TQQQ"]["q_ideal"]), 0)
        self.assertEqual(int(con_funds["SOXL"]["q_ideal"]), 0)

    def test_v4_missing_or_invalid_long_run_volatility_fails_closed_and_blocks_increases(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        ev = VendoredFuseEvaluator()

        # Omitted long_run_volatility with zero holdings MUST fail closed V4 (NO-NEW-RISK, missing_input) and block buys
        for bad_lrv in (None, "bad", float("nan"), 0, "-0.1"):
            si = json.loads(json.dumps(_valid_inputs()))
            for sym in ("TQQQ", "SOXL"):
                si["funds"][sym]["q_pre"] = 0
                if bad_lrv is None:
                    si["funds"][sym].pop("long_run_volatility", None)
                else:
                    si["funds"][sym]["long_run_volatility"] = bad_lrv
            translated = build_fuse_validated_inputs(
                si,
                cal,
                policy_hash="p",
                session_date="2026-01-05",
                as_of="2026-01-05T22:00:00.000Z",
            )
            if bad_lrv is None or bad_lrv in ("bad",) or (isinstance(bad_lrv, float) and bad_lrv != bad_lrv):
                self.assertIsNone(translated["funds"]["TQQQ"]["long_run_volatility"])
                self.assertIsNone(translated["funds"]["SOXL"]["long_run_volatility"])
            else:
                self.assertLessEqual(float(translated["funds"]["TQQQ"]["long_run_volatility"]), 0.0)

            built = build_record_body(
                chain_id="drill-v4-missing-lrv",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2026-01-05",
                decision_timestamp_utc="2026-01-05T22:00:00.000Z",
                session_inputs=si,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state={},
                calendar_spec=spec,
            )
            self.assertTrue(built["appended"])
            rec = built["record_body"]
            v4_row = next(b for b in rec["evaluator"]["breakers"] if b["id"] == "V4")
            self.assertEqual(v4_row["state"], "NO-NEW-RISK")
            self.assertEqual(v4_row["reason"], "missing_input")
            self.assertTrue(v4_row["would_have_tripped"])
            balanced_rec = next(t for t in rec["tiers"] if t["tier_id"] == "balanced")
            for fdec in balanced_rec["funds"]:
                self.assertEqual(fdec["q_ideal"], 0)
                self.assertEqual(fdec["delta_q_exec"], 0)

    def test_v5_and_v7_missing_observations_fail_closed_and_preserve_v5_counter(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-06", spec=spec)
        code_sha = compute_code_manifest_hash(_REPO_ROOT, calendar_version="csa22-v2")
        ev = VendoredFuseEvaluator()

        # Resolved v5_tolerance + v7_spread_width with omitted observations MUST fail closed as missing_input
        si = json.loads(json.dumps(_valid_inputs()))
        si["positions_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si["broker_reconciliation_source_record"]["as_of"] = "2026-01-06T22:00:00.000Z"
        si["funds"]["TQQQ"]["v5_tolerance"] = 0.001
        si["funds"]["SOXL"]["v5_tolerance"] = 0.001
        si["funds"]["TQQQ"]["v7_spread_width"] = 0.01
        si["funds"]["SOXL"]["v7_spread_width"] = 0.01
        prior_state = {
            "V5": {
                "TQQQ": {
                    "consecutive_beyond_tolerance": 39,
                    "last_session": "2026-01-05",
                    "escalated": False,
                }
            }
        }
        translated = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        for sym in ("TQQQ", "SOXL"):
            self.assertIsNone(translated["funds"][sym]["shortfall_observed"])
            self.assertIsNone(translated["funds"][sym]["shortfall_predicted"])
            self.assertIsNone(translated["funds"][sym]["quote_staleness"])
            self.assertIsNone(translated["funds"][sym]["spread_width"])
            self.assertIsNone(translated["funds"][sym]["trading_pause"])

        built = build_record_body(
            chain_id="drill-v5-v7-missing-obs",
            sequence=1,
            prev_record_sha256="a" * 64,
            mode="drill",
            session_date="2026-01-06",
            decision_timestamp_utc="2026-01-06T22:00:00.000Z",
            session_inputs=si,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state=prior_state,
            calendar_spec=spec,
        )
        self.assertTrue(built["appended"])
        v5_row = next(
            b for b in built["record_body"]["evaluator"]["breakers"] if b["id"] == "V5"
        )
        v7_row = next(
            b for b in built["record_body"]["evaluator"]["breakers"] if b["id"] == "V7"
        )
        self.assertTrue(v5_row["would_have_tripped"])
        self.assertEqual(v5_row["reason"], "missing_input")
        self.assertEqual(
            built["next_row_state"]["V5"]["TQQQ"]["consecutive_beyond_tolerance"],
            39,
        )
        self.assertTrue(v7_row["would_have_tripped"])
        self.assertEqual(v7_row["reason"], "missing_input")

    def test_v1_missing_fee_or_financing_fails_closed(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        for missing_key in ("modelled_fee", "modelled_financing"):
            si = json.loads(json.dumps(_valid_inputs()))
            si["funds"]["TQQQ"].pop(missing_key, None)
            vin = build_fuse_validated_inputs(
                si,
                cal,
                policy_hash="p",
                session_date="2026-01-05",
                as_of="2026-01-05T22:00:00.000Z",
            )
            self.assertIsNone(vin["funds"]["TQQQ"][missing_key])
            rows, _, nxt = ev.evaluate(
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
            self.assertEqual(rows["V1"]["state"], "NO-NEW-RISK")
            self.assertEqual(rows["V1"]["reason"], "missing_input")
            self.assertEqual(nxt.get("V1", {}).get("TQQQ", {}).get("five_session_history", []), [])

    def test_l6_benchmark_returns_derived_independently_of_missing_fund_return(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si.pop("index_one_session_returns", None)
        si["prior_closes"] = {"XNDX": "100", "SEMI": "100", "SOXL": "40"}  # omit TQQQ prior close
        si["current_index_closes"] = {"XNDX": "90", "SEMI": "100"}
        si["funds"]["TQQQ"].pop("fund_return", None)
        si["funds"]["TQQQ"].pop("index_return", None)
        si["funds"]["SOXL"]["fund_return"] = 0.0
        si["funds"]["SOXL"]["index_return"] = 0.0

        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin["funds"]["TQQQ"]["fund_return"])
        self.assertIsNotNone(vin["index_one_session_returns"])
        self.assertAlmostEqual(vin["index_one_session_returns"]["NDX"], -0.1, places=6)
        self.assertAlmostEqual(vin["index_one_session_returns"]["SEMI"], 0.0, places=6)

        rows, _, nxt = ev.evaluate(
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
        self.assertEqual(rows["L6"]["state"], "HALT")
        self.assertTrue(rows["L6"]["latched"])
        self.assertEqual(rows["L6"]["reason"], "index_fall_10pct")
        self.assertTrue(nxt["L6"]["latched"])

    def test_l6_partial_benchmark_crash_latches_halt_when_other_benchmark_missing(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si.pop("index_one_session_returns", None)
        si["prior_closes"] = {"TQQQ": "50", "SOXL": "40", "XNDX": "100", "SEMI": "200"}
        si["current_index_closes"] = {"XNDX": "90"}  # SEMI missing/ineligible

        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNotNone(vin["index_one_session_returns"])
        self.assertAlmostEqual(vin["index_one_session_returns"]["NDX"], -0.1, places=6)
        self.assertNotIn("SEMI", vin["index_one_session_returns"])

        rows, _, nxt = ev.evaluate(
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
        self.assertEqual(rows["L6"]["state"], "HALT")
        self.assertTrue(rows["L6"]["latched"])
        self.assertEqual(rows["L6"]["reason"], "index_fall_10pct")
        self.assertTrue(nxt["L6"]["latched"])


if __name__ == "__main__":
    unittest.main()
