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
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "SOXL",
                    "normalized_value": "40",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "QQQ",
                    "normalized_value": "445",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "XNDX",
                    "normalized_value": "89",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "SEMI",
                    "normalized_value": "100",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "SOFR",
                    "normalized_value": "0.045",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-02",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "TBILL_3M",
                    "normalized_value": "0.044",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
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
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
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
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                    "corporate_action": {"split_ratio": "1", "cash_per_share_usd": "0", "ex_date": "2026-01-05"},
                },
                {
                    "instrument": "QQQ",
                    "normalized_value": "500",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "XNDX",
                    "normalized_value": "20000",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "SEMI",
                    "normalized_value": "5000",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "SOFR",
                    "normalized_value": "0.045",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-02",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                },
                {
                    "instrument": "TBILL_3M",
                    "normalized_value": "0.044",
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal["spec_digest"],
                    "effective_session": "2026-01-05",
                    "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
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
                "ex_date": "2026-01-05",
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

    def test_valid_compiled_pycache_is_accepted_without_inherited_future_flags(self) -> None:
        import marshal
        import py_compile

        with tempfile.TemporaryDirectory() as tmpdir:
            copy_dir = Path(tmpdir) / "vendor"
            shutil.copytree(DEFAULT_VENDOR_ENGINE_ROOT, copy_dir)
            src = copy_dir / "fuse_engine" / "__init__.py"
            pyc_path = Path(importlib.util.cache_from_source(str(src)))
            py_compile.compile(str(src), cfile=str(pyc_path), doraise=True)
            verify_vendored_engine_bundle(copy_dir)

            # Same-size bytecode for altered source (changed docstring constant) is still rejected.
            src_bytes = src.read_bytes()
            altered = src_bytes.replace(b"FUSE-1 reference engine", b"FUSE-X reference engine", 1)
            self.assertNotEqual(altered, src_bytes)
            header = pyc_path.read_bytes()[:16]
            pyc_path.write_bytes(header + marshal.dumps(compile(altered, str(src), "exec", dont_inherit=True)))
            with self.assertRaises(EngineIdentityError):
                verify_vendored_engine_bundle(copy_dir)

    def test_o5_settled_cash_alias_and_missing_v7_session_time_are_not_defaulted(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        def _rows(si: dict) -> dict:
            rows, _, _ = ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-06",
                "2026-01-06T22:00:00.000Z",
                "shadow",
                build_fuse_validated_inputs(
                    si,
                    cal6,
                    policy_hash="p",
                    session_date="2026-01-06",
                    as_of="2026-01-06T22:00:00.000Z",
                ),
                {},
                None,
            )
            return rows

        o5_order = {
            "settled_cash": "50000",
            "o5_collar": "0.05",
            "o5_symbol": "TQQQ",
            "o5_order_size": 10,
            "o5_side": "buy",
            "o5_limit_price": "50",
            "o5_reference_price": "50",
            "o5_account": "stage_n_shadow",
        }
        si_cash_ok = _valid_inputs()
        si_cash_ok.update(o5_order)
        self.assertFalse(_rows(si_cash_ok)["O5"]["would_have_tripped"])

        si_no_cash = _valid_inputs()
        si_no_cash.update(o5_order)
        si_no_cash.pop("settled_cash")
        si_no_cash["o5_settled_cash"] = "0"
        rows_no_cash = _rows(si_no_cash)
        self.assertTrue(rows_no_cash["O5"]["would_have_tripped"])
        self.assertEqual(rows_no_cash["O5"]["reason"], "pre_trade_limit_violation")

        for null_key in ("settled_cash", "o5_settled_cash"):
            si_null_cash = _valid_inputs()
            si_null_cash.update(o5_order)
            si_null_cash.pop("settled_cash")
            si_null_cash[null_key] = None
            rows_null_cash = _rows(si_null_cash)
            self.assertTrue(rows_null_cash["O5"]["would_have_tripped"], null_key)
            self.assertEqual(rows_null_cash["O5"]["reason"], "pre_trade_limit_violation", null_key)

        v7_obs = {"quote_staleness": 0.0, "spread_width": 0.001, "trading_pause": False}
        si_v7_regular = _valid_inputs()
        si_v7_regular["v7_spread_width"] = "0.01"
        for fund in si_v7_regular["funds"].values():
            fund.update(v7_obs, session_time="regular")
        self.assertFalse(_rows(si_v7_regular)["V7"]["would_have_tripped"])

        si_v7_missing = _valid_inputs()
        si_v7_missing["v7_spread_width"] = "0.01"
        for fund in si_v7_missing["funds"].values():
            fund.update(v7_obs)
        self.assertTrue(_rows(si_v7_missing)["V7"]["would_have_tripped"])

    def test_o5_counter_aliases_h_for_o9_and_retier_loss_are_not_manufactured(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        def _rows(si: dict, authorized_events: object = None) -> dict:
            rows, _, _ = ev.evaluate(
                ev.spec_version,
                ev.engine_hash,
                "p",
                "2026-01-06",
                "2026-01-06T22:00:00.000Z",
                "shadow",
                build_fuse_validated_inputs(
                    si,
                    cal6,
                    policy_hash="p",
                    session_date="2026-01-06",
                    as_of="2026-01-06T22:00:00.000Z",
                ),
                {},
                authorized_events,
            )
            return rows

        # O5: alias-only session counters must not collide with manufactured zero canonical counters.
        si_o5 = _valid_inputs()
        si_o5.update(
            {
                "o5_collar": "0.05",
                "o5_symbol": "TQQQ",
                "o5_order_size": 10,
                "o5_side": "buy",
                "o5_limit_price": "50",
                "o5_reference_price": "50",
                "o5_account": "stage_n_shadow",
                "settled_cash": "50000",
                "o5_orders_today": 1,
                "o5_max_orders_per_session": 10,
                "o5_turnover_today": 500,
                "o5_max_turnover_per_session": 10000,
            }
        )
        self.assertFalse(_rows(si_o5)["O5"]["would_have_tripped"])

        # O5: explicitly unknown canonical counters are preserved, not defaulted to zero.
        for null_counter, limit_key, limit_val in (
            ("session_turnover", "o5_max_turnover_per_session", 10000),
            ("session_order_count", "o5_max_orders_per_session", 10),
        ):
            si_null = _valid_inputs()
            si_null.update(
                {
                    "o5_collar": "0.05",
                    "o5_symbol": "TQQQ",
                    "o5_order_size": 10,
                    "o5_side": "buy",
                    "o5_limit_price": "50",
                    "o5_reference_price": "50",
                    "o5_account": "stage_n_shadow",
                    "settled_cash": "50000",
                    limit_key: limit_val,
                    null_counter: None,
                }
            )
            rows_null = _rows(si_null)
            self.assertTrue(rows_null["O5"]["would_have_tripped"], null_counter)
            self.assertEqual(rows_null["O5"]["reason"], "pre_trade_limit_violation", null_counter)

        # O9: an authorized H change is checked against the live H used for sizing.
        si_h = _valid_inputs()
        si_h["H"] = "10000"
        si_h["o9_change_descriptors"] = [{"param": "H", "to": "10000"}]
        card = {
            "type": "parameter_change_card",
            "session": "2026-01-06",
            "as_of": "2026-01-06T21:00:00.000Z",
            "change_descriptors": [{"param": "H", "to": "10000"}],
        }
        rows_h = _rows(si_h, {"account": [card]})
        self.assertEqual(rows_h["O9"]["state"], "NORMAL")
        self.assertFalse(rows_h["O9"]["would_have_tripped"])

        # Re-tiered explicit payload: a one-session return fraction is not cumulative dollar loss L.
        vin_retier = build_fuse_validated_inputs(
            {
                "tier": "balanced",
                "fuse_inputs": {
                    "tier": "aggressive",
                    "bankroll": 45000,
                    "high_water_mark": 50000,
                    "H": "0",
                    "one_session_loss": 0.01,
                },
            },
            None,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        self.assertEqual(vin_retier["tier"], "balanced")
        self.assertAlmostEqual(vin_retier["max_loss_cap"], 6382.293860109469, places=6)

    def test_astra_p1_market_records_ca_interval_and_uncoerced_controls(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal6 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()
        sd = "2026-01-06"
        as_of = "2026-01-06T22:00:00.000Z"

        def _vin(si: dict) -> dict:
            return build_fuse_validated_inputs(si, cal6, policy_hash="p", session_date=sd, as_of=as_of)

        def _rows(si: dict) -> dict:
            rows, _, _ = ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", _vin(si), {}, None)
            return rows

        closes = {"TQQQ": "50", "SOXL": "40", "QQQ": "500", "XNDX": "20000", "SEMI": "5000", "SOFR": "0.045", "TBILL_3M": "0.044"}

        def _records(eff: str, **overrides: dict) -> list[dict]:
            recs = []
            for inst, val in closes.items():
                rec = {
                    "instrument": inst,
                    "normalized_value": val,
                    "eligibility": {"eligible": True},
                    "calendar_spec_digest": cal6["spec_digest"],
                    # SOFR publishes T-1 (2026-01-05 for a 2026-01-06 decision).
                    "effective_session": "2026-01-05" if (inst == "SOFR" and eff == sd) else eff,
                    "retrieval_time_utc": "2026-01-06T21:30:00.000Z",
                }
                rec.update(overrides.get(inst, {}))
                recs.append(rec)
            return recs

        def _mkt_inputs(recs: list[dict]) -> dict:
            si = _valid_inputs()
            si.pop("index_one_session_returns", None)
            for f in si["funds"].values():
                f.pop("fund_return", None)
                f.pop("index_return", None)
            si["market_records"] = recs
            si["prior_closes"] = {"TQQQ": "50", "SOXL": "40", "XNDX": "20000", "SEMI": "5000", "SPX": "100"}
            return si

        # P1-3: a previous session's eligible record with the same calendar digest is not current evidence.
        vin_cur = _vin(_mkt_inputs(_records(sd)))
        self.assertEqual(vin_cur["funds"]["TQQQ"]["fund_return"], 0.0)
        vin_stale = _vin(_mkt_inputs(_records("2026-01-05")))
        self.assertIsNone(vin_stale["funds"]["TQQQ"]["fund_return"])
        self.assertIsNone(vin_stale["funds"]["TQQQ"]["index_return"])
        self.assertTrue(_rows(_mkt_inputs(_records("2026-01-05")))["O1"]["would_have_tripped"])
        # Rate series follow the feeder's exact freshness rule: SOFR at the T-1 publication day,
        # T-bill at the session day or T-1. Never a later date, and never an older archived rate.
        lagged_rates = _records(sd, SOFR={"effective_session": "2026-01-05"}, TBILL_3M={"effective_session": "2026-01-05"})
        self.assertEqual(_vin(_mkt_inputs(lagged_rates))["funds"]["TQQQ"]["fund_return"], 0.0)
        self.assertFalse(_rows(_mkt_inputs(lagged_rates))["O1"]["would_have_tripped"])
        for bad_rate in (
            {"SOFR": {"effective_session": "2026-01-07"}},
            {"SOFR": {"effective_session": sd}},
            {"SOFR": {"effective_session": "2025-12-15"}},
            {"TBILL_3M": {"effective_session": "2025-12-15"}},
        ):
            self.assertTrue(_rows(_mkt_inputs(_records(sd, **bad_rate)))["O1"]["would_have_tripped"], bad_rate)

        # P1(r2)-2: a record retrieved after the decision instant (or without a retrieval time) is
        # look-ahead evidence in replay and is rejected even if marked eligible.
        for retrieval in ("2026-01-06T22:00:00.001Z", None):
            look_ahead = _records(sd, TQQQ={"retrieval_time_utc": retrieval})
            self.assertIsNone(_vin(_mkt_inputs(look_ahead))["funds"]["TQQQ"]["fund_return"], retrieval)
            self.assertTrue(_rows(_mkt_inputs(look_ahead))["O1"]["would_have_tripped"], retrieval)

        # P1(r2)-6: the return uses the validated market close; a disagreeing caller close fails closed.
        mkt_close_in = _mkt_inputs(_records(sd, TQQQ={"normalized_value": "55"}))
        mkt_close_in["funds"]["TQQQ"].pop("close_price")
        self.assertAlmostEqual(_vin(mkt_close_in)["funds"]["TQQQ"]["fund_return"], 0.1)
        mkt_close_in["funds"]["TQQQ"]["close_price"] = "55"
        self.assertAlmostEqual(_vin(mkt_close_in)["funds"]["TQQQ"]["fund_return"], 0.1)
        mismatch_in = _mkt_inputs(_records(sd, TQQQ={"normalized_value": "55"}))
        mismatch_in["funds"]["TQQQ"]["close_price"] = "50"
        self.assertIsNone(_vin(mismatch_in)["funds"]["TQQQ"]["fund_return"])
        self.assertTrue(_rows(mismatch_in)["O1"]["would_have_tripped"])

        # P1(r2)-4: a fund-payload CA and a distinct new feeder CA on the same bar fail closed.
        ca_in = _mkt_inputs(
            _records(sd, TQQQ={"corporate_action": {"split_ratio": "2", "cash_per_share_usd": "0", "ex_date": sd}})
        )
        ca_in["funds"]["TQQQ"]["corporate_action"] = {
            "type": "distribution",
            "cash_per_share_usd": 0.5,
            "ex_date": "2026-01-02",
            "adjustment_complete": True,
            "exact_reconciliation": True,
            "hwm_adjusted": True,
        }
        ca_in["funds"]["TQQQ"]["hwm_adjusted"] = True
        # P1(r3)-1: rejected before any successor state (V2 never sees only one of the two).
        from shadow.fuse_adapter import CorporateActionConflictError

        with self.assertRaises(CorporateActionConflictError):
            _vin(ca_in)
        same_ca = _mkt_inputs(
            _records(sd, TQQQ={"corporate_action": {"split_ratio": "2", "cash_per_share_usd": "0", "ex_date": sd}})
        )
        same_ca["prior_closes"]["TQQQ"] = "100"
        same_ca["funds"]["TQQQ"]["corporate_action"] = {"type": "split", "ratio": 2.0, "ex_date": sd}
        self.assertAlmostEqual(_vin(same_ca)["funds"]["TQQQ"]["fund_return"], 0.0)

        # P1-4: an ineligible optional SPX bar must not derive an L6 index-crash return.
        spx_bad = _records(sd) + [
            {
                "instrument": "SPX",
                "normalized_value": "80",
                "eligibility": {"eligible": False, "reason": "stale_index_bar"},
                "calendar_spec_digest": cal6["spec_digest"],
                "effective_session": sd,
                "retrieval_time_utc": "2026-01-06T21:30:00.000Z",
            }
        ]
        vin_spx_bad = _vin(_mkt_inputs(spx_bad))
        self.assertNotIn("SPX", vin_spx_bad["index_one_session_returns"] or {})
        self.assertNotEqual(_rows(_mkt_inputs(spx_bad))["L6"]["state"], "HALT")
        spx_ok = _records(sd) + [
            {
                "instrument": "SPX",
                "normalized_value": "80",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal6["spec_digest"],
                "effective_session": sd,
                "retrieval_time_utc": "2026-01-06T21:30:00.000Z",
            }
        ]
        self.assertAlmostEqual(_vin(_mkt_inputs(spx_ok))["index_one_session_returns"]["SPX"], -0.2)

        # P1-5: a corporate action dated before the return interval does not re-adjust V1's return.
        def _split_inputs(ca: dict) -> dict:
            si = _valid_inputs()
            si["prior_closes"] = {"TQQQ": "25", "SOXL": "40", "XNDX": "20000", "SEMI": "5000"}
            si["funds"]["TQQQ"].pop("fund_return", None)
            si["funds"]["TQQQ"]["close_price"] = "25"
            si["funds"]["TQQQ"]["corporate_action"] = dict(
                {"type": "split", "ratio": 2, "adjustment_complete": True, "exact_reconciliation": True}, **ca
            )
            return si

        # Friday 2026-01-02 split reconciled on Tuesday 2026-01-06: both closes are post-split.
        self.assertEqual(_vin(_split_inputs({"ex_date": "2026-01-02"}))["funds"]["TQQQ"]["fund_return"], 0.0)
        self.assertAlmostEqual(_vin(_split_inputs({"ex_date": sd}))["funds"]["TQQQ"]["fund_return"], 1.0)
        self.assertIsNone(_vin(_split_inputs({}))["funds"]["TQQQ"]["fund_return"])

        # P1(r2)-5: explicit null broker/cash observations are unknown evidence, not model values.
        for fund_key in ("broker_position", "broker_value"):
            si = _valid_inputs()
            si["funds"]["TQQQ"][fund_key] = None
            self.assertIsNone(_vin(si)["funds"]["TQQQ"][fund_key], fund_key)
            self.assertTrue(_rows(si)["O3"]["would_have_tripped"], fund_key)
        for cash_key in ("broker_cash", "positions_cash"):
            si = _valid_inputs()
            si[cash_key] = None
            self.assertIsNone(_vin(si)[cash_key], cash_key)
            self.assertTrue(_rows(si)["O3"]["would_have_tripped"], cash_key)
        self.assertFalse(_rows(_valid_inputs())["O3"]["would_have_tripped"])

        # P1(r2)-3: an unsupported kill_switch_tripped flag is rejected, never silently ignored.
        si_kill = _valid_inputs()
        si_kill["kill_switch_tripped"] = True
        with self.assertRaisesRegex(ValueError, "authorized_events"):
            _vin(si_kill)
        rows_kill, _, _ = ev.evaluate(
            ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", _vin(_valid_inputs()), {},
            {"account": [{"type": "kill_switch", "scope": "account", "session": sd, "as_of": as_of}]},
        )
        self.assertEqual(rows_kill["O8"]["state"], "HALT")

        # Default-substitution scan: explicit nulls / unknown values are never replaced by defaults.
        for key in ("joint_exposure", "proposed_exposure", "reconciliation_tolerance"):
            si = _valid_inputs()
            si[key] = None
            self.assertIsNone(_vin(si)[key], key)
        si = _valid_inputs()
        si["funds"]["TQQQ"]["exposure"] = None
        si["funds"]["TQQQ"]["ledger_value"] = None
        self.assertIsNone(_vin(si)["funds"]["TQQQ"]["exposure"])
        self.assertIsNone(_vin(si)["funds"]["TQQQ"]["ledger_value"])
        for bad_tier in (None, "turbo"):
            si = _valid_inputs()
            si["tier"] = bad_tier
            vin_bad_tier = _vin(si)
            self.assertIsNone(vin_bad_tier["tier_joint_cap"], bad_tier)
            self.assertIsNone(vin_bad_tier["tier_loss_budget_D"], bad_tier)
        retier_unknown = build_fuse_validated_inputs(
            {"tier": "turbo", "fuse_inputs": {"tier": "aggressive", "bankroll": 45000, "high_water_mark": 50000,
                                               "tier_joint_cap": 1.0, "max_loss_cap": 1.0, "tier_loss_budget_D": 0.4}},
            None, policy_hash="p", session_date=sd, as_of=as_of,
        )
        for stale_key in ("tier_joint_cap", "max_loss_cap", "tier_loss_budget_D"):
            self.assertNotIn(stale_key, retier_unknown)
        retier_no_hwm = build_fuse_validated_inputs(
            {"tier": "balanced", "fuse_inputs": {"tier": "aggressive", "bankroll": 45000, "H": "0", "max_loss_cap": 1.0}},
            None, policy_hash="p", session_date=sd, as_of=as_of,
        )
        self.assertNotIn("max_loss_cap", retier_no_hwm)

        # P1(r3)-2: current_index_closes never overwrites a validated record; disagreement fails closed.
        idx_in = _mkt_inputs(_records(sd, XNDX={"normalized_value": "17800"}))
        self.assertAlmostEqual(_vin(idx_in)["index_one_session_returns"]["NDX"], -0.11)
        idx_in["current_index_closes"] = {"XNDX": "20000"}
        vin_idx = _vin(idx_in)
        self.assertNotIn("NDX", vin_idx["index_one_session_returns"] or {})
        self.assertTrue(_rows(idx_in)["O1"]["would_have_tripped"])
        # With no record for an (optional) index, the override supplies its close.
        idx_no_rec = _mkt_inputs(_records(sd))
        idx_no_rec["current_index_closes"] = {"SPX": "90"}
        self.assertAlmostEqual(_vin(idx_no_rec)["index_one_session_returns"]["SPX"], -0.1)
        # A missing REQUIRED record is not replaced by an override (fails closed).
        idx_req_missing = _mkt_inputs([r for r in _records(sd) if r["instrument"] != "SEMI"])
        idx_req_missing["current_index_closes"] = {"SEMI": "4500"}
        self.assertNotIn("SEMI", _vin(idx_req_missing)["index_one_session_returns"] or {})

        # P1(r3)-3: a held fund with an unknown close has unknown ledger/broker values (no invented 0).
        si_noclose = _valid_inputs()
        si_noclose["funds"]["TQQQ"].pop("close_price")
        vin_noclose = _vin(si_noclose)
        for key in ("ledger_value", "broker_value", "exposure"):
            self.assertIsNone(vin_noclose["funds"]["TQQQ"][key], key)
        self.assertIsNone(vin_noclose["joint_exposure"])
        self.assertTrue(_rows(si_noclose)["O3"]["would_have_tripped"])
        si_flat = _valid_inputs()
        si_flat["funds"]["TQQQ"].pop("close_price")
        si_flat["funds"]["TQQQ"]["q_pre"] = 0
        self.assertEqual(_vin(si_flat)["funds"]["TQQQ"]["ledger_value"], 0.0)

        # P1(r3)-4: kill_switch_tripped is rejected at the evaluator boundary on the effective payload.
        nested = _valid_inputs()
        nested["fuse_inputs"] = dict(_vin(_valid_inputs()), kill_switch_tripped=True)
        with self.assertRaisesRegex(ValueError, "authorized_events"):
            ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", _vin(nested), {}, None)

        # P1(r3)-5: caller source records dated after as_of are dropped, so O1 fails closed.
        for rec_key in ("positions_source_record", "broker_reconciliation_source_record"):
            si_future = _valid_inputs()
            si_future[rec_key] = {"as_of": "2026-01-07T22:00:00.000Z", "age_sessions": 0}
            self.assertTrue(_rows(si_future)["O1"]["would_have_tripped"], rec_key)

        # P1(r3)-6: retrieval timestamps compare as aware instants, not strings.
        offset_ok = _records(sd, TQQQ={"retrieval_time_utc": "2026-01-06T16:30:00-05:00"})
        self.assertEqual(_vin(_mkt_inputs(offset_ok))["funds"]["TQQQ"]["fund_return"], 0.0)
        offset_late = _records(sd, TQQQ={"retrieval_time_utc": "2026-01-06T17:30:00-05:00"})
        self.assertIsNone(_vin(_mkt_inputs(offset_late))["funds"]["TQQQ"]["fund_return"])
        naive = _records(sd, TQQQ={"retrieval_time_utc": "2026-01-06T21:30:00"})
        self.assertIsNone(_vin(_mkt_inputs(naive))["funds"]["TQQQ"]["fund_return"])

        # Safety pass (r4)-1: future-dated source records are dropped at the evaluator boundary on
        # every path, including nested fuse_inputs and the direct payload.
        future_src = {"as_of": "2026-01-07T22:00:00.000Z", "age_sessions": 0}
        base_vin = _vin(_valid_inputs())
        self.assertFalse(
            ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", base_vin, {}, None)[0]["O1"][
                "would_have_tripped"
            ]
        )
        nested_src = _valid_inputs()
        nested_src["fuse_inputs"] = dict(
            base_vin, source_records=dict(base_vin["source_records"], positions=future_src)
        )
        rows_nested = ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", _vin(nested_src), {}, None)[0]
        self.assertTrue(rows_nested["O1"]["would_have_tripped"])
        direct_src = dict(base_vin, source_records=dict(base_vin["source_records"], positions=future_src))
        direct_src["session_calendar_status"] = base_vin["session_calendar_status"]
        rows_direct = ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", _vin(direct_src), {}, None)[0]
        self.assertTrue(rows_direct["O1"]["would_have_tripped"])
        self.assertEqual(direct_src["source_records"]["positions"], future_src)

        # Safety pass (r4)-2: an unknown q_pre gives share_count None, so V2 cannot clear a pending
        # action by matching an invented 0 against a caller-supplied broker position of 0.
        cal5 = resolve_session("XNYS", "2026-01-05", spec=spec)
        s1 = _valid_inputs()
        s1["funds"]["TQQQ"]["corporate_action"] = {
            "type": "split", "ratio": 2, "ex_date": "2026-01-05",
            "adjustment_complete": False, "exact_reconciliation": False,
        }
        v1 = build_fuse_validated_inputs(s1, cal5, policy_hash="p", session_date="2026-01-05", as_of="2026-01-05T22:00:00.000Z")
        _, _, pending_state = ev.evaluate(
            ev.spec_version, ev.engine_hash, "p", "2026-01-05", "2026-01-05T22:00:00.000Z", "shadow", v1, {}, None
        )
        self.assertIn("TQQQ", pending_state["V2"]["pending_adjustments"])
        s2 = _valid_inputs()
        s2["funds"]["TQQQ"].pop("q_pre")
        s2["funds"]["TQQQ"].update(
            {
                "broker_position": 0,
                "ledger_value": 5000,
                "broker_value": 5000,
                "hwm_adjusted": True,
                "corporate_action": {
                    "type": "split", "ratio": 2, "ex_date": "2026-01-05", "adjustment_complete": True,
                    "exact_reconciliation": True, "hwm_adjusted": True,
                },
            }
        )
        v2 = _vin(s2)
        self.assertIsNone(v2["funds"]["TQQQ"]["share_count"])
        rows2, _, next2 = ev.evaluate(ev.spec_version, ev.engine_hash, "p", sd, as_of, "shadow", v2, pending_state, None)
        self.assertNotEqual(rows2["V2"].get("reason"), "exact_reconciliation_auto_clear")
        self.assertTrue(next2.get("V2"))
        si_flat_q = _valid_inputs()
        si_flat_q["funds"]["TQQQ"]["q_pre"] = 0
        self.assertEqual(_vin(si_flat_q)["funds"]["TQQQ"]["share_count"], 0)

        # P1-6: null policy hashes stay null so O6 fails closed (not the string "None").
        for key in ("loaded_policy_hash", "registered_policy_hash"):
            si = _valid_inputs()
            si[key] = None
            self.assertIsNone(_vin(si)[key])
            self.assertTrue(_rows(si)["O6"]["would_have_tripped"], key)

        # P1-1: heartbeat provenance is explicit; the adapter default is labelled a self-heartbeat.
        self.assertEqual(_vin(_valid_inputs())["heartbeat_source"], "adapter_self")
        si_hb = _valid_inputs()
        si_hb["last_heartbeat"] = "2026-01-06T21:59:30.000Z"
        self.assertEqual(_vin(si_hb)["heartbeat_source"], "caller")
        self.assertEqual(_vin(si_hb)["last_heartbeat"], "2026-01-06T21:59:30.000Z")

        # P1-1 (explicit evidence): a supplied null heartbeat or interval is not replaced by as_of / 60s.
        for key in ("last_heartbeat", "scheduled_interval"):
            si = _valid_inputs()
            si[key] = None
            self.assertIsNone(_vin(si)[key])
            self.assertTrue(_rows(si)["O7"]["would_have_tripped"], key)

        # P1-7: with no settled-cash evidence O5 does not treat total equity as settled cash.
        si_buy = _valid_inputs()
        si_buy.update(
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
        self.assertIsNone(_vin(si_buy)["settled_cash"])
        rows_buy = _rows(si_buy)
        self.assertTrue(rows_buy["O5"]["would_have_tripped"])
        self.assertEqual(rows_buy["O5"]["reason"], "pre_trade_limit_violation")

    def test_astra_p1_metadata_pinned_and_loader_executes_verified_source_only(self) -> None:
        import marshal

        from shadow.fuse_adapter import PINNED_FUSE_METADATA_SHA256

        self.assertIn("provenance.json", PINNED_FUSE_METADATA_SHA256)
        self.assertIn("SH1-ACCEPTANCE.json", PINNED_FUSE_METADATA_SHA256)
        with tempfile.TemporaryDirectory() as tmpdir:
            # P1-8: provenance.json and SH1-ACCEPTANCE.json cannot be rewritten consistently.
            for fname in ("SH1-ACCEPTANCE.json", "provenance.json", "spec.json"):
                copy_dir = Path(tmpdir) / f"vendor_{fname}"
                shutil.copytree(DEFAULT_VENDOR_ENGINE_ROOT, copy_dir)
                target = copy_dir / "metadata" / fname
                os.chmod(target, 0o644)
                target.write_bytes(target.read_bytes().replace(b"{", b"{ ", 1))
                with self.assertRaises(EngineIdentityError, msg=fname):
                    verify_vendored_engine_bundle(copy_dir)

            # P1-9: a poisoned cache under PYTHONPYCACHEPREFIX is never executed.
            copy_dir = Path(tmpdir) / "vendor_cache"
            shutil.copytree(DEFAULT_VENDOR_ENGINE_ROOT, copy_dir)
            prefix = Path(tmpdir) / "pycache_prefix"
            prev_prefix = sys.pycache_prefix
            sys.pycache_prefix = str(prefix)
            try:
                for py in sorted((copy_dir / "fuse_engine").glob("*.py")):
                    cfile = Path(importlib.util.cache_from_source(str(py.resolve())))
                    cfile.parent.mkdir(parents=True, exist_ok=True)
                    poisoned = compile("raise RuntimeError('poisoned cache executed')\n", str(py), "exec")
                    header = importlib.util.MAGIC_NUMBER + (0).to_bytes(4, "little")
                    st = py.stat()
                    header += int(st.st_mtime).to_bytes(4, "little") + (st.st_size & 0xFFFFFFFF).to_bytes(4, "little")
                    cfile.write_bytes(header + marshal.dumps(poisoned))
                ev_cache = VendoredFuseEvaluator(vendor_root=copy_dir)
            finally:
                sys.pycache_prefix = prev_prefix
            self.assertEqual(ev_cache.engine_hash, PINNED_FUSE_ENGINE_HASH)

    def test_half_day_eod_decision_maps_open_and_late_runs_are_rejected_by_scheduler_cutoff(self) -> None:
        from shadow.scheduler import DrillSchedulerConfig, compute_session_deadlines

        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal_hd = resolve_session("XNYS", "2026-11-27", spec=spec)
        self.assertEqual(cal_hd["session_state"], "half_day")
        self.assertEqual(cal_hd["order_cutoff_utc"], "2026-11-27T18:00:00.000Z")
        deadlines = compute_session_deadlines("2026-11-27", DrillSchedulerConfig(), calendar_session=cal_hd)
        decision_utc = deadlines["scheduled_decision_instant_utc"]
        # SH-1 half_day_run_time 14:30 ET is an EOD decision after the 13:00 ET order cutoff.
        self.assertEqual(decision_utc, "2026-11-27T19:30:00.000Z")
        self.assertGreater(decision_utc, cal_hd["order_cutoff_utc"])
        self.assertEqual(deadlines["decision_cutoff_utc"], "2026-11-27T20:00:00.000Z")

        ev = VendoredFuseEvaluator()
        for as_of in (decision_utc, "2026-11-27T20:05:00.000Z"):
            vin = build_fuse_validated_inputs(
                _valid_inputs(), cal_hd, policy_hash="p", session_date="2026-11-27", as_of=as_of
            )
            self.assertEqual(vin["session_calendar_status"], "open", as_of)
            self.assertEqual(vin["exchange_status"], "open", as_of)
            rows, _, _ = ev.evaluate(
                ev.spec_version, ev.engine_hash, "p", "2026-11-27", as_of, "shadow", vin, {}, None
            )
            self.assertFalse(rows["O2"]["would_have_tripped"], as_of)
        # A late run (15:05 ET) is past the scheduler's half-day decision cutoff (15:00 ET):
        # run_daily_drill_cycle rejects it as late_after_decision_cutoff before evaluation
        # (covered end-to-end by test_scheduler.test_market_cutoff_separate_from_commitment_deadline),
        # independent of O2.
        self.assertGreater("2026-11-27T20:05:00.000Z", deadlines["decision_cutoff_utc"])

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
                "settled_cash": "50000",
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
                "effective_session": "2026-01-06",
                "retrieval_time_utc": "2026-01-06T21:30:00.000Z",
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

    def test_split_with_cash_distribution_preserves_both_components_and_avoids_false_v1_latch(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si["prior_closes"] = {"TQQQ": "100", "SOXL": "40", "XNDX": "100", "SEMI": "100"}
        si["funds"]["TQQQ"]["close_price"] = "49"
        si["funds"]["TQQQ"].pop("fund_return", None)
        si["funds"]["TQQQ"].pop("corporate_action", None)
        si["funds"]["TQQQ"]["index_return"] = 0.0
        si["funds"]["TQQQ"]["modelled_fee"] = 0.0
        si["funds"]["TQQQ"]["modelled_financing"] = 0.0
        si["funds"]["SOXL"]["fund_return"] = 0.0
        si["funds"]["SOXL"]["index_return"] = 0.0
        si["funds"]["SOXL"]["modelled_fee"] = 0.0
        si["funds"]["SOXL"]["modelled_financing"] = 0.0
        si["market_records"] = [
            {
                "instrument": "TQQQ",
                "normalized_value": "49",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                "corporate_action": {
                    "split_ratio": "2",
                    "cash_per_share_usd": "1",
                    "ex_date": "2026-01-05",
                },
            },
            {
                "instrument": "SOXL",
                "normalized_value": "40",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                "corporate_action": {"split_ratio": "1", "cash_per_share_usd": "0", "ex_date": "2026-01-05"},
            },
            {
                "instrument": "QQQ",
                "normalized_value": "500",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "XNDX",
                "normalized_value": "20000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SEMI",
                "normalized_value": "5000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOFR",
                "normalized_value": "0.045",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-02",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
        ]

        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        tqqq_ca = vin["funds"]["TQQQ"]["corporate_action"]
        self.assertIsNotNone(tqqq_ca)
        self.assertEqual(tqqq_ca["ratio"], 2.0)
        self.assertEqual(tqqq_ca["cash_per_share_usd"], 1.0)
        self.assertAlmostEqual(vin["funds"]["TQQQ"]["fund_return"], 0.0, places=9)

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
        self.assertEqual(rows["V1"]["state"], "NORMAL")
        self.assertFalse(rows["V1"]["latched"])
        self.assertFalse(nxt.get("V1", {}).get("latched", False))

    def test_qqq_forward_split_does_not_leak_into_l6_index_crash_inputs(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si.pop("index_one_session_returns", None)
        si["prior_closes"] = {"XNDX": "20000", "SEMI": "5000", "QQQ": "100"}
        si["market_records"] = [
            {
                "instrument": "TQQQ",
                "normalized_value": "50",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOXL",
                "normalized_value": "40",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "QQQ",
                "normalized_value": "50",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
                "corporate_action": {"split_ratio": "2", "ex_date": "2026-01-05"},
            },
            {
                "instrument": "XNDX",
                "normalized_value": "20000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SEMI",
                "normalized_value": "5000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOFR",
                "normalized_value": "0.045",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-02",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "TBILL_3M",
                "normalized_value": "0.042",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
        ]
        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNotNone(vin["index_one_session_returns"])
        self.assertNotIn("QQQ", vin["index_one_session_returns"])
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
        self.assertEqual(rows["O1"]["state"], "NORMAL")
        self.assertEqual(rows["L6"]["state"], "NORMAL")
        self.assertFalse(nxt.get("L6", {}).get("latched", False))

    def test_nonpositive_sigma_forecast_marks_vol_invalid_for_o1_rejection(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        for bad_sigma in ("0", "-0.1"):
            with self.subTest(bad_sigma=bad_sigma):
                si = json.loads(json.dumps(_valid_inputs()))
                si["funds"]["TQQQ"]["sigma_forecast"] = bad_sigma
                vin = build_fuse_validated_inputs(
                    si,
                    cal,
                    policy_hash="p",
                    session_date="2026-01-05",
                    as_of="2026-01-05T22:00:00.000Z",
                )
                self.assertNotIn("volatility", vin["source_records"])
                self.assertNotIn("market_data", vin["source_records"])
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
                self.assertEqual(rows["O1"]["state"], "NO-NEW-RISK")

    def test_ineligible_benchmark_price_is_excluded_from_v1_return_derivation(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si.pop("index_one_session_returns", None)
        si["funds"]["TQQQ"].pop("index_return", None)
        si["funds"]["TQQQ"]["fund_return"] = 0.0
        si["prior_closes"] = {"XNDX": "100", "SEMI": "5000"}
        si["market_records"] = [
            {
                "instrument": "TQQQ",
                "normalized_value": "50",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOXL",
                "normalized_value": "40",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "QQQ",
                "normalized_value": "500",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "XNDX",
                "normalized_value": "90",
                "eligibility": {"eligible": False, "reason": "stale_benchmark_bar"},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SEMI",
                "normalized_value": "5000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOFR",
                "normalized_value": "0.045",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-02",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "TBILL_3M",
                "normalized_value": "0.042",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
        ]

        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin["funds"]["TQQQ"]["index_return"])
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
        self.assertFalse(rows["V1"].get("latched", False))
        self.assertFalse(nxt.get("V1", {}).get("latched", False))
        self.assertNotEqual(rows["V1"].get("reason"), "tracking_deviation_exceeds_0_75pp")

    def test_ineligible_fund_price_is_excluded_from_v1_return_derivation(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal = resolve_session("XNYS", "2026-01-05", spec=spec)
        ev = VendoredFuseEvaluator()

        si = json.loads(json.dumps(_valid_inputs()))
        si["prior_closes"] = {"TQQQ": "50", "SOXL": "40", "XNDX": "20000", "SEMI": "5000"}
        si["funds"]["TQQQ"].pop("fund_return", None)
        si["funds"]["TQQQ"]["close_price"] = "45"
        si["funds"]["TQQQ"]["index_return"] = 0.0
        si["funds"]["TQQQ"]["modelled_fee"] = 0.0
        si["funds"]["TQQQ"]["modelled_financing"] = 0.0
        si["market_records"] = [
            {
                "instrument": "TQQQ",
                "normalized_value": "45",
                "eligibility": {"eligible": False, "reason": "stale_fund_bar"},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOXL",
                "normalized_value": "40",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "XNDX",
                "normalized_value": "20000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SEMI",
                "normalized_value": "5000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "SOFR",
                "normalized_value": "0.045",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-02",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
            {
                "instrument": "TBILL_3M",
                "normalized_value": "0.042",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal["spec_digest"],
                "effective_session": "2026-01-05",
                "retrieval_time_utc": "2026-01-05T21:30:00.000Z",
            },
        ]

        vin = build_fuse_validated_inputs(
            si,
            cal,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin["funds"]["TQQQ"]["fund_return"])
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
        self.assertFalse(rows["V1"].get("latched", False))
        self.assertFalse(nxt.get("V1", {}).get("latched", False))
        self.assertNotEqual(rows["V1"].get("reason"), "tracking_deviation_exceeds_0_75pp")

    def test_ineligible_fund_corporate_action_is_excluded_from_v2_state(self) -> None:
        spec = load_spec(_REPO_ROOT / "specs" / "csa22-v2")
        cal1 = resolve_session("XNYS", "2026-01-05", spec=spec)
        cal2 = resolve_session("XNYS", "2026-01-06", spec=spec)
        ev = VendoredFuseEvaluator()

        si1 = json.loads(json.dumps(_valid_inputs()))
        si1["funds"]["TQQQ"].pop("corporate_action", None)
        si1["funds"]["TQQQ"]["close_price"] = "25"
        si1["funds"]["TQQQ"]["fund_return"] = 0.0
        si1["funds"]["TQQQ"]["index_return"] = 0.0
        si1["market_records"] = [
            {
                "instrument": "TQQQ",
                "normalized_value": "25",
                "eligibility": {"eligible": False, "reason": "stale_fund_bar"},
                "calendar_spec_digest": cal1["spec_digest"],
                "corporate_action": {"split_ratio": "2", "ex_date": "2026-01-05"},
            },
            {
                "instrument": "SOXL",
                "normalized_value": "40",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal1["spec_digest"],
            },
            {
                "instrument": "XNDX",
                "normalized_value": "20000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal1["spec_digest"],
            },
            {
                "instrument": "SEMI",
                "normalized_value": "5000",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal1["spec_digest"],
            },
            {
                "instrument": "SOFR",
                "normalized_value": "0.045",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal1["spec_digest"],
            },
            {
                "instrument": "TBILL_3M",
                "normalized_value": "0.042",
                "eligibility": {"eligible": True},
                "calendar_spec_digest": cal1["spec_digest"],
            },
        ]
        vin1 = build_fuse_validated_inputs(
            si1,
            cal1,
            policy_hash="p",
            session_date="2026-01-05",
            as_of="2026-01-05T22:00:00.000Z",
        )
        self.assertIsNone(vin1["funds"]["TQQQ"]["corporate_action"])
        rows1, _, nxt1 = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-05",
            "2026-01-05T22:00:00.000Z",
            "shadow",
            vin1,
            {},
            None,
        )
        self.assertEqual(rows1["O1"]["state"], "NO-NEW-RISK")

        si2 = json.loads(json.dumps(_valid_inputs()))
        si2["positions_source_record"] = {"as_of": "2026-01-06T22:00:00.000Z", "age_sessions": 0}
        si2["broker_reconciliation_source_record"] = {
            "as_of": "2026-01-06T22:00:00.000Z",
            "age_sessions": 0,
        }
        vin2 = build_fuse_validated_inputs(
            si2,
            cal2,
            policy_hash="p",
            session_date="2026-01-06",
            as_of="2026-01-06T22:00:00.000Z",
        )
        rows2, _, _ = ev.evaluate(
            ev.spec_version,
            ev.engine_hash,
            "p",
            "2026-01-06",
            "2026-01-06T22:00:00.000Z",
            "shadow",
            vin2,
            nxt1,
            None,
        )
        self.assertEqual(rows2["V2"]["state"], "NORMAL")
        self.assertNotEqual(rows2["V2"].get("reason"), "pending_corporate_action_carried_TQQQ")

    def test_market_corporate_action_with_none_optional_fields_is_preserved(self) -> None:
        from shadow.fuse_adapter import _map_market_corporate_action

        split_only = _map_market_corporate_action(
            {"split_ratio": "2", "cash_per_share_usd": None, "ex_date": "2026-01-05"}
        )
        self.assertIsNotNone(split_only)
        assert split_only is not None
        self.assertEqual(split_only["type"], "split")
        self.assertEqual(split_only["ratio"], 2.0)

        dist_only = _map_market_corporate_action(
            {"split_ratio": None, "cash_per_share_usd": "0.5", "ex_date": "2026-01-05"}
        )
        self.assertIsNotNone(dist_only)
        assert dist_only is not None
        self.assertEqual(dist_only["type"], "distribution")
        self.assertEqual(dist_only["cash_per_share_usd"], 0.5)

    def test_defensive_bounds_omitted_trade_date_and_clears_stale_max_loss_cap(self) -> None:
        from decimal import Decimal
        from shadow.fuse_adapter import (
            _apply_explicit_payload_tier,
            _finite_number,
            _map_calendar_statuses,
        )

        self.assertIsNone(_finite_number(Decimal("1e400")))
        self.assertEqual(
            _map_calendar_statuses(
                {"session_state": "regular", "is_trading_day": True, "blocks_eligibility": False},
                session_date="2026-01-05",
            ),
            (None, None),
        )
        updated = _apply_explicit_payload_tier(
            {"tier": "aggressive", "max_loss_cap": 12345.0},
            {"tier": "conservative"},
            policy_hash="a" * 64,
        )
        self.assertEqual(updated["tier"], "conservative")
        self.assertNotIn("max_loss_cap", updated)


if __name__ == "__main__":
    unittest.main()
