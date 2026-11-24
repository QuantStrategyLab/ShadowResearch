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
        "funds": {
            "TQQQ": {"s": "1", "close_price": "50", "q_pre": 100, "sigma_forecast": "0.24"},
            "SOXL": {"s": "1", "close_price": "40", "q_pre": 100, "sigma_forecast": "0.22"},
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


if __name__ == "__main__":
    unittest.main()
