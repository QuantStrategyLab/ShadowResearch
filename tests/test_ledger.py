"""Unit tests for C-SH-LEDGER canonicalization, hash chain, tamper detection, and receipts."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.evaluator import StubEvaluator
from shadow.ledger import (
    GENESIS_PREV_HASH,
    CanonicalizationError,
    LedgerAppendError,
    ShadowLedger,
    build_record_body,
    canonical_json_bytes,
    check_forward_clock_credit,
    compute_code_manifest_hash,
    is_canonical_decimal_str,
    is_utc_ms_timestamp,
    parse_json_no_duplicates,
)


def _sample_inputs() -> dict:
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


class LedgerTest(unittest.TestCase):
    """Verify RFC 8785 profile, append rules, tamper rejection, and receipts."""

    def test_rfc8785_profile_rejects_floats_duplicates_and_non_canonical_decimals(self) -> None:
        # Key ordering & escaping
        b = canonical_json_bytes({"b": "a/b\n\x01", "a": 1, "c": True})
        self.assertEqual(b, b'{"a":1,"b":"a/b\\n\\u0001","c":true}')

        # Float rejected
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes({"x": 1.5})

        # Duplicate JSON key & NaN rejected
        with self.assertRaises(CanonicalizationError):
            parse_json_no_duplicates('{"a": 1, "a": 2}')
        with self.assertRaises(CanonicalizationError):
            parse_json_no_duplicates('{"a": NaN}')

        # Decimal string rules
        for good in ("0", "50000", "0.2", "-0.05", "123.456"):
            self.assertTrue(is_canonical_decimal_str(good), good)
        for bad in ("-0", "0.0", "0.20", "01", "50000.00", "1e5", ".5", "1."):
            self.assertFalse(is_canonical_decimal_str(bad), bad)

        # UTC millisecond timestamp rules
        self.assertTrue(is_utc_ms_timestamp("2025-11-21T21:00:00.000Z"))
        self.assertFalse(is_utc_ms_timestamp("2025-11-21T21:00:00Z"))
        self.assertFalse(is_utc_ms_timestamp("2025-11-21T21:00:00.000+00:00"))

    def _populate_three_records(self, ledger_dir: Path) -> ShadowLedger:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        ledger = ShadowLedger(ledger_dir)
        prev = GENESIS_PREV_HASH
        prior_state: dict = {}
        dates = ("2025-11-21", "2025-11-24", "2025-11-25")

        for seq, ds in enumerate(dates):
            cal = resolve_session("XNYS", ds, spec=spec)
            built = build_record_body(
                chain_id="drill-test-chain",
                sequence=seq,
                prev_record_sha256=prev,
                mode="drill",
                session_date=ds,
                decision_timestamp_utc=cal["close_utc"],
                session_inputs=_sample_inputs(),
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state=prior_state,
            )
            env = ledger.append_record(built["record_body"])
            prev = env["record_sha256"]
            prior_state = built["next_row_state"]
        return ledger

    def test_append_verify_mutation_fork_gap_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = self._populate_three_records(root / "chain_ok")
            ok, bad_seq, err = ledger.verify_chain()
            self.assertTrue(ok, err)
            self.assertIsNone(bad_seq)
            rok, rseq, rerr = ledger.verify_all_receipts()
            self.assertTrue(rok, rerr)
            self.assertIsNone(rseq)

            # Duplicate append rejected
            head = ledger.head()
            assert head is not None
            dup_body = copy.deepcopy(head["record_body"])
            dup_body["sequence"] = 3
            dup_body["prev_record_sha256"] = head["record_sha256"]
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(dup_body)

            # Forked prev hash rejected
            fork_body = copy.deepcopy(head["record_body"])
            fork_body["sequence"] = 3
            fork_body["session_date"] = "2025-11-26"
            fork_body["prev_record_sha256"] = "f" * 64
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(fork_body)

            # One-byte mutation at seq 1 -> verify fails and identifies first_bad_sequence=1
            mut_dir = root / "chain_mut"
            self._populate_three_records(mut_dir)
            target_rec = mut_dir / "seq_000001.record.json"
            raw = bytearray(target_rec.read_bytes())
            idx = raw.index(b'"50000"') + 2
            raw[idx] = ord("1") if raw[idx] != ord("1") else ord("2")
            target_rec.write_bytes(bytes(raw))
            mut_ledger = ShadowLedger(mut_dir)
            ok_m, bad_m, _ = mut_ledger.verify_chain()
            self.assertFalse(ok_m)
            self.assertEqual(bad_m, 1)

            # Dropped record (gap at seq 1) -> verify fails and identifies first_bad_sequence=1
            gap_dir = root / "chain_gap"
            self._populate_three_records(gap_dir)
            (gap_dir / "seq_000001.record.json").unlink()
            gap_ledger = ShadowLedger(gap_dir)
            ok_g, bad_g, _ = gap_ledger.verify_chain()
            self.assertFalse(ok_g)
            self.assertEqual(bad_g, 1)

            # Forward clock credit check rejects drills
            eligible, reasons = check_forward_clock_credit(
                ledger.load_records(),
                ledger.load_receipts(),
            )
            self.assertFalse(eligible)
            self.assertTrue(len(reasons) > 0)


if __name__ == "__main__":
    unittest.main()
