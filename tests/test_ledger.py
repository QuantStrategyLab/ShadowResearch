"""Unit tests for C-SH-LEDGER canonicalization, hash chain, tamper detection, and receipts."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import load_spec, resolve_session
from shadow.evaluator import StubEvaluator, StubProductionForbiddenError
from shadow.ledger import (
    GENESIS_PREV_HASH,
    CanonicalizationError,
    LedgerAppendError,
    ShadowLedger,
    build_record_body,
    canonical_json_bytes,
    check_forward_clock_credit,
    compute_code_manifest_hash,
    compute_session_commitment_deadline_utc,
    is_canonical_decimal_str,
    is_receipt_timely,
    is_utc_ms_timestamp,
    parse_json_no_duplicates,
    sha256_canonical,
    validate_record_body,
)
from shadow.ots import build_synthetic_ots_fixture
from shadow.sizing import compute_session_sizing


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
            env = ledger.append_record(built["record_body"], obtained_at_utc=cal["close_utc"])
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
                ledger.append_record(dup_body, obtained_at_utc="2025-11-25T21:00:00.000Z")

            # Forked prev hash rejected
            fork_body = copy.deepcopy(head["record_body"])
            fork_body["sequence"] = 3
            fork_body["session_date"] = "2025-11-26"
            fork_body["prev_record_sha256"] = "f" * 64
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(fork_body, obtained_at_utc="2025-11-26T21:00:00.000Z")

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

    def test_fund_orders_placed_validated_on_persisted_rows(self) -> None:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal = resolve_session("XNYS", "2025-11-21", spec=spec)
        built = build_record_body(
            chain_id="drill-fund-orders-check",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc=cal["close_utc"],
            session_inputs=_sample_inputs(),
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
        )
        base_body = built["record_body"]
        for tier in base_body["tiers"]:
            for fund in tier["funds"]:
                self.assertIs(type(fund["orders_placed"]), int)
                self.assertEqual(fund["orders_placed"], 0)

        missing_body = copy.deepcopy(base_body)
        del missing_body["tiers"][0]["funds"][0]["orders_placed"]
        with self.assertRaises(CanonicalizationError):
            validate_record_body(missing_body)

        bool_body = copy.deepcopy(base_body)
        bool_body["tiers"][0]["funds"][0]["orders_placed"] = False
        with self.assertRaises(CanonicalizationError):
            validate_record_body(bool_body)

        nonzero_body = copy.deepcopy(base_body)
        nonzero_body["tiers"][0]["funds"][0]["orders_placed"] = 1
        with self.assertRaises(CanonicalizationError):
            validate_record_body(nonzero_body)

    def test_rfc8785_utf16be_key_sort_and_nested_rejections(self) -> None:
        # RFC 8785 §3.2.3: U+1F600 (UTF-16BE D83D DE00) sorts BEFORE U+FF00 (UTF-16BE FF00)
        obj = {"\uff00": 2, "\U0001f600": 1, "a": 0}
        self.assertEqual(
            canonical_json_bytes(obj).decode("utf-8"),
            '{"a":0,"\U0001f600":1,"\uff00":2}',
        )
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes({"nested": [{"bad": 0.1}]})
        with self.assertRaises(CanonicalizationError):
            canonical_json_bytes({"bad": "\ud800"})
        with self.assertRaises(CanonicalizationError):
            parse_json_no_duplicates('{"outer": {"k": 1, "k": 2}}')
        for non_finite in ('{"x": Infinity}', '{"x": -Infinity}', '{"x": NaN}'):
            with self.assertRaises(CanonicalizationError):
                parse_json_no_duplicates(non_finite)

    def test_distinct_chain_and_o2_fail_closed_and_ots_receipt_discipline(self) -> None:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal_closed = resolve_session("XNYS", "2025-11-27", spec=spec)
        o2_res = build_record_body(
            chain_id="drill-o2-check",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-27",
            decision_timestamp_utc="2025-11-27T21:00:00.000Z",
            session_inputs=_sample_inputs(),
            calendar_session=cal_closed,
            code_version_sha=code_sha,
            evaluator=ev,
        )
        self.assertFalse(o2_res["appended"])
        self.assertTrue(o2_res["fail_closed"])
        self.assertTrue(o2_res["o2_output"]["would_have_tripped"])

        # Stub production mode structurally blocked in both evaluator and validate_record_body
        with self.assertRaises(StubProductionForbiddenError):
            build_record_body(
                chain_id="prod-blocked",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="production",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=resolve_session("XNYS", "2025-11-21", spec=spec),
                code_version_sha=code_sha,
                evaluator=ev,
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger = ShadowLedger(root / "ots_chain")
            cal_0 = resolve_session("XNYS", "2025-11-21", spec=spec)
            built_0 = build_record_body(
                chain_id="drill-ots-chain",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal_0,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            body_0 = built_0["record_body"]
            hash_0 = sha256_canonical(body_0)
            ots_0, _ = build_synthetic_ots_fixture(hash_0)
            env_0 = ledger.append_record(
                body_0,
                obtained_at_utc="2025-11-21T21:05:00.000Z",
                ots_bytes=ots_0,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
            )
            self.assertTrue((root / "ots_chain" / "seq_000000.ots").is_file())
            self.assertTrue(ledger.verify_chain()[0])
            self.assertTrue(ledger.verify_all_receipts()[0])
            # Pending-only OTS without Bitcoin block header cannot be creditable
            self.assertFalse(ledger.assess_receipt_creditability()[0])

            # Distinct-chain check rejects different mode on same chain
            cal_1 = resolve_session("XNYS", "2025-11-24", spec=spec)
            built_1_recon = build_record_body(
                chain_id="drill-ots-chain",
                sequence=1,
                prev_record_sha256=env_0["record_sha256"],
                mode="reconstruction",
                session_date="2025-11-24",
                decision_timestamp_utc="2025-11-24T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal_1,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(built_1_recon["record_body"])

            # Append seq 1 with a LATE OTS receipt -> chain + crypto verify pass, creditability flags late!
            built_1 = build_record_body(
                chain_id="drill-ots-chain",
                sequence=1,
                prev_record_sha256=env_0["record_sha256"],
                mode="drill",
                session_date="2025-11-24",
                decision_timestamp_utc="2025-11-24T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal_1,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            hash_1 = sha256_canonical(built_1["record_body"])
            ots_1, _ = build_synthetic_ots_fixture(hash_1)
            ledger.append_record(
                built_1["record_body"],
                obtained_at_utc="2025-11-25T06:00:00.000Z",
                ots_bytes=ots_1,
                commitment_deadline_utc="2025-11-25T04:30:00.000Z",
            )
            self.assertTrue(ledger.verify_chain()[0])
            self.assertTrue(ledger.verify_all_receipts()[0])
            cred_ok, cred_issues = ledger.assess_receipt_creditability()
            self.assertFalse(cred_ok)
            issues_by_seq = {x["sequence"]: x["reason"] for x in cred_issues}
            self.assertEqual(issues_by_seq[1], "late_after_commitment_deadline")

            # Tampering detached .ots fails verify_all_receipts separately from verify_chain
            ots_path_0 = root / "ots_chain" / "seq_000000.ots"
            orig_ots_0 = ots_path_0.read_bytes()
            ots_path_0.write_bytes(orig_ots_0[:-1] + bytes([orig_ots_0[-1] ^ 0xFF]))
            self.assertTrue(ledger.verify_chain()[0])
            ok_rcp, bad_rcp_seq, _ = ledger.verify_all_receipts()
            self.assertFalse(ok_rcp)
            self.assertEqual(bad_rcp_seq, 0)

    def test_creditability_requires_bitcoin_headers_and_pinned_deadline(self) -> None:
        # Finding #3 regression: creditability fails without trusted Bitcoin headers or deadline
        timely_none, err_none = is_receipt_timely(
            {"obtained_at_utc": "2025-11-21T21:05:00.000Z", "commitment_deadline_utc": None}
        )
        self.assertFalse(timely_none)
        self.assertEqual(err_none, "missing_or_invalid_pinned_commitment_deadline")

        spec = load_spec()
        cal = resolve_session("XNYS", "2025-11-21", spec=spec)
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ShadowLedger(Path(tmpdir) / "btc_chain")
            built = build_record_body(
                chain_id="btc-credit-test",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            rec_hash = sha256_canonical(built["record_body"])
            btc_ots, btc_root = build_synthetic_ots_fixture(rec_hash, bitcoin_height=870500)
            assert btc_root is not None
            ledger.append_record(
                built["record_body"],
                obtained_at_utc="2025-11-21T21:05:00.000Z",
                ots_bytes=btc_ots,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
            )
            self.assertTrue(ledger.verify_all_receipts()[0])
            # Without trusted headers: NOT creditable
            ok_no_hdr, issues_no_hdr = ledger.assess_receipt_creditability()
            self.assertFalse(ok_no_hdr)
            self.assertEqual(issues_no_hdr[0]["reason"], "ots_missing_trusted_bitcoin_headers")
            # Bare 64-hex Merkle root without block timestamp: NOT creditable
            ok_bare, issues_bare = ledger.assess_receipt_creditability(
                bitcoin_merkle_roots={870500: btc_root}
            )
            self.assertFalse(ok_bare)
            self.assertEqual(issues_bare[0]["reason"], "ots_missing_trusted_bitcoin_block_timestamp")
            # Late Bitcoin block timestamp (> 23:30 ET deadline) fails even if JSON obtained_at_utc was early!
            ok_late_btc, issues_late_btc = ledger.assess_receipt_creditability(
                bitcoin_merkle_roots={870500: {"merkle_root": btc_root, "timestamp": "2025-11-22T05:00:00.000Z"}}
            )
            self.assertFalse(ok_late_btc)
            self.assertEqual(issues_late_btc[0]["reason"], "ots_bitcoin_block_time_after_commitment_deadline")
            # With matching trusted headers AND timely Bitcoin block timestamp: creditable
            ok_hdr, issues_hdr = ledger.assess_receipt_creditability(
                bitcoin_merkle_roots={870500: {"merkle_root": btc_root, "timestamp": "2025-11-21T22:00:00.000Z"}}
            )
            self.assertTrue(ok_hdr, issues_hdr)

    def test_chain_rejects_code_version_or_calendar_digest_drift(self) -> None:
        # Finding #5 regression: C7 distinct-chain enforces code_version_sha and calendar_spec_digest
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal0 = resolve_session("XNYS", "2025-11-21", spec=spec)
        cal1 = resolve_session("XNYS", "2025-11-24", spec=spec)
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = ShadowLedger(Path(tmpdir) / "c7_chain")
            built0 = build_record_body(
                chain_id="c7-test",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal0,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            env0 = ledger.append_record(built0["record_body"], obtained_at_utc="2025-11-21T21:05:00.000Z")

            # Changed code_version_sha -> rejected (both anchored and unanchored)
            diff_code = "0" * 64
            built_diff_code = build_record_body(
                chain_id="c7-test",
                sequence=1,
                prev_record_sha256=env0["record_sha256"],
                mode="drill",
                session_date="2025-11-24",
                decision_timestamp_utc="2025-11-24T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal1,
                code_version_sha=diff_code,
                evaluator=ev,
            )
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(built_diff_code["record_body"], obtained_at_utc="2025-11-24T21:05:00.000Z")
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(built_diff_code["record_body"], allow_unanchored=True)

            # Changed calendar_spec_digest -> rejected (both anchored and unanchored)
            cal1_mut = {**cal1, "spec_digest": "f" * 64}
            built_diff_cal = build_record_body(
                chain_id="c7-test",
                sequence=1,
                prev_record_sha256=env0["record_sha256"],
                mode="drill",
                session_date="2025-11-24",
                decision_timestamp_utc="2025-11-24T21:00:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal1_mut,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(built_diff_cal["record_body"], obtained_at_utc="2025-11-24T21:05:00.000Z")
            with self.assertRaises(LedgerAppendError):
                ledger.append_record(built_diff_cal["record_body"], allow_unanchored=True)

    def test_stage_n_ledger_enforces_exact_zero_h(self) -> None:
        # Finding #9 regression: Stage N ledger rejects H != '0', while sizing.py supports H > 0
        non_zero_inputs = {**_sample_inputs(), "H": "250000"}
        sizing_res = compute_session_sizing(non_zero_inputs)
        self.assertEqual(sizing_res["bankroll"]["H"], "250000")

        spec = load_spec()
        cal = resolve_session("XNYS", "2025-11-21", spec=spec)
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        with self.assertRaises(CanonicalizationError):
            build_record_body(
                chain_id="stage-n-h-zero",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:00:00.000Z",
                session_inputs=non_zero_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
            )

        valid_built = build_record_body(
            chain_id="stage-n-h-zero",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T21:00:00.000Z",
            session_inputs=_sample_inputs(),
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
        )
        tampered_bankroll = copy.deepcopy(valid_built["record_body"])
        tampered_bankroll["bankroll"]["H"] = "250000"
        with self.assertRaises(CanonicalizationError):
            validate_record_body(tampered_bankroll)

        tampered_assert = copy.deepcopy(valid_built["record_body"])
        tampered_assert["assertions"]["H"] = "250000"
        with self.assertRaises(CanonicalizationError):
            validate_record_body(tampered_assert)

    def test_r2_strict_ledger_guards_and_forward_clock_credit(self) -> None:
        spec = load_spec()
        cal = resolve_session("XNYS", "2025-11-21", spec=spec)
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()

        # 1. Empty ledger creditability fails closed with ["empty_ledger"]
        with tempfile.TemporaryDirectory() as tmpdir:
            empty_ledger = ShadowLedger(Path(tmpdir) / "empty")
            self.assertEqual(empty_ledger.assess_receipt_creditability(), (False, ["empty_ledger"]))

            # 2. append_record without obtained_at_utc refuses fallback to decision_timestamp_utc
            built = build_record_body(
                chain_id="r2-ledger",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T22:30:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
            )
            with self.assertRaises(ValueError):
                empty_ledger.append_record(built["record_body"])

            # 3. Tier-level orders_placed=False (bool) rejected
            bool_tier = copy.deepcopy(built["record_body"])
            bool_tier["tiers"][0]["orders_placed"] = False
            with self.assertRaises(CanonicalizationError):
                validate_record_body(bool_tier)

        # 4. inputs_digest binds W_ref cleanly
        b_w50 = build_record_body(
            chain_id="r2-wref",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T22:30:00.000Z",
            session_inputs={**_sample_inputs(), "W_ref": "50000"},
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
        )
        b_w49 = build_record_body(
            chain_id="r2-wref",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T22:30:00.000Z",
            session_inputs={**_sample_inputs(), "W_ref": "49000"},
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
        )
        self.assertNotEqual(b_w50["record_body"]["inputs_digest"], b_w49["record_body"]["inputs_digest"])

        # 5. is_receipt_timely enforces pinned session commitment_deadline_utc
        pinned_dl = compute_session_commitment_deadline_utc("2025-11-21", "23:30")
        ok_mism, err_mism = is_receipt_timely(
            {
                "obtained_at_utc": "2025-11-21T22:35:00.000Z",
                "commitment_deadline_utc": "2025-11-25T23:59:00.000Z",
                "within_deadline": True,
            },
            expected_commitment_deadline_utc=pinned_dl,
        )
        self.assertFalse(ok_mism)
        self.assertEqual(err_mism, "commitment_deadline_mismatch_with_pinned_session_deadline")

        # 6. check_forward_clock_credit rejects missing receipts & forged metadata, passes true BTC proof
        prod_body = copy.deepcopy(built["record_body"])
        prod_body["mode"] = "production"
        prod_body["observation_class"] = "decision_time_market"
        prod_body["engine_hash"] = "fuse-prod-12345678"
        prod_body["evaluator"]["engine_hash"] = "fuse-prod-12345678"
        prod_body["assertions"]["forward_clock_credit_eligible"] = True
        prod_hash = sha256_canonical(prod_body)
        prod_env = {"record_body": prod_body, "record_sha256": prod_hash}
        btc_ots, btc_root = build_synthetic_ots_fixture(prod_hash, bitcoin_height=870700)
        assert btc_root is not None

        ok_no_rcp, reasons_no_rcp = check_forward_clock_credit([prod_env], None)
        self.assertFalse(ok_no_rcp)
        self.assertIn("missing_receipts", reasons_no_rcp)

        fake_rcp = {
            "anchor": "opentimestamps",
            "chain_id": "r2-ledger",
            "sequence": 0,
            "session_date": "2025-11-21",
            "record_sha256": prod_hash,
            "obtained_at_utc": "2025-11-21T22:35:00.000Z",
            "commitment_deadline_utc": pinned_dl,
            "within_deadline": True,
            "upgrade_state": "bitcoin_confirmed",
            "bitcoin_verified": True,
        }
        timely_hdrs = {870700: {"merkle_root": btc_root, "timestamp": "2025-11-21T23:00:00.000Z"}}
        ok_fake, _ = check_forward_clock_credit(
            [prod_env],
            [fake_rcp],
            bitcoin_merkle_roots=timely_hdrs,
        )
        self.assertFalse(ok_fake)

        with tempfile.TemporaryDirectory() as tmpdir:
            prod_ledger = ShadowLedger(Path(tmpdir) / "prod_credit")
            prod_ledger.append_record(
                prod_body,
                obtained_at_utc="2025-11-21T22:35:00.000Z",
                ots_bytes=btc_ots,
                commitment_deadline_utc=pinned_dl,
                bitcoin_merkle_roots=timely_hdrs,
            )
            ok_real, reasons_real = check_forward_clock_credit(
                prod_ledger.load_records(),
                prod_ledger.load_receipts(),
                ledger_dir=prod_ledger.root_dir,
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertTrue(ok_real, reasons_real)

            # R3 #3 regression: mutated record_body without matching hash MUST FAIL check_forward_clock_credit
            tampered_records = copy.deepcopy(prod_ledger.load_records())
            tampered_records[0]["record_body"]["mode"] = "drill"  # does not match envelope record_sha256
            tampered_ok, tampered_reasons = check_forward_clock_credit(
                tampered_records,
                prod_ledger.load_receipts(),
                ledger_dir=prod_ledger.root_dir,
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(tampered_ok)
            self.assertTrue(any("record_envelope_sha256_mismatch" in r for r in tampered_reasons), tampered_reasons)

            # Mutated record_body with fake drill body carrying valid prod envelope hash still fails on verify_receipt
            drill_with_stale_hash = copy.deepcopy(built["record_body"])
            drill_with_stale_hash["mode"] = "production"
            drill_with_stale_hash["engine_hash"] = "fuse-prod-12345678"
            drill_with_stale_hash["evaluator"]["engine_hash"] = "fuse-prod-12345678"
            drill_with_stale_hash["observation_class"] = "decision_time_market"
            drill_with_stale_hash["assertions"]["forward_clock_credit_eligible"] = False
            spoof_env = {"record_body": drill_with_stale_hash, "record_sha256": prod_hash}
            ok_spoof, spoof_reasons = check_forward_clock_credit(
                [spoof_env],
                prod_ledger.load_receipts(),
                ledger_dir=prod_ledger.root_dir,
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_spoof)
            self.assertTrue(any("record_envelope_sha256_mismatch" in r for r in spoof_reasons), spoof_reasons)

            # R3 #4 regression: unparseable commitment_deadline_et NEVER degrades to None (fails closed!)
            ok_bad_et, bad_et_reasons = check_forward_clock_credit(
                prod_ledger.load_records(),
                prod_ledger.load_receipts(),
                ledger_dir=prod_ledger.root_dir,
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="not-an-hhmm",
            )
            self.assertFalse(ok_bad_et)
            self.assertTrue(any("invalid_commitment_deadline_config" in r for r in bad_et_reasons), bad_et_reasons)

            ok_bad_assess, bad_assess_issues = prod_ledger.assess_receipt_creditability(
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="not-an-hhmm",
            )
            self.assertFalse(ok_bad_assess)
            self.assertTrue(
                any("invalid_commitment_deadline_config" in str(item.get("reason", "")) for item in bad_assess_issues if isinstance(item, dict)),
                bad_assess_issues,
            )

            # R3 #4 regression: backdated obtained_at_utc < decision_timestamp_utc rejected even with valid deadline
            early_rcp = copy.deepcopy(prod_ledger.load_receipts()[0])
            early_rcp["obtained_at_utc"] = "2025-11-21T20:00:00.000Z"  # earlier than decision 22:30Z
            ok_early_credit, early_reasons = check_forward_clock_credit(
                prod_ledger.load_records(),
                [early_rcp],
                ledger_dir=prod_ledger.root_dir,
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_early_credit)
            self.assertTrue(
                any("receipt_obtained_before_decision_timestamp" in r for r in early_reasons),
                early_reasons,
            )

            # R3 #3 regression: assess_receipt_creditability runs verify_chain() and rejects corrupted record bytes
            rec0_path = prod_ledger._record_path(0)
            orig_bytes = rec0_path.read_bytes()
            rec0_path.write_bytes(orig_bytes.replace(b'"50000"', b'"50001"', 1))
            ok_corrupt, corrupt_issues = prod_ledger.assess_receipt_creditability(
                bitcoin_merkle_roots=timely_hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_corrupt)
            self.assertTrue(any("chain_invalid:" in str(x) for x in corrupt_issues), corrupt_issues)

    def test_r4_forward_credit_identity_uniqueness_deadline_and_unanchored_orphan_cleanup(self) -> None:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal0 = resolve_session("XNYS", "2025-11-21", spec=spec)
        cal1 = resolve_session("XNYS", "2025-11-24", spec=spec)

        b0 = build_record_body(
            chain_id="r4-prod-chain",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T22:30:00.000Z",
            session_inputs=_sample_inputs(),
            calendar_session=cal0,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
        )["record_body"]
        b0["mode"] = "production"
        b0["observation_class"] = "decision_time_market"
        b0["engine_hash"] = "fuse-prod-12345678"
        b0["evaluator"]["engine_hash"] = "fuse-prod-12345678"
        b0["assertions"]["forward_clock_credit_eligible"] = True
        h0 = sha256_canonical(b0)
        ots0, root0 = build_synthetic_ots_fixture(h0, bitcoin_height=870800)
        assert root0 is not None
        hdrs = {870800: {"merkle_root": root0, "timestamp": "2025-11-21T23:00:00.000Z"}}
        dl0 = compute_session_commitment_deadline_utc("2025-11-21", "23:30")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_ledger = ShadowLedger(root / "prod_base")
            base_ledger.append_record(
                b0,
                obtained_at_utc="2025-11-21T22:35:00.000Z",
                ots_bytes=ots0,
                commitment_deadline_utc=dl0,
                bitcoin_merkle_roots=hdrs,
            )
            env0 = base_ledger.load_records()[0]
            rcp0 = base_ledger.load_receipts()[0]

            # (a) Duplicate session_date across seq=0 and seq=1 MUST FAIL check_forward_clock_credit
            b1_dup = copy.deepcopy(b0)
            b1_dup["sequence"] = 1
            b1_dup["prev_record_sha256"] = h0
            h1_dup = sha256_canonical(b1_dup)
            ots1_dup, _ = build_synthetic_ots_fixture(h1_dup, bitcoin_height=870800)
            rcp1_dup = {
                "anchor": "opentimestamps",
                "chain_id": "r4-prod-chain",
                "sequence": 1,
                "session_date": "2025-11-21",
                "record_sha256": h1_dup,
                "obtained_at_utc": "2025-11-21T22:36:00.000Z",
                "commitment_deadline_utc": dl0,
                "within_deadline": True,
                "ots_file": "seq_000001.ots",
                "ots_sha256": sha256_canonical({"ots": "dup"}),
                "upgrade_state": "bitcoin_confirmed",
                "bitcoin_verified": True,
            }
            del rcp1_dup["ots_sha256"]
            import hashlib as _hl

            rcp1_dup["ots_sha256"] = _hl.sha256(ots1_dup).hexdigest()
            ok_dup, dup_reasons = check_forward_clock_credit(
                [env0, {"record_body": b1_dup, "record_sha256": h1_dup}],
                [rcp0, rcp1_dup],
                ots_bytes_by_seq={0: ots0, 1: ots1_dup},
                bitcoin_merkle_roots=hdrs,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_dup)
            self.assertTrue(any("duplicate_session_date:2025-11-21" in r for r in dup_reasons), dup_reasons)

            # (b) Mid-chain policy_config_hash change MUST FAIL check_forward_clock_credit
            b1_pol = build_record_body(
                chain_id="r4-prod-chain",
                sequence=1,
                prev_record_sha256=h0,
                mode="drill",
                session_date="2025-11-24",
                decision_timestamp_utc="2025-11-24T22:30:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal1,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state={},
            )["record_body"]
            b1_pol["mode"] = "production"
            b1_pol["observation_class"] = "decision_time_market"
            b1_pol["engine_hash"] = "fuse-prod-12345678"
            b1_pol["evaluator"]["engine_hash"] = "fuse-prod-12345678"
            b1_pol["assertions"]["forward_clock_credit_eligible"] = True
            b1_pol["policy_config_hash"] = "e" * 64
            h1_pol = sha256_canonical(b1_pol)
            ots1_pol, root1_pol = build_synthetic_ots_fixture(h1_pol, bitcoin_height=870801)
            assert root1_pol is not None
            hdrs_2 = dict(hdrs)
            hdrs_2[870801] = {"merkle_root": root1_pol, "timestamp": "2025-11-24T23:00:00.000Z"}
            dl1 = compute_session_commitment_deadline_utc("2025-11-24", "23:30")
            rcp1_pol = {
                "anchor": "opentimestamps",
                "chain_id": "r4-prod-chain",
                "sequence": 1,
                "session_date": "2025-11-24",
                "record_sha256": h1_pol,
                "obtained_at_utc": "2025-11-24T22:35:00.000Z",
                "commitment_deadline_utc": dl1,
                "within_deadline": True,
                "ots_file": "seq_000001.ots",
                "ots_sha256": _hl.sha256(ots1_pol).hexdigest(),
                "upgrade_state": "bitcoin_confirmed",
                "bitcoin_verified": True,
            }
            ok_pol, pol_reasons = check_forward_clock_credit(
                [env0, {"record_body": b1_pol, "record_sha256": h1_pol}],
                [rcp0, rcp1_pol],
                ots_bytes_by_seq={0: ots0, 1: ots1_pol},
                bitcoin_merkle_roots=hdrs_2,
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_pol)
            self.assertTrue(any("policy_config_hash_mismatch" in r for r in pol_reasons), pol_reasons)

            # (c) Missing decision_timestamp_utc MUST FAIL check_forward_clock_credit
            b_nodts = copy.deepcopy(b0)
            del b_nodts["decision_timestamp_utc"]
            h_nodts = sha256_canonical(b_nodts)
            ots_nodts, r_nodts = build_synthetic_ots_fixture(h_nodts, bitcoin_height=870802)
            assert r_nodts is not None
            rcp_nodts = copy.deepcopy(rcp0)
            rcp_nodts["record_sha256"] = h_nodts
            rcp_nodts["ots_sha256"] = _hl.sha256(ots_nodts).hexdigest()
            ok_nodts, nodts_reasons = check_forward_clock_credit(
                [{"record_body": b_nodts, "record_sha256": h_nodts}],
                [rcp_nodts],
                ots_bytes_by_seq={0: ots_nodts},
                bitcoin_merkle_roots={870802: {"merkle_root": r_nodts, "timestamp": "2025-11-21T23:00:00.000Z"}},
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_nodts)
            self.assertTrue(any("invalid_decision_timestamp_utc" in r for r in nodts_reasons), nodts_reasons)

            # (d) Nested evaluator.engine_hash="stub-..." while top-level engine_hash is non-stub MUST FAIL
            b_nstub = copy.deepcopy(b0)
            b_nstub["evaluator"]["engine_hash"] = ev.engine_hash
            h_nstub = sha256_canonical(b_nstub)
            ots_nstub, r_nstub = build_synthetic_ots_fixture(h_nstub, bitcoin_height=870803)
            assert r_nstub is not None
            rcp_nstub = copy.deepcopy(rcp0)
            rcp_nstub["record_sha256"] = h_nstub
            rcp_nstub["ots_sha256"] = _hl.sha256(ots_nstub).hexdigest()
            ok_nstub, nstub_reasons = check_forward_clock_credit(
                [{"record_body": b_nstub, "record_sha256": h_nstub}],
                [rcp_nstub],
                ots_bytes_by_seq={0: ots_nstub},
                bitcoin_merkle_roots={870803: {"merkle_root": r_nstub, "timestamp": "2025-11-21T23:00:00.000Z"}},
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_nstub)
            self.assertTrue(
                any("nested_evaluator_stub_engine_hash" in r for r in nstub_reasons),
                nstub_reasons,
            )

            # (e) R4 #4 regression: None / "" / False / missing config NEVER fall back to 23:30
            for bad_cfg in (
                {"commitment_deadline_et": None},
                {"commitment_deadline_et": ""},
                {"commitment_deadline_et": False},
                {},
            ):
                with patch("shadow.ledger.load_effective_runtime_config", return_value=bad_cfg):
                    with self.assertRaises(ValueError):
                        compute_session_commitment_deadline_utc("2025-11-21")
                    ok_cfg, cfg_reasons = check_forward_clock_credit(
                        base_ledger.load_records(),
                        base_ledger.load_receipts(),
                        ledger_dir=base_ledger.root_dir,
                        bitcoin_merkle_roots=hdrs,
                    )
                    self.assertFalse(ok_cfg, bad_cfg)
                    self.assertTrue(
                        any("invalid_commitment_deadline_config" in r for r in cfg_reasons),
                        cfg_reasons,
                    )
                    ok_acc, acc_issues = base_ledger.assess_receipt_creditability(
                        bitcoin_merkle_roots=hdrs,
                    )
                    self.assertFalse(ok_acc, bad_cfg)
                    self.assertTrue(
                        any("invalid_commitment_deadline_config" in str(item.get("reason", "")) for item in acc_issues if isinstance(item, dict)),
                        acc_issues,
                    )

            missing_cfg_path = root / "does-not-exist.json"
            with self.assertRaises(ValueError):
                compute_session_commitment_deadline_utc("2025-11-21", config_path=missing_cfg_path)
            ok_miss_cfg, miss_cfg_reasons = check_forward_clock_credit(
                base_ledger.load_records(),
                base_ledger.load_receipts(),
                ledger_dir=base_ledger.root_dir,
                bitcoin_merkle_roots=hdrs,
                config_path=missing_cfg_path,
            )
            self.assertFalse(ok_miss_cfg)
            self.assertTrue(
                any("invalid_commitment_deadline_config" in r for r in miss_cfg_reasons),
                miss_cfg_reasons,
            )

            # (f) R4 #5 orphan cleanup: unanchored append removes stale orphan .ots and .receipt.json at seq=0
            orphan_ledger = ShadowLedger(root / "unanchored_orphan")
            orphan_ledger._ots_path(0).write_bytes(b"stale-orphan-ots")
            orphan_ledger._receipt_path(0).write_text('{"stale":true}', encoding="utf-8")
            drill_b0 = build_record_body(
                chain_id="r4-orphan-drill",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T22:30:00.000Z",
                session_inputs=_sample_inputs(),
                calendar_session=cal0,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state={},
            )["record_body"]
            orphan_ledger.append_record(drill_b0, allow_unanchored=True)
            self.assertTrue(orphan_ledger._record_path(0).is_file())
            self.assertFalse(orphan_ledger._ots_path(0).exists())
            self.assertFalse(orphan_ledger._receipt_path(0).exists())

    def test_r5_canonical_date_deduplication_and_decision_date_lower_bound(self) -> None:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal0 = resolve_session("XNYS", "2025-11-21", spec=spec)
        b0 = build_record_body(
            chain_id="r5-prod-chain",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T22:30:00.000Z",
            session_inputs=_sample_inputs(),
            calendar_session=cal0,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
        )["record_body"]
        b0["mode"] = "production"
        b0["observation_class"] = "decision_time_market"
        b0["engine_hash"] = "fuse-prod-12345678"
        b0["evaluator"]["engine_hash"] = "fuse-prod-12345678"
        b0["assertions"]["forward_clock_credit_eligible"] = True
        h0 = sha256_canonical(b0)
        ots0, root0 = build_synthetic_ots_fixture(h0, bitcoin_height=870900)
        assert root0 is not None
        hdrs = {870900: {"merkle_root": root0, "timestamp": "2025-11-21T23:00:00.000Z"}}
        dl0 = compute_session_commitment_deadline_utc("2025-11-21", "23:30")

        import hashlib as _hl

        rcp0 = {
            "anchor": "opentimestamps",
            "chain_id": "r5-prod-chain",
            "sequence": 0,
            "session_date": "2025-11-21",
            "record_sha256": h0,
            "obtained_at_utc": "2025-11-21T22:35:00.000Z",
            "commitment_deadline_utc": dl0,
            "within_deadline": True,
            "ots_file": "seq_000000.ots",
            "ots_sha256": _hl.sha256(ots0).hexdigest(),
            "upgrade_state": "bitcoin_confirmed",
            "bitcoin_verified": True,
        }
        env0 = {"record_body": b0, "record_sha256": h0}

        # (a) Alternate ISO date representations ("20251121", "2025-W47-5") MUST BE REJECTED
        for alt_ds in ("20251121", "2025-W47-5"):
            b1 = copy.deepcopy(b0)
            b1["sequence"] = 1
            b1["prev_record_sha256"] = h0
            b1["session_date"] = alt_ds
            h1 = sha256_canonical(b1)
            ots1, root1 = build_synthetic_ots_fixture(h1, bitcoin_height=870900)
            assert root1 is not None
            rcp1 = copy.deepcopy(rcp0)
            rcp1["sequence"] = 1
            rcp1["session_date"] = alt_ds
            rcp1["record_sha256"] = h1
            rcp1["obtained_at_utc"] = "2025-11-21T22:36:00.000Z"
            rcp1["ots_file"] = "seq_000001.ots"
            rcp1["ots_sha256"] = _hl.sha256(ots1).hexdigest()
            ok_alt, alt_reasons = check_forward_clock_credit(
                [env0, {"record_body": b1, "record_sha256": h1}],
                [rcp0, rcp1],
                ots_bytes_by_seq={0: ots0, 1: ots1},
                bitcoin_merkle_roots={870900: {"merkle_root": root0, "timestamp": "2025-11-21T23:00:00.000Z"}},
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_alt, alt_ds)
            self.assertTrue(
                any("non_canonical_session_date" in r for r in alt_reasons),
                (alt_ds, alt_reasons),
            )
            with self.assertRaises(CanonicalizationError):
                validate_record_body(b1)

        # (b) Pre-session decision_timestamp_utc (Nov 20 on Nov 21 record) & pre-16:00 ET close MUST FAIL creditability
        for bad_ts, expected_tag in (
            ("2025-11-20T22:30:00.000Z", "decision_timestamp_before_session_date"),
            ("2025-11-21T16:00:00.000Z", "decision_timestamp_before_session_close"),
        ):
            b_early = copy.deepcopy(b0)
            b_early["decision_timestamp_utc"] = bad_ts
            h_early = sha256_canonical(b_early)
            ots_early, r_early = build_synthetic_ots_fixture(h_early, bitcoin_height=870901)
            assert r_early is not None
            rcp_early = copy.deepcopy(rcp0)
            rcp_early["record_sha256"] = h_early
            rcp_early["ots_sha256"] = _hl.sha256(ots_early).hexdigest()
            ok_early, early_reasons = check_forward_clock_credit(
                [{"record_body": b_early, "record_sha256": h_early}],
                [rcp_early],
                ots_bytes_by_seq={0: ots_early},
                bitcoin_merkle_roots={870901: {"merkle_root": r_early, "timestamp": "2025-11-21T23:00:00.000Z"}},
                commitment_deadline_et="23:30",
            )
            self.assertFalse(ok_early, bad_ts)
            self.assertTrue(any(expected_tag in r for r in early_reasons), (bad_ts, early_reasons))

    def test_r6_forward_credit_actual_calendar_close_and_ascii_deadline(self) -> None:
        spec = load_spec()
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        cal_reg = resolve_session("XNYS", "2025-11-21", spec=spec)  # regular 16:00 ET = 21:00Z close
        cal_hd = resolve_session("XNYS", "2025-11-28", spec=spec)   # half-day 13:00 ET = 18:00Z close

        import hashlib as _hl

        # (1) Normal 16:00 ET close day 2025-11-21: 14:00 ET (19:00Z) decision MUST FAIL creditability!
        b_reg_1400 = build_record_body(
            chain_id="r6-close-bound",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="drill",
            session_date="2025-11-21",
            decision_timestamp_utc="2025-11-21T19:00:00.000Z",  # 14:00 EST < 16:00 EST close
            session_inputs=_sample_inputs(),
            calendar_session=cal_reg,
            code_version_sha=code_sha,
            evaluator=ev,
            prior_row_state={},
        )["record_body"]
        b_reg_1400["mode"] = "production"
        b_reg_1400["observation_class"] = "decision_time_market"
        b_reg_1400["engine_hash"] = "fuse-prod-12345678"
        b_reg_1400["evaluator"]["engine_hash"] = "fuse-prod-12345678"
        b_reg_1400["assertions"]["forward_clock_credit_eligible"] = True
        h_reg = sha256_canonical(b_reg_1400)
        ots_reg, root_reg = build_synthetic_ots_fixture(h_reg, bitcoin_height=870910)
        assert root_reg is not None
        dl_reg = compute_session_commitment_deadline_utc("2025-11-21", "23:30")
        rcp_reg = {
            "anchor": "opentimestamps",
            "chain_id": "r6-close-bound",
            "sequence": 0,
            "session_date": "2025-11-21",
            "record_sha256": h_reg,
            "obtained_at_utc": "2025-11-21T21:05:00.000Z",
            "commitment_deadline_utc": dl_reg,
            "within_deadline": True,
            "ots_file": "seq_000000.ots",
            "ots_sha256": _hl.sha256(ots_reg).hexdigest(),
            "upgrade_state": "bitcoin_confirmed",
            "bitcoin_verified": True,
        }
        ok_reg, reg_reasons = check_forward_clock_credit(
            [{"record_body": b_reg_1400, "record_sha256": h_reg}],
            [rcp_reg],
            ots_bytes_by_seq={0: ots_reg},
            bitcoin_merkle_roots={870910: {"merkle_root": root_reg, "timestamp": "2025-11-21T22:00:00.000Z"}},
            commitment_deadline_et="23:30",
        )
        self.assertFalse(ok_reg)
        self.assertTrue(
            any("decision_timestamp_before_session_close" in r for r in reg_reasons),
            reg_reasons,
        )

        # (2) Half-day 2025-11-28 (13:00 ET = 18:00Z close): 14:00 ET (19:00Z) PASSES, 12:30 ET (17:30Z) FAILS
        for hd_ts, expect_pass in (("2025-11-28T19:00:00.000Z", True), ("2025-11-28T17:30:00.000Z", False)):
            b_hd = build_record_body(
                chain_id="r6-hd-bound",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-28",
                decision_timestamp_utc=hd_ts,
                session_inputs=_sample_inputs(),
                calendar_session=cal_hd,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state={},
            )["record_body"]
            b_hd["mode"] = "production"
            b_hd["observation_class"] = "decision_time_market"
            b_hd["engine_hash"] = "fuse-prod-12345678"
            b_hd["evaluator"]["engine_hash"] = "fuse-prod-12345678"
            b_hd["assertions"]["forward_clock_credit_eligible"] = True
            h_hd = sha256_canonical(b_hd)
            ots_hd, root_hd = build_synthetic_ots_fixture(h_hd, bitcoin_height=870911)
            assert root_hd is not None
            dl_hd = compute_session_commitment_deadline_utc("2025-11-28", "23:30")
            rcp_hd = {
                "anchor": "opentimestamps",
                "chain_id": "r6-hd-bound",
                "sequence": 0,
                "session_date": "2025-11-28",
                "record_sha256": h_hd,
                "obtained_at_utc": "2025-11-28T19:05:00.000Z",
                "commitment_deadline_utc": dl_hd,
                "within_deadline": True,
                "ots_file": "seq_000000.ots",
                "ots_sha256": _hl.sha256(ots_hd).hexdigest(),
                "upgrade_state": "bitcoin_confirmed",
                "bitcoin_verified": True,
            }
            ok_hd, hd_reasons = check_forward_clock_credit(
                [{"record_body": b_hd, "record_sha256": h_hd}],
                [rcp_hd],
                ots_bytes_by_seq={0: ots_hd},
                bitcoin_merkle_roots={870911: {"merkle_root": root_hd, "timestamp": "2025-11-28T20:00:00.000Z"}},
                commitment_deadline_et="23:30",
            )
            self.assertEqual(ok_hd, expect_pass, (hd_ts, hd_reasons))

        # (3) Non-ASCII full-width Unicode digits rejected in deadline parser
        with self.assertRaises(ValueError):
            compute_session_commitment_deadline_utc("2025-11-21", "\uff12\uff13:\uff13\uff10")


if __name__ == "__main__":
    unittest.main()
