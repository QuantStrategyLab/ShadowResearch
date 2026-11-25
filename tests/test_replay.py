"""Unit tests for deterministic replay CLI and bit-for-bit verification."""

from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_session
from shadow.evaluator import StubEvaluator
from shadow.feeder import (
    DEMO_SERIES,
    VINTAGE_Y2,
    emit_market_records_from_fixtures,
    run_reconstruction_window,
    sha256_canonical as feeder_sha256,
)
from shadow.sizing import policy_config_hash
from shadow.feeder_live import LIVE_SERIES
from shadow.ledger import (
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    compute_code_manifest_hash,
    sha256_canonical,
)
from shadow.ots import build_synthetic_ots_fixture
from shadow.replay import main as replay_cli_main
from shadow.replay import replay_window

FEEDER_FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "feeder"
DRILL_FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "drill"


class ReplayTest(unittest.TestCase):
    """Verify bit-for-bit replay and CLI exit status."""

    def test_replay_window_and_cli(self) -> None:
        spec = load_spec(DEFAULT_SPEC_DIR)
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        sessions_in = [
            {
                "session_date": "2025-11-21",
                "decision_timestamp_utc": "2025-11-21T21:00:00.000Z",
                "inputs": {
                    "E": "50000",
                    "E_star": "50000",
                    "H": "0",
                    "L": "0",
                    "funds": {
                        "TQQQ": {"s": "1", "close_price": "50", "q_pre": 0, "sigma_forecast": "0.05"},
                        "SOXL": {"s": "1", "close_price": "40", "q_pre": 0, "sigma_forecast": "0.05"},
                    },
                },
            },
            {
                "session_date": "2025-11-24",
                "decision_timestamp_utc": "2025-11-24T21:00:00.000Z",
                "inputs": {
                    "E": "45000",
                    "E_star": "50000",
                    "H": "0",
                    "L": "5000",
                    "funds": {
                        "TQQQ": {"s": "1", "close_price": "50", "q_pre": 0, "sigma_forecast": "0.10"},
                        "SOXL": {"s": "1", "close_price": "40", "q_pre": 0, "sigma_forecast": "0.10"},
                    },
                },
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            ledger_dir = root / "ledger"
            bundle_dir = root / "bundle"
            bundle_dir.mkdir()

            ledger = ShadowLedger(ledger_dir)
            prev = GENESIS_PREV_HASH
            prior: dict = {}
            for seq, item in enumerate(sessions_in):
                cal = resolve_session("XNYS", item["session_date"], spec=spec)
                built = build_record_body(
                    chain_id="replay-test-chain",
                    sequence=seq,
                    prev_record_sha256=prev,
                    mode="drill",
                    session_date=item["session_date"],
                    decision_timestamp_utc=item["decision_timestamp_utc"],
                    session_inputs=item["inputs"],
                    calendar_session=cal,
                    code_version_sha=code_sha,
                    evaluator=ev,
                    prior_row_state=prior,
                )
                env = ledger.append_record(
                    built["record_body"],
                    obtained_at_utc=item["decision_timestamp_utc"],
                )
                prev = env["record_sha256"]
                prior = built["next_row_state"]

            bundle = {
                "chain_id": "replay-test-chain",
                "mode": "drill",
                "calendar_spec_dir": str(DEFAULT_SPEC_DIR),
                "calendar_spec_digest": spec["spec_digest"],
                "code_version_sha": code_sha,
                "sessions": sessions_in,
            }
            (bundle_dir / "replay_bundle.json").write_text(
                json.dumps(bundle, indent=2),
                encoding="utf-8",
            )

            res = replay_window(bundle_dir, ledger_dir)
            self.assertTrue(res["bit_for_bit_match"])
            self.assertTrue(res["chain_ok"])
            self.assertTrue(res["receipts_ok"])
            self.assertEqual(res["replayed_count"], 2)

            exit_code = replay_cli_main(["--bundle", str(bundle_dir), "--ledger", str(ledger_dir)])
            self.assertEqual(exit_code, 0)

            # Synthetic-stub receipts fail --require-creditable-receipts
            strict_exit = replay_cli_main(
                ["--bundle", str(bundle_dir), "--ledger", str(ledger_dir), "--require-creditable-receipts"]
            )
            self.assertEqual(strict_exit, 1)

    def test_replay_with_ots_receipts_and_archived_market_inputs(self) -> None:
        spec = load_spec(DEFAULT_SPEC_DIR)
        ev = StubEvaluator()
        code_sha = compute_code_manifest_hash()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            archive_dir = root / "archive"
            ledger_dir = root / "ledger"
            bundle_dir = root / "bundle"
            bundle_dir.mkdir()

            mkt_recs = emit_market_records_from_fixtures(
                fixture_dir=DRILL_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T21:15:00.000Z",
                archive_dir=archive_dir,
                calendar_spec=spec,
                series=LIVE_SERIES,
            )
            by_inst = {r["instrument"]: r for r in mkt_recs}
            sess_inputs = {
                "E": "50000",
                "E_star": "50000",
                "H": "0",
                "L": "0",
                "funds": {
                    "TQQQ": {"s": "1", "close_price": by_inst["TQQQ"]["normalized_value"], "q_pre": 0, "sigma_forecast": "0.18"},
                    "SOXL": {"s": "1", "close_price": by_inst["SOXL"]["normalized_value"], "q_pre": 0, "sigma_forecast": "0.24"},
                },
            }
            cal = resolve_session("XNYS", "2025-11-21", spec=spec)
            mkt_digest = sha256_canonical(mkt_recs)
            built = build_record_body(
                chain_id="drill-ots-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                market_records_digest=mkt_digest,
            )
            rec_hash = sha256_canonical(built["record_body"])
            ots_bytes, _ = build_synthetic_ots_fixture(rec_hash)
            ledger = ShadowLedger(ledger_dir)
            ledger.append_record(
                built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
                ots_bytes=ots_bytes,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
            )

            bundle = {
                "chain_id": "drill-ots-replay",
                "mode": "drill",
                "calendar_spec_dir": str(DEFAULT_SPEC_DIR),
                "calendar_spec_digest": spec["spec_digest"],
                "code_version_sha": code_sha,
                "archive_dir": str(archive_dir),
                "sessions": [
                    {
                        "session_date": "2025-11-21",
                        "decision_timestamp_utc": "2025-11-21T21:20:00.000Z",
                        "inputs": sess_inputs,
                        "market_records": mkt_recs,
                        "market_records_digest": mkt_digest,
                    }
                ],
            }
            (bundle_dir / "replay_bundle.json").write_text(json.dumps(bundle, indent=2), encoding="utf-8")

            res = replay_window(bundle_dir, ledger_dir)
            self.assertTrue(res["bit_for_bit_match"])
            self.assertTrue(res["chain_ok"])
            self.assertTrue(res["receipts_ok"])
            # Pending-only OTS without Bitcoin headers cannot claim creditability
            self.assertFalse(res["receipts_creditable"])
            self.assertTrue(res["archived_inputs_ok"])
            self.assertEqual(
                replay_cli_main(["--bundle", str(bundle_dir), "--ledger", str(ledger_dir)]),
                0,
            )
            self.assertEqual(
                replay_cli_main(["--bundle", str(bundle_dir), "--ledger", str(ledger_dir), "--require-creditable-receipts"]),
                1,
            )

            # Finding #3 regression: Bitcoin-attested receipt requires --trusted-headers for creditability
            btc_ledger_dir = root / "btc_ledger"
            btc_ots, btc_root = build_synthetic_ots_fixture(rec_hash, bitcoin_height=870400)
            assert btc_root is not None
            btc_ledger = ShadowLedger(btc_ledger_dir)
            btc_ledger.append_record(
                built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
                ots_bytes=btc_ots,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
            )
            headers_path = root / "trusted_headers.json"
            headers_path.write_text(
                json.dumps({"870400": {"merkle_root": btc_root, "timestamp": "2025-11-21T22:00:00.000Z"}}),
                encoding="utf-8",
            )

            self.assertEqual(
                replay_cli_main(["--bundle", str(bundle_dir), "--ledger", str(btc_ledger_dir), "--require-creditable-receipts"]),
                1,
            )
            self.assertEqual(
                replay_cli_main(
                    [
                        "--bundle",
                        str(bundle_dir),
                        "--ledger",
                        str(btc_ledger_dir),
                        "--trusted-headers",
                        str(headers_path),
                        "--require-creditable-receipts",
                    ]
                ),
                0,
            )

            # Finding #4 regression 1: empty market_records fails closed even when digest field matches
            empty_mkt_dir = root / "bundle_empty_mkt"
            empty_mkt_dir.mkdir()
            empty_bundle = copy.deepcopy(bundle)
            empty_bundle["sessions"][0]["market_records"] = []
            (empty_mkt_dir / "replay_bundle.json").write_text(json.dumps(empty_bundle), encoding="utf-8")
            res_empty = replay_window(empty_mkt_dir, ledger_dir)
            self.assertFalse(res_empty["archived_inputs_ok"])
            self.assertFalse(res_empty["bit_for_bit_match"])

            # Finding #4 regression 2: omitted market_records key fails closed
            omit_mkt_dir = root / "bundle_omit_mkt"
            omit_mkt_dir.mkdir()
            omit_bundle = copy.deepcopy(bundle)
            del omit_bundle["sessions"][0]["market_records"]
            (omit_mkt_dir / "replay_bundle.json").write_text(json.dumps(omit_bundle), encoding="utf-8")
            res_omit = replay_window(omit_mkt_dir, ledger_dir)
            self.assertFalse(res_omit["archived_inputs_ok"])
            self.assertFalse(res_omit["bit_for_bit_match"])

            # Finding #4 regression 3: forged market_records_digest mismatch fails closed
            forged_dir = root / "bundle_forged_digest"
            forged_dir.mkdir()
            forged_bundle = copy.deepcopy(bundle)
            forged_bundle["sessions"][0]["market_records_digest"] = "a" * 64
            (forged_dir / "replay_bundle.json").write_text(json.dumps(forged_bundle), encoding="utf-8")
            res_forged = replay_window(forged_dir, ledger_dir)
            self.assertFalse(res_forged["archived_inputs_ok"])
            self.assertFalse(res_forged["bit_for_bit_match"])

            # Finding #4 regression 4: tampered sizing close_price disagreeing with archived market_records fails closed
            mismatch_price_dir = root / "bundle_price_mismatch"
            mismatch_price_dir.mkdir()
            mismatch_bundle = copy.deepcopy(bundle)
            mismatch_bundle["sessions"][0]["inputs"]["funds"]["TQQQ"]["close_price"] = "999"
            (mismatch_price_dir / "replay_bundle.json").write_text(
                json.dumps(mismatch_bundle), encoding="utf-8"
            )
            res_price = replay_window(mismatch_price_dir, ledger_dir)
            self.assertFalse(res_price["archived_inputs_ok"])
            self.assertFalse(res_price["bit_for_bit_match"])

            # Item #4 regression: 6-series market_records missing QQQ fails completeness check in replay_window
            six_mkt_dir = root / "bundle_six_series"
            six_mkt_dir.mkdir()
            six_recs = emit_market_records_from_fixtures(
                fixture_dir=FEEDER_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T21:15:00.000Z",
                archive_dir=archive_dir,
                calendar_spec=spec,
                series=DEMO_SERIES,
            )
            six_built = build_record_body(
                chain_id="drill-six-series-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                market_records_digest=sha256_canonical(six_recs),
            )
            six_ledger_dir = root / "ledger_six_series"
            ShadowLedger(six_ledger_dir).append_record(
                six_built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
            )
            six_bundle = copy.deepcopy(bundle)
            six_bundle["chain_id"] = "drill-six-series-replay"
            six_bundle["sessions"][0]["market_records"] = six_recs
            six_bundle["sessions"][0]["market_records_digest"] = sha256_canonical(six_recs)
            (six_mkt_dir / "replay_bundle.json").write_text(json.dumps(six_bundle), encoding="utf-8")
            res_six = replay_window(six_mkt_dir, six_ledger_dir)
            self.assertFalse(res_six["archived_inputs_ok"])
            self.assertFalse(res_six["bit_for_bit_match"])
            self.assertIn("missing_required_market_series", "".join(res_six["archive_errors"]))

            # R3 #8 regression: pre-close 20:55Z retrieval WITH forged eligible=True MUST BE REJECTED by replay_window
            pre_close_recs = emit_market_records_from_fixtures(
                fixture_dir=DRILL_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T20:55:00.000Z",
                archive_dir=archive_dir,
                calendar_spec=spec,
                series=LIVE_SERIES,
            )
            for prec in pre_close_recs:
                prec["eligibility"] = {"eligible": True, "reason": None}
            pre_built = build_record_body(
                chain_id="drill-pre-close-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                market_records_digest=sha256_canonical(pre_close_recs),
            )
            pre_ledger_dir = root / "ledger_pre_close"
            ShadowLedger(pre_ledger_dir).append_record(
                pre_built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
            )
            pre_bundle_dir = root / "bundle_pre_close"
            pre_bundle_dir.mkdir()
            pre_bundle = copy.deepcopy(bundle)
            pre_bundle["chain_id"] = "drill-pre-close-replay"
            pre_bundle["sessions"][0]["market_records"] = pre_close_recs
            pre_bundle["sessions"][0]["market_records_digest"] = sha256_canonical(pre_close_recs)
            (pre_bundle_dir / "replay_bundle.json").write_text(json.dumps(pre_bundle), encoding="utf-8")
            res_pre = replay_window(pre_bundle_dir, pre_ledger_dir)
            self.assertFalse(res_pre["archived_inputs_ok"])
            self.assertFalse(res_pre["bit_for_bit_match"])
            self.assertIn("retrieval_before_session_close", "".join(res_pre["archive_errors"]))

            # R4 #8 regression: retrieval AFTER decision_timestamp_utc (even inside EOD cutoff) MUST BE REJECTED
            post_dec_recs = emit_market_records_from_fixtures(
                fixture_dir=DRILL_FIXTURES,
                session_date="2025-11-21",
                retrieval_time_utc="2025-11-21T21:30:00.000Z",
                archive_dir=archive_dir,
                calendar_spec=spec,
                series=LIVE_SERIES,
            )
            post_dec_built = build_record_body(
                chain_id="drill-post-dec-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                market_records_digest=sha256_canonical(post_dec_recs),
            )
            post_dec_ledger_dir = root / "ledger_post_dec"
            ShadowLedger(post_dec_ledger_dir).append_record(
                post_dec_built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
            )
            post_dec_bundle_dir = root / "bundle_post_dec"
            post_dec_bundle_dir.mkdir()
            post_dec_bundle = copy.deepcopy(bundle)
            post_dec_bundle["chain_id"] = "drill-post-dec-replay"
            post_dec_bundle["sessions"][0]["market_records"] = post_dec_recs
            post_dec_bundle["sessions"][0]["market_records_digest"] = sha256_canonical(post_dec_recs)
            (post_dec_bundle_dir / "replay_bundle.json").write_text(json.dumps(post_dec_bundle), encoding="utf-8")
            res_post_dec = replay_window(post_dec_bundle_dir, post_dec_ledger_dir)
            self.assertFalse(res_post_dec["archived_inputs_ok"])
            self.assertFalse(res_post_dec["bit_for_bit_match"])
            self.assertIn("retrieval_after_decision_timestamp", "".join(res_post_dec["archive_errors"]))

            # R5 #8 regression: injecting VINTAGE_Y2 on a single TQQQ market record inside a drill session
            # MUST NOT bypass real-time retrieval_time_utc <= decision_timestamp_utc bounds!
            spoof_y2_recs = copy.deepcopy(mkt_recs)
            tqqq_spoof = next(r for r in spoof_y2_recs if r["instrument"] == "TQQQ")
            tqqq_spoof["vintage"] = VINTAGE_Y2
            tqqq_spoof["retrieval_time_utc"] = "2025-11-21T21:30:00.000Z"  # > 21:20Z decision time
            spoof_y2_built = build_record_body(
                chain_id="drill-spoof-y2-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                discipline_labels={"drill_chain": True, "forward_clock_credit_eligible": False},
                market_records_digest=sha256_canonical(spoof_y2_recs),
            )
            spoof_y2_ledger_dir = root / "ledger_spoof_y2"
            spoof_y2_hash = sha256_canonical(spoof_y2_built["record_body"])
            spoof_y2_ots, spoof_y2_root = build_synthetic_ots_fixture(spoof_y2_hash, bitcoin_height=870905)
            assert spoof_y2_root is not None
            ShadowLedger(spoof_y2_ledger_dir).append_record(
                spoof_y2_built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
                ots_bytes=spoof_y2_ots,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
                bitcoin_merkle_roots={870905: {"merkle_root": spoof_y2_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            spoof_y2_bundle_dir = root / "bundle_spoof_y2"
            spoof_y2_bundle_dir.mkdir()
            spoof_y2_bundle = copy.deepcopy(bundle)
            spoof_y2_bundle["chain_id"] = "drill-spoof-y2-replay"
            spoof_y2_bundle["sessions"][0]["discipline_labels"] = {
                "drill_chain": True,
                "forward_clock_credit_eligible": False,
            }
            spoof_y2_bundle["sessions"][0]["market_records"] = spoof_y2_recs
            spoof_y2_bundle["sessions"][0]["market_records_digest"] = sha256_canonical(spoof_y2_recs)
            (spoof_y2_bundle_dir / "replay_bundle.json").write_text(json.dumps(spoof_y2_bundle), encoding="utf-8")
            res_spoof_y2 = replay_window(
                spoof_y2_bundle_dir,
                spoof_y2_ledger_dir,
                bitcoin_merkle_roots={870905: {"merkle_root": spoof_y2_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            self.assertFalse(res_spoof_y2["archived_inputs_ok"])
            self.assertFalse(res_spoof_y2["bit_for_bit_match"])
            self.assertIn("retrieval_after_decision_timestamp", "".join(res_spoof_y2["archive_errors"]))

            # R6 #8 regression: duplicate TQQQ record (1st VINTAGE_Y2 after decision_timestamp_utc, 2nd timely)
            # MUST NOT overwrite and hide the out-of-bounds record!
            dup_y2_first = copy.deepcopy(tqqq_spoof)
            dup_y2_recs = [dup_y2_first] + copy.deepcopy(mkt_recs)
            dup_y2_built = build_record_body(
                chain_id="drill-dup-y2-replay",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                discipline_labels={"drill_chain": True, "forward_clock_credit_eligible": False},
                market_records_digest=sha256_canonical(dup_y2_recs),
            )
            dup_y2_ledger_dir = root / "ledger_dup_y2"
            dup_y2_hash = sha256_canonical(dup_y2_built["record_body"])
            dup_y2_ots, dup_y2_root = build_synthetic_ots_fixture(dup_y2_hash, bitcoin_height=870906)
            assert dup_y2_root is not None
            ShadowLedger(dup_y2_ledger_dir).append_record(
                dup_y2_built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
                ots_bytes=dup_y2_ots,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
                bitcoin_merkle_roots={870906: {"merkle_root": dup_y2_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            dup_y2_bundle_dir = root / "bundle_dup_y2"
            dup_y2_bundle_dir.mkdir()
            dup_y2_bundle = copy.deepcopy(bundle)
            dup_y2_bundle["chain_id"] = "drill-dup-y2-replay"
            dup_y2_bundle["sessions"][0]["discipline_labels"] = {
                "drill_chain": True,
                "forward_clock_credit_eligible": False,
            }
            dup_y2_bundle["sessions"][0]["market_records"] = dup_y2_recs
            dup_y2_bundle["sessions"][0]["market_records_digest"] = sha256_canonical(dup_y2_recs)
            (dup_y2_bundle_dir / "replay_bundle.json").write_text(json.dumps(dup_y2_bundle), encoding="utf-8")
            res_dup_y2 = replay_window(
                dup_y2_bundle_dir,
                dup_y2_ledger_dir,
                bitcoin_merkle_roots={870906: {"merkle_root": dup_y2_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            self.assertFalse(res_dup_y2["archived_inputs_ok"])
            self.assertFalse(res_dup_y2["bit_for_bit_match"])
            dup_err_str = "".join(res_dup_y2["archive_errors"])
            self.assertIn("duplicate_market_record_instrument:TQQQ", dup_err_str)
            self.assertIn("retrieval_after_decision_timestamp", dup_err_str)

    def test_y2_reconstruction_replay_six_series_succeeds_while_drill_requires_qqq(self) -> None:
        """P2 regression: noncreditable Y2 reconstruction replays cleanly with 6 series; drills require 7 incl. QQQ."""
        y2_dir = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "y2overlap"
        spec = load_spec(DEFAULT_SPEC_DIR)
        code_sha = compute_code_manifest_hash()
        p_hash = policy_config_hash(calendar_spec=spec, code_manifest_hash=code_sha)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            res = run_reconstruction_window(
                y2_dir=y2_dir,
                ledger_dir=root / "y2_ledger",
                archive_dir=root / "y2_arch",
            )
            envs = res["ledger"].load_records()
            self.assertEqual(len(envs), 10)
            y2_bundle = {
                "bundle_version": "shadow-replay-bundle/v1",
                "chain_id": "y2-overlap-reconstruction",
                "mode": "reconstruction",
                "calendar_spec_dir": str(DEFAULT_SPEC_DIR),
                "calendar_spec_digest": spec["spec_digest"],
                "code_version_sha": code_sha,
                "policy_config_hash": p_hash,
                "archive_dir": str(root / "y2_arch"),
                "sessions": [
                    {
                        "session_date": env["record_body"]["session_date"],
                        "decision_timestamp_utc": env["record_body"]["decision_timestamp_utc"],
                        "inputs": {
                            "E": "50000",
                            "E_star": "50000",
                            "H": "0",
                            "L": "0",
                            "funds": {
                                "TQQQ": {
                                    "s": "1",
                                    "close_price": next(
                                        r["normalized_value"]
                                        for r in res["market_records"][env["record_body"]["session_date"]]
                                        if r["instrument"] == "TQQQ"
                                    ),
                                    "q_pre": 0,
                                    "sigma_forecast": "0.24",
                                },
                                "SOXL": {
                                    "s": "1",
                                    "close_price": next(
                                        r["normalized_value"]
                                        for r in res["market_records"][env["record_body"]["session_date"]]
                                        if r["instrument"] == "SOXL"
                                    ),
                                    "q_pre": 0,
                                    "sigma_forecast": "0.24",
                                },
                            },
                        },
                        "discipline_labels": env["record_body"]["discipline_labels"],
                        "market_records": res["market_records"][env["record_body"]["session_date"]],
                        "market_records_digest": feeder_sha256(res["market_records"][env["record_body"]["session_date"]]),
                    }
                    for env in envs
                ],
            }
            (root / "replay_bundle.json").write_text(json.dumps(y2_bundle), encoding="utf-8")
            replayed = replay_window(root, root / "y2_ledger")
            self.assertTrue(replayed["chain_ok"], replayed)
            self.assertTrue(replayed["archived_inputs_ok"], replayed)
            self.assertTrue(replayed["bit_for_bit_match"], replayed)
            self.assertEqual(replayed["replayed_count"], 10)
            self.assertEqual(replayed["archive_errors"], [])

            # Non-Y2 drill session missing QQQ MUST still fail closed with missing_required_market_series:['QQQ']
            drill_ledger = ShadowLedger(root / "drill_ledger")
            cal0 = resolve_session("XNYS", "2025-11-21", spec=spec)
            recs_full = emit_market_records_from_fixtures(
                FEEDER_FIXTURES,
                "2025-11-21",
                "2025-11-21T21:20:00.000Z",
                root / "drill_arch",
                calendar_spec=spec,
            )
            recs_no_qqq = [copy.deepcopy(r) for r in recs_full if r["instrument"] != "QQQ"]
            sess0_in = {
                "E": "50000",
                "E_star": "50000",
                "H": "0",
                "L": "0",
                "funds": {
                    "TQQQ": {
                        "s": "1",
                        "close_price": next(r["normalized_value"] for r in recs_no_qqq if r["instrument"] == "TQQQ"),
                        "q_pre": 0,
                        "sigma_forecast": "0.24",
                    },
                    "SOXL": {
                        "s": "1",
                        "close_price": next(r["normalized_value"] for r in recs_no_qqq if r["instrument"] == "SOXL"),
                        "q_pre": 0,
                        "sigma_forecast": "0.24",
                    },
                },
            }
            built = build_record_body(
                chain_id="drill-missing-qqq",
                sequence=0,
                prev_record_sha256=GENESIS_PREV_HASH,
                mode="drill",
                session_date="2025-11-21",
                decision_timestamp_utc="2025-11-21T21:20:00.000Z",
                session_inputs=sess0_in,
                calendar_session=cal0,
                code_version_sha=code_sha,
                evaluator=StubEvaluator(),
                discipline_labels={"drill_chain": True, "forward_clock_credit_eligible": False},
                market_records_digest=feeder_sha256(recs_no_qqq),
            )
            rec_hash = sha256_canonical(built["record_body"])
            ots_bytes, b_root = build_synthetic_ots_fixture(rec_hash, bitcoin_height=870907)
            assert b_root is not None
            drill_ledger.append_record(
                built["record_body"],
                obtained_at_utc="2025-11-21T21:22:00.000Z",
                ots_bytes=ots_bytes,
                commitment_deadline_utc="2025-11-22T04:30:00.000Z",
                bitcoin_merkle_roots={870907: {"merkle_root": b_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            drill_bundle = {
                "bundle_version": "shadow-replay-bundle/v1",
                "chain_id": "drill-missing-qqq",
                "mode": "drill",
                "calendar_spec_dir": str(DEFAULT_SPEC_DIR),
                "calendar_spec_digest": spec["spec_digest"],
                "code_version_sha": code_sha,
                "policy_config_hash": p_hash,
                "archive_dir": str(root / "drill_arch"),
                "sessions": [
                    {
                        "session_date": "2025-11-21",
                        "decision_timestamp_utc": "2025-11-21T21:20:00.000Z",
                        "inputs": sess0_in,
                        "discipline_labels": {"drill_chain": True, "forward_clock_credit_eligible": False},
                        "market_records": recs_no_qqq,
                        "market_records_digest": feeder_sha256(recs_no_qqq),
                    }
                ],
            }
            (root / "drill_bundle").mkdir()
            import shutil
            shutil.copytree(root / "drill_arch", root / "drill_bundle" / "drill_arch")
            (root / "drill_bundle" / "replay_bundle.json").write_text(json.dumps(drill_bundle), encoding="utf-8")
            drill_res = replay_window(
                root / "drill_bundle",
                root / "drill_ledger",
                bitcoin_merkle_roots={870907: {"merkle_root": b_root, "timestamp": "2025-11-21T22:00:00.000Z"}},
            )
            self.assertFalse(drill_res["archived_inputs_ok"])
            self.assertFalse(drill_res["bit_for_bit_match"])
            self.assertIn("missing_required_market_series:['QQQ']", "".join(drill_res["archive_errors"]))

    def test_replay_rejects_mismatched_prior_row_state_used_after_seq_zero(self) -> None:
        import dataclasses
        from datetime import datetime, timezone
        from shadow.fuse_adapter import (
            PINNED_CSA22_V2_DIGEST,
            PINNED_FUSE_ENGINE_HASH,
            PINNED_FUSE_SPEC_VERSION,
        )
        from shadow.scheduler import REPO_ROOT, load_scheduler_config, run_daily_drill_cycle

        demo_dir = str(REPO_ROOT / "demo")
        if demo_dir not in sys.path:
            sys.path.insert(0, demo_dir)
        from run_sh2_demo import _build_session_fixture_files

        drill_cfg = dataclasses.replace(
            load_scheduler_config(REPO_ROOT / "config" / "drill-scheduler.json"),
            chain_id="drill-prior-state-mismatch",
            calendar_version="csa22-v2",
            calendar_spec_digest=PINNED_CSA22_V2_DIGEST,
            fuse_spec_version=PINNED_FUSE_SPEC_VERSION,
            fuse_engine_hash=PINNED_FUSE_ENGINE_HASH,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fix_root = root / "fixtures"
            for idx, ds in enumerate(("2026-01-05", "2026-01-06")):
                _build_session_fixture_files(fix_root / ds, ds)
                res = run_daily_drill_cycle(
                    session_date=ds,
                    data_dir=root,
                    config=drill_cfg,
                    fixtures_dir=fix_root / ds,
                    sigma_by_inst={"TQQQ": "0.50" if idx == 0 else "0.24", "SOXL": "0.24"},
                    long_run_vol_by_inst={"TQQQ": "0.24", "SOXL": "0.24"},
                    wall_clock=lambda d=ds: datetime.fromisoformat(f"{d}T22:30:00+00:00"),
                )
                self.assertTrue(res["appended"])

            bundle_path = root / "replay" / "replay_bundle.json"
            orig_bundle_bytes = bundle_path.read_bytes()

            # (a) Unanchored tampered prior_row_state_used
            bundle_a = json.loads(orig_bundle_bytes.decode("utf-8"))
            bundle_a["sessions"][1].setdefault("fuse_context", {})["prior_row_state_used"] = {"tampered": True}
            bundle_path.write_text(json.dumps(bundle_a), encoding="utf-8")

            out_a = replay_window(root / "replay", root / "ledger")
            self.assertFalse(out_a["bit_for_bit_match"])
            self.assertTrue(any("seq_1:prior_row_state_used_mismatch" in m for m in out_a["mismatches"]))

            # (b) Forged breaker downgrade (clearing V4) even with matching base SHA256 and valid date
            snap0 = json.loads((root / "state_snapshots" / "seq_000000.state.json").read_text(encoding="utf-8"))
            forged_prior = json.loads(json.dumps(snap0["next_row_state"]))
            forged_prior["V4"] = {}
            bundle_b = json.loads(orig_bundle_bytes.decode("utf-8"))
            fctx_b = bundle_b["sessions"][1].setdefault("fuse_context", {})
            fctx_b["prior_row_state_used"] = forged_prior
            fctx_b["prior_row_state_base_sha256"] = snap0["state_sha256"]
            fctx_b["prior_rejected_session_date"] = "2026-01-06"
            bundle_path.write_text(json.dumps(bundle_b), encoding="utf-8")

            out_b = replay_window(root / "replay", root / "ledger")
            self.assertFalse(out_b["bit_for_bit_match"])
            self.assertTrue(any("seq_1:prior_row_state_used_mismatch" in m for m in out_b["mismatches"]))


if __name__ == "__main__":
    unittest.main()
