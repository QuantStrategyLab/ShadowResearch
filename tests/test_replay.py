"""Unit tests for deterministic replay CLI and bit-for-bit verification."""

from __future__ import annotations

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
from shadow.ledger import (
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    compute_code_manifest_hash,
)
from shadow.replay import main as replay_cli_main
from shadow.replay import replay_window


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
                env = ledger.append_record(built["record_body"])
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


if __name__ == "__main__":
    unittest.main()
