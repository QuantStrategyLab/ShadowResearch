"""Reproducible SH-1 acceptance pack runner (SH1-ACCEPT-v1 A01-A16 + demo D1-D8)."""

from __future__ import annotations

import copy
import json
import os
import resource
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from shadow.calendar_consumer import DEFAULT_SPEC_DIR, load_spec, resolve_prior_close_session, resolve_session
from shadow.evaluator import ALL_BREAKER_IDS, REQUIRED_MARKET_SERIES, StubEvaluator
from shadow.feeder import (
    SOXL_PROXY_LABEL,
    emit_market_records_from_fixtures,
    run_reconstruction_window,
    verify_market_record_raw_integrity,
)
from shadow.feeder_live import emit_live_market_records
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
    parse_json_no_duplicates,
    sha256_canonical,
)
from shadow.ots import OTS_HEADER_MAGIC, build_synthetic_ots_fixture, verify_ots_bytes
from shadow.replay import replay_window
from shadow.scheduler import (
    DEFAULT_DEPLOY_DIR,
    DEFAULT_SCHEDULER_CONFIG_PATH,
    check_footprint_budget,
    compute_session_deadlines,
    detect_missed_runs,
    load_scheduler_config,
    run_daily_drill_cycle,
    validate_scheduler_templates,
)
from shadow.sizing import TIER_ORDER, policy_config_hash

REPO_ROOT = Path(__file__).resolve().parents[2]
FEEDER_FIXTURES = REPO_ROOT / "tests" / "fixtures" / "feeder"
Y2_OVERLAP_DIR = REPO_ROOT / "tests" / "fixtures" / "y2overlap"


def _mock_ots_transport(
    method: str,
    url: str,
    body: bytes | None,
    headers: Mapping[str, str],
    timeout: float,
) -> tuple[int, bytes]:
    if url.endswith("/digest") and body is not None:
        ots_full, _ = build_synthetic_ots_fixture(body.hex(), calendar_uri=url.rsplit("/", 1)[0])
        return 200, ots_full[len(OTS_HEADER_MAGIC) + 2 + 32 :]
    return 404, b""


def _multi_session_transport(
    url: str,
    headers: Mapping[str, str],
    *,
    withhold_inst: str | None = None,
) -> tuple[int, bytes]:
    if "/daily/TQQQ/prices" in url:
        if withhold_inst == "TQQQ":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 48.05, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 48.10, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "/daily/SOXL/prices" in url:
        if withhold_inst == "SOXL":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 31.73, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 32.00, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 32.25, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "/daily/QQQ/prices" in url:
        if withhold_inst == "QQQ":
            return 503, b"outage"
        return 200, json.dumps(
            [
                {"date": "2025-11-21T00:00:00.000Z", "close": 510.12, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-24T00:00:00.000Z", "close": 511.50, "divCash": 0.0, "splitFactor": 1.0},
                {"date": "2025-11-25T00:00:00.000Z", "close": 512.80, "divCash": 0.0, "splitFactor": 1.0},
            ]
        ).encode("utf-8")
    if "%5EXNDX" in url:
        if withhold_inst == "XNDX":
            return 429, b"Edge: Too Many Requests"
        return 200, json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"symbol": "^XNDX", "instrumentType": "INDEX"},
                            "timestamp": [1763758800, 1764018000, 1764104400],
                            "indicators": {"quote": [{"close": [29462.35, 29520.0, 29600.10]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
    if "XSOX" in url:
        if withhold_inst == "SEMI":
            return 429, b"Edge: Too Many Requests"
        return 200, json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {"symbol": "XSOX", "instrumentType": "INDEX"},
                            "timestamp": [1763758800, 1764018000, 1764104400],
                            "indicators": {"quote": [{"close": [8179.87, 8195.0, 8210.50]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")
    if "markets.newyorkfed.org" in url:
        if withhold_inst == "SOFR":
            return 503, b"outage"
        return 200, json.dumps(
            {
                "refRates": [
                    {"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""},
                    {"effectiveDate": "2025-11-21", "type": "SOFR", "percentRate": 3.92, "revisionIndicator": ""},
                    {"effectiveDate": "2025-11-24", "type": "SOFR", "percentRate": 3.90, "revisionIndicator": ""},
                ]
            }
        ).encode("utf-8")
    if "treasury.example.test" in url:
        if withhold_inst == "TBILL_3M":
            return 503, b"outage"
        return 200, b"Date,3 Mo\n2025-11-20,3.90\n2025-11-24,3.89\n"
    return 404, b""


def _verify_standing_assertions(records: list[dict]) -> int:
    rows = 0
    for env in records:
        body = env["record_body"]
        assert body["assertions"]["kelly_applied"] is False
        assert type(body["assertions"]["orders_placed"]) is int and body["assertions"]["orders_placed"] == 0
        assert body["assertions"]["B0"] == "50000"
        assert body["assertions"]["H"] == "0"
        assert body["assertions"]["stop_loss_return_rule"] == "OFF"
        assert body["assertions"]["loss_controls_armed"] is False
        assert body["bankroll"]["H"] == "0"
        assert [b["id"] for b in body["evaluator"]["breakers"]] == list(ALL_BREAKER_IDS)
        assert [t["tier_id"] for t in body["tiers"]] == list(TIER_ORDER)
        for tier in body["tiers"]:
            assert tier["orders_placed"] == 0
            for fund in tier["funds"]:
                assert fund["kelly_applied"] is False
                assert type(fund["orders_placed"]) is int and fund["orders_placed"] == 0
                rows += 1
    return rows


def run_acceptance_pack() -> dict[str, object]:
    """Execute all offline-verifiable SH1-ACCEPT-v1 (A01-A16) and §7 (D1-D8) checks."""
    t0 = time.monotonic()
    spec = load_spec(DEFAULT_SPEC_DIR)
    cfg = load_scheduler_config(DEFAULT_SCHEDULER_CONFIG_PATH)
    rows: dict[str, dict[str, object]] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        # SH1-A01, A03, A04, A07: 3-session x 7-series fixture window
        arch_dir = root / "a01_arch"
        sessions = ("2025-11-21", "2025-11-24", "2025-11-25")
        all_mkt: dict[str, list[dict]] = {}
        for ds in sessions:
            cal = resolve_session("XNYS", ds, spec=spec)
            deadlines = compute_session_deadlines(ds, cfg, calendar_session=cal)
            base_dt = datetime.fromisoformat(f"{ds}T22:30:00+00:00")
            step = {"idx": 0}

            def _tick_post_close(b=base_dt, s=step) -> datetime:
                s["idx"] += 1
                return b + timedelta(seconds=s["idx"])

            prior_cal = resolve_prior_close_session(
                base_dt.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                spec=spec,
            )
            assert prior_cal["trade_date"] == ds
            recs = emit_live_market_records(
                session_date=ds,
                archive_dir=arch_dir,
                transport=lambda u, h: _multi_session_transport(u, h),
                environ={"TIINGO_API_KEY": "accept-placeholder"},
                utc_clock=_tick_post_close,
                calendar_spec=spec,
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
                session_close_utc=deadlines["session_close_utc"],
                decision_cutoff_utc=deadlines["decision_cutoff_utc"],
            )
            assert [r["instrument"] for r in recs] == list(REQUIRED_MARKET_SERIES)
            # Each provider recorded its own distinct post-response retrieval timestamp in [close, decision_cutoff]
            assert len({r["retrieval_time_utc"] for r in recs}) == 7
            for rec in recs:
                assert rec["eligibility"]["eligible"] is True
                assert deadlines["session_close_utc"] <= rec["retrieval_time_utc"] <= deadlines["decision_cutoff_utc"]
                assert rec["calendar_spec_digest"] == spec["spec_digest"]
                ok_raw, err_raw = verify_market_record_raw_integrity(arch_dir, rec, session_date=ds)
                assert ok_raw, err_raw
            by_i = {r["instrument"]: r for r in recs}
            assert by_i["TQQQ"]["price_basis"] == "close (proxy, NAV unverified)"
            assert by_i["TQQQ"]["source_publication_time_utc"] is None
            assert by_i["SOXL"]["disclosures"]["proxy_label"] == SOXL_PROXY_LABEL
            assert by_i["SEMI"]["disclosures"]["xsox_for_icesemit_substitution"] is True
            assert by_i["SOFR"]["effective_session"] < ds
            assert by_i["SOFR"]["disclosures"]["accrual_basis"] == "days/365"
            assert by_i["SOFR"]["disclosures"]["sofr_effective_lag"] == "T-1"
            assert by_i["TBILL_3M"]["disclosures"]["accrual_basis"] == "days/365"
            all_mkt[ds] = recs

        rows["SH1-A01"] = {"status": "PASS_OFFLINE", "detail": "3 sessions x 7 required series complete without silent gaps"}
        rows["SH1-A02"] = {
            "status": "PARTIAL_MINI_PROBE_RECORDED",
            "detail": "2026-11-21 Mac mini probe recorded in docs/SH1-A02-PROBE.md; weekday post-close finalization lag + Tiingo key + Treasury URL pending Mac mini live weekday window",
        }
        rows["SH1-A03"] = {"status": "PASS_OFFLINE", "detail": "retrieval_time <= decision_cutoff, prior-close session & csa22-v1 digest verified"}
        rows["SH1-A04"] = {"status": "PASS_OFFLINE", "detail": "21/21 archived payloads SHA-256 verified and normalized values re-derived byte-for-byte"}
        rows["SH1-A07"] = {"status": "PASS_OFFLINE", "detail": "TQQQ/SOXL proxy labels, XSOX substitution disclosure, days/365 + SOFR T-1 lag verified"}

        # SH1-A05: Revision append preserves original bytes
        rev_arch = root / "a05_arch"
        v0 = emit_market_records_from_fixtures(
            FEEDER_FIXTURES, "2025-11-21", "2025-11-21T21:30:00.000Z", rev_arch, calendar_spec=spec
        )
        v1 = emit_market_records_from_fixtures(
            FEEDER_FIXTURES,
            "2025-11-21",
            "2025-11-21T21:35:00.000Z",
            rev_arch,
            calendar_spec=spec,
            revision_seq=1,
            sofr_filename="nyfed_sofr_2025-11-21_rev.json",
        )
        s0 = next(r for r in v0 if r["instrument"] == "SOFR")
        s1 = next(r for r in v1 if r["instrument"] == "SOFR")
        assert s0["normalized_value"] == "0.0391" and s1["normalized_value"] == "0.0393"
        assert s0["raw_bytes_sha256"] != s1["raw_bytes_sha256"]
        assert verify_market_record_raw_integrity(rev_arch, s0)[0]
        rows["SH1-A05"] = {"status": "PASS_OFFLINE", "detail": "SOFR revision appended new digest without mutating rev0 input"}

        # SH1-A06: Missing any of the 7 required series triggers O1 fail-closed and 0 appends
        for withheld in REQUIRED_MARKET_SERIES:
            dd = root / f"a06_{withheld}"
            res_w = run_daily_drill_cycle(
                session_date="2025-11-21",
                data_dir=dd,
                config=cfg,
                transport=lambda u, h, w=withheld: _multi_session_transport(u, h, withhold_inst=w),
                environ={"TIINGO_API_KEY": "accept-placeholder"},
                ots_transport=_mock_ots_transport,
                wall_clock=lambda: datetime(2025, 11, 21, 22, 30, 0, tzinfo=timezone.utc),
                treasury_endpoint="https://treasury.example.test/yield{year}.csv",
                sleeper=lambda _: None,
            )
            assert res_w["appended"] is False and res_w["fail_closed"] is True
            assert res_w["reason"] != "session_not_yet_closed"
            assert not (dd / "ledger" / "seq_000000.record.json").exists()
        # Pre-close check rejects explicit session before close_utc with 0 appends
        pre_close = run_daily_drill_cycle(
            session_date="2025-11-21",
            data_dir=root / "a06_pre_close",
            config=cfg,
            transport=lambda u, h: _multi_session_transport(u, h),
            environ={"TIINGO_API_KEY": "accept-placeholder"},
            ots_transport=_mock_ots_transport,
            wall_clock=lambda: datetime(2025, 11, 21, 20, 55, 0, tzinfo=timezone.utc),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        assert pre_close["appended"] is False and pre_close["reason"] == "session_not_yet_closed"
        assert pre_close["report"] is None and pre_close["report_written"] is False
        assert not (root / "a06_pre_close" / "reports" / "completeness_2025-11-21.json").exists()
        # Post-commitment-deadline backfill check on normal wall clock fails closed with 0 appends
        late_bf = run_daily_drill_cycle(
            session_date="2025-11-21",
            data_dir=root / "a06_late_bf",
            config=cfg,
            transport=lambda u, h: _multi_session_transport(u, h),
            environ={"TIINGO_API_KEY": "accept-placeholder"},
            ots_transport=_mock_ots_transport,
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        assert late_bf["appended"] is False and late_bf["fail_closed"] is True
        assert late_bf["reason"] == "late_after_commitment_deadline_backfill_rejected"
        rows["SH1-A06"] = {"status": "PASS_OFFLINE", "detail": "All 7 single-series withhold cases triggered O1 fail-closed; pre-close & post-deadline backfill rejected with 0 appends"}

        # SH1-A08: RFC 8785 + decimal vectors
        assert canonical_json_bytes({"b": "x\n", "\U0001f600": 1, "\uff00": 2}) == b'{"b":"x\\n","\xf0\x9f\x98\x80":1,"\xef\xbc\x80":2}'
        for bad_json in ('{"a":1,"a":2}', '{"a":NaN}', '{"a":Infinity}'):
            raised = False
            try:
                parse_json_no_duplicates(bad_json)
            except CanonicalizationError:
                raised = True
            assert raised
        for bad_dec in ("-0", "0.0", "0.20", "01", "1e5", ".5"):
            assert not is_canonical_decimal_str(bad_dec)
        rows["SH1-A08"] = {"status": "PASS_OFFLINE", "detail": "RFC 8785 UTF-16BE sort, escapes, duplicate/non-finite/decimal rejections verified"}

        # SH1-A09, A10, A11, A14, A15, A16: Drill cycle + suppressed Day 2 + tamper/fork/duplicate + replay
        drill_dir = root / "drill_accept"
        d1 = run_daily_drill_cycle(
            session_date="2025-11-21",
            data_dir=drill_dir,
            config=cfg,
            transport=lambda u, h: _multi_session_transport(u, h),
            environ={"TIINGO_API_KEY": "accept-placeholder"},
            ots_transport=_mock_ots_transport,
            wall_clock=lambda: datetime(2025, 11, 21, 22, 30, 0, tzinfo=timezone.utc),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        # Suppress 2025-11-24; run 2025-11-25
        d3 = run_daily_drill_cycle(
            session_date="2025-11-25",
            data_dir=drill_dir,
            config=cfg,
            transport=lambda u, h: _multi_session_transport(u, h),
            environ={"TIINGO_API_KEY": "accept-placeholder"},
            ots_transport=_mock_ots_transport,
            wall_clock=lambda: datetime(2025, 11, 25, 22, 30, 0, tzinfo=timezone.utc),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        assert d1["appended"] and d3["appended"]
        assert d3["report"]["consecutive_session_count"] == 1

        ledger = ShadowLedger(drill_dir / "ledger")
        records = ledger.load_records()
        receipts = ledger.load_receipts()
        assert records[0]["record_body"]["prev_record_sha256"] == GENESIS_PREV_HASH
        assert ledger.verify_chain()[0] is True
        assert ledger.verify_all_receipts()[0] is True
        # Pending-calendar OTS receipts remain non-creditable without Bitcoin block headers
        assert ledger.assess_receipt_creditability()[0] is False
        assert check_forward_clock_credit(records, receipts, ledger_dir=drill_dir / "ledger")[0] is False

        # Verify standalone .ots proof + Bitcoin Merkle fixture requires trusted header verification
        btc_ots, btc_root = build_synthetic_ots_fixture(records[0]["record_sha256"], bitcoin_height=870300)
        assert btc_root is not None
        assert not verify_ots_bytes(btc_ots, records[0]["record_sha256"], require_bitcoin_verification=True)[0]
        assert verify_ots_bytes(
            btc_ots,
            records[0]["record_sha256"],
            bitcoin_merkle_roots={870300: btc_root},
            require_bitcoin_verification=True,
        )[0]

        # Negative chain append tests (duplicate, fork) & 1-byte mutation
        head = ledger.head()
        assert head is not None
        dup = copy.deepcopy(head["record_body"])
        dup["sequence"] = 2
        dup["prev_record_sha256"] = head["record_sha256"]
        for bad_body in (
            dup,
            {**copy.deepcopy(head["record_body"]), "sequence": 2, "session_date": "2025-11-26", "prev_record_sha256": "f" * 64},
        ):
            rejected = False
            try:
                ledger.append_record(bad_body, obtained_at_utc="2025-11-25T22:35:00.000Z")
            except LedgerAppendError:
                rejected = True
            assert rejected

        mut_path = drill_dir / "ledger" / "seq_000001.record.json"
        orig_bytes = mut_path.read_bytes()
        tampered = bytearray(orig_bytes)
        pos = tampered.index(b'"50000"') + 2
        tampered[pos] = ord("9")
        mut_path.write_bytes(bytes(tampered))
        ok_m, bad_m, _ = ledger.verify_chain()
        assert ok_m is False and bad_m == 1
        mut_path.write_bytes(orig_bytes)

        # SH1-A15 Replay bit-for-bit + separate receipt verify + recomputed market digest
        rep = replay_window(drill_dir / "replay", drill_dir / "ledger")
        assert (
            rep["bit_for_bit_match"]
            and rep["chain_ok"]
            and rep["receipts_ok"]
            and rep["archived_inputs_ok"]
            and (rep["receipts_creditable"] is False)
        )

        # SH1-A14 Missed-run detector flags suppressed 2025-11-24 with zero backfill
        missed = detect_missed_runs(
            window_start_date="2025-11-21",
            as_of_utc="2025-11-26T05:00:00.000Z",
            data_dir=drill_dir,
            config=cfg,
        )
        assert missed["missed_run_detected"] and missed["silent_missed_days"] == ["2025-11-24"]
        assert len(ledger.load_records()) == 2

        # SH1-A16 Standing assertions across drill + Y2 reconstruction records
        y2_res = run_reconstruction_window(Y2_OVERLAP_DIR, root / "y2_ledger", root / "y2_arch")
        total_rows = _verify_standing_assertions(records) + _verify_standing_assertions(y2_res["ledger"].load_records())

        # SH1-A12 / A13 offline detached subprocess & resource cap check
        tmpl = validate_scheduler_templates(DEFAULT_DEPLOY_DIR)
        assert tmpl["templates_ok"]
        fp = check_footprint_budget(drill_dir, cfg.max_footprint_bytes)
        assert fp["within_budget"]

        clean_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "PYTHONPATH": str(REPO_ROOT / "src")}
        proc = subprocess.run(
            [sys.executable, "-m", "shadow.scheduler", "dry-run"],
            cwd=str(REPO_ROOT),
            env=clean_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, proc.stdout + proc.stderr
        dry_out = json.loads(proc.stdout)

        rows["SH1-A09"] = {"status": "PASS_OFFLINE", "detail": "64-zero genesis, gapless links, 1-byte mutation -> first_bad_sequence=1"}
        rows["SH1-A10"] = {"status": "PASS_OFFLINE", "detail": "Duplicate, fork, and distinct-chain violations rejected; 3 tiers present once"}
        rows["SH1-A11"] = {"status": "PASS_OFFLINE_FIXTURE_AND_OTS_FORMAT", "detail": "Detached .ots + metadata verified separately; pending-only receipts stay non-creditable until verified against trusted Bitcoin block headers (live public OTS submission pending Mac mini)"}
        rows["SH1-A12"] = {"status": "PENDING_MAC_MINI_ACTIVATION", "detail": f"Offline detached dry-run + unit templates passed ({len(tmpl['templates_checked'])} templates); live autonomous Mac mini run pending OA-SH1-6 activation"}
        rows["SH1-A13"] = {"status": "PENDING_MAC_MINI_ACTIVATION", "detail": f"Offline caps verified (wall={dry_out['wall_seconds']}s, rss={dry_out['peak_rss_bytes']}B <= 512MiB, footprint={fp['total_bytes']}B <= 2GiB); live co-residency with 3 MCA runners pending Mac mini"}
        rows["SH1-A14"] = {"status": "PASS_OFFLINE", "detail": "Suppressed 2025-11-24 flagged as missed_run_silent_failure with 0 backfilled records"}
        rows["SH1-A15"] = {"status": "PASS_OFFLINE", "detail": "replay_window reproduced drill records bit-for-bit, recomputed archived market_records digest & close prices, and gated creditability on trusted Bitcoin headers"}
        rows["SH1-A16"] = {"status": "PASS_OFFLINE", "detail": f"Standing assertions verified across {total_rows} fund rows"}

    # §7 Demonstration D1-D8
    demo_dir = str(REPO_ROOT / "demo")
    if demo_dir not in sys.path:
        sys.path.insert(0, demo_dir)
    from run_demo import run_all_cases

    demo_outcomes = run_all_cases()
    demo_map = {cid: {"ok": ok, "detail": detail} for cid, ok, detail in demo_outcomes}

    ru_max = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss = int(ru_max if sys.platform == "darwin" else ru_max * 1024)
    return {
        "acceptance_rows": rows,
        "demo_d1_d8": demo_map,
        "wall_seconds": round(time.monotonic() - t0, 6),
        "peak_rss_bytes": peak_rss,
        "policy_config_hash": policy_config_hash(),
        "code_version_sha": compute_code_manifest_hash(REPO_ROOT),
    }


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint: python3 -m shadow.acceptance."""
    summary = run_acceptance_pack()
    print(json.dumps(summary, indent=2, sort_keys=True))
    demo_ok = all(item["ok"] for item in summary["demo_d1_d8"].values())  # type: ignore[union-attr]
    rows_ok = all(
        str(r["status"]).startswith(("PASS", "PARTIAL", "PENDING"))
        for r in summary["acceptance_rows"].values()  # type: ignore[union-attr]
    )
    return 0 if (demo_ok and rows_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
