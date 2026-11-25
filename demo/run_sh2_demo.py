"""SH-2 Design Demonstration (D1-D10): vendored FUSE-1 swap, 23/5 isolation, restart state, Cloud Run idempotency."""

from __future__ import annotations

import dataclasses
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from shadow.calendar_consumer import (  # noqa: E402
    classify_us_equity_session_window,
    load_spec,
    resolve_session,
)
from shadow.cloudrun_job import run_cloudrun_job  # noqa: E402
from shadow.evaluator import StubEvaluator, StubProductionForbiddenError  # noqa: E402
from shadow.feeder import select_tiingo_row, select_yahoo_close  # noqa: E402
from shadow.fuse_adapter import (  # noqa: E402
    DEFAULT_VENDOR_ENGINE_ROOT,
    EngineIdentityError,
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    ProductionChainLockedError,
    V2PayloadSchemaError,
    VendoredFuseEvaluator,
    build_fuse_validated_inputs,
    resolve_aggregate_control_state,
    verify_f1_entry_gates,
)
from shadow.ledger import (  # noqa: E402
    GENESIS_PREV_HASH,
    ShadowLedger,
    build_record_body,
    check_forward_clock_credit,
    load_row_state_snapshot,
    save_row_state_snapshot,
)
from shadow.manifest import compute_code_manifest_hash  # noqa: E402
from shadow.ots import build_synthetic_ots_fixture  # noqa: E402
from shadow.replay import replay_window  # noqa: E402
from shadow.scheduler import (  # noqa: E402
    load_production_scheduler_config,
    load_scheduler_config,
    run_daily_drill_cycle,
    run_production_cycle,
)

_NY_TZ = ZoneInfo("America/New_York")


def _sample_inputs(
    *,
    e_val: str = "50000",
    sigma_tqqq: str = "0.24",
    include_sources: bool = True,
    extra: dict | None = None,
) -> dict:
    payload: dict = {
        "E": e_val,
        "E_star": "50000",
        "H": "0",
        "L": "0",
        "funds": {
            "TQQQ": {
                "s": "1",
                "close_price": "50",
                "q_pre": 100,
                "sigma_forecast": sigma_tqqq,
                "long_run_volatility": "0.24",
            },
            "SOXL": {
                "s": "1",
                "close_price": "40",
                "q_pre": 100,
                "sigma_forecast": "0.22",
                "long_run_volatility": "0.24",
            },
        },
    }
    if include_sources:
        payload.update(
            {
                "prior_closes": {
                    "TQQQ": "49",
                    "SOXL": "39.5",
                    "XNDX": "29350",
                    "SEMI": "8150",
                },
                "current_index_closes": {"XNDX": "29500", "SEMI": "8200"},
                "positions_source_record": {"as_of": "2026-01-05T22:30:00.000Z", "age_sessions": 0},
                "broker_reconciliation_source_record": {
                    "as_of": "2026-01-05T22:30:00.000Z",
                    "age_sessions": 0,
                },
            }
        )
    if extra:
        payload.update(extra)
    return payload


def _prior_weekday_str(session_date: str) -> str:
    d = date.fromisoformat(session_date) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.isoformat()


def _build_two_day_payloads(
    session_date: str,
    *,
    tqqq_split: float = 1.0,
    xndx_ret_override: float | None = None,
) -> dict[str, bytes]:
    from shadow.calendar_consumer import resolve_previous_trading_date
    from shadow.feeder import expected_prior_sofr_business_day

    spec = load_spec(REPO_ROOT / "specs" / "csa22-v2")
    prev_ds = resolve_previous_trading_date(spec, "XNYS", session_date) or _prior_weekday_str(session_date)
    sofr_eff = expected_prior_sofr_business_day(session_date, calendar_spec=spec)
    out: dict[str, bytes] = {}
    for sym, prev_px, cur_px in (("TQQQ", 49.0, 50.0), ("SOXL", 39.5, 40.0), ("QQQ", 492.0, 495.0)):
        cur_split = tqqq_split if sym == "TQQQ" else 1.0
        out[sym] = json.dumps(
            [
                {
                    "date": f"{prev_ds}T00:00:00.000Z",
                    "close": prev_px,
                    "divCash": 0.0,
                    "splitFactor": 1.0,
                },
                {
                    "date": f"{session_date}T00:00:00.000Z",
                    "close": cur_px,
                    "divCash": 0.0,
                    "splitFactor": cur_split,
                },
            ]
        ).encode("utf-8")

    ts_prev = int(
        datetime.strptime(f"{prev_ds} 16:00", "%Y-%m-%d %H:%M").replace(tzinfo=_NY_TZ).timestamp()
    )
    ts_cur = int(
        datetime.strptime(f"{session_date} 16:00", "%Y-%m-%d %H:%M").replace(tzinfo=_NY_TZ).timestamp()
    )
    xndx_cur = (
        round(29350.0 * (1.0 + xndx_ret_override), 4)
        if xndx_ret_override is not None
        else 29500.0
    )
    for sym, meta_sym, prev_px, cur_px in (
        ("XNDX", "^XNDX", 29350.0, xndx_cur),
        ("SEMI", "XSOX", 8150.0, 8200.0),
    ):
        out[sym] = json.dumps(
            {
                "chart": {
                    "result": [
                        {
                            "meta": {
                                "symbol": meta_sym,
                                "instrumentType": "INDEX",
                                "dataGranularity": "1d",
                            },
                            "timestamp": [ts_prev, ts_cur],
                            "indicators": {"quote": [{"close": [prev_px, cur_px]}]},
                        }
                    ]
                }
            }
        ).encode("utf-8")

    out["SOFR"] = json.dumps(
        {"refRates": [{"effectiveDate": sofr_eff, "type": "SOFR", "percentRate": 4.35}]}
    ).encode("utf-8")
    out["TBILL_3M"] = (
        f"Date,1 Mo,3 Mo\n{prev_ds},4.38,4.42\n{session_date},4.40,4.45\n".encode("utf-8")
    )
    return out


def _filter_tiingo_payload_by_url(raw_bytes: bytes, url: str) -> bytes:
    parsed = urllib.parse.urlsplit(url)
    qs = urllib.parse.parse_qs(parsed.query)
    start_ds = qs.get("startDate", [None])[0]
    end_ds = qs.get("endDate", [None])[0]
    if not start_ds and not end_ds:
        return raw_bytes
    rows = json.loads(raw_bytes.decode("utf-8"))
    filtered = [
        row
        for row in rows
        if isinstance(row, dict)
        and isinstance(row.get("date"), str)
        and (start_ds is None or row["date"][:10] >= start_ds)
        and (end_ds is None or row["date"][:10] <= end_ds)
    ]
    return json.dumps(filtered).encode("utf-8")


def build_live_mock_transport(
    session_date: str,
    *,
    tqqq_split: float = 1.0,
    xndx_ret_override: float | None = None,
):
    payloads = _build_two_day_payloads(
        session_date,
        tqqq_split=tqqq_split,
        xndx_ret_override=xndx_ret_override,
    )

    def _transport(url: str, headers: dict) -> tuple[int, bytes]:
        _ = headers
        low_url = url.lower()
        if "api.tiingo.com" in low_url:
            if "/tqqq/" in low_url:
                return 200, _filter_tiingo_payload_by_url(payloads["TQQQ"], url)
            if "/soxl/" in low_url:
                return 200, _filter_tiingo_payload_by_url(payloads["SOXL"], url)
            if "/qqq/" in low_url:
                return 200, _filter_tiingo_payload_by_url(payloads["QQQ"], url)
        if "finance.yahoo.com" in low_url:
            if "xndx" in low_url:
                return 200, payloads["XNDX"]
            if "xsox" in low_url:
                return 200, payloads["SEMI"]
        if "markets.newyorkfed.org" in low_url:
            return 200, payloads["SOFR"]
        if "treasury.gov" in low_url:
            return 200, payloads["TBILL_3M"]
        return 404, b"not_found"

    return _transport


def _build_session_fixture_files(
    fixture_dir: Path,
    session_date: str,
    *,
    tqqq_split: float = 1.0,
    xndx_ret_override: float | None = None,
) -> None:
    fixture_dir.mkdir(parents=True, exist_ok=True)
    payloads = _build_two_day_payloads(
        session_date,
        tqqq_split=tqqq_split,
        xndx_ret_override=xndx_ret_override,
    )
    (fixture_dir / f"tiingo_tqqq_{session_date}.json").write_bytes(payloads["TQQQ"])
    (fixture_dir / f"tiingo_soxl_{session_date}.json").write_bytes(payloads["SOXL"])
    (fixture_dir / f"tiingo_qqq_{session_date}.json").write_bytes(payloads["QQQ"])
    (fixture_dir / f"yahoo_xndx_{session_date}.json").write_bytes(payloads["XNDX"])
    (fixture_dir / f"yahoo_xsox_{session_date}.json").write_bytes(payloads["SEMI"])
    (fixture_dir / f"nyfed_sofr_{session_date}.json").write_bytes(payloads["SOFR"])
    (fixture_dir / f"treasury_tbill_{session_date}.csv").write_bytes(payloads["TBILL_3M"])


def _mock_ots_client(body: dict) -> tuple[bytes, dict]:
    from shadow.ledger import sha256_canonical

    rec_sha = sha256_canonical(body)
    ots_bytes, btc_root = build_synthetic_ots_fixture(rec_sha, bitcoin_height=880000)
    assert btc_root is not None
    return ots_bytes, {880000: {"merkle_root": btc_root, "timestamp": "2026-01-05T23:00:00.000Z"}}


def run_all_demos() -> dict[str, dict]:
    results: dict[str, dict] = {}
    spec_v2 = load_spec(REPO_ROOT / "specs" / "csa22-v2")
    code_sha = compute_code_manifest_hash(REPO_ROOT, calendar_version="csa22-v2")

    # D1: Entry gates F1-A01/A03/A04/A10/A11 + zero __pycache__ drift
    gates = verify_f1_entry_gates(require_read_only=False)
    assert gates["ok"] and gates["f1_a01_id_set_equal"]
    results["D1"] = {"status": "PASS", **gates}

    # D2: Before/After swap, 1-byte tamper rejection, and hwm_adjusted caller gate
    ev = VendoredFuseEvaluator()
    cal = resolve_session("XNYS", "2026-01-05", spec=spec_v2)
    before_stub_failed = False
    try:
        build_record_body(
            chain_id="prod-neg",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="production",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:30:00.000Z",
            session_inputs=_sample_inputs(),
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=StubEvaluator(),
            calendar_spec=spec_v2,
        )
    except StubProductionForbiddenError:
        before_stub_failed = True
    assert before_stub_failed

    tamper_failed = False
    with tempfile.TemporaryDirectory() as tmpdir:
        tv = Path(tmpdir) / "vendor_copy"
        shutil.copytree(DEFAULT_VENDOR_ENGINE_ROOT, tv)
        target_py = tv / "fuse_engine" / "canonical.py"
        os.chmod(target_py, 0o644)
        target_py.write_bytes(target_py.read_bytes() + b"\n# tamper\n")
        try:
            VendoredFuseEvaluator(vendor_root=tv)
        except EngineIdentityError:
            tamper_failed = True
    assert tamper_failed

    hwm_schema_rejected = False
    bad_v2 = _sample_inputs()
    bad_v2["funds"]["TQQQ"]["corporate_action"] = {
        "type": "split",
        "ratio": 2,
        "adjustment_complete": True,
        "exact_reconciliation": True,
    }
    try:
        build_record_body(
            chain_id="prod-hwm",
            sequence=0,
            prev_record_sha256=GENESIS_PREV_HASH,
            mode="production",
            session_date="2026-01-05",
            decision_timestamp_utc="2026-01-05T22:30:00.000Z",
            session_inputs=bad_v2,
            calendar_session=cal,
            code_version_sha=code_sha,
            evaluator=ev,
            runtime_config=json.loads(
                (REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
            ),
            calendar_spec=spec_v2,
        )
    except V2PayloadSchemaError:
        hwm_schema_rejected = True
    assert hwm_schema_rejected

    built_prod = build_record_body(
        chain_id="prod-sh2-demo",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="production",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=_sample_inputs(),
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        runtime_config=json.loads(
            (REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")
        ),
        calendar_spec=spec_v2,
    )
    assert built_prod["record_body"]["observation_class"] == "decision_time_market"
    assert built_prod["record_body"]["assertions"]["orders_placed"] == 0
    results["D2"] = {
        "status": "PASS",
        "stub_before_swap_blocked": before_stub_failed,
        "tamper_blocked": tamper_failed,
        "hwm_adjusted_schema_blocked": hwm_schema_rejected,
        "engine_hash": built_prod["record_body"]["engine_hash"],
    }

    # D3: Multi-session FUSE-1 sizing + real breaker transitions (NO-NEW-RISK, DE-RISK, HALT)
    v4_inputs = _sample_inputs(sigma_tqqq="0.50")
    v4_built = build_record_body(
        chain_id="d3-chain",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="drill",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=v4_inputs,
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        calendar_spec=spec_v2,
    )
    agg_v4 = resolve_aggregate_control_state(
        v4_built["record_body"]["evaluator"]["effective_state_by_scope"]
    )
    assert agg_v4 == "NO-NEW-RISK"
    assert all(t["orders_placed"] == 0 for t in v4_built["record_body"]["tiers"])

    v3_inputs = _sample_inputs()
    v3_inputs["funds"]["TQQQ"]["fund_event"] = "suspended_creations"
    v3_built = build_record_body(
        chain_id="d3-v3-chain",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="drill",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=v3_inputs,
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        calendar_spec=spec_v2,
    )
    agg_v3 = resolve_aggregate_control_state(
        v3_built["record_body"]["evaluator"]["effective_state_by_scope"]
    )
    assert agg_v3 == "DE-RISK"

    l3_inputs = _sample_inputs(e_val="34000")
    l3_built = build_record_body(
        chain_id="d3-l3-chain",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="drill",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=l3_inputs,
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        calendar_spec=spec_v2,
    )
    agg_l3 = resolve_aggregate_control_state(
        l3_built["record_body"]["evaluator"]["effective_state_by_scope"]
    )
    assert agg_l3 == "DE-RISK"

    l6_inputs = _sample_inputs(
        extra={"index_one_session_returns": {"NDX": -0.11, "SPX": -0.05}}
    )
    l6_built = build_record_body(
        chain_id="d3-l6-chain",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="drill",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=l6_inputs,
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        calendar_spec=spec_v2,
    )
    agg_l6 = resolve_aggregate_control_state(
        l6_built["record_body"]["evaluator"]["effective_state_by_scope"]
    )
    assert agg_l6 == "HALT"
    results["D3"] = {
        "status": "PASS",
        "v4_effective_state": agg_v4,
        "v3_effective_state": agg_v3,
        "l3_effective_state": agg_l3,
        "l6_effective_state": agg_l6,
    }

    # D4: Subprocess restart continuity via state_snapshots/
    with tempfile.TemporaryDirectory() as tmpdir:
        snap_dir = Path(tmpdir) / "state_snapshots"
        sub_env = {**os.environ, "PYTHONPATH": str(SRC_DIR)}
        code_p1 = (
            "from shadow.fuse_adapter import VendoredFuseEvaluator, build_fuse_validated_inputs\n"
            "from shadow.calendar_consumer import load_spec, resolve_session\n"
            "from shadow.ledger import save_row_state_snapshot\n"
            f"spec = load_spec({str(REPO_ROOT / 'specs' / 'csa22-v2')!r})\n"
            "cal = resolve_session('XNYS', '2026-01-05', spec=spec)\n"
            "ev = VendoredFuseEvaluator()\n"
            "sin = {'E': '50000', 'E_star': '50000', 'H': '0', 'L': '0', "
            "'prior_closes': {'TQQQ': '49', 'SOXL': '39.5', 'XNDX': '29350', 'SEMI': '8150'}, "
            "'current_index_closes': {'XNDX': '29500', 'SEMI': '8200'}, "
            "'positions_source_record': {'as_of': '2026-01-05T22:30:00.000Z', 'age_sessions': 0}, "
            "'broker_reconciliation_source_record': {'as_of': '2026-01-05T22:30:00.000Z', 'age_sessions': 0}, "
            "'funds': {'TQQQ': {'s': '1', 'close_price': '50', 'q_pre': 100, 'sigma_forecast': '0.50', 'long_run_volatility': '0.24'}, "
            "'SOXL': {'s': '1', 'close_price': '40', 'q_pre': 100, 'sigma_forecast': '0.22', 'long_run_volatility': '0.24'}}}\n"
            "vin = build_fuse_validated_inputs(sin, cal, policy_hash='p', session_date='2026-01-05', as_of='2026-01-05T22:30:00.000Z')\n"
            "_, _, nxt = ev.evaluate(ev.spec_version, ev.engine_hash, 'p', '2026-01-05', '2026-01-05T22:30:00.000Z', 'shadow', vin, {}, None)\n"
            f"save_row_state_snapshot({str(snap_dir)!r}, chain_id='d4', sequence=0, session_date='2026-01-05', record_sha256='0'*64, engine_hash=ev.engine_hash, spec_version=ev.spec_version, next_row_state=nxt)\n"
        )
        subprocess.run([sys.executable, "-B", "-c", code_p1], env=sub_env, check=True)
        code_p2 = (
            "from shadow.fuse_adapter import VendoredFuseEvaluator, build_fuse_validated_inputs\n"
            "from shadow.calendar_consumer import load_spec, resolve_session\n"
            "from shadow.ledger import load_row_state_snapshot\n"
            f"spec = load_spec({str(REPO_ROOT / 'specs' / 'csa22-v2')!r})\n"
            "cal = resolve_session('XNYS', '2026-01-06', spec=spec)\n"
            "ev = VendoredFuseEvaluator()\n"
            f"prior, _ = load_row_state_snapshot({str(snap_dir)!r}, expected_chain_id='d4', expected_engine_hash=ev.engine_hash, expected_spec_version=ev.spec_version, expected_sequence=0)\n"
            "sin = {'E': '50000', 'E_star': '50000', 'H': '0', 'L': '0', "
            "'prior_closes': {'TQQQ': '50', 'SOXL': '40', 'XNDX': '29500', 'SEMI': '8200'}, "
            "'current_index_closes': {'XNDX': '29600', 'SEMI': '8240'}, "
            "'positions_source_record': {'as_of': '2026-01-06T22:30:00.000Z', 'age_sessions': 0}, "
            "'broker_reconciliation_source_record': {'as_of': '2026-01-06T22:30:00.000Z', 'age_sessions': 0}, "
            "'funds': {'TQQQ': {'s': '1', 'close_price': '50', 'q_pre': 100, 'sigma_forecast': '0.24', 'long_run_volatility': '0.24'}, "
            "'SOXL': {'s': '1', 'close_price': '40', 'q_pre': 100, 'sigma_forecast': '0.22', 'long_run_volatility': '0.24'}}}\n"
            "vin = build_fuse_validated_inputs(sin, cal, policy_hash='p', session_date='2026-01-06', as_of='2026-01-06T22:30:00.000Z')\n"
            "rows, _, nxt = ev.evaluate(ev.spec_version, ev.engine_hash, 'p', '2026-01-06', '2026-01-06T22:30:00.000Z', 'shadow', vin, prior, None)\n"
            "assert rows['V4']['state'] == 'NO-NEW-RISK' and rows['V4']['would_have_tripped'] is True and nxt['V4']['TQQQ']['clearing_counter'] == 1\n"
        )
        subprocess.run([sys.executable, "-B", "-c", code_p2], env=sub_env, check=True)
        _, _ = load_row_state_snapshot(
            snap_dir,
            expected_chain_id="d4",
            expected_engine_hash=ev.engine_hash,
            expected_spec_version=ev.spec_version,
            expected_sequence=0,
        )
    results["D4"] = {"status": "PASS", "restart_v4_clearing_preserved": True}

    # D5: Locked production config blocks OA-SH2-2 writes cleanly with 0 appends
    with tempfile.TemporaryDirectory() as tmpdir:
        locked_rejected = False
        try:
            run_production_cycle(session_date="2026-01-05", data_dir=Path(tmpdir) / "locked")
        except ProductionChainLockedError:
            locked_rejected = True
        assert locked_rejected
        assert not (Path(tmpdir) / "locked" / "ledger").exists()
    results["D5"] = {"status": "PASS", "production_chain_locked": True, "zero_appends": True}

    # D6: Zero conformance shims (U-01 unresolved + missing source/prior_closes O1 fail-closed)
    v_unresolved = build_fuse_validated_inputs(
        _sample_inputs(), cal, policy_hash="p", session_date="2026-01-05", as_of="2026-01-05T22:30:00.000Z"
    )
    rows_u, _, _ = ev.evaluate(
        ev.spec_version, ev.engine_hash, "p", "2026-01-05", "2026-01-05T22:30:00.000Z", "shadow", v_unresolved, {}, None
    )
    assert all(rows_u[rid]["reason"] == "unresolved_fail_closed" and rows_u[rid]["would_have_tripped"] for rid in ("V5", "V7", "O5"))
    missing_sources_res = build_record_body(
        chain_id="d6-no-fab",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="production",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs=_sample_inputs(include_sources=False),
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        runtime_config=json.loads((REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")),
        calendar_spec=spec_v2,
    )
    assert missing_sources_res["fail_closed"] is True and missing_sources_res["o1_output"]["state"] == "NO-NEW-RISK"
    missing_res = build_record_body(
        chain_id="d6-missing",
        sequence=0,
        prev_record_sha256=GENESIS_PREV_HASH,
        mode="production",
        session_date="2026-01-05",
        decision_timestamp_utc="2026-01-05T22:30:00.000Z",
        session_inputs={**_sample_inputs(), "market_records": []},
        calendar_session=cal,
        code_version_sha=code_sha,
        evaluator=ev,
        runtime_config=json.loads((REPO_ROOT / "config" / "production-scheduler.json").read_text(encoding="utf-8")),
        calendar_spec=spec_v2,
    )
    assert missing_res["fail_closed"] is True and missing_res["appended"] is False
    results["D6"] = {"status": "PASS", "unresolved_rows_fail_closed": ["O5", "V5", "V7"], "no_fabricated_sources": True}

    # D7: Bit-for-bit deterministic replay on FUSE-1 vendored chain + csa22-v2
    with tempfile.TemporaryDirectory() as tmpdir:
        ddir = Path(tmpdir) / "drill_d7"
        fdir = Path(tmpdir) / "fixtures"
        _build_session_fixture_files(fdir, "2026-01-05")
        base_cfg = dataclasses.replace(
            load_scheduler_config(REPO_ROOT / "config" / "drill-scheduler.json"),
            calendar_version="csa22-v2",
            calendar_spec_digest=PINNED_CSA22_V2_DIGEST,
            fuse_spec_version=PINNED_FUSE_SPEC_VERSION,
            fuse_engine_hash=PINNED_FUSE_ENGINE_HASH,
        )
        res_d7 = run_daily_drill_cycle(
            session_date="2026-01-05",
            data_dir=ddir,
            config=base_cfg,
            fixtures_dir=fdir,
            wall_clock=lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc),
            evaluator=ev,
        )
        assert res_d7["appended"] is True
        rep_d7 = replay_window(ddir / "replay", ddir / "ledger")
        assert rep_d7["bit_for_bit_match"] and rep_d7["chain_ok"]
    results["D7"] = {"status": "PASS", "bit_for_bit_match": True, "replayed_count": 1}

    # D8: csa22-v2 calendar + US 23/5 16:00 ET Official Close & post-21:00 ET isolation (Item 13)
    cal_jul3 = resolve_session("XNYS", "2026-07-03", spec=spec_v2)
    cal_nov27 = resolve_session("XNYS", "2026-11-27", spec=spec_v2)
    assert not cal_jul3["is_trading_day"] and cal_nov27["close_local"] == "13:00"
    assert classify_us_equity_session_window("2026-01-05T21:00:00.000Z", cal) == "core_session_open"
    assert classify_us_equity_session_window("2026-01-06T01:30:00.000Z", cal) == "extended_overnight_open"
    # Overnight print rejected cleanly while 16:00 ET official print survives
    ts_1600 = int(datetime(2026, 1, 5, 16, 0, tzinfo=_NY_TZ).timestamp())
    ts_2115 = int(datetime(2026, 1, 5, 21, 15, tzinfo=_NY_TZ).timestamp())
    bad_y = json.dumps({"chart": {"result": [{"timestamp": [ts_2115], "indicators": {"quote": [{"close": [25000.0]}]}}]}}).encode()
    rejected_235 = False
    try:
        select_yahoo_close(bad_y, "2026-01-05", calendar_session=cal)
    except ValueError as exc:
        rejected_235 = str(exc) in ("extended_hours_overnight_rejected", "next_trade_date_contamination")
    assert rejected_235
    mixed_y = json.dumps({"chart": {"result": [{"timestamp": [ts_1600, ts_2115], "indicators": {"quote": [{"close": [29500.0, 25000.0]}]}}]}}).encode()
    assert str(select_yahoo_close(mixed_y, "2026-01-05", calendar_session=cal)) == "29500.0"
    results["D8"] = {"status": "PASS", "csa22_v2_digest": spec_v2["spec_digest"], "overnight_isolated": True}

    # D9: Cloud Run Jobs (us-east1) stateless sequential & concurrent idempotency (Item 18)
    with tempfile.TemporaryDirectory() as tmpdir:
        cdir = Path(tmpdir) / "cloudrun_data"
        fdir = Path(tmpdir) / "fixtures"
        _build_session_fixture_files(fdir, "2026-01-05")
        wc = lambda: datetime(2026, 1, 5, 22, 30, tzinfo=timezone.utc)
        first = run_cloudrun_job(
            data_dir=cdir,
            config_path=REPO_ROOT / "config" / "drill-scheduler.json",
            session_date="2026-01-05",
            fixtures_dir=fdir,
            wall_clock=wc,
        )
        assert first["status"] == "committed" and first["appended"] is True
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(
                run_cloudrun_job,
                data_dir=cdir,
                config_path=REPO_ROOT / "config" / "drill-scheduler.json",
                session_date="2026-01-05",
                fixtures_dir=fdir,
                wall_clock=wc,
            )
            f2 = pool.submit(
                run_cloudrun_job,
                data_dir=cdir,
                config_path=REPO_ROOT / "config" / "drill-scheduler.json",
                session_date="2026-01-05",
                fixtures_dir=fdir,
                wall_clock=wc,
            )
            r_c1, r_c2 = f1.result(), f2.result()
        assert r_c1["status"] == "already_committed_no_op" and not r_c1["appended"]
        assert r_c2["status"] == "already_committed_no_op" and not r_c2["appended"]
        assert len(ShadowLedger(cdir / "ledger").load_records()) == 1
    results["D9"] = {"status": "PASS", "idempotent_retry": "already_committed_no_op", "ledger_count": 1}

    # D10: Creditability verification (locked config rejected; unlocked + BTC proof verified)
    with tempfile.TemporaryDirectory() as tmpdir:
        ldir = Path(tmpdir) / "prod_credit"
        ledger = ShadowLedger(ldir)
        ots_b, hdrs = _mock_ots_client(built_prod["record_body"])
        dl_utc = "2026-01-06T04:30:00.000Z"
        ledger.append_record(
            built_prod["record_body"],
            obtained_at_utc="2026-01-05T22:35:00.000Z",
            ots_bytes=ots_b,
            commitment_deadline_utc=dl_utc,
            bitcoin_merkle_roots=hdrs,
        )
        recs = ledger.load_records()
        rcps = ledger.load_receipts()
        ok_default_locked, reasons_default_locked = check_forward_clock_credit(
            recs,
            rcps,
            ledger_dir=ldir,
            bitcoin_merkle_roots=hdrs,
            commitment_deadline_et="23:30",
            calendar_spec=spec_v2,
        )
        assert not ok_default_locked and "production_chain_locked" in reasons_default_locked
        ok_locked, reasons_locked = check_forward_clock_credit(
            recs,
            rcps,
            ledger_dir=ldir,
            bitcoin_merkle_roots=hdrs,
            commitment_deadline_et="23:30",
            calendar_spec=spec_v2,
            expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
            expected_spec_version=PINNED_FUSE_SPEC_VERSION,
            production_chain_locked=True,
        )
        assert not ok_locked and "production_chain_locked" in reasons_locked
        ok_unlocked, reasons_unlocked = check_forward_clock_credit(
            recs,
            rcps,
            ledger_dir=ldir,
            bitcoin_merkle_roots=hdrs,
            commitment_deadline_et="23:30",
            calendar_spec=spec_v2,
            expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
            expected_spec_version=PINNED_FUSE_SPEC_VERSION,
            production_chain_locked=False,
        )
        assert ok_unlocked and reasons_unlocked == []
    results["D10"] = {"status": "PASS", "locked_rejected": True, "default_locked_rejected": True, "unlocked_creditable": True}

    return {"demo_suite": "SH2-D1-D10", "passed": len(results), "total": 10, "cases": results}


if __name__ == "__main__":
    print(json.dumps(run_all_demos(), indent=2, sort_keys=True))
