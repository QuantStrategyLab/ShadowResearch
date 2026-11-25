"""AI-independent drill-chain daily cycle recorder, missed-run detector, and dry-run harness."""

from __future__ import annotations

import argparse
import copy
import fcntl
import hmac
import json
import os
import re
import resource
import sys
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from shadow.calendar_consumer import (
    load_spec,
    resolve_previous_trading_date,
    resolve_session,
)
from shadow.evaluator import REQUIRED_MARKET_SERIES, StubEvaluator, StubProductionForbiddenError
from shadow.feeder import (
    SOXL_PROXY_LABEL,
    emit_market_records_from_fixtures,
    extract_prior_close_from_tiingo_bytes,
    extract_prior_close_from_yahoo_bytes,
    read_verified_archived_bytes,
    verify_market_record_raw_integrity,
)
from shadow.feeder_live import (
    DEFAULT_TREASURY_URL_TEMPLATE,
    DEFAULT_YAHOO_SYMBOL_MAP,
    LIVE_SERIES,
    Transport,
    emit_live_market_records,
)
from shadow.fuse_adapter import (
    EngineIdentityError,
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    ProductionChainLockedError,
    VendoredFuseEvaluator,
    build_fuse_validated_inputs,
    is_stub_evaluator,
    resolve_aggregate_control_state,
)
from shadow.ledger import (
    GENESIS_PREV_HASH,
    NO_PENDING_REJECTED_STATE_SHA256,
    ShadowLedger,
    _DATE_YYYY_MM_DD_RE,
    _HEX64_RE,
    _atomic_write_bytes,
    _sha256_canonical_state_dict,
    authenticate_pending_rejected_state_snapshot,
    build_pending_rejected_state_payload,
    build_record_body,
    canonical_json_bytes,
    compute_code_manifest_hash,
    compute_inputs_digest,
    compute_pending_rejected_state_sha256,
    compute_session_commitment_deadline_utc,
    evaluate_non_stub_tiers,
    has_committed_detached_evidence,
    is_receipt_timely,
    is_utc_ms_timestamp,
    load_pending_rejected_state_snapshot,
    load_row_state_snapshot,
    normalize_pending_authorized_events,
    o1_blocks_session_commit,
    parse_json_no_duplicates,
    save_pending_rejected_state_snapshot,
    save_row_state_snapshot,
    sha256_canonical,
    verify_receipt,
)
from shadow.manifest import _normalize_config_value
from shadow.ots import DEFAULT_OTS_CALENDARS, OpenTimestampsClient, OtsTransport, build_synthetic_ots_fixture
from shadow.replay import replay_window
from shadow.sizing import INSTRUMENT_ORDER, canonical_decimal_str, policy_config_hash

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEDULER_CONFIG_PATH = REPO_ROOT / "config" / "drill-scheduler.json"
DEFAULT_PRODUCTION_CONFIG_PATH = REPO_ROOT / "config" / "production-scheduler.json"
DEFAULT_DEPLOY_DIR = REPO_ROOT / "deploy" / "scheduler"
_NY_TZ = ZoneInfo("America/New_York")
_STRICT_HHMM_RE = re.compile(r"^[0-9]{2}:[0-9]{2}$", re.ASCII)
_ALLOWED_SESSION_INPUT_KEYS = frozenset(
    {"E", "E_star", "H", "L", "funds", "prior_E", "external_flow", "monthly_returns"}
)
_ALLOWED_FUND_INPUT_KEYS = frozenset({"s", "close_price", "q_pre", "sigma_forecast"})
_STANDING_ASSERTION_KEYS = (
    "kelly_applied",
    "orders_placed",
    "B0",
    "H",
    "stop_loss_return_rule",
    "loss_controls_armed",
)


def _parse_strict_hhmm(raw_val: object, *, field_name: str) -> tuple[int, int, str]:
    if (
        isinstance(raw_val, bool)
        or not isinstance(raw_val, str)
        or not raw_val.isascii()
        or not _STRICT_HHMM_RE.fullmatch(raw_val)
    ):
        raise ValueError(f"missing_or_invalid_{field_name}:{raw_val!r}")
    hour = int(raw_val[:2])
    minute = int(raw_val[3:])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(f"missing_or_invalid_{field_name}:{raw_val!r}")
    return hour, minute, raw_val


class DrillOnlyViolationError(RuntimeError):
    """Raised if scheduler configuration or call attempts production / CLK-01 execution."""


class CalendarCoverageError(ValueError):
    """Raised when requested session/window exceeds configured calendar spec coverage."""


@dataclass(frozen=True)
class DrillSchedulerConfig:
    """Pinned configuration for the SH-1 drill-chain scheduler and missed-run detector."""

    chain_id: str = "drill-sh1-v1"
    mode: str = "drill"
    clk01_active: bool = False
    venue: str = "XNYS"
    calendar_version: str = "csa22-v1"
    decision_instant_et: str = "17:30"
    half_day_run_time_et: str = "14:30"
    decision_cutoff_et: str = "18:00"
    decision_cutoff_half_day_et: str = "15:00"
    commitment_deadline_et: str = "23:30"
    treasury_endpoint: str = DEFAULT_TREASURY_URL_TEMPLATE
    yahoo_symbol_map: dict[str, str] | None = None
    ots_calendars: tuple[str, ...] = DEFAULT_OTS_CALENDARS
    ots_timeout_seconds: float = 10.0
    max_footprint_bytes: int = 2 * 1024 * 1024 * 1024
    log_retention_days: int = 30
    log_max_bytes: int = 50 * 1024 * 1024
    resource_caps: dict[str, object] | None = None
    schema_version: str = "sh1-drill-scheduler-v1"
    production_mode_forbidden: bool = True
    production_chain_locked: bool | None = None
    deployment_target: str | None = None
    cloudrun_region: str | None = None
    calendar_spec_digest: str | None = None
    fuse_spec_version: str | None = None
    fuse_engine_hash: str | None = None

    def __post_init__(self) -> None:
        for fname in (
            "decision_instant_et",
            "half_day_run_time_et",
            "decision_cutoff_et",
            "decision_cutoff_half_day_et",
            "commitment_deadline_et",
        ):
            _parse_strict_hhmm(getattr(self, fname), field_name=fname)


def load_scheduler_config(config_path: str | Path | None = None) -> DrillSchedulerConfig:
    """Load and validate drill-only scheduler configuration."""
    path = Path(config_path) if config_path is not None else DEFAULT_SCHEDULER_CONFIG_PATH
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Scheduler configuration JSON must be an object")
    if raw.get("mode") != "drill" or raw.get("clk01_active") is not False:
        raise DrillOnlyViolationError(
            "SH-1 scheduler configuration MUST enforce mode='drill' and clk01_active=false"
        )
    for req_str in ("chain_id", "venue", "calendar_version"):
        val = raw.get(req_str)
        if not isinstance(val, str) or not val:
            raise ValueError(f"missing_required_scheduler_config:{req_str}")
    validated_times: dict[str, str] = {}
    for time_key in (
        "decision_instant_et",
        "half_day_run_time_et",
        "decision_cutoff_et",
        "decision_cutoff_half_day_et",
        "commitment_deadline_et",
    ):
        if time_key not in raw:
            raise ValueError(f"missing_required_scheduler_config:{time_key}")
        _, _, validated_times[time_key] = _parse_strict_hhmm(raw[time_key], field_name=time_key)
    return DrillSchedulerConfig(
        chain_id=str(raw["chain_id"]),
        mode="drill",
        clk01_active=False,
        venue=str(raw["venue"]),
        calendar_version=str(raw["calendar_version"]),
        decision_instant_et=validated_times["decision_instant_et"],
        half_day_run_time_et=validated_times["half_day_run_time_et"],
        decision_cutoff_et=validated_times["decision_cutoff_et"],
        decision_cutoff_half_day_et=validated_times["decision_cutoff_half_day_et"],
        commitment_deadline_et=validated_times["commitment_deadline_et"],
        treasury_endpoint=str(raw.get("treasury_endpoint", DEFAULT_TREASURY_URL_TEMPLATE)),
        yahoo_symbol_map=dict(raw.get("yahoo_symbol_map", DEFAULT_YAHOO_SYMBOL_MAP)),
        ots_calendars=tuple(raw.get("ots_calendars", DEFAULT_OTS_CALENDARS)),
        ots_timeout_seconds=float(raw.get("ots_timeout_seconds", 10.0)),
        max_footprint_bytes=int(raw.get("max_footprint_bytes", 2 * 1024 * 1024 * 1024)),
        log_retention_days=int(raw.get("log_retention_days", 30)),
        log_max_bytes=int(raw.get("log_max_bytes", 50 * 1024 * 1024)),
        resource_caps=dict(raw.get("resource_caps", {})),
        schema_version=str(raw.get("schema_version", "sh1-drill-scheduler-v1")),
        production_mode_forbidden=bool(raw.get("production_mode_forbidden", True)),
        production_chain_locked=raw.get("production_chain_locked"),
        deployment_target=raw.get("deployment_target"),
        cloudrun_region=raw.get("cloudrun_region"),
        calendar_spec_digest=raw.get("calendar_spec_digest"),
        fuse_spec_version=raw.get("fuse_spec_version"),
        fuse_engine_hash=raw.get("fuse_engine_hash"),
    )


def _require_strict_bool(
    raw: Mapping[str, object],
    key: str,
    *,
    default: bool | None = None,
) -> bool:
    if key not in raw:
        if default is None:
            raise ValueError(f"missing_required_boolean:{key}")
        return default
    val = raw[key]
    if not isinstance(val, bool):
        raise ValueError(f"invalid_boolean_for_{key}:{val!r}")
    return val


def load_production_scheduler_config(config_path: str | Path | None = None) -> DrillSchedulerConfig:
    """Load and validate production scheduler config with strict boolean and OA-SH2-2 lock checks."""
    path = Path(config_path) if config_path is not None else DEFAULT_PRODUCTION_CONFIG_PATH
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Production scheduler configuration JSON must be an object")
    if raw.get("mode") != "production":
        raise DrillOnlyViolationError("Production scheduler config MUST specify mode='production'")
    clk01_active = _require_strict_bool(raw, "clk01_active")
    production_chain_locked = _require_strict_bool(raw, "production_chain_locked")
    production_mode_forbidden = _require_strict_bool(raw, "production_mode_forbidden", default=False)
    if production_mode_forbidden is True:
        raise DrillOnlyViolationError("production_mode_forbidden=true cannot run production mode")
    if raw.get("fuse_spec_version") != PINNED_FUSE_SPEC_VERSION:
        raise ValueError(
            f"Production config must pin fuse_spec_version={PINNED_FUSE_SPEC_VERSION!r}, got {raw.get('fuse_spec_version')!r}"
        )
    if raw.get("fuse_engine_hash") != PINNED_FUSE_ENGINE_HASH:
        raise ValueError(
            f"Production config must pin fuse_engine_hash={PINNED_FUSE_ENGINE_HASH!r}, got {raw.get('fuse_engine_hash')!r}"
        )
    if raw.get("calendar_version") != "csa22-v2" or raw.get("calendar_spec_digest") != PINNED_CSA22_V2_DIGEST:
        raise CalendarCoverageError(
            "Production config must pin calendar_version='csa22-v2' and csa22-v2 spec_digest"
        )
    validated_times: dict[str, str] = {}
    for time_key in (
        "decision_instant_et",
        "half_day_run_time_et",
        "decision_cutoff_et",
        "decision_cutoff_half_day_et",
        "commitment_deadline_et",
    ):
        if time_key not in raw:
            raise ValueError(f"missing_required_scheduler_config:{time_key}")
        _, _, validated_times[time_key] = _parse_strict_hhmm(raw[time_key], field_name=time_key)
    return DrillSchedulerConfig(
        chain_id=str(raw["chain_id"]),
        mode="production",
        clk01_active=clk01_active,
        venue=str(raw["venue"]),
        calendar_version=str(raw["calendar_version"]),
        decision_instant_et=validated_times["decision_instant_et"],
        half_day_run_time_et=validated_times["half_day_run_time_et"],
        decision_cutoff_et=validated_times["decision_cutoff_et"],
        decision_cutoff_half_day_et=validated_times["decision_cutoff_half_day_et"],
        commitment_deadline_et=validated_times["commitment_deadline_et"],
        treasury_endpoint=str(raw.get("treasury_endpoint", DEFAULT_TREASURY_URL_TEMPLATE)),
        yahoo_symbol_map=dict(raw.get("yahoo_symbol_map", DEFAULT_YAHOO_SYMBOL_MAP)),
        ots_calendars=tuple(raw.get("ots_calendars", DEFAULT_OTS_CALENDARS)),
        ots_timeout_seconds=float(raw.get("ots_timeout_seconds", 10.0)),
        max_footprint_bytes=int(raw.get("max_footprint_bytes", 2 * 1024 * 1024 * 1024)),
        log_retention_days=int(raw.get("log_retention_days", 30)),
        log_max_bytes=int(raw.get("log_max_bytes", 50 * 1024 * 1024)),
        resource_caps=dict(raw.get("resource_caps", {})),
        schema_version=str(raw.get("schema_version", "sh2-production-scheduler-v1")),
        production_mode_forbidden=production_mode_forbidden,
        production_chain_locked=production_chain_locked,
        deployment_target=str(raw.get("deployment_target", "cloudrun-job")),
        cloudrun_region=str(raw.get("cloudrun_region", "us-east1")),
        calendar_spec_digest=str(raw["calendar_spec_digest"]),
        fuse_spec_version=str(raw["fuse_spec_version"]),
        fuse_engine_hash=str(raw["fuse_engine_hash"]),
    )


def _effective_runtime_config(
    cfg: DrillSchedulerConfig,
    *,
    treasury_endpoint: str | None = None,
    yahoo_symbol_map: Mapping[str, str] | None = None,
) -> dict[str, object]:
    eff_treasury = treasury_endpoint if treasury_endpoint is not None else cfg.treasury_endpoint
    eff_yahoo = (
        dict(yahoo_symbol_map)
        if yahoo_symbol_map is not None
        else dict(cfg.yahoo_symbol_map or DEFAULT_YAHOO_SYMBOL_MAP)
    )
    raw: dict[str, object] = {
        "schema_version": cfg.schema_version,
        "chain_id": cfg.chain_id,
        "mode": cfg.mode,
        "clk01_active": cfg.clk01_active,
        "production_mode_forbidden": cfg.production_mode_forbidden,
        "venue": cfg.venue,
        "calendar_version": cfg.calendar_version,
        "decision_instant_et": cfg.decision_instant_et,
        "half_day_run_time_et": cfg.half_day_run_time_et,
        "decision_cutoff_et": cfg.decision_cutoff_et,
        "decision_cutoff_half_day_et": cfg.decision_cutoff_half_day_et,
        "commitment_deadline_et": cfg.commitment_deadline_et,
        "treasury_endpoint": eff_treasury,
        "yahoo_symbol_map": eff_yahoo,
        "ots_calendars": list(cfg.ots_calendars),
        "ots_timeout_seconds": cfg.ots_timeout_seconds,
        "max_footprint_bytes": cfg.max_footprint_bytes,
        "log_retention_days": cfg.log_retention_days,
        "log_max_bytes": cfg.log_max_bytes,
        "resource_caps": dict(cfg.resource_caps or {}),
    }
    for opt_key in (
        "production_chain_locked",
        "deployment_target",
        "cloudrun_region",
        "calendar_spec_digest",
        "fuse_spec_version",
        "fuse_engine_hash",
    ):
        opt_val = getattr(cfg, opt_key)
        if opt_val is not None:
            raw[opt_key] = opt_val
    return _normalize_config_value(raw)  # type: ignore[return-value]


def _resolve_spec_dir_for_config(
    cfg: DrillSchedulerConfig,
    spec_dir: str | Path | None = None,
) -> Path:
    if spec_dir is not None:
        cand = Path(spec_dir)
        if not cand.is_dir():
            raise CalendarCoverageError(f"calendar_coverage_insufficient: missing_spec_dir:{cand}")
        return cand
    cand = REPO_ROOT / "specs" / cfg.calendar_version
    if not cand.is_dir():
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: missing_calendar_version:{cfg.calendar_version}"
        )
    return cand


def _format_utc_ms(dt: datetime) -> str:
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


def ny_local_time_matches(
    now_utc: datetime,
    target_hhmm: str,
    *,
    tolerance_minutes: int = 10,
) -> bool:
    """Return True iff now_utc falls within tolerance_minutes of any comma-separated HH:MM America/New_York."""
    if not isinstance(target_hhmm, str) or not target_hhmm.strip():
        _parse_strict_hhmm(target_hhmm, field_name="require_ny_hhmm")
    parts = [part.strip() for part in target_hhmm.split(",")]
    validated_targets = [
        _parse_strict_hhmm(part, field_name="require_ny_hhmm")[:2]
        for part in parts
    ]
    now_ny = now_utc.astimezone(_NY_TZ)
    tol_sec = tolerance_minutes * 60
    for hour, minute in validated_targets:
        for day_offset in (-1, 0, 1):
            cand_date = now_ny.date() + timedelta(days=day_offset)
            if cand_date.weekday() >= 5:
                continue
            cand_dt = datetime.combine(cand_date, dt_time(hour, minute), tzinfo=_NY_TZ)
            if abs((now_ny - cand_dt).total_seconds()) <= tol_sec:
                return True
    return False


def _et_hhmm_to_utc_ms(session_date: str, hhmm: str) -> str:
    d = date.fromisoformat(session_date)
    hour, minute, _ = _parse_strict_hhmm(hhmm, field_name="et_hhmm")
    local_dt = datetime.combine(d, dt_time(hour, minute), tzinfo=_NY_TZ)
    return local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def compute_session_deadlines(
    session_date: str,
    cfg: DrillSchedulerConfig,
    *,
    calendar_session: Mapping[str, object] | None = None,
) -> dict[str, str]:
    """Compute three distinct session timestamps plus scheduled decision instant in UTC ms."""
    is_half_day = bool(calendar_session and calendar_session.get("session_state") == "half_day")
    cal_close = calendar_session.get("close_utc") if calendar_session else None
    session_close_utc = (
        str(cal_close)
        if isinstance(cal_close, str) and cal_close
        else _et_hhmm_to_utc_ms(session_date, "13:00" if is_half_day else "16:00")
    )
    decision_et = cfg.half_day_run_time_et if is_half_day else cfg.decision_instant_et
    decision_utc = _et_hhmm_to_utc_ms(session_date, decision_et)
    cutoff_et = cfg.decision_cutoff_half_day_et if is_half_day else cfg.decision_cutoff_et
    decision_cutoff_utc = _et_hhmm_to_utc_ms(session_date, cutoff_et)
    deadline_utc = _et_hhmm_to_utc_ms(session_date, cfg.commitment_deadline_et)
    if not (session_close_utc < decision_utc <= decision_cutoff_utc < deadline_utc):
        raise ValueError(
            "Three-timestamp EOD contract violated: must satisfy "
            f"session_close_utc ({session_close_utc}) < decision_instant_utc ({decision_utc}) "
            f"<= decision_cutoff_utc ({decision_cutoff_utc}) < commitment_deadline_utc ({deadline_utc})"
        )
    return {
        "session_close_utc": session_close_utc,
        "scheduled_decision_instant_utc": decision_utc,
        "decision_cutoff_utc": decision_cutoff_utc,
        "commitment_deadline_utc": deadline_utc,
    }


def _utc_now_ms() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def _peak_rss_bytes() -> int:
    ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports ru_maxrss in bytes; Linux reports KiB
    return int(ru if sys.platform == "darwin" else ru * 1024)


def check_footprint_budget(data_dir: str | Path, max_bytes: int = 2 * 1024 * 1024 * 1024) -> dict[str, object]:
    """Measure total byte footprint under data_dir and compare against max_bytes (2 GiB)."""
    root = Path(data_dir)
    total = 0
    file_count = 0
    if root.exists():
        for path in root.rglob("*"):
            if path.is_file():
                total += path.stat().st_size
                file_count += 1
    return {
        "data_dir": str(root),
        "total_bytes": total,
        "max_bytes": max_bytes,
        "within_budget": total <= max_bytes,
        "file_count": file_count,
    }


_ACTIVE_LOG_NAMES: frozenset[str] = frozenset({"shadow-drill.log", "shadow-missed-run.log"})


def _truncate_log_in_place(path: Path) -> int:
    """Truncate an active log file in place to preserve its open inode; return bytes reclaimed."""
    sz = path.stat().st_size
    with path.open("r+b") as fh:
        fh.truncate(0)
    return sz


def rotate_logs(
    logs_dir: str | Path,
    *,
    max_age_days: int = 30,
    max_total_bytes: int = 50 * 1024 * 1024,
    now_ts: float | None = None,
) -> dict:
    """Enforce 30-day retention and size cap on logs_dir while keeping active runner inodes intact."""
    root = Path(logs_dir)
    if not root.exists():
        return {"removed_old": 0, "removed_oversize": 0, "truncated_active": 0, "remaining_bytes": 0}
    ref_ts = now_ts if now_ts is not None else time.time()
    cutoff = ref_ts - max_age_days * 86400
    removed_old = 0
    truncated_active = 0
    kept: list[tuple[float, int, bool, Path]] = []
    for p in root.glob("*.log"):
        if not p.is_file():
            continue
        st = p.stat()
        is_active = p.name in _ACTIVE_LOG_NAMES
        if st.st_mtime < cutoff:
            if is_active:
                reclaimed = _truncate_log_in_place(p)
                if reclaimed > 0:
                    truncated_active += 1
                kept.append((st.st_mtime, 0, True, p))
            else:
                p.unlink()
                removed_old += 1
        else:
            kept.append((st.st_mtime, st.st_size, is_active, p))

    # Prefer unlinking non-active rotated logs before truncating active inodes under total size cap
    kept.sort(key=lambda item: (1 if item[2] else 0, item[0]))
    total = sum(item[1] for item in kept)
    removed_oversize = 0
    while total > max_total_bytes and kept:
        _, sz, is_active, oldest = kept.pop(0)
        if is_active:
            reclaimed = _truncate_log_in_place(oldest)
            total -= reclaimed
            if reclaimed > 0:
                truncated_active += 1
        else:
            oldest.unlink()
            total -= sz
            removed_oversize += 1

    return {
        "removed_old": removed_old,
        "removed_oversize": removed_oversize,
        "truncated_active": truncated_active,
        "remaining_bytes": total,
    }


def _append_unanchored_record(ledger: ShadowLedger, record_body: dict) -> dict:
    """Persist unanchored record via ShadowLedger enforcing full C7 chain identity & atomic write."""
    return ledger.append_record(record_body, allow_unanchored=True)


def _count_consecutive_trading_sessions(
    prior_records: list[dict],
    session_date: str,
    spec: dict,
    venue: str,
    *,
    current_appended: bool,
) -> int:
    """Count trailing consecutive XNYS trading sessions ending at or before session_date."""
    if not current_appended and not prior_records:
        return 0
    prior_sessions = {
        env["record_body"]["session_date"]
        for env in prior_records
        if isinstance(env, dict) and isinstance(env.get("record_body"), dict)
    }
    cov_start = date.fromisoformat(spec["rules"]["coverage_start_date"])
    cur = date.fromisoformat(session_date) - timedelta(days=1)
    streak = 0
    while cur >= cov_start:
        ds = cur.isoformat()
        cal = resolve_session(venue, ds, spec=spec)
        if not cal.get("is_trading_day") or cal.get("blocks_eligibility"):
            cur -= timedelta(days=1)
            continue
        if ds in prior_sessions:
            streak += 1
            cur -= timedelta(days=1)
        else:
            break
    return (streak + 1) if current_appended else streak


def _render_completeness_markdown(report: dict) -> str:
    lines = [
        f"# Daily Drill Completeness Report — {report['session_date']}",
        "",
        f"- **Chain ID:** `{report['chain_id']}` (mode: `{report['mode']}`, drill-only; CLK-01 inactive)",
        f"- **Calendar:** `{report['calendar_version']}` (`{report['calendar_spec_digest']}`)",
        f"- **Session State:** `{report['session_state']}`",
        f"- **Decision Instant (UTC):** `{report['decision_timestamp_utc']}`",
        f"- **Commitment Deadline (UTC):** `{report['commitment_deadline_utc']}`",
        f"- **Ledger Appended:** `{report['ledger']['appended']}` (head: `{report['ledger']['chain_head_digest']}`)",
        f"- **Receipt Status:** `{report['receipt']['status']}` (`anchor={report['receipt']['anchor']}`, `creditable={report['receipt']['creditable']}`)",
        f"- **Standing Assertions:** `kelly_applied={report['standing_assertions']['kelly_applied']}`, `orders_placed={report['standing_assertions']['orders_placed']}`, `B0={report['standing_assertions']['B0']}`, `H={report['standing_assertions']['H']}`",
        f"- **Notifications Sent:** `{report['notifications_sent']}` (`none_authorized_per_brief_1_6`)",
        "",
        "## Required Series Status",
        "",
        "| Instrument | Provider | Status | Normalized Value | Raw SHA-256 | Reason / Disclosures |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for s in report["required_series"]:
        val = s["normalized_value"] if s["normalized_value"] is not None else "null"
        sha = s["raw_bytes_sha256"][:16] + "..." if s["raw_bytes_sha256"] else "none"
        detail = s["reason"] or json.dumps(s["disclosures"], sort_keys=True)
        lines.append(f"| {s['instrument']} | {s['provider']} | {s['status']} | `{val}` | `{sha}` | {detail} |")
    return "\n".join(lines) + "\n"


def _atomic_write_text(path: Path, text: str) -> None:
    _atomic_write_bytes(path, text.encode("utf-8"))


def _report_series_complete(series_list: object) -> bool:
    if not isinstance(series_list, list):
        return False
    present: set[str] = set()
    for item in series_list:
        if not isinstance(item, dict):
            return False
        inst = item.get("instrument")
        if not isinstance(inst, str) or not inst or inst in present:
            return False
        if item.get("status") not in ("arrived", "revised"):
            return False
        val = item.get("normalized_value")
        if not isinstance(val, str) or not val:
            return False
        present.add(inst)
    return present >= set(REQUIRED_MARKET_SERIES)


def _market_records_complete(mkt_list: object) -> bool:
    if not isinstance(mkt_list, list):
        return False
    present: set[str] = set()
    for item in mkt_list:
        if not isinstance(item, dict):
            return False
        inst = item.get("instrument")
        if not isinstance(inst, str) or not inst or inst in present:
            return False
        val = item.get("normalized_value")
        elig = item.get("eligibility")
        if (
            not isinstance(val, str)
            or not val
            or not isinstance(elig, dict)
            or elig.get("eligible") is not True
        ):
            return False
        present.add(inst)
    return present >= set(REQUIRED_MARKET_SERIES)


def _validate_bundle_session_against_record(
    sess_entry: object,
    env: dict,
    idx: int,
) -> tuple[bool, str | None]:
    """Verify one replay_bundle session entry matches its committed ledger record."""
    if not isinstance(sess_entry, dict):
        return False, f"invalid_bundle_session_entry_{idx}"
    pb = env.get("record_body", {}) if isinstance(env, dict) else {}
    if not isinstance(pb, dict):
        return False, f"invalid_committed_record_body_{idx}"
    if sess_entry.get("session_date") != pb.get("session_date"):
        return False, f"bundle_session_date_mismatch_seq_{idx}"
    if sess_entry.get("decision_timestamp_utc") != pb.get("decision_timestamp_utc"):
        return False, f"bundle_decision_timestamp_mismatch_seq_{idx}"
    if sess_entry.get("discipline_labels") != pb.get("discipline_labels"):
        return False, f"bundle_discipline_labels_mismatch_seq_{idx}"

    mkt_recs = sess_entry.get("market_records")
    if not _market_records_complete(mkt_recs):
        return False, f"bundle_incomplete_or_null_market_records_seq_{idx}"
    assert isinstance(mkt_recs, list)
    computed_mkt_digest = sha256_canonical(mkt_recs)
    if sess_entry.get("market_records_digest") != computed_mkt_digest:
        return False, f"bundle_market_records_digest_mismatch_seq_{idx}"

    sess_inputs = sess_entry.get("inputs")
    if not isinstance(sess_inputs, dict) or not set(sess_inputs.keys()).issubset(_ALLOWED_SESSION_INPUT_KEYS):
        return False, f"bundle_invalid_inputs_shape_seq_{idx}"
    funds = sess_inputs.get("funds")
    if not isinstance(funds, dict) or set(funds.keys()) != set(INSTRUMENT_ORDER):
        return False, f"bundle_invalid_inputs_funds_seq_{idx}"
    mkt_by_inst = {rec["instrument"]: rec for rec in mkt_recs}
    for inst in INSTRUMENT_ORDER:
        fcfg = funds.get(inst)
        if not isinstance(fcfg, dict) or set(fcfg.keys()) != _ALLOWED_FUND_INPUT_KEYS:
            return False, f"bundle_invalid_fund_keys_{inst}_seq_{idx}"
        try:
            if canonical_decimal_str(fcfg["close_price"]) != canonical_decimal_str(
                mkt_by_inst[inst]["normalized_value"]
            ):
                return False, f"bundle_close_price_mismatch_{inst}_seq_{idx}"
        except Exception as exc:
            return False, f"bundle_invalid_close_price_{inst}_seq_{idx}:{exc}"
    try:
        if canonical_decimal_str(sess_inputs.get("H", "0")) != "0":
            return False, f"bundle_non_zero_H_seq_{idx}"
        w_ref = pb.get("bankroll", {}).get("W_ref") if isinstance(pb.get("bankroll"), dict) else None
        ev_block = pb.get("evaluator") if isinstance(pb.get("evaluator"), dict) else {}
        eff_scope = ev_block.get("effective_state_by_scope") if isinstance(ev_block, dict) else {}
        act_state = resolve_aggregate_control_state(eff_scope) if isinstance(eff_scope, dict) else None
        computed_inputs_digest = compute_inputs_digest(
            sess_inputs,
            w_ref=w_ref,
            active_control_state=act_state,
            market_records_digest=computed_mkt_digest,
            session_date=str(pb.get("session_date")),
        )
    except Exception as exc:
        return False, f"bundle_inputs_digest_error_seq_{idx}:{exc}"
    if computed_inputs_digest != pb.get("inputs_digest"):
        return False, f"bundle_inputs_digest_mismatch_seq_{idx}"
    return True, None


def _validate_replay_bundle_matches_ledger(
    bundle_dir: Path,
    prior_records: list[dict],
    *,
    cfg: DrillSchedulerConfig,
    spec: dict,
    allow_trailing_uncommitted_tail: bool = False,
    prune_uncommitted_tail: bool = False,
    ledger: ShadowLedger | None = None,
    reports_dir: Path | None = None,
) -> tuple[bool, str | None, dict | None]:
    """Require replay_bundle.json to match all committed prior_records when ledger is non-empty."""
    if not prior_records:
        return True, None, None
    bundle_path = bundle_dir / "replay_bundle.json"
    if not bundle_path.is_file():
        return False, "missing_replay_bundle_for_non_empty_ledger", None
    try:
        bundle_obj = parse_json_no_duplicates(bundle_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, f"corrupt_replay_bundle:{exc}", None
    if not isinstance(bundle_obj, dict):
        return False, "invalid_replay_bundle_type", None
    if bundle_obj.get("chain_id") != cfg.chain_id or bundle_obj.get("mode") != cfg.mode:
        return False, "replay_bundle_chain_identity_mismatch", None
    if bundle_obj.get("calendar_version") != cfg.calendar_version:
        return False, "replay_bundle_calendar_version_mismatch", None
    if bundle_obj.get("calendar_spec_digest") != spec["spec_digest"]:
        return False, "replay_bundle_calendar_digest_mismatch", None

    pb0 = prior_records[0].get("record_body", {}) if isinstance(prior_records[0], dict) else {}
    b_code = bundle_obj.get("code_version_sha")
    b_phash = bundle_obj.get("policy_config_hash")
    b_rcfg = bundle_obj.get("runtime_config")
    if (
        not isinstance(b_code, str)
        or not isinstance(b_phash, str)
        or not isinstance(b_rcfg, dict)
        or b_code != pb0.get("code_version_sha")
        or b_phash != pb0.get("policy_config_hash")
    ):
        return False, "replay_bundle_policy_or_code_identity_mismatch", None
    try:
        recomputed_phash = policy_config_hash(
            runtime_config=b_rcfg,
            calendar_spec=spec,
            code_manifest_hash=b_code,
        )
    except Exception as exc:
        return False, f"replay_bundle_policy_hash_error:{exc}", None
    if recomputed_phash != b_phash:
        return False, "replay_bundle_policy_config_hash_mismatch", None

    sessions = bundle_obj.get("sessions")
    if not isinstance(sessions, list):
        return False, "replay_bundle_session_count_mismatch:ledger=valid,bundle=invalid", None

    n = len(prior_records)
    has_uncommitted_tail = allow_trailing_uncommitted_tail and len(sessions) == n + 1
    if len(sessions) != n and not has_uncommitted_tail:
        return (
            False,
            f"replay_bundle_session_count_mismatch:ledger={n},bundle={len(sessions)}",
            None,
        )

    for idx, (env, sess_entry) in enumerate(zip(prior_records, sessions[:n])):
        ok_sess, err_sess = _validate_bundle_session_against_record(sess_entry, env, idx)
        if not ok_sess:
            return False, err_sess, None

    if has_uncommitted_tail:
        committed_dates = {
            r["record_body"]["session_date"]
            for r in prior_records
            if isinstance(r, dict) and isinstance(r.get("record_body"), dict)
        }
        tail = sessions[-1]
        tail_ds = tail.get("session_date") if isinstance(tail, dict) else None
        if (
            not isinstance(tail, dict)
            or not isinstance(tail_ds, str)
            or not _DATE_YYYY_MM_DD_RE.fullmatch(tail_ds)
            or tail_ds in committed_dates
        ):
            return False, f"invalid_or_duplicate_trailing_bundle_session:{tail_ds!r}", None
        try:
            date.fromisoformat(tail_ds)
        except ValueError:
            return False, f"invalid_trailing_bundle_session_date:{tail_ds!r}", None
        if reports_dir is not None:
            has_committed_rep, rep_err = _report_has_committed_evidence(reports_dir, tail_ds, expected_seq=n)
            if has_committed_rep:
                return (
                    False,
                    f"{rep_err}:{tail_ds}:seq={n}",
                    None,
                )
        if ledger is not None:
            has_det_ev, det_reason = has_committed_detached_evidence(ledger, n)
            if has_det_ev:
                return (
                    False,
                    f"committed_session_missing_ledger_record:{tail_ds}:seq={n}:{det_reason}",
                    None,
                )
        if prune_uncommitted_tail:
            bundle_obj["sessions"] = list(sessions[:n])
            _atomic_write_text(bundle_path, json.dumps(bundle_obj, indent=2, sort_keys=True) + "\n")
            if ledger is not None and not ledger._record_path(n).exists():
                for orphan_p in (ledger._ots_path(n), ledger._receipt_path(n)):
                    if orphan_p.exists():
                        orphan_p.unlink()
            if len(bundle_obj["sessions"]) != n:
                return False, "replay_bundle_prune_verification_failed", None

    return True, None, bundle_obj


def _report_has_committed_evidence(
    reports_dir: Path,
    session_date: str,
    *,
    expected_seq: int | None = None,
) -> tuple[bool, str | None]:
    """Return (True, reason) if completeness JSON or Markdown shows committed ledger evidence or corrupt committed state."""
    tail_json = reports_dir / f"completeness_{session_date}.json"
    tail_md = reports_dir / f"completeness_{session_date}.md"
    if tail_json.is_file():
        try:
            tail_rep = parse_json_no_duplicates(tail_json.read_text(encoding="utf-8"))
        except Exception:
            return True, "committed_tail_report_unreadable"
        if not isinstance(tail_rep, dict):
            return True, "committed_tail_report_unreadable"
        tail_lmeta = tail_rep.get("ledger")
        if isinstance(tail_lmeta, dict) and (
            tail_lmeta.get("appended") is True
            or (expected_seq is not None and tail_lmeta.get("sequence") == expected_seq)
            or (
                isinstance(tail_lmeta.get("chain_head_digest"), str)
                and len(tail_lmeta.get("chain_head_digest")) == 64
                and tail_lmeta.get("fail_closed_reason") is None
            )
        ):
            return True, "committed_session_missing_ledger_record"
    if tail_md.is_file():
        try:
            md_txt = tail_md.read_text(encoding="utf-8")
        except Exception:
            return True, "committed_tail_report_unreadable"
        if (
            "- **Ledger Appended:** `True`" in md_txt
            or "- **Appended:** `True`" in md_txt
            or (expected_seq is not None and f"- **Sequence:** `{expected_seq}`" in md_txt)
        ):
            return True, "committed_session_missing_ledger_record"
    return False, None


def _detect_orphaned_committed_evidence_for_session(
    root: Path,
    ledger: ShadowLedger,
    records: list[dict],
    reports_dir: Path,
    session_date: str,
) -> str | None:
    """Detect committed evidence on disk (report MD/JSON, replay_bundle, or uncommitted receipt/OTS) without a ledger record for session_date."""
    has_rep_ev, rep_err = _report_has_committed_evidence(reports_dir, session_date)
    if has_rep_ev:
        return rep_err or "committed_report_without_ledger_record"

    bundle_path = root / "replay" / "replay_bundle.json"
    bundle_sessions: list[object] = []
    if bundle_path.is_file():
        try:
            bundle_obj = parse_json_no_duplicates(bundle_path.read_text(encoding="utf-8"))
        except Exception:
            return "corrupt_replay_bundle_without_ledger_record"
        if not isinstance(bundle_obj, dict) or not isinstance(bundle_obj.get("sessions"), list):
            return "corrupt_replay_bundle_without_ledger_record"
        bundle_sessions = bundle_obj["sessions"]
        for entry in bundle_sessions:
            if not isinstance(entry, dict):
                return "corrupt_replay_bundle_without_ledger_record"
            if entry.get("session_date") == session_date:
                return "orphaned_replay_bundle_without_ledger_record"

    committed_seqs = {
        r["record_body"]["sequence"]
        for r in records
        if isinstance(r, dict)
        and isinstance(r.get("record_body"), dict)
        and isinstance(r["record_body"].get("sequence"), int)
        and not isinstance(r["record_body"].get("sequence"), bool)
    }
    receipt_session_by_seq: dict[int, object] = {}
    if ledger.root_dir.is_dir():
        for rpath in sorted(ledger.root_dir.glob("seq_*.receipt.json")):
            stem = rpath.name[len("seq_") : -len(".receipt.json")]
            if not stem.isdigit():
                continue
            seq_num = int(stem)
            if seq_num in committed_seqs:
                continue
            try:
                rcp_obj = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
            except Exception:
                return f"corrupt_orphaned_receipt_seq_{seq_num}"
            if isinstance(rcp_obj, dict):
                receipt_session_by_seq[seq_num] = rcp_obj.get("session_date")
                if rcp_obj.get("session_date") == session_date:
                    return f"orphaned_receipt_without_ledger_record_seq_{seq_num}"
        for opath in sorted(ledger.root_dir.glob("seq_*.ots")):
            ostem = opath.name[len("seq_") : -len(".ots")]
            if not ostem.isdigit():
                continue
            oseq = int(ostem)
            if oseq in committed_seqs:
                continue
            if oseq in receipt_session_by_seq and receipt_session_by_seq[oseq] not in (None, session_date):
                continue
            has_o_ev, o_reason = has_committed_detached_evidence(ledger, oseq)
            if has_o_ev:
                return str(o_reason or f"orphaned_ots_without_ledger_record_seq_{oseq}")
        if not records and not ledger._record_files() and (ledger._ots_path(0).is_file() or ledger._receipt_path(0).is_file()):
            # Missing seq_0 record file with seq_0 detached OTS/receipt and any existing bundle/report evidence indicates orphaned/missing seq_0
            if bundle_sessions or (reports_dir / f"completeness_{session_date}.json").is_file():
                return "orphaned_seq0_proof_without_ledger_record"

    return None


def _bundle_deadline_config_mismatch(
    bundle_obj: object,
    cfg: DrillSchedulerConfig,
) -> str | None:
    if not isinstance(bundle_obj, dict):
        return None
    rcfg = bundle_obj.get("runtime_config")
    if not isinstance(rcfg, dict):
        return None
    for key in (
        "decision_instant_et",
        "half_day_run_time_et",
        "decision_cutoff_et",
        "decision_cutoff_half_day_et",
        "commitment_deadline_et",
    ):
        committed_val = rcfg.get(key)
        cur_val = getattr(cfg, key)
        if committed_val != cur_val:
            return f"committed_bundle_config_mismatch:{key}:committed={committed_val!r}!=current={cur_val!r}"
    return None


def _is_committed_session_report_valid(
    reports_dir: Path,
    session_date: str,
    committed_env: dict,
    *,
    committed_mkt_records: list[dict],
    deadlines: Mapping[str, str],
    cfg: DrillSchedulerConfig,
    cal: Mapping[str, object],
    ledger: ShadowLedger,
    expected_consecutive_count: int,
) -> bool:
    """Return True iff both completeness JSON and Markdown exist, match ledger+bundle+receipt, and render cleanly."""
    json_path = reports_dir / f"completeness_{session_date}.json"
    md_path = reports_dir / f"completeness_{session_date}.md"
    if not json_path.is_file() or not md_path.is_file():
        return False
    try:
        rep_obj = parse_json_no_duplicates(json_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(rep_obj, dict):
        return False
    body = committed_env.get("record_body", {})
    if not isinstance(body, dict):
        return False

    if rep_obj.get("session_date") != session_date or body.get("session_date") != session_date:
        return False
    if rep_obj.get("chain_id") != cfg.chain_id or body.get("chain_id") != cfg.chain_id:
        return False
    if rep_obj.get("mode") != cfg.mode or body.get("mode") != cfg.mode:
        return False
    if (
        rep_obj.get("calendar_version") != cal.get("calendar_version")
        or body.get("calendar_version") != cal.get("calendar_version")
    ):
        return False
    if (
        rep_obj.get("calendar_spec_digest") != cal.get("spec_digest")
        or body.get("calendar_spec_digest") != cal.get("spec_digest")
    ):
        return False
    if rep_obj.get("session_state") != cal.get("session_state"):
        return False
    if rep_obj.get("session_close_utc") != deadlines.get("session_close_utc"):
        return False
    if rep_obj.get("decision_cutoff_utc") != deadlines.get("decision_cutoff_utc"):
        return False
    if rep_obj.get("commitment_deadline_utc") != deadlines.get("commitment_deadline_utc"):
        return False
    if rep_obj.get("decision_timestamp_utc") != body.get("decision_timestamp_utc"):
        return False
    decision_ts = body.get("decision_timestamp_utc")
    if (
        not is_utc_ms_timestamp(decision_ts)
        or decision_ts < deadlines["session_close_utc"]
        or decision_ts > deadlines["decision_cutoff_utc"]
        or decision_ts > deadlines["commitment_deadline_utc"]
    ):
        return False
    if rep_obj.get("observation_class") != body.get("observation_class"):
        return False
    if rep_obj.get("notifications_sent") != 0:
        return False
    expected_scope = "clk01_forward_clock" if cfg.mode == "production" else "drill_only_not_clk01"
    if rep_obj.get("consecutive_session_scope") != expected_scope:
        return False
    consec = rep_obj.get("consecutive_session_count")
    if isinstance(consec, bool) or consec != expected_consecutive_count:
        return False

    rep_created = rep_obj.get("created_at_utc")
    if (
        not is_utc_ms_timestamp(rep_created)
        or rep_created < deadlines["session_close_utc"]
        or rep_created > deadlines["commitment_deadline_utc"]
    ):
        return False

    ledger_meta = rep_obj.get("ledger")
    if not isinstance(ledger_meta, dict) or ledger_meta.get("appended") is not True:
        return False
    if ledger_meta.get("sequence") != body.get("sequence"):
        return False
    if ledger_meta.get("chain_head_digest") != committed_env.get("record_sha256"):
        return False
    if ledger_meta.get("fail_closed_reason") is not None:
        return False

    body_assert = body.get("assertions")
    if not isinstance(body_assert, dict):
        return False
    expected_standing = {k: body_assert.get(k) for k in _STANDING_ASSERTION_KEYS}
    rep_standing = rep_obj.get("standing_assertions")
    if not _standing_assertions_valid(rep_standing) or rep_standing != expected_standing:
        return False

    expected_series = _summarize_required_series(committed_mkt_records)
    if (
        not _report_series_complete(rep_obj.get("required_series"))
        or rep_obj.get("required_series") != expected_series
    ):
        return False

    try:
        expected_rcp = _summarize_committed_receipt(
            ledger=ledger,
            seq=body["sequence"],
            env=committed_env,
            deadline_utc=str(deadlines["commitment_deadline_utc"]),
            decision_utc=str(body["decision_timestamp_utc"]),
        )
    except Exception:
        return False
    rep_rcp = rep_obj.get("receipt")
    if not isinstance(rep_rcp, dict):
        return False
    for rcp_key in (
        "anchor",
        "status",
        "ots_file",
        "ots_sha256",
        "obtained_at_utc",
        "commitment_deadline_utc",
        "creditable",
    ):
        if rep_rcp.get(rcp_key) != expected_rcp.get(rcp_key):
            return False
    rep_reasons = rep_rcp.get("non_creditable_reasons")
    if expected_rcp.get("status") == "missing":
        if (
            not isinstance(rep_reasons, list)
            or len(rep_reasons) < 2
            or not str(rep_reasons[0]).startswith("missing_ots_receipt")
            or "drill_mode" not in rep_reasons
        ):
            return False
    elif rep_reasons != expected_rcp.get("non_creditable_reasons"):
        return False

    try:
        md_text = md_path.read_text(encoding="utf-8")
        expected_md = _render_completeness_markdown(rep_obj)
    except Exception:
        return False
    return md_text == expected_md


_NON_FAIL_CLOSED_PRE_OR_LATE_REASONS = (
    "late_after_commitment_deadline_backfill_rejected",
    "late_after_decision_cutoff",
    "session_not_yet_closed",
)


def _standing_assertions_valid(standing: object) -> bool:
    if not isinstance(standing, dict) or set(standing.keys()) != set(_STANDING_ASSERTION_KEYS):
        return False
    return (
        standing.get("kelly_applied") is False
        and type(standing.get("orders_placed")) is int
        and standing.get("orders_placed") == 0
        and standing.get("B0") == "50000"
        and standing.get("H") == "0"
        and standing.get("stop_loss_return_rule") == "OFF"
        and standing.get("loss_controls_armed") is False
    )


_REQUIRED_SERIES_ROW_KEYS = frozenset(
    {
        "instrument",
        "provider",
        "series_id",
        "status",
        "retrieval_time_utc",
        "normalized_value",
        "raw_bytes_sha256",
        "reason",
        "disclosures",
    }
)
_VALID_REQUIRED_SERIES_STATUSES = frozenset({"arrived", "revised", "missing", "late", "pre_close"})


_PRE_INGEST_FAIL_CLOSED_PREFIXES = (
    "existing_chain_verification_failed:",
    "O2:",
)


def _is_fail_closed_session_report_valid(
    *,
    rep_path: Path,
    md_path: Path,
    rep_obj: dict,
    session_date: str,
    cfg: DrillSchedulerConfig,
    cal: dict,
    deadlines: dict[str, str],
) -> bool:
    if not rep_path.is_file() or not md_path.is_file() or not isinstance(rep_obj, dict):
        return False
    if rep_obj.get("session_date") != session_date:
        return False
    if rep_obj.get("chain_id") != cfg.chain_id:
        return False
    if rep_obj.get("mode") != cfg.mode:
        return False
    if rep_obj.get("calendar_version") != cal.get("calendar_version"):
        return False
    if rep_obj.get("calendar_spec_digest") != cal.get("spec_digest"):
        return False
    if rep_obj.get("session_state") != cal.get("session_state"):
        return False
    if rep_obj.get("session_close_utc") != deadlines.get("session_close_utc"):
        return False
    if rep_obj.get("decision_cutoff_utc") != deadlines.get("decision_cutoff_utc"):
        return False
    if rep_obj.get("commitment_deadline_utc") != deadlines.get("commitment_deadline_utc"):
        return False
    if rep_obj.get("observation_class") != "synthetic":
        return False
    notif = rep_obj.get("notifications_sent")
    if type(notif) is not int or notif != 0:
        return False
    expected_scope = "clk01_forward_clock" if cfg.mode == "production" else "drill_only_not_clk01"
    if rep_obj.get("consecutive_session_scope") != expected_scope:
        return False
    consec = rep_obj.get("consecutive_session_count")
    if type(consec) is not int or consec < 0:
        return False

    rep_decision = rep_obj.get("decision_timestamp_utc")
    rep_created = rep_obj.get("created_at_utc")
    if not is_utc_ms_timestamp(rep_decision) or not is_utc_ms_timestamp(rep_created):
        return False
    if (
        rep_decision < deadlines["session_close_utc"]
        or rep_decision > deadlines["decision_cutoff_utc"]
        or rep_decision > deadlines["commitment_deadline_utc"]
    ):
        return False
    if (
        rep_created < deadlines["session_close_utc"]
        or rep_created > deadlines["commitment_deadline_utc"]
    ):
        return False

    ledger_meta = rep_obj.get("ledger")
    if not isinstance(ledger_meta, dict) or ledger_meta.get("appended") is not False:
        return False
    head_dig = ledger_meta.get("chain_head_digest")
    if not isinstance(head_dig, str) or len(head_dig) != 64:
        return False
    fc_reason = ledger_meta.get("fail_closed_reason")
    if not isinstance(fc_reason, str) or not fc_reason.strip():
        return False
    if any(tok in fc_reason for tok in _NON_FAIL_CLOSED_PRE_OR_LATE_REASONS):
        return False

    if not _standing_assertions_valid(rep_obj.get("standing_assertions")):
        return False

    req_series = rep_obj.get("required_series")
    if not isinstance(req_series, list):
        return False
    is_pre_ingest = any(fc_reason.startswith(prefix) for prefix in _PRE_INGEST_FAIL_CLOSED_PREFIXES)
    if is_pre_ingest:
        if req_series != []:
            return False
    else:
        if len(req_series) != len(REQUIRED_MARKET_SERIES):
            return False
    seen_instruments: set[str] = set()
    for row in req_series:
        if not isinstance(row, dict) or set(row.keys()) != _REQUIRED_SERIES_ROW_KEYS:
            return False
        inst = row.get("instrument")
        prov = row.get("provider")
        sid = row.get("series_id")
        status = row.get("status")
        if (
            not isinstance(inst, str)
            or not inst
            or inst in seen_instruments
            or not isinstance(prov, str)
            or not prov
            or not isinstance(sid, str)
            or not sid
            or not isinstance(status, str)
            or status not in _VALID_REQUIRED_SERIES_STATUSES
        ):
            return False
        seen_instruments.add(inst)
        ret_utc = row.get("retrieval_time_utc")
        if status in ("arrived", "revised"):
            if not is_utc_ms_timestamp(ret_utc):
                return False
        elif ret_utc is not None and not is_utc_ms_timestamp(ret_utc):
            return False
    if not is_pre_ingest and seen_instruments != set(REQUIRED_MARKET_SERIES):
        return False

    rep_rcp = rep_obj.get("receipt")
    if not isinstance(rep_rcp, dict):
        return False
    if rep_rcp.get("commitment_deadline_utc") != deadlines.get("commitment_deadline_utc"):
        return False
    rcp_status = rep_rcp.get("status")
    if not isinstance(rcp_status, str) or not rcp_status.strip():
        return False
    if rep_rcp.get("creditable") is not False:
        return False
    if "anchor" not in rep_rcp:
        return False
    rcp_reasons = rep_rcp.get("non_creditable_reasons")
    if not isinstance(rcp_reasons, list) or not rcp_reasons or not all(isinstance(r, str) and r for r in rcp_reasons):
        return False

    try:
        md_text = md_path.read_text(encoding="utf-8")
        expected_md = _render_completeness_markdown(rep_obj)
    except Exception:
        return False
    return md_text == expected_md


def _is_immutable_late_or_preclose_report(existing_rep: dict | None) -> bool:
    if not isinstance(existing_rep, dict):
        return False
    ex_ledger = existing_rep.get("ledger")
    if not isinstance(ex_ledger, dict) or ex_ledger.get("appended") is not False:
        return False
    ex_reason = str(ex_ledger.get("fail_closed_reason") or "")
    if "late_after_" in ex_reason or "session_not_yet_closed" in ex_reason:
        return True
    ex_close = existing_rep.get("session_close_utc")
    ex_cutoff = existing_rep.get("decision_cutoff_utc")
    ex_dec = existing_rep.get("decision_timestamp_utc")
    return bool(
        is_utc_ms_timestamp(ex_close)
        and is_utc_ms_timestamp(ex_cutoff)
        and is_utc_ms_timestamp(ex_dec)
        and (ex_dec < ex_close or ex_dec > ex_cutoff)
    )


def _can_upgrade_prior_fail_closed_report(
    existing_rep: dict | None,
    new_report: dict,
    *,
    created_at_utc: str,
) -> bool:
    if not isinstance(existing_rep, dict):
        return False
    ex_ledger = existing_rep.get("ledger")
    if not isinstance(ex_ledger, dict) or ex_ledger.get("appended") is not False:
        return False
    if _is_immutable_late_or_preclose_report(existing_rep):
        return False
    new_ledger = new_report.get("ledger")
    if not isinstance(new_ledger, dict) or new_ledger.get("appended") is not True:
        return False
    new_dec = new_report.get("decision_timestamp_utc")
    new_cutoff = new_report.get("decision_cutoff_utc")
    new_deadline = new_report.get("commitment_deadline_utc")
    if not (
        is_utc_ms_timestamp(new_dec)
        and is_utc_ms_timestamp(new_cutoff)
        and is_utc_ms_timestamp(new_deadline)
        and is_utc_ms_timestamp(created_at_utc)
    ):
        return False
    if new_dec > new_cutoff or created_at_utc > new_deadline:
        return False
    return _report_series_complete(new_report.get("required_series"))


def _write_completeness_report(
    reports_dir: Path,
    report: dict,
    *,
    created_at_utc: str,
    post_json_write_hook: Callable[[], None] | None = None,
    allow_committed_recovery: bool = False,
) -> tuple[tuple[Path, Path], bool]:
    """Write completeness JSON+MD atomically; handle initial upgrade vs committed missing/corrupt recovery."""
    reports_dir.mkdir(parents=True, exist_ok=True)
    ds = report["session_date"]
    json_path = reports_dir / f"completeness_{ds}.json"
    md_path = reports_dir / f"completeness_{ds}.md"
    if json_path.exists() or md_path.exists():
        existing_rep: dict | None = None
        if json_path.is_file():
            try:
                parsed_rep = parse_json_no_duplicates(json_path.read_text(encoding="utf-8"))
                if isinstance(parsed_rep, dict):
                    existing_rep = parsed_rep
            except Exception:
                existing_rep = None
        if allow_committed_recovery:
            new_ledger = report.get("ledger")
            new_deadline = report.get("commitment_deadline_utc")
            if (
                not isinstance(new_ledger, dict)
                or new_ledger.get("appended") is not True
                or not is_utc_ms_timestamp(new_deadline)
                or not is_utc_ms_timestamp(created_at_utc)
                or created_at_utc > new_deadline
                or not _report_series_complete(report.get("required_series"))
                or _is_immutable_late_or_preclose_report(existing_rep)
            ):
                return (json_path, md_path), False
        elif not _can_upgrade_prior_fail_closed_report(
            existing_rep, report, created_at_utc=created_at_utc
        ):
            return (json_path, md_path), False
    out_report = dict(report)
    out_report["created_at_utc"] = created_at_utc
    _atomic_write_text(json_path, json.dumps(out_report, indent=2, sort_keys=True) + "\n")
    if post_json_write_hook is not None:
        post_json_write_hook()
    _atomic_write_text(md_path, _render_completeness_markdown(out_report))
    return (json_path, md_path), True


def _update_replay_bundle(
    bundle_dir: Path,
    *,
    cfg: DrillSchedulerConfig,
    spec: dict,
    code_sha: str,
    runtime_config: dict[str, object],
    archive_dir: Path,
    session_entry: dict,
    seq: int,
    engine_hash: str | None = None,
    spec_version: str | None = None,
) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = bundle_dir / "replay_bundle.json"
    p_hash = policy_config_hash(
        runtime_config=runtime_config,
        calendar_spec=spec,
        code_manifest_hash=code_sha,
    )
    if bundle_path.is_file():
        bundle = parse_json_no_duplicates(bundle_path.read_text(encoding="utf-8"))
        if not isinstance(bundle, dict):
            raise ValueError("invalid_replay_bundle_type")
    else:
        if seq > 0:
            raise ValueError(f"missing_replay_bundle_for_non_empty_ledger:seq={seq}")
        bundle = {
            "chain_id": cfg.chain_id,
            "mode": cfg.mode,
            "calendar_version": cfg.calendar_version,
            "calendar_spec_dir": spec["spec_dir"],
            "calendar_spec_digest": spec["spec_digest"],
            "policy_config_hash": p_hash,
            "runtime_config": runtime_config,
            "code_version_sha": code_sha,
            "archive_dir": str(archive_dir),
            "sessions": [],
        }
        if engine_hash is not None:
            bundle["engine_hash"] = engine_hash
        if spec_version is not None:
            bundle["spec_version"] = spec_version
    sessions = list(bundle.get("sessions", []))
    if len(sessions) < seq:
        raise ValueError(f"replay_bundle_truncated_vs_ledger:len={len(sessions)}<seq={seq}")
    # Drop any uncommitted orphan session entry left by an abrupt kill before seq_NNNNNN.record.json rename
    if len(sessions) > seq:
        sessions = sessions[:seq]
    if seq == 0:
        bundle["chain_id"] = cfg.chain_id
        bundle["mode"] = cfg.mode
        bundle["calendar_version"] = cfg.calendar_version
        bundle["calendar_spec_dir"] = spec["spec_dir"]
        bundle["calendar_spec_digest"] = spec["spec_digest"]
        bundle["policy_config_hash"] = p_hash
        bundle["runtime_config"] = runtime_config
        bundle["code_version_sha"] = code_sha
        bundle["archive_dir"] = str(archive_dir)
        if engine_hash is not None:
            bundle["engine_hash"] = engine_hash
        if spec_version is not None:
            bundle["spec_version"] = spec_version
    sessions.append(session_entry)
    bundle["sessions"] = sessions
    _atomic_write_text(bundle_path, json.dumps(bundle, indent=2, sort_keys=True) + "\n")
    return bundle_path


def _commit_drill_session_atomically(
    *,
    ledger: ShadowLedger,
    seq: int,
    body: dict,
    ots_bytes: bytes | None,
    rcp_obtained_utc: str,
    deadline_utc: str,
    allow_unanchored: bool,
    bundle_dir: Path,
    cfg: DrillSchedulerConfig,
    spec: dict,
    code_sha: str,
    runtime_config: dict[str, object],
    archive_dir: Path,
    session_entry: dict,
    post_ledger_hook: Callable[[], None] | None = None,
    snapshots_dir: Path | None = None,
    next_row_state: dict | None = None,
    engine_hash: str | None = None,
    spec_version: str | None = None,
    trusted_head_anchor_path: Path | None = None,
    expected_prior_anchor_bytes: bytes | None = None,
    anchor_updated_at_utc: str | None = None,
) -> dict:
    """Serialize via chain_lock, stage replay_bundle + state snapshot before seq_NNNNNN.record.json commit."""
    with ledger.chain_lock():
        bundle_path = bundle_dir / "replay_bundle.json"
        prior_bundle_bytes = bundle_path.read_bytes() if bundle_path.is_file() else None
        bundle_modified = False

        should_stage_snap = (
            snapshots_dir is not None
            and next_row_state is not None
            and engine_hash is not None
            and spec_version is not None
        )
        seq_snap_path = snapshots_dir / f"seq_{seq:06d}.state.json" if should_stage_snap and snapshots_dir is not None else None
        latest_snap_path = snapshots_dir / "latest.state.json" if should_stage_snap and snapshots_dir is not None else None
        pending_snap_path = snapshots_dir / "pending_rejected_state.json" if snapshots_dir is not None else None
        prior_latest_bytes = (
            latest_snap_path.read_bytes()
            if latest_snap_path is not None and latest_snap_path.is_file()
            else None
        )
        snap_staged = False

        def _stage_bundle_before_marker() -> None:
            nonlocal bundle_modified, snap_staged
            _update_replay_bundle(
                bundle_dir,
                cfg=cfg,
                spec=spec,
                code_sha=code_sha,
                runtime_config=runtime_config,
                archive_dir=archive_dir,
                session_entry=session_entry,
                seq=seq,
                engine_hash=engine_hash,
                spec_version=spec_version,
            )
            bundle_modified = True
            if should_stage_snap and snapshots_dir is not None:
                save_row_state_snapshot(
                    snapshots_dir,
                    chain_id=cfg.chain_id,
                    sequence=seq,
                    session_date=str(body["session_date"]),
                    record_sha256=sha256_canonical(body),
                    engine_hash=engine_hash,  # type: ignore[arg-type]
                    spec_version=spec_version,  # type: ignore[arg-type]
                    next_row_state=next_row_state,  # type: ignore[arg-type]
                )
                snap_staged = True
            if post_ledger_hook is not None:
                post_ledger_hook()

        try:
            if ots_bytes is not None:
                env = ledger.append_record(
                    body,
                    obtained_at_utc=rcp_obtained_utc,
                    ots_bytes=ots_bytes,
                    commitment_deadline_utc=deadline_utc,
                    before_record_commit=_stage_bundle_before_marker,
                )
            elif allow_unanchored:
                env = ledger.append_record(
                    body,
                    allow_unanchored=True,
                    before_record_commit=_stage_bundle_before_marker,
                )
            else:
                raise RuntimeError("OTS receipt missing and unanchored append disabled")
        except Exception:
            if bundle_modified:
                if prior_bundle_bytes is not None:
                    _atomic_write_bytes(bundle_path, prior_bundle_bytes)
                elif bundle_path.exists():
                    bundle_path.unlink()
            if snap_staged:
                if seq_snap_path is not None and seq_snap_path.exists():
                    seq_snap_path.unlink()
                if latest_snap_path is not None:
                    if prior_latest_bytes is not None:
                        _atomic_write_bytes(latest_snap_path, prior_latest_bytes)
                    elif latest_snap_path.exists():
                        latest_snap_path.unlink()
            raise
        if pending_snap_path is not None and pending_snap_path.is_file():
            pending_snap_path.unlink()
        if trusted_head_anchor_path is not None:
            _advance_trusted_head_anchor(
                trusted_head_anchor_path,
                chain_id=cfg.chain_id,
                expected_prior_bytes=expected_prior_anchor_bytes,
                sequence=seq,
                session_date=str(body["session_date"]),
                head_sha256=env["record_sha256"],
                pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
                now_utc=anchor_updated_at_utc or rcp_obtained_utc,
            )
        return env


def _summarize_required_series(mkt_recs: list[dict]) -> list[dict]:
    summary: list[dict] = []
    for rec in mkt_recs:
        elig = rec.get("eligibility", {})
        if rec.get("normalized_value") is None or not elig.get("eligible"):
            reason = elig.get("reason")
            if reason == "late_after_decision_cutoff":
                st = "late"
            elif reason == "session_not_yet_closed":
                st = "pre_close"
            else:
                st = "missing"
        elif rec.get("revision_seq", 0) > 0:
            st = "revised"
        else:
            st = "arrived"
        summary.append(
            {
                "instrument": rec["instrument"],
                "provider": rec["provider"],
                "series_id": rec["series_id"],
                "status": st,
                "retrieval_time_utc": rec["retrieval_time_utc"],
                "normalized_value": rec["normalized_value"],
                "raw_bytes_sha256": rec["raw_bytes_sha256"],
                "reason": elig.get("reason"),
                "disclosures": rec["disclosures"],
            }
        )
    return summary


def _summarize_committed_receipt(
    *,
    ledger: ShadowLedger,
    seq: int,
    env: dict,
    deadline_utc: str,
    decision_utc: str,
    ots_error: str | None = None,
) -> dict:
    rpath = ledger._receipt_path(seq)
    opath = ledger._ots_path(seq)
    if rpath.is_file():
        rcp_obj = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
        ots_raw = opath.read_bytes() if opath.is_file() else None
        ok_rcp, err_rcp = verify_receipt(env, rcp_obj, ots_bytes=ots_raw)
        timely, timely_err = is_receipt_timely(
            rcp_obj,
            expected_commitment_deadline_utc=deadline_utc,
            decision_timestamp_utc=decision_utc,
            require_expected_deadline=True,
        )
        if not ok_rcp:
            status = "invalid"
            reasons = [str(err_rcp), "drill_mode"]
        elif timely:
            status = "timely"
            reasons = ["drill_mode"]
        else:
            status = "late"
            reasons = [str(timely_err), "drill_mode"]
        return {
            "anchor": "opentimestamps",
            "status": status,
            "ots_file": rcp_obj.get("ots_file"),
            "ots_sha256": rcp_obj.get("ots_sha256"),
            "obtained_at_utc": rcp_obj.get("obtained_at_utc"),
            "commitment_deadline_utc": deadline_utc,
            "creditable": False,
            "non_creditable_reasons": reasons,
        }
    missing_reason = f"missing_ots_receipt:{ots_error}" if ots_error else "missing_ots_receipt"
    return {
        "anchor": "opentimestamps",
        "status": "missing",
        "ots_file": None,
        "ots_sha256": None,
        "obtained_at_utc": None,
        "commitment_deadline_utc": deadline_utc,
        "creditable": False,
        "non_creditable_reasons": [missing_reason, "drill_mode"],
    }


def _build_committed_completeness_report(
    *,
    t_start: float,
    root: Path,
    logs_dir: Path,
    cfg: DrillSchedulerConfig,
    cal: dict,
    deadlines: dict[str, str],
    session_date: str,
    decision_utc: str,
    series_summary: list[dict],
    seq: int,
    record_sha256: str,
    receipt_summary: dict,
    consecutive_now: int,
    recovered_missing_report: bool = False,
) -> tuple[dict, dict[str, object]]:
    log_rotation = rotate_logs(
        logs_dir,
        max_age_days=cfg.log_retention_days,
        max_total_bytes=cfg.log_max_bytes,
    )
    footprint = check_footprint_budget(root, cfg.max_footprint_bytes)
    ledger_meta: dict[str, object] = {
        "appended": True,
        "sequence": seq,
        "chain_head_digest": record_sha256,
        "fail_closed_reason": None,
    }
    if recovered_missing_report:
        ledger_meta["recovered_missing_report"] = True
    report = {
        "session_date": session_date,
        "chain_id": cfg.chain_id,
        "mode": cfg.mode,
        "calendar_version": cal["calendar_version"],
        "calendar_spec_digest": cal["spec_digest"],
        "session_state": cal["session_state"],
        "session_close_utc": deadlines["session_close_utc"],
        "decision_cutoff_utc": deadlines["decision_cutoff_utc"],
        "decision_timestamp_utc": decision_utc,
        "commitment_deadline_utc": deadlines["commitment_deadline_utc"],
        "required_series": series_summary,
        "ledger": ledger_meta,
        "receipt": receipt_summary,
        "consecutive_session_count": consecutive_now,
        "consecutive_session_scope": (
            "clk01_forward_clock" if cfg.mode == "production" else "drill_only_not_clk01"
        ),
        "standing_assertions": {
            "kelly_applied": False,
            "orders_placed": 0,
            "B0": "50000",
            "H": "0",
            "stop_loss_return_rule": "OFF",
            "loss_controls_armed": False,
        },
        "observation_class": (
            "decision_time_market" if cfg.mode == "production" else "synthetic"
        ),
        "notifications_sent": 0,
        "metrics": {
            "wall_seconds": round(time.monotonic() - t_start, 6),
            "peak_rss_bytes": _peak_rss_bytes(),
            "log_rotation": log_rotation,
            "footprint": footprint,
        },
    }
    return report, footprint


def _build_fail_closed_result(
    *,
    t_start: float,
    root: Path,
    reports_dir: Path,
    cfg: DrillSchedulerConfig,
    cal: dict,
    session_date: str,
    decision_utc: str,
    deadlines: dict[str, str],
    head_hash: str,
    consecutive_count: int,
    created_at_utc: str,
    reason_key: str,
    fail_reason: str,
    required_series: list[dict],
    receipt_anchor: str | None = None,
    receipt_status: str = "not_applicable_fail_closed",
    receipt_obtained_at_utc: str | None = None,
    receipt_reasons: list[str] | None = None,
    pending_rejected_state_sha256: str | None = None,
) -> dict:
    report = {
        "session_date": session_date,
        "chain_id": cfg.chain_id,
        "mode": cfg.mode,
        "calendar_version": cal["calendar_version"],
        "calendar_spec_digest": cal["spec_digest"],
        "session_state": cal["session_state"],
        "session_close_utc": deadlines["session_close_utc"],
        "decision_cutoff_utc": deadlines["decision_cutoff_utc"],
        "decision_timestamp_utc": decision_utc,
        "commitment_deadline_utc": deadlines["commitment_deadline_utc"],
        "required_series": required_series,
        "ledger": {
            "appended": False,
            "sequence": None,
            "chain_head_digest": head_hash,
            "fail_closed_reason": fail_reason,
        },
        "receipt": {
            "anchor": receipt_anchor,
            "status": receipt_status,
            "ots_file": None,
            "ots_sha256": None,
            "obtained_at_utc": receipt_obtained_at_utc,
            "commitment_deadline_utc": deadlines["commitment_deadline_utc"],
            "creditable": False,
            "non_creditable_reasons": receipt_reasons or [fail_reason, "drill_mode"],
        },
        "consecutive_session_count": consecutive_count,
        "consecutive_session_scope": "drill_only_not_clk01",
        "standing_assertions": {
            "kelly_applied": False,
            "orders_placed": 0,
            "B0": "50000",
            "H": "0",
            "stop_loss_return_rule": "OFF",
            "loss_controls_armed": False,
        },
        "observation_class": "synthetic",
        "notifications_sent": 0,
        "metrics": {
            "wall_seconds": round(time.monotonic() - t_start, 6),
            "peak_rss_bytes": _peak_rss_bytes(),
        },
    }
    if pending_rejected_state_sha256 is not None:
        report["pending_rejected_state_sha256"] = pending_rejected_state_sha256
    report_paths, wrote_report = _write_completeness_report(
        reports_dir,
        report,
        created_at_utc=created_at_utc,
    )
    if wrote_report:
        report["created_at_utc"] = created_at_utc
    res_out = {
        "appended": False,
        "fail_closed": True,
        "reason": reason_key,
        "report": report,
        "report_paths": [str(p) for p in report_paths],
        "report_written": wrote_report,
        "existing_report_preserved": not wrote_report,
        "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
    }
    if pending_rejected_state_sha256 is not None:
        res_out["pending_rejected_state_sha256"] = pending_rejected_state_sha256
    return res_out


def run_daily_drill_cycle(
    *,
    session_date: str,
    data_dir: str | Path,
    config: DrillSchedulerConfig | None = None,
    spec_dir: str | Path | None = None,
    transport: Transport | None = None,
    environ: Mapping[str, str] | None = None,
    fixtures_dir: str | Path | None = None,
    ots_client: OpenTimestampsClient | None = None,
    ots_transport: OtsTransport | None = None,
    wall_clock: Callable[[], datetime] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    treasury_endpoint: str | None = None,
    yahoo_symbol_map: Mapping[str, str] | None = None,
    q_pre_by_inst: Mapping[str, object] | None = None,
    sigma_by_inst: Mapping[str, str] | None = None,
    revision_seq: int = 0,
    allow_unanchored_on_ots_failure: bool = True,
    evaluator: object | None = None,
    authorized_events: dict | list | None = None,
    _post_ledger_commit_hook: Callable[[], None] | None = None,
    _post_record_commit_hook: Callable[[], None] | None = None,
    _post_report_json_hook: Callable[[], None] | None = None,
) -> dict:
    """Execute one complete AI-independent drill cycle for session_date."""
    t_start = time.monotonic()
    cfg = config if config is not None else load_scheduler_config()
    if cfg.mode != "drill" or cfg.clk01_active:
        raise DrillOnlyViolationError("Drill cycle refused non-drill or clk01_active config")
    return _execute_cycle_with_config(
        t_start=t_start,
        cfg=cfg,
        session_date=session_date,
        data_dir=data_dir,
        spec_dir=spec_dir,
        transport=transport,
        environ=environ,
        fixtures_dir=fixtures_dir,
        ots_client=ots_client,
        ots_transport=ots_transport,
        wall_clock=wall_clock,
        clock=clock,
        sleeper=sleeper,
        treasury_endpoint=treasury_endpoint,
        yahoo_symbol_map=yahoo_symbol_map,
        q_pre_by_inst=q_pre_by_inst,
        sigma_by_inst=sigma_by_inst,
        revision_seq=revision_seq,
        allow_unanchored_on_ots_failure=allow_unanchored_on_ots_failure,
        evaluator=evaluator,
        authorized_events=authorized_events,
        _post_ledger_commit_hook=_post_ledger_commit_hook,
        _post_record_commit_hook=_post_record_commit_hook,
        _post_report_json_hook=_post_report_json_hook,
    )


def run_production_cycle(
    *,
    session_date: str,
    data_dir: str | Path,
    config_path: str | Path | None = None,
    config: DrillSchedulerConfig | None = None,
    spec_dir: str | Path | None = None,
    transport: Transport | None = None,
    environ: Mapping[str, str] | None = None,
    fixtures_dir: str | Path | None = None,
    ots_client: OpenTimestampsClient | None = None,
    ots_transport: OtsTransport | None = None,
    wall_clock: Callable[[], datetime] | None = None,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    treasury_endpoint: str | None = None,
    yahoo_symbol_map: Mapping[str, str] | None = None,
    q_pre_by_inst: Mapping[str, object] | None = None,
    sigma_by_inst: Mapping[str, str] | None = None,
    revision_seq: int = 0,
    allow_unanchored_on_ots_failure: bool = False,
    evaluator: object | None = None,
    authorized_events: dict | list | None = None,
    expected_prior_head_sha256: str | None = None,
    expected_pending_state_sha256: str | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    trusted_head_anchor_path: str | Path | None = None,
) -> dict:
    """Execute one CLK-01 production cycle ONLY when OA-SH2-2 lock is explicitly unlocked."""
    t_start = time.monotonic()
    cfg = config if config is not None else load_production_scheduler_config(config_path)
    if cfg.mode != "production":
        raise DrillOnlyViolationError("run_production_cycle requires mode='production'")
    if cfg.production_chain_locked is not False or not cfg.clk01_active:
        raise ProductionChainLockedError(
            "Production chain is locked pending OA-SH2-2 sign-off; 0 production appends permitted"
        )
    if fixtures_dir is not None:
        raise ValueError("production_mode_forbids_offline_fixtures")
    if allow_unanchored_on_ots_failure:
        raise ValueError("Production chain structurally forbids unanchored appends")
    ev = evaluator if evaluator is not None else VendoredFuseEvaluator()
    if is_stub_evaluator(ev):
        raise StubProductionForbiddenError(
            "StubEvaluator is structurally forbidden in production forward-clock mode"
        )
    if (
        getattr(ev, "engine_hash", None) != PINNED_FUSE_ENGINE_HASH
        or getattr(ev, "spec_version", None) != PINNED_FUSE_SPEC_VERSION
    ):
        raise ValueError("Production cycle requires exact pinned FUSE-1 identity")
    return _execute_cycle_with_config(
        t_start=t_start,
        cfg=cfg,
        session_date=session_date,
        data_dir=data_dir,
        spec_dir=spec_dir,
        transport=transport,
        environ=environ,
        fixtures_dir=fixtures_dir,
        ots_client=ots_client,
        ots_transport=ots_transport,
        wall_clock=wall_clock,
        clock=clock,
        sleeper=sleeper,
        treasury_endpoint=treasury_endpoint,
        yahoo_symbol_map=yahoo_symbol_map,
        q_pre_by_inst=q_pre_by_inst,
        sigma_by_inst=sigma_by_inst,
        revision_seq=revision_seq,
        allow_unanchored_on_ots_failure=False,
        evaluator=ev,
        authorized_events=authorized_events,
        expected_prior_head_sha256=expected_prior_head_sha256,
        expected_pending_state_sha256=expected_pending_state_sha256,
        bitcoin_merkle_roots=bitcoin_merkle_roots,
        trusted_head_anchor_path=trusted_head_anchor_path,
        _post_ledger_commit_hook=None,
        _post_record_commit_hook=None,
        _post_report_json_hook=None,
    )


def _execute_cycle_with_config(
    *,
    t_start: float,
    cfg: DrillSchedulerConfig,
    session_date: str,
    data_dir: str | Path,
    spec_dir: str | Path | None,
    transport: Transport | None,
    environ: Mapping[str, str] | None,
    fixtures_dir: str | Path | None,
    ots_client: OpenTimestampsClient | None,
    ots_transport: OtsTransport | None,
    wall_clock: Callable[[], datetime] | None,
    clock: Callable[[], float] | None,
    sleeper: Callable[[float], None] | None,
    treasury_endpoint: str | None,
    yahoo_symbol_map: Mapping[str, str] | None,
    q_pre_by_inst: Mapping[str, object] | None,
    sigma_by_inst: Mapping[str, str] | None,
    revision_seq: int,
    allow_unanchored_on_ots_failure: bool,
    evaluator: object | None,
    authorized_events: dict | list | None = None,
    expected_prior_head_sha256: str | None = None,
    expected_pending_state_sha256: str | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    trusted_head_anchor_path: str | Path | None = None,
    _post_ledger_commit_hook: Callable[[], None] | None,
    _post_record_commit_hook: Callable[[], None] | None,
    _post_report_json_hook: Callable[[], None] | None,
) -> dict:

    root = Path(data_dir)
    ledger_dir = root / "ledger"
    archive_dir = root / "archive"
    reports_dir = root / "reports"
    bundle_dir = root / "replay"
    logs_dir = root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    spec_path = _resolve_spec_dir_for_config(cfg, spec_dir)
    spec = load_spec(spec_path)
    if spec["rules"]["calendar_version"] != cfg.calendar_version:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: configured {cfg.calendar_version} != loaded {spec['rules']['calendar_version']}"
        )
    if cfg.calendar_spec_digest is not None and spec.get("spec_digest") != cfg.calendar_spec_digest:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: configured spec_digest {cfg.calendar_spec_digest} != loaded {spec.get('spec_digest')}"
        )
    if cfg.venue != spec["rules"]["exchange"]:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: unsupported_venue:{cfg.venue} != {spec['rules']['exchange']}"
        )
    cov_start = date.fromisoformat(spec["rules"]["coverage_start_date"])
    cov_end = date.fromisoformat(spec["rules"]["coverage_end_date"])
    sess_d = date.fromisoformat(session_date)
    if sess_d < cov_start or sess_d > cov_end:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: {session_date} outside "
            f"{cfg.calendar_version} coverage [{cov_start.isoformat()}, {cov_end.isoformat()}]"
        )

    clock_fn = wall_clock if wall_clock is not None else (lambda: datetime.now(timezone.utc))
    cal = resolve_session(cfg.venue, session_date, spec=spec)
    if cal.get("session_state") == "unknown":
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: {cal.get('reason')}:{cfg.venue}:{session_date}"
        )

    deadlines = compute_session_deadlines(session_date, cfg, calendar_session=cal)
    ledger = ShadowLedger(ledger_dir)
    with ledger.chain_lock():
        return _run_daily_drill_cycle_locked(
            t_start=t_start,
            cfg=cfg,
            root=root,
            ledger=ledger,
            archive_dir=archive_dir,
            reports_dir=reports_dir,
            bundle_dir=bundle_dir,
            logs_dir=logs_dir,
            spec=spec,
            cal=cal,
            deadlines=deadlines,
            session_date=session_date,
            clock_fn=clock_fn,
            transport=transport,
            environ=environ,
            fixtures_dir=fixtures_dir,
            ots_client=ots_client,
            ots_transport=ots_transport,
            clock=clock,
            sleeper=sleeper,
            treasury_endpoint=treasury_endpoint,
            yahoo_symbol_map=yahoo_symbol_map,
            q_pre_by_inst=q_pre_by_inst,
            sigma_by_inst=sigma_by_inst,
            revision_seq=revision_seq,
            allow_unanchored_on_ots_failure=allow_unanchored_on_ots_failure,
            evaluator=evaluator,
            authorized_events=authorized_events,
            expected_prior_head_sha256=expected_prior_head_sha256,
            expected_pending_state_sha256=expected_pending_state_sha256,
            bitcoin_merkle_roots=bitcoin_merkle_roots,
            trusted_head_anchor_path=trusted_head_anchor_path,
            _post_ledger_commit_hook=_post_ledger_commit_hook,
            _post_record_commit_hook=_post_record_commit_hook,
            _post_report_json_hook=_post_report_json_hook,
        )


def _extract_prior_session_closes(
    *,
    archive_dir: Path,
    mkt_recs: list[dict],
    session_date: str,
    bundle_obj: dict | None,
    cal: Mapping[str, object],
    spec: dict | None = None,
    venue: str = "XNYS",
) -> dict[str, str]:
    """Derive immediately preceding trading day closes from committed bundle or multi-day archives."""
    _ = cal
    loaded_spec = spec if spec is not None else load_spec()
    prev_trading_date = resolve_previous_trading_date(loaded_spec, venue, session_date)
    if prev_trading_date is None:
        return {}

    closes: dict[str, str] = {}
    if isinstance(bundle_obj, dict):
        sessions = bundle_obj.get("sessions")
        if isinstance(sessions, list) and sessions:
            prior_sess = next(
                (
                    cand
                    for cand in reversed(sessions)
                    if isinstance(cand, dict) and str(cand.get("session_date", "")) == prev_trading_date
                ),
                None,
            )
            if prior_sess is not None and isinstance(prior_sess.get("market_records"), list):
                for rec in prior_sess["market_records"]:
                    if (
                        isinstance(rec, dict)
                        and rec.get("instrument") in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI")
                        and rec.get("normalized_value") is not None
                    ):
                        closes[str(rec["instrument"])] = str(rec["normalized_value"])

    cal_prior = resolve_session(venue, prev_trading_date, spec=loaded_spec)
    for rec in mkt_recs:
        inst = str(rec.get("instrument", ""))
        if inst not in ("TQQQ", "SOXL", "QQQ", "XNDX", "SEMI") or inst in closes:
            continue
        ref = rec.get("raw_bytes_ref") or ""
        digest = rec.get("raw_bytes_sha256") or ""
        if not ref or not digest:
            continue
        try:
            raw_bytes = read_verified_archived_bytes(archive_dir, ref, digest)
        except Exception:
            continue
        provider = str(rec.get("provider", ""))
        if inst in ("TQQQ", "SOXL", "QQQ") or "tiingo" in provider:
            val = extract_prior_close_from_tiingo_bytes(
                raw_bytes,
                session_date,
                calendar_session=cal_prior,
                target_prior_date=prev_trading_date,
            )
        else:
            val = extract_prior_close_from_yahoo_bytes(
                raw_bytes,
                session_date,
                calendar_session=cal_prior,
                target_prior_date=prev_trading_date,
            )
        if val is not None:
            closes[inst] = val
    return closes


def _committed_expected_next_state_sha256(record_env: Mapping[str, object]) -> str:
    body = record_env.get("record_body")
    ev_obj = body.get("evaluator") if isinstance(body, Mapping) else None
    nrs_sha = ev_obj.get("next_row_state_sha256") if isinstance(ev_obj, Mapping) else None
    if not isinstance(nrs_sha, str) or not _HEX64_RE.fullmatch(nrs_sha):
        raise EngineIdentityError(
            "Committed non-stub ledger record missing valid evaluator.next_row_state_sha256"
        )
    return nrs_sha


def _retire_superseded_pending_rejected_state(
    snapshots_dir: Path,
    *,
    committed_seq: int,
    committed_session_date: str,
    expected_chain_id: str | None = None,
) -> bool:
    """Remove stale pending_rejected_state.json strictly superseded by an already-committed session."""
    pending_path = snapshots_dir / "pending_rejected_state.json"
    if not pending_path.is_file():
        return False
    try:
        payload = parse_json_no_duplicates(pending_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise EngineIdentityError(f"Corrupt pending rejected state snapshot {pending_path}: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != "c-sh-pending-rejected-state-v1":
        raise EngineIdentityError(f"Invalid pending rejected state snapshot schema in {pending_path}")
    if expected_chain_id is not None and payload.get("chain_id") != expected_chain_id:
        return False
    base_seq = payload.get("base_sequence")
    rej_ds = payload.get("rejected_session_date")
    if (
        isinstance(base_seq, int)
        and not isinstance(base_seq, bool)
        and base_seq < committed_seq
        and isinstance(rej_ds, str)
        and _DATE_YYYY_MM_DD_RE.fullmatch(rej_ds)
        and rej_ds <= committed_session_date
    ):
        pending_path.unlink(missing_ok=True)
        return True
    return False


def _reconcile_committed_state_snapshot_if_missing(
    *,
    snapshots_dir: Path,
    ledger: ShadowLedger,
    bundle_obj: dict | None,
    prior_records: list[dict],
    ex_seq: int,
    existing_env: dict,
    cfg: DrillSchedulerConfig,
    spec: dict,
    cal: dict,
) -> None:
    """Verify or deterministically rebuild seq_{ex_seq}.state.json after mid-commit interruption."""
    ex_body = existing_env["record_body"]
    ex_sess_date = str(ex_body["session_date"])
    ex_hash = ex_body.get("engine_hash")
    ex_spec = (ex_body.get("evaluator") or {}).get("spec_version")
    if ex_hash != PINNED_FUSE_ENGINE_HASH or ex_spec != PINNED_FUSE_SPEC_VERSION:
        _retire_superseded_pending_rejected_state(
            snapshots_dir,
            committed_seq=ex_seq,
            committed_session_date=ex_sess_date,
            expected_chain_id=cfg.chain_id,
        )
        return
    ex_expected_state_sha = _committed_expected_next_state_sha256(existing_env)
    try:
        seq_state, _ = load_row_state_snapshot(
            snapshots_dir,
            expected_chain_id=cfg.chain_id,
            expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
            expected_spec_version=PINNED_FUSE_SPEC_VERSION,
            expected_sequence=ex_seq,
            expected_record_sha256=existing_env["record_sha256"],
            expected_state_sha256=ex_expected_state_sha,
            allow_missing_at_genesis=False,
        )
        if ex_seq == len(prior_records) - 1:
            latest_path = snapshots_dir / "latest.state.json"
            if latest_path.is_file():
                try:
                    latest_obj = parse_json_no_duplicates(latest_path.read_text(encoding="utf-8"))
                    if (
                        isinstance(latest_obj, dict)
                        and latest_obj.get("sequence") == ex_seq
                        and latest_obj.get("record_sha256") == existing_env["record_sha256"]
                    ):
                        load_row_state_snapshot(
                            snapshots_dir,
                            expected_chain_id=cfg.chain_id,
                            expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
                            expected_spec_version=PINNED_FUSE_SPEC_VERSION,
                            expected_sequence=None,
                            expected_record_sha256=existing_env["record_sha256"],
                            expected_state_sha256=ex_expected_state_sha,
                            allow_missing_at_genesis=False,
                        )
                        _retire_superseded_pending_rejected_state(
                            snapshots_dir,
                            committed_seq=ex_seq,
                            committed_session_date=ex_sess_date,
                            expected_chain_id=cfg.chain_id,
                        )
                        return
                except Exception:
                    pass
            save_row_state_snapshot(
                snapshots_dir,
                chain_id=cfg.chain_id,
                sequence=ex_seq,
                session_date=ex_sess_date,
                record_sha256=existing_env["record_sha256"],
                engine_hash=PINNED_FUSE_ENGINE_HASH,
                spec_version=PINNED_FUSE_SPEC_VERSION,
                next_row_state=seq_state,
            )
        _retire_superseded_pending_rejected_state(
            snapshots_dir,
            committed_seq=ex_seq,
            committed_session_date=ex_sess_date,
            expected_chain_id=cfg.chain_id,
        )
        return
    except Exception:
        pass

    if not isinstance(bundle_obj, dict):
        raise ValueError("missing_replay_bundle_for_snapshot_recovery")
    sessions = bundle_obj.get("sessions")
    if not isinstance(sessions, list) or ex_seq >= len(sessions):
        raise ValueError(f"missing_bundle_session_for_snapshot_recovery:seq={ex_seq}")
    sess_entry = sessions[ex_seq]
    fctx = sess_entry.get("fuse_context") if isinstance(sess_entry.get("fuse_context"), Mapping) else {}
    if ex_seq == 0:
        if isinstance(fctx, Mapping) and fctx.get("prior_row_state_used") is not None:
            prior_state = copy.deepcopy(fctx["prior_row_state_used"])
        else:
            prior_state = {}
    else:
        prev_env = prior_records[ex_seq - 1]
        prev_expected_sha = _committed_expected_next_state_sha256(prev_env)
        committed_prev_state, _ = load_row_state_snapshot(
            snapshots_dir,
            expected_chain_id=cfg.chain_id,
            expected_engine_hash=PINNED_FUSE_ENGINE_HASH,
            expected_spec_version=PINNED_FUSE_SPEC_VERSION,
            expected_sequence=ex_seq - 1,
            expected_record_sha256=prev_env["record_sha256"],
            expected_state_sha256=prev_expected_sha,
            allow_missing_at_genesis=False,
        )
        if isinstance(fctx, Mapping) and fctx.get("prior_row_state_used") is not None:
            prior_state = copy.deepcopy(fctx["prior_row_state_used"])
        else:
            prior_state = committed_prev_state

    rec_inputs = dict(sess_entry["inputs"])
    rec_inputs["market_records"] = sess_entry["market_records"]
    rec_inputs["required_series"] = REQUIRED_MARKET_SERIES
    if isinstance(fctx, Mapping):
        for ctx_key in (
            "prior_closes",
            "positions_source_record",
            "broker_reconciliation_source_record",
            "index_one_session_returns",
            "authorized_events",
        ):
            if ctx_key in fctx and fctx[ctx_key] is not None:
                rec_inputs[ctx_key] = fctx[ctx_key]

    ev = VendoredFuseEvaluator()
    ex_cal = cal if str(cal.get("date", "")) == ex_sess_date else resolve_session(cfg.venue, ex_sess_date, spec=spec)
    rebuilt = build_record_body(
        chain_id=cfg.chain_id,
        sequence=ex_seq,
        prev_record_sha256=str(ex_body["prev_record_sha256"]),
        mode=str(ex_body["mode"]),
        session_date=str(ex_body["session_date"]),
        decision_timestamp_utc=str(ex_body["decision_timestamp_utc"]),
        session_inputs=rec_inputs,
        calendar_session=ex_cal,
        code_version_sha=str(ex_body["code_version_sha"]),
        evaluator=ev,
        prior_row_state=prior_state,
        discipline_labels=sess_entry.get("discipline_labels"),
        market_records_digest=sess_entry.get("market_records_digest"),
        runtime_config=bundle_obj.get("runtime_config"),
        calendar_spec=spec,
    )
    if not rebuilt.get("appended") or sha256_canonical(rebuilt["record_body"]) != existing_env["record_sha256"]:
        raise ValueError(f"snapshot_recovery_record_hash_mismatch:seq={ex_seq}")
    save_row_state_snapshot(
        snapshots_dir,
        chain_id=cfg.chain_id,
        sequence=ex_seq,
        session_date=ex_sess_date,
        record_sha256=existing_env["record_sha256"],
        engine_hash=PINNED_FUSE_ENGINE_HASH,
        spec_version=PINNED_FUSE_SPEC_VERSION,
        next_row_state=rebuilt["next_row_state"],
    )
    _retire_superseded_pending_rejected_state(
        snapshots_dir,
        committed_seq=ex_seq,
        committed_session_date=ex_sess_date,
        expected_chain_id=cfg.chain_id,
    )


def _reconcile_uncommitted_staged_snapshots(
    *,
    snapshots_dir: Path,
    ledger: ShadowLedger,
    bundle_obj: dict | None,
    prior_records: list[dict],
    cfg: DrillSchedulerConfig,
    spec: dict,
) -> None:
    """Prune uncommitted staged seq snapshots and restore latest.state.json to committed head."""
    if not snapshots_dir.is_dir():
        return
    committed_len = len(prior_records)
    removed_uncommitted = False
    for cand in sorted(snapshots_dir.iterdir()):
        if not cand.is_file():
            continue
        m = re.fullmatch(r"seq_(\d{6})\.state\.json", cand.name)
        if m is not None and int(m.group(1)) >= committed_len:
            cand.unlink(missing_ok=True)
            removed_uncommitted = True

    latest_path = snapshots_dir / "latest.state.json"
    if not latest_path.is_file():
        return
    latest_obj: dict | None = None
    latest_unparseable = False
    try:
        parsed = parse_json_no_duplicates(latest_path.read_text(encoding="utf-8"))
        if isinstance(parsed, dict):
            latest_obj = parsed
    except Exception:
        latest_unparseable = True

    latest_is_uncommitted = bool(
        latest_obj is not None
        and latest_obj.get("chain_id") == cfg.chain_id
        and isinstance(latest_obj.get("sequence"), int)
        and not isinstance(latest_obj.get("sequence"), bool)
        and latest_obj["sequence"] >= committed_len
    ) or (removed_uncommitted and latest_unparseable)

    if not latest_is_uncommitted:
        return
    if committed_len == 0:
        latest_path.unlink(missing_ok=True)
        return
    head_idx = committed_len - 1
    head_env = prior_records[head_idx]
    head_sess = str(head_env["record_body"]["session_date"])
    head_cal = resolve_session(cfg.venue, head_sess, spec=spec)
    _reconcile_committed_state_snapshot_if_missing(
        snapshots_dir=snapshots_dir,
        ledger=ledger,
        bundle_obj=bundle_obj,
        prior_records=prior_records,
        ex_seq=head_idx,
        existing_env=head_env,
        cfg=cfg,
        spec=spec,
        cal=head_cal,
    )


def _check_no_unconsumed_pending_rejected_reports(
    reports_dir: Path,
    *,
    head_session_date: str | None,
) -> None:
    """Fail closed if a post-head rejected completeness report recorded a pending state now absent."""
    if not reports_dir.is_dir():
        return
    for rpath in sorted(reports_dir.glob("completeness_*.json")):
        try:
            rep = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rep, Mapping):
            continue
        rep_ledger = rep.get("ledger")
        pend_sha = rep.get("pending_rejected_state_sha256")
        if (
            isinstance(rep_ledger, Mapping)
            and rep_ledger.get("appended") is False
            and isinstance(pend_sha, str)
            and pend_sha
        ):
            rep_ds = str(rep.get("session_date") or "")
            if head_session_date is None or rep_ds > head_session_date:
                raise EngineIdentityError(
                    "production_pending_rejected_state_missing: post-head rejected report records pending_rejected_state_sha256 but pending_rejected_state.json is absent"
                )


def _validate_trusted_head_anchor_path(
    anchor_path: str | Path | None,
    data_dir: str | Path,
) -> Path:
    if anchor_path is None or not str(anchor_path).strip():
        raise EngineIdentityError(
            "production_trusted_head_anchor_required: require external trusted_head_anchor_path"
        )
    resolved_anchor = Path(anchor_path).resolve()
    resolved_data = Path(data_dir).resolve()
    try:
        resolved_anchor.relative_to(resolved_data)
        inside_data = True
    except ValueError:
        inside_data = False
    if inside_data:
        raise EngineIdentityError(
            "production_trusted_head_anchor_inside_data_dir: anchor must reside outside SHADOW_DATA_DIR"
        )
    if resolved_anchor.exists() and resolved_anchor.is_dir():
        raise EngineIdentityError("production_trusted_head_anchor_is_directory")
    return resolved_anchor


def _load_trusted_head_anchor(
    anchor_path: Path,
    *,
    expected_chain_id: str,
) -> tuple[dict | None, bytes | None]:
    if not anchor_path.exists():
        return None, None
    try:
        raw_bytes = anchor_path.read_bytes()
        obj = parse_json_no_duplicates(raw_bytes.decode("utf-8"))
    except Exception as exc:
        raise EngineIdentityError(f"invalid_trusted_head_anchor:{exc}") from exc
    if not isinstance(obj, dict):
        raise EngineIdentityError("invalid_trusted_head_anchor:not_dict")
    if obj.get("schema_version") != "c-sh-trusted-head-anchor-v1":
        raise EngineIdentityError("invalid_trusted_head_anchor:schema_version")
    if obj.get("chain_id") != expected_chain_id:
        raise EngineIdentityError("invalid_trusted_head_anchor:chain_id_mismatch")
    seq = obj.get("sequence")
    anchor_ver = obj.get("anchor_version")
    head_sha = obj.get("head_sha256")
    pend_sha = obj.get("pending_rejected_state_sha256")
    if (
        isinstance(seq, bool)
        or not isinstance(seq, int)
        or seq < -1
        or isinstance(anchor_ver, bool)
        or not isinstance(anchor_ver, int)
        or anchor_ver < 1
        or not isinstance(head_sha, str)
        or not _HEX64_RE.fullmatch(head_sha)
        or not isinstance(pend_sha, str)
        or not _HEX64_RE.fullmatch(pend_sha)
    ):
        raise EngineIdentityError("invalid_trusted_head_anchor:malformed_fields")
    if seq == -1 and not hmac.compare_digest(head_sha, GENESIS_PREV_HASH):
        raise EngineIdentityError("invalid_trusted_head_anchor:genesis_seq_head_mismatch")
    pend_ds = obj.get("pending_rejected_session_date")
    if pend_ds is not None:
        if not isinstance(pend_ds, str) or not _DATE_YYYY_MM_DD_RE.fullmatch(pend_ds):
            raise EngineIdentityError("invalid_trusted_head_anchor:malformed_fields")
        try:
            date.fromisoformat(pend_ds)
        except ValueError as exc:
            raise EngineIdentityError("invalid_trusted_head_anchor:malformed_fields") from exc
    return obj, raw_bytes


def _advance_trusted_head_anchor(
    anchor_path: Path,
    *,
    chain_id: str,
    expected_prior_bytes: bytes | None,
    sequence: int,
    session_date: str | None,
    head_sha256: str,
    pending_rejected_state_sha256: str,
    now_utc: str,
    pending_rejected_session_date: str | None = None,
) -> dict:
    anchor_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = anchor_path.with_name(f".{anchor_path.name}.lock")
    lfd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(lfd, fcntl.LOCK_EX)
        try:
            cur_obj, cur_bytes = _load_trusted_head_anchor(
                anchor_path, expected_chain_id=chain_id
            )
            if expected_prior_bytes is None:
                if cur_bytes is not None:
                    raise EngineIdentityError(
                        "production_trusted_head_rollback_rejected: anchor appeared concurrently"
                    )
                next_version = 1
            else:
                if cur_bytes != expected_prior_bytes or cur_obj is None:
                    raise EngineIdentityError(
                        "production_trusted_head_rollback_rejected: anchor modified concurrently"
                    )
                if sequence < int(cur_obj["sequence"]):
                    raise EngineIdentityError(
                        "production_trusted_head_rollback_rejected: anchor sequence regression"
                    )
                next_version = int(cur_obj["anchor_version"]) + 1

            new_obj: dict[str, object] = {
                "schema_version": "c-sh-trusted-head-anchor-v1",
                "chain_id": chain_id,
                "sequence": sequence,
                "session_date": session_date,
                "head_sha256": head_sha256,
                "pending_rejected_state_sha256": pending_rejected_state_sha256,
                "pending_rejected_session_date": pending_rejected_session_date,
                "anchor_version": next_version,
                "updated_at_utc": now_utc,
            }
            new_bytes = (
                json.dumps(new_obj, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode("utf-8")
            tmp_path = anchor_path.with_name(
                f".{anchor_path.name}.tmp.{os.getpid()}.{time.monotonic_ns()}"
            )
            tfd = os.open(str(tmp_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            try:
                os.write(tfd, new_bytes)
                os.fsync(tfd)
            finally:
                os.close(tfd)
            os.replace(tmp_path, anchor_path)
            dfd = os.open(str(anchor_path.parent), os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
            return new_obj
        finally:
            fcntl.flock(lfd, fcntl.LOCK_UN)
    finally:
        os.close(lfd)


def _verify_or_catchup_pending_anchor(
    *,
    anchor_resolved: Path,
    anchor_obj: dict | None,
    anchor_bytes: bytes | None,
    chain_id: str,
    head_seq: int,
    head_ds: str | None,
    head_sha: str,
    session_date: str,
    expected_pending_state_sha256: str | None,
    auth_pending: dict | None,
    now_utc_fn: Callable[[], str] | None,
    rollback_err_reason: str,
) -> tuple[dict | None, bytes | None]:
    """Verify pending snapshot matches trusted anchor, or catch up a forward interrupted pending anchor write."""
    actual_pend_sha = (
        compute_pending_rejected_state_sha256(auth_pending)
        if auth_pending is not None
        else NO_PENDING_REJECTED_STATE_SHA256
    )
    if anchor_obj is None:
        if (
            head_seq == -1
            and auth_pending is not None
            and expected_pending_state_sha256 is not None
            and not hmac.compare_digest(actual_pend_sha, NO_PENDING_REJECTED_STATE_SHA256)
            and hmac.compare_digest(expected_pending_state_sha256, actual_pend_sha)
        ):
            # Genesis pending catch-up is only allowed after strict binding validation below.
            pass
        else:
            return anchor_obj, anchor_bytes
    else:
        exp_anchor_pend = str(anchor_obj["pending_rejected_state_sha256"])
        if hmac.compare_digest(actual_pend_sha, exp_anchor_pend) and (
            expected_pending_state_sha256 is None
            or hmac.compare_digest(expected_pending_state_sha256, exp_anchor_pend)
        ):
            return anchor_obj, anchor_bytes

    rej_ds = auth_pending.get("rejected_session_date") if isinstance(auth_pending, Mapping) else None
    valid_rej_ds = False
    if isinstance(rej_ds, str) and _DATE_YYYY_MM_DD_RE.fullmatch(rej_ds):
        try:
            date.fromisoformat(rej_ds)
            valid_rej_ds = (head_ds is None or rej_ds > head_ds) and rej_ds <= session_date
        except ValueError:
            valid_rej_ds = False

    exp_anchor_pend = (
        str(anchor_obj["pending_rejected_state_sha256"])
        if anchor_obj is not None
        else NO_PENDING_REJECTED_STATE_SHA256
    )
    anchor_pend_ds = anchor_obj.get("pending_rejected_session_date") if anchor_obj is not None else None
    monotonic_forward = (
        anchor_obj is None
        or hmac.compare_digest(exp_anchor_pend, NO_PENDING_REJECTED_STATE_SHA256)
        or (isinstance(anchor_pend_ds, str) and valid_rej_ds and rej_ds > anchor_pend_ds)
    )
    can_catchup = (
        auth_pending is not None
        and valid_rej_ds
        and monotonic_forward
        and not hmac.compare_digest(actual_pend_sha, NO_PENDING_REJECTED_STATE_SHA256)
        and expected_pending_state_sha256 is not None
        and hmac.compare_digest(expected_pending_state_sha256, actual_pend_sha)
        and auth_pending.get("chain_id") == chain_id
        and auth_pending.get("base_sequence") == head_seq
        and not isinstance(auth_pending.get("base_sequence"), bool)
        and isinstance(auth_pending.get("base_record_sha256"), str)
        and hmac.compare_digest(auth_pending["base_record_sha256"], head_sha)
    )
    if not can_catchup:
        raise EngineIdentityError(rollback_err_reason)

    catchup_now = (
        now_utc_fn()
        if now_utc_fn is not None
        else _format_utc_ms(datetime.now(timezone.utc))
    )
    _advance_trusted_head_anchor(
        anchor_resolved,
        chain_id=chain_id,
        expected_prior_bytes=anchor_bytes,
        sequence=head_seq,
        session_date=head_ds,
        head_sha256=head_sha,
        pending_rejected_state_sha256=actual_pend_sha,
        pending_rejected_session_date=str(rej_ds),
        now_utc=catchup_now,
    )
    return _load_trusted_head_anchor(anchor_resolved, expected_chain_id=chain_id)


def _authenticate_production_history(
    *,
    ledger: ShadowLedger,
    cfg: DrillSchedulerConfig,
    prior_records: list[dict],
    session_date: str,
    expected_prior_head_sha256: str | None,
    expected_pending_state_sha256: str | None = None,
    snapshots_dir: Path | None = None,
    reports_dir: Path | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None,
    data_dir: Path | None = None,
    trusted_head_anchor_path: str | Path | None = None,
    now_utc_fn: Callable[[], str] | None = None,
) -> tuple[Path | None, bytes | None]:
    """Fail closed if persisted production history or pending state is unauthenticated before reuse."""
    if cfg.mode != "production":
        return None, None
    anchor_resolved = _validate_trusted_head_anchor_path(
        trusted_head_anchor_path,
        data_dir if data_dir is not None else ledger.root_dir.parent,
    )
    anchor_obj, anchor_bytes = _load_trusted_head_anchor(
        anchor_resolved, expected_chain_id=cfg.chain_id
    )
    if expected_prior_head_sha256 is not None:
        if not isinstance(expected_prior_head_sha256, str) or not _HEX64_RE.fullmatch(
            expected_prior_head_sha256
        ):
            raise EngineIdentityError(
                f"invalid_expected_prior_head_sha256:{expected_prior_head_sha256!r}"
            )
    if not prior_records:
        if expected_prior_head_sha256 is None:
            raise EngineIdentityError(
                "production_genesis_unauthenticated: require expected_prior_head_sha256=GENESIS_PREV_HASH"
            )
        if not hmac.compare_digest(expected_prior_head_sha256, GENESIS_PREV_HASH):
            raise EngineIdentityError("production_trusted_head_mismatch:empty_ledger")
        if anchor_obj is not None:
            if int(anchor_obj["sequence"]) >= 0:
                raise EngineIdentityError(
                    "production_trusted_head_rollback_rejected: empty ledger behind monotonic trusted head anchor"
                )
            if not hmac.compare_digest(str(anchor_obj["head_sha256"]), GENESIS_PREV_HASH):
                raise EngineIdentityError("production_trusted_head_rollback_rejected")
        if snapshots_dir is not None:
            auth_pending = authenticate_pending_rejected_state_snapshot(
                snapshots_dir,
                expected_pending_state_sha256=expected_pending_state_sha256,
                require_external_pending_auth=True,
            )
            anchor_obj, anchor_bytes = _verify_or_catchup_pending_anchor(
                anchor_resolved=anchor_resolved,
                anchor_obj=anchor_obj,
                anchor_bytes=anchor_bytes,
                chain_id=cfg.chain_id,
                head_seq=-1,
                head_ds=None,
                head_sha=GENESIS_PREV_HASH,
                session_date=session_date,
                expected_pending_state_sha256=expected_pending_state_sha256,
                auth_pending=auth_pending,
                now_utc_fn=now_utc_fn,
                rollback_err_reason="production_trusted_head_rollback_rejected: pending state behind trusted anchor",
            )
            if auth_pending is None and reports_dir is not None:
                _check_no_unconsumed_pending_rejected_reports(
                    reports_dir,
                    head_session_date=None,
                )
        return anchor_resolved, anchor_bytes

    if anchor_obj is None:
        raise EngineIdentityError(
            "production_trusted_head_anchor_missing: non-empty production ledger requires existing trusted head anchor"
        )
    if expected_prior_head_sha256 is None:
        raise EngineIdentityError(
            "production_history_unauthenticated: require expected_prior_head_sha256"
        )

    existing_env = next(
        (
            r
            for r in prior_records
            if isinstance(r, dict)
            and isinstance(r.get("record_body"), dict)
            and r["record_body"].get("session_date") == session_date
        ),
        None,
    )
    actual_seq = int(prior_records[-1]["record_body"]["sequence"])
    head_rec_sha = prior_records[-1]["record_sha256"]
    if existing_env is not None and existing_env["record_body"]["sequence"] != len(prior_records) - 1:
        raise EngineIdentityError("production_trusted_head_mismatch")
    if actual_seq < int(anchor_obj["sequence"]):
        raise EngineIdentityError(
            f"production_trusted_head_rollback_rejected: ledger sequence {actual_seq} < trusted anchor sequence {anchor_obj['sequence']}"
        )
    anchor_seq = int(anchor_obj["sequence"])
    pend_file = snapshots_dir / "pending_rejected_state.json" if snapshots_dir is not None else None
    catchup_body = (
        prior_records[-1].get("record_body")
        if prior_records and isinstance(prior_records[-1], Mapping)
        else None
    )
    catchup_ds = str(catchup_body.get("session_date") or "") if isinstance(catchup_body, Mapping) else ""
    superseded_pending_ok = False
    if pend_file is not None and pend_file.is_file() and catchup_ds:
        try:
            p_raw = parse_json_no_duplicates(pend_file.read_text(encoding="utf-8"))
            superseded_pending_ok = (
                isinstance(p_raw, Mapping)
                and p_raw.get("schema_version") == "c-sh-pending-rejected-state-v1"
                and p_raw.get("chain_id") == cfg.chain_id
                and isinstance(p_raw.get("base_sequence"), int)
                and not isinstance(p_raw.get("base_sequence"), bool)
                and p_raw["base_sequence"] < actual_seq
                and isinstance(p_raw.get("rejected_session_date"), str)
                and p_raw["rejected_session_date"] <= catchup_ds
            )
        except Exception:
            superseded_pending_ok = False
    can_catchup_interrupted_commit = (
        actual_seq == anchor_seq + 1
        and isinstance(head_rec_sha, str)
        and hmac.compare_digest(head_rec_sha, expected_prior_head_sha256)
        and isinstance(catchup_body, Mapping)
        and bool(catchup_ds)
        and hmac.compare_digest(
            str(catchup_body.get("prev_record_sha256") or ""),
            str(anchor_obj["head_sha256"]),
        )
        and (
            expected_pending_state_sha256 is None
            or hmac.compare_digest(expected_pending_state_sha256, NO_PENDING_REJECTED_STATE_SHA256)
        )
        and (pend_file is None or not pend_file.is_file() or superseded_pending_ok)
    )
    if can_catchup_interrupted_commit:
        if bitcoin_merkle_roots is not None:
            ok_credit, credit_issues = ledger.assess_receipt_creditability(
                bitcoin_merkle_roots=bitcoin_merkle_roots,
                commitment_deadline_et=cfg.commitment_deadline_et,
                runtime_config=cfg,
            )
            if not ok_credit:
                raise EngineIdentityError(f"production_history_uncreditable:{credit_issues}")
        if superseded_pending_ok and snapshots_dir is not None:
            _retire_superseded_pending_rejected_state(
                snapshots_dir,
                committed_seq=actual_seq,
                committed_session_date=catchup_ds,
                expected_chain_id=cfg.chain_id,
            )
        catchup_now = (
            now_utc_fn()
            if now_utc_fn is not None
            else _format_utc_ms(datetime.now(timezone.utc))
        )
        _advance_trusted_head_anchor(
            anchor_resolved,
            chain_id=cfg.chain_id,
            expected_prior_bytes=anchor_bytes,
            sequence=actual_seq,
            session_date=catchup_ds,
            head_sha256=head_rec_sha,
            pending_rejected_state_sha256=NO_PENDING_REJECTED_STATE_SHA256,
            now_utc=catchup_now,
        )
        anchor_obj, anchor_bytes = _load_trusted_head_anchor(
            anchor_resolved, expected_chain_id=cfg.chain_id
        )
    if (
        anchor_obj is None
        or actual_seq != int(anchor_obj["sequence"])
        or not isinstance(head_rec_sha, str)
        or not hmac.compare_digest(head_rec_sha, str(anchor_obj["head_sha256"]))
    ):
        raise EngineIdentityError("production_trusted_head_mismatch:diverges_from_anchor")
    if not hmac.compare_digest(head_rec_sha, expected_prior_head_sha256):
        raise EngineIdentityError("production_trusted_head_mismatch")

    if snapshots_dir is not None:
        if existing_env is None:
            auth_pending = authenticate_pending_rejected_state_snapshot(
                snapshots_dir,
                expected_pending_state_sha256=expected_pending_state_sha256,
                require_external_pending_auth=True,
            )
            head_body = prior_records[-1].get("record_body") if isinstance(prior_records[-1], Mapping) else None
            head_ds = str(head_body.get("session_date")) if isinstance(head_body, Mapping) and head_body.get("session_date") else None
            anchor_obj, anchor_bytes = _verify_or_catchup_pending_anchor(
                anchor_resolved=anchor_resolved,
                anchor_obj=anchor_obj,
                anchor_bytes=anchor_bytes,
                chain_id=cfg.chain_id,
                head_seq=actual_seq,
                head_ds=head_ds,
                head_sha=str(head_rec_sha),
                session_date=session_date,
                expected_pending_state_sha256=expected_pending_state_sha256,
                auth_pending=auth_pending,
                now_utc_fn=now_utc_fn,
                rollback_err_reason="production_trusted_head_rollback_rejected: pending state diverged from trusted anchor",
            )
            if auth_pending is None and reports_dir is not None:
                _check_no_unconsumed_pending_rejected_reports(
                    reports_dir,
                    head_session_date=head_ds,
                )
        elif not hmac.compare_digest(
            str(anchor_obj["pending_rejected_state_sha256"]),
            NO_PENDING_REJECTED_STATE_SHA256,
        ):
            raise EngineIdentityError(
                "production_trusted_head_rollback_rejected: unexpected pending digest on committed anchor"
            )

    if bitcoin_merkle_roots is not None:
        ok_credit, credit_issues = ledger.assess_receipt_creditability(
            bitcoin_merkle_roots=bitcoin_merkle_roots,
            commitment_deadline_et=cfg.commitment_deadline_et,
            runtime_config=cfg,
        )
        if not ok_credit:
            raise EngineIdentityError(f"production_history_uncreditable:{credit_issues}")

    return anchor_resolved, anchor_bytes


def _run_daily_drill_cycle_locked(
    *,
    t_start: float,
    cfg: DrillSchedulerConfig,
    root: Path,
    ledger: ShadowLedger,
    archive_dir: Path,
    reports_dir: Path,
    bundle_dir: Path,
    logs_dir: Path,
    spec: dict,
    cal: dict,
    deadlines: dict[str, str],
    session_date: str,
    clock_fn: Callable[[], datetime],
    transport: Transport | None,
    environ: Mapping[str, str] | None,
    fixtures_dir: str | Path | None,
    ots_client: OpenTimestampsClient | None,
    ots_transport: OtsTransport | None,
    clock: Callable[[], float] | None,
    sleeper: Callable[[float], None] | None,
    treasury_endpoint: str | None,
    yahoo_symbol_map: Mapping[str, str] | None,
    q_pre_by_inst: Mapping[str, object] | None,
    sigma_by_inst: Mapping[str, str] | None,
    revision_seq: int,
    allow_unanchored_on_ots_failure: bool,
    evaluator: object | None,
    authorized_events: dict | list | None = None,
    expected_prior_head_sha256: str | None = None,
    expected_pending_state_sha256: str | None = None,
    bitcoin_merkle_roots: Mapping[int, object] | None = None,
    trusted_head_anchor_path: str | Path | None = None,
    _post_ledger_commit_hook: Callable[[], None] | None,
    _post_record_commit_hook: Callable[[], None] | None,
    _post_report_json_hook: Callable[[], None] | None,
) -> dict:
    session_close_utc = deadlines["session_close_utc"]
    decision_cutoff_utc = deadlines["decision_cutoff_utc"]
    deadline_utc = deadlines["commitment_deadline_utc"]
    now_utc_ms = _format_utc_ms(clock_fn())

    # 1. Pre-close guard (trading sessions only) MUST run before writing completeness reports
    if cal.get("is_trading_day") and not cal.get("blocks_eligibility") and now_utc_ms < session_close_utc:
        fail_reason = "session_not_yet_closed"
        return {
            "appended": False,
            "fail_closed": True,
            "reason": fail_reason,
            "detail": f"{fail_reason}: now_utc={now_utc_ms} < session_close_utc={session_close_utc}",
            "session_date": session_date,
            "session_close_utc": session_close_utc,
            "decision_utc": now_utc_ms,
            "report": None,
            "report_paths": [],
            "report_written": False,
            "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists(),
            "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
        }

    # 2. Verify raw existing ledger BEFORE parsing head()/load_records() or counting streak
    ok_chain, bad_seq, chain_err = ledger.verify_chain()
    if not ok_chain:
        return _build_fail_closed_result(
            t_start=t_start,
            root=root,
            reports_dir=reports_dir,
            cfg=cfg,
            cal=cal,
            session_date=session_date,
            decision_utc=now_utc_ms,
            deadlines=deadlines,
            head_hash=GENESIS_PREV_HASH,
            consecutive_count=0,
            created_at_utc=now_utc_ms,
            reason_key="ledger_integrity",
            fail_reason=(
                f"existing_chain_verification_failed:{chain_err}"
                + (f":seq={bad_seq}" if bad_seq is not None else "")
            ),
            required_series=[],
            receipt_reasons=["existing_chain_verification_failed", "drill_mode"],
        )

    head_env = ledger.head()
    head_hash = head_env["record_sha256"] if head_env is not None else GENESIS_PREV_HASH
    prior_records = ledger.load_records()
    anchor_resolved, anchor_bytes = _authenticate_production_history(
        ledger=ledger,
        cfg=cfg,
        prior_records=prior_records,
        session_date=session_date,
        expected_prior_head_sha256=expected_prior_head_sha256,
        expected_pending_state_sha256=expected_pending_state_sha256,
        snapshots_dir=root / "state_snapshots",
        reports_dir=reports_dir,
        bitcoin_merkle_roots=bitcoin_merkle_roots,
        data_dir=root,
        trusted_head_anchor_path=trusted_head_anchor_path,
        now_utc_fn=lambda: _format_utc_ms(clock_fn()),
    )
    prior_streak = _count_consecutive_trading_sessions(
        prior_records,
        session_date,
        spec,
        cfg.venue,
        current_appended=False,
    )

    # 3. Non-trading day or calendar blocks eligibility -> O2 fail-closed, 0 appends
    if not cal.get("is_trading_day") or cal.get("blocks_eligibility"):
        return _build_fail_closed_result(
            t_start=t_start,
            root=root,
            reports_dir=reports_dir,
            cfg=cfg,
            cal=cal,
            session_date=session_date,
            decision_utc=now_utc_ms,
            deadlines=deadlines,
            head_hash=head_hash,
            consecutive_count=prior_streak,
            created_at_utc=now_utc_ms,
            reason_key="O2",
            fail_reason=f"O2:{cal.get('reason') or 'non_trading_session'}",
            required_series=[],
            receipt_status="not_applicable_closed_session",
            receipt_reasons=["non_trading_session", "drill_mode"],
        )

    # 3a. Enforce ledger <-> replay_bundle consistency and committed-receipt integrity before any retry or append
    bundle_obj_validated: dict | None = None
    if prior_records:
        bundle_ok, bundle_err, bundle_obj_validated = _validate_replay_bundle_matches_ledger(
            bundle_dir,
            prior_records,
            cfg=cfg,
            spec=spec,
            allow_trailing_uncommitted_tail=True,
            prune_uncommitted_tail=(now_utc_ms <= deadline_utc),
            ledger=ledger,
            reports_dir=reports_dir,
        )
        if not bundle_ok:
            bundle_reason = (
                "incomplete_missing_replay_bundle"
                if bundle_err == "missing_replay_bundle_for_non_empty_ledger"
                else "incomplete_or_corrupt_replay_bundle"
            )
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": bundle_reason,
                "detail": str(bundle_err),
                "session_date": session_date,
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }
        det_ok, det_reason = has_committed_detached_evidence(ledger, len(prior_records))
        if det_ok:
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "committed_evidence_without_ledger_records",
                "detail": str(det_reason or f"Committed detached evidence exists for seq={len(prior_records)}"),
                "session_date": session_date,
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists()
                or (reports_dir / f"completeness_{session_date}.md").exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }
    else:
        prior_committed_artifacts = False
        if reports_dir.is_dir():
            for rep_candidate in sorted(reports_dir.glob("completeness_*.json")):
                stem_ds = rep_candidate.stem.removeprefix("completeness_")
                has_ev, _ = _report_has_committed_evidence(reports_dir, stem_ds, expected_seq=0)
                if has_ev:
                    prior_committed_artifacts = True
                    break
            if not prior_committed_artifacts:
                for md_candidate in sorted(reports_dir.glob("completeness_*.md")):
                    stem_ds = md_candidate.stem.removeprefix("completeness_")
                    has_ev, _ = _report_has_committed_evidence(reports_dir, stem_ds, expected_seq=0)
                    if has_ev:
                        prior_committed_artifacts = True
                        break
        if not prior_committed_artifacts:
            has_seq0_ev, _ = has_committed_detached_evidence(ledger, 0)
            if has_seq0_ev:
                prior_committed_artifacts = True
        bundle_path = bundle_dir / "replay_bundle.json"
        if not prior_committed_artifacts and bundle_path.is_file():
            try:
                b0 = parse_json_no_duplicates(bundle_path.read_text(encoding="utf-8"))
                b0_sessions = b0.get("sessions") if isinstance(b0, dict) else None
                if not isinstance(b0_sessions, list):
                    prior_committed_artifacts = True
                elif len(b0_sessions) > 1:
                    prior_committed_artifacts = True
                elif len(b0_sessions) == 1:
                    s0 = b0_sessions[0]
                    if not isinstance(s0, dict) or s0.get("session_date") != session_date:
                        prior_committed_artifacts = True
            except Exception:
                prior_committed_artifacts = True
        if prior_committed_artifacts:
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "committed_evidence_without_ledger_records",
                "detail": "Existing replay_bundle or committed report found when ledger has 0 records; refusing seq=0 overwrite",
                "session_date": session_date,
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists()
                or (reports_dir / f"completeness_{session_date}.md").exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

    _reconcile_uncommitted_staged_snapshots(
        snapshots_dir=root / "state_snapshots",
        ledger=ledger,
        bundle_obj=bundle_obj_validated,
        prior_records=prior_records,
        cfg=cfg,
        spec=spec,
    )

    # 3b. Reconcile already-committed session if completeness report is missing, invalid, or out-of-sync with ledger+bundle
    existing_env = next(
        (
            r
            for r in prior_records
            if isinstance(r, dict)
            and isinstance(r.get("record_body"), dict)
            and r["record_body"].get("session_date") == session_date
        ),
        None,
    )
    if existing_env is not None:
        ex_body = existing_env["record_body"]
        ex_seq = ex_body["sequence"]
        json_path = reports_dir / f"completeness_{session_date}.json"
        md_path = reports_dir / f"completeness_{session_date}.md"

        recovered_mkt_records: list[dict] | None = None
        if isinstance(bundle_obj_validated, dict):
            sess_entry = bundle_obj_validated["sessions"][ex_seq]
            mkt_list = sess_entry.get("market_records")
            if isinstance(mkt_list, list) and _market_records_complete(mkt_list):
                recovered_mkt_records = mkt_list

        if recovered_mkt_records is None:
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "incomplete_missing_replay_bundle",
                "detail": (
                    f"Committed session {session_date} cannot reconcile completeness report without "
                    "valid 7-series market_records in replay_bundle.json"
                ),
                "sequence": ex_seq,
                "record_sha256": existing_env["record_sha256"],
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": json_path.exists() or md_path.exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

        consecutive_now = _count_consecutive_trading_sessions(
            [r for r in prior_records if r["record_body"]["sequence"] < ex_seq],
            session_date,
            spec,
            cfg.venue,
            current_appended=True,
        )

        _reconcile_committed_state_snapshot_if_missing(
            snapshots_dir=root / "state_snapshots",
            ledger=ledger,
            bundle_obj=bundle_obj_validated,
            prior_records=prior_records,
            ex_seq=ex_seq,
            existing_env=existing_env,
            cfg=cfg,
            spec=spec,
            cal=cal,
        )

        if _is_committed_session_report_valid(
            reports_dir,
            session_date,
            existing_env,
            committed_mkt_records=recovered_mkt_records,
            deadlines=deadlines,
            cfg=cfg,
            cal=cal,
            ledger=ledger,
            expected_consecutive_count=consecutive_now,
        ):
            existing_rep = parse_json_no_duplicates(json_path.read_text(encoding="utf-8"))
            return {
                "appended": False,
                "fail_closed": False,
                "reason": "already_committed_session_preserved",
                "sequence": ex_seq,
                "record_sha256": existing_env["record_sha256"],
                "report": existing_rep,
                "report_paths": [str(json_path), str(md_path)],
                "report_written": False,
                "existing_report_preserved": True,
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

        cfg_mismatch_err = _bundle_deadline_config_mismatch(bundle_obj_validated, cfg)
        if cfg_mismatch_err is not None:
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "committed_bundle_deadline_config_mismatch",
                "detail": cfg_mismatch_err,
                "sequence": ex_seq,
                "record_sha256": existing_env["record_sha256"],
                "report": None,
                "report_paths": [p for p in (str(json_path), str(md_path)) if Path(p).is_file()],
                "report_written": False,
                "existing_report_preserved": json_path.exists() or md_path.exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

        ex_decision_utc = ex_body.get("decision_timestamp_utc")
        if (
            not is_utc_ms_timestamp(ex_decision_utc)
            or ex_decision_utc < session_close_utc
            or ex_decision_utc > decision_cutoff_utc
            or ex_decision_utc > deadline_utc
        ):
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "committed_session_deadline_config_conflict",
                "detail": (
                    f"Committed session {session_date} decision {ex_decision_utc} falls outside "
                    f"configured window [{session_close_utc}, cutoff={decision_cutoff_utc}, deadline={deadline_utc}]"
                ),
                "sequence": ex_seq,
                "record_sha256": existing_env["record_sha256"],
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": json_path.exists() or md_path.exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

        if json_path.is_file():
            try:
                prior_rep_obj = parse_json_no_duplicates(json_path.read_text(encoding="utf-8"))
            except Exception:
                prior_rep_obj = None
            if isinstance(prior_rep_obj, dict):
                prior_lmeta = prior_rep_obj.get("ledger")
                prior_cutoff = prior_rep_obj.get("decision_cutoff_utc")
                prior_deadline = prior_rep_obj.get("commitment_deadline_utc")
                if (
                    isinstance(prior_lmeta, dict)
                    and prior_lmeta.get("appended") is True
                    and prior_lmeta.get("chain_head_digest") == existing_env["record_sha256"]
                    and (
                        (is_utc_ms_timestamp(prior_cutoff) and prior_cutoff != decision_cutoff_utc)
                        or (is_utc_ms_timestamp(prior_deadline) and prior_deadline != deadline_utc)
                    )
                ):
                    return {
                        "appended": False,
                        "recovered_missing_report": False,
                        "fail_closed": True,
                        "reason": "committed_report_config_mismatch",
                        "detail": (
                            f"Committed report for {session_date} was written under cutoff={prior_cutoff} "
                            f"deadline={prior_deadline}, conflicting with current config cutoff={decision_cutoff_utc} "
                            f"deadline={deadline_utc}"
                        ),
                        "sequence": ex_seq,
                        "record_sha256": existing_env["record_sha256"],
                        "report": prior_rep_obj,
                        "report_paths": [str(json_path), str(md_path)] if md_path.is_file() else [str(json_path)],
                        "report_written": False,
                        "existing_report_preserved": True,
                        "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
                    }

        recovered_series = _summarize_required_series(recovered_mkt_records)
        receipt_summary = _summarize_committed_receipt(
            ledger=ledger,
            seq=ex_seq,
            env=existing_env,
            deadline_utc=deadline_utc,
            decision_utc=ex_body["decision_timestamp_utc"],
        )
        report, footprint = _build_committed_completeness_report(
            t_start=t_start,
            root=root,
            logs_dir=logs_dir,
            cfg=cfg,
            cal=cal,
            deadlines=deadlines,
            session_date=session_date,
            decision_utc=ex_body["decision_timestamp_utc"],
            series_summary=recovered_series,
            seq=ex_seq,
            record_sha256=existing_env["record_sha256"],
            receipt_summary=receipt_summary,
            consecutive_now=consecutive_now,
            recovered_missing_report=True,
        )
        report_created_utc = _format_utc_ms(clock_fn())
        wrote_report = False
        report_paths: tuple[Path, Path] = (json_path, md_path)
        write_error: str | None = None
        if report_created_utc > deadline_utc:
            write_error = "late_after_commitment_deadline_backfill_rejected"
        else:
            try:
                report_paths, wrote_report = _write_completeness_report(
                    reports_dir,
                    report,
                    created_at_utc=report_created_utc,
                    post_json_write_hook=_post_report_json_hook,
                    allow_committed_recovery=True,
                )
            except OSError as exc:
                if _post_report_json_hook is not None:
                    raise
                wrote_report = False
                write_error = f"completeness_report_write_error:{exc}"

        verified_after_write = bool(
            wrote_report
            and _is_committed_session_report_valid(
                reports_dir,
                session_date,
                existing_env,
                committed_mkt_records=recovered_mkt_records,
                deadlines=deadlines,
                cfg=cfg,
                cal=cal,
                ledger=ledger,
                expected_consecutive_count=consecutive_now,
            )
        )
        if not verified_after_write:
            fail_reason = write_error or "incomplete_missing_completeness_report"
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": fail_reason,
                "detail": (
                    f"Committed session {session_date} failed completeness report regeneration/verification"
                ),
                "sequence": ex_seq,
                "record_sha256": existing_env["record_sha256"],
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": json_path.exists() or md_path.exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }

        report["created_at_utc"] = report_created_utc
        return {
            "appended": False,
            "recovered_missing_report": True,
            "fail_closed": False,
            "sequence": ex_seq,
            "record_sha256": existing_env["record_sha256"],
            "report": report,
            "report_paths": [str(p) for p in report_paths],
            "report_written": True,
            "existing_report_preserved": False,
            "footprint": footprint,
        }

    # 3c. Post-commitment-deadline backfill guard (after committed-session/bundle checks so committed sessions never write appended=False reports)
    if now_utc_ms > deadline_utc:
        orphan_err = _detect_orphaned_committed_evidence_for_session(
            root, ledger, prior_records, reports_dir, session_date
        )
        if orphan_err is not None:
            return {
                "appended": False,
                "recovered_missing_report": False,
                "fail_closed": True,
                "reason": "committed_evidence_without_ledger_records",
                "detail": orphan_err,
                "session_date": session_date,
                "report": None,
                "report_paths": [],
                "report_written": False,
                "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists()
                or (reports_dir / f"completeness_{session_date}.md").exists(),
                "footprint": check_footprint_budget(root, cfg.max_footprint_bytes),
            }
        fail_reason = "late_after_commitment_deadline_backfill_rejected"
        return _build_fail_closed_result(
            t_start=t_start,
            root=root,
            reports_dir=reports_dir,
            cfg=cfg,
            cal=cal,
            session_date=session_date,
            decision_utc=now_utc_ms,
            deadlines=deadlines,
            head_hash=head_hash,
            consecutive_count=prior_streak,
            created_at_utc=now_utc_ms,
            reason_key=fail_reason,
            fail_reason=fail_reason,
            required_series=[],
            receipt_status="late_backfill_rejected",
            receipt_reasons=[fail_reason, "drill_mode"],
        )

    eff_treasury = treasury_endpoint if treasury_endpoint is not None else cfg.treasury_endpoint
    eff_yahoo_map = (
        dict(yahoo_symbol_map)
        if yahoo_symbol_map is not None
        else dict(cfg.yahoo_symbol_map or DEFAULT_YAHOO_SYMBOL_MAP)
    )

    # Step 1 & 2: Fetch canonical 7-series (with per-provider timestamps) inside [session_close_utc, decision_cutoff_utc]
    required_for_eval = REQUIRED_MARKET_SERIES
    if cfg.mode == "production" and fixtures_dir is not None:
        raise ValueError("production_mode_forbids_offline_fixtures")
    if fixtures_dir is not None:
        fixture_ret_utc = _format_utc_ms(clock_fn())
        mkt_recs = emit_market_records_from_fixtures(
            fixture_dir=fixtures_dir,
            session_date=session_date,
            retrieval_time_utc=fixture_ret_utc,
            archive_dir=archive_dir,
            calendar_spec=spec,
            revision_seq=revision_seq,
            series=LIVE_SERIES,
            session_close_utc=session_close_utc,
            decision_cutoff_utc=decision_cutoff_utc,
            auto_advance_revision=True,
        )
    else:
        mkt_recs = emit_live_market_records(
            session_date=session_date,
            archive_dir=archive_dir,
            transport=transport,
            environ=environ,
            clock=clock,
            sleeper=sleeper,
            utc_clock=clock_fn,
            calendar_spec=spec,
            revision_seq=revision_seq,
            treasury_endpoint=eff_treasury,
            yahoo_symbol_map=eff_yahoo_map,
            session_close_utc=session_close_utc,
            decision_cutoff_utc=decision_cutoff_utc,
            observation_class="decision_time_market" if cfg.mode == "production" else "synthetic",
            auto_advance_revision=True,
        )

    raw_integrity_failures: list[str] = []
    for rec in mkt_recs:
        ok_raw, raw_err = verify_market_record_raw_integrity(
            archive_dir, rec, session_date=session_date, calendar_session=cal
        )
        if not ok_raw:
            raw_integrity_failures.append(str(raw_err))

    by_inst = {r["instrument"]: r for r in mkt_recs}
    series_summary = _summarize_required_series(mkt_recs)

    # Step 3: Build notional inputs & evaluate C-FUSE-EVAL (stub or vendored FUSE-1)
    q_pre_map = dict(q_pre_by_inst or {})
    sig_map = dict(sigma_by_inst or {})
    tqqq_close = by_inst.get("TQQQ", {}).get("normalized_value")
    soxl_close = by_inst.get("SOXL", {}).get("normalized_value")

    session_inputs: dict[str, object] = {
        "E": "50000",
        "E_star": "50000",
        "H": "0",
        "L": "0",
        "funds": {
            "TQQQ": {
                "s": "1",
                "close_price": tqqq_close,
                "q_pre": q_pre_map.get("TQQQ", 0),
                "sigma_forecast": sig_map.get("TQQQ", "0.24"),
            },
            "SOXL": {
                "s": "1",
                "close_price": soxl_close,
                "q_pre": q_pre_map.get("SOXL", 0),
                "sigma_forecast": sig_map.get("SOXL", "0.24"),
            },
        },
    }
    if head_env is not None:
        prev_bankroll = (
            head_env.get("record_body", {}).get("bankroll", {})
            if isinstance(head_env, Mapping)
            else {}
        )
        if isinstance(prev_bankroll, Mapping) and prev_bankroll.get("E") is not None:
            session_inputs["prior_E"] = str(prev_bankroll["E"])
    eval_inputs = dict(session_inputs)
    eval_inputs["market_records"] = mkt_recs
    eval_inputs["required_series"] = required_for_eval

    if evaluator is not None:
        ev = evaluator
    elif cfg.mode == "production" or cfg.fuse_engine_hash == PINNED_FUSE_ENGINE_HASH:
        ev = VendoredFuseEvaluator()
    else:
        ev = StubEvaluator()

    if cfg.mode == "production" and is_stub_evaluator(ev):
        raise StubProductionForbiddenError(
            "StubEvaluator is structurally forbidden in production forward-clock mode"
        )

    eff_cfg = _effective_runtime_config(
        cfg,
        treasury_endpoint=eff_treasury,
        yahoo_symbol_map=eff_yahoo_map,
    )
    code_sha = compute_code_manifest_hash(REPO_ROOT, calendar_version=cfg.calendar_version)
    p_hash = policy_config_hash(
        runtime_config=eff_cfg,
        calendar_spec=spec,
        code_manifest_hash=code_sha,
    )
    seq = len(prior_records)
    snapshots_dir = root / "state_snapshots"
    prior_state: dict = {}
    prior_row_state_used: dict | None = None
    session_base_prior_state: dict = {}
    prior_o4_resolved_symbols: list[str] = []
    prior_authorized_events: list[dict] = []
    if not is_stub_evaluator(ev):
        committed_base_state: dict = {}
        expected_base_prior_sha = _sha256_canonical_state_dict({})
        if head_env is not None:
            expected_base_prior_sha = _committed_expected_next_state_sha256(head_env)
            committed_base_state, _ = load_row_state_snapshot(
                snapshots_dir,
                expected_chain_id=cfg.chain_id,
                expected_engine_hash=ev.engine_hash,
                expected_spec_version=ev.spec_version,
                expected_sequence=seq - 1,
                expected_record_sha256=head_hash,
                expected_state_sha256=expected_base_prior_sha,
                allow_missing_at_genesis=False,
            )
            if (snapshots_dir / "latest.state.json").is_file():
                load_row_state_snapshot(
                    snapshots_dir,
                    expected_chain_id=cfg.chain_id,
                    expected_engine_hash=ev.engine_hash,
                    expected_spec_version=ev.spec_version,
                    expected_sequence=None,
                    expected_record_sha256=head_hash,
                    expected_state_sha256=expected_base_prior_sha,
                    allow_missing_at_genesis=False,
                )
        expected_base_sess_date = (
            str(head_env["record_body"]["session_date"]) if head_env is not None else None
        )
        pending_rej = load_pending_rejected_state_snapshot(
            snapshots_dir,
            expected_chain_id=cfg.chain_id,
            expected_base_sequence=seq - 1,
            expected_base_record_sha256=head_hash,
            current_session_date=session_date,
            expected_engine_hash=ev.engine_hash,
            expected_spec_version=ev.spec_version,
            expected_base_prior_state_sha256=expected_base_prior_sha,
            committed_base_prior_row_state=committed_base_state,
            expected_base_session_date=expected_base_sess_date,
            expected_pending_state_sha256=expected_pending_state_sha256,
            require_external_pending_auth=(cfg.mode == "production"),
        )
        if pending_rej is not None:
            prior_state = pending_rej[0]
            prior_o4_resolved_symbols = [
                str(s) for s in (pending_rej[1].get("o4_resolved_symbols") or ())
                if isinstance(s, str) and s
            ]
            prior_authorized_events = normalize_pending_authorized_events(
                pending_rej[1].get("authorized_events")
            )
            prior_row_state_used = copy.deepcopy(prior_state)
            session_base_prior_state = (
                copy.deepcopy(committed_base_state) if head_env is not None else {}
            )
        elif head_env is not None:
            prior_state = committed_base_state
            session_base_prior_state = copy.deepcopy(committed_base_state)
    elif head_env is not None:
        breakers = head_env["record_body"]["evaluator"]["breakers"]
        prior_state = {
            b["id"]: {"state": b["state"], "latched": b["latched"], "reason": b["reason"]}
            for b in breakers
        }

    decision_utc = _format_utc_ms(clock_fn())
    prior_closes: dict[str, str] = {}
    pos_src: dict[str, object] | None = None
    brk_src: dict[str, object] | None = None
    if not is_stub_evaluator(ev):
        prior_closes = _extract_prior_session_closes(
            archive_dir=archive_dir,
            mkt_recs=mkt_recs,
            session_date=session_date,
            bundle_obj=bundle_obj_validated,
            cal=cal,
            spec=spec,
            venue=cfg.venue,
        )
        if prior_closes:
            eval_inputs["prior_closes"] = prior_closes
        # Explicit Stage-N notional source records when operating without live broker execution (H='0', orders_placed=0)
        pos_src = {"as_of": decision_utc, "age_sessions": 0}
        brk_src = {"as_of": decision_utc, "age_sessions": 0}
        eval_inputs["positions_source_record"] = pos_src
        eval_inputs["broker_reconciliation_source_record"] = brk_src

    if raw_integrity_failures:
        built = {
            "appended": False,
            "fail_closed": True,
            "o1_output": {
                "state": "NO-NEW-RISK",
                "would_have_tripped": True,
                "reason": "; ".join(raw_integrity_failures),
            },
        }
    elif decision_utc > decision_cutoff_utc:
        built = {
            "appended": False,
            "fail_closed": True,
            "o1_output": {
                "state": "NO-NEW-RISK",
                "would_have_tripped": True,
                "reason": "late_after_decision_cutoff",
            },
        }
    else:
        if is_stub_evaluator(ev):
            eval_inputs["calendar"] = cal
            ev_validated = eval_inputs
            ev_mode = cfg.mode
            rows_by_id, eff_scope, next_state = ev.evaluate(
                spec_version=ev.spec_version,
                engine_hash=ev.engine_hash,
                policy_hash=p_hash,
                session=session_date,
                as_of=decision_utc,
                mode=ev_mode,
                validated_inputs=ev_validated,
                prior_row_state=prior_state,
                authorized_events=authorized_events,
            )
            o4_resolved: list[str] = []
        else:
            tiered_eval = evaluate_non_stub_tiers(
                evaluator=ev,
                session_inputs=eval_inputs,
                calendar_session=cal,
                policy_hash=p_hash,
                session_date=session_date,
                decision_timestamp_utc=decision_utc,
                prior_row_state=prior_state,
                authorized_events=authorized_events,
            )
            rows_by_id = tiered_eval["rows_by_id"]
            eff_scope = tiered_eval["effective_state_by_scope"]
            next_state = tiered_eval["next_row_state"]
            o4_resolved = tiered_eval["o4_resolved_symbols"]
        if o1_blocks_session_commit(rows_by_id["O1"]) or rows_by_id["O2"]["would_have_tripped"]:
            built = {
                "appended": False,
                "fail_closed": True,
                "o1_output": rows_by_id["O1"],
                "o2_output": rows_by_id["O2"],
                "effective_state_by_scope": eff_scope,
                "next_row_state": next_state,
                "o4_resolved_symbols": o4_resolved,
            }
        else:
            mkt_digest = sha256_canonical(mkt_recs)
            cycle_inputs = (
                dict(session_inputs)
                if is_stub_evaluator(ev)
                else {
                    **session_inputs,
                    "market_records": mkt_recs,
                    "required_series": required_for_eval,
                    "positions_source_record": pos_src,
                    "broker_reconciliation_source_record": brk_src,
                    **({"prior_closes": prior_closes} if prior_closes else {}),
                }
            )
            if authorized_events is not None:
                cycle_inputs["authorized_events"] = copy.deepcopy(authorized_events)
            cycle_labels = (
                {
                    "production_chain": True,
                    "soxl_basis": SOXL_PROXY_LABEL,
                    "forward_clock_credit_eligible": True,
                }
                if cfg.mode == "production"
                else {
                    "drill_chain": True,
                    "soxl_basis": SOXL_PROXY_LABEL,
                    "forward_clock_credit_eligible": False,
                }
            )
            built = build_record_body(
                chain_id=cfg.chain_id,
                sequence=seq,
                prev_record_sha256=head_hash,
                mode=cfg.mode,
                session_date=session_date,
                decision_timestamp_utc=decision_utc,
                session_inputs=cycle_inputs,
                calendar_session=cal,
                code_version_sha=code_sha,
                evaluator=ev,
                prior_row_state=prior_state,
                discipline_labels=cycle_labels,
                market_records_digest=mkt_digest,
                runtime_config=eff_cfg,
                calendar_spec=spec,
            )

    def _persist_pending_rejected_state() -> str | None:
        nonlocal anchor_bytes
        if is_stub_evaluator(ev) or not isinstance(built.get("next_row_state"), dict):
            return None
        merged_o4_resolved = sorted(
            set(prior_o4_resolved_symbols)
            | {
                str(s)
                for s in (built.get("o4_resolved_symbols") or ())
                if isinstance(s, str) and s
            }
        )
        merged_authorized_events = normalize_pending_authorized_events(
            prior_authorized_events + normalize_pending_authorized_events(authorized_events)
        )
        pending_payload = build_pending_rejected_state_payload(
            chain_id=cfg.chain_id,
            base_sequence=seq - 1,
            base_record_sha256=head_hash,
            rejected_session_date=session_date,
            engine_hash=ev.engine_hash,
            spec_version=ev.spec_version,
            next_row_state=built["next_row_state"],
            base_prior_row_state=session_base_prior_state,
            o4_resolved_symbols=merged_o4_resolved,
            authorized_events=merged_authorized_events or None,
        )
        save_pending_rejected_state_snapshot(
            snapshots_dir,
            chain_id=cfg.chain_id,
            base_sequence=seq - 1,
            base_record_sha256=head_hash,
            rejected_session_date=session_date,
            engine_hash=ev.engine_hash,
            spec_version=ev.spec_version,
            next_row_state=built["next_row_state"],
            base_prior_row_state=session_base_prior_state,
            o4_resolved_symbols=merged_o4_resolved,
            authorized_events=merged_authorized_events or None,
        )
        pending_sha_val = compute_pending_rejected_state_sha256(pending_payload)
        if anchor_resolved is not None:
            prior_head_ds = (
                str(prior_records[-1]["record_body"]["session_date"])
                if prior_records
                else None
            )
            _advance_trusted_head_anchor(
                anchor_resolved,
                chain_id=cfg.chain_id,
                expected_prior_bytes=anchor_bytes,
                sequence=seq - 1,
                session_date=prior_head_ds,
                head_sha256=head_hash,
                pending_rejected_state_sha256=pending_sha_val,
                pending_rejected_session_date=session_date,
                now_utc=_format_utc_ms(clock_fn()),
            )
            anchor_bytes = anchor_resolved.read_bytes()
        return pending_sha_val

    if not built["appended"]:
        pending_sha = _persist_pending_rejected_state()
        fail_reason = built["o1_output"].get("reason") or built.get("o2_output", {}).get("reason")
        fail_res = _build_fail_closed_result(
            t_start=t_start,
            root=root,
            reports_dir=reports_dir,
            cfg=cfg,
            cal=cal,
            session_date=session_date,
            decision_utc=decision_utc,
            deadlines=deadlines,
            head_hash=head_hash,
            consecutive_count=prior_streak,
            created_at_utc=_format_utc_ms(clock_fn()),
            reason_key=str(fail_reason),
            fail_reason=str(fail_reason),
            required_series=series_summary,
            receipt_status="not_applicable_fail_closed",
            receipt_reasons=["fail_closed_no_append", f"{cfg.mode}_mode"],
            pending_rejected_state_sha256=pending_sha,
        )
        return fail_res

    # Step 4: Obtain detached OTS receipt, check real post-stamp wall-clock, commit atomically
    body = built["record_body"]
    rec_sha256 = sha256_canonical(body)
    client = ots_client or OpenTimestampsClient(
        calendars=cfg.ots_calendars,
        transport=ots_transport,
        timeout=cfg.ots_timeout_seconds,
    )

    ots_bytes: bytes | None = None
    ots_error: str | None = None
    try:
        ots_bytes, _ = client.stamp_digest(rec_sha256)
    except Exception as exc:
        ots_error = str(exc)

    post_stamp_utc = _format_utc_ms(clock_fn())
    rcp_obtained_utc = post_stamp_utc

    if post_stamp_utc > deadline_utc:
        pending_sha = _persist_pending_rejected_state()
        fail_reason = "late_after_commitment_deadline_backfill_rejected"
        fail_res = _build_fail_closed_result(
            t_start=t_start,
            root=root,
            reports_dir=reports_dir,
            cfg=cfg,
            cal=cal,
            session_date=session_date,
            decision_utc=decision_utc,
            deadlines=deadlines,
            head_hash=head_hash,
            consecutive_count=prior_streak,
            created_at_utc=post_stamp_utc,
            reason_key=fail_reason,
            fail_reason=fail_reason,
            required_series=series_summary,
            receipt_anchor="opentimestamps",
            receipt_status="late_backfill_rejected",
            receipt_obtained_at_utc=rcp_obtained_utc,
            receipt_reasons=[fail_reason, "drill_mode"],
            pending_rejected_state_sha256=pending_sha,
        )
        return fail_res

    if ots_bytes is None and not allow_unanchored_on_ots_failure:
        pending_sha = _persist_pending_rejected_state()
        suffix = (
            f" (pending_rejected_state_sha256={pending_sha})"
            if pending_sha is not None
            else ""
        )
        raise RuntimeError(f"OTS receipt acquisition failed: {ots_error}{suffix}")

    session_entry: dict[str, object] = {
        "session_date": session_date,
        "decision_timestamp_utc": decision_utc,
        "inputs": session_inputs,
        "discipline_labels": body.get("discipline_labels"),
        "market_records_digest": sha256_canonical(mkt_recs),
        "market_records": mkt_recs,
    }
    if not is_stub_evaluator(ev):
        fctx: dict[str, object] = {
            "prior_closes": prior_closes,
            "positions_source_record": pos_src,
            "broker_reconciliation_source_record": brk_src,
        }
        if prior_row_state_used is not None:
            fctx["prior_row_state_used"] = prior_row_state_used
        if authorized_events is not None:
            fctx["authorized_events"] = copy.deepcopy(authorized_events)
        session_entry["fuse_context"] = fctx
    env = _commit_drill_session_atomically(
        ledger=ledger,
        seq=seq,
        body=body,
        ots_bytes=ots_bytes,
        rcp_obtained_utc=rcp_obtained_utc,
        deadline_utc=deadline_utc,
        allow_unanchored=allow_unanchored_on_ots_failure,
        bundle_dir=bundle_dir,
        cfg=cfg,
        spec=spec,
        code_sha=code_sha,
        runtime_config=eff_cfg,
        archive_dir=archive_dir,
        session_entry=session_entry,
        post_ledger_hook=_post_ledger_commit_hook,
        snapshots_dir=None if is_stub_evaluator(ev) else snapshots_dir,
        next_row_state=None if is_stub_evaluator(ev) else built["next_row_state"],
        engine_hash=None if is_stub_evaluator(ev) else ev.engine_hash,
        spec_version=None if is_stub_evaluator(ev) else ev.spec_version,
        trusted_head_anchor_path=anchor_resolved,
        expected_prior_anchor_bytes=anchor_bytes,
        anchor_updated_at_utc=(
            _format_utc_ms(clock_fn()) if anchor_resolved is not None else None
        ),
    )
    if _post_record_commit_hook is not None:
        _post_record_commit_hook()

    receipt_summary = _summarize_committed_receipt(
        ledger=ledger,
        seq=seq,
        env=env,
        deadline_utc=deadline_utc,
        decision_utc=decision_utc,
        ots_error=ots_error,
    )
    consecutive_now = _count_consecutive_trading_sessions(
        prior_records,
        session_date,
        spec,
        cfg.venue,
        current_appended=True,
    )
    report, footprint = _build_committed_completeness_report(
        t_start=t_start,
        root=root,
        logs_dir=logs_dir,
        cfg=cfg,
        cal=cal,
        deadlines=deadlines,
        session_date=session_date,
        decision_utc=decision_utc,
        series_summary=series_summary,
        seq=seq,
        record_sha256=env["record_sha256"],
        receipt_summary=receipt_summary,
        consecutive_now=consecutive_now,
    )
    report_created_utc = _format_utc_ms(clock_fn())
    if report_created_utc > deadline_utc:
        fail_reason = "late_after_commitment_deadline_backfill_rejected"
        return {
            "appended": True,
            "fail_closed": True,
            "reason": fail_reason,
            "detail": (
                f"{fail_reason}: report creation crossed commitment deadline "
                f"({report_created_utc} > {deadline_utc})"
            ),
            "sequence": seq,
            "record_sha256": env["record_sha256"],
            "report": None,
            "report_paths": [],
            "report_written": False,
            "existing_report_preserved": (reports_dir / f"completeness_{session_date}.json").exists()
            or (reports_dir / f"completeness_{session_date}.md").exists(),
            "footprint": footprint,
        }
    report_paths, wrote_report = _write_completeness_report(
        reports_dir,
        report,
        created_at_utc=report_created_utc,
        post_json_write_hook=_post_report_json_hook,
    )
    if wrote_report:
        report["created_at_utc"] = report_created_utc
    return {
        "appended": True,
        "fail_closed": False,
        "sequence": seq,
        "record_sha256": env["record_sha256"],
        "report": report,
        "report_paths": [str(p) for p in report_paths],
        "report_written": wrote_report,
        "footprint": footprint,
    }


def _parse_utc_iso(raw_ts: str) -> datetime:
    iso = raw_ts[:-1] + "+00:00" if raw_ts.endswith("Z") else raw_ts
    return datetime.fromisoformat(iso).astimezone(timezone.utc)


def detect_missed_runs(
    *,
    window_start_date: str,
    as_of_utc: str,
    data_dir: str | Path,
    config: DrillSchedulerConfig | None = None,
    spec_dir: str | Path | None = None,
) -> dict:
    """Detect silent missed runs, late/missing receipts, and backfill anomalies without AI."""
    cfg = config if config is not None else load_scheduler_config()
    spec = load_spec(_resolve_spec_dir_for_config(cfg, spec_dir))
    if spec["rules"]["calendar_version"] != cfg.calendar_version:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: configured {cfg.calendar_version} != loaded {spec['rules']['calendar_version']}"
        )
    if cfg.calendar_spec_digest is not None and spec.get("spec_digest") != cfg.calendar_spec_digest:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: configured spec_digest {cfg.calendar_spec_digest} != loaded {spec.get('spec_digest')}"
        )
    if cfg.venue != spec["rules"]["exchange"]:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: unsupported_venue:{cfg.venue} != {spec['rules']['exchange']}"
        )

    root = Path(data_dir)
    ledger = ShadowLedger(root / "ledger")
    reports_dir = root / "reports"

    chain_ok, bad_seq, chain_err = ledger.verify_chain()
    chain_integrity_err: str | None = None
    if chain_ok:
        records = ledger.load_records()
        by_session: dict[str, dict] = {
            env["record_body"]["session_date"]: env for env in records
        }
        bundle_ok = True
        bundle_err: str | None = None
        bundle_obj_validated: dict | None = None
        if records:
            bundle_ok, bundle_err, bundle_obj_validated = _validate_replay_bundle_matches_ledger(
                root / "replay",
                records,
                cfg=cfg,
                spec=spec,
                allow_trailing_uncommitted_tail=True,
                prune_uncommitted_tail=False,
                reports_dir=reports_dir,
            )
    else:
        records = []
        by_session = {}
        bundle_ok = False
        chain_integrity_err = (
            f"existing_chain_verification_failed:{chain_err}"
            + (f":seq={bad_seq}" if bad_seq is not None else "")
        )
        bundle_err = chain_integrity_err
        bundle_obj_validated = None

    as_of_dt = _parse_utc_iso(as_of_utc)
    as_of_ms = _format_utc_ms(as_of_dt)
    cov_start = date.fromisoformat(spec["rules"]["coverage_start_date"])
    cov_end = date.fromisoformat(spec["rules"]["coverage_end_date"])
    start_d = date.fromisoformat(window_start_date)
    as_of_local_date = as_of_dt.astimezone(_NY_TZ).date()
    if (
        start_d < cov_start
        or as_of_local_date > cov_end
        or start_d > cov_end
        or start_d > as_of_local_date
    ):
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: requested [{start_d.isoformat()}, {as_of_local_date.isoformat()}] "
            f"exceeds {spec['rules']['calendar_version']} coverage [{cov_start.isoformat()}, {cov_end.isoformat()}]"
        )
    end_date = as_of_local_date
    cur = start_d

    evaluated_days: list[dict[str, object]] = []
    silent_missed_days: list[str] = []
    failed_days: list[str] = []
    backfill_anomalies: list[str] = []
    ok_days: list[str] = []

    while cur <= end_date:
        ds = cur.isoformat()
        cal = resolve_session(cfg.venue, ds, spec=spec)
        if cal.get("session_state") == "unknown":
            raise CalendarCoverageError(
                f"calendar_coverage_insufficient: {cal.get('reason')}:{cfg.venue}:{ds}"
            )
        if not cal.get("is_trading_day") or cal.get("blocks_eligibility"):
            cur += timedelta(days=1)
            continue

        deadlines = compute_session_deadlines(ds, cfg, calendar_session=cal)
        deadline_utc = deadlines["commitment_deadline_utc"]
        if as_of_ms <= deadline_utc:
            # Commitment window for ds is still open
            cur += timedelta(days=1)
            continue

        env = by_session.get(ds)
        rep_path = reports_dir / f"completeness_{ds}.json"
        md_path = reports_dir / f"completeness_{ds}.md"
        rep_obj: dict | None = None
        if rep_path.is_file():
            try:
                parsed_rep = parse_json_no_duplicates(rep_path.read_text(encoding="utf-8"))
                if isinstance(parsed_rep, dict):
                    rep_obj = parsed_rep
            except Exception:
                rep_obj = None

        if env is not None:
            body = env["record_body"]
            seq = body["sequence"]
            if not bundle_ok or bundle_obj_validated is None:
                status = (
                    "incomplete_missing_replay_bundle"
                    if bundle_err == "missing_replay_bundle_for_non_empty_ledger"
                    else "incomplete_or_corrupt_replay_bundle"
                )
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = str(bundle_err or "committed_record_missing_valid_replay_bundle")
            elif (bundle_cfg_err := _bundle_deadline_config_mismatch(bundle_obj_validated, cfg)) is not None:
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = bundle_cfg_err
            elif body["decision_timestamp_utc"] > deadline_utc:
                status = "backfill_anomaly"
                backfill_anomalies.append(ds)
                failed_days.append(ds)
                detail = "decision_timestamp_after_commitment_deadline"
            else:
                b_sess = bundle_obj_validated["sessions"][seq]
                bmkt = b_sess["market_records"]
                expected_consecutive = _count_consecutive_trading_sessions(
                    [r for r in records if r["record_body"]["sequence"] < seq],
                    ds,
                    spec,
                    cfg.venue,
                    current_appended=True,
                )
                rep_valid_vs_ledger = _is_committed_session_report_valid(
                    reports_dir,
                    ds,
                    env,
                    committed_mkt_records=bmkt,
                    deadlines=deadlines,
                    cfg=cfg,
                    cal=cal,
                    ledger=ledger,
                    expected_consecutive_count=expected_consecutive,
                )
                if not rep_valid_vs_ledger:
                    status = "incomplete_missing_completeness_report"
                    silent_missed_days.append(ds)
                    failed_days.append(ds)
                    detail = "committed_record_missing_valid_completeness_report"
                elif body["decision_timestamp_utc"] > deadlines["decision_cutoff_utc"]:
                    status = "failed_documented"
                    failed_days.append(ds)
                    detail = "decision_timestamp_after_decision_cutoff"
                else:
                    rpath = ledger._receipt_path(seq)
                    opath = ledger._ots_path(seq)
                    if not rpath.is_file():
                        status = "failed_documented"
                        failed_days.append(ds)
                        detail = "missing_detached_receipt"
                    else:
                        try:
                            rcp = parse_json_no_duplicates(rpath.read_text(encoding="utf-8"))
                            ots_bytes = opath.read_bytes() if opath.is_file() else None
                        except Exception as exc:
                            status = "incomplete_or_corrupt_replay_bundle"
                            silent_missed_days.append(ds)
                            failed_days.append(ds)
                            detail = f"unreadable_or_corrupt_detached_receipt:{type(exc).__name__}"
                        else:
                            ok_rcp, err_rcp = verify_receipt(env, rcp, ots_bytes=ots_bytes)
                            timely, err_timely = is_receipt_timely(
                                rcp,
                                expected_commitment_deadline_utc=deadline_utc,
                                decision_timestamp_utc=body.get("decision_timestamp_utc"),
                                require_expected_deadline=True,
                            )
                            if not ok_rcp:
                                status = "failed_documented"
                                failed_days.append(ds)
                                detail = str(err_rcp)
                            elif not timely:
                                status = "failed_documented"
                                failed_days.append(ds)
                                detail = str(err_timely)
                            else:
                                status = "ok"
                                ok_days.append(ds)
                                detail = "appended_with_timely_receipt"
        elif rep_obj is not None:
            rep_created = rep_obj.get("created_at_utc")
            rep_ledger = rep_obj.get("ledger") if isinstance(rep_obj.get("ledger"), dict) else {}
            rep_appended = rep_ledger.get("appended") is True
            rep_reason = str(rep_ledger.get("fail_closed_reason", ""))
            rep_decision = rep_obj.get("decision_timestamp_utc")
            orphan_err = _detect_orphaned_committed_evidence_for_session(
                root, ledger, records, reports_dir, ds
            )
            if rep_appended:
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = "committed_report_present_without_ledger_record"
            elif orphan_err is not None:
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = orphan_err
            elif not _is_fail_closed_session_report_valid(
                rep_path=rep_path,
                md_path=md_path,
                rep_obj=rep_obj,
                session_date=ds,
                cfg=cfg,
                cal=cal,
                deadlines=deadlines,
            ):
                status = "missed_run_silent_failure"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = "completeness_report_outside_session_window_or_unverified"
            elif not chain_ok and not rep_reason.startswith("existing_chain_verification_failed:"):
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = chain_integrity_err or "existing_chain_verification_failed"
            else:
                status = "failed_documented"
                failed_days.append(ds)
                detail = f"documented_fail_closed:{rep_reason}"
        else:
            orphan_err = _detect_orphaned_committed_evidence_for_session(
                root, ledger, records, reports_dir, ds
            )
            if orphan_err is not None:
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = orphan_err
            elif not chain_ok:
                status = "incomplete_or_corrupt_replay_bundle"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = chain_integrity_err or "existing_chain_verification_failed"
            else:
                status = "missed_run_silent_failure"
                silent_missed_days.append(ds)
                failed_days.append(ds)
                detail = "no_ledger_record_and_no_completeness_report_past_deadline"

        evaluated_days.append(
            {
                "session_date": ds,
                "commitment_deadline_utc": deadline_utc,
                "status": status,
                "detail": detail,
            }
        )
        cur += timedelta(days=1)

    audit_report = {
        "window_start_date": window_start_date,
        "as_of_utc": as_of_ms,
        "chain_id": cfg.chain_id,
        "mode": cfg.mode,
        "chain_ok": chain_ok,
        "chain_error": chain_integrity_err,
        "ai_session_independent": True,
        "zero_backfill_enforced": True,
        "evaluated_trading_days": evaluated_days,
        "ok_days": ok_days,
        "failed_days": failed_days,
        "silent_missed_days": silent_missed_days,
        "backfill_anomalies": backfill_anomalies,
        "missed_run_detected": len(silent_missed_days) > 0,
    }
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = reports_dir / f"missed_run_report_{end_date.isoformat()}.json"
    out_path.write_text(json.dumps(audit_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    audit_report["report_path"] = str(out_path)
    return audit_report


def validate_scheduler_templates(deploy_dir: str | Path | None = None) -> dict[str, object]:
    """Verify systemd, watchdog, and launchd unit templates enforce drill-only & resource caps."""
    d_dir = Path(deploy_dir) if deploy_dir is not None else DEFAULT_DEPLOY_DIR
    expected_files = (
        "shadow-drill.service",
        "shadow-drill.timer",
        "shadow-missed-run.service",
        "shadow-missed-run.timer",
        "shadow-watchdog.sh",
        "com.quantstrategylab.shadow-drill.plist",
        "com.quantstrategylab.shadow-missed-run.plist",
    )
    missing = [name for name in expected_files if not (d_dir / name).is_file()]
    errors: list[str] = []
    if missing:
        errors.append(f"missing_templates:{missing}")

    for name in expected_files:
        p = d_dir / name
        if not p.is_file():
            continue
        text = p.read_text(encoding="utf-8")
        lowered = text.lower()
        if "--mode production" in lowered or "clk-01" in lowered and "inactive" not in lowered and "never" not in lowered and "drill" not in lowered:
            errors.append(f"forbidden_production_flag_in:{name}")
        if name.endswith(".service"):
            for req in ("Nice=15", "CPUQuota=25%", "MemoryMax=512M", "shadow.scheduler"):
                if req not in text:
                    errors.append(f"{name}:missing_{req}")
        if name == "shadow-drill.timer":
            if "OnCalendar=Mon..Fri *-*-* 14:30:00 America/New_York" not in text:
                errors.append(f"{name}:must_pin_14_30_America_New_York")
            if "OnCalendar=Mon..Fri *-*-* 17:30:00 America/New_York" not in text:
                errors.append(f"{name}:must_pin_17_30_America_New_York")
            if "Asia/Singapore" in text:
                errors.append(f"{name}:must_not_schedule_in_Asia_Singapore")
        if name == "shadow-missed-run.timer":
            if "OnCalendar=Mon..Fri *-*-* 23:45:00 America/New_York" not in text:
                errors.append(f"{name}:must_pin_23_45_America_New_York")
            if "Asia/Singapore" in text:
                errors.append(f"{name}:must_not_schedule_in_Asia_Singapore")
        if name.endswith(".plist"):
            import plistlib

            try:
                plist_obj = plistlib.loads(p.read_bytes())
                if plist_obj.get("Disabled") is not True:
                    errors.append(f"{name}:must_set_Disabled_true")
                cal_entries = plist_obj.get("StartCalendarInterval", [])
                if not isinstance(cal_entries, list):
                    errors.append(f"{name}:StartCalendarInterval_must_be_list")
                else:
                    hours_mins = {
                        (entry.get("Hour"), entry.get("Minute"))
                        for entry in cal_entries
                        if isinstance(entry, dict)
                    }
                    prog_args = " ".join(str(x) for x in plist_obj.get("ProgramArguments", []))
                    if name == "com.quantstrategylab.shadow-drill.plist":
                        for slot in ((2, 30), (3, 30), (5, 30), (6, 30)):
                            if slot not in hours_mins:
                                errors.append(f"{name}:missing_est_edt_schedule_{slot[0]:02d}_{slot[1]:02d}")
                        if "--require-ny-hhmm" not in prog_args or "14:30,17:30" not in prog_args:
                            errors.append(f"{name}:missing_require_ny_hhmm_14_30_and_17_30")
                    if name == "com.quantstrategylab.shadow-missed-run.plist":
                        if (11, 45) not in hours_mins or (12, 45) not in hours_mins:
                            errors.append(f"{name}:missing_est_edt_dual_schedule_11_45_and_12_45")
                        if "--require-ny-hhmm" not in prog_args or "23:45" not in prog_args:
                            errors.append(f"{name}:missing_require_ny_hhmm_23_45")
            except Exception as exc:
                errors.append(f"{name}:invalid_plist:{exc}")
            for req in ("<key>Disabled</key>", "<true/>", "<key>Nice</key>", "<integer>15</integer>", "<key>LowPriorityIO</key>", "shadow.scheduler"):
                if req not in text:
                    errors.append(f"{name}:missing_{req}")
        if name == "shadow-watchdog.sh":
            for req in ("nice -n 15", "env.sh", "shadow.scheduler", "detect-missed", "--require-ny-hhmm"):
                if req not in text:
                    errors.append(f"{name}:missing_{req}")

    return {
        "deploy_dir": str(d_dir),
        "templates_checked": list(expected_files),
        "templates_ok": len(errors) == 0,
        "errors": errors,
    }


def run_dry_run_harness(repo_root: str | Path | None = None) -> dict[str, object]:
    """Execute end-to-end offline drill-chain + missed-run + replay dry-run in temp space."""
    root_repo = Path(repo_root) if repo_root is not None else REPO_ROOT
    cfg = load_scheduler_config(root_repo / "config" / "drill-scheduler.json")
    tmpl_check = validate_scheduler_templates(root_repo / "deploy" / "scheduler")

    def _mock_ots_transport(
        method: str,
        url: str,
        body: bytes | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> tuple[int, bytes]:
        if url.endswith("/digest") and body is not None:
            ots_full, _ = build_synthetic_ots_fixture(body.hex(), calendar_uri=url.rsplit("/", 1)[0])
            from shadow.ots import OTS_HEADER_MAGIC

            return 200, ots_full[len(OTS_HEADER_MAGIC) + 2 + 32 :]
        return 404, b""

    def _mock_market_transport(url: str, headers: Mapping[str, str]) -> tuple[int, bytes]:
        # Deterministic synthetic multi-day payloads covering 2025-11-21 & 2025-11-25
        if "/daily/TQQQ/prices" in url:
            return 200, json.dumps(
                [
                    {"date": "2025-11-21T00:00:00.000Z", "close": 47.48, "divCash": 0.0, "splitFactor": 1.0},
                    {"date": "2025-11-25T00:00:00.000Z", "close": 48.10, "divCash": 0.0, "splitFactor": 1.0},
                ]
            ).encode("utf-8")
        if "/daily/SOXL/prices" in url:
            return 200, json.dumps(
                [
                    {"date": "2025-11-21T00:00:00.000Z", "close": 31.73, "divCash": 0.0, "splitFactor": 1.0},
                    {"date": "2025-11-25T00:00:00.000Z", "close": 32.25, "divCash": 0.0, "splitFactor": 1.0},
                ]
            ).encode("utf-8")
        if "/daily/QQQ/prices" in url:
            return 200, json.dumps(
                [
                    {"date": "2025-11-21T00:00:00.000Z", "close": 510.12, "divCash": 0.0, "splitFactor": 1.0},
                    {"date": "2025-11-25T00:00:00.000Z", "close": 512.80, "divCash": 0.0, "splitFactor": 1.0},
                ]
            ).encode("utf-8")
        if "%5EXNDX" in url:
            return 200, json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "meta": {"symbol": "^XNDX", "instrumentType": "INDEX"},
                                "timestamp": [1763758800, 1764104400],
                                "indicators": {"quote": [{"close": [29462.35, 29600.10]}]},
                            }
                        ]
                    }
                }
            ).encode("utf-8")
        if "XSOX" in url:
            return 200, json.dumps(
                {
                    "chart": {
                        "result": [
                            {
                                "meta": {"symbol": "XSOX", "instrumentType": "INDEX"},
                                "timestamp": [1763758800, 1764104400],
                                "indicators": {"quote": [{"close": [8179.87, 8210.50]}]},
                            }
                        ]
                    }
                }
            ).encode("utf-8")
        if "markets.newyorkfed.org" in url:
            return 200, json.dumps(
                {
                    "refRates": [
                        {"effectiveDate": "2025-11-20", "type": "SOFR", "percentRate": 3.91, "revisionIndicator": ""},
                        {"effectiveDate": "2025-11-24", "type": "SOFR", "percentRate": 3.90, "revisionIndicator": ""},
                    ]
                }
            ).encode("utf-8")
        if "treasury.example.test" in url:
            return 200, b"Date,3 Mo\n2025-11-20,3.90\n2025-11-24,3.89\n"
        return 404, b""

    t0 = time.monotonic()
    with tempfile.TemporaryDirectory() as tmpdir:
        data_dir = Path(tmpdir) / "drill_data"
        day1 = run_daily_drill_cycle(
            session_date="2025-11-21",
            data_dir=data_dir,
            config=cfg,
            transport=_mock_market_transport,
            environ={"TIINGO_API_KEY": "dry-run-placeholder"},
            ots_transport=_mock_ots_transport,
            wall_clock=lambda: datetime(2025, 11, 21, 22, 30, 0, tzinfo=timezone.utc),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        # Deliberately suppress 2025-11-24 firing, then run 2025-11-25
        day3 = run_daily_drill_cycle(
            session_date="2025-11-25",
            data_dir=data_dir,
            config=cfg,
            transport=_mock_market_transport,
            environ={"TIINGO_API_KEY": "dry-run-placeholder"},
            ots_transport=_mock_ots_transport,
            wall_clock=lambda: datetime(2025, 11, 25, 22, 30, 0, tzinfo=timezone.utc),
            treasury_endpoint="https://treasury.example.test/yield{year}.csv",
            sleeper=lambda _: None,
        )
        missed = detect_missed_runs(
            window_start_date="2025-11-21",
            as_of_utc="2025-11-26T05:00:00.000Z",
            data_dir=data_dir,
            config=cfg,
        )
        replay_res = replay_window(data_dir / "replay", data_dir / "ledger")
        footprint = check_footprint_budget(data_dir, cfg.max_footprint_bytes)

    return {
        "templates": tmpl_check,
        "day1_appended": day1["appended"],
        "day3_appended": day3["appended"],
        "day3_consecutive_session_count": day3["report"]["consecutive_session_count"],
        "missed_run_detected": missed["missed_run_detected"],
        "silent_missed_days": missed["silent_missed_days"],
        "ok_days": missed["ok_days"],
        "replay": {
            "bit_for_bit_match": replay_res["bit_for_bit_match"],
            "chain_ok": replay_res["chain_ok"],
            "receipts_ok": replay_res["receipts_ok"],
            "receipts_creditable": replay_res["receipts_creditable"],
            "archived_inputs_ok": replay_res["archived_inputs_ok"],
            "replayed_count": replay_res["replayed_count"],
        },
        "footprint": footprint,
        "wall_seconds": round(time.monotonic() - t0, 6),
        "peak_rss_bytes": _peak_rss_bytes(),
    }


def _resolve_auto_run_target(
    cfg: DrillSchedulerConfig,
    now_dt: datetime,
    *,
    data_dir: str | Path | None = None,
) -> tuple[str | None, dict | None]:
    """Resolve current NY date for '--session auto'; cleanly no-op on holidays, pre-close slots, or committed half-days."""
    spec = load_spec(_resolve_spec_dir_for_config(cfg))
    if cfg.venue != spec["rules"]["exchange"]:
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: unsupported_venue:{cfg.venue}"
        )
    ny_today = now_dt.astimezone(_NY_TZ).date().isoformat()
    today_cal = resolve_session(cfg.venue, ny_today, spec=spec)
    if today_cal.get("session_state") == "unknown":
        raise CalendarCoverageError(
            f"calendar_coverage_insufficient: {today_cal.get('reason')} for {ny_today}"
        )
    if not today_cal.get("is_trading_day") or today_cal.get("blocks_eligibility"):
        return None, {
            "status": "holiday_no_op",
            "ny_date": ny_today,
            "reason": today_cal.get("reason"),
            "appended": False,
            "reports_written": 0,
        }
    deadlines = compute_session_deadlines(ny_today, cfg, calendar_session=today_cal)
    now_utc_ms = _format_utc_ms(now_dt)
    if now_utc_ms < deadlines["session_close_utc"]:
        return None, {
            "status": "pre_close_slot_no_op",
            "ny_date": ny_today,
            "session_state": today_cal.get("session_state"),
            "session_close_utc": deadlines["session_close_utc"],
            "appended": False,
            "reports_written": 0,
        }
    if today_cal.get("session_state") == "half_day" and data_dir is not None:
        ledger_dir = Path(data_dir) / "ledger"
        reports_dir = Path(data_dir) / "reports"
        bundle_dir = Path(data_dir) / "replay"
        if ledger_dir.is_dir():
            ledger = ShadowLedger(ledger_dir)
            with ledger.chain_lock():
                ok_chain, _, _ = ledger.verify_chain()
                prior_records = ledger.load_records() if ok_chain else []
            committed_env = next(
                (
                    env
                    for env in prior_records
                    if isinstance(env, dict)
                    and isinstance(env.get("record_body"), dict)
                    and env["record_body"].get("session_date") == ny_today
                ),
                None,
            )
            if committed_env is not None:
                bundle_ok, _, bundle_obj = _validate_replay_bundle_matches_ledger(
                    bundle_dir,
                    prior_records,
                    cfg=cfg,
                    spec=spec,
                    allow_trailing_uncommitted_tail=True,
                    prune_uncommitted_tail=False,
                )
                if bundle_ok and bundle_obj is not None:
                    ex_seq = committed_env["record_body"]["sequence"]
                    bmkt = bundle_obj["sessions"][ex_seq]["market_records"]
                    expected_consecutive = _count_consecutive_trading_sessions(
                        [r for r in prior_records if r["record_body"]["sequence"] < ex_seq],
                        ny_today,
                        spec,
                        cfg.venue,
                        current_appended=True,
                    )
                    if _is_committed_session_report_valid(
                        reports_dir,
                        ny_today,
                        committed_env,
                        committed_mkt_records=bmkt,
                        deadlines=deadlines,
                        cfg=cfg,
                        cal=today_cal,
                        ledger=ledger,
                        expected_consecutive_count=expected_consecutive,
                    ):
                        _reconcile_committed_state_snapshot_if_missing(
                            snapshots_dir=Path(data_dir) / "state_snapshots",
                            ledger=ledger,
                            bundle_obj=bundle_obj,
                            prior_records=prior_records,
                            ex_seq=ex_seq,
                            existing_env=committed_env,
                            cfg=cfg,
                            spec=spec,
                            cal=today_cal,
                        )
                        return None, {
                            "status": "already_committed_no_op",
                            "ny_date": ny_today,
                            "session_state": "half_day",
                            "appended": False,
                            "reports_written": 0,
                        }
    return ny_today, None


def _validate_isolated_simulation_target(data_dir: str | Path, chain_id: str) -> None:
    if not chain_id.startswith("sim-") or chain_id == "drill-sh1-v1" or chain_id.startswith("drill-"):
        raise DrillOnlyViolationError(
            f"simulate-drill requires isolated chain_id starting with 'sim-', got {chain_id!r}"
        )
    resolved = Path(data_dir).resolve()
    repo_data = (REPO_ROOT / "data").resolve()
    if resolved == repo_data or repo_data in resolved.parents:
        raise DrillOnlyViolationError(
            f"simulate-drill refuses writing into repository data directory: {resolved}"
        )
    if "drill" in resolved.parts:
        raise DrillOnlyViolationError(
            f"simulate-drill refuses directory path with 'drill' component: {resolved}"
        )
    ledger_dir = resolved / "ledger"
    if ledger_dir.is_dir():
        existing = ShadowLedger(ledger_dir).load_records()
        for env in existing:
            existing_chain = str(env.get("record_body", {}).get("chain_id", ""))
            if not existing_chain.startswith("sim-"):
                raise DrillOnlyViolationError(
                    f"simulate-drill refuses modifying non-simulation chain {existing_chain!r}"
                )


def _cli_guard_ny_hhmm(raw_hhmm: str | None, ref_dt: datetime) -> tuple[bool, int]:
    """Evaluate --require-ny-hhmm whenever explicitly passed (including ''). Return (should_run, exit_code)."""
    if raw_hhmm is None:
        return True, 0
    try:
        matched = ny_local_time_matches(ref_dt, raw_hhmm)
    except ValueError as exc:
        print(
            json.dumps(
                {"error": "invalid_require_ny_hhmm", "detail": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return False, 1
    if not matched:
        print(
            json.dumps(
                {
                    "skipped_dst_guard": True,
                    "now_utc": _format_utc_ms(ref_dt),
                    "required_ny_hhmm": raw_hhmm,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return False, 0
    return True, 0


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint for AI-independent drill scheduler, detector, and dry-run."""
    parser = argparse.ArgumentParser(description="SHADOW-N SH-1 drill-chain scheduler CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run-drill", help="Execute one drill-chain daily cycle (real wall clock)")
    p_run.add_argument("--session", required=True, help="XNYS session date YYYY-MM-DD or 'auto'")
    p_run.add_argument("--data-dir", required=True, help="Drill data directory")
    p_run.add_argument("--config", default=str(DEFAULT_SCHEDULER_CONFIG_PATH))
    p_run.add_argument("--fixtures-dir", default=None)
    p_run.add_argument("--treasury-endpoint", default=None)
    p_run.add_argument(
        "--require-ny-hhmm",
        default=None,
        help="Optional DST guard (HH:MM America/New_York); exit 0 no-op if outside window",
    )

    p_sim = sub.add_parser(
        "simulate-drill",
        help="Isolated synthetic-clock simulation CLI (refuses drill chains and drill data dirs)",
    )
    p_sim.add_argument("--session", required=True, help="XNYS session date YYYY-MM-DD or 'auto'")
    p_sim.add_argument("--now-utc", required=True, help="Synthetic UTC clock ISO-8601 timestamp")
    p_sim.add_argument("--chain-id", default="sim-sh1-test", help="Isolated simulation chain_id ('sim-*')")
    p_sim.add_argument("--data-dir", required=True, help="Isolated synthetic simulation output directory")
    p_sim.add_argument("--config", default=str(DEFAULT_SCHEDULER_CONFIG_PATH))
    p_sim.add_argument("--fixtures-dir", default=None)
    p_sim.add_argument("--treasury-endpoint", default=None)
    p_sim.add_argument(
        "--require-ny-hhmm",
        default=None,
        help="Optional DST guard (HH:MM America/New_York); exit 0 no-op if outside window",
    )

    p_det = sub.add_parser("detect-missed", help="Run AI-independent missed-run detector")
    p_det.add_argument("--start-date", required=True, help="Window start YYYY-MM-DD")
    p_det.add_argument("--as-of-utc", default=None, help="Evaluation UTC ISO timestamp")
    p_det.add_argument("--data-dir", required=True, help="Drill data directory")
    p_det.add_argument("--config", default=str(DEFAULT_SCHEDULER_CONFIG_PATH))
    p_det.add_argument(
        "--require-ny-hhmm",
        default=None,
        help="Optional DST guard (HH:MM America/New_York); exit 0 no-op if outside window",
    )

    p_fp = sub.add_parser("check-footprint", help="Verify drill data footprint <= 2 GiB")
    p_fp.add_argument("--data-dir", required=True)
    p_fp.add_argument("--config", default=str(DEFAULT_SCHEDULER_CONFIG_PATH))

    sub.add_parser("dry-run", help="Validate unit templates and run offline drill simulation")

    args = parser.parse_args(argv)
    if args.cmd == "dry-run":
        res = run_dry_run_harness()
        print(json.dumps(res, indent=2, sort_keys=True))
        ok = (
            res["templates"]["templates_ok"]
            and res["day1_appended"]
            and res["day3_appended"]
            and (res["day3_consecutive_session_count"] == 1)
            and res["missed_run_detected"]
            and res["silent_missed_days"] == ["2025-11-24"]
            and res["replay"]["bit_for_bit_match"]
            and res["replay"]["chain_ok"]
            and res["replay"]["receipts_ok"]
            and (res["replay"]["receipts_creditable"] is False)
            and res["footprint"]["within_budget"]
        )
        return 0 if ok else 1

    cfg = load_scheduler_config(args.config)
    if args.cmd == "run-drill":
        now_dt = datetime.now(timezone.utc)
        should_run, guard_rc = _cli_guard_ny_hhmm(args.require_ny_hhmm, now_dt)
        if not should_run:
            return guard_rc
        if (args.fixtures_dir is not None and not str(args.fixtures_dir).strip()) or (
            args.treasury_endpoint is not None and not str(args.treasury_endpoint).strip()
        ):
            print(json.dumps({"error": "invalid_cli_argument", "detail": "empty optional path or endpoint"}, indent=2, sort_keys=True))
            return 1
        try:
            sess_date = args.session
            if sess_date == "auto":
                resolved_date, no_op = _resolve_auto_run_target(
                    cfg, now_dt, data_dir=args.data_dir
                )
                if no_op is not None:
                    print(json.dumps(no_op, indent=2, sort_keys=True))
                    return 0
                assert resolved_date is not None
                sess_date = resolved_date
            res = run_daily_drill_cycle(
                session_date=sess_date,
                data_dir=args.data_dir,
                config=cfg,
                fixtures_dir=args.fixtures_dir,
                treasury_endpoint=args.treasury_endpoint,
                wall_clock=None,
            )
        except CalendarCoverageError as exc:
            print(
                json.dumps(
                    {"error": "calendar_coverage_insufficient", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        except ValueError as exc:
            print(
                json.dumps(
                    {"error": "invalid_cli_argument", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(res, indent=2, sort_keys=True))
        return 0

    if args.cmd == "simulate-drill":
        try:
            _validate_isolated_simulation_target(args.data_dir, args.chain_id)
        except DrillOnlyViolationError as exc:
            print(json.dumps({"error": "simulation_isolation_rejected", "detail": str(exc)}, indent=2, sort_keys=True))
            return 1
        try:
            sim_dt = _parse_utc_iso(args.now_utc)
        except ValueError as exc:
            print(json.dumps({"error": "invalid_now_utc", "detail": str(exc)}, indent=2, sort_keys=True))
            return 1
        should_run, guard_rc = _cli_guard_ny_hhmm(args.require_ny_hhmm, sim_dt)
        if not should_run:
            return guard_rc
        if (args.fixtures_dir is not None and not str(args.fixtures_dir).strip()) or (
            args.treasury_endpoint is not None and not str(args.treasury_endpoint).strip()
        ):
            print(json.dumps({"error": "invalid_cli_argument", "detail": "empty optional path or endpoint"}, indent=2, sort_keys=True))
            return 1
        sim_cfg = replace(cfg, chain_id=args.chain_id)
        try:
            sess_date = args.session
            if sess_date == "auto":
                resolved_date, no_op = _resolve_auto_run_target(
                    sim_cfg, sim_dt, data_dir=args.data_dir
                )
                if no_op is not None:
                    print(json.dumps(no_op, indent=2, sort_keys=True))
                    return 0
                assert resolved_date is not None
                sess_date = resolved_date
            res = run_daily_drill_cycle(
                session_date=sess_date,
                data_dir=args.data_dir,
                config=sim_cfg,
                fixtures_dir=args.fixtures_dir,
                treasury_endpoint=args.treasury_endpoint,
                wall_clock=lambda: sim_dt,
            )
        except CalendarCoverageError as exc:
            print(
                json.dumps(
                    {"error": "calendar_coverage_insufficient", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        except ValueError as exc:
            print(
                json.dumps(
                    {"error": "invalid_cli_argument", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(res, indent=2, sort_keys=True))
        return 0

    if args.cmd == "detect-missed":
        try:
            as_of_dt = (
                _parse_utc_iso(args.as_of_utc)
                if args.as_of_utc is not None
                else datetime.now(timezone.utc)
            )
        except ValueError as exc:
            print(json.dumps({"error": "invalid_as_of_utc", "detail": str(exc)}, indent=2, sort_keys=True))
            return 1
        should_run, guard_rc = _cli_guard_ny_hhmm(args.require_ny_hhmm, as_of_dt)
        if not should_run:
            return guard_rc
        as_of = _format_utc_ms(as_of_dt)
        try:
            res = detect_missed_runs(
                window_start_date=args.start_date,
                as_of_utc=as_of,
                data_dir=args.data_dir,
                config=cfg,
            )
        except CalendarCoverageError as exc:
            print(
                json.dumps(
                    {"error": "calendar_coverage_insufficient", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        except ValueError as exc:
            print(
                json.dumps(
                    {"error": "invalid_cli_argument", "detail": str(exc)},
                    indent=2,
                    sort_keys=True,
                )
            )
            return 1
        print(json.dumps(res, indent=2, sort_keys=True))
        return 2 if res["missed_run_detected"] or res["backfill_anomalies"] else 0

    if args.cmd == "check-footprint":
        res = check_footprint_budget(args.data_dir, cfg.max_footprint_bytes)
        print(json.dumps(res, indent=2, sort_keys=True))
        return 0 if res["within_budget"] else 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
