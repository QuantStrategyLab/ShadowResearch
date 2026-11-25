"""C-SA-22 deterministic XNYS sessions calendar consumer."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

DEFAULT_SPEC_DIR = Path(__file__).resolve().parents[2] / "specs" / "csa22-v1"


class CalendarDigestError(ValueError):
    """Raised when the C-SA-22 spec package fails SHA-256 digest verification."""


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _to_utc_ms(d: date, hhmm: str, tz: ZoneInfo) -> str:
    hour, minute = map(int, hhmm.split(":"))
    local_dt = datetime.combine(d, time(hour, minute), tzinfo=tz)
    return local_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def load_spec(spec_dir: str | Path | None = None) -> dict:
    """Load and verify the C-SA-22 spec package digests before returning rules."""
    base_dir = Path(spec_dir) if spec_dir is not None else DEFAULT_SPEC_DIR
    manifest_path = base_dir / "manifest.json"
    if not manifest_path.is_file():
        raise CalendarDigestError(f"missing manifest.json in {base_dir}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    recorded_files = manifest.get("files")
    if not isinstance(recorded_files, dict) or not recorded_files:
        raise CalendarDigestError("manifest.json missing recorded files map")

    for filename, expected_digest in recorded_files.items():
        target = base_dir / filename
        if not target.is_file():
            raise CalendarDigestError(f"missing spec package file: {filename}")
        actual_digest = _sha256_file(target)
        if actual_digest != expected_digest:
            raise CalendarDigestError(
                f"digest mismatch for {filename}: expected {expected_digest}, got {actual_digest}"
            )

    spec_path = base_dir / "SPEC.md"
    exc_candidates = sorted(
        name for name in recorded_files if name.startswith("exceptions-") and name.endswith(".json")
    )
    if len(exc_candidates) != 1:
        raise CalendarDigestError(f"expected single exceptions-*.json in manifest, got {exc_candidates}")
    exc_path = base_dir / exc_candidates[0]
    computed_spec_digest = hashlib.sha256(
        spec_path.read_bytes() + exc_path.read_bytes()
    ).hexdigest()
    if computed_spec_digest != manifest.get("spec_digest"):
        raise CalendarDigestError(
            f"spec_digest mismatch: expected {manifest.get('spec_digest')}, got {computed_spec_digest}"
        )

    exceptions_data = json.loads(exc_path.read_text(encoding="utf-8"))
    return {
        "spec_dir": str(base_dir),
        "spec_digest": computed_spec_digest,
        "rules": exceptions_data,
    }


def _unknown_session(
    venue: str,
    trade_date: str,
    rules: dict,
    spec_digest: str,
    reason: str,
    is_dst: bool | None = None,
) -> dict:
    return {
        "exchange": venue,
        "trade_date": trade_date,
        "timezone": rules["timezone"],
        "calendar_version": rules["calendar_version"],
        "coverage_start_date": rules["coverage_start_date"],
        "coverage_end_date": rules["coverage_end_date"],
        "spec_digest": spec_digest,
        "is_dst": is_dst,
        "session_state": "unknown",
        "is_trading_day": False,
        "reason": reason,
        "holiday_name": None,
        "open_local": None,
        "close_local": None,
        "cutoff_local": None,
        "open_utc": None,
        "close_utc": None,
        "order_cutoff_utc": None,
        "order_window": None,
        "blocks_orders": True,
        "blocks_eligibility": True,
    }


def resolve_session(
    venue: str,
    trade_date: str,
    spec: dict | None = None,
    cutoff_local: str | None = None,
    observed_venue_status: str | None = None,
) -> dict:
    """Resolve session facts for (venue, trade_date) per C-SA-22."""
    loaded = spec if spec is not None else load_spec()
    rules = loaded["rules"]
    spec_digest = loaded["spec_digest"]
    tz = ZoneInfo(rules["timezone"])

    try:
        parsed_date = date.fromisoformat(trade_date)
    except ValueError:
        return _unknown_session(venue, trade_date, rules, spec_digest, "invalid_date")

    if venue != rules["exchange"]:
        return _unknown_session(venue, trade_date, rules, spec_digest, "unsupported_venue")

    cov_start = date.fromisoformat(rules["coverage_start_date"])
    cov_end = date.fromisoformat(rules["coverage_end_date"])
    if parsed_date < cov_start or parsed_date > cov_end:
        return _unknown_session(venue, trade_date, rules, spec_digest, "out_of_range")

    noon_local = datetime.combine(parsed_date, time(12, 0), tzinfo=tz)
    is_dst = noon_local.dst() != timedelta(0)
    base = {
        "exchange": rules["exchange"],
        "trade_date": trade_date,
        "timezone": rules["timezone"],
        "calendar_version": rules["calendar_version"],
        "coverage_start_date": rules["coverage_start_date"],
        "coverage_end_date": rules["coverage_end_date"],
        "spec_digest": spec_digest,
        "is_dst": is_dst,
    }

    unscheduled = rules["unscheduled_closures"]
    holidays = rules["holidays"]
    early_closes = rules["early_closes"]

    if trade_date in unscheduled:
        name = unscheduled[trade_date]
        result = {
            **base,
            "session_state": "unscheduled_closure",
            "is_trading_day": False,
            "reason": f"unscheduled_closure: {name}",
            "holiday_name": name,
            "open_local": None,
            "close_local": None,
            "cutoff_local": None,
            "open_utc": None,
            "close_utc": None,
            "order_cutoff_utc": None,
            "order_window": None,
            "blocks_orders": True,
            "blocks_eligibility": True,
        }
    elif trade_date in holidays:
        name = holidays[trade_date]
        result = {
            **base,
            "session_state": "closed",
            "is_trading_day": False,
            "reason": f"holiday: {name}",
            "holiday_name": name,
            "open_local": None,
            "close_local": None,
            "cutoff_local": None,
            "open_utc": None,
            "close_utc": None,
            "order_cutoff_utc": None,
            "order_window": None,
            "blocks_orders": True,
            "blocks_eligibility": True,
        }
    elif parsed_date.weekday() >= 5:
        result = {
            **base,
            "session_state": "closed",
            "is_trading_day": False,
            "reason": "weekend",
            "holiday_name": None,
            "open_local": None,
            "close_local": None,
            "cutoff_local": None,
            "open_utc": None,
            "close_utc": None,
            "order_cutoff_utc": None,
            "order_window": None,
            "blocks_orders": True,
            "blocks_eligibility": True,
        }
    else:
        open_local = rules["regular_open_local"]
        if trade_date in early_closes:
            info = early_closes[trade_date]
            close_local = info["close_local"]
            session_state = "half_day"
            reason = f"early_close: {info['name']}"
            holiday_name = info["name"]
        else:
            close_local = rules["regular_close_local"]
            session_state = "regular"
            reason = None
            holiday_name = None

        eff_cutoff_local = close_local if cutoff_local is None else min(cutoff_local, close_local)
        open_utc = _to_utc_ms(parsed_date, open_local, tz)
        close_utc = _to_utc_ms(parsed_date, close_local, tz)
        order_cutoff_utc = _to_utc_ms(parsed_date, eff_cutoff_local, tz)
        if order_cutoff_utc > close_utc:
            order_cutoff_utc = close_utc
            eff_cutoff_local = close_local

        result = {
            **base,
            "session_state": session_state,
            "is_trading_day": True,
            "reason": reason,
            "holiday_name": holiday_name,
            "open_local": open_local,
            "close_local": close_local,
            "cutoff_local": eff_cutoff_local,
            "open_utc": open_utc,
            "close_utc": close_utc,
            "order_cutoff_utc": order_cutoff_utc,
            "order_window": {
                "start_utc": open_utc,
                "end_utc": order_cutoff_utc,
                "half_open": True,
            },
            "blocks_orders": False,
            "blocks_eligibility": False,
        }

    venue_matches = (
        observed_venue_status == result["session_state"]
        or (
            observed_venue_status == "core_session_open"
            and result["session_state"] in ("regular", "half_day")
        )
    )
    if observed_venue_status is not None and not venue_matches:
        result["blocks_orders"] = True
        result["blocks_eligibility"] = True
        result["venue_disagreement"] = True
        result["observed_venue_status"] = observed_venue_status

    return result


def resolve_prior_close_session(
    decision_instant_utc: str,
    spec: dict | None = None,
    venue: str = "XNYS",
) -> dict:
    """Resolve the calendar's latest trading session whose close_utc <= decision_instant_utc."""
    loaded = spec if spec is not None else load_spec()
    rules = loaded["rules"]
    spec_digest = loaded["spec_digest"]
    tz = ZoneInfo(rules["timezone"])

    if venue != rules["exchange"]:
        return _unknown_session(venue, decision_instant_utc[:10], rules, spec_digest, "unsupported_venue")

    iso_norm = decision_instant_utc[:-1] + "+00:00" if decision_instant_utc.endswith("Z") else decision_instant_utc
    try:
        dt_utc = datetime.fromisoformat(iso_norm).astimezone(timezone.utc)
    except ValueError:
        return _unknown_session(venue, decision_instant_utc[:10], rules, spec_digest, "invalid_timestamp")

    instant_ms_utc = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt_utc.microsecond // 1000:03d}Z"
    local_date = dt_utc.astimezone(tz).date()
    cov_start = date.fromisoformat(rules["coverage_start_date"])
    cov_end = date.fromisoformat(rules["coverage_end_date"])

    if local_date < cov_start or local_date > cov_end:
        return _unknown_session(venue, local_date.isoformat(), rules, spec_digest, "out_of_range")

    for offset in range(15):
        cand = local_date - timedelta(days=offset)
        if cand < cov_start:
            break
        sess = resolve_session(venue, cand.isoformat(), spec=loaded)
        close_utc = sess.get("close_utc")
        if sess.get("is_trading_day") and not sess.get("blocks_eligibility") and close_utc is not None:
            if close_utc <= instant_ms_utc:
                return sess

    return _unknown_session(venue, local_date.isoformat(), rules, spec_digest, "out_of_range")


def resolve_previous_trading_date(
    spec: dict | None,
    venue: str,
    session_date: str,
) -> str | None:
    """Return the immediately preceding trading date strictly before session_date, or None."""
    loaded = spec if spec is not None else load_spec()
    rules = loaded["rules"]
    if venue != rules["exchange"]:
        return None
    try:
        cur = date.fromisoformat(session_date)
    except ValueError:
        return None
    cov_start = date.fromisoformat(rules["coverage_start_date"])
    cov_end = date.fromisoformat(rules["coverage_end_date"])
    if cur <= cov_start or cur > cov_end:
        return None
    cand = cur - timedelta(days=1)
    while cand >= cov_start:
        sess = resolve_session(venue, cand.isoformat(), spec=loaded)
        if sess.get("is_trading_day") and not sess.get("blocks_eligibility"):
            return cand.isoformat()
        cand -= timedelta(days=1)
    return None


def classify_us_equity_session_window(
    timestamp_utc: str,
    calendar_session: Mapping[str, object],
) -> str:
    """Classify a UTC timestamp into core RTH vs US 23/5 extended/overnight session windows."""
    tz = ZoneInfo(str(calendar_session.get("timezone", "America/New_York")))
    iso_norm = timestamp_utc[:-1] + "+00:00" if timestamp_utc.endswith("Z") else timestamp_utc
    dt_parsed = datetime.fromisoformat(iso_norm)
    if dt_parsed.tzinfo is None:
        raise ValueError(f"timezone_naive_timestamp:{timestamp_utc!r}")
    dt_local = dt_parsed.astimezone(tz)
    local_date_str = dt_local.date().isoformat()
    local_t = dt_local.time()
    trade_date = str(calendar_session.get("trade_date", ""))

    if local_t >= time(20, 0) or local_t < time(4, 0):
        return "extended_overnight_open"
    if not calendar_session.get("is_trading_day") or calendar_session.get("blocks_eligibility"):
        return "closed"
    if local_date_str > trade_date:
        return "next_trade_date_window"
    if local_date_str < trade_date:
        return "closed"

    close_hhmm = str(calendar_session.get("close_local") or "16:00")
    ch, cm = map(int, close_hhmm.split(":"))
    close_t = time(ch, cm)
    if time(9, 30) <= local_t <= close_t:
        return "core_session_open"
    if close_t < local_t < time(20, 0):
        return "after_hours_extended"
    if time(4, 0) <= local_t < time(9, 30):
        return "pre_market_extended"
    return "closed"


def is_official_close_bar_timestamp(
    timestamp_utc: str,
    calendar_session: Mapping[str, object],
) -> tuple[bool, str | None]:
    """Validate that a market print timestamp belongs to core RTH and not 23/5 overnight/T+1."""
    tz = ZoneInfo(str(calendar_session.get("timezone", "America/New_York")))
    iso_norm = timestamp_utc[:-1] + "+00:00" if timestamp_utc.endswith("Z") else timestamp_utc
    try:
        dt_parsed = datetime.fromisoformat(iso_norm)
        if dt_parsed.tzinfo is None:
            return False, "invalid_bar_timestamp"
        dt_local = dt_parsed.astimezone(tz)
    except ValueError:
        return False, "invalid_bar_timestamp"

    trade_date = str(calendar_session.get("trade_date", ""))
    local_date_str = dt_local.date().isoformat()
    if local_date_str > trade_date:
        return False, "next_trade_date_contamination"
    if local_date_str < trade_date:
        return False, "session_not_in_payload"

    if not calendar_session.get("is_trading_day") or calendar_session.get("blocks_eligibility"):
        return False, "non_trading_session"

    close_hhmm = str(calendar_session.get("close_local") or "16:00")
    ch, cm = map(int, close_hhmm.split(":"))
    close_t = time(ch, cm)
    local_t = dt_local.time()
    if local_t >= time(21, 0):
        return False, "next_trade_date_contamination"
    if local_t < time(9, 30) or local_t > close_t:
        return False, "extended_hours_overnight_rejected"
    return True, None
