"""Canonical executable and effective-configuration manifest helpers (C7 policy identity)."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEDULER_CONFIG_PATH = REPO_ROOT / "config" / "drill-scheduler.json"

RUNTIME_MODULE_FILES: tuple[str, ...] = (
    "src/shadow/__init__.py",
    "src/shadow/calendar_consumer.py",
    "src/shadow/cloudrun_job.py",
    "src/shadow/evaluator.py",
    "src/shadow/feeder.py",
    "src/shadow/feeder_live.py",
    "src/shadow/fuse_adapter.py",
    "src/shadow/ledger.py",
    "src/shadow/manifest.py",
    "src/shadow/ots.py",
    "src/shadow/replay.py",
    "src/shadow/scheduler.py",
    "src/shadow/sh2_acceptance.py",
    "src/shadow/sizing.py",
)


def _normalize_config_value(val: object) -> object:
    if isinstance(val, float):
        d = Decimal(str(val))
        if d == 0:
            return "0"
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return "0" if s in ("", "-0") else s
    if isinstance(val, dict):
        return {str(k): _normalize_config_value(v) for k, v in sorted(val.items())}
    if isinstance(val, (list, tuple)):
        return [_normalize_config_value(v) for v in val]
    return val


def load_effective_runtime_config(
    repo_root: str | Path | None = None,
    *,
    config_path: str | Path | None = None,
) -> dict[str, object]:
    """Load effective scheduler/anchor parameters as float-free canonical values."""
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    cfg_file = Path(config_path) if config_path is not None else (root / "config" / "drill-scheduler.json")
    if not cfg_file.is_file():
        raise FileNotFoundError(f"missing_runtime_config:{cfg_file}")
    raw = json.loads(cfg_file.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"invalid_runtime_config_type:{type(raw).__name__}")
    keys = (
        "schema_version",
        "chain_id",
        "mode",
        "clk01_active",
        "production_mode_forbidden",
        "production_chain_locked",
        "deployment_target",
        "cloudrun_region",
        "venue",
        "calendar_version",
        "calendar_spec_digest",
        "fuse_spec_version",
        "fuse_engine_hash",
        "decision_instant_et",
        "half_day_run_time_et",
        "decision_cutoff_et",
        "decision_cutoff_half_day_et",
        "commitment_deadline_et",
        "treasury_endpoint",
        "yahoo_symbol_map",
        "ots_calendars",
        "ots_timeout_seconds",
        "max_footprint_bytes",
        "log_retention_days",
        "log_max_bytes",
        "resource_caps",
    )
    return {k: _normalize_config_value(raw[k]) for k in keys if k in raw}


def load_calendar_manifest_identity(
    repo_root: str | Path | None = None,
    *,
    calendar_version: str = "csa22-v1",
) -> dict[str, str]:
    """Return pinned calendar_version and spec_digest from specs/<calendar_version>/manifest.json."""
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    manifest_path = root / "specs" / calendar_version / "manifest.json"
    if not manifest_path.is_file():
        return {"calendar_version": calendar_version, "spec_digest": ""}
    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "calendar_version": str(data.get("calendar_version", calendar_version)),
        "spec_digest": str(data.get("spec_digest", "")),
    }


def compute_code_manifest_hash(
    repo_root: str | Path | None = None,
    *,
    calendar_version: str | None = None,
    config_path: str | Path | None = None,
) -> str:
    """Compute SHA-256 over all runtime Python modules, scheduler config, and calendar manifest."""
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    cfg_file = Path(config_path) if config_path is not None else (root / "config" / "drill-scheduler.json")
    cal_ver = calendar_version
    if cal_ver is None and cfg_file.is_file():
        cal_ver = str(json.loads(cfg_file.read_text(encoding="utf-8")).get("calendar_version", "csa22-v1"))
    cal_ver = cal_ver or "csa22-v1"

    targets: list[tuple[str, Path]] = [(rel, root / rel) for rel in RUNTIME_MODULE_FILES]
    cfg_rel = "config/drill-scheduler.json" if config_path is None else f"config/{cfg_file.name}"
    targets.append((cfg_rel, cfg_file))
    targets.append((f"specs/{cal_ver}/manifest.json", root / "specs" / cal_ver / "manifest.json"))

    h = hashlib.sha256()
    for rel, path in targets:
        if path.is_file():
            content = path.read_bytes()
            h.update(rel.encode("utf-8"))
            h.update(b"\x00")
            h.update(len(content).to_bytes(8, "big"))
            h.update(content)
    return h.hexdigest()
