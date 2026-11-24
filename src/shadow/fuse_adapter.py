"""Digest-pinned vendored FUSE-1 consumer adapter and entry-gate verifier."""

from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import marshal
import re
import sys
import types
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

from shadow.evaluator import ALL_BREAKER_IDS, REQUIRED_MARKET_SERIES, StubEvaluator
from shadow.sizing import TIER_CONFIGS, canonical_decimal_str

PINNED_FUSE_SPEC_VERSION = "fuse-eval-spec/v1"
PINNED_FUSE_ENGINE_HASH = (
    "12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45"
)
PINNED_FUSE_BUNDLE_HASH = (
    "78fed9dfc9b0b0a618fce6bb5a956063135f34845545e6a5154a32e119354cc9"
)
PINNED_FUSE_FREEZE_HASH = (
    "1da74ca508161ba6d9a424101321381a9f5156d8cb2c84c03be2fd2fe3f22a74"
)
PINNED_CSA22_V2_DIGEST = (
    "2ba8cd7684c361209df68bfa2e10004c1f9922a645c7ec55d421fda520561798"
)
PINNED_SH1_COMMIT = "20be23f1ce4f4f23bd2f863aef43213894947bf3"

EXPECTED_ENGINE_FILES: tuple[str, ...] = (
    "__init__.py",
    "canonical.py",
    "canonical_state.py",
    "evaluator.py",
    "rows_loss.py",
    "rows_ops.py",
    "rows_vehicle.py",
    "state.py",
)
_EXPECTED_ENGINE_STEMS = frozenset(Path(name).stem for name in EXPECTED_ENGINE_FILES)
_PYCACHE_RE = re.compile(
    r"^([A-Za-z0-9_]+)\.(cpython-\d+(?:t)?)(?:\.opt-([12]))?\.pyc$"
)

DEFAULT_VENDOR_ENGINE_ROOT = (
    Path(__file__).resolve().parents[2]
    / "vendor"
    / "fuse_engine"
    / "fuse-eval-spec-v1"
    / PINNED_FUSE_ENGINE_HASH
)

_STATE_PRECEDENCE = {
    "NORMAL": 0,
    "NO-NEW-RISK": 1,
    "DE-RISK": 2,
    "ORDER-UNKNOWN": 3,
    "HALT": 4,
}

_TIER_D_MAP = {
    "conservative": 0.15,
    "balanced": 0.30,
    "aggressive": 0.50,
}

_LOADED_PACKAGE_CACHE: dict[tuple[str, str], types.ModuleType] = {}


class EngineIdentityError(RuntimeError):
    """Raised on any mismatch in vendored FUSE-1 identity, hash, files, or bytecode."""


class ProductionChainLockedError(RuntimeError):
    """Raised when production forward-clock writes are attempted while locked (OA-SH2-2)."""


class V2PayloadSchemaError(ValueError):
    """Raised when V2 reconciliation payload omits or mis-types boolean hwm_adjusted."""


def is_stub_evaluator(evaluator: object) -> bool:
    """Return True if evaluator is a synthetic stub or lacks production capability."""
    if evaluator is None or isinstance(evaluator, StubEvaluator):
        return True
    if getattr(evaluator, "production_clock_capable", False) is not True:
        return True
    ehash = getattr(evaluator, "engine_hash", "")
    return not isinstance(ehash, str) or not ehash or ehash.startswith("stub-")


def resolve_aggregate_control_state(effective_state_by_scope: Mapping[str, str]) -> str:
    """Resolve most restrictive state across scopes (supports native or aggregated maps)."""
    if not isinstance(effective_state_by_scope, Mapping) or not effective_state_by_scope:
        return "NORMAL"
    if "aggregate" in effective_state_by_scope:
        val = str(effective_state_by_scope["aggregate"])
        if val in _STATE_PRECEDENCE:
            return val
    best_state = "NORMAL"
    best_rank = 0
    for state in effective_state_by_scope.values():
        rank = _STATE_PRECEDENCE.get(str(state))
        if rank is None:
            raise EngineIdentityError(f"Unrecognized control state in scope map: {state!r}")
        if rank > best_rank:
            best_rank = rank
            best_state = str(state)
    return best_state


def _sanitize_for_canonical_ledger(val: object) -> object:
    """Recursively convert finite floats in FUSE-1 breaker diagnostics into canonical decimal strings."""
    if val is None or isinstance(val, (bool, str, int)):
        return val
    if isinstance(val, Decimal):
        return canonical_decimal_str(val)
    if isinstance(val, float):
        return canonical_decimal_str(Decimal(str(val)))
    if isinstance(val, dict):
        return {str(k): _sanitize_for_canonical_ledger(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_sanitize_for_canonical_ledger(item) for item in val]
    return val


def serialize_breakers_for_ledger(rows_by_id: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Serialize FUSE-1 rows_by_ID into sorted C-SH-LEDGER 24-breaker list without floats."""
    actual_ids = set(rows_by_id.keys())
    expected_ids = set(ALL_BREAKER_IDS)
    if actual_ids != expected_ids:
        raise EngineIdentityError(
            f"F1-A01 breaker ID-set mismatch: expected={sorted(expected_ids)}, got={sorted(actual_ids)}"
        )
    out: list[dict[str, Any]] = []
    for bid in ALL_BREAKER_IDS:
        row = dict(rows_by_id[bid])
        if "id" in row and row["id"] != bid:
            raise EngineIdentityError(f"Breaker row id mismatch for {bid}: {row['id']!r}")
        sanitized = _sanitize_for_canonical_ledger(row)
        assert isinstance(sanitized, dict)
        sanitized["id"] = bid
        out.append(sanitized)
    return out


def _verify_pyc_matches_source(
    pyc_path: Path,
    src_path: Path,
    src_bytes: bytes,
    rel: str,
    cache_tag: str,
    optimize: int,
) -> None:
    pyc_bytes = pyc_path.read_bytes()
    if len(pyc_bytes) < 16 or pyc_bytes[:4] != importlib.util.MAGIC_NUMBER:
        raise EngineIdentityError(f"Invalid .pyc magic header in vendored engine: {rel!r}")
    if sys.implementation.cache_tag and cache_tag != sys.implementation.cache_tag:
        raise EngineIdentityError(f"Unexpected .pyc cache tag in vendored engine: {rel!r}")

    bitfield = int.from_bytes(pyc_bytes[4:8], "little")
    if bitfield & ~0b11:
        raise EngineIdentityError(f"Invalid .pyc bitfield in vendored engine: {rel!r}")
    if bitfield & 0b01:
        if pyc_bytes[8:16] != importlib.util.source_hash(src_bytes):
            raise EngineIdentityError(f"Tampered hash-based .pyc in vendored engine: {rel!r}")
    else:
        pyc_size = int.from_bytes(pyc_bytes[12:16], "little")
        if pyc_size != (len(src_bytes) & 0xFFFFFFFF):
            raise EngineIdentityError(f"Mismatched .pyc source size in vendored engine: {rel!r}")

    try:
        pyc_code = marshal.loads(pyc_bytes[16:])
    except Exception as exc:
        raise EngineIdentityError(f"Corrupt .pyc code payload in vendored engine: {rel!r}") from exc

    if not isinstance(pyc_code, types.CodeType) or not isinstance(pyc_code.co_filename, str):
        raise EngineIdentityError(f"Invalid .pyc code object in vendored engine: {rel!r}")
    norm_fn = pyc_code.co_filename.replace("\\", "/")
    if norm_fn != src_path.name and not norm_fn.endswith("/" + src_path.name):
        raise EngineIdentityError(f"Unexpected .pyc co_filename in vendored engine: {rel!r}")

    expected_code = compile(src_bytes, pyc_code.co_filename, "exec", optimize=optimize)
    if marshal.dumps(pyc_code) != marshal.dumps(expected_code):
        raise EngineIdentityError(f"Tampered .pyc bytecode mismatch in vendored engine: {rel!r}")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_vendored_engine_bundle(
    vendor_root: str | Path | None = None,
    *,
    expected_engine_hash: str = PINNED_FUSE_ENGINE_HASH,
    expected_spec_version: str = PINNED_FUSE_SPEC_VERSION,
    require_read_only: bool = False,
) -> dict[str, Any]:
    """Verify vendored FUSE-1 source files, provenance SHA-256s, framed digest, and __pycache__."""
    root = Path(vendor_root) if vendor_root is not None else DEFAULT_VENDOR_ENGINE_ROOT
    if root.is_symlink() or not root.is_dir():
        raise EngineIdentityError(f"Vendored engine root missing or invalid: {root}")

    engine_dir = root / "fuse_engine"
    metadata_dir = root / "metadata"
    if engine_dir.is_symlink() or not engine_dir.is_dir():
        raise EngineIdentityError(f"Vendored engine directory missing or symlinked: {engine_dir}")
    if metadata_dir.is_symlink() or not metadata_dir.is_dir():
        raise EngineIdentityError(f"Vendored metadata directory missing or symlinked: {metadata_dir}")

    prov_path = metadata_dir / "provenance.json"
    if prov_path.is_symlink() or not prov_path.is_file():
        raise EngineIdentityError(f"Missing vendored provenance.json: {prov_path}")
    provenance = json.loads(prov_path.read_text(encoding="utf-8"))
    if provenance.get("spec_version") != expected_spec_version:
        raise EngineIdentityError(
            f"provenance spec_version mismatch: expected {expected_spec_version!r}, got {provenance.get('spec_version')!r}"
        )
    if provenance.get("engine_hash") != expected_engine_hash:
        raise EngineIdentityError(
            f"provenance engine_hash mismatch: expected {expected_engine_hash!r}, got {provenance.get('engine_hash')!r}"
        )

    py_files: list[Path] = []
    rel_py_paths: list[str] = []
    pyc_entries: list[tuple[Path, str, str, str, int]] = []
    pycache_present = False

    for entry in sorted(engine_dir.rglob("*"), key=lambda p: p.relative_to(engine_dir).as_posix()):
        rel = entry.relative_to(engine_dir).as_posix()
        if entry.is_symlink():
            raise EngineIdentityError(f"Unexpected symlink in vendored engine dir: {rel!r}")
        if entry.is_dir():
            if rel != "__pycache__":
                raise EngineIdentityError(f"Unexpected subdirectory in vendored engine dir: {rel!r}")
            pycache_present = True
            continue
        if not entry.is_file():
            raise EngineIdentityError(f"Unexpected non-regular entry in vendored engine dir: {rel!r}")
        if entry.parent == engine_dir:
            if rel not in EXPECTED_ENGINE_FILES:
                raise EngineIdentityError(f"Unexpected artifact in vendored engine dir: {rel!r}")
            py_files.append(entry)
            rel_py_paths.append(rel)
        elif entry.parent == engine_dir / "__pycache__":
            pycache_present = True
            match = _PYCACHE_RE.match(entry.name)
            if not match or match.group(1) not in _EXPECTED_ENGINE_STEMS:
                raise EngineIdentityError(f"Unexpected __pycache__ artifact: {rel!r}")
            opt = int(match.group(3)) if match.group(3) else 0
            pyc_entries.append((entry, rel, match.group(1), match.group(2), opt))
        else:
            raise EngineIdentityError(f"Unexpected nested artifact in vendored engine dir: {rel!r}")

    if rel_py_paths != list(EXPECTED_ENGINE_FILES):
        raise EngineIdentityError(
            f"Vendored engine file set mismatch: expected {list(EXPECTED_ENGINE_FILES)}, got {rel_py_paths}"
        )

    prov_engine_files = provenance.get("engine_files")
    if not isinstance(prov_engine_files, dict) or set(prov_engine_files.keys()) != set(EXPECTED_ENGINE_FILES):
        raise EngineIdentityError("provenance engine_files map missing expected entries")

    src_by_stem: dict[str, tuple[Path, bytes]] = {}
    actual_file_hashes: dict[str, dict[str, Any]] = {}
    hasher = hashlib.sha256()
    read_only_ok = True

    for py_file, rel_path in zip(py_files, rel_py_paths):
        data = py_file.read_bytes()
        digest = _sha256_bytes(data)
        recorded = prov_engine_files.get(rel_path)
        if (
            not isinstance(recorded, dict)
            or recorded.get("sha256") != digest
            or recorded.get("bytes") != len(data)
        ):
            raise EngineIdentityError(
                f"Vendored engine file hash mismatch for {rel_path}: actual={digest}"
            )
        if py_file.stat().st_mode & 0o222:
            read_only_ok = False
            if require_read_only:
                raise EngineIdentityError(f"Vendored engine file is writable (expected read-only): {rel_path}")
        src_by_stem[py_file.stem] = (py_file, data)
        actual_file_hashes[rel_path] = {"sha256": digest, "bytes": len(data)}
        rel_bytes = rel_path.encode("utf-8")
        hasher.update(len(rel_bytes).to_bytes(4, "big"))
        hasher.update(rel_bytes)
        hasher.update(len(data).to_bytes(8, "big"))
        hasher.update(data)

    for pyc_file, rel, stem, cache_tag, opt in pyc_entries:
        py_file, src_bytes = src_by_stem[stem]
        _verify_pyc_matches_source(pyc_file, py_file, src_bytes, rel, cache_tag, opt)

    computed_engine_hash = hasher.hexdigest()
    if computed_engine_hash != expected_engine_hash:
        raise EngineIdentityError(
            f"Vendored engine framed SHA-256 mismatch: expected={expected_engine_hash}, got={computed_engine_hash}"
        )

    prov_meta_files = provenance.get("metadata_files")
    if not isinstance(prov_meta_files, dict) or not prov_meta_files:
        raise EngineIdentityError("provenance metadata_files map missing")
    for fname, recorded in prov_meta_files.items():
        mpath = metadata_dir / fname
        if mpath.is_symlink() or not mpath.is_file():
            raise EngineIdentityError(f"Missing vendored metadata file: {fname}")
        mbytes = mpath.read_bytes()
        mdigest = _sha256_bytes(mbytes)
        if (
            not isinstance(recorded, dict)
            or recorded.get("sha256") != mdigest
            or recorded.get("bytes") != len(mbytes)
        ):
            raise EngineIdentityError(f"Vendored metadata digest mismatch for {fname}")
        if mpath.stat().st_mode & 0o222:
            read_only_ok = False
            if require_read_only:
                raise EngineIdentityError(f"Vendored metadata file is writable: {fname}")

    return {
        "vendor_root": str(root),
        "engine_dir": str(engine_dir),
        "metadata_dir": str(metadata_dir),
        "spec_version": expected_spec_version,
        "engine_hash": computed_engine_hash,
        "bundle_hash": provenance.get("bundle_hash"),
        "freeze_hash": provenance.get("freeze_hash"),
        "file_hashes": actual_file_hashes,
        "pycache_present": pycache_present,
        "pycache_verified": True,
        "read_only_verified": read_only_ok,
    }


def _load_verified_fuse_package(
    vendor_root: str | Path | None = None,
    *,
    expected_engine_hash: str = PINNED_FUSE_ENGINE_HASH,
    expected_spec_version: str = PINNED_FUSE_SPEC_VERSION,
    require_read_only: bool = False,
) -> types.ModuleType:
    """Verify vendored engine dir on disk and return isolated imported package."""
    info = verify_vendored_engine_bundle(
        vendor_root,
        expected_engine_hash=expected_engine_hash,
        expected_spec_version=expected_spec_version,
        require_read_only=require_read_only,
    )
    engine_dir = Path(info["engine_dir"]).resolve()
    cache_key = (str(engine_dir), info["engine_hash"])
    cached = _LOADED_PACKAGE_CACHE.get(cache_key)
    if cached is not None:
        if getattr(cached, "__version__", None) != expected_spec_version:
            raise EngineIdentityError("Cached vendored module spec_version mismatch")
        if cached.evaluator.compute_engine_hash() != expected_engine_hash:
            raise EngineIdentityError("Cached vendored module engine_hash mismatch")
        return cached

    pkg_suffix = hashlib.sha256(f"{engine_dir}:{info['engine_hash']}".encode("utf-8")).hexdigest()[:16]
    pkg_name = f"_shadow_vendored_fuse_{pkg_suffix}"
    init_path = engine_dir / "__init__.py"
    spec = importlib.util.spec_from_file_location(
        pkg_name,
        init_path,
        submodule_search_locations=[str(engine_dir)],
    )
    if spec is None or spec.loader is None:
        raise EngineIdentityError(f"Unable to build import spec for vendored engine: {init_path}")

    module = importlib.util.module_from_spec(spec)
    prev_dont_write = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        sys.modules[pkg_name] = module
        spec.loader.exec_module(module)
    except Exception:
        for key in list(sys.modules.keys()):
            if key == pkg_name or key.startswith(pkg_name + "."):
                sys.modules.pop(key, None)
        raise
    finally:
        sys.dont_write_bytecode = prev_dont_write

    if getattr(module, "__version__", None) != expected_spec_version:
        raise EngineIdentityError(
            f"Loaded FUSE-1 spec_version mismatch: expected {expected_spec_version!r}, got {getattr(module, '__version__', None)!r}"
        )
    actual_hash = module.evaluator.compute_engine_hash()
    if actual_hash != expected_engine_hash:
        raise EngineIdentityError(
            f"Loaded FUSE-1 compute_engine_hash mismatch: expected {expected_engine_hash!r}, got {actual_hash!r}"
        )
    if set(module.evaluator.ROW_IDS) != set(ALL_BREAKER_IDS) or len(module.evaluator.ROW_IDS) != 24:
        raise EngineIdentityError("Loaded FUSE-1 ROW_IDS fails F1-A01 24-ID set equality")

    _LOADED_PACKAGE_CACHE[cache_key] = module
    return module


def verify_f1_entry_gates(
    vendor_root: str | Path | None = None,
    *,
    require_read_only: bool = False,
) -> dict[str, Any]:
    """Verify F1-A01/A03/A04/A10/A11 entry gates and archived SH-1 acceptance record."""
    bundle_info = verify_vendored_engine_bundle(
        vendor_root,
        require_read_only=require_read_only,
    )
    pkg = _load_verified_fuse_package(
        vendor_root,
        require_read_only=require_read_only,
    )
    metadata_dir = Path(bundle_info["metadata_dir"])

    spec_obj = json.loads((metadata_dir / "spec.json").read_text(encoding="utf-8"))
    result_bundle = json.loads((metadata_dir / "result-bundle.json").read_text(encoding="utf-8"))
    disp_register = json.loads((metadata_dir / "disposition-register.json").read_text(encoding="utf-8"))
    freeze_obj = json.loads((metadata_dir / "FREEZE.json").read_text(encoding="utf-8"))
    sh1_obj = json.loads((metadata_dir / "SH1-ACCEPTANCE.json").read_text(encoding="utf-8"))

    # F1-A01 exact 24-ID set equality
    spec_ids = [r.get("id") for r in spec_obj.get("rows", [])]
    if len(spec_ids) != 24 or len(set(spec_ids)) != 24 or set(spec_ids) != set(ALL_BREAKER_IDS):
        raise EngineIdentityError("F1-A01 spec.json ID-set mismatch")
    if set(pkg.evaluator.ROW_IDS) != set(ALL_BREAKER_IDS):
        raise EngineIdentityError("F1-A01 engine ROW_IDS ID-set mismatch")

    # F1-A03 / F1-A04 / F1-A10 result bundle checks
    if (
        result_bundle.get("spec_version") != PINNED_FUSE_SPEC_VERSION
        or result_bundle.get("engine_hash") != PINNED_FUSE_ENGINE_HASH
        or result_bundle.get("bundle_hash") != PINNED_FUSE_BUNDLE_HASH
        or result_bundle.get("freeze_hash") != PINNED_FUSE_FREEZE_HASH
        or result_bundle.get("enforcement_mode") != "shadow_would_have_tripped"
        or result_bundle.get("loss_breakers_armed") is not False
    ):
        raise EngineIdentityError("Pinned result-bundle.json identity or safety invariant mismatch")
    if freeze_obj.get("combined_freeze_hash") != PINNED_FUSE_FREEZE_HASH:
        raise EngineIdentityError("FREEZE.json combined_freeze_hash mismatch")

    base_cells = result_bundle.get("base_cells", {})
    tier_reps = result_bundle.get("tier_repetitions", {})
    succ_cases = result_bundle.get("successor_cases", {})
    if base_cells.get("total") != 264 or base_cells.get("passed") != 264 or base_cells.get("failed") != 0:
        raise EngineIdentityError(f"F1-A03 base_cells gate failed: {base_cells}")
    if tier_reps.get("total") != 132 or tier_reps.get("passed") != 132 or tier_reps.get("failed") != 0:
        raise EngineIdentityError(f"F1-A04 tier_repetitions gate failed: {tier_reps}")
    if succ_cases.get("total") != 13 or succ_cases.get("passed") != 13 or succ_cases.get("failed") != 0:
        raise EngineIdentityError(f"F1-A10 successor_cases gate failed: {succ_cases}")

    # F1-A11 disposition register verification
    entries = disp_register.get("entries", [])
    unresolved_rows = {
        e.get("row_id"): e.get("disposition")
        for e in entries
        if e.get("disposition") == "UNRESOLVED-FAIL-CLOSED"
    }
    if set(unresolved_rows.keys()) != {"V5", "V7", "O5"}:
        raise EngineIdentityError(
            f"F1-A11 unexpected unresolved disposition rows: {sorted(unresolved_rows.keys())}"
        )
    allowed_disps = {"CONFIRMED-AS-SPEC", "UNRESOLVED-FAIL-CLOSED"}
    unknown_disps = [
        (e.get("row_id"), e.get("disposition"))
        for e in entries
        if e.get("disposition") not in allowed_disps
    ]
    if unknown_disps:
        raise EngineIdentityError(f"F1-A11 unresolved/unrecognized disposition rows: {unknown_disps}")

    # SH1-ACCEPTANCE record check
    if (
        sh1_obj.get("sh1_commit") != PINNED_SH1_COMMIT
        or sh1_obj.get("csa22_v2_spec_digest") != PINNED_CSA22_V2_DIGEST
        or sh1_obj.get("offline_verification", {}).get("acceptance_offline_status") != "PASS_OFFLINE"
    ):
        raise EngineIdentityError("SH1-ACCEPTANCE.json failed pinned entry-gate checks")

    return {
        "ok": True,
        "spec_version": PINNED_FUSE_SPEC_VERSION,
        "engine_hash": bundle_info["engine_hash"],
        "bundle_hash": PINNED_FUSE_BUNDLE_HASH,
        "freeze_hash": PINNED_FUSE_FREEZE_HASH,
        "f1_a01_id_set_equal": True,
        "f1_a03_base_cells": f"{base_cells['passed']}/{base_cells['total']}",
        "f1_a04_tier_repetitions": f"{tier_reps['passed']}/{tier_reps['total']}",
        "f1_a10_successor_cases": f"{succ_cases['passed']}/{succ_cases['total']}",
        "f1_a11_unresolved_fail_closed": sorted(unresolved_rows.keys()),
        "sh1_commit": sh1_obj["sh1_commit"],
        "csa22_v2_spec_digest": sh1_obj["csa22_v2_spec_digest"],
        "read_only_verified": bundle_info["read_only_verified"],
        "pycache_verified": bundle_info["pycache_verified"],
    }


def validate_v2_reconciliation_payload(validated_inputs: Mapping[str, Any]) -> None:
    """Enforce caller schema contract requiring explicit boolean hwm_adjusted on V2 reconciliation payloads."""
    if not isinstance(validated_inputs, Mapping):
        return
    if "hwm_adjusted" in validated_inputs and not isinstance(validated_inputs["hwm_adjusted"], bool):
        raise V2PayloadSchemaError(
            "Top-level hwm_adjusted must be explicit bool when present"
        )
    funds = validated_inputs.get("funds")
    if not isinstance(funds, Mapping):
        return
    for symbol in sorted(funds.keys()):
        fdata = funds[symbol]
        if not isinstance(fdata, Mapping):
            continue
        if "hwm_adjusted" in fdata and not isinstance(fdata["hwm_adjusted"], bool):
            raise V2PayloadSchemaError(
                f"Fund {symbol} hwm_adjusted must be explicit bool when present"
            )
        ca = fdata.get("corporate_action")
        if not isinstance(ca, Mapping):
            continue
        if "hwm_adjusted" in ca and not isinstance(ca["hwm_adjusted"], bool):
            raise V2PayloadSchemaError(
                f"Fund {symbol} corporate_action.hwm_adjusted must be explicit bool when present"
            )
        if ca.get("adjustment_complete") is True:
            if "hwm_adjusted" not in ca or not isinstance(ca.get("hwm_adjusted"), bool):
                raise V2PayloadSchemaError(
                    f"V2 reconciliation payload for {symbol} with adjustment_complete=True "
                    "requires explicit boolean hwm_adjusted"
                )


def _finite_number(raw: object) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        dec = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception:
        return None
    if not dec.is_finite():
        return None
    val = float(dec)
    return val


def _map_calendar_statuses(calendar_session: Mapping[str, Any] | None) -> tuple[str | None, str | None]:
    if not isinstance(calendar_session, Mapping):
        return None, None
    state = calendar_session.get("session_state")
    reason = str(calendar_session.get("reason") or "")
    disagreement = bool(calendar_session.get("venue_disagreement"))
    observed_venue = calendar_session.get("observed_venue_status")

    if state in ("regular", "half_day") and calendar_session.get("is_trading_day") and not calendar_session.get("blocks_eligibility"):
        cal_status = "open"
        exch_status = str(observed_venue) if disagreement and observed_venue is not None else "open"
    elif state in ("closed", "unscheduled_closure"):
        tok = "holiday" if "holiday" in reason.lower() else "closed"
        cal_status = tok
        exch_status = str(observed_venue) if disagreement and observed_venue is not None else tok
    elif disagreement:
        cal_status = "open" if state in ("regular", "half_day") else "closed"
        exch_status = str(observed_venue) if observed_venue is not None else "closed"
    else:
        return None, None
    return cal_status, exch_status


def build_fuse_validated_inputs(
    session_inputs: Mapping[str, Any],
    calendar_session: Mapping[str, Any] | None,
    *,
    policy_hash: str,
    session_date: str,
    as_of: str,
) -> dict[str, Any]:
    """Translate ShadowResearch session inputs into FUSE-1 validated_inputs without shims."""
    if not isinstance(session_inputs, Mapping):
        raise ValueError("session_inputs must be a mapping")

    explicit_fuse = session_inputs.get("fuse_inputs")
    if explicit_fuse is not None:
        if not isinstance(explicit_fuse, Mapping):
            raise ValueError("fuse_inputs must be a mapping when provided")
        merged = copy.deepcopy(dict(explicit_fuse))
        merged.setdefault("loaded_policy_hash", policy_hash)
        merged.setdefault("registered_policy_hash", policy_hash)
        return merged

    if any(
        k in session_inputs
        for k in ("bankroll", "high_water_mark", "source_records", "session_calendar_status")
    ):
        direct = copy.deepcopy(dict(session_inputs))
        direct.setdefault("tier", "balanced")
        direct.setdefault("loaded_policy_hash", policy_hash)
        direct.setdefault("registered_policy_hash", policy_hash)
        return direct

    tier = str(session_inputs.get("tier", "balanced"))
    tier_cfg = TIER_CONFIGS.get(tier, TIER_CONFIGS["balanced"])
    bankroll_val = _finite_number(session_inputs.get("E"))
    hwm_raw = session_inputs.get("high_water_mark", session_inputs.get("E_star", session_inputs.get("E")))
    hwm_val = _finite_number(hwm_raw)
    if hwm_val is None and bankroll_val is not None:
        hwm_val = bankroll_val

    cal_status, exch_status = _map_calendar_statuses(calendar_session)
    if "tier_joint_cap" in session_inputs:
        tier_joint_cap = _finite_number(session_inputs.get("tier_joint_cap"))
    elif bankroll_val is not None:
        tier_joint_cap = float(Decimal(str(tier_cfg["x_joint"])) * Decimal(str(bankroll_val)))
    else:
        tier_joint_cap = None
    if "max_loss_cap" in session_inputs:
        max_loss_cap = _finite_number(session_inputs.get("max_loss_cap"))
    else:
        max_loss_cap = tier_joint_cap

    raw_funds = session_inputs.get("funds") if isinstance(session_inputs.get("funds"), Mapping) else {}
    mkt_recs = session_inputs.get("market_records")
    req_series = tuple(session_inputs.get("required_series", REQUIRED_MARKET_SERIES))

    mkt_ok = True
    idx_ok = True
    cal_ok = cal_status in ("open", "closed", "holiday") and exch_status == cal_status and not bool((calendar_session or {}).get("venue_disagreement"))
    vol_ok = True

    if "market_records" in session_inputs:
        if not isinstance(mkt_recs, list):
            mkt_ok = False
            idx_ok = False
        else:
            by_inst = {
                r.get("instrument"): r
                for r in mkt_recs
                if isinstance(r, Mapping) and r.get("instrument")
            }
            cal_digest = calendar_session.get("spec_digest") if isinstance(calendar_session, Mapping) else None
            for inst in req_series:
                rec = by_inst.get(inst)
                elig = rec.get("eligibility") if isinstance(rec, Mapping) and isinstance(rec.get("eligibility"), Mapping) else {}
                bad_rec = (
                    rec is None
                    or rec.get("normalized_value") is None
                    or not elig.get("eligible")
                    or (cal_digest is not None and rec.get("calendar_spec_digest") != cal_digest)
                )
                if bad_rec:
                    if inst in ("XNDX", "SEMI", "QQQ"):
                        idx_ok = False
                    mkt_ok = False

    fuse_funds: dict[str, dict[str, Any]] = {}
    joint_exposure = 0.0
    for symbol in sorted(raw_funds.keys()):
        fdata = raw_funds[symbol]
        if not isinstance(fdata, Mapping):
            continue
        close_val = _finite_number(fdata.get("close_price"))
        q_pre_val = _finite_number(fdata.get("q_pre", 0))
        sigma_val = _finite_number(fdata.get("sigma_forecast"))
        if sigma_val is None:
            vol_ok = False
        if close_val is None or q_pre_val is None:
            mkt_ok = False
        pos_val = (close_val * q_pre_val) if (close_val is not None and q_pre_val is not None) else 0.0
        joint_exposure += pos_val
        long_run_vol = _finite_number(fdata.get("long_run_volatility"))
        if long_run_vol is None:
            long_run_vol = 0.24
        fund_entry: dict[str, Any] = {
            "symbol": symbol,
            "exposure": _finite_number(fdata.get("exposure")) if fdata.get("exposure") is not None else pos_val,
            "fund_return": _finite_number(fdata.get("fund_return")) if fdata.get("fund_return") is not None else 0.0,
            "index_return": _finite_number(fdata.get("index_return")) if fdata.get("index_return") is not None else 0.0,
            "modelled_fee": _finite_number(fdata.get("modelled_fee")) if fdata.get("modelled_fee") is not None else 0.0,
            "modelled_financing": _finite_number(fdata.get("modelled_financing")) if fdata.get("modelled_financing") is not None else 0.0,
            "forecast_volatility": sigma_val,
            "long_run_volatility": long_run_vol,
            "shortfall_observed": _finite_number(fdata.get("shortfall_observed")) if fdata.get("shortfall_observed") is not None else 0.0,
            "shortfall_predicted": _finite_number(fdata.get("shortfall_predicted")) if fdata.get("shortfall_predicted") is not None else 0.0,
            "carrying_cost_measured": _finite_number(fdata.get("carrying_cost_measured")) if fdata.get("carrying_cost_measured") is not None else 0.0,
            "excess_return_shrunk": _finite_number(fdata.get("excess_return_shrunk")) if fdata.get("excess_return_shrunk") is not None else 0.04,
            "quote_staleness": _finite_number(fdata.get("quote_staleness")) if fdata.get("quote_staleness") is not None else 0.0,
            "spread_width": _finite_number(fdata.get("spread_width")) if fdata.get("spread_width") is not None else 0.0,
            "trading_pause": bool(fdata.get("trading_pause", False)),
            "session_time": str(fdata.get("session_time", "regular")),
            "share_count": int(q_pre_val) if q_pre_val is not None else 0,
            "ledger_value": _finite_number(fdata.get("ledger_value")) if fdata.get("ledger_value") is not None else pos_val,
            "broker_position": int(_finite_number(fdata.get("broker_position"))) if fdata.get("broker_position") is not None else (int(q_pre_val) if q_pre_val is not None else 0),
            "broker_value": _finite_number(fdata.get("broker_value")) if fdata.get("broker_value") is not None else pos_val,
            "corporate_action": copy.deepcopy(fdata.get("corporate_action")),
            "fund_event": copy.deepcopy(fdata.get("fund_event")),
        }
        if "v5_tolerance" in fdata:
            fund_entry["v5_tolerance"] = fdata["v5_tolerance"]
        if "v7_spread_width" in fdata:
            fund_entry["v7_spread_width"] = fdata["v7_spread_width"]
        fuse_funds[symbol] = fund_entry

    raw_source_records: dict[str, dict[str, Any] | None] = {
        "market_data": {"as_of": as_of, "age_sessions": 0} if mkt_ok else None,
        "index_returns": {"as_of": as_of, "age_sessions": 0} if idx_ok else None,
        "volatility": {"as_of": as_of, "age_sessions": 0} if vol_ok else None,
        "positions": {"as_of": as_of, "age_sessions": 0},
        "broker_reconciliation": {"as_of": as_of, "age_sessions": 0},
        "session_calendar": {"as_of": as_of, "age_sessions": 0} if cal_ok else None,
    }
    source_records = {k: v for k, v in raw_source_records.items() if v is not None}

    idx_returns = session_inputs.get("index_one_session_returns")
    if idx_returns is None and idx_ok:
        idx_returns = {"NDX": 0.0, "SPX": 0.0}

    out: dict[str, Any] = {
        "tier": tier,
        "bankroll": bankroll_val,
        "high_water_mark": hwm_val,
        "joint_exposure": _finite_number(session_inputs.get("joint_exposure")) if session_inputs.get("joint_exposure") is not None else joint_exposure,
        "tier_joint_cap": tier_joint_cap,
        "max_loss_cap": max_loss_cap,
        "proposed_exposure": _finite_number(session_inputs.get("proposed_exposure")) if session_inputs.get("proposed_exposure") is not None else joint_exposure,
        "one_session_loss": _finite_number(session_inputs.get("one_session_loss")) if session_inputs.get("one_session_loss") is not None else 0.0,
        "market_halt_level": session_inputs.get("market_halt_level", None),
        "index_one_session_returns": idx_returns,
        "funds": fuse_funds,
        "source_records": source_records,
        "session_calendar_status": cal_status,
        "exchange_status": exch_status,
        "positions_cash": _finite_number(session_inputs.get("positions_cash")) if session_inputs.get("positions_cash") is not None else bankroll_val,
        "broker_cash": _finite_number(session_inputs.get("broker_cash")) if session_inputs.get("broker_cash") is not None else bankroll_val,
        "reconciliation_tolerance": _finite_number(session_inputs.get("reconciliation_tolerance")) if session_inputs.get("reconciliation_tolerance") is not None else 1.0,
        "pending_orders": copy.deepcopy(session_inputs.get("pending_orders", {})),
        "symbol_whitelist": list(session_inputs.get("symbol_whitelist", ["TQQQ", "SOXL", "BIL"])),
        "account_whitelist": list(session_inputs.get("account_whitelist", ["stage_n_shadow"])),
        "settled_cash": _finite_number(session_inputs.get("settled_cash")) if session_inputs.get("settled_cash") is not None else bankroll_val,
        "session_order_count": int(session_inputs.get("session_order_count", 0)),
        "session_turnover": _finite_number(session_inputs.get("session_turnover")) if session_inputs.get("session_turnover") is not None else 0.0,
        "loaded_policy_hash": str(session_inputs.get("loaded_policy_hash", policy_hash)),
        "registered_policy_hash": str(session_inputs.get("registered_policy_hash", policy_hash)),
        "last_heartbeat": str(session_inputs.get("last_heartbeat", as_of)),
        "scheduled_interval": _finite_number(session_inputs.get("scheduled_interval")) if session_inputs.get("scheduled_interval") is not None else 60.0,
        "tier_loss_budget_D": _TIER_D_MAP.get(tier, 0.30),
        "v5_tolerance": session_inputs.get("v5_tolerance", None),
        "v7_spread_width": session_inputs.get("v7_spread_width", None),
        "o5_collar": session_inputs.get("o5_collar", None),
    }
    if "hwm_adjusted" in session_inputs:
        out["hwm_adjusted"] = session_inputs["hwm_adjusted"]
    if session_inputs.get("kill_switch_tripped") is True:
        out["kill_switch_tripped"] = True
    return out


class VendoredFuseEvaluator:
    """Real FUSE-1 vendored evaluator with runtime hash, spec, __pycache__, and V2 checks."""

    production_clock_capable: bool = True

    def __init__(
        self,
        vendor_root: str | Path | None = None,
        *,
        expected_engine_hash: str = PINNED_FUSE_ENGINE_HASH,
        expected_spec_version: str = PINNED_FUSE_SPEC_VERSION,
        require_read_only: bool = False,
    ) -> None:
        self.vendor_root = (
            Path(vendor_root) if vendor_root is not None else DEFAULT_VENDOR_ENGINE_ROOT
        )
        self.expected_engine_hash = expected_engine_hash
        self.expected_spec_version = expected_spec_version
        self.require_read_only = require_read_only
        self._pkg = _load_verified_fuse_package(
            self.vendor_root,
            expected_engine_hash=self.expected_engine_hash,
            expected_spec_version=self.expected_spec_version,
            require_read_only=self.require_read_only,
        )
        self.spec_version: str = self._pkg.__version__
        self.engine_hash: str = self._pkg.evaluator.compute_engine_hash()

    def verify_runtime_identity(self) -> dict[str, Any]:
        """Re-verify vendored disk artifacts, __pycache__, spec_version, and engine_hash."""
        info = verify_vendored_engine_bundle(
            self.vendor_root,
            expected_engine_hash=self.expected_engine_hash,
            expected_spec_version=self.expected_spec_version,
            require_read_only=self.require_read_only,
        )
        loaded_hash = self._pkg.evaluator.compute_engine_hash()
        if loaded_hash != self.expected_engine_hash or self._pkg.__version__ != self.expected_spec_version:
            raise EngineIdentityError(
                f"Runtime FUSE-1 identity mismatch: loaded=({self._pkg.__version__}, {loaded_hash})"
            )
        return info

    def evaluate(
        self,
        spec_version: str,
        engine_hash: str,
        policy_hash: str,
        session: str,
        as_of: str,
        mode: str,
        validated_inputs: dict,
        prior_row_state: dict | None,
        authorized_events: dict | list | None = None,
    ) -> tuple[dict[str, dict], dict[str, str], dict]:
        """Verify runtime bundle + V2 schema contract and invoke FUSE-1 evaluate() verbatim."""
        self.verify_runtime_identity()

        if spec_version != self.expected_spec_version:
            raise EngineIdentityError(
                f"Caller spec_version mismatch: expected {self.expected_spec_version!r}, got {spec_version!r}"
            )
        if engine_hash != self.expected_engine_hash:
            raise EngineIdentityError(
                f"Caller engine_hash mismatch: expected {self.expected_engine_hash!r}, got {engine_hash!r}"
            )

        validate_v2_reconciliation_payload(validated_inputs)

        auth_events_arg: dict | None
        if isinstance(authorized_events, dict) or authorized_events is None:
            auth_events_arg = authorized_events
        else:
            auth_events_arg = {"events": list(authorized_events)}

        return self._pkg.evaluator.evaluate(
            spec_version,
            engine_hash,
            policy_hash,
            session,
            as_of,
            mode,
            validated_inputs,
            prior_row_state,
            auth_events_arg,
        )
