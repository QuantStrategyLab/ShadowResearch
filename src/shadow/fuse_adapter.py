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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from shadow.evaluator import ALL_BREAKER_IDS, REQUIRED_MARKET_SERIES, StubEvaluator
from shadow.sizing import (
    INSTRUMENT_ORDER,
    TIER_CONFIGS,
    SizingInputError,
    canonical_decimal_str,
    compute_raw_vs_adjusted_separation,
    compute_tier_max_loss_fraction,
)

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


def resolve_account_and_fund_control_states(
    effective_state_by_scope: Mapping[str, str],
    fund_symbols: tuple[str, ...] | list[str] = ("SOXL", "TQQQ"),
) -> tuple[str, dict[str, str] | None, str]:
    """Return (account_state, per_fund_states_or_None, overall_aggregate_state)."""
    overall = resolve_aggregate_control_state(effective_state_by_scope)
    if not isinstance(effective_state_by_scope, Mapping) or "aggregate" in effective_state_by_scope:
        return overall, None, overall
    account_scopes = {
        k: v for k, v in effective_state_by_scope.items() if k in ("account", "joint")
    }
    account_state = (
        resolve_aggregate_control_state(account_scopes) if account_scopes else overall
    )
    fund_states: dict[str, str] = {}
    for sym in fund_symbols:
        sym_scopes = {
            k: v
            for k, v in effective_state_by_scope.items()
            if k in (f"fund:{sym}", f"symbol:{sym}")
        }
        fund_states[sym] = (
            resolve_aggregate_control_state(sym_scopes) if sym_scopes else "NORMAL"
        )
    return account_state, fund_states, overall


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


def _preserve_numeric_quantity(val: float | None) -> int | float | None:
    if val is None:
        return None
    fval = float(val)
    return int(fval) if fval.is_integer() else fval


def _map_calendar_statuses(
    calendar_session: Mapping[str, Any] | None,
    *,
    session_date: str | None = None,
) -> tuple[str | None, str | None]:
    if not isinstance(calendar_session, Mapping):
        return None, None
    if session_date is not None and "trade_date" in calendar_session:
        raw_td = calendar_session.get("trade_date")
        if raw_td is None or str(raw_td) != str(session_date):
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


_FUND_BENCHMARK_INDEX = {
    "TQQQ": "XNDX",
    "SOXL": "SEMI",
}

_FUND_EXPOSURE_BETA = {
    "TQQQ": Decimal("1"),
    "SOXL": Decimal("1.5"),
}

_LEVERAGE_MULTIPLE = Decimal("3")

_O5_PASSTHROUGH_KEYS = (
    "o5_symbol",
    "symbol",
    "o5_symbol_whitelist",
    "o5_order_size",
    "order_size",
    "o5_order_qty",
    "order_qty",
    "qty",
    "o5_side",
    "side",
    "order_side",
    "o5_order_value",
    "order_value",
    "o5_order_price",
    "o5_limit_price",
    "limit_price",
    "order_price",
    "unit_price",
    "o5_unit_price",
    "price",
    "o5_reference_price",
    "reference_price",
    "account",
    "o5_account",
    "o5_account_whitelist",
    "o5_settled_cash",
    "o5_max_order_size",
    "max_order_size",
    "o5_orders_today",
    "orders_today",
    "o5_max_orders_per_session",
    "max_orders_per_session",
    "o5_turnover_today",
    "turnover_today",
    "o5_max_turnover_per_session",
    "max_turnover_per_session",
)

_INDEX_RETURN_LABELS = (
    ("XNDX", "NDX"),
    ("SEMI", "SEMI"),
    ("SPX", "SPX"),
)


def _is_valid_sizing_signal(raw: object) -> bool:
    """Return True when raw decodes to a finite Decimal signal within sizing's [0, 1] contract."""
    if raw is None or isinstance(raw, bool):
        return False
    try:
        dec = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError):
        return False
    return dec.is_finite() and Decimal("0") <= dec <= Decimal("1")


def _compute_return_ratio(cur_raw: object, prev_raw: object) -> float | None:
    cur_f = _finite_number(cur_raw)
    prev_f = _finite_number(prev_raw)
    if cur_f is None or prev_f is None or prev_f <= 0.0:
        return None
    cur_d = Decimal(str(cur_raw))
    prev_d = Decimal(str(prev_raw))
    return float((cur_d - prev_d) / prev_d)


def _compute_ca_adjusted_fund_return(
    cur_raw: object,
    prev_raw: object,
    ca_obj: object,
) -> float | None:
    """Derive 1-session fund return from closes, adjusting prior close for splits/distributions."""
    if ca_obj is None:
        return _compute_return_ratio(cur_raw, prev_raw)
    if not isinstance(ca_obj, Mapping):
        return None
    cur_f = _finite_number(cur_raw)
    prev_f = _finite_number(prev_raw)
    if cur_f is None or prev_f is None or prev_f <= 0.0:
        return None
    cur_d = Decimal(str(cur_raw))
    prev_d = Decimal(str(prev_raw))
    ca_type = ca_obj.get("type")
    if ca_type in ("split", "reverse_split"):
        ratio_f = _finite_number(ca_obj.get("ratio"))
        if ratio_f is None or ratio_f <= 0.0:
            return None
        prev_adj = prev_d / Decimal(str(ca_obj.get("ratio")))
        if prev_adj <= 0:
            return None
        raw_cash = ca_obj.get("cash_per_share_usd")
        if raw_cash is None:
            cash_d = Decimal("0")
        else:
            cash_f = _finite_number(raw_cash)
            if cash_f is None or cash_f < 0.0:
                return None
            cash_d = Decimal(str(raw_cash))
        return float((cur_d + cash_d - prev_adj) / prev_adj)
    if ca_type in ("distribution", "cash_distribution", "dividend"):
        cash_f = _finite_number(ca_obj.get("cash_per_share_usd"))
        if cash_f is None or cash_f < 0.0:
            return None
        cash_d = Decimal(str(ca_obj.get("cash_per_share_usd")))
        return float((cur_d + cash_d - prev_d) / prev_d)
    return None


def _derive_one_session_loss(session_inputs: Mapping[str, Any]) -> float | None:
    """Derive L4 one_session_loss from prior_E / external_flow when omitted; return None if undeterminable."""
    if "one_session_loss" in session_inputs:
        return _finite_number(session_inputs.get("one_session_loss"))
    e_f = _finite_number(session_inputs.get("E"))
    if e_f is None or e_f <= 0.0:
        return None
    raw_ext = session_inputs.get("external_flow", "0")
    ext_f = _finite_number(raw_ext)
    if ext_f is None:
        return None
    raw_prior_e = session_inputs.get("prior_E")
    if raw_prior_e is not None:
        pe_f = _finite_number(raw_prior_e)
        if pe_f is None or pe_f <= 0.0:
            return None
        try:
            rva = compute_raw_vs_adjusted_separation(
                Decimal(str(session_inputs["E"])),
                Decimal(str(raw_prior_e)),
                Decimal(str(raw_ext)),
            )
            adj_str = rva.get("adjusted_return_pct")
            if not isinstance(adj_str, str):
                return None
            return float(max(Decimal("0"), -Decimal(adj_str)))
        except (SizingInputError, ArithmeticError, ValueError):
            return None
    if (
        _finite_number(session_inputs.get("E_star")) is not None
        and _finite_number(session_inputs.get("L")) is not None
    ):
        e_d = Decimal(str(session_inputs["E"]))
        es_d = Decimal(str(session_inputs["E_star"]))
        l_d = Decimal(str(session_inputs["L"]))
        ext_d = Decimal(str(raw_ext))
        if e_d == es_d and l_d == Decimal("0") and ext_d == Decimal("0"):
            return 0.0
    return None


def _map_market_corporate_action(mca: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(mca, Mapping):
        return None
    try:
        split_dec = Decimal(str(mca.get("split_ratio", "1")))
        cash_dec = Decimal(str(mca.get("cash_per_share_usd", "0")))
    except Exception:
        return None
    if not split_dec.is_finite() or not cash_dec.is_finite():
        return None
    if split_dec != Decimal("1"):
        action: dict[str, Any] = {
            "type": "split" if split_dec > Decimal("1") else "reverse_split",
            "ratio": float(split_dec),
            "ex_date": mca.get("ex_date"),
            "adjustment_complete": False,
            "exact_reconciliation": False,
        }
        if cash_dec > Decimal("0"):
            action["cash_per_share_usd"] = float(cash_dec)
        return action
    if cash_dec > Decimal("0"):
        return {
            "type": "distribution",
            "cash_per_share_usd": float(cash_dec),
            "ex_date": mca.get("ex_date"),
            "adjustment_complete": False,
            "exact_reconciliation": False,
        }
    return None


def _resolve_tier_quantity(raw_val: object, tier: str) -> float | None:
    """Resolve scalar or tier-keyed quantity before numeric coercion."""
    if isinstance(raw_val, Mapping):
        if tier not in raw_val:
            return None
        return _finite_number(raw_val[tier])
    return _finite_number(raw_val)


def _resolve_sizing_q_pre(raw_val: object, tier: str) -> int | None:
    """Resolve scalar or tier-keyed q_pre enforcing sizing._resolve_q_pre() integer contract."""
    cand = raw_val
    if isinstance(cand, Mapping):
        if tier not in cand:
            return None
        cand = cand[tier]
    if cand is None or isinstance(cand, (bool, float)):
        return None
    if isinstance(cand, int):
        return cand if cand >= 0 else None
    if isinstance(cand, str):
        s = cand.strip()
        if not s:
            return None
        try:
            val = int(s)
        except ValueError:
            return None
        return val if val >= 0 else None
    try:
        val = int(cand)  # type: ignore[call-overload]
    except Exception:
        return None
    return val if val >= 0 else None


def _sizing_decimal(raw: object) -> Decimal | None:
    """Parse a finite Decimal matching sizing._to_decimal()'s strict bankroll contract."""
    if raw is None or isinstance(raw, (bool, float)):
        return None
    try:
        dec = raw if isinstance(raw, Decimal) else Decimal(str(raw))
    except Exception:
        return None
    return dec if dec.is_finite() else None


def _has_valid_sizing_bankroll(session_inputs: Mapping[str, Any]) -> bool:
    """Return True when SHADOW inputs satisfy compute_session_sizing()'s required bankroll contract."""
    e_dec = _sizing_decimal(session_inputs.get("E"))
    e_star_dec = _sizing_decimal(session_inputs.get("E_star"))
    h_dec = _sizing_decimal(session_inputs.get("H", "0"))
    l_dec = _sizing_decimal(session_inputs.get("L"))
    if e_dec is None or e_star_dec is None or h_dec is None or l_dec is None:
        return False
    if e_dec <= 0 or e_star_dec <= 0 or e_dec > e_star_dec or h_dec < 0 or l_dec < 0:
        return False
    raw_w_ref = session_inputs.get("W_ref")
    if raw_w_ref is not None:
        w_ref_dec = _sizing_decimal(raw_w_ref)
        if w_ref_dec is None:
            return False
        w_ref_eff = w_ref_dec
    else:
        w_ref_eff = e_star_dec + h_dec
    return w_ref_eff - l_dec > 0


def _derive_default_max_loss_cap(
    session_inputs: Mapping[str, Any],
    tier: str,
    bankroll_val: float | None,
    hwm_val: float | None,
    *,
    require_sizing_bankroll: bool = True,
) -> float | None:
    """Derive L0 max_loss_cap from U-01 sizing Step 3 x_ML(c) * E, failing closed on bad inputs."""
    if bankroll_val is None or bankroll_val <= 0 or tier not in TIER_CONFIGS:
        return None
    if require_sizing_bankroll and not _has_valid_sizing_bankroll(session_inputs):
        return None
    try:
        e_dec = Decimal(str(session_inputs["E"]))
        raw_e_star = session_inputs.get("E_star")
        if raw_e_star is not None:
            if _finite_number(raw_e_star) is None:
                return None
            e_star_dec = Decimal(str(raw_e_star))
        elif not require_sizing_bankroll and hwm_val is not None:
            e_star_dec = Decimal(str(hwm_val))
        elif not require_sizing_bankroll:
            e_star_dec = e_dec
        else:
            return None

        raw_h = session_inputs.get("H", "0")
        if raw_h is None or _finite_number(raw_h) is None:
            return None
        h_dec = Decimal(str(raw_h))

        raw_w_ref = session_inputs.get("W_ref")
        w_ref_dec: Decimal | None = None
        if raw_w_ref is not None:
            if _finite_number(raw_w_ref) is None:
                return None
            w_ref_dec = Decimal(str(raw_w_ref))

        raw_l = session_inputs.get("L")
        if raw_l is not None:
            if _finite_number(raw_l) is None:
                return None
            l_dec = Decimal(str(raw_l))
        elif not require_sizing_bankroll:
            w_ref_eff = w_ref_dec if w_ref_dec is not None else (e_star_dec + h_dec)
            l_dec = max(Decimal("0"), w_ref_eff - (e_dec + h_dec))
        else:
            return None

        _, _, x_ml = compute_tier_max_loss_fraction(
            tier,
            e_val=e_dec,
            e_star=e_star_dec,
            l_val=l_dec,
            h_val=h_dec,
            w_ref=w_ref_dec,
        )
        return float(x_ml * e_dec)
    except (SizingInputError, ArithmeticError, ValueError, KeyError):
        return None


def _derive_conv_scale(
    session_inputs: Mapping[str, Any],
    bankroll_val: float | None,
    hwm_val: float | None,
) -> Decimal | None:
    """Compute U-01 Step 6 convexity denominator (1 + H / E*), failing closed on invalid inputs."""
    del bankroll_val
    try:
        raw_e_star = session_inputs.get("E_star")
        if raw_e_star is not None:
            if _finite_number(raw_e_star) is None:
                return None
            e_star_dec = Decimal(str(raw_e_star))
        elif hwm_val is not None:
            e_star_dec = Decimal(str(hwm_val))
        else:
            return None

        raw_h = session_inputs.get("H", "0")
        if raw_h is None or _finite_number(raw_h) is None:
            return None
        h_dec = Decimal(str(raw_h))
        if e_star_dec <= 0 or h_dec < 0:
            return None
        return Decimal("1") + (h_dec / e_star_dec)
    except (ArithmeticError, ValueError):
        return None


def _apply_explicit_payload_tier(
    payload: dict[str, Any],
    session_inputs: Mapping[str, Any],
    *,
    policy_hash: str,
) -> dict[str, Any]:
    """Align an explicit/direct FUSE payload with the outer tier request and tier-dependent caps."""
    has_outer_tier = "tier" in session_inputs and session_inputs.get("tier") is not None
    requested_tier = str(session_inputs["tier"]) if has_outer_tier else str(payload.get("tier", "balanced"))
    if has_outer_tier and requested_tier != payload.get("tier"):
        payload["tier"] = requested_tier
        if requested_tier in TIER_CONFIGS:
            payload["tier_loss_budget_D"] = _TIER_D_MAP[requested_tier]
            bankroll_val = _finite_number(payload.get("bankroll", session_inputs.get("E")))
            hwm_val = _finite_number(
                payload.get(
                    "high_water_mark",
                    session_inputs.get("high_water_mark", session_inputs.get("E_star", bankroll_val)),
                )
            )
            if bankroll_val is not None:
                payload["tier_joint_cap"] = float(
                    Decimal(str(TIER_CONFIGS[requested_tier]["x_joint"]))
                    * Decimal(str(bankroll_val))
                )
            if "E" in session_inputs:
                cap_inputs = session_inputs
            else:
                cap_inputs = {
                    "E": bankroll_val,
                    "E_star": hwm_val,
                    "H": payload.get("H", payload.get("h_value", session_inputs.get("H", "0"))),
                }
                if "L" in session_inputs:
                    cap_inputs["L"] = session_inputs["L"]
                elif payload.get("one_session_loss") is not None:
                    cap_inputs["L"] = payload["one_session_loss"]
            derived_cap = _derive_default_max_loss_cap(
                cap_inputs,
                requested_tier,
                bankroll_val,
                hwm_val,
                require_sizing_bankroll=("E" in session_inputs),
            )
            if derived_cap is not None:
                payload["max_loss_cap"] = derived_cap
    else:
        payload.setdefault("tier", "balanced")
    payload.setdefault("loaded_policy_hash", policy_hash)
    payload.setdefault("registered_policy_hash", policy_hash)
    return payload


def apply_fund_long_run_volatility(
    target_inputs: dict[str, Any],
    long_run_vol_by_inst: object | None,
) -> None:
    """Inject explicit per-fund long_run_volatility observations without mutating shared inputs."""
    if not isinstance(long_run_vol_by_inst, Mapping):
        return
    funds = target_inputs.get("funds")
    if not isinstance(funds, Mapping):
        return
    funds_copy: dict[str, object] = {}
    for inst, fdata in funds.items():
        if isinstance(fdata, Mapping):
            entry = dict(copy.deepcopy(fdata))
            if inst in long_run_vol_by_inst and long_run_vol_by_inst[inst] is not None:
                entry["long_run_volatility"] = long_run_vol_by_inst[inst]
            funds_copy[str(inst)] = entry
        else:
            funds_copy[str(inst)] = copy.deepcopy(fdata)
    target_inputs["funds"] = funds_copy


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
        return _apply_explicit_payload_tier(merged, session_inputs, policy_hash=policy_hash)

    if (
        "bankroll" in session_inputs
        and "session_calendar_status" in session_inputs
        and "E" not in session_inputs
    ):
        direct = copy.deepcopy(dict(session_inputs))
        return _apply_explicit_payload_tier(direct, session_inputs, policy_hash=policy_hash)

    tier = str(session_inputs.get("tier", "balanced"))
    tier_cfg = TIER_CONFIGS.get(tier, TIER_CONFIGS["balanced"])
    bankroll_val = _finite_number(session_inputs.get("E"))
    if "high_water_mark" in session_inputs:
        hwm_val = _finite_number(session_inputs.get("high_water_mark"))
    elif "E_star" in session_inputs:
        hwm_val = _finite_number(session_inputs.get("E_star"))
    else:
        hwm_val = None

    cal_status, exch_status = _map_calendar_statuses(calendar_session, session_date=session_date)
    if "tier_joint_cap" in session_inputs:
        tier_joint_cap = _finite_number(session_inputs.get("tier_joint_cap"))
    elif bankroll_val is not None:
        tier_joint_cap = float(Decimal(str(tier_cfg["x_joint"])) * Decimal(str(bankroll_val)))
    else:
        tier_joint_cap = None
    if not _has_valid_sizing_bankroll(session_inputs):
        max_loss_cap = None
    elif "max_loss_cap" in session_inputs:
        max_loss_cap = _finite_number(session_inputs.get("max_loss_cap"))
    else:
        max_loss_cap = _derive_default_max_loss_cap(session_inputs, tier, bankroll_val, hwm_val)

    raw_funds = session_inputs.get("funds") if isinstance(session_inputs.get("funds"), Mapping) else {}
    mkt_recs = session_inputs.get("market_records")
    req_series = tuple(session_inputs.get("required_series", REQUIRED_MARKET_SERIES))
    prior_closes = (
        session_inputs.get("prior_closes")
        if isinstance(session_inputs.get("prior_closes"), Mapping)
        else {}
    )
    by_inst: dict[str, Mapping[str, Any]] = {}

    mkt_ok = True
    idx_ok = True
    bad_market_insts: set[str] = set()
    bad_market_payload = False
    cal_ok = cal_status in ("open", "closed", "holiday") and exch_status == cal_status and not bool((calendar_session or {}).get("venue_disagreement"))
    vol_ok = True

    if "market_records" in session_inputs:
        if not isinstance(mkt_recs, list):
            mkt_ok = False
            idx_ok = False
            bad_market_payload = True
        else:
            by_inst = {
                str(r.get("instrument")): r
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
                    bad_market_insts.add(inst)
                    if inst in ("XNDX", "SEMI", "QQQ"):
                        idx_ok = False
                    mkt_ok = False

    # Derive current closes across instruments for return computation
    cur_closes: dict[str, object] = {}
    for inst_name, mrec in by_inst.items():
        if mrec.get("normalized_value") is not None:
            cur_closes[inst_name] = mrec["normalized_value"]
    cur_idx_override = session_inputs.get("current_index_closes")
    if isinstance(cur_idx_override, Mapping):
        for k, v in cur_idx_override.items():
            if v is not None:
                cur_closes[str(k)] = v

    conv_scale = _derive_conv_scale(session_inputs, bankroll_val, hwm_val)
    fuse_funds: dict[str, dict[str, Any]] = {}
    joint_exposure_dec: Decimal | None = Decimal("0") if conv_scale is not None else None
    for req_inst in INSTRUMENT_ORDER:
        if not isinstance(raw_funds.get(req_inst), Mapping):
            mkt_ok = False
    for symbol in sorted(raw_funds.keys()):
        fdata = raw_funds[symbol]
        if not isinstance(fdata, Mapping):
            continue
        close_val = _finite_number(fdata.get("close_price"))
        q_pre_val = _resolve_sizing_q_pre(fdata.get("q_pre", 0), tier)
        sigma_val = _finite_number(fdata.get("sigma_forecast"))
        signal_ok = _is_valid_sizing_signal(fdata.get("s"))
        if sigma_val is None or sigma_val <= 0.0:
            vol_ok = False
        if close_val is None or close_val <= 0.0 or q_pre_val is None or not signal_ok:
            mkt_ok = False
        pos_val = (close_val * q_pre_val) if (close_val is not None and q_pre_val is not None) else 0.0
        if joint_exposure_dec is not None and conv_scale is not None:
            beta_dec = _FUND_EXPOSURE_BETA.get(symbol, Decimal("1"))
            joint_exposure_dec += (
                _LEVERAGE_MULTIPLE * beta_dec * Decimal(str(pos_val))
            ) / conv_scale
        long_run_vol = _finite_number(fdata.get("long_run_volatility"))

        # Wire feeder market_records[].corporate_action into V2 when fund payload does not override it
        ca_obj = copy.deepcopy(fdata.get("corporate_action"))
        if (
            ca_obj is None
            and not bad_market_payload
            and symbol not in bad_market_insts
            and symbol in by_inst
        ):
            ca_obj = _map_market_corporate_action(
                by_inst[symbol].get("corporate_action")  # type: ignore[arg-type]
            )

        # Derive 1-session fund and benchmark index returns from real closes when not explicitly given
        if fdata.get("fund_return") is not None:
            fund_ret_val = _finite_number(fdata.get("fund_return"))
        elif (
            not bad_market_payload
            and symbol not in bad_market_insts
            and symbol in prior_closes
            and fdata.get("close_price") is not None
        ):
            fund_ret_val = _compute_ca_adjusted_fund_return(
                fdata.get("close_price"),
                prior_closes.get(symbol),
                ca_obj,
            )
        else:
            fund_ret_val = None

        bench_inst = _FUND_BENCHMARK_INDEX.get(symbol, "XNDX")
        if fdata.get("index_return") is not None:
            idx_ret_val = _finite_number(fdata.get("index_return"))
        elif (
            not bad_market_payload
            and bench_inst not in bad_market_insts
            and bench_inst in prior_closes
            and bench_inst in cur_closes
        ):
            idx_ret_val = _compute_return_ratio(cur_closes.get(bench_inst), prior_closes.get(bench_inst))
        else:
            idx_ret_val = None

        broker_pos_val = (
            _resolve_tier_quantity(fdata.get("broker_position"), tier)
            if "broker_position" in fdata and fdata.get("broker_position") is not None
            else q_pre_val
        )
        fund_entry: dict[str, Any] = {
            "symbol": symbol,
            "exposure": _finite_number(fdata.get("exposure")) if fdata.get("exposure") is not None else pos_val,
            "fund_return": fund_ret_val,
            "index_return": idx_ret_val,
            "modelled_fee": _finite_number(fdata.get("modelled_fee")),
            "modelled_financing": _finite_number(fdata.get("modelled_financing")),
            "forecast_volatility": sigma_val,
            "long_run_volatility": long_run_vol,
            "shortfall_observed": _finite_number(fdata.get("shortfall_observed")),
            "shortfall_predicted": _finite_number(fdata.get("shortfall_predicted")),
            "carrying_cost_measured": _finite_number(fdata.get("carrying_cost_measured")),
            "excess_return_shrunk": _finite_number(fdata.get("excess_return_shrunk")),
            "quote_staleness": _finite_number(fdata.get("quote_staleness")),
            "spread_width": _finite_number(fdata.get("spread_width")),
            "trading_pause": fdata.get("trading_pause"),
            "session_time": str(fdata.get("session_time", "regular")),
            "share_count": _preserve_numeric_quantity(q_pre_val) if q_pre_val is not None else 0,
            "ledger_value": _finite_number(fdata.get("ledger_value")) if fdata.get("ledger_value") is not None else pos_val,
            "broker_position": _preserve_numeric_quantity(broker_pos_val),
            "broker_value": _finite_number(fdata.get("broker_value")) if fdata.get("broker_value") is not None else pos_val,
            "corporate_action": ca_obj,
            "fund_event": copy.deepcopy(fdata.get("fund_event")),
        }
        if "hwm_adjusted" in fdata:
            fund_entry["hwm_adjusted"] = fdata["hwm_adjusted"]
        if "v5_tolerance" in fdata:
            fund_entry["v5_tolerance"] = fdata["v5_tolerance"]
        if "v7_spread_width" in fdata:
            fund_entry["v7_spread_width"] = fdata["v7_spread_width"]
        for v1_key in ("five_session_history", "sessions_window"):
            if v1_key in fdata:
                fund_entry[v1_key] = copy.deepcopy(fdata[v1_key])
        fuse_funds[symbol] = fund_entry

    idx_returns: dict[str, float] | None
    if "index_one_session_returns" in session_inputs:
        raw_idx_rets = session_inputs.get("index_one_session_returns")
        idx_returns = copy.deepcopy(raw_idx_rets) if isinstance(raw_idx_rets, dict) else raw_idx_rets  # type: ignore[assignment]
    elif not bad_market_payload and prior_closes:
        derived_idx: dict[str, float] = {}
        for inst_key, ret_label in _INDEX_RETURN_LABELS:
            if (
                inst_key not in bad_market_insts
                and inst_key in prior_closes
                and inst_key in cur_closes
            ):
                r_val = _compute_return_ratio(cur_closes.get(inst_key), prior_closes.get(inst_key))
                if r_val is not None:
                    derived_idx[ret_label] = r_val
        idx_returns = derived_idx if derived_idx else None
    else:
        idx_returns = None

    # Only populate positions / broker_reconciliation when caller supplies explicit records (fail closed otherwise)
    caller_sources = (
        session_inputs.get("source_records")
        if isinstance(session_inputs.get("source_records"), Mapping)
        else {}
    )
    pos_rec = (
        session_inputs.get("positions_source_record")
        if "positions_source_record" in session_inputs
        else caller_sources.get("positions")
    )
    brk_rec = (
        session_inputs.get("broker_reconciliation_source_record")
        if "broker_reconciliation_source_record" in session_inputs
        else caller_sources.get("broker_reconciliation")
    )
    has_all_fund_rets = bool(fuse_funds) and all(
        fuse_funds[sym].get("fund_return") is not None
        and fuse_funds[sym].get("index_return") is not None
        for sym in fuse_funds
    )
    raw_source_records: dict[str, dict[str, Any] | None] = {
        "market_data": (
            copy.deepcopy(caller_sources.get("market_data"))
            if "market_data" in caller_sources
            else {"as_of": as_of, "age_sessions": 0}
        )
        if (mkt_ok and idx_ok and vol_ok and has_all_fund_rets)
        else None,
        "index_returns": caller_sources.get("index_returns")
        if "index_returns" in caller_sources
        else ({"as_of": as_of, "age_sessions": 0} if (idx_ok and idx_returns is not None) else None),
        "volatility": caller_sources.get("volatility")
        if "volatility" in caller_sources
        else ({"as_of": as_of, "age_sessions": 0} if vol_ok else None),
        "positions": copy.deepcopy(pos_rec) if isinstance(pos_rec, Mapping) else None,
        "broker_reconciliation": copy.deepcopy(brk_rec) if isinstance(brk_rec, Mapping) else None,
        "session_calendar": (
            copy.deepcopy(caller_sources.get("session_calendar"))
            if "session_calendar" in caller_sources
            else {"as_of": as_of, "age_sessions": 0}
        )
        if cal_ok
        else None,
    }
    source_records = {k: v for k, v in raw_source_records.items() if v is not None}

    derived_joint_exposure = float(joint_exposure_dec) if joint_exposure_dec is not None else None
    out: dict[str, Any] = {
        "tier": tier,
        "bankroll": bankroll_val,
        "high_water_mark": hwm_val,
        "joint_exposure": _finite_number(session_inputs.get("joint_exposure")) if session_inputs.get("joint_exposure") is not None else derived_joint_exposure,
        "tier_joint_cap": tier_joint_cap,
        "max_loss_cap": max_loss_cap,
        "proposed_exposure": _finite_number(session_inputs.get("proposed_exposure")) if session_inputs.get("proposed_exposure") is not None else derived_joint_exposure,
        "one_session_loss": _derive_one_session_loss(session_inputs),
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
        "symbol_whitelist": (
            copy.deepcopy(session_inputs["symbol_whitelist"])
            if "symbol_whitelist" in session_inputs
            else ["TQQQ", "SOXL", "BIL"]
        ),
        "account_whitelist": (
            copy.deepcopy(session_inputs["account_whitelist"])
            if "account_whitelist" in session_inputs
            else ["stage_n_shadow"]
        ),
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
    for o7_key in ("o7_runtime_silent", "o7_monitor_silent"):
        if o7_key in session_inputs:
            out[o7_key] = bool(session_inputs[o7_key])
    for o5_key in _O5_PASSTHROUGH_KEYS:
        if o5_key in session_inputs:
            out[o5_key] = copy.deepcopy(session_inputs[o5_key])
    for key in ("tier_change_event", "o9_change_descriptors"):
        if key in session_inputs:
            out[key] = copy.deepcopy(session_inputs[key])
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

        auth_events_arg: dict | list | None
        if isinstance(authorized_events, (dict, list)) or authorized_events is None:
            auth_events_arg = authorized_events
        else:
            auth_events_arg = list(authorized_events)

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
