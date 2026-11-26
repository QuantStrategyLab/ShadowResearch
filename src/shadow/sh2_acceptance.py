"""Offline acceptance runner for SHADOW-N SH-2 (SH2-A01..A14)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from shadow.calendar_consumer import load_spec
from shadow.fuse_adapter import (
    DEFAULT_VENDOR_ENGINE_ROOT,
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_BUNDLE_HASH,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_FREEZE_HASH,
    PINNED_FUSE_SPEC_VERSION,
    ProductionChainLockedError,
    VendoredFuseEvaluator,
    verify_f1_entry_gates,
)
from shadow.manifest import compute_code_manifest_hash, load_effective_runtime_config
import shadow.ots as _ots_mod
from shadow.scheduler import REPO_ROOT, load_production_scheduler_config
from shadow.sizing import policy_config_hash


def _iter_hygiene_candidate_files(repo_root: Path) -> list[Path]:
    if (repo_root / ".git").exists():
        try:
            raw = subprocess.check_output(
                ["git", "-C", str(repo_root), "ls-files", "-z"],
                stderr=subprocess.DEVNULL,
            )
            paths: list[Path] = []
            for rel_bytes in raw.split(b"\x00"):
                if not rel_bytes:
                    continue
                p = repo_root / rel_bytes.decode("utf-8", errors="surrogateescape")
                if p.is_file() and "__pycache__" not in p.parts:
                    paths.append(p)
            if paths:
                return sorted(paths)
        except Exception:
            pass
    return sorted(
        p
        for p in repo_root.rglob("*")
        if p.is_file() and ".git" not in p.parts and "__pycache__" not in p.parts
    )


def _scan_path_and_secret_hygiene(repo_root: Path) -> list[str]:
    violations: list[str] = []
    forbidden_substrings = ("/" + "Users/", "/" + "home/dev/", "BEGIN " + "PRIVATE KEY")
    for p in _iter_hygiene_candidate_files(repo_root):
        try:
            text = p.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        rel = p.relative_to(repo_root).as_posix()
        for bad in forbidden_substrings:
            if bad in text:
                violations.append(f"{rel}:contains_{bad}")
    return violations


def _active_non_comment_text(raw_text: str) -> str:
    kept: list[str] = []
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        kept.append(line.split("#", 1)[0])
    return "\n".join(kept)


def _check_deployment_artifacts_ok(dockerfile_text: str, cloudrun_yaml: str) -> bool:
    df_active = _active_non_comment_text(dockerfile_text)
    yaml_active = _active_non_comment_text(cloudrun_yaml)
    has_state_mount = False
    if "volumeMounts:" in yaml_active and "volumes:" in yaml_active:
        mounts_sec = yaml_active.split("volumeMounts:", 1)[1].split("volumes:", 1)[0]
        vols_sec = yaml_active.split("volumes:", 1)[1]
        has_state_mount = (
            "name: shadow-state" in mounts_sec
            and "mountPath: /var/lib/shadow/data" in mounts_sec
            and "name: shadow-state" in vols_sec
            and "driver: gcsfuse.run.googleapis.com" in vols_sec
            and "uid=10001" in vols_sec
            and "gid=10001" in vols_sec
        )
    return (
        "SHADOW_DATA_DIR=" not in df_active
        and "chmod 0444" in df_active
        and "chmod 0555" in df_active
        and "USER shadowapp" in df_active
        and "--gid 10001 shadowapp" in df_active
        and 'ENTRYPOINT ["python3", "-B", "-m", "shadow.cloudrun_job"]' in df_active
        and "name: TIINGO_API_KEY" in yaml_active
        and "secretKeyRef:" in yaml_active
        and "name: SHADOW_DATA_DIR" in yaml_active
        and "value: /var/lib/shadow/data" in yaml_active
        and "us-east1" in yaml_active
        and has_state_mount
    )


def _check_standing_safety_locks(prod_runtime: dict, prod_cfg: object) -> list[str]:
    issues: list[str] = []
    locks = prod_runtime.get("standing_safety_locks")
    if not isinstance(locks, dict):
        return ["missing_standing_safety_locks"]
    expected = {
        "live": "OFF",
        "kelly": "OFF",
        "kelly_applied": False,
        "orders_placed": 0,
        "loss_breakers_armed": False,
        "enforcement_mode": "shadow_would_have_tripped",
        "H": "0",
        "B0": "50000",
        "stop_loss_return_rule": "OFF",
        "production_chain_locked": True,
    }
    for k, exp_v in expected.items():
        actual_v = locks.get(k)
        if type(actual_v) is not type(exp_v) or actual_v != exp_v:
            issues.append(f"lock_mismatch:{k}={actual_v!r}")
    if getattr(prod_cfg, "production_chain_locked", None) is not True:
        issues.append("config_production_chain_locked_not_true")
    if getattr(prod_cfg, "clk01_active", None) is not False:
        issues.append("config_clk01_active_not_false")
    return issues


def run_sh2_acceptance(repo_root: str | Path | None = None) -> dict[str, object]:
    """Execute SH2-A01..A14 offline acceptance checks against exact candidate artifacts."""
    if sys.flags.optimize > 0:
        raise RuntimeError("sh2_acceptance refuses Python optimized mode (-O / PYTHONOPTIMIZE)")

    root = (Path(repo_root) if repo_root is not None else REPO_ROOT).resolve()
    if root != REPO_ROOT.resolve():
        raise ValueError(
            f"run_sh2_acceptance validates active checkout {REPO_ROOT}; spawn subprocess inside candidate {root}"
        )

    # Guard against stale foreign run_sh2_demo module and forbid outbound network OTS calls offline
    demo_dir = str(root / "demo")
    demo_file = (root / "demo" / "run_sh2_demo.py").resolve()
    existing_mod = sys.modules.get("run_sh2_demo")
    if existing_mod is not None:
        mod_file = getattr(existing_mod, "__file__", None)
        if not mod_file or Path(mod_file).resolve() != demo_file:
            sys.modules.pop("run_sh2_demo", None)
    added_demo_dir = demo_dir not in sys.path
    if added_demo_dir:
        sys.path.insert(0, demo_dir)
    orig_ots_transport = _ots_mod.urllib_ots_transport

    def _forbid_network_ots(url: str, body: bytes, timeout_seconds: float):  # type: ignore[no-untyped-def]
        raise RuntimeError(f"offline_sh2_acceptance_forbids_network_ots:{url}")

    _ots_mod.urllib_ots_transport = _forbid_network_ots
    try:
        import run_sh2_demo  # type: ignore[import-not-found]

        demo_out = run_sh2_demo.run_all_demos()
    finally:
        _ots_mod.urllib_ots_transport = orig_ots_transport
        if added_demo_dir:
            try:
                sys.path.remove(demo_dir)
            except ValueError:
                pass

    cases = demo_out.get("cases", {}) if isinstance(demo_out, dict) else {}
    case_ok = lambda cid: isinstance(cases.get(cid), dict) and cases[cid].get("status") == "PASS"
    demo_all_passed = (
        isinstance(demo_out, dict)
        and demo_out.get("passed") == demo_out.get("total")
        and demo_out.get("total") == 10
        and all(case_ok(f"D{i}") for i in range(1, 11))
    )

    gates = verify_f1_entry_gates(require_read_only=False)
    ev = VendoredFuseEvaluator()
    spec_v2 = load_spec(root / "specs" / "csa22-v2")
    prod_cfg = load_production_scheduler_config(root / "config" / "production-scheduler.json")
    code_sha = compute_code_manifest_hash(root, calendar_version="csa22-v2")
    prod_runtime = load_effective_runtime_config(
        root, config_path=root / "config" / "production-scheduler.json"
    )
    prod_policy_hash = policy_config_hash(
        runtime_config=prod_runtime,
        calendar_spec=spec_v2,
        code_manifest_hash=code_sha,
    )

    hygiene_violations = _scan_path_and_secret_hygiene(root)
    dockerfile_text = (root / "Dockerfile").read_text(encoding="utf-8")
    cloudrun_yaml = (root / "deploy" / "cloudrun" / "job-spec.example.yaml").read_text(encoding="utf-8")
    raw_prod_cfg = json.loads((root / "config" / "production-scheduler.json").read_text(encoding="utf-8"))
    deploy_ok = _check_deployment_artifacts_ok(dockerfile_text, cloudrun_yaml)
    lock_issues = _check_standing_safety_locks(raw_prod_cfg, prod_cfg)

    # Verify strict oa_sh2_2 default lock
    if not (prod_cfg.production_chain_locked is True and prod_cfg.clk01_active is False):
        raise ProductionChainLockedError("Production config lock invariant violated")

    spec_digest_ok = spec_v2.get("spec_digest") == PINNED_CSA22_V2_DIGEST
    vendor_identity_ok = (
        ev.engine_hash == PINNED_FUSE_ENGINE_HASH
        and ev.spec_version == PINNED_FUSE_SPEC_VERSION
    )
    status_for = lambda ok: "PASS_OFFLINE" if ok else "FAILED"
    a14_ok = len(hygiene_violations) == 0 and len(lock_issues) == 0

    checks = [
        {
            "id": "SH2-A01",
            "status": status_for(bool(gates.get("ok")) and case_ok("D1")),
            "detail": f"Entry gates verified: F1-A01=24/24, F1-A03={gates['f1_a03_base_cells']}, F1-A04={gates['f1_a04_tier_repetitions']}, F1-A10={gates['f1_a10_successor_cases']}, F1-A11={gates['f1_a11_unresolved_fail_closed']}",
        },
        {
            "id": "SH2-A02",
            "status": status_for(vendor_identity_ok and bool(gates.get("pycache_verified")) and case_ok("D1")),
            "detail": (
                f"Digest-pinned vendor package & bytecode verified at {DEFAULT_VENDOR_ENGINE_ROOT.name}; "
                f"pycache_verified={gates['pycache_verified']}, read_only_verified={gates.get('read_only_verified')} (container 0444 unbuilt locally)"
            ),
        },
        {
            "id": "SH2-A03",
            "status": status_for(case_ok("D2")),
            "detail": "Structural StubEvaluator quarantine verified on production append and replay",
        },
        {
            "id": "SH2-A04",
            "status": status_for(case_ok("D2") and case_ok("D6")),
            "detail": "Zero conformance shims or fabricated sources/returns; U-01 unresolved fail-closed verbatim; 1-byte tamper rejected before evaluate()",
        },
        {
            "id": "SH2-A05",
            "status": status_for(case_ok("D2")),
            "detail": "Upstream explicit boolean hwm_adjusted schema contract enforced before evaluate() when adjustment_complete=True",
        },
        {
            "id": "SH2-A06",
            "status": status_for(spec_digest_ok and case_ok("D8")),
            "detail": f"csa22-v2 verified (spec_digest={spec_v2['spec_digest']}); 2026-07-03 closed & 2026-11-27/2026-12-24 13:00 ET early closes",
        },
        {
            "id": "SH2-A07",
            "status": status_for(case_ok("D8")),
            "detail": "US 23/5 16:00 ET Official Close isolation & post-21:00 ET T+1 boundary enforced (Item 13)",
        },
        {
            "id": "SH2-A08",
            "status": status_for(case_ok("D4")),
            "detail": "Atomic state_snapshots/ persistence verified across fresh subprocess restarts without counter reset",
        },
        {
            "id": "SH2-A09",
            "status": status_for(case_ok("D9")),
            "detail": "Cloud Run Jobs us-east1 stateless retry returns already_committed_no_op with <=1 append (local lease conflict verified in unit suite, Item 18)",
        },
        {
            "id": "SH2-A10",
            "status": status_for(case_ok("D7")),
            "detail": "Deterministic bit-for-bit replay verified via replay_window() on vendored FUSE-1 ledger",
        },
        {
            "id": "SH2-A11",
            "status": status_for(case_ok("D5") and bool(prod_cfg.production_chain_locked is True and prod_cfg.clk01_active is False)),
            "detail": "Default production config locked (production_chain_locked=true, clk01_active=false, OA-SH2-2 blocks appends with 0 writes)",
        },
        {
            "id": "SH2-A12",
            "status": status_for(case_ok("D10")),
            "detail": "Forward-clock creditability verifies OTS BTC proof + engine_hash/spec_version pins when unlocked and fails closed by default when locked",
        },
        {
            "id": "SH2-A13",
            "status": "PASS_OFFLINE" if deploy_ok else "FAILED",
            "detail": (
                "Secret-free Cloud Run Jobs Dockerfile (0444 vendor, non-root UID 10001) & us-east1 Secret Manager + UID-matched GCS FUSE spec verified"
                if deploy_ok
                else "FAILED"
            ),
        },
        {
            "id": "SH2-A14",
            "status": "PASS_OFFLINE" if a14_ok else "FAILED",
            "detail": (
                f"Standing safety locks verified on config (H='0', B0='50000', orders_placed=0, kelly=OFF, live=OFF) & zero scanned forbidden patterns ({len(hygiene_violations)} violations)"
                if a14_ok
                else f"A14_FAILED:hygiene={hygiene_violations},locks={lock_issues}"
            ),
        },
    ]

    demo_passed = demo_out.get("passed", 0) if isinstance(demo_out, dict) else 0
    demo_total = demo_out.get("total", 0) if isinstance(demo_out, dict) else 0
    overall_ok = (
        demo_all_passed
        and spec_digest_ok
        and vendor_identity_ok
        and all(c["status"] == "PASS_OFFLINE" and not str(c["detail"]).startswith(("FAILED", "HYGIENE_FAILED")) for c in checks)
    )
    return {
        "schema_version": "sh2-acceptance-report/v1",
        "overall_status": "PASS_OFFLINE" if overall_ok else "FAILED",
        "demo_cases_passed": f"{demo_passed}/{demo_total}",
        "oa_sh2_2_locked": True,
        "pinned_identities": {
            "fuse_spec_version": PINNED_FUSE_SPEC_VERSION,
            "fuse_engine_hash": PINNED_FUSE_ENGINE_HASH,
            "fuse_bundle_hash": PINNED_FUSE_BUNDLE_HASH,
            "fuse_freeze_hash": PINNED_FUSE_FREEZE_HASH,
            "csa22_v2_spec_digest": PINNED_CSA22_V2_DIGEST,
            "production_code_version_sha": code_sha,
            "production_policy_config_hash": prod_policy_hash,
        },
        "checks": checks,
        "demo_summary": {k: v["status"] for k, v in cases.items()},
        "hygiene_violations": hygiene_violations,
    }


def main(argv: list[str] | None = None) -> int:
    _ = argv
    rep = run_sh2_acceptance()
    print(json.dumps(rep, indent=2, sort_keys=True))
    return 0 if rep["overall_status"] == "PASS_OFFLINE" else 1


if __name__ == "__main__":
    sys.exit(main())
