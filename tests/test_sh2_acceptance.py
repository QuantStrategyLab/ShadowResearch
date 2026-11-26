"""Tests for SH-2 offline acceptance suite (SH2-A01..A14) and documentation hygiene."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
_DEMO = str(_REPO_ROOT / "demo")
if _DEMO not in sys.path:
    sys.path.insert(0, _DEMO)

from shadow.fuse_adapter import (
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_BUNDLE_HASH,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_FREEZE_HASH,
    PINNED_FUSE_SPEC_VERSION,
)
from shadow.sh2_acceptance import (
    _check_deployment_artifacts_ok,
    _check_standing_safety_locks,
    run_sh2_acceptance,
)


class Sh2AcceptanceTest(unittest.TestCase):
    def test_sh2_acceptance_all_14_checks_pass_offline(self) -> None:
        rep = run_sh2_acceptance(_REPO_ROOT)
        self.assertEqual(rep["overall_status"], "PASS_OFFLINE")
        self.assertEqual(rep["demo_cases_passed"], "10/10")
        self.assertTrue(rep["oa_sh2_2_locked"])
        self.assertEqual(rep["hygiene_violations"], [])

        checks = rep["checks"]
        self.assertEqual([c["id"] for c in checks], [f"SH2-A{i:02d}" for i in range(1, 15)])
        for check in checks:
            self.assertEqual(check["status"], "PASS_OFFLINE", check)

        pinned = rep["pinned_identities"]
        self.assertEqual(pinned["fuse_spec_version"], PINNED_FUSE_SPEC_VERSION)
        self.assertEqual(pinned["fuse_engine_hash"], PINNED_FUSE_ENGINE_HASH)
        self.assertEqual(pinned["fuse_bundle_hash"], PINNED_FUSE_BUNDLE_HASH)
        self.assertEqual(pinned["fuse_freeze_hash"], PINNED_FUSE_FREEZE_HASH)
        self.assertEqual(pinned["csa22_v2_spec_digest"], PINNED_CSA22_V2_DIGEST)

    def test_sh2_docs_exist_and_keep_required_hashes(self) -> None:
        gates_doc = (_REPO_ROOT / "docs" / "sh2-entry-gates.md").read_text(encoding="utf-8")
        self.assertIn(PINNED_FUSE_ENGINE_HASH, gates_doc)
        self.assertIn("20be23f1ce4f4f23bd2f863aef43213894947bf3", gates_doc)

        runbook_doc = (_REPO_ROOT / "docs" / "sh2-cloudrun-runbook.md").read_text(encoding="utf-8")
        self.assertIn("us-east1", runbook_doc)
        self.assertIn("Item 18", runbook_doc)

        pack_doc = (_REPO_ROOT / "docs" / "sh2-acceptance-pack.md").read_text(encoding="utf-8")
        self.assertIn(PINNED_FUSE_ENGINE_HASH, pack_doc)
        self.assertIn(PINNED_CSA22_V2_DIGEST, pack_doc)
        self.assertIn("SH2-A14", pack_doc)

    def test_sh2_acceptance_fails_closed_when_demo_case_fails(self) -> None:
        from unittest import mock

        bad_demo = {
            "demo_suite": "SH2-D1-D10",
            "passed": 9,
            "total": 10,
            "cases": {f"D{i}": {"status": "FAIL" if i == 8 else "PASS"} for i in range(1, 11)},
        }
        with mock.patch("run_sh2_demo.run_all_demos", return_value=bad_demo):
            rep = run_sh2_acceptance(_REPO_ROOT)
        self.assertEqual(rep["overall_status"], "FAILED")
        check_a06 = next(c for c in rep["checks"] if c["id"] == "SH2-A06")
        check_a07 = next(c for c in rep["checks"] if c["id"] == "SH2-A07")
        self.assertEqual(check_a06["status"], "FAILED")
        self.assertEqual(check_a07["status"], "FAILED")

    def test_sh2_acceptance_handles_malformed_demo_output_without_keyerror(self) -> None:
        from unittest import mock

        for malformed in ({}, None):
            with mock.patch("run_sh2_demo.run_all_demos", return_value=malformed):
                rep = run_sh2_acceptance(_REPO_ROOT)
            self.assertEqual(rep["overall_status"], "FAILED")
            self.assertEqual(rep["demo_cases_passed"], "0/0")

    def test_sh2_acceptance_rejects_foreign_repo_root_network_ots_and_missing_deploy_or_lock_invariants(self) -> None:
        import tempfile
        from unittest import mock

        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(ValueError):
                run_sh2_acceptance(Path(tmpdir))

        with mock.patch("shadow.ots.urllib_ots_transport", side_effect=AssertionError("network OTS forbidden")) as net_mock:
            rep = run_sh2_acceptance(_REPO_ROOT)
        self.assertEqual(rep["overall_status"], "PASS_OFFLINE")
        self.assertEqual(net_mock.call_count, 0)

        df = (_REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
        yaml_txt = (_REPO_ROOT / "deploy" / "cloudrun" / "job-spec.example.yaml").read_text(encoding="utf-8")
        self.assertTrue(_check_deployment_artifacts_ok(df, yaml_txt))
        self.assertFalse(_check_deployment_artifacts_ok(df.replace("USER shadowapp", "# USER shadowapp"), yaml_txt))
        self.assertFalse(_check_deployment_artifacts_ok(df + "\nENV SHADOW_DATA_DIR=/tmp/bad\n", yaml_txt))
        self.assertFalse(_check_deployment_artifacts_ok(df, yaml_txt.replace("uid=10001", "uid=0")))
        self.assertFalse(_check_deployment_artifacts_ok(df, yaml_txt.replace("mountPath: /var/lib/shadow/data", "mountPath: /tmp/other")))

        self.assertTrue(_check_standing_safety_locks({"standing_safety_locks": {"kelly_applied": True}}, object()))


if __name__ == "__main__":
    unittest.main()
