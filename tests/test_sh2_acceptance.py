"""Tests for SH-2 offline acceptance suite (SH2-A01..A14) and documentation hygiene."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = str(_REPO_ROOT / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.fuse_adapter import (
    PINNED_CSA22_V2_DIGEST,
    PINNED_FUSE_BUNDLE_HASH,
    PINNED_FUSE_ENGINE_HASH,
    PINNED_FUSE_FREEZE_HASH,
    PINNED_FUSE_SPEC_VERSION,
)
from shadow.sh2_acceptance import run_sh2_acceptance


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


if __name__ == "__main__":
    unittest.main()
