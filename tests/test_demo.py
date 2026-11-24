"""Integration tests verifying the D1-D8 SH-1 design demonstration."""

from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "demo") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "demo"))

from run_demo import run_all_cases


class DemoIntegrationTest(unittest.TestCase):
    """Verify all 8 demonstration cases pass via library call and CLI subprocess."""

    def test_run_all_cases_d1_through_d8_pass(self) -> None:
        outcomes = run_all_cases()
        self.assertEqual([cid for cid, _, _ in outcomes], [f"D{i}" for i in range(1, 9)])
        for cid, ok, detail in outcomes:
            self.assertTrue(ok, f"{cid} failed: {detail}")

    def test_demo_cli_exits_zero(self) -> None:
        proc = subprocess.run(
            [sys.executable, str(REPO_ROOT / "demo" / "run_demo.py")],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        for i in range(1, 9):
            self.assertIn(f"D{i} PASS:", proc.stdout)


if __name__ == "__main__":
    unittest.main()
