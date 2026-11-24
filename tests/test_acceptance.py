"""Integration test executing the SH1-ACCEPT-v1 acceptance pack (A01-A16) and D1-D8."""

from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path

_SRC = str(Path(__file__).resolve().parents[1] / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from shadow.acceptance import run_acceptance_pack

REPO_ROOT = Path(__file__).resolve().parents[1]


class AcceptancePackTest(unittest.TestCase):
    """Verify all offline SH1-ACCEPT-v1 rows and D1-D8 demonstration checks."""

    def test_acceptance_pack_and_cli(self) -> None:
        res = run_acceptance_pack()
        rows = res["acceptance_rows"]
        self.assertEqual(set(rows.keys()), {f"SH1-A{i:02d}" for i in range(1, 17)})
        for row_id, item in rows.items():
            self.assertNotIn("FAIL", str(item["status"]), f"{row_id}: {item}")

        demo = res["demo_d1_d8"]
        self.assertEqual(set(demo.keys()), {f"D{i}" for i in range(1, 9)})
        for cid, item in demo.items():
            self.assertTrue(item["ok"], f"{cid}: {item['detail']}")

        proc = subprocess.run(
            [sys.executable, "-m", "shadow.acceptance"],
            cwd=str(REPO_ROOT),
            env={"PATH": "/usr/bin:/bin:/usr/local/bin", "PYTHONPATH": str(REPO_ROOT / "src")},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        parsed = json.loads(proc.stdout)
        self.assertIn("SH1-A16", parsed["acceptance_rows"])


if __name__ == "__main__":
    unittest.main()
