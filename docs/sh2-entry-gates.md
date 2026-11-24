# SH-2 Entry-Gate Record & Digest-Pinned Engine Provenance

## 1. Upstream Entry Gates (`FuseControl` -> `ShadowResearch`)

- **Upstream Repository:** `QuantStrategyLab/FuseControl` (`main` merge commit `46a86eedb0d33f6133a066edeb8c3660cda9097d`)
- **Vendored Package Path:** `vendor/fuse_engine/fuse-eval-spec-v1/12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45/fuse_engine/` (`0444` read-only permissions, 8 `.py` files only, zero committed `__pycache__/`)
- **`spec_version`:** `fuse-eval-spec/v1`
- **`engine_hash`:** `12e9b502367a4993d0c36e75ac5c8d24acaee2d1247c5b594796d5adda04ca45`
- **`bundle_hash`:** `78fed9dfc9b0b0a618fce6bb5a956063135f34845545e6a5154a32e119354cc9`
- **`freeze_hash`:** `1da74ca508161ba6d9a424101321381a9f5156d8cb2c84c03be2fd2fe3f22a74`

| Gate | Requirement | Result | Source Artifact |
| --- | --- | --- | --- |
| `F1-A01` | Exact 24-ID set equality (`spec.json` == `ROW_IDS` == `ALL_BREAKER_IDS`) | `PASS` (`24/24`) | `metadata/spec.json` |
| `F1-A03` | Base matrix trip verification | `PASS` (`264/264`, 0 failed) | `metadata/result-bundle.json` |
| `F1-A04` | Tier repetitions verification | `PASS` (`132/132`, 0 failed) | `metadata/result-bundle.json` |
| `F1-A10` | Successor continuity cases | `PASS` (`13/13`, 0 failed) | `metadata/result-bundle.json` |
| `F1-A11` | Disposition register unresolved items fail closed | `PASS` (`O5`, `V5`, `V7` `UNRESOLVED-FAIL-CLOSED`, 0 unknown) | `metadata/disposition-register.json` |

## 2. SH-1 & C-SA-22 Entry Gate Continuity

- **SH-1 Baseline Commit:** `20be23f1ce4f4f23bd2f863aef43213894947bf3` (`96/96` unit tests preserved unbroken)
- **SH-1 Policy Config Hash:** `1f4fd701ee63f78b4445e3ece177b854e75618f307491a638c4e3d4546d17ad8`
- **C-SA-22 v2 Merge Commit:** `204120875403519d10d75d35a5abc17ade71a71a` (`csa22-v2` `spec_digest = 2ba8cd7684c361209df68bfa2e10004c1f9922a645c7ec55d421fda520561798`)
- **Runtime Verification:** [`VendoredFuseEvaluator`](../src/shadow/fuse_adapter.py) verifies all 8 `.py` SHA-256 digests against `metadata/provenance.json`, checks framed SHA-256 (`engine_hash`), checks `spec_version == "fuse-eval-spec/v1"`, and validates any existing `__pycache__/*.pyc` against source bytecode prior to every `evaluate()` call.
