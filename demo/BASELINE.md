# SH-1 §7 Design Demonstration — Failing Baseline

- **Branch:** `demo/sh1-design-demo`
- **Skeleton Head:** `b140c2aff1279ef7160dcd9d3d2d39765383ad16` (`b140c2a`)
- **Frozen Case List:** [`demo/DEMO-CASES.md`](DEMO-CASES.md)
- **Command:** `python3 demo/run_demo.py`
- **Exit Code:** `1`

## Baseline Summary

| Case | Status | Summary |
| --- | --- | --- |
| D1 | FAIL | Not implemented (skeleton head) |
| D2 | FAIL | Not implemented (skeleton head) |
| D3 | FAIL | Not implemented (skeleton head) |
| D4 | FAIL | Not implemented (skeleton head) |
| D5 | FAIL | Not implemented (skeleton head) |
| D6 | FAIL | Not implemented (skeleton head) |
| D7 | FAIL | Not implemented (skeleton head) |
| D8 | FAIL | Not implemented (skeleton head) |

## Exact Command Output

```text
D1 FAIL (not implemented): Exact 3-tier sizing outputs (6-session synthetic fixture)
D2 FAIL (not implemented): Split invariance (2:1) and half-day invariance
D3 FAIL (not implemented): Standing Stage N assertions (kelly_applied=false, orders_placed=0)
D4 FAIL (not implemented): Bit-for-bit replay of D1 fixture window
D5 FAIL (not implemented): Hash-chain tamper rejection (mutation, fork, gap, duplicate)
D6 FAIL (not implemented): Y2-overlap 10-session reconstruction & forward-credit rejection
D7 FAIL (not implemented): O1 fail-closed output on withheld SOXL close
D8 FAIL (not implemented): C-SA-22 calendar vectors (2025 full year + error cases)
```
