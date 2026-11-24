# Y2-Overlap 10-Session Fixture (`2025-11-14` .. `2025-11-28`)

Extracted from the **read-only** YARDSTICK Batch Y2 dataset at
`/home/hatch/workspace/quant-s2/yardstick/y2/data/` for SH-1 §7 demonstration
case **D6** (`reconstruction` mode).

## Window & Discipline Notes

- **Sessions:** 10 consecutive `XNYS` sessions: `2025-11-14`, `2025-11-17`,
  `2025-11-18`, `2025-11-19`, `2025-11-20` (TQQQ 2:1 split), `2025-11-21`,
  `2025-11-24`, `2025-11-25`, `2025-11-26`, `2025-11-28` (early close `13:00`
  EST; `2025-11-27` Thanksgiving closed).
- **Standing Y2 Qualifications (U-02):**
  - `observation_class`: `historical_reconstruction`
  - Vintage qualification: `"as-retrieved reconstruction; not point-in-time inputs"`
  - `SOXL`: `"market-close proxy; NAV criterion unverified"`
  - Semiconductor index (`2021-08-25`+): `tr_regime = native_xsox_sub_for_icesemit`
    (disclosed `XSOX` substitution for `ICESEMIT`)
  - Forward clock credit: **ineligible** (`forward_clock_credit_eligible: false`;
    rejected for CLK-01..04).

## File Digests (SHA-256)

| File | Rows | Source SHA-256 (`/home/hatch/workspace/quant-s2/yardstick/y2/data/`) | Sliced Fixture SHA-256 |
| --- | ---: | --- | --- |
| `tqqq_daily.csv` | 10 | `d770897f3c7df8e0cdd056415e2aea10f83d438939ee77602ad3a295b083fd02` | `571ece754aac08e797be627e6845f06335f67974b2ca7bb4e310fdce32782de5` |
| `soxl_daily.csv` | 10 | `f5e664a976c45df1166c7c401fbea807d754f1d5b26300d101ae163a38062cdd` | `2eb43817bf9b24bfc06bb8cc81fc52f9f219c37846107d666855b0ffd0eaa0e8` |
| `ndx_index_daily.csv` | 10 | `875c6cde908285b536d094181119caf0338e92f358e85798e236b5c5c33d65a5` | `f3cc22a2052e7c18b4f0a7add169f28634395074cbf55e9c0cb573c1f540fdf1` |
| `semi_index_daily.csv` | 10 | `4ef50831d033711958beff9ee7027d68c361f04e02642a9a19bd436753d7abef` | `f67104b0a1403f5187ba36acd3ec8ec06376c4199dcf7129a6249378982f301a` |
| `risk_free_rates_daily.csv` | 10 | `211fc1e0f99b6004fbd3f55f0c7a178ed40bee7d72c3a78969c16289a8d5debb` | `b634fb376ee80f4d5f3cf966082876bd9ecf54ec3e1f7237f49e5be5951b16f6` |
