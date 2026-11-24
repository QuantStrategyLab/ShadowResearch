# SH-1 Acceptance Pack (`SH1-ACCEPT-v1`) & C-SA-22 Readiness

- **Branch:** `feat/sh1-v2-clean`
- **Authority:** [`docs/BRIEF-SH-1.md`](BRIEF-SH-1.md) §1, §3.2 (`SH1-W4`–`SH1-W8`), §5, §6 (`SH1-ACCEPT-v1`), §7 (`D1`–`D8`), §8 (`R-1`–`R-7`)
- **Standing Safety Locks:** `kelly_applied: false`, `orders_placed: 0`, `B0 = "50000"`, `H = "0"`, `stop_loss_return_rule = "OFF"`, `loss_controls_armed = false`, `StubEvaluator` structurally incapable of production forward-clock evaluation (`CLK-01` inactive).

## 1. Pinned Digests & Configuration Identities

| Artifact | Value |
| --- | --- |
| `policy_id` | `policy_v0_pre` |
| `policy_config_hash` | `8409ea2722f3ca25e2cd750a957aeb01054743b908341fb61936f44d247666fd` |
| `code_version_sha` | `ad2ffcf244641a5d9055ab855810fc2c0bab68bdf84e627f5754476f33be4fa8` |
| `csa22-v1` `spec_digest` | `6601c01f2907119c25a2687d4b67210285d3c13ebf2d86a2953745dbd1f6f2af` |
| `StubEvaluator` `engine_hash` | `stub-d4dd72fbcf2f8d86` (synthetic drill only; `production_clock_capable = false`) |
| Drill config | [`config/drill-scheduler.json`](../config/drill-scheduler.json) (`chain_id = "drill-sh1-v1"`, `mode = "drill"`, `clk01_active = false`) |

## 2. Acceptance Matrix (`SH1-ACCEPT-v1` — `SH1-A01`…`SH1-A16`)

Reproduced offline via `PYTHONPATH=src python3 -m shadow.acceptance` and `python3 -m unittest discover -s tests -v`:

| Row | Status | Verified Evidence & Scope |
| --- | --- | --- |
| `SH1-A01` | `PASS_OFFLINE` | 3 sessions × 7 required series (`TQQQ`, `SOXL`, `QQQ`, `XNDX`, `SEMI`, `SOFR`, `TBILL_3M`) + 10-session Y2 reconstruction complete without silent gaps. |
| `SH1-A02` | `PARTIAL_MINI_PROBE_RECORDED` | Initial Mac mini probe recorded in [`docs/SH1-A02-PROBE.md`](SH1-A02-PROBE.md) (`2026-11-21`). NY Fed SOFR + FRED DTB3 live-verified; Stooq JS challenge (`OA-SH1-7`), Yahoo ETF mismatch / HTTP 429, Treasury timeout/404, Tiingo (`OA-SH1-2` pending owner key), and weekday post-`16:00` ET finalization lag remain **pending Mac mini live weekday market window**. |
| `SH1-A03` | `PASS_OFFLINE` | Per-provider live wall-clock post-response `retrieval_time_utc` inside `[session_close_utc, decision_cutoff_utc]` (`[16:00, 18:00]` ET regular / `[13:00, 15:00]` ET half-day), exact prior-business-day SOFR `T-1` check (skipping weekends, US federal bond holidays, and scheduled NYSE holidays without skipping unscheduled equity closures), `resolve_prior_close_session`, and `csa22-v1` `spec_digest` verified. |
| `SH1-A04` | `PASS_OFFLINE` | 21/21 archived raw payloads SHA-256 verified and `normalized_value` re-derived byte-for-byte via `verify_market_record_raw_integrity` (with strict `T-1` SOFR and Tiingo `divCash`/`splitFactor` enforcement). |
| `SH1-A05` | `PASS_OFFLINE` | Same-day SOFR revision (`revisionIndicator="R"`) advances automatically to `rev1` with a distinct SHA-256 while preserving `rev0` archived bytes untouched. |
| `SH1-A06` | `PASS_OFFLINE` | Withholding any of the 7 required series (or stale prior SOFR) triggers `O1` (`NO-NEW-RISK`) fail-closed output; pre-close runs (`session_not_yet_closed`, writing 0 completeness reports) and post-commitment-deadline backfill calls reject append with 0 decision records. |
| `SH1-A07` | `PASS_OFFLINE` | `TQQQ`/`SOXL` carry `close (proxy, NAV unverified)`, `SEMI` carries `xsox_for_icesemit_substitution: true`, rates carry `days/365` and `SOFR` `T-1` lag (`stale` disclosed cleanly when delayed). |
| `SH1-A08` | `PASS_OFFLINE` | RFC 8785 UTF-16BE key sort, control escapes, duplicate-key rejection, `NaN`/`Infinity`/float rejection, and canonical decimal checks pass. |
| `SH1-A09` | `PASS_OFFLINE` | 64-zero genesis, gapless links, `prev_record_sha256` verification, and 1-byte mutation returning `first_bad_sequence`. |
| `SH1-A10` | `PASS_OFFLINE` | Duplicate `(mode, policy_id, policy_config_hash, session_date)`, duplicate `session_date` across records, forked `prev_record_sha256`, code/runtime-config/calendar/evaluator identity drift, serialized `.chain.lock` commit protection, orphan cleanup on unanchored append, and cross-mode mixing rejected; 3 tiers present once; Stage N enforces exact `H="0"`. |
| `SH1-A11` | `PASS_OFFLINE_FIXTURE_AND_OTS_FORMAT` | Detached `.ots` binary receipt (`seq_XXXXXX.ots`) + metadata stored atomically under `.chain.lock` before final `seq_NNNNNN.record.json` commit marker (with ownership-scoped rollback and orphan recovery); verified against recomputed canonical `record_body` SHA-256, fail-closed pinned deadline config, `obtained_at_utc >= decision_timestamp_utc`, and trusted Bitcoin block headers & block timestamps (`[36:68]` internal Merkle order, `[68:72]` LE epoch `<= commitment_deadline_utc`, `4096`-byte op caps, calendar URI allowlist, and HTTP 3xx redirect rejection). Live public calendar HTTP submission awaits Mac mini deployment. |
| `SH1-A12` | `PENDING_MAC_MINI_ACTIVATION` | Offline detached subprocess execution (`python3 -m shadow.scheduler dry-run` in stripped env), `holiday_no_op` / `pre_close_slot_no_op` / `already_committed_no_op` auto-session handling, immutable daily completeness reporting + post-record crash recovery, and EST/EDT DST-pinned scheduler templates covering all `250/250` 2025 sessions (incl. `14:30` ET half-days) verified; live autonomous firing on Mac mini awaits `OA-SH1-6` activation. |
| `SH1-A13` | `PENDING_MAC_MINI_ACTIVATION` | Offline acceptance measurements (`wall=0.207218s`, `rss=24895488B <= 512MiB`, `footprint=68502B <= 2GiB`) stay well within unit caps; live co-residency alongside the 3 MCA runners awaits Mac mini activation. |
| `SH1-A14` | `PASS_OFFLINE` | Suppressing `2025-11-24` drill firing, `late_after_decision_cutoff` half-day late runs, completeness reports outside `[session_close_utc, commitment_deadline_utc]`, committed records missing completeness reports, or receipts obtained before `decision_timestamp_utc` fail `detect_missed_runs` with 0 backfilled records; out-of-coverage windows and unsupported venues fail closed with `calendar_coverage_insufficient`. |
| `SH1-A15` | `PASS_OFFLINE` | `replay_window` / `python3 -m shadow.replay` reproduces drill and D1 records bit-for-bit, requires all 7 required market series present, eligible, and independently verified inside `[session_close_utc, min(decision_cutoff_utc, decision_timestamp_utc)]`, recomputes `market_records_digest` and `inputs_digest` (including `W_ref`), cross-checks sizing `close_price`, and gates creditability on valid chains + trusted Bitcoin headers with block timestamps. |
| `SH1-A16` | `PASS_OFFLINE` | Every emitted record/tier/fund row asserts `kelly_applied: false`, integer `orders_placed: 0`, `B0="50000"`, `H="0"`, `stop_loss_return_rule="OFF"`, `loss_controls_armed=false`, and all 24 named breakers. |

## 3. §7 Design Demonstration (`D1`–`D8`)

`python3 demo/run_demo.py` passes `8/8` on the exact candidate head (`D1`–`D8`), including Astra Interpretation 4 (`demo/DEMO-CASES-CORRECTION-1.md`) and integer `orders_placed: 0` across all 96 fund rows (`36` D1 + `60` D6).

## 4. C-SA-22 Calendar Readiness (`csa22-v1`, `csa22-v2`, `2027+`) & SH-2 Handoff

- **Current branch (`feat/sh1-v2-clean`):** Bundles published [`specs/csa22-v1/`](../specs/csa22-v1/manifest.json) (`spec_digest = 6601c01f2907119c25a2687d4b67210285d3c13ebf2d86a2953745dbd1f6f2af`), covering `2025-01-01`..`2025-12-31` (`XNYS`) for D1–D8 and offline fixture verification.
- **2026 Readiness (`origin/feat/csa22-v2`):** Read-only inspection of `origin/feat/csa22-v2:specs/csa22-v2/manifest.json` and `SPEC.md` confirms published 2026 coverage (`2026-01-01`..`2026-12-31`, `spec_digest = 2ba8cd7684c361209df68bfa2e10004c1f9922a645c7ec55d421fda520561798`, `2026-07-03` Independence Day observed closed, `2026-11-27` and `2026-12-24` 13:00 early closes, and Rule 7 projection semantics). `csa22-v2` is intentionally not merged into `feat/sh1-v2-clean`; `src/shadow/calendar_consumer.py` discovers `exceptions-*.json` dynamically from `manifest.json` so `csa22-v2` loads cleanly by digest once promoted to `main` / mounted for 2026 Mac mini live drills and `SH-2`.
- **2027+ Readiness (`CLK-03` / `CLK-04`):** Because `CLK-03` (~6 months) and `CLK-04` (250 sessions / 12 months) extend into 2027, the coordinator must publish an immutable `csa22-v3` spec package covering `2027-01-01`..`2027-12-31` prior to `2027-01-01` so calendar resolution never falls back to `out_of_range`.
- **SH-2 Handoff Gate:** `SH-2` requires (1) accepted `FUSE-1` engine hash replacing `StubEvaluator`, (2) `csa22-v2` (`2026`) / `csa22-v3` (`2027`) mounted by digest, (3) `SH1-A02` weekday EOD finalization probe + Tiingo (`OA-SH1-2`) + Treasury/Yahoo resolution on Mac mini, and (4) `OA-SH1-6` drill validation prior to starting `CLK-01` on a separate production chain.

## 5. Registrations Checklist (`R-1`–`R-7`)

| ID | Status | Notes |
| --- | --- | --- |
| `R-1` | Complete | `QuantStrategyLab/ShadowResearch` created (`OA-SH1-4`) with PR governance workflows. |
| `R-2` | Enrolled | `PrGovernance` workflows (`risk-gate.yml`, `pr-review-signal.yml`, `codex-nudge.yml`) active. |
| `R-3` | Published | `csa22-v1` published in `specs/csa22-v1/`; `csa22-v2` published on `origin/feat/csa22-v2`. |
| `R-4` | Pending Mini Activation | Unit templates + footprint budget (`<= 2 GiB`) verified; mini systemd linger & runner check upon deployment. |
| `R-5` | Pinned | `policy_v0_pre` canonical digest `8409ea2722f3ca25e2cd750a957aeb01054743b908341fb61936f44d247666fd` (covering policy params, effective run config including `half_day_run_time_et`, `decision_cutoff_et`, `treasury_endpoint`, and `yahoo_symbol_map`, calendar digest, and executable manifest). |
| `R-6` | Partial | `docs/SH1-A02-PROBE.md` recorded; weekday post-close EOD timing carry-over pending live weekday window. |
| `R-7` | Pinned | OpenTimestamps calendars + `23:30` ET commitment deadline pinned in `config/drill-scheduler.json`. |
