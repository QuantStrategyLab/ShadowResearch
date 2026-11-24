# SH-1 Drill-Chain Scheduler & Runbook

This runbook covers the AI-independent daily drill-chain scheduler (`OA-SH1-6`, **drill chain only**; `CLK-01` remains inactive until `SH-2`).

## 1. Layout & Environment Discipline (`BRIEF-SH-1.md` §3.3–3.4)

- Repository checkout: `<dev-home>/shadow-research`
- Append-only drill data root (never committed to `main`): `<dev-home>/shadow-research/data/drill` (`ledger/`, `archive/`, `replay/`, `reports/`, `logs/`)
- Environment file on Mac mini (`Ubuntu 24.04` `dev` user): `<dev-home>/dev-env-mini/env.sh` (`TZ=Asia/Singapore`, `LANG/LC_ALL=en_SG.UTF-8`, `HTTP_PROXY=http://127.0.0.1:18080`, `HTTPS_PROXY=http://127.0.0.1:18080`)
- Resource discipline: `Nice=15`, `CPUQuota=25%`, `MemoryMax=512M`, `TasksMax=32`, 30-day / 50 MiB log rotation, total footprint budget `<= 2 GiB`.

## 2. Scheduler Templates (`deploy/scheduler/`)

| Platform / Mode | Unit / Script | Purpose |
| --- | --- | --- |
| Linux systemd user | `deploy/scheduler/shadow-drill.service` & `.timer` | Fires `run-drill --session auto` `Mon..Fri 14:30:00` (half-day) and `17:30:00` (regular) `America/New_York` (EST/EDT DST-pinned) |
| Linux systemd user | `deploy/scheduler/shadow-missed-run.service` & `.timer` | Fires `detect-missed` `Mon..Fri 23:45:00 America/New_York` (15m after `23:30` ET commitment deadline) |
| Linux `@reboot` / cron fallback | `deploy/scheduler/shadow-watchdog.sh` | Fallback when systemd user linger is unavailable (`nice -n 15`, sources `env.sh`, optional `SHADOW_REQUIRE_NY_HHMM`, comma-separated e.g. `14:30,17:30`) |
| macOS LaunchAgent template | `deploy/scheduler/com.quantstrategylab.shadow-drill.plist` & `shadow-missed-run.plist` | `<key>Disabled</key><true/>`, `Nice=15`, `LowPriorityIO=true`, EDT/EST SGT intervals gated by `--require-ny-hhmm` (`14:30,17:30` / `23:45` `America/New_York`) |

No template may contain `--mode production` or activate `CLK-01`. Templates are validated structurally by `shadow.scheduler.validate_scheduler_templates`.

## 3. CLI Operations & Safety Guarantees

- Normal `run-drill` invocations have no `--now-utc` clock override (`wall_clock=None`): they read real `datetime.now(timezone.utc)` at each individual provider fetch response, decision check, and again after `OpenTimestampsClient.stamp_digest()`, and fail closed if actual wall clock is outside the Three-Timestamp EOD Contract or past `commitment_deadline_utc` (`late_after_commitment_deadline_backfill_rejected`, 0 ledger appends).
- Under `--session auto`, `run-drill` resolves the session using the current `America/New_York` calendar date and exits cleanly (`reports_written: 0`) on weekends/holidays (`holiday_no_op`), pre-close half-day slots fired on regular days (`pre_close_slot_no_op`), or `17:30` ET slots fired on half-days already committed at `14:30` ET (`already_committed_no_op`). Explicit `--session YYYY-MM-DD` on a non-trading day emits an immutable `O2` (`non_trading_session`) fail-closed report; explicit pre-close runs reject `session_not_yet_closed` without writing completeness reports.
- Three-Timestamp EOD Contract per trading session `T` (`config/drill-scheduler.json`):
  - `session_close_utc`: `C-SA-22` `close_utc` (`16:00` ET regular / `13:00` ET half-day).
  - Scheduled decision slots: `17:30` ET regular (`decision_instant_et`) / `14:30` ET half-day (`half_day_run_time_et`).
  - `decision_cutoff_utc`: `18:00` ET regular (`decision_cutoff_et`) / `15:00` ET half-day (`decision_cutoff_half_day_et`), hashed into `policy_config_hash`. Every individual provider retrieval timestamp must lie within `[session_close_utc, decision_cutoff_utc]`.
  - `commitment_deadline_utc`: `23:30` ET (`commitment_deadline_et`). Ledger commit + `.ots` anchoring + completeness report creation (`created_at_utc`) + trusted Bitcoin block timestamp verification must fall on or before `commitment_deadline_utc`.
- Deterministic historical fixture simulation uses the isolated `simulate-drill` subcommand, which requires `--now-utc`, requires `--chain-id sim-...`, and refuses `data/` or any directory containing a `drill` path component.
- Ledger persistence (`replay_bundle.json`, `seq_NNNNNN.ots`, `seq_NNNNNN.receipt.json` followed last by `seq_NNNNNN.record.json` commit marker with `fsync` + `os.replace`) executes under an exclusive re-entrant `flock` lock (`.chain.lock`) with ownership-scoped rollback and clean orphan retry recovery. Existing daily completeness reports are immutable (non-overwriting).
- `detect-missed` honors `config.calendar_version` (`specs/<calendar_version>`) and fails closed with `CalendarCoverageError` (`calendar_coverage_insufficient`) instead of truncating if the requested window exceeds loaded calendar coverage or references an unsupported venue. Daily reports outside `[session_close_utc, commitment_deadline_utc]` are classified as `missed_run_silent_failure`.
- Offline replay (`python3 -m shadow.replay`) requires all 7 canonical `REQUIRED_MARKET_SERIES` (`TQQQ`, `SOXL`, `QQQ`, `XNDX`, `SEMI`, `SOFR`, `TBILL_3M`) present, eligible, and independently verified inside `[session_close_utc, decision_cutoff_utc]`, recomputes `market_records_digest` and `inputs_digest` (including `W_ref`), verifies sizing `close_price` alignment, and requires `verify_chain()` + `--trusted-headers <json>` Bitcoin block headers with block timestamps before reporting `receipts_creditable: true`.

```sh
# Offline template validation + simulated multi-day missed-run & replay dry-run
PYTHONPATH=src python3 -m shadow.scheduler dry-run

# Isolated synthetic fixture simulation (refuses drill directories/chains)
PYTHONPATH=src python3 -m shadow.scheduler simulate-drill \
  --session 2025-11-21 \
  --now-utc 2025-11-21T22:30:00.000Z \
  --chain-id sim-check-v1 \
  --fixtures-dir tests/fixtures/drill \
  --data-dir /tmp/shadow-sim-check

# Independent missed-run check (exit code 2 if silent missed run or backfill anomaly; 1 on insufficient calendar coverage)
PYTHONPATH=src python3 -m shadow.scheduler detect-missed \
  --start-date 2025-11-21 \
  --as-of-utc 2025-11-26T05:00:00.000Z \
  --data-dir /tmp/shadow-drill-check
```

## 4. Weekday Live Market Carry-Overs Before `OA-SH1-6` Activation

1. Run a live weekday post-`16:00` ET EOD availability timing measurement from Mac mini (`dev` user) to finalize `decision_instant_et` / `decision_cutoff_et` (currently provisional `"17:30"` / `"18:00"` ET regular and `"15:00"` ET half-day in `config/drill-scheduler.json`) and note half-day `13:00` ET session timing before live `OA-SH1-6` activation.
2. Verify `TIINGO_API_KEY` (`OA-SH1-2`), active U.S. Treasury yield CSV/XML endpoint (`treasury_endpoint` in `config/drill-scheduler.json`) vs FRED `DTB3`, and Yahoo spaced TR index symbols (`yahoo_symbol_map` in `config/drill-scheduler.json`, `^XNDX` / `XSOX`) on a weekday.
3. Verify `csa22-v2` (`2026`) calendar spec is mounted/merged before running live 2026 drills.
4. Capture live public OpenTimestamps HTTP `.ots` stamping plus Bitcoin block-header verification (`--trusted-headers`), `SH1-A12` (autonomous firing with zero AI/SSH sessions), and `SH1-A13` (peak RSS, wall time, and 3 MCA runner co-residency check) directly on Mac mini.
