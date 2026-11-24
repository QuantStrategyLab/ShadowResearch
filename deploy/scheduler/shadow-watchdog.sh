#!/usr/bin/env bash
# Fallback user-level watchdog/crontab runner for Mac mini when systemd linger is unavailable.
# Strictly drill-chain only (OA-SH1-6 gated); never runs production mode or CLK-01.
set -euo pipefail

REPO_DIR="${SHADOW_REPO_DIR:-$HOME/shadow-research}"
DATA_DIR="${SHADOW_DATA_DIR:-$REPO_DIR/data/drill}"
CFG_PATH="${SHADOW_CONFIG_PATH:-$REPO_DIR/config/drill-scheduler.json}"
LOG_DIR="$DATA_DIR/logs"
mkdir -p "$LOG_DIR"

if [[ -f "$HOME/dev-env-mini/env.sh" ]]; then
  # shellcheck source=/dev/null
  source "$HOME/dev-env-mini/env.sh"
fi

export PYTHONPATH="$REPO_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
CMD="${1:-}"

case "$CMD" in
  run-drill)
    SESSION="${2:-auto}"
    EXTRA_ARGS=()
    if [[ -n "${SHADOW_REQUIRE_NY_HHMM:-}" ]]; then
      EXTRA_ARGS+=(--require-ny-hhmm "$SHADOW_REQUIRE_NY_HHMM")
    fi
    exec nice -n 15 python3 -m shadow.scheduler run-drill \
      --session "$SESSION" \
      --data-dir "$DATA_DIR" \
      --config "$CFG_PATH" \
      "${EXTRA_ARGS[@]}" \
      >> "$LOG_DIR/shadow-drill.log" 2>&1
    ;;
  detect-missed)
    START_DATE="${2:-${SHADOW_WINDOW_START:-2026-11-24}}"
    EXTRA_ARGS=()
    if [[ -n "${SHADOW_REQUIRE_NY_HHMM:-}" ]]; then
      EXTRA_ARGS+=(--require-ny-hhmm "$SHADOW_REQUIRE_NY_HHMM")
    fi
    exec nice -n 15 python3 -m shadow.scheduler detect-missed \
      --start-date "$START_DATE" \
      --data-dir "$DATA_DIR" \
      --config "$CFG_PATH" \
      "${EXTRA_ARGS[@]}" \
      >> "$LOG_DIR/shadow-missed-run.log" 2>&1
    ;;
  *)
    echo "Usage: $0 {run-drill [YYYY-MM-DD]|detect-missed [START_DATE]}" >&2
    exit 64
    ;;
esac
