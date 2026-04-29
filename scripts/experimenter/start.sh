#!/bin/zsh
# Start the AI experimenter + dashboard in one command.
#
# Usage:
#   ./scripts/experimenter/start.sh                     # resume with same settings
#   ./scripts/experimenter/start.sh --experiments 30    # run 30 more from current count
#   ./scripts/experimenter/start.sh --budget 10         # set budget in USD
#   ./scripts/experimenter/start.sh --model minimax/minimax-m2  # override OpenRouter model
# Planner truncates RESULTS.md by default (~120k char tail). Override: EXPERIMENTER_RESULTS_MAX_CHARS=180000 EXPERIMENTER_FULL_RESULTS=1
#
# The dashboard opens automatically at http://localhost:7331

set -e

SCRIPT_DIR="${0:A:h}"
ROOT="${SCRIPT_DIR:h:h}"

# ── Defaults ───────────────────────────────────────────────────────────────────
BUDGET_USD=5.0
MAX_PARALLEL=4
TIMEOUT_MIN=20
MAX_RUN_MIN=20
EXTRA_EXPERIMENTS=""

# ── Parse args ─────────────────────────────────────────────────────────────────
MODEL="${EXPERIMENTER_MODEL:-minimax/minimax-m2.7}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --experiments|-e) EXTRA_EXPERIMENTS="$2"; shift 2 ;;
    --budget|-b)      BUDGET_USD="$2";         shift 2 ;;
    --parallel|-p)    MAX_PARALLEL="$2";       shift 2 ;;
    --model|-m)       MODEL="$2";              shift 2 ;;
    *) echo "Unknown arg: $1"; shift ;;
  esac
done

# ── Compute max-experiments (current completed + requested more) ───────────────
STATE_FILE="$SCRIPT_DIR/state.json"
if [[ -n "$EXTRA_EXPERIMENTS" ]]; then
  CURRENT=$(python3 -c "import json; d=json.load(open('$STATE_FILE')); print(d.get('experiments_completed', 0))" 2>/dev/null || echo 0)
  MAX_EXPERIMENTS=$(( CURRENT + EXTRA_EXPERIMENTS ))
else
  # Default: run until budget runs out (set a very high cap)
  MAX_EXPERIMENTS=9999
fi

# ── Check API key ──────────────────────────────────────────────────────────────
if [[ -z "$OPENROUTER_API_KEY" ]]; then
  echo "ERROR: OPENROUTER_API_KEY is not set."
  echo "  Run: export OPENROUTER_API_KEY=sk-or-..."
  exit 1
fi

# ── Kill any existing instances ────────────────────────────────────────────────
echo "Stopping any existing orchestrator/dashboard..."
pkill -f "orchestrator.py" 2>/dev/null || true
pkill -f "dashboard.py"    2>/dev/null || true
sleep 1

# ── Start orchestrator ─────────────────────────────────────────────────────────
LOG_FILE="$SCRIPT_DIR/orchestrator.log"
echo "" >> "$LOG_FILE"   # separator between sessions
echo "=== Session start $(date) ===" >> "$LOG_FILE"

echo "Starting orchestrator (Exp limit: $MAX_EXPERIMENTS, Budget: \$$BUDGET_USD, model: $MODEL)..."
cd "$ROOT"
PYTHONHASHSEED=0 caffeinate -i python3 scripts/experimenter/orchestrator.py \
  --budget-usd     "$BUDGET_USD"      \
  --max-experiments "$MAX_EXPERIMENTS" \
  --max-parallel   "$MAX_PARALLEL"    \
  --timeout-minutes "$TIMEOUT_MIN"    \
  --max-run-minutes "$MAX_RUN_MIN"    \
  --model          "$MODEL"           \
  2>&1 | tee -a "$LOG_FILE" &

ORCH_PID=$!
echo "Orchestrator PID: $ORCH_PID"

# ── Start dashboard ────────────────────────────────────────────────────────────
sleep 1
echo "Starting dashboard..."
python3 "$SCRIPT_DIR/dashboard.py" &
DASH_PID=$!
echo "Dashboard PID: $DASH_PID"

# ── Open browser ───────────────────────────────────────────────────────────────
sleep 2
open "http://localhost:7331" 2>/dev/null || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Orchestrator running  (pid $ORCH_PID)"
echo "  Dashboard:  http://localhost:7331"
echo "  Logs:       scripts/experimenter/orchestrator.log"
echo "  Results:    RESULTS.md"
echo ""
echo "  To stop:    pkill -f orchestrator.py && pkill -f dashboard.py"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
