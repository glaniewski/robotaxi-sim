"""
Real-time experimenter dashboard.

Run separately from the experimenter:
    python3 scripts/experimenter/dashboard.py
Then open http://localhost:7331 in your browser.

The server uses uvicorn ``reload=True`` so edits to this file restart the process
automatically (no manual restart). Hard-refresh the browser (⌘⇧R) if the tab still
shows old UI.

Reads state.json and the active terminal log — no changes to the experimenter needed.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn

ROOT = Path(__file__).resolve().parents[2]
STATE_PATH = Path(__file__).parent / "state.json"
PROGRESS_DIR = Path(__file__).parent / "progress"
DEDICATED_LOG = Path(__file__).parent / "orchestrator.log"
TERMINALS_DIR = Path.home() / ".cursor/projects/Users-lanie-Desktop-robotaxi-sim/terminals"

app = FastAPI()


def _find_active_log() -> Path | None:
    """
    Find the active orchestrator log.
    Prefers the dedicated orchestrator.log file (written by tee when running
    the orchestrator from a shell). Falls back to the most recently modified
    Cursor terminal file that contains orchestrator output.
    """
    if DEDICATED_LOG.exists() and DEDICATED_LOG.stat().st_size > 0:
        return DEDICATED_LOG
    candidates = sorted(
        TERMINALS_DIR.glob("*.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for p in candidates:
        try:
            text = p.read_text(errors="replace")
            if "orchestrator" in text and "experimenter" in text:
                return p
        except OSError:
            continue
    return candidates[0] if candidates else None


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def _load_run_progress() -> dict[str, dict]:
    """Read live tqdm progress files for running sims."""
    result = {}
    if not PROGRESS_DIR.exists():
        return result
    for p in PROGRESS_DIR.glob("*.json"):
        try:
            result[p.stem] = json.loads(p.read_text())
        except Exception:
            pass
    return result


def _parse_logs(log_path: Path) -> list[str]:
    try:
        lines = log_path.read_text(errors="replace").splitlines()
        content_lines = [l for l in lines if re.match(r"^\d{2}:\d{2}:\d{2}", l)]
        return content_lines[-150:]
    except Exception:
        return []


def _parse_current_exp(log_lines: list[str]) -> dict:
    """
    Extract current experiment info from log lines.

    Two-pass approach:
      1. Scan in reverse to find the index of the most recent "Planning ExpN" line.
      2. Replay lines from that point forward to build correct run states.
    """
    # Pass 1: find where the current experiment starts
    start_idx = None
    exp_num = None
    for i in range(len(log_lines) - 1, -1, -1):
        m = re.search(r"Planning Exp(\d+)", log_lines[i])
        if m:
            exp_num = int(m.group(1))
            start_idx = i
            break

    if start_idx is None:
        return {"exp_number": None, "hypothesis": "", "runs": [], "batch_info": ""}

    # Pass 2: replay forward from start of this experiment
    hypothesis = ""
    runs: dict[str, dict] = {}  # run_id → state
    batch_info = ""

    for line in log_lines[start_idx:]:
        if "Hypothesis:" in line and not hypothesis:
            hypothesis = line.split("Hypothesis:", 1)[-1].strip()

        m = re.search(r"Launched run (\S+) \(pid (\d+)\): (\S+)", line)
        if m:
            rid = m.group(1)
            runs[rid] = {"id": rid, "pid": m.group(2), "script": m.group(3),
                         "status": "running", "served_pct": None, "wall_s": None}

        m = re.search(r"Run (\S+) done: exit=(-?\d+) wall=(\d+)s.*served%=([\d.]+)", line)
        if m:
            rid = m.group(1)
            if rid in runs:
                runs[rid].update({"status": "done", "wall_s": int(m.group(3)),
                                  "served_pct": float(m.group(4))})

        if ("killing early" in line or "ETA kill" in line or "CANCELLED" in line):
            m = re.search(r"[Rr]un (\S+)", line)
            if m and m.group(1) in runs:
                runs[m.group(1)]["status"] = "killed"

        if "Batch " in line and "launching" in line:
            batch_info = line.split("—")[-1].strip() if "—" in line else line

    return {
        "exp_number": exp_num,
        "hypothesis": hypothesis,
        "runs": list(runs.values()),
        "batch_info": batch_info,
    }


def _parse_results_md() -> list[dict]:
    """
    Parse RESULTS.md to build a complete, durable experiment history.
    Works across restarts and session boundaries.
    Each entry: {exp_number, hypothesis, avg_served_pct, avg_cost_per_trip, vehicle_preset, runs, outcome}
    """
    results_path = ROOT / "RESULTS.md"
    try:
        text = results_path.read_text(errors="replace")
    except Exception:
        return []

    exps = []
    parts = re.split(r"\n## Experiment (\d+)", text)
    for i in range(1, len(parts), 2):
        try:
            exp_num = int(parts[i])
        except ValueError:
            continue
        body = parts[i + 1] if i + 1 < len(parts) else ""

        hyp_m = re.search(r"\*\*Hypothesis:\*\*\s+(.+)", body)
        hypothesis = hyp_m.group(1).strip() if hyp_m else ""

        served_values = [float(m.group(1)) for m in re.finditer(r"\|\s*served%\s*\|\s*([\d.]+)\s*\|", body)]
        cost_values = [float(m.group(1)) for m in re.finditer(r"\|\s*cost_per_trip\s*\|\s*([\d.]+)\s*\|", body)]
        p10_values = [float(m.group(1)) for m in re.finditer(r"\|\s*p10_wait_min\s*\|\s*([\d.]+)\s*\|", body)]
        median_wait_values = [float(m.group(1)) for m in re.finditer(r"\|\s*median_wait_min\s*\|\s*([\d.]+)\s*\|", body)]
        p90_wait_values = [float(m.group(1)) for m in re.finditer(r"\|\s*p90_wait_min\s*\|\s*([\d.]+)\s*\|", body)]

        # Experiment-level preset (fallback for old rows without per-run Config)
        preset_m = re.search(r"preset=(\w+)", body)
        vehicle_preset = preset_m.group(1).lower() if preset_m else None

        runs = []
        for rm in re.finditer(
            r"##### Run `([^`]+)`.*?wall (\d+)s\).*?\*\*Config:\*\*\s*`([^`]+)`",
            body,
            re.DOTALL,
        ):
            cfg = rm.group(3)
            pr = re.search(r"preset=(\w+)", cfg)
            run_preset = pr.group(1).lower() if pr else None
            runs.append({
                "run_id": rm.group(1),
                "wall_s": int(rm.group(2)),
                "status": "done",
                "served_pct": None,
                "cost_per_trip": None,
                "preset": run_preset,
            })
        # Legacy blocks without **Config:** line — keep wall-only parsing
        if not runs:
            for rm in re.finditer(r"##### Run `([^`]+)`.*?wall (\d+)s\)", body):
                runs.append({
                    "run_id": rm.group(1),
                    "wall_s": int(rm.group(2)),
                    "status": "done",
                    "served_pct": None,
                    "cost_per_trip": None,
                    "preset": None,
                })
        for j, v in enumerate(served_values):
            if j < len(runs):
                runs[j]["served_pct"] = v
        for j, v in enumerate(cost_values):
            if j < len(runs):
                runs[j]["cost_per_trip"] = v
        for j, v in enumerate(p10_values):
            if j < len(runs):
                runs[j]["p10_wait_min"] = v
        for j, v in enumerate(median_wait_values):
            if j < len(runs):
                runs[j]["p50_wait_min"] = v
        for j, v in enumerate(p90_wait_values):
            if j < len(runs):
                runs[j]["p90_wait_min"] = v
        for r in runs:
            if r.get("preset") is None:
                r["preset"] = vehicle_preset

        finding_m = re.search(r"### Finding\s*\n+(.+?)(?=\n#|\Z)", body, re.DOTALL)
        outcome = finding_m.group(1).strip()[:300] if finding_m else ""

        avg_served = round(sum(served_values) / len(served_values), 1) if served_values else None
        avg_cost = round(sum(cost_values) / len(cost_values), 2) if cost_values else None

        def _avg(xs: list[float]) -> float | None:
            return round(sum(xs) / len(xs), 2) if xs else None

        exps.append({
            "exp_number": exp_num,
            "hypothesis": hypothesis,
            "avg_served_pct": avg_served,
            "avg_cost_per_trip": avg_cost,
            "avg_p10_wait_min": _avg(p10_values),
            "avg_p50_wait_min": _avg(median_wait_values),
            "avg_p90_wait_min": _avg(p90_wait_values),
            "vehicle_preset": vehicle_preset,
            "runs": runs,
            "outcome": outcome,
        })

    seen: dict[int, dict] = {}
    for e in exps:
        n = e["exp_number"]
        if n not in seen or (e["avg_served_pct"] is not None and seen[n]["avg_served_pct"] is None) or len(e["runs"]) > len(seen[n]["runs"]):
            seen[n] = e

    return sorted(seen.values(), key=lambda e: e["exp_number"])


@app.get("/api/data")
def get_data():
    state = _load_state()
    log_path = _find_active_log()
    log_lines = _parse_logs(log_path) if log_path else []
    current_exp = _parse_current_exp(log_lines)
    run_progress = _load_run_progress()
    exp_history = _parse_results_md()

    for run in current_exp.get("runs", []):
        if run["status"] == "running" and run["id"] in run_progress:
            run["progress"] = run_progress[run["id"]]

    spend = state.get("total_spend_usd", 0)

    # Build Pareto scatter data from experiment history runs
    pareto_points = []
    for exp in exp_history:
        preset = exp.get("vehicle_preset")
        for run in exp.get("runs", []):
            sp = run.get("served_pct")
            cp = run.get("cost_per_trip")
            if sp is not None and cp is not None:
                pr = (run.get("preset") or preset or "tesla").lower()
                if pr not in ("tesla", "waymo"):
                    pr = "tesla"
                pareto_points.append({
                    "exp": exp["exp_number"],
                    "run_id": run["run_id"],
                    "served_pct": sp,
                    "cost_per_trip": cp,
                    "preset": pr,
                    "p10_wait_min": run.get("p10_wait_min"),
                    "p50_wait_min": run.get("p50_wait_min"),
                    "p90_wait_min": run.get("p90_wait_min"),
                })

    return JSONResponse(
        {
        "spend_usd": round(spend, 4),
        "budget_usd": 5.0,
        "exps_completed": state.get("experiments_completed", 0),
        "arms_completed": state.get("arms_completed", 0),
        "arms_killed": state.get("arms_killed", 0),
        "next_exp": state.get("next_exp_number", 76),
        "current_exp": current_exp,
        "exp_history": exp_history,
        "pareto_points": pareto_points,
        "log_lines": log_lines[-60:],
        "updated_at": datetime.now().strftime("%H:%M:%S"),
        },
        headers={"Cache-Control": "no-store"},
    )


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(
        HTML,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
        },
    )


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Robotaxi Experimenter</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'SF Mono', 'Fira Code', monospace; background: #0d1117; color: #e6edf3; font-size: 13px; }

  /* ── Header ── */
  .header { background: #161b22; border-bottom: 1px solid #30363d; padding: 10px 20px; display: flex; align-items: center; gap: 24px; flex-wrap: wrap; }
  .header h1 { font-size: 15px; color: #58a6ff; font-weight: 600; white-space: nowrap; }
  .stat { display: flex; flex-direction: column; gap: 2px; }
  .stat-label { font-size: 10px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; }
  .stat-value { font-size: 15px; font-weight: 600; color: #e6edf3; }
  .stat-value.green { color: #3fb950; }
  .stat-value.yellow { color: #d29922; }
  .budget-bar-wrap { flex: 1; min-width: 140px; }
  .budget-bar { height: 5px; background: #21262d; border-radius: 3px; overflow: hidden; margin-top: 5px; }
  .budget-fill { height: 100%; background: #3fb950; border-radius: 3px; transition: width 0.5s; }
  .updated { margin-left: auto; font-size: 11px; color: #8b949e; white-space: nowrap; }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #3fb950; display: inline-block; margin-right: 6px; animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

  /* ── Layout ── */
  .main { display: grid; grid-template-columns: 1fr 1fr; grid-template-rows: auto auto 1fr; gap: 1px; height: calc(100vh - 50px); background: #21262d; overflow: hidden; }
  .panel { background: #0d1117; padding: 12px 14px; overflow: auto; }
  .panel-title { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #8b949e; margin-bottom: 10px; border-bottom: 1px solid #21262d; padding-bottom: 6px; display: flex; justify-content: space-between; align-items: center; }

  /* ── Current experiment ── */
  .current-exp { grid-column: 1 / -1; }
  .exp-hyp { color: #e6edf3; line-height: 1.5; margin-bottom: 10px; font-size: 12px; }
  .runs-grid { display: flex; gap: 8px; flex-wrap: wrap; }
  .run-card { background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 9px 13px; min-width: 190px; }
  .run-card.running { border-color: #1f6feb; }
  .run-card.done    { border-color: #3fb950; }
  .run-card.killed  { border-color: #f85149; }
  .run-id { font-weight: 600; font-size: 12px; margin-bottom: 3px; }
  .run-status { font-size: 10px; color: #8b949e; }
  .run-status.running { color: #58a6ff; }
  .run-status.done    { color: #3fb950; }
  .run-status.killed  { color: #f85149; }
  .run-metric { font-size: 14px; font-weight: 600; margin-top: 4px; }
  .run-progress-wrap { margin-top: 6px; }
  .run-progress-bar  { height: 4px; background: #21262d; border-radius: 2px; overflow: hidden; }
  .run-progress-fill { height: 100%; border-radius: 2px; transition: width 0.8s; }
  .run-progress-fill.running { background: #1f6feb; }
  .run-progress-fill.done    { background: #3fb950; }
  .run-eta { font-size: 10px; color: #8b949e; margin-top: 3px; }

  /* ── Badges ── */
  .badge { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 10px; font-weight: 600; white-space: nowrap; }
  .badge.pending { background: #1f3a5f; color: #58a6ff; }
  .badge.done    { background: #1a3a1e; color: #3fb950; }

  /* ── Pareto chart ── */
  .pareto-wrap { position: relative; height: 300px; margin-bottom: 4px; }
  .pareto-svg { width: 100%; height: 100%; }

  /* ── History chart ── */
  .chart-wrap { position: relative; height: 80px; margin-bottom: 8px; }
  .chart-svg { width: 100%; height: 100%; }
  .history-table { width: 100%; border-collapse: collapse; font-size: 11px; }
  .history-table th { text-align: left; padding: 4px 8px; font-size: 10px; text-transform: uppercase; color: #8b949e; border-bottom: 1px solid #21262d; }
  .history-table td { padding: 4px 8px; border-bottom: 1px solid #161b22; vertical-align: top; }
  .history-table tr.clickable { cursor: pointer; }
  .history-table tr.clickable:hover td { background: #161b22; }
  .served-bar-cell { width: 80px; }
  .served-bar { height: 8px; border-radius: 2px; background: #3fb950; display: inline-block; vertical-align: middle; }
  .served-bar.low { background: #f85149; }
  .served-bar.mid { background: #d29922; }

  /* ── Logs ── */
  .log-panel { font-size: 11px; line-height: 1.55; }
  .log-line { padding: 1px 0; white-space: pre-wrap; word-break: break-all; }
  .log-line.info     { color: #c9d1d9; }
  .log-line.warning  { color: #d29922; }
  .log-line.error    { color: #f85149; }
  .log-line.http     { color: #8b949e; }
  .log-line.launched { color: #58a6ff; }
  .log-line.done     { color: #3fb950; }
  .log-line.kill     { color: #f85149; font-weight: 600; }
  .log-line.planning { color: #e3b341; font-weight: 600; }

  /* ── Modal ── */
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.7); z-index: 100; align-items: center; justify-content: center; }
  .modal-overlay.open { display: flex; }
  .modal { background: #161b22; border: 1px solid #30363d; border-radius: 10px; max-width: 680px; width: 90%; max-height: 80vh; overflow: auto; padding: 22px 24px; position: relative; }
  .modal h2 { font-size: 13px; color: #58a6ff; margin-bottom: 12px; }
  .modal-section { margin-bottom: 14px; }
  .modal-section h3 { font-size: 10px; text-transform: uppercase; letter-spacing: 0.8px; color: #8b949e; margin-bottom: 5px; }
  .modal-section p { color: #e6edf3; line-height: 1.6; font-size: 12px; }
  .modal-close { position: absolute; top: 12px; right: 14px; background: none; border: none; color: #8b949e; font-size: 18px; cursor: pointer; }
  .modal-close:hover { color: #e6edf3; }
</style>
</head>
<body>

<!-- Header -->
<div class="header">
  <h1><span class="dot"></span>Robotaxi Experimenter</h1>
  <div class="stat">
    <div class="stat-label">Experiment</div>
    <div class="stat-value" id="exp-num">—</div>
  </div>
  <div class="stat">
    <div class="stat-label">Exps Done</div>
    <div class="stat-value green" id="exps-done">—</div>
  </div>
  <div class="stat">
    <div class="stat-label">Arms / Killed</div>
    <div class="stat-value" id="arms-done">—</div>
  </div>
  <div class="stat budget-bar-wrap">
    <div class="stat-label">Session LLM Spend&nbsp;<span id="spend-pct" style="color:#e6edf3;font-weight:400"></span></div>
    <div class="stat-value yellow" id="spend">$0.0000</div>
    <div class="budget-bar"><div class="budget-fill" id="budget-fill" style="width:0%"></div></div>
  </div>
  <div class="updated" id="updated">—</div>
</div>

<!-- Grid -->
<div class="main">

  <!-- Current Experiment -->
  <div class="panel current-exp">
    <div class="panel-title">Current Experiment — <span id="cur-exp-num">—</span></div>
    <div class="exp-hyp" id="cur-hypothesis">Waiting for experiment…</div>
    <div class="runs-grid" id="runs-grid"></div>
  </div>

  <!-- Pareto Frontier -->
  <div class="panel">
    <div class="panel-title" style="flex-wrap:wrap;gap:8px">
      <span>
        Pareto Frontier <span style="color:#e6edf3;text-transform:none;font-size:10px">— cost/trip vs served%</span>
      </span>
      <label style="font-size:10px;color:#8b949e;font-weight:400;cursor:pointer;user-select:none;white-space:nowrap">
        <input type="checkbox" id="paretoYFull" style="vertical-align:middle;margin-right:4px"/>
        <span id="pareto-yfull-label">Full served% (50–100, incl. 0 outliers)</span>
      </label>
    </div>
    <div class="pareto-wrap"><svg class="pareto-svg" id="pareto-svg"></svg></div>
    <div style="font-size:10px;color:#8b949e;margin-top:6px;line-height:1.45">
      <span style="color:#58a6ff">● Tesla</span> &nbsp;
      <span style="color:#f0883e">● Waymo</span>
      <span style="opacity:0.85"> — each dot is one run; <strong style="color:#c9d1d9">x = cost/trip (log scale)</strong>. Tooltip shows rider wait <strong style="color:#c9d1d9">p10 / p50 / p90</strong> (min) when logged. Default Y is 85–100%; runs below 85% served are hidden until you enable full range (count shown in the toggle).</span>
    </div>
  </div>

  <!-- Experiment History -->
  <div class="panel">
    <div class="panel-title">Experiment History <span style="color:#e6edf3;text-transform:none;font-size:10px">— click row for details</span></div>
    <div class="chart-wrap"><svg class="chart-svg" id="chart-svg"></svg></div>
    <table class="history-table">
      <thead><tr><th>Exp</th><th>Hypothesis</th><th>Avg served%</th><th>Cost/trip</th><th title="Mean over completed arms">Wait p10/p50/p90</th><th>Runs</th></tr></thead>
      <tbody id="history-body"></tbody>
    </table>
  </div>

  <!-- Live Logs -->
  <div class="panel log-panel" id="log-panel">
    <div class="panel-title">Live Logs</div>
    <div id="log-lines"></div>
  </div>

</div>

<!-- Modal -->
<div class="modal-overlay" id="modal-overlay" onclick="closeModal(event)">
  <div class="modal" id="modal-content">
    <button class="modal-close" onclick="document.getElementById('modal-overlay').classList.remove('open')">✕</button>
    <h2 id="modal-title">—</h2>
    <div id="modal-body"></div>
  </div>
</div>

<script>
let _lastHistory = [];
let _lastParetoPoints = [];

function openModal(title, sections) {
  document.getElementById('modal-title').textContent = title;
  const body = document.getElementById('modal-body');
  body.innerHTML = sections.map(s =>
    `<div class="modal-section"><h3>${s.label}</h3><p>${s.text.replace(/\n/g,'<br>')}</p></div>`
  ).join('');
  document.getElementById('modal-overlay').classList.add('open');
}
function closeModal(e) {
  if (e.target === document.getElementById('modal-overlay'))
    document.getElementById('modal-overlay').classList.remove('open');
}

async function refresh() {
  let d;
  try {
    const r = await fetch('/api/data');
    d = await r.json();
  } catch(e) { return; }

  // ── Header ──
  document.getElementById('exp-num').textContent = 'Exp#' + d.next_exp;
  document.getElementById('exps-done').textContent = d.exps_completed;
  document.getElementById('arms-done').textContent = d.arms_completed + ' / ' + (d.arms_killed || 0);
  document.getElementById('spend').textContent = '$' + d.spend_usd.toFixed(4);
  const pct = (d.spend_usd / d.budget_usd * 100).toFixed(1);
  document.getElementById('spend-pct').textContent = '(' + pct + '% of $' + d.budget_usd + ')';
  document.getElementById('budget-fill').style.width = Math.min(pct, 100) + '%';
  document.getElementById('updated').textContent = 'Updated ' + d.updated_at;

  // ── Current Exp ──
  const ce = d.current_exp;
  document.getElementById('cur-exp-num').textContent = ce.exp_number ? 'Exp' + ce.exp_number : '—';
  document.getElementById('cur-hypothesis').textContent = ce.hypothesis || '—';

  const grid = document.getElementById('runs-grid');
  grid.innerHTML = '';
  (ce.runs || []).forEach(run => {
    const card = document.createElement('div');
    card.className = 'run-card ' + run.status;
    const wall = run.wall_s ? run.wall_s + 's' : '';
    let metricHtml = '', progressHtml = '';

    if (run.status === 'done' && run.served_pct != null) {
      const color = run.served_pct >= 95 ? '#3fb950' : run.served_pct >= 85 ? '#d29922' : '#f85149';
      metricHtml = `<div class="run-metric" style="color:${color}">${run.served_pct.toFixed(1)}% served</div>`;
      progressHtml = `<div class="run-progress-wrap"><div class="run-progress-bar"><div class="run-progress-fill done" style="width:100%"></div></div><div class="run-eta">✓ ${wall}</div></div>`;
    } else if (run.status === 'killed') {
      metricHtml = `<div class="run-metric" style="color:#f85149">⚡ killed</div>`;
      progressHtml = wall ? `<div class="run-eta">${wall} elapsed</div>` : '';
    } else {
      const prog = run.progress;
      if (prog) {
        const p = prog.pct;
        const etaMin = (prog.eta_s / 60).toFixed(1);
        const elMin  = (prog.elapsed_s / 60).toFixed(1);
        metricHtml = `<div class="run-metric" style="color:#58a6ff">${p}%</div>`;
        progressHtml = `<div class="run-progress-wrap"><div class="run-progress-bar"><div class="run-progress-fill running" style="width:${p}%"></div></div><div class="run-eta">${elMin}m elapsed · ~${etaMin}m left</div></div>`;
      } else {
        metricHtml = `<div class="run-metric" style="color:#8b949e">starting…</div>`;
      }
    }
    card.innerHTML = `<div class="run-id">${run.id}</div>
      <div class="run-status ${run.status}">${run.status.toUpperCase()}${wall ? ' · ' + wall : ''}</div>
      ${metricHtml}${progressHtml}`;
    grid.appendChild(card);
  });

  // ── Pareto chart ──
  _lastParetoPoints = d.pareto_points || [];
  renderPareto(_lastParetoPoints);

  // ── History chart + table ──
  _lastHistory = d.exp_history || [];
  renderChart(_lastHistory);

  const hbody = document.getElementById('history-body');
  hbody.innerHTML = '';
  _lastHistory.forEach(e => {
    const tr = document.createElement('tr');
    tr.className = 'clickable';
    tr.onclick = () => {
      const runLines = (e.runs || []).map(r => {
        let w = '';
        if (r.p10_wait_min != null && r.p50_wait_min != null && r.p90_wait_min != null) {
          w = ` | wait p10/p50/p90 ${r.p10_wait_min.toFixed(1)}/${r.p50_wait_min.toFixed(1)}/${r.p90_wait_min.toFixed(1)}m`;
        }
        return `  ${r.run_id}: ${r.served_pct != null ? r.served_pct.toFixed(1)+'% served'+w+', '+r.wall_s+'s' : r.status}`;
      }).join('\n');
      openModal('Exp' + e.exp_number, [
        { label: 'Hypothesis', text: e.hypothesis || '—' },
        { label: 'Runs', text: runLines || '—' },
        ...(e.outcome ? [{ label: 'Outcome', text: e.outcome }] : []),
      ]);
    };
    const sp = e.avg_served_pct;
    const barClass = sp == null ? '' : sp >= 95 ? '' : sp >= 85 ? 'mid' : 'low';
    const barW = sp != null ? Math.round(sp * 0.8) + 'px' : '0px';
    const spText = sp != null ? sp.toFixed(1) + '%' : '—';
    const cpText = e.avg_cost_per_trip != null ? '$' + e.avg_cost_per_trip.toFixed(2) : '—';
    const w10 = e.avg_p10_wait_min, w50 = e.avg_p50_wait_min, w90 = e.avg_p90_wait_min;
    const waitText = (w10 != null && w50 != null && w90 != null)
      ? `${w10.toFixed(1)} / ${w50.toFixed(1)} / ${w90.toFixed(1)}`
      : '—';
    const hyp = (e.hypothesis || '').substring(0, 50) + ((e.hypothesis || '').length > 50 ? '…' : '');
    tr.innerHTML = `<td style="white-space:nowrap;color:#58a6ff">Exp${e.exp_number}</td>
      <td>${hyp}</td>
      <td class="served-bar-cell" style="white-space:nowrap">
        <span class="served-bar ${barClass}" style="width:${barW}"></span>
        <span style="margin-left:4px;color:${barClass==='low'?'#f85149':barClass==='mid'?'#d29922':'#3fb950'}">${spText}</span>
      </td>
      <td style="color:#d29922;white-space:nowrap">${cpText}</td>
      <td style="color:#79c0ff;white-space:nowrap;font-size:11px" title="p10 / p50 / p90 wait (min), averaged over arms">${waitText}</td>
      <td style="color:#8b949e">${(e.runs||[]).length}</td>`;
    hbody.appendChild(tr);
  });

  // ── Logs ──
  const logDiv = document.getElementById('log-lines');
  const atBottom = logDiv.parentElement.scrollTop + logDiv.parentElement.clientHeight >= logDiv.parentElement.scrollHeight - 24;
  logDiv.innerHTML = '';
  (d.log_lines || []).forEach(line => {
    const div = document.createElement('div');
    div.className = 'log-line ' + classifyLog(line);
    div.textContent = line;
    logDiv.appendChild(div);
  });
  if (atBottom) logDiv.parentElement.scrollTop = logDiv.parentElement.scrollHeight;
}

function renderChart(history) {
  const svg = document.getElementById('chart-svg');
  const points = history.filter(e => e.avg_served_pct != null && e.runs && e.runs.length > 0);
  if (points.length < 2) { svg.innerHTML = ''; return; }

  const W = svg.clientWidth || 400, H = 80;
  const pad = { l: 28, r: 8, t: 6, b: 18 };
  const xs = points.map((_, i) => pad.l + i * (W - pad.l - pad.r) / (points.length - 1));
  const minY = 40, maxY = 100;
  const ys = points.map(e => pad.t + (H - pad.t - pad.b) * (1 - (e.avg_served_pct - minY) / (maxY - minY)));

  const gridLines = [60, 75, 90, 95].map(v => {
    const y = pad.t + (H - pad.t - pad.b) * (1 - (v - minY) / (maxY - minY));
    return `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" stroke="#21262d" stroke-width="1"/>
            <text x="${pad.l - 4}" y="${y + 3}" fill="#8b949e" font-size="8" text-anchor="end">${v}</text>`;
  }).join('');

  const path = 'M ' + xs.map((x, i) => `${x},${ys[i]}`).join(' L ');
  const area = path + ` L ${xs[xs.length-1]},${H - pad.b} L ${xs[0]},${H - pad.b} Z`;

  const dots = points.map((e, i) => {
    const c = e.avg_served_pct >= 95 ? '#3fb950' : e.avg_served_pct >= 85 ? '#d29922' : '#f85149';
    return `<circle cx="${xs[i]}" cy="${ys[i]}" r="3" fill="${c}" stroke="#0d1117" stroke-width="1.5">
              <title>Exp${e.exp_number}: ${e.avg_served_pct}%</title></circle>`;
  }).join('');

  const labels = points.filter((_, i) => i % Math.ceil(points.length / 8) === 0 || i === points.length - 1).map((e, _, arr) => {
    const idx = points.indexOf(e);
    return `<text x="${xs[idx]}" y="${H - 4}" fill="#8b949e" font-size="8" text-anchor="middle">${e.exp_number}</text>`;
  }).join('');

  svg.innerHTML = `<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">
    ${gridLines}
    <path d="${area}" fill="#1f6feb" fill-opacity="0.12"/>
    <path d="${path}" fill="none" stroke="#58a6ff" stroke-width="1.5" stroke-linejoin="round"/>
    ${dots}
    ${labels}
  </svg>`;
}

function renderPareto(points) {
  const svg = document.getElementById('pareto-svg');
  const pts = points || [];
  const nOutliers = pts.filter((p) => p.served_pct < 85).length;
  const lbl = document.getElementById('pareto-yfull-label');
  if (lbl) {
    lbl.textContent =
      nOutliers === 0
        ? 'Full served% (50–100, incl. no outliers)'
        : `Full served% (50–100, incl. ${nOutliers} outlier${nOutliers === 1 ? '' : 's'})`;
  }
  if (!pts.length) { svg.innerHTML = '<text x="50%" y="50%" fill="#8b949e" font-size="11" text-anchor="middle">No cost data yet — waiting for new experiments</text>'; return; }

  const yFull = document.getElementById('paretoYFull') && document.getElementById('paretoYFull').checked;

  const W = svg.clientWidth || 400;
  const H = svg.clientHeight || 300;
  const pad = { l: 50, r: 18, t: 14, b: 34 };
  const costs = pts.map((p) => p.cost_per_trip).filter((c) => c > 0);
  if (!costs.length) { svg.innerHTML = '<text x="50%" y="50%" fill="#8b949e" font-size="11" text-anchor="middle">No positive cost/trip values</text>'; return; }

  const rawMin = Math.min(...costs);
  const rawMax = Math.max(...costs);
  function niceAxisMax(v) {
    if (v <= 0) return 1;
    const want = v * 1.12;
    const exp = Math.floor(Math.log10(want));
    const mag = Math.pow(10, exp);
    const m = want / mag;
    const nice = m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10;
    return nice * mag;
  }
  // Log X: avoid $0.10–$0.90 dead zone when all costs are ≥ ~$1 (floor(log10(0.85)) was −1 → 0.1)
  let X_MIN;
  if (rawMin >= 1.0) {
    X_MIN = Math.max(0.05, Math.pow(10, Math.floor(Math.log10(rawMin))));
  } else {
    X_MIN = Math.max(0.05, Math.pow(10, Math.floor(Math.log10(Math.max(0.06, rawMin * 0.85)))));
  }
  let X_MAX = Math.max(0.5, niceAxisMax(rawMax));
  if (X_MIN >= X_MAX * 0.99) {
    X_MAX = Math.max(X_MAX * 1.2, rawMax * 1.15, X_MIN * 10);
  }
  // Pull left edge up toward ~$1 when cheapest runs are still ~$1+ (avoids empty $0.10–$0.80)
  if (rawMin >= 0.95 && X_MIN < 0.8) {
    X_MIN = Math.max(X_MIN, 0.85);
  }
  const logLo = Math.log10(X_MIN);
  const logHi = Math.log10(X_MAX);

  const Y_MIN = yFull ? 50 : 85;
  const Y_MAX = 100;
  const plotBottom = H - pad.b;
  const chartW = W - pad.l - pad.r;
  const chartH = plotBottom - pad.t;

  const xOfLog = (c) => {
    const cl = Math.max(X_MIN, Math.min(X_MAX, c));
    return pad.l + ((Math.log10(cl) - logLo) / (logHi - logLo)) * chartW;
  };
  const yOf = (s) => pad.t + (1 - (s - Y_MIN) / (Y_MAX - Y_MIN)) * chartH;

  // X is still log-spaced (xOfLog); tick *positions* follow log, but we label every $0.50
  // when the span is modest (coarser steps if that would crowd > ~45 labels).
  let hMin = Math.ceil(X_MIN * 2 - 1e-9);
  let hMax = Math.floor(X_MAX * 2 + 1e-9);
  let stepH = 1; // 1 half-unit = $0.50
  while (stepH <= 1024 && (hMax - hMin) / stepH > 45) stepH *= 2;
  const ticks = [];
  for (let h = Math.ceil(hMin / stepH) * stepH; h <= hMax; h += stepH) {
    ticks.push(h / 2);
  }
  if (ticks.length === 0) {
    ticks.push(Math.max(X_MIN, Math.min(X_MAX, (X_MIN + X_MAX) / 2)));
  }

  let html = '';

  ticks.forEach((v) => {
    const x = xOfLog(v);
    const whole = Math.abs(v - Math.round(v)) < 1e-9;
    const major = whole;
    html += `<line x1="${x}" y1="${pad.t}" x2="${x}" y2="${plotBottom}" stroke="${major ? '#30363d' : '#21262d'}" stroke-width="${major ? 1 : 0.75}" opacity="${major ? 1 : 0.55}"/>`;
    const lab = v >= 10 ? v.toFixed(0) : v.toFixed(2);
    html += `<text x="${x}" y="${H - pad.b + 14}" fill="#8b949e" font-size="8" text-anchor="middle">$${lab}</text>`;
  });

  const yMajors = yFull ? [50, 60, 70, 80, 90, 100] : [85, 90, 95, 100];
  yMajors.forEach((v) => {
    const y = yOf(v);
    html += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" stroke="#30363d" stroke-width="1"/>`;
    html += `<text x="${pad.l - 6}" y="${y + 3}" fill="#8b949e" font-size="8" text-anchor="end">${v}%</text>`;
  });
  const yMinors = yFull ? [55, 65, 75, 85] : [87, 88, 89, 91, 92, 93, 94, 96, 97, 98, 99];
  yMinors.forEach((v) => {
    if (yMajors.includes(v)) return;
    const y = yOf(v);
    html += `<line x1="${pad.l}" y1="${y}" x2="${W - pad.r}" y2="${y}" stroke="#21262d" stroke-width="0.75" opacity="0.45"/>`;
  });

  const y95 = yOf(95);
  html += `<line x1="${pad.l}" y1="${y95}" x2="${W - pad.r}" y2="${y95}" stroke="#3fb950" stroke-width="1.25" stroke-dasharray="5,4" opacity="0.65"/>`;
  html += `<text x="${W - pad.r}" y="${y95 - 5}" fill="#3fb950" font-size="8" text-anchor="end" opacity="0.9">95% target</text>`;

  const xSub = `log scale: $${X_MIN.toFixed(2)} … $${X_MAX < 10 ? X_MAX.toFixed(2) : X_MAX.toFixed(1)}`;
  html += `<text x="${W / 2}" y="${H - 6}" fill="#8b949e" font-size="9" text-anchor="middle">cost/trip ($) — ${xSub}</text>`;
  html += `<text x="12" y="${H / 2}" fill="#8b949e" font-size="9" text-anchor="middle" transform="rotate(-90,12,${H / 2})">served % (${Y_MIN}–${Y_MAX}${yFull ? '' : ' zoom'})</text>`;

  const base = (pr) => (pr === 'waymo' ? '#f0883e' : '#58a6ff');
  pts.forEach((p) => {
    if (!yFull && p.served_pct < 85) return;
    const cx = Math.min(W - pad.r, Math.max(pad.l, xOfLog(p.cost_per_trip > 0 ? p.cost_per_trip : X_MIN)));
    const cy = Math.min(plotBottom, Math.max(pad.t, yOf(p.served_pct)));
    const below95 = p.served_pct < 95;
    const fo = below95 ? 0.38 : 0.95;
    const so = below95 ? 0.5 : 1;
    const pr = p.preset || 'tesla';
    const outlier = p.served_pct < 85;
    const strokeColor = outlier ? '#e8c547' : '#0d1117';
    const strokeW = outlier ? 0.75 : 1.5;
    const tipOut = outlier ? ' (outlier: below 85% served)' : '';
    const wt = (p.p10_wait_min != null && p.p50_wait_min != null && p.p90_wait_min != null)
      ? `, waits ${p.p10_wait_min.toFixed(1)}/${p.p50_wait_min.toFixed(1)}/${p.p90_wait_min.toFixed(1)}m (p10/p50/p90)`
      : '';
    html += `<circle cx="${cx}" cy="${cy}" r="4" fill="${base(pr)}" fill-opacity="${fo}" stroke="${strokeColor}" stroke-opacity="${outlier ? 1 : so}" stroke-width="${strokeW}">
      <title>Exp${p.exp} ${p.run_id} (${pr}): $${p.cost_per_trip.toFixed(2)}/trip, ${p.served_pct.toFixed(1)}% served${wt}${below95 ? ' (below 95% target)' : ''}${tipOut}</title></circle>`;
  });

  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
  svg.innerHTML = html;
}

function classifyLog(line) {
  if (line.includes('WARNING') || line.includes('JSON parse failed')) return 'warning';
  if (line.includes('ERROR')) return 'error';
  if (line.includes('HTTP Request')) return 'http';
  if (line.includes('Launched run')) return 'launched';
  if (line.includes(' done:') && line.includes('served%')) return 'done';
  if (line.includes('killing early') || line.includes('ETA kill') || line.includes('CANCELLED')) return 'kill';
  if (line.includes('Planning Exp') || line.includes('Hypothesis:')) return 'planning';
  return 'info';
}

(function initParetoYToggle() {
  const cb = document.getElementById('paretoYFull');
  if (!cb) return;
  try { cb.checked = localStorage.getItem('paretoYFull') === '1'; } catch (e) {}
  cb.addEventListener('change', () => {
    try { localStorage.setItem('paretoYFull', cb.checked ? '1' : '0'); } catch (e) {}
    renderPareto(_lastParetoPoints);
  });
})();

refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import sys

    _here = Path(__file__).resolve().parent
    if str(_here) not in sys.path:
        sys.path.insert(0, str(_here))

    print("Dashboard http://localhost:7331 — auto-reload on save (this file)")
    uvicorn.run(
        "dashboard:app",
        host="0.0.0.0",
        port=7331,
        log_level="warning",
        reload=True,
        reload_dirs=[str(_here)],
    )
